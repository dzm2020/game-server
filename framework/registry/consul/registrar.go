package consul

import (
	"context"
	"errors"
	"fmt"
	"game-server/framework/gen"
	"game-server/framework/grs"
	"game-server/framework/pkg/glog"
	"game-server/framework/pkg/netutil"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
	"go.uber.org/zap"
)

// Registrar wraps register-side capabilities.
type Registrar struct {
	client     *api.Client
	ttlMu      sync.RWMutex
	ttlCancels map[string]context.CancelFunc
	ttlStates  map[string]ttlState
}

type ttlState struct {
	status string
	note   string
}

// NewRegistrar
//
//	@Description: 基于配置创建独立的注册端客户端
//	@param cfg
//	@return *Registrar
//	@return error
func NewRegistrar(options *gen.ConsulOptions) (*Registrar, error) {
	client, err := newConsulClient(options)
	if err != nil {
		return nil, err
	}
	glog.Info("consul registrar initialized")
	return newRegistrar(client), nil
}

// newRegistrar
//
//	@Description: 使用客户端与日志器创建 Registrar
//	@param client
//	@param logger
//	@return *Registrar
func newRegistrar(client *api.Client) *Registrar {
	return &Registrar{
		client:     client,
		ttlCancels: make(map[string]context.CancelFunc),
		ttlStates:  make(map[string]ttlState),
	}
}

// Register
//
//	@Description: 向 Consul 注册服务实例，并在启用 TTL 时自动启动心跳保活
//	@receiver rr
//	@param reg
//	@return error
func (rr *Registrar) Register(reg ServiceInstance, options *gen.ConsulOptions) error {
	if options == nil {
		return errors.New("consul options is required")
	}
	if reg.ID == "" || reg.Name == "" {
		return errors.New("invalid service registration: id/name are required")
	}

	host, rawPort, err := netutil.SplitHostPort(reg.RpcAddress)
	if err != nil {
		return err
	}

	check := &api.AgentServiceCheck{
		TTL:                            options.TTL.String(),
		DeregisterCriticalServiceAfter: options.DeregisterAfter.String(),
	}

	meta := make(map[string]string, len(reg.Meta)+2)
	for k, v := range reg.Meta {
		meta[k] = v
	}

	meta["ext_address"] = reg.ExtAddress

	serviceReg := &api.AgentServiceRegistration{
		ID:      reg.ID,
		Name:    reg.Name,
		Address: host,
		Port:    rawPort,
		Tags:    reg.Tags,
		Meta:    meta,
		Check:   check,
	}

	if err = rr.client.Agent().ServiceRegister(serviceReg); err != nil {
		glog.Error("register service failed", zap.String("service_id", reg.ID), zap.Error(err))
		return err
	}
	glog.Info("register service success", zap.String("service_id", reg.ID), zap.String("service_name", reg.Name))

	if options.TTL > 0 {
		rr.startTTLHeartbeat(reg.ID, options.TTL, "")
	}
	return nil
}

// Deregister
//
//	@Description: 从 Consul 注销服务实例，成功后停止对应 TTL 心跳协程
//	@receiver rr
//	@param serviceID
//	@return error
func (rr *Registrar) Deregister(serviceID string) error {
	if serviceID == "" {
		return errors.New("service id is required")
	}
	if err := rr.client.Agent().ServiceDeregister(serviceID); err != nil {
		glog.Error("deregister service failed", zap.String("service_id", serviceID), zap.Error(err))
		return fmt.Errorf("deregister service: %w", err)
	}
	rr.stopTTLHeartbeat(serviceID)
	glog.Info("deregister service success", zap.String("service_id", serviceID))
	return nil
}

// SetHealthState
//
//	@Description: 更新服务健康状态
//	@receiver rr
//	@param serviceID
//	@param note
//	@return error
func (rr *Registrar) SetHealthState(serviceID string, state gen.ServiceHealthState) error {
	if err := rr.setTTLState(serviceID, "", state); err != nil {
		glog.Error("set ttl pass state failed", zap.String("service_id", serviceID), zap.Error(err))
		return err
	}
	glog.Info("ttl state updated", zap.String("service_id", serviceID), zap.String("state", state))
	return nil
}

// updateTTL
//
//	@Description: 调用 Consul API 更新指定服务 TTL 状态
//	@receiver rr
//	@param serviceID
//	@param note
//	@param status
//	@return error
func (rr *Registrar) updateTTL(serviceID, note, status string) error {
	if serviceID == "" {
		return errors.New("service id is required")
	}
	checkID := serviceID
	if !strings.HasPrefix(checkID, "service:") {
		checkID = "service:" + checkID
	}
	if err := rr.client.Agent().UpdateTTL(checkID, note, status); err != nil {
		return fmt.Errorf("update ttl for %q: %w", serviceID, err)
	}
	return nil
}

// startTTLHeartbeat
//
//	@Description: 启动 TTL 心跳协程并定时上报当前状态
//	@receiver rr
//	@param serviceID
//	@param ttl

// @param note
func (rr *Registrar) startTTLHeartbeat(serviceID string, ttl time.Duration, note string) {
	interval := ttl / 2

	rr.stopTTLHeartbeat(serviceID)

	ctx, cancel := context.WithCancel(context.Background())
	rr.ttlMu.Lock()
	rr.ttlStates[serviceID] = ttlState{
		status: api.HealthPassing,
		note:   note,
	}
	rr.ttlCancels[serviceID] = cancel
	rr.ttlMu.Unlock()

	grs.SafeGo(func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		// Send first heartbeat immediately.
		current, ok := rr.getTTLState(serviceID)
		if ok {
			if err := rr.updateTTL(serviceID, current.note, current.status); err != nil {
				glog.Warn("initial ttl heartbeat failed", zap.String("service_id", serviceID), zap.Error(err))
			}
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				current, ok := rr.getTTLState(serviceID)
				if !ok {
					return
				}
				if err := rr.updateTTL(serviceID, current.note, current.status); err != nil {
					glog.Warn("ttl heartbeat failed", zap.String("service_id", serviceID), zap.Error(err))
				}
			}
		}
	})
	glog.Debug("ttl heartbeat started", zap.String("service_id", serviceID), zap.Duration("interval", interval))
}

// stopTTLHeartbeat
//
//	@Description: 停止 TTL 心跳协程并清理内存状态
//	@receiver rr
//	@param serviceID
func (rr *Registrar) stopTTLHeartbeat(serviceID string) {
	rr.ttlMu.Lock()
	cancel, ok := rr.ttlCancels[serviceID]
	if ok {
		delete(rr.ttlCancels, serviceID)
		delete(rr.ttlStates, serviceID)
	}
	rr.ttlMu.Unlock()
	if ok {
		cancel()
	}
	glog.Debug("ttl heartbeat stopped", zap.String("service_id", serviceID))
}

// setTTLState
//
//	@Description: 更新内存中的 TTL 状态，实际上报由心跳协程执行
//	@receiver rr
//	@param serviceID
//	@param note
//	@param status
//	@return error
func (rr *Registrar) setTTLState(serviceID, note, status string) error {
	if serviceID == "" {
		return errors.New("service id is required")
	}
	rr.ttlMu.Lock()
	_, ok := rr.ttlCancels[serviceID]
	if !ok {
		rr.ttlMu.Unlock()
		return fmt.Errorf("ttl heartbeat not running for %q", serviceID)
	}
	current := rr.ttlStates[serviceID]
	current.status = status
	if note != "" {
		current.note = note
	}
	rr.ttlStates[serviceID] = current
	rr.ttlMu.Unlock()
	return nil
}

// getTTLState
//
//	@Description: 获取指定服务当前缓存的 TTL 状态
//	@receiver rr
//	@param serviceID
//	@return ttlState
//	@return bool
func (rr *Registrar) getTTLState(serviceID string) (ttlState, bool) {
	rr.ttlMu.RLock()
	state, ok := rr.ttlStates[serviceID]
	rr.ttlMu.RUnlock()
	return state, ok
}
