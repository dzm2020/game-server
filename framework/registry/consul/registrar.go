package consul

import (
	"context"
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
	ttlGroup   *grs.Group
}

type ttlState struct {
	status string
	note   string
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
		ttlGroup:   grs.NewGroup(),
	}
}

// Register
//
//	@Description: 向 Consul 注册服务实例，并在启用 TTL 时自动启动心跳保活
//	@receiver rr
//	@param reg
//	@return error
func (rr *Registrar) Register(reg ServiceInstance, options gen.ConsulOptions) error {
	if reg.ID == "" || reg.Name == "" {
		glog.Error("Consul注册服务实例", zap.String("service_name", reg.Name), zap.String("service_id", reg.ID))
		return fmt.Errorf("%w: id/name are required", gen.ErrConsulInvalidServiceReg)
	}

	host, rawPort, err := netutil.SplitHostPort(reg.RpcAddress)
	if err != nil {
		glog.Error("Consul注册服务实例", glog.Component("registry.consul.registrar"), zap.String("address", reg.RpcAddress), glog.Err(err))
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
		glog.Error("Consul注册服务实例", glog.Component("registry.consul.registrar"), zap.String("service_id", reg.ID), glog.Err(err))
		return err
	}

	glog.Info("Consul注册服务实例",
		zap.String("service_id", reg.ID),
		zap.String("service_name", reg.Name),
		zap.String("address", reg.RpcAddress),
		zap.String("ext_address", reg.ExtAddress),
		zap.Strings("tags", reg.Tags))

	rr.startTTLHeartbeat(reg.ID, options.TTL, "")

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
		return gen.ErrConsulServiceIDRequired
	}
	if err := rr.client.Agent().ServiceDeregister(serviceID); err != nil {
		glog.Error("deregister service failed", glog.Component("registry.consul.registrar"), zap.String("service_id", serviceID), glog.Err(err))
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
		glog.Error("set ttl pass state failed", glog.Component("registry.consul.registrar"), zap.String("service_id", serviceID), glog.Err(err))
		return err
	}
	glog.Info("ttl state updated", zap.String("service_id", serviceID), zap.String("state", state))
	return nil
}

// updateTTL
//
//	@Description: TTL定时更新服务状态
//	@receiver rr
//	@param serviceID
//	@param note
//	@param status
//	@return error
func (rr *Registrar) updateTTL(serviceID, note, status string) error {
	if serviceID == "" {
		glog.Error("TTL定时更新服务状态", zap.String("service_id", serviceID), zap.String("note", note))
		return gen.ErrConsulServiceIDRequired
	}
	checkID := serviceID
	if !strings.HasPrefix(checkID, "service:") {
		checkID = "service:" + checkID
	}
	if err := rr.client.Agent().UpdateTTL(checkID, note, status); err != nil {
		glog.Error("TTL定时更新服务状态", glog.Component("registry.consul.registrar"), zap.String("service_id", serviceID), zap.String("note", note), glog.Err(err))
		return err
	}
	glog.Debug("TTL定时更新服务状态", zap.String("service_id", serviceID), zap.String("note", note), zap.String("status", status))
	return nil
}

// startTTLHeartbeat
//
//	@Description: 启动服务TTL协程启动
//	@receiver rr
//	@param serviceID
//	@param ttl

// @param note
func (rr *Registrar) startTTLHeartbeat(serviceID string, ttl time.Duration, note string) {
	interval := ttl / 2

	glog.Info("启动服务TTL协程启动", zap.String("service_id", serviceID),
		zap.Duration("ttl", ttl), zap.Duration("interval", interval))

	rr.stopTTLHeartbeat(serviceID)

	ctx, cancel := context.WithCancel(context.Background())
	rr.ttlMu.Lock()
	rr.ttlStates[serviceID] = ttlState{
		status: api.HealthPassing,
		note:   note,
	}
	rr.ttlCancels[serviceID] = cancel
	rr.ttlMu.Unlock()

	rr.ttlGroup.Go(func() {
		glog.Info("服务TTL协程启动成功", zap.String("service_id", serviceID),
			zap.Duration("ttl", ttl), zap.Duration("interval", interval))

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		// Send first heartbeat immediately.
		current, ok := rr.getTTLState(serviceID)
		if ok {
			_ = rr.updateTTL(serviceID, current.note, current.status)
		}

		for {
			select {
			case <-ctx.Done():
				glog.Info("服务TTL协程退出", zap.String("service_id", serviceID))
				return
			case <-ticker.C:
				current, ok := rr.getTTLState(serviceID)
				if !ok {
					return
				}
				_ = rr.updateTTL(serviceID, current.note, current.status)
			}
		}
	})
}

// stopTTLHeartbeat
//
//	@Description: 停止TTL心跳协程并清理内存状态
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
		glog.Info("停止服务TTL心跳协程并清理内存状态", zap.String("service_id", serviceID))
	}
}

func (rr *Registrar) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	rr.ttlMu.Lock()
	cancels := make([]context.CancelFunc, 0, len(rr.ttlCancels))
	for serviceID, cancel := range rr.ttlCancels {
		cancels = append(cancels, cancel)
		delete(rr.ttlCancels, serviceID)
		delete(rr.ttlStates, serviceID)
	}
	rr.ttlMu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
	if rr.ttlGroup == nil {
		return nil
	}
	return rr.ttlGroup.Wait(ctx)
}

// setTTLState
//
//	@Description: 更新内存中的TTL状态，实际上报由心跳协程执行
//	@receiver rr
//	@param serviceID
//	@param note
//	@param status
//	@return error
func (rr *Registrar) setTTLState(serviceID, note, status string) error {
	if serviceID == "" {
		glog.Error("更新内存中的TTL状态", zap.String("service_id", serviceID))
		return gen.ErrConsulServiceIDRequired
	}
	rr.ttlMu.Lock()
	_, ok := rr.ttlCancels[serviceID]
	if !ok {
		rr.ttlMu.Unlock()
		glog.Error("更新内存中的TTL状态", zap.String("service_id", serviceID), zap.String("reason", "服务不存在"))
		return fmt.Errorf("%w: %q", gen.ErrConsulTTLHeartbeatNotRun, serviceID)
	}
	current := rr.ttlStates[serviceID]
	current.status = status
	if note != "" {
		current.note = note
	}
	rr.ttlStates[serviceID] = current
	rr.ttlMu.Unlock()
	glog.Info("更新内存中的TTL状态", zap.String("service_id", serviceID), zap.String("status", status))
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
