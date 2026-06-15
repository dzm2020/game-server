package consulregistry

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
)

// Registrar wraps register-side capabilities.
type Registrar struct {
	client     *api.Client
	logger     Logger
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
func NewRegistrar(cfg Config) (*Registrar, error) {
	logger := ensureLogger(cfg.Logger)
	client, err := newConsulClient(cfg, logger)
	if err != nil {
		return nil, err
	}
	logger.Infof("独立 Registrar 初始化完成")
	return newRegistrar(client, logger), nil
}

// newRegistrar
//
//	@Description: 使用客户端与日志器创建 Registrar
//	@param client
//	@param logger
//	@return *Registrar
func newRegistrar(client *api.Client, logger Logger) *Registrar {
	return &Registrar{
		client:     client,
		logger:     ensureLogger(logger),
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
func (rr *Registrar) Register(reg ServiceRegistration) error {
	if reg.ID == "" || reg.Name == "" || reg.Address == "" || reg.Port <= 0 {
		return errors.New("invalid service registration: id/name/address/port are required")
	}
	var check *api.AgentServiceCheck
	if reg.TTL > 0 {
		check = &api.AgentServiceCheck{
			TTL: reg.TTL.String(),
		}
		if reg.DeregisterAfter > 0 {
			check.DeregisterCriticalServiceAfter = reg.DeregisterAfter.String()
		} else {
			check.DeregisterCriticalServiceAfter = "1m"
		}
	}

	serviceReg := &api.AgentServiceRegistration{
		ID:      reg.ID,
		Name:    reg.Name,
		Address: reg.Address,
		Port:    reg.Port,
		Tags:    reg.Tags,
		Meta:    reg.Meta,
		Check:   check,
	}

	if err := rr.client.Agent().ServiceRegister(serviceReg); err != nil {
		rr.logger.Errorf("注册服务失败, id=%s err=%v", reg.ID, err)
		return fmt.Errorf("register service: %w", err)
	}
	rr.logger.Infof("服务注册成功, id=%s name=%s", reg.ID, reg.Name)

	if reg.TTL > 0 {
		note := reg.HeartbeatNote
		if note == "" {
			note = "auto ttl heartbeat"
		}
		rr.startTTLHeartbeat(reg.ID, reg.TTL, reg.HeartbeatInterval, note)
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
		rr.logger.Errorf("注销服务失败, id=%s err=%v", serviceID, err)
		return fmt.Errorf("deregister service: %w", err)
	}
	rr.stopTTLHeartbeat(serviceID)
	rr.logger.Infof("服务注销成功, id=%s", serviceID)
	return nil
}

// TTLPass
//
//	@Description: 将服务 TTL 状态更新为健康
//	@receiver rr
//	@param serviceID
//	@param note
//	@return error
func (rr *Registrar) TTLPass(serviceID string, note string) error {
	if err := rr.setTTLState(serviceID, note, api.HealthPassing); err != nil {
		rr.logger.Errorf("设置 TTL 为健康失败, id=%s err=%v", serviceID, err)
		return err
	}
	rr.logger.Infof("TTL 状态已更新, id=%s status=passing", serviceID)
	return nil
}

// TTLWarn
//
//	@Description: 将服务 TTL 状态更新为警告
//	@receiver rr
//	@param serviceID
//	@param note
//	@return error
func (rr *Registrar) TTLWarn(serviceID string, note string) error {
	if err := rr.setTTLState(serviceID, note, api.HealthWarning); err != nil {
		rr.logger.Errorf("设置 TTL 为警告失败, id=%s err=%v", serviceID, err)
		return err
	}
	rr.logger.Infof("TTL 状态已更新, id=%s status=warning", serviceID)
	return nil
}

// TTLFail
//
//	@Description: 将服务 TTL 状态更新为故障
//	@receiver rr
//	@param serviceID
//	@param note
//	@return error
func (rr *Registrar) TTLFail(serviceID string, note string) error {
	if err := rr.setTTLState(serviceID, note, api.HealthCritical); err != nil {
		rr.logger.Errorf("设置 TTL 为故障失败, id=%s err=%v", serviceID, err)
		return err
	}
	rr.logger.Infof("TTL 状态已更新, id=%s status=critical", serviceID)
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
//	@param interval
//	@param note
func (rr *Registrar) startTTLHeartbeat(serviceID string, ttl time.Duration, interval time.Duration, note string) {
	if interval <= 0 {
		interval = ttl / 2
	}
	if interval <= 0 {
		interval = time.Second
	}

	rr.stopTTLHeartbeat(serviceID)

	ctx, cancel := context.WithCancel(context.Background())
	rr.ttlMu.Lock()
	rr.ttlStates[serviceID] = ttlState{
		status: api.HealthPassing,
		note:   note,
	}
	rr.ttlCancels[serviceID] = cancel
	rr.ttlMu.Unlock()

	safeGo(rr.logger, goName("registrar.ttlHeartbeat", serviceID), func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		// Send first heartbeat immediately.
		current, ok := rr.getTTLState(serviceID)
		if ok {
			if err := rr.updateTTL(serviceID, current.note, current.status); err != nil {
				rr.logger.Warnf("首次 TTL 心跳上报失败, id=%s err=%v", serviceID, err)
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
					rr.logger.Warnf("TTL 心跳上报失败, id=%s err=%v", serviceID, err)
				}
			}
		}
	})
	rr.logger.Debugf("TTL 心跳协程已启动, id=%s interval=%s", serviceID, interval)
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
	rr.logger.Debugf("TTL 心跳协程已停止, id=%s", serviceID)
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
