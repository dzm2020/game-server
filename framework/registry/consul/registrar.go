package consul

import (
	"context"
	"game-server/framework/gen"
	"game-server/framework/grs"
	"game-server/framework/pkg/glog"
	"game-server/framework/pkg/netutil"
	"time"

	"github.com/duke-git/lancet/v2/maputil"
	"github.com/hashicorp/consul/api"
	"go.uber.org/zap"
)

// newRegistrar
//
//	@Description: 使用客户端与日志器创建 Registrar
//	@param client
//	@param logger
//	@return *Registrar
func newRegistrar(client *api.Client, logger *zap.Logger) *Registrar {
	return &Registrar{
		client:   client,
		services: maputil.NewConcurrentMap[string, *serviceKeeper](1),
		logger:   logger,
		group:    grs.NewGroup(context.Background()),
	}
}

// Registrar wraps register-side capabilities.
type Registrar struct {
	client   *api.Client
	services *maputil.ConcurrentMap[string, *serviceKeeper]
	group    *grs.Group
	logger   *zap.Logger
}

func (rr *Registrar) Start(ctx context.Context) error {
	rr.group = grs.NewGroup(ctx)
	return nil
}

// Register
//
//	@Description: 向 Consul 注册服务实例，并在启用 TTL 时自动启动心跳保活
//	@receiver rr
//	@param reg
//	@return error
func (rr *Registrar) Register(reg ServiceInstance, options gen.ConsulOptions) error {
	if reg.ID == "" || reg.Name == "" {
		return gen.ErrConsulInvalidServiceReg
	}
	serviceReg, err := rr.instanceToRegistration(reg, options)
	if err != nil {
		return err
	}
	if err = rr.client.Agent().ServiceRegister(serviceReg); err != nil {
		rr.logger.Error("注册失败", zap.String("service_id", reg.ID), glog.Err(err))
		return err
	}

	rr.logger.Info("注册成功",
		zap.String("service_id", reg.ID),
		zap.String("service_name", reg.Name),
		zap.String("address", reg.RpcAddress),
		zap.String("ext_address", reg.ExtAddress),
		zap.Strings("tags", reg.Tags))

	rr.addServiceKeeper(reg.ID, options.TTL)

	return nil
}

// instanceToRegistration
//
//	@Description: 将instance转换成consul api需要的格式
//	@receiver rr
//	@param reg
//	@param options
//	@return *api.AgentServiceRegistration
func (rr *Registrar) instanceToRegistration(reg ServiceInstance, options gen.ConsulOptions) (*api.AgentServiceRegistration, error) {
	host, rawPort, err := netutil.SplitHostPort(reg.RpcAddress)
	if err != nil {
		rr.logger.Error("注册失败", zap.String("address", reg.RpcAddress), glog.Err(err))
		return nil, err
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
	return serviceReg, nil
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
		rr.logger.Error("注销失败", zap.String("service_id", serviceID), glog.Err(err))
		return err
	}
	rr.removeServiceKeeper(serviceID)
	return nil
}

// addService
//
//	@Description:
//	@receiver rr
//	@param serviceID
//	@param ttl

// @param note
func (rr *Registrar) addServiceKeeper(serviceID string, ttl time.Duration) {
	ok := false
	keeper := newServiceKeeper(rr.client, serviceID, rr.logger, ttl/2)
	keeper, ok = rr.services.GetOrSet(serviceID, keeper)
	if ok {
		return
	}
	keeper.run(rr.group.Context())
}

// removeService
//
//	@Description: 移除服务
//	@receiver rr
//	@param serviceID
func (rr *Registrar) removeServiceKeeper(serviceID string) {
	s, ok := rr.services.Get(serviceID)
	if !ok || s == nil {
		return
	}
	s.stop()
	rr.logger.Info("移除注册服务", zap.String("service_id", serviceID))
}

// SetHealthState
//
//	@Description: 更新内存中的TTL状态，实际上报由心跳协程执行
//	@receiver rr
//	@param serviceID
//	@param note
//	@param status
//	@return error
func (rr *Registrar) SetHealthState(serviceID, status string) error {
	s, ok := rr.services.Get(serviceID)
	if !ok || s == nil {
		rr.logger.Error("更新内存中的TTL状态", zap.String("service_id", serviceID), zap.String("reason", "服务不存在"))
		return gen.ErrConsulTTLHeartbeatNotRun
	}
	s.setState(status)
	rr.logger.Info("更新内存中的TTL状态", zap.String("service_id", serviceID), zap.String("status", status))
	return nil
}

// getTTLState
//
//	@Description: 获取指定服务当前缓存的 TTL 状态
//	@receiver rr
//	@param serviceID
//	@return ttlState
//	@return bool
func (rr *Registrar) getTTLState(serviceID string) (string, bool) {
	s, ok := rr.services.Get(serviceID)
	if !ok || s == nil {
		return "", false
	}
	return s.getState(), ok
}

func (rr *Registrar) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if rr.group != nil {
		rr.group.Cancel()
		if err := rr.group.Wait(ctx); err != nil {
			return err
		}
	}
	return nil
}
