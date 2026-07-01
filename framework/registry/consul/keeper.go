package consul

import (
	"context"
	"game-server/framework/gen"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
	"go.uber.org/zap"
)

func newServiceKeeper(client *api.Client, serviceId string, logger *zap.Logger, interval time.Duration) *serviceKeeper {
	s := &serviceKeeper{
		client:    client,
		serviceId: serviceId,
		state:     gen.ServiceHealthStatePassing,
		interval:  interval,
		logger:    logger,
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	return s
}

type serviceKeeper struct {
	client    *api.Client
	serviceId string
	mu        sync.RWMutex
	state     string
	interval  time.Duration
	logger    *zap.Logger

	ctx    context.Context
	cancel context.CancelFunc
}

func (service *serviceKeeper) getState() string {
	service.mu.RLock()
	defer service.mu.RUnlock()
	return service.state
}

func (service *serviceKeeper) setState(state string) {
	service.mu.Lock()
	defer service.mu.Unlock()
	service.state = state
}

func (service *serviceKeeper) run() {

	ticker := time.NewTicker(service.interval)
	defer ticker.Stop()
	_ = service.updateTTL()
	for {
		select {
		case <-service.ctx.Done():
			return
		case <-ticker.C:
			_ = service.updateTTL()
		}
	}
}

// updateTTL
//
//	@Description: TTL定时更新服务状态
//	@receiver rr
//	@param serviceID
//	@param note
//	@param status
//	@return error
func (service *serviceKeeper) updateTTL() error {
	state := service.getState()
	serviceID := service.serviceId
	if serviceID == "" {
		service.logger.Error("TTL定时更新服务状态", zap.String("service_id", serviceID))
		return gen.ErrConsulServiceIDRequired
	}
	checkID := serviceCheckID(serviceID)
	if err := service.client.Agent().UpdateTTL(checkID, "", state); err != nil {
		if isUnknownCheckIDError(err) {
			// 服务已被注销后，check 可能已不存在；停止心跳循环避免噪音日志。
			service.logger.Debug("TTL心跳结束，check已不存在", zap.String("service_id", serviceID))
			service.stop()
			return nil
		}
		service.logger.Error("TTL定时更新服务状态", zap.String("service_id", serviceID), gen.FieldErr(err))
		return err
	}
	service.logger.Debug("TTL定时更新服务状态", zap.String("service_id", serviceID), zap.String("status", state))
	return nil
}

func isUnknownCheckIDError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "Unknown check ID")
}

func (service *serviceKeeper) stop() {
	if service.cancel != nil {
		service.cancel()
	}
}
