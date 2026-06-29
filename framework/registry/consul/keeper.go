package consul

import (
	"context"
	"game-server/framework/gen"
	"game-server/framework/pkg/glog"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
	"go.uber.org/zap"
)

func newServiceKeeper(client *api.Client, serviceId string, logger *zap.Logger, interval time.Duration) *serviceKeeper {
	return &serviceKeeper{
		client:    client,
		serviceId: serviceId,
		state:     gen.ServiceHealthStatePassing,
		interval:  interval,
		logger:    logger,
	}
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

func (service *serviceKeeper) run(ctx context.Context) {
	service.ctx, service.cancel = context.WithCancel(ctx)
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
	checkID := serviceID
	if !strings.HasPrefix(checkID, "service:") {
		checkID = "service:" + checkID
	}
	if err := service.client.Agent().UpdateTTL(checkID, "", state); err != nil {
		service.logger.Error("TTL定时更新服务状态", zap.String("service_id", serviceID), glog.Err(err))
		return err
	}
	service.logger.Debug("TTL定时更新服务状态", zap.String("service_id", serviceID), zap.String("status", state))
	return nil
}

func (service *serviceKeeper) stop() {
	if service.cancel != nil {
		service.cancel()
	}
}
