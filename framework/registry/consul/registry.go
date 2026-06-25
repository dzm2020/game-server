package consul

import (
	"context"
	"game-server/framework/gen"
	"game-server/framework/pkg/glog"
	"sync"

	"github.com/hashicorp/consul/api"
	"go.uber.org/zap"
)

type ServiceInstance = gen.ServiceInstance

// Registry aggregates register and discover capabilities.
type Registry struct {
	*Registrar
	*Discoverer
	options gen.ConsulOptions

	runMu     sync.Mutex
	runCancel context.CancelFunc
}

// New creates a registry using supplied config.
func New(options gen.ConsulOptions) (*Registry, error) {
	config := toConsulConfig(options)

	client, err := api.NewClient(config)
	if err != nil {
		glog.Error("consul新建客户端", zap.Error(err))
		return nil, err
	}
	registry := &Registry{
		options:    options,
		Registrar:  newRegistrar(client),
		Discoverer: newDiscoverer(client),
	}
	glog.Info("consul初始化",
		zap.String("address", options.Address),
		zap.String("scheme", options.Scheme),
		zap.String("token", options.Token),
		zap.Duration("ttl", options.TTL),
		zap.Duration("deregisterAfter", options.DeregisterAfter))
	return registry, nil
}

func (r *Registry) Register(reg ServiceInstance) error {
	return r.Registrar.Register(reg, r.options)
}

func (r *Registry) Deregister(serviceID string) error {
	return r.Registrar.Deregister(serviceID)
}

func (r *Registry) SetHealthState(serviceID string, state gen.ServiceHealthState) error {
	return r.Registrar.SetHealthState(serviceID, state)
}

func (r *Registry) Start(ctx context.Context) error {
	r.runMu.Lock()
	if r.runCancel != nil {
		r.runMu.Unlock()
		return nil
	}
	runCtx, cancel := context.WithCancel(ctx)
	r.runCancel = cancel
	r.runMu.Unlock()

	if err := r.Discoverer.Start(runCtx); err != nil {
		cancel()
		r.runMu.Lock()
		r.runCancel = nil
		r.runMu.Unlock()
		return err
	}
	return nil
}

func (r *Registry) Discover(serviceName string) []ServiceInstance {
	return r.Discoverer.Discover(serviceName)
}

func (r *Registry) DiscoverAll() map[string][]ServiceInstance {
	return r.Discoverer.DiscoverAll()
}

func (r *Registry) ListServices() []string {
	return r.Discoverer.ListServices()
}

func (r *Registry) Watch(serviceName string, onChange gen.ServiceChangeHandler) (string, error) {
	return r.Discoverer.Watch(serviceName, onChange)
}

func (r *Registry) Unwatch(serviceName, watchID string) {
	r.Discoverer.Unwatch(serviceName, watchID)
}

func (r *Registry) Stop(ctx context.Context) error {
	r.runMu.Lock()
	cancel := r.runCancel
	r.runCancel = nil
	r.runMu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}
