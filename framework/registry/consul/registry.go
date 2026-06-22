package consul

import (
	"context"
	"fmt"
	"game-server/framework/gen"
	"game-server/framework/pkg/glog"

	"github.com/hashicorp/consul/api"
	"go.uber.org/zap"
)

type ServiceInstance = gen.ServiceInstance

// Registry aggregates register and discover capabilities.
type Registry struct {
	*Registrar
	*Discoverer
	options *gen.ConsulOptions
}

// New creates a registry using supplied config.
func New(options *gen.ConsulOptions) (*Registry, error) {
	client, err := newConsulClient(options)
	if err != nil {
		return nil, err
	}

	registry := &Registry{
		options:    options,
		Registrar:  newRegistrar(client),
		Discoverer: newDiscoverer(client),
	}
	glog.Info("consul registry initialized")
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

func (r *Registry) Run(ctx context.Context) error {
	return r.Discoverer.Run(ctx)
}

func (r *Registry) Discover(serviceName string) ([]ServiceInstance, error) {
	return r.Discoverer.Discover(serviceName)
}

func (r *Registry) DiscoverAll() (map[string][]ServiceInstance, error) {
	return r.Discoverer.DiscoverAll()
}

func (r *Registry) ListServices() ([]string, error) {
	return r.Discoverer.ListServices()
}

func (r *Registry) Watch(serviceName string, onChange gen.ServiceChangeHandler) (string, error) {
	return r.Discoverer.Watch(serviceName, onChange)
}

func (r *Registry) Unwatch(serviceName, watchID string) {
	r.Discoverer.Unwatch(serviceName, watchID)
}

func (r *Registry) Shutdown() {

}

// newConsulClient
//
//	@Description: 基于配置创建 Consul API 客户端
//	@param cfg
//	@param logger
//	@return *api.Client
//	@return error
func newConsulClient(options *gen.ConsulOptions) (*api.Client, error) {
	consulCfg := api.DefaultConfig()
	if options.Address != "" {
		consulCfg.Address = options.Address
	}
	if options.Scheme != "" {
		consulCfg.Scheme = options.Scheme
	}
	if options.Token != "" {
		consulCfg.Token = options.Token
	}
	if options.Datacenter != "" {
		consulCfg.Datacenter = options.Datacenter
	}

	client, err := api.NewClient(consulCfg)
	if err != nil {
		glog.Error("create consul client failed", zap.Error(err))
		return nil, fmt.Errorf("create consul client: %w", err)
	}
	glog.Info("create consul client success", zap.String("address", consulCfg.Address))
	return client, nil
}
