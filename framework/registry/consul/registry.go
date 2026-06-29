package consul

import (
	"context"
	"game-server/framework/gen"
	"game-server/framework/pkg/component"
	"game-server/framework/pkg/glog"

	"github.com/hashicorp/consul/api"
	"go.uber.org/zap"
)

const registryComponent = "registry.consul"

type ServiceInstance = gen.ServiceInstance

func New(node gen.INode) *Registry {
	return &Registry{
		node: node,
	}
}

type Registry struct {
	component.BaseComponent
	*Registrar
	*Discoverer
	node    gen.INode
	options gen.ConsulOptions
	logger  *zap.Logger
}

func (r *Registry) Init(ctx context.Context) error {
	if err := r.BaseComponent.Init(ctx); err != nil {
		return err
	}
	//  初始化日志
	r.logger = glog.GetLogger().With(zap.String("component", registryComponent))

	//  初始化options
	options := r.node.GetOptions().Consul
	r.options = normalization(options)
	if err := validate(r.options); err != nil {
		return err
	}
	config := toConsulConfig(options)

	//  创建客户端
	client, err := api.NewClient(config)
	if err != nil {
		return err
	}
	r.Registrar = newRegistrar(client, r.logger)
	r.Discoverer = newDiscoverer(client, r.logger)

	r.logger.Info("初始化完成",
		zap.String("address", options.Address),
		zap.Duration("ttl", options.TTL),
		zap.Duration("deregisterAfter", options.DeregisterAfter))

	return nil
}

func (r *Registry) Start(ctx context.Context) error {
	if er := r.BaseComponent.Start(ctx); er != nil {
		return er
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.Discoverer.Start(ctx); err != nil {
		return err
	}
	if err := r.Registrar.Start(ctx); err != nil {
		return err
	}
	return nil
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
	if er := r.BaseComponent.Stop(ctx); er != nil {
		return er
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.Discoverer.Stop(ctx); err != nil {
		return err
	}
	if err := r.Registrar.Stop(ctx); err != nil {
		return err
	}
	return nil
}
