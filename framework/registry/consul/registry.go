package consul

import (
	"context"
	"fmt"
	"game-server/framework/gen"
	"game-server/framework/pkg/component"
	"game-server/framework/pkg/glog"

	"github.com/hashicorp/consul/api"
	"go.uber.org/zap"
)

const registryComponent = "registry.consul"

type ServiceInstance = gen.ServiceInstance

func New(node gen.INode) *Registry {
	return NewWithOptions(node, Options{})
}

func NewWithOptions(node gen.INode, options Options) *Registry {
	return &Registry{
		node:    node,
		options: options,
	}
}

type Registry struct {
	component.BaseComponent
	*Registrar
	*Discoverer
	node    gen.INode
	options Options
	logger  *zap.Logger
}

func (r *Registry) Init(ctx context.Context) error {
	return r.BaseComponent.GuardInit(ctx, func(ctx context.Context) error {
		//  初始化日志
		r.logger = glog.GetLogger().With(zap.String("component", registryComponent))

		r.options = NormalizeOptions(r.options)
		if err := ValidateOptions(r.options); err != nil {
			return fmt.Errorf("invalid consul options: %w", err)
		}
		config := toConsulConfig(r.options)

		//  创建客户端
		client, err := api.NewClient(config)
		if err != nil {
			return err
		}
		r.Registrar = newRegistrar(client, r.logger)
		r.Discoverer = newDiscoverer(client, r.logger)

		r.logger.Info("初始化完成",
			zap.String("address", r.options.Address),
			zap.Duration("ttl", r.options.TTL),
			zap.Duration("deregisterAfter", r.options.DeregisterAfter))

		return nil
	})
}

func (r *Registry) Start(ctx context.Context) error {
	return r.BaseComponent.GuardStart(ctx, func(ctx context.Context) error {
		if r.Discoverer == nil || r.Registrar == nil {
			return gen.ErrRegistryNil
		}
		if ctx == nil {
			ctx = context.Background()
		}
		if err := r.Discoverer.Start(ctx); err != nil {
			return err
		}
		if err := r.Registrar.Start(ctx); err != nil {
			_ = r.Discoverer.Stop(context.Background())
			return err
		}
		return nil
	})
}

func (r *Registry) Register(reg ServiceInstance) error {
	if r.Status() != component.StateStarted {
		return gen.ErrComponentNotStart
	}
	return r.Registrar.Register(reg, r.options)
}

func (r *Registry) Deregister(serviceID string) error {
	if r.Status() != component.StateStarted {
		return gen.ErrComponentNotStart
	}
	return r.Registrar.Deregister(serviceID)
}

func (r *Registry) SetHealthState(serviceID string, state gen.ServiceHealthState) error {
	if r.Status() != component.StateStarted {
		return gen.ErrComponentNotStart
	}
	return r.Registrar.SetHealthState(serviceID, state)
}

func (r *Registry) Discover(serviceName string) []ServiceInstance {
	if r.Status() != component.StateStarted {
		return nil
	}
	return r.Discoverer.Discover(serviceName)
}

func (r *Registry) DiscoverAll() map[string][]ServiceInstance {
	if r.Status() != component.StateStarted {
		return nil
	}
	return r.Discoverer.DiscoverAll()
}

func (r *Registry) ListServices() []string {
	if r.Status() != component.StateStarted {
		return nil
	}
	return r.Discoverer.ListServices()
}

func (r *Registry) Watch(serviceName string, onChange gen.ServiceChangeHandler) (string, error) {
	if r.Status() != component.StateStarted {
		return "", gen.ErrComponentNotStart
	}
	return r.Discoverer.Watch(serviceName, onChange)
}

func (r *Registry) Unwatch(serviceName, watchID string) {
	if r.Status() != component.StateStarted {
		return
	}
	r.Discoverer.Unwatch(serviceName, watchID)
}

func (r *Registry) Stop(ctx context.Context) error {
	return r.BaseComponent.GuardStop(ctx, func(ctx context.Context) error {
		if ctx == nil {
			ctx = context.Background()
		}
		var lastErr error
		if err := r.Discoverer.Stop(ctx); err != nil {
			lastErr = err
		}
		if err := r.Registrar.Stop(ctx); err != nil {
			lastErr = err
		}
		return lastErr
	})
}
