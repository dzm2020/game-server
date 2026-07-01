package consul

import (
	"context"
	"errors"
	"game-server/framework/gen"
	"game-server/framework/grs"
	"game-server/framework/pkg/component"
	"game-server/framework/pkg/glog"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

const registryComponent = "registry.consul"

type ServiceInstance = gen.ServiceInstance

func New(options Options) *Registry {
	return &Registry{
		options: options,
	}
}

type Registry struct {
	component.BaseComponent
	client  *api.Client
	options Options
	logger  *zap.Logger

	mu        sync.RWMutex
	keeper    *serviceKeeper
	serviceID string

	cacheMu       sync.RWMutex
	discoverCache map[string]discoverCacheEntry
	discoverSF    singleflight.Group
}

type discoverCacheEntry struct {
	expireAt  time.Time
	instances map[string]ServiceInstance
}

func (r *Registry) Init(ctx context.Context) error {
	return r.BaseComponent.GuardInit(ctx, func(ctx context.Context) error {
		var err error
		//  初始化日志
		r.logger = glog.GetLogger().With(gen.FieldComponent(registryComponent))
		//  检验参数
		r.options = NormalizeOptions(r.options)
		if err = ValidateOptions(r.options); err != nil {
			return err
		}
		//  创建客户端
		config := toConsulConfig(r.options)
		r.client, err = api.NewClient(config)
		if err != nil {
			return err
		}
		r.discoverCache = make(map[string]discoverCacheEntry)
		r.logger.Info("初始化完成",
			zap.String("address", r.options.Address),
			zap.Duration("ttl", r.options.TTL),
			zap.Duration("deregisterAfter", r.options.DeregisterAfter),
			zap.Duration("discoverCacheTTL", r.options.DiscoverCacheTTL),
		)

		return nil
	})
}

func (r *Registry) Start(ctx context.Context) error {
	return r.BaseComponent.GuardStart(ctx, func(ctx context.Context) error {
		return nil
	})
}

func (r *Registry) Register(reg ServiceInstance) error {
	if r.Status() != component.StateStarted {
		return gen.ErrComponentNotStart
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.keeper != nil {
		return gen.ErrServiceRegistered
	}

	if reg.ID == "" || reg.Name == "" {
		return gen.ErrConsulInvalidServiceReg
	}

	serviceReg, err := instanceToRegistration(reg, r.options)
	if err != nil {
		return err
	}

	if err = r.client.Agent().ServiceRegister(serviceReg); err != nil {
		return err
	}

	keeper := newServiceKeeper(r.client, reg.ID, r.logger, r.options.TTL/2)
	r.keeper = keeper
	r.serviceID = reg.ID
	r.clearDiscoverCache()
	grs.SafeGo(func() {
		keeper.run()
	})
	r.logger.Info("注册成功",
		zap.String("service_id", reg.ID),
		zap.String("service_name", reg.Name),
		zap.String("address", reg.RpcAddress),
		zap.String("ext_address", reg.ExtAddress),
		zap.Strings("tags", reg.Tags),
	)
	return nil
}

func (r *Registry) Deregister() error {
	if r.Status() != component.StateStarted {
		return gen.ErrComponentNotStart
	}
	return r.deregister()
}

func (r *Registry) deregister() error {
	r.mu.Lock()
	if r.keeper == nil {
		r.mu.Unlock()
		return gen.ErrServiceNotRegister
	}
	keeper := r.keeper
	serviceID := r.serviceID
	r.mu.Unlock()

	if err := r.client.Agent().ServiceDeregister(serviceID); err != nil {
		return err
	}

	r.mu.Lock()
	if r.keeper == keeper {
		r.keeper.stop()
		r.keeper = nil
		r.serviceID = ""
	}
	r.mu.Unlock()
	r.clearDiscoverCache()

	return nil
}

func (r *Registry) SetHealthState(state gen.ServiceHealthState) error {
	if r.Status() != component.StateStarted {
		return gen.ErrComponentNotStart
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.keeper == nil {
		return gen.ErrServiceNotRegister
	}
	r.keeper.setState(state)
	return nil
}

func (r *Registry) DiscoverByID(serviceID string) *ServiceInstance {
	r.cacheMu.RLock()
	defer r.cacheMu.RUnlock()
	for service, _ := range r.discoverCache {
		cache := r.Discover(service)
		if cache == nil {
			continue
		}
		instance, ok := cache[serviceID]
		if !ok {
			continue
		}
		return &instance
	}
	return nil
}

func (r *Registry) Discover(service string) map[string]ServiceInstance {
	if r.Status() != component.StateStarted {
		return nil
	}
	if service == "" {
		return nil
	}

	if cache, fresh, ok := r.getCache(service); ok && fresh {
		return cache
	}

	//  写锁包 RPC：也能防重复，但代价是更大临界区阻塞
	//  singleflight + 锁外 RPC：同样防重复，同时降低锁竞争，结构更可扩展
	result, _, _ := r.discoverSF.Do(service, func() (any, error) {
		if cache, fresh, ok := r.getCache(service); ok && fresh {
			return cache, nil
		}
		// stale-while-error: 回源失败时优先返回过期缓存
		staleCache, _, _ := r.getCache(service)
		query := &api.QueryOptions{
			UseCache: true,
		}
		entries, _, err := r.client.Health().Service(service, "", true, query)
		if err != nil {
			//  读取失败 降级使用过期缓存
			return staleCache, nil
		}
		instances := entriesToInstances(entries)
		r.setCache(service, instances)
		return instances, nil
	})
	if result == nil {
		return nil
	}
	return result.(map[string]ServiceInstance)
}

func (r *Registry) setCache(service string, instances map[string]ServiceInstance) {
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()
	r.discoverCache[service] = discoverCacheEntry{
		expireAt:  time.Now().Add(r.options.DiscoverCacheTTL),
		instances: instances,
	}
}

func (r *Registry) getCache(service string) (map[string]ServiceInstance, bool, bool) {
	r.cacheMu.RLock()
	cached, ok := r.discoverCache[service]
	r.cacheMu.RUnlock()
	if !ok {
		return nil, false, false
	}
	fresh := time.Now().Before(cached.expireAt)
	return cached.instances, fresh, true
}

func (r *Registry) clearDiscoverCache() {
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()
	for key := range r.discoverCache {
		delete(r.discoverCache, key)
	}
}

func (r *Registry) Stop(ctx context.Context) error {
	return r.BaseComponent.GuardStop(ctx, func(ctx context.Context) error {
		if err := r.deregister(); err != nil && !errors.Is(err, gen.ErrServiceNotRegister) {
			return err
		}
		r.clearDiscoverCache()
		return nil
	})
}
