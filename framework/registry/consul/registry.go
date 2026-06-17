package consul

import (
	"context"
	"fmt"
	"game-server/framework/pkg/glog"
	"game-server/framework/registry/define"
	"time"

	"github.com/hashicorp/consul/api"
	"go.uber.org/zap"
)

// Config defines Consul client and polling behavior.
type Config struct {
	Address           string        `json:"address"`
	Scheme            string        `json:"scheme"`
	Token             string        `json:"token"`
	Datacenter        string        `json:"datacenter"`
	TTL               time.Duration `json:"ttl"`
	DeregisterAfter   time.Duration `json:"deregisterAfter"` // 默认值：1m (1分钟)   最小值：1m (1分钟)。Consul 的清理进程每30秒运行一次
	HeartbeatInterval time.Duration `json:"heartbeatInterval"`
	HeartbeatNote     string        `json:"heartbeatNote"`
}

type ServiceInstance = define.ServiceInstance

// Registry aggregates register and discover capabilities.
type Registry struct {
	*Registrar
	*Discoverer
	cfg Config
}

// New creates a registry using supplied config.
func New(cfg Config) (*Registry, error) {
	client, err := newConsulClient(cfg)
	if err != nil {
		return nil, err
	}

	registry := &Registry{
		cfg:        cfg,
		Registrar:  newRegistrar(client),
		Discoverer: newDiscoverer(client),
	}
	glog.Info("consul registry initialized")
	return registry, nil
}

func (r *Registry) Register(reg ServiceInstance) error {
	return r.Registrar.Register(reg, r.cfg)
}

func (r *Registry) Deregister(serviceID string) error {
	return r.Registrar.Deregister(serviceID)
}

func (r *Registry) SetHealthState(serviceID string, state define.HealthState) error {
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

func (r *Registry) Watch(serviceName string, onChange define.ServiceChangeHandler) (string, error) {
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
func newConsulClient(cfg Config) (*api.Client, error) {
	consulCfg := api.DefaultConfig()
	if cfg.Address != "" {
		consulCfg.Address = cfg.Address
	}
	if cfg.Scheme != "" {
		consulCfg.Scheme = cfg.Scheme
	}
	if cfg.Token != "" {
		consulCfg.Token = cfg.Token
	}
	if cfg.Datacenter != "" {
		consulCfg.Datacenter = cfg.Datacenter
	}

	client, err := api.NewClient(consulCfg)
	if err != nil {
		glog.Error("create consul client failed", zap.Error(err))
		return nil, fmt.Errorf("create consul client: %w", err)
	}
	glog.Info("create consul client success", zap.String("address", consulCfg.Address))
	return client, nil
}
