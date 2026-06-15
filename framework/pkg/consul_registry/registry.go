package consulregistry

import (
	"context"
	"fmt"
	"time"

	"github.com/hashicorp/consul/api"
)

// Config defines Consul client and polling behavior.
type Config struct {
	Address    string `json:"address"`
	Scheme     string `json:"scheme"`
	Token      string `json:"token"`
	Datacenter string `json:"datacenter"`
	Logger     Logger `json:"-"`
}

// ServiceRegistration describes a service instance to register.
type ServiceRegistration struct {
	ID                string
	Name              string
	Address           string
	Port              int
	Tags              []string
	Meta              map[string]string
	TTL               time.Duration
	DeregisterAfter   time.Duration // 默认值：1m (1分钟)   最小值：1m (1分钟)。Consul 的清理进程每30秒运行一次
	HeartbeatInterval time.Duration
	HeartbeatNote     string
}

// ServiceInstance represents a discovered service endpoint.
type ServiceInstance struct {
	ID      string
	Name    string
	Address string
	Port    int
	Tags    []string
	Meta    map[string]string
}

// SyncErrorHandler handles background sync errors.
type SyncErrorHandler func(err error)

// WatchOptions configures service watch behavior.
type WatchOptions struct {
	Interval    time.Duration
	OnSyncError SyncErrorHandler
}

// Registry aggregates register and discover capabilities.
type Registry struct {
	*Registrar
	*Discoverer
}

// IRegistry 定义 Registry 对外能力，便于依赖倒置与单测替身注入。
type IRegistry interface {
	Register(reg ServiceRegistration) error
	Deregister(serviceID string) error
	TTLPass(serviceID, note string) error
	TTLWarn(serviceID, note string) error
	TTLFail(serviceID, note string) error

	StartSync(ctx context.Context, opts WatchOptions) error
	Discover(serviceName string) ([]ServiceInstance, error)
	DiscoverDefault(serviceName string) ([]ServiceInstance, error)
	DiscoverAll() (map[string][]ServiceInstance, error)
	ListServices() ([]string, error)
	Watch(serviceName string, onChange ServiceChangeHandler) (string, error)
	Unwatch(serviceName, watchID string)
	Shutdown()
}

// New creates a registry using supplied config.
func New(cfg Config) (*Registry, error) {
	logger := ensureLogger(cfg.Logger)
	client, err := newConsulClient(cfg, logger)
	if err != nil {
		return nil, err
	}

	registry := &Registry{}
	registry.Registrar = newRegistrar(client, logger)
	registry.Discoverer = newDiscoverer(client, logger)
	logger.Infof("Consul 注册发现组件初始化完成")
	return registry, nil
}

func (r *Registry) Register(reg ServiceRegistration) error {
	return r.Registrar.Register(reg)
}

func (r *Registry) Deregister(serviceID string) error {
	return r.Registrar.Deregister(serviceID)
}

func (r *Registry) TTLPass(serviceID, note string) error {
	return r.Registrar.TTLPass(serviceID, note)
}

func (r *Registry) TTLWarn(serviceID, note string) error {
	return r.Registrar.TTLWarn(serviceID, note)
}

func (r *Registry) TTLFail(serviceID, note string) error {
	return r.Registrar.TTLFail(serviceID, note)
}

func (r *Registry) StartSync(ctx context.Context, opts WatchOptions) error {
	return r.Discoverer.StartSync(ctx, opts)
}

func (r *Registry) Discover(serviceName string) ([]ServiceInstance, error) {
	return r.Discoverer.Discover(serviceName)
}

func (r *Registry) DiscoverDefault(serviceName string) ([]ServiceInstance, error) {
	return r.Discoverer.DiscoverDefault(serviceName)
}

func (r *Registry) DiscoverAll() (map[string][]ServiceInstance, error) {
	return r.Discoverer.DiscoverAll()
}

func (r *Registry) ListServices() ([]string, error) {
	return r.Discoverer.ListServices()
}

func (r *Registry) Watch(serviceName string, onChange ServiceChangeHandler) (string, error) {
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
func newConsulClient(cfg Config, logger Logger) (*api.Client, error) {
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
		logger.Errorf("创建 Consul 客户端失败: %v", err)
		return nil, fmt.Errorf("create consul client: %w", err)
	}
	logger.Infof("创建 Consul 客户端成功, address=%s", consulCfg.Address)
	return client, nil
}
