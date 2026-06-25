package gen

import "context"

type Topology struct {
	All     []ServiceInstance
	Added   []ServiceInstance
	Updated []ServiceInstance
	Removed []ServiceInstance
}

type ServiceChangeHandler func(topology *Topology)

// ServiceInstance represents a discovered service endpoint.
type ServiceInstance struct {
	ID         string
	Name       string
	ExtAddress string // 对外地址
	RpcAddress string // 对内地址
	Tags       []string
	Meta       map[string]string
}

func (s *ServiceInstance) GetID() string {
	return s.ID
}
func (s *ServiceInstance) GetName() string {
	return s.Name
}

func (s *ServiceInstance) GetRpcAddress() string {
	return s.RpcAddress
}
func (s *ServiceInstance) GetExtAddress() string {
	return s.ExtAddress
}

type ServiceHealthState = string

const (
	ServiceHealthStatePassing  = ServiceHealthState("passing")
	ServiceHealthStateWarning  = ServiceHealthState("warning")
	ServiceHealthStateCritical = ServiceHealthState("critical")
)

// IRegistry 定义 Registry 对外能力，便于依赖倒置与单测替身注入。
type IRegistry interface {
	IDiscovery
	IRegistrar

	// Run 启动服务发现后台任务。
	// 约定：应快速返回（非长期阻塞）；重复调用应尽量幂等。
	Run(ctx context.Context) error

	Stop(ctx context.Context) error
}

type IRegistrar interface {
	Register(reg ServiceInstance) error
	Deregister(serviceID string) error
	SetHealthState(serviceID, state ServiceHealthState) error
}

type IDiscovery interface {
	GetInstance(serverID string) (ServiceInstance, bool)
	Discover(serviceName string) []ServiceInstance
	DiscoverAll() map[string][]ServiceInstance
	ListServices() []string
	Watch(serviceName string, onChange ServiceChangeHandler) (string, error)
	Unwatch(serviceName, watchID string)
}
