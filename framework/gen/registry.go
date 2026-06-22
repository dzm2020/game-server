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
	Run(ctx context.Context) error

	Register(reg ServiceInstance) error
	Deregister(serviceID string) error
	SetHealthState(serviceID, state ServiceHealthState) error

	Discover(serviceName string) ([]ServiceInstance, error)
	DiscoverAll() (map[string][]ServiceInstance, error)
	ListServices() ([]string, error)
	Watch(serviceName string, onChange ServiceChangeHandler) (string, error)
	Unwatch(serviceName, watchID string)
	Shutdown()
}
