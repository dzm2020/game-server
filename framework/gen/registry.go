package gen

import (
	"context"
	"game-server/framework/pkg/component"
)

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
//
// 生命周期命名冻结约定：
// - Start：启动长期运行组件；要求幂等，且不得长期阻塞。
// - Stop：优雅停止长期运行组件；要求幂等，并阻塞等待停止完成或 ctx.Done。
// - Shutdown：仅用于“服务端入口”级别优雅退出，语义等同 Stop。
// - Close：仅用于“单个资源句柄”关闭，调用后资源不可继续使用。
type IRegistry interface {
	component.IComponent
	IDiscovery
	IRegistrar
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
