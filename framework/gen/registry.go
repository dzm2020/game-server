package gen

import (
	"game-server/framework/pkg/component"
)

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

type IRegistry interface {
	component.IComponent
	IDiscovery
	IRegistrar
}

type IRegistrar interface {
	Register(reg ServiceInstance) error
	Deregister() error
	SetHealthState(state ServiceHealthState) error
}

type IDiscovery interface {
	Discover(service string) []ServiceInstance
}
