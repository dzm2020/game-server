package registry

import (
	"context"
	"game-server/framework/registry/define"
)

// IRegistry 定义 Registry 对外能力，便于依赖倒置与单测替身注入。
type IRegistry interface {
	Run(ctx context.Context) error

	Register(reg define.ServiceInstance) error
	Deregister(serviceID string) error
	SetHealthState(serviceID, state define.HealthState) error

	Discover(serviceName string) ([]define.ServiceInstance, error)
	DiscoverAll() (map[string][]define.ServiceInstance, error)
	ListServices() ([]string, error)
	Watch(serviceName string, onChange define.ServiceChangeHandler) (string, error)
	Unwatch(serviceName, watchID string)
	Shutdown()
}
