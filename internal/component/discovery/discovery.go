package discovery

import (
	"context"
	"game-server/internal/iface"
	"game-server/internal/profile"

	consulregistry "consul_registry"
	"game-server/pkg/component"
)

type Component struct {
	component.BaseComponent
	*consulregistry.Registry
}

func New() *Component {
	return &Component{}
}

func (c *Component) Init() error {
	cfg := profile.GetBase().Consul
	registry, err := consulregistry.New(cfg)
	if err != nil {
		return err
	}
	c.Registry = registry
	return nil
}

func (c *Component) Start(ctx context.Context) error {
	node := iface.GetCurrentNode()
	if node == nil {
		return nil
	}
	return c.Registry.StartSync(ctx, consulregistry.WatchOptions{})
}

func (c *Component) Stop(_ context.Context) error {
	if c.Registry != nil {
		c.Registry.Shutdown()
	}
	return nil
}
