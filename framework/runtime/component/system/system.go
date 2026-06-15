package system

import (
	"actor"
	"context"
	"game-server/framework/runtime/profile"
	"game-server/framework/pkg/component"
)

type Component struct {
	component.BaseComponent
	actor.ISystem
}

func New() *Component {
	return &Component{}
}

func (c *Component) Init() error {
	cfg := profile.GetBase().Self
	c.ISystem = actor.NewSystemWithNodeID(cfg.GetID())
	return nil
}

func (c *Component) Stop(_ context.Context) error {
	if c.ISystem != nil {
		c.ISystem.Shutdown()
	}
	return nil
}
