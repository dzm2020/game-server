package gateway

import (
	"context"
	"game-server/framework/gen"
)

func NewComponent(node gen.INode) *Component {
	return &Component{
		node: node,
	}
}

type Component struct {
	node gen.INode
	*gatWay
}

func (c *Component) Init() error {
	system := c.node.GetSystem()
	if system == nil {
		return ErrSystemComponentAbsent
	}
	cfg := DefaultConfig()
	if cfg == nil {
		return ErrConfigNil
	}
	c.gatWay = newGatWay(cfg, system)
	return nil
}
func (c *Component) Start(ctx context.Context) error {
	if !c.cfg.Enable {
		return nil
	}

	return c.gatWay.Start(ctx)
}
func (c *Component) Stop(ctx context.Context) error {
	if !c.cfg.Enable {
		return nil
	}
	return c.gatWay.Stop(ctx)
}
