package gateway

import (
	"context"
	"game-server/framework/gen"
)

func NewComponent(node gen.INode, options Options) *Component {
	return &Component{
		node:    node,
		options: normalization(options),
	}
}

type Component struct {
	node    gen.INode
	options Options
	*gatWay
}

func (c *Component) Init() error {
	system := c.node.GetSystem()
	if system == nil {
		return ErrSystemComponentAbsent
	}
	if err := validate(c.options); err != nil {
		return err
	}
	c.gatWay = newGatWay(c.options, system)
	return c.gatWay.Init()
}

func (c *Component) Start(ctx context.Context) error {
	if c.gatWay == nil {
		return ErrComponentNotInited
	}
	return c.gatWay.Start(ctx)
}

func (c *Component) Stop(ctx context.Context) error {
	if c.gatWay == nil {
		return nil
	}
	return c.gatWay.Stop(ctx)
}
