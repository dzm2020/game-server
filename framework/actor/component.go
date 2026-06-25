package actor

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
	gen.ISystem
}

func (c *Component) Init() error {
	c.ISystem = NewSystemWithNodeID(c.node.GetId())
	c.ISystem.SetRemoteInvoker(&remoteInvoker{node: c.node})
	return nil
}

func (c *Component) Start(ctx context.Context) error {
	return nil
}

func (c *Component) Stop(ctx context.Context) error {
	if c.ISystem != nil {
		return c.ISystem.Stop(ctx)
	}
	return nil
}
