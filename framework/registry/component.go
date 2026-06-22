package registry

import (
	"context"
	"game-server/framework/gen"
)

func NewComponent(node gen.INode, registry gen.IRegistry) *Component {
	return &Component{
		node:      node,
		IRegistry: registry,
	}
}

type Component struct {
	node gen.INode
	gen.IRegistry
}

func (c *Component) Init() error {
	return nil
}

func (c *Component) Start(ctx context.Context) error {
	reg := gen.ServiceInstance{
		ID:         c.node.GetId(),
		Name:       c.node.GetName(),
		ExtAddress: c.node.GetExtAddress(),
		RpcAddress: c.node.GetRpcAddress(),
	}
	if err := c.Register(reg); err != nil {
		return err
	}
	return c.IRegistry.Run(ctx)
}

func (c *Component) Stop(ctx context.Context) error {
	_ = c.Deregister(c.node.GetId())
	c.IRegistry.Shutdown()
	return nil
}
