package actor

import (
	"context"
	"game-server/framework/gen"
	"time"
)

type RemoteInvoker struct {
	node gen.INode
}

func (r *RemoteInvoker) Tell(from *gen.PID, target *gen.PID, msg *gen.Message) error {
	cluster := r.node.GetCluster()
	if cluster == nil {
		return gen.ErrActorClusterNil
	}
	return cluster.SendToNode(from, target, msg)
}
func (r *RemoteInvoker) Ask(from *gen.PID, target any, msg *gen.Message, timeout time.Duration) ([]byte, error) {
	return nil, gen.ErrActorClusterAskNotImpl
}

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
	c.ISystem.SetRemoteInvoker(&RemoteInvoker{node: c.node})
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
