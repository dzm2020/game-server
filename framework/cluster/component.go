package cluster

import (
	"context"
	"fmt"
	"game-server/framework/gen"
)

type LocalInvoker struct {
	node gen.INode
}

func (l *LocalInvoker) Handler(from *gen.PID, target *gen.PID, msg *gen.Message) error {
	system := l.node.GetSystem()
	if system == nil {
		return fmt.Errorf("system is nil")
	}
	return system.Tell(from, target, msg)
}

type ServerListGetter struct {
	node gen.INode
}

func (s *ServerListGetter) Get() (map[string][]gen.ServiceInstance, error) {
	registry := s.node.GetRegistry()
	if registry == nil {
		return nil, fmt.Errorf("registry is nil")
	}
	return registry.DiscoverAll()
}

func NewComponent(node gen.INode, cluster gen.ICluster) *Component {
	return &Component{
		node:     node,
		ICluster: cluster,
	}
}

type Component struct {
	node gen.INode
	gen.ICluster
}

func (c *Component) Init() error {
	c.ICluster.SetLocalInvoker(&LocalInvoker{node: c.node})
	c.ICluster.SetServerListGetter(&ServerListGetter{node: c.node})
	return nil
}

func (c *Component) Start(ctx context.Context) error {
	return c.ICluster.Run()
}

func (c *Component) Stop(ctx context.Context) error {
	c.ICluster.Close()
	return nil
}
