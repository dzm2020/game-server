package cluster

import (
	consulregistry "consul_registry"
	"context"
	"errors"
	"fmt"
	compDiscovery "game-server/framework/runtime/component/discovery"
	compMQ "game-server/framework/runtime/component/messagequeue"
	compSystem "game-server/framework/runtime/component/system"
	"game-server/framework/runtime/iface"
	"game-server/framework/pkg/component"

	"game-server/framework/runtime/cluster"
)

var errClusterDepsMissing = errors.New("cluster dependencies are not ready")

type Component struct {
	component.BaseComponent
	*cluster.Cluster
}

func New() *Component {
	return &Component{}
}

func (c *Component) Init() error {
	node := iface.GetCurrentNode()
	if node == nil {
		return nil
	}

	mqComp := iface.GetComponent[*compMQ.Component]()
	discoveryComp := iface.GetComponent[*compDiscovery.Component]()
	systemComp := iface.GetComponent[*compSystem.Component]()
	if mqComp == nil || mqComp.IMessageQue == nil {
		return fmt.Errorf("%w: message queue component is nil", errClusterDepsMissing)
	}
	if discoveryComp == nil || discoveryComp.Registry == nil {
		return fmt.Errorf("%w: discovery component is nil", errClusterDepsMissing)
	}
	if systemComp == nil || systemComp.ISystem == nil {
		return fmt.Errorf("%w: system component is nil", errClusterDepsMissing)
	}

	cl, err := cluster.New(context.Background(), mqComp.IMessageQue, discoveryComp.Registry, systemComp.ISystem)
	if err != nil {
		return err
	}
	c.Cluster = cl

	return nil
}

func (c *Component) Start(ctx context.Context) error {
	if c.Cluster == nil {
		return nil
	}
	return c.Cluster.Start(ctx, consulregistry.WatchOptions{})
}

func (c *Component) Stop(_ context.Context) error {
	if c.Cluster != nil {
		c.Cluster.Close()
	}
	return nil
}
