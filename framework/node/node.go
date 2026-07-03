package node

import (
	"game-server/framework/gen"
	"game-server/framework/pkg/component"
)

var _ gen.INode = (*Node)(nil)

type phase int

const (
	phaseNew phase = iota
	phaseRunning
	phaseStopped
)

// New creates a node instance.
func New(options gen.NodeOptions) *Node {
	n := &Node{
		options:  options,
		IManager: component.NewComponentsMgr(),
	}
	n.bootstrap(options)
	return n
}

type Node struct {
	options gen.NodeOptions
	component.IManager
	registry gen.IRegistry
	system   gen.ISystem
	cluster  gen.ICluster
	phase    phase
}

func (n *Node) GetId() string {
	return n.options.ID
}

func (n *Node) GetExtAddress() string {
	return n.options.ExtAddress
}

func (n *Node) GetRpcAddress() string {
	return n.options.RpcAddress
}

func (n *Node) GetName() string {
	return n.options.Name
}

func (n *Node) GetOptions() *gen.NodeOptions {
	return &n.options
}

func (n *Node) GetRegistry() gen.IRegistry {
	return n.registry
}

func (n *Node) GetSystem() gen.ISystem {
	return n.system
}

func (n *Node) GetCluster() gen.ICluster {
	return n.cluster
}

func (n *Node) AddComponents(comps ...component.IComponent) {
	if err := n.AddComponent(comps...); err != nil {
		panic(err)
	}
}

func (n *Node) SetRegistry(registry gen.IRegistry) error {
	if registry == nil {
		return gen.ErrRegistryNil
	}
	if err := guardComponentReplace(n.registry); err != nil {
		return err
	}
	n.registry = registry
	return nil
}

func (n *Node) SetCluster(cluster gen.ICluster) error {
	if cluster == nil {
		return gen.ErrClusterNil
	}
	if err := guardComponentReplace(n.cluster); err != nil {
		return err
	}
	n.cluster = cluster
	return nil
}

func (n *Node) SetSystem(system gen.ISystem) error {
	if system == nil {
		return gen.ErrActorSystemNil
	}
	if err := guardComponentReplace(n.system); err != nil {
		return err
	}
	n.system = system
	return nil
}
