package gen

import (
	"fmt"
	"game-server/framework/pkg/component"
)

// INode defines node-level capabilities exposed to components.
type INode interface {
	GetId() string
	GetName() string
	GetExtAddress() string
	GetRpcAddress() string
	component.IManager
	GetOptions() *NodeOptions
	AddComponents(comps ...component.IComponent)
	GetCluster() ICluster
	GetSystem() ISystem
	GetRegistry() IRegistry
}

// INodeBehavior defines node lifecycle callbacks.
type INodeBehavior interface {
	OnInit(node INode)
	OnBeforeStart(node INode)
	OnAfterStart(node INode)
	OnBeforeStop(node INode)
	OnAfterStop(node INode, stopErr error)
	OnPanic(node INode, err error)
}

var _ INodeBehavior = (*BaseNodeBehavior)(nil)

type BaseNodeBehavior struct{}

func (b BaseNodeBehavior) OnInit(node INode) {
}

func (b BaseNodeBehavior) OnBeforeStart(node INode) {
}

func (b BaseNodeBehavior) OnAfterStart(node INode) {
}

func (b BaseNodeBehavior) OnBeforeStop(node INode) {
}

func (b BaseNodeBehavior) OnAfterStop(node INode, stopErr error) {
}

func (b BaseNodeBehavior) OnPanic(node INode, err error) {}

func RequireComponent[T component.IComponent](node INode, key T) (T, error) {
	var zero T
	if node == nil {
		return zero, ErrNodeNil
	}
	raw := node.GetComponent(key)
	if raw == nil {
		return zero, fmt.Errorf("%w: %T", ErrNodeComponentNotRegistered, key)
	}
	typed, ok := raw.(T)
	if !ok {
		return zero, fmt.Errorf("%w: key=%T actual=%T", ErrNodeComponentTypeMismatched, key, raw)
	}
	return typed, nil
}
