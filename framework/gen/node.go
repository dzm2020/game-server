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

func RequireComponent[T component.IComponent](node INode, key T) (T, error) {
	var zero T
	if node == nil {
		return zero, fmt.Errorf("node 为空")
	}
	raw := node.GetComponent(key)
	if raw == nil {
		return zero, fmt.Errorf("组件未注册: %T", key)
	}
	typed, ok := raw.(T)
	if !ok {
		return zero, fmt.Errorf("组件类型不匹配: key=%T actual=%T", key, raw)
	}
	return typed, nil
}
