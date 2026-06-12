package iface

import (
	"game-server/pkg/component"
)

var currentNode INode

func SetCurrentNode(node INode) {
	currentNode = node
}

func GetCurrentNode() INode {
	return currentNode
}

func GetComponent[T component.IComponent]() T {
	var t T
	if currentNode == nil {
		return t
	}
	if com := currentNode.GetComponent(t); com != nil {
		return com.(T)
	}
	return t
}

// INode 节点对外能力。
type INode interface {
	IMember
	component.IManager
}

// INodeLifecycleHook 定义 Node 生命周期回调。
type INodeLifecycleHook interface {
	OnInit(node INode)
	BeforeStart(node INode)
	AfterStart(node INode)
	BeforeStop(node INode)
	AfterStop(node INode, stopErr error)
}

// Member 节点基础信息，由 Profile 组件从配置加载。
type Member struct {
	ID      string            `mapstructure:"id" yaml:"id"`
	Name    string            `mapstructure:"name" yaml:"name"`
	Address string            `mapstructure:"address" yaml:"address"`
	Port    int               `mapstructure:"port" yaml:"port"`
	Meta    map[string]string `mapstructure:"meta" yaml:"meta"`
}

func (m *Member) GetID() string {
	if m == nil {
		return ""
	}
	return m.ID
}

type IMember interface {
	GetID() string
}
