package iface

import (
	"game-server/framework/pkg/component"
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

// INode defines node-level capabilities exposed to components.
type INode interface {
	IMember
	component.IManager
	AddComponents(comps ...component.IComponent) error
}

// INodeBehavior defines node lifecycle callbacks.
type INodeBehavior interface {
	OnInit(node INode)
	BeforeStart(node INode)
	AfterStart(node INode)
	BeforeStop(node INode)
	AfterStop(node INode, stopErr error)
}

var _ INodeBehavior = (*BaseNodeBehavior)(nil)

type BaseNodeBehavior struct{}

func (b BaseNodeBehavior) OnInit(node INode) {
}

func (b BaseNodeBehavior) BeforeStart(node INode) {
}

func (b BaseNodeBehavior) AfterStart(node INode) {
}

func (b BaseNodeBehavior) BeforeStop(node INode) {
}

func (b BaseNodeBehavior) AfterStop(node INode, stopErr error) {
}

// Member contains node identity and network metadata.
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
