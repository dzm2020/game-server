package chatsvr

import (
	"game-server/framework/gen"
	"game-server/internal/chatsvr/chat"
)

var _ gen.INodeBehavior = (*Behavior)(nil)

type Behavior struct {
	gen.BaseNodeBehavior
}

func (b Behavior) OnInit(node gen.INode) {

	return
}

func (b Behavior) OnBeforeStart(node gen.INode) {
	return
}

func (b Behavior) OnAfterStart(node gen.INode) {
	system := node.GetSystem()
	system.SpawnActor(&chat.ChatActor{}, gen.SpawnOptions{
		Name:  "chat",
		Route: chat.Router,
	})
	return
}

func (b Behavior) OnBeforeStop(node gen.INode) {
	return
}

func (b Behavior) OnAfterStop(node gen.INode, stopErr error) {
	return
}
