package chatsvr

import (
	"game-server/framework/gen"
	"game-server/internal/chatsvr/chat"
)

var _ gen.INodeBehavior = (*Behavior)(nil)

type Behavior struct {
	gen.BaseNodeBehavior
}

func (b Behavior) OnAfterStart(node gen.INode) {
	system := node.GetSystem()
	system.SpawnActor(&chat.ChatActor{}, gen.SpawnOptions{
		Name:  "chat",
		Route: chat.Router,
	})
}
