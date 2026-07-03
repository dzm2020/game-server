package gamesvr

import (
	"game-server/framework/gen"
	"game-server/internal/gamesvr/gamer"
)

var _ gen.INodeBehavior = (*Behavior)(nil)

type Behavior struct {
	gen.BaseNodeBehavior
}

func (b Behavior) OnAfterStart(node gen.INode) {
	system := node.GetSystem()
	system.SpawnActor(&gamer.GameActor{}, gen.SpawnOptions{})
}
