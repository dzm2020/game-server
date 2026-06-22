package gamesvr

import (
	"game-server/framework/gen"
	"game-server/internal/gamesvr/gamer"
)

var _ gen.INodeBehavior = (*Behavior)(nil)

type Behavior struct {
}

func (b Behavior) OnInit(node gen.INode) {

	return
}

func (b Behavior) OnBeforeStart(node gen.INode) {
	return
}

func (b Behavior) OnAfterStart(node gen.INode) {
	system := node.GetSystem()
	system.SpawnActor(&gamer.GameActor{}, gen.SpawnOptions{})
	return
}

func (b Behavior) OnBeforeStop(node gen.INode) {
	return
}

func (b Behavior) OnAfterStop(node gen.INode, stopErr error) {
	return
}
