package main

import (
	"actor"
	"fmt"
	compsystem "game-server/framework/runtime/component/system"
	"game-server/framework/runtime/iface"
	"game-server/framework/runtime/node"
	"game-server/framework/runtime/route"
	"log"
)

type actor1 struct {
	actor.BaseActor
}

type actor2 struct {
	actor.BaseActor
}

func Hello(session route.ISession, request interface{}) {

}

type lifecycleHook struct{}

func (l *lifecycleHook) OnInit(node iface.INode) {
	fmt.Printf("[hook] on_init node_id=%s\n", node.GetID())
}

func (l *lifecycleHook) BeforeStart(node iface.INode) {
	fmt.Printf("[hook] before_start node_id=%s\n", node.GetID())
}

func (l *lifecycleHook) AfterStart(node iface.INode) {
	fmt.Printf("[hook] after_start node_id=%s\n", node.GetID())

	systemComp := iface.GetComponent[*compsystem.Component]()

	pid1, _ := systemComp.SpawnActor(&actor1{})
	pid2, _ := systemComp.SpawnActor(&actor2{})

}

func (l *lifecycleHook) BeforeStop(node iface.INode) {
	fmt.Printf("[hook] before_stop node_id=%s\n", node.GetID())
}

func (l *lifecycleHook) AfterStop(node iface.INode, stopErr error) {
	fmt.Printf("[hook] after_stop node_id=%s err=%v\n", node.GetID(), stopErr)
}

func main() {
	n := node.New("examples/cluster_actor/config.yaml")
	n.SetLifecycleHook(&lifecycleHook{})

	if err := n.Startup(); err != nil {
		log.Fatalf("node startup failed: %v", err)
	}
}
