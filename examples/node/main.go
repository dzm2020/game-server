package main

import (
	"fmt"
	"game-server/examples/node/service"
	compSystem "game-server/internal/component/system"
	"log"

	"game-server/internal/iface"
	"game-server/internal/node"
)

type lifecycleHook struct{}

func (l lifecycleHook) OnInit(node iface.INode) {
	fmt.Printf("[hook] on_init node_id=%s\n", node.GetID())
}

func (l lifecycleHook) BeforeStart(node iface.INode) {
	fmt.Printf("[hook] before_start node_id=%s\n", node.GetID())

}

func (l lifecycleHook) AfterStart(node iface.INode) {
	fmt.Printf("[hook] after_start node_id=%s\n", node.GetID())

	systemComp := iface.GetComponent[*compSystem.Component]()
	systemComp.SpawnActor(&service.Actor{})
}

func (l lifecycleHook) BeforeStop(node iface.INode) {
	fmt.Printf("[hook] before_stop node_id=%s\n", node.GetID())
}

func (l lifecycleHook) AfterStop(node iface.INode, stopErr error) {
	fmt.Printf("[hook] after_stop node_id=%s err=%v\n", node.GetID(), stopErr)
}

func main() {
	n := node.New("examples/node/config.yaml")
	n.SetLifecycleHook(lifecycleHook{})

	if err := n.Startup(); err != nil {
		log.Fatalf("node startup failed: %v", err)
	}
}
