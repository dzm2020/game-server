package main

import (
	"fmt"
	"log"
	"time"

	"actor"
	compcluster "game-server/internal/component/cluster"
	compsystem "game-server/internal/component/system"
	"game-server/internal/iface"
	"game-server/internal/node"
	"game-server/internal/protocol"
)

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
	clusterComp := iface.GetComponent[*compcluster.Component]()
	if systemComp == nil || systemComp.ISystem == nil || clusterComp == nil || clusterComp.Cluster == nil {
		log.Printf("component not ready, system=%v cluster=%v", systemComp != nil, clusterComp != nil)
		return
	}

	_, err := systemComp.Spawn(func(ctx actor.Context) {
		msg, ok := ctx.Message().(*protocol.Message)
		if !ok || msg == nil {
			_ = ctx.Respond([]byte("bad-request"))
			return
		}
		_ = ctx.Respond([]byte("pong"))
	}, actor.WithName("echo"))
	if err != nil {
		log.Printf("spawn echo actor failed: %v", err)
		return
	}

	sourcePID := actor.NewPID(0, "caller", "caller-node")
	targetPID := actor.NewPID(0, "echo", node.GetID())
	reply, err := clusterComp.RequestToPID(sourcePID, targetPID, protocol.NewMessage(1, 2, []byte("ping")), 3*time.Second)
	if err != nil {
		log.Printf("cluster request failed: %v", err)
		return
	}
	log.Printf("cluster actor reply: %s", string(reply))
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
