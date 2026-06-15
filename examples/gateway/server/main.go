package main

import (
	"actor"
	"fmt"
	"game-server/examples/gateway/server/service"
	compgateway "game-server/framework/runtime/component/gateway"
	compsystem "game-server/framework/runtime/component/system"
	"game-server/framework/runtime/iface"
	"game-server/framework/runtime/node"
	"game-server/framework/runtime/protocol"
	"log"
)

const gatewayRouteActorName = "gateway-echo"

type lifecycleHook struct{}

func (l *lifecycleHook) OnInit(n iface.INode) {
	fmt.Printf("[hook] on_init node_id=%s\n", n.GetID())
}

func (l *lifecycleHook) BeforeStart(n iface.INode) {
	fmt.Printf("[hook] before_start node_id=%s\n", n.GetID())
}

func (l *lifecycleHook) AfterStart(n iface.INode) {
	fmt.Printf("[hook] after_start node_id=%s\n", n.GetID())

	systemComp := iface.GetComponent[*compsystem.Component]()
	if systemComp == nil || systemComp.ISystem == nil {
		log.Printf("system component is not ready")
		return
	}

	_, err := systemComp.Spawn(func(ctx actor.Context) {
		msg, ok := ctx.Message().(*protocol.Message)
		if !ok || msg == nil {
			_ = ctx.Respond([]byte("bad-request"))
			return
		}
		_ = ctx.Respond([]byte("gateway-example:" + string(msg.Data)))
	}, actor.WithName(gatewayRouteActorName))
	if err != nil {
		log.Printf("spawn route actor failed: %v", err)
	}
}

func (l *lifecycleHook) BeforeStop(n iface.INode) {
	fmt.Printf("[hook] before_stop node_id=%s\n", n.GetID())
}

func (l *lifecycleHook) AfterStop(n iface.INode, stopErr error) {
	fmt.Printf("[hook] after_stop node_id=%s err=%v\n", n.GetID(), stopErr)
}

func main() {
	compgateway.SetDefaultClientAgentFactory(service.AgentFactory)

	n := node.New("examples/gateway/config.yaml")
	n.SetLifecycleHook(&lifecycleHook{})

	if err := n.Startup(); err != nil {
		log.Fatalf("node startup failed: %v", err)
	}
}
