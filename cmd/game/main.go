package main

import (
	"game-server/framework/pkg/glog"
	"game-server/framework/runtime/component/gateway"
	"game-server/framework/runtime/iface"
	"game-server/framework/runtime/node"
	"game-server/internal/gamesvr/player"
)

type Behavior struct {
	iface.BaseNodeBehavior
}

func (l *Behavior) BeforeStart(n iface.INode) {
	com := gateway.New()
	com.SetClientAgentFactory(player.AgentFactory)
	_ = n.AddComponents(com)
}

func main() {
	n := node.New("./config/config.yaml")
	n.SetLifecycleHook(&Behavior{})

	if err := n.Startup(); err != nil {
		glog.Errorf("node startup failed: %v", err)
	}
}
