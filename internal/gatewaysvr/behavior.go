package gatewaysvr

import (
	compgateway "game-server/framework/component/gateway"
	"game-server/framework/gen"
	"game-server/internal/gatewaysvr/agent"
)

var _ gen.INodeBehavior = (*Behavior)(nil)

type Behavior struct {
	gen.BaseNodeBehavior
}

func (b Behavior) OnBeforeInit(node gen.INode) {
	protoAddr := "ws://127.0.0.1:7000/ws"
	if ext := node.GetExtAddress(); ext != "" {
		protoAddr = "ws://" + ext + "/ws"
	}
	gatewayComp := compgateway.NewComponent(node, compgateway.Options{
		ProtoAddr: protoAddr,
		AgentFactory: func() (compgateway.IAgent, gen.SpawnOptions) {
			return agent.New(),
				gen.SpawnOptions{
					Route: agent.Route,
				}
		},
	})
	node.AddComponents(gatewayComp)
}
