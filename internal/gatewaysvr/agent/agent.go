package agent

import (
	"game-server/framework/actor"
	compgateway "game-server/framework/component/gateway"
	"game-server/framework/gen"
	"game-server/framework/pkg/glog"

	"go.uber.org/zap"
)

var Route = actor.NewRoute()

func init() {
	Route.Register(1, 1, Handler, nil)
}

func Handler(ctx gen.IContext, request interface{}) error {
	glog.Info("agent receive message", zap.Any("request", request))
	//  直接回复客户端
	//agent := ctx.Actor().(*ClientAgent)
	//agent.Push(request.(*gen.Message))

	//  转发到其他节点
	pid := gen.NewPID(0, "chat", "chat1")
	_ = ctx.Tell(pid, request.(*gen.Message))
	return nil
}

func New() *ClientAgent {
	return &ClientAgent{}
}

type ClientAgent struct {
	compgateway.Agent
}

func (b *ClientAgent) OnInit(ctx gen.IContext) error {

	return b.Agent.OnInit(ctx)
}
