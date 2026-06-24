package chat

import (
	"game-server/framework/actor"
	"game-server/framework/component/gateway"
	"game-server/framework/gen"
	"game-server/framework/pkg/glog"

	"go.uber.org/zap"
)

var Router = actor.NewRoute()

func init() {
	Router.Register(1, 1, ChatHandler, nil)
}

func ChatHandler(ctx gen.IContext, request interface{}) error {
	glog.Info("ChatHandler", zap.Any("request", request))
	gateway.SendToClient(ctx, ctx.Sender(), request.(*gen.Message))
	return nil
}

type ChatActor struct {
	gen.BaseActor
}
