package gateway

import (
	"fmt"
	"game-server/framework/gen"
	"game-server/framework/network"
	"game-server/framework/pkg/glog"

	"go.uber.org/zap"
)

type IAgent interface {
	gen.IActor
	SetConnection(connection network.IConnection)
	GetConnection() network.IConnection
	Push(msg interface{}) error
}

type Agent struct {
	gen.BaseActor
	connection network.IConnection
}

const (
	DefaultPushCmd = 255
	DefaultPushAct = 255
)

func (a *Agent) OnInit(ctx gen.IContext) error {
	ctx.GetRouter().Register(DefaultPushCmd, DefaultPushAct, func(ctx gen.IContext, request interface{}) error {
		switch req := request.(type) {
		case *gen.Message:
			return a.Push(req.Data)
		case []byte:
			return a.Push(req)
		default:
			return ErrInvalidMessageType
		}
	}, nil)
	return nil
}

func (a *Agent) GetConnection() network.IConnection {
	return a.connection
}

func (a *Agent) SetConnection(connection network.IConnection) {
	a.connection = connection
}

func (a *Agent) Push(msg interface{}) error {
	if a == nil || a.connection == nil || msg == nil {
		glog.Error("网关Actor下行推送参数非法",
			gen.FieldComponent("gateway.agent"),
			zap.Bool("agent_nil", a == nil),
			zap.Bool("message_nil", msg == nil),
			gen.FieldErr(ErrInvalidPushParams))
		return ErrInvalidPushParams
	}
	if a.connection.IsStop() {
		glog.Error("网关Actor下行推送连接不可用",
			gen.FieldComponent("gateway.agent"),
			gen.FieldConnID(a.connection.ID()),
			gen.FieldErr(ErrConnectionUnavailable))
		return ErrConnectionUnavailable
	}
	switch v := msg.(type) {
	case *gen.Message:
		data, err := gen.Encode(v)
		if err != nil {
			return err
		}
		if err := a.connection.Send(data); err != nil {
			return err
		}

		return nil
	case []byte:
		if err := a.connection.Send(v); err != nil {
			return err
		}
		return nil
	default:

		glog.Error("网关Actor下行推送消息类型非法",
			gen.FieldComponent("gateway.agent"),
			zap.String("message_type", fmt.Sprintf("%T", msg)),
			gen.FieldErr(ErrInvalidMessageType))
		return ErrInvalidMessageType
	}
}

func SendToClient(ctx gen.IContext, agentPid *gen.PID, msg *gen.Message) error {
	data, err := gen.Encode(msg)
	if err != nil {
		return err
	}
	m := gen.NewMessage(255, 255, data)
	return ctx.Tell(agentPid, m)
}
