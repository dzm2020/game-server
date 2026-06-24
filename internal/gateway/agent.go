package gateway

import (
	"fmt"
	"game-server/framework/gen"
	"game-server/framework/network"
)

type IAgent interface {
	gen.IActor
	SetConnection(connection network.IConnection)
	GetConnection() network.IConnection
	Push(msg interface{}) error
}

type Agent struct {
	gen.IActor
	connection network.IConnection
}

func (a *Agent) OnInit(ctx gen.IContext) error {
	ctx.GetRouter().Register(255, 255, func(ctx gen.IContext, request interface{}) error {
		requestMsg, _ := request.(*gen.Message)
		return a.Push(requestMsg.Data)
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
		return fmt.Errorf("invalid params")
	}
	if !a.connection.Available() {
		return fmt.Errorf("connection is unavailable")
	}
	switch msg.(type) {
	case *gen.Message:
	case []byte:
	default:
		return fmt.Errorf("invalid msg type: %T", msg)
	}
	return a.connection.Send(msg)
}

func connID(conn network.IConnection) uint64 {
	if conn == nil {
		return 0
	}
	return conn.ID()
}

func SendToClient(ctx gen.IContext, pid *gen.PID, msg *gen.Message) error {
	data := gen.Encode(msg)
	m := gen.NewMessage(255, 255, data)
	return ctx.Tell(pid, m)
}
