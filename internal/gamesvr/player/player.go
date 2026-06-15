package player

import (
	"actor"
	"fmt"
	"game-server/framework/pkg/glog"
	"game-server/framework/pkg/network"
	compcluster "game-server/framework/runtime/component/cluster"
	"game-server/framework/runtime/iface"
	"game-server/framework/runtime/protocol"
	"game-server/framework/runtime/route"
	"runtime"
)

var router = route.NewRegistry()

func init() {
	router.Register(1, 1, defaultHandler, nil)

}
func defaultHandler(session route.ISession, request interface{}) error {
	agent, ok := session.Actor().(*Agent)
	if !ok || agent == nil {
		return
	}
	msg, ok := request.(*protocol.Message)
	if !ok || msg == nil {
		return
	}
	return agent.Forward(session.Context(), "", "gateway-echo", msg)
}

var AgentFactory = func(conn network.IConnection) (actor.Actor, error) {
	return &Agent{Conn: conn}, nil
}

type Agent struct {
	actor.BaseActor
	Conn network.IConnection
}

const (
	defaultSendCmd = 255
	defaultSendAct = 255
)

func (a *Agent) OnMessage(ctx actor.Context) {
	msg, ok := ctx.Message().(*protocol.Message)
	if !ok || msg == nil || msg.Head == nil {
		return
	}

	// Default message 255/255 means pushing payload to client.
	if msg.Cmd == defaultSendCmd && msg.Act == defaultSendAct {
		if err := a.SendToClient(msg); err != nil {
			glog.Errorf("agent default send failed: %v", err)
		}
		return
	}

	if err := router.Handle(a, ctx, msg); err != nil {
		glog.Errorf("agent handle failed: %v", err)
	}

}

func (a *Agent) Forward(ctx actor.Context, targetNodeID string, targetActorName string, msg *protocol.Message) error {
	if targetNodeID == "" || ctx.Self().NodeID == targetNodeID {
		return ctx.Tell(targetActorName, msg)
	} else {
		clusterComp := iface.GetComponent[*compcluster.Component]()
		targetPID := actor.NewPID(0, targetActorName, targetNodeID)
		return clusterComp.SendToPID(ctx.Self(), targetPID, msg)
	}
}

func (a *Agent) SendToClient(msg *protocol.Message) error {
	if !a.Conn.Available() {
		return fmt.Errorf("client connection is not available")
	}
	if msg == nil {
		return fmt.Errorf("client message is nil")
	}
	return a.Conn.Send(msg)
}
