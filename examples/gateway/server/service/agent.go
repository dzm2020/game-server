package service

import (
	"actor"
	"fmt"
	"game-server/framework/pkg/glog"
	"game-server/framework/pkg/network"
	compcluster "game-server/framework/runtime/component/cluster"
	"game-server/framework/runtime/iface"
	"game-server/framework/runtime/protocol"
	"game-server/framework/runtime/route"
)

var router = route.NewRegistry()

func init() {
	router.RegisterAsync(1, 1, asyncHandler)

}
func asyncHandler(session route.ISession, request interface{}) {
	agent, ok := session.Actor().(*Agent)
	if !ok || agent == nil {
		return
	}
	msg, ok := request.(*protocol.Message)
	if !ok || msg == nil {
		return
	}
	_ = agent.Forward(session.Context(), "", "gateway-echo", msg)
}

var PlayerFactory = func(conn network.IConnection) (actor.Actor, error) {
	return &Player{Conn: conn}, nil
}

type Player struct {
	actor.BaseActor
	Conn network.IConnection
}

const (
	defaultSendCmd = 255
	defaultSendAct = 255
)

type agentSession struct {
	ctx   actor.Context
	agent actor.Actor
}

func (s *agentSession) Context() actor.Context {
	return s.ctx
}

func (s *agentSession) Actor() actor.Actor {
	return s.agent
}

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

	session := &agentSession{
		ctx:   ctx,
		agent: a,
	}
	asyncHandlers, _ := router.Snapshot()
	if h, ok := asyncHandlers[msg.ID()]; ok && h != nil {
		h(session, msg)
		return
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
