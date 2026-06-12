package gateway

import (
	"actor"
	"errors"
	"fmt"
	"game-server/internal/protocol"
	"game-server/pkg/network"
	"sync"
	"time"
)

const gatewayInternalErrCode = 1

type connectionRouter struct {
	actor.BaseHandler
	component *Component
	conn      network.IConnection
	state     SessionState
	handlers  map[uint16]routeHandler
}

func newConnectionRouter(component *Component, conn network.IConnection) *connectionRouter {
	r := &connectionRouter{
		component: component,
		conn:      conn,
		handlers:  make(map[uint16]routeHandler),
	}
	r.state = SessionState{
		ConnID:      conn.ID(),
		RemoteAddr:  conn.RemoteAddr(),
		ConnectedAt: time.Now(),
		LastActive:  time.Now(),
		Meta:        make(map[string]string),
		pending:     make(map[uint32]SessionCallback),
	}
	return r
}

func (r *connectionRouter) OnMessage(ctx actor.Context) {
	msg, ok := ctx.Message().(*protocol.Message)
	if !ok || msg == nil || msg.Head == nil || r.conn == nil {
		return
	}
	r.state.LastActive = time.Now()

	if msg.Index != 0 && r.invokePendingCallback(msg) {
		return
	}

	if h, ok := r.handlers[msg.ID()]; ok {
		h(ctx, r.conn, msg)
		return
	}

	targetActor := r.component.cfg.RouteActorName
	if targetActor == "" {
		r.replyError(r.conn, msg, errors.New("gateway route actor is empty"))
		return
	}

	targetNodeID := r.component.cfg.RouteNodeID
	if targetNodeID == "" {
		targetNodeID = ctx.Self().NodeID
	}

	if targetNodeID == "" || targetNodeID == ctx.Self().NodeID {
		r.handleLocalRoute(ctx, r.conn, msg, targetActor)
		return
	}
	r.handleRemoteRoute(ctx, r.conn, msg, targetNodeID, targetActor)
}

func (r *connectionRouter) handleLocalRoute(ctx actor.Context, conn network.IConnection, msg *protocol.Message, targetActorName string) {
	if msg.Index == 0 {
		if err := ctx.Tell(targetActorName, msg); err != nil {
			r.replyError(conn, msg, err)
		}
		return
	}

	resp, err := ctx.System().Ask(ctx.Self(), targetActorName, msg, r.component.cfg.RequestTimeout)
	if err != nil {
		r.replyError(conn, msg, err)
		return
	}

	data, ok := resp.([]byte)
	if !ok {
		r.replyError(conn, msg, fmt.Errorf("unexpected local route response type: %T", resp))
		return
	}
	r.replyData(conn, msg, data)
}

func (r *connectionRouter) handleRemoteRoute(ctx actor.Context, conn network.IConnection, msg *protocol.Message, targetNodeID string, targetActorName string) {
	if r.component.cluster == nil {
		r.replyError(conn, msg, errors.New("cluster is not ready"))
		return
	}

	target := actor.NewPID(0, targetActorName, targetNodeID)
	if msg.Index == 0 {
		if err := r.component.cluster.SendToPID(ctx.Self(), target, msg); err != nil {
			r.replyError(conn, msg, err)
		}
		return
	}

	data, err := r.component.cluster.RequestToPID(ctx.Self(), target, msg, r.component.cfg.RequestTimeout)
	if err != nil {
		r.replyError(conn, msg, err)
		return
	}
	r.replyData(conn, msg, data)
}

func (r *connectionRouter) replyData(conn network.IConnection, src *protocol.Message, data []byte) {
	if conn == nil || src == nil || src.Head == nil {
		return
	}
	reply := protocol.NewMessage(src.Cmd, src.Act, data)
	reply.Copy(src)
	_ = conn.Send(reply)
}

func (r *connectionRouter) replyError(conn network.IConnection, src *protocol.Message, err error) {
	if conn == nil || src == nil || src.Head == nil {
		return
	}
	reply := protocol.NewErr(src.Cmd, src.Act, gatewayInternalErrCode)
	reply.Copy(src)
	reply.Data = []byte(err.Error())
	_ = conn.Send(reply)
}

type routeHandler func(ctx actor.Context, conn network.IConnection, msg *protocol.Message)

type SessionCallback func(msg *protocol.Message)

type SessionState struct {
	UserID      string
	ConnID      uint64
	RemoteAddr  string
	ConnectedAt time.Time
	LastActive  time.Time
	Meta        map[string]string

	mu      sync.Mutex
	pending map[uint32]SessionCallback
}

func (r *connectionRouter) registerPendingCallback(index uint32, cb SessionCallback) {
	if index == 0 || cb == nil {
		return
	}
	r.state.mu.Lock()
	r.state.pending[index] = cb
	r.state.mu.Unlock()
}

func (r *connectionRouter) invokePendingCallback(msg *protocol.Message) bool {
	if msg == nil || msg.Head == nil || msg.Index == 0 {
		return false
	}
	r.state.mu.Lock()
	cb, ok := r.state.pending[msg.Index]
	if ok {
		delete(r.state.pending, msg.Index)
	}
	r.state.mu.Unlock()
	if ok && cb != nil {
		cb(msg)
		return true
	}
	return false
}

func (r *connectionRouter) OnDestroy(actor.Context) {
	r.state.mu.Lock()
	r.state.pending = make(map[uint32]SessionCallback)
	r.state.mu.Unlock()
}
