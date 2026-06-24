package gateway

import (
	"fmt"
	"game-server/framework/gen"
	"game-server/framework/network"
	"game-server/framework/pkg/glog"
)

type eventHandler struct {
	g *gatWay
}

func newEventHandler(g *gatWay) *eventHandler {
	return &eventHandler{g: g}
}

func (h *eventHandler) OnConnect(conn network.IConnection) {
	if conn == nil {
		return
	}
	if err := h.g.bindConnection(conn); err != nil {
		glog.Errorf("gateway bind client agent failed conn_id=%d err=%v", conn.ID(), err)
		conn.Close()
		return
	}
	glog.Infof("gateway client connected conn_id=%d remote=%s", conn.ID(), conn.RemoteAddr())
}

func (h *eventHandler) OnDisconnect(conn network.IConnection, err error) {
	if conn == nil {
		return
	}
	h.g.unbindConnection(conn.ID())
	glog.Infof("gateway client disconnected conn_id=%d remote=%s err=%v", conn.ID(), conn.RemoteAddr(), err)
}

func (h *eventHandler) OnMessage(conn network.IConnection, msg interface{}) {
	switch v := msg.(type) {
	case *gen.Message:
		_ = h.g.routeInbound(conn, v)
	case []*gen.Message:
		for _, item := range v {
			_ = h.g.routeInbound(conn, item)
		}
	}
}

type wsProtocolCodec struct {
}

const protocolHeaderSize = 12

func newWSProtocolCodec() *wsProtocolCodec {
	return &wsProtocolCodec{}
}

func (c *wsProtocolCodec) Decode(data []byte) (*gen.Message, error) {
	return gen.Decode(data)
}

func (c *wsProtocolCodec) Encode(v *gen.Message) ([]byte, error) {
	switch value := v.(type) {
	case []byte:
		return value, nil
	case *gen.Message:
		return gen.Encode(value), nil
	default:
		return nil, fmt.Errorf("unsupported gateway encode payload: %T", v)
	}
}
