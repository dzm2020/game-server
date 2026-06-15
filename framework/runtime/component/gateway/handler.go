package gateway

import (
	"encoding/binary"
	"errors"
	"fmt"
	"game-server/framework/runtime/protocol"
	"game-server/framework/pkg/glog"
	"game-server/framework/pkg/network"
)

type eventHandler struct {
	component *Component
}

func newEventHandler(component *Component) *eventHandler {
	return &eventHandler{component: component}
}

func (h *eventHandler) OnConnect(conn network.IConnection) {
	if conn == nil {
		return
	}
	if err := h.component.bindConnection(conn); err != nil {
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
	h.component.unbindConnection(conn.ID())
	glog.Infof("gateway client disconnected conn_id=%d remote=%s err=%v", conn.ID(), conn.RemoteAddr(), err)
}

func (h *eventHandler) OnMessage(conn network.IConnection, msg interface{}) {
	switch v := msg.(type) {
	case *protocol.Message:
		_ = h.component.routeInbound(conn, v)
	case []*protocol.Message:
		for _, item := range v {
			_ = h.component.routeInbound(conn, item)
		}
	}
}

type wsProtocolCodec struct {
	codec protocol.ICodec
}

const protocolHeaderSize = 12

func newWSProtocolCodec() *wsProtocolCodec {
	return &wsProtocolCodec{codec: protocol.NewCodec()}
}

func (c *wsProtocolCodec) Decode(data []byte) (interface{}, error) {
	msgList, err := decodeProtocolMessages(data)
	if err != nil {
		return nil, err
	}
	if len(msgList) == 0 {
		return nil, errors.New("no protocol message decoded")
	}
	if len(msgList) == 1 {
		return msgList[0], nil
	}
	return msgList, nil
}

func decodeProtocolMessages(buf []byte) ([]*protocol.Message, error) {
	if len(buf) < protocolHeaderSize {
		return nil, errors.New("protocol payload too short")
	}

	msgList := make([]*protocol.Message, 0, 1)
	for len(buf) >= protocolHeaderSize {
		l := binary.BigEndian.Uint32(buf[0:4])
		total := protocolHeaderSize + int(l)
		if len(buf) < total {
			return nil, errors.New("incomplete protocol frame")
		}

		msg := &protocol.Message{
			Head: &protocol.Head{
				Len:   l,
				Cmd:   buf[4],
				Act:   buf[5],
				Error: binary.BigEndian.Uint16(buf[6:8]),
				Index: binary.BigEndian.Uint32(buf[8:12]),
			},
		}
		msg.Data = append([]byte(nil), buf[protocolHeaderSize:total]...)
		msgList = append(msgList, msg)
		buf = buf[total:]
	}
	return msgList, nil
}

func (c *wsProtocolCodec) Encode(v interface{}) ([]byte, error) {
	switch value := v.(type) {
	case []byte:
		return value, nil
	case *protocol.Message:
		return c.codec.Encode(value), nil
	default:
		return nil, fmt.Errorf("unsupported gateway encode payload: %T", v)
	}
}
