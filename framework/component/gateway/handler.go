package gateway

import (
	"game-server/framework/gen"
	"game-server/framework/network"
	"game-server/framework/pkg/glog"

	"go.uber.org/zap"
)

type eventHandler struct {
	g *gatWay
}

func newEventHandler(g *gatWay) *eventHandler {
	return &eventHandler{g: g}
}

func (h *eventHandler) OnConnect(conn network.IConnection) error {
	if conn == nil {
		return nil
	}
	if err := h.g.bindConnection(conn); err != nil {
		glog.Error("网关绑定客户端Actor失败",
			zap.Int64("conn_id", conn.ID()),
			zap.Error(err))
		_ = conn.Close(err)
		return err
	}
	glog.Info("网关客户端连接建立",
		zap.Int64("conn_id", conn.ID()),
		zap.String("remote", conn.RemoteAddr()))
	return nil
}

func (h *eventHandler) OnClose(conn network.IConnection, err error) {
	if conn == nil {
		return
	}
	h.g.unbindConnection(conn.ID())
	glog.Info("网关客户端连接断开",
		zap.Int64("conn_id", conn.ID()),
		zap.String("remote", conn.RemoteAddr()),
		zap.Error(err))
}

func (h *eventHandler) OnMessage(conn network.IConnection, data []byte) (int, error) {
	consumed := 0
	for consumed < len(data) {
		msg, n, err := gen.Decode(data[consumed:])
		if err != nil {
			glog.Error("网关解码消息失败", zap.Error(err))
			return consumed, err
		}
		if n == 0 {
			return consumed, nil
		}
		if err := h.g.routeInbound(conn, msg); err != nil {
			return consumed, err
		}
		consumed += n
	}
	return consumed, nil
}
