package network

import (
	"errors"
	"game-server/framework/gen"
	"game-server/framework/obs"
	"game-server/framework/pkg/glog"
	"time"

	"github.com/gorilla/websocket"
)

type WebSocketConnection struct {
	*baseConn
	conn *websocket.Conn
}

func newWebSocketConnection(common connCommon, conn *websocket.Conn) *WebSocketConnection {
	base := newBaseConn(common, "ws", conn.NetConn(), conn.RemoteAddr())
	return &WebSocketConnection{
		baseConn: base,
		conn:     conn,
	}
}

func (c *WebSocketConnection) readLoop() {
	var err error
	defer func() {
		_ = c.Close(err)
	}()

	if err = c.onConnect(c); err != nil {
		return
	}

	for !c.IsStop() {
		messageType, data, w := c.conn.ReadMessage()
		if w != nil {
			err = w
			return
		}
		if messageType != websocket.TextMessage && messageType != websocket.BinaryMessage {
			continue
		}
		_, err = c.OnMessage(c, data)
		if err != nil {
			return
		}
	}
}

func (c *WebSocketConnection) writeLoop() {
	var err error
	defer func() {
		_ = c.Close(err)
	}()

	for !c.IsStop() {
		select {
		case <-c.ctx.Done():
			return
		case msg, ok := <-c.sendChan:
			if !ok {
				return
			}
			_, err = c.Write(msg)
			if err != nil {
				return
			}
		}
	}
}

func (c *WebSocketConnection) Write(p []byte) (n int, err error) {
	err = c.conn.WriteMessage(websocket.BinaryMessage, p)
	return len(p), err
}

func (c *WebSocketConnection) Close(err error) (w error) {
	if !c.Stop() {
		return gen.ErrConnectionClosed
	}

	timeout := time.Now().Add(1 * time.Second)
	if err := c.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), timeout); err != nil && !errors.Is(err, websocket.ErrCloseSent) {
		obs.Inc("network.websocket_close_error_total")
		glog.Error("WebSocket WriteControl 关闭帧失败", gen.FieldComponent("network.websocket"), gen.FieldConnID(c.ID()), gen.FieldErr(err))
	}
	if err := c.conn.Close(); err != nil {
		obs.Inc("network.websocket_close_error_total")
		glog.Error("WebSocket conn.Close 失败", gen.FieldComponent("network.websocket"), gen.FieldConnID(c.ID()), gen.FieldErr(err))
	}

	c.baseConn.Close(c, err)

	obs.Inc("network.websocket_close_total")
	glog.Info("WebSocket连接断开", gen.FieldComponent("network.websocket"), gen.FieldConnID(c.ID()), gen.FieldErr(err))
	return
}
