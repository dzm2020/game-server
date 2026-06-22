package network

import (
	"game-server/framework/grs"
	"game-server/framework/pkg/glog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// -------------------------------------------
// 连接封装（读写分离）
// -------------------------------------------

// Connection 代表一个 WebSocket 连接
type Connection struct {
	id      uint64
	conn    *websocket.Conn
	server  *WebsocketServer
	send    chan []byte // 写队列
	done    chan struct{}
	once    sync.Once
	stopped atomic.Bool
}

var _ IConnection = (*Connection)(nil)

func (c *Connection) ID() uint64 {
	if c == nil {
		return 0
	}
	return c.id
}

func (c *Connection) LocalAddr() string {
	if c == nil || c.conn == nil {
		return ""
	}
	addr := c.conn.LocalAddr()
	if addr == nil {
		return ""
	}
	return addr.String()
}

func (c *Connection) RemoteAddr() string {
	if c == nil || c.conn == nil {
		return ""
	}
	addr := c.conn.RemoteAddr()
	if addr == nil {
		return ""
	}
	return addr.String()
}

func (c *Connection) Available() bool {
	if c == nil || c.conn == nil || c.stopped.Load() {
		return false
	}
	return true
}

// Send 将业务消息打包后放入写队列（线程安全）
func (c *Connection) Send(v interface{}) error {
	codec := c.server.opts.codec
	if codec == nil {
		return ErrCodecNotConfigured
	}
	data, err := codec.Encode(v)
	if err != nil {
		return err
	}
	select {
	case c.send <- data:
	case <-c.done:
		return ErrConnectionClosed
	}
	return nil
}

func (c *Connection) callOnDisconnect(closeErr error) {
	grs.Try(func() {
		handler := c.server.opts.handler
		if handler == nil {
			return
		}
		handler.OnDisconnect(c, closeErr)
	}, nil)
}

func (c *Connection) callOnMessage(msg interface{}) {
	grs.Try(func() {
		handler := c.server.opts.handler
		if handler == nil {
			return
		}
		handler.OnMessage(c, msg)
	}, nil)
}

func (c *Connection) configureReadPump() {
	c.conn.SetReadLimit(c.server.opts.readLimit)

	// 如果长时间收不到 pong，ReadMessage() 会超时返回错误，连接被清理。
	_ = c.conn.SetReadDeadline(time.Now().Add(c.server.opts.pongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(c.server.opts.pongWait))
	})
}

func (c *Connection) runRead(closeErr *error) (shouldExit bool) {
	_, data, err := c.conn.ReadMessage()
	if err != nil {
		*closeErr = err
		if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
			glog.Error("连接读取消息失败",
				zap.Uint64("conn_id", c.ID()),
				zap.Error(err),
			)
		}
		shouldExit = true
		return
	}

	codec := c.server.opts.codec
	if codec == nil {
		return
	}

	msg, err := codec.Decode(data)
	if err != nil {
		glog.Warn("消息解码失败",
			zap.Uint64("conn_id", c.ID()),
			zap.Error(err),
		)
		return
	}

	c.callOnMessage(msg)
	return
}

// readPump 读协程：循环读取 -> 解包 -> 回调
func (c *Connection) readPump() {
	var closeErr error
	defer func() {
		c.callOnDisconnect(closeErr)
		c.Close()
		c.server.wg.Done()
	}()
	c.configureReadPump()

	for {
		shouldExit := false
		grs.Try(func() {
			shouldExit = c.runRead(&closeErr)
		}, nil)
		if shouldExit {
			return
		}
	}
}

// writePump 写协程：从队列取数据 -> 打包 -> 发送
func (c *Connection) writePump() {
	defer func() {
		c.server.wg.Done()
		c.Close()
	}()
	ticker := time.NewTicker(c.server.opts.pingPeriod)
	defer ticker.Stop()
	loop := true
	for loop {
		grs.Try(func() {
			loop = c.runWrite(ticker)
		}, nil)
	}
}

func (c *Connection) runWrite(ticker *time.Ticker) bool {
	select {
	case data, ok := <-c.send:
		if !ok {
			// 通道关闭，发送一条关闭消息后退出
			_ = c.writeFrame(websocket.CloseMessage, []byte{})
			return false
		}
		// 可以支持不同类型的消息，这里简单发送二进制帧
		if err := c.writeFrame(websocket.BinaryMessage, data); err != nil {
			glog.Error("连接写消息失败",
				zap.Uint64("conn_id", c.ID()),
				zap.Error(err),
			)
			return false
		}
	case <-ticker.C:
		if err := c.writeFrame(websocket.PingMessage, nil); err != nil {
			return false
		}
	case <-c.done:
		return false
	}
	return true
}

func (c *Connection) writeFrame(messageType int, data []byte) error {
	_ = c.conn.SetWriteDeadline(time.Now().Add(c.server.opts.writeWait))
	return c.conn.WriteMessage(messageType, data)
}

// Close 安全关闭连接
func (c *Connection) Close() {
	c.once.Do(func() {
		c.stopped.Store(true)
		close(c.done)
		// 关闭底层连接（会触发读写协程退出）
		c.conn.Close()
		c.server.removeConn(c)
	})
}
