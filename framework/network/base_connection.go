package network

import (
	"context"
	"game-server/framework/gen"
	"game-server/framework/pkg/glog"
	"game-server/framework/pkg/netutil"
	"game-server/framework/pkg/stopper"
	"net"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type connCommon struct {
	ctx        context.Context
	serverOpts ServerOptions
	connMgr    *ConnManager
	handler    IHandler
}

func newBaseConn(common connCommon, network string, conn net.Conn, remoteAddr net.Addr) *baseConn {
	bc := &baseConn{
		id:                  genConnID(),
		network:             network,
		conn:                conn,
		remoteAddr:          remoteAddr,
		sendChan:            make(chan []byte, common.serverOpts.SendChanSize),
		connMgr:             common.connMgr,
		handler:             common.handler,
		heartIntervalSecond: common.serverOpts.HeartIntervalSecond,
	}
	bc.lastActive.Store(time.Now().Unix())
	bc.ctx, bc.cancel = context.WithCancel(common.ctx)

	glog.Info("新建网络连接", gen.FieldComponent("network.connection"), gen.FieldConnID(bc.ID()),
		zap.String("network", bc.Network()),
		zap.String("localAddr", bc.LocalAddr()),
		zap.String("remoteAddr", bc.RemoteAddr()))
	return bc
}

type baseConn struct {
	stopper.Stopper
	id                  int64
	network             string
	handler             IHandler
	conn                net.Conn
	user                interface{}
	sendChan            chan []byte
	ctx                 context.Context
	cancel              context.CancelFunc
	remoteAddr          net.Addr
	connMgr             *ConnManager
	lastActive          atomic.Int64
	heartIntervalSecond int64
}

func (b *baseConn) ID() int64 {
	return b.id
}

func (b *baseConn) Network() string {
	return b.network
}

func (b *baseConn) LocalAddr() string {
	return b.conn.LocalAddr().String()
}

func (b *baseConn) RemoteAddr() string {
	return b.remoteAddr.String()
}

func (b *baseConn) Context() interface{} {
	return b.user
}

func (b *baseConn) SetContext(ctx interface{}) {
	b.user = ctx
}

func (b *baseConn) SetReadBuffer(bytes int) error {
	return netutil.SetRcvBuffer(b.conn, bytes)
}

func (b *baseConn) SetWriteBuffer(bytes int) error {
	return netutil.SetSndBuffer(b.conn, bytes)
}

func (b *baseConn) onConnect(connection IConnection) error {
	return b.handler.OnConnect(connection)
}

func (b *baseConn) OnMessage(conn IConnection, data []byte) (int, error) {
	b.lastActive.Store(time.Now().Unix())
	return b.handler.OnMessage(conn, data)
}

func (b *baseConn) OnClose(conn IConnection, err error) {
	b.handler.OnClose(conn, err)
}

func (b *baseConn) Send(msg []byte) error {
	if b.IsStop() {
		return gen.ErrConnectionClosed
	}
	if msg == nil {
		return nil
	}
	select {
	case b.sendChan <- msg:
	default:
		glog.Warn("连接发送队列已满",
			gen.FieldComponent("network.connection"),
			gen.FieldConnID(b.id),
			zap.String("network", b.network),
			zap.Int("queueLen", len(b.sendChan)),
			zap.Int("queueCap", cap(b.sendChan)))
		return gen.ErrNetworkChannelFull
	}
	return nil
}

func (b *baseConn) heartbeat(connection IConnection) {
	ticker := time.NewTicker(time.Duration(b.heartIntervalSecond) * time.Second / 2)
	defer ticker.Stop()
	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			lastActive := b.lastActive.Load()
			if time.Now().Unix()-lastActive >= b.heartIntervalSecond {
				_ = connection.Close(gen.ErrNetworkHeartTimeout)
			}
		}
	}
}

func (b *baseConn) Close(connection IConnection, err error) {
	if !b.Stop() {
		return
	}
	b.OnClose(connection, err)
	if b.connMgr != nil {
		b.connMgr.Remove(connection)
	}
	b.cancel()
	return
}
