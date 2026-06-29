package network

import (
	"context"
	"errors"
	"game-server/framework/gen"
	"game-server/framework/grs"
	"game-server/framework/obs"
	"game-server/framework/pkg/glog"
	"game-server/framework/pkg/netutil"
	"net"

	"go.uber.org/zap"
)

func NewTCPServer(base *baseServer) *TCPServer {
	return &TCPServer{
		baseServer: base,
	}
}

type TCPServer struct {
	*baseServer
	listener net.Listener
}

const tcpServerComponent = "network.tcp.server"

func (s *TCPServer) Start() (err error) {
	config := netutil.ListenConfig{
		ReuseAddr: s.serverOpts.ReuseAddr,
		ReusePort: s.serverOpts.ReusePort,
	}
	if s.listener, err = config.Listen(s.ctx, s.network, s.address); err != nil {
		return
	}

	s.runGroup.Go(func(ctx context.Context) {
		s.acceptLoop()
	})
	return
}

func (s *TCPServer) acceptLoop() {
	for !s.IsStop() {
		grs.Try(func() {
			s.accept()
		}, nil)
	}
}

func (s *TCPServer) accept() {
	conn, err := s.listener.Accept()
	if err != nil {
		if !errors.Is(err, net.ErrClosed) {
			glog.Error("TCP服务器退出ACCEPT协程", gen.FieldComponent(tcpServerComponent), zap.String("address", s.Addr()), gen.FieldErr(err))
		}
		return
	}
	obs.Inc("network.tcp_server_accept_total")
	s.newTcpCon(conn)
}

func (s *TCPServer) newTcpCon(conn net.Conn) {
	tcpCon, ok := conn.(*net.TCPConn)
	if !ok {
		glog.Error("连接类型错误，期望 *net.TCPConn", gen.FieldComponent(tcpServerComponent), zap.String("address", s.Addr()))
		if closeErr := conn.Close(); closeErr != nil {
			glog.Error("关闭非 TCPConn 时出错", gen.FieldComponent(tcpServerComponent), gen.FieldErr(closeErr))
		}
		return
	}
	connection := newTCPConnection(s.connCommon(), tcpCon)

	s.connMgr.Add(connection)

	s.runGroup.Go(func(ctx context.Context) {
		connection.readLoop()
		glog.Info("TCP连接读协程关闭", gen.FieldComponent(tcpServerComponent), gen.FieldConnID(connection.ID()))
	})

	s.runGroup.Go(func(ctx context.Context) {
		connection.writeLoop()
		glog.Info("TCP连接写协程关闭", gen.FieldComponent(tcpServerComponent), gen.FieldConnID(connection.ID()))
	})

	s.runGroup.Go(func(ctx context.Context) {
		connection.heartbeat(connection)
	})
}

func (s *TCPServer) Shutdown(ctx context.Context) {
	if !s.Stop() {
		return
	}
	if err := s.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		glog.Error("关闭 TCP listener 时出错", gen.FieldComponent(tcpServerComponent), zap.String("address", s.Addr()), gen.FieldErr(err))
	}
	s.baseServer.Shutdown(ctx)
	glog.Info("TCP服务器关闭", gen.FieldComponent(tcpServerComponent), zap.String("address", s.Addr()))
	return
}
