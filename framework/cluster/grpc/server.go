package grpc_cluster

import (
	"context"
	"errors"
	"game-server/framework/grs"
	"game-server/framework/pkg/glog"
	"io"
	"net"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// NewNodeServer 创建服务端
func NewNodeServer(address string, dispatcher Dispatcher) *NodeServer {
	return &NodeServer{
		address:    address,
		dispatcher: dispatcher,
	}
}

// NodeServer gRPC服务端
type NodeServer struct {
	UnimplementedNodeServiceServer
	address    string
	lis        net.Listener
	server     *grpc.Server
	dispatcher Dispatcher
}

// listen
//
//	@Description: grpc集群服务
//	@receiver s
//	@return error
func (s *NodeServer) listen() error {
	//  启动服务端
	lis, err := net.Listen("tcp", s.address)
	if err != nil {
		glog.Error("grpc集群服务监听", zap.String("listen_addr", s.address))
		return err
	}
	server := grpc.NewServer(
		grpc.MaxConcurrentStreams(100000), // 支持大量并发流
		// 服务端
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second, // 如果客户端发送 PING 太频繁（低于 MinTime），服务端会返回 GOAWAY 帧并断开连接
			PermitWithoutStream: true,            // 设为 true：允许客户端在空闲时发送 Keepalive PING（更宽松）
		}),
	)
	RegisterNodeServiceServer(server, s)
	s.lis = lis
	s.server = server
	glog.Info("grpc集群服务监听", zap.String("listen_addr", s.address))
	return nil
}

// Serve
//
//	@Description: 阻塞调用
//	@receiver s
//	@return error
func (s *NodeServer) Serve() error {
	if err := s.listen(); err != nil {
		return err
	}
	timeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	var err error
	grs.SafeGo(func() {
		err = s.server.Serve(s.lis)
		cancel()
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			glog.Error("grpc集群服务端接收协程", zap.String("listen_addr", s.address), zap.Error(err))
			return
		}
		glog.Info("grpc集群服务端协程停止", zap.String("listen_addr", s.address))
	})
	select {
	case <-timeCtx.Done():
	}
	return err
}

// Stream 实现双向流
func (s *NodeServer) Stream(stream NodeService_StreamServer) error {
	glog.Info("grpc集群服务端收协程启动")
	for {
		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				return nil
			}
			glog.Error("grpc集群服务端接收", zap.Error(err))
			return err
		}
		if err = s.dispatcher.Dispatch(msg); err != nil {
			glog.Error("grpc集群服务端接收", zap.Error(err))
		}
	}
}

func (s *NodeServer) shutdown() {
	glog.Info("grpc集群服务端关闭")
	if s.lis != nil {
		_ = s.lis.Close()
	}
	if s.server != nil {
		done := make(chan struct{})
		grs.SafeGo(func() {
			s.server.GracefulStop()
			close(done)
		})
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			s.server.Stop()
		}
	}
}
