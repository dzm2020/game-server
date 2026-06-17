package cluster

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
func NewNodeServer(address string, dispatcher IDispatcher) *NodeServer {
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
	dispatcher IDispatcher
}

func (s *NodeServer) listen() error {
	//  启动服务端
	lis, err := net.Listen("tcp", s.address)
	if err != nil {
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
	glog.Info("节点服务启动中",
		zap.String("listen_addr", s.address),
	)
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
	err := s.server.Serve(s.lis)
	if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return err
	}
	glog.Info("节点服务已停止",
		zap.String("listen_addr", s.address),
	)
	return nil
}

// Stream 实现双向流
func (s *NodeServer) Stream(stream NodeService_StreamServer) error {
	glog.Info("节点流连接已建立")

	// 接收协程
	for {
		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				glog.Info("节点流连接已断开")
				return nil
			}
			glog.Error("节点流接收失败", zap.Error(err))
			return err
		}
		if err = s.dispatcher.Handler(msg); err != nil {
			glog.Error("节点流分发失败", zap.Error(err))
		}
	}
}

func (s *NodeServer) shutdown() {
	glog.Info("节点服务停止")
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
