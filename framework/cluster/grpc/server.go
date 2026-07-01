package grpc

import (
	"context"
	"game-server/framework/gen"
	"game-server/framework/grs"
	"game-server/framework/pkg/glog"
	"net"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const serverComponent = "cluster.grpc.server"

// newServer 创建服务端
func newServer(address string, cluster *Cluster) *server {
	return &server{
		address: address,
		cluster: cluster,
		logger:  glog.GetLogger().With(gen.FieldComponent(serverComponent), zap.String("address", address)),
	}
}

// server gRPC服务端
type server struct {
	UnimplementedNodeServiceServer
	address  string
	listener net.Listener
	server   *grpc.Server
	cluster  *Cluster
	logger   *zap.Logger
}

func (s *server) run() error {
	s.logger.Info("Server启动")
	if err := s.listen(); err != nil {
		s.logger.Error("Server启动")
		return err
	}
	s.cluster.bgGroup.Go(func(ctx context.Context) {
		if err := s.server.Serve(s.listener); err != nil {
			if isServerStreamClosedErr(err) {
				s.logger.Info("Server退出", zap.Error(err))
				return
			}
			s.logger.Error("Server退出", zap.Error(err))
			return
		}
		s.logger.Info("Server退出")
	})
	return nil
}

// listen
//
//	@Description: grpc集群服务
//	@receiver s
//	@return error
func (s *server) listen() error {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer(
		grpc.MaxConcurrentStreams(100000), // 支持大量并发流
		// 服务端
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second, // 如果客户端发送 PING 太频繁（低于 MinTime），服务端会返回 GOAWAY 帧并断开连接
			PermitWithoutStream: true,            // 设为 true：允许客户端在空闲时发送 Keepalive PING（更宽松）
		}),
	)
	RegisterNodeServiceServer(grpcServer, s)
	s.listener = listener
	s.server = grpcServer
	return nil
}

// Stream
//
//	@Description: 实现双向流
//	@receiver s
//	@param stream
//	@return error
func (s *server) Stream(stream NodeService_StreamServer) error {
	s.logger.Info("接收Stream启动")
	for {
		msg, err := stream.Recv()
		if err != nil {
			if isServerStreamClosedErr(err) {
				s.logger.Info("接收Stream退出", gen.FieldErr(err))
				return nil
			}
			return err
		}
		grs.Try(func() {
			if err = s.cluster.Dispatch(msg); err != nil {
				s.logger.Error("接收Stream", gen.FieldErr(err))
			}
		}, nil)
	}
}

// shutdown
//
//	@Description: 关闭服务
//	@receiver s
//	@return error
func (s *server) shutdown() {
	if s.listener != nil {
		_ = s.listener.Close()
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
	s.logger.Info("Server退出")
}
