package grpc

import (
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
func newServer(address string, invoker gen.ILocalInvoker) *server {
	return &server{
		address: address,
		invoker: invoker,
		logger:  glog.GetLogger().With(gen.FieldComponent(serverComponent), zap.String("address", address)),
	}
}

// server gRPC服务端
type server struct {
	UnimplementedNodeServiceServer
	invoker  gen.ILocalInvoker
	address  string
	listener net.Listener
	server   *grpc.Server
	logger   *zap.Logger
}

func (s *server) run() error {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer(
		grpc.MaxConcurrentStreams(100000), // 支持大量并发流
		// 服务端
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	RegisterNodeServiceServer(grpcServer, s)
	s.listener = listener
	s.server = grpcServer

	grs.SafeGo(func() {
		_ = s.server.Serve(s.listener)
	})
	return nil
}

// Stream
//
//	@Description: 实现双向流
//	@receiver s
//	@param stream
//	@return error
func (s *server) Stream(stream NodeService_StreamServer) error {
	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		if err = s.dispatcher(msg); err != nil {
			s.logger.Error("消息分发失败", zap.Error(err))
		}
	}
}

func (s *server) dispatcher(msg *gen.ClusterMessage) error {
	if msg == nil {
		return gen.ErrMessageNil
	}
	decodedMsg, _, err := gen.Decode(msg.Data)
	if err != nil {
		return err
	}
	if decodedMsg == nil {
		return gen.ErrClusterDecodeFailed
	}
	if err = s.invoker.Tell(msg.SourcePid, msg.TargetPid, decodedMsg); err != nil {
		return err
	}
	return nil
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
