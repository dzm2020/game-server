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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

const serverComponent = "cluster.grpc.server"

// NewNodeServer 创建服务端
func NewNodeServer(address string, cluster *Cluster) *NodeServer {
	return &NodeServer{
		address: address,
		cluster: cluster,
		logger:  glog.GetLogger().With(glog.Component(serverComponent), zap.String("address", address)),
	}
}

// NodeServer gRPC服务端
type NodeServer struct {
	UnimplementedNodeServiceServer
	address string
	lis     net.Listener
	server  *grpc.Server
	cluster *Cluster
	logger  *zap.Logger
}

func (s *NodeServer) run() error {

	s.logger.Info("Server启动")

	if err := s.listen(); err != nil {
		s.logger.Error("Server启动")
		return err
	}

	s.cluster.bgGroup.Go(func(ctx context.Context) {
		if err := s.server.Serve(s.lis); err != nil {
			if isServerStreamClosedErr(err) {
				s.logger.Info("Server退出", zap.Error(err))
				return
			}
			s.logger.Error("Server退出", zap.Error(err))
			return
		} else {
			s.logger.Info("Server退出")
		}
	})
	return nil
}

// listen
//
//	@Description: grpc集群服务
//	@receiver s
//	@return error
func (s *NodeServer) listen() error {
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
	return nil
}

// Stream
//
//	@Description: 实现双向流
//	@receiver s
//	@param stream
//	@return error
func (s *NodeServer) Stream(stream NodeService_StreamServer) error {
	s.logger.Info("接收Stream启动")
	for {
		msg, err := stream.Recv()
		if err != nil {
			if isServerStreamClosedErr(err) {
				s.logger.Info("接收Stream退出", glog.Err(err))
				return nil
			}
			return err
		}
		grs.Try(func() {
			if err = s.cluster.Dispatch(msg); err != nil {
				s.logger.Error("接收Stream", glog.Err(err))
			}
		}, nil)
	}
}

func isServerStreamClosedErr(err error) bool {
	return err == io.EOF || errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled
}

// shutdown
//
//	@Description: 关闭服务
//	@receiver s
//	@return error
func (s *NodeServer) shutdown() {
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
	s.logger.Info("Server退出")
	return
}
