package grpc_cluster

import (
	"context"
	"errors"
	"game-server/framework/grs"
	"game-server/framework/pkg/glog"
	"io"
	"net"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// NewNodeServer 创建服务端
func NewNodeServer(address string, cluster *Cluster) *NodeServer {
	return &NodeServer{
		address:    address,
		cluster:    cluster,
		serveGroup: cluster.bgGroup,
	}
}

// NodeServer gRPC服务端
type NodeServer struct {
	UnimplementedNodeServiceServer
	address    string
	lis        net.Listener
	server     *grpc.Server
	cluster    *Cluster
	serveGroup *grs.Group
}

func (s *NodeServer) run() error {
	logger := s.cluster.logger
	logger.Info("Starting gRPC Server", zap.String("address", s.address))

	if err := s.listen(); err != nil {
		logger.Error("Starting gRPC Server", zap.String("address", s.address))
		return err
	}
	s.serveGroup.Go(func(ctx context.Context) {
		if err := s.server.Serve(s.lis); err != nil {
			if s.isNodeServerStopErr(err) {
				logger.Info("GrpcServer.Serve", zap.Error(err))
				return
			}
			logger.Error("GrpcServer.Serve", zap.Error(err))
			return
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

// Stream 实现双向流
func (s *NodeServer) Stream(stream NodeService_StreamServer) error {
	logger := s.cluster.logger
	logger.Info("grpc远程节点流接收消息")
	for {
		msg, err := stream.Recv()
		if err != nil {
			// 对端 handler 正常 return nil，流结束 || 连接关闭
			if err == io.EOF || errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
				logger.Info("grpc远程节点流接收消息", glog.Err(err))
				return nil
			}

			logger.Error("grpc远程节点流接收消息", glog.Err(err))
			return err

		}
		grs.Try(func() {
			if err = s.cluster.Dispatch(msg); err != nil {
				logger.Error("grpc远程节点流接收消息", glog.Err(err))
			}
		}, nil)
	}
}

func (s *NodeServer) isNodeServerStopErr(err error) bool {
	return err == nil ||
		errors.Is(err, grpc.ErrServerStopped) ||
		errors.Is(err, net.ErrClosed) ||
		strings.Contains(err.Error(), "use of closed network connection")
}

func (s *NodeServer) shutdown() error {
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
	return nil
}
