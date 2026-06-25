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

const serverComponent = "cluster.grpc.server"

// NewNodeServer 创建服务端
func NewNodeServer(serveGroup *grs.Group, address string, dispatcher Dispatcher) *NodeServer {
	return &NodeServer{
		address:    address,
		dispatcher: dispatcher,
		serveGroup: serveGroup,
	}
}

// NodeServer gRPC服务端
type NodeServer struct {
	UnimplementedNodeServiceServer
	address    string
	lis        net.Listener
	server     *grpc.Server
	dispatcher Dispatcher
	serveGroup *grs.Group
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

		glog.Error("grpc集群服务监听", glog.Component(serverComponent), zap.String("listen_addr", s.address), glog.Err(err))
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

	glog.Info("grpc集群服务监听", glog.Component(serverComponent), zap.String("listen_addr", s.address))
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
	startCtx, cancel := context.WithTimeout(s.serveGroup.Context(), 3*time.Second)
	defer cancel()
	serveErrCh := make(chan error, 1)
	s.serveGroup.Go(func(context.Context) {
		err := s.server.Serve(s.lis)
		if isNodeServerStopErr(err) {
			glog.Info("grpc集群服务端协程停止", glog.Component(serverComponent), zap.String("listen_addr", s.address))
			select {
			case serveErrCh <- nil:
			default:
			}
			return
		}
		select {
		case serveErrCh <- err:
		default:
		}
		glog.Info("grpc集群服务端协程停止", glog.Component(serverComponent), zap.String("listen_addr", s.address), glog.Err(err))
	})

	select {
	case err := <-serveErrCh:
		return err
	case <-startCtx.Done():
		return nil
	}
}

// Stream 实现双向流
func (s *NodeServer) Stream(stream NodeService_StreamServer) error {
	glog.Info("grpc集群服务端收协程启动", glog.Component(serverComponent))
	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF || errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
				glog.Info("grpc集群服务端接收终止", glog.Component(serverComponent), glog.Err(err))
				return nil
			}

			glog.Error("grpc集群服务端接收", glog.Component(serverComponent), glog.Err(err))
			return err
		}

		if err = s.dispatcher.Dispatch(msg); err != nil {
			glog.Error("grpc集群服务端接收", glog.Component(serverComponent), glog.Err(err))
		}
	}
}

func (s *NodeServer) shutdown() error {
	glog.Info("grpc集群服务端关闭", glog.Component(serverComponent))
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

func isNodeServerStopErr(err error) bool {
	return err == nil ||
		errors.Is(err, grpc.ErrServerStopped) ||
		errors.Is(err, net.ErrClosed) ||
		strings.Contains(err.Error(), "use of closed network connection")
}
