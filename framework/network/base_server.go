package network

import (
	"context"
	"fmt"
	"game-server/framework/gen"
	"game-server/framework/grs"
	"game-server/framework/pkg/glog"
	"game-server/framework/pkg/stopper"

	"go.uber.org/zap"
)

func newBaseServer(network, address string, handler IHandler, options ServerOptions) *baseServer {
	server := &baseServer{
		serverOpts:   options,
		network:      network,
		address:      address,
		protoAddress: fmt.Sprintf("%s:%s", network, address),
		handler:      handler,
		connMgr:      NewConnManager(),
		runGroup:     grs.NewGroup(context.Background()),
	}
	server.ctx, server.cancel = context.WithCancel(context.Background())
	return server
}

type baseServer struct {
	stopper.Stopper
	serverOpts       ServerOptions
	handler          IHandler
	network, address string
	protoAddress     string
	runGroup         *grs.Group
	ctx              context.Context
	cancel           context.CancelFunc
	connMgr          *ConnManager
}

func (s *baseServer) connCommon() connCommon {
	return connCommon{
		ctx:        s.ctx,
		serverOpts: s.serverOpts,
		connMgr:    s.connMgr,
		handler:    s.handler,
	}
}

func (s *baseServer) Addr() string {
	return s.protoAddress
}

func (s *baseServer) Shutdown(ctx context.Context) {
	s.cancel()
	if ctx == nil {
		ctx = context.Background()
	}
	if s.runGroup == nil {
		return
	}
	if err := s.runGroup.Wait(ctx); err != nil {
		glog.Warn("等待网络服务协程退出超时",
			gen.FieldComponent("network.server"),
			zap.String("addr", s.Addr()),
			gen.FieldErr(err),
		)
	}
}
