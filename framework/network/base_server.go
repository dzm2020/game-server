package network

import (
	"context"
	"fmt"
	"game-server/framework/pkg/stopper"
	"sync"
)

func newBaseServer(network, address string, handler IHandler, options ServerOptions) *baseServer {
	server := &baseServer{
		serverOpts:   options,
		network:      network,
		address:      address,
		protoAddress: fmt.Sprintf("%s:%s", network, address),
		handler:      handler,
		connMgr:      NewConnManager(),
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
	waitGroup        sync.WaitGroup
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
	s.waitGroup.Wait()
	return
}
