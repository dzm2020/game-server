package network

import (
	"context"
	"errors"
	"game-server/framework/gen"
	"game-server/framework/pkg/glog"
	"net/http"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

var (
	upgrader = websocket.Upgrader{
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
)

type WebSocketServer struct {
	*baseServer
	upgrader   websocket.Upgrader
	httpServer *http.Server
	path       string
	useTLS     bool
}

const websocketServerComponent = "network.websocket.server"

func NewWebSocketServer(base *baseServer, path string) *WebSocketServer {
	wsUpgrader := upgrader
	if base.serverOpts.WebOptions.CheckOrigin != nil {
		wsUpgrader.CheckOrigin = base.serverOpts.WebOptions.CheckOrigin
	}
	return &WebSocketServer{
		baseServer: base,
		upgrader:   wsUpgrader,
		path:       path,
		useTLS:     base.network == "wss",
	}
}

func (s *WebSocketServer) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc(s.path, s.handleWebSocket)
	s.httpServer = &http.Server{
		Addr:    s.address,
		Handler: mux,
	}

	s.runGroup.Go(func(ctx context.Context) {
		glog.Info("WebSocket监听", gen.FieldComponent(websocketServerComponent), zap.String("addr", s.Addr()), zap.String("path", s.path))

		if err := s.listenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {

			glog.Error("WebSocket监听", gen.FieldComponent(websocketServerComponent), zap.String("addr", s.Addr()), zap.String("path", s.path), gen.FieldErr(err))
			return
		}
	})

	return nil
}

func (s *WebSocketServer) listenAndServe() error {
	if s.useTLS && s.serverOpts.WebOptions.TLSConfig != nil {
		s.httpServer.TLSConfig = s.serverOpts.WebOptions.TLSConfig
		return s.httpServer.ListenAndServeTLS("", "")
	} else {
		return s.httpServer.ListenAndServe()
	}
}

func (s *WebSocketServer) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		glog.Error("WebSocket升级失败", gen.FieldComponent(websocketServerComponent), zap.String("addr", s.Addr()), gen.FieldErr(err))
		return
	}

	wsConn := newWebSocketConnection(s.connCommon(), conn)
	s.connMgr.Add(wsConn)

	s.runGroup.Go(func(ctx context.Context) {
		wsConn.readLoop()
	})

	s.runGroup.Go(func(ctx context.Context) {
		wsConn.writeLoop()
	})

	s.runGroup.Go(func(ctx context.Context) {
		wsConn.heartbeat(wsConn)
	})
}

func (s *WebSocketServer) Shutdown(ctx context.Context) {
	if !s.Stop() {
		return
	}
	_ = s.httpServer.Shutdown(ctx)
	s.baseServer.Shutdown(ctx)

	glog.Info("WebSocket服务器关闭", gen.FieldComponent(websocketServerComponent), zap.String("addr", s.Addr()), zap.String("path", s.path))
}
