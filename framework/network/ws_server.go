package network

import (
	"context"
	"crypto/tls"
	"errors"
	"game-server/framework/grs"
	"game-server/framework/pkg/glog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// -------------------------------------------
// WebSocket 服务器
// -------------------------------------------

// WebsocketServer WebSocket 服务器定义
type WebsocketServer struct {
	Addr string

	Upgrader  websocket.Upgrader
	wg        sync.WaitGroup
	TLSConfig *tls.Config
	opts      serverOptions

	mu     sync.RWMutex
	conns  map[*Connection]struct{}
	server *http.Server
	connID atomic.Uint64

	readyMu  sync.RWMutex
	readyCh  chan struct{}
	readySet sync.Once
}

// NewWebsocketServer 创建一个新的 WebSocket 服务器实例
func NewWebsocketServer(addr string, opts ...WebsocketServerOption) *WebsocketServer {
	s := &WebsocketServer{
		Addr: addr,
		Upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // 允许所有来源（生产环境需注意）
			},
		},
		conns: make(map[*Connection]struct{}),
		opts:  defaultServerOptions(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	return s
}

// Start 启动服务
func (s *WebsocketServer) Start() error {
	return s.start(false, "", "")
}

// StartTLS 使用 TLS 启动 WebSocket 服务（WSS）。
func (s *WebsocketServer) StartTLS(certFile, keyFile string) error {
	if certFile == "" {
		return ErrTLSCertFileRequired
	}
	if keyFile == "" {
		return ErrTLSKeyFileRequired
	}
	return s.start(true, certFile, keyFile)
}

// StartWSS 是 StartTLS 的语义别名。
func (s *WebsocketServer) StartWSS(certFile, keyFile string) error {
	return s.StartTLS(certFile, keyFile)
}

func (s *WebsocketServer) start(useTLS bool, certFile, keyFile string) error {
	s.server = s.newHTTPServer()
	s.setReadyChan()
	err := s.serve(useTLS, certFile, keyFile)
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *WebsocketServer) newHTTPServer() *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleWebSocket)

	server := &http.Server{
		Addr:    s.Addr,
		Handler: mux,
	}
	if s.TLSConfig != nil {
		server.TLSConfig = s.TLSConfig.Clone()
	}

	return server
}

func (s *WebsocketServer) serve(useTLS bool, certFile, keyFile string) error {
	listener, err := net.Listen("tcp", s.server.Addr)
	if err != nil {
		return err
	}
	s.markReady()

	if useTLS {
		glog.Info("WSS服务启动成功",
			zap.String("listen_addr", s.Addr),
		)
		return s.server.ServeTLS(listener, certFile, keyFile)
	}

	glog.Info("WebSocket服务启动成功",
		zap.String("listen_addr", s.Addr),
	)
	return s.server.Serve(listener)
}

func (s *WebsocketServer) upgrade(w http.ResponseWriter, r *http.Request) (*websocket.Conn, error) {
	conn, err := s.Upgrader.Upgrade(w, r, nil)
	if err != nil {
		glog.Error("WebSocket升级失败",
			zap.Error(err),
			zap.String("remote_addr", r.RemoteAddr),
		)
		return nil, err
	}
	return conn, nil
}

func (s *WebsocketServer) newConnection(conn *websocket.Conn) *Connection {
	return &Connection{
		id:     s.connID.Add(1),
		conn:   conn,
		server: s,
		send:   make(chan []byte, s.opts.sendBuffer),
		done:   make(chan struct{}),
	}
}

// 处理 WebSocket 升级请求
func (s *WebsocketServer) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrade(w, r)
	if err != nil {
		return
	}

	c := s.newConnection(conn)
	s.addConn(c)

	grs.Try(func() {
		s.callOnConnect(c)
	}, nil)

	s.wg.Add(2)

	// 读写分离
	go c.readPump()
	go c.writePump()
}

func (s *WebsocketServer) callOnConnect(c *Connection) {
	handler := s.opts.handler
	if handler == nil {
		return
	}
	handler.OnConnect(c)
}

// ShutdownContext 停止监听并等待连接协程退出。
func (s *WebsocketServer) ShutdownContext(ctx context.Context) error {
	if s.server != nil {
		if err := s.server.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}

	// 主动关闭所有活跃连接，触发读写协程退出
	s.mu.RLock()
	conns := make([]*Connection, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.RUnlock()
	for _, c := range conns {
		c.Close()
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Shutdown 使用默认超时执行优雅关闭。
func (s *WebsocketServer) Shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = s.ShutdownContext(ctx)
}

func (s *WebsocketServer) addConn(c *Connection) {
	s.mu.Lock()
	s.conns[c] = struct{}{}
	s.mu.Unlock()
}

func (s *WebsocketServer) removeConn(c *Connection) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
}

func (s *WebsocketServer) Ready() <-chan struct{} {
	s.readyMu.RLock()
	ch := s.readyCh
	s.readyMu.RUnlock()
	return ch
}

func (s *WebsocketServer) setReadyChan() {
	s.readyMu.Lock()
	s.readyCh = make(chan struct{})
	s.readySet = sync.Once{}
	s.readyMu.Unlock()
}

func (s *WebsocketServer) markReady() {
	s.readyMu.RLock()
	ch := s.readyCh
	s.readyMu.RUnlock()
	if ch == nil {
		return
	}
	s.readySet.Do(func() {
		close(ch)
	})
}
