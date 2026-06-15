package network

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
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
	logger    Logger
	loggerMu  sync.RWMutex

	mu     sync.RWMutex
	conns  map[*Connection]struct{}
	server *http.Server
	connID atomic.Uint64
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
		conns:  make(map[*Connection]struct{}),
		opts:   defaultServerOptions(),
		logger: noopLogger{},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	return s
}

// SetLogger sets logger for current WebsocketServer instance.
func (s *WebsocketServer) SetLogger(logger Logger) {
	s.loggerMu.Lock()
	s.logger = ensureLogger(logger)
	s.loggerMu.Unlock()
}

// Logger returns logger instance bound to current WebsocketServer.
func (s *WebsocketServer) Logger() Logger {
	s.loggerMu.RLock()
	logger := s.logger
	s.loggerMu.RUnlock()
	return ensureLogger(logger)
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
	if useTLS {
		s.Logger().Infof("WSS server listening on %s", s.Addr)
		return s.server.ListenAndServeTLS(certFile, keyFile)
	}

	s.Logger().Infof("WebSocket server listening on %s", s.Addr)
	return s.server.ListenAndServe()
}

func (s *WebsocketServer) upgrade(w http.ResponseWriter, r *http.Request) (*websocket.Conn, error) {
	conn, err := s.Upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.Logger().Errorf("upgrade error: %v", err)
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

	safeCall(s.Logger(), func() {
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
