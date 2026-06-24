package network

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
)

var (
	connIDCounter atomic.Int64
)

func genConnID() int64 {
	return connIDCounter.Add(1)
}

type IHandler interface {
	OnConnect(conn IConnection) error
	OnMessage(conn IConnection, data []byte) (int, error)
	OnClose(conn IConnection, err error)
}

type IConnection interface {
	ID() int64
	Send(msg []byte) error
	Close(err error) error
	LocalAddr() string
	RemoteAddr() string
	IsStop() bool
	Context() interface{}
	SetContext(interface{})
	SetReadBuffer(bytes int) error
	SetWriteBuffer(bytes int) error
}

type IServer interface {
	Start() error
	Shutdown(ctx context.Context)
	Addr() string
}

type EmptyHandler struct{}

func (e *EmptyHandler) OnConnect(conn IConnection) error { return nil }

func (e *EmptyHandler) OnMessage(conn IConnection, data []byte) (int, error) { return 0, nil }

func (e *EmptyHandler) OnClose(conn IConnection, err error) {}
func NewServer(handler IHandler, protoAddr string, options ServerOptions) (IServer, error) {
	if handler == nil {
		handler = new(EmptyHandler)
	}
	options = normalization(options)
	network, address := parseProtoAddr(protoAddr)
	if network == "" || address == "" {
		return nil, fmt.Errorf("%w: %s", ErrInvalidProtoAddr, protoAddr)
	}
	path, address := parseWsAddr(address)
	base := newBaseServer(network, address, handler, options)
	switch network {
	case "tcp", "tcp4", "tcp6":
		return NewTCPServer(base), nil
	case "udp", "udp4", "udp6":
		return NewUDPServer(base), nil
	case "ws", "wss":
		return NewWebSocketServer(base, path), nil
	default:
		return nil, ErrUnsupportedProtocol(network)
	}
}

func parseWsAddr(address string) (string, string) {
	path := "/"
	if idx := strings.Index(address, "/"); idx >= 0 {
		path = address[idx:]
		address = address[:idx]
	}
	return path, address
}

func parseProtoAddr(addr string) (string, string) {
	data := strings.Split(addr, "://")
	if len(data) != 2 {
		return "", ""
	}
	return data[0], data[1]
}
