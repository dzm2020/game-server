package network

import (
	"crypto/tls"
	"time"

	"github.com/gorilla/websocket"
)

type WebsocketServerOption func(*WebsocketServer)

const (
	defaultReadLimit  int64 = 1024 * 1024
	defaultSendBuffer       = 256
)

type serverOptions struct {
	codec      Codec
	handler    EventHandler
	readLimit  int64
	pongWait   time.Duration
	pingPeriod time.Duration
	writeWait  time.Duration
	sendBuffer int
}

func defaultServerOptions() serverOptions {
	return serverOptions{
		codec:      jsonCodec{},
		handler:    BaseEventHandler{},
		readLimit:  defaultReadLimit,
		pongWait:   60 * time.Second,
		pingPeriod: 54 * time.Second,
		writeWait:  10 * time.Second,
		sendBuffer: defaultSendBuffer,
	}
}

func WithCodec(codec Codec) WebsocketServerOption {
	return func(s *WebsocketServer) {
		s.opts.codec = codec
	}
}

func WithEventHandler(handler EventHandler) WebsocketServerOption {
	return func(s *WebsocketServer) {
		s.opts.handler = handler
	}
}

func WithLogger(logger Logger) WebsocketServerOption {
	return func(s *WebsocketServer) {
		s.SetLogger(logger)
	}
}

func WithTLSConfig(cfg *tls.Config) WebsocketServerOption {
	return func(s *WebsocketServer) {
		s.TLSConfig = cfg
	}
}

func WithUpgrader(upgrader websocket.Upgrader) WebsocketServerOption {
	return func(s *WebsocketServer) {
		s.Upgrader = upgrader
	}
}

func WithReadLimit(limit int64) WebsocketServerOption {
	return func(s *WebsocketServer) {
		if limit <= 0 {
			s.opts.readLimit = defaultReadLimit
			return
		}
		s.opts.readLimit = limit
	}
}

func WithPongWait(d time.Duration) WebsocketServerOption {
	return func(s *WebsocketServer) {
		s.opts.pongWait = d
	}
}

func WithPingPeriod(d time.Duration) WebsocketServerOption {
	return func(s *WebsocketServer) {
		s.opts.pingPeriod = d
	}
}

func WithWriteWait(d time.Duration) WebsocketServerOption {
	return func(s *WebsocketServer) {
		s.opts.writeWait = d
	}
}

func WithSendBuffer(size int) WebsocketServerOption {
	return func(s *WebsocketServer) {
		if size <= 0 {
			s.opts.sendBuffer = defaultSendBuffer
			return
		}
		s.opts.sendBuffer = size
	}
}
