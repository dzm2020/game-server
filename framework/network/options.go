package network

import (
	"crypto/tls"
	"net/http"
)

const (
	defaultSendBufferSize  = 1024 * 4
	defaultReadBufSize     = 1024 * 4
	defaultUdpRcvChanSize  = 1024
	defaultSendChanSize    = 1024
	defaultHeartTimeoutSec = 5
)

type ServerOptions struct {
	HeartIntervalSecond int64
	SendChanSize        int
	ReusePort           bool
	ReuseAddr           bool
	WebOptions          WebServerOptions
	TcpOptions          TcpServerOptions
	UdpOptions          UdpServerOptions
}

type WebServerOptions struct {
	TLSConfig   *tls.Config
	CheckOrigin func(r *http.Request) bool
}

type TcpServerOptions struct {
	WriteBufferSize int
	ReadBufferSize  int
}
type UdpServerOptions struct {
	ReadChanSize int
	SendChanSize int
}

func normalization(opts ServerOptions) ServerOptions {
	if opts.HeartIntervalSecond <= 0 {
		opts.HeartIntervalSecond = defaultHeartTimeoutSec
	}
	if opts.SendChanSize <= 0 {
		opts.SendChanSize = defaultSendChanSize
	}
	if opts.TcpOptions.ReadBufferSize <= 0 {
		opts.TcpOptions.ReadBufferSize = defaultReadBufSize
	}
	if opts.TcpOptions.WriteBufferSize <= 0 {
		opts.TcpOptions.WriteBufferSize = defaultSendBufferSize
	}
	if opts.UdpOptions.ReadChanSize <= 0 {
		opts.UdpOptions.ReadChanSize = defaultUdpRcvChanSize
	}
	if opts.UdpOptions.SendChanSize <= 0 {
		opts.UdpOptions.SendChanSize = opts.SendChanSize
	}
	if opts.WebOptions.CheckOrigin == nil {
		opts.WebOptions.CheckOrigin = func(r *http.Request) bool { return true }
	}
	return opts
}
