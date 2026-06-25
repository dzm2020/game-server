package network

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"time"
)

const (
	defaultSendBufferSize  = 1024 * 4
	defaultReadBufSize     = 1024 * 4
	defaultUdpRcvChanSize  = 1024
	defaultSendChanSize    = 1024
	defaultHeartTimeoutSec = 5
	defaultTCPWriteTimeout = time.Millisecond * 500
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
	WriteTimeout    time.Duration
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
	if opts.TcpOptions.WriteTimeout <= 0 {
		opts.TcpOptions.WriteTimeout = defaultTCPWriteTimeout
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

func validate(opts ServerOptions) error {
	if opts.HeartIntervalSecond <= 0 {
		return fmt.Errorf("invalid network heart interval second: %d", opts.HeartIntervalSecond)
	}
	if opts.SendChanSize <= 0 {
		return fmt.Errorf("invalid network send chan size: %d", opts.SendChanSize)
	}
	if opts.TcpOptions.ReadBufferSize <= 0 {
		return fmt.Errorf("invalid tcp read buffer size: %d", opts.TcpOptions.ReadBufferSize)
	}
	if opts.TcpOptions.WriteBufferSize <= 0 {
		return fmt.Errorf("invalid tcp write buffer size: %d", opts.TcpOptions.WriteBufferSize)
	}
	if opts.TcpOptions.WriteTimeout <= 0 {
		return fmt.Errorf("invalid tcp write timeout: %s", opts.TcpOptions.WriteTimeout)
	}
	if opts.UdpOptions.ReadChanSize <= 0 {
		return fmt.Errorf("invalid udp read chan size: %d", opts.UdpOptions.ReadChanSize)
	}
	if opts.UdpOptions.SendChanSize <= 0 {
		return fmt.Errorf("invalid udp send chan size: %d", opts.UdpOptions.SendChanSize)
	}
	if opts.WebOptions.CheckOrigin == nil {
		return fmt.Errorf("invalid web check-origin handler: nil")
	}
	return nil
}

func Normalization(opts ServerOptions) ServerOptions {
	return normalization(opts)
}

func Validate(opts ServerOptions) error {
	return validate(opts)
}
