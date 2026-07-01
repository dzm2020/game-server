package grpc

import (
	"fmt"
	"time"
)

const (
	defaultConnectInterval           = 3 * time.Second
	defaultClientSendChanSize        = 1024
	defaultClientConnectTimeout      = 5 * time.Second
	defaultClientLoadBalancingPolicy = "pick_first"
	defaultClientKeepaliveTime       = 30 * time.Second
	defaultClientKeepaliveTimeout    = 10 * time.Second
	defaultClientPermitWithoutStream = true
)

type Options struct {
	ID                  string
	ListenAddr          string
	Remotes             []string
	RefreshNodeInterval time.Duration
	Client              ClientOptions
}

type ClientOptions struct {
	SendChanSize        int
	ConnectTimeout      time.Duration
	LoadBalancingPolicy string
	Keepalive           ClientKeepaliveOptions
}

type ClientKeepaliveOptions struct {
	Time                time.Duration
	Timeout             time.Duration
	PermitWithoutStream *bool
}

func DefaultOptions() Options {
	return NormalizeOptions(Options{})
}

func NormalizeOptions(options Options) Options {
	if options.RefreshNodeInterval <= 0 {
		options.RefreshNodeInterval = defaultConnectInterval
	}
	if options.Client.SendChanSize <= 0 {
		options.Client.SendChanSize = defaultClientSendChanSize
	}
	if options.Client.ConnectTimeout <= 0 {
		options.Client.ConnectTimeout = defaultClientConnectTimeout
	}
	if options.Client.LoadBalancingPolicy == "" {
		options.Client.LoadBalancingPolicy = defaultClientLoadBalancingPolicy
	}

	if options.Client.Keepalive.Time <= 0 {
		options.Client.Keepalive.Time = defaultClientKeepaliveTime
	}
	if options.Client.Keepalive.Timeout <= 0 {
		options.Client.Keepalive.Timeout = defaultClientKeepaliveTimeout
	}
	if options.Client.Keepalive.PermitWithoutStream == nil {
		options.Client.Keepalive.PermitWithoutStream = boolPtr(defaultClientPermitWithoutStream)
	}
	return options
}

func ValidateOptions(options Options) error {
	if options.RefreshNodeInterval <= 0 {
		return fmt.Errorf("invalid grpc refresh node interval: %s", options.RefreshNodeInterval)
	}
	if options.Client.SendChanSize <= 0 {
		return fmt.Errorf("invalid grpc client send chan size: %d", options.Client.SendChanSize)
	}
	if options.Client.ConnectTimeout <= 0 {
		return fmt.Errorf("invalid grpc client connect timeout: %s", options.Client.ConnectTimeout)
	}
	if options.Client.LoadBalancingPolicy == "" {
		return fmt.Errorf("grpc load balancing policy is empty")
	}

	if options.Client.Keepalive.Time <= 0 {
		return fmt.Errorf("invalid grpc client keepalive time: %s", options.Client.Keepalive.Time)
	}
	if options.Client.Keepalive.Timeout <= 0 {
		return fmt.Errorf("invalid grpc client keepalive timeout: %s", options.Client.Keepalive.Timeout)
	}
	if options.Client.Keepalive.PermitWithoutStream == nil {
		return fmt.Errorf("grpc keepalive permit-without-stream option is nil")
	}
	return nil
}

func boolPtr(v bool) *bool {
	return &v
}
