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
	defaultClientBackoffBaseDelay    = 1 * time.Second
	defaultClientBackoffMultiplier   = 1.6
	defaultClientBackoffJitter       = 0.2
	defaultClientBackoffMaxDelay     = 30 * time.Second
	defaultClientMinConnectTimeout   = 5 * time.Second
	defaultClientKeepaliveTime       = 30 * time.Second
	defaultClientKeepaliveTimeout    = 10 * time.Second
	defaultClientPermitWithoutStream = true
	defaultClientStreamWaitForReady  = true
)

type Options struct {
	Remotes         []string
	ConnectInterval time.Duration
	Client          ClientOptions
}

type ClientOptions struct {
	SendChanSize        int
	ConnectTimeout      time.Duration
	LoadBalancingPolicy string
	WaitForReady        *bool
	Backoff             ClientBackoffOptions
	Keepalive           ClientKeepaliveOptions
}

type ClientBackoffOptions struct {
	BaseDelay         time.Duration
	Multiplier        float64
	Jitter            float64
	MaxDelay          time.Duration
	MinConnectTimeout time.Duration
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
	if options.ConnectInterval <= 0 {
		options.ConnectInterval = defaultConnectInterval
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
	if options.Client.WaitForReady == nil {
		options.Client.WaitForReady = boolPtr(defaultClientStreamWaitForReady)
	}

	if options.Client.Backoff.BaseDelay <= 0 {
		options.Client.Backoff.BaseDelay = defaultClientBackoffBaseDelay
	}
	if options.Client.Backoff.Multiplier <= 0 {
		options.Client.Backoff.Multiplier = defaultClientBackoffMultiplier
	}
	if options.Client.Backoff.Jitter < 0 {
		options.Client.Backoff.Jitter = 0
	}
	if options.Client.Backoff.MaxDelay <= 0 {
		options.Client.Backoff.MaxDelay = defaultClientBackoffMaxDelay
	}
	if options.Client.Backoff.MinConnectTimeout <= 0 {
		options.Client.Backoff.MinConnectTimeout = defaultClientMinConnectTimeout
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
	if options.ConnectInterval <= 0 {
		return fmt.Errorf("invalid grpc connect interval: %s", options.ConnectInterval)
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
	if options.Client.WaitForReady == nil {
		return fmt.Errorf("grpc wait-for-ready option is nil")
	}

	if options.Client.Backoff.BaseDelay <= 0 {
		return fmt.Errorf("invalid grpc client backoff base delay: %s", options.Client.Backoff.BaseDelay)
	}
	if options.Client.Backoff.Multiplier <= 0 {
		return fmt.Errorf("invalid grpc client backoff multiplier: %f", options.Client.Backoff.Multiplier)
	}
	if options.Client.Backoff.Jitter < 0 || options.Client.Backoff.Jitter > 1 {
		return fmt.Errorf("invalid grpc client backoff jitter: %f", options.Client.Backoff.Jitter)
	}
	if options.Client.Backoff.MaxDelay <= 0 {
		return fmt.Errorf("invalid grpc client backoff max delay: %s", options.Client.Backoff.MaxDelay)
	}
	if options.Client.Backoff.MinConnectTimeout <= 0 {
		return fmt.Errorf("invalid grpc client min connect timeout: %s", options.Client.Backoff.MinConnectTimeout)
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
