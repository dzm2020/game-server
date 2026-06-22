package gen

import (
	"game-server/framework/pkg/glog"

	"time"
)

type LoggerOptions = glog.Config

type NodeOptions struct {
	ID          string
	Name        string
	ExtAddress  string
	RpcAddress  string
	RemoteNames []string
	Grpc        GrpcOptions
	Consul      ConsulOptions
	Logger      LoggerOptions
	Behavior    INodeBehavior
	Registry    IRegistry
	Cluster     ICluster
}

type GrpcOptions struct {
	PeerSendChanSize int
}

type ConsulOptions struct {
	Address         string
	Scheme          string
	Token           string
	Datacenter      string
	TTL             time.Duration
	DeregisterAfter time.Duration
}

func DefaultGrpcOptions() *GrpcOptions {
	return &GrpcOptions{
		PeerSendChanSize: 1000,
	}
}

func DefaultConsulOptions() *ConsulOptions {
	return &ConsulOptions{
		Address:         "127.0.0.1:8500",
		Scheme:          "http",
		Token:           "",
		Datacenter:      "dc1",
		TTL:             3 * time.Second,
		DeregisterAfter: 120 * time.Second,
	}
}

func DefaultNodeOptions() *NodeOptions {
	return &NodeOptions{
		ID:         "",
		Name:       "",
		ExtAddress: "",
		RpcAddress: "",
	}
}

// EnsureNodeOptions validates NodeOptions and fills default values for zero fields.
// It always returns a non-nil options instance.
func EnsureNodeOptions(opts *NodeOptions) *NodeOptions {
	if opts == nil {
		return DefaultNodeOptions()
	}

	if opts.Behavior == nil {
		opts.Behavior = BaseNodeBehavior{}
	}
	// Grpc 子项默认值
	grpcDefault := DefaultGrpcOptions()

	if opts.Grpc.PeerSendChanSize <= 0 {
		opts.Grpc.PeerSendChanSize = grpcDefault.PeerSendChanSize
	}

	// Consul 子项默认值
	consulDefault := DefaultConsulOptions()
	if opts.Consul.Address == "" {
		opts.Consul.Address = consulDefault.Address
	}
	if opts.Consul.Scheme == "" {
		opts.Consul.Scheme = consulDefault.Scheme
	}
	if opts.Consul.Datacenter == "" {
		opts.Consul.Datacenter = consulDefault.Datacenter
	}
	if opts.Consul.TTL <= 0 {
		opts.Consul.TTL = consulDefault.TTL
	}
	if opts.Consul.DeregisterAfter <= 0 {
		opts.Consul.DeregisterAfter = consulDefault.DeregisterAfter
	}

	// Logger 子项默认值
	loggerDefault := glog.DefaultConfig()
	if opts.Logger.Path == "" {
		opts.Logger.Path = loggerDefault.Path
	}
	if opts.Logger.Level == "" {
		opts.Logger.Level = loggerDefault.Level
	}
	if opts.Logger.MaxSize <= 0 {
		opts.Logger.MaxSize = loggerDefault.MaxSize
	}
	if opts.Logger.MaxBackups <= 0 {
		opts.Logger.MaxBackups = loggerDefault.MaxBackups
	}
	if opts.Logger.MaxAge <= 0 {
		opts.Logger.MaxAge = loggerDefault.MaxAge
	}

	return opts
}
