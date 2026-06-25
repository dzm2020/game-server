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

func NormalizationNodeOptions(opts NodeOptions) NodeOptions {
	if opts.Behavior == nil {
		opts.Behavior = BaseNodeBehavior{}
	}
	opts.Grpc = normalizationGrpc(opts.Grpc)
	opts.Consul = normalizationConsul(opts.Consul)
	opts.Logger = normalizationLogger(opts.Logger)
	return opts
}

func normalizationConsul(options ConsulOptions) ConsulOptions {
	if options.TTL <= 0 {
		options.TTL = time.Second * 5
	}
	if options.DeregisterAfter <= 0 {
		options.DeregisterAfter = time.Minute * 5
	}
	if options.Address == "" {
		options.Address = "127.0.0.1:8500"
	}
	return options
}

func normalizationGrpc(options GrpcOptions) GrpcOptions {
	if options.PeerSendChanSize <= 0 {
		options.PeerSendChanSize = 1024
	}
	return options
}

func normalizationLogger(options LoggerOptions) LoggerOptions {
	if options.Path == "" {
		options.Path = "./logs/app.log"
	}
	if options.Level == "" {
		options.Level = "info"
	}
	if options.MaxSize <= 0 {
		options.MaxSize = 500
	}
	if options.MaxBackups <= 0 {
		options.MaxBackups = 100
	}
	if options.MaxAge <= 0 {
		options.MaxAge = 30
	}
	return options
}
