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
	return opts
}
