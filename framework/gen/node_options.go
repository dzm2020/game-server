package gen

import (
	"fmt"
	"game-server/framework/pkg/glog"
	"time"
)

type LoggerOptions = glog.Config

type NodeOptions struct {
	ID         string
	Name       string
	ExtAddress string
	RpcAddress string
	Clusters   []string
	Grpc       GrpcOptions
	Consul     ConsulOptions
	Logger     LoggerOptions
	Behavior   INodeBehavior
	Registry   IRegistry
	Cluster    ICluster
}

type GrpcOptions struct {
	ClientSendChanSize int
}

type ConsulOptions struct {
	Address         string
	Scheme          string
	Token           string
	Datacenter      string
	TTL             time.Duration
	DeregisterAfter time.Duration
}

const (
	defaultConsulAddress         = "127.0.0.1:8500"
	defaultConsulTTL             = 5 * time.Second
	defaultConsulDeregisterAfter = 5 * time.Minute
	defaultGrpcPeerSendChanSize  = 1024
)

func NormalizeNodeOptions(opts NodeOptions) NodeOptions {
	if opts.Behavior == nil {
		opts.Behavior = BaseNodeBehavior{}
	}
	opts.Grpc = NormalizeGrpcOptions(opts.Grpc)
	opts.Consul = NormalizeConsulOptions(opts.Consul)
	opts.Logger = NormalizeLoggerOptions(opts.Logger)

	return opts
}

func ValidateNodeOptions(opts NodeOptions) error {
	if opts.Behavior == nil {
		return fmt.Errorf("node behavior is nil")
	}
	if err := ValidateGrpcOptions(opts.Grpc); err != nil {
		return err
	}
	if err := ValidateConsulOptions(opts.Consul); err != nil {
		return err
	}
	if err := ValidateLoggerOptions(opts.Logger); err != nil {
		return err
	}
	return nil
}

func NormalizationNodeOptions(opts NodeOptions) NodeOptions {
	return NormalizeNodeOptions(opts)
}

func NormalizeConsulOptions(options ConsulOptions) ConsulOptions {
	if options.TTL <= 0 {
		options.TTL = defaultConsulTTL
	}
	if options.DeregisterAfter <= 0 {
		options.DeregisterAfter = defaultConsulDeregisterAfter
	}
	if options.Address == "" {
		options.Address = defaultConsulAddress
	}
	return options
}

func ValidateConsulOptions(options ConsulOptions) error {
	if options.Address == "" {
		return fmt.Errorf("consul address is empty")
	}
	if options.TTL <= 0 {
		return fmt.Errorf("invalid consul ttl: %s", options.TTL)
	}
	if options.DeregisterAfter <= 0 {
		return fmt.Errorf("invalid consul deregister-after: %s", options.DeregisterAfter)
	}
	return nil
}

func NormalizeGrpcOptions(options GrpcOptions) GrpcOptions {
	if options.ClientSendChanSize <= 0 {
		options.ClientSendChanSize = defaultGrpcPeerSendChanSize
	}
	return options
}

func ValidateGrpcOptions(options GrpcOptions) error {
	if options.ClientSendChanSize <= 0 {
		return fmt.Errorf("invalid grpc peer send chan size: %d", options.ClientSendChanSize)
	}
	return nil
}

func NormalizeLoggerOptions(options LoggerOptions) LoggerOptions {
	defaults := glog.DefaultConfig()
	if options.Path == "" {
		options.Path = defaults.Path
	}
	if options.Level == "" {
		options.Level = defaults.Level
	}
	if options.MaxSize <= 0 {
		options.MaxSize = defaults.MaxSize
	}
	if options.MaxBackups <= 0 {
		options.MaxBackups = defaults.MaxBackups
	}
	if options.MaxAge <= 0 {
		options.MaxAge = defaults.MaxAge
	}
	return options
}

func ValidateLoggerOptions(options LoggerOptions) error {
	if options.Path == "" {
		return fmt.Errorf("logger path is empty")
	}
	if options.Level == "" {
		return fmt.Errorf("logger level is empty")
	}
	if options.MaxSize <= 0 {
		return fmt.Errorf("invalid logger max size: %d", options.MaxSize)
	}
	if options.MaxBackups <= 0 {
		return fmt.Errorf("invalid logger max backups: %d", options.MaxBackups)
	}
	if options.MaxAge <= 0 {
		return fmt.Errorf("invalid logger max age: %d", options.MaxAge)
	}
	return nil
}
