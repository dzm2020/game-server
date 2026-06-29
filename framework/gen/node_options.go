package gen

import (
	"fmt"
	"game-server/framework/pkg/glog"
)

type LoggerOptions = glog.Config

type NodeOptions struct {
	ID         string
	Name       string
	ExtAddress string
	RpcAddress string
	Meta       map[string]string
	Tags       []string
	Logger     LoggerOptions
	Behavior   INodeBehavior
}

func NormalizeNodeOptions(opts NodeOptions) NodeOptions {
	if opts.Behavior == nil {
		opts.Behavior = BaseNodeBehavior{}
	}
	opts.Logger = NormalizeLoggerOptions(opts.Logger)

	return opts
}

func ValidateNodeOptions(opts NodeOptions) error {
	if opts.Behavior == nil {
		return fmt.Errorf("node behavior is nil")
	}
	if err := ValidateLoggerOptions(opts.Logger); err != nil {
		return err
	}
	return nil
}

func NormalizationNodeOptions(opts NodeOptions) NodeOptions {
	return NormalizeNodeOptions(opts)
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
