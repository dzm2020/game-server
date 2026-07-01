package consul

import (
	"fmt"
	"time"

	"github.com/hashicorp/consul/api"
)

const (
	defaultAddress          = "127.0.0.1:8500"
	defaultTTL              = 5 * time.Second
	defaultDeregisterAfter  = 5 * time.Minute
	defaultDiscoverCacheTTL = 2 * time.Second
)

type Options struct {
	Address          string
	Scheme           string
	Token            string
	Datacenter       string
	TTL              time.Duration
	DeregisterAfter  time.Duration
	DiscoverCacheTTL time.Duration
}

func DefaultOptions() Options {
	return NormalizeOptions(Options{})
}

func toConsulConfig(options Options) *api.Config {
	consulCfg := api.DefaultConfig()
	if options.Address != "" {
		consulCfg.Address = options.Address
	}
	if options.Scheme != "" {
		consulCfg.Scheme = options.Scheme
	}
	if options.Token != "" {
		consulCfg.Token = options.Token
	}
	if options.Datacenter != "" {
		consulCfg.Datacenter = options.Datacenter
	}
	return consulCfg
}

func NormalizeOptions(options Options) Options {
	if options.TTL <= 0 {
		options.TTL = defaultTTL
	}
	if options.DeregisterAfter <= 0 {
		options.DeregisterAfter = defaultDeregisterAfter
	}
	if options.Address == "" {
		options.Address = defaultAddress
	}
	if options.DiscoverCacheTTL <= 0 {
		options.DiscoverCacheTTL = defaultDiscoverCacheTTL
	}
	return options
}

func ValidateOptions(options Options) error {
	if options.Address == "" {
		return fmt.Errorf("consul address is empty")
	}
	if options.TTL <= 0 {
		return fmt.Errorf("invalid consul ttl: %s", options.TTL)
	}
	if options.DeregisterAfter <= 0 {
		return fmt.Errorf("invalid consul deregister-after: %s", options.DeregisterAfter)
	}
	if options.DiscoverCacheTTL <= 0 {
		return fmt.Errorf("invalid consul discover cache ttl: %s", options.DiscoverCacheTTL)
	}
	return nil
}
