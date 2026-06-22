package consul

import (
	"game-server/framework/gen"
	"time"

	"github.com/hashicorp/consul/api"
)

func normalization(options gen.ConsulOptions) gen.ConsulOptions {
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

func toConsulConfig(options gen.ConsulOptions) *api.Config {
	options = normalization(options)
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
