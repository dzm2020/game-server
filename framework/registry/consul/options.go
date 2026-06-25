package consul

import (
	"game-server/framework/gen"
	"time"

	"github.com/hashicorp/consul/api"
)

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
