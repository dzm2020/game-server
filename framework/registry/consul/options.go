package consul

import (
	"fmt"
	"game-server/framework/gen"

	"github.com/hashicorp/consul/api"
)

func toConsulConfig(options gen.ConsulOptions) *api.Config {
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

func normalization(options gen.ConsulOptions) gen.ConsulOptions {
	return gen.NormalizeConsulOptions(options)
}

func validate(options gen.ConsulOptions) error {
	if err := gen.ValidateConsulOptions(options); err != nil {
		return fmt.Errorf("invalid consul options: %w", err)
	}
	return nil
}
