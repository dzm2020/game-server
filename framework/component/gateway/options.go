package gateway

import (
	"fmt"
	"game-server/framework/gen"
	"game-server/framework/network"
)

type AgentFactory func() (IAgent, gen.SpawnOptions)

type Options struct {
	ProtoAddr      string
	NetworkOptions network.ServerOptions
	AgentFactory   AgentFactory
}

func normalization(opts Options) Options {
	if opts.ProtoAddr == "" {
		opts.ProtoAddr = "ws://127.0.0.1:9000/ws"
	}
	opts.NetworkOptions = network.Normalization(opts.NetworkOptions)
	return opts
}

func validate(opts Options) error {
	if opts.ProtoAddr == "" {
		return fmt.Errorf("%w: proto address is empty", gen.ErrNetworkInvalidProtoAddr)
	}
	if opts.AgentFactory == nil {
		return ErrAgentSpawnerNil
	}
	if err := network.Validate(opts.NetworkOptions); err != nil {
		return err
	}
	return nil
}
