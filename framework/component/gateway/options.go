package gateway

import (
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
