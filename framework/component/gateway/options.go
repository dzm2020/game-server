package gateway

import (
	"game-server/framework/network"
)

type Options struct {
	ProtoAddr      string
	NetworkOptions network.ServerOptions
	AgentFactory   AgentFactory
}

func normalization(opts Options) Options {
	if opts.ProtoAddr == "" {
		opts.ProtoAddr = "ws://127.0.0.1:9000/ws"
	}
	return opts
}
