package gateway

import (
	"errors"
	"game-server/framework/network"
)

type Options struct {
	ProtoAddr              string
	NetworkOptions         network.ServerOptions
	AgentFactory           AgentFactory
	MaxInboundPayloadBytes int
	MaxBufferedBytes       int
}

func normalization(opts Options) Options {
	if opts.ProtoAddr == "" {
		opts.ProtoAddr = "ws://127.0.0.1:9000/ws"
	}
	if opts.AgentFactory == nil {
		opts.AgentFactory = func() (IAgent, error) {
			return nil, errors.New("agent factory is nil")
		}
	}
	if opts.MaxBufferedBytes < opts.MaxInboundPayloadBytes {
		opts.MaxBufferedBytes = opts.MaxInboundPayloadBytes
	}
	return opts
}
