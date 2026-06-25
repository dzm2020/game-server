package grpc_cluster

import (
	"fmt"
	"game-server/framework/gen"
)

func normalization(opts *Options) *Options {
	if opts == nil {
		opts = &Options{}
	}
	grpcOpts := gen.NormalizeGrpcOptions(gen.GrpcOptions{PeerSendChanSize: opts.PeerSendChanSize})
	opts.PeerSendChanSize = grpcOpts.PeerSendChanSize
	return opts
}

func validate(opts *Options) error {
	if opts == nil {
		return fmt.Errorf("cluster options is nil")
	}
	if opts.ListenAddr == "" {
		return fmt.Errorf("cluster listen address is empty")
	}
	if err := gen.ValidateGrpcOptions(gen.GrpcOptions{PeerSendChanSize: opts.PeerSendChanSize}); err != nil {
		return err
	}
	return nil
}
