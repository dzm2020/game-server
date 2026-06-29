package grpc

import "fmt"

const defaultClientSendChanSize = 1024

type Options struct {
	Remotes []string
	Client  ClientOptions
}

type ClientOptions struct {
	SendChanSize int
}

func DefaultOptions() Options {
	return NormalizeOptions(Options{})
}

func NormalizeOptions(options Options) Options {
	if options.Client.SendChanSize <= 0 {
		options.Client.SendChanSize = defaultClientSendChanSize
	}
	return options
}

func ValidateOptions(options Options) error {
	if options.Client.SendChanSize <= 0 {
		return fmt.Errorf("invalid grpc client send chan size: %d", options.Client.SendChanSize)
	}
	return nil
}
