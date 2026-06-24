package network

import (
	"errors"
	"fmt"
)

var (
	ErrConnectionClosed = errors.New("connection closed")
	ErrChannelFull      = errors.New("channel full")
	ErrConnHeartTimeout = errors.New("heart timeout")
)

func ErrUnsupportedProtocol(proto string) error {
	return fmt.Errorf("proto: %s is not support", proto)
}
