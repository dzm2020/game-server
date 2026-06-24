package gateway

import "errors"

var (
	ErrSystemComponentAbsent = errors.New("gateway depends on system component")
	ErrComponentNotInited    = errors.New("gateway component is not initialized")
	ErrClientAgentNotFound   = errors.New("gateway client agent not found")
	ErrInboundPayloadTooLarge = errors.New("gateway inbound payload too large")
	ErrInboundBufferOverflow  = errors.New("gateway inbound buffer overflow")
	ErrInboundDecodeNoProgress = errors.New("gateway inbound decode no progress")
)
