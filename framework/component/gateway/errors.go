package gateway

import (
	"errors"
)

var (
	ErrSystemComponentAbsent  = errors.New("gateway depends on system component")
	ErrComponentNotInited     = errors.New("gateway component is not initialized")
	ErrClientAgentNotFound    = errors.New("gateway client agent not found")
	ErrInboundPayloadTooLarge = errors.New("gateway inbound payload too large")
	ErrAgentSpawnerNil        = errors.New("gateway agent spawner is nil")
	ErrCreateNetworkServer    = errors.New("gateway create network server failed")
	ErrBuildClientAgent       = errors.New("gateway build client agent failed")
	ErrSpawnClientAgent       = errors.New("gateway spawn client agent failed")
	ErrNilClientAgent         = errors.New("gateway client agent is nil")
	ErrInvalidPushParams      = errors.New("gateway invalid push params")
	ErrConnectionUnavailable  = errors.New("gateway connection is unavailable")
	ErrInvalidMessageType     = errors.New("gateway invalid message type")
)
