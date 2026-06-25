package gateway

import (
	"game-server/framework/gen"
)

var (
	ErrSystemComponentAbsent  = gen.NewError("gateway depends on system component", 100)
	ErrComponentNotInited     = gen.NewError("gateway component is not initialized", 101)
	ErrClientAgentNotFound    = gen.NewError("gateway client agent not found", 102)
	ErrInboundPayloadTooLarge = gen.NewError("gateway inbound payload too large", 103)
	ErrAgentSpawnerNil        = gen.NewError("gateway agent spawner is nil", 104)
	ErrCreateNetworkServer    = gen.NewError("gateway create network server failed", 105)
	ErrBuildClientAgent       = gen.NewError("gateway build client agent failed", 106)
	ErrSpawnClientAgent       = gen.NewError("gateway spawn client agent failed", 107)
	ErrNilClientAgent         = gen.NewError("gateway client agent is nil", 108)
	ErrInvalidPushParams      = gen.NewError("gateway invalid push params", 109)
	ErrConnectionUnavailable  = gen.NewError("gateway connection is unavailable", 110)
	ErrInvalidMessageType     = gen.NewError("gateway invalid message type", 111)
)
