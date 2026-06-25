package gateway

import (
	"game-server/framework/gen"
)

var (
	ErrSystemComponentAbsent  = gen.ErrGatewaySystemComponentAbsent
	ErrComponentNotInited     = gen.ErrGatewayComponentNotInited
	ErrClientAgentNotFound    = gen.ErrGatewayClientAgentNotFound
	ErrInboundPayloadTooLarge = gen.ErrGatewayInboundPayloadTooLarge
	ErrAgentSpawnerNil        = gen.ErrGatewayAgentSpawnerNil
	ErrCreateNetworkServer    = gen.ErrGatewayCreateNetworkServer
	ErrBuildClientAgent       = gen.ErrGatewayBuildClientAgent
	ErrSpawnClientAgent       = gen.ErrGatewaySpawnClientAgent
	ErrNilClientAgent         = gen.ErrGatewayNilClientAgent
	ErrInvalidPushParams      = gen.ErrGatewayInvalidPushParams
	ErrConnectionUnavailable  = gen.ErrGatewayConnectionUnavailable
	ErrInvalidMessageType     = gen.ErrGatewayInvalidMessageType
)
