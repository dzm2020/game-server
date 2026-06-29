package gen

import "fmt"

func NewError(err string, code int32) *Error {
	return &Error{
		err:  err,
		code: code,
	}
}

var _ error = (*Error)(nil)

type Error struct {
	err  string
	code int32
}

func (E *Error) Error() string {
	return E.err
}
func (E *Error) Code() int32 {
	return E.code
}

var (

	// actor
	ErrActorMailboxFull          = NewError("actor mailbox is full", 2)
	ErrActorAskTimeout           = NewError("actor ask timeout", 3)
	ErrActorNoResponder          = NewError("actor message is not ask-able", 4)
	ErrActorSystemClosed         = NewError("actor system is closed", 5)
	ErrActorNilHandler           = NewError("actor handler is nil", 6)
	ErrActorNameExists           = NewError("actor name already exists", 7)
	ErrActorIDConflict           = NewError("actor id already exists", 8)
	ErrActorNotFound             = NewError("actor not found", 9)
	ErrActorInvalidName          = NewError("invalid actor name", 10)
	ErrActorInvalidTarget        = NewError("invalid actor target", 11)
	ErrActorRemoteInvokerNotSet  = NewError("actor remote invoker is not set", 12)
	ErrActorRouteNotFound        = NewError("actor route entry not found", 13)
	ErrActorClusterNil           = NewError("actor cluster is nil", 14)
	ErrActorClusterAskNotImpl    = NewError("actor cluster ask is not implemented", 15)
	ErrActorSystemNil            = NewError("actor system is nil", 16)
	ErrActorProcessStopped       = NewError("actor process is stopped", 17)
	ErrActorPidNil               = NewError("actor pid is nil", 18)
	ErrActorNoAskClusterProvided = NewError("actor no cluster calls are provided.", 19)

	// network
	ErrCodecNotConfigured  = NewError("codec is not configured", 30)
	ErrConnectionClosed    = NewError("connection is closed", 31)
	ErrTLSCertFileRequired = NewError("tls cert file is required", 32)
	ErrTLSKeyFileRequired  = NewError("tls key file is required", 33)
	ErrMessageNil          = NewError("message is nil", 19)

	// cluster / registry
	ErrClusterSystemNil            = NewError("cluster system is nil", 50)
	ErrClusterNil                  = NewError("cluster is nil", 51)
	ErrClusterClosed               = NewError("cluster is closed", 52)
	ErrClusterWaitPeerReadyTimeout = NewError("cluster wait peers ready timeout", 53)
	ErrClusterNodeNotFound         = NewError("cluster node not found", 54)
	ErrClusterNodeNotInServiceList = NewError("cluster node not in service list", 55)
	ErrClusterNodeNotConnected     = NewError("cluster node not connected", 56)
	ErrClusterDecodeFailed         = NewError("cluster decode failed", 57)
	ErrClusterPeerNotConnected     = NewError("cluster peer not connected", 58)
	ErrClusterSendChannelFull      = NewError("cluster send channel full", 59)
	ErrRegistryNil                 = NewError("registry is nil", 60)
	ErrConsulInvalidServiceReg     = NewError("consul invalid service registration", 61)
	ErrConsulServiceIDRequired     = NewError("consul service id is required", 62)
	ErrConsulTTLHeartbeatNotRun    = NewError("consul ttl heartbeat not running", 63)
	ErrNodeNil                     = NewError("node is nil", 64)
	ErrNodeComponentNotRegistered  = NewError("node component not registered", 65)
	ErrNodeComponentTypeMismatched = NewError("node component type mismatch", 66)
	ErrClusterInvokerIsNil         = NewError("cluster local invoker is nil", 67)

	// network detail
	ErrNetworkChannelFull         = NewError("network channel full", 80)
	ErrNetworkHeartTimeout        = NewError("network heart timeout", 81)
	ErrNetworkWriteTimeout        = NewError("network write timeout", 82)
	ErrNetworkInvalidProtoAddr    = NewError("network invalid proto address", 83)
	ErrNetworkUnsupportedProtocol = NewError("network protocol is not supported", 84)
	ErrMessageDataOverSize        = NewError("message data over size ", 85)

	// gateway
	ErrGatewaySystemComponentAbsent  = NewError("gateway depends on system component", 100)
	ErrGatewayComponentNotInited     = NewError("gateway component is not initialized", 101)
	ErrGatewayClientAgentNotFound    = NewError("gateway client agent not found", 102)
	ErrGatewayInboundPayloadTooLarge = NewError("gateway inbound payload too large", 103)
	ErrGatewayAgentSpawnerNil        = NewError("gateway agent spawner is nil", 104)
	ErrGatewayCreateNetworkServer    = NewError("gateway create network server failed", 105)
	ErrGatewayBuildClientAgent       = NewError("gateway build client agent failed", 106)
	ErrGatewaySpawnClientAgent       = NewError("gateway spawn client agent failed", 107)
	ErrGatewayNilClientAgent         = NewError("gateway client agent is nil", 108)
	ErrGatewayInvalidPushParams      = NewError("gateway invalid push params", 109)
	ErrGatewayConnectionUnavailable  = NewError("gateway connection is unavailable", 110)
	ErrGatewayInvalidMessageType     = NewError("gateway invalid message type", 111)

	ErrNodeIsNil         = NewError("node is nil", 150)
	ErrComponentNotStart = NewError("component no started", 151)
)

func WrapErrNetworkUnsupportedProtocol(proto string) error {
	return fmt.Errorf("%w: %s", ErrNetworkUnsupportedProtocol, proto)
}
