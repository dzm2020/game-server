package gen

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
	ErrActorProcessStopped      = NewError("actor process is stopped", 1)
	ErrActorMailboxFull         = NewError("actor mailbox is full", 2)
	ErrActorAskTimeout          = NewError("actor ask timeout", 3)
	ErrActorNoResponder         = NewError("actor message is not ask-able", 4)
	ErrActorSystemClosed        = NewError("actor system is closed", 5)
	ErrActorNilHandler          = NewError("actor handler is nil", 6)
	ErrActorNameExists          = NewError("actor name already exists", 7)
	ErrActorIDConflict          = NewError("actor id already exists", 8)
	ErrActorNotFound            = NewError("actor not found", 9)
	ErrActorInvalidName         = NewError("invalid actor name", 10)
	ErrActorInvalidTarget       = NewError("invalid actor target", 11)
	ErrActorRemoteInvokerNotSet = NewError("actor remote invoker is not set", 12)
	ErrActorRouteNotFound       = NewError("actor route entry not found", 13)
	ErrActorClusterNil          = NewError("actor cluster is nil", 14)
	ErrActorClusterAskNotImpl   = NewError("actor cluster ask is not implemented", 15)

	// network
	ErrCodecNotConfigured  = NewError("codec is not configured", 22)
	ErrConnectionClosed    = NewError("connection is closed", 23)
	ErrTLSCertFileRequired = NewError("tls cert file is required", 24)
	ErrTLSKeyFileRequired  = NewError("tls key file is required", 25)

	// cluster / registry
	ErrClusterSystemNil            = NewError("cluster system is nil", 26)
	ErrClusterNil                  = NewError("cluster is nil", 27)
	ErrClusterClosed               = NewError("cluster is closed", 28)
	ErrClusterWaitPeerReadyTimeout = NewError("cluster wait peers ready timeout", 29)
	ErrClusterNodeNotFound         = NewError("cluster node not found", 30)
	ErrClusterNodeNotInServiceList = NewError("cluster node not in service list", 31)
	ErrClusterNodeNotConnected     = NewError("cluster node not connected", 32)
	ErrClusterDecodeFailed         = NewError("cluster decode failed", 33)
	ErrClusterPeerNotConnected     = NewError("cluster peer not connected", 34)
	ErrClusterSendChannelFull      = NewError("cluster send channel full", 35)
	ErrRegistryNil                 = NewError("registry is nil", 36)
	ErrConsulInvalidServiceReg     = NewError("consul invalid service registration", 37)
	ErrConsulServiceIDRequired     = NewError("consul service id is required", 38)
	ErrConsulTTLHeartbeatNotRun    = NewError("consul ttl heartbeat not running", 39)
	ErrNodeNil                     = NewError("node is nil", 40)
	ErrNodeComponentNotRegistered  = NewError("node component not registered", 41)
	ErrNodeComponentTypeMismatched = NewError("node component type mismatch", 42)
)
