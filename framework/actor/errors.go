package actor

import "game-server/framework/gen"

var (
	ErrStopped         = gen.ErrActorProcessStopped
	ErrMailboxFull     = gen.ErrActorMailboxFull
	ErrAskTimeout      = gen.ErrActorAskTimeout
	ErrNoResponder     = gen.ErrActorNoResponder
	ErrSystemClosed    = gen.ErrActorSystemClosed
	ErrNilHandler      = gen.ErrActorNilHandler
	ErrActorNameExists = gen.ErrActorNameExists
	ErrActorIDConflict = gen.ErrActorIDConflict
	ErrActorNotFound   = gen.ErrActorNotFound
	ErrInvalidName     = gen.ErrActorInvalidName
	ErrInvalidTarget   = gen.ErrActorInvalidTarget
	ErrRemoteNotSet    = gen.ErrActorRemoteInvokerNotSet
)
