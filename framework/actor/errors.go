package actor

import "errors"

var (
	ErrStopped         = errors.New("process is stopped")
	ErrMailboxFull     = errors.New("process mailbox is full")
	ErrAskTimeout      = errors.New("ask timeout")
	ErrNoResponder     = errors.New("message is not ask-able")
	ErrSystemClosed    = errors.New("system is closed")
	ErrNilHandler      = errors.New("process handler is nil")
	ErrActorNameExists = errors.New("process name already exists")
	ErrActorIDConflict = errors.New("process id already exists")
	ErrActorNotFound   = errors.New("process not found")
	ErrInvalidName     = errors.New("invalid process name")
	ErrInvalidTarget   = errors.New("invalid message target")
)
