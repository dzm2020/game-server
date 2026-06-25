package gen

import (
	"time"
)

type IRemoteInvoker interface {
	Tell(from *PID, target *PID, msg *Message) error
	Ask(from *PID, target any, msg *Message, timeout time.Duration) ([]byte, error)
}

type ILocalInvoker interface {
	Handler(from *PID, target *PID, msg *Message) error
}
