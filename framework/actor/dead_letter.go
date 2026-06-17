package actor

import "time"

type DeadLetter struct {
	Time    time.Time
	From    PID
	Target  any
	Message any
	Error   error
}
