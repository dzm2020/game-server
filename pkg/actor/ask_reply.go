package actor

import (
	"sync"
	"time"
)

type askReply struct {
	ch   chan any
	once sync.Once
}

func newAskReply() *askReply {
	return &askReply{
		ch: make(chan any, 1),
	}
}

func (r *askReply) responder() Responder {
	return func(v any) error {
		r.once.Do(func() {
			select {
			case r.ch <- v:
			default:
			}
		})
		return nil
	}
}

func (r *askReply) wait(timeout time.Duration) (any, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case v := <-r.ch:
		return v, nil
	case <-timer.C:
		return nil, ErrAskTimeout
	}
}
