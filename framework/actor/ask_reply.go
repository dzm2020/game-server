package actor

import (
	"game-server/framework/gen"
	"sync"
	"time"
)

type askReply struct {
	ch   chan []byte
	once sync.Once
}

func newAskReply() *askReply {
	return &askReply{
		ch: make(chan []byte, 1),
	}
}

func (r *askReply) responder() gen.Responder {
	return func(v []byte) error {
		r.once.Do(func() {
			select {
			case r.ch <- v:
			default:
			}
		})
		return nil
	}
}

func (r *askReply) wait(timeout time.Duration) ([]byte, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case v := <-r.ch:
		return v, nil
	case <-timer.C:
		return nil, ErrAskTimeout
	}
}
