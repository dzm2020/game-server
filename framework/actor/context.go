package actor

import (
	"game-server/framework/gen"
	"game-server/framework/grs"
	"sync"
	"time"

	"go.uber.org/zap"
)

type actorContext struct {
	self       *gen.PID
	system     *System
	initArgs   []any
	current    gen.ActorEnvelope
	actor      gen.IActor
	askTimeout time.Duration
	route      gen.IActorRoute
	logger     *zap.Logger
}

func noopStop() {}

func (c *actorContext) Self() *gen.PID {
	return c.self
}

func (c *actorContext) Sender() *gen.PID {
	return c.current.Sender
}

func (c *actorContext) InitArgs() []any {
	if len(c.initArgs) == 0 {
		return nil
	}
	return append([]any(nil), c.initArgs...)
}

func (c *actorContext) System() gen.ISystem {
	return c.system
}

func (c *actorContext) Ticker(interval time.Duration, task gen.ActorTask) (stop func()) {
	if interval <= 0 || task == nil {
		return noopStop
	}

	ticker := time.NewTicker(interval)
	stop, stopCh := newStopController(ticker.Stop)

	grs.SafeGo(func() {
		defer stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				if err := c.DoTask(c.self, task); err != nil {
					return
				}
			}
		}
	})

	return stop
}

func (c *actorContext) AfterFunc(delay time.Duration, task gen.ActorTask) (stop func()) {
	if delay <= 0 || task == nil {
		return noopStop
	}

	timer := time.NewTimer(delay)
	stop, stopCh := newStopController(func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	})

	grs.SafeGo(func() {
		defer stop()
		select {
		case <-stopCh:
			return
		case <-timer.C:
			_ = c.DoTask(c.self, task)
		}
	})

	return stop
}

func newStopController(cleanup func()) (func(), <-chan struct{}) {
	stopCh := make(chan struct{})
	var once sync.Once
	stop := func() {
		once.Do(func() {
			close(stopCh)
			if cleanup != nil {
				cleanup()
			}
		})
	}
	return stop, stopCh
}

func (c *actorContext) Tell(target any, msg *gen.Message) error {
	return c.system.Tell(c.self, target, msg)
}

func (c *actorContext) SetAskTimeout(timeout time.Duration) {
	c.askTimeout = timeout
}

func (c *actorContext) Ask(target any, msg *gen.Message) ([]byte, error) {
	return c.system.Ask(c.self, target, msg, c.askTimeout)
}

func (c *actorContext) DoTask(target any, task gen.ActorTask) error {
	return c.system.DoTask(c.self, target, task)
}

func (c *actorContext) Respond(v []byte) error {
	if c.current.Respond == nil {
		return gen.ErrActorNoResponder
	}
	return c.current.Respond(v)
}

func (c *actorContext) Actor() gen.IActor {
	return c.actor
}

func (c *actorContext) GetRouter() gen.IActorRoute {
	return c.route
}

func (c *actorContext) Exit() {
	c.system.StopProcess(c.self)
}

func (c *actorContext) Logger() *zap.Logger {
	return c.logger
}
