package actor

import (
	"game-server/framework/gen"
	"game-server/framework/grs"
	"sync"
	"time"
)

type actorContext struct {
	self       *gen.PID
	system     *System
	initArgs   []any
	done       <-chan struct{}
	current    gen.ActorEnvelope
	actor      gen.IActor
	askTimeout time.Duration
	route      gen.IActorRoute
}

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

func (c *actorContext) Done() <-chan struct{} {
	return c.done
}

func (c *actorContext) Ticker(interval time.Duration, task gen.ActorTask) (stop func()) {
	if interval <= 0 || task == nil {
		return func() {}
	}
	//  todo 后续可以替换成异步定时器，这样就不需要每个timer启动一个协程了
	ticker := time.NewTicker(interval)
	stopCh := make(chan struct{})
	var once sync.Once

	stop = func() {
		once.Do(func() {
			close(stopCh)
			ticker.Stop()
		})
	}

	grs.SafeGo(func() {
		defer stop()
		for {
			select {
			case <-c.done:
				return
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
		return func() {}
	}
	//  todo 后续可以替换成异步定时器，这样就不需要每个timer启动一个协程了
	timer := time.NewTimer(delay)
	stopCh := make(chan struct{})
	var once sync.Once

	stop = func() {
		once.Do(func() {
			close(stopCh)
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		})
	}

	grs.SafeGo(func() {
		defer stop()
		select {
		case <-c.done:
			return
		case <-stopCh:
			return
		case <-timer.C:
			_ = c.DoTask(c.self, task)
		}
	})

	return stop
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
		return ErrNoResponder
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
