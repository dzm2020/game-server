package actor

import (
	"context"
	"fmt"
	"game-server/framework/protocol"
	"sync"
	"sync/atomic"
	"time"

	"github.com/duke-git/lancet/v2/maputil"
)

// ISystem 抽象 System 的对外公共能力，便于依赖注入与单元测试替身。
type ISystem interface {
	Spawn(handler Handler, opts ...Option) (PID, error)
	SpawnActor(handler IActor, opts ...Option) (PID, error)
	Tell(from PID, target any, msg *protocol.Message) error
	Ask(from PID, target any, msg *protocol.Message, timeout time.Duration) ([]byte, error)
	SendEnvelope(target any, env Envelope) error
	Shutdown()
}

func NewSystem() *System {
	return NewSystemWithNodeID("")
}

func NewSystemWithNodeID(nodeID string) *System {
	if nodeID == "" {
		nodeID = "local"
	}
	s := &System{
		processDict: maputil.NewConcurrentMap[uint64, *process](0),
		nameDict:    maputil.NewConcurrentMap[string, *process](0),
		deadLetters: make(chan DeadLetter, 1024),
		nodeID:      nodeID,
	}
	s.runCtx, s.cancel = context.WithCancel(context.Background())
	return s
}

type System struct {
	processDict *maputil.ConcurrentMap[uint64, *process]
	nameDict    *maputil.ConcurrentMap[string, *process]
	deadLetters chan DeadLetter
	nextID      atomic.Uint64
	closed      atomic.Bool
	closeOnce   sync.Once
	wg          sync.WaitGroup
	lifecycleMu sync.Mutex
	runCtx      context.Context
	cancel      context.CancelFunc
	nodeID      string
}

var _ ISystem = (*System)(nil)

func (s *System) Spawn(handler Handler, opts ...Option) (PID, error) {
	if handler == nil {
		return NoSender, ErrNilHandler
	}
	return s.SpawnActor(messageActorAdapter{fn: handler}, opts...)
}

func (s *System) SpawnActor(handler IActor, opts ...Option) (PID, error) {
	if handler == nil {
		return NoSender, ErrNilHandler
	}

	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.closed.Load() {
		return NoSender, ErrSystemClosed
	}

	cfg := applyOptions(opts...)

	id := s.nextID.Add(1)

	pid := NewPID(id, cfg.name, s.nodeID)
	runCtx, cancel := context.WithCancel(s.runCtx)
	proc := &process{
		system:   s,
		pid:      pid,
		mailbox:  make(chan Envelope, cfg.mailboxSize),
		initArgs: append([]any(nil), cfg.initArgs...),
		runCtx:   runCtx,
		cancel:   cancel,
		route:    cfg.route,
	}

	if err := s.addProcess(proc); err != nil {
		return NoSender, err
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		proc.run(handler)
	}()
	return pid, nil
}

func (s *System) addProcess(proc *process) error {
	if proc.getName() != "" {
		if _, exists := s.nameDict.GetOrSet(proc.getName(), proc); exists {
			return fmt.Errorf("%w: %s", ErrActorNameExists, proc.getName())
		}
	}
	s.processDict.GetOrSet(proc.getPID().ActorID, proc)
	return nil
}

func (s *System) removeProcess(proc *process) {
	if proc == nil {
		return
	}
	s.processDict.Delete(proc.getPID().ActorID)
	s.nameDict.Delete(proc.getName())
}

func (s *System) GetProcess(target any) (*process, bool) {
	switch v := target.(type) {
	case PID:
		if v.ActorID != 0 {
			return s.processDict.Get(v.ActorID)
		}
		if v.ActorName != "" {
			return s.nameDict.Get(v.ActorName)
		}
		return nil, false
	case string:
		return s.nameDict.Get(v)
	default:
		return nil, false
	}
}

func (s *System) Tell(from PID, target any, msg *protocol.Message) error {
	if s.closed.Load() {
		return ErrSystemClosed
	}
	proc, ok := s.GetProcess(target)
	if !ok {
		return ErrActorNotFound
	}
	return proc.send(Envelope{
		Payload: msg,
		Sender:  from,
	})
}

func (s *System) Ask(from PID, target any, msg *protocol.Message, timeout time.Duration) ([]byte, error) {
	if s.closed.Load() {
		return nil, ErrSystemClosed
	}
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	proc, ok := s.GetProcess(target)
	if !ok {
		return nil, ErrActorNotFound
	}

	reply := newAskReply()
	err := proc.send(Envelope{
		Payload: msg,
		Sender:  from,
		Respond: reply.responder(),
	})
	if err != nil {
		return nil, err
	}
	return reply.wait(timeout)
}

func (s *System) SendEnvelope(target any, env Envelope) (err error) {
	if s.closed.Load() {
		return ErrSystemClosed
	}
	proc, ok := s.GetProcess(target)
	if !ok {
		return ErrActorNotFound
	}
	return proc.send(env)
}

func (s *System) Shutdown() {
	s.closeOnce.Do(func() {
		s.lifecycleMu.Lock()
		s.closed.Store(true)
		close(s.deadLetters)
		s.cancel()
		s.wg.Wait()
	})
}
