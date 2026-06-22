package actor

import (
	"context"
	"fmt"
	"game-server/framework/gen"
	"game-server/framework/grs"
	"sync"
	"sync/atomic"
	"time"

	"github.com/duke-git/lancet/v2/maputil"
)

type messageActorAdapter struct {
	fn gen.ActorHandler
}

func (h messageActorAdapter) OnInit(gen.IContext) error { return nil }

func (h messageActorAdapter) OnDestroy(gen.IContext) error { return nil }

func (h messageActorAdapter) OnMessage(ctx gen.IContext) error {
	if h.fn != nil {
		h.fn(ctx)
	}
	return nil
}

func (h messageActorAdapter) OnError(gen.IContext, any) error { return nil }

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
		nodeID:      nodeID,
	}
	s.runCtx, s.cancel = context.WithCancel(context.Background())
	return s
}

type System struct {
	processDict *maputil.ConcurrentMap[uint64, *process]
	nameDict    *maputil.ConcurrentMap[string, *process]
	nextID      atomic.Uint64
	closed      atomic.Bool
	closeOnce   sync.Once
	wg          sync.WaitGroup
	lifecycleMu sync.Mutex
	runCtx      context.Context
	cancel      context.CancelFunc
	nodeID      string
	invoker     gen.IRemoteInvoker
}

func (s *System) SetRemoteInvoker(invoker gen.IRemoteInvoker) {
	s.invoker = invoker
}

func (s *System) Spawn(handler gen.ActorHandler, opts gen.SpawnOptions) (*gen.PID, error) {
	if handler == nil {
		return gen.NoSender, ErrNilHandler
	}
	return s.SpawnActor(messageActorAdapter{fn: handler}, opts)
}

func (s *System) SpawnActor(handler gen.IActor, opts gen.SpawnOptions) (*gen.PID, error) {
	if handler == nil {
		return gen.NoSender, ErrNilHandler
	}

	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.closed.Load() {
		return gen.NoSender, ErrSystemClosed
	}

	id := s.nextID.Add(1)
	mailboxSize := 1024
	if opts.MailboxSize > 0 {
		mailboxSize = opts.MailboxSize
	}
	pid := gen.NewPID(id, opts.Name, s.nodeID)
	runCtx, cancel := context.WithCancel(s.runCtx)
	proc := &process{
		system:   s,
		pid:      pid,
		mailbox:  make(chan gen.ActorEnvelope, mailboxSize),
		initArgs: append([]any(nil), opts.InitArgs...),
		runCtx:   runCtx,
		cancel:   cancel,
		route:    opts.Route,
	}

	if err := s.addProcess(proc); err != nil {
		return gen.NoSender, err
	}

	s.wg.Add(1)
	grs.SafeGo(func() {
		defer s.wg.Done()
		proc.run(handler)
	})
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
	case *gen.PID:
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

func (s *System) Tell(from *gen.PID, target any, msg *gen.Message) error {
	if s.closed.Load() {
		return ErrSystemClosed
	}
	switch to := target.(type) {
	case *gen.PID:
		if to.NodeID == s.nodeID {
			return s.localTell(from, to, msg)
		} else {
			return s.remoteTell(from, to, msg)
		}
	default:
		return s.localTell(from, target, msg)
	}
}

func (s *System) localTell(from *gen.PID, target any, msg *gen.Message) error {
	proc, ok := s.GetProcess(target)
	if !ok {
		return ErrActorNotFound
	}
	return proc.send(gen.ActorEnvelope{
		Payload: msg,
		Sender:  from,
	})
}

func (s *System) remoteTell(from *gen.PID, to *gen.PID, msg *gen.Message) error {
	if s.invoker == nil {
		return ErrRemoteNotSet
	}
	return s.invoker.Tell(from, to, msg)
}

func (s *System) Ask(from *gen.PID, target any, msg *gen.Message, timeout time.Duration) ([]byte, error) {
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
	err := proc.send(gen.ActorEnvelope{
		Payload: msg,
		Sender:  from,
		Respond: reply.responder(),
	})
	if err != nil {
		return nil, err
	}
	return reply.wait(timeout)
}

func (s *System) SendEnvelope(target any, env gen.ActorEnvelope) (err error) {
	if s.closed.Load() {
		return ErrSystemClosed
	}
	proc, ok := s.GetProcess(target)
	if !ok {
		return ErrActorNotFound
	}
	return proc.send(env)
}

func (s *System) StopProcess(target any) {
	proc, ok := s.GetProcess(target)
	if !ok {
		return
	}
	proc.requestStop()
}

func (s *System) Shutdown() {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		s.cancel()
		s.wg.Wait()
	})
}
