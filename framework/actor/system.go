package actor

import (
	"context"
	"game-server/framework/gen"
	"game-server/framework/grs"
	"game-server/framework/pkg/component"
	"game-server/framework/pkg/glog"

	"sync/atomic"
	"time"

	"github.com/duke-git/lancet/v2/maputil"
	"go.uber.org/zap"
)

func NewSystem(node gen.INode) *System {
	return &System{
		processDict: maputil.NewConcurrentMap[uint64, *process](0),
		nameDict:    maputil.NewConcurrentMap[string, *process](0),
		node:        node,
		waitGroup:   grs.NewGroup(context.Background()),
	}
}

var _ gen.ISystem = (*System)(nil)
var _ component.IComponent = (*System)(nil)

type System struct {
	component.BaseComponent
	node        gen.INode
	processDict *maputil.ConcurrentMap[uint64, *process]
	nameDict    *maputil.ConcurrentMap[string, *process]
	nextID      atomic.Uint64
	waitGroup   *grs.Group
}

func (s *System) Spawn(handler gen.ActorHandler, opts gen.SpawnOptions) (*gen.PID, error) {
	if handler == nil {
		return gen.NoSender, gen.ErrActorNilHandler
	}
	return s.SpawnActor(messageActorAdapter{fn: handler}, opts)
}

func (s *System) SpawnActor(handler gen.IActor, opts gen.SpawnOptions) (*gen.PID, error) {
	if s.Status() != component.LifecycleStateStarted {
		return gen.NoSender, gen.ErrComponentNotStart
	}
	if handler == nil {
		return gen.NoSender, gen.ErrActorNilHandler
	}
	opts = gen.NormalizationSpawnOptions(opts)
	if err := gen.ValidateSpawnOptions(opts); err != nil {
		return gen.NoSender, err
	}

	id := s.nextID.Add(1)
	pid := gen.NewPID(id, opts.Name, s.node.GetId())

	ctx := &actorContext{
		self:       pid,
		system:     s,
		initArgs:   opts.InitArgs,
		actor:      handler,
		askTimeout: time.Second * 3,
		route:      opts.Route,
		logger:     glog.GetLogger().With(gen.FieldComponent(actorProcessComponent), zap.String("pid", pid.String())),
	}

	proc := &process{
		system:  s,
		ctx:     ctx,
		mailbox: newMailbox(opts.MailboxSize, ctx),
	}

	if err := s.addProcess(proc); err != nil {
		return gen.NoSender, err
	}
	s.waitGroup.Go(func(ctx context.Context) {
		proc.mailbox.run()
	})
	return pid, nil
}

func (s *System) addProcess(proc *process) error {
	if proc.getName() != "" {
		if _, exists := s.nameDict.GetOrSet(proc.getName(), proc); exists {
			return gen.ErrActorNameExists
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

func (s *System) getProcess(target any) (*process, bool) {
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
	if to, ok := target.(*gen.PID); ok && to.NodeID != s.node.GetId() {
		if s.Status() == component.LifecycleStateStopped {
			return gen.ErrActorSystemClosed
		}
		return s.remoteTell(from, to, msg)
	}
	return s.localTell(from, target, msg)
}

func (s *System) localTell(from *gen.PID, target any, msg *gen.Message) error {
	return s.SendEnvelope(target, gen.ActorEnvelope{
		Payload: msg,
		Sender:  from,
	})
}

func (s *System) remoteTell(from *gen.PID, to *gen.PID, msg *gen.Message) error {
	cluster := s.node.GetCluster()
	if cluster == nil {
		return gen.ErrClusterNil
	}
	return cluster.SendToNode(from, to, msg)
}

func (s *System) Ask(from *gen.PID, target any, msg *gen.Message, timeout time.Duration) ([]byte, error) {
	if timeout <= 0 {
		timeout = gen.DefaultActorAskTimeout
	}
	reply := newAskReply()
	err := s.SendEnvelope(target, gen.ActorEnvelope{
		Payload: msg,
		Sender:  from,
		Respond: reply.responder(),
	})
	if err != nil {
		return nil, err
	}
	resp, err := reply.wait(timeout)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *System) DoTask(from *gen.PID, target any, task gen.ActorTask) error {
	if s.Status() != component.LifecycleStateStarted {
		return gen.ErrComponentNotStart
	}
	return s.SendEnvelope(target, gen.ActorEnvelope{
		Sender:  from,
		Payload: task,
	})
}

func (s *System) SendEnvelope(target any, env gen.ActorEnvelope) (err error) {
	proc, err := s.getActiveProcess(target)
	if err != nil {
		return err
	}
	return proc.push(env)
}

func (s *System) getActiveProcess(target any) (*process, error) {
	proc, ok := s.getProcess(target)
	if !ok {
		return nil, gen.ErrActorNotFound
	}
	return proc, nil
}

func (s *System) StopProcess(target any) {
	proc, ok := s.getProcess(target)
	if !ok {
		return
	}
	proc.stop()
}

func (s *System) Stop(ctx context.Context) error {
	return s.BaseComponent.GuardStop(ctx, func(ctx context.Context) error {
		if ctx == nil {
			ctx = context.Background()
		}
		var list []*process
		s.processDict.Range(func(key uint64, p *process) bool {
			list = append(list, p)
			return true
		})
		for _, p := range list {
			p.stop()
		}
		s.waitGroup.Cancel()
		return s.waitGroup.Wait(ctx)
	})
}
