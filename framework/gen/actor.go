package gen

import (
	"time"

	"google.golang.org/protobuf/proto"
)

type ActorTask func(ctx IContext) error

type Responder func(v []byte) error

type ActorEnvelope struct {
	Payload any
	Sender  *PID
	Respond Responder
}

// ISystem 抽象 System 的对外公共能力，便于依赖注入与单元测试替身。
type ISystem interface {
	Spawn(handler ActorHandler, opts SpawnOptions) (*PID, error)
	SpawnActor(handler IActor, opts SpawnOptions) (*PID, error)
	SetRemoteInvoker(invoker IRemoteInvoker)
	Tell(from *PID, target any, msg *Message) error
	Ask(from *PID, target any, msg *Message, timeout time.Duration) ([]byte, error)
	DoTask(from *PID, target any, task ActorTask) error
	SendEnvelope(target any, env ActorEnvelope) error
	StopProcess(target any)
	Shutdown()
}

type ActorHandler func(IContext)

type IActor interface {
	OnInit(IContext) error
	OnDestroy(IContext) error
	OnMessage(IContext) error
	OnError(IContext, any) error
}

type IContext interface {
	Self() *PID
	Sender() *PID
	InitArgs() []any
	System() ISystem
	Done() <-chan struct{}
	Tell(target any, msg *Message) error
	SetAskTimeout(timeout time.Duration)
	Ask(target any, msg *Message) ([]byte, error)
	Respond(v []byte) error
	Actor() IActor
	GetRouter() IActorRoute
	DoTask(target any, task ActorTask) error
}

type BaseActor struct {
}

func (h *BaseActor) OnInit(IContext) error { return nil }

func (h *BaseActor) OnDestroy(IContext) error { return nil }

func (h *BaseActor) OnMessage(ctx IContext) error { return nil }

func (h *BaseActor) OnError(IContext, any) error { return nil }

type SpawnOptions struct {
	Name        string
	MailboxSize int
	InitArgs    []any
	Route       IActorRoute
}

type IActorRoute interface {
	Register(cmd, act uint8, handler ActorRouteHandler, request proto.Message)
	Handle(ctx IContext, msg *Message) error
	Exist(cmd, act uint8) bool
}

type ActorRouteHandler func(ctx IContext, request interface{}) error

var NoSender = &PID{}

func NewPID(actorID uint64, actorName, nodeID string) *PID {
	return &PID{
		ActorID:   actorID,
		ActorName: actorName,
		NodeID:    nodeID,
	}
}

func (p *PID) IsZero() bool {
	return p.ActorID == 0 && p.ActorName == "" && p.NodeID == ""
}
