package gen

import (
	"context"
	"fmt"
	"game-server/framework/pkg/component"
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

type ILocalInvoker interface {
	Tell(from *PID, target any, msg *Message) error
}

type ISystem interface {
	component.IComponent
	ILocalInvoker
	SetNodeID(nodeID string)
	SetRemoteInvoker(invoker IRemoteInvoker)
	Spawn(handler ActorHandler, opts SpawnOptions) (*PID, error)
	SpawnActor(handler IActor, opts SpawnOptions) (*PID, error)
	Ask(from *PID, target any, msg *Message, timeout time.Duration) ([]byte, error)
	DoTask(from *PID, target any, task ActorTask) error
	SendEnvelope(target any, env ActorEnvelope) error
	StopProcess(target any)
	Stop(ctx context.Context) error
}

type ActorHandler func(IContext)

type IActor interface {
	OnInit(IContext) error
	OnDestroy(IContext) error
	OnMessage(IContext) error
	OnError(IContext, any)
}

type IContext interface {
	Self() *PID
	Sender() *PID
	InitArgs() []any
	System() ISystem
	Ticker(interval time.Duration, task ActorTask) (stop func())
	AfterFunc(delay time.Duration, task ActorTask) (stop func())
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

func (h *BaseActor) OnError(IContext, any) {}

type SpawnOptions struct {
	Name        string
	MailboxSize int
	InitArgs    []any
	Route       IActorRoute
}

const (
	defaultActorMailboxSize = 1024
	DefaultActorAskTimeout  = 3 * time.Second
)

// NormalizationSpawnOptions 统一补齐 SpawnOptions 默认值。
func NormalizationSpawnOptions(opts SpawnOptions) SpawnOptions {
	if opts.MailboxSize <= 0 {
		opts.MailboxSize = defaultActorMailboxSize
	}
	return opts
}

// ValidateSpawnOptions 校验 SpawnOptions 是否满足运行约束。
func ValidateSpawnOptions(opts SpawnOptions) error {
	if opts.MailboxSize <= 0 {
		return fmt.Errorf("invalid actor mailbox size: %d", opts.MailboxSize)
	}
	return nil
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
