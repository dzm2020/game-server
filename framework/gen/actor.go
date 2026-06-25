package gen

import (
	"context"
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
//
// 生命周期命名冻结约定：
// - Start：启动长期运行组件；要求幂等，且不得长期阻塞。
// - Stop：优雅停止长期运行组件；要求幂等，并阻塞等待停止完成或 ctx.Done。
// - Shutdown：仅用于“服务端入口”级别优雅退出，语义等同 Stop。
// - Close：仅用于“单个资源句柄”关闭，调用后资源不可继续使用。
type ISystem interface {
	Spawn(handler ActorHandler, opts SpawnOptions) (*PID, error)
	SpawnActor(handler IActor, opts SpawnOptions) (*PID, error)
	SetRemoteInvoker(invoker IRemoteInvoker)
	Tell(from *PID, target any, msg *Message) error
	Ask(from *PID, target any, msg *Message, timeout time.Duration) ([]byte, error)
	DoTask(from *PID, target any, task ActorTask) error
	SendEnvelope(target any, env ActorEnvelope) error
	StopProcess(target any)
	// Stop 优雅停止 Actor 系统。
	// 幂等：可重复调用，仅第一次执行真实停止流程；后续调用返回与首次一致的结果。
	// 阻塞：阻塞直到所有 Actor 退出完成，或 ctx.Done。
	// 返回：完全停止返回 nil；若被 ctx 取消/超时或停止过程失败，返回 error。
	Stop(ctx context.Context) error
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
