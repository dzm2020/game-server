package route

import (
	"actor"
	"fmt"
	"game-server/framework/runtime/protocol"
	"reflect"
	"sync"

	"google.golang.org/protobuf/proto"
)

type ISession interface {
	Context() actor.Context
	Entity() actor.Actor
	Message() *protocol.Message
}

type Session struct {
	ctx     actor.Context
	actor   actor.Actor
	message *protocol.Message
}

func (s *Session) Context() actor.Context {
	return s.ctx
}

func (s *Session) Entity() actor.Actor {
	return s.actor
}

func (s *Session) Message() *protocol.Message { return s.message }

type entry struct {
	handler Handler
	t       reflect.Type
}

type Handler func(session ISession, request interface{}) error
type AsyncHandler = Handler

type Registry struct {
	mu       sync.RWMutex
	handlers map[uint16]*entry
}

func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[uint16]*entry),
	}
}

func (r *Registry) Register(cmd, act uint8, handler Handler, request proto.Message) {
	r.mu.Lock()
	routeID := protocol.CmdAct(cmd, act)
	r.handlers[routeID] = &entry{
		handler: handler,
		t:       reflect.TypeOf(request),
	}
	r.mu.Unlock()
}

func (r *Registry) Handle(actor actor.Actor, ctx actor.Context, msg *protocol.Message) error {
	r.mu.RLock()
	e := r.handlers[protocol.CmdAct(msg.Cmd, msg.Act)]
	r.mu.RUnlock()
	if e == nil {
		return fmt.Errorf("route entry not found cmd:%d act:%d", msg.Cmd, msg.Act)
	}

	request := reflect.New(e.t).Interface()
	if err := proto.Unmarshal(msg.Data, request.(proto.Message)); err != nil {
		return err
	}

	return e.handler(&Session{actor: actor, ctx: ctx, message: msg}, request)
}
