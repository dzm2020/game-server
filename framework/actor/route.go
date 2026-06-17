package actor

import (
	"fmt"
	"game-server/framework/protocol"
	"reflect"
	"sync"

	"google.golang.org/protobuf/proto"
)

type entry struct {
	handler RouteHandler
	t       reflect.Type
}

type RouteHandler func(ctx Context, request interface{}) error

type Route struct {
	mu       sync.RWMutex
	handlers map[uint16]*entry
}

func NewRoute() *Route {
	return &Route{
		handlers: make(map[uint16]*entry),
	}
}

func (r *Route) Register(cmd, act uint8, handler RouteHandler, request proto.Message) {
	r.mu.Lock()
	routeID := protocol.CmdAct(cmd, act)
	var requestType reflect.Type
	if request != nil {
		requestType = reflect.TypeOf(request)
	}
	r.handlers[routeID] = &entry{
		handler: handler,
		t:       requestType,
	}
	r.mu.Unlock()
}

func (r *Route) Handle(ctx Context, msg *protocol.Message) error {
	r.mu.RLock()
	e := r.handlers[protocol.CmdAct(msg.Cmd, msg.Act)]
	r.mu.RUnlock()
	if e == nil {
		return fmt.Errorf("route entry not found cmd:%d act:%d", msg.Cmd, msg.Act)
	}

	var request interface{}
	if e.t == nil {
		request = msg
	} else {
		request = reflect.New(e.t.Elem()).Interface()
		if err := proto.Unmarshal(msg.Data, request.(proto.Message)); err != nil {
			return err
		}
	}

	return e.handler(ctx, request)
}

func (r *Route) Exist(cmd, act uint8) bool {
	r.mu.RLock()
	_, ok := r.handlers[protocol.CmdAct(cmd, act)]
	r.mu.RUnlock()
	return ok
}
