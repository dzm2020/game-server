package actor

import (
	"fmt"
	"game-server/framework/gen"
	"reflect"
	"sync"

	"google.golang.org/protobuf/proto"
)

type entry struct {
	handler gen.ActorRouteHandler
	t       reflect.Type
}

type Route struct {
	mu       sync.RWMutex
	handlers map[uint16]*entry
}

func NewRoute() *Route {
	return &Route{
		handlers: make(map[uint16]*entry),
	}
}

func (r *Route) Register(cmd, act uint8, handler gen.ActorRouteHandler, request proto.Message) {
	routeID := gen.CmdAct(cmd, act)
	var requestType reflect.Type
	if request != nil {
		requestType = reflect.TypeOf(request)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[routeID] = &entry{
		handler: handler,
		t:       requestType,
	}
}

func (r *Route) Handle(ctx gen.IContext, msg *gen.Message) error {
	e := r.get(msg.Cmd, msg.Act)
	if e == nil {
		return fmt.Errorf("%w cmd:%d act:%d", gen.ErrActorRouteNotFound, msg.Cmd, msg.Act)
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
	return r.get(cmd, act) != nil
}

func (r *Route) get(cmd, act uint8) *entry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.handlers[gen.CmdAct(cmd, act)]
}
