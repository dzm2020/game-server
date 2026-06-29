package actor

import (
	"fmt"
	"game-server/framework/gen"
	"game-server/framework/pkg/glog"
	"reflect"

	"github.com/duke-git/lancet/v2/maputil"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type entry struct {
	handler gen.ActorRouteHandler
	t       reflect.Type
}

type Route struct {
	handlers *maputil.ConcurrentMap[uint16, *entry]
}

func NewRoute() *Route {
	return &Route{
		handlers: maputil.NewConcurrentMap[uint16, *entry](1),
	}
}

// Register
//
//	@Description: 注册消息
//	@receiver r
//	@param cmd
//	@param act
//	@param handler
//	@param request
func (r *Route) Register(cmd, act uint8, handler gen.ActorRouteHandler, request proto.Message) {
	if r.Exist(cmd, act) {
		glog.Warn("Message duplicate registration", zap.Uint8("cmd", cmd), zap.Uint8("act", act))
		return
	}

	routeID := gen.CmdAct(cmd, act)
	var requestType reflect.Type
	if request != nil {
		requestType = reflect.TypeOf(request)
	}

	r.handlers.Set(routeID, &entry{
		handler: handler,
		t:       requestType,
	})
}

// Handle
//
//	@Description: 执行消息回调
//	@receiver r
//	@param ctx
//	@param msg
//	@return error
func (r *Route) Handle(ctx gen.IContext, msg *gen.Message) error {
	e, _ := r.handlers.Get(gen.CmdAct(msg.Cmd, msg.Act))
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
	_, ok := r.handlers.Get(gen.CmdAct(cmd, act))
	return ok
}
