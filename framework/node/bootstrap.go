package node

import (
	"fmt"
	"game-server/framework/actor"
	grpc_cluster "game-server/framework/cluster/grpc"
	"game-server/framework/gen"
	"game-server/framework/pkg/component"
	"game-server/framework/pkg/glog"
	"game-server/framework/registry/consul"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type panicHook struct {
	fn func(entry *zapcore.CheckedEntry, fields []zapcore.Field)
}

func (h panicHook) OnWrite(entry *zapcore.CheckedEntry, fields []zapcore.Field) {
	if h.fn != nil {
		h.fn(entry, fields)
	}
}

func (n *Node) bootstrap(options gen.NodeOptions) {
	options = gen.NormalizeNodeOptions(options)
	if err := gen.ValidateNodeOptions(options); err != nil {
		panic(err)
	}
	n.options = options
	initLogger(n, options)

	n.registry = defaultFactory(options.Factories.Registry, func() gen.IRegistry { return consul.New() })
	n.cluster = defaultFactory(options.Factories.Cluster, func() gen.ICluster { return grpc_cluster.New() })
	n.system = defaultFactory(options.Factories.System, func() gen.ISystem { return actor.NewSystem() })
}

func initLogger(n *Node, options gen.NodeOptions) {
	logOptions := []zap.Option{
		zap.WithPanicHook(panicHook{
			fn: func(entry *zapcore.CheckedEntry, fields []zapcore.Field) {
				n.options.Behavior.OnPanic(n, fmt.Errorf("%s", entry.Message))
			},
		}),
		zap.Fields(
			gen.FieldNodeID(options.ID),
			gen.FieldComponent("node"),
			zap.String("node_name", options.Name),
		),
	}
	glog.Init(options.Logger, logOptions...)
}

func defaultFactory[T any](factory func() T, fallback func() T) T {
	if factory != nil {
		return factory()
	}
	return fallback()
}

func guardComponentReplace(current component.IComponent) error {
	if current != nil && current.Status() == component.StateStarted {
		return component.ErrCannotRegisterComponentAfterStarted
	}
	return nil
}
