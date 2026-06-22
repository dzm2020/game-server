package node

import (
	"context"
	"errors"
	"game-server/framework/actor"
	"game-server/framework/cluster"
	"game-server/framework/gen"
	"game-server/framework/pkg/component"
	"game-server/framework/pkg/glog"
	"game-server/framework/registry"
	"reflect"

	"os"
	"os/signal"
	"syscall"

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

// New creates a node instance.
func New(options gen.NodeOptions) *Node {
	node := &Node{
		options:  options,
		IManager: component.NewComponentsMgr(),
	}
	node.init(options)
	return node
}

type Node struct {
	options gen.NodeOptions
	component.IManager
	components []component.IComponent

	cluster  gen.ICluster
	system   gen.ISystem
	registry gen.IRegistry
}

func (n *Node) init(options gen.NodeOptions) {
	options = gen.NormalizationNodeOptions(options)

	logOptions := []zap.Option{
		zap.WithPanicHook(panicHook{
			fn: func(entry *zapcore.CheckedEntry, fields []zapcore.Field) {
				n.options.Behavior.OnPanic(n, errors.New(entry.Message))
			},
		}),
		zap.Fields(
			zap.String("nodeId", options.ID),
			zap.String("nodeName", options.Name),
		),
	}

	glog.Init(options.Logger, logOptions...)

	compRegistry := registry.NewComponent(n)

	compCluster := cluster.NewComponent(n)

	compSystem := actor.NewComponent(n)

	n.AddComponents(compRegistry, compSystem, compCluster)

	n.cluster = compCluster
	n.registry = compRegistry
	n.system = compSystem

	n.options.Behavior.OnInit(n)
}

func (n *Node) GetId() string {
	return n.options.ID
}

func (n *Node) GetExtAddress() string {
	return n.options.ExtAddress
}

func (n *Node) GetRpcAddress() string {
	return n.options.RpcAddress
}

func (n *Node) GetName() string {
	return n.options.Name
}

func (n *Node) GetOptions() *gen.NodeOptions {
	return &n.options
}

func (n *Node) GetRegistry() gen.IRegistry {
	return n.registry
}

func (n *Node) GetSystem() gen.ISystem {
	return n.system
}

func (n *Node) GetCluster() gen.ICluster {
	return n.cluster
}

func (n *Node) AddComponents(comps ...component.IComponent) {
	n.components = append(n.components, comps...)
}

func (n *Node) Startup() (err error) {
	if err = n.Start(context.Background()); err != nil {
		return err
	}

	//  todo 补充日志打印节点信息 格式可以组织称这样
	//  clog.Info("-------------------------------------------------")
	//	clog.Infof("[nodeID      = %s] application is starting...", a.NodeID())
	//	clog.Infof("[nodeType    = %s]", a.NodeType())
	//	clog.Infof("[pid         = %d]", os.Getpid())
	//	clog.Infof("[startTime   = %s]", a.StartTime())
	//	clog.Infof("[profilePath = %s]", cprofile.Path())
	//	clog.Infof("[profileName = %s]", cprofile.Name())
	//	clog.Infof("[env         = %s]", cprofile.Env())
	//	clog.Infof("[debug       = %v]", cprofile.Debug())
	//	clog.Infof("[printLevel  = %s]", cprofile.PrintLevel())
	//	clog.Infof("[logLevel    = %s]", clog.DefaultLogger.LogLevel)
	//	clog.Infof("[stackLevel  = %s]", clog.DefaultLogger.StackLevel)
	//	clog.Infof("[writeFile   = %v]", clog.DefaultLogger.EnableWriteFile)
	//	clog.Infof("[serializer  = %s]", a.serializer.Name())
	//	clog.Info("-------------------------------------------------")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGKILL, syscall.SIGTERM)
	glog.Info("节点等待终止信号", zap.Strings("signals", []string{"SIGINT", "SIGQUIT", "SIGKILL", "SIGTERM"}))
	sig := <-sigChan
	glog.Info("节点停止运行", zap.String("signal", sig.String()))
	return n.Stop(context.Background())
}

func (n *Node) Start(ctx context.Context) (err error) {
	glog.Info("节点启动", zap.Int("component_count", len(n.components)))

	n.options.Behavior.OnBeforeStart(n)

	for _, comp := range n.components {
		if err := n.IManager.AddComponent(comp); err != nil {
			glog.Error("节点添加组件失败", zap.Error(err), zap.String("typ", reflect.TypeOf(comp).String()))
			return err
		}
	}
	if err = n.IManager.Start(ctx); err != nil {
		glog.Error("节点启动组件失败", zap.Error(err), zap.Error(err))
		return err
	}

	n.options.Behavior.OnAfterStart(n)

	glog.Info("节点启动完成")
	return nil
}

// Stop gracefully stops the node and all components.
func (n *Node) Stop(ctx context.Context) error {
	glog.Info("节点开始停止")

	n.options.Behavior.OnBeforeStop(n)

	stopErr := n.IManager.Stop(ctx)
	if stopErr != nil {
		glog.Error("节点组件停止错误", zap.Error(stopErr))
	}

	n.options.Behavior.OnAfterStop(n, stopErr)

	glog.Info("节点完成停止", zap.Error(stopErr))
	_ = glog.Stop()
	return stopErr
}
