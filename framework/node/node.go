package node

import (
	"context"
	"fmt"
	"game-server/framework/actor"

	"game-server/framework/cluster/grpc"
	"game-server/framework/gen"

	"game-server/framework/pkg/component"
	"game-server/framework/pkg/glog"

	"game-server/framework/registry/consul"
	"reflect"

	"os"
	"os/signal"
	"sync"
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
	addOnce    sync.Once
	addErr     error

	cluster  gen.ICluster
	system   gen.ISystem
	registry gen.IRegistry
}

func (n *Node) init(options gen.NodeOptions) {
	options = gen.NormalizeNodeOptions(options)
	if err := gen.ValidateNodeOptions(options); err != nil {
		panic(err)
	}
	n.options = options

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
	if n.options.Registry == nil {
		n.options.Registry = consul.New(n)
	}

	if n.options.Cluster == nil {
		n.options.Cluster = grpc_cluster.New(n)
	}

	glog.Init(options.Logger, logOptions...)

	system := actor.NewSystem(n)

	n.AddComponents(options.Registry, system, options.Cluster)

	n.cluster = options.Cluster
	n.registry = options.Registry
	n.system = system

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
	var names []string
	n.IManager.Range(func(component component.IComponent) {
		names = append(names, reflect.TypeOf(component).String())
	})

	glog.Info("节点启动信息",
		gen.FieldComponent("node"),
		gen.FieldNodeID(n.GetId()),
		zap.String("ext_address", n.GetExtAddress()),
		zap.String("rpc_address", n.GetRpcAddress()),
		gen.FieldPID(os.Getpid()),
		zap.Int("component_count", len(n.components)),
		zap.Strings("clusters", n.options.Clusters),
		zap.Strings("components", names),
	)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	defer signal.Stop(sigChan)
	glog.Info("节点等待终止信号", gen.FieldComponent("node"), gen.FieldNodeID(n.GetId()), zap.Strings("signals", []string{"SIGINT", "SIGQUIT", "SIGTERM"}))
	sig := <-sigChan
	glog.Info("节点停止运行", gen.FieldComponent("node"), gen.FieldNodeID(n.GetId()), zap.String("signal", sig.String()))
	return n.Stop(context.Background())
}

func (n *Node) addComponentsToManager() error {
	n.addOnce.Do(func() {
		for _, comp := range n.components {
			if err := n.IManager.AddComponent(comp); err != nil {
				glog.Error("节点添加组件失败", gen.FieldComponent("node"), gen.FieldNodeID(n.GetId()), gen.FieldErr(err), zap.String("typ", reflect.TypeOf(comp).String()))
				n.addErr = err
				return
			}
		}
	})
	return n.addErr
}

func (n *Node) Start(ctx context.Context) (err error) {
	glog.Info("节点启动", gen.FieldComponent("node"), gen.FieldNodeID(n.GetId()), zap.Int("component_count", len(n.components)))

	n.options.Behavior.OnBeforeStart(n)

	if err := n.addComponentsToManager(); err != nil {
		return err
	}

	if err = n.IManager.Start(ctx); err != nil {
		glog.Error("节点启动组件失败", gen.FieldComponent("node"), gen.FieldNodeID(n.GetId()), gen.FieldErr(err))
		return err
	}

	n.options.Behavior.OnAfterStart(n)

	glog.Info("节点启动完成", gen.FieldComponent("node"), gen.FieldNodeID(n.GetId()))
	return nil
}

// Stop gracefully stops the node and all components.
func (n *Node) Stop(ctx context.Context) error {
	glog.Info("节点开始停止", gen.FieldComponent("node"), gen.FieldNodeID(n.GetId()))

	n.options.Behavior.OnBeforeStop(n)

	stopErr := n.IManager.Stop(ctx)
	if stopErr != nil {
		glog.Error("节点组件停止错误", gen.FieldComponent("node"), gen.FieldNodeID(n.GetId()), gen.FieldErr(stopErr))
	}

	n.options.Behavior.OnAfterStop(n, stopErr)

	glog.Info("节点完成停止", gen.FieldComponent("node"), gen.FieldNodeID(n.GetId()), gen.FieldErr(stopErr))
	_ = glog.Stop()
	return stopErr
}
