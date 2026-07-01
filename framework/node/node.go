package node

import (
	"context"
	"fmt"
	"game-server/framework/actor"

	grpc_cluster "game-server/framework/cluster/grpc"
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
	cluster    gen.ICluster
	system     gen.ISystem
	registry   gen.IRegistry
	addOnce    sync.Once
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

	glog.Init(options.Logger, logOptions...)

	n.registry = consul.New()
	n.cluster = grpc_cluster.New(n)
	n.system = actor.NewSystem(n)

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

func (n *Node) SetRegistry(registry gen.IRegistry) error {
	if registry == nil {
		return gen.ErrRegistryNil
	}
	old := n.registry
	if old != nil && old.Status() == component.StateStarted {
		return component.ErrCannotRegisterComponentAfterStarted
	}
	n.registry = registry
	return nil
}

func (n *Node) SetCluster(cluster gen.ICluster) error {
	if cluster == nil {
		return gen.ErrClusterNil
	}
	old := n.cluster
	if old != nil && old.Status() == component.StateStarted {
		return component.ErrCannotRegisterComponentAfterStarted
	}
	n.cluster = cluster
	return nil
}

func (n *Node) Startup() (err error) {
	n.AddComponents(n.registry, n.system, n.cluster)

	n.cluster.SetLocalInvoker(n.system)
	n.cluster.SetDiscovery()

	n.system.SetRemoteInvoker()

	if err = n.Start(context.Background()); err != nil {
		return err
	}

	reg := gen.ServiceInstance{
		ID:         n.GetId(),
		Name:       n.GetName(),
		ExtAddress: n.GetExtAddress(),
		RpcAddress: n.GetRpcAddress(),
		Meta:       n.options.Meta,
		Tags:       n.options.Tags,
	}
	//  注册节点
	if err = n.GetRegistry().Register(reg); err != nil {
		glog.Error("注册节点", gen.FieldErr(err))
		return err
	}

	glog.Info("节点进入运行",
		zap.String("ext_address", n.GetExtAddress()),
		zap.String("rpc_address", n.GetRpcAddress()),
		zap.Int("process_id", os.Getpid()),
	)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	defer signal.Stop(sigChan)
	sig := <-sigChan
	glog.Info("节点收到退出信号", zap.String("signal", sig.String()))
	return n.Stop(context.Background())
}

func (n *Node) addComponentsToManager() error {
	var addErr error
	n.addOnce.Do(func() {
		components := append([]component.IComponent(nil), n.components...)
		for _, comp := range components {
			if err := n.IManager.AddComponent(comp); err != nil {
				glog.Error("节点添加组件失败", gen.FieldErr(err), zap.String("typ", reflect.TypeOf(comp).String()))
				addErr = err
				return
			}
		}
	})
	return addErr
}

func (n *Node) Start(ctx context.Context) (err error) {
	n.options.Behavior.OnBeforeStart(n)

	if err := n.addComponentsToManager(); err != nil {
		return err
	}

	if err = n.IManager.Start(ctx); err != nil {
		glog.Error("节点启动组件失败", gen.FieldErr(err))
		return err
	}

	n.options.Behavior.OnAfterStart(n)

	glog.Info("节点组件启动完成", zap.Int("component_count", len(n.components)))
	return nil
}

// Stop gracefully stops the node and all components.
func (n *Node) Stop(ctx context.Context) error {
	n.options.Behavior.OnBeforeStop(n)

	if err := n.GetRegistry().Deregister(n.GetId()); err != nil {
		glog.Error("注销节点", gen.FieldErr(err))
		return err
	}

	stopErr := n.IManager.Stop(ctx)
	if stopErr != nil {
		glog.Error("节点组件停止错误", gen.FieldErr(stopErr))
	}

	n.options.Behavior.OnAfterStop(n, stopErr)

	glog.Info("节点完成停止")
	_ = glog.Stop()
	return stopErr
}
