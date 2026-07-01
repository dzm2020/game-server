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
	ctx        context.Context
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
	n.cluster = grpc_cluster.New()
	n.system = actor.NewSystem()
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
func (n *Node) assemble() {
	n.cluster.SetLocalInvoker(n.system)
	n.cluster.SetDiscovery(n.registry)
	n.system.SetRemoteInvoker(n.cluster)
	n.AddComponents(n.registry, n.system, n.cluster)
}

func (n *Node) Startup() (err error) {
	//  组装基础模块
	n.assemble()
	//  添加到组件管理器
	if err = n.IManager.AddComponent(n.components...); err != nil {
		return
	}
	//  初始化所有组件
	if err = n.InitComponents(n.ctx); err != nil {
		return err
	}
	//  启动化所有组件
	if err = n.StartComponents(n.ctx); err != nil {
		return err
	}

	//  所有模块启动完成后注册节点
	if err = n.registryNode(); err != nil {
		return err
	}
	glog.Info("节点进入运行",
		zap.String("ext_address", n.GetExtAddress()),
		zap.String("rpc_address", n.GetRpcAddress()),
		zap.Int("process_id", os.Getpid()),
	)
	//  等待退出信号
	n.waitSig()
	//  注销节点
	if err = n.GetRegistry().Deregister(); err != nil {
		return err
	}
	//  停止所有组件
	if err = n.StopComponents(n.ctx); err != nil {
		return err
	}

	_ = glog.Stop()
	return
}
func (n *Node) waitSig() {
	//  等待退出信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	defer signal.Stop(sigChan)
	sig := <-sigChan
	glog.Info("节点收到退出信号", zap.String("signal", sig.String()))
}

func (n *Node) registryNode() error {
	reg := gen.ServiceInstance{
		ID:         n.GetId(),
		Name:       n.GetName(),
		ExtAddress: n.GetExtAddress(),
		RpcAddress: n.GetRpcAddress(),
		Meta:       n.options.Meta,
		Tags:       n.options.Tags,
	}
	return n.GetRegistry().Register(reg)
}

func (n *Node) InitComponents(ctx context.Context) (err error) {
	n.options.Behavior.OnBeforeInit(n)
	if err = n.IManager.Init(ctx); err != nil {
		glog.Error("节点组件初始化组件失败", gen.FieldErr(err))
		return err
	}
	n.options.Behavior.OnAfterInit(n)
	glog.Info("节点组件初始化完成", zap.Int("component_count", len(n.components)))
	return nil
}

func (n *Node) StartComponents(ctx context.Context) (err error) {
	n.options.Behavior.OnBeforeStart(n)
	if err = n.IManager.Start(ctx); err != nil {
		glog.Error("节点启动组件失败", gen.FieldErr(err))
		return err
	}
	n.options.Behavior.OnAfterStart(n)
	glog.Info("节点组件启动完成", zap.Int("component_count", len(n.components)))
	return nil
}

func (n *Node) StopComponents(ctx context.Context) error {
	n.options.Behavior.OnBeforeStop(n)
	if err := n.IManager.Stop(ctx); err != nil {
		glog.Info("节点组件停止失败", gen.FieldErr(err))
		return err
	}
	n.options.Behavior.OnAfterStop(n)
	glog.Info("节点组件完成停止")
	return nil
}
