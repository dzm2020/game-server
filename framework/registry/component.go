package registry

//
//func NewComponent(node gen.INode) *Component {
//	return &Component{
//		node: node,
//	}
//}
//
//type Component struct {
//	node gen.INode
//	gen.IRegistry
//
//	runMu     sync.Mutex
//	runCancel context.CancelFunc
//}
//
//func (c *Component) Init() error {
//	glog.Info("注册发现组件初始化")
//	options := c.node.GetOptions()
//	c.IRegistry = options.Registry
//	if c.IRegistry != nil {
//		return nil
//	}
//
//	reg, err := consul.New(options.Consul)
//	if err != nil {
//		glog.Error("consul初始化失败", glog.Component("registry.component"), glog.Err(err))
//		return err
//	}
//	c.IRegistry = reg
//
//	return nil
//}
//
//func (c *Component) Start(ctx context.Context) error {
//	if c.IRegistry == nil {
//		return gen.ErrRegistryNil
//	}
//
//	c.runMu.Lock()
//	if c.runCancel != nil {
//		c.runMu.Unlock()
//		return nil
//	}
//	c.runMu.Unlock()
//
//	glog.Info("注册发现组件运行")
//	reg := gen.ServiceInstance{
//		ID:         c.node.GetId(),
//		Name:       c.node.GetName(),
//		ExtAddress: c.node.GetExtAddress(),
//		RpcAddress: c.node.GetRpcAddress(),
//	}
//	if err := c.Register(reg); err != nil {
//		glog.Error("注册服务失败", glog.Component("registry.component"), glog.NodeID(c.node.GetId()), glog.Err(err))
//		return err
//	}
//
//	glog.Info("注册服务", zap.Any("reg", reg))
//	runCtx, cancel := context.WithCancel(ctx)
//	c.runMu.Lock()
//	c.runCancel = cancel
//	c.runMu.Unlock()
//	if err := c.IRegistry.Start(runCtx); err != nil {
//		cancel()
//		c.runMu.Lock()
//		c.runCancel = nil
//		c.runMu.Unlock()
//		return err
//	}
//	return nil
//}
//
//func (c *Component) Stop(ctx context.Context) error {
//	if c.IRegistry == nil {
//		return nil
//	}
//
//	c.runMu.Lock()
//	cancel := c.runCancel
//	c.runCancel = nil
//	c.runMu.Unlock()
//	if cancel != nil {
//		cancel()
//	}
//
//	if err := c.IRegistry.Deregister(c.node.GetId()); err != nil {
//		glog.Error("注销服务失败", glog.Component("registry.component"), glog.NodeID(c.node.GetId()), glog.Err(err))
//		return err
//	}
//	glog.Info("注销服务", zap.String("node", c.node.GetId()))
//
//	return c.IRegistry.Stop(ctx)
//}
