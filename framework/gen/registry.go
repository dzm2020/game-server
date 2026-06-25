package gen

import "context"

type Topology struct {
	All     []ServiceInstance
	Added   []ServiceInstance
	Updated []ServiceInstance
	Removed []ServiceInstance
}

type ServiceChangeHandler func(topology *Topology)

// ServiceInstance represents a discovered service endpoint.
type ServiceInstance struct {
	ID         string
	Name       string
	ExtAddress string // 对外地址
	RpcAddress string // 对内地址
	Tags       []string
	Meta       map[string]string
}

func (s *ServiceInstance) GetID() string {
	return s.ID
}
func (s *ServiceInstance) GetName() string {
	return s.Name
}

func (s *ServiceInstance) GetRpcAddress() string {
	return s.RpcAddress
}
func (s *ServiceInstance) GetExtAddress() string {
	return s.ExtAddress
}

type ServiceHealthState = string

const (
	ServiceHealthStatePassing  = ServiceHealthState("passing")
	ServiceHealthStateWarning  = ServiceHealthState("warning")
	ServiceHealthStateCritical = ServiceHealthState("critical")
)

// IRegistry 定义 Registry 对外能力，便于依赖倒置与单测替身注入。
//
// 生命周期命名冻结约定：
// - Start：启动长期运行组件；要求幂等，且不得长期阻塞。
// - Stop：优雅停止长期运行组件；要求幂等，并阻塞等待停止完成或 ctx.Done。
// - Shutdown：仅用于“服务端入口”级别优雅退出，语义等同 Stop。
// - Close：仅用于“单个资源句柄”关闭，调用后资源不可继续使用。
type IRegistry interface {
	IDiscovery
	IRegistrar

	// Start 启动服务注册/发现后台任务。
	// 幂等：重复调用不得重复启动，已启动时应返回 nil。
	// 阻塞：可短暂阻塞到后台任务初始化完成，不得长期阻塞。
	// 返回：后台任务进入运行态返回 nil；初始化失败或已不可启动时返回 error。
	Start(ctx context.Context) error

	// Stop 优雅停止服务注册/发现后台任务。
	// 幂等：可重复调用，仅第一次执行真实停止流程；后续调用返回与首次一致的结果。
	// 阻塞：阻塞直到后台任务退出完成，或 ctx.Done。
	// 返回：完全停止返回 nil；若被 ctx 取消/超时或停止过程失败，返回 error。
	Stop(ctx context.Context) error
}

type IRegistrar interface {
	Register(reg ServiceInstance) error
	Deregister(serviceID string) error
	SetHealthState(serviceID, state ServiceHealthState) error
}

type IDiscovery interface {
	GetInstance(serverID string) (ServiceInstance, bool)
	Discover(serviceName string) []ServiceInstance
	DiscoverAll() map[string][]ServiceInstance
	ListServices() []string
	Watch(serviceName string, onChange ServiceChangeHandler) (string, error)
	Unwatch(serviceName, watchID string)
}
