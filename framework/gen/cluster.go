package gen

import (
	"context"
	"game-server/framework/pkg/component"
)

type IClusterComp interface {
	component.IComponent
	ICluster
}

// ICluster 定义集群通信组件能力。
//
// 生命周期命名冻结约定：
// 1. Start：启动长期运行组件；要求幂等，且不得长期阻塞。
// 2. Stop：优雅停止长期运行组件；要求幂等，并阻塞等待停止完成或 ctx.Done。
// 3. Shutdown：仅用于“服务端入口”级别优雅退出（如网络服务器），语义等同 Stop，但强调停止接入。
// 4. Close：仅用于“单个资源句柄”关闭（连接/文件等），调用后资源不可继续使用。
type ICluster interface {
	// Start 启动集群通信组件。
	// 幂等：重复调用不得重复启动，已启动时应返回 nil。
	// 阻塞：可短暂阻塞到监听与必要后台任务就绪，不得长期阻塞。
	// 返回：集群可用返回 nil；初始化失败或已不可启动时返回 error。
	Start(ctx context.Context) error
	SetLocalInvoker(invoker ILocalInvoker)
	SetDiscovery(discovery IDiscovery)
	SendToNode(from, to *PID, msg *Message) error
	Broadcast(to *PID, msg *Message)
	// Stop 优雅停止集群通信组件。
	// 幂等：可重复调用，仅第一次执行真实停止流程；后续调用返回与首次一致的结果。
	// 阻塞：阻塞直到后台任务和连接关闭完成，或 ctx.Done。
	// 返回：完全停止返回 nil；若被 ctx 取消/超时或停止过程失败，返回 error。
	Stop(ctx context.Context) error
}
