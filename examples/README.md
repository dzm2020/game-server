# Examples

本目录提供可直接运行的示例代码。

## 1) profile 全局配置示例

```bash
go run ./examples/profile
```

该示例演示：
- 加载 `yaml` 配置到 `internal/profile` 全局配置中心
- 读取 `base` 节点配置并打印关键字段

## 2) node 启动示例

```bash
go run ./examples/node
```

该示例演示：
- 创建并启动 `internal/node`
- 使用 `internal/iface.INodeLifecycleHook` 打印生命周期回调

注意：
- 示例会阻塞等待退出信号（`Ctrl+C`）
- 依赖本地可用的 Consul/NATS（默认地址见 `examples/node/config.yaml`）

## 3) cluster actor 启动示例

```bash
go run ./examples/cluster_actor
```

该示例演示：
- 基于 `node.New(...).Startup()` 的标准节点启动流程
- 集群 actor 的请求-应答链路（`RequestToPID`）
- 生命周期回调里注册服务并处理请求

说明：
- 需要本地可访问 NATS 和 Consul
- 示例会阻塞等待退出信号（`Ctrl+C`）
