# actor

一个轻量级的 Go Actor 并发库，提供：

- Actor 创建与命名路由
- 异步消息 (`Tell`) 与请求响应 (`Ask`)
- 生命周期回调 (`OnInit` / `OnMessage` / `OnDestroy` / `OnPanic`)
- 基于 `PoisonPill` 的优雅停止语义

## 安装

```bash
go get actor
```

## 核心概念

- **System**：Actor 系统实例，负责管理进程、路由和关闭。
- **PID**：Actor 唯一标识。
- **Handler / ActorHandler**：
  - `Handler`：函数式，适合简单场景。
  - `ActorHandler`：接口式，支持完整生命周期。
- **Context**：消息处理上下文，提供 `Self`、`Sender`、`Message`、`Tell`、`Respond`、`Done` 等能力。

## 快速开始

```go
package main

import (
	"fmt"
	"time"

	"actor"
)

func main() {
	sys := actor.NewSystem()
	defer sys.Shutdown()

	// Echo actor: 收到消息后直接回复
	pid, err := sys.Spawn(func(ctx actor.Context) {
		_ = ctx.Respond(ctx.Message())
	}, actor.WithName("echo"), actor.WithMailboxSize(128))
	if err != nil {
		panic(err)
	}

	// Tell: 异步发送
	_ = sys.Tell(actor.NoSender, pid, "fire-and-forget")

	// Ask: 同步请求响应
	v, err := sys.Ask(actor.NoSender, "echo", "ping", time.Second)
	if err != nil {
		panic(err)
	}
	fmt.Println(v) // ping
}
```

## API 概览

### 创建系统

```go
sys := actor.NewSystem()
```

### 创建 Actor

```go
pid, err := sys.Spawn(handler, opts...)
pid, err := sys.SpawnActor(actorHandler, opts...)
```

可选项：

- `WithName(name string)`：为 Actor 指定唯一名称
- `WithMailboxSize(size int)`：设置邮箱容量（默认 `1024`）
- `WithInitArgs(args ...any)`：设置初始化参数

### 发送消息

```go
err := sys.Tell(fromPID, targetPIDOrName, msg)
resp, err := sys.Ask(fromPID, targetPIDOrName, msg, timeout)
```

### 上下文能力

在 `OnMessage` / `Handler` 中可使用：

- `ctx.Self()`：当前 Actor PID
- `ctx.Sender()`：发送方 PID
- `ctx.Message()`：当前消息
- `ctx.InitArgs()`：初始化参数副本
- `ctx.Tell(target, msg)`：以当前 Actor 身份转发
- `ctx.Respond(value)`：回复 Ask 请求
- `ctx.Done()`：退出信号





## 错误语义

常见错误：

- `ErrNilHandler`：handler 为空
- `ErrActorNameExists`：名称冲突
- `ErrActorNotFound`：目标不存在
- `ErrMailboxFull`：邮箱满
- `ErrAskTimeout`：Ask 超时
- `ErrStopped`：进程已停止
- `ErrSystemClosed`：系统已关闭

## 测试

```bash
go test ./...
go vet ./...
```

## 设计建议

- 每个 Actor 的 `OnMessage` 保持短小，避免长时间阻塞。
- 对可能耗时的处理逻辑，建议结合 `ctx.Done()` 做可取消处理。
- 为关键 Actor 设置合理 `mailbox` 容量，避免高峰期 `ErrMailboxFull`。
