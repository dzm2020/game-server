actor 退出 


gate 限流 鉴权






game-server/
├── cmd/                          # 各服务入口 (只放 main.go)
│   ├── gateway/                  # 网关服务
│   │   └── main.go
│   ├── game/                     # 游戏逻辑服务 (Actor节点)
│   │   └── main.go
│   ├── auth/                     # 认证服务
│   │   └── main.go
│   ├── match/                    # 匹配服务
│   │   └── main.go
│   └── tools/                    # 工具 (压力测试、GM后台)
│       └── bench/
│           └── main.go
│
├── internal/                     # 私有实现 (外部无法引用)
│   ├── gateway/                  # 网关核心逻辑
│   │   ├── server.go             # TCP/WS 监听
│   │   ├── client.go             # 连接对象 (ReadPump/WritePump)
│   │   ├── session.go            # 会话管理 (uid↔conn)
│   │   ├── router.go             # 消息上行路由 (到Actor)
│   │   ├── pusher.go             # 下行推送 (Actor→客户端)
│   │   └── auth.go               # 登录验签
│   │
│   ├── actor/                    # Actor 框架 (可单独升级)
│   │   ├── actor.go              # 核心接口: Receive(), PID
│   │   ├── system.go             # ActorSystem (创建/销毁/路由)
│   │   ├── mailbox.go            # 信箱与调度
│   │   ├── remote.go             # 远程 Actor 通信 (跨节点)
│   │   └── placement/            # 寻址策略
│   │       ├── hash.go           # 一致性哈希
│   │       └── registry.go       # 基于 etcd/consul
│   │
│   ├── gamesvr/                  # 游戏业务逻辑
│   │   ├── player/
│   │   │   ├── actor.go          # PlayerActor (登录、属性)
│   │   │   ├── bag.go
│   │   │   └── task.go
│   │   ├── room/
│   │   │   ├── actor.go          # RoomActor (进入、移动、退出)
│   │   │   └── match.go
│   │   └── world/
│   │       └── actor.go          # 全局服务 (排行榜)
│   │
│   ├── net/                      # 网络库封装
│   │   ├── tcp.go
│   │   ├── ws.go
│   │   ├── codec/                # 粘包/编解码
│   │   └── heartbeat.go
│   │
│   ├── protocol/                 # 协议处理
│   │   ├── envelope.go           # 内/外部消息封装
│   │   ├── dispatcher.go         # 消息分发 (msgID→handler)
│   │   └── pb/                   # protobuf 生成的 Go 代码
│   │       ├── cs/               # 客户端-服务端协议
│   │       └── ss/               # 服务端间协议
│   │
│   ├── rpc/                      # 服务间 RPC 封装
│   │   ├── grpc/
│   │   ├── nats/
│   │   └── custom/
│   │
│   ├── registry/                 # 服务注册/发现抽象
│   │   ├── etcd.go
│   │   ├── consul.go
│   │   └── cache.go              # 本地路由缓存
│   │
│   ├── config/                   # 配置加载
│   │   ├── config.go
│   │   └── gateway.toml
│   │
│   └── pkg/                      # 内部通用工具
│       ├── logger/               # 日志
│       ├── pool/                 # 对象池 (sync.Pool 封装)
│       ├── snowflake/            # 唯一ID生成
│       └── errutil/
│
├── api/                          # 协议定义 (Proto)
│   ├── cs/                       # 客户端协议
│   │   ├── login.proto
│   │   ├── chat.proto
│   │   └── ...
│   └── ss/                       # 服务端协议
│       ├── gateway.proto         # 网关与Actor的 Push 接口
│       └── actor.proto           # Actor 远程调用
│
├── configs/                      # 各服务配置文件
│   ├── gateway.yaml
│   ├── game.yaml
│   └── auth.yaml
│
├── deployments/                  # 部署相关
│   ├── docker/
│   │   ├── Dockerfile.gateway
│   │   └── Dockerfile.game
│   └── kubernetes/
│       └── ...
│
├── scripts/                      # 构建/测试/运行脚本
│   ├── build.sh
│   ├── run_local.sh
│   └── gen_proto.sh
│
├── test/                         # 集成/端到端测试
│   └── e2e/
│
├── go.mod
├── go.sum
└── Makefile