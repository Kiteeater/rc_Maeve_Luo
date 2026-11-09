# API 通知服务（notifier）

面向企业内部业务事件的统一路由与供应商通知服务。业务系统提交事件及发生时的数据，
通知服务按配置路由决定接收方、完成字段映射与供应商协议适配，并对各投递任务独立投递、追踪与恢复。

- 产品需求：[PRD.md](./PRD.md)
- 技术方案：[TECH_DESIGN.md](./TECH_DESIGN.md)

## 技术栈

| 层 | 选型 |
| --- | --- |
| 语言与 HTTP | Go 1.26.2、`net/http` |
| 数据库 | MySQL 8.4、InnoDB |
| 数据访问 | `database/sql` + `go-sql-driver/mysql`、手写 SQL |
| MQ | RocketMQ 5.x、`rocketmq-clients/golang/v5`（SimpleConsumer） |
| 缓存 | Redis、`go-redis/v9` |
| 迁移 | Goose v3 |
| 本地环境 | Docker Compose |

当前脚手架使用标准库 HTTP、Wire 编译期依赖注入和 Azure Key Vault SDK。
数据库、MQ 与缓存的业务实现仍待开发，相关依赖按需引入（`make deps` 可一次性拉取）。

## 目录结构

| 路径 | 职责 |
| --- | --- |
| `cmd/notifier/{main.go,wire.go,wire_gen.go}` | 进程生命周期、依赖图声明及生成的依赖组装 |
| `cmd/mockvendor` | 独立模拟供应商 |
| `internal/conf` | 环境变量与 Azure Key Vault 汇总为只读 `GlobalConfig` |
| `internal/server` | HTTP 路由、中间件与服务构造；后续按需增加 gRPC 和后台入口 |
| `internal/service` | 协议结构与业务对象转换；当前提供健康检查和 /v1 占位响应 |
| `internal/biz/biz_*` | 业务用例，通过 `data_interface` 使用仓储及外部能力 |
| `internal/data_interface` | 仓储与外部能力接口，方法使用领域类型 |
| `internal/data` | MySQL 事务、缓存、消息及供应商客户端等具体实现；数据库只在此访问 |
| `internal/domain`、`enums`、`constant` | 业务对象、状态规则、枚举和常量 |
| `internal/remote_grpc`、`utils` | 按需引入的内部 gRPC 客户端及纯工具 |
| `pkg/` | errors、metrics、middleware 等真正跨模块复用的代码 |
| `unit_test/` | 镜像 `internal` 目录、仅通过导出接口测试的黑盒单测 |
| `migrations/` | Goose SQL 迁移（初始数据模型） |
| `tests/integration/` | 集成验证（真实 MySQL/RocketMQ/Redis + 模拟供应商） |

完整依赖约束及模块映射见 [TECH_DESIGN.md 第 12 节](./TECH_DESIGN.md#12-接口与代码组织)。
尚未实现的业务目录按需创建；HTTP API 仍为骨架，尚无 protobuf 或 gRPC 服务。

## 快速开始

```sh
# 构建与运行
make build
./bin/notifier

# 本地基础设施与模拟供应商
docker compose up -d

# 数据库迁移
make migrate-up

# 测试与静态检查
make test
make vet

# 修改依赖图后重新生成（生成文件随代码交付，普通构建无需运行 Wire）
make wire
```

配置见 [.env.example](./.env.example) 与 `internal/conf`。本地开发可使用默认值和环境变量。
部署时设置 `AZURE_KEY_VAULT_URL`、`MYSQL_DSN_SECRET`；Redis 使用密码时同时设置 `REDIS_PASSWORD_SECRET`。
后两项是密钥名称，密钥值只存 Key Vault。启动时读取并覆盖本地值，失败或空值会阻止启动，
整个读取过程受 `AZURE_KEY_VAULT_TIMEOUT` 约束。配置通过构造函数注入，无可变全局单例。
身份使用 Azure SDK 的 `DefaultAzureCredential`，Azure 部署推荐托管身份；
配置与授权方式见 [Azure 官方指南](https://learn.microsoft.com/en-us/azure/key-vault/secrets/quick-create-go)。

目前 `/readyz` 保持骨架行为，未连接 MySQL、Redis 或 MQ；基础设施实现后必须在 Wire 中注入真实就绪检查。

通知服务通过宿主机 `8080` 端口访问，RocketMQ Proxy 的 gRPC 端口为 `8081`。
MySQL、Redis 和 Broker 的数据使用命名卷，`docker compose down` 后重新启动仍复用；
`docker compose down -v` 会删除这些数据。
