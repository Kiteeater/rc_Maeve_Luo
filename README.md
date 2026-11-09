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

当前脚手架仅使用标准库，外部依赖在实现阶段按需引入（`make deps` 可一次性拉取）。

## 目录结构

| 路径 | 职责 |
| --- | --- |
| `cmd/notifier` | 依赖组装与生命周期 |
| `cmd/mockvendor` | 独立模拟供应商 |
| `internal/config` | 启动配置（环境变量加载） |
| `internal/httpapi` | 认证与输入输出骨架（healthz/readyz 与 /v1 占位） |
| `migrations/` | Goose SQL 迁移（初始数据模型） |
| `tests/integration/` | 集成验证（真实 MySQL/RocketMQ/Redis + 模拟供应商） |

其余模块的职责见 TECH_DESIGN.md 第 12 节，在引入实际实现时创建对应包。

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
```

配置通过环境变量覆盖，见 [.env.example](./.env.example) 与 `internal/config`。

通知服务通过宿主机 `8080` 端口访问，RocketMQ Proxy 的 gRPC 端口为 `8081`。
MySQL、Redis 和 Broker 的数据使用命名卷，`docker compose down` 后重新启动仍复用；
`docker compose down -v` 会删除这些数据。
