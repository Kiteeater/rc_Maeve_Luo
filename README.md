# API 通知服务（notifier）

面向企业内部业务事件的统一路由与供应商通知服务。业务系统提交事件及发生时的数据，
服务按已发布的配置生成分发计划，完成字段映射和供应商协议适配，再独立投递、追踪和恢复各个任务。

提交仓库：[Kiteeater/rc_Maeve_Luo](https://github.com/Kiteeater/rc_Maeve_Luo)。
AI 完整原始会话归档：[ai-sessions.zip](./ai-sessions.zip)，校验文件：[ai-sessions.zip.sha256](./ai-sessions.zip.sha256)。

## 希望评审者先知道的内容

- 这是后端服务，仓库包含需求、技术设计、开发规格、实现、单元测试和集成测试。
  阅读入口：[PRD](./PRD.md) → [技术设计](./TECH_DESIGN.md) → [开发规格索引](./docs/README.md)。
- 当前代码包含 S00～S08 对应能力：配置校验与发布、事件幂等受理、独立投递与重试、
  Outbox 可靠指令、供应商运行约束、自动恢复、人工重投与核对、查询与数据保留。
- 事件受理在同一个 MySQL 事务中保存事件、任务和 Outbox；MQ 承担调度，MySQL 保存事实。
  受理成功表示服务已接管事件，供应商是否送达需要继续查询。
- 分发计划固定配置版本和映射结果，后续重试复用原计划。一次事件通知多个目标时，任务独立推进。
  对无法确认的外部结果保留 `UNKNOWN`，人工核对与重投有独立权限和审计记录。
- 当前注册的供应商适配器为 `mock-json/1`，用于验证协议适配和故障场景。
  真实供应商需要实现并注册自己的适配器；当前没有管理前端和 gRPC API。
- [统一错误码体系](./docs/09-error-codes.md) 保留通用 `code`，通过 `reason_code` 精确区分业务失败，
  统一维护分类与安全消息；任务和尝试查询同步展示投递诊断分类。

## 技术栈

| 层 | 选型 |
| --- | --- |
| 语言与 HTTP | Go 1.26.2、`net/http` |
| 数据库 | MySQL 8.4.13、InnoDB |
| 数据访问 | `database/sql` + `go-sql-driver/mysql` v1.9.3、手写 SQL |
| MQ | RocketMQ 5.3.1、`rocketmq-clients/golang/v5` v5.1.4（SimpleConsumer） |
| 缓存与限流 | Redis 7.4.12、`go-redis/v9` v9.17.2 |
| 迁移 | Goose v3.26.0，版本锁定在 `go.mod` |
| 依赖组装 | Wire 编译期生成，生成文件随代码提交 |
| 本地环境 | Docker Compose |

## 快速开始

依赖 Go 1.26.2、Docker 和 Docker Compose。以下命令从仓库根目录执行：

```sh
# 1. 只启动基础设施，等待 MySQL、Redis 和 RocketMQ Proxy 就绪
docker compose up -d mysql redis rocketmq-namesrv rocketmq-broker rocketmq-proxy
docker compose ps

# 2. 初始化数据库和即时、延迟 Topic（失败时待依赖就绪后重试）
make migrate-up
make topics

# 3. 构建并启动通知服务
make build
./bin/notifier
```

另一个终端启动模拟供应商；需要执行供应商请求时使用：

```sh
./bin/mockvendor
```

健康检查：

```sh
curl -fsS http://127.0.0.1:8080/healthz
curl -fsS http://127.0.0.1:8080/readyz
```

服务地址为 `8080`，RocketMQ Proxy 的 gRPC 端口为 `8081`，模拟供应商默认监听 `127.0.0.1:9090`。
`/healthz` 检查进程存活；`/readyz` 检查 MySQL、Redis、RocketMQ Proxy 和可用身份，失败时返回 503。

配置来自环境变量，默认值适用于上述宿主机运行方式，示例见 [.env.example](./.env.example)。
程序不会自动读取 `.env` 文件，需要通过 shell 或部署环境注入。
`config/identities.example.json` 只提供本地身份结构和 Token 的 SHA-256，权限为事件写入和业务查询。
调用业务 API 前需要配置自己的 Token 哈希、来源和调用方范围；配置发布、人工处置另需相应权限，
具体权限见 [共享契约](./docs/00-shared-contracts.md)。

列表分页需要通过 `TRACKING_CURSOR_KEY_REF` 指向游标签名密钥文件，
格式为 `{"active":"k1","keys":{"k1":"<至少 32 字节密钥的十六进制字符串>"}}`。
供应商秘密通过 `PROVIDER_SECRETS_FILE` 指向的 JSON 文件提供，配置发布时只引用凭证标识。
通道、凭证元数据与发布内容的初始化示例见 [配置发布集成测试](./tests/integration/config_release_test.go)。

部署时可设置 `AZURE_KEY_VAULT_URL`、`MYSQL_DSN_SECRET`，Redis 使用密码时再设置 `REDIS_PASSWORD_SECRET`。
后两项是密钥名称，启动时通过 Azure `DefaultAzureCredential` 读取；读取失败或空值会阻止启动。
配置通过构造函数注入，无可变全局配置单例。

MySQL、Redis 和 Broker 使用命名卷；`docker compose down` 后数据仍保留，`docker compose down -v` 会删除数据。

## 验证

不依赖外部基础设施：

```sh
make test
make vet
make race
make build
```

集成测试通过 `integration` build tag 启用，依赖上述真实基础设施，默认使用本地 MySQL 的 `root/root`。
测试自行创建测试库并执行迁移，部分场景会重启 Compose 服务，应在本地测试环境运行。

```sh
go test -tags=integration -count=1 -timeout=30m ./tests/integration
go test -race -tags=integration -count=1 -timeout=30m ./tests/integration
```

集成测试覆盖配置并发发布、事件并发幂等、多目标投递、重试与恢复、人工处置、查询与保留等场景。
`TestMQ06_RealBrokerImmediateAndDelayed` 因 RocketMQ Go 客户端 v5.1.4 内部数据竞争，
在 `-race` 模式下显式跳过；普通集成测试仍会执行它。测试入口见 [tests/integration](./tests/integration/README.md)。

本次提交材料整理时执行通过了 `make test`、`make vet`、`make race` 和 `make build`。
本次未启动基础设施重跑集成测试，上述集成命令供复现使用。

## API 入口

除健康检查外，以下接口均使用 Bearer Token，并校验权限与调用方范围。
请求结构和错误契约见 [开发规格](./docs/README.md)，可复现的调用场景见 `tests/integration/`。

| 能力 | 接口 |
| --- | --- |
| 配置校验、创建、发布 | `POST /v1/config/releases/validate`、`POST /v1/config/releases`、`POST /v1/config/releases/{id}/publish` |
| 配置读取 | `GET /v1/config/releases/current`、`GET /v1/config/releases/{id}` |
| 事件提交 | `POST /v1/events` |
| 事件与任务查询 | `GET /v1/events`、`GET /v1/events/{id}`、`GET /v1/deliveries`、`GET /v1/deliveries/{id}` |
| 尝试与时间线 | `GET /v1/deliveries/{id}/attempts`、`GET /v1/deliveries/{id}/history` |
| 人工重投、核对 | `POST /v1/deliveries/{id}/retry`、`POST /v1/deliveries/{id}/resolve` |
| 运行概览 | `GET /v1/overview` |

## 代码组织

| 路径 | 职责 |
| --- | --- |
| `cmd/notifier` | 主进程、Wire 依赖图和依赖组装 |
| `cmd/mockvendor`、`internal/mockvendor` | 独立模拟供应商及故障场景 |
| `internal/conf`、`internal/auth` | 配置加载、身份认证和权限上下文 |
| `internal/server`、`internal/service` | HTTP 路由、协议转换和后台角色生命周期 |
| `internal/biz` | 配置、事件、投递、Outbox、恢复、人工处置和查询用例 |
| `internal/domain`、`internal/canonicaljson` | 领域类型、状态规则、精确 JSON 和计划计算 |
| `internal/data_interface` | 领域仓储及外部能力接口 |
| `internal/data` | MySQL、Redis、RocketMQ、供应商 HTTP 和密钥读取实现 |
| `unit_test` | 镜像业务目录、通过导出接口测试的黑盒单测 |
| `tests/integration` | 真实依赖集成验证 |
| `migrations` | 按序执行的 Goose SQL 迁移 |
| `docs` | 共享契约与 S01～S08 开发规格 |

修改依赖图后执行 `make wire`；普通构建不需要重新生成。

## AI 会话记录

[ai-sessions.zip](./ai-sessions.zip) 收录本项目在 Codex、Claude Code、Grok 本地保存的原始会话文件。
本次快照包含 Codex 8 个、Claude Code 4 个、Grok 7 个会话，共 227 个原始文件。
按项目工作目录及其子目录筛选，包含用户消息、助手回复、工具调用与结果，以及客户端保存的会话相关文件；
原始文件逐字节写入 ZIP，没有手工删改、脱敏、合并或用摘要替换。
Grok 会话目录中的历史摘要和终端记录也随原目录保留。

ZIP 内的 `manifest.json` 列出会话 ID、源文件路径、文件大小、捕获时间和每个文件的 SHA-256。
归档包含这次整理提交材料的会话；“完整”以各文件捕获时已经落盘的记录为界，后续新增消息需要再次导出。

```sh
# 检查 ZIP 本身和外部校验值
unzip -t ai-sessions.zip
shasum -a 256 -c ai-sessions.zip.sha256

# 在保存原始会话的开发机器上重新导出，要求 Python 3.14+
python3 scripts/export-ai-sessions.py
```

导出脚本会重新扫描三种客户端的项目会话，保留源文件字节，并在生成后校验 ZIP 内文件。
可通过 `--project`、`--home` 和 `--output` 指定项目、用户目录和输出路径。
