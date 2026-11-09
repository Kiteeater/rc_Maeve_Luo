# 黑盒单元测试

目录与 `internal` 对应，例如 `unit_test/conf`、`unit_test/server`、
`unit_test/biz/biz_event`。测试使用 `conf_test` 等外部包，只调用导出接口，
业务用例通过 `data_interface` 注入仓储、外部调用和时钟替身。

运行 `make test` 或 `go test ./unit_test/...`。修改传输层时检查完整路由的输入输出；
修改用例时验证业务行为、事务命令及失败路径，不访问私有字段或依赖实现细节。

真实数据库、MQ 与缓存的事务、并发及故障恢复验证放在 `tests/integration`。
