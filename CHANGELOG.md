# Changelog — taosx

本文件记录 `taosx` 的用户可见变更，遵循 [Keep a Changelog](https://keepachangelog.com/)
与 [Semantic Versioning](https://semver.org/)。

本仓库代码自 `xhyper.rs` 的 `crates/platform/drivers/taos` 抽取而来（抽取时点为 `0.3.16`）。
该工程内的版本线不在本文件中延续，本仓库从 `0.1.0` 重新起算。

## [Unreleased]

## [0.1.0] - 2026-09-21

### 新增

- 客户端 `TaosPool`（别名 `TaosClient`）首次以独立 crate 形式提供：`connect` / `exec` /
  `query` / `query_series` / `write_batch` / `write_batch_report` / `ping` /
  `health_check` / `close`，连接池以 `max_in_flight` 信号量背压。
- 双传输：`TransportMode::Rest`（默认，`POST /rest/sql[/database]`，端口 6041，Basic 认证）
  与 `TransportMode::NativeWs`（`ws(s)://host:port/rest/ws`）。
- 原生 WS 独立函数：`build_native_ws_url` / `validate_mode` / `connect_native_ws` /
  `exec_sql_ws` / `probe_native_tcp`。
- SQL 注入防护：`build_insert_sql_chunks` 是构造 INSERT 的唯一入口，标识符白名单校验、
  tag 值十六进制子表编码、字符串字面量转义、时间戳仅十进制整数拼接。
- 批量写入：`TaosPoint`、异步累积器 `WriteBatcher` / `WriteBatcherConfig` /
  `BatcherCloseReport` / `BatcherCloseError`，以及写入报告 `BatchWriteReport` /
  `BatchWritePartialError`。
- 有界查询流 `TaosQueryStream`；受 `max_query_rows` 约束。
- 重试策略 `RetryPolicy`：指数退避 + 抖动 + deadline（`compute_backoff` 为可测纯函数），
  提供 `for_read()` / `for_idempotent_write()` 预设。
- 错误分类 `TaosError`：`is_retryable()`、`from_taos_code(code, ctx)` 与
  `from_http_status(status, ctx)`。
- 资源治理常量 `HARD_MAX_IN_FLIGHT`、`HARD_MAX_BATCH_ROWS`、`HARD_MAX_BATCH_BYTES`、
  `HARD_MAX_RESPONSE_BYTES`、`HARD_MAX_QUERY_ROWS`、`HARD_MAX_CLOSE_TIMEOUT`。
- 观测 `TaosMetricsSnapshot` 与 `ws_probe_totals`。

### 变更

- **解耦**：错误模型从主工程的 `kernel`（`XError` / `ErrorKind` / `PrecisionLossPolicy`）
  下沉为 crate 内 `src/error.rs` 的 `TaosError` / `TaosResult`，`Cargo.toml` 不再声明任何
  内部 crate 依赖。
- **破坏性变更**：移除源模块的时间精度合同模块 `time_storage`、`selfcheck` 自验证框架、
  `soak`、`tmq`、`adapter`（scaffold）与 `async-trait` 依赖，`scaffold` feature 不再提供。
- 原 `time_storage` 的「禁止静默精度损失」语义以内联方式保留在 `build_insert_sql_chunks`：
  纳秒时间戳无法无损换算为目标精度时直接返回 `TaosError::Invalid`，不再依赖外部
  `PrecisionLossPolicy`。
- 环境变量前缀与 TOML 加载改由本仓库自有的 `FOUNDATIONX_TAOSX_` 前缀与
  `TaosConfig::from_env()` / `from_toml()` 承担。

### 说明

- REST 是数据读写的主路径；`TransportMode::NativeWs` 下 `TaosPool::connect` 会先做一次
  WS 握手探测，SQL 数据面仍默认走 REST。
- WS 空帧不伪造成功，一律 fail-closed 为 `TaosError::Unavailable`。
- `TaosQueryStream` 不是服务端游标：先按 `max_query_rows` 有界物化，再逐行 yield。
- 查询结果受 `HARD_MAX_QUERY_ROWS` / `HARD_MAX_RESPONSE_BYTES` 上界约束，超限报错而非
  静默截断。
- 凭据（user / password）只能经环境变量或构建器注入，`Debug` 输出脱敏，TOML 禁止非空密码。
- 本 crate **不发布到 crates.io**，仅以 GitHub 源码 / git 依赖形式复用，安装方式见 `README.md`。
