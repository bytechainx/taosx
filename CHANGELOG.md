# Changelog — taosx

本文件记录 `taosx` 的用户可见变更，遵循 [Keep a Changelog](https://keepachangelog.com/)
与 [Semantic Versioning](https://semver.org/)。

本仓库代码自 `xhyper.rs` 的 `crates/platform/drivers/taos` 抽取而来（抽取时点为 `0.3.16`）。
该工程内的版本线不在本文件中延续，本仓库从 `0.1.0` 重新起算。

## [Unreleased]

### 修复

- **竞态缺陷 — `WriteBatcher::close()` 窗口期数据静默丢失**：`close()` 在取出缓冲区并
  释放锁之后、重新获取锁之前存在无锁 `flush_batch` 窗口；此期间并发的 `push()` 因
  `closed` 仍为 `false` 而成功写入新数据，但 `close()` 重获锁后仅检查 `failed_pending`，
  不检查缓冲区，导致窗口内数据永久静默丢失。修复：`close()` 在释放锁之前先设置 `closing`
  标志；`push()` 与 `flush()` 检查 `closed || closing` 时拒绝操作（返回
  `TaosError::Closed`）。失败路径会清除 `closing` 以允许外部恢复后重试关闭。
- **`with_message` 对 `Io` 变体静默丢弃上下文消息**（P1-2）：此前 `TaosError::with_message`
  对其他 9 个变体均替换消息，唯独 `Io` 变体忽略调用方传入的上下文，文档承诺「替换错误消息」
  与实现不一致。现改为保留原始 `std::io::Error` 的 `kind`，用新消息重建 `io::Error`。
- **`validate()` 超时校验合并为同一条模糊错误消息**（P1-1）：此前 `timeout`、
  `acquire_timeout`、`close_timeout` 的四条校验条件合并输出一条不含字段名的错误，
  其余字段均有独立的「字段名+范围」消息。现拆分为四条独立错误，每条包含字段名、当前值
  与允许范围；`close_timeout` 上限错误消息引用 `HARD_MAX_CLOSE_TIMEOUT` 常量而非
  硬编码「30 秒」。
- **`detect_precision` 的 database 名直拼 SQL 字面量位置**（P1-3）：此前 database 名
  直接置于 `WHERE name='{database}'` 的单引号字面量位置，安全性仅依赖 `validate_ident`
  白名单校验（脆断耦合——未来若放宽 `validate_ident` 即破坏转义假设）。现改为经
  `escape_str` 转义后再拼入 SQL，与标识符校验解耦。

### 测试

- 补 `with_message` 对 `Io` 变体的两条单测（消息替换 + kind 保留）
- 补 `validate()` 超时字段级错误消息的独立单测
- 补 `detect_precision` 经 `escape_str` 转义 database 名的 HTTP 捕获回归测试
- 补多主机 failover 成功路径离线 mock 测试（`tests/failover_success.rs`）：首 host 失败、
  次 host 成功 + 多备用 host 两场景
- 补原生 WebSocket 层离线 mock 三分支测试（`tests/ws_native_mock.rs`）：Text 帧、
  Binary 帧解码、非数据帧 fail-closed、Ping 帧 fail-closed

## [0.1.4] - 2026-09-22

### 变更

- **内部结构改写（公开 API 与可观察契约均不变）**：按 `docs/module-rules.md` §5.5 的手法，把
  `src/client.rs` 的 `impl TaosPool` 整块（构造、连接、并发额度、健康检查、SQL 收发与批量写入
  入口共 19 个方法）下沉为 `src/client/pool.rs`（427 行）。门面 `src/client.rs` 保留模块文档、
  `CLOSED_BIT` / `IN_FLIGHT_MASK` 常量、`build_http_client`、`TaosPool` 与 `PoolInner` /
  `RequestGuard` 的**结构定义与字段**、`impl Debug` / `impl Drop`、`TaosClient` 别名、
  `mod` / `pub use` 声明与**原有内联测试**。
  两处提为 `pub(super)`，都是「父/兄弟/门面测试」必须看见的：`acquire`（门面内联测试直接驱动
  并发额度与超时用例）与 `verify_decimal_schema`（被**兄弟模块** `client/write.rs` 的批量写入
  路径调用）；其余私有辅助（`connect_one` / `detect_precision` / `exec_sql_raw*` /
  `ensure_open`）只在本 impl 内互调，**保持私有**。
  `src/client.rs` 生产段 **535 → 120** 行。
  动机：`module-rules` 是元仓库必需检查，且它审计各仓**默认分支**，故当 `client.rs` 距
  `MR-STRUCT-007` 的 800 行 ERROR 阈值只剩 265 行时，任一仓的任意改动都可能卡住元仓库的全部 PR。
  属**纯搬移**（行多重集比对确认零代码行丢失：「仅旧」恰为提级的两条签名；内联测试段与旧文件
  536–1061 行**逐字节一致**），129 项测试与 doctest 结果不变。

## [0.1.3] - 2026-09-22

### 变更

- **内部结构改写（公开 API 与可观察契约均不变）**：按 `docs/module-rules.md` §5.5 的手法，把
  `src/config.rs` 的三块职责下沉为子模块 —— 精度与传输模式枚举 → `src/config/enums.rs`、
  端点 URL 构造 → `src/config/endpoint.rs`、链式构建器 → `src/config/builder.rs`。
  门面 `src/config.rs` 保留模块文档、`ENV_*` / `DEFAULT_*` / `HARD_MAX_*` 常量、
  `TaosConfig` 定义与 `Default` / `Debug`、`from_env` / `from_toml` / `from_toml_file` /
  `validate` / `builder`、私有的 `apply_env_overrides`，以及**原有内联测试**。
  `TsPrecision` / `TransportMode` / `TaosConfigBuilder` 经门面 `pub use` 导出，故公开路径与
  crate 内部路径（`crate::config::{TsPrecision, TransportMode, TaosConfigBuilder}`）均不变；
  `src/config/parse.rs` 的 `use super::{…}` 与 `src/client/sql.rs` 的 `use crate::config::{…}`
  **一行未改**。`src/config.rs` 生产段由 **736 → 426** 行。
  动机：`module-rules` 是元仓库必需检查，且它审计各仓**默认分支**，故当 `config.rs` 生产段距
  `MR-STRUCT-007` 的 800 行 ERROR 阈值只剩 64 行时，任一仓的任意改动都可能卡住元仓库的全部 PR。
  属**纯搬移**（行多重集比对确认零代码行丢失），全部 129 项测试与 doctest 结果不变。

## [0.1.2] - 2026-09-22

### 修正

- **错误消息不再携带原始响应正文**（`docs/标准.md` §4「错误消息与 `Debug` 输出不回显
  密码」，`src/error.rs` 亦约定「响应正文与凭据一律不入消息」）。修复前，非 2xx 响应与
  「2xx 但正文非法 JSON」都会把正文截断后拼进错误消息（`taos HTTP 400: <正文>` /
  `TDengine JSON 解析失败（…）; body=<正文>`）：一旦远端或中间代理在正文里回显凭据 / DSN，
  错误消息连同日志会一起泄漏。现改为非 2xx 只保留 HTTP 状态码 + `响应正文已省略`，
  非法 JSON 只保留 serde 的位置信息。服务端结构化 `desc` 仍保留（诊断必需），并补上
  256 字符截断，避免服务端可控文本无界进入消息。公开签名未变，属实现向契约靠拢的行为收紧。
- 新增对抗用例 `error_messages_never_echo_response_body`（`tests/aidd_boundary.rs`）：
  以本地一次性 HTTP 桩回放「正文夹带凭据」的 400 与非 JSON 200 两种响应，断言错误消息
  不含凭据、SQL 片段与正文。先红后绿证据见 PR 描述（修复前实测消息为
  `远端返回错误(code=0): taos HTTP 400: password=s3cr3t-from-server; SELECT secret_col FROM t`）。

## [0.1.1] - 2026-09-22

### 新增

- 三类测试基线（特性 002）：`tests/tdd_contracts.rs`（逐公开入口的行为契约，头部
  `TDD-PROBE` 表登记「入口 / 变异 / 红 / 绿」）、`tests/sdd_spec.rs`（`docs/标准.md`
  全部 `##` 章节 1:1 对照的可执行断言）、`tests/aidd_boundary.rs`（AI 生成、人工复核
  后保留的边界用例）。三者全部离线运行，不依赖真实服务。
- live 真连服用例 `tests/live_taos.rs`（全部 `#[ignore]`，默认不参与 CI；凭据只读
  `FOUNDATIONX_TAOSX_*`）。双传输各一个独立用例：NativeWs（原生端口 6030 可达性 +
  WS 握手 + 短会话首帧 + 数据面往返）与 REST（taosAdapter 6041），两者均以唯一化
  超级表完成建表 → 批量写 → 查询 → 清理并断言无残留 → close。

### 修正

- **库名标识符补上长度上限**（`docs/标准.md` §2、`docs/API.md`、crate 级 rustdoc 均
  声明「超级表名 / 库名…限长」）。此前 `config.rs` 的 `valid_ident` 只校验首字符与
  字符集，库名无长度上限，与标准不符：现新增 `MAX_IDENT_BYTES = 192` 并使库名与子表名
  共用同一上界（`client.rs` 的 `validate_ident` 同步改用该常量，消除字面量漂移）。
  超长库名由「透传到服务端报错」变为**本地 fail-closed**；公开签名未变，属实现向契约
  靠拢的行为收紧。先红后绿证据见 PR 描述。

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
