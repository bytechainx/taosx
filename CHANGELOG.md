# Changelog — taosx

本文件记录 `taosx` 的用户可见变更，遵循 [Keep a Changelog](https://keepachangelog.com/)
与 [Semantic Versioning](https://semver.org/)。

本仓库代码自 `xhyper.rs` 的 `crates/platform/drivers/taos` 抽取而来（抽取时点为 `0.3.16`）。
该工程内的版本线不在本文件中延续，本仓库从 `0.1.0` 重新起算。

## [Unreleased]

### 文档

- **`connect` 与版本自称对齐实现**：`docs/API.md` / `docs/标准.md` 改为 Cargo `0.1.5`；删除「REST `connect` 不发网」过期句。现行行为：`NativeWs` 先握手；`database` 非空则 REST `CREATE DATABASE IF NOT EXISTS` + 精度探测；两种模式均 `ping`。

### 变更

- **非成功 HTTP 响应的诊断输出改经 debug 日志**（P2-9）：错误消息维持「响应正文已省略」
  占位符（安全边界，由 `tests/aidd_boundary.rs` 锁定，不回显响应正文），截断 256 字节的
  正文改经 `debug!(target: "taosx", status, body = …)` 输出，诊断信息仅在 debug 日志级别展开。
- **`json_cell_to_string` 文档标注 Number 精度局限**（P2-8）：大浮点值可能以科学记数法
  输出（如 `1e300` → `"1e+300"`），以特征化测试锁定当前行为；生产逻辑不变。
- **补齐三处语义文档/注释**（P2-11、P2-13、P2-14）：`WriteBatcher::push` 取消语义
  （flush future 被取消时已 take 的 batch 丢失，非 exactly-once）、retry fallback
  逻辑不可达分支注释、批量缓冲容量 `min(1024)` 预分配权衡注释。
- **CI 门禁命令与 AGENTS.md 对齐为组织标准**（P2-20）：`cargo fmt --all -- --check` /
  `cargo clippy --workspace --all-targets --all-features -- -D warnings` /
  `cargo test --workspace --all-features`，两处逐字一致；`cargo doc` 与 `cargo deny check`
  补书面暂缓声明（doc 由 `#![deny(missing_docs)]` + doctest 覆盖；deny.toml 计划 2026-12 前建立）。

### 修复

- **`TransportMode::as_str()` 的输出 `parse()` 不接受（issue #16）**：`as_str()` 对 `NativeWs`
  返回 `"nativews"`，而 `parse` 的接受集只有 `native` / `ws` / `native_ws` / `native-ws`
  ⇒ `parse(TransportMode::NativeWs.as_str())` 返回 `None`（`Rest` 与同文件的 `TsPrecision`
  的 `ms`/`us`/`ns` 三档都对称，故属**疏漏**而非有意语义）。
  影响**两条配置入口**：`FOUNDATIONX_TAOSX_TRANSPORT=nativews` 与 TOML `transport = "nativews"`
  （后者经 `src/config/parse.rs` 的 `de_transport` 走同一个 `parse`）都会被 fail-closed 拒绝 ——
  而 `nativews` 恰是本仓公开 API `as_str()` 给出的规范拼写。现把 `"nativews"` 纳入接受集，
  两个变体的往返均成立（大小写与首尾空白不敏感）；`de_transport` 的文档注释与错误提示同步列全接受集。
  属**实现向契约靠拢**（`docs/versioning.md` §5「实现向契约靠拢」除外条款；与同一函数据此判 PATCH 的
  先例同型）：函数注释承诺的接受集与同类型 `as_str()` 的输出本应自洽，实现与之不符。
  **版本不在本条切**：`[Unreleased]` 已承载 P1×5 与 P2 两批条目（自 `3305472` 起已合并、未发布），
  单独切版会把它们一并归入同一版本号 ⇒ 留给该批次的发布方在切版时一并带走。

### 测试

- 补 `write_batch_idempotent` 直接测试（P2-15）：mock 回放 896 繁忙错误驱动整批重试，
  断言请求次数与 report 字段；不可重试子路径断言本地拒绝未触达网络。同时修正
  `tests/http_roundtrip.rs` 非法表名段的失实注释并强化其断言
- 补 batcher 时间窗触发 flush 测试（P2-16）：`flush_interval` 10ms + sleep 驱动，
  断言 totals 由 (0,0) 到 (2,0)，消除该路径零覆盖
- 强化 `tests/` 目录 7 个文件共 37 处裸 `is_err()` / `is_ok()` 断言（P2-17，R-TEST-001/002）：
  改为错误变体 `matches!` 判定，关键场景补消息内容与「不回显非法标识符」断言；
  1 处紧跟类型断言的计数用法保留并就地注释理由
- `tests/live_taos.rs` 两处 `#[ignore]` 补 owner 与 2026-12 复查期限（P2-18）
- 清理 `tests/api_surface.rs` 重言式断言（常量求和恒真比较改为有鉴别力的秩序关系断言），
  并补 `ws_probe_totals` 数值校验测试：触发一次不可达握手，严格断言失败计数 +1、
  成功计数不变（P2-19）

## [0.1.5] - 2026-09-23

### 修复

- **原生 WS `exec_sql_ws` 缺 `conn` 会话握手，且把错误信封当成功返回（语义级 fail-open）**：
  此前实现连上 `/rest/ws` 后**直接发 `{"action":"query",…}`**（没有 `conn` 建会话），且只
  判断「响应体非空」即 `Ok(body)`。两个后果：**(a)** TDengine 对未握手请求一律回
  `code:65535`（"server not connected"），请求实际从未执行；**(b)** 该错误信封被当作查询
  结果返回给调用方。
  现改为**两步协议**：发 `{"action":"conn","args":{"user":…,"password":…}}`（凭据取自
  `TaosConfig`）→ 校验其响应 `code == 0` → 发 `{"action":"query","args":{"sql":…}}` →
  校验其响应 `code == 0` → 返回 `query` 元数据帧。
  **只有「明确读到整数 `code == 0`」才算成功**；以下一律 fail-closed 为
  `TaosError::Unavailable`：响应帧既非 `Text` 也非 `Binary`（`Ping`/`Pong`/`Close`）、
  帧不是合法 JSON、JSON 合法但缺 `code` 字段、`code` 存在但非整数（例如字符串 `"0"`）、
  `code` 非 `0`。服务端 `message` 文本与口令一律不入错误消息。
  属**实现向契约靠拢**（`docs/versioning.md` §3.1）：既有文档与函数注释已承诺「执行 SQL
  且 fail-closed」，实现与之不符，故级别为 **PATCH**。
  **边界**：本次只完成 `conn` 握手、`query` 元数据帧与状态码错误映射；**结果行需在
  `query` 之后另发 `fetch`，未实现**，故 `exec_sql_ws` 的返回值**不含结果行**。
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
- **`timeout` / `acquire_timeout` 新增硬上界**（P2-1）：`HARD_MAX_TIMEOUT`（3600 秒），
  `validate()` 超界报错。此前仅有下界校验，TOML `u64::MAX` 毫秒会饱和为 `Duration::MAX`，
  超时形同虚设；现由上界校验 fail-fast 兜底。
- **`write_max_attempts` 新增硬上界**（P2-2）：`HARD_MAX_WRITE_MAX_ATTEMPTS`（10），
  `validate()` 超界报错，消息含允许范围与当前值。
- **`env_parsed` 空串/空白串视为未设置**（P2-4）：返回 `Ok(None)`，对齐 `env_non_empty` /
  `env_trimmed` 语义；非空但非法的取值仍报错且只报告变量名、不回显取值。
- **TOML `password` 非字符串类型报错不泄漏取值**（P2-5）：错误消息改为「必须为字符串类型；
  禁止非空 password 字段，请改用环境变量注入」，揭示类型问题并引导正确注入方式。
- **`escape_str` 补充 NUL 转义**（P2-6）：`\0` → `\\0`，加固 SQL 字符串字面量的注入防护
  （防御性加固，TDengine 语义下 NUL 本非合法字面量成分）。
- **`build_http_client` 的 async 路径消除同步文件 I/O**（P2-7，R-RT-010）：CA 证书读取改经
  `tokio::task::spawn_blocking(std::fs::read)`；新增共享装配点 `assemble_http_client` 与
  异步构造 `new_async`（`pub(super)`，crate 外不可见），同步 `TaosPool::new` 保留
  `std::fs::read`（R-RT-010 只约束 async 可达路径）。实施偏差说明：因本仓 tokio 未启用
  `fs` feature，以 `spawn_blocking` 替代 `tokio::fs::read`（组织认可手段，零 Cargo.toml 变更）。
- **WS 帧/消息大小上限联动 `max_response_bytes`**（P2-10）：`exec_sql_ws` 改用
  `connect_async_with_config`，经私有纯函数 `ws_config_from` 把 `max_frame_size` /
  `max_message_size` 绑定为 `Some(config.max_response_bytes)`，防止服务端超大帧导致无界
  内存放大，与 REST 路径响应限额策略一致；`connect_native_ws` 握手探测不读数据帧，保持默认。
- **`exec_sql_ws` 关闭错误不再静默**（P2-12）：close 失败改经 `debug!` 日志输出
  （响应已获取，不影响正确性，但不再无声吞错）。

### 测试

- 补 `with_message` 对 `Io` 变体的两条单测（消息替换 + kind 保留）
- 补 `validate()` 超时字段级错误消息的独立单测
- 补 `detect_precision` 经 `escape_str` 转义 database 名的 HTTP 捕获回归测试
- 补多主机 failover 成功路径离线 mock 测试（`tests/failover_success.rs`）：首 host 失败、
  次 host 成功 + 多备用 host 两场景
- 补原生 WebSocket 层离线 mock 测试（`tests/ws_native_mock.rs`，5 用例）：按两步协议
  驱动「一次客户端请求 → 一批响应帧」，覆盖 Text 帧、Binary 帧解码、握手阶段非数据帧
  （Close / Ping）fail-closed、握手响应前连接即结束 fail-closed
- 新增 `tests/ws_conn_handshake.rs`（9 用例）：断言实现真的按 `conn` → `query` 顺序发送
  且凭据取自配置、握手失败 ⇒ `Err` 且不再发 `query`、非 0 `code` ⇒ `Err`（fail-open 必红
  对照）、P5「未握手」信封 ⇒ `Err`、以及非 JSON / 缺 `code` / `code` 非整数 / query 阶段
  缺 `code` / query 阶段非数据帧 各一条 ⇒ 均 `Err`
- 补 7 条 `src/native.rs` 内联单测：状态码严格解析（含超出 `i32` 的整数被拒）、非 0 码
  映射、畸形状态消息文案、JSON 类型名与帧标签全覆盖、控制帧 fail-closed、Binary 帧非法
  UTF-8 不得被当作成功

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
