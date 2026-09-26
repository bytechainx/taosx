# taosx

TDengine（涛思）异步客户端。REST 为主传输，原生 WebSocket 为补充传输；内置连接池与背压、
分块批量写入、有界查询流、本地重试策略与 Prometheus 指标导出。

- 默认传输：HTTP REST `POST http(s)://host:port/rest/sql[/database]`（端口 6041，Basic 认证）
- 补充传输：原生 WebSocket `ws(s)://host:port/rest/ws`（握手探测 + 短会话 SQL）
- 零内部耦合：只依赖 crates.io 公开 crate，不依赖任何内部框架
- 硬上限 fail-closed：并发、批量行数/字节、响应体、查询行数、关闭 deadline 全部有上限常量
- 写路径安全：标识符白名单校验 + tag 值十六进制子表编码 + 字面量转义

## 安装

本 crate **不发布到 crates.io**，通过 git 依赖引入：

```toml
[dependencies]
taosx = { git = "https://github.com/bytechainx/taosx" }
```

## 最小可运行示例

```rust
use taosx::{TaosConfig, TaosPoint, TaosPool};

#[tokio::main]
async fn main() -> taosx::TaosResult<()> {
    // 1. 配置（也可用 TaosConfig::from_env() / from_toml() / builder()）
    let config = TaosConfig::builder()
        .host("127.0.0.1")
        .port(6041)
        .database("ticks")
        .user("root")
        .password(std::env::var("FOUNDATIONX_TAOSX_PASSWORD").unwrap_or_default())
        .build()?;

    // 2. 建连：CREATE DATABASE（如缺）→ 精度探测 → ping
    let client = TaosPool::connect(config).await?;

    // 3. 建表（`ensure_stable` 也会在批量写入前自动调用）
    client
        .exec(
            "CREATE STABLE IF NOT EXISTS `ticks` (\
               ts TIMESTAMP, bid NCHAR(64), ask NCHAR(64)\
             ) TAGS (symbol NCHAR(128))",
        )
        .await?;

    // 4. 写入（按 batch_max_rows / batch_max_bytes 自动分块）
    let points = vec![
        TaosPoint::new("BTC/USDT", 1_700_000_000_000_000_000, "66522.40", "66523.10"),
        TaosPoint::new("ETH/USDT", 1_700_000_001_000_000_000, "3000.10", "3000.20"),
    ];
    let report = client.write_batch_report("ticks", &points).await?;
    println!("accepted={} failed={}", report.accepted, report.failed);

    // 5. 查询（`query` 语义同 `exec`，返回结构化结果；`query_series` 返回类型化点）
    let result = client.query("SELECT COUNT(*) FROM `ticks`").await?;
    println!("code={} rows={:?}", result.code, result.rows);

    // 6. 健康检查
    client.ping().await?;
    let health = client.health_check().await?;
    println!("ready={} version={:?}", health.ready, health.server_version);

    client.close().await?;
    Ok(())
}
```

## 两种传输

### REST（默认，推荐）

`TransportMode::Rest`。所有读写都走 `POST /rest/sql`（带 database 时走 `/rest/sql/{database}`），
Basic 认证；请求/响应体大小都受 `batch_max_bytes` / `max_response_bytes` 约束，
读响应是流式累加并逐块校验上限，不会先整包读入再判断。连接池使用 reqwest 内建连接池，
并发由 `max_in_flight` 信号量控制；`close()` 会等待在途请求排空（受 `close_timeout` 约束）。

### 原生 WebSocket（`TransportMode::NativeWs`）

同端口 `/rest/ws` 通道，适合频繁小请求：

| 函数 | 作用 |
| --- | --- |
| `build_native_ws_url(&config)` | 纯函数，构造 `ws(s)://host:port/rest/ws` |
| `validate_mode(&config)` | 校验配置 + 传输模式一致性 |
| `connect_native_ws(&config)` | 有 deadline 的握手探测，成功即关闭 |
| `exec_sql_ws(&config, sql)` | 短会话两步协议：`conn` 建会话 → `query` → 读元数据帧 → 关闭 |
| `probe_native_tcp(&config, port)` | 原生 SQL 端口（默认 6030）可达性探测，不发协议帧 |

`TaosPool::new` **不发网**（`validate` + 装配 HTTP 客户端；若配置了 `tls_ca_file` 会同步读 PEM）。`TaosPool::connect` **会发网**：`NativeWs` 先做
WS 握手探测；`database` 非空时经 REST 建库并探测精度；两种模式最后都 `ping`。SQL 数据面
仍默认走 REST（可用 `TaosPool::exec_sql_ws` 显式走 WS）。`/rest/ws` 是**两步协议**：
`exec_sql_ws` 先发 `{"action":"conn","args":{"user":…,"password":…}}` 建会话，读到
`code == 0` 后再发 `{"action":"query","args":{"sql":…}}`，返回 `query` 的**元数据响应帧**。
**只有「明确读到整数 `code == 0`」才算成功**：非 0 `code`、非 JSON 帧、缺 `code` 字段、
`code` 非整数、以及既非 `Text` 也非 `Binary` 的帧（`Ping`/`Pong`/`Close`）一律
fail-closed 为 `TaosError::Unavailable`；凭据与服务端 `message` 文本均不入日志与错误消息。

> **阶段 1 边界**：只完成 `conn` 握手 + `query` 元数据帧 + 状态码错误映射。**结果行需在
> `query` 之后另发 `fetch`，本阶段不实现**，因此 `exec_sql_ws` 的返回值**不含结果行**，
> 不得据此声称已支持完整结果读取。

## SQL 注入防护

所有进入 SQL 文本的调用方输入都经白名单或转义，`build_insert_sql_chunks` 是唯一构造
INSERT 的入口：

| 成分 | 处理 |
| --- | --- |
| 超级表名 / 库名 | 标识符白名单：首字符为字母或下划线，其余仅 `[A-Za-z0-9_]`；表名 ≤ 94 字节，标识符 ≤ 192 字节，非法即返回 `TaosError::Invalid` |
| 子表名 | 由 `{stable}_s{tag 值 UTF-8 十六进制}` 生成，tag 值不直接进入标识符，天然规避反引号/引号/路径字符 |
| tag 值（`TAGS` 字面量） | 长度 ≤ 48 字节；`\` → `\\`，`'` → `\'` |
| 两个数据列字面量 | 同上转义规则 |
| 时间戳 | 只以十进制整数拼接；未对齐目标精度时 fail-closed，禁止静默截断 |
| 参数化 | 单条 INSERT 的 `max_rows` / `max_bytes` 与硬上限比较，越界即拒绝（不会拼接出超大语句） |

示例（`tests/pure_functions.rs` 有对应断言）：

```text
输入 tag = "a'; DROP DATABASE x; --"
输出 SQL 片段：`ticks_s61273b2044524f502220544154414241534520783b202d2d` USING `ticks` TAGS ('a\'; DROP DATABASE x; --') VALUES (...)
```

> `query_series` / `query_series_stream` 不接收 SQL，只接收表名 + 纳秒区间，表名同样走
> 标识符校验；`exec` / `query` 是给调用方的原样 SQL 通道，请勿拼接未校验的外部输入。

## 配置项

`TaosConfig` 字段全部 `pub`，可用结构体字面量 + `..Default::default()`、`TaosConfigBuilder`
或 `TaosConfig::from_env()` / `from_toml()` 构造；`validate()` 在连接前 fail-fast。

| 字段 | TOML / 环境变量 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `host` | `host` / `FOUNDATIONX_TAOSX_HOST` | `127.0.0.1` | 主机名或 IP；非 loopback 必须启用 TLS + 密码 |
| `port` | `port` / `FOUNDATIONX_TAOSX_PORT` | `6041` | REST / WS 端口 |
| `database` | `database` / `FOUNDATIONX_TAOSX_DATABASE` | `infra_draft` | 库名（标识符校验），空串表示不指定 |
| `user` | `user` / `FOUNDATIONX_TAOSX_USER` | `root` | 用户名 |
| `password` | 仅 `FOUNDATIONX_TAOSX_PASSWORD` / builder | 空 | **敏感**：`Debug` 脱敏为 `***`，TOML 禁止非空值 |
| `tls` | `tls` / `FOUNDATIONX_TAOSX_TLS` | `false` | 启用 HTTPS / WSS |
| `tls_ca_file` | `tls_ca_file` / `FOUNDATIONX_TAOSX_TLS_CA_FILE` | `None` | 私有 CA 的 PEM；配置时必须启用 `tls` |
| `timeout` | `timeout_ms` / `FOUNDATIONX_TAOSX_TIMEOUT_MS` | `10s` | 单请求超时 |
| `precision` | `precision` / `FOUNDATIONX_TAOSX_PRECISION` | `None`（连接后探测） | `ms` / `us` / `ns`；显式值与库不一致时 fail-closed |
| `transport` | `transport` / `FOUNDATIONX_TAOSX_TRANSPORT` | `rest` | `rest` / `native` / `ws` |
| `max_in_flight` | `max_in_flight` / `FOUNDATIONX_TAOSX_MAX_IN_FLIGHT` | `64` | 并发上限，`1..=1024` |
| `acquire_timeout` | `acquire_timeout_ms` / `FOUNDATIONX_TAOSX_ACQUIRE_TIMEOUT_MS` | `5s` | 等待 in-flight 许可超时 |
| `batch_max_rows` | `batch_max_rows` / `FOUNDATIONX_TAOSX_BATCH_MAX_ROWS` | `500` | 单批最大行数，`1..=10000` |
| `batch_max_bytes` | `batch_max_bytes` / `FOUNDATIONX_TAOSX_BATCH_MAX_BYTES` | `1 MiB` | 单条 SQL 最大字节，`1..=8 MiB` |
| `max_response_bytes` | `max_response_bytes` / `FOUNDATIONX_TAOSX_MAX_RESPONSE_BYTES` | `8 MiB` | 响应体上限，`1..=64 MiB` |
| `max_query_rows` | `max_query_rows` / `FOUNDATIONX_TAOSX_MAX_QUERY_ROWS` | `10000` | 单次查询行数上限，`1..=100000` |
| `close_timeout` | `close_timeout_ms` / `FOUNDATIONX_TAOSX_CLOSE_TIMEOUT_MS` | `5s` | 关闭排空 deadline，≤ `30s` |
| `hosts` | `hosts` / `FOUNDATIONX_TAOSX_HOSTS` | `[]` | 备用主机，逗号分隔；主 host 失败后按序故障转移 |
| `write_max_attempts` | `write_max_attempts` / `FOUNDATIONX_TAOSX_WRITE_MAX_ATTEMPTS` | `1` | 幂等写重试次数（含首次），≥ 1 |

TOML 形态（`schema_version` 必填，未知字段拒绝）：

```toml
schema_version = 1
host = "127.0.0.1"
port = 6041
database = "ticks"
user = "writer"
tls = false
timeout_ms = 10000
precision = "ns"
transport = "rest"
max_in_flight = 64
batch_max_rows = 500
hosts = ["127.0.0.2"]
```

硬上限常量：`HARD_MAX_IN_FLIGHT`、`HARD_MAX_BATCH_ROWS`、`HARD_MAX_BATCH_BYTES`、
`HARD_MAX_RESPONSE_BYTES`、`HARD_MAX_QUERY_ROWS`、`HARD_MAX_CLOSE_TIMEOUT`。

## 错误分类

`TaosError` 覆盖 `Config` / `Connection` / `Backend` / `Unavailable` / `Serialization` /
`Io` / `Timeout` / `Invalid` / `Closed` / `Unsupported`，并提供：

- `is_retryable()`：仅 `Connection` / `Unavailable` / `Timeout` / `Io` 可重试；
- `from_taos_code(code, ctx)`：TDengine `code` 语义映射——`896`（服务端繁忙）→ 可重试，
  `0x2603` / `9826`（表不存在）→ `is_not_found()` 为真，其它正数 → `Invalid`，非正数 → `Backend`；
- `from_http_status(status, ctx)`：`408` → `Timeout`，`429` / `5xx` → `Unavailable`，其余 → `Backend`。

`RetryPolicy` 提供指数退避 + 抖动 + deadline（`compute_backoff` 为可测纯函数），
`for_read()` / `for_idempotent_write()` 给出读/幂等写预设。

## 迁移取舍

- **保留**：REST 客户端与连接池、批量写入与分块 SQL、查询流、重试、指标、
  `HARD_MAX_*` 硬上限、传输模式与精度枚举、原生 WS（`build_native_ws_url` /
  `connect_native_ws` / `exec_sql_ws` / `probe_native_tcp` / `validate_mode`）。
- **移除**：主工程内部的 `kernel`（`XError` / `ErrorKind` / `PrecisionLossPolicy`）依赖、
  时间精度合同模块 `time_storage`、`selfcheck` 自验证框架、`soak`、`tmq`、
  `adapter`（scaffold）、主工程专属的环境变量前缀与分层 TOML 加载。
- 原 `time_storage` 的「禁止静默精度损失」语义以内联方式保留在
  `build_insert_sql_chunks` 中：纳秒时间戳无法无损换算为目标精度时直接返回
  `TaosError::Invalid`，不再依赖外部 `PrecisionLossPolicy`。
- `TaosQueryStream` 保持源语义（受 `max_query_rows` 约束的「先有界物化、再逐行 yield」），
  不是服务端游标；`chunk_hint` 作为拉取块大小提示并做参数校验。

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion
in this crate by you, as defined in the Apache-2.0 license, shall be dual licensed as above,
without any additional terms or conditions.
