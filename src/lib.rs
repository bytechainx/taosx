//! `taosx` — TDengine 异步客户端（REST + 原生 WebSocket）。
//!
//! 本 crate 把 TDengine 访问能力收敛为一组零内部耦合的组件：
//! 连接池与背压、批量写入与分块 SQL 构造、有界查询流、重试策略、指标导出。
//!
//! # 两种传输
//!
//! - **REST（默认）**：`reqwest` → `POST http(s)://host:port/rest/sql[/database]`，
//!   默认端口 6041，Basic 认证。覆盖面最广，是数据读写的主路径。
//! - **Native WebSocket**：`tokio-tungstenite` → `ws(s)://host:port/rest/ws`，
//!   由 [`connect_native_ws`] 做握手探测、[`exec_sql_ws`] 执行短会话 SQL、
//!   [`probe_native_tcp`] 探测原生端口。`TransportMode::NativeWs` 时
//!   [`TaosPool::connect`] 会先完成一次 WS 握手探测。
//!
//! # SQL 注入防护
//!
//! 所有进入 SQL 文本的调用方输入都经过白名单或转义：
//! 超级表名 / 库名走标识符校验（字母或下划线开头，仅含 `[A-Za-z0-9_]`，且限长），
//! tag 值十六进制编码进子表名，字符串字面量按 TDengine 规则转义
//! （`\` → `\\`、`'` → `\'`），时间戳只以十进制整数拼接。
//! 详见 [`build_insert_sql_chunks`]。
//!
//! # 最小示例
//!
//! ```no_run
//! use taosx::{TaosConfig, TaosPoint, TaosPool};
//!
//! # #[tokio::main]
//! # async fn main() -> taosx::TaosResult<()> {
//! let config = TaosConfig::builder().host("127.0.0.1").database("ticks").build()?;
//! let client = TaosPool::connect(config).await?;
//!
//! client
//!     .exec(
//!         "CREATE STABLE IF NOT EXISTS `ticks` (\
//!            ts TIMESTAMP, bid NCHAR(64), ask NCHAR(64)\
//!          ) TAGS (symbol NCHAR(128))",
//!     )
//!     .await?;
//!
//! let points = vec![TaosPoint::new("BTC/USDT", 1_700_000_000_000_000_000, "66522.40", "66523.10")];
//! client.write_batch("ticks", &points).await?;
//!
//! let result = client.query("SELECT COUNT(*) FROM `ticks`").await?;
//! println!("rows={}", result.rows.len());
//!
//! client.ping().await?;
//! client.close().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # 公开 API 一览
//!
//! - 客户端： [`TaosPool`]（别名 [`TaosClient`]）、[`TaosPoolStats`]、
//!   [`TaosHealth`]、[`TaosExecResult`]、[`BatchWriteReport`]、[`BatchWritePartialError`]
//! - 配置： [`TaosConfig`]、[`TaosConfigBuilder`]、[`TransportMode`]、[`TsPrecision`]、
//!   [`HARD_MAX_IN_FLIGHT`] 等硬上限常量
//! - 错误： [`TaosError`]、[`TaosResult`]
//! - 写入： [`TaosPoint`]、[`build_insert_sql_chunks`]、[`WriteBatcher`]、
//!   [`WriteBatcherConfig`]、[`BatcherCloseReport`]、[`BatcherCloseError`]
//! - 查询： [`TaosQueryStream`]
//! - 可靠性： [`RetryPolicy`]
//! - 观测： [`TaosMetricsSnapshot`]、[`ws_probe_totals`]
//! - 原生 WS： [`build_native_ws_url`]、[`connect_native_ws`]、[`exec_sql_ws`]、
//!   [`probe_native_tcp`]、[`validate_mode`]

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(unreachable_pub)]
#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod batcher;
mod client;
mod config;
mod error;
mod metrics;
mod native;
mod point;
mod retry;
mod stream;

pub use batcher::{BatcherCloseError, BatcherCloseReport, WriteBatcher, WriteBatcherConfig};
pub use client::{
    build_insert_sql_chunks, BatchWritePartialError, BatchWriteReport, TaosClient, TaosExecResult,
    TaosHealth, TaosPool, TaosPoolStats,
};
pub use config::{
    TaosConfig, TaosConfigBuilder, TransportMode, TsPrecision, DEFAULT_DATABASE, DEFAULT_HOST,
    DEFAULT_PORT, DEFAULT_USER, ENV_ACQUIRE_TIMEOUT_MS, ENV_BATCH_MAX_BYTES, ENV_BATCH_MAX_ROWS,
    ENV_CLOSE_TIMEOUT_MS, ENV_DATABASE, ENV_HOST, ENV_HOSTS, ENV_MAX_IN_FLIGHT, ENV_MAX_QUERY_ROWS,
    ENV_MAX_RESPONSE_BYTES, ENV_PASSWORD, ENV_PORT, ENV_PRECISION, ENV_PREFIX, ENV_TIMEOUT_MS,
    ENV_TLS, ENV_TLS_CA_FILE, ENV_TRANSPORT, ENV_USER, ENV_WRITE_MAX_ATTEMPTS,
    HARD_MAX_BATCH_BYTES, HARD_MAX_BATCH_ROWS, HARD_MAX_CLOSE_TIMEOUT, HARD_MAX_IN_FLIGHT,
    HARD_MAX_QUERY_ROWS, HARD_MAX_RESPONSE_BYTES, HARD_MAX_TIMEOUT, HARD_MAX_WRITE_MAX_ATTEMPTS,
};
pub use error::{TaosError, TaosResult};
pub use metrics::{ws_probe_totals, TaosMetricsSnapshot};
pub use native::{
    build_native_ws_url, connect_native_ws, exec_sql_ws, probe_native_tcp, validate_mode,
};
pub use point::TaosPoint;
pub use retry::RetryPolicy;
pub use stream::TaosQueryStream;
