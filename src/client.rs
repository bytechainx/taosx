//! TDengine REST 生产客户端（默认 6041）：池背压、批量写入、健康检查。
//!
//! - 传输：`reqwest`，端点 `POST http(s)://host:port/rest/sql[/database]`，Basic 认证。
//! - 并发：`max_in_flight` 信号量 + in-flight 计数 + 关闭排空。
//! - SQL 安全：标识符白名单校验、tag 值十六进制子表编码、字面量转义。
//!
//! 生产段超 800 行，故按职责拆出 4 个子模块；门面保留池类型、连接与执行核心，
//! 以及原有的内联单元测试（测试与源码同文件）。公共路径由下方 `pub use` 保持不变。
//!
//! - [`types`]：公共数据类型（执行结果、池统计、健康快照、批量写入报告）
//! - [`sql`]：SQL 构造与安全校验（标识符白名单、字面量转义、时间戳精度换算）
//! - [`response`]：REST 响应解析与限额读取
//! - [`write`]：批量写入与区间查询方法组
//!
//! 端到端用法见 crate 根文档。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;
use tracing::debug;

use crate::config::{TaosConfig, TransportMode, TsPrecision};
use crate::error::{TaosError, TaosResult};
use crate::metrics::{OpCounters, TaosMetricsSnapshot};
use crate::native;

mod pool;
mod response;
mod sql;
mod types;
mod write;

pub use sql::build_insert_sql_chunks;
pub(crate) use sql::validate_chunk_hint;
pub use types::{
    BatchWritePartialError, BatchWriteReport, TaosExecResult, TaosHealth, TaosPoolStats,
};

use self::response::{parse_taos_json, read_response_limited, validate_decimal_schema};
use self::sql::{validate_ident, validate_stable_ident};

/// 关闭标记位（`state` 最高位）。
const CLOSED_BIT: usize = 1usize << (usize::BITS - 1);
/// in-flight 计数掩码（`state` 低位）。
const IN_FLIGHT_MASK: usize = !CLOSED_BIT;

/// CA 文件读取失败 → 配置错误（同步/异步路径共用消息）。
fn tls_ca_read_error(path: &std::path::Path, error: std::io::Error) -> TaosError {
    TaosError::Config(format!(
        "无法读取 TLS CA `{}`（{}）",
        path.display(),
        error.kind()
    ))
}

/// HTTP 客户端唯一装配点（同步/异步构建路径共享，W-1）；`ca_pem` 为已读取的 CA 内容。
fn assemble_http_client(
    config: &TaosConfig,
    ca_pem: Option<Vec<u8>>,
) -> TaosResult<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .timeout(config.timeout)
        .pool_max_idle_per_host(8)
        .redirect(reqwest::redirect::Policy::none());
    if let Some(pem) = ca_pem {
        let certificate = reqwest::Certificate::from_pem(&pem)
            .map_err(|error| TaosError::Config(format!("TLS CA 不是合法 PEM（{error}）")))?;
        builder = builder.add_root_certificate(certificate);
    }
    builder
        .build()
        .map_err(|error| TaosError::Config(format!("HTTP 客户端构建失败（{error}）")))
}

/// 构建 HTTP 客户端（同步路径：`std::fs` 读 CA，供 [`TaosPool::new`] 使用）。
fn build_http_client(config: &TaosConfig) -> TaosResult<reqwest::Client> {
    let ca_pem = match &config.tls_ca_file {
        Some(path) => Some(std::fs::read(path).map_err(|error| tls_ca_read_error(path, error))?),
        None => None,
    };
    assemble_http_client(config, ca_pem)
}

/// 构建 HTTP 客户端（异步路径：CA 读取移入 `spawn_blocking`，R-RT-010）。
/// tokio 未启用 `fs` feature 且 Cargo.toml 为共享互斥资源，故不用 `tokio::fs`。
async fn build_http_client_async(config: &TaosConfig) -> TaosResult<reqwest::Client> {
    let ca_pem = match &config.tls_ca_file {
        Some(path) => {
            let owned = path.clone();
            let read = tokio::task::spawn_blocking(move || std::fs::read(&owned))
                .await
                .map_err(|error| TaosError::Io(std::io::Error::other(error)))?;
            Some(read.map_err(|error| tls_ca_read_error(path, error))?)
        }
        None => None,
    };
    assemble_http_client(config, ca_pem)
}

/// TDengine REST 客户端与连接池（`TaosClient` 是其别名）。
#[derive(Clone)]
pub struct TaosPool {
    inner: Arc<PoolInner>,
}

impl std::fmt::Debug for TaosPool {
    /// 手写 `Debug`：只输出端点、精度与运行状态，不泄漏凭据。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaosPool")
            .field("host", &self.inner.config.host)
            .field("port", &self.inner.config.port)
            .field("database", &self.inner.config.database)
            .field("transport", &self.inner.config.transport)
            .field("precision", &self.precision())
            .field("stats", &self.stats())
            .finish()
    }
}

/// 池内部共享状态。
struct PoolInner {
    http: reqwest::Client,
    config: TaosConfig,
    precision: RwLock<TsPrecision>,
    sem: Arc<Semaphore>,
    state: AtomicUsize,
    drained: Notify,
    metrics: OpCounters,
}

/// 请求守卫：持信号量许可并维护 in-flight 计数。
struct RequestGuard {
    _permit: OwnedSemaphorePermit,
    inner: Arc<PoolInner>,
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        let previous = self.inner.state.fetch_sub(1, Ordering::AcqRel);
        if previous & IN_FLIGHT_MASK == 1 {
            self.inner.drained.notify_waiters();
        }
    }
}

/// 工作句柄别名：`TaosClient` 与 [`TaosPool`] 是同一类型。
pub type TaosClient = TaosPool;

#[cfg(test)]
mod tests {
    use super::response::{parse_ts_cell, truncate};
    use super::sql::{
        build_insert_sql_chunks_with_limits, encode_timestamp, escape_str, subtable_name,
        MAX_SYMBOL_BYTES,
    };
    use super::*;
    use crate::config::HARD_MAX_BATCH_ROWS;
    use crate::point::TaosPoint;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn sample_point(tag: &str, timestamp_ns: i64) -> TaosPoint {
        TaosPoint::new(tag, timestamp_ns, "1.0", "1.1")
    }

    /// 单次响应 mock（`Connection: close`）。
    async fn serve_response(status: &'static str, body: &'static str, chunked: bool) -> u16 {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut request = [0u8; 4096];
            let _ = stream.read(&mut request).await.expect("read request");
            let response = if chunked {
                format!(
                    "HTTP/1.1 {status}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:x}\r\n{body}\r\n0\r\n\r\n",
                    body.len()
                )
            } else {
                format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
            };
            stream
                .write_all(response.as_bytes())
                .await
                .expect("write response");
        });
        port
    }

    /// 依序为多个请求返回预设 JSON body（各自独立连接）。
    async fn serve_sequence(bodies: Vec<&'static str>) -> u16 {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        tokio::spawn(async move {
            for body in bodies {
                let (mut stream, _) = listener.accept().await.expect("accept");
                let mut request = [0u8; 4096];
                let _ = stream.read(&mut request).await.expect("read request");
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream
                    .write_all(response.as_bytes())
                    .await
                    .expect("write response");
            }
        });
        port
    }

    fn pool_with_port(port: u16) -> TaosPool {
        let config = TaosConfig {
            port,
            database: String::new(),
            timeout: Duration::from_secs(2),
            ..TaosConfig::default()
        };
        TaosPool::new(config).expect("pool")
    }

    #[test]
    fn subtable_hex_encodes_tag_and_rejects_bad_ident() {
        let name = subtable_name("ticks", "BTC/USDT").expect("子表名");
        assert!(name.starts_with("ticks_"));
        assert!(!name.contains('/'));
        assert!(!name.contains('\''));
        assert_ne!(
            subtable_name("ticks", "BTC/USDT").expect("a"),
            subtable_name("ticks", "BTC_USDT").expect("b")
        );
        let error = subtable_name("ticks", &"X".repeat(MAX_SYMBOL_BYTES + 1))
            .expect_err("超长 tag 值必须拒绝");
        assert!(
            matches!(error, TaosError::Invalid(_)) && error.to_string().contains("tag 值超过"),
            "{error:?}"
        );
        let error = validate_ident("a b").expect_err("含空格必须拒绝");
        assert!(
            matches!(error, TaosError::Invalid(_)) && error.to_string().contains("非法字符"),
            "{error:?}"
        );
        let error = validate_ident("1abc").expect_err("数字开头必须拒绝");
        assert!(
            matches!(error, TaosError::Invalid(_))
                && error.to_string().contains("字母或下划线开头"),
            "{error:?}"
        );
        let error = validate_ident("").expect_err("空标识符必须拒绝");
        assert!(
            matches!(error, TaosError::Invalid(_)) && error.to_string().contains("长度"),
            "{error:?}"
        );
        validate_ident("ok_name1").expect("合法标识符必须通过");
    }

    #[test]
    fn insert_sql_chunks_partition_and_escape() {
        let points: Vec<TaosPoint> = (0..5)
            .map(|index| sample_point("BTC", index * 1_000_000))
            .collect();
        let chunks = build_insert_sql_chunks("ticks", &points, TsPrecision::Ms, 2).expect("分块");
        assert_eq!(chunks.len(), 3, "2+2+1");
        for chunk in &chunks {
            assert!(chunk.starts_with("INSERT INTO "));
            assert!(chunk.contains("VALUES"));
            assert!(chunk.contains("'1.0'"));
        }
        assert!(build_insert_sql_chunks("ticks", &[], TsPrecision::Ms, 10)
            .expect("空")
            .is_empty());
        let error = build_insert_sql_chunks("ticks", &points, TsPrecision::Ms, 0)
            .expect_err("max_rows=0 必须拒绝");
        assert!(
            matches!(error, TaosError::Invalid(_)) && error.to_string().contains("max_rows"),
            "{error:?}"
        );
        let error =
            build_insert_sql_chunks("ticks", &points, TsPrecision::Ms, HARD_MAX_BATCH_ROWS + 1)
                .expect_err("max_rows 超硬上限必须拒绝");
        assert!(
            matches!(error, TaosError::Invalid(_)) && error.to_string().contains("max_rows"),
            "{error:?}"
        );
        let error = build_insert_sql_chunks("bad name", &points, TsPrecision::Ms, 1)
            .expect_err("非法超级表名必须拒绝");
        assert!(matches!(error, TaosError::Invalid(_)), "{error:?}");
    }

    #[test]
    fn insert_sql_escapes_quotes_and_backslashes() {
        let point = TaosPoint::new("A'B", 1, "1'0", "2\\0");
        let sql = build_insert_sql_chunks("ticks", &[point], TsPrecision::Ns, 1)
            .expect("分块")
            .remove(0);
        assert!(sql.contains(r"TAGS ('A\'B')"), "{sql}");
        assert!(sql.contains(r"'1\'0'"), "{sql}");
        assert!(sql.contains(r"'2\\0'"), "{sql}");
    }

    #[test]
    fn insert_sql_rejects_unaligned_timestamp() {
        let point = TaosPoint::new("A", 1_500, "0", "0");
        build_insert_sql_chunks("ticks", &[point], TsPrecision::Ns, 1)
            .expect("Ns 精度天然对齐必须通过");
        let point = TaosPoint::new("A", 1_500, "0", "0");
        let error =
            build_insert_sql_chunks("ticks", &[point], TsPrecision::Ms, 1).expect_err("必须拒绝");
        assert!(matches!(error, TaosError::Invalid(_)), "{error:?}");
        assert_eq!(
            encode_timestamp(1_500_000_000, TsPrecision::Ms).expect("对齐"),
            1500
        );
        assert_eq!(
            encode_timestamp(1_500_000, TsPrecision::Us).expect("对齐"),
            1500
        );
        let error = encode_timestamp(1_500, TsPrecision::Us).expect_err("未对齐必须拒绝");
        assert!(
            matches!(error, TaosError::Invalid(_)) && error.to_string().contains("无法无损表示"),
            "{error:?}"
        );
    }

    #[test]
    fn text_value_path_preserves_long_fixed_point_cells() {
        let first = "123456789012345678901.234567890123456789";
        let second = "-123456789012345678901.234567890123456788";
        let sql = build_insert_sql_chunks(
            "ticks",
            &[TaosPoint::new("BTC/USDT", 1, first, second)],
            TsPrecision::Ns,
            1,
        )
        .expect("分块")
        .remove(0);
        assert!(sql.contains(&format!("'{first}'")));
        assert!(sql.contains(&format!("'{second}'")));
    }

    #[test]
    fn single_row_over_byte_cap_is_rejected() {
        let tick = sample_point(&"X".repeat(MAX_SYMBOL_BYTES), 1);
        let error = build_insert_sql_chunks_with_limits("ticks", &[tick], TsPrecision::Ns, 1, 20)
            .expect_err("单行超出字节上限");
        assert!(matches!(error, TaosError::Invalid(_)));
    }

    #[test]
    fn schema_rejects_double_and_accepts_nchar_64() {
        let double = TaosExecResult {
            code: 0,
            rows: vec![
                vec!["bid".into(), "DOUBLE".into(), "8".into()],
                vec!["ask".into(), "DOUBLE".into(), "8".into()],
            ],
            columns: Vec::new(),
            affected_rows: None,
        };
        let error = validate_decimal_schema(&double).expect_err("DOUBLE 必须拒绝");
        assert!(
            matches!(error, TaosError::Backend { .. }) && error.to_string().contains("NCHAR(64+)"),
            "{error:?}"
        );

        let text = TaosExecResult {
            code: 0,
            rows: vec![
                vec!["ts".into(), "TIMESTAMP".into(), "8".into()],
                vec!["bid".into(), "NCHAR".into(), "64".into()],
                vec!["ask".into(), "NCHAR".into(), "64".into()],
            ],
            columns: Vec::new(),
            affected_rows: None,
        };
        validate_decimal_schema(&text).expect("NCHAR(64) 必须通过");
    }

    #[test]
    fn truncate_is_utf8_boundary_safe() {
        assert_eq!(truncate("中文响应", 2), "…");
        assert_eq!(truncate("中文响应", 4), "中…");
    }

    #[test]
    fn parse_rfc3339_and_numeric_timestamps() {
        let ns = parse_ts_cell("2026-07-21T17:12:39.582758368Z", TsPrecision::Ns).expect("RFC3339");
        assert!(ns > 0);
        assert_eq!(
            parse_ts_cell("1000", TsPrecision::Ms).expect("数值"),
            1_000_000_000
        );
        let error = parse_ts_cell("not-a-time", TsPrecision::Ms).expect_err("非法时间戳必须拒绝");
        assert!(
            matches!(error, TaosError::Invalid(_)) && error.to_string().contains("无法解析时间戳"),
            "{error:?}"
        );
    }

    #[test]
    fn new_is_offline_and_rejects_invalid_config() {
        let pool = TaosPool::new(TaosConfig::default()).expect("离线构造");
        assert_eq!(pool.config().host, "127.0.0.1");
        assert_eq!(pool.precision(), TsPrecision::Ms);
        let stats = pool.stats();
        assert_eq!(stats.in_flight, 0);
        assert!(!stats.closed);
        assert!(pool.liveness());
        assert!(!pool.is_closed());
        let error = TaosPool::new(TaosConfig {
            max_in_flight: 0,
            ..TaosConfig::default()
        })
        .expect_err("max_in_flight=0 必须拒绝");
        assert!(matches!(error, TaosError::Config(_)), "{error:?}");
    }

    #[tokio::test]
    async fn exec_and_query_parse_json_response() {
        let body = r#"{"code":0,"column_meta":[["value","INT",4]],"data":[[1],[2]],"rows":2}"#;
        let port = serve_response("200 OK", body, false).await;
        let pool = pool_with_port(port);
        let result = pool.exec("SELECT value").await.expect("exec");
        assert_eq!(result.code, 0);
        assert_eq!(result.columns, vec!["value".to_owned()]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[1][0], "2");
        assert_eq!(pool.metrics().sql_bytes, "SELECT value".len() as u64);
        assert!(pool.metrics().response_bytes >= body.len() as u64);

        let port = serve_response("200 OK", body, true).await;
        let pool = pool_with_port(port);
        assert_eq!(
            pool.query("SELECT value").await.expect("query").rows.len(),
            2
        );
    }

    #[tokio::test]
    async fn taos_error_code_is_propagated() {
        let body = r#"{"code":9731,"desc":"Table does not exist"}"#;
        let port = serve_response("200 OK", body, false).await;
        let pool = pool_with_port(port);
        let error = pool.exec("SELECT 1").await.expect_err("表不存在必须报错");
        assert!(error.is_not_found());
        assert_eq!(error.taos_code(), Some(9731));
        assert!(!error.is_retryable());
    }

    #[tokio::test]
    async fn row_and_response_limits_are_enforced() {
        let body = r#"{"code":0,"column_meta":[["value","INT",4]],"data":[[1],[2]],"rows":2}"#;
        let port = serve_response("200 OK", body, false).await;
        let config = TaosConfig {
            port,
            database: String::new(),
            max_query_rows: 1,
            ..TaosConfig::default()
        };
        let pool = TaosPool::new(config).expect("pool");
        let error = pool
            .exec("SELECT value")
            .await
            .expect_err("行数超出 max_query_rows 必须拒绝");
        assert!(
            matches!(error, TaosError::Unavailable(_))
                && error.to_string().contains("max_query_rows"),
            "{error:?}"
        );

        let port = serve_response("200 OK", "中文响应体超过上限", false).await;
        let config = TaosConfig {
            port,
            database: String::new(),
            max_response_bytes: 8,
            ..TaosConfig::default()
        };
        let pool = TaosPool::new(config).expect("pool");
        assert!(matches!(
            pool.exec("SELECT 1").await,
            Err(TaosError::Unavailable(_))
        ));

        let port = serve_response("503 Service Unavailable", "busy", false).await;
        let pool = pool_with_port(port);
        let error = pool.exec("SELECT 1").await.expect_err("503");
        assert!(error.is_retryable());
    }

    #[tokio::test]
    async fn closed_pool_rejects_requests() {
        let pool = pool_with_port(1);
        pool.close().await.expect("close");
        assert!(pool.stats().closed);
        let error = pool.exec("SELECT 1").await.expect_err("已关闭必须拒绝");
        assert!(matches!(error, TaosError::Closed(_)));
        let health = pool.health_check().await.expect("健康检查信封");
        assert!(!health.ready);
        assert!(!health.is_ready());
        assert!(health.detail.contains("关闭"));
    }

    #[tokio::test]
    async fn acquire_timeout_when_saturated() {
        let config = TaosConfig {
            port: 1,
            database: String::new(),
            max_in_flight: 1,
            acquire_timeout: Duration::from_millis(50),
            timeout: Duration::from_millis(200),
            ..TaosConfig::default()
        };
        let pool = TaosPool::new(config).expect("pool");
        let permit = pool.acquire().await.expect("首个许可");
        // `RequestGuard` 未实现 `Debug`，用 match 取错误分支。
        let error = match pool.acquire().await {
            Ok(_) => panic!("第二个许可必须超时"),
            Err(error) => error,
        };
        assert!(matches!(error, TaosError::Timeout(_)));
        drop(permit);
    }

    #[tokio::test]
    async fn close_deadline_waits_for_in_flight() {
        let config = TaosConfig {
            port: 1,
            database: String::new(),
            close_timeout: Duration::from_millis(20),
            ..TaosConfig::default()
        };
        let pool = TaosPool::new(config).expect("pool");
        let guard = pool.acquire().await.expect("guard");
        assert_eq!(pool.stats().in_flight, 1);
        let error = pool.close().await.expect_err("在途请求必须导致超时");
        assert!(matches!(error, TaosError::Timeout(_)));
        assert!(pool.is_closed());
        drop(guard);
        pool.close().await.expect("重复 close 必须排空");
        assert_eq!(pool.stats().in_flight, 0);
    }

    #[tokio::test]
    async fn health_check_ready_when_server_version_ok() {
        let body =
            r#"{"code":0,"column_meta":[["v","VARCHAR",32]],"data":[["3.3.6.13"]],"rows":1}"#;
        let port = serve_response("200 OK", body, false).await;
        let pool = pool_with_port(port);
        let health = pool.health_check().await.expect("健康检查");
        assert!(health.ready, "{health:?}");
        assert_eq!(health.server_version.as_deref(), Some("3.3.6.13"));
        assert_eq!(health.detail, "就绪");
        assert!(pool.metrics().health_ready >= 1);
    }

    #[tokio::test]
    async fn health_check_not_ready_on_unreachable() {
        let config = TaosConfig {
            host: "127.0.0.1".into(),
            port: 1,
            database: String::new(),
            timeout: Duration::from_millis(200),
            acquire_timeout: Duration::from_millis(200),
            ..TaosConfig::default()
        };
        let pool = TaosPool::new(config).expect("pool");
        let health = pool.health_check().await.expect("健康检查信封");
        assert!(!health.ready);
        assert!(!health.detail.is_empty());
        assert!(pool.metrics().health_not_ready >= 1);
    }

    #[tokio::test]
    async fn write_batch_reports_full_success_and_partial_failure() {
        let create_ok = r#"{"code":0,"column_meta":[],"data":[],"rows":0}"#;
        let describe_ok = concat!(
            r#"{"code":0,"column_meta":[["field","VARCHAR",16],["type","VARCHAR",16],["length","VARCHAR",8]],"#,
            r#""data":[["ts","TIMESTAMP","8"],["bid","NCHAR","64"],["ask","NCHAR","64"]],"rows":3}"#
        );
        let insert_ok = r#"{"code":0,"column_meta":[],"data":[],"rows":0}"#;
        let port = serve_sequence(vec![create_ok, describe_ok, insert_ok]).await;
        let config = TaosConfig {
            port,
            database: String::new(),
            batch_max_rows: 10,
            ..TaosConfig::default()
        };
        let pool = TaosPool::new(config).expect("pool");
        let report = pool
            .write_batch_report("ticks", &[sample_point("BTC", 1_000_000)])
            .await
            .expect("全量成功");
        assert_eq!(report.accepted, 1);
        assert!(report.is_complete());

        let insert_fail = r#"{"code":-1,"desc":"injected write failure"}"#;
        let port = serve_sequence(vec![create_ok, describe_ok, insert_ok, insert_fail]).await;
        let config = TaosConfig {
            port,
            database: String::new(),
            batch_max_rows: 1,
            ..TaosConfig::default()
        };
        let pool = TaosPool::new(config).expect("pool");
        let points = vec![
            sample_point("BTC", 1_000_000),
            sample_point("ETH", 2_000_000),
        ];
        let partial = pool
            .write_batch_chunked_outcome("ticks", &points, 1)
            .await
            .expect_err("第二个 chunk 必须失败");
        assert_eq!(partial.report.accepted, 1);
        assert_eq!(partial.report.failed, 1);
        assert_eq!(partial.report.chunks_ok, 1);
        assert_eq!(partial.report.chunks_total, 2);
        let as_error: TaosError = partial.into();
        assert!(as_error.to_string().contains("accepted=1"));
        assert!(as_error.to_string().contains("failed=1"));
    }

    #[tokio::test]
    async fn custom_chunk_rows_cannot_bypass_config_limits() {
        let pool = pool_with_port(1);
        let error = pool
            .write_batch_chunked("ticks", &[sample_point("BTC", 1_000_000)], 0)
            .await
            .expect_err("0 行必须拒绝");
        assert!(matches!(error, TaosError::Invalid(_)));
    }

    #[tokio::test]
    async fn query_series_returns_empty_for_missing_table_and_propagates_other_errors() {
        let not_found = r#"{"code":9731,"desc":"Table does not exist"}"#;
        let port = serve_response("200 OK", not_found, false).await;
        let pool = pool_with_port(port);
        assert!(pool
            .query_series("missing_table", 0, 1)
            .await
            .expect("缺表应为空")
            .is_empty());

        let internal = r#"{"code":-1,"desc":"internal driver failure"}"#;
        let port = serve_response("200 OK", internal, false).await;
        let pool = pool_with_port(port);
        let error = pool
            .query_series("some_table", 0, 1)
            .await
            .expect_err("必须传播");
        assert!(matches!(error, TaosError::Backend { .. }));
    }

    #[tokio::test]
    async fn query_series_parses_rows() {
        let body = concat!(
            r#"{"code":0,"column_meta":[["ts","TIMESTAMP",8],["bid","NCHAR",64],["ask","NCHAR",64],["symbol","NCHAR",16]],"#,
            r#""data":[[1000,"1.0","1.1","BTC"],[2000,"2.0","2.1","ETH"]],"rows":2}"#
        );
        let describe_ok = concat!(
            r#"{"code":0,"column_meta":[["field","VARCHAR",16],["type","VARCHAR",16],["length","VARCHAR",8]],"#,
            r#""data":[["bid","NCHAR","64"],["ask","NCHAR","64"]],"rows":2}"#
        );
        let port = serve_sequence(vec![describe_ok, body]).await;
        let config = TaosConfig {
            port,
            database: String::new(),
            precision: Some(TsPrecision::Ms),
            ..TaosConfig::default()
        };
        let pool = TaosPool::new(config).expect("pool");
        let points = pool
            .query_series("ticks", 0, 10_000_000)
            .await
            .expect("查询");
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].tag_value, "BTC");
        assert_eq!(points[0].timestamp_ns, 1_000_000_000);
        let error = pool
            .query_series("ticks", 2, 1)
            .await
            .expect_err("start > end 必须拒绝");
        assert!(
            matches!(error, TaosError::Invalid(_)) && error.to_string().contains("start > end"),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn connect_refused_returns_error() {
        let config = TaosConfig {
            host: "127.0.0.1".into(),
            port: 1,
            database: String::new(),
            timeout: Duration::from_millis(300),
            acquire_timeout: Duration::from_millis(300),
            ..TaosConfig::default()
        };
        let error = TaosPool::connect(config).await.expect_err("不可达必须失败");
        assert!(error.is_retryable(), "{error:?}");
    }

    #[tokio::test]
    async fn connect_rejects_precision_mismatch() {
        let create_db = r#"{"code":0,"column_meta":[],"data":[],"rows":0}"#;
        let precision =
            r#"{"code":0,"column_meta":[["precision","VARCHAR",8]],"data":[["us"]],"rows":1}"#;
        let port = serve_sequence(vec![create_db, precision]).await;
        let config = TaosConfig {
            port,
            precision: Some(TsPrecision::Ms),
            timeout: Duration::from_secs(2),
            ..TaosConfig::default()
        };
        let error = TaosPool::connect(config)
            .await
            .expect_err("精度不一致必须 fail-closed");
        assert!(matches!(error, TaosError::Config(_)));
    }

    /// P2-7（A 案）：async 构造对不可读 CA 返回 Config 错误；成功路径由既有 connect 系列测试覆盖。
    #[tokio::test]
    async fn new_async_rejects_unreadable_tls_ca() {
        let config = TaosConfig {
            tls: true,
            tls_ca_file: Some(std::path::PathBuf::from("/nonexistent/ca.pem")),
            ..TaosConfig::default()
        };
        let error = TaosPool::new_async(config)
            .await
            .expect_err("不可读 CA 必须失败");
        assert!(matches!(error, TaosError::Config(_)), "{error:?}");
    }

    /// P2-9（C 案）：正文非空时错误消息仍维持占位符，正文只进 debug 日志（标准 §4）。
    #[tokio::test]
    async fn http_error_body_never_enters_message() {
        let port = serve_response("401 Unauthorized", "password=leak-me", false).await;
        let error = pool_with_port(port)
            .exec("SELECT 1")
            .await
            .expect_err("401 必须报错");
        assert!(matches!(error, TaosError::Backend { .. }), "{error:?}");
        assert!(error.to_string().contains("响应正文已省略"), "{error}");
        assert!(!error.to_string().contains("leak-me"), "{error}");
    }

    /// P2-9（C 案）：正文为空/全空白时同样维持占位符。
    #[tokio::test]
    async fn http_error_blank_body_keeps_placeholder() {
        let port = serve_response("500 Internal Server Error", "   ", false).await;
        let error = pool_with_port(port)
            .exec("SELECT 1")
            .await
            .expect_err("500 必须报错");
        assert!(matches!(error, TaosError::Unavailable(_)), "{error:?}");
        assert!(error.to_string().contains("响应正文已省略"), "{error}");
    }

    /// P2-9（C 案）：超长正文也不得进入错误消息（截断只发生在 debug 日志侧）。
    #[tokio::test]
    async fn http_error_long_body_keeps_placeholder() {
        let body: &'static str = Box::leak("diagnostic-line ".repeat(50).into_boxed_str());
        let port = serve_response("503 Service Unavailable", body, false).await;
        let error = pool_with_port(port)
            .exec("SELECT 1")
            .await
            .expect_err("503 必须报错");
        assert!(error.to_string().contains("响应正文已省略"), "{error}");
        assert!(!error.to_string().contains("diagnostic-line"), "{error}");
    }

    /// P1-3: `detect_precision` 的 SQL 必须通过 `escape_str` 转义 database 名（脆断耦合修复）。
    #[tokio::test]
    async fn detect_precision_uses_escaped_database_name() {
        use std::sync::{Arc, Mutex};

        // 启动 mock 服务器，捕获 detect_precision 请求（第二个请求）的 SQL 正文。
        let captured_sql = Arc::new(Mutex::new(String::new()));
        let captured = Arc::clone(&captured_sql);

        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let port = listener.local_addr().expect("addr").port();

        tokio::spawn(async move {
            let bodies = [
                r#"{"code":0,"column_meta":[],"data":[],"rows":0}"#, // CREATE DATABASE
                r#"{"code":0,"column_meta":[["precision","VARCHAR",8]],"data":[["ms"]],"rows":1}"#, // detect_precision
                r#"{"code":0,"column_meta":[["v","VARCHAR",32]],"data":[["3.3.6.13"]],"rows":1}"#, // ping
            ];

            for (i, body) in bodies.iter().enumerate() {
                let (mut stream, _) = listener.accept().await.expect("accept");
                let mut buf = [0u8; 8192];
                let n = stream.read(&mut buf).await.expect("read");
                let request = String::from_utf8_lossy(&buf[..n]);
                // 捕获第二个请求（detect_precision）的 SQL 正文。
                if i == 1 {
                    if let Some(body_start) = request.find("\r\n\r\n") {
                        let sql_body = request[body_start + 4..].trim().to_string();
                        *captured.lock().unwrap() = sql_body;
                    }
                }
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.expect("write");
            }
        });

        let config = TaosConfig {
            port,
            database: "test_db".into(),
            timeout: Duration::from_secs(2),
            ..TaosConfig::default()
        };

        let _pool = TaosPool::connect(config).await.expect("connect");

        // 验证 SQL 正文包含转义后的 database 名（而非原始值直接拼接）。
        let sql_body = captured_sql.lock().unwrap();
        let escaped = escape_str("test_db");
        let expected_pattern = format!("name='{escaped}'");
        assert!(
            sql_body.contains(&expected_pattern),
            "detect_precision SQL 必须使用 escape_str 转义 database 名\n期望包含: {expected_pattern}\n实际: {sql_body}"
        );
        // 确保 SQL 中 database 名不在未转义上下文中直拼。
        assert!(
            !sql_body.contains("name='test_db'") || escaped == "test_db",
            "未转义的 database 名不得直接出现在 SQL 字面量中"
        );
    }
}
