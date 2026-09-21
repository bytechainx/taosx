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

/// 构建 HTTP 客户端（超时、连接池、禁用重定向、可选私有 CA）。
fn build_http_client(config: &TaosConfig) -> TaosResult<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .timeout(config.timeout)
        .pool_max_idle_per_host(8)
        .redirect(reqwest::redirect::Policy::none());
    if let Some(path) = &config.tls_ca_file {
        let pem = std::fs::read(path).map_err(|error| {
            TaosError::Config(format!(
                "无法读取 TLS CA `{}`（{}）",
                path.display(),
                error.kind()
            ))
        })?;
        let certificate = reqwest::Certificate::from_pem(&pem)
            .map_err(|error| TaosError::Config(format!("TLS CA 不是合法 PEM（{error}）")))?;
        builder = builder.add_root_certificate(certificate);
    }
    builder
        .build()
        .map_err(|error| TaosError::Config(format!("HTTP 客户端构建失败（{error}）")))
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

impl TaosPool {
    /// 同步构造（**仅校验配置**，不建立网络连接、不探测服务端）。
    ///
    /// 适用于 fail-closed 校验与离线背压路径；生产入口请使用 [`TaosPool::connect`]。
    pub fn new(config: TaosConfig) -> TaosResult<Self> {
        config.validate()?;
        let http = build_http_client(&config)?;
        let initial_precision = config.precision.unwrap_or(TsPrecision::Ms);
        let max_in_flight = config.max_in_flight;
        Ok(Self {
            inner: Arc::new(PoolInner {
                http,
                config,
                precision: RwLock::new(initial_precision),
                sem: Arc::new(Semaphore::new(max_in_flight)),
                state: AtomicUsize::new(0),
                drained: Notify::new(),
                metrics: OpCounters::new(),
            }),
        })
    }

    /// 连接：构建 HTTP 客户端、可选建库、探测精度、`ping`。
    ///
    /// - `TransportMode::NativeWs` 时先做一次原生 WS 握手探测（失败即返回）。
    /// - 主 `host` 不可用时按 `config.hosts` 顺序故障转移。
    /// - 配置精度与数据库实际精度不一致时 fail-closed。
    pub async fn connect(config: TaosConfig) -> TaosResult<Self> {
        config.validate()?;
        let mut last_error = TaosError::Unavailable("无可用 TDengine endpoint".to_owned());
        for host in config.endpoint_hosts() {
            let mut attempt_config = config.clone();
            attempt_config.host = host;
            match Self::connect_one(attempt_config).await {
                Ok(pool) => return Ok(pool),
                Err(error) => last_error = error,
            }
        }
        Err(last_error)
    }

    /// 从环境变量连接（前缀 `FOUNDATIONX_TAOSX_`）。
    pub async fn connect_from_env() -> TaosResult<Self> {
        Self::connect(TaosConfig::from_env()?).await
    }

    /// 单主机连接流程。
    async fn connect_one(config: TaosConfig) -> TaosResult<Self> {
        config.validate()?;

        if config.transport == TransportMode::NativeWs {
            native::connect_native_ws(&config).await?;
        }
        let pool = Self::new(config)?;

        // REST 路径：确保 database 存在 + 精度探测 + ping。
        // NativeWs 仅完成握手探测；SQL 默认仍走 REST（`exec_sql_ws` 提供 WS 会话）。
        if !pool.inner.config.database.is_empty() {
            let database = pool.inner.config.database.clone();
            validate_ident(&database)?;
            pool.exec_sql_raw(
                &format!("CREATE DATABASE IF NOT EXISTS `{database}` KEEP 3650"),
                false,
            )
            .await?;
            let detected = pool.detect_precision().await?;
            if let Some(configured) = pool.inner.config.precision {
                if configured != detected {
                    return Err(TaosError::Config(format!(
                        "配置精度 {configured:?} 与数据库精度 {detected:?} 不一致"
                    )));
                }
            }
            *pool
                .inner
                .precision
                .write()
                .map_err(|_| TaosError::Config("精度状态锁已中毒".to_owned()))? = detected;
        }

        pool.ping().await?;
        Ok(pool)
    }

    /// 工作客户端（`Clone` 便宜，内部共享 `Arc`）。
    #[must_use]
    pub fn client(&self) -> TaosClient {
        self.clone()
    }

    /// 配置。
    #[must_use]
    pub fn config(&self) -> &TaosConfig {
        &self.inner.config
    }

    /// 当前生效精度。
    #[must_use]
    pub fn precision(&self) -> TsPrecision {
        self.inner
            .precision
            .read()
            .map_or(TsPrecision::Ms, |guard| *guard)
    }

    /// 池瞬时统计。
    #[must_use]
    pub fn stats(&self) -> TaosPoolStats {
        let state = self.inner.state.load(Ordering::Acquire);
        TaosPoolStats {
            in_flight: state & IN_FLIGHT_MASK,
            closed: state & CLOSED_BIT != 0,
        }
    }

    /// 进程内有界操作计数快照（含进程级 WS 探测累计）。
    #[must_use]
    pub fn metrics(&self) -> TaosMetricsSnapshot {
        self.inner.metrics.snapshot()
    }

    /// 指标以 Prometheus 文本导出。
    #[must_use]
    pub fn metrics_prometheus(&self) -> String {
        self.metrics().to_prometheus_text()
    }

    /// 是否已关闭。
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.state.load(Ordering::Acquire) & CLOSED_BIT != 0
    }

    /// liveness：仅看本地池是否仍接受请求（不访问网络）。
    #[must_use]
    pub fn liveness(&self) -> bool {
        !self.is_closed()
    }

    /// 健康检查：本地 open + 有 deadline 的 `SELECT SERVER_VERSION()`。
    ///
    /// 未就绪时返回 `Ok(TaosHealth { ready: false, .. })` 而非 `Err`。
    pub async fn health_check(&self) -> TaosResult<TaosHealth> {
        let stats = self.stats();
        let precision = self.precision();
        if stats.closed {
            self.inner.metrics.inc_health_not_ready();
            return Ok(TaosHealth {
                ready: false,
                precision,
                server_version: None,
                stats,
                metrics: self.metrics(),
                detail: "池已关闭".to_owned(),
            });
        }
        match self.exec("SELECT SERVER_VERSION()").await {
            Ok(result) if result.code == 0 => {
                self.inner.metrics.inc_ping_ok();
                self.inner.metrics.inc_health_ready();
                Ok(TaosHealth {
                    ready: true,
                    precision,
                    server_version: result.rows.first().and_then(|row| row.first()).cloned(),
                    stats: self.stats(),
                    metrics: self.metrics(),
                    detail: "就绪".to_owned(),
                })
            }
            Ok(result) => {
                self.inner.metrics.inc_ping_err();
                self.inner.metrics.inc_health_not_ready();
                Ok(TaosHealth {
                    ready: false,
                    precision,
                    server_version: None,
                    stats: self.stats(),
                    metrics: self.metrics(),
                    detail: format!("ping code={}", result.code),
                })
            }
            Err(error) => {
                self.inner.metrics.inc_ping_err();
                self.inner.metrics.inc_health_not_ready();
                Ok(TaosHealth {
                    ready: false,
                    precision,
                    server_version: None,
                    stats: self.stats(),
                    metrics: self.metrics(),
                    detail: format!("不可用: {error}"),
                })
            }
        }
    }

    /// 轻量健康检查：`SELECT SERVER_VERSION()` 成功且 `code == 0` 则返回 `Ok(())`。
    pub async fn ping(&self) -> TaosResult<()> {
        match self.exec("SELECT SERVER_VERSION()").await {
            Ok(result) if result.code == 0 => {
                self.inner.metrics.inc_ping_ok();
                Ok(())
            }
            Ok(result) => {
                self.inner.metrics.inc_ping_err();
                Err(TaosError::Unavailable(format!("ping code={}", result.code)))
            }
            Err(error) => {
                self.inner.metrics.inc_ping_err();
                Err(error)
            }
        }
    }

    /// 在配置 database 上下文执行 SQL。
    pub async fn exec(&self, sql: &str) -> TaosResult<TaosExecResult> {
        self.exec_sql_raw(sql, true).await
    }

    /// 执行只读查询 SQL（语义同 [`TaosPool::exec`]，便于调用方表达意图）。
    pub async fn query(&self, sql: &str) -> TaosResult<TaosExecResult> {
        self.exec(sql).await
    }

    /// 通过 Native WS 执行一条 SQL（短会话：握手 → 发送 → 关闭）。
    pub async fn exec_sql_ws(&self, sql: &str) -> TaosResult<String> {
        native::exec_sql_ws(self.config(), sql).await
    }

    /// 写入序列前确保超级表存在（`ts TIMESTAMP, bid NCHAR(64), ask NCHAR(64)`）。
    pub async fn ensure_stable(&self, table: &str) -> TaosResult<()> {
        validate_stable_ident(table)?;
        let sql = format!(
            "CREATE STABLE IF NOT EXISTS `{table}` (\
               ts TIMESTAMP, bid NCHAR(64), ask NCHAR(64)\
             ) TAGS (symbol NCHAR(128))"
        );
        let result = self.exec(&sql).await?;
        if result.code != 0 {
            return Err(TaosError::from_taos_code(result.code, "ensure_stable 失败"));
        }
        self.verify_decimal_schema(table).await
    }

    /// 关闭池：标记关闭、关闭信号量并等待在途请求排空（受 `close_timeout` 约束）。
    ///
    /// 可重复调用；已关闭时再次调用仍会等待排空。
    pub async fn close(&self) -> TaosResult<()> {
        self.inner.state.fetch_or(CLOSED_BIT, Ordering::AcqRel);
        self.inner.sem.close();
        let drain = async {
            loop {
                let notified = self.inner.drained.notified();
                let mut notified = std::pin::pin!(notified);
                notified.as_mut().enable();
                if self.inner.state.load(Ordering::Acquire) & IN_FLIGHT_MASK == 0 {
                    return;
                }
                notified.await;
            }
        };
        timeout(self.inner.config.close_timeout, drain)
            .await
            .map_err(|_| TaosError::Timeout("close 等待在途请求排空超时".to_owned()))?;
        Ok(())
    }

    /// 校验 `bid` / `ask` 必须为 `NCHAR(64+)`，拒绝 DOUBLE 精度降级。
    async fn verify_decimal_schema(&self, table: &str) -> TaosResult<()> {
        validate_stable_ident(table)?;
        let result = self.exec(&format!("DESCRIBE `{table}`")).await?;
        validate_decimal_schema(&result)
    }

    /// 从 `information_schema.ins_databases` 探测数据库精度。
    async fn detect_precision(&self) -> TaosResult<TsPrecision> {
        let database = self.inner.config.database.clone();
        validate_ident(&database)?;
        let sql = format!(
            "SELECT `precision` FROM information_schema.ins_databases WHERE name='{database}'"
        );
        let result = self.exec_sql_raw(&sql, false).await?;
        result
            .rows
            .first()
            .and_then(|row| row.first())
            .and_then(|value| TsPrecision::parse(value))
            .ok_or_else(|| {
                TaosError::Unavailable("无法从 information_schema 探测数据库精度".to_owned())
            })
    }

    /// 获取 in-flight 许可（含 acquire 超时与关闭检查）。
    async fn acquire(&self) -> TaosResult<RequestGuard> {
        self.ensure_open()?;
        let acquired = timeout(
            self.inner.config.acquire_timeout,
            self.inner.sem.clone().acquire_owned(),
        )
        .await;
        match acquired {
            Ok(Ok(permit)) => loop {
                let state = self.inner.state.load(Ordering::Acquire);
                if state & CLOSED_BIT != 0 {
                    drop(permit);
                    return Err(TaosError::Closed("pool 已关闭".to_owned()));
                }
                let next = state
                    .checked_add(1)
                    .ok_or_else(|| TaosError::Config("in-flight 计数溢出".to_owned()))?;
                if self
                    .inner
                    .state
                    .compare_exchange_weak(state, next, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return Ok(RequestGuard {
                        _permit: permit,
                        inner: Arc::clone(&self.inner),
                    });
                }
            },
            Ok(Err(_)) => Err(TaosError::Closed("背压信号量已关闭".to_owned())),
            Err(_) => Err(TaosError::Timeout(format!(
                "获取 in-flight 许可超时（max={}）",
                self.inner.config.max_in_flight
            ))),
        }
    }

    /// 池级 SQL 入口：字节上限 + 背压 + 计数。
    async fn exec_sql_raw(&self, sql: &str, use_database: bool) -> TaosResult<TaosExecResult> {
        if sql.len() > self.inner.config.batch_max_bytes {
            self.inner.metrics.inc_sql_err();
            return Err(TaosError::Invalid(format!(
                "SQL 请求超过 batch_max_bytes={} 字节",
                self.inner.config.batch_max_bytes
            )));
        }
        self.inner.metrics.add_sql_bytes(sql.len());
        let _guard = self.acquire().await?;
        match self.exec_sql_raw_inner(sql, use_database).await {
            Ok(result) => {
                self.inner.metrics.inc_sql_ok();
                Ok(result)
            }
            Err(error) => {
                self.inner.metrics.inc_sql_err();
                Err(error)
            }
        }
    }

    /// 单次 REST 请求（已持有 in-flight 许可）。
    async fn exec_sql_raw_inner(
        &self,
        sql: &str,
        use_database: bool,
    ) -> TaosResult<TaosExecResult> {
        let config = &self.inner.config;
        let url = if use_database {
            config.rest_sql_db_url()
        } else {
            config.rest_sql_url()
        };

        debug!(target: "taosx", database = %config.database, "taos rest sql");

        let response = self
            .inner
            .http
            .post(&url)
            .basic_auth(&config.user, Some(&config.password))
            .header(reqwest::header::CONTENT_TYPE, "text/plain; charset=utf-8")
            .body(sql.to_owned())
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() {
                    TaosError::Timeout(format!("请求超时: {error}"))
                } else {
                    TaosError::Connection(format!("请求失败: {error}"))
                }
            })?;

        let status = response.status();
        let text = read_response_limited(response, config.max_response_bytes).await?;
        self.inner.metrics.add_response_bytes(text.len());

        if !status.is_success() {
            // 响应正文可能夹带凭据或 SQL 片段，一律不入错误消息（见 src/error.rs 的约定）。
            return Err(TaosError::from_http_status(
                status.as_u16(),
                "响应正文已省略",
            ));
        }

        let result = parse_taos_json(&text)?;
        if result.rows.len() > config.max_query_rows {
            return Err(TaosError::Unavailable(format!(
                "SQL 结果超过 max_query_rows={}",
                config.max_query_rows
            )));
        }
        Ok(result)
    }

    /// 关闭检查。
    fn ensure_open(&self) -> TaosResult<()> {
        if self.is_closed() {
            return Err(TaosError::Closed("pool 已关闭".to_owned()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::response::{parse_ts_cell, truncate};
    use super::sql::{
        build_insert_sql_chunks_with_limits, encode_timestamp, subtable_name, MAX_SYMBOL_BYTES,
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
        assert!(subtable_name("ticks", &"X".repeat(MAX_SYMBOL_BYTES + 1)).is_err());
        assert!(validate_ident("a b").is_err());
        assert!(validate_ident("1abc").is_err());
        assert!(validate_ident("").is_err());
        assert!(validate_ident("ok_name1").is_ok());
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
        assert!(build_insert_sql_chunks("ticks", &points, TsPrecision::Ms, 0).is_err());
        assert!(build_insert_sql_chunks(
            "ticks",
            &points,
            TsPrecision::Ms,
            HARD_MAX_BATCH_ROWS + 1
        )
        .is_err());
        assert!(build_insert_sql_chunks("bad name", &points, TsPrecision::Ms, 1).is_err());
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
        assert!(build_insert_sql_chunks("ticks", &[point], TsPrecision::Ns, 1).is_ok());
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
        assert!(
            encode_timestamp(1_500, TsPrecision::Us).is_err(),
            "未对齐必须拒绝"
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
        assert!(validate_decimal_schema(&double).is_err());

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
        assert!(parse_ts_cell("not-a-time", TsPrecision::Ms).is_err());
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
        assert!(TaosPool::new(TaosConfig {
            max_in_flight: 0,
            ..TaosConfig::default()
        })
        .is_err());
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
        assert!(pool.exec("SELECT value").await.is_err());

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
        assert!(
            pool.query_series("ticks", 2, 1).await.is_err(),
            "start > end 必须拒绝"
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
}
