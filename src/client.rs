//! TDengine REST 生产客户端（默认 6041）：池背压、批量写入、健康检查。
//!
//! - 传输：`reqwest`，端点 `POST http(s)://host:port/rest/sql[/database]`，Basic 认证。
//! - 并发：`max_in_flight` 信号量 + in-flight 计数 + 关闭排空。
//! - SQL 安全：标识符白名单校验、tag 值十六进制子表编码、字面量转义。

use std::fmt::Write as _;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use chrono::{DateTime, Utc};
use serde::Deserialize;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;
use tracing::debug;

use crate::config::{
    TaosConfig, TransportMode, TsPrecision, HARD_MAX_BATCH_BYTES, HARD_MAX_BATCH_ROWS,
};
use crate::error::{TaosError, TaosResult};
use crate::metrics::{OpCounters, TaosMetricsSnapshot};
use crate::native;
use crate::point::TaosPoint;

/// 关闭标记位（`state` 最高位）。
const CLOSED_BIT: usize = 1usize << (usize::BITS - 1);
/// in-flight 计数掩码（`state` 低位）。
const IN_FLIGHT_MASK: usize = !CLOSED_BIT;
/// 多子表 INSERT 前缀。
const INSERT_PREFIX: &str = "INSERT INTO ";
/// 超级表名最大 UTF-8 字节数。
const MAX_STABLE_NAME_BYTES: usize = 94;
/// tag 值最大 UTF-8 字节数（十六进制编码后进入子表名）。
const MAX_SYMBOL_BYTES: usize = 48;

/// REST 执行结果（精简）。
#[derive(Debug, Clone)]
pub struct TaosExecResult {
    /// 驱动 code（0 = 成功）。
    pub code: i32,
    /// 行数据（字符串化单元格）。
    pub rows: Vec<Vec<String>>,
    /// 列名（若响应携带 `column_meta`）。
    pub columns: Vec<String>,
    /// 受影响行数（写路径可能有）。
    pub affected_rows: Option<i64>,
}

/// 池运行时快照。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaosPoolStats {
    /// 正在执行的请求数。
    pub in_flight: usize,
    /// 是否已关闭。
    pub closed: bool,
}

/// 健康检查结果（有 deadline 的轻量 SQL + 本地状态）。
///
/// - `ready == true`：池未关闭且 `SELECT SERVER_VERSION()` 成功，并带回生效精度。
/// - `ready == false`：本地已关闭或远端不可达；**不**用 `Err` 表示「未就绪」，
///   便于编排探针区分「依赖暂不可用」与「探针实现错误」。
/// - 配置精度与探测精度冲突只在 `connect` 时 fail-closed；本探针只报告当前生效精度。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaosHealth {
    /// 是否可服务写查。
    pub ready: bool,
    /// 当前生效时间精度。
    pub precision: TsPrecision,
    /// 服务端版本字符串（若可得）。
    pub server_version: Option<String>,
    /// 池瞬时统计。
    pub stats: TaosPoolStats,
    /// 操作计数快照。
    pub metrics: TaosMetricsSnapshot,
    /// 中文简短说明（不含凭据）。
    pub detail: String,
}

impl TaosHealth {
    /// 编排探针用：仅看 `ready`。
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.ready
    }
}

/// 批量写入结果报告（行数与 chunk 计数）。
///
/// - 全部成功：`failed == 0` 且 `accepted == 请求行数`。
/// - 中途失败：通过 [`BatchWritePartialError`] 带回**已成功提交**的 `accepted`
///   与未提交的 `failed`；**不**自动重试（非幂等写重试为 NO-GO）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BatchWriteReport {
    /// 已成功提交的行数。
    pub accepted: usize,
    /// 未提交行数（含失败 chunk 及其后未尝试行）。
    pub failed: usize,
    /// 成功 chunk 数。
    pub chunks_ok: usize,
    /// 计划 chunk 总数。
    pub chunks_total: usize,
}

impl BatchWriteReport {
    /// 是否整批完成且无失败行。
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.failed == 0 && self.chunks_ok == self.chunks_total
    }
}

/// 批量写入部分成功错误：结构化报告与根因一并返回。
#[derive(Debug)]
pub struct BatchWritePartialError {
    /// 失败瞬间的可定位报告。
    pub report: BatchWriteReport,
    /// 驱动/传输错误。
    pub source: TaosError,
}

impl std::fmt::Display for BatchWritePartialError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "write_batch 部分成功 accepted={} failed={} chunks_ok={}/{}: {}",
            self.report.accepted,
            self.report.failed,
            self.report.chunks_ok,
            self.report.chunks_total,
            self.source
        )
    }
}

impl std::error::Error for BatchWritePartialError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

impl From<BatchWritePartialError> for TaosError {
    fn from(value: BatchWritePartialError) -> Self {
        let message = value.to_string();
        value.source.with_message(message)
    }
}

/// TDengine REST 响应体（仅取所需字段）。
#[derive(Debug, Deserialize)]
struct RawResponse {
    code: i32,
    #[serde(default)]
    desc: Option<String>,
    #[serde(default)]
    column_meta: Vec<serde_json::Value>,
    #[serde(default)]
    data: Vec<Vec<serde_json::Value>>,
    #[serde(default)]
    rows: Option<i64>,
}

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

/// 构建分块 INSERT SQL（纯函数；由调用方驱动 chunk 尺寸）。
///
/// 每个 chunk 生成一条多子表 `INSERT INTO ... USING ... TAGS (...) VALUES (...)`：
///
/// - `table` 必须是合法标识符（字母/下划线开头、≤94 字节），否则返回
///   [`TaosError::Invalid`]；
/// - 子表名由 `table` + tag 值的十六进制编码构成，tag 值不直接进入标识符；
/// - 字符串字面量按 TDengine 规则转义（`\` → `\\`、`'` → `\'`）；
/// - 时间戳按 `precision` 换算，**未对齐目标精度时 fail-closed**，不静默截断。
///
/// `max_rows` 必须在 `1..=HARD_MAX_BATCH_ROWS` 且单行不得超过 [`HARD_MAX_BATCH_BYTES`]。
pub fn build_insert_sql_chunks(
    table: &str,
    points: &[TaosPoint],
    precision: TsPrecision,
    max_rows: usize,
) -> TaosResult<Vec<String>> {
    Ok(build_insert_sql_chunks_with_limits(
        table,
        points,
        precision,
        max_rows,
        HARD_MAX_BATCH_BYTES,
    )?
    .into_iter()
    .map(|(sql, _rows)| sql)
    .collect())
}

/// 单个 SQL chunk 及其行数。
type SqlChunk = (String, usize);

/// 带字节上限的分块构造；返回 `(sql, 行数)` 以支持精确的部分成功报告。
fn build_insert_sql_chunks_with_limits(
    table: &str,
    points: &[TaosPoint],
    precision: TsPrecision,
    max_rows: usize,
    max_bytes: usize,
) -> TaosResult<Vec<SqlChunk>> {
    validate_stable_ident(table)?;
    if max_rows == 0 || max_rows > HARD_MAX_BATCH_ROWS {
        return Err(TaosError::Invalid(format!(
            "max_rows 必须为 1..={HARD_MAX_BATCH_ROWS}"
        )));
    }
    if max_bytes < INSERT_PREFIX.len() || max_bytes > HARD_MAX_BATCH_BYTES {
        return Err(TaosError::Invalid(format!(
            "max_bytes 必须为 {}..={HARD_MAX_BATCH_BYTES}",
            INSERT_PREFIX.len()
        )));
    }
    if points.is_empty() {
        return Ok(Vec::new());
    }

    let mut chunks = Vec::new();
    let mut sql = String::from(INSERT_PREFIX);
    let mut rows = 0usize;
    for point in points {
        let subtable = subtable_name(table, &point.tag_value)?;
        let tag = escape_str(&point.tag_value);
        let timestamp = encode_timestamp(point.timestamp_ns, precision)?;
        let first = escape_str(&point.values[0]);
        let second = escape_str(&point.values[1]);
        let row = format!(
            "`{subtable}` USING `{table}` TAGS ('{tag}') VALUES ({timestamp},'{first}','{second}')"
        );
        let separator = usize::from(rows > 0);
        let next_len = sql
            .len()
            .checked_add(separator)
            .and_then(|length| length.checked_add(row.len()))
            .ok_or_else(|| TaosError::Invalid("批量 SQL 字节数溢出".to_owned()))?;
        if rows > 0 && (rows >= max_rows || next_len > max_bytes) {
            chunks.push((sql, rows));
            sql = String::from(INSERT_PREFIX);
            rows = 0;
        }
        let row_len = INSERT_PREFIX
            .len()
            .checked_add(row.len())
            .ok_or_else(|| TaosError::Invalid("单行 SQL 字节数溢出".to_owned()))?;
        if row_len > max_bytes {
            return Err(TaosError::Invalid(format!(
                "单行 SQL 超过 batch_max_bytes={max_bytes}"
            )));
        }
        if rows > 0 {
            sql.push(' ');
        }
        sql.push_str(&row);
        rows += 1;
    }
    if rows > 0 {
        chunks.push((sql, rows));
    }
    Ok(chunks)
}

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

    /// 显式批量写入：按配置 `batch_max_rows` 分块 INSERT。
    ///
    /// 空 `points` → `Ok(())`。任一片失败 → `Err`（可能已有部分行提交）。
    pub async fn write_batch(&self, table: &str, points: &[TaosPoint]) -> TaosResult<()> {
        self.write_batch_report(table, points).await.map(|_| ())
    }

    /// 批量写入并返回 [`BatchWriteReport`]。
    ///
    /// 中途失败时错误由 [`BatchWritePartialError`] 映射为 [`TaosError`]，
    /// 文案含 `accepted` / `failed`。
    pub async fn write_batch_report(
        &self,
        table: &str,
        points: &[TaosPoint],
    ) -> TaosResult<BatchWriteReport> {
        let max_rows = self.inner.config.batch_max_rows;
        self.write_batch_chunked_report(table, points, max_rows)
            .await
    }

    /// 幂等友好写：按配置 `write_max_attempts` 对可重试错误整批重试。
    ///
    /// 调用方须保证 `points` 时间戳/标签唯一，避免重复副作用。
    pub async fn write_batch_idempotent(
        &self,
        table: &str,
        points: &[TaosPoint],
    ) -> TaosResult<BatchWriteReport> {
        let policy = crate::retry::RetryPolicy {
            max_attempts: self.inner.config.write_max_attempts.max(1),
            ..crate::retry::RetryPolicy::for_idempotent_write()
        };
        policy
            .run(|| async { self.write_batch_report(table, points).await })
            .await
    }

    /// 带自定义 chunk 行数的批量写入。
    pub async fn write_batch_chunked(
        &self,
        table: &str,
        points: &[TaosPoint],
        max_rows: usize,
    ) -> TaosResult<()> {
        self.write_batch_chunked_report(table, points, max_rows)
            .await
            .map(|_| ())
    }

    /// 带自定义 chunk 行数的批量写入，返回结构化报告。
    ///
    /// 空 `points` → `accepted=0` 的完整报告。不自动重试已提交 chunk。
    pub async fn write_batch_chunked_report(
        &self,
        table: &str,
        points: &[TaosPoint],
        max_rows: usize,
    ) -> TaosResult<BatchWriteReport> {
        self.write_batch_chunked_outcome(table, points, max_rows)
            .await
            .map_err(Into::into)
    }

    /// 与 [`TaosPool::write_batch_chunked_report`] 相同，但部分成功时返回结构化
    /// [`BatchWritePartialError`]（含准确 `accepted` / `failed` / `chunks_*`）。
    pub async fn write_batch_chunked_outcome(
        &self,
        table: &str,
        points: &[TaosPoint],
        max_rows: usize,
    ) -> Result<BatchWriteReport, BatchWritePartialError> {
        let failed_all = |chunks_total: usize, source: TaosError| BatchWritePartialError {
            report: BatchWriteReport {
                accepted: 0,
                failed: points.len(),
                chunks_ok: 0,
                chunks_total,
            },
            source,
        };

        if let Err(source) = validate_stable_ident(table) {
            return Err(failed_all(0, source));
        }
        if points.is_empty() {
            self.inner.metrics.inc_write_ok();
            return Ok(BatchWriteReport::default());
        }
        if max_rows == 0 || max_rows > self.inner.config.batch_max_rows {
            return Err(failed_all(
                0,
                TaosError::Invalid(format!(
                    "max_rows 必须为 1..={}（配置上限）",
                    self.inner.config.batch_max_rows
                )),
            ));
        }
        if let Err(source) = self.ensure_stable(table).await {
            return Err(failed_all(0, source));
        }
        let chunks = match build_insert_sql_chunks_with_limits(
            table,
            points,
            self.precision(),
            max_rows,
            self.inner.config.batch_max_bytes,
        ) {
            Ok(chunks) => chunks,
            Err(source) => return Err(failed_all(0, source)),
        };

        let chunks_total = chunks.len();
        let mut accepted = 0usize;
        let mut chunks_ok = 0usize;
        for (sql, row_count) in chunks {
            match self.exec(&sql).await {
                Ok(result) if result.code == 0 => {
                    accepted += row_count;
                    chunks_ok += 1;
                }
                Ok(result) => {
                    self.inner.metrics.inc_write_err();
                    return Err(BatchWritePartialError {
                        report: BatchWriteReport {
                            accepted,
                            failed: points.len().saturating_sub(accepted),
                            chunks_ok,
                            chunks_total,
                        },
                        source: TaosError::from_taos_code(result.code, "write_batch 失败"),
                    });
                }
                Err(source) => {
                    self.inner.metrics.inc_write_err();
                    return Err(BatchWritePartialError {
                        report: BatchWriteReport {
                            accepted,
                            failed: points.len().saturating_sub(accepted),
                            chunks_ok,
                            chunks_total,
                        },
                        source,
                    });
                }
            }
        }
        self.inner.metrics.inc_write_ok();
        Ok(BatchWriteReport {
            accepted,
            failed: 0,
            chunks_ok,
            chunks_total,
        })
    }

    /// 写入一组技术点；委托显式批量 API。
    pub async fn write_series(&self, table: &str, points: &[TaosPoint]) -> TaosResult<()> {
        self.write_batch(table, points).await
    }

    /// 按纳秒闭区间查询技术行（`ts >= start AND ts <= end`）。
    ///
    /// 表不存在时返回空集（依赖 [`TaosError::is_not_found`] 的类型化判定，
    /// 而非对错误文案做字符串匹配）。
    pub async fn query_series(
        &self,
        table: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> TaosResult<Vec<TaosPoint>> {
        let result = self.query_series_inner(table, start_ns, end_ns).await;
        match &result {
            Ok(_) => self.inner.metrics.inc_query_ok(),
            Err(_) => self.inner.metrics.inc_query_err(),
        }
        result
    }

    /// `query_series` 的实现体。
    async fn query_series_inner(
        &self,
        table: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> TaosResult<Vec<TaosPoint>> {
        validate_stable_ident(table)?;
        if start_ns > end_ns {
            return Err(TaosError::Invalid("query_series: start > end".to_owned()));
        }
        if let Err(error) = self.verify_decimal_schema(table).await {
            if error.is_not_found() {
                return Ok(Vec::new());
            }
            return Err(error);
        }
        let precision = self.precision();
        let limit = self
            .inner
            .config
            .max_query_rows
            .checked_add(1)
            .ok_or_else(|| TaosError::Config("max_query_rows 溢出".to_owned()))?;
        let sql = format!(
            "SELECT ts, bid, ask, symbol FROM `{table}` WHERE ts >= {} AND ts <= {} \
             ORDER BY ts ASC LIMIT {limit}",
            precision.from_nanos(start_ns),
            precision.from_nanos(end_ns)
        );
        let result = match self.exec(&sql).await {
            Ok(result) => result,
            Err(error) if error.is_not_found() => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        if result.rows.len() > self.inner.config.max_query_rows {
            return Err(TaosError::Unavailable(format!(
                "查询结果超过 max_query_rows={}",
                self.inner.config.max_query_rows
            )));
        }

        let mut points = Vec::with_capacity(result.rows.len());
        for row in result.rows {
            if row.len() < 4 {
                continue;
            }
            let timestamp_ns = parse_ts_cell(&row[0], precision)?;
            points.push(TaosPoint::new(
                row[3].clone(),
                timestamp_ns,
                row[1].clone(),
                row[2].clone(),
            ));
        }
        Ok(points)
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
            return Err(TaosError::from_http_status(
                status.as_u16(),
                &truncate(&text, 256),
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

/// 解析 TDengine REST JSON 响应。
fn parse_taos_json(text: &str) -> TaosResult<TaosExecResult> {
    let raw: RawResponse = serde_json::from_str(text).map_err(|error| {
        TaosError::Serialization(format!(
            "TDengine JSON 解析失败（{error}）; body={}",
            truncate(text, 256)
        ))
    })?;

    if raw.code != 0 {
        return Err(TaosError::from_taos_code(
            raw.code,
            &raw.desc.unwrap_or_default(),
        ));
    }

    let columns = raw
        .column_meta
        .iter()
        .filter_map(|column| {
            column
                .as_array()
                .and_then(|entries| entries.first())
                .and_then(|value| value.as_str())
                .map(str::to_owned)
        })
        .collect::<Vec<_>>();

    let mut rows: Vec<Vec<String>> = Vec::with_capacity(raw.data.len());
    for row in raw.data {
        rows.push(row.iter().map(json_cell_to_string).collect());
    }

    let affected_rows = if columns.first().map(String::as_str) == Some("affected_rows") {
        rows.first()
            .and_then(|row| row.first())
            .and_then(|cell| cell.parse().ok())
    } else {
        raw.rows
    };

    Ok(TaosExecResult {
        code: raw.code,
        rows,
        columns,
        affected_rows,
    })
}

/// 读取响应体并强制 `max_bytes` 上限（同时约束 `Content-Length` 与分块流）。
async fn read_response_limited(
    mut response: reqwest::Response,
    max_bytes: usize,
) -> TaosResult<String> {
    let limit = u64::try_from(max_bytes).unwrap_or(u64::MAX);
    if response
        .content_length()
        .is_some_and(|length| length > limit)
    {
        return Err(TaosError::Unavailable(format!(
            "响应超过 max_response_bytes={max_bytes}"
        )));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| TaosError::Connection(format!("读响应失败: {error}")))?
    {
        let next_len = body
            .len()
            .checked_add(chunk.len())
            .ok_or_else(|| TaosError::Unavailable("响应字节数溢出".to_owned()))?;
        if next_len > max_bytes {
            return Err(TaosError::Unavailable(format!(
                "响应超过 max_response_bytes={max_bytes}"
            )));
        }
        body.extend_from_slice(&chunk);
    }
    String::from_utf8(body)
        .map_err(|error| TaosError::Serialization(format!("响应不是 UTF-8（{error}）")))
}

/// 校验 `DESCRIBE` 结果：`bid` / `ask` 必须为 `NCHAR(64+)`。
fn validate_decimal_schema(result: &TaosExecResult) -> TaosResult<()> {
    let mut bid_ok = false;
    let mut ask_ok = false;
    for row in &result.rows {
        if row.len() < 3 {
            continue;
        }
        let field = row[0].trim();
        let data_type = row[1].trim();
        let length_ok = row[2]
            .trim()
            .parse::<usize>()
            .is_ok_and(|length| length >= 64);
        let exact_text = data_type.eq_ignore_ascii_case("NCHAR") && length_ok;
        if field.eq_ignore_ascii_case("bid") {
            bid_ok = exact_text;
        } else if field.eq_ignore_ascii_case("ask") {
            ask_ok = exact_text;
        }
    }
    if !bid_ok || !ask_ok {
        return Err(TaosError::backend(
            "TDengine schema 不兼容：bid/ask 必须为 NCHAR(64+)；拒绝 DOUBLE 精度降级",
        ));
    }
    Ok(())
}

/// 子表名：`{stable}_s{tag 值十六进制}`（tag 值不直接进入标识符）。
fn subtable_name(stable: &str, tag_value: &str) -> TaosResult<String> {
    validate_stable_ident(stable)?;
    if tag_value.len() > MAX_SYMBOL_BYTES {
        return Err(TaosError::Invalid(format!(
            "tag 值超过 {MAX_SYMBOL_BYTES} UTF-8 字节"
        )));
    }
    let mut encoded = String::with_capacity(tag_value.len().saturating_mul(2));
    for byte in tag_value.as_bytes() {
        write!(&mut encoded, "{byte:02x}")
            .map_err(|_| TaosError::Invalid("tag 子表编码失败".to_owned()))?;
    }
    let name = format!("{stable}_s{encoded}");
    validate_ident(&name)?;
    Ok(name)
}

/// 超级表名校验（合法标识符且 ≤ [`MAX_STABLE_NAME_BYTES`] 字节）。
fn validate_stable_ident(name: &str) -> TaosResult<()> {
    validate_ident(name)?;
    if name.len() > MAX_STABLE_NAME_BYTES {
        return Err(TaosError::Invalid(format!(
            "stable 名称超过 {MAX_STABLE_NAME_BYTES} 字节"
        )));
    }
    Ok(())
}

/// SQL 标识符白名单校验（防注入）。
fn validate_ident(name: &str) -> TaosResult<()> {
    if name.is_empty() || name.len() > 192 {
        return Err(TaosError::Invalid("非法标识符长度".to_owned()));
    }
    let mut characters = name.chars();
    let Some(first) = characters.next() else {
        return Err(TaosError::Invalid("空标识符".to_owned()));
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(TaosError::Invalid("标识符须以字母或下划线开头".to_owned()));
    }
    if !characters.all(|character| character.is_ascii_alphanumeric() || character == '_') {
        return Err(TaosError::Invalid("标识符含非法字符".to_owned()));
    }
    Ok(())
}

/// SQL 字符串字面量转义（`\` → `\\`，`'` → `\'`）。
fn escape_str(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "\\'")
}

/// 纳秒时间戳 → 目标精度数值；不允许静默精度损失。
fn encode_timestamp(timestamp_ns: i64, precision: TsPrecision) -> TaosResult<i64> {
    let stored = precision.from_nanos(timestamp_ns);
    if precision.to_nanos(stored) != timestamp_ns {
        return Err(TaosError::Invalid(format!(
            "时间戳 {timestamp_ns} ns 无法无损表示为 {} 精度（请对齐精度或改用 ns 库）",
            precision.as_str()
        )));
    }
    Ok(stored)
}

/// JSON 单元格 → 字符串（保持调用方原始文本表示）。
fn json_cell_to_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => String::new(),
        serde_json::Value::String(text) => text.clone(),
        serde_json::Value::Bool(flag) => flag.to_string(),
        serde_json::Value::Number(number) => number.to_string(),
        other => other.to_string(),
    }
}

/// 解析时间戳单元格（库数值或 RFC3339 文本）。
fn parse_ts_cell(raw: &str, precision: TsPrecision) -> TaosResult<i64> {
    if let Ok(value) = raw.parse::<i64>() {
        return Ok(precision.to_nanos(value));
    }
    if let Ok(parsed) = DateTime::parse_from_rfc3339(raw) {
        return Ok(parsed
            .timestamp_nanos_opt()
            .unwrap_or_else(|| parsed.timestamp().saturating_mul(1_000_000_000)));
    }
    if let Ok(parsed) = DateTime::<Utc>::from_str(raw) {
        return Ok(parsed
            .timestamp_nanos_opt()
            .unwrap_or_else(|| parsed.timestamp().saturating_mul(1_000_000_000)));
    }
    Err(TaosError::Invalid(format!("无法解析时间戳: {raw}")))
}

/// 截断文本用于错误消息（UTF-8 边界安全、单行化）。
fn truncate(text: &str, max: usize) -> String {
    let mut trimmed = text.trim().replace('\n', " ");
    if trimmed.len() > max {
        let mut boundary = max;
        while !trimmed.is_char_boundary(boundary) {
            boundary -= 1;
        }
        trimmed.truncate(boundary);
        trimmed.push('…');
    }
    trimmed
}

/// 供流式 API 使用：分块提示必须 ≥ 1。
pub(crate) fn validate_chunk_hint(chunk_hint: usize) -> TaosResult<()> {
    if chunk_hint == 0 {
        return Err(TaosError::Invalid("chunk_hint 必须 ≥ 1".to_owned()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
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
