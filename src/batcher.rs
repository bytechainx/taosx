//! 有界异步写批处理器：`push` → 按行/字节/时间窗口刷写 → 显式 `flush` / `close`。
//!
//! ## 失败恢复（调用方负责）
//!
//! - **不**自动重试；部分成功时保留未确认 suffix 进入 pending。
//! - [`WriteBatcher::take_pending`] / [`WriteBatcher::ack_pending`] 转移或确认后方能
//!   继续 `push` / `flush` / `close`。
//! - **非** exactly-once；禁止整批重放掩盖部分成功。
//!
//! `drop` **不**保证远端写入；关闭必须 `close().await`。

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

use crate::client::{BatchWriteReport, TaosPool};
use crate::error::{TaosError, TaosResult};
use crate::point::TaosPoint;

/// 批处理器配置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteBatcherConfig {
    /// 触发刷写的缓冲行数上限。
    pub max_rows: usize,
    /// 缓冲字节数软上限（提示；实际 SQL 字节上限仍由池的 `batch_max_bytes` 决定）。
    pub max_bytes_hint: usize,
    /// 时间窗口：距上次刷写超过该时长后，下一次 `push` 触发刷写。
    pub flush_interval: Duration,
}

impl Default for WriteBatcherConfig {
    fn default() -> Self {
        Self {
            max_rows: 500,
            max_bytes_hint: 256 * 1024,
            flush_interval: Duration::from_millis(200),
        }
    }
}

/// `close` 失败时的结构化摘要（accepted / failed / pending）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BatcherCloseReport {
    /// 会话累计已成功提交行数。
    pub total_accepted: usize,
    /// 会话累计未成功提交行数（含仍持有的 pending）。
    pub total_failed: usize,
    /// 当前仍由 batcher 持有的未确认行数。
    pub pending: usize,
    /// 触发关闭的末次 flush 报告（无刷写则为默认值）。
    pub last_flush: BatchWriteReport,
}

/// `close` 失败：携带摘要与根因；**不**标记 closed。
#[derive(Debug)]
pub struct BatcherCloseError {
    /// 失败瞬间的可定位摘要。
    pub summary: BatcherCloseReport,
    /// 驱动/批写错误。
    pub source: TaosError,
}

impl std::fmt::Display for BatcherCloseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WriteBatcher close 失败 total_accepted={} total_failed={} pending={}: {}",
            self.summary.total_accepted,
            self.summary.total_failed,
            self.summary.pending,
            self.source
        )
    }
}

impl std::error::Error for BatcherCloseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// 未确认点的暂存区。
struct FailedPending {
    points: Vec<TaosPoint>,
}

/// 批处理器可变状态。
struct Inner {
    table: String,
    buffer: Vec<TaosPoint>,
    closed: bool,
    failed_pending: Option<FailedPending>,
    last_flush: Instant,
    config: WriteBatcherConfig,
    total_accepted: usize,
    total_failed: usize,
}

/// 异步写批处理器。
pub struct WriteBatcher {
    pool: TaosPool,
    inner: Arc<Mutex<Inner>>,
}

impl WriteBatcher {
    /// 绑定池与目标超级表。
    #[must_use]
    pub fn new(pool: TaosPool, table: impl Into<String>, config: WriteBatcherConfig) -> Self {
        Self {
            pool,
            inner: Arc::new(Mutex::new(Inner {
                table: table.into(),
                buffer: Vec::with_capacity(config.max_rows.min(1024)),
                closed: false,
                failed_pending: None,
                last_flush: Instant::now(),
                config,
                total_accepted: 0,
                total_failed: 0,
            })),
        }
    }

    /// 推入点；达到行数上限或超出时间窗口时自动刷写。
    pub async fn push(&self, point: TaosPoint) -> TaosResult<()> {
        let mut guard = self.inner.lock().await;
        if guard.closed {
            return Err(TaosError::Closed("WriteBatcher 已关闭".to_owned()));
        }
        if guard.failed_pending.is_some() {
            return Err(pending_gate_error());
        }
        guard.buffer.push(point);
        let should_flush = guard.buffer.len() >= guard.config.max_rows
            || guard.last_flush.elapsed() >= guard.config.flush_interval;
        if should_flush {
            let table = guard.table.clone();
            let batch = std::mem::take(&mut guard.buffer);
            drop(guard);
            self.flush_batch(&table, batch).await?;
            let mut guard = self.inner.lock().await;
            guard.last_flush = Instant::now();
        }
        Ok(())
    }

    /// 刷空缓冲。
    pub async fn flush(&self) -> TaosResult<BatchWriteReport> {
        let mut guard = self.inner.lock().await;
        if guard.closed {
            return Err(TaosError::Closed("WriteBatcher 已关闭".to_owned()));
        }
        if guard.failed_pending.is_some() {
            return Err(pending_gate_error());
        }
        let table = guard.table.clone();
        let batch = std::mem::take(&mut guard.buffer);
        guard.last_flush = Instant::now();
        drop(guard);
        self.flush_batch(&table, batch).await
    }

    /// 刷写并关闭；成功返回末次 flush 报告，失败返回 [`BatcherCloseError`]。
    ///
    /// 存在未恢复 pending 时 fail-closed；部分 flush 失败时不标记 closed。
    pub async fn close(&self) -> Result<BatchWriteReport, BatcherCloseError> {
        let mut guard = self.inner.lock().await;
        if guard.closed {
            return Ok(BatchWriteReport::default());
        }
        if guard.failed_pending.is_some() {
            return Err(BatcherCloseError {
                summary: close_report_from(&guard, BatchWriteReport::default()),
                source: pending_gate_error(),
            });
        }
        let table = guard.table.clone();
        let batch = std::mem::take(&mut guard.buffer);
        guard.last_flush = Instant::now();
        drop(guard);

        match self.flush_batch(&table, batch).await {
            Ok(report) => {
                let mut guard = self.inner.lock().await;
                if guard.failed_pending.is_some() {
                    return Err(BatcherCloseError {
                        summary: close_report_from(&guard, report),
                        source: pending_gate_error(),
                    });
                }
                guard.closed = true;
                Ok(report)
            }
            Err(source) => {
                let guard = self.inner.lock().await;
                Err(BatcherCloseError {
                    summary: close_report_from(&guard, BatchWriteReport::default()),
                    source,
                })
            }
        }
    }

    /// 是否存在未恢复 pending。
    pub async fn has_pending(&self) -> bool {
        self.inner.lock().await.failed_pending.is_some()
    }

    /// 未确认行数（无 pending 时为 0）。
    pub async fn pending_len(&self) -> usize {
        match &self.inner.lock().await.failed_pending {
            Some(pending) => pending.points.len(),
            None => 0,
        }
    }

    /// 取出未确认点交给 durable recovery owner；转移后清除 pending，可继续写入。
    pub async fn take_pending(&self) -> TaosResult<Vec<TaosPoint>> {
        let mut guard = self.inner.lock().await;
        match guard.failed_pending.take() {
            Some(pending) => Ok(pending.points),
            None => Err(TaosError::Invalid(
                "WriteBatcher 无 pending 可转移".to_owned(),
            )),
        }
    }

    /// 确认调用方已在外部完成恢复；清除 pending 且不返回点。
    pub async fn ack_pending(&self) -> TaosResult<()> {
        let mut guard = self.inner.lock().await;
        if guard.failed_pending.is_none() {
            return Err(TaosError::Invalid(
                "WriteBatcher 无 pending 可确认".to_owned(),
            ));
        }
        guard.failed_pending = None;
        Ok(())
    }

    /// 累计 `(accepted, failed)`（pending 计入 failed）。
    pub async fn totals(&self) -> (usize, usize) {
        let guard = self.inner.lock().await;
        (guard.total_accepted, guard.total_failed)
    }

    /// 构造 `close` 失败摘要（不持锁跨越 await）。
    pub async fn close_report(&self) -> BatcherCloseReport {
        let guard = self.inner.lock().await;
        close_report_from(&guard, BatchWriteReport::default())
    }

    /// 单批刷写；部分成功时把未确认 suffix 记入 pending。
    async fn flush_batch(
        &self,
        table: &str,
        batch: Vec<TaosPoint>,
    ) -> TaosResult<BatchWriteReport> {
        if batch.is_empty() {
            return Ok(BatchWriteReport::default());
        }
        let max_rows = self.pool.config().batch_max_rows;
        match self
            .pool
            .write_batch_chunked_outcome(table, &batch, max_rows)
            .await
        {
            Ok(report) => {
                let mut guard = self.inner.lock().await;
                guard.total_accepted = guard.total_accepted.saturating_add(report.accepted);
                Ok(report)
            }
            Err(partial) => {
                let accepted_in_batch = partial.report.accepted;
                let unconfirmed: Vec<TaosPoint> =
                    batch.into_iter().skip(accepted_in_batch).collect();
                let pending_count = unconfirmed.len();
                let mut guard = self.inner.lock().await;
                guard.total_accepted = guard.total_accepted.saturating_add(partial.report.accepted);
                guard.total_failed = guard.total_failed.saturating_add(pending_count);
                guard.failed_pending = Some(FailedPending {
                    points: unconfirmed,
                });
                Err(partial.into())
            }
        }
    }
}

/// pending 未恢复时的统一拒绝错误。
fn pending_gate_error() -> TaosError {
    TaosError::Unavailable(
        "WriteBatcher 存在未恢复 pending，须先 take_pending 或 ack_pending".to_owned(),
    )
}

/// 由内部状态构造关闭摘要。
fn close_report_from(inner: &Inner, last_flush: BatchWriteReport) -> BatcherCloseReport {
    let pending = inner
        .failed_pending
        .as_ref()
        .map(|pending| pending.points.len())
        .unwrap_or(0);
    BatcherCloseReport {
        total_accepted: inner.total_accepted,
        total_failed: inner.total_failed,
        pending,
        last_flush,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TaosConfig;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    const CREATE_OK: &str = r#"{"code":0,"column_meta":[],"data":[],"rows":0}"#;
    const DESCRIBE_OK: &str = concat!(
        r#"{"code":0,"column_meta":[["field","VARCHAR",16],["type","VARCHAR",16],["length","VARCHAR",8]],"#,
        r#""data":[["ts","TIMESTAMP","8"],["bid","NCHAR","64"],["ask","NCHAR","64"]],"rows":3}"#
    );
    const INSERT_OK: &str = r#"{"code":0,"column_meta":[],"data":[],"rows":0,"affected_rows":1}"#;
    const INSERT_FAIL: &str = r#"{"code":-1,"desc":"injected write failure"}"#;

    fn point(tag: &str, timestamp_ns: i64) -> TaosPoint {
        TaosPoint::new(tag, timestamp_ns, "0.01", "0.02")
    }

    /// 依序为多个请求返回 200 JSON body。
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

    fn pool_with(port: u16, batch_max_rows: usize) -> TaosPool {
        let config = TaosConfig {
            port,
            database: String::new(),
            batch_max_rows,
            timeout: Duration::from_secs(2),
            ..TaosConfig::default()
        };
        TaosPool::new(config).expect("pool")
    }

    fn batcher(pool: TaosPool) -> WriteBatcher {
        WriteBatcher::new(
            pool,
            "ticks",
            WriteBatcherConfig {
                max_rows: 100,
                flush_interval: Duration::from_secs(60),
                ..Default::default()
            },
        )
    }

    #[tokio::test]
    async fn empty_flush_is_complete_and_offline() {
        let pool = TaosPool::new(TaosConfig::default()).expect("pool");
        let batcher = WriteBatcher::new(pool, "sc_batcher", WriteBatcherConfig::default());
        let report = batcher.flush().await.expect("空刷写");
        assert_eq!(report, BatchWriteReport::default());
        assert!(report.is_complete());
        assert!(!batcher.has_pending().await);
        assert_eq!(batcher.close_report().await, BatcherCloseReport::default());
    }

    #[tokio::test]
    async fn full_success_flush_then_close() {
        let port = serve_sequence(vec![CREATE_OK, DESCRIBE_OK, INSERT_OK]).await;
        let batcher = batcher(pool_with(port, 10));
        batcher.push(point("A", 1_000_000)).await.expect("push");
        let flush_report = batcher.flush().await.expect("flush");
        assert_eq!(flush_report.accepted, 1);
        assert_eq!(flush_report.failed, 0);
        let close_report = batcher.close().await.expect("close");
        assert_eq!(close_report.accepted, 0);
        assert_eq!(batcher.totals().await, (1, 0));
        assert!(
            matches!(
                batcher.push(point("B", 2_000_000)).await,
                Err(TaosError::Closed(_))
            ),
            "关闭后 push 必须拒绝"
        );
    }

    #[tokio::test]
    async fn partial_failure_enters_pending_and_gates_writes() {
        let port = serve_sequence(vec![CREATE_OK, DESCRIBE_OK, INSERT_OK, INSERT_FAIL]).await;
        let batcher = batcher(pool_with(port, 1));
        batcher.push(point("BTC", 1_000_000)).await.expect("push1");
        batcher.push(point("ETH", 2_000_000)).await.expect("push2");
        let error = batcher.flush().await.expect_err("部分成功必须报错");
        assert!(error.to_string().contains("accepted=1"), "{error}");
        assert!(batcher.has_pending().await);
        assert_eq!(batcher.pending_len().await, 1);
        assert_eq!(batcher.totals().await, (1, 1));
        assert!(matches!(
            batcher.push(point("X", 3)).await,
            Err(TaosError::Unavailable(_))
        ));

        let pending = batcher.take_pending().await.expect("转移 pending");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].tag_value, "ETH");
        assert!(!batcher.has_pending().await);
        assert!(matches!(
            batcher.take_pending().await,
            Err(TaosError::Invalid(_))
        ));
        batcher
            .push(point("RESUME", 3_000_000))
            .await
            .expect("恢复后可继续写入");
    }

    #[tokio::test]
    async fn ack_pending_clears_without_transfer() {
        let port = serve_sequence(vec![CREATE_OK, DESCRIBE_OK, INSERT_OK, INSERT_FAIL]).await;
        let batcher = batcher(pool_with(port, 1));
        batcher.push(point("BTC", 1_000_000)).await.expect("push1");
        batcher.push(point("ETH", 2_000_000)).await.expect("push2");
        batcher.flush().await.expect_err("部分成功");
        assert!(batcher.has_pending().await, "部分成功后必须进入 pending");
        batcher.ack_pending().await.expect("确认 pending");
        assert!(!batcher.has_pending().await);
        assert!(batcher.ack_pending().await.is_err(), "重复确认必须拒绝");
    }

    #[tokio::test]
    async fn close_failure_returns_structured_summary() {
        let port = serve_sequence(vec![CREATE_OK, DESCRIBE_OK, INSERT_OK, INSERT_FAIL]).await;
        let batcher = batcher(pool_with(port, 1));
        batcher.push(point("BTC", 1_000_000)).await.expect("push1");
        batcher.push(point("ETH", 2_000_000)).await.expect("push2");
        let error = batcher.close().await.expect_err("部分成功必须报错");
        assert_eq!(error.summary.total_accepted, 1);
        assert_eq!(error.summary.total_failed, 1);
        assert_eq!(error.summary.pending, 1);
        assert!(error.to_string().contains("pending=1"));
        assert_eq!(batcher.close_report().await.pending, 1);
    }

    #[tokio::test]
    async fn auto_flush_on_row_threshold() {
        // 每次 flush 都会先 ensure_stable（CREATE + DESCRIBE）再 INSERT，故需 6 条响应。
        let port = serve_sequence(vec![
            CREATE_OK,
            DESCRIBE_OK,
            INSERT_OK,
            CREATE_OK,
            DESCRIBE_OK,
            INSERT_OK,
        ])
        .await;
        let pool = pool_with(port, 10);
        let batcher = WriteBatcher::new(
            pool,
            "ticks",
            WriteBatcherConfig {
                max_rows: 1,
                flush_interval: Duration::from_secs(60),
                ..Default::default()
            },
        );
        batcher
            .push(point("A", 1_000_000))
            .await
            .expect("首次 push 触发刷写");
        assert_eq!(batcher.totals().await, (1, 0));
        batcher
            .push(point("B", 2_000_000))
            .await
            .expect("第二次 push 触发刷写");
        assert_eq!(batcher.totals().await, (2, 0));
    }
}
