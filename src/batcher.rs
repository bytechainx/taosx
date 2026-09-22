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
    /// 禁止新写入（`close()` 或重复 `close()` 后）。
    closed: bool,
    /// `close()` 进入无锁 flush 窗口前设置，禁止窗口期内并发 push 写入。
    closing: bool,
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
                // 容量硬截断为 1024 是有意权衡：避免大 max_rows（配置上限
                // HARD_MAX_BATCH_ROWS = 10_000）时过量预分配；超限后 Vec
                // 重分配为摊还 O(1)，性能影响微小。
                buffer: Vec::with_capacity(config.max_rows.min(1024)),
                closed: false,
                closing: false,
                failed_pending: None,
                last_flush: Instant::now(),
                config,
                total_accepted: 0,
                total_failed: 0,
            })),
        }
    }

    /// 推入点；达到行数上限或超出时间窗口时自动刷写。
    ///
    /// 在 `close()` 的刷写窗口期内（`closing` 已置位、锁已释放）拒绝写入，防止数据
    /// 进入缓冲区后被即将结束的 `close()` 静默丢弃。
    ///
    /// # Cancellation
    ///
    /// **非 cancel-safe**：达到阈值触发刷写时，缓冲已被 `take`（点已移出 batcher）；
    /// 若该刷写 future 在 `.await` 点被取消（如外层 `select!` / `timeout` 丢弃），
    /// 这批点随之丢失，不会回到缓冲或 pending。调用方须自行保证不取消进行中的
    /// `push`，或接受丢失（与模块级文档「非 exactly-once」一致）。
    pub async fn push(&self, point: TaosPoint) -> TaosResult<()> {
        let mut guard = self.inner.lock().await;
        if guard.closed || guard.closing {
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
    ///
    /// 在 `close()` 进行中（`closing` 已置位）时拒绝刷写，避免并发刷写干扰关闭流程。
    pub async fn flush(&self) -> TaosResult<BatchWriteReport> {
        let mut guard = self.inner.lock().await;
        if guard.closed || guard.closing {
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
    ///
    /// ## 关闭语义
    ///
    /// `close()` 在取出缓冲并释放锁**之前**设置 `closing` 标志；此后任何并发
    /// `push()` 或 `flush()` 都将被拒绝（返回 `Closed`），消除原实现中无锁
    /// `flush_batch` 窗口期内并发 push 数据被静默丢失的竞态缺陷。失败路径会清除
    /// `closing` 以允许外部恢复后重试。
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
        // 持锁置 closing：此后任何并发 push/flush 在窗口期内将被拒绝
        guard.closing = true;
        let table = guard.table.clone();
        let batch = std::mem::take(&mut guard.buffer);
        guard.last_flush = Instant::now();
        drop(guard);

        match self.flush_batch(&table, batch).await {
            Ok(report) => {
                let mut guard = self.inner.lock().await;
                if guard.failed_pending.is_some() {
                    guard.closing = false;
                    return Err(BatcherCloseError {
                        summary: close_report_from(&guard, report),
                        source: pending_gate_error(),
                    });
                }
                guard.closed = true;
                Ok(report)
            }
            Err(source) => {
                let mut guard = self.inner.lock().await;
                guard.closing = false;
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

    /// 创建 mock 服务器：前 `quick` 个响应立即返回，最后一个响应在 barrier 同步后、
    /// `go` 通知后返回。用于确定性复现 close() 窗口期竞态。
    async fn serve_sequence_with_delayed_last(
        quick: Vec<&'static str>,
        delayed: &'static str,
        go: Arc<tokio::sync::Notify>,
        ready: Arc<tokio::sync::Barrier>,
    ) -> u16 {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        tokio::spawn(async move {
            for body in quick {
                let (mut stream, _) = listener.accept().await.expect("accept quick");
                let mut request = [0u8; 4096];
                let _ = stream.read(&mut request).await.expect("read quick request");
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream
                    .write_all(response.as_bytes())
                    .await
                    .expect("write quick response");
            }
            // 最后一个响应：等待 barrier 同步，再等待 go 通知
            let (mut stream, _) = listener.accept().await.expect("accept delayed");
            let mut request = [0u8; 4096];
            let _ = stream
                .read(&mut request)
                .await
                .expect("read delayed request");
            ready.wait().await;
            go.notified().await;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{delayed}",
                delayed.len()
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("write delayed response");
        });
        port
    }

    /// 确定性复现 P0-1 竞态：close() 无锁 flush 窗口期内并发 push 的数据被静默丢失。
    ///
    /// 场景：close() 在 flush_batch（无锁网络 I/O）期间，另一个 task 的 push()
    /// 因 `guard.closed` 仍为 `false` 而成功写入 buffer，但 close() 重获锁后不检查
    /// buffer，直接设 `closed = true`，导致该数据永久静默丢失。
    #[tokio::test]
    async fn close_window_concurrent_push_rejected_not_silently_lost() {
        let go = Arc::new(tokio::sync::Notify::new());
        let ready = Arc::new(tokio::sync::Barrier::new(2));

        // 服务端：CREATE_OK + DESCRIBE_OK 立即返回，INSERT_OK 等 barrier + go 信号
        let port = serve_sequence_with_delayed_last(
            vec![CREATE_OK, DESCRIBE_OK],
            INSERT_OK,
            go.clone(),
            ready.clone(),
        )
        .await;

        let batcher = Arc::new(batcher(pool_with(port, 100)));

        // 预推入 INIT 点（不触发刷写：max_rows=100 > 1），使 close() 有数据可刷
        batcher
            .push(point("INIT", 1_000_000))
            .await
            .expect("init push");

        // 后台 spawn close() — insert 将阻塞在 ready barrier + go 通知处
        let b = batcher.clone();
        let close_handle = tokio::spawn(async move { b.close().await });

        // barrier 同步：确保 close 已发送 INSERT 请求、进入 flush_batch 等待
        ready.wait().await;

        // close 卡在 flush_batch（Mutex 已释放），主线程 push —— 这是竞态窗口
        let push_result = batcher.push(point("RACE", 2_000_000)).await;

        // 放行 close 的 INSERT 响应
        go.notify_one();

        // 等待 close 完成
        let _close_report = close_handle
            .await
            .expect("close task 不应 panic")
            .expect("close 应成功（INSERT_OK）");

        // 核心断言：窗口内 push 不得静默丢失
        match push_result {
            Ok(()) => {
                // push 成功 → 数据必须被计入，不能丢失
                let (accepted, _) = batcher.totals().await;
                assert!(
                    accepted >= 2,
                    "push 在 close 窗口内成功则数据必须被刷写计入: \
                     accepted={accepted}（期望 >=2，INIT + RACE）"
                );
            }
            Err(ref e) => {
                // push 被拒绝 → 这将是方案 A 修复后的预期行为
                assert!(
                    matches!(e, TaosError::Closed(_)),
                    "窗口内 push 应返回 Closed 错误，实际: {e}"
                );
            }
        }
    }

    #[tokio::test]
    async fn auto_flush_on_time_window() {
        // 时间窗分支：行数未达 max_rows，但距上次刷写超过 flush_interval 后，
        // 下一次 push 必须触发刷写（单次 flush = CREATE + DESCRIBE + INSERT 三条响应）。
        let port = serve_sequence(vec![CREATE_OK, DESCRIBE_OK, INSERT_OK]).await;
        let pool = pool_with(port, 10);
        let batcher = WriteBatcher::new(
            pool,
            "ticks",
            WriteBatcherConfig {
                max_rows: 100,
                flush_interval: Duration::from_millis(10),
                ..Default::default()
            },
        );
        batcher
            .push(point("A", 1_000_000))
            .await
            .expect("首次 push 不触发刷写");
        assert_eq!(
            batcher.totals().await,
            (0, 0),
            "行数与时间窗均未达，不应刷写"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
        batcher
            .push(point("B", 2_000_000))
            .await
            .expect("时间窗触发刷写");
        assert_eq!(
            batcher.totals().await,
            (2, 0),
            "时间窗到期后 push 必须刷写全部缓冲行"
        );
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
