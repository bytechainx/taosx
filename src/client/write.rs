//! `TaosPool` 的批量写入与区间查询方法组。
//!
//! 自 `client.rs` 拆出（生产段超 800 行的拆分）；`impl TaosPool` 按职责分块，
//! 公共 API 与路径均不变。

use crate::error::{TaosError, TaosResult};
use crate::point::TaosPoint;

use super::response::parse_ts_cell;
use super::sql::{build_insert_sql_chunks_with_limits, validate_stable_ident};
use super::types::{BatchWritePartialError, BatchWriteReport};
use super::TaosPool;

impl TaosPool {
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
}
