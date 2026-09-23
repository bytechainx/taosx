//! 有界查询流：把 [`TaosPool::query_series`] 的结果按行 yield，慢消费者不会无限堆积。
//!
//! 取舍：REST 传输一次性返回整个结果集（受 `max_query_rows` 与 `max_response_bytes`
//! 约束），因此本流是「先有界物化、再逐行 yield」的包装，而非服务端游标。
//! `chunk_hint` 保留为对底层拉取块大小的提示与参数校验，不改变行级语义。

use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;

use crate::client::{validate_chunk_hint, TaosPool};
use crate::error::TaosResult;
use crate::point::TaosPoint;

/// Driver 技术行流式查询包装（内部仍受 `max_query_rows` 限制）。
#[derive(Debug)]
pub struct TaosQueryStream {
    rows: std::vec::IntoIter<TaosPoint>,
    chunk_hint: usize,
    done: bool,
}

impl TaosQueryStream {
    /// 从已物化的行构造流。
    #[must_use]
    pub fn from_rows(rows: Vec<TaosPoint>) -> Self {
        Self {
            rows: rows.into_iter(),
            chunk_hint: 1,
            done: false,
        }
    }

    /// 从已物化的行构造流，并记录拉取块大小提示（`chunk_hint` 必须 ≥ 1）。
    pub fn from_rows_chunked(rows: Vec<TaosPoint>, chunk_hint: usize) -> TaosResult<Self> {
        validate_chunk_hint(chunk_hint)?;
        Ok(Self {
            rows: rows.into_iter(),
            chunk_hint,
            done: false,
        })
    }

    /// 剩余大致行数。
    #[must_use]
    pub fn remaining_hint(&self) -> usize {
        self.rows.len()
    }

    /// 构造时记录的拉取块大小提示。
    #[must_use]
    pub fn chunk_hint(&self) -> usize {
        self.chunk_hint
    }
}

impl Stream for TaosQueryStream {
    type Item = TaosResult<TaosPoint>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        match self.rows.next() {
            Some(point) => Poll::Ready(Some(Ok(point))),
            None => {
                self.done = true;
                Poll::Ready(None)
            }
        }
    }
}

impl TaosPool {
    /// 流式查询：先有界 collect，再按行 yield（遵守 `max_query_rows`）。
    ///
    /// 取消：丢弃 stream 即停止消费；服务端查询在 collect 阶段已完成（REST 限制）。
    pub async fn query_series_stream(
        &self,
        table: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> TaosResult<TaosQueryStream> {
        let rows = self.query_series(table, start_ns, end_ns).await?;
        Ok(TaosQueryStream::from_rows(rows))
    }

    /// 带块大小提示的流式查询（语义同 [`TaosPool::query_series_stream`]）。
    ///
    /// `chunk_hint == 0` 返回 [`crate::TaosError::Invalid`]。
    pub async fn query_series_stream_chunked(
        &self,
        table: &str,
        start_ns: i64,
        end_ns: i64,
        chunk_hint: usize,
    ) -> TaosResult<TaosQueryStream> {
        validate_chunk_hint(chunk_hint)?;
        let rows = self.query_series(table, start_ns, end_ns).await?;
        TaosQueryStream::from_rows_chunked(rows, chunk_hint)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::TaosError;
    use futures_util::StreamExt;

    #[tokio::test]
    async fn stream_yields_all_rows() {
        let rows = vec![
            TaosPoint::new("A", 1, "0.01", "0.02"),
            TaosPoint::new("B", 2, "0.03", "0.04"),
        ];
        let mut stream = TaosQueryStream::from_rows(rows);
        assert_eq!(stream.remaining_hint(), 2);
        let first = stream.next().await.expect("首行").expect("Ok");
        assert_eq!(first.tag_value, "A");
        let second = stream.next().await.expect("次行").expect("Ok");
        assert_eq!(second.tag_value, "B");
        assert!(stream.next().await.is_none());
        assert!(stream.next().await.is_none(), "结束后必须保持 Ready(None)");
    }

    #[test]
    fn chunk_hint_is_validated_and_exposed() {
        let stream = TaosQueryStream::from_rows_chunked(Vec::new(), 32).expect("合法提示");
        assert_eq!(stream.chunk_hint(), 32);
        // `TaosQueryStream` 已实现 `Debug`（本项新增），此处可直接用 `expect_err`。
        let error =
            TaosQueryStream::from_rows_chunked(Vec::new(), 0).expect_err("chunk_hint=0 必须拒绝");
        assert!(matches!(error, TaosError::Invalid(_)), "{error:?}");
        let error = validate_chunk_hint(0).expect_err("chunk_hint=0 必须拒绝");
        assert!(matches!(error, TaosError::Invalid(_)), "{error:?}");
    }

    /// `TaosQueryStream` 必须可 `Debug`，且输出含关键字段（剩余行数、块提示、结束标志）。
    ///
    /// 三个字段的底层类型均实现 `Debug`（`IntoIter<TaosPoint>` / `usize` / `bool`），
    /// 故直接 derive；行内容均为市场数据，不含任何凭据。
    #[test]
    fn debug_output_is_available_and_non_empty() {
        let stream =
            TaosQueryStream::from_rows_chunked(vec![TaosPoint::new("A", 1, "0.01", "0.02")], 16)
                .expect("合法提示");
        let rendered = format!("{stream:?}");
        assert!(!rendered.is_empty(), "Debug 输出不得为空");
        assert!(rendered.contains("TaosQueryStream"), "{rendered}");
        assert!(rendered.contains("chunk_hint: 16"), "{rendered}");
        assert!(rendered.contains("done: false"), "{rendered}");
    }
}
