//! TDengine 客户端的公共数据类型。
//!
//! 自 `client.rs` 拆出；公共路径由 `client.rs` 的 `pub use` 保持不变。

use crate::config::TsPrecision;
use crate::error::TaosError;
use crate::metrics::TaosMetricsSnapshot;

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
