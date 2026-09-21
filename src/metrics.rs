//! 进程内有界操作计数 + 请求/响应字节计数 + Prometheus 文本导出。
//!
//! 标签仅 `op` / `outcome` 语义，无高基数 symbol/table，避免指标爆炸。

use std::sync::atomic::{AtomicU64, Ordering};

/// 池/传输操作计数快照。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TaosMetricsSnapshot {
    /// REST SQL 成功次数。
    pub sql_ok: u64,
    /// REST SQL 失败次数。
    pub sql_err: u64,
    /// 请求 SQL 累计 UTF-8 字节数。
    pub sql_bytes: u64,
    /// 响应体累计字节数。
    pub response_bytes: u64,
    /// 批量写入整批成功次数。
    pub write_ok: u64,
    /// 批量写入失败次数（含部分成功映射为错误）。
    pub write_err: u64,
    /// `query_series` 成功次数。
    pub query_ok: u64,
    /// `query_series` 失败次数。
    pub query_err: u64,
    /// `ping` 成功次数。
    pub ping_ok: u64,
    /// `ping` 失败次数。
    pub ping_err: u64,
    /// `health_check` 判定就绪次数。
    pub health_ready: u64,
    /// `health_check` 判定未就绪次数。
    pub health_not_ready: u64,
    /// Native WS 握手探测成功（进程级累计）。
    pub ws_probe_ok: u64,
    /// Native WS 握手探测失败（进程级累计）。
    pub ws_probe_err: u64,
}

impl TaosMetricsSnapshot {
    /// 全部操作计数之和（不含字节计数，用作粗粒度负载指示）。
    #[must_use]
    pub fn total_events(&self) -> u64 {
        self.sql_ok
            + self.sql_err
            + self.write_ok
            + self.write_err
            + self.query_ok
            + self.query_err
            + self.ping_ok
            + self.ping_err
            + self.health_ready
            + self.health_not_ready
            + self.ws_probe_ok
            + self.ws_probe_err
    }

    /// Prometheus 文本格式（counter；低基数标签）。
    #[must_use]
    pub fn to_prometheus_text(&self) -> String {
        let mut out = String::with_capacity(768);
        out.push_str("# HELP taosx_ops_total Operation counters\n");
        out.push_str("# TYPE taosx_ops_total counter\n");
        for (op, outcome, value) in [
            ("sql", "ok", self.sql_ok),
            ("sql", "err", self.sql_err),
            ("write", "ok", self.write_ok),
            ("write", "err", self.write_err),
            ("query", "ok", self.query_ok),
            ("query", "err", self.query_err),
            ("ping", "ok", self.ping_ok),
            ("ping", "err", self.ping_err),
            ("health", "ready", self.health_ready),
            ("health", "not_ready", self.health_not_ready),
            ("ws_probe", "ok", self.ws_probe_ok),
            ("ws_probe", "err", self.ws_probe_err),
        ] {
            out.push_str(&format!(
                "taosx_ops_total{{op=\"{op}\",outcome=\"{outcome}\"}} {value}\n"
            ));
        }
        out.push_str("# HELP taosx_bytes_total Request and response byte counters\n");
        out.push_str("# TYPE taosx_bytes_total counter\n");
        out.push_str(&format!(
            "taosx_bytes_total{{op=\"request\"}} {}\n",
            self.sql_bytes
        ));
        out.push_str(&format!(
            "taosx_bytes_total{{op=\"response\"}} {}\n",
            self.response_bytes
        ));
        out
    }
}

/// 池级计数器（随 `TaosPool` 生命周期）。
pub(crate) struct OpCounters {
    sql_ok: AtomicU64,
    sql_err: AtomicU64,
    sql_bytes: AtomicU64,
    response_bytes: AtomicU64,
    write_ok: AtomicU64,
    write_err: AtomicU64,
    query_ok: AtomicU64,
    query_err: AtomicU64,
    ping_ok: AtomicU64,
    ping_err: AtomicU64,
    health_ready: AtomicU64,
    health_not_ready: AtomicU64,
}

impl OpCounters {
    pub(crate) fn new() -> Self {
        Self {
            sql_ok: AtomicU64::new(0),
            sql_err: AtomicU64::new(0),
            sql_bytes: AtomicU64::new(0),
            response_bytes: AtomicU64::new(0),
            write_ok: AtomicU64::new(0),
            write_err: AtomicU64::new(0),
            query_ok: AtomicU64::new(0),
            query_err: AtomicU64::new(0),
            ping_ok: AtomicU64::new(0),
            ping_err: AtomicU64::new(0),
            health_ready: AtomicU64::new(0),
            health_not_ready: AtomicU64::new(0),
        }
    }

    pub(crate) fn snapshot(&self) -> TaosMetricsSnapshot {
        let (ws_probe_ok, ws_probe_err) = ws_probe_totals();
        TaosMetricsSnapshot {
            sql_ok: self.sql_ok.load(Ordering::Relaxed),
            sql_err: self.sql_err.load(Ordering::Relaxed),
            sql_bytes: self.sql_bytes.load(Ordering::Relaxed),
            response_bytes: self.response_bytes.load(Ordering::Relaxed),
            write_ok: self.write_ok.load(Ordering::Relaxed),
            write_err: self.write_err.load(Ordering::Relaxed),
            query_ok: self.query_ok.load(Ordering::Relaxed),
            query_err: self.query_err.load(Ordering::Relaxed),
            ping_ok: self.ping_ok.load(Ordering::Relaxed),
            ping_err: self.ping_err.load(Ordering::Relaxed),
            health_ready: self.health_ready.load(Ordering::Relaxed),
            health_not_ready: self.health_not_ready.load(Ordering::Relaxed),
            ws_probe_ok,
            ws_probe_err,
        }
    }

    pub(crate) fn add_sql_bytes(&self, bytes: usize) {
        self.sql_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn add_response_bytes(&self, bytes: usize) {
        self.response_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn inc_sql_ok(&self) {
        self.sql_ok.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_sql_err(&self) {
        self.sql_err.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_write_ok(&self) {
        self.write_ok.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_write_err(&self) {
        self.write_err.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_query_ok(&self) {
        self.query_ok.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_query_err(&self) {
        self.query_err.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_ping_ok(&self) {
        self.ping_ok.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_ping_err(&self) {
        self.ping_err.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_health_ready(&self) {
        self.health_ready.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_health_not_ready(&self) {
        self.health_not_ready.fetch_add(1, Ordering::Relaxed);
    }
}

static WS_PROBE_OK: AtomicU64 = AtomicU64::new(0);
static WS_PROBE_ERR: AtomicU64 = AtomicU64::new(0);

/// 进程级 WS 探测计数（[`crate::connect_native_ws`] / [`crate::exec_sql_ws`] 路径）。
#[must_use]
pub fn ws_probe_totals() -> (u64, u64) {
    (
        WS_PROBE_OK.load(Ordering::Relaxed),
        WS_PROBE_ERR.load(Ordering::Relaxed),
    )
}

pub(crate) fn record_ws_probe(ok: bool) {
    if ok {
        WS_PROBE_OK.fetch_add(1, Ordering::Relaxed);
    } else {
        WS_PROBE_ERR.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_accumulate_with_bytes() {
        let counters = OpCounters::new();
        counters.inc_sql_ok();
        counters.inc_sql_err();
        counters.inc_write_ok();
        counters.inc_query_ok();
        counters.inc_query_err();
        counters.inc_ping_ok();
        counters.inc_ping_err();
        counters.inc_health_ready();
        counters.inc_health_not_ready();
        counters.inc_write_err();
        counters.add_sql_bytes(120);
        counters.add_response_bytes(4096);

        let snapshot = counters.snapshot();
        assert_eq!(snapshot.sql_ok, 1);
        assert_eq!(snapshot.sql_err, 1);
        assert_eq!(snapshot.write_ok, 1);
        assert_eq!(snapshot.sql_bytes, 120);
        assert_eq!(snapshot.response_bytes, 4096);
        assert!(snapshot.total_events() >= 9);

        let text = snapshot.to_prometheus_text();
        assert!(text.contains("taosx_ops_total"));
        assert!(text.contains("op=\"sql\""));
        assert!(text.contains("taosx_bytes_total{op=\"request\"} 120"));
        assert!(text.contains("taosx_bytes_total{op=\"response\"} 4096"));
    }
}
