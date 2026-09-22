#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! E2E（taosx）：在**真实** TDengine 上端到端执行**全部**公开接口。
//!
//! 与 `live_taos.rs`（双传输冒烟往返）不同，本文件的对齐对象是
//! `cargo +nightly public-api --simplified` 导出的完整公开面：
//! `fn` / `type` / `field` / `const` / `variant` 五类逐条登记在 [`E2E_MANIFEST`]，
//! 运行期由 `cover` 登记表核对「声明 = 实际执行」（缺一即失败）。
//!
//! **独立核对**：`scripts/verify-e2e-coverage.mjs` 会重新派生公开面与清单双向 diff，
//! 并用 `-C instrument-coverage` + `llvm-cov report --show-functions` 断言每条公开
//! 函数执行次数 > 0；本文件内的登记表只是**声明**，不是唯一证据。
//!
//! ## 双传输（一次运行内都真跑）
//!
//! `taosx` 的两种传输端点都由 **taosAdapter** 提供，因而共用同一个端口：
//! REST 走 `POST /rest/sql`，原生 WebSocket 走 `WS /rest/ws`（实测 6041 对 WS 升级
//! 返回 101）。`FOUNDATIONX_TAOSX_PORT` 给的是**原生 TCP 端口**（6030），它不服务
//! `/rest/ws`；故数据面端口取 `FOUNDATIONX_TAOSX_REST_PORT`（缺省 6041），而 6030
//! 只用于 [`probe_native_tcp`] 的端口探活（与 `live_taos.rs` 的归一化口径一致）。
//! 两套显式配置（`Rest` / `NativeWs`）各跑一遍
//! connect → ping → health_check → DDL → write → query → drop → close。
//!
//! ## 隔离与凭据
//!
//! 凭据只从环境变量 `FOUNDATIONX_TAOSX_*` 读取，不硬编码、不回显；所有超级表名都是
//! `<前缀>_<pid>_<纳秒>` 且在收尾 DROP 并通过 `information_schema` 断言无残留，
//! 不触碰库内任何既有表。
//!
//! ```text
//! set -a; . /home/zone/workspace/sre/secrets/env/taosx.env; set +a
//! cd /home/workspace/bytechainx/taosx
//! CARGO_TARGET_DIR=/home/workspace/bytechainx/.cargo/target \
//!   cargo test --test e2e_taos -- --ignored --test-threads=1
//! ```

use std::collections::BTreeSet;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{FutureExt as _, StreamExt};
use taosx::{
    build_insert_sql_chunks, build_native_ws_url, connect_native_ws, exec_sql_ws, probe_native_tcp,
    validate_mode, ws_probe_totals, BatchWritePartialError, BatchWriteReport, BatcherCloseError,
    BatcherCloseReport, RetryPolicy, TaosClient, TaosConfig, TaosConfigBuilder, TaosError,
    TaosExecResult, TaosHealth, TaosMetricsSnapshot, TaosPoint, TaosPool, TaosPoolStats,
    TaosQueryStream, TaosResult, TransportMode, TsPrecision, WriteBatcher, WriteBatcherConfig,
    DEFAULT_DATABASE, DEFAULT_HOST, DEFAULT_PORT, DEFAULT_USER, ENV_ACQUIRE_TIMEOUT_MS,
    ENV_BATCH_MAX_BYTES, ENV_BATCH_MAX_ROWS, ENV_CLOSE_TIMEOUT_MS, ENV_DATABASE, ENV_HOST,
    ENV_HOSTS, ENV_MAX_IN_FLIGHT, ENV_MAX_QUERY_ROWS, ENV_MAX_RESPONSE_BYTES, ENV_PASSWORD,
    ENV_PORT, ENV_PRECISION, ENV_PREFIX, ENV_TIMEOUT_MS, ENV_TLS, ENV_TLS_CA_FILE, ENV_TRANSPORT,
    ENV_USER, ENV_WRITE_MAX_ATTEMPTS, HARD_MAX_BATCH_BYTES, HARD_MAX_BATCH_ROWS,
    HARD_MAX_CLOSE_TIMEOUT, HARD_MAX_IN_FLIGHT, HARD_MAX_QUERY_ROWS, HARD_MAX_RESPONSE_BYTES,
    HARD_MAX_TIMEOUT, HARD_MAX_WRITE_MAX_ATTEMPTS,
};

/// 公开面清单：`(条目类别, 入口 id)`，由 `cargo +nightly public-api --simplified` 派生并冻结。
///
/// 类别取值域：`fn` / `type` / `field` / `const` / `variant`。
/// 该清单是运行时登记的**唯一事实源**——`cover::hit` 拒绝清单外的 id，收尾断言拒绝
/// 「声明了却没执行」的条目。清单本身的时效性由外部核对器与公开面 diff 保证。
const E2E_MANIFEST: &[(&str, &str)] = &[
    ("type", "TaosError"),
    ("variant", "TaosError::Backend"),
    ("variant", "TaosError::Closed"),
    ("variant", "TaosError::Config"),
    ("variant", "TaosError::Connection"),
    ("variant", "TaosError::Invalid"),
    ("variant", "TaosError::Io"),
    ("variant", "TaosError::Serialization"),
    ("variant", "TaosError::Timeout"),
    ("variant", "TaosError::Unavailable"),
    ("variant", "TaosError::Unsupported"),
    ("fn", "TaosError::backend"),
    ("fn", "TaosError::from_http_status"),
    ("fn", "TaosError::from_taos_code"),
    ("fn", "TaosError::is_not_found"),
    ("fn", "TaosError::is_retryable"),
    ("fn", "TaosError::taos_code"),
    ("fn", "TaosError::with_message"),
    ("type", "TransportMode"),
    ("variant", "TransportMode::NativeWs"),
    ("variant", "TransportMode::Rest"),
    ("fn", "TransportMode::as_str"),
    ("fn", "TransportMode::parse"),
    ("type", "TsPrecision"),
    ("variant", "TsPrecision::Ms"),
    ("variant", "TsPrecision::Ns"),
    ("variant", "TsPrecision::Us"),
    ("fn", "TsPrecision::as_str"),
    ("fn", "TsPrecision::from_nanos"),
    ("fn", "TsPrecision::parse"),
    ("fn", "TsPrecision::to_nanos"),
    ("type", "BatchWritePartialError"),
    ("field", "BatchWritePartialError::report"),
    ("field", "BatchWritePartialError::source"),
    ("type", "BatchWriteReport"),
    ("field", "BatchWriteReport::accepted"),
    ("field", "BatchWriteReport::chunks_ok"),
    ("field", "BatchWriteReport::chunks_total"),
    ("field", "BatchWriteReport::failed"),
    ("fn", "BatchWriteReport::is_complete"),
    ("type", "BatcherCloseError"),
    ("field", "BatcherCloseError::source"),
    ("field", "BatcherCloseError::summary"),
    ("type", "BatcherCloseReport"),
    ("field", "BatcherCloseReport::last_flush"),
    ("field", "BatcherCloseReport::pending"),
    ("field", "BatcherCloseReport::total_accepted"),
    ("field", "BatcherCloseReport::total_failed"),
    ("type", "RetryPolicy"),
    ("field", "RetryPolicy::deadline"),
    ("field", "RetryPolicy::initial_backoff"),
    ("field", "RetryPolicy::jitter_ratio"),
    ("field", "RetryPolicy::max_attempts"),
    ("field", "RetryPolicy::max_backoff"),
    ("fn", "RetryPolicy::backoff_for_attempt"),
    ("fn", "RetryPolicy::compute_backoff"),
    ("fn", "RetryPolicy::exponential_backoff"),
    ("fn", "RetryPolicy::for_idempotent_write"),
    ("fn", "RetryPolicy::for_read"),
    ("fn", "RetryPolicy::is_retryable"),
    ("fn", "RetryPolicy::no_retry"),
    ("fn", "RetryPolicy::run"),
    ("type", "TaosConfig"),
    ("field", "TaosConfig::acquire_timeout"),
    ("field", "TaosConfig::batch_max_bytes"),
    ("field", "TaosConfig::batch_max_rows"),
    ("field", "TaosConfig::close_timeout"),
    ("field", "TaosConfig::database"),
    ("field", "TaosConfig::host"),
    ("field", "TaosConfig::hosts"),
    ("field", "TaosConfig::max_in_flight"),
    ("field", "TaosConfig::max_query_rows"),
    ("field", "TaosConfig::max_response_bytes"),
    ("field", "TaosConfig::password"),
    ("field", "TaosConfig::port"),
    ("field", "TaosConfig::precision"),
    ("field", "TaosConfig::timeout"),
    ("field", "TaosConfig::tls"),
    ("field", "TaosConfig::tls_ca_file"),
    ("field", "TaosConfig::transport"),
    ("field", "TaosConfig::user"),
    ("field", "TaosConfig::write_max_attempts"),
    ("fn", "TaosConfig::builder"),
    ("fn", "TaosConfig::from_env"),
    ("fn", "TaosConfig::from_toml"),
    ("fn", "TaosConfig::from_toml_file"),
    ("fn", "TaosConfig::validate"),
    ("fn", "TaosConfig::endpoint_hosts"),
    ("fn", "TaosConfig::native_ws_endpoint"),
    ("fn", "TaosConfig::native_ws_url"),
    ("fn", "TaosConfig::rest_sql_db_url"),
    ("fn", "TaosConfig::rest_sql_endpoint"),
    ("fn", "TaosConfig::rest_sql_url"),
    ("fn", "TaosConfig::rest_sql_url_for"),
    ("type", "TaosConfigBuilder"),
    ("fn", "TaosConfigBuilder::acquire_timeout"),
    ("fn", "TaosConfigBuilder::batch_max_bytes"),
    ("fn", "TaosConfigBuilder::batch_max_rows"),
    ("fn", "TaosConfigBuilder::build"),
    ("fn", "TaosConfigBuilder::close_timeout"),
    ("fn", "TaosConfigBuilder::database"),
    ("fn", "TaosConfigBuilder::from_config"),
    ("fn", "TaosConfigBuilder::host"),
    ("fn", "TaosConfigBuilder::hosts"),
    ("fn", "TaosConfigBuilder::max_in_flight"),
    ("fn", "TaosConfigBuilder::max_query_rows"),
    ("fn", "TaosConfigBuilder::max_response_bytes"),
    ("fn", "TaosConfigBuilder::new"),
    ("fn", "TaosConfigBuilder::password"),
    ("fn", "TaosConfigBuilder::port"),
    ("fn", "TaosConfigBuilder::precision"),
    ("fn", "TaosConfigBuilder::timeout"),
    ("fn", "TaosConfigBuilder::tls"),
    ("fn", "TaosConfigBuilder::tls_ca_file"),
    ("fn", "TaosConfigBuilder::transport"),
    ("fn", "TaosConfigBuilder::user"),
    ("fn", "TaosConfigBuilder::write_max_attempts"),
    ("type", "TaosExecResult"),
    ("field", "TaosExecResult::affected_rows"),
    ("field", "TaosExecResult::code"),
    ("field", "TaosExecResult::columns"),
    ("field", "TaosExecResult::rows"),
    ("type", "TaosHealth"),
    ("field", "TaosHealth::detail"),
    ("field", "TaosHealth::metrics"),
    ("field", "TaosHealth::precision"),
    ("field", "TaosHealth::ready"),
    ("field", "TaosHealth::server_version"),
    ("field", "TaosHealth::stats"),
    ("fn", "TaosHealth::is_ready"),
    ("type", "TaosMetricsSnapshot"),
    ("field", "TaosMetricsSnapshot::health_not_ready"),
    ("field", "TaosMetricsSnapshot::health_ready"),
    ("field", "TaosMetricsSnapshot::ping_err"),
    ("field", "TaosMetricsSnapshot::ping_ok"),
    ("field", "TaosMetricsSnapshot::query_err"),
    ("field", "TaosMetricsSnapshot::query_ok"),
    ("field", "TaosMetricsSnapshot::response_bytes"),
    ("field", "TaosMetricsSnapshot::sql_bytes"),
    ("field", "TaosMetricsSnapshot::sql_err"),
    ("field", "TaosMetricsSnapshot::sql_ok"),
    ("field", "TaosMetricsSnapshot::write_err"),
    ("field", "TaosMetricsSnapshot::write_ok"),
    ("field", "TaosMetricsSnapshot::ws_probe_err"),
    ("field", "TaosMetricsSnapshot::ws_probe_ok"),
    ("fn", "TaosMetricsSnapshot::to_prometheus_text"),
    ("fn", "TaosMetricsSnapshot::total_events"),
    ("type", "TaosPoint"),
    ("field", "TaosPoint::tag_value"),
    ("field", "TaosPoint::timestamp_ns"),
    ("field", "TaosPoint::values"),
    ("fn", "TaosPoint::new"),
    ("type", "TaosPool"),
    ("fn", "TaosPool::client"),
    ("fn", "TaosPool::close"),
    ("fn", "TaosPool::config"),
    ("fn", "TaosPool::connect"),
    ("fn", "TaosPool::connect_from_env"),
    ("fn", "TaosPool::ensure_stable"),
    ("fn", "TaosPool::exec"),
    ("fn", "TaosPool::exec_sql_ws"),
    ("fn", "TaosPool::health_check"),
    ("fn", "TaosPool::is_closed"),
    ("fn", "TaosPool::liveness"),
    ("fn", "TaosPool::metrics"),
    ("fn", "TaosPool::metrics_prometheus"),
    ("fn", "TaosPool::new"),
    ("fn", "TaosPool::ping"),
    ("fn", "TaosPool::precision"),
    ("fn", "TaosPool::query"),
    ("fn", "TaosPool::stats"),
    ("fn", "TaosPool::query_series"),
    ("fn", "TaosPool::write_batch"),
    ("fn", "TaosPool::write_batch_chunked"),
    ("fn", "TaosPool::write_batch_chunked_outcome"),
    ("fn", "TaosPool::write_batch_chunked_report"),
    ("fn", "TaosPool::write_batch_idempotent"),
    ("fn", "TaosPool::write_batch_report"),
    ("fn", "TaosPool::write_series"),
    ("fn", "TaosPool::query_series_stream"),
    ("fn", "TaosPool::query_series_stream_chunked"),
    ("type", "TaosPoolStats"),
    ("field", "TaosPoolStats::closed"),
    ("field", "TaosPoolStats::in_flight"),
    ("type", "TaosQueryStream"),
    ("fn", "TaosQueryStream::chunk_hint"),
    ("fn", "TaosQueryStream::from_rows"),
    ("fn", "TaosQueryStream::from_rows_chunked"),
    ("fn", "TaosQueryStream::remaining_hint"),
    ("type", "WriteBatcher"),
    ("fn", "WriteBatcher::ack_pending"),
    ("fn", "WriteBatcher::close"),
    ("fn", "WriteBatcher::close_report"),
    ("fn", "WriteBatcher::flush"),
    ("fn", "WriteBatcher::has_pending"),
    ("fn", "WriteBatcher::new"),
    ("fn", "WriteBatcher::pending_len"),
    ("fn", "WriteBatcher::push"),
    ("fn", "WriteBatcher::take_pending"),
    ("fn", "WriteBatcher::totals"),
    ("type", "WriteBatcherConfig"),
    ("field", "WriteBatcherConfig::flush_interval"),
    ("field", "WriteBatcherConfig::max_bytes_hint"),
    ("field", "WriteBatcherConfig::max_rows"),
    ("const", "DEFAULT_DATABASE"),
    ("const", "DEFAULT_HOST"),
    ("const", "DEFAULT_PORT"),
    ("const", "DEFAULT_USER"),
    ("const", "ENV_ACQUIRE_TIMEOUT_MS"),
    ("const", "ENV_BATCH_MAX_BYTES"),
    ("const", "ENV_BATCH_MAX_ROWS"),
    ("const", "ENV_CLOSE_TIMEOUT_MS"),
    ("const", "ENV_DATABASE"),
    ("const", "ENV_HOST"),
    ("const", "ENV_HOSTS"),
    ("const", "ENV_MAX_IN_FLIGHT"),
    ("const", "ENV_MAX_QUERY_ROWS"),
    ("const", "ENV_MAX_RESPONSE_BYTES"),
    ("const", "ENV_PASSWORD"),
    ("const", "ENV_PORT"),
    ("const", "ENV_PRECISION"),
    ("const", "ENV_PREFIX"),
    ("const", "ENV_TIMEOUT_MS"),
    ("const", "ENV_TLS"),
    ("const", "ENV_TLS_CA_FILE"),
    ("const", "ENV_TRANSPORT"),
    ("const", "ENV_USER"),
    ("const", "ENV_WRITE_MAX_ATTEMPTS"),
    ("const", "HARD_MAX_BATCH_BYTES"),
    ("const", "HARD_MAX_BATCH_ROWS"),
    ("const", "HARD_MAX_CLOSE_TIMEOUT"),
    ("const", "HARD_MAX_IN_FLIGHT"),
    ("const", "HARD_MAX_QUERY_ROWS"),
    ("const", "HARD_MAX_RESPONSE_BYTES"),
    ("const", "HARD_MAX_TIMEOUT"),
    ("const", "HARD_MAX_WRITE_MAX_ATTEMPTS"),
    ("fn", "build_insert_sql_chunks"),
    ("fn", "build_native_ws_url"),
    ("fn", "connect_native_ws"),
    ("fn", "exec_sql_ws"),
    ("fn", "probe_native_tcp"),
    ("fn", "validate_mode"),
    ("fn", "ws_probe_totals"),
    ("type", "TaosClient"),
    ("type", "TaosResult"),
];

/// 覆盖登记表：只登记**真实发生**的调用/读取，不登记「计划要调用」。
mod cover {
    use std::collections::BTreeSet;
    use std::sync::{Mutex, OnceLock};

    static EXECUTED: OnceLock<Mutex<BTreeSet<(&'static str, &'static str)>>> = OnceLock::new();

    fn log() -> &'static Mutex<BTreeSet<(&'static str, &'static str)>> {
        EXECUTED.get_or_init(|| Mutex::new(BTreeSet::new()))
    }

    /// 登记一次真实执行。清单外的 `(类别, id)` 立即 panic，防止调用点与清单漂移。
    pub fn hit(kind: &'static str, id: &'static str) {
        assert!(
            super::E2E_MANIFEST
                .iter()
                .any(|(declared_kind, declared_id)| *declared_kind == kind && *declared_id == id),
            "登记了清单外的公开条目：{kind} {id}"
        );
        log().lock().expect("覆盖登记表锁中毒").insert((kind, id));
    }

    pub fn executed() -> BTreeSet<(&'static str, &'static str)> {
        log().lock().expect("覆盖登记表锁中毒").clone()
    }
}

/// 覆盖登记的简写入口（保持调用点可读）。
fn hit(kind: &'static str, id: &'static str) {
    cover::hit(kind, id);
}

/// 清单自身良构：类别取值域合法、`(类别, id)` 不重复。
fn assert_manifest_wellformed() {
    let mut seen: BTreeSet<(&str, &str)> = BTreeSet::new();
    for (kind, id) in E2E_MANIFEST {
        assert!(
            matches!(*kind, "fn" | "type" | "field" | "const" | "variant"),
            "未知条目类别 {kind}（id={id}）"
        );
        assert!(seen.insert((kind, id)), "清单重复条目：{kind} {id}");
    }
    assert!(!E2E_MANIFEST.is_empty(), "清单不得为空");
}

/// 收尾断言：声明集合与执行集合必须**双向相等**。
fn assert_coverage_complete() {
    let declared: BTreeSet<(&str, &str)> = E2E_MANIFEST.iter().copied().collect();
    let executed = cover::executed();

    let missing: Vec<&(&str, &str)> = declared.difference(&executed).collect();
    let ghost: Vec<&(&str, &str)> = executed.difference(&declared).collect();

    assert!(
        missing.is_empty(),
        "以下 {} 条公开条目被声明却未执行：{missing:?}",
        missing.len()
    );
    assert!(
        ghost.is_empty(),
        "以下 {} 条执行未登记在清单：{ghost:?}",
        ghost.len()
    );
    eprintln!(
        "E2E 覆盖：{}/{} 条公开条目全部执行（taosx）",
        executed.len(),
        declared.len()
    );
}

/// 进程内唯一的资源名：`<前缀>_<pid>_<纳秒>`。
fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("系统时钟应晚于 UNIX_EPOCH")
        .as_nanos();
    format!("{prefix}_{}_{}", std::process::id(), nanos)
}

/// 唯一化 tag 值（≤48 UTF-8 字节，十六进制编码后进入子表名）。
fn unique_tag() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("系统时钟应晚于 UNIX_EPOCH")
        .as_nanos();
    format!("s{}x{}", std::process::id(), nanos % 100_000_000)
}

/// 秒对齐的纳秒时间戳：对 ms / us / ns 三种库精度都无损。
fn aligned_now_ns() -> i64 {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("系统时钟应晚于 UNIX_EPOCH")
        .as_secs();
    i64::try_from(seconds).expect("时间戳应在 i64 范围内") * 1_000_000_000
}

/// 临时目录（收尾删除并断言删除生效）。
fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(unique_name(prefix));
    std::fs::create_dir_all(&dir).expect("创建临时目录必须成功");
    dir
}

/// 本次运行创建的资源登记表：异常安全清理的唯一依据。
///
/// 名字在**建表之前**登记——`DROP ... IF EXISTS` 是幂等的，因此即便建表只完成一半，
/// 收尾也能清干净。登记表只是资源台账，不参与公开面覆盖。
struct ResourceRegistry {
    stables: Mutex<Vec<String>>,
}

impl ResourceRegistry {
    fn new() -> Self {
        Self {
            stables: Mutex::new(Vec::new()),
        }
    }

    /// 建表**之前**登记（幂等清理的前提）。
    fn register_stable(&self, stable: &str) {
        self.stables
            .lock()
            .expect("资源登记表锁中毒")
            .push(stable.to_owned());
    }

    fn names(&self) -> Vec<String> {
        self.stables.lock().expect("资源登记表锁中毒").clone()
    }
}

/// 故障注入（**仅测试自身**）：`TAOSX_E2E_FAULT=<stage>` 时在该阶段 panic。
///
/// 用途是**证明**异常安全清理真的成立：注入一次失败 → 用例必红 → 共享库中不得残留
/// `taosx_e2e_*` 对象。默认不设置该变量，故正常路径永不受影响。
fn maybe_inject_fault(stage: &str) {
    if std::env::var("TAOSX_E2E_FAULT").ok().as_deref() == Some(stage) {
        panic!("注入的故障（TAOSX_E2E_FAULT={stage}）：用于验证中途 panic 也会清理建出的对象");
    }
}

/// 阶段 1：30 个公开常量逐条取值断言。
fn phase_constants() {
    // 20 个环境变量常量：必须带统一前缀且两两不同（否则配置面会互相踩键）。
    let env_consts: [(&'static str, &str); 20] = [
        ("ENV_PREFIX", ENV_PREFIX),
        ("ENV_HOST", ENV_HOST),
        ("ENV_PORT", ENV_PORT),
        ("ENV_DATABASE", ENV_DATABASE),
        ("ENV_USER", ENV_USER),
        ("ENV_PASSWORD", ENV_PASSWORD),
        ("ENV_TLS", ENV_TLS),
        ("ENV_TLS_CA_FILE", ENV_TLS_CA_FILE),
        ("ENV_TIMEOUT_MS", ENV_TIMEOUT_MS),
        ("ENV_PRECISION", ENV_PRECISION),
        ("ENV_TRANSPORT", ENV_TRANSPORT),
        ("ENV_MAX_IN_FLIGHT", ENV_MAX_IN_FLIGHT),
        ("ENV_ACQUIRE_TIMEOUT_MS", ENV_ACQUIRE_TIMEOUT_MS),
        ("ENV_BATCH_MAX_ROWS", ENV_BATCH_MAX_ROWS),
        ("ENV_BATCH_MAX_BYTES", ENV_BATCH_MAX_BYTES),
        ("ENV_MAX_RESPONSE_BYTES", ENV_MAX_RESPONSE_BYTES),
        ("ENV_MAX_QUERY_ROWS", ENV_MAX_QUERY_ROWS),
        ("ENV_CLOSE_TIMEOUT_MS", ENV_CLOSE_TIMEOUT_MS),
        ("ENV_HOSTS", ENV_HOSTS),
        ("ENV_WRITE_MAX_ATTEMPTS", ENV_WRITE_MAX_ATTEMPTS),
    ];
    let mut seen = BTreeSet::new();
    for (id, value) in env_consts {
        hit("const", id);
        assert!(
            value.starts_with(ENV_PREFIX),
            "{id} 必须带前缀 {ENV_PREFIX}，实际 {value}"
        );
        assert!(seen.insert(value), "{id} 与其它常量重复：{value}");
    }
    assert_eq!(ENV_PREFIX, "FOUNDATIONX_TAOSX_");

    hit("const", "DEFAULT_HOST");
    assert_eq!(DEFAULT_HOST, "127.0.0.1");
    hit("const", "DEFAULT_PORT");
    assert_eq!(DEFAULT_PORT, 6041);
    hit("const", "DEFAULT_DATABASE");
    assert_eq!(DEFAULT_DATABASE, "infra_draft");
    hit("const", "DEFAULT_USER");
    assert_eq!(DEFAULT_USER, "root");

    // 6 个硬上限：用「默认配置必须落在上限内」的关系断言，避免对常量做恒真断言，
    // 同时把上限与实现默认值绑定（改上限而忘了改默认值会在编译期之外被发现）。
    let default = TaosConfig::default();
    hit("const", "HARD_MAX_IN_FLIGHT");
    assert!(default.max_in_flight <= HARD_MAX_IN_FLIGHT);
    hit("const", "HARD_MAX_BATCH_ROWS");
    assert!(default.batch_max_rows <= HARD_MAX_BATCH_ROWS);
    hit("const", "HARD_MAX_BATCH_BYTES");
    assert!(default.batch_max_bytes <= HARD_MAX_BATCH_BYTES);
    hit("const", "HARD_MAX_RESPONSE_BYTES");
    assert!(default.max_response_bytes <= HARD_MAX_RESPONSE_BYTES);
    hit("const", "HARD_MAX_QUERY_ROWS");
    assert!(default.max_query_rows <= HARD_MAX_QUERY_ROWS);
    hit("const", "HARD_MAX_CLOSE_TIMEOUT");
    assert_eq!(HARD_MAX_CLOSE_TIMEOUT, Duration::from_secs(30));
    assert!(default.close_timeout <= HARD_MAX_CLOSE_TIMEOUT);
    // 8 个硬上限（P2 批次补了后两个）：除「默认值在上限内」，再各走一次**真实拒绝路径**
    // ——常量本身被断言取值，其**校验作用**也必须在公开入口（TOML）上被观测到。
    hit("const", "HARD_MAX_TIMEOUT");
    assert_eq!(HARD_MAX_TIMEOUT, Duration::from_secs(3_600));
    assert!(default.timeout <= HARD_MAX_TIMEOUT);
    assert!(default.acquire_timeout <= HARD_MAX_TIMEOUT);
    hit("const", "HARD_MAX_WRITE_MAX_ATTEMPTS");
    assert_eq!(HARD_MAX_WRITE_MAX_ATTEMPTS, 10);
    assert!(default.write_max_attempts <= HARD_MAX_WRITE_MAX_ATTEMPTS);
    let over_timeout = TaosConfig::from_toml(&format!(
        "schema_version = 1\ntimeout_ms = {}\n",
        HARD_MAX_TIMEOUT.as_millis() + 1
    ))
    .expect_err("超过 HARD_MAX_TIMEOUT 必须 fail-closed");
    assert!(
        over_timeout.to_string().contains("timeout"),
        "{over_timeout}"
    );
    let over_attempts = TaosConfig::from_toml(&format!(
        "schema_version = 1\nwrite_max_attempts = {}\n",
        HARD_MAX_WRITE_MAX_ATTEMPTS + 1
    ))
    .expect_err("超过 HARD_MAX_WRITE_MAX_ATTEMPTS 必须 fail-closed");
    assert!(
        over_attempts.to_string().contains("write_max_attempts"),
        "{over_attempts}"
    );
}

/// 阶段 2：不依赖真实服务的值类型（错误分类、精度/传输枚举、重试策略、报告结构、DTO）。
fn phase_value_types() {
    // —— TaosError：10 个变体逐个构造，并逐条核对分类与错误码语义 ——
    hit("type", "TaosError");
    let variants: [(&'static str, TaosError, bool); 10] = [
        (
            "TaosError::Backend",
            TaosError::Backend {
                code: 0,
                message: "e2e".into(),
            },
            false,
        ),
        ("TaosError::Closed", TaosError::Closed("e2e".into()), false),
        ("TaosError::Config", TaosError::Config("e2e".into()), false),
        (
            "TaosError::Connection",
            TaosError::Connection("e2e".into()),
            true,
        ),
        (
            "TaosError::Invalid",
            TaosError::Invalid("e2e".into()),
            false,
        ),
        (
            "TaosError::Io",
            TaosError::Io(std::io::Error::other("e2e")),
            true,
        ),
        (
            "TaosError::Serialization",
            TaosError::Serialization("e2e".into()),
            false,
        ),
        ("TaosError::Timeout", TaosError::Timeout("e2e".into()), true),
        (
            "TaosError::Unavailable",
            TaosError::Unavailable("e2e".into()),
            true,
        ),
        (
            "TaosError::Unsupported",
            TaosError::Unsupported("e2e".into()),
            false,
        ),
    ];
    for (id, error, retryable) in variants {
        hit("variant", id);
        // `TaosError` 是 `#[non_exhaustive]`：只能按「单变体 + 兜底」判定，不能穷尽 match。
        let is_backend = matches!(&error, TaosError::Backend { .. });
        let is_io = matches!(&error, TaosError::Io(_));
        hit("fn", "TaosError::is_retryable");
        assert_eq!(
            error.is_retryable(),
            retryable,
            "{id} 的可重试分类不符合契约"
        );
        hit("fn", "TaosError::taos_code");
        let code = error.taos_code();
        hit("fn", "TaosError::is_not_found");
        let not_found = error.is_not_found();
        if is_backend {
            assert_eq!(code, Some(0), "{id} 应携带 TDengine 错误码");
        } else {
            assert!(code.is_none(), "{id} 不应携带错误码");
            assert!(!not_found, "{id} 不得被判为「表不存在」");
        }
        hit("fn", "TaosError::with_message");
        let replaced = error.with_message("e2e-替换");
        assert_eq!(
            replaced.is_retryable(),
            retryable,
            "with_message 必须保留分类"
        );
        if !is_io {
            // `Io` 变体按契约保留底层 io 错误，不采用替换文案。
            assert!(
                replaced.to_string().contains("e2e-替换"),
                "{id} with_message 必须替换消息"
            );
        }
        assert!(!replaced.to_string().is_empty());
    }

    hit("fn", "TaosError::backend");
    assert_eq!(TaosError::backend("e2e").taos_code(), Some(0));

    hit("fn", "TaosError::from_taos_code");
    assert!(matches!(
        TaosError::from_taos_code(896, "繁忙"),
        TaosError::Unavailable(_)
    ));
    let not_exist = TaosError::from_taos_code(0x2603, "表不存在");
    assert!(not_exist.is_not_found());
    assert_eq!(not_exist.taos_code(), Some(0x2603));
    assert!(!not_exist.is_retryable());
    assert!(matches!(
        TaosError::from_taos_code(42, "语法错误"),
        TaosError::Invalid(_)
    ));
    assert!(matches!(
        TaosError::from_taos_code(-1, "内部错误"),
        TaosError::Backend { .. }
    ));

    hit("fn", "TaosError::from_http_status");
    assert!(matches!(
        TaosError::from_http_status(408, ""),
        TaosError::Timeout(_)
    ));
    assert!(matches!(
        TaosError::from_http_status(429, ""),
        TaosError::Unavailable(_)
    ));
    assert!(TaosError::from_http_status(503, "").is_retryable());
    assert!(matches!(
        TaosError::from_http_status(401, ""),
        TaosError::Backend { .. }
    ));

    // —— 精度与传输枚举：每个变体都构造，并核对 parse 接受集 ——
    hit("type", "TransportMode");
    hit("fn", "TransportMode::parse");
    hit("variant", "TransportMode::Rest");
    assert_eq!(TransportMode::parse("rest"), Some(TransportMode::Rest));
    assert_eq!(TransportMode::parse("HTTP"), Some(TransportMode::Rest));
    hit("variant", "TransportMode::NativeWs");
    assert_eq!(
        TransportMode::parse("native"),
        Some(TransportMode::NativeWs)
    );
    assert_eq!(TransportMode::parse("ws"), Some(TransportMode::NativeWs));
    assert_eq!(
        TransportMode::parse("native_wS"),
        Some(TransportMode::NativeWs)
    );
    assert_eq!(TransportMode::parse("bogus"), None);
    // `as_str()` 的输出必须能被 `parse` 接受（issue #16）：修复前 `NativeWs` 断裂
    // （`as_str()` 给 `nativews`，接受集里只有 `native`/`ws`/`native_ws`/`native-ws`），
    // 且 `nativews` 在 env 与 TOML **两条配置入口**上都会被 fail-closed 拒绝。
    // 现在两个变体都要往返成立。
    hit("fn", "TransportMode::as_str");
    assert_eq!(TransportMode::Rest.as_str(), "rest");
    assert_eq!(TransportMode::NativeWs.as_str(), "nativews");
    for mode in [TransportMode::Rest, TransportMode::NativeWs] {
        assert_eq!(
            TransportMode::parse(mode.as_str()),
            Some(mode),
            "{mode:?} 的 as_str 输出必须能被 parse 接受"
        );
    }

    hit("type", "TsPrecision");
    for (id, precision) in [
        ("TsPrecision::Ms", TsPrecision::Ms),
        ("TsPrecision::Us", TsPrecision::Us),
        ("TsPrecision::Ns", TsPrecision::Ns),
    ] {
        hit("variant", id);
        hit("fn", "TsPrecision::parse");
        hit("fn", "TsPrecision::as_str");
        assert_eq!(TsPrecision::parse(precision.as_str()), Some(precision));
    }
    assert_eq!(TsPrecision::parse("MS"), Some(TsPrecision::Ms));
    assert_eq!(TsPrecision::parse(" bogus "), None);
    // 纳秒 ↔ 库精度的换算必须是**无损往返**（未对齐由 build_insert_sql_chunks 拒绝）。
    hit("fn", "TsPrecision::from_nanos");
    hit("fn", "TsPrecision::to_nanos");
    assert_eq!(TsPrecision::Ms.from_nanos(1_500_000_000), 1500);
    assert_eq!(TsPrecision::Us.from_nanos(1_500_000), 1500);
    assert_eq!(TsPrecision::Ns.from_nanos(42), 42);
    assert_eq!(TsPrecision::Ms.to_nanos(1500), 1_500_000_000);
    assert_eq!(TsPrecision::Us.to_nanos(1500), 1_500_000);
    assert_eq!(TsPrecision::Ns.to_nanos(42), 42);

    // —— RetryPolicy：类型 + 5 字段 + 8 个方法 ——
    hit("type", "RetryPolicy");
    hit("fn", "RetryPolicy::for_read");
    let read = RetryPolicy::for_read();
    hit("fn", "RetryPolicy::for_idempotent_write");
    let idempotent_write = RetryPolicy::for_idempotent_write();
    hit("fn", "RetryPolicy::no_retry");
    let no_retry = RetryPolicy::no_retry();
    assert_eq!(read.max_attempts, 3);
    assert_eq!(idempotent_write.max_attempts, 3);
    assert_eq!(no_retry.max_attempts, 1);

    let probe = RetryPolicy {
        max_attempts: 8,
        initial_backoff: Duration::from_millis(50),
        max_backoff: Duration::from_millis(400),
        jitter_ratio: 0.0,
        deadline: None,
    };
    hit("fn", "RetryPolicy::compute_backoff");
    assert_eq!(probe.compute_backoff(0, 0.5), Duration::from_millis(50));
    assert_eq!(probe.compute_backoff(9, 0.5), Duration::from_millis(400));
    hit("fn", "RetryPolicy::exponential_backoff");
    assert_eq!(probe.exponential_backoff(1), Duration::from_millis(100));
    assert_eq!(probe.exponential_backoff(9), Duration::from_millis(400));
    hit("fn", "RetryPolicy::backoff_for_attempt");
    assert!(probe.backoff_for_attempt(0) <= probe.max_backoff);
    hit("fn", "RetryPolicy::is_retryable");
    assert!(RetryPolicy::is_retryable(&TaosError::Connection(
        "x".into()
    )));
    assert!(!RetryPolicy::is_retryable(&TaosError::Config("x".into())));

    // 穷尽解构（不写 `..`）：新增公开字段会在此处编译失败，强制补齐覆盖。
    let RetryPolicy {
        max_attempts,
        initial_backoff,
        max_backoff,
        jitter_ratio,
        deadline,
    } = probe;
    hit("field", "RetryPolicy::max_attempts");
    assert_eq!(max_attempts, 8);
    hit("field", "RetryPolicy::initial_backoff");
    assert_eq!(initial_backoff, Duration::from_millis(50));
    hit("field", "RetryPolicy::max_backoff");
    assert_eq!(max_backoff, Duration::from_millis(400));
    hit("field", "RetryPolicy::jitter_ratio");
    assert_eq!(jitter_ratio, 0.0);
    hit("field", "RetryPolicy::deadline");
    assert!(deadline.is_none());

    // —— BatchWriteReport：类型 + 4 字段 + is_complete ——
    hit("type", "BatchWriteReport");
    let complete = BatchWriteReport {
        accepted: 3,
        failed: 0,
        chunks_ok: 2,
        chunks_total: 2,
    };
    hit("fn", "BatchWriteReport::is_complete");
    assert!(complete.is_complete());
    assert!(!BatchWriteReport {
        accepted: 1,
        failed: 1,
        chunks_ok: 1,
        chunks_total: 2,
    }
    .is_complete());
    let BatchWriteReport {
        accepted,
        failed,
        chunks_ok,
        chunks_total,
    } = complete;
    hit("field", "BatchWriteReport::accepted");
    assert_eq!(accepted, 3);
    hit("field", "BatchWriteReport::failed");
    assert_eq!(failed, 0);
    hit("field", "BatchWriteReport::chunks_ok");
    assert_eq!(chunks_ok, 2);
    hit("field", "BatchWriteReport::chunks_total");
    assert_eq!(chunks_total, 2);

    // —— BatchWritePartialError：类型 + 2 字段（部分成功语义） ——
    hit("type", "BatchWritePartialError");
    let partial = BatchWritePartialError {
        report: complete,
        source: TaosError::backend("e2e"),
    };
    let BatchWritePartialError {
        report: partial_report,
        source,
    } = partial;
    hit("field", "BatchWritePartialError::report");
    assert_eq!(partial_report.accepted, 3);
    hit("field", "BatchWritePartialError::source");
    assert!(!source.is_retryable());
    // From<BatchWritePartialError> for TaosError：文案必须带 accepted/failed。
    let mapped: TaosError = BatchWritePartialError {
        report: partial_report,
        source: TaosError::Invalid("e2e".into()),
    }
    .into();
    assert!(mapped.to_string().contains("accepted=3"));
    assert!(mapped.to_string().contains("failed=0"));

    // —— BatcherCloseReport：类型 + 4 字段 ——
    hit("type", "BatcherCloseReport");
    let close_summary = BatcherCloseReport {
        total_accepted: 5,
        total_failed: 1,
        pending: 1,
        last_flush: complete,
    };
    let BatcherCloseReport {
        total_accepted,
        total_failed,
        pending,
        last_flush,
    } = close_summary;
    hit("field", "BatcherCloseReport::total_accepted");
    assert_eq!(total_accepted, 5);
    hit("field", "BatcherCloseReport::total_failed");
    assert_eq!(total_failed, 1);
    hit("field", "BatcherCloseReport::pending");
    assert_eq!(pending, 1);
    hit("field", "BatcherCloseReport::last_flush");
    assert_eq!(last_flush.accepted, 3);

    // —— BatcherCloseError：类型 + 2 字段 ——
    hit("type", "BatcherCloseError");
    let close_error = BatcherCloseError {
        summary: close_summary,
        source: TaosError::Unavailable("e2e".into()),
    };
    let BatcherCloseError { summary, source } = close_error;
    hit("field", "BatcherCloseError::summary");
    assert_eq!(summary.pending, 1);
    hit("field", "BatcherCloseError::source");
    assert!(source.is_retryable());
    // 摘要必须能与根因一起定位（Debug 输出含 pending 与 accepted 两类关键字段）。
    let rendered = format!("{summary:?}");
    assert!(rendered.contains("total_accepted"), "实际 {rendered}");
    assert!(rendered.contains("pending"), "实际 {rendered}");

    // —— WriteBatcherConfig：类型 + 3 字段 ——
    hit("type", "WriteBatcherConfig");
    let batcher_config = WriteBatcherConfig {
        max_rows: 7,
        max_bytes_hint: 1024,
        flush_interval: Duration::from_millis(50),
    };
    let WriteBatcherConfig {
        max_rows,
        max_bytes_hint,
        flush_interval,
    } = batcher_config;
    hit("field", "WriteBatcherConfig::max_rows");
    assert_eq!(max_rows, 7);
    hit("field", "WriteBatcherConfig::max_bytes_hint");
    assert_eq!(max_bytes_hint, 1024);
    hit("field", "WriteBatcherConfig::flush_interval");
    assert_eq!(flush_interval, Duration::from_millis(50));
    assert!(WriteBatcherConfig::default().max_rows >= 1);
    assert!(!WriteBatcherConfig::default().flush_interval.is_zero());

    // —— TaosPoint：类型 + 3 字段 + new ——
    hit("type", "TaosPoint");
    hit("fn", "TaosPoint::new");
    let point = TaosPoint::new("tag", 1_000_000_000, "1.0", "1.1");
    let TaosPoint {
        timestamp_ns,
        tag_value,
        values,
    } = point;
    hit("field", "TaosPoint::timestamp_ns");
    assert_eq!(timestamp_ns, 1_000_000_000);
    hit("field", "TaosPoint::tag_value");
    assert_eq!(tag_value, "tag");
    hit("field", "TaosPoint::values");
    assert_eq!(values, ["1.0".to_owned(), "1.1".to_owned()]);

    // —— TaosExecResult：类型 + 4 字段 ——
    hit("type", "TaosExecResult");
    let exec_result = TaosExecResult {
        code: 0,
        rows: vec![vec!["a".to_owned()]],
        columns: vec!["c".to_owned()],
        affected_rows: Some(1),
    };
    let TaosExecResult {
        code,
        rows,
        columns,
        affected_rows,
    } = exec_result;
    hit("field", "TaosExecResult::code");
    assert_eq!(code, 0);
    hit("field", "TaosExecResult::rows");
    assert_eq!(rows.len(), 1);
    hit("field", "TaosExecResult::columns");
    assert_eq!(columns, vec!["c".to_owned()]);
    hit("field", "TaosExecResult::affected_rows");
    assert_eq!(affected_rows, Some(1));

    // —— TaosMetricsSnapshot：类型 + 14 字段 + 2 方法 ——
    hit("type", "TaosMetricsSnapshot");
    let snapshot = TaosMetricsSnapshot {
        sql_ok: 1,
        sql_err: 0,
        sql_bytes: 10,
        response_bytes: 20,
        write_ok: 1,
        write_err: 0,
        query_ok: 1,
        query_err: 0,
        ping_ok: 1,
        ping_err: 0,
        health_ready: 1,
        health_not_ready: 0,
        ws_probe_ok: 1,
        ws_probe_err: 0,
    };
    let TaosMetricsSnapshot {
        sql_ok,
        sql_err,
        sql_bytes,
        response_bytes,
        write_ok,
        write_err,
        query_ok,
        query_err,
        ping_ok,
        ping_err,
        health_ready,
        health_not_ready,
        ws_probe_ok,
        ws_probe_err,
    } = snapshot;
    hit("field", "TaosMetricsSnapshot::sql_ok");
    assert_eq!(sql_ok, 1);
    hit("field", "TaosMetricsSnapshot::sql_err");
    assert_eq!(sql_err, 0);
    hit("field", "TaosMetricsSnapshot::sql_bytes");
    assert_eq!(sql_bytes, 10);
    hit("field", "TaosMetricsSnapshot::response_bytes");
    assert_eq!(response_bytes, 20);
    hit("field", "TaosMetricsSnapshot::write_ok");
    assert_eq!(write_ok, 1);
    hit("field", "TaosMetricsSnapshot::write_err");
    assert_eq!(write_err, 0);
    hit("field", "TaosMetricsSnapshot::query_ok");
    assert_eq!(query_ok, 1);
    hit("field", "TaosMetricsSnapshot::query_err");
    assert_eq!(query_err, 0);
    hit("field", "TaosMetricsSnapshot::ping_ok");
    assert_eq!(ping_ok, 1);
    hit("field", "TaosMetricsSnapshot::ping_err");
    assert_eq!(ping_err, 0);
    hit("field", "TaosMetricsSnapshot::health_ready");
    assert_eq!(health_ready, 1);
    hit("field", "TaosMetricsSnapshot::health_not_ready");
    assert_eq!(health_not_ready, 0);
    hit("field", "TaosMetricsSnapshot::ws_probe_ok");
    assert_eq!(ws_probe_ok, 1);
    hit("field", "TaosMetricsSnapshot::ws_probe_err");
    assert_eq!(ws_probe_err, 0);
    hit("fn", "TaosMetricsSnapshot::total_events");
    assert_eq!(snapshot.total_events(), 6);
    hit("fn", "TaosMetricsSnapshot::to_prometheus_text");
    let text = snapshot.to_prometheus_text();
    assert!(text.contains("taosx_ops_total"));
    assert!(text.contains("taosx_bytes_total{op=\"request\"} 10"));
    assert!(text.contains("taosx_bytes_total{op=\"response\"} 20"));

    // —— TaosResult 别名（成功 + 失败各一次） ——
    fn as_result(value: u8) -> TaosResult<u8> {
        Ok(value)
    }
    hit("type", "TaosResult");
    assert_eq!(as_result(7).expect("Ok 分支"), 7);
    let failed: TaosResult<u8> = Err(TaosError::Config("e2e".into()));
    assert!(failed.is_err());

    // —— build_insert_sql_chunks：分块、转义、精度 fail-closed ——
    hit("fn", "build_insert_sql_chunks");
    let points = vec![
        TaosPoint::new("BTC/USDT", 1_000_000_000, "1'0", "2\\0"),
        TaosPoint::new("ETH/USDT", 2_000_000_000, "3.0", "4.0"),
    ];
    let chunks =
        build_insert_sql_chunks("ticks", &points, TsPrecision::Ns, 1).expect("分块必须成功");
    assert_eq!(chunks.len(), 2, "每批 1 行");
    assert!(chunks[0].starts_with("INSERT INTO "));
    assert!(
        chunks[0].contains(r"'1\'0'"),
        "单引号必须转义: {}",
        chunks[0]
    );
    assert!(chunks[0].contains(r"TAGS ('BTC/USDT')"));
    assert!(build_insert_sql_chunks("ticks", &[], TsPrecision::Ns, 1)
        .expect("空输入")
        .is_empty());
    // 未对齐到目标精度的时间戳必须 fail-closed，不静默截断。
    let unaligned = vec![TaosPoint::new("BTC", 1_500, "1.0", "1.1")];
    assert!(build_insert_sql_chunks("ticks", &unaligned, TsPrecision::Ms, 1).is_err());
    assert!(build_insert_sql_chunks("ticks", &points, TsPrecision::Ns, 0).is_err());
    assert!(build_insert_sql_chunks("bad name", &points, TsPrecision::Ns, 1).is_err());
}

/// 阶段 3：需要 await 的纯本地路径（重试策略真实执行、查询流离线消费）。
async fn phase_retry_and_streams() {
    // RetryPolicy::run：不可重试立即返回，可重试在预算内退避后成功。
    hit("fn", "RetryPolicy::run");
    let calls = AtomicU32::new(0);
    let policy = RetryPolicy {
        max_attempts: 3,
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(2),
        jitter_ratio: 0.0,
        deadline: None,
    };
    let value = policy
        .run(|| async {
            let attempt = calls.fetch_add(1, Ordering::SeqCst);
            if attempt < 1 {
                Err(TaosError::Unavailable("temp".into()))
            } else {
                Ok(9u8)
            }
        })
        .await
        .expect("第 2 次必须成功");
    assert_eq!(value, 9);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let permanent = RetryPolicy::for_read()
        .run(|| async { Err::<(), _>(TaosError::Invalid("bad".into())) })
        .await
        .expect_err("不可重试错误必须原样返回");
    assert!(matches!(permanent, TaosError::Invalid(_)));

    // TaosQueryStream：离线构造与消费（真实服务侧的流在数据面阶段跑）。
    hit("type", "TaosQueryStream");
    hit("fn", "TaosQueryStream::from_rows");
    let rows = vec![
        TaosPoint::new("A", 1, "0.1", "0.2"),
        TaosPoint::new("B", 2, "0.3", "0.4"),
    ];
    let mut stream = TaosQueryStream::from_rows(rows.clone());
    hit("fn", "TaosQueryStream::remaining_hint");
    assert_eq!(stream.remaining_hint(), 2);
    hit("fn", "TaosQueryStream::chunk_hint");
    assert_eq!(stream.chunk_hint(), 1);
    let first = stream.next().await.expect("首行").expect("Ok");
    assert_eq!(first.tag_value, "A");
    let second = stream.next().await.expect("次行").expect("Ok");
    assert_eq!(second.tag_value, "B");
    assert!(stream.next().await.is_none());
    assert!(stream.next().await.is_none(), "结束后必须保持 Ready(None)");
    hit("fn", "TaosQueryStream::from_rows_chunked");
    let chunked = TaosQueryStream::from_rows_chunked(rows, 32).expect("合法提示");
    assert_eq!(chunked.chunk_hint(), 32);
    assert!(TaosQueryStream::from_rows_chunked(Vec::new(), 0).is_err());
}

/// 显式双传输配置 + env 事实。
struct E2EConfigs {
    /// 由 `FOUNDATIONX_TAOSX_*` 直接读出的配置。
    env: TaosConfig,
    /// 显式原生 WebSocket 配置（`/rest/ws`，taosAdapter 端口）。
    native: TaosConfig,
    /// 显式 REST 配置（`/rest/sql`，taosAdapter 端口）。
    rest: TaosConfig,
    /// 原生 TCP 端口（仅用于 `probe_native_tcp` 探活）。
    native_tcp_port: u16,
}

/// 阶段 4：配置面（`from_env` 读真实注入值、TOML、构建器、双传输显式配置）。
fn phase_config_plane() -> E2EConfigs {
    // —— from_env：真实环境变量（必须在 source 过 taosx.env 的前提下调用） ——
    let env_config = TaosConfig::from_env()
        .expect("必须能读取 FOUNDATIONX_TAOSX_*：请先 source taosx.env 或 taosx-rest.env");
    hit("fn", "TaosConfig::from_env");
    hit("type", "TaosConfig");
    assert!(!env_config.host.is_empty(), "env 主机必须被读到");
    assert_ne!(env_config.port, 0, "env 端口必须被读到");
    assert!(!env_config.database.is_empty(), "env 库名必须被读到");
    assert!(!env_config.user.is_empty(), "env 用户必须被读到");
    assert!(!env_config.password.is_empty(), "env 密码必须被读到");

    // 19 个公开字段穷尽解构（新增字段会编译失败）。
    let TaosConfig {
        host,
        port,
        database,
        user,
        password,
        tls,
        tls_ca_file,
        timeout,
        precision,
        transport,
        max_in_flight,
        acquire_timeout,
        batch_max_rows,
        batch_max_bytes,
        max_response_bytes,
        max_query_rows,
        close_timeout,
        hosts,
        write_max_attempts,
    } = env_config.clone();
    hit("field", "TaosConfig::host");
    assert!(!host.is_empty());
    hit("field", "TaosConfig::port");
    assert_ne!(port, 0);
    hit("field", "TaosConfig::database");
    assert!(!database.is_empty());
    hit("field", "TaosConfig::user");
    assert!(!user.is_empty());
    hit("field", "TaosConfig::password");
    assert!(!password.is_empty());
    hit("field", "TaosConfig::tls");
    assert!(tls_ca_file.is_none() || tls, "配置 CA 时必须启用 TLS");
    hit("field", "TaosConfig::tls_ca_file");
    assert!(tls || tls_ca_file.is_none());
    hit("field", "TaosConfig::timeout");
    assert!(!timeout.is_zero());
    hit("field", "TaosConfig::precision");
    assert!(
        precision.is_none()
            || matches!(
                precision,
                Some(TsPrecision::Ms | TsPrecision::Us | TsPrecision::Ns)
            )
    );
    hit("field", "TaosConfig::transport");
    match transport {
        TransportMode::Rest | TransportMode::NativeWs => {}
    }
    hit("field", "TaosConfig::max_in_flight");
    assert!((1..=HARD_MAX_IN_FLIGHT).contains(&max_in_flight));
    hit("field", "TaosConfig::acquire_timeout");
    assert!(!acquire_timeout.is_zero());
    hit("field", "TaosConfig::batch_max_rows");
    assert!((1..=HARD_MAX_BATCH_ROWS).contains(&batch_max_rows));
    hit("field", "TaosConfig::batch_max_bytes");
    assert!((1..=HARD_MAX_BATCH_BYTES).contains(&batch_max_bytes));
    hit("field", "TaosConfig::max_response_bytes");
    assert!((1..=HARD_MAX_RESPONSE_BYTES).contains(&max_response_bytes));
    hit("field", "TaosConfig::max_query_rows");
    assert!((1..=HARD_MAX_QUERY_ROWS).contains(&max_query_rows));
    hit("field", "TaosConfig::close_timeout");
    assert!(!close_timeout.is_zero() && close_timeout <= HARD_MAX_CLOSE_TIMEOUT);
    hit("field", "TaosConfig::hosts");
    assert!(hosts.len() <= 1024);
    hit("field", "TaosConfig::write_max_attempts");
    assert!(write_max_attempts >= 1);

    hit("fn", "TaosConfig::validate");
    env_config.validate().expect("env 配置必须合法");

    // —— 端点构造（纯函数） ——
    let scheme = if env_config.tls { "https" } else { "http" };
    hit("fn", "TaosConfig::rest_sql_url");
    let rest_url = env_config.rest_sql_url();
    assert_eq!(
        rest_url,
        format!(
            "{scheme}://{}:{}/rest/sql",
            env_config.host, env_config.port
        )
    );
    hit("fn", "TaosConfig::rest_sql_url_for");
    assert_eq!(
        env_config.rest_sql_url_for("other.example"),
        format!("{scheme}://other.example:{}/rest/sql", env_config.port)
    );
    hit("fn", "TaosConfig::rest_sql_db_url");
    assert!(env_config
        .rest_sql_db_url()
        .ends_with(&format!("/{}", env_config.database)));
    hit("fn", "TaosConfig::rest_sql_endpoint");
    let rest_endpoint = env_config.rest_sql_endpoint().expect("REST 端点必须合法");
    assert_eq!(rest_endpoint.as_str(), rest_url);
    let ws_scheme = if env_config.tls { "wss" } else { "ws" };
    hit("fn", "TaosConfig::native_ws_url");
    let ws_url = env_config.native_ws_url();
    assert_eq!(
        ws_url,
        format!(
            "{ws_scheme}://{}:{}/rest/ws",
            env_config.host, env_config.port
        )
    );
    hit("fn", "TaosConfig::native_ws_endpoint");
    assert_eq!(
        env_config
            .native_ws_endpoint()
            .expect("WS 端点必须合法")
            .as_str(),
        ws_url
    );
    hit("fn", "TaosConfig::endpoint_hosts");
    assert_eq!(
        env_config.endpoint_hosts().first().map(String::as_str),
        Some(env_config.host.as_str())
    );

    // —— from_toml / from_toml_file ——
    let toml_text = "schema_version = 1\nhost = \"127.0.0.1\"\nport = 6041\ndatabase = \"infra_draft\"\ntimeout_ms = 2500\nprecision = \"ns\"\ntransport = \"rest\"\n";
    hit("fn", "TaosConfig::from_toml");
    let from_toml = TaosConfig::from_toml(toml_text).expect("合法 TOML 必须可解析");
    assert_eq!(from_toml.port, 6041);
    assert_eq!(from_toml.timeout, Duration::from_millis(2500));
    assert_eq!(from_toml.precision, Some(TsPrecision::Ns));
    assert!(from_toml.password.is_empty());
    // 拒绝路径必须**带上 schema_version**，否则会因「缺 schema_version」而假通过。
    assert!(
        TaosConfig::from_toml("schema_version = 1\npassword = \"hunter2\"\n").is_err(),
        "from_toml 必须拒绝非空明文密码"
    );
    assert!(
        TaosConfig::from_toml("schema_version = 1\nsink_id = \"x\"\n").is_err(),
        "from_toml 必须拒绝未知字段（fail-closed）"
    );
    assert!(TaosConfig::from_toml("schema_version = 2\n").is_err());
    assert!(TaosConfig::from_toml("host = \"127.0.0.1\"\n").is_err());

    let dir = unique_temp_dir("taosx_e2e_toml");
    let toml_path = dir.join("config.toml");
    std::fs::write(&toml_path, toml_text).expect("写 TOML 必须成功");
    hit("fn", "TaosConfig::from_toml_file");
    let from_file = TaosConfig::from_toml_file(&toml_path).expect("TOML 文件必须可解析");
    assert_eq!(from_file.database, "infra_draft");
    assert!(TaosConfig::from_toml_file(dir.join("missing.toml")).is_err());

    // —— 构建器：22 个公开方法 ——
    hit("type", "TaosConfigBuilder");
    hit("fn", "TaosConfigBuilder::new");
    let defaults = TaosConfigBuilder::new().build().expect("默认值必须合法");
    assert_eq!(defaults.host, DEFAULT_HOST);
    hit("fn", "TaosConfigBuilder::from_config");
    let seeded = TaosConfigBuilder::from_config(env_config.clone())
        .build()
        .expect("从既有配置出发必须合法");
    assert_eq!(seeded.port, env_config.port);

    hit("fn", "TaosConfig::builder");
    let ca = dir.join("ca.pem");
    let built = TaosConfig::builder()
        .host("127.0.0.1")
        .port(6041)
        .database("infra_draft")
        .user("writer")
        .password("e2e-secret")
        .tls(true)
        .tls_ca_file(ca.clone())
        .timeout(Duration::from_secs(7))
        .precision(TsPrecision::Ns)
        .transport(TransportMode::Rest)
        .max_in_flight(8)
        .acquire_timeout(Duration::from_secs(2))
        .batch_max_rows(10)
        .batch_max_bytes(4096)
        .max_response_bytes(1 << 20)
        .max_query_rows(100)
        .close_timeout(Duration::from_secs(3))
        .hosts(["127.0.0.2"])
        .write_max_attempts(0)
        .build()
        .expect("构建器产出的配置必须合法（路径存在性不参与校验）");
    for method in [
        "TaosConfigBuilder::new",
        "TaosConfigBuilder::from_config",
        "TaosConfigBuilder::host",
        "TaosConfigBuilder::port",
        "TaosConfigBuilder::database",
        "TaosConfigBuilder::user",
        "TaosConfigBuilder::password",
        "TaosConfigBuilder::tls",
        "TaosConfigBuilder::tls_ca_file",
        "TaosConfigBuilder::timeout",
        "TaosConfigBuilder::precision",
        "TaosConfigBuilder::transport",
        "TaosConfigBuilder::max_in_flight",
        "TaosConfigBuilder::acquire_timeout",
        "TaosConfigBuilder::batch_max_rows",
        "TaosConfigBuilder::batch_max_bytes",
        "TaosConfigBuilder::max_response_bytes",
        "TaosConfigBuilder::max_query_rows",
        "TaosConfigBuilder::close_timeout",
        "TaosConfigBuilder::hosts",
        "TaosConfigBuilder::write_max_attempts",
        "TaosConfigBuilder::build",
    ] {
        hit("fn", method);
    }
    assert_eq!(built.max_in_flight, 8);
    assert_eq!(built.write_max_attempts, 1, "0 应被夹到 1");
    assert_eq!(built.hosts, vec!["127.0.0.2".to_owned()]);
    assert_eq!(built.precision, Some(TsPrecision::Ns));
    assert!(built.tls);
    assert_eq!(built.tls_ca_file.as_deref(), Some(ca.as_path()));
    assert!(
        !format!("{built:?}").contains("e2e-secret"),
        "Debug 不得回显密码"
    );
    assert!(TaosConfigBuilder::new().host("").build().is_err());

    // —— 显式双传输配置 ——
    // REST/WS 端点同由 taosAdapter 提供，共用端口：优先 FOUNDATIONX_TAOSX_REST_PORT，否则 6041。
    let adapter_port: u16 = std::env::var("FOUNDATIONX_TAOSX_REST_PORT")
        .ok()
        .and_then(|value| value.trim().parse::<u16>().ok())
        .filter(|value| *value != 0)
        .unwrap_or(6041);
    // env 的 PORT 是原生 TCP 端口（6030）；若它恰好等于 adapter 端口则回退到 6030。
    let env_port: u16 = std::env::var(ENV_PORT)
        .ok()
        .and_then(|value| value.trim().parse::<u16>().ok())
        .unwrap_or(DEFAULT_PORT);
    let native_tcp_port = if env_port == adapter_port {
        6030
    } else {
        env_port
    };

    let mut native = env_config.clone();
    native.transport = TransportMode::NativeWs;
    native.port = adapter_port;
    native.timeout = Duration::from_secs(15);
    let mut rest = env_config.clone();
    rest.transport = TransportMode::Rest;
    rest.port = adapter_port;
    rest.timeout = Duration::from_secs(15);
    native.validate().expect("显式 NativeWs 配置必须合法");
    rest.validate().expect("显式 REST 配置必须合法");

    hit("fn", "build_native_ws_url");
    assert_eq!(build_native_ws_url(&native), native.native_ws_url());
    hit("fn", "validate_mode");
    validate_mode(&native).expect("NativeWs 模式必须通过");
    validate_mode(&rest).expect("Rest 模式必须通过");
    let invalid = TaosConfig {
        max_in_flight: 0,
        ..native.clone()
    };
    assert!(validate_mode(&invalid).is_err(), "非法配置必须被拒绝");

    std::fs::remove_dir_all(&dir).expect("清理临时目录必须成功");
    assert!(!dir.exists(), "清理后临时目录不得残留");

    E2EConfigs {
        env: env_config,
        native,
        rest,
        native_tcp_port,
    }
}

/// 阶段 5：离线池（不建连，只校验配置与本地状态机）。
async fn phase_offline_pool(env_config: &TaosConfig) {
    hit("type", "TaosPool");
    hit("fn", "TaosPool::new");
    let offline =
        TaosPool::new(env_config.clone()).expect("离线构造必须成功（仅校验配置，不建立网络连接）");
    hit("fn", "TaosPool::config");
    assert_eq!(offline.config().host, env_config.host);
    hit("fn", "TaosPool::precision");
    assert_eq!(
        offline.precision(),
        TsPrecision::Ms,
        "未连接时精度回退为 Ms"
    );
    hit("fn", "TaosPool::stats");
    hit("type", "TaosPoolStats");
    let TaosPoolStats { in_flight, closed } = offline.stats();
    hit("field", "TaosPoolStats::in_flight");
    assert_eq!(in_flight, 0, "离线池不得有在途请求");
    hit("field", "TaosPoolStats::closed");
    assert!(!closed);
    hit("fn", "TaosPool::is_closed");
    assert!(!offline.is_closed());
    hit("fn", "TaosPool::liveness");
    assert!(offline.liveness());
    hit("fn", "TaosPool::metrics");
    assert_eq!(offline.metrics().sql_ok, 0);
    hit("fn", "TaosPool::metrics_prometheus");
    assert!(offline.metrics_prometheus().contains("taosx_ops_total"));
    hit("fn", "TaosPool::client");
    let alias: TaosClient = offline.client();
    hit("type", "TaosClient");
    assert!(!alias.is_closed(), "别名与池共享同一状态");
    // fail-closed：非法配置必须在构造处被拒绝。
    assert!(TaosPool::new(TaosConfig {
        max_in_flight: 0,
        ..env_config.clone()
    })
    .is_err());
    hit("fn", "TaosPool::close");
    offline.close().await.expect("离线池 close 必须成功");
    assert!(offline.is_closed());
}

/// 删除超级表及其子表，并断言 `information_schema` 无残留。
///
/// 返回 `Err` 而**不** panic：这样收尾清理在 panic 路径上也能逐项收集错误，
/// 而不是自身再次炸掉、把原始失败原因盖住。
async fn drop_stable(pool: &TaosPool, stable: &str) -> TaosResult<()> {
    let children = pool
        .query(&format!(
            "SELECT table_name FROM information_schema.ins_tables WHERE stable_name='{stable}'"
        ))
        .await?;
    for row in &children.rows {
        if let Some(name) = row.first() {
            pool.exec(&format!("DROP TABLE IF EXISTS `{name}`")).await?;
        }
    }
    pool.exec(&format!("DROP STABLE IF EXISTS `{stable}`"))
        .await?;
    let remaining = pool
        .query(&format!(
            "SELECT count(*) FROM information_schema.ins_tables WHERE stable_name='{stable}'"
        ))
        .await?;
    let count = remaining
        .rows
        .first()
        .and_then(|row| row.first())
        .map(String::as_str);
    if count != Some("0") {
        return Err(TaosError::Unavailable(format!(
            "清理后仍有残留：stable={stable} count={count:?}"
        )));
    }
    Ok(())
}

/// 异常安全收尾：删除登记过的**所有**超级表并断言无残留。
///
/// 幂等、可在 panic 之后调用、不修改登记表。返回未被清干净的对象描述（空 = 已清干净）。
async fn cleanup_registry(configs: &E2EConfigs, registry: &ResourceRegistry) -> Result<(), String> {
    let names = registry.names();
    if names.is_empty() {
        return Ok(());
    }
    let pool = TaosPool::connect(configs.rest.clone())
        .await
        .map_err(|error| format!("收尾清理建连失败: {error}"))?;
    let mut leftover = Vec::new();
    for stable in &names {
        if let Err(error) = drop_stable(&pool, stable).await {
            leftover.push(format!("{stable}: {error}"));
        }
    }
    if let Err(error) = pool.close().await {
        leftover.push(format!("清理连接 close 失败: {error}"));
    }
    if leftover.is_empty() {
        Ok(())
    } else {
        Err(leftover.join("; "))
    }
}

/// 单传输数据面往返：DDL → 七条写入路径 → 查询/流式 → 清理并断言无残留。
///
/// 返回实际写入的行数，供调用方断言。
async fn data_plane(
    pool: &TaosPool,
    stable: &str,
    tag: &str,
    base_ts: i64,
    registry: &ResourceRegistry,
) -> usize {
    // 建表**之前**登记：中途 panic 时收尾仍能靠登记表清干净。
    registry.register_stable(stable);
    hit("fn", "TaosPool::ensure_stable");
    pool.ensure_stable(stable).await.expect("建超级表必须成功");

    // 偏移量以**秒**为单位：库精度可能是 ms/us，秒对齐对三种精度都无损。
    let make = |offset_seconds: i64, count: i64| -> Vec<TaosPoint> {
        (0..count)
            .map(|index| {
                TaosPoint::new(
                    tag,
                    base_ts + (offset_seconds + index) * 1_000_000_000,
                    format!("{index}.0"),
                    format!("{index}.1"),
                )
            })
            .collect()
    };
    let upper = base_ts + 1_000_000_000_000;

    // 七条写入路径：write_batch / write_series / write_batch_report /
    // write_batch_chunked / write_batch_chunked_report / write_batch_chunked_outcome /
    // write_batch_idempotent，合计 2+1+1+1+2+1+1 = 9 行。
    hit("fn", "TaosPool::write_batch");
    pool.write_batch(stable, &make(0, 2))
        .await
        .expect("write_batch 必须成功");

    hit("fn", "TaosPool::write_series");
    pool.write_series(stable, &make(10, 1))
        .await
        .expect("write_series 必须成功");

    hit("fn", "TaosPool::write_batch_report");
    let report = pool
        .write_batch_report(stable, &make(20, 1))
        .await
        .expect("write_batch_report 必须成功");
    hit("type", "BatchWriteReport");
    let BatchWriteReport {
        accepted,
        failed,
        chunks_ok,
        chunks_total,
    } = report;
    hit("field", "BatchWriteReport::accepted");
    assert_eq!(accepted, 1);
    hit("field", "BatchWriteReport::failed");
    assert_eq!(failed, 0);
    hit("field", "BatchWriteReport::chunks_ok");
    assert_eq!(chunks_ok, 1);
    hit("field", "BatchWriteReport::chunks_total");
    assert_eq!(chunks_total, 1);

    hit("fn", "TaosPool::write_batch_chunked");
    pool.write_batch_chunked(stable, &make(30, 1), 1)
        .await
        .expect("write_batch_chunked 必须成功");

    hit("fn", "TaosPool::write_batch_chunked_report");
    let chunked = pool
        .write_batch_chunked_report(stable, &make(40, 2), 1)
        .await
        .expect("write_batch_chunked_report 必须成功");
    assert_eq!(chunked.chunks_total, 2, "每批 1 行");
    assert_eq!(chunked.accepted, 2);

    hit("fn", "TaosPool::write_batch_chunked_outcome");
    let outcome = pool
        .write_batch_chunked_outcome(stable, &make(50, 1), 1)
        .await
        .expect("write_batch_chunked_outcome 必须 Ok");
    assert!(outcome.is_complete());

    hit("fn", "TaosPool::write_batch_idempotent");
    let idempotent = pool
        .write_batch_idempotent(stable, &make(60, 1))
        .await
        .expect("幂等写必须成功");
    assert_eq!(idempotent.accepted, 1);

    // 失败路径：非法 chunk 行数 → 结构化部分成功错误（真实调用，accepted=0）。
    let partial = pool
        .write_batch_chunked_outcome(stable, &make(70, 1), 0)
        .await
        .expect_err("max_rows=0 必须被拒绝");
    hit("type", "BatchWritePartialError");
    let BatchWritePartialError {
        report: partial_report,
        source,
    } = partial;
    hit("field", "BatchWritePartialError::report");
    assert_eq!(partial_report.accepted, 0);
    hit("field", "BatchWritePartialError::source");
    assert!(matches!(source, TaosError::Invalid(_)), "实际 {source:?}");

    // —— 查询：三种读路径 + 行内容 ——
    hit("fn", "TaosPool::query_series");
    let rows = pool
        .query_series(stable, base_ts - 1, upper)
        .await
        .expect("query_series 必须成功");
    assert_eq!(rows.len(), 9, "七条写入路径合计 9 行");
    hit("type", "TaosPoint");
    let TaosPoint {
        timestamp_ns,
        tag_value,
        values,
    } = rows[0].clone();
    hit("field", "TaosPoint::timestamp_ns");
    assert_eq!(timestamp_ns, base_ts);
    hit("field", "TaosPoint::tag_value");
    assert_eq!(tag_value, tag);
    hit("field", "TaosPoint::values");
    assert_eq!(values.len(), 2);

    hit("fn", "TaosPool::exec");
    let counted = pool
        .exec(&format!("SELECT count(*) FROM `{stable}`"))
        .await
        .expect("exec 必须成功");
    hit("type", "TaosExecResult");
    let TaosExecResult {
        code,
        rows: exec_rows,
        columns,
        affected_rows,
    } = counted;
    hit("field", "TaosExecResult::code");
    assert_eq!(code, 0);
    hit("field", "TaosExecResult::rows");
    assert_eq!(
        exec_rows
            .first()
            .and_then(|row| row.first())
            .map(String::as_str),
        Some("9")
    );
    hit("field", "TaosExecResult::columns");
    assert!(!columns.is_empty());
    hit("field", "TaosExecResult::affected_rows");
    assert!(affected_rows.is_none() || affected_rows.is_some_and(|value| value >= 0));

    hit("fn", "TaosPool::query");
    let queried = pool
        .query(&format!(
            "SELECT ts, bid, ask, symbol FROM `{stable}` ORDER BY ts ASC"
        ))
        .await
        .expect("query 必须成功");
    assert_eq!(queried.rows.len(), 9);

    hit("fn", "TaosPool::query_series_stream");
    let mut stream = pool
        .query_series_stream(stable, base_ts - 1, upper)
        .await
        .expect("query_series_stream 必须成功");
    hit("fn", "TaosQueryStream::remaining_hint");
    assert_eq!(stream.remaining_hint(), 9);
    let mut seen = 0usize;
    while let Some(item) = stream.next().await {
        item.expect("流内必须全为 Ok");
        seen += 1;
    }
    assert_eq!(seen, 9);
    assert!(stream.next().await.is_none());

    hit("fn", "TaosPool::query_series_stream_chunked");
    let chunked_stream = pool
        .query_series_stream_chunked(stable, base_ts - 1, upper, 3)
        .await
        .expect("query_series_stream_chunked 必须成功");
    hit("fn", "TaosQueryStream::chunk_hint");
    assert_eq!(chunked_stream.chunk_hint(), 3);
    assert!(
        pool.query_series_stream_chunked(stable, base_ts - 1, upper, 0)
            .await
            .is_err(),
        "chunk_hint=0 必须被拒绝"
    );

    // 故障注入点：此刻超级表与子表都已存在、数据已写入，但正常收尾尚未执行。
    maybe_inject_fault("after-write");

    drop_stable(pool, stable)
        .await
        .expect("收尾清理必须成功且无残留");
    // 清理后同一区间必须查不到任何行（缺表 → 空集，而非报错）。
    assert!(pool
        .query_series(stable, base_ts - 1, upper)
        .await
        .expect("清理后查询必须为空集")
        .is_empty());
    9
}

/// 阶段 6：REST 传输的数据面 + 池健康/统计/客户端别名。
async fn phase_rest_transport(configs: &E2EConfigs, registry: &ResourceRegistry) {
    let pool = TaosPool::connect(configs.rest.clone())
        .await
        .expect("REST 建连必须成功（检查 taosAdapter 与 FOUNDATIONX_TAOSX_*）");
    hit("type", "TaosPool");
    hit("fn", "TaosPool::connect");
    assert_eq!(pool.config().transport, TransportMode::Rest);

    hit("fn", "TaosPool::ping");
    pool.ping().await.expect("ping 必须成功");

    hit("fn", "TaosPool::health_check");
    let health = pool.health_check().await.expect("健康检查信封");
    hit("type", "TaosHealth");
    let TaosHealth {
        ready,
        precision,
        server_version,
        stats,
        metrics,
        detail,
    } = health.clone();
    hit("field", "TaosHealth::ready");
    assert!(ready, "{health:?}");
    hit("field", "TaosHealth::precision");
    assert!(matches!(
        precision,
        TsPrecision::Ms | TsPrecision::Us | TsPrecision::Ns
    ));
    hit("field", "TaosHealth::server_version");
    assert!(
        server_version
            .as_deref()
            .is_some_and(|value| !value.is_empty()),
        "health_check 必须返回服务端版本"
    );
    hit("field", "TaosHealth::stats");
    assert_eq!(stats.in_flight, 0);
    hit("field", "TaosHealth::metrics");
    assert!(metrics.total_events() >= 1);
    hit("field", "TaosHealth::detail");
    assert!(!detail.is_empty());
    hit("fn", "TaosHealth::is_ready");
    assert!(health.is_ready());

    hit("fn", "TaosPool::client");
    let alias: TaosClient = pool.client();
    hit("type", "TaosClient");
    hit("fn", "TaosPool::config");
    assert_eq!(alias.config().transport, TransportMode::Rest);

    hit("fn", "TaosPool::precision");
    assert!(matches!(
        pool.precision(),
        TsPrecision::Ms | TsPrecision::Us | TsPrecision::Ns
    ));

    // 真实运行期快照：14 个计数字段逐条读出，并用 total_events 做交叉核对。
    hit("fn", "TaosPool::metrics");
    let snapshot = pool.metrics();
    hit("type", "TaosMetricsSnapshot");
    let TaosMetricsSnapshot {
        sql_ok,
        sql_err,
        sql_bytes,
        response_bytes,
        write_ok,
        write_err,
        query_ok,
        query_err,
        ping_ok,
        ping_err,
        health_ready,
        health_not_ready,
        ws_probe_ok,
        ws_probe_err,
    } = snapshot;
    hit("field", "TaosMetricsSnapshot::sql_ok");
    assert!(sql_ok >= 1, "建连与 ping 已经消耗 REST SQL");
    hit("field", "TaosMetricsSnapshot::sql_err");
    assert!(sql_err <= sql_ok + sql_err);
    hit("field", "TaosMetricsSnapshot::sql_bytes");
    assert!(sql_bytes >= 1);
    hit("field", "TaosMetricsSnapshot::response_bytes");
    assert!(response_bytes >= 1);
    hit("field", "TaosMetricsSnapshot::write_ok");
    assert!(write_ok <= snapshot.total_events());
    hit("field", "TaosMetricsSnapshot::write_err");
    hit("field", "TaosMetricsSnapshot::query_ok");
    hit("field", "TaosMetricsSnapshot::query_err");
    hit("field", "TaosMetricsSnapshot::ping_ok");
    assert!(ping_ok >= 1, "ping 成功计数必须增长");
    hit("field", "TaosMetricsSnapshot::ping_err");
    assert!(ping_err <= ping_ok + ping_err);
    hit("field", "TaosMetricsSnapshot::health_ready");
    assert!(health_ready >= 1);
    hit("field", "TaosMetricsSnapshot::health_not_ready");
    hit("field", "TaosMetricsSnapshot::ws_probe_ok");
    hit("field", "TaosMetricsSnapshot::ws_probe_err");
    hit("fn", "TaosMetricsSnapshot::total_events");
    assert_eq!(
        sql_ok
            + sql_err
            + write_ok
            + write_err
            + query_ok
            + query_err
            + ping_ok
            + ping_err
            + health_ready
            + health_not_ready
            + ws_probe_ok
            + ws_probe_err,
        snapshot.total_events(),
        "total_events 必须等于各计数之和"
    );
    hit("fn", "TaosMetricsSnapshot::to_prometheus_text");
    assert!(snapshot.to_prometheus_text().contains("taosx_bytes_total"));
    hit("fn", "TaosPool::metrics_prometheus");
    assert!(pool.metrics_prometheus().contains("taosx_ops_total"));

    let rows = data_plane(
        &pool,
        &unique_name("taosx_e2e_rest"),
        &unique_tag(),
        aligned_now_ns(),
        registry,
    )
    .await;
    assert_eq!(rows, 9);

    // 缺表查询 → 空集（`is_not_found` 类型化判定，不做文案匹配）。
    hit("fn", "TaosPool::query_series");
    assert!(pool
        .query_series(&unique_name("taosx_e2e_absent"), 0, 1)
        .await
        .expect("缺表必须为空集")
        .is_empty());

    hit("fn", "TaosPool::stats");
    let TaosPoolStats { in_flight, closed } = pool.stats();
    hit("field", "TaosPoolStats::in_flight");
    assert_eq!(in_flight, 0);
    hit("field", "TaosPoolStats::closed");
    assert!(!closed);
    hit("fn", "TaosPool::is_closed");
    assert!(!pool.is_closed());
    hit("fn", "TaosPool::liveness");
    assert!(pool.liveness());

    hit("fn", "TaosPool::close");
    pool.close().await.expect("close 必须成功");
    assert!(pool.is_closed());
    assert!(pool.ping().await.is_err(), "close 后必须拒绝新请求");
}

/// 阶段 7：原生传输（`probe_native_tcp` + WS 握手 + `/rest/ws` 短会话 + 数据面）。
async fn phase_native_transport(configs: &E2EConfigs, registry: &ResourceRegistry) {
    // 原生 TCP 端口可达性（env 事实端口；不发送协议帧）。
    hit("fn", "probe_native_tcp");
    probe_native_tcp(&configs.native, configs.native_tcp_port)
        .await
        .unwrap_or_else(|error| {
            panic!(
                "原生端口 {}:{} 不可达（检查 tdengine 服务）: {error}",
                configs.native.host, configs.native_tcp_port
            )
        });
    assert!(probe_native_tcp(&configs.native, 0).await.is_err());

    // 自由函数握手探测（与 `TaosPool::connect` 内部复用同一路径）。
    hit("fn", "connect_native_ws");
    connect_native_ws(&configs.native)
        .await
        .expect("WS 握手探测必须成功");
    assert!(
        connect_native_ws(&configs.rest).await.is_err(),
        "Rest 模式必须被 connect_native_ws 拒绝"
    );

    let pool = TaosPool::connect(configs.native.clone())
        .await
        .expect("NativeWs 建连必须成功（taosAdapter 提供 /rest/ws）");
    hit("type", "TaosPool");
    hit("fn", "TaosPool::connect");
    assert_eq!(pool.config().transport, TransportMode::NativeWs);

    // 与 REST 相同的探活面：ping → health_check（保证两传输的数据面序列一致）。
    hit("fn", "TaosPool::ping");
    pool.ping().await.expect("NativeWs 池 ping 必须成功");
    hit("fn", "TaosPool::health_check");
    let native_health = pool.health_check().await.expect("健康检查信封");
    assert!(native_health.ready, "{native_health:?}");
    hit("fn", "TaosHealth::is_ready");
    assert!(native_health.is_ready());
    assert!(matches!(
        native_health.precision,
        TsPrecision::Ms | TsPrecision::Us | TsPrecision::Ns
    ));

    // WS 短会话：池方法与自由函数各一次，均要求真的收到服务端首帧。
    hit("fn", "TaosPool::exec_sql_ws");
    let frame = pool
        .exec_sql_ws("SELECT SERVER_VERSION()")
        .await
        .expect("WS 短会话必须收到服务端首帧");
    assert!(!frame.trim().is_empty(), "WS 首帧不得为空");
    hit("fn", "exec_sql_ws");
    let free_frame = exec_sql_ws(&configs.native, "SELECT SERVER_VERSION()")
        .await
        .expect("自由函数 WS 会话必须收到首帧");
    assert!(!free_frame.trim().is_empty(), "WS 首帧不得为空");
    assert!(exec_sql_ws(&configs.native, "   ").await.is_err());

    hit("fn", "ws_probe_totals");
    let (ok, err) = ws_probe_totals();
    assert!(ok >= 1, "WS 探测成功计数必须增长：ok={ok} err={err}");

    let rows = data_plane(
        &pool,
        &unique_name("taosx_e2e_native"),
        &unique_tag(),
        aligned_now_ns(),
        registry,
    )
    .await;
    assert_eq!(rows, 9);

    hit("fn", "TaosPool::close");
    pool.close().await.expect("close 必须成功");
    assert!(pool.is_closed());
}

/// 阶段 8：`connect_from_env`（env 路径的完整建连）。
///
/// env 的 `FOUNDATIONX_TAOSX_PORT` 可能是原生 TCP 端口（6030），它不服务 `/rest/ws`；
/// 故调用前把该变量临时改指 taosAdapter 端口，调用后按原值恢复，避免污染后续阶段。
async fn phase_connect_from_env(configs: &E2EConfigs) {
    let original = std::env::var(ENV_PORT).ok();
    std::env::set_var(ENV_PORT, configs.rest.port.to_string());
    let result = TaosPool::connect_from_env().await;
    match &original {
        Some(value) => std::env::set_var(ENV_PORT, value),
        None => std::env::remove_var(ENV_PORT),
    }
    let pool = result.expect("connect_from_env 必须成功（FOUNDATIONX_TAOSX_* 已注入）");
    hit("fn", "TaosPool::connect_from_env");
    hit("type", "TaosPool");
    hit("fn", "TaosPool::ping");
    pool.ping().await.expect("env 路径的池 ping 必须成功");
    hit("fn", "TaosPool::close");
    pool.close().await.expect("close 必须成功");
    assert!(pool.is_closed());
}

/// 阶段 9：`WriteBatcher` 的有界刷写与关闭语义。
async fn phase_write_batcher(configs: &E2EConfigs, registry: &ResourceRegistry) {
    let pool = TaosPool::connect(configs.rest.clone())
        .await
        .expect("batcher 用池建连必须成功");
    let stable = unique_name("taosx_e2e_batcher");
    let tag = unique_tag();
    let base_ts = aligned_now_ns();
    // 同样在建表之前登记，保证 batcher 阶段中途 panic 也能被收尾清理。
    registry.register_stable(&stable);
    pool.ensure_stable(&stable).await.expect("建超级表必须成功");

    hit("type", "WriteBatcher");
    hit("fn", "WriteBatcher::new");
    let batcher = WriteBatcher::new(
        pool.clone(),
        stable.clone(),
        WriteBatcherConfig {
            max_rows: 100,
            max_bytes_hint: 4096,
            flush_interval: Duration::from_secs(60),
        },
    );

    hit("fn", "WriteBatcher::push");
    batcher
        .push(TaosPoint::new(&tag, base_ts, "1.0", "1.1"))
        .await
        .expect("push 必须成功");
    hit("fn", "WriteBatcher::push");
    batcher
        .push(TaosPoint::new(&tag, base_ts + 1_000_000_000, "1.2", "1.3"))
        .await
        .expect("push 必须成功");

    hit("fn", "WriteBatcher::has_pending");
    assert!(!batcher.has_pending().await);
    hit("fn", "WriteBatcher::pending_len");
    assert_eq!(batcher.pending_len().await, 0);
    hit("fn", "WriteBatcher::totals");
    assert_eq!(batcher.totals().await, (0, 0), "未刷写前不得计入");

    hit("fn", "WriteBatcher::flush");
    let flushed = batcher.flush().await.expect("flush 必须成功");
    assert_eq!(flushed.accepted, 2);
    assert!(flushed.is_complete());

    hit("fn", "WriteBatcher::close_report");
    let summary = batcher.close_report().await;
    let BatcherCloseReport {
        total_accepted,
        total_failed,
        pending,
        last_flush,
    } = summary;
    hit("field", "BatcherCloseReport::total_accepted");
    assert_eq!(total_accepted, 2);
    hit("field", "BatcherCloseReport::total_failed");
    assert_eq!(total_failed, 0);
    hit("field", "BatcherCloseReport::pending");
    assert_eq!(pending, 0);
    hit("field", "BatcherCloseReport::last_flush");
    assert_eq!(last_flush, BatchWriteReport::default());

    hit("fn", "WriteBatcher::close");
    let closed = batcher.close().await.expect("close 必须成功");
    assert_eq!(closed.accepted, 0, "缓冲区已空");
    assert!(
        matches!(
            batcher
                .push(TaosPoint::new(&tag, base_ts + 2_000_000_000, "1", "2"))
                .await,
            Err(TaosError::Closed(_))
        ),
        "关闭后 push 必须被拒绝"
    );

    // 无 pending 时的门禁：take/ack 都必须 fail-closed。
    hit("fn", "WriteBatcher::take_pending");
    assert!(matches!(
        batcher.take_pending().await,
        Err(TaosError::Invalid(_))
    ));
    hit("fn", "WriteBatcher::ack_pending");
    assert!(matches!(
        batcher.ack_pending().await,
        Err(TaosError::Invalid(_))
    ));

    // 已刷写 2 行原样可查。
    hit("fn", "TaosPool::query_series");
    assert_eq!(
        pool.query_series(&stable, base_ts - 1, base_ts + 100_000_000_000)
            .await
            .expect("batcher 写入必须可查")
            .len(),
        2
    );

    // 故障注入点：batcher 的超级表已建、数据已刷写，正常收尾尚未执行。
    maybe_inject_fault("after-batcher-flush");

    drop_stable(&pool, &stable)
        .await
        .expect("收尾清理必须成功且无残留");
    hit("fn", "TaosPool::close");
    pool.close().await.expect("close 必须成功");
}

/// 单一驱动用例：保证阶段顺序与覆盖断言在同一个进程内完成。
///
/// **异常安全**：场景整体包在 `catch_unwind`（`futures_util` 的 async 版本，非
/// `Handle::block_on`——后者在 runtime 线程内会 panic）里；无论正常结束还是中途 panic，
/// 都由 [`cleanup_registry`] 删掉登记过的所有 `taosx_e2e_*` 超级表并断言无残留，
/// 之后才重新抛出原始 panic。共享库因此不会因一次失败而留下垃圾。
#[tokio::test]
#[ignore = "需要真实 TDengine（NativeWs /rest/ws 与 REST /rest/sql 同 taosAdapter 端口）与 FOUNDATIONX_TAOSX_* 环境变量"]
async fn e2e_taos_all_public_api() {
    // 配置面不碰服务端，放在捕获块之外，便于收尾清理复用其显式 REST 配置。
    let configs = phase_config_plane();
    let registry = ResourceRegistry::new();

    let outcome = AssertUnwindSafe(async {
        assert_manifest_wellformed();
        phase_constants();
        phase_value_types();
        phase_retry_and_streams().await;
        phase_offline_pool(&configs.env).await;
        phase_rest_transport(&configs, &registry).await;
        phase_native_transport(&configs, &registry).await;
        phase_connect_from_env(&configs).await;
        phase_write_batcher(&configs, &registry).await;
    })
    .catch_unwind()
    .await;

    // 收集异常安全清理（幂等：正常路径下对象早已删掉，此处只是无残留兜底断言）。
    let cleanup = cleanup_registry(&configs, &registry).await;

    match outcome {
        Ok(()) => {
            cleanup.expect("收尾清理必须成功且无残留");
            assert_coverage_complete();
        }
        Err(payload) => {
            // 失败路径同样必须清干净；清理若也失败，先把它暴露出来（保留原始 panic 前先断言）。
            assert!(
                cleanup.is_ok(),
                "失败路径的收尾清理未完成：{cleanup:?}（随后重新抛出原始 panic）"
            );
            std::panic::resume_unwind(payload);
        }
    }
}
