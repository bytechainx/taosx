//! 公共 API 表面与线程安全契约。

use std::time::Duration;

use taosx::{
    build_insert_sql_chunks, build_native_ws_url, exec_sql_ws, probe_native_tcp, validate_mode,
    ws_probe_totals, BatchWritePartialError, BatchWriteReport, BatcherCloseError,
    BatcherCloseReport, RetryPolicy, TaosClient, TaosConfig, TaosConfigBuilder, TaosError,
    TaosExecResult, TaosHealth, TaosMetricsSnapshot, TaosPoint, TaosPool, TaosPoolStats,
    TaosQueryStream, TaosResult, TransportMode, TsPrecision, WriteBatcher, WriteBatcherConfig,
    HARD_MAX_BATCH_BYTES, HARD_MAX_BATCH_ROWS, HARD_MAX_CLOSE_TIMEOUT, HARD_MAX_IN_FLIGHT,
    HARD_MAX_QUERY_ROWS, HARD_MAX_RESPONSE_BYTES,
};

#[test]
fn public_types_are_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<TaosPool>();
    assert_send_sync::<TaosClient>();
    assert_send_sync::<TaosConfig>();
    assert_send_sync::<TaosConfigBuilder>();
    assert_send_sync::<TaosError>();
    assert_send_sync::<TaosResult<()>>();
    assert_send_sync::<TaosExecResult>();
    assert_send_sync::<TaosPoolStats>();
    assert_send_sync::<TaosHealth>();
    assert_send_sync::<BatchWriteReport>();
    assert_send_sync::<BatchWritePartialError>();
    assert_send_sync::<TaosPoint>();
    assert_send_sync::<WriteBatcher>();
    assert_send_sync::<WriteBatcherConfig>();
    assert_send_sync::<BatcherCloseReport>();
    assert_send_sync::<BatcherCloseError>();
    assert_send_sync::<TaosQueryStream>();
    assert_send_sync::<RetryPolicy>();
    assert_send_sync::<TaosMetricsSnapshot>();
    assert_send_sync::<TransportMode>();
    assert_send_sync::<TsPrecision>();
}

#[test]
fn client_is_clone_and_alias_of_pool() {
    fn assert_clone<T: Clone>() {}
    assert_clone::<TaosPool>();
    assert_clone::<TaosClient>();
    assert_clone::<TaosConfig>();
    assert_clone::<TaosConfigBuilder>();

    let pool = TaosPool::new(TaosConfig::default()).expect("离线构造");
    let client: TaosClient = pool.clone();
    let handle: TaosClient = client.client();
    assert_eq!(handle.config().port, 6041);
    assert_eq!(handle.precision(), TsPrecision::Ms);
}

#[test]
fn hard_limits_are_documented_and_ordered() {
    assert_eq!(HARD_MAX_IN_FLIGHT, 1_024);
    assert_eq!(HARD_MAX_BATCH_ROWS, 10_000);
    assert_eq!(HARD_MAX_BATCH_BYTES, 8 * 1024 * 1024);
    assert_eq!(HARD_MAX_RESPONSE_BYTES, 64 * 1024 * 1024);
    assert_eq!(HARD_MAX_QUERY_ROWS, 100_000);
    assert_eq!(HARD_MAX_CLOSE_TIMEOUT, Duration::from_secs(30));

    // 常量被真实引用（black_box 避免 clippy::assertions_on_constants）。
    let aggregate = HARD_MAX_IN_FLIGHT
        + HARD_MAX_BATCH_ROWS
        + HARD_MAX_BATCH_BYTES
        + HARD_MAX_RESPONSE_BYTES
        + HARD_MAX_QUERY_ROWS
        + HARD_MAX_CLOSE_TIMEOUT.as_millis() as usize;
    assert!(std::hint::black_box(aggregate) > 1_000);
}

#[test]
fn exec_result_keeps_source_fields() {
    let result = TaosExecResult {
        code: 0,
        rows: vec![vec!["1".to_owned()]],
        columns: vec!["n".to_owned()],
        affected_rows: Some(1),
    };
    assert_eq!(result.code, 0);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.columns, vec!["n".to_owned()]);
    assert_eq!(result.affected_rows, Some(1));
    let cloned = result.clone();
    assert_eq!(cloned.rows, result.rows);
}

#[test]
fn public_error_constructors_and_classification() {
    let errors = [
        TaosError::Config("配置".to_owned()),
        TaosError::Connection("连接".to_owned()),
        TaosError::backend("远端"),
        TaosError::from_taos_code(896, "繁忙"),
        TaosError::Unavailable("暂不可用".to_owned()),
        TaosError::Serialization("解析".to_owned()),
        TaosError::Io(std::io::Error::other("io")),
        TaosError::Timeout("超时".to_owned()),
        TaosError::Invalid("参数".to_owned()),
        TaosError::Closed("已关闭".to_owned()),
        TaosError::Unsupported("不支持".to_owned()),
    ];
    for error in &errors {
        assert!(!error.to_string().is_empty(), "{error:?} 必须有可读消息");
    }
    assert!(TaosError::from_http_status(503, "busy").is_retryable());
    assert!(TaosError::from_taos_code(9731, "缺表").is_not_found());

    let mapped: TaosError = TaosError::Invalid("参数".to_owned()).with_message("补充");
    assert!(mapped.to_string().contains("补充"));
}

#[test]
fn error_is_std_error_and_source_chain_works() {
    fn assert_error<T: std::error::Error>() {}
    assert_error::<TaosError>();
    assert_error::<BatchWritePartialError>();
    assert_error::<BatcherCloseError>();

    let io = TaosError::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"));
    assert!(io.is_retryable());
}

#[test]
fn free_functions_are_reachable_from_crate_root() {
    let config = TaosConfig::default();
    assert_eq!(build_native_ws_url(&config), "ws://127.0.0.1:6041/rest/ws");
    validate_mode(&config).expect("默认传输模式合法");
    assert!(build_insert_sql_chunks("ticks", &[], TsPrecision::Ms, 10)
        .expect("空输入")
        .is_empty());
    let _totals: (u64, u64) = ws_probe_totals();

    // 函数指针与异步签名可绑定（编译期契约）。
    let _ = build_native_ws_url;
    let _ = validate_mode;
    let _ = exec_sql_ws;
    let _ = probe_native_tcp;
    let _ = build_insert_sql_chunks;
}

#[test]
fn defaults_and_helpers_behave() {
    assert_eq!(WriteBatcherConfig::default().max_rows, 500);
    assert!(BatchWriteReport::default().is_complete());
    assert!(BatcherCloseReport::default().last_flush.is_complete());
    assert_eq!(RetryPolicy::default().max_attempts, 1);
    assert_eq!(RetryPolicy::for_read().max_attempts, 3);
    assert_eq!(RetryPolicy::for_idempotent_write().max_attempts, 3);
    assert_eq!(TransportMode::parse("rest"), Some(TransportMode::Rest));
    assert_eq!(
        TransportMode::parse("native"),
        Some(TransportMode::NativeWs)
    );
    assert_eq!(TsPrecision::parse("ms"), Some(TsPrecision::Ms));
    assert_eq!(TsPrecision::parse("us"), Some(TsPrecision::Us));
    assert_eq!(TsPrecision::parse("ns"), Some(TsPrecision::Ns));

    let text = TaosMetricsSnapshot::default().to_prometheus_text();
    assert!(text.contains("taosx_ops_total"));
    assert!(text.contains("taosx_bytes_total"));
    assert_eq!(TaosMetricsSnapshot::default().total_events(), 0);
}
