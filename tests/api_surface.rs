#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! 公共 API 表面与线程安全契约。

use std::time::Duration;

use taosx::{
    build_insert_sql_chunks, build_native_ws_url, connect_native_ws, exec_sql_ws, probe_native_tcp,
    validate_mode, ws_probe_totals, BatchWritePartialError, BatchWriteReport, BatcherCloseError,
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

    // 常量被真实引用，且断言其间的秩序关系（black_box 避免 clippy::assertions_on_constants）。
    // 原「求和 > 1_000」为重言式（各常量已在上方逐一钉死，求和不可能小于该值），
    // 现改为有鉴别力的关系断言：未来调整任一上限破坏秩序时用例变红。
    let (in_flight, batch_rows, batch_bytes, response_bytes, query_rows, close_ms) =
        std::hint::black_box((
            HARD_MAX_IN_FLIGHT,
            HARD_MAX_BATCH_ROWS,
            HARD_MAX_BATCH_BYTES,
            HARD_MAX_RESPONSE_BYTES,
            HARD_MAX_QUERY_ROWS,
            HARD_MAX_CLOSE_TIMEOUT.as_millis() as usize,
        ));
    assert!(in_flight > 0, "并发硬上限必须为正");
    assert!(
        batch_rows <= query_rows,
        "单批行数上限（{batch_rows}）不得超过单次查询行数上限（{query_rows}）"
    );
    assert!(
        batch_bytes < response_bytes,
        "单批字节上限（{batch_bytes}）必须小于响应字节上限（{response_bytes}）"
    );
    assert!(close_ms > 0, "关闭超时硬上限必须为正");
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
    // 函数指针与异步签名可绑定（编译期契约）。ws_probe_totals 的数值校验
    // 由独立异步用例 ws_probe_totals_counts_failed_probe 承担（需触发真实探测）。
    let _ = build_native_ws_url;
    let _ = validate_mode;
    let _ = exec_sql_ws;
    let _ = probe_native_tcp;
    let _ = build_insert_sql_chunks;
    let _ = ws_probe_totals;
}

/// `ws_probe_totals` 数值校验：失败的 WS 握手探测必须计入 err（恰好 +1），
/// 且不得污染 ok 计数。本二进制内无其他用例会触发 ws 探测计数，
/// 故可使用严格相等断言。
#[tokio::test]
async fn ws_probe_totals_counts_failed_probe() {
    let (ok_before, err_before) = ws_probe_totals();
    let config = TaosConfig {
        host: "127.0.0.1".to_owned(),
        port: 1, // 端口 1 无监听者，握手必然失败
        transport: TransportMode::NativeWs,
        timeout: Duration::from_millis(300),
        acquire_timeout: Duration::from_millis(300),
        ..TaosConfig::default()
    };
    let error = connect_native_ws(&config)
        .await
        .expect_err("不可达地址的 WS 握手必须失败");
    assert!(error.is_retryable(), "{error:?}");

    let (ok_after, err_after) = ws_probe_totals();
    assert_eq!(ok_after, ok_before, "失败探测不得计入 ok");
    assert_eq!(err_after, err_before + 1, "失败探测必须计入 err");
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
