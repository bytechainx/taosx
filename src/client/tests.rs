//! TDengine REST 客户端（`TaosPool`）的内联单元测试。
//!
//! 由 `src/client.rs` 的 `#[cfg(test)] mod tests;` 引入，仅在测试构建中编译。
//! 首个导入以 `#[cfg(test)]` 标注，使审计器的测试段判定起点落在文件开头。

use super::response::parse_ts_cell;
use super::sql::{
    build_insert_sql_chunks_with_limits, encode_timestamp, subtable_name, MAX_SYMBOL_BYTES,
};
#[cfg(test)]
use super::*;
use crate::config::HARD_MAX_BATCH_ROWS;
use crate::point::TaosPoint;
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
    assert!(
        build_insert_sql_chunks("ticks", &points, TsPrecision::Ms, HARD_MAX_BATCH_ROWS + 1)
            .is_err()
    );
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
    let body = r#"{"code":0,"column_meta":[["v","VARCHAR",32]],"data":[["3.3.6.13"]],"rows":1}"#;
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
