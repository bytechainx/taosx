#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! 纯函数行为：SQL 分块与转义、URL 构造、模式校验、重试退避计算。

use std::time::Duration;

use taosx::{
    build_insert_sql_chunks, build_native_ws_url, validate_mode, RetryPolicy, TaosConfig,
    TaosError, TaosPoint, TransportMode, TsPrecision,
};

fn point(tag: &str, timestamp_ns: i64) -> TaosPoint {
    TaosPoint::new(tag, timestamp_ns, "1.0", "1.1")
}

#[test]
fn taos_point_constructor_keeps_protocol_cells() {
    let borrowed = TaosPoint::new("BTC/USDT", 42, "1.2300", "4.5600");
    assert_eq!(borrowed.timestamp_ns, 42);
    assert_eq!(borrowed.tag_value, "BTC/USDT");
    assert_eq!(borrowed.values, ["1.2300", "4.5600"]);

    let owned = TaosPoint::new(
        String::from("BTC"),
        43,
        String::from("0.1"),
        String::from("0.2"),
    );
    assert_eq!(owned, TaosPoint::new("BTC", 43, "0.1", "0.2"));
    assert_ne!(owned, borrowed);

    let cloned = owned.clone();
    assert_eq!(cloned, owned);
}

#[test]
fn insert_sql_chunks_partition_by_rows() {
    let points: Vec<TaosPoint> = (0..5)
        .map(|index| point("BTC", index * 1_000_000))
        .collect();
    let chunks = build_insert_sql_chunks("ticks", &points, TsPrecision::Ms, 2).expect("分块");
    assert_eq!(chunks.len(), 3, "5 行按 2 行/批应得到 2+2+1");
    for (index, chunk) in chunks.iter().enumerate() {
        assert!(
            chunk.starts_with("INSERT INTO "),
            "chunk {index} 前缀错误: {chunk}"
        );
        assert!(chunk.contains("VALUES"), "chunk {index} 缺少 VALUES");
        assert!(chunk.contains("USING `ticks`"), "chunk {index} 缺少 USING");
        assert!(chunk.contains("'1.0'"));
    }
    assert_eq!(chunks[0].matches("USING `ticks`").count(), 2);
    assert_eq!(chunks[2].matches("USING `ticks`").count(), 1);

    let single = build_insert_sql_chunks("ticks", &points, TsPrecision::Ms, 5).expect("单批");
    assert_eq!(single.len(), 1);

    let per_row = build_insert_sql_chunks("ticks", &points, TsPrecision::Ms, 1).expect("逐行");
    assert_eq!(per_row.len(), 5);

    assert!(build_insert_sql_chunks("ticks", &[], TsPrecision::Ms, 10)
        .expect("空输入")
        .is_empty());
    let error = build_insert_sql_chunks("ticks", &points, TsPrecision::Ms, 0)
        .expect_err("max_rows = 0 必须拒绝");
    assert!(matches!(error, TaosError::Invalid(_)), "{error:?}");
    let error = build_insert_sql_chunks("ticks", &points, TsPrecision::Ms, 10_001)
        .expect_err("max_rows 越界必须拒绝");
    assert!(matches!(error, TaosError::Invalid(_)), "{error:?}");
    let error = build_insert_sql_chunks("bad table", &points, TsPrecision::Ms, 1)
        .expect_err("非法表名必须拒绝");
    assert!(matches!(error, TaosError::Invalid(_)), "{error:?}");
    assert!(
        !error.to_string().contains("bad table"),
        "错误不得回显非法标识符: {error}"
    );
}

#[test]
fn insert_sql_escapes_literals_and_hex_encodes_subtable() {
    let hostile = TaosPoint::new("a'; DROP DATABASE x; --", 1, "1'0", "2\\0");
    let sql = build_insert_sql_chunks("ticks", &[hostile], TsPrecision::Ns, 1)
        .expect("分块")
        .remove(0);

    // tag 值经十六进制编码进入子表名，原样文本只出现在被转义的 TAGS 字面量中。
    assert!(!sql.contains("a'; DROP"), "tag 值不得原样进入标识符: {sql}");
    assert!(sql.contains(r"TAGS ('a\'; DROP DATABASE x; --')"), "{sql}");
    assert!(
        sql.contains("`ticks_s61273b2044524f5020444154414241534520783b202d2d`"),
        "子表名必须为 tag 值的十六进制编码: {sql}"
    );
    assert!(sql.contains(r"'1\'0'"), "单引号必须转义: {sql}");
    assert!(sql.contains(r"'2\\0'"), "反斜杠必须转义: {sql}");

    // 十六进制子表名：不同 tag 值映射到不同子表，且不含非法字符。
    let first = build_insert_sql_chunks("ticks", &[point("BTC/USDT", 1)], TsPrecision::Ns, 1)
        .expect("a")
        .remove(0);
    let second = build_insert_sql_chunks("ticks", &[point("BTC_USDT", 1)], TsPrecision::Ns, 1)
        .expect("b")
        .remove(0);
    assert_ne!(first, second);
    assert!(first.contains("`ticks_s4254432f55534454`"), "{first}");
}

#[test]
fn insert_sql_rejects_unaligned_or_oversized_input() {
    // 1_500 ns 在 ns 精度下无损，在 ms 精度下必须 fail-closed（禁止静默截断）。
    build_insert_sql_chunks("ticks", &[point("A", 1_500)], TsPrecision::Ns, 1).expect("ns 无损");
    let error = build_insert_sql_chunks("ticks", &[point("A", 1_500)], TsPrecision::Ms, 1)
        .expect_err("ms 精度必须拒绝");
    assert!(error.to_string().contains("精度"), "{error}");

    // tag 值超过 48 字节被拒绝。
    let oversized_tag = point(&"X".repeat(49), 1);
    let error = build_insert_sql_chunks("ticks", &[oversized_tag], TsPrecision::Ns, 1)
        .expect_err("49 字节 tag 必须拒绝");
    assert!(matches!(error, TaosError::Invalid(_)), "{error:?}");
    let max_tag = point(&"X".repeat(48), 1);
    build_insert_sql_chunks("ticks", &[max_tag], TsPrecision::Ns, 1).expect("48 字节必须通过");
}

#[test]
fn native_ws_url_and_mode_validation() {
    let plain = TaosConfig::default();
    assert_eq!(build_native_ws_url(&plain), "ws://127.0.0.1:6041/rest/ws");
    assert_eq!(plain.native_ws_url(), "ws://127.0.0.1:6041/rest/ws");
    assert_eq!(plain.rest_sql_url(), "http://127.0.0.1:6041/rest/sql");
    assert_eq!(
        plain.rest_sql_db_url(),
        "http://127.0.0.1:6041/rest/sql/infra_draft"
    );

    let secure = TaosConfig {
        host: "td.example".to_owned(),
        port: 6041,
        tls: true,
        password: "p".to_owned(),
        ..TaosConfig::default()
    };
    assert_eq!(
        build_native_ws_url(&secure),
        "wss://td.example:6041/rest/ws"
    );
    assert_eq!(secure.rest_sql_url(), "https://td.example:6041/rest/sql");

    let ipv6 = TaosConfig {
        host: "::1".to_owned(),
        ..TaosConfig::default()
    };
    assert_eq!(ipv6.rest_sql_url(), "http://[::1]:6041/rest/sql");
    assert_eq!(build_native_ws_url(&ipv6), "ws://[::1]:6041/rest/ws");
    assert_eq!(ipv6.rest_sql_endpoint().expect("可解析").port(), Some(6041));

    for mode in [TransportMode::Rest, TransportMode::NativeWs] {
        let config = TaosConfig {
            transport: mode,
            ..TaosConfig::default()
        };
        validate_mode(&config).expect("两种模式都必须通过校验");
    }
    let error = validate_mode(&TaosConfig {
        max_in_flight: 0,
        ..TaosConfig::default()
    })
    .expect_err("max_in_flight = 0 必须拒绝");
    assert!(matches!(error, TaosError::Config(_)), "{error:?}");
    let error = validate_mode(&TaosConfig {
        host: "td.example".to_owned(),
        ..TaosConfig::default()
    })
    .expect_err("远程明文必须拒绝");
    assert!(matches!(error, TaosError::Config(_)), "{error:?}");
}

#[test]
fn precision_and_mode_parsing() {
    assert_eq!(TsPrecision::parse(" ms "), Some(TsPrecision::Ms));
    assert_eq!(TsPrecision::parse("US"), Some(TsPrecision::Us));
    assert_eq!(TsPrecision::parse("ns"), Some(TsPrecision::Ns));
    assert_eq!(TsPrecision::parse("second"), None);
    assert_eq!(TsPrecision::Ms.from_nanos(1_500_000), 1);
    assert_eq!(TsPrecision::Ms.to_nanos(1), 1_000_000);
    assert_eq!(TsPrecision::Us.from_nanos(1_500), 1);
    assert_eq!(TsPrecision::Ns.from_nanos(1_500), 1_500);
    assert_eq!(TsPrecision::Ms.as_str(), "ms");

    assert_eq!(TransportMode::parse("rest"), Some(TransportMode::Rest));
    assert_eq!(TransportMode::parse("http"), Some(TransportMode::Rest));
    assert_eq!(
        TransportMode::parse("native"),
        Some(TransportMode::NativeWs)
    );
    assert_eq!(TransportMode::parse("ws"), Some(TransportMode::NativeWs));
    assert_eq!(
        TransportMode::parse("native_ws"),
        Some(TransportMode::NativeWs)
    );
    assert_eq!(
        TransportMode::parse("native-ws"),
        Some(TransportMode::NativeWs)
    );
    assert_eq!(TransportMode::parse("grpc"), None);
}

#[test]
fn retry_backoff_is_exponential_jittered_and_capped() {
    let policy = RetryPolicy {
        max_attempts: 6,
        initial_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_millis(800),
        jitter_ratio: 0.5,
        deadline: Some(Duration::from_secs(10)),
    };

    // 无抖动时严格 2 倍增长并在 max_backoff 处截断。
    let plain = RetryPolicy {
        jitter_ratio: 0.0,
        ..policy
    };
    assert_eq!(plain.compute_backoff(0, 0.5), Duration::from_millis(100));
    assert_eq!(plain.exponential_backoff(1), Duration::from_millis(200));
    assert_eq!(plain.exponential_backoff(2), Duration::from_millis(400));
    assert_eq!(plain.exponential_backoff(3), Duration::from_millis(800));
    assert_eq!(plain.exponential_backoff(20), Duration::from_millis(800));

    // 抖动区间为 [base*(1-r), base*(1+r)]，且不超过 max_backoff。
    assert_eq!(policy.compute_backoff(0, 0.0), Duration::from_millis(50));
    assert_eq!(policy.compute_backoff(0, 0.5), Duration::from_millis(100));
    assert_eq!(policy.compute_backoff(0, 1.0), Duration::from_millis(150));

    // 指数退避到达上限后，抖动结果仍被 max_backoff 截断。
    assert_eq!(policy.compute_backoff(3, 0.5), Duration::from_millis(800));
    assert_eq!(policy.compute_backoff(3, 1.0), Duration::from_millis(800));
    assert_eq!(policy.compute_backoff(9, 1.0), Duration::from_millis(800));
    assert!(policy.backoff_for_attempt(0) <= policy.max_backoff);
    for attempt in 0..8 {
        assert!(policy.backoff_for_attempt(attempt) <= policy.max_backoff);
    }

    assert!(RetryPolicy::is_retryable(&taosx::TaosError::Unavailable(
        "x".to_owned()
    )));
    assert!(!RetryPolicy::is_retryable(&taosx::TaosError::Invalid(
        "x".to_owned()
    )));
    assert_eq!(RetryPolicy::default().max_attempts, 1);
    assert_eq!(RetryPolicy::for_read().max_attempts, 3);
    assert_eq!(RetryPolicy::for_idempotent_write().max_attempts, 3);
}

#[tokio::test]
async fn retry_run_stops_on_permanent_error_and_stops_after_success() {
    use std::sync::atomic::{AtomicU32, Ordering};

    let calls = AtomicU32::new(0);
    let policy = RetryPolicy {
        max_attempts: 4,
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(2),
        jitter_ratio: 0.0,
        deadline: None,
    };
    let value = policy
        .run(|| async {
            let attempt = calls.fetch_add(1, Ordering::SeqCst);
            if attempt < 2 {
                Err(taosx::TaosError::Connection("refused".to_owned()))
            } else {
                Ok(attempt)
            }
        })
        .await
        .expect("第三次必须成功");
    assert_eq!(value, 2);
    assert_eq!(calls.load(Ordering::SeqCst), 3);

    let calls = AtomicU32::new(0);
    let error = policy
        .run(|| async {
            calls.fetch_add(1, Ordering::SeqCst);
            Err::<(), _>(taosx::TaosError::Serialization("bad".to_owned()))
        })
        .await
        .expect_err("不可重试错误必须立即返回");
    assert!(matches!(error, taosx::TaosError::Serialization(_)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
