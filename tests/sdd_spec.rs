#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! SDD 规格对照（特性 002）：把 `docs/标准.md` 的章节条款转成可执行断言。
//!
//! 章节与断言一一对应，`SPEC-MAP` 表即映射清单。
//!
//! // SPEC-MAP: S-1 | 1. 定位 | assert_positioning
//! // SPEC-MAP: S-2 | 2. 数据约定 | assert_data_conventions
//! // SPEC-MAP: S-3 | 3. 资源治理 | assert_resource_governance
//! // SPEC-MAP: S-4 | 4. 安全约定 | assert_security_conventions
//! // SPEC-MAP: S-5 | 5. 验收 | assert_acceptance
//! // SPEC-MAP: S-6 | 6. 质量门禁约束 | assert_gate_constraints

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
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// 慢响应桩：接受 1 次连接后延迟 `delay` 再应答，用于观测 in-flight 背压。
async fn spawn_slow_mock(delay: Duration) -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("绑定临时端口");
    let port = listener.local_addr().expect("读取临时端口").port();
    tokio::spawn(async move {
        if let Ok((mut stream, _)) = listener.accept().await {
            let mut buffer = [0_u8; 2048];
            let _ = stream.read(&mut buffer).await;
            tokio::time::sleep(delay).await;
            let body = r#"{"code":0,"column_meta":[],"data":[],"rows":0}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes()).await;
        }
    });
    port
}

/// S-1：双传输（REST + 原生 WebSocket）与池原语；零领域模型。
#[test]
fn assert_positioning() {
    let defaults = TaosConfig::default();
    assert_eq!(defaults.transport, TransportMode::Rest, "默认走 REST");
    assert_eq!(defaults.port, 6041);
    assert_eq!(defaults.rest_sql_url(), "http://127.0.0.1:6041/rest/sql");
    assert_eq!(
        defaults.rest_sql_db_url(),
        "http://127.0.0.1:6041/rest/sql/infra_draft"
    );

    let native = TaosConfig {
        transport: TransportMode::NativeWs,
        ..TaosConfig::default()
    };
    assert_eq!(
        build_native_ws_url(&native),
        "ws://127.0.0.1:6041/rest/ws",
        "WS 与 REST 同端口（taosAdapter）"
    );
    // 两种模式都属已知取值；非法配置在模式校验阶段即被拒。
    validate_mode(&defaults).expect("REST 模式必须通过");
    validate_mode(&native).expect("NativeWs 模式必须通过");
    let error = validate_mode(&TaosConfig {
        max_in_flight: 0,
        ..TaosConfig::default()
    })
    .expect_err("max_in_flight = 0 必须拒绝");
    assert!(matches!(error, TaosError::Config(_)), "{error:?}");

    // DTO 只描述物理表协议（时间戳 + tag + 两个文本单元格），不含领域语义。
    let point = TaosPoint::new("BTC/USDT", 42, "1.0", "1.1");
    assert_eq!(
        (point.timestamp_ns, point.tag_value.as_str(), &point.values),
        (42, "BTC/USDT", &["1.0".to_owned(), "1.1".to_owned()])
    );
    assert!(!TaosPool::new(TaosConfig::default())
        .expect("离线构造")
        .is_closed());
}

/// S-2：时间戳整数换算（禁浮点）、标识符白名单、tag 十六进制子表名与字面量转义。
#[test]
fn assert_data_conventions() {
    // 纳秒 ↔ 库精度是整数换算，且不允许静默精度损失。
    assert_eq!(TsPrecision::Ms.from_nanos(1_500_000_000), 1500);
    assert_eq!(TsPrecision::Ns.to_nanos(42), 42);
    assert_eq!(TsPrecision::parse("US"), Some(TsPrecision::Us));

    let points = vec![TaosPoint::new("BTC/USDT", 1_500_000, "1.0", "2.0")];
    let error = build_insert_sql_chunks("ticks", &points, TsPrecision::Ms, 10)
        .expect_err("1_500_000 ns 无法无损表示为 ms，必须 fail-closed");
    assert!(matches!(error, TaosError::Invalid(_)), "{error:?}");
    assert!(error.to_string().contains("精度"), "{error}");

    // tag 值十六进制编码进子表名，绝不直接进入标识符；字符串字面量按 TDengine 规则转义。
    let escaped = TaosPoint::new("A'B", 1, "1'0", "2\\0");
    let sql = build_insert_sql_chunks("ticks", &[escaped], TsPrecision::Ns, 1)
        .expect("分块")
        .remove(0);
    assert!(sql.contains(r"TAGS ('A\'B')"), "{sql}");
    assert!(sql.contains(r"'1\'0'"), "{sql}");
    assert!(sql.contains(r"'2\\0'"), "{sql}");
    let subtable = sql
        .split('`')
        .nth(1)
        .expect("多子表 INSERT 必须带反引号子表名");
    assert!(
        !subtable.contains('/') && subtable.starts_with("ticks_s"),
        "子表名应为 <stable>_s<hex>: {subtable}"
    );

    // 标识符白名单：非字母/下划线开头或含非法字符一律拒绝。
    for table in ["1bad", "a;drop", ""] {
        let error =
            build_insert_sql_chunks(table, &[], TsPrecision::Ns, 1).expect_err("非法表名必须拒绝");
        assert!(
            matches!(error, TaosError::Invalid(_)),
            "table={table:?} -> {error:?}"
        );
    }
}

/// S-3：硬上限 fail-closed、信号量背压受 `acquire_timeout` 约束、重试只针对瞬时错误。
#[tokio::test]
async fn assert_resource_governance() {
    // 资源上界在构建期校验：越界一律拒绝，不做运行期无界增长。
    for config in [
        TaosConfig {
            batch_max_rows: HARD_MAX_BATCH_ROWS + 1,
            ..TaosConfig::default()
        },
        TaosConfig {
            batch_max_bytes: HARD_MAX_BATCH_BYTES + 1,
            ..TaosConfig::default()
        },
        TaosConfig {
            max_response_bytes: HARD_MAX_RESPONSE_BYTES + 1,
            ..TaosConfig::default()
        },
        TaosConfig {
            max_query_rows: HARD_MAX_QUERY_ROWS + 1,
            ..TaosConfig::default()
        },
        TaosConfig {
            max_in_flight: HARD_MAX_IN_FLIGHT + 1,
            ..TaosConfig::default()
        },
        TaosConfig {
            close_timeout: HARD_MAX_CLOSE_TIMEOUT + Duration::from_millis(1),
            ..TaosConfig::default()
        },
    ] {
        let error = config.validate().expect_err("越界配置必须拒绝");
        assert!(
            matches!(error, TaosError::Config(_)),
            "{config:?} -> {error:?}"
        );
    }

    // 背压：max_in_flight = 1 时第二个并发请求在 acquire_timeout 内拿不到许可即超时。
    let port = spawn_slow_mock(Duration::from_millis(250)).await;
    let pool = TaosPool::new(TaosConfig {
        port,
        database: String::new(),
        max_in_flight: 1,
        acquire_timeout: Duration::from_millis(50),
        timeout: Duration::from_secs(2),
        ..TaosConfig::default()
    })
    .expect("池");
    let (first, second) = tokio::join!(pool.exec("SELECT 1"), pool.exec("SELECT 2"));
    // 此处 is_ok() 仅用于统计成功个数（下方紧跟 matches!(Timeout) 类型断言），非裸判定。
    let outcomes = [first.is_ok(), second.is_ok()];
    assert_eq!(
        outcomes.iter().filter(|ok| **ok).count(),
        1,
        "1 个额度下并发两个请求应恰好一个成功"
    );
    let starved = [first, second]
        .into_iter()
        .find_map(Result::err)
        .expect("必须有一个请求被背压拒绝");
    assert!(matches!(starved, TaosError::Timeout(_)), "{starved:?}");

    // 有界查询流：chunk_hint = 0 拒绝；行数与 chunk_hint 可观测。
    // TaosQueryStream 未实现 Debug，无法用 expect_err，改 match 提取错误。
    let error = match TaosQueryStream::from_rows_chunked(vec![], 0) {
        Err(error) => error,
        Ok(_) => panic!("chunk_hint = 0 必须拒绝"),
    };
    assert!(matches!(error, TaosError::Invalid(_)), "{error:?}");
    let stream = TaosQueryStream::from_rows_chunked(
        vec![
            TaosPoint::new("A", 1, "1", "2"),
            TaosPoint::new("B", 2, "3", "4"),
        ],
        2,
    )
    .expect("有界流");
    assert_eq!(stream.remaining_hint(), 2);
    assert_eq!(stream.chunk_hint(), 2);

    // 重试：只重试瞬时错误，退避受 max_backoff 约束，非幂等写默认不重试。
    assert!(RetryPolicy::is_retryable(&TaosError::Timeout("x".into())));
    assert!(!RetryPolicy::is_retryable(&TaosError::backend("x")));
    assert_eq!(
        RetryPolicy::no_retry().max_attempts,
        1,
        "非幂等写默认不重试"
    );
    assert_eq!(RetryPolicy::for_read().max_attempts, 3);
    let policy = RetryPolicy::for_read();
    assert!(
        policy.exponential_backoff(3) <= policy.max_backoff,
        "退避不得超过 max_backoff"
    );
    assert!(policy.exponential_backoff(1) >= policy.exponential_backoff(0));
}

/// S-4：凭据只能经环境变量 / builder；Debug 脱敏；远程强制 TLS 且必须带密码。
#[test]
fn assert_security_conventions() {
    // 凭据禁止进入 TOML 明文仓库配置。
    let error = TaosConfig::from_toml("schema_version = 1\npassword = \"hunter2\"\n")
        .expect_err("TOML 非空密码必须拒绝");
    assert!(error.to_string().contains("password"));
    assert!(!error.to_string().contains("hunter2"), "错误不得回显密码");

    // Debug 输出脱敏。
    let configured = TaosConfig::builder()
        .password("secret-value")
        .build()
        .expect("配置有效");
    let rendered = format!("{configured:?}");
    assert!(rendered.contains("***"));
    assert!(!rendered.contains("secret-value"));

    // 远程端点强制 HTTPS/WSS：明文仅限 loopback。
    let remote_plain = TaosConfig {
        host: "td.example".into(),
        ..TaosConfig::default()
    };
    let error = remote_plain.validate().expect_err("远程明文必须拒绝");
    assert!(matches!(error, TaosError::Config(_)), "{error:?}");
    let error = validate_mode(&TaosConfig {
        host: "td.example".into(),
        transport: TransportMode::NativeWs,
        ..TaosConfig::default()
    })
    .expect_err("远程明文 NativeWs 必须拒绝");
    assert!(matches!(error, TaosError::Config(_)), "{error:?}");

    // 远程 TLS 必须同时配置认证密码。
    let remote_tls_no_password = TaosConfig {
        host: "td.example".into(),
        tls: true,
        ..TaosConfig::default()
    };
    let error = remote_tls_no_password
        .validate()
        .expect_err("远程 TLS 无密码必须拒绝");
    assert!(matches!(error, TaosError::Config(_)), "{error:?}");
    let remote_secure = TaosConfig {
        host: "td.example".into(),
        tls: true,
        password: "configured".into(),
        ..TaosConfig::default()
    };
    remote_secure.validate().expect("远程 TLS + 密码必须通过");
}

/// S-5：验收命令可在无网络条件下执行——纯函数、配置面与池统计一致性。
#[tokio::test]
async fn assert_acceptance() {
    // 纯函数：分块构造、端点构造、模式校验。
    let points: Vec<TaosPoint> = (0..5)
        .map(|index| TaosPoint::new("BTC", index * 1_000_000, "1.0", "1.1"))
        .collect();
    assert_eq!(
        build_insert_sql_chunks("ticks", &points, TsPrecision::Ms, 2)
            .expect("分块")
            .len(),
        3
    );
    let error = build_insert_sql_chunks("ticks", &points, TsPrecision::Ms, 0)
        .expect_err("max_rows = 0 必须拒绝");
    assert!(matches!(error, TaosError::Invalid(_)), "{error:?}");

    // 池统计一致性：离线构造后 in_flight / closed 均为初始态。
    let pool = TaosPool::new(TaosConfig::default()).expect("离线构造");
    let stats: TaosPoolStats = pool.stats();
    assert_eq!(stats.in_flight, 0);
    assert!(!stats.closed);
    assert!(pool.liveness());
    assert!(!pool.is_closed());
    assert!(!pool.metrics_prometheus().is_empty());
    pool.close().await.expect("关闭");
    assert_eq!(pool.stats().in_flight, 0);
    assert!(pool.stats().closed);

    // 原生 TCP 端口探测对非法端口 fail-closed（不发握手帧）。
    let error = probe_native_tcp(&TaosConfig::default(), 0)
        .await
        .expect_err("native_port = 0 必须拒绝");
    assert!(matches!(error, TaosError::Invalid(_)), "{error:?}");
    assert!(error.to_string().contains("native_port"), "{error}");

    // WS 短会话对空 SQL fail-closed（不伪造成功）。
    let error = exec_sql_ws(&TaosConfig::default(), "   ")
        .await
        .expect_err("空 SQL 必须拒绝");
    assert!(matches!(error, TaosError::Invalid(_)), "{error:?}");
    assert!(error.to_string().contains("SQL"), "{error}");

    // 非 NativeWs 模式不得走原生 WS 建连。
    assert!(matches!(
        connect_native_ws(&TaosConfig::default()).await,
        Err(TaosError::Config(_))
    ));
}

/// S-6：`docs/API.md` 与本文件登记的公开面必须与实际一致——重命名 / 移除会让本测试编译失败。
#[test]
fn assert_gate_constraints() {
    fn exists<T>() {}
    exists::<TaosPool>();
    exists::<TaosClient>();
    exists::<TaosConfig>();
    exists::<TaosConfigBuilder>();
    exists::<TransportMode>();
    exists::<TsPrecision>();
    exists::<TaosError>();
    exists::<TaosResult<()>>();
    exists::<TaosExecResult>();
    exists::<TaosPoolStats>();
    exists::<TaosHealth>();
    exists::<TaosMetricsSnapshot>();
    exists::<BatchWriteReport>();
    exists::<BatchWritePartialError>();
    exists::<TaosPoint>();
    exists::<WriteBatcher>();
    exists::<WriteBatcherConfig>();
    exists::<BatcherCloseReport>();
    exists::<BatcherCloseError>();
    exists::<TaosQueryStream>();
    exists::<RetryPolicy>();

    let _ = (
        HARD_MAX_IN_FLIGHT,
        HARD_MAX_BATCH_ROWS,
        HARD_MAX_BATCH_BYTES,
        HARD_MAX_RESPONSE_BYTES,
        HARD_MAX_QUERY_ROWS,
        HARD_MAX_CLOSE_TIMEOUT,
    );
    let _ = (
        ws_probe_totals,
        build_insert_sql_chunks,
        build_native_ws_url,
        validate_mode,
        probe_native_tcp,
        exec_sql_ws,
        connect_native_ws,
    );
}
