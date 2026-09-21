#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! AIDD 对抗 / 边界用例（特性 002）。
//!
//! 候选由 AI 生成，逐条人工复核后仅保留「结论=保留」项；丢弃项登记于 PR 描述。
//!
//! // AIDD: 超级表名 94/95 字节边界 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §2 标识符限长 | 结论=保留
//! // AIDD: tag 值 48/49 字节边界 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §2 tag 限长与子表编码 | 结论=保留
//! // AIDD: tag 值十六进制编码的碰撞抵抗 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §2 子表名不直接携带 tag | 结论=保留
//! // AIDD: 未对齐时间戳与饱和运算 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §2 禁止浮点与静默精度损失 | 结论=保留
//! // AIDD: 硬上限取等与越界 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §3 构建期 clamp 到 HARD_MAX_* | 结论=保留
//! // AIDD: TDengine 错误码 896 / 0x2603 / 9826 / 0 / -1 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §3 重试仅针对瞬时错误 | 结论=保留
//! // AIDD: 库名标识符 192/193 字节边界 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §2 标识符白名单含限长（库名 / 超级表名） | 结论=保留
//! // AIDD: 非 2xx 响应体夹带凭据 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §4 错误消息不回显凭据 | 结论=保留
//! // AIDD: 2xx 但正文非法 JSON 且夹带凭据 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §4 错误消息不回显凭据 | 结论=保留

use std::time::Duration;

use taosx::{
    build_insert_sql_chunks, TaosConfig, TaosError, TaosPoint, TaosPool, TsPrecision,
    HARD_MAX_BATCH_ROWS, HARD_MAX_CLOSE_TIMEOUT,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// 边界：超级表名恰好 94 字节放行、95 字节拒绝（限长为下界闭、上界闭）。
#[test]
fn stable_name_length_boundary() {
    let at_limit = "a".repeat(94);
    assert!(
        build_insert_sql_chunks(&at_limit, &[], TsPrecision::Ns, 1)
            .expect("94 字节应通过白名单")
            .is_empty(),
        "空点集应产出空分块而非错误"
    );

    let over_limit = "a".repeat(95);
    let error =
        build_insert_sql_chunks(&over_limit, &[], TsPrecision::Ns, 1).expect_err("95 字节必须拒绝");
    assert!(matches!(error, TaosError::Invalid(_)), "{error:?}");
}

/// 边界：tag 值恰好 48 字节放行、49 字节拒绝（十六进制编码前按原始字节计长）。
#[test]
fn tag_value_length_boundary() {
    let at_limit = "B".repeat(48);
    let chunks = build_insert_sql_chunks(
        "ticks",
        &[TaosPoint::new(at_limit, 1, "1.0", "1.1")],
        TsPrecision::Ns,
        1,
    )
    .expect("48 字节 tag 应通过");
    assert_eq!(chunks.len(), 1);

    let over_limit = "B".repeat(49);
    let error = build_insert_sql_chunks(
        "ticks",
        &[TaosPoint::new(over_limit, 1, "1.0", "1.1")],
        TsPrecision::Ns,
        1,
    )
    .expect_err("49 字节 tag 必须拒绝");
    assert!(matches!(error, TaosError::Invalid(_)), "{error:?}");
}

/// 边界：不同 tag 值必须映射到不同子表，且子表名不携带原始 tag 字符。
#[test]
fn tag_hex_encoding_is_collision_resistant() {
    let subtable_of = |tag: &str| {
        let sql = build_insert_sql_chunks(
            "ticks",
            &[TaosPoint::new(tag, 1, "1.0", "1.1")],
            TsPrecision::Ns,
            1,
        )
        .expect("分块")
        .remove(0);
        sql.split('`').nth(1).expect("子表名带反引号").to_owned()
    };

    let slash = subtable_of("BTC/USDT");
    let underscore = subtable_of("BTC_USDT");
    assert_ne!(slash, underscore, "分隔符不同不得映射到同一子表");
    for name in [&slash, &underscore] {
        assert!(name.starts_with("ticks_s"), "{name}");
        assert!(
            name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
            "子表名不得携带原始 tag 字符: {name}"
        );
    }
}

/// 边界：未对齐目标精度的时间戳 fail-closed；极值换算走饱和运算、不 panic。
#[test]
fn timestamp_alignment_and_saturation() {
    let unaligned = TaosPoint::new("A", 1_500, "0", "0");
    assert!(
        build_insert_sql_chunks("ticks", &[unaligned], TsPrecision::Us, 1).is_err(),
        "1500 ns 无法无损表示为 us，必须拒绝"
    );

    let aligned = TaosPoint::new("A", 1_500_000, "0", "0");
    let sql = build_insert_sql_chunks("ticks", &[aligned], TsPrecision::Us, 1)
        .expect("1500 us 对齐应通过")
        .remove(0);
    assert!(sql.contains("VALUES (1500,"), "{sql}");

    assert_eq!(TsPrecision::Ms.to_nanos(i64::MAX), i64::MAX, "饱和不溢出");
    assert_eq!(TsPrecision::Us.to_nanos(i64::MIN), i64::MIN, "饱和不溢出");
    assert_eq!(TsPrecision::Ms.from_nanos(i64::MIN), i64::MIN / 1_000_000);
}

/// 边界：硬上限取等放行、越界拒绝；分块行数为 0 一律拒绝。
#[test]
fn hard_limit_boundaries() {
    let at_limit = TaosConfig {
        close_timeout: HARD_MAX_CLOSE_TIMEOUT,
        ..TaosConfig::default()
    };
    at_limit.validate().expect("close_timeout 取等应放行");

    let over = TaosConfig {
        close_timeout: HARD_MAX_CLOSE_TIMEOUT + std::time::Duration::from_millis(1),
        ..TaosConfig::default()
    };
    assert!(over.validate().is_err(), "超过硬上限必须拒绝");

    let points = [TaosPoint::new("A", 1, "0", "0")];
    assert!(
        build_insert_sql_chunks("ticks", &points, TsPrecision::Ns, HARD_MAX_BATCH_ROWS + 1)
            .is_err(),
        "max_rows 越界必须拒绝"
    );
    assert!(
        build_insert_sql_chunks("ticks", &points, TsPrecision::Ns, 0).is_err(),
        "max_rows = 0 必须拒绝"
    );

    // 离线背压池在越界配置下同样 fail-closed。
    assert!(TaosPool::new(TaosConfig {
        max_in_flight: 0,
        ..TaosConfig::default()
    })
    .is_err());
}

/// 边界：库名标识符恰好 192 字节放行、193 字节拒绝（与子表名共用同一限长）。
#[test]
fn database_ident_length_boundary() {
    let at_limit = TaosConfig {
        database: "d".repeat(192),
        ..TaosConfig::default()
    };
    at_limit.validate().expect("192 字节库名应通过白名单");

    let over_limit = TaosConfig {
        database: "d".repeat(193),
        ..TaosConfig::default()
    };
    let error = over_limit
        .validate()
        .expect_err("193 字节库名必须 fail-closed 拒绝");
    assert!(matches!(error, TaosError::Config(_)), "{error:?}");

    // 空库名表示「不指定库」，仍应放行（走无库路径的 REST 端点）。
    TaosConfig {
        database: String::new(),
        ..TaosConfig::default()
    }
    .validate()
    .expect("空库名表示未指定，必须放行");
}

/// 边界：TDengine 错误码分类的极值与非正数输入。
#[test]
fn taos_error_code_boundaries() {
    assert!(
        TaosError::from_taos_code(896, "繁忙").is_retryable(),
        "896 为唯一可重试业务码"
    );
    assert!(TaosError::from_taos_code(0x2603, "表不存在").is_not_found());
    assert!(TaosError::from_taos_code(9826, "表不存在(兼容码)").is_not_found());
    assert!(!TaosError::from_taos_code(0x2603, "表不存在").is_retryable());

    // 0 与非正数落在 Backend，且保留原始码（便于诊断，不影响可重试判定）。
    for code in [0, -1, i32::MIN] {
        let error = TaosError::from_taos_code(code, "内部错误");
        assert!(
            matches!(error, TaosError::Backend { .. }),
            "{code}: {error:?}"
        );
        assert_eq!(error.taos_code(), Some(code), "{code} 必须保留错误码");
        assert!(!error.is_retryable(), "{code} 不可重试");
    }

    // 正数非特例码归入参数错误（不可重试）。
    assert!(matches!(
        TaosError::from_taos_code(i32::MAX, "语法错误"),
        TaosError::Invalid(_)
    ));
}

/// 本地一次性 HTTP 桩：按给定状态码与正文应答一次。
async fn spawn_http_mock(status: u16, reason: &'static str, body: &'static str) -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("绑定临时端口");
    let port = listener.local_addr().expect("读取临时端口").port();
    tokio::spawn(async move {
        if let Ok((mut stream, _)) = listener.accept().await {
            let mut buffer = [0_u8; 4096];
            let _ = stream.read(&mut buffer).await;
            let response = format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.shutdown().await;
        }
    });
    port
}

/// 指向本地桩、跳过建库与精度探测的池。
fn offline_pool(port: u16) -> TaosPool {
    TaosPool::new(TaosConfig {
        port,
        database: String::new(),
        timeout: Duration::from_secs(2),
        acquire_timeout: Duration::from_secs(2),
        ..TaosConfig::default()
    })
    .expect("离线构造池")
}

/// 边界：远端响应正文夹带凭据 / SQL 时，错误消息一律不得回显正文（标准 §4）。
#[tokio::test]
async fn error_messages_never_echo_response_body() {
    const LEAKED: &str = "password=s3cr3t-from-server; SELECT secret_col FROM t";

    // (1) 非 2xx：分类由 HTTP 状态决定，正文只作诊断来源，绝不进消息。
    let port = spawn_http_mock(400, "Bad Request", LEAKED).await;
    let error = offline_pool(port)
        .exec("SELECT 1")
        .await
        .expect_err("非 2xx 必须失败");
    let message = error.to_string();
    assert!(
        !message.contains("s3cr3t-from-server"),
        "错误不得回显响应正文中的凭据: {message}"
    );
    assert!(
        !message.contains("secret_col") && !message.contains("SELECT"),
        "错误不得回显响应正文 / SQL 片段: {message}"
    );
    assert!(!error.is_retryable());

    // (2) 2xx 但正文非法 JSON：解析失败同样不得回显正文。
    let port = spawn_http_mock(200, "OK", "not-json password=s3cr3t-from-server").await;
    let error = offline_pool(port)
        .exec("SELECT 1")
        .await
        .expect_err("非法 JSON 必须失败");
    assert!(matches!(error, TaosError::Serialization(_)), "{error:?}");
    let message = error.to_string();
    assert!(
        !message.contains("s3cr3t-from-server"),
        "错误不得回显响应正文中的凭据: {message}"
    );
    assert!(
        !message.contains("not-json"),
        "错误不得回显响应正文: {message}"
    );
}
