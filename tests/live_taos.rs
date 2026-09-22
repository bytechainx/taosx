#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! live 真连服：TDengine 双传输（NativeWs 主测 + REST 补测）。
//!
//! 全部用例 `#[ignore]`，默认不参与 CI；凭据**只**从环境变量 `FOUNDATIONX_TAOSX_*`
//! 读取，不硬编码。运行方式见 `scripts/live/README.md`：
//!
//! ```text
//! # 主测：NativeWs（env 给原生端口 6030）
//! set -a; source /home/workspace/sre/secrets/env/taosx.env; set +a
//! cd /home/workspace/bytechainx/taosx
//! CARGO_TARGET_DIR=/home/workspace/bytechainx/.cargo/target \
//!   cargo test --test live_taos -- --ignored --test-threads=1
//!
//! # 补测：REST（env 给 taosAdapter 端口 6041）
//! set -a; source /home/workspace/sre/secrets/env/taosx-rest.env; set +a
//! CARGO_TARGET_DIR=/home/workspace/bytechainx/.cargo/target \
//!   cargo test --test live_taos -- --ignored --test-threads=1
//! ```
//!
//! 两个用例各自固定「本模式需要的端口」，因此用任一 env 文件跑全量都能通过：
//! `taosx.env` 给原生端口 6030、`taosx-rest.env` 给 adapter 端口 6041，二者是同一
//! 主机的两个监听；`adapter_port()` 把前者归一为后者。

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use taosx::{probe_native_tcp, TaosConfig, TaosPoint, TaosPool, TransportMode};

/// TDengine 原生协议端口（Native SQL / FFI 前置探测）。
const NATIVE_TCP_PORT: u16 = 6030;
/// taosAdapter 端口：REST 与原生 WebSocket 共用（`/rest/sql`、`/rest/ws`）。
const ADAPTER_PORT: u16 = 6041;

/// 把 env 可能给出的原生端口归一为 taosAdapter 端口。
fn adapter_port(env_port: u16) -> u16 {
    if env_port == NATIVE_TCP_PORT {
        ADAPTER_PORT
    } else {
        env_port
    }
}

/// 唯一化标识：`<前缀>_<pid>_<纳秒时间戳>`。
fn unique_id(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("系统时钟应晚于 UNIX_EPOCH")
        .as_nanos();
    format!("{prefix}_{}_{}", std::process::id(), nanos)
}

/// 唯一化 tag 值（≤48 字节，十六进制编码后进入子表名）。
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

/// 探活 + 结构化健康断言。
async fn assert_healthy(pool: &TaosPool) {
    pool.ping().await.expect("ping 必须成功");
    let health = pool.health_check().await.expect("健康检查信封");
    assert!(health.ready, "health_check 应为就绪: {health:?}");
    assert!(health.is_ready());
    assert!(
        health
            .server_version
            .as_deref()
            .is_some_and(|v| !v.is_empty()),
        "应返回服务端版本: {health:?}"
    );
    assert!(health.detail == "就绪", "detail={}", health.detail);
}

/// 唯一化超级表上的最小数据面往返：建表 → 批量写 → 查询 → 清理并断言无残留。
async fn data_plane_roundtrip(pool: &TaosPool, stable: &str, tag: &str, timestamp_ns: i64) {
    pool.ensure_stable(stable).await.expect("建超级表必须成功");

    let points = vec![
        TaosPoint::new(tag, timestamp_ns, "1.0", "1.1"),
        TaosPoint::new(tag, timestamp_ns + 1_000_000_000, "1.2", "1.3"),
    ];
    pool.write_batch(stable, &points)
        .await
        .expect("批量写入必须成功");

    let rows = pool
        .query_series(
            stable,
            timestamp_ns - 1_000_000_000,
            timestamp_ns + 5_000_000_000,
        )
        .await
        .expect("查询必须成功");
    assert_eq!(rows.len(), 2, "写回的 2 行必须原样可查");
    assert_eq!(rows[0].timestamp_ns, timestamp_ns);
    assert_eq!(rows[0].tag_value, tag);
    assert_eq!(
        rows[0].values,
        ["1.0".to_owned(), "1.1".to_owned()],
        "查询值必须与写入值一致"
    );
    assert_eq!(rows[1].values, ["1.2".to_owned(), "1.3".to_owned()]);

    // 清理：先删子表（名称由 tag 十六进制编码生成，故按 information_schema 实际列举删除），
    // 再删超级表，最后断言无残留。
    let children = pool
        .query(&format!(
            "SELECT table_name FROM information_schema.ins_tables WHERE stable_name='{stable}'"
        ))
        .await
        .expect("列举子表必须成功");
    for row in &children.rows {
        if let Some(name) = row.first() {
            pool.exec(&format!("DROP TABLE IF EXISTS `{name}`"))
                .await
                .expect("删子表必须成功");
        }
    }
    pool.exec(&format!("DROP STABLE IF EXISTS `{stable}`"))
        .await
        .expect("删超级表必须成功");
    let remaining = pool
        .query(&format!(
            "SELECT count(*) FROM information_schema.ins_tables WHERE stable_name='{stable}'"
        ))
        .await
        .expect("清理确认查询必须成功");
    assert_eq!(
        remaining
            .rows
            .first()
            .and_then(|row| row.first())
            .map(String::as_str),
        Some("0"),
        "清理后不得残留超级表 {stable} 及其子表"
    );
}

/// 主测：原生 TCP 端口可达 + 原生 WebSocket 握手 + 双传输池的数据面往返。
#[tokio::test]
#[ignore = "需要真实 TDengine（原生 6030 / taosAdapter 6041）与 FOUNDATIONX_TAOSX_* 环境变量"]
async fn live_taos_native_ws_roundtrip() {
    // 配置只来自环境变量；缺失或非法时给出可操作的提示，不打印任何取值。
    let env_config = TaosConfig::from_env()
        .expect("必须能从 FOUNDATIONX_TAOSX_* 读取配置：请先 source taosx.env 或 taosx-rest.env");
    let host = env_config.host.clone();

    // 1) 原生协议端口可达性（6030；不发送握手帧）。
    probe_native_tcp(&env_config, NATIVE_TCP_PORT)
        .await
        .unwrap_or_else(|error| {
            panic!("原生端口 {host}:{NATIVE_TCP_PORT} 不可达（检查 tdengine 服务）: {error}")
        });

    // 2) NativeWs 模式：`connect` 内部先做一次 WS 握手探测。
    let mut native_config = env_config.clone();
    native_config.transport = TransportMode::NativeWs;
    native_config.port = adapter_port(env_config.port);
    native_config.timeout = Duration::from_secs(15);
    let pool = TaosPool::connect(native_config.clone())
        .await
        .unwrap_or_else(|error| {
            panic!(
                "NativeWs 建连失败（taosAdapter {host}:{}）: {error}",
                native_config.port
            )
        });
    assert_eq!(pool.config().transport, TransportMode::NativeWs);
    assert_eq!(pool.stats().in_flight, 0);

    assert_healthy(&pool).await;

    // WS 短会话：`exec_sql_ws` 的契约是「读到服务端首帧」，此处只断言确实收到了帧。
    let frame = pool
        .exec_sql_ws("SELECT SERVER_VERSION()")
        .await
        .expect("WS 短会话必须收到服务端首帧");
    assert!(!frame.trim().is_empty(), "WS 首帧不得为空");

    // 3) 数据面往返（NativeWs 模式下 SQL 仍走 REST，见 docs/API.md 能力边界）。
    let stable = unique_id("infra_draft_taos");
    let tag = unique_tag();
    data_plane_roundtrip(&pool, &stable, &tag, aligned_now_ns()).await;

    // 4) 收尾。
    pool.close().await.expect("close 必须成功");
    assert!(pool.is_closed(), "close 后 is_closed 必须为 true");
}

/// 补测：REST 传输（taosAdapter 6041）的建连、探活与数据面往返。
#[tokio::test]
#[ignore = "需要真实 TDengine REST（taosAdapter 6041）与 FOUNDATIONX_TAOSX_* 环境变量"]
async fn live_taos_rest_roundtrip() {
    let env_config = TaosConfig::from_env()
        .expect("必须能从 FOUNDATIONX_TAOSX_* 读取配置：请先 source taosx-rest.env");
    let host = env_config.host.clone();

    let mut rest_config = env_config.clone();
    rest_config.transport = TransportMode::Rest;
    rest_config.port = adapter_port(env_config.port);
    rest_config.timeout = Duration::from_secs(15);
    let pool = TaosPool::connect(rest_config.clone())
        .await
        .unwrap_or_else(|error| {
            panic!(
                "REST 建连失败（taosAdapter {host}:{}）: {error}",
                rest_config.port
            )
        });
    assert_eq!(pool.config().transport, TransportMode::Rest);

    assert_healthy(&pool).await;

    let stable = unique_id("infra_draft_taosr");
    let tag = unique_tag();
    data_plane_roundtrip(&pool, &stable, &tag, aligned_now_ns()).await;

    pool.close().await.expect("close 必须成功");
    assert!(pool.is_closed(), "close 后 is_closed 必须为 true");
}
