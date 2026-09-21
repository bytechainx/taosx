#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! 不可达地址的失败路径：`ping` / `connect` / 原生 WS 必须返回 `Err`，`health_check` 给出未就绪信封。

use std::time::Duration;

use taosx::{
    connect_native_ws, exec_sql_ws, probe_native_tcp, validate_mode, TaosConfig, TaosError,
    TaosPool, TransportMode,
};

/// 必然被拒绝的地址（端口 1 上没有监听者）。
fn unreachable_config() -> TaosConfig {
    TaosConfig {
        host: "127.0.0.1".to_owned(),
        port: 1,
        database: String::new(),
        timeout: Duration::from_millis(300),
        acquire_timeout: Duration::from_millis(300),
        ..TaosConfig::default()
    }
}

#[tokio::test]
async fn ping_on_unreachable_address_returns_error() {
    let pool = TaosPool::new(unreachable_config()).expect("离线构造");
    let error = pool.ping().await.expect_err("不可达地址 ping 必须失败");
    assert!(error.is_retryable(), "连接失败必须可重试: {error:?}");
    assert!(
        matches!(error, TaosError::Connection(_) | TaosError::Timeout(_)),
        "{error:?}"
    );
    assert!(pool.metrics().ping_err >= 1);
    assert!(pool.liveness(), "本地池仍应处于 open 状态");
}

#[tokio::test]
async fn connect_on_unreachable_address_returns_error() {
    let error = TaosPool::connect(unreachable_config())
        .await
        .expect_err("connect 必须失败");
    assert!(error.is_retryable(), "{error:?}");

    // 备用主机全部不可用时同样 fail-closed。
    let config = TaosConfig {
        hosts: vec!["127.0.0.1".to_owned()],
        ..unreachable_config()
    };
    assert!(TaosPool::connect(config).await.is_err());
}

#[tokio::test]
async fn exec_and_query_on_unreachable_address_return_error() {
    let pool = TaosPool::new(unreachable_config()).expect("离线构造");
    assert!(pool.exec("SELECT SERVER_VERSION()").await.is_err());
    assert!(pool.query("SELECT 1").await.is_err());
    assert!(pool.metrics().sql_err >= 2);
}

#[tokio::test]
async fn health_check_wraps_unreachable_as_not_ready() {
    let pool = TaosPool::new(unreachable_config()).expect("离线构造");
    let health = pool
        .health_check()
        .await
        .expect("health_check 必须返回信封而非 Err");
    assert!(!health.ready);
    assert!(!health.is_ready());
    assert!(health.server_version.is_none());
    assert!(!health.detail.is_empty());
    assert!(!health.detail.contains("SELECT"), "详情不得包含 SQL 片段");
    assert!(pool.metrics().health_not_ready >= 1);
}

#[tokio::test]
async fn closed_pool_reports_closed_error() {
    let pool = TaosPool::new(TaosConfig {
        database: String::new(),
        ..TaosConfig::default()
    })
    .expect("离线构造");
    pool.close().await.expect("close");
    assert!(pool.is_closed());
    assert!(!pool.liveness());
    let error = pool.ping().await.expect_err("已关闭必须拒绝");
    assert!(matches!(error, TaosError::Closed(_)), "{error:?}");
    assert!(!error.is_retryable(), "已关闭不可重试");
}

#[tokio::test]
async fn native_ws_helpers_fail_closed_on_unreachable_address() {
    let config = TaosConfig {
        transport: TransportMode::NativeWs,
        ..unreachable_config()
    };
    validate_mode(&config).expect("模式合法");
    assert!(build_url(&config).ends_with(":1/rest/ws"));

    let error = connect_native_ws(&config)
        .await
        .expect_err("WS 握手必须失败");
    assert!(error.is_retryable(), "{error:?}");

    let error = exec_sql_ws(&config, "SELECT 1")
        .await
        .expect_err("WS SQL 必须失败");
    assert!(error.is_retryable(), "{error:?}");

    let error = probe_native_tcp(&config, 1)
        .await
        .expect_err("原生端口探测必须失败");
    assert!(error.is_retryable(), "{error:?}");

    // Rest 模式下调用原生连接必须显式拒绝。
    let rest = TaosConfig {
        transport: TransportMode::Rest,
        ..unreachable_config()
    };
    let error = connect_native_ws(&rest)
        .await
        .expect_err("Rest 模式必须拒绝");
    assert!(matches!(error, TaosError::Config(_)), "{error:?}");
}

fn build_url(config: &TaosConfig) -> String {
    taosx::build_native_ws_url(config)
}
