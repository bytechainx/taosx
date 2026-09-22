#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! 原生 WebSocket 层离线 mock 测试（P1-5 覆盖补全）。
//!
//! 此前 WS 成功路径（Text/Binary 帧解析、非数据帧 fail-closed）仅被 `#[ignore]` live 用例
//! 行使，CI 从不执行。本文件使用 `tokio_tungstenite` 的 accept 侧建立本地 mock server，
//! **无需新依赖**，覆盖三分支。

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

use taosx::{exec_sql_ws, TaosConfig, TaosError, TransportMode};

/// 启动本地 WS mock server，接收客户端请求后按序发送 `responses` 帧。
async fn start_ws_mock(responses: Vec<Message>) -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut ws = accept_async(stream).await.expect("ws handshake");
        // 读取客户端请求帧（query 消息），忽略其内容。
        while let Some(Ok(msg)) = ws.next().await {
            if msg.is_text() || msg.is_binary() || msg.is_close() {
                break;
            }
            // Ping/Pong 由 tungstenite 自动处理，继续读取。
        }
        // 按序发送配置的响应帧。
        for msg in responses {
            let _ = ws.send(msg).await;
        }
        let _ = ws.close(None).await;
    });
    port
}

/// WS 配置（NativeWs + 短超时，便于离线 mock）。
fn ws_config(port: u16) -> TaosConfig {
    TaosConfig {
        host: "127.0.0.1".into(),
        port,
        transport: TransportMode::NativeWs,
        timeout: Duration::from_secs(2),
        ..TaosConfig::default()
    }
}

/// Text 帧：mock 返回 Text 帧，exec_sql_ws 应返回帧内容。
#[tokio::test]
async fn ws_text_frame_returns_body() {
    let response_text =
        r#"{"code":0,"column_meta":[["v","VARCHAR",32]],"data":[["3.3.6.13"]],"rows":1}"#;
    let port = start_ws_mock(vec![Message::Text(response_text.into())]).await;
    let config = ws_config(port);

    let body = exec_sql_ws(&config, "SELECT SERVER_VERSION()")
        .await
        .expect("Text 帧必须返回成功");

    assert_eq!(
        body, response_text,
        "exec_sql_ws 返回的 Text 帧内容必须与 mock 一致"
    );
}

/// Binary 帧：mock 返回 Binary 帧（UTF-8 可解码内容），exec_sql_ws 应返回解码后的文本。
#[tokio::test]
async fn ws_binary_frame_returns_decoded_body() {
    let response_text =
        r#"{"code":0,"column_meta":[["v","VARCHAR",32]],"data":[["3.3.6.13"]],"rows":1}"#;
    let binary_bytes = response_text.as_bytes().to_vec();
    let port = start_ws_mock(vec![Message::Binary(binary_bytes.into())]).await;
    let config = ws_config(port);

    let body = exec_sql_ws(&config, "SELECT SERVER_VERSION()")
        .await
        .expect("Binary 帧必须返回成功");

    assert_eq!(
        body, response_text,
        "exec_sql_ws 必须正确解码 Binary 帧的 UTF-8 内容"
    );
}

/// 非数据帧 fail-closed：mock 返回 Close 帧（非 Text/Binary），exec_sql_ws 必须返回 Err。
#[tokio::test]
async fn ws_non_data_frame_fails_closed() {
    let port = start_ws_mock(vec![Message::Close(None)]).await;
    let config = ws_config(port);

    let error = exec_sql_ws(&config, "SELECT 1")
        .await
        .expect_err("非数据帧必须 fail-closed");

    assert!(
        matches!(error, TaosError::Unavailable(_)),
        "非数据帧响应必须映射为 Unavailable: {error:?}"
    );
    assert!(
        error.to_string().contains("ws"),
        "错误消息必须包含上下文 'ws': {error}"
    );
}

/// Ping 帧（非 Text/Binary 非 Close）也应 fail-closed。
#[tokio::test]
async fn ws_ping_frame_fails_closed() {
    let port = start_ws_mock(vec![Message::Ping(vec![].into())]).await;
    let config = ws_config(port);

    let error = exec_sql_ws(&config, "SELECT 1")
        .await
        .expect_err("Ping 帧必须 fail-closed");

    assert!(
        matches!(error, TaosError::Unavailable(_)),
        "Ping 帧响应必须映射为 Unavailable: {error:?}"
    );
}
