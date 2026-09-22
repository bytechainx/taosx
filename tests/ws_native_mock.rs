#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! 原生 WebSocket 层离线 mock 测试（P1-5 覆盖补全）。
//!
//! 协议为**两步**：先 `{"action":"conn",…}` 建会话，服务端回 `code == 0` 后才发
//! `{"action":"query",…}`。本文件的 mock 按「一次客户端请求对应一批响应帧」驱动，
//! 覆盖 Text 帧解析、Binary 帧解码与**两阶段各自**的非数据帧 fail-closed。
//! 握手与错误映射的细分用例（非 0 code / 畸形状态字段 / P5 对照）见
//! `tests/ws_conn_handshake.rs`。

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

use taosx::{exec_sql_ws, TaosConfig, TaosError, TransportMode};

/// `conn` 握手成功响应帧（`code == 0`）。
fn conn_ok_frame() -> Message {
    Message::Text(r#"{"code":0,"message":"","action":"conn","req_id":0,"timing":2576500}"#.into())
}

/// 启动本地 WS mock server：每收到一个客户端数据帧，就按序发送 `batches[i]`。
async fn start_ws_mock(batches: Vec<Vec<Message>>) -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(async move {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let Ok(mut ws) = accept_async(stream).await else {
            return;
        };
        let mut index = 0usize;
        while let Some(Ok(message)) = ws.next().await {
            if !(message.is_text() || message.is_binary()) {
                // Ping/Pong/Close 由 tungstenite 自动处理或已终止会话。
                continue;
            }
            let Some(replies) = batches.get(index) else {
                break;
            };
            for reply in replies {
                if ws.send(reply.clone()).await.is_err() {
                    break;
                }
            }
            index += 1;
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

/// Text 帧：`conn`、`query` 均回 Text 帧，exec_sql_ws 应返回 query 帧内容。
#[tokio::test]
async fn ws_text_frame_returns_body() {
    let query_text =
        r#"{"code":0,"action":"query","id":1,"fields_names":["server_version()"],"rows":1}"#;
    let port = start_ws_mock(vec![
        vec![conn_ok_frame()],
        vec![Message::Text(query_text.into())],
    ])
    .await;
    let config = ws_config(port);

    let body = exec_sql_ws(&config, "SELECT SERVER_VERSION()")
        .await
        .expect("Text 帧必须返回成功");

    assert_eq!(
        body, query_text,
        "exec_sql_ws 返回的 Text 帧内容必须与 mock 的 query 响应一致"
    );
}

/// Binary 帧：`conn` 回 Text、`query` 回 Binary，exec_sql_ws 应返回解码后的文本。
#[tokio::test]
async fn ws_binary_frame_returns_decoded_body() {
    let query_text =
        r#"{"code":0,"action":"query","id":1,"fields_names":["server_version()"],"rows":1}"#;
    let binary_bytes = query_text.as_bytes().to_vec();
    let port = start_ws_mock(vec![
        vec![conn_ok_frame()],
        vec![Message::Binary(binary_bytes.into())],
    ])
    .await;
    let config = ws_config(port);

    let body = exec_sql_ws(&config, "SELECT SERVER_VERSION()")
        .await
        .expect("Binary 帧必须返回成功");

    assert_eq!(
        body, query_text,
        "exec_sql_ws 必须正确解码 Binary 帧的 UTF-8 内容"
    );
}

/// 非数据帧 fail-closed（握手阶段）：`conn` 响应不是 Text/Binary，必须返回 Err。
#[tokio::test]
async fn ws_non_data_frame_fails_closed() {
    let port = start_ws_mock(vec![vec![Message::Close(None)]]).await;
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

/// Ping 帧（非 Text/Binary 非 Close）在握手阶段也应 fail-closed。
#[tokio::test]
async fn ws_ping_frame_fails_closed() {
    let port = start_ws_mock(vec![vec![Message::Ping(vec![].into())]]).await;
    let config = ws_config(port);

    let error = exec_sql_ws(&config, "SELECT 1")
        .await
        .expect_err("Ping 帧必须 fail-closed");

    assert!(
        matches!(error, TaosError::Unavailable(_)),
        "Ping 帧响应必须映射为 Unavailable: {error:?}"
    );
}

/// 完成 WS 握手后立即断开、不发送任何数据帧：mock 在握手响应前结束连接。
async fn start_ws_mock_closing_without_frame() -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(async move {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let Ok(ws) = accept_async(stream).await else {
            return;
        };
        // 直接丢弃：不发 Close 帧、也不发任何 Text/Binary 帧。
        drop(ws);
    });
    port
}

/// 连接在握手响应前即结束（无任何帧）⇒ Err，不得伪造成功。
#[tokio::test]
async fn ws_stream_ends_before_handshake_fails_closed() {
    let port = start_ws_mock_closing_without_frame().await;
    let config = ws_config(port);

    let error = exec_sql_ws(&config, "SELECT 1")
        .await
        .expect_err("握手前流结束必须 fail-closed");

    assert!(
        matches!(error, TaosError::Unavailable(_)),
        "流提前结束必须映射为 Unavailable: {error:?}"
    );
}
