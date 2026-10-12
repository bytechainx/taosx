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

use std::sync::{Arc, Mutex};
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
async fn start_ws_mock(batches: Vec<Vec<Message>>) -> (u16, Arc<Mutex<Vec<serde_json::Value>>>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&seen);
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
            let request: serde_json::Value =
                serde_json::from_str(message.to_text().expect("请求为文本")).expect("请求为 JSON");
            let req_id = request["args"]["req_id"].as_u64().expect("请求须含 req_id");
            recorded.lock().expect("轨迹锁").push(request);
            for reply in replies {
                let mut outgoing = reply.clone();
                let payload = match reply {
                    Message::Text(text) => {
                        serde_json::from_str::<serde_json::Value>(text.as_str()).ok()
                    }
                    Message::Binary(bytes) => {
                        serde_json::from_slice::<serde_json::Value>(bytes).ok()
                    }
                    _ => None,
                };
                if let Some(mut payload) = payload {
                    payload["req_id"] = req_id.into();
                    outgoing = match reply {
                        Message::Binary(_) => {
                            Message::Binary(payload.to_string().into_bytes().into())
                        }
                        _ => Message::Text(payload.to_string().into()),
                    };
                }
                if ws.send(outgoing).await.is_err() {
                    break;
                }
            }
            index += 1;
        }
        let _ = ws.close(None).await;
    });
    (port, seen)
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

/// Text 帧：`conn`、`query` 均回 Text 帧，exec_sql_ws 应保留元数据并返回完整行。
#[tokio::test]
async fn ws_text_frame_returns_body() {
    let query_text = r#"{"code":0,"action":"query","id":1,"is_update":false,"fields_count":1,"fields_names":["server_version()"],"fields_types":[4],"fields_lengths":[4],"precision":0,"unknown_metadata":{"text":"源字段","value":17}}"#;
    let (port, seen) = start_ws_mock(vec![
        vec![conn_ok_frame()],
        vec![Message::Text(query_text.into())],
        vec![Message::Text(
            r#"{"code":0,"action":"fetch","id":1,"completed":false,"rows":1,"lengths":[4]}"#.into(),
        )],
        vec![Message::Binary(
            vec![
                0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 42, 0, 0, 0, 1, 0, 0,
                0, 1, 0, 0, 0, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 0, 0, 4, 4, 0, 0, 0, 4, 0, 0, 0, 0,
                11, 0, 0, 0,
            ]
            .into(),
        )],
        vec![Message::Text(
            r#"{"code":0,"action":"fetch","id":1,"completed":true}"#.into(),
        )],
    ])
    .await;
    let config = ws_config(port);

    let body = exec_sql_ws(&config, "SELECT SERVER_VERSION()")
        .await
        .expect("Text 帧必须返回成功");

    let actual: serde_json::Value = serde_json::from_str(&body).expect("完整输出为 JSON");
    let expected: serde_json::Value = serde_json::from_str(query_text).expect("合成元数据为 JSON");
    for (key, value) in expected.as_object().expect("元数据对象") {
        assert_eq!(&actual[key], value, "源元数据字段必须保真：{key}");
    }
    assert_eq!(actual["data"], serde_json::json!([[11]]));
    let requests = seen.lock().expect("轨迹锁");
    assert_eq!(
        requests
            .iter()
            .map(|request| request["action"].as_str().expect("action"))
            .collect::<Vec<_>>(),
        ["conn", "query", "fetch", "fetch_block", "fetch"]
    );
}

/// Binary 帧：`conn` 回 Text、`query` 回 Binary，exec_sql_ws 应保留解码元数据并返回完整行。
#[tokio::test]
async fn ws_binary_frame_returns_decoded_body() {
    let query_text = r#"{"code":0,"action":"query","id":1,"is_update":false,"fields_count":1,"fields_names":["server_version()"],"fields_types":[4],"fields_lengths":[4],"precision":0,"unknown_metadata":{"text":"源字段","value":17}}"#;
    let binary_bytes = query_text.as_bytes().to_vec();
    let (port, seen) = start_ws_mock(vec![
        vec![conn_ok_frame()],
        vec![Message::Binary(binary_bytes.into())],
        vec![Message::Text(
            r#"{"code":0,"action":"fetch","id":1,"completed":false,"rows":1,"lengths":[4]}"#.into(),
        )],
        vec![Message::Binary(
            vec![
                0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 42, 0, 0, 0, 1, 0, 0,
                0, 1, 0, 0, 0, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 0, 0, 4, 4, 0, 0, 0, 4, 0, 0, 0, 0,
                11, 0, 0, 0,
            ]
            .into(),
        )],
        vec![Message::Text(
            r#"{"code":0,"action":"fetch","id":1,"completed":true}"#.into(),
        )],
    ])
    .await;
    let config = ws_config(port);

    let body = exec_sql_ws(&config, "SELECT SERVER_VERSION()")
        .await
        .expect("Binary 帧必须返回成功");

    let actual: serde_json::Value = serde_json::from_str(&body).expect("完整输出为 JSON");
    let expected: serde_json::Value = serde_json::from_str(query_text).expect("合成元数据为 JSON");
    for (key, value) in expected.as_object().expect("元数据对象") {
        assert_eq!(&actual[key], value, "源元数据字段必须保真：{key}");
    }
    assert_eq!(actual["data"], serde_json::json!([[11]]));
    let requests = seen.lock().expect("轨迹锁");
    assert_eq!(
        requests
            .iter()
            .map(|request| request["action"].as_str().expect("action"))
            .collect::<Vec<_>>(),
        ["conn", "query", "fetch", "fetch_block", "fetch"]
    );
}

#[tokio::test]
async fn ws_conn_binary_invalid_utf8_fails_closed() {
    let mut conn_bytes =
        br#"{"code":0,"action":"conn","message":"synthetic-conn-payload-"#.to_vec();
    conn_bytes.push(0xff);
    conn_bytes.extend_from_slice(br#""}"#);
    let query_text = r#"{"code":0,"action":"query","id":1,"is_update":false,"fields_count":1,"fields_names":["synthetic_int"],"fields_types":[4],"fields_lengths":[4],"precision":0}"#;
    let (port, _seen) = start_ws_mock(vec![
        vec![Message::Binary(conn_bytes.into())],
        vec![Message::Text(query_text.into())],
        vec![Message::Text(
            r#"{"code":0,"action":"fetch","id":1,"completed":false,"rows":1,"lengths":[4]}"#.into(),
        )],
        vec![Message::Binary(
            vec![
                0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 42, 0, 0, 0, 1, 0, 0,
                0, 1, 0, 0, 0, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 0, 0, 4, 4, 0, 0, 0, 4, 0, 0, 0, 0,
                11, 0, 0, 0,
            ]
            .into(),
        )],
        vec![Message::Text(
            r#"{"code":0,"action":"fetch","id":1,"completed":true}"#.into(),
        )],
    ])
    .await;
    let config = ws_config(port);

    let error = exec_sql_ws(&config, "SELECT 1")
        .await
        .expect_err("conn 的 Binary JSON 字符串含非法 UTF-8 时必须拒绝");

    assert!(matches!(error, TaosError::Unavailable(_)));
    let rendered = error.to_string();
    assert!(
        rendered.contains("UTF-8"),
        "必须明确拒绝非法编码：{rendered}"
    );
    assert!(!rendered.contains("synthetic-conn-payload"));
    assert!(!rendered.contains('\u{fffd}'));
}

#[tokio::test]
async fn ws_query_binary_invalid_utf8_fails_closed() {
    let mut query_bytes =
        br#"{"code":0,"action":"query","message":"synthetic-query-payload-"#.to_vec();
    query_bytes.push(0xff);
    query_bytes.extend_from_slice(br#""}"#);
    let (port, seen) = start_ws_mock(vec![
        vec![conn_ok_frame()],
        vec![Message::Binary(query_bytes.into())],
    ])
    .await;
    let config = ws_config(port);

    let error = exec_sql_ws(&config, "SELECT 1")
        .await
        .expect_err("query 的 Binary JSON 字符串含非法 UTF-8 时必须拒绝");

    assert!(matches!(error, TaosError::Unavailable(_)));
    let rendered = error.to_string();
    assert!(
        rendered.contains("UTF-8"),
        "必须明确拒绝非法编码：{rendered}"
    );
    assert_eq!(seen.lock().expect("轨迹锁")[1]["action"], "query");
    assert!(!rendered.contains("synthetic-query-payload"));
    assert!(!rendered.contains('\u{fffd}'));
}

/// 非数据帧 fail-closed（握手阶段）：`conn` 响应不是 Text/Binary，必须返回 Err。
#[tokio::test]
async fn ws_non_data_frame_fails_closed() {
    let (port, _seen) = start_ws_mock(vec![vec![Message::Close(None)]]).await;
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
    let (port, _seen) = start_ws_mock(vec![vec![Message::Ping(vec![].into())]]).await;
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
