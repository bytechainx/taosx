#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! 原生 WS `conn` 会话握手的离线用例（阶段 1）。
//!
//! TDengine 的 `/rest/ws` 是**两步协议**：先发 `{"action":"conn",…}` 建会话，服务端回
//! `code == 0` 后才接受 `{"action":"query",…}`；未握手时服务端对所有请求回
//! `code:65535`（"server not connected"）。本文件用本地 accept 侧 mock（无需新依赖）
//! 覆盖四类判据：
//!
//! 1. 握手成功（`conn` `code 0` → `query` `code 0`）⇒ `Ok`，返回 query 元数据帧；
//! 2. 握手失败（`conn` 非 0）⇒ `Err`，且**不得**继续发 query；
//! 3. 响应 `code` 非 0 ⇒ `Err`（原实现把 `code:65535` 信封当成功返回 = fail-open 缺陷）；
//! 4. 状态字段缺失/畸形（非 JSON / 无 `code` / `code` 非整数 / 非数据帧）⇒ 一律 `Err`。
//!
//! 另含 P5 必红对照：未握手场景的 65535 信封必须失败。
//!
//! **边界（阶段 1）**：只校验到 `query` 的元数据响应帧；结果行需另发 `fetch`，本阶段
//! 未实现，用例不断言任何结果行语义。

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

use taosx::{exec_sql_ws, TaosConfig, TaosError, TransportMode};

/// 被查询的探针 SQL（与立项报告 P1/P3 一致）。
const SERVER_VERSION_QUERY: &str = "SELECT SERVER_VERSION()";

/// mock 记录的客户端请求文本（按到达顺序）。
type Requests = Arc<Mutex<Vec<String>>>;

/// `conn` 握手成功响应帧（P2 形态；响应内不含 token）。
fn conn_ok_frame() -> Message {
    Message::Text(r#"{"code":0,"message":"","action":"conn","req_id":0,"timing":2576500}"#.into())
}

/// `query` 成功响应帧（P3 形态：含字段元数据，`id` 为会话内递增序号）。
fn query_ok_frame() -> Message {
    Message::Text(
        r#"{"code":0,"message":"","action":"query","id":1,"is_update":false,"affected_rows":0,"fields_count":1,"fields_names":["server_version()"],"fields_types":[8],"fields_lengths":[7],"precision":0}"#
            .into(),
    )
}

/// `code:65535` 错误信封（P1/P5 形态）。
fn not_connected_frame() -> Message {
    Message::Text(
        r#"{"code":65535,"message":"server not connected","action":"query","req_id":0,"timing":30586}"#
            .into(),
    )
}

/// 启动本地 WS mock server：每收到一个客户端数据帧，就发送 `batches[i]` 这一批响应帧；
/// 同时把收到的请求文本按序记录到返回值里（供用例断言「实现真的发了什么」）。
async fn start_ws_mock(batches: Vec<Vec<Message>>) -> (u16, Requests) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let seen: Requests = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
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
            if let Ok(text) = message.to_text() {
                sink.lock().expect("lock").push(text.to_owned());
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
    (port, seen)
}

/// WS 配置（NativeWs + 显式凭据 + 短超时，便于离线 mock）。
///
/// 口令是**测试夹具**（非真实凭据），用于断言「凭据取自配置」且「错误消息不含口令」。
fn ws_config(port: u16) -> TaosConfig {
    TaosConfig {
        host: "127.0.0.1".into(),
        port,
        user: "root".into(),
        password: "fixture-ws-password".into(),
        transport: TransportMode::NativeWs,
        timeout: Duration::from_secs(2),
        ..TaosConfig::default()
    }
}

/// 取出并解析 mock 记录到的请求文本。
fn json_requests(seen: &Requests) -> Vec<serde_json::Value> {
    seen.lock()
        .expect("lock")
        .iter()
        .map(|text| serde_json::from_str(text).expect("客户端请求必须是合法 JSON"))
        .collect()
}

/// ① 握手成功：`conn` `code 0` → `query` `code 0` ⇒ `Ok`，且返回 query 元数据帧；
/// 同时断言实现真的按「conn → query」顺序发送、且凭据取自配置。
#[tokio::test]
async fn conn_handshake_then_query_returns_metadata() {
    let (port, seen) = start_ws_mock(vec![vec![conn_ok_frame()], vec![query_ok_frame()]]).await;
    let config = ws_config(port);

    let body = exec_sql_ws(&config, SERVER_VERSION_QUERY)
        .await
        .expect("conn code0 + query code0 必须成功");

    assert!(
        body.contains("\"fields_names\""),
        "返回体必须是 query 元数据帧: {body}"
    );
    assert!(
        body.contains("server_version()"),
        "返回体必须含 query 字段名: {body}"
    );

    let requests = json_requests(&seen);
    assert_eq!(
        requests.len(),
        2,
        "必须先 conn 后 query 各一次: {requests:?}"
    );
    assert_eq!(requests[0]["action"], "conn", "第一帧必须是 conn 握手");
    assert_eq!(requests[0]["args"]["user"], config.user);
    assert_eq!(
        requests[0]["args"]["password"], config.password,
        "握手凭据必须取自配置"
    );
    assert_eq!(requests[1]["action"], "query", "第二帧必须是 query");
    assert_eq!(requests[1]["args"]["sql"], SERVER_VERSION_QUERY);
}

/// ② 握手失败：`conn` 回非 0 ⇒ `Err`，且不得继续发 query，错误消息不得含口令。
#[tokio::test]
async fn conn_rejected_maps_to_err_without_leaking_password() {
    let (port, seen) = start_ws_mock(vec![vec![Message::Text(
        r#"{"code":65535,"message":"authentication failed","action":"conn","req_id":0}"#.into(),
    )]])
    .await;
    let config = ws_config(port);

    let error = exec_sql_ws(&config, SERVER_VERSION_QUERY)
        .await
        .expect_err("conn 非 0 code 必须失败");

    assert!(
        matches!(error, TaosError::Unavailable(_)),
        "conn 失败必须映射为 Unavailable: {error:?}"
    );
    let rendered = error.to_string();
    assert!(
        rendered.contains("conn"),
        "错误消息必须点明握手阶段: {rendered}"
    );
    assert!(
        !rendered.contains(&config.password),
        "错误消息不得含口令: {rendered}"
    );
    assert_eq!(json_requests(&seen).len(), 1, "握手失败后不得继续发 query");
}

/// ③ fail-open 必红对照：`query` 回 `code:65535` 的信封时，实现必须返回 `Err`。
///
/// 修复前该路径返回 `Ok(错误信封)`——正是本阶段要修掉的语义级 fail-open。
#[tokio::test]
async fn query_nonzero_code_is_error_not_success() {
    let (port, _seen) =
        start_ws_mock(vec![vec![conn_ok_frame()], vec![not_connected_frame()]]).await;
    let config = ws_config(port);

    let error = exec_sql_ws(&config, SERVER_VERSION_QUERY)
        .await
        .expect_err("query 非 0 code 必须失败（不得返回 Ok）");

    assert!(
        matches!(error, TaosError::Unavailable(_)),
        "非 0 code 必须映射为 Unavailable: {error:?}"
    );
    assert!(
        error.to_string().contains("65535"),
        "错误消息必须含状态码: {error}"
    );
}

/// ④-a P5 必红对照：模拟「未握手」的服务端行为（首帧即 65535 信封），必须失败。
#[tokio::test]
async fn not_connected_envelope_must_fail() {
    let (port, seen) = start_ws_mock(vec![
        vec![not_connected_frame()],
        vec![not_connected_frame()],
    ])
    .await;

    let error = exec_sql_ws(&ws_config(port), SERVER_VERSION_QUERY)
        .await
        .expect_err("P5 信封（server not connected）必须失败，不得当作成功");

    assert!(
        matches!(error, TaosError::Unavailable(_)),
        "P5 信封必须映射为 Unavailable: {error:?}"
    );
    assert_eq!(
        json_requests(&seen).len(),
        1,
        "首帧即失败后不得继续发 query"
    );
}

/// ④-b 非 JSON 帧 ⇒ `Err`（不得把任意文本当成功返回）。
#[tokio::test]
async fn non_json_frame_fails_closed() {
    let (port, _seen) = start_ws_mock(vec![vec![Message::Text("not json at all".into())]]).await;

    let error = exec_sql_ws(&ws_config(port), SERVER_VERSION_QUERY)
        .await
        .expect_err("非 JSON 帧必须失败");

    assert!(
        matches!(error, TaosError::Unavailable(_)),
        "非 JSON 帧必须映射为 Unavailable: {error:?}"
    );
}

/// ④-c JSON 合法但缺 `code` 字段 ⇒ `Err`（不得因「无错误码」而默认成功）。
#[tokio::test]
async fn frame_without_code_field_fails_closed() {
    let (port, _seen) = start_ws_mock(vec![vec![Message::Text(
        r#"{"action":"conn","req_id":0,"timing":2576500}"#.into(),
    )]])
    .await;

    let error = exec_sql_ws(&ws_config(port), SERVER_VERSION_QUERY)
        .await
        .expect_err("缺 code 字段必须失败");

    assert!(
        matches!(error, TaosError::Unavailable(_)),
        "缺 code 字段必须映射为 Unavailable: {error:?}"
    );
    assert!(
        error.to_string().contains("code"),
        "错误消息必须点明缺 code: {error}"
    );
}

/// ④-d `code` 存在但非整数（字符串 `"0"`）⇒ `Err`。
#[tokio::test]
async fn non_integer_code_fails_closed() {
    let (port, _seen) = start_ws_mock(vec![vec![Message::Text(
        r#"{"code":"0","action":"conn"}"#.into(),
    )]])
    .await;

    let error = exec_sql_ws(&ws_config(port), SERVER_VERSION_QUERY)
        .await
        .expect_err("code 非整数必须失败");

    assert!(
        matches!(error, TaosError::Unavailable(_)),
        "code 非整数必须映射为 Unavailable: {error:?}"
    );
}

/// ④-e query 阶段的元数据帧缺 `code` ⇒ `Err`（缺失判定对两步都生效）。
#[tokio::test]
async fn query_frame_without_code_fails_closed() {
    let (port, _seen) = start_ws_mock(vec![
        vec![conn_ok_frame()],
        vec![Message::Text(
            r#"{"action":"query","id":1,"fields_names":["server_version()"]}"#.into(),
        )],
    ])
    .await;

    let error = exec_sql_ws(&ws_config(port), SERVER_VERSION_QUERY)
        .await
        .expect_err("query 帧缺 code 必须失败");

    assert!(
        matches!(error, TaosError::Unavailable(_)),
        "query 帧缺 code 必须映射为 Unavailable: {error:?}"
    );
}

/// ④-f query 阶段收到非数据帧（Ping）⇒ `Err`（fail-closed 对两步都生效）。
#[tokio::test]
async fn query_stage_non_data_frame_fails_closed() {
    let (port, _seen) = start_ws_mock(vec![
        vec![conn_ok_frame()],
        vec![Message::Ping(Vec::new().into())],
    ])
    .await;

    let error = exec_sql_ws(&ws_config(port), SERVER_VERSION_QUERY)
        .await
        .expect_err("query 阶段非数据帧必须 fail-closed");

    assert!(
        matches!(error, TaosError::Unavailable(_)),
        "query 阶段非数据帧必须映射为 Unavailable: {error:?}"
    );
    assert!(
        error.to_string().contains("ws"),
        "错误消息必须含上下文 'ws': {error}"
    );
}
