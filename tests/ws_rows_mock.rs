#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! `exec_sql_ws_rows` 离线 mock 端到端测试（特性 009 T2 / T4 / T5）。
//!
//! mock 服务端记录客户端发出的每个 JSON 请求，并按 action 回帧（回显 `req_id`）。
//! JSON 响应帧均为合成数据（顶层 `_synthetic: true` + `_note`）；`fetch_block` 成功帧
//! 直接使用真实夹具 `tests/fixtures/fetch-block-20261012.bin`（`_synthetic: false`）。
//! `free_result` 一律不回帧，用以证明客户端不等待其响应。

use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

use taosx::{
    exec_sql_ws_rows, TaosConfig, TaosError, TransportMode, TsPrecision, WsValue,
};

const NOTE: &str = "离线 mock 合成帧，仅验证 fetch / fetch_block / free_result 状态机";

/// 真实 fetch_block 二进制帧（头内 id = 1）。
fn real_block_frame() -> Vec<u8> {
    std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/fetch-block-20261012.bin"
    ))
    .expect("读取真实夹具")
}

/// 按 action 回帧的脚本；第二个参数为该 action 第几次出现（从 0 起）。
type Responder = Box<dyn FnMut(&str, u64, usize) -> Vec<Message> + Send>;

/// 启动 mock：返回端口与「客户端请求记录」句柄（客户端关闭连接后结束）。
async fn start_rows_mock(mut respond: Responder) -> (u16, JoinHandle<Vec<Value>>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let handle = tokio::spawn(async move {
        let mut recorded = Vec::new();
        let Ok((stream, _)) = listener.accept().await else {
            return recorded;
        };
        let Ok(mut ws) = accept_async(stream).await else {
            return recorded;
        };
        let mut counts = std::collections::HashMap::<String, usize>::new();
        while let Some(Ok(message)) = ws.next().await {
            let Message::Text(text) = message else {
                continue;
            };
            let request: Value = serde_json::from_str(text.as_str()).expect("客户端请求为 JSON");
            let action = request["action"].as_str().expect("action").to_owned();
            let req_id = request["args"]["req_id"].as_u64().expect("每个请求都带 req_id");
            let count = counts.entry(action.clone()).or_insert(0);
            let replies = respond(&action, req_id, *count);
            *count += 1;
            recorded.push(request);
            for reply in replies {
                if ws.send(reply).await.is_err() {
                    break;
                }
            }
        }
        recorded
    });
    (port, handle)
}

fn ws_config(port: u16) -> TaosConfig {
    TaosConfig {
        host: "127.0.0.1".into(),
        port,
        transport: TransportMode::NativeWs,
        timeout: Duration::from_secs(2),
        ..TaosConfig::default()
    }
}

fn text(value: Value) -> Vec<Message> {
    vec![Message::Text(value.to_string().into())]
}

fn conn_ok(req_id: u64) -> Vec<Message> {
    text(json!({
        "_synthetic": true, "_note": NOTE,
        "code": 0, "message": "", "action": "conn", "req_id": req_id, "timing": 1
    }))
}

/// 与真实夹具一致的 query 元数据（`select server_version(),now(),1,1.5,'a'`）。
fn query_ok(req_id: u64) -> Vec<Message> {
    text(json!({
        "_synthetic": true, "_note": NOTE,
        "code": 0, "message": "", "action": "query", "req_id": req_id, "timing": 1,
        "id": 1, "is_update": false, "affected_rows": 0, "fields_count": 5,
        "fields_names": ["server_version()", "now()", "1", "1.5", "'a'"],
        "fields_types": [8, 9, 5, 7, 8],
        "fields_lengths": [72, 8, 8, 8, 3],
        "precision": 0
    }))
}

fn fetch_rows(req_id: u64, rows: u64) -> Vec<Message> {
    text(json!({
        "_synthetic": true, "_note": NOTE,
        "code": 0, "message": "", "action": "fetch", "req_id": req_id, "timing": 1,
        "id": 1, "completed": false, "lengths": [9, 8, 8, 8, 3], "rows": rows
    }))
}

fn fetch_completed(req_id: u64) -> Vec<Message> {
    text(json!({
        "_synthetic": true, "_note": NOTE,
        "code": 0, "message": "", "action": "fetch", "req_id": req_id, "timing": 1,
        "id": 1, "completed": true
    }))
}

fn error_frame(action: &str, req_id: u64, code: u64) -> Vec<Message> {
    text(json!({
        "_synthetic": true, "_note": NOTE,
        "code": code, "message": "synthetic-server-message", "action": action,
        "req_id": req_id, "timing": 1
    }))
}

/// 正常路径的回帧脚本：一块数据 + 第二次 fetch 回 completed。
fn happy_path(action: &str, req_id: u64, nth: usize) -> Vec<Message> {
    match (action, nth) {
        ("conn", _) => conn_ok(req_id),
        ("query", _) => query_ok(req_id),
        ("fetch", 0) => fetch_rows(req_id, 1),
        ("fetch", _) => fetch_completed(req_id),
        ("fetch_block", _) => vec![Message::Binary(real_block_frame().into())],
        // free_result：服务端不回任何帧（03a §3）。
        _ => Vec::new(),
    }
}

fn actions(recorded: &[Value]) -> Vec<String> {
    recorded
        .iter()
        .map(|request| request["action"].as_str().unwrap().to_owned())
        .collect()
}

async fn recorded(handle: JoinHandle<Vec<Value>>) -> Vec<Value> {
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("客户端必须关闭连接")
        .expect("mock 任务")
}

/// conn → query → fetch → fetch_block（真实块）→ fetch(completed)：读出完整行，
/// 且 completed 后不发 free_result。
#[tokio::test]
async fn rows_read_until_completed_without_free_result() {
    let (port, handle) = start_rows_mock(Box::new(happy_path)).await;
    let result = exec_sql_ws_rows(&ws_config(port), "select server_version(),now(),1,1.5,'a'")
        .await
        .expect("完整结果读取必须成功");

    assert_eq!(
        result.fields_names,
        vec!["server_version()", "now()", "1", "1.5", "'a'"]
    );
    assert_eq!(result.fields_types, vec![8, 9, 5, 7, 8]);
    assert_eq!(result.precision, TsPrecision::Ms);
    assert_eq!(
        result.rows,
        vec![vec![
            WsValue::Text("3.3.6.0".to_owned()),
            WsValue::Timestamp {
                value: 1_791_777_705_997,
                precision: TsPrecision::Ms,
            },
            WsValue::Int(1),
            WsValue::Double(1.5),
            WsValue::Text("a".to_owned()),
        ]]
    );

    let requests = recorded(handle).await;
    assert_eq!(
        actions(&requests),
        vec!["conn", "query", "fetch", "fetch_block", "fetch"],
        "completed:true 后服务端已释放，不得再发 free_result"
    );
    // req_id 客户端生成且互不重复；fetch / fetch_block 以结果集句柄 id 路由。
    let mut req_ids: Vec<u64> = requests
        .iter()
        .map(|request| request["args"]["req_id"].as_u64().unwrap())
        .collect();
    req_ids.sort_unstable();
    req_ids.dedup();
    assert_eq!(req_ids.len(), requests.len());
    for request in &requests[2..] {
        assert_eq!(request["args"]["id"], json!(1));
    }
}

/// fetch_block 回 Text JSON 错误帧：按错误处理（不当二进制块解析），服务端消息不外泄。
#[tokio::test]
async fn fetch_block_text_error_frame_is_error() {
    let (port, handle) = start_rows_mock(Box::new(|action: &str, req_id: u64, nth: usize| match action {
        "fetch_block" => error_frame("fetch_block", req_id, 65535),
        _ => happy_path(action, req_id, nth),
    }))
    .await;
    let error = exec_sql_ws_rows(&ws_config(port), "select 1")
        .await
        .expect_err("fetch_block 错误帧必须报错");
    assert!(matches!(error, TaosError::Unavailable(_)), "{error:?}");
    let rendered = error.to_string();
    assert!(rendered.contains("fetch_block"), "{rendered}");
    assert!(rendered.contains("65535"), "{rendered}");
    assert!(!rendered.contains("synthetic-server-message"));

    // fetch_block 出错时服务端句柄状态未知：补发 free_result（对已释放句柄为 no-op），且不等待。
    let requests = recorded(handle).await;
    assert_eq!(
        actions(&requests),
        vec!["conn", "query", "fetch", "fetch_block", "free_result"]
    );
}

/// 本地中途放弃（超 max_query_rows）：发 free_result 且不等待响应（mock 永不回帧）。
#[tokio::test]
async fn abandon_sends_free_result_without_waiting() {
    let (port, handle) = start_rows_mock(Box::new(|action: &str, req_id: u64, nth: usize| match action {
        "fetch" => fetch_rows(req_id, 2),
        _ => happy_path(action, req_id, nth),
    }))
    .await;
    let config = TaosConfig {
        max_query_rows: 1,
        ..ws_config(port)
    };
    let started = Instant::now();
    let error = exec_sql_ws_rows(&config, "select 1")
        .await
        .expect_err("超行数必须报错");
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "free_result 不得等待响应"
    );
    assert!(matches!(error, TaosError::Unavailable(_)), "{error:?}");
    assert!(error.to_string().contains("max_query_rows"), "{error}");

    let requests = recorded(handle).await;
    assert_eq!(
        actions(&requests),
        vec!["conn", "query", "fetch", "free_result"]
    );
    assert_eq!(requests[3]["args"]["id"], json!(1));
}

/// fetch 出错：服务端已释放结果集，不得再发 free_result。
#[tokio::test]
async fn fetch_error_means_released_without_free_result() {
    let (port, handle) = start_rows_mock(Box::new(|action: &str, req_id: u64, nth: usize| match action {
        "fetch" => error_frame("fetch", req_id, 9732),
        _ => happy_path(action, req_id, nth),
    }))
    .await;
    let error = exec_sql_ws_rows(&ws_config(port), "select 1")
        .await
        .expect_err("fetch 错误必须报错");
    assert!(matches!(error, TaosError::Unavailable(_)), "{error:?}");
    assert!(error.to_string().contains("9732"), "{error}");
    let requests = recorded(handle).await;
    assert_eq!(actions(&requests), vec!["conn", "query", "fetch"]);
}

/// 二进制帧头内 id 与在途 fetch_block 的结果集句柄不符：路由失败，fail-closed。
#[tokio::test]
async fn binary_frame_with_unknown_id_is_rejected() {
    let (port, handle) = start_rows_mock(Box::new(|action: &str, req_id: u64, nth: usize| match action {
        "fetch_block" => {
            let mut frame = real_block_frame();
            frame[8..16].copy_from_slice(&2u64.to_le_bytes());
            vec![Message::Binary(frame.into())]
        }
        _ => happy_path(action, req_id, nth),
    }))
    .await;
    let error = exec_sql_ws_rows(&ws_config(port), "select 1")
        .await
        .expect_err("id 不符必须报错");
    assert!(matches!(error, TaosError::Unavailable(_)), "{error:?}");
    assert!(error.to_string().contains("id=2"), "{error}");
    let requests = recorded(handle).await;
    assert_eq!(actions(&requests).last().map(String::as_str), Some("free_result"));
}

/// fetch 响应的 req_id 回显与请求不配对：fail-closed。
#[tokio::test]
async fn fetch_reply_with_mismatched_req_id_is_rejected() {
    let (port, handle) = start_rows_mock(Box::new(|action: &str, req_id: u64, nth: usize| match action {
        "fetch" => fetch_rows(req_id + 100, 1),
        _ => happy_path(action, req_id, nth),
    }))
    .await;
    let error = exec_sql_ws_rows(&ws_config(port), "select 1")
        .await
        .expect_err("req_id 不配对必须报错");
    assert!(matches!(error, TaosError::Unavailable(_)), "{error:?}");
    assert!(error.to_string().contains("req_id"), "{error}");
    let _ = recorded(handle).await;
}

/// 真实块的 version 被改写（派生畸形样本，合成）：解码层明确报错并 free_result。
#[tokio::test]
async fn malformed_block_is_serialization_error() {
    let (port, handle) = start_rows_mock(Box::new(|action: &str, req_id: u64, nth: usize| match action {
        "fetch_block" => {
            let mut frame = real_block_frame();
            frame[16..20].copy_from_slice(&9u32.to_le_bytes());
            vec![Message::Binary(frame.into())]
        }
        _ => happy_path(action, req_id, nth),
    }))
    .await;
    let error = exec_sql_ws_rows(&ws_config(port), "select 1")
        .await
        .expect_err("version 不符必须报错");
    assert!(matches!(error, TaosError::Serialization(_)), "{error:?}");
    let requests = recorded(handle).await;
    assert_eq!(actions(&requests).last().map(String::as_str), Some("free_result"));
}

/// 非查询语句（is_update）：不发 fetch，返回零行。
#[tokio::test]
async fn update_statement_returns_empty_without_fetch() {
    let (port, handle) = start_rows_mock(Box::new(|action: &str, req_id: u64, _nth: usize| match action {
        "conn" => conn_ok(req_id),
        "query" => text(json!({
            "_synthetic": true, "_note": NOTE,
            "code": 0, "message": "", "action": "query", "req_id": req_id, "timing": 1,
            "id": 0, "is_update": true, "affected_rows": 1, "fields_count": 0,
            "fields_names": null, "fields_types": null, "fields_lengths": null, "precision": 0
        })),
        _ => Vec::new(),
    }))
    .await;
    let result = exec_sql_ws_rows(&ws_config(port), "insert into t values(now, 1)")
        .await
        .expect("写入语句成功");
    assert!(result.rows.is_empty());
    assert!(result.fields_types.is_empty());
    let requests = recorded(handle).await;
    assert_eq!(actions(&requests), vec!["conn", "query"]);
}
