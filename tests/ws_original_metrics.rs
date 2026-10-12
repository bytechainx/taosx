#![allow(clippy::expect_used, clippy::panic)]
//! 原入口真实离线会话的成功、转换拒绝与输出预算指标；独立二进制隔离全局计数。

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use taosx::{exec_sql_ws, ws_probe_totals, TaosConfig, TaosError, TransportMode};
use tokio::net::TcpListener;
use tokio::time::timeout;
use tokio_tungstenite::{accept_async, tungstenite::Message};

#[tokio::test]
async fn original_entry_counts_success_and_conversion_failures_once() {
    let cases = [
        (4_u8, 11_i32.to_le_bytes().to_vec(), false),
        (7, f64::INFINITY.to_le_bytes().to_vec(), true),
        (7, f64::NAN.to_le_bytes().to_vec(), true),
        (6, f32::NEG_INFINITY.to_le_bytes().to_vec(), true),
        (16, vec![255; 1000], true),
    ];
    for (kind, payload, must_fail) in cases {
        let mut column = Vec::new();
        if kind == 16 {
            column.extend_from_slice(&0_i32.to_le_bytes());
            column.extend_from_slice(&(payload.len() as u16).to_le_bytes());
        } else {
            column.push(0);
        }
        column.extend_from_slice(&payload);
        let column_length = if kind == 16 {
            payload.len() + 2
        } else {
            payload.len()
        };
        let mut raw = Vec::new();
        raw.extend_from_slice(&1_u32.to_le_bytes());
        raw.extend_from_slice(&(37_u32 + column.len() as u32).to_le_bytes());
        raw.extend_from_slice(&1_u32.to_le_bytes());
        raw.extend_from_slice(&1_u32.to_le_bytes());
        raw.extend_from_slice(&0x8000_0000_u32.to_le_bytes());
        raw.extend_from_slice(&0_u64.to_le_bytes());
        raw.push(kind);
        raw.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        raw.extend_from_slice(&(column_length as u32).to_le_bytes());
        raw.extend_from_slice(&column);
        let mut frame = Vec::new();
        frame.extend_from_slice(&0_u64.to_le_bytes());
        frame.extend_from_slice(&71_u64.to_le_bytes());
        frame.extend_from_slice(&raw);
        assert!(frame.len() < 2048);
        if kind == 16 {
            assert!(json!({"data":[[payload.clone()]]}).to_string().len() > 2048);
        }
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("绑定离线 loopback");
        let port = listener.local_addr().expect("随机端口").port();
        let mut mock = tokio::spawn(async move {
            let outcome = timeout(Duration::from_secs(4), async {
                let (stream, _) = listener.accept().await.expect("接受会话");
                let mut socket = accept_async(stream).await.expect("WS 握手");
                let mut requests = Vec::new();
                let mut fetched = false;
                while let Some(message) = socket.next().await {
                    let message = message.expect("读取请求");
                    let text = match message {
                        Message::Text(text) => text,
                        Message::Close(_) => break,
                        Message::Ping(_) | Message::Pong(_) => continue,
                        _ => panic!("请求须为 Text"),
                    };
                    let request: Value = serde_json::from_str(&text).expect("请求 JSON");
                    let action = request["action"].as_str().expect("action");
                    let req_id = request["args"]["req_id"].as_u64().expect("req_id");
                    let response = match action {
                        "conn" => Message::Text(
                            json!({"code":0,"action":"conn","req_id":req_id})
                                .to_string()
                                .into(),
                        ),
                        "query" => Message::Text(
                            json!({"code":0,"action":"query","req_id":req_id,"id":71,
                            "is_update":false,"fields_count":1,"fields_names":["synthetic_value"],
                            "fields_types":[kind],"fields_lengths":[payload.len()],"precision":0,
                            "unknown_metadata":{"text":"源元数据","count":17}})
                            .to_string()
                            .into(),
                        ),
                        "fetch" if !fetched => Message::Text(
                            json!({"code":0,"action":"fetch","req_id":req_id,
                            "id":71,"completed":false,"rows":1,"lengths":[column_length]})
                            .to_string()
                            .into(),
                        ),
                        "fetch_block" => {
                            assert!(!fetched, "单块只能读取一次");
                            fetched = true;
                            Message::Binary(frame.clone().into())
                        }
                        "fetch" => Message::Text(
                            json!({"code":0,"action":"fetch","req_id":req_id,
                            "id":71,"completed":true})
                            .to_string()
                            .into(),
                        ),
                        _ => panic!("未约定请求：{action}"),
                    };
                    if action == "fetch" || action == "fetch_block" {
                        assert_eq!(request["args"]["id"], 71);
                    }
                    requests.push(request);
                    socket.send(response).await.expect("发送合成响应");
                }
                requests
            })
            .await;
            outcome.expect("mock 超时不是转换目标失败")
        });
        let config = TaosConfig {
            host: "127.0.0.1".into(),
            port,
            transport: TransportMode::NativeWs,
            timeout: Duration::from_secs(2),
            max_response_bytes: 2048,
            ..TaosConfig::default()
        };
        let before = ws_probe_totals();
        let result = timeout(
            Duration::from_secs(3),
            exec_sql_ws(&config, "SELECT synthetic_value"),
        )
        .await
        .expect("原入口超时不是转换目标失败");
        let requests = match timeout(Duration::from_secs(5), &mut mock).await {
            Ok(joined) => joined.expect("mock 不得后台 panic"),
            Err(_) => {
                mock.abort();
                let _ = mock.await;
                panic!("mock 超时且已回收");
            }
        };
        assert_eq!(
            requests
                .iter()
                .map(|request| request["action"].as_str().expect("action"))
                .collect::<Vec<_>>(),
            ["conn", "query", "fetch", "fetch_block", "fetch"]
        );
        assert_eq!(requests[1]["args"]["sql"], "SELECT synthetic_value");
        if must_fail {
            let error = result.expect_err("非有限值或输出膨胀必须拒绝，不得置 null 或越过预算");
            assert!(
                matches!(
                    error,
                    TaosError::Serialization(_) | TaosError::Unavailable(_)
                ),
                "{error}"
            );
            assert_eq!(
                ws_probe_totals(),
                (before.0, before.1 + 1),
                "失败只计一次且不得计成功"
            );
        } else {
            let body = result.expect("完整 INT 行必须成功");
            let output: Value = serde_json::from_str(&body).expect("输出 JSON");
            assert_eq!(output["data"], json!([[11]]));
            assert_eq!(
                output["unknown_metadata"],
                json!({"text":"源元数据","count":17})
            );
            assert_eq!(ws_probe_totals(), (before.0 + 1, before.1), "成功只计一次");
        }
    }
}
