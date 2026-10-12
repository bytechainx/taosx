#![allow(clippy::expect_used, clippy::panic)]
//! 原始 Text 元数据的未知大整数与大指数保真；两个入口均实际读完合成 INT 块。

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use taosx::{exec_sql_ws, exec_sql_ws_rows, TaosConfig, TransportMode, WsValue};
use tokio::net::TcpListener;
use tokio::time::timeout;
use tokio_tungstenite::{accept_async, tungstenite::Message};

#[tokio::test]
async fn unknown_metadata_numbers_preserve_literals_and_typed_rows() {
    for original_entry in [true, false] {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("离线 loopback");
        let port = listener.local_addr().expect("随机端口").port();
        let mut mock = tokio::spawn(async move {
            timeout(Duration::from_secs(4), async {
                let (stream, _) = listener.accept().await.expect("接受连接");
                let mut socket = accept_async(stream).await.expect("WS 握手");
                let mut raw = Vec::new();
                raw.extend_from_slice(&1_u32.to_le_bytes());
                raw.extend_from_slice(&42_u32.to_le_bytes());
                raw.extend_from_slice(&1_u32.to_le_bytes());
                raw.extend_from_slice(&1_u32.to_le_bytes());
                raw.extend_from_slice(&0x8000_0000_u32.to_le_bytes());
                raw.extend_from_slice(&0_u64.to_le_bytes());
                raw.push(4);
                raw.extend_from_slice(&4_u32.to_le_bytes());
                raw.extend_from_slice(&4_u32.to_le_bytes());
                raw.push(0);
                raw.extend_from_slice(&11_i32.to_le_bytes());
                let mut block = Vec::new();
                block.extend_from_slice(&0_u64.to_le_bytes());
                block.extend_from_slice(&71_u64.to_le_bytes());
                block.extend_from_slice(&raw);
                let mut requests = Vec::new();
                let mut fetched = false;
                let mut query_text = None;
                while let Some(message) = socket.next().await {
                    let text = match message.expect("读取请求") {
                        Message::Text(text) => text,
                        Message::Close(_) => break,
                        Message::Ping(_) | Message::Pong(_) => continue,
                        _ => panic!("请求必须为 Text"),
                    };
                    let request: Value = serde_json::from_str(&text).expect("请求 JSON");
                    let action = request["action"].as_str().expect("action");
                    let req_id = request["args"]["req_id"].as_u64().expect("req_id");
                    let response = match action {
                        "conn" => Message::Text(json!({"code":0,"action":"conn","req_id":req_id}).to_string().into()),
                        "query" => {
                            assert!(query_text.is_none(), "同一会话只能查询一次");
                            let text = format!(r#"{{"code":0,"action":"query","req_id":{req_id},"id":71,"is_update":false,"fields_count":1,"fields_names":["synthetic_int"],"fields_types":[4],"fields_lengths":[4],"precision":0,"unknown_metadata":{{"extra":18446744073709551617,"huge":1e400}}}}"#);
                            query_text = Some(text.clone());
                            Message::Text(text.into())
                        }
                        "fetch" if !fetched => Message::Text(json!({"code":0,"action":"fetch","req_id":req_id,
                            "id":71,"completed":false,"rows":1,"lengths":[4]}).to_string().into()),
                        "fetch_block" => {
                            assert!(!fetched, "单块只能取一次");
                            fetched = true;
                            Message::Binary(block.clone().into())
                        }
                        "fetch" => Message::Text(json!({"code":0,"action":"fetch","req_id":req_id,
                            "id":71,"completed":true}).to_string().into()),
                        _ => panic!("未约定请求：{action}"),
                    };
                    if action == "fetch" || action == "fetch_block" {
                        assert_eq!(request["args"]["id"], 71);
                    }
                    requests.push(request);
                    socket.send(response).await.expect("发送原始响应");
                }
                (requests, query_text.expect("必须实际发送 query 原文"))
            }).await.expect("mock 超时不是精度目标失败")
        });
        let config = TaosConfig {
            host: "127.0.0.1".into(),
            port,
            transport: TransportMode::NativeWs,
            timeout: Duration::from_secs(2),
            ..TaosConfig::default()
        };
        let body = timeout(Duration::from_secs(3), async {
            if original_entry {
                Some(
                    exec_sql_ws(&config, "SELECT synthetic_int")
                        .await
                        .expect("原入口必须接受未知数值元数据"),
                )
            } else {
                let result = exec_sql_ws_rows(&config, "SELECT synthetic_int")
                    .await
                    .expect("typed 入口必须接受未知数值元数据");
                assert_eq!(result.fields_names, ["synthetic_int"]);
                assert_eq!(result.fields_types, [4]);
                assert_eq!(result.rows, vec![vec![WsValue::Int(11)]]);
                None
            }
        })
        .await
        .expect("调用超时不是精度目标失败");
        let (requests, query_text) = match timeout(Duration::from_secs(5), &mut mock).await {
            Ok(joined) => joined.expect("mock 不得后台 panic"),
            Err(_) => {
                mock.abort();
                let _ = mock.await;
                panic!("mock 超时且已收回");
            }
        };
        assert_eq!(
            requests
                .iter()
                .map(|request| request["action"].as_str().expect("action"))
                .collect::<Vec<_>>(),
            ["conn", "query", "fetch", "fetch_block", "fetch"]
        );
        assert_eq!(requests[1]["args"]["sql"], "SELECT synthetic_int");
        assert!(query_text.contains("\"extra\":18446744073709551617"));
        assert!(query_text.contains("\"huge\":1e400"));
        if let Some(body) = body {
            let output: Value = serde_json::from_str(&body).expect("原入口输出 JSON");
            assert_eq!(output["data"], json!([[11]]));
            let extra = output["unknown_metadata"]["extra"]
                .as_number()
                .expect("未知大整数不得字符串化");
            let huge = output["unknown_metadata"]["huge"]
                .as_number()
                .expect("未知大指数不得字符串化");
            assert_eq!(
                extra.to_string(),
                "18446744073709551617",
                "未知大整数不得舍入"
            );
            let original_metadata: Value = serde_json::from_str(&query_text)
                .expect("实际 query 原文须通过 arbitrary_precision 精确解析");
            let expected_huge = original_metadata["unknown_metadata"]["huge"]
                .as_number()
                .expect("实际 query 原文的未知大指数须为数值");
            assert_eq!(
                huge, expected_huge,
                "未知大指数必须保留精确数值，允许指数规范化而非要求原始字节"
            );
            assert!(body.contains("\"extra\":18446744073709551617"));
        }
    }
}
