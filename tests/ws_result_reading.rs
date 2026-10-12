#![allow(clippy::expect_used, clippy::panic)]
//! 原入口完整结果读取的离线合成反例，不代表 T1 服务端实测。
//!
//! raw block 的 version=1、flag=0x80000000 采用已独核取值；块内容仍为离线合成样本，不代表 T1 服务端实测。
//! 返回 String 的 data 数组是本测试要求的本地结果表示，不承诺它是 wire 协议字段。

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use taosx::{exec_sql_ws, TaosConfig, TransportMode};
use tokio::net::TcpListener;
use tokio::time::timeout;
use tokio_tungstenite::{accept_async, tungstenite::Message};

fn synthetic_int_block(values: &[i32], result_id: u64) -> Vec<u8> {
    let mut raw = Vec::new();
    raw.extend_from_slice(&1_u32.to_le_bytes());
    raw.extend_from_slice(&(38_u32 + values.len() as u32 * 4).to_le_bytes());
    raw.extend_from_slice(&(values.len() as u32).to_le_bytes());
    raw.extend_from_slice(&1_u32.to_le_bytes());
    raw.extend_from_slice(&0x8000_0000_u32.to_le_bytes());
    raw.extend_from_slice(&0_u64.to_le_bytes());
    raw.push(4);
    raw.extend_from_slice(&4_u32.to_le_bytes());
    raw.extend_from_slice(&(values.len() as u32 * 4).to_le_bytes());
    raw.push(0);
    for value in values {
        raw.extend_from_slice(&value.to_le_bytes());
    }
    let mut frame = Vec::new();
    frame.extend_from_slice(&0_u64.to_le_bytes());
    frame.extend_from_slice(&result_id.to_le_bytes());
    frame.extend_from_slice(&raw);
    frame
}

#[tokio::test]
async fn original_exec_sql_ws_returns_all_rows_across_two_blocks() {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("离线 mock 必须绑定 loopback 随机端口");
    let port = listener.local_addr().expect("取得随机端口").port();
    let mut mock = tokio::spawn(async move {
        let mut requests = Vec::<Value>::new();
        let outcome = timeout(Duration::from_secs(4), async {
            let (stream, _) = listener.accept().await.map_err(|error| error.to_string())?;
            let mut socket = accept_async(stream)
                .await
                .map_err(|error| error.to_string())?;
            let blocks = [
                synthetic_int_block(&[11, 22], 71),
                synthetic_int_block(&[33], 71),
            ];
            let mut block_index = 0_usize;
            while let Some(message) = socket.next().await {
                let message = message.map_err(|error| error.to_string())?;
                let text = match message {
                    Message::Text(text) => text,
                    Message::Close(_) => break,
                    Message::Ping(_) | Message::Pong(_) => continue,
                    _ => return Err("客户端发来非 Text 请求".to_owned()),
                };
                let request: Value =
                    serde_json::from_str(&text).map_err(|error| error.to_string())?;
                requests.push(request.clone());
                let action = request["action"].as_str().unwrap_or("");
                let req_id = request["args"]["req_id"].as_u64().unwrap_or(0);
                let response = match action {
                    "conn" => {
                        let fixture = json!({
                            "_synthetic": true, "_note": "离线合成连接响应，非真实服务",
                            "response": {
                                "code": 0, "message": "", "action": "conn", "req_id": req_id
                            }
                        });
                        Message::Text(fixture["response"].to_string().into())
                    }
                    "query" => {
                        let fixture = json!({
                            "_synthetic": true, "_note": "两块 INT 元数据，未携带结果行",
                            "response": {
                                "code": 0, "message": "", "action": "query", "req_id": req_id,
                                "id": 71, "is_update": false, "fields_count": 1,
                                "fields_names": ["synthetic_int"], "fields_types": [4],
                                "fields_lengths": [4], "precision": 0
                            }
                        });
                        Message::Text(fixture["response"].to_string().into())
                    }
                    "fetch" => {
                        if request["args"]["id"] != json!(71) {
                            return Err("fetch 未引用合成结果句柄".to_owned());
                        }
                        let response = if block_index < blocks.len() {
                            let rows = if block_index == 0 { 2 } else { 1 };
                            json!({
                                "_synthetic": true, "_note": "合成块缓存响应",
                                "response": {
                                    "code": 0, "message": "", "action": "fetch",
                                    "req_id": req_id, "id": 71, "completed": false,
                                    "rows": rows, "lengths": [rows * 4], "timing": 0
                                }
                            })
                        } else {
                            json!({
                                "_synthetic": true, "_note": "合成结果结束并自动释放",
                                "response": {
                                    "code": 0, "message": "", "action": "fetch",
                                    "req_id": req_id, "id": 71, "completed": true, "timing": 0
                                }
                            })
                        };
                        Message::Text(response["response"].to_string().into())
                    }
                    "fetch_block" => {
                        if request["args"]["id"] != json!(71) {
                            return Err("fetch_block 未引用合成结果句柄".to_owned());
                        }
                        let block = blocks
                            .get(block_index)
                            .ok_or_else(|| "请求了不存在的合成块".to_owned())?;
                        block_index += 1;
                        Message::Binary(block.clone().into())
                    }
                    "free_result" => continue,
                    _ => return Err(format!("未约定的客户端 action：{action}")),
                };
                socket
                    .send(response)
                    .await
                    .map_err(|error| error.to_string())?;
            }
            Ok::<(), String>(())
        })
        .await;
        (requests, outcome)
    });
    let config = TaosConfig {
        host: "127.0.0.1".into(),
        port,
        user: "synthetic-user".into(),
        password: "synthetic-password".into(),
        transport: TransportMode::NativeWs,
        timeout: Duration::from_secs(2),
        ..TaosConfig::default()
    };
    let result = timeout(
        Duration::from_secs(3),
        exec_sql_ws(&config, "SELECT synthetic_int FROM synthetic_rows"),
    )
    .await;
    let joined = timeout(Duration::from_secs(5), &mut mock).await;
    let (requests, mock_outcome) = match joined {
        Ok(joined) => joined.expect("mock 必须正常回传轨迹，不得后台 panic"),
        Err(_) => {
            mock.abort();
            let _ = mock.await;
            panic!("mock 超时；句柄已收回，此失败不是全部行目标红");
        }
    };
    mock_outcome
        .expect("mock 超时不是全部行目标红")
        .expect("mock 协议或连接失败不是全部行目标红");
    let body = result
        .expect("原入口调用超时不是全部行目标红")
        .expect("原入口应返回可比较的本地结果");
    let response: Value = serde_json::from_str(&body).expect("本地结果必须是 JSON");
    let actions: Vec<&str> = requests
        .iter()
        .map(|request| request["action"].as_str().expect("轨迹必须含 action"))
        .collect();
    assert!(actions.len() >= 2, "原入口必须实际完成连接与查询");
    assert_eq!(&actions[..2], &["conn", "query"]);
    assert_eq!(
        requests[1]["args"]["sql"],
        json!("SELECT synthetic_int FROM synthetic_rows")
    );
    assert_eq!(response["code"], json!(0));
    assert_eq!(response["fields_names"], json!(["synthetic_int"]));
    eprintln!("原入口实际 action 轨迹：{actions:?}");
    assert_eq!(
        response["data"],
        json!([[11], [22], [33]]),
        "原入口必须返回两块的全部三行"
    );
    assert_eq!(
        actions,
        [
            "conn",
            "query",
            "fetch",
            "fetch_block",
            "fetch",
            "fetch_block",
            "fetch"
        ]
    );
    assert_eq!(
        requests[1]["args"]["sql"],
        json!("SELECT synthetic_int FROM synthetic_rows")
    );
    for request in &requests[2..] {
        assert_eq!(request["args"]["id"], json!(71));
        assert!(request["args"]["req_id"].as_u64().is_some());
    }
}
