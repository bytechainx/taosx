#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! 多主机 failover 成功路径测试（P1-4 覆盖补全）。
//!
//! 此前 failover 成功路径零测试：首 host 失败、次 host 成功的场景仅被全失败用例间接覆盖，
//! 对「首个 Err 即 return」等回归零检测力。本文件补离线 mock 测试，锁定正确行为。

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use taosx::{TaosConfig, TaosPool};

/// 按序为多个请求返回预设 JSON body（各自独立连接）。
async fn serve_sequence(bodies: Vec<&'static str>) -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(async move {
        for body in bodies {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut request = [0u8; 4096];
            let _ = stream.read(&mut request).await.expect("read request");
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("write response");
        }
    });
    port
}

/// 首 host 失败（127.0.0.2 无 listener，连接被拒）、次 host 成功（127.0.0.1，mock 服务器）。
#[tokio::test]
async fn failover_first_host_fails_second_succeeds() {
    let create_db = r#"{"code":0,"column_meta":[],"data":[],"rows":0}"#;
    let precision =
        r#"{"code":0,"column_meta":[["precision","VARCHAR",8]],"data":[["ms"]],"rows":1}"#;
    let ping = r#"{"code":0,"column_meta":[["v","VARCHAR",32]],"data":[["3.3.6.13"]],"rows":1}"#;

    let good_port = serve_sequence(vec![create_db, precision, ping]).await;

    let config = TaosConfig {
        host: "127.0.0.2".into(), // 无 listener，连接被拒快速失败
        port: good_port,
        hosts: vec!["127.0.0.1".into()], // 备用 host，mock 服务器
        timeout: Duration::from_secs(2),
        ..TaosConfig::default()
    };

    let pool = TaosPool::connect(config)
        .await
        .expect("failover 必须成功连接到第二 host");

    // 验证池最终连接的是第二 host（127.0.0.1），而非失败的首 host。
    assert_eq!(
        pool.config().host,
        "127.0.0.1",
        "failover 后 host 必须为成功的备用 host"
    );
    assert!(pool.liveness());
    assert!(!pool.is_closed());
}

/// 多个备用 host：前两个失败，第三个成功。
#[tokio::test]
async fn failover_second_host_succeeds_after_two_failures() {
    let create_db = r#"{"code":0,"column_meta":[],"data":[],"rows":0}"#;
    let precision =
        r#"{"code":0,"column_meta":[["precision","VARCHAR",8]],"data":[["ms"]],"rows":1}"#;
    let ping = r#"{"code":0,"column_meta":[["v","VARCHAR",32]],"data":[["3.3.6.13"]],"rows":1}"#;

    let good_port = serve_sequence(vec![create_db, precision, ping]).await;

    let config = TaosConfig {
        host: "127.0.0.2".into(), // 失败
        port: good_port,
        hosts: vec![
            // 备用 host：第一个也失败，第二个成功
            "127.0.0.3".into(),
            "127.0.0.1".into(),
        ],
        timeout: Duration::from_secs(2),
        ..TaosConfig::default()
    };

    let pool = TaosPool::connect(config)
        .await
        .expect("failover 必须成功连接到第三 host");

    assert_eq!(pool.config().host, "127.0.0.1");
    assert!(pool.liveness());
}
