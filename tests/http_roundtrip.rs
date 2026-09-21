//! REST 往返：用本地 TCP mock 驱动 `connect` → 建表 → 批量写 → 查询 → 批处理器 全链路。
//!
//! mock 只回放预设的 TDengine JSON 响应，并记录每条请求行，用于校验端点与认证契约。

use std::sync::{Arc, Mutex};
use std::time::Duration;

use taosx::{TaosConfig, TaosPoint, TaosPool, TsPrecision, WriteBatcher, WriteBatcherConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const OK_EMPTY: &str = r#"{"code":0,"column_meta":[],"data":[],"rows":0}"#;
const PRECISION_MS: &str =
    r#"{"code":0,"column_meta":[["precision","VARCHAR",8]],"data":[["ms"]],"rows":1}"#;
const PING_OK: &str =
    r#"{"code":0,"column_meta":[["v","VARCHAR",32]],"data":[["3.3.6.13"]],"rows":1}"#;
const DESCRIBE_NCHAR: &str = concat!(
    r#"{"code":0,"column_meta":[["field","VARCHAR",16],["type","VARCHAR",16],["length","VARCHAR",8]],"#,
    r#""data":[["ts","TIMESTAMP","8"],["bid","NCHAR","64"],["ask","NCHAR","64"]],"rows":3}"#
);
const INSERT_OK: &str = r#"{"code":0,"column_meta":[],"data":[],"rows":0,"affected_rows":1}"#;
const SELECT_TICKS: &str = concat!(
    r#"{"code":0,"column_meta":[["ts","TIMESTAMP",8],["bid","NCHAR",64],["ask","NCHAR",64],["symbol","NCHAR",16]],"#,
    r#""data":[[1700000000000,"66522.40","66523.10","BTC/USDT"],[1700000001000,"66524.00","66525.00","ETH/USDT"]],"rows":2}"#
);

/// 回放 `bodies`，并记录每条请求行（第一个请求行 + Basic 认证头）。
struct MockTaos {
    port: u16,
    requests: Arc<Mutex<Vec<String>>>,
}

impl MockTaos {
    /// 启动 mock；`bodies` 按请求顺序回放。
    async fn start(bodies: Vec<&'static str>) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&requests);
        tokio::spawn(async move {
            for body in bodies {
                let (mut stream, _) = listener.accept().await.expect("accept");
                let mut buffer = vec![0u8; 8192];
                let read = stream.read(&mut buffer).await.expect("read request");
                let request = String::from_utf8_lossy(&buffer[..read]).into_owned();
                recorder.lock().expect("record").push(request);
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
        Self { port, requests }
    }

    fn request_count(&self) -> usize {
        self.requests.lock().expect("requests").len()
    }

    fn request_line(&self, index: usize) -> String {
        self.requests
            .lock()
            .expect("requests")
            .get(index)
            .and_then(|request| request.lines().next())
            .unwrap_or_default()
            .to_owned()
    }

    fn request_text(&self, index: usize) -> String {
        self.requests
            .lock()
            .expect("requests")
            .get(index)
            .cloned()
            .unwrap_or_default()
    }
}

fn config_for(port: u16) -> TaosConfig {
    TaosConfig {
        port,
        timeout: Duration::from_secs(2),
        acquire_timeout: Duration::from_secs(2),
        user: "root".to_owned(),
        password: "test-secret".to_owned(),
        ..TaosConfig::default()
    }
}

fn sample_points() -> Vec<TaosPoint> {
    vec![
        TaosPoint::new(
            "BTC/USDT",
            1_700_000_000_000_000_000,
            "66522.40",
            "66523.10",
        ),
        TaosPoint::new("ETH/USDT", 1_700_000_001_000_000_000, "3000.10", "3000.20"),
    ]
}

#[tokio::test]
async fn connect_creates_database_detects_precision_and_pings() {
    let mock = MockTaos::start(vec![OK_EMPTY, PRECISION_MS, PING_OK]).await;
    let pool = TaosPool::connect(config_for(mock.port))
        .await
        .expect("连接成功");

    assert_eq!(pool.precision(), TsPrecision::Ms);
    assert_eq!(pool.stats().in_flight, 0);
    assert!(!pool.stats().closed);
    assert_eq!(mock.request_count(), 3);

    // 建库与精度探测走不带 database 的端点，ping 走带 database 的端点。
    assert!(
        mock.request_line(0).starts_with("POST /rest/sql "),
        "{}",
        mock.request_line(0)
    );
    assert!(
        mock.request_line(1).starts_with("POST /rest/sql "),
        "{}",
        mock.request_line(1)
    );
    assert!(
        mock.request_line(2)
            .starts_with("POST /rest/sql/infra_draft "),
        "{}",
        mock.request_line(2)
    );
    assert!(mock
        .request_text(0)
        .contains("CREATE DATABASE IF NOT EXISTS `infra_draft`"));
    assert!(mock
        .request_text(1)
        .contains("information_schema.ins_databases"));
    assert!(mock.request_text(2).contains("SELECT SERVER_VERSION()"));
    // Basic 认证头存在，但凭据不出现在 URL 中。
    assert!(mock
        .request_text(0)
        .to_lowercase()
        .contains("authorization: basic"));
    assert!(!mock.request_line(0).contains("test-secret"));

    pool.close().await.expect("关闭成功");
    assert!(pool.is_closed());
}

#[tokio::test]
async fn write_batch_ensures_stable_then_inserts_chunks() {
    let mock = MockTaos::start(vec![
        OK_EMPTY,       // CREATE DATABASE
        PRECISION_MS,   // 精度探测
        PING_OK,        // ping
        OK_EMPTY,       // CREATE STABLE
        DESCRIBE_NCHAR, // DESCRIBE
        INSERT_OK,      // INSERT chunk
    ])
    .await;
    let pool = TaosPool::connect(config_for(mock.port))
        .await
        .expect("连接成功");
    let report = pool
        .write_batch_report("ticks", &sample_points())
        .await
        .expect("批量写入");

    assert_eq!(report.accepted, 2);
    assert_eq!(report.failed, 0);
    assert_eq!(report.chunks_ok, 1);
    assert_eq!(report.chunks_total, 1);
    assert!(report.is_complete());
    assert_eq!(mock.request_count(), 6);

    let insert = mock.request_text(5);
    assert!(!insert.contains("CREATE STABLE"));
    assert!(insert.contains("INSERT INTO "));
    assert!(insert.contains("USING `ticks`"));
    assert!(insert.contains("TAGS ('BTC/USDT')"));
    assert!(
        insert.contains("VALUES (1700000000000,'66522.40','66523.10')"),
        "{insert}"
    );

    let metrics = pool.metrics();
    assert!(metrics.write_ok >= 1);
    assert!(metrics.sql_ok >= 6);
    assert!(metrics.sql_bytes > 0);
    assert!(metrics.response_bytes > 0);
    assert!(pool.metrics_prometheus().contains("taosx_bytes_total"));

    // 幂等写路径（`write_max_attempts = 1` 时等价单次尝试）。
    let error = pool
        .write_batch("bad table", &sample_points())
        .await
        .expect_err("非法表名");
    assert!(!error.is_retryable());
}

#[tokio::test]
async fn query_series_and_stream_return_typed_points() {
    let mock = MockTaos::start(vec![DESCRIBE_NCHAR, SELECT_TICKS]).await;
    let pool = TaosPool::new(config_for(mock.port)).expect("离线构造");
    let points = pool
        .query_series("ticks", 0, 2_000_000_000_000_000_000)
        .await
        .expect("查询");

    assert_eq!(points.len(), 2);
    assert_eq!(points[0].tag_value, "BTC/USDT");
    assert_eq!(points[0].timestamp_ns, 1_700_000_000_000_000_000);
    assert_eq!(
        points[0].values,
        ["66522.40".to_owned(), "66523.10".to_owned()]
    );
    assert_eq!(points[1].tag_value, "ETH/USDT");
    assert_eq!(mock.request_count(), 2);
    assert!(
        mock.request_text(1).contains("ORDER BY ts ASC LIMIT 10001"),
        "{}",
        mock.request_text(1)
    );

    // 流式封装按行 yield。
    let mock = MockTaos::start(vec![DESCRIBE_NCHAR, SELECT_TICKS]).await;
    let pool = TaosPool::new(config_for(mock.port)).expect("离线构造");
    let mut stream = pool
        .query_series_stream_chunked("ticks", 0, 2_000_000_000_000_000_000, 128)
        .await
        .expect("流式查询");
    assert_eq!(stream.remaining_hint(), 2);
    assert_eq!(stream.chunk_hint(), 128);
    let mut tags = Vec::new();
    while let Some(item) = futures_util::StreamExt::next(&mut stream).await {
        tags.push(item.expect("行必须 Ok").tag_value);
    }
    assert_eq!(tags, vec!["BTC/USDT".to_owned(), "ETH/USDT".to_owned()]);
    assert!(pool
        .query_series_stream_chunked("ticks", 0, 1, 0)
        .await
        .is_err());
}

#[tokio::test]
async fn write_batcher_flushes_and_closes_through_pool() {
    let mock = MockTaos::start(vec![OK_EMPTY, DESCRIBE_NCHAR, INSERT_OK]).await;
    let pool = TaosPool::new(config_for(mock.port)).expect("离线构造");
    let batcher = WriteBatcher::new(
        pool.clone(),
        "ticks",
        WriteBatcherConfig {
            max_rows: 100,
            flush_interval: Duration::from_secs(60),
            ..Default::default()
        },
    );

    batcher.push(sample_points().remove(0)).await.expect("push");
    assert!(!batcher.has_pending().await);
    assert_eq!(batcher.pending_len().await, 0);

    let flush_report = batcher.flush().await.expect("flush");
    assert_eq!(flush_report.accepted, 1);
    assert_eq!(batcher.totals().await, (1, 0));
    assert_eq!(mock.request_count(), 3);

    let last_flush = batcher.close().await.expect("close");
    assert_eq!(last_flush.accepted, 0, "关闭时缓冲区已空");
    let summary = batcher.close_report().await;
    assert_eq!(summary.total_accepted, 1);
    assert_eq!(summary.total_failed, 0);
    assert_eq!(summary.pending, 0);
    assert!(summary.last_flush.is_complete());
    assert!(
        batcher.push(sample_points().remove(0)).await.is_err(),
        "关闭后 push 必须拒绝"
    );
}
