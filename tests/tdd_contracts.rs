#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! TDD 行为契约（特性 002）。
//!
//! 逐公开入口的「变异必红、原树必绿」对照：下表每行对应 `src/` 上的一处最小语义变异，
//! 变异副本上该行标注的红用例必须失败、本工作树上必须通过。变异与复现命令见 PR 描述。
//!
//! // TDD-PROBE: TaosConfig::from_env | 变异：from_env 不再读取 ENV_DATABASE | 红=config_from_env_reads_prefixed_env | 绿=config_from_env_reads_prefixed_env
//! // TDD-PROBE: TaosConfig::from_toml | 变异：from_toml 的 password 拒绝条件取反 | 红=config_from_toml_rejects_non_empty_password | 绿=config_from_toml_rejects_non_empty_password
//! // TDD-PROBE: TaosConfig::validate | 变异：max_in_flight 下界由 1 放宽为 0 | 红=config_validate_enforces_hard_limits | 绿=config_validate_enforces_hard_limits
//! // TDD-PROBE: TaosPool::connect | 变异：connect 去掉返回前的 ping 冒烟 | 红=pool_connect_pings_after_build | 绿=pool_connect_pings_after_build
//! // TDD-PROBE: TaosPool::exec | 变异：JSON 字符串单元格一律解析为空串 | 红=exec_parses_taos_json_rows | 绿=exec_parses_taos_json_rows
//! // TDD-PROBE: TaosPool::query | 变异：query 忽略入参 SQL，改写发送固定 SQL | 红=query_forwards_the_given_sql | 绿=query_forwards_the_given_sql
//! // TDD-PROBE: TaosPool::write_batch | 变异：成功报告把 accepted 恒置为 0 | 红=write_batch_reports_accepted_rows | 绿=write_batch_reports_accepted_rows
//! // TDD-PROBE: TaosPool::ping | 变异：ping 的成功分支条件由 code == 0 改为 code != 0 | 红=ping_requires_code_zero | 绿=ping_requires_code_zero
//! // TDD-PROBE: TaosPool::health_check | 变异：health_check 就绪分支把 ready 恒置为 false | 红=health_check_reports_ready_and_not_ready | 绿=health_check_reports_ready_and_not_ready
//! // TDD-PROBE: WriteBatcher | 变异：push 的自动刷写条件由 >= max_rows 改为 > max_rows | 红=write_batcher_auto_flushes_at_row_threshold | 绿=write_batcher_auto_flushes_at_row_threshold
//! // TDD-PROBE: TaosError::is_retryable | 变异：TDengine 繁忙码 896 由可重试改为不可重试 | 红=is_retryable_classifies_taos_codes | 绿=is_retryable_classifies_taos_codes

use std::sync::{Arc, Mutex};
use std::time::Duration;

use taosx::{
    TaosConfig, TaosError, TaosPoint, TaosPool, TransportMode, TsPrecision, WriteBatcher,
    WriteBatcherConfig, DEFAULT_DATABASE, DEFAULT_HOST, DEFAULT_PORT, ENV_DATABASE, ENV_HOST,
    ENV_PASSWORD, ENV_PORT, ENV_PREFIX, ENV_TRANSPORT, ENV_USER, HARD_MAX_IN_FLIGHT,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// 环境变量是进程级全局状态，串行化所有会读写 env 的用例。
static ENV_LOCK: Mutex<()> = Mutex::new(());

/// 清空本 crate 关心的环境变量。
fn clear_env() {
    for name in [
        ENV_HOST,
        ENV_PORT,
        ENV_DATABASE,
        ENV_USER,
        ENV_PASSWORD,
        ENV_TRANSPORT,
    ] {
        std::env::remove_var(name);
    }
}

/// 建表成功响应。
const CREATE_OK: &str = r#"{"code":0,"column_meta":[],"data":[],"rows":0}"#;
/// `DESCRIBE` 成功响应：`bid` / `ask` 为 `NCHAR(64)`。
const DESCRIBE_OK: &str = concat!(
    r#"{"code":0,"column_meta":[["field","VARCHAR",16],["type","VARCHAR",16],["length","VARCHAR",8]],"#,
    r#""data":[["ts","TIMESTAMP","8"],["bid","NCHAR","64"],["ask","NCHAR","64"]],"rows":3}"#
);
/// 写入成功响应。
const INSERT_OK: &str = r#"{"code":0,"column_meta":[],"data":[],"rows":0}"#;
/// `SELECT SERVER_VERSION()` 成功响应。
const VERSION_OK: &str =
    r#"{"code":0,"column_meta":[["v","VARCHAR",32]],"data":[["3.3.6.0"]],"rows":1}"#;

/// 本地一次性 TDengine REST 桩：按序返回预设 body，并记录收到的请求文本。
struct TaosMock {
    port: u16,
    received: Arc<Mutex<Vec<String>>>,
}

impl TaosMock {
    fn requests(&self) -> Vec<String> {
        self.received
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

/// 起一个按序应答 `bodies.len()` 次请求的本地桩（响应带 `Connection: close`，每请求一条连接）。
async fn spawn_taos_mock(bodies: Vec<&'static str>) -> TaosMock {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("绑定临时端口");
    let port = listener.local_addr().expect("读取临时端口").port();
    let received = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&received);
    tokio::spawn(async move {
        for body in bodies {
            let (mut stream, _) = listener.accept().await.expect("接受连接");
            let request = read_request(&mut stream).await;
            sink.lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(request);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.expect("写响应");
            let _ = stream.shutdown().await;
        }
    });
    TaosMock { port, received }
}

/// 读完一个完整 HTTP 请求（含 `Content-Length` 指定的正文）。
async fn read_request(stream: &mut tokio::net::TcpStream) -> String {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        let read = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut chunk))
            .await
            .expect("读取请求不得超时")
            .expect("读取请求");
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
        let text = String::from_utf8_lossy(&buffer).into_owned();
        if let Some(header_end) = text.find("\r\n\r\n") {
            let content_length = text[..header_end]
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(|value| value.trim().parse::<usize>().unwrap_or(0))
                })
                .unwrap_or(0);
            if buffer.len() >= header_end + 4 + content_length {
                break;
            }
        }
    }
    String::from_utf8_lossy(&buffer).into_owned()
}

/// 指向本地桩的池（`database` 置空，跳过建库与精度探测）。
fn pool_for(port: u16) -> TaosPool {
    TaosPool::new(TaosConfig {
        port,
        database: String::new(),
        timeout: Duration::from_secs(2),
        acquire_timeout: Duration::from_secs(2),
        ..TaosConfig::default()
    })
    .expect("离线构造池")
}

/// 入口 1：`TaosConfig::from_env` 读取契约前缀；transport 取值 `rest|native`。
#[test]
fn config_from_env_reads_prefixed_env() {
    let guard = ENV_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    clear_env();
    std::env::set_var(ENV_HOST, "127.0.0.3");
    std::env::set_var(ENV_PORT, "6141");
    std::env::set_var(ENV_DATABASE, "market_binance");
    std::env::set_var(ENV_USER, "writer");
    std::env::set_var(ENV_PASSWORD, "pw-from-env");
    std::env::set_var(ENV_TRANSPORT, "native");

    let config = TaosConfig::from_env().expect("环境加载必须成功");
    let observed = (
        config.host.clone(),
        config.port,
        config.database.clone(),
        config.user.clone(),
        config.password.clone(),
        config.transport,
    );

    clear_env();
    drop(guard);

    assert_eq!(observed.0, "127.0.0.3", "ENV_HOST 必须生效");
    assert_eq!(observed.1, 6141, "ENV_PORT 必须生效");
    assert_eq!(observed.2, "market_binance", "ENV_DATABASE 必须生效");
    assert_eq!(observed.3, "writer", "ENV_USER 必须生效");
    assert_eq!(observed.4, "pw-from-env", "密码只能经环境变量注入");
    assert_eq!(
        observed.5,
        TransportMode::NativeWs,
        "transport=native 应解析"
    );
    assert!(ENV_PREFIX.starts_with("FOUNDATIONX_TAOSX"));
}

/// 入口 2：`TaosConfig::from_toml` 拒绝非空 password，且不回显取值。
#[test]
fn config_from_toml_rejects_non_empty_password() {
    let error = TaosConfig::from_toml("schema_version = 1\npassword = \"hunter2\"\n")
        .expect_err("TOML 中的非空 password 必须拒绝");
    assert!(error.to_string().contains("password"));
    assert!(
        !error.to_string().contains("hunter2"),
        "错误不得回显 password"
    );

    let parsed =
        TaosConfig::from_toml("schema_version = 1\nport = 6041\n").expect("扁平字段可解析");
    assert!(parsed.password.is_empty(), "TOML 不参与密码注入");
    assert_eq!(parsed.port, 6041);
}

/// 入口 3：`TaosConfig::validate` 在建立连接前对硬上限 fail-closed。
#[test]
fn config_validate_enforces_hard_limits() {
    let zero_in_flight = TaosConfig {
        max_in_flight: 0,
        ..Default::default()
    };
    let error = zero_in_flight.validate().expect_err("0 并发必须拒绝");
    assert!(matches!(error, TaosError::Config(_)), "{error:?}");

    let over_limit = TaosConfig {
        max_in_flight: HARD_MAX_IN_FLIGHT + 1,
        ..Default::default()
    };
    let error = over_limit.validate().expect_err("超过硬上限必须拒绝");
    assert!(matches!(error, TaosError::Config(_)), "{error:?}");

    let remote_plaintext = TaosConfig {
        host: "td.example".into(),
        ..Default::default()
    };
    let error = remote_plaintext
        .validate()
        .expect_err("远程明文必须 fail-closed");
    assert!(matches!(error, TaosError::Config(_)), "{error:?}");

    let defaults = TaosConfig::default();
    assert_eq!(defaults.host, DEFAULT_HOST);
    assert_eq!(defaults.port, DEFAULT_PORT);
    assert_eq!(defaults.database, DEFAULT_DATABASE);
    defaults.validate().expect("默认配置必须有效");
}

/// 入口 4：`TaosPool::connect` 返回前完成一次 `SELECT SERVER_VERSION()` 冒烟。
#[tokio::test]
async fn pool_connect_pings_after_build() {
    let mock = spawn_taos_mock(vec![VERSION_OK]).await;
    let pool = TaosPool::connect(TaosConfig {
        port: mock.port,
        database: String::new(),
        timeout: Duration::from_secs(2),
        ..TaosConfig::default()
    })
    .await
    .expect("建连必须成功");

    assert_eq!(pool.stats().in_flight, 0);
    assert!(!pool.is_closed());
    assert_eq!(pool.precision(), TsPrecision::Ms, "默认精度为 ms");

    let requests = mock.requests();
    assert_eq!(requests.len(), 1, "connect 必须先 ping 一次");
    assert!(requests[0].contains("SELECT SERVER_VERSION()"));
    pool.close().await.expect("关闭");
}

/// 入口 5：`TaosPool::exec` 解析 TDengine JSON（列名、单元格与 code 传播）。
#[tokio::test]
async fn exec_parses_taos_json_rows() {
    let body = r#"{"code":0,"column_meta":[["id","INT",4],["side","VARCHAR",8]],"data":[[1,"open"],[2,"close"]],"rows":2}"#;
    let mock = spawn_taos_mock(vec![body]).await;
    let pool = pool_for(mock.port);

    let result = pool
        .exec("SELECT id, side FROM ticks")
        .await
        .expect("exec 必须成功");
    assert_eq!(result.code, 0);
    assert_eq!(result.columns, vec!["id".to_owned(), "side".to_owned()]);
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0], vec!["1".to_owned(), "open".to_owned()]);
    assert_eq!(result.rows[1], vec!["2".to_owned(), "close".to_owned()]);

    let requests = mock.requests();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].contains("SELECT id, side FROM ticks"));
}

/// 入口 6：`TaosPool::query` 语义同 `exec`，原样转发调用方 SQL。
#[tokio::test]
async fn query_forwards_the_given_sql() {
    let body = r#"{"code":0,"column_meta":[["count(*)","BIGINT",8]],"data":[[7]],"rows":1}"#;
    let mock = spawn_taos_mock(vec![body]).await;
    let pool = pool_for(mock.port);

    let result = pool
        .query("SELECT count(*) FROM `ticks`")
        .await
        .expect("query 必须成功");
    assert_eq!(result.rows, vec![vec!["7".to_owned()]]);

    let requests = mock.requests();
    assert_eq!(requests.len(), 1);
    assert!(
        requests[0].contains("SELECT count(*) FROM `ticks`"),
        "query 必须原样转发入参 SQL: {}",
        requests[0]
    );
}

/// 入口 7：`TaosPool::write_batch` 返回已接受行数与分块计数。
#[tokio::test]
async fn write_batch_reports_accepted_rows() {
    let mock = spawn_taos_mock(vec![
        CREATE_OK,
        DESCRIBE_OK,
        INSERT_OK,
        CREATE_OK,
        DESCRIBE_OK,
        INSERT_OK,
    ])
    .await;
    let pool = TaosPool::new(TaosConfig {
        port: mock.port,
        database: String::new(),
        batch_max_rows: 10,
        timeout: Duration::from_secs(2),
        ..TaosConfig::default()
    })
    .expect("池");
    let points = [TaosPoint::new("BTC", 1_000_000, "1.0", "1.1")];

    let report = pool
        .write_batch_report("ticks", &points)
        .await
        .expect("批量写入必须成功");
    assert_eq!(report.accepted, 1, "全部行必须计入 accepted");
    assert_eq!(report.failed, 0);
    assert_eq!(report.chunks_ok, 1);
    assert_eq!(report.chunks_total, 1);
    assert!(report.is_complete());

    // 无报告的便捷入口语义一致（成功即 Ok）。
    pool.write_batch("ticks", &points)
        .await
        .expect("write_batch 必须成功");

    let requests = mock.requests();
    assert_eq!(
        requests.len(),
        6,
        "两轮 ensure_stable（建表 + DESCRIBE）+ 2 次 INSERT"
    );
    assert!(requests[0].contains("CREATE STABLE IF NOT EXISTS `ticks`"));
    assert!(requests[1].contains("DESCRIBE `ticks`"));
    assert!(requests[2].contains("INSERT INTO "));
    assert!(
        requests[2].contains("TAGS ('BTC')"),
        "tag 值应以字面量进入 TAGS: {}",
        requests[2]
    );
}

/// 入口 8：`TaosPool::ping` 只在 `code == 0` 时成功；业务错误码原样传播。
#[tokio::test]
async fn ping_requires_code_zero() {
    let mock = spawn_taos_mock(vec![VERSION_OK]).await;
    let pool = pool_for(mock.port);
    pool.ping().await.expect("code=0 时 ping 必须成功");
    assert_eq!(mock.requests().len(), 1);

    let mock = spawn_taos_mock(vec![r#"{"code":9731,"desc":"Table does not exist"}"#]).await;
    let pool = pool_for(mock.port);
    let error = pool.ping().await.expect_err("code!=0 必须失败");
    assert!(error.is_not_found(), "{error:?}");
    assert!(!error.is_retryable(), "表不存在不可重试");
}

/// 入口 9：`TaosPool::health_check` 以 `ready` 表达就绪，失败不返回 `Err`。
#[tokio::test]
async fn health_check_reports_ready_and_not_ready() {
    let mock = spawn_taos_mock(vec![VERSION_OK]).await;
    let pool = pool_for(mock.port);
    let health = pool.health_check().await.expect("健康检查信封");
    assert!(health.ready, "{health:?}");
    assert!(health.is_ready());
    assert_eq!(health.server_version.as_deref(), Some("3.3.6.0"));
    assert_eq!(health.detail, "就绪");

    // 不可达：仍返回 Ok，以 ready=false 表达。
    let pool = pool_for(1);
    let health = pool.health_check().await.expect("健康检查信封");
    assert!(!health.ready, "{health:?}");
    assert!(health.server_version.is_none());
    assert!(!health.detail.is_empty());
}

/// 入口 10：`WriteBatcher` 在达到行数阈值时自动刷写。
#[tokio::test]
async fn write_batcher_auto_flushes_at_row_threshold() {
    let mock = spawn_taos_mock(vec![CREATE_OK, DESCRIBE_OK, INSERT_OK]).await;
    let pool = pool_for(mock.port);
    let batcher = WriteBatcher::new(
        pool,
        "ticks",
        WriteBatcherConfig {
            max_rows: 1,
            flush_interval: Duration::from_secs(60),
            ..WriteBatcherConfig::default()
        },
    );

    batcher
        .push(TaosPoint::new("BTC", 1_000_000, "1.0", "1.1"))
        .await
        .expect("首次 push 必须触发自动刷写");
    assert_eq!(batcher.totals().await, (1, 0));
    assert!(!batcher.has_pending().await);

    let report = batcher.close().await.expect("关闭");
    assert_eq!(report.accepted, 0, "关闭时缓冲已空");
    assert_eq!(mock.requests().len(), 3);
}

/// 入口 11：`TaosError::is_retryable` 保留源码语义（896 可重试；表不存在不可重试）。
#[test]
fn is_retryable_classifies_taos_codes() {
    let busy = TaosError::from_taos_code(896, "服务端繁忙");
    assert!(matches!(busy, TaosError::Unavailable(_)), "{busy:?}");
    assert!(busy.is_retryable(), "896 应可重试");

    let not_found = TaosError::from_taos_code(0x2603, "表不存在");
    assert!(not_found.is_not_found());
    assert!(!not_found.is_retryable(), "表不存在不可重试");
    assert_eq!(not_found.taos_code(), Some(0x2603));

    for retryable in [
        TaosError::Connection("x".into()),
        TaosError::Unavailable("x".into()),
        TaosError::Timeout("x".into()),
        TaosError::Io(std::io::Error::other("x")),
    ] {
        assert!(retryable.is_retryable(), "{retryable:?} 应可重试");
    }
    for permanent in [
        TaosError::Config("x".into()),
        TaosError::backend("x"),
        TaosError::Serialization("x".into()),
        TaosError::Invalid("x".into()),
        TaosError::Closed("x".into()),
        TaosError::Unsupported("x".into()),
    ] {
        assert!(!permanent.is_retryable(), "{permanent:?} 不应可重试");
    }
}
