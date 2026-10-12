//! TDengine 原生 WebSocket 传输（`/rest/ws`）：握手探测与完整结果短会话。
//!
//! 原 String 入口与类型化入口复用同一次 `conn` → `query` → `fetch` / `fetch_block`
//! 读取链，直到 `completed:true`；二进制块直接解码，不经过 UTF-8 通道。
//! 原入口保留 query 的字段及未知元数据，以本地 JSON `data` 数组返回全部行；
//! 输出编码字节与整体 deadline 均受配置约束，不把本地表示当作 wire 协议字段。
//! 仍为单连接短会话，不含连接池或跨连接并发；不支持 V2 块格式及未确认的类型布局。
//!
//! - `raw_block`：V3 块解码与结果值类型；
//! - `rows`：结果集会话、路由、释放语义与私有有界 JSON 输出。

use futures_util::{Stream, StreamExt};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::connect_async_with_config;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tracing::debug;

use crate::config::{TaosConfig, TransportMode};
use crate::error::{TaosError, TaosResult};

mod raw_block;
mod rows;

pub use raw_block::{WsQueryResult, WsValue};
pub use rows::exec_sql_ws_rows;

/// 构建原生 WS URL（委托配置的 `native_ws_url`，纯函数）。
///
/// # Examples
///
/// ```
/// use taosx::{build_native_ws_url, TaosConfig};
///
/// let config = TaosConfig::default();
/// assert_eq!(build_native_ws_url(&config), "ws://127.0.0.1:6041/rest/ws");
/// ```
#[must_use]
pub fn build_native_ws_url(config: &TaosConfig) -> String {
    config.native_ws_url()
}

/// 校验传输模式与配置一致性。
///
/// 先做完整 [`TaosConfig::validate`]，再确认传输模式属于已知取值。
pub fn validate_mode(config: &TaosConfig) -> TaosResult<()> {
    config.validate()?;
    match config.transport {
        TransportMode::Rest | TransportMode::NativeWs => Ok(()),
    }
}

/// 尝试建立原生 WebSocket 连接（受 `config.timeout` 约束）。
///
/// 成功时立即关闭连接并返回 `Ok(())`：本函数只验证可达性与握手，不维持长连接
/// 会话。离线环境返回 [`TaosError::Unavailable`] 或 [`TaosError::Timeout`]。
/// 结果计入进程级 `ws_probe_*` 计数（见 [`crate::ws_probe_totals`]）。
pub async fn connect_native_ws(config: &TaosConfig) -> TaosResult<()> {
    validate_mode(config)?;
    if config.transport != TransportMode::NativeWs {
        return Err(TaosError::Config(
            "connect_native_ws 要求 TransportMode::NativeWs".to_owned(),
        ));
    }
    let url = build_native_ws_url(config);
    debug!(target: "taosx", %url, "taos native ws connect attempt");

    let attempt = async {
        let (mut socket, _response) = connect_async(&url)
            .await
            .map_err(|error| TaosError::Unavailable(format!("ws 握手失败: {error}")))?;
        socket
            .close(None)
            .await
            .map_err(|error| TaosError::Unavailable(format!("ws 关闭失败: {error}")))
    };
    let result = match tokio::time::timeout(config.timeout, attempt).await {
        Ok(result) => result,
        Err(_) => Err(TaosError::Timeout(format!("native ws 连接超时: {url}"))),
    };
    // `is_ok()` 在此是「统计成功个数」（喂给 ws_probe 指标），不是断言判定，
    // 故保留布尔取值、不改为类型匹配。
    crate::metrics::record_ws_probe(result.is_ok());
    result
}

/// 由 `max_response_bytes` 构造 WS 客户端配置（纯函数）。
///
/// WS 路径与 REST 路径共用同一响应体积限额策略：帧与消息上限均绑定
/// `config.max_response_bytes`，防止服务端超大帧导致无界内存放大。
/// 仅供 [`exec_sql_ws`] / [`exec_sql_ws_rows`]（需要读取数据帧）使用；[`connect_native_ws`] 只做
/// 握手探测、不读数据帧，保持默认配置。
fn ws_config_from(config: &TaosConfig) -> WebSocketConfig {
    // WebSocketConfig 为 #[non_exhaustive]，跨 crate 须经 default() 后赋值字段。
    let mut ws_config = WebSocketConfig::default();
    ws_config.max_frame_size = Some(config.max_response_bytes);
    ws_config.max_message_size = Some(config.max_response_bytes);
    ws_config
}

/// 短会话 WS SQL：同一次查询读取全部行，保留原 query 元数据并返回本地 JSON。
///
/// `data` 是按列顺序排列的行数组，不是服务端 wire 字段承诺；整数保持精确值，
/// 时间戳保持原整数并保留 query 的 `precision`，原始字节表示为字节值数组。
/// 非查询语句不 fetch，保留更新元数据并返回空 `data`；未知元数据不丢弃。
///
/// 复用类型化读取链的 req_id、句柄、释放、行数和块预算规则；整体 timeout 包括
/// 同步 JSON 转换，成功计数仅在转换完成且未超时后记录一次。
/// 最终编码字节（含元数据、转义和字节数组）不得超过 `max_response_bytes`。
/// 非有限浮点数返回序列化错误；空 SQL、协议错误与超时保持具名拒绝。
/// 服务端 message 与凭据不入错误消息。
pub async fn exec_sql_ws(config: &TaosConfig, sql: &str) -> TaosResult<String> {
    config.validate()?;
    if sql.trim().is_empty() {
        return Err(TaosError::Invalid("exec_sql_ws: 空 SQL".to_owned()));
    }
    let deadline = std::time::Instant::now()
        .checked_add(config.timeout)
        .ok_or_else(|| TaosError::Invalid("ws sql deadline 超出范围".to_owned()))?;
    let url = build_native_ws_url(config);
    let ws_config = ws_config_from(config);
    let attempt = async {
        let (mut socket, _response) = connect_async_with_config(&url, Some(ws_config), false)
            .await
            .map_err(|error| TaosError::Unavailable(format!("ws 连接失败: {error}")))?;
        let result = rows::read_result_set(&mut socket, config, sql).await;
        if let Err(error) = socket.close(None).await {
            debug!(target: "taosx", %error, "ws 关闭失败（不覆盖读取结果）");
        }
        let (metadata, result) = result?;
        rows::encode_result_json(&metadata, &result, config.max_response_bytes, deadline)
    };
    match tokio::time::timeout(config.timeout, attempt).await {
        Ok(result) => {
            crate::metrics::record_ws_probe(result.is_ok());
            result
        }
        Err(_) => {
            crate::metrics::record_ws_probe(false);
            Err(TaosError::Timeout(format!("ws sql 超时: {url}")))
        }
    }
}

/// 读取下一数据帧，保留 Text / Binary 类型与原始载荷。
///
/// 流结束（服务端未返回帧）或读失败一律返回 [`TaosError::Unavailable`]，不伪造成功。
async fn read_frame<S>(socket: &mut S) -> TaosResult<Message>
where
    S: Stream<Item = Result<Message, WsError>> + Unpin,
{
    match socket.next().await {
        Some(Ok(message @ (Message::Text(_) | Message::Binary(_)))) => Ok(message),
        Some(Ok(other)) => Err(TaosError::Unavailable(format!(
            "ws 响应帧类型非法（期望 Text/Binary），实际为 {}",
            frame_kind(&other)
        ))),
        Some(Err(error)) => Err(TaosError::Unavailable(format!("ws 读失败: {error}"))),
        None => Err(TaosError::Unavailable(
            "ws 响应流已结束：服务端未返回帧".to_owned(),
        )),
    }
}

/// 为 conn / query 的 JSON 消费严格读取 UTF-8 载荷。
///
/// 只有 `Text` / `Binary` 两种数据帧参与协议解析；其余帧（`Ping` / `Pong` / `Close`）
/// 视为协议异常返回 [`TaosError::Unavailable`]，保持 fail-closed。
fn frame_payload(message: Message) -> TaosResult<String> {
    match message {
        Message::Text(text) => Ok(text.to_string()),
        Message::Binary(bytes) => std::str::from_utf8(&bytes)
            .map(str::to_owned)
            .map_err(|_| TaosError::Unavailable("ws JSON 响应不是合法 UTF-8".to_owned())),
        other => Err(TaosError::Unavailable(format!(
            "ws 响应帧类型非法（期望 Text/Binary），实际为 {}",
            frame_kind(&other)
        ))),
    }
}

/// WS 帧类型标签（用于错误消息；不回显帧载荷）。
fn frame_kind(message: &Message) -> &'static str {
    if message.is_text() {
        "Text"
    } else if message.is_binary() {
        "Binary"
    } else if message.is_ping() {
        "Ping"
    } else if message.is_pong() {
        "Pong"
    } else if message.is_close() {
        "Close"
    } else {
        "Frame"
    }
}

/// 校验某一步响应帧的状态码：仅 `code == 0` 视为成功。
///
/// 缺失或畸形的状态字段由 [`parse_status_code`] 判为失败，此处只处理非 0 错误码——
/// 正是先前「把 `code:65535` 信封当成功返回」的 fail-open 缺陷所在。
fn ensure_code_zero(payload: &str, stage: &str) -> TaosResult<()> {
    let code = parse_status_code(payload)?;
    if code == 0 {
        return Ok(());
    }
    Err(TaosError::Unavailable(format!(
        "ws {stage} 被服务端拒绝: code={code}"
    )))
}

/// 严格解析响应帧状态码：只有「明确读到整数 `code`」才返回 `Ok(code)`。
///
/// 以下情形一律返回 [`TaosError::Unavailable`]，**不把缺失/畸形状态当作成功**：
///
/// - 载荷不是合法 JSON；
/// - JSON 合法但顶层不是对象，或缺少 `code` 字段；
/// - `code` 存在但不是整数（例如字符串 `"0"`、浮点或布尔）。
fn parse_status_code(payload: &str) -> TaosResult<i32> {
    let value: serde_json::Value = serde_json::from_str(payload)
        .map_err(|error| TaosError::Unavailable(format!("ws 响应不是合法 JSON: {error}")))?;
    let code = value
        .get("code")
        .ok_or_else(|| TaosError::Unavailable("ws 响应缺少 code 字段".to_owned()))?;
    match code.as_i64().and_then(|raw| i32::try_from(raw).ok()) {
        Some(code) => Ok(code),
        None => Err(TaosError::Unavailable(format!(
            "ws 响应的 code 字段不是整数（实际类型为 {}）",
            json_type_name(code)
        ))),
    }
}

/// JSON 值的类型名（用于错误消息；不回显其内容）。
fn json_type_name(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "布尔",
        serde_json::Value::Number(_) => "数字",
        serde_json::Value::String(_) => "字符串",
        serde_json::Value::Array(_) => "数组",
        serde_json::Value::Object(_) => "对象",
    }
}

/// 原生 TCP 端口可达性探测（Native SQL / FFI 前置；不发送协议握手帧）。
pub async fn probe_native_tcp(config: &TaosConfig, native_port: u16) -> TaosResult<()> {
    config.validate()?;
    if native_port == 0 {
        return Err(TaosError::Invalid("native_port 非法".to_owned()));
    }
    let address = format!("{}:{}", config.host, native_port);
    let attempt = tokio::net::TcpStream::connect(&address);
    match tokio::time::timeout(config.timeout, attempt).await {
        Ok(Ok(_stream)) => Ok(()),
        Ok(Err(error)) => Err(TaosError::Unavailable(format!(
            "native tcp 连接失败: {error}"
        ))),
        Err(_) => Err(TaosError::Timeout(format!("native tcp 超时: {address}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn url_builder_and_mode() {
        let config = TaosConfig {
            host: "localhost".into(),
            port: 6041,
            transport: TransportMode::NativeWs,
            ..TaosConfig::default()
        };
        assert_eq!(build_native_ws_url(&config), "ws://localhost:6041/rest/ws");
        validate_mode(&config).expect("合法配置必须通过");

        let bad = TaosConfig {
            max_in_flight: 0,
            ..config
        };
        let error = validate_mode(&bad).expect_err("max_in_flight=0 必须拒绝");
        assert!(matches!(error, TaosError::Config(_)), "{error:?}");
    }

    #[tokio::test]
    async fn native_connect_rejects_rest_mode() {
        let config = TaosConfig {
            transport: TransportMode::Rest,
            timeout: Duration::from_millis(100),
            ..TaosConfig::default()
        };
        let error = connect_native_ws(&config)
            .await
            .expect_err("Rest 模式必须拒绝");
        assert!(matches!(error, TaosError::Config(_)));
    }

    #[tokio::test]
    async fn exec_sql_ws_rejects_empty_sql() {
        let config = TaosConfig {
            transport: TransportMode::NativeWs,
            timeout: Duration::from_millis(100),
            ..TaosConfig::default()
        };
        let error = exec_sql_ws(&config, "   ")
            .await
            .expect_err("空 SQL 必须拒绝");
        assert!(matches!(error, TaosError::Invalid(_)));
    }

    #[tokio::test]
    async fn probe_native_tcp_rejects_zero_port() {
        let error = probe_native_tcp(&TaosConfig::default(), 0)
            .await
            .expect_err("0 必须拒绝");
        assert!(matches!(error, TaosError::Invalid(_)));
    }

    /// 状态码解析必须严格：只有「明确的整数 `code`」才被接受。
    #[test]
    fn status_code_parsing_is_strict() {
        assert_eq!(parse_status_code(r#"{"code":0}"#).expect("code 0"), 0);
        assert_eq!(
            parse_status_code(r#"{"code":65535,"message":"server not connected"}"#)
                .expect("code 65535"),
            65535
        );
        // 以下每类畸形状态都必须 fail-closed 为 `Unavailable`（而非误判成功）。
        for (payload, why) in [
            ("not json", "非 JSON"),
            (r#"[1,2]"#, "顶层非对象"),
            (r#"{"action":"conn","req_id":0}"#, "缺 code 字段"),
            (r#"{"code":"0"}"#, "code 为字符串"),
            (r#"{"code":0.5}"#, "code 为浮点"),
            (r#"{"code":true}"#, "code 为布尔"),
            (r#"{"code":null}"#, "code 为 null"),
            // 超出 i32（不得截断或回绕）
            (r#"{"code":4294967296}"#, "code 超出 i32 取值域"),
        ] {
            let error = parse_status_code(payload).expect_err(why);
            assert!(
                matches!(error, TaosError::Unavailable(_)),
                "{why}: 必须 fail-closed 为 Unavailable，实际 {error:?}"
            );
        }
    }

    /// 非 0 状态码必须映射为 [`TaosError::Unavailable`]，且服务端 `message` 不入消息。
    #[test]
    fn ensure_code_zero_rejects_nonzero_code() {
        ensure_code_zero(r#"{"code":0}"#, "conn").expect("code=0 必须通过");

        let error = ensure_code_zero(r#"{"code":65535,"message":"server not connected"}"#, "conn")
            .expect_err("非 0 code 必须失败");
        assert!(matches!(error, TaosError::Unavailable(_)));
        let rendered = error.to_string();
        assert!(rendered.contains("65535"), "必须保留错误码: {rendered}");
        assert!(rendered.contains("conn"), "必须点明阶段: {rendered}");
        assert!(
            !rendered.contains("server not connected"),
            "服务端 message 文本不得进入错误消息: {rendered}"
        );
    }

    /// 畸形状态的错误消息必须点明「缺 code」/「类型不对」，便于诊断。
    #[test]
    fn malformed_status_messages_are_descriptive() {
        let missing = parse_status_code(r#"{"action":"conn"}"#).expect_err("缺字段必须失败");
        assert!(missing.to_string().contains("code"), "{missing}");

        let wrong = parse_status_code(r#"{"code":"0"}"#).expect_err("类型错必须失败");
        let rendered = wrong.to_string();
        assert!(rendered.contains("不是整数"), "{rendered}");
        assert!(rendered.contains("字符串"), "必须报出实际类型: {rendered}");
    }

    /// JSON 类型名覆盖全部取值域。
    #[test]
    fn json_type_name_covers_all_variants() {
        assert_eq!(json_type_name(&serde_json::Value::Null), "null");
        assert_eq!(json_type_name(&serde_json::json!(true)), "布尔");
        assert_eq!(json_type_name(&serde_json::json!(1)), "数字");
        assert_eq!(json_type_name(&serde_json::json!("x")), "字符串");
        assert_eq!(json_type_name(&serde_json::json!([])), "数组");
        assert_eq!(json_type_name(&serde_json::json!({})), "对象");
    }

    /// 帧类型标签覆盖全部数据/控制帧。
    #[test]
    fn frame_kind_labels_are_stable() {
        assert_eq!(frame_kind(&Message::Text("x".into())), "Text");
        assert_eq!(frame_kind(&Message::Binary(vec![].into())), "Binary");
        assert_eq!(frame_kind(&Message::Ping(vec![].into())), "Ping");
        assert_eq!(frame_kind(&Message::Pong(vec![].into())), "Pong");
        assert_eq!(frame_kind(&Message::Close(None)), "Close");
    }

    /// 只有 `Text` / `Binary` 帧产生载荷；控制帧一律 fail-closed。
    #[test]
    fn frame_payload_accepts_data_frames_only() {
        assert_eq!(
            frame_payload(Message::Text(r#"{"code":0}"#.into())).expect("Text"),
            r#"{"code":0}"#
        );
        assert_eq!(
            frame_payload(Message::Binary(b"{\"code\":0}".to_vec().into())).expect("Binary"),
            r#"{"code":0}"#
        );
        for control in [
            Message::Ping(vec![].into()),
            Message::Pong(vec![].into()),
            Message::Close(None),
        ] {
            let error = frame_payload(control).expect_err("控制帧必须失败");
            assert!(
                matches!(error, TaosError::Unavailable(_)),
                "控制帧必须映射为 Unavailable: {error:?}"
            );
        }
    }

    #[test]
    fn binary_frame_with_invalid_utf8_fails_closed() {
        let error = frame_payload(Message::Binary(vec![0xff, 0xfe].into()))
            .expect_err("JSON 消费不得有损解码非法 UTF-8");
        assert!(matches!(error, TaosError::Unavailable(_)), "{error:?}");
        assert!(error.to_string().contains("UTF-8"));
    }

    #[tokio::test]
    async fn read_frame_preserves_text_and_binary_bytes() {
        let bytes = vec![
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 0xff, 42, 0,
        ];
        let expected_binary = Message::Binary(bytes.clone().into());
        let mut binary_stream =
            futures_util::stream::iter([Ok::<_, WsError>(expected_binary.clone())]);
        let received = read_frame(&mut binary_stream)
            .await
            .expect("原始二进制数据帧");
        assert_eq!(received, expected_binary);
        match &received {
            Message::Binary(actual) => assert_eq!(actual.as_ref(), bytes.as_slice()),
            other => panic!("不得改变 Binary 类型：{}", frame_kind(other)),
        }
        let error = frame_payload(received).expect_err("仅 JSON 消费阶段拒绝非法 UTF-8");
        assert!(matches!(error, TaosError::Unavailable(_)));
        assert_eq!(
            error.to_string(),
            "远端暂时不可用: ws JSON 响应不是合法 UTF-8"
        );

        let expected_text = Message::Text(r#"{"code":0,"message":"合成"}"#.into());
        let mut text_stream = futures_util::stream::iter([Ok::<_, WsError>(expected_text.clone())]);
        let received = read_frame(&mut text_stream).await.expect("文本帧");
        assert_eq!(received, expected_text);
        assert!(matches!(&received, Message::Text(_)));
        let payload = frame_payload(received).expect("合法 Text JSON");
        assert_eq!(payload, r#"{"code":0,"message":"合成"}"#);
        ensure_code_zero(&payload, "query").expect("合法状态");

        let expected_binary = Message::Binary(br#"{"code":0}"#.to_vec().into());
        let mut binary_stream =
            futures_util::stream::iter([Ok::<_, WsError>(expected_binary.clone())]);
        let received = read_frame(&mut binary_stream)
            .await
            .expect("合法 UTF-8 Binary");
        assert_eq!(received, expected_binary);
        let payload = frame_payload(received).expect("合法 Binary JSON");
        assert_eq!(payload, r#"{"code":0}"#);
        ensure_code_zero(&payload, "conn").expect("合法状态");
    }

    #[tokio::test]
    async fn read_frame_defers_json_validation_to_consumer() {
        let expected = Message::Binary(b"not json".to_vec().into());
        let mut stream = futures_util::stream::iter([Ok::<_, WsError>(expected.clone())]);
        let received = read_frame(&mut stream).await.expect("读取层不解释数据帧");
        assert_eq!(received, expected);
        let payload = frame_payload(received).expect("UTF-8 载荷");
        assert_eq!(payload, "not json");
        let error = ensure_code_zero(&payload, "query").expect_err("JSON 消费必须拒绝");
        assert!(matches!(error, TaosError::Unavailable(_)));
        assert!(!error.to_string().contains("not json"));
    }

    #[tokio::test]
    async fn read_frame_rejects_control_eof_and_read_error() {
        for control in [
            Message::Ping(vec![0xff].into()),
            Message::Pong(vec![0xff].into()),
            Message::Close(None),
        ] {
            let mut stream = futures_util::stream::iter([Ok::<_, WsError>(control)]);
            let error = read_frame(&mut stream).await.expect_err("控制帧必须拒绝");
            assert!(matches!(error, TaosError::Unavailable(_)));
            assert!(error.to_string().contains("帧类型非法"));
        }
        let mut empty = futures_util::stream::empty::<Result<Message, WsError>>();
        let error = read_frame(&mut empty).await.expect_err("EOF 不得伪造数据");
        assert!(matches!(error, TaosError::Unavailable(_)));
        assert!(error.to_string().contains("响应流已结束"));
        let mut failed = futures_util::stream::iter([Err::<Message, _>(WsError::ConnectionClosed)]);
        let error = read_frame(&mut failed).await.expect_err("读错误必须拒绝");
        assert!(matches!(error, TaosError::Unavailable(_)));
        assert!(error.to_string().contains("ws 读失败"));
    }

    #[test]
    fn original_json_output_keeps_unknown_large_exponent() {
        let metadata: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"extra":1e400}"#).expect("未知大指数 JSON 数值");
        let result = WsQueryResult {
            fields_names: Vec::new(),
            fields_types: Vec::new(),
            precision: crate::TsPrecision::Ns,
            rows: Vec::new(),
        };
        let output = rows::encode_result_json(
            &metadata,
            &result,
            1024,
            std::time::Instant::now() + Duration::from_secs(5),
        )
        .expect("保留数值原值");
        let decoded: serde_json::Value = serde_json::from_str(&output).expect("输出 JSON");
        let mut expected: serde_json::Value =
            serde_json::from_str(r#"{"extra":1e400}"#).expect("独立预期数值");
        expected["data"] = serde_json::Value::Array(Vec::new());
        assert_eq!(decoded, expected);
        assert!(decoded["extra"].is_number());
        assert!(!decoded["extra"].is_string());
        assert_eq!(decoded["extra"], metadata["extra"]);
        assert_eq!(decoded["data"], serde_json::Value::Array(Vec::new()));
    }

    #[test]
    fn ws_config_links_max_response_bytes() {
        let config = TaosConfig {
            max_response_bytes: 1234,
            ..TaosConfig::default()
        };
        let ws = ws_config_from(&config);
        assert_eq!(ws.max_frame_size, Some(1234), "帧上限必须联动配置");
        assert_eq!(ws.max_message_size, Some(1234), "消息上限必须联动配置");
    }
}
