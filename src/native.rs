//! TDengine 原生 WebSocket 传输（`/rest/ws`）：握手探测与短会话 SQL。
//!
//! 与 REST 相比，WebSocket 走同端口（默认 6041）的长连接通道，适合需要频繁小
//! 请求的场景。本模块只实现「有 deadline 的短会话」：握手 → 发送 → 读取首帧 →
//! 关闭；不维持连接池，因此不引入额外的连接状态机。

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::debug;

use crate::config::{TaosConfig, TransportMode};
use crate::error::{TaosError, TaosResult};

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
    crate::metrics::record_ws_probe(result.is_ok());
    result
}

/// 短会话 WS SQL：连接 `/rest/ws`，发送查询文本，读取首帧文本响应后关闭。
///
/// 返回服务端首帧内容；无 `Text` / `Binary` 帧时 fail-closed（返回
/// [`TaosError::Unavailable`]），不伪造成功。协议细节随服务端版本而异，失败统一
/// 映射为 [`TaosError::Unavailable`] / [`TaosError::Timeout`]。
pub async fn exec_sql_ws(config: &TaosConfig, sql: &str) -> TaosResult<String> {
    config.validate()?;
    if sql.trim().is_empty() {
        return Err(TaosError::Invalid("exec_sql_ws: 空 SQL".to_owned()));
    }
    let url = build_native_ws_url(config);
    let attempt = async {
        let (mut socket, _response) = connect_async(&url)
            .await
            .map_err(|error| TaosError::Unavailable(format!("ws 连接失败: {error}")))?;
        let payload = serde_json::json!({
            "action": "query",
            "args": { "sql": sql },
        })
        .to_string();
        socket
            .send(Message::Text(payload.into()))
            .await
            .map_err(|error| TaosError::Unavailable(format!("ws 发送失败: {error}")))?;
        let mut body = String::new();
        if let Some(message) = socket.next().await {
            match message {
                Ok(Message::Text(text)) => body = text.to_string(),
                Ok(Message::Binary(bytes)) => body = String::from_utf8_lossy(&bytes).into_owned(),
                Ok(_) => {}
                Err(error) => {
                    return Err(TaosError::Unavailable(format!("ws 读失败: {error}")));
                }
            }
        }
        let _ = socket.close(None).await;
        if body.is_empty() {
            return Err(TaosError::Unavailable(
                "ws sql 无响应体：服务端未返回 Text/Binary 帧".to_owned(),
            ));
        }
        Ok(body)
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
        assert!(validate_mode(&bad).is_err());
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
}
