//! 配置解析辅助：TOML 字段反序列化、环境变量读取、主机与标识符校验。
//!
//! 自 `config.rs` 拆出（生产段超 800 行的拆分）；只服务于
//! [`crate::config::TaosConfig`] 与 [`crate::config::TaosConfigBuilder`] 的构造与校验，
//! 不构成独立公共面。门面对应项以 `use self::parse::…` 引用。

use std::time::Duration;

use serde::Deserialize;

use crate::error::{TaosError, TaosResult};

use super::{TransportMode, TsPrecision};

/// TOML 中毫秒字段（`timeout_ms` 等）的解析器。
pub(super) fn de_millis<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let millis = <u64 as Deserialize>::deserialize(deserializer)?;
    Ok(Duration::from_millis(millis))
}

/// TOML 中可选精度字段的解析器（`ms` / `us` / `ns`）。
pub(super) fn de_optional_precision<'de, D>(
    deserializer: D,
) -> Result<Option<TsPrecision>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = <Option<String> as Deserialize>::deserialize(deserializer)?;
    match raw {
        None => Ok(None),
        Some(value) => TsPrecision::parse(&value).map(Some).ok_or_else(|| {
            serde::de::Error::custom(format!("precision 非法: {value}（期望 ms|us|ns）"))
        }),
    }
}

/// TOML 中传输模式字段的解析器（`rest` / `native` / `ws`）。
pub(super) fn de_transport<'de, D>(deserializer: D) -> Result<TransportMode, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = <String as Deserialize>::deserialize(deserializer)?;
    TransportMode::parse(&raw).ok_or_else(|| {
        serde::de::Error::custom(format!("transport 非法: {raw}（期望 rest|native|ws）"))
    })
}

/// 读取非空环境变量（不做 trim）。
pub(super) fn env_non_empty(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

/// 读取 trim 后非空的环境变量。
pub(super) fn env_trimmed(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

/// 读取并解析环境变量；解析失败只报告变量名，不回显取值。
pub(super) fn env_parsed<T>(name: &str) -> TaosResult<Option<T>>
where
    T: std::str::FromStr,
{
    match std::env::var(name) {
        Ok(value) => value
            .trim()
            .parse::<T>()
            .map(Some)
            .map_err(|_| TaosError::Config(format!("环境变量 {name} 取值非法"))),
        Err(_) => Ok(None),
    }
}

/// 读取布尔型环境变量，兼容 `1/0`、`true/false`、`yes/no`、`on/off`。
pub(super) fn env_bool(name: &str) -> TaosResult<Option<bool>> {
    let Some(value) = env_trimmed(name) else {
        return Ok(None);
    };
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(Some(true)),
        "0" | "false" | "no" | "off" => Ok(Some(false)),
        _ => Err(TaosError::Config(format!("环境变量 {name} 取值非法"))),
    }
}

/// 判断主机是否为 loopback（`localhost` 或环回 IP）。
pub(super) fn host_is_loopback(host: &str) -> bool {
    let host = host
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(host);
    host.eq_ignore_ascii_case("localhost")
        || host.eq_ignore_ascii_case("localhost.")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// 主机名字面量校验（拒绝 URL、凭据、空白与路径成分）。
pub(super) fn valid_host(host: &str) -> bool {
    let trimmed = host.trim();
    if trimmed.is_empty()
        || trimmed != host
        || trimmed.contains("//")
        || trimmed.contains('@')
        || trimmed.contains('/')
        || trimmed.contains('\\')
        || trimmed.contains('?')
        || trimmed.contains('#')
        || trimmed.chars().any(char::is_whitespace)
    {
        return false;
    }
    let unbracketed = trimmed
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(trimmed);
    if unbracketed.contains(':') {
        return unbracketed.parse::<std::net::IpAddr>().is_ok();
    }
    unbracketed
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '.' | '-'))
}

/// SQL 标识符校验（字母或下划线开头，仅含字母数字与下划线）。
pub(super) fn valid_ident(value: &str) -> bool {
    let mut characters = value.chars();
    characters
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
}

/// IPv6 主机在 URL 中需要用方括号包裹。
pub(super) fn url_host(host: &str) -> String {
    if host.starts_with('[') || !host.contains(':') {
        host.to_string()
    } else {
        format!("[{host}]")
    }
}
