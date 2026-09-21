//! TDengine REST 响应解析与限额读取。
//!
//! 自 `client.rs` 拆出；`RawResponse` 是响应结构的私有镜像，不进入公共面。

use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::config::TsPrecision;
use crate::error::{TaosError, TaosResult};

use super::types::TaosExecResult;

/// TDengine REST 响应体（仅取所需字段）。
#[derive(Debug, Deserialize)]
struct RawResponse {
    code: i32,
    #[serde(default)]
    desc: Option<String>,
    #[serde(default)]
    column_meta: Vec<serde_json::Value>,
    #[serde(default)]
    data: Vec<Vec<serde_json::Value>>,
    #[serde(default)]
    rows: Option<i64>,
}

/// 解析 TDengine REST JSON 响应。
pub(super) fn parse_taos_json(text: &str) -> TaosResult<TaosExecResult> {
    let raw: RawResponse = serde_json::from_str(text).map_err(|error| {
        TaosError::Serialization(format!(
            "TDengine JSON 解析失败（{error}）; body={}",
            truncate(text, 256)
        ))
    })?;

    if raw.code != 0 {
        return Err(TaosError::from_taos_code(
            raw.code,
            &raw.desc.unwrap_or_default(),
        ));
    }

    let columns = raw
        .column_meta
        .iter()
        .filter_map(|column| {
            column
                .as_array()
                .and_then(|entries| entries.first())
                .and_then(|value| value.as_str())
                .map(str::to_owned)
        })
        .collect::<Vec<_>>();

    let mut rows: Vec<Vec<String>> = Vec::with_capacity(raw.data.len());
    for row in raw.data {
        rows.push(row.iter().map(json_cell_to_string).collect());
    }

    let affected_rows = if columns.first().map(String::as_str) == Some("affected_rows") {
        rows.first()
            .and_then(|row| row.first())
            .and_then(|cell| cell.parse().ok())
    } else {
        raw.rows
    };

    Ok(TaosExecResult {
        code: raw.code,
        rows,
        columns,
        affected_rows,
    })
}

/// 读取响应体并强制 `max_bytes` 上限（同时约束 `Content-Length` 与分块流）。
pub(super) async fn read_response_limited(
    mut response: reqwest::Response,
    max_bytes: usize,
) -> TaosResult<String> {
    let limit = u64::try_from(max_bytes).unwrap_or(u64::MAX);
    if response
        .content_length()
        .is_some_and(|length| length > limit)
    {
        return Err(TaosError::Unavailable(format!(
            "响应超过 max_response_bytes={max_bytes}"
        )));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| TaosError::Connection(format!("读响应失败: {error}")))?
    {
        let next_len = body
            .len()
            .checked_add(chunk.len())
            .ok_or_else(|| TaosError::Unavailable("响应字节数溢出".to_owned()))?;
        if next_len > max_bytes {
            return Err(TaosError::Unavailable(format!(
                "响应超过 max_response_bytes={max_bytes}"
            )));
        }
        body.extend_from_slice(&chunk);
    }
    String::from_utf8(body)
        .map_err(|error| TaosError::Serialization(format!("响应不是 UTF-8（{error}）")))
}

/// 校验 `DESCRIBE` 结果：`bid` / `ask` 必须为 `NCHAR(64+)`。
pub(super) fn validate_decimal_schema(result: &TaosExecResult) -> TaosResult<()> {
    let mut bid_ok = false;
    let mut ask_ok = false;
    for row in &result.rows {
        if row.len() < 3 {
            continue;
        }
        let field = row[0].trim();
        let data_type = row[1].trim();
        let length_ok = row[2]
            .trim()
            .parse::<usize>()
            .is_ok_and(|length| length >= 64);
        let exact_text = data_type.eq_ignore_ascii_case("NCHAR") && length_ok;
        if field.eq_ignore_ascii_case("bid") {
            bid_ok = exact_text;
        } else if field.eq_ignore_ascii_case("ask") {
            ask_ok = exact_text;
        }
    }
    if !bid_ok || !ask_ok {
        return Err(TaosError::backend(
            "TDengine schema 不兼容：bid/ask 必须为 NCHAR(64+)；拒绝 DOUBLE 精度降级",
        ));
    }
    Ok(())
}

/// 解析时间戳单元格（库数值或 RFC3339 文本）。
pub(super) fn parse_ts_cell(raw: &str, precision: TsPrecision) -> TaosResult<i64> {
    if let Ok(value) = raw.parse::<i64>() {
        return Ok(precision.to_nanos(value));
    }
    if let Ok(parsed) = DateTime::parse_from_rfc3339(raw) {
        return Ok(parsed
            .timestamp_nanos_opt()
            .unwrap_or_else(|| parsed.timestamp().saturating_mul(1_000_000_000)));
    }
    if let Ok(parsed) = DateTime::<Utc>::from_str(raw) {
        return Ok(parsed
            .timestamp_nanos_opt()
            .unwrap_or_else(|| parsed.timestamp().saturating_mul(1_000_000_000)));
    }
    Err(TaosError::Invalid(format!("无法解析时间戳: {raw}")))
}

/// 截断文本用于错误消息（UTF-8 边界安全、单行化）。
pub(super) fn truncate(text: &str, max: usize) -> String {
    let mut trimmed = text.trim().replace('\n', " ");
    if trimmed.len() > max {
        let mut boundary = max;
        while !trimmed.is_char_boundary(boundary) {
            boundary -= 1;
        }
        trimmed.truncate(boundary);
        trimmed.push('…');
    }
    trimmed
}

/// JSON 单元格 → 字符串（保持调用方原始文本表示）。
fn json_cell_to_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => String::new(),
        serde_json::Value::String(text) => text.clone(),
        serde_json::Value::Bool(flag) => flag.to_string(),
        serde_json::Value::Number(number) => number.to_string(),
        other => other.to_string(),
    }
}
