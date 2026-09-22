//! SQL 构造与安全校验。
//!
//! 自 `client.rs` 拆出（生产段超 800 行的拆分）；`build_insert_sql_chunks` 仍由门面
//! 以 `pub use` 重导出，外部路径不变。

use std::fmt::Write as _;

use crate::config::{TsPrecision, HARD_MAX_BATCH_BYTES, HARD_MAX_BATCH_ROWS, MAX_IDENT_BYTES};
use crate::error::{TaosError, TaosResult};
use crate::point::TaosPoint;

/// 多子表 INSERT 前缀。
pub(super) const INSERT_PREFIX: &str = "INSERT INTO ";

/// 超级表名最大 UTF-8 字节数。
pub(super) const MAX_STABLE_NAME_BYTES: usize = 94;

/// tag 值最大 UTF-8 字节数（十六进制编码后进入子表名）。
pub(super) const MAX_SYMBOL_BYTES: usize = 48;

/// 构建分块 INSERT SQL（纯函数；由调用方驱动 chunk 尺寸）。
///
/// 每个 chunk 生成一条多子表 `INSERT INTO ... USING ... TAGS (...) VALUES (...)`：
///
/// - `table` 必须是合法标识符（字母/下划线开头、≤94 字节），否则返回
///   [`TaosError::Invalid`]；
/// - 子表名由 `table` + tag 值的十六进制编码构成，tag 值不直接进入标识符；
/// - 字符串字面量按 TDengine 规则转义（`\` → `\\`、`'` → `\'`）；
/// - 时间戳按 `precision` 换算，**未对齐目标精度时 fail-closed**，不静默截断。
///
/// `max_rows` 必须在 `1..=HARD_MAX_BATCH_ROWS` 且单行不得超过 [`HARD_MAX_BATCH_BYTES`]。
pub fn build_insert_sql_chunks(
    table: &str,
    points: &[TaosPoint],
    precision: TsPrecision,
    max_rows: usize,
) -> TaosResult<Vec<String>> {
    Ok(build_insert_sql_chunks_with_limits(
        table,
        points,
        precision,
        max_rows,
        HARD_MAX_BATCH_BYTES,
    )?
    .into_iter()
    .map(|(sql, _rows)| sql)
    .collect())
}

/// 单个 SQL chunk 及其行数。
pub(super) type SqlChunk = (String, usize);

/// 带字节上限的分块构造；返回 `(sql, 行数)` 以支持精确的部分成功报告。
pub(super) fn build_insert_sql_chunks_with_limits(
    table: &str,
    points: &[TaosPoint],
    precision: TsPrecision,
    max_rows: usize,
    max_bytes: usize,
) -> TaosResult<Vec<SqlChunk>> {
    validate_stable_ident(table)?;
    if max_rows == 0 || max_rows > HARD_MAX_BATCH_ROWS {
        return Err(TaosError::Invalid(format!(
            "max_rows 必须为 1..={HARD_MAX_BATCH_ROWS}"
        )));
    }
    if max_bytes < INSERT_PREFIX.len() || max_bytes > HARD_MAX_BATCH_BYTES {
        return Err(TaosError::Invalid(format!(
            "max_bytes 必须为 {}..={HARD_MAX_BATCH_BYTES}",
            INSERT_PREFIX.len()
        )));
    }
    if points.is_empty() {
        return Ok(Vec::new());
    }

    let mut chunks = Vec::new();
    let mut sql = String::from(INSERT_PREFIX);
    let mut rows = 0usize;
    for point in points {
        let subtable = subtable_name(table, &point.tag_value)?;
        let tag = escape_str(&point.tag_value);
        let timestamp = encode_timestamp(point.timestamp_ns, precision)?;
        let first = escape_str(&point.values[0]);
        let second = escape_str(&point.values[1]);
        let row = format!(
            "`{subtable}` USING `{table}` TAGS ('{tag}') VALUES ({timestamp},'{first}','{second}')"
        );
        let separator = usize::from(rows > 0);
        let next_len = sql
            .len()
            .checked_add(separator)
            .and_then(|length| length.checked_add(row.len()))
            .ok_or_else(|| TaosError::Invalid("批量 SQL 字节数溢出".to_owned()))?;
        if rows > 0 && (rows >= max_rows || next_len > max_bytes) {
            chunks.push((sql, rows));
            sql = String::from(INSERT_PREFIX);
            rows = 0;
        }
        let row_len = INSERT_PREFIX
            .len()
            .checked_add(row.len())
            .ok_or_else(|| TaosError::Invalid("单行 SQL 字节数溢出".to_owned()))?;
        if row_len > max_bytes {
            return Err(TaosError::Invalid(format!(
                "单行 SQL 超过 batch_max_bytes={max_bytes}"
            )));
        }
        if rows > 0 {
            sql.push(' ');
        }
        sql.push_str(&row);
        rows += 1;
    }
    if rows > 0 {
        chunks.push((sql, rows));
    }
    Ok(chunks)
}

/// 供流式 API 使用：分块提示必须 ≥ 1。
pub(crate) fn validate_chunk_hint(chunk_hint: usize) -> TaosResult<()> {
    if chunk_hint == 0 {
        return Err(TaosError::Invalid("chunk_hint 必须 ≥ 1".to_owned()));
    }
    Ok(())
}

/// 子表名：`{stable}_s{tag 值十六进制}`（tag 值不直接进入标识符）。
pub(super) fn subtable_name(stable: &str, tag_value: &str) -> TaosResult<String> {
    validate_stable_ident(stable)?;
    if tag_value.len() > MAX_SYMBOL_BYTES {
        return Err(TaosError::Invalid(format!(
            "tag 值超过 {MAX_SYMBOL_BYTES} UTF-8 字节"
        )));
    }
    let mut encoded = String::with_capacity(tag_value.len().saturating_mul(2));
    for byte in tag_value.as_bytes() {
        write!(&mut encoded, "{byte:02x}")
            .map_err(|_| TaosError::Invalid("tag 子表编码失败".to_owned()))?;
    }
    let name = format!("{stable}_s{encoded}");
    validate_ident(&name)?;
    Ok(name)
}

/// 超级表名校验（合法标识符且 ≤ [`MAX_STABLE_NAME_BYTES`] 字节）。
pub(super) fn validate_stable_ident(name: &str) -> TaosResult<()> {
    validate_ident(name)?;
    if name.len() > MAX_STABLE_NAME_BYTES {
        return Err(TaosError::Invalid(format!(
            "stable 名称超过 {MAX_STABLE_NAME_BYTES} 字节"
        )));
    }
    Ok(())
}

/// SQL 标识符白名单校验（防注入）。
pub(super) fn validate_ident(name: &str) -> TaosResult<()> {
    if name.is_empty() || name.len() > MAX_IDENT_BYTES {
        return Err(TaosError::Invalid("非法标识符长度".to_owned()));
    }
    let mut characters = name.chars();
    let Some(first) = characters.next() else {
        return Err(TaosError::Invalid("空标识符".to_owned()));
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(TaosError::Invalid("标识符须以字母或下划线开头".to_owned()));
    }
    if !characters.all(|character| character.is_ascii_alphanumeric() || character == '_') {
        return Err(TaosError::Invalid("标识符含非法字符".to_owned()));
    }
    Ok(())
}

/// SQL 字符串字面量转义（`\` → `\\`，`'` → `\'`）。
///
/// `pub(super)`（=`pub(in crate::client)`）：与 `validate_ident` 对称，对 `client`
/// 子树（含 `pool`）可见，不构成公开 API 面（R-API-001）。
pub(super) fn escape_str(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "\\'")
}

/// 纳秒时间戳 → 目标精度数值；不允许静默精度损失。
pub(super) fn encode_timestamp(timestamp_ns: i64, precision: TsPrecision) -> TaosResult<i64> {
    let stored = precision.from_nanos(timestamp_ns);
    if precision.to_nanos(stored) != timestamp_ns {
        return Err(TaosError::Invalid(format!(
            "时间戳 {timestamp_ns} ns 无法无损表示为 {} 精度（请对齐精度或改用 ns 库）",
            precision.as_str()
        )));
    }
    Ok(stored)
}

#[cfg(test)]
mod tests {
    use super::escape_str;

    #[test]
    fn plain_text_passes_through() {
        assert_eq!(escape_str("test"), "test");
    }

    #[test]
    fn single_quote_is_escaped() {
        assert_eq!(escape_str("it's"), "it\\'s");
    }

    #[test]
    fn backslash_is_escaped() {
        assert_eq!(escape_str("a\\b"), "a\\\\b");
    }
}
