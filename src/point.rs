//! TDengine 写入点 DTO 与建表/查询所需的最小技术行表示。

/// TDengine 写入点。
///
/// 该类型只描述当前物理表协议：一个纳秒时间戳、一个 tag 值（写入 `symbol`
/// tag）和两个 `NCHAR` 单元格（第 0、1 项分别写入 `bid`、`ask` 列）。列名仅为
/// 存量表兼容约束，不赋予 DTO 业务语义。
///
/// 时间戳始终以纳秒表示，写入时按数据库精度换算；未对齐目标精度的时间戳会在
/// [`crate::build_insert_sql_chunks`] 处 fail-closed，不会被静默截断。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaosPoint {
    /// 纳秒 epoch；写入时按数据库精度换算。
    pub timestamp_ns: i64,
    /// 写入 `symbol` tag 的原始技术标签值。
    pub tag_value: String,
    /// 写入两个 `NCHAR(64)` 数据列的原始文本值。
    pub values: [String; 2],
}

impl TaosPoint {
    /// 构造一个技术写入点。
    #[must_use]
    pub fn new(
        tag_value: impl Into<String>,
        timestamp_ns: i64,
        first_value: impl Into<String>,
        second_value: impl Into<String>,
    ) -> Self {
        Self {
            timestamp_ns,
            tag_value: tag_value.into(),
            values: [first_value.into(), second_value.into()],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructor_preserves_protocol_cells() {
        let point = TaosPoint::new("series-a", 42, "1.2300", "4.5600");
        assert_eq!(point.timestamp_ns, 42);
        assert_eq!(point.tag_value, "series-a");
        assert_eq!(point.values, ["1.2300", "4.5600"]);
    }

    #[test]
    fn constructor_accepts_owned_and_borrowed() {
        let owned = TaosPoint::new(
            String::from("A"),
            1,
            String::from("0.1"),
            String::from("0.2"),
        );
        let borrowed = TaosPoint::new("A", 1, "0.1", "0.2");
        assert_eq!(owned, borrowed);
    }
}
