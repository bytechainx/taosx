//! `taosx` 错误类型与 TDengine / HTTP 错误映射。
//!
//! 设计要点：
//!
//! - 远端业务错误统一落在 [`TaosError::Backend`]，并**保留 TDengine 响应中的
//!   数字 `code` 字段**（`0` 表示无错误码可用）。TDengine REST 响应里的 `code`
//!   是驱动级错误码：正数表示服务端拒绝（语法、表不存在、认证失败等），非正数
//!   表示客户端/内部错误，二者都不可重试；唯一例外是 `896`（服务端繁忙/超时），
//!   它被映射为可重试的 [`TaosError::Unavailable`]。
//! - 错误消息只保留 TDengine 错误码、HTTP 状态码与调用方提供的上下文；SQL 片段、
//!   响应正文与凭据一律不入消息，避免随日志外泄。

/// TDengine 服务端繁忙/超时错误码（可重试）。
const TDCODE_BUSY: i32 = 896;
/// TDengine「表不存在」错误码（`0x2603`）。
const TDCODE_TABLE_NOT_EXIST: i32 = 0x2603;
/// TDengine「表不存在」兼容错误码。
const TDCODE_TABLE_NOT_EXIST_LEGACY: i32 = 9826;

/// `taosx` 统一错误类型。
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TaosError {
    /// 配置非法（构造或校验阶段即可判定）。
    #[error("配置无效: {0}")]
    Config(String),
    /// 连接建立或维护失败（远端不可达、被拒绝、连接被中断）。
    #[error("连接失败: {0}")]
    Connection(String),
    /// 远端返回业务或协议错误；`code` 为 TDengine 错误码（`0` = 无可用错误码）。
    #[error("远端返回错误(code={code}): {message}")]
    Backend {
        /// TDengine 响应中的数字错误码。
        code: i32,
        /// 中文错误说明（不含 SQL 片段与响应正文）。
        message: String,
    },
    /// 远端暂时不可用（HTTP 429 / 5xx 或 TDengine 繁忙码），重试有机会成功。
    #[error("远端暂时不可用: {0}")]
    Unavailable(String),
    /// 序列化或解析失败（例如响应不是合法 TDengine JSON）。
    #[error("序列化失败: {0}")]
    Serialization(String),
    /// 网络或底层 I/O 失败。
    #[error("I/O 失败: {0}")]
    Io(#[from] std::io::Error),
    /// 操作超时（请求超时、获取 in-flight 许可超时或重试 deadline 耗尽）。
    #[error("操作超时: {0}")]
    Timeout(String),
    /// 调用参数非法（例如 SQL 标识符不合法、分块参数越界）。
    #[error("参数无效: {0}")]
    Invalid(String),
    /// 目标资源已关闭。
    #[error("资源已关闭: {0}")]
    Closed(String),
    /// 当前能力不支持。
    #[error("不支持的操作: {0}")]
    Unsupported(String),
}

impl TaosError {
    /// 构造无错误码的远端业务错误。
    #[must_use]
    pub fn backend(message: impl Into<String>) -> Self {
        Self::Backend {
            code: 0,
            message: message.into(),
        }
    }

    /// 由 TDengine 数字错误码构造错误（保留源码的语义分类）。
    ///
    /// 映射规则：
    ///
    /// | 输入 `code` | 结果 | 可重试 |
    /// | --- | --- | --- |
    /// | `896` | [`TaosError::Unavailable`] | 是 |
    /// | `0x2603` / `9826` | [`TaosError::Backend`]（`is_not_found()` 为真） | 否 |
    /// | 其它正数 | [`TaosError::Invalid`] | 否 |
    /// | 非正数 | [`TaosError::Backend`] | 否 |
    #[must_use]
    pub fn from_taos_code(code: i32, context: &str) -> Self {
        let message = if context.is_empty() {
            format!("taos code={code}")
        } else {
            format!("taos code={code}: {context}")
        };
        match code {
            TDCODE_BUSY => Self::Unavailable(message),
            TDCODE_TABLE_NOT_EXIST | TDCODE_TABLE_NOT_EXIST_LEGACY => {
                Self::Backend { code, message }
            }
            code if code > 0 => Self::Invalid(message),
            code => Self::Backend { code, message },
        }
    }

    /// 由 HTTP 状态码构造错误（响应正文不进入消息）。
    #[must_use]
    pub fn from_http_status(status: u16, context: &str) -> Self {
        let message = if context.is_empty() {
            format!("taos HTTP {status}")
        } else {
            format!("taos HTTP {status}: {context}")
        };
        match status {
            408 => Self::Timeout(message),
            429 => Self::Unavailable(message),
            500..=599 => Self::Unavailable(message),
            _ => Self::Backend { code: 0, message },
        }
    }

    /// 携带的 TDengine 错误码（仅 [`TaosError::Backend`] 可能返回）。
    #[must_use]
    pub fn taos_code(&self) -> Option<i32> {
        match self {
            Self::Backend { code, .. } => Some(*code),
            _ => None,
        }
    }

    /// 是否表示「目标表/库不存在」。
    ///
    /// 供查询路径把「首次查询尚无表」与真正的故障区分开。
    #[must_use]
    pub fn is_not_found(&self) -> bool {
        matches!(
            self.taos_code(),
            Some(TDCODE_TABLE_NOT_EXIST | TDCODE_TABLE_NOT_EXIST_LEGACY)
        )
    }

    /// 是否属于可以安全重试的瞬时错误。
    ///
    /// - 可重试：[`TaosError::Connection`]、[`TaosError::Unavailable`]、
    ///   [`TaosError::Timeout`]、[`TaosError::Io`]。
    /// - 不可重试：配置、参数、序列化、远端业务错误（含表不存在）与已关闭资源。
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Connection(_) | Self::Unavailable(_) | Self::Timeout(_) | Self::Io(_) => true,
            Self::Config(_)
            | Self::Backend { .. }
            | Self::Serialization(_)
            | Self::Invalid(_)
            | Self::Closed(_)
            | Self::Unsupported(_) => false,
        }
    }

    /// 替换错误消息并保留分类与 TDengine 错误码（用于补充上下文）。
    #[must_use]
    pub fn with_message(self, message: impl Into<String>) -> Self {
        let message = message.into();
        match self {
            Self::Config(_) => Self::Config(message),
            Self::Connection(_) => Self::Connection(message),
            Self::Backend { code, .. } => Self::Backend { code, message },
            Self::Unavailable(_) => Self::Unavailable(message),
            Self::Serialization(_) => Self::Serialization(message),
            Self::Io(error) => Self::Io(error),
            Self::Timeout(_) => Self::Timeout(message),
            Self::Invalid(_) => Self::Invalid(message),
            Self::Closed(_) => Self::Closed(message),
            Self::Unsupported(_) => Self::Unsupported(message),
        }
    }
}

/// crate 专用 `Result` 别名。
pub type TaosResult<T> = Result<T, TaosError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn taos_codes_keep_source_semantics() {
        assert!(TaosError::from_taos_code(TDCODE_BUSY, "繁忙").is_retryable());
        assert!(matches!(
            TaosError::from_taos_code(TDCODE_BUSY, "繁忙"),
            TaosError::Unavailable(_)
        ));

        let not_exist = TaosError::from_taos_code(TDCODE_TABLE_NOT_EXIST, "表不存在");
        assert!(not_exist.is_not_found());
        assert!(!not_exist.is_retryable());
        assert_eq!(not_exist.taos_code(), Some(TDCODE_TABLE_NOT_EXIST));
        assert!(TaosError::from_taos_code(TDCODE_TABLE_NOT_EXIST_LEGACY, "").is_not_found());

        assert!(matches!(
            TaosError::from_taos_code(42, "语法错误"),
            TaosError::Invalid(_)
        ));
        assert!(matches!(
            TaosError::from_taos_code(-1, "内部错误"),
            TaosError::Backend { .. }
        ));
        assert_eq!(
            TaosError::from_taos_code(-1, "内部错误").taos_code(),
            Some(-1)
        );
    }

    #[test]
    fn http_status_mapping_is_classified() {
        assert!(matches!(
            TaosError::from_http_status(408, ""),
            TaosError::Timeout(_)
        ));
        assert!(matches!(
            TaosError::from_http_status(429, ""),
            TaosError::Unavailable(_)
        ));
        assert!(matches!(
            TaosError::from_http_status(503, ""),
            TaosError::Unavailable(_)
        ));
        assert!(TaosError::from_http_status(503, "").is_retryable());
        assert!(!TaosError::from_http_status(401, "").is_retryable());
        assert!(matches!(
            TaosError::from_http_status(401, ""),
            TaosError::Backend { .. }
        ));
    }

    #[test]
    fn retryable_matrix_is_exhaustive() {
        let retryable = [
            TaosError::Connection("x".into()),
            TaosError::Unavailable("x".into()),
            TaosError::Timeout("x".into()),
            TaosError::Io(std::io::Error::other("x")),
        ];
        for error in retryable {
            assert!(error.is_retryable(), "{error:?} 应可重试");
        }

        let permanent = [
            TaosError::Config("x".into()),
            TaosError::backend("x"),
            TaosError::Serialization("x".into()),
            TaosError::Invalid("x".into()),
            TaosError::Closed("x".into()),
            TaosError::Unsupported("x".into()),
        ];
        for error in permanent {
            assert!(!error.is_retryable(), "{error:?} 不应可重试");
        }
    }

    #[test]
    fn with_message_preserves_variant_and_code() {
        let error = TaosError::from_taos_code(-1, "原始").with_message("补充上下文");
        assert!(error.to_string().contains("补充上下文"));
        assert_eq!(error.taos_code(), Some(-1));
        assert!(matches!(
            TaosError::Invalid("x".into()).with_message("y"),
            TaosError::Invalid(_)
        ));
    }

    #[test]
    fn io_error_converts_via_from() {
        fn read() -> TaosResult<()> {
            Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "eof").into())
        }
        assert!(matches!(read(), Err(TaosError::Io(_))));
    }
}
