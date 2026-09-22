//! TDengine 连接配置（`TaosConfig` / `TaosConfigBuilder`）。
//!
//! 配置来源与优先级：
//!
//! 1. [`TaosConfig::default`] 内置默认值；
//! 2. TOML 文本（[`TaosConfig::from_toml`]）或环境变量（[`TaosConfig::from_env`]）；
//! 3. [`TaosConfig::builder`] 链式覆盖；
//! 4. [`TaosConfig::validate`] 在建立连接前 fail-fast。
//!
//! 密码属于敏感字段：`Debug` 输出固定脱敏为 `***`，不从 TOML 反序列化，只能通过
//! 环境变量 `FOUNDATIONX_TAOSX_PASSWORD` 或
//! [`TaosConfigBuilder::password`] 注入。

use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::error::{TaosError, TaosResult};

mod builder;
mod endpoint;
mod enums;
mod parse;

pub use builder::TaosConfigBuilder;
pub use enums::{TransportMode, TsPrecision};

use self::parse::{
    de_millis, de_optional_precision, de_transport, env_bool, env_non_empty, env_parsed,
    env_trimmed, host_is_loopback, valid_host, valid_ident,
};

/// 环境变量前缀。
pub const ENV_PREFIX: &str = "FOUNDATIONX_TAOSX_";
/// 环境变量：主机名或 IP。
pub const ENV_HOST: &str = "FOUNDATIONX_TAOSX_HOST";
/// 环境变量：REST / WS 端口。
pub const ENV_PORT: &str = "FOUNDATIONX_TAOSX_PORT";
/// 环境变量：数据库名。
pub const ENV_DATABASE: &str = "FOUNDATIONX_TAOSX_DATABASE";
/// 环境变量：用户名。
pub const ENV_USER: &str = "FOUNDATIONX_TAOSX_USER";
/// 环境变量：密码（**唯一**允许注入密码的通道之一）。
pub const ENV_PASSWORD: &str = "FOUNDATIONX_TAOSX_PASSWORD";
/// 环境变量：是否启用 HTTPS / WSS。
pub const ENV_TLS: &str = "FOUNDATIONX_TAOSX_TLS";
/// 环境变量：PEM CA 文件路径（自签/私有 CA）。
pub const ENV_TLS_CA_FILE: &str = "FOUNDATIONX_TAOSX_TLS_CA_FILE";
/// 环境变量：请求超时（毫秒）。
pub const ENV_TIMEOUT_MS: &str = "FOUNDATIONX_TAOSX_TIMEOUT_MS";
/// 环境变量：时间戳精度（`ms` / `us` / `ns`）。
pub const ENV_PRECISION: &str = "FOUNDATIONX_TAOSX_PRECISION";
/// 环境变量：传输模式（`rest` / `native` / `ws`）。
pub const ENV_TRANSPORT: &str = "FOUNDATIONX_TAOSX_TRANSPORT";
/// 环境变量：全局 in-flight 上限。
pub const ENV_MAX_IN_FLIGHT: &str = "FOUNDATIONX_TAOSX_MAX_IN_FLIGHT";
/// 环境变量：获取 in-flight 许可超时（毫秒）。
pub const ENV_ACQUIRE_TIMEOUT_MS: &str = "FOUNDATIONX_TAOSX_ACQUIRE_TIMEOUT_MS";
/// 环境变量：批量写入默认每批最大行数。
pub const ENV_BATCH_MAX_ROWS: &str = "FOUNDATIONX_TAOSX_BATCH_MAX_ROWS";
/// 环境变量：单条 SQL 请求最大 UTF-8 字节数。
pub const ENV_BATCH_MAX_BYTES: &str = "FOUNDATIONX_TAOSX_BATCH_MAX_BYTES";
/// 环境变量：REST 响应体最大字节数。
pub const ENV_MAX_RESPONSE_BYTES: &str = "FOUNDATIONX_TAOSX_MAX_RESPONSE_BYTES";
/// 环境变量：单次查询最大结果行数。
pub const ENV_MAX_QUERY_ROWS: &str = "FOUNDATIONX_TAOSX_MAX_QUERY_ROWS";
/// 环境变量：关闭排空 deadline（毫秒）。
pub const ENV_CLOSE_TIMEOUT_MS: &str = "FOUNDATIONX_TAOSX_CLOSE_TIMEOUT_MS";
/// 环境变量：备用主机列表（逗号分隔）。
pub const ENV_HOSTS: &str = "FOUNDATIONX_TAOSX_HOSTS";
/// 环境变量：幂等写默认最大重试次数（含首次）。
pub const ENV_WRITE_MAX_ATTEMPTS: &str = "FOUNDATIONX_TAOSX_WRITE_MAX_ATTEMPTS";
/// 默认主机。
pub const DEFAULT_HOST: &str = "127.0.0.1";
/// 默认 REST / WS 端口。
pub const DEFAULT_PORT: u16 = 6041;
/// 默认数据库。
pub const DEFAULT_DATABASE: &str = "infra_draft";
/// 默认用户。
pub const DEFAULT_USER: &str = "root";

/// 单进程允许的最大并发请求数。
pub const HARD_MAX_IN_FLIGHT: usize = 1_024;
/// 单条 SQL 请求允许的最大 UTF-8 字节数。
pub const HARD_MAX_BATCH_BYTES: usize = 8 * 1024 * 1024;
/// 单个批次允许的最大行数。
pub const HARD_MAX_BATCH_ROWS: usize = 10_000;
/// 单个 REST 响应允许的最大字节数。
pub const HARD_MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
/// 单次查询允许返回的最大行数。
pub const HARD_MAX_QUERY_ROWS: usize = 100_000;
/// 关闭排空允许配置的最长时间。
pub const HARD_MAX_CLOSE_TIMEOUT: Duration = Duration::from_secs(30);
/// 请求超时（`timeout` / `acquire_timeout`）允许配置的最长时间。
///
/// 毫秒字段解析（`de_millis`）对 `u64::MAX` 等极端取值会饱和为
/// [`Duration::MAX`]，本上限在 [`TaosConfig::validate`] 中兜底 fail-fast。
pub const HARD_MAX_TIMEOUT: Duration = Duration::from_secs(3_600);
/// 幂等写允许配置的最大重试次数（含首次）。
pub const HARD_MAX_WRITE_MAX_ATTEMPTS: u32 = 10;
/// SQL 标识符（库名 / 子表名）允许的最大 UTF-8 字节数。
///
/// 库名校验与 `client` 模块的标识符校验共用同一上界，避免两处校验逻辑漂移。
pub(crate) const MAX_IDENT_BYTES: usize = 192;

/// TDengine 客户端配置。
///
/// 所有字段均为 `pub`，可直接用结构体字面量 + `..Default::default()` 构造；
/// 唯一敏感字段 `password` 的 `Debug` 输出被脱敏，且不从 TOML 反序列化。
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TaosConfig {
    /// 主机名或 IP。
    pub host: String,
    /// REST / WS 端口（默认 6041）。
    pub port: u16,
    /// 数据库名。
    pub database: String,
    /// 用户名。
    pub user: String,
    /// 密码。
    ///
    /// **敏感字段**：`Debug` 输出固定脱敏为 `***`，且不从 TOML 反序列化
    /// （只能通过环境变量或 [`TaosConfigBuilder::password`] 注入）。
    #[serde(skip)]
    pub password: String,
    /// 是否启用 HTTPS / WSS。
    pub tls: bool,
    /// 可选 PEM CA 文件（自签/私有 CA）。
    pub tls_ca_file: Option<PathBuf>,
    /// 请求超时（TOML 字段名 `timeout_ms`）。
    #[serde(rename = "timeout_ms", deserialize_with = "de_millis")]
    pub timeout: Duration,
    /// 可选显式精度；`None` 时在 `connect` 后从数据库探测。
    #[serde(deserialize_with = "de_optional_precision")]
    pub precision: Option<TsPrecision>,
    /// 传输模式。
    #[serde(deserialize_with = "de_transport")]
    pub transport: TransportMode,
    /// 全局 in-flight 上限（≥1）。
    pub max_in_flight: usize,
    /// 获取 in-flight 许可超时（TOML 字段名 `acquire_timeout_ms`）。
    #[serde(rename = "acquire_timeout_ms", deserialize_with = "de_millis")]
    pub acquire_timeout: Duration,
    /// 批量写入默认每批最大行数。
    pub batch_max_rows: usize,
    /// 单条 SQL 请求最大 UTF-8 字节数。
    pub batch_max_bytes: usize,
    /// REST 响应体最大字节数。
    pub max_response_bytes: usize,
    /// 单次查询最大结果行数。
    pub max_query_rows: usize,
    /// 关闭时等待在途请求排空的 deadline（TOML 字段名 `close_timeout_ms`）。
    #[serde(rename = "close_timeout_ms", deserialize_with = "de_millis")]
    pub close_timeout: Duration,
    /// 备用主机列表（主 `host` 失败时按序尝试）。
    pub hosts: Vec<String>,
    /// 幂等写默认最大重试次数（含首次；1 = 不重试）。
    pub write_max_attempts: u32,
}

impl Default for TaosConfig {
    fn default() -> Self {
        Self {
            host: DEFAULT_HOST.into(),
            port: DEFAULT_PORT,
            database: DEFAULT_DATABASE.into(),
            user: DEFAULT_USER.into(),
            password: String::new(),
            tls: false,
            tls_ca_file: None,
            timeout: Duration::from_secs(10),
            precision: None,
            transport: TransportMode::Rest,
            max_in_flight: 64,
            acquire_timeout: Duration::from_secs(5),
            batch_max_rows: 500,
            batch_max_bytes: 1024 * 1024,
            max_response_bytes: 8 * 1024 * 1024,
            max_query_rows: 10_000,
            close_timeout: Duration::from_secs(5),
            hosts: Vec::new(),
            write_max_attempts: 1,
        }
    }
}

impl fmt::Debug for TaosConfig {
    /// 手写 `Debug`：密码固定渲染为 `***`，其余字段原样输出。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaosConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &"***")
            .field("tls", &self.tls)
            .field("tls_ca_file", &self.tls_ca_file)
            .field("timeout", &self.timeout)
            .field("precision", &self.precision)
            .field("transport", &self.transport)
            .field("max_in_flight", &self.max_in_flight)
            .field("acquire_timeout", &self.acquire_timeout)
            .field("batch_max_rows", &self.batch_max_rows)
            .field("batch_max_bytes", &self.batch_max_bytes)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("max_query_rows", &self.max_query_rows)
            .field("close_timeout", &self.close_timeout)
            .field("hosts", &self.hosts)
            .field("write_max_attempts", &self.write_max_attempts)
            .finish()
    }
}

impl TaosConfig {
    /// 从环境变量加载（前缀 `FOUNDATIONX_TAOSX_`），未设置项使用默认值。
    ///
    /// 加载后立即 [`validate`](Self::validate)；任一变量取值非法都会 fail-closed，
    /// 且错误只报告变量名、不回显取值。
    pub fn from_env() -> TaosResult<Self> {
        let mut config = Self::default();
        config.apply_env_overrides()?;
        config.validate()?;
        Ok(config)
    }

    /// 从 TOML 文本解析并校验（**不**读取环境变量，便于确定性测试）。
    ///
    /// 期望 `schema_version = 1` + 扁平字段；`password` 只允许空占位，非空值一律
    /// 拒绝，避免密钥进入版本库。
    pub fn from_toml(text: &str) -> TaosResult<Self> {
        let mut root: toml::Table = toml::from_str(text)
            .map_err(|error| TaosError::Config(format!("TOML 解析失败: {}", error.message())))?;

        let version = root
            .remove("schema_version")
            .ok_or_else(|| TaosError::Config("TOML 缺少 schema_version 字段".to_owned()))?;
        let version = version
            .as_integer()
            .ok_or_else(|| TaosError::Config("TOML schema_version 必须为整数".to_owned()))?;
        if version != 1 {
            return Err(TaosError::Config(format!(
                "TOML schema_version 不支持: {version}"
            )));
        }

        if let Some(password) = root.remove("password") {
            let password = password.as_str().unwrap_or("非字符串");
            if !password.is_empty() {
                return Err(TaosError::Config(
                    "TOML 禁止非空 password 字段，请改用环境变量注入".to_owned(),
                ));
            }
        }

        let config: Self = toml::Value::Table(root).try_into().map_err(|error| {
            TaosError::Config(format!("TOML 反序列化失败: {}", error.message()))
        })?;
        config.validate()?;
        Ok(config)
    }

    /// 从 TOML 文件读取；语义同 [`TaosConfig::from_toml`]。
    pub fn from_toml_file(path: impl AsRef<Path>) -> TaosResult<Self> {
        let text = std::fs::read_to_string(path.as_ref()).map_err(|error| {
            TaosError::Config(format!(
                "TOML 文件读取失败 `{}`: {}",
                path.as_ref().display(),
                error.kind()
            ))
        })?;
        Self::from_toml(&text)
    }

    /// 校验配置合法性（建立连接前 fail-fast）。
    ///
    /// 覆盖硬上限（[`HARD_MAX_IN_FLIGHT`] 等）、超时、主机/端口、备用主机、
    /// 库名标识符、TLS 与远程认证要求。
    pub fn validate(&self) -> TaosResult<()> {
        if self.max_in_flight < 1 || self.max_in_flight > HARD_MAX_IN_FLIGHT {
            return Err(TaosError::Config(format!(
                "max_in_flight 必须为 1..={HARD_MAX_IN_FLIGHT}"
            )));
        }
        if self.batch_max_rows < 1 || self.batch_max_rows > HARD_MAX_BATCH_ROWS {
            return Err(TaosError::Config(format!(
                "batch_max_rows 必须为 1..={HARD_MAX_BATCH_ROWS}"
            )));
        }
        if self.batch_max_bytes < 1 || self.batch_max_bytes > HARD_MAX_BATCH_BYTES {
            return Err(TaosError::Config(format!(
                "batch_max_bytes 必须为 1..={HARD_MAX_BATCH_BYTES}"
            )));
        }
        if self.max_response_bytes < 1 || self.max_response_bytes > HARD_MAX_RESPONSE_BYTES {
            return Err(TaosError::Config(format!(
                "max_response_bytes 必须为 1..={HARD_MAX_RESPONSE_BYTES}"
            )));
        }
        if self.max_query_rows < 1 || self.max_query_rows > HARD_MAX_QUERY_ROWS {
            return Err(TaosError::Config(format!(
                "max_query_rows 必须为 1..={HARD_MAX_QUERY_ROWS}"
            )));
        }
        if self.timeout.is_zero() {
            return Err(TaosError::Config(format!(
                "timeout 必须大于 0（当前 {:.0?}）",
                self.timeout
            )));
        }
        if self.acquire_timeout.is_zero() {
            return Err(TaosError::Config(format!(
                "acquire_timeout 必须大于 0（当前 {:.0?}）",
                self.acquire_timeout
            )));
        }
        if self.close_timeout.is_zero() {
            return Err(TaosError::Config(format!(
                "close_timeout 必须大于 0（当前 {:.0?}）",
                self.close_timeout
            )));
        }
        if self.close_timeout > HARD_MAX_CLOSE_TIMEOUT {
            return Err(TaosError::Config(format!(
                "close_timeout 超过上限 {:.0?}（当前 {:.0?}）",
                HARD_MAX_CLOSE_TIMEOUT, self.close_timeout
            )));
        }
        if self.timeout > HARD_MAX_TIMEOUT {
            return Err(TaosError::Config(format!(
                "timeout 超过上限 {:.0?}（当前 {:.0?}）",
                HARD_MAX_TIMEOUT, self.timeout
            )));
        }
        if self.acquire_timeout > HARD_MAX_TIMEOUT {
            return Err(TaosError::Config(format!(
                "acquire_timeout 超过上限 {:.0?}（当前 {:.0?}）",
                HARD_MAX_TIMEOUT, self.acquire_timeout
            )));
        }
        if !valid_host(&self.host) || self.port == 0 {
            return Err(TaosError::Config("host/port 非法".to_owned()));
        }
        for host in &self.hosts {
            if !valid_host(host) {
                return Err(TaosError::Config(format!("备用 host 非法: {host}")));
            }
        }
        if self.write_max_attempts == 0 {
            return Err(TaosError::Config("write_max_attempts 必须 ≥ 1".to_owned()));
        }
        if !self.database.is_empty() && !valid_ident(&self.database) {
            return Err(TaosError::Config("database 标识符非法".to_owned()));
        }
        if self.user.trim().is_empty() {
            return Err(TaosError::Config("user 不能为空".to_owned()));
        }
        if self.tls_ca_file.is_some() && !self.tls {
            return Err(TaosError::Config(
                "配置 tls_ca_file 时必须启用 tls".to_owned(),
            ));
        }
        if !host_is_loopback(&self.host) {
            if !self.tls {
                return Err(TaosError::Config("远程 TDengine 必须使用 TLS".to_owned()));
            }
            if self.password.trim().is_empty() {
                return Err(TaosError::Config(
                    "远程 TDengine 必须配置认证密码".to_owned(),
                ));
            }
        }
        // 结构化解析最终端点：拒绝无法构成合法 URL 的 host/port 组合。
        self.rest_sql_endpoint()?;
        self.native_ws_endpoint()?;
        Ok(())
    }

    /// 链式构建器入口。
    #[must_use]
    pub fn builder() -> TaosConfigBuilder {
        TaosConfigBuilder::new()
    }
}

impl TaosConfig {
    /// 从环境变量覆盖当前配置（env 值优先于结构体已有值）。
    fn apply_env_overrides(&mut self) -> TaosResult<()> {
        if let Some(value) = env_non_empty(ENV_HOST) {
            self.host = value;
        }
        if let Some(value) = env_parsed::<u16>(ENV_PORT)? {
            self.port = value;
        }
        if let Some(value) = env_non_empty(ENV_DATABASE) {
            self.database = value;
        }
        if let Some(value) = env_non_empty(ENV_USER) {
            self.user = value;
        }
        if let Ok(value) = std::env::var(ENV_PASSWORD) {
            self.password = value;
        }
        if let Some(value) = env_bool(ENV_TLS)? {
            self.tls = value;
        }
        if let Some(value) = env_trimmed(ENV_TLS_CA_FILE) {
            self.tls_ca_file = Some(PathBuf::from(value));
        }
        if let Some(value) = env_parsed::<u64>(ENV_TIMEOUT_MS)? {
            self.timeout = Duration::from_millis(value);
        }
        if let Some(value) = env_trimmed(ENV_PRECISION) {
            self.precision =
                Some(TsPrecision::parse(&value).ok_or_else(|| {
                    TaosError::Config(format!("环境变量 {ENV_PRECISION} 取值非法"))
                })?);
        }
        if let Some(value) = env_trimmed(ENV_TRANSPORT) {
            self.transport = TransportMode::parse(&value)
                .ok_or_else(|| TaosError::Config(format!("环境变量 {ENV_TRANSPORT} 取值非法")))?;
        }
        if let Some(value) = env_parsed::<usize>(ENV_MAX_IN_FLIGHT)? {
            self.max_in_flight = value;
        }
        if let Some(value) = env_parsed::<u64>(ENV_ACQUIRE_TIMEOUT_MS)? {
            self.acquire_timeout = Duration::from_millis(value);
        }
        if let Some(value) = env_parsed::<usize>(ENV_BATCH_MAX_ROWS)? {
            self.batch_max_rows = value;
        }
        if let Some(value) = env_parsed::<usize>(ENV_BATCH_MAX_BYTES)? {
            self.batch_max_bytes = value;
        }
        if let Some(value) = env_parsed::<usize>(ENV_MAX_RESPONSE_BYTES)? {
            self.max_response_bytes = value;
        }
        if let Some(value) = env_parsed::<usize>(ENV_MAX_QUERY_ROWS)? {
            self.max_query_rows = value;
        }
        if let Some(value) = env_parsed::<u64>(ENV_CLOSE_TIMEOUT_MS)? {
            self.close_timeout = Duration::from_millis(value);
        }
        if let Some(value) = env_trimmed(ENV_HOSTS) {
            self.hosts = value
                .split(',')
                .map(str::trim)
                .filter(|entry| !entry.is_empty())
                .map(str::to_owned)
                .collect();
        }
        if let Some(value) = env_parsed::<u32>(ENV_WRITE_MAX_ATTEMPTS)? {
            self.write_max_attempts = value.max(1);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_values_are_loopback_http() {
        let config = TaosConfig::default();
        assert_eq!(config.host, DEFAULT_HOST);
        assert_eq!(config.port, DEFAULT_PORT);
        assert_eq!(config.user, DEFAULT_USER);
        assert_eq!(config.database, DEFAULT_DATABASE);
        assert!(config.password.is_empty());
        assert_eq!(config.rest_sql_url(), "http://127.0.0.1:6041/rest/sql");
        assert_eq!(config.native_ws_url(), "ws://127.0.0.1:6041/rest/ws");
        config.validate().expect("默认配置必须有效");
    }

    #[test]
    fn debug_redacts_password() {
        let config = TaosConfig {
            password: "fake-pass-value-42".into(),
            ..Default::default()
        };
        let rendered = format!("{config:?}");
        assert!(rendered.contains("***"));
        assert!(!rendered.contains("fake-pass-value-42"));
    }

    #[test]
    fn toml_parses_flat_fields() {
        let config = TaosConfig::from_toml(
            r#"
schema_version = 1
host = "127.0.0.1"
port = 6041
database = "macro_data"
user = "writer"
tls = false
timeout_ms = 15000
precision = "ns"
transport = "native"
max_in_flight = 32
hosts = ["127.0.0.2"]
write_max_attempts = 3
"#,
        )
        .expect("TOML 解析必须成功");
        assert_eq!(config.database, "macro_data");
        assert_eq!(config.user, "writer");
        assert_eq!(config.timeout, Duration::from_millis(15000));
        assert_eq!(config.precision, Some(TsPrecision::Ns));
        assert_eq!(config.transport, TransportMode::NativeWs);
        assert_eq!(config.max_in_flight, 32);
        assert_eq!(config.hosts, vec!["127.0.0.2".to_owned()]);
        assert_eq!(config.write_max_attempts, 3);
        assert!(config.password.is_empty());
    }

    #[test]
    fn toml_rejects_schema_and_unknown_fields() {
        assert!(TaosConfig::from_toml("host = \"127.0.0.1\"\n").is_err());
        assert!(TaosConfig::from_toml("schema_version = 99\n").is_err());
        assert!(TaosConfig::from_toml("schema_version = 1\nsink_id = \"m\"\n").is_err());
    }

    #[test]
    fn toml_rejects_non_empty_password_without_echoing_it() {
        let error = TaosConfig::from_toml("schema_version = 1\npassword = \"hunter2\"\n")
            .expect_err("非空 password 必须拒绝");
        assert!(error.to_string().contains("password"));
        assert!(!error.to_string().contains("hunter2"));
    }

    #[test]
    fn toml_rejects_invalid_precision_and_transport() {
        assert!(TaosConfig::from_toml("schema_version = 1\nprecision = \"bogus\"\n").is_err());
        assert!(TaosConfig::from_toml("schema_version = 1\ntransport = \"grpc\"\n").is_err());
    }

    #[test]
    fn hard_limits_fail_closed() {
        let cases = [
            TaosConfig {
                max_in_flight: HARD_MAX_IN_FLIGHT + 1,
                ..Default::default()
            },
            TaosConfig {
                max_in_flight: 0,
                ..Default::default()
            },
            TaosConfig {
                batch_max_rows: HARD_MAX_BATCH_ROWS + 1,
                ..Default::default()
            },
            TaosConfig {
                batch_max_bytes: HARD_MAX_BATCH_BYTES + 1,
                ..Default::default()
            },
            TaosConfig {
                max_response_bytes: HARD_MAX_RESPONSE_BYTES + 1,
                ..Default::default()
            },
            TaosConfig {
                max_query_rows: HARD_MAX_QUERY_ROWS + 1,
                ..Default::default()
            },
            TaosConfig {
                close_timeout: HARD_MAX_CLOSE_TIMEOUT + Duration::from_millis(1),
                ..Default::default()
            },
        ];
        for config in cases {
            assert!(config.validate().is_err(), "{config:?} 必须被拒绝");
        }
    }

    #[test]
    fn remote_plaintext_and_auth_fail_closed() {
        let plaintext = TaosConfig {
            host: "td.example".into(),
            ..Default::default()
        };
        assert!(plaintext.validate().is_err());

        let no_password = TaosConfig {
            host: "td.example".into(),
            tls: true,
            ..Default::default()
        };
        assert!(no_password.validate().is_err());

        let secure = TaosConfig {
            host: "td.example".into(),
            tls: true,
            password: "configured".into(),
            ..Default::default()
        };
        secure.validate().expect("远程 TLS + 认证必须通过");
    }

    #[test]
    fn host_classification_and_ipv6_are_strict() {
        for bad in [
            "localhost.evil",
            "127.0.0.1.evil",
            "user@localhost",
            "http://localhost",
        ] {
            let config = TaosConfig {
                host: bad.into(),
                ..Default::default()
            };
            assert!(config.validate().is_err(), "坏主机 {bad} 必须被拒绝");
        }
        let ipv6 = TaosConfig {
            host: "::1".into(),
            ..Default::default()
        };
        ipv6.validate().expect("IPv6 环回必须通过");
        assert_eq!(ipv6.rest_sql_url(), "http://[::1]:6041/rest/sql");
    }

    #[test]
    fn endpoint_hosts_dedupes_and_keeps_order() {
        let config = TaosConfig {
            host: "a".into(),
            hosts: vec!["a".into(), "b".into(), "c".into()],
            ..Default::default()
        };
        assert_eq!(config.endpoint_hosts(), vec!["a", "b", "c"]);
        assert_eq!(
            config.rest_sql_url_for("db.example"),
            "http://db.example:6041/rest/sql"
        );
        assert!(config.rest_sql_db_url().ends_with("/infra_draft"));
    }

    #[test]
    fn builder_overrides_and_builds() {
        let config = TaosConfig::builder()
            .host("127.0.0.1")
            .port(6041)
            .database("ticks")
            .user("writer")
            .password("p")
            .max_in_flight(4)
            .batch_max_rows(10)
            .precision(TsPrecision::Ns)
            .transport(TransportMode::NativeWs)
            .hosts(["127.0.0.2"])
            .timeout(Duration::from_millis(500))
            .acquire_timeout(Duration::from_millis(100))
            .close_timeout(Duration::from_millis(100))
            .write_max_attempts(0)
            .build()
            .expect("构建必须成功");
        assert_eq!(config.max_in_flight, 4);
        assert_eq!(config.precision, Some(TsPrecision::Ns));
        assert_eq!(config.transport, TransportMode::NativeWs);
        assert_eq!(config.write_max_attempts, 1, "0 应被夹到 1");
        assert_eq!(config.hosts, vec!["127.0.0.2".to_owned()]);

        let rebuilt = TaosConfigBuilder::from_config(config)
            .build()
            .expect("重新构建");
        assert_eq!(rebuilt.database, "ticks");
    }

    #[test]
    fn builder_rejects_invalid_config() {
        let error = TaosConfig::builder()
            .host("")
            .build()
            .expect_err("空 host 必须拒绝");
        assert!(!error.is_retryable(), "配置错误不可重试");
    }

    #[test]
    fn precision_roundtrip_and_parse() {
        assert_eq!(TsPrecision::Ms.from_nanos(1_500_000_000), 1500);
        assert_eq!(TsPrecision::Ms.to_nanos(1500), 1_500_000_000);
        assert_eq!(TsPrecision::Ns.from_nanos(42), 42);
        assert_eq!(TsPrecision::parse("US"), Some(TsPrecision::Us));
        assert_eq!(TsPrecision::parse(" bogus "), None);
        assert_eq!(TsPrecision::Ms.as_str(), "ms");
        assert_eq!(TransportMode::parse("rest"), Some(TransportMode::Rest));
        assert_eq!(
            TransportMode::parse("native-ws"),
            Some(TransportMode::NativeWs)
        );
        assert!(TransportMode::parse("bogus").is_none());
    }

    #[test]
    fn env_parsed_reports_variable_name_without_echoing_value() {
        std::env::set_var(ENV_TIMEOUT_MS, "secret-not-a-number");
        let error = env_parsed::<u64>(ENV_TIMEOUT_MS).expect_err("非法数值必须拒绝");
        std::env::remove_var(ENV_TIMEOUT_MS);
        assert!(error.to_string().contains(ENV_TIMEOUT_MS));
        assert!(!error.to_string().contains("secret-not-a-number"));
    }

    #[test]
    fn from_toml_does_not_read_env_overrides() {
        std::env::set_var(ENV_DATABASE, "from_env_database");
        let config =
            TaosConfig::from_toml("schema_version = 1\ndatabase = \"from_toml_database\"\n")
                .expect("TOML 解析");
        std::env::remove_var(ENV_DATABASE);
        assert_eq!(
            config.database, "from_toml_database",
            "from_toml 必须与环境变量隔离"
        );
    }

    #[test]
    fn from_toml_file_missing_path_fails_closed() {
        let missing =
            std::env::temp_dir().join(format!("taosx-missing-{}.toml", std::process::id()));
        let error = TaosConfig::from_toml_file(&missing).expect_err("缺失文件必须拒绝");
        assert!(error.to_string().contains("TOML 文件读取失败"));
    }

    /// P1-1: 每条 timeout 校验错误必须包含字段名、实际值与允许范围（修复前合并为模糊消息）。
    #[test]
    fn timeout_validation_errors_include_field_name_and_range() {
        let zero_timeout = TaosConfig {
            timeout: Duration::ZERO,
            ..Default::default()
        };
        let error = zero_timeout.validate().expect_err("零 timeout 必须拒绝");
        let msg = error.to_string();
        assert!(
            msg.contains("timeout"),
            "错误消息必须包含字段名 'timeout': {msg}"
        );

        let zero_acquire = TaosConfig {
            acquire_timeout: Duration::ZERO,
            ..Default::default()
        };
        let error = zero_acquire
            .validate()
            .expect_err("零 acquire_timeout 必须拒绝");
        let msg = error.to_string();
        assert!(
            msg.contains("acquire_timeout"),
            "错误消息必须包含字段名 'acquire_timeout': {msg}"
        );

        let zero_close = TaosConfig {
            close_timeout: Duration::ZERO,
            ..Default::default()
        };
        let error = zero_close
            .validate()
            .expect_err("零 close_timeout 必须拒绝");
        let msg = error.to_string();
        assert!(
            msg.contains("close_timeout"),
            "错误消息必须包含字段名 'close_timeout': {msg}"
        );

        let over_close = TaosConfig {
            close_timeout: HARD_MAX_CLOSE_TIMEOUT + Duration::from_secs(1),
            ..Default::default()
        };
        let error = over_close
            .validate()
            .expect_err("超限 close_timeout 必须拒绝");
        let msg = error.to_string();
        assert!(
            msg.contains("close_timeout"),
            "错误消息必须包含字段名 'close_timeout': {msg}"
        );
        // 错误消息必须引用 HARD_MAX_CLOSE_TIMEOUT 常量值而非硬编码 "30 秒"。
        assert!(
            msg.contains(&HARD_MAX_CLOSE_TIMEOUT.as_secs().to_string()),
            "错误消息必须引用 HARD_MAX_CLOSE_TIMEOUT 实际值而非硬编码: {msg}"
        );
    }

    /// P2-1: timeout / acquire_timeout 超过 HARD_MAX_TIMEOUT 必须 fail-fast，
    /// 错误消息模式对齐 close_timeout 分支（含字段名、上限与实际值）。
    #[test]
    fn timeout_over_hard_max_reports_upper_bound() {
        let over_timeout = TaosConfig {
            timeout: HARD_MAX_TIMEOUT + Duration::from_millis(1),
            ..Default::default()
        };
        let error = over_timeout.validate().expect_err("超限 timeout 必须拒绝");
        let msg = error.to_string();
        assert!(
            msg.contains("timeout 超过上限"),
            "错误消息必须包含「timeout 超过上限」: {msg}"
        );
        assert!(
            msg.contains(&format!("{:.0?}", HARD_MAX_TIMEOUT)),
            "错误消息必须引用 HARD_MAX_TIMEOUT 实际值: {msg}"
        );

        let over_acquire = TaosConfig {
            acquire_timeout: HARD_MAX_TIMEOUT + Duration::from_millis(1),
            ..Default::default()
        };
        let error = over_acquire
            .validate()
            .expect_err("超限 acquire_timeout 必须拒绝");
        let msg = error.to_string();
        assert!(
            msg.contains("acquire_timeout 超过上限"),
            "错误消息必须包含「acquire_timeout 超过上限」: {msg}"
        );
        assert!(
            msg.contains(&format!("{:.0?}", HARD_MAX_TIMEOUT)),
            "错误消息必须引用 HARD_MAX_TIMEOUT 实际值: {msg}"
        );

        // 恰好等于上限必须放行（边界不误伤）。
        let at_limit = TaosConfig {
            timeout: HARD_MAX_TIMEOUT,
            acquire_timeout: HARD_MAX_TIMEOUT,
            ..Default::default()
        };
        at_limit.validate().expect("等于上限必须通过");
    }
}
