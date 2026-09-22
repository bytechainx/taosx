//! `TaosConfigBuilder`：`TaosConfig` 的链式构建器。
//!
//! 自 `src/config.rs` 下沉而来。公开类型经门面 `pub use` 导出，路径不变；
//! 构建器只持有并逐步覆盖 `TaosConfig` 的各 `pub` 字段，非法组合在 `build()`
//! 里由 `TaosConfig::validate` 统一 fail-closed。

use std::path::PathBuf;
use std::time::Duration;

use crate::error::TaosResult;

use super::{TaosConfig, TransportMode, TsPrecision};

/// [`TaosConfig`] 的链式构建器。
///
/// 密码等敏感字段只能通过构建器或环境变量注入。
#[derive(Clone, Debug)]
pub struct TaosConfigBuilder {
    inner: TaosConfig,
}

impl Default for TaosConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TaosConfigBuilder {
    /// 从默认值开始。
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: TaosConfig::default(),
        }
    }

    /// 从已有配置开始（便于覆盖少量字段）。
    #[must_use]
    pub fn from_config(config: TaosConfig) -> Self {
        Self { inner: config }
    }

    /// 设置主机名或 IP。
    #[must_use]
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.inner.host = host.into();
        self
    }

    /// 设置 REST / WS 端口。
    #[must_use]
    pub fn port(mut self, port: u16) -> Self {
        self.inner.port = port;
        self
    }

    /// 设置数据库名。
    #[must_use]
    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.inner.database = database.into();
        self
    }

    /// 设置用户名。
    #[must_use]
    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.inner.user = user.into();
        self
    }

    /// 设置密码；密码不会出现在 `Debug` 输出中。
    #[must_use]
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.inner.password = password.into();
        self
    }

    /// 设置是否启用 HTTPS / WSS。
    #[must_use]
    pub fn tls(mut self, enabled: bool) -> Self {
        self.inner.tls = enabled;
        self
    }

    /// 设置 PEM CA 文件路径。
    #[must_use]
    pub fn tls_ca_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.inner.tls_ca_file = Some(path.into());
        self
    }

    /// 设置请求超时。
    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.inner.timeout = timeout;
        self
    }

    /// 设置显式时间戳精度。
    #[must_use]
    pub fn precision(mut self, precision: TsPrecision) -> Self {
        self.inner.precision = Some(precision);
        self
    }

    /// 设置传输模式。
    #[must_use]
    pub fn transport(mut self, transport: TransportMode) -> Self {
        self.inner.transport = transport;
        self
    }

    /// 设置全局 in-flight 上限。
    #[must_use]
    pub fn max_in_flight(mut self, max_in_flight: usize) -> Self {
        self.inner.max_in_flight = max_in_flight;
        self
    }

    /// 设置获取 in-flight 许可的超时。
    #[must_use]
    pub fn acquire_timeout(mut self, timeout: Duration) -> Self {
        self.inner.acquire_timeout = timeout;
        self
    }

    /// 设置批量写入默认每批最大行数。
    #[must_use]
    pub fn batch_max_rows(mut self, batch_max_rows: usize) -> Self {
        self.inner.batch_max_rows = batch_max_rows;
        self
    }

    /// 设置单条 SQL 请求最大字节数。
    #[must_use]
    pub fn batch_max_bytes(mut self, batch_max_bytes: usize) -> Self {
        self.inner.batch_max_bytes = batch_max_bytes;
        self
    }

    /// 设置 REST 响应体最大字节数。
    #[must_use]
    pub fn max_response_bytes(mut self, max_response_bytes: usize) -> Self {
        self.inner.max_response_bytes = max_response_bytes;
        self
    }

    /// 设置单次查询最大结果行数。
    #[must_use]
    pub fn max_query_rows(mut self, max_query_rows: usize) -> Self {
        self.inner.max_query_rows = max_query_rows;
        self
    }

    /// 设置关闭排空 deadline。
    #[must_use]
    pub fn close_timeout(mut self, timeout: Duration) -> Self {
        self.inner.close_timeout = timeout;
        self
    }

    /// 设置备用主机列表。
    #[must_use]
    pub fn hosts<I, S>(mut self, hosts: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.inner.hosts = hosts.into_iter().map(Into::into).collect();
        self
    }

    /// 设置幂等写默认最大重试次数。
    #[must_use]
    pub fn write_max_attempts(mut self, attempts: u32) -> Self {
        self.inner.write_max_attempts = attempts.max(1);
        self
    }

    /// 校验并产出配置。
    pub fn build(self) -> TaosResult<TaosConfig> {
        self.inner.validate()?;
        Ok(self.inner)
    }
}
