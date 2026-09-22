//! `TaosConfig` 的端点 URL 构造。
//!
//! 自 `src/config.rs` 下沉而来：REST SQL 与原生 WebSocket 的 URL / 结构化 endpoint，
//! 以及连接尝试主机序列。`TaosConfig` 的定义与其余门面方法仍在 `src/config.rs`；
//! 本模块是 `config` 的子模块，故可直接使用 `parse` 的 `pub(super)` 辅助。

use crate::error::{TaosError, TaosResult};

use super::parse::url_host;
use super::TaosConfig;

impl TaosConfig {
    /// REST SQL 端点：`http(s)://host:port/rest/sql`。
    #[must_use]
    pub fn rest_sql_url(&self) -> String {
        self.rest_sql_url_for(&self.host)
    }

    /// 指定主机的 REST SQL 端点。
    #[must_use]
    pub fn rest_sql_url_for(&self, host: &str) -> String {
        let scheme = if self.tls { "https" } else { "http" };
        format!("{scheme}://{}:{}/rest/sql", url_host(host), self.port)
    }

    /// 带 database 路径的 REST SQL 端点。
    #[must_use]
    pub fn rest_sql_db_url(&self) -> String {
        let base = self.rest_sql_url();
        if self.database.is_empty() {
            base
        } else {
            format!("{base}/{}", self.database)
        }
    }

    /// 原生 WebSocket SQL 端点：`ws(s)://host:port/rest/ws`。
    #[must_use]
    pub fn native_ws_url(&self) -> String {
        let scheme = if self.tls { "wss" } else { "ws" };
        format!("{scheme}://{}:{}/rest/ws", url_host(&self.host), self.port)
    }

    /// 结构化解析 REST SQL 端点（校验 scheme/host/port 合法）。
    pub fn rest_sql_endpoint(&self) -> TaosResult<url::Url> {
        url::Url::parse(&self.rest_sql_url())
            .map_err(|error| TaosError::Config(format!("REST 端点非法（{error}）")))
    }

    /// 结构化解析原生 WebSocket 端点。
    pub fn native_ws_endpoint(&self) -> TaosResult<url::Url> {
        url::Url::parse(&self.native_ws_url())
            .map_err(|error| TaosError::Config(format!("Native WS 端点非法（{error}）")))
    }

    /// 连接尝试主机序列：主 `host` + `hosts` 备用（去重、保序）。
    #[must_use]
    pub fn endpoint_hosts(&self) -> Vec<String> {
        let mut hosts = Vec::with_capacity(1 + self.hosts.len());
        hosts.push(self.host.clone());
        for host in &self.hosts {
            if !hosts.iter().any(|known| known == host) {
                hosts.push(host.clone());
            }
        }
        hosts
    }
}
