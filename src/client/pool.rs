//! [`TaosPool`] 的请求路径实现：构造、连接、并发额度、健康检查与 SQL 收发。
//!
//! 从门面 `client.rs` 下沉（`MR-STRUCT-007` 腾余量）。`TaosPool` / `PoolInner` /
//! `RequestGuard` 的**定义与字段**留在门面（子模块可访问父模块私有项，故无需提级字段）；
//! 两处提为 `pub(super)` 的都是「父/兄弟/门面测试」要用的：
//! - `acquire` —— 被门面内联测试直接驱动（并发额度与超时用例）；
//! - `verify_decimal_schema` —— 被**兄弟模块** `client/write.rs` 的批量写入路径调用。
//!
//! 其余私有辅助（`connect_one` / `detect_precision` / `exec_sql_raw*` / `ensure_open`）
//! 只在本 impl 内互调，**保持私有**。

use super::response::truncate;
use super::sql::escape_str;
use super::*;

impl TaosPool {
    /// 同步构造（**仅校验配置**，不建立网络连接、不探测服务端）。
    ///
    /// 适用于 fail-closed 校验与离线背压路径；生产入口请使用 [`TaosPool::connect`]。
    pub fn new(config: TaosConfig) -> TaosResult<Self> {
        config.validate()?;
        let http = build_http_client(&config)?;
        let initial_precision = config.precision.unwrap_or(TsPrecision::Ms);
        let max_in_flight = config.max_in_flight;
        Ok(Self {
            inner: Arc::new(PoolInner {
                http,
                config,
                precision: RwLock::new(initial_precision),
                sem: Arc::new(Semaphore::new(max_in_flight)),
                state: AtomicUsize::new(0),
                drained: Notify::new(),
                metrics: OpCounters::new(),
            }),
        })
    }

    /// 连接：构建 HTTP 客户端、可选建库、探测精度、`ping`。
    ///
    /// - `TransportMode::NativeWs` 时先做一次原生 WS 握手探测（失败即返回）。
    /// - 主 `host` 不可用时按 `config.hosts` 顺序故障转移。
    /// - 配置精度与数据库实际精度不一致时 fail-closed。
    pub async fn connect(config: TaosConfig) -> TaosResult<Self> {
        config.validate()?;
        let mut last_error = TaosError::Unavailable("无可用 TDengine endpoint".to_owned());
        for host in config.endpoint_hosts() {
            let mut attempt_config = config.clone();
            attempt_config.host = host;
            match Self::connect_one(attempt_config).await {
                Ok(pool) => return Ok(pool),
                Err(error) => last_error = error,
            }
        }
        Err(last_error)
    }

    /// 从环境变量连接（前缀 `FOUNDATIONX_TAOSX_`）。
    pub async fn connect_from_env() -> TaosResult<Self> {
        Self::connect(TaosConfig::from_env()?).await
    }

    /// 单主机连接流程。
    async fn connect_one(config: TaosConfig) -> TaosResult<Self> {
        config.validate()?;

        if config.transport == TransportMode::NativeWs {
            native::connect_native_ws(&config).await?;
        }
        let pool = Self::new(config)?;

        // REST 路径：确保 database 存在 + 精度探测 + ping。
        // NativeWs 仅完成握手探测；SQL 默认仍走 REST（`exec_sql_ws` 提供 WS 会话）。
        if !pool.inner.config.database.is_empty() {
            let database = pool.inner.config.database.clone();
            validate_ident(&database)?;
            pool.exec_sql_raw(
                &format!("CREATE DATABASE IF NOT EXISTS `{database}` KEEP 3650"),
                false,
            )
            .await?;
            let detected = pool.detect_precision().await?;
            if let Some(configured) = pool.inner.config.precision {
                if configured != detected {
                    return Err(TaosError::Config(format!(
                        "配置精度 {configured:?} 与数据库精度 {detected:?} 不一致"
                    )));
                }
            }
            *pool
                .inner
                .precision
                .write()
                .map_err(|_| TaosError::Config("精度状态锁已中毒".to_owned()))? = detected;
        }

        pool.ping().await?;
        Ok(pool)
    }

    /// 工作客户端（`Clone` 便宜，内部共享 `Arc`）。
    #[must_use]
    pub fn client(&self) -> TaosClient {
        self.clone()
    }

    /// 配置。
    #[must_use]
    pub fn config(&self) -> &TaosConfig {
        &self.inner.config
    }

    /// 当前生效精度。
    #[must_use]
    pub fn precision(&self) -> TsPrecision {
        self.inner
            .precision
            .read()
            .map_or(TsPrecision::Ms, |guard| *guard)
    }

    /// 池瞬时统计。
    #[must_use]
    pub fn stats(&self) -> TaosPoolStats {
        let state = self.inner.state.load(Ordering::Acquire);
        TaosPoolStats {
            in_flight: state & IN_FLIGHT_MASK,
            closed: state & CLOSED_BIT != 0,
        }
    }

    /// 进程内有界操作计数快照（含进程级 WS 探测累计）。
    #[must_use]
    pub fn metrics(&self) -> TaosMetricsSnapshot {
        self.inner.metrics.snapshot()
    }

    /// 指标以 Prometheus 文本导出。
    #[must_use]
    pub fn metrics_prometheus(&self) -> String {
        self.metrics().to_prometheus_text()
    }

    /// 是否已关闭。
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.state.load(Ordering::Acquire) & CLOSED_BIT != 0
    }

    /// liveness：仅看本地池是否仍接受请求（不访问网络）。
    #[must_use]
    pub fn liveness(&self) -> bool {
        !self.is_closed()
    }

    /// 健康检查：本地 open + 有 deadline 的 `SELECT SERVER_VERSION()`。
    ///
    /// 未就绪时返回 `Ok(TaosHealth { ready: false, .. })` 而非 `Err`。
    pub async fn health_check(&self) -> TaosResult<TaosHealth> {
        let stats = self.stats();
        let precision = self.precision();
        if stats.closed {
            self.inner.metrics.inc_health_not_ready();
            return Ok(TaosHealth {
                ready: false,
                precision,
                server_version: None,
                stats,
                metrics: self.metrics(),
                detail: "池已关闭".to_owned(),
            });
        }
        match self.exec("SELECT SERVER_VERSION()").await {
            Ok(result) if result.code == 0 => {
                self.inner.metrics.inc_ping_ok();
                self.inner.metrics.inc_health_ready();
                Ok(TaosHealth {
                    ready: true,
                    precision,
                    server_version: result.rows.first().and_then(|row| row.first()).cloned(),
                    stats: self.stats(),
                    metrics: self.metrics(),
                    detail: "就绪".to_owned(),
                })
            }
            Ok(result) => {
                self.inner.metrics.inc_ping_err();
                self.inner.metrics.inc_health_not_ready();
                Ok(TaosHealth {
                    ready: false,
                    precision,
                    server_version: None,
                    stats: self.stats(),
                    metrics: self.metrics(),
                    detail: format!("ping code={}", result.code),
                })
            }
            Err(error) => {
                self.inner.metrics.inc_ping_err();
                self.inner.metrics.inc_health_not_ready();
                Ok(TaosHealth {
                    ready: false,
                    precision,
                    server_version: None,
                    stats: self.stats(),
                    metrics: self.metrics(),
                    detail: format!("不可用: {error}"),
                })
            }
        }
    }

    /// 轻量健康检查：`SELECT SERVER_VERSION()` 成功且 `code == 0` 则返回 `Ok(())`。
    pub async fn ping(&self) -> TaosResult<()> {
        match self.exec("SELECT SERVER_VERSION()").await {
            Ok(result) if result.code == 0 => {
                self.inner.metrics.inc_ping_ok();
                Ok(())
            }
            Ok(result) => {
                self.inner.metrics.inc_ping_err();
                Err(TaosError::Unavailable(format!("ping code={}", result.code)))
            }
            Err(error) => {
                self.inner.metrics.inc_ping_err();
                Err(error)
            }
        }
    }

    /// 在配置 database 上下文执行 SQL。
    pub async fn exec(&self, sql: &str) -> TaosResult<TaosExecResult> {
        self.exec_sql_raw(sql, true).await
    }

    /// 执行只读查询 SQL（语义同 [`TaosPool::exec`]，便于调用方表达意图）。
    pub async fn query(&self, sql: &str) -> TaosResult<TaosExecResult> {
        self.exec(sql).await
    }

    /// 通过 Native WS 执行一条 SQL（短会话：握手 → 发送 → 关闭）。
    pub async fn exec_sql_ws(&self, sql: &str) -> TaosResult<String> {
        native::exec_sql_ws(self.config(), sql).await
    }

    /// 写入序列前确保超级表存在（`ts TIMESTAMP, bid NCHAR(64), ask NCHAR(64)`）。
    pub async fn ensure_stable(&self, table: &str) -> TaosResult<()> {
        validate_stable_ident(table)?;
        let sql = format!(
            "CREATE STABLE IF NOT EXISTS `{table}` (\
               ts TIMESTAMP, bid NCHAR(64), ask NCHAR(64)\
             ) TAGS (symbol NCHAR(128))"
        );
        let result = self.exec(&sql).await?;
        if result.code != 0 {
            return Err(TaosError::from_taos_code(result.code, "ensure_stable 失败"));
        }
        self.verify_decimal_schema(table).await
    }

    /// 关闭池：标记关闭、关闭信号量并等待在途请求排空（受 `close_timeout` 约束）。
    ///
    /// 可重复调用；已关闭时再次调用仍会等待排空。
    pub async fn close(&self) -> TaosResult<()> {
        self.inner.state.fetch_or(CLOSED_BIT, Ordering::AcqRel);
        self.inner.sem.close();
        let drain = async {
            loop {
                let notified = self.inner.drained.notified();
                let mut notified = std::pin::pin!(notified);
                notified.as_mut().enable();
                if self.inner.state.load(Ordering::Acquire) & IN_FLIGHT_MASK == 0 {
                    return;
                }
                notified.await;
            }
        };
        timeout(self.inner.config.close_timeout, drain)
            .await
            .map_err(|_| TaosError::Timeout("close 等待在途请求排空超时".to_owned()))?;
        Ok(())
    }

    /// 校验 `bid` / `ask` 必须为 `NCHAR(64+)`，拒绝 DOUBLE 精度降级。
    pub(super) async fn verify_decimal_schema(&self, table: &str) -> TaosResult<()> {
        validate_stable_ident(table)?;
        let result = self.exec(&format!("DESCRIBE `{table}`")).await?;
        validate_decimal_schema(&result)
    }

    /// 从 `information_schema.ins_databases` 探测数据库精度。
    async fn detect_precision(&self) -> TaosResult<TsPrecision> {
        let database = self.inner.config.database.clone();
        validate_ident(&database)?;
        let escaped = escape_str(&database);
        let sql = format!(
            "SELECT `precision` FROM information_schema.ins_databases WHERE name='{escaped}'"
        );
        let result = self.exec_sql_raw(&sql, false).await?;
        result
            .rows
            .first()
            .and_then(|row| row.first())
            .and_then(|value| TsPrecision::parse(value))
            .ok_or_else(|| {
                TaosError::Unavailable("无法从 information_schema 探测数据库精度".to_owned())
            })
    }

    /// 获取 in-flight 许可（含 acquire 超时与关闭检查）。
    pub(super) async fn acquire(&self) -> TaosResult<RequestGuard> {
        self.ensure_open()?;
        let acquired = timeout(
            self.inner.config.acquire_timeout,
            self.inner.sem.clone().acquire_owned(),
        )
        .await;
        match acquired {
            Ok(Ok(permit)) => loop {
                let state = self.inner.state.load(Ordering::Acquire);
                if state & CLOSED_BIT != 0 {
                    drop(permit);
                    return Err(TaosError::Closed("pool 已关闭".to_owned()));
                }
                let next = state
                    .checked_add(1)
                    .ok_or_else(|| TaosError::Config("in-flight 计数溢出".to_owned()))?;
                if self
                    .inner
                    .state
                    .compare_exchange_weak(state, next, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return Ok(RequestGuard {
                        _permit: permit,
                        inner: Arc::clone(&self.inner),
                    });
                }
            },
            Ok(Err(_)) => Err(TaosError::Closed("背压信号量已关闭".to_owned())),
            Err(_) => Err(TaosError::Timeout(format!(
                "获取 in-flight 许可超时（max={}）",
                self.inner.config.max_in_flight
            ))),
        }
    }

    /// 池级 SQL 入口：字节上限 + 背压 + 计数。
    async fn exec_sql_raw(&self, sql: &str, use_database: bool) -> TaosResult<TaosExecResult> {
        if sql.len() > self.inner.config.batch_max_bytes {
            self.inner.metrics.inc_sql_err();
            return Err(TaosError::Invalid(format!(
                "SQL 请求超过 batch_max_bytes={} 字节",
                self.inner.config.batch_max_bytes
            )));
        }
        self.inner.metrics.add_sql_bytes(sql.len());
        let _guard = self.acquire().await?;
        match self.exec_sql_raw_inner(sql, use_database).await {
            Ok(result) => {
                self.inner.metrics.inc_sql_ok();
                Ok(result)
            }
            Err(error) => {
                self.inner.metrics.inc_sql_err();
                Err(error)
            }
        }
    }

    /// 单次 REST 请求（已持有 in-flight 许可）。
    async fn exec_sql_raw_inner(
        &self,
        sql: &str,
        use_database: bool,
    ) -> TaosResult<TaosExecResult> {
        let config = &self.inner.config;
        let url = if use_database {
            config.rest_sql_db_url()
        } else {
            config.rest_sql_url()
        };

        debug!(target: "taosx", database = %config.database, "taos rest sql");

        let response = self
            .inner
            .http
            .post(&url)
            .basic_auth(&config.user, Some(&config.password))
            .header(reqwest::header::CONTENT_TYPE, "text/plain; charset=utf-8")
            .body(sql.to_owned())
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() {
                    TaosError::Timeout(format!("请求超时: {error}"))
                } else {
                    TaosError::Connection(format!("请求失败: {error}"))
                }
            })?;

        let status = response.status();
        let text = read_response_limited(response, config.max_response_bytes).await?;
        self.inner.metrics.add_response_bytes(text.len());

        if !status.is_success() {
            // 标准 §4：响应正文可能夹带凭据或 SQL 片段，一律不入错误消息
            //（错误会向上传播进调用方日志/UI，泄露面不可控）。诊断信息改走
            // debug 日志通道（默认关闭、运维显式 opt-in），截断至 256 字符
            // 限制日志侧泄露面（R-SEC-006 / R-OBS-003）。
            debug!(
                target: "taosx",
                status = status.as_u16(),
                body = %truncate(&text, 256),
                "非成功 HTTP 响应正文（仅诊断，不入错误消息）"
            );
            return Err(TaosError::from_http_status(
                status.as_u16(),
                "响应正文已省略",
            ));
        }

        let result = parse_taos_json(&text)?;
        if result.rows.len() > config.max_query_rows {
            return Err(TaosError::Unavailable(format!(
                "SQL 结果超过 max_query_rows={}",
                config.max_query_rows
            )));
        }
        Ok(result)
    }

    /// 关闭检查。
    fn ensure_open(&self) -> TaosResult<()> {
        if self.is_closed() {
            return Err(TaosError::Closed("pool 已关闭".to_owned()));
        }
        Ok(())
    }
}
