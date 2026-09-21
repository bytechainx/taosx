# taosx Agent 指南

> 本文件为 AI Agent 在本仓库工作时的入口指南。

## 项目定位

TDengine 异步客户端：REST + 原生 WebSocket 双传输，提供连接池背压、批量写入与分块 SQL 构造、SQL 注入防护、重试策略与有界查询流。

## 技术栈

- Rust edition 2021, rust-version 1.85
- 关键依赖: `reqwest`（rustls-tls）、`tokio-tungstenite`、`tokio`、`serde`/`serde_json`、`thiserror`、`tracing`、`chrono`、`futures-util`、`toml`、`url`
- 零内部耦合，不依赖 kernel/contracts 等私有 crate

## 代码结构

```text
src/
├── lib.rs       # 入口：模块声明 + 受控 re-export
├── batcher.rs   # WriteBatcher 异步批量写入器
├── client.rs    # TaosPool 连接池 + build_insert_sql_chunks 分块 SQL 构造
├── config.rs    # TaosConfig 配置结构体 + builder + env/toml 加载 + 校验
├── error.rs     # TaosError / TaosResult
├── metrics.rs   # TaosMetricsSnapshot / ws_probe_totals 观测指标
├── native.rs    # 原生 WebSocket 握手、短会话 SQL 执行、TCP 探测
├── point.rs     # TaosPoint 数据点
├── retry.rs     # RetryPolicy 重试策略
└── stream.rs    # TaosQueryStream 有界查询流
```

## 开发约定

- 注释与文档使用简体中文；标识符保持英文
- 错误：`TaosError` thiserror 枚举 + `#[non_exhaustive]` + `TaosResult<T>` 别名
- 配置：结构体 + `builder()`/`from_env()`/`from_toml()` + `validate()` + fail-fast
- 凭据只能从 env 或 builder 注入，`Debug` 输出脱敏
- 禁止裸 `unwrap()`（库代码，crate 已 `#![deny(clippy::unwrap_used)]`）
- async tokio，禁止阻塞 I/O；`#![forbid(unsafe_code)]`
- 所有进入 SQL 文本的调用方输入必须经过白名单标识符校验或转义（见 `build_insert_sql_chunks` 文档）
- 资源上界（批量行数/字节、in-flight、查询行数、响应字节）在构建期校验并 clamp 到 `HARD_MAX_*`

## 门禁（P0）

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

热路径基准（离线，无需 TDengine 服务）：

```bash
cargo bench --bench hot_path -- --quick
```

## 相关文档

- 组织 Rust 规范：`~/org-config/rulesets/rust/RULES.md`
- API 文档：`docs/API.md`
- 标准与验收：`docs/标准.md`
- 术语与领域语言：`CONTEXT.md`
- 贡献指南：`CONTRIBUTING.md`
- 变更记录：`CHANGELOG.md`
- 基准测试：`benches/hot_path.rs`
