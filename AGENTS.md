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
├── client/      # 连接池实现子模块
│   ├── response.rs # 响应解析
│   ├── sql.rs      # SQL 执行与标识符校验
│   ├── types.rs    # 内部类型
│   └── write.rs    # 写入路径
├── config.rs    # TaosConfig 门面：ENV_*/DEFAULT_*/HARD_MAX_* 常量、定义与 Default/Debug、
│                # from_env/from_toml(_file)/validate/builder、apply_env_overrides、内联测试
├── config/      # 配置子模块
│   ├── builder.rs  # TaosConfigBuilder（链式覆盖）
│   ├── endpoint.rs # REST / WS 端点 URL 与连接尝试主机序列
│   ├── enums.rs    # TsPrecision / TransportMode
│   └── parse.rs    # TOML 反序列化、env 读取、主机与标识符校验
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
- 资源上界（批量行数/字节、in-flight、查询行数、响应字节）在构建期 `validate` fail-fast，禁止静默 clamp 到 `HARD_MAX_*`

## 门禁（P0）

与 `.github/workflows/ci.yml` 的 gate job 逐字一致（组织标准命令）：

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

doc / deny 书面暂缓声明（组织 `ci.md` §1：fmt + clippy + test 不可豁免，doc/deny 可由项目声明暂缓）：

- `cargo doc --workspace --no-deps --all-features`：**暂缓**。本仓库为单 crate 库，文档质量已由
  `#![deny(missing_docs)]` 与 doctest 覆盖；独立 doc 构建门禁待与 `cargo deny` 一并评估引入。
- `cargo deny check`：**暂缓**。仓库尚无 `deny.toml`，供应链审计计划 2026-12 前建立。

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
