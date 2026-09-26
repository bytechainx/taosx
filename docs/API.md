# taosx 公开 API

**版本 / 角色**：`taosx 0.1.5` · TDengine 异步客户端（REST + 原生 WebSocket 双传输）

## 公开消费面

| 主题 | 类型 / 函数 | 说明 |
| --- | --- | --- |
| 客户端 | `TaosPool`（别名 `TaosClient`） | `new`（不发网）/ `connect` / `connect_from_env` / `exec` / `query` / `query_series` / `write_batch*` / `write_series` / `ensure_stable` / `exec_sql_ws` / `ping` / `health_check` / `close` |
| 客户端状态 | `TaosPoolStats`、`TaosHealth`、`TaosExecResult` | 池统计、健康检查、执行结果 |
| 批量写入 | `BatchWriteReport`、`BatchWritePartialError` | 写入报告与部分失败诊断 |
| 配置 | `TaosConfig`、`TaosConfigBuilder` | `builder()` / `from_env()`（前缀 `FOUNDATIONX_TAOSX_`）/ `from_toml()` / `validate()` |
| 传输 | `TransportMode` | `Rest`（默认，端口 6041）/ `NativeWs`（`ws(s)://host:port/rest/ws`） |
| 精度 | `TsPrecision` | `Ms`（默认）/ `Us` / `Ns` |
| 硬上限 | `HARD_MAX_IN_FLIGHT`、`HARD_MAX_BATCH_ROWS`、`HARD_MAX_BATCH_BYTES`、`HARD_MAX_QUERY_ROWS`、`HARD_MAX_RESPONSE_BYTES`、`HARD_MAX_CLOSE_TIMEOUT`、`HARD_MAX_TIMEOUT`、`HARD_MAX_WRITE_MAX_ATTEMPTS` | `validate` 越界 fail-fast。env/builder 会把 `write_max_attempts=0` 抬到 `1` 再校验 |
| 错误 | `TaosError`、`TaosResult` | thiserror 枚举 + `#[non_exhaustive]` |
| 数据点 | `TaosPoint` | `new(tag_value, timestamp_ns, first_value, second_value)` |
| SQL 构造 | `build_insert_sql_chunks` | 纯函数：分块 INSERT SQL 构造，含注入防护 |
| 批处理器 | `WriteBatcher`、`WriteBatcherConfig`、`BatcherCloseReport`、`BatcherCloseError` | 异步累积批量写入 |
| 查询流 | `TaosQueryStream` | 有界查询结果流 |
| 重试 | `RetryPolicy` | 指数退避 + 抖动 |
| 观测 | `TaosMetricsSnapshot`、`ws_probe_totals` | 指标快照与 WS 探测计数 |
| 原生 WS | `build_native_ws_url`、`connect_native_ws`、`exec_sql_ws`、`probe_native_tcp`、`validate_mode` | 握手探测与短会话 SQL 执行 |

## SQL 注入防护

所有进入 SQL 文本的调用方输入都经过白名单或转义：

- 超级表名 / 库名：标识符校验（字母或下划线开头，仅含 `[A-Za-z0-9_]`，限长）；
- tag 值：十六进制编码进子表名；
- 字符串字面量：按 TDengine 规则转义（`\` → `\\`、`'` → `\'`）；
- 时间戳：只以十进制整数拼接。

## 最小用法

```rust,no_run
use taosx::{TaosConfig, TaosPoint, TaosPool};

# #[tokio::main]
# async fn main() -> taosx::TaosResult<()> {
let config = TaosConfig::builder().host("127.0.0.1").database("ticks").build()?;
let client = TaosPool::connect(config).await?;

let points = vec![TaosPoint::new("BTC/USDT", 1_700_000_000_000_000_000, "66522.40", "66523.10")];
client.write_batch("ticks", &points).await?;

let result = client.query("SELECT COUNT(*) FROM `ticks`").await?;
client.close().await?;
# Ok(())
# }
```

## 能力边界

- 只做 TDengine 访问原语：连接、SQL 执行、批量写入、有界查询流；不含领域模型或业务编排。
- `new` **不发网**（只 `validate` + 装配客户端）。`connect` **会发网**：`NativeWs` 先做 WS 握手；`database` 非空时经 REST 执行 `CREATE DATABASE IF NOT EXISTS` 并探测精度（与配置不一致则 fail-closed）；两种模式最后都 `ping`。主 `host` 失败则按 `hosts` 故障转移。默认库名 `infra_draft`、默认端口 `6041`。`NativeWs` 下 DDL/`exec`/`query` 仍走 REST。`exec_sql_ws` 仍只到 `query` 元数据帧，不含结果行。`query_series` 只收表名 + 纳秒闭区间，缺表返回空集。
- 查询结果受 `HARD_MAX_QUERY_ROWS` / `HARD_MAX_RESPONSE_BYTES` 上界约束，超限报错而非静默截断。
- 凭据（user/password）只从 env 或 builder 注入，`Debug` 输出脱敏。
