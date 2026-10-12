//! `exec_sql_ws_rows`：单连接同步读取完整结果集（特性 009 T2 / T4 / T5）。
//!
//! 协议依据 03a §1–§3、§6：`conn` → `query` → 循环 {`fetch` → `fetch_block`} 直到
//! `fetch` 回 `completed:true`。要点：
//!
//! - **req_id**：所有请求带客户端自增 `req_id`，JSON 响应（含错误帧）必须原样回显，
//!   否则视为配对失败（fail-closed）。
//! - **id × req_id 路由**：`fetch_block` 成功响应是 Binary 帧，只含 16 字节头
//!   （`timing` u64 LE + 结果集句柄 `id` u64 LE），不含 `req_id`；本连接一次只有一个
//!   在途请求，故按「头内 `id` → 在途 `fetch_block` 的 `req_id`」关联（`InFlightBlock`）。
//! - **双形态**：`fetch_block` 失败回 Text JSON 错误帧，按错误处理，不当二进制块解析。
//! - **字节通道**（T5）：Binary 帧直接取原始字节交给解码器，不经 `frame_payload`
//!   的 UTF-8 校验。
//! - **释放语义**：`completed:true` 或 `fetch` 出错时服务端已自动释放结果集，不再发
//!   `free_result`；其余中途放弃（本地校验失败、超行数等）发 `free_result` 且**不等待**
//!   响应（服务端本就不回帧）。
//!
//! 不做连接池或跨连接并发；整体超时（`config.timeout`）触发时直接丢弃连接，服务端随
//! 会话断开回收结果集。

use futures_util::{Sink, SinkExt, Stream};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use tokio_tungstenite::connect_async_with_config;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tracing::debug;

use super::raw_block::{decode_raw_block, precision_from_code, WsQueryResult, WsValue};
use super::{
    build_native_ws_url, ensure_code_zero, frame_kind, frame_payload, parse_status_code,
    read_frame, ws_config_from,
};
use crate::config::{TaosConfig, TsPrecision};
use crate::error::{TaosError, TaosResult};

/// `fetch_block` 二进制帧头长度：`timing` u64 LE + `id` u64 LE（03a §2，3.x 格式）。
const BLOCK_FRAME_HEADER_LEN: usize = 16;

/// 带 `req_id` 回显的最小响应模型（`conn` 用）。
#[derive(Deserialize)]
struct EchoReply {
    req_id: Option<u64>,
}

/// `query` 成功响应中本模块用到的字段（未知字段忽略）。
#[derive(Deserialize)]
struct QueryReply {
    req_id: Option<u64>,
    id: Option<u64>,
    #[serde(default)]
    is_update: bool,
    fields_count: Option<usize>,
    fields_names: Option<Vec<String>>,
    fields_types: Option<Vec<u8>>,
    precision: Option<i64>,
}

/// `fetch` 成功响应（03a §1）；`completed:true` 时无 `lengths` / `rows`。
#[derive(Deserialize)]
struct FetchReply {
    req_id: Option<u64>,
    id: Option<u64>,
    #[serde(default)]
    completed: bool,
    lengths: Option<Vec<u32>>,
    rows: Option<usize>,
}

/// 服务端结果集句柄及其释放状态。
struct ResultHandle {
    id: u64,
    /// `true` 表示服务端已自动释放（`completed:true` 或 `fetch` 出错），不得再 `free_result`。
    server_released: bool,
}

/// 在途 `fetch_block`：二进制帧只带 `id`，靠它映射回发出该请求的 `req_id`。
struct InFlightBlock {
    req_id: u64,
    result_id: u64,
}

/// 读取所需的结果集元数据（来自 `query` 响应）。
struct ResultMeta {
    fields_names: Vec<String>,
    fields_types: Vec<u8>,
    precision: TsPrecision,
}

/// 单连接同步会话：持有 socket 与 `req_id` 计数器。
struct RowsSession<'s, S> {
    socket: &'s mut S,
    last_req_id: u64,
}

/// 完整结果读取：连接 `/rest/ws`，`conn` → `query` → `fetch` / `fetch_block` 直到
/// `completed:true`，返回列名、类型码、精度与全部行值。
///
/// 与 [`crate::exec_sql_ws`] 并存：后者仍只返回 `query` 元数据帧。
///
/// - 非查询语句（`query` 响应 `is_update:true`）不发 `fetch`，返回零行结果。
/// - 累计行数受 `config.max_query_rows` 约束；累计块字节与变长值物化字节分别受
///   `config.max_response_bytes` 约束；超限返回 [`TaosError::Unavailable`] 并发
///   `free_result`（不等待）。
/// - 协议异常 / 服务端非 0 `code` / `req_id` 不配对 → [`TaosError::Unavailable`]；
///   raw block 结构非法 → [`TaosError::Serialization`]；DECIMAL / BLOB 等未确认布局的类型
///   → [`TaosError::Unsupported`]；空 SQL → [`TaosError::Invalid`]；超时 →
///   [`TaosError::Timeout`]。
/// - 服务端 `message` 与凭据不入错误消息。
pub async fn exec_sql_ws_rows(config: &TaosConfig, sql: &str) -> TaosResult<WsQueryResult> {
    config.validate()?;
    if sql.trim().is_empty() {
        return Err(TaosError::Invalid("exec_sql_ws_rows: 空 SQL".to_owned()));
    }
    let url = build_native_ws_url(config);
    let ws_config = ws_config_from(config);
    let attempt = async {
        let (mut socket, _response) = connect_async_with_config(&url, Some(ws_config), false)
            .await
            .map_err(|error| TaosError::Unavailable(format!("ws 连接失败: {error}")))?;
        let result = read_result_set(&mut socket, config, sql).await;
        if let Err(error) = socket.close(None).await {
            debug!(target: "taosx", %error, "ws 关闭失败（不覆盖读取结果）");
        }
        result
    };
    match tokio::time::timeout(config.timeout, attempt).await {
        Ok(result) => {
            // 与 exec_sql_ws 同口径：计入 ws_probe 成功/失败计数。
            crate::metrics::record_ws_probe(result.is_ok());
            result
        }
        Err(_) => {
            crate::metrics::record_ws_probe(false);
            Err(TaosError::Timeout(format!("ws sql 超时: {url}")))
        }
    }
}

/// 会话主流程；与连接建立/关闭解耦，便于对任意 Sink + Stream 驱动。
async fn read_result_set<S>(
    socket: &mut S,
    config: &TaosConfig,
    sql: &str,
) -> TaosResult<WsQueryResult>
where
    S: Sink<Message, Error = WsError> + Stream<Item = Result<Message, WsError>> + Unpin,
{
    let mut session = RowsSession {
        socket,
        last_req_id: 0,
    };
    session.conn(config).await?;
    let query = session.query(sql).await?;
    if query.is_update {
        // 写入 / DDL 无结果集：服务端不保留句柄，无需 fetch 或 free_result。
        let precision = match query.precision {
            Some(code) => precision_from_code(code)?,
            None => TsPrecision::Ms,
        };
        return Ok(WsQueryResult {
            fields_names: Vec::new(),
            fields_types: Vec::new(),
            precision,
            rows: Vec::new(),
        });
    }
    let (id, meta) = result_meta(query)?;
    let mut handle = ResultHandle {
        id,
        server_released: false,
    };
    match session.fetch_all(&mut handle, &meta, config).await {
        Ok(rows) => Ok(WsQueryResult {
            fields_names: meta.fields_names,
            fields_types: meta.fields_types,
            precision: meta.precision,
            rows,
        }),
        Err(error) => {
            if !handle.server_released {
                session.free_result(handle.id).await;
            }
            Err(error)
        }
    }
}

/// 校验 `query` 响应并提取句柄与列元数据。
fn result_meta(query: QueryReply) -> TaosResult<(u64, ResultMeta)> {
    let id = match query.id {
        Some(id) if id != 0 => id,
        _ => {
            return Err(TaosError::Unavailable(
                "ws query 响应缺少结果集 id".to_owned(),
            ))
        }
    };
    let fields_names = query.fields_names.unwrap_or_default();
    let fields_types = query.fields_types.unwrap_or_default();
    if fields_types.is_empty()
        || fields_names.len() != fields_types.len()
        || query.fields_count != Some(fields_types.len())
    {
        return Err(TaosError::Unavailable(
            "ws query 响应列元数据不一致".to_owned(),
        ));
    }
    let precision = precision_from_code(query.precision.ok_or_else(|| {
        TaosError::Unavailable("ws query 响应缺少 precision".to_owned())
    })?)?;
    Ok((
        id,
        ResultMeta {
            fields_names,
            fields_types,
            precision,
        },
    ))
}

impl<S> RowsSession<'_, S>
where
    S: Sink<Message, Error = WsError> + Stream<Item = Result<Message, WsError>> + Unpin,
{
    /// 发送一条带新 `req_id` 的 Text JSON 请求，返回该 `req_id`。
    async fn send(
        &mut self,
        action: &str,
        mut args: serde_json::Map<String, serde_json::Value>,
    ) -> TaosResult<u64> {
        self.last_req_id += 1;
        let req_id = self.last_req_id;
        args.insert("req_id".to_owned(), req_id.into());
        let payload = serde_json::json!({ "action": action, "args": args }).to_string();
        self.socket
            .send(Message::Text(payload.into()))
            .await
            .map_err(|error| TaosError::Unavailable(format!("ws 发送 {action} 失败: {error}")))?;
        Ok(req_id)
    }

    /// 读下一帧作为 JSON 响应：`code` 必须为 0，且 `req_id` 回显必须配对。
    async fn read_json<T>(&mut self, action: &str, req_id: u64) -> TaosResult<T>
    where
        T: DeserializeOwned,
    {
        let payload = frame_payload(read_frame(&mut *self.socket).await?)?;
        ensure_code_zero(&payload, action)?;
        let echo: EchoReply = parse_reply(&payload, action)?;
        ensure_req_id(echo.req_id, req_id, action)?;
        parse_reply(&payload, action)
    }

    async fn conn(&mut self, config: &TaosConfig) -> TaosResult<()> {
        let mut args = serde_json::Map::new();
        args.insert("user".to_owned(), config.user.clone().into());
        args.insert("password".to_owned(), config.password.clone().into());
        let req_id = self.send("conn", args).await?;
        let _reply: EchoReply = self.read_json("conn", req_id).await?;
        Ok(())
    }

    async fn query(&mut self, sql: &str) -> TaosResult<QueryReply> {
        let mut args = serde_json::Map::new();
        args.insert("sql".to_owned(), sql.into());
        let req_id = self.send("query", args).await?;
        self.read_json("query", req_id).await
    }

    /// 循环 `fetch` / `fetch_block` 直到 `completed:true`。
    ///
    /// 返回 `Err` 时由调用方依据 `handle.server_released` 决定是否 `free_result`。
    async fn fetch_all(
        &mut self,
        handle: &mut ResultHandle,
        meta: &ResultMeta,
        config: &TaosConfig,
    ) -> TaosResult<Vec<Vec<WsValue>>> {
        let mut rows: Vec<Vec<WsValue>> = Vec::new();
        let mut block_budget = config.max_response_bytes;
        let mut text_budget = config.max_response_bytes;
        loop {
            let fetch_req_id = self.send("fetch", handle_args(handle.id)).await?;
            let payload = frame_payload(read_frame(&mut *self.socket).await?)?;
            let code = parse_status_code(&payload)?;
            if code != 0 {
                // fetch 出错：服务端已自动释放结果集（03a §1），不得再 free_result。
                handle.server_released = true;
                return Err(TaosError::Unavailable(format!(
                    "ws fetch 被服务端拒绝: code={code}"
                )));
            }
            let reply: FetchReply = parse_reply(&payload, "fetch")?;
            ensure_req_id(reply.req_id, fetch_req_id, "fetch")?;
            if reply.id != Some(handle.id) {
                return Err(TaosError::Unavailable(
                    "ws fetch 响应 id 与结果集句柄不符".to_owned(),
                ));
            }
            if reply.completed {
                // 取完：服务端已自动释放，free_result 非必需。
                handle.server_released = true;
                return Ok(rows);
            }
            let block_rows = match reply.rows {
                Some(count) if count > 0 => count,
                _ => {
                    return Err(TaosError::Unavailable(
                        "ws fetch 未完成却无行数".to_owned(),
                    ))
                }
            };
            let lengths = reply
                .lengths
                .filter(|lengths| lengths.len() == meta.fields_types.len())
                .ok_or_else(|| {
                    TaosError::Unavailable("ws fetch 响应 lengths 与列数不符".to_owned())
                })?;
            if rows.len().saturating_add(block_rows) > config.max_query_rows {
                return Err(TaosError::Unavailable(format!(
                    "ws 结果超过 max_query_rows={}",
                    config.max_query_rows
                )));
            }

            let block_req_id = self.send("fetch_block", handle_args(handle.id)).await?;
            let in_flight = InFlightBlock {
                req_id: block_req_id,
                result_id: handle.id,
            };
            let frame = match read_frame(&mut *self.socket).await? {
                Message::Binary(bytes) => bytes,
                Message::Text(text) => {
                    return Err(fetch_block_text_error(text.as_str(), &in_flight))
                }
                other => {
                    return Err(TaosError::Unavailable(format!(
                        "ws fetch_block 响应帧类型非法: {}",
                        frame_kind(&other)
                    )))
                }
            };
            let block = route_block_frame(&frame, &in_flight)?;
            block_budget = block_budget.checked_sub(block.len()).ok_or_else(|| {
                TaosError::Unavailable(format!(
                    "ws 累计块字节超过 max_response_bytes={}",
                    config.max_response_bytes
                ))
            })?;
            let decoded =
                decode_raw_block(block, &meta.fields_types, meta.precision, &mut text_budget)?;
            if decoded.rows.len() != block_rows || decoded.col_lengths != lengths {
                return Err(TaosError::Serialization(
                    "raw block 解码失败: nrows / 列长与 fetch 响应不符".to_owned(),
                ));
            }
            rows.extend(decoded.rows);
        }
    }

    /// fire-and-forget：服务端对 `free_result` 不回任何帧（03a §3），故只发送不读取。
    /// 发送失败只记日志，不覆盖调用方已取得的业务错误。
    async fn free_result(&mut self, id: u64) {
        if let Err(error) = self.send("free_result", handle_args(id)).await {
            debug!(target: "taosx", %error, "ws free_result 发送失败（连接即将关闭）");
        }
    }
}

/// `fetch` / `fetch_block` / `free_result` 共用参数 `{"id": 句柄}`（`req_id` 由 `send` 补）。
fn handle_args(id: u64) -> serde_json::Map<String, serde_json::Value> {
    let mut args = serde_json::Map::new();
    args.insert("id".to_owned(), id.into());
    args
}

fn parse_reply<T: DeserializeOwned>(payload: &str, action: &str) -> TaosResult<T> {
    serde_json::from_str(payload)
        .map_err(|_| TaosError::Unavailable(format!("ws {action} 响应结构非法")))
}

fn ensure_req_id(echo: Option<u64>, sent: u64, action: &str) -> TaosResult<()> {
    if echo == Some(sent) {
        return Ok(());
    }
    Err(TaosError::Unavailable(format!(
        "ws {action} 响应 req_id 与请求不配对"
    )))
}

/// 按头内 `id` 把二进制帧路由到在途 `fetch_block`，返回 raw block 载荷（`[16..)`）。
///
/// `timing` 只是服务端耗时（纳秒），不参与判定。
fn route_block_frame<'f>(frame: &'f [u8], in_flight: &InFlightBlock) -> TaosResult<&'f [u8]> {
    let (header, block) = frame
        .split_at_checked(BLOCK_FRAME_HEADER_LEN)
        .ok_or_else(|| TaosError::Unavailable("ws fetch_block 二进制帧短于 16 字节头".to_owned()))?;
    let mut id_bytes = [0u8; 8];
    id_bytes.copy_from_slice(&header[8..16]);
    let id = u64::from_le_bytes(id_bytes);
    if id != in_flight.result_id {
        return Err(TaosError::Unavailable(format!(
            "ws 二进制帧 id={id} 无对应在途 fetch_block（req_id={}）",
            in_flight.req_id
        )));
    }
    Ok(block)
}

/// `fetch_block` 收到 Text 帧：按 JSON 错误帧处理（双形态），绝不当二进制块解析。
fn fetch_block_text_error(text: &str, in_flight: &InFlightBlock) -> TaosError {
    match parse_status_code(text) {
        Ok(0) => TaosError::Unavailable(
            "ws fetch_block 期望 Binary 块，收到 code=0 的 Text 帧".to_owned(),
        ),
        Ok(code) => {
            // 错误帧只带 req_id 不带 id；不配对时仍是错误，只在消息里区分。
            let paired = parse_reply::<EchoReply>(text, "fetch_block")
                .map(|echo| echo.req_id == Some(in_flight.req_id))
                .unwrap_or(false);
            let suffix = if paired { "" } else { "（req_id 不配对）" };
            TaosError::Unavailable(format!(
                "ws fetch_block 被服务端拒绝: code={code}{suffix}"
            ))
        }
        Err(error) => error,
    }
}
