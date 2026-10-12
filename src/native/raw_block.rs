//! TDengine 3.x raw block（V3）解码：`fetch_block` 二进制帧 `[16..)` 载荷 → 行值。
//!
//! 布局依据 03a §4（taosadapter `driver/common/parser/block.go` 与 taos-connector-rust
//! `raw/mod.rs` 双证），`version` / `flag` 取值依据特性 009 `research.md` §4 实测。
//! 全部字段小端：
//!
//! ```text
//! 0   version u32 | 4 length u32 | 8 nrows u32 | 12 ncols u32 | 16 flag u32 | 20 group_id u64
//! 28  列信息 ncols × 5 字节 {type:u8, bytes:u32}
//! 28+5n 列长 ncols × u32（每列数据段字节数，不含位图 / offsets）
//! 之后逐列：定长列 = null 位图 ceil(rows/8)（MSB 优先）+ rows×宽度；
//!           变长列 = offsets rows×i32（-1 = NULL）+ 数据段（每值 u16 长度前缀）
//! ```
//!
//! 解码分两遍：第一遍只校验边界、记录每列切片（不分配行值），第二遍才物化。这样在分配
//! `rows × cols` 个单元之前已证明每列至少占 `rows` 字节，分配量受块长约束。

use crate::config::TsPrecision;
use crate::error::{TaosError, TaosResult};

/// raw block 头部固定长度（字节）。
const HEADER_LEN: usize = 28;
/// 单列列信息长度：`type:u8` + `bytes:u32`（packed，无对齐填充）。
const COL_INFO_LEN: usize = 5;
/// 实测 `version`（2026-10-12，TDengine 3.3.6.0）。两端官方解析器都不读该字段，
/// 源码无法确定取值，故只接受实测值，不符即报错而不是猜测兼容。
const EXPECTED_VERSION: u32 = 1;
/// 实测 `flag`（Go 名 `hasColumnSegment`），理由同 [`EXPECTED_VERSION`]。
const EXPECTED_FLAG: u32 = 0x8000_0000;

/// WS 结果集的单元格值（由 raw block 解码而来）。
///
/// 整数统一放宽为 `i64` / `u64`，原列类型以 [`WsQueryResult::fields_types`] 为准，
/// 避免为每种宽度各设一个变体。
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum WsValue {
    /// SQL NULL（定长列 null 位图置位，或变长列 offset 为 `-1`）。
    Null,
    /// `BOOL`（类型码 1）。
    Bool(bool),
    /// 有符号整数：`TINYINT` / `SMALLINT` / `INT` / `BIGINT`（类型码 2–5）。
    Int(i64),
    /// 无符号整数：类型码 11–14。
    UInt(u64),
    /// `FLOAT`（类型码 6）。
    Float(f32),
    /// `DOUBLE`（类型码 7）。
    Double(f64),
    /// `TIMESTAMP`（类型码 9）：服务端原始 i64，按结果集精度解释，不做换算。
    Timestamp {
        /// 原始时间戳数值。
        value: i64,
        /// 结果集精度（query 响应 `precision`：0=ms / 1=us / 2=ns）。
        precision: TsPrecision,
    },
    /// `VARCHAR` / `BINARY`（类型码 8，严格 UTF-8）与 `NCHAR`（类型码 10，UCS-4 解码）。
    Text(String),
    /// `JSON` / `VARBINARY` / `GEOMETRY`（类型码 15 / 16 / 20）：原样字节，不猜编码。
    Bytes(Vec<u8>),
}

/// [`crate::exec_sql_ws_rows`] 的完整结果集。
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct WsQueryResult {
    /// 列名（query 响应 `fields_names`）。
    pub fields_names: Vec<String>,
    /// 列类型码（query 响应 `fields_types`，TSDB_DATA_TYPE_* 编码，见 03a §5）。
    pub fields_types: Vec<u8>,
    /// 结果集时间戳精度（query 响应 `precision`）。
    pub precision: TsPrecision,
    /// 按列顺序排列的行值。
    pub rows: Vec<Vec<WsValue>>,
}

/// 单块解码结果；`nrows` / `col_lengths` 供会话层与 `fetch` 响应交叉核对。
#[derive(Debug)]
pub(super) struct DecodedBlock {
    pub(super) col_lengths: Vec<u32>,
    pub(super) rows: Vec<Vec<WsValue>>,
}

/// 第一遍校验后单列的切片视图。
enum ColumnView<'a> {
    Fixed {
        kind: u8,
        width: usize,
        bitmap: &'a [u8],
        data: &'a [u8],
    },
    Variable {
        kind: u8,
        offsets: &'a [u8],
        data: &'a [u8],
    },
}

/// 结构性解码错误（块头 / 边界 / 编码不合法）。
fn malformed(detail: &str) -> TaosError {
    TaosError::Serialization(format!("raw block 解码失败: {detail}"))
}

fn read_u32(bytes: &[u8], at: usize) -> TaosResult<u32> {
    let end = at.checked_add(4).ok_or_else(|| malformed("偏移溢出"))?;
    let slice = bytes.get(at..end).ok_or_else(|| malformed("块截断"))?;
    let mut buf = [0u8; 4];
    buf.copy_from_slice(slice);
    Ok(u32::from_le_bytes(buf))
}

fn to_usize(value: u32) -> TaosResult<usize> {
    usize::try_from(value).map_err(|_| malformed("长度超出平台寻址范围"))
}

/// 从 `cursor` 起切出 `count` 字节并前移游标；越过块长即报错。
fn take<'a>(block: &'a [u8], cursor: &mut usize, count: usize) -> TaosResult<&'a [u8]> {
    let end = cursor
        .checked_add(count)
        .ok_or_else(|| malformed("偏移溢出"))?;
    let slice = block
        .get(*cursor..end)
        .ok_or_else(|| malformed("列数据越过块长"))?;
    *cursor = end;
    Ok(slice)
}

/// 定长类型宽度；变长类型返回 `Ok(None)`；协议未定义布局或本特性不支持的类型明确报错。
fn fixed_width(kind: u8) -> TaosResult<Option<usize>> {
    match kind {
        1 | 2 | 11 => Ok(Some(1)),
        3 | 12 => Ok(Some(2)),
        4 | 6 | 13 => Ok(Some(4)),
        5 | 7 | 9 | 14 => Ok(Some(8)),
        8 | 10 | 15 | 16 | 20 => Ok(None),
        // DECIMAL 的 bytes 位被复用为 {scale, prec, _, len}，数值布局未经实测；BLOB 用 u32 长度
        // 前缀、MEDIUMBLOB 布局 03a 未定义——宁可明确拒绝，也不按猜测布局给出错误数值。
        17 => Err(unsupported(kind, "DECIMAL")),
        21 => Err(unsupported(kind, "DECIMAL64")),
        18 => Err(unsupported(kind, "BLOB")),
        19 => Err(unsupported(kind, "MEDIUMBLOB")),
        other => Err(TaosError::Unsupported(format!(
            "raw block 类型码 {other} 无已确认布局"
        ))),
    }
}

fn unsupported(kind: u8, name: &str) -> TaosError {
    TaosError::Unsupported(format!("raw block 暂不支持 {name}（类型码 {kind}）"))
}

/// 结果集精度码（query 响应 `precision`）→ [`TsPrecision`]；未知码明确报错。
pub(super) fn precision_from_code(code: i64) -> TaosResult<TsPrecision> {
    match code {
        0 => Ok(TsPrecision::Ms),
        1 => Ok(TsPrecision::Us),
        2 => Ok(TsPrecision::Ns),
        other => Err(TaosError::Unavailable(format!(
            "ws query 响应 precision 非法: {other}"
        ))),
    }
}

/// 解码一个 V3 raw block。
///
/// - `fields_types`：query 响应的列类型码，块内列信息必须逐列一致；
/// - `text_budget`：跨块累计的变长值物化字节预算（重复 offset 会让同一段字节被多次
///   展开，故按「每次引用」扣减，防止小块放大成大内存）。
///
/// 列解析结束后、块长之内的剩余字节**忽略**：真实块（research §4.1）列后有 1 字节尾部，
/// 官方 `ReadBlock` 也不检查尾部；要求耗尽会拒绝每个真实块。
pub(super) fn decode_raw_block(
    block: &[u8],
    fields_types: &[u8],
    precision: TsPrecision,
    text_budget: &mut usize,
) -> TaosResult<DecodedBlock> {
    if block.len() < HEADER_LEN {
        return Err(malformed("块长不足 28 字节头"));
    }
    let version = read_u32(block, 0)?;
    if version != EXPECTED_VERSION {
        return Err(malformed(&format!(
            "version={version} 与实测值 {EXPECTED_VERSION} 不符"
        )));
    }
    let length = to_usize(read_u32(block, 4)?)?;
    if length != block.len() {
        return Err(malformed(&format!(
            "length 字段 {length} 与实际块长 {} 不符",
            block.len()
        )));
    }
    let nrows = to_usize(read_u32(block, 8)?)?;
    let ncols = to_usize(read_u32(block, 12)?)?;
    let flag = read_u32(block, 16)?;
    if flag != EXPECTED_FLAG {
        return Err(malformed(&format!(
            "flag=0x{flag:08x} 与实测值 0x{EXPECTED_FLAG:08x} 不符"
        )));
    }
    if ncols == 0 || ncols != fields_types.len() {
        return Err(malformed("ncols 与 query fields_types 列数不符"));
    }

    let mut cursor = HEADER_LEN;
    let col_info = take(
        block,
        &mut cursor,
        ncols
            .checked_mul(COL_INFO_LEN)
            .ok_or_else(|| malformed("列信息长度溢出"))?,
    )?;
    let col_len_bytes = take(
        block,
        &mut cursor,
        ncols
            .checked_mul(4)
            .ok_or_else(|| malformed("列长表长度溢出"))?,
    )?;

    // 第一遍：校验每列边界并记录切片，尚不分配行值。
    let mut views = Vec::with_capacity(ncols);
    let mut col_lengths = Vec::with_capacity(ncols);
    for (col, expected_kind) in fields_types.iter().enumerate() {
        let info_at = col * COL_INFO_LEN;
        let kind = col_info[info_at];
        if kind != *expected_kind {
            return Err(malformed(&format!(
                "第 {col} 列类型码 {kind} 与 query fields_types {expected_kind} 不符"
            )));
        }
        let col_bytes = to_usize(read_u32(col_info, info_at + 1)?)?;
        let col_length = read_u32(col_len_bytes, col * 4)?;
        col_lengths.push(col_length);
        let col_length = to_usize(col_length)?;
        match fixed_width(kind)? {
            Some(width) => {
                if col_bytes != width {
                    return Err(malformed(&format!("第 {col} 列宽度 {col_bytes} 与类型不符")));
                }
                if nrows.checked_mul(width) != Some(col_length) {
                    return Err(malformed(&format!("第 {col} 列长与 nrows×宽度不符")));
                }
                let bitmap = take(block, &mut cursor, nrows.div_ceil(8))?;
                let data = take(block, &mut cursor, col_length)?;
                views.push(ColumnView::Fixed {
                    kind,
                    width,
                    bitmap,
                    data,
                });
            }
            None => {
                let offsets = take(
                    block,
                    &mut cursor,
                    nrows
                        .checked_mul(4)
                        .ok_or_else(|| malformed("offsets 长度溢出"))?,
                )?;
                let data = take(block, &mut cursor, col_length)?;
                views.push(ColumnView::Variable {
                    kind,
                    offsets,
                    data,
                });
            }
        }
    }
    // 此处 `cursor <= block.len()` 已由 `take` 保证；剩余尾部字节按上文说明忽略。

    // 第二遍：物化。每列至少占 nrows 字节（位图+定宽 ≥ nrows，offsets = 4×nrows），
    // 故 rows×cols 单元数已受块长约束。
    let mut rows: Vec<Vec<WsValue>> = (0..nrows).map(|_| Vec::with_capacity(ncols)).collect();
    for view in &views {
        for (row, values) in rows.iter_mut().enumerate() {
            let value = match view {
                ColumnView::Fixed {
                    kind,
                    width,
                    bitmap,
                    data,
                } => {
                    // null 位图字节内 MSB 优先：第 row 行对应 bit 7-(row%8)。
                    if bitmap[row / 8] & (0x80 >> (row % 8)) != 0 {
                        WsValue::Null
                    } else {
                        let start = row * width;
                        fixed_value(*kind, &data[start..start + width], precision)?
                    }
                }
                ColumnView::Variable {
                    kind,
                    offsets,
                    data,
                } => {
                    let at = row * 4;
                    let mut buf = [0u8; 4];
                    buf.copy_from_slice(&offsets[at..at + 4]);
                    variable_value(*kind, i32::from_le_bytes(buf), data, text_budget)?
                }
            };
            values.push(value);
        }
    }
    Ok(DecodedBlock { col_lengths, rows })
}

/// 定长值解码；`bytes` 长度已由调用方保证等于类型宽度。
fn fixed_value(kind: u8, bytes: &[u8], precision: TsPrecision) -> TaosResult<WsValue> {
    let mut buf = [0u8; 8];
    buf[..bytes.len()].copy_from_slice(bytes);
    let value = match kind {
        1 => match bytes[0] {
            0 => WsValue::Bool(false),
            1 => WsValue::Bool(true),
            // 非 0/1 说明布局理解有误或数据损坏，不做「非零即真」的宽松解释。
            _ => return Err(malformed("BOOL 取值不在 0..=1")),
        },
        2 => WsValue::Int(i64::from(i8::from_le_bytes([buf[0]]))),
        3 => WsValue::Int(i64::from(i16::from_le_bytes([buf[0], buf[1]]))),
        4 => WsValue::Int(i64::from(i32::from_le_bytes([
            buf[0], buf[1], buf[2], buf[3],
        ]))),
        5 => WsValue::Int(i64::from_le_bytes(buf)),
        6 => WsValue::Float(f32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]])),
        7 => WsValue::Double(f64::from_le_bytes(buf)),
        9 => WsValue::Timestamp {
            value: i64::from_le_bytes(buf),
            precision,
        },
        11 => WsValue::UInt(u64::from(buf[0])),
        12 => WsValue::UInt(u64::from(u16::from_le_bytes([buf[0], buf[1]]))),
        13 => WsValue::UInt(u64::from(u32::from_le_bytes([
            buf[0], buf[1], buf[2], buf[3],
        ]))),
        14 => WsValue::UInt(u64::from_le_bytes(buf)),
        other => return Err(unsupported(other, "该定长类型")),
    };
    Ok(value)
}

/// 变长值解码：offset 相对本列数据段起点，值前缀为 u16 字节长度。
fn variable_value(
    kind: u8,
    offset: i32,
    data: &[u8],
    text_budget: &mut usize,
) -> TaosResult<WsValue> {
    if offset == -1 {
        return Ok(WsValue::Null);
    }
    let start = usize::try_from(offset).map_err(|_| malformed("变长 offset 小于 -1"))?;
    let len_end = start
        .checked_add(2)
        .ok_or_else(|| malformed("变长 offset 溢出"))?;
    let prefix = data
        .get(start..len_end)
        .ok_or_else(|| malformed("变长 offset 越过列数据段"))?;
    let len = usize::from(u16::from_le_bytes([prefix[0], prefix[1]]));
    let end = len_end
        .checked_add(len)
        .ok_or_else(|| malformed("变长值长度溢出"))?;
    let bytes = data
        .get(len_end..end)
        .ok_or_else(|| malformed("变长值越过列数据段"))?;
    *text_budget = text_budget
        .checked_sub(len)
        .ok_or_else(|| malformed("变长值累计物化超过 max_response_bytes"))?;
    match kind {
        8 => std::str::from_utf8(bytes)
            .map(|text| WsValue::Text(text.to_owned()))
            // 严格 UTF-8：不做有损替换，避免把损坏数据静默当成正常文本。
            .map_err(|_| malformed("VARCHAR 不是合法 UTF-8")),
        10 => {
            if len % 4 != 0 {
                return Err(malformed("NCHAR 长度不是 4 的倍数（UCS-4）"));
            }
            let mut text = String::with_capacity(len);
            for chunk in bytes.chunks_exact(4) {
                let scalar = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
                text.push(
                    char::from_u32(scalar).ok_or_else(|| malformed("NCHAR 含非法 Unicode 标量"))?,
                );
            }
            Ok(WsValue::Text(text))
        }
        _ => Ok(WsValue::Bytes(bytes.to_vec())),
    }
}

#[cfg(test)]
mod tests {
    //! 解码层测试。合成块均按 03a §4 手工构造（代码内构造，等同 `_synthetic: true`，
    //! 非服务端产物）；真实块回归读取 `tests/fixtures/fetch-block-20261012.bin`
    //! （`_synthetic: false`）。
    use super::*;

    /// 合成列：定长（位图 + 定宽数据）或变长（offsets + 数据段）。
    enum Col {
        Fixed {
            kind: u8,
            width: u32,
            bitmap: Vec<u8>,
            data: Vec<u8>,
        },
        Var {
            kind: u8,
            offsets: Vec<i32>,
            data: Vec<u8>,
        },
    }

    /// 按 03a §4 布局拼装 V3 块：version=1、flag=0x80000000、length=实际总长。
    fn build_block(nrows: u32, cols: &[Col], trailing: &[u8]) -> Vec<u8> {
        let mut info = Vec::new();
        let mut lens = Vec::new();
        let mut body = Vec::new();
        for col in cols {
            match col {
                Col::Fixed {
                    kind,
                    width,
                    bitmap,
                    data,
                } => {
                    info.push(*kind);
                    info.extend_from_slice(&width.to_le_bytes());
                    lens.extend_from_slice(&(data.len() as u32).to_le_bytes());
                    body.extend_from_slice(bitmap);
                    body.extend_from_slice(data);
                }
                Col::Var {
                    kind,
                    offsets,
                    data,
                } => {
                    info.push(*kind);
                    info.extend_from_slice(&64u32.to_le_bytes());
                    lens.extend_from_slice(&(data.len() as u32).to_le_bytes());
                    for offset in offsets {
                        body.extend_from_slice(&offset.to_le_bytes());
                    }
                    body.extend_from_slice(data);
                }
            }
        }
        let total = HEADER_LEN + info.len() + lens.len() + body.len() + trailing.len();
        let mut block = Vec::with_capacity(total);
        block.extend_from_slice(&EXPECTED_VERSION.to_le_bytes());
        block.extend_from_slice(&(total as u32).to_le_bytes());
        block.extend_from_slice(&nrows.to_le_bytes());
        block.extend_from_slice(&(cols.len() as u32).to_le_bytes());
        block.extend_from_slice(&EXPECTED_FLAG.to_le_bytes());
        block.extend_from_slice(&0u64.to_le_bytes());
        block.extend_from_slice(&info);
        block.extend_from_slice(&lens);
        block.extend_from_slice(&body);
        block.extend_from_slice(trailing);
        block
    }

    /// 变长值：u16 字节长度前缀 + 内容。
    fn var_value(bytes: &[u8]) -> Vec<u8> {
        let mut out = (bytes.len() as u16).to_le_bytes().to_vec();
        out.extend_from_slice(bytes);
        out
    }

    fn ucs4(text: &str) -> Vec<u8> {
        text.chars()
            .flat_map(|ch| (ch as u32).to_le_bytes())
            .collect()
    }

    fn decode(block: &[u8], types: &[u8]) -> TaosResult<DecodedBlock> {
        let mut budget = usize::MAX;
        decode_raw_block(block, types, TsPrecision::Ms, &mut budget)
    }

    fn int_col(values: &[i32], bitmap: Vec<u8>) -> Col {
        Col::Fixed {
            kind: 4,
            width: 4,
            bitmap,
            data: values.iter().flat_map(|value| value.to_le_bytes()).collect(),
        }
    }

    fn real_fixture() -> Vec<u8> {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/fetch-block-20261012.bin"
        );
        std::fs::read(path).expect("读取真实 fetch_block 夹具")
    }

    /// 真实块回归：137 字节帧 = 16 字节头 + 121 字节块，块末 1 字节 0x00 尾部。
    #[test]
    fn real_fixture_with_trailing_byte_decodes() {
        let frame = real_fixture();
        assert_eq!(frame.len(), 137);
        assert_eq!(u64::from_le_bytes(frame[8..16].try_into().unwrap()), 1);
        let block = &frame[16..];
        assert_eq!(block.len(), 121);
        assert_eq!(block.last(), Some(&0x00), "尾字节必须存在（不得删改夹具）");

        let decoded = decode(block, &[8, 9, 5, 7, 8]).expect("真实块（含尾字节）必须解码成功");
        assert_eq!(decoded.col_lengths, vec![9, 8, 8, 8, 3]);
        assert_eq!(
            decoded.rows,
            vec![vec![
                WsValue::Text("3.3.6.0".to_owned()),
                WsValue::Timestamp {
                    value: 1_791_777_705_997,
                    precision: TsPrecision::Ms,
                },
                WsValue::Int(1),
                WsValue::Double(1.5),
                WsValue::Text("a".to_owned()),
            ]]
        );
    }

    /// 真实夹具登记 JSON 与解码结果交叉核对（类型、期望行、尾字节）。
    #[test]
    fn real_fixture_matches_registered_json() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/fetch-block-20261012.json"
        );
        let json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(json["_synthetic"], serde_json::Value::Bool(false));
        let types: Vec<u8> = json["query"]["fields_types"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| u8::try_from(value.as_u64().unwrap()).unwrap())
            .collect();
        assert_eq!(types, vec![8, 9, 5, 7, 8]);
        assert_eq!(json["raw_block"]["trailing_bytes_hex"], "00");

        let frame = real_fixture();
        let decoded = decode(&frame[16..], &types).unwrap();
        let expected = &json["expected_rows"][0];
        let row = &decoded.rows[0];
        assert_eq!(row[0], WsValue::Text(expected[0].as_str().unwrap().into()));
        assert_eq!(
            row[1],
            WsValue::Timestamp {
                value: expected[1].as_i64().unwrap(),
                precision: TsPrecision::Ms,
            }
        );
        assert_eq!(row[2], WsValue::Int(expected[2].as_i64().unwrap()));
        assert_eq!(row[3], WsValue::Double(expected[3].as_f64().unwrap()));
        assert_eq!(row[4], WsValue::Text(expected[4].as_str().unwrap().into()));
    }

    /// 合成：列后多出若干尾字节（length 计入）仍解码成功。
    #[test]
    fn trailing_bytes_within_length_are_ignored() {
        let block = build_block(1, &[int_col(&[7], vec![0])], &[0xAB, 0xCD, 0xEF]);
        let decoded = decode(&block, &[4]).expect("尾部字节必须忽略");
        assert_eq!(decoded.rows, vec![vec![WsValue::Int(7)]]);
    }

    /// null 位图 MSB 优先：9 行中第 0、8 行为 NULL → 位图 [0x80, 0x80]。
    #[test]
    fn fixed_null_bitmap_is_msb_first() {
        let values: Vec<i32> = (0..9).collect();
        let block = build_block(9, &[int_col(&values, vec![0x80, 0x80])], &[]);
        let decoded = decode(&block, &[4]).unwrap();
        let column: Vec<WsValue> = decoded
            .rows
            .into_iter()
            .map(|mut row| row.remove(0))
            .collect();
        assert_eq!(column[0], WsValue::Null);
        assert_eq!(column[1], WsValue::Int(1));
        assert_eq!(column[7], WsValue::Int(7));
        assert_eq!(column[8], WsValue::Null);
    }

    /// 变长列 offset=-1 为 NULL；同一 offset 可被多行复用；小于 -1 拒绝。
    #[test]
    fn variable_null_offset_and_shared_offset() {
        let mut data = var_value(b"ab");
        data.extend(var_value(b"xyz"));
        let block = build_block(
            3,
            &[Col::Var {
                kind: 8,
                offsets: vec![0, -1, 0],
                data: data.clone(),
            }],
            &[],
        );
        let decoded = decode(&block, &[8]).unwrap();
        assert_eq!(
            decoded.rows,
            vec![
                vec![WsValue::Text("ab".into())],
                vec![WsValue::Null],
                vec![WsValue::Text("ab".into())],
            ]
        );
        let negative = build_block(
            1,
            &[Col::Var {
                kind: 8,
                offsets: vec![-2],
                data,
            }],
            &[],
        );
        assert!(matches!(
            decode(&negative, &[8]),
            Err(TaosError::Serialization(_))
        ));
    }

    /// NCHAR 内容为 UCS-4（每字符 4 字节），u16 前缀为字节长度。
    #[test]
    fn nchar_is_ucs4() {
        let block = build_block(
            1,
            &[Col::Var {
                kind: 10,
                offsets: vec![0],
                data: var_value(&ucs4("中a😀")),
            }],
            &[],
        );
        let decoded = decode(&block, &[10]).unwrap();
        assert_eq!(decoded.rows, vec![vec![WsValue::Text("中a😀".into())]]);

        let bad_len = build_block(
            1,
            &[Col::Var {
                kind: 10,
                offsets: vec![0],
                data: var_value(&[0x41, 0, 0]),
            }],
            &[],
        );
        assert!(matches!(
            decode(&bad_len, &[10]),
            Err(TaosError::Serialization(_))
        ));
        let bad_scalar = build_block(
            1,
            &[Col::Var {
                kind: 10,
                offsets: vec![0],
                data: var_value(&0xD800u32.to_le_bytes()),
            }],
            &[],
        );
        assert!(matches!(
            decode(&bad_scalar, &[10]),
            Err(TaosError::Serialization(_))
        ));
    }

    /// TIMESTAMP 原值按结果集 precision 0/1/2 标注，不做换算。
    #[test]
    fn timestamp_follows_result_precision() {
        let raw: i64 = 1_791_777_705_997_123;
        let block = build_block(
            1,
            &[Col::Fixed {
                kind: 9,
                width: 8,
                bitmap: vec![0],
                data: raw.to_le_bytes().to_vec(),
            }],
            &[],
        );
        for (code, precision) in [
            (0, TsPrecision::Ms),
            (1, TsPrecision::Us),
            (2, TsPrecision::Ns),
        ] {
            let parsed = precision_from_code(code).unwrap();
            assert_eq!(parsed, precision);
            let mut budget = usize::MAX;
            let decoded = decode_raw_block(&block, &[9], parsed, &mut budget).unwrap();
            assert_eq!(
                decoded.rows,
                vec![vec![WsValue::Timestamp {
                    value: raw,
                    precision,
                }]]
            );
        }
        assert!(matches!(
            precision_from_code(3),
            Err(TaosError::Unavailable(_))
        ));
        assert!(matches!(
            precision_from_code(-1),
            Err(TaosError::Unavailable(_))
        ));
    }

    /// 全部已支持定长 / 字节类型的取值。
    #[test]
    fn supported_types_decode() {
        let fixed = |kind: u8, bytes: Vec<u8>| Col::Fixed {
            kind,
            width: bytes.len() as u32,
            bitmap: vec![0],
            data: bytes,
        };
        let var = |kind: u8, bytes: &[u8]| Col::Var {
            kind,
            offsets: vec![0],
            data: var_value(bytes),
        };
        let cols = [
            fixed(1, vec![1]),
            fixed(2, (-2i8).to_le_bytes().to_vec()),
            fixed(3, (-300i16).to_le_bytes().to_vec()),
            fixed(5, i64::MIN.to_le_bytes().to_vec()),
            fixed(6, 0.25f32.to_le_bytes().to_vec()),
            fixed(7, (-2.5f64).to_le_bytes().to_vec()),
            fixed(11, vec![255]),
            fixed(12, u16::MAX.to_le_bytes().to_vec()),
            fixed(13, u32::MAX.to_le_bytes().to_vec()),
            fixed(14, u64::MAX.to_le_bytes().to_vec()),
            var(15, br#"{"k":1}"#),
            var(16, &[0x00, 0xff]),
            var(20, &[0x01, 0x02, 0x03]),
        ];
        let types = [1, 2, 3, 5, 6, 7, 11, 12, 13, 14, 15, 16, 20];
        let block = build_block(1, &cols, &[]);
        let decoded = decode(&block, &types).unwrap();
        assert_eq!(
            decoded.rows[0],
            vec![
                WsValue::Bool(true),
                WsValue::Int(-2),
                WsValue::Int(-300),
                WsValue::Int(i64::MIN),
                WsValue::Float(0.25),
                WsValue::Double(-2.5),
                WsValue::UInt(255),
                WsValue::UInt(u64::from(u16::MAX)),
                WsValue::UInt(u64::from(u32::MAX)),
                WsValue::UInt(u64::MAX),
                WsValue::Bytes(br#"{"k":1}"#.to_vec()),
                WsValue::Bytes(vec![0x00, 0xff]),
                WsValue::Bytes(vec![0x01, 0x02, 0x03]),
            ]
        );
    }

    #[test]
    fn invalid_bool_and_utf8_fail_closed() {
        let bool_block = build_block(
            1,
            &[Col::Fixed {
                kind: 1,
                width: 1,
                bitmap: vec![0],
                data: vec![2],
            }],
            &[],
        );
        assert!(matches!(
            decode(&bool_block, &[1]),
            Err(TaosError::Serialization(_))
        ));
        let utf8_block = build_block(
            1,
            &[Col::Var {
                kind: 8,
                offsets: vec![0],
                data: var_value(&[0xff, 0xfe]),
            }],
            &[],
        );
        assert!(matches!(
            decode(&utf8_block, &[8]),
            Err(TaosError::Serialization(_))
        ));
    }

    /// version / flag 只接受实测值，不符明确报错。
    #[test]
    fn version_and_flag_must_match_measured_values() {
        let block = build_block(1, &[int_col(&[1], vec![0])], &[]);
        let mut bad_version = block.clone();
        bad_version[0..4].copy_from_slice(&2u32.to_le_bytes());
        let error = decode(&bad_version, &[4]).unwrap_err();
        assert!(matches!(error, TaosError::Serialization(_)));
        assert!(error.to_string().contains("version"), "{error}");

        let mut bad_flag = block;
        bad_flag[16..20].copy_from_slice(&0u32.to_le_bytes());
        let error = decode(&bad_flag, &[4]).unwrap_err();
        assert!(matches!(error, TaosError::Serialization(_)));
        assert!(error.to_string().contains("flag"), "{error}");
    }

    /// length 字段必须等于实际块长（多 / 少均拒绝），短于 28 字节头也拒绝。
    #[test]
    fn length_field_must_equal_block_len() {
        let block = build_block(1, &[int_col(&[1], vec![0])], &[]);
        let mut longer = block.clone();
        longer.push(0);
        let error = decode(&longer, &[4]).unwrap_err();
        assert!(error.to_string().contains("length"), "{error}");
        let error = decode(&block[..block.len() - 1], &[4]).unwrap_err();
        assert!(error.to_string().contains("length"), "{error}");
        assert!(matches!(
            decode(&block[..20], &[4]),
            Err(TaosError::Serialization(_))
        ));
    }

    /// 列长声明越过块长 → 列越界报错；变长前缀越界、定长列长与 nrows×宽度不符也报错。
    #[test]
    fn column_bounds_are_enforced() {
        let block = build_block(
            1,
            &[Col::Var {
                kind: 8,
                offsets: vec![0],
                data: var_value(b"abc"),
            }],
            &[],
        );
        // 列长表位于 28 + 5×1 起；把唯一一列列长改大到越过块长。
        let col_len_at = HEADER_LEN + COL_INFO_LEN;
        let mut overflow = block.clone();
        overflow[col_len_at..col_len_at + 4].copy_from_slice(&1000u32.to_le_bytes());
        let error = decode(&overflow, &[8]).unwrap_err();
        assert!(error.to_string().contains("越过块长"), "{error}");

        // 变长值长度前缀越过本列数据段（数据段起于列长表 + 1 个 offset 之后）。
        let mut bad_prefix = block;
        let data_at = col_len_at + 4 + 4;
        bad_prefix[data_at..data_at + 2].copy_from_slice(&40u16.to_le_bytes());
        let error = decode(&bad_prefix, &[8]).unwrap_err();
        assert!(error.to_string().contains("列数据段"), "{error}");

        let fixed = build_block(2, &[int_col(&[1], vec![0])], &[]);
        let error = decode(&fixed, &[4]).unwrap_err();
        assert!(error.to_string().contains("nrows×宽度"), "{error}");
    }

    #[test]
    fn column_type_and_count_must_match_query() {
        let block = build_block(1, &[int_col(&[1], vec![0])], &[]);
        assert!(matches!(
            decode(&block, &[5]),
            Err(TaosError::Serialization(_))
        ));
        assert!(matches!(
            decode(&block, &[4, 4]),
            Err(TaosError::Serialization(_))
        ));
    }

    /// DECIMAL / DECIMAL64 / BLOB / MEDIUMBLOB 以及未定义布局的类型码明确返回不支持。
    #[test]
    fn unsupported_types_are_rejected() {
        for kind in [17u8, 21, 18, 19, 0, 22] {
            let block = build_block(
                1,
                &[Col::Fixed {
                    kind,
                    width: 8,
                    bitmap: vec![0],
                    data: vec![0; 8],
                }],
                &[],
            );
            let error = decode(&block, &[kind]).unwrap_err();
            assert!(
                matches!(error, TaosError::Unsupported(_)),
                "类型码 {kind}: {error:?}"
            );
        }
    }

    /// 重复 offset 引用按次扣减物化预算，超限报错。
    #[test]
    fn text_budget_is_charged_per_reference() {
        let block = build_block(
            3,
            &[Col::Var {
                kind: 8,
                offsets: vec![0, 0, 0],
                data: var_value(b"abcd"),
            }],
            &[],
        );
        let mut budget = 8;
        let error = decode_raw_block(&block, &[8], TsPrecision::Ms, &mut budget).unwrap_err();
        assert!(error.to_string().contains("max_response_bytes"), "{error}");
        let mut enough = 12;
        decode_raw_block(&block, &[8], TsPrecision::Ms, &mut enough).unwrap();
        assert_eq!(enough, 0);
    }
}
