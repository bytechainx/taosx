//! 精度与传输模式枚举。
//!
//! 自 `src/config.rs` 下沉而来：`TsPrecision`（时间戳精度）与 `TransportMode`
//! （REST / 原生 WebSocket）。二者经门面 `pub use` 导出，公开路径与 crate 内部路径
//! （`crate::config::{TsPrecision, TransportMode}`）均不变；`config` 内其它子模块按
//! `super::{…}` 引用。

/// 时间戳精度（库级；`TaosPoint::timestamp_ns` 始终为纳秒，写入前按精度换算）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TsPrecision {
    /// 毫秒（TDengine 默认）。
    #[default]
    Ms,
    /// 微秒。
    Us,
    /// 纳秒。
    Ns,
}

impl TsPrecision {
    /// 从 TDengine 返回值解析（`ms` / `us` / `ns`，大小写不敏感）。
    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        match text.trim().to_ascii_lowercase().as_str() {
            "ms" => Some(Self::Ms),
            "us" => Some(Self::Us),
            "ns" => Some(Self::Ns),
            _ => None,
        }
    }

    /// 精度名（小写，与 TDengine `PRECISION` 取值一致）。
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ms => "ms",
            Self::Us => "us",
            Self::Ns => "ns",
        }
    }

    /// 纳秒 → 库时间戳数值（**显式向 0 截断**）。
    ///
    /// 本方法不做精度损失检查；需要拒绝静默截断的写入路径请使用
    /// [`crate::build_insert_sql_chunks`]，它会对未对齐的时间戳 fail-closed。
    #[must_use]
    pub const fn from_nanos(self, timestamp_ns: i64) -> i64 {
        match self {
            Self::Ns => timestamp_ns,
            Self::Us => timestamp_ns / 1_000,
            Self::Ms => timestamp_ns / 1_000_000,
        }
    }

    /// 库时间戳数值 → 纳秒（饱和运算，不 panic）。
    #[must_use]
    pub const fn to_nanos(self, timestamp: i64) -> i64 {
        match self {
            Self::Ns => timestamp,
            Self::Us => timestamp.saturating_mul(1_000),
            Self::Ms => timestamp.saturating_mul(1_000_000),
        }
    }
}

/// 传输模式：REST（默认）或原生 WebSocket。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TransportMode {
    /// HTTP REST（`/rest/sql`，端口 6041）。
    #[default]
    Rest,
    /// 原生 WebSocket（`/rest/ws`）。
    NativeWs,
}

impl TransportMode {
    /// 从字符串解析（`rest` / `http` / `native` / `ws` / `nativews` / `native_ws` / `native-ws`）。
    ///
    /// 接受集**包含 [`Self::as_str`] 的输出** ⇒ 两个变体都满足 `parse(x.as_str()) == Some(x)`。
    /// （大小写与首尾空白不敏感。）
    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        match text.trim().to_ascii_lowercase().as_str() {
            "rest" | "http" => Some(Self::Rest),
            "native" | "ws" | "nativews" | "native_ws" | "native-ws" => Some(Self::NativeWs),
            _ => None,
        }
    }

    /// 模式名（`rest` / `nativews`）。
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rest => "rest",
            Self::NativeWs => "nativews",
        }
    }
}
