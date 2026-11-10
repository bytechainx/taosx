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

    /// 每单位纳秒数。
    #[must_use]
    pub const fn nanos_per_unit(self) -> i64 {
        match self {
            Self::Ns => 1,
            Self::Us => 1_000,
            Self::Ms => 1_000_000,
        }
    }
}

/// 原生闭区间网格换算结果。
///
/// 对应 `specs/repos/type/temporal/spec.md` §30.2：目标 `[s, e)` 映射为
/// 包含式原生网格端点 `native_start = ceil_div(s, δ)`、
/// `native_end = ceil_div(e, δ) - 1`。`native_end < native_start` 表示该网格
/// 没有命中点，调用方应返回空请求结果，不得发出起止颠倒的请求。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GridWindow {
    /// 包含式原生起点网格序号。
    pub native_start: i64,
    /// 包含式原生终点网格序号。
    pub native_end: i64,
}

impl GridWindow {
    /// 该网格是否没有命中点。
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.native_end < self.native_start
    }
}

/// 计算 `ceil(a / δ)`，`δ > 0`。
///
/// 使用欧几里得除法，对负值同样正确（Rust `/` 向零截断）；不溢出。
fn ceil_div(a: i64, delta: i64) -> i64 {
    let quotient = a.div_euclid(delta);
    if a.rem_euclid(delta) == 0 {
        quotient
    } else {
        quotient + 1
    }
}

/// 把目标半开纳秒区间 `[start, end)` 映射为包含式原生网格端点。
///
/// `delta` 为原生网格步长（纳秒），必须为正。返回 `None` 表示参数非法
/// （`delta <= 0` 或 `end <= start`）。运算全程用宽整数，不溢出。
#[must_use]
pub fn to_native_grid(start: i64, end: i64, delta: i64) -> Option<GridWindow> {
    if delta <= 0 || end <= start {
        return None;
    }
    let native_start = ceil_div(start, delta);
    let native_end = ceil_div(end, delta) - 1;
    Some(GridWindow {
        native_start,
        native_end,
    })
}

#[cfg(test)]
mod grid_tests {
    use super::{ceil_div, to_native_grid, TsPrecision};

    #[test]
    fn ceil_div_is_exact_for_aligned_values() {
        let delta = TsPrecision::Ms.nanos_per_unit();
        assert_eq!(ceil_div(0, delta), 0);
        assert_eq!(ceil_div(1_500_000_000, delta), 1_500);
        assert_eq!(ceil_div(-1_500_000_000, delta), -1_500);
    }

    #[test]
    fn ceil_div_rounds_toward_positive_infinity() {
        let delta = TsPrecision::Ms.nanos_per_unit();
        assert_eq!(ceil_div(1_500_000_001, delta), 1_501);
        assert_eq!(ceil_div(-1_500_000_001, delta), -1_500);
    }

    #[test]
    fn ceil_div_handles_extremes_without_overflow() {
        let delta = TsPrecision::Ms.nanos_per_unit();
        assert_eq!(ceil_div(i64::MAX, delta), i64::MAX.div_euclid(delta) + 1);
        assert_eq!(ceil_div(i64::MIN, delta), i64::MIN.div_euclid(delta) + 1);
    }

    #[test]
    fn grid_window_maps_half_open_to_inclusive() {
        let delta = TsPrecision::Ms.nanos_per_unit();
        // [0, 1ms) → 网格点 0。
        let window = to_native_grid(0, delta, delta).expect("合法区间");
        assert_eq!(window.native_start, 0);
        assert_eq!(window.native_end, 0);
        assert!(!window.is_empty());
        // [1ms, 3ms) → 网格点 1..=2。
        let window = to_native_grid(delta, 3 * delta, delta).expect("合法区间");
        assert_eq!(window.native_start, 1);
        assert_eq!(window.native_end, 2);
        // 非整起点向上取整：[1, 2ms) → 1..=1。
        let window = to_native_grid(1, 2 * delta, delta).expect("合法区间");
        assert_eq!(window.native_start, 1);
        assert_eq!(window.native_end, 1);
    }

    #[test]
    fn grid_window_reports_empty_when_no_point_fits() {
        let delta = TsPrecision::Ms.nanos_per_unit();
        // 区间长度小于一个网格步长且不跨网格点 → 无命中点。
        let window = to_native_grid(1, 2, delta).expect("合法区间");
        assert!(window.is_empty());
        assert!(window.native_end < window.native_start);
    }

    #[test]
    fn grid_window_handles_negative_ranges() {
        let delta = TsPrecision::Ms.nanos_per_unit();
        let window = to_native_grid(-2 * delta, -delta, delta).expect("合法区间");
        assert_eq!(window.native_start, -2);
        assert_eq!(window.native_end, -2);
        assert!(!window.is_empty());
    }

    #[test]
    fn grid_window_rejects_invalid_arguments() {
        assert!(to_native_grid(0, 1, 0).is_none(), "δ 必须为正");
        assert!(to_native_grid(0, 1, -1).is_none(), "δ 必须为正");
        assert!(to_native_grid(1, 1, 1).is_none(), "空区间拒绝");
        assert!(to_native_grid(2, 1, 1).is_none(), "起止颠倒拒绝");
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
