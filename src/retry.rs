//! 本地重试策略：指数退避 + 抖动 + 可选 deadline。
//!
//! 只重试 [`TaosError::is_retryable`] 为真的错误（连接失败、远端暂不可用、超时、
//! I/O）。**非幂等写默认不重试**；需要重试写路径时使用
//! [`RetryPolicy::for_idempotent_write`]，并由调用方保证幂等键/时间戳唯一。

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::time::sleep;

use crate::error::{TaosError, TaosResult};

/// 抖动状态（进程级；并发竞争只影响抖动样本，不影响正确性）。
static JITTER_STATE: AtomicU64 = AtomicU64::new(0);

/// 重试策略。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RetryPolicy {
    /// 最大尝试次数（含首次；≥1）。
    pub max_attempts: u32,
    /// 初始退避。
    pub initial_backoff: Duration,
    /// 最大退避（抖动后仍不超过该值）。
    pub max_backoff: Duration,
    /// 抖动比例，取值 `0.0..=1.0`：退避在 `[1-r, 1+r]` 区间内均匀抖动。
    pub jitter_ratio: f64,
    /// 重试总时长上限；`None` 表示只受 `max_attempts` 约束。
    pub deadline: Option<Duration>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(2),
            jitter_ratio: 0.2,
            deadline: None,
        }
    }
}

impl RetryPolicy {
    /// 读路径默认：最多 3 次，deadline 5 秒。
    #[must_use]
    pub fn for_read() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(1),
            jitter_ratio: 0.2,
            deadline: Some(Duration::from_secs(5)),
        }
    }

    /// 幂等写路径：最多 3 次，deadline 30 秒。
    #[must_use]
    pub fn for_idempotent_write() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(2),
            jitter_ratio: 0.2,
            deadline: Some(Duration::from_secs(30)),
        }
    }

    /// 关闭所有重试（等价于 `max_attempts = 1`）。
    #[must_use]
    pub fn no_retry() -> Self {
        Self::default()
    }

    /// 是否可重试（委托 [`TaosError::is_retryable`]）。
    #[must_use]
    pub fn is_retryable(error: &TaosError) -> bool {
        error.is_retryable()
    }

    /// 纯函数：第 `attempt` 次（从 0 计）重试前的指数退避，**追加抖动**。
    ///
    /// `unit_sample` 为 `[0, 1)` 上的均匀样本；`1.0` 及以上的取值按 `1.0` 处理。
    /// 返回值不超过 [`Self::max_backoff`]。
    #[must_use]
    pub fn compute_backoff(&self, attempt: u32, unit_sample: f64) -> Duration {
        let exponential = 2u32.checked_pow(attempt).unwrap_or(u32::MAX);
        let base = self
            .initial_backoff
            .checked_mul(exponential)
            .unwrap_or(self.max_backoff)
            .min(self.max_backoff);
        let ratio = self.jitter_ratio.clamp(0.0, 1.0);
        let sample = unit_sample.clamp(0.0, 1.0);
        let factor = 1.0 - ratio + 2.0 * ratio * sample;
        let nanos = (base.as_nanos() as f64 * factor).max(0.0);
        Duration::from_nanos(nanos as u64).min(self.max_backoff)
    }

    /// 纯函数：仅指数退避（不含抖动），供诊断与文档使用。
    #[must_use]
    pub fn exponential_backoff(&self, attempt: u32) -> Duration {
        self.compute_backoff(attempt, 0.5).min(self.max_backoff)
    }

    /// 第 `attempt` 次重试前的退避（使用进程级抖动样本）。
    #[must_use]
    pub fn backoff_for_attempt(&self, attempt: u32) -> Duration {
        self.compute_backoff(attempt, next_unit_sample())
    }

    /// 在策略下执行异步操作。
    ///
    /// 不可重试的错误立即返回；可重试错误在 `max_attempts` 与 `deadline` 内退避重试，
    /// deadline 耗尽时返回 [`TaosError::Timeout`]。
    pub async fn run<T, F, Fut>(&self, mut op: F) -> TaosResult<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = TaosResult<T>>,
    {
        let attempts = self.max_attempts.max(1);
        let started = Instant::now();
        let mut last: Option<TaosError> = None;
        for attempt in 0..attempts {
            match op().await {
                Ok(value) => return Ok(value),
                Err(error) => {
                    if !error.is_retryable() || attempt + 1 >= attempts {
                        return Err(error);
                    }
                    let delay = self.backoff_for_attempt(attempt);
                    if let Some(deadline) = self.deadline {
                        if started.elapsed().saturating_add(delay) >= deadline {
                            return Err(TaosError::Timeout(format!(
                                "重试 deadline 耗尽（已尝试 {} 次）: {error}",
                                attempt + 1
                            )));
                        }
                    }
                    last = Some(error);
                    sleep(delay).await;
                }
            }
        }
        // 逻辑不可达：attempts ≥ 1 时末次迭代必在循环内 return
        // （is_retryable=false 或 attempt+1>=attempts 分支）；此 fallback
        // 仅满足编译器的控制流分析。
        Err(last.unwrap_or_else(|| TaosError::Timeout("重试策略未执行任何尝试".to_owned())))
    }
}

/// 取 `[0, 1)` 上的均匀样本（xorshift64*；无需引入随机数依赖）。
fn next_unit_sample() -> f64 {
    let mut state = JITTER_STATE.load(Ordering::Relaxed);
    if state == 0 {
        state = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos() as u64)
            .unwrap_or(0x9E37_79B9_7F4A_7C15)
            | 1;
    }
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    JITTER_STATE.store(state, Ordering::Relaxed);
    (state >> 11) as f64 / (1u64 << 53) as f64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    #[test]
    fn backoff_is_exponential_and_capped() {
        let policy = RetryPolicy {
            max_attempts: 8,
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(400),
            jitter_ratio: 0.0,
            deadline: None,
        };
        assert_eq!(policy.exponential_backoff(0), Duration::from_millis(50));
        assert_eq!(policy.exponential_backoff(1), Duration::from_millis(100));
        assert_eq!(policy.exponential_backoff(2), Duration::from_millis(200));
        assert_eq!(policy.exponential_backoff(3), Duration::from_millis(400));
        assert_eq!(
            policy.exponential_backoff(9),
            Duration::from_millis(400),
            "必须被 max_backoff 截断"
        );
    }

    #[test]
    fn jitter_stays_within_ratio_and_cap() {
        let policy = RetryPolicy {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(100),
            jitter_ratio: 0.5,
            ..RetryPolicy::default()
        };
        assert_eq!(policy.compute_backoff(0, 0.0), Duration::from_millis(50));
        assert_eq!(policy.compute_backoff(0, 0.5), Duration::from_millis(100));
        assert_eq!(
            policy.compute_backoff(0, 1.0),
            Duration::from_millis(100),
            "抖动后仍受上限约束"
        );

        let no_jitter = RetryPolicy {
            jitter_ratio: 0.0,
            ..policy
        };
        assert_eq!(
            no_jitter.compute_backoff(0, 0.9),
            Duration::from_millis(100)
        );
    }

    #[test]
    fn unit_sample_is_in_range() {
        for _ in 0..64 {
            let sample = next_unit_sample();
            assert!((0.0..1.0).contains(&sample), "样本越界: {sample}");
        }
    }

    #[tokio::test]
    async fn retries_transient_then_ok() {
        let calls = AtomicU32::new(0);
        let policy = RetryPolicy {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
            jitter_ratio: 0.0,
            deadline: None,
        };
        let value = policy
            .run(|| async {
                let attempt = calls.fetch_add(1, Ordering::SeqCst);
                if attempt < 2 {
                    Err(TaosError::Unavailable("temp".into()))
                } else {
                    Ok(42)
                }
            })
            .await
            .expect("第 3 次必须成功");
        assert_eq!(value, 42);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn no_retry_on_permanent_error() {
        let calls = AtomicU32::new(0);
        let error = RetryPolicy::for_read()
            .run(|| async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(TaosError::Invalid("bad".into()))
            })
            .await
            .expect_err("不可重试错误必须原样返回");
        assert!(matches!(error, TaosError::Invalid(_)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn deadline_exhaustion_reports_timeout() {
        let policy = RetryPolicy {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(50),
            jitter_ratio: 0.0,
            deadline: Some(Duration::from_millis(1)),
        };
        let error = policy
            .run(|| async { Err::<(), _>(TaosError::Unavailable("down".into())) })
            .await
            .expect_err("deadline 必须触发");
        assert!(matches!(error, TaosError::Timeout(_)), "实际: {error:?}");
    }

    #[test]
    fn retryable_delegates_to_error() {
        assert!(RetryPolicy::is_retryable(&TaosError::Connection(
            "x".into()
        )));
        assert!(!RetryPolicy::is_retryable(&TaosError::Config("x".into())));
    }
}
