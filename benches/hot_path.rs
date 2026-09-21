#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! taosx 热路径：`TaosConfig` 构建校验 + `build_insert_sql_chunks` 分块 SQL 构造。
//!
//! 纯本地路径，不发起任何网络请求、不依赖 TDengine 服务。
use std::hint::black_box;
use std::time::Instant;

use taosx::{build_insert_sql_chunks, TaosConfig, TaosPoint, TsPrecision};

fn iters() -> u32 {
    if std::env::args().any(|a| a == "--quick") {
        1_000
    } else {
        50_000
    }
}

fn sample_points() -> Vec<TaosPoint> {
    (0..64)
        .map(|i| {
            TaosPoint::new(
                "BTC/USDT",
                1_700_000_000_000_000_000 + i,
                "66522.40",
                "66523.10",
            )
        })
        .collect()
}

fn main() {
    let n = iters();
    let points = sample_points();

    // 预热：配置构建 + 校验。
    for _ in 0..n.min(1_000) {
        let config = TaosConfig::builder()
            .host("127.0.0.1")
            .database("ticks")
            .build()
            .unwrap();
        config.validate().unwrap();
        black_box(&config);
    }
    // 预热：分块 SQL 构造。
    for _ in 0..n.min(1_000) {
        let chunks = build_insert_sql_chunks("ticks", &points, TsPrecision::Ns, 32).unwrap();
        black_box(&chunks);
    }

    let start = Instant::now();
    for i in 0..n {
        let config = TaosConfig::builder()
            .host("127.0.0.1")
            .database("ticks")
            .build()
            .unwrap();
        config.validate().unwrap();
        let chunks = build_insert_sql_chunks("ticks", &points, TsPrecision::Ns, 32).unwrap();
        black_box((i, &config, &chunks));
    }
    let elapsed = start.elapsed();
    println!(
        "bench_taosx_hot_path: iters={n} total={elapsed:?} per_iter={:?}",
        elapsed / n
    );
}
