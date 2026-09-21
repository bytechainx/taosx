#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! 配置校验（硬上限 / 非法精度 / 非法标识符）+ 环境变量 + 密码脱敏。

use std::time::Duration;

use taosx::{
    build_insert_sql_chunks, TaosConfig, TaosError, TransportMode, TsPrecision,
    ENV_ACQUIRE_TIMEOUT_MS, ENV_BATCH_MAX_BYTES, ENV_BATCH_MAX_ROWS, ENV_CLOSE_TIMEOUT_MS,
    ENV_DATABASE, ENV_HOST, ENV_HOSTS, ENV_MAX_IN_FLIGHT, ENV_MAX_QUERY_ROWS,
    ENV_MAX_RESPONSE_BYTES, ENV_PASSWORD, ENV_PORT, ENV_PRECISION, ENV_PREFIX, ENV_TIMEOUT_MS,
    ENV_TRANSPORT, ENV_USER, HARD_MAX_BATCH_BYTES, HARD_MAX_BATCH_ROWS, HARD_MAX_CLOSE_TIMEOUT,
    HARD_MAX_IN_FLIGHT, HARD_MAX_QUERY_ROWS, HARD_MAX_RESPONSE_BYTES,
};

/// 清理本文件用到的全部环境变量。
fn clear_env() {
    for name in [
        ENV_HOST,
        ENV_PORT,
        ENV_DATABASE,
        ENV_USER,
        ENV_PASSWORD,
        ENV_TIMEOUT_MS,
        ENV_PRECISION,
        ENV_TRANSPORT,
        ENV_MAX_IN_FLIGHT,
        ENV_ACQUIRE_TIMEOUT_MS,
        ENV_BATCH_MAX_ROWS,
        ENV_BATCH_MAX_BYTES,
        ENV_MAX_RESPONSE_BYTES,
        ENV_MAX_QUERY_ROWS,
        ENV_CLOSE_TIMEOUT_MS,
        ENV_HOSTS,
    ] {
        std::env::remove_var(name);
    }
}

#[test]
fn env_prefix_and_names_are_stable() {
    assert_eq!(ENV_PREFIX, "FOUNDATIONX_TAOSX_");
    for name in [
        ENV_HOST,
        ENV_PORT,
        ENV_PASSWORD,
        ENV_PRECISION,
        ENV_TRANSPORT,
    ] {
        assert!(name.starts_with(ENV_PREFIX), "{name} 必须使用统一前缀");
    }
}

#[test]
fn hard_limits_fail_closed() {
    let rejected = [
        TaosConfig {
            max_in_flight: 0,
            ..TaosConfig::default()
        },
        TaosConfig {
            max_in_flight: HARD_MAX_IN_FLIGHT + 1,
            ..TaosConfig::default()
        },
        TaosConfig {
            batch_max_rows: 0,
            ..TaosConfig::default()
        },
        TaosConfig {
            batch_max_rows: HARD_MAX_BATCH_ROWS + 1,
            ..TaosConfig::default()
        },
        TaosConfig {
            batch_max_bytes: 0,
            ..TaosConfig::default()
        },
        TaosConfig {
            batch_max_bytes: HARD_MAX_BATCH_BYTES + 1,
            ..TaosConfig::default()
        },
        TaosConfig {
            max_response_bytes: 0,
            ..TaosConfig::default()
        },
        TaosConfig {
            max_response_bytes: HARD_MAX_RESPONSE_BYTES + 1,
            ..TaosConfig::default()
        },
        TaosConfig {
            max_query_rows: HARD_MAX_QUERY_ROWS + 1,
            ..TaosConfig::default()
        },
        TaosConfig {
            close_timeout: Duration::ZERO,
            ..TaosConfig::default()
        },
        TaosConfig {
            close_timeout: HARD_MAX_CLOSE_TIMEOUT + Duration::from_secs(1),
            ..TaosConfig::default()
        },
        TaosConfig {
            timeout: Duration::ZERO,
            ..TaosConfig::default()
        },
        TaosConfig {
            acquire_timeout: Duration::ZERO,
            ..TaosConfig::default()
        },
        TaosConfig {
            port: 0,
            ..TaosConfig::default()
        },
        TaosConfig {
            user: "  ".to_owned(),
            ..TaosConfig::default()
        },
        TaosConfig {
            database: "bad-name".to_owned(),
            ..TaosConfig::default()
        },
        TaosConfig {
            host: "td.example".to_owned(),
            ..TaosConfig::default()
        },
        TaosConfig {
            hosts: vec!["bad host".to_owned()],
            ..TaosConfig::default()
        },
    ];
    for config in rejected {
        let error = config.validate().expect_err("必须被拒绝");
        assert!(matches!(error, TaosError::Config(_)), "{error:?}");
        assert!(!error.is_retryable(), "配置错误不可重试");
    }

    // 边界值必须被接受。
    for config in [
        TaosConfig {
            max_in_flight: 1,
            ..TaosConfig::default()
        },
        TaosConfig {
            max_in_flight: HARD_MAX_IN_FLIGHT,
            ..TaosConfig::default()
        },
        TaosConfig {
            batch_max_rows: HARD_MAX_BATCH_ROWS,
            ..TaosConfig::default()
        },
        TaosConfig {
            batch_max_bytes: HARD_MAX_BATCH_BYTES,
            ..TaosConfig::default()
        },
        TaosConfig {
            max_response_bytes: HARD_MAX_RESPONSE_BYTES,
            ..TaosConfig::default()
        },
        TaosConfig {
            max_query_rows: HARD_MAX_QUERY_ROWS,
            ..TaosConfig::default()
        },
        TaosConfig {
            close_timeout: HARD_MAX_CLOSE_TIMEOUT,
            ..TaosConfig::default()
        },
        TaosConfig {
            database: String::new(),
            ..TaosConfig::default()
        },
    ] {
        config.validate().expect("边界值必须通过");
    }
}

#[test]
fn remote_requires_tls_and_password() {
    let remote = TaosConfig {
        host: "td.internal".to_owned(),
        ..TaosConfig::default()
    };
    assert!(remote.validate().is_err(), "远程明文必须拒绝");

    let tls_without_password = TaosConfig {
        host: "td.internal".to_owned(),
        tls: true,
        ..TaosConfig::default()
    };
    assert!(
        tls_without_password.validate().is_err(),
        "远程无密码必须拒绝"
    );

    let blank_password = TaosConfig {
        host: "td.internal".to_owned(),
        tls: true,
        password: "   ".to_owned(),
        ..TaosConfig::default()
    };
    assert!(blank_password.validate().is_err(), "空白密码必须拒绝");

    let secure = TaosConfig {
        host: "td.internal".to_owned(),
        tls: true,
        password: "configured".to_owned(),
        ..TaosConfig::default()
    };
    secure.validate().expect("远程 TLS + 密码必须通过");
}

#[test]
fn invalid_precision_is_rejected_from_toml() {
    let toml = "schema_version = 1\nhost = \"127.0.0.1\"\nprecision = \"bogus\"\n";
    let error = TaosConfig::from_toml(toml).expect_err("非法精度必须拒绝");
    assert!(error.to_string().contains("precision"), "{error}");

    let bad_transport = "schema_version = 1\nhost = \"127.0.0.1\"\ntransport = \"grpc\"\n";
    let error = TaosConfig::from_toml(bad_transport).expect_err("非法传输必须拒绝");
    assert!(error.to_string().contains("transport"), "{error}");
}

#[test]
fn illegal_table_names_are_rejected_before_sql_is_built() {
    let points = [taosx::TaosPoint::new("BTC", 1, "0.1", "0.2")];
    let too_long = "a".repeat(95);
    for table in [
        "",
        "ticks; DROP DATABASE x",
        "ticks`",
        "1ticks",
        "ticks-btc",
        "ticks btc",
        "ticks'",
        "ticks/btc",
        "ticks\nDROP",
        too_long.as_str(),
    ] {
        let error = build_insert_sql_chunks(table, &points, TsPrecision::Ns, 1)
            .expect_err("非法表名必须拒绝");
        assert!(
            matches!(error, TaosError::Invalid(_)),
            "table={table:?} -> {error:?}"
        );
        assert!(
            !error.to_string().contains("DROP DATABASE"),
            "错误不得回显注入串"
        );
    }
    build_insert_sql_chunks("ticks_v1", &points, TsPrecision::Ns, 1).expect("合法表名必须通过");
}

#[test]
fn password_is_redacted_in_debug_and_never_parsed_from_toml() {
    let config = TaosConfig::builder()
        .password("s3cret-value-77")
        .build()
        .expect("配置有效");
    let rendered = format!("{config:?}");
    assert!(rendered.contains("***"), "{rendered}");
    assert!(
        !rendered.contains("s3cret-value-77"),
        "明文密码不得出现在 Debug: {rendered}"
    );

    let error = TaosConfig::from_toml("schema_version = 1\npassword = \"hunter2\"\n")
        .expect_err("TOML 非空密码必须拒绝");
    assert!(error.to_string().contains("password"));
    assert!(!error.to_string().contains("hunter2"), "错误不得回显密码");

    let placeholder =
        TaosConfig::from_toml("schema_version = 1\npassword = \"\"\n").expect("空占位必须允许");
    assert!(placeholder.password.is_empty());
}

#[test]
fn toml_parses_supported_fields_and_rejects_unknown() {
    let config = TaosConfig::from_toml(
        r#"
schema_version = 1
host = "127.0.0.1"
port = 6041
database = "macro_data"
user = "writer"
tls = false
timeout_ms = 15000
acquire_timeout_ms = 2500
close_timeout_ms = 1000
precision = "ns"
transport = "native"
max_in_flight = 32
batch_max_rows = 128
batch_max_bytes = 65536
max_response_bytes = 1048576
max_query_rows = 512
hosts = ["127.0.0.2", "127.0.0.3"]
write_max_attempts = 3
"#,
    )
    .expect("TOML 解析必须成功");
    assert_eq!(config.database, "macro_data");
    assert_eq!(config.timeout, Duration::from_millis(15000));
    assert_eq!(config.acquire_timeout, Duration::from_millis(2500));
    assert_eq!(config.close_timeout, Duration::from_millis(1000));
    assert_eq!(config.precision, Some(TsPrecision::Ns));
    assert_eq!(config.transport, TransportMode::NativeWs);
    assert_eq!(config.max_in_flight, 32);
    assert_eq!(config.batch_max_rows, 128);
    assert_eq!(config.batch_max_bytes, 65536);
    assert_eq!(config.max_response_bytes, 1_048_576);
    assert_eq!(config.max_query_rows, 512);
    assert_eq!(config.hosts.len(), 2);
    assert_eq!(config.write_max_attempts, 3);
    assert_eq!(config.endpoint_hosts().len(), 3);
    assert!(config.password.is_empty());

    assert!(TaosConfig::from_toml("schema_version = 1\nsink_id = \"x\"\n").is_err());
    assert!(
        TaosConfig::from_toml("host = \"127.0.0.1\"\n").is_err(),
        "缺少 schema_version"
    );
}

/// 环境变量用例集中在**一个** `#[test]` 中：`std::env` 是进程级共享状态，
/// 拆成多个并行测试会互相干扰。
#[test]
fn env_variables_are_honored_and_fail_closed() {
    clear_env();
    std::env::set_var(ENV_HOST, "127.0.0.1");
    std::env::set_var(ENV_PORT, "6042");
    std::env::set_var(ENV_DATABASE, "env_database");
    std::env::set_var(ENV_USER, "env_user");
    std::env::set_var(ENV_PASSWORD, "env-secret");
    std::env::set_var(ENV_TIMEOUT_MS, "1234");
    std::env::set_var(ENV_PRECISION, "us");
    std::env::set_var(ENV_TRANSPORT, "ws");
    std::env::set_var(ENV_MAX_IN_FLIGHT, "8");
    std::env::set_var(ENV_ACQUIRE_TIMEOUT_MS, "321");
    std::env::set_var(ENV_BATCH_MAX_ROWS, "77");
    std::env::set_var(ENV_BATCH_MAX_BYTES, "4096");
    std::env::set_var(ENV_MAX_RESPONSE_BYTES, "8192");
    std::env::set_var(ENV_MAX_QUERY_ROWS, "99");
    std::env::set_var(ENV_CLOSE_TIMEOUT_MS, "222");
    std::env::set_var(ENV_HOSTS, "127.0.0.3, 127.0.0.4");

    let config = TaosConfig::from_env().expect("环境变量加载必须成功");

    assert_eq!(config.host, "127.0.0.1");
    assert_eq!(config.port, 6042);
    assert_eq!(config.database, "env_database");
    assert_eq!(config.user, "env_user");
    assert_eq!(config.password, "env-secret");
    assert_eq!(config.timeout, Duration::from_millis(1234));
    assert_eq!(config.precision, Some(TsPrecision::Us));
    assert_eq!(config.transport, TransportMode::NativeWs);
    assert_eq!(config.max_in_flight, 8);
    assert_eq!(config.acquire_timeout, Duration::from_millis(321));
    assert_eq!(config.batch_max_rows, 77);
    assert_eq!(config.batch_max_bytes, 4096);
    assert_eq!(config.max_response_bytes, 8192);
    assert_eq!(config.max_query_rows, 99);
    assert_eq!(config.close_timeout, Duration::from_millis(222));
    assert_eq!(
        config.hosts,
        vec!["127.0.0.3".to_owned(), "127.0.0.4".to_owned()]
    );
    assert!(
        !format!("{config:?}").contains("env-secret"),
        "Debug 必须脱敏"
    );

    // 非法精度：只报告变量名，不回显取值。
    std::env::set_var(ENV_PRECISION, "fortnight");
    let error = TaosConfig::from_env().expect_err("非法环境精度必须拒绝");
    assert!(error.to_string().contains(ENV_PRECISION), "{error}");
    assert!(!error.to_string().contains("fortnight"), "错误不得回显取值");
    std::env::set_var(ENV_PRECISION, "us");

    // 越界硬上限：fail-closed。
    std::env::set_var(ENV_MAX_IN_FLIGHT, (HARD_MAX_IN_FLIGHT + 1).to_string());
    let error = TaosConfig::from_env().expect_err("越界环境变量必须拒绝");
    assert!(error.to_string().contains("max_in_flight"), "{error}");

    clear_env();
}

#[test]
fn builder_covers_every_tunable_and_validates() {
    let config = TaosConfig::builder()
        .host("127.0.0.1")
        .port(6041)
        .database("env_database")
        .user("writer")
        .password("p")
        .tls(false)
        .timeout(Duration::from_millis(500))
        .precision(TsPrecision::Us)
        .transport(TransportMode::Rest)
        .max_in_flight(4)
        .acquire_timeout(Duration::from_millis(100))
        .batch_max_rows(10)
        .batch_max_bytes(2048)
        .max_response_bytes(4096)
        .max_query_rows(50)
        .close_timeout(Duration::from_millis(100))
        .hosts(["127.0.0.5"])
        .write_max_attempts(0)
        .build()
        .expect("构建必须成功");
    assert_eq!(config.precision, Some(TsPrecision::Us));
    assert_eq!(config.max_query_rows, 50);
    assert_eq!(config.hosts, vec!["127.0.0.5".to_owned()]);
    assert_eq!(config.write_max_attempts, 1, "0 应被夹到 1");

    let error = TaosConfig::builder()
        .port(6041)
        .database("bad-name")
        .build()
        .expect_err("非法库名必须拒绝");
    assert!(matches!(error, TaosError::Config(_)));
}
