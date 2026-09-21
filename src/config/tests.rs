//! TDengine 连接配置（`TaosConfig` / `TaosConfigBuilder`）的内联单元测试。
//!
//! 由 `src/config.rs` 的 `#[cfg(test)] mod tests;` 引入，仅在测试构建中编译。
//! 首个导入以 `#[cfg(test)]` 标注，使审计器的测试段判定起点落在文件开头。

#[cfg(test)]
use super::*;

#[test]
fn default_values_are_loopback_http() {
    let config = TaosConfig::default();
    assert_eq!(config.host, DEFAULT_HOST);
    assert_eq!(config.port, DEFAULT_PORT);
    assert_eq!(config.user, DEFAULT_USER);
    assert_eq!(config.database, DEFAULT_DATABASE);
    assert!(config.password.is_empty());
    assert_eq!(config.rest_sql_url(), "http://127.0.0.1:6041/rest/sql");
    assert_eq!(config.native_ws_url(), "ws://127.0.0.1:6041/rest/ws");
    config.validate().expect("默认配置必须有效");
}

#[test]
fn debug_redacts_password() {
    let config = TaosConfig {
        password: "fake-pass-value-42".into(),
        ..Default::default()
    };
    let rendered = format!("{config:?}");
    assert!(rendered.contains("***"));
    assert!(!rendered.contains("fake-pass-value-42"));
}

#[test]
fn toml_parses_flat_fields() {
    let config = TaosConfig::from_toml(
        r#"
schema_version = 1
host = "127.0.0.1"
port = 6041
database = "macro_data"
user = "writer"
tls = false
timeout_ms = 15000
precision = "ns"
transport = "native"
max_in_flight = 32
hosts = ["127.0.0.2"]
write_max_attempts = 3
"#,
    )
    .expect("TOML 解析必须成功");
    assert_eq!(config.database, "macro_data");
    assert_eq!(config.user, "writer");
    assert_eq!(config.timeout, Duration::from_millis(15000));
    assert_eq!(config.precision, Some(TsPrecision::Ns));
    assert_eq!(config.transport, TransportMode::NativeWs);
    assert_eq!(config.max_in_flight, 32);
    assert_eq!(config.hosts, vec!["127.0.0.2".to_owned()]);
    assert_eq!(config.write_max_attempts, 3);
    assert!(config.password.is_empty());
}

#[test]
fn toml_rejects_schema_and_unknown_fields() {
    assert!(TaosConfig::from_toml("host = \"127.0.0.1\"\n").is_err());
    assert!(TaosConfig::from_toml("schema_version = 99\n").is_err());
    assert!(TaosConfig::from_toml("schema_version = 1\nsink_id = \"m\"\n").is_err());
}

#[test]
fn toml_rejects_non_empty_password_without_echoing_it() {
    let error = TaosConfig::from_toml("schema_version = 1\npassword = \"hunter2\"\n")
        .expect_err("非空 password 必须拒绝");
    assert!(error.to_string().contains("password"));
    assert!(!error.to_string().contains("hunter2"));
}

#[test]
fn toml_rejects_invalid_precision_and_transport() {
    assert!(TaosConfig::from_toml("schema_version = 1\nprecision = \"bogus\"\n").is_err());
    assert!(TaosConfig::from_toml("schema_version = 1\ntransport = \"grpc\"\n").is_err());
}

#[test]
fn hard_limits_fail_closed() {
    let cases = [
        TaosConfig {
            max_in_flight: HARD_MAX_IN_FLIGHT + 1,
            ..Default::default()
        },
        TaosConfig {
            max_in_flight: 0,
            ..Default::default()
        },
        TaosConfig {
            batch_max_rows: HARD_MAX_BATCH_ROWS + 1,
            ..Default::default()
        },
        TaosConfig {
            batch_max_bytes: HARD_MAX_BATCH_BYTES + 1,
            ..Default::default()
        },
        TaosConfig {
            max_response_bytes: HARD_MAX_RESPONSE_BYTES + 1,
            ..Default::default()
        },
        TaosConfig {
            max_query_rows: HARD_MAX_QUERY_ROWS + 1,
            ..Default::default()
        },
        TaosConfig {
            close_timeout: HARD_MAX_CLOSE_TIMEOUT + Duration::from_millis(1),
            ..Default::default()
        },
    ];
    for config in cases {
        assert!(config.validate().is_err(), "{config:?} 必须被拒绝");
    }
}

#[test]
fn remote_plaintext_and_auth_fail_closed() {
    let plaintext = TaosConfig {
        host: "td.example".into(),
        ..Default::default()
    };
    assert!(plaintext.validate().is_err());

    let no_password = TaosConfig {
        host: "td.example".into(),
        tls: true,
        ..Default::default()
    };
    assert!(no_password.validate().is_err());

    let secure = TaosConfig {
        host: "td.example".into(),
        tls: true,
        password: "configured".into(),
        ..Default::default()
    };
    secure.validate().expect("远程 TLS + 认证必须通过");
}

#[test]
fn host_classification_and_ipv6_are_strict() {
    for bad in [
        "localhost.evil",
        "127.0.0.1.evil",
        "user@localhost",
        "http://localhost",
    ] {
        let config = TaosConfig {
            host: bad.into(),
            ..Default::default()
        };
        assert!(config.validate().is_err(), "坏主机 {bad} 必须被拒绝");
    }
    let ipv6 = TaosConfig {
        host: "::1".into(),
        ..Default::default()
    };
    ipv6.validate().expect("IPv6 环回必须通过");
    assert_eq!(ipv6.rest_sql_url(), "http://[::1]:6041/rest/sql");
}

#[test]
fn endpoint_hosts_dedupes_and_keeps_order() {
    let config = TaosConfig {
        host: "a".into(),
        hosts: vec!["a".into(), "b".into(), "c".into()],
        ..Default::default()
    };
    assert_eq!(config.endpoint_hosts(), vec!["a", "b", "c"]);
    assert_eq!(
        config.rest_sql_url_for("db.example"),
        "http://db.example:6041/rest/sql"
    );
    assert!(config.rest_sql_db_url().ends_with("/infra_draft"));
}

#[test]
fn builder_overrides_and_builds() {
    let config = TaosConfig::builder()
        .host("127.0.0.1")
        .port(6041)
        .database("ticks")
        .user("writer")
        .password("p")
        .max_in_flight(4)
        .batch_max_rows(10)
        .precision(TsPrecision::Ns)
        .transport(TransportMode::NativeWs)
        .hosts(["127.0.0.2"])
        .timeout(Duration::from_millis(500))
        .acquire_timeout(Duration::from_millis(100))
        .close_timeout(Duration::from_millis(100))
        .write_max_attempts(0)
        .build()
        .expect("构建必须成功");
    assert_eq!(config.max_in_flight, 4);
    assert_eq!(config.precision, Some(TsPrecision::Ns));
    assert_eq!(config.transport, TransportMode::NativeWs);
    assert_eq!(config.write_max_attempts, 1, "0 应被夹到 1");
    assert_eq!(config.hosts, vec!["127.0.0.2".to_owned()]);

    let rebuilt = TaosConfigBuilder::from_config(config)
        .build()
        .expect("重新构建");
    assert_eq!(rebuilt.database, "ticks");
}

#[test]
fn builder_rejects_invalid_config() {
    let error = TaosConfig::builder()
        .host("")
        .build()
        .expect_err("空 host 必须拒绝");
    assert!(!error.is_retryable(), "配置错误不可重试");
}

#[test]
fn precision_roundtrip_and_parse() {
    assert_eq!(TsPrecision::Ms.from_nanos(1_500_000_000), 1500);
    assert_eq!(TsPrecision::Ms.to_nanos(1500), 1_500_000_000);
    assert_eq!(TsPrecision::Ns.from_nanos(42), 42);
    assert_eq!(TsPrecision::parse("US"), Some(TsPrecision::Us));
    assert_eq!(TsPrecision::parse(" bogus "), None);
    assert_eq!(TsPrecision::Ms.as_str(), "ms");
    assert_eq!(TransportMode::parse("rest"), Some(TransportMode::Rest));
    assert_eq!(
        TransportMode::parse("native-ws"),
        Some(TransportMode::NativeWs)
    );
    assert!(TransportMode::parse("bogus").is_none());
}

#[test]
fn env_parsed_reports_variable_name_without_echoing_value() {
    std::env::set_var(ENV_TIMEOUT_MS, "secret-not-a-number");
    let error = env_parsed::<u64>(ENV_TIMEOUT_MS).expect_err("非法数值必须拒绝");
    std::env::remove_var(ENV_TIMEOUT_MS);
    assert!(error.to_string().contains(ENV_TIMEOUT_MS));
    assert!(!error.to_string().contains("secret-not-a-number"));
}

#[test]
fn from_toml_does_not_read_env_overrides() {
    std::env::set_var(ENV_DATABASE, "from_env_database");
    let config = TaosConfig::from_toml("schema_version = 1\ndatabase = \"from_toml_database\"\n")
        .expect("TOML 解析");
    std::env::remove_var(ENV_DATABASE);
    assert_eq!(
        config.database, "from_toml_database",
        "from_toml 必须与环境变量隔离"
    );
}

#[test]
fn from_toml_file_missing_path_fails_closed() {
    let missing = std::env::temp_dir().join(format!("taosx-missing-{}.toml", std::process::id()));
    let error = TaosConfig::from_toml_file(&missing).expect_err("缺失文件必须拒绝");
    assert!(error.to_string().contains("TOML 文件读取失败"));
}
