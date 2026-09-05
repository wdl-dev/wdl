use std::sync::OnceLock;

use serde_json::{Value, json};
pub(crate) use wdl_rust_common::log::LogLevel;
use wdl_rust_common::log::{emit_log_line, log_level_from_env};

use crate::SERVICE;

pub(crate) type Metrics = wdl_rust_common::metrics::MetricStore;

pub(crate) fn current_level() -> LogLevel {
    static LEVEL: OnceLock<LogLevel> = OnceLock::new();
    *LEVEL.get_or_init(log_level_from_env)
}

pub(crate) fn log_event(level: LogLevel, event: &str, fields: Value) {
    emit_log_line(SERVICE, level, current_level(), event, fields);
}

pub(crate) fn log_info(event: &str, fields: Value) {
    log_event(LogLevel::Info, event, fields);
}

pub(crate) fn started_log(
    port: u16,
    redis_configured: bool,
    data_redis_configured: bool,
    redis_connections_per_pool: usize,
) -> Value {
    json!({
        "port": port,
        "redis_configured": redis_configured,
        "data_redis_configured": data_redis_configured,
        "redis_connections_per_pool": redis_connections_per_pool,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_render_prometheus_shape() {
        let metrics = Metrics::default();
        metrics.increment(
            "requests",
            &[("service", SERVICE), ("route", "kv_get"), ("status", "200")],
            1.0,
        );
        metrics.observe(
            "request_duration_ms",
            &[("service", SERVICE), ("route", "kv_get")],
            3.5,
        );

        let body = metrics.render_prometheus();
        assert!(body.contains("# TYPE wdl_requests_total counter"));
        assert!(body.contains(
            r#"wdl_requests_total{route="kv_get",service="redis-proxy",status="200"} 1"#
        ));
        assert!(body.contains("# TYPE wdl_request_duration_ms summary"));
        assert!(
            body.contains(
                r#"wdl_request_duration_ms_count{route="kv_get",service="redis-proxy"} 1"#
            )
        );
        assert!(
            body.contains(
                r#"wdl_request_duration_ms_sum{route="kv_get",service="redis-proxy"} 3.5"#
            )
        );
        assert!(
            body.contains(
                r#"wdl_request_duration_ms_max{route="kv_get",service="redis-proxy"} 3.5"#
            )
        );
    }

    #[test]
    fn started_log_reports_configured_urls() {
        let fields = started_log(7070, true, false, 4);
        assert_eq!(fields["port"], 7070);
        assert_eq!(fields["redis_configured"], true);
        assert_eq!(fields["data_redis_configured"], false);
        assert_eq!(fields["redis_connections_per_pool"], 4);
    }
}
