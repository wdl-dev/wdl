use std::time::Duration;

use serde_json::{Value as JsonValue, json};

use crate::log::{LogLevel, emit_log_line};
use crate::metrics::MetricStore;
use crate::time::duration_ms_for_log;

pub struct RequestCompletion<'a> {
    pub method: &'a str,
    pub route: &'a str,
    pub status: u16,
    pub status_label: &'a str,
    pub request_id: Option<&'a str>,
    pub duration: Duration,
    pub error: Option<(&'a str, &'a str)>,
}

pub fn record_request_completion(
    metrics: &MetricStore,
    service: &str,
    min_log_level: LogLevel,
    completion: RequestCompletion<'_>,
) {
    debug_assert_eq!(
        completion.status_label.parse::<u16>().ok(),
        Some(completion.status),
        "request status label must match the numeric status"
    );
    let duration_ms = completion.duration.as_secs_f64() * 1000.0;
    metrics.increment(
        "requests",
        &[
            ("service", service),
            ("route", completion.route),
            ("status", completion.status_label),
        ],
        1.0,
    );
    metrics.observe(
        "request_duration_ms",
        &[("service", service), ("route", completion.route)],
        duration_ms,
    );

    let server_error = (500..600).contains(&completion.status);
    if server_error {
        metrics.increment(
            "request_errors",
            &[
                ("service", service),
                ("route", completion.route),
                ("status", completion.status_label),
            ],
            1.0,
        );
    }

    let probe = matches!(completion.route, "healthz" | "metrics");
    let level = if server_error {
        LogLevel::Error
    } else {
        LogLevel::Info
    };
    if (probe && !server_error) || level < min_log_level {
        return;
    }

    emit_log_line(
        service,
        level,
        min_log_level,
        "request_complete",
        request_log_fields(&completion),
    );
}

fn request_log_fields(completion: &RequestCompletion<'_>) -> JsonValue {
    json!({
        "request_id": completion.request_id,
        "method": completion.method,
        "route": completion.route,
        "status": completion.status,
        "duration_ms": duration_ms_for_log(completion.duration),
        "error_code": completion.error.map(|(code, _)| code),
        "error_message": completion.error.map(|(_, message)| message),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn completion<'a>(route: &'a str, status: u16, status_label: &'a str) -> RequestCompletion<'a> {
        RequestCompletion {
            method: "GET",
            route,
            status,
            status_label,
            request_id: Some("request-1"),
            duration: Duration::from_micros(2_309),
            error: None,
        }
    }

    #[test]
    fn request_completion_records_canonical_metrics() {
        let metrics = MetricStore::default();
        record_request_completion(
            &metrics,
            "test",
            LogLevel::Error,
            completion("kv_get", 200, "200"),
        );

        let body = metrics.render_prometheus();
        assert!(
            body.contains(r#"wdl_requests_total{route="kv_get",service="test",status="200"} 1"#)
        );
        assert!(body.contains(r#"wdl_request_duration_ms_count{route="kv_get",service="test"} 1"#));
        assert!(!body.contains("wdl_request_errors_total"));
    }

    #[test]
    fn request_completion_fields_preserve_log_contract() {
        let completion = RequestCompletion {
            error: Some(("invalid_request", "invalid request")),
            ..completion("workflow_create", 400, "400")
        };
        assert_eq!(
            request_log_fields(&completion),
            json!({
                "request_id": "request-1",
                "method": "GET",
                "route": "workflow_create",
                "status": 400,
                "duration_ms": 2,
                "error_code": "invalid_request",
                "error_message": "invalid request",
            })
        );
    }

    #[test]
    fn server_errors_increment_error_metrics() {
        let metrics = MetricStore::default();
        record_request_completion(
            &metrics,
            "test",
            LogLevel::Error,
            RequestCompletion {
                error: Some(("backend_unavailable", "backend unavailable")),
                ..completion("kv_get", 503, "503")
            },
        );

        assert!(
            metrics.render_prometheus().contains(
                r#"wdl_request_errors_total{route="kv_get",service="test",status="503"} 1"#
            )
        );
    }
}
