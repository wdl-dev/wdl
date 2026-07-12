use crate::{DRAIN_RETRY_DELAY_MS, SupervisorConfig, drain_timeout_ms, log, truncate_chars};
use serde_json::{Value, json};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use wdl_rust_common::internal_auth::{INTERNAL_AUTH_HEADER, internal_auth_token_from_env};
use wdl_rust_common::time::now_ms;

pub(crate) async fn drain(
    config: &SupervisorConfig,
    client: &reqwest::Client,
    signal_name: &str,
) -> bool {
    let timeout_ms = drain_timeout_ms(config);
    let internal_auth_token =
        internal_auth_token_from_env().expect("WDL_INTERNAL_AUTH_TOKEN must be configured");
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    let request_id = config.drain_request_id_prefix.map(build_request_id);
    let mut attempt: u32 = 0;
    let mut last_failure: Option<Value> = None;

    while Instant::now() < deadline {
        attempt += 1;
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let mut request = client
            .post(config.drain_url)
            .timeout(remaining)
            .header(INTERNAL_AUTH_HEADER, internal_auth_token.as_str())
            .header("content-length", "0");
        if let Some(rid) = request_id.as_deref() {
            request = request.header("x-request-id", rid);
        }
        match request.send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let body = resp.text().await.unwrap_or_default();
                let body_short = truncate_chars(&body, 500);
                let drain_payload = parse_drain_payload(&body);
                let mut fields = json!({
                    "signal": signal_name,
                    "attempt": attempt,
                    "status": status,
                    "body": body_short,
                });
                if let Some(rid) = request_id.as_deref() {
                    fields["request_id"] = json!(rid);
                }
                if let Err(err) = &drain_payload {
                    fields["parse_error"] = json!(*err);
                }
                let success = (200..300).contains(&status)
                    && drain_payload
                        .as_ref()
                        .map(|payload| {
                            payload.owned == 0
                                && (!config.drain_failure_on_errors_field
                                    || payload.errors_len == 0)
                        })
                        .unwrap_or(false);
                if success {
                    log::info(config.service, config.drain_complete_event, fields);
                    return true;
                }
                last_failure = Some(fields);
            }
            Err(err) => {
                let mut fields = json!({
                    "signal": signal_name,
                    "attempt": attempt,
                });
                merge_into(&mut fields, log::reqwest_error_fields(&err));
                if let Some(rid) = request_id.as_deref() {
                    fields["request_id"] = json!(rid);
                }
                last_failure = Some(fields);
            }
        }

        if Instant::now() + Duration::from_millis(DRAIN_RETRY_DELAY_MS) >= deadline {
            break;
        }
        sleep(Duration::from_millis(DRAIN_RETRY_DELAY_MS)).await;
    }

    let final_fields = last_failure
        .unwrap_or_else(|| json!({ "signal": signal_name, "error_message": "drain timeout" }));
    log::error(config.service, config.drain_failed_event, final_fields);
    false
}

struct DrainPayload {
    owned: u64,
    errors_len: usize,
}

fn parse_drain_payload(body: &str) -> Result<DrainPayload, &'static str> {
    let payload: Value = serde_json::from_str(body).map_err(|_| "invalid_json")?;
    let owned = payload
        .get("owned")
        .and_then(Value::as_u64)
        .ok_or("missing_owned")?;
    let errors_len = match payload.get("errors") {
        Some(Value::Array(errors)) => errors.len(),
        Some(_) => return Err("invalid_errors"),
        None => 0,
    };
    Ok(DrainPayload { owned, errors_len })
}

fn merge_into(target: &mut Value, source: Value) {
    if let (Value::Object(target_map), Value::Object(source_map)) = (target, source) {
        for (k, v) in source_map {
            target_map.insert(k, v);
        }
    }
}

pub(crate) fn build_request_id(prefix: &str) -> String {
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into());
    let pid = std::process::id();
    let ts = now_ms();
    format!("{prefix}-{host}-{pid}-{ts}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use wdl_rust_common::test_env::with_temp_env;

    #[test]
    fn build_request_id_starts_with_prefix() {
        with_temp_env("HOSTNAME", Some("test-host"), || {
            let id = build_request_id("do-drain");
            assert!(
                id.starts_with("do-drain-test-host-"),
                "expected `do-drain-test-host-...` shape, got {id}",
            );
        });
    }

    #[test]
    fn parse_drain_payload_requires_numeric_owned() {
        let payload = parse_drain_payload(r#"{"owned":0,"errors":[]}"#)
            .expect("valid drain payload should parse");
        assert_eq!(payload.owned, 0);
        assert_eq!(payload.errors_len, 0);

        assert!(parse_drain_payload(r#"{"errors":[]}"#).is_err());
        assert!(parse_drain_payload(r#"{"owned":"0","errors":[]}"#).is_err());
        assert!(parse_drain_payload(r#"{"owned":0,"errors":"boom"}"#).is_err());
        assert!(parse_drain_payload("not-json").is_err());
    }
}
