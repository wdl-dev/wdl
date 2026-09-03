use std::time::Duration;

use reqwest::StatusCode;
use serde_json::{Value as JsonValue, json};

use crate::response_body::read_bounded_response_text;
use crate::{AppState, Config, SchedulerError, SchedulerResult, now_ms};
use wdl_rust_common::internal_auth::INTERNAL_AUTH_HEADER;

const MAX_WORKFLOW_TICK_RESPONSE_BYTES: usize = 64 * 1024;

pub(crate) struct RemoteTickResponse {
    pub(crate) request_id: String,
    pub(crate) started_at_ms: i64,
    pub(crate) status: StatusCode,
    pub(crate) text: String,
    pub(crate) body: JsonValue,
}

pub(crate) fn json_usize(value: Option<&JsonValue>) -> usize {
    value
        .and_then(JsonValue::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(0)
}

fn workflow_tick_request(
    client: &reqwest::Client,
    config: &Config,
    url: &str,
    request_id: &str,
) -> reqwest::RequestBuilder {
    client
        .post(url)
        .header("content-type", "application/json")
        .header(INTERNAL_AUTH_HEADER, config.internal_auth_token.as_str())
        .header("x-request-id", request_id)
        .timeout(Duration::from_millis(config.workflows_tick_timeout_ms))
        .json(&json!({}))
}

async fn read_remote_tick_text(
    response: reqwest::Response,
    failure_message: &str,
) -> SchedulerResult<String> {
    let status = response.status();
    read_bounded_response_text(
        response,
        MAX_WORKFLOW_TICK_RESPONSE_BYTES,
        "workflow tick response body",
    )
    .await
    .map_err(|err| {
        SchedulerError::internal_error(format!(
            "{failure_message} while reading HTTP {} response body: {err}",
            status.as_u16()
        ))
    })
}

fn parse_workflow_tick_body(text: &str, status: StatusCode) -> SchedulerResult<JsonValue> {
    let body = serde_json::from_str::<JsonValue>(text).map_err(|_| {
        SchedulerError::internal_error(format!(
            "Workflow tick returned invalid JSON with HTTP {}",
            status.as_u16()
        ))
    })?;
    if !body.is_object() {
        return Err(SchedulerError::internal_error(format!(
            "Workflow tick returned a non-object body with HTTP {}",
            status.as_u16()
        )));
    }
    Ok(body)
}

pub(crate) async fn post_workflow_tick(
    state: &AppState,
) -> SchedulerResult<Option<RemoteTickResponse>> {
    let Some(host) = state.config.workflows_host.as_deref() else {
        return Ok(None);
    };
    let url = format!(
        "http://{}:{}/internal/workflows/tick",
        host, state.config.workflows_port
    );
    let request_id = format!("workflow-tick-{}-{}", state.instance_id, now_ms());
    let started_at_ms = now_ms();
    let response = workflow_tick_request(&state.http, &state.config, &url, &request_id)
        .send()
        .await
        .map_err(|err| SchedulerError::internal_error(format!("Workflow tick failed: {err}")))?;
    let status = response.status();
    let text = read_remote_tick_text(response, "Workflow tick failed").await?;
    let body = match parse_workflow_tick_body(&text, status) {
        Ok(body) => body,
        Err(err) if status.is_success() => return Err(err),
        Err(_) => json!({}),
    };
    Ok(Some(RemoteTickResponse {
        request_id,
        started_at_ms,
        status,
        text,
        body,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use wdl_rust_common::test_env::with_temp_envs;

    #[test]
    fn workflow_tick_request_uses_its_dedicated_timeout() {
        with_temp_envs(
            &[
                ("WDL_INTERNAL_AUTH_TOKEN", Some("test-internal-auth-token")),
                ("SCHEDULER_FIRE_TIMEOUT_MS", Some("60000")),
                ("WORKFLOWS_TICK_TIMEOUT_MS", Some("175000")),
            ],
            || {
                let config = crate::config_from_env();
                let request = workflow_tick_request(
                    &reqwest::Client::new(),
                    &config,
                    "http://127.0.0.1:9120/internal/workflows/tick",
                    "workflow-tick-test",
                )
                .build()
                .expect("workflow tick request should build");

                assert_eq!(
                    request.timeout().copied(),
                    Some(Duration::from_millis(175_000))
                );
            },
        );
    }

    #[test]
    fn workflow_tick_body_requires_json_object_root() {
        assert!(parse_workflow_tick_body("{}", StatusCode::OK).is_ok());
        for body in ["not-json", "null", "[]", "1", "\"text\""] {
            let err = parse_workflow_tick_body(body, StatusCode::OK)
                .expect_err("malformed and non-object tick bodies must fail closed");
            assert_eq!(err.code, "internal_error");
        }
    }

    #[tokio::test]
    async fn remote_tick_body_read_errors_are_not_treated_as_empty_successes() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request).unwrap();
            stream
                .write_all(
                    b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 20\r\nConnection: close\r\n\r\n{}",
                )
                .unwrap();
        });

        let response = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .post(format!("http://{addr}/internal/workflows/tick"))
            .body("{}")
            .send()
            .await
            .unwrap();
        let err = read_remote_tick_text(response, "Workflow tick failed")
            .await
            .unwrap_err();

        server.join().unwrap();
        assert_eq!(err.code, "internal_error");
        assert!(
            err.message
                .starts_with("Workflow tick failed while reading HTTP 503 response body:")
        );
    }
}
