use std::time::Duration;

use serde_json::Value as JsonValue;

use crate::{AppState, Config};
use wdl_rust_common::internal_auth::INTERNAL_AUTH_HEADER;

const MAX_RUNTIME_RESPONSE_BYTES: usize = 1024 * 1024;

pub(crate) struct RuntimeResponse {
    pub(crate) status: Option<u16>,
    pub(crate) json: Option<JsonValue>,
    pub(crate) text: Option<String>,
    pub(crate) error: Option<String>,
}

pub(crate) fn runtime_outcome_label(res: &RuntimeResponse) -> &'static str {
    if res.error.is_none()
        && res
            .json
            .as_ref()
            .and_then(|v| v.get("outcome"))
            .and_then(JsonValue::as_str)
            == Some("ok")
    {
        "ok"
    } else {
        "error"
    }
}

fn pick_runtime(config: &Config, worker_id: &str) -> (String, u16) {
    if worker_id.starts_with("__system__:") {
        (
            config.system_runtime_host.clone(),
            config.system_runtime_port,
        )
    } else {
        (config.runtime_host.clone(), config.runtime_port)
    }
}

pub(crate) async fn post_runtime(
    state: &AppState,
    path: &str,
    body: JsonValue,
    worker_id: &str,
    request_id: &str,
) -> RuntimeResponse {
    let (host, port) = pick_runtime(&state.config, worker_id);
    let url = format!("http://{host}:{port}{path}");
    let result = state
        .http
        .post(url)
        .header("content-type", "application/json")
        .header(
            INTERNAL_AUTH_HEADER,
            state.config.internal_auth_token.as_str(),
        )
        .header("x-worker-id", worker_id)
        .header("x-request-id", request_id)
        .timeout(Duration::from_millis(state.config.fire_timeout_ms))
        .json(&body)
        .send()
        .await;
    let Ok(response) = result else {
        return RuntimeResponse {
            status: None,
            json: None,
            text: None,
            error: Some(result.unwrap_err().to_string()),
        };
    };
    let status = response.status().as_u16();
    let text = match read_runtime_response(response).await {
        Ok(text) => text,
        Err(error) => {
            return RuntimeResponse {
                status: Some(status),
                json: None,
                text: None,
                error: Some(error),
            };
        }
    };
    let parsed = serde_json::from_str::<JsonValue>(&text).ok();
    RuntimeResponse {
        status: Some(status),
        json: parsed,
        text: Some(text),
        error: None,
    }
}

async fn read_runtime_response(mut response: reqwest::Response) -> Result<String, String> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RUNTIME_RESPONSE_BYTES as u64)
    {
        return Err("runtime response body exceeds 1048576 bytes".to_string());
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|err| format!("failed to read runtime response body: {err}"))?
    {
        append_runtime_response_chunk(&mut body, &chunk)?;
    }
    String::from_utf8(body).map_err(|_| "runtime response body is not valid UTF-8".to_string())
}

fn append_runtime_response_chunk(body: &mut Vec<u8>, chunk: &[u8]) -> Result<(), String> {
    if body.len().saturating_add(chunk.len()) > MAX_RUNTIME_RESPONSE_BYTES {
        return Err("runtime response body exceeds 1048576 bytes".to_string());
    }
    body.extend_from_slice(chunk);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn runtime_outcome_label_is_bounded_to_ok_or_error() {
        assert_eq!(
            runtime_outcome_label(&RuntimeResponse {
                status: Some(200),
                json: Some(json!({ "outcome": "ok" })),
                text: None,
                error: None,
            }),
            "ok"
        );
        assert_eq!(
            runtime_outcome_label(&RuntimeResponse {
                status: Some(200),
                json: Some(json!({ "outcome": "weird" })),
                text: None,
                error: None,
            }),
            "error"
        );
        assert_eq!(
            runtime_outcome_label(&RuntimeResponse {
                status: None,
                json: None,
                text: None,
                error: Some("ECONNREFUSED".to_string()),
            }),
            "error"
        );
    }

    #[test]
    fn runtime_response_body_accepts_the_limit_and_rejects_the_next_byte() {
        let mut body = Vec::new();
        let exact_limit = vec![b'a'; MAX_RUNTIME_RESPONSE_BYTES];
        append_runtime_response_chunk(&mut body, &exact_limit).unwrap();
        assert_eq!(body.len(), MAX_RUNTIME_RESPONSE_BYTES);
        assert_eq!(
            append_runtime_response_chunk(&mut body, b"x"),
            Err("runtime response body exceeds 1048576 bytes".to_string())
        );
    }
}
