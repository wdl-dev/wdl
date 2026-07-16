use std::time::Duration;

use serde_json::Value as JsonValue;

use crate::{AppState, Config};
use wdl_rust_common::internal_auth::INTERNAL_AUTH_HEADER;

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
    let text = match response.text().await {
        Ok(text) => text,
        Err(error) => {
            return RuntimeResponse {
                status: Some(status),
                json: None,
                text: None,
                error: Some(format!("failed to read runtime response body: {error}")),
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
}
