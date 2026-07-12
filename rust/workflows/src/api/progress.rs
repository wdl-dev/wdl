use super::{execution::WorkflowStepRequest, parse_positive_identity_i64, runtime_endpoint};
use serde_json::{Value as JsonValue, json};
use wdl_rust_common::internal_auth::INTERNAL_AUTH_HEADER;
use wdl_rust_common::time::now_ms;

use crate::{
    AppState, InstanceIdentity, LogLevel, WorkflowRequest, WorkflowResult, instance_state_key, log,
};

const PROGRESS_CALLBACK_CACHE_LIMIT: usize = 4096;

fn progress_payload(event: &str, status: &str, step: Option<JsonValue>) -> JsonValue {
    json!({
        "event": event,
        "status": status,
        "timestampMs": now_ms(),
        "step": step,
    })
}

fn progress_callback_cache_key(identity: &InstanceIdentity) -> String {
    format!(
        "{}\t{}\t{}\t{}\t{}",
        identity.ns,
        identity.workflow_key,
        identity.instance_id,
        identity.generation,
        identity.created_at_ms
    )
}

async fn callback_from_state(
    app: &AppState,
    identity: &InstanceIdentity,
) -> WorkflowResult<Option<String>> {
    let cache_key = progress_callback_cache_key(identity);
    if let Ok(cache) = app.progress_callback_cache.lock()
        && let Some(callback) = cache.get(&cache_key)
    {
        return Ok(callback.clone());
    }
    let state_key = instance_state_key(&identity.ns, &identity.workflow_key, &identity.instance_id);
    let callback: Option<String> = app
        .redis
        .with_conn(async |mut conn| {
            redis::cmd("HGET")
                .arg(state_key)
                .arg("callback")
                .query_async(&mut conn)
                .await
        })
        .await?;
    let callback = callback.filter(|value| !value.is_empty());
    if let Ok(mut cache) = app.progress_callback_cache.lock() {
        cache.insert(cache_key, callback.clone(), PROGRESS_CALLBACK_CACHE_LIMIT);
    }
    Ok(callback)
}

async fn notify(app: &AppState, identity: InstanceIdentity, callback: String, progress: JsonValue) {
    let Ok(generation) = parse_positive_identity_i64(&identity.generation, "generation") else {
        app.metrics
            .increment("workflow_progress_callbacks", &[("outcome", "error")], 1.0);
        log(
            app,
            LogLevel::Warn,
            "workflow_progress_callback_failed",
            json!({
                "error": "invalid workflow generation",
            }),
        );
        return;
    };
    let url = runtime_endpoint(app, &identity.ns, "/internal/workflows/notify");
    let response = app
        .http
        .post(url)
        .header(
            INTERNAL_AUTH_HEADER,
            app.config.internal_auth_tokens.current.as_str(),
        )
        .timeout(std::time::Duration::from_millis(
            app.config.dispatch_timeout_ms.min(10_000),
        ))
        .json(&json!({
            "ns": identity.ns,
            "worker": identity.worker,
            "frozenVersion": identity.frozen_version,
            "workflowName": identity.workflow_name,
            "workflowKey": identity.workflow_key,
            "className": identity.class_name,
            "instanceId": identity.instance_id,
            "generation": generation,
            "callback": serde_json::from_str::<JsonValue>(&callback).unwrap_or(JsonValue::Null),
            "progress": progress,
        }))
        .send()
        .await;
    match response {
        Ok(response) if response.status().is_success() => {
            app.metrics
                .increment("workflow_progress_callbacks", &[("outcome", "ok")], 1.0);
        }
        Ok(response) => {
            app.metrics
                .increment("workflow_progress_callbacks", &[("outcome", "error")], 1.0);
            log(
                app,
                LogLevel::Warn,
                "workflow_progress_callback_failed",
                json!({
                    "status": response.status().as_u16(),
                }),
            );
        }
        Err(err) => {
            app.metrics
                .increment("workflow_progress_callbacks", &[("outcome", "error")], 1.0);
            log(
                app,
                LogLevel::Warn,
                "workflow_progress_callback_failed",
                json!({
                    "error": err.to_string(),
                }),
            );
        }
    }
}

pub(super) fn spawn_progress_from_identity(
    app: &AppState,
    identity: &InstanceIdentity,
    event: &str,
    status: &str,
    step: Option<JsonValue>,
) {
    let Ok(lookup_permit) = app.progress_callback_lookups.clone().try_acquire_owned() else {
        app.metrics.increment(
            "workflow_progress_callback_lookups",
            &[("outcome", "dropped")],
            1.0,
        );
        log(
            app,
            LogLevel::Debug,
            "workflow_progress_callback_lookup_dropped",
            json!({
                "ns": identity.ns,
                "worker": identity.worker,
                "workflow": identity.workflow_name,
                "instance_id": identity.instance_id,
            }),
        );
        return;
    };
    let app = app.clone();
    let identity = identity.clone();
    let event = event.to_string();
    let status = status.to_string();
    tokio::spawn(async move {
        let lookup_permit_guard = lookup_permit;
        let Ok(Some(callback)) = callback_from_state(&app, &identity).await else {
            return;
        };
        drop(lookup_permit_guard);
        let Ok(_permit) = app.progress_callbacks.clone().try_acquire_owned() else {
            app.metrics.increment(
                "workflow_progress_callbacks",
                &[("outcome", "dropped")],
                1.0,
            );
            log(
                &app,
                LogLevel::Debug,
                "workflow_progress_callback_dropped",
                json!({
                    "ns": identity.ns,
                    "worker": identity.worker,
                    "workflow": identity.workflow_name,
                    "instance_id": identity.instance_id,
                }),
            );
            return;
        };
        notify(
            &app,
            identity,
            callback,
            progress_payload(&event, &status, step),
        )
        .await;
    });
}

pub(super) fn spawn_progress_from_request(
    app: &AppState,
    req: &WorkflowRequest,
    instance_id: &str,
    created_at_ms: i64,
    event: &str,
    status: &str,
    step: Option<JsonValue>,
) {
    let identity = InstanceIdentity {
        ns: req.ns.clone(),
        worker: req.worker.clone(),
        frozen_version: req.frozen_version.clone(),
        workflow_name: req.workflow_name.clone(),
        workflow_key: req.workflow_key.clone(),
        class_name: req.class_name.clone(),
        instance_id: instance_id.to_string(),
        generation: "1".to_string(),
        created_at_ms: created_at_ms.to_string(),
    };
    spawn_progress_from_identity(app, &identity, event, status, step);
}

pub(super) fn spawn_progress_from_step(
    app: &AppState,
    req: &WorkflowStepRequest,
    event: &str,
    status: &str,
    attempt: u32,
) {
    let identity = InstanceIdentity {
        ns: req.ns.clone(),
        worker: req.worker.clone(),
        frozen_version: req.frozen_version.clone(),
        workflow_name: req.workflow_name.clone(),
        workflow_key: req.workflow_key.clone(),
        class_name: req.class_name.clone(),
        instance_id: req.instance_id.clone(),
        generation: req.generation.to_string(),
        created_at_ms: req.created_at_ms.to_string(),
    };
    spawn_progress_from_identity(
        app,
        &identity,
        event,
        status,
        Some(json!({
            "name": req.step_name,
            "ordinal": req.ordinal,
            "attempt": attempt,
        })),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_callback_cache_key_includes_generation() {
        let mut identity = InstanceIdentity {
            ns: "demo".to_string(),
            worker: "shop".to_string(),
            frozen_version: "v1".to_string(),
            workflow_name: "Order".to_string(),
            workflow_key: "demo:shop:Order".to_string(),
            class_name: "OrderWorkflow".to_string(),
            instance_id: "inst".to_string(),
            generation: "1".to_string(),
            created_at_ms: "0".to_string(),
        };
        let first = progress_callback_cache_key(&identity);
        identity.generation = "2".to_string();
        assert_ne!(first, progress_callback_cache_key(&identity));
        identity.generation = "1".to_string();
        identity.created_at_ms = "1".to_string();
        assert_ne!(first, progress_callback_cache_key(&identity));
        assert_eq!(first, "demo\tdemo:shop:Order\tinst\t1\t0");
    }
}
