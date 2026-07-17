use serde_json::Value as JsonValue;
use wdl_rust_common::worker_contract::{routes_key, worker_delete_lock_key};

use crate::{AppState, WorkflowError, WorkflowResult};

use super::{WorkflowDef, WorkflowRequest, bundle_key, workflow_defs_key};

async fn active_worker_version(state: &AppState, ns: &str, worker: &str) -> WorkflowResult<String> {
    let routes_key = routes_key(ns);
    let worker = worker.to_string();
    let active: Option<String> = state
        .control_redis
        .with_conn(async |mut conn| {
            redis::cmd("HGET")
                .arg(routes_key)
                .arg(worker)
                .query_async(&mut conn)
                .await
        })
        .await?;
    active.ok_or_else(|| WorkflowError::invalid_state("Worker is no longer active"))
}

struct ActiveWorkflowExport {
    version: String,
    class_name: String,
}

async fn active_workflow_export(
    state: &AppState,
    ns: &str,
    worker: &str,
    workflow_name: &str,
    workflow_key: &str,
) -> WorkflowResult<ActiveWorkflowExport> {
    let version = active_worker_version(state, ns, worker).await?;
    let key = bundle_key(ns, worker, &version)?;
    let raw: Option<String> = state
        .control_redis
        .with_conn(async |mut conn| {
            redis::cmd("HGET")
                .arg(key)
                .arg("__meta__")
                .query_async(&mut conn)
                .await
        })
        .await?;
    let raw = raw.ok_or_else(|| {
        WorkflowError::not_exported("Workflow is not exported by the active worker version")
    })?;
    let meta: JsonValue = serde_json::from_str(&raw).map_err(|err| {
        WorkflowError::invalid_state(format!("Active worker metadata is corrupt: {err}"))
    })?;
    let workflows = meta
        .get("workflows")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| {
            WorkflowError::not_exported("Workflow is not exported by the active worker version")
        })?;
    let class_name = workflows
        .iter()
        .find(|entry| {
            entry.get("name").and_then(JsonValue::as_str) == Some(workflow_name)
                && entry.get("workflowKey").and_then(JsonValue::as_str) == Some(workflow_key)
        })
        .and_then(|entry| entry.get("className").and_then(JsonValue::as_str))
        .ok_or_else(|| {
            WorkflowError::not_exported("Workflow is not exported by the active worker version")
        })?;
    Ok(ActiveWorkflowExport {
        version,
        class_name: class_name.to_string(),
    })
}

pub(super) async fn request_with_active_version(
    state: &AppState,
    mut req: WorkflowRequest,
) -> WorkflowResult<WorkflowRequest> {
    let active = active_workflow_export(
        state,
        &req.ns,
        &req.worker,
        &req.workflow_name,
        &req.workflow_key,
    )
    .await?;
    req.frozen_version = active.version;
    req.class_name = active.class_name;
    Ok(req)
}

pub(super) async fn verify_workflow_def_values(
    state: &AppState,
    ns: &str,
    worker: &str,
    workflow_name: &str,
    workflow_key: &str,
    _class_name: &str,
) -> WorkflowResult<()> {
    let key = workflow_defs_key(ns, worker);
    let field = workflow_name.to_string();
    let raw: Option<String> = state
        .control_redis
        .with_conn(async |mut conn| {
            redis::cmd("HGET")
                .arg(key)
                .arg(field)
                .query_async(&mut conn)
                .await
        })
        .await?;
    let raw = raw.ok_or_else(|| {
        WorkflowError::invalid_state("Workflow definition is missing from control metadata")
    })?;
    let def: WorkflowDef = serde_json::from_str(&raw).map_err(|err| {
        WorkflowError::invalid_state(format!("Workflow definition is corrupt: {err}"))
    })?;
    if def.workflow_key != workflow_key {
        return Err(WorkflowError::invalid_state(
            "Workflow definition does not match runtime metadata",
        ));
    }
    Ok(())
}

pub(super) async fn verify_active_workflow_current(
    state: &AppState,
    req: &WorkflowRequest,
) -> WorkflowResult<()> {
    let active = active_workflow_export(
        state,
        &req.ns,
        &req.worker,
        &req.workflow_name,
        &req.workflow_key,
    )
    .await?;
    if active.version != req.frozen_version || active.class_name != req.class_name {
        return Err(WorkflowError::not_exported(
            "Workflow active worker export changed during mutation",
        ));
    }
    Ok(())
}

pub(super) async fn ensure_worker_not_deleting(
    state: &AppState,
    ns: &str,
    worker: &str,
) -> WorkflowResult<()> {
    let lock_key = worker_delete_lock_key(ns, worker);
    let lock_exists: i64 = state
        .control_redis
        .with_conn(async |mut conn| {
            redis::cmd("EXISTS")
                .arg(lock_key)
                .query_async(&mut conn)
                .await
        })
        .await?;
    if lock_exists > 0 {
        return Err(WorkflowError::deleting(
            "Worker is being deleted; workflow mutation is blocked",
        ));
    }
    Ok(())
}

pub(super) async fn verify_workflow_def(
    state: &AppState,
    req: &WorkflowRequest,
) -> WorkflowResult<()> {
    verify_workflow_def_values(
        state,
        &req.ns,
        &req.worker,
        &req.workflow_name,
        &req.workflow_key,
        &req.class_name,
    )
    .await
}
