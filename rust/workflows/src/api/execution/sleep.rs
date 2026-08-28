use serde_json::{Value as JsonValue, json};
use wdl_rust_common::time::now_ms;

use crate::{AppState, LogLevel, WorkflowError, WorkflowResult, log};

use super::super::spawn_progress_from_step;
use super::{
    STEP_KIND_SLEEP, STEP_KIND_SLEEP_UNTIL, StepRecord, StepRecordCommit, WorkflowStepRequest,
    commit_step_record, log_step_event, observe_step_duration, read_step_record_for_claim,
    step_record_json, step_summary_json, validate_step_request, verify_step_record,
};

fn sleep_kind(req: &WorkflowStepRequest) -> WorkflowResult<&'static str> {
    match req.config.get("type").and_then(JsonValue::as_str) {
        Some(STEP_KIND_SLEEP) => Ok(STEP_KIND_SLEEP),
        Some(STEP_KIND_SLEEP_UNTIL) => Ok(STEP_KIND_SLEEP_UNTIL),
        _ => Err(WorkflowError::invalid_request(
            "Workflow sleep step kind is invalid",
        )),
    }
}

fn completed_sleep_record(
    req: &WorkflowStepRequest,
    kind: &str,
    config: String,
    due_at_ms: i64,
) -> StepRecord {
    StepRecord {
        ordinal: req.ordinal,
        step_name: req.step_name.clone(),
        name_count: req.name_count,
        dependencies: req.dependencies.clone(),
        kind: kind.to_string(),
        config,
        status: "completed".to_string(),
        attempt: 1,
        output_ref: None,
        error_ref: None,
        output: None,
        error: None,
        completed_at_ms: Some(now_ms()),
        failed_at_ms: None,
        due_at_ms: Some(due_at_ms),
    }
}

fn waiting_sleep_record(
    req: &WorkflowStepRequest,
    kind: &str,
    config: String,
    due_at_ms: i64,
) -> StepRecord {
    StepRecord {
        ordinal: req.ordinal,
        step_name: req.step_name.clone(),
        name_count: req.name_count,
        dependencies: req.dependencies.clone(),
        kind: kind.to_string(),
        config,
        status: "waiting".to_string(),
        attempt: 1,
        output_ref: None,
        error_ref: None,
        output: None,
        error: None,
        completed_at_ms: None,
        failed_at_ms: None,
        due_at_ms: Some(due_at_ms),
    }
}

async fn write_sleep_record(
    state: &AppState,
    req: &WorkflowStepRequest,
    record: StepRecord,
    due_at_ms: Option<i64>,
) -> WorkflowResult<()> {
    let record_json = step_record_json(&record)?;
    let summary_json = step_summary_json(&record)?;
    let status = if due_at_ms.is_some() {
        "waiting"
    } else {
        "running"
    };
    let completed_at_ms = now_ms();
    commit_step_record(
        state,
        req,
        StepRecordCommit {
            record_json,
            summary_json,
            state_status: status,
            due_at_ms,
            payload_ref: None,
            payload_json: None,
            event_record: None,
            event_index_member: None,
            waiting_event_index_prefix: None,
        },
    )
    .await?;
    let outcome = if due_at_ms.is_some() {
        "waiting"
    } else {
        "completed"
    };
    state
        .metrics
        .increment("workflow_steps", &[("outcome", outcome)], 1.0);
    if outcome == "completed" {
        observe_step_duration(state, req, completed_at_ms);
        log_step_event(state, "workflow_step_completed", req, record.attempt.max(1));
        spawn_progress_from_step(
            state,
            req,
            "workflow_step_completed",
            "completed",
            record.attempt.max(1),
        );
    } else {
        log(
            state,
            LogLevel::Info,
            "workflow_instance_waiting",
            json!({
                "namespace": req.ns,
                "worker": req.worker,
                "workflow_name": req.workflow_name,
                "workflow_key": req.workflow_key,
                "workflow_class": req.class_name,
                "instance_id": req.instance_id,
                "frozen_version": req.frozen_version,
                "generation": req.generation,
                "step_name": req.step_name,
                "ordinal": req.ordinal,
            }),
        );
        spawn_progress_from_step(state, req, "workflow_instance_waiting", "waiting", 1);
    }
    Ok(())
}

pub(crate) async fn register_sleep(
    state: &AppState,
    req: WorkflowStepRequest,
) -> WorkflowResult<JsonValue> {
    let config = validate_step_request(&req)?;
    let kind = sleep_kind(&req)?;
    let due_at_ms = req
        .due_at_ms
        .ok_or_else(|| WorkflowError::invalid_request("dueAtMs is required"))?;
    if due_at_ms <= 0 {
        return Err(WorkflowError::invalid_request(
            "dueAtMs must be a positive integer",
        ));
    }
    let raw = read_step_record_for_claim(state, &req).await?;
    let now = now_ms();
    if let Some(raw) = raw {
        let record: StepRecord = serde_json::from_str(&raw).map_err(|err| {
            WorkflowError::invalid_state(format!("Workflow sleep step record is corrupt: {err}"))
        })?;
        verify_step_record(&req, &config, &record, kind)?;
        match record.status.as_str() {
            "completed" => return Ok(json!({ "state": "complete" })),
            "waiting" => {
                let stored_due = record.due_at_ms.ok_or_else(|| {
                    WorkflowError::invalid_state("Workflow waiting sleep step is missing dueAtMs")
                })?;
                if stored_due > now {
                    write_sleep_record(state, &req, record, Some(stored_due)).await?;
                    return Ok(json!({ "state": "waiting" }));
                }
                let completed = completed_sleep_record(&req, kind, config, stored_due);
                write_sleep_record(state, &req, completed, None).await?;
                return Ok(json!({ "state": "complete" }));
            }
            _ => {
                return Err(WorkflowError::invalid_state(
                    "Workflow sleep step is not waiting or completed",
                ));
            }
        }
    }

    if due_at_ms <= now {
        let completed = completed_sleep_record(&req, kind, config, due_at_ms);
        write_sleep_record(state, &req, completed, None).await?;
        return Ok(json!({ "state": "complete" }));
    }
    let waiting = waiting_sleep_record(&req, kind, config, due_at_ms);
    write_sleep_record(state, &req, waiting, Some(due_at_ms)).await?;
    Ok(json!({ "state": "waiting" }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sleep_request() -> WorkflowStepRequest {
        WorkflowStepRequest {
            ns: "tenant".to_string(),
            worker: "worker".to_string(),
            frozen_version: "v1".to_string(),
            workflow_name: "wf".to_string(),
            workflow_key: "wf_key".to_string(),
            class_name: "Workflow".to_string(),
            instance_id: "inst".to_string(),
            generation: 1,
            created_at_ms: 1_700_000_000_000,
            run_token: "run".to_string(),
            ordinal: 7,
            step_name: "sleep".to_string(),
            name_count: 1,
            dependencies: vec![1, 3],
            attempt: None,
            non_retryable: false,
            started_at_ms: None,
            config: json!({ "type": "sleep", "durationMs": 1000 }),
            output: JsonValue::Null,
            error: JsonValue::Null,
            due_at_ms: Some(1_700_000_001_000),
        }
    }

    #[test]
    fn waiting_sleep_record_preserves_identity_and_due_time() {
        let req = sleep_request();
        let record = waiting_sleep_record(
            &req,
            STEP_KIND_SLEEP,
            "{\"durationMs\":1000,\"type\":\"sleep\"}".to_string(),
            1234,
        );

        assert_eq!(record.ordinal, 7);
        assert_eq!(record.step_name, "sleep");
        assert_eq!(record.name_count, 1);
        assert_eq!(record.dependencies, vec![1, 3]);
        assert_eq!(record.kind, STEP_KIND_SLEEP);
        assert_eq!(record.config, "{\"durationMs\":1000,\"type\":\"sleep\"}");
        assert_eq!(record.status, "waiting");
        assert_eq!(record.attempt, 1);
        assert_eq!(record.due_at_ms, Some(1234));
        assert_eq!(record.completed_at_ms, None);
        assert_eq!(record.failed_at_ms, None);
        assert_eq!(record.output_ref, None);
        assert_eq!(record.error_ref, None);
    }

    #[test]
    fn completed_sleep_record_marks_step_completed_without_payload_refs() {
        let req = sleep_request();
        let record = completed_sleep_record(&req, STEP_KIND_SLEEP, "{}".to_string(), 5678);

        assert_eq!(record.status, "completed");
        assert_eq!(record.kind, STEP_KIND_SLEEP);
        assert_eq!(record.attempt, 1);
        assert_eq!(record.due_at_ms, Some(5678));
        assert!(record.completed_at_ms.is_some());
        assert_eq!(record.failed_at_ms, None);
        assert_eq!(record.output_ref, None);
        assert_eq!(record.error_ref, None);
        assert_eq!(record.output, None);
        assert_eq!(record.error, None);
    }
}
