use serde_json::{Value as JsonValue, json};
use wdl_rust_common::workflow_tick::WorkflowTickResponse;

use crate::{AppState, LogLevel, SchedulerError, SchedulerResult, json_usize, log, now_ms};

fn workflow_tick_has_progress(summary: &WorkflowTickResponse) -> bool {
    summary.workflow_admitted > 0
        || summary.due_moved > 0
        || summary.retention_cleaned > 0
        || summary.do_alarm_due_moved > 0
        || summary.do_alarm_admitted > 0
}

pub(crate) fn workflow_tick_needs_active_poll(summary: &WorkflowTickResponse) -> bool {
    workflow_tick_has_progress(summary)
        || summary.workflow_capacity_blocked
        || summary.do_alarm_capacity_blocked
}

fn workflow_tick_summary(body: &JsonValue) -> WorkflowTickResponse {
    WorkflowTickResponse {
        workflow_admitted: json_usize(body.get("workflowAdmitted")),
        workflow_capacity_blocked: body
            .get("workflowCapacityBlocked")
            .and_then(JsonValue::as_bool)
            .unwrap_or(false),
        due_moved: json_usize(body.get("dueMoved")),
        retention_cleaned: json_usize(body.get("retentionCleaned")),
        do_alarm_due_moved: json_usize(body.get("doAlarmDueMoved")),
        do_alarm_admitted: json_usize(body.get("doAlarmAdmitted")),
        do_alarm_capacity_blocked: body
            .get("doAlarmCapacityBlocked")
            .and_then(JsonValue::as_bool)
            .unwrap_or(false),
    }
}

pub(crate) async fn workflows_tick(state: AppState) -> SchedulerResult<WorkflowTickResponse> {
    let Some(response) = crate::post_workflow_tick(&state).await? else {
        return Ok(WorkflowTickResponse::default());
    };
    let status = response.status;
    let body = response.body;
    let duration_ms = now_ms() - response.started_at_ms;
    let summary = workflow_tick_summary(&body);
    let outcome = if status.is_success() { "ok" } else { "error" };
    log(
        &state,
        if workflow_tick_has_progress(&summary) || !status.is_success() {
            LogLevel::Info
        } else {
            LogLevel::Debug
        },
        "workflow_tick",
        json!({
            "request_id": response.request_id,
            "outcome": outcome,
            "status": status.as_u16(),
            "workflow_admitted": summary.workflow_admitted,
            "workflow_capacity_blocked": summary.workflow_capacity_blocked,
            "due_moved": summary.due_moved,
            "retention_cleaned": summary.retention_cleaned,
            "do_alarm_due_moved": summary.do_alarm_due_moved,
            "do_alarm_admitted": summary.do_alarm_admitted,
            "do_alarm_capacity_blocked": summary.do_alarm_capacity_blocked,
            "duration_ms": duration_ms,
        }),
    );
    if status.is_success() {
        return Ok(summary);
    }
    Err(SchedulerError::internal_error(format!(
        "Workflow tick returned {}: {}",
        status.as_u16(),
        response.text
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_tick_summary_separates_progress_from_active_poll_pressure() {
        assert!(!workflow_tick_has_progress(&WorkflowTickResponse::default()));
        assert!(!workflow_tick_needs_active_poll(
            &WorkflowTickResponse::default()
        ));
        for summary in [
            WorkflowTickResponse {
                workflow_admitted: 1,
                ..WorkflowTickResponse::default()
            },
            WorkflowTickResponse {
                workflow_capacity_blocked: true,
                ..WorkflowTickResponse::default()
            },
            WorkflowTickResponse {
                due_moved: 1,
                ..WorkflowTickResponse::default()
            },
            WorkflowTickResponse {
                retention_cleaned: 1,
                ..WorkflowTickResponse::default()
            },
            WorkflowTickResponse {
                do_alarm_due_moved: 1,
                ..WorkflowTickResponse::default()
            },
            WorkflowTickResponse {
                do_alarm_admitted: 1,
                ..WorkflowTickResponse::default()
            },
            WorkflowTickResponse {
                do_alarm_capacity_blocked: true,
                ..WorkflowTickResponse::default()
            },
        ] {
            assert!(workflow_tick_needs_active_poll(&summary));
        }
        assert!(workflow_tick_has_progress(&WorkflowTickResponse {
            workflow_admitted: 1,
            ..WorkflowTickResponse::default()
        }));
        assert!(!workflow_tick_has_progress(&WorkflowTickResponse {
            workflow_capacity_blocked: true,
            ..WorkflowTickResponse::default()
        }));
    }

    #[test]
    fn workflow_tick_summary_parses_the_admission_response_contract() {
        assert_eq!(
            workflow_tick_summary(
                &serde_json::from_str(include_str!(
                    "../../../tests/fixtures/workflow-tick-response.json"
                ))
                .expect("workflow tick response fixture parses")
            ),
            WorkflowTickResponse {
                workflow_admitted: 2,
                workflow_capacity_blocked: true,
                due_moved: 3,
                retention_cleaned: 4,
                do_alarm_due_moved: 5,
                do_alarm_admitted: 6,
                do_alarm_capacity_blocked: false,
            }
        );
    }

    #[test]
    fn workflow_tick_summary_treats_missing_or_non_count_fields_as_zero() {
        assert_eq!(
            workflow_tick_summary(&json!({
                "workflowAdmitted": -1,
                "workflowCapacityBlocked": 1,
                "dueMoved": "3",
                "retentionCleaned": null,
                "doAlarmDueMoved": 1.5,
                "doAlarmCapacityBlocked": "true",
            })),
            WorkflowTickResponse::default()
        );
    }
}
