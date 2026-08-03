use serde_json::{Value as JsonValue, json};

use crate::{AppState, LogLevel, SchedulerError, SchedulerResult, json_usize, log, now_ms};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct WorkflowTickSummary {
    pub(crate) workflow_admitted: usize,
    pub(crate) workflow_capacity_blocked: bool,
    pub(crate) due_moved: usize,
    pub(crate) retention_cleaned: usize,
    pub(crate) do_alarm_due_moved: usize,
    pub(crate) do_alarm_admitted: usize,
    pub(crate) do_alarm_capacity_blocked: bool,
}

impl WorkflowTickSummary {
    fn has_progress(self) -> bool {
        self.workflow_admitted > 0
            || self.due_moved > 0
            || self.retention_cleaned > 0
            || self.do_alarm_due_moved > 0
            || self.do_alarm_admitted > 0
    }

    pub(crate) fn needs_active_poll(self) -> bool {
        self.has_progress() || self.workflow_capacity_blocked || self.do_alarm_capacity_blocked
    }
}

fn workflow_tick_summary(body: &JsonValue) -> WorkflowTickSummary {
    WorkflowTickSummary {
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

pub(crate) async fn workflows_tick(state: AppState) -> SchedulerResult<WorkflowTickSummary> {
    let Some(response) = crate::post_workflow_tick(&state).await? else {
        return Ok(WorkflowTickSummary::default());
    };
    let status = response.status;
    let body = response.body;
    let duration_ms = now_ms() - response.started_at_ms;
    let summary = workflow_tick_summary(&body);
    let outcome = if status.is_success() { "ok" } else { "error" };
    log(
        &state,
        if summary.has_progress() || !status.is_success() {
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
        assert!(!WorkflowTickSummary::default().has_progress());
        assert!(!WorkflowTickSummary::default().needs_active_poll());
        for summary in [
            WorkflowTickSummary {
                workflow_admitted: 1,
                ..WorkflowTickSummary::default()
            },
            WorkflowTickSummary {
                workflow_capacity_blocked: true,
                ..WorkflowTickSummary::default()
            },
            WorkflowTickSummary {
                due_moved: 1,
                ..WorkflowTickSummary::default()
            },
            WorkflowTickSummary {
                retention_cleaned: 1,
                ..WorkflowTickSummary::default()
            },
            WorkflowTickSummary {
                do_alarm_due_moved: 1,
                ..WorkflowTickSummary::default()
            },
            WorkflowTickSummary {
                do_alarm_admitted: 1,
                ..WorkflowTickSummary::default()
            },
            WorkflowTickSummary {
                do_alarm_capacity_blocked: true,
                ..WorkflowTickSummary::default()
            },
        ] {
            assert!(summary.needs_active_poll());
        }
        assert!(
            WorkflowTickSummary {
                workflow_admitted: 1,
                ..WorkflowTickSummary::default()
            }
            .has_progress()
        );
        assert!(
            !WorkflowTickSummary {
                workflow_capacity_blocked: true,
                ..WorkflowTickSummary::default()
            }
            .has_progress()
        );
    }

    #[test]
    fn workflow_tick_summary_parses_the_admission_response_contract() {
        assert_eq!(
            workflow_tick_summary(&json!({
                "workflowAdmitted": 2,
                "workflowCapacityBlocked": true,
                "dueMoved": 3,
                "retentionCleaned": 4,
                "doAlarmDueMoved": 5,
                "doAlarmAdmitted": 6,
                "doAlarmCapacityBlocked": false,
            })),
            WorkflowTickSummary {
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
            WorkflowTickSummary::default()
        );
    }
}
