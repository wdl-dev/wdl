use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use wdl_rust_common::identity::{is_valid_runtime_load_ns, is_valid_worker_name};
use wdl_rust_common::version::parse_version_tag;

use crate::{
    DoAlarmJobKeys, WorkflowError, WorkflowResult, do_alarm_by_worker_key, do_alarm_job_id,
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DoAlarmSetRequest {
    pub(crate) ns: String,
    pub(crate) worker: String,
    pub(crate) version: String,
    pub(crate) do_storage_id: String,
    pub(crate) class_name: String,
    pub(crate) object_name: String,
    pub(crate) scheduled_time: i64,
    pub(crate) retry_count: u64,
    pub(crate) token: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DoAlarmDeleteRequest {
    pub(crate) ns: String,
    pub(crate) worker: String,
    pub(crate) do_storage_id: String,
    pub(crate) class_name: String,
    pub(crate) object_name: String,
    pub(crate) token: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DoAlarmCleanupRequest {
    pub(crate) ns: String,
    pub(crate) worker: String,
    pub(crate) do_storage_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DoAlarmMutationResponse {
    pub(crate) ok: bool,
    pub(crate) job_id: Option<String>,
    pub(crate) changed: bool,
    pub(crate) deleted: usize,
}

#[derive(Clone)]
pub(super) struct DoAlarmJob {
    pub(super) job_id: String,
    pub(super) ns: String,
    pub(super) worker: String,
    pub(super) version: String,
    pub(super) do_storage_id: String,
    pub(super) class_name: String,
    pub(super) object_name: String,
    pub(super) row_token: String,
    pub(super) run_token: String,
    pub(super) retry_count: u64,
}

impl DoAlarmJob {
    pub(super) fn keys(&self) -> DoAlarmJobKeys {
        DoAlarmJobKeys::new(self.job_id.clone())
    }

    pub(super) fn by_worker_key(&self) -> String {
        do_alarm_by_worker_key(&self.ns, &self.worker)
    }
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DoAlarmTickCounters {
    pub(crate) due_moved: usize,
    pub(crate) dispatched: usize,
    pub(crate) delivered: usize,
    pub(crate) retried: usize,
    pub(crate) discarded: usize,
    pub(crate) skipped: usize,
}

pub(super) fn validate_non_empty(value: &str, field: &'static str) -> WorkflowResult<()> {
    if value.is_empty() {
        return Err(WorkflowError::invalid_request(format!(
            "{field} is required"
        )));
    }
    Ok(())
}

pub(super) fn validate_set_request(req: &DoAlarmSetRequest) -> WorkflowResult<()> {
    // These identity fields arrive from the private do-runtime alarm shim. They are
    // hashed into job ids before touching Redis key names, so non-empty validation
    // preserves the internal contract without imposing tenant namespace/name grammar
    // on Durable Object class names or object names.
    validate_non_empty(&req.ns, "ns")?;
    validate_non_empty(&req.worker, "worker")?;
    validate_non_empty(&req.version, "version")?;
    validate_non_empty(&req.do_storage_id, "doStorageId")?;
    validate_non_empty(&req.class_name, "className")?;
    validate_non_empty(&req.object_name, "objectName")?;
    validate_non_empty(&req.token, "token")?;
    if req.scheduled_time <= 0 {
        return Err(WorkflowError::invalid_request(
            "scheduledTime must be a positive Unix millisecond timestamp",
        ));
    }
    Ok(())
}

pub(super) fn validate_delete_request(req: &DoAlarmDeleteRequest) -> WorkflowResult<()> {
    validate_non_empty(&req.ns, "ns")?;
    validate_non_empty(&req.worker, "worker")?;
    validate_non_empty(&req.do_storage_id, "doStorageId")?;
    validate_non_empty(&req.class_name, "className")?;
    validate_non_empty(&req.object_name, "objectName")?;
    validate_non_empty(&req.token, "token")?;
    Ok(())
}

pub(super) fn job_keys_for_identity(
    ns: &str,
    worker: &str,
    do_storage_id: &str,
    class_name: &str,
    object_name: &str,
) -> DoAlarmJobKeys {
    DoAlarmJobKeys::new(do_alarm_job_id(
        ns,
        worker,
        do_storage_id,
        class_name,
        object_name,
    ))
}

pub(super) fn map_hgetall(flat: Vec<String>) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for pair in flat.chunks_exact(2) {
        map.insert(pair[0].clone(), pair[1].clone());
    }
    map
}

const MAX_PROTOCOL_STRING_BYTES: usize = 512;

fn required_field<'a>(
    state: &'a HashMap<String, String>,
    field: &'static str,
) -> WorkflowResult<&'a str> {
    state
        .get(field)
        .map(String::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| WorkflowError::invalid_state(format!("DO alarm job missing {field}")))
}

fn invalid_job_field(field: &'static str) -> WorkflowError {
    WorkflowError::invalid_state(format!("DO alarm job has invalid {field}"))
}

fn has_control_char(value: &str) -> bool {
    value.bytes().any(|byte| byte < 0x20 || byte == 0x7f)
}

fn is_class_name(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first == '$' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch == '$' || ch.is_ascii_alphanumeric())
}

fn is_storage_id(value: &str) -> bool {
    value.bytes().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_' || byte == b'-'
    })
}

fn protocol_string_field<'a>(
    state: &'a HashMap<String, String>,
    field: &'static str,
    valid: impl Fn(&str) -> bool,
) -> WorkflowResult<&'a str> {
    let value = required_field(state, field)?;
    if value.len() > MAX_PROTOCOL_STRING_BYTES || has_control_char(value) || !valid(value) {
        return Err(invalid_job_field(field));
    }
    Ok(value)
}

fn object_name_field<'a>(
    state: &'a HashMap<String, String>,
    field: &'static str,
) -> WorkflowResult<&'a str> {
    let value = required_field(state, field)?;
    if value.len() > MAX_PROTOCOL_STRING_BYTES || has_control_char(value) {
        return Err(invalid_job_field(field));
    }
    Ok(value)
}

fn parse_u64_field(state: &HashMap<String, String>, field: &'static str) -> WorkflowResult<u64> {
    required_field(state, field)?
        .parse::<u64>()
        .map_err(|_| WorkflowError::invalid_state(format!("DO alarm job has invalid {field}")))
}

fn parse_positive_i64_field(
    state: &HashMap<String, String>,
    field: &'static str,
) -> WorkflowResult<i64> {
    let value = required_field(state, field)?
        .parse::<i64>()
        .map_err(|_| WorkflowError::invalid_state(format!("DO alarm job has invalid {field}")))?;
    if value <= 0 {
        return Err(WorkflowError::invalid_state(format!(
            "DO alarm job has invalid {field}"
        )));
    }
    Ok(value)
}

pub(super) fn job_from_state(
    job_id: String,
    state: HashMap<String, String>,
) -> WorkflowResult<DoAlarmJob> {
    parse_positive_i64_field(&state, "dueAtMs")?;
    Ok(DoAlarmJob {
        job_id,
        ns: protocol_string_field(&state, "ns", is_valid_runtime_load_ns)?.to_string(),
        worker: protocol_string_field(&state, "worker", is_valid_worker_name)?.to_string(),
        version: protocol_string_field(&state, "scheduledVersion", |value| {
            parse_version_tag(value).is_ok()
        })?
        .to_string(),
        do_storage_id: protocol_string_field(&state, "doStorageId", is_storage_id)?.to_string(),
        class_name: protocol_string_field(&state, "className", is_class_name)?.to_string(),
        object_name: object_name_field(&state, "objectName")?.to_string(),
        row_token: required_field(&state, "rowToken")?.to_string(),
        run_token: required_field(&state, "runToken")?.to_string(),
        retry_count: parse_u64_field(&state, "retryCount")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_job_state() -> HashMap<String, String> {
        HashMap::from([
            ("ns".to_string(), "demo".to_string()),
            ("worker".to_string(), "alarms".to_string()),
            ("scheduledVersion".to_string(), "v1".to_string()),
            ("doStorageId".to_string(), "do_abc".to_string()),
            ("className".to_string(), "AlarmCounter".to_string()),
            ("objectName".to_string(), "alpha".to_string()),
            ("rowToken".to_string(), "row-token".to_string()),
            ("runToken".to_string(), "run-token".to_string()),
            ("retryCount".to_string(), "0".to_string()),
            ("dueAtMs".to_string(), "123456789".to_string()),
        ])
    }

    #[test]
    fn do_alarm_job_state_requires_valid_due_time() {
        for due_at_ms in [None, Some(""), Some("bad"), Some("0"), Some("-1")] {
            let mut state = valid_job_state();
            if let Some(due_at_ms) = due_at_ms {
                state.insert("dueAtMs".to_string(), due_at_ms.to_string());
            } else {
                state.remove("dueAtMs");
            }
            let err = match job_from_state("job".to_string(), state) {
                Ok(_) => panic!("invalid dueAtMs should reject DO alarm job state"),
                Err(err) => err,
            };
            assert_eq!(err.code, "workflow_invalid_state");
            assert!(
                err.message.contains("dueAtMs"),
                "unexpected error message: {}",
                err.message
            );
        }
    }

    #[test]
    fn do_alarm_job_state_rejects_malformed_dispatch_identity() {
        for (field, value) in [
            ("ns", ""),
            ("ns", "Demo"),
            ("ns", "admin"),
            ("ns", "__community__"),
            ("ns", "____"),
            ("worker", "-bad"),
            ("scheduledVersion", "1"),
            ("scheduledVersion", "v0"),
            ("scheduledVersion", "v01"),
            ("doStorageId", "DO_ABC"),
            ("className", "bad-name"),
            ("objectName", "bad\nname"),
        ] {
            let mut state = valid_job_state();
            state.insert(field.to_string(), value.to_string());
            let err = match job_from_state("job".to_string(), state) {
                Ok(_) => panic!("{field} should reject malformed DO alarm job state"),
                Err(err) => err,
            };
            assert_eq!(err.code, "workflow_invalid_state");
            assert!(
                err.message.contains(field),
                "unexpected error message: {}",
                err.message
            );
        }
    }
}
