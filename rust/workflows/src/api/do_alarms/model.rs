use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use wdl_rust_common::hash::fnv1a32;
use wdl_rust_common::identity::{is_valid_runtime_load_ns, is_valid_worker_name};
use wdl_rust_common::worker_contract::parse_version_tag;

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

pub(super) fn validate_set_request(req: &DoAlarmSetRequest) -> WorkflowResult<()> {
    // The job id is hashed, but the by-worker index embeds ns/worker directly.
    // Validate the complete dispatch identity before either key family is built.
    validate_protocol_request_field(&req.ns, "ns", is_valid_runtime_load_ns)?;
    validate_protocol_request_field(&req.worker, "worker", is_valid_worker_name)?;
    validate_protocol_request_field(&req.version, "version", |value| {
        parse_version_tag(value).is_ok()
    })?;
    validate_protocol_request_field(&req.do_storage_id, "doStorageId", is_storage_id)?;
    validate_protocol_request_field(&req.class_name, "className", is_class_name)?;
    validate_opaque_request_field(&req.object_name, "objectName")?;
    validate_opaque_request_field(&req.token, "token")?;
    validate_alarm_host_id(&req.do_storage_id, &req.class_name, &req.object_name)?;
    if req.scheduled_time <= 0 {
        return Err(WorkflowError::invalid_request(
            "scheduledTime must be a positive Unix millisecond timestamp",
        ));
    }
    Ok(())
}

pub(super) fn validate_delete_request(req: &DoAlarmDeleteRequest) -> WorkflowResult<()> {
    validate_protocol_request_field(&req.ns, "ns", is_valid_runtime_load_ns)?;
    validate_protocol_request_field(&req.worker, "worker", is_valid_worker_name)?;
    validate_protocol_request_field(&req.do_storage_id, "doStorageId", is_storage_id)?;
    validate_protocol_request_field(&req.class_name, "className", is_class_name)?;
    validate_opaque_request_field(&req.object_name, "objectName")?;
    validate_opaque_request_field(&req.token, "token")?;
    validate_alarm_host_id(&req.do_storage_id, &req.class_name, &req.object_name)?;
    Ok(())
}

pub(super) fn validate_cleanup_request(req: &DoAlarmCleanupRequest) -> WorkflowResult<()> {
    validate_protocol_request_field(&req.ns, "ns", is_valid_runtime_load_ns)?;
    validate_protocol_request_field(&req.worker, "worker", is_valid_worker_name)?;
    validate_protocol_request_field(&req.do_storage_id, "doStorageId", is_storage_id)?;
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
const DO_HOST_SHARD_COUNT: u32 = 16;

fn alarm_host_id_len(do_storage_id: &str, class_name: &str, object_name: &str) -> usize {
    let shard = fnv1a32(object_name.as_bytes()) % DO_HOST_SHARD_COUNT;
    do_storage_id.len() + class_name.len() + ":".len() + ":shard".len() + shard.to_string().len()
}

fn validate_alarm_host_id(
    do_storage_id: &str,
    class_name: &str,
    object_name: &str,
) -> WorkflowResult<()> {
    if alarm_host_id_len(do_storage_id, class_name, object_name) > MAX_PROTOCOL_STRING_BYTES {
        return Err(WorkflowError::invalid_request("hostId is invalid"));
    }
    Ok(())
}

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

fn is_protocol_string(value: &str, valid: impl Fn(&str) -> bool) -> bool {
    !value.is_empty()
        && value.len() <= MAX_PROTOCOL_STRING_BYTES
        && !has_control_char(value)
        && valid(value)
}

fn validate_protocol_request_field(
    value: &str,
    field: &'static str,
    valid: impl Fn(&str) -> bool,
) -> WorkflowResult<()> {
    if !is_protocol_string(value, valid) {
        return Err(WorkflowError::invalid_request(format!(
            "{field} is invalid"
        )));
    }
    Ok(())
}

fn validate_opaque_request_field(value: &str, field: &'static str) -> WorkflowResult<()> {
    validate_protocol_request_field(value, field, |_| true)
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
    if !is_protocol_string(value, valid) {
        return Err(invalid_job_field(field));
    }
    Ok(value)
}

fn opaque_string_field<'a>(
    state: &'a HashMap<String, String>,
    field: &'static str,
) -> WorkflowResult<&'a str> {
    let value = required_field(state, field)?;
    if !is_protocol_string(value, |_| true) {
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
    let ns = protocol_string_field(&state, "ns", is_valid_runtime_load_ns)?;
    let worker = protocol_string_field(&state, "worker", is_valid_worker_name)?;
    let version = protocol_string_field(&state, "scheduledVersion", |value| {
        parse_version_tag(value).is_ok()
    })?;
    let do_storage_id = protocol_string_field(&state, "doStorageId", is_storage_id)?;
    let class_name = protocol_string_field(&state, "className", is_class_name)?;
    let object_name = opaque_string_field(&state, "objectName")?;
    if alarm_host_id_len(do_storage_id, class_name, object_name) > MAX_PROTOCOL_STRING_BYTES {
        return Err(invalid_job_field("hostId"));
    }
    if job_id != do_alarm_job_id(ns, worker, do_storage_id, class_name, object_name) {
        return Err(invalid_job_field("jobId"));
    }
    Ok(DoAlarmJob {
        job_id,
        ns: ns.to_string(),
        worker: worker.to_string(),
        version: version.to_string(),
        do_storage_id: do_storage_id.to_string(),
        class_name: class_name.to_string(),
        object_name: object_name.to_string(),
        row_token: opaque_string_field(&state, "rowToken")?.to_string(),
        run_token: opaque_string_field(&state, "runToken")?.to_string(),
        retry_count: parse_u64_field(&state, "retryCount")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_set_request() -> DoAlarmSetRequest {
        DoAlarmSetRequest {
            ns: "demo".to_string(),
            worker: "alarms".to_string(),
            version: "v1".to_string(),
            do_storage_id: "do_abc".to_string(),
            class_name: "AlarmCounter".to_string(),
            object_name: "alpha".to_string(),
            scheduled_time: 123_456_789,
            retry_count: 0,
            token: "row-token".to_string(),
        }
    }

    fn valid_delete_request() -> DoAlarmDeleteRequest {
        DoAlarmDeleteRequest {
            ns: "demo".to_string(),
            worker: "alarms".to_string(),
            do_storage_id: "do_abc".to_string(),
            class_name: "AlarmCounter".to_string(),
            object_name: "alpha".to_string(),
            token: "row-token".to_string(),
        }
    }

    fn fixture_string(value: &serde_json::Value) -> String {
        if let Some(value) = value.as_str() {
            return value.to_string();
        }
        value["repeat"]
            .as_str()
            .expect("fixture repeat is a string")
            .repeat(
                value["count"]
                    .as_u64()
                    .expect("fixture count is an integer") as usize,
            )
    }

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

    fn job_id_for_state(state: &HashMap<String, String>) -> String {
        do_alarm_job_id(
            state.get("ns").map(String::as_str).unwrap_or_default(),
            state.get("worker").map(String::as_str).unwrap_or_default(),
            state
                .get("doStorageId")
                .map(String::as_str)
                .unwrap_or_default(),
            state
                .get("className")
                .map(String::as_str)
                .unwrap_or_default(),
            state
                .get("objectName")
                .map(String::as_str)
                .unwrap_or_default(),
        )
    }

    #[test]
    fn do_alarm_mutation_ingress_rejects_noncanonical_identity() {
        for (field, value) in [
            ("ns", "Demo"),
            ("worker", "bad:worker"),
            ("version", "v01"),
            ("doStorageId", "DO_ABC"),
            ("className", "bad-name"),
            ("objectName", "bad\nname"),
            ("token", "bad\ntoken"),
        ] {
            let mut req = valid_set_request();
            match field {
                "ns" => req.ns = value.to_string(),
                "worker" => req.worker = value.to_string(),
                "version" => req.version = value.to_string(),
                "doStorageId" => req.do_storage_id = value.to_string(),
                "className" => req.class_name = value.to_string(),
                "objectName" => req.object_name = value.to_string(),
                "token" => req.token = value.to_string(),
                _ => unreachable!(),
            }
            let err = validate_set_request(&req).expect_err("invalid set request");
            assert_eq!(err.code, "invalid_request");
            assert!(err.message.contains(field), "{}", err.message);
        }

        let delete = DoAlarmDeleteRequest {
            ns: "bad:ns".to_string(),
            worker: "alarms".to_string(),
            do_storage_id: "do_abc".to_string(),
            class_name: "AlarmCounter".to_string(),
            object_name: "alpha".to_string(),
            token: "row-token".to_string(),
        };
        assert!(validate_delete_request(&delete).is_err());

        let cleanup = DoAlarmCleanupRequest {
            ns: "demo".to_string(),
            worker: "alarms".to_string(),
            do_storage_id: "bad/storage".to_string(),
        };
        assert!(validate_cleanup_request(&cleanup).is_err());
    }

    #[test]
    fn do_alarm_protocol_fields_match_cross_language_fixture() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/do-alarm-identity.json"
        ))
        .expect("DO alarm identity fixture parses");
        assert_eq!(
            fixture["maxBytes"].as_u64(),
            Some(MAX_PROTOCOL_STRING_BYTES as u64)
        );
        assert_eq!(
            fixture["hostShardCount"].as_u64(),
            Some(DO_HOST_SHARD_COUNT as u64)
        );
        for case in fixture["cases"]
            .as_array()
            .expect("DO alarm identity fixture cases is an array")
        {
            let do_storage_id = fixture_string(&case["doStorageId"]);
            let class_name = fixture_string(&case["className"]);
            let object_name = fixture_string(&case["objectName"]);
            let token = fixture_string(&case["token"]);
            let mut set = valid_set_request();
            set.do_storage_id = do_storage_id.clone();
            set.class_name = class_name.clone();
            set.object_name = object_name.clone();
            set.token = token.clone();
            let mut delete = valid_delete_request();
            delete.do_storage_id = do_storage_id;
            delete.class_name = class_name;
            delete.object_name = object_name;
            delete.token = token;
            let valid = case["valid"].as_bool().expect("valid is a boolean");
            assert_eq!(
                validate_set_request(&set).is_ok(),
                valid,
                "set:{}",
                case["name"].as_str().expect("name is a string")
            );
            assert_eq!(
                validate_delete_request(&delete).is_ok(),
                valid,
                "delete:{}",
                case["name"].as_str().expect("name is a string")
            );
            let mut state = valid_job_state();
            state.insert("doStorageId".to_string(), set.do_storage_id.clone());
            state.insert("className".to_string(), set.class_name.clone());
            state.insert("objectName".to_string(), set.object_name.clone());
            state.insert("rowToken".to_string(), set.token.clone());
            let job_id = job_id_for_state(&state);
            assert_eq!(
                job_from_state(job_id, state).is_ok(),
                valid,
                "persisted:{}",
                case["name"].as_str().expect("name is a string")
            );
        }
    }

    #[test]
    fn do_alarm_job_state_requires_canonical_job_id() {
        let state = valid_job_state();
        let canonical = job_id_for_state(&state);
        assert!(job_from_state(canonical, state.clone()).is_ok());

        let mut other_state = state.clone();
        other_state.insert("objectName".to_string(), "other-object".to_string());
        let other_canonical = job_id_for_state(&other_state);
        assert_ne!(other_canonical, job_id_for_state(&state));

        let err = match job_from_state(other_canonical, state) {
            Ok(_) => panic!("mis-keyed DO alarm state must fail closed"),
            Err(err) => err,
        };
        assert_eq!(err.code, "workflow_invalid_state");
        assert!(err.message.contains("jobId"));
    }

    #[test]
    fn do_alarm_json_rejects_unpaired_surrogate_object_names() {
        let request = r#"{
            "ns":"demo",
            "worker":"alarms",
            "version":"v1",
            "doStorageId":"do_abc",
            "className":"AlarmCounter",
            "objectName":"\ud800",
            "scheduledTime":123456789,
            "retryCount":0,
            "token":"row-token"
        }"#;
        assert!(serde_json::from_str::<DoAlarmSetRequest>(request).is_err());
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

    #[test]
    fn do_alarm_set_version_matches_cross_language_fixture() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/version-tags.json"
        ))
        .expect("version tag fixture parses");
        for case in fixture["cases"]
            .as_array()
            .expect("version tag fixture cases is an array")
        {
            let tag = case["tag"].as_str().expect("version tag is a string");
            let mut req = valid_set_request();
            req.version = tag.to_string();
            assert_eq!(
                validate_set_request(&req).is_ok(),
                case["parsed"].as_u64().is_some(),
                "{tag:?}"
            );
        }
    }
}
