use axum::body::Body;
use serde::{
    Deserialize, Serialize, Serializer,
    ser::{SerializeMap, SerializeSeq},
};
use serde_json::{Value as JsonValue, json};

use crate::{AppState, LogLevel, WorkflowError, WorkflowResult, log};

use super::super::{
    MAX_WORKFLOW_STEP_CONFIG_BYTES, MAX_WORKFLOW_STEP_NAME_BYTES, read_json_request,
    require_non_empty,
};

// Keep inline step payloads small so step records stay cheap to replay and
// summaries/indexes never become a backdoor for large tenant payload storage.
pub(super) const INLINE_STEP_PAYLOAD_BYTES_MAX: usize = 2 * 1024;
const MAX_STEP_DEPENDENCIES: usize = 1000;
pub(super) const STEP_KIND_DO: &str = "do";
pub(super) const STEP_KIND_SLEEP: &str = "sleep";
pub(super) const STEP_KIND_SLEEP_UNTIL: &str = "sleepUntil";
pub(super) const STEP_KIND_WAIT_FOR_EVENT: &str = "waitForEvent";

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WorkflowStepRequest {
    pub(crate) ns: String,
    pub(crate) worker: String,
    pub(crate) frozen_version: String,
    pub(crate) workflow_name: String,
    pub(crate) workflow_key: String,
    pub(crate) class_name: String,
    pub(crate) instance_id: String,
    pub(crate) generation: i64,
    pub(crate) created_at_ms: i64,
    pub(crate) run_token: String,
    pub(crate) ordinal: u32,
    pub(crate) step_name: String,
    pub(crate) name_count: u32,
    pub(super) dependencies: Vec<u32>,
    #[serde(default)]
    pub(super) attempt: Option<u32>,
    #[serde(default)]
    pub(super) non_retryable: bool,
    #[serde(default)]
    pub(super) started_at_ms: Option<i64>,
    #[serde(default)]
    pub(super) config: JsonValue,
    #[serde(default)]
    pub(super) output: JsonValue,
    #[serde(default)]
    pub(super) error: JsonValue,
    #[serde(default)]
    pub(super) due_at_ms: Option<i64>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct StepRecord {
    pub(super) ordinal: u32,
    pub(super) step_name: String,
    pub(super) name_count: u32,
    pub(super) dependencies: Vec<u32>,
    pub(super) kind: String,
    pub(super) config: String,
    pub(super) status: String,
    #[serde(default)]
    pub(super) attempt: u32,
    pub(super) output_ref: Option<String>,
    pub(super) error_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) output: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) error: Option<JsonValue>,
    pub(super) completed_at_ms: Option<i64>,
    pub(super) failed_at_ms: Option<i64>,
    pub(super) due_at_ms: Option<i64>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct StepSummary {
    pub(super) ordinal: u32,
    pub(super) name: String,
    pub(super) name_count: u32,
    pub(super) dependencies: Vec<u32>,
    pub(super) status: String,
    pub(super) attempt: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) output_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) error_ref: Option<String>,
    #[serde(default)]
    pub(super) has_output: bool,
    #[serde(default)]
    pub(super) has_error: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) completed_at_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) failed_at_ms: Option<i64>,
}

pub(crate) async fn read_workflow_step_request(body: Body) -> WorkflowResult<WorkflowStepRequest> {
    read_json_request(body, "Invalid step request JSON").await
}

pub(super) fn step_field(ordinal: u32) -> String {
    ordinal.to_string()
}

pub(super) fn step_output_ref(ordinal: u32) -> String {
    format!("step:{ordinal}:output")
}

pub(super) fn step_error_ref(ordinal: u32) -> String {
    format!("step:{ordinal}:error")
}

pub(super) fn step_event_ref(ordinal: u32) -> String {
    format!("step:{ordinal}:event")
}

pub(super) fn inline_step_payload(value: &JsonValue, json: &str) -> Option<JsonValue> {
    if value.is_null() {
        return None;
    }
    (json.len() <= INLINE_STEP_PAYLOAD_BYTES_MAX).then(|| value.clone())
}

pub(super) fn step_record_json(record: &StepRecord) -> WorkflowResult<String> {
    serde_json::to_string(record).map_err(|err| {
        WorkflowError::internal_error(format!("Workflow step serialization failed: {err}"))
    })
}

pub(super) fn step_summary_json(record: &StepRecord) -> WorkflowResult<String> {
    let summary = StepSummary {
        ordinal: record.ordinal,
        name: record.step_name.clone(),
        name_count: record.name_count,
        dependencies: record.dependencies.clone(),
        status: record.status.clone(),
        attempt: record.attempt.max(1),
        output_ref: record.output_ref.clone(),
        error_ref: record.error_ref.clone(),
        has_output: record.output_ref.is_some() || record.output.is_some(),
        has_error: record.error_ref.is_some() || record.error.is_some(),
        completed_at_ms: record.completed_at_ms,
        failed_at_ms: record.failed_at_ms,
    };
    serde_json::to_string(&summary).map_err(|err| {
        WorkflowError::internal_error(format!("Workflow step summary serialization failed: {err}"))
    })
}

struct CanonicalJson<'a>(&'a JsonValue);

impl Serialize for CanonicalJson<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.0 {
            JsonValue::Array(values) => {
                let mut sequence = serializer.serialize_seq(Some(values.len()))?;
                for value in values {
                    sequence.serialize_element(&CanonicalJson(value))?;
                }
                sequence.end()
            }
            JsonValue::Object(map) => {
                let mut keys = map.keys().collect::<Vec<_>>();
                keys.sort();
                let mut entries = serializer.serialize_map(Some(map.len()))?;
                for key in keys {
                    entries.serialize_entry(key, &CanonicalJson(&map[key]))?;
                }
                entries.end()
            }
            value => value.serialize(serializer),
        }
    }
}

pub(crate) fn canonical_json(value: &JsonValue) -> WorkflowResult<String> {
    serde_json::to_string(&CanonicalJson(value))
        .map_err(|err| WorkflowError::invalid_request(format!("Invalid JSON value: {err}")))
}

pub(super) fn request_attempt(req: &WorkflowStepRequest) -> WorkflowResult<u32> {
    let attempt = req.attempt.unwrap_or(1);
    if attempt == 0 {
        return Err(WorkflowError::invalid_request(
            "attempt must be a positive integer",
        ));
    }
    Ok(attempt)
}

pub(super) fn observe_step_duration(
    state: &AppState,
    req: &WorkflowStepRequest,
    completed_at_ms: i64,
) {
    if let Some(started_at_ms) = req.started_at_ms {
        let duration_ms = completed_at_ms.saturating_sub(started_at_ms).max(0) as f64;
        state
            .metrics
            .observe("workflow_step_duration_ms", &[], duration_ms);
    }
}

pub(super) fn log_step_event(
    state: &AppState,
    event: &str,
    req: &WorkflowStepRequest,
    attempt: u32,
) {
    log(
        state,
        LogLevel::Info,
        event,
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
            "attempt": attempt,
        }),
    );
}

pub(super) fn validate_step_request(req: &WorkflowStepRequest) -> WorkflowResult<String> {
    require_non_empty(&req.ns, "ns")?;
    require_non_empty(&req.worker, "worker")?;
    require_non_empty(&req.frozen_version, "frozenVersion")?;
    require_non_empty(&req.workflow_name, "workflowName")?;
    require_non_empty(&req.workflow_key, "workflowKey")?;
    require_non_empty(&req.class_name, "className")?;
    require_non_empty(&req.instance_id, "instanceId")?;
    require_non_empty(&req.step_name, "stepName")?;
    if req.step_name.len() > MAX_WORKFLOW_STEP_NAME_BYTES {
        return Err(WorkflowError::request_too_large(format!(
            "Workflow stepName exceeds the {MAX_WORKFLOW_STEP_NAME_BYTES} byte limit"
        )));
    }
    if req.generation <= 0 {
        return Err(WorkflowError::invalid_request(
            "generation must be a positive integer",
        ));
    }
    if req.created_at_ms <= 0 {
        return Err(WorkflowError::invalid_request(
            "createdAtMs must be a positive integer",
        ));
    }
    require_non_empty(&req.run_token, "runToken")?;
    if req.name_count == 0 {
        return Err(WorkflowError::invalid_request(
            "nameCount must be a positive integer",
        ));
    }
    if req.dependencies.len() > MAX_STEP_DEPENDENCIES {
        return Err(WorkflowError::request_too_large(format!(
            "Workflow step dependencies exceed the {MAX_STEP_DEPENDENCIES} edge limit"
        )));
    }
    let mut previous = None;
    for dependency in &req.dependencies {
        if *dependency >= req.ordinal {
            return Err(WorkflowError::invalid_request(
                "workflow step dependencies must reference prior ordinals",
            ));
        }
        if previous.is_some_and(|previous| *dependency <= previous) {
            return Err(WorkflowError::invalid_request(
                "workflow step dependencies must be unique and sorted",
            ));
        }
        previous = Some(*dependency);
    }
    let config = canonical_json(&req.config)?;
    if config.len() > MAX_WORKFLOW_STEP_CONFIG_BYTES {
        return Err(WorkflowError::request_too_large(format!(
            "Workflow step config exceeds the {MAX_WORKFLOW_STEP_CONFIG_BYTES} byte limit"
        )));
    }
    Ok(config)
}

pub(super) fn verify_step_record(
    req: &WorkflowStepRequest,
    config: &str,
    record: &StepRecord,
    expected_kind: &str,
) -> WorkflowResult<()> {
    if record.ordinal != req.ordinal
        || record.step_name != req.step_name
        || record.name_count != req.name_count
        || record.dependencies != req.dependencies
        || record.kind.as_str() != expected_kind
        || record.config != config
    {
        return Err(WorkflowError::step_mismatch(
            "Workflow step replay does not match stored step",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn canonical_json_matches_cross_language_fixture() {
        let fixture: JsonValue = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/workflow-canonical-json-parity.json"
        ))
        .expect("canonical JSON fixture parses");
        let cases = fixture
            .as_array()
            .expect("canonical JSON fixture is an array");
        for entry in cases {
            let id = entry["id"].as_str().expect("fixture id is a string");
            let raw_json = entry["rawJson"]
                .as_str()
                .expect("fixture rawJson is a string");
            let expected = entry["rustExpected"]
                .as_str()
                .expect("fixture rustExpected is a string");
            let value: JsonValue = serde_json::from_str(raw_json).expect("fixture rawJson parses");
            assert_eq!(
                canonical_json(&value).expect("canonical JSON"),
                expected,
                "{id}"
            );
        }
    }

    #[test]
    fn inline_step_payload_keeps_only_small_json_values() {
        let small = json!({ "ok": true });
        assert_eq!(inline_step_payload(&small, r#"{"ok":true}"#), Some(small));

        assert_eq!(inline_step_payload(&JsonValue::Null, "null"), None);

        let large = JsonValue::String("x".repeat(INLINE_STEP_PAYLOAD_BYTES_MAX + 1));
        let large_json = serde_json::to_string(&large).unwrap();
        assert_eq!(inline_step_payload(&large, &large_json), None);
    }

    #[test]
    fn step_summary_preserves_inline_payload_presence() {
        let record = StepRecord {
            ordinal: 7,
            step_name: "tiny".to_string(),
            name_count: 1,
            dependencies: vec![3, 5],
            kind: STEP_KIND_DO.to_string(),
            config: "null".to_string(),
            status: "completed".to_string(),
            attempt: 1,
            output_ref: None,
            error_ref: None,
            output: Some(json!({ "small": true })),
            error: None,
            completed_at_ms: Some(123),
            failed_at_ms: None,
            due_at_ms: None,
        };
        let summary: StepSummary =
            serde_json::from_str(&step_summary_json(&record).unwrap()).unwrap();
        assert!(summary.has_output);
        assert!(!summary.has_error);
        assert_eq!(summary.output_ref, None);
        assert_eq!(summary.dependencies, vec![3, 5]);
    }

    #[test]
    fn step_request_caps_tenant_controlled_metadata() {
        let mut req = WorkflowStepRequest {
            ns: "demo".to_string(),
            worker: "worker".to_string(),
            frozen_version: "v1".to_string(),
            workflow_name: "orders".to_string(),
            workflow_key: "wf_test".to_string(),
            class_name: "OrderWorkflow".to_string(),
            instance_id: "order-1".to_string(),
            generation: 1,
            created_at_ms: 123,
            run_token: "token".to_string(),
            ordinal: 0,
            step_name: "x".to_string(),
            name_count: 1,
            dependencies: Vec::new(),
            attempt: None,
            non_retryable: false,
            started_at_ms: None,
            config: JsonValue::Null,
            output: JsonValue::Null,
            error: JsonValue::Null,
            due_at_ms: None,
        };

        req.step_name = "x".repeat(MAX_WORKFLOW_STEP_NAME_BYTES + 1);
        let err = validate_step_request(&req).expect_err("oversized step name should fail");
        assert_eq!(err.code, "request_too_large");

        req.step_name = "x".to_string();
        req.config = JsonValue::String("x".repeat(MAX_WORKFLOW_STEP_CONFIG_BYTES + 1));
        let err = validate_step_request(&req).expect_err("oversized step config should fail");
        assert_eq!(err.code, "request_too_large");

        req.config = JsonValue::Null;
        req.created_at_ms = 0;
        let err = validate_step_request(&req).expect_err("missing creation time should fail");
        assert_eq!(err.code, "invalid_request");
    }

    #[test]
    fn step_dependencies_must_be_prior_sorted_ordinals() {
        let mut req = WorkflowStepRequest {
            ns: "demo".to_string(),
            worker: "worker".to_string(),
            frozen_version: "v1".to_string(),
            workflow_name: "orders".to_string(),
            workflow_key: "wf_test".to_string(),
            class_name: "OrderWorkflow".to_string(),
            instance_id: "order-1".to_string(),
            generation: 1,
            created_at_ms: 123,
            run_token: "token".to_string(),
            ordinal: 3,
            step_name: "join".to_string(),
            name_count: 1,
            dependencies: vec![0, 2],
            attempt: None,
            non_retryable: false,
            started_at_ms: None,
            config: JsonValue::Null,
            output: JsonValue::Null,
            error: JsonValue::Null,
            due_at_ms: None,
        };
        assert!(validate_step_request(&req).is_ok());

        req.dependencies = vec![2, 2];
        let err = validate_step_request(&req).expect_err("duplicate dependency should fail");
        assert_eq!(err.code, "invalid_request");

        req.dependencies = vec![2, 1];
        let err = validate_step_request(&req).expect_err("unsorted dependency should fail");
        assert_eq!(err.code, "invalid_request");

        req.dependencies = vec![3];
        let err = validate_step_request(&req).expect_err("self dependency should fail");
        assert_eq!(err.code, "invalid_request");
    }

    #[test]
    fn step_request_requires_dependency_shape() {
        let value = json!({
            "ns": "demo",
            "worker": "worker",
            "frozenVersion": "v1",
            "workflowName": "orders",
            "workflowKey": "wf_test",
            "className": "OrderWorkflow",
            "instanceId": "order-1",
            "generation": 1,
            "createdAtMs": 123,
            "runToken": "token",
            "ordinal": 0,
            "stepName": "root",
            "nameCount": 1
        });
        let Err(err) = serde_json::from_value::<WorkflowStepRequest>(value) else {
            panic!("missing dependencies must fail closed");
        };
        assert!(
            err.to_string().contains("missing field `dependencies`"),
            "unexpected serde error: {err}"
        );
    }

    #[test]
    fn stored_step_records_require_identity_shape() {
        let record = json!({
            "ordinal": 0,
            "stepName": "root",
            "nameCount": 1,
            "kind": STEP_KIND_DO,
            "config": "null",
            "status": "completed",
            "attempt": 1,
            "outputRef": null,
            "errorRef": null,
            "completedAtMs": 123,
            "failedAtMs": null,
            "dueAtMs": null
        });
        let Err(record_err) = serde_json::from_value::<StepRecord>(record) else {
            panic!("missing step record dependencies must fail closed");
        };
        assert!(
            record_err
                .to_string()
                .contains("missing field `dependencies`"),
            "unexpected serde error: {record_err}"
        );

        let record = json!({
            "ordinal": 0,
            "stepName": "root",
            "nameCount": 1,
            "dependencies": [],
            "config": "null",
            "status": "completed",
            "attempt": 1,
            "outputRef": null,
            "errorRef": null,
            "completedAtMs": 123,
            "failedAtMs": null,
            "dueAtMs": null
        });
        let Err(record_err) = serde_json::from_value::<StepRecord>(record) else {
            panic!("missing step record kind must fail closed");
        };
        assert!(
            record_err.to_string().contains("missing field `kind`"),
            "unexpected serde error: {record_err}"
        );

        let summary = json!({
            "ordinal": 0,
            "name": "root",
            "nameCount": 1,
            "status": "completed",
            "attempt": 1,
            "outputRef": null,
            "errorRef": null,
            "hasOutput": false,
            "hasError": false,
            "completedAtMs": 123,
            "failedAtMs": null
        });
        let Err(summary_err) = serde_json::from_value::<StepSummary>(summary) else {
            panic!("missing step summary dependencies must fail closed");
        };
        assert!(
            summary_err
                .to_string()
                .contains("missing field `dependencies`"),
            "unexpected serde error: {summary_err}"
        );
    }

    #[test]
    fn step_record_verification_rejects_dependency_shape_drift() {
        let req = WorkflowStepRequest {
            ns: "demo".to_string(),
            worker: "worker".to_string(),
            frozen_version: "v1".to_string(),
            workflow_name: "orders".to_string(),
            workflow_key: "wf_test".to_string(),
            class_name: "OrderWorkflow".to_string(),
            instance_id: "order-1".to_string(),
            generation: 1,
            created_at_ms: 123,
            run_token: "token".to_string(),
            ordinal: 3,
            step_name: "join".to_string(),
            name_count: 1,
            dependencies: vec![0, 2],
            attempt: None,
            non_retryable: false,
            started_at_ms: None,
            config: JsonValue::Null,
            output: JsonValue::Null,
            error: JsonValue::Null,
            due_at_ms: None,
        };
        let config = validate_step_request(&req).expect("valid request");
        let mut record = StepRecord {
            ordinal: 3,
            step_name: "join".to_string(),
            name_count: 1,
            dependencies: vec![0, 1],
            kind: STEP_KIND_DO.to_string(),
            config: config.clone(),
            status: "completed".to_string(),
            attempt: 1,
            output_ref: None,
            error_ref: None,
            output: Some(JsonValue::Null),
            error: None,
            completed_at_ms: Some(123),
            failed_at_ms: None,
            due_at_ms: None,
        };

        let err = verify_step_record(&req, &config, &record, STEP_KIND_DO)
            .expect_err("dependency mismatch should fail replay");
        assert_eq!(err.code, "workflow_step_mismatch");

        record.dependencies = req.dependencies.clone();
        record.kind = STEP_KIND_WAIT_FOR_EVENT.to_string();
        let err = verify_step_record(&req, &config, &record, STEP_KIND_DO)
            .expect_err("mismatched operation kind should fail replay");
        assert_eq!(err.code, "workflow_step_mismatch");
    }
}
