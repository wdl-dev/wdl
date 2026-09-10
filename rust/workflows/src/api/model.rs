use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use super::StepHistory;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WorkflowRequest {
    pub(crate) ns: String,
    pub(crate) worker: String,
    pub(crate) frozen_version: String,
    pub(crate) workflow_name: String,
    pub(crate) workflow_key: String,
    pub(crate) class_name: String,
    pub(crate) instance_id: Option<String>,
    #[serde(default)]
    pub(crate) params: JsonValue,
    #[serde(default)]
    pub(crate) event: JsonValue,
    #[serde(default)]
    pub(crate) options: JsonValue,
    #[serde(default)]
    pub(crate) retention: JsonValue,
    #[serde(default)]
    pub(crate) callback: JsonValue,
    #[serde(default)]
    pub(crate) entries: Vec<CreateBatchEntry>,
    #[serde(default)]
    pub(crate) request_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CreateBatchEntry {
    pub(crate) instance_id: String,
    #[serde(default)]
    pub(crate) params: JsonValue,
    #[serde(default)]
    pub(crate) retention: JsonValue,
    #[serde(default)]
    pub(crate) callback: JsonValue,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WorkflowDef {
    pub(crate) workflow_key: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InstanceResponse {
    pub(crate) id: String,
    pub(crate) status: String,
    pub(crate) output: Option<JsonValue>,
    pub(crate) error: Option<JsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) steps: Option<StepHistory>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CreateBatchResponse {
    pub(crate) instances: Vec<InstanceResponse>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListInstancesResponse {
    pub(crate) instances: Vec<Box<serde_json::value::RawValue>>,
    pub(crate) cursor: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LifecycleCheckRequest {
    pub(crate) ns: String,
    pub(crate) worker: String,
    pub(crate) version: Option<String>,
    #[serde(default)]
    pub(crate) allow_cleanup: bool,
    #[serde(default)]
    pub(crate) cursor: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LifecycleCheckResponse {
    pub(crate) allowed: bool,
    pub(crate) count: usize,
    pub(crate) blockers: Vec<LifecycleBlocker>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) cursor: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LifecycleBlocker {
    pub(crate) workflow_key: String,
    pub(crate) instance_id: String,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct EventRecord {
    pub(crate) id: String,
    #[serde(rename = "type")]
    pub(crate) event_type: String,
    pub(crate) payload_ref: String,
    pub(crate) consumed_by_ordinal: Option<u32>,
    pub(crate) created_at_ms: i64,
}

#[derive(Clone)]
pub(crate) struct InstanceIdentity {
    pub(crate) ns: String,
    pub(crate) worker: String,
    pub(crate) frozen_version: String,
    pub(crate) workflow_name: String,
    pub(crate) workflow_key: String,
    pub(crate) class_name: String,
    pub(crate) instance_id: String,
    pub(crate) generation: String,
    pub(crate) created_at_ms: String,
}
