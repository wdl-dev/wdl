pub(crate) const MAX_WORKFLOW_JSON_BODY_BYTES: usize = 2 * 1024 * 1024;
pub(crate) const MAX_WORKFLOW_PARAMS_BYTES: usize = 1024 * 1024;
pub(crate) const MAX_WORKFLOW_RESULT_BYTES: usize = 1024 * 1024;
pub(crate) const WORKFLOW_PAYLOAD_TOO_LARGE_CODE: &str = "workflow_payload_too_large";
pub(crate) const MAX_WORKFLOW_RUNTIME_RESPONSE_BYTES: usize = MAX_WORKFLOW_RESULT_BYTES + 16 * 1024;
pub(crate) const MAX_WORKFLOW_EVENT_BYTES: usize = 256 * 1024;
pub(crate) const MAX_WORKFLOW_EVENT_TYPE_BYTES: usize = 512;
pub(crate) const MAX_WORKFLOW_INSTANCE_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_WORKFLOW_STEP_NAME_BYTES: usize = 512;
pub(crate) const MAX_WORKFLOW_STEP_CONFIG_BYTES: usize = 64 * 1024;
pub(crate) const MAX_CREATE_BATCH_SIZE: usize = 100;
pub(crate) const READY_SHARDS: usize = crate::WORKFLOW_READY_SHARDS;
pub(crate) const LIFECYCLE_BLOCKER_LIMIT: usize = 20;

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::{
        MAX_CREATE_BATCH_SIZE, MAX_WORKFLOW_JSON_BODY_BYTES, MAX_WORKFLOW_RESULT_BYTES,
        WORKFLOW_PAYLOAD_TOO_LARGE_CODE,
    };

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct WorkflowLimitsContract {
        result_bytes_max: usize,
        backend_request_bytes_max: usize,
        create_batch_max: usize,
        payload_too_large_code: String,
    }

    #[test]
    fn workflow_limits_match_runtime_fixture() {
        let contract: WorkflowLimitsContract = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/workflow-limits.json"
        ))
        .expect("workflow limits fixture");

        assert_eq!(contract.result_bytes_max, MAX_WORKFLOW_RESULT_BYTES);
        assert_eq!(
            contract.backend_request_bytes_max,
            MAX_WORKFLOW_JSON_BODY_BYTES
        );
        assert_eq!(contract.create_batch_max, MAX_CREATE_BATCH_SIZE);
        assert_eq!(
            contract.payload_too_large_code,
            WORKFLOW_PAYLOAD_TOO_LARGE_CODE
        );
    }
}
