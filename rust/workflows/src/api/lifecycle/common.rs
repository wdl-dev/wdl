use std::collections::HashMap;

use crate::{AppState, InstanceIdentity, WorkflowError, WorkflowResult};

use super::super::{
    InstanceResponse, identity_from_state, log_instance_event, parse_positive_identity_i64,
    read_public_state_by_id, response_from_state, spawn_progress_from_identity,
};

pub(super) struct TransitionSignal {
    pub(super) outcome: &'static str,
    pub(super) event: &'static str,
    pub(super) progress_status: &'static str,
}

pub(super) fn next_generation(identity: &InstanceIdentity) -> WorkflowResult<String> {
    let generation = parse_positive_identity_i64(&identity.generation, "generation")?;
    Ok(generation.saturating_add(1).to_string())
}

pub(super) async fn stale_transition_response(
    state: &AppState,
    identity: &InstanceIdentity,
    id: &str,
) -> WorkflowResult<InstanceResponse> {
    let current = read_public_state_by_id(state, &identity.ns, &identity.workflow_key, id).await?;
    if current.is_empty() {
        return Err(WorkflowError::not_found("Workflow instance not found"));
    }
    response_from_state(state, &identity.ns, &identity.workflow_key, id, &current).await
}

pub(super) async fn successful_transition_response(
    state: &AppState,
    previous_identity: &InstanceIdentity,
    id: &str,
    existing: &HashMap<String, String>,
    signal: TransitionSignal,
) -> WorkflowResult<InstanceResponse> {
    let updated_identity = identity_from_state(
        &previous_identity.ns,
        &previous_identity.workflow_key,
        id,
        existing,
    )?;
    state
        .metrics
        .increment("workflow_instances", &[("outcome", signal.outcome)], 1.0);
    log_instance_event(state, signal.event, &updated_identity);
    spawn_progress_from_identity(
        state,
        &updated_identity,
        signal.event,
        signal.progress_status,
        None,
    );
    response_from_state(
        state,
        &updated_identity.ns,
        &updated_identity.workflow_key,
        id,
        existing,
    )
    .await
}
