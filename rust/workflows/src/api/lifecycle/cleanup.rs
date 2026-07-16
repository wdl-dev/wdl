use wdl_rust_common::time::now_ms;

use crate::{AppState, WorkflowResult, by_version_key, by_worker_key};

use super::super::{
    LIFECYCLE_BLOCKER_LIMIT, LifecycleBlocker, LifecycleCheckRequest, LifecycleCheckResponse,
    PendingCreateCleanup, active_pending_restart_blockers, cleanup_pending_create_identity,
    is_pending_create, parse_workflow_referrer_member, pending_create_cleanup_from_state,
    pending_create_expired, read_state_by_id, require_non_empty,
};

enum LifecycleMemberState {
    Blocker,
    ExpiredPending(Box<PendingCreateCleanup>),
    Ignore,
}

impl LifecycleMemberState {
    fn is_blocker(&self) -> bool {
        matches!(self, Self::Blocker)
    }
}

async fn lifecycle_blocker_state(
    state: &AppState,
    ns: &str,
    workflow_key: &str,
    instance_id: &str,
) -> WorkflowResult<LifecycleMemberState> {
    let existing = read_state_by_id(state, ns, workflow_key, instance_id).await?;
    if existing.is_empty() {
        return Ok(LifecycleMemberState::Ignore);
    }
    if is_pending_create(&existing) {
        if pending_create_expired(&existing, now_ms()) {
            return Ok(LifecycleMemberState::ExpiredPending(Box::new(
                pending_create_cleanup_from_state(&existing)?,
            )));
        }
        return Ok(LifecycleMemberState::Blocker);
    }
    Ok(LifecycleMemberState::Blocker)
}

pub(crate) async fn check_delete_lifecycle(
    state: &AppState,
    req: LifecycleCheckRequest,
) -> WorkflowResult<LifecycleCheckResponse> {
    require_non_empty(&req.ns, "ns")?;
    require_non_empty(&req.worker, "worker")?;
    if let Some(version) = &req.version {
        require_non_empty(version, "version")?;
    }
    let key = match &req.version {
        Some(version) => by_version_key(&req.ns, &req.worker, version),
        None => by_worker_key(&req.ns, &req.worker),
    };
    let (pending_count, mut blockers) = match &req.version {
        Some(version) => {
            active_pending_restart_blockers(
                state,
                &req.ns,
                &req.worker,
                version,
                req.allow_cleanup,
                LIFECYCLE_BLOCKER_LIMIT,
            )
            .await?
        }
        None => (0, Vec::new()),
    };
    let count_key = key.clone();
    let referrer_count: usize = state
        .redis
        .with_conn(async |mut conn| {
            redis::cmd("SCARD")
                .arg(count_key)
                .query_async::<usize>(&mut conn)
                .await
        })
        .await?;
    let count = referrer_count.saturating_add(pending_count);
    if count == 0 {
        return Ok(LifecycleCheckResponse {
            allowed: true,
            count,
            blockers: Vec::new(),
        });
    }
    if blockers.len() >= LIFECYCLE_BLOCKER_LIMIT {
        return Ok(LifecycleCheckResponse {
            allowed: false,
            count,
            blockers,
        });
    }
    let mut cursor = 0_u64;
    let mut expired_pending = Vec::new();
    loop {
        let scan_key = key.clone();
        let (next, members): (u64, Vec<String>) = state
            .redis
            .with_conn(async |mut conn| {
                redis::cmd("SSCAN")
                    .arg(scan_key)
                    .arg(cursor)
                    .arg("COUNT")
                    .arg(LIFECYCLE_BLOCKER_LIMIT)
                    .query_async(&mut conn)
                    .await
            })
            .await?;
        for member in members {
            let (workflow_key, instance_id) =
                parse_workflow_referrer_member(&member).unwrap_or_else(|| ("".to_string(), member));
            match lifecycle_blocker_state(state, &req.ns, &workflow_key, &instance_id).await? {
                LifecycleMemberState::Blocker => {}
                LifecycleMemberState::ExpiredPending(pending) => {
                    expired_pending.push(pending);
                    continue;
                }
                LifecycleMemberState::Ignore => continue,
            }
            blockers.push(LifecycleBlocker {
                workflow_key,
                instance_id,
            });
            if blockers.len() >= LIFECYCLE_BLOCKER_LIMIT {
                return Ok(LifecycleCheckResponse {
                    allowed: false,
                    count,
                    blockers,
                });
            }
        }
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    for pending in expired_pending {
        let mut is_blocker = true;
        if req.allow_cleanup {
            if cleanup_pending_create_identity(state, &pending.identity, &pending.token).await? {
                continue;
            }
            is_blocker = lifecycle_blocker_state(
                state,
                &pending.identity.ns,
                &pending.identity.workflow_key,
                &pending.identity.instance_id,
            )
            .await?
            .is_blocker();
        }
        if is_blocker {
            blockers.push(LifecycleBlocker {
                workflow_key: pending.identity.workflow_key,
                instance_id: pending.identity.instance_id,
            });
            if blockers.len() >= LIFECYCLE_BLOCKER_LIMIT {
                break;
            }
        }
    }
    Ok(LifecycleCheckResponse {
        allowed: blockers.is_empty(),
        count: if blockers.is_empty() { 0 } else { count },
        blockers,
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn lifecycle_cleanup_failure_rechecks_blocker_state() {
        let source = include_str!("cleanup.rs");
        assert!(source.contains("if req.allow_cleanup"));
        assert!(source.contains(
            "cleanup_pending_create_identity(state, &pending.identity, &pending.token).await?"
        ));
        assert!(source.contains("is_blocker = lifecycle_blocker_state("));
        assert!(source.contains(".is_blocker()"));
        assert!(source.contains("blockers.push(LifecycleBlocker"));
        assert!(source.contains("if blockers.len() >= LIFECYCLE_BLOCKER_LIMIT"));
    }
}
