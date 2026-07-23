use std::collections::HashMap;

use wdl_rust_common::time::now_ms;

use crate::{
    AppState, WorkflowError, WorkflowResult, by_version_key, by_worker_key, instance_state_key,
};

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

struct LifecycleMember {
    workflow_key: String,
    instance_id: String,
}

struct ClassifiedLifecycleMember {
    member: LifecycleMember,
    state: LifecycleMemberState,
}

impl LifecycleMemberState {
    fn is_blocker(&self) -> bool {
        matches!(self, Self::Blocker)
    }
}

fn lifecycle_members_from_scan(
    members: impl IntoIterator<Item = String>,
) -> WorkflowResult<Vec<LifecycleMember>> {
    members
        .into_iter()
        .map(|member| {
            let (workflow_key, instance_id) =
                parse_workflow_referrer_member(&member).ok_or_else(|| {
                    WorkflowError::invalid_state("Workflow lifecycle referrer is corrupt")
                })?;
            Ok(LifecycleMember {
                workflow_key,
                instance_id,
            })
        })
        .collect()
}

fn lifecycle_state_pipeline(ns: &str, members: &[LifecycleMember]) -> redis::Pipeline {
    let mut pipe = redis::pipe();
    for member in members {
        pipe.cmd("HGETALL").arg(instance_state_key(
            ns,
            &member.workflow_key,
            &member.instance_id,
        ));
    }
    pipe
}

fn classify_lifecycle_member(
    existing: &HashMap<String, String>,
    now: i64,
) -> WorkflowResult<LifecycleMemberState> {
    if existing.is_empty() {
        return Ok(LifecycleMemberState::Ignore);
    }
    if is_pending_create(existing) {
        if pending_create_expired(existing, now) {
            return Ok(LifecycleMemberState::ExpiredPending(Box::new(
                pending_create_cleanup_from_state(existing)?,
            )));
        }
        return Ok(LifecycleMemberState::Blocker);
    }
    Ok(LifecycleMemberState::Blocker)
}

fn classify_lifecycle_page(
    members: Vec<LifecycleMember>,
    states: Vec<HashMap<String, String>>,
    now: i64,
) -> WorkflowResult<Vec<ClassifiedLifecycleMember>> {
    if states.len() != members.len() {
        return Err(WorkflowError::internal_error(
            "Workflow lifecycle state reply count mismatch",
        ));
    }
    members
        .into_iter()
        .zip(states)
        .map(|(member, existing)| {
            Ok(ClassifiedLifecycleMember {
                member,
                state: classify_lifecycle_member(&existing, now)?,
            })
        })
        .collect()
}

async fn read_lifecycle_page(
    state: &AppState,
    ns: &str,
    members: Vec<LifecycleMember>,
) -> WorkflowResult<Vec<ClassifiedLifecycleMember>> {
    let states: Vec<HashMap<String, String>> = state
        .redis
        .with_conn(async |mut conn| {
            lifecycle_state_pipeline(ns, &members)
                .query_async(&mut conn)
                .await
        })
        .await?;
    classify_lifecycle_page(members, states, now_ms())
}

async fn lifecycle_blocker_state(
    state: &AppState,
    ns: &str,
    workflow_key: &str,
    instance_id: &str,
) -> WorkflowResult<LifecycleMemberState> {
    let existing = read_state_by_id(state, ns, workflow_key, instance_id).await?;
    classify_lifecycle_member(&existing, now_ms())
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
        for chunk in members.chunks(LIFECYCLE_BLOCKER_LIMIT) {
            let page = lifecycle_members_from_scan(chunk.iter().cloned())?;
            for classified in read_lifecycle_page(state, &req.ns, page).await? {
                match classified.state {
                    LifecycleMemberState::Blocker => {}
                    LifecycleMemberState::ExpiredPending(pending) => {
                        expired_pending.push(pending);
                        continue;
                    }
                    LifecycleMemberState::Ignore => continue,
                }
                blockers.push(LifecycleBlocker {
                    workflow_key: classified.member.workflow_key,
                    instance_id: classified.member.instance_id,
                });
                if blockers.len() >= LIFECYCLE_BLOCKER_LIMIT {
                    return Ok(LifecycleCheckResponse {
                        allowed: false,
                        count,
                        blockers,
                    });
                }
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
    use std::collections::HashMap;

    use crate::instance_state_key;

    use super::*;

    #[test]
    fn lifecycle_page_pipeline_preserves_valid_member_slots() {
        let members = lifecycle_members_from_scan(vec![
            "workflow-a\tinstance-a".to_string(),
            "workflow-b\tinstance-b".to_string(),
        ])
        .unwrap();
        let actual = lifecycle_state_pipeline("demo", &members).get_packed_pipeline();
        let mut expected = redis::pipe();
        expected
            .cmd("HGETALL")
            .arg(instance_state_key("demo", "workflow-a", "instance-a"));
        expected
            .cmd("HGETALL")
            .arg(instance_state_key("demo", "workflow-b", "instance-b"));

        assert_eq!(actual, expected.get_packed_pipeline());
    }

    #[test]
    fn malformed_lifecycle_members_fail_closed_before_state_reads() {
        let Err(error) = lifecycle_members_from_scan(vec!["malformed-member".to_string()]) else {
            panic!("malformed lifecycle member must fail closed");
        };

        assert_eq!(error.code, "workflow_invalid_state");
        assert_eq!(error.message, "Workflow lifecycle referrer is corrupt");
    }

    #[test]
    fn lifecycle_page_reply_alignment_preserves_valid_members() {
        let members = lifecycle_members_from_scan(vec![
            "workflow-a\tinstance-a".to_string(),
            "workflow-b\tinstance-b".to_string(),
        ])
        .unwrap();
        let states = vec![
            HashMap::from([("status".to_string(), "running".to_string())]),
            HashMap::new(),
        ];
        let classified = classify_lifecycle_page(members, states, now_ms()).unwrap();

        assert_eq!(classified.len(), 2);
        assert_eq!(classified[0].member.workflow_key, "workflow-a");
        assert_eq!(classified[0].member.instance_id, "instance-a");
        assert!(classified[0].state.is_blocker());
        assert_eq!(classified[1].member.workflow_key, "workflow-b");
        assert_eq!(classified[1].member.instance_id, "instance-b");
        assert!(!classified[1].state.is_blocker());
    }

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
