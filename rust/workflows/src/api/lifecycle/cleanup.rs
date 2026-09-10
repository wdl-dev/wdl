use std::collections::HashMap;

use wdl_rust_common::redis_eval::StaticRedisScript;
use wdl_rust_common::time::now_ms;

use crate::{
    AppState, WorkflowError, WorkflowResult, by_version_key, by_worker_key, instance_state_key,
};

use super::super::{
    CLEANUP_PENDING_CREATE, LIFECYCLE_BLOCKER_LIMIT, LifecycleBlocker, LifecycleCheckRequest,
    LifecycleCheckResponse, PendingCreateCleanup, active_pending_restart_blockers, eval_script,
    is_pending_create, parse_workflow_referrer_member, pending_create_cleanup_from_state,
    pending_create_cleanup_keys, pending_create_expired, require_non_empty,
    workflow_referrer_member,
};

const LIFECYCLE_SCAN_COUNT: usize = 128;
const LIFECYCLE_SCAN_MAX_MEMBERS: usize = 512;
const LIFECYCLE_SCAN_MAX_BYTES: usize = 128 * 1024;

static READ_LIFECYCLE_PAGE: StaticRedisScript = StaticRedisScript::new(
    r#"
local page = redis.call("SSCAN", KEYS[1], ARGV[1], "COUNT", ARGV[2])
if #page[2] > tonumber(ARGV[3]) then return {page[1], false} end
local bytes = 0
for _, member in ipairs(page[2]) do
  bytes = bytes + #member
  if bytes > tonumber(ARGV[4]) then return {page[1], false} end
end
return page
"#,
);

static PRUNE_MISSING_LIFECYCLE_MEMBER: StaticRedisScript = StaticRedisScript::new(
    r#"
if redis.call("EXISTS", KEYS[1]) == 0 then
  return redis.call("SREM", KEYS[2], ARGV[1])
end
return 0
"#,
);

enum LifecycleMemberState {
    Blocker,
    ExpiredPending(Box<PendingCreateCleanup>),
    Ignore,
}

#[derive(Clone)]
struct LifecycleMember {
    workflow_key: String,
    instance_id: String,
}

struct ClassifiedLifecycleMember {
    member: LifecycleMember,
    state: LifecycleMemberState,
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

fn lifecycle_page_response(
    count: usize,
    blockers: Vec<LifecycleBlocker>,
    next: u64,
) -> LifecycleCheckResponse {
    let allowed = blockers.is_empty() && next == 0;
    let cursor = (blockers.is_empty() && next != 0).then(|| next.to_string());
    LifecycleCheckResponse {
        allowed,
        count: if allowed { 0 } else { count },
        blockers,
        cursor,
    }
}

fn validate_pending_scope(
    req: &LifecycleCheckRequest,
    member: &LifecycleMember,
    pending: &PendingCreateCleanup,
) -> WorkflowResult<()> {
    if pending.identity.ns != req.ns
        || pending.identity.worker != req.worker
        || pending.identity.workflow_key != member.workflow_key
        || pending.identity.instance_id != member.instance_id
        || req
            .version
            .as_ref()
            .is_some_and(|version| version != &pending.identity.frozen_version)
    {
        return Err(WorkflowError::invalid_state(
            "Workflow lifecycle state identity does not match its referrer",
        ));
    }
    Ok(())
}

fn lifecycle_cleanup_pipeline(
    req: &LifecycleCheckRequest,
    key: &str,
    page: &[ClassifiedLifecycleMember],
) -> WorkflowResult<(redis::Pipeline, Vec<usize>)> {
    let cleanup_count = page
        .iter()
        .filter(|item| matches!(item.state, LifecycleMemberState::ExpiredPending(_)))
        .count();
    let prune_count = page
        .iter()
        .filter(|item| matches!(item.state, LifecycleMemberState::Ignore))
        .count();
    let mut pipe = redis::pipe();
    let cleanup = CLEANUP_PENDING_CREATE.prepare_pipeline(&mut pipe, cleanup_count);
    let prune = PRUNE_MISSING_LIFECYCLE_MEMBER.prepare_pipeline(&mut pipe, prune_count);
    let mut slots = Vec::with_capacity(cleanup_count + prune_count);
    for (index, classified) in page.iter().enumerate() {
        let member = &classified.member;
        let referrer = workflow_referrer_member(&member.workflow_key, &member.instance_id);
        match &classified.state {
            LifecycleMemberState::ExpiredPending(pending) => {
                validate_pending_scope(req, member, pending)?;
                let keys = pending_create_cleanup_keys(&pending.identity);
                cleanup.append(
                    &mut pipe,
                    &keys.each_ref().map(String::as_str),
                    &[&referrer, &pending.token],
                );
            }
            LifecycleMemberState::Ignore => {
                let state_key =
                    instance_state_key(&req.ns, &member.workflow_key, &member.instance_id);
                prune.append(&mut pipe, &[&state_key, key], &[&referrer]);
            }
            LifecycleMemberState::Blocker => continue,
        }
        slots.push(index);
    }
    Ok((pipe, slots))
}

fn lifecycle_cleanup_rechecks(
    page: &mut [ClassifiedLifecycleMember],
    slots: &[usize],
    results: Vec<i64>,
) -> WorkflowResult<Vec<usize>> {
    if results.len() != slots.len() {
        return Err(WorkflowError::internal_error(
            "Workflow lifecycle cleanup reply count mismatch",
        ));
    }
    let mut rechecks = Vec::new();
    for (&slot, result) in slots.iter().zip(results) {
        match result {
            1 => page[slot].state = LifecycleMemberState::Ignore,
            0 => rechecks.push(slot),
            _ => {
                return Err(WorkflowError::internal_error(
                    "Workflow lifecycle cleanup result is invalid",
                ));
            }
        }
    }
    Ok(rechecks)
}

async fn cleanup_lifecycle_page(
    state: &AppState,
    req: &LifecycleCheckRequest,
    key: &str,
    mut page: Vec<ClassifiedLifecycleMember>,
) -> WorkflowResult<Vec<ClassifiedLifecycleMember>> {
    let (pipe, slots) = lifecycle_cleanup_pipeline(req, key, &page)?;
    if slots.is_empty() {
        return Ok(page);
    }
    // Each script keeps its fence; an ambiguous mixed pipeline is never replayed.
    let results = state
        .redis
        .with_conn(async |mut conn| pipe.query_async(&mut conn).await)
        .await?;
    let rechecks = lifecycle_cleanup_rechecks(&mut page, &slots, results)?;
    if !rechecks.is_empty() {
        let members = rechecks
            .iter()
            .map(|&slot| page[slot].member.clone())
            .collect();
        let checked = read_lifecycle_page(state, &req.ns, members).await?;
        for (slot, classified) in rechecks.into_iter().zip(checked) {
            page[slot].state = match classified.state {
                LifecycleMemberState::Ignore => LifecycleMemberState::Ignore,
                _ => LifecycleMemberState::Blocker,
            };
        }
    }
    Ok(page)
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
    let cursor = req
        .cursor
        .as_deref()
        .unwrap_or("0")
        .parse::<u64>()
        .map_err(|_| WorkflowError::invalid_request("Workflow lifecycle cursor is invalid"))?;
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
        return Ok(lifecycle_page_response(0, Vec::new(), 0));
    }
    if !blockers.is_empty() {
        return Ok(lifecycle_page_response(count, blockers, 0));
    }
    let (next, members): (u64, Option<Vec<String>>) = eval_script(
        state,
        &READ_LIFECYCLE_PAGE,
        &[&key],
        &[
            &cursor.to_string(),
            &LIFECYCLE_SCAN_COUNT.to_string(),
            &LIFECYCLE_SCAN_MAX_MEMBERS.to_string(),
            &LIFECYCLE_SCAN_MAX_BYTES.to_string(),
        ],
    )
    .await?;
    let members = members.ok_or_else(|| {
        WorkflowError::invalid_state("Workflow lifecycle scan page exceeds its size limit")
    })?;
    for chunk in members.chunks(LIFECYCLE_BLOCKER_LIMIT) {
        let page = lifecycle_members_from_scan(chunk.iter().cloned())?;
        let mut page = read_lifecycle_page(state, &req.ns, page).await?;
        if req.allow_cleanup {
            page = cleanup_lifecycle_page(state, &req, &key, page).await?;
        }
        for classified in page {
            if !matches!(classified.state, LifecycleMemberState::Ignore) {
                let member = classified.member;
                blockers.push(LifecycleBlocker {
                    workflow_key: member.workflow_key,
                    instance_id: member.instance_id,
                });
                if blockers.len() == LIFECYCLE_BLOCKER_LIMIT {
                    return Ok(lifecycle_page_response(count, blockers, 0));
                }
            }
        }
    }
    if req.allow_cleanup && next == 0 && blockers.is_empty() {
        // A cursor traversal is not a deletion fence across reconnect/failover.
        let remaining: usize = state
            .redis
            .with_conn(async |mut conn| redis::cmd("SCARD").arg(&key).query_async(&mut conn).await)
            .await?;
        if remaining > 0 {
            return Ok(LifecycleCheckResponse {
                allowed: false,
                count: remaining,
                blockers,
                cursor: None,
            });
        }
    }
    Ok(lifecycle_page_response(count, blockers, next))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::instance_state_key;

    use super::*;

    fn cleanup_page() -> (LifecycleCheckRequest, Vec<ClassifiedLifecycleMember>) {
        let request = LifecycleCheckRequest {
            ns: "demo".to_string(),
            worker: "shop".to_string(),
            version: Some("1".to_string()),
            allow_cleanup: true,
            cursor: None,
        };
        let page = (0..5)
            .map(|index| {
                let id = format!("item-{index}");
                let state = match index {
                    0 => LifecycleMemberState::Blocker,
                    2 | 4 => LifecycleMemberState::Ignore,
                    _ => LifecycleMemberState::ExpiredPending(Box::new(PendingCreateCleanup {
                        identity: crate::InstanceIdentity {
                            ns: request.ns.clone(),
                            worker: request.worker.clone(),
                            frozen_version: "1".to_string(),
                            workflow_key: "flow".to_string(),
                            workflow_name: "orders".to_string(),
                            class_name: "OrderWorkflow".to_string(),
                            instance_id: id.clone(),
                            generation: "1".to_string(),
                            created_at_ms: "1".to_string(),
                        },
                        token: format!("token-{index}"),
                    })),
                };
                ClassifiedLifecycleMember {
                    member: LifecycleMember {
                        workflow_key: "flow".to_string(),
                        instance_id: id,
                    },
                    state,
                }
            })
            .collect();
        (request, page)
    }

    #[test]
    fn lifecycle_cleanup_batches_mixed_actions_with_their_original_fences() {
        let (request, page) = cleanup_page();
        let index_key = by_version_key("demo", "shop", "1");
        let (pipe, slots) = lifecycle_cleanup_pipeline(&request, &index_key, &page).unwrap();
        assert_eq!(slots, [1, 2, 3, 4]);
        let commands: Vec<Vec<String>> = pipe
            .cmd_iter()
            .map(|command| {
                redis::from_redis_value(
                    redis::parse_redis_value(&command.get_packed_command()).unwrap(),
                )
                .unwrap()
            })
            .collect();
        assert_eq!(
            commands.len(),
            6,
            "one load per repeated script and four fenced mutations"
        );
        for (slot, command) in slots.iter().zip(&commands[2..]) {
            assert_eq!(
                command[3],
                instance_state_key("demo", "flow", &format!("item-{slot}"))
            );
            let referrer = workflow_referrer_member("flow", &format!("item-{slot}"));
            if slot % 2 == 1 {
                assert_eq!(command[2], "9");
                assert_eq!(command[10], by_worker_key("demo", "shop"));
                assert_eq!(command[11], index_key);
                assert_eq!(command[12], referrer);
                assert_eq!(command[13], format!("token-{slot}"));
            } else {
                assert_eq!(command[2], "2");
                assert_eq!(command[4], index_key);
                assert_eq!(command[5], referrer);
            }
        }
    }

    #[test]
    fn lifecycle_cleanup_rechecks_only_failed_slots_and_rejects_misalignment() {
        let (_, mut page) = cleanup_page();
        let slots = [1, 2, 3, 4];
        let rechecks = lifecycle_cleanup_rechecks(&mut page, &slots, vec![1, 0, 0, 1]).unwrap();
        assert_eq!(rechecks, [2, 3]);
        assert!(matches!(page[0].state, LifecycleMemberState::Blocker));
        assert!(matches!(page[1].state, LifecycleMemberState::Ignore));
        assert!(matches!(
            page[3].state,
            LifecycleMemberState::ExpiredPending(_)
        ));
        assert!(matches!(page[4].state, LifecycleMemberState::Ignore));
        assert!(lifecycle_cleanup_rechecks(&mut page, &slots, vec![1]).is_err());
        assert!(lifecycle_cleanup_rechecks(&mut page, &[1], vec![2]).is_err());
    }

    #[test]
    fn lifecycle_cleanup_rejects_a_foreign_pending_identity_before_execution() {
        let (request, mut page) = cleanup_page();
        let LifecycleMemberState::ExpiredPending(pending) = &mut page[3].state else {
            panic!("pending fixture")
        };
        pending.identity.worker = "other-worker".to_string();
        let error =
            lifecycle_cleanup_pipeline(&request, &by_version_key("demo", "shop", "1"), &page)
                .err()
                .unwrap();
        assert_eq!(error.code, "workflow_invalid_state");
    }

    #[test]
    fn lifecycle_pages_match_the_control_fixture() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/workflow-lifecycle-check.json"
        ))
        .unwrap();
        assert_eq!(fixture["limits"]["blockers"], LIFECYCLE_BLOCKER_LIMIT);
        assert_eq!(fixture["limits"]["scanCount"], LIFECYCLE_SCAN_COUNT);
        assert_eq!(
            fixture["limits"]["scanMembersMax"],
            LIFECYCLE_SCAN_MAX_MEMBERS
        );
        assert_eq!(fixture["limits"]["scanBytesMax"], LIFECYCLE_SCAN_MAX_BYTES);
        let request: LifecycleCheckRequest =
            serde_json::from_value(fixture["request"].clone()).unwrap();
        assert_eq!(request.cursor.as_deref(), Some("128"));
        assert!(request.allow_cleanup);
        for (name, blockers, cursor) in [
            ("complete", Vec::new(), 0),
            (
                "blocked",
                vec![LifecycleBlocker {
                    workflow_key: "wf_11111111111111111111111111111111".to_string(),
                    instance_id: "item".to_string(),
                }],
                0,
            ),
            ("continuation", Vec::new(), 128),
        ] {
            let body =
                serde_json::to_value(lifecycle_page_response(123, blockers, cursor)).unwrap();
            assert_eq!(body, fixture["responses"][name]);
        }
    }

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
        assert!(matches!(classified[0].state, LifecycleMemberState::Blocker));
        assert_eq!(classified[1].member.workflow_key, "workflow-b");
        assert_eq!(classified[1].member.instance_id, "instance-b");
        assert!(matches!(classified[1].state, LifecycleMemberState::Ignore));
    }

    #[test]
    fn incomplete_scan_never_authorizes_delete() {
        let partial = lifecycle_page_response(1000, Vec::new(), 128);
        assert!(!partial.allowed);
        assert_eq!(partial.cursor.as_deref(), Some("128"));
        let complete = lifecycle_page_response(1000, Vec::new(), 0);
        assert!(complete.allowed);
        assert_eq!(complete.count, 0);
        assert!(complete.cursor.is_none());
        let blocked = lifecycle_page_response(
            1000,
            vec![LifecycleBlocker {
                workflow_key: "workflow".to_string(),
                instance_id: "instance".to_string(),
            }],
            128,
        );
        assert!(!blocked.allowed);
        assert_eq!(blocked.blockers.len(), 1);
        assert!(blocked.cursor.is_none());
    }
}
