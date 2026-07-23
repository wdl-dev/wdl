use std::collections::VecDeque;
use std::future::Future;

use futures_util::stream::{FuturesUnordered, StreamExt};
use wdl_rust_common::redis_eval::{StaticRedisScript, append_eval_cmd};
use wdl_rust_common::time::now_ms;

use crate::{AppState, ShardQueueKeys, WorkflowError, WorkflowResult};

use super::eval_script;

pub(crate) struct ReadyAdmissionConfig {
    pub(crate) batch_size: usize,
    pub(crate) concurrency: usize,
    pub(crate) prune_on_error: bool,
}

pub(crate) struct DuePromotionConfig {
    pub(crate) total_limit: usize,
    pub(crate) per_shard_limit: usize,
    pub(crate) scan_overfetch_factor: usize,
}

pub(crate) struct DuePromotionMember {
    pub(crate) member: String,
    pub(crate) extra_keys: Vec<String>,
    pub(crate) extra_args: Vec<String>,
}

pub(crate) struct ReadyAdmissionOutcome<C> {
    pub(crate) counters: C,
    control: ReadyAdmissionControl,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReadyAdmissionControl {
    Continue,
    Stop,
    CapacityUnavailable,
    CapacityReleased,
}

impl<C> ReadyAdmissionOutcome<C> {
    pub(crate) fn admitted(counters: C) -> Self {
        Self {
            counters,
            control: ReadyAdmissionControl::Continue,
        }
    }

    pub(crate) fn stop_after_current_batch(counters: C) -> Self {
        Self {
            counters,
            control: ReadyAdmissionControl::Stop,
        }
    }

    pub(crate) fn capacity_unavailable(counters: C) -> Self {
        Self {
            counters,
            control: ReadyAdmissionControl::CapacityUnavailable,
        }
    }

    pub(crate) fn capacity_released(counters: C) -> Self {
        Self {
            counters,
            control: ReadyAdmissionControl::CapacityReleased,
        }
    }
}

pub(crate) struct ReadyAdmissionResult<C> {
    pub(crate) counters: C,
    pub(crate) error: Option<WorkflowError>,
    pub(crate) capacity_blocked: bool,
}

const PRUNE_READY_SHARD_SCRIPT: &str = r#"
if redis.call("SCARD", KEYS[1]) == 0 then
  redis.call("SREM", KEYS[2], ARGV[1])
  return 1
end
return 0
"#;

pub(crate) const REMOVE_READY_MEMBER_IF_STATE_MISSING_SCRIPT: &str = r#"
if redis.call("EXISTS", KEYS[1]) == 0 then
  redis.call("SREM", KEYS[2], ARGV[1])
  return 1
end
return 0
"#;

static REMOVE_READY_MEMBER_IF_STATE_MISSING: StaticRedisScript =
    StaticRedisScript::new(REMOVE_READY_MEMBER_IF_STATE_MISSING_SCRIPT);

pub(crate) fn rotate_active_shards(mut shards: Vec<usize>, seed: usize) -> Vec<usize> {
    shards.sort_unstable();
    shards.dedup();
    let len = shards.len();
    if len > 0 {
        shards.rotate_left(seed % len);
    }
    shards
}

pub(crate) async fn active_ready_shards(
    app: &AppState,
    keys: ShardQueueKeys,
) -> WorkflowResult<Vec<usize>> {
    let (raw, cursor): (Vec<String>, i64) = app
        .redis
        .with_conn(async |mut conn| {
            let mut pipe = redis::pipe();
            pipe.cmd("SMEMBERS")
                .arg(keys.ready_active())
                .cmd("INCR")
                .arg(keys.ready_cursor())
                .query_async(&mut conn)
                .await
        })
        .await?;
    let shards = raw
        .into_iter()
        .filter_map(|value| value.parse::<usize>().ok())
        .filter(|shard| *shard < keys.shard_count())
        .collect::<Vec<_>>();
    Ok(rotate_active_shards(shards, cursor.max(0) as usize))
}

async fn sample_ready_members(
    app: &AppState,
    keys: ShardQueueKeys,
    shards: &[usize],
    limit: usize,
) -> WorkflowResult<Vec<(usize, Vec<String>)>> {
    if shards.is_empty() || limit == 0 {
        return Ok(Vec::new());
    }
    let pages: Vec<Vec<String>> = app
        .redis
        .with_conn(async |mut conn| {
            let mut pipe = redis::pipe();
            for shard in shards {
                pipe.cmd("SRANDMEMBER").arg(keys.ready(*shard)).arg(limit);
            }
            pipe.query_async(&mut conn).await
        })
        .await?;
    Ok(shards.iter().copied().zip(pages).collect())
}

fn interleave_ready_members(sampled: Vec<(usize, Vec<String>)>) -> VecDeque<(usize, String)> {
    let mut shards = sampled
        .into_iter()
        .map(|(shard, members)| (shard, VecDeque::from(members)))
        .collect::<Vec<_>>();
    let mut candidates = VecDeque::new();
    loop {
        let mut added = false;
        for (shard, members) in &mut shards {
            if let Some(member) = members.pop_front() {
                candidates.push_back((*shard, member));
                added = true;
            }
        }
        if !added {
            break;
        }
    }
    candidates
}

async fn prune_ready_shards_if_empty(
    app: &AppState,
    keys: ShardQueueKeys,
    shards: &[usize],
) -> WorkflowResult<()> {
    if shards.is_empty() {
        return Ok(());
    }
    app.redis
        .with_conn(async |mut conn| {
            let mut pipe = redis::pipe();
            for shard in shards {
                let ready = keys.ready(*shard);
                let shard_arg = shard.to_string();
                append_eval_cmd(
                    &mut pipe,
                    PRUNE_READY_SHARD_SCRIPT,
                    &[&ready, keys.ready_active()],
                    &[&shard_arg],
                );
            }
            pipe.query_async::<Vec<i64>>(&mut conn).await
        })
        .await?;
    Ok(())
}

fn shards_to_prune(
    empty_sampled: &[usize],
    touched: &[usize],
    error_shards: &[usize],
    prune_on_error: bool,
) -> Vec<usize> {
    let mut shards = empty_sampled
        .iter()
        .chain(touched)
        .copied()
        .filter(|shard| prune_on_error || !error_shards.contains(shard))
        .collect::<Vec<_>>();
    shards.sort_unstable();
    shards.dedup();
    shards
}

pub(crate) async fn remove_ready_member_if_state_missing(
    app: &AppState,
    state_key: &str,
    ready_key: &str,
    member: &str,
) -> WorkflowResult<bool> {
    let removed: i64 = eval_script(
        app,
        &REMOVE_READY_MEMBER_IF_STATE_MISSING,
        &[state_key, ready_key],
        &[member],
    )
    .await?;
    Ok(removed == 1)
}

pub(crate) async fn promote_due_members<F>(
    app: &AppState,
    keys: ShardQueueKeys,
    config: DuePromotionConfig,
    promote_script: &str,
    prepare_member: F,
) -> WorkflowResult<usize>
where
    F: Fn(&str) -> Option<DuePromotionMember> + Copy,
{
    if config.total_limit == 0 || config.per_shard_limit == 0 {
        return Ok(0);
    }
    let now = now_ms();
    let mut moved = 0;
    for shard in due_shards_with_due_members(app, keys, now).await? {
        if moved >= config.total_limit {
            break;
        }
        let due = keys.due(shard);
        let ready = keys.ready(shard);
        let remaining = config.total_limit - moved;
        let scan_count = remaining
            .min(config.per_shard_limit)
            .saturating_mul(config.scan_overfetch_factor.max(1));
        let members: Vec<String> = app
            .redis
            .with_conn(async |mut conn| {
                redis::cmd("ZRANGEBYSCORE")
                    .arg(&due)
                    .arg("-inf")
                    .arg(now)
                    .arg("LIMIT")
                    .arg(0)
                    .arg(scan_count)
                    .query_async(&mut conn)
                    .await
            })
            .await?;
        if members.is_empty() {
            continue;
        }

        let mut candidates = Vec::new();
        let mut malformed = Vec::new();
        for member in members {
            match prepare_member(&member) {
                Some(candidate) => candidates.push(candidate),
                None => malformed.push(member),
            }
        }
        if !malformed.is_empty() {
            app.redis
                .with_conn({
                    let due = due.clone();
                    async move |mut conn| {
                        redis::cmd("ZREM")
                            .arg(due)
                            .arg(malformed)
                            .query_async::<()>(&mut conn)
                            .await
                    }
                })
                .await?;
        }
        if candidates.is_empty() {
            continue;
        }

        let shard_arg = shard.to_string();
        let now_arg = now.to_string();
        let mut offset = 0;
        let mut shard_moved = 0;
        while moved < config.total_limit
            && shard_moved < config.per_shard_limit
            && offset < candidates.len()
        {
            let remaining = (config.total_limit - moved).min(config.per_shard_limit - shard_moved);
            let end = (offset + remaining).min(candidates.len());
            let results: Vec<i64> = app
                .redis
                .with_conn(async |mut conn| {
                    let mut pipe = redis::pipe();
                    for candidate in &candidates[offset..end] {
                        let mut script_keys =
                            vec![due.as_str(), ready.as_str(), keys.ready_active()];
                        script_keys.extend(candidate.extra_keys.iter().map(String::as_str));
                        let mut script_args = vec![
                            candidate.member.as_str(),
                            now_arg.as_str(),
                            shard_arg.as_str(),
                        ];
                        script_args.extend(candidate.extra_args.iter().map(String::as_str));
                        append_eval_cmd(&mut pipe, promote_script, &script_keys, &script_args);
                    }
                    pipe.query_async(&mut conn).await
                })
                .await?;
            let moved_now = results.into_iter().filter(|value| *value == 1).count();
            moved += moved_now;
            shard_moved += moved_now;
            offset = end;
        }
    }
    Ok(moved)
}

pub(crate) async fn due_shards_with_due_members(
    app: &AppState,
    keys: ShardQueueKeys,
    now: i64,
) -> WorkflowResult<Vec<usize>> {
    let pages: Vec<Vec<String>> = app
        .redis
        .with_conn(async |mut conn| {
            let mut pipe = redis::pipe();
            for shard in 0..keys.shard_count() {
                pipe.cmd("ZRANGEBYSCORE")
                    .arg(keys.due(shard))
                    .arg("-inf")
                    .arg(now)
                    .arg("LIMIT")
                    .arg(0)
                    .arg(1);
            }
            pipe.query_async(&mut conn).await
        })
        .await?;
    Ok(pages
        .into_iter()
        .enumerate()
        .filter_map(|(shard, members)| (!members.is_empty()).then_some(shard))
        .collect())
}

pub(crate) async fn admit_ready_members<C, F, Fut, M>(
    app: &AppState,
    keys: ShardQueueKeys,
    config: ReadyAdmissionConfig,
    counters: C,
    process_member: F,
    merge_counters: M,
) -> WorkflowResult<ReadyAdmissionResult<C>>
where
    F: Fn(usize, String) -> Fut + Copy,
    Fut: Future<Output = WorkflowResult<ReadyAdmissionOutcome<C>>>,
    M: FnMut(&mut C, C),
{
    let active_shards = active_ready_shards(app, keys).await?;
    let sampled = sample_ready_members(app, keys, &active_shards, config.batch_size).await?;
    let empty_sampled_shards = sampled
        .iter()
        .filter_map(|(shard, members)| members.is_empty().then_some(*shard))
        .collect::<Vec<_>>();
    let candidates = interleave_ready_members(sampled);
    let run = run_ready_admission(
        &config,
        candidates,
        counters,
        process_member,
        merge_counters,
    )
    .await;
    let prune_shards = shards_to_prune(
        &empty_sampled_shards,
        &run.touched_shards,
        &run.error_shards,
        config.prune_on_error,
    );
    prune_ready_shards_if_empty(app, keys, &prune_shards).await?;
    Ok(ReadyAdmissionResult {
        counters: run.counters,
        error: run.error,
        capacity_blocked: run.capacity_blocked,
    })
}

struct ReadyAdmissionRun<C> {
    counters: C,
    error: Option<WorkflowError>,
    capacity_blocked: bool,
    touched_shards: Vec<usize>,
    error_shards: Vec<usize>,
}

async fn run_ready_admission<C, F, Fut, M>(
    config: &ReadyAdmissionConfig,
    mut candidates: VecDeque<(usize, String)>,
    mut counters: C,
    process_member: F,
    mut merge_counters: M,
) -> ReadyAdmissionRun<C>
where
    F: Fn(usize, String) -> Fut + Copy,
    Fut: Future<Output = WorkflowResult<ReadyAdmissionOutcome<C>>>,
    M: FnMut(&mut C, C),
{
    let concurrency = config.concurrency.max(1);
    let mut in_flight = FuturesUnordered::new();
    let mut first_error = None;
    let mut stop_after_current_batch = false;
    let mut capacity_blocked = false;
    let mut capacity_released = 0;
    let mut capacity_retry_limit = None;
    let mut deferred = VecDeque::new();
    let mut started_count = 0;
    let mut touched_shards = Vec::new();
    let mut error_shards = Vec::new();
    loop {
        while first_error.is_none()
            && !stop_after_current_batch
            && !capacity_blocked
            && capacity_retry_limit.is_none_or(|remaining| remaining > 0)
            && in_flight.len() < concurrency
            && started_count < config.batch_size
        {
            let Some((shard, member)) = candidates.pop_front() else {
                break;
            };
            if !touched_shards.contains(&shard) {
                touched_shards.push(shard);
            }
            started_count += 1;
            if let Some(remaining) = &mut capacity_retry_limit {
                *remaining -= 1;
            }
            in_flight.push(async move {
                let retained = member.clone();
                (shard, retained, process_member(shard, member).await)
            });
        }

        let Some((shard, member, result)) = in_flight.next().await else {
            if first_error.is_none()
                && !stop_after_current_batch
                && capacity_blocked
                && capacity_released > 0
                && started_count < config.batch_size
            {
                while let Some(candidate) = deferred.pop_back() {
                    candidates.push_front(candidate);
                }
                capacity_blocked = false;
                // Each released permit funds one retry, keeping stale-heavy waves O(batch).
                capacity_retry_limit = Some(capacity_released);
                capacity_released = 0;
                continue;
            }
            if first_error.is_none()
                && !stop_after_current_batch
                && capacity_retry_limit.is_some()
                && capacity_released > 0
                && started_count < config.batch_size
            {
                capacity_retry_limit = Some(capacity_released);
                capacity_released = 0;
                continue;
            }
            break;
        };
        if result.is_err() && !error_shards.contains(&shard) {
            error_shards.push(shard);
        }
        match apply_ready_admission_result(
            result,
            &mut counters,
            &mut first_error,
            &mut merge_counters,
        ) {
            ReadyAdmissionControl::Continue => {}
            ReadyAdmissionControl::Stop => stop_after_current_batch = true,
            ReadyAdmissionControl::CapacityUnavailable => {
                started_count -= 1;
                capacity_blocked = true;
                deferred.push_back((shard, member));
            }
            ReadyAdmissionControl::CapacityReleased => capacity_released += 1,
        }
    }
    let capacity_blocked = first_error.is_none()
        && !stop_after_current_batch
        && (capacity_blocked
            || (capacity_retry_limit == Some(0)
                && (!candidates.is_empty() || !deferred.is_empty())));
    ReadyAdmissionRun {
        counters,
        error: first_error,
        capacity_blocked,
        touched_shards,
        error_shards,
    }
}

fn apply_ready_admission_result<C, M>(
    result: WorkflowResult<ReadyAdmissionOutcome<C>>,
    counters: &mut C,
    first_error: &mut Option<WorkflowError>,
    merge_counters: &mut M,
) -> ReadyAdmissionControl
where
    M: FnMut(&mut C, C),
{
    match result {
        Ok(outcome) => {
            merge_counters(counters, outcome.counters);
            outcome.control
        }
        Err(err) => {
            first_error.get_or_insert(err);
            ReadyAdmissionControl::Stop
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::{Barrier, Semaphore};
    use tokio::time::{sleep, timeout};

    #[derive(Clone, Copy)]
    enum HaltMode {
        Error,
        Stop,
    }

    #[test]
    fn active_shards_rotate_from_stable_order() {
        assert_eq!(rotate_active_shards(vec![3, 1, 3, 2], 0), vec![1, 2, 3]);
        assert_eq!(rotate_active_shards(vec![3, 1, 3, 2], 1), vec![2, 3, 1]);
        assert_eq!(rotate_active_shards(vec![3, 1, 3, 2], 5), vec![3, 1, 2]);
    }

    #[test]
    fn ready_shard_prune_removes_only_empty_shards_from_active_index() {
        assert!(PRUNE_READY_SHARD_SCRIPT.contains(r#"redis.call("SCARD", KEYS[1]) == 0"#));
        assert!(PRUNE_READY_SHARD_SCRIPT.contains(r#"redis.call("SREM", KEYS[2], ARGV[1])"#));
    }

    #[test]
    fn ready_shard_prune_excludes_error_shards_unless_configured() {
        assert_eq!(
            shards_to_prune(&[0, 2], &[1, 2, 3], &[2, 3], false),
            vec![0, 1],
        );
        assert_eq!(
            shards_to_prune(&[0, 2], &[1, 2, 3], &[2, 3], true),
            vec![0, 1, 2, 3],
        );
    }

    #[test]
    fn stale_ready_member_cleanup_rechecks_state_inside_lua() {
        assert!(
            REMOVE_READY_MEMBER_IF_STATE_MISSING_SCRIPT
                .contains(r#"redis.call("EXISTS", KEYS[1]) == 0"#)
        );
        assert!(
            REMOVE_READY_MEMBER_IF_STATE_MISSING_SCRIPT
                .contains(r#"redis.call("SREM", KEYS[2], ARGV[1])"#)
        );
        assert!(REMOVE_READY_MEMBER_IF_STATE_MISSING_SCRIPT.contains("return 1"));
        assert!(REMOVE_READY_MEMBER_IF_STATE_MISSING_SCRIPT.contains("return 0"));
    }

    #[test]
    fn ready_members_are_interleaved_across_shards() {
        let candidates = interleave_ready_members(vec![
            (2, vec!["2a".to_string(), "2b".to_string()]),
            (7, vec!["7a".to_string()]),
            (9, vec!["9a".to_string(), "9b".to_string()]),
        ]);

        assert_eq!(
            candidates.into_iter().collect::<Vec<_>>(),
            vec![
                (2, "2a".to_string()),
                (7, "7a".to_string()),
                (9, "9a".to_string()),
                (2, "2b".to_string()),
                (9, "9b".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn ready_admission_uses_one_global_limit_across_shards() {
        let active = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);
        let started = AtomicUsize::new(0);
        let first_wave_shards = AtomicUsize::new(0);
        let wave = Barrier::new(3);
        let candidates = interleave_ready_members(vec![
            (0, vec!["0a".to_string(), "0b".to_string()]),
            (1, vec!["1a".to_string(), "1b".to_string()]),
            (2, vec!["2a".to_string(), "2b".to_string()]),
        ]);
        let config = ReadyAdmissionConfig {
            batch_size: 6,
            concurrency: 3,
            prune_on_error: false,
        };
        let active_ref = &active;
        let peak_ref = &peak;
        let started_ref = &started;
        let first_wave_shards_ref = &first_wave_shards;
        let wave_ref = &wave;
        let run = timeout(
            Duration::from_secs(1),
            run_ready_admission(
                &config,
                candidates,
                0_usize,
                move |shard, _| async move {
                    let ordinal = started_ref.fetch_add(1, Ordering::SeqCst);
                    if ordinal < 3 {
                        first_wave_shards_ref.fetch_or(1 << shard, Ordering::SeqCst);
                    }
                    let current = active_ref.fetch_add(1, Ordering::SeqCst) + 1;
                    peak_ref.fetch_max(current, Ordering::SeqCst);
                    wave_ref.wait().await;
                    active_ref.fetch_sub(1, Ordering::SeqCst);
                    Ok::<_, WorkflowError>(ReadyAdmissionOutcome::admitted(1))
                },
                |count, delta| *count += delta,
            ),
        )
        .await
        .expect("three shards should dispatch concurrently");

        assert_eq!(run.counters, 6);
        assert!(run.error.is_none());
        assert_eq!(peak.load(Ordering::SeqCst), 3);
        assert_eq!(first_wave_shards.load(Ordering::SeqCst), 0b111);
    }

    #[tokio::test]
    async fn ready_admission_caps_started_candidates_at_batch_size() {
        let started = AtomicUsize::new(0);
        let candidates = VecDeque::from(
            (0..10)
                .map(|index| (index % 2, index.to_string()))
                .collect::<Vec<_>>(),
        );
        let config = ReadyAdmissionConfig {
            batch_size: 4,
            concurrency: 3,
            prune_on_error: false,
        };

        let run = run_ready_admission(
            &config,
            candidates,
            0_usize,
            |_, _| async {
                started.fetch_add(1, Ordering::SeqCst);
                Ok::<_, WorkflowError>(ReadyAdmissionOutcome::admitted(0))
            },
            |count, delta| *count += delta,
        )
        .await;

        assert_eq!(run.counters, 0);
        assert!(run.error.is_none());
        assert!(!run.capacity_blocked);
        assert_eq!(started.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn overlapping_admission_runs_share_held_execution_capacity() {
        async fn run(
            permits: &Arc<Semaphore>,
            held: &Mutex<Vec<tokio::sync::OwnedSemaphorePermit>>,
        ) -> ReadyAdmissionRun<usize> {
            let config = ReadyAdmissionConfig {
                batch_size: 4,
                concurrency: 4,
                prune_on_error: false,
            };
            let candidates = VecDeque::from(
                (0..4)
                    .map(|index| (index % 2, index.to_string()))
                    .collect::<Vec<_>>(),
            );
            run_ready_admission(
                &config,
                candidates,
                0_usize,
                |_, _| async move {
                    let Ok(permit) = permits.clone().try_acquire_owned() else {
                        return Ok::<_, WorkflowError>(
                            ReadyAdmissionOutcome::capacity_unavailable(0),
                        );
                    };
                    held.lock().unwrap().push(permit);
                    Ok(ReadyAdmissionOutcome::admitted(1))
                },
                |count, delta| *count += delta,
            )
            .await
        }

        let permits = Arc::new(Semaphore::new(2));
        let held = Mutex::new(Vec::new());

        let admitted = run(&permits, &held).await;
        assert_eq!(admitted.counters, 2);
        assert!(admitted.capacity_blocked);
        assert_eq!(permits.available_permits(), 0);
        let blocked = run(&permits, &held).await;
        assert_eq!(blocked.counters, 0);
        assert!(blocked.capacity_blocked);

        held.lock().unwrap().clear();
        let admitted_after_release = run(&permits, &held).await;
        assert_eq!(admitted_after_release.counters, 2);
        assert!(admitted_after_release.capacity_blocked);
    }

    #[tokio::test]
    async fn one_released_permit_retries_only_one_candidate_blocked_in_the_same_wave() {
        let permits = Arc::new(Semaphore::new(1));
        let immediate_holds_permit = Arc::new(AtomicUsize::new(0));
        let blocked_attempts = Arc::new(AtomicUsize::new(0));
        let queued_attempts = Arc::new(AtomicUsize::new(0));
        let retained = Arc::new(Mutex::new(Vec::new()));
        let candidates = VecDeque::from(vec![
            (0, "immediate".to_string()),
            (1, "queued-a".to_string()),
            (2, "queued-b".to_string()),
            (3, "queued-c".to_string()),
        ]);
        let config = ReadyAdmissionConfig {
            batch_size: 4,
            concurrency: 4,
            prune_on_error: false,
        };

        let run = timeout(
            Duration::from_secs(1),
            run_ready_admission(
                &config,
                candidates,
                0_usize,
                |_, member| {
                    let permits = permits.clone();
                    let immediate_holds_permit = immediate_holds_permit.clone();
                    let blocked_attempts = blocked_attempts.clone();
                    let queued_attempts = queued_attempts.clone();
                    let retained = retained.clone();
                    async move {
                        if member == "immediate" {
                            let permit = permits
                                .try_acquire_owned()
                                .expect("immediate candidate should acquire capacity");
                            immediate_holds_permit.store(1, Ordering::SeqCst);
                            while blocked_attempts.load(Ordering::SeqCst) < 3 {
                                tokio::task::yield_now().await;
                            }
                            drop(permit);
                            return Ok::<_, WorkflowError>(
                                ReadyAdmissionOutcome::capacity_released(0),
                            );
                        }

                        queued_attempts.fetch_add(1, Ordering::SeqCst);
                        while immediate_holds_permit.load(Ordering::SeqCst) == 0 {
                            tokio::task::yield_now().await;
                        }
                        let Ok(permit) = permits.try_acquire_owned() else {
                            blocked_attempts.fetch_add(1, Ordering::SeqCst);
                            return Ok(ReadyAdmissionOutcome::capacity_unavailable(0));
                        };
                        retained.lock().unwrap().push(permit);
                        Ok(ReadyAdmissionOutcome::admitted(1))
                    }
                },
                |count, delta| *count += delta,
            ),
        )
        .await
        .expect("released capacity should retry the blocked candidate");

        assert_eq!(run.counters, 1);
        assert!(run.error.is_none());
        assert!(run.capacity_blocked);
        assert_eq!(blocked_attempts.load(Ordering::SeqCst), 3);
        assert_eq!(queued_attempts.load(Ordering::SeqCst), 4);
        assert_eq!(retained.lock().unwrap().len(), 1);
        assert_eq!(permits.available_permits(), 0);
    }

    async fn assert_halt_stops_new_work_and_drains_in_flight(mode: HaltMode) {
        let started = AtomicUsize::new(0);
        let halt_returned = AtomicUsize::new(0);
        let release = Semaphore::new(0);
        let candidates = VecDeque::from(vec![
            (0, "halt".to_string()),
            (1, "one".to_string()),
            (2, "two".to_string()),
            (0, "three".to_string()),
            (1, "four".to_string()),
            (2, "five".to_string()),
        ]);
        let config = ReadyAdmissionConfig {
            batch_size: 6,
            concurrency: 3,
            prune_on_error: false,
        };
        let started_ref = &started;
        let halt_returned_ref = &halt_returned;
        let release_ref = &release;
        let run = run_ready_admission(
            &config,
            candidates,
            0_usize,
            |_, member| async move {
                started_ref.fetch_add(1, Ordering::SeqCst);
                if member == "halt" {
                    halt_returned_ref.store(1, Ordering::SeqCst);
                    return match mode {
                        HaltMode::Error => Err(WorkflowError::internal_error("halt")),
                        HaltMode::Stop => Ok(ReadyAdmissionOutcome::stop_after_current_batch(1)),
                    };
                }
                release_ref
                    .acquire()
                    .await
                    .expect("test semaphore should stay open")
                    .forget();
                Ok(ReadyAdmissionOutcome::admitted(1))
            },
            |count, delta| *count += delta,
        );
        let controller = async {
            while started.load(Ordering::SeqCst) < 3 || halt_returned.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
            sleep(Duration::from_millis(10)).await;
            assert_eq!(started.load(Ordering::SeqCst), 3);
            release.add_permits(2);
        };

        let (run, ()) = timeout(Duration::from_secs(1), async {
            tokio::join!(run, controller)
        })
        .await
        .expect("halted dispatch should drain its initial in-flight work");

        assert_eq!(started.load(Ordering::SeqCst), 3);
        assert!(!run.capacity_blocked);
        match mode {
            HaltMode::Error => {
                assert_eq!(run.counters, 2);
                assert_eq!(
                    run.error.as_ref().map(|err| err.code),
                    Some("internal_error")
                );
            }
            HaltMode::Stop => {
                assert_eq!(run.counters, 3);
                assert!(run.error.is_none());
            }
        }
    }

    #[tokio::test]
    async fn ready_admission_error_stops_new_work_and_drains_in_flight() {
        assert_halt_stops_new_work_and_drains_in_flight(HaltMode::Error).await;
    }

    #[tokio::test]
    async fn ready_admission_stop_signal_stops_new_work_and_drains_in_flight() {
        assert_halt_stops_new_work_and_drains_in_flight(HaltMode::Stop).await;
    }
}
