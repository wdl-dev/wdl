use serde_json::json;
use wdl_rust_common::redis_eval::append_eval_cmd;
use wdl_rust_common::time::{now_ms, random_hex_64};
use wdl_rust_common::worker_contract::do_storage_id_key;

use crate::{AppState, LogLevel, WorkflowResult, do_alarm_by_worker_key, log};

use super::super::eval_script;
use super::model::{
    DoAlarmCleanupRequest, DoAlarmDeleteRequest, DoAlarmMutationResponse, DoAlarmSetRequest,
    job_keys_for_identity, validate_cleanup_request, validate_delete_request, validate_set_request,
};
use super::scripts::{CLEANUP_DO_ALARM_FOR_STORAGE_SCRIPT, DELETE_DO_ALARM, SET_DO_ALARM};

const DO_ALARM_CLEANUP_BATCH_SIZE: usize = 256;
const DO_ALARM_CLEANUP_SNAPSHOT_TTL_SECONDS: usize = 60;

pub(crate) async fn set_do_alarm(
    app: &AppState,
    req: DoAlarmSetRequest,
) -> WorkflowResult<DoAlarmMutationResponse> {
    validate_set_request(&req)?;
    let keys = job_keys_for_identity(
        &req.ns,
        &req.worker,
        &req.do_storage_id,
        &req.class_name,
        &req.object_name,
    );
    if !worker_storage_matches(app, &req).await? {
        log_ignored_set(app, &req, keys.job_id(), "storage_mismatch");
        return Ok(DoAlarmMutationResponse {
            ok: true,
            job_id: Some(keys.job_id().to_string()),
            changed: false,
            deleted: 0,
        });
    }
    let by_worker = do_alarm_by_worker_key(&req.ns, &req.worker);
    let now = now_ms().to_string();
    let state_key = keys.state();
    let due_key = keys.due();
    let ready_key = keys.ready();
    let scheduled_time = req.scheduled_time.to_string();
    let retry_count = req.retry_count.to_string();
    let generation: i64 = eval_script(
        app,
        &SET_DO_ALARM,
        &[&state_key, &due_key, &ready_key, &by_worker],
        &[
            &req.ns,
            &req.worker,
            &req.version,
            &req.do_storage_id,
            &req.class_name,
            &req.object_name,
            &scheduled_time,
            &retry_count,
            &req.token,
            &now,
            keys.job_id(),
        ],
    )
    .await?;
    match worker_storage_matches(app, &req).await {
        Ok(true) => {}
        Ok(false) => {
            let deleted = delete_do_alarm_job(app, &keys, &by_worker, &req.token).await?;
            log_ignored_set(app, &req, keys.job_id(), "storage_mismatch_after_write");
            return Ok(DoAlarmMutationResponse {
                ok: true,
                job_id: Some(keys.job_id().to_string()),
                changed: false,
                deleted: usize::from(deleted == 1),
            });
        }
        Err(err) => {
            let _ = delete_do_alarm_job(app, &keys, &by_worker, &req.token).await;
            return Err(err);
        }
    }
    log(
        app,
        LogLevel::Info,
        "do_alarm_scheduled",
        json!({
            "namespace": req.ns,
            "worker": req.worker,
            "class_name": req.class_name,
            "object_name": req.object_name,
            "job_id": keys.job_id(),
            "generation": generation,
            "due_at_ms": req.scheduled_time,
        }),
    );
    Ok(DoAlarmMutationResponse {
        ok: true,
        job_id: Some(keys.job_id().to_string()),
        changed: true,
        deleted: 0,
    })
}

async fn worker_storage_matches(app: &AppState, req: &DoAlarmSetRequest) -> WorkflowResult<bool> {
    let storage_key = do_storage_id_key(&req.ns, &req.worker);
    let current_storage_id: Option<String> = app
        .control_redis
        .with_conn(async |mut conn| {
            redis::cmd("GET")
                .arg(storage_key)
                .query_async(&mut conn)
                .await
        })
        .await?;
    Ok(current_storage_id.as_deref() == Some(req.do_storage_id.as_str()))
}

fn log_ignored_set(app: &AppState, req: &DoAlarmSetRequest, job_id: &str, reason: &str) {
    log(
        app,
        LogLevel::Info,
        "do_alarm_set_ignored",
        json!({
            "namespace": &req.ns,
            "worker": &req.worker,
            "class_name": &req.class_name,
            "object_name": &req.object_name,
            "job_id": job_id,
            "reason": reason,
        }),
    );
}

pub(crate) async fn delete_do_alarm(
    app: &AppState,
    req: DoAlarmDeleteRequest,
) -> WorkflowResult<DoAlarmMutationResponse> {
    validate_delete_request(&req)?;
    let keys = job_keys_for_identity(
        &req.ns,
        &req.worker,
        &req.do_storage_id,
        &req.class_name,
        &req.object_name,
    );
    let by_worker = do_alarm_by_worker_key(&req.ns, &req.worker);
    let changed = delete_do_alarm_job(app, &keys, &by_worker, &req.token).await?;
    Ok(DoAlarmMutationResponse {
        ok: true,
        job_id: Some(keys.job_id().to_string()),
        changed: changed == 1,
        deleted: usize::from(changed == 1),
    })
}

async fn delete_do_alarm_job(
    app: &AppState,
    keys: &crate::DoAlarmJobKeys,
    by_worker: &str,
    token: &str,
) -> WorkflowResult<i64> {
    let state_key = keys.state();
    let due_key = keys.due();
    let ready_key = keys.ready();
    eval_script(
        app,
        &DELETE_DO_ALARM,
        &[&state_key, &due_key, &ready_key, by_worker],
        &[token, keys.job_id()],
    )
    .await
}

pub(crate) async fn cleanup_do_alarms_for_worker(
    app: &AppState,
    req: DoAlarmCleanupRequest,
) -> WorkflowResult<DoAlarmMutationResponse> {
    validate_cleanup_request(&req)?;
    let by_worker = do_alarm_by_worker_key(&req.ns, &req.worker);
    let snapshot_key = format!("{by_worker}:cleanup-snapshot:{}", random_hex_64());
    let mut cursor = 0_u64;
    let mut snapshot_has_members = false;
    loop {
        let (next, job_ids): (u64, Vec<String>) = app
            .redis
            .with_conn(async |mut conn| {
                redis::cmd("SSCAN")
                    .arg(&by_worker)
                    .arg(cursor)
                    .arg("COUNT")
                    .arg(DO_ALARM_CLEANUP_BATCH_SIZE)
                    .query_async(&mut conn)
                    .await
            })
            .await?;
        if !job_ids.is_empty() {
            snapshot_has_members = true;
            app.redis
                .with_conn(async |mut conn| {
                    let mut pipe = redis::pipe();
                    pipe.atomic();
                    pipe.cmd("SADD").arg(&snapshot_key).arg(job_ids);
                    pipe.ignore();
                    pipe.cmd("EXPIRE")
                        .arg(&snapshot_key)
                        .arg(DO_ALARM_CLEANUP_SNAPSHOT_TTL_SECONDS);
                    pipe.ignore();
                    pipe.query_async::<()>(&mut conn).await
                })
                .await?;
        }
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    if !snapshot_has_members {
        return Ok(DoAlarmMutationResponse {
            ok: true,
            job_id: None,
            changed: false,
            deleted: 0,
        });
    }
    let mut deleted = 0usize;
    loop {
        let job_ids: Vec<String> = app
            .redis
            .with_conn(async |mut conn| {
                redis::cmd("SPOP")
                    .arg(&snapshot_key)
                    .arg(DO_ALARM_CLEANUP_BATCH_SIZE)
                    .query_async(&mut conn)
                    .await
            })
            .await?;
        if job_ids.is_empty() {
            break;
        }
        let outcomes: Vec<i64> = app
            .redis
            .with_conn(async |mut conn| {
                let mut pipe = redis::pipe();
                for job_id in &job_ids {
                    let keys = crate::DoAlarmJobKeys::new(job_id.to_string());
                    let state = keys.state();
                    let due = keys.due();
                    let ready = keys.ready();
                    append_eval_cmd(
                        &mut pipe,
                        CLEANUP_DO_ALARM_FOR_STORAGE_SCRIPT,
                        &[
                            state.as_str(),
                            due.as_str(),
                            ready.as_str(),
                            by_worker.as_str(),
                        ],
                        &[job_id.as_str(), req.do_storage_id.as_str()],
                    );
                }
                pipe.cmd("EXPIRE")
                    .arg(&snapshot_key)
                    .arg(DO_ALARM_CLEANUP_SNAPSHOT_TTL_SECONDS);
                pipe.ignore();
                pipe.query_async(&mut conn).await
            })
            .await?;
        for outcome in outcomes {
            if outcome == 1 {
                deleted += 1;
            }
        }
    }
    app.redis
        .with_conn(async |mut conn| {
            redis::cmd("DEL")
                .arg(&snapshot_key)
                .query_async::<()>(&mut conn)
                .await
        })
        .await?;
    Ok(DoAlarmMutationResponse {
        ok: true,
        job_id: None,
        changed: deleted > 0,
        deleted,
    })
}
