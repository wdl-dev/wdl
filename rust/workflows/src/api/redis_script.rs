use redis::FromRedisValue;
use wdl_rust_common::redis_eval::StaticRedisScript;

use crate::{AppState, WorkflowResult};

pub(crate) async fn eval_script<T>(
    app: &AppState,
    script: &StaticRedisScript,
    keys: &[&str],
    args: &[&str],
) -> WorkflowResult<T>
where
    T: FromRedisValue,
{
    app.redis
        .with_conn(async |mut conn| {
            script
                .prepare_invoke(keys, args)
                .invoke_async(&mut conn)
                .await
        })
        .await
        .map_err(Into::into)
}
