use crate::{AppState, Redis, WorkflowError, WorkflowResult, schema_version_key};

pub(crate) const WORKFLOWS_SCHEMA_VERSION: &str = "3";
const SCHEMA_RECOVERY_GUIDANCE: &str = "point WORKFLOWS_REDIS_URL at an endpoint with an empty dedicated DB 2, or clear DB 2 only after confirming it is dedicated and disposable";
const SCHEMA2_MIGRATION_GUIDANCE: &str = "follow the documented quiescence, old-participant drain, and final-release startup order; migrate with `/workflows schema3-migrate check`, then `apply` or `resume`; DB 15 is retained unless --delete-archive is explicitly selected after verified completion. Clear dedicated DB 2 instead only if its Workflow state and Workflows-owned Durable Object alarm projection are independently confirmed disposable";

pub(crate) async fn ensure_workflows_schema(state: &AppState) -> WorkflowResult<()> {
    ensure_schema_on(&state.redis).await
}

async fn ensure_schema_on(redis: &Redis) -> WorkflowResult<()> {
    let key = schema_version_key();
    let version: Option<String> = redis
        .with_conn(async |mut conn| redis::cmd("GET").arg(key).query_async(&mut conn).await)
        .await?;
    if version.is_some() {
        return validate_installed_schema(version.as_deref());
    }

    let key_count: u64 = redis
        .with_conn(async |mut conn| redis::cmd("DBSIZE").query_async(&mut conn).await)
        .await?;
    if key_count != 0 {
        // Another new replica may install the marker between the initial GET and DBSIZE.
        let concurrent_version: Option<String> = redis
            .with_conn(async |mut conn| redis::cmd("GET").arg(key).query_async(&mut conn).await)
            .await?;
        return validate_installed_schema(concurrent_version.as_deref());
    }

    let _: Option<String> = redis
        .with_conn(async |mut conn| {
            redis::cmd("SET")
                .arg(key)
                .arg(WORKFLOWS_SCHEMA_VERSION)
                .arg("NX")
                .query_async(&mut conn)
                .await
        })
        .await?;

    let adopted: Option<String> = redis
        .with_conn(async |mut conn| redis::cmd("GET").arg(key).query_async(&mut conn).await)
        .await?;
    validate_installed_schema(adopted.as_deref())
}

fn validate_installed_schema(version: Option<&str>) -> WorkflowResult<()> {
    match version {
        Some(WORKFLOWS_SCHEMA_VERSION) => Ok(()),
        Some("2") => Err(schema_mismatch(format!(
            "Workflows DB 2 schema is 2, expected {WORKFLOWS_SCHEMA_VERSION}; {SCHEMA2_MIGRATION_GUIDANCE}"
        ))),
        Some(found) => Err(schema_mismatch(format!(
            "Workflows DB 2 schema is {found}, expected {WORKFLOWS_SCHEMA_VERSION}; {SCHEMA_RECOVERY_GUIDANCE}"
        ))),
        None => Err(schema_mismatch(format!(
            "Workflows DB 2 is not empty and has no schema marker; {SCHEMA_RECOVERY_GUIDANCE} before starting schema {WORKFLOWS_SCHEMA_VERSION}"
        ))),
    }
}

fn schema_mismatch(message: String) -> WorkflowError {
    WorkflowError::schema_mismatch(message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn installed_schema_accepts_only_the_current_version() {
        validate_installed_schema(Some(WORKFLOWS_SCHEMA_VERSION))
            .expect("current schema must be accepted");

        for version in [None, Some("2")] {
            let error = validate_installed_schema(version)
                .expect_err("missing or stale schema must fail closed");
            assert_eq!(error.code, "workflow_schema_mismatch");
            if version == Some("2") {
                assert!(error.message.contains("schema3-migrate check"));
                assert!(error.message.contains("independently confirmed disposable"));
            } else {
                assert!(error.message.contains(SCHEMA_RECOVERY_GUIDANCE));
            }
        }
    }
}
