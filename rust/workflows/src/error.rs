use axum::http::StatusCode;

#[derive(Debug)]
pub(crate) struct WorkflowError {
    pub(crate) code: &'static str,
    pub(crate) message: String,
    pub(crate) status: StatusCode,
}

impl WorkflowError {
    pub(crate) fn internal_error(message: impl Into<String>) -> Self {
        Self {
            code: "internal_error",
            message: message.into(),
            status: StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub(crate) fn invalid_request(message: impl Into<String>) -> Self {
        Self {
            code: "invalid_request",
            message: message.into(),
            status: StatusCode::BAD_REQUEST,
        }
    }

    pub(crate) fn request_too_large(message: impl Into<String>) -> Self {
        Self {
            code: "request_too_large",
            message: message.into(),
            status: StatusCode::PAYLOAD_TOO_LARGE,
        }
    }

    pub(crate) fn unavailable(message: impl Into<String>) -> Self {
        Self {
            code: "workflow_backend_unavailable",
            message: message.into(),
            status: StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    pub(crate) fn payload_missing(message: impl Into<String>) -> Self {
        Self {
            code: "workflow_payload_missing",
            message: message.into(),
            status: StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self {
            code: "workflow_instance_not_found",
            message: message.into(),
            status: StatusCode::NOT_FOUND,
        }
    }

    pub(crate) fn invalid_state(message: impl Into<String>) -> Self {
        Self {
            code: "workflow_invalid_state",
            message: message.into(),
            status: StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub(crate) fn conflict(message: impl Into<String>) -> Self {
        Self {
            code: "workflow_conflict",
            message: message.into(),
            status: StatusCode::CONFLICT,
        }
    }

    pub(crate) fn not_exported(message: impl Into<String>) -> Self {
        Self {
            code: "workflow_not_exported",
            message: message.into(),
            status: StatusCode::CONFLICT,
        }
    }

    pub(crate) fn step_mismatch(message: impl Into<String>) -> Self {
        Self {
            code: "workflow_step_mismatch",
            message: message.into(),
            status: StatusCode::CONFLICT,
        }
    }

    pub(crate) fn deleting(message: impl Into<String>) -> Self {
        Self {
            code: "workflow_worker_deleting",
            message: message.into(),
            status: StatusCode::CONFLICT,
        }
    }

    pub(crate) fn schema_mismatch(message: impl Into<String>) -> Self {
        Self {
            code: "workflow_schema_mismatch",
            message: message.into(),
            status: StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub(crate) fn migration_pending(message: impl Into<String>) -> Self {
        Self {
            code: "workflow_migration_pending",
            message: message.into(),
            status: StatusCode::CONFLICT,
        }
    }
}

impl From<redis::RedisError> for WorkflowError {
    fn from(err: redis::RedisError) -> Self {
        Self {
            code: "redis_error",
            message: err.to_string(),
            status: StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::WorkflowError;

    #[test]
    fn migration_pending_matches_the_cross_language_contract() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/workflow-service-errors.json"
        ))
        .expect("Workflow service error fixture parses");
        let expected = &fixture["migrationPending"];
        let error = WorkflowError::migration_pending("migration pending");

        assert_eq!(expected["code"].as_str(), Some(error.code));
        assert_eq!(
            expected["status"].as_u64(),
            Some(error.status.as_u16().into())
        );
    }
}
