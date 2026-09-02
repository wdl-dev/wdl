pub(crate) const SERVICE: &str = "workflows";

mod api;
mod config;
mod error;
mod keys;
mod observability;
mod schema;
mod schema_migration;
mod server;
mod state;
#[cfg(test)]
mod tests;

pub(crate) use api::*;
pub(crate) use config::*;
pub(crate) use error::*;
pub(crate) use keys::*;
pub(crate) use observability::*;
pub(crate) use schema::*;
pub(crate) use schema_migration::workflow_migration_pending;
pub use schema_migration::{Schema3ResetMode, run_schema3_reset};
pub use server::{healthcheck, run};
pub(crate) use state::*;

pub(crate) type WorkflowResult<T> = Result<T, WorkflowError>;
