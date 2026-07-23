// Queue scheduling is split by responsibility so future discovery-index work can
// change one path without mixing consume, delayed, retry, and orphan semantics.
pub(crate) const MAX_BATCH_SIZE_CAP: usize = 100;
pub(crate) const MAX_BATCH_TIMEOUT_MS: i64 = 60_000;
pub(crate) const MAX_QUEUE_DELAY_SECONDS: i64 = 86_400;
pub(crate) const MAX_RETRIES: i64 = 100;

mod consume;
mod delayed;
mod delivery;
mod keys;
mod orphan;
mod registry;
mod types;

fn redis_error_code_is(err: &redis::RedisError, expected: &str) -> bool {
    err.code().is_some_and(|code| code == expected)
}

fn redis_error_is_nogroup(err: &redis::RedisError) -> bool {
    redis_error_code_is(err, "NOGROUP")
}

pub(crate) use consume::*;
pub(crate) use delayed::*;
pub(crate) use delivery::*;
pub(crate) use keys::*;
pub(crate) use orphan::*;
pub(crate) use registry::*;
pub(crate) use types::*;
