//! Serialized process-environment overrides for Rust tests.

use std::ffi::OsString;
use std::sync::Mutex;

static ENV_LOCK: Mutex<()> = Mutex::new(());

struct EnvRestore(Vec<(String, Option<OsString>)>);

impl Drop for EnvRestore {
    fn drop(&mut self) {
        for (key, value) in self.0.iter().rev() {
            // SAFETY: ENV_LOCK remains held until after this restore guard drops.
            match value {
                Some(value) => unsafe { std::env::set_var(key, value) },
                None => unsafe { std::env::remove_var(key) },
            }
        }
    }
}

pub fn with_temp_env<R>(key: &str, value: Option<&str>, f: impl FnOnce() -> R) -> R {
    with_temp_envs(&[(key, value)], f)
}

pub fn with_temp_envs<R>(items: &[(&str, Option<&str>)], f: impl FnOnce() -> R) -> R {
    let _lock = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let _restore = EnvRestore(
        items
            .iter()
            .map(|(key, _)| ((*key).to_string(), std::env::var_os(key)))
            .collect(),
    );
    for (key, value) in items {
        // SAFETY: callers keep all environment reads inside the closure, and every
        // WDL test environment override uses this crate-wide lock.
        match value {
            Some(value) => unsafe { std::env::set_var(key, value) },
            None => unsafe { std::env::remove_var(key) },
        }
    }
    f()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restores_multiple_values_after_unwind_and_recovers_poisoned_lock() {
        const FIRST: &str = "WDL_COMMON_TEST_ENV_FIRST";
        const SECOND: &str = "WDL_COMMON_TEST_ENV_SECOND";
        let before = [std::env::var_os(FIRST), std::env::var_os(SECOND)];

        let result = std::panic::catch_unwind(|| {
            with_temp_envs(&[(FIRST, Some("one")), (SECOND, None)], || {
                assert_eq!(std::env::var(FIRST).as_deref(), Ok("one"));
                assert_eq!(std::env::var_os(SECOND), None);
                panic!("exercise panic-safe restore");
            });
        });
        assert!(result.is_err());
        assert_eq!(std::env::var_os(FIRST), before[0]);
        assert_eq!(std::env::var_os(SECOND), before[1]);

        with_temp_env(FIRST, Some("two"), || {
            assert_eq!(std::env::var(FIRST).as_deref(), Ok("two"));
        });
        assert_eq!(std::env::var_os(FIRST), before[0]);
    }
}
