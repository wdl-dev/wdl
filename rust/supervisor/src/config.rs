use crate::log;
use wdl_rust_common::env::positive_or;

pub(crate) const DEFAULT_DRAIN_TIMEOUT_MS: u64 = 10_000;
pub(crate) const DEFAULT_SHUTDOWN_TIMEOUT_MS: u64 = 15_000;
pub(crate) const DEFAULT_OWNER_TTL_SECONDS: u64 = 120;
pub(crate) const DEFAULT_RENEW_START_DELAY_MS: u64 = 1_000;
pub(crate) const DEFAULT_RENEW_TIMEOUT_MS: u64 = 5_000;
pub(crate) const DEFAULT_RENEW_ERROR_GRACE_MS: u64 = 10_000;
pub(crate) const DRAIN_RETRY_DELAY_MS: u64 = 250;

pub(crate) const WORKERD: &str = "/usr/local/bin/workerd";

// d1-runtime has no local-variant capnp because it makes no workerd-to-workerd
// outbound through a service binding (Redis + EFS only). Both compose dev and
// production load the same compiled `.bin`.
pub(crate) const D1_COMPILED_CONFIG: &str = "/app/dist/workerd-configs/d1-runtime.bin";

// do-runtime has a local-variant capnp that reroutes the `d1-runtime` service
// binding through Envoy mesh in compose; production goes through ECS Service
// Connect via the production .bin. WDL_WORKERD_CONFIG_VARIANT=local picks the
// local-variant compiled bundle to match.
pub(crate) const DO_COMPILED_CONFIG_PRODUCTION: &str = "/app/dist/workerd-configs/do-runtime.bin";
pub(crate) const DO_COMPILED_CONFIG_LOCAL: &str = "/app/dist/workerd-configs/do-runtime-local.bin";

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum KillSignal {
    Term,
    Kill,
}

pub(crate) struct SupervisorConfig {
    pub(crate) service: &'static str,
    pub(crate) env_prefix: &'static str,
    pub(crate) drain_url: &'static str,
    pub(crate) renew_url: &'static str,
    pub(crate) drain_complete_event: &'static str,
    pub(crate) drain_failed_event: &'static str,
    pub(crate) shutdown_after_drain_failure_event: &'static str,
    pub(crate) renew_failed_event: &'static str,
    pub(crate) renew_partial_event: &'static str,
    pub(crate) renew_error_event: &'static str,
    pub(crate) drain_failure_on_errors_field: bool,
    pub(crate) drain_request_id_prefix: Option<&'static str>,
    pub(crate) kill_on_drain_success: KillSignal,
    pub(crate) repeated_signal_escalates: bool,
    // DO uses 1000 because its post-drain SIGKILL must land before the ECS
    // stopTimeout; D1 uses 0 because its drain-success SIGTERM lets workerd's
    // graceful window absorb slack.
    pub(crate) stop_buffer_ms: u64,
}

pub(crate) static D1_CONFIG: SupervisorConfig = SupervisorConfig {
    service: "d1-runtime-supervisor",
    env_prefix: "D1_",
    drain_url: "http://127.0.0.1:8787/internal/d1/drain",
    renew_url: "http://127.0.0.1:8787/internal/d1/renew",
    drain_complete_event: "d1_drain_complete",
    drain_failed_event: "d1_drain_failed",
    shutdown_after_drain_failure_event: "d1_shutdown_after_drain_failure",
    renew_failed_event: "d1_renew_failed",
    renew_partial_event: "d1_renew_partial",
    renew_error_event: "d1_renew_error",
    drain_failure_on_errors_field: true,
    drain_request_id_prefix: None,
    kill_on_drain_success: KillSignal::Term,
    repeated_signal_escalates: false,
    stop_buffer_ms: 0,
};

pub(crate) static DO_CONFIG: SupervisorConfig = SupervisorConfig {
    service: "do-runtime-supervisor",
    env_prefix: "DO_",
    drain_url: "http://127.0.0.1:8788/internal/do/drain",
    renew_url: "http://127.0.0.1:8788/internal/do/renew",
    drain_complete_event: "do_drain_complete",
    drain_failed_event: "do_drain_failed",
    shutdown_after_drain_failure_event: "do_shutdown_after_drain_failure",
    renew_failed_event: "do_renew_failed",
    renew_partial_event: "do_renew_partial",
    renew_error_event: "do_renew_error",
    drain_failure_on_errors_field: false,
    drain_request_id_prefix: Some("do-drain"),
    // SIGKILL on drain success: workerd's post-SIGTERM graceful window keeps
    // the HTTP listener half-alive (returns 504s without responses), so the
    // next task takeover would race that window without an immediate kill.
    kill_on_drain_success: KillSignal::Kill,
    repeated_signal_escalates: true,
    stop_buffer_ms: 1000,
};

pub(crate) fn positive_int_env(prefix: &str, name: &str, fallback: u64) -> u64 {
    let key = format!("{prefix}{name}");
    positive_or(std::env::var(&key).ok(), fallback)
}

pub(crate) fn positive_int_env_chained(prefix: &str, names: &[&str], fallback: u64) -> u64 {
    for name in names {
        let key = format!("{prefix}{name}");
        let value = positive_or(std::env::var(&key).ok(), 0);
        if value > 0 {
            return value;
        }
    }
    fallback
}

pub(crate) fn shutdown_timeout_ms(config: &SupervisorConfig) -> u64 {
    positive_int_env_chained(
        config.env_prefix,
        &["SHUTDOWN_TIMEOUT_MS", "WORKERD_STOP_TIMEOUT_MS"],
        DEFAULT_SHUTDOWN_TIMEOUT_MS,
    )
}

pub(crate) fn drain_timeout_ms(config: &SupervisorConfig) -> u64 {
    positive_int_env(
        config.env_prefix,
        "DRAIN_TIMEOUT_MS",
        DEFAULT_DRAIN_TIMEOUT_MS,
    )
}

pub(crate) fn owner_ttl_seconds(config: &SupervisorConfig) -> u64 {
    positive_int_env(
        config.env_prefix,
        "OWNER_TTL_SECONDS",
        DEFAULT_OWNER_TTL_SECONDS,
    )
}

pub(crate) fn owner_ttl_ms(config: &SupervisorConfig) -> u64 {
    owner_ttl_seconds(config).saturating_mul(1000)
}

pub(crate) fn renew_interval_ms(config: &SupervisorConfig) -> u64 {
    let explicit = positive_int_env(config.env_prefix, "RENEW_INTERVAL_MS", 0);
    if explicit > 0 {
        return explicit;
    }
    let ttl_ms = owner_ttl_ms(config);
    std::cmp::max(1_000, ttl_ms / 3)
}

pub(crate) fn renew_timeout_ms(config: &SupervisorConfig) -> u64 {
    positive_int_env(
        config.env_prefix,
        "RENEW_TIMEOUT_MS",
        DEFAULT_RENEW_TIMEOUT_MS,
    )
}

pub(crate) fn renew_start_delay_ms(config: &SupervisorConfig) -> u64 {
    positive_int_env(
        config.env_prefix,
        "RENEW_START_DELAY_MS",
        DEFAULT_RENEW_START_DELAY_MS,
    )
}

pub(crate) fn renew_error_grace_ms(config: &SupervisorConfig) -> u64 {
    positive_int_env(
        config.env_prefix,
        "RENEW_ERROR_GRACE_MS",
        DEFAULT_RENEW_ERROR_GRACE_MS,
    )
}

pub(crate) fn signal_exit_code(signal: i32) -> i32 {
    // POSIX shell convention: 128 + signal number (SIGINT=2, SIGTERM=15).
    match signal {
        libc::SIGINT => 130,
        libc::SIGTERM => 143,
        _ => 1,
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TimingWarning {
    ShutdownTimeoutTooLow,
    DrainTimeoutExceedsOwnerTtl,
    DrainTimeoutNearOwnerTtl,
}

// Pure decision function (no env reads, no logging) for direct test drive.
pub(crate) fn evaluate_shutdown_timing(
    drain_ms: u64,
    shutdown_ms: u64,
    owner_ttl_ms: u64,
    stop_buffer_ms: u64,
) -> Vec<TimingWarning> {
    let mut out = Vec::new();
    if shutdown_ms <= drain_ms.saturating_add(stop_buffer_ms) {
        out.push(TimingWarning::ShutdownTimeoutTooLow);
    }
    if drain_ms >= owner_ttl_ms {
        out.push(TimingWarning::DrainTimeoutExceedsOwnerTtl);
        return out;
    }
    if drain_ms >= owner_ttl_ms / 2 {
        out.push(TimingWarning::DrainTimeoutNearOwnerTtl);
    }
    out
}

pub(crate) fn validate_shutdown_timing(config: &SupervisorConfig) {
    let drain_ms = drain_timeout_ms(config);
    let shutdown_ms = shutdown_timeout_ms(config);
    let owner_ttl_ms = owner_ttl_ms(config);
    for warning in
        evaluate_shutdown_timing(drain_ms, shutdown_ms, owner_ttl_ms, config.stop_buffer_ms)
    {
        match warning {
            TimingWarning::ShutdownTimeoutTooLow => log::warn(
                config.service,
                "shutdown_timeout_too_low",
                serde_json::json!({
                    "shutdown_timeout_ms": shutdown_ms,
                    "drain_timeout_ms": drain_ms,
                }),
            ),
            TimingWarning::DrainTimeoutExceedsOwnerTtl => log::warn(
                config.service,
                "drain_timeout_exceeds_owner_ttl",
                serde_json::json!({
                    "drain_timeout_ms": drain_ms,
                    "owner_ttl_ms": owner_ttl_ms,
                }),
            ),
            TimingWarning::DrainTimeoutNearOwnerTtl => log::warn(
                config.service,
                "drain_timeout_near_owner_ttl",
                serde_json::json!({
                    "drain_timeout_ms": drain_ms,
                    "owner_ttl_ms": owner_ttl_ms,
                    "renew_interval_ms": renew_interval_ms(config),
                }),
            ),
        }
    }
}

pub(crate) fn workerd_args(compiled_config: &str, experimental: bool) -> Vec<String> {
    let mut args = vec!["serve".into(), "-b".into(), compiled_config.into()];
    if experimental {
        args.push("--experimental".into());
    }
    args
}

pub(crate) fn pick_do_compiled_config() -> &'static str {
    if std::env::var("WDL_WORKERD_CONFIG_VARIANT").as_deref() == Ok("local") {
        DO_COMPILED_CONFIG_LOCAL
    } else {
        DO_COMPILED_CONFIG_PRODUCTION
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, OnceLock};

    use super::*;

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn temp_env<R>(key: &str, value: Option<&str>, f: impl FnOnce() -> R) -> R {
        let _guard = ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("env mutex poisoned");
        let prev = std::env::var(key).ok();
        // SAFETY: these tests serialize environment mutation with ENV_LOCK, so no
        // concurrent test can read or write the same process environment while this
        // temporary override is active.
        match value {
            Some(v) => unsafe { std::env::set_var(key, v) },
            // SAFETY: Same serialization rationale as set_var above.
            None => unsafe { std::env::remove_var(key) },
        }
        let result = f();
        // SAFETY: ENV_LOCK is still held, so restoring the process environment is
        // serialized with all other test environment mutation in this module.
        match prev {
            Some(p) => unsafe { std::env::set_var(key, p) },
            // SAFETY: Same serialization rationale as set_var above.
            None => unsafe { std::env::remove_var(key) },
        }
        result
    }

    #[test]
    fn positive_int_env_accepts_only_strict_positive_integer_strings() {
        temp_env("WDLTEST_VAL", None, || {
            assert_eq!(positive_int_env("WDLTEST_", "VAL", 7), 7);
        });
        temp_env("WDLTEST_VAL", Some(""), || {
            assert_eq!(positive_int_env("WDLTEST_", "VAL", 7), 7);
        });
        temp_env("WDLTEST_VAL", Some("0"), || {
            assert_eq!(positive_int_env("WDLTEST_", "VAL", 7), 7);
        });
        temp_env("WDLTEST_VAL", Some("-3"), || {
            assert_eq!(positive_int_env("WDLTEST_", "VAL", 7), 7);
        });
        temp_env("WDLTEST_VAL", Some("nope"), || {
            assert_eq!(positive_int_env("WDLTEST_", "VAL", 7), 7);
        });
        temp_env("WDLTEST_VAL", Some("12.9"), || {
            assert_eq!(positive_int_env("WDLTEST_", "VAL", 7), 7);
        });
        temp_env("WDLTEST_VAL", Some("1e3"), || {
            assert_eq!(positive_int_env("WDLTEST_", "VAL", 7), 7);
        });
        temp_env("WDLTEST_VAL", Some("18446744073709551616"), || {
            assert_eq!(positive_int_env("WDLTEST_", "VAL", 7), 7);
        });
        temp_env("WDLTEST_VAL", Some("42"), || {
            assert_eq!(positive_int_env("WDLTEST_", "VAL", 7), 42);
        });
    }

    #[test]
    fn owner_ttl_ms_saturates_extreme_env_values() {
        temp_env(
            "WDLTEST_OWNER_TTL_SECONDS",
            Some("18446744073709552"),
            || {
                let config = SupervisorConfig {
                    env_prefix: "WDLTEST_",
                    ..D1_CONFIG
                };
                assert_eq!(owner_ttl_ms(&config), u64::MAX);
            },
        );
    }

    #[test]
    fn signal_exit_code_maps_graceful_signals_to_posix_else_generic() {
        assert_eq!(signal_exit_code(libc::SIGINT), 130);
        assert_eq!(signal_exit_code(libc::SIGTERM), 143);
        assert_eq!(signal_exit_code(libc::SIGHUP), 1);
    }

    #[test]
    fn workerd_args_adds_experimental_only_when_requested() {
        assert_eq!(
            workerd_args("/app/dist/workerd-configs/d1-runtime.bin", false),
            vec!["serve", "-b", "/app/dist/workerd-configs/d1-runtime.bin"]
        );
        assert_eq!(
            workerd_args("/app/dist/workerd-configs/do-runtime.bin", true),
            vec![
                "serve",
                "-b",
                "/app/dist/workerd-configs/do-runtime.bin",
                "--experimental"
            ]
        );
    }

    #[test]
    fn evaluate_shutdown_timing_default_no_warnings() {
        assert_eq!(evaluate_shutdown_timing(10_000, 15_000, 120_000, 0), vec![]);
        assert_eq!(
            evaluate_shutdown_timing(10_000, 15_000, 120_000, 1_000),
            vec![]
        );
    }

    #[test]
    fn evaluate_shutdown_timing_flags_shutdown_too_low() {
        assert_eq!(
            evaluate_shutdown_timing(10_000, 10_000, 120_000, 0),
            vec![TimingWarning::ShutdownTimeoutTooLow]
        );
        assert_eq!(
            evaluate_shutdown_timing(10_000, 11_000, 120_000, 1_000),
            vec![TimingWarning::ShutdownTimeoutTooLow]
        );
    }

    #[test]
    fn evaluate_shutdown_timing_flags_drain_exceeds_ttl_and_short_circuits() {
        assert_eq!(
            evaluate_shutdown_timing(120_000, 130_000, 120_000, 0),
            vec![TimingWarning::DrainTimeoutExceedsOwnerTtl]
        );
    }

    #[test]
    fn evaluate_shutdown_timing_flags_drain_near_ttl_at_half() {
        assert_eq!(
            evaluate_shutdown_timing(60_000, 70_000, 120_000, 0),
            vec![TimingWarning::DrainTimeoutNearOwnerTtl]
        );
    }

    #[test]
    fn evaluate_shutdown_timing_no_near_warning_just_below_half() {
        assert_eq!(evaluate_shutdown_timing(59_999, 70_000, 120_000, 0), vec![]);
    }
}
