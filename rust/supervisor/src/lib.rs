mod config;
mod drain;
mod log;
mod process;
mod renew;
mod response_body;

pub(crate) use config::*;
pub(crate) use wdl_rust_common::text::truncate_chars;

pub async fn run_d1() -> ! {
    process::run(&D1_CONFIG, WORKERD, workerd_args(D1_COMPILED_CONFIG, false)).await
}

pub async fn run_do() -> ! {
    let prevent_eviction = match do_prevent_eviction() {
        Ok(value) => value,
        Err(error_message) => {
            log::error(
                DO_CONFIG.service,
                "do_prevent_eviction_config_error",
                serde_json::json!({ "error_message": error_message }),
            );
            std::process::exit(1);
        }
    };
    let compiled_config = pick_do_compiled_config(prevent_eviction);
    log::info(
        DO_CONFIG.service,
        "do_actor_residency_configured",
        serde_json::json!({
            "prevent_eviction": prevent_eviction,
            "workerd_config": compiled_config,
        }),
    );
    process::run(&DO_CONFIG, WORKERD, workerd_args(compiled_config, true)).await
}
