mod config;
mod drain;
mod log;
mod process;
mod renew;

pub(crate) use config::*;
pub(crate) use wdl_rust_common::text::truncate_chars;

pub async fn run_d1() -> ! {
    process::run(&D1_CONFIG, WORKERD, workerd_args(D1_COMPILED_CONFIG, false)).await
}

pub async fn run_do() -> ! {
    process::run(
        &DO_CONFIG,
        WORKERD,
        workerd_args(pick_do_compiled_config(), true),
    )
    .await
}
