#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("healthcheck") => std::process::exit(workflows::healthcheck()),
        Some("schema3-reset") => {
            let mode = match args.next().as_deref() {
                Some("check") => workflows::Schema3ResetMode::Check,
                Some("apply") => workflows::Schema3ResetMode::Apply,
                Some("resume") => workflows::Schema3ResetMode::Resume,
                Some(arg) => {
                    return Err(std::io::Error::other(format!(
                        "unknown schema3-reset mode `{arg}`; expected `check`, `apply`, or `resume`"
                    ))
                    .into());
                }
                None => {
                    return Err(std::io::Error::other(
                        "schema3-reset requires mode `check`, `apply`, or `resume`",
                    )
                    .into());
                }
            };
            if let Some(arg) = args.next() {
                return Err(std::io::Error::other(format!(
                    "unexpected schema3-reset argument `{arg}`"
                ))
                .into());
            }
            workflows::run_schema3_reset(mode).await?;
            return Ok(());
        }
        Some(arg) => {
            return Err(std::io::Error::other(format!("unknown workflows command `{arg}`")).into());
        }
        None => {}
    }
    workflows::run().await
}
