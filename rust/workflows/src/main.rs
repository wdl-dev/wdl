#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("healthcheck") => std::process::exit(workflows::healthcheck()),
        Some("schema3-migrate") => {
            let mode = match args.next().as_deref() {
                Some("check") => workflows::Schema3MigrationMode::Check,
                Some("apply") => workflows::Schema3MigrationMode::Apply,
                Some("resume") => workflows::Schema3MigrationMode::Resume,
                Some(arg) => {
                    return Err(std::io::Error::other(format!(
                        "unknown schema3-migrate mode `{arg}`; expected `check`, `apply`, or `resume`"
                    ))
                    .into());
                }
                None => {
                    return Err(std::io::Error::other(
                        "schema3-migrate requires mode `check`, `apply`, or `resume`",
                    )
                    .into());
                }
            };
            let delete_archive = match args.next().as_deref() {
                None => false,
                Some("--delete-archive") => true,
                Some(arg) => {
                    return Err(std::io::Error::other(format!(
                        "unexpected schema3-migrate argument `{arg}`"
                    ))
                    .into());
                }
            };
            if let Some(arg) = args.next() {
                return Err(std::io::Error::other(format!(
                    "unexpected schema3-migrate argument `{arg}`"
                ))
                .into());
            }
            workflows::run_schema3_migration(mode, delete_archive).await?;
            return Ok(());
        }
        Some(arg) => {
            return Err(std::io::Error::other(format!("unknown workflows command `{arg}`")).into());
        }
        None => {}
    }
    workflows::run().await
}
