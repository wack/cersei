//! End-to-end tour of the Vercel Sandbox backend.
//!
//! Requires a Vercel access token plus team/project ids:
//!
//! ```sh
//! VERCEL_TOKEN=... VERCEL_TEAM_ID=team_xxx VERCEL_PROJECT_ID=prj_xxx \
//!   cargo run -p cersei-vms --features backend-vercel --example vercel_sandbox
//! ```

use cersei_vms::prelude::*;
use cersei_vms::VercelRuntime;

#[tokio::main]
async fn main() -> Result<()> {
    let runtime = VercelRuntime::from_env()?;
    println!(
        "runtime: {} (remote: {})",
        runtime.name(),
        runtime.capabilities().remote
    );

    let sandbox = runtime
        .create(
            SandboxOpts::default()
                .with_workdir("/work")
                .with_env("GREETING", "hello from cersei")
                .with_label("cersei.example", "vercel_sandbox"),
        )
        .await?;
    println!("created sandbox {}", sandbox.id());

    let out = sandbox
        .commands()
        .run(RunRequest::new("echo \"$GREETING\" && uname -a"))
        .await?;
    println!("exit {}: {}", out.exit_code, out.stdout.trim());

    let fs = sandbox.filesystem();
    fs.write("/work/notes/hello.txt", b"persisted in the microVM\n")
        .await?;
    let bytes = fs.read("/work/notes/hello.txt").await?;
    println!("read back: {}", String::from_utf8_lossy(&bytes).trim());

    let snap = sandbox.snapshot().await?;
    println!("snapshotted as {snap}");

    sandbox.kill().await?;
    println!("sandbox destroyed");
    Ok(())
}
