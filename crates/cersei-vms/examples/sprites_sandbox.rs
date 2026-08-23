//! End-to-end tour of the Sprites.dev backend.
//!
//! Requires a Sprites.dev API token:
//!
//! ```sh
//! SPRITES_TOKEN=... cargo run -p cersei-vms --features backend-sprites --example sprites_sandbox
//! ```

use cersei_vms::prelude::*;
use cersei_vms::SpritesRuntime;

#[tokio::main]
async fn main() -> Result<()> {
    let runtime = SpritesRuntime::from_env()?;
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
                .with_label("cersei.example", "sprites_sandbox"),
        )
        .await?;
    println!("created sandbox {}", sandbox.id());

    let out = sandbox
        .commands()
        .run(RunRequest::new("echo \"$GREETING\" && uname -a"))
        .await?;
    println!("exit {}: {}", out.exit_code, out.stdout.trim());

    let fs = sandbox.filesystem();
    fs.write("/work/notes/hello.txt", b"persisted in the sprite\n")
        .await?;
    let bytes = fs.read("/work/notes/hello.txt").await?;
    println!("read back: {}", String::from_utf8_lossy(&bytes).trim());

    let snap = sandbox.snapshot().await?;
    println!("checkpointed as {snap}");

    sandbox.kill().await?;
    println!("sandbox destroyed");
    Ok(())
}
