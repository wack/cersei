//! `SpritesRuntime` — remote sandboxes on [Sprites.dev](https://sprites.dev)
//! (a Fly.io product) via the official `sprites` Rust SDK.
//!
//! Each Cersei sandbox maps 1:1 to a sprite — an isolated remote VM with a
//! persistent filesystem, checkpoint/restore, and per-domain network policy.
//!
//! Lifecycle:
//!   - `create()`  → `client.create_with_config(name, ...)`
//!   - `kill()`    → `sprite.destroy()`
//!   - `snapshot()` → `sprite.checkpoint(...)` + JSON manifest
//!   - `restore(snap)` → attach to the source sprite, `sprite.restore(checkpoint)`
//!
//! Command execution rides the SDK's exec WebSocket:
//!   - `run`    → `sprite.command("/bin/sh").arg("-c").arg(cmd).output()`
//!   - `stream` → `spawn()` + chunked stdout/stderr forwarding
//!
//! Filesystem ops map straight onto the sprite Filesystem API
//! (`read_file`, `write_file`, `read_dir`, `stat`, ...).
//!
//! Backend-specific notes:
//!   - Sprites boot from a fixed base image; `SandboxOpts::image` is ignored.
//!   - `SandboxOpts::env` is injected per-command (the create API has no env).
//!   - Volume mounts are not supported and are rejected at `create()`.
//!   - Checkpoints restore *in place*: `restore()` re-attaches to the sprite
//!     the snapshot was taken from rather than cloning a new one.

use crate::commands::{CommandStream, Commands, StreamChunk};
use crate::error::{Result, VmError};
use crate::filesystem::{Filesystem, WatchStream};
use crate::primitives::{KvStore, Mailbox};
use crate::runtime::{Sandbox, SandboxHandle, SandboxRuntime};
use crate::snapshot::{SnapshotManifest, SnapshotRegistry};
use crate::types::{
    FileEntry, FileKind, RunOutput, RunRequest, RuntimeCaps, SandboxId, SandboxInfo, SandboxOpts,
    SandboxStatus, Signal, SnapshotId,
};
use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use sprites::{ListOptions, SpriteConfig, SpriteStatus, SpritesClient};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);
const MAX_TIMEOUT: Duration = Duration::from_secs(600);
const SPRITE_NAME_PREFIX: &str = "cersei-vm-";

/// Env var holding the Sprites.dev API token for [`SpritesRuntime::from_env`].
pub const SPRITES_TOKEN_ENV: &str = "SPRITES_TOKEN";
/// Optional env var overriding the API base URL.
pub const SPRITES_BASE_URL_ENV: &str = "SPRITES_BASE_URL";

pub struct SpritesRuntime {
    inner: Arc<SpritesInner>,
}

struct SpritesInner {
    client: SpritesClient,
    sandboxes: DashMap<SandboxId, Arc<SpritesSandbox>>,
    mailbox: Mailbox,
    kv: KvStore,
    snapshots: SnapshotRegistry,
}

impl SpritesRuntime {
    /// Build a runtime from an API token (`sprites.dev` dashboard or
    /// `SpritesClient::create_token`).
    pub fn new(token: impl Into<String>) -> Result<Self> {
        Self::with_client(SpritesClient::new(token))
    }

    /// Build a runtime against a non-default API endpoint.
    pub fn with_base_url(token: impl Into<String>, base_url: impl Into<String>) -> Result<Self> {
        Self::with_client(SpritesClient::with_base_url(token, base_url))
    }

    /// Build a runtime from `SPRITES_TOKEN` (and optional `SPRITES_BASE_URL`).
    pub fn from_env() -> Result<Self> {
        let token = std::env::var(SPRITES_TOKEN_ENV)
            .map_err(|_| VmError::Invalid(format!("{SPRITES_TOKEN_ENV} is not set")))?;
        match std::env::var(SPRITES_BASE_URL_ENV) {
            Ok(url) => Self::with_base_url(token, url),
            Err(_) => Self::new(token),
        }
    }

    /// Build a runtime around a pre-configured [`SpritesClient`].
    pub fn with_client(client: SpritesClient) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(SpritesInner {
                client,
                sandboxes: DashMap::new(),
                mailbox: Mailbox::new(),
                kv: KvStore::in_memory(),
                snapshots: SnapshotRegistry::default_user()?,
            }),
        })
    }

    pub fn client(&self) -> &SpritesClient {
        &self.inner.client
    }

    pub fn mailbox(&self) -> Mailbox {
        self.inner.mailbox.clone()
    }

    pub fn kv(&self) -> KvStore {
        self.inner.kv.clone()
    }

    pub fn snapshots(&self) -> SnapshotRegistry {
        self.inner.snapshots.clone()
    }

    fn register(
        &self,
        id: SandboxId,
        sprite: sprites::Sprite,
        opts: SandboxOpts,
    ) -> Arc<SpritesSandbox> {
        let sandbox = Arc::new(SpritesSandbox {
            id: id.clone(),
            sprite,
            labels: opts.labels.clone(),
            status: RwLock::new(SandboxStatus::Running),
            created_at: chrono::Utc::now(),
            mailbox: self.inner.mailbox.clone(),
            kv: self.inner.kv.clone(),
            snapshots: self.inner.snapshots.clone(),
            opts,
        });
        self.inner.sandboxes.insert(id, sandbox.clone());
        sandbox
    }

    /// Attach to the sprite a snapshot manifest points at and restore the
    /// checkpoint into it.
    async fn attach_restored(&self, manifest: &SnapshotManifest) -> Result<SandboxHandle> {
        let (sprite_name, checkpoint_id) = parse_fs_pointer(&manifest.fs_pointer)?;
        // Confirm the sprite still exists server-side before restoring.
        self.inner
            .client
            .get(&sprite_name)
            .await
            .map_err(map_sprites_err)?;
        let sprite = self.inner.client.sprite(sprite_name);
        sprite
            .restore(&checkpoint_id)
            .await
            .map_err(|e| VmError::Snapshot(format!("sprite restore: {e}")))?;
        let mut opts = manifest.original_opts.clone();
        opts.from_snapshot = Some(manifest.id.clone());
        Ok(self.register(SandboxId::new(), sprite, opts))
    }
}

#[async_trait]
impl SandboxRuntime for SpritesRuntime {
    fn name(&self) -> &str {
        "sprites"
    }

    fn capabilities(&self) -> RuntimeCaps {
        RuntimeCaps {
            snapshots: true,
            pause_resume: false, // sprites hibernate automatically instead
            gpu: false,
            network_isolation: true, // per-domain NetworkPolicy
            shared_volumes: false,
            remote: true,
        }
    }

    async fn create(&self, opts: SandboxOpts) -> Result<SandboxHandle> {
        if !opts.volumes.is_empty() {
            return Err(VmError::Invalid(
                "sprites backend does not support volume mounts".into(),
            ));
        }
        if let Some(snap) = &opts.from_snapshot {
            let manifest = self.inner.snapshots.get(snap)?;
            return self.attach_restored(&manifest).await;
        }

        let id = SandboxId::new();
        let sprite_name = format!("{SPRITE_NAME_PREFIX}{}", id.as_str());

        let config = SpriteConfig {
            ram_mb: opts.mem_limit.map(|b| (b / (1024 * 1024)).max(1) as u32),
            cpus: opts.cpu_limit.map(|c| (c.ceil() as u32).max(1)),
            region: None,
            storage_gb: None,
        };
        let sprite = self
            .inner
            .client
            .create_with_config(&sprite_name, Some(config), None)
            .await
            .map_err(map_sprites_err)?;

        if let Some(workdir) = &opts.workdir {
            sprite
                .filesystem()
                .mkdir_all(&workdir.display().to_string())
                .await
                .map_err(map_sprites_err)?;
        }

        Ok(self.register(id, sprite, opts))
    }

    async fn get(&self, id: &SandboxId) -> Result<SandboxHandle> {
        if let Some(kv) = self.inner.sandboxes.get(id) {
            return Ok(kv.value().clone() as SandboxHandle);
        }
        // Not tracked locally — the runtime is remote, so fall back to
        // attaching by sprite name (accepts both bare ids and full names).
        let sprite_name = if id.as_str().starts_with(SPRITE_NAME_PREFIX) {
            id.as_str().to_string()
        } else {
            format!("{SPRITE_NAME_PREFIX}{}", id.as_str())
        };
        let info = self
            .inner
            .client
            .get(&sprite_name)
            .await
            .map_err(|_| VmError::NotFound(id.to_string()))?;
        let sprite = self.inner.client.sprite(sprite_name);
        let sandbox = self.register(id.clone(), sprite, SandboxOpts::default());
        *sandbox.status.write() = map_status(info.status);
        Ok(sandbox)
    }

    async fn list(&self) -> Result<Vec<SandboxInfo>> {
        // Query the server so sprites created by other processes show up too.
        let options = ListOptions {
            prefix: Some(SPRITE_NAME_PREFIX.to_string()),
            ..Default::default()
        };
        let infos = self
            .inner
            .client
            .list_all_with_options(options)
            .await
            .map_err(map_sprites_err)?;
        Ok(infos
            .into_iter()
            .map(|info| {
                let id = SandboxId::from(
                    info.name
                        .strip_prefix(SPRITE_NAME_PREFIX)
                        .unwrap_or(&info.name),
                );
                let labels = self
                    .inner
                    .sandboxes
                    .get(&id)
                    .map(|s| s.labels.clone())
                    .unwrap_or_default();
                SandboxInfo {
                    id,
                    backend: "sprites".to_string(),
                    image: format!("sprite://{}", info.name),
                    status: map_status(info.status),
                    created_at: info.created_at.unwrap_or_else(chrono::Utc::now),
                    labels,
                }
            })
            .collect())
    }

    async fn restore(&self, snapshot: &SnapshotId) -> Result<SandboxHandle> {
        let manifest = self.inner.snapshots.get(snapshot)?;
        let handle = self.attach_restored(&manifest).await?;
        self.inner.kv.restore(manifest.kv.clone())?;
        Ok(handle)
    }
}

pub(crate) struct SpritesSandbox {
    id: SandboxId,
    sprite: sprites::Sprite,
    labels: HashMap<String, String>,
    status: RwLock<SandboxStatus>,
    created_at: chrono::DateTime<chrono::Utc>,
    mailbox: Mailbox,
    kv: KvStore,
    snapshots: SnapshotRegistry,
    opts: SandboxOpts,
}

#[async_trait]
impl Sandbox for SpritesSandbox {
    fn id(&self) -> &SandboxId {
        &self.id
    }

    fn info(&self) -> SandboxInfo {
        SandboxInfo {
            id: self.id.clone(),
            backend: "sprites".to_string(),
            image: format!("sprite://{}", self.sprite.name()),
            status: *self.status.read(),
            created_at: self.created_at,
            labels: self.labels.clone(),
        }
    }

    fn commands(&self) -> Arc<dyn Commands> {
        Arc::new(SpritesCommands {
            sprite: self.sprite.clone(),
            env: self.opts.env.clone(),
            workdir: self.opts.workdir.clone(),
        })
    }

    fn filesystem(&self) -> Arc<dyn Filesystem> {
        Arc::new(SpritesFilesystem {
            sprite: self.sprite.clone(),
        })
    }

    async fn snapshot(&self) -> Result<SnapshotId> {
        let id = SnapshotId::new();
        let checkpoint = self
            .sprite
            .checkpoint(&format!("cersei snapshot {}", id.as_str()))
            .await
            .map_err(|e| VmError::Snapshot(format!("sprite checkpoint: {e}")))?;
        let manifest = SnapshotManifest {
            id: id.clone(),
            backend: "sprites".to_string(),
            fs_pointer: encode_fs_pointer(self.sprite.name(), &checkpoint.id),
            original_opts: self.opts.clone(),
            volumes: Vec::new(),
            kv: self.kv.snapshot(),
            mailbox_topics: self.mailbox.topics(),
            created_at: chrono::Utc::now(),
            labels: self.labels.clone(),
        };
        self.snapshots.put(manifest)?;
        Ok(id)
    }

    async fn kill(&self) -> Result<()> {
        self.sprite.destroy().await.map_err(map_sprites_err)?;
        *self.status.write() = SandboxStatus::Killed;
        Ok(())
    }
}

struct SpritesCommands {
    sprite: sprites::Sprite,
    env: HashMap<String, String>,
    workdir: Option<PathBuf>,
}

impl SpritesCommands {
    fn build(&self, req: &RunRequest, shell_cmd: &str) -> sprites::Command {
        let mut cmd = self.sprite.command("/bin/sh").arg("-c").arg(shell_cmd);
        if let Some(w) = req.workdir.as_ref().or(self.workdir.as_ref()) {
            cmd = cmd.current_dir(w.display().to_string());
        }
        cmd.envs(self.env.iter().chain(req.env.iter()))
    }
}

#[async_trait]
impl Commands for SpritesCommands {
    async fn run(&self, req: RunRequest) -> Result<RunOutput> {
        let timeout = req.timeout.unwrap_or(DEFAULT_TIMEOUT).min(MAX_TIMEOUT);

        if req.background {
            // Detach inside the sprite and hand back the remote PID; the
            // exec WebSocket returns immediately.
            let script = format!(
                "nohup sh -c {} >/dev/null 2>&1 & echo $!",
                shell_quote(&req.command)
            );
            let out = self
                .build(&req, &script)
                .max_run_after_disconnect(u32::MAX)
                .output()
                .await
                .map_err(map_sprites_err)?;
            let pid = out.stdout_str().trim().parse::<u32>().ok();
            return Ok(RunOutput {
                stdout: String::new(),
                stderr: out.stderr_str(),
                exit_code: out.status,
                timed_out: false,
                pid,
            });
        }

        let cmd = self.build(&req, &req.command);
        match tokio::time::timeout(timeout, cmd.output()).await {
            Ok(Ok(out)) => Ok(RunOutput {
                stdout: out.stdout_str(),
                stderr: out.stderr_str(),
                exit_code: out.status,
                timed_out: false,
                pid: None,
            }),
            Ok(Err(e)) => Err(map_sprites_err(e)),
            // Dropping the future closes the exec WebSocket, which kills the
            // remote process.
            Err(_) => Ok(RunOutput {
                stdout: String::new(),
                stderr: format!("timeout after {timeout:?}"),
                exit_code: -1,
                timed_out: true,
                pid: None,
            }),
        }
    }

    async fn stream(&self, req: RunRequest) -> Result<CommandStream> {
        let cmd = self.build(&req, &req.command);
        let mut child = cmd.spawn().await.map_err(map_sprites_err)?;
        let mut stdout = child.take_stdout();
        let mut stderr = child.take_stderr();

        let (tx, rx) = tokio::sync::mpsc::channel::<StreamChunk>(64);
        let _ = tx.send(StreamChunk::Started { pid: 0 }).await;

        let tx_out = tx.clone();
        let stdout_task = tokio::spawn(async move {
            let Some(out) = stdout.as_mut() else { return };
            let mut buf = vec![0u8; 8192];
            while let Ok(n) = out.read(&mut buf).await {
                if n == 0 {
                    break;
                }
                let data = String::from_utf8_lossy(&buf[..n]).into_owned();
                if tx_out.send(StreamChunk::Stdout { data }).await.is_err() {
                    break;
                }
            }
        });
        let tx_err = tx.clone();
        let stderr_task = tokio::spawn(async move {
            let Some(err) = stderr.as_mut() else { return };
            let mut buf = vec![0u8; 8192];
            while let Ok(n) = err.read(&mut buf).await {
                if n == 0 {
                    break;
                }
                let data = String::from_utf8_lossy(&buf[..n]).into_owned();
                if tx_err.send(StreamChunk::Stderr { data }).await.is_err() {
                    break;
                }
            }
        });

        tokio::spawn(async move {
            let _ = stdout_task.await;
            let _ = stderr_task.await;
            match child.wait().await {
                Ok(status) => {
                    let _ = tx.send(StreamChunk::Exit { code: status.code() }).await;
                }
                Err(e) => {
                    let _ = tx
                        .send(StreamChunk::Error {
                            message: e.to_string(),
                        })
                        .await;
                }
            }
        });

        Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }

    async fn signal(&self, pid: u32, sig: Signal) -> Result<()> {
        let out = self
            .sprite
            .command("/bin/sh")
            .arg("-c")
            .arg(format!("kill -{} {}", sig.as_i32(), pid))
            .output()
            .await
            .map_err(map_sprites_err)?;
        if !out.success() {
            return Err(VmError::Lifecycle(format!(
                "kill -{} {pid}: {}",
                sig.as_i32(),
                out.stderr_str().trim()
            )));
        }
        Ok(())
    }
}

struct SpritesFilesystem {
    sprite: sprites::Sprite,
}

impl SpritesFilesystem {
    fn fs(&self) -> sprites::Filesystem {
        self.sprite.filesystem()
    }
}

#[async_trait]
impl Filesystem for SpritesFilesystem {
    async fn read(&self, path: &str) -> Result<Bytes> {
        let data = self.fs().read_file(path).await.map_err(map_sprites_err)?;
        Ok(Bytes::from(data))
    }

    async fn write(&self, path: &str, data: &[u8]) -> Result<()> {
        if let Some(parent) = Path::new(path).parent() {
            let parent = parent.display().to_string();
            if !parent.is_empty() && parent != "/" {
                self.fs()
                    .mkdir_all(&parent)
                    .await
                    .map_err(map_sprites_err)?;
            }
        }
        self.fs()
            .write_file(path, data)
            .await
            .map_err(map_sprites_err)
    }

    async fn list(&self, path: &str, depth: u32) -> Result<Vec<FileEntry>> {
        // The sprite API lists one level at a time; walk breadth-first.
        let fs = self.fs();
        let mut entries = Vec::new();
        let mut frontier = vec![(PathBuf::from(path), 1u32)];
        while let Some((dir, level)) = frontier.pop() {
            let listed = fs
                .read_dir(&dir.display().to_string())
                .await
                .map_err(map_sprites_err)?;
            for e in listed {
                let full = dir.join(&e.name);
                if e.is_dir && level < depth.max(1) {
                    frontier.push((full.clone(), level + 1));
                }
                entries.push(FileEntry {
                    path: full,
                    kind: if e.is_dir { FileKind::Dir } else { FileKind::File },
                    size: e.size,
                    modified_unix_ms: e.modified.map(|t| t.timestamp_millis()).unwrap_or(0),
                });
            }
        }
        Ok(entries)
    }

    async fn stat(&self, path: &str) -> Result<FileEntry> {
        let info = self.fs().stat(path).await.map_err(map_sprites_err)?;
        Ok(FileEntry {
            path: PathBuf::from(path),
            kind: if info.is_dir {
                FileKind::Dir
            } else {
                FileKind::File
            },
            size: info.size,
            modified_unix_ms: info.modified.map(|t| t.timestamp_millis()).unwrap_or(0),
        })
    }

    async fn watch(&self, _path: &str, _recursive: bool) -> Result<WatchStream> {
        Err(VmError::Lifecycle(
            "watch not implemented for sprites backend".into(),
        ))
    }

    async fn mkdir(&self, path: &str, recursive: bool) -> Result<()> {
        let fs = self.fs();
        if recursive {
            fs.mkdir_all(path).await.map_err(map_sprites_err)
        } else {
            fs.mkdir(path).await.map_err(map_sprites_err)
        }
    }

    async fn remove(&self, path: &str, recursive: bool) -> Result<()> {
        let fs = self.fs();
        if recursive {
            fs.remove_all(path).await.map_err(map_sprites_err)
        } else {
            fs.remove(path).await.map_err(map_sprites_err)
        }
    }

    async fn upload(&self, local: &Path, remote: &str) -> Result<()> {
        let meta = tokio::fs::metadata(local).await?;
        if meta.is_dir() {
            return Err(VmError::Invalid(format!(
                "sprites upload supports files only, {} is a directory",
                local.display()
            )));
        }
        let data = tokio::fs::read(local).await?;
        self.write(remote, &data).await
    }

    async fn download(&self, remote: &str, local: &Path) -> Result<()> {
        let data = self.read(remote).await?;
        if let Some(parent) = local.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(local, &data).await?;
        Ok(())
    }
}

fn map_status(status: SpriteStatus) -> SandboxStatus {
    match status {
        // Cold/warm sprites wake transparently on the next exec, so they
        // still count as usable.
        SpriteStatus::Cold | SpriteStatus::Warm | SpriteStatus::Running => SandboxStatus::Running,
        SpriteStatus::Stopped => SandboxStatus::Exited,
    }
}

fn map_sprites_err(e: sprites::Error) -> VmError {
    match e {
        sprites::Error::NotFound(name) => VmError::NotFound(name),
        sprites::Error::Timeout => VmError::Timeout(DEFAULT_TIMEOUT),
        other => VmError::Backend {
            backend: "sprites".into(),
            message: other.to_string(),
        },
    }
}

fn encode_fs_pointer(sprite_name: &str, checkpoint_id: &str) -> String {
    format!("sprite={sprite_name};checkpoint={checkpoint_id}")
}

fn parse_fs_pointer(pointer: &str) -> Result<(String, String)> {
    let mut sprite = None;
    let mut checkpoint = None;
    for part in pointer.split(';') {
        if let Some(v) = part.strip_prefix("sprite=") {
            sprite = Some(v.to_string());
        } else if let Some(v) = part.strip_prefix("checkpoint=") {
            checkpoint = Some(v.to_string());
        }
    }
    match (sprite, checkpoint) {
        (Some(s), Some(c)) if !s.is_empty() && !c.is_empty() => Ok((s, c)),
        _ => Err(VmError::Snapshot(format!(
            "malformed sprites fs_pointer: {pointer}"
        ))),
    }
}

/// Minimal shell-quoting for strings passed to `sh -c`.
fn shell_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for ch in s.chars() {
        if ch == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(ch);
        }
    }
    out.push('\'');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fs_pointer_roundtrip() {
        let ptr = encode_fs_pointer("cersei-vm-abc", "v3");
        assert_eq!(ptr, "sprite=cersei-vm-abc;checkpoint=v3");
        let (sprite, checkpoint) = parse_fs_pointer(&ptr).unwrap();
        assert_eq!(sprite, "cersei-vm-abc");
        assert_eq!(checkpoint, "v3");
    }

    #[test]
    fn fs_pointer_rejects_garbage() {
        assert!(parse_fs_pointer("docker-image:tag").is_err());
        assert!(parse_fs_pointer("sprite=;checkpoint=v1").is_err());
        assert!(parse_fs_pointer("sprite=x").is_err());
    }

    #[test]
    fn status_mapping() {
        assert_eq!(map_status(SpriteStatus::Cold), SandboxStatus::Running);
        assert_eq!(map_status(SpriteStatus::Warm), SandboxStatus::Running);
        assert_eq!(map_status(SpriteStatus::Running), SandboxStatus::Running);
        assert_eq!(map_status(SpriteStatus::Stopped), SandboxStatus::Exited);
    }

    #[test]
    fn shell_quote_escapes_single_quotes() {
        assert_eq!(shell_quote("echo 'hi'"), "'echo '\\''hi'\\'''");
    }

    #[test]
    fn runtime_reports_capabilities() {
        let rt = SpritesRuntime::new("test-token").unwrap();
        assert_eq!(rt.name(), "sprites");
        let caps = rt.capabilities();
        assert!(caps.snapshots);
        assert!(caps.remote);
        assert!(caps.network_isolation);
        assert!(!caps.shared_volumes);
        assert!(!caps.pause_resume);
    }
}
