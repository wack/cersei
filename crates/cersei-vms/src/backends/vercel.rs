//! `VercelRuntime` — remote Firecracker microVM sandboxes on
//! [Vercel Sandbox](https://vercel.com/docs/sandbox).
//!
//! Vercel does not ship an official Rust SDK, so this backend implements a
//! minimal HTTP client with `reqwest` against the same REST API the official
//! `@vercel/sandbox` JS SDK uses (base URL `https://vercel.com/api`):
//!
//!   - `create()`  → `POST /v3/sandboxes`
//!   - `kill()`    → `POST /v2/sandboxes/sessions/<sid>/stop` + `DELETE /v2/sandboxes/<name>`
//!   - `pause()`   → `POST /v2/sandboxes/sessions/<sid>/stop`
//!   - `resume()`  → `GET /v2/sandboxes/<name>?resume=true` (new session id)
//!   - `snapshot()` → `POST /v2/sandboxes/sessions/<sid>/snapshot` + JSON manifest
//!   - `restore(snap)` → `POST /v3/sandboxes` with `source: {type: "snapshot"}`
//!
//! Command execution:
//!   - `run`/`stream` → `POST /v2/sandboxes/sessions/<sid>/cmd` with
//!     `wait: true, logs: true`; the response is an ND-JSON stream of the
//!     command record, stdout/stderr log lines, and the finished record.
//!
//! Filesystem:
//!   - `read`  → `POST .../fs/read` (octet-stream body)
//!   - `write` → `POST .../fs/write` (gzipped tar extracted at the `x-cwd` dir)
//!   - `mkdir(recursive)` → `POST .../fs/mkdir`
//!   - `list`/`stat`/`remove` → shell commands, as in the docker backend
//!
//! Backend-specific notes:
//!   - Auth uses a Vercel access token plus team/project ids
//!     (`VERCEL_TOKEN`, `VERCEL_TEAM_ID`, `VERCEL_PROJECT_ID`).
//!   - `SandboxOpts::image` is forwarded only when it differs from the Cersei
//!     default; otherwise Vercel's default image is used. Custom images must
//!     live in Vercel Container Registry or be Vercel managed images.
//!   - `cpu_limit` maps to `resources.vcpus`; `mem_limit` is ignored (Vercel
//!     scales memory with vCPUs).
//!   - Volume mounts are not supported and are rejected at `create()`.
//!   - `snapshot()` may transition the session to `snapshotting`/`stopped`;
//!     `resume()` brings a sandbox back with a fresh session id.

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
use futures::StreamExt;
use parking_lot::RwLock;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);
const MAX_TIMEOUT: Duration = Duration::from_secs(600);
const SANDBOX_NAME_PREFIX: &str = "cersei-vm-";
const DEFAULT_BASE_URL: &str = "https://vercel.com/api";
/// The image Cersei's `SandboxOpts::default()` targets; never forwarded to
/// Vercel (its sandboxes boot Vercel-managed or VCR images instead).
const CERSEI_DEFAULT_IMAGE: &str = "cersei/sandbox-base:latest";

/// Env var holding the Vercel access token for [`VercelRuntime::from_env`].
pub const VERCEL_TOKEN_ENV: &str = "VERCEL_TOKEN";
/// Env var holding the Vercel team id.
pub const VERCEL_TEAM_ID_ENV: &str = "VERCEL_TEAM_ID";
/// Env var holding the Vercel project id.
pub const VERCEL_PROJECT_ID_ENV: &str = "VERCEL_PROJECT_ID";
/// Optional env var overriding the API base URL.
pub const VERCEL_BASE_URL_ENV: &str = "VERCEL_SANDBOX_BASE_URL";

/// Connection settings for the Vercel Sandbox API.
#[derive(Debug, Clone)]
pub struct VercelConfig {
    /// Vercel access token with access to the team.
    pub token: String,
    /// Team id (`team_xxx`). Omitted from requests when `None` (personal scope).
    pub team_id: Option<String>,
    /// Project id (`prj_xxx`) or project name the sandboxes belong to.
    pub project_id: String,
    /// API base URL; [`DEFAULT_BASE_URL`] unless overridden.
    pub base_url: String,
    /// Sandbox lifetime in milliseconds sent at create time.
    /// `None` = Vercel's server-side default.
    pub sandbox_timeout_ms: Option<u64>,
}

impl VercelConfig {
    pub fn new(token: impl Into<String>, project_id: impl Into<String>) -> Self {
        Self {
            token: token.into(),
            team_id: None,
            project_id: project_id.into(),
            base_url: DEFAULT_BASE_URL.to_string(),
            sandbox_timeout_ms: None,
        }
    }

    pub fn with_team_id(mut self, team_id: impl Into<String>) -> Self {
        self.team_id = Some(team_id.into());
        self
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    pub fn with_sandbox_timeout_ms(mut self, ms: u64) -> Self {
        self.sandbox_timeout_ms = Some(ms);
        self
    }

    /// Read `VERCEL_TOKEN`, `VERCEL_PROJECT_ID`, and optionally
    /// `VERCEL_TEAM_ID` / `VERCEL_SANDBOX_BASE_URL` from the environment.
    pub fn from_env() -> Result<Self> {
        let token = std::env::var(VERCEL_TOKEN_ENV)
            .map_err(|_| VmError::Invalid(format!("{VERCEL_TOKEN_ENV} is not set")))?;
        let project_id = std::env::var(VERCEL_PROJECT_ID_ENV)
            .map_err(|_| VmError::Invalid(format!("{VERCEL_PROJECT_ID_ENV} is not set")))?;
        let mut config = Self::new(token, project_id);
        if let Ok(team) = std::env::var(VERCEL_TEAM_ID_ENV) {
            config.team_id = Some(team);
        }
        if let Ok(url) = std::env::var(VERCEL_BASE_URL_ENV) {
            config.base_url = url;
        }
        Ok(config)
    }
}

// ---------------------------------------------------------------------------
// Wire types (subset of the API responses this backend consumes)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
struct ApiSession {
    id: String,
    status: String,
    #[serde(default)]
    cwd: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiSandbox {
    name: String,
    status: String,
    created_at: i64,
    #[serde(default)]
    tags: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiCommand {
    #[allow(dead_code)]
    id: String,
    exit_code: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
struct ApiSnapshot {
    id: String,
}

#[derive(Debug, Deserialize)]
struct SandboxAndSessionResponse {
    sandbox: ApiSandbox,
    session: ApiSession,
}

#[derive(Debug, Deserialize)]
struct SandboxesPage {
    sandboxes: Vec<ApiSandbox>,
    pagination: Pagination,
}

#[derive(Debug, Deserialize)]
struct Pagination {
    next: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CreateSnapshotResponse {
    snapshot: ApiSnapshot,
    session: ApiSession,
}

/// One ND-JSON line of a `cmd` stream: either a command record (first and
/// last lines) or a log line. `Command` must come first — log lines carry a
/// `stream` key that a command record never has, and vice versa.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum CmdStreamLine {
    Command { command: ApiCommand },
    Log(ApiLogLine),
}

#[derive(Debug, Deserialize)]
#[serde(tag = "stream", rename_all = "lowercase")]
enum ApiLogLine {
    Stdout { data: String },
    Stderr { data: String },
    Error { data: ApiLogError },
}

#[derive(Debug, Deserialize)]
struct ApiLogError {
    code: String,
    message: String,
}

#[derive(Debug, Deserialize)]
struct ApiErrorBody {
    error: ApiErrorMessage,
}

#[derive(Debug, Deserialize)]
struct ApiErrorMessage {
    message: String,
}

// ---------------------------------------------------------------------------
// HTTP client
// ---------------------------------------------------------------------------

struct VercelClient {
    http: reqwest::Client,
    config: VercelConfig,
}

impl VercelClient {
    fn new(config: VercelConfig) -> Self {
        Self {
            http: reqwest::Client::new(),
            config,
        }
    }

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let mut req = self
            .http
            .request(method, format!("{}{}", self.config.base_url, path))
            .bearer_auth(&self.config.token);
        if let Some(team) = &self.config.team_id {
            req = req.query(&[("teamId", team.as_str())]);
        }
        req
    }

    async fn send(&self, req: reqwest::RequestBuilder) -> Result<reqwest::Response> {
        let resp = req.send().await.map_err(|e| VmError::Backend {
            backend: "vercel".into(),
            message: format!("request failed: {e}"),
        })?;
        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }
        let text = resp.text().await.unwrap_or_default();
        let message = serde_json::from_str::<ApiErrorBody>(&text)
            .map(|b| b.error.message)
            .unwrap_or_else(|_| text.chars().take(300).collect());
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(VmError::NotFound(message));
        }
        Err(VmError::Backend {
            backend: "vercel".into(),
            message: format!("HTTP {status}: {message}"),
        })
    }

    async fn json<T: serde::de::DeserializeOwned>(
        &self,
        req: reqwest::RequestBuilder,
    ) -> Result<T> {
        let resp = self.send(req).await?;
        resp.json::<T>().await.map_err(|e| VmError::Backend {
            backend: "vercel".into(),
            message: format!("bad response body: {e}"),
        })
    }

    async fn create_sandbox(&self, body: serde_json::Value) -> Result<SandboxAndSessionResponse> {
        self.json(
            self.request(reqwest::Method::POST, "/v3/sandboxes")
                .json(&body),
        )
        .await
    }

    async fn get_sandbox(&self, name: &str, resume: bool) -> Result<SandboxAndSessionResponse> {
        let mut req = self
            .request(reqwest::Method::GET, &format!("/v2/sandboxes/{name}"))
            .query(&[("projectId", self.config.project_id.as_str())]);
        if resume {
            req = req.query(&[("resume", "true")]);
        }
        self.json(req).await
    }

    async fn list_sandboxes(&self, name_prefix: &str) -> Result<Vec<ApiSandbox>> {
        let mut sandboxes = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let mut req = self
                .request(reqwest::Method::GET, "/v2/sandboxes")
                .query(&[
                    ("project", self.config.project_id.as_str()),
                    ("namePrefix", name_prefix),
                ]);
            if let Some(c) = &cursor {
                req = req.query(&[("cursor", c.as_str())]);
            }
            let page: SandboxesPage = self.json(req).await?;
            sandboxes.extend(page.sandboxes);
            match page.pagination.next {
                Some(next) => cursor = Some(next),
                None => return Ok(sandboxes),
            }
        }
    }

    async fn delete_sandbox(&self, name: &str) -> Result<()> {
        self.send(
            self.request(reqwest::Method::DELETE, &format!("/v2/sandboxes/{name}"))
                .query(&[("projectId", self.config.project_id.as_str())]),
        )
        .await?;
        Ok(())
    }

    async fn stop_session(&self, session_id: &str) -> Result<()> {
        self.send(self.request(
            reqwest::Method::POST,
            &format!("/v2/sandboxes/sessions/{session_id}/stop"),
        ))
        .await?;
        Ok(())
    }

    async fn create_snapshot(&self, session_id: &str) -> Result<CreateSnapshotResponse> {
        self.json(self.request(
            reqwest::Method::POST,
            &format!("/v2/sandboxes/sessions/{session_id}/snapshot"),
        ))
        .await
    }

    /// Start a command with `wait: true, logs: true` and return the raw
    /// ND-JSON response stream.
    async fn start_command(
        &self,
        session_id: &str,
        body: serde_json::Value,
    ) -> Result<reqwest::Response> {
        self.send(
            self.request(
                reqwest::Method::POST,
                &format!("/v2/sandboxes/sessions/{session_id}/cmd"),
            )
            .json(&body),
        )
        .await
    }

    async fn fs_read(&self, session_id: &str, path: &str, cwd: &str) -> Result<Option<Bytes>> {
        let resp = self
            .request(
                reqwest::Method::POST,
                &format!("/v2/sandboxes/sessions/{session_id}/fs/read"),
            )
            .json(&json!({ "path": path, "cwd": cwd }))
            .send()
            .await
            .map_err(|e| VmError::Backend {
                backend: "vercel".into(),
                message: format!("fs/read: {e}"),
            })?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(VmError::Backend {
                backend: "vercel".into(),
                message: format!("fs/read HTTP {status}: {}", text.chars().take(300).collect::<String>()),
            });
        }
        let bytes = resp.bytes().await.map_err(|e| VmError::Backend {
            backend: "vercel".into(),
            message: format!("fs/read body: {e}"),
        })?;
        Ok(Some(bytes))
    }

    async fn fs_mkdir(&self, session_id: &str, path: &str, cwd: &str) -> Result<()> {
        self.send(
            self.request(
                reqwest::Method::POST,
                &format!("/v2/sandboxes/sessions/{session_id}/fs/mkdir"),
            )
            .json(&json!({ "path": path, "cwd": cwd })),
        )
        .await?;
        Ok(())
    }

    /// Upload a gzipped tarball extracted at `extract_dir` inside the sandbox.
    async fn fs_write(&self, session_id: &str, extract_dir: &str, tar_gz: Vec<u8>) -> Result<()> {
        self.send(
            self.request(
                reqwest::Method::POST,
                &format!("/v2/sandboxes/sessions/{session_id}/fs/write"),
            )
            .header("content-type", "application/gzip")
            .header("x-cwd", extract_dir)
            .body(tar_gz),
        )
        .await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Command stream plumbing
// ---------------------------------------------------------------------------

/// Incremental ND-JSON splitter over a `reqwest` byte stream.
struct NdjsonLines {
    stream: futures::stream::BoxStream<'static, reqwest::Result<Bytes>>,
    buffer: Vec<u8>,
    done: bool,
}

impl NdjsonLines {
    fn new(resp: reqwest::Response) -> Self {
        Self {
            stream: resp.bytes_stream().boxed(),
            buffer: Vec::new(),
            done: false,
        }
    }

    async fn next_line(&mut self) -> Result<Option<String>> {
        loop {
            if let Some(pos) = self.buffer.iter().position(|&b| b == b'\n') {
                let line: Vec<u8> = self.buffer.drain(..=pos).collect();
                let text = String::from_utf8_lossy(&line).trim().to_string();
                if text.is_empty() {
                    continue;
                }
                return Ok(Some(text));
            }
            if self.done {
                let text = String::from_utf8_lossy(&self.buffer).trim().to_string();
                self.buffer.clear();
                return Ok((!text.is_empty()).then_some(text));
            }
            match self.stream.next().await {
                Some(Ok(chunk)) => self.buffer.extend_from_slice(&chunk),
                Some(Err(e)) => {
                    return Err(VmError::Backend {
                        backend: "vercel".into(),
                        message: format!("command stream: {e}"),
                    })
                }
                None => self.done = true,
            }
        }
    }
}

fn parse_cmd_line(line: &str) -> Result<CmdStreamLine> {
    serde_json::from_str::<CmdStreamLine>(line).map_err(|e| VmError::Backend {
        backend: "vercel".into(),
        message: format!("unparseable command stream line ({e}): {line}"),
    })
}

/// Drain a `wait: true` command stream into a completed [`RunOutput`].
async fn collect_run_output(resp: reqwest::Response) -> Result<RunOutput> {
    let mut lines = NdjsonLines::new(resp);
    let mut stdout = String::new();
    let mut stderr = String::new();
    let mut exit_code = -1;
    while let Some(line) = lines.next_line().await? {
        match parse_cmd_line(&line)? {
            CmdStreamLine::Command { command } => {
                if let Some(code) = command.exit_code {
                    exit_code = code as i32;
                }
            }
            CmdStreamLine::Log(ApiLogLine::Stdout { data }) => stdout.push_str(&data),
            CmdStreamLine::Log(ApiLogLine::Stderr { data }) => stderr.push_str(&data),
            CmdStreamLine::Log(ApiLogLine::Error { data }) => {
                return Err(VmError::Backend {
                    backend: "vercel".into(),
                    message: format!("command error ({}): {}", data.code, data.message),
                })
            }
        }
    }
    Ok(RunOutput {
        stdout,
        stderr,
        exit_code,
        timed_out: false,
        pid: None,
    })
}

fn command_body(req: &RunRequest, shell_cmd: &str, base: &CommandContext) -> serde_json::Value {
    let mut env = base.env.clone();
    env.extend(req.env.clone());
    let mut body = json!({
        "command": "/bin/sh",
        "args": ["-c", shell_cmd],
        "env": env,
        "sudo": false,
        "wait": true,
        "logs": true,
    });
    if let Some(w) = req.workdir.as_ref().or(base.workdir.as_ref()) {
        body.as_object_mut()
            .expect("body is an object")
            .insert("cwd".into(), json!(w.display().to_string()));
    }
    body
}

/// Per-sandbox context shared by command execution and filesystem helpers.
#[derive(Clone)]
struct CommandContext {
    client: Arc<VercelClient>,
    session_id: Arc<RwLock<String>>,
    env: HashMap<String, String>,
    workdir: Option<PathBuf>,
}

impl CommandContext {
    fn session(&self) -> String {
        self.session_id.read().clone()
    }

    async fn run_shell(&self, script: &str) -> Result<RunOutput> {
        let req = RunRequest::new(script);
        let body = command_body(&req, script, self);
        let resp = self.client.start_command(&self.session(), body).await?;
        collect_run_output(resp).await
    }

    /// Like [`run_shell`], but errors when the command exits non-zero.
    async fn run_shell_ok(&self, script: &str) -> Result<RunOutput> {
        let out = self.run_shell(script).await?;
        if out.exit_code != 0 {
            return Err(VmError::Backend {
                backend: "vercel".into(),
                message: format!("`{script}` exited {}: {}", out.exit_code, out.stderr.trim()),
            });
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Runtime
// ---------------------------------------------------------------------------

pub struct VercelRuntime {
    inner: Arc<VercelInner>,
}

struct VercelInner {
    client: Arc<VercelClient>,
    sandboxes: DashMap<SandboxId, Arc<VercelSandbox>>,
    mailbox: Mailbox,
    kv: KvStore,
    snapshots: SnapshotRegistry,
}

impl VercelRuntime {
    pub fn new(config: VercelConfig) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(VercelInner {
                client: Arc::new(VercelClient::new(config)),
                sandboxes: DashMap::new(),
                mailbox: Mailbox::new(),
                kv: KvStore::in_memory(),
                snapshots: SnapshotRegistry::default_user()?,
            }),
        })
    }

    /// Build a runtime from `VERCEL_TOKEN` / `VERCEL_TEAM_ID` /
    /// `VERCEL_PROJECT_ID` (and optional `VERCEL_SANDBOX_BASE_URL`).
    pub fn from_env() -> Result<Self> {
        Self::new(VercelConfig::from_env()?)
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
        response: SandboxAndSessionResponse,
        opts: SandboxOpts,
    ) -> Arc<VercelSandbox> {
        let workdir = opts
            .workdir
            .clone()
            .or_else(|| response.session.cwd.clone().map(PathBuf::from));
        let sandbox = Arc::new(VercelSandbox {
            id: id.clone(),
            sandbox_name: response.sandbox.name.clone(),
            client: self.inner.client.clone(),
            session_id: Arc::new(RwLock::new(response.session.id.clone())),
            workdir,
            labels: opts.labels.clone(),
            status: RwLock::new(map_status(&response.session.status)),
            created_at: chrono::DateTime::from_timestamp_millis(response.sandbox.created_at)
                .unwrap_or_else(chrono::Utc::now),
            mailbox: self.inner.mailbox.clone(),
            kv: self.inner.kv.clone(),
            snapshots: self.inner.snapshots.clone(),
            opts,
        });
        self.inner.sandboxes.insert(id, sandbox.clone());
        sandbox
    }

    fn create_body(&self, opts: &SandboxOpts, name: &str) -> serde_json::Value {
        let config = &self.inner.client.config;
        let mut body = json!({
            "projectId": config.project_id,
            "name": name,
        });
        let map = body.as_object_mut().expect("body is an object");
        if !opts.env.is_empty() {
            map.insert("env".into(), json!(opts.env));
        }
        if !opts.labels.is_empty() {
            map.insert("tags".into(), json!(opts.labels));
        }
        if let Some(cpus) = opts.cpu_limit {
            map.insert(
                "resources".into(),
                json!({ "vcpus": (cpus.ceil() as u32).max(1) }),
            );
        }
        if let Some(timeout) = config.sandbox_timeout_ms {
            map.insert("timeout".into(), json!(timeout));
        }
        if opts.image != CERSEI_DEFAULT_IMAGE {
            map.insert("image".into(), json!(opts.image));
        }
        body
    }
}

#[async_trait]
impl SandboxRuntime for VercelRuntime {
    fn name(&self) -> &str {
        "vercel"
    }

    fn capabilities(&self) -> RuntimeCaps {
        RuntimeCaps {
            snapshots: true,
            pause_resume: true, // stop session / resume sandbox
            gpu: false,
            network_isolation: true, // per-domain network policies
            shared_volumes: false,
            remote: true,
        }
    }

    async fn create(&self, opts: SandboxOpts) -> Result<SandboxHandle> {
        if !opts.volumes.is_empty() {
            return Err(VmError::Invalid(
                "vercel backend does not support volume mounts".into(),
            ));
        }

        let id = SandboxId::new();
        let name = format!("{SANDBOX_NAME_PREFIX}{}", id.as_str());
        let mut body = self.create_body(&opts, &name);

        if let Some(snap) = &opts.from_snapshot {
            let manifest = self.inner.snapshots.get(snap)?;
            let snapshot_id = parse_fs_pointer(&manifest.fs_pointer)?;
            body.as_object_mut().expect("body is an object").insert(
                "source".into(),
                json!({ "type": "snapshot", "snapshotId": snapshot_id }),
            );
        }

        let response = self.inner.client.create_sandbox(body).await?;
        let sandbox = self.register(id, response, opts);

        if let Some(workdir) = &sandbox.workdir {
            let session = { sandbox.session_id.read().clone() };
            sandbox
                .client
                .fs_mkdir(&session, &workdir.display().to_string(), "/")
                .await?;
        }
        Ok(sandbox)
    }

    async fn get(&self, id: &SandboxId) -> Result<SandboxHandle> {
        if let Some(kv) = self.inner.sandboxes.get(id) {
            return Ok(kv.value().clone() as SandboxHandle);
        }
        // Not tracked locally — attach by sandbox name server-side
        // (accepts both bare ids and full `cersei-vm-*` names).
        let name = if id.as_str().starts_with(SANDBOX_NAME_PREFIX) {
            id.as_str().to_string()
        } else {
            format!("{SANDBOX_NAME_PREFIX}{}", id.as_str())
        };
        let response = self
            .inner
            .client
            .get_sandbox(&name, false)
            .await
            .map_err(|_| VmError::NotFound(id.to_string()))?;
        Ok(self.register(id.clone(), response, SandboxOpts::default()))
    }

    async fn list(&self) -> Result<Vec<SandboxInfo>> {
        let sandboxes = self
            .inner
            .client
            .list_sandboxes(SANDBOX_NAME_PREFIX)
            .await?;
        Ok(sandboxes
            .into_iter()
            .map(|s| {
                let id = SandboxId::from(
                    s.name
                        .strip_prefix(SANDBOX_NAME_PREFIX)
                        .unwrap_or(&s.name),
                );
                SandboxInfo {
                    id,
                    backend: "vercel".to_string(),
                    image: format!("vercel://{}", s.name),
                    status: map_status(&s.status),
                    created_at: chrono::DateTime::from_timestamp_millis(s.created_at)
                        .unwrap_or_else(chrono::Utc::now),
                    labels: s.tags.unwrap_or_default(),
                }
            })
            .collect())
    }

    async fn restore(&self, snapshot: &SnapshotId) -> Result<SandboxHandle> {
        let manifest = self.inner.snapshots.get(snapshot)?;
        let mut opts = manifest.original_opts.clone();
        opts.from_snapshot = Some(snapshot.clone());
        let handle = self.create(opts).await?;
        self.inner.kv.restore(manifest.kv.clone())?;
        Ok(handle)
    }
}

// ---------------------------------------------------------------------------
// Sandbox
// ---------------------------------------------------------------------------

pub(crate) struct VercelSandbox {
    id: SandboxId,
    sandbox_name: String,
    client: Arc<VercelClient>,
    /// Session ids rotate on resume, so command/filesystem handles share it.
    session_id: Arc<RwLock<String>>,
    workdir: Option<PathBuf>,
    labels: HashMap<String, String>,
    status: RwLock<SandboxStatus>,
    created_at: chrono::DateTime<chrono::Utc>,
    mailbox: Mailbox,
    kv: KvStore,
    snapshots: SnapshotRegistry,
    opts: SandboxOpts,
}

impl VercelSandbox {
    fn context(&self) -> CommandContext {
        CommandContext {
            client: self.client.clone(),
            session_id: self.session_id.clone(),
            env: self.opts.env.clone(),
            workdir: self.workdir.clone(),
        }
    }
}

#[async_trait]
impl Sandbox for VercelSandbox {
    fn id(&self) -> &SandboxId {
        &self.id
    }

    fn info(&self) -> SandboxInfo {
        SandboxInfo {
            id: self.id.clone(),
            backend: "vercel".to_string(),
            image: format!("vercel://{}", self.sandbox_name),
            status: *self.status.read(),
            created_at: self.created_at,
            labels: self.labels.clone(),
        }
    }

    fn commands(&self) -> Arc<dyn Commands> {
        Arc::new(VercelCommands {
            ctx: self.context(),
        })
    }

    fn filesystem(&self) -> Arc<dyn Filesystem> {
        Arc::new(VercelFilesystem {
            ctx: self.context(),
        })
    }

    async fn snapshot(&self) -> Result<SnapshotId> {
        let session = self.session_id.read().clone();
        let response = self
            .client
            .create_snapshot(&session)
            .await
            .map_err(|e| VmError::Snapshot(format!("vercel snapshot: {e}")))?;
        // Snapshotting can transition the session; reflect the fresh status.
        *self.status.write() = map_status(&response.session.status);
        let id = SnapshotId::new();
        let manifest = SnapshotManifest {
            id: id.clone(),
            backend: "vercel".to_string(),
            fs_pointer: encode_fs_pointer(&response.snapshot.id),
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

    async fn pause(&self) -> Result<()> {
        let session = self.session_id.read().clone();
        self.client
            .stop_session(&session)
            .await
            .map_err(|e| VmError::Lifecycle(format!("vercel stop session: {e}")))?;
        *self.status.write() = SandboxStatus::Paused;
        Ok(())
    }

    async fn resume(&self) -> Result<()> {
        let response = self
            .client
            .get_sandbox(&self.sandbox_name, true)
            .await
            .map_err(|e| VmError::Lifecycle(format!("vercel resume: {e}")))?;
        *self.session_id.write() = response.session.id;
        *self.status.write() = map_status(&response.session.status);
        Ok(())
    }

    async fn kill(&self) -> Result<()> {
        // Stop the live session first (best effort), then delete the sandbox.
        let session = self.session_id.read().clone();
        let _ = self.client.stop_session(&session).await;
        self.client.delete_sandbox(&self.sandbox_name).await?;
        *self.status.write() = SandboxStatus::Killed;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

struct VercelCommands {
    ctx: CommandContext,
}

#[async_trait]
impl Commands for VercelCommands {
    async fn run(&self, req: RunRequest) -> Result<RunOutput> {
        let timeout = req.timeout.unwrap_or(DEFAULT_TIMEOUT).min(MAX_TIMEOUT);

        if req.background {
            // Detach inside the sandbox and hand back the remote PID.
            let script = format!(
                "nohup sh -c {} >/dev/null 2>&1 & echo $!",
                shell_quote(&req.command)
            );
            let body = command_body(&req, &script, &self.ctx);
            let resp = self
                .ctx
                .client
                .start_command(&self.ctx.session(), body)
                .await?;
            let out = collect_run_output(resp).await?;
            let pid = out.stdout.trim().parse::<u32>().ok();
            return Ok(RunOutput {
                stdout: String::new(),
                stderr: out.stderr,
                exit_code: out.exit_code,
                timed_out: false,
                pid,
            });
        }

        let body = command_body(&req, &req.command, &self.ctx);
        let resp = self
            .ctx
            .client
            .start_command(&self.ctx.session(), body)
            .await?;
        match tokio::time::timeout(timeout, collect_run_output(resp)).await {
            Ok(result) => result,
            // Dropping the response stream disconnects; the API kills
            // commands whose client goes away.
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
        let body = command_body(&req, &req.command, &self.ctx);
        let resp = self
            .ctx
            .client
            .start_command(&self.ctx.session(), body)
            .await?;

        let (tx, rx) = tokio::sync::mpsc::channel::<StreamChunk>(64);
        tokio::spawn(async move {
            let _ = tx.send(StreamChunk::Started { pid: 0 }).await;
            let mut lines = NdjsonLines::new(resp);
            let mut exit_code: Option<i32> = None;
            loop {
                let line = match lines.next_line().await {
                    Ok(Some(line)) => line,
                    Ok(None) => break,
                    Err(e) => {
                        let _ = tx
                            .send(StreamChunk::Error {
                                message: e.to_string(),
                            })
                            .await;
                        return;
                    }
                };
                let chunk = match parse_cmd_line(&line) {
                    Ok(CmdStreamLine::Command { command }) => {
                        exit_code = command.exit_code.map(|c| c as i32).or(exit_code);
                        continue;
                    }
                    Ok(CmdStreamLine::Log(ApiLogLine::Stdout { data })) => {
                        StreamChunk::Stdout { data }
                    }
                    Ok(CmdStreamLine::Log(ApiLogLine::Stderr { data })) => {
                        StreamChunk::Stderr { data }
                    }
                    Ok(CmdStreamLine::Log(ApiLogLine::Error { data })) => StreamChunk::Error {
                        message: format!("{}: {}", data.code, data.message),
                    },
                    Err(e) => StreamChunk::Error {
                        message: e.to_string(),
                    },
                };
                if tx.send(chunk).await.is_err() {
                    return;
                }
            }
            let _ = tx
                .send(StreamChunk::Exit {
                    code: exit_code.unwrap_or(-1),
                })
                .await;
        });

        Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }

    async fn signal(&self, pid: u32, sig: Signal) -> Result<()> {
        let out = self
            .ctx
            .run_shell(&format!("kill -{} {}", sig.as_i32(), pid))
            .await?;
        if out.exit_code != 0 {
            return Err(VmError::Lifecycle(format!(
                "kill -{} {pid}: {}",
                sig.as_i32(),
                out.stderr.trim()
            )));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Filesystem
// ---------------------------------------------------------------------------

struct VercelFilesystem {
    ctx: CommandContext,
}

impl VercelFilesystem {
    fn cwd(&self) -> String {
        self.ctx
            .workdir
            .as_ref()
            .map(|w| w.display().to_string())
            .unwrap_or_else(|| "/".to_string())
    }

    /// Resolve `path` to an absolute in-sandbox path (relative paths resolve
    /// against the sandbox workdir).
    fn absolute(&self, path: &str) -> PathBuf {
        let p = Path::new(path);
        if p.is_absolute() {
            p.to_path_buf()
        } else {
            PathBuf::from(self.cwd()).join(p)
        }
    }
}

#[async_trait]
impl Filesystem for VercelFilesystem {
    async fn read(&self, path: &str) -> Result<Bytes> {
        self.ctx
            .client
            .fs_read(&self.ctx.session(), path, &self.cwd())
            .await?
            .ok_or_else(|| VmError::NotFound(path.to_string()))
    }

    async fn write(&self, path: &str, data: &[u8]) -> Result<()> {
        let absolute = self.absolute(path);
        let tar_gz = build_tar_gz(&absolute, data)?;
        self.ctx
            .client
            .fs_write(&self.ctx.session(), "/", tar_gz)
            .await
    }

    async fn list(&self, path: &str, depth: u32) -> Result<Vec<FileEntry>> {
        let script = format!(
            "find {} -mindepth 1 -maxdepth {} -printf '%y\\t%s\\t%T@\\t%p\\n' 2>/dev/null || true",
            shell_quote(path),
            depth.max(1)
        );
        let out = self.ctx.run_shell_ok(&script).await?;
        let mut entries = Vec::new();
        for line in out.stdout.lines() {
            let parts: Vec<&str> = line.splitn(4, '\t').collect();
            if parts.len() != 4 {
                continue;
            }
            let kind = match parts[0] {
                "d" => FileKind::Dir,
                "l" => FileKind::Symlink,
                _ => FileKind::File,
            };
            let size: u64 = parts[1].parse().unwrap_or(0);
            let modified_unix_ms = parts[2]
                .parse::<f64>()
                .map(|f| (f * 1000.0) as i64)
                .unwrap_or(0);
            entries.push(FileEntry {
                path: PathBuf::from(parts[3]),
                kind,
                size,
                modified_unix_ms,
            });
        }
        Ok(entries)
    }

    async fn stat(&self, path: &str) -> Result<FileEntry> {
        let script = format!(
            "stat -c '%F|%s|%Y' {} 2>/dev/null || true",
            shell_quote(path)
        );
        let out = self.ctx.run_shell_ok(&script).await?;
        let line = out.stdout.trim();
        if line.is_empty() {
            return Err(VmError::NotFound(path.to_string()));
        }
        let parts: Vec<&str> = line.split('|').collect();
        if parts.len() != 3 {
            return Err(VmError::Backend {
                backend: "vercel".into(),
                message: format!("bad stat output: {line}"),
            });
        }
        let kind = if parts[0].contains("directory") {
            FileKind::Dir
        } else if parts[0].contains("symbolic") {
            FileKind::Symlink
        } else {
            FileKind::File
        };
        Ok(FileEntry {
            path: PathBuf::from(path),
            kind,
            size: parts[1].parse().unwrap_or(0),
            modified_unix_ms: parts[2].parse::<i64>().unwrap_or(0) * 1000,
        })
    }

    async fn watch(&self, _path: &str, _recursive: bool) -> Result<WatchStream> {
        Err(VmError::Lifecycle(
            "watch not implemented for vercel backend".into(),
        ))
    }

    async fn mkdir(&self, path: &str, recursive: bool) -> Result<()> {
        if recursive {
            // The fs/mkdir endpoint creates parents like `mkdir -p`.
            self.ctx
                .client
                .fs_mkdir(&self.ctx.session(), path, &self.cwd())
                .await
        } else {
            self.ctx
                .run_shell_ok(&format!("mkdir {}", shell_quote(path)))
                .await
                .map(|_| ())
        }
    }

    async fn remove(&self, path: &str, recursive: bool) -> Result<()> {
        let flag = if recursive { "-rf" } else { "-f" };
        self.ctx
            .run_shell_ok(&format!("rm {flag} {}", shell_quote(path)))
            .await
            .map(|_| ())
    }

    async fn upload(&self, local: &Path, remote: &str) -> Result<()> {
        let meta = tokio::fs::metadata(local).await?;
        if meta.is_dir() {
            return Err(VmError::Invalid(format!(
                "vercel upload supports files only, {} is a directory",
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn map_status(status: &str) -> SandboxStatus {
    match status {
        "pending" => SandboxStatus::Creating,
        // Snapshotting sessions are still transitioning from a live VM.
        "running" | "snapshotting" => SandboxStatus::Running,
        "stopping" | "stopped" => SandboxStatus::Paused,
        "aborted" => SandboxStatus::Killed,
        "failed" => SandboxStatus::Failed,
        _ => SandboxStatus::Running,
    }
}

fn encode_fs_pointer(snapshot_id: &str) -> String {
    format!("vercel-snapshot={snapshot_id}")
}

fn parse_fs_pointer(pointer: &str) -> Result<String> {
    match pointer.strip_prefix("vercel-snapshot=") {
        Some(id) if !id.is_empty() => Ok(id.to_string()),
        _ => Err(VmError::Snapshot(format!(
            "malformed vercel fs_pointer: {pointer}"
        ))),
    }
}

/// Build an in-memory `.tar.gz` holding one file at `absolute` (relative to
/// `/` inside the archive), matching the `fs/write` extraction contract.
fn build_tar_gz(absolute: &Path, data: &[u8]) -> Result<Vec<u8>> {
    let relative = absolute
        .strip_prefix("/")
        .unwrap_or(absolute)
        .to_path_buf();
    let mut header = tar::Header::new_gnu();
    header.set_size(data.len() as u64);
    header.set_mode(0o644);
    header.set_mtime(chrono::Utc::now().timestamp().max(0) as u64);
    header.set_cksum();

    let encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    let mut builder = tar::Builder::new(encoder);
    builder
        .append_data(&mut header, &relative, data)
        .map_err(VmError::Io)?;
    let encoder = builder.into_inner().map_err(VmError::Io)?;
    encoder.finish().map_err(VmError::Io)
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
    use std::io::Read;

    #[test]
    fn fs_pointer_roundtrip() {
        let ptr = encode_fs_pointer("snap_123");
        assert_eq!(ptr, "vercel-snapshot=snap_123");
        assert_eq!(parse_fs_pointer(&ptr).unwrap(), "snap_123");
        assert!(parse_fs_pointer("vercel-snapshot=").is_err());
        assert!(parse_fs_pointer("sprite=x;checkpoint=y").is_err());
    }

    #[test]
    fn status_mapping() {
        assert_eq!(map_status("pending"), SandboxStatus::Creating);
        assert_eq!(map_status("running"), SandboxStatus::Running);
        assert_eq!(map_status("snapshotting"), SandboxStatus::Running);
        assert_eq!(map_status("stopping"), SandboxStatus::Paused);
        assert_eq!(map_status("stopped"), SandboxStatus::Paused);
        assert_eq!(map_status("aborted"), SandboxStatus::Killed);
        assert_eq!(map_status("failed"), SandboxStatus::Failed);
    }

    #[test]
    fn cmd_stream_lines_parse() {
        let started: CmdStreamLine =
            serde_json::from_str(r#"{"command":{"id":"cmd_1","name":"sh","args":[],"cwd":"/","sessionId":"s","exitCode":null,"startedAt":1}}"#)
                .unwrap();
        assert!(matches!(
            started,
            CmdStreamLine::Command { ref command } if command.exit_code.is_none()
        ));

        let stdout: CmdStreamLine =
            serde_json::from_str(r#"{"stream":"stdout","data":"hello\n"}"#).unwrap();
        assert!(matches!(
            stdout,
            CmdStreamLine::Log(ApiLogLine::Stdout { ref data }) if data == "hello\n"
        ));

        let finished: CmdStreamLine =
            serde_json::from_str(r#"{"command":{"id":"cmd_1","exitCode":2}}"#).unwrap();
        assert!(matches!(
            finished,
            CmdStreamLine::Command { ref command } if command.exit_code == Some(2)
        ));

        let error: CmdStreamLine = serde_json::from_str(
            r#"{"stream":"error","data":{"code":"boom","message":"it broke"}}"#,
        )
        .unwrap();
        assert!(matches!(
            error,
            CmdStreamLine::Log(ApiLogLine::Error { ref data }) if data.code == "boom"
        ));
    }

    #[test]
    fn tar_gz_contains_relative_entry() {
        let archive = build_tar_gz(Path::new("/work/notes/hello.txt"), b"hi there").unwrap();
        let gz = flate2::read::GzDecoder::new(archive.as_slice());
        let mut tar = tar::Archive::new(gz);
        let mut entries = tar.entries().unwrap();
        let mut entry = entries.next().unwrap().unwrap();
        assert_eq!(
            entry.path().unwrap().display().to_string(),
            "work/notes/hello.txt"
        );
        let mut content = String::new();
        entry.read_to_string(&mut content).unwrap();
        assert_eq!(content, "hi there");
        assert!(entries.next().is_none());
    }

    #[test]
    fn runtime_reports_capabilities() {
        let rt = VercelRuntime::new(VercelConfig::new("tok", "prj_test")).unwrap();
        assert_eq!(rt.name(), "vercel");
        let caps = rt.capabilities();
        assert!(caps.snapshots);
        assert!(caps.pause_resume);
        assert!(caps.remote);
        assert!(caps.network_isolation);
        assert!(!caps.shared_volumes);
    }

    #[test]
    fn create_body_maps_opts() {
        let rt = VercelRuntime::new(
            VercelConfig::new("tok", "prj_test").with_sandbox_timeout_ms(60_000),
        )
        .unwrap();
        let opts = SandboxOpts::default()
            .with_env("FOO", "bar")
            .with_label("team", "cersei");
        let mut opts = opts;
        opts.cpu_limit = Some(2.5);
        let body = rt.create_body(&opts, "cersei-vm-x");
        assert_eq!(body["projectId"], "prj_test");
        assert_eq!(body["name"], "cersei-vm-x");
        assert_eq!(body["env"]["FOO"], "bar");
        assert_eq!(body["tags"]["team"], "cersei");
        assert_eq!(body["resources"]["vcpus"], 3);
        assert_eq!(body["timeout"], 60_000);
        // The Cersei default image must not be forwarded to Vercel.
        assert!(body.get("image").is_none());

        let custom = SandboxOpts::image("vercel/sandbox/node:22");
        let body = rt.create_body(&custom, "cersei-vm-y");
        assert_eq!(body["image"], "vercel/sandbox/node:22");
    }

    #[tokio::test]
    async fn ndjson_splitter_handles_partial_lines() {
        // Feed a response body split at awkward boundaries through the parser.
        let body = "{\"stream\":\"stdout\",\"data\":\"a\"}\n{\"stream\":\"std";
        let tail = "err\",\"data\":\"b\"}\n{\"command\":{\"id\":\"c\",\"exitCode\":0}}";
        let chunks: Vec<std::result::Result<Bytes, std::io::Error>> = vec![
            Ok(Bytes::from_static(body.as_bytes())),
            Ok(Bytes::from_static(tail.as_bytes())),
        ];
        let stream = futures::stream::iter(chunks);
        let resp = reqwest::Response::from(
            http_response_from_stream(stream)
        );
        let out = collect_run_output(resp).await.unwrap();
        assert_eq!(out.stdout, "a");
        assert_eq!(out.stderr, "b");
        assert_eq!(out.exit_code, 0);
    }

    /// Build an `http::Response` with a streaming body for parser tests.
    fn http_response_from_stream<S>(stream: S) -> http::Response<reqwest::Body>
    where
        S: futures::Stream<Item = std::result::Result<Bytes, std::io::Error>>
            + Send
            + Sync
            + 'static,
    {
        http::Response::builder()
            .status(200)
            .body(reqwest::Body::wrap_stream(stream))
            .unwrap()
    }
}
