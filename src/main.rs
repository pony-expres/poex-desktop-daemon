use anyhow::{Context as _, Result, anyhow, bail};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use flags2env::BundledFlags2Env;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    env,
    fs::{self, OpenOptions},
    io::{ErrorKind, Write},
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::Command,
    sync::Semaphore,
    time::timeout,
};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const MAX_TIMEOUT_MS: u64 = 20 * 60 * 1_000;
const MAX_BODY_BYTES: usize = 256 * 1024;
const MAX_PARALLELISM: usize = 256;
const MAX_WORKER_STDOUT_BYTES: usize = 4 * 1024 * 1024;
const MAX_WORKER_STDERR_BYTES: usize = 1024 * 1024;
const MAX_TOKEN_FILE_BYTES: u64 = 4096;

#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
struct CliConfig {
    POEX_DESKTOP_ADDR: String,
    POEX_WORKER_COMMAND: String,
    POEX_WORKER_ARGS_JSON: String,
    POEX_MAX_PARALLEL_INVOCATIONS: i64,\n    POEX_TENANT_POOL_SIZE: i64,
    POEX_DESKTOP_TOKEN_FILE: Option<String>,
    POEX_DESKTOP_LOG: String,
}

#[derive(Debug)]
struct RuntimeConfig {
    addr: SocketAddr,
    worker_command: String,
    worker_args: Vec<String>,
    parallelism: usize,\n    pool_size: usize,
    token_path: PathBuf,
    log_filter: String,
}

#[derive(Clone)]
struct AppState {
    token: Arc<str>,
    worker_command: Arc<str>,
    worker_args: Arc<Vec<String>>,
    permits: Arc<Semaphore>,\n    pools: Arc<Mutex<HashMap<CellKey, Arc<CellPool>>>>,\n    pool_size: usize,
    started_at: Instant,
    accepted: Arc<AtomicU64>,
    completed: Arc<AtomicU64>,
    failed: Arc<AtomicU64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InvocationRequest {
    invocation_id: String,
    tenant_id: String,
    deployment_id: String,
    payload_json: Value,
    timeout_ms: Option<u64>,
}

#[derive(Debug, Serialize)]
struct InvocationResponse {
    invocation_id: String,
    deployment_id: String,
    ok: bool,
    payload_json: Option<Value>,
    error: Option<String>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CellKey {
    tenant_id: String,
    deployment_id: String,
}

struct Cell {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    invocation_count: u64,
}

struct CellPool {
    cells: Vec<Arc<Mutex<Cell>>>,
    next: AtomicU64,
}

#[derive(Debug, Serialize)]
struct StatusResponse {
    runtime: &'static str,
    actor_reusable: bool,
    worker_mode: &'static str,
    config_source: &'static str,
    uptime_ms: u128,
    accepted: u64,
    completed: u64,
    failed: u64,
    available_slots: usize,\n    tenant_pool_size: usize,\n    live_generation_pools: usize,
}

#[derive(Debug)]
struct DrainCapture {
    bytes: Vec<u8>,
    exceeded: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = load_config()?;
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new(&config.log_filter).context("invalid tracing filter")?)
        .init();

    let token = load_or_create_token(&config.token_path)?;
    let state = AppState {
        token: Arc::from(token),
        worker_command: Arc::from(config.worker_command),
        worker_args: Arc::new(config.worker_args),
        permits: Arc::new(Semaphore::new(config.parallelism)),
        pools: Arc::new(Mutex::new(HashMap::new())),
        pool_size: config.pool_size,
        started_at: Instant::now(),
        accepted: Arc::new(AtomicU64::new(0)),
        completed: Arc::new(AtomicU64::new(0)),
        failed: Arc::new(AtomicU64::new(0)),
    };

    let app = Router::new()
        .route("/healthz", get(health))
        .route("/v1/status", get(status))
        .route("/v1/doctor", get(status))
        .route("/v1/invoke", post(invoke))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(config.addr).await?;
    tracing::info!(addr = %config.addr, "pony desktop daemon listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    return Ok(());
}

fn load_config() -> Result<RuntimeConfig> {
    let config_path = resolve_config_path()?;
    let config_path_text = config_path
        .to_str()
        .ok_or_else(|| anyhow!(".cli-flags.toml path is not UTF-8"))?;
    let parser = BundledFlags2Env::new();
    parser
        .audit_config(Some(config_path_text))
        .map_err(|error| anyhow!(error.to_string()))?;

    let argv = env::args().collect::<Vec<_>>();
    let parsed = parser
        .parse_structured(&argv, Some(config_path_text))
        .map_err(|error| anyhow!(error.to_string()))?;
    if !parsed.unknown_options.is_empty() {
        bail!(
            "unknown command-line options: {}",
            parsed.unknown_options.len()
        );
    }
    if !parsed.errors.is_empty() {
        bail!("invalid command-line values: {}", parsed.errors.join("; "));
    }
    if !parsed.extras.is_empty() {
        bail!("unexpected positional arguments: {}", parsed.extras.len());
    }

    let mut raw = env::vars().collect::<HashMap<_, _>>();
    raw.extend(parsed.provided_flags);
    let raw_config = parser
        .coerce::<CliConfig, _>(&raw, Some(config_path_text))
        .map_err(|error| anyhow!(error.to_string()))?;

    let addr = parse_loopback_addr(&raw_config.POEX_DESKTOP_ADDR)?;
    let worker_command = raw_config.POEX_WORKER_COMMAND.trim().to_owned();
    if worker_command.is_empty() {
        bail!("POEX_WORKER_COMMAND may not be empty");
    }
    let worker_args = serde_json::from_str::<Vec<String>>(&raw_config.POEX_WORKER_ARGS_JSON)
        .context("POEX_WORKER_ARGS_JSON must be a JSON string array")?;
    if worker_args.len() > 128 || worker_args.iter().any(|value| value.len() > 16 * 1024) {
        bail!("worker argument vector exceeds desktop limits");
    }

    let parallelism = usize::try_from(raw_config.POEX_MAX_PARALLEL_INVOCATIONS)
        .ok()
        .filter(|value| *value > 0 && *value <= MAX_PARALLELISM)
        .ok_or_else(|| {
            anyhow!("POEX_MAX_PARALLEL_INVOCATIONS must be between 1 and {MAX_PARALLELISM}")
        })?;
    let pool_size = usize::try_from(raw_config.POEX_TENANT_POOL_SIZE)
        .ok()
        .filter(|value| *value > 0 && *value <= 16)
        .ok_or_else(|| anyhow!("POEX_TENANT_POOL_SIZE must be between 1 and 16"))?;
    let token_path = match raw_config.POEX_DESKTOP_TOKEN_FILE {
        Some(path) if !path.trim().is_empty() => expand_home(Path::new(&path))?,
        _ => default_token_path()?,
    };

    return Ok(RuntimeConfig {
        addr,
        worker_command,
        worker_args,
        parallelism,
        token_path,
        log_filter: raw_config.POEX_DESKTOP_LOG,
    });
}

async fn health() -> &'static str {
    return "ok";
}

async fn status(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<StatusResponse>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    return Ok(Json(StatusResponse {
        runtime: "pony_native",
        actor_reusable: false,
        worker_mode: "tenant_generation_process_pool",
        config_source: "flags-2-env",
        uptime_ms: state.started_at.elapsed().as_millis(),
        accepted: state.accepted.load(Ordering::Relaxed),
        completed: state.completed.load(Ordering::Relaxed),
        failed: state.failed.load(Ordering::Relaxed),
        available_slots: state.permits.available_permits(),
        tenant_pool_size: state.pool_size,
        live_generation_pools: state.pools.lock().await.len(),
    }));
}

async fn invoke(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<InvocationRequest>,
) -> Result<Json<InvocationResponse>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    validate_identifier("invocation_id", &request.invocation_id)?;
    validate_identifier("tenant_id", &request.tenant_id)?;
    validate_identifier("deployment_id", &request.deployment_id)?;

    let timeout_ms = request.timeout_ms.unwrap_or(30_000);
    if timeout_ms == 0 || timeout_ms > MAX_TIMEOUT_MS {
        return Err((StatusCode::BAD_REQUEST,
            format!("timeout_ms must be between 1 and {MAX_TIMEOUT_MS}")));
    }

    let permit = state.permits.clone().try_acquire_owned().map_err(|_| {
        (StatusCode::SERVICE_UNAVAILABLE, "Pony invocation capacity is exhausted".to_owned())
    })?;
    state.accepted.fetch_add(1, Ordering::Relaxed);
    let key = CellKey {
        tenant_id: request.tenant_id.clone(),
        deployment_id: request.deployment_id.clone(),
    };
    let pool = ensure_pool(&state, &key).await.map_err(service_unavailable)?;
    let index = (pool.next.fetch_add(1, Ordering::Relaxed) as usize) % pool.cells.len();
    let cell = pool.cells[index].clone();
    let result = invoke_cell(&cell, &request, Duration::from_millis(timeout_ms)).await;
    drop(permit);

    state.completed.fetch_add(1, Ordering::Relaxed);
    if result.is_err() {
        state.failed.fetch_add(1, Ordering::Relaxed);
        retire_pool(&state, &key).await;
    }

    let response = match result {
        Ok(payload_json) => InvocationResponse {
            invocation_id: request.invocation_id,
            deployment_id: request.deployment_id,
            ok: true,
            payload_json: Some(payload_json),
            error: None,
        },
        Err(error) => InvocationResponse {
            invocation_id: request.invocation_id,
            deployment_id: request.deployment_id,
            ok: false,
            payload_json: None,
            error: Some(error.to_string()),
        },
    };
    Ok(Json(response))
}

async fn ensure_pool(state: &AppState, key: &CellKey) -> Result<Arc<CellPool>> {
    if let Some(pool) = state.pools.lock().await.get(key).cloned() {
        return Ok(pool);
    }

    let mut cells = Vec::with_capacity(state.pool_size);
    for _ in 0..state.pool_size {
        match spawn_cell(state, key).await {
            Ok(cell) => cells.push(Arc::new(Mutex::new(cell))),
            Err(error) => {
                for cell in cells {
                    let mut cell = cell.lock().await;
                    let _ = cell.child.kill().await;
                    let _ = cell.child.wait().await;
                }
                return Err(error);
            }
        }
    }
    let pool = Arc::new(CellPool { cells, next: AtomicU64::new(0) });
    let mut pools = state.pools.lock().await;
    Ok(pools.entry(key.clone()).or_insert_with(|| pool.clone()).clone())
}

async fn spawn_cell(state: &AppState, key: &CellKey) -> Result<Cell> {
    let mut child = Command::new(state.worker_command.as_ref())
        .args(state.worker_args.iter())
        .env("POEX_TENANT_ID", &key.tenant_id)
        .env("POEX_DEPLOYMENT_ID", &key.deployment_id)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("failed to start {}", state.worker_command))?;
    let stdin = child.stdin.take().ok_or_else(|| anyhow!("worker stdin unavailable"))?;
    let stdout = child.stdout.take().ok_or_else(|| anyhow!("worker stdout unavailable"))?;
    Ok(Cell { child, stdin, stdout: BufReader::new(stdout), invocation_count: 0 })
}

async fn invoke_cell(
    cell: &Arc<Mutex<Cell>>,
    request: &InvocationRequest,
    deadline: Duration,
) -> Result<Value> {
    let mut cell = cell.lock().await;
    if cell.child.try_wait()?.is_some() {
        bail!("Pony tenant cell exited before invocation");
    }
    let mut payload = serde_json::to_vec(&request.payload_json)?;
    if payload.contains(&b'\n') {
        bail!("serialized invocation payload unexpectedly contains a newline");
    }
    payload.push(b'\n');
    cell.stdin.write_all(&payload).await?;
    cell.stdin.flush().await?;

    let mut response = String::new();
    let read = timeout(deadline, cell.stdout.read_line(&mut response))
        .await
        .map_err(|_| anyhow!("Pony invocation timed out; generation pool will be retired"))??;
    if read == 0 {
        bail!("Pony tenant cell closed its output");
    }
    if response.len() > MAX_WORKER_STDOUT_BYTES {
        bail!("Pony response exceeded {MAX_WORKER_STDOUT_BYTES} bytes");
    }
    cell.invocation_count = cell.invocation_count.saturating_add(1);
    serde_json::from_str(response.trim_end()).context("Pony worker response was not JSON")
}

async fn retire_pool(state: &AppState, key: &CellKey) {
    if let Some(pool) = state.pools.lock().await.remove(key) {
        for cell in &pool.cells {
            let mut cell = cell.lock().await;
            let _ = cell.child.kill().await;
            let _ = cell.child.wait().await;
        }
    }
}

fn service_unavailable(error: impl std::fmt::Display) -> (StatusCode, String) {
    (StatusCode::SERVICE_UNAVAILABLE, error.to_string())
}

fn authorize(headers: &HeaderMap, state: &AppState) -> Result<(), (StatusCode, String)> {
    let provided = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));

    if provided == Some(state.token.as_ref()) {
        return Ok(());
    }

    return Err((StatusCode::UNAUTHORIZED, "unauthorized".to_owned()));
}

fn validate_identifier(name: &str, value: &str) -> Result<(), (StatusCode, String)> {
    let valid = !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        && value != "."
        && value != "..";
    if valid {
        return Ok(());
    }
    return Err((StatusCode::BAD_REQUEST, format!("invalid {name}")));
}

fn parse_loopback_addr(value: &str) -> Result<SocketAddr> {
    let addr: SocketAddr = value
        .parse()
        .context("POEX_DESKTOP_ADDR is not a socket address")?;
    if !is_loopback(addr.ip()) {
        bail!("POEX_DESKTOP_ADDR must bind to loopback");
    }
    return Ok(addr);
}

fn is_loopback(ip: IpAddr) -> bool {
    return ip.is_loopback();
}

fn resolve_config_path() -> Result<PathBuf> {
    if let Some(path) = env::var_os("POEX_DESKTOP_FLAGS_CONFIG") {
        let path = PathBuf::from(path);
        if path.is_file() {
            return Ok(path);
        }
        bail!("POEX_DESKTOP_FLAGS_CONFIG is not a readable file");
    }

    let current = env::current_dir()?.join(".cli-flags.toml");
    if current.is_file() {
        return Ok(current);
    }

    let executable = env::current_exe()?;
    if let Some(parent) = executable.parent() {
        let adjacent = parent.join(".cli-flags.toml");
        if adjacent.is_file() {
            return Ok(adjacent);
        }
    }

    bail!("cannot locate .cli-flags.toml");
}

fn default_token_path() -> Result<PathBuf> {
    let home = env::var_os("HOME")
        .or_else(|| env::var_os("USERPROFILE"))
        .ok_or_else(|| anyhow!("HOME or USERPROFILE is required"))?;
    return Ok(PathBuf::from(home).join(".pony-expres/daemon/token"));
}

fn expand_home(path: &Path) -> Result<PathBuf> {
    let text = path.to_string_lossy();
    if text == "~" || text.starts_with("~/") {
        let home = env::var_os("HOME")
            .or_else(|| env::var_os("USERPROFILE"))
            .ok_or_else(|| anyhow!("HOME or USERPROFILE is required"))?;
        let suffix = text.trim_start_matches('~').trim_start_matches('/');
        return Ok(PathBuf::from(home).join(suffix));
    }
    return Ok(path.to_path_buf());
}

fn read_token_file(path: &Path) -> Result<Option<String>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("cannot inspect token file {}", path.display()));
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("desktop daemon token path must be a regular non-symlink file");
    }
    if metadata.len() == 0 || metadata.len() > MAX_TOKEN_FILE_BYTES {
        bail!("desktop daemon token file size is invalid");
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            bail!("desktop daemon token file must not be accessible by group/other users");
        }
    }

    let token = fs::read_to_string(path)
        .with_context(|| format!("cannot read desktop daemon token file {}", path.display()))?;
    let token = token.trim();
    if token.len() < 32 || token.len() > 4096 || token.chars().any(char::is_whitespace) {
        bail!("desktop daemon token file is malformed");
    }
    return Ok(Some(token.to_owned()));
}

fn load_or_create_token(path: &Path) -> Result<String> {
    if let Some(token) = read_token_file(path)? {
        return Ok(token);
    }

    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("token path has no parent"))?;
    fs::create_dir_all(parent)
        .with_context(|| format!("cannot create token directory {}", parent.display()))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;

        let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        let mut options = OpenOptions::new();
        options.write(true).create_new(true).mode(0o600);
        match options.open(path) {
            Ok(mut file) => {
                file.write_all(format!("{token}\n").as_bytes())?;
                file.sync_all()?;
                return Ok(token);
            }
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                return read_token_file(path)?.ok_or_else(|| {
                    anyhow!("desktop daemon token file appeared but could not be read")
                });
            }
            Err(error) => return Err(error.into()),
        }
    }

    #[cfg(not(unix))]
    {
        let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        match options.open(path) {
            Ok(mut file) => {
                file.write_all(format!("{token}\n").as_bytes())?;
                file.sync_all()?;
                return Ok(token);
            }
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                return read_token_file(path)?.ok_or_else(|| {
                    anyhow!("desktop daemon token file appeared but could not be read")
                });
            }
            Err(error) => return Err(error.into()),
        }
    }
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_loopback_bind() {
        assert!(parse_loopback_addr("127.0.0.1:8762").is_ok());
        assert!(parse_loopback_addr("0.0.0.0:8762").is_err());
    }

    #[test]
    fn identifiers_reject_path_traversal() {
        assert!(validate_identifier("deployment_id", "deployment-1").is_ok());
        assert!(validate_identifier("deployment_id", "..").is_err());
        assert!(validate_identifier("deployment_id", "tenant/escape").is_err());
    }

    #[test]
    fn truncates_worker_errors() {
        let value = "x".repeat(1024);
        assert_eq!(truncate(&value, 512).len(), 512);
    }

    #[tokio::test]
    async fn bounded_drain_keeps_draining_after_limit() {
        let (mut writer, mut reader) = tokio::io::duplex(64);
        let write = tokio::spawn(async move {
            writer.write_all(b"0123456789abcdef").await?;
            writer.shutdown().await?;
            return Ok::<_, std::io::Error>(());
        });

        let capture = drain_bounded(&mut reader, 8).await;
        assert!(capture.is_ok());
        if let Ok(capture) = capture {
            assert_eq!(capture.bytes, b"01234567");
            assert!(capture.exceeded);
        }
        assert!(write.await.is_ok());
    }
}use anyhow::{Context as _, Result, anyhow, bail};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use flags2env::BundledFlags2Env;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    env,
    fs::{self, OpenOptions},
    io::{ErrorKind, Write},
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::Command,
    sync::Semaphore,
    time::timeout,
};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const MAX_TIMEOUT_MS: u64 = 20 * 60 * 1_000;
const MAX_BODY_BYTES: usize = 256 * 1024;
const MAX_PARALLELISM: usize = 256;
const MAX_WORKER_STDOUT_BYTES: usize = 4 * 1024 * 1024;
const MAX_WORKER_STDERR_BYTES: usize = 1024 * 1024;
const MAX_TOKEN_FILE_BYTES: u64 = 4096;

#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
struct CliConfig {
    POEX_DESKTOP_ADDR: String,
    POEX_WORKER_COMMAND: String,
    POEX_WORKER_ARGS_JSON: String,
    POEX_MAX_PARALLEL_INVOCATIONS: i64,\n    POEX_TENANT_POOL_SIZE: i64,
    POEX_DESKTOP_TOKEN_FILE: Option<String>,
    POEX_DESKTOP_LOG: String,
}

#[derive(Debug)]
struct RuntimeConfig {
    addr: SocketAddr,
    worker_command: String,
    worker_args: Vec<String>,
    parallelism: usize,\n    pool_size: usize,
    token_path: PathBuf,
    log_filter: String,
}

#[derive(Clone)]
struct AppState {
    token: Arc<str>,
    worker_command: Arc<str>,
    worker_args: Arc<Vec<String>>,
    permits: Arc<Semaphore>,\n    pools: Arc<Mutex<HashMap<CellKey, Arc<CellPool>>>>,\n    pool_size: usize,
    started_at: Instant,
    accepted: Arc<AtomicU64>,
    completed: Arc<AtomicU64>,
    failed: Arc<AtomicU64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InvocationRequest {
    invocation_id: String,
    tenant_id: String,
    deployment_id: String,
    payload_json: Value,
    timeout_ms: Option<u64>,
}

#[derive(Debug, Serialize)]
struct InvocationResponse {
    invocation_id: String,
    deployment_id: String,
    ok: bool,
    payload_json: Option<Value>,
    error: Option<String>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CellKey {
    tenant_id: String,
    deployment_id: String,
}

struct Cell {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    invocation_count: u64,
}

struct CellPool {
    cells: Vec<Arc<Mutex<Cell>>>,
    next: AtomicU64,
}

#[derive(Debug, Serialize)]
struct StatusResponse {
    runtime: &'static str,
    actor_reusable: bool,
    worker_mode: &'static str,
    config_source: &'static str,
    uptime_ms: u128,
    accepted: u64,
    completed: u64,
    failed: u64,
    available_slots: usize,\n    tenant_pool_size: usize,\n    live_generation_pools: usize,
}

#[derive(Debug)]
struct DrainCapture {
    bytes: Vec<u8>,
    exceeded: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = load_config()?;
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new(&config.log_filter).context("invalid tracing filter")?)
        .init();

    let token = load_or_create_token(&config.token_path)?;
    let state = AppState {
        token: Arc::from(token),
        worker_command: Arc::from(config.worker_command),
        worker_args: Arc::new(config.worker_args),
        permits: Arc::new(Semaphore::new(config.parallelism)),
        started_at: Instant::now(),
        accepted: Arc::new(AtomicU64::new(0)),
        completed: Arc::new(AtomicU64::new(0)),
        failed: Arc::new(AtomicU64::new(0)),
    };

    let app = Router::new()
        .route("/healthz", get(health))
        .route("/v1/status", get(status))
        .route("/v1/doctor", get(status))
        .route("/v1/invoke", post(invoke))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(config.addr).await?;
    tracing::info!(addr = %config.addr, "pony desktop daemon listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    return Ok(());
}

fn load_config() -> Result<RuntimeConfig> {
    let config_path = resolve_config_path()?;
    let config_path_text = config_path
        .to_str()
        .ok_or_else(|| anyhow!(".cli-flags.toml path is not UTF-8"))?;
    let parser = BundledFlags2Env::new();
    parser
        .audit_config(Some(config_path_text))
        .map_err(|error| anyhow!(error.to_string()))?;

    let argv = env::args().collect::<Vec<_>>();
    let parsed = parser
        .parse_structured(&argv, Some(config_path_text))
        .map_err(|error| anyhow!(error.to_string()))?;
    if !parsed.unknown_options.is_empty() {
        bail!(
            "unknown command-line options: {}",
            parsed.unknown_options.len()
        );
    }
    if !parsed.errors.is_empty() {
        bail!("invalid command-line values: {}", parsed.errors.join("; "));
    }
    if !parsed.extras.is_empty() {
        bail!("unexpected positional arguments: {}", parsed.extras.len());
    }

    let mut raw = env::vars().collect::<HashMap<_, _>>();
    raw.extend(parsed.provided_flags);
    let raw_config = parser
        .coerce::<CliConfig, _>(&raw, Some(config_path_text))
        .map_err(|error| anyhow!(error.to_string()))?;

    let addr = parse_loopback_addr(&raw_config.POEX_DESKTOP_ADDR)?;
    let worker_command = raw_config.POEX_WORKER_COMMAND.trim().to_owned();
    if worker_command.is_empty() {
        bail!("POEX_WORKER_COMMAND may not be empty");
    }
    let worker_args = serde_json::from_str::<Vec<String>>(&raw_config.POEX_WORKER_ARGS_JSON)
        .context("POEX_WORKER_ARGS_JSON must be a JSON string array")?;
    if worker_args.len() > 128 || worker_args.iter().any(|value| value.len() > 16 * 1024) {
        bail!("worker argument vector exceeds desktop limits");
    }

    let parallelism = usize::try_from(raw_config.POEX_MAX_PARALLEL_INVOCATIONS)
        .ok()
        .filter(|value| *value > 0 && *value <= MAX_PARALLELISM)
        .ok_or_else(|| {
            anyhow!("POEX_MAX_PARALLEL_INVOCATIONS must be between 1 and {MAX_PARALLELISM}")
        })?;
    let token_path = match raw_config.POEX_DESKTOP_TOKEN_FILE {
        Some(path) if !path.trim().is_empty() => expand_home(Path::new(&path))?,
        _ => default_token_path()?,
    };

    return Ok(RuntimeConfig {
        addr,
        worker_command,
        worker_args,
        parallelism,
        token_path,
        log_filter: raw_config.POEX_DESKTOP_LOG,
    });
}

async fn health() -> &'static str {
    return "ok";
}

async fn status(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<StatusResponse>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    return Ok(Json(StatusResponse {
        runtime: "pony_native",
        actor_reusable: false,
        worker_mode: "fresh_process",
        config_source: "flags-2-env",
        uptime_ms: state.started_at.elapsed().as_millis(),
        accepted: state.accepted.load(Ordering::Relaxed),
        completed: state.completed.load(Ordering::Relaxed),
        failed: state.failed.load(Ordering::Relaxed),
        available_slots: state.permits.available_permits(),
    }));
}

async fn invoke(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<InvocationRequest>,
) -> Result<Json<InvocationResponse>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    validate_identifier("invocation_id", &request.invocation_id)?;
    validate_identifier("tenant_id", &request.tenant_id)?;
    validate_identifier("deployment_id", &request.deployment_id)?;

    let timeout_ms = request.timeout_ms.unwrap_or(30_000);
    if timeout_ms == 0 || timeout_ms > MAX_TIMEOUT_MS {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("timeout_ms must be between 1 and {MAX_TIMEOUT_MS}"),
        ));
    }

    let permit = state.permits.clone().try_acquire_owned().map_err(|_| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "fresh Pony worker capacity is exhausted".to_owned(),
        )
    })?;
    state.accepted.fetch_add(1, Ordering::Relaxed);

    let result = run_fresh_worker(&state, &request, Duration::from_millis(timeout_ms)).await;
    drop(permit);
    state.completed.fetch_add(1, Ordering::Relaxed);
    if result.is_err() {
        state.failed.fetch_add(1, Ordering::Relaxed);
    }

    let response = match result {
        Ok(payload_json) => InvocationResponse {
            invocation_id: request.invocation_id,
            deployment_id: request.deployment_id,
            ok: true,
            payload_json: Some(payload_json),
            error: None,
        },
        Err(error) => InvocationResponse {
            invocation_id: request.invocation_id,
            deployment_id: request.deployment_id,
            ok: false,
            payload_json: None,
            error: Some(error.to_string()),
        },
    };

    return Ok(Json(response));
}

async fn run_fresh_worker(
    state: &AppState,
    request: &InvocationRequest,
    deadline: Duration,
) -> Result<Value> {
    let mut child = Command::new(state.worker_command.as_ref())
        .args(state.worker_args.iter())
        .env("POEX_TENANT_ID", &request.tenant_id)
        .env("POEX_DEPLOYMENT_ID", &request.deployment_id)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("failed to start {}", state.worker_command))?;

    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow!("worker stdin unavailable"))?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("worker stdout unavailable"))?;
    let mut stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow!("worker stderr unavailable"))?;

    let payload = serde_json::to_vec(&request.payload_json)?;
    stdin.write_all(&payload).await?;
    stdin.shutdown().await?;
    drop(stdin);

    let execution = async {
        let (status, stdout_capture, stderr_capture) = tokio::join!(
            child.wait(),
            drain_bounded(&mut stdout, MAX_WORKER_STDOUT_BYTES),
            drain_bounded(&mut stderr, MAX_WORKER_STDERR_BYTES),
        );
        return Ok::<_, anyhow::Error>((status?, stdout_capture?, stderr_capture?));
    };

    let (status, stdout_capture, stderr_capture) = timeout(deadline, execution)
        .await
        .map_err(|_| anyhow!("invocation timed out; fresh worker was terminated"))??;

    if stdout_capture.exceeded {
        bail!("worker stdout exceeded {MAX_WORKER_STDOUT_BYTES} bytes");
    }
    if stderr_capture.exceeded {
        bail!("worker stderr exceeded {MAX_WORKER_STDERR_BYTES} bytes");
    }

    if !status.success() {
        let stderr = String::from_utf8_lossy(&stderr_capture.bytes);
        let summary = stderr
            .lines()
            .find(|line| !line.trim().is_empty())
            .unwrap_or("worker exited unsuccessfully");
        bail!("worker failed: {}", truncate(summary, 512));
    }

    let stdout = String::from_utf8(stdout_capture.bytes).context("worker stdout was not UTF-8")?;
    let payload_json = serde_json::from_str(stdout.trim()).context("worker stdout was not JSON")?;
    return Ok(payload_json);
}

async fn drain_bounded<R>(reader: &mut R, max_bytes: usize) -> Result<DrainCapture>
where
    R: AsyncRead + Unpin,
{
    let mut bytes = Vec::with_capacity(max_bytes.min(64 * 1024));
    let mut exceeded = false;
    let mut buffer = [0_u8; 8192];

    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }

        let remaining = max_bytes.saturating_sub(bytes.len());
        let retained = remaining.min(read);
        if retained > 0 {
            bytes.extend_from_slice(&buffer[..retained]);
        }
        if read > retained {
            exceeded = true;
        }
    }

    return Ok(DrainCapture { bytes, exceeded });
}

fn truncate(value: &str, max_chars: usize) -> String {
    return value.chars().take(max_chars).collect();
}

fn authorize(headers: &HeaderMap, state: &AppState) -> Result<(), (StatusCode, String)> {
    let provided = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));

    if provided == Some(state.token.as_ref()) {
        return Ok(());
    }

    return Err((StatusCode::UNAUTHORIZED, "unauthorized".to_owned()));
}

fn validate_identifier(name: &str, value: &str) -> Result<(), (StatusCode, String)> {
    let valid = !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        && value != "."
        && value != "..";
    if valid {
        return Ok(());
    }
    return Err((StatusCode::BAD_REQUEST, format!("invalid {name}")));
}

fn parse_loopback_addr(value: &str) -> Result<SocketAddr> {
    let addr: SocketAddr = value
        .parse()
        .context("POEX_DESKTOP_ADDR is not a socket address")?;
    if !is_loopback(addr.ip()) {
        bail!("POEX_DESKTOP_ADDR must bind to loopback");
    }
    return Ok(addr);
}

fn is_loopback(ip: IpAddr) -> bool {
    return ip.is_loopback();
}

fn resolve_config_path() -> Result<PathBuf> {
    if let Some(path) = env::var_os("POEX_DESKTOP_FLAGS_CONFIG") {
        let path = PathBuf::from(path);
        if path.is_file() {
            return Ok(path);
        }
        bail!("POEX_DESKTOP_FLAGS_CONFIG is not a readable file");
    }

    let current = env::current_dir()?.join(".cli-flags.toml");
    if current.is_file() {
        return Ok(current);
    }

    let executable = env::current_exe()?;
    if let Some(parent) = executable.parent() {
        let adjacent = parent.join(".cli-flags.toml");
        if adjacent.is_file() {
            return Ok(adjacent);
        }
    }

    bail!("cannot locate .cli-flags.toml");
}

fn default_token_path() -> Result<PathBuf> {
    let home = env::var_os("HOME")
        .or_else(|| env::var_os("USERPROFILE"))
        .ok_or_else(|| anyhow!("HOME or USERPROFILE is required"))?;
    return Ok(PathBuf::from(home).join(".pony-expres/daemon/token"));
}

fn expand_home(path: &Path) -> Result<PathBuf> {
    let text = path.to_string_lossy();
    if text == "~" || text.starts_with("~/") {
        let home = env::var_os("HOME")
            .or_else(|| env::var_os("USERPROFILE"))
            .ok_or_else(|| anyhow!("HOME or USERPROFILE is required"))?;
        let suffix = text.trim_start_matches('~').trim_start_matches('/');
        return Ok(PathBuf::from(home).join(suffix));
    }
    return Ok(path.to_path_buf());
}

fn read_token_file(path: &Path) -> Result<Option<String>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("cannot inspect token file {}", path.display()));
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("desktop daemon token path must be a regular non-symlink file");
    }
    if metadata.len() == 0 || metadata.len() > MAX_TOKEN_FILE_BYTES {
        bail!("desktop daemon token file size is invalid");
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            bail!("desktop daemon token file must not be accessible by group/other users");
        }
    }

    let token = fs::read_to_string(path)
        .with_context(|| format!("cannot read desktop daemon token file {}", path.display()))?;
    let token = token.trim();
    if token.len() < 32 || token.len() > 4096 || token.chars().any(char::is_whitespace) {
        bail!("desktop daemon token file is malformed");
    }
    return Ok(Some(token.to_owned()));
}

fn load_or_create_token(path: &Path) -> Result<String> {
    if let Some(token) = read_token_file(path)? {
        return Ok(token);
    }

    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("token path has no parent"))?;
    fs::create_dir_all(parent)
        .with_context(|| format!("cannot create token directory {}", parent.display()))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;

        let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        let mut options = OpenOptions::new();
        options.write(true).create_new(true).mode(0o600);
        match options.open(path) {
            Ok(mut file) => {
                file.write_all(format!("{token}\n").as_bytes())?;
                file.sync_all()?;
                return Ok(token);
            }
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                return read_token_file(path)?.ok_or_else(|| {
                    anyhow!("desktop daemon token file appeared but could not be read")
                });
            }
            Err(error) => return Err(error.into()),
        }
    }

    #[cfg(not(unix))]
    {
        let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        match options.open(path) {
            Ok(mut file) => {
                file.write_all(format!("{token}\n").as_bytes())?;
                file.sync_all()?;
                return Ok(token);
            }
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                return read_token_file(path)?.ok_or_else(|| {
                    anyhow!("desktop daemon token file appeared but could not be read")
                });
            }
            Err(error) => return Err(error.into()),
        }
    }
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_loopback_bind() {
        assert!(parse_loopback_addr("127.0.0.1:8762").is_ok());
        assert!(parse_loopback_addr("0.0.0.0:8762").is_err());
    }

    #[test]
    fn identifiers_reject_path_traversal() {
        assert!(validate_identifier("deployment_id", "deployment-1").is_ok());
        assert!(validate_identifier("deployment_id", "..").is_err());
        assert!(validate_identifier("deployment_id", "tenant/escape").is_err());
    }

    #[test]
    fn truncates_worker_errors() {
        let value = "x".repeat(1024);
        assert_eq!(truncate(&value, 512).len(), 512);
    }

    #[tokio::test]
    async fn bounded_drain_keeps_draining_after_limit() {
        let (mut writer, mut reader) = tokio::io::duplex(64);
        let write = tokio::spawn(async move {
            writer.write_all(b"0123456789abcdef").await?;
            writer.shutdown().await?;
            return Ok::<_, std::io::Error>(());
        });

        let capture = drain_bounded(&mut reader, 8).await;
        assert!(capture.is_ok());
        if let Ok(capture) = capture {
            assert_eq!(capture.bytes, b"01234567");
            assert!(capture.exceeded);
        }
        assert!(write.await.is_ok());
    }
}
