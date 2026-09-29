use anyhow::{Context as _, Result, anyhow, bail};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path as AxumPath, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use flags2env::BundledFlags2Env;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
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
    io::{AsyncReadExt, AsyncWriteExt},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{Mutex, OwnedSemaphorePermit, Semaphore},
    time::timeout,
};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const MAX_TIMEOUT_MS: u64 = 20 * 60 * 1_000;
const MAX_BODY_BYTES: usize = 256 * 1024;
const MAX_PARALLELISM: usize = 256;
const MAX_LIVE_CELLS: usize = 2048;
const MAX_CELL_INVOCATIONS: u64 = 1_000_000;
const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;
const MAX_TOKEN_FILE_BYTES: u64 = 4096;

#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
struct CliConfig {
    POEX_DESKTOP_ADDR: String,
    POEX_WORKER_COMMAND: String,
    POEX_WORKER_ARGS_JSON: String,
    POEX_ARTIFACT_ROOT: Option<String>,
    POEX_MAX_PARALLEL_INVOCATIONS: i64,
    POEX_TENANT_POOL_SIZE: i64,
    POEX_MAX_LIVE_CELLS: i64,
    POEX_MAX_CELL_INVOCATIONS: i64,
    POEX_DESKTOP_TOKEN_FILE: Option<String>,
    POEX_DESKTOP_LOG: String,
}

#[derive(Debug)]
struct RuntimeConfig {
    addr: SocketAddr,
    worker_command: String,
    worker_args: Vec<String>,
    artifact_root: Option<PathBuf>,
    parallelism: usize,
    pool_size: usize,
    max_live_cells: usize,
    max_cell_invocations: u64,
    token_path: PathBuf,
    log_filter: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CellKey {
    tenant_id: String,
    deployment_id: String,
}

struct Cell {
    child: Child,
    stdin: ChildStdin,
    stdout: ChildStdout,
    invocation_count: u64,
    _live_permit: OwnedSemaphorePermit,
}

struct CellHandle {
    cell: Mutex<Cell>,
    slot: Arc<Semaphore>,
}

struct CellPool {
    cells: Vec<Arc<CellHandle>>,
    next: AtomicU64,
}

#[derive(Clone)]
struct AppState {
    token: Arc<str>,
    worker_command: Arc<str>,
    worker_args: Arc<Vec<String>>,
    artifact_root: Option<Arc<PathBuf>>,
    invocation_slots: Arc<Semaphore>,
    cell_slots: Arc<Semaphore>,
    pools: Arc<Mutex<HashMap<CellKey, Arc<CellPool>>>>,
    pool_size: usize,
    max_live_cells: usize,
    max_cell_invocations: u64,
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

#[derive(Debug, Serialize)]
struct StatusResponse {
    runtime: &'static str,
    actor_reusable: bool,
    process_cell_reusable: bool,
    cell_reuse_scope: &'static str,
    worker_mode: &'static str,
    config_source: &'static str,
    uptime_ms: u128,
    accepted: u64,
    completed: u64,
    failed: u64,
    live_cells: usize,
    live_generation_pools: usize,
    tenant_pool_size: usize,
    max_live_cells: usize,
    max_cell_invocations: u64,
    available_invocation_slots: usize,
    available_cell_slots: usize,
}

#[derive(Debug, Serialize)]
struct CellStatus {
    tenant_id: String,
    deployment_id: String,
    cell_index: usize,
    invocation_count: u64,
    running: bool,
    busy: bool,
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
        artifact_root: config.artifact_root.map(Arc::new),
        invocation_slots: Arc::new(Semaphore::new(config.parallelism)),
        cell_slots: Arc::new(Semaphore::new(config.max_live_cells)),
        pools: Arc::new(Mutex::new(HashMap::new())),
        pool_size: config.pool_size,
        max_live_cells: config.max_live_cells,
        max_cell_invocations: config.max_cell_invocations,
        started_at: Instant::now(),
        accepted: Arc::new(AtomicU64::new(0)),
        completed: Arc::new(AtomicU64::new(0)),
        failed: Arc::new(AtomicU64::new(0)),
    };

    let app = Router::new()
        .route("/healthz", get(health))
        .route("/v1/status", get(status))
        .route("/v1/doctor", get(status))
        .route("/v1/cells", get(list_cells))
        .route(
            "/v1/cells/{tenant_id}/{deployment_id}/retire",
            post(retire_pool_route),
        )
        .route("/v1/invoke", post(invoke))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(config.addr).await?;
    tracing::info!(addr = %config.addr, "pony desktop daemon listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    terminate_all_pools(&state).await;
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
        bail!("unknown command-line options: {}", parsed.unknown_options.len());
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
    let artifact_root = match raw_config.POEX_ARTIFACT_ROOT {
        Some(path) if !path.trim().is_empty() => Some(expand_home(Path::new(&path))?),
        _ => None,
    };
    let parallelism = bounded_usize(
        "POEX_MAX_PARALLEL_INVOCATIONS",
        raw_config.POEX_MAX_PARALLEL_INVOCATIONS,
        MAX_PARALLELISM,
    )?;
    let pool_size = bounded_usize("POEX_TENANT_POOL_SIZE", raw_config.POEX_TENANT_POOL_SIZE, 16)?;
    let max_live_cells = bounded_usize(
        "POEX_MAX_LIVE_CELLS",
        raw_config.POEX_MAX_LIVE_CELLS,
        MAX_LIVE_CELLS,
    )?;
    if pool_size > max_live_cells {
        bail!("POEX_TENANT_POOL_SIZE may not exceed POEX_MAX_LIVE_CELLS");
    }
    let max_cell_invocations = u64::try_from(raw_config.POEX_MAX_CELL_INVOCATIONS)
        .ok()
        .filter(|value| *value > 0 && *value <= MAX_CELL_INVOCATIONS)
        .ok_or_else(|| {
            anyhow!("POEX_MAX_CELL_INVOCATIONS must be between 1 and {MAX_CELL_INVOCATIONS}")
        })?;
    let token_path = match raw_config.POEX_DESKTOP_TOKEN_FILE {
        Some(path) if !path.trim().is_empty() => expand_home(Path::new(&path))?,
        _ => default_token_path()?,
    };

    return Ok(RuntimeConfig {
        addr,
        worker_command,
        worker_args,
        artifact_root,
        parallelism,
        pool_size,
        max_live_cells,
        max_cell_invocations,
        token_path,
        log_filter: raw_config.POEX_DESKTOP_LOG,
    });
}

fn bounded_usize(name: &str, value: i64, max: usize) -> Result<usize> {
    return usize::try_from(value)
        .ok()
        .filter(|value| *value > 0 && *value <= max)
        .ok_or_else(|| anyhow!("{name} must be between 1 and {max}"));
}

async fn health() -> &'static str {
    return "ok";
}

async fn status(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<StatusResponse>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    let pools = state.pools.lock().await;
    let live_generation_pools = pools.len();
    let live_cells = pools.values().map(|pool| pool.cells.len()).sum();
    drop(pools);

    return Ok(Json(StatusResponse {
        runtime: "pony_native",
        actor_reusable: false,
        process_cell_reusable: true,
        cell_reuse_scope: "same_tenant_generation",
        worker_mode: "tenant_generation_process_pool",
        config_source: "flags-2-env",
        uptime_ms: state.started_at.elapsed().as_millis(),
        accepted: state.accepted.load(Ordering::Relaxed),
        completed: state.completed.load(Ordering::Relaxed),
        failed: state.failed.load(Ordering::Relaxed),
        live_cells,
        live_generation_pools,
        tenant_pool_size: state.pool_size,
        max_live_cells: state.max_live_cells,
        max_cell_invocations: state.max_cell_invocations,
        available_invocation_slots: state.invocation_slots.available_permits(),
        available_cell_slots: state.cell_slots.available_permits(),
    }));
}

async fn list_cells(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<CellStatus>>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    let pools = {
        let pools = state.pools.lock().await;
        pools
            .iter()
            .map(|(key, pool)| (key.clone(), pool.clone()))
            .collect::<Vec<_>>()
    };

    let mut statuses = Vec::new();
    for (key, pool) in pools {
        for (cell_index, handle) in pool.cells.iter().enumerate() {
            let busy = handle.slot.available_permits() == 0;
            let mut cell = handle.cell.lock().await;
            let running = cell.child.try_wait().map_err(internal_error)?.is_none();
            statuses.push(CellStatus {
                tenant_id: key.tenant_id.clone(),
                deployment_id: key.deployment_id.clone(),
                cell_index,
                invocation_count: cell.invocation_count,
                running,
                busy,
            });
        }
    }
    return Ok(Json(statuses));
}

async fn retire_pool_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath((tenant_id, deployment_id)): AxumPath<(String, String)>,
) -> Result<Json<Value>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    validate_identifier("tenant_id", &tenant_id)?;
    validate_identifier("deployment_id", &deployment_id)?;
    let key = CellKey {
        tenant_id,
        deployment_id,
    };
    let retired = retire_pool(&state, &key).await;
    return Ok(Json(json!({ "retired": retired })));
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

    let invocation_permit = state
        .invocation_slots
        .clone()
        .try_acquire_owned()
        .map_err(|_| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Pony invocation capacity is exhausted".to_owned(),
            )
        })?;
    state.accepted.fetch_add(1, Ordering::Relaxed);

    let key = CellKey {
        tenant_id: request.tenant_id.clone(),
        deployment_id: request.deployment_id.clone(),
    };
    let result = async {
        let pool = ensure_pool(&state, &key).await?;
        let (handle, cell_permit) = acquire_cell(&pool).await?;
        let result = invoke_cell(&handle.cell, &request, Duration::from_millis(timeout_ms)).await;
        drop(cell_permit);
        return result;
    }
    .await;
    drop(invocation_permit);

    state.completed.fetch_add(1, Ordering::Relaxed);
    let retire = match &result {
        Ok(_) => {
            let pool = state.pools.lock().await.get(&key).cloned();
            if let Some(pool) = pool {
                cell_limit_reached(&pool, state.max_cell_invocations).await
            } else {
                false
            }
        }
        Err(_) => true,
    };
    if result.is_err() {
        state.failed.fetch_add(1, Ordering::Relaxed);
    }
    if retire {
        let _ = retire_pool(&state, &key).await;
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

async fn acquire_cell(pool: &Arc<CellPool>) -> Result<(Arc<CellHandle>, OwnedSemaphorePermit)> {
    let len = pool.cells.len();
    if len == 0 {
        bail!("tenant generation pool has no cells");
    }
    let start = (pool.next.fetch_add(1, Ordering::Relaxed) as usize) % len;
    for offset in 0..len {
        let index = (start + offset) % len;
        let handle = pool.cells[index].clone();
        if let Ok(permit) = handle.slot.clone().try_acquire_owned() {
            return Ok((handle, permit));
        }
    }

    let handle = pool.cells[start].clone();
    let permit = handle
        .slot
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| anyhow!("tenant generation cell was closed"))?;
    return Ok((handle, permit));
}

async fn ensure_pool(state: &AppState, key: &CellKey) -> Result<Arc<CellPool>> {
    if let Some(pool) = state.pools.lock().await.get(key).cloned() {
        return Ok(pool);
    }

    let candidate = spawn_pool(state, key).await?;
    let mut pools = state.pools.lock().await;
    if let Some(existing) = pools.get(key).cloned() {
        drop(pools);
        terminate_pool(&candidate).await;
        return Ok(existing);
    }
    pools.insert(key.clone(), candidate.clone());
    return Ok(candidate);
}

async fn spawn_pool(state: &AppState, key: &CellKey) -> Result<Arc<CellPool>> {
    let mut cells = Vec::with_capacity(state.pool_size);
    for _ in 0..state.pool_size {
        match spawn_cell(state, key).await {
            Ok(cell) => cells.push(Arc::new(CellHandle {
                cell: Mutex::new(cell),
                slot: Arc::new(Semaphore::new(1)),
            })),
            Err(error) => {
                let pool = Arc::new(CellPool {
                    cells,
                    next: AtomicU64::new(0),
                });
                terminate_pool(&pool).await;
                return Err(error);
            }
        }
    }
    return Ok(Arc::new(CellPool {
        cells,
        next: AtomicU64::new(0),
    }));
}

async fn spawn_cell(state: &AppState, key: &CellKey) -> Result<Cell> {
    let live_permit = state
        .cell_slots
        .clone()
        .try_acquire_owned()
        .map_err(|_| anyhow!("live Pony cell limit reached; retire an idle generation first"))?;

    let executable = worker_executable(state, key)?;
    let mut child = Command::new(&executable)
        .args(state.worker_args.iter())
        .env("POEX_TENANT_ID", &key.tenant_id)
        .env("POEX_DEPLOYMENT_ID", &key.deployment_id)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("failed to start {}", executable.display()))?;

    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow!("Pony cell stdin unavailable"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("Pony cell stdout unavailable"))?;
    return Ok(Cell {
        child,
        stdin,
        stdout,
        invocation_count: 0,
        _live_permit: live_permit,
    });
}

fn worker_executable(state: &AppState, key: &CellKey) -> Result<PathBuf> {
    if let Some(root) = &state.artifact_root {
        let artifact = artifact_path(root, key, "lambda")?;
        let metadata = fs::symlink_metadata(&artifact)
            .with_context(|| format!("cannot inspect Pony artifact {}", artifact.display()))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            bail!("Pony deployment artifact must be a regular non-symlink file");
        }
        return Ok(artifact);
    }
    return Ok(PathBuf::from(state.worker_command.as_ref()));
}

async fn invoke_cell(
    cell: &Mutex<Cell>,
    request: &InvocationRequest,
    deadline: Duration,
) -> Result<Value> {
    let mut cell = cell.lock().await;
    if cell.child.try_wait()?.is_some() {
        bail!("Pony tenant cell exited before invocation");
    }

    let payload = serde_json::to_vec(&request.payload_json)?;
    if payload.len() > MAX_FRAME_BYTES {
        bail!("Pony invocation payload exceeded {MAX_FRAME_BYTES} bytes");
    }

    let execution = async {
        write_frame(&mut cell.stdin, &payload).await?;
        let response = read_frame(&mut cell.stdout).await?;
        return Ok::<_, anyhow::Error>(response);
    };
    let response = timeout(deadline, execution)
        .await
        .map_err(|_| anyhow!("Pony invocation timed out; tenant generation will be retired"))??;

    cell.invocation_count = cell.invocation_count.saturating_add(1);
    return serde_json::from_slice(&response).context("Pony worker response was not JSON");
}

async fn write_frame(writer: &mut ChildStdin, payload: &[u8]) -> Result<()> {
    let length = u32::try_from(payload.len()).context("Pony invocation frame is too large")?;
    writer.write_all(&length.to_be_bytes()).await?;
    writer.write_all(payload).await?;
    writer.flush().await?;
    return Ok(());
}

async fn read_frame(reader: &mut ChildStdout) -> Result<Vec<u8>> {
    let mut prefix = [0_u8; 4];
    reader.read_exact(&mut prefix).await?;
    let length = u32::from_be_bytes(prefix) as usize;
    if length > MAX_FRAME_BYTES {
        bail!("Pony response frame exceeded {MAX_FRAME_BYTES} bytes");
    }
    let mut payload = vec![0_u8; length];
    reader.read_exact(&mut payload).await?;
    return Ok(payload);
}

async fn cell_limit_reached(pool: &Arc<CellPool>, max_invocations: u64) -> bool {
    for handle in &pool.cells {
        let cell = handle.cell.lock().await;
        if cell.invocation_count >= max_invocations {
            return true;
        }
    }
    return false;
}

async fn retire_pool(state: &AppState, key: &CellKey) -> bool {
    let pool = state.pools.lock().await.remove(key);
    if let Some(pool) = pool {
        terminate_pool(&pool).await;
        return true;
    }
    return false;
}

async fn terminate_pool(pool: &Arc<CellPool>) {
    for handle in &pool.cells {
        let mut cell = handle.cell.lock().await;
        let _ = cell.child.kill().await;
        let _ = cell.child.wait().await;
    }
}

async fn terminate_all_pools(state: &AppState) {
    let pools = {
        let mut pools = state.pools.lock().await;
        pools.drain().map(|(_, pool)| pool).collect::<Vec<_>>()
    };
    for pool in pools {
        terminate_pool(&pool).await;
    }
}

fn artifact_path(root: &Path, key: &CellKey, filename: &str) -> Result<PathBuf> {
    validate_path_component(&key.tenant_id)?;
    validate_path_component(&key.deployment_id)?;
    return Ok(root
        .join(&key.tenant_id)
        .join(&key.deployment_id)
        .join(filename));
}

fn validate_path_component(value: &str) -> Result<()> {
    let valid = !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        && value != "."
        && value != "..";
    if !valid {
        bail!("invalid artifact path component");
    }
    return Ok(());
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
    if validate_path_component(value).is_ok() {
        return Ok(());
    }
    return Err((StatusCode::BAD_REQUEST, format!("invalid {name}")));
}

fn internal_error(error: impl std::fmt::Display) -> (StatusCode, String) {
    return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
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
    fn artifact_path_is_generation_scoped() {
        let key = CellKey {
            tenant_id: "tenant-a".to_owned(),
            deployment_id: "generation-17".to_owned(),
        };
        let path = artifact_path(Path::new("/tmp/artifacts"), &key, "lambda");
        assert!(path.is_ok());
        if let Ok(path) = path {
            assert!(path.ends_with("tenant-a/generation-17/lambda"));
        }
    }
}
