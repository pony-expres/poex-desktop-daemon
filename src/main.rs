mod cells;

use anyhow::{Context as _, Result, anyhow, bail};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path as AxumPath, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use cells::{CellPool, CellPoolConfig, CellStatus};
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
    time::Duration,
};
use tokio::sync::Semaphore;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const MAX_TIMEOUT_MS: u64 = 20 * 60 * 1_000;
const MAX_BODY_BYTES: usize = 256 * 1024;
const MAX_PARALLELISM: usize = 4096;
const MAX_LIVE_CELLS: usize = 4096;
const MAX_CELLS_PER_GENERATION: usize = 64;
const MAX_CELL_CONCURRENCY: usize = 1024;
const MAX_CELL_INVOCATIONS: u64 = 10_000_000;
const MAX_CELL_IDLE_TTL_MS: u64 = 24 * 60 * 60 * 1_000;
const MAX_CELL_AGE_MS: u64 = 7 * 24 * 60 * 60 * 1_000;
const MAX_TOKEN_FILE_BYTES: u64 = 4096;

#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
struct CliConfig {
    POEX_DESKTOP_ADDR: String,
    POEX_WORKER_COMMAND: String,
    POEX_WORKER_ARGS_JSON: String,
    POEX_ARTIFACT_ROOT: Option<String>,
    POEX_MAX_PARALLEL_INVOCATIONS: i64,
    POEX_MAX_LIVE_CELLS: i64,
    POEX_MAX_CELLS_PER_GENERATION: i64,
    POEX_MAX_CELL_CONCURRENCY: i64,
    POEX_MAX_CELL_INVOCATIONS: i64,
    POEX_CELL_IDLE_TTL_MS: i64,
    POEX_MAX_CELL_AGE_MS: i64,
    POEX_DESKTOP_TOKEN_FILE: Option<String>,
    POEX_DESKTOP_LOG: String,
}

#[derive(Debug)]
struct RuntimeConfig {
    addr: SocketAddr,
    worker_command: String,
    worker_args: Vec<String>,
    artifact_root: PathBuf,
    parallelism: usize,
    max_live_cells: usize,
    max_cells_per_generation: usize,
    max_cell_concurrency: usize,
    max_cell_invocations: u64,
    cell_idle_ttl: Duration,
    max_cell_age: Duration,
    token_path: PathBuf,
    log_filter: String,
}

#[derive(Clone)]
struct AppState {
    token: Arc<str>,
    permits: Arc<Semaphore>,
    cells: CellPool,
    started_at: std::time::Instant,
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
    worker_mode: &'static str,
    cell_reuse_scope: &'static str,
    security_boundary: &'static str,
    request_protocol: &'static str,
    config_source: &'static str,
    uptime_ms: u128,
    accepted: u64,
    completed: u64,
    failed: u64,
    live_cells: usize,
    available_invocation_slots: usize,
    available_cell_slots: usize,
    max_cells_per_generation: usize,
    max_cell_concurrency: usize,
    max_cell_invocations: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = load_config()?;
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new(&config.log_filter).context("invalid tracing filter")?)
        .init();

    let token = load_or_create_token(&config.token_path)?;
    let cells = CellPool::new(CellPoolConfig {
        worker_command: config.worker_command,
        worker_args: config.worker_args,
        artifact_root: config.artifact_root,
        max_live_cells: config.max_live_cells,
        max_cells_per_generation: config.max_cells_per_generation,
        max_cell_concurrency: config.max_cell_concurrency,
        max_cell_invocations: config.max_cell_invocations,
        cell_idle_ttl: config.cell_idle_ttl,
        max_cell_age: config.max_cell_age,
    });
    cells.start_reaper();

    let state = AppState {
        token: Arc::from(token),
        permits: Arc::new(Semaphore::new(config.parallelism)),
        cells,
        started_at: std::time::Instant::now(),
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
            "/v1/cells/{tenant_id}/{deployment_id}/{cell_index}/drain",
            post(drain_cell_route),
        )
        .route(
            "/v1/cells/{tenant_id}/{deployment_id}/{cell_index}/retire",
            post(retire_cell_route),
        )
        .route("/v1/invoke", post(invoke))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(config.addr).await?;
    tracing::info!(addr = %config.addr, "pony desktop daemon listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    state.cells.shutdown().await;
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

    let artifact_root = match raw_config.POEX_ARTIFACT_ROOT {
        Some(path) if !path.trim().is_empty() => expand_home(Path::new(&path))?,
        _ => default_artifact_root()?,
    };
    let parallelism = bounded_usize(
        "POEX_MAX_PARALLEL_INVOCATIONS",
        raw_config.POEX_MAX_PARALLEL_INVOCATIONS,
        MAX_PARALLELISM,
    )?;
    let max_live_cells = bounded_usize(
        "POEX_MAX_LIVE_CELLS",
        raw_config.POEX_MAX_LIVE_CELLS,
        MAX_LIVE_CELLS,
    )?;
    let max_cells_per_generation = bounded_usize(
        "POEX_MAX_CELLS_PER_GENERATION",
        raw_config.POEX_MAX_CELLS_PER_GENERATION,
        MAX_CELLS_PER_GENERATION,
    )?;
    let max_cell_concurrency = bounded_usize(
        "POEX_MAX_CELL_CONCURRENCY",
        raw_config.POEX_MAX_CELL_CONCURRENCY,
        MAX_CELL_CONCURRENCY,
    )?;
    let max_cell_invocations = bounded_u64(
        "POEX_MAX_CELL_INVOCATIONS",
        raw_config.POEX_MAX_CELL_INVOCATIONS,
        MAX_CELL_INVOCATIONS,
    )?;
    let cell_idle_ttl_ms = bounded_u64(
        "POEX_CELL_IDLE_TTL_MS",
        raw_config.POEX_CELL_IDLE_TTL_MS,
        MAX_CELL_IDLE_TTL_MS,
    )?;
    let max_cell_age_ms = bounded_u64(
        "POEX_MAX_CELL_AGE_MS",
        raw_config.POEX_MAX_CELL_AGE_MS,
        MAX_CELL_AGE_MS,
    )?;
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
        max_live_cells,
        max_cells_per_generation,
        max_cell_concurrency,
        max_cell_invocations,
        cell_idle_ttl: Duration::from_millis(cell_idle_ttl_ms),
        max_cell_age: Duration::from_millis(max_cell_age_ms),
        token_path,
        log_filter: raw_config.POEX_DESKTOP_LOG,
    });
}

fn bounded_usize(name: &str, value: i64, maximum: usize) -> Result<usize> {
    let value = usize::try_from(value)
        .ok()
        .filter(|value| *value > 0 && *value <= maximum)
        .ok_or_else(|| anyhow!("{name} must be between 1 and {maximum}"))?;
    return Ok(value);
}

fn bounded_u64(name: &str, value: i64, maximum: u64) -> Result<u64> {
    let value = u64::try_from(value)
        .ok()
        .filter(|value| *value > 0 && *value <= maximum)
        .ok_or_else(|| anyhow!("{name} must be between 1 and {maximum}"))?;
    return Ok(value);
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
        worker_mode: "warm_tenant_generation_cells",
        cell_reuse_scope: "same_tenant_generation",
        security_boundary: "os_process",
        request_protocol: "u32be_length_prefixed_json_v1",
        config_source: "flags-2-env",
        uptime_ms: state.started_at.elapsed().as_millis(),
        accepted: state.accepted.load(Ordering::Relaxed),
        completed: state.completed.load(Ordering::Relaxed),
        failed: state.failed.load(Ordering::Relaxed),
        live_cells: state.cells.live_cells().await,
        available_invocation_slots: state.permits.available_permits(),
        available_cell_slots: state.cells.available_cell_slots(),
        max_cells_per_generation: state.cells.max_cells_per_generation(),
        max_cell_concurrency: state.cells.max_cell_concurrency(),
        max_cell_invocations: state.cells.max_cell_invocations(),
    }));
}

async fn list_cells(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<CellStatus>>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    return Ok(Json(state.cells.statuses().await));
}

async fn drain_cell_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath((tenant_id, deployment_id, cell_index)): AxumPath<(String, String, u32)>,
) -> Result<Json<Value>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    validate_identifier("tenant_id", &tenant_id)?;
    validate_identifier("deployment_id", &deployment_id)?;
    let draining = state
        .cells
        .drain(&tenant_id, &deployment_id, cell_index)
        .await;
    return Ok(Json(json!({ "draining": draining })));
}

async fn retire_cell_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath((tenant_id, deployment_id, cell_index)): AxumPath<(String, String, u32)>,
) -> Result<Json<Value>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    validate_identifier("tenant_id", &tenant_id)?;
    validate_identifier("deployment_id", &deployment_id)?;
    let retired = state
        .cells
        .retire(&tenant_id, &deployment_id, cell_index)
        .await;
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

    let _permit = state.permits.clone().try_acquire_owned().map_err(|_| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "Pony invocation capacity is exhausted".to_owned(),
        )
    })?;
    state.accepted.fetch_add(1, Ordering::Relaxed);

    let result = state
        .cells
        .invoke(
            &request.tenant_id,
            &request.deployment_id,
            &request.invocation_id,
            &request.payload_json,
            Duration::from_millis(timeout_ms),
        )
        .await;
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

fn authorize(headers: &HeaderMap, state: &AppState) -> Result<(), (StatusCode, String)> {
    let provided = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));

    if provided.is_some_and(|token| constant_time_eq(token.as_bytes(), state.token.as_bytes())) {
        return Ok(());
    }

    return Err((StatusCode::UNAUTHORIZED, "unauthorized".to_owned()));
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let max_len = left.len().max(right.len());
    let mut difference = left.len() ^ right.len();
    for index in 0..max_len {
        difference |= usize::from(
            left.get(index).copied().unwrap_or_default()
                ^ right.get(index).copied().unwrap_or_default(),
        );
    }
    return difference == 0;
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

fn default_artifact_root() -> Result<PathBuf> {
    let home = env::var_os("HOME")
        .or_else(|| env::var_os("USERPROFILE"))
        .ok_or_else(|| anyhow!("HOME or USERPROFILE is required"))?;
    return Ok(PathBuf::from(home).join(".pony-expres/artifacts"));
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
    fn numeric_limits_are_bounded() {
        assert_eq!(bounded_usize("x", 4, 8).ok(), Some(4));
        assert!(bounded_usize("x", 0, 8).is_err());
        assert!(bounded_usize("x", 9, 8).is_err());
        assert_eq!(bounded_u64("x", 4, 8).ok(), Some(4));
    }
}

// rustfmt EOF sentinel

#[cfg(test)]
mod constant_time_auth_tests {
    use super::constant_time_eq;

    #[test]
    fn constant_time_token_comparison_matches_only_exact_bytes() {
        assert!(constant_time_eq(
            b"abcdefghijklmnopqrstuvwxyz012345",
            b"abcdefghijklmnopqrstuvwxyz012345"
        ));
        assert!(!constant_time_eq(
            b"abcdefghijklmnopqrstuvwxyz012345",
            b"abcdefghijklmnopqrstuvwxyz012346"
        ));
        assert!(!constant_time_eq(b"short", b"shorter"));
        assert!(!constant_time_eq(b"longer", b"long"));
    }
}
