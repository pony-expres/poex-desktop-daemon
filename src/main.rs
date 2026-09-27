use anyhow::{Context as _, Result, anyhow, bail};
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    env,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::AsyncWriteExt,
    process::Command,
    sync::Semaphore,
    time::timeout,
};
use uuid::Uuid;

const DEFAULT_ADDR: &str = "127.0.0.1:8762";
const DEFAULT_WORKER: &str = "poex-lambda";
const MAX_TIMEOUT_MS: u64 = 20 * 60 * 1_000;

#[derive(Clone)]
struct AppState {
    token: Arc<str>,
    worker_command: Arc<str>,
    worker_args: Arc<Vec<String>>,
    permits: Arc<Semaphore>,
    started_at: Instant,
    accepted: Arc<AtomicU64>,
    completed: Arc<AtomicU64>,
}

#[derive(Debug, Deserialize)]
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
    uptime_ms: u128,
    accepted: u64,
    completed: u64,
    available_slots: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "poex_desktop_daemon=info".into()),
        )
        .init();

    let addr = parse_loopback_addr(env::var("POEX_DESKTOP_ADDR").as_deref().unwrap_or(DEFAULT_ADDR))?;
    let token_path = token_path()?;
    let token = load_or_create_token(&token_path)?;
    let worker_command = env::var("POEX_WORKER_COMMAND").unwrap_or_else(|_| DEFAULT_WORKER.to_owned());
    let worker_args = parse_args_json("POEX_WORKER_ARGS_JSON")?;
    let parallelism = positive_usize_env("POEX_MAX_PARALLEL_INVOCATIONS", 8)?;

    let state = AppState {
        token: Arc::from(token),
        worker_command: Arc::from(worker_command),
        worker_args: Arc::new(worker_args),
        permits: Arc::new(Semaphore::new(parallelism)),
        started_at: Instant::now(),
        accepted: Arc::new(AtomicU64::new(0)),
        completed: Arc::new(AtomicU64::new(0)),
    };

    let app = Router::new()
        .route("/healthz", get(health))
        .route("/v1/status", get(status))
        .route("/v1/invoke", post(invoke))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "pony desktop daemon listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    return Ok(());
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
        uptime_ms: state.started_at.elapsed().as_millis(),
        accepted: state.accepted.load(Ordering::Relaxed),
        completed: state.completed.load(Ordering::Relaxed),
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

    let permit = state
        .permits
        .clone()
        .acquire_owned()
        .await
        .map_err(internal_error)?;
    state.accepted.fetch_add(1, Ordering::Relaxed);

    let result = run_fresh_worker(&state, &request, Duration::from_millis(timeout_ms)).await;
    drop(permit);
    state.completed.fetch_add(1, Ordering::Relaxed);

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

    let payload = serde_json::to_vec(&request.payload_json)?;
    let mut stdin = child.stdin.take().ok_or_else(|| anyhow!("worker stdin unavailable"))?;
    stdin.write_all(&payload).await?;
    stdin.shutdown().await?;
    drop(stdin);

    let output = timeout(deadline, child.wait_with_output())
        .await
        .map_err(|_| anyhow!("invocation timed out"))??;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let summary = stderr.lines().next().unwrap_or("worker exited unsuccessfully");
        bail!("worker failed: {summary}");
    }

    let stdout = String::from_utf8(output.stdout).context("worker stdout was not UTF-8")?;
    let payload_json = serde_json::from_str(stdout.trim()).context("worker stdout was not JSON")?;
    return Ok(payload_json);
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
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    if valid {
        return Ok(());
    }
    return Err((StatusCode::BAD_REQUEST, format!("invalid {name}")));
}

fn parse_loopback_addr(value: &str) -> Result<SocketAddr> {
    let addr: SocketAddr = value.parse().context("POEX_DESKTOP_ADDR is not a socket address")?;
    if !is_loopback(addr.ip()) {
        bail!("POEX_DESKTOP_ADDR must bind to loopback");
    }
    return Ok(addr);
}

fn is_loopback(ip: IpAddr) -> bool {
    return ip.is_loopback();
}

fn parse_args_json(name: &str) -> Result<Vec<String>> {
    let Some(raw) = env::var(name).ok().filter(|value| !value.trim().is_empty()) else {
        return Ok(Vec::new());
    };
    let args = serde_json::from_str::<Vec<String>>(&raw)
        .with_context(|| format!("{name} must be a JSON string array"))?;
    return Ok(args);
}

fn positive_usize_env(name: &str, default_value: usize) -> Result<usize> {
    let Some(raw) = env::var(name).ok() else {
        return Ok(default_value);
    };
    let value = raw.parse::<usize>().with_context(|| format!("{name} must be an integer"))?;
    if value == 0 {
        bail!("{name} must be greater than zero");
    }
    return Ok(value);
}

fn token_path() -> Result<PathBuf> {
    if let Ok(path) = env::var("POEX_DESKTOP_TOKEN_FILE") {
        return Ok(expand_home(Path::new(&path))?);
    }
    let home = env::var_os("HOME").ok_or_else(|| anyhow!("HOME is required"))?;
    return Ok(PathBuf::from(home).join(".pony-expres/daemon/token"));
}

fn expand_home(path: &Path) -> Result<PathBuf> {
    let text = path.to_string_lossy();
    if text == "~" || text.starts_with("~/") {
        let home = env::var_os("HOME").ok_or_else(|| anyhow!("HOME is required"))?;
        let suffix = text.trim_start_matches('~').trim_start_matches('/');
        return Ok(PathBuf::from(home).join(suffix));
    }
    return Ok(path.to_path_buf());
}

fn load_or_create_token(path: &Path) -> Result<String> {
    if let Ok(token) = std::fs::read_to_string(path) {
        let token = token.trim();
        if token.len() >= 32 {
            return Ok(token.to_owned());
        }
        bail!("desktop daemon token file is too short");
    }

    let parent = path.parent().ok_or_else(|| anyhow!("token path has no parent"))?;
    std::fs::create_dir_all(parent)?;
    let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    std::fs::write(path, format!("{token}\n"))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }

    return Ok(token);
}

fn internal_error(error: impl std::fmt::Display) -> (StatusCode, String) {
    return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
