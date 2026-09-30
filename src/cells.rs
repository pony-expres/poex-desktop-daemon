use anyhow::{Context as _, Result, anyhow, bail};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fs,
    io::Read,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, Command},
    sync::{Mutex, OwnedSemaphorePermit, Semaphore, oneshot},
    time::{interval, timeout},
};

const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;
const CLEANUP_INTERVAL: Duration = Duration::from_secs(30);

type WorkerResult = std::result::Result<Value, String>;
type PendingMap = HashMap<String, oneshot::Sender<WorkerResult>>;

#[derive(Clone, Debug)]
pub struct CellPoolConfig {
    pub worker_command: String,
    pub worker_args: Vec<String>,
    pub artifact_root: PathBuf,
    pub max_live_cells: usize,
    pub max_cells_per_generation: usize,
    pub max_cell_concurrency: usize,
    pub max_cell_invocations: u64,
    pub cell_idle_ttl: Duration,
    pub max_cell_age: Duration,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CellKey {
    tenant_id: String,
    deployment_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WorkerResolution {
    command: PathBuf,
    args: Vec<String>,
    generation_sha256: Option<String>,
    generation_source: &'static str,
    reusable: bool,
}

struct Cell {
    key: CellKey,
    generation_sha256: Option<String>,
    generation_source: &'static str,
    reusable_generation: bool,
    index: u32,
    child: Mutex<Child>,
    stdin: Mutex<ChildStdin>,
    pending: Arc<Mutex<PendingMap>>,
    capacity: Arc<Semaphore>,
    invocation_count: AtomicU64,
    active: AtomicU64,
    failed: Arc<AtomicBool>,
    draining: AtomicBool,
    started_at: Instant,
    last_used: Mutex<Instant>,
    _live_permit: OwnedSemaphorePermit,
}

struct Inner {
    config: CellPoolConfig,
    cell_slots: Arc<Semaphore>,
    cells: Mutex<HashMap<CellKey, Vec<Arc<Cell>>>>,
}

#[derive(Clone)]
pub struct CellPool {
    inner: Arc<Inner>,
}

#[derive(Debug, Serialize)]
pub struct CellStatus {
    pub tenant_id: String,
    pub deployment_id: String,
    pub generation_sha256: Option<String>,
    pub generation_source: &'static str,
    pub reusable_generation: bool,
    pub cell_index: u32,
    pub invocation_count: u64,
    pub active_invocations: u64,
    pub available_invocation_slots: usize,
    pub draining: bool,
    pub failed: bool,
    pub age_ms: u128,
    pub idle_ms: u128,
}

impl CellPool {
    pub fn new(config: CellPoolConfig) -> Self {
        let max_live_cells = config.max_live_cells;
        return Self {
            inner: Arc::new(Inner {
                config,
                cell_slots: Arc::new(Semaphore::new(max_live_cells)),
                cells: Mutex::new(HashMap::new()),
            }),
        };
    }

    pub fn start_reaper(&self) {
        let pool = self.clone();
        tokio::spawn(async move {
            pool.reaper_loop().await;
        });
    }

    pub fn max_cells_per_generation(&self) -> usize {
        return self.inner.config.max_cells_per_generation;
    }

    pub fn max_cell_concurrency(&self) -> usize {
        return self.inner.config.max_cell_concurrency;
    }

    pub fn max_cell_invocations(&self) -> u64 {
        return self.inner.config.max_cell_invocations;
    }

    pub fn available_cell_slots(&self) -> usize {
        return self.inner.cell_slots.available_permits();
    }

    pub async fn live_cells(&self) -> usize {
        return self.inner.cells.lock().await.values().map(Vec::len).sum();
    }

    pub async fn statuses(&self) -> Vec<CellStatus> {
        let cells = self
            .inner
            .cells
            .lock()
            .await
            .values()
            .flatten()
            .cloned()
            .collect::<Vec<_>>();
        let mut statuses = Vec::with_capacity(cells.len());
        for cell in cells {
            let idle_ms = cell.last_used.lock().await.elapsed().as_millis();
            statuses.push(CellStatus {
                tenant_id: cell.key.tenant_id.clone(),
                deployment_id: cell.key.deployment_id.clone(),
                generation_sha256: cell.generation_sha256.clone(),
                generation_source: cell.generation_source,
                reusable_generation: cell.reusable_generation,
                cell_index: cell.index,
                invocation_count: cell.invocation_count.load(Ordering::Relaxed),
                active_invocations: cell.active.load(Ordering::Relaxed),
                available_invocation_slots: cell.capacity.available_permits(),
                draining: cell.draining.load(Ordering::Acquire),
                failed: cell.failed.load(Ordering::Acquire),
                age_ms: cell.started_at.elapsed().as_millis(),
                idle_ms,
            });
        }
        return statuses;
    }

    pub async fn invoke(
        &self,
        tenant_id: &str,
        deployment_id: &str,
        invocation_id: &str,
        payload: &Value,
        deadline: Duration,
    ) -> Result<Value> {
        let key = CellKey {
            tenant_id: tenant_id.to_owned(),
            deployment_id: deployment_id.to_owned(),
        };
        let (cell, _permit) = self.lease_cell(&key).await?;
        let result = invoke_cell(&cell, invocation_id, payload, deadline).await;

        if result.is_err() {
            cell.failed.store(true, Ordering::Release);
            let _ = self
                .retire(&key.tenant_id, &key.deployment_id, cell.index)
                .await;
        } else if !cell.reusable_generation
            || cell.invocation_count.load(Ordering::Relaxed)
                >= self.inner.config.max_cell_invocations
        {
            cell.draining.store(true, Ordering::Release);
        }

        if cell.draining.load(Ordering::Acquire) && cell.active.load(Ordering::Acquire) == 0 {
            let _ = self
                .retire(&key.tenant_id, &key.deployment_id, cell.index)
                .await;
        }

        return result;
    }

    pub async fn drain(&self, tenant_id: &str, deployment_id: &str, index: u32) -> bool {
        let key = CellKey {
            tenant_id: tenant_id.to_owned(),
            deployment_id: deployment_id.to_owned(),
        };
        let cell = {
            let cells = self.inner.cells.lock().await;
            cells
                .get(&key)
                .and_then(|group| group.iter().find(|cell| cell.index == index))
                .cloned()
        };
        let Some(cell) = cell else {
            return false;
        };
        cell.draining.store(true, Ordering::Release);
        if cell.active.load(Ordering::Acquire) == 0 {
            return self.retire(tenant_id, deployment_id, index).await;
        }
        return true;
    }

    pub async fn retire(&self, tenant_id: &str, deployment_id: &str, index: u32) -> bool {
        let key = CellKey {
            tenant_id: tenant_id.to_owned(),
            deployment_id: deployment_id.to_owned(),
        };
        let cell = {
            let mut cells = self.inner.cells.lock().await;
            let Some(group) = cells.get_mut(&key) else {
                return false;
            };
            let Some(position) = group.iter().position(|cell| cell.index == index) else {
                return false;
            };
            let cell = group.remove(position);
            if group.is_empty() {
                cells.remove(&key);
            }
            cell
        };

        cell.draining.store(true, Ordering::Release);
        fail_all_pending(&cell.pending, "Pony execution cell retired").await;
        let mut child = cell.child.lock().await;
        let _ = child.kill().await;
        let _ = child.wait().await;
        return true;
    }

    pub async fn shutdown(&self) {
        let cells = {
            let mut guard = self.inner.cells.lock().await;
            let cells = guard.values().flatten().cloned().collect::<Vec<_>>();
            guard.clear();
            cells
        };
        for cell in cells {
            cell.draining.store(true, Ordering::Release);
            fail_all_pending(&cell.pending, "Pony desktop daemon shutting down").await;
            let mut child = cell.child.lock().await;
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
    }

    async fn lease_cell(&self, key: &CellKey) -> Result<(Arc<Cell>, OwnedSemaphorePermit)> {
        let artifact = artifact_path(&self.inner.config.artifact_root, key);
        let resolution = resolve_worker(&artifact, &self.inner.config)?;
        let mut cells = self.inner.cells.lock().await;
        let group = cells.entry(key.clone()).or_default();
        group.retain(|cell| !cell.failed.load(Ordering::Acquire));

        for cell in group.iter() {
            if cell.reusable_generation
                && (!resolution.reusable || cell.generation_sha256 != resolution.generation_sha256)
            {
                cell.draining.store(true, Ordering::Release);
            }
        }

        if resolution.reusable {
            for cell in group.iter() {
                if cell.draining.load(Ordering::Acquire)
                    || !cell.reusable_generation
                    || cell.generation_sha256 != resolution.generation_sha256
                {
                    continue;
                }
                if cell.invocation_count.load(Ordering::Relaxed)
                    >= self.inner.config.max_cell_invocations
                {
                    cell.draining.store(true, Ordering::Release);
                    continue;
                }
                if let Ok(permit) = cell.capacity.clone().try_acquire_owned() {
                    return Ok((cell.clone(), permit));
                }
            }
        }

        let current_generation_cells = if resolution.reusable {
            group
                .iter()
                .filter(|cell| {
                    !cell.failed.load(Ordering::Acquire)
                        && cell.reusable_generation
                        && cell.generation_sha256 == resolution.generation_sha256
                })
                .count()
        } else {
            group
                .iter()
                .filter(|cell| {
                    !cell.failed.load(Ordering::Acquire)
                        && !cell.reusable_generation
                        && !cell.draining.load(Ordering::Acquire)
                })
                .count()
        };
        if current_generation_cells >= self.inner.config.max_cells_per_generation {
            bail!("all Pony cells for this immutable generation are busy");
        }

        let index = next_cell_index(group);
        let cell = self.spawn_cell(key.clone(), index, &resolution)?;
        let permit = cell
            .capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| anyhow!("new Pony cell had no invocation capacity"))?;
        group.push(cell.clone());
        return Ok((cell, permit));
    }

    fn spawn_cell(
        &self,
        key: CellKey,
        index: u32,
        resolution: &WorkerResolution,
    ) -> Result<Arc<Cell>> {
        let live_permit = self
            .inner
            .cell_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| anyhow!("live Pony cell limit reached"))?;
        let artifact = artifact_path(&self.inner.config.artifact_root, &key);
        let verified = resolve_worker(&artifact, &self.inner.config)?;
        if verified != *resolution {
            bail!("Pony worker generation changed while preparing a warm cell");
        }

        let mut command = Command::new(&verified.command);
        command
            .args(&verified.args)
            .env("POEX_TENANT_ID", &key.tenant_id)
            .env("POEX_DEPLOYMENT_ID", &key.deployment_id)
            .env("POEX_CELL_INDEX", index.to_string());
        if let Some(generation_sha256) = verified.generation_sha256.as_deref() {
            command.env("POEX_GENERATION_SHA256", generation_sha256);
        }
        let mut child = command
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("failed to start Pony cell {}", verified.command.display()))?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("Pony cell stdin unavailable"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("Pony cell stdout unavailable"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| anyhow!("Pony cell stderr unavailable"))?;
        let pending = Arc::new(Mutex::new(PendingMap::new()));
        let failed = Arc::new(AtomicBool::new(false));

        tokio::spawn(read_worker_stdout(
            key.clone(),
            index,
            stdout,
            pending.clone(),
            failed.clone(),
        ));
        tokio::spawn(read_worker_stderr(key.clone(), index, stderr));

        return Ok(Arc::new(Cell {
            key,
            generation_sha256: verified.generation_sha256,
            generation_source: verified.generation_source,
            reusable_generation: verified.reusable,
            index,
            child: Mutex::new(child),
            stdin: Mutex::new(stdin),
            pending,
            capacity: Arc::new(Semaphore::new(self.inner.config.max_cell_concurrency)),
            invocation_count: AtomicU64::new(0),
            active: AtomicU64::new(0),
            failed,
            draining: AtomicBool::new(false),
            started_at: Instant::now(),
            last_used: Mutex::new(Instant::now()),
            _live_permit: live_permit,
        }));
    }

    async fn reaper_loop(self) {
        let mut ticker = interval(CLEANUP_INTERVAL);
        loop {
            ticker.tick().await;
            let cells = self
                .inner
                .cells
                .lock()
                .await
                .values()
                .flatten()
                .cloned()
                .collect::<Vec<_>>();
            for cell in cells {
                if cell.active.load(Ordering::Acquire) != 0 {
                    continue;
                }
                let idle = cell.last_used.lock().await.elapsed();
                let should_retire = cell.failed.load(Ordering::Acquire)
                    || cell.draining.load(Ordering::Acquire)
                    || idle >= self.inner.config.cell_idle_ttl
                    || cell.started_at.elapsed() >= self.inner.config.max_cell_age;
                if should_retire {
                    let _ = self
                        .retire(&cell.key.tenant_id, &cell.key.deployment_id, cell.index)
                        .await;
                }
            }
        }
    }
}

fn next_cell_index(cells: &[Arc<Cell>]) -> u32 {
    let mut index = 0_u32;
    loop {
        if cells.iter().all(|cell| cell.index != index) {
            return index;
        }
        index = index.saturating_add(1);
    }
}

fn artifact_path(root: &Path, key: &CellKey) -> PathBuf {
    return root
        .join(&key.tenant_id)
        .join(&key.deployment_id)
        .join("lambda");
}

fn resolve_worker(artifact: &Path, config: &CellPoolConfig) -> Result<WorkerResolution> {
    match fs::symlink_metadata(artifact) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                bail!("Pony deployment artifact must be a regular non-symlink file");
            }
            return Ok(WorkerResolution {
                command: artifact.to_path_buf(),
                args: Vec::new(),
                generation_sha256: Some(generation_sha256_file(artifact, &[])?),
                generation_source: "deployment_artifact",
                reusable: true,
            });
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    let configured = PathBuf::from(&config.worker_command);
    match fs::symlink_metadata(&configured) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Ok(WorkerResolution {
                command: configured,
                args: config.worker_args.clone(),
                generation_sha256: None,
                generation_source: "unverified_worker_command",
                reusable: false,
            });
        }
        Ok(metadata) if metadata.is_file() => {
            return Ok(WorkerResolution {
                command: configured.clone(),
                args: config.worker_args.clone(),
                generation_sha256: Some(generation_sha256_file(&configured, &config.worker_args)?),
                generation_source: "configured_worker_file",
                reusable: true,
            });
        }
        Ok(_) => bail!("configured Pony fallback worker must be a regular file"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(WorkerResolution {
                command: configured,
                args: config.worker_args.clone(),
                generation_sha256: None,
                generation_source: "unverified_worker_command",
                reusable: false,
            });
        }
        Err(error) => return Err(error.into()),
    }
}

fn generation_sha256_file(path: &Path, args: &[String]) -> Result<String> {
    let file = fs::File::open(path)
        .with_context(|| format!("cannot open Pony worker generation {}", path.display()))?;
    return generation_sha256_reader(file, args);
}

fn generation_sha256_reader<R: Read>(mut reader: R, args: &[String]) -> Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(b"poex-worker-generation/v1\0");
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    hasher.update(b"\0args\0");
    for arg in args {
        let bytes = arg.as_bytes();
        hasher.update(u64::try_from(bytes.len())?.to_be_bytes());
        hasher.update(bytes);
    }
    return Ok(format!("{:x}", hasher.finalize()));
}

async fn invoke_cell(
    cell: &Arc<Cell>,
    invocation_id: &str,
    payload: &Value,
    deadline: Duration,
) -> Result<Value> {
    if cell.failed.load(Ordering::Acquire) || cell.draining.load(Ordering::Acquire) {
        bail!("Pony cell is not accepting new invocations");
    }

    let (sender, receiver) = oneshot::channel();
    {
        let mut pending = cell.pending.lock().await;
        if pending.contains_key(invocation_id) {
            bail!("duplicate invocation_id in Pony cell");
        }
        pending.insert(invocation_id.to_owned(), sender);
    }

    cell.active.fetch_add(1, Ordering::AcqRel);
    cell.invocation_count.fetch_add(1, Ordering::Relaxed);
    *cell.last_used.lock().await = Instant::now();

    let frame = json!({
        "frame_type": "invoke",
        "invocation_id": invocation_id,
        "tenant_id": cell.key.tenant_id,
        "deployment_id": cell.key.deployment_id,
        "payload": payload,
    });
    let encoded = serde_json::to_vec(&frame)?;
    if encoded.is_empty() || encoded.len() > MAX_FRAME_BYTES {
        cell.pending.lock().await.remove(invocation_id);
        cell.active.fetch_sub(1, Ordering::AcqRel);
        bail!("Pony invocation frame exceeded limit");
    }

    let write_result = async {
        let mut stdin = cell.stdin.lock().await;
        let length = u32::try_from(encoded.len())
            .map_err(|_| anyhow!("Pony invocation frame length overflow"))?;
        stdin.write_all(&length.to_be_bytes()).await?;
        stdin.write_all(&encoded).await?;
        stdin.flush().await?;
        return Ok::<_, anyhow::Error>(());
    }
    .await;
    if let Err(error) = write_result {
        cell.pending.lock().await.remove(invocation_id);
        cell.active.fetch_sub(1, Ordering::AcqRel);
        return Err(error).context("failed to write Pony invocation frame");
    }

    let received = timeout(deadline, receiver).await;
    cell.active.fetch_sub(1, Ordering::AcqRel);
    *cell.last_used.lock().await = Instant::now();

    match received {
        Ok(Ok(Ok(payload))) => return Ok(payload),
        Ok(Ok(Err(message))) => bail!(message),
        Ok(Err(_)) => bail!("Pony cell response channel closed"),
        Err(_) => {
            cell.pending.lock().await.remove(invocation_id);
            bail!("Pony invocation timed out; cell will be retired");
        }
    }
}

async fn read_worker_stdout(
    key: CellKey,
    index: u32,
    mut stdout: tokio::process::ChildStdout,
    pending: Arc<Mutex<PendingMap>>,
    failed: Arc<AtomicBool>,
) {
    loop {
        let mut header = [0_u8; 4];
        if let Err(error) = stdout.read_exact(&mut header).await {
            failed.store(true, Ordering::Release);
            fail_all_pending(&pending, &format!("Pony cell stdout closed: {error}")).await;
            return;
        }
        let length = u32::from_be_bytes(header) as usize;
        if length == 0 || length > MAX_FRAME_BYTES {
            failed.store(true, Ordering::Release);
            fail_all_pending(&pending, "Pony cell response frame length is invalid").await;
            return;
        }
        let mut bytes = vec![0_u8; length];
        if let Err(error) = stdout.read_exact(&mut bytes).await {
            failed.store(true, Ordering::Release);
            fail_all_pending(
                &pending,
                &format!("Pony cell response was truncated: {error}"),
            )
            .await;
            return;
        }
        let frame = match serde_json::from_slice::<Value>(&bytes) {
            Ok(frame) => frame,
            Err(error) => {
                failed.store(true, Ordering::Release);
                fail_all_pending(
                    &pending,
                    &format!("Pony cell returned invalid JSON: {error}"),
                )
                .await;
                return;
            }
        };
        let invocation_id = match frame.get("invocation_id").and_then(Value::as_str) {
            Some(value) => value.to_owned(),
            None => {
                failed.store(true, Ordering::Release);
                fail_all_pending(&pending, "Pony cell response omitted invocation_id").await;
                return;
            }
        };
        if let Some(sender) = pending.lock().await.remove(&invocation_id) {
            let _ = sender.send(decode_worker_frame(frame));
        } else {
            tracing::warn!(
                tenant_id = %key.tenant_id,
                deployment_id = %key.deployment_id,
                cell_index = index,
                %invocation_id,
                "discarding response for unknown Pony invocation"
            );
        }
    }
}

async fn read_worker_stderr(key: CellKey, index: u32, stderr: tokio::process::ChildStderr) {
    let mut lines = BufReader::new(stderr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        tracing::warn!(
            tenant_id = %key.tenant_id,
            deployment_id = %key.deployment_id,
            cell_index = index,
            worker_stderr = %truncate(&line, 2048),
            "Pony cell stderr"
        );
    }
}

fn decode_worker_frame(frame: Value) -> WorkerResult {
    if frame.get("ok").and_then(Value::as_bool) == Some(false) {
        return Err(frame
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("Pony invocation failed")
            .to_owned());
    }
    return Ok(frame.get("payload").cloned().unwrap_or(Value::Null));
}

async fn fail_all_pending(pending: &Arc<Mutex<PendingMap>>, message: &str) {
    let senders = pending
        .lock()
        .await
        .drain()
        .map(|(_, sender)| sender)
        .collect::<Vec<_>>();
    for sender in senders {
        let _ = sender.send(Err(message.to_owned()));
    }
}

fn truncate(value: &str, max_chars: usize) -> String {
    return value.chars().take(max_chars).collect();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_digest_includes_fixed_worker_arguments() -> anyhow::Result<()> {
        let no_args = generation_sha256_reader(std::io::Cursor::new(b"worker"), &[])?;
        let args = vec!["--mode=test".to_owned()];
        let with_args = generation_sha256_reader(std::io::Cursor::new(b"worker"), &args)?;
        assert_eq!(no_args.len(), 64);
        assert_eq!(with_args.len(), 64);
        assert_ne!(no_args, with_args);
        return Ok(());
    }

    #[test]
    fn response_frame_extracts_payload() {
        let frame = json!({
            "invocation_id": "inv-1",
            "ok": true,
            "payload": {"hello": "world"}
        });
        let result = decode_worker_frame(frame);
        assert_eq!(result.ok(), Some(json!({"hello": "world"})));
    }

    #[test]
    fn response_frame_propagates_guest_error() {
        let frame = json!({
            "invocation_id": "inv-1",
            "ok": false,
            "error": "boom"
        });
        assert!(decode_worker_frame(frame).is_err());
    }
}

// rustfmt EOF sentinel
