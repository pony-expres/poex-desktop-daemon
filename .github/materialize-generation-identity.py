from pathlib import Path

cells_path = Path("src/cells.rs")
text = cells_path.read_text()


def replace_once(old: str, new: str, label: str) -> None:
    global text
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected one match, found {count}")
    text = text.replace(old, new, 1)


replace_once(
    "use serde_json::{Value, json};\nuse std::{\n    collections::HashMap,\n    fs,\n    path::{Path, PathBuf},",
    "use serde_json::{Value, json};\nuse sha2::{Digest, Sha256};\nuse std::{\n    collections::HashMap,\n    fs,\n    io::Read,\n    path::{Path, PathBuf},",
    "imports",
)
replace_once(
    '''struct CellKey {
    tenant_id: String,
    deployment_id: String,
}

struct Cell {''',
    '''struct CellKey {
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

struct Cell {''',
    "worker resolution",
)
replace_once(
    '''struct Cell {
    key: CellKey,
    index: u32,''',
    '''struct Cell {
    key: CellKey,
    generation_sha256: Option<String>,
    generation_source: &'static str,
    reusable_generation: bool,
    index: u32,''',
    "cell generation fields",
)
replace_once(
    '''pub struct CellStatus {
    pub tenant_id: String,
    pub deployment_id: String,
    pub cell_index: u32,''',
    '''pub struct CellStatus {
    pub tenant_id: String,
    pub deployment_id: String,
    pub generation_sha256: Option<String>,
    pub generation_source: &'static str,
    pub reusable_generation: bool,
    pub cell_index: u32,''',
    "status generation fields",
)
replace_once(
    '''                tenant_id: cell.key.tenant_id.clone(),
                deployment_id: cell.key.deployment_id.clone(),
                cell_index: cell.index,''',
    '''                tenant_id: cell.key.tenant_id.clone(),
                deployment_id: cell.key.deployment_id.clone(),
                generation_sha256: cell.generation_sha256.clone(),
                generation_source: cell.generation_source,
                reusable_generation: cell.reusable_generation,
                cell_index: cell.index,''',
    "status values",
)
replace_once(
    '''        if result.is_err() {
            cell.failed.store(true, Ordering::Release);
            let _ = self
                .retire(&key.tenant_id, &key.deployment_id, cell.index)
                .await;
        } else if cell.invocation_count.load(Ordering::Relaxed)
            >= self.inner.config.max_cell_invocations
        {
            cell.draining.store(true, Ordering::Release);
        }
''',
    '''        if result.is_err() {
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
''',
    "fresh-only fallback retirement",
)
old_lease = '''    async fn lease_cell(&self, key: &CellKey) -> Result<(Arc<Cell>, OwnedSemaphorePermit)> {
        let mut cells = self.inner.cells.lock().await;
        let group = cells.entry(key.clone()).or_default();
        group.retain(|cell| !cell.failed.load(Ordering::Acquire));

        for cell in group.iter() {
            if cell.draining.load(Ordering::Acquire) {
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

        if group.len() >= self.inner.config.max_cells_per_generation {
            bail!("all warm Pony cells for this tenant generation are busy");
        }

        let index = next_cell_index(group);
        let cell = self.spawn_cell(key.clone(), index)?;
        let permit = cell
            .capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| anyhow!("new Pony cell had no invocation capacity"))?;
        group.push(cell.clone());
        return Ok((cell, permit));
    }

    fn spawn_cell(&self, key: CellKey, index: u32) -> Result<Arc<Cell>> {
        let live_permit = self
            .inner
            .cell_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| anyhow!("live Pony cell limit reached"))?;
        let artifact = artifact_path(&self.inner.config.artifact_root, &key);
        let (command, args) = resolve_worker(&artifact, &self.inner.config)?;

        let mut child = Command::new(&command)
            .args(args)
            .env("POEX_TENANT_ID", &key.tenant_id)
            .env("POEX_DEPLOYMENT_ID", &key.deployment_id)
            .env("POEX_CELL_INDEX", index.to_string())
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("failed to start Pony cell {}", command.display()))?;

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
'''
new_lease = '''    async fn lease_cell(&self, key: &CellKey) -> Result<(Arc<Cell>, OwnedSemaphorePermit)> {
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
            .with_context(|| {
                format!("failed to start Pony cell {}", verified.command.display())
            })?;

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
'''
replace_once(old_lease, new_lease, "lease and spawn generation identity")
old_resolve = '''fn resolve_worker(artifact: &Path, config: &CellPoolConfig) -> Result<(PathBuf, Vec<String>)> {
    match fs::symlink_metadata(artifact) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                bail!("Pony deployment artifact must be a regular non-symlink file");
            }
            return Ok((artifact.to_path_buf(), Vec::new()));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((
                PathBuf::from(&config.worker_command),
                config.worker_args.clone(),
            ));
        }
        Err(error) => return Err(error.into()),
    }
}
'''
new_resolve = '''fn resolve_worker(artifact: &Path, config: &CellPoolConfig) -> Result<WorkerResolution> {
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
                generation_sha256: Some(generation_sha256_file(
                    &configured,
                    &config.worker_args,
                )?),
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
    hasher.update(b"poex-worker-generation/v1\\0");
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    hasher.update(b"\\0args\\0");
    for arg in args {
        let bytes = arg.as_bytes();
        hasher.update(u64::try_from(bytes.len())?.to_be_bytes());
        hasher.update(bytes);
    }
    return Ok(format!("{:x}", hasher.finalize()));
}
'''
replace_once(old_resolve, new_resolve, "worker resolution")
replace_once(
    '''    #[test]
    fn response_frame_extracts_payload() {''',
    '''    #[test]
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
    fn response_frame_extracts_payload() {''',
    "generation digest test",
)
cells_path.write_text(text)

flags_path = Path(".cli-flags.toml")
flags = flags_path.read_text()
old_help = 'help = "Development fallback Pony worker. Immutable tenant deployment artifacts are preferred when present."'
new_help = 'help = "Development fallback Pony worker. Deployment artifacts are preferred; explicit regular-file fallbacks may be warm-reused by digest, while PATH-only fallbacks are fresh-cell only."'
if flags.count(old_help) != 1:
    raise SystemExit("worker help did not match exactly once")
flags_path.write_text(flags.replace(old_help, new_help, 1))
