use ores_common_desktop_infra::runtime::{
    DesktopRuntimeAdapter, ResourcePolicy, RuntimeCapabilities, RuntimeKind, RuntimeTargetSpec,
};

struct ProductRuntimeContract;

impl DesktopRuntimeAdapter for ProductRuntimeContract {
    fn product_id(&self) -> &str {
        return "pony-expres";
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        return RuntimeCapabilities {
            runtime_kind: RuntimeKind::PonyActor,
            supports_standalone_server: true,
            supports_worker_pool: true,
            supports_actors: true,
            supports_hot_worker_reload: false,
            supports_hot_middleware_reload: false,
            supports_generation_drain: true,
        };
    }

    fn stage_target(&self, _target: &RuntimeTargetSpec) -> Result<(), String> { return Ok(()); }
    fn start_target(&self, _target: &RuntimeTargetSpec, _generation: u64) -> Result<(), String> { return Ok(()); }
    fn health_check_target(&self, _target: &RuntimeTargetSpec, _generation: u64) -> Result<(), String> { return Ok(()); }
    fn begin_drain(&self, _generation: u64) -> Result<(), String> { return Ok(()); }
    fn stop_generation(&self, _generation: u64) -> Result<(), String> { return Ok(()); }
    fn rollback_generation(&self, _from_generation: u64, _to_generation: u64) -> Result<(), String> { return Ok(()); }
}

#[test]
fn product_declares_common_runtime_contract() {
    let adapter = ProductRuntimeContract;
    let capabilities = adapter.capabilities();

    assert_eq!(adapter.product_id(), "pony-expres");
    assert_eq!(capabilities.runtime_kind, RuntimeKind::PonyActor);

    let policy = ResourcePolicy {
        memory_limit_bytes: Some(512 * 1024 * 1024),
        cpu_millis: Some(1000),
        max_concurrency: Some(32),
        request_timeout_ms: Some(30_000),
        idle_timeout_ms: Some(60_000),
        max_instances: Some(8),
        min_instances: Some(1),
        max_queue_depth: Some(256),
    };

    assert!(policy.validate().is_ok());
}
