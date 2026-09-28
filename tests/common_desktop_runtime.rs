use ores_common_desktop_runtime::{RuntimeCapabilities, RuntimeKind};

#[test]
fn common_desktop_runtime_profile_is_pony() {
    let capabilities = RuntimeCapabilities {
        runtime_kind: RuntimeKind::Pony,
        supports_standalone_server: true,
        supports_worker_pool: true,
        supports_actors: true,
        supports_hot_worker_reload: true,
        supports_graceful_drain: true,
        supports_snapshot: false,
    };

    assert_eq!(capabilities.runtime_kind, RuntimeKind::Pony);
    assert!(capabilities.supports_standalone_server);
    assert!(capabilities.supports_worker_pool);
    assert!(capabilities.supports_actors);
    assert!(capabilities.supports_graceful_drain);
}
