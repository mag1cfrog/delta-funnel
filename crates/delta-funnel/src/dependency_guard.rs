//! Dependency policy tests for the Delta source foundation.

const CRATE_MANIFEST: &str = include_str!("../Cargo.toml");
const WORKSPACE_LOCK: &str = include_str!("../../../Cargo.lock");

#[test]
fn does_not_depend_on_alternate_delta_providers() {
    assert!(!WORKSPACE_LOCK.contains("\nname = \"deltalake\"\n"));
    assert!(!WORKSPACE_LOCK.contains("\nname = \"buoyant_kernel\"\n"));
}

#[test]
fn does_not_add_direct_object_store_dependency() {
    assert!(
        !CRATE_MANIFEST
            .lines()
            .any(|line| line.trim_start().starts_with("object_store"))
    );
}
