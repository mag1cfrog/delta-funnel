//! Private boundary for stability-sensitive `delta_kernel` APIs.

use std::sync::Arc;

use delta_kernel::{Engine, Snapshot, SnapshotRef, try_parse_uri};
use delta_kernel_default_engine::{DefaultEngineBuilder, storage::store_from_url_opts};
use object_store::ObjectStore;
use url::Url;

use crate::DeltaStorageOptions;

pub(crate) fn parse_uri(table_uri: &str) -> delta_kernel::DeltaResult<Url> {
    try_parse_uri(table_uri)
}

/// One parsed table location, object store, and Kernel engine.
pub(crate) struct DeltaKernelEngineContext {
    table_url: Url,
    object_store: Arc<dyn ObjectStore>,
    engine: Arc<dyn Engine + Send + Sync>,
}

impl DeltaKernelEngineContext {
    pub(crate) fn build(
        table_url: Url,
        storage_options: &DeltaStorageOptions,
    ) -> delta_kernel::DeltaResult<Self> {
        let object_store = store_from_url_opts(
            &table_url,
            storage_options
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        )?;
        let engine = Arc::new(DefaultEngineBuilder::new(Arc::clone(&object_store)).build());

        Ok(Self {
            table_url,
            object_store,
            engine,
        })
    }

    pub(crate) fn table_url(&self) -> &Url {
        &self.table_url
    }

    #[allow(dead_code)]
    pub(crate) fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.object_store)
    }

    pub(crate) fn load_snapshot(
        &self,
        version: Option<u64>,
    ) -> delta_kernel::DeltaResult<KernelSnapshot> {
        let mut builder = Snapshot::builder_for(self.table_url.clone());
        if let Some(version) = version {
            builder = builder.at_version(version);
        }
        builder.build(self.engine.as_ref()).map(KernelSnapshot)
    }
}

#[derive(Clone)]
pub(crate) struct KernelSnapshot(SnapshotRef);

impl KernelSnapshot {
    pub(crate) fn version(&self) -> u64 {
        self.0.version()
    }
}

#[cfg(test)]
pub(crate) fn is_kernel_error(error: &(dyn std::error::Error + 'static)) -> bool {
    error.downcast_ref::<delta_kernel::Error>().is_some()
}
