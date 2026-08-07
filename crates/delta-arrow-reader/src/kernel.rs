//! Private boundary for stability-sensitive `delta_kernel` APIs.

use std::sync::Arc;

use arrow::{datatypes::SchemaRef, error::ArrowError};
use delta_kernel::{
    Engine, Snapshot, SnapshotRef,
    engine::arrow_conversion::TryIntoArrow,
    table_features::{TABLE_FEATURES_MIN_READER_VERSION, TableFeature},
    try_parse_uri,
};
use delta_kernel_default_engine::{DefaultEngineBuilder, storage::store_from_url_opts};
use object_store::ObjectStore;
use url::Url;

use crate::DeltaStorageOptions;

#[allow(dead_code)]
pub(crate) const TABLE_FEATURES_READER_VERSION: i32 = TABLE_FEATURES_MIN_READER_VERSION;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeltaKernelProtocol {
    pub(crate) min_reader_version: i32,
    pub(crate) min_writer_version: i32,
    pub(crate) reader_features: Vec<String>,
    pub(crate) writer_features: Vec<String>,
}

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

pub(crate) fn snapshot_protocol_report(snapshot: &KernelSnapshot) -> DeltaKernelProtocol {
    let protocol = snapshot.0.table_configuration().protocol();

    DeltaKernelProtocol {
        min_reader_version: protocol.min_reader_version(),
        min_writer_version: protocol.min_writer_version(),
        reader_features: feature_names(protocol.reader_features()),
        writer_features: feature_names(protocol.writer_features()),
    }
}

pub(crate) fn snapshot_arrow_schema(snapshot: &KernelSnapshot) -> Result<SchemaRef, ArrowError> {
    snapshot.0.schema().as_ref().try_into_arrow().map(Arc::new)
}

fn feature_names(features: Option<&[TableFeature]>) -> Vec<String> {
    features
        .unwrap_or_default()
        .iter()
        .map(feature_name)
        .collect()
}

fn feature_name(feature: &TableFeature) -> String {
    match feature {
        TableFeature::Unknown(name) => name.clone(),
        _ => feature.as_ref().to_owned(),
    }
}

#[cfg(test)]
pub(crate) fn is_kernel_error(error: &(dyn std::error::Error + 'static)) -> bool {
    error.downcast_ref::<delta_kernel::Error>().is_some()
}
