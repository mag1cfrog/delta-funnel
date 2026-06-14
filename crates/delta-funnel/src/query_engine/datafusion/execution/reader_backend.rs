//! Provider reader backend boundary for Delta scan execution.
//!
//! This module owns backend selection for normal provider execution. Backends
//! are responsible for file-level Delta correctness: reading a planned file
//! task, applying physical-to-logical transforms, applying deletion-vector
//! masks, and returning file-local deletion-vector stats before batches reach
//! DataFusion stream handoff.

use std::sync::Arc;

use crate::DeltaFunnelError;

use super::file_reader::{
    DeltaFileReadRequest, DeltaFileReadResult, DeltaFileReader, DeltaFileReaderConfig,
};
use super::scheduling::DeltaProviderReaderBackend;

/// Context required to construct a provider file reader backend.
pub(crate) struct DeltaProviderReaderBackendConfig<'a> {
    /// Selected backend identity from provider execution options.
    pub(crate) reader_backend: DeltaProviderReaderBackend,
    /// DataFusion table name for diagnostics.
    pub(crate) source_name: &'a str,
    /// Normalized Delta table URI used to resolve table-relative file paths.
    pub(crate) table_uri: &'a str,
    /// Snapshot version that selected the file tasks.
    pub(crate) snapshot_version: u64,
}

/// File reader used by one DataFusion execution partition.
pub(crate) trait DeltaScanPartitionFileReader: Send + Sync {
    /// Reads one planned Delta file task into logical Arrow batches.
    fn read_file(
        &self,
        request: DeltaFileReadRequest<'_>,
    ) -> Result<DeltaFileReadResult, DeltaFunnelError>;
}

/// Builds the selected provider reader backend for normal scan execution.
pub(crate) fn build_partition_file_reader(
    config: DeltaProviderReaderBackendConfig<'_>,
) -> Result<Arc<dyn DeltaScanPartitionFileReader>, DeltaFunnelError> {
    match config.reader_backend {
        DeltaProviderReaderBackend::OfficialKernel => {
            let reader = DeltaFileReader::try_new(DeltaFileReaderConfig {
                source_name: config.source_name,
                table_uri: config.table_uri,
                snapshot_version: config.snapshot_version,
            })?;
            Ok(Arc::new(reader))
        }
        DeltaProviderReaderBackend::NativeAsync => Err(DeltaFunnelError::Config {
            message:
                "native async reader backend is not wired into sync partition reader execution"
                    .to_owned(),
        }),
    }
}

impl DeltaScanPartitionFileReader for DeltaFileReader {
    fn read_file(
        &self,
        request: DeltaFileReadRequest<'_>,
    ) -> Result<DeltaFileReadResult, DeltaFunnelError> {
        Self::read_file(self, request)
    }
}
