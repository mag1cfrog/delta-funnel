//! Private Delta Kernel data-file read adapter.
//!
//! This module is the official-kernel synchronous reader baseline for #4. It
//! must not be called directly from a DataFusion stream polling loop; later
//! provider execution work is responsible for placing this boundary behind
//! bounded scheduling and backpressure.

use crate::{
    DeltaFunnelError,
    error::{DeltaScanFileReadPhase, DeltaScanFileReadSnafu},
};
use snafu::ResultExt;

use super::{KernelPhysicalToLogicalTransform, kernel};

/// Kernel scan schema state required to read physical Parquet data.
#[allow(dead_code)]
pub(crate) struct KernelScanReadSchema {
    physical_schema: kernel::KernelSchemaRef,
    logical_schema: kernel::KernelSchemaRef,
    physical_predicate: Option<kernel::PredicateRef>,
}

impl KernelScanReadSchema {
    pub(super) fn new(
        physical_schema: kernel::KernelSchemaRef,
        logical_schema: kernel::KernelSchemaRef,
        physical_predicate: Option<kernel::PredicateRef>,
    ) -> Self {
        Self {
            physical_schema,
            logical_schema,
            physical_predicate,
        }
    }

    /// Physical schema requested from the kernel Parquet handler.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn physical_schema(&self) -> &kernel::KernelSchemaRef {
        &self.physical_schema
    }

    /// Logical schema selected by kernel scan planning.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn logical_schema(&self) -> &kernel::KernelSchemaRef {
        &self.logical_schema
    }

    /// Optional kernel physical predicate for later read execution.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn physical_predicate(&self) -> Option<&kernel::PredicateRef> {
        self.physical_predicate.as_ref()
    }
}

/// Context required to construct the official-kernel reader baseline.
#[allow(dead_code)]
pub(crate) struct KernelDataFileReaderConfig<'a> {
    /// DataFusion table name for diagnostics.
    pub(crate) source_name: &'a str,
    /// Normalized Delta table URI used to resolve the table-relative file path.
    pub(crate) table_uri: &'a str,
    /// Snapshot version that selected this file.
    pub(crate) snapshot_version: u64,
}

/// Reusable official-kernel reader baseline for one provider scan context.
#[allow(dead_code)]
pub(crate) struct KernelDataFileReader {
    source_name: String,
    table_uri: String,
    snapshot_version: u64,
    engine: std::sync::Arc<dyn kernel::Engine>,
}

/// Request to read one Delta data file through the official kernel engine path.
#[allow(dead_code)]
pub(crate) struct KernelDataFileReadRequest<'a> {
    /// Delta add-action table-relative file path.
    pub(crate) path: &'a str,
    /// File size from scan metadata.
    pub(crate) size: Option<u64>,
    /// Last modification timestamp from scan metadata, in milliseconds.
    pub(crate) modification_time_ms: Option<i64>,
    /// Kernel scan schema state for this provider scan.
    pub(crate) schema: &'a KernelScanReadSchema,
}

/// Data read from one Delta data file by the official-kernel adapter.
#[allow(dead_code)]
pub(crate) struct KernelDataFileReadResult {
    /// Fully resolved data-file metadata handed to Delta Kernel.
    pub(crate) file_meta: kernel::FileMeta,
    /// Arrow batches returned by the kernel Arrow engine.
    pub(crate) batches: Vec<kernel::RecordBatch>,
}

/// Request to apply one scan-file physical-to-logical transform.
#[allow(dead_code)]
pub(crate) struct KernelDataFileTransformRequest<'a> {
    /// Delta add-action table-relative file path.
    pub(crate) path: &'a str,
    /// Physical batch returned by the Parquet handler.
    pub(crate) batch: kernel::RecordBatch,
    /// Kernel scan schema state for this provider scan.
    pub(crate) schema: &'a KernelScanReadSchema,
    /// Kernel transform selected for the scan file.
    pub(crate) transform: &'a KernelPhysicalToLogicalTransform,
}

impl KernelDataFileReader {
    /// Builds a reusable official-kernel reader for one provider scan context.
    #[allow(dead_code)]
    pub(crate) fn try_new(
        config: KernelDataFileReaderConfig<'_>,
    ) -> Result<Self, DeltaFunnelError> {
        const TABLE_ROOT_CONTEXT: &str = "<table-root>";

        let table_url =
            kernel::try_parse_uri(config.table_uri).context(DeltaScanFileReadSnafu {
                source_name: config.source_name.to_owned(),
                table_uri: config.table_uri.to_owned(),
                snapshot_version: config.snapshot_version,
                path: TABLE_ROOT_CONTEXT.to_owned(),
                phase: DeltaScanFileReadPhase::TableUriParsing,
            })?;
        let store = kernel::store_from_url_opts(&table_url, std::iter::empty::<(&str, &str)>())
            .context(DeltaScanFileReadSnafu {
                source_name: config.source_name.to_owned(),
                table_uri: config.table_uri.to_owned(),
                snapshot_version: config.snapshot_version,
                path: TABLE_ROOT_CONTEXT.to_owned(),
                phase: DeltaScanFileReadPhase::ObjectStoreEngineConstruction,
            })?;
        let engine = std::sync::Arc::new(kernel::DefaultEngineBuilder::new(store).build());

        Ok(Self {
            source_name: config.source_name.to_owned(),
            table_uri: config.table_uri.to_owned(),
            snapshot_version: config.snapshot_version,
            engine,
        })
    }

    /// Reads one provider-selected Delta data file through official Delta Kernel APIs.
    #[allow(dead_code)]
    pub(crate) fn read_file_batches(
        &self,
        request: KernelDataFileReadRequest<'_>,
    ) -> Result<KernelDataFileReadResult, DeltaFunnelError> {
        let table_url = kernel::try_parse_uri(&self.table_uri).context(DeltaScanFileReadSnafu {
            source_name: self.source_name.clone(),
            table_uri: self.table_uri.clone(),
            snapshot_version: self.snapshot_version,
            path: request.path.to_owned(),
            phase: DeltaScanFileReadPhase::TableUriParsing,
        })?;
        let size = request
            .size
            .ok_or_else(|| {
                kernel::DeltaKernelError::generic("file size is required to read a Delta data file")
            })
            .context(DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: request.path.to_owned(),
                phase: DeltaScanFileReadPhase::FileMetadataConversion,
            })?;
        let modification_time_ms = request
            .modification_time_ms
            .ok_or_else(|| {
                kernel::DeltaKernelError::generic(
                    "file modification time is required to read a Delta data file",
                )
            })
            .context(DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: request.path.to_owned(),
                phase: DeltaScanFileReadPhase::FileMetadataConversion,
            })?;
        let location = table_url
            .join(request.path)
            .map_err(kernel::DeltaKernelError::from)
            .context(DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: request.path.to_owned(),
                phase: DeltaScanFileReadPhase::FilePathResolution,
            })?;
        let file_meta = kernel::FileMeta::new(location, modification_time_ms, size);
        let read_results = self
            .engine
            .parquet_handler()
            .read_parquet_files(
                std::slice::from_ref(&file_meta),
                request.schema.physical_schema.clone(),
                request.schema.physical_predicate.clone(),
            )
            .context(DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: request.path.to_owned(),
                phase: DeltaScanFileReadPhase::ParquetReadSetup,
            })?;
        let mut batches = Vec::new();

        for read_result in read_results {
            let engine_data = read_result.context(DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: request.path.to_owned(),
                phase: DeltaScanFileReadPhase::ParquetBatchRead,
            })?;
            let batch = kernel::EngineDataArrowExt::try_into_record_batch(engine_data).context(
                DeltaScanFileReadSnafu {
                    source_name: self.source_name.clone(),
                    table_uri: self.table_uri.clone(),
                    snapshot_version: self.snapshot_version,
                    path: request.path.to_owned(),
                    phase: DeltaScanFileReadPhase::ArrowConversion,
                },
            )?;
            batches.push(batch);
        }

        Ok(KernelDataFileReadResult { file_meta, batches })
    }

    /// Applies the official-kernel physical-to-logical transform for one batch.
    #[allow(dead_code)]
    pub(crate) fn apply_physical_to_logical_transform(
        &self,
        request: KernelDataFileTransformRequest<'_>,
    ) -> Result<kernel::RecordBatch, DeltaFunnelError> {
        let KernelPhysicalToLogicalTransform::Required(transform) = request.transform else {
            return Ok(request.batch);
        };

        let physical_rows = request.batch.num_rows();
        let physical_data: Box<dyn delta_kernel::EngineData> =
            Box::new(kernel::ArrowEngineData::new(request.batch));
        let logical_data = kernel::transform_to_logical(
            self.engine.as_ref(),
            physical_data,
            request.schema.physical_schema(),
            request.schema.logical_schema(),
            Some(transform.transform.clone()),
        )
        .context(DeltaScanFileReadSnafu {
            source_name: self.source_name.clone(),
            table_uri: self.table_uri.clone(),
            snapshot_version: self.snapshot_version,
            path: request.path.to_owned(),
            phase: DeltaScanFileReadPhase::TransformApplication,
        })?;
        let logical_batch = kernel::EngineDataArrowExt::try_into_record_batch(logical_data)
            .context(DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: request.path.to_owned(),
                phase: DeltaScanFileReadPhase::ArrowConversion,
            })?;

        if logical_batch.num_rows() == physical_rows {
            return Ok(logical_batch);
        }

        Err(kernel::DeltaKernelError::generic(format!(
            "physical-to-logical transform changed row count from {physical_rows} to {}",
            logical_batch.num_rows()
        )))
        .context(DeltaScanFileReadSnafu {
            source_name: self.source_name.clone(),
            table_uri: self.table_uri.clone(),
            snapshot_version: self.snapshot_version,
            path: request.path.to_owned(),
            phase: DeltaScanFileReadPhase::TransformApplication,
        })
    }
}

#[cfg(test)]
mod tests {
    use delta_kernel::arrow::array::{Array, Int32Array, StringArray};

    use super::{KernelDataFileReadRequest, KernelDataFileReader, KernelDataFileReaderConfig};
    use crate::table_formats::RealParquetDeltaTable;
    use crate::table_formats::delta::{
        DeltaSourceConfig, PlannedDeltaSource, build_projected_delta_scan,
        build_projected_predicated_stats_delta_scan, load_delta_source,
    };

    #[test]
    fn adapter_reads_real_parquet_batches_from_delta_fixture()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("adapter-full-read")?;
        let source = load_source("orders", &table)?;
        let scan = build_projected_predicated_stats_delta_scan(&source, None, None)?;
        let read_schema = scan.read_schema();
        let reader = test_reader(&source)?;
        let file = scan
            .expand_kernel_scan_metadata(source.table_uri())?
            .files
            .into_iter()
            .next()
            .ok_or("expected one scan file")?;
        let result = reader.read_file_batches(KernelDataFileReadRequest {
            path: &file.path,
            size: Some(u64::try_from(file.size)?),
            modification_time_ms: Some(file.modification_time),
            schema: &read_schema,
        })?;

        assert_eq!(file.path, table.data_file_path());
        assert_eq!(u64::try_from(file.size)?, table.data_file_size());
        assert!(
            result
                .file_meta
                .location
                .as_str()
                .ends_with(table.data_file_path())
        );
        assert_eq!(result.batches.len(), 1);

        let batch = result.batches.first().ok_or("expected one record batch")?;
        assert_eq!(batch.num_rows(), table.rows());
        assert_eq!(batch.num_columns(), 2);
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected id Int32Array")?;
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected customer_name StringArray")?;

        assert_eq!(ids.values(), &[1, 2, 3]);
        assert_eq!(names.value(0), "alice");
        assert_eq!(names.value(1), "bob");
        assert!(names.is_null(2));

        Ok(())
    }

    #[test]
    fn adapter_honors_projected_kernel_physical_schema() -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("adapter-projection")?;
        let source = load_source("orders", &table)?;
        let projected_columns = vec!["customer_name".to_owned()];
        let scan = build_projected_delta_scan(&source, Some(&projected_columns))?;
        let read_schema = scan.read_schema();
        let reader = test_reader(&source)?;
        let file = scan
            .expand_kernel_scan_metadata(source.table_uri())?
            .files
            .into_iter()
            .next()
            .ok_or("expected one scan file")?;
        let result = reader.read_file_batches(KernelDataFileReadRequest {
            path: &file.path,
            size: Some(u64::try_from(file.size)?),
            modification_time_ms: Some(file.modification_time),
            schema: &read_schema,
        })?;
        let batch = result.batches.first().ok_or("expected one record batch")?;

        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "customer_name");

        Ok(())
    }

    #[test]
    fn adapter_file_metadata_error_preserves_read_context() -> Result<(), Box<dyn std::error::Error>>
    {
        let table = RealParquetDeltaTable::new_default("adapter-error-context")?;
        let source = load_source("orders", &table)?;
        let scan = build_projected_delta_scan(&source, None)?;
        let read_schema = scan.read_schema();
        let reader = test_reader(&source)?;
        let error = reader
            .read_file_batches(KernelDataFileReadRequest {
                path: "missing-size.parquet",
                size: None,
                modification_time_ms: Some(1_587_968_586_000),
                schema: &read_schema,
            })
            .err()
            .ok_or("expected missing size error")?;
        let display = error.to_string();

        assert!(display.contains("source `orders`"), "{display}");
        assert!(display.contains("snapshot version 1"), "{display}");
        assert!(display.contains("missing-size.parquet"), "{display}");
        assert!(display.contains("file metadata conversion"), "{display}");
        assert!(display.contains("file size is required"), "{display}");

        Ok(())
    }

    #[test]
    fn issue_136_does_not_add_forbidden_delta_readers() -> Result<(), Box<dyn std::error::Error>> {
        let manifest = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))?;

        assert!(!manifest.contains("deltalake"));
        assert!(!manifest.contains("buoyant_kernel"));

        Ok(())
    }

    fn load_source(
        source_name: &str,
        table: &RealParquetDeltaTable,
    ) -> Result<PlannedDeltaSource, Box<dyn std::error::Error>> {
        Ok(load_delta_source(DeltaSourceConfig {
            name: source_name.to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?)
    }

    fn test_reader(
        source: &PlannedDeltaSource,
    ) -> Result<KernelDataFileReader, Box<dyn std::error::Error>> {
        Ok(KernelDataFileReader::try_new(KernelDataFileReaderConfig {
            source_name: source.name(),
            table_uri: source.table_uri(),
            snapshot_version: source.version(),
        })?)
    }
}
