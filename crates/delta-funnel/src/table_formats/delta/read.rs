//! Private Delta Kernel data-file read adapter.
//!
//! This module is the official-kernel synchronous reader baseline for #4. It
//! must not be called directly from a DataFusion stream polling loop. Provider
//! execution places this boundary behind bounded scheduling and backpressure.

use crate::{
    DeltaFunnelError,
    error::{DeltaScanFileReadPhase, DeltaScanFileReadSnafu},
};
use delta_kernel::arrow::array::BooleanArray;
use snafu::ResultExt;

use super::{DeltaStorageOptions, KernelPhysicalToLogicalTransform, kernel};

/// Kernel scan schema state required to read physical Parquet data.
#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct KernelScanReadSchema {
    physical_schema: kernel::KernelSchemaRef,
    logical_schema: kernel::KernelSchemaRef,
    physical_predicate: Option<kernel::PredicateRef>,
    enforce_physical_predicate_rows: bool,
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
            enforce_physical_predicate_rows: false,
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

    /// Optional kernel physical predicate for read execution.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn physical_predicate(&self) -> Option<&kernel::PredicateRef> {
        self.physical_predicate.as_ref()
    }

    /// Whether this scan schema carries a kernel physical predicate.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn has_physical_predicate(&self) -> bool {
        self.physical_predicate.is_some()
    }

    /// Whether the file reader must apply the physical predicate as a
    /// provider-enforced row-level filter.
    ///
    /// This is intentionally separate from `physical_predicate`: the same
    /// predicate can be used for metadata pruning while DataFusion keeps a
    /// residual filter for inexact pushdown.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn enforces_physical_predicate_rows(&self) -> bool {
        self.enforce_physical_predicate_rows && self.physical_predicate.is_some()
    }

    /// Enables provider-enforced row-level filtering for the physical predicate.
    ///
    /// Callers should only use this when provider planning reports an equivalent
    /// filter as exact or otherwise explicitly accepts duplicate residual
    /// evaluation.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn with_provider_enforced_physical_predicate_rows(mut self) -> Self {
        self.enforce_physical_predicate_rows = true;
        self
    }

    /// Returns this read schema without a kernel Parquet read predicate.
    ///
    /// Backends that cannot preserve original physical row indexes for
    /// predicate-pruned DV rows can drop the physical predicate and rely on
    /// DataFusion residual filters when scan planning kept one.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn without_physical_predicate(mut self) -> Self {
        self.physical_predicate = None;
        self.enforce_physical_predicate_rows = false;
        self
    }

    fn transform_schema_context(&self) -> String {
        format!(
            "physical schema fields [{}], logical schema fields [{}]",
            schema_field_names(&self.physical_schema),
            schema_field_names(&self.logical_schema)
        )
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
    /// Source-local options forwarded to Delta Kernel object-store construction.
    pub(crate) storage_options: &'a DeltaStorageOptions,
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

/// Request to evaluate one row-level scan predicate against a native batch.
///
/// This request is used by the native parquet-rs `RowFilter` path after the
/// reader has materialized the predicate columns for rows that reached row-level
/// evaluation. It is separate from file, row-group, or page metadata pruning.
#[allow(dead_code)]
pub(crate) struct KernelDataFilePredicateEvalRequest<'a> {
    /// Delta add-action table-relative file path.
    pub(crate) path: &'a str,
    /// Provider physical batch to evaluate.
    pub(crate) batch: kernel::RecordBatch,
    /// Kernel scan schema state for this provider scan.
    pub(crate) schema: &'a KernelScanReadSchema,
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
        let store = kernel::store_from_url_opts(
            &table_url,
            config
                .storage_options
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        )
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
        let logical_data = match kernel::transform_to_logical(
            self.engine.as_ref(),
            physical_data,
            request.schema.physical_schema(),
            request.schema.logical_schema(),
            Some(transform.transform.clone()),
        ) {
            Ok(logical_data) => logical_data,
            Err(error) => {
                return Err(kernel::DeltaKernelError::generic(format!(
                    "{error}; {}",
                    request.schema.transform_schema_context()
                )))
                .context(DeltaScanFileReadSnafu {
                    source_name: self.source_name.clone(),
                    table_uri: self.table_uri.clone(),
                    snapshot_version: self.snapshot_version,
                    path: request.path.to_owned(),
                    phase: DeltaScanFileReadPhase::TransformApplication,
                });
            }
        };
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
            "physical-to-logical transform changed row count from {physical_rows} to {}; {}",
            logical_batch.num_rows(),
            request.schema.transform_schema_context()
        )))
        .context(DeltaScanFileReadSnafu {
            source_name: self.source_name.clone(),
            table_uri: self.table_uri.clone(),
            snapshot_version: self.snapshot_version,
            path: request.path.to_owned(),
            phase: DeltaScanFileReadPhase::TransformApplication,
        })
    }

    /// Evaluates one row-level scan predicate against a provider physical batch.
    ///
    /// Native parquet-rs row filters receive batches outside the official kernel
    /// Parquet handler. This keeps predicate evaluation on the same kernel
    /// Arrow evaluator used by the baseline reader, but does not perform
    /// metadata pruning.
    #[allow(dead_code)]
    pub(crate) fn evaluate_physical_predicate(
        &self,
        request: KernelDataFilePredicateEvalRequest<'_>,
    ) -> Result<BooleanArray, DeltaFunnelError> {
        let predicate = request
            .schema
            .physical_predicate
            .clone()
            .ok_or_else(|| {
                kernel::DeltaKernelError::generic(
                    "physical predicate evaluation requires a scan predicate",
                )
            })
            .context(DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: request.path.to_owned(),
                phase: DeltaScanFileReadPhase::PredicateEvaluation,
            })?;
        let evaluator = self
            .engine
            .evaluation_handler()
            .new_predicate_evaluator(request.schema.physical_schema.clone(), predicate)
            .context(DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: request.path.to_owned(),
                phase: DeltaScanFileReadPhase::PredicateEvaluation,
            })?;
        let batch = kernel::ArrowEngineData::new(request.batch);
        let result = evaluator.evaluate(&batch).context(DeltaScanFileReadSnafu {
            source_name: self.source_name.clone(),
            table_uri: self.table_uri.clone(),
            snapshot_version: self.snapshot_version,
            path: request.path.to_owned(),
            phase: DeltaScanFileReadPhase::PredicateEvaluation,
        })?;
        // Kernel predicate evaluators return EngineData whose schema is a
        // single nullable boolean column named "output". parquet-rs RowFilter
        // needs that boolean array directly, so convert the EngineData back to
        // a RecordBatch and extract the first column below.
        let result = kernel::EngineDataArrowExt::try_into_record_batch(result).context(
            DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: request.path.to_owned(),
                phase: DeltaScanFileReadPhase::ArrowConversion,
            },
        )?;
        // Treat any contract mismatch as a predicate-evaluation failure before
        // rows can be exposed. A non-boolean mask would make parquet-rs row
        // filtering ambiguous, and silently ignoring it could drop an exact
        // pushed filter.
        let Some(mask) = result
            .columns()
            .first()
            .and_then(|column| column.as_any().downcast_ref::<BooleanArray>())
        else {
            return Err(kernel::DeltaKernelError::generic(
                "physical predicate evaluator did not return a BooleanArray",
            ))
            .context(DeltaScanFileReadSnafu {
                source_name: self.source_name.clone(),
                table_uri: self.table_uri.clone(),
                snapshot_version: self.snapshot_version,
                path: request.path.to_owned(),
                phase: DeltaScanFileReadPhase::PredicateEvaluation,
            });
        };

        Ok(mask.clone())
    }
}

fn schema_field_names(schema: &kernel::KernelSchemaRef) -> String {
    schema
        .fields()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>()
        .join(", ")
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
            .expand_kernel_scan_metadata(source.table_uri(), source.storage_options())?
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
            .expand_kernel_scan_metadata(source.table_uri(), source.storage_options())?
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
            storage_options: Default::default(),
        })?)
    }

    fn test_reader(
        source: &PlannedDeltaSource,
    ) -> Result<KernelDataFileReader, Box<dyn std::error::Error>> {
        Ok(KernelDataFileReader::try_new(KernelDataFileReaderConfig {
            source_name: source.name(),
            table_uri: source.table_uri(),
            snapshot_version: source.version(),
            storage_options: source.storage_options(),
        })?)
    }
}
