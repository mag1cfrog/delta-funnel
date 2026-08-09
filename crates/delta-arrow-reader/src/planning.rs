//! Private scan planning models.

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap, HashSet},
    sync::Arc,
};

use arrow::datatypes::{Schema, SchemaRef};
use snafu::ResultExt;

use crate::{
    DeltaReadMetrics, DeltaReaderError, DeltaReaderExecutionOptions,
    deletion_vector::DeletionVectorMetadata,
    error::{
        InvalidConfigurationSnafu, InvalidProjectionSnafu, ScanPartitionPlanningSnafu,
        ScanPlanningSnafu,
    },
    kernel::{
        DeltaKernelEngineContext, DeltaKernelPredicate, KernelPhysicalToLogicalTransform,
        KernelScan, KernelScanFileMetadata, KernelScanSchemas,
    },
    metrics::DeltaReadMetricsConfig,
    partition_target::{
        DeltaScanPartitionTargetDiagnosticOutput,
        delta_scan_partition_target_local_environment_diagnostic,
        derive_delta_scan_partition_target_diagnostic,
    },
    protocol::validate_protocol,
    snapshot::LoadedDeltaTableSnapshot,
};

#[derive(Default)]
pub(crate) struct DeltaScanPartitionTargetOptions {
    pub(crate) explicit_target_partitions: Option<usize>,
    pub(crate) caller_target_partitions: Option<usize>,
}

#[allow(dead_code)]
pub(crate) struct DeltaScanPlan {
    pub(crate) snapshot_version: u64,
    pub(crate) engine_context: Arc<DeltaKernelEngineContext>,
    pub(crate) logical_schema: SchemaRef,
    pub(crate) physical_schema: SchemaRef,
    pub(crate) projected_schema: SchemaRef,
    pub(crate) kernel_schemas: KernelScanSchemas,
    pub(crate) partitions: Vec<DeltaScanFileTaskPartition>,
    pub(crate) partition_target_diagnostic: DeltaScanPartitionTargetDiagnosticOutput,
    pub(crate) scan_metadata_exhausted: bool,
    pub(crate) files_filtered_during_planning: Option<u64>,
    pub(crate) estimated_bytes: Option<u64>,
    pub(crate) estimated_rows: Option<u64>,
    pub(crate) physical_predicate: Option<DeltaKernelPredicate>,
    pub(crate) execution_options: DeltaReaderExecutionOptions,
    pub(crate) metrics: DeltaReadMetrics,
}

#[allow(dead_code)]
pub(crate) fn build_scan(
    snapshot: &LoadedDeltaTableSnapshot,
    projection: Option<&[String]>,
    predicate: Option<DeltaKernelPredicate>,
    include_stats: bool,
) -> Result<KernelScan, DeltaReaderError> {
    validate_protocol(snapshot.protocol_info())?;
    validate_projection(snapshot.schema().as_ref(), projection)?;
    snapshot
        .kernel_snapshot()
        .build_scan(projection, predicate, include_stats)
        .boxed()
        .context(ScanPlanningSnafu {
            reason: "kernel_scan_build_failed",
        })
}

#[allow(dead_code)]
pub(crate) fn plan_scan(
    snapshot: &LoadedDeltaTableSnapshot,
    projection: Option<&[String]>,
    hidden_columns: &[String],
    kernel_predicate: Option<DeltaKernelPredicate>,
    include_stats: bool,
    execution_options: DeltaReaderExecutionOptions,
    partition_target_options: DeltaScanPartitionTargetOptions,
) -> Result<DeltaScanPlan, DeltaReaderError> {
    execution_options.validate()?;
    let partition_target_diagnostic = local_partition_target_diagnostic(partition_target_options)?;
    let logical_projection =
        logical_projection(snapshot.schema().as_ref(), projection, hidden_columns)?;
    let scan = build_scan(
        snapshot,
        logical_projection.as_deref(),
        kernel_predicate.clone(),
        include_stats,
    )?;
    let metadata = scan
        .file_metadata(snapshot.engine_context())
        .boxed()
        .context(ScanPlanningSnafu {
            reason: "kernel_scan_metadata_failed",
        })?;
    let file_tasks = metadata
        .files
        .into_iter()
        .map(DeltaScanFileTask::try_from_kernel)
        .collect::<Result<Vec<_>, _>>()?;
    let files_planned = file_tasks.len();
    let estimated_bytes = checked_sum(
        file_tasks.iter().map(|task| task.estimated_bytes),
        "scan_estimated_bytes_overflow",
    )?;
    let estimated_rows = checked_sum(
        file_tasks.iter().map(|task| task.estimated_rows),
        "scan_estimated_rows_overflow",
    )?;
    let partitions =
        group_scan_file_tasks(file_tasks, partition_target_diagnostic.target_partitions)?;
    let logical_schema = scan.logical_schema();
    let physical_predicate = scan.physical_predicate();
    let projected_schema = match projection {
        None => Arc::clone(&logical_schema),
        Some(names) => Arc::new(Schema::new_with_metadata(
            logical_schema.fields()[..names.len()].to_vec(),
            logical_schema.metadata().clone(),
        )),
    };

    let metrics = DeltaReadMetrics::new(DeltaReadMetricsConfig {
        snapshot_version: snapshot.version(),
        reader_backend: execution_options.reader_backend(),
        scan_metadata_exhausted: Some(true),
        scan_partitions_planned: partitions.len(),
        files_planned,
        files_filtered_during_planning: metadata.files_filtered_during_planning,
        estimated_rows,
        estimated_bytes,
    });

    Ok(DeltaScanPlan {
        snapshot_version: snapshot.version(),
        engine_context: Arc::clone(snapshot.engine_context()),
        logical_schema,
        physical_schema: scan.physical_schema(),
        projected_schema,
        kernel_schemas: scan.schemas(),
        partitions,
        partition_target_diagnostic,
        scan_metadata_exhausted: true,
        files_filtered_during_planning: metadata.files_filtered_during_planning,
        estimated_bytes,
        estimated_rows,
        physical_predicate,
        execution_options,
        metrics,
    })
}

fn local_partition_target_diagnostic(
    options: DeltaScanPartitionTargetOptions,
) -> Result<DeltaScanPartitionTargetDiagnosticOutput, DeltaReaderError> {
    let mut input = delta_scan_partition_target_local_environment_diagnostic().policy_input;
    input.explicit_target_partitions = options.explicit_target_partitions;
    if options.caller_target_partitions.is_some() {
        input.datafusion_target_partitions = options.caller_target_partitions;
    }
    derive_delta_scan_partition_target_diagnostic(input)
}

fn logical_projection(
    schema: &Schema,
    projection: Option<&[String]>,
    hidden_columns: &[String],
) -> Result<Option<Vec<String>>, DeltaReaderError> {
    validate_projection(schema, projection)?;
    for name in hidden_columns {
        if schema.index_of(name).is_err() {
            return InvalidProjectionSnafu {
                reason: "column_not_found",
            }
            .fail();
        }
    }

    Ok(projection.map(|projection| {
        let mut logical = projection.to_vec();
        for name in hidden_columns {
            if !logical.contains(name) {
                logical.push(name.clone());
            }
        }
        logical
    }))
}

#[allow(dead_code)]
pub(crate) struct DeltaScanFileTaskPartition {
    pub(crate) file_tasks: Vec<DeltaScanFileTask>,
    pub(crate) estimated_bytes: Option<u64>,
    pub(crate) estimated_rows: Option<u64>,
}

#[allow(dead_code)]
pub(crate) fn group_scan_file_tasks(
    file_tasks: Vec<DeltaScanFileTask>,
    target_partitions: usize,
) -> Result<Vec<DeltaScanFileTaskPartition>, DeltaReaderError> {
    if target_partitions == 0 {
        return InvalidConfigurationSnafu {
            reason: "scan_partition_target_must_be_positive",
        }
        .fail();
    }

    let estimated_bytes = checked_sum(
        file_tasks.iter().map(|task| task.estimated_bytes),
        "scan_estimated_bytes_overflow",
    )?;
    if file_tasks.is_empty() {
        return Ok(Vec::new());
    }
    if matches!(estimated_bytes, Some(0) | None) {
        group_by_file_count(file_tasks, target_partitions)
    } else {
        group_by_estimated_bytes(file_tasks, target_partitions)
    }
}

fn group_by_estimated_bytes(
    mut file_tasks: Vec<DeltaScanFileTask>,
    target_partitions: usize,
) -> Result<Vec<DeltaScanFileTaskPartition>, DeltaReaderError> {
    let output_limit = target_partitions.min(file_tasks.len());
    file_tasks.sort_by_key(|task| Reverse(task.estimated_bytes));
    let mut file_tasks = file_tasks.into_iter();
    let mut partition_tasks = Vec::with_capacity(output_limit);
    let mut partition_loads = BinaryHeap::with_capacity(output_limit);

    for partition_index in 0..output_limit {
        let Some(file_task) = file_tasks.next() else {
            return partition_planning_error("known_size_grouping_exhausted_tasks");
        };
        let Some(file_bytes) = file_task.estimated_bytes else {
            return partition_planning_error("known_size_grouping_missing_estimated_bytes");
        };
        partition_tasks.push(vec![file_task]);
        partition_loads.push(Reverse((file_bytes, partition_index)));
    }

    for file_task in file_tasks {
        let Some(file_bytes) = file_task.estimated_bytes else {
            return partition_planning_error("known_size_grouping_missing_estimated_bytes");
        };
        let Some(Reverse((partition_bytes, partition_index))) = partition_loads.pop() else {
            return partition_planning_error("known_size_grouping_missing_partition");
        };
        let Some(partition_bytes) = partition_bytes.checked_add(file_bytes) else {
            return partition_planning_error("partition_estimated_bytes_overflow");
        };
        partition_tasks[partition_index].push(file_task);
        partition_loads.push(Reverse((partition_bytes, partition_index)));
    }

    partition_tasks.into_iter().map(build_partition).collect()
}

fn group_by_file_count(
    file_tasks: Vec<DeltaScanFileTask>,
    target_partitions: usize,
) -> Result<Vec<DeltaScanFileTaskPartition>, DeltaReaderError> {
    let output_limit = target_partitions.min(file_tasks.len());
    let mut partitions = Vec::with_capacity(output_limit);
    let mut file_tasks = file_tasks.into_iter();
    let mut remaining_files = file_tasks.len();

    for partition_index in 0..output_limit {
        let remaining_partitions = output_limit - partition_index;
        let take_count = remaining_files.div_ceil(remaining_partitions);
        let partition_tasks = file_tasks.by_ref().take(take_count).collect::<Vec<_>>();
        if partition_tasks.len() != take_count {
            return partition_planning_error("file_count_grouping_exhausted_tasks");
        }
        remaining_files -= take_count;
        partitions.push(build_partition(partition_tasks)?);
    }

    Ok(partitions)
}

fn build_partition(
    file_tasks: Vec<DeltaScanFileTask>,
) -> Result<DeltaScanFileTaskPartition, DeltaReaderError> {
    let estimated_bytes = checked_sum(
        file_tasks.iter().map(|task| task.estimated_bytes),
        "partition_estimated_bytes_overflow",
    )?;
    let estimated_rows = checked_sum(
        file_tasks.iter().map(|task| task.estimated_rows),
        "partition_estimated_rows_overflow",
    )?;
    Ok(DeltaScanFileTaskPartition {
        file_tasks,
        estimated_bytes,
        estimated_rows,
    })
}

fn checked_sum(
    estimates: impl IntoIterator<Item = Option<u64>>,
    overflow_reason: &'static str,
) -> Result<Option<u64>, DeltaReaderError> {
    let mut total = 0_u64;
    for estimate in estimates {
        let Some(estimate) = estimate else {
            return Ok(None);
        };
        let Some(next) = total.checked_add(estimate) else {
            return partition_planning_error(overflow_reason);
        };
        total = next;
    }
    Ok(Some(total))
}

fn partition_planning_error<T>(reason: &'static str) -> Result<T, DeltaReaderError> {
    ScanPartitionPlanningSnafu { reason }.fail()
}

fn validate_projection(
    schema: &Schema,
    projection: Option<&[String]>,
) -> Result<(), DeltaReaderError> {
    let Some(projection) = projection else {
        return Ok(());
    };
    let mut seen = HashSet::with_capacity(projection.len());

    for name in projection {
        if !seen.insert(name) {
            return InvalidProjectionSnafu {
                reason: "duplicate_column",
            }
            .fail();
        }
        if schema.index_of(name).is_err() {
            return InvalidProjectionSnafu {
                reason: "column_not_found",
            }
            .fail();
        }
    }

    Ok(())
}

#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct DeltaScanFileTask {
    pub(crate) path: String,
    pub(crate) estimated_bytes: Option<u64>,
    pub(crate) estimated_rows: Option<u64>,
    pub(crate) stats: Option<DeltaScanFileStats>,
    pub(crate) modification_time_ms: Option<i64>,
    pub(crate) partition_values: BTreeMap<String, String>,
    pub(crate) deletion_vector: DeletionVectorMetadata,
    pub(crate) transform: KernelPhysicalToLogicalTransform,
}

#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct DeltaScanFileStats {
    pub(crate) num_records: u64,
}

#[allow(dead_code)]
impl DeltaScanFileTask {
    pub(crate) fn try_from_kernel(file: KernelScanFileMetadata) -> Result<Self, DeltaReaderError> {
        let estimated_bytes = u64::try_from(file.size)
            .boxed()
            .context(ScanPlanningSnafu {
                reason: "negative_file_size",
            })?;

        let stats = file
            .estimated_rows
            .map(|num_records| DeltaScanFileStats { num_records });

        Ok(Self {
            path: file.path,
            estimated_bytes: Some(estimated_bytes),
            estimated_rows: stats.as_ref().map(|stats| stats.num_records),
            stats,
            modification_time_ms: file.modification_time_ms,
            partition_values: file.partition_values,
            deletion_vector: DeletionVectorMetadata::from_kernel(file.deletion_vector),
            transform: file.transform,
        })
    }
}

#[cfg(test)]
mod tests {
    mod statistics_pruning;

    use std::{
        collections::HashMap,
        fs,
        future::{pending, poll_fn},
        path::{Path, PathBuf},
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        task::Poll,
        time::{SystemTime, UNIX_EPOCH},
    };

    use arrow::{
        array::{Int32Array, StringArray, StructArray},
        datatypes::{DataType, Schema},
        record_batch::RecordBatch,
    };
    use delta_kernel::{
        actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType},
        expressions::{ColumnName, Expression},
        scan::state::{DvInfo, ScanFile, Stats},
    };
    use futures_util::{FutureExt, StreamExt, stream};

    use super::{
        DeltaScanFileStats, DeltaScanFileTask, DeltaScanFileTaskPartition, DeltaScanPlan,
        KernelPhysicalToLogicalTransform, build_scan, checked_sum, group_scan_file_tasks,
    };
    use crate::{
        DeltaComparison, DeltaPredicate, DeltaReaderPhase, DeltaScalar, DeltaSnapshotSelection,
        DeltaStorageOptions,
        kernel::{KernelScanFileMetadata, delta_predicate_to_kernel_pruning},
        predicate::validate_predicate,
        scheduling::{
            DeltaScanExecution, FileAdmission, FileAdmissionFn, FileBatchStream, FileExecutor,
            FileReadPermit,
        },
        snapshot::load_delta_table_snapshot_blocking,
    };

    const PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
    const UNSUPPORTED_PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["madeUpFeature"],"writerFeatures":["madeUpFeature"]}}"#;
    const COLUMN_MAPPING_PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["columnMapping"],"writerFeatures":["columnMapping"]}}"#;
    const METADATA_JSON: &str = r#"{"metaData":{"id":"scan-planning-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"label\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"hidden\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
    const PARTITIONED_METADATA_JSON: &str = r#"{"metaData":{"id":"scan-planning-partition-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["region"],"configuration":{},"createdTime":1587968585495}}"#;
    const INVALID_PARTITION_METADATA_JSON: &str = r#"{"metaData":{"id":"scan-planning-invalid-partition-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"long_part\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["long_part"],"configuration":{},"createdTime":1587968585495}}"#;
    const COLUMN_MAPPING_METADATA_JSON: &str = r#"{"metaData":{"id":"scan-planning-column-mapping-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{\"delta.columnMapping.id\":1,\"delta.columnMapping.physicalName\":\"phys_id\"}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":2,\"delta.columnMapping.physicalName\":\"phys_customer_name\"}},{\"name\":\"profile\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"first_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":4,\"delta.columnMapping.physicalName\":\"phys_first_name\"}},{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":5,\"delta.columnMapping.physicalName\":\"phys_age\"}}]},\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":3,\"delta.columnMapping.physicalName\":\"phys_profile\"}}]}","partitionColumns":[],"configuration":{"delta.columnMapping.mode":"name","delta.columnMapping.maxColumnId":"5"},"createdTime":1587968585495}}"#;

    struct DeltaLogTable(PathBuf);

    impl DeltaLogTable {
        fn new_with_metadata_and_adds(
            name: &str,
            metadata: &str,
            adds: &[String],
        ) -> Result<Self, Box<dyn std::error::Error>> {
            Self::new_with_protocol_metadata_and_adds(name, PROTOCOL_JSON, metadata, adds)
        }

        fn new_with_protocol_metadata_and_adds(
            name: &str,
            protocol: &str,
            metadata: &str,
            adds: &[String],
        ) -> Result<Self, Box<dyn std::error::Error>> {
            let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
            let path = Path::new("target")
                .join("delta-arrow-reader-planning-tests")
                .join(format!("{}-{name}-{nanos}", std::process::id()));
            let log_path = path.join("_delta_log");
            fs::create_dir_all(&log_path)?;
            fs::write(
                log_path.join("00000000000000000000.json"),
                format!("{protocol}\n{metadata}\n{}", adds.join("\n")),
            )?;
            Ok(Self(path))
        }
    }

    impl Drop for DeltaLogTable {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn loaded_snapshot(
        name: &str,
    ) -> Result<
        (DeltaLogTable, crate::snapshot::LoadedDeltaTableSnapshot),
        Box<dyn std::error::Error>,
    > {
        loaded_snapshot_with_adds(name, &[])
    }

    fn loaded_snapshot_with_adds(
        name: &str,
        adds: &[String],
    ) -> Result<
        (DeltaLogTable, crate::snapshot::LoadedDeltaTableSnapshot),
        Box<dyn std::error::Error>,
    > {
        loaded_snapshot_with_metadata_and_adds(name, METADATA_JSON, adds)
    }

    fn loaded_snapshot_with_metadata_and_adds(
        name: &str,
        metadata: &str,
        adds: &[String],
    ) -> Result<
        (DeltaLogTable, crate::snapshot::LoadedDeltaTableSnapshot),
        Box<dyn std::error::Error>,
    > {
        let table = DeltaLogTable::new_with_metadata_and_adds(name, metadata, adds)?;
        let snapshot = load_delta_table_snapshot_blocking(
            &table.0.to_string_lossy(),
            &DeltaStorageOptions::new(),
            DeltaSnapshotSelection::Latest,
        )?;
        Ok((table, snapshot))
    }

    fn add(path: &str, size: i64, rows: Option<u64>) -> String {
        let stats = rows.map(|rows| format!(r#"{{"numRecords":{rows}}}"#));
        add_with_stats(path, size, stats.as_deref())
    }

    fn add_with_stats(path: &str, size: i64, stats: Option<&str>) -> String {
        add_action(path, size, "{}", stats)
    }

    fn add_with_partition(path: &str, size: i64, rows: u64, region: &str) -> String {
        let stats = format!(r#"{{"numRecords":{rows}}}"#);
        let region = serde_json::to_string(region).expect("partition value is serializable");
        add_action(
            path,
            size,
            &format!(r#"{{"region":{region}}}"#),
            Some(&stats),
        )
    }

    fn add_action(path: &str, size: i64, partition_values: &str, stats: Option<&str>) -> String {
        let stats = stats.map_or_else(String::new, |stats| {
            format!(
                ",\"stats\":{}",
                serde_json::to_string(stats).expect("stats string is serializable")
            )
        });
        format!(
            r#"{{"add":{{"path":"{path}","partitionValues":{partition_values},"size":{size},"modificationTime":1587968586000,"dataChange":true{stats}}}}}"#
        )
    }

    fn field_names(schema: &arrow::datatypes::SchemaRef) -> Vec<&str> {
        schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect()
    }

    fn kernel_file(path: &str) -> ScanFile {
        ScanFile {
            path: path.to_owned(),
            size: 123,
            modification_time: 1_587_968_586_000,
            stats: Some(Stats { num_records: 7 }),
            dv_info: DvInfo::default(),
            transform: None,
            partition_values: HashMap::from([
                ("region".to_owned(), "us-west".to_owned()),
                ("day".to_owned(), "2026-06-11".to_owned()),
            ]),
        }
    }

    fn task(file: ScanFile) -> Result<DeltaScanFileTask, crate::DeltaReaderError> {
        DeltaScanFileTask::try_from_kernel(KernelScanFileMetadata::from_scan_file(file))
    }

    fn grouping_task(
        path: &str,
        estimated_bytes: Option<u64>,
        estimated_rows: Option<u64>,
    ) -> Result<DeltaScanFileTask, crate::DeltaReaderError> {
        let mut task = task(kernel_file(path))?;
        task.estimated_bytes = estimated_bytes;
        task.estimated_rows = estimated_rows;
        task.stats = estimated_rows.map(|num_records| DeltaScanFileStats { num_records });
        Ok(task)
    }

    fn partition_paths(partitions: &[DeltaScanFileTaskPartition]) -> Vec<Vec<&str>> {
        partitions
            .iter()
            .map(|partition| {
                partition
                    .file_tasks
                    .iter()
                    .map(|task| task.path.as_str())
                    .collect()
            })
            .collect()
    }

    fn planned_tasks(plan: &DeltaScanPlan) -> impl Iterator<Item = &DeltaScanFileTask> {
        plan.partitions
            .iter()
            .flat_map(|partition| partition.file_tasks.iter())
    }

    fn execution_file_stream(permit: FileReadPermit, batch: RecordBatch) -> FileBatchStream {
        Box::pin(stream::unfold(
            (Some(batch), permit),
            |(batch, permit)| async move { batch.map(|batch| (Ok(batch), (None, permit))) },
        ))
    }

    fn pending_execution_file_stream(permit: FileReadPermit) -> FileBatchStream {
        Box::pin(stream::once(async move {
            let _permit = permit;
            pending::<Result<RecordBatch, crate::DeltaReaderError>>().await
        }))
    }

    fn plan_scan(
        snapshot: &crate::snapshot::LoadedDeltaTableSnapshot,
        projection: Option<&[String]>,
        hidden_columns: &[String],
        kernel_predicate: Option<crate::kernel::DeltaKernelPredicate>,
        include_stats: bool,
        execution_options: crate::DeltaReaderExecutionOptions,
    ) -> Result<DeltaScanPlan, crate::DeltaReaderError> {
        super::plan_scan(
            snapshot,
            projection,
            hidden_columns,
            kernel_predicate,
            include_stats,
            execution_options,
            Default::default(),
        )
    }

    #[test]
    fn file_task_preserves_kernel_metadata_without_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut file = kernel_file("part-00000.parquet");
        file.dv_info = DeletionVectorDescriptor::try_new(
            DeletionVectorStorageType::Inline,
            "inline-payload",
            None,
            14,
            2,
        )?
        .into();
        file.transform = Some(Arc::new(Expression::Column(ColumnName::new([
            "physical_id",
        ]))));

        let task = task(file)?;

        assert_eq!(task.path, "part-00000.parquet");
        assert_eq!(task.estimated_bytes, Some(123));
        assert_eq!(task.estimated_rows, Some(7));
        assert_eq!(task.stats.as_ref().map(|stats| stats.num_records), Some(7));
        assert_eq!(task.modification_time_ms, Some(1_587_968_586_000));
        assert_eq!(
            task.partition_values.into_iter().collect::<Vec<_>>(),
            [
                ("day".to_owned(), "2026-06-11".to_owned()),
                ("region".to_owned(), "us-west".to_owned()),
            ]
        );
        assert!(task.deletion_vector.is_present());
        assert!(task.transform.is_required());
        assert!(task.transform.into_inner().is_some());

        Ok(())
    }

    #[test]
    fn file_task_preserves_zero_and_missing_estimates() -> Result<(), Box<dyn std::error::Error>> {
        let mut file = kernel_file("empty.parquet");
        file.size = 0;
        file.stats = None;

        let task = task(file)?;

        assert_eq!(task.estimated_bytes, Some(0));
        assert_eq!(task.estimated_rows, None);
        assert!(task.stats.is_none());
        assert!(!task.deletion_vector.is_present());
        assert!(!task.transform.is_required());

        Ok(())
    }

    #[test]
    fn file_task_rejects_negative_size_without_disclosing_path() {
        let mut file = kernel_file("secret-file.parquet");
        file.size = -1;

        let error = match task(file) {
            Ok(_) => panic!("negative size must fail"),
            Err(error) => error,
        };
        let display = error.to_string();

        assert_eq!(error.as_str(), "scan_planning");
        assert_eq!(error.phase(), DeltaReaderPhase::ScanPlanning);
        assert!(!display.contains("secret-file"));
        assert!(!format!("{error:?}").contains("secret-file"));
    }

    #[test]
    fn file_task_planning_exhausts_empty_single_and_multi_batch_scans()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_empty_table, empty_snapshot) = loaded_snapshot("empty-files")?;
        let execution_options = crate::DeltaReaderExecutionOptions::default()
            .with_reader_backend(crate::DeltaReaderBackend::OfficialKernel)?;
        let empty = plan_scan(&empty_snapshot, None, &[], None, true, execution_options)?;
        assert!(empty.partitions.is_empty());
        assert_eq!(empty.estimated_bytes, Some(0));
        assert_eq!(empty.estimated_rows, Some(0));
        let empty_metrics = empty.metrics.snapshot();
        assert_eq!(empty_metrics.snapshot_version, empty.snapshot_version);
        assert_eq!(
            empty_metrics.reader_backend,
            crate::DeltaReaderBackend::OfficialKernel
        );
        assert_eq!(empty_metrics.scan_metadata_exhausted, Some(true));
        assert_eq!(empty_metrics.scan_partitions_planned, 0);
        assert_eq!(empty_metrics.files_planned, 0);
        assert_eq!(
            empty_metrics.files_filtered_during_planning,
            empty.files_filtered_during_planning
        );
        assert_eq!(empty_metrics.estimated_bytes, Some(0));
        assert_eq!(empty_metrics.estimated_rows, Some(0));
        assert_eq!(empty_metrics.scan_partitions_started, 0);
        assert_eq!(empty_metrics.scan_partitions_completed, 0);
        assert_eq!(empty_metrics.files_started, 0);
        assert_eq!(empty_metrics.files_completed, 0);
        assert_eq!(empty_metrics.batches_produced, 0);
        assert_eq!(empty_metrics.rows_produced, 0);
        assert_eq!(empty_metrics.deletion_vector_payloads_loaded, 0);
        assert_eq!(empty_metrics.deletion_vectors_applied, 0);
        assert_eq!(empty_metrics.deletion_vector_rows_deleted, 0);
        assert_eq!(empty_metrics.deletion_vector_failures, 0);
        assert_eq!(empty_metrics.deletion_vector_rejections, 0);
        assert_eq!(empty_metrics.parquet_data_file_range_get_operations, None);
        assert_eq!(empty_metrics.parquet_data_file_full_get_operations, None);
        assert_eq!(empty_metrics.parquet_data_file_bytes_received, None);
        assert_eq!(empty_metrics.parquet_data_file_opened_bytes, None);

        let single_add = [add("single.parquet", 0, None)];
        let (_single_table, single_snapshot) =
            loaded_snapshot_with_adds("single-file", &single_add)?;
        let single = plan_scan(&single_snapshot, None, &[], None, true, Default::default())?;
        let single_task = planned_tasks(&single).next().ok_or("expected one task")?;
        assert_eq!(planned_tasks(&single).count(), 1);
        assert_eq!(single_task.path, "single.parquet");
        assert_eq!(single_task.estimated_bytes, Some(0));
        assert_eq!(single.estimated_bytes, Some(0));
        assert_eq!(single.estimated_rows, None);

        let adds = (0_u32..1_001)
            .map(|index| {
                add(
                    &format!("part-{index:04}.parquet"),
                    i64::from(index),
                    (index % 2 == 0).then_some(u64::from(index)),
                )
            })
            .collect::<Vec<_>>();
        let (_many_table, many_snapshot) = loaded_snapshot_with_adds("many-files", &adds)?;
        let projection = ["label".to_owned(), "id".to_owned()];
        let many = plan_scan(
            &many_snapshot,
            Some(&projection),
            &[],
            None,
            true,
            Default::default(),
        )?;

        assert_eq!(planned_tasks(&many).count(), adds.len());
        let first = planned_tasks(&many)
            .find(|task| task.path == "part-0000.parquet")
            .ok_or("expected first task")?;
        let second = planned_tasks(&many)
            .find(|task| task.path == "part-0001.parquet")
            .ok_or("expected second task")?;
        let last = planned_tasks(&many)
            .find(|task| task.path == "part-1000.parquet")
            .ok_or("expected last task")?;
        assert_eq!(first.estimated_bytes, Some(0));
        assert_eq!(second.estimated_rows, None);
        assert_eq!(last.estimated_bytes, Some(1_000));
        assert_eq!(last.estimated_rows, Some(1_000));
        assert!(many.scan_metadata_exhausted);
        assert_eq!(many.files_filtered_during_planning, Some(0));
        assert_eq!(many.estimated_bytes, Some(500_500));
        assert_eq!(many.estimated_rows, None);
        assert!(Arc::ptr_eq(
            &many.engine_context,
            many_snapshot.engine_context()
        ));
        assert_eq!(many.snapshot_version, many_snapshot.version());
        assert_eq!(field_names(&many.logical_schema), ["label", "id"]);
        assert_eq!(field_names(&many.physical_schema), ["label", "id"]);
        assert_eq!(field_names(&many.projected_schema), ["label", "id"]);
        assert!(many.physical_predicate.is_none());
        assert_eq!(many.execution_options, Default::default());

        Ok(())
    }

    #[test]
    fn file_task_planning_returns_no_partial_result() -> Result<(), Box<dyn std::error::Error>> {
        let adds = [
            add("first.parquet", 1, Some(1)),
            add("secret-invalid.parquet", -1, Some(1)),
            add("last.parquet", 1, Some(1)),
        ];
        let (_table, snapshot) = loaded_snapshot_with_adds("all-or-error", &adds)?;

        let error = match plan_scan(&snapshot, None, &[], None, true, Default::default()) {
            Ok(_) => return Err("invalid task must fail the plan".into()),
            Err(error) => error,
        };

        assert_eq!(error.as_str(), "scan_planning");
        assert!(!error.to_string().contains("secret-invalid"));

        Ok(())
    }

    #[test]
    fn scan_rejects_unsupported_protocol_before_metadata_expansion()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_protocol_metadata_and_adds(
            "unsupported-protocol",
            UNSUPPORTED_PROTOCOL_JSON,
            METADATA_JSON,
            &[add("secret-invalid.parquet", -1, Some(1))],
        )?;
        let snapshot = load_delta_table_snapshot_blocking(
            &table.0.to_string_lossy(),
            &DeltaStorageOptions::new(),
            DeltaSnapshotSelection::Latest,
        )?;

        let error = match plan_scan(&snapshot, None, &[], None, true, Default::default()) {
            Ok(_) => return Err("unsupported protocol must fail before metadata expansion".into()),
            Err(error) => error,
        };

        assert_eq!(error.phase(), DeltaReaderPhase::Protocol);
        assert_eq!(error.as_str(), "unsupported_protocol");
        assert!(!error.to_string().contains("madeUpFeature"));
        assert!(!error.to_string().contains("secret-invalid.parquet"));
        Ok(())
    }

    #[test]
    fn scan_metadata_visitor_failure_returns_no_partial_plan_or_sensitive_context()
    -> Result<(), Box<dyn std::error::Error>> {
        const INVALID_VALUE: &str = "secret-not-an-integer";
        let adds = [add_action(
            "secret-invalid-partition.parquet",
            1,
            &format!(r#"{{"long_part":"{INVALID_VALUE}"}}"#),
            None,
        )];
        let (_table, snapshot) = loaded_snapshot_with_metadata_and_adds(
            "invalid-partition",
            INVALID_PARTITION_METADATA_JSON,
            &adds,
        )?;

        let error = match plan_scan(&snapshot, None, &[], None, false, Default::default()) {
            Ok(_) => return Err("invalid partition metadata must fail the whole plan".into()),
            Err(error) => error,
        };
        let display = error.to_string();
        let debug = format!("{error:?}");

        assert_eq!(error.as_str(), "scan_planning");
        assert_eq!(error.phase(), DeltaReaderPhase::ScanPlanning);
        assert!(!display.contains(INVALID_VALUE));
        assert!(!display.contains("secret-invalid-partition"));
        assert!(!debug.contains(INVALID_VALUE));
        assert!(!debug.contains("secret-invalid-partition"));

        Ok(())
    }

    #[test]
    fn aggregate_estimates_are_exact_unknown_or_rejected_on_overflow()
    -> Result<(), crate::DeltaReaderError> {
        assert_eq!(checked_sum([], "overflow")?, Some(0));
        assert_eq!(checked_sum([Some(2), Some(3)], "overflow")?, Some(5));
        assert_eq!(checked_sum([Some(2), None, Some(3)], "overflow")?, None);
        assert!(checked_sum([Some(u64::MAX), Some(1)], "overflow").is_err());
        Ok(())
    }

    #[test]
    fn partition_grouping_rejects_zero_target() {
        let error = match group_scan_file_tasks(Vec::new(), 0) {
            Ok(_) => panic!("zero partition target must fail"),
            Err(error) => error,
        };

        assert_eq!(error.as_str(), "invalid_configuration");
        assert_eq!(error.phase(), DeltaReaderPhase::Configuration);
    }

    #[test]
    fn empty_file_task_list_returns_no_partitions() -> Result<(), Box<dyn std::error::Error>> {
        assert!(group_scan_file_tasks(Vec::new(), 4)?.is_empty());
        Ok(())
    }

    #[test]
    fn oversized_known_size_file_stays_whole() -> Result<(), Box<dyn std::error::Error>> {
        let partitions = group_scan_file_tasks(
            vec![
                grouping_task("huge.parquet", Some(1_000), Some(100))?,
                grouping_task("small-0.parquet", Some(10), Some(1))?,
                grouping_task("small-1.parquet", Some(10), Some(1))?,
            ],
            2,
        )?;

        assert_eq!(
            partition_paths(&partitions),
            vec![
                vec!["huge.parquet"],
                vec!["small-0.parquet", "small-1.parquet"]
            ]
        );
        assert_eq!(partitions[0].estimated_bytes, Some(1_000));
        assert_eq!(partitions[1].estimated_bytes, Some(20));
        Ok(())
    }

    #[test]
    fn known_size_files_group_by_estimated_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let partitions = group_scan_file_tasks(
            vec![
                grouping_task("large.parquet", Some(90), Some(9))?,
                grouping_task("small-1.parquet", Some(10), Some(1))?,
                grouping_task("small-2.parquet", Some(10), Some(1))?,
                grouping_task("small-3.parquet", Some(10), Some(1))?,
            ],
            2,
        )?;

        assert_eq!(
            partition_paths(&partitions),
            vec![
                vec!["large.parquet"],
                vec!["small-1.parquet", "small-2.parquet", "small-3.parquet"],
            ]
        );
        assert_eq!(
            partitions
                .iter()
                .map(|partition| partition.estimated_bytes)
                .collect::<Vec<_>>(),
            vec![Some(90), Some(30)]
        );
        assert_eq!(
            partitions
                .iter()
                .map(|partition| partition.estimated_rows)
                .collect::<Vec<_>>(),
            vec![Some(9), Some(3)]
        );
        Ok(())
    }

    #[test]
    fn known_size_files_use_stable_order_and_lowest_partition_tie_breaker()
    -> Result<(), Box<dyn std::error::Error>> {
        fn grouped_paths() -> Result<Vec<Vec<String>>, crate::DeltaReaderError> {
            group_scan_file_tasks(
                vec![
                    grouping_task("part-0.parquet", Some(6), Some(1))?,
                    grouping_task("part-1.parquet", Some(6), Some(1))?,
                    grouping_task("part-2.parquet", Some(4), Some(1))?,
                    grouping_task("part-3.parquet", Some(4), Some(1))?,
                ],
                2,
            )
            .map(|partitions| {
                partitions
                    .into_iter()
                    .map(|partition| {
                        partition
                            .file_tasks
                            .into_iter()
                            .map(|task| task.path)
                            .collect()
                    })
                    .collect()
            })
        }

        let expected = vec![
            vec!["part-0.parquet".to_owned(), "part-2.parquet".to_owned()],
            vec!["part-1.parquet".to_owned(), "part-3.parquet".to_owned()],
        ];
        assert_eq!(grouped_paths()?, expected);
        assert_eq!(grouped_paths()?, expected);
        Ok(())
    }

    #[test]
    fn known_size_files_do_not_accumulate_slack_in_the_last_partition()
    -> Result<(), Box<dyn std::error::Error>> {
        let partitions = group_scan_file_tasks(
            vec![
                grouping_task("part-0.parquet", Some(6), Some(1))?,
                grouping_task("part-1.parquet", Some(6), Some(1))?,
                grouping_task("part-2.parquet", Some(6), Some(1))?,
                grouping_task("part-3.parquet", Some(6), Some(1))?,
                grouping_task("part-4.parquet", Some(4), Some(1))?,
                grouping_task("part-5.parquet", Some(4), Some(1))?,
            ],
            2,
        )?;

        assert_eq!(
            partitions
                .iter()
                .map(|partition| partition.estimated_bytes)
                .collect::<Vec<_>>(),
            vec![Some(16), Some(16)]
        );
        Ok(())
    }

    #[test]
    fn mixed_zero_byte_files_keep_partitions_non_empty() -> Result<(), Box<dyn std::error::Error>> {
        let partitions = group_scan_file_tasks(
            vec![
                grouping_task("non-zero.parquet", Some(10), Some(1))?,
                grouping_task("zero-0.parquet", Some(0), Some(0))?,
                grouping_task("zero-1.parquet", Some(0), Some(0))?,
            ],
            3,
        )?;

        assert_eq!(
            partition_paths(&partitions),
            vec![
                vec!["non-zero.parquet"],
                vec!["zero-0.parquet"],
                vec!["zero-1.parquet"]
            ]
        );
        assert!(
            partitions
                .iter()
                .all(|partition| !partition.file_tasks.is_empty())
        );
        Ok(())
    }

    #[test]
    fn target_above_file_count_emits_one_partition_per_task()
    -> Result<(), Box<dyn std::error::Error>> {
        let partitions = group_scan_file_tasks(
            vec![
                grouping_task("part-0.parquet", Some(10), Some(1))?,
                grouping_task("part-1.parquet", Some(10), Some(1))?,
            ],
            8,
        )?;

        assert_eq!(partitions.len(), 2);
        assert_eq!(
            partition_paths(&partitions),
            vec![vec!["part-0.parquet"], vec!["part-1.parquet"]]
        );
        Ok(())
    }

    #[test]
    fn unknown_size_files_fallback_to_scan_order_file_count_balancing()
    -> Result<(), Box<dyn std::error::Error>> {
        let partitions = group_scan_file_tasks(
            vec![
                grouping_task("part-0.parquet", None, Some(1))?,
                grouping_task("part-1.parquet", Some(10), Some(1))?,
                grouping_task("part-2.parquet", Some(10), Some(1))?,
                grouping_task("part-3.parquet", Some(10), Some(1))?,
                grouping_task("part-4.parquet", Some(10), Some(1))?,
            ],
            2,
        )?;

        assert_eq!(
            partition_paths(&partitions),
            vec![
                vec!["part-0.parquet", "part-1.parquet", "part-2.parquet"],
                vec!["part-3.parquet", "part-4.parquet"]
            ]
        );
        assert_eq!(partitions[0].estimated_bytes, None);
        assert_eq!(partitions[1].estimated_bytes, Some(20));
        Ok(())
    }

    #[test]
    fn all_zero_byte_files_use_scan_order_file_count_fallback()
    -> Result<(), Box<dyn std::error::Error>> {
        let partitions = group_scan_file_tasks(
            vec![
                grouping_task("zero-0.parquet", Some(0), Some(0))?,
                grouping_task("zero-1.parquet", Some(0), Some(0))?,
            ],
            4,
        )?;

        assert_eq!(
            partition_paths(&partitions),
            vec![vec!["zero-0.parquet"], vec!["zero-1.parquet"]]
        );
        assert_eq!(partitions[0].estimated_bytes, Some(0));
        assert_eq!(partitions[1].estimated_bytes, Some(0));
        assert_eq!(partitions[0].estimated_rows, Some(0));
        assert_eq!(partitions[1].estimated_rows, Some(0));
        Ok(())
    }

    #[test]
    fn each_input_file_task_appears_exactly_once() -> Result<(), Box<dyn std::error::Error>> {
        for target in [1, 4, 8] {
            let partitions = group_scan_file_tasks(
                vec![
                    grouping_task("part-0.parquet", Some(10), Some(1))?,
                    grouping_task("part-1.parquet", Some(10), Some(1))?,
                    grouping_task("part-2.parquet", Some(10), Some(1))?,
                    grouping_task("part-3.parquet", Some(10), Some(1))?,
                ],
                target,
            )?;
            let mut paths = partitions
                .iter()
                .flat_map(|partition| partition.file_tasks.iter())
                .map(|task| task.path.as_str())
                .collect::<Vec<_>>();
            paths.sort_unstable();

            assert_eq!(
                paths,
                vec![
                    "part-0.parquet",
                    "part-1.parquet",
                    "part-2.parquet",
                    "part-3.parquet"
                ]
            );
            assert!(
                partitions
                    .iter()
                    .all(|partition| !partition.file_tasks.is_empty())
            );
            assert!(partitions.len() <= target.min(4));
        }
        Ok(())
    }

    #[test]
    fn grouped_tasks_preserve_delta_metadata() -> Result<(), Box<dyn std::error::Error>> {
        let mut file = kernel_file("part-with-delta-metadata.parquet");
        file.dv_info = DeletionVectorDescriptor::try_new(
            DeletionVectorStorageType::Inline,
            "inline-payload",
            None,
            14,
            2,
        )?
        .into();
        file.transform = Some(Arc::new(Expression::Column(ColumnName::new([
            "physical_id",
        ]))));
        let partitions = group_scan_file_tasks(vec![task(file)?], 1)?;
        let grouped = &partitions[0].file_tasks[0];

        assert_eq!(grouped.path, "part-with-delta-metadata.parquet");
        assert_eq!(grouped.estimated_bytes, Some(123));
        assert_eq!(grouped.estimated_rows, Some(7));
        assert_eq!(
            grouped.partition_values.get("region").map(String::as_str),
            Some("us-west")
        );
        assert!(grouped.deletion_vector.is_present());
        assert!(grouped.transform.is_required());
        Ok(())
    }

    #[test]
    fn unknown_rows_keep_partition_row_estimates_unknown() -> Result<(), Box<dyn std::error::Error>>
    {
        let partitions = group_scan_file_tasks(
            vec![
                grouping_task("part-0.parquet", Some(10), None)?,
                grouping_task("part-1.parquet", Some(10), Some(1))?,
            ],
            1,
        )?;

        assert_eq!(partitions[0].estimated_rows, None);
        Ok(())
    }

    #[test]
    fn grouping_reports_estimate_overflow_without_disclosing_paths()
    -> Result<(), Box<dyn std::error::Error>> {
        let byte_error = group_scan_file_tasks(
            vec![
                grouping_task("secret-byte.parquet", Some(u64::MAX), Some(1))?,
                grouping_task("other.parquet", Some(1), Some(1))?,
            ],
            1,
        )
        .err()
        .ok_or("byte overflow must fail")?;
        assert_eq!(byte_error.as_str(), "scan_partition_planning");
        assert_eq!(byte_error.phase(), DeltaReaderPhase::ScanPlanning);
        assert!(!byte_error.to_string().contains("secret-byte"));

        let row_error = group_scan_file_tasks(
            vec![
                grouping_task("secret-row.parquet", Some(1), Some(u64::MAX))?,
                grouping_task("other.parquet", Some(1), Some(1))?,
            ],
            1,
        )
        .err()
        .ok_or("row overflow must fail")?;
        assert_eq!(row_error.as_str(), "scan_partition_planning");
        assert!(!row_error.to_string().contains("secret-row"));
        Ok(())
    }

    #[test]
    fn final_scan_plan_groups_once_and_initializes_one_shared_metrics_handle()
    -> Result<(), Box<dyn std::error::Error>> {
        let adds = [
            add("part-0.parquet", 40, Some(4)),
            add("part-1.parquet", 30, Some(3)),
            add("part-2.parquet", 20, Some(2)),
            add("part-3.parquet", 10, Some(1)),
        ];
        let (_table, snapshot) = loaded_snapshot_with_adds("final-plan", &adds)?;
        let plan = super::plan_scan(
            &snapshot,
            None,
            &[],
            None,
            true,
            Default::default(),
            super::DeltaScanPartitionTargetOptions {
                explicit_target_partitions: Some(2),
                caller_target_partitions: Some(1),
            },
        )?;

        assert_eq!(plan.snapshot_version, snapshot.version());
        assert!(Arc::ptr_eq(&plan.engine_context, snapshot.engine_context()));
        assert_eq!(plan.partition_target_diagnostic.target_partitions, 2);
        assert_eq!(
            plan.partition_target_diagnostic.source,
            crate::DeltaScanPartitionTargetDiagnosticSource::ExplicitOverride
        );
        assert_eq!(
            partition_paths(&plan.partitions),
            vec![
                vec!["part-0.parquet", "part-3.parquet"],
                vec!["part-1.parquet", "part-2.parquet"]
            ]
        );
        assert_eq!(plan.estimated_bytes, Some(100));
        assert_eq!(plan.estimated_rows, Some(10));

        let retained_metrics = plan.metrics.clone();
        let metrics = retained_metrics.snapshot();
        assert_eq!(metrics.snapshot_version, snapshot.version());
        assert_eq!(
            metrics.reader_backend,
            crate::DeltaReaderBackend::NativeAsync
        );
        assert_eq!(metrics.scan_metadata_exhausted, Some(true));
        assert_eq!(metrics.scan_partitions_planned, 2);
        assert_eq!(metrics.files_planned, 4);
        assert_eq!(metrics.files_filtered_during_planning, Some(0));
        assert_eq!(metrics.estimated_rows, Some(10));
        assert_eq!(metrics.estimated_bytes, Some(100));
        assert_eq!(metrics.scan_partitions_started, 0);
        assert_eq!(metrics.scan_partitions_completed, 0);
        assert_eq!(metrics.files_started, 0);
        assert_eq!(metrics.files_completed, 0);
        assert_eq!(metrics.batches_produced, 0);
        assert_eq!(metrics.rows_produced, 0);
        assert_eq!(metrics.deletion_vector_payloads_loaded, 0);
        assert_eq!(metrics.deletion_vectors_applied, 0);
        assert_eq!(metrics.deletion_vector_rows_deleted, 0);
        assert_eq!(metrics.deletion_vector_failures, 0);
        assert_eq!(metrics.deletion_vector_rejections, 0);
        assert_eq!(metrics.parquet_data_file_range_get_operations, Some(0));
        assert_eq!(metrics.parquet_data_file_full_get_operations, Some(0));
        assert_eq!(metrics.parquet_data_file_bytes_received, Some(0));
        assert_eq!(metrics.parquet_data_file_opened_bytes, Some(0));

        plan.metrics.record_deletion_vector_failure();
        assert_eq!(retained_metrics.snapshot().deletion_vector_failures, 1);
        Ok(())
    }

    #[tokio::test]
    async fn scan_execution_binds_plan_tasks_options_and_shared_metrics()
    -> Result<(), Box<dyn std::error::Error>> {
        let adds = [
            add("part-0.parquet", 20, Some(2)),
            add("part-1.parquet", 10, Some(1)),
        ];
        let (_table, snapshot) = loaded_snapshot_with_adds("scan-execution", &adds)?;
        let execution_options = crate::DeltaReaderExecutionOptions::new()
            .with_native_async_prefetch_file_count_per_partition(0)?
            .with_max_concurrent_file_reads_per_partition(1)?
            .with_max_concurrent_file_reads_per_scan(Some(1))?
            .with_output_buffer_capacity_per_partition(2)?;
        let plan = Arc::new(super::plan_scan(
            &snapshot,
            None,
            &[],
            None,
            true,
            execution_options,
            super::DeltaScanPartitionTargetOptions {
                explicit_target_partitions: Some(2),
                caller_target_partitions: None,
            },
        )?);
        let paths = Arc::new(Mutex::new(Vec::new()));
        let admission: FileAdmissionFn<DeltaScanFileTask> = Arc::new(|_| Ok(FileAdmission::Admit));
        let executor: FileExecutor<DeltaScanFileTask, FileBatchStream> = {
            let paths = Arc::clone(&paths);
            let schema = Arc::clone(&plan.logical_schema);
            Arc::new(move |task, permit, _| {
                paths
                    .lock()
                    .expect("paths lock is available")
                    .push(task.path);
                let batch = RecordBatch::new_empty(Arc::clone(&schema));
                async move { Ok(execution_file_stream(permit, batch)) }.boxed()
            })
        };
        let execution = DeltaScanExecution::new(Arc::clone(&plan));

        for partition in 0..plan.partitions.len() {
            let batches = execution
                .partition_stream(partition, Arc::clone(&admission), Arc::clone(&executor))?
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?;
            assert_eq!(batches.len(), 1);
        }
        let invalid = match execution.partition_stream(
            plan.partitions.len(),
            Arc::clone(&admission),
            Arc::clone(&executor),
        ) {
            Ok(_) => return Err("out-of-range execution partition must fail".into()),
            Err(error) => error,
        };
        assert_eq!(invalid.as_str(), "invalid_configuration");

        let cancelled_execution = DeltaScanExecution::new(Arc::clone(&plan));
        drop(cancelled_execution.partition_stream(
            0,
            Arc::clone(&admission),
            Arc::clone(&executor),
        )?);
        let repeated_execution = DeltaScanExecution::new(Arc::clone(&plan));
        let repeated = repeated_execution
            .partition_stream(0, admission, executor)?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(repeated.len(), 1);

        let mut paths = paths.lock().expect("paths lock is available").clone();
        paths.sort_unstable();
        assert_eq!(
            paths,
            ["part-0.parquet", "part-0.parquet", "part-1.parquet"]
        );
        let metrics = plan.metrics.snapshot();
        assert_eq!(metrics.scan_partitions_started, 3);
        assert_eq!(metrics.scan_partitions_completed, 3);
        assert_eq!(metrics.files_started, 3);
        assert_eq!(metrics.files_completed, 3);
        assert_eq!(metrics.batches_produced, 3);
        assert_eq!(metrics.rows_produced, 0);
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_scan_executions_share_only_the_plan()
    -> Result<(), Box<dyn std::error::Error>> {
        let adds = [add("part.parquet", 10, Some(1))];
        let (_table, snapshot) = loaded_snapshot_with_adds("concurrent-executions", &adds)?;
        let execution_options = crate::DeltaReaderExecutionOptions::new()
            .with_native_async_prefetch_file_count_per_partition(0)?
            .with_max_concurrent_file_reads_per_partition(1)?
            .with_max_concurrent_file_reads_per_scan(Some(1))?;
        let plan = Arc::new(super::plan_scan(
            &snapshot,
            None,
            &[],
            None,
            true,
            execution_options,
            super::DeltaScanPartitionTargetOptions {
                explicit_target_partitions: Some(1),
                caller_target_partitions: None,
            },
        )?);
        let calls = Arc::new(AtomicUsize::new(0));
        let executor: FileExecutor<DeltaScanFileTask, FileBatchStream> = {
            let calls = Arc::clone(&calls);
            Arc::new(move |_, permit, _| {
                calls.fetch_add(1, Ordering::SeqCst);
                async move { Ok(pending_execution_file_stream(permit)) }.boxed()
            })
        };
        let admission: FileAdmissionFn<DeltaScanFileTask> = Arc::new(|_| Ok(FileAdmission::Admit));
        let first_execution = DeltaScanExecution::new(Arc::clone(&plan));
        let second_execution = DeltaScanExecution::new(Arc::clone(&plan));
        let mut first =
            first_execution.partition_stream(0, Arc::clone(&admission), Arc::clone(&executor))?;
        let mut second = second_execution.partition_stream(0, admission, executor)?;
        let mut first_next = Box::pin(first.next());
        poll_fn(|context| {
            assert!(matches!(first_next.as_mut().poll(context), Poll::Pending));
            Poll::Ready(())
        })
        .await;
        for _ in 0..100 {
            if calls.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let mut second_next = Box::pin(second.next());
        poll_fn(|context| {
            assert!(matches!(second_next.as_mut().poll(context), Poll::Pending));
            Poll::Ready(())
        })
        .await;
        for _ in 0..100 {
            if calls.load(Ordering::SeqCst) == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        drop(first_next);
        drop(first);
        poll_fn(|context| {
            assert!(matches!(second_next.as_mut().poll(context), Poll::Pending));
            Poll::Ready(())
        })
        .await;
        drop(second_next);
        drop(second);
        let metrics = plan.metrics.snapshot();
        assert_eq!(metrics.scan_partitions_started, 2);
        assert_eq!(metrics.scan_partitions_completed, 0);
        assert_eq!(metrics.files_started, 2);
        assert_eq!(metrics.files_completed, 0);
        Ok(())
    }

    #[test]
    fn invalid_final_plan_target_fails_before_file_task_expansion()
    -> Result<(), Box<dyn std::error::Error>> {
        let adds = [add("secret-invalid.parquet", -1, Some(1))];
        let (_table, snapshot) = loaded_snapshot_with_adds("invalid-final-target", &adds)?;
        let error = super::plan_scan(
            &snapshot,
            None,
            &[],
            None,
            true,
            Default::default(),
            super::DeltaScanPartitionTargetOptions {
                explicit_target_partitions: Some(0),
                caller_target_partitions: None,
            },
        )
        .err()
        .ok_or("zero target must fail")?;

        assert_eq!(error.as_str(), "invalid_configuration");
        assert_eq!(error.phase(), DeltaReaderPhase::Configuration);
        assert!(!error.to_string().contains("secret-invalid"));
        Ok(())
    }

    #[test]
    fn final_scan_plan_rejects_aggregate_overflow() -> Result<(), Box<dyn std::error::Error>> {
        let adds = [
            add("secret-0.parquet", i64::MAX, Some(1)),
            add("secret-1.parquet", i64::MAX, Some(1)),
            add("secret-2.parquet", i64::MAX, Some(1)),
        ];
        let (_table, snapshot) = loaded_snapshot_with_adds("overflow-final-plan", &adds)?;
        let error = plan_scan(&snapshot, None, &[], None, true, Default::default())
            .err()
            .ok_or("aggregate byte overflow must fail")?;

        assert_eq!(error.as_str(), "scan_partition_planning");
        assert_eq!(error.phase(), DeltaReaderPhase::ScanPlanning);
        assert!(!error.to_string().contains("secret-"));
        Ok(())
    }

    #[test]
    fn scan_plan_keeps_hidden_columns_and_applies_static_stats_pruning()
    -> Result<(), Box<dyn std::error::Error>> {
        let adds = [
            add_with_stats(
                "impossible.parquet",
                10,
                Some(
                    r#"{"numRecords":2,"minValues":{"hidden":1},"maxValues":{"hidden":10},"nullCount":{"hidden":0}}"#,
                ),
            ),
            add_with_stats(
                "possible.parquet",
                20,
                Some(
                    r#"{"numRecords":3,"minValues":{"hidden":101},"maxValues":{"hidden":200},"nullCount":{"hidden":0}}"#,
                ),
            ),
            add("missing-stats.parquet", 30, None),
        ];
        let (_table, snapshot) = loaded_snapshot_with_adds("stats-pruning", &adds)?;
        let predicate = DeltaPredicate::Compare {
            column: "hidden".to_owned(),
            op: DeltaComparison::Gt,
            value: DeltaScalar::Int32(100),
        };
        validate_predicate(&predicate, snapshot.schema().as_ref())?;
        let kernel_predicate =
            delta_predicate_to_kernel_pruning(&predicate).ok_or("expected Kernel predicate")?;
        let projection = ["label".to_owned()];
        let hidden = ["hidden".to_owned()];

        let plan = plan_scan(
            &snapshot,
            Some(&projection),
            &hidden,
            Some(kernel_predicate),
            true,
            Default::default(),
        )?;

        assert_eq!(field_names(&plan.logical_schema), ["label", "hidden"]);
        assert_eq!(field_names(&plan.physical_schema), ["label", "hidden"]);
        assert_eq!(field_names(&plan.projected_schema), ["label"]);
        let mut paths = planned_tasks(&plan)
            .map(|task| task.path.as_str())
            .collect::<Vec<_>>();
        paths.sort_unstable();
        assert_eq!(paths, ["missing-stats.parquet", "possible.parquet"]);
        assert_eq!(plan.files_filtered_during_planning, Some(1));
        assert_eq!(plan.estimated_bytes, Some(50));
        assert_eq!(plan.estimated_rows, None);
        assert!(plan.physical_predicate.is_some());

        let empty_projection = Vec::new();
        let empty = plan_scan(
            &snapshot,
            Some(&empty_projection),
            &hidden,
            None,
            false,
            Default::default(),
        )?;
        assert_eq!(field_names(&empty.logical_schema), ["hidden"]);
        assert_eq!(field_names(&empty.physical_schema), ["hidden"]);
        assert!(empty.projected_schema.fields().is_empty());

        Ok(())
    }

    #[test]
    fn scan_plan_applies_one_transform_without_copying_no_transform_batches()
    -> Result<(), Box<dyn std::error::Error>> {
        let adds = [add("part.parquet", 10, Some(3))];
        let (_table, snapshot) = loaded_snapshot_with_adds("transforms", &adds)?;
        let projection = ["id".to_owned(), "label".to_owned()];
        let mut plan = plan_scan(
            &snapshot,
            Some(&projection),
            &[],
            None,
            false,
            Default::default(),
        )?;
        let mut task = plan
            .partitions
            .first_mut()
            .and_then(|partition| partition.file_tasks.pop())
            .ok_or("expected one task")?;
        let batch = || {
            RecordBatch::try_new(
                Arc::clone(&plan.physical_schema),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec!["a", "b", "c"])),
                ],
            )
        };

        let physical = batch()?;
        let first_column = Arc::clone(physical.column(0));
        let unchanged = plan.apply_transform(&task, physical)?;
        assert!(Arc::ptr_eq(&first_column, unchanged.column(0)));
        assert_eq!(unchanged.schema(), plan.logical_schema);

        let mismatch = plan
            .apply_transform(&task, RecordBatch::new_empty(Arc::new(Schema::empty())))
            .expect_err("wrong logical schema must fail");
        assert_eq!(mismatch.as_str(), "physical_to_logical_transform");
        assert_eq!(mismatch.phase(), DeltaReaderPhase::Transform);

        task.transform =
            KernelPhysicalToLogicalTransform::from_test_expression(Expression::struct_from([
                Expression::Column(ColumnName::new(["id"])),
                Expression::Literal(delta_kernel::expressions::Scalar::String(
                    "transformed".to_owned(),
                )),
            ]));
        let transformed = plan.apply_transform(&task, batch()?)?;
        let labels = transformed
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected string labels")?;

        assert_eq!(transformed.num_rows(), 3);
        assert_eq!(transformed.schema(), plan.logical_schema);
        assert_eq!(
            labels.iter().collect::<Vec<_>>(),
            [
                Some("transformed"),
                Some("transformed"),
                Some("transformed"),
            ]
        );

        task.transform = KernelPhysicalToLogicalTransform::from_test_expression(
            Expression::Column(ColumnName::new(["secret_missing"])),
        );
        let error = plan
            .apply_transform(&task, batch()?)
            .expect_err("invalid transform must fail");
        assert_eq!(error.as_str(), "physical_to_logical_transform");
        assert_eq!(error.phase(), DeltaReaderPhase::Transform);
        assert!(!error.to_string().contains("secret_missing"));
        assert!(!format!("{error:?}").contains("secret_missing"));

        Ok(())
    }

    #[test]
    fn scan_plan_applies_nested_kernel_column_mapping_transform()
    -> Result<(), Box<dyn std::error::Error>> {
        let adds = [add("mapped.parquet", 10, Some(2))];
        let table = DeltaLogTable::new_with_protocol_metadata_and_adds(
            "column-mapping-transform",
            COLUMN_MAPPING_PROTOCOL_JSON,
            COLUMN_MAPPING_METADATA_JSON,
            &adds,
        )?;
        let snapshot = load_delta_table_snapshot_blocking(
            &table.0.to_string_lossy(),
            &DeltaStorageOptions::new(),
            DeltaSnapshotSelection::Latest,
        )?;
        let plan = plan_scan(&snapshot, None, &[], None, false, Default::default())?;
        let task = planned_tasks(&plan)
            .next()
            .ok_or("expected one mapped task")?;

        assert_eq!(
            field_names(&plan.physical_schema),
            ["phys_id", "phys_customer_name", "phys_profile"]
        );
        assert_eq!(
            field_names(&plan.logical_schema),
            ["id", "customer_name", "profile"]
        );
        assert!(task.transform.is_required());
        let DataType::Struct(profile_fields) = plan.physical_schema.field(2).data_type() else {
            return Err("expected a physical profile struct".into());
        };
        assert_eq!(profile_fields[0].name(), "phys_first_name");
        assert_eq!(profile_fields[1].name(), "phys_age");
        let profile = StructArray::new(
            profile_fields.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("alice"), None])),
                Arc::new(Int32Array::from(vec![Some(30), None])),
            ],
            None,
        );

        let physical = RecordBatch::try_new(
            Arc::clone(&plan.physical_schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("customer-a"), None])),
                Arc::new(profile),
            ],
        )?;
        let logical = plan.apply_transform(task, physical)?;
        let profile = logical
            .column(2)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected mapped profile")?;
        let first_names = profile
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected mapped first names")?;
        let ages = profile
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected mapped ages")?;

        assert_eq!(logical.schema(), plan.logical_schema);
        assert_eq!(logical.num_rows(), 2);
        assert_eq!(profile.fields()[0].name(), "first_name");
        assert_eq!(profile.fields()[1].name(), "age");
        assert_eq!(
            first_names.iter().collect::<Vec<_>>(),
            [Some("alice"), None]
        );
        assert_eq!(ages.iter().collect::<Vec<_>>(), [Some(30), None]);

        Ok(())
    }

    #[test]
    fn scan_plan_preserves_partition_pruning_transform_and_final_478_state()
    -> Result<(), Box<dyn std::error::Error>> {
        let adds = [
            add_with_partition("east.parquet", 10, 1, "us-east"),
            add_with_partition("west.parquet", 20, 2, "us-west"),
        ];
        let (_table, snapshot) = loaded_snapshot_with_metadata_and_adds(
            "partition-transform",
            PARTITIONED_METADATA_JSON,
            &adds,
        )?;
        let predicate = DeltaPredicate::Compare {
            column: "region".to_owned(),
            op: DeltaComparison::Eq,
            value: DeltaScalar::Utf8("us-west".to_owned()),
        };
        validate_predicate(&predicate, snapshot.schema().as_ref())?;
        let kernel_predicate =
            delta_predicate_to_kernel_pruning(&predicate).ok_or("expected Kernel predicate")?;
        let projection = ["id".to_owned()];
        let hidden = ["region".to_owned()];
        let plan = plan_scan(
            &snapshot,
            Some(&projection),
            &hidden,
            Some(kernel_predicate),
            false,
            Default::default(),
        )?;

        assert_eq!(field_names(&plan.logical_schema), ["id", "region"]);
        assert_eq!(field_names(&plan.physical_schema), ["id"]);
        assert_eq!(field_names(&plan.projected_schema), ["id"]);
        let final_state = (
            plan.scan_metadata_exhausted,
            plan.partitions.len(),
            planned_tasks(&plan).count(),
            plan.files_filtered_during_planning,
            plan.estimated_bytes,
            plan.estimated_rows,
        );
        assert_eq!(final_state, (true, 1, 1, Some(1), Some(20), Some(2)));
        assert!(Arc::ptr_eq(&plan.engine_context, snapshot.engine_context()));

        let task = planned_tasks(&plan)
            .next()
            .ok_or("expected one selected task")?;
        assert_eq!(task.path, "west.parquet");
        assert_eq!(task.stats.as_ref().map(|stats| stats.num_records), Some(2));
        assert_eq!(
            task.partition_values.get("region").map(String::as_str),
            Some("us-west")
        );
        assert!(task.transform.is_required());
        let physical = RecordBatch::try_new(
            Arc::clone(&plan.physical_schema),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        )?;
        let logical = plan.apply_transform(task, physical)?;
        let regions = logical
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected partition values")?;

        assert_eq!(logical.schema(), plan.logical_schema);
        assert_eq!(
            regions.iter().collect::<Vec<_>>(),
            [Some("us-west"), Some("us-west"),]
        );

        Ok(())
    }

    #[test]
    fn scan_preserves_full_ordered_and_empty_projections() -> Result<(), Box<dyn std::error::Error>>
    {
        let (_table, snapshot) = loaded_snapshot("projections")?;

        let full = build_scan(&snapshot, None, None, false)?;
        assert_eq!(
            field_names(&full.logical_schema()),
            ["id", "label", "hidden"]
        );
        assert_eq!(
            field_names(&full.physical_schema()),
            ["id", "label", "hidden"]
        );

        let ordered_names = ["label".to_owned(), "id".to_owned()];
        let ordered = build_scan(&snapshot, Some(&ordered_names), None, false)?;
        assert_eq!(field_names(&ordered.logical_schema()), ["label", "id"]);
        assert_eq!(field_names(&ordered.physical_schema()), ["label", "id"]);

        let empty_names = Vec::<String>::new();
        let empty = build_scan(&snapshot, Some(&empty_names), None, false)?;
        assert!(empty.logical_schema().fields().is_empty());
        assert!(empty.physical_schema().fields().is_empty());

        Ok(())
    }

    #[test]
    fn scan_keeps_metadata_predicate_out_of_projected_schemas()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, snapshot) = loaded_snapshot("hidden-predicate")?;
        let predicate = DeltaPredicate::Compare {
            column: "hidden".to_owned(),
            op: DeltaComparison::Gt,
            value: DeltaScalar::Int32(1),
        };
        validate_predicate(&predicate, snapshot.schema().as_ref())?;
        let kernel_predicate =
            delta_predicate_to_kernel_pruning(&predicate).ok_or("expected Kernel predicate")?;
        let projection = ["label".to_owned()];

        let scan = build_scan(&snapshot, Some(&projection), Some(kernel_predicate), false)?;

        assert_eq!(field_names(&scan.logical_schema()), ["label"]);
        assert_eq!(field_names(&scan.physical_schema()), ["label"]);
        assert!(scan.has_physical_predicate());

        Ok(())
    }

    #[test]
    fn scan_rejects_missing_and_duplicate_projections_without_disclosure()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, snapshot) = loaded_snapshot("invalid-projection")?;

        for projection in [
            vec!["secret-missing".to_owned()],
            vec!["id".to_owned(), "id".to_owned()],
        ] {
            let error = match build_scan(&snapshot, Some(&projection), None, false) {
                Ok(_) => return Err("invalid projection must fail".into()),
                Err(error) => error,
            };
            let display = error.to_string();

            assert_eq!(error.as_str(), "invalid_projection");
            assert_eq!(error.phase(), DeltaReaderPhase::ScanPlanning);
            assert!(!display.contains("secret-missing"));
            assert!(!format!("{error:?}").contains("secret-missing"));
        }

        let projection = ["id".to_owned()];
        let hidden = ["secret-hidden".to_owned()];
        let error = match plan_scan(
            &snapshot,
            Some(&projection),
            &hidden,
            None,
            false,
            Default::default(),
        ) {
            Ok(_) => return Err("invalid hidden column must fail".into()),
            Err(error) => error,
        };
        assert_eq!(error.as_str(), "invalid_projection");
        assert!(!error.to_string().contains("secret-hidden"));

        Ok(())
    }

    #[test]
    fn planning_boundary_contains_no_execution_or_second_engine() {
        let planning_source = include_str!("planning.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("production planning source");
        let transform_source = include_str!("transform.rs");

        for forbidden in [
            "DefaultEngineBuilder",
            "store_from_url_opts",
            "Runtime::",
            "block_on(",
            "read_parquet",
            "get_row_indexes",
            "datafusion::",
            "MetricBuilder",
            "ExecutionPlanMetricsSet",
            "tracing::",
        ] {
            assert!(!planning_source.contains(forbidden), "{forbidden}");
            assert!(!transform_source.contains(forbidden), "{forbidden}");
        }
    }
}
