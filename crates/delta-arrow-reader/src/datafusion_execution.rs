//! Optional DataFusion physical execution adapter.

use std::{
    collections::HashSet,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion::{
    common::{DataFusionError, Result as DataFusionResult, config::ConfigOptions},
    execution::TaskContext,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream,
        execution_plan::{Boundedness, EmissionType, SchedulingType},
        filter_pushdown::{
            ChildPushdownResult, FilterPushdownPhase, FilterPushdownPropagation, PushedDown,
        },
        stream::RecordBatchStreamAdapter,
    },
};
use futures_util::{StreamExt, stream};

use crate::{
    DeltaReadMetrics, DeltaReadMetricsSnapshot, DeltaReaderBackend, DeltaReaderError,
    datafusion_dynamic_filters::{
        DeltaDynamicFilterOutcome, DeltaDynamicFilterPlan, DeltaRetainedDynamicFilter,
    },
    datafusion_dynamic_partition_pruning::{
        DeltaDynamicPartitionKeepReason, DeltaDynamicPartitionPruningDecision,
        evaluate_dynamic_partition_filter,
    },
    datafusion_planning::DataFusionScanPlanning,
    direct::{native_async_executor, official_kernel_executor},
    kernel::DeltaKernelPredicate,
    metrics::saturating_fetch_add,
    planning::{DeltaScanFileTask, DeltaScanPlan},
    scheduling::{DeltaScanExecution, FileAdmission, FileAdmissionFn, ScanReadLimiter},
};

/// Immutable point-in-time DataFusion scan metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaDataFusionMetricsSnapshot {
    /// Core reader planning and execution metrics.
    pub reader: DeltaReadMetricsSnapshot,
    /// Effective DataFusion task batch size observed at execution.
    pub output_batch_size: Option<u64>,
    /// Files pruned before admission by a dynamic partition filter.
    pub dynamic_partition_files_pruned: u64,
    /// Files kept after consulting retained dynamic partition filters.
    pub dynamic_partition_files_kept: u64,
    /// Physical filters offered to the post-optimization hook.
    pub dynamic_filters_received: u64,
    /// Offered filters retained for dynamic partition pruning.
    pub dynamic_filters_accepted: u64,
    /// Offered filters rejected by the dynamic partition policy.
    pub dynamic_filters_unsupported: u64,
    /// Current dynamic expressions consulted during file admission.
    pub dynamic_filter_snapshots: u64,
    /// Kept files with missing, invalid, or unparsable partition metadata.
    pub dynamic_files_not_pruned_missing_metadata: u64,
    /// Kept files with unavailable, unsupported, or failed expressions.
    pub dynamic_files_not_pruned_unsupported_expression: u64,
}

/// Shared live metrics for one DataFusion physical scan plan.
#[derive(Clone)]
pub struct DeltaDataFusionMetrics {
    inner: Arc<DeltaDataFusionMetricsInner>,
}

struct DeltaDataFusionMetricsInner {
    source_name: Option<String>,
    reader: DeltaReadMetrics,
    output_batch_size: AtomicU64,
    dynamic_partition_files_pruned: AtomicU64,
    dynamic_partition_files_kept: AtomicU64,
    dynamic_filters_received: AtomicU64,
    dynamic_filters_accepted: AtomicU64,
    dynamic_filters_unsupported: AtomicU64,
    dynamic_filter_snapshots: AtomicU64,
    dynamic_files_not_pruned_missing_metadata: AtomicU64,
    dynamic_files_not_pruned_unsupported_expression: AtomicU64,
}

impl DeltaDataFusionMetrics {
    #[allow(dead_code)]
    fn new(source_name: Option<String>, reader: DeltaReadMetrics) -> Self {
        Self {
            inner: Arc::new(DeltaDataFusionMetricsInner {
                source_name,
                reader,
                output_batch_size: AtomicU64::new(0),
                dynamic_partition_files_pruned: AtomicU64::new(0),
                dynamic_partition_files_kept: AtomicU64::new(0),
                dynamic_filters_received: AtomicU64::new(0),
                dynamic_filters_accepted: AtomicU64::new(0),
                dynamic_filters_unsupported: AtomicU64::new(0),
                dynamic_filter_snapshots: AtomicU64::new(0),
                dynamic_files_not_pruned_missing_metadata: AtomicU64::new(0),
                dynamic_files_not_pruned_unsupported_expression: AtomicU64::new(0),
            }),
        }
    }

    /// Returns the optional registration label supplied by the DataFusion provider.
    pub fn source_name(&self) -> Option<&str> {
        self.inner.source_name.as_deref()
    }

    /// Returns an immutable point-in-time copy of all DataFusion scan metrics.
    pub fn snapshot(&self) -> DeltaDataFusionMetricsSnapshot {
        let inner = self.inner.as_ref();
        DeltaDataFusionMetricsSnapshot {
            reader: inner.reader.snapshot(),
            output_batch_size: nonzero_load(&inner.output_batch_size),
            dynamic_partition_files_pruned: load(&inner.dynamic_partition_files_pruned),
            dynamic_partition_files_kept: load(&inner.dynamic_partition_files_kept),
            dynamic_filters_received: load(&inner.dynamic_filters_received),
            dynamic_filters_accepted: load(&inner.dynamic_filters_accepted),
            dynamic_filters_unsupported: load(&inner.dynamic_filters_unsupported),
            dynamic_filter_snapshots: load(&inner.dynamic_filter_snapshots),
            dynamic_files_not_pruned_missing_metadata: load(
                &inner.dynamic_files_not_pruned_missing_metadata,
            ),
            dynamic_files_not_pruned_unsupported_expression: load(
                &inner.dynamic_files_not_pruned_unsupported_expression,
            ),
        }
    }

    fn record_output_batch_size(&self, value: usize) {
        self.inner
            .output_batch_size
            .store(u64::try_from(value).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    fn record_dynamic_partition_file_pruned(&self) {
        saturating_fetch_add(&self.inner.dynamic_partition_files_pruned, 1);
    }

    fn record_dynamic_partition_file_kept(&self) {
        saturating_fetch_add(&self.inner.dynamic_partition_files_kept, 1);
    }

    fn record_dynamic_filters_received(&self, value: usize) {
        saturating_fetch_add(
            &self.inner.dynamic_filters_received,
            u64::try_from(value).unwrap_or(u64::MAX),
        );
    }

    fn record_dynamic_filters_accepted(&self, value: usize) {
        saturating_fetch_add(
            &self.inner.dynamic_filters_accepted,
            u64::try_from(value).unwrap_or(u64::MAX),
        );
    }

    fn record_dynamic_filters_unsupported(&self, value: usize) {
        saturating_fetch_add(
            &self.inner.dynamic_filters_unsupported,
            u64::try_from(value).unwrap_or(u64::MAX),
        );
    }

    fn record_dynamic_filter_snapshot(&self) {
        saturating_fetch_add(&self.inner.dynamic_filter_snapshots, 1);
    }

    fn record_missing_metadata(&self) {
        saturating_fetch_add(&self.inner.dynamic_files_not_pruned_missing_metadata, 1);
    }

    fn record_unsupported_expression(&self) {
        saturating_fetch_add(
            &self.inner.dynamic_files_not_pruned_unsupported_expression,
            1,
        );
    }

    fn identity(&self) -> usize {
        Arc::as_ptr(&self.inner) as usize
    }
}

impl fmt::Debug for DeltaDataFusionMetrics {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeltaDataFusionMetrics")
            .finish_non_exhaustive()
    }
}

fn load(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}

fn nonzero_load(counter: &AtomicU64) -> Option<u64> {
    match load(counter) {
        0 => None,
        value => Some(value),
    }
}

/// Collects distinct Delta DataFusion scan metrics in depth-first plan order.
pub fn collect_delta_datafusion_metrics(plan: &dyn ExecutionPlan) -> Vec<DeltaDataFusionMetrics> {
    fn collect(
        plan: &dyn ExecutionPlan,
        seen_plans: &mut HashSet<usize>,
        seen_metrics: &mut HashSet<usize>,
        metrics: &mut Vec<DeltaDataFusionMetrics>,
    ) {
        let plan_identity = plan as *const dyn ExecutionPlan as *const () as usize;
        if !seen_plans.insert(plan_identity) {
            return;
        }
        if let Some(scan) = plan.downcast_ref::<DeltaDataFusionExec>() {
            let handle = scan.metrics.clone();
            if seen_metrics.insert(handle.identity()) {
                metrics.push(handle);
            }
        }
        for child in plan.children() {
            collect(child.as_ref(), seen_plans, seen_metrics, metrics);
        }
    }

    let mut metrics = Vec::new();
    collect(plan, &mut HashSet::new(), &mut HashSet::new(), &mut metrics);
    metrics
}

#[allow(dead_code)]
pub(crate) fn create_datafusion_execution_plan(
    plan: DeltaScanPlan,
    planning: DataFusionScanPlanning,
    row_predicate: Option<DeltaKernelPredicate>,
    source_name: Option<String>,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(DeltaDataFusionExec::new(
        plan,
        planning,
        row_predicate,
        source_name,
    ))
}

struct DeltaDataFusionExec {
    plan: Arc<DeltaScanPlan>,
    schema: SchemaRef,
    output_projection: Option<Arc<[usize]>>,
    row_predicate: Option<DeltaKernelPredicate>,
    properties: Arc<PlanProperties>,
    metrics: DeltaDataFusionMetrics,
    limiter: Arc<ScanReadLimiter>,
    dynamic_filters: Arc<[DeltaRetainedDynamicFilter]>,
}

impl DeltaDataFusionExec {
    #[allow(dead_code)]
    fn new(
        plan: DeltaScanPlan,
        planning: DataFusionScanPlanning,
        row_predicate: Option<DeltaKernelPredicate>,
        source_name: Option<String>,
    ) -> Self {
        let schema = planning.projection.output_schema;
        let output_projection = planning.projection.output_projection.map(Arc::from);
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(plan.partitions.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
        .with_scheduling_type(SchedulingType::Cooperative);
        let metrics = DeltaDataFusionMetrics::new(source_name, plan.metrics.clone());
        let limiter = ScanReadLimiter::new(
            plan.execution_options,
            plan.partition_target_diagnostic.target_partitions,
            plan.partitions.len(),
        );

        Self {
            plan: Arc::new(plan),
            schema,
            output_projection,
            row_predicate,
            properties: Arc::new(properties),
            metrics,
            limiter,
            dynamic_filters: Arc::from([]),
        }
    }

    fn with_dynamic_filters(
        &self,
        dynamic_filters: Vec<DeltaRetainedDynamicFilter>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self {
            plan: Arc::clone(&self.plan),
            schema: Arc::clone(&self.schema),
            output_projection: self.output_projection.clone(),
            row_predicate: self.row_predicate.clone(),
            properties: Arc::clone(&self.properties),
            metrics: self.metrics.clone(),
            limiter: Arc::clone(&self.limiter),
            dynamic_filters: Arc::from(dynamic_filters),
        })
    }
}

impl fmt::Debug for DeltaDataFusionExec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeltaDataFusionExec")
            .field("snapshot_version", &self.plan.snapshot_version)
            .field("partition_count", &self.plan.partitions.len())
            .field("dynamic_filter_count", &self.dynamic_filters.len())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for DeltaDataFusionExec {
    fn fmt_as(
        &self,
        display_type: DisplayFormatType,
        formatter: &mut fmt::Formatter,
    ) -> fmt::Result {
        match display_type {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                formatter,
                "DeltaDataFusionExec: snapshot_version={}, partitions={}",
                self.plan.snapshot_version,
                self.plan.partitions.len()
            ),
            DisplayFormatType::TreeRender => write!(formatter, "DeltaDataFusionExec"),
        }
    }
}

impl ExecutionPlan for DeltaDataFusionExec {
    fn name(&self) -> &str {
        "DeltaDataFusionExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "DeltaDataFusionExec does not accept child execution plans".to_owned(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition >= self.plan.partitions.len() {
            return Err(adapter_error("scan_partition_index_out_of_range"));
        }

        let output_batch_size = context.session_config().batch_size();
        self.metrics.record_output_batch_size(output_batch_size);
        let admission = dynamic_admission(self.metrics.clone(), Arc::clone(&self.dynamic_filters));
        let executor = match self.plan.execution_options.reader_backend() {
            DeltaReaderBackend::NativeAsync => native_async_executor(
                &self.plan,
                Some(output_batch_size),
                self.row_predicate.clone(),
            )
            .map_err(datafusion_error)?,
            DeltaReaderBackend::OfficialKernel => {
                official_kernel_executor(&self.plan).map_err(datafusion_error)?
            }
        };
        let stream = DeltaScanExecution::with_shared_limiter(
            Arc::clone(&self.plan),
            Arc::clone(&self.limiter),
        )
        .partition_stream(partition, admission, executor)
        .map_err(datafusion_error)?;
        let schema = Arc::clone(&self.schema);
        let projection = self.output_projection.clone();
        let stream = stream::unfold(
            (Some(stream), projection),
            |(stream, projection)| async move {
                let mut stream = stream?;
                let result = stream.next().await?;
                let result = finalize_output_batch(result, projection.as_deref());
                let stream = result.is_ok().then_some(stream);
                Some((result, (stream, projection)))
            },
        );

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn handle_child_pushdown_result(
        &self,
        phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> DataFusionResult<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        let parent_filters = child_pushdown_result
            .parent_filters
            .iter()
            .map(|result| Arc::clone(&result.filter))
            .collect::<Vec<_>>();
        let unsupported = || {
            FilterPushdownPropagation::with_parent_pushdown_result(vec![
                PushedDown::No;
                parent_filters.len()
            ])
        };
        if phase != FilterPushdownPhase::Post || parent_filters.is_empty() {
            return Ok(unsupported());
        }

        let dynamic_filter_plan = DeltaDynamicFilterPlan::from_filters(
            &parent_filters,
            &self.schema,
            &self.plan.partition_columns,
        );
        let accepted = dynamic_filter_plan.accepted_filters.len();
        self.metrics
            .record_dynamic_filters_received(parent_filters.len());
        self.metrics.record_dynamic_filters_accepted(accepted);
        self.metrics
            .record_dynamic_filters_unsupported(parent_filters.len().saturating_sub(accepted));
        if !dynamic_filter_plan.has_accepted_filters() {
            return Ok(unsupported());
        }

        let pushed = dynamic_filter_plan
            .decisions
            .iter()
            .map(|decision| match decision.outcome {
                DeltaDynamicFilterOutcome::Accepted => PushedDown::Yes,
                DeltaDynamicFilterOutcome::Rejected => PushedDown::No,
            })
            .collect();
        Ok(
            FilterPushdownPropagation::with_parent_pushdown_result(pushed)
                .with_updated_node(self.with_dynamic_filters(dynamic_filter_plan.accepted_filters)),
        )
    }
}

fn dynamic_admission(
    metrics: DeltaDataFusionMetrics,
    filters: Arc<[DeltaRetainedDynamicFilter]>,
) -> FileAdmissionFn<DeltaScanFileTask> {
    Arc::new(move |task| {
        if filters.is_empty() {
            return Ok(FileAdmission::Admit);
        }

        let mut missing_metadata = false;
        let mut unsupported_expression = false;
        for filter in filters.iter() {
            metrics.record_dynamic_filter_snapshot();
            match evaluate_dynamic_partition_filter(filter, task) {
                DeltaDynamicPartitionPruningDecision::Prune(_) => {
                    metrics.record_dynamic_partition_file_pruned();
                    return Ok(FileAdmission::Skip);
                }
                DeltaDynamicPartitionPruningDecision::Keep(reason) => {
                    missing_metadata |= is_missing_metadata(reason);
                    unsupported_expression |= is_unsupported_expression(reason);
                }
            }
        }
        if missing_metadata {
            metrics.record_missing_metadata();
        }
        if unsupported_expression {
            metrics.record_unsupported_expression();
        }
        metrics.record_dynamic_partition_file_kept();
        Ok(FileAdmission::Admit)
    })
}

fn is_missing_metadata(reason: DeltaDynamicPartitionKeepReason) -> bool {
    matches!(
        reason,
        DeltaDynamicPartitionKeepReason::PartitionMetadataInvalid
            | DeltaDynamicPartitionKeepReason::PartitionValueMissing
            | DeltaDynamicPartitionKeepReason::PartitionValueUnparseable
    )
}

fn is_unsupported_expression(reason: DeltaDynamicPartitionKeepReason) -> bool {
    matches!(
        reason,
        DeltaDynamicPartitionKeepReason::SnapshotUnavailable
            | DeltaDynamicPartitionKeepReason::UnsupportedPartitionType
            | DeltaDynamicPartitionKeepReason::EvaluationFailed
            | DeltaDynamicPartitionKeepReason::NonBooleanResult
    )
}

fn project_output_batch(
    batch: RecordBatch,
    projection: Option<&[usize]>,
) -> Result<RecordBatch, arrow::error::ArrowError> {
    match projection {
        Some(projection) => batch.project(projection),
        None => Ok(batch),
    }
}

fn finalize_output_batch(
    result: Result<RecordBatch, DeltaReaderError>,
    projection: Option<&[usize]>,
) -> DataFusionResult<RecordBatch> {
    let batch = result.map_err(datafusion_error)?;
    project_output_batch(batch, projection).map_err(|source| {
        datafusion_error(DeltaReaderError::DataFusionAdapter {
            reason: "scan_output_projection_failed",
            source: Box::new(DataFusionError::from(source)),
        })
    })
}

fn datafusion_error(error: DeltaReaderError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

fn adapter_error(reason: &'static str) -> DataFusionError {
    datafusion_error(DeltaReaderError::DataFusionAdapter {
        reason,
        source: Box::new(DataFusionError::Execution(reason.to_owned())),
    })
}

#[cfg(all(test, feature = "native-async"))]
mod tests {
    use std::{
        collections::HashSet,
        error::Error,
        fs,
        path::{Path, PathBuf},
        thread,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[cfg(feature = "official-kernel")]
    use arrow::array::StringArray;
    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    #[cfg(feature = "official-kernel")]
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::{
        common::config::ConfigOptions,
        logical_expr::{Operator, col, lit},
        physical_expr::expressions::{
            BinaryExpr, Column, DynamicFilterPhysicalExpr, lit as physical_lit,
        },
        physical_plan::{
            ExecutionPlan,
            filter_pushdown::{
                ChildFilterPushdownResult, ChildPushdownResult, FilterPushdownPhase, PushedDown,
            },
            union::UnionExec,
        },
        prelude::{SessionConfig, SessionContext},
    };
    use futures_util::StreamExt;
    use parquet::arrow::ArrowWriter;
    use serde_json::{Value, json};

    use super::*;
    use crate::{
        DeltaReaderExecutionOptions, DeltaTable, DeltaTableBuilder,
        datafusion_planning::{DataFusionFilterCapabilities, plan_datafusion_scan},
        kernel::delta_predicate_to_kernel_pruning,
        planning::{DeltaScanPartitionTargetOptions, plan_row_predicate, plan_scan},
    };

    type TestResult<T = ()> = Result<T, Box<dyn Error>>;

    struct TestTable(PathBuf);

    impl TestTable {
        fn empty(name: &str) -> TestResult<Self> {
            let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
            let path = Path::new("target")
                .join("delta-arrow-reader-datafusion-tests")
                .join(format!("{}-{name}-{nanos}", std::process::id()));
            fs::create_dir_all(path.join("_delta_log"))?;
            let table = Self(path);
            table.write_log(&[protocol(), metadata()])?;
            Ok(table)
        }

        fn partitioned(name: &str) -> TestResult<Self> {
            let table = Self::empty(name)?;
            let west = table.write_parquet("west.parquet", &[1, 2])?;
            let east = table.write_parquet("east.parquet", &[3, 4])?;
            table.write_log(&[
                protocol(),
                metadata(),
                add("west.parquet", west, "west", 2, 1, 2),
                add("east.parquet", east, "east", 2, 3, 4),
            ])?;
            Ok(table)
        }

        fn late_dynamic(name: &str) -> TestResult<Self> {
            let table = Self::empty(name)?;
            let west = table.write_parquet("west.parquet", &[1, 2, 3])?;
            let east = table.write_parquet("east.parquet", &[4, 5])?;
            table.write_log(&[
                protocol(),
                metadata(),
                add("west.parquet", west, "west", 3, 1, 3),
                add("east.parquet", east, "east", 2, 4, 5),
            ])?;
            Ok(table)
        }

        fn missing(name: &str) -> TestResult<Self> {
            let table = Self::partitioned(name)?;
            table.write_log(&[
                protocol(),
                metadata(),
                add("missing.parquet", 100, "west", 1, 1, 1),
            ])?;
            Ok(table)
        }

        fn uri(&self) -> String {
            self.0.to_string_lossy().into_owned()
        }

        fn write_parquet(&self, name: &str, ids: &[i32]) -> TestResult<u64> {
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(ids.to_vec()))],
            )?;
            let path = self.0.join(name);
            let mut writer = ArrowWriter::try_new(fs::File::create(&path)?, schema, None)?;
            writer.write(&batch)?;
            writer.close()?;
            Ok(fs::metadata(path)?.len())
        }

        fn write_log(&self, actions: &[Value]) -> TestResult {
            let contents = actions
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n");
            fs::write(
                self.0.join("_delta_log/00000000000000000000.json"),
                format!("{contents}\n"),
            )?;
            Ok(())
        }
    }

    impl Drop for TestTable {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn protocol() -> Value {
        json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}})
    }

    fn metadata() -> Value {
        let schema = json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": false, "metadata": {}},
                {"name": "region", "type": "string", "nullable": true, "metadata": {}}
            ]
        });
        json!({
            "metaData": {
                "id": "delta-arrow-reader-datafusion-test",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema.to_string(),
                "partitionColumns": ["region"],
                "configuration": {},
                "createdTime": 1587968585495_i64
            }
        })
    }

    fn add(
        path: &str,
        size: u64,
        region: &str,
        num_records: u64,
        min_id: i32,
        max_id: i32,
    ) -> Value {
        let stats = json!({
            "numRecords": num_records,
            "minValues": {"id": min_id},
            "maxValues": {"id": max_id},
            "nullCount": {"id": 0}
        });
        json!({
            "add": {
                "path": path,
                "partitionValues": {"region": region},
                "size": size,
                "modificationTime": 1587968586000_i64,
                "dataChange": true,
                "stats": stats.to_string()
            }
        })
    }

    fn build_plan(
        table: &DeltaTable,
        projection: Option<&[usize]>,
        filters: &[datafusion::logical_expr::Expr],
        target_partitions: usize,
        execution_options: DeltaReaderExecutionOptions,
        source_name: Option<String>,
    ) -> Result<Arc<dyn ExecutionPlan>, DeltaReaderError> {
        let partition_columns = table
            .partition_columns()
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let planning = plan_datafusion_scan(
            table.schema(),
            &partition_columns,
            projection,
            &filter_refs,
            DataFusionFilterCapabilities {
                exact_predicate_evaluation: execution_options.reader_backend()
                    == DeltaReaderBackend::NativeAsync,
            },
        )?;
        let physical_projection = planning.projection.physical_projection.clone();
        let hidden_columns = planning.projection.hidden_columns.clone();
        let kernel_predicate = planning
            .filters
            .predicate
            .as_ref()
            .and_then(delta_predicate_to_kernel_pruning);
        let row_predicate = match planning.filters.row_predicate.as_ref() {
            Some(predicate) => Some(delta_predicate_to_kernel_pruning(predicate).ok_or(
                DeltaReaderError::UnsupportedPredicate {
                    reason: "exact_row_predicate_not_kernel_safe",
                },
            )?),
            None => None,
        };
        let row_predicate = plan_row_predicate(
            table.snapshot(),
            physical_projection.as_deref(),
            &hidden_columns,
            row_predicate,
        )?;
        let include_stats = planning.filters.requires_statistics;
        let core = plan_scan(
            table.snapshot(),
            physical_projection.as_deref(),
            &hidden_columns,
            kernel_predicate,
            include_stats,
            execution_options,
            DeltaScanPartitionTargetOptions {
                explicit_target_partitions: Some(target_partitions),
                caller_target_partitions: None,
            },
        )?;
        Ok(create_datafusion_execution_plan(
            core,
            planning,
            row_predicate,
            source_name,
        ))
    }

    fn session(batch_size: usize) -> SessionContext {
        SessionContext::new_with_config(SessionConfig::new().with_batch_size(batch_size))
    }

    fn ids(batches: &[RecordBatch]) -> Vec<i32> {
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(batch.schema().index_of("id").expect("id column"))
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("Int32 id")
                    .values()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn dynamic_filter(name: &str, index: usize) -> Arc<DynamicFilterPhysicalExpr> {
        Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new(name, index))],
            physical_lit(true),
        ))
    }

    fn hook_input(
        filters: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>>,
    ) -> ChildPushdownResult {
        ChildPushdownResult {
            parent_filters: filters
                .into_iter()
                .map(|filter| ChildFilterPushdownResult {
                    filter,
                    child_results: Vec::new(),
                })
                .collect(),
            self_filters: Vec::new(),
        }
    }

    #[tokio::test]
    #[cfg(feature = "native-async")]
    async fn properties_projection_partitions_metrics_and_reexecution_match_provider_behavior()
    -> TestResult {
        let fixture = TestTable::partitioned("properties")?;
        let table = DeltaTableBuilder::new(fixture.uri()).load()?;
        let logical_filter = col("id").gt(lit(1_i32));
        let plan = build_plan(
            &table,
            Some(&[1, 0]),
            &[logical_filter],
            2,
            DeltaReaderExecutionOptions::new(),
            None,
        )?;

        assert_eq!(plan.name(), "DeltaDataFusionExec");
        assert!(plan.children().is_empty());
        assert!(plan.metrics().is_none());
        assert_eq!(plan.schema().fields().len(), 2);
        assert_eq!(plan.schema().field(0).name(), "region");
        assert_eq!(plan.schema().field(1).name(), "id");
        assert_eq!(plan.properties().output_partitioning().partition_count(), 2);
        assert_eq!(
            plan.partition_statistics(None)?,
            Arc::new(datafusion::common::Statistics::new_unknown(&plan.schema()))
        );
        let context = session(1);
        let first =
            datafusion::physical_plan::collect(Arc::clone(&plan), context.task_ctx()).await?;
        assert!(first.iter().all(|batch| batch.num_rows() <= 1));
        let mut first_ids = ids(&first);
        first_ids.sort_unstable();
        assert_eq!(first_ids, [2, 3, 4]);

        let second =
            datafusion::physical_plan::collect(Arc::clone(&plan), context.task_ctx()).await?;
        let mut second_ids = ids(&second);
        second_ids.sort_unstable();
        assert_eq!(second_ids, first_ids);
        let handles = collect_delta_datafusion_metrics(plan.as_ref());
        assert_eq!(handles.len(), 1);
        assert_eq!(handles[0].source_name(), None);
        let metrics = handles[0].snapshot();
        assert_eq!(metrics.output_batch_size, Some(1));
        assert_eq!(metrics.reader.scan_partitions_started, 4);
        assert_eq!(metrics.reader.files_completed, 4);
        assert_eq!(metrics.reader.rows_produced, 6);

        let hidden = build_plan(
            &table,
            Some(&[1]),
            &[col("id").gt(lit(1_i32))],
            1,
            DeltaReaderExecutionOptions::new(),
            None,
        )?;
        let hidden_batches = datafusion::physical_plan::collect(
            Arc::clone(&hidden),
            SessionContext::new().task_ctx(),
        )
        .await?;
        assert_eq!(hidden.schema().fields().len(), 1);
        assert_eq!(hidden.schema().field(0).name(), "region");
        assert!(hidden_batches.iter().all(|batch| batch.num_columns() == 1));
        assert_eq!(
            hidden_batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            3
        );

        let partition_filter = build_plan(
            &table,
            None,
            &[col("region").eq(lit("west"))],
            2,
            DeltaReaderExecutionOptions::new(),
            None,
        )?;
        let partition_batches = datafusion::physical_plan::collect(
            Arc::clone(&partition_filter),
            SessionContext::new().task_ctx(),
        )
        .await?;
        assert_eq!(ids(&partition_batches), [1, 2]);
        assert_eq!(
            collect_delta_datafusion_metrics(partition_filter.as_ref())[0]
                .snapshot()
                .reader
                .files_started,
            1
        );

        let empty = build_plan(
            &table,
            Some(&[]),
            &[],
            1,
            DeltaReaderExecutionOptions::new(),
            None,
        )?;
        let empty_batches = datafusion::physical_plan::collect(
            Arc::clone(&empty),
            SessionContext::new().task_ctx(),
        )
        .await?;
        assert!(empty.schema().fields().is_empty());
        assert!(empty_batches.iter().all(|batch| batch.num_columns() == 0));
        assert_eq!(
            empty_batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            4
        );

        let empty_fixture = TestTable::empty("empty-scan")?;
        let empty_table = DeltaTableBuilder::new(empty_fixture.uri()).load()?;
        let empty_plan = build_plan(
            &empty_table,
            None,
            &[],
            1,
            DeltaReaderExecutionOptions::new(),
            None,
        )?;
        assert_eq!(
            empty_plan
                .properties()
                .output_partitioning()
                .partition_count(),
            0
        );
        assert!(
            datafusion::physical_plan::collect(empty_plan, SessionContext::new().task_ctx(),)
                .await?
                .is_empty()
        );

        let invalid = plan.execute(2, context.task_ctx());
        let error = match invalid {
            Ok(_) => return Err("out-of-range partition unexpectedly executed".into()),
            Err(error) => error,
        };
        let DataFusionError::External(source) = error else {
            return Err("invalid partition did not preserve the reader error".into());
        };
        let reader = source
            .downcast_ref::<DeltaReaderError>()
            .ok_or("external error was not DeltaReaderError")?;
        assert_eq!(reader.as_str(), "data_fusion_adapter");
        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "native-async")]
    async fn dynamic_filter_hook_prunes_before_file_start_and_counts_once() -> TestResult {
        let fixture = TestTable::partitioned("dynamic")?;
        let table = DeltaTableBuilder::new(fixture.uri()).load()?;
        let plan = build_plan(
            &table,
            None,
            &[],
            1,
            DeltaReaderExecutionOptions::new(),
            None,
        )?;
        let dynamic = dynamic_filter("region", 1);
        let physical: Arc<dyn datafusion::physical_plan::PhysicalExpr> = dynamic.clone();
        let rejected: Arc<dyn datafusion::physical_plan::PhysicalExpr> = dynamic_filter("id", 0);
        let pushed = plan.handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            hook_input(vec![physical, rejected]),
            &ConfigOptions::new(),
        )?;
        assert!(matches!(
            pushed.filters.as_slice(),
            [PushedDown::Yes, PushedDown::No]
        ));
        let updated = pushed.updated_node.ok_or("dynamic plan was not retained")?;
        dynamic.update(Arc::new(BinaryExpr::new(
            Arc::new(Column::new("region", 1)),
            Operator::Eq,
            physical_lit("west"),
        )))?;

        let batches = datafusion::physical_plan::collect(
            Arc::clone(&updated),
            SessionContext::new().task_ctx(),
        )
        .await?;
        assert_eq!(ids(&batches), [1, 2]);
        let metrics = collect_delta_datafusion_metrics(updated.as_ref())
            .pop()
            .ok_or("missing dynamic metrics")?
            .snapshot();
        assert_eq!(metrics.dynamic_filters_received, 2);
        assert_eq!(metrics.dynamic_filters_accepted, 1);
        assert_eq!(metrics.dynamic_filters_unsupported, 1);
        assert_eq!(metrics.dynamic_filter_snapshots, 2);
        assert_eq!(metrics.dynamic_partition_files_pruned, 1);
        assert_eq!(metrics.dynamic_partition_files_kept, 1);
        assert_eq!(metrics.reader.files_started, 1);
        assert_eq!(metrics.reader.files_completed, 1);
        assert_eq!(
            collect_delta_datafusion_metrics(plan.as_ref())[0]
                .snapshot()
                .dynamic_filters_received,
            2
        );
        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "native-async")]
    async fn physical_pushdown_preserves_dynamic_filters_across_plan_rebuild() -> TestResult {
        let fixture = TestTable::partitioned("dynamic-plan-rebuild")?;
        let table = DeltaTableBuilder::new(fixture.uri()).load()?;
        let plan = build_plan(
            &table,
            None,
            &[],
            1,
            DeltaReaderExecutionOptions::new(),
            None,
        )?;
        let physical: Arc<dyn datafusion::physical_plan::PhysicalExpr> =
            dynamic_filter("region", 1);
        let pushed = plan.handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            hook_input(vec![physical]),
            &ConfigOptions::new(),
        )?;
        let updated = pushed.updated_node.ok_or("expected updated scan")?;
        let rebuilt = Arc::clone(&updated).with_new_children(Vec::new())?;
        let reset = updated.reset_state()?;

        for candidate in [&rebuilt, &reset] {
            let debug = format!("{candidate:?}");
            assert!(debug.contains("dynamic_filter_count: 1"), "{debug}");
        }
        let display = datafusion::physical_plan::displayable(rebuilt.as_ref())
            .one_line()
            .to_string();
        assert!(display.contains("DeltaDataFusionExec:"), "{display}");
        assert!(display.contains("partitions="), "{display}");
        assert!(!display.contains("DynamicFilter"), "{display}");
        assert!(
            Arc::clone(&rebuilt)
                .with_new_children(vec![Arc::clone(&rebuilt)])
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "native-async")]
    async fn late_dynamic_filter_keeps_admitted_file_and_prunes_the_next() -> TestResult {
        let fixture = TestTable::late_dynamic("late-dynamic")?;
        let table = DeltaTableBuilder::new(fixture.uri()).load()?;
        let options = DeltaReaderExecutionOptions::new()
            .with_native_async_prefetch_file_count_per_partition(0)?
            .with_max_concurrent_file_reads_per_partition(1)?
            .with_max_concurrent_file_reads_per_scan(Some(1))?
            .with_output_buffer_capacity_per_partition(1)?;
        let plan = build_plan(&table, None, &[], 1, options, None)?;
        let dynamic = dynamic_filter("region", 1);
        let physical: Arc<dyn datafusion::physical_plan::PhysicalExpr> = dynamic.clone();
        let pushed = plan.handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            hook_input(vec![physical]),
            &ConfigOptions::new(),
        )?;
        let updated = pushed.updated_node.ok_or("dynamic plan was not retained")?;
        let mut stream = updated.execute(0, session(1).task_ctx())?;
        let first = stream.next().await.ok_or("missing first batch")??;
        assert_eq!(ids(std::slice::from_ref(&first)), [1]);

        dynamic.update(Arc::new(BinaryExpr::new(
            Arc::new(Column::new("region", 1)),
            Operator::Eq,
            physical_lit("none"),
        )))?;
        let mut batches = vec![first];
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }

        assert_eq!(ids(&batches), [1, 2, 3]);
        let metrics = collect_delta_datafusion_metrics(updated.as_ref())[0].snapshot();
        assert_eq!(metrics.dynamic_filter_snapshots, 2);
        assert_eq!(metrics.dynamic_partition_files_kept, 1);
        assert_eq!(metrics.dynamic_partition_files_pruned, 1);
        assert_eq!(metrics.reader.files_started, 1);
        assert_eq!(metrics.reader.files_completed, 1);
        Ok(())
    }

    #[test]
    #[cfg(feature = "native-async")]
    fn hook_is_post_only_empty_safe_and_collector_is_ordered_and_distinct() -> TestResult {
        let fixture = TestTable::partitioned("collector")?;
        let table = DeltaTableBuilder::new(fixture.uri()).load()?;
        let first = build_plan(
            &table,
            None,
            &[],
            1,
            DeltaReaderExecutionOptions::new(),
            Some("first".to_owned()),
        )?;
        let second = build_plan(
            &table,
            None,
            &[],
            1,
            DeltaReaderExecutionOptions::new(),
            Some("second".to_owned()),
        )?;
        let dynamic = dynamic_filter("region", 1);
        let physical: Arc<dyn datafusion::physical_plan::PhysicalExpr> = dynamic;
        let pre = first.handle_child_pushdown_result(
            FilterPushdownPhase::Pre,
            hook_input(vec![physical]),
            &ConfigOptions::new(),
        )?;
        assert!(pre.updated_node.is_none());
        assert!(matches!(pre.filters.as_slice(), [PushedDown::No]));
        let empty = first.handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            hook_input(Vec::new()),
            &ConfigOptions::new(),
        )?;
        assert!(empty.updated_node.is_none());

        let union: Arc<dyn ExecutionPlan> = UnionExec::try_new(vec![
            Arc::clone(&first),
            Arc::clone(&second),
            Arc::clone(&first),
        ])?;
        let handles = collect_delta_datafusion_metrics(union.as_ref());
        assert_eq!(handles.len(), 2);
        assert_eq!(handles[0].source_name(), Some("first"));
        assert_eq!(handles[1].source_name(), Some("second"));
        assert!(!format!("{:?}", handles[0]).contains("first"));
        let initial = handles[0].snapshot();
        assert_eq!(initial.output_batch_size, None);
        assert_eq!(
            [
                initial.dynamic_partition_files_pruned,
                initial.dynamic_partition_files_kept,
                initial.dynamic_filters_received,
                initial.dynamic_filters_accepted,
                initial.dynamic_filters_unsupported,
                initial.dynamic_filter_snapshots,
                initial.dynamic_files_not_pruned_missing_metadata,
                initial.dynamic_files_not_pruned_unsupported_expression,
            ],
            [0; 8]
        );

        let accepted: Arc<dyn datafusion::physical_plan::PhysicalExpr> =
            dynamic_filter("region", 1);
        let updated = first
            .handle_child_pushdown_result(
                FilterPushdownPhase::Post,
                hook_input(vec![accepted]),
                &ConfigOptions::new(),
            )?
            .updated_node
            .ok_or("expected updated scan")?;
        let shared_metrics_union: Arc<dyn ExecutionPlan> =
            UnionExec::try_new(vec![updated, Arc::clone(&first), Arc::clone(&second)])?;
        let shared_handles = collect_delta_datafusion_metrics(shared_metrics_union.as_ref());
        assert_eq!(shared_handles.len(), 2);
        assert_eq!(shared_handles[0].source_name(), Some("first"));
        assert_eq!(shared_handles[1].source_name(), Some("second"));

        drop(shared_metrics_union);
        drop(union);
        drop(first);
        drop(second);
        assert_eq!(handles[0].snapshot().reader.files_started, 0);
        assert_eq!(handles[0].source_name(), Some("first"));
        Ok(())
    }

    #[test]
    #[cfg(feature = "native-async")]
    fn dynamic_admission_reason_counts_are_once_per_file_and_saturating() -> TestResult {
        use crate::{
            datafusion_dynamic_filters::DeltaDynamicFilterPlan,
            deletion_vector::DeletionVectorMetadata, kernel::KernelPhysicalToLogicalTransform,
        };

        let fixture = TestTable::partitioned("dynamic-counters")?;
        let table = DeltaTableBuilder::new(fixture.uri()).load()?;
        let plan = build_plan(
            &table,
            None,
            &[],
            1,
            DeltaReaderExecutionOptions::new(),
            None,
        )?;
        let metrics = collect_delta_datafusion_metrics(plan.as_ref())
            .pop()
            .ok_or("missing metrics")?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("region", DataType::Utf8, true),
        ]));
        let retained = |dynamic: Arc<DynamicFilterPhysicalExpr>| -> TestResult<_> {
            let physical: Arc<dyn datafusion::physical_plan::PhysicalExpr> = dynamic;
            Ok(DeltaDynamicFilterPlan::from_filters(
                std::slice::from_ref(&physical),
                &schema,
                &["region".to_owned()],
            )
            .accepted_filters
            .into_iter()
            .next()
            .ok_or("dynamic filter was not retained")?)
        };
        let first = retained(dynamic_filter("region", 1))?;
        let second = retained(dynamic_filter("region", 1))?;
        let missing = DeltaScanFileTask {
            path: "missing-partition.parquet".to_owned(),
            estimated_bytes: None,
            estimated_rows: None,
            stats: None,
            modification_time_ms: None,
            partition_values: Default::default(),
            deletion_vector: DeletionVectorMetadata::default(),
            transform: KernelPhysicalToLogicalTransform::default(),
        };
        assert_eq!(
            dynamic_admission(metrics.clone(), Arc::from([first, second]))(&missing)?,
            FileAdmission::Admit
        );
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.dynamic_filter_snapshots, 2);
        assert_eq!(snapshot.dynamic_partition_files_kept, 1);
        assert_eq!(snapshot.dynamic_files_not_pruned_missing_metadata, 1);

        let rejecting = dynamic_filter("region", 1);
        rejecting.update(physical_lit(false))?;
        let first = retained(rejecting)?;
        let second = retained(dynamic_filter("region", 1))?;
        let mut present = missing.clone();
        present
            .partition_values
            .insert("region".to_owned(), "west".to_owned());
        assert_eq!(
            dynamic_admission(metrics.clone(), Arc::from([first, second]))(&present)?,
            FileAdmission::Skip
        );
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.dynamic_filter_snapshots, 3);
        assert_eq!(snapshot.dynamic_partition_files_pruned, 1);
        assert_eq!(snapshot.dynamic_partition_files_kept, 1);
        assert_eq!(snapshot.dynamic_files_not_pruned_missing_metadata, 1);

        let unsupported = dynamic_filter("region", 1);
        unsupported.update(physical_lit("not boolean"))?;
        assert_eq!(
            dynamic_admission(metrics.clone(), Arc::from([retained(unsupported)?]))(&present)?,
            FileAdmission::Admit
        );
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.dynamic_filter_snapshots, 4);
        assert_eq!(snapshot.dynamic_partition_files_kept, 2);
        assert_eq!(snapshot.dynamic_files_not_pruned_unsupported_expression, 1);

        metrics
            .inner
            .dynamic_filters_received
            .store(u64::MAX - 1, Ordering::Relaxed);
        metrics.record_dynamic_filters_received(2);
        metrics.record_dynamic_filters_received(1);
        assert_eq!(metrics.snapshot().dynamic_filters_received, u64::MAX);
        Ok(())
    }

    #[test]
    #[cfg(feature = "native-async")]
    fn dynamic_metrics_updates_are_thread_safe() -> TestResult {
        const THREADS: usize = 4;
        const ITERATIONS: usize = 100;

        let fixture = TestTable::partitioned("dynamic-metrics-concurrency")?;
        let table = DeltaTableBuilder::new(fixture.uri()).load()?;
        let plan = build_plan(
            &table,
            None,
            &[],
            1,
            DeltaReaderExecutionOptions::new(),
            None,
        )?;
        let metrics = collect_delta_datafusion_metrics(plan.as_ref())
            .pop()
            .ok_or("missing metrics")?;
        let mut handles = Vec::new();

        for _ in 0..THREADS {
            let metrics = metrics.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..ITERATIONS {
                    metrics.record_dynamic_partition_file_pruned();
                    metrics.record_dynamic_partition_file_kept();
                    metrics.record_dynamic_filters_received(3);
                    metrics.record_dynamic_filters_accepted(1);
                    metrics.record_dynamic_filters_unsupported(2);
                    metrics.record_dynamic_filter_snapshot();
                    metrics.record_missing_metadata();
                    metrics.record_unsupported_expression();
                }
            }));
        }
        for handle in handles {
            handle.join().map_err(|_| "metrics worker panicked")?;
        }

        let calls = u64::try_from(THREADS * ITERATIONS)?;
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.dynamic_partition_files_pruned, calls);
        assert_eq!(snapshot.dynamic_partition_files_kept, calls);
        assert_eq!(snapshot.dynamic_filters_received, calls * 3);
        assert_eq!(snapshot.dynamic_filters_accepted, calls);
        assert_eq!(snapshot.dynamic_filters_unsupported, calls * 2);
        assert_eq!(snapshot.dynamic_filter_snapshots, calls);
        assert_eq!(snapshot.dynamic_files_not_pruned_missing_metadata, calls);
        assert_eq!(
            snapshot.dynamic_files_not_pruned_unsupported_expression,
            calls
        );
        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "native-async")]
    async fn execution_error_and_stream_drop_preserve_partial_metrics() -> TestResult {
        let missing_fixture = TestTable::missing("error")?;
        let missing_table = DeltaTableBuilder::new(missing_fixture.uri()).load()?;
        let missing_plan = build_plan(
            &missing_table,
            None,
            &[],
            1,
            DeltaReaderExecutionOptions::new(),
            None,
        )?;
        let result = datafusion::physical_plan::collect(
            Arc::clone(&missing_plan),
            SessionContext::new().task_ctx(),
        )
        .await;
        let error = result.expect_err("missing file must fail");
        assert!(matches!(&error, DataFusionError::External(_)));
        assert!(!error.to_string().contains("missing.parquet"));
        let failed = collect_delta_datafusion_metrics(missing_plan.as_ref())
            .pop()
            .ok_or("missing failure metrics")?;
        assert_eq!(failed.snapshot().reader.files_started, 1);

        let fixture = TestTable::partitioned("drop")?;
        let table = DeltaTableBuilder::new(fixture.uri()).load()?;
        let options = DeltaReaderExecutionOptions::new()
            .with_native_async_prefetch_file_count_per_partition(0)?
            .with_max_concurrent_file_reads_per_partition(1)?
            .with_max_concurrent_file_reads_per_scan(Some(1))?
            .with_output_buffer_capacity_per_partition(1)?;
        let drop_plan = build_plan(&table, None, &[], 1, options, None)?;
        let handle = collect_delta_datafusion_metrics(drop_plan.as_ref())
            .pop()
            .ok_or("missing drop metrics")?;
        let mut stream = drop_plan.execute(0, SessionContext::new().task_ctx())?;
        assert!(stream.next().await.transpose()?.is_some());
        drop(stream);
        tokio::task::yield_now().await;
        let stable = handle.snapshot();
        tokio::task::yield_now().await;
        assert_eq!(handle.snapshot(), stable);
        assert!(stable.reader.files_started >= 1);
        let retry = datafusion::physical_plan::collect(
            Arc::clone(&drop_plan),
            SessionContext::new().task_ctx(),
        )
        .await?;
        assert_eq!(ids(&retry), [1, 2, 3, 4]);
        Ok(())
    }

    #[tokio::test]
    #[cfg(all(feature = "native-async", feature = "official-kernel"))]
    async fn reader_backends_produce_the_same_logical_rows() -> TestResult {
        let fixture = TestTable::partitioned("backends")?;
        let table = DeltaTableBuilder::new(fixture.uri()).load()?;
        let mut outputs = Vec::new();
        for backend in [
            DeltaReaderBackend::NativeAsync,
            DeltaReaderBackend::OfficialKernel,
        ] {
            let options = DeltaReaderExecutionOptions::new().with_reader_backend(backend)?;
            let plan = build_plan(&table, Some(&[1, 0]), &[], 2, options, None)?;
            let mut batches =
                datafusion::physical_plan::collect(plan, SessionContext::new().task_ctx()).await?;
            batches.sort_by_key(|batch| {
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("Int32 id")
                    .value(0)
            });
            outputs.push(
                batches
                    .iter()
                    .flat_map(|batch| {
                        let ids = batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .expect("Int32 id");
                        let regions = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .expect("Utf8 region");
                        (0..batch.num_rows())
                            .map(|row| (regions.value(row).to_owned(), ids.value(row)))
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>(),
            );
        }
        assert_eq!(outputs[0], outputs[1]);

        let official_options = DeltaReaderExecutionOptions::new()
            .with_reader_backend(DeltaReaderBackend::OfficialKernel)?;
        let inexact = build_plan(
            &table,
            None,
            &[col("id").gt(lit(1_i32))],
            1,
            official_options,
            None,
        )?;
        let unfiltered = datafusion::physical_plan::collect(
            Arc::clone(&inexact),
            SessionContext::new().task_ctx(),
        )
        .await?;
        assert_eq!(ids(&unfiltered), [1, 2, 3, 4]);

        let residual: Arc<dyn datafusion::physical_plan::PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("id", 0)),
            Operator::Gt,
            physical_lit(1_i32),
        ));
        let residual_plan: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(residual, inexact)?);
        let filtered =
            datafusion::physical_plan::collect(residual_plan, SessionContext::new().task_ctx())
                .await?;
        assert_eq!(ids(&filtered), [2, 3, 4]);
        Ok(())
    }
}
