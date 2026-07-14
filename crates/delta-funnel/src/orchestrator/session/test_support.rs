use std::{
    any::Any,
    fmt, fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{Array, ArrayRef, StringArray},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    catalog::Session,
    datasource::{MemTable, TableProvider},
    error::{DataFusionError, Result as DataFusionResult},
    execution::TaskContext,
    logical_expr::{Expr, TableType},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
        test::exec::{BlockingExec, ErrorExec},
    },
};
use futures_util::StreamExt;

use crate::{
    DeltaFunnelError, LoadMode, MssqlConnectionConfig, MssqlOutputBatchStream, MssqlTargetConfig,
    MssqlTargetTable,
};

use super::{LazyTable, MssqlOutputTarget, OutputWritePlan, RunMode};

/// Keeps a real plan as a child but fails before returning its output stream.
#[derive(Debug)]
pub(super) struct StreamSetupFailingPlan {
    child: Arc<dyn ExecutionPlan>,
    error: ErrorExec,
}

impl StreamSetupFailingPlan {
    pub(super) fn new(child: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            child,
            error: ErrorExec::new(),
        }
    }
}

impl DisplayAs for StreamSetupFailingPlan {
    fn fmt_as(
        &self,
        _display_type: DisplayFormatType,
        formatter: &mut fmt::Formatter,
    ) -> fmt::Result {
        formatter.write_str("StreamSetupFailingPlan")
    }
}

impl ExecutionPlan for StreamSetupFailingPlan {
    fn name(&self) -> &str {
        "StreamSetupFailingPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.error.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "StreamSetupFailingPlan requires one child".to_owned(),
            ));
        }
        Ok(Arc::new(Self::new(Arc::clone(&children[0]))))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        self.error.execute(partition, context)
    }
}

pub(super) struct DeltaLogTable {
    path: PathBuf,
}

impl Drop for DeltaLogTable {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

impl DeltaLogTable {
    pub(super) fn new(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_and_schema(name, PROTOCOL_JSON, DEFAULT_SCHEMA_FIELDS_JSON)
    }

    pub(super) fn new_with_protocol(
        name: &str,
        protocol_json: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_and_schema(name, protocol_json, DEFAULT_SCHEMA_FIELDS_JSON)
    }

    pub(super) fn new_with_schema(
        name: &str,
        schema_fields_json: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_and_schema(name, PROTOCOL_JSON, schema_fields_json)
    }

    fn new_with_protocol_and_schema(
        name: &str,
        protocol_json: &str,
        schema_fields_json: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let path = Path::new("target")
            .join("delta-funnel-orchestrator-tests")
            .join(unique_name(name)?);
        let log_path = path.join("_delta_log");
        fs::create_dir_all(&log_path)?;
        fs::write(
            log_path.join("00000000000000000000.json"),
            format!("{}\n{}\n", protocol_json, metadata_json(schema_fields_json)),
        )?;
        fs::write(
            log_path.join("00000000000000000001.json"),
            format!("{}\n", add_json("part-00000.parquet")),
        )?;

        Ok(Self { path })
    }

    pub(super) fn uri(&self) -> String {
        self.path.to_string_lossy().to_string()
    }

    pub(super) fn file_uri_with_secret_parts(&self) -> Result<String, Box<dyn std::error::Error>> {
        let path = fs::canonicalize(&self.path)?;

        Ok(format!(
            "file://{}?token=super-secret#debug-secret",
            path.to_string_lossy()
        ))
    }
}

const PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
const DEFAULT_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;
pub(super) const UNSUPPORTED_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":true},\"nullable\":true,\"metadata\":{}}]"#;

fn metadata_json(schema_fields_json: &str) -> String {
    format!(
        r#"{{"metaData":{{"id":"delta-funnel-test","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{{\"type\":\"struct\",\"fields\":{schema_fields_json}}}","partitionColumns":[],"configuration":{{}},"createdTime":1587968585495}}}}"#
    )
}

fn add_json(path: &str) -> String {
    format!(
        r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":0,"modificationTime":1587968586000,"dataChange":true}}}}"#
    )
}

fn unique_name(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(format!("{}-{name}-{nanos}", std::process::id()))
}

pub(super) fn marker_region_provider(
    marker: &str,
) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("marker", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![marker, marker])) as ArrayRef,
            Arc::new(StringArray::from(vec!["west", "east"])) as ArrayRef,
        ],
    )?;

    Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
}

pub(super) fn secret_connection() -> Result<MssqlConnectionConfig, DeltaFunnelError> {
    Ok(MssqlConnectionConfig::new(
        "server=tcp:sql.example.com;database=warehouse;user=admin;password=secret-token",
    )?
    .with_display_label("warehouse-primary"))
}

pub(super) fn override_connection() -> Result<MssqlConnectionConfig, DeltaFunnelError> {
    Ok(MssqlConnectionConfig::new(
        "server=tcp:override.example.com;database=warehouse;user=writer;password=override-secret",
    )?
    .with_display_label("warehouse-override"))
}

pub(super) fn output_request(
    table: LazyTable,
    output_name: &str,
    target_table: &str,
    load_mode: LoadMode,
) -> Result<OutputWritePlan, DeltaFunnelError> {
    output_request_with_run_mode(table, output_name, target_table, load_mode, RunMode::DryRun)
}

pub(super) fn execute_output_request(
    table: LazyTable,
    output_name: &str,
    target_table: &str,
    load_mode: LoadMode,
) -> Result<OutputWritePlan, DeltaFunnelError> {
    output_request_with_run_mode(
        table,
        output_name,
        target_table,
        load_mode,
        RunMode::Execute,
    )
}

fn output_request_with_run_mode(
    table: LazyTable,
    output_name: &str,
    target_table: &str,
    load_mode: LoadMode,
    run_mode: RunMode,
) -> Result<OutputWritePlan, DeltaFunnelError> {
    let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", target_table)?)
        .with_load_mode(load_mode);
    Ok(OutputWritePlan::new(
        table,
        MssqlOutputTarget::new(output_name, target_config, run_mode),
    ))
}

pub(super) async fn collect_stream_row_count(
    mut stream: MssqlOutputBatchStream,
) -> Result<usize, DeltaFunnelError> {
    let mut rows = 0_usize;

    while let Some(batch) = stream.next().await {
        rows = rows.saturating_add(batch?.num_rows());
    }

    Ok(rows)
}

pub(super) async fn collect_stream_marker_values(
    mut stream: MssqlOutputBatchStream,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut batches = Vec::new();

    while let Some(batch) = stream.next().await {
        batches.push(batch?);
    }

    marker_values_from_batches(&batches)
}

pub(super) fn marker_values_from_batches(
    batches: &[RecordBatch],
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut markers = Vec::new();

    for batch in batches {
        let column = batch
            .column_by_name("marker")
            .ok_or_else(|| std::io::Error::other("expected marker column"))?;
        let strings = column
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| std::io::Error::other("expected marker StringArray"))?;

        for row in 0..strings.len() {
            markers.push(strings.value(row).to_owned());
        }
    }

    Ok(markers)
}

#[derive(Debug)]
struct ScanCountingProvider {
    table: MemTable,
    scans: Arc<AtomicUsize>,
}

#[derive(Debug)]
struct FailingScanProvider {
    schema: SchemaRef,
    scans: Arc<AtomicUsize>,
}

#[derive(Debug)]
struct StreamSetupFailingProvider {
    child: Arc<dyn TableProvider>,
}

#[derive(Debug)]
struct BlockingProvider {
    plan: Arc<BlockingPlan>,
}

/// Exposes DataFusion's blocking test executor as an optimizer-rebuildable leaf.
#[derive(Debug)]
struct BlockingPlan {
    inner: BlockingExec,
}

#[derive(Debug)]
struct PlanLifetimeTrackingProvider {
    child: Arc<dyn TableProvider>,
    last_plan_marker: PlanLifetimeMarker,
}

#[derive(Debug)]
struct PlanLifetimeTrackingExec {
    child: Arc<dyn ExecutionPlan>,
    marker: Arc<()>,
}

type CountedProvider = (Arc<dyn TableProvider>, Arc<AtomicUsize>);
type PlanLifetimeMarker = Arc<Mutex<Option<Weak<()>>>>;
type PlanLifetimeTrackedProvider = (Arc<dyn TableProvider>, PlanLifetimeMarker);

#[async_trait]
impl TableProvider for ScanCountingProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table.schema()
    }

    fn table_type(&self) -> TableType {
        self.table.table_type()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.scans.fetch_add(1, Ordering::SeqCst);
        self.table.scan(state, projection, filters, limit).await
    }
}

#[async_trait]
impl TableProvider for FailingScanProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.scans.fetch_add(1, Ordering::SeqCst);
        Err(DataFusionError::Execution(
            "forced scan planning failure".to_owned(),
        ))
    }
}

#[async_trait]
impl TableProvider for StreamSetupFailingProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.child.schema()
    }

    fn table_type(&self) -> TableType {
        self.child.table_type()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let child = self.child.scan(state, projection, filters, limit).await?;
        Ok(Arc::new(StreamSetupFailingPlan::new(child)))
    }
}

#[async_trait]
impl TableProvider for BlockingProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self.plan.clone())
    }
}

impl DisplayAs for BlockingPlan {
    fn fmt_as(
        &self,
        _display_type: DisplayFormatType,
        formatter: &mut fmt::Formatter,
    ) -> fmt::Result {
        formatter.write_str("BlockingPlan")
    }
}

impl ExecutionPlan for BlockingPlan {
    fn name(&self) -> &str {
        "BlockingPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Plan(
                "BlockingPlan requires no children".to_owned(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }
}

impl DisplayAs for PlanLifetimeTrackingExec {
    fn fmt_as(
        &self,
        _display_type: DisplayFormatType,
        formatter: &mut fmt::Formatter,
    ) -> fmt::Result {
        formatter.write_str("PlanLifetimeTrackingExec")
    }
}

impl ExecutionPlan for PlanLifetimeTrackingExec {
    fn name(&self) -> &str {
        "PlanLifetimeTrackingExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.child.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "PlanLifetimeTrackingExec requires one child".to_owned(),
            ));
        }
        Ok(Arc::new(Self {
            child: Arc::clone(&children[0]),
            marker: Arc::clone(&self.marker),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        self.child.execute(partition, context)
    }
}

#[async_trait]
impl TableProvider for PlanLifetimeTrackingProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.child.schema()
    }

    fn table_type(&self) -> TableType {
        self.child.table_type()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let child = self.child.scan(state, projection, filters, limit).await?;
        let marker = Arc::new(());
        match self.last_plan_marker.lock() {
            Ok(mut last_plan_marker) => *last_plan_marker = Some(Arc::downgrade(&marker)),
            Err(poisoned) => *poisoned.into_inner() = Some(Arc::downgrade(&marker)),
        }
        Ok(Arc::new(PlanLifetimeTrackingExec { child, marker }))
    }
}

pub(super) fn scan_counting_marker_region_provider(
    marker: &str,
) -> Result<CountedProvider, Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("marker", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![marker, marker])) as ArrayRef,
            Arc::new(StringArray::from(vec!["west", "east"])) as ArrayRef,
        ],
    )?;
    let scans = Arc::new(AtomicUsize::new(0));
    let provider = ScanCountingProvider {
        table: MemTable::try_new(schema, vec![vec![batch]])?,
        scans: Arc::clone(&scans),
    };

    Ok((Arc::new(provider), scans))
}

pub(super) fn failing_scan_marker_region_provider() -> CountedProvider {
    let schema = Arc::new(Schema::new(vec![
        Field::new("marker", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
    ]));
    let scans = Arc::new(AtomicUsize::new(0));
    let provider = FailingScanProvider {
        schema,
        scans: Arc::clone(&scans),
    };

    (Arc::new(provider), scans)
}

pub(super) fn stream_setup_failing_marker_region_provider()
-> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error>> {
    Ok(Arc::new(StreamSetupFailingProvider {
        child: marker_region_provider("setup-failure")?,
    }))
}

pub(super) fn blocking_marker_provider() -> (Arc<dyn TableProvider>, Weak<()>) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "marker",
        DataType::Utf8,
        false,
    )]));
    let plan = Arc::new(BlockingPlan {
        inner: BlockingExec::new(schema, 1),
    });
    let refs = plan.inner.refs();

    (Arc::new(BlockingProvider { plan }), refs)
}

pub(super) fn plan_lifetime_tracking_marker_region_provider(
    marker: &str,
) -> Result<PlanLifetimeTrackedProvider, Box<dyn std::error::Error>> {
    let last_plan_marker = Arc::new(Mutex::new(None));
    let provider = PlanLifetimeTrackingProvider {
        child: marker_region_provider(marker)?,
        last_plan_marker: Arc::clone(&last_plan_marker),
    };

    Ok((Arc::new(provider), last_plan_marker))
}
