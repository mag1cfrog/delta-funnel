//! Rust backing model for lazy query-load orchestration.
//!
//! This module owns the high-level session and request data shapes that will
//! later back the Python `Session` and `Table` API. Metadata report helpers do
//! not contact SQL Server or execute rows unless a write path explicitly does so.

mod dry_run_report;
mod errors;
mod handles;
mod mssql_output;
mod options;
mod registry;
mod source_report;
mod streams;
#[cfg(test)]
mod test_support;
mod write_all;

use std::fmt;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::prelude::{DataFrame, SessionContext};
use futures_util::StreamExt;

use crate::{
    DeltaFunnelError, DeltaSourceConfig, DeltaTableProviderConfig, MssqlOutputBatchStream,
    datafusion_query_output_stream, datafusion_session_context, load_delta_source,
    preflight_delta_protocol, register_delta_sources_with_scan_execution_options,
};

pub use handles::{
    LazyTable, LazyTableKind, MssqlOutputTarget, OutputWritePlan, PlannedMssqlOutput, RunMode,
};
pub use options::SessionOptions;
pub use registry::{RegisteredDerivedTable, RegisteredSessionSource};
pub use source_report::{DeltaProviderSchedulingReport, DeltaSourceReport, SourceUsageStatus};
pub use write_all::{
    WriteAllCacheAliasReport, WriteAllCacheAliasStatus, WriteAllCacheCandidateSkip,
    WriteAllCacheCandidateSkipReason, WriteAllCacheMode, WriteAllCacheReport,
    WriteAllNoCacheReason, WriteAllOptions, WriteAllReport,
};

pub use dry_run_report::{
    MssqlDryRunOutputFieldReport, MssqlDryRunOutputReport, MssqlDryRunSqlIdentityReport,
    MssqlDryRunSqlIdentityState, MssqlDryRunWorkflowReport,
};
use errors::{datafusion_handoff_setup_error, unknown_lazy_table_error};
#[cfg(test)]
pub(crate) use mssql_output::OrchestratorMssqlOutputWriter;
use registry::PendingDerivedTable;
use streams::dataframe_for_lazy_table_from_session_parts;

/// Rust backing session for lazy query-load workflows.
pub struct DeltaFunnelSession {
    options: SessionOptions,
    context: SessionContext,
    next_table_id: u64,
    sources: Vec<RegisteredSessionSource>,
    derived_tables: Vec<RegisteredDerivedTable>,
    pending_derived_tables: Vec<PendingDerivedTable>,
}

impl DeltaFunnelSession {
    /// Builds a new session with validated local options.
    ///
    /// # Errors
    ///
    /// Returns the first local option validation failure before any source
    /// loading, SQL planning, SQL Server connection, or row execution.
    pub fn new(options: SessionOptions) -> Result<Self, DeltaFunnelError> {
        options.validate()?;
        let context = datafusion_session_context(options.query_options())?;
        Ok(Self {
            options,
            context,
            next_table_id: 0,
            sources: Vec::new(),
            derived_tables: Vec::new(),
            pending_derived_tables: Vec::new(),
        })
    }

    /// Returns the validated session options.
    #[must_use]
    pub const fn options(&self) -> &SessionOptions {
        &self.options
    }

    /// Returns the DataFusion session context owned by this orchestrator.
    ///
    /// The session context is exposed so later planning steps can analyze SQL
    /// against registered session aliases. Delta source registration should
    /// still go through [`DeltaFunnelSession::delta_lake`] so the orchestrator's
    /// source reports stay aligned with the DataFusion catalog.
    #[must_use]
    pub const fn context(&self) -> &SessionContext {
        &self.context
    }

    /// Returns the next unassigned session-local lazy table id.
    #[must_use]
    pub const fn next_table_id(&self) -> u64 {
        self.next_table_id
    }

    /// Registers one Delta source and returns its lazy table handle.
    ///
    /// The method performs source setup only: Delta snapshot metadata loading,
    /// protocol preflight, and DataFusion table registration. It does not scan
    /// data files for row production, parse user SQL, contact SQL Server, or
    /// execute an output action.
    ///
    /// # Errors
    ///
    /// Returns the first Delta source loading, protocol preflight, duplicate
    /// alias, schema conversion, or DataFusion registration error. Session
    /// source state is updated only after the DataFusion registration succeeds.
    pub fn delta_lake(&mut self, source: DeltaSourceConfig) -> Result<LazyTable, DeltaFunnelError> {
        self.reject_registered_alias_name(&source.name)?;
        let planned = load_delta_source(source)?;
        let preflight = preflight_delta_protocol(&planned)?;
        let registered = register_delta_sources_with_scan_execution_options(
            &self.context,
            vec![DeltaTableProviderConfig {
                source: planned,
                protocol: preflight,
                scan_target_partitions: None,
            }],
            self.options.provider_scan_options(),
        )?;
        let registered =
            registered
                .sources
                .into_iter()
                .next()
                .ok_or_else(|| DeltaFunnelError::Config {
                    message: "Delta source registration returned no registered source".to_owned(),
                })?;
        let table = self.allocate_delta_source_table(registered.name.clone());
        let session_source = RegisteredSessionSource::from_registered(table.clone(), registered);
        self.sources.push(session_source);
        Ok(table)
    }

    fn allocate_delta_source_table(&mut self, name: String) -> LazyTable {
        let id = self.next_table_id;
        self.next_table_id = self.next_table_id.saturating_add(1);
        LazyTable::delta_source(id, name)
    }

    pub(crate) async fn batch_stream_for_lazy_table(
        &self,
        table: &LazyTable,
    ) -> Result<MssqlOutputBatchStream, DeltaFunnelError> {
        let dataframe = self.dataframe_for_lazy_table(table).await?;
        let physical_plan = dataframe
            .create_physical_plan()
            .await
            .map_err(|error| datafusion_handoff_setup_error("physical_plan", error))?;
        let stream = datafusion_query_output_stream(physical_plan, self.context.task_ctx())
            .map_err(|error| datafusion_handoff_setup_error("query_output_stream", error))?;

        Ok(Box::pin(stream.map(|batch| {
            batch.map_err(|error| datafusion_handoff_setup_error("query_output_stream", error))
        })))
    }

    async fn dataframe_for_lazy_table(
        &self,
        table: &LazyTable,
    ) -> Result<DataFrame, DeltaFunnelError> {
        dataframe_for_lazy_table_from_session_parts(
            &self.context,
            table,
            &self.sources,
            &self.derived_tables,
            &self.pending_derived_tables,
        )
        .await
    }

    fn schema_for_lazy_table(&self, table: &LazyTable) -> Result<&SchemaRef, DeltaFunnelError> {
        match table.kind() {
            LazyTableKind::DeltaSource => self
                .sources
                .iter()
                .find(|source| source.table().id() == table.id())
                .map(RegisteredSessionSource::schema),
            LazyTableKind::DerivedSql => self
                .derived_tables
                .iter()
                .find(|derived| derived.table().id() == table.id())
                .map(RegisteredDerivedTable::schema)
                .or_else(|| {
                    self.pending_derived_tables
                        .iter()
                        .find(|pending| pending.table.id() == table.id())
                        .map(|pending| &pending.schema)
                }),
        }
        .ok_or_else(|| unknown_lazy_table_error(table))
    }
}

impl fmt::Debug for DeltaFunnelSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeltaFunnelSession")
            .field("options", &self.options)
            .field("sources", &self.sources)
            .field("derived_tables", &self.derived_tables)
            .field("next_table_id", &self.next_table_id)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        any::Any,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use super::registry::{DerivedTableDependency, DerivedTableLineage};
    use super::test_support::{
        DeltaLogTable, collect_stream_row_count, execute_output_request, marker_region_provider,
        output_request, override_connection, secret_connection,
    };
    use super::write_all::{
        MssqlCacheCandidateSkipReason, MssqlCachedOutputStreamRoute, MssqlNoCacheReason,
    };
    use super::write_all::{
        MssqlDerivedCacheAliasPlan, MssqlOutputCacheDecision, MssqlScopedCacheAliasReplacement,
        cache_error_with_restore_error, restore_mssql_cache_aliases_after_error,
    };
    use super::*;
    use crate::{
        LoadMode, MssqlConnectionSource, MssqlSchemaPlanOptions, MssqlTargetCleanupStatus,
        MssqlTargetConfig, MssqlTargetOutputPlan, MssqlTargetTable, MssqlWorkflowOutputWriter,
        MssqlWriteOptions, MssqlWriteReport, OutputStatus, ReportReasonCode, ResolvedMssqlTarget,
        ValidationOptions, ValidationStatus, WorkflowStatus, plan_mssql_target_for_resolved_output,
        table_formats::RealParquetDeltaTable,
    };
    use async_trait::async_trait;
    use datafusion::{
        arrow::{
            array::{Array, ArrayRef, StringArray},
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        },
        catalog::Session,
        common::tree_node::{TreeNode, TreeNodeRecursion},
        datasource::{MemTable, TableProvider},
        error::{DataFusionError, Result as DataFusionResult},
        logical_expr::{Expr, LogicalPlan, TableType},
        physical_plan::ExecutionPlan,
        sql::{parser::DFParser, resolve::resolve_table_references},
    };

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct FakeOrchestratorWriteCall {
        output_name: String,
        target_table: MssqlTargetTable,
        connection_source: MssqlConnectionSource,
        rows: u64,
        batches: u64,
        schema_fields: usize,
    }

    #[derive(Default)]
    struct FakeOrchestratorWriter {
        calls: Vec<FakeOrchestratorWriteCall>,
    }

    #[derive(Clone, Default)]
    struct FakeWorkflowWriter {
        calls: Arc<Mutex<Vec<FakeOrchestratorWriteCall>>>,
        fail_output_name: Option<String>,
    }

    impl FakeWorkflowWriter {
        fn failing_on(output_name: &str) -> Self {
            Self {
                fail_output_name: Some(output_name.to_owned()),
                ..Self::default()
            }
        }

        fn calls(&self) -> Arc<Mutex<Vec<FakeOrchestratorWriteCall>>> {
            Arc::clone(&self.calls)
        }
    }

    #[async_trait]
    impl OrchestratorMssqlOutputWriter for FakeOrchestratorWriter {
        async fn write_output(
            &mut self,
            output_schema: SchemaRef,
            output_plan: MssqlTargetOutputPlan,
            resolved_target: ResolvedMssqlTarget,
            mut batches: MssqlOutputBatchStream,
            _write_options: MssqlWriteOptions,
        ) -> Result<MssqlWriteReport, DeltaFunnelError> {
            let mut rows = 0_u64;
            let mut batch_count = 0_u64;

            while let Some(batch) = batches.next().await {
                let batch = batch?;
                rows = rows.saturating_add(u64::try_from(batch.num_rows()).map_err(|_| {
                    DeltaFunnelError::Config {
                        message: "fake writer row count overflowed u64".to_owned(),
                    }
                })?);
                batch_count = batch_count.saturating_add(1);
            }

            self.calls.push(FakeOrchestratorWriteCall {
                output_name: resolved_target.output_name().to_owned(),
                target_table: resolved_target.table().clone(),
                connection_source: resolved_target.connection_source(),
                rows,
                batches: batch_count,
                schema_fields: output_schema.fields().len(),
            });

            Ok(MssqlWriteReport::from_output_plan(
                &output_plan,
                rows,
                batch_count,
                0,
                false,
                MssqlTargetCleanupStatus::NotApplicable,
            ))
        }
    }

    #[async_trait]
    impl MssqlWorkflowOutputWriter for FakeWorkflowWriter {
        async fn write_output(
            &mut self,
            output_schema: SchemaRef,
            resolved_target: ResolvedMssqlTarget,
            schema_options: MssqlSchemaPlanOptions,
            mut batches: MssqlOutputBatchStream,
            _write_options: MssqlWriteOptions,
        ) -> Result<MssqlWriteReport, DeltaFunnelError> {
            let mut rows = 0_u64;
            let mut batch_count = 0_u64;

            while let Some(batch) = batches.next().await {
                let batch = batch?;
                rows = rows.saturating_add(u64::try_from(batch.num_rows()).map_err(|_| {
                    DeltaFunnelError::Config {
                        message: "fake workflow writer row count overflowed u64".to_owned(),
                    }
                })?);
                batch_count = batch_count.saturating_add(1);
            }

            let output_plan = plan_mssql_target_for_resolved_output(
                output_schema.as_ref(),
                &resolved_target,
                schema_options,
            )?;
            self.calls
                .lock()
                .map_err(|_| DeltaFunnelError::Config {
                    message: "fake workflow writer call lock poisoned".to_owned(),
                })?
                .push(FakeOrchestratorWriteCall {
                    output_name: resolved_target.output_name().to_owned(),
                    target_table: resolved_target.table().clone(),
                    connection_source: resolved_target.connection_source(),
                    rows,
                    batches: batch_count,
                    schema_fields: output_schema.fields().len(),
                });

            if self
                .fail_output_name
                .as_deref()
                .is_some_and(|output_name| output_name == resolved_target.output_name())
            {
                return Err(DeltaFunnelError::MssqlWorkflowPlanning {
                    message: format!(
                        "fake workflow writer failed for `{}`",
                        resolved_target.output_name()
                    ),
                });
            }

            Ok(MssqlWriteReport::from_output_plan(
                &output_plan,
                rows,
                batch_count,
                0,
                false,
                MssqlTargetCleanupStatus::NotApplicable,
            ))
        }
    }

    async fn collect_stream_marker_values(
        mut stream: MssqlOutputBatchStream,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut batches = Vec::new();

        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }

        marker_values_from_batches(&batches)
    }

    fn marker_values_from_batches(
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

    type CountedProvider = (Arc<dyn TableProvider>, Arc<AtomicUsize>);

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

    fn scan_counting_marker_region_provider(
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

    fn failing_scan_marker_region_provider() -> CountedProvider {
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

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TableScanProofReference {
        table_name: String,
        nested_table_names: Vec<String>,
    }

    fn table_scan_proof_references(
        plan: &LogicalPlan,
    ) -> DataFusionResult<Vec<TableScanProofReference>> {
        let mut references = Vec::new();

        plan.apply(|node| {
            if let LogicalPlan::TableScan(scan) = node {
                let nested_table_names = scan
                    .source
                    .get_logical_plan()
                    .map(|nested| table_scan_table_names(nested.as_ref()))
                    .transpose()?
                    .unwrap_or_default();
                references.push(TableScanProofReference {
                    table_name: scan.table_name.table().to_owned(),
                    nested_table_names,
                });
            }

            Ok(TreeNodeRecursion::Continue)
        })?;

        Ok(references)
    }

    fn table_scan_table_names(plan: &LogicalPlan) -> DataFusionResult<Vec<String>> {
        let mut names = Vec::new();

        plan.apply(|node| {
            if let LogicalPlan::TableScan(scan) = node {
                names.push(scan.table_name.table().to_owned());
            }

            Ok(TreeNodeRecursion::Continue)
        })?;

        Ok(names)
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct AstReferenceProof {
        relations: Vec<String>,
        ctes: Vec<String>,
    }

    fn ast_reference_proof(sql: &str) -> Result<AstReferenceProof, Box<dyn std::error::Error>> {
        let mut statements = DFParser::parse_sql(sql)?;
        if statements.len() != 1 {
            return Err(std::io::Error::other("expected exactly one parsed statement").into());
        }
        let statement = statements
            .pop_front()
            .ok_or_else(|| std::io::Error::other("expected parsed statement"))?;
        let (relations, ctes) = resolve_table_references(&statement, true)?;

        Ok(AstReferenceProof {
            relations: relations
                .into_iter()
                .map(|reference| reference.to_string())
                .collect(),
            ctes: ctes
                .into_iter()
                .map(|reference| reference.to_string())
                .collect(),
        })
    }

    #[test]
    fn cache_plan_shell_preserves_selected_output_order() -> Result<(), DeltaFunnelError> {
        let session = DeltaFunnelSession::new(SessionOptions::default())?;
        let west = output_request(
            LazyTable::placeholder(7, LazyTableKind::DerivedSql),
            "west_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let east = output_request(
            LazyTable::placeholder(8, LazyTableKind::DerivedSql),
            "east_output",
            "east_orders",
            LoadMode::AppendExisting,
        )?;

        let plan = session.plan_mssql_output_cache(&[west, east]);

        assert_eq!(
            plan.decision(),
            &MssqlOutputCacheDecision::NoCache {
                reason: MssqlNoCacheReason::NoSharedRegisteredDerivedAlias,
            }
        );
        assert!(plan.skipped_candidates().is_empty());
        assert_eq!(plan.selected_outputs().len(), 2);
        assert_eq!(plan.selected_outputs()[0].index(), 0);
        assert_eq!(plan.selected_outputs()[0].table_id(), 7);
        assert_eq!(plan.selected_outputs()[0].table_name(), "table_7");
        assert_eq!(plan.selected_outputs()[0].output_name(), "west_output");
        assert_eq!(plan.selected_outputs()[1].index(), 1);
        assert_eq!(plan.selected_outputs()[1].table_id(), 8);
        assert_eq!(plan.selected_outputs()[1].table_name(), "table_8");
        assert_eq!(plan.selected_outputs()[1].output_name(), "east_output");
        Ok(())
    }

    #[test]
    fn cache_plan_shell_reports_single_output_as_not_shared() -> Result<(), DeltaFunnelError> {
        let session = DeltaFunnelSession::new(SessionOptions::default())?;
        let output = output_request(
            LazyTable::placeholder(7, LazyTableKind::DerivedSql),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;

        let plan = session.plan_mssql_output_cache(&[output]);

        assert_eq!(
            plan.decision(),
            &MssqlOutputCacheDecision::NoCache {
                reason: MssqlNoCacheReason::FewerThanTwoOutputs,
            }
        );
        assert_eq!(plan.selected_outputs().len(), 1);
        Ok(())
    }

    #[test]
    fn cache_plan_debug_omits_target_connection_material() -> Result<(), DeltaFunnelError> {
        let session = DeltaFunnelSession::new(SessionOptions::default())?;
        let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders")?)
            .with_connection(secret_connection()?);
        let output = OutputWritePlan::new(
            LazyTable::placeholder(7, LazyTableKind::DerivedSql),
            MssqlOutputTarget::new("orders\noutput", target_config, RunMode::DryRun),
        );

        let debug = format!("{:?}", session.plan_mssql_output_cache(&[output]));

        assert!(debug.contains("orders"));
        assert!(debug.contains("output"));
        assert!(!debug.contains('\n'));
        assert!(!debug.contains("secret-token"));
        assert!(!debug.contains("password"));
        assert!(!debug.contains("server=tcp"));
        Ok(())
    }

    #[tokio::test]
    async fn cache_plan_selects_shared_registered_derived_dependency()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select id from big where customer_name = 'alice'")
            .await?;
        let east = session
            .table_from_sql("select id from big where customer_name = 'bob'")
            .await?;
        let west = output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east = output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;

        let plan = session.plan_mssql_output_cache(&[west, east]);

        assert!(plan.skipped_candidates().is_empty());
        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };
        assert_eq!(caches.len(), 1);
        let cache = &caches[0];
        assert_eq!(cache.table_id(), big.id());
        assert_eq!(cache.alias(), "big");
        assert_eq!(cache.output_indexes(), &[0, 1]);
        Ok(())
    }

    #[tokio::test]
    async fn cache_plan_counts_direct_selected_alias_use() -> Result<(), Box<dyn std::error::Error>>
    {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select id from big where customer_name = 'alice'")
            .await?;
        let big_output = output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let west_output =
            output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;

        let plan = session.plan_mssql_output_cache(&[big_output, west_output]);

        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };
        assert_eq!(caches.len(), 1);
        let cache = &caches[0];
        assert_eq!(cache.table_id(), big.id());
        assert_eq!(cache.alias(), "big");
        assert_eq!(cache.output_indexes(), &[0, 1]);
        Ok(())
    }

    #[tokio::test]
    async fn cached_output_stream_route_classifies_direct_dependent_and_unrelated_outputs()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select id from big where customer_name = 'alice'")
            .await?;
        let unrelated = session
            .table_from_sql("select customer_name from orders")
            .await?;
        let big_output = output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let west_output =
            output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let unrelated_output = output_request(
            unrelated,
            "unrelated_output",
            "unrelated_orders",
            LoadMode::AppendExisting,
        )?;
        let plan = session.plan_mssql_output_cache(&[big_output.clone(), west_output.clone()]);
        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };

        assert_eq!(
            session.cached_output_stream_route(&big_output, caches)?,
            MssqlCachedOutputStreamRoute::DirectCachedAlias(caches[0].clone())
        );
        assert_eq!(
            session.cached_output_stream_route(&west_output, caches)?,
            MssqlCachedOutputStreamRoute::ReplannedCachedDependency(vec![caches[0].clone()])
        );
        assert_eq!(
            session.cached_output_stream_route(&unrelated_output, caches)?,
            MssqlCachedOutputStreamRoute::UncachedLazyTable
        );
        Ok(())
    }

    #[tokio::test]
    async fn cached_output_stream_route_keeps_multiple_active_dependency_aliases()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let pending_names = session
            .table_from_sql("select customer_name from orders")
            .await?;
        let names = session.register_alias("names", &pending_names)?;
        let west = session
            .table_from_sql(
                "select big.id from big join names on big.customer_name = names.customer_name",
            )
            .await?;
        let east = session
            .table_from_sql(
                "select big.id from big join names on big.customer_name = names.customer_name",
            )
            .await?;
        let west_output = output_request(
            west.clone(),
            "west_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let east_output =
            output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;
        let plan = session.plan_mssql_output_cache(&[west_output.clone(), east_output]);
        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };

        assert_eq!(caches.len(), 2);
        assert_eq!(caches[0].table_id(), big.id());
        assert_eq!(caches[1].table_id(), names.id());
        assert_eq!(
            session.cached_output_stream_route(&west_output, caches)?,
            MssqlCachedOutputStreamRoute::ReplannedCachedDependency(caches.clone())
        );
        Ok(())
    }

    #[test]
    fn cached_output_stream_route_rejects_unknown_active_alias() -> Result<(), DeltaFunnelError> {
        let session = DeltaFunnelSession::new(SessionOptions::default())?;
        let output = output_request(
            LazyTable::placeholder(7, LazyTableKind::DerivedSql),
            "west_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let aliases = vec![MssqlDerivedCacheAliasPlan::new(
            252,
            "missing_cache".to_owned(),
            vec![0],
        )];

        let error = session.cached_output_stream_route(&output, &aliases);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("missing_cache")
                    && message.contains("not registered in this session")
        ));
        Ok(())
    }

    #[tokio::test]
    async fn cached_output_stream_factory_direct_alias_reads_active_cache()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let big_output = output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let west_output =
            output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let plan = session.plan_mssql_output_cache(&[big_output.clone(), west_output]);
        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };
        let replacement = session
            .replace_registered_derived_alias_with_cache(&big)
            .await?;
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let factory = session.cached_output_batch_stream_factory(&big_output, caches)?;
        let markers = collect_stream_marker_values(factory().await?).await?;

        assert_eq!(markers, vec!["shared", "shared"]);
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);
        let _restoration = replacement.restore().await?;
        Ok(())
    }

    #[tokio::test]
    async fn cached_output_stream_factory_unrelated_output_uses_existing_lazy_table_path()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let unrelated = session
            .table_from_sql("select 'unrelated' as marker, 'north' as region")
            .await?;
        let big_output = output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let west_output =
            output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let unrelated_output = output_request(
            unrelated,
            "unrelated_output",
            "unrelated_orders",
            LoadMode::AppendExisting,
        )?;
        let plan = session.plan_mssql_output_cache(&[big_output, west_output]);
        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };
        let replacement = session
            .replace_registered_derived_alias_with_cache(&big)
            .await?;
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let factory = session.cached_output_batch_stream_factory(&unrelated_output, caches)?;
        let markers = collect_stream_marker_values(factory().await?).await?;

        assert_eq!(markers, vec!["unrelated"]);
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);
        let _restoration = replacement.restore().await?;
        Ok(())
    }

    #[tokio::test]
    async fn cached_output_stream_factory_replans_dependent_outputs_against_active_cache()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let east = session
            .table_from_sql("select marker from big where region = 'east'")
            .await?;
        let big_output = output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let west_output = output_request(
            west.clone(),
            "west_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let east_output = output_request(
            east.clone(),
            "east_output",
            "east_orders",
            LoadMode::AppendExisting,
        )?;
        let plan = session.plan_mssql_output_cache(&[
            big_output.clone(),
            west_output.clone(),
            east_output.clone(),
        ]);
        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };
        let replacement = session
            .replace_registered_derived_alias_with_cache(&big)
            .await?;
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let big_factory = session.cached_output_batch_stream_factory(&big_output, caches)?;
        let west_factory = session.cached_output_batch_stream_factory(&west_output, caches)?;
        let east_factory = session.cached_output_batch_stream_factory(&east_output, caches)?;
        let big_markers = collect_stream_marker_values(big_factory().await?).await?;
        let west_markers = collect_stream_marker_values(west_factory().await?).await?;
        let east_markers = collect_stream_marker_values(east_factory().await?).await?;

        assert_eq!(big_markers, vec!["shared", "shared"]);
        assert_eq!(west_markers, vec!["shared"]);
        assert_eq!(east_markers, vec!["shared"]);
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);
        let _restoration = replacement.restore().await?;
        Ok(())
    }

    #[tokio::test]
    async fn cached_output_stream_factory_replans_dependent_output_against_multiple_active_caches()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (big_source_provider, big_source_scans) = scan_counting_marker_region_provider("big")?;
        let (names_source_provider, names_source_scans) =
            scan_counting_marker_region_provider("name")?;
        session
            .context()
            .register_table("big_source", big_source_provider)?;
        session
            .context()
            .register_table("names_source", names_source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let pending_names = session
            .table_from_sql("select marker, region from names_source")
            .await?;
        let names = session.register_alias("names", &pending_names)?;
        let west = session
            .table_from_sql(
                "select big.marker from big join names on big.region = names.region where big.region = 'west' and names.marker = 'name'",
            )
            .await?;
        let east = session
            .table_from_sql(
                "select big.marker from big join names on big.region = names.region where big.region = 'east' and names.marker = 'name'",
            )
            .await?;
        let west_output = output_request(
            west.clone(),
            "west_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let east_output =
            output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;
        let plan = session.plan_mssql_output_cache(&[west_output.clone(), east_output]);
        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };
        assert_eq!(caches.len(), 2);
        assert_eq!(caches[0].table_id(), big.id());
        assert_eq!(caches[1].table_id(), names.id());
        let big_replacement = session
            .replace_registered_derived_alias_with_cache(&big)
            .await?;
        let names_replacement = session
            .replace_registered_derived_alias_with_cache(&names)
            .await?;
        assert_eq!(big_source_scans.load(Ordering::SeqCst), 1);
        assert_eq!(names_source_scans.load(Ordering::SeqCst), 1);

        let factory = session.cached_output_batch_stream_factory(&west_output, caches)?;
        let markers = collect_stream_marker_values(factory().await?).await?;

        assert_eq!(markers, vec!["big"]);
        assert_eq!(big_source_scans.load(Ordering::SeqCst), 1);
        assert_eq!(names_source_scans.load(Ordering::SeqCst), 1);
        let _names_restoration = names_replacement.restore().await?;
        let _big_restoration = big_replacement.restore().await?;
        Ok(())
    }

    #[tokio::test]
    async fn cached_output_stream_factory_rejects_replanned_schema_mismatch()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (source_provider, _source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let big_output = output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let west_output = output_request(
            west.clone(),
            "west_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let plan = session.plan_mssql_output_cache(&[big_output, west_output.clone()]);
        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };
        let pending_west = session
            .pending_derived_tables
            .iter_mut()
            .find(|pending| pending.table.id() == west.id())
            .ok_or("expected pending west table")?;
        pending_west.schema = Arc::new(Schema::new(vec![Field::new(
            "different_marker",
            DataType::Utf8,
            false,
        )]));
        let replacement = session
            .replace_registered_derived_alias_with_cache(&big)
            .await?;

        let factory = session.cached_output_batch_stream_factory(&west_output, caches)?;
        let error = factory().await;

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("cached output stream setup failed for `west_output`")
                    && message.contains("replanned output schema does not match")
        ));
        let _restoration = replacement.restore().await?;
        Ok(())
    }

    #[tokio::test]
    async fn cached_output_stream_factory_returns_async_error_for_unreplayable_sql()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (source_provider, _source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let big_output = output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let west_output = output_request(
            west.clone(),
            "west_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let plan = session.plan_mssql_output_cache(&[big_output, west_output.clone()]);
        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };
        let pending_west = session
            .pending_derived_tables
            .iter_mut()
            .find(|pending| pending.table.id() == west.id())
            .ok_or("expected pending west table")?;
        pending_west.sql_text.clear();
        let replacement = session
            .replace_registered_derived_alias_with_cache(&big)
            .await?;

        let factory = session.cached_output_batch_stream_factory(&west_output, caches)?;
        let error = factory().await;

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("cached output stream setup failed for `west_output`")
        ));
        let _restoration = replacement.restore().await?;
        Ok(())
    }

    #[tokio::test]
    async fn cache_plan_skips_unshared_registered_derived_alias()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let unrelated = session
            .table_from_sql("select customer_name from orders")
            .await?;
        let big_output = output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let unrelated_output = output_request(
            unrelated,
            "unrelated_output",
            "unrelated_orders",
            LoadMode::AppendExisting,
        )?;

        let plan = session.plan_mssql_output_cache(&[big_output, unrelated_output]);

        assert_eq!(
            plan.decision(),
            &MssqlOutputCacheDecision::NoCache {
                reason: MssqlNoCacheReason::NoSharedRegisteredDerivedAlias,
            }
        );
        assert_eq!(plan.skipped_candidates().len(), 1);
        let skipped = &plan.skipped_candidates()[0];
        assert_eq!(skipped.table_id(), big.id());
        assert_eq!(skipped.alias(), "big");
        assert_eq!(
            skipped.reason(),
            &MssqlCacheCandidateSkipReason::NotShared { output_count: 1 }
        );
        Ok(())
    }

    #[tokio::test]
    async fn cache_plan_prefers_deepest_shared_registered_derived_alias()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let pending_filtered = session
            .table_from_sql("select id, customer_name from big where id > 0")
            .await?;
        let filtered_big = session.register_alias("filtered_big", &pending_filtered)?;
        let west = session
            .table_from_sql("select id from filtered_big where customer_name = 'alice'")
            .await?;
        let east = session
            .table_from_sql("select id from filtered_big where customer_name = 'bob'")
            .await?;
        let west = output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east = output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;

        let plan = session.plan_mssql_output_cache(&[west, east]);

        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };
        assert_eq!(caches.len(), 1);
        let cache = &caches[0];
        assert_eq!(cache.table_id(), filtered_big.id());
        assert_eq!(cache.alias(), "filtered_big");
        assert_eq!(cache.output_indexes(), &[0, 1]);
        assert_eq!(plan.skipped_candidates().len(), 1);
        let skipped = &plan.skipped_candidates()[0];
        assert_eq!(skipped.table_id(), big.id());
        assert_eq!(skipped.alias(), "big");
        assert_eq!(
            skipped.reason(),
            &MssqlCacheCandidateSkipReason::CoveredByDeeperSharedAlias {
                selected_table_id: filtered_big.id(),
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn cache_plan_selects_independent_shared_aliases_with_same_output_indexes()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let pending_names = session
            .table_from_sql("select customer_name from orders")
            .await?;
        let names = session.register_alias("names", &pending_names)?;
        let west = session
            .table_from_sql(
                "select big.id from big join names on big.customer_name = names.customer_name",
            )
            .await?;
        let east = session
            .table_from_sql(
                "select big.id from big join names on big.customer_name = names.customer_name",
            )
            .await?;
        let west = output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east = output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;

        let plan = session.plan_mssql_output_cache(&[west, east]);

        // Sharing the same selected output indexes is not ambiguity when the
        // aliases are independent in the derived lineage graph.
        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };
        assert!(plan.skipped_candidates().is_empty());
        assert_eq!(caches.len(), 2);
        assert_eq!(caches[0].table_id(), big.id());
        assert_eq!(caches[0].alias(), "big");
        assert_eq!(caches[0].output_indexes(), &[0, 1]);
        assert_eq!(caches[1].table_id(), names.id());
        assert_eq!(caches[1].alias(), "names");
        assert_eq!(caches[1].output_indexes(), &[0, 1]);
        Ok(())
    }

    #[tokio::test]
    async fn cache_plan_rejects_cyclic_shared_candidate_relationships()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let pending_names = session
            .table_from_sql("select customer_name from orders")
            .await?;
        let names = session.register_alias("names", &pending_names)?;
        for derived in &mut session.derived_tables {
            if derived.table().id() == big.id() {
                derived.lineage = DerivedTableLineage::complete(
                    vec![DerivedTableDependency::RegisteredDerived {
                        table_id: names.id(),
                        name: "names".to_owned(),
                    }],
                    Vec::new(),
                    Vec::new(),
                );
            } else if derived.table().id() == names.id() {
                derived.lineage = DerivedTableLineage::complete(
                    vec![DerivedTableDependency::RegisteredDerived {
                        table_id: big.id(),
                        name: "big".to_owned(),
                    }],
                    Vec::new(),
                    Vec::new(),
                );
            }
        }
        let west = session
            .table_from_sql(
                "select big.id from big join names on big.customer_name = names.customer_name",
            )
            .await?;
        let east = session
            .table_from_sql(
                "select big.id from big join names on big.customer_name = names.customer_name",
            )
            .await?;
        let west = output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east = output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;

        let plan = session.plan_mssql_output_cache(&[west, east]);

        assert_eq!(
            plan.decision(),
            &MssqlOutputCacheDecision::NoCache {
                reason: MssqlNoCacheReason::AmbiguousSharedDerivedAlias,
            }
        );
        assert_eq!(plan.skipped_candidates().len(), 2);
        assert_eq!(plan.skipped_candidates()[0].table_id(), big.id());
        assert_eq!(
            plan.skipped_candidates()[0].reason(),
            &MssqlCacheCandidateSkipReason::AmbiguousDepth
        );
        assert_eq!(plan.skipped_candidates()[1].table_id(), names.id());
        assert_eq!(
            plan.skipped_candidates()[1].reason(),
            &MssqlCacheCandidateSkipReason::AmbiguousDepth
        );
        Ok(())
    }

    #[tokio::test]
    async fn cache_plan_rejects_partially_ambiguous_shared_candidate_graph()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let pending_names = session
            .table_from_sql("select customer_name from orders")
            .await?;
        let names = session.register_alias("names", &pending_names)?;
        let pending_regions = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let regions = session.register_alias("regions", &pending_regions)?;
        for derived in &mut session.derived_tables {
            if derived.table().id() == big.id() {
                derived.lineage = DerivedTableLineage::complete(
                    vec![DerivedTableDependency::RegisteredDerived {
                        table_id: names.id(),
                        name: "names".to_owned(),
                    }],
                    Vec::new(),
                    Vec::new(),
                );
            } else if derived.table().id() == names.id() {
                derived.lineage = DerivedTableLineage::complete(
                    vec![DerivedTableDependency::RegisteredDerived {
                        table_id: big.id(),
                        name: "big".to_owned(),
                    }],
                    Vec::new(),
                    Vec::new(),
                );
            }
        }
        let west = session
            .table_from_sql(
                "select big.id from big \
                 join names on big.customer_name = names.customer_name \
                 join regions on big.id = regions.id",
            )
            .await?;
        let east = session
            .table_from_sql(
                "select big.id from big \
                 join names on big.customer_name = names.customer_name \
                 join regions on big.id = regions.id",
            )
            .await?;
        let west = output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east = output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;

        let plan = session.plan_mssql_output_cache(&[west, east]);

        assert_eq!(
            plan.decision(),
            &MssqlOutputCacheDecision::NoCache {
                reason: MssqlNoCacheReason::AmbiguousSharedDerivedAlias,
            }
        );
        assert_eq!(plan.skipped_candidates().len(), 3);
        assert_eq!(plan.skipped_candidates()[0].table_id(), big.id());
        assert_eq!(
            plan.skipped_candidates()[0].reason(),
            &MssqlCacheCandidateSkipReason::AmbiguousDepth
        );
        assert_eq!(plan.skipped_candidates()[1].table_id(), names.id());
        assert_eq!(
            plan.skipped_candidates()[1].reason(),
            &MssqlCacheCandidateSkipReason::AmbiguousDepth
        );
        assert_eq!(plan.skipped_candidates()[2].table_id(), regions.id());
        assert_eq!(
            plan.skipped_candidates()[2].reason(),
            &MssqlCacheCandidateSkipReason::AmbiguousDepth
        );
        Ok(())
    }

    #[tokio::test]
    async fn cache_plan_does_not_consider_shared_raw_source_as_candidate()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let orders = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let west = output_request(
            orders.clone(),
            "west_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let east = output_request(
            orders,
            "east_output",
            "east_orders",
            LoadMode::AppendExisting,
        )?;

        let plan = session.plan_mssql_output_cache(&[west, east]);

        assert_eq!(
            plan.decision(),
            &MssqlOutputCacheDecision::NoCache {
                reason: MssqlNoCacheReason::NoSharedRegisteredDerivedAlias,
            }
        );
        assert!(plan.skipped_candidates().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn cache_plan_skips_registered_derived_alias_with_incomplete_lineage()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let registered = session
            .derived_tables
            .iter_mut()
            .find(|derived| derived.table().id() == big.id())
            .ok_or("registered derived alias missing")?;
        registered.lineage = DerivedTableLineage::incomplete("forced incomplete lineage");
        let west = session
            .table_from_sql("select id from big where customer_name = 'alice'")
            .await?;
        let east = session
            .table_from_sql("select id from big where customer_name = 'bob'")
            .await?;
        let west = output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east = output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;

        let plan = session.plan_mssql_output_cache(&[west, east]);

        assert_eq!(
            plan.decision(),
            &MssqlOutputCacheDecision::NoCache {
                reason: MssqlNoCacheReason::NoSharedRegisteredDerivedAlias,
            }
        );
        assert_eq!(plan.skipped_candidates().len(), 1);
        let skipped = &plan.skipped_candidates()[0];
        assert_eq!(skipped.table_id(), big.id());
        assert_eq!(skipped.alias(), "big");
        assert_eq!(
            skipped.reason(),
            &MssqlCacheCandidateSkipReason::IncompleteLineage
        );
        Ok(())
    }

    #[tokio::test]
    async fn cache_plan_skips_registered_derived_alias_with_missing_sql_text()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let registered = session
            .derived_tables
            .iter_mut()
            .find(|derived| derived.table().id() == big.id())
            .ok_or("registered derived alias missing")?;
        registered.sql_text.clear();
        let west = session
            .table_from_sql("select id from big where customer_name = 'alice'")
            .await?;
        let east = session
            .table_from_sql("select id from big where customer_name = 'bob'")
            .await?;
        let west = output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east = output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;

        let plan = session.plan_mssql_output_cache(&[west, east]);

        assert_eq!(
            plan.decision(),
            &MssqlOutputCacheDecision::NoCache {
                reason: MssqlNoCacheReason::NoSharedRegisteredDerivedAlias,
            }
        );
        assert_eq!(plan.skipped_candidates().len(), 1);
        let skipped = &plan.skipped_candidates()[0];
        assert_eq!(skipped.table_id(), big.id());
        assert_eq!(skipped.alias(), "big");
        assert_eq!(
            skipped.reason(),
            &MssqlCacheCandidateSkipReason::MissingSqlText
        );
        Ok(())
    }

    #[tokio::test]
    async fn cache_plan_skips_independent_unshared_registered_derived_aliases()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let pending_names = session
            .table_from_sql("select customer_name from orders")
            .await?;
        let names = session.register_alias("names", &pending_names)?;
        let west = session
            .table_from_sql("select id from big where customer_name = 'alice'")
            .await?;
        let name_output = session
            .table_from_sql("select customer_name from names")
            .await?;
        let west = output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let name_output = output_request(
            name_output,
            "name_output",
            "name_orders",
            LoadMode::AppendExisting,
        )?;

        let plan = session.plan_mssql_output_cache(&[west, name_output]);

        assert_eq!(
            plan.decision(),
            &MssqlOutputCacheDecision::NoCache {
                reason: MssqlNoCacheReason::NoSharedRegisteredDerivedAlias,
            }
        );
        assert_eq!(plan.skipped_candidates().len(), 2);
        assert_eq!(plan.skipped_candidates()[0].table_id(), big.id());
        assert_eq!(plan.skipped_candidates()[0].alias(), "big");
        assert_eq!(
            plan.skipped_candidates()[0].reason(),
            &MssqlCacheCandidateSkipReason::NotShared { output_count: 1 }
        );
        assert_eq!(plan.skipped_candidates()[1].table_id(), names.id());
        assert_eq!(plan.skipped_candidates()[1].alias(), "names");
        assert_eq!(
            plan.skipped_candidates()[1].reason(),
            &MssqlCacheCandidateSkipReason::NotShared { output_count: 1 }
        );
        Ok(())
    }

    #[tokio::test]
    async fn scoped_cache_alias_replacement_materializes_cache_and_restores_original_provider()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        assert_eq!(source_scans.load(Ordering::SeqCst), 0);

        let replacement = session
            .replace_registered_derived_alias_with_cache(&big)
            .await?;

        assert_eq!(replacement.table_id(), big.id());
        assert_eq!(replacement.alias_name(), "big");
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let direct_cached_big = session
            .context()
            .sql("select marker from big where region = 'west'")
            .await?
            .collect()
            .await?;
        assert_eq!(
            marker_values_from_batches(&direct_cached_big)?,
            vec!["shared"]
        );
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let restoration = replacement.restore().await?;

        assert_eq!(restoration.table_id(), big.id());
        assert_eq!(restoration.alias_name(), "big");
        assert!(restoration.cached_alias_was_present());

        let direct_restored_big = session
            .context()
            .sql("select marker from big where region = 'west'")
            .await?
            .collect()
            .await?;
        assert_eq!(
            marker_values_from_batches(&direct_restored_big)?,
            vec!["shared"]
        );
        assert_eq!(source_scans.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn scoped_cache_alias_replacement_restores_original_after_cached_register_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let _big = session.register_alias("big", &pending_big)?;
        let original_provider = session
            .context()
            .deregister_table("big")?
            .ok_or("expected original provider")?;

        let error = session.restore_original_after_cached_register_failure(
            "big",
            original_provider,
            "injected cached register failure",
        );

        assert!(matches!(
            &error,
            DeltaFunnelError::MssqlWorkflowPlanning { message }
                if message.contains("failed to register cached provider")
                    && message.contains("injected cached register failure")
        ));

        let direct_restored_big = session
            .context()
            .sql("select marker from big where region = 'west'")
            .await?
            .collect()
            .await?;
        assert_eq!(
            marker_values_from_batches(&direct_restored_big)?,
            vec!["shared"]
        );
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn scoped_cache_alias_replacement_explicit_restore_cleans_up_after_later_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let replacement = session
            .replace_registered_derived_alias_with_cache(&big)
            .await?;
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let later_error = DeltaFunnelError::MssqlWorkflowPlanning {
            message: "simulated downstream planning failure".to_owned(),
        };
        let restoration = replacement.restore().await?;

        assert!(matches!(
            later_error,
            DeltaFunnelError::MssqlWorkflowPlanning { message }
                if message.contains("simulated downstream planning failure")
        ));
        assert_eq!(restoration.alias_name(), "big");

        let direct_restored_big = session
            .context()
            .sql("select marker from big where region = 'west'")
            .await?
            .collect()
            .await?;
        assert_eq!(
            marker_values_from_batches(&direct_restored_big)?,
            vec!["shared"]
        );
        assert_eq!(source_scans.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn scoped_cache_alias_restore_reinstalls_original_when_cached_alias_is_missing()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let replacement = session
            .replace_registered_derived_alias_with_cache(&big)
            .await?;
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let removed_cached = session.context().deregister_table("big")?;
        assert!(removed_cached.is_some());

        let restoration = replacement.restore().await?;

        assert_eq!(restoration.alias_name(), "big");
        assert!(!restoration.cached_alias_was_present());

        let direct_restored_big = session
            .context()
            .sql("select marker from big where region = 'west'")
            .await?
            .collect()
            .await?;
        assert_eq!(
            marker_values_from_batches(&direct_restored_big)?,
            vec!["shared"]
        );
        assert_eq!(source_scans.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[test]
    fn cache_error_with_restore_error_preserves_both_contexts() {
        let primary_error = DeltaFunnelError::MssqlWorkflowPlanning {
            message: "simulated output workflow failure".to_owned(),
        };
        let restore_error = DeltaFunnelError::MssqlWorkflowPlanning {
            message: "simulated restore failure for alias big".to_owned(),
        };

        let error = cache_error_with_restore_error(primary_error, restore_error);

        assert!(matches!(
            error,
            DeltaFunnelError::MssqlWorkflowPlanning { message }
                if message.contains("write_all auto cache failed")
                    && message.contains("simulated output workflow failure")
                    && message.contains("also failed to restore cache aliases")
                    && message.contains("simulated restore failure for alias big")
        ));
    }

    #[tokio::test]
    async fn restore_mssql_cache_aliases_after_error_preserves_broken_restore_context()
    -> Result<(), Box<dyn std::error::Error>> {
        let session = DeltaFunnelSession::new(SessionOptions::default())?;
        let broken_replacement = MssqlScopedCacheAliasReplacement::broken_for_test(
            session.context(),
            42,
            "big".to_owned(),
        );
        let primary_error = DeltaFunnelError::MssqlWorkflowPlanning {
            message: "simulated cached workflow failure".to_owned(),
        };

        let error =
            restore_mssql_cache_aliases_after_error(primary_error, vec![broken_replacement]).await;

        assert!(matches!(
            error,
            DeltaFunnelError::MssqlWorkflowPlanning { message }
                if message.contains("write_all auto cache failed")
                    && message.contains("simulated cached workflow failure")
                    && message.contains("also failed to restore cache aliases")
                    && message.contains("scoped MSSQL cache alias restore failed")
                    && message.contains("big")
                    && message.contains("original provider was already restored")
        ));
        Ok(())
    }

    #[tokio::test]
    async fn cached_alias_replacement_does_not_feed_existing_downstream_derived_tables()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session
            .context()
            .register_table("big_source", marker_region_provider("original")?)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let _big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let east = session
            .table_from_sql("select marker from big where region = 'east'")
            .await?;

        let replacement = session
            .context()
            .read_table(marker_region_provider("replacement")?)?
            .cache()
            .await?
            .into_view();
        let removed_big = session.context().deregister_table("big")?;
        assert!(removed_big.is_some());
        session.context().register_table("big", replacement)?;

        let direct_big = session
            .context()
            .sql("select marker from big where region = 'west'")
            .await?
            .collect()
            .await?;
        assert_eq!(
            marker_values_from_batches(&direct_big)?,
            vec!["replacement"]
        );

        let west_stream = session.batch_stream_for_lazy_table(&west).await?;
        let east_stream = session.batch_stream_for_lazy_table(&east).await?;
        let west_markers = collect_stream_marker_values(west_stream).await?;
        let east_markers = collect_stream_marker_values(east_stream).await?;

        // Conclusion for #245: existing downstream ViewTable providers keep the
        // original resolved provider; catalog replacement alone does not rewire them.
        assert_eq!(west_markers, vec!["original"]);
        assert_eq!(east_markers, vec!["original"]);
        Ok(())
    }

    #[tokio::test]
    async fn replanned_downstream_sql_uses_cached_alias_replacement()
    -> Result<(), Box<dyn std::error::Error>> {
        const WEST_SQL: &str = "select marker from big where region = 'west'";
        const EAST_SQL: &str = "select marker from big where region = 'east'";

        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let _big = session.register_alias("big", &pending_big)?;
        let _old_west = session.table_from_sql(WEST_SQL).await?;
        let _old_east = session.table_from_sql(EAST_SQL).await?;
        assert_eq!(source_scans.load(Ordering::SeqCst), 0);

        let cached_big = session
            .context()
            .table("big")
            .await?
            .cache()
            .await?
            .into_view();
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let removed_big = session.context().deregister_table("big")?;
        assert!(removed_big.is_some());
        session.context().register_table("big", cached_big)?;

        let direct_big = session.context().sql(WEST_SQL).await?.collect().await?;
        assert_eq!(marker_values_from_batches(&direct_big)?, vec!["shared"]);
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let replanned_west = session.table_from_sql(WEST_SQL).await?;
        let replanned_east = session.table_from_sql(EAST_SQL).await?;
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let west_stream = session.batch_stream_for_lazy_table(&replanned_west).await?;
        let east_stream = session.batch_stream_for_lazy_table(&replanned_east).await?;
        let west_markers = collect_stream_marker_values(west_stream).await?;
        let east_markers = collect_stream_marker_values(east_stream).await?;

        // Conclusion for #247: after cached big is installed under alias big,
        // replanning downstream SQL reads the cached provider and does not
        // rescan the original upstream provider per output.
        assert_eq!(west_markers, vec!["shared"]);
        assert_eq!(east_markers, vec!["shared"]);
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn planned_downstream_sql_expands_registered_derived_alias_reference()
    -> Result<(), Box<dyn std::error::Error>> {
        const MARKER_REGION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"marker\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}}]"#;

        let table = DeltaLogTable::new_with_schema("orders", MARKER_REGION_SCHEMA_FIELDS_JSON)?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        let source_dataframe = session
            .plan_read_only_sql("select marker from orders")
            .await?;
        let source_references = table_scan_proof_references(source_dataframe.logical_plan())?;
        assert_eq!(
            source_references,
            vec![TableScanProofReference {
                table_name: "orders".to_owned(),
                nested_table_names: Vec::new(),
            }]
        );
        assert!(session.registered_source("orders").is_some());
        assert!(session.registered_derived_table("orders").is_none());

        let pending_big = session
            .table_from_sql("select marker, region from orders")
            .await?;
        let _big = session.register_alias("big", &pending_big)?;
        let west_dataframe = session
            .plan_read_only_sql("select marker from BIG where region = 'west'")
            .await?;
        let east_dataframe = session
            .plan_read_only_sql("select marker from big where region = 'east'")
            .await?;
        let west_references = table_scan_proof_references(west_dataframe.logical_plan())?;
        let east_references = table_scan_proof_references(east_dataframe.logical_plan())?;

        for references in [&west_references, &east_references] {
            assert_eq!(
                references,
                &vec![TableScanProofReference {
                    table_name: "orders".to_owned(),
                    nested_table_names: Vec::new(),
                }]
            );
            assert!(
                session
                    .registered_source(&references[0].table_name)
                    .is_some()
            );
            assert!(
                session
                    .registered_derived_table(&references[0].table_name)
                    .is_none()
            );
        }

        // Conclusion for #257: DataFusion expands the registered derived
        // alias during SQL planning, so planned LogicalPlan table scans do not
        // preserve a structured west/east -> big dependency for #250.
        assert!(
            !west_references
                .iter()
                .any(|reference| reference.table_name.eq_ignore_ascii_case("big"))
        );
        assert!(
            !east_references
                .iter()
                .any(|reference| reference.table_name.eq_ignore_ascii_case("big"))
        );
        assert!(session.registered_derived_table("big").is_some());
        assert!(session.registered_source("big").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn datafusion_sql_ast_captures_session_alias_dependencies_before_planning()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let _big = session.register_alias("big", &pending_big)?;

        assert_eq!(
            ast_reference_proof("select * from big where customer_name = 'alice'")?,
            AstReferenceProof {
                relations: vec!["big".to_owned()],
                ctes: Vec::new(),
            }
        );
        assert_eq!(
            ast_reference_proof("select * from BIG where customer_name = 'alice'")?,
            AstReferenceProof {
                relations: vec!["big".to_owned()],
                ctes: Vec::new(),
            }
        );
        assert_eq!(
            ast_reference_proof("select * from big b")?,
            AstReferenceProof {
                relations: vec!["big".to_owned()],
                ctes: Vec::new(),
            }
        );
        assert_eq!(
            ast_reference_proof("select * from big join other_alias on big.id = other_alias.id")?,
            AstReferenceProof {
                relations: vec!["big".to_owned(), "other_alias".to_owned()],
                ctes: Vec::new(),
            }
        );
        assert_eq!(
            ast_reference_proof("select * from (select * from big) b")?,
            AstReferenceProof {
                relations: vec!["big".to_owned()],
                ctes: Vec::new(),
            }
        );

        let shadowed = ast_reference_proof("with big as (select * from orders) select * from big")?;
        assert_eq!(
            shadowed,
            AstReferenceProof {
                relations: vec!["orders".to_owned()],
                ctes: vec!["big".to_owned()],
            }
        );

        assert!(session.registered_derived_table("big").is_some());
        assert!(session.registered_source("big").is_none());
        assert!(session.registered_source("orders").is_some());
        assert!(session.registered_derived_table("orders").is_none());

        // Conclusion for #259: DataFusion's DFParser plus
        // resolve_table_references provides a structured pre-planning AST path
        // that captures session alias dependencies and CTE shadowing for #250.
        let derived_dependency = ast_reference_proof("select * from big")?
            .relations
            .into_iter()
            .any(|name| session.registered_derived_table(&name).is_some());
        let shadowed_derived_dependency = shadowed
            .relations
            .iter()
            .any(|name| session.registered_derived_table(name).is_some());
        assert!(derived_dependency);
        assert!(!shadowed_derived_dependency);
        Ok(())
    }

    #[tokio::test]
    async fn dry_run_to_mssql_plans_output_without_row_or_writer_side_effects()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("orders_source", source_provider)?;
        let output = session
            .table_from_sql("select marker from orders_source where region = 'west'")
            .await?;
        let request = output_request(
            output,
            "west_output",
            "west_orders",
            LoadMode::CreateAndLoad,
        )?;

        let report = session.dry_run_to_mssql(&request)?;

        assert_eq!(report.output_name(), "west_output");
        assert_eq!(report.run_mode(), RunMode::DryRun);
        assert_eq!(report.status(), OutputStatus::dry_run_planned());
        assert_eq!(
            report.validation_status(),
            ValidationStatus::skipped(ReportReasonCode::DryRun)
        );
        assert_eq!(report.target_table().schema(), Some("dbo"));
        assert_eq!(report.target_table().table(), "west_orders");
        assert_eq!(report.load_mode(), LoadMode::CreateAndLoad);
        assert_eq!(report.target_schema_plan().mappings().len(), 1);
        assert!(report.target_ddl_plan().create_table_sql().is_some());
        assert!(report.target_lifecycle_plan().create_table_sql_required());
        assert_eq!(
            report.target_lifecycle_plan().expected_target_state(),
            crate::MssqlTargetTableState::Absent
        );
        assert_eq!(
            report.planned_output().output_plan().target_table().table(),
            "west_orders"
        );
        assert_eq!(
            report
                .planned_output()
                .output_plan()
                .schema_mappings()
                .len(),
            1
        );
        assert_eq!(report.output_schema().len(), 1);
        assert_eq!(report.output_schema()[0].index(), 0);
        assert_eq!(report.output_schema()[0].name(), "marker");
        assert_eq!(report.output_schema()[0].arrow_type(), "Utf8");
        assert!(!report.output_schema()[0].nullable());
        assert_eq!(report.source_usage_status(), SourceUsageStatus::NotUsed);
        assert!(report.used_source_names().is_empty());
        assert_eq!(report.output_row_count(), crate::RowCount::unavailable());
        assert_eq!(
            report.output_row_count_reason(),
            Some(ReportReasonCode::NotExecuted)
        );
        assert_eq!(
            report.sql_identity().state(),
            MssqlDryRunSqlIdentityState::Present
        );
        assert_eq!(report.sql_identity().hash(), Some("a65390dacb7eb6f1"));
        assert_eq!(report.sql_identity().reason(), None);
        let debug = format!("{report:?}");
        assert!(debug.contains("a65390dacb7eb6f1"));
        assert!(!debug.contains("select marker"));
        assert!(!debug.contains("region = 'west'"));
        assert!(!report.sql_server_contacted());
        assert!(!report.row_production_started());
        assert!(!report.table_lifecycle_started());
        assert!(!report.bulk_writer_started());
        assert_eq!(source_scans.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn dry_run_all_to_mssql_reports_all_outputs_without_row_side_effects()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("orders_source", source_provider)?;
        let west = session
            .table_from_sql("select marker from orders_source where region = 'west'")
            .await?;
        let east = session
            .table_from_sql("select marker from orders_source where region = 'east'")
            .await?;
        let west_table_id = west.id();
        let west_table_name = west.name().to_owned();
        let west = output_request(west, "west_output", "west_orders", LoadMode::CreateAndLoad)?;
        let east = output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;

        let report = session.dry_run_all_to_mssql(&[west, east])?;

        assert_eq!(report.run_mode(), RunMode::DryRun);
        assert_eq!(report.status(), WorkflowStatus::success());
        assert_eq!(report.len(), 2);
        assert!(!report.is_empty());
        assert_eq!(report.outputs()[0].output_name(), "west_output");
        assert_eq!(report.outputs()[1].output_name(), "east_output");
        assert_eq!(report.outputs()[0].table_id(), west_table_id);
        assert_eq!(report.outputs()[0].table_kind(), LazyTableKind::DerivedSql);
        assert_eq!(report.outputs()[0].table_name(), west_table_name);
        assert_eq!(
            report.outputs()[0].status(),
            OutputStatus::dry_run_planned()
        );
        assert_eq!(
            report.outputs()[0].validation_status(),
            ValidationStatus::skipped(ReportReasonCode::DryRun)
        );
        assert_eq!(
            report.outputs()[0].output_row_count(),
            crate::RowCount::unavailable()
        );
        assert_eq!(
            report.outputs()[0].output_row_count_reason(),
            Some(ReportReasonCode::NotExecuted)
        );
        assert!(report.sources().is_empty());
        assert_eq!(report.outputs()[0].target_table().table(), "west_orders");
        assert_eq!(report.outputs()[0].load_mode(), LoadMode::CreateAndLoad);
        assert_eq!(
            report.outputs()[0]
                .target_lifecycle_plan()
                .expected_target_state(),
            crate::MssqlTargetTableState::Absent
        );
        assert_eq!(report.outputs()[1].target_table().table(), "east_orders");
        assert_eq!(report.outputs()[1].load_mode(), LoadMode::AppendExisting);
        assert_eq!(
            report.outputs()[1]
                .target_lifecycle_plan()
                .expected_target_state(),
            crate::MssqlTargetTableState::Exists
        );
        assert!(!report.sql_server_contacted());
        assert!(!report.row_production_started());
        assert!(!report.table_lifecycle_started());
        assert!(!report.bulk_writer_started());
        assert_eq!(source_scans.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn dry_run_all_to_mssql_includes_registered_delta_source_reports()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = output_request(
            source,
            "orders_output",
            "orders_sink",
            LoadMode::CreateAndLoad,
        )?;

        let report = session.dry_run_all_to_mssql(&[request])?;

        assert_eq!(report.outputs().len(), 1);
        assert!(!report.query_used_source_scan_metadata_exhausted());
        assert_eq!(
            report.outputs()[0].sql_identity().state(),
            MssqlDryRunSqlIdentityState::Absent
        );
        assert_eq!(report.outputs()[0].sql_identity().hash(), None);
        assert_eq!(report.outputs()[0].sql_identity().reason(), None);
        assert_eq!(
            report.outputs()[0].source_usage_status(),
            SourceUsageStatus::Used
        );
        assert_eq!(
            report.outputs()[0].used_source_names(),
            &["orders".to_owned()]
        );
        assert_eq!(report.sources().len(), 1);
        let source = &report.sources()[0];
        assert_eq!(source.source_name(), "orders");
        assert_eq!(source.snapshot_version(), 1);
        assert_eq!(source.protocol().source_name, "orders");
        assert_eq!(source.file_count(), crate::FileCount::unavailable());
        assert_eq!(
            source.file_count_reason(),
            Some(crate::ReportReasonCode::CostAvoidance)
        );
        assert!(!source.scan_metadata_exhausted());
        assert_eq!(source.usage_status(), SourceUsageStatus::Used);
        assert_eq!(source.used_by_output_names(), &["orders_output".to_owned()]);
        assert!(!report.row_production_started());
        Ok(())
    }

    #[tokio::test]
    async fn dry_run_all_to_mssql_scan_summary_exhausts_provider_metadata_without_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new()
                .with_default_mssql_connection(secret_connection()?)
                .with_validation_options(ValidationOptions::new().with_dry_run_scan_summary_mode(
                    crate::DryRunScanSummaryMode::ExhaustScanMetadata,
                )),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = output_request(
            source,
            "orders_output",
            "orders_sink",
            LoadMode::CreateAndLoad,
        )?;

        let report = session
            .dry_run_all_to_mssql_with_scan_summary(&[request])
            .await?;

        assert_eq!(report.outputs().len(), 1);
        assert!(!report.outputs()[0].row_production_started());
        assert_eq!(report.sources().len(), 1);
        let source = &report.sources()[0];
        assert_eq!(source.source_name(), "orders");
        assert_eq!(source.usage_status(), SourceUsageStatus::Used);
        assert_eq!(source.used_by_output_names(), &["orders_output".to_owned()]);
        assert_eq!(source.provider_stats_reason(), None);
        let stats = source
            .provider_read_stats()
            .ok_or("expected provider stats from dry-run scan summary")?;
        assert_eq!(stats.source_name, "orders");
        assert_eq!(stats.files_started, 0);
        assert_eq!(stats.files_completed, 0);
        assert_eq!(stats.batches_produced, 0);
        assert_eq!(stats.rows_produced, 0);
        match stats.scan_metadata_exhausted {
            Some(true) => {
                assert_eq!(
                    source.file_count(),
                    crate::FileCount::exact(stats.files_planned)
                );
                assert_eq!(source.file_count_reason(), None);
            }
            Some(false) => {
                assert_eq!(
                    source.file_count(),
                    crate::FileCount::estimated(stats.files_planned)
                );
                assert_eq!(source.file_count_reason(), None);
            }
            None => {
                assert_eq!(source.file_count(), crate::FileCount::unavailable());
                assert_eq!(
                    source.file_count_reason(),
                    Some(crate::ReportReasonCode::CapabilityUnavailable)
                );
            }
        }
        assert_eq!(
            report.query_used_source_scan_metadata_exhausted(),
            source.scan_metadata_exhausted()
        );
        Ok(())
    }

    #[tokio::test]
    async fn dry_run_all_to_mssql_reports_multi_source_usage_when_lineage_is_known()
    -> Result<(), Box<dyn std::error::Error>> {
        let orders_table = DeltaLogTable::new("orders")?;
        let customers_table = DeltaLogTable::new("customers")?;
        let inventory_table = DeltaLogTable::new("inventory")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        session.delta_lake(DeltaSourceConfig::new("orders", orders_table.uri()))?;
        session.delta_lake(DeltaSourceConfig::new("customers", customers_table.uri()))?;
        session.delta_lake(DeltaSourceConfig::new("inventory", inventory_table.uri()))?;
        let joined = session
            .table_from_sql(
                "select orders.id from orders inner join customers on orders.id = customers.id",
            )
            .await?;
        let request = output_request(
            joined,
            "joined_output",
            "joined_sink",
            LoadMode::CreateAndLoad,
        )?;

        let report = session.dry_run_all_to_mssql(&[request])?;

        assert!(!report.query_used_source_scan_metadata_exhausted());
        assert_eq!(
            report.outputs()[0].source_usage_status(),
            SourceUsageStatus::Used
        );
        assert_eq!(
            report.outputs()[0].used_source_names(),
            &["orders".to_owned(), "customers".to_owned()]
        );
        assert_eq!(report.sources().len(), 3);
        for source in report.sources() {
            match source.source_name() {
                "orders" | "customers" => {
                    assert_eq!(source.usage_status(), SourceUsageStatus::Used);
                    assert_eq!(source.used_by_output_names(), &["joined_output".to_owned()]);
                }
                "inventory" => {
                    assert_eq!(source.usage_status(), SourceUsageStatus::NotUsed);
                    assert!(source.used_by_output_names().is_empty());
                }
                name => return Err(format!("unexpected source report: {name}").into()),
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn dry_run_all_to_mssql_rejects_execute_request_before_row_side_effects()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("orders_source", source_provider)?;
        let output = session
            .table_from_sql("select marker from orders_source")
            .await?;
        let request = execute_output_request(
            output,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;

        let error = session.dry_run_all_to_mssql(&[request]);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("dry_run_all_to_mssql requires RunMode::DryRun")
        ));
        assert_eq!(source_scans.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn dry_run_all_to_mssql_rejects_missing_connection_before_row_side_effects()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("orders_source", source_provider)?;
        let output = session
            .table_from_sql("select marker from orders_source")
            .await?;
        let request = output_request(
            output,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;

        let error = session.dry_run_all_to_mssql(&[request]);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MissingMssqlConnection { output_name })
                if output_name == "orders_output"
        ));
        assert_eq!(source_scans.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn dry_run_all_to_mssql_rejects_duplicate_output_names()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let west = session.table_from_sql("select 1 as id").await?;
        let east = session.table_from_sql("select 2 as id").await?;
        let west = output_request(
            west,
            "orders_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let east = output_request(
            east,
            "orders_output",
            "east_orders",
            LoadMode::AppendExisting,
        )?;

        let error = session.dry_run_all_to_mssql(&[west, east]);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("write_all output names must be unique")
                    && message.contains("orders_output")
        ));
        Ok(())
    }

    #[tokio::test]
    async fn write_to_mssql_requires_effective_connection_before_stream_setup()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = execute_output_request(
            source,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;

        let error = session.write_to_mssql(&request).await;

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MissingMssqlConnection { output_name })
                if output_name == "orders_output"
        ));
        Ok(())
    }

    #[tokio::test]
    async fn write_to_mssql_with_writer_hands_query_stream_to_one_output_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let derived = session
            .table_from_sql("select 1 as id union all select 2 as id")
            .await?;
        let request = execute_output_request(
            derived,
            "orders_output",
            "orders_sink",
            LoadMode::CreateAndLoad,
        )?;
        let mut writer = FakeOrchestratorWriter::default();

        let report = session
            .write_to_mssql_with_writer(&request, &mut writer)
            .await?;

        assert_eq!(writer.calls.len(), 1);
        let call = writer.calls.first().ok_or("expected fake writer call")?;
        assert_eq!(call.output_name, "orders_output");
        assert_eq!(call.target_table.schema(), Some("dbo"));
        assert_eq!(call.target_table.table(), "orders_sink");
        assert_eq!(
            call.connection_source,
            MssqlConnectionSource::ContextDefault
        );
        assert_eq!(call.rows, 2);
        assert!(call.batches >= 1);
        assert_eq!(call.schema_fields, 1);
        assert_eq!(report.output_name(), "orders_output");
        assert_eq!(report.stats().rows_written(), 2);
        assert_eq!(report.stats().batches_written(), call.batches);
        assert_eq!(report.cleanup(), MssqlTargetCleanupStatus::NotApplicable);
        Ok(())
    }

    #[tokio::test]
    async fn write_to_mssql_with_writer_executes_real_delta_source_fixture()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        session.delta_lake(DeltaSourceConfig::new(
            "orders",
            table.path().to_string_lossy().to_string(),
        ))?;
        let selected_orders = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let request = execute_output_request(
            selected_orders,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;
        let mut writer = FakeOrchestratorWriter::default();

        let report = session
            .write_to_mssql_with_writer(&request, &mut writer)
            .await?;

        assert_eq!(writer.calls.len(), 1);
        let call = writer.calls.first().ok_or("expected fake writer call")?;
        assert_eq!(call.output_name, "orders_output");
        assert_eq!(call.rows, u64::try_from(table.rows())?);
        assert!(call.batches >= 1);
        assert_eq!(call.schema_fields, 2);
        assert_eq!(report.stats().rows_written(), u64::try_from(table.rows())?);
        assert_eq!(report.stats().batches_written(), call.batches);
        Ok(())
    }

    #[tokio::test]
    async fn write_to_mssql_with_writer_executes_multi_source_delta_join_fixture()
    -> Result<(), Box<dyn std::error::Error>> {
        let orders = RealParquetDeltaTable::new_default("orders")?;
        let customers = RealParquetDeltaTable::new_default("customers")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        session.delta_lake(DeltaSourceConfig::new(
            "orders",
            orders.path().to_string_lossy().to_string(),
        ))?;
        session.delta_lake(DeltaSourceConfig::new(
            "customers",
            customers.path().to_string_lossy().to_string(),
        ))?;
        let joined = session
            .table_from_sql(
                "select o.id, c.customer_name \
                 from orders o \
                 join customers c on o.id = c.id",
            )
            .await?;
        let request = execute_output_request(
            joined,
            "joined_output",
            "joined_sink",
            LoadMode::AppendExisting,
        )?;
        let mut writer = FakeOrchestratorWriter::default();

        let report = session
            .write_to_mssql_with_writer(&request, &mut writer)
            .await?;

        assert_eq!(writer.calls.len(), 1);
        let call = writer.calls.first().ok_or("expected fake writer call")?;
        assert_eq!(call.output_name, "joined_output");
        assert_eq!(call.rows, 3);
        assert!(call.batches >= 1);
        assert_eq!(call.schema_fields, 2);
        assert_eq!(report.stats().rows_written(), 3);
        assert_eq!(report.stats().batches_written(), call.batches);
        Ok(())
    }

    #[tokio::test]
    async fn write_to_mssql_rejects_dry_run_before_planning_or_writer()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let derived = session.table_from_sql("select 1 as id").await?;
        let request = output_request(
            derived,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;
        let mut writer = FakeOrchestratorWriter::default();

        let error = session
            .write_to_mssql_with_writer(&request, &mut writer)
            .await;

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("RunMode::Execute")
                    && message.contains("dry_run_to_mssql")
        ));
        assert!(writer.calls.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn plan_write_all_outputs_plans_valid_outputs_in_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let west = session.table_from_sql("select 1 as id").await?;
        let east = session.table_from_sql("select 2 as id").await?;
        let west =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east =
            execute_output_request(east, "east_output", "east_orders", LoadMode::CreateAndLoad)?;

        let planned = session.plan_write_all_outputs(&[west, east])?;

        assert_eq!(planned.len(), 2);
        assert_eq!(planned[0].output_plan().output_name(), "west_output");
        assert_eq!(
            planned[0].output_plan().target_table().table(),
            "west_orders"
        );
        assert_eq!(planned[1].output_plan().output_name(), "east_output");
        assert_eq!(
            planned[1].output_plan().target_table().table(),
            "east_orders"
        );
        Ok(())
    }

    #[tokio::test]
    async fn plan_write_all_outputs_rejects_duplicate_output_names()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let west = session.table_from_sql("select 1 as id").await?;
        let east = session.table_from_sql("select 2 as id").await?;
        let west = execute_output_request(
            west,
            "orders_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let east = execute_output_request(
            east,
            "orders_output",
            "east_orders",
            LoadMode::AppendExisting,
        )?;

        let error = session.plan_write_all_outputs(&[west, east]);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("write_all output names must be unique")
                    && message.contains("orders_output")
        ));
        Ok(())
    }

    #[tokio::test]
    async fn plan_write_all_outputs_rejects_missing_connection_before_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let west = session.table_from_sql("select 1 as id").await?;
        let east = session.table_from_sql("select 2 as id").await?;
        let west =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east =
            execute_output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;

        let error = session.plan_write_all_outputs(&[west, east]);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MissingMssqlConnection { output_name })
                if output_name == "west_output"
        ));
        Ok(())
    }

    #[tokio::test]
    async fn plan_write_all_outputs_rejects_replace_before_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let west = session.table_from_sql("select 1 as id").await?;
        let east = session.table_from_sql("select 2 as id").await?;
        let west =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east = execute_output_request(east, "east_output", "east_orders", LoadMode::Replace)?;

        let error = session.plan_write_all_outputs(&[west, east]);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlLifecyclePlanning { output_name, message })
                if output_name == "east_output"
                    && message.contains("replace load mode is reserved")
        ));
        Ok(())
    }

    #[tokio::test]
    async fn plan_write_all_outputs_rejects_dry_run_before_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let west = session.table_from_sql("select 1 as id").await?;
        let west = output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;

        let error = session.plan_write_all_outputs(&[west]);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("write_all requires RunMode::Execute")
                    && message.contains("dry_run_all_to_mssql")
        ));
        Ok(())
    }

    #[tokio::test]
    async fn build_write_all_baseline_jobs_preserves_output_metadata_without_stream_setup()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("orders_source", source_provider)?;
        let west = session
            .table_from_sql("select marker from orders_source where region = 'west'")
            .await?;
        let east = session
            .table_from_sql("select marker from orders_source where region = 'east'")
            .await?;
        let west =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east =
            execute_output_request(east, "east_output", "east_orders", LoadMode::CreateAndLoad)?;
        let planned = session.plan_write_all_outputs(&[west, east])?;
        assert_eq!(source_scans.load(Ordering::SeqCst), 0);

        let jobs = session.build_write_all_baseline_jobs(&planned)?;

        assert_eq!(source_scans.load(Ordering::SeqCst), 0);
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].output_name(), "west_output");
        assert_eq!(jobs[0].target_summary().table().table(), "west_orders");
        assert_eq!(jobs[1].output_name(), "east_output");
        assert_eq!(jobs[1].target_summary().table().table(), "east_orders");
        Ok(())
    }

    #[tokio::test]
    async fn write_all_with_writer_executes_valid_outputs_in_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let west = session
            .table_from_sql("select 1 as id union all select 2 as id")
            .await?;
        let east = session.table_from_sql("select 3 as id").await?;
        let west =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east =
            execute_output_request(east, "east_output", "east_orders", LoadMode::CreateAndLoad)?;
        let writer = FakeWorkflowWriter::default();
        let calls = writer.calls();

        let report = session.write_all_with_writer(&[west, east], writer).await?;
        let calls = calls
            .lock()
            .map_err(|_| "fake workflow call lock poisoned")?;

        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].output_name, "west_output");
        assert_eq!(calls[0].target_table.table(), "west_orders");
        assert_eq!(calls[0].rows, 2);
        assert_eq!(calls[1].output_name, "east_output");
        assert_eq!(calls[1].target_table.table(), "east_orders");
        assert_eq!(calls[1].rows, 1);
        assert_eq!(report.len(), 2);
        assert!(report.all_succeeded());
        assert_eq!(report.outputs()[0].output_name(), "west_output");
        assert_eq!(report.outputs()[1].output_name(), "east_output");
        assert_eq!(report.workflow().outputs(), report.outputs());
        let crate::sql_server::MssqlOutputWriteStatus::Succeeded(west_report) =
            &report.outputs()[0]
        else {
            return Err(format!("expected succeeded status, got {:?}", report.outputs()[0]).into());
        };
        assert_eq!(west_report.stats().rows_written(), 2);
        assert_eq!(west_report.stats().batches_written(), calls[0].batches);
        assert_eq!(
            west_report.cleanup(),
            MssqlTargetCleanupStatus::NotApplicable
        );
        Ok(())
    }

    #[tokio::test]
    async fn write_all_with_writer_reports_delta_sources_for_executed_outputs()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        session.delta_lake(DeltaSourceConfig::new(
            "orders",
            table.path().to_string_lossy().to_string(),
        ))?;
        let selected_orders = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let request = execute_output_request(
            selected_orders,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;
        let writer = FakeWorkflowWriter::default();

        let report = session
            .write_all_with_options_and_writer(
                &[request],
                WriteAllOptions::new().with_cache_mode(WriteAllCacheMode::Disabled),
                writer,
            )
            .await?;

        assert_eq!(report.outputs().len(), 1);
        assert_eq!(report.outputs()[0].output_name(), "orders_output");
        assert_eq!(report.sources().len(), 1);
        let source = &report.sources()[0];
        assert_eq!(source.source_name(), "orders");
        assert_eq!(source.usage_status(), SourceUsageStatus::Used);
        assert_eq!(source.used_by_output_names(), &["orders_output".to_owned()]);
        assert_eq!(source.provider_stats_reason(), None);
        let stats = source
            .provider_read_stats()
            .ok_or("expected execution provider stats")?;
        assert_eq!(stats.source_name, "orders");
        assert_eq!(stats.snapshot_version, source.snapshot_version());
        assert!(stats.files_started > 0);
        assert_eq!(stats.files_started, stats.files_completed);
        assert!(stats.rows_produced > 0);
        assert!(stats.batches_produced > 0);
        match stats.scan_metadata_exhausted {
            Some(true) => {
                assert_eq!(
                    source.file_count(),
                    crate::FileCount::exact(stats.files_planned)
                );
                assert_eq!(source.file_count_reason(), None);
            }
            Some(false) => {
                assert_eq!(
                    source.file_count(),
                    crate::FileCount::estimated(stats.files_planned)
                );
                assert_eq!(source.file_count_reason(), None);
            }
            None => {
                assert_eq!(source.file_count(), crate::FileCount::unavailable());
                assert_eq!(
                    source.file_count_reason(),
                    Some(crate::ReportReasonCode::CapabilityUnavailable)
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn write_all_keeps_source_rows_separate_from_output_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        session.delta_lake(DeltaSourceConfig::new(
            "orders",
            table.path().to_string_lossy().to_string(),
        ))?;
        let aggregate = session
            .table_from_sql("select count(*) as order_count from orders")
            .await?;
        let request = execute_output_request(
            aggregate,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;
        let writer = FakeWorkflowWriter::default();

        let report = session
            .write_all_with_options_and_writer(
                &[request],
                WriteAllOptions::new().with_cache_mode(WriteAllCacheMode::Disabled),
                writer,
            )
            .await?;

        let crate::sql_server::MssqlOutputWriteStatus::Succeeded(output_report) =
            &report.outputs()[0]
        else {
            return Err(format!("expected succeeded status, got {:?}", report.outputs()[0]).into());
        };
        assert_eq!(output_report.stats().rows_written(), 1);
        let source = report
            .sources()
            .first()
            .ok_or("expected executed source report")?;
        let stats = source
            .provider_read_stats()
            .ok_or("expected execution provider stats")?;
        assert_eq!(stats.rows_produced, u64::try_from(table.rows())?);
        assert_ne!(stats.rows_produced, output_report.stats().rows_written());
        Ok(())
    }

    #[tokio::test]
    async fn write_all_auto_no_candidate_uses_baseline_path()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("orders_source", source_provider)?;
        let west = session
            .table_from_sql("select marker from orders_source where region = 'west'")
            .await?;
        let east = session
            .table_from_sql("select marker from orders_source where region = 'east'")
            .await?;
        let west =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east =
            execute_output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;
        let writer = FakeWorkflowWriter::default();
        let calls = writer.calls();

        let report = session.write_all_with_writer(&[west, east], writer).await?;
        let calls = calls
            .lock()
            .map_err(|_| "fake workflow call lock poisoned")?;

        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].output_name, "west_output");
        assert_eq!(calls[0].rows, 1);
        assert_eq!(calls[1].output_name, "east_output");
        assert_eq!(calls[1].rows, 1);
        assert_eq!(source_scans.load(Ordering::SeqCst), 2);
        assert!(report.all_succeeded());
        assert!(matches!(
            report.cache(),
            WriteAllCacheReport::NoCache {
                reason: WriteAllNoCacheReason::NoSharedRegisteredDerivedAlias,
                skipped_candidates
            } if skipped_candidates.is_empty()
        ));
        Ok(())
    }

    #[tokio::test]
    async fn write_all_auto_caches_shared_alias_for_direct_and_dependent_outputs()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let big_output = execute_output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let west_output =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let writer = FakeWorkflowWriter::default();
        let calls = writer.calls();

        let report = session
            .write_all_with_writer(&[big_output, west_output], writer)
            .await?;
        {
            let calls = calls
                .lock()
                .map_err(|_| "fake workflow call lock poisoned")?;

            assert_eq!(calls.len(), 2);
            assert_eq!(calls[0].output_name, "big_output");
            assert_eq!(calls[0].rows, 2);
            assert_eq!(calls[1].output_name, "west_output");
            assert_eq!(calls[1].rows, 1);
            assert_eq!(source_scans.load(Ordering::SeqCst), 1);
            assert!(report.all_succeeded());
        }
        let WriteAllCacheReport::CacheAliases {
            aliases,
            skipped_candidates,
        } = report.cache()
        else {
            return Err(format!("expected cache aliases report, got {:?}", report.cache()).into());
        };
        assert!(skipped_candidates.is_empty());
        assert_eq!(aliases.len(), 1);
        assert_eq!(aliases[0].table_id(), big.id());
        assert_eq!(aliases[0].alias(), "big");
        assert_eq!(aliases[0].output_indexes(), &[0, 1]);
        assert_eq!(
            aliases[0].status(),
            WriteAllCacheAliasStatus::MaterializedAndRestored
        );

        let restored_big_factory = session.lazy_table_batch_stream_factory(big);
        let restored_big_rows = collect_stream_row_count(restored_big_factory().await?).await?;
        assert_eq!(restored_big_rows, 2);
        assert_eq!(source_scans.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn write_all_report_debug_redacts_connections_and_retained_sql()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, _source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql(
                "select 'super-secret-literal' as marker, region \
                 from big_source",
            )
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let big_output =
            execute_output_request(big, "big_output", "big_orders", LoadMode::AppendExisting)?;
        let override_target_config =
            MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "west_orders")?)
                .with_load_mode(LoadMode::AppendExisting)
                .with_connection(override_connection()?);
        let west_output = OutputWritePlan::new(
            west,
            MssqlOutputTarget::new("west_output", override_target_config, RunMode::Execute),
        );
        let writer = FakeWorkflowWriter::default();

        let report = session
            .write_all_with_writer(&[big_output, west_output], writer)
            .await?;

        let debug = format!("{report:?}");
        assert!(debug.contains("warehouse-primary"));
        assert!(debug.contains("warehouse-override"));
        assert!(debug.contains("CacheAliases"));
        assert!(debug.contains("MaterializedAndRestored"));
        assert!(!debug.contains("secret-token"));
        assert!(!debug.contains("override-secret"));
        assert!(!debug.contains("super-secret-literal"));
        Ok(())
    }

    #[tokio::test]
    async fn write_all_auto_caches_multiple_shared_aliases_for_dependent_outputs()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (big_source_provider, big_source_scans) = scan_counting_marker_region_provider("big")?;
        let (names_source_provider, names_source_scans) =
            scan_counting_marker_region_provider("names")?;
        session
            .context()
            .register_table("big_source", big_source_provider)?;
        session
            .context()
            .register_table("names_source", names_source_provider)?;
        let pending_big = session
            .table_from_sql(
                "select marker as big_marker, region, \
                 case when region = 'west' then 1 else 2 end as id \
                 from big_source",
            )
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let pending_names = session
            .table_from_sql(
                "select marker as name_marker, region, \
                 case when region = 'west' then 1 else 2 end as id \
                 from names_source",
            )
            .await?;
        let names = session.register_alias("names", &pending_names)?;
        let west = session
            .table_from_sql(
                "select big.id, big.big_marker, names.name_marker \
                 from big join names on big.id = names.id \
                 where big.region = 'west'",
            )
            .await?;
        let east = session
            .table_from_sql(
                "select big.id, big.big_marker, names.name_marker \
                 from big join names on big.id = names.id \
                 where big.region = 'east'",
            )
            .await?;
        let west_output =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east_output =
            execute_output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;
        let writer = FakeWorkflowWriter::default();
        let calls = writer.calls();

        let report = session
            .write_all_with_writer(&[west_output, east_output], writer)
            .await?;
        {
            let calls = calls
                .lock()
                .map_err(|_| "fake workflow call lock poisoned")?;

            assert_eq!(calls.len(), 2);
            assert_eq!(calls[0].output_name, "west_output");
            assert_eq!(calls[0].rows, 1);
            assert_eq!(calls[1].output_name, "east_output");
            assert_eq!(calls[1].rows, 1);
            assert_eq!(big_source_scans.load(Ordering::SeqCst), 1);
            assert_eq!(names_source_scans.load(Ordering::SeqCst), 1);
            assert!(report.all_succeeded());
        }
        let WriteAllCacheReport::CacheAliases {
            aliases,
            skipped_candidates,
        } = report.cache()
        else {
            return Err(format!("expected cache aliases report, got {:?}", report.cache()).into());
        };
        assert!(skipped_candidates.is_empty());
        assert_eq!(aliases.len(), 2);
        assert_eq!(aliases[0].table_id(), big.id());
        assert_eq!(aliases[0].alias(), "big");
        assert_eq!(aliases[0].output_indexes(), &[0, 1]);
        assert_eq!(
            aliases[0].status(),
            WriteAllCacheAliasStatus::MaterializedAndRestored
        );
        assert_eq!(aliases[1].table_id(), names.id());
        assert_eq!(aliases[1].alias(), "names");
        assert_eq!(aliases[1].output_indexes(), &[0, 1]);
        assert_eq!(
            aliases[1].status(),
            WriteAllCacheAliasStatus::MaterializedAndRestored
        );

        let restored_big_factory = session.lazy_table_batch_stream_factory(big);
        let restored_names_factory = session.lazy_table_batch_stream_factory(names);
        assert_eq!(
            collect_stream_row_count(restored_big_factory().await?).await?,
            2
        );
        assert_eq!(
            collect_stream_row_count(restored_names_factory().await?).await?,
            2
        );
        assert_eq!(big_source_scans.load(Ordering::SeqCst), 2);
        assert_eq!(names_source_scans.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn write_all_auto_restores_cache_alias_after_output_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let east = session
            .table_from_sql("select marker from big where region = 'east'")
            .await?;
        let big_output = execute_output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let west_output =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east_output =
            execute_output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;
        let writer = FakeWorkflowWriter::failing_on("west_output");
        let calls = writer.calls();

        let report = session
            .write_all_with_writer(&[big_output, west_output, east_output], writer)
            .await?;
        {
            let calls = calls
                .lock()
                .map_err(|_| "fake workflow call lock poisoned")?;

            assert_eq!(calls.len(), 2);
            assert_eq!(calls[0].output_name, "big_output");
            assert_eq!(calls[0].rows, 2);
            assert_eq!(calls[1].output_name, "west_output");
            assert_eq!(calls[1].rows, 1);
            assert_eq!(source_scans.load(Ordering::SeqCst), 1);
            assert_eq!(report.succeeded_count(), 1);
            assert_eq!(report.failed_count(), 1);
            assert_eq!(report.skipped_count(), 1);
            assert!(report.outputs()[0].is_succeeded());
            assert_eq!(report.outputs()[0].output_name(), "big_output");
            assert!(report.outputs()[1].is_failed());
            assert_eq!(report.outputs()[1].output_name(), "west_output");
            assert!(report.outputs()[2].is_skipped());
            assert_eq!(report.outputs()[2].output_name(), "east_output");
        }
        let WriteAllCacheReport::CacheAliases {
            aliases,
            skipped_candidates,
        } = report.cache()
        else {
            return Err(format!("expected cache aliases report, got {:?}", report.cache()).into());
        };
        assert!(skipped_candidates.is_empty());
        assert_eq!(aliases.len(), 1);
        assert_eq!(aliases[0].table_id(), big.id());
        assert_eq!(aliases[0].alias(), "big");
        assert_eq!(aliases[0].output_indexes(), &[0, 1, 2]);
        assert_eq!(
            aliases[0].status(),
            WriteAllCacheAliasStatus::MaterializedAndRestored
        );

        let restored_big_factory = session.lazy_table_batch_stream_factory(big);
        let restored_big_rows = collect_stream_row_count(restored_big_factory().await?).await?;
        assert_eq!(restored_big_rows, 2);
        assert_eq!(source_scans.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn write_all_auto_cache_materialization_failure_prevents_output_attempts()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, source_scans) = failing_scan_marker_region_provider();
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let east = session
            .table_from_sql("select marker from big where region = 'east'")
            .await?;
        let west_output =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east_output =
            execute_output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;
        let writer = FakeWorkflowWriter::default();
        let calls = writer.calls();

        let error = session
            .write_all_with_writer(&[west_output, east_output], writer)
            .await;
        {
            let calls = calls
                .lock()
                .map_err(|_| "fake workflow call lock poisoned")?;

            assert!(matches!(
                error,
                Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                    if message.contains("scoped MSSQL cache alias materialize failed")
                        && message.contains("big")
            ));
            assert!(calls.is_empty());
            assert_eq!(source_scans.load(Ordering::SeqCst), 1);
        }

        let restored_error = match session.batch_stream_for_lazy_table(&big).await {
            Ok(stream) => match collect_stream_row_count(stream).await {
                Ok(rows) => {
                    return Err(
                        format!("expected restored big read to fail, got {rows} rows").into(),
                    );
                }
                Err(error) => error,
            },
            Err(error) => error,
        };
        assert!(matches!(
            &restored_error,
            DeltaFunnelError::BatchPipeline { message, .. }
                if message.contains("forced scan planning failure")
        ));
        assert_eq!(source_scans.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn write_all_auto_restores_replaced_alias_after_later_cache_materialization_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (big_source_provider, big_source_scans) = scan_counting_marker_region_provider("big")?;
        let (names_source_provider, names_source_scans) = failing_scan_marker_region_provider();
        session
            .context()
            .register_table("big_source", big_source_provider)?;
        session
            .context()
            .register_table("names_source", names_source_provider)?;
        let pending_big = session
            .table_from_sql(
                "select marker as big_marker, region, \
                 case when region = 'west' then 1 else 2 end as id \
                 from big_source",
            )
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let pending_names = session
            .table_from_sql(
                "select marker as name_marker, region, \
                 case when region = 'west' then 1 else 2 end as id \
                 from names_source",
            )
            .await?;
        let names = session.register_alias("names", &pending_names)?;
        let west = session
            .table_from_sql(
                "select big.id, big.big_marker, names.name_marker \
                 from big join names on big.id = names.id \
                 where big.region = 'west'",
            )
            .await?;
        let east = session
            .table_from_sql(
                "select big.id, big.big_marker, names.name_marker \
                 from big join names on big.id = names.id \
                 where big.region = 'east'",
            )
            .await?;
        let west_output =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east_output =
            execute_output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;
        let writer = FakeWorkflowWriter::default();
        let calls = writer.calls();

        let error = session
            .write_all_with_writer(&[west_output, east_output], writer)
            .await;
        {
            let calls = calls
                .lock()
                .map_err(|_| "fake workflow call lock poisoned")?;

            assert!(matches!(
                error,
                Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                    if message.contains("scoped MSSQL cache alias materialize failed")
                        && message.contains("names")
            ));
            assert!(calls.is_empty());
            assert_eq!(big_source_scans.load(Ordering::SeqCst), 1);
            assert_eq!(names_source_scans.load(Ordering::SeqCst), 1);
        }

        let restored_big_factory = session.lazy_table_batch_stream_factory(big);
        assert_eq!(
            collect_stream_row_count(restored_big_factory().await?).await?,
            2
        );
        assert_eq!(big_source_scans.load(Ordering::SeqCst), 2);

        let restored_names_error = match session.batch_stream_for_lazy_table(&names).await {
            Ok(stream) => match collect_stream_row_count(stream).await {
                Ok(rows) => {
                    return Err(
                        format!("expected restored names read to fail, got {rows} rows").into(),
                    );
                }
                Err(error) => error,
            },
            Err(error) => error,
        };
        assert!(matches!(
            &restored_names_error,
            DeltaFunnelError::BatchPipeline { message, .. }
                if message.contains("forced scan planning failure")
        ));
        assert_eq!(names_source_scans.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn write_all_auto_reports_dependent_stream_setup_failure_before_writer()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let east = session
            .table_from_sql("select marker from big where region = 'east'")
            .await?;
        let pending_west = session
            .pending_derived_tables
            .iter_mut()
            .find(|pending| pending.table.id() == west.id())
            .ok_or("expected pending west table")?;
        pending_west.schema = Arc::new(Schema::new(vec![Field::new(
            "different_marker",
            DataType::Utf8,
            false,
        )]));
        let big_output = execute_output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let west_output =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east_output =
            execute_output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;
        let writer = FakeWorkflowWriter::default();
        let calls = writer.calls();

        let report = session
            .write_all_with_writer(&[big_output, west_output, east_output], writer)
            .await?;
        {
            let calls = calls
                .lock()
                .map_err(|_| "fake workflow call lock poisoned")?;

            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0].output_name, "big_output");
            assert_eq!(calls[0].rows, 2);
            assert_eq!(source_scans.load(Ordering::SeqCst), 1);
            assert_eq!(report.succeeded_count(), 1);
            assert_eq!(report.failed_count(), 1);
            assert_eq!(report.skipped_count(), 1);
            assert!(report.outputs()[0].is_succeeded());
            assert_eq!(report.outputs()[0].output_name(), "big_output");
            assert!(report.outputs()[1].is_failed());
            assert_eq!(report.outputs()[1].output_name(), "west_output");
            let failure_message = match &report.outputs()[1] {
                crate::sql_server::MssqlOutputWriteStatus::Failed(failure) => failure.error(),
                status => return Err(format!("expected failed status, got {status:?}").into()),
            };
            assert!(
                failure_message.contains("cached output stream setup failed for `west_output`")
            );
            assert!(failure_message.contains("replanned output schema does not match"));
            assert!(report.outputs()[2].is_skipped());
            assert_eq!(report.outputs()[2].output_name(), "east_output");
        }
        let WriteAllCacheReport::CacheAliases {
            aliases,
            skipped_candidates,
        } = report.cache()
        else {
            return Err(format!("expected cache aliases report, got {:?}", report.cache()).into());
        };
        assert!(skipped_candidates.is_empty());
        assert_eq!(aliases.len(), 1);
        assert_eq!(aliases[0].table_id(), big.id());
        assert_eq!(aliases[0].alias(), "big");
        assert_eq!(aliases[0].output_indexes(), &[0, 1, 2]);
        assert_eq!(
            aliases[0].status(),
            WriteAllCacheAliasStatus::MaterializedAndRestored
        );

        let restored_big_factory = session.lazy_table_batch_stream_factory(big);
        let restored_big_rows = collect_stream_row_count(restored_big_factory().await?).await?;
        assert_eq!(restored_big_rows, 2);
        assert_eq!(source_scans.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn write_all_auto_keeps_unrelated_output_on_normal_stream_path()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (shared_provider, shared_scans) = scan_counting_marker_region_provider("shared")?;
        let (unrelated_provider, unrelated_scans) =
            scan_counting_marker_region_provider("unrelated")?;
        session
            .context()
            .register_table("big_source", shared_provider)?;
        session
            .context()
            .register_table("unrelated_source", unrelated_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let unrelated = session
            .table_from_sql("select marker from unrelated_source where region = 'west'")
            .await?;
        let big_output =
            execute_output_request(big, "big_output", "big_orders", LoadMode::AppendExisting)?;
        let west_output =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let unrelated_output = execute_output_request(
            unrelated,
            "unrelated_output",
            "unrelated_orders",
            LoadMode::AppendExisting,
        )?;
        let writer = FakeWorkflowWriter::default();
        let calls = writer.calls();

        let report = session
            .write_all_with_writer(&[big_output, unrelated_output, west_output], writer)
            .await?;
        {
            let calls = calls
                .lock()
                .map_err(|_| "fake workflow call lock poisoned")?;

            assert_eq!(calls.len(), 3);
            assert_eq!(calls[0].output_name, "big_output");
            assert_eq!(calls[0].rows, 2);
            assert_eq!(calls[1].output_name, "unrelated_output");
            assert_eq!(calls[1].rows, 1);
            assert_eq!(calls[2].output_name, "west_output");
            assert_eq!(calls[2].rows, 1);
            assert_eq!(shared_scans.load(Ordering::SeqCst), 1);
            assert_eq!(unrelated_scans.load(Ordering::SeqCst), 1);
            assert!(report.all_succeeded());
        }
        Ok(())
    }

    #[tokio::test]
    async fn write_all_disabled_cache_mode_uses_baseline_path()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let big_output =
            execute_output_request(big, "big_output", "big_orders", LoadMode::AppendExisting)?;
        let west_output =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let writer = FakeWorkflowWriter::default();
        let calls = writer.calls();

        let report = session
            .write_all_with_options_and_writer(
                &[big_output, west_output],
                WriteAllOptions::new().with_cache_mode(WriteAllCacheMode::Disabled),
                writer,
            )
            .await?;
        let calls = calls
            .lock()
            .map_err(|_| "fake workflow call lock poisoned")?;

        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].rows, 2);
        assert_eq!(calls[1].rows, 1);
        assert_eq!(source_scans.load(Ordering::SeqCst), 2);
        assert!(report.all_succeeded());
        assert_eq!(report.cache(), &WriteAllCacheReport::Disabled);
        Ok(())
    }

    #[tokio::test]
    async fn write_all_with_writer_skips_later_outputs_after_writer_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (first_provider, first_scans) = scan_counting_marker_region_provider("first")?;
        let (second_provider, second_scans) = scan_counting_marker_region_provider("second")?;
        let (third_provider, third_scans) = scan_counting_marker_region_provider("third")?;
        session
            .context()
            .register_table("first_source", first_provider)?;
        session
            .context()
            .register_table("second_source", second_provider)?;
        session
            .context()
            .register_table("third_source", third_provider)?;
        let first = session
            .table_from_sql("select marker from first_source where region = 'west'")
            .await?;
        let second = session
            .table_from_sql("select marker from second_source where region = 'west'")
            .await?;
        let third = session
            .table_from_sql("select marker from third_source where region = 'west'")
            .await?;
        let first = execute_output_request(
            first,
            "first_output",
            "first_orders",
            LoadMode::AppendExisting,
        )?;
        let second = execute_output_request(
            second,
            "second_output",
            "second_orders",
            LoadMode::AppendExisting,
        )?;
        let third = execute_output_request(
            third,
            "third_output",
            "third_orders",
            LoadMode::AppendExisting,
        )?;
        let writer = FakeWorkflowWriter::failing_on("second_output");
        let calls = writer.calls();

        let report = session
            .write_all_with_writer(&[first, second, third], writer)
            .await?;
        let calls = calls
            .lock()
            .map_err(|_| "fake workflow call lock poisoned")?;

        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].output_name, "first_output");
        assert_eq!(calls[1].output_name, "second_output");
        assert_eq!(first_scans.load(Ordering::SeqCst), 1);
        assert_eq!(second_scans.load(Ordering::SeqCst), 1);
        assert_eq!(third_scans.load(Ordering::SeqCst), 0);
        assert_eq!(report.succeeded_count(), 1);
        assert_eq!(report.failed_count(), 1);
        assert_eq!(report.skipped_count(), 1);
        assert!(report.outputs()[0].is_succeeded());
        assert_eq!(report.outputs()[0].output_name(), "first_output");
        assert!(report.outputs()[1].is_failed());
        assert_eq!(report.outputs()[1].output_name(), "second_output");
        assert!(report.outputs()[2].is_skipped());
        assert_eq!(report.outputs()[2].output_name(), "third_output");
        Ok(())
    }

    #[tokio::test]
    async fn write_all_with_writer_reports_stream_setup_failure_before_writer()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (first_provider, first_scans) = scan_counting_marker_region_provider("first")?;
        let (failing_provider, failing_scans) = failing_scan_marker_region_provider();
        let (third_provider, third_scans) = scan_counting_marker_region_provider("third")?;
        session
            .context()
            .register_table("first_source", first_provider)?;
        session
            .context()
            .register_table("failing_source", failing_provider)?;
        session
            .context()
            .register_table("third_source", third_provider)?;
        let first = session
            .table_from_sql("select marker from first_source where region = 'west'")
            .await?;
        let failing = session
            .table_from_sql("select marker from failing_source where region = 'west'")
            .await?;
        let third = session
            .table_from_sql("select marker from third_source where region = 'west'")
            .await?;
        let first = execute_output_request(
            first,
            "first_output",
            "first_orders",
            LoadMode::AppendExisting,
        )?;
        let failing = execute_output_request(
            failing,
            "failing_output",
            "failing_orders",
            LoadMode::AppendExisting,
        )?;
        let third = execute_output_request(
            third,
            "third_output",
            "third_orders",
            LoadMode::AppendExisting,
        )?;
        let writer = FakeWorkflowWriter::default();
        let calls = writer.calls();

        let report = session
            .write_all_with_writer(&[first, failing, third], writer)
            .await?;
        let calls = calls
            .lock()
            .map_err(|_| "fake workflow call lock poisoned")?;

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].output_name, "first_output");
        assert_eq!(first_scans.load(Ordering::SeqCst), 1);
        assert_eq!(failing_scans.load(Ordering::SeqCst), 1);
        assert_eq!(third_scans.load(Ordering::SeqCst), 0);
        assert_eq!(report.succeeded_count(), 1);
        assert_eq!(report.failed_count(), 1);
        assert_eq!(report.skipped_count(), 1);
        assert!(report.outputs()[0].is_succeeded());
        assert_eq!(report.outputs()[0].output_name(), "first_output");
        assert!(report.outputs()[1].is_failed());
        assert_eq!(report.outputs()[1].output_name(), "failing_output");
        assert!(report.outputs()[2].is_skipped());
        assert_eq!(report.outputs()[2].output_name(), "third_output");
        Ok(())
    }

    #[tokio::test]
    async fn write_all_rejects_duplicate_output_names_before_stream_setup()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("orders_source", source_provider)?;
        let west = session
            .table_from_sql("select marker from orders_source where region = 'west'")
            .await?;
        let east = session
            .table_from_sql("select marker from orders_source where region = 'east'")
            .await?;
        let west = execute_output_request(
            west,
            "orders_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let east = execute_output_request(
            east,
            "orders_output",
            "east_orders",
            LoadMode::AppendExisting,
        )?;

        let error = session.write_all(&[west, east]).await;

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("write_all output names must be unique")
                    && message.contains("orders_output")
        ));
        assert_eq!(source_scans.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn write_all_validation_errors_redact_connection_material()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let west = session.table_from_sql("select 1 as id").await?;
        let east = session.table_from_sql("select 2 as id").await?;
        let west = execute_output_request(
            west,
            "orders_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let east = execute_output_request(
            east,
            "orders_output",
            "east_orders",
            LoadMode::AppendExisting,
        )?;

        let error = session
            .write_all(&[west, east])
            .await
            .map(|_| ())
            .map_err(|error| format!("{error:?} {error}"));

        assert!(
            matches!(error, Err(display) if display.contains("orders_output")
                && !display.contains("secret-token")
                && !display.contains("password")
                && !display.contains("server=tcp"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn source_reports_for_lazy_table_plan_include_provider_stats_without_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        let reports = session.source_reports_for_lazy_table_plan(&source).await?;

        assert_eq!(reports.len(), 1);
        let report = &reports[0];
        assert_eq!(report.source_name(), "orders");
        assert_eq!(report.provider_stats_reason(), None);
        let stats = report
            .provider_read_stats()
            .ok_or("expected provider read stats")?;
        assert_eq!(stats.source_name, "orders");
        assert_eq!(stats.snapshot_version, report.snapshot_version());
        assert_eq!(stats.files_started, 0);
        assert_eq!(stats.files_completed, 0);
        assert_eq!(stats.batches_produced, 0);
        assert_eq!(stats.rows_produced, 0);
        match stats.scan_metadata_exhausted {
            Some(true) => {
                assert_eq!(
                    report.file_count(),
                    crate::FileCount::exact(stats.files_planned)
                );
                assert_eq!(report.file_count_reason(), None);
                assert!(report.scan_metadata_exhausted());
            }
            Some(false) => {
                assert_eq!(
                    report.file_count(),
                    crate::FileCount::estimated(stats.files_planned)
                );
                assert_eq!(report.file_count_reason(), None);
                assert!(!report.scan_metadata_exhausted());
            }
            None => {
                assert_eq!(report.file_count(), crate::FileCount::unavailable());
                assert_eq!(
                    report.file_count_reason(),
                    Some(crate::ReportReasonCode::CapabilityUnavailable)
                );
                assert!(!report.scan_metadata_exhausted());
            }
        }
        Ok(())
    }
}
