use std::{fmt, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::TableProvider,
    prelude::{DataFrame, SQLOptions},
};

use crate::{
    DeltaFunnelError, DeltaProtocolReport, DeltaSourceConfig, DeltaTableProviderConfig,
    RegisteredDeltaSource, SqlTablePhase, load_delta_source, preflight_delta_protocol,
    register_delta_sources_with_scan_execution_options, table_formats::validate_table_source_names,
};

use super::{
    DeltaFunnelSession, LazyTable, LazyTableKind,
    errors::{sql_table_error, unknown_lazy_table_error},
};

mod lineage;

pub(crate) use lineage::{DerivedTableDependency, DerivedTableLineage};

/// Registered Delta source tracked by a query-load session.
#[derive(Clone, PartialEq, Eq)]
pub struct RegisteredSessionSource {
    table: LazyTable,
    source_uri: String,
    snapshot_version: u64,
    schema: SchemaRef,
    protocol: DeltaProtocolReport,
}

impl RegisteredSessionSource {
    pub(super) fn from_registered(table: LazyTable, registered: RegisteredDeltaSource) -> Self {
        Self {
            table,
            source_uri: registered.table_uri,
            snapshot_version: registered.snapshot_version,
            schema: registered.schema,
            protocol: registered.protocol,
        }
    }

    /// Returns the lazy table handle for this registered source.
    #[must_use]
    pub const fn table(&self) -> &LazyTable {
        &self.table
    }

    /// Returns the DataFusion table name for this source.
    #[must_use]
    pub fn name(&self) -> &str {
        self.table.name()
    }

    /// Returns the sanitized Delta source URI or display summary.
    #[must_use]
    pub fn source_uri(&self) -> &str {
        &self.source_uri
    }

    /// Returns the resolved Delta snapshot version.
    #[must_use]
    pub const fn snapshot_version(&self) -> u64 {
        self.snapshot_version
    }

    /// Returns the logical Arrow schema exposed to DataFusion.
    #[must_use]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns the sanitized protocol report captured before registration.
    #[must_use]
    pub const fn protocol(&self) -> &DeltaProtocolReport {
        &self.protocol
    }
}

impl fmt::Debug for RegisteredSessionSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredSessionSource")
            .field("table", &self.table)
            .field("source_uri", &self.source_uri)
            .field("snapshot_version", &self.snapshot_version)
            .field("schema", &self.schema)
            .field("protocol", &self.protocol)
            .finish()
    }
}

/// Registered SQL-derived table alias tracked by a query-load session.
#[derive(Clone, PartialEq, Eq)]
pub struct RegisteredDerivedTable {
    table: LazyTable,
    schema: SchemaRef,
    pub(super) sql_text: String,
    pub(super) lineage: DerivedTableLineage,
}

impl RegisteredDerivedTable {
    pub(super) fn new(
        table: LazyTable,
        schema: SchemaRef,
        sql_text: String,
        lineage: DerivedTableLineage,
    ) -> Self {
        Self {
            table,
            schema,
            sql_text,
            lineage,
        }
    }

    /// Returns the lazy table handle for this registered derived alias.
    #[must_use]
    pub const fn table(&self) -> &LazyTable {
        &self.table
    }

    /// Returns the DataFusion table name for this derived alias.
    #[must_use]
    pub fn name(&self) -> &str {
        self.table.name()
    }

    /// Returns the logical Arrow schema exposed to DataFusion.
    #[must_use]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns the retained SQL text used to create this derived alias.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn sql_text(&self) -> &str {
        &self.sql_text
    }

    /// Returns dependency lineage captured from the retained SQL text.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) const fn lineage(&self) -> &DerivedTableLineage {
        &self.lineage
    }
}

impl fmt::Debug for RegisteredDerivedTable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredDerivedTable")
            .field("table", &self.table)
            .field("schema", &self.schema)
            .field("sql_text", &"<redacted>")
            .field("lineage", &self.lineage)
            .finish()
    }
}

#[derive(Clone)]
pub(super) struct PendingDerivedTable {
    pub(super) table: LazyTable,
    pub(super) provider: Arc<dyn TableProvider>,
    pub(super) schema: SchemaRef,
    pub(super) sql_text: String,
    pub(super) lineage: DerivedTableLineage,
}

impl DeltaFunnelSession {
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

    /// Builds a lazy SQL-derived table without registering a query alias.
    ///
    /// The SQL must be one read-only tabular query. Planning uses DataFusion to
    /// produce a lazy table provider and does not execute rows, contact SQL
    /// Server, or create an output target.
    ///
    /// # Errors
    ///
    /// Returns [`DeltaFunnelError::SqlTable`] when the SQL is empty, contains
    /// an unsupported or non-read-only statement, or cannot be planned against
    /// the session's registered aliases.
    pub async fn table_from_sql(&mut self, sql: &str) -> Result<LazyTable, DeltaFunnelError> {
        let sql = sql.trim();
        if sql.is_empty() {
            return sql_table_error(SqlTablePhase::ValidateSql, "SQL text must not be empty");
        }

        let dataframe = self.plan_read_only_sql(sql).await?;
        let schema = Arc::new(dataframe.schema().as_arrow().clone());
        let provider = dataframe.into_view();
        let lineage = self.derive_table_lineage_from_sql(sql);
        let table = self.allocate_derived_sql_table();
        self.pending_derived_tables.push(PendingDerivedTable {
            table: table.clone(),
            provider,
            schema,
            sql_text: sql.to_owned(),
            lineage,
        });
        Ok(table)
    }

    /// Registers a session-owned alias for a lazy SQL-derived table.
    ///
    /// Alias names use the same unquoted identifier rules as Delta source
    /// aliases. The alias is registered into the session's DataFusion catalog
    /// only after all local validation succeeds.
    ///
    /// # Errors
    ///
    /// Returns [`DeltaFunnelError::InvalidSourceName`] or
    /// [`DeltaFunnelError::DuplicateSourceName`] for invalid or ambiguous
    /// aliases, and [`DeltaFunnelError::SqlTable`] when the table handle is not
    /// a pending SQL-derived table owned by this session or DataFusion rejects
    /// the alias registration.
    pub fn register_alias(
        &mut self,
        name: impl Into<String>,
        table: &LazyTable,
    ) -> Result<LazyTable, DeltaFunnelError> {
        let name = name.into();
        validate_table_source_names([name.as_str()])?;
        self.reject_registered_alias_name(&name)?;

        let Some(index) = self.find_pending_derived_table(table) else {
            return sql_table_error(
                SqlTablePhase::RegisterDerivedAlias,
                "lazy table is not a pending SQL-derived table owned by this session",
            );
        };
        let pending = self.pending_derived_tables.remove(index);

        if let Err(error) = self
            .context
            .register_table(name.as_str(), Arc::clone(&pending.provider))
        {
            let message = error.to_string();
            self.pending_derived_tables.push(pending);
            return sql_table_error(SqlTablePhase::RegisterDerivedAlias, message);
        }

        let alias_table = pending.table.with_name(name);
        self.derived_tables.push(RegisteredDerivedTable::new(
            alias_table.clone(),
            pending.schema,
            pending.sql_text,
            pending.lineage,
        ));
        Ok(alias_table)
    }

    /// Finds a registered Delta source by alias using unquoted SQL semantics.
    #[must_use]
    pub fn registered_source(&self, name: &str) -> Option<&RegisteredSessionSource> {
        self.sources
            .iter()
            .find(|source| source.name().eq_ignore_ascii_case(name))
    }

    /// Returns registered SQL-derived aliases in registration order.
    #[must_use]
    pub fn derived_tables(&self) -> &[RegisteredDerivedTable] {
        &self.derived_tables
    }

    /// Finds a registered SQL-derived alias by name using unquoted SQL semantics.
    #[must_use]
    pub fn registered_derived_table(&self, name: &str) -> Option<&RegisteredDerivedTable> {
        self.derived_tables
            .iter()
            .find(|table| table.name().eq_ignore_ascii_case(name))
    }

    pub(super) fn registered_derived_table_by_id(
        &self,
        table_id: u64,
    ) -> Option<&RegisteredDerivedTable> {
        self.derived_tables
            .iter()
            .find(|table| table.table().id() == table_id)
    }

    /// Resolves the session metadata for an alias that is eligible for scoped caching.
    ///
    /// The cache primitive only supports registered SQL-derived aliases. Raw
    /// sources, pending derived tables, and foreign or stale table handles are
    /// rejected before any DataFusion catalog mutation can happen.
    pub(super) fn registered_derived_for_scoped_cache_alias(
        &self,
        table: &LazyTable,
    ) -> Result<&RegisteredDerivedTable, DeltaFunnelError> {
        if table.kind() != LazyTableKind::DerivedSql {
            return Err(unknown_lazy_table_error(table));
        }

        self.registered_derived_table_by_id(table.id())
            .ok_or_else(|| unknown_lazy_table_error(table))
    }

    #[allow(dead_code)]
    pub(crate) fn sql_text_for_derived_table(
        &self,
        table: &LazyTable,
    ) -> Result<&str, DeltaFunnelError> {
        if table.kind() != LazyTableKind::DerivedSql {
            return Err(unknown_lazy_table_error(table));
        }

        self.derived_tables
            .iter()
            .find(|derived| derived.table().id() == table.id())
            .map(RegisteredDerivedTable::sql_text)
            .or_else(|| {
                self.pending_derived_tables
                    .iter()
                    .find(|pending| pending.table.id() == table.id())
                    .map(|pending| pending.sql_text.as_str())
            })
            .ok_or_else(|| unknown_lazy_table_error(table))
    }

    pub(super) async fn plan_read_only_sql(
        &self,
        sql: &str,
    ) -> Result<DataFrame, DeltaFunnelError> {
        self.context
            .sql_with_options(sql, read_only_sql_options())
            .await
            .map_err(|error| DeltaFunnelError::SqlTable {
                phase: classify_sql_error_phase(&error.to_string()),
                message: error.to_string(),
            })
    }

    fn allocate_derived_sql_table(&mut self) -> LazyTable {
        let id = self.next_table_id;
        self.next_table_id = self.next_table_id.saturating_add(1);
        LazyTable::derived_sql(id)
    }

    fn allocate_delta_source_table(&mut self, name: String) -> LazyTable {
        let id = self.next_table_id;
        self.next_table_id = self.next_table_id.saturating_add(1);
        LazyTable::delta_source(id, name)
    }

    pub(super) fn reject_registered_alias_name(&self, name: &str) -> Result<(), DeltaFunnelError> {
        if self.registered_source(name).is_some() || self.registered_derived_table(name).is_some() {
            return Err(DeltaFunnelError::DuplicateSourceName {
                name: name.to_owned(),
            });
        }
        Ok(())
    }

    fn find_pending_derived_table(&self, table: &LazyTable) -> Option<usize> {
        if table.kind() != LazyTableKind::DerivedSql {
            return None;
        }

        self.pending_derived_tables
            .iter()
            .position(|pending| pending.table.id() == table.id())
    }
}

pub(super) fn read_only_sql_options() -> SQLOptions {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
}

fn classify_sql_error_phase(error: &str) -> SqlTablePhase {
    if error.contains("DDL not supported")
        || error.contains("DML not supported")
        || error.contains("Statement not supported")
        || error.contains("only supports a single SQL statement")
    {
        SqlTablePhase::ValidateSql
    } else {
        SqlTablePhase::PlanSql
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{
        common::tree_node::{TreeNode, TreeNodeRecursion},
        error::Result as DataFusionResult,
        logical_expr::LogicalPlan,
    };

    use crate::{
        DeltaFunnelError, DeltaProviderReaderBackend, DeltaProviderScanExecutionOptions,
        DeltaSourceConfig, DeltaStorageOptions, QueryOptions, SqlTablePhase,
    };

    use super::super::{
        DeltaFunnelSession, LazyTableKind, SessionOptions, SourceUsageStatus,
        test_support::DeltaLogTable,
    };
    const UNSUPPORTED_PROTOCOL_JSON: &str =
        r#"{"protocol":{"minReaderVersion":99,"minWriterVersion":2}}"#;

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

    #[test]
    fn delta_lake_registers_source_and_returns_lazy_table() -> Result<(), Box<dyn std::error::Error>>
    {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        let lazy = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        assert_eq!(lazy.id(), 0);
        assert_eq!(lazy.kind(), LazyTableKind::DeltaSource);
        assert_eq!(lazy.name(), "orders");
        assert_eq!(session.next_table_id(), 1);
        assert_eq!(session.sources().len(), 1);
        let registered = session
            .registered_source("ORDERS")
            .ok_or("expected registered source")?;
        assert_eq!(registered.table(), &lazy);
        assert_eq!(registered.name(), "orders");
        assert!(registered.source_uri().starts_with("file://"));
        assert_eq!(registered.snapshot_version(), 1);
        assert_eq!(registered.protocol().source_name, "orders");
        assert_eq!(registered.schema().fields().len(), 2);
        let source_reports = session.source_reports();
        assert_eq!(source_reports.len(), 1);
        let report = &source_reports[0];
        assert_eq!(report.source_name(), "orders");
        assert_eq!(report.source_uri(), registered.source_uri());
        assert_eq!(report.snapshot_version(), 1);
        assert_eq!(report.protocol().source_name, "orders");
        assert_eq!(report.scheduling().query_target_partitions(), None);
        assert_eq!(
            report.scheduling().reader_backend(),
            DeltaProviderReaderBackend::NativeAsync
        );
        assert_eq!(report.file_count(), crate::FileCount::unavailable());
        assert_eq!(
            report.file_count_reason(),
            Some(crate::ReportReasonCode::CostAvoidance)
        );
        assert!(!report.scan_metadata_exhausted());
        assert_eq!(report.usage_status(), SourceUsageStatus::Unknown);
        assert!(report.used_by_output_names().is_empty());
        assert!(report.provider_read_stats().is_none());
        assert_eq!(
            report.provider_stats_reason(),
            Some(crate::ReportReasonCode::NotExecuted)
        );

        Ok(())
    }

    #[test]
    fn delta_lake_registers_multiple_distinct_sources() -> Result<(), Box<dyn std::error::Error>> {
        let orders = DeltaLogTable::new("orders")?;
        let customers = DeltaLogTable::new("customers")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        let orders = session.delta_lake(DeltaSourceConfig::new("orders", orders.uri()))?;
        let customers = session.delta_lake(DeltaSourceConfig::new("customers", customers.uri()))?;

        assert_eq!(orders.id(), 0);
        assert_eq!(customers.id(), 1);
        assert_eq!(session.sources().len(), 2);
        assert!(session.registered_source("orders").is_some());
        assert!(session.registered_source("customers").is_some());
        Ok(())
    }

    #[test]
    fn duplicate_source_alias_fails_before_loading_second_source()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        let error = session.delta_lake(DeltaSourceConfig::new("ORDERS", ""));

        assert!(matches!(
            error,
            Err(DeltaFunnelError::DuplicateSourceName { name }) if name == "ORDERS"
        ));
        assert_eq!(session.sources().len(), 1);
        assert_eq!(session.next_table_id(), 1);
        Ok(())
    }

    #[test]
    fn invalid_source_alias_fails_before_registration() -> Result<(), DeltaFunnelError> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        let error = session.delta_lake(DeltaSourceConfig::new("select", ""));

        assert!(matches!(
            error,
            Err(DeltaFunnelError::InvalidSourceName { name, .. }) if name == "select"
        ));
        assert!(session.sources().is_empty());
        assert_eq!(session.next_table_id(), 0);
        Ok(())
    }

    #[test]
    fn protocol_preflight_failure_does_not_register_source()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_protocol("unsupported", UNSUPPORTED_PROTOCOL_JSON)?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        let error = session.delta_lake(DeltaSourceConfig::new("unsupported", table.uri()));

        let display = format!("{}", error.as_ref().err().ok_or("expected error")?);
        assert!(display.contains("unsupported"));
        assert!(display.contains("unsupported Delta minReaderVersion"));
        assert!(matches!(
            error,
            Err(DeltaFunnelError::DeltaProtocolCompatibility { .. })
        ));
        assert!(session.sources().is_empty());
        assert_eq!(session.next_table_id(), 0);
        Ok(())
    }

    #[test]
    fn protocol_preflight_failure_redacts_secret_uri_parts()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_protocol("unsupported", UNSUPPORTED_PROTOCOL_JSON)?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        let error = session
            .delta_lake(DeltaSourceConfig::new(
                "unsupported",
                table.file_uri_with_secret_parts()?,
            ))
            .map(|_| ())
            .map_err(|error| error.to_string());

        assert!(
            matches!(error, Err(display) if display.contains("unsupported")
            && display.contains("unsupported Delta minReaderVersion")
            && !display.contains("super-secret")
            && !display.contains("debug-secret")
            && !display.contains("token"))
        );
        assert!(session.sources().is_empty());
        assert_eq!(session.next_table_id(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn protocol_preflight_failure_does_not_leak_datafusion_alias()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_protocol("unsupported", UNSUPPORTED_PROTOCOL_JSON)?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        let error = session.delta_lake(DeltaSourceConfig::new("unsupported", table.uri()));

        assert!(matches!(
            error,
            Err(DeltaFunnelError::DeltaProtocolCompatibility { .. })
        ));
        assert!(session.context().table("unsupported").await.is_err());
        assert!(session.sources().is_empty());
        assert_eq!(session.next_table_id(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn registered_source_sql_analysis_does_not_read_data_files_for_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let dataframe = session
            .context()
            .sql("select id, customer_name from orders")
            .await?;
        let schema = dataframe.schema();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "customer_name");
        assert_eq!(session.sources().len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn table_from_sql_builds_lazy_derived_table_without_row_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        let derived = session
            .table_from_sql("select id, customer_name from orders")
            .await?;

        assert_eq!(derived.id(), 1);
        assert_eq!(derived.kind(), LazyTableKind::DerivedSql);
        assert_eq!(derived.name(), "table_1");
        assert_eq!(session.next_table_id(), 2);
        assert!(session.derived_tables().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn table_from_sql_retains_trimmed_pending_sql_text()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        let derived = session
            .table_from_sql(" \n\t select id from orders \t ")
            .await?;

        assert_eq!(
            session.sql_text_for_derived_table(&derived)?,
            "select id from orders"
        );
        assert!(session.derived_tables().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn registered_derived_alias_can_be_referenced_by_later_sql()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let derived = session
            .table_from_sql("select id, customer_name from orders")
            .await?;

        let alias = session.register_alias("recent_orders", &derived)?;
        let second = session
            .table_from_sql("select id from recent_orders")
            .await?;

        assert_eq!(alias.id(), derived.id());
        assert_eq!(alias.name(), "recent_orders");
        assert_eq!(second.id(), 2);
        assert_eq!(second.kind(), LazyTableKind::DerivedSql);
        assert_eq!(session.derived_tables().len(), 1);
        let registered = session
            .registered_derived_table("RECENT_ORDERS")
            .ok_or("registered derived alias missing")?;
        assert_eq!(registered.table(), &alias);
        assert_eq!(registered.schema().fields().len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn registered_derived_alias_retains_sql_text() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let derived = session
            .table_from_sql("select id, customer_name from orders")
            .await?;

        let alias = session.register_alias("recent_orders", &derived)?;
        let registered = session
            .registered_derived_table("RECENT_ORDERS")
            .ok_or("registered derived alias missing")?;

        assert_eq!(
            session.sql_text_for_derived_table(&alias)?,
            "select id, customer_name from orders"
        );
        assert_eq!(
            registered.sql_text(),
            "select id, customer_name from orders"
        );
        assert_eq!(
            session.sql_text_for_derived_table(&derived)?,
            "select id, customer_name from orders"
        );
        Ok(())
    }

    #[tokio::test]
    async fn empty_sql_fails_before_lazy_table_allocation() -> Result<(), DeltaFunnelError> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        let error = session.table_from_sql(" \n\t ").await;

        assert!(matches!(
            error,
            Err(DeltaFunnelError::SqlTable {
                phase: SqlTablePhase::ValidateSql,
                message,
            }) if message.contains("must not be empty")
        ));
        assert_eq!(session.next_table_id(), 0);
        assert!(session.derived_tables().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn multiple_sql_statements_fail_before_lazy_table_allocation()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        let error = session
            .table_from_sql("select id from orders; select id from orders")
            .await;

        assert!(matches!(
            error,
            Err(DeltaFunnelError::SqlTable {
                phase: SqlTablePhase::ValidateSql,
                ..
            })
        ));
        assert_eq!(session.next_table_id(), 1);
        assert!(session.derived_tables().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn ddl_sql_fails_before_alias_registration() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        let error = session
            .table_from_sql("create table created_orders as select id from orders")
            .await;

        assert!(matches!(
            error,
            Err(DeltaFunnelError::SqlTable {
                phase: SqlTablePhase::ValidateSql,
                ..
            })
        ));
        assert_eq!(session.next_table_id(), 1);
        assert!(session.derived_tables().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn dml_sql_fails_before_alias_registration() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        let error = session
            .table_from_sql("insert into orders select id, customer_name from orders")
            .await;

        assert!(matches!(
            error,
            Err(DeltaFunnelError::SqlTable {
                phase: SqlTablePhase::ValidateSql,
                ..
            })
        ));
        assert_eq!(session.next_table_id(), 1);
        assert!(session.derived_tables().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn missing_table_sql_fails_with_planning_context() -> Result<(), DeltaFunnelError> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        let error = session
            .table_from_sql("select id from missing_orders")
            .await;

        assert!(matches!(
            error,
            Err(DeltaFunnelError::SqlTable {
                phase: SqlTablePhase::PlanSql,
                message,
            }) if message.contains("missing_orders")
        ));
        assert_eq!(session.next_table_id(), 0);
        assert!(session.derived_tables().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn derived_alias_duplicate_with_source_fails_before_registration()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let derived = session.table_from_sql("select id from orders").await?;

        let error = session.register_alias("ORDERS", &derived);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::DuplicateSourceName { name }) if name == "ORDERS"
        ));
        assert!(session.derived_tables().is_empty());
        assert!(session.context().table("ORDERS").await.is_ok());
        let alias = session.register_alias("recent_orders", &derived)?;
        assert_eq!(alias.name(), "recent_orders");
        assert_eq!(session.derived_tables().len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn invalid_derived_alias_fails_without_consuming_pending_table()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let derived = session.table_from_sql("select id from orders").await?;

        let error = session.register_alias("select", &derived);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::InvalidSourceName { name, .. }) if name == "select"
        ));
        assert!(session.derived_tables().is_empty());
        let alias = session.register_alias("recent_orders", &derived)?;
        assert_eq!(alias.name(), "recent_orders");
        assert_eq!(session.derived_tables().len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn invalid_derived_alias_preserves_pending_sql_text()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let derived = session.table_from_sql("select id from orders").await?;

        let error = session.register_alias("select", &derived);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::InvalidSourceName { name, .. }) if name == "select"
        ));
        assert_eq!(
            session.sql_text_for_derived_table(&derived)?,
            "select id from orders"
        );
        let alias = session.register_alias("recent_orders", &derived)?;
        assert_eq!(
            session.sql_text_for_derived_table(&alias)?,
            "select id from orders"
        );
        Ok(())
    }

    #[tokio::test]
    async fn register_alias_rejects_non_pending_table_handle()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        let error = session.register_alias("recent_orders", &source);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::SqlTable {
                phase: SqlTablePhase::RegisterDerivedAlias,
                message,
            }) if message.contains("pending SQL-derived table")
        ));
        assert!(session.derived_tables().is_empty());
        assert!(session.context().table("recent_orders").await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn derived_alias_duplicate_with_derived_alias_fails_before_registration()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let first = session.table_from_sql("select id from orders").await?;
        session.register_alias("recent_orders", &first)?;
        let second = session.table_from_sql("select id from orders").await?;

        let error = session.register_alias("RECENT_ORDERS", &second);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::DuplicateSourceName { name }) if name == "RECENT_ORDERS"
        ));
        assert_eq!(session.derived_tables().len(), 1);
        assert!(session.context().table("RECENT_ORDERS").await.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn registered_derived_debug_redacts_retained_sql_text()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let derived = session
            .table_from_sql("select 'super-secret-literal' as marker")
            .await?;
        session.register_alias("secret_marker", &derived)?;
        let registered = session
            .registered_derived_table("secret_marker")
            .ok_or("registered derived alias missing")?;

        let debug = format!("{registered:?}");

        assert!(debug.contains("sql_text"));
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("super-secret-literal"));
        assert!(!debug.contains("select '"));
        Ok(())
    }

    #[tokio::test]
    async fn source_alias_duplicate_with_derived_alias_fails_before_loading_source()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let derived = session.table_from_sql("select id from orders").await?;
        session.register_alias("recent_orders", &derived)?;

        let error = session.delta_lake(DeltaSourceConfig::new("RECENT_ORDERS", ""));

        assert!(matches!(
            error,
            Err(DeltaFunnelError::DuplicateSourceName { name }) if name == "RECENT_ORDERS"
        ));
        assert_eq!(session.sources().len(), 1);
        assert_eq!(session.derived_tables().len(), 1);
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

    #[test]
    fn source_debug_does_not_expose_storage_option_values() -> Result<(), Box<dyn std::error::Error>>
    {
        let table = DeltaLogTable::new("storage-options")?;
        let mut storage_options = DeltaStorageOptions::new();
        storage_options.insert(
            "AWS_SECRET_ACCESS_KEY".to_owned(),
            "super-secret".to_owned(),
        );
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        session.delta_lake(
            DeltaSourceConfig::new("orders", table.uri()).with_storage_options(storage_options),
        )?;

        let debug = format!("{session:?}");
        assert!(debug.contains("orders"));
        assert!(!debug.contains("super-secret"));
        assert!(!debug.contains("AWS_SECRET_ACCESS_KEY"));
        let report_debug = format!("{:?}", session.source_reports());
        assert!(report_debug.contains("orders"));
        assert!(!report_debug.contains("super-secret"));
        assert!(!report_debug.contains("AWS_SECRET_ACCESS_KEY"));
        Ok(())
    }

    #[test]
    fn source_registration_honors_configured_provider_options()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("configured-provider")?;
        let provider_scan_options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::OfficialKernel,
            2,
            1,
        )?
        .with_output_buffer_capacity_per_partition(3)?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new()
                .with_query_options(QueryOptions {
                    target_partitions: Some(4),
                    output_batch_size: None,
                })
                .with_provider_scan_options(provider_scan_options),
        )?;

        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        assert_eq!(session.sources().len(), 1);
        assert!(session.registered_source("orders").is_some());
        let reports = session.source_reports();
        assert_eq!(reports.len(), 1);
        let scheduling = reports[0].scheduling();
        assert_eq!(scheduling.query_target_partitions(), Some(4));
        assert_eq!(
            scheduling.reader_backend(),
            DeltaProviderReaderBackend::OfficialKernel
        );
        assert_eq!(scheduling.max_concurrent_file_reads_per_scan(), 2);
        assert_eq!(scheduling.max_concurrent_file_reads_per_partition(), 1);
        assert_eq!(scheduling.output_buffer_capacity_per_partition(), 3);
        assert_eq!(
            scheduling.native_async_prefetch_file_count_per_partition(),
            0
        );
        Ok(())
    }
}
