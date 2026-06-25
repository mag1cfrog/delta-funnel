use std::{collections::BTreeSet, fmt, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    common::TableReference,
    datasource::TableProvider,
    prelude::{DataFrame, SQLOptions},
    sql::{parser::DFParser, resolve::resolve_table_references},
};

use crate::{
    DeltaFunnelError, DeltaProtocolReport, DeltaSourceConfig, DeltaTableProviderConfig,
    RegisteredDeltaSource, SqlTablePhase, load_delta_source, preflight_delta_protocol,
    register_delta_sources_with_scan_execution_options, support::sanitize_text_for_display,
    table_formats::validate_table_source_names,
};

use super::{
    DeltaFunnelSession, LazyTable, LazyTableKind,
    errors::{sql_table_error, unknown_lazy_table_error},
};

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

/// Direct dependency captured for one SQL-derived table.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum DerivedTableDependency {
    /// Reference to a registered Delta source alias.
    RegisteredSource { table_id: u64, name: String },
    /// Reference to a registered SQL-derived alias.
    RegisteredDerived { table_id: u64, name: String },
}

impl DerivedTableDependency {
    pub(super) fn registered_source(source: &RegisteredSessionSource) -> Self {
        Self::RegisteredSource {
            table_id: source.table().id(),
            name: source.name().to_owned(),
        }
    }

    pub(super) fn registered_derived(derived: &RegisteredDerivedTable) -> Self {
        Self::RegisteredDerived {
            table_id: derived.table().id(),
            name: derived.name().to_owned(),
        }
    }
}

/// Dependency lineage captured for one SQL-derived table.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct DerivedTableLineage {
    /// Session-owned source or derived aliases that this SQL-derived table reads
    /// directly.
    direct_dependencies: Vec<DerivedTableDependency>,
    /// Names declared inside this SQL statement, such as CTE names.
    ///
    /// These names are query-local and can shadow session aliases, so they are
    /// tracked separately to avoid treating them as session-owned dependencies.
    local_references: Vec<String>,
    /// Table references that DataFusion found in the SQL but that do not map to
    /// session-owned metadata or query-local names.
    unknown_references: Vec<String>,
    /// Reason lineage extraction could not complete while preserving
    /// table_from_sql behavior.
    incomplete_reason: Option<String>,
}

impl DerivedTableLineage {
    pub(super) fn complete(
        direct_dependencies: Vec<DerivedTableDependency>,
        local_references: Vec<String>,
        unknown_references: Vec<String>,
    ) -> Self {
        Self {
            direct_dependencies,
            local_references,
            unknown_references,
            incomplete_reason: None,
        }
    }

    pub(super) fn incomplete(message: impl Into<String>) -> Self {
        Self {
            incomplete_reason: Some(message.into()),
            ..Self::default()
        }
    }

    /// Returns direct session-owned source or derived dependencies.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn direct_dependencies(&self) -> &[DerivedTableDependency] {
        &self.direct_dependencies
    }

    /// Returns local query-scope references, such as CTE names.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn local_references(&self) -> &[String] {
        &self.local_references
    }

    /// Returns references that did not map to session-owned metadata.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn unknown_references(&self) -> &[String] {
        &self.unknown_references
    }

    /// Returns whether lineage capture completed without an extractor error.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) const fn is_complete(&self) -> bool {
        self.incomplete_reason.is_none()
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

    #[allow(dead_code)]
    pub(crate) fn lineage_for_derived_table(
        &self,
        table: &LazyTable,
    ) -> Result<&DerivedTableLineage, DeltaFunnelError> {
        if table.kind() != LazyTableKind::DerivedSql {
            return Err(unknown_lazy_table_error(table));
        }

        self.derived_tables
            .iter()
            .find(|derived| derived.table().id() == table.id())
            .map(RegisteredDerivedTable::lineage)
            .or_else(|| {
                self.pending_derived_tables
                    .iter()
                    .find(|pending| pending.table.id() == table.id())
                    .map(|pending| &pending.lineage)
            })
            .ok_or_else(|| unknown_lazy_table_error(table))
    }

    #[allow(dead_code)]
    pub(crate) fn transitive_registered_derived_dependencies(
        &self,
        table: &LazyTable,
    ) -> Result<Vec<DerivedTableDependency>, DeltaFunnelError> {
        let lineage = self.lineage_for_derived_table(table)?;
        let mut visited_table_ids = BTreeSet::new();
        let mut dependencies = BTreeSet::new();

        self.collect_transitive_registered_derived_dependencies(
            lineage,
            &mut visited_table_ids,
            &mut dependencies,
        )?;

        Ok(dependencies.into_iter().collect())
    }

    pub(super) fn known_source_dependencies_for_table(
        &self,
        table: &LazyTable,
    ) -> Result<Option<BTreeSet<u64>>, DeltaFunnelError> {
        self.schema_for_lazy_table(table)?;

        match table.kind() {
            LazyTableKind::DeltaSource => Ok(Some(BTreeSet::from([table.id()]))),
            LazyTableKind::DerivedSql => {
                let lineage = self.lineage_for_derived_table(table)?;
                if !lineage.is_complete() {
                    return Ok(None);
                }
                let mut visited_derived_table_ids = BTreeSet::new();
                let mut source_table_ids = BTreeSet::new();
                let usage_known = self.collect_transitive_source_dependencies(
                    lineage,
                    &mut visited_derived_table_ids,
                    &mut source_table_ids,
                )?;
                Ok(usage_known.then_some(source_table_ids))
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn lazy_table_depends_on_registered_derived(
        &self,
        table: &LazyTable,
        candidate: &LazyTable,
    ) -> Result<bool, DeltaFunnelError> {
        self.schema_for_lazy_table(table)?;
        if candidate.kind() != LazyTableKind::DerivedSql {
            return Err(unknown_lazy_table_error(candidate));
        }

        self.registered_derived_table_by_id(candidate.id())
            .ok_or_else(|| unknown_lazy_table_error(candidate))?;
        if table.id() == candidate.id() {
            return Ok(true);
        }
        if table.kind() == LazyTableKind::DeltaSource {
            return Ok(false);
        }

        Ok(self
            .transitive_registered_derived_dependencies(table)?
            .iter()
            .any(|dependency| {
                matches!(
                    dependency,
                    DerivedTableDependency::RegisteredDerived { table_id, .. }
                        if *table_id == candidate.id()
                )
            }))
    }

    fn collect_transitive_registered_derived_dependencies(
        &self,
        lineage: &DerivedTableLineage,
        visited_table_ids: &mut BTreeSet<u64>,
        dependencies: &mut BTreeSet<DerivedTableDependency>,
    ) -> Result<(), DeltaFunnelError> {
        for dependency in lineage.direct_dependencies() {
            let DerivedTableDependency::RegisteredDerived { table_id, name } = dependency else {
                continue;
            };
            if !visited_table_ids.insert(*table_id) {
                continue;
            }

            dependencies.insert(dependency.clone());
            let derived = self.registered_derived_table_by_id(*table_id).ok_or_else(|| {
                DeltaFunnelError::MssqlWorkflowPlanning {
                    message: format!(
                        "registered derived lineage dependency `{}` is not registered in this session",
                        sanitize_text_for_display(name)
                    ),
                }
            })?;
            self.collect_transitive_registered_derived_dependencies(
                derived.lineage(),
                visited_table_ids,
                dependencies,
            )?;
        }

        Ok(())
    }

    fn collect_transitive_source_dependencies(
        &self,
        lineage: &DerivedTableLineage,
        visited_derived_table_ids: &mut BTreeSet<u64>,
        source_table_ids: &mut BTreeSet<u64>,
    ) -> Result<bool, DeltaFunnelError> {
        let mut usage_known = true;

        for dependency in lineage.direct_dependencies() {
            match dependency {
                DerivedTableDependency::RegisteredSource { table_id, .. } => {
                    source_table_ids.insert(*table_id);
                }
                DerivedTableDependency::RegisteredDerived { table_id, name } => {
                    if !visited_derived_table_ids.insert(*table_id) {
                        continue;
                    }
                    let derived = self.registered_derived_table_by_id(*table_id).ok_or_else(|| {
                        DeltaFunnelError::MssqlWorkflowPlanning {
                            message: format!(
                                "registered derived lineage dependency `{}` is not registered in this session",
                                sanitize_text_for_display(name)
                            ),
                        }
                    })?;
                    if !derived.lineage().is_complete() {
                        usage_known = false;
                        continue;
                    }
                    usage_known &= self.collect_transitive_source_dependencies(
                        derived.lineage(),
                        visited_derived_table_ids,
                        source_table_ids,
                    )?;
                }
            }
        }

        Ok(usage_known)
    }

    pub(super) fn derive_table_lineage_from_sql(&self, sql: &str) -> DerivedTableLineage {
        match self.extract_table_lineage_from_sql(sql) {
            Ok(lineage) => lineage,
            // Lineage is advisory metadata for later cache planning. Keep the
            // existing table_from_sql behavior intact if extraction fails.
            Err(error) => DerivedTableLineage::incomplete(error.to_string()),
        }
    }

    fn extract_table_lineage_from_sql(
        &self,
        sql: &str,
    ) -> Result<DerivedTableLineage, DeltaFunnelError> {
        // Reuse DataFusion's SQL parser and table-reference resolver so lineage
        // follows the same SQL dialect and CTE scoping rules as planning.
        let mut statements =
            DFParser::parse_sql(sql).map_err(|error| DeltaFunnelError::SqlTable {
                phase: SqlTablePhase::ValidateSql,
                message: error.to_string(),
            })?;
        if statements.len() != 1 {
            return sql_table_error(
                SqlTablePhase::ValidateSql,
                "expected exactly one SQL statement for lineage extraction",
            );
        }
        let statement = statements
            .pop_front()
            .ok_or_else(|| DeltaFunnelError::SqlTable {
                phase: SqlTablePhase::ValidateSql,
                message: "expected parsed SQL statement for lineage extraction".to_owned(),
            })?;
        let (relations, ctes) = resolve_table_references(&statement, true).map_err(|error| {
            DeltaFunnelError::SqlTable {
                phase: SqlTablePhase::ValidateSql,
                message: error.to_string(),
            }
        })?;

        Ok(self.classify_lineage_references(relations, ctes))
    }

    fn classify_lineage_references(
        &self,
        relations: Vec<TableReference>,
        ctes: Vec<TableReference>,
    ) -> DerivedTableLineage {
        // CTE names are local to this SQL statement. They can shadow session
        // aliases, so classify them before checking registered session tables.
        let local_references = sorted_reference_strings(ctes.into_iter());
        let local_reference_names = local_references
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let mut dependencies = BTreeSet::new();
        let mut unknown_references = BTreeSet::new();

        for relation in relations {
            // Session aliases are currently registered as bare names. Qualified
            // references might be external catalog/schema names, so keep them
            // visible as unknown instead of guessing.
            let Some(name) = bare_table_reference_name(&relation) else {
                unknown_references.insert(relation.to_string());
                continue;
            };
            if local_reference_names.contains(name) {
                continue;
            }
            // Alias registration rejects source/derived name collisions, so a
            // bare name can map to at most one session-owned object.
            if let Some(derived) = self.registered_derived_table(name) {
                dependencies.insert(DerivedTableDependency::registered_derived(derived));
            } else if let Some(source) = self.registered_source(name) {
                dependencies.insert(DerivedTableDependency::registered_source(source));
            } else {
                unknown_references.insert(relation.to_string());
            }
        }

        DerivedTableLineage::complete(
            dependencies.into_iter().collect(),
            local_references,
            unknown_references.into_iter().collect(),
        )
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

fn sorted_reference_strings(references: impl Iterator<Item = TableReference>) -> Vec<String> {
    // Stable ordering and de-duplication make lineage deterministic for tests,
    // debug output, and later cache candidate comparisons.
    references
        .map(|reference| reference.to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn bare_table_reference_name(reference: &TableReference) -> Option<&str> {
    match reference {
        TableReference::Bare { table } => Some(table.as_ref()),
        // Do not collapse catalog/schema-qualified names into a bare alias.
        // That would make external tables look session-owned.
        TableReference::Partial { .. } | TableReference::Full { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{
        common::tree_node::{TreeNode, TreeNodeRecursion},
        error::Result as DataFusionResult,
        logical_expr::LogicalPlan,
        sql::{parser::DFParser, resolve::resolve_table_references},
    };

    use crate::{
        DeltaFunnelError, DeltaProviderReaderBackend, DeltaProviderScanExecutionOptions,
        DeltaSourceConfig, DeltaStorageOptions, QueryOptions, SqlTablePhase,
    };

    use super::super::{
        DeltaFunnelSession, LazyTableKind, SessionOptions, SourceUsageStatus,
        test_support::{DeltaLogTable, marker_region_provider},
    };
    use super::DerivedTableDependency;

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
    async fn derived_lineage_records_raw_source_dependency()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let orders = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        let big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let lineage = session.lineage_for_derived_table(&big)?;

        assert!(lineage.is_complete());
        assert_eq!(
            lineage.direct_dependencies(),
            &[DerivedTableDependency::RegisteredSource {
                table_id: orders.id(),
                name: "orders".to_owned(),
            }]
        );
        assert!(lineage.unknown_references().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn derived_lineage_records_registered_derived_dependency()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;

        let west = session
            .table_from_sql("select * from BIG b where customer_name = 'alice'")
            .await?;
        let lineage = session.lineage_for_derived_table(&west)?;

        assert!(lineage.is_complete());
        assert_eq!(
            lineage.direct_dependencies(),
            &[DerivedTableDependency::RegisteredDerived {
                table_id: big.id(),
                name: "big".to_owned(),
            }]
        );
        assert!(lineage.unknown_references().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn derived_lineage_deduplicates_repeated_dependencies()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;

        let repeated = session
            .table_from_sql("select * from big where id in (select id from big)")
            .await?;
        let lineage = session.lineage_for_derived_table(&repeated)?;

        assert_eq!(
            lineage.direct_dependencies(),
            &[DerivedTableDependency::RegisteredDerived {
                table_id: big.id(),
                name: "big".to_owned(),
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn derived_lineage_records_dependency_inside_from_subquery()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;

        let derived = session
            .table_from_sql("select id from (select id from big) nested")
            .await?;
        let lineage = session.lineage_for_derived_table(&derived)?;

        assert_eq!(
            lineage.direct_dependencies(),
            &[DerivedTableDependency::RegisteredDerived {
                table_id: big.id(),
                name: "big".to_owned(),
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn derived_lineage_finds_transitive_registered_derived_dependencies()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let pending_regional = session
            .table_from_sql("select id, customer_name from big")
            .await?;
        let regional = session.register_alias("regional", &pending_regional)?;

        let west = session
            .table_from_sql("select id from regional where customer_name = 'alice'")
            .await?;
        let dependencies = session.transitive_registered_derived_dependencies(&west)?;

        assert_eq!(
            dependencies,
            vec![
                DerivedTableDependency::RegisteredDerived {
                    table_id: big.id(),
                    name: "big".to_owned(),
                },
                DerivedTableDependency::RegisteredDerived {
                    table_id: regional.id(),
                    name: "regional".to_owned(),
                },
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn derived_lineage_matches_shared_transitive_dependency_for_multiple_outputs()
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
        let expected = vec![DerivedTableDependency::RegisteredDerived {
            table_id: big.id(),
            name: "big".to_owned(),
        }];

        assert_eq!(
            session.transitive_registered_derived_dependencies(&west)?,
            expected
        );
        assert_eq!(
            session.transitive_registered_derived_dependencies(&east)?,
            expected
        );
        Ok(())
    }

    #[tokio::test]
    async fn derived_lineage_checks_registered_derived_candidate_dependency()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let orders = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let pending_regional = session
            .table_from_sql("select id, customer_name from big")
            .await?;
        let regional = session.register_alias("regional", &pending_regional)?;
        let west = session
            .table_from_sql("select id from regional where customer_name = 'alice'")
            .await?;
        let unrelated = session
            .table_from_sql("select customer_name from orders")
            .await?;

        assert!(session.lazy_table_depends_on_registered_derived(&west, &big)?);
        assert!(session.lazy_table_depends_on_registered_derived(&west, &regional)?);
        assert!(session.lazy_table_depends_on_registered_derived(&big, &big)?);
        assert!(!session.lazy_table_depends_on_registered_derived(&unrelated, &big)?);
        assert!(!session.lazy_table_depends_on_registered_derived(&orders, &big)?);
        Ok(())
    }

    #[tokio::test]
    async fn derived_lineage_treats_cte_shadowing_as_local_reference()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let orders = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let _big = session.register_alias("big", &pending_big)?;

        let shadowed = session
            .table_from_sql("with big as (select id from orders) select id from big")
            .await?;
        let lineage = session.lineage_for_derived_table(&shadowed)?;

        assert_eq!(lineage.local_references(), &["big".to_owned()]);
        assert_eq!(
            lineage.direct_dependencies(),
            &[DerivedTableDependency::RegisteredSource {
                table_id: orders.id(),
                name: "orders".to_owned(),
            }]
        );
        assert!(
            !lineage
                .direct_dependencies()
                .iter()
                .any(|dependency| matches!(
                    dependency,
                    DerivedTableDependency::RegisteredDerived { name, .. } if name == "big"
                ))
        );
        Ok(())
    }

    #[tokio::test]
    async fn derived_lineage_records_unknown_external_references()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session
            .context()
            .register_table("external_orders", marker_region_provider("external")?)?;

        let derived = session
            .table_from_sql("select marker from external_orders")
            .await?;
        let lineage = session.lineage_for_derived_table(&derived)?;

        assert!(lineage.is_complete());
        assert!(lineage.direct_dependencies().is_empty());
        assert_eq!(
            lineage.unknown_references(),
            &["external_orders".to_owned()]
        );
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
