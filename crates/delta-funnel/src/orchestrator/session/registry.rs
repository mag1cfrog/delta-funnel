use std::{collections::BTreeSet, fmt, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    common::TableReference,
    datasource::TableProvider,
    prelude::{DataFrame, SQLOptions},
    sql::{parser::DFParser, resolve::resolve_table_references},
};

use crate::{
    DeltaFunnelError, DeltaProtocolReport, RegisteredDeltaSource, SqlTablePhase,
    support::sanitize_text_for_display, table_formats::validate_table_source_names,
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
