use std::{fmt, sync::Arc};

use datafusion::{arrow::datatypes::SchemaRef, datasource::TableProvider};

use crate::{DeltaProtocolReport, RegisteredDeltaSource};

use super::LazyTable;

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
