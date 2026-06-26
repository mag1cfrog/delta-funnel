use std::collections::BTreeSet;

use datafusion::{
    common::TableReference,
    sql::{parser::DFParser, resolve::resolve_table_references},
};

use crate::{DeltaFunnelError, SqlTablePhase, support::sanitize_text_for_display};

use super::super::{
    DeltaFunnelSession, LazyTable, LazyTableKind,
    errors::{sql_table_error, unknown_lazy_table_error},
};
use super::{RegisteredDerivedTable, RegisteredSessionSource};

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
    pub(crate) fn complete(
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

    pub(crate) fn incomplete(message: impl Into<String>) -> Self {
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

impl DeltaFunnelSession {
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

    pub(crate) fn known_source_dependencies_for_table(
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
    use datafusion::sql::{parser::DFParser, resolve::resolve_table_references};

    use crate::DeltaSourceConfig;

    use super::super::super::{
        DeltaFunnelSession, SessionOptions,
        test_support::{DeltaLogTable, marker_region_provider},
    };
    use super::DerivedTableDependency;

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
}
