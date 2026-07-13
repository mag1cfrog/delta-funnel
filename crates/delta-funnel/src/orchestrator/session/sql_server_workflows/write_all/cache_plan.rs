use std::fmt;

use crate::support::sanitize_text_for_display;

use super::super::super::{
    DeltaFunnelSession, LazyTable, LazyTableKind, OutputWritePlan, RegisteredDerivedTable,
    registry::DerivedTableDependency,
};

/// Planner output for one `write_all` cache-selection pass.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct MssqlOutputCachePlan {
    decision: MssqlOutputCacheDecision,
    skipped_candidates: Vec<MssqlCacheCandidateSkip>,
}

impl MssqlOutputCachePlan {
    pub(super) fn new(
        decision: MssqlOutputCacheDecision,
        skipped_candidates: Vec<MssqlCacheCandidateSkip>,
    ) -> Self {
        Self {
            decision,
            skipped_candidates,
        }
    }

    pub(super) fn no_cache(reason: MssqlNoCacheReason) -> Self {
        Self {
            decision: MssqlOutputCacheDecision::NoCache { reason },
            skipped_candidates: Vec::new(),
        }
    }

    /// Returns the cache choice for this planning pass.
    #[must_use]
    pub(crate) const fn decision(&self) -> &MssqlOutputCacheDecision {
        &self.decision
    }

    /// Returns candidates skipped for explicit conservative reasons.
    #[must_use]
    pub(crate) fn skipped_candidates(&self) -> &[MssqlCacheCandidateSkip] {
        &self.skipped_candidates
    }
}

impl fmt::Debug for MssqlOutputCachePlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MssqlOutputCachePlan")
            .field("decision", &self.decision)
            .field("skipped_candidates", &self.skipped_candidates)
            .finish()
    }
}

/// Cache decision for one `write_all` cache-selection pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MssqlOutputCacheDecision {
    /// No safe shared cache candidate was selected.
    NoCache { reason: MssqlNoCacheReason },
    /// Registered derived aliases that should be cached for selected outputs.
    ///
    /// This vector represents the cache frontier: eligible shared derived
    /// aliases that are not covered by any deeper eligible shared alias.
    CacheAliases(Vec<MssqlDerivedCacheAliasPlan>),
}

/// Conservative reason no cache alias was selected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MssqlNoCacheReason {
    /// Cache selection only helps when at least two outputs use a candidate.
    FewerThanTwoOutputs,
    /// No registered derived alias is shared by at least two selected outputs.
    NoSharedRegisteredDerivedAlias,
    /// Candidate relationships could not produce a deterministic cache frontier.
    AmbiguousSharedDerivedAlias,
}

/// Selected registered derived alias cache candidate.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct MssqlDerivedCacheAliasPlan {
    table_id: u64,
    alias: String,
    output_indexes: Vec<usize>,
}

impl MssqlDerivedCacheAliasPlan {
    #[cfg(test)]
    pub(crate) fn new(table_id: u64, alias: String, output_indexes: Vec<usize>) -> Self {
        Self {
            table_id,
            alias,
            output_indexes,
        }
    }

    pub(super) fn from_registered(
        derived: &RegisteredDerivedTable,
        output_indexes: Vec<usize>,
    ) -> Self {
        Self {
            table_id: derived.table().id(),
            alias: derived.name().to_owned(),
            output_indexes,
        }
    }

    /// Returns the selected registered derived table id.
    #[must_use]
    pub(crate) const fn table_id(&self) -> u64 {
        self.table_id
    }

    /// Returns the selected registered derived alias.
    #[must_use]
    pub(crate) fn alias(&self) -> &str {
        &self.alias
    }

    /// Returns selected output indexes that use this alias.
    #[must_use]
    pub(crate) fn output_indexes(&self) -> &[usize] {
        &self.output_indexes
    }
}

impl fmt::Debug for MssqlDerivedCacheAliasPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MssqlDerivedCacheAliasPlan")
            .field("table_id", &self.table_id)
            .field("alias", &sanitize_text_for_display(&self.alias))
            .field("output_indexes", &self.output_indexes)
            .finish()
    }
}

/// Stream construction route for one output while cache aliases are active.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MssqlCachedOutputStreamRoute {
    /// The selected output table is itself an active cached alias.
    DirectCachedAlias(MssqlDerivedCacheAliasPlan),
    /// The selected output depends on one or more active cached aliases.
    ReplannedCachedDependency(Vec<MssqlDerivedCacheAliasPlan>),
    /// The selected output does not use any active cached alias.
    UncachedLazyTable,
}

/// Candidate skipped during conservative cache selection.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct MssqlCacheCandidateSkip {
    table_id: u64,
    alias: String,
    reason: MssqlCacheCandidateSkipReason,
}

impl MssqlCacheCandidateSkip {
    pub(super) fn from_registered(
        derived: &RegisteredDerivedTable,
        reason: MssqlCacheCandidateSkipReason,
    ) -> Self {
        Self {
            table_id: derived.table().id(),
            alias: derived.name().to_owned(),
            reason,
        }
    }

    /// Returns the skipped registered derived table id.
    #[must_use]
    pub(crate) const fn table_id(&self) -> u64 {
        self.table_id
    }

    /// Returns the skipped registered derived alias.
    #[must_use]
    pub(crate) fn alias(&self) -> &str {
        &self.alias
    }

    /// Returns why the candidate was skipped.
    #[must_use]
    pub(crate) const fn reason(&self) -> &MssqlCacheCandidateSkipReason {
        &self.reason
    }
}

impl fmt::Debug for MssqlCacheCandidateSkip {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MssqlCacheCandidateSkip")
            .field("table_id", &self.table_id)
            .field("alias", &sanitize_text_for_display(&self.alias))
            .field("reason", &self.reason)
            .finish()
    }
}

/// Reason a candidate was not eligible for cache selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MssqlCacheCandidateSkipReason {
    /// Fewer than two selected outputs use this candidate.
    NotShared { output_count: usize },
    /// Retained SQL text was missing, so later replanning would be unsafe.
    MissingSqlText,
    /// Lineage was incomplete or could not be trusted.
    IncompleteLineage,
    /// A deeper shared alias is closer to all dependent outputs.
    CoveredByDeeperSharedAlias { selected_table_id: u64 },
    /// The candidate's relative depth could not be ordered deterministically.
    AmbiguousDepth,
}

// Result of selecting the cache frontier from already eligible shared
// candidates.
pub(super) enum MssqlCacheFrontierSelection {
    Selected {
        // Candidates that remain on the frontier and should be cached.
        selected_aliases: Vec<MssqlDerivedCacheAliasPlan>,
        // Broader upstream candidates removed because one selected alias is
        // deeper and can cover the same upstream work more precisely.
        covered_aliases: Vec<MssqlCoveredCacheAlias>,
    },
    Ambiguous {
        // Candidates whose ordering could not produce a deterministic frontier.
        ambiguous_aliases: Vec<MssqlDerivedCacheAliasPlan>,
    },
}

pub(super) struct MssqlCoveredCacheAlias {
    // The skipped upstream alias.
    pub(super) alias: MssqlDerivedCacheAliasPlan,
    // The selected downstream alias that covered it.
    pub(super) selected_table_id: u64,
}

impl DeltaFunnelSession {
    pub(crate) fn plan_mssql_output_cache(
        &self,
        requests: &[OutputWritePlan],
    ) -> MssqlOutputCachePlan {
        if requests.len() < 2 {
            return MssqlOutputCachePlan::no_cache(MssqlNoCacheReason::FewerThanTwoOutputs);
        }

        let mut shared_candidates = Vec::new();
        let mut skipped_candidates = Vec::new();
        for derived in &self.derived_tables {
            if derived.sql_text().trim().is_empty() {
                skipped_candidates.push(MssqlCacheCandidateSkip::from_registered(
                    derived,
                    MssqlCacheCandidateSkipReason::MissingSqlText,
                ));
                continue;
            }
            if !derived.lineage().is_complete() {
                skipped_candidates.push(MssqlCacheCandidateSkip::from_registered(
                    derived,
                    MssqlCacheCandidateSkipReason::IncompleteLineage,
                ));
                continue;
            }

            let output_indexes = requests
                .iter()
                .enumerate()
                .filter_map(|(index, request)| {
                    self.cache_output_uses_registered_derived(request.table(), derived)
                        .then_some(index)
                })
                .collect::<Vec<_>>();
            if output_indexes.len() >= 2 {
                shared_candidates.push(MssqlDerivedCacheAliasPlan::from_registered(
                    derived,
                    output_indexes,
                ));
            } else {
                skipped_candidates.push(MssqlCacheCandidateSkip::from_registered(
                    derived,
                    MssqlCacheCandidateSkipReason::NotShared {
                        output_count: output_indexes.len(),
                    },
                ));
            }
        }

        if shared_candidates.len() == 1 {
            return MssqlOutputCachePlan::new(
                MssqlOutputCacheDecision::CacheAliases(vec![shared_candidates.remove(0)]),
                skipped_candidates,
            );
        }
        if shared_candidates.len() > 1 {
            match self.select_shared_cache_frontier(shared_candidates) {
                MssqlCacheFrontierSelection::Selected {
                    selected_aliases,
                    covered_aliases,
                } => {
                    skipped_candidates.extend(covered_aliases.into_iter().filter_map(|covered| {
                        self.registered_derived_table_by_id(covered.alias.table_id())
                            .map(|derived| {
                                MssqlCacheCandidateSkip::from_registered(
                                    derived,
                                    MssqlCacheCandidateSkipReason::CoveredByDeeperSharedAlias {
                                        selected_table_id: covered.selected_table_id,
                                    },
                                )
                            })
                    }));
                    return MssqlOutputCachePlan::new(
                        MssqlOutputCacheDecision::CacheAliases(selected_aliases),
                        skipped_candidates,
                    );
                }
                MssqlCacheFrontierSelection::Ambiguous { ambiguous_aliases } => {
                    skipped_candidates.extend(ambiguous_aliases.into_iter().filter_map(
                        |candidate| {
                            self.registered_derived_table_by_id(candidate.table_id())
                                .map(|derived| {
                                    MssqlCacheCandidateSkip::from_registered(
                                        derived,
                                        MssqlCacheCandidateSkipReason::AmbiguousDepth,
                                    )
                                })
                        },
                    ));
                    return MssqlOutputCachePlan::new(
                        MssqlOutputCacheDecision::NoCache {
                            reason: MssqlNoCacheReason::AmbiguousSharedDerivedAlias,
                        },
                        skipped_candidates,
                    );
                }
            }
        }

        MssqlOutputCachePlan::new(
            MssqlOutputCacheDecision::NoCache {
                reason: MssqlNoCacheReason::NoSharedRegisteredDerivedAlias,
            },
            skipped_candidates,
        )
    }

    /// Returns whether a selected output uses a registered derived candidate.
    ///
    /// Direct use and lineage use both count for cache selection. Direct use
    /// covers `big.to_mssql(...)`; lineage use covers downstream SQL such as
    /// `west` reading from `big`, including transitive derived dependencies.
    fn cache_output_uses_registered_derived(
        &self,
        table: &LazyTable,
        candidate: &RegisteredDerivedTable,
    ) -> bool {
        // The selected output itself can be the cache candidate.
        if table.id() == candidate.table().id() {
            return true;
        }
        // Raw Delta sources cannot depend on registered derived aliases.
        if table.kind() == LazyTableKind::DeltaSource {
            return false;
        }

        // Pending or registered derived outputs use captured lineage. If lineage
        // lookup fails, the candidate is not counted for this output.
        self.transitive_registered_derived_dependencies(table)
            .map(|dependencies| {
                dependencies.iter().any(|dependency| {
                    matches!(
                        dependency,
                        DerivedTableDependency::RegisteredDerived { table_id, .. }
                            if *table_id == candidate.table().id()
                    )
                })
            })
            .unwrap_or(false)
    }

    /// Selects the cache frontier from eligible shared derived aliases.
    ///
    /// The frontier is every shared alias that is not covered by a deeper
    /// shared alias. Chain-shaped candidates collapse to the deepest alias,
    /// while independent candidates are kept together even when they serve the
    /// same selected output indexes.
    fn select_shared_cache_frontier(
        &self,
        candidates: Vec<MssqlDerivedCacheAliasPlan>,
    ) -> MssqlCacheFrontierSelection {
        // A candidate is covered when another shared candidate depends on it.
        // Covered aliases are useful upstream work, but caching the deeper
        // shared alias is closer to the final selected outputs.
        let deepest_indexes = candidates
            .iter()
            .enumerate()
            .filter_map(|(candidate_index, candidate)| {
                let covered_by_deeper_candidate =
                    candidates.iter().enumerate().any(|(other_index, other)| {
                        candidate_index != other_index
                            && self.cache_candidate_is_deeper_than(other, candidate)
                    });
                (!covered_by_deeper_candidate).then_some(candidate_index)
            })
            .collect::<Vec<_>>();

        match deepest_indexes.as_slice() {
            [] => MssqlCacheFrontierSelection::Ambiguous {
                ambiguous_aliases: candidates,
            },
            [_, ..] => {
                // The indexes that remain are the frontier. More than one means
                // independent shared aliases, not ambiguity, because no selected
                // alias can replace the work represented by another.
                let selected_aliases = deepest_indexes
                    .iter()
                    .map(|index| candidates[*index].clone())
                    .collect::<Vec<_>>();
                // Keep covered aliases visible in the plan so later reports can
                // explain why broader upstream candidates were not selected.
                let covered_aliases = candidates
                    .iter()
                    .enumerate()
                    .filter_map(|(candidate_index, candidate)| {
                        if deepest_indexes.contains(&candidate_index) {
                            return None;
                        }
                        selected_aliases
                            .iter()
                            .find(|selected| {
                                self.cache_candidate_is_deeper_than(selected, candidate)
                            })
                            .map(|selected| MssqlCoveredCacheAlias {
                                alias: candidate.clone(),
                                selected_table_id: selected.table_id(),
                            })
                    })
                    .collect::<Vec<_>>();
                if deepest_indexes.len() + covered_aliases.len() != candidates.len() {
                    return MssqlCacheFrontierSelection::Ambiguous {
                        ambiguous_aliases: candidates,
                    };
                }
                MssqlCacheFrontierSelection::Selected {
                    selected_aliases,
                    covered_aliases,
                }
            }
        }
    }

    /// Returns whether `candidate` is a downstream derived alias of `other`.
    ///
    /// This is the ordering test used by frontier selection. It is intentionally
    /// based on session-owned table identity plus captured lineage, not on alias
    /// names, SQL text, registration order, or output order.
    fn cache_candidate_is_deeper_than(
        &self,
        candidate: &MssqlDerivedCacheAliasPlan,
        other: &MssqlDerivedCacheAliasPlan,
    ) -> bool {
        if candidate.table_id() == other.table_id() {
            return false;
        }

        // Missing metadata should not create a deeper-than relationship. The
        // caller treats unprovable ordering conservatively when needed.
        let Some(candidate_table) = self.registered_derived_table_by_id(candidate.table_id())
        else {
            return false;
        };
        let Some(other_table) = self.registered_derived_table_by_id(other.table_id()) else {
            return false;
        };

        // Reuse the same direct-or-transitive dependency check as output
        // classification. If candidate depends on other, candidate is closer to
        // outputs that use candidate and can cover the broader upstream alias.
        self.cache_output_uses_registered_derived(candidate_table.table(), other_table)
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        DeltaFunnelError, DeltaSourceConfig, LoadMode, MssqlTargetConfig, MssqlTargetTable,
    };

    use super::super::super::super::{
        DeltaFunnelSession, LazyTable, LazyTableKind, MssqlOutputTarget, OutputWritePlan, RunMode,
        SessionOptions,
        registry::{DerivedTableDependency, DerivedTableLineage},
        test_support::{DeltaLogTable, output_request, secret_connection},
    };

    #[test]
    fn cache_plan_reports_no_shared_alias_for_independent_outputs() -> Result<(), DeltaFunnelError>
    {
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

        assert!(debug.contains("FewerThanTwoOutputs"));
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
}
