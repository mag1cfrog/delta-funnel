use std::fmt;

use crate::support::sanitize_text_for_display;

use super::super::super::{
    DeltaFunnelSession, LazyTable, LazyTableKind, OutputWritePlan, RegisteredDerivedTable,
    registry::DerivedTableDependency,
};

/// Planner output for one `write_all` cache-selection pass.
#[allow(dead_code)]
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct MssqlOutputCachePlan {
    selected_outputs: Vec<MssqlOutputCachePlanOutput>,
    decision: MssqlOutputCacheDecision,
    skipped_candidates: Vec<MssqlCacheCandidateSkip>,
}

#[allow(dead_code)]
impl MssqlOutputCachePlan {
    pub(super) fn new(
        selected_outputs: Vec<MssqlOutputCachePlanOutput>,
        decision: MssqlOutputCacheDecision,
        skipped_candidates: Vec<MssqlCacheCandidateSkip>,
    ) -> Self {
        Self {
            selected_outputs,
            decision,
            skipped_candidates,
        }
    }

    pub(super) fn no_cache(
        selected_outputs: Vec<MssqlOutputCachePlanOutput>,
        reason: MssqlNoCacheReason,
    ) -> Self {
        Self {
            selected_outputs,
            decision: MssqlOutputCacheDecision::NoCache { reason },
            skipped_candidates: Vec::new(),
        }
    }

    /// Returns selected outputs in caller-provided order.
    #[must_use]
    pub(crate) fn selected_outputs(&self) -> &[MssqlOutputCachePlanOutput] {
        &self.selected_outputs
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
            .field("selected_outputs", &self.selected_outputs)
            .field("decision", &self.decision)
            .field("skipped_candidates", &self.skipped_candidates)
            .finish()
    }
}

/// Selected output identity captured for cache planning.
#[allow(dead_code)]
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct MssqlOutputCachePlanOutput {
    index: usize,
    table_id: u64,
    table_name: String,
    output_name: String,
}

#[allow(dead_code)]
impl MssqlOutputCachePlanOutput {
    pub(super) fn from_request(index: usize, request: &OutputWritePlan) -> Self {
        Self {
            index,
            table_id: request.table().id(),
            table_name: request.table().name().to_owned(),
            output_name: request.target().output_name().to_owned(),
        }
    }

    /// Returns the output index from the caller-provided request list.
    #[must_use]
    pub(crate) const fn index(&self) -> usize {
        self.index
    }

    /// Returns the selected lazy table id.
    #[must_use]
    pub(crate) const fn table_id(&self) -> u64 {
        self.table_id
    }

    /// Returns the selected lazy table name.
    #[must_use]
    pub(crate) fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Returns the selected output name.
    #[must_use]
    pub(crate) fn output_name(&self) -> &str {
        &self.output_name
    }
}

impl fmt::Debug for MssqlOutputCachePlanOutput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MssqlOutputCachePlanOutput")
            .field("index", &self.index)
            .field("table_id", &self.table_id)
            .field("table_name", &sanitize_text_for_display(&self.table_name))
            .field("output_name", &sanitize_text_for_display(&self.output_name))
            .finish()
    }
}

/// Cache decision for one `write_all` cache-selection pass.
#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[allow(dead_code)]
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct MssqlDerivedCacheAliasPlan {
    table_id: u64,
    alias: String,
    output_indexes: Vec<usize>,
}

#[allow(dead_code)]
impl MssqlDerivedCacheAliasPlan {
    pub(super) fn new(table_id: u64, alias: String, output_indexes: Vec<usize>) -> Self {
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
#[allow(dead_code)]
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
#[allow(dead_code)]
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct MssqlCacheCandidateSkip {
    table_id: u64,
    alias: String,
    reason: MssqlCacheCandidateSkipReason,
}

#[allow(dead_code)]
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
#[allow(dead_code)]
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
    #[allow(dead_code)]
    pub(crate) fn plan_mssql_output_cache(
        &self,
        requests: &[OutputWritePlan],
    ) -> MssqlOutputCachePlan {
        let selected_outputs = requests
            .iter()
            .enumerate()
            .map(|(index, request)| MssqlOutputCachePlanOutput::from_request(index, request))
            .collect::<Vec<_>>();
        if selected_outputs.len() < 2 {
            return MssqlOutputCachePlan::no_cache(
                selected_outputs,
                MssqlNoCacheReason::FewerThanTwoOutputs,
            );
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
                selected_outputs,
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
                        selected_outputs,
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
                        selected_outputs,
                        MssqlOutputCacheDecision::NoCache {
                            reason: MssqlNoCacheReason::AmbiguousSharedDerivedAlias,
                        },
                        skipped_candidates,
                    );
                }
            }
        }

        MssqlOutputCachePlan::new(
            selected_outputs,
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
