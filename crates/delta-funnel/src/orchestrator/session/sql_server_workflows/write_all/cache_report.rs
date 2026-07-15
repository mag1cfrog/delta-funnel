use crate::{
    WriteAllCacheAliasReport, WriteAllCacheCandidateSkip, WriteAllCacheCandidateSkipReason,
    WriteAllCacheReport, WriteAllNoCacheReason,
};

use super::{
    MssqlCacheCandidateSkip, MssqlCacheCandidateSkipReason, MssqlNoCacheReason,
    MssqlOutputCacheDecision, MssqlOutputCachePlan,
};

pub(super) fn disabled() -> WriteAllCacheReport {
    WriteAllCacheReport::disabled()
}

pub(super) fn from_plan(plan: &MssqlOutputCachePlan) -> WriteAllCacheReport {
    let skipped_candidates = skipped_candidate_reports(plan);

    match plan.decision() {
        MssqlOutputCacheDecision::NoCache { reason } => {
            WriteAllCacheReport::no_cache(no_cache_reason_report(reason), skipped_candidates)
        }
        MssqlOutputCacheDecision::CacheAliases(aliases) => {
            let aliases = aliases
                .iter()
                .map(|alias| {
                    WriteAllCacheAliasReport::selected(
                        alias.table_id(),
                        alias.alias(),
                        alias.output_indexes().to_vec(),
                    )
                })
                .collect::<Vec<_>>();

            WriteAllCacheReport::cache_aliases(aliases, skipped_candidates)
        }
    }
}

pub(super) fn from_executed_plan(
    plan: &MssqlOutputCachePlan,
    aliases: Vec<WriteAllCacheAliasReport>,
) -> WriteAllCacheReport {
    let skipped_candidates = skipped_candidate_reports(plan);

    match plan.decision() {
        MssqlOutputCacheDecision::NoCache { reason } => {
            debug_assert!(aliases.is_empty());
            WriteAllCacheReport::no_cache(no_cache_reason_report(reason), skipped_candidates)
        }
        MssqlOutputCacheDecision::CacheAliases(planned_aliases) => {
            debug_assert_eq!(aliases.len(), planned_aliases.len());
            WriteAllCacheReport::cache_aliases(aliases, skipped_candidates)
        }
    }
}

fn skipped_candidate_reports(plan: &MssqlOutputCachePlan) -> Vec<WriteAllCacheCandidateSkip> {
    plan.skipped_candidates()
        .iter()
        .map(candidate_skip_report)
        .collect()
}

fn no_cache_reason_report(reason: &MssqlNoCacheReason) -> WriteAllNoCacheReason {
    match reason {
        MssqlNoCacheReason::FewerThanTwoOutputs => WriteAllNoCacheReason::FewerThanTwoOutputs,
        MssqlNoCacheReason::NoSharedRegisteredDerivedAlias => {
            WriteAllNoCacheReason::NoSharedRegisteredDerivedAlias
        }
        MssqlNoCacheReason::AmbiguousSharedDerivedAlias => {
            WriteAllNoCacheReason::AmbiguousSharedDerivedAlias
        }
    }
}

fn candidate_skip_report(skip: &MssqlCacheCandidateSkip) -> WriteAllCacheCandidateSkip {
    WriteAllCacheCandidateSkip::new(
        skip.table_id(),
        skip.alias(),
        candidate_skip_reason_report(skip.reason()),
    )
}

fn candidate_skip_reason_report(
    reason: &MssqlCacheCandidateSkipReason,
) -> WriteAllCacheCandidateSkipReason {
    match reason {
        MssqlCacheCandidateSkipReason::NotShared { output_count } => {
            WriteAllCacheCandidateSkipReason::NotShared {
                output_count: *output_count,
            }
        }
        MssqlCacheCandidateSkipReason::MissingSqlText => {
            WriteAllCacheCandidateSkipReason::MissingSqlText
        }
        MssqlCacheCandidateSkipReason::IncompleteLineage => {
            WriteAllCacheCandidateSkipReason::IncompleteLineage
        }
        MssqlCacheCandidateSkipReason::CoveredByDeeperSharedAlias { selected_table_id } => {
            WriteAllCacheCandidateSkipReason::CoveredByDeeperSharedAlias {
                selected_table_id: *selected_table_id,
            }
        }
        MssqlCacheCandidateSkipReason::AmbiguousDepth => {
            WriteAllCacheCandidateSkipReason::AmbiguousDepth
        }
    }
}
