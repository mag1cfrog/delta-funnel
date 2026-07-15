use std::{fmt, panic::resume_unwind, sync::Arc};

use datafusion::{
    arrow::record_batch::RecordBatch,
    datasource::{MemTable, TableProvider},
    execution::TaskContext,
    physical_plan::{ExecutionPlan, execute_stream_partitioned},
    prelude::SessionContext,
};
use futures_util::{StreamExt, TryStreamExt};
use tokio::{sync::watch, task::JoinSet};

use crate::{
    DeltaFunnelError, MssqlOutputBatchStream, MssqlWorkflowWriteReport, PhaseTimingReport,
    ReportReasonCode, WriteAllCacheAliasReport, WriteAllCacheAliasStatus, WriteAllCacheFailure,
    observability::DeltaProviderScanOutcome,
    progress::{ProgressEvent, ProgressPhase, ProgressReporter},
    query_engine::datafusion::{
        DeltaProviderReadStatsHandle, collect_delta_provider_read_stats_handles,
    },
    report::PhaseTimer,
    support::sanitize_text_for_display,
};

use super::super::super::{
    DeltaFunnelSession, LazyTable,
    errors::{mssql_scoped_cache_alias_error, unknown_cached_alias_error},
    query_handoff::{
        DeltaFileProgressSampler, finalize_tracked_query_execution,
        track_partitioned_scan_completion,
    },
};
use super::MssqlDerivedCacheAliasPlan;

const CACHE_ALIAS_DATAFRAME_RESOLUTION_PHASE: &str = "cache_alias_dataframe_resolution";
const CACHE_ALIAS_PHYSICAL_PLANNING_PHASE: &str = "cache_alias_physical_planning";
const CACHE_ALIAS_STREAM_SETUP_PHASE: &str = "cache_alias_stream_setup";
const CACHE_ALIAS_EXECUTE_COLLECT_PHASE: &str = "cache_alias_execute_collect";
const CACHE_ALIAS_MEMTABLE_BUILD_PHASE: &str = "cache_alias_memtable_build";
const CACHE_ALIAS_MATERIALIZATION_TOTAL_PHASE: &str = "cache_alias_materialization_total";
const CACHE_ALIAS_INSTALL_PHASE: &str = "cache_alias_install";
const CACHE_ALIAS_RESTORE_PHASE: &str = "cache_alias_restore";
const CACHE_ALIAS_MATERIALIZATION_PHASES: [&str; 5] = [
    CACHE_ALIAS_DATAFRAME_RESOLUTION_PHASE,
    CACHE_ALIAS_PHYSICAL_PLANNING_PHASE,
    CACHE_ALIAS_STREAM_SETUP_PHASE,
    CACHE_ALIAS_EXECUTE_COLLECT_PHASE,
    CACHE_ALIAS_MEMTABLE_BUILD_PHASE,
];

struct MaterializedCache {
    provider: Arc<dyn TableProvider>,
    phase_timings: Vec<PhaseTimingReport>,
}

struct CacheAliasPhaseFailure {
    source: DeltaFunnelError,
    phase_timings: Vec<PhaseTimingReport>,
    failed_phase: &'static str,
}

impl CacheAliasPhaseFailure {
    #[cfg(test)]
    fn into_source(self) -> DeltaFunnelError {
        self.source
    }

    fn into_alias_failure(
        self,
        table_id: u64,
        alias: String,
        output_indexes: Vec<usize>,
    ) -> CacheAliasReplacementFailure {
        CacheAliasReplacementFailure {
            source: self.source,
            report: Some(WriteAllCacheAliasReport::executed(
                table_id,
                alias,
                output_indexes,
                WriteAllCacheAliasStatus::Failed,
                self.phase_timings,
                Some(self.failed_phase),
            )),
        }
    }
}

struct CacheAliasReplacementFailure {
    source: DeltaFunnelError,
    report: Option<WriteAllCacheAliasReport>,
}

impl CacheAliasReplacementFailure {
    fn before_attempt(source: DeltaFunnelError) -> Self {
        Self {
            source,
            report: None,
        }
    }
}

struct CacheAliasInstallFailure {
    source: Box<DeltaFunnelError>,
    restore_timing: Option<PhaseTimingReport>,
}

/// Active replacement of one registered derived alias with a cached provider.
///
/// The original provider is owned by this scope until `restore` is called.
/// Callers must not rely on `Drop` for restoration.
pub(crate) struct MssqlScopedCacheAliasReplacement<'a> {
    context: &'a SessionContext,
    table_id: u64,
    alias_name: String,
    output_indexes: Vec<usize>,
    original_provider: Arc<dyn TableProvider>,
    phase_timings: Vec<PhaseTimingReport>,
}

impl<'a> MssqlScopedCacheAliasReplacement<'a> {
    pub(super) fn new(
        context: &'a SessionContext,
        table_id: u64,
        alias_name: String,
        output_indexes: Vec<usize>,
        original_provider: Arc<dyn TableProvider>,
        phase_timings: Vec<PhaseTimingReport>,
    ) -> Self {
        Self {
            context,
            table_id,
            alias_name,
            output_indexes,
            original_provider,
            phase_timings,
        }
    }

    /// Restores the original provider under the alias and consumes the scope.
    ///
    /// This method transitions the catalog from "alias points at cached
    /// provider" back to "alias points at the original provider".
    ///
    /// Callers should use this method on both success and error paths that
    /// leave the scoped replacement active.
    #[cfg(test)]
    pub(crate) fn restore(self) -> Result<(), DeltaFunnelError> {
        self.restore_with_report().1
    }

    fn restore_with_report(self) -> (WriteAllCacheAliasReport, Result<(), DeltaFunnelError>) {
        let Self {
            context,
            table_id,
            alias_name,
            output_indexes,
            original_provider,
            mut phase_timings,
        } = self;
        let restore_timer = PhaseTimer::start(CACHE_ALIAS_RESTORE_PHASE);
        let restore_result = context
            .deregister_table(alias_name.as_str())
            .map_err(|error| {
                mssql_scoped_cache_alias_error("restore_deregister", &alias_name, error)
            })
            .and_then(|_| {
                context
                    .register_table(alias_name.as_str(), original_provider)
                    .map(|_| ())
                    .map_err(|error| {
                        mssql_scoped_cache_alias_error("restore_register", &alias_name, error)
                    })
            });

        let (status, failed_phase) = match &restore_result {
            Ok(()) => {
                phase_timings.push(restore_timer.completed());
                (WriteAllCacheAliasStatus::MaterializedAndRestored, None)
            }
            Err(_) => {
                phase_timings.push(restore_timer.failed());
                (
                    WriteAllCacheAliasStatus::Failed,
                    Some(CACHE_ALIAS_RESTORE_PHASE),
                )
            }
        };
        let report = WriteAllCacheAliasReport::executed(
            table_id,
            alias_name,
            output_indexes,
            status,
            phase_timings,
            failed_phase,
        );
        (report, restore_result)
    }
}

impl DeltaFunnelSession {
    /// Materializes one registered derived alias and temporarily replaces that alias.
    ///
    /// The method leaves the original catalog alias active while DataFusion
    /// builds the cache, then swaps the catalog entry to the cached provider.
    /// The returned scope owns the original provider and must be restored with
    /// `MssqlScopedCacheAliasReplacement::restore`.
    ///
    /// This is intentionally a one-alias primitive. It does not choose cache
    /// candidates, replan downstream SQL, or execute any outputs.
    #[cfg(test)]
    pub(crate) async fn replace_registered_derived_alias_with_cache(
        &self,
        table: &LazyTable,
        reporter: Option<&ProgressReporter>,
    ) -> Result<MssqlScopedCacheAliasReplacement<'_>, DeltaFunnelError> {
        self.replace_registered_derived_alias_with_cache_attempt(table, Vec::new(), reporter)
            .await
            .map_err(|failure| failure.source)
    }

    async fn replace_registered_derived_alias_with_cache_attempt(
        &self,
        table: &LazyTable,
        output_indexes: Vec<usize>,
        reporter: Option<&ProgressReporter>,
    ) -> Result<MssqlScopedCacheAliasReplacement<'_>, CacheAliasReplacementFailure> {
        let registered = self
            .registered_derived_for_scoped_cache_alias(table)
            .map_err(CacheAliasReplacementFailure::before_attempt)?;
        let alias_name = registered.name().to_owned();
        if self.context.state_ref().read().cache_factory().is_some() {
            return Err(CacheAliasReplacementFailure::before_attempt(
                DeltaFunnelError::MssqlWorkflowPlanning {
                    message: "`write_all` cache alias materialization requires DataFusion's default cache behavior; custom cache factories are not supported"
                        .to_owned(),
                },
            ));
        }

        if let Some(reporter) = reporter {
            reporter.emit(&ProgressEvent::phase_changed(
                ProgressPhase::MaterializingCache,
                None,
            ));
        }
        let MaterializedCache {
            provider,
            mut phase_timings,
        } = self
            .materialize_cache(alias_name.as_str(), reporter)
            .await
            .map_err(|failure| {
                failure.into_alias_failure(table.id(), alias_name.clone(), output_indexes.clone())
            })?;

        let install_timer = PhaseTimer::start(CACHE_ALIAS_INSTALL_PHASE);
        let original_provider =
            match self.install_scoped_cache_alias_provider(alias_name.as_str(), provider) {
                Ok(original_provider) => {
                    phase_timings.push(install_timer.completed());
                    original_provider
                }
                Err(failure) => {
                    phase_timings.push(install_timer.failed());
                    phase_timings.push(failure.restore_timing.unwrap_or_else(|| {
                        PhaseTimingReport::not_started(
                            CACHE_ALIAS_RESTORE_PHASE,
                            ReportReasonCode::PriorFailure,
                        )
                    }));
                    return Err(CacheAliasPhaseFailure {
                        source: *failure.source,
                        phase_timings,
                        failed_phase: CACHE_ALIAS_INSTALL_PHASE,
                    }
                    .into_alias_failure(table.id(), alias_name, output_indexes));
                }
            };

        Ok(MssqlScopedCacheAliasReplacement::new(
            &self.context,
            table.id(),
            alias_name,
            output_indexes,
            original_provider,
            phase_timings,
        ))
    }

    pub(super) async fn replace_mssql_cache_aliases(
        &self,
        cache_aliases: &[MssqlDerivedCacheAliasPlan],
        reporter: Option<&ProgressReporter>,
    ) -> Result<Vec<MssqlScopedCacheAliasReplacement<'_>>, DeltaFunnelError> {
        let mut replacements = Vec::new();

        for cache_alias in cache_aliases {
            let Some(registered) = self.registered_derived_table_by_id(cache_alias.table_id())
            else {
                return Err(restore_cache_aliases_after_failure(
                    unknown_cached_alias_error(cache_alias),
                    replacements,
                    None,
                    reporter,
                ));
            };
            let table = registered.table().clone();

            match self
                .replace_registered_derived_alias_with_cache_attempt(
                    &table,
                    cache_alias.output_indexes().to_vec(),
                    reporter,
                )
                .await
            {
                Ok(replacement) => replacements.push(replacement),
                Err(failure) => {
                    return Err(restore_cache_aliases_after_failure(
                        failure.source,
                        replacements,
                        failure.report,
                        reporter,
                    ));
                }
            }
        }

        Ok(replacements)
    }

    /// Materializes the default DataFusion memory cache.
    ///
    /// This preserves the physical plan's partition layout and concurrent
    /// partition collection. When supplied, progress is action-level and has
    /// no output name or position.
    async fn materialize_cache(
        &self,
        alias_name: &str,
        reporter: Option<&ProgressReporter>,
    ) -> Result<MaterializedCache, CacheAliasPhaseFailure> {
        let materialization_timer = PhaseTimer::start(CACHE_ALIAS_MATERIALIZATION_TOTAL_PHASE);
        let mut phase_timings = Vec::with_capacity(8);

        let resolution_timer = PhaseTimer::start(CACHE_ALIAS_DATAFRAME_RESOLUTION_PHASE);
        let dataframe = match self.context.table(alias_name).await {
            Ok(dataframe) => dataframe,
            Err(error) => {
                return Err(cache_alias_materialization_failure(
                    mssql_scoped_cache_alias_error("resolve", alias_name, error),
                    phase_timings,
                    resolution_timer,
                    materialization_timer,
                ));
            }
        };
        phase_timings.push(resolution_timer.completed());

        let task_ctx = Arc::new(dataframe.task_ctx());
        let planning_timer = PhaseTimer::start(CACHE_ALIAS_PHYSICAL_PLANNING_PHASE);
        let physical_plan = match dataframe.create_physical_plan().await {
            Ok(physical_plan) => physical_plan,
            Err(error) => {
                return Err(cache_alias_materialization_failure(
                    mssql_scoped_cache_alias_error("materialize", alias_name, error),
                    phase_timings,
                    planning_timer,
                    materialization_timer,
                ));
            }
        };
        phase_timings.push(planning_timer.completed());

        let schema = physical_plan.schema();
        let read_stats_handles = collect_delta_provider_read_stats_handles(physical_plan.as_ref());
        let stream_setup_timer = PhaseTimer::start(CACHE_ALIAS_STREAM_SETUP_PHASE);
        let sampler = reporter.map(|reporter| {
            DeltaFileProgressSampler::new(
                read_stats_handles.clone(),
                reporter.clone(),
                ProgressPhase::MaterializingCache,
                None,
            )
        });
        let streams = match start_cache_partition_streams(
            physical_plan,
            task_ctx,
            read_stats_handles,
            alias_name,
        ) {
            Ok(streams) => streams,
            Err(error) => {
                return Err(cache_alias_materialization_failure(
                    error,
                    phase_timings,
                    stream_setup_timer,
                    materialization_timer,
                ));
            }
        };
        phase_timings.push(stream_setup_timer.completed());

        let collect_timer = PhaseTimer::start(CACHE_ALIAS_EXECUTE_COLLECT_PHASE);
        let partitions = match collect_cache_partitions(streams, sampler, alias_name).await {
            Ok(partitions) => partitions,
            Err(error) => {
                return Err(cache_alias_materialization_failure(
                    error,
                    phase_timings,
                    collect_timer,
                    materialization_timer,
                ));
            }
        };
        phase_timings.push(collect_timer.completed());

        let memtable_timer = PhaseTimer::start(CACHE_ALIAS_MEMTABLE_BUILD_PHASE);
        let cached_provider = match MemTable::try_new(schema, partitions) {
            Ok(cached_provider) => cached_provider,
            Err(error) => {
                return Err(cache_alias_materialization_failure(
                    mssql_scoped_cache_alias_error("materialize", alias_name, error),
                    phase_timings,
                    memtable_timer,
                    materialization_timer,
                ));
            }
        };
        let provider: Arc<dyn TableProvider> = Arc::new(cached_provider);
        phase_timings.push(memtable_timer.completed());
        phase_timings.push(materialization_timer.completed());

        Ok(MaterializedCache {
            provider,
            phase_timings,
        })
    }

    /// Swaps a catalog alias from its original provider to a cached provider.
    ///
    /// On success, the alias points at `cached_provider` and the original
    /// provider is returned to the caller for later restoration. If registering
    /// the cached provider fails after the original provider has been removed,
    /// this helper attempts to put the original provider back before returning
    /// the error.
    fn install_scoped_cache_alias_provider(
        &self,
        alias_name: &str,
        cached_provider: Arc<dyn TableProvider>,
    ) -> Result<Arc<dyn TableProvider>, CacheAliasInstallFailure> {
        let original_provider = self
            .context
            .deregister_table(alias_name)
            .map_err(|error| CacheAliasInstallFailure {
                source: Box::new(mssql_scoped_cache_alias_error(
                    "deregister",
                    alias_name,
                    error,
                )),
                restore_timing: None,
            })?
            .ok_or_else(|| CacheAliasInstallFailure {
                source: Box::new(mssql_scoped_cache_alias_error(
                    "deregister",
                    alias_name,
                    "registered alias was missing from the catalog",
                )),
                restore_timing: None,
            })?;

        if let Err(register_error) = self.context.register_table(alias_name, cached_provider) {
            return Err(self.restore_original_after_cached_register_failure(
                alias_name,
                original_provider,
                register_error,
            ));
        }

        Ok(original_provider)
    }

    /// Restores the original provider after cached-provider registration fails.
    ///
    /// This helper is used only for the narrow failure window where the
    /// original provider has already been deregistered but the cached provider
    /// could not be registered. The returned error reports the cached register
    /// failure while the separate restore timing records cleanup success or failure.
    fn restore_original_after_cached_register_failure(
        &self,
        alias_name: &str,
        original_provider: Arc<dyn TableProvider>,
        register_error: impl fmt::Display,
    ) -> CacheAliasInstallFailure {
        let restore_timer = PhaseTimer::start(CACHE_ALIAS_RESTORE_PHASE);
        let restore_result = self.context.register_table(alias_name, original_provider);
        let restore_timing = match restore_result {
            Ok(_) => restore_timer.completed(),
            Err(_) => restore_timer.failed(),
        };
        CacheAliasInstallFailure {
            source: Box::new(DeltaFunnelError::MssqlWorkflowPlanning {
                message: format!(
                    "failed to register cached provider for alias `{}`: {}",
                    sanitize_text_for_display(alias_name),
                    sanitize_text_for_display(&register_error.to_string())
                ),
            }),
            restore_timing: Some(restore_timing),
        }
    }
}

fn cache_alias_materialization_failure(
    source: DeltaFunnelError,
    mut phase_timings: Vec<PhaseTimingReport>,
    failed_timer: PhaseTimer,
    materialization_timer: PhaseTimer,
) -> CacheAliasPhaseFailure {
    let failed_phase_index = phase_timings.len();
    let failed_phase = CACHE_ALIAS_MATERIALIZATION_PHASES[failed_phase_index];
    phase_timings.push(failed_timer.failed());
    phase_timings.extend(
        CACHE_ALIAS_MATERIALIZATION_PHASES
            .iter()
            .skip(failed_phase_index.saturating_add(1))
            .map(|phase_name| {
                PhaseTimingReport::not_started(*phase_name, ReportReasonCode::PriorFailure)
            }),
    );
    phase_timings.push(materialization_timer.failed());
    phase_timings.extend(
        [CACHE_ALIAS_INSTALL_PHASE, CACHE_ALIAS_RESTORE_PHASE]
            .into_iter()
            .map(|phase_name| {
                PhaseTimingReport::not_started(phase_name, ReportReasonCode::PriorFailure)
            }),
    );
    CacheAliasPhaseFailure {
        source,
        phase_timings,
        failed_phase,
    }
}

/// Starts partitioned cache execution and attaches one shared terminal tracker.
///
/// If DataFusion fails before returning stream ownership, the already collected
/// Delta scan handles are finalized immediately with an error outcome.
fn start_cache_partition_streams(
    physical_plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
    read_stats_handles: Vec<DeltaProviderReadStatsHandle>,
    alias_name: &str,
) -> Result<Vec<MssqlOutputBatchStream>, DeltaFunnelError> {
    let streams = match execute_stream_partitioned(physical_plan, task_ctx) {
        Ok(streams) => streams,
        Err(error) => {
            finalize_tracked_query_execution(
                &read_stats_handles,
                None,
                None,
                DeltaProviderScanOutcome::Error,
            );
            return Err(mssql_scoped_cache_alias_error(
                "materialize",
                alias_name,
                error,
            ));
        }
    };
    let streams = streams
        .into_iter()
        .map(|stream| {
            let alias_name = alias_name.to_owned();
            let stream: MssqlOutputBatchStream = Box::pin(stream.map(move |batch| {
                batch.map_err(|error| {
                    mssql_scoped_cache_alias_error("materialize", alias_name.as_str(), error)
                })
            }));
            stream
        })
        .collect();
    Ok(track_partitioned_scan_completion(
        streams,
        read_stats_handles,
    ))
}

/// Collects each cache partition in its own Tokio task and restores its order.
async fn collect_cache_partitions(
    streams: Vec<MssqlOutputBatchStream>,
    sampler: Option<DeltaFileProgressSampler>,
    alias_name: &str,
) -> Result<Vec<Vec<RecordBatch>>, DeltaFunnelError> {
    let (progress_changed, progress_changes) = watch::channel(());
    let task_progress_signal = sampler.as_ref().map(|_| progress_changed.clone());
    let tasks = spawn_cache_partition_tasks(streams, task_progress_signal);
    // Once the tasks start, only their senders should keep this channel open.
    drop(progress_changed);

    let mut partitions =
        join_cache_partition_tasks(tasks, progress_changes, sampler, alias_name).await?;
    partitions.sort_by_key(|(partition_index, _)| *partition_index);
    Ok(partitions.into_iter().map(|(_, batches)| batches).collect())
}

type CachePartitionTaskOutput = (usize, Result<Vec<RecordBatch>, DeltaFunnelError>);

/// Starts one collector task per partition and retains each partition index.
fn spawn_cache_partition_tasks(
    streams: Vec<MssqlOutputBatchStream>,
    progress_signal: Option<watch::Sender<()>>,
) -> JoinSet<CachePartitionTaskOutput> {
    let mut tasks = JoinSet::new();
    // Partition tasks only announce stream-consumption boundaries. The parent
    // task samples counters and calls the reporter, so no callback needs a
    // cross-task delivery lock.
    for (partition_index, stream) in streams.into_iter().enumerate() {
        let progress_signal = progress_signal.clone();
        tasks.spawn(async move {
            let batches = match progress_signal {
                Some(progress_signal) => {
                    // Notify the parent after each ready item. It samples
                    // counters and delivers callbacks serially.
                    stream
                        .inspect(move |_| {
                            let _ = progress_signal.send(());
                        })
                        .try_collect::<Vec<_>>()
                        .await
                }
                // Avoid per-batch channel work when progress is disabled.
                None => stream.try_collect::<Vec<_>>().await,
            };
            (partition_index, batches)
        });
    }

    tasks
}

/// Joins partition collectors while sampling progress on the parent task.
///
/// Stream errors remain attached to their partition result. Task panics resume
/// unwinding, while other join failures become normal cache materialization
/// errors.
async fn join_cache_partition_tasks(
    mut tasks: JoinSet<CachePartitionTaskOutput>,
    mut progress_changes: watch::Receiver<()>,
    mut sampler: Option<DeltaFileProgressSampler>,
    alias_name: &str,
) -> Result<Vec<(usize, Vec<RecordBatch>)>, DeltaFunnelError> {
    let mut partitions = Vec::new();
    while !tasks.is_empty() {
        tokio::select! {
            Ok(()) = progress_changes.changed(), if sampler.is_some() => {
                // A partition consumed another stream item, so its shared
                // provider counters may now have changed.
                emit_cache_progress(&mut sampler);
            }
            Some(result) = tasks.join_next() => {
                // One collector stopped. Settle its task result before keeping
                // the partition batches.
                let (partition_index, batches) = match result {
                    Ok(partition) => partition,
                    // Preserve the existing panic instead of hiding it inside
                    // a workflow error.
                    Err(error) if error.is_panic() => resume_unwind(error.into_panic()),
                    Err(error) => {
                        // A cancelled task has no partition result to retain.
                        emit_cache_progress(&mut sampler);
                        let primary_error = mssql_scoped_cache_alias_error(
                            "materialize",
                            alias_name,
                            error,
                        );
                        abort_and_drain_cache_partition_tasks(&mut tasks).await;
                        return Err(primary_error);
                    }
                };
                emit_cache_progress(&mut sampler);
                let batches = match batches {
                    Ok(batches) => batches,
                    Err(primary_error) => {
                        abort_and_drain_cache_partition_tasks(&mut tasks).await;
                        return Err(primary_error);
                    }
                };
                partitions.push((partition_index, batches));
            }
        }
    }
    Ok(partitions)
}

/// Cancels sibling collectors and waits until every owned stream is dropped.
async fn abort_and_drain_cache_partition_tasks(tasks: &mut JoinSet<CachePartitionTaskOutput>) {
    tasks.abort_all();
    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result
            && error.is_panic()
        {
            resume_unwind(error.into_panic());
        }
    }
}

fn emit_cache_progress(sampler: &mut Option<DeltaFileProgressSampler>) {
    if let Some(sampler) = sampler {
        sampler.emit_if_changed();
    }
}

pub(super) fn restore_mssql_cache_aliases_with_reports(
    replacements: Vec<MssqlScopedCacheAliasReplacement<'_>>,
    reporter: Option<&ProgressReporter>,
) -> (
    Vec<WriteAllCacheAliasReport>,
    Result<(), (u64, DeltaFunnelError)>,
) {
    if !replacements.is_empty()
        && let Some(reporter) = reporter
    {
        reporter.emit(&ProgressEvent::phase_changed(
            ProgressPhase::RestoringCache,
            None,
        ));
    }
    let mut first_error = None;
    let mut reports = Vec::with_capacity(replacements.len());

    for replacement in replacements.into_iter().rev() {
        let (report, restore_result) = replacement.restore_with_report();
        if let Err(error) = restore_result
            && first_error.is_none()
        {
            first_error = Some((report.table_id(), error));
        }
        reports.push(report);
    }
    reports.reverse();

    let result = match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    };
    (reports, result)
}

pub(super) fn restore_cache_aliases_after_failure(
    source: DeltaFunnelError,
    replacements: Vec<MssqlScopedCacheAliasReplacement<'_>>,
    failed_alias: Option<WriteAllCacheAliasReport>,
    reporter: Option<&ProgressReporter>,
) -> DeltaFunnelError {
    let primary_failed_alias_table_id = failed_alias
        .as_ref()
        .map(WriteAllCacheAliasReport::table_id);
    let (mut reports, _restore_result) =
        restore_mssql_cache_aliases_with_reports(replacements, reporter);
    if let Some(failed_alias) = failed_alias {
        reports.push(failed_alias);
    }
    write_all_cache_failure(source, reports, primary_failed_alias_table_id, None)
}

pub(super) fn write_all_cache_failure(
    source: DeltaFunnelError,
    aliases: Vec<WriteAllCacheAliasReport>,
    primary_failed_alias_table_id: Option<u64>,
    workflow: Option<MssqlWorkflowWriteReport>,
) -> DeltaFunnelError {
    DeltaFunnelError::WriteAllCache {
        failure: Box::new(WriteAllCacheFailure::new(
            aliases,
            primary_failed_alias_table_id,
            workflow,
        )),
        source: Box::new(source),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        any::Any,
        collections::HashMap,
        sync::{
            Arc, Mutex, MutexGuard,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        task::Poll,
        time::Duration,
    };

    use async_trait::async_trait;
    use datafusion::{
        arrow::{
            array::{ArrayRef, StringArray},
            datatypes::{DataType, Field, Schema, SchemaRef},
            record_batch::RecordBatch,
        },
        catalog::SchemaProvider,
        common::{DataFusionError, Result as DataFusionResult},
        execution::session_state::{CacheFactory, SessionState},
        logical_expr::LogicalPlan,
        physical_plan::collect_partitioned,
    };
    use futures_util::stream;
    use tokio::sync::Notify;

    use super::*;

    use crate::{
        DeltaFunnelError, DeltaSourceConfig, ExecutionProfileMode, LoadMode,
        MssqlStreamBenchmarkOutputWriter, PhaseStatus,
        observability::test_capture::{CapturedEvent, CapturedEvents, TracingCapture},
        orchestrator::session::{
            DeltaFunnelSession, SessionOptions,
            test_support::{
                StreamSetupFailingPlan, execute_output_request, marker_values_from_batches,
                scan_counting_marker_region_provider, secret_connection,
            },
        },
        query_engine::datafusion::test_support::SingleSchemaCatalogProvider,
        table_formats::RealParquetDeltaTable,
    };
    use tracing::Level;

    #[derive(Debug)]
    struct RecordingCacheFactory {
        calls: Arc<AtomicUsize>,
    }

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[derive(Debug, Default)]
    struct FailOnceSchema {
        tables: Mutex<HashMap<String, Arc<dyn TableProvider>>>,
        fail_next_registration: AtomicBool,
    }

    impl FailOnceSchema {
        fn fail_next_registration(&self) {
            self.fail_next_registration.store(true, Ordering::SeqCst);
        }

        fn tables(&self) -> MutexGuard<'_, HashMap<String, Arc<dyn TableProvider>>> {
            self.tables
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
        }
    }

    #[async_trait]
    impl SchemaProvider for FailOnceSchema {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn table_names(&self) -> Vec<String> {
            self.tables().keys().cloned().collect()
        }

        async fn table(
            &self,
            name: &str,
        ) -> DataFusionResult<Option<Arc<dyn TableProvider>>, DataFusionError> {
            Ok(self.tables().get(name).cloned())
        }

        fn register_table(
            &self,
            name: String,
            table: Arc<dyn TableProvider>,
        ) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
            if self.fail_next_registration.swap(false, Ordering::SeqCst) {
                return Err(DataFusionError::Execution(
                    "injected schema registration failure".to_owned(),
                ));
            }
            if self.table_exist(name.as_str()) {
                return Err(DataFusionError::Execution(format!(
                    "table `{name}` already exists"
                )));
            }
            Ok(self.tables().insert(name, table))
        }

        fn deregister_table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
            Ok(self.tables().remove(name))
        }

        fn table_exist(&self, name: &str) -> bool {
            self.tables().contains_key(name)
        }
    }

    impl CacheFactory for RecordingCacheFactory {
        fn create(
            &self,
            plan: LogicalPlan,
            _session_state: &SessionState,
        ) -> datafusion::error::Result<LogicalPlan> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(plan)
        }
    }

    fn marker_batch(
        schema: &SchemaRef,
        markers: Vec<&str>,
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        Ok(RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(StringArray::from(markers)) as ArrayRef],
        )?)
    }

    fn session_with_fail_once_schema()
    -> Result<(DeltaFunnelSession, Arc<FailOnceSchema>), Box<dyn std::error::Error>> {
        let session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let schema = Arc::new(FailOnceSchema::default());
        let schema_provider: Arc<dyn SchemaProvider> = schema.clone();
        session.context().register_catalog(
            "datafusion",
            Arc::new(SingleSchemaCatalogProvider::new(schema_provider)),
        );
        Ok((session, schema))
    }

    async fn register_cache_alias(
        session: &mut DeltaFunnelSession,
    ) -> Result<LazyTable, Box<dyn std::error::Error>> {
        let (source_provider, _) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        Ok(session.register_alias("big", &pending_big)?)
    }

    fn assert_cache_phase_statuses(
        report: &WriteAllCacheAliasReport,
        expected_statuses: [PhaseStatus; 8],
    ) {
        assert_eq!(
            report
                .phase_timings()
                .iter()
                .map(PhaseTimingReport::phase_name)
                .collect::<Vec<_>>(),
            [
                CACHE_ALIAS_DATAFRAME_RESOLUTION_PHASE,
                CACHE_ALIAS_PHYSICAL_PLANNING_PHASE,
                CACHE_ALIAS_STREAM_SETUP_PHASE,
                CACHE_ALIAS_EXECUTE_COLLECT_PHASE,
                CACHE_ALIAS_MEMTABLE_BUILD_PHASE,
                CACHE_ALIAS_MATERIALIZATION_TOTAL_PHASE,
                CACHE_ALIAS_INSTALL_PHASE,
                CACHE_ALIAS_RESTORE_PHASE,
            ]
        );
        assert_eq!(
            report
                .phase_timings()
                .iter()
                .map(PhaseTimingReport::status)
                .collect::<Vec<_>>(),
            expected_statuses
        );
    }

    async fn memtable_partitions(
        provider: &Arc<dyn TableProvider>,
    ) -> Result<Vec<Vec<RecordBatch>>, Box<dyn std::error::Error>> {
        let table = provider
            .as_any()
            .downcast_ref::<MemTable>()
            .ok_or("expected MemTable cache provider")?;
        let mut partitions = Vec::with_capacity(table.batches.len());
        for partition in &table.batches {
            partitions.push(partition.read().await.clone());
        }
        Ok(partitions)
    }

    fn marker_batches_by_partition(
        partitions: &[Vec<RecordBatch>],
    ) -> Result<Vec<Vec<Vec<String>>>, Box<dyn std::error::Error>> {
        partitions
            .iter()
            .map(|partition| {
                partition
                    .iter()
                    .map(|batch| marker_values_from_batches(std::slice::from_ref(batch)))
                    .collect()
            })
            .collect()
    }

    fn provider_io_events(events: &CapturedEvents) -> Vec<CapturedEvent> {
        events
            .events()
            .into_iter()
            .filter(|event| {
                event.fields.get("telemetry_event").map(String::as_str)
                    == Some("delta_provider_parquet_io_summary")
            })
            .collect()
    }

    #[tokio::test]
    async fn cache_stream_setup_error_emits_one_error_summary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("cache-stream-setup-error")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new(
            "orders",
            table.path().to_string_lossy().to_string(),
        ))?;
        let dataframe = session.dataframe_for_lazy_table(&source).await?;
        let task_ctx = Arc::new(dataframe.task_ctx());
        let physical_plan = dataframe.create_physical_plan().await?;
        let failing_plan: Arc<dyn ExecutionPlan> =
            Arc::new(StreamSetupFailingPlan::new(physical_plan));
        let handles = collect_delta_provider_read_stats_handles(failing_plan.as_ref());
        assert_eq!(handles.len(), 1);

        let capture = TracingCapture::start();
        let result = start_cache_partition_streams(failing_plan, task_ctx, handles, "orders_cache");
        let summaries = provider_io_events(capture.captured());

        assert!(result.is_err());
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].target, "delta_funnel");
        assert_eq!(summaries[0].level, Level::DEBUG);
        assert_eq!(
            summaries[0].fields.get("source_name").map(String::as_str),
            Some("orders")
        );
        assert_eq!(
            summaries[0].fields.get("outcome").map(String::as_str),
            Some("error")
        );
        assert_eq!(
            summaries[0]
                .fields
                .get("metrics_available")
                .map(String::as_str),
            Some("true")
        );
        for field in [
            "parquet_data_file_range_get_operations",
            "parquet_data_file_full_get_operations",
            "parquet_data_file_bytes_received",
            "parquet_data_file_opened_bytes",
        ] {
            assert_eq!(
                summaries[0].fields.get(field).map(String::as_str),
                Some("0")
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn cache_partition_collection_uses_one_task_per_partition()
    -> Result<(), Box<dyn std::error::Error>> {
        let task_ids = Arc::new(Mutex::new(Vec::new()));
        let streams = (0..2)
            .map(|_| {
                let task_ids = Arc::clone(&task_ids);
                let stream = stream::once(async move {
                    task_ids
                        .lock()
                        .map_err(|_| DeltaFunnelError::Config {
                            message: "cache task id lock poisoned".to_owned(),
                        })?
                        .push(tokio::task::id());
                    Ok(RecordBatch::new_empty(Arc::new(Schema::empty())))
                });
                Box::pin(stream) as MssqlOutputBatchStream
            })
            .collect();
        let sampler = DeltaFileProgressSampler::new(
            Vec::new(),
            ProgressReporter::default(),
            ProgressPhase::MaterializingCache,
            None,
        );

        let partitions = collect_cache_partitions(streams, Some(sampler), "big").await?;

        assert_eq!(partitions.len(), 2);
        assert!(partitions.iter().all(|batches| batches.len() == 1));
        let task_ids = task_ids.lock().map_err(|_| "cache task id lock poisoned")?;
        assert_eq!(task_ids.len(), 2);
        assert_ne!(task_ids[0], task_ids[1]);
        Ok(())
    }

    #[tokio::test]
    async fn cache_partition_error_drains_live_sibling_before_returning()
    -> Result<(), Box<dyn std::error::Error>> {
        let sibling_started = Arc::new(Notify::new());
        let sibling_dropped = Arc::new(AtomicBool::new(false));

        let wait_for_sibling = Arc::clone(&sibling_started);
        let failing_stream = stream::once(async move {
            wait_for_sibling.notified().await;
            Err(DeltaFunnelError::Config {
                message: "injected cache partition failure".to_owned(),
            })
        });

        let announce_sibling = Arc::clone(&sibling_started);
        let drop_flag = DropFlag(Arc::clone(&sibling_dropped));
        let mut announced = false;
        let live_stream = stream::poll_fn(move |_| {
            let _ = &drop_flag;
            if !announced {
                announced = true;
                announce_sibling.notify_one();
            }
            Poll::<Option<Result<RecordBatch, DeltaFunnelError>>>::Pending
        });

        let streams: Vec<MssqlOutputBatchStream> =
            vec![Box::pin(failing_stream), Box::pin(live_stream)];
        let error = collect_cache_partitions(streams, None, "big")
            .await
            .err()
            .ok_or("expected cache partition failure")?;

        assert!(matches!(
            error,
            DeltaFunnelError::Config { message }
                if message == "injected cache partition failure"
        ));
        assert!(sibling_dropped.load(Ordering::SeqCst));
        Ok(())
    }

    #[tokio::test]
    async fn cache_partition_join_failure_drains_live_sibling_before_returning()
    -> Result<(), Box<dyn std::error::Error>> {
        let sibling_started = Arc::new(Notify::new());
        let sibling_dropped = Arc::new(AtomicBool::new(false));
        let mut tasks: JoinSet<CachePartitionTaskOutput> = JoinSet::new();

        let announce_sibling = Arc::clone(&sibling_started);
        let drop_flag = DropFlag(Arc::clone(&sibling_dropped));
        tasks.spawn(async move {
            let _drop_flag = drop_flag;
            announce_sibling.notify_one();
            std::future::pending::<CachePartitionTaskOutput>().await
        });
        sibling_started.notified().await;

        let cancelled = tasks.spawn(std::future::pending::<CachePartitionTaskOutput>());
        cancelled.abort();
        let (_progress_changed, progress_changes) = watch::channel(());
        let error = join_cache_partition_tasks(tasks, progress_changes, None, "big")
            .await
            .err()
            .ok_or("expected cache partition join failure")?;

        assert!(matches!(
            error,
            DeltaFunnelError::MssqlWorkflowPlanning { message }
                if message.contains("scoped MSSQL cache alias materialize failed")
        ));
        assert!(sibling_dropped.load(Ordering::SeqCst));
        Ok(())
    }

    fn panic_cache_partition_task() -> CachePartitionTaskOutput {
        resume_unwind(Box::new("injected cache partition panic"))
    }

    #[tokio::test]
    async fn cache_partition_task_panic_resumes_unwind() -> Result<(), Box<dyn std::error::Error>> {
        let mut tasks: JoinSet<CachePartitionTaskOutput> = JoinSet::new();
        tasks.spawn(async { panic_cache_partition_task() });
        let (_progress_changed, progress_changes) = watch::channel(());

        let result = tokio::spawn(async move {
            join_cache_partition_tasks(tasks, progress_changes, None, "big").await
        })
        .await;

        let error = result.err().ok_or("expected cache partition panic")?;
        assert!(error.is_panic());
        Ok(())
    }

    #[test]
    fn every_materialization_failure_has_exact_leaf_and_aggregate_statuses() {
        let expected_phase_names = [
            CACHE_ALIAS_DATAFRAME_RESOLUTION_PHASE,
            CACHE_ALIAS_PHYSICAL_PLANNING_PHASE,
            CACHE_ALIAS_STREAM_SETUP_PHASE,
            CACHE_ALIAS_EXECUTE_COLLECT_PHASE,
            CACHE_ALIAS_MEMTABLE_BUILD_PHASE,
            CACHE_ALIAS_MATERIALIZATION_TOTAL_PHASE,
            CACHE_ALIAS_INSTALL_PHASE,
            CACHE_ALIAS_RESTORE_PHASE,
        ];

        for (failed_phase_index, failed_phase) in CACHE_ALIAS_MATERIALIZATION_PHASES
            .iter()
            .copied()
            .enumerate()
        {
            let completed_timings = CACHE_ALIAS_MATERIALIZATION_PHASES
                .iter()
                .take(failed_phase_index)
                .map(|phase_name| PhaseTimingReport::completed(*phase_name, Duration::ZERO))
                .collect();
            let failure = cache_alias_materialization_failure(
                DeltaFunnelError::Config {
                    message: "injected cache materialization failure".to_owned(),
                },
                completed_timings,
                PhaseTimer::start(failed_phase),
                PhaseTimer::start(CACHE_ALIAS_MATERIALIZATION_TOTAL_PHASE),
            );

            assert_eq!(
                failure
                    .phase_timings
                    .iter()
                    .map(PhaseTimingReport::phase_name)
                    .collect::<Vec<_>>(),
                expected_phase_names
            );
            assert_eq!(failure.failed_phase, failed_phase);
            for (phase_index, timing) in failure.phase_timings[..5].iter().enumerate() {
                let expected_status = match phase_index.cmp(&failed_phase_index) {
                    std::cmp::Ordering::Less => PhaseStatus::completed(),
                    std::cmp::Ordering::Equal => PhaseStatus::failed(),
                    std::cmp::Ordering::Greater => {
                        PhaseStatus::not_started(ReportReasonCode::PriorFailure)
                    }
                };
                assert_eq!(timing.status(), expected_status);
            }
            assert_eq!(failure.phase_timings[5].status(), PhaseStatus::failed());
            assert!(failure.phase_timings[6..].iter().all(|timing| {
                timing.status() == PhaseStatus::not_started(ReportReasonCode::PriorFailure)
            }));
        }
    }

    #[tokio::test]
    async fn explicit_cache_matches_datafusion_default_with_and_without_progress()
    -> Result<(), Box<dyn std::error::Error>> {
        let session = DeltaFunnelSession::new(SessionOptions::default())?;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "marker",
            DataType::Utf8,
            false,
        )]));
        let source_partitions = vec![
            vec![
                marker_batch(&schema, vec!["partition-0-batch-0"])?,
                marker_batch(
                    &schema,
                    vec!["partition-0-batch-1-a", "partition-0-batch-1-b"],
                )?,
            ],
            vec![
                marker_batch(&schema, vec!["partition-1-batch-0"])?,
                marker_batch(&schema, vec!["partition-1-batch-1"])?,
            ],
        ];
        session.context().register_table(
            "cache_source",
            Arc::new(MemTable::try_new(Arc::clone(&schema), source_partitions)?),
        )?;

        let default_cached = session
            .context()
            .table("cache_source")
            .await?
            .cache()
            .await?;
        let default_task_ctx = Arc::new(default_cached.task_ctx());
        let default_plan = default_cached.create_physical_plan().await?;
        let default_schema = default_plan.schema();
        let default_partitions = collect_partitioned(default_plan, default_task_ctx).await?;

        let without_progress = session
            .materialize_cache("cache_source", None)
            .await
            .map_err(CacheAliasPhaseFailure::into_source)?;
        let reporter = ProgressReporter::default();
        let with_progress = session
            .materialize_cache("cache_source", Some(&reporter))
            .await
            .map_err(CacheAliasPhaseFailure::into_source)?;

        let expected_phase_names = [
            CACHE_ALIAS_DATAFRAME_RESOLUTION_PHASE,
            CACHE_ALIAS_PHYSICAL_PLANNING_PHASE,
            CACHE_ALIAS_STREAM_SETUP_PHASE,
            CACHE_ALIAS_EXECUTE_COLLECT_PHASE,
            CACHE_ALIAS_MEMTABLE_BUILD_PHASE,
            CACHE_ALIAS_MATERIALIZATION_TOTAL_PHASE,
        ];
        for materialized in [&without_progress, &with_progress] {
            assert_eq!(
                materialized
                    .phase_timings
                    .iter()
                    .map(PhaseTimingReport::phase_name)
                    .collect::<Vec<_>>(),
                expected_phase_names
            );
            assert!(materialized.phase_timings.iter().all(|timing| {
                timing.status() == PhaseStatus::completed() && timing.elapsed_micros().is_some()
            }));
        }

        assert_eq!(without_progress.provider.schema(), default_schema);
        assert_eq!(with_progress.provider.schema(), default_schema);
        let expected = marker_batches_by_partition(&default_partitions)?;
        assert_eq!(
            marker_batches_by_partition(&memtable_partitions(&without_progress.provider).await?)?,
            expected
        );
        assert_eq!(
            marker_batches_by_partition(&memtable_partitions(&with_progress.provider).await?)?,
            expected
        );
        Ok(())
    }

    #[tokio::test]
    async fn custom_cache_factory_fails_before_materialization()
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
        let factory_calls = Arc::new(AtomicUsize::new(0));
        session
            .context()
            .state_ref()
            .write()
            .set_cache_factory(Arc::new(RecordingCacheFactory {
                calls: Arc::clone(&factory_calls),
            }));

        let error = session
            .replace_registered_derived_alias_with_cache(&big, None)
            .await
            .err()
            .ok_or("expected custom cache factory compatibility error")?;

        assert!(matches!(
            error,
            DeltaFunnelError::MssqlWorkflowPlanning { message }
                if message == "`write_all` cache alias materialization requires DataFusion's default cache behavior; custom cache factories are not supported"
        ));
        assert_eq!(factory_calls.load(Ordering::SeqCst), 0);
        assert_eq!(source_scans.load(Ordering::SeqCst), 0);
        assert!(session.context().table("big").await.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn install_failure_reports_failed_install_and_completed_immediate_restore()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut session, schema) = session_with_fail_once_schema()?;
        let big = register_cache_alias(&mut session).await?;
        schema.fail_next_registration();

        let failure = session
            .replace_registered_derived_alias_with_cache_attempt(&big, vec![0, 1], None)
            .await
            .err()
            .ok_or("expected cache install failure")?;
        let CacheAliasReplacementFailure { source, report } = failure;
        let report = report.ok_or("expected attempted cache alias report")?;

        assert!(matches!(
            source,
            DeltaFunnelError::MssqlWorkflowPlanning { message }
                if message.contains("failed to register cached provider")
                    && message.contains("injected schema registration failure")
        ));
        assert_eq!(report.table_id(), big.id());
        assert_eq!(report.output_indexes(), &[0, 1]);
        assert_eq!(report.status(), WriteAllCacheAliasStatus::Failed);
        assert_eq!(report.failed_phase(), Some(CACHE_ALIAS_INSTALL_PHASE));
        assert_cache_phase_statuses(
            &report,
            [
                PhaseStatus::completed(),
                PhaseStatus::completed(),
                PhaseStatus::completed(),
                PhaseStatus::completed(),
                PhaseStatus::completed(),
                PhaseStatus::completed(),
                PhaseStatus::failed(),
                PhaseStatus::completed(),
            ],
        );
        assert!(session.context().table("big").await.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn cached_job_construction_failure_restores_every_installed_alias()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut session, _schema) = session_with_fail_once_schema()?;
        let (big_source, big_scans) = scan_counting_marker_region_provider("big")?;
        let (names_source, names_scans) = scan_counting_marker_region_provider("names")?;
        session.context().register_table("big_source", big_source)?;
        session
            .context()
            .register_table("names_source", names_source)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let pending_names = session
            .table_from_sql("select marker, region from names_source")
            .await?;
        let names = session.register_alias("names", &pending_names)?;
        let combined = session
            .table_from_sql(
                "select big.marker as big_marker, names.marker as name_marker \
                 from big join names on big.region = names.region",
            )
            .await?;
        let combined_id = combined.id();
        let output = execute_output_request(
            combined,
            "combined_output",
            "combined_orders",
            LoadMode::AppendExisting,
        )?;
        let planned = session.plan_write_all_outputs(std::slice::from_ref(&output))?;
        session
            .pending_derived_tables
            .retain(|pending| pending.table.id() != combined_id);
        let cache_aliases = [
            MssqlDerivedCacheAliasPlan::new(big.id(), "big".to_owned(), vec![0]),
            MssqlDerivedCacheAliasPlan::new(names.id(), "names".to_owned(), vec![0]),
        ];

        let error = session
            .write_all_cached_with_writer(
                &planned,
                &cache_aliases,
                MssqlStreamBenchmarkOutputWriter,
                None,
                None,
                ExecutionProfileMode::Disabled,
            )
            .await
            .err()
            .ok_or("expected cached job construction failure")?;
        let DeltaFunnelError::WriteAllCache { failure, source } = error else {
            return Err("expected structured write_all cache failure".into());
        };

        assert!(
            matches!(
                source.as_ref(),
                DeltaFunnelError::MssqlWorkflowPlanning { message }
                    if message.contains("is not registered in this session")
            ),
            "unexpected primary source: {source:?}"
        );
        assert_eq!(failure.primary_failed_alias_table_id(), None);
        assert_eq!(failure.workflow(), None);
        assert_eq!(failure.aliases().len(), 2);
        for (report, table_id) in failure.aliases().iter().zip([big.id(), names.id()]) {
            assert_eq!(report.table_id(), table_id);
            assert_eq!(
                report.status(),
                WriteAllCacheAliasStatus::MaterializedAndRestored
            );
            assert_cache_phase_statuses(report, [PhaseStatus::completed(); 8]);
        }
        assert_eq!(big_scans.load(Ordering::SeqCst), 1);
        assert_eq!(names_scans.load(Ordering::SeqCst), 1);

        session.context().table("big").await?.collect().await?;
        session.context().table("names").await?.collect().await?;
        assert_eq!(big_scans.load(Ordering::SeqCst), 2);
        assert_eq!(names_scans.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn cached_workflow_restore_failure_retains_completed_workflow()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut session, schema) = session_with_fail_once_schema()?;
        let big = register_cache_alias(&mut session).await?;
        let output = execute_output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let planned = session.plan_write_all_outputs(std::slice::from_ref(&output))?;
        let cache_aliases = [MssqlDerivedCacheAliasPlan::new(
            big.id(),
            "big".to_owned(),
            vec![0],
        )];
        let restore_schema = Arc::clone(&schema);
        let reporter = ProgressReporter::new(move |event| {
            if event.phase() == Some(ProgressPhase::RestoringCache) {
                restore_schema.fail_next_registration();
            }
        });

        let error = session
            .write_all_cached_with_writer(
                &planned,
                &cache_aliases,
                MssqlStreamBenchmarkOutputWriter,
                None,
                Some(&reporter),
                ExecutionProfileMode::Disabled,
            )
            .await
            .err()
            .ok_or("expected cache restore failure")?;
        let DeltaFunnelError::WriteAllCache { failure, source } = error else {
            return Err("expected structured write_all cache failure".into());
        };

        assert!(matches!(
            *source,
            DeltaFunnelError::MssqlWorkflowPlanning { message }
                if message.contains("restore_register")
                    && message.contains("injected schema registration failure")
        ));
        assert_eq!(failure.primary_failed_alias_table_id(), Some(big.id()));
        let workflow = failure.workflow().ok_or("expected completed workflow")?;
        assert_eq!(workflow.len(), 1);
        assert!(workflow.all_succeeded());
        assert_eq!(failure.aliases().len(), 1);
        assert_eq!(
            failure.aliases()[0].status(),
            WriteAllCacheAliasStatus::Failed
        );
        assert_eq!(
            failure.aliases()[0].failed_phase(),
            Some(CACHE_ALIAS_RESTORE_PHASE)
        );
        assert_cache_phase_statuses(
            &failure.aliases()[0],
            [
                PhaseStatus::completed(),
                PhaseStatus::completed(),
                PhaseStatus::completed(),
                PhaseStatus::completed(),
                PhaseStatus::completed(),
                PhaseStatus::completed(),
                PhaseStatus::completed(),
                PhaseStatus::failed(),
            ],
        );
        Ok(())
    }

    #[tokio::test]
    async fn primary_failure_survives_secondary_restore_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut session, schema) = session_with_fail_once_schema()?;
        let big = register_cache_alias(&mut session).await?;
        let replacement = session
            .replace_registered_derived_alias_with_cache(&big, None)
            .await?;
        schema.fail_next_registration();
        let primary_error = DeltaFunnelError::Config {
            message: "injected primary workflow failure".to_owned(),
        };

        let error =
            restore_cache_aliases_after_failure(primary_error, vec![replacement], None, None);
        let DeltaFunnelError::WriteAllCache { failure, source } = error else {
            return Err("expected structured write_all cache failure".into());
        };

        assert!(matches!(
            *source,
            DeltaFunnelError::Config { message }
                if message == "injected primary workflow failure"
        ));
        assert_eq!(failure.primary_failed_alias_table_id(), None);
        assert_eq!(failure.workflow(), None);
        assert_eq!(failure.aliases().len(), 1);
        assert_eq!(failure.aliases()[0].table_id(), big.id());
        assert_eq!(
            failure.aliases()[0].failed_phase(),
            Some(CACHE_ALIAS_RESTORE_PHASE)
        );
        assert_cache_phase_statuses(
            &failure.aliases()[0],
            [
                PhaseStatus::completed(),
                PhaseStatus::completed(),
                PhaseStatus::completed(),
                PhaseStatus::completed(),
                PhaseStatus::completed(),
                PhaseStatus::completed(),
                PhaseStatus::completed(),
                PhaseStatus::failed(),
            ],
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
            .replace_registered_derived_alias_with_cache(&big, None)
            .await?;

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

        replacement.restore()?;

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

        let failure = session.restore_original_after_cached_register_failure(
            "big",
            original_provider,
            "injected cached register failure",
        );

        assert!(matches!(
            &*failure.source,
            DeltaFunnelError::MssqlWorkflowPlanning { message }
                if message.contains("failed to register cached provider")
                    && message.contains("injected cached register failure")
        ));
        assert_eq!(
            failure.restore_timing.map(|timing| timing.status()),
            Some(PhaseStatus::completed())
        );

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
    async fn restore_after_error_preserves_error_and_restores_original()
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
            .replace_registered_derived_alias_with_cache(&big, None)
            .await?;
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let later_error = DeltaFunnelError::MssqlWorkflowPlanning {
            message: "simulated downstream planning failure".to_owned(),
        };
        let error = restore_cache_aliases_after_failure(later_error, vec![replacement], None, None);

        let DeltaFunnelError::WriteAllCache { failure, source } = error else {
            return Err("expected structured write_all cache failure".into());
        };
        assert_eq!(failure.aliases().len(), 1);
        assert_eq!(
            failure.aliases()[0].status(),
            WriteAllCacheAliasStatus::MaterializedAndRestored
        );
        assert_eq!(failure.primary_failed_alias_table_id(), None);
        assert_eq!(failure.workflow(), None);
        assert!(matches!(
            *source,
            DeltaFunnelError::MssqlWorkflowPlanning { message }
                if message.contains("simulated downstream planning failure")
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
            .replace_registered_derived_alias_with_cache(&big, None)
            .await?;
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let removed_cached = session.context().deregister_table("big")?;
        assert!(removed_cached.is_some());

        replacement.restore()?;

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
    fn write_all_cache_failure_preserves_the_primary_source()
    -> Result<(), Box<dyn std::error::Error>> {
        let primary_error = DeltaFunnelError::MssqlWorkflowPlanning {
            message: "simulated output workflow failure".to_owned(),
        };
        let error = write_all_cache_failure(primary_error, Vec::new(), None, None);

        let DeltaFunnelError::WriteAllCache { failure, source } = error else {
            return Err("expected structured write_all cache failure".into());
        };
        assert!(failure.aliases().is_empty());
        assert_eq!(failure.primary_failed_alias_table_id(), None);
        assert_eq!(failure.workflow(), None);
        assert!(matches!(
            *source,
            DeltaFunnelError::MssqlWorkflowPlanning { message }
                if message.contains("simulated output workflow failure")
        ));
        Ok(())
    }
}
