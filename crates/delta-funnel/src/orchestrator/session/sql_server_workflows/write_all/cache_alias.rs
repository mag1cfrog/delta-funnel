use std::{fmt, panic::resume_unwind, sync::Arc};

use datafusion::{
    datasource::{MemTable, TableProvider},
    physical_plan::execute_stream_partitioned,
    prelude::{DataFrame, SessionContext},
};
use futures_util::{StreamExt, TryStreamExt};
use tokio::{sync::watch, task::JoinSet};

use crate::{
    DeltaFunnelError, MssqlOutputBatchStream,
    observability::DeltaProviderScanOutcome,
    progress::{ProgressEvent, ProgressPhase, ProgressReporter},
    query_engine::datafusion::collect_delta_provider_read_stats_handles,
    support::sanitize_text_for_display,
};

use super::super::super::{
    DeltaFunnelSession, LazyTable,
    errors::{mssql_scoped_cache_alias_error, unknown_cached_alias_error},
    query_handoff::{
        DeltaFileProgressSampler, finalize_provider_scan_execution,
        track_partitioned_scan_completion,
    },
};
use super::MssqlDerivedCacheAliasPlan;

/// Active replacement of one registered derived alias with a cached provider.
///
/// The original provider is owned by this scope until `restore` is called.
/// Callers must not rely on `Drop` for restoration.
pub(crate) struct MssqlScopedCacheAliasReplacement<'a> {
    context: &'a SessionContext,
    alias_name: String,
    original_provider: Arc<dyn TableProvider>,
}

impl<'a> MssqlScopedCacheAliasReplacement<'a> {
    pub(super) fn new(
        context: &'a SessionContext,
        alias_name: String,
        original_provider: Arc<dyn TableProvider>,
    ) -> Self {
        Self {
            context,
            alias_name,
            original_provider,
        }
    }

    /// Restores the original provider under the alias and consumes the scope.
    ///
    /// This method transitions the catalog from "alias points at cached
    /// provider" back to "alias points at the original provider".
    ///
    /// Callers should use this method on both success and error paths that
    /// leave the scoped replacement active.
    pub(crate) fn restore(self) -> Result<(), DeltaFunnelError> {
        self.context
            .deregister_table(self.alias_name.as_str())
            .map_err(|error| {
                mssql_scoped_cache_alias_error("restore_deregister", &self.alias_name, error)
            })?;

        self.context
            .register_table(self.alias_name.as_str(), self.original_provider)
            .map_err(|error| {
                mssql_scoped_cache_alias_error("restore_register", &self.alias_name, error)
            })?;

        Ok(())
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
    pub(crate) async fn replace_registered_derived_alias_with_cache(
        &self,
        table: &LazyTable,
        reporter: Option<&ProgressReporter>,
    ) -> Result<MssqlScopedCacheAliasReplacement<'_>, DeltaFunnelError> {
        let registered = self.registered_derived_for_scoped_cache_alias(table)?;
        let alias_name = registered.name().to_owned();

        if let Some(reporter) = reporter {
            reporter.emit(&ProgressEvent::phase_changed(
                ProgressPhase::MaterializingCache,
                None,
            ));
        }
        let dataframe = self
            .context
            .table(alias_name.as_str())
            .await
            .map_err(|error| {
                mssql_scoped_cache_alias_error("resolve", alias_name.as_str(), error)
            })?;
        let cached_provider = self
            .materialize_cache(dataframe, alias_name.as_str(), reporter)
            .await?;

        let original_provider =
            self.install_scoped_cache_alias_provider(alias_name.as_str(), cached_provider)?;

        Ok(MssqlScopedCacheAliasReplacement::new(
            &self.context,
            alias_name,
            original_provider,
        ))
    }

    pub(super) async fn replace_mssql_cache_aliases(
        &self,
        cache_aliases: &[MssqlDerivedCacheAliasPlan],
        reporter: Option<&ProgressReporter>,
    ) -> Result<Vec<MssqlScopedCacheAliasReplacement<'_>>, DeltaFunnelError> {
        let mut replacements = Vec::new();

        for cache_alias in cache_aliases {
            let table = self
                .registered_derived_table_by_id(cache_alias.table_id())
                .ok_or_else(|| unknown_cached_alias_error(cache_alias))?
                .table()
                .clone();

            match self
                .replace_registered_derived_alias_with_cache(&table, reporter)
                .await
            {
                Ok(replacement) => replacements.push(replacement),
                Err(error) => {
                    return Err(restore_mssql_cache_aliases_after_error(
                        error,
                        replacements,
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
        dataframe: DataFrame,
        alias_name: &str,
        reporter: Option<&ProgressReporter>,
    ) -> Result<Arc<dyn TableProvider>, DeltaFunnelError> {
        let task_ctx = Arc::new(dataframe.task_ctx());
        let physical_plan = dataframe
            .create_physical_plan()
            .await
            .map_err(|error| mssql_scoped_cache_alias_error("materialize", alias_name, error))?;
        let schema = physical_plan.schema();
        let read_stats_handles = collect_delta_provider_read_stats_handles(physical_plan.as_ref());
        let sampler = reporter.map(|reporter| {
            DeltaFileProgressSampler::new(
                read_stats_handles.clone(),
                reporter.clone(),
                ProgressPhase::MaterializingCache,
                None,
            )
        });
        let streams = match execute_stream_partitioned(physical_plan, task_ctx) {
            Ok(streams) => streams,
            Err(error) => {
                finalize_provider_scan_execution(
                    &read_stats_handles,
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
            .collect::<Vec<_>>();
        let streams = track_partitioned_scan_completion(streams, read_stats_handles);
        let partitions = collect_cache_partitions(streams, sampler, alias_name).await?;
        let cached_provider = MemTable::try_new(schema, partitions)
            .map_err(|error| mssql_scoped_cache_alias_error("materialize", alias_name, error))?;
        Ok(Arc::new(cached_provider))
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
    ) -> Result<Arc<dyn TableProvider>, DeltaFunnelError> {
        let original_provider = self
            .context
            .deregister_table(alias_name)
            .map_err(|error| mssql_scoped_cache_alias_error("deregister", alias_name, error))?
            .ok_or_else(|| {
                mssql_scoped_cache_alias_error(
                    "deregister",
                    alias_name,
                    "registered alias was missing from the catalog",
                )
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
    /// failure and, if restoration also fails, includes that cleanup failure.
    pub(super) fn restore_original_after_cached_register_failure(
        &self,
        alias_name: &str,
        original_provider: Arc<dyn TableProvider>,
        register_error: impl fmt::Display,
    ) -> DeltaFunnelError {
        let restore_result = self.context.register_table(alias_name, original_provider);
        let message = match restore_result {
            Ok(_) => format!(
                "failed to register cached provider for alias `{}`: {}",
                sanitize_text_for_display(alias_name),
                sanitize_text_for_display(&register_error.to_string())
            ),
            Err(restore_error) => format!(
                "failed to register cached provider for alias `{}`: {}; also failed to restore original provider: {}",
                sanitize_text_for_display(alias_name),
                sanitize_text_for_display(&register_error.to_string()),
                sanitize_text_for_display(&restore_error.to_string())
            ),
        };

        DeltaFunnelError::MssqlWorkflowPlanning { message }
    }
}

/// Collects each cache partition in its own Tokio task and restores its order.
async fn collect_cache_partitions(
    streams: Vec<MssqlOutputBatchStream>,
    mut sampler: Option<DeltaFileProgressSampler>,
    alias_name: &str,
) -> Result<Vec<Vec<datafusion::arrow::record_batch::RecordBatch>>, DeltaFunnelError> {
    let mut tasks = JoinSet::new();
    // Partition tasks only announce stream-consumption boundaries. The parent
    // task samples counters and calls the reporter, so no callback needs a
    // cross-task delivery lock.
    let (progress_changed, mut progress_changes) = watch::channel(());
    for (partition_index, stream) in streams.into_iter().enumerate() {
        let progress_changed = sampler.is_some().then(|| progress_changed.clone());
        tasks.spawn(async move {
            let batches = match progress_changed {
                Some(progress_changed) => {
                    stream
                        .inspect(move |_| {
                            let _ = progress_changed.send(());
                        })
                        .try_collect::<Vec<_>>()
                        .await
                }
                None => stream.try_collect::<Vec<_>>().await,
            };
            (partition_index, batches)
        });
    }
    drop(progress_changed);

    let mut partitions = Vec::new();
    while !tasks.is_empty() {
        tokio::select! {
            Ok(()) = progress_changes.changed(), if sampler.is_some() => {
                emit_cache_progress(&mut sampler);
            }
            Some(result) = tasks.join_next() => {
                match result {
                    Ok((partition_index, batches)) => {
                        emit_cache_progress(&mut sampler);
                        partitions.push((partition_index, batches?));
                    }
                    Err(error) if error.is_panic() => resume_unwind(error.into_panic()),
                    Err(error) => {
                        emit_cache_progress(&mut sampler);
                        return Err(mssql_scoped_cache_alias_error(
                            "materialize",
                            alias_name,
                            error,
                        ));
                    }
                }
            }
        }
    }
    partitions.sort_by_key(|(partition_index, _)| *partition_index);
    Ok(partitions.into_iter().map(|(_, batches)| batches).collect())
}

fn emit_cache_progress(sampler: &mut Option<DeltaFileProgressSampler>) {
    if let Some(sampler) = sampler {
        sampler.emit_if_changed();
    }
}

pub(super) fn restore_mssql_cache_aliases(
    replacements: Vec<MssqlScopedCacheAliasReplacement<'_>>,
    reporter: Option<&ProgressReporter>,
) -> Result<(), DeltaFunnelError> {
    if !replacements.is_empty()
        && let Some(reporter) = reporter
    {
        reporter.emit(&ProgressEvent::phase_changed(
            ProgressPhase::RestoringCache,
            None,
        ));
    }
    let mut first_error = None;

    for replacement in replacements.into_iter().rev() {
        if let Err(error) = replacement.restore()
            && first_error.is_none()
        {
            first_error = Some(error);
        }
    }

    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

pub(super) fn restore_mssql_cache_aliases_after_error(
    error: DeltaFunnelError,
    replacements: Vec<MssqlScopedCacheAliasReplacement<'_>>,
    reporter: Option<&ProgressReporter>,
) -> DeltaFunnelError {
    match restore_mssql_cache_aliases(replacements, reporter) {
        Ok(()) => error,
        Err(restore_error) => cache_error_with_restore_error(error, restore_error),
    }
}

pub(super) fn cache_error_with_restore_error(
    error: DeltaFunnelError,
    restore_error: DeltaFunnelError,
) -> DeltaFunnelError {
    DeltaFunnelError::MssqlWorkflowPlanning {
        message: format!(
            "write_all auto cache failed: {}; also failed to restore cache aliases: {}",
            sanitize_text_for_display(&error.to_string()),
            sanitize_text_for_display(&restore_error.to_string())
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, atomic::Ordering};

    use datafusion::arrow::{datatypes::Schema, record_batch::RecordBatch};
    use futures_util::stream;

    use super::*;

    use crate::{
        DeltaFunnelError,
        orchestrator::session::{
            DeltaFunnelSession, SessionOptions,
            test_support::{marker_values_from_batches, scan_counting_marker_region_provider},
        },
    };

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
        let error = restore_mssql_cache_aliases_after_error(later_error, vec![replacement], None);

        assert!(matches!(
            error,
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
}
