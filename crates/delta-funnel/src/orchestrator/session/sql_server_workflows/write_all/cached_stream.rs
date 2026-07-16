use std::sync::Arc;

use datafusion::{arrow::datatypes::SchemaRef, prelude::SessionContext};

#[cfg(test)]
use crate::MssqlOutputBatchStreamFactory;
use crate::{
    DeltaFunnelError, ExecutionProfileMode, MssqlOutputQueryError, MssqlOutputQueryExecution,
    MssqlOutputQueryFuture, MssqlWritePhase,
    progress::ProgressReporter,
    report::{OperationTimelineRecorder, PhaseTimer},
};

use super::super::super::{
    DeltaFunnelSession, LazyTableKind, OutputWritePlan, PlannedMssqlOutput,
    errors::{cached_output_stream_setup_error, unknown_cached_alias_error},
    query_handoff::SharedProviderStatsSnapshots,
    registry::{DerivedTableDependency, read_only_sql_options},
};
use super::super::output::{
    QUERY_DATAFRAME_PLANNING_PHASE,
    create_mssql_output_query_execution_from_dataframe_with_timeline, mssql_output_query_error,
};
use super::MssqlDerivedCacheAliasPlan;

fn failing_cached_output_query_factory(
    output_name: String,
    error: DeltaFunnelError,
) -> Box<dyn FnOnce() -> MssqlOutputQueryFuture + Send> {
    Box::new(move || {
        Box::pin(async move {
            Err(MssqlOutputQueryError {
                error: cached_output_stream_setup_error(&output_name, error),
                query_phase_timings: Vec::new(),
            })
        })
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "cached query setup carries retained SQL, reporting, and profiling state"
)]
async fn create_cached_output_query_execution_from_retained_sql(
    context: SessionContext,
    planned: PlannedMssqlOutput,
    sql_text: String,
    expected_schema: SchemaRef,
    provider_stats_snapshots: Option<SharedProviderStatsSnapshots>,
    reporter: Option<ProgressReporter>,
    profile_mode: ExecutionProfileMode,
    timeline: Option<OperationTimelineRecorder>,
) -> Result<MssqlOutputQueryExecution, MssqlOutputQueryError> {
    let output_name = planned.resolved_target().output_name();
    let dataframe_timer = PhaseTimer::start(QUERY_DATAFRAME_PLANNING_PHASE);
    let dataframe_span = timeline.as_ref().map(|timeline| {
        timeline
            .start_span(
                "Build query DataFrame",
                "delta_funnel.write.query",
                "Query DataFrame planning",
            )
            .with_attribute("output_name", output_name.to_owned().into())
    });
    let dataframe = match context
        .sql_with_options(sql_text.as_str(), read_only_sql_options())
        .await
    {
        Ok(dataframe) => dataframe,
        Err(error) => {
            if let Some(span) = dataframe_span {
                span.failed();
            }
            return Err(mssql_output_query_error(
                &planned,
                MssqlWritePhase::QueryDataFramePlanning,
                Vec::new(),
                dataframe_timer,
                cached_output_stream_setup_error(output_name, error),
                None,
            ));
        }
    };
    if let Err(error) = validate_replanned_output_schema(
        output_name,
        dataframe.schema().as_arrow(),
        &expected_schema,
    ) {
        if let Some(span) = dataframe_span {
            span.failed();
        }
        return Err(mssql_output_query_error(
            &planned,
            MssqlWritePhase::QueryDataFramePlanning,
            Vec::new(),
            dataframe_timer,
            error,
            None,
        ));
    }
    if let Some(span) = dataframe_span {
        span.completed();
    }
    let query_phase_timings = vec![dataframe_timer.completed()];

    create_mssql_output_query_execution_from_dataframe_with_timeline(
        &context,
        &planned,
        dataframe,
        query_phase_timings,
        provider_stats_snapshots,
        reporter,
        profile_mode,
        timeline.as_ref(),
    )
    .await
}

impl DeltaFunnelSession {
    /// Returns whether one selected output must be replanned against active caches.
    ///
    /// Direct selected-alias use wins over lineage use because the normal
    /// registered-alias stream path should read the active cached provider.
    /// Other dependent outputs are identified from captured lineage and must
    /// be replanned from retained SQL while all cache aliases are installed.
    fn output_requires_cache_replan(
        &self,
        request: &OutputWritePlan,
        active_aliases: &[MssqlDerivedCacheAliasPlan],
    ) -> Result<bool, DeltaFunnelError> {
        if active_aliases.is_empty() {
            return Ok(false);
        }

        for alias in active_aliases {
            self.registered_derived_table_by_id(alias.table_id())
                .ok_or_else(|| unknown_cached_alias_error(alias))?;
        }

        if active_aliases
            .iter()
            .any(|alias| request.table().id() == alias.table_id())
        {
            return Ok(false);
        }

        if request.table().kind() == LazyTableKind::DeltaSource {
            return Ok(false);
        }

        let dependencies = self.transitive_registered_derived_dependencies(request.table())?;
        Ok(active_aliases.iter().any(|alias| {
            dependencies.iter().any(|dependency| {
                matches!(
                    dependency,
                    DerivedTableDependency::RegisteredDerived { table_id, .. }
                        if *table_id == alias.table_id()
                )
            })
        }))
    }

    /// Builds a deferred query factory for one output while cache aliases are active.
    ///
    /// The returned factory performs DataFusion query planning and stream setup
    /// when the workflow attempts this output. Direct cached aliases and
    /// unrelated outputs reuse the normal lazy-table query path. Dependent
    /// outputs replan from the retained SQL text so active scoped cache aliases
    /// can replace the registered providers referenced by that SQL.
    /// Optional provider stats, progress, and profiling are attached without
    /// changing how the cache route is selected.
    #[cfg(test)]
    pub(crate) fn cached_output_query_factory(
        &self,
        planned: &PlannedMssqlOutput,
        active_aliases: &[MssqlDerivedCacheAliasPlan],
        provider_stats_snapshots: Option<SharedProviderStatsSnapshots>,
        reporter: Option<ProgressReporter>,
        profile_mode: ExecutionProfileMode,
    ) -> Result<Box<dyn FnOnce() -> MssqlOutputQueryFuture + Send>, DeltaFunnelError> {
        self.cached_output_query_factory_with_timeline(
            planned,
            active_aliases,
            provider_stats_snapshots,
            reporter,
            profile_mode,
            None,
        )
    }

    pub(super) fn cached_output_query_factory_with_timeline(
        &self,
        planned: &PlannedMssqlOutput,
        active_aliases: &[MssqlDerivedCacheAliasPlan],
        provider_stats_snapshots: Option<SharedProviderStatsSnapshots>,
        reporter: Option<ProgressReporter>,
        profile_mode: ExecutionProfileMode,
        timeline: Option<OperationTimelineRecorder>,
    ) -> Result<Box<dyn FnOnce() -> MssqlOutputQueryFuture + Send>, DeltaFunnelError> {
        let request = planned.request();
        if !self.output_requires_cache_replan(request, active_aliases)? {
            return Ok(self.mssql_output_query_factory_with_timeline(
                planned.clone(),
                provider_stats_snapshots,
                reporter,
                profile_mode,
                timeline,
            ));
        }

        let output_name = request.target().output_name().to_owned();
        let sql_text = match self.sql_text_for_derived_table(request.table()) {
            Ok(sql_text) => sql_text.to_owned(),
            Err(error) => {
                return Ok(failing_cached_output_query_factory(output_name, error));
            }
        };
        let expected_schema = match self.schema_for_lazy_table(request.table()) {
            Ok(schema) => Arc::clone(schema),
            Err(error) => {
                return Ok(failing_cached_output_query_factory(output_name, error));
            }
        };
        let context = self.context.clone();
        let planned = planned.clone();
        Ok(Box::new(move || {
            Box::pin(async move {
                create_cached_output_query_execution_from_retained_sql(
                    context,
                    planned,
                    sql_text,
                    expected_schema,
                    provider_stats_snapshots,
                    reporter,
                    profile_mode,
                    timeline,
                )
                .await
            })
        }))
    }

    #[cfg(test)]
    fn cached_output_batch_stream_factory(
        &self,
        request: &OutputWritePlan,
        active_aliases: &[MssqlDerivedCacheAliasPlan],
    ) -> Result<MssqlOutputBatchStreamFactory, DeltaFunnelError> {
        let planned = self.plan_mssql_output(request)?;
        let create_query_execution = self.cached_output_query_factory(
            &planned,
            active_aliases,
            None,
            None,
            ExecutionProfileMode::Disabled,
        )?;
        Ok(Box::new(move || {
            Box::pin(async move {
                create_query_execution()
                    .await
                    .map(|execution| execution.stream)
                    .map_err(|failure| failure.error)
            })
        }))
    }
}

fn validate_replanned_output_schema(
    output_name: &str,
    replanned_schema: &datafusion::arrow::datatypes::Schema,
    expected_schema: &SchemaRef,
) -> Result<(), DeltaFunnelError> {
    if replanned_schema == expected_schema.as_ref() {
        return Ok(());
    }

    Err(cached_output_stream_setup_error(
        output_name,
        "replanned output schema does not match the original output schema",
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, atomic::Ordering};

    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    use crate::{DeltaFunnelError, DeltaSourceConfig, LoadMode};

    use super::super::super::super::{
        DeltaFunnelSession, LazyTable, LazyTableKind, SessionOptions,
        test_support::{
            DeltaLogTable, collect_stream_marker_values, output_request,
            scan_counting_marker_region_provider, secret_connection,
        },
    };
    use super::super::{MssqlDerivedCacheAliasPlan, MssqlOutputCacheDecision};

    #[tokio::test]
    async fn output_requires_cache_replan_classifies_direct_dependent_and_unrelated_outputs()
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
        let unrelated = session
            .table_from_sql("select customer_name from orders")
            .await?;
        let big_output = output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let west_output =
            output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let unrelated_output = output_request(
            unrelated,
            "unrelated_output",
            "unrelated_orders",
            LoadMode::AppendExisting,
        )?;
        let plan = session.plan_mssql_output_cache(&[big_output.clone(), west_output.clone()]);
        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };

        assert!(!session.output_requires_cache_replan(&big_output, caches)?);
        assert!(session.output_requires_cache_replan(&west_output, caches)?);
        assert!(!session.output_requires_cache_replan(&unrelated_output, caches)?);
        Ok(())
    }

    #[test]
    fn output_requires_cache_replan_rejects_unknown_active_alias() -> Result<(), DeltaFunnelError> {
        let session = DeltaFunnelSession::new(SessionOptions::default())?;
        let output = output_request(
            LazyTable::placeholder(7, LazyTableKind::DerivedSql),
            "west_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let aliases = vec![MssqlDerivedCacheAliasPlan::new(
            252,
            "missing_cache".to_owned(),
            vec![0],
        )];

        let error = session.output_requires_cache_replan(&output, &aliases);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("missing_cache")
                    && message.contains("not registered in this session")
        ));
        Ok(())
    }

    #[tokio::test]
    async fn cached_output_stream_factory_direct_alias_reads_active_cache()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let big_output = output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let west_output =
            output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let plan = session.plan_mssql_output_cache(&[big_output.clone(), west_output]);
        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };
        let replacement = session
            .replace_registered_derived_alias_with_cache(&big, None)
            .await?;
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let factory = session.cached_output_batch_stream_factory(&big_output, caches)?;
        let markers = collect_stream_marker_values(factory().await?).await?;

        assert_eq!(markers, vec!["shared", "shared"]);
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);
        replacement.restore()?;
        Ok(())
    }

    #[tokio::test]
    async fn cached_output_stream_factory_unrelated_output_uses_existing_lazy_table_path()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let unrelated = session
            .table_from_sql("select 'unrelated' as marker, 'north' as region")
            .await?;
        let big_output = output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let west_output =
            output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let unrelated_output = output_request(
            unrelated,
            "unrelated_output",
            "unrelated_orders",
            LoadMode::AppendExisting,
        )?;
        let plan = session.plan_mssql_output_cache(&[big_output, west_output]);
        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };
        let replacement = session
            .replace_registered_derived_alias_with_cache(&big, None)
            .await?;
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let factory = session.cached_output_batch_stream_factory(&unrelated_output, caches)?;
        let markers = collect_stream_marker_values(factory().await?).await?;

        assert_eq!(markers, vec!["unrelated"]);
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);
        replacement.restore()?;
        Ok(())
    }

    #[tokio::test]
    async fn cached_output_stream_factory_replans_dependent_outputs_against_active_cache()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let east = session
            .table_from_sql("select marker from big where region = 'east'")
            .await?;
        let big_output = output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let west_output = output_request(
            west.clone(),
            "west_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let east_output = output_request(
            east.clone(),
            "east_output",
            "east_orders",
            LoadMode::AppendExisting,
        )?;
        let plan = session.plan_mssql_output_cache(&[
            big_output.clone(),
            west_output.clone(),
            east_output.clone(),
        ]);
        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };
        let replacement = session
            .replace_registered_derived_alias_with_cache(&big, None)
            .await?;
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let big_factory = session.cached_output_batch_stream_factory(&big_output, caches)?;
        let west_factory = session.cached_output_batch_stream_factory(&west_output, caches)?;
        let east_factory = session.cached_output_batch_stream_factory(&east_output, caches)?;
        let big_markers = collect_stream_marker_values(big_factory().await?).await?;
        let west_markers = collect_stream_marker_values(west_factory().await?).await?;
        let east_markers = collect_stream_marker_values(east_factory().await?).await?;

        assert_eq!(big_markers, vec!["shared", "shared"]);
        assert_eq!(west_markers, vec!["shared"]);
        assert_eq!(east_markers, vec!["shared"]);
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);
        replacement.restore()?;
        Ok(())
    }

    #[tokio::test]
    async fn cached_output_stream_factory_replans_dependent_output_against_multiple_active_caches()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (big_source_provider, big_source_scans) = scan_counting_marker_region_provider("big")?;
        let (names_source_provider, names_source_scans) =
            scan_counting_marker_region_provider("name")?;
        session
            .context()
            .register_table("big_source", big_source_provider)?;
        session
            .context()
            .register_table("names_source", names_source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let pending_names = session
            .table_from_sql("select marker, region from names_source")
            .await?;
        let names = session.register_alias("names", &pending_names)?;
        let west = session
            .table_from_sql(
                "select big.marker from big join names on big.region = names.region where big.region = 'west' and names.marker = 'name'",
            )
            .await?;
        let east = session
            .table_from_sql(
                "select big.marker from big join names on big.region = names.region where big.region = 'east' and names.marker = 'name'",
            )
            .await?;
        let west_output = output_request(
            west.clone(),
            "west_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let east_output =
            output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;
        let plan = session.plan_mssql_output_cache(&[west_output.clone(), east_output]);
        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };
        assert_eq!(caches.len(), 2);
        assert_eq!(caches[0].table_id(), big.id());
        assert_eq!(caches[1].table_id(), names.id());
        let big_replacement = session
            .replace_registered_derived_alias_with_cache(&big, None)
            .await?;
        let names_replacement = session
            .replace_registered_derived_alias_with_cache(&names, None)
            .await?;
        assert_eq!(big_source_scans.load(Ordering::SeqCst), 1);
        assert_eq!(names_source_scans.load(Ordering::SeqCst), 1);

        let factory = session.cached_output_batch_stream_factory(&west_output, caches)?;
        let markers = collect_stream_marker_values(factory().await?).await?;

        assert_eq!(markers, vec!["big"]);
        assert_eq!(big_source_scans.load(Ordering::SeqCst), 1);
        assert_eq!(names_source_scans.load(Ordering::SeqCst), 1);
        names_replacement.restore()?;
        big_replacement.restore()?;
        Ok(())
    }

    #[tokio::test]
    async fn cached_output_stream_factory_rejects_replanned_schema_mismatch()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, _source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let big_output = output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let west_output = output_request(
            west.clone(),
            "west_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let plan = session.plan_mssql_output_cache(&[big_output, west_output.clone()]);
        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };
        let pending_west = session
            .pending_derived_tables
            .iter_mut()
            .find(|pending| pending.table.id() == west.id())
            .ok_or("expected pending west table")?;
        pending_west.schema = Arc::new(Schema::new(vec![Field::new(
            "different_marker",
            DataType::Utf8,
            false,
        )]));
        let replacement = session
            .replace_registered_derived_alias_with_cache(&big, None)
            .await?;

        let factory = session.cached_output_batch_stream_factory(&west_output, caches)?;
        let error = factory().await;

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlQueryPhase { source, .. })
                if matches!(
                    &*source,
                    DeltaFunnelError::MssqlWorkflowPlanning { message }
                        if message.contains("cached output stream setup failed for `west_output`")
                            && message.contains("replanned output schema does not match")
                )
        ));
        replacement.restore()?;
        Ok(())
    }

    #[tokio::test]
    async fn cached_output_stream_factory_returns_async_error_for_unreplayable_sql()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, _source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let big_output = output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let west_output = output_request(
            west.clone(),
            "west_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let plan = session.plan_mssql_output_cache(&[big_output, west_output.clone()]);
        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };
        let pending_west = session
            .pending_derived_tables
            .iter_mut()
            .find(|pending| pending.table.id() == west.id())
            .ok_or("expected pending west table")?;
        pending_west.sql_text.clear();
        let replacement = session
            .replace_registered_derived_alias_with_cache(&big, None)
            .await?;

        let factory = session.cached_output_batch_stream_factory(&west_output, caches)?;
        let error = factory().await;

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlQueryPhase { source, .. })
                if matches!(
                    &*source,
                    DeltaFunnelError::MssqlWorkflowPlanning { message }
                        if message.contains("cached output stream setup failed for `west_output`")
                )
        ));
        replacement.restore()?;
        Ok(())
    }
}
