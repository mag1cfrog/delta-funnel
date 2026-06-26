use std::sync::Arc;

use datafusion::{arrow::datatypes::SchemaRef, prelude::SessionContext};
use futures_util::StreamExt;

use crate::{
    DeltaFunnelError, MssqlOutputBatchStream, MssqlOutputBatchStreamFactory,
    datafusion_query_output_stream,
};

use super::super::super::{
    DeltaFunnelSession, LazyTableKind, OutputWritePlan,
    errors::{cached_output_stream_setup_error, unknown_cached_alias_error},
    query_handoff::{ProviderStatsRecordingStream, SharedProviderReadStats},
    registry::{DerivedTableDependency, read_only_sql_options},
};
use super::{MssqlCachedOutputStreamRoute, MssqlDerivedCacheAliasPlan};

pub(super) fn failing_cached_output_batch_stream_factory(
    output_name: String,
    error: DeltaFunnelError,
) -> MssqlOutputBatchStreamFactory {
    Box::new(move || {
        Box::pin(async move { Err(cached_output_stream_setup_error(&output_name, error)) })
    })
}

pub(super) async fn replanned_sql_batch_stream(
    context: SessionContext,
    output_name: String,
    sql_text: String,
    expected_schema: SchemaRef,
    provider_stats: Option<SharedProviderReadStats>,
) -> Result<MssqlOutputBatchStream, DeltaFunnelError> {
    let dataframe = context
        .sql_with_options(sql_text.as_str(), read_only_sql_options())
        .await
        .map_err(|error| cached_output_stream_setup_error(&output_name, error))?;
    validate_replanned_output_schema(
        &output_name,
        dataframe.schema().as_arrow(),
        &expected_schema,
    )?;
    let physical_plan = dataframe
        .create_physical_plan()
        .await
        .map_err(|error| cached_output_stream_setup_error(&output_name, error))?;
    let stream = datafusion_query_output_stream(Arc::clone(&physical_plan), context.task_ctx())
        .map_err(|error| cached_output_stream_setup_error(&output_name, error))?;
    if let Some(provider_stats) = provider_stats {
        let error_output_name = output_name.clone();
        return Ok(Box::pin(ProviderStatsRecordingStream::new(
            Box::pin(stream.map(move |batch| {
                batch.map_err(|error| cached_output_stream_setup_error(&error_output_name, error))
            })),
            physical_plan,
            provider_stats,
        )));
    }

    Ok(Box::pin(stream.map(move |batch| {
        batch.map_err(|error| cached_output_stream_setup_error(&output_name, error))
    })))
}

impl DeltaFunnelSession {
    /// Classifies one selected output relative to active cached aliases.
    ///
    /// Direct selected-alias use wins over lineage use because the normal
    /// registered-alias stream path should read the active cached provider.
    /// Dependent outputs are identified from captured lineage so later stream
    /// construction can replan from retained SQL while all cache aliases are
    /// installed.
    #[allow(dead_code)]
    pub(crate) fn cached_output_stream_route(
        &self,
        request: &OutputWritePlan,
        active_aliases: &[MssqlDerivedCacheAliasPlan],
    ) -> Result<MssqlCachedOutputStreamRoute, DeltaFunnelError> {
        if active_aliases.is_empty() {
            return Ok(MssqlCachedOutputStreamRoute::UncachedLazyTable);
        }

        for alias in active_aliases {
            self.registered_derived_table_by_id(alias.table_id())
                .ok_or_else(|| unknown_cached_alias_error(alias))?;
        }

        if let Some(alias) = active_aliases
            .iter()
            .find(|alias| request.table().id() == alias.table_id())
        {
            return Ok(MssqlCachedOutputStreamRoute::DirectCachedAlias(
                alias.clone(),
            ));
        }

        if request.table().kind() == LazyTableKind::DeltaSource {
            return Ok(MssqlCachedOutputStreamRoute::UncachedLazyTable);
        }

        let dependencies = self.transitive_registered_derived_dependencies(request.table())?;
        let dependent_aliases = active_aliases
            .iter()
            .filter(|alias| {
                dependencies.iter().any(|dependency| {
                    matches!(
                        dependency,
                        DerivedTableDependency::RegisteredDerived { table_id, .. }
                            if *table_id == alias.table_id()
                    )
                })
            })
            .cloned()
            .collect::<Vec<_>>();

        if dependent_aliases.is_empty() {
            Ok(MssqlCachedOutputStreamRoute::UncachedLazyTable)
        } else {
            Ok(MssqlCachedOutputStreamRoute::ReplannedCachedDependency(
                dependent_aliases,
            ))
        }
    }

    /// Builds an async stream factory for one output while cache aliases are active.
    ///
    /// The returned factory performs DataFusion stream setup when the workflow
    /// attempts this output. Direct cached aliases and unrelated outputs reuse
    /// the normal lazy-table stream path. Dependent outputs replan from the
    /// retained SQL text so active scoped cache aliases can replace the
    /// registered providers referenced by that SQL.
    #[allow(dead_code)]
    pub(crate) fn cached_output_batch_stream_factory(
        &self,
        request: &OutputWritePlan,
        active_aliases: &[MssqlDerivedCacheAliasPlan],
    ) -> Result<MssqlOutputBatchStreamFactory, DeltaFunnelError> {
        self.cached_output_batch_stream_factory_with_provider_stats(request, active_aliases, None)
    }

    pub(crate) fn cached_output_batch_stream_factory_with_provider_stats(
        &self,
        request: &OutputWritePlan,
        active_aliases: &[MssqlDerivedCacheAliasPlan],
        provider_stats: Option<SharedProviderReadStats>,
    ) -> Result<MssqlOutputBatchStreamFactory, DeltaFunnelError> {
        let route = self.cached_output_stream_route(request, active_aliases)?;
        match route {
            MssqlCachedOutputStreamRoute::DirectCachedAlias(_)
            | MssqlCachedOutputStreamRoute::UncachedLazyTable => Ok(self
                .lazy_table_batch_stream_factory_with_provider_stats(
                    request.table().clone(),
                    provider_stats,
                )),
            MssqlCachedOutputStreamRoute::ReplannedCachedDependency(_) => {
                let output_name = request.target().output_name().to_owned();
                let sql_text = match self.sql_text_for_derived_table(request.table()) {
                    Ok(sql_text) => sql_text.to_owned(),
                    Err(error) => {
                        return Ok(failing_cached_output_batch_stream_factory(
                            output_name,
                            error,
                        ));
                    }
                };
                let expected_schema = match self.schema_for_lazy_table(request.table()) {
                    Ok(schema) => Arc::clone(schema),
                    Err(error) => {
                        return Ok(failing_cached_output_batch_stream_factory(
                            output_name,
                            error,
                        ));
                    }
                };
                let context = self.context.clone();
                Ok(Box::new(move || {
                    Box::pin(async move {
                        replanned_sql_batch_stream(
                            context,
                            output_name,
                            sql_text,
                            expected_schema,
                            provider_stats,
                        )
                        .await
                    })
                }))
            }
        }
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
            scan_counting_marker_region_provider,
        },
    };
    use super::super::{
        MssqlCachedOutputStreamRoute, MssqlDerivedCacheAliasPlan, MssqlOutputCacheDecision,
    };

    #[tokio::test]
    async fn cached_output_stream_route_classifies_direct_dependent_and_unrelated_outputs()
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

        assert_eq!(
            session.cached_output_stream_route(&big_output, caches)?,
            MssqlCachedOutputStreamRoute::DirectCachedAlias(caches[0].clone())
        );
        assert_eq!(
            session.cached_output_stream_route(&west_output, caches)?,
            MssqlCachedOutputStreamRoute::ReplannedCachedDependency(vec![caches[0].clone()])
        );
        assert_eq!(
            session.cached_output_stream_route(&unrelated_output, caches)?,
            MssqlCachedOutputStreamRoute::UncachedLazyTable
        );
        Ok(())
    }

    #[tokio::test]
    async fn cached_output_stream_route_keeps_multiple_active_dependency_aliases()
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
        assert_eq!(
            session.cached_output_stream_route(&west_output, caches)?,
            MssqlCachedOutputStreamRoute::ReplannedCachedDependency(caches.clone())
        );
        Ok(())
    }

    #[test]
    fn cached_output_stream_route_rejects_unknown_active_alias() -> Result<(), DeltaFunnelError> {
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

        let error = session.cached_output_stream_route(&output, &aliases);

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
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
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
            .replace_registered_derived_alias_with_cache(&big)
            .await?;
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let factory = session.cached_output_batch_stream_factory(&big_output, caches)?;
        let markers = collect_stream_marker_values(factory().await?).await?;

        assert_eq!(markers, vec!["shared", "shared"]);
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);
        let _restoration = replacement.restore().await?;
        Ok(())
    }

    #[tokio::test]
    async fn cached_output_stream_factory_unrelated_output_uses_existing_lazy_table_path()
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
            .replace_registered_derived_alias_with_cache(&big)
            .await?;
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let factory = session.cached_output_batch_stream_factory(&unrelated_output, caches)?;
        let markers = collect_stream_marker_values(factory().await?).await?;

        assert_eq!(markers, vec!["unrelated"]);
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);
        let _restoration = replacement.restore().await?;
        Ok(())
    }

    #[tokio::test]
    async fn cached_output_stream_factory_replans_dependent_outputs_against_active_cache()
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
            .replace_registered_derived_alias_with_cache(&big)
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
        let _restoration = replacement.restore().await?;
        Ok(())
    }

    #[tokio::test]
    async fn cached_output_stream_factory_replans_dependent_output_against_multiple_active_caches()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
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
            .replace_registered_derived_alias_with_cache(&big)
            .await?;
        let names_replacement = session
            .replace_registered_derived_alias_with_cache(&names)
            .await?;
        assert_eq!(big_source_scans.load(Ordering::SeqCst), 1);
        assert_eq!(names_source_scans.load(Ordering::SeqCst), 1);

        let factory = session.cached_output_batch_stream_factory(&west_output, caches)?;
        let markers = collect_stream_marker_values(factory().await?).await?;

        assert_eq!(markers, vec!["big"]);
        assert_eq!(big_source_scans.load(Ordering::SeqCst), 1);
        assert_eq!(names_source_scans.load(Ordering::SeqCst), 1);
        let _names_restoration = names_replacement.restore().await?;
        let _big_restoration = big_replacement.restore().await?;
        Ok(())
    }

    #[tokio::test]
    async fn cached_output_stream_factory_rejects_replanned_schema_mismatch()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
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
            .replace_registered_derived_alias_with_cache(&big)
            .await?;

        let factory = session.cached_output_batch_stream_factory(&west_output, caches)?;
        let error = factory().await;

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("cached output stream setup failed for `west_output`")
                    && message.contains("replanned output schema does not match")
        ));
        let _restoration = replacement.restore().await?;
        Ok(())
    }

    #[tokio::test]
    async fn cached_output_stream_factory_returns_async_error_for_unreplayable_sql()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
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
            .replace_registered_derived_alias_with_cache(&big)
            .await?;

        let factory = session.cached_output_batch_stream_factory(&west_output, caches)?;
        let error = factory().await;

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("cached output stream setup failed for `west_output`")
        ));
        let _restoration = replacement.restore().await?;
        Ok(())
    }
}
