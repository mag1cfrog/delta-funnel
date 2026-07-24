//! DataFusion table provider for one Delta source.

use std::any::Any;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::{
    Column, Result as DataFusionResult,
    tree_node::{Transformed, TransformedResult, TreeNode},
};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use snafu::ResultExt;

use crate::{
    DeltaFunnelError, DeltaProtocolReport, PlannedDeltaSource, ProtocolPreflight,
    error::{DeltaScanConstructionSnafu, DeltaScanFilterSnafu},
    table_formats::{
        DeltaKernelPredicate, ProjectedDeltaScan, build_projected_predicated_delta_scan,
        build_projected_predicated_stats_delta_scan, delta_source_arrow_schema,
    },
};

use super::super::execution::{
    DeltaNativeAsyncFileReaderConfig, DeltaProviderReaderBackend,
    DeltaProviderScanExecutionOptions, DeltaScanPlanningExec, validate_native_async_reader_config,
};
use super::super::planning::filters::{DeltaFilterPushdownOutcome, DeltaFilterPushdownPlan};
use super::super::planning::partition_target::{
    DeltaScanPartitionTargetConfig, DeltaScanPartitionTargetContext, DeltaScanPartitionTargetPolicy,
};
use super::super::planning::projection::{ProjectionPlan, plan_projection};
use super::super::planning::scan_plan::{
    ProviderScanPlan, ProviderScanPlanParts, ProviderScanPlanRequest,
};
use super::super::profile_query_planning_sync_result;
use super::registration::reject_mismatched_preflight;

pub(crate) struct DeltaTableProvider {
    source: PlannedDeltaSource,
    protocol: DeltaProtocolReport,
    schema: SchemaRef,
    scan_target_partitions: Option<usize>,
    execution_options: DeltaProviderScanExecutionOptions,
}

impl DeltaTableProvider {
    #[allow(dead_code)]
    pub(crate) fn try_new(
        source: PlannedDeltaSource,
        preflight: ProtocolPreflight,
    ) -> Result<Self, DeltaFunnelError> {
        Self::try_new_with_scan_target_partitions(source, preflight, None)
    }

    #[allow(dead_code)]
    pub(crate) fn try_new_with_scan_target_partitions(
        source: PlannedDeltaSource,
        preflight: ProtocolPreflight,
        scan_target_partitions: Option<usize>,
    ) -> Result<Self, DeltaFunnelError> {
        let execution_options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::OfficialKernel,
            1,
            1,
        )?;
        Self::try_new_with_execution_options(
            source,
            preflight,
            scan_target_partitions,
            execution_options,
        )
    }

    #[allow(dead_code)]
    pub(crate) fn try_new_with_execution_options(
        source: PlannedDeltaSource,
        preflight: ProtocolPreflight,
        scan_target_partitions: Option<usize>,
        execution_options: DeltaProviderScanExecutionOptions,
    ) -> Result<Self, DeltaFunnelError> {
        reject_mismatched_preflight(&source, preflight.protocol())?;
        execution_options.validate()?;
        reject_unsupported_reader_backend_source(&execution_options, &source)?;
        let schema = delta_source_arrow_schema(&source)?;

        Ok(Self {
            source,
            protocol: preflight.into_protocol(),
            schema,
            scan_target_partitions,
            execution_options,
        })
    }

    pub(crate) fn source_name(&self) -> &str {
        self.source.name()
    }

    pub(crate) fn snapshot_version(&self) -> u64 {
        self.source.version()
    }

    pub(crate) fn protocol(&self) -> &DeltaProtocolReport {
        &self.protocol
    }

    pub(crate) fn source_table_uri(&self) -> &str {
        self.source.table_uri()
    }

    fn partition_columns(&self) -> HashSet<String> {
        self.source
            .loaded_snapshot()
            .kernel_snapshot()
            .table_configuration()
            .metadata()
            .partition_columns()
            .iter()
            .cloned()
            .collect()
    }

    #[allow(dead_code)]
    pub(crate) fn plan_supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DeltaFilterPushdownPlan {
        let filters =
            self.normalize_provider_filters(filters.iter().map(|filter| (*filter).clone()));

        self.plan_normalized_provider_filters(&filters)
    }

    #[allow(dead_code)]
    /// Builds the complete provider scan plan for DataFusion's scan callback.
    ///
    /// Filter planning and scan planning intentionally stay separate here. Exact
    /// partition filters are converted into a kernel predicate and passed into
    /// the official delta_kernel scan path.
    pub(crate) fn plan_scan(
        &self,
        request: ProviderScanPlanRequest,
    ) -> Result<ProviderScanPlan, DeltaFunnelError> {
        let ProviderScanPlanRequest {
            requested_projection,
            pushed_filters,
        } = request;
        let ProjectionPlan {
            projected_schema,
            scan_projection,
            projected_column_names,
        } = profile_query_planning_sync_result(
            "Delta projection planning",
            "delta_projection_planning",
            || self.plan_projection(requested_projection),
        )?;
        let (
            pushed_filter_plan,
            partition_columns,
            kernel_partition_predicate,
            kernel_projected_column_names,
            provider_enforced_row_predicate,
        ) = profile_query_planning_sync_result(
            "Delta filter planning",
            "delta_filter_planning",
            || {
                let normalized_pushed_filters = self.normalize_provider_filters(pushed_filters);
                let pushed_filter_plan =
                    self.plan_normalized_provider_filters(&normalized_pushed_filters);
                let mut partition_columns =
                    self.partition_columns().into_iter().collect::<Vec<_>>();
                partition_columns.sort();
                self.reject_unaccepted_pushed_filters(&pushed_filter_plan)?;
                self.reject_projected_inexact_pushed_filters_without_residual_columns(
                    &scan_projection,
                    &pushed_filter_plan,
                )?;
                let kernel_partition_predicate =
                    self.build_kernel_partition_predicate(&pushed_filter_plan)?;
                let kernel_projected_column_names = Self::kernel_projected_column_names(
                    projected_column_names,
                    &pushed_filter_plan,
                );
                let provider_enforced_row_predicate = self.build_provider_enforced_row_predicate(
                    &pushed_filter_plan,
                    kernel_projected_column_names.as_deref(),
                )?;

                Ok::<_, DeltaFunnelError>((
                    pushed_filter_plan,
                    partition_columns,
                    kernel_partition_predicate,
                    kernel_projected_column_names,
                    provider_enforced_row_predicate,
                ))
            },
        )?;
        let kernel_scan = self.build_kernel_scan(
            kernel_projected_column_names.as_deref(),
            kernel_partition_predicate.clone(),
            pushed_filter_plan.has_data_stats_filter(),
        )?;

        Ok(ProviderScanPlan::from_parts(ProviderScanPlanParts {
            source_name: self.source_name().to_owned(),
            table_uri: self.source.table_uri().to_owned(),
            storage_options: self.source.storage_options().clone(),
            snapshot_version: self.snapshot_version(),
            projected_schema,
            protocol: self.protocol.clone(),
            scan_projection,
            pushed_filter_plan,
            partition_columns,
            kernel_partition_predicate,
            provider_enforced_row_predicate,
            kernel_scan,
        }))
    }

    /// Applies safe name normalization to provider-boundary filters.
    ///
    /// DataFusion may present relation-qualified expressions to the support
    /// callback and unqualified expressions to `scan`. This helper owns the
    /// normalization step for both entry points before strict partition
    /// pushdown planning or kernel predicate conversion.
    fn normalize_provider_filters(&self, filters: impl IntoIterator<Item = Expr>) -> Vec<Expr> {
        filters
            .into_iter()
            .map(|filter| unqualify_filter_columns(filter, &self.schema))
            .collect()
    }

    /// Plans provider-boundary filters after normalization has been applied.
    ///
    /// Keeping this as a separate step lets scan planning reuse the same
    /// normalized expressions when converting accepted filters to kernel
    /// predicates, so filter classification and kernel conversion cannot see
    /// different column names.
    fn plan_normalized_provider_filters(&self, filters: &[Expr]) -> DeltaFilterPushdownPlan {
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &self.schema,
            &self.partition_columns(),
        );

        match self.execution_options.reader_backend {
            DeltaProviderReaderBackend::OfficialKernel => plan,
            DeltaProviderReaderBackend::NativeAsync => plan.with_native_row_predicate_exactness(),
        }
    }

    /// Rejects pushed filters that this provider cannot safely use.
    ///
    /// Exact and inexact filters must have a kernel scan filter expression that
    /// can be converted to a kernel predicate. Unsupported filters cannot safely
    /// affect scan planning.
    fn reject_unaccepted_pushed_filters(
        &self,
        pushed_filter_plan: &DeltaFilterPushdownPlan,
    ) -> Result<(), DeltaFunnelError> {
        let missing_partition_expression = pushed_filter_plan.decisions.iter().any(|decision| {
            decision.outcome != DeltaFilterPushdownOutcome::Unsupported
                && decision.kernel_scan_filter.is_none()
        });
        if pushed_filter_plan.unsupported_count > 0 || missing_partition_expression {
            return DeltaScanFilterSnafu {
                source_name: self.source_name().to_owned(),
                table_uri: self.source.table_uri().to_owned(),
                reason: "pushed filters must be exact partition predicates or safely inexact mixed AND predicates".to_owned(),
            }
            .fail();
        }

        Ok(())
    }

    fn reject_projected_inexact_pushed_filters_without_residual_columns(
        &self,
        scan_projection: &Option<Vec<usize>>,
        pushed_filter_plan: &DeltaFilterPushdownPlan,
    ) -> Result<(), DeltaFunnelError> {
        let Some(scan_projection) = scan_projection else {
            return Ok(());
        };

        let projected_columns = scan_projection
            .iter()
            .map(|index| self.schema.field(*index).name().as_str())
            .collect::<HashSet<_>>();

        let missing_residual_column = pushed_filter_plan
            .decisions
            .iter()
            .filter(|decision| decision.outcome == DeltaFilterPushdownOutcome::Inexact)
            .flat_map(|decision| decision.filter_analysis.referenced_columns.iter())
            .any(|column| !projected_columns.contains(column.as_str()));

        if missing_residual_column {
            return DeltaScanFilterSnafu {
                source_name: self.source_name().to_owned(),
                table_uri: self.source.table_uri().to_owned(),
                reason: "inexact pushed filter residual columns must be projected".to_owned(),
            }
            .fail();
        }

        Ok(())
    }

    /// Builds the kernel predicate for accepted exact and inexact filters.
    ///
    /// Accepted filters must be enforced by the same predicate passed into
    /// `ScanBuilder::with_predicate`.
    fn build_kernel_partition_predicate(
        &self,
        pushed_filter_plan: &DeltaFilterPushdownPlan,
    ) -> Result<Option<DeltaKernelPredicate>, DeltaFunnelError> {
        let predicates = pushed_filter_plan
            .decisions
            .iter()
            .filter_map(|decision| decision.kernel_scan_filter.as_ref())
            .map(|kernel_scan_filter| kernel_scan_filter.kernel_predicate.clone())
            .collect::<Vec<_>>();

        Ok(DeltaKernelPredicate::and_from(predicates))
    }

    fn build_provider_enforced_row_predicate(
        &self,
        pushed_filter_plan: &DeltaFilterPushdownPlan,
        projected_column_names: Option<&[String]>,
    ) -> Result<Option<DeltaKernelPredicate>, DeltaFunnelError> {
        if !pushed_filter_plan.has_provider_enforced_row_predicate() {
            return Ok(None);
        }

        let Some(predicate) = pushed_filter_plan.provider_enforced_row_predicate() else {
            return Ok(None);
        };

        let physical_predicate = self
            .build_kernel_scan(projected_column_names, Some(predicate), false)?
            .physical_predicate();
        if physical_predicate.is_none() {
            return DeltaScanFilterSnafu {
                source_name: self.source_name().to_owned(),
                table_uri: self.source.table_uri().to_owned(),
                reason: "exact native row predicate did not produce a physical predicate"
                    .to_owned(),
            }
            .fail();
        }

        Ok(physical_predicate)
    }

    /// Expands the kernel scan schema with accepted predicate columns.
    ///
    /// DataFusion output projection stays governed by `projected_schema` and
    /// `scan_projection`; this only gives delta_kernel enough schema context to
    /// validate and evaluate metadata predicates during scan planning.
    fn kernel_projected_column_names(
        projected_column_names: Option<Vec<String>>,
        pushed_filter_plan: &DeltaFilterPushdownPlan,
    ) -> Option<Vec<String>> {
        let mut projected_column_names = projected_column_names?;

        for decision in &pushed_filter_plan.decisions {
            if decision.kernel_scan_filter.is_none() {
                continue;
            }

            let columns = decision
                .filter_analysis
                .partition_columns
                .iter()
                .chain(decision.filter_analysis.data_columns.iter());

            for column in columns {
                if !projected_column_names.contains(column) {
                    projected_column_names.push(column.clone());
                }
            }
        }

        Some(projected_column_names)
    }

    /// Builds the delta_kernel scan state for a projected provider scan.
    fn build_kernel_scan(
        &self,
        projected_column_names: Option<&[String]>,
        kernel_partition_predicate: Option<DeltaKernelPredicate>,
        include_stats_columns: bool,
    ) -> Result<ProjectedDeltaScan, DeltaFunnelError> {
        profile_query_planning_sync_result(
            "Delta Kernel scan construction",
            "delta_kernel_scan_construction",
            || {
                let kernel_projected_column_names =
                    projected_column_names.map(|names| names.to_vec());

                let result = if include_stats_columns {
                    build_projected_predicated_stats_delta_scan(
                        &self.source,
                        kernel_projected_column_names.as_deref(),
                        kernel_partition_predicate,
                    )
                } else {
                    build_projected_predicated_delta_scan(
                        &self.source,
                        kernel_projected_column_names.as_deref(),
                        kernel_partition_predicate,
                    )
                };

                result.context(DeltaScanConstructionSnafu {
                    source_name: self.source_name().to_owned(),
                    table_uri: self.source.table_uri().to_owned(),
                })
            },
        )
    }

    #[allow(dead_code)]
    fn plan_projection(
        &self,
        projection: Option<Vec<usize>>,
    ) -> Result<ProjectionPlan, DeltaFunnelError> {
        plan_projection(
            self.source_name(),
            self.source.table_uri(),
            &self.schema,
            projection,
        )
    }

    #[cfg(test)]
    pub(crate) fn set_schema_for_tests(&mut self, schema: SchemaRef) {
        self.schema = schema;
    }
}

/// Removes relation qualifiers from provider support-check filters.
///
/// DataFusion's physical planner strips qualifiers before passing filters into
/// `scan`. The support callback receives the logical filter earlier, while it
/// may still contain references like `orders.region`. Normalizing here keeps
/// `supports_filters_pushdown` and `scan` aligned without relaxing the lower
/// level Delta kernel adapter for direct qualified-column inputs.
///
/// Nested-field style references are deliberately preserved. For example,
/// `profile.age` must not become `age` just because DataFusion stores it as a
/// column with a relation component.
fn unqualify_filter_columns(filter: Expr, schema: &SchemaRef) -> Expr {
    let original_filter = filter.clone();
    match filter
        .transform(|expr| {
            if let Expr::Column(column) = expr {
                if is_relation_qualified_top_level_column(&column, schema) {
                    Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                        column.name,
                    ))))
                } else {
                    Ok(Transformed::no(Expr::Column(column)))
                }
            } else {
                Ok(Transformed::no(expr))
            }
        })
        .data()
    {
        Ok(filter) => filter,
        Err(_error) => original_filter,
    }
}

fn reject_unsupported_reader_backend_source(
    execution_options: &DeltaProviderScanExecutionOptions,
    source: &PlannedDeltaSource,
) -> Result<(), DeltaFunnelError> {
    match execution_options.reader_backend {
        DeltaProviderReaderBackend::OfficialKernel => Ok(()),
        DeltaProviderReaderBackend::NativeAsync => {
            validate_native_async_reader_config(DeltaNativeAsyncFileReaderConfig {
                source_name: source.name(),
                table_uri: source.table_uri(),
                snapshot_version: source.version(),
                storage_options: source.storage_options(),
                engine_context: Arc::clone(source.engine_context()),
            })
        }
    }
}

fn is_relation_qualified_top_level_column(column: &Column, schema: &SchemaRef) -> bool {
    let flat_name = column.flat_name();
    let Some((first_segment, _remainder)) = flat_name.split_once('.') else {
        return false;
    };

    schema.field_with_name(&column.name).is_ok() && schema.field_with_name(first_segment).is_err()
}

impl fmt::Debug for DeltaTableProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeltaTableProvider")
            .field("source_name", &self.source_name())
            .field("snapshot_version", &self.snapshot_version())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for DeltaTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        profile_query_planning_sync_result("Delta scan planning", "delta_scan_planning", || {
            // SQL LIMIT is enforced by DataFusion above the provider scan. Treat the
            // provider limit as advisory until a scan-local limit case is proven
            // safe across residual filters, joins, deletion vectors, transforms, and
            // ordering-sensitive plans.
            let scan_plan = self.plan_scan(ProviderScanPlanRequest {
                requested_projection: projection.cloned(),
                pushed_filters: filters.to_vec(),
            })?;
            let partition_target_decision = profile_query_planning_sync_result(
                "Delta partition target selection",
                "delta_partition_target_selection",
                || {
                    DeltaScanPartitionTargetPolicy::default().derive_target(
                        DeltaScanPartitionTargetContext {
                            source_name: &scan_plan.source_name,
                            table_uri: &scan_plan.table_uri,
                            snapshot_version: scan_plan.snapshot_version,
                        },
                        DeltaScanPartitionTargetConfig::from_scan_targets(
                            state.config().target_partitions(),
                            self.scan_target_partitions,
                        ),
                    )
                },
            )?;
            let partition_plan = scan_plan.plan_file_task_partitions(
                partition_target_decision.file_task_partition_options(),
            )?;
            let execution_options = profile_query_planning_sync_result(
                "Delta scan execution setup",
                "delta_scan_execution_setup",
                || {
                    self.execution_options
                        .with_default_scan_wide_capacity_for_target_partitions(
                            partition_target_decision.target_partitions,
                        )
                },
            )?;

            Ok(Arc::new(DeltaScanPlanningExec::new(
                scan_plan,
                partition_plan,
                partition_target_decision,
                execution_options,
            ))
                as Arc<dyn datafusion::physical_plan::ExecutionPlan>)
        })
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(self
            .plan_supports_filters_pushdown(filters)
            .datafusion_pushdowns())
    }
}

#[cfg(test)]
#[path = "provider_tests.rs"]
mod tests;
