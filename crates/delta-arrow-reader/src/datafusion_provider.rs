//! Optional DataFusion table-provider and registration surface.

use std::{collections::HashSet, fmt, sync::Arc};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::{DataFusionError, Result as DataFusionResult},
    datasource::{TableProvider, TableType},
    execution::context::SessionContext,
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
};

use crate::{
    DeltaReaderBackend, DeltaReaderError, DeltaReaderExecutionOptions, DeltaTable,
    datafusion_execution::create_datafusion_execution_plan,
    datafusion_planning::{
        DataFusionFilterCapabilities, plan_datafusion_filters, plan_datafusion_scan,
    },
    kernel::delta_predicate_to_kernel_pruning,
    planning::{
        DeltaScanPartitionTargetOptions, plan_row_predicate, plan_scan, validate_backend_available,
    },
};

const TRACING_TARGET: &str = "delta_arrow_reader::datafusion";

/// DataFusion-specific scan settings for one provider.
#[derive(Debug, Clone, Default)]
pub struct DeltaDataFusionScanOptions {
    /// Reader execution settings used by each provider scan.
    pub execution_options: DeltaReaderExecutionOptions,
    /// Optional explicit scan partition target.
    pub target_partitions: Option<usize>,
}

/// Immutable DataFusion provider for one loaded Delta table snapshot.
///
/// ```no_run
/// use std::sync::Arc;
/// use datafusion::prelude::SessionContext;
/// use delta_arrow_reader::{
///     DeltaDataFusionScanOptions, DeltaTableBuilder, DeltaTableProvider,
/// };
///
/// # fn build_provider() -> Result<(), Box<dyn std::error::Error>> {
/// let table = DeltaTableBuilder::new("/tmp/example-delta-table").load()?;
/// let provider = DeltaTableProvider::try_new(
///     table,
///     DeltaDataFusionScanOptions::default(),
/// )?;
/// SessionContext::new().register_table("orders", Arc::new(provider))?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct DeltaTableProvider {
    table: DeltaTable,
    options: DeltaDataFusionScanOptions,
    source_name: Option<String>,
}

impl DeltaTableProvider {
    /// Creates a provider after validating its options and table protocol.
    pub fn try_new(
        table: DeltaTable,
        options: DeltaDataFusionScanOptions,
    ) -> Result<Self, DeltaReaderError> {
        Self::try_new_with_source_name(table, options, None)
    }

    fn try_new_with_source_name(
        table: DeltaTable,
        options: DeltaDataFusionScanOptions,
        source_name: Option<String>,
    ) -> Result<Self, DeltaReaderError> {
        options.execution_options.validate()?;
        validate_backend_available(options.execution_options)?;
        if options.target_partitions == Some(0) {
            return Err(DeltaReaderError::InvalidConfiguration {
                reason: "scan_partition_target_must_be_positive",
            });
        }
        table.validate_protocol()?;
        Ok(Self {
            table,
            options,
            source_name,
        })
    }

    fn plan(
        &self,
        state: &dyn Session,
        projection: Option<&[usize]>,
        filters: &[Expr],
    ) -> Result<(Arc<dyn ExecutionPlan>, usize), DeltaReaderError> {
        let partition_columns = self
            .table
            .partition_columns()
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let planning = plan_datafusion_scan(
            self.table.schema(),
            &partition_columns,
            projection,
            &filter_refs,
            DataFusionFilterCapabilities {
                exact_predicate_evaluation: self.options.execution_options.reader_backend()
                    == DeltaReaderBackend::NativeAsync,
            },
        )?;
        if planning
            .filters
            .decisions
            .iter()
            .any(|decision| decision.pushdown == TableProviderFilterPushDown::Unsupported)
        {
            return Err(DeltaReaderError::UnsupportedPredicate {
                reason: "datafusion_scan_contains_unsupported_filter",
            });
        }
        let physical_projection = planning.projection.physical_projection.clone();
        let hidden_columns = planning.projection.hidden_columns.clone();
        let kernel_predicate = planning
            .filters
            .predicate
            .as_ref()
            .map(|predicate| {
                delta_predicate_to_kernel_pruning(predicate).ok_or(
                    DeltaReaderError::UnsupportedPredicate {
                        reason: "datafusion_predicate_not_kernel_safe",
                    },
                )
            })
            .transpose()?;
        let row_predicate = planning
            .filters
            .row_predicate
            .as_ref()
            .map(|predicate| {
                delta_predicate_to_kernel_pruning(predicate).ok_or(
                    DeltaReaderError::UnsupportedPredicate {
                        reason: "exact_row_predicate_not_kernel_safe",
                    },
                )
            })
            .transpose()?;
        let row_predicate = plan_row_predicate(
            self.table.snapshot(),
            physical_projection.as_deref(),
            &hidden_columns,
            row_predicate,
        )?;
        let core = plan_scan(
            self.table.snapshot(),
            physical_projection.as_deref(),
            &hidden_columns,
            kernel_predicate,
            planning.filters.requires_statistics,
            self.options.execution_options,
            DeltaScanPartitionTargetOptions {
                explicit_target_partitions: self.options.target_partitions,
                caller_target_partitions: Some(state.config().target_partitions()),
            },
        )?;
        let partition_count = core.partitions.len();
        Ok((
            create_datafusion_execution_plan(
                core,
                planning,
                row_predicate,
                self.source_name.clone(),
            ),
            partition_count,
        ))
    }
}

impl fmt::Debug for DeltaTableProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeltaTableProvider")
            .field("snapshot_version", &self.table.version())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for DeltaTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(self.table.schema())
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
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        match self.plan(state, projection.map(Vec::as_slice), filters) {
            Ok((plan, partition_count)) => {
                tracing::debug!(
                    target: TRACING_TARGET,
                    event = "provider_scan.planned",
                    snapshot_version = self.table.version(),
                    partition_count,
                    backend = ?self.options.execution_options.reader_backend(),
                    outcome = "planned"
                );
                Ok(plan)
            }
            Err(error) => {
                trace_failure(
                    "provider_scan.failed",
                    self.table.version(),
                    self.options.execution_options.reader_backend(),
                    &error,
                );
                Err(DataFusionError::External(Box::new(error)))
            }
        }
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        let partition_columns = self
            .table
            .partition_columns()
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let planning = plan_datafusion_filters(
            self.table.schema(),
            &partition_columns,
            filters,
            DataFusionFilterCapabilities {
                exact_predicate_evaluation: self.options.execution_options.reader_backend()
                    == DeltaReaderBackend::NativeAsync,
            },
        );
        Ok(planning
            .decisions
            .iter()
            .map(|decision| decision.pushdown.clone())
            .collect())
    }
}

/// Result of registering one loaded Delta table in a DataFusion context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredDeltaTable {
    /// Caller-supplied DataFusion table name.
    pub name: String,
    /// Loaded Delta snapshot version.
    pub version: u64,
}

/// Registers one loaded Delta table in a DataFusion session.
///
/// Registration performs no scan. Existing registrations are preserved and
/// reported through [`DeltaReaderError`].
///
/// ```no_run
/// use datafusion::prelude::SessionContext;
/// use delta_arrow_reader::{
///     DeltaDataFusionScanOptions, DeltaTableBuilder, register_delta_table,
/// };
///
/// # fn register() -> Result<(), Box<dyn std::error::Error>> {
/// let context = SessionContext::new();
/// let table = DeltaTableBuilder::new("/tmp/example-delta-table").load()?;
/// let registered = register_delta_table(
///     &context,
///     "orders",
///     table,
///     DeltaDataFusionScanOptions::default(),
/// )?;
/// assert_eq!(registered.name, "orders");
/// # Ok(())
/// # }
/// ```
pub fn register_delta_table(
    context: &SessionContext,
    name: impl Into<String>,
    table: DeltaTable,
    options: DeltaDataFusionScanOptions,
) -> Result<RegisteredDeltaTable, DeltaReaderError> {
    let name = name.into();
    let version = table.version();
    let backend = options.execution_options.reader_backend();
    let result = (|| {
        validate_registration_name(&name)?;
        let provider =
            DeltaTableProvider::try_new_with_source_name(table, options, Some(name.clone()))?;
        context
            .register_table(name.as_str(), Arc::new(provider))
            .map_err(|source| DeltaReaderError::DataFusionAdapter {
                reason: "table_registration_failed",
                source: Box::new(source),
            })?;
        Ok(RegisteredDeltaTable { name, version })
    })();
    match result {
        Ok(registered) => {
            tracing::debug!(
                target: TRACING_TARGET,
                event = "provider_registration.registered",
                snapshot_version = version,
                partition_count = tracing::field::Empty,
                backend = ?backend,
                outcome = "registered"
            );
            Ok(registered)
        }
        Err(error) => {
            trace_failure("provider_registration.failed", version, backend, &error);
            Err(error)
        }
    }
}

fn validate_registration_name(name: &str) -> Result<(), DeltaReaderError> {
    let mut chars = name.chars();
    let valid = chars
        .next()
        .is_some_and(|first| first == '_' || first.is_ascii_alphabetic())
        && chars.all(|value| value == '_' || value.is_ascii_alphanumeric());
    if !valid || is_reserved_sql_keyword(name) {
        let reason = if name.is_empty() {
            "table_registration_name_empty"
        } else {
            "table_registration_name_invalid"
        };
        return Err(DeltaReaderError::DataFusionAdapter {
            reason,
            source: Box::new(DataFusionError::Plan(reason.to_owned())),
        });
    }
    Ok(())
}

fn is_reserved_sql_keyword(name: &str) -> bool {
    const KEYWORDS: &[&str] = &[
        "all",
        "alter",
        "analyze",
        "and",
        "anti",
        "as",
        "asof",
        "by",
        "case",
        "connect",
        "cross",
        "delete",
        "distinct",
        "distribute",
        "drop",
        "else",
        "end",
        "except",
        "exists",
        "explain",
        "false",
        "fetch",
        "for",
        "format",
        "from",
        "full",
        "global",
        "group",
        "having",
        "in",
        "inner",
        "insert",
        "intersect",
        "into",
        "is",
        "join",
        "lateral",
        "left",
        "like",
        "limit",
        "minus",
        "natural",
        "not",
        "null",
        "offset",
        "on",
        "open",
        "or",
        "order",
        "outer",
        "partition",
        "pivot",
        "prewhere",
        "qualify",
        "returning",
        "right",
        "sample",
        "select",
        "semi",
        "set",
        "settings",
        "sort",
        "start",
        "table",
        "tablesample",
        "then",
        "top",
        "true",
        "union",
        "unpivot",
        "update",
        "using",
        "values",
        "view",
        "when",
        "where",
        "window",
        "with",
    ];
    KEYWORDS
        .iter()
        .any(|keyword| name.eq_ignore_ascii_case(keyword))
}

fn trace_failure(
    event: &'static str,
    snapshot_version: u64,
    backend: DeltaReaderBackend,
    error: &DeltaReaderError,
) {
    tracing::debug!(
        target: TRACING_TARGET,
        event,
        snapshot_version,
        partition_count = tracing::field::Empty,
        backend = ?backend,
        outcome = "failed",
        error_variant = error.as_str(),
        error_phase = error.phase().as_str()
    );
}

#[cfg(test)]
mod tests {
    use super::validate_registration_name;

    #[test]
    fn registration_names_preserve_the_frozen_unquoted_identifier_boundary() {
        for name in ["orders", "_customers", "Regions_2026", "line_items"] {
            assert!(validate_registration_name(name).is_ok(), "{name}");
        }

        for name in [
            "",
            "2026_orders",
            "orders.latest",
            "line-items",
            "line items",
            "\"orders\"",
            "orders$",
            "ordérs",
            "select",
            "FROM",
            "Join",
            "where",
            "table",
        ] {
            assert!(validate_registration_name(name).is_err(), "{name}");
        }
    }
}
