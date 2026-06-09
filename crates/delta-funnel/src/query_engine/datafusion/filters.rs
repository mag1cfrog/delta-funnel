//! Filter pushdown planning for the Delta DataFusion provider.

mod analysis;
mod partition_pushdown;

use std::collections::HashSet;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};

use self::analysis::DeltaFilterAnalysis;
use crate::table_formats::DeltaKernelPredicate;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DeltaFilterPushdownOutcome {
    Exact,
    Inexact,
    Unsupported,
}

impl DeltaFilterPushdownOutcome {
    fn to_datafusion(self) -> TableProviderFilterPushDown {
        match self {
            Self::Exact => TableProviderFilterPushDown::Exact,
            Self::Inexact => TableProviderFilterPushDown::Inexact,
            Self::Unsupported => TableProviderFilterPushDown::Unsupported,
        }
    }
}

/// Conservative reason a provider-boundary filter was not accepted for pushdown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DeltaFilterPushdownRejectionReason {
    /// The expression uses known provider columns and an understood predicate
    /// shape, but current pushdown policy does not accept it as provider-enforced.
    UnsupportedByPolicy,
    /// The expression shape is outside the supported provider filter grammar.
    ExpressionShape,
    /// The expression references a provider-internal synthetic column.
    InternalColumn,
    /// The expression references at least one column that the provider schema
    /// cannot resolve as a top-level field.
    UnknownColumn,
}

impl DeltaFilterPushdownRejectionReason {
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn code(self) -> &'static str {
        match self {
            Self::UnsupportedByPolicy => "unsupported_by_policy",
            Self::ExpressionShape => "unsupported_expression_shape",
            Self::InternalColumn => "unsupported_internal_column",
            Self::UnknownColumn => "unsupported_unknown_column",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
/// Exact partition filter payload used by kernel scan planning.
pub(crate) struct ExactPartitionKernelFilter {
    /// DataFusion expression after any kernel-safe rewrites.
    pub(crate) datafusion_expr: Expr,
    /// Converted Delta kernel predicate for the same expression.
    pub(crate) kernel_predicate: DeltaKernelPredicate,
}

#[derive(Clone, Debug, PartialEq)]
/// Provider decision for one input filter, preserving input order.
pub(crate) struct DeltaFilterPushdownDecision {
    /// Position of this decision in the original DataFusion filter input slice.
    pub(crate) input_index: usize,
    /// Pushdown status reported back to DataFusion for this input filter.
    pub(crate) outcome: DeltaFilterPushdownOutcome,
    /// Whether DataFusion must still evaluate the original filter above the scan.
    pub(crate) residual: bool,
    /// Conservative rejection reason when the filter is unsupported.
    pub(crate) rejection_reason: Option<DeltaFilterPushdownRejectionReason>,
    /// Provider-boundary diagnostics and column classification for the original filter.
    pub(crate) filter_analysis: DeltaFilterAnalysis,
    /// Exact partition filter converted for Delta kernel scan planning.
    ///
    /// For most exact filters this is the original filter. Some exact filters
    /// use an equivalent expression, such as empty list predicates whose
    /// kernel-safe form must preserve null partition behavior.
    pub(crate) kernel_scan_filter: Option<ExactPartitionKernelFilter>,
}

#[derive(Clone, Debug, Default, PartialEq)]
/// Ordered provider pushdown plan for a batch of DataFusion input filters.
pub(crate) struct DeltaFilterPushdownPlan {
    /// One decision per input filter, preserving input order.
    pub(crate) decisions: Vec<DeltaFilterPushdownDecision>,
    /// Number of filters reported as exact.
    pub(crate) exact_count: usize,
    /// Number of filters reported as inexact.
    pub(crate) inexact_count: usize,
    /// Number of filters reported as unsupported.
    pub(crate) unsupported_count: usize,
    /// Number of filters accepted for provider-side work.
    pub(crate) pushed_filter_count: usize,
    /// Number of filters DataFusion must keep as residual filters.
    pub(crate) residual_filter_count: usize,
}

impl DeltaFilterPushdownPlan {
    #[must_use]
    /// Plans the exact static partition operator policy.
    ///
    /// The same policy is used by `supports_filters_pushdown` and by direct
    /// `scan` filter validation so the public support callback and scan
    /// boundary cannot drift apart.
    pub(crate) fn partition_operator_pushdown(
        filters: &[&Expr],
        schema: &SchemaRef,
        partition_columns: &HashSet<String>,
    ) -> Self {
        partition_pushdown::plan_partition_operator_pushdown(filters, schema, partition_columns)
    }

    fn from_decisions(decisions: Vec<DeltaFilterPushdownDecision>) -> Self {
        let exact_count = decisions
            .iter()
            .filter(|decision| decision.outcome == DeltaFilterPushdownOutcome::Exact)
            .count();
        let inexact_count = decisions
            .iter()
            .filter(|decision| decision.outcome == DeltaFilterPushdownOutcome::Inexact)
            .count();
        let unsupported_count = decisions
            .iter()
            .filter(|decision| decision.outcome == DeltaFilterPushdownOutcome::Unsupported)
            .count();
        let residual_filter_count = decisions
            .iter()
            .filter(|decision| decision.residual)
            .count();
        let pushed_filter_count = decisions.len().saturating_sub(unsupported_count);

        Self {
            decisions,
            exact_count,
            inexact_count,
            unsupported_count,
            pushed_filter_count,
            residual_filter_count,
        }
    }

    pub(crate) fn datafusion_pushdowns(&self) -> Vec<TableProviderFilterPushDown> {
        self.decisions
            .iter()
            .map(|decision| decision.outcome.to_datafusion())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use datafusion::datasource::TableProvider;
    use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, col, lit};

    use super::super::provider::DeltaTableProvider;
    use super::*;
    use crate::query_engine::datafusion::test_support::{
        DeltaLogTable, PARTITIONED_SCHEMA_FIELDS_JSON,
    };
    use crate::{DeltaSourceConfig, load_delta_source, preflight_delta_protocol};

    #[test]
    fn filter_pushdown_reports_exact_for_supported_partition_equality()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "filter-pushdown-exact-partition-equality",
            PARTITIONED_SCHEMA_FIELDS_JSON,
            r#"["region"]"#,
            r#""partitionValues":{"region":"us-west"}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let filter = col("region").eq(lit("us-west"));

        let support = provider.supports_filters_pushdown(&[&filter])?;
        let plan = provider.plan_supports_filters_pushdown(&[&filter]);

        assert_eq!(support, vec![TableProviderFilterPushDown::Exact]);
        assert_eq!(plan.exact_count, 1);
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.residual_filter_count, 0);

        Ok(())
    }

    #[test]
    fn filter_pushdown_reports_one_exact_status_for_partition_equality_and()
    -> Result<(), Box<dyn std::error::Error>> {
        const TWO_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"day\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;
        let table = DeltaLogTable::new_with_schema(
            "filter-pushdown-exact-partition-and",
            TWO_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["region","day"]"#,
            r#""partitionValues":{"region":"us-west","day":"2026-05-31"}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let filter = col("region")
            .eq(lit("us-west"))
            .and(col("day").eq(lit("2026-05-31")));

        let support = provider.supports_filters_pushdown(&[&filter])?;
        let plan = provider.plan_supports_filters_pushdown(&[&filter]);

        assert_eq!(support, vec![TableProviderFilterPushDown::Exact]);
        assert_eq!(plan.exact_count, 1);
        assert_eq!(plan.pushed_filter_count, 1);
        assert_eq!(plan.residual_filter_count, 0);

        Ok(())
    }

    #[test]
    fn filter_pushdown_keeps_data_filters_unsupported() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("filter-pushdown-data-unsupported")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let id_filter = col("id").gt(lit(1));
        let name_filter = col("customer_name").eq(lit("a"));

        let support = provider.supports_filters_pushdown(&[&id_filter, &name_filter])?;

        assert_eq!(
            support,
            vec![
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported
            ]
        );

        Ok(())
    }

    #[test]
    fn filter_pushdown_accepts_string_null_checks_and_rejects_other_shapes()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "filter-pushdown-partition-in",
            PARTITIONED_SCHEMA_FIELDS_JSON,
            r#"["region"]"#,
            &[
                r#""partitionValues":{"region":"us-west"}"#,
                r#""partitionValues":{"region":""}"#,
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let partition_in_filter =
            col("region").in_list(vec![lit("us-west"), lit("us-east")], false);
        let data_between_filter = col("id").between(lit(10), lit(20));
        let mixed_and_filter = col("region").eq(lit("us-west")).and(col("id").gt(lit(10)));
        let mixed_or_filter = col("region").eq(lit("us-west")).or(col("id").gt(lit(10)));
        let not_filter = Expr::Not(Box::new(col("id").gt(lit(10))));
        let null_check_filter = col("region").is_not_null();

        let filters = [
            &partition_in_filter,
            &data_between_filter,
            &mixed_and_filter,
            &mixed_or_filter,
            &not_filter,
            &null_check_filter,
        ];
        let support = provider.supports_filters_pushdown(&filters)?;
        let plan = provider.plan_supports_filters_pushdown(&filters);

        assert_eq!(
            support,
            vec![
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Exact,
            ]
        );
        assert_eq!(plan.exact_count, 2);
        assert_eq!(plan.inexact_count, 0);
        assert_eq!(plan.unsupported_count, 4);
        assert_eq!(plan.residual_filter_count, 4);

        Ok(())
    }

    #[test]
    fn filter_plan_empty_input_has_consistent_zero_counts() -> Result<(), Box<dyn std::error::Error>>
    {
        let table = DeltaLogTable::new("empty-filter-plan")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;

        let plan = provider.plan_supports_filters_pushdown(&[]);

        assert!(plan.datafusion_pushdowns().is_empty());
        assert!(plan.decisions.is_empty());
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, 0);
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.pushed_filter_count, 0);
        assert_eq!(plan.residual_filter_count, 0);

        Ok(())
    }

    #[test]
    fn filter_planning_contract_does_not_call_scan_or_read_paths()
    -> Result<(), Box<dyn std::error::Error>> {
        let filter_module_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("query_engine")
            .join("datafusion");
        let filter_source_files = [
            filter_module_root.join("filters.rs"),
            filter_module_root.join("filters").join("analysis.rs"),
            filter_module_root
                .join("filters")
                .join("partition_pushdown.rs"),
        ];
        let production_source = filter_source_files
            .iter()
            .map(fs::read_to_string)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|source| {
                source
                    .split("\n#[cfg(test)]")
                    .next()
                    .unwrap_or(source.as_str())
                    .to_owned()
            })
            .collect::<Vec<_>>()
            .join("\n");

        assert!(!production_source.contains("with_predicate"));
        assert!(!production_source.contains("with_filter"));
        assert!(!production_source.contains("RecordBatch"));
        assert!(!production_source.to_ascii_lowercase().contains("parquet"));

        Ok(())
    }

    #[test]
    fn filter_plan_reason_codes_are_control_character_safe()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("filter-plan-control-characters")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let hostile_filter = col("ghost\ncolumn").eq(lit("x"));

        let plan = provider.plan_supports_filters_pushdown(&[&hostile_filter]);
        let reason_code = plan.decisions[0]
            .rejection_reason
            .map(DeltaFilterPushdownRejectionReason::code);

        assert_eq!(
            plan.decisions[0].rejection_reason,
            Some(DeltaFilterPushdownRejectionReason::UnknownColumn)
        );
        assert_eq!(reason_code.map(|code| code.contains('\n')), Some(false));
        assert_eq!(reason_code.map(|code| code.contains('\r')), Some(false));
        assert_eq!(reason_code.map(|code| code.contains('\t')), Some(false));
        assert_eq!(reason_code, Some("unsupported_unknown_column"));

        Ok(())
    }
}
