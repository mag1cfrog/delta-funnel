//! Filter pushdown planning for the Delta DataFusion provider.

use std::collections::HashSet;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProviderFilterPushdownKind {
    Exact,
    Inexact,
    Unsupported,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProviderFilterReason {
    InitialPolicy,
    ExpressionShape,
    InternalColumn,
    UnknownColumn,
}

impl ProviderFilterReason {
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn code(self) -> &'static str {
        match self {
            Self::InitialPolicy => "unsupported_initial_policy",
            Self::ExpressionShape => "unsupported_expression_shape",
            Self::InternalColumn => "unsupported_internal_column",
            Self::UnknownColumn => "unsupported_unknown_column",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProviderFilterDecision {
    pub(crate) input_index: usize,
    pub(crate) pushdown: TableProviderFilterPushDown,
    pub(crate) kind: ProviderFilterPushdownKind,
    pub(crate) residual: bool,
    pub(crate) reason: ProviderFilterReason,
    pub(crate) referenced_columns: Vec<String>,
    pub(crate) partition_columns: Vec<String>,
    pub(crate) data_columns: Vec<String>,
    pub(crate) unknown_columns: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ProviderFilterPlan {
    pub(crate) pushdown_statuses: Vec<TableProviderFilterPushDown>,
    pub(crate) decisions: Vec<ProviderFilterDecision>,
    pub(crate) exact_count: usize,
    pub(crate) inexact_count: usize,
    pub(crate) unsupported_count: usize,
    pub(crate) pushed_filter_count: usize,
    pub(crate) residual_filter_count: usize,
}

impl ProviderFilterPlan {
    #[must_use]
    pub(crate) fn unsupported(
        filters: &[&Expr],
        schema: &SchemaRef,
        partition_columns: &HashSet<String>,
    ) -> Self {
        let decisions = filters
            .iter()
            .enumerate()
            .map(|(input_index, filter)| {
                ProviderFilterDecision::unsupported(input_index, filter, schema, partition_columns)
            })
            .collect::<Vec<_>>();

        Self::from_decisions(decisions)
    }

    #[must_use]
    pub(crate) fn empty_pushed() -> Self {
        Self::from_decisions(Vec::new())
    }

    fn from_decisions(decisions: Vec<ProviderFilterDecision>) -> Self {
        let pushdown_statuses = decisions
            .iter()
            .map(|decision| decision.pushdown.clone())
            .collect::<Vec<_>>();
        let exact_count = decisions
            .iter()
            .filter(|decision| decision.kind == ProviderFilterPushdownKind::Exact)
            .count();
        let inexact_count = decisions
            .iter()
            .filter(|decision| decision.kind == ProviderFilterPushdownKind::Inexact)
            .count();
        let unsupported_count = decisions
            .iter()
            .filter(|decision| decision.kind == ProviderFilterPushdownKind::Unsupported)
            .count();
        let residual_filter_count = decisions
            .iter()
            .filter(|decision| decision.residual)
            .count();
        let pushed_filter_count = decisions.len().saturating_sub(unsupported_count);

        Self {
            pushdown_statuses,
            decisions,
            exact_count,
            inexact_count,
            unsupported_count,
            pushed_filter_count,
            residual_filter_count,
        }
    }
}

impl ProviderFilterDecision {
    fn unsupported(
        input_index: usize,
        filter: &Expr,
        schema: &SchemaRef,
        partition_columns: &HashSet<String>,
    ) -> Self {
        let mut referenced_columns = filter
            .column_refs()
            .iter()
            .map(|column| column.flat_name())
            .collect::<Vec<_>>();
        referenced_columns.sort();
        referenced_columns.dedup();

        let unknown_columns = referenced_columns
            .iter()
            .filter(|column| {
                schema
                    .field_with_name(schema_lookup_name(column, schema).as_str())
                    .is_err()
            })
            .cloned()
            .collect::<Vec<_>>();
        let referenced_partition_columns = referenced_columns
            .iter()
            .filter(|column| {
                partition_columns.contains(schema_lookup_name(column, schema).as_str())
            })
            .cloned()
            .collect::<Vec<_>>();
        let data_columns = referenced_columns
            .iter()
            .filter(|column| {
                let lookup_name = schema_lookup_name(column, schema);
                schema.field_with_name(lookup_name.as_str()).is_ok()
                    && !partition_columns.contains(lookup_name.as_str())
            })
            .cloned()
            .collect::<Vec<_>>();

        Self {
            input_index,
            pushdown: TableProviderFilterPushDown::Unsupported,
            kind: ProviderFilterPushdownKind::Unsupported,
            residual: true,
            reason: unsupported_filter_reason(filter, &unknown_columns),
            referenced_columns,
            partition_columns: referenced_partition_columns,
            data_columns,
            unknown_columns,
        }
    }
}

fn schema_lookup_name(flat_column_ref: &str, schema: &SchemaRef) -> String {
    // Case 1: the flat reference already names a top-level Arrow field. This
    // also preserves unusual but legal top-level names that contain dots.
    // Example: schema has top-level `id`, input is `id`.
    // Example: schema has top-level `a.b`, input is `a.b`.
    if schema.field_with_name(flat_column_ref).is_ok() {
        return flat_column_ref.to_owned();
    }

    // Case 2: no qualifier or dotted path was present, and the exact top-level
    // lookup failed above. Keep the original name so it is reported as unknown.
    // Example: schema has no `ghost`, input is `ghost`.
    let Some((first_segment, _remainder)) = flat_column_ref.split_once('.') else {
        return flat_column_ref.to_owned();
    };

    // Case 3: the first path segment is itself a top-level field, as in
    // `profile.age` against a schema that contains `profile`. Treat this as a
    // nested-field style reference for this planning slice, keep the full
    // reference, and let the top-level lookup fail so the filter stays
    // unsupported.
    // Example: schema has top-level `profile`, input is `profile.age`.
    // Example: schema has top-level `profile`, input is `profile.address.city`.
    if schema.field_with_name(first_segment).is_ok() {
        flat_column_ref.to_owned()
    } else {
        let unqualified_name = flat_column_ref
            .rsplit_once('.')
            .map_or(flat_column_ref, |(_qualifier, name)| name);
        // Case 4: the prefix is not a top-level field, as in `orders.id`
        // against a provider schema with top-level `id`. Treat the prefix as a
        // relation qualifier and use the suffix for top-level schema metadata.
        // Example: schema has top-level `id`, input is `orders.id`.
        // Example: schema has top-level `id`, input is `catalog.public.orders.id`.
        unqualified_name.to_owned()
    }
}

fn unsupported_filter_reason(filter: &Expr, unknown_columns: &[String]) -> ProviderFilterReason {
    if filter
        .column_refs()
        .iter()
        .any(|column| column.name.starts_with("__delta_funnel_"))
    {
        return ProviderFilterReason::InternalColumn;
    }

    if !unknown_columns.is_empty() {
        return ProviderFilterReason::UnknownColumn;
    }

    if is_simple_comparison(filter) {
        ProviderFilterReason::InitialPolicy
    } else {
        ProviderFilterReason::ExpressionShape
    }
}

fn is_simple_comparison(filter: &Expr) -> bool {
    match filter {
        Expr::BinaryExpr(binary)
            if matches!(
                binary.op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
            ) =>
        {
            is_column_or_literal(binary.left.as_ref())
                && is_column_or_literal(binary.right.as_ref())
        }
        _ => false,
    }
}

fn is_column_or_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(_) | Expr::Literal(_, _))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::DataType;
    use datafusion::common::{Column, ScalarValue};
    use datafusion::datasource::TableProvider;
    use datafusion::logical_expr::{
        ColumnarValue, Expr, TableProviderFilterPushDown, Volatility, cast, col, create_udf, lit,
    };

    use super::super::provider::DeltaTableProvider;
    use super::*;
    use crate::query_engine::datafusion::test_support::{
        DEEP_NESTED_WITH_CITY_SCHEMA_FIELDS_JSON, DeltaLogTable, NESTED_SCHEMA_FIELDS_JSON,
        PARTITIONED_SCHEMA_FIELDS_JSON,
    };
    use crate::{DeltaSourceConfig, load_delta_source, preflight_delta_protocol};

    #[test]
    fn filter_pushdown_is_explicitly_unsupported_for_all_filters()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("filter-pushdown-unsupported")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let id_filter = datafusion::logical_expr::col("id").gt(datafusion::logical_expr::lit(1));
        let name_filter =
            datafusion::logical_expr::col("customer_name").eq(datafusion::logical_expr::lit("a"));

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

        let plan = provider.plan_filters(&[]);

        assert!(plan.pushdown_statuses.is_empty());
        assert!(plan.decisions.is_empty());
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, 0);
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.pushed_filter_count, 0);
        assert_eq!(plan.residual_filter_count, 0);

        Ok(())
    }

    #[test]
    fn filter_plan_preserves_order_duplicates_and_column_classification()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "ordered-filter-plan",
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
        let region_filter = col("region").eq(lit("us-west"));
        let id_filter = col("id").gt(lit(1));
        let id_filter_duplicate = col("id").gt(lit(1));
        let unknown_filter = col("ghost_column").eq(lit("x"));
        let internal_filter = col("__delta_funnel_file_id").eq(lit("part-00001.parquet"));

        let plan = provider.plan_filters(&[
            &region_filter,
            &id_filter,
            &id_filter_duplicate,
            &unknown_filter,
            &internal_filter,
        ]);

        assert_eq!(
            plan.pushdown_statuses,
            vec![
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
            ]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, 0);
        assert_eq!(plan.unsupported_count, 5);
        assert_eq!(plan.pushed_filter_count, 0);
        assert_eq!(plan.residual_filter_count, 5);
        assert_eq!(
            plan.decisions
                .iter()
                .map(|decision| decision.input_index)
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4]
        );

        assert_eq!(
            plan.decisions[0].kind,
            ProviderFilterPushdownKind::Unsupported
        );
        assert_eq!(
            plan.decisions[0].reason,
            ProviderFilterReason::InitialPolicy
        );
        assert_eq!(plan.decisions[0].referenced_columns, vec!["region"]);
        assert_eq!(plan.decisions[0].partition_columns, vec!["region"]);
        assert!(plan.decisions[0].data_columns.is_empty());
        assert!(plan.decisions[0].unknown_columns.is_empty());

        assert_eq!(plan.decisions[1].referenced_columns, vec!["id"]);
        assert_eq!(plan.decisions[1].data_columns, vec!["id"]);
        assert!(plan.decisions[1].partition_columns.is_empty());
        assert_eq!(plan.decisions[2].referenced_columns, vec!["id"]);
        assert_eq!(
            plan.decisions[3].reason,
            ProviderFilterReason::UnknownColumn
        );
        assert_eq!(plan.decisions[3].unknown_columns, vec!["ghost_column"]);
        assert_eq!(
            plan.decisions[4].reason,
            ProviderFilterReason::InternalColumn
        );
        assert_eq!(
            plan.decisions[4].unknown_columns,
            vec!["__delta_funnel_file_id"]
        );

        Ok(())
    }

    #[test]
    fn filter_plan_marks_complex_expression_shapes_unsupported()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("complex-filter-plan")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let cast_filter = cast(col("id"), DataType::Int64).eq(lit(7_i64));
        let and_filter = col("id")
            .gt(lit(1))
            .and(col("customer_name").eq(lit("alice")));
        let or_filter = col("id")
            .gt(lit(1))
            .or(col("customer_name").eq(lit("alice")));
        let not_filter = Expr::Not(Box::new(col("id").gt(lit(1))));
        let scalar_udf = create_udf(
            "is_interesting",
            vec![DataType::Utf8],
            DataType::Boolean,
            Volatility::Immutable,
            Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))))),
        );
        let scalar_function_filter =
            Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
                Arc::new(scalar_udf),
                vec![col("customer_name")],
            ));

        let plan = provider.plan_filters(&[
            &cast_filter,
            &and_filter,
            &or_filter,
            &not_filter,
            &scalar_function_filter,
        ]);

        assert_eq!(plan.unsupported_count, 5);
        assert_eq!(plan.residual_filter_count, 5);
        assert_eq!(
            plan.decisions
                .iter()
                .map(|decision| decision.reason)
                .collect::<Vec<_>>(),
            vec![
                ProviderFilterReason::ExpressionShape,
                ProviderFilterReason::ExpressionShape,
                ProviderFilterReason::ExpressionShape,
                ProviderFilterReason::ExpressionShape,
                ProviderFilterReason::ExpressionShape
            ]
        );

        Ok(())
    }

    #[test]
    fn filter_plan_tracks_nested_field_reference_as_unsupported_metadata()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "nested-filter-plan",
            NESTED_SCHEMA_FIELDS_JSON,
            "[]",
            r#""partitionValues":{}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let nested_filter = col("profile.age").gt(lit(21));

        let plan = provider.plan_filters(&[&nested_filter]);

        assert_eq!(
            plan.pushdown_statuses,
            vec![TableProviderFilterPushDown::Unsupported]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, 0);
        assert_eq!(plan.unsupported_count, 1);
        assert_eq!(plan.pushed_filter_count, 0);
        assert_eq!(plan.residual_filter_count, 1);
        assert_eq!(plan.decisions[0].referenced_columns, vec!["profile.age"]);
        assert_eq!(plan.decisions[0].unknown_columns, vec!["profile.age"]);
        assert_eq!(
            plan.decisions[0].reason,
            ProviderFilterReason::UnknownColumn
        );

        Ok(())
    }

    #[test]
    fn filter_plan_does_not_misclassify_deep_nested_ref_as_top_level_suffix()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "deep-nested-filter-plan",
            DEEP_NESTED_WITH_CITY_SCHEMA_FIELDS_JSON,
            "[]",
            r#""partitionValues":{}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let nested_filter = col("profile.address.city").eq(lit("Phoenix"));

        let plan = provider.plan_filters(&[&nested_filter]);

        assert_eq!(plan.unsupported_count, 1);
        assert_eq!(plan.residual_filter_count, 1);
        assert_eq!(
            plan.decisions[0].referenced_columns,
            vec!["profile.address.city"]
        );
        assert_eq!(
            plan.decisions[0].unknown_columns,
            vec!["profile.address.city"]
        );
        assert!(plan.decisions[0].data_columns.is_empty());
        assert_eq!(
            plan.decisions[0].reason,
            ProviderFilterReason::UnknownColumn
        );

        Ok(())
    }

    #[test]
    fn filter_plan_classifies_qualified_top_level_reference_without_losing_diagnostic_ref()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("qualified-filter-plan")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let qualified_filter = Expr::Column(Column::new(Some("orders"), "id")).eq(lit(7));

        let plan = provider.plan_filters(&[&qualified_filter]);

        assert_eq!(plan.unsupported_count, 1);
        assert_eq!(plan.residual_filter_count, 1);
        assert_eq!(plan.decisions[0].referenced_columns, vec!["orders.id"]);
        assert_eq!(plan.decisions[0].data_columns, vec!["orders.id"]);
        assert!(plan.decisions[0].unknown_columns.is_empty());
        assert_eq!(
            plan.decisions[0].reason,
            ProviderFilterReason::InitialPolicy
        );

        Ok(())
    }

    #[test]
    fn filter_planning_contract_does_not_call_kernel_or_read_paths()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("src")
                .join("query_engine")
                .join("datafusion")
                .join("filters.rs"),
        )?;

        let production_source = source
            .split("\n#[cfg(test)]")
            .next()
            .unwrap_or(source.as_str());

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

        let plan = provider.plan_filters(&[&hostile_filter]);
        let reason_code = plan.decisions[0].reason.code();

        assert_eq!(
            plan.decisions[0].reason,
            ProviderFilterReason::UnknownColumn
        );
        assert!(!reason_code.contains('\n'));
        assert!(!reason_code.contains('\r'));
        assert!(!reason_code.contains('\t'));
        assert_eq!(reason_code, "unsupported_unknown_column");

        Ok(())
    }
}
