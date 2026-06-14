//! Tests for the Delta DataFusion table provider.

use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};

use super::super::super::execution::{
    DeltaProviderReaderBackend, DeltaProviderScanExecutionOptions, DeltaScanPlanningExec,
};
use super::super::super::planning::partition_target::DeltaScanPartitionTargetSource;
use super::*;
use crate::query_engine::datafusion::catalog::registration::{
    DeltaTableProviderConfig, register_delta_sources,
};
use crate::query_engine::datafusion::test_support::{
    DEFAULT_SCHEMA_FIELDS_JSON, DeltaLogTable, INVALID_NESTED_IDS_SCHEMA_FIELDS_JSON,
    NESTED_SCHEMA_FIELDS_JSON, PARTITIONED_SCHEMA_FIELDS_JSON, find_delta_scan_plans,
    register_fixture_source,
};
use crate::{DeltaFunnelError, DeltaSourceConfig, load_delta_source, preflight_delta_protocol};

const INTEGER_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"byte_part\",\"type\":\"byte\",\"nullable\":true,\"metadata\":{}},{\"name\":\"short_part\",\"type\":\"short\",\"nullable\":true,\"metadata\":{}},{\"name\":\"int_part\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"long_part\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]"#;
const BOOLEAN_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"is_current\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}}]"#;
const DATE_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"event_date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]"#;
const DECIMAL_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"amount\",\"type\":\"decimal(10,2)\",\"nullable\":true,\"metadata\":{}}]"#;
const HIGH_PRECISION_DECIMAL_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"amount\",\"type\":\"decimal(38,18)\",\"nullable\":true,\"metadata\":{}}]"#;
const FLOATING_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"float_part\",\"type\":\"float\",\"nullable\":true,\"metadata\":{}},{\"name\":\"double_part\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]"#;
const TIMESTAMP_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"event_ts\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}}]"#;
const BINARY_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"payload\",\"type\":\"binary\",\"nullable\":true,\"metadata\":{}}]"#;
const TIMESTAMP_NTZ_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"event_ts_ntz\",\"type\":\"timestamp_ntz\",\"nullable\":true,\"metadata\":{}}]"#;
const TIMESTAMP_NTZ_PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["timestampNtz"],"writerFeatures":["timestampNtz"]}}"#;
const DELETION_VECTOR_PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"#;
const BOOLEAN_DATA_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"is_current\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}}]"#;
const BINARY_DATA_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"payload\",\"type\":\"binary\",\"nullable\":true,\"metadata\":{}}]"#;
const DATE_DATA_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"event_date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]"#;
const DECIMAL_DATA_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"amount\",\"type\":\"decimal(10,2)\",\"nullable\":true,\"metadata\":{}}]"#;
const FLOATING_DATA_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"float_score\",\"type\":\"float\",\"nullable\":true,\"metadata\":{}},{\"name\":\"double_score\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]"#;
const TIMESTAMP_DATA_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"event_ts\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}}]"#;
const TIMESTAMP_NTZ_DATA_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"event_ts_ntz\",\"type\":\"timestamp_ntz\",\"nullable\":true,\"metadata\":{}}]"#;

fn scan_file_paths(
    scan: &DeltaScanPlanningExec,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let scan_plan = scan.scan_plan();
    scan_plan
        .kernel_scan()
        .scan_file_paths(&scan_plan.table_uri)
}

fn scan_partition_file_paths(scan: &DeltaScanPlanningExec) -> Vec<Vec<String>> {
    scan.partition_plan()
        .partitions
        .iter()
        .map(|partition| {
            partition
                .file_tasks
                .iter()
                .map(|file_task| file_task.path.clone())
                .collect()
        })
        .collect()
}

fn assert_scan_does_not_support_limit_pushdown(scan: &DeltaScanPlanningExec) {
    assert!(
        !scan.supports_limit_pushdown(),
        "DeltaScanPlanningExec must not advertise provider-level limit pushdown"
    );
    assert_eq!(scan.fetch(), None);
    assert!(scan.with_fetch(Some(1)).is_none());
}

fn id_stats_add_json(num_records: i64, min_value: i32, max_value: i32, null_count: i64) -> String {
    format!(
        r#""partitionValues":{{}},"stats":"{{\"numRecords\":{num_records},\"minValues\":{{\"id\":{min_value}}},\"maxValues\":{{\"id\":{max_value}}},\"nullCount\":{{\"id\":{null_count}}}}}""#
    )
}

fn partitioned_id_stats_add_json(
    partition_values_json: &str,
    num_records: i64,
    min_value: i32,
    max_value: i32,
    null_count: i64,
) -> String {
    format!(
        r#""partitionValues":{partition_values_json},"stats":"{{\"numRecords\":{num_records},\"minValues\":{{\"id\":{min_value}}},\"maxValues\":{{\"id\":{max_value}}},\"nullCount\":{{\"id\":{null_count}}}}}""#
    )
}

fn id_partial_stats_add_json(
    num_records: i64,
    min_value: Option<i32>,
    max_value: Option<i32>,
    null_count: i64,
) -> String {
    let mut stats_fields = vec![format!(r#"\"numRecords\":{num_records}"#)];
    if let Some(min_value) = min_value {
        stats_fields.push(format!(r#"\"minValues\":{{\"id\":{min_value}}}"#));
    }
    if let Some(max_value) = max_value {
        stats_fields.push(format!(r#"\"maxValues\":{{\"id\":{max_value}}}"#));
    }
    stats_fields.push(format!(r#"\"nullCount\":{{\"id\":{null_count}}}"#));

    format!(
        r#""partitionValues":{{}},"stats":"{{{}}}""#,
        stats_fields.join(",")
    )
}

fn stats_add_json(stats_json: &str) -> String {
    format!(r#""partitionValues":{{}},"stats":"{stats_json}""#)
}

fn id_stats_add_json_with_optional_counts(
    num_records: Option<i64>,
    min_value: i32,
    max_value: i32,
    null_count: Option<i64>,
) -> String {
    let mut stats_fields = Vec::new();
    if let Some(num_records) = num_records {
        stats_fields.push(format!(r#"\"numRecords\":{num_records}"#));
    }
    stats_fields.push(format!(r#"\"minValues\":{{\"id\":{min_value}}}"#));
    stats_fields.push(format!(r#"\"maxValues\":{{\"id\":{max_value}}}"#));
    if let Some(null_count) = null_count {
        stats_fields.push(format!(r#"\"nullCount\":{{\"id\":{null_count}}}"#));
    }

    format!(
        r#""partitionValues":{{}},"stats":"{{{}}}""#,
        stats_fields.join(",")
    )
}

fn boolean_stats_add_json(
    num_records: i64,
    min_value: bool,
    max_value: bool,
    null_count: i64,
) -> String {
    format!(
        r#""partitionValues":{{}},"stats":"{{\"numRecords\":{num_records},\"minValues\":{{\"is_current\":{min_value}}},\"maxValues\":{{\"is_current\":{max_value}}},\"nullCount\":{{\"is_current\":{null_count}}}}}""#
    )
}

fn partitioned_boolean_stats_add_json(
    partition_values_json: &str,
    num_records: i64,
    min_value: bool,
    max_value: bool,
    null_count: i64,
) -> String {
    format!(
        r#""partitionValues":{partition_values_json},"stats":"{{\"numRecords\":{num_records},\"minValues\":{{\"is_current\":{min_value}}},\"maxValues\":{{\"is_current\":{max_value}}},\"nullCount\":{{\"is_current\":{null_count}}}}}""#
    )
}

fn partitioned_boolean_partial_stats_add_json(
    partition_values_json: &str,
    num_records: i64,
    null_count: Option<i64>,
) -> String {
    let mut stats_fields = vec![format!(r#"\"numRecords\":{num_records}"#)];
    if let Some(null_count) = null_count {
        stats_fields.push(format!(r#"\"nullCount\":{{\"is_current\":{null_count}}}"#));
    }

    format!(
        r#""partitionValues":{partition_values_json},"stats":"{{{}}}""#,
        stats_fields.join(",")
    )
}

fn boolean_partial_stats_add_json(
    num_records: i64,
    min_value: Option<bool>,
    max_value: Option<bool>,
    null_count: Option<i64>,
) -> String {
    let mut stats_fields = vec![format!(r#"\"numRecords\":{num_records}"#)];
    if let Some(min_value) = min_value {
        stats_fields.push(format!(r#"\"minValues\":{{\"is_current\":{min_value}}}"#));
    }
    if let Some(max_value) = max_value {
        stats_fields.push(format!(r#"\"maxValues\":{{\"is_current\":{max_value}}}"#));
    }
    if let Some(null_count) = null_count {
        stats_fields.push(format!(r#"\"nullCount\":{{\"is_current\":{null_count}}}"#));
    }

    format!(
        r#""partitionValues":{{}},"stats":"{{{}}}""#,
        stats_fields.join(",")
    )
}

fn binary_partial_stats_add_json(num_records: i64, null_count: Option<i64>) -> String {
    let mut stats_fields = vec![format!(r#"\"numRecords\":{num_records}"#)];
    if let Some(null_count) = null_count {
        stats_fields.push(format!(r#"\"nullCount\":{{\"payload\":{null_count}}}"#));
    }

    format!(
        r#""partitionValues":{{}},"stats":"{{{}}}""#,
        stats_fields.join(",")
    )
}

fn date_stats_add_json(
    num_records: i64,
    min_value: &str,
    max_value: &str,
    null_count: i64,
) -> String {
    format!(
        r#""partitionValues":{{}},"stats":"{{\"numRecords\":{num_records},\"minValues\":{{\"event_date\":\"{min_value}\"}},\"maxValues\":{{\"event_date\":\"{max_value}\"}},\"nullCount\":{{\"event_date\":{null_count}}}}}""#
    )
}

fn partitioned_date_stats_add_json(
    partition_values_json: &str,
    num_records: i64,
    min_value: &str,
    max_value: &str,
    null_count: i64,
) -> String {
    format!(
        r#""partitionValues":{partition_values_json},"stats":"{{\"numRecords\":{num_records},\"minValues\":{{\"event_date\":\"{min_value}\"}},\"maxValues\":{{\"event_date\":\"{max_value}\"}},\"nullCount\":{{\"event_date\":{null_count}}}}}""#
    )
}

fn decimal_stats_add_json(
    num_records: i64,
    min_value: &str,
    max_value: &str,
    null_count: i64,
) -> String {
    format!(
        r#""partitionValues":{{}},"stats":"{{\"numRecords\":{num_records},\"minValues\":{{\"amount\":\"{min_value}\"}},\"maxValues\":{{\"amount\":\"{max_value}\"}},\"nullCount\":{{\"amount\":{null_count}}}}}""#
    )
}

fn partitioned_decimal_stats_add_json(
    partition_values_json: &str,
    num_records: i64,
    min_value: &str,
    max_value: &str,
    null_count: i64,
) -> String {
    format!(
        r#""partitionValues":{partition_values_json},"stats":"{{\"numRecords\":{num_records},\"minValues\":{{\"amount\":\"{min_value}\"}},\"maxValues\":{{\"amount\":\"{max_value}\"}},\"nullCount\":{{\"amount\":{null_count}}}}}""#
    )
}

fn floating_stats_add_json(
    num_records: i64,
    float_min_value: &str,
    float_max_value: &str,
    double_min_value: &str,
    double_max_value: &str,
    null_count: i64,
) -> String {
    format!(
        r#""partitionValues":{{}},"stats":"{{\"numRecords\":{num_records},\"minValues\":{{\"float_score\":{float_min_value},\"double_score\":{double_min_value}}},\"maxValues\":{{\"float_score\":{float_max_value},\"double_score\":{double_max_value}}},\"nullCount\":{{\"float_score\":{null_count},\"double_score\":{null_count}}}}}""#
    )
}

fn partitioned_floating_stats_add_json(
    partition_values_json: &str,
    num_records: i64,
    float_min_value: &str,
    float_max_value: &str,
    double_min_value: &str,
    double_max_value: &str,
    null_count: i64,
) -> String {
    format!(
        r#""partitionValues":{partition_values_json},"stats":"{{\"numRecords\":{num_records},\"minValues\":{{\"float_score\":{float_min_value},\"double_score\":{double_min_value}}},\"maxValues\":{{\"float_score\":{float_max_value},\"double_score\":{double_max_value}}},\"nullCount\":{{\"float_score\":{null_count},\"double_score\":{null_count}}}}}""#
    )
}

fn floating_partial_stats_add_json(num_records: i64, null_count: Option<i64>) -> String {
    let mut stats_fields = vec![format!(r#"\"numRecords\":{num_records}"#)];
    if let Some(null_count) = null_count {
        stats_fields.push(format!(
            r#"\"nullCount\":{{\"float_score\":{null_count},\"double_score\":{null_count}}}"#
        ));
    }

    format!(
        r#""partitionValues":{{}},"stats":"{{{}}}""#,
        stats_fields.join(",")
    )
}

fn string_stats_add_json(
    num_records: i64,
    min_value: &str,
    max_value: &str,
    null_count: i64,
) -> String {
    format!(
        r#""partitionValues":{{}},"stats":"{{\"numRecords\":{num_records},\"minValues\":{{\"customer_name\":\"{min_value}\"}},\"maxValues\":{{\"customer_name\":\"{max_value}\"}},\"nullCount\":{{\"customer_name\":{null_count}}}}}""#
    )
}

fn partitioned_string_stats_add_json(
    partition_values_json: &str,
    num_records: i64,
    min_value: &str,
    max_value: &str,
    null_count: i64,
) -> String {
    format!(
        r#""partitionValues":{partition_values_json},"stats":"{{\"numRecords\":{num_records},\"minValues\":{{\"customer_name\":\"{min_value}\"}},\"maxValues\":{{\"customer_name\":\"{max_value}\"}},\"nullCount\":{{\"customer_name\":{null_count}}}}}""#
    )
}

fn string_partial_stats_add_json(num_records: i64, null_count: Option<i64>) -> String {
    let mut stats_fields = vec![format!(r#"\"numRecords\":{num_records}"#)];
    if let Some(null_count) = null_count {
        stats_fields.push(format!(
            r#"\"nullCount\":{{\"customer_name\":{null_count}}}"#
        ));
    }

    format!(
        r#""partitionValues":{{}},"stats":"{{{}}}""#,
        stats_fields.join(",")
    )
}

fn string_partial_bounds_stats_add_json(
    num_records: i64,
    min_value: Option<&str>,
    max_value: Option<&str>,
    null_count: Option<i64>,
) -> String {
    let mut stats_fields = vec![format!(r#"\"numRecords\":{num_records}"#)];
    if let Some(min_value) = min_value {
        stats_fields.push(format!(
            r#"\"minValues\":{{\"customer_name\":\"{min_value}\"}}"#
        ));
    }
    if let Some(max_value) = max_value {
        stats_fields.push(format!(
            r#"\"maxValues\":{{\"customer_name\":\"{max_value}\"}}"#
        ));
    }
    if let Some(null_count) = null_count {
        stats_fields.push(format!(
            r#"\"nullCount\":{{\"customer_name\":{null_count}}}"#
        ));
    }

    format!(
        r#""partitionValues":{{}},"stats":"{{{}}}""#,
        stats_fields.join(",")
    )
}

fn decimal_partial_stats_add_json(num_records: i64, null_count: Option<i64>) -> String {
    let mut stats_fields = vec![format!(r#"\"numRecords\":{num_records}"#)];
    if let Some(null_count) = null_count {
        stats_fields.push(format!(r#"\"nullCount\":{{\"amount\":{null_count}}}"#));
    }

    format!(
        r#""partitionValues":{{}},"stats":"{{{}}}""#,
        stats_fields.join(",")
    )
}

fn timestamp_stats_add_json(
    column_name: &str,
    num_records: i64,
    min_value: &str,
    max_value: &str,
    null_count: i64,
) -> String {
    format!(
        r#""partitionValues":{{}},"stats":"{{\"numRecords\":{num_records},\"minValues\":{{\"{column_name}\":\"{min_value}\"}},\"maxValues\":{{\"{column_name}\":\"{max_value}\"}},\"nullCount\":{{\"{column_name}\":{null_count}}}}}""#
    )
}

fn temporal_partial_stats_add_json(
    column_name: &str,
    num_records: i64,
    null_count: Option<i64>,
) -> String {
    let mut stats_fields = vec![format!(r#"\"numRecords\":{num_records}"#)];
    if let Some(null_count) = null_count {
        stats_fields.push(format!(
            r#"\"nullCount\":{{\"{column_name}\":{null_count}}}"#
        ));
    }

    format!(
        r#""partitionValues":{{}},"stats":"{{{}}}""#,
        stats_fields.join(",")
    )
}

#[test]
fn datafusion_table_provider_api_symbols_are_available() -> datafusion::error::Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let table: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(Arc::clone(&schema)));
    let ctx = SessionContext::new();

    ctx.register_table("orders", Arc::clone(&table))?;

    assert_eq!(table.table_type(), TableType::Base);
    assert_eq!(table.schema().as_ref(), schema.as_ref());

    Ok(())
}

#[test]
fn delta_provider_exposes_logical_arrow_schema() -> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new("schema")?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;

    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let schema = provider.schema();

    assert_eq!(provider.source_name(), "orders");
    assert_eq!(provider.snapshot_version(), 1);
    assert_eq!(provider.protocol().source_name, "orders");
    assert_eq!(provider.table_type(), TableType::Base);
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(0).data_type(), &DataType::Int32);
    assert!(!schema.field(0).is_nullable());
    assert_eq!(schema.field(1).name(), "customer_name");
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
    assert!(schema.field(1).is_nullable());

    Ok(())
}

#[test]
fn delta_provider_accepts_native_async_backend_for_local_file_uri()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new("native-async-local-file-provider")?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;

    let provider = DeltaTableProvider::try_new_with_execution_options(
        source,
        preflight,
        None,
        DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::NativeAsync,
            1,
            1,
        )?,
    )?;

    assert_eq!(provider.source_name(), "orders");

    Ok(())
}

#[test]
fn native_async_backend_does_not_claim_exact_filter_pushdown()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema(
        "native-async-no-exact-filter-pushdown",
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
    let provider = DeltaTableProvider::try_new_with_execution_options(
        source,
        preflight,
        None,
        DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::NativeAsync,
            1,
            1,
        )?,
    )?;
    let filter =
        datafusion::logical_expr::col("region").eq(datafusion::logical_expr::lit("us-west"));

    assert_eq!(
        provider.supports_filters_pushdown(&[&filter])?,
        vec![TableProviderFilterPushDown::Unsupported]
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_returns_projected_non_reading_plan()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new("table-provider-scan-projection")?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let projection = vec![1];

    let plan = provider
        .scan(&state, Some(&projection), &[], Some(10))
        .await?;
    let delta_plan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;

    assert_eq!(plan.schema().fields().len(), 1);
    assert_eq!(plan.schema().field(0).name(), "customer_name");
    assert_eq!(delta_plan.scan_plan().source_name, "orders");
    assert_eq!(delta_plan.scan_plan().scan_projection, Some(vec![1]));

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_without_projection_returns_full_non_reading_plan()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new("table-provider-full-scan")?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();

    let plan = provider.scan(&state, None, &[], None).await?;
    let delta_plan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;

    assert_eq!(plan.schema().fields().len(), 2);
    assert_eq!(plan.schema().field(0).name(), "id");
    assert_eq!(plan.schema().field(1).name(), "customer_name");
    assert_eq!(delta_plan.scan_plan().scan_projection, None);

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_records_session_target_but_uses_auto_file_task_grouping()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_sized_adds(
        "table-provider-target-partitions",
        DEFAULT_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[
            (r#""partitionValues":{}"#, 90),
            (r#""partitionValues":{}"#, 10),
            (r#""partitionValues":{}"#, 10),
            (r#""partitionValues":{}"#, 10),
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
    let state = ctx.state();

    let plan = provider.scan(&state, None, &[], None).await?;
    let delta_plan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;

    let decision = delta_plan.partition_target_decision();
    assert_eq!(decision.datafusion_target_partitions, Some(2));
    assert!(matches!(
        decision.source,
        DeltaScanPartitionTargetSource::AvailableParallelismFallback
            | DeltaScanPartitionTargetSource::StaticFallback
    ));
    assert_eq!(
        plan.properties().output_partitioning().partition_count(),
        delta_plan.partition_plan().partitions.len()
    );
    assert!(!delta_plan.partition_plan().partitions.is_empty());
    assert!(
        delta_plan
            .partition_plan()
            .partitions
            .iter()
            .all(|partition| !partition.file_tasks.is_empty())
    );
    let file_paths = scan_partition_file_paths(delta_plan)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(
        file_paths,
        vec![
            "part-00000.parquet".to_owned(),
            "part-00001.parquet".to_owned(),
            "part-00002.parquet".to_owned(),
            "part-00003.parquet".to_owned(),
        ]
    );
    assert_eq!(delta_plan.partition_plan().estimated_bytes, Some(120));

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_uses_explicit_delta_target_override()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_sized_adds(
        "table-provider-explicit-target-override",
        DEFAULT_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[
            (r#""partitionValues":{}"#, 10),
            (r#""partitionValues":{}"#, 10),
            (r#""partitionValues":{}"#, 10),
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider =
        DeltaTableProvider::try_new_with_scan_target_partitions(source, preflight, Some(3))?;
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(8));
    let state = ctx.state();

    let plan = provider.scan(&state, None, &[], None).await?;
    let delta_plan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;

    let decision = delta_plan.partition_target_decision();
    assert_eq!(decision.target_partitions, 3);
    assert_eq!(decision.explicit_target_partitions, Some(3));
    assert_eq!(decision.datafusion_target_partitions, Some(8));
    assert_eq!(
        decision.source,
        DeltaScanPartitionTargetSource::ExplicitOverride
    );
    assert_eq!(decision.applied_caps.datafusion_target_partitions, None);
    assert_eq!(delta_plan.partition_plan().partitions.len(), 3);
    assert_eq!(
        scan_partition_file_paths(delta_plan),
        vec![
            vec!["part-00000.parquet".to_owned()],
            vec!["part-00001.parquet".to_owned()],
            vec!["part-00002.parquet".to_owned()],
        ]
    );

    Ok(())
}

#[tokio::test]
async fn registered_table_provider_scan_uses_configured_delta_target_override()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(8));
    let table = DeltaLogTable::new_with_schema_and_sized_adds(
        "registered-provider-explicit-target-override",
        DEFAULT_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[
            (r#""partitionValues":{}"#, 10),
            (r#""partitionValues":{}"#, 10),
            (r#""partitionValues":{}"#, 10),
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: Some(3),
        }],
    )?;

    let dataframe = ctx.sql("select id from orders").await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let mut scans = Vec::new();
    find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

    assert_eq!(scans.len(), 1);
    let decision = scans[0].partition_target_decision();
    assert_eq!(decision.target_partitions, 3);
    assert_eq!(decision.explicit_target_partitions, Some(3));
    assert_eq!(decision.datafusion_target_partitions, Some(8));
    assert_eq!(
        decision.source,
        DeltaScanPartitionTargetSource::ExplicitOverride
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_does_not_create_empty_partitions_when_target_exceeds_files()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_sized_adds(
        "table-provider-target-exceeds-files",
        DEFAULT_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[
            (r#""partitionValues":{}"#, 20),
            (r#""partitionValues":{}"#, 20),
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(8));
    let state = ctx.state();

    let plan = provider.scan(&state, None, &[], None).await?;
    let delta_plan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;

    assert_eq!(plan.properties().output_partitioning().partition_count(), 2);
    assert_eq!(delta_plan.partition_plan().partitions.len(), 2);
    assert_eq!(
        scan_partition_file_paths(delta_plan),
        vec![
            vec!["part-00000.parquet".to_owned()],
            vec!["part-00001.parquet".to_owned()],
        ]
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_with_no_active_files_reports_zero_partitions()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-empty-partition-plan",
        DEFAULT_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    let state = ctx.state();

    let plan = provider.scan(&state, None, &[], None).await?;
    let delta_plan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;
    let plan_display = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(true)
        .to_string();

    assert_eq!(plan.properties().output_partitioning().partition_count(), 0);
    assert!(delta_plan.partition_plan().partitions.is_empty());
    assert!(plan_display.contains("partitions=0"), "{plan_display}");

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_exec_carries_direct_partition_execution_handoff()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_sized_adds(
        "table-provider-direct-partition-execution-handoff",
        DEFAULT_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[
            (r#""partitionValues":{}"#, 64),
            (r#""partitionValues":{}"#, 32),
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
    let state = ctx.state();
    let projection = vec![1];

    let plan = provider.scan(&state, Some(&projection), &[], None).await?;
    let delta_plan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;
    let scan_plan = delta_plan.scan_plan();
    let partition_plan = delta_plan.partition_plan();

    assert_eq!(scan_plan.source_name, "orders");
    assert_eq!(scan_plan.snapshot_version, 1);
    assert_eq!(scan_plan.scan_projection, Some(vec![1]));
    assert_eq!(scan_plan.projected_schema.fields().len(), 1);
    assert_eq!(scan_plan.projected_schema.field(0).name(), "customer_name");
    assert_eq!(partition_plan.source_name, scan_plan.source_name);
    assert_eq!(partition_plan.table_uri, scan_plan.table_uri);
    assert_eq!(partition_plan.snapshot_version, scan_plan.snapshot_version);
    assert!(partition_plan.scan_metadata_exhausted);
    let decision = delta_plan.partition_target_decision();
    assert_eq!(decision.datafusion_target_partitions, Some(2));
    assert!(matches!(
        decision.source,
        DeltaScanPartitionTargetSource::AvailableParallelismFallback
            | DeltaScanPartitionTargetSource::StaticFallback
    ));
    assert_eq!(partition_plan.partitions.len(), 2);
    assert_eq!(partition_plan.estimated_bytes, Some(96));

    let file_tasks = partition_plan
        .partitions
        .iter()
        .flat_map(|partition| partition.file_tasks.iter())
        .collect::<Vec<_>>();
    assert_eq!(file_tasks.len(), 2);
    assert_eq!(file_tasks[0].source_name, "orders");
    assert_eq!(file_tasks[0].table_uri, scan_plan.table_uri);
    assert_eq!(file_tasks[0].snapshot_version, 1);
    assert_eq!(file_tasks[0].path, "part-00000.parquet");
    assert_eq!(file_tasks[0].estimated_bytes, Some(64));
    assert_eq!(file_tasks[1].path, "part-00001.parquet");
    assert_eq!(file_tasks[1].estimated_bytes, Some(32));

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_rejects_invalid_projection_before_execution()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new("table-provider-invalid-projection")?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let projection = vec![2];

    let result = provider.scan(&state, Some(&projection), &[], None).await;

    assert!(
        matches!(result, Err(DataFusionError::External(error)) if error
        .to_string()
        .contains("projection index 2 is out of bounds"))
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_rejects_duplicate_projection_at_public_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new("table-provider-duplicate-projection")?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let projection = vec![1, 1];

    let result = provider.scan(&state, Some(&projection), &[], None).await;

    assert!(
        matches!(result, Err(DataFusionError::External(error)) if error
        .to_string()
        .contains("projection index 1 is duplicated"))
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_rejects_unsupported_pushed_filter()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new("table-provider-filter-injection")?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let filter = datafusion::logical_expr::col("id")
        .in_list(vec![datafusion::logical_expr::lit(7_i32)], false);

    let result = provider.scan(&state, None, &[filter], None).await;

    assert!(
        matches!(result, Err(DataFusionError::External(error)) if error
        .to_string()
        .contains("pushed filters must be exact partition predicates"))
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_accepts_exact_partition_equality_filter()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-exact-partition-filter",
        PARTITIONED_SCHEMA_FIELDS_JSON,
        r#"["region"]"#,
        &[
            r#""partitionValues":{"region":"us-west"}"#,
            r#""partitionValues":{"region":"us-east"}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let filter =
        datafusion::logical_expr::col("region").eq(datafusion::logical_expr::lit("us-west"));

    let plan = provider.scan(&state, None, &[filter], None).await?;
    let scan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;

    assert_eq!(scan.scan_plan().pushed_filter_plan.exact_count, 1);
    assert_eq!(scan.scan_plan().pushed_filter_plan.unsupported_count, 0);
    assert_eq!(scan.scan_plan().pushed_filter_plan.residual_filter_count, 0);
    assert_eq!(scan.scan_plan().pushed_filter_plan.pushed_filter_count, 1);
    assert!(scan.scan_plan().kernel_partition_predicate.is_some());
    assert_eq!(scan_file_paths(scan)?, vec!["part-00000.parquet"]);
    assert_eq!(
        scan_partition_file_paths(scan),
        vec![vec!["part-00000.parquet".to_owned()]]
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_accepts_exact_partition_in_filter()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-exact-partition-in-filter",
        PARTITIONED_SCHEMA_FIELDS_JSON,
        r#"["region"]"#,
        &[
            r#""partitionValues":{"region":"us-west"}"#,
            r#""partitionValues":{"region":"us-east"}"#,
            r#""partitionValues":{"region":"eu-central"}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let filter = datafusion::logical_expr::col("region").in_list(
        vec![
            datafusion::logical_expr::lit("us-west"),
            datafusion::logical_expr::lit("us-east"),
        ],
        false,
    );

    let plan = provider.scan(&state, None, &[filter], None).await?;
    let scan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;

    assert_eq!(scan.scan_plan().pushed_filter_plan.exact_count, 1);
    assert_eq!(scan.scan_plan().pushed_filter_plan.unsupported_count, 0);
    assert_eq!(scan.scan_plan().pushed_filter_plan.residual_filter_count, 0);
    assert_eq!(scan.scan_plan().pushed_filter_plan.pushed_filter_count, 1);
    assert!(scan.scan_plan().kernel_partition_predicate.is_some());
    assert_eq!(
        scan_file_paths(scan)?,
        vec!["part-00000.parquet", "part-00001.parquet"]
    );
    assert_eq!(
        scan_partition_file_paths(scan),
        vec![
            vec!["part-00000.parquet".to_owned()],
            vec!["part-00001.parquet".to_owned()]
        ]
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_accepts_inexact_integer_data_stats_filter()
-> Result<(), Box<dyn std::error::Error>> {
    let possible_stats = id_stats_add_json(10, 101, 150, 0);
    let impossible_stats = id_stats_add_json(10, 1, 50, 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-integer-data-stats-filter",
        DEFAULT_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[
            impossible_stats.as_str(),
            possible_stats.as_str(),
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let filter = datafusion::logical_expr::col("id").gt(datafusion::logical_expr::lit(100_i32));
    assert_eq!(
        provider.supports_filters_pushdown(&[&filter])?,
        vec![TableProviderFilterPushDown::Inexact]
    );

    let plan = provider
        .scan(&state, Some(&vec![0]), &[filter], None)
        .await?;
    let scan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;

    assert_eq!(scan.scan_plan().pushed_filter_plan.exact_count, 0);
    assert_eq!(scan.scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(scan.scan_plan().pushed_filter_plan.residual_filter_count, 1);
    assert!(scan.scan_plan().kernel_partition_predicate.is_some());
    assert_eq!(
        scan_file_paths(scan)?,
        vec!["part-00001.parquet", "part-00002.parquet"]
    );

    Ok(())
}

#[test]
fn datafusion_provider_preflight_allows_deletion_vectors_before_stats_pushdown()
-> Result<(), Box<dyn std::error::Error>> {
    let stats = id_stats_add_json(10, 101, 150, 0);
    let table = DeltaLogTable::new_with_schema_protocol_and_adds(
        "provider-dv-stats-preflight-rejection",
        DELETION_VECTOR_PROTOCOL_JSON,
        DEFAULT_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[stats.as_str()],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;

    let preflight = preflight_delta_protocol(&source)?;

    assert_eq!(
        preflight.protocol().reader_features,
        vec!["deletionVectors"]
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_combines_exact_partition_and_integer_data_stats_filters()
-> Result<(), Box<dyn std::error::Error>> {
    let west_impossible_stats =
        partitioned_id_stats_add_json(r#"{"region":"us-west"}"#, 10, 1, 50, 0);
    let west_possible_stats =
        partitioned_id_stats_add_json(r#"{"region":"us-west"}"#, 10, 101, 150, 0);
    let east_possible_stats =
        partitioned_id_stats_add_json(r#"{"region":"us-east"}"#, 10, 101, 150, 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-partition-and-integer-data-stats",
        PARTITIONED_SCHEMA_FIELDS_JSON,
        r#"["region"]"#,
        &[
            west_impossible_stats.as_str(),
            west_possible_stats.as_str(),
            east_possible_stats.as_str(),
            r#""partitionValues":{"region":"us-west"}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let partition_filter =
        datafusion::logical_expr::col("region").eq(datafusion::logical_expr::lit("us-west"));
    let stats_filter =
        datafusion::logical_expr::col("id").gt(datafusion::logical_expr::lit(100_i32));
    assert_eq!(
        provider.supports_filters_pushdown(&[&partition_filter, &stats_filter])?,
        vec![
            TableProviderFilterPushDown::Exact,
            TableProviderFilterPushDown::Inexact,
        ]
    );

    let plan = provider
        .scan(
            &state,
            Some(&vec![0, 1]),
            &[partition_filter, stats_filter],
            None,
        )
        .await?;
    let scan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;

    assert_eq!(scan.scan_plan().pushed_filter_plan.exact_count, 1);
    assert_eq!(scan.scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(scan.scan_plan().pushed_filter_plan.unsupported_count, 0);
    assert_eq!(scan.scan_plan().pushed_filter_plan.residual_filter_count, 1);
    assert!(scan.scan_plan().kernel_partition_predicate.is_some());
    assert_eq!(
        scan_file_paths(scan)?,
        vec!["part-00001.parquet", "part-00003.parquet"]
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_uses_top_level_and_partition_and_integer_data_stats_filter()
-> Result<(), Box<dyn std::error::Error>> {
    let west_impossible_stats =
        partitioned_id_stats_add_json(r#"{"region":"us-west"}"#, 10, 1, 50, 0);
    let west_possible_stats =
        partitioned_id_stats_add_json(r#"{"region":"us-west"}"#, 10, 101, 150, 0);
    let east_possible_stats =
        partitioned_id_stats_add_json(r#"{"region":"us-east"}"#, 10, 101, 150, 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-top-level-and-partition-integer-data-stats",
        PARTITIONED_SCHEMA_FIELDS_JSON,
        r#"["region"]"#,
        &[
            west_impossible_stats.as_str(),
            west_possible_stats.as_str(),
            east_possible_stats.as_str(),
            r#""partitionValues":{"region":"us-west"}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let filter = datafusion::logical_expr::col("region")
        .eq(datafusion::logical_expr::lit("us-west"))
        .and(datafusion::logical_expr::col("id").gt(datafusion::logical_expr::lit(100_i32)));
    assert_eq!(
        provider.supports_filters_pushdown(&[&filter])?,
        vec![TableProviderFilterPushDown::Inexact]
    );

    let plan = provider
        .scan(&state, Some(&vec![0, 1]), &[filter], None)
        .await?;
    let scan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;

    assert_eq!(scan.scan_plan().pushed_filter_plan.exact_count, 0);
    assert_eq!(scan.scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(scan.scan_plan().pushed_filter_plan.unsupported_count, 0);
    assert_eq!(scan.scan_plan().pushed_filter_plan.residual_filter_count, 1);
    assert!(scan.scan_plan().kernel_partition_predicate.is_some());
    assert_eq!(
        scan_file_paths(scan)?,
        vec!["part-00001.parquet", "part-00003.parquet"]
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_uses_top_level_and_integer_data_stats_with_data_residual_filter()
-> Result<(), Box<dyn std::error::Error>> {
    let possible_stats = id_stats_add_json(10, 101, 150, 0);
    let impossible_stats = id_stats_add_json(10, 1, 50, 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-top-level-and-integer-data-stats-data-residual",
        DEFAULT_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[
            impossible_stats.as_str(),
            possible_stats.as_str(),
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let filter = datafusion::logical_expr::col("id")
        .gt(datafusion::logical_expr::lit(100_i32))
        .and(
            datafusion::logical_expr::col("customer_name")
                .eq(datafusion::logical_expr::lit("alice")),
        );
    assert_eq!(
        provider.supports_filters_pushdown(&[&filter])?,
        vec![TableProviderFilterPushDown::Inexact]
    );

    let plan = provider
        .scan(&state, Some(&vec![0, 1]), &[filter], None)
        .await?;
    let scan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;

    assert_eq!(scan.scan_plan().pushed_filter_plan.exact_count, 0);
    assert_eq!(scan.scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(scan.scan_plan().pushed_filter_plan.unsupported_count, 0);
    assert_eq!(scan.scan_plan().pushed_filter_plan.residual_filter_count, 1);
    assert!(scan.scan_plan().kernel_partition_predicate.is_some());
    assert_eq!(
        scan_file_paths(scan)?,
        vec!["part-00001.parquet", "part-00002.parquet"]
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_rejects_projected_integer_data_stats_filter()
-> Result<(), Box<dyn std::error::Error>> {
    let stats = id_stats_add_json(10, 1, 50, 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-projected-integer-data-stats-filter",
        DEFAULT_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[stats.as_str()],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let projection = vec![1];
    let filter = datafusion::logical_expr::col("id").gt(datafusion::logical_expr::lit(7_i32));

    let result = provider
        .scan(&state, Some(&projection), &[filter], None)
        .await;

    assert!(
        matches!(result, Err(DataFusionError::External(error)) if error
            .to_string()
            .contains("inexact pushed filter residual columns must be projected"))
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_uses_integer_stats_pruning_for_supported_operators()
-> Result<(), Box<dyn std::error::Error>> {
    let low_stats = id_stats_add_json(10, 1, 5, 0);
    let target_stats = id_stats_add_json(10, 7, 7, 0);
    let high_stats = id_stats_add_json(10, 8, 10, 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-integer-data-stats-operators",
        DEFAULT_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[
            low_stats.as_str(),
            target_stats.as_str(),
            high_stats.as_str(),
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let cases = [
        (
            "equals",
            datafusion::logical_expr::col("id").eq(datafusion::logical_expr::lit(7_i32)),
            vec!["part-00001.parquet", "part-00003.parquet"],
        ),
        (
            "not equals",
            datafusion::logical_expr::col("id").not_eq(datafusion::logical_expr::lit(7_i32)),
            vec![
                "part-00000.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
            ],
        ),
        (
            "less than",
            datafusion::logical_expr::col("id").lt(datafusion::logical_expr::lit(7_i32)),
            vec!["part-00000.parquet", "part-00003.parquet"],
        ),
        (
            "less than or equal",
            datafusion::logical_expr::col("id").lt_eq(datafusion::logical_expr::lit(7_i32)),
            vec![
                "part-00000.parquet",
                "part-00001.parquet",
                "part-00003.parquet",
            ],
        ),
        (
            "greater than",
            datafusion::logical_expr::col("id").gt(datafusion::logical_expr::lit(7_i32)),
            vec!["part-00002.parquet", "part-00003.parquet"],
        ),
        (
            "greater than or equal",
            datafusion::logical_expr::col("id").gt_eq(datafusion::logical_expr::lit(7_i32)),
            vec![
                "part-00001.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
            ],
        ),
    ];

    for (name, filter, expected_paths) in cases {
        assert_eq!(
            provider.supports_filters_pushdown(&[&filter])?,
            vec![TableProviderFilterPushDown::Inexact],
            "{name}"
        );

        let plan = provider
            .scan(&state, Some(&vec![0]), &[filter], None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        assert_eq!(
            scan.scan_plan().pushed_filter_plan.inexact_count,
            1,
            "{name}"
        );
        assert_eq!(
            scan.scan_plan().pushed_filter_plan.residual_filter_count,
            1,
            "{name}"
        );
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_uses_integer_stats_pruning_for_not_equals_null_boundaries()
-> Result<(), Box<dyn std::error::Error>> {
    let all_literal_no_null_stats = id_stats_add_json(10, 7, 7, 0);
    let mixed_value_stats = id_stats_add_json(10, 7, 8, 0);
    let all_literal_with_null_stats = id_stats_add_json(10, 7, 7, 1);
    let missing_null_count_stats = id_stats_add_json_with_optional_counts(Some(10), 7, 7, None);
    let missing_num_records_stats = id_stats_add_json_with_optional_counts(None, 7, 7, Some(0));
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-integer-data-stats-not-equals-null-boundaries",
        DEFAULT_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[
            all_literal_no_null_stats.as_str(),
            mixed_value_stats.as_str(),
            all_literal_with_null_stats.as_str(),
            missing_null_count_stats.as_str(),
            missing_num_records_stats.as_str(),
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let filter = datafusion::logical_expr::col("id").not_eq(datafusion::logical_expr::lit(7_i32));
    assert_eq!(
        provider.supports_filters_pushdown(&[&filter])?,
        vec![TableProviderFilterPushDown::Inexact]
    );

    let plan = provider
        .scan(&state, Some(&vec![0]), &[filter], None)
        .await?;
    let scan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;

    assert_eq!(scan.scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(scan.scan_plan().pushed_filter_plan.residual_filter_count, 1);
    assert_eq!(scan_file_paths(scan)?, vec!["part-00001.parquet"]);

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_uses_decimal_stats_pruning_for_supported_operators()
-> Result<(), Box<dyn std::error::Error>> {
    let low_stats = decimal_stats_add_json(10, "-1.23", "-1.23", 0);
    let target_stats = decimal_stats_add_json(10, "2.00", "2.00", 0);
    let high_stats = decimal_stats_add_json(10, "10.00", "10.00", 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-decimal-data-stats-operators",
        DECIMAL_DATA_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[
            low_stats.as_str(),
            target_stats.as_str(),
            high_stats.as_str(),
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let target = Expr::Literal(ScalarValue::Decimal128(Some(200), 10, 2), None);
    let high = Expr::Literal(ScalarValue::Decimal128(Some(1_000), 10, 2), None);
    let cases = [
        (
            "equals",
            datafusion::logical_expr::col("amount").eq(target.clone()),
            vec!["part-00001.parquet", "part-00003.parquet"],
        ),
        (
            "not equals",
            datafusion::logical_expr::col("amount").not_eq(target.clone()),
            vec![
                "part-00000.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
            ],
        ),
        (
            "less than",
            datafusion::logical_expr::col("amount").lt(target.clone()),
            vec!["part-00000.parquet", "part-00003.parquet"],
        ),
        (
            "less than or equal",
            datafusion::logical_expr::col("amount").lt_eq(target.clone()),
            vec![
                "part-00000.parquet",
                "part-00001.parquet",
                "part-00003.parquet",
            ],
        ),
        (
            "greater than",
            datafusion::logical_expr::col("amount").gt(target.clone()),
            vec!["part-00002.parquet", "part-00003.parquet"],
        ),
        (
            "greater than or equal",
            datafusion::logical_expr::col("amount").gt_eq(target.clone()),
            vec![
                "part-00001.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
            ],
        ),
        (
            "greater than reversed",
            high.gt(datafusion::logical_expr::col("amount")),
            vec![
                "part-00000.parquet",
                "part-00001.parquet",
                "part-00003.parquet",
            ],
        ),
    ];

    for (name, filter, expected_paths) in cases {
        assert_eq!(
            provider.supports_filters_pushdown(&[&filter])?,
            vec![TableProviderFilterPushDown::Inexact],
            "{name}"
        );

        let plan = provider
            .scan(&state, Some(&vec![0, 1]), &[filter], None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        assert_eq!(
            scan.scan_plan().pushed_filter_plan.inexact_count,
            1,
            "{name}"
        );
        assert_eq!(
            scan.scan_plan().pushed_filter_plan.residual_filter_count,
            1,
            "{name}"
        );
        assert!(scan.scan_plan().kernel_partition_predicate.is_some());
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_uses_decimal_null_count_stats_pruning()
-> Result<(), Box<dyn std::error::Error>> {
    let non_null_stats = decimal_stats_add_json(10, "2.00", "2.00", 0);
    let all_null_stats = decimal_partial_stats_add_json(10, Some(10));
    let with_null_stats = decimal_stats_add_json(10, "2.00", "2.00", 2);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-decimal-null-count-data-stats",
        DECIMAL_DATA_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[
            non_null_stats.as_str(),
            all_null_stats.as_str(),
            with_null_stats.as_str(),
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let cases = [
        (
            "is null",
            datafusion::logical_expr::col("amount").is_null(),
            vec![
                "part-00001.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
            ],
        ),
        (
            "is not null",
            datafusion::logical_expr::col("amount").is_not_null(),
            vec![
                "part-00000.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
            ],
        ),
    ];

    for (name, filter, expected_paths) in cases {
        assert_eq!(
            provider.supports_filters_pushdown(&[&filter])?,
            vec![TableProviderFilterPushDown::Inexact],
            "{name}"
        );

        let plan = provider
            .scan(&state, Some(&vec![0, 1]), &[filter], None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        assert_eq!(
            scan.scan_plan().pushed_filter_plan.inexact_count,
            1,
            "{name}"
        );
        assert_eq!(
            scan.scan_plan().pushed_filter_plan.residual_filter_count,
            1,
            "{name}"
        );
        assert!(scan.scan_plan().kernel_partition_predicate.is_some());
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_uses_string_stats_pruning_for_supported_operators()
-> Result<(), Box<dyn std::error::Error>> {
    let low_stats = string_stats_add_json(10, "Alice", "Alice", 0);
    let target_stats = string_stats_add_json(10, "alice", "alice", 0);
    let high_stats = string_stats_add_json(10, "zed", "zed", 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-string-data-stats-operators",
        DEFAULT_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[
            low_stats.as_str(),
            target_stats.as_str(),
            high_stats.as_str(),
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let target = datafusion::logical_expr::lit("alice");
    let high = datafusion::logical_expr::lit("m");
    let cases = [
        (
            "equals",
            datafusion::logical_expr::col("customer_name").eq(target.clone()),
            vec!["part-00001.parquet", "part-00003.parquet"],
        ),
        (
            "not equals",
            datafusion::logical_expr::col("customer_name").not_eq(target.clone()),
            vec![
                "part-00000.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
            ],
        ),
        (
            "less than",
            datafusion::logical_expr::col("customer_name").lt(target.clone()),
            vec!["part-00000.parquet", "part-00003.parquet"],
        ),
        (
            "less than or equal",
            datafusion::logical_expr::col("customer_name").lt_eq(target.clone()),
            vec![
                "part-00000.parquet",
                "part-00001.parquet",
                "part-00003.parquet",
            ],
        ),
        (
            "greater than",
            datafusion::logical_expr::col("customer_name").gt(target.clone()),
            vec!["part-00002.parquet", "part-00003.parquet"],
        ),
        (
            "greater than or equal",
            datafusion::logical_expr::col("customer_name").gt_eq(target.clone()),
            vec![
                "part-00001.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
            ],
        ),
        (
            "greater than reversed",
            high.gt(datafusion::logical_expr::col("customer_name")),
            vec![
                "part-00000.parquet",
                "part-00001.parquet",
                "part-00003.parquet",
            ],
        ),
    ];

    for (name, filter, expected_paths) in cases {
        assert_eq!(
            provider.supports_filters_pushdown(&[&filter])?,
            vec![TableProviderFilterPushDown::Inexact],
            "{name}"
        );

        let plan = provider
            .scan(&state, Some(&vec![0, 1]), &[filter], None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        assert_eq!(
            scan.scan_plan().pushed_filter_plan.inexact_count,
            1,
            "{name}"
        );
        assert_eq!(
            scan.scan_plan().pushed_filter_plan.residual_filter_count,
            1,
            "{name}"
        );
        assert!(scan.scan_plan().kernel_partition_predicate.is_some());
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_uses_string_null_count_stats_pruning()
-> Result<(), Box<dyn std::error::Error>> {
    let non_null_stats = string_stats_add_json(10, "alice", "alice", 0);
    let all_null_stats = string_partial_stats_add_json(10, Some(10));
    let with_null_stats = string_stats_add_json(10, "alice", "alice", 2);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-string-null-count-data-stats",
        DEFAULT_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[
            non_null_stats.as_str(),
            all_null_stats.as_str(),
            with_null_stats.as_str(),
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let cases = [
        (
            "is null",
            datafusion::logical_expr::col("customer_name").is_null(),
            vec![
                "part-00001.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
            ],
        ),
        (
            "is not null",
            datafusion::logical_expr::col("customer_name").is_not_null(),
            vec![
                "part-00000.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
            ],
        ),
    ];

    for (name, filter, expected_paths) in cases {
        assert_eq!(
            provider.supports_filters_pushdown(&[&filter])?,
            vec![TableProviderFilterPushDown::Inexact],
            "{name}"
        );

        let plan = provider
            .scan(&state, Some(&vec![0, 1]), &[filter], None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        assert_eq!(
            scan.scan_plan().pushed_filter_plan.inexact_count,
            1,
            "{name}"
        );
        assert_eq!(
            scan.scan_plan().pushed_filter_plan.residual_filter_count,
            1,
            "{name}"
        );
        assert!(scan.scan_plan().kernel_partition_predicate.is_some());
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_uses_floating_stats_pruning_for_supported_operators()
-> Result<(), Box<dyn std::error::Error>> {
    let low_stats = floating_stats_add_json(10, "-1.5", "-1.5", "-2.25", "-2.25", 0);
    let target_stats = floating_stats_add_json(10, "1.5", "1.5", "2.25", "2.25", 0);
    let range_stats = floating_stats_add_json(10, "-1.0", "2.0", "-2.0", "3.0", 0);
    let high_stats = floating_stats_add_json(10, "10.0", "10.0", "10.0", "10.0", 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-floating-data-stats-operators",
        FLOATING_DATA_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[
            low_stats.as_str(),
            target_stats.as_str(),
            range_stats.as_str(),
            high_stats.as_str(),
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let float_target = Expr::Literal(ScalarValue::Float32(Some(1.5)), None);
    let double_target = Expr::Literal(ScalarValue::Float64(Some(2.25)), None);
    let cases = [
        (
            "equals",
            datafusion::logical_expr::col("float_score").eq(float_target.clone()),
            vec![
                "part-00001.parquet",
                "part-00002.parquet",
                "part-00004.parquet",
            ],
        ),
        (
            "not equals",
            datafusion::logical_expr::col("float_score").not_eq(float_target.clone()),
            vec![
                "part-00000.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
                "part-00004.parquet",
            ],
        ),
        (
            "less than",
            datafusion::logical_expr::col("float_score").lt(float_target.clone()),
            vec![
                "part-00000.parquet",
                "part-00002.parquet",
                "part-00004.parquet",
            ],
        ),
        (
            "less than or equal",
            datafusion::logical_expr::col("float_score").lt_eq(float_target.clone()),
            vec![
                "part-00000.parquet",
                "part-00001.parquet",
                "part-00002.parquet",
                "part-00004.parquet",
            ],
        ),
        (
            "greater than",
            datafusion::logical_expr::col("float_score").gt(float_target.clone()),
            vec![
                "part-00002.parquet",
                "part-00003.parquet",
                "part-00004.parquet",
            ],
        ),
        (
            "greater than or equal",
            datafusion::logical_expr::col("float_score").gt_eq(float_target.clone()),
            vec![
                "part-00001.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
                "part-00004.parquet",
            ],
        ),
        (
            "greater than reversed",
            double_target.gt(datafusion::logical_expr::col("double_score")),
            vec![
                "part-00000.parquet",
                "part-00002.parquet",
                "part-00004.parquet",
            ],
        ),
    ];

    for (name, filter, expected_paths) in cases {
        assert_eq!(
            provider.supports_filters_pushdown(&[&filter])?,
            vec![TableProviderFilterPushDown::Inexact],
            "{name}"
        );

        let plan = provider
            .scan(&state, Some(&vec![0, 1, 2]), &[filter], None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        assert_eq!(
            scan.scan_plan().pushed_filter_plan.inexact_count,
            1,
            "{name}"
        );
        assert_eq!(
            scan.scan_plan().pushed_filter_plan.residual_filter_count,
            1,
            "{name}"
        );
        assert!(scan.scan_plan().kernel_partition_predicate.is_some());
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_uses_floating_null_count_stats_pruning()
-> Result<(), Box<dyn std::error::Error>> {
    let non_null_stats = floating_stats_add_json(10, "1.5", "1.5", "2.25", "2.25", 0);
    let all_null_stats = floating_partial_stats_add_json(10, Some(10));
    let with_null_stats = floating_stats_add_json(10, "1.5", "1.5", "2.25", "2.25", 2);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-floating-null-count-data-stats",
        FLOATING_DATA_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[
            non_null_stats.as_str(),
            all_null_stats.as_str(),
            with_null_stats.as_str(),
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let cases = [
        (
            "is null",
            datafusion::logical_expr::col("float_score").is_null(),
            vec![
                "part-00001.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
            ],
        ),
        (
            "is not null",
            datafusion::logical_expr::col("double_score").is_not_null(),
            vec![
                "part-00000.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
            ],
        ),
    ];

    for (name, filter, expected_paths) in cases {
        assert_eq!(
            provider.supports_filters_pushdown(&[&filter])?,
            vec![TableProviderFilterPushDown::Inexact],
            "{name}"
        );

        let plan = provider
            .scan(&state, Some(&vec![0, 1, 2]), &[filter], None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        assert_eq!(
            scan.scan_plan().pushed_filter_plan.inexact_count,
            1,
            "{name}"
        );
        assert_eq!(
            scan.scan_plan().pushed_filter_plan.residual_filter_count,
            1,
            "{name}"
        );
        assert!(scan.scan_plan().kernel_partition_predicate.is_some());
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_rejects_unproven_floating_data_stats_shapes()
-> Result<(), Box<dyn std::error::Error>> {
    let stats = floating_stats_add_json(10, "-0.0", "-0.0", "-0.0", "-0.0", 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-floating-data-stats-unsupported",
        FLOATING_DATA_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[stats.as_str()],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let zero = Expr::Literal(ScalarValue::Float32(Some(0.0)), None);
    let filter = datafusion::logical_expr::col("float_score").eq(zero);

    let result = provider
        .scan(&state, Some(&vec![0, 1, 2]), &[filter], None)
        .await;

    assert!(
        matches!(result, Err(DataFusionError::External(error)) if error
            .to_string()
            .contains("pushed filters must be exact partition predicates"))
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_rejects_projected_floating_data_stats_filter()
-> Result<(), Box<dyn std::error::Error>> {
    let stats = floating_stats_add_json(10, "1.5", "1.5", "2.25", "2.25", 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-projected-floating-data-stats-filter",
        FLOATING_DATA_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[stats.as_str()],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let projection = vec![0];
    let float_value = Expr::Literal(ScalarValue::Float32(Some(1.5)), None);
    let filter = datafusion::logical_expr::col("float_score").eq(float_value);

    let result = provider
        .scan(&state, Some(&projection), &[filter], None)
        .await;

    assert!(
        matches!(result, Err(DataFusionError::External(error)) if error
            .to_string()
            .contains("inexact pushed filter residual columns must be projected"))
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_accepts_projected_floating_data_stats_when_residual_columns_are_projected()
-> Result<(), Box<dyn std::error::Error>> {
    let stats = floating_stats_add_json(10, "1.5", "1.5", "2.25", "2.25", 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-projected-floating-data-stats-filter-with-residual",
        FLOATING_DATA_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[stats.as_str()],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let projection = vec![0, 1];
    let float_value = Expr::Literal(ScalarValue::Float32(Some(1.5)), None);
    let filter = datafusion::logical_expr::col("float_score").eq(float_value);

    let plan = provider
        .scan(&state, Some(&projection), &[filter], None)
        .await?;
    let scan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;

    assert_eq!(scan.scan_plan().scan_projection, Some(vec![0, 1]));
    assert_eq!(scan.scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(scan.scan_plan().pushed_filter_plan.residual_filter_count, 1);
    assert!(scan.scan_plan().kernel_partition_predicate.is_some());
    assert_eq!(scan_file_paths(scan)?, vec!["part-00000.parquet"]);

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_uses_top_level_and_partition_and_floating_data_stats_filter()
-> Result<(), Box<dyn std::error::Error>> {
    const MIXED_FLOATING_STATS_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"float_score\",\"type\":\"float\",\"nullable\":true,\"metadata\":{}},{\"name\":\"double_score\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;

    let west_low_stats = partitioned_floating_stats_add_json(
        r#"{"region":"us-west"}"#,
        10,
        "0.5",
        "0.5",
        "0.5",
        "0.5",
        0,
    );
    let west_target_stats = partitioned_floating_stats_add_json(
        r#"{"region":"us-west"}"#,
        10,
        "1.5",
        "1.5",
        "2.25",
        "2.25",
        0,
    );
    let east_target_stats = partitioned_floating_stats_add_json(
        r#"{"region":"us-east"}"#,
        10,
        "1.5",
        "1.5",
        "2.25",
        "2.25",
        0,
    );
    let west_missing_stats = r#""partitionValues":{"region":"us-west"}"#;
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-top-level-and-partition-floating-data-stats",
        MIXED_FLOATING_STATS_SCHEMA_FIELDS_JSON,
        r#"["region"]"#,
        &[
            west_low_stats.as_str(),
            west_target_stats.as_str(),
            east_target_stats.as_str(),
            west_missing_stats,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let float_value = Expr::Literal(ScalarValue::Float32(Some(1.5)), None);
    let filter = datafusion::logical_expr::col("region")
        .eq(datafusion::logical_expr::lit("us-west"))
        .and(datafusion::logical_expr::col("float_score").eq(float_value));
    assert_eq!(
        provider.supports_filters_pushdown(&[&filter])?,
        vec![TableProviderFilterPushDown::Inexact]
    );

    let plan = provider.scan(&state, None, &[filter], None).await?;
    let scan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;

    assert_eq!(scan.scan_plan().pushed_filter_plan.exact_count, 0);
    assert_eq!(scan.scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(scan.scan_plan().pushed_filter_plan.unsupported_count, 0);
    assert_eq!(scan.scan_plan().pushed_filter_plan.residual_filter_count, 1);
    assert!(scan.scan_plan().kernel_partition_predicate.is_some());
    assert_eq!(
        scan_file_paths(scan)?,
        vec!["part-00001.parquet", "part-00003.parquet"]
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_keeps_partial_string_bounds_uncertain()
-> Result<(), Box<dyn std::error::Error>> {
    let full_impossible_stats = string_stats_add_json(10, "aaron", "bob", 0);
    let min_only_stats = string_partial_bounds_stats_add_json(10, Some("aaron"), None, Some(0));
    let max_only_stats = string_partial_bounds_stats_add_json(10, None, Some("bob"), Some(0));
    let counts_only_stats = string_partial_bounds_stats_add_json(10, None, None, Some(0));
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-string-partial-bounds-data-stats",
        DEFAULT_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[
            full_impossible_stats.as_str(),
            min_only_stats.as_str(),
            max_only_stats.as_str(),
            counts_only_stats.as_str(),
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let filter =
        datafusion::logical_expr::col("customer_name").eq(datafusion::logical_expr::lit("carol"));
    assert_eq!(
        provider.supports_filters_pushdown(&[&filter])?,
        vec![TableProviderFilterPushDown::Inexact]
    );

    let plan = provider
        .scan(&state, Some(&vec![0, 1]), &[filter], None)
        .await?;
    let scan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;

    assert_eq!(scan.scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(scan.scan_plan().pushed_filter_plan.residual_filter_count, 1);
    assert_eq!(
        scan_file_paths(scan)?,
        vec![
            "part-00001.parquet",
            "part-00003.parquet",
            "part-00004.parquet"
        ]
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_rejects_projected_string_data_stats_filter()
-> Result<(), Box<dyn std::error::Error>> {
    let stats = string_stats_add_json(10, "alice", "alice", 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-projected-string-data-stats-filter",
        DEFAULT_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[stats.as_str()],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let projection = vec![0];
    let filter =
        datafusion::logical_expr::col("customer_name").eq(datafusion::logical_expr::lit("alice"));

    let result = provider
        .scan(&state, Some(&projection), &[filter], None)
        .await;

    assert!(
        matches!(result, Err(DataFusionError::External(error)) if error
            .to_string()
            .contains("inexact pushed filter residual columns must be projected"))
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_accepts_projected_string_data_stats_when_residual_columns_are_projected()
-> Result<(), Box<dyn std::error::Error>> {
    let stats = string_stats_add_json(10, "alice", "alice", 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-projected-string-data-stats-filter-with-residual",
        DEFAULT_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[stats.as_str()],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let projection = vec![0, 1];
    let filter =
        datafusion::logical_expr::col("customer_name").eq(datafusion::logical_expr::lit("alice"));

    let plan = provider
        .scan(&state, Some(&projection), &[filter], None)
        .await?;
    let scan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;

    assert_eq!(scan.scan_plan().scan_projection, Some(vec![0, 1]));
    assert_eq!(scan.scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(scan.scan_plan().pushed_filter_plan.residual_filter_count, 1);
    assert!(scan.scan_plan().kernel_partition_predicate.is_some());
    assert_eq!(scan_file_paths(scan)?, vec!["part-00000.parquet"]);

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_uses_top_level_and_partition_and_string_data_stats_filter()
-> Result<(), Box<dyn std::error::Error>> {
    const MIXED_STRING_STATS_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;

    let west_low_stats =
        partitioned_string_stats_add_json(r#"{"region":"us-west"}"#, 10, "bob", "bob", 0);
    let west_target_stats =
        partitioned_string_stats_add_json(r#"{"region":"us-west"}"#, 10, "alice", "alice", 0);
    let east_target_stats =
        partitioned_string_stats_add_json(r#"{"region":"us-east"}"#, 10, "alice", "alice", 0);
    let west_missing_stats = r#""partitionValues":{"region":"us-west"}"#;
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-top-level-and-partition-string-data-stats",
        MIXED_STRING_STATS_SCHEMA_FIELDS_JSON,
        r#"["region"]"#,
        &[
            west_low_stats.as_str(),
            west_target_stats.as_str(),
            east_target_stats.as_str(),
            west_missing_stats,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let filter = datafusion::logical_expr::col("region")
        .eq(datafusion::logical_expr::lit("us-west"))
        .and(
            datafusion::logical_expr::col("customer_name")
                .eq(datafusion::logical_expr::lit("alice")),
        );
    assert_eq!(
        provider.supports_filters_pushdown(&[&filter])?,
        vec![TableProviderFilterPushDown::Inexact]
    );

    let plan = provider.scan(&state, None, &[filter], None).await?;
    let scan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;

    assert_eq!(scan.scan_plan().pushed_filter_plan.exact_count, 0);
    assert_eq!(scan.scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(scan.scan_plan().pushed_filter_plan.unsupported_count, 0);
    assert_eq!(scan.scan_plan().pushed_filter_plan.residual_filter_count, 1);
    assert!(scan.scan_plan().kernel_partition_predicate.is_some());
    assert_eq!(
        scan_file_paths(scan)?,
        vec!["part-00001.parquet", "part-00003.parquet"]
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_rejects_unproven_string_data_stats_shapes()
-> Result<(), Box<dyn std::error::Error>> {
    let stats = string_stats_add_json(10, "alice", "alice", 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-string-data-stats-unsupported",
        DEFAULT_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[stats.as_str()],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let filter =
        datafusion::logical_expr::col("customer_name").like(datafusion::logical_expr::lit("a%"));

    let result = provider
        .scan(&state, Some(&vec![0, 1]), &[filter], None)
        .await;

    assert!(
        matches!(result, Err(DataFusionError::External(error)) if error
            .to_string()
            .contains("pushed filters must be exact partition predicates"))
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_rejects_unproven_decimal_data_stats_shapes()
-> Result<(), Box<dyn std::error::Error>> {
    let stats = decimal_stats_add_json(10, "2.00", "2.00", 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-decimal-data-stats-unsupported",
        DECIMAL_DATA_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[stats.as_str()],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let precision_mismatch = Expr::Literal(ScalarValue::Decimal128(Some(200), 11, 2), None);
    let filter = datafusion::logical_expr::col("amount").eq(precision_mismatch);

    let result = provider
        .scan(&state, Some(&vec![0, 1]), &[filter], None)
        .await;

    assert!(
        matches!(result, Err(DataFusionError::External(error)) if error
            .to_string()
            .contains("pushed filters must be exact partition predicates"))
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_rejects_projected_decimal_data_stats_filter()
-> Result<(), Box<dyn std::error::Error>> {
    let stats = decimal_stats_add_json(10, "2.00", "2.00", 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-projected-decimal-data-stats-filter",
        DECIMAL_DATA_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[stats.as_str()],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let projection = vec![0];
    let amount = Expr::Literal(ScalarValue::Decimal128(Some(200), 10, 2), None);
    let filter = datafusion::logical_expr::col("amount").eq(amount);

    let result = provider
        .scan(&state, Some(&projection), &[filter], None)
        .await;

    assert!(
        matches!(result, Err(DataFusionError::External(error)) if error
            .to_string()
            .contains("inexact pushed filter residual columns must be projected"))
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_accepts_projected_decimal_data_stats_when_residual_columns_are_projected()
-> Result<(), Box<dyn std::error::Error>> {
    let stats = decimal_stats_add_json(10, "2.00", "2.00", 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-projected-decimal-data-stats-filter-with-residual",
        DECIMAL_DATA_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[stats.as_str()],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let projection = vec![0, 1];
    let amount = Expr::Literal(ScalarValue::Decimal128(Some(200), 10, 2), None);
    let filter = datafusion::logical_expr::col("amount").eq(amount);

    let plan = provider
        .scan(&state, Some(&projection), &[filter], None)
        .await?;
    let scan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;

    assert_eq!(scan.scan_plan().scan_projection, Some(vec![0, 1]));
    assert_eq!(scan.scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(scan.scan_plan().pushed_filter_plan.residual_filter_count, 1);
    assert!(scan.scan_plan().kernel_partition_predicate.is_some());
    assert_eq!(scan_file_paths(scan)?, vec!["part-00000.parquet"]);

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_uses_top_level_and_partition_and_decimal_data_stats_filter()
-> Result<(), Box<dyn std::error::Error>> {
    const MIXED_DECIMAL_STATS_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"amount\",\"type\":\"decimal(10,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;

    let west_low_stats =
        partitioned_decimal_stats_add_json(r#"{"region":"us-west"}"#, 10, "0.00", "0.00", 0);
    let west_target_stats =
        partitioned_decimal_stats_add_json(r#"{"region":"us-west"}"#, 10, "2.00", "2.00", 0);
    let east_target_stats =
        partitioned_decimal_stats_add_json(r#"{"region":"us-east"}"#, 10, "2.00", "2.00", 0);
    let west_missing_stats = r#""partitionValues":{"region":"us-west"}"#;
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-top-level-and-partition-decimal-data-stats",
        MIXED_DECIMAL_STATS_SCHEMA_FIELDS_JSON,
        r#"["region"]"#,
        &[
            west_low_stats.as_str(),
            west_target_stats.as_str(),
            east_target_stats.as_str(),
            west_missing_stats,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let amount = Expr::Literal(ScalarValue::Decimal128(Some(200), 10, 2), None);
    let filter = datafusion::logical_expr::col("region")
        .eq(datafusion::logical_expr::lit("us-west"))
        .and(datafusion::logical_expr::col("amount").eq(amount));
    assert_eq!(
        provider.supports_filters_pushdown(&[&filter])?,
        vec![TableProviderFilterPushDown::Inexact]
    );

    let plan = provider.scan(&state, None, &[filter], None).await?;
    let scan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;

    assert_eq!(scan.scan_plan().pushed_filter_plan.exact_count, 0);
    assert_eq!(scan.scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(scan.scan_plan().pushed_filter_plan.unsupported_count, 0);
    assert_eq!(scan.scan_plan().pushed_filter_plan.residual_filter_count, 1);
    assert!(scan.scan_plan().kernel_partition_predicate.is_some());
    assert_eq!(
        scan_file_paths(scan)?,
        vec!["part-00001.parquet", "part-00003.parquet"]
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_uses_boolean_null_count_stats_pruning()
-> Result<(), Box<dyn std::error::Error>> {
    let false_only_stats = boolean_stats_add_json(10, false, false, 0);
    let true_only_stats = boolean_stats_add_json(10, true, true, 0);
    let mixed_stats = boolean_stats_add_json(10, false, true, 0);
    let all_null_stats = boolean_partial_stats_add_json(10, None, None, Some(10));
    let false_with_null_stats = boolean_stats_add_json(10, false, false, 2);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-boolean-null-count-data-stats",
        BOOLEAN_DATA_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[
            false_only_stats.as_str(),
            true_only_stats.as_str(),
            mixed_stats.as_str(),
            all_null_stats.as_str(),
            false_with_null_stats.as_str(),
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let cases = [
        (
            "is null",
            datafusion::logical_expr::col("is_current").is_null(),
            vec![
                "part-00003.parquet",
                "part-00004.parquet",
                "part-00005.parquet",
            ],
        ),
        (
            "is not null",
            datafusion::logical_expr::col("is_current").is_not_null(),
            vec![
                "part-00000.parquet",
                "part-00001.parquet",
                "part-00002.parquet",
                "part-00004.parquet",
                "part-00005.parquet",
            ],
        ),
    ];

    for (name, filter, expected_paths) in cases {
        assert_eq!(
            provider.supports_filters_pushdown(&[&filter])?,
            vec![TableProviderFilterPushDown::Inexact],
            "{name}"
        );

        let plan = provider
            .scan(&state, Some(&vec![0, 1]), &[filter], None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        assert_eq!(
            scan.scan_plan().pushed_filter_plan.inexact_count,
            1,
            "{name}"
        );
        assert_eq!(
            scan.scan_plan().pushed_filter_plan.residual_filter_count,
            1,
            "{name}"
        );
        assert!(scan.scan_plan().kernel_partition_predicate.is_some());
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_keeps_missing_boolean_null_count_uncertain()
-> Result<(), Box<dyn std::error::Error>> {
    let counts_only_stats = boolean_partial_stats_add_json(10, None, None, Some(0));
    let min_max_only_stats = boolean_partial_stats_add_json(10, Some(false), Some(true), None);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-boolean-null-count-partial",
        BOOLEAN_DATA_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[
            counts_only_stats.as_str(),
            min_max_only_stats.as_str(),
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let filter = datafusion::logical_expr::col("is_current").is_null();
    assert_eq!(
        provider.supports_filters_pushdown(&[&filter])?,
        vec![TableProviderFilterPushDown::Inexact]
    );

    let plan = provider
        .scan(&state, Some(&vec![0, 1]), &[filter], None)
        .await?;
    let scan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;

    assert_eq!(scan.scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(scan.scan_plan().pushed_filter_plan.residual_filter_count, 1);
    assert_eq!(
        scan_file_paths(scan)?,
        vec!["part-00001.parquet", "part-00002.parquet"]
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_uses_binary_null_count_stats_pruning()
-> Result<(), Box<dyn std::error::Error>> {
    let non_null_stats = binary_partial_stats_add_json(10, Some(0));
    let all_null_stats = binary_partial_stats_add_json(10, Some(10));
    let with_null_stats = binary_partial_stats_add_json(10, Some(2));
    let missing_null_count_stats = binary_partial_stats_add_json(10, None);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-binary-null-count-data-stats",
        BINARY_DATA_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[
            non_null_stats.as_str(),
            all_null_stats.as_str(),
            with_null_stats.as_str(),
            missing_null_count_stats.as_str(),
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let cases = [
        (
            "is null",
            datafusion::logical_expr::col("payload").is_null(),
            vec![
                "part-00001.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
                "part-00004.parquet",
            ],
        ),
        (
            "is not null",
            datafusion::logical_expr::col("payload").is_not_null(),
            vec![
                "part-00000.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
                "part-00004.parquet",
            ],
        ),
    ];

    for (name, filter, expected_paths) in cases {
        assert_eq!(
            provider.supports_filters_pushdown(&[&filter])?,
            vec![TableProviderFilterPushDown::Inexact],
            "{name}"
        );

        let plan = provider
            .scan(&state, Some(&vec![0, 1]), &[filter], None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        assert_eq!(
            scan.scan_plan().pushed_filter_plan.inexact_count,
            1,
            "{name}"
        );
        assert_eq!(
            scan.scan_plan().pushed_filter_plan.residual_filter_count,
            1,
            "{name}"
        );
        assert!(scan.scan_plan().kernel_partition_predicate.is_some());
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_rejects_binary_data_stats_comparators()
-> Result<(), Box<dyn std::error::Error>> {
    let stats = binary_partial_stats_add_json(10, Some(0));
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-binary-data-stats-comparator-unsupported",
        BINARY_DATA_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[stats.as_str()],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let payload = Expr::Literal(ScalarValue::Binary(Some(b"hello".to_vec())), None);
    let filter = datafusion::logical_expr::col("payload").eq(payload);

    assert_eq!(
        provider.supports_filters_pushdown(&[&filter])?,
        vec![TableProviderFilterPushDown::Unsupported]
    );

    let result = provider
        .scan(&state, Some(&vec![0, 1]), &[filter], None)
        .await;

    assert!(
        matches!(result, Err(DataFusionError::External(error)) if error
            .to_string()
            .contains("pushed filters must be exact partition predicates"))
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_rejects_unsupported_data_stats_matrix_entries()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new("table-provider-unsupported-data-stats-matrix")?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let mut provider = DeltaTableProvider::try_new(source, preflight)?;
    provider.set_schema_for_tests(Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("amount256", DataType::Decimal256(38, 18), true),
        Field::new(
            "event_ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("customer_name", DataType::Utf8, true),
        Field::new("payload", DataType::Binary, true),
        Field::new(
            "profile",
            DataType::Struct(vec![Field::new("age", DataType::Int32, true)].into()),
            true,
        ),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "properties",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Utf8, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        ),
        Field::new(
            "dict_name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new("unsigned_count", DataType::UInt32, true),
    ])));
    let state = SessionContext::new().state();
    let decimal256 = Expr::Literal(ScalarValue::Decimal256(Some(12_345.into()), 38, 18), None);
    let timestamp = Expr::Literal(
        ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_456), Some("UTC".into())),
        None,
    );
    let binary = Expr::Literal(ScalarValue::Binary(Some(b"hello".to_vec())), None);
    let filters = [
        (
            "integer null count",
            datafusion::logical_expr::col("id").is_null(),
        ),
        (
            "decimal256 equality",
            datafusion::logical_expr::col("amount256").eq(decimal256),
        ),
        (
            "timestamp inequality",
            datafusion::logical_expr::col("event_ts").not_eq(timestamp),
        ),
        (
            "string membership",
            datafusion::logical_expr::col("customer_name")
                .in_list(vec![datafusion::logical_expr::lit("alice")], false),
        ),
        (
            "binary equality",
            datafusion::logical_expr::col("payload").eq(binary),
        ),
        (
            "struct null count",
            datafusion::logical_expr::col("profile").is_null(),
        ),
        (
            "list null count",
            datafusion::logical_expr::col("tags").is_null(),
        ),
        (
            "map null count",
            datafusion::logical_expr::col("properties").is_null(),
        ),
        (
            "dictionary null count",
            datafusion::logical_expr::col("dict_name").is_null(),
        ),
        (
            "unsigned equality",
            datafusion::logical_expr::col("unsigned_count")
                .eq(datafusion::logical_expr::lit(7_u32)),
        ),
    ];

    for (name, filter) in filters {
        assert_eq!(
            provider.supports_filters_pushdown(&[&filter])?,
            vec![TableProviderFilterPushDown::Unsupported],
            "{name}"
        );

        let result = provider
            .scan(&state, Some(&vec![0]), std::slice::from_ref(&filter), None)
            .await;

        assert!(
            matches!(result, Err(DataFusionError::External(error)) if error
                .to_string()
                .contains("pushed filters must be exact partition predicates")),
            "{name} should be rejected before kernel scan planning"
        );
    }

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_rejects_projected_boolean_data_stats_filter()
-> Result<(), Box<dyn std::error::Error>> {
    let stats = boolean_stats_add_json(10, false, false, 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-projected-boolean-data-stats-filter",
        BOOLEAN_DATA_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[stats.as_str()],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let projection = vec![0];
    let filter = datafusion::logical_expr::col("is_current").is_not_null();

    let result = provider
        .scan(&state, Some(&projection), &[filter], None)
        .await;

    assert!(
        matches!(result, Err(DataFusionError::External(error)) if error
            .to_string()
            .contains("inexact pushed filter residual columns must be projected"))
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_accepts_projected_boolean_data_stats_when_residual_columns_are_projected()
-> Result<(), Box<dyn std::error::Error>> {
    let stats = boolean_stats_add_json(10, false, false, 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-projected-boolean-data-stats-filter-with-residual",
        BOOLEAN_DATA_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[stats.as_str()],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let projection = vec![0, 1];
    let filter = datafusion::logical_expr::col("is_current").is_not_null();

    let plan = provider
        .scan(&state, Some(&projection), &[filter], None)
        .await?;
    let scan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;

    assert_eq!(scan.scan_plan().scan_projection, Some(vec![0, 1]));
    assert_eq!(scan.scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(scan.scan_plan().pushed_filter_plan.residual_filter_count, 1);
    assert!(scan.scan_plan().kernel_partition_predicate.is_some());
    assert_eq!(scan_file_paths(scan)?, vec!["part-00000.parquet"]);

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_uses_top_level_and_partition_and_boolean_data_stats_filter()
-> Result<(), Box<dyn std::error::Error>> {
    const MIXED_BOOLEAN_STATS_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"is_current\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;

    let west_not_null_stats =
        partitioned_boolean_stats_add_json(r#"{"region":"us-west"}"#, 10, false, true, 0);
    let west_all_null_stats =
        partitioned_boolean_partial_stats_add_json(r#"{"region":"us-west"}"#, 10, Some(10));
    let east_not_null_stats =
        partitioned_boolean_stats_add_json(r#"{"region":"us-east"}"#, 10, false, true, 0);
    let west_missing_stats = r#""partitionValues":{"region":"us-west"}"#;
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-top-level-and-partition-boolean-data-stats",
        MIXED_BOOLEAN_STATS_SCHEMA_FIELDS_JSON,
        r#"["region"]"#,
        &[
            west_not_null_stats.as_str(),
            west_all_null_stats.as_str(),
            east_not_null_stats.as_str(),
            west_missing_stats,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let filter = datafusion::logical_expr::col("region")
        .eq(datafusion::logical_expr::lit("us-west"))
        .and(datafusion::logical_expr::col("is_current").is_not_null());
    assert_eq!(
        provider.supports_filters_pushdown(&[&filter])?,
        vec![TableProviderFilterPushDown::Inexact]
    );

    let plan = provider.scan(&state, None, &[filter], None).await?;
    let scan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;

    assert_eq!(scan.scan_plan().pushed_filter_plan.exact_count, 0);
    assert_eq!(scan.scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(scan.scan_plan().pushed_filter_plan.unsupported_count, 0);
    assert_eq!(scan.scan_plan().pushed_filter_plan.residual_filter_count, 1);
    assert!(scan.scan_plan().kernel_partition_predicate.is_some());
    assert_eq!(
        scan_file_paths(scan)?,
        vec!["part-00000.parquet", "part-00003.parquet"]
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_uses_date_stats_pruning_for_supported_operators()
-> Result<(), Box<dyn std::error::Error>> {
    let pre_epoch_stats = date_stats_add_json(10, "1969-12-31", "1969-12-31", 0);
    let leap_stats = date_stats_add_json(10, "2024-02-29", "2024-02-29", 0);
    let target_stats = date_stats_add_json(10, "2026-01-01", "2026-01-01", 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-date-data-stats-operators",
        DATE_DATA_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[
            pre_epoch_stats.as_str(),
            leap_stats.as_str(),
            target_stats.as_str(),
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let target = Expr::Literal(ScalarValue::Date32(Some(20_454)), None);
    let leap = Expr::Literal(ScalarValue::Date32(Some(19_782)), None);
    let cases = [
        (
            "equals",
            datafusion::logical_expr::col("event_date").eq(target.clone()),
            vec!["part-00002.parquet", "part-00003.parquet"],
        ),
        (
            "not equals",
            datafusion::logical_expr::col("event_date").not_eq(target.clone()),
            vec![
                "part-00000.parquet",
                "part-00001.parquet",
                "part-00003.parquet",
            ],
        ),
        (
            "greater than",
            datafusion::logical_expr::col("event_date").gt(leap.clone()),
            vec!["part-00002.parquet", "part-00003.parquet"],
        ),
        (
            "less than reversed",
            target.gt(datafusion::logical_expr::col("event_date")),
            vec![
                "part-00000.parquet",
                "part-00001.parquet",
                "part-00003.parquet",
            ],
        ),
    ];

    for (name, filter, expected_paths) in cases {
        assert_eq!(
            provider.supports_filters_pushdown(&[&filter])?,
            vec![TableProviderFilterPushDown::Inexact],
            "{name}"
        );

        let plan = provider
            .scan(&state, Some(&vec![0, 1]), &[filter], None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        assert_eq!(
            scan.scan_plan().pushed_filter_plan.inexact_count,
            1,
            "{name}"
        );
        assert_eq!(
            scan.scan_plan().pushed_filter_plan.residual_filter_count,
            1,
            "{name}"
        );
        assert!(scan.scan_plan().kernel_partition_predicate.is_some());
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_uses_timestamp_stats_pruning_for_supported_operators()
-> Result<(), Box<dyn std::error::Error>> {
    let low_stats = timestamp_stats_add_json(
        "event_ts",
        10,
        "2025-12-31T23:59:59.999999Z",
        "2025-12-31T23:59:59.999999Z",
        0,
    );
    let target_stats = timestamp_stats_add_json(
        "event_ts",
        10,
        "2026-01-01T00:00:00.123456Z",
        "2026-01-01T00:00:00.123456Z",
        0,
    );
    let high_stats = timestamp_stats_add_json(
        "event_ts",
        10,
        "2026-01-01T00:00:00.123457Z",
        "2026-01-01T00:00:00.123457Z",
        0,
    );
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-timestamp-data-stats-operators",
        TIMESTAMP_DATA_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[
            low_stats.as_str(),
            target_stats.as_str(),
            high_stats.as_str(),
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let target = Expr::Literal(
        ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_456), Some("UTC".into())),
        None,
    );
    let high = Expr::Literal(
        ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_457), Some("UTC".into())),
        None,
    );
    let cases = [
        (
            "equals",
            datafusion::logical_expr::col("event_ts").eq(target.clone()),
            vec!["part-00001.parquet", "part-00003.parquet"],
        ),
        (
            "less than",
            datafusion::logical_expr::col("event_ts").lt(target.clone()),
            vec!["part-00000.parquet", "part-00003.parquet"],
        ),
        (
            "greater than reversed",
            high.gt(datafusion::logical_expr::col("event_ts")),
            vec![
                "part-00000.parquet",
                "part-00001.parquet",
                "part-00003.parquet",
            ],
        ),
    ];

    for (name, filter, expected_paths) in cases {
        assert_eq!(
            provider.supports_filters_pushdown(&[&filter])?,
            vec![TableProviderFilterPushDown::Inexact],
            "{name}"
        );

        let plan = provider
            .scan(&state, Some(&vec![0, 1]), &[filter], None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        assert_eq!(
            scan.scan_plan().pushed_filter_plan.inexact_count,
            1,
            "{name}"
        );
        assert_eq!(
            scan.scan_plan().pushed_filter_plan.residual_filter_count,
            1,
            "{name}"
        );
        assert!(scan.scan_plan().kernel_partition_predicate.is_some());
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_uses_timestamp_ntz_stats_pruning_for_supported_operators()
-> Result<(), Box<dyn std::error::Error>> {
    let low_stats = timestamp_stats_add_json(
        "event_ts_ntz",
        10,
        "2025-12-31 23:59:59.999999",
        "2025-12-31 23:59:59.999999",
        0,
    );
    let target_stats = timestamp_stats_add_json(
        "event_ts_ntz",
        10,
        "2026-01-01 00:00:00.123456",
        "2026-01-01 00:00:00.123456",
        0,
    );
    let high_stats = timestamp_stats_add_json(
        "event_ts_ntz",
        10,
        "2026-01-01 00:00:00.123457",
        "2026-01-01 00:00:00.123457",
        0,
    );
    let table = DeltaLogTable::new_with_schema_protocol_and_adds(
        "table-provider-timestamp-ntz-data-stats-operators",
        TIMESTAMP_NTZ_PROTOCOL_JSON,
        TIMESTAMP_NTZ_DATA_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[
            low_stats.as_str(),
            target_stats.as_str(),
            high_stats.as_str(),
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let target = Expr::Literal(
        ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_456), None),
        None,
    );
    let high = Expr::Literal(
        ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_457), None),
        None,
    );
    let cases = [
        (
            "equals",
            datafusion::logical_expr::col("event_ts_ntz").eq(target.clone()),
            vec!["part-00001.parquet", "part-00003.parquet"],
        ),
        (
            "less than",
            datafusion::logical_expr::col("event_ts_ntz").lt(target.clone()),
            vec!["part-00000.parquet", "part-00003.parquet"],
        ),
        (
            "greater than reversed",
            high.gt(datafusion::logical_expr::col("event_ts_ntz")),
            vec![
                "part-00000.parquet",
                "part-00001.parquet",
                "part-00003.parquet",
            ],
        ),
    ];

    for (name, filter, expected_paths) in cases {
        assert_eq!(
            provider.supports_filters_pushdown(&[&filter])?,
            vec![TableProviderFilterPushDown::Inexact],
            "{name}"
        );

        let plan = provider
            .scan(&state, Some(&vec![0, 1]), &[filter], None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        assert_eq!(
            scan.scan_plan().pushed_filter_plan.inexact_count,
            1,
            "{name}"
        );
        assert_eq!(
            scan.scan_plan().pushed_filter_plan.residual_filter_count,
            1,
            "{name}"
        );
        assert!(scan.scan_plan().kernel_partition_predicate.is_some());
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_uses_temporal_null_count_stats_pruning()
-> Result<(), Box<dyn std::error::Error>> {
    let non_null_stats = date_stats_add_json(10, "2026-01-01", "2026-01-01", 0);
    let all_null_stats = temporal_partial_stats_add_json("event_date", 10, Some(10));
    let with_null_stats = date_stats_add_json(10, "2026-01-01", "2026-01-01", 2);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-temporal-null-count-data-stats",
        DATE_DATA_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[
            non_null_stats.as_str(),
            all_null_stats.as_str(),
            with_null_stats.as_str(),
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let cases = [
        (
            "is null",
            datafusion::logical_expr::col("event_date").is_null(),
            vec![
                "part-00001.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
            ],
        ),
        (
            "is not null",
            datafusion::logical_expr::col("event_date").is_not_null(),
            vec![
                "part-00000.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
            ],
        ),
    ];

    for (name, filter, expected_paths) in cases {
        assert_eq!(
            provider.supports_filters_pushdown(&[&filter])?,
            vec![TableProviderFilterPushDown::Inexact],
            "{name}"
        );

        let plan = provider
            .scan(&state, Some(&vec![0, 1]), &[filter], None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        assert_eq!(
            scan.scan_plan().pushed_filter_plan.inexact_count,
            1,
            "{name}"
        );
        assert_eq!(
            scan.scan_plan().pushed_filter_plan.residual_filter_count,
            1,
            "{name}"
        );
        assert!(scan.scan_plan().kernel_partition_predicate.is_some());
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_rejects_unproven_temporal_data_stats_shapes()
-> Result<(), Box<dyn std::error::Error>> {
    let stats = timestamp_stats_add_json(
        "event_ts",
        10,
        "2026-01-01T00:00:00.123456Z",
        "2026-01-01T00:00:00.123456Z",
        0,
    );
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-temporal-data-stats-unsupported",
        TIMESTAMP_DATA_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[stats.as_str()],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let timestamp = Expr::Literal(
        ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_456), Some("UTC".into())),
        None,
    );
    let filter = datafusion::logical_expr::col("event_ts").not_eq(timestamp);

    let result = provider
        .scan(&state, Some(&vec![0, 1]), &[filter], None)
        .await;

    assert!(
        matches!(result, Err(DataFusionError::External(error)) if error
            .to_string()
            .contains("pushed filters must be exact partition predicates"))
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_uses_top_level_and_partition_and_temporal_data_stats_filter()
-> Result<(), Box<dyn std::error::Error>> {
    const MIXED_TEMPORAL_STATS_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"event_date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;

    let west_low_stats = partitioned_date_stats_add_json(
        r#"{"region":"us-west"}"#,
        10,
        "2024-02-29",
        "2024-02-29",
        0,
    );
    let west_target_stats = partitioned_date_stats_add_json(
        r#"{"region":"us-west"}"#,
        10,
        "2026-01-01",
        "2026-01-01",
        0,
    );
    let east_target_stats = partitioned_date_stats_add_json(
        r#"{"region":"us-east"}"#,
        10,
        "2026-01-01",
        "2026-01-01",
        0,
    );
    let west_missing_stats = r#""partitionValues":{"region":"us-west"}"#;
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-top-level-and-partition-temporal-data-stats",
        MIXED_TEMPORAL_STATS_SCHEMA_FIELDS_JSON,
        r#"["region"]"#,
        &[
            west_low_stats.as_str(),
            west_target_stats.as_str(),
            east_target_stats.as_str(),
            west_missing_stats,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let date = Expr::Literal(ScalarValue::Date32(Some(20_454)), None);
    let filter = datafusion::logical_expr::col("region")
        .eq(datafusion::logical_expr::lit("us-west"))
        .and(datafusion::logical_expr::col("event_date").eq(date));
    assert_eq!(
        provider.supports_filters_pushdown(&[&filter])?,
        vec![TableProviderFilterPushDown::Inexact]
    );

    let plan = provider.scan(&state, None, &[filter], None).await?;
    let scan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;

    assert_eq!(scan.scan_plan().pushed_filter_plan.exact_count, 0);
    assert_eq!(scan.scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(scan.scan_plan().pushed_filter_plan.unsupported_count, 0);
    assert_eq!(scan.scan_plan().pushed_filter_plan.residual_filter_count, 1);
    assert!(scan.scan_plan().kernel_partition_predicate.is_some());
    assert_eq!(
        scan_file_paths(scan)?,
        vec!["part-00001.parquet", "part-00003.parquet"]
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_rejects_projected_temporal_data_stats_filter()
-> Result<(), Box<dyn std::error::Error>> {
    let stats = date_stats_add_json(10, "2026-01-01", "2026-01-01", 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-projected-temporal-data-stats-filter",
        DATE_DATA_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[stats.as_str()],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let projection = vec![0];
    let date = Expr::Literal(ScalarValue::Date32(Some(20_454)), None);
    let filter = datafusion::logical_expr::col("event_date").eq(date);

    let result = provider
        .scan(&state, Some(&projection), &[filter], None)
        .await;

    assert!(
        matches!(result, Err(DataFusionError::External(error)) if error
            .to_string()
            .contains("inexact pushed filter residual columns must be projected"))
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_accepts_projected_temporal_data_stats_when_residual_columns_are_projected()
-> Result<(), Box<dyn std::error::Error>> {
    let stats = date_stats_add_json(10, "2026-01-01", "2026-01-01", 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-projected-temporal-data-stats-filter-with-residual",
        DATE_DATA_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[stats.as_str()],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let projection = vec![0, 1];
    let date = Expr::Literal(ScalarValue::Date32(Some(20_454)), None);
    let filter = datafusion::logical_expr::col("event_date").eq(date);

    let plan = provider
        .scan(&state, Some(&projection), &[filter], None)
        .await?;
    let scan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;

    assert_eq!(scan.scan_plan().scan_projection, Some(vec![0, 1]));
    assert_eq!(scan.scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(scan.scan_plan().pushed_filter_plan.residual_filter_count, 1);
    assert!(scan.scan_plan().kernel_partition_predicate.is_some());
    assert_eq!(scan_file_paths(scan)?, vec!["part-00000.parquet"]);

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_keeps_partial_integer_stats_uncertain()
-> Result<(), Box<dyn std::error::Error>> {
    let max_only_low_stats = id_partial_stats_add_json(10, None, Some(5), 0);
    let min_only_high_stats = id_partial_stats_add_json(10, Some(8), None, 0);
    let max_only_high_stats = id_partial_stats_add_json(10, None, Some(10), 0);
    let min_only_low_stats = id_partial_stats_add_json(10, Some(1), None, 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-integer-data-stats-partial",
        DEFAULT_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[
            max_only_low_stats.as_str(),
            min_only_high_stats.as_str(),
            max_only_high_stats.as_str(),
            min_only_low_stats.as_str(),
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let cases = [
        (
            "greater than",
            datafusion::logical_expr::col("id").gt(datafusion::logical_expr::lit(7_i32)),
            vec![
                "part-00001.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
                "part-00004.parquet",
            ],
        ),
        (
            "less than",
            datafusion::logical_expr::col("id").lt(datafusion::logical_expr::lit(7_i32)),
            vec![
                "part-00000.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
                "part-00004.parquet",
            ],
        ),
    ];

    for (name, filter, expected_paths) in cases {
        assert_eq!(
            provider.supports_filters_pushdown(&[&filter])?,
            vec![TableProviderFilterPushDown::Inexact],
            "{name}"
        );

        let plan = provider
            .scan(&state, Some(&vec![0]), &[filter], None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        assert_eq!(
            scan.scan_plan().pushed_filter_plan.inexact_count,
            1,
            "{name}"
        );
        assert_eq!(
            scan.scan_plan().pushed_filter_plan.residual_filter_count,
            1,
            "{name}"
        );
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_keeps_invalid_integer_stats_uncertain()
-> Result<(), Box<dyn std::error::Error>> {
    let impossible_stats = id_stats_add_json(10, 1, 5, 0);
    let schema_mismatch_stats = stats_add_json(
        r#"{\"numRecords\":10,\"minValues\":{\"id\":\"low\"},\"maxValues\":{\"id\":\"high\"},\"nullCount\":{\"id\":0}}"#,
    );
    let malformed_stats = stats_add_json(r#"not-json"#);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-integer-data-stats-invalid",
        DEFAULT_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[
            impossible_stats.as_str(),
            schema_mismatch_stats.as_str(),
            malformed_stats.as_str(),
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let filter = datafusion::logical_expr::col("id").gt(datafusion::logical_expr::lit(7_i32));
    assert_eq!(
        provider.supports_filters_pushdown(&[&filter])?,
        vec![TableProviderFilterPushDown::Inexact]
    );

    let plan = provider
        .scan(&state, Some(&vec![0]), &[filter], None)
        .await?;
    let scan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;

    assert_eq!(scan.scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(scan.scan_plan().pushed_filter_plan.residual_filter_count, 1);
    let paths = scan_file_paths(scan)?;
    assert!(paths.contains(&"part-00001.parquet".to_owned()));
    assert!(paths.contains(&"part-00002.parquet".to_owned()));

    Ok(())
}

#[tokio::test]
async fn exact_string_partition_predicates_use_kernel_pruning()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "exact-string-partition-kernel-pruning",
        PARTITIONED_SCHEMA_FIELDS_JSON,
        r#"["region"]"#,
        &[
            r#""partitionValues":{"region":"us-west"}"#,
            r#""partitionValues":{"region":"us-east"}"#,
            r#""partitionValues":{"region":null}"#,
            r#""partitionValues":{"region":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let cases = vec![
        (
            "single non-empty value",
            datafusion::logical_expr::col("region").eq(datafusion::logical_expr::lit("us-west")),
            vec!["part-00000.parquet"],
        ),
        (
            "empty value equality",
            datafusion::logical_expr::col("region").eq(datafusion::logical_expr::lit("")),
            Vec::new(),
        ),
        (
            "is null",
            datafusion::logical_expr::col("region").is_null(),
            vec![
                "part-00002.parquet",
                "part-00003.parquet",
                "part-00004.parquet",
            ],
        ),
        (
            "is not null",
            datafusion::logical_expr::col("region").is_not_null(),
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
        (
            "inequality non-empty value",
            datafusion::logical_expr::col("region")
                .not_eq(datafusion::logical_expr::lit("us-west")),
            vec!["part-00001.parquet"],
        ),
        (
            "inequality empty value",
            datafusion::logical_expr::col("region").not_eq(datafusion::logical_expr::lit("")),
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
        (
            "in non-empty values",
            datafusion::logical_expr::col("region").in_list(
                vec![
                    datafusion::logical_expr::lit("us-west"),
                    datafusion::logical_expr::lit("us-east"),
                    datafusion::logical_expr::lit("us-west"),
                ],
                false,
            ),
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
        (
            "in with empty literal",
            datafusion::logical_expr::col("region").in_list(
                vec![
                    datafusion::logical_expr::lit("us-west"),
                    datafusion::logical_expr::lit(""),
                ],
                false,
            ),
            vec!["part-00000.parquet"],
        ),
        (
            "empty in",
            datafusion::logical_expr::col("region").in_list(Vec::<Expr>::new(), false),
            Vec::new(),
        ),
        (
            "not in non-empty values",
            datafusion::logical_expr::col("region").in_list(
                vec![
                    datafusion::logical_expr::lit("us-west"),
                    datafusion::logical_expr::lit("us-east"),
                ],
                true,
            ),
            Vec::new(),
        ),
        (
            "not in with empty literal",
            datafusion::logical_expr::col("region").in_list(
                vec![
                    datafusion::logical_expr::lit("us-west"),
                    datafusion::logical_expr::lit(""),
                ],
                true,
            ),
            vec!["part-00001.parquet"],
        ),
        (
            "empty not in",
            datafusion::logical_expr::col("region").in_list(Vec::<Expr>::new(), true),
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
        (
            "less than",
            datafusion::logical_expr::col("region").lt(datafusion::logical_expr::lit("us-west")),
            vec!["part-00001.parquet"],
        ),
        (
            "reversed less than",
            datafusion::logical_expr::lit("us-east").lt(datafusion::logical_expr::col("region")),
            vec!["part-00000.parquet"],
        ),
        (
            "greater than empty string literal",
            datafusion::logical_expr::col("region").gt(datafusion::logical_expr::lit("")),
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
        (
            "less than empty string literal",
            datafusion::logical_expr::col("region").lt(datafusion::logical_expr::lit("")),
            Vec::new(),
        ),
        (
            "between empty and z",
            datafusion::logical_expr::col("region").between(
                datafusion::logical_expr::lit(""),
                datafusion::logical_expr::lit("z"),
            ),
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
        (
            "not between empty and z",
            datafusion::logical_expr::col("region").not_between(
                datafusion::logical_expr::lit(""),
                datafusion::logical_expr::lit("z"),
            ),
            Vec::new(),
        ),
        (
            "contradictory between",
            datafusion::logical_expr::col("region").between(
                datafusion::logical_expr::lit("z"),
                datafusion::logical_expr::lit("a"),
            ),
            Vec::new(),
        ),
        (
            "contradictory not between",
            datafusion::logical_expr::col("region").not_between(
                datafusion::logical_expr::lit("z"),
                datafusion::logical_expr::lit("a"),
            ),
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
        (
            "not equality wrapper",
            Expr::Not(Box::new(
                datafusion::logical_expr::col("region")
                    .eq(datafusion::logical_expr::lit("us-west")),
            )),
            vec!["part-00001.parquet"],
        ),
        (
            "not empty equality wrapper",
            Expr::Not(Box::new(
                datafusion::logical_expr::col("region").eq(datafusion::logical_expr::lit("")),
            )),
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
        (
            "not is null wrapper",
            Expr::Not(Box::new(datafusion::logical_expr::col("region").is_null())),
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
        (
            "not is not null wrapper",
            Expr::Not(Box::new(
                datafusion::logical_expr::col("region").is_not_null(),
            )),
            vec![
                "part-00002.parquet",
                "part-00003.parquet",
                "part-00004.parquet",
            ],
        ),
        (
            "partition-only or",
            datafusion::logical_expr::col("region")
                .eq(datafusion::logical_expr::lit("us-west"))
                .or(datafusion::logical_expr::col("region").is_null()),
            vec![
                "part-00000.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
                "part-00004.parquet",
            ],
        ),
        (
            "equality terms in top-level and",
            datafusion::logical_expr::col("region")
                .eq(datafusion::logical_expr::lit("us-west"))
                .and(
                    datafusion::logical_expr::col("region")
                        .eq(datafusion::logical_expr::lit("us-west")),
                ),
            vec!["part-00000.parquet"],
        ),
        (
            "null check and equality in top-level and",
            datafusion::logical_expr::col("region").is_not_null().and(
                datafusion::logical_expr::col("region")
                    .eq(datafusion::logical_expr::lit("us-west")),
            ),
            vec!["part-00000.parquet"],
        ),
    ];

    for (name, filter, expected_paths) in cases {
        let plan = provider
            .scan(&state, None, std::slice::from_ref(&filter), None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        assert_eq!(scan.scan_plan().pushed_filter_plan.exact_count, 1, "{name}");
        assert_eq!(
            scan.scan_plan().pushed_filter_plan.residual_filter_count,
            0,
            "{name}"
        );
        assert!(
            scan.scan_plan().kernel_partition_predicate.is_some(),
            "{name}"
        );
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_mixed_string_null_and_empty_terms_use_kernel_pruning()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "mixed-string-null-empty-kernel-pruning",
        PARTITIONED_SCHEMA_FIELDS_JSON,
        r#"["region"]"#,
        &[
            r#""partitionValues":{"region":"us-west"}"#,
            r#""partitionValues":{"region":"us-east"}"#,
            r#""partitionValues":{"region":null}"#,
            r#""partitionValues":{"region":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let residual_filter =
        datafusion::logical_expr::col("id").gt(datafusion::logical_expr::lit(1_i64));
    let cases = vec![
        (
            "is null",
            datafusion::logical_expr::col("region")
                .is_null()
                .and(residual_filter.clone()),
            vec![
                "part-00002.parquet",
                "part-00003.parquet",
                "part-00004.parquet",
            ],
        ),
        (
            "is not null",
            datafusion::logical_expr::col("region")
                .is_not_null()
                .and(residual_filter.clone()),
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
        (
            "empty value equality",
            datafusion::logical_expr::col("region")
                .eq(datafusion::logical_expr::lit(""))
                .and(residual_filter),
            Vec::new(),
        ),
    ];

    for (name, filter, expected_paths) in cases {
        let plan = provider
            .scan(&state, None, std::slice::from_ref(&filter), None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;

        assert_eq!(scan.scan_plan().pushed_filter_plan.exact_count, 0, "{name}");
        assert_eq!(
            scan.scan_plan().pushed_filter_plan.inexact_count,
            1,
            "{name}"
        );
        assert_eq!(
            scan.scan_plan().pushed_filter_plan.unsupported_count,
            0,
            "{name}"
        );
        assert_eq!(
            scan.scan_plan().pushed_filter_plan.residual_filter_count,
            1,
            "{name}"
        );
        assert!(
            scan.scan_plan().kernel_partition_predicate.is_some(),
            "{name}"
        );
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_rejects_unsupported_string_partition_shapes()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-unsupported-string-partition-shapes",
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
    let state = SessionContext::new().state();
    let filters = vec![
        (
            "null between",
            datafusion::logical_expr::col("region").between(
                Expr::Literal(ScalarValue::Utf8(None), None),
                datafusion::logical_expr::lit("us-west"),
            ),
        ),
        (
            "numeric between",
            datafusion::logical_expr::col("region").between(
                datafusion::logical_expr::lit(7_i64),
                datafusion::logical_expr::lit("us-west"),
            ),
        ),
    ];

    for (name, filter) in filters {
        let result = provider
            .scan(&state, None, std::slice::from_ref(&filter), None)
            .await;

        assert!(
            matches!(result, Err(DataFusionError::External(error)) if error
                .to_string()
                .contains("pushed filters must be exact partition predicates")),
            "{name} should be rejected"
        );
    }

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_rejects_unproven_partition_in_filters()
-> Result<(), Box<dyn std::error::Error>> {
    const TWO_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"day\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;
    let table = DeltaLogTable::new_with_schema(
        "table-provider-unproven-partition-in-filters",
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
    let state = SessionContext::new().state();
    let filters = vec![
        (
            "null in",
            datafusion::logical_expr::col("region")
                .in_list(vec![Expr::Literal(ScalarValue::Utf8(None), None)], false),
        ),
        (
            "mixed null in",
            datafusion::logical_expr::col("region").in_list(
                vec![
                    datafusion::logical_expr::lit("us-west"),
                    Expr::Literal(ScalarValue::Utf8(None), None),
                ],
                false,
            ),
        ),
        (
            "wrong literal type in",
            datafusion::logical_expr::col("region")
                .in_list(vec![datafusion::logical_expr::lit(1_i64)], false),
        ),
        (
            "data column item in",
            datafusion::logical_expr::col("region")
                .in_list(vec![datafusion::logical_expr::col("id")], false),
        ),
        (
            "partition column item in",
            datafusion::logical_expr::col("region")
                .in_list(vec![datafusion::logical_expr::col("day")], false),
        ),
        (
            "cast item in",
            datafusion::logical_expr::col("region").in_list(
                vec![datafusion::logical_expr::cast(
                    datafusion::logical_expr::lit("us-west"),
                    DataType::Utf8,
                )],
                false,
            ),
        ),
        (
            "not in with null",
            datafusion::logical_expr::col("region").in_list(
                vec![
                    datafusion::logical_expr::lit("us-west"),
                    Expr::Literal(ScalarValue::Utf8(None), None),
                ],
                true,
            ),
        ),
    ];

    for (name, filter) in filters {
        let result = provider
            .scan(&state, None, std::slice::from_ref(&filter), None)
            .await;

        assert!(
            matches!(result, Err(DataFusionError::External(error)) if error
                .to_string()
                .contains("pushed filters must be exact partition predicates")),
            "{name} should be rejected"
        );
    }

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_rejects_unsafe_negated_partition_filters()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-unsafe-negated-partition-filters",
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
    let state = SessionContext::new().state();
    let filters = vec![(
        "non literal not in",
        datafusion::logical_expr::col("region")
            .in_list(vec![datafusion::logical_expr::col("id")], true),
    )];

    for (name, filter) in filters {
        let result = provider
            .scan(&state, None, std::slice::from_ref(&filter), None)
            .await;

        assert!(
            matches!(result, Err(DataFusionError::External(error)) if error
                .to_string()
                .contains("pushed filters must be exact partition predicates")),
            "{name} should be rejected"
        );
    }

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_handles_mixed_boolean_partition_filters()
-> Result<(), Box<dyn std::error::Error>> {
    const TWO_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"day\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;
    let table = DeltaLogTable::new_with_schema(
        "table-provider-mixed-boolean-partition-filters",
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
    let state = SessionContext::new().state();
    let partition_in = datafusion::logical_expr::col("region").in_list(
        vec![
            datafusion::logical_expr::lit("us-west"),
            datafusion::logical_expr::lit("us-east"),
        ],
        false,
    );
    let exact_partition_or =
        partition_in
            .clone()
            .or(datafusion::logical_expr::col("region")
                .eq(datafusion::logical_expr::lit("eu-central")));
    enum MixedBooleanExpectation {
        Inexact { paths: Vec<&'static str> },
        Rejected,
    }

    let filters = vec![
        (
            "partition in and data",
            partition_in
                .clone()
                .and(datafusion::logical_expr::col("id").gt(datafusion::logical_expr::lit(1_i64))),
            MixedBooleanExpectation::Inexact {
                paths: vec!["part-00000.parquet"],
            },
        ),
        (
            "partition in or data",
            partition_in
                .clone()
                .or(datafusion::logical_expr::col("id").eq(datafusion::logical_expr::lit(1_i64))),
            MixedBooleanExpectation::Rejected,
        ),
        (
            "partition equality or data",
            datafusion::logical_expr::col("region")
                .eq(datafusion::logical_expr::lit("us-west"))
                .or(datafusion::logical_expr::col("id").eq(datafusion::logical_expr::lit(1_i64))),
            MixedBooleanExpectation::Rejected,
        ),
        (
            "partition in or unknown",
            partition_in
                .clone()
                .or(datafusion::logical_expr::col("ghost").eq(datafusion::logical_expr::lit("x"))),
            MixedBooleanExpectation::Rejected,
        ),
        (
            "partition in or nested field",
            partition_in
                .clone()
                .or(datafusion::logical_expr::col("profile.age")
                    .eq(datafusion::logical_expr::lit(1_i64))),
            MixedBooleanExpectation::Rejected,
        ),
        (
            "nested exact partition or and data",
            exact_partition_or
                .and(datafusion::logical_expr::col("id").gt(datafusion::logical_expr::lit(1_i64))),
            MixedBooleanExpectation::Inexact {
                paths: vec!["part-00000.parquet"],
            },
        ),
    ];

    for (name, filter, expectation) in filters {
        let result = provider
            .scan(&state, None, std::slice::from_ref(&filter), None)
            .await;

        match expectation {
            MixedBooleanExpectation::Inexact { paths } => {
                let plan = result?;
                let scan = plan
                    .as_any()
                    .downcast_ref::<DeltaScanPlanningExec>()
                    .ok_or("expected DeltaScanPlanningExec")?;

                assert_eq!(scan.scan_plan().pushed_filter_plan.exact_count, 0, "{name}");
                assert_eq!(
                    scan.scan_plan().pushed_filter_plan.inexact_count,
                    1,
                    "{name}"
                );
                assert_eq!(
                    scan.scan_plan().pushed_filter_plan.residual_filter_count,
                    1,
                    "{name}"
                );
                assert_eq!(
                    scan.scan_plan().pushed_filter_plan.unsupported_count,
                    0,
                    "{name}"
                );
                assert!(
                    scan.scan_plan().kernel_partition_predicate.is_some(),
                    "{name}"
                );
                assert_eq!(scan_file_paths(scan)?, paths, "{name}");
            }
            MixedBooleanExpectation::Rejected => {
                assert!(
                    matches!(result, Err(DataFusionError::External(error)) if error
                        .to_string()
                        .contains("pushed filters must be exact partition predicates")),
                    "{name} should be rejected"
                );
            }
        }
    }

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_accepts_qualified_exact_partition_filter()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema(
        "table-provider-qualified-exact-partition-filter",
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
    let state = SessionContext::new().state();
    let filter = Expr::Column(datafusion::common::Column::new(Some("orders"), "region"))
        .eq(datafusion::logical_expr::lit("us-west"));

    let plan = provider.scan(&state, None, &[filter], None).await?;
    let scan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;

    assert_eq!(scan.scan_plan().pushed_filter_plan.exact_count, 1);
    assert_eq!(scan.scan_plan().pushed_filter_plan.unsupported_count, 0);
    assert_eq!(scan.scan_plan().pushed_filter_plan.residual_filter_count, 0);
    assert_eq!(scan.scan_plan().pushed_filter_plan.pushed_filter_count, 1);

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_rejects_ambiguous_partition_column_references()
-> Result<(), Box<dyn std::error::Error>> {
    const DOTTED_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"address.city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;

    struct RejectedReferenceProbe {
        name: &'static str,
        schema_fields_json: &'static str,
        partition_columns_json: &'static str,
        add_partition_values_json: &'static str,
        filter: Expr,
    }

    let cases = [
        RejectedReferenceProbe {
            name: "wrong-case partition reference",
            schema_fields_json: PARTITIONED_SCHEMA_FIELDS_JSON,
            partition_columns_json: r#"["region"]"#,
            add_partition_values_json: r#""partitionValues":{"region":"us-west"}"#,
            filter: Expr::Column(datafusion::common::Column::new_unqualified("Region"))
                .eq(datafusion::logical_expr::lit("us-west")),
        },
        RejectedReferenceProbe {
            name: "dotted partition reference",
            schema_fields_json: DOTTED_PARTITION_SCHEMA_FIELDS_JSON,
            partition_columns_json: r#"["address.city"]"#,
            add_partition_values_json: r#""partitionValues":{"address.city":"Phoenix"}"#,
            filter: datafusion::logical_expr::col("address.city")
                .eq(datafusion::logical_expr::lit("Phoenix")),
        },
        RejectedReferenceProbe {
            name: "nested data field reference",
            schema_fields_json: NESTED_SCHEMA_FIELDS_JSON,
            partition_columns_json: "[]",
            add_partition_values_json: r#""partitionValues":{}"#,
            filter: datafusion::logical_expr::col("profile.age")
                .gt(datafusion::logical_expr::lit(21)),
        },
    ];

    for case in cases {
        let table = DeltaLogTable::new_with_schema(
            case.name,
            case.schema_fields_json,
            case.partition_columns_json,
            case.add_partition_values_json,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let state = SessionContext::new().state();

        let result = provider
            .scan(&state, None, std::slice::from_ref(&case.filter), None)
            .await;

        assert!(
            matches!(result, Err(DataFusionError::External(error)) if error
                .to_string()
                .contains("pushed filters must be exact partition predicates")),
            "{} should be rejected",
            case.name
        );
    }

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_accepts_inexact_mixed_partition_filter()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema(
        "table-provider-mixed-partition-filter",
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
    let state = SessionContext::new().state();
    let filter = datafusion::logical_expr::col("region")
        .in_list(
            vec![
                datafusion::logical_expr::lit("us-west"),
                datafusion::logical_expr::lit("us-east"),
            ],
            false,
        )
        .and(datafusion::logical_expr::col("id").between(
            datafusion::logical_expr::lit(10),
            datafusion::logical_expr::lit(20),
        ));

    let plan = provider.scan(&state, None, &[filter], None).await?;
    let scan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;

    assert_eq!(scan.scan_plan().pushed_filter_plan.exact_count, 0);
    assert_eq!(scan.scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(scan.scan_plan().pushed_filter_plan.unsupported_count, 0);
    assert_eq!(scan.scan_plan().pushed_filter_plan.residual_filter_count, 1);
    assert!(scan.scan_plan().kernel_partition_predicate.is_some());
    assert_eq!(scan_file_paths(scan)?, vec!["part-00000.parquet"]);

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_combines_mixed_and_exact_kernel_filters()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-mixed-and-exact-filter",
        PARTITIONED_SCHEMA_FIELDS_JSON,
        r#"["region"]"#,
        &[
            r#""partitionValues":{"region":"us-west"}"#,
            r#""partitionValues":{"region":"us-east"}"#,
            r#""partitionValues":{"region":"eu-central"}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let mixed_filter = datafusion::logical_expr::col("region")
        .in_list(
            vec![
                datafusion::logical_expr::lit("us-west"),
                datafusion::logical_expr::lit("us-east"),
            ],
            false,
        )
        .and(datafusion::logical_expr::col("id").gt(datafusion::logical_expr::lit(1000)));
    let exact_filter =
        datafusion::logical_expr::col("region").eq(datafusion::logical_expr::lit("us-east"));

    let plan = provider
        .scan(&state, None, &[mixed_filter, exact_filter], None)
        .await?;
    let scan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;

    assert_eq!(scan.scan_plan().pushed_filter_plan.exact_count, 1);
    assert_eq!(scan.scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(scan.scan_plan().pushed_filter_plan.unsupported_count, 0);
    assert_eq!(scan.scan_plan().pushed_filter_plan.residual_filter_count, 1);
    assert!(scan.scan_plan().kernel_partition_predicate.is_some());
    assert_eq!(scan_file_paths(scan)?, vec!["part-00001.parquet"]);

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_rejects_projected_inexact_mixed_partition_filter()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema(
        "table-provider-projected-inexact-mixed-partition-filter",
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
    let state = SessionContext::new().state();
    let projection = vec![0];
    let filter = datafusion::logical_expr::col("region")
        .eq(datafusion::logical_expr::lit("us-west"))
        .and(datafusion::logical_expr::col("id").gt(datafusion::logical_expr::lit(1)));

    let result = provider
        .scan(&state, Some(&projection), &[filter], None)
        .await;

    assert!(
        matches!(result, Err(DataFusionError::External(error)) if error
            .to_string()
            .contains("inexact pushed filter residual columns must be projected"))
    );

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_accepts_projected_mixed_partition_filter_when_residual_columns_are_projected()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema(
        "table-provider-projected-mixed-partition-filter",
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
    let state = SessionContext::new().state();
    let projection = vec![0, 1];
    let filter = datafusion::logical_expr::col("region")
        .eq(datafusion::logical_expr::lit("us-west"))
        .and(datafusion::logical_expr::col("id").gt(datafusion::logical_expr::lit(1)));

    let plan = provider
        .scan(&state, Some(&projection), &[filter], None)
        .await?;
    let scan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;

    assert_eq!(scan.scan_plan().scan_projection, Some(vec![0, 1]));
    assert_eq!(scan.scan_plan().pushed_filter_plan.exact_count, 0);
    assert_eq!(scan.scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(scan.scan_plan().pushed_filter_plan.unsupported_count, 0);
    assert_eq!(scan.scan_plan().pushed_filter_plan.residual_filter_count, 1);
    assert!(scan.scan_plan().kernel_partition_predicate.is_some());
    assert_eq!(scan_file_paths(scan)?, vec!["part-00000.parquet"]);

    Ok(())
}

#[tokio::test]
async fn table_provider_scan_limit_does_not_change_scan_planning_contract()
-> Result<(), Box<dyn std::error::Error>> {
    let matching_stats = id_stats_add_json(10, 101, 150, 0);
    let skipped_stats = id_stats_add_json(10, 1, 50, 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "table-provider-limit-unsupported",
        DEFAULT_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[matching_stats.as_str(), skipped_stats.as_str()],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let projection = vec![0, 1];
    let filter = datafusion::logical_expr::col("id").gt(datafusion::logical_expr::lit(100));

    let with_limit = provider
        .scan(
            &state,
            Some(&projection),
            std::slice::from_ref(&filter),
            Some(1),
        )
        .await?;
    let without_limit = provider
        .scan(&state, Some(&projection), &[filter], None)
        .await?;
    let with_limit_scan = with_limit
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;
    let without_limit_scan = without_limit
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;

    assert_eq!(with_limit.schema(), without_limit.schema());
    assert_eq!(
        with_limit_scan.scan_plan().scan_projection,
        without_limit_scan.scan_plan().scan_projection
    );
    assert_eq!(
        with_limit_scan.scan_plan().pushed_filter_plan.exact_count,
        without_limit_scan
            .scan_plan()
            .pushed_filter_plan
            .exact_count
    );
    assert_eq!(
        with_limit_scan.scan_plan().pushed_filter_plan.inexact_count,
        without_limit_scan
            .scan_plan()
            .pushed_filter_plan
            .inexact_count
    );
    assert_eq!(
        with_limit_scan
            .scan_plan()
            .pushed_filter_plan
            .residual_filter_count,
        without_limit_scan
            .scan_plan()
            .pushed_filter_plan
            .residual_filter_count
    );
    assert_eq!(
        with_limit_scan
            .scan_plan()
            .kernel_partition_predicate
            .is_some(),
        without_limit_scan
            .scan_plan()
            .kernel_partition_predicate
            .is_some()
    );
    assert_eq!(
        scan_file_paths(with_limit_scan)?,
        scan_file_paths(without_limit_scan)?
    );
    assert_scan_does_not_support_limit_pushdown(with_limit_scan);
    assert_scan_does_not_support_limit_pushdown(without_limit_scan);

    Ok(())
}

#[tokio::test]
async fn sql_limit_stays_above_inexact_residual_filter_scan()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let possible_stats = id_stats_add_json(10, 101, 150, 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "limit-above-residual-filter-scan",
        DEFAULT_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[possible_stats.as_str()],
    )?;
    let table_uri = table.path().to_string_lossy().to_string();
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri,
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    let dataframe = ctx
        .sql("select customer_name from orders where id > 100 limit 1")
        .await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    let mut scans = Vec::new();
    find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

    assert!(
        plan_display.contains("CoalescePartitionsExec: fetch=1"),
        "{plan_display}"
    );
    assert!(plan_display.contains("FilterExec"), "{plan_display}");
    assert!(
        plan_display.contains("FilterExec: id@0 > 100"),
        "{plan_display}"
    );
    assert!(
        plan_display.contains("DeltaScanPlanningExec"),
        "{plan_display}"
    );
    assert_eq!(scans.len(), 1);
    assert_eq!(scans[0].scan_plan().scan_projection, Some(vec![0, 1]));
    assert_eq!(scans[0].scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(
        scans[0]
            .scan_plan()
            .pushed_filter_plan
            .residual_filter_count,
        1
    );
    assert_scan_does_not_support_limit_pushdown(scans[0]);

    Ok(())
}

#[tokio::test]
async fn sql_limit_stays_above_joined_delta_scans() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let _orders = register_fixture_source(&ctx, "orders", "limit-join-orders")?;
    let _customers = register_fixture_source(&ctx, "customers", "limit-join-customers")?;

    let dataframe = ctx
        .sql(
            "select orders.id \
             from orders join customers on orders.id = customers.id \
             limit 1",
        )
        .await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    let mut scans = Vec::new();
    find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

    assert!(
        plan_display.contains("CoalescePartitionsExec: fetch=1"),
        "{plan_display}"
    );
    assert!(plan_display.contains("HashJoinExec"), "{plan_display}");
    assert!(plan_display.contains("HashJoinExec") && plan_display.contains("fetch=1"));
    assert!(
        plan_display.contains("DeltaScanPlanningExec"),
        "{plan_display}"
    );
    assert_eq!(scans.len(), 2);
    for scan in scans {
        assert_eq!(scan.scan_plan().scan_projection, Some(vec![0]));
        assert_eq!(scan.scan_plan().pushed_filter_plan.pushed_filter_count, 0);
        assert_scan_does_not_support_limit_pushdown(scan);
    }

    Ok(())
}

#[tokio::test]
async fn sql_analysis_works_for_select_star_without_scan_execution()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let _table = register_fixture_source(&ctx, "orders", "select-star")?;

    let dataframe = ctx.sql("select * from orders").await?;
    let schema = dataframe.schema();

    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "customer_name");

    Ok(())
}

#[tokio::test]
async fn sql_analysis_works_for_projection_without_delta_projection_config()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let _table = register_fixture_source(&ctx, "orders", "projection")?;

    let dataframe = ctx.sql("select customer_name from orders").await?;
    let optimized = dataframe.into_optimized_plan()?;
    let schema = optimized.schema();

    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "customer_name");
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);

    Ok(())
}

#[tokio::test]
async fn residual_filter_column_remains_available_below_final_projection()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let _table = register_fixture_source(&ctx, "orders", "residual-filter-projection")?;

    let dataframe = ctx
        .sql("select id from orders where customer_name = 'alice'")
        .await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    let mut scans = Vec::new();
    find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

    assert!(plan_display.contains("FilterExec"), "{plan_display}");
    assert!(
        plan_display.contains("DeltaScanPlanningExec"),
        "{plan_display}"
    );
    assert_eq!(physical_plan.schema().fields().len(), 1);
    assert_eq!(physical_plan.schema().field(0).name(), "id");
    assert_eq!(scans.len(), 1);
    assert_eq!(
        scans[0].scan_plan().scan_projection,
        Some(vec![0, 1]),
        "scan must keep the residual filter column even though final output only projects id"
    );
    assert_eq!(scans[0].schema().fields().len(), 2);
    assert_eq!(scans[0].schema().field(0).name(), "id");
    assert_eq!(scans[0].schema().field(1).name(), "customer_name");

    Ok(())
}

#[tokio::test]
async fn data_stats_residual_column_remains_available_below_final_projection()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let possible_stats = id_stats_add_json(10, 101, 150, 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "data-stats-residual-filter-projection",
        DEFAULT_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[possible_stats.as_str()],
    )?;
    let table_uri = table.path().to_string_lossy().to_string();
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri,
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    let dataframe = ctx
        .sql("select customer_name from orders where id > 100")
        .await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    let mut scans = Vec::new();
    find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

    assert!(plan_display.contains("FilterExec"), "{plan_display}");
    assert!(
        plan_display.contains("DeltaScanPlanningExec"),
        "{plan_display}"
    );
    assert_eq!(physical_plan.schema().fields().len(), 1);
    assert_eq!(physical_plan.schema().field(0).name(), "customer_name");
    assert_eq!(scans.len(), 1);
    assert_eq!(scans[0].scan_plan().scan_projection, Some(vec![0, 1]));
    assert_eq!(scans[0].schema().fields().len(), 2);
    assert_eq!(scans[0].schema().field(0).name(), "id");
    assert_eq!(scans[0].schema().field(1).name(), "customer_name");
    assert_eq!(scans[0].scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(
        scans[0]
            .scan_plan()
            .pushed_filter_plan
            .residual_filter_count,
        1
    );
    assert!(scans[0].scan_plan().kernel_partition_predicate.is_some());

    Ok(())
}

#[tokio::test]
async fn string_data_stats_residual_column_remains_available_below_final_projection()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let possible_stats = string_stats_add_json(10, "alice", "alice", 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "string-data-stats-residual-filter-projection",
        DEFAULT_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[possible_stats.as_str()],
    )?;
    let table_uri = table.path().to_string_lossy().to_string();
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri,
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    let dataframe = ctx
        .sql("select id from orders where customer_name = 'alice'")
        .await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    let mut scans = Vec::new();
    find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

    assert!(plan_display.contains("FilterExec"), "{plan_display}");
    assert!(
        plan_display.contains("DeltaScanPlanningExec"),
        "{plan_display}"
    );
    assert_eq!(physical_plan.schema().fields().len(), 1);
    assert_eq!(physical_plan.schema().field(0).name(), "id");
    assert_eq!(scans.len(), 1);
    assert_eq!(scans[0].scan_plan().scan_projection, Some(vec![0, 1]));
    assert_eq!(scans[0].schema().fields().len(), 2);
    assert_eq!(scans[0].schema().field(0).name(), "id");
    assert_eq!(scans[0].schema().field(1).name(), "customer_name");
    assert_eq!(scans[0].scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(
        scans[0]
            .scan_plan()
            .pushed_filter_plan
            .residual_filter_count,
        1
    );
    assert!(scans[0].scan_plan().kernel_partition_predicate.is_some());

    Ok(())
}

#[tokio::test]
async fn floating_data_stats_residual_column_remains_available_below_final_projection()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let possible_stats = floating_stats_add_json(10, "1.5", "1.5", "2.25", "2.25", 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "floating-data-stats-residual-filter-projection",
        FLOATING_DATA_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[possible_stats.as_str()],
    )?;
    let table_uri = table.path().to_string_lossy().to_string();
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri,
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    let dataframe = ctx
        .sql("select id from orders where float_score = cast(1.5 as float)")
        .await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    let mut scans = Vec::new();
    find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

    assert!(plan_display.contains("FilterExec"), "{plan_display}");
    assert!(
        plan_display.contains("DeltaScanPlanningExec"),
        "{plan_display}"
    );
    assert_eq!(physical_plan.schema().fields().len(), 1);
    assert_eq!(physical_plan.schema().field(0).name(), "id");
    assert_eq!(scans.len(), 1);
    assert_eq!(scans[0].scan_plan().scan_projection, Some(vec![0, 1]));
    assert_eq!(scans[0].schema().fields().len(), 2);
    assert_eq!(scans[0].schema().field(0).name(), "id");
    assert_eq!(scans[0].schema().field(1).name(), "float_score");
    assert_eq!(scans[0].scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(
        scans[0]
            .scan_plan()
            .pushed_filter_plan
            .residual_filter_count,
        1
    );
    assert!(scans[0].scan_plan().kernel_partition_predicate.is_some());

    Ok(())
}

#[tokio::test]
async fn sql_floating_data_stats_supported_filters_remain_residual()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let low_stats = floating_stats_add_json(10, "0.5", "0.5", "0.5", "0.5", 0);
    let high_stats = floating_stats_add_json(10, "2.5", "2.5", "2.5", "2.5", 0);
    let all_null_stats = floating_partial_stats_add_json(10, Some(10));
    let table = DeltaLogTable::new_with_schema_and_adds(
        "sql-floating-data-stats-supported-residuals",
        FLOATING_DATA_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[
            low_stats.as_str(),
            high_stats.as_str(),
            all_null_stats.as_str(),
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    let cases = [
        (
            "finite comparison",
            "select id, float_score from orders where float_score > cast(1.5 as float)",
            vec![0, 1],
            vec!["id", "float_score"],
            vec!["part-00001.parquet", "part-00003.parquet"],
        ),
        (
            "null check",
            "select id, double_score from orders where double_score is not null",
            vec![0, 2],
            vec!["id", "double_score"],
            vec![
                "part-00000.parquet",
                "part-00001.parquet",
                "part-00003.parquet",
            ],
        ),
    ];

    for (name, sql, scan_projection, field_names, expected_paths) in cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(
            plan_display.contains("FilterExec"),
            "{name}: {plan_display}"
        );
        assert!(
            plan_display.contains("DeltaScanPlanningExec"),
            "{name}: {plan_display}"
        );
        assert_eq!(physical_plan.schema().fields().len(), 2, "{name}");
        assert_eq!(scans.len(), 1, "{name}: {plan_display}");
        assert_eq!(
            scans[0].scan_plan().scan_projection,
            Some(scan_projection),
            "{name}"
        );
        assert_eq!(scans[0].schema().fields().len(), 2, "{name}");
        for (field_index, field_name) in field_names.into_iter().enumerate() {
            assert_eq!(
                scans[0].schema().field(field_index).name(),
                field_name,
                "{name}"
            );
        }
        assert_eq!(
            scans[0].scan_plan().pushed_filter_plan.inexact_count,
            1,
            "{name}"
        );
        assert_eq!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .residual_filter_count,
            1,
            "{name}"
        );
        assert!(
            scans[0].scan_plan().kernel_partition_predicate.is_some(),
            "{name}"
        );
        assert_eq!(scan_file_paths(scans[0])?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn boolean_data_stats_residual_column_remains_available_below_final_projection()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let stats = boolean_stats_add_json(10, false, false, 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "boolean-data-stats-residual-filter-projection",
        BOOLEAN_DATA_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[stats.as_str()],
    )?;
    let table_uri = table.path().to_string_lossy().to_string();
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri,
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    let dataframe = ctx
        .sql("select id from orders where is_current is not null")
        .await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    let mut scans = Vec::new();
    find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

    assert!(plan_display.contains("FilterExec"), "{plan_display}");
    assert!(
        plan_display.contains("DeltaScanPlanningExec"),
        "{plan_display}"
    );
    assert_eq!(physical_plan.schema().fields().len(), 1);
    assert_eq!(physical_plan.schema().field(0).name(), "id");
    assert_eq!(scans.len(), 1);
    assert_eq!(scans[0].scan_plan().scan_projection, Some(vec![0, 1]));
    assert_eq!(scans[0].schema().fields().len(), 2);
    assert_eq!(scans[0].schema().field(0).name(), "id");
    assert_eq!(scans[0].schema().field(1).name(), "is_current");
    assert_eq!(scans[0].scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(
        scans[0]
            .scan_plan()
            .pushed_filter_plan
            .residual_filter_count,
        1
    );
    assert!(scans[0].scan_plan().kernel_partition_predicate.is_some());

    Ok(())
}

#[tokio::test]
async fn binary_data_stats_residual_column_remains_available_below_final_projection()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let stats = binary_partial_stats_add_json(10, Some(0));
    let table = DeltaLogTable::new_with_schema_and_adds(
        "binary-data-stats-residual-filter-projection",
        BINARY_DATA_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[stats.as_str()],
    )?;
    let table_uri = table.path().to_string_lossy().to_string();
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri,
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    let dataframe = ctx
        .sql("select id from orders where payload is not null")
        .await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    let mut scans = Vec::new();
    find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

    assert!(plan_display.contains("FilterExec"), "{plan_display}");
    assert!(
        plan_display.contains("DeltaScanPlanningExec"),
        "{plan_display}"
    );
    assert_eq!(physical_plan.schema().fields().len(), 1);
    assert_eq!(physical_plan.schema().field(0).name(), "id");
    assert_eq!(scans.len(), 1);
    assert_eq!(scans[0].scan_plan().scan_projection, Some(vec![0, 1]));
    assert_eq!(scans[0].schema().fields().len(), 2);
    assert_eq!(scans[0].schema().field(0).name(), "id");
    assert_eq!(scans[0].schema().field(1).name(), "payload");
    assert_eq!(scans[0].scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(
        scans[0]
            .scan_plan()
            .pushed_filter_plan
            .residual_filter_count,
        1
    );
    assert!(scans[0].scan_plan().kernel_partition_predicate.is_some());
    assert_eq!(scan_file_paths(scans[0])?, vec!["part-00000.parquet"]);

    Ok(())
}

#[tokio::test]
async fn temporal_data_stats_residual_column_remains_available_below_final_projection()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let stats = date_stats_add_json(10, "2026-01-01", "2026-01-01", 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "temporal-data-stats-residual-filter-projection",
        DATE_DATA_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[stats.as_str()],
    )?;
    let table_uri = table.path().to_string_lossy().to_string();
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri,
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    let dataframe = ctx
        .sql("select id from orders where event_date = DATE '2026-01-01'")
        .await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    let mut scans = Vec::new();
    find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

    assert!(plan_display.contains("FilterExec"), "{plan_display}");
    assert!(
        plan_display.contains("DeltaScanPlanningExec"),
        "{plan_display}"
    );
    assert_eq!(physical_plan.schema().fields().len(), 1);
    assert_eq!(physical_plan.schema().field(0).name(), "id");
    assert_eq!(scans.len(), 1);
    assert_eq!(scans[0].scan_plan().scan_projection, Some(vec![0, 1]));
    assert_eq!(scans[0].schema().fields().len(), 2);
    assert_eq!(scans[0].schema().field(0).name(), "id");
    assert_eq!(scans[0].schema().field(1).name(), "event_date");
    assert_eq!(scans[0].scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(
        scans[0]
            .scan_plan()
            .pushed_filter_plan
            .residual_filter_count,
        1
    );
    assert!(scans[0].scan_plan().kernel_partition_predicate.is_some());

    Ok(())
}

#[tokio::test]
async fn decimal_data_stats_residual_column_remains_available_below_final_projection()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let stats = decimal_stats_add_json(10, "2.00", "2.00", 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "decimal-data-stats-residual-filter-projection",
        DECIMAL_DATA_SCHEMA_FIELDS_JSON,
        r#"[]"#,
        &[stats.as_str()],
    )?;
    let table_uri = table.path().to_string_lossy().to_string();
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri,
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    let dataframe = ctx
        .sql("select id from orders where amount = DECIMAL '2.00'")
        .await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    let mut scans = Vec::new();
    find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

    assert!(plan_display.contains("FilterExec"), "{plan_display}");
    assert!(
        plan_display.contains("DeltaScanPlanningExec"),
        "{plan_display}"
    );
    assert_eq!(physical_plan.schema().fields().len(), 1);
    assert_eq!(physical_plan.schema().field(0).name(), "id");
    assert_eq!(scans.len(), 1);
    assert_eq!(scans[0].scan_plan().scan_projection, Some(vec![0, 1]));
    assert_eq!(scans[0].schema().fields().len(), 2);
    assert_eq!(scans[0].schema().field(0).name(), "id");
    assert_eq!(scans[0].schema().field(1).name(), "amount");
    assert_eq!(scans[0].scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(
        scans[0]
            .scan_plan()
            .pushed_filter_plan
            .residual_filter_count,
        1
    );
    assert!(scans[0].scan_plan().kernel_partition_predicate.is_some());

    Ok(())
}

#[tokio::test]
async fn composed_stats_residual_column_remains_available_below_final_projection()
-> Result<(), Box<dyn std::error::Error>> {
    const MIXED_STATS_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;

    let ctx = SessionContext::new();
    let possible_stats = partitioned_id_stats_add_json(r#"{"region":"us-west"}"#, 10, 101, 150, 0);
    let table = DeltaLogTable::new_with_schema_and_adds(
        "composed-stats-residual-filter-projection",
        MIXED_STATS_SCHEMA_FIELDS_JSON,
        r#"["region"]"#,
        &[possible_stats.as_str()],
    )?;
    let table_uri = table.path().to_string_lossy().to_string();
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri,
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    let dataframe = ctx
        .sql("select customer_name from orders where region = 'us-west' and id > 100")
        .await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    let mut scans = Vec::new();
    find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

    assert!(plan_display.contains("FilterExec"), "{plan_display}");
    assert!(
        plan_display.contains("DeltaScanPlanningExec"),
        "{plan_display}"
    );
    assert_eq!(physical_plan.schema().fields().len(), 1);
    assert_eq!(physical_plan.schema().field(0).name(), "customer_name");
    assert_eq!(scans.len(), 1);
    assert_eq!(scans[0].scan_plan().scan_projection, Some(vec![0, 1]));
    assert_eq!(scans[0].schema().fields().len(), 2);
    assert_eq!(scans[0].schema().field(0).name(), "id");
    assert_eq!(scans[0].schema().field(1).name(), "customer_name");
    assert_eq!(scans[0].scan_plan().pushed_filter_plan.exact_count, 1);
    assert_eq!(scans[0].scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(
        scans[0]
            .scan_plan()
            .pushed_filter_plan
            .residual_filter_count,
        1
    );
    assert!(scans[0].scan_plan().kernel_partition_predicate.is_some());

    Ok(())
}

#[tokio::test]
async fn mixed_partition_pruning_keeps_residual_column_below_final_projection()
-> Result<(), Box<dyn std::error::Error>> {
    const MIXED_FILTER_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;

    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema(
        "mixed-partition-pruning-residual-projection",
        MIXED_FILTER_SCHEMA_FIELDS_JSON,
        r#"["region"]"#,
        r#""partitionValues":{"region":"us-west"}"#,
    )?;
    let table_uri = table.path().to_string_lossy().to_string();
    let probe_source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table_uri.clone(),
        version: None,
    })?;
    let probe_preflight = preflight_delta_protocol(&probe_source)?;
    let provider = DeltaTableProvider::try_new(probe_source, probe_preflight)?;
    let mixed_filter = datafusion::logical_expr::col("region")
        .eq(datafusion::logical_expr::lit("us-west"))
        .and(
            datafusion::logical_expr::col("customer_name")
                .eq(datafusion::logical_expr::lit("alice")),
        );
    assert_eq!(
        provider.supports_filters_pushdown(&[&mixed_filter])?,
        vec![TableProviderFilterPushDown::Inexact]
    );

    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri,
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    let optimized_dataframe = ctx
        .sql("select id from orders where region = 'us-west' and customer_name = 'alice'")
        .await?;
    let optimized_plan = optimized_dataframe.into_optimized_plan()?;
    let optimized_display = optimized_plan.display_indent().to_string();

    assert!(optimized_display.contains("Filter:"), "{optimized_display}");
    assert!(
        optimized_display.contains("customer_name"),
        "{optimized_display}"
    );
    assert!(
        optimized_display.contains("full_filters"),
        "{optimized_display}"
    );
    assert!(optimized_display.contains("region"), "{optimized_display}");

    let dataframe = ctx
        .sql("select id from orders where region = 'us-west' and customer_name = 'alice'")
        .await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    let mut scans = Vec::new();
    find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

    assert!(plan_display.contains("FilterExec"), "{plan_display}");
    assert!(plan_display.contains("customer_name"), "{plan_display}");
    assert_eq!(physical_plan.schema().fields().len(), 1);
    assert_eq!(physical_plan.schema().field(0).name(), "id");
    assert_eq!(scans.len(), 1);
    assert_eq!(scans[0].scan_plan().scan_projection, Some(vec![0, 1]));
    assert_eq!(scans[0].schema().fields().len(), 2);
    assert_eq!(scans[0].schema().field(0).name(), "id");
    assert_eq!(scans[0].schema().field(1).name(), "customer_name");
    assert_eq!(scans[0].scan_plan().pushed_filter_plan.exact_count, 1);
    assert_eq!(scans[0].scan_plan().pushed_filter_plan.inexact_count, 1);
    assert_eq!(scans[0].scan_plan().pushed_filter_plan.unsupported_count, 0);
    assert_eq!(
        scans[0]
            .scan_plan()
            .pushed_filter_plan
            .residual_filter_count,
        1
    );
    assert!(scans[0].scan_plan().kernel_partition_predicate.is_some());
    let kernel_names = scans[0]
        .scan_plan()
        .kernel_scan()
        .kernel_schema()
        .fields()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();
    assert_eq!(kernel_names, vec!["id", "customer_name", "region"]);
    assert_eq!(scan_file_paths(scans[0])?, vec!["part-00000.parquet"]);

    Ok(())
}

#[tokio::test]
async fn sql_exact_partition_filter_is_pushed_without_residual_filter()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema(
        "sql-exact-partition-filter",
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
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    let dataframe = ctx
        .sql("select id from orders where region = 'us-west'")
        .await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    let mut scans = Vec::new();
    find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

    assert!(!plan_display.contains("FilterExec"), "{plan_display}");
    assert_eq!(physical_plan.schema().fields().len(), 1);
    assert_eq!(physical_plan.schema().field(0).name(), "id");
    assert_eq!(scans.len(), 1);
    assert_eq!(scans[0].scan_plan().scan_projection, Some(vec![0]));
    assert_eq!(scans[0].scan_plan().pushed_filter_plan.exact_count, 1);
    assert_eq!(scans[0].scan_plan().pushed_filter_plan.unsupported_count, 0);
    assert_eq!(
        scans[0]
            .scan_plan()
            .pushed_filter_plan
            .residual_filter_count,
        0
    );
    assert_eq!(scans[0].schema().fields().len(), 1);
    assert_eq!(scans[0].schema().field(0).name(), "id");
    assert!(scans[0].scan_plan().kernel_partition_predicate.is_some());
    assert_eq!(scan_file_paths(scans[0])?, vec!["part-00000.parquet"]);
    let kernel_names = scans[0]
        .scan_plan()
        .kernel_scan()
        .kernel_schema()
        .fields()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();
    assert_eq!(kernel_names, vec!["id", "region"]);

    Ok(())
}

#[tokio::test]
async fn sql_partition_in_filter_is_exact_kernel_pushdown() -> Result<(), Box<dyn std::error::Error>>
{
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema_and_adds(
        "sql-exact-partition-in-filter",
        PARTITIONED_SCHEMA_FIELDS_JSON,
        r#"["region"]"#,
        &[
            r#""partitionValues":{"region":"us-west"}"#,
            r#""partitionValues":{"region":"us-east"}"#,
            r#""partitionValues":{"region":"eu-central"}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    let dataframe = ctx
        .sql("select id from orders where region in ('us-west', 'us-east')")
        .await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    let mut scans = Vec::new();
    find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

    assert!(!plan_display.contains("FilterExec"), "{plan_display}");
    assert_eq!(physical_plan.schema().fields().len(), 1);
    assert_eq!(physical_plan.schema().field(0).name(), "id");
    assert_eq!(scans.len(), 1);
    assert_eq!(scans[0].scan_plan().scan_projection, Some(vec![0]));
    assert_eq!(scans[0].scan_plan().pushed_filter_plan.exact_count, 1);
    assert_eq!(
        scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
        1
    );
    assert_eq!(
        scans[0]
            .scan_plan()
            .pushed_filter_plan
            .residual_filter_count,
        0
    );
    assert!(scans[0].scan_plan().kernel_partition_predicate.is_some());
    assert_eq!(
        scan_file_paths(scans[0])?,
        vec!["part-00000.parquet", "part-00001.parquet"]
    );
    let kernel_names = scans[0]
        .scan_plan()
        .kernel_scan()
        .kernel_schema()
        .fields()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();
    assert_eq!(kernel_names, vec!["id", "region"]);

    Ok(())
}

#[tokio::test]
async fn sql_duplicate_and_contradictory_partition_filters_are_exact_kernel_pushdown()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema_and_adds(
        "sql-duplicate-contradictory-partition-filters",
        PARTITIONED_SCHEMA_FIELDS_JSON,
        r#"["region"]"#,
        &[
            r#""partitionValues":{"region":"us-west"}"#,
            r#""partitionValues":{"region":"us-east"}"#,
            r#""partitionValues":{"region":null}"#,
            r#""partitionValues":{"region":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    enum ExpectedSqlPartitionEdge {
        ExactScan {
            exact_count: usize,
            paths: Vec<&'static str>,
        },
        EmptyBeforeScan,
    }

    let sql_cases = [
        (
            "duplicate equality",
            "select id from orders where region = 'us-west' and region = 'us-west'",
            ExpectedSqlPartitionEdge::ExactScan {
                exact_count: 1,
                paths: vec!["part-00000.parquet"],
            },
        ),
        (
            "contradictory equality",
            "select id from orders where region = 'us-west' and region = 'us-east'",
            ExpectedSqlPartitionEdge::EmptyBeforeScan,
        ),
    ];

    for (name, sql, expectation) in sql_cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        match expectation {
            ExpectedSqlPartitionEdge::ExactScan { exact_count, paths } => {
                assert!(
                    !plan_display.contains("FilterExec"),
                    "{name} unexpectedly kept a residual filter:\n{plan_display}"
                );
                assert_eq!(scans.len(), 1, "{name}: {plan_display}");
                assert_eq!(
                    scans[0].scan_plan().pushed_filter_plan.exact_count,
                    exact_count,
                    "{name}: {plan_display}"
                );
                assert_eq!(
                    scans[0]
                        .scan_plan()
                        .pushed_filter_plan
                        .residual_filter_count,
                    0,
                    "{name}: {plan_display}"
                );
                assert!(
                    scans[0].scan_plan().kernel_partition_predicate.is_some(),
                    "{name}: {plan_display}"
                );
                assert_eq!(scan_file_paths(scans[0])?, paths, "{name}");
            }
            ExpectedSqlPartitionEdge::EmptyBeforeScan => {
                assert!(plan_display.contains("EmptyExec"), "{name}: {plan_display}");
                assert!(scans.is_empty(), "{name}: {plan_display}");
            }
        }
    }

    Ok(())
}

#[tokio::test]
async fn sql_partition_in_edge_variants_document_rewrite_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    const TWO_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"day\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema(
        "sql-partition-in-edge-variants",
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
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    enum ExpectedInProbe {
        EmptyBeforeScan,
        ExactAfterRewrite,
        ResidualFilter,
    }

    let sql_cases = [
        (
            "null only",
            "select id from orders where region in (null)",
            ExpectedInProbe::EmptyBeforeScan,
        ),
        (
            "mixed null",
            "select id from orders where region in ('us-west', null)",
            ExpectedInProbe::ResidualFilter,
        ),
        (
            "wrong literal type",
            "select id from orders where region in (1)",
            ExpectedInProbe::ExactAfterRewrite,
        ),
        (
            "data column item",
            "select id from orders where region in (id)",
            ExpectedInProbe::ResidualFilter,
        ),
        (
            "partition column item",
            "select id from orders where region in (day)",
            ExpectedInProbe::ResidualFilter,
        ),
        (
            "scalar function item",
            "select id from orders where region in (lower('us-west'))",
            ExpectedInProbe::ExactAfterRewrite,
        ),
        (
            "cast item",
            "select id from orders where region in (cast('us-west' as string))",
            ExpectedInProbe::ExactAfterRewrite,
        ),
        (
            "not in",
            "select id from orders where region not in ('us-west')",
            ExpectedInProbe::ExactAfterRewrite,
        ),
    ];

    for (name, sql, expectation) in sql_cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        match expectation {
            ExpectedInProbe::EmptyBeforeScan => {
                assert!(plan_display.contains("EmptyExec"), "{name}: {plan_display}");
                assert!(scans.is_empty(), "{name}: {plan_display}");
            }
            ExpectedInProbe::ExactAfterRewrite => {
                assert!(
                    !plan_display.contains("FilterExec"),
                    "{name}: {plan_display}"
                );
                assert_eq!(scans.len(), 1, "{name}: {plan_display}");
                assert_eq!(scans[0].scan_plan().pushed_filter_plan.exact_count, 1);
                assert_eq!(scans[0].scan_plan().pushed_filter_plan.unsupported_count, 0);
                assert_eq!(
                    scans[0]
                        .scan_plan()
                        .pushed_filter_plan
                        .residual_filter_count,
                    0
                );
            }
            ExpectedInProbe::ResidualFilter => {
                assert!(
                    plan_display.contains("FilterExec"),
                    "{name} unexpectedly became exact:\n{plan_display}"
                );
                assert_eq!(scans.len(), 1, "{name}: {plan_display}");
                assert_eq!(scans[0].scan_plan().pushed_filter_plan.exact_count, 0);
                assert_eq!(
                    scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
                    0
                );
            }
        }
    }

    Ok(())
}

#[tokio::test]
async fn sql_mixed_boolean_partition_filters_keep_required_residual_filters()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema(
        "sql-mixed-boolean-partition-filters",
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
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    struct SqlMixedBooleanProbe {
        name: &'static str,
        sql: &'static str,
        exact_count: usize,
        residual_filter_count: usize,
    }

    let sql_cases = [
        SqlMixedBooleanProbe {
            name: "partition in and data",
            sql: "select id from orders where region in ('us-west', 'us-east') and id > 1",
            exact_count: 1,
            residual_filter_count: 1,
        },
        SqlMixedBooleanProbe {
            name: "partition in or data",
            sql: "select id from orders where region in ('us-west', 'us-east') or id = 1",
            exact_count: 0,
            residual_filter_count: 0,
        },
        SqlMixedBooleanProbe {
            name: "partition equality or data",
            sql: "select id from orders where region = 'us-west' or id = 1",
            exact_count: 0,
            residual_filter_count: 0,
        },
        SqlMixedBooleanProbe {
            name: "partition in or nested exact partition and data",
            sql: "select id from orders where (region in ('us-west', 'us-east') \
                  or region = 'eu-central') and id > 1",
            exact_count: 1,
            residual_filter_count: 1,
        },
    ];

    for case in sql_cases {
        let dataframe = ctx.sql(case.sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(
            plan_display.contains("FilterExec"),
            "{} should keep a residual filter:\n{}",
            case.name,
            plan_display
        );
        assert_eq!(scans.len(), 1, "{}: {}", case.name, plan_display);
        assert_eq!(
            scans[0].scan_plan().pushed_filter_plan.exact_count,
            case.exact_count,
            "{}: {}",
            case.name,
            plan_display
        );
        assert_eq!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .residual_filter_count,
            case.residual_filter_count,
            "{}: {}",
            case.name,
            plan_display
        );
    }

    Ok(())
}

#[tokio::test]
async fn sql_null_partition_filters_are_exact_kernel_pushdown()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema_and_adds(
        "sql-null-partition-filters-exact",
        PARTITIONED_SCHEMA_FIELDS_JSON,
        r#"["region"]"#,
        &[
            r#""partitionValues":{"region":"us-west"}"#,
            r#""partitionValues":{"region":null}"#,
            r#""partitionValues":{"region":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    let sql_cases = [
        (
            "is null",
            "select id from orders where region is null",
            vec![
                "part-00001.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
            ],
        ),
        (
            "is not null",
            "select id from orders where region is not null",
            vec!["part-00000.parquet"],
        ),
    ];

    for (name, sql, expected_paths) in sql_cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(
            !plan_display.contains("FilterExec"),
            "{name} unexpectedly kept a residual filter:\n{plan_display}"
        );
        assert_eq!(scans.len(), 1, "{name}: {plan_display}");
        assert_eq!(scans[0].scan_plan().pushed_filter_plan.exact_count, 1);
        assert_eq!(scans[0].scan_plan().pushed_filter_plan.unsupported_count, 0);
        assert_eq!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .residual_filter_count,
            0
        );
        assert!(
            scans[0].scan_plan().kernel_partition_predicate.is_some(),
            "{name}"
        );
        assert_eq!(scan_file_paths(scans[0])?, expected_paths, "{name}");

        let kernel_names = scans[0]
            .scan_plan()
            .kernel_scan()
            .kernel_schema()
            .fields()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        assert_eq!(kernel_names, vec!["id", "region"], "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn sql_negated_partition_filters_follow_supported_kernel_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema_and_adds(
        "sql-negated-partition-filters-boundary",
        PARTITIONED_SCHEMA_FIELDS_JSON,
        r#"["region"]"#,
        &[
            r#""partitionValues":{"region":"us-west"}"#,
            r#""partitionValues":{"region":"us-east"}"#,
            r#""partitionValues":{"region":null}"#,
            r#""partitionValues":{"region":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    enum ExpectedNegatedSql {
        ExactKernel {
            exact_count: usize,
            paths: Vec<&'static str>,
        },
    }

    let sql_cases = [
        (
            "not equality",
            "select id from orders where region != 'us-west'",
            ExpectedNegatedSql::ExactKernel {
                exact_count: 1,
                paths: vec!["part-00001.parquet"],
            },
        ),
        (
            "not equality expression",
            "select id from orders where not(region = 'us-west')",
            ExpectedNegatedSql::ExactKernel {
                exact_count: 1,
                paths: vec!["part-00001.parquet"],
            },
        ),
        (
            "not in",
            "select id from orders where region not in ('us-west', 'us-east')",
            ExpectedNegatedSql::ExactKernel {
                exact_count: 2,
                paths: Vec::new(),
            },
        ),
    ];

    for (name, sql, expectation) in sql_cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        match expectation {
            ExpectedNegatedSql::ExactKernel { exact_count, paths } => {
                assert!(
                    !plan_display.contains("FilterExec"),
                    "{name} unexpectedly kept a residual filter:\n{plan_display}"
                );
                assert_eq!(scans.len(), 1, "{name}: {plan_display}");
                assert_eq!(
                    scans[0].scan_plan().pushed_filter_plan.exact_count,
                    exact_count,
                    "{name}: {plan_display}"
                );
                assert_eq!(
                    scans[0]
                        .scan_plan()
                        .pushed_filter_plan
                        .residual_filter_count,
                    0
                );
                assert!(
                    scans[0].scan_plan().kernel_partition_predicate.is_some(),
                    "{name}"
                );
                assert_eq!(scan_file_paths(scans[0])?, paths, "{name}");
            }
        }
    }

    Ok(())
}

#[tokio::test]
async fn sql_partition_comparison_filters_are_exact_kernel_pushdown()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema_and_adds(
        "sql-partition-comparison-filters-exact",
        PARTITIONED_SCHEMA_FIELDS_JSON,
        r#"["region"]"#,
        &[
            r#""partitionValues":{"region":"us-west"}"#,
            r#""partitionValues":{"region":"us-east"}"#,
            r#""partitionValues":{"region":null}"#,
            r#""partitionValues":{"region":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    let sql_cases = [
        (
            "less than",
            "select id from orders where region < 'us-west'",
            vec!["part-00001.parquet"],
        ),
        (
            "less than or equal",
            "select id from orders where region <= 'us-east'",
            vec!["part-00001.parquet"],
        ),
        (
            "greater than",
            "select id from orders where region > 'us-east'",
            vec!["part-00000.parquet"],
        ),
        (
            "reversed greater than",
            "select id from orders where 'us-east' < region",
            vec!["part-00000.parquet"],
        ),
        (
            "between",
            "select id from orders where region between 'us-east' and 'us-west'",
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
        (
            "not between",
            "select id from orders where region not between 'us-east' and 'us-west'",
            Vec::new(),
        ),
        (
            "contradictory between",
            "select id from orders where region between 'z' and 'a'",
            Vec::new(),
        ),
        (
            "contradictory not between",
            "select id from orders where region not between 'z' and 'a'",
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
    ];

    for (name, sql, expected_paths) in sql_cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(
            !plan_display.contains("FilterExec"),
            "{name} unexpectedly kept a residual filter:\n{plan_display}"
        );
        assert_eq!(scans.len(), 1, "{name}: {plan_display}");
        assert!(
            scans[0].scan_plan().pushed_filter_plan.exact_count > 0,
            "{name}: {plan_display}"
        );
        assert_eq!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .residual_filter_count,
            0
        );
        assert!(
            scans[0].scan_plan().kernel_partition_predicate.is_some(),
            "{name}"
        );
        assert_eq!(scan_file_paths(scans[0])?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn sql_empty_string_partition_filters_follow_kernel_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema_and_adds(
        "sql-null-sensitive-partition-filters",
        PARTITIONED_SCHEMA_FIELDS_JSON,
        r#"["region"]"#,
        &[
            r#""partitionValues":{"region":"us-west"}"#,
            r#""partitionValues":{"region":null}"#,
            r#""partitionValues":{"region":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    enum ExpectedEmptyStringSql {
        ExactKernel { paths: Vec<&'static str> },
    }

    let sql_cases = [
        (
            "empty string equality",
            "select id from orders where region = ''",
            ExpectedEmptyStringSql::ExactKernel { paths: Vec::new() },
        ),
        (
            "empty string in",
            "select id from orders where region in ('us-west', '')",
            ExpectedEmptyStringSql::ExactKernel {
                paths: vec!["part-00000.parquet"],
            },
        ),
        (
            "empty string comparison",
            "select id from orders where region < ''",
            ExpectedEmptyStringSql::ExactKernel { paths: Vec::new() },
        ),
        (
            "empty string between",
            "select id from orders where region between '' and 'us-west'",
            ExpectedEmptyStringSql::ExactKernel {
                paths: vec!["part-00000.parquet"],
            },
        ),
    ];

    for (name, sql, expectation) in sql_cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        match expectation {
            ExpectedEmptyStringSql::ExactKernel { paths } => {
                assert!(
                    !plan_display.contains("FilterExec"),
                    "{name} unexpectedly kept a residual filter:\n{plan_display}"
                );
                assert_eq!(scans.len(), 1, "{name}: {plan_display}");
                assert!(
                    scans[0].scan_plan().pushed_filter_plan.exact_count > 0,
                    "{name}: {plan_display}"
                );
                assert!(
                    scans[0].scan_plan().pushed_filter_plan.pushed_filter_count > 0,
                    "{name}: {plan_display}"
                );
                assert!(scans[0].scan_plan().kernel_partition_predicate.is_some());
                assert_eq!(scan_file_paths(scans[0])?, paths, "{name}");
            }
        }
    }

    Ok(())
}

#[tokio::test]
async fn sql_analysis_works_for_join_across_registered_sources()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let _orders = register_fixture_source(&ctx, "orders", "join-orders")?;
    let _customers = register_fixture_source(&ctx, "customers", "join-customers")?;

    let dataframe = ctx
        .sql(
            "select orders.id, customers.customer_name \
             from orders join customers on orders.id = customers.id",
        )
        .await?;
    let optimized = dataframe.into_optimized_plan()?;
    let schema = optimized.schema();

    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(0).data_type(), &DataType::Int32);
    assert_eq!(schema.field(1).name(), "customer_name");
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);

    Ok(())
}

#[test]
fn provider_schema_includes_partition_columns() -> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema(
        "partition-schema",
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
    let schema = provider.schema();

    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(0).data_type(), &DataType::Int32);
    assert_eq!(schema.field(1).name(), "region");
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);

    Ok(())
}

#[test]
fn integer_partition_schema_maps_delta_types_to_arrow_widths()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema(
        "integer-partition-schema",
        INTEGER_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["byte_part","short_part","int_part","long_part"]"#,
        r#""partitionValues":{"byte_part":"-8","short_part":"-1024","int_part":"0","long_part":"9223372036854775807"}"#,
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;

    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let schema = provider.schema();

    assert_eq!(
        schema.field_with_name("byte_part")?.data_type(),
        &DataType::Int8
    );
    assert_eq!(
        schema.field_with_name("short_part")?.data_type(),
        &DataType::Int16
    );
    assert_eq!(
        schema.field_with_name("int_part")?.data_type(),
        &DataType::Int32
    );
    assert_eq!(
        schema.field_with_name("long_part")?.data_type(),
        &DataType::Int64
    );

    Ok(())
}

#[test]
fn boolean_partition_schema_maps_delta_type_to_arrow_boolean()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema(
        "boolean-partition-schema",
        BOOLEAN_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["is_current"]"#,
        r#""partitionValues":{"is_current":"true"}"#,
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;

    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let schema = provider.schema();

    assert_eq!(
        schema.field_with_name("is_current")?.data_type(),
        &DataType::Boolean
    );

    Ok(())
}

#[test]
fn date_partition_schema_maps_delta_type_to_arrow_date32() -> Result<(), Box<dyn std::error::Error>>
{
    let table = DeltaLogTable::new_with_schema(
        "date-partition-schema",
        DATE_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_date"]"#,
        r#""partitionValues":{"event_date":"2026-01-01"}"#,
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;

    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let schema = provider.schema();

    assert_eq!(
        schema.field_with_name("event_date")?.data_type(),
        &DataType::Date32
    );

    Ok(())
}

#[test]
fn decimal_partition_schema_maps_delta_type_to_arrow_decimal128()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema(
        "decimal-partition-schema",
        DECIMAL_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["amount"]"#,
        r#""partitionValues":{"amount":"123.45"}"#,
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;

    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let schema = provider.schema();

    assert_eq!(
        schema.field_with_name("amount")?.data_type(),
        &DataType::Decimal128(10, 2)
    );

    Ok(())
}

#[test]
fn floating_partition_schema_maps_delta_types_to_arrow_float_widths()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema(
        "floating-partition-schema",
        FLOATING_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["float_part","double_part"]"#,
        r#""partitionValues":{"float_part":"1.5","double_part":"-2.25"}"#,
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;

    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let schema = provider.schema();

    assert_eq!(
        schema.field_with_name("float_part")?.data_type(),
        &DataType::Float32
    );
    assert_eq!(
        schema.field_with_name("double_part")?.data_type(),
        &DataType::Float64
    );

    Ok(())
}

#[test]
fn timestamp_partition_schema_maps_delta_type_to_arrow_timestamp_microseconds()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema(
        "timestamp-partition-schema",
        TIMESTAMP_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_ts"]"#,
        r#""partitionValues":{"event_ts":"2026-01-01T00:00:00.123456Z"}"#,
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;

    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let schema = provider.schema();

    assert_eq!(
        schema.field_with_name("event_ts")?.data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );

    Ok(())
}

#[test]
fn timestamp_ntz_partition_schema_maps_delta_type_to_arrow_timestamp_microseconds_without_timezone()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_protocol_and_adds(
        "timestamp-ntz-partition-schema",
        TIMESTAMP_NTZ_PROTOCOL_JSON,
        TIMESTAMP_NTZ_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_ts_ntz"]"#,
        &[r#""partitionValues":{"event_ts_ntz":"2026-01-01 00:00:00.123456"}"#],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;

    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let schema = provider.schema();

    assert_eq!(
        schema.field_with_name("event_ts_ntz")?.data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );

    Ok(())
}

#[test]
fn binary_partition_schema_maps_delta_type_to_arrow_binary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema(
        "binary-partition-schema",
        BINARY_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["payload"]"#,
        r#""partitionValues":{"payload":"hello"}"#,
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;

    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let schema = provider.schema();

    assert_eq!(
        schema.field_with_name("payload")?.data_type(),
        &DataType::Binary
    );

    Ok(())
}

#[tokio::test]
async fn date_partition_null_checks_are_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "date-partition-null-checks-boundary",
        DATE_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_date"]"#,
        &[
            r#""partitionValues":{"event_date":"2026-01-01"}"#,
            r#""partitionValues":{"event_date":"1969-12-31"}"#,
            r#""partitionValues":{"event_date":null}"#,
            r#""partitionValues":{"event_date":""}"#,
            r#""partitionValues":{"event_date":"2027-12-31"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let cases = [
        (
            "is null",
            datafusion::logical_expr::col("event_date").is_null(),
        ),
        (
            "is not null",
            datafusion::logical_expr::col("event_date").is_not_null(),
        ),
    ];

    for (name, filter) in cases {
        let support = provider.supports_filters_pushdown(&[&filter])?;
        assert_eq!(support, vec![TableProviderFilterPushDown::Exact], "{name}");

        provider
            .scan(&state, Some(&vec![0]), &[filter], None)
            .await?;
    }

    Ok(())
}

#[tokio::test]
async fn date_partition_equality_and_membership_are_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "date-partition-equality-membership-boundary",
        DATE_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_date"]"#,
        &[
            r#""partitionValues":{"event_date":"2026-01-01"}"#,
            r#""partitionValues":{"event_date":"2024-02-29"}"#,
            r#""partitionValues":{"event_date":"1969-12-31"}"#,
            r#""partitionValues":{"event_date":null}"#,
            r#""partitionValues":{"event_date":""}"#,
            r#""partitionValues":{"event_date":"2027-12-31"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let new_year_2026 = Expr::Literal(ScalarValue::Date32(Some(20_454)), None);
    let leap_day_2024 = Expr::Literal(ScalarValue::Date32(Some(19_782)), None);
    let pre_epoch_day = Expr::Literal(ScalarValue::Date32(Some(-1)), None);
    let cases = [
        (
            "equality",
            datafusion::logical_expr::col("event_date").eq(new_year_2026.clone()),
        ),
        (
            "reversed equality pre epoch",
            pre_epoch_day.eq(datafusion::logical_expr::col("event_date")),
        ),
        (
            "inequality",
            datafusion::logical_expr::col("event_date").not_eq(new_year_2026.clone()),
        ),
        (
            "in list",
            datafusion::logical_expr::col("event_date").in_list(
                vec![
                    new_year_2026.clone(),
                    leap_day_2024.clone(),
                    new_year_2026.clone(),
                ],
                false,
            ),
        ),
        (
            "not in list",
            datafusion::logical_expr::col("event_date").in_list(vec![new_year_2026.clone()], true),
        ),
    ];

    for (name, filter) in cases {
        let support = provider.supports_filters_pushdown(&[&filter])?;
        assert_eq!(support, vec![TableProviderFilterPushDown::Exact], "{name}");

        provider
            .scan(&state, Some(&vec![0]), &[filter], None)
            .await?;
    }

    Ok(())
}

#[tokio::test]
async fn date_partition_unsafe_literal_shapes_are_rejected_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema(
        "date-partition-unsafe-literal-shapes",
        DATE_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_date"]"#,
        r#""partitionValues":{"event_date":"2026-01-01"}"#,
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let date = Expr::Literal(ScalarValue::Date32(Some(20_454)), None);
    let scalar_udf = create_udf(
        "date_identity_for_pushdown_boundary",
        vec![DataType::Date32],
        DataType::Date32,
        Volatility::Immutable,
        Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Date32(Some(20_454))))),
    );
    let scalar_function =
        Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
            Arc::new(scalar_udf),
            vec![datafusion::logical_expr::col("event_date")],
        ));
    let filters = vec![
        datafusion::logical_expr::col("event_date").eq(datafusion::logical_expr::lit("2026-01-01")),
        datafusion::logical_expr::col("event_date")
            .eq(Expr::Literal(ScalarValue::Date32(None), None)),
        datafusion::logical_expr::col("event_date").eq(Expr::Literal(
            ScalarValue::Date64(Some(1_767_225_600_000)),
            None,
        )),
        datafusion::logical_expr::col("event_date").in_list(Vec::<Expr>::new(), false),
        datafusion::logical_expr::col("event_date").in_list(Vec::<Expr>::new(), true),
        datafusion::logical_expr::col("event_date").in_list(
            vec![date.clone(), Expr::Literal(ScalarValue::Date32(None), None)],
            false,
        ),
        datafusion::logical_expr::col("event_date").in_list(
            vec![date.clone(), datafusion::logical_expr::lit("2024-02-29")],
            false,
        ),
        datafusion::logical_expr::col("event_date")
            .in_list(vec![datafusion::logical_expr::col("id")], false),
        datafusion::logical_expr::col("event_date")
            .between(Expr::Literal(ScalarValue::Date32(None), None), date.clone()),
        datafusion::logical_expr::col("event_date")
            .between(datafusion::logical_expr::col("id"), date.clone()),
        datafusion::logical_expr::col("event_date").eq(datafusion::logical_expr::cast(
            date.clone(),
            DataType::Date32,
        )),
        datafusion::logical_expr::col("event_date").eq(scalar_function),
    ];
    let filter_refs = filters.iter().collect::<Vec<_>>();

    let support = provider.supports_filters_pushdown(&filter_refs)?;
    let plan = provider.plan_supports_filters_pushdown(&filter_refs);

    assert_eq!(
        support,
        vec![TableProviderFilterPushDown::Unsupported; filters.len()]
    );
    assert_eq!(plan.exact_count, 0);
    assert_eq!(plan.unsupported_count, filters.len());
    assert_eq!(plan.residual_filter_count, filters.len());

    for filter in filters {
        let result = provider
            .scan(&state, None, std::slice::from_ref(&filter), None)
            .await;

        assert!(
            matches!(result, Err(DataFusionError::External(error)) if error
            .to_string()
            .contains("pushed filters must be exact partition predicates"))
        );
    }

    Ok(())
}

#[tokio::test]
async fn decimal_partition_unsafe_literal_filters_are_rejected_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema(
        "decimal-partition-unsafe-literal-boundary",
        DECIMAL_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["amount"]"#,
        r#""partitionValues":{"amount":"123.45"}"#,
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let amount = Expr::Literal(ScalarValue::Decimal128(Some(12_345), 10, 2), None);
    let non_exact_scale = Expr::Literal(ScalarValue::Decimal128(Some(12_346), 10, 3), None);
    let decimal256 = Expr::Literal(ScalarValue::Decimal256(Some(12_345.into()), 10, 2), None);
    let scalar_udf = create_udf(
        "decimal_identity_for_pushdown_boundary",
        vec![DataType::Decimal128(10, 2)],
        DataType::Decimal128(10, 2),
        Volatility::Immutable,
        Arc::new(|_| {
            Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                Some(12_345),
                10,
                2,
            )))
        }),
    );
    let scalar_function =
        Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
            Arc::new(scalar_udf),
            vec![datafusion::logical_expr::col("amount")],
        ));
    let filters = vec![
        (
            "non exact scale equality",
            datafusion::logical_expr::col("amount").eq(non_exact_scale.clone()),
        ),
        (
            "non exact scale ordering",
            datafusion::logical_expr::col("amount").gt(non_exact_scale.clone()),
        ),
        (
            "non exact scale in list",
            datafusion::logical_expr::col("amount")
                .in_list(vec![amount.clone(), non_exact_scale.clone()], false),
        ),
        (
            "non exact scale between",
            datafusion::logical_expr::col("amount").between(
                Expr::Literal(ScalarValue::Decimal128(Some(0), 10, 2), None),
                non_exact_scale.clone(),
            ),
        ),
        (
            "decimal256 equality",
            datafusion::logical_expr::col("amount").eq(decimal256),
        ),
        (
            "empty in list",
            datafusion::logical_expr::col("amount").in_list(vec![], false),
        ),
        (
            "empty not in list",
            datafusion::logical_expr::col("amount").in_list(vec![], true),
        ),
        (
            "string equality",
            datafusion::logical_expr::col("amount").eq(datafusion::logical_expr::lit("123.45")),
        ),
        (
            "integer equality",
            datafusion::logical_expr::col("amount").eq(datafusion::logical_expr::lit(123_i64)),
        ),
        (
            "float equality",
            datafusion::logical_expr::col("amount").eq(datafusion::logical_expr::lit(123.45_f64)),
        ),
        (
            "null equality",
            datafusion::logical_expr::col("amount")
                .eq(Expr::Literal(ScalarValue::Decimal128(None, 10, 2), None)),
        ),
        (
            "cast operand",
            datafusion::logical_expr::col("amount").eq(datafusion::logical_expr::cast(
                amount.clone(),
                DataType::Decimal128(10, 2),
            )),
        ),
        (
            "scalar function operand",
            datafusion::logical_expr::col("amount").eq(scalar_function),
        ),
    ];
    let filter_refs = filters.iter().map(|(_, filter)| filter).collect::<Vec<_>>();

    let support = provider.supports_filters_pushdown(&filter_refs)?;
    let plan = provider.plan_supports_filters_pushdown(&filter_refs);

    assert_eq!(
        support,
        vec![TableProviderFilterPushDown::Unsupported; filters.len()]
    );
    assert_eq!(plan.exact_count, 0);
    assert_eq!(plan.unsupported_count, filters.len());
    assert_eq!(plan.residual_filter_count, filters.len());

    for (name, filter) in filters {
        let result = provider
            .scan(&state, None, std::slice::from_ref(&filter), None)
            .await;

        assert!(
            matches!(result, Err(DataFusionError::External(error)) if error
                .to_string()
                .contains("pushed filters must be exact partition predicates")),
            "{name} should be rejected"
        );
    }

    Ok(())
}

#[tokio::test]
async fn timestamp_partition_unsafe_filters_are_rejected_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema(
        "timestamp-partition-unsafe-boundary",
        TIMESTAMP_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_ts"]"#,
        r#""partitionValues":{"event_ts":"2026-01-01T00:00:00.123456Z"}"#,
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let timestamp_utc = Expr::Literal(
        ScalarValue::TimestampMicrosecond(
            Some(1_767_225_600_123_456),
            Some(Arc::<str>::from("UTC")),
        ),
        None,
    );
    let timestamp_empty_timezone = Expr::Literal(
        ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_456), Some(Arc::<str>::from(""))),
        None,
    );
    let timestamp_nanosecond = Expr::Literal(
        ScalarValue::TimestampNanosecond(
            Some(1_767_225_600_123_456_000),
            Some(Arc::<str>::from("UTC")),
        ),
        None,
    );
    let timestamp_null = Expr::Literal(
        ScalarValue::TimestampMicrosecond(None, Some(Arc::<str>::from("UTC"))),
        None,
    );
    let scalar_udf = create_udf(
        "timestamp_identity_for_pushdown_boundary",
        vec![DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        )],
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        Volatility::Immutable,
        Arc::new(|_| {
            Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                Some(1_767_225_600_123_456),
                Some(Arc::<str>::from("UTC")),
            )))
        }),
    );
    let scalar_function =
        Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
            Arc::new(scalar_udf),
            vec![datafusion::logical_expr::col("event_ts")],
        ));
    let filters = vec![
        (
            "timestamp null in list",
            datafusion::logical_expr::col("event_ts").in_list(vec![timestamp_null], false),
        ),
        (
            "timestamp empty timezone literal",
            datafusion::logical_expr::col("event_ts").eq(timestamp_empty_timezone),
        ),
        (
            "timestamp nanosecond literal",
            datafusion::logical_expr::col("event_ts").eq(timestamp_nanosecond),
        ),
        (
            "timestamp null literal",
            datafusion::logical_expr::col("event_ts").eq(Expr::Literal(
                ScalarValue::TimestampMicrosecond(None, Some(Arc::<str>::from("UTC"))),
                None,
            )),
        ),
        (
            "cast operand",
            datafusion::logical_expr::col("event_ts").eq(datafusion::logical_expr::cast(
                timestamp_utc.clone(),
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            )),
        ),
        (
            "scalar function operand",
            datafusion::logical_expr::col("event_ts").eq(scalar_function),
        ),
        (
            "mixed partition data equality",
            datafusion::logical_expr::col("event_ts").eq(datafusion::logical_expr::col("id")),
        ),
    ];
    let filter_refs = filters.iter().map(|(_, filter)| filter).collect::<Vec<_>>();

    let support = provider.supports_filters_pushdown(&filter_refs)?;
    let plan = provider.plan_supports_filters_pushdown(&filter_refs);

    assert_eq!(
        support,
        vec![TableProviderFilterPushDown::Unsupported; filters.len()]
    );
    assert_eq!(plan.exact_count, 0);
    assert_eq!(plan.unsupported_count, filters.len());
    assert_eq!(plan.residual_filter_count, filters.len());

    for (name, filter) in filters {
        let result = provider
            .scan(&state, None, std::slice::from_ref(&filter), None)
            .await;

        assert!(
            matches!(result, Err(DataFusionError::External(error)) if error
                .to_string()
                .contains("pushed filters must be exact partition predicates")),
            "{name} should be rejected"
        );
    }

    Ok(())
}

#[tokio::test]
async fn binary_partition_null_checks_are_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "binary-partition-null-checks-boundary",
        BINARY_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["payload"]"#,
        &[
            r#""partitionValues":{"payload":"hello"}"#,
            r#""partitionValues":{"payload":"world"}"#,
            r#""partitionValues":{"payload":null}"#,
            r#""partitionValues":{"payload":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let cases = [
        (
            "is null",
            datafusion::logical_expr::col("payload").is_null(),
            vec![
                "part-00002.parquet",
                "part-00003.parquet",
                "part-00004.parquet",
            ],
        ),
        (
            "is not null",
            datafusion::logical_expr::col("payload").is_not_null(),
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
    ];

    for (name, filter, expected_paths) in cases {
        let support = provider.supports_filters_pushdown(&[&filter])?;
        assert_eq!(support, vec![TableProviderFilterPushDown::Exact], "{name}");

        let plan = provider
            .scan(&state, Some(&vec![0]), &[filter], None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;
        let scan_plan = scan.scan_plan();
        let kernel_names = scan_plan
            .kernel_scan()
            .kernel_schema()
            .fields()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();

        assert_eq!(scan_plan.projected_schema.field(0).name(), "id", "{name}");
        assert_eq!(kernel_names, vec!["id", "payload"], "{name}");
        assert_eq!(scan_plan.pushed_filter_plan.exact_count, 1);
        assert_eq!(scan_plan.pushed_filter_plan.unsupported_count, 0);
        assert_eq!(scan_plan.pushed_filter_plan.residual_filter_count, 0);
        assert_eq!(scan_plan.pushed_filter_plan.pushed_filter_count, 1);
        assert!(scan_plan.kernel_partition_predicate.is_some(), "{name}");
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn binary_partition_equality_and_membership_are_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "binary-partition-equality-membership-boundary",
        BINARY_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["payload"]"#,
        &[
            r#""partitionValues":{"payload":"hello"}"#,
            r#""partitionValues":{"payload":"world"}"#,
            r#""partitionValues":{"payload":"/=%"}"#,
            r#""partitionValues":{"payload":null}"#,
            r#""partitionValues":{"payload":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let hello = Expr::Literal(ScalarValue::Binary(Some(b"hello".to_vec())), None);
    let world = Expr::Literal(ScalarValue::Binary(Some(b"world".to_vec())), None);
    let slash_equals_percent = Expr::Literal(ScalarValue::Binary(Some(b"/=%".to_vec())), None);
    let cases = [
        (
            "equality",
            datafusion::logical_expr::col("payload").eq(hello.clone()),
            vec!["part-00000.parquet"],
        ),
        (
            "reversed equality",
            slash_equals_percent
                .clone()
                .eq(datafusion::logical_expr::col("payload")),
            vec!["part-00002.parquet"],
        ),
        (
            "inequality",
            datafusion::logical_expr::col("payload").not_eq(hello.clone()),
            vec!["part-00001.parquet", "part-00002.parquet"],
        ),
        (
            "in list",
            datafusion::logical_expr::col("payload").in_list(
                vec![hello.clone(), slash_equals_percent.clone(), hello.clone()],
                false,
            ),
            vec!["part-00000.parquet", "part-00002.parquet"],
        ),
        (
            "not in list",
            datafusion::logical_expr::col("payload").in_list(vec![world], true),
            vec!["part-00000.parquet", "part-00002.parquet"],
        ),
    ];

    for (name, filter, expected_paths) in cases {
        let support = provider.supports_filters_pushdown(&[&filter])?;
        assert_eq!(support, vec![TableProviderFilterPushDown::Exact], "{name}");

        let plan = provider
            .scan(&state, Some(&vec![0]), &[filter], None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;
        let scan_plan = scan.scan_plan();
        let kernel_names = scan_plan
            .kernel_scan()
            .kernel_schema()
            .fields()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();

        assert_eq!(scan_plan.projected_schema.field(0).name(), "id", "{name}");
        assert_eq!(kernel_names, vec!["id", "payload"], "{name}");
        assert_eq!(scan_plan.pushed_filter_plan.exact_count, 1);
        assert_eq!(scan_plan.pushed_filter_plan.unsupported_count, 0);
        assert_eq!(scan_plan.pushed_filter_plan.residual_filter_count, 0);
        assert_eq!(scan_plan.pushed_filter_plan.pushed_filter_count, 1);
        assert!(scan_plan.kernel_partition_predicate.is_some(), "{name}");
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn binary_partition_boolean_composition_and_projection_are_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "binary-partition-boolean-composition-boundary",
        BINARY_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["payload"]"#,
        &[
            r#""partitionValues":{"payload":"hello"}"#,
            r#""partitionValues":{"payload":"world"}"#,
            r#""partitionValues":{"payload":"/=%"}"#,
            r#""partitionValues":{"payload":null}"#,
            r#""partitionValues":{"payload":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let hello = Expr::Literal(ScalarValue::Binary(Some(b"hello".to_vec())), None);
    let world = Expr::Literal(ScalarValue::Binary(Some(b"world".to_vec())), None);
    let slash_equals_percent = Expr::Literal(ScalarValue::Binary(Some(b"/=%".to_vec())), None);
    let separate_and_filters = vec![
        datafusion::logical_expr::col("payload").is_not_null(),
        datafusion::logical_expr::col("payload").not_eq(world.clone()),
    ];
    let whole_and_filter = datafusion::logical_expr::col("payload")
        .in_list(
            vec![hello.clone(), slash_equals_percent.clone(), hello.clone()],
            false,
        )
        .and(datafusion::logical_expr::col("payload").is_not_null());
    let whole_or_filter = datafusion::logical_expr::col("payload")
        .eq(hello.clone())
        .or(datafusion::logical_expr::col("payload").is_null());
    let whole_not_filter = Expr::Not(Box::new(datafusion::logical_expr::col("payload").eq(hello)));
    let cases = [
        (
            "separate filters combine with and",
            separate_and_filters,
            vec!["part-00000.parquet", "part-00002.parquet"],
        ),
        (
            "whole and",
            vec![whole_and_filter],
            vec!["part-00000.parquet", "part-00002.parquet"],
        ),
        (
            "whole or",
            vec![whole_or_filter],
            vec![
                "part-00000.parquet",
                "part-00003.parquet",
                "part-00004.parquet",
                "part-00005.parquet",
            ],
        ),
        (
            "whole not",
            vec![whole_not_filter],
            vec!["part-00001.parquet", "part-00002.parquet"],
        ),
    ];

    for (name, filters, expected_paths) in cases {
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let support = provider.supports_filters_pushdown(&filter_refs)?;
        assert_eq!(
            support,
            vec![TableProviderFilterPushDown::Exact; filters.len()],
            "{name}"
        );

        let plan = provider
            .scan(&state, Some(&vec![0]), &filters, None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;
        let scan_plan = scan.scan_plan();
        let kernel_names = scan_plan
            .kernel_scan()
            .kernel_schema()
            .fields()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();

        assert_eq!(scan_plan.projected_schema.field(0).name(), "id", "{name}");
        assert_eq!(kernel_names, vec!["id", "payload"], "{name}");
        assert_eq!(scan_plan.pushed_filter_plan.exact_count, filters.len());
        assert_eq!(scan_plan.pushed_filter_plan.unsupported_count, 0);
        assert_eq!(scan_plan.pushed_filter_plan.residual_filter_count, 0);
        assert_eq!(
            scan_plan.pushed_filter_plan.pushed_filter_count,
            filters.len()
        );
        assert!(scan_plan.kernel_partition_predicate.is_some(), "{name}");
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn binary_partition_unsafe_filters_are_rejected_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema(
        "binary-partition-unsupported-boundary",
        BINARY_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["payload"]"#,
        r#""partitionValues":{"payload":"hello"}"#,
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let payload = Expr::Literal(ScalarValue::Binary(Some(b"hello".to_vec())), None);
    let payload_null = Expr::Literal(ScalarValue::Binary(None), None);
    let scalar_udf = create_udf(
        "binary_identity_for_pushdown_boundary",
        vec![DataType::Binary],
        DataType::Binary,
        Volatility::Immutable,
        Arc::new(|_| {
            Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(
                b"hello".to_vec(),
            ))))
        }),
    );
    let scalar_function =
        Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
            Arc::new(scalar_udf),
            vec![datafusion::logical_expr::col("payload")],
        ));
    let filters = vec![
        (
            "binary ordering",
            datafusion::logical_expr::col("payload").gt(payload.clone()),
        ),
        (
            "binary between",
            datafusion::logical_expr::col("payload").between(payload.clone(), payload.clone()),
        ),
        (
            "binary null literal",
            datafusion::logical_expr::col("payload").eq(payload_null),
        ),
        (
            "binary empty literal",
            datafusion::logical_expr::col("payload")
                .eq(Expr::Literal(ScalarValue::Binary(Some(Vec::new())), None)),
        ),
        (
            "binary empty literal in list",
            datafusion::logical_expr::col("payload").in_list(
                vec![
                    payload.clone(),
                    Expr::Literal(ScalarValue::Binary(Some(Vec::new())), None),
                ],
                false,
            ),
        ),
        (
            "string literal",
            datafusion::logical_expr::col("payload").eq(datafusion::logical_expr::lit("hello")),
        ),
        (
            "cast operand",
            datafusion::logical_expr::col("payload").eq(datafusion::logical_expr::cast(
                payload.clone(),
                DataType::Binary,
            )),
        ),
        (
            "scalar function operand",
            datafusion::logical_expr::col("payload").eq(scalar_function),
        ),
        (
            "mixed partition data equality",
            datafusion::logical_expr::col("payload").eq(datafusion::logical_expr::col("id")),
        ),
    ];
    let filter_refs = filters.iter().map(|(_, filter)| filter).collect::<Vec<_>>();

    let support = provider.supports_filters_pushdown(&filter_refs)?;
    let plan = provider.plan_supports_filters_pushdown(&filter_refs);

    assert_eq!(
        support,
        vec![TableProviderFilterPushDown::Unsupported; filters.len()]
    );
    assert_eq!(plan.exact_count, 0);
    assert_eq!(plan.unsupported_count, filters.len());
    assert_eq!(plan.residual_filter_count, filters.len());

    for (name, filter) in filters {
        let result = provider
            .scan(&state, None, std::slice::from_ref(&filter), None)
            .await;

        assert!(
            matches!(result, Err(DataFusionError::External(error)) if error
                .to_string()
                .contains("pushed filters must be exact partition predicates")),
            "{name} should be rejected"
        );
    }

    Ok(())
}

#[tokio::test]
async fn timestamp_ntz_partition_unsafe_filters_are_rejected_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_protocol_and_adds(
        "timestamp-ntz-partition-unsafe-boundary",
        TIMESTAMP_NTZ_PROTOCOL_JSON,
        TIMESTAMP_NTZ_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_ts_ntz"]"#,
        &[r#""partitionValues":{"event_ts_ntz":"2026-01-01 00:00:00.123456"}"#],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let timestamp_ntz = Expr::Literal(
        ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_456), None),
        None,
    );
    let timestamp_utc = Expr::Literal(
        ScalarValue::TimestampMicrosecond(
            Some(1_767_225_600_123_456),
            Some(Arc::<str>::from("UTC")),
        ),
        None,
    );
    let timestamp_null = Expr::Literal(ScalarValue::TimestampMicrosecond(None, None), None);
    let scalar_udf = create_udf(
        "timestamp_ntz_identity_for_pushdown_boundary",
        vec![DataType::Timestamp(TimeUnit::Microsecond, None)],
        DataType::Timestamp(TimeUnit::Microsecond, None),
        Volatility::Immutable,
        Arc::new(|_| {
            Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                Some(1_767_225_600_123_456),
                None,
            )))
        }),
    );
    let scalar_function =
        Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
            Arc::new(scalar_udf),
            vec![datafusion::logical_expr::col("event_ts_ntz")],
        ));
    let filters = vec![
        (
            "timestamp_ntz null in list",
            datafusion::logical_expr::col("event_ts_ntz")
                .in_list(vec![timestamp_null.clone()], false),
        ),
        (
            "timestamp_ntz utc timezone literal",
            datafusion::logical_expr::col("event_ts_ntz").eq(timestamp_utc),
        ),
        (
            "timestamp_ntz null literal",
            datafusion::logical_expr::col("event_ts_ntz").eq(timestamp_null),
        ),
        (
            "cast operand",
            datafusion::logical_expr::col("event_ts_ntz").eq(datafusion::logical_expr::cast(
                timestamp_ntz.clone(),
                DataType::Timestamp(TimeUnit::Microsecond, None),
            )),
        ),
        (
            "scalar function operand",
            datafusion::logical_expr::col("event_ts_ntz").eq(scalar_function),
        ),
        (
            "mixed partition data equality",
            datafusion::logical_expr::col("event_ts_ntz").eq(datafusion::logical_expr::col("id")),
        ),
    ];
    let filter_refs = filters.iter().map(|(_, filter)| filter).collect::<Vec<_>>();

    let support = provider.supports_filters_pushdown(&filter_refs)?;
    let plan = provider.plan_supports_filters_pushdown(&filter_refs);

    assert_eq!(
        support,
        vec![TableProviderFilterPushDown::Unsupported; filters.len()]
    );
    assert_eq!(plan.exact_count, 0);
    assert_eq!(plan.unsupported_count, filters.len());
    assert_eq!(plan.residual_filter_count, filters.len());

    for (name, filter) in filters {
        let result = provider
            .scan(&state, None, std::slice::from_ref(&filter), None)
            .await;

        assert!(
            matches!(result, Err(DataFusionError::External(error)) if error
                .to_string()
                .contains("pushed filters must be exact partition predicates")),
            "{name} should be rejected"
        );
    }

    Ok(())
}

#[tokio::test]
async fn timestamp_partition_equality_and_membership_are_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "timestamp-partition-equality-membership-boundary",
        TIMESTAMP_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_ts"]"#,
        &[
            r#""partitionValues":{"event_ts":"2026-01-01T00:00:00.123456Z"}"#,
            r#""partitionValues":{"event_ts":"2025-12-31T23:59:59.999999Z"}"#,
            r#""partitionValues":{"event_ts":"2026-01-01T00:00:00.123457Z"}"#,
            r#""partitionValues":{"event_ts":null}"#,
            r#""partitionValues":{"event_ts":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let timestamp = Expr::Literal(
        ScalarValue::TimestampMicrosecond(
            Some(1_767_225_600_123_456),
            Some(Arc::<str>::from("UTC")),
        ),
        None,
    );
    let timestamp_named_timezone = Expr::Literal(
        ScalarValue::TimestampMicrosecond(
            Some(1_767_225_600_123_456),
            Some(Arc::<str>::from("America/Phoenix")),
        ),
        None,
    );
    let timestamp_offset_timezone = Expr::Literal(
        ScalarValue::TimestampMicrosecond(
            Some(1_767_225_600_123_456),
            Some(Arc::<str>::from("-07:00")),
        ),
        None,
    );
    let low = Expr::Literal(
        ScalarValue::TimestampMicrosecond(
            Some(1_767_225_599_999_999),
            Some(Arc::<str>::from("UTC")),
        ),
        None,
    );
    let cases = [
        (
            "timestamp equality",
            datafusion::logical_expr::col("event_ts").eq(timestamp.clone()),
            vec!["part-00000.parquet"],
        ),
        (
            "timestamp named timezone equality",
            datafusion::logical_expr::col("event_ts").eq(timestamp_named_timezone),
            vec!["part-00000.parquet"],
        ),
        (
            "timestamp offset timezone equality",
            datafusion::logical_expr::col("event_ts").eq(timestamp_offset_timezone),
            vec!["part-00000.parquet"],
        ),
        (
            "reversed timestamp equality",
            timestamp
                .clone()
                .eq(datafusion::logical_expr::col("event_ts")),
            vec!["part-00000.parquet"],
        ),
        (
            "timestamp inequality",
            datafusion::logical_expr::col("event_ts").not_eq(timestamp.clone()),
            vec!["part-00001.parquet", "part-00002.parquet"],
        ),
        (
            "timestamp in list",
            datafusion::logical_expr::col("event_ts").in_list(
                vec![timestamp.clone(), low.clone(), timestamp.clone()],
                false,
            ),
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
        (
            "timestamp not in list",
            datafusion::logical_expr::col("event_ts").in_list(vec![timestamp], true),
            vec!["part-00001.parquet", "part-00002.parquet"],
        ),
    ];

    for (name, filter, expected_paths) in cases {
        let support = provider.supports_filters_pushdown(&[&filter])?;
        assert_eq!(support, vec![TableProviderFilterPushDown::Exact], "{name}");

        let plan = provider
            .scan(&state, Some(&vec![0]), &[filter], None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;
        let scan_plan = scan.scan_plan();

        assert_eq!(scan_plan.projected_schema.field(0).name(), "id", "{name}");
        assert_eq!(scan_plan.pushed_filter_plan.exact_count, 1, "{name}");
        assert_eq!(scan_plan.pushed_filter_plan.unsupported_count, 0, "{name}");
        assert_eq!(
            scan_plan.pushed_filter_plan.residual_filter_count, 0,
            "{name}"
        );
        assert_eq!(
            scan_plan.pushed_filter_plan.pushed_filter_count, 1,
            "{name}"
        );
        assert!(scan_plan.kernel_partition_predicate.is_some(), "{name}");
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn timestamp_ntz_partition_equality_and_membership_are_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_protocol_and_adds(
        "timestamp-ntz-partition-equality-membership-boundary",
        TIMESTAMP_NTZ_PROTOCOL_JSON,
        TIMESTAMP_NTZ_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_ts_ntz"]"#,
        &[
            r#""partitionValues":{"event_ts_ntz":"2026-01-01 00:00:00.123456"}"#,
            r#""partitionValues":{"event_ts_ntz":"2025-12-31 23:59:59.999999"}"#,
            r#""partitionValues":{"event_ts_ntz":"2026-01-01 00:00:00.123457"}"#,
            r#""partitionValues":{"event_ts_ntz":null}"#,
            r#""partitionValues":{"event_ts_ntz":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let timestamp = Expr::Literal(
        ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_456), None),
        None,
    );
    let low = Expr::Literal(
        ScalarValue::TimestampMicrosecond(Some(1_767_225_599_999_999), None),
        None,
    );
    let cases = [
        (
            "timestamp_ntz equality",
            datafusion::logical_expr::col("event_ts_ntz").eq(timestamp.clone()),
            vec!["part-00000.parquet"],
        ),
        (
            "reversed timestamp_ntz equality",
            timestamp
                .clone()
                .eq(datafusion::logical_expr::col("event_ts_ntz")),
            vec!["part-00000.parquet"],
        ),
        (
            "timestamp_ntz inequality",
            datafusion::logical_expr::col("event_ts_ntz").not_eq(timestamp.clone()),
            vec!["part-00001.parquet", "part-00002.parquet"],
        ),
        (
            "timestamp_ntz in list",
            datafusion::logical_expr::col("event_ts_ntz").in_list(
                vec![timestamp.clone(), low.clone(), timestamp.clone()],
                false,
            ),
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
        (
            "timestamp_ntz not in list",
            datafusion::logical_expr::col("event_ts_ntz").in_list(vec![timestamp], true),
            vec!["part-00001.parquet", "part-00002.parquet"],
        ),
    ];

    for (name, filter, expected_paths) in cases {
        let support = provider.supports_filters_pushdown(&[&filter])?;
        assert_eq!(support, vec![TableProviderFilterPushDown::Exact], "{name}");

        let plan = provider
            .scan(&state, Some(&vec![0]), &[filter], None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;
        let scan_plan = scan.scan_plan();

        assert_eq!(scan_plan.projected_schema.field(0).name(), "id", "{name}");
        assert_eq!(scan_plan.pushed_filter_plan.exact_count, 1, "{name}");
        assert_eq!(scan_plan.pushed_filter_plan.unsupported_count, 0, "{name}");
        assert_eq!(
            scan_plan.pushed_filter_plan.residual_filter_count, 0,
            "{name}"
        );
        assert_eq!(
            scan_plan.pushed_filter_plan.pushed_filter_count, 1,
            "{name}"
        );
        assert!(scan_plan.kernel_partition_predicate.is_some(), "{name}");
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn timestamp_partition_comparisons_and_between_are_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "timestamp-partition-comparisons-between-boundary",
        TIMESTAMP_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_ts"]"#,
        &[
            r#""partitionValues":{"event_ts":"2026-01-01T00:00:00.123456Z"}"#,
            r#""partitionValues":{"event_ts":"2025-12-31T23:59:59.999999Z"}"#,
            r#""partitionValues":{"event_ts":"2026-01-01T00:00:00.123457Z"}"#,
            r#""partitionValues":{"event_ts":"1969-12-31T23:59:59.999999Z"}"#,
            r#""partitionValues":{"event_ts":null}"#,
            r#""partitionValues":{"event_ts":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let target = Expr::Literal(
        ScalarValue::TimestampMicrosecond(
            Some(1_767_225_600_123_456),
            Some(Arc::<str>::from("UTC")),
        ),
        None,
    );
    let low = Expr::Literal(
        ScalarValue::TimestampMicrosecond(
            Some(1_767_225_599_999_999),
            Some(Arc::<str>::from("UTC")),
        ),
        None,
    );
    let high = Expr::Literal(
        ScalarValue::TimestampMicrosecond(
            Some(1_767_225_600_123_457),
            Some(Arc::<str>::from("UTC")),
        ),
        None,
    );
    let cases = [
        (
            "timestamp less than",
            datafusion::logical_expr::col("event_ts").lt(target.clone()),
            vec!["part-00001.parquet", "part-00003.parquet"],
        ),
        (
            "timestamp less than or equal",
            datafusion::logical_expr::col("event_ts").lt_eq(low.clone()),
            vec!["part-00001.parquet", "part-00003.parquet"],
        ),
        (
            "timestamp greater than",
            datafusion::logical_expr::col("event_ts").gt(low.clone()),
            vec!["part-00000.parquet", "part-00002.parquet"],
        ),
        (
            "reversed timestamp greater than",
            high.clone().gt(datafusion::logical_expr::col("event_ts")),
            vec![
                "part-00000.parquet",
                "part-00001.parquet",
                "part-00003.parquet",
            ],
        ),
        (
            "timestamp greater than or equal",
            datafusion::logical_expr::col("event_ts").gt_eq(target.clone()),
            vec!["part-00000.parquet", "part-00002.parquet"],
        ),
        (
            "timestamp between",
            datafusion::logical_expr::col("event_ts").between(low.clone(), target.clone()),
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
        (
            "timestamp not between",
            datafusion::logical_expr::col("event_ts").not_between(low.clone(), target.clone()),
            vec!["part-00002.parquet", "part-00003.parquet"],
        ),
        (
            "timestamp contradictory between",
            datafusion::logical_expr::col("event_ts").between(high.clone(), low.clone()),
            Vec::new(),
        ),
        (
            "timestamp contradictory not between",
            datafusion::logical_expr::col("event_ts").not_between(high, low.clone()),
            vec![
                "part-00000.parquet",
                "part-00001.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
            ],
        ),
        (
            "timestamp and composition",
            datafusion::logical_expr::col("event_ts")
                .gt(low)
                .and(datafusion::logical_expr::col("event_ts").lt_eq(target)),
            vec!["part-00000.parquet"],
        ),
    ];

    for (name, filter, expected_paths) in cases {
        let support = provider.supports_filters_pushdown(&[&filter])?;
        assert_eq!(support, vec![TableProviderFilterPushDown::Exact], "{name}");

        let plan = provider
            .scan(&state, Some(&vec![0]), &[filter], None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;
        let scan_plan = scan.scan_plan();

        assert_eq!(scan_plan.projected_schema.field(0).name(), "id", "{name}");
        assert_eq!(scan_plan.pushed_filter_plan.exact_count, 1, "{name}");
        assert_eq!(scan_plan.pushed_filter_plan.unsupported_count, 0, "{name}");
        assert_eq!(
            scan_plan.pushed_filter_plan.residual_filter_count, 0,
            "{name}"
        );
        assert_eq!(
            scan_plan.pushed_filter_plan.pushed_filter_count, 1,
            "{name}"
        );
        assert!(scan_plan.kernel_partition_predicate.is_some(), "{name}");
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn timestamp_ntz_partition_comparisons_and_between_are_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_protocol_and_adds(
        "timestamp-ntz-partition-comparisons-between-boundary",
        TIMESTAMP_NTZ_PROTOCOL_JSON,
        TIMESTAMP_NTZ_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_ts_ntz"]"#,
        &[
            r#""partitionValues":{"event_ts_ntz":"2026-01-01 00:00:00.123456"}"#,
            r#""partitionValues":{"event_ts_ntz":"2025-12-31 23:59:59.999999"}"#,
            r#""partitionValues":{"event_ts_ntz":"2026-01-01 00:00:00.123457"}"#,
            r#""partitionValues":{"event_ts_ntz":"1969-12-31 23:59:59.999999"}"#,
            r#""partitionValues":{"event_ts_ntz":null}"#,
            r#""partitionValues":{"event_ts_ntz":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let target = Expr::Literal(
        ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_456), None),
        None,
    );
    let low = Expr::Literal(
        ScalarValue::TimestampMicrosecond(Some(1_767_225_599_999_999), None),
        None,
    );
    let high = Expr::Literal(
        ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_457), None),
        None,
    );
    let cases = [
        (
            "timestamp_ntz less than",
            datafusion::logical_expr::col("event_ts_ntz").lt(target.clone()),
            vec!["part-00001.parquet", "part-00003.parquet"],
        ),
        (
            "timestamp_ntz less than or equal",
            datafusion::logical_expr::col("event_ts_ntz").lt_eq(low.clone()),
            vec!["part-00001.parquet", "part-00003.parquet"],
        ),
        (
            "timestamp_ntz greater than",
            datafusion::logical_expr::col("event_ts_ntz").gt(low.clone()),
            vec!["part-00000.parquet", "part-00002.parquet"],
        ),
        (
            "reversed timestamp_ntz greater than",
            high.clone()
                .gt(datafusion::logical_expr::col("event_ts_ntz")),
            vec![
                "part-00000.parquet",
                "part-00001.parquet",
                "part-00003.parquet",
            ],
        ),
        (
            "timestamp_ntz greater than or equal",
            datafusion::logical_expr::col("event_ts_ntz").gt_eq(target.clone()),
            vec!["part-00000.parquet", "part-00002.parquet"],
        ),
        (
            "timestamp_ntz between",
            datafusion::logical_expr::col("event_ts_ntz").between(low.clone(), target.clone()),
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
        (
            "timestamp_ntz not between",
            datafusion::logical_expr::col("event_ts_ntz").not_between(low.clone(), target.clone()),
            vec!["part-00002.parquet", "part-00003.parquet"],
        ),
        (
            "timestamp_ntz contradictory between",
            datafusion::logical_expr::col("event_ts_ntz").between(high.clone(), low.clone()),
            Vec::new(),
        ),
        (
            "timestamp_ntz contradictory not between",
            datafusion::logical_expr::col("event_ts_ntz").not_between(high, low.clone()),
            vec![
                "part-00000.parquet",
                "part-00001.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
            ],
        ),
        (
            "timestamp_ntz and composition",
            datafusion::logical_expr::col("event_ts_ntz")
                .gt(low)
                .and(datafusion::logical_expr::col("event_ts_ntz").lt_eq(target)),
            vec!["part-00000.parquet"],
        ),
    ];

    for (name, filter, expected_paths) in cases {
        let support = provider.supports_filters_pushdown(&[&filter])?;
        assert_eq!(support, vec![TableProviderFilterPushDown::Exact], "{name}");

        let plan = provider
            .scan(&state, Some(&vec![0]), &[filter], None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;
        let scan_plan = scan.scan_plan();

        assert_eq!(scan_plan.projected_schema.field(0).name(), "id", "{name}");
        assert_eq!(scan_plan.pushed_filter_plan.exact_count, 1, "{name}");
        assert_eq!(scan_plan.pushed_filter_plan.unsupported_count, 0, "{name}");
        assert_eq!(
            scan_plan.pushed_filter_plan.residual_filter_count, 0,
            "{name}"
        );
        assert_eq!(
            scan_plan.pushed_filter_plan.pushed_filter_count, 1,
            "{name}"
        );
        assert!(scan_plan.kernel_partition_predicate.is_some(), "{name}");
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn timestamp_partition_boolean_composition_and_projection_are_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "timestamp-partition-boolean-composition-boundary",
        TIMESTAMP_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_ts"]"#,
        &[
            r#""partitionValues":{"event_ts":"2026-01-01T00:00:00.123456Z"}"#,
            r#""partitionValues":{"event_ts":"2025-12-31T23:59:59.999999Z"}"#,
            r#""partitionValues":{"event_ts":"2026-01-01T00:00:00.123457Z"}"#,
            r#""partitionValues":{"event_ts":null}"#,
            r#""partitionValues":{"event_ts":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let target = Expr::Literal(
        ScalarValue::TimestampMicrosecond(
            Some(1_767_225_600_123_456),
            Some(Arc::<str>::from("UTC")),
        ),
        None,
    );
    let low = Expr::Literal(
        ScalarValue::TimestampMicrosecond(
            Some(1_767_225_599_999_999),
            Some(Arc::<str>::from("UTC")),
        ),
        None,
    );
    let high = Expr::Literal(
        ScalarValue::TimestampMicrosecond(
            Some(1_767_225_600_123_457),
            Some(Arc::<str>::from("UTC")),
        ),
        None,
    );
    let separate_and_filters = vec![
        datafusion::logical_expr::col("event_ts").gt_eq(low.clone()),
        datafusion::logical_expr::col("event_ts").lt(high.clone()),
    ];
    let whole_and_filter = datafusion::logical_expr::col("event_ts")
        .gt_eq(low.clone())
        .and(datafusion::logical_expr::col("event_ts").lt(high.clone()));
    let whole_or_filter = datafusion::logical_expr::col("event_ts")
        .eq(target.clone())
        .or(datafusion::logical_expr::col("event_ts").eq(high.clone()));
    let whole_not_filter = Expr::Not(Box::new(
        datafusion::logical_expr::col("event_ts").eq(target),
    ));
    let cases = [
        (
            "separate filters combine with and",
            separate_and_filters,
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
        (
            "whole and",
            vec![whole_and_filter],
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
        (
            "whole or",
            vec![whole_or_filter],
            vec!["part-00000.parquet", "part-00002.parquet"],
        ),
        (
            "whole not",
            vec![whole_not_filter],
            vec!["part-00001.parquet", "part-00002.parquet"],
        ),
    ];

    for (name, filters, expected_paths) in cases {
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let support = provider.supports_filters_pushdown(&filter_refs)?;
        assert_eq!(
            support,
            vec![TableProviderFilterPushDown::Exact; filters.len()],
            "{name}"
        );

        let plan = provider
            .scan(&state, Some(&vec![0]), &filters, None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;
        let scan_plan = scan.scan_plan();

        assert_eq!(scan_plan.projected_schema.field(0).name(), "id", "{name}");
        assert_eq!(scan_plan.pushed_filter_plan.exact_count, filters.len());
        assert_eq!(scan_plan.pushed_filter_plan.unsupported_count, 0, "{name}");
        assert_eq!(
            scan_plan.pushed_filter_plan.residual_filter_count, 0,
            "{name}"
        );
        assert_eq!(
            scan_plan.pushed_filter_plan.pushed_filter_count,
            filters.len()
        );
        assert!(scan_plan.kernel_partition_predicate.is_some(), "{name}");
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn timestamp_ntz_partition_boolean_composition_and_projection_are_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_protocol_and_adds(
        "timestamp-ntz-partition-boolean-composition-boundary",
        TIMESTAMP_NTZ_PROTOCOL_JSON,
        TIMESTAMP_NTZ_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_ts_ntz"]"#,
        &[
            r#""partitionValues":{"event_ts_ntz":"2026-01-01 00:00:00.123456"}"#,
            r#""partitionValues":{"event_ts_ntz":"2025-12-31 23:59:59.999999"}"#,
            r#""partitionValues":{"event_ts_ntz":"2026-01-01 00:00:00.123457"}"#,
            r#""partitionValues":{"event_ts_ntz":null}"#,
            r#""partitionValues":{"event_ts_ntz":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let target = Expr::Literal(
        ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_456), None),
        None,
    );
    let low = Expr::Literal(
        ScalarValue::TimestampMicrosecond(Some(1_767_225_599_999_999), None),
        None,
    );
    let high = Expr::Literal(
        ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_457), None),
        None,
    );
    let separate_and_filters = vec![
        datafusion::logical_expr::col("event_ts_ntz").gt_eq(low.clone()),
        datafusion::logical_expr::col("event_ts_ntz").lt(high.clone()),
    ];
    let whole_and_filter = datafusion::logical_expr::col("event_ts_ntz")
        .gt_eq(low.clone())
        .and(datafusion::logical_expr::col("event_ts_ntz").lt(high.clone()));
    let whole_or_filter = datafusion::logical_expr::col("event_ts_ntz")
        .eq(target.clone())
        .or(datafusion::logical_expr::col("event_ts_ntz").eq(high.clone()));
    let whole_not_filter = Expr::Not(Box::new(
        datafusion::logical_expr::col("event_ts_ntz").eq(target),
    ));
    let cases = [
        (
            "separate filters combine with and",
            separate_and_filters,
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
        (
            "whole and",
            vec![whole_and_filter],
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
        (
            "whole or",
            vec![whole_or_filter],
            vec!["part-00000.parquet", "part-00002.parquet"],
        ),
        (
            "whole not",
            vec![whole_not_filter],
            vec!["part-00001.parquet", "part-00002.parquet"],
        ),
    ];

    for (name, filters, expected_paths) in cases {
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let support = provider.supports_filters_pushdown(&filter_refs)?;
        assert_eq!(
            support,
            vec![TableProviderFilterPushDown::Exact; filters.len()],
            "{name}"
        );

        let plan = provider
            .scan(&state, Some(&vec![0]), &filters, None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;
        let scan_plan = scan.scan_plan();

        assert_eq!(scan_plan.projected_schema.field(0).name(), "id", "{name}");
        assert_eq!(scan_plan.pushed_filter_plan.exact_count, filters.len());
        assert_eq!(scan_plan.pushed_filter_plan.unsupported_count, 0, "{name}");
        assert_eq!(
            scan_plan.pushed_filter_plan.residual_filter_count, 0,
            "{name}"
        );
        assert_eq!(
            scan_plan.pushed_filter_plan.pushed_filter_count,
            filters.len()
        );
        assert!(scan_plan.kernel_partition_predicate.is_some(), "{name}");
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn timestamp_partition_null_checks_are_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "timestamp-partition-null-checks-boundary",
        TIMESTAMP_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_ts"]"#,
        &[
            r#""partitionValues":{"event_ts":"2026-01-01T00:00:00.123456Z"}"#,
            r#""partitionValues":{"event_ts":"2025-12-31T23:59:59.999999Z"}"#,
            r#""partitionValues":{"event_ts":null}"#,
            r#""partitionValues":{"event_ts":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let cases = [
        (
            "timestamp is null",
            datafusion::logical_expr::col("event_ts").is_null(),
            vec![
                "part-00002.parquet",
                "part-00003.parquet",
                "part-00004.parquet",
            ],
        ),
        (
            "timestamp is not null",
            datafusion::logical_expr::col("event_ts").is_not_null(),
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
    ];

    for (name, filter, expected_paths) in cases {
        let support = provider.supports_filters_pushdown(&[&filter])?;
        assert_eq!(support, vec![TableProviderFilterPushDown::Exact], "{name}");

        let plan = provider
            .scan(&state, Some(&vec![0]), &[filter], None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;
        let scan_plan = scan.scan_plan();

        assert_eq!(scan_plan.projected_schema.field(0).name(), "id", "{name}");
        assert_eq!(scan_plan.pushed_filter_plan.exact_count, 1, "{name}");
        assert_eq!(scan_plan.pushed_filter_plan.unsupported_count, 0, "{name}");
        assert_eq!(
            scan_plan.pushed_filter_plan.residual_filter_count, 0,
            "{name}"
        );
        assert_eq!(
            scan_plan.pushed_filter_plan.pushed_filter_count, 1,
            "{name}"
        );
        assert!(scan_plan.kernel_partition_predicate.is_some(), "{name}");
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn timestamp_ntz_partition_null_checks_are_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_protocol_and_adds(
        "timestamp-ntz-partition-null-checks-boundary",
        TIMESTAMP_NTZ_PROTOCOL_JSON,
        TIMESTAMP_NTZ_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_ts_ntz"]"#,
        &[
            r#""partitionValues":{"event_ts_ntz":"2026-01-01 00:00:00.123456"}"#,
            r#""partitionValues":{"event_ts_ntz":"2025-12-31 23:59:59.999999"}"#,
            r#""partitionValues":{"event_ts_ntz":null}"#,
            r#""partitionValues":{"event_ts_ntz":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let cases = [
        (
            "timestamp_ntz is null",
            datafusion::logical_expr::col("event_ts_ntz").is_null(),
            vec![
                "part-00002.parquet",
                "part-00003.parquet",
                "part-00004.parquet",
            ],
        ),
        (
            "timestamp_ntz is not null",
            datafusion::logical_expr::col("event_ts_ntz").is_not_null(),
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
    ];

    for (name, filter, expected_paths) in cases {
        let support = provider.supports_filters_pushdown(&[&filter])?;
        assert_eq!(support, vec![TableProviderFilterPushDown::Exact], "{name}");

        let plan = provider
            .scan(&state, Some(&vec![0]), &[filter], None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;
        let scan_plan = scan.scan_plan();

        assert_eq!(scan_plan.projected_schema.field(0).name(), "id", "{name}");
        assert_eq!(scan_plan.pushed_filter_plan.exact_count, 1, "{name}");
        assert_eq!(scan_plan.pushed_filter_plan.unsupported_count, 0, "{name}");
        assert_eq!(
            scan_plan.pushed_filter_plan.residual_filter_count, 0,
            "{name}"
        );
        assert_eq!(
            scan_plan.pushed_filter_plan.pushed_filter_count, 1,
            "{name}"
        );
        assert!(scan_plan.kernel_partition_predicate.is_some(), "{name}");
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn sql_timestamp_partition_comparisons_and_between_are_exact_kernel_pushdown()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema_and_adds(
        "sql-timestamp-partition-comparisons-between-exact",
        TIMESTAMP_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_ts"]"#,
        &[
            r#""partitionValues":{"event_ts":"2026-01-01T00:00:00.123456Z"}"#,
            r#""partitionValues":{"event_ts":"2026-01-01 00:00:00.123456"}"#,
            r#""partitionValues":{"event_ts":"2025-12-31T23:59:59.999999Z"}"#,
            r#""partitionValues":{"event_ts":"2026-01-01T00:00:00.123457Z"}"#,
            r#""partitionValues":{"event_ts":null}"#,
            r#""partitionValues":{"event_ts":""}"#,
            r#""partitionValues":{"event_ts":"2027-12-31T00:00:00.000000Z"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    let cases = [
        (
            "timestamp less than",
            "select id from orders where event_ts < timestamp '2026-01-01 00:00:00.123456'",
        ),
        (
            "timestamp less than or equal",
            "select id from orders where event_ts <= timestamp '2025-12-31 23:59:59.999999'",
        ),
        (
            "timestamp greater than",
            "select id from orders where event_ts > timestamp '2025-12-31 23:59:59.999999'",
        ),
        (
            "timestamp greater than or equal",
            "select id from orders where event_ts >= timestamp '2026-01-01 00:00:00.123456'",
        ),
        (
            "timestamp between inclusive",
            "select id from orders where event_ts between timestamp '2025-12-31 23:59:59.999999' and timestamp '2026-01-01 00:00:00.123456'",
        ),
        (
            "timestamp not between",
            "select id from orders where event_ts not between timestamp '2025-12-31 23:59:59.999999' and timestamp '2026-01-01 00:00:00.123456'",
        ),
    ];

    for (name, sql) in cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(
            !plan_display.contains("FilterExec"),
            "{name} should not keep residual filter:\n{plan_display}"
        );
        assert_eq!(scans.len(), 1, "{name}: {plan_display}");
        assert!(
            scans[0].scan_plan().pushed_filter_plan.exact_count > 0,
            "{name}: {plan_display}"
        );
        assert!(
            scans[0].scan_plan().pushed_filter_plan.pushed_filter_count > 0,
            "{name}: {plan_display}"
        );
        assert_eq!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .residual_filter_count,
            0,
            "{name}: {plan_display}"
        );
        assert!(
            scans[0].scan_plan().kernel_partition_predicate.is_some(),
            "{name}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn sql_timestamp_ntz_partition_comparisons_and_between_are_exact_kernel_pushdown()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema_protocol_and_adds(
        "sql-timestamp-ntz-partition-comparisons-between-exact",
        TIMESTAMP_NTZ_PROTOCOL_JSON,
        TIMESTAMP_NTZ_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_ts_ntz"]"#,
        &[
            r#""partitionValues":{"event_ts_ntz":"2026-01-01 00:00:00.123456"}"#,
            r#""partitionValues":{"event_ts_ntz":"2025-12-31 23:59:59.999999"}"#,
            r#""partitionValues":{"event_ts_ntz":"2026-01-01 00:00:00.123457"}"#,
            r#""partitionValues":{"event_ts_ntz":"1969-12-31 23:59:59.999999"}"#,
            r#""partitionValues":{"event_ts_ntz":null}"#,
            r#""partitionValues":{"event_ts_ntz":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    let cases = [
        (
            "timestamp_ntz less than",
            "select id from orders where event_ts_ntz < timestamp '2026-01-01 00:00:00.123456'",
        ),
        (
            "timestamp_ntz less than or equal",
            "select id from orders where event_ts_ntz <= timestamp '2025-12-31 23:59:59.999999'",
        ),
        (
            "timestamp_ntz greater than",
            "select id from orders where event_ts_ntz > timestamp '2025-12-31 23:59:59.999999'",
        ),
        (
            "timestamp_ntz greater than or equal",
            "select id from orders where event_ts_ntz >= timestamp '2026-01-01 00:00:00.123456'",
        ),
        (
            "timestamp_ntz between inclusive",
            "select id from orders where event_ts_ntz between timestamp '2025-12-31 23:59:59.999999' and timestamp '2026-01-01 00:00:00.123456'",
        ),
        (
            "timestamp_ntz not between",
            "select id from orders where event_ts_ntz not between timestamp '2025-12-31 23:59:59.999999' and timestamp '2026-01-01 00:00:00.123456'",
        ),
    ];

    for (name, sql) in cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(
            !plan_display.contains("FilterExec"),
            "{name} should not keep residual filter:\n{plan_display}"
        );
        assert_eq!(scans.len(), 1, "{name}: {plan_display}");
        assert!(
            scans[0].scan_plan().pushed_filter_plan.exact_count > 0,
            "{name}: {plan_display}"
        );
        assert!(
            scans[0].scan_plan().pushed_filter_plan.pushed_filter_count > 0,
            "{name}: {plan_display}"
        );
        assert_eq!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .residual_filter_count,
            0,
            "{name}: {plan_display}"
        );
        assert!(
            scans[0].scan_plan().kernel_partition_predicate.is_some(),
            "{name}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn sql_timestamp_partition_equality_and_membership_are_exact_kernel_pushdown()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema_and_adds(
        "sql-timestamp-partition-equality-membership-exact",
        TIMESTAMP_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_ts"]"#,
        &[
            r#""partitionValues":{"event_ts":"2026-01-01T00:00:00.123456Z"}"#,
            r#""partitionValues":{"event_ts":"2026-01-01 00:00:00.123456"}"#,
            r#""partitionValues":{"event_ts":"2025-12-31T23:59:59.999999Z"}"#,
            r#""partitionValues":{"event_ts":null}"#,
            r#""partitionValues":{"event_ts":""}"#,
            r#""partitionValues":{"event_ts":"2027-12-31T00:00:00.000000Z"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;
    let cases = [
        (
            "timestamp equality",
            "select id from orders where event_ts = timestamp '2026-01-01 00:00:00.123456'",
        ),
        (
            "timestamp utc suffix equality",
            "select id from orders where event_ts = timestamp '2026-01-01T00:00:00.123456Z'",
        ),
        (
            "timestamp offset equality",
            "select id from orders where event_ts = timestamp '2025-12-31T17:00:00.123456-07:00'",
        ),
        (
            "timestamp inequality",
            "select id from orders where event_ts != timestamp '2026-01-01 00:00:00.123456'",
        ),
        (
            "timestamp in list",
            "select id from orders where event_ts in (timestamp '2026-01-01 00:00:00.123456', timestamp '2025-12-31 23:59:59.999999')",
        ),
        (
            "timestamp not in list",
            "select id from orders where event_ts not in (timestamp '2026-01-01 00:00:00.123456')",
        ),
    ];

    for (name, sql) in cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(
            !plan_display.contains("FilterExec"),
            "{name} should not keep residual filter:\n{plan_display}"
        );
        assert_eq!(scans.len(), 1, "{name}: {plan_display}");
        assert!(
            scans[0].scan_plan().pushed_filter_plan.exact_count > 0,
            "{name}: {plan_display}"
        );
        assert!(
            scans[0].scan_plan().pushed_filter_plan.pushed_filter_count > 0,
            "{name}: {plan_display}"
        );
        assert_eq!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .residual_filter_count,
            0,
            "{name}: {plan_display}"
        );
        assert!(
            scans[0].scan_plan().kernel_partition_predicate.is_some(),
            "{name}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn sql_timestamp_ntz_partition_equality_and_membership_are_exact_kernel_pushdown()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema_protocol_and_adds(
        "sql-timestamp-ntz-partition-equality-membership-exact",
        TIMESTAMP_NTZ_PROTOCOL_JSON,
        TIMESTAMP_NTZ_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_ts_ntz"]"#,
        &[
            r#""partitionValues":{"event_ts_ntz":"2026-01-01 00:00:00.123456"}"#,
            r#""partitionValues":{"event_ts_ntz":"2025-12-31 23:59:59.999999"}"#,
            r#""partitionValues":{"event_ts_ntz":"2026-01-01 00:00:00.123457"}"#,
            r#""partitionValues":{"event_ts_ntz":null}"#,
            r#""partitionValues":{"event_ts_ntz":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;
    let cases = [
        (
            "timestamp_ntz equality",
            "select id from orders where event_ts_ntz = timestamp '2026-01-01 00:00:00.123456'",
        ),
        (
            "timestamp_ntz inequality",
            "select id from orders where event_ts_ntz != timestamp '2026-01-01 00:00:00.123456'",
        ),
        (
            "timestamp_ntz in list",
            "select id from orders where event_ts_ntz in (timestamp '2026-01-01 00:00:00.123456', timestamp '2025-12-31 23:59:59.999999')",
        ),
        (
            "timestamp_ntz not in list",
            "select id from orders where event_ts_ntz not in (timestamp '2026-01-01 00:00:00.123456')",
        ),
    ];

    for (name, sql) in cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(
            !plan_display.contains("FilterExec"),
            "{name} should not keep residual filter:\n{plan_display}"
        );
        assert_eq!(scans.len(), 1, "{name}: {plan_display}");
        assert!(
            scans[0].scan_plan().pushed_filter_plan.exact_count > 0,
            "{name}: {plan_display}"
        );
        assert!(
            scans[0].scan_plan().pushed_filter_plan.pushed_filter_count > 0,
            "{name}: {plan_display}"
        );
        assert_eq!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .residual_filter_count,
            0,
            "{name}: {plan_display}"
        );
        assert!(
            scans[0].scan_plan().kernel_partition_predicate.is_some(),
            "{name}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn sql_timestamp_partition_null_checks_are_exact_kernel_pushdown()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema_and_adds(
        "sql-timestamp-partition-null-checks-exact",
        TIMESTAMP_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_ts"]"#,
        &[
            r#""partitionValues":{"event_ts":"2026-01-01T00:00:00.123456Z"}"#,
            r#""partitionValues":{"event_ts":"2025-12-31T23:59:59.999999Z"}"#,
            r#""partitionValues":{"event_ts":null}"#,
            r#""partitionValues":{"event_ts":""}"#,
            r#""partitionValues":{"event_ts":"2027-12-31T00:00:00.000000Z"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;
    let cases = [
        (
            "timestamp is null",
            "select id from orders where event_ts is null",
        ),
        (
            "timestamp is not null",
            "select id from orders where event_ts is not null",
        ),
    ];

    for (name, sql) in cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(
            !plan_display.contains("FilterExec"),
            "{name} should not keep residual filter:\n{plan_display}"
        );
        assert_eq!(scans.len(), 1, "{name}: {plan_display}");
        assert_eq!(
            scans[0].scan_plan().pushed_filter_plan.exact_count,
            1,
            "{name}: {plan_display}"
        );
        assert_eq!(
            scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
            1,
            "{name}: {plan_display}"
        );
        assert_eq!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .residual_filter_count,
            0,
            "{name}: {plan_display}"
        );
        assert!(
            scans[0].scan_plan().kernel_partition_predicate.is_some(),
            "{name}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn sql_timestamp_ntz_partition_null_checks_are_exact_kernel_pushdown()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema_protocol_and_adds(
        "sql-timestamp-ntz-partition-null-checks-exact",
        TIMESTAMP_NTZ_PROTOCOL_JSON,
        TIMESTAMP_NTZ_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_ts_ntz"]"#,
        &[
            r#""partitionValues":{"event_ts_ntz":"2026-01-01 00:00:00.123456"}"#,
            r#""partitionValues":{"event_ts_ntz":"2025-12-31 23:59:59.999999"}"#,
            r#""partitionValues":{"event_ts_ntz":null}"#,
            r#""partitionValues":{"event_ts_ntz":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;
    let cases = [
        (
            "timestamp_ntz is null",
            "select id from orders where event_ts_ntz is null",
        ),
        (
            "timestamp_ntz is not null",
            "select id from orders where event_ts_ntz is not null",
        ),
    ];

    for (name, sql) in cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(
            !plan_display.contains("FilterExec"),
            "{name} should not keep residual filter:\n{plan_display}"
        );
        assert_eq!(scans.len(), 1, "{name}: {plan_display}");
        assert_eq!(
            scans[0].scan_plan().pushed_filter_plan.exact_count,
            1,
            "{name}: {plan_display}"
        );
        assert_eq!(
            scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
            1,
            "{name}: {plan_display}"
        );
        assert_eq!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .residual_filter_count,
            0,
            "{name}: {plan_display}"
        );
        assert!(
            scans[0].scan_plan().kernel_partition_predicate.is_some(),
            "{name}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn floating_partition_value_filters_remain_unsupported_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema(
        "floating-partition-unsupported-boundary",
        FLOATING_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["float_part","double_part"]"#,
        r#""partitionValues":{"float_part":"1.5","double_part":"-2.25"}"#,
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let float_value = Expr::Literal(ScalarValue::Float32(Some(1.5)), None);
    let negative_zero_float = Expr::Literal(ScalarValue::Float32(Some(-0.0)), None);
    let positive_zero_float = Expr::Literal(ScalarValue::Float32(Some(0.0)), None);
    let float_nan = Expr::Literal(ScalarValue::Float32(Some(f32::NAN)), None);
    let float_infinity = Expr::Literal(ScalarValue::Float32(Some(f32::INFINITY)), None);
    let float_null = Expr::Literal(ScalarValue::Float32(None), None);
    let float_low = Expr::Literal(ScalarValue::Float32(Some(0.5)), None);
    let double_value = Expr::Literal(ScalarValue::Float64(Some(-2.25)), None);
    let double_nan = Expr::Literal(ScalarValue::Float64(Some(f64::NAN)), None);
    let double_infinity = Expr::Literal(ScalarValue::Float64(Some(f64::INFINITY)), None);
    let double_low = Expr::Literal(ScalarValue::Float64(Some(-3.0)), None);
    let scalar_udf = create_udf(
        "floating_identity_for_pushdown_boundary",
        vec![DataType::Float32],
        DataType::Float32,
        Volatility::Immutable,
        Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Float32(Some(1.5))))),
    );
    let scalar_function =
        Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
            Arc::new(scalar_udf),
            vec![datafusion::logical_expr::col("float_part")],
        ));
    let filters = vec![
        (
            "float negative zero equality",
            datafusion::logical_expr::col("float_part").eq(negative_zero_float.clone()),
        ),
        (
            "float positive zero equality",
            datafusion::logical_expr::col("float_part").eq(positive_zero_float.clone()),
        ),
        (
            "float nan equality",
            datafusion::logical_expr::col("float_part").eq(float_nan.clone()),
        ),
        (
            "float infinity equality",
            datafusion::logical_expr::col("float_part").eq(float_infinity.clone()),
        ),
        (
            "float null equality",
            datafusion::logical_expr::col("float_part").eq(float_null.clone()),
        ),
        (
            "float width mismatch",
            datafusion::logical_expr::col("float_part")
                .eq(Expr::Literal(ScalarValue::Float64(Some(1.5)), None)),
        ),
        (
            "float zero in list",
            datafusion::logical_expr::col("float_part")
                .in_list(vec![float_value.clone(), positive_zero_float], false),
        ),
        (
            "float negative zero not in list",
            datafusion::logical_expr::col("float_part")
                .in_list(vec![float_value.clone(), negative_zero_float], true),
        ),
        (
            "float nan in list",
            datafusion::logical_expr::col("float_part")
                .in_list(vec![float_value.clone(), float_nan.clone()], false),
        ),
        (
            "float null in list",
            datafusion::logical_expr::col("float_part")
                .in_list(vec![float_value.clone(), float_null.clone()], false),
        ),
        (
            "float nan ordering",
            datafusion::logical_expr::col("float_part").lt(float_nan.clone()),
        ),
        (
            "float infinity ordering",
            datafusion::logical_expr::col("float_part").gt(float_infinity),
        ),
        (
            "float nan between",
            datafusion::logical_expr::col("float_part")
                .between(float_low.clone(), float_nan.clone()),
        ),
        (
            "float null between",
            datafusion::logical_expr::col("float_part").not_between(float_low, float_null.clone()),
        ),
        (
            "double nan equality",
            datafusion::logical_expr::col("double_part").eq(double_nan.clone()),
        ),
        (
            "double infinity equality",
            datafusion::logical_expr::col("double_part").eq(double_infinity.clone()),
        ),
        (
            "double nan in list",
            datafusion::logical_expr::col("double_part")
                .in_list(vec![double_value.clone(), double_nan.clone()], false),
        ),
        (
            "double nan ordering",
            datafusion::logical_expr::col("double_part").lt(double_nan),
        ),
        (
            "double infinity between",
            datafusion::logical_expr::col("double_part")
                .between(double_low.clone(), double_infinity.clone()),
        ),
        (
            "double wrong width between",
            datafusion::logical_expr::col("double_part")
                .not_between(double_low, float_value.clone()),
        ),
        (
            "cast operand",
            datafusion::logical_expr::col("float_part").eq(datafusion::logical_expr::cast(
                float_value.clone(),
                DataType::Float32,
            )),
        ),
        (
            "scalar function operand",
            datafusion::logical_expr::col("float_part").eq(scalar_function),
        ),
        (
            "mixed partition data equality",
            datafusion::logical_expr::col("float_part").eq(datafusion::logical_expr::col("id")),
        ),
        (
            "and composition",
            datafusion::logical_expr::col("float_part")
                .lt(float_null)
                .and(datafusion::logical_expr::col("double_part").eq(double_value.clone())),
        ),
        (
            "or composition",
            datafusion::logical_expr::col("float_part")
                .eq(float_nan)
                .or(datafusion::logical_expr::col("double_part").gt(double_infinity)),
        ),
    ];
    let filter_refs = filters.iter().map(|(_, filter)| filter).collect::<Vec<_>>();

    let support = provider.supports_filters_pushdown(&filter_refs)?;
    let plan = provider.plan_supports_filters_pushdown(&filter_refs);

    assert_eq!(
        support,
        vec![TableProviderFilterPushDown::Unsupported; filters.len()]
    );
    assert_eq!(plan.exact_count, 0);
    assert_eq!(plan.unsupported_count, filters.len());
    assert_eq!(plan.residual_filter_count, filters.len());

    for (name, filter) in filters {
        let result = provider
            .scan(&state, None, std::slice::from_ref(&filter), None)
            .await;

        assert!(
            matches!(result, Err(DataFusionError::External(error)) if error
                .to_string()
                .contains("pushed filters must be exact partition predicates")),
            "{name} should be rejected"
        );
    }

    Ok(())
}

#[tokio::test]
async fn floating_partition_equality_and_membership_are_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "floating-partition-equality-membership-boundary",
        FLOATING_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["float_part","double_part"]"#,
        &[
            r#""partitionValues":{"float_part":"1.5","double_part":"-2.25"}"#,
            r#""partitionValues":{"float_part":"-0.0","double_part":"0.0"}"#,
            r#""partitionValues":{"float_part":"0.0","double_part":"-0.0"}"#,
            r#""partitionValues":{"float_part":null,"double_part":null}"#,
            r#""partitionValues":{"float_part":"","double_part":"8.0"}"#,
            r#""partitionValues":{"float_part":"NaN","double_part":"Infinity"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let float_value = Expr::Literal(ScalarValue::Float32(Some(1.5)), None);
    let negative_float = Expr::Literal(ScalarValue::Float32(Some(-1.5)), None);
    let double_value = Expr::Literal(ScalarValue::Float64(Some(-2.25)), None);
    let double_other = Expr::Literal(ScalarValue::Float64(Some(4.0)), None);
    let cases = [
        (
            "float equality",
            datafusion::logical_expr::col("float_part").eq(float_value.clone()),
        ),
        (
            "float reversed equality",
            negative_float
                .clone()
                .eq(datafusion::logical_expr::col("float_part")),
        ),
        (
            "float inequality",
            datafusion::logical_expr::col("float_part").not_eq(float_value.clone()),
        ),
        (
            "float in list",
            datafusion::logical_expr::col("float_part")
                .in_list(vec![float_value.clone(), negative_float.clone()], false),
        ),
        (
            "float not in list",
            datafusion::logical_expr::col("float_part").in_list(vec![float_value.clone()], true),
        ),
        (
            "double equality",
            datafusion::logical_expr::col("double_part").eq(double_value.clone()),
        ),
        (
            "double not in list",
            datafusion::logical_expr::col("double_part").in_list(vec![double_other], true),
        ),
    ];

    for (name, filter) in cases {
        let support = provider.supports_filters_pushdown(&[&filter])?;
        assert_eq!(support, vec![TableProviderFilterPushDown::Exact], "{name}");

        provider
            .scan(&state, Some(&vec![0]), &[filter], None)
            .await?;
    }

    Ok(())
}

#[tokio::test]
async fn floating_partition_comparisons_and_between_are_rejected_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "floating-partition-comparisons-between-boundary",
        FLOATING_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["float_part","double_part"]"#,
        &[
            r#""partitionValues":{"float_part":"1.5","double_part":"-2.25"}"#,
            r#""partitionValues":{"float_part":"-0.0","double_part":"0.0"}"#,
            r#""partitionValues":{"float_part":"0.0","double_part":"1.0"}"#,
            r#""partitionValues":{"float_part":null,"double_part":null}"#,
            r#""partitionValues":{"float_part":"","double_part":"8.0"}"#,
            r#""partitionValues":{"float_part":"NaN","double_part":"Infinity"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let float_value = Expr::Literal(ScalarValue::Float32(Some(1.5)), None);
    let negative_zero_float = Expr::Literal(ScalarValue::Float32(Some(-0.0)), None);
    let positive_zero_float = Expr::Literal(ScalarValue::Float32(Some(0.0)), None);
    let double_value = Expr::Literal(ScalarValue::Float64(Some(-2.25)), None);
    let double_high = Expr::Literal(ScalarValue::Float64(Some(0.0)), None);
    let cases = [
        (
            "float less than",
            datafusion::logical_expr::col("float_part").lt(float_value.clone()),
        ),
        (
            "float less than or equal negative zero",
            datafusion::logical_expr::col("float_part").lt_eq(negative_zero_float.clone()),
        ),
        (
            "float greater than negative zero",
            datafusion::logical_expr::col("float_part").gt(negative_zero_float.clone()),
        ),
        (
            "reversed float greater than or equal",
            float_value
                .clone()
                .lt_eq(datafusion::logical_expr::col("float_part")),
        ),
        (
            "float between includes signed zero order",
            datafusion::logical_expr::col("float_part")
                .between(negative_zero_float.clone(), float_value.clone()),
        ),
        (
            "float not between",
            datafusion::logical_expr::col("float_part")
                .not_between(positive_zero_float, float_value),
        ),
        (
            "double between",
            datafusion::logical_expr::col("double_part")
                .between(double_value.clone(), double_high.clone()),
        ),
        (
            "double not between",
            datafusion::logical_expr::col("double_part").not_between(double_value, double_high),
        ),
    ];

    for (name, filter) in cases {
        let support = provider.supports_filters_pushdown(&[&filter])?;
        assert_eq!(
            support,
            vec![TableProviderFilterPushDown::Unsupported],
            "{name}"
        );

        let result = provider.scan(&state, Some(&vec![0]), &[filter], None).await;

        assert!(
            matches!(result, Err(DataFusionError::External(error)) if error
                .to_string()
                .contains("pushed filters must be exact partition predicates")),
            "{name} should be rejected"
        );
    }

    Ok(())
}

#[tokio::test]
async fn floating_partition_boolean_composition_and_projection_are_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "floating-partition-boolean-composition-boundary",
        FLOATING_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["float_part","double_part"]"#,
        &[
            r#""partitionValues":{"float_part":"1.5","double_part":"-2.25"}"#,
            r#""partitionValues":{"float_part":"-0.0","double_part":"0.0"}"#,
            r#""partitionValues":{"float_part":"0.0","double_part":"1.0"}"#,
            r#""partitionValues":{"float_part":"3.0","double_part":"4.0"}"#,
            r#""partitionValues":{"float_part":null,"double_part":null}"#,
            r#""partitionValues":{"float_part":"","double_part":"8.0"}"#,
            r#""partitionValues":{"float_part":"NaN","double_part":"Infinity"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let float_value = Expr::Literal(ScalarValue::Float32(Some(1.5)), None);
    let float_other = Expr::Literal(ScalarValue::Float32(Some(3.0)), None);
    let double_other = Expr::Literal(ScalarValue::Float64(Some(4.0)), None);
    let separate_and_filters = vec![
        datafusion::logical_expr::col("float_part").eq(float_value.clone()),
        datafusion::logical_expr::col("double_part").eq(double_other.clone()),
    ];
    let whole_and_filter = datafusion::logical_expr::col("float_part")
        .eq(float_value.clone())
        .and(datafusion::logical_expr::col("double_part").eq(double_other.clone()));
    let whole_or_filter = datafusion::logical_expr::col("float_part")
        .eq(float_value.clone())
        .or(datafusion::logical_expr::col("double_part").eq(double_other));
    let whole_not_filter = Expr::Not(Box::new(
        datafusion::logical_expr::col("float_part").eq(float_other),
    ));
    let cases = [
        ("separate filters combine with and", separate_and_filters),
        ("whole and", vec![whole_and_filter]),
        ("whole or", vec![whole_or_filter]),
        ("whole not", vec![whole_not_filter]),
    ];

    for (name, filters) in cases {
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let support = provider.supports_filters_pushdown(&filter_refs)?;
        assert_eq!(
            support,
            vec![TableProviderFilterPushDown::Exact; filters.len()],
            "{name}"
        );

        provider
            .scan(&state, Some(&vec![0]), &filters, None)
            .await?;
    }

    Ok(())
}

#[tokio::test]
async fn floating_partition_null_checks_are_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "floating-partition-null-checks-boundary",
        FLOATING_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["float_part","double_part"]"#,
        &[
            r#""partitionValues":{"float_part":"1.5","double_part":"-2.25"}"#,
            r#""partitionValues":{"float_part":null,"double_part":"-2.25"}"#,
            r#""partitionValues":{"float_part":"1.5","double_part":null}"#,
            r#""partitionValues":{"float_part":null,"double_part":null}"#,
            r#""partitionValues":{"float_part":"","double_part":"8.0"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let cases = [
        (
            "float is null",
            datafusion::logical_expr::col("float_part").is_null(),
        ),
        (
            "float is not null",
            datafusion::logical_expr::col("float_part").is_not_null(),
        ),
        (
            "double is null",
            datafusion::logical_expr::col("double_part").is_null(),
        ),
        (
            "double is not null",
            datafusion::logical_expr::col("double_part").is_not_null(),
        ),
        (
            "and composition",
            datafusion::logical_expr::col("float_part")
                .is_null()
                .and(datafusion::logical_expr::col("double_part").is_null()),
        ),
        (
            "or composition",
            datafusion::logical_expr::col("float_part")
                .is_null()
                .or(datafusion::logical_expr::col("double_part").is_null()),
        ),
        (
            "not composition",
            Expr::Not(Box::new(
                datafusion::logical_expr::col("float_part").is_null(),
            )),
        ),
    ];

    for (name, filter) in cases {
        let support = provider.supports_filters_pushdown(&[&filter])?;
        assert_eq!(support, vec![TableProviderFilterPushDown::Exact], "{name}");

        provider
            .scan(&state, Some(&vec![0]), &[filter], None)
            .await?;
    }

    Ok(())
}

#[tokio::test]
async fn floating_partition_exact_filters_prune_files_through_kernel_scan_plan()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "floating-partition-kernel-scan-pruning",
        FLOATING_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["float_part","double_part"]"#,
        &[
            r#""partitionValues":{"float_part":"1.5","double_part":"2.25"}"#,
            r#""partitionValues":{"float_part":"3.0","double_part":"4.0"}"#,
            r#""partitionValues":{"float_part":"-1.5","double_part":"-2.25"}"#,
            r#""partitionValues":{"float_part":null,"double_part":null}"#,
            r#""partitionValues":{"float_part":"","double_part":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let float_one = Expr::Literal(ScalarValue::Float32(Some(1.5)), None);
    let float_three = Expr::Literal(ScalarValue::Float32(Some(3.0)), None);
    let float_negative = Expr::Literal(ScalarValue::Float32(Some(-1.5)), None);
    let double_two = Expr::Literal(ScalarValue::Float64(Some(2.25)), None);
    let double_four = Expr::Literal(ScalarValue::Float64(Some(4.0)), None);
    type FloatingPruningCase = (
        &'static str,
        Vec<Expr>,
        Vec<&'static str>,
        Vec<&'static str>,
    );
    let cases: Vec<FloatingPruningCase> = vec![
        (
            "float equality",
            vec![datafusion::logical_expr::col("float_part").eq(float_one.clone())],
            vec!["part-00000.parquet"],
            vec!["float_part", "id"],
        ),
        (
            "float reversed equality",
            vec![
                float_negative
                    .clone()
                    .eq(datafusion::logical_expr::col("float_part")),
            ],
            vec!["part-00002.parquet"],
            vec!["float_part", "id"],
        ),
        (
            "float inequality",
            vec![datafusion::logical_expr::col("float_part").not_eq(float_one.clone())],
            vec!["part-00001.parquet", "part-00002.parquet"],
            vec!["float_part", "id"],
        ),
        (
            "float in list",
            vec![datafusion::logical_expr::col("float_part").in_list(
                vec![float_one.clone(), float_negative.clone(), float_one.clone()],
                false,
            )],
            vec!["part-00000.parquet", "part-00002.parquet"],
            vec!["float_part", "id"],
        ),
        (
            "float not in list",
            vec![
                datafusion::logical_expr::col("float_part")
                    .in_list(vec![float_three.clone()], true),
            ],
            vec!["part-00000.parquet", "part-00002.parquet"],
            vec!["float_part", "id"],
        ),
        (
            "double equality",
            vec![datafusion::logical_expr::col("double_part").eq(double_two.clone())],
            vec!["part-00000.parquet"],
            vec!["double_part", "id"],
        ),
        (
            "float is null",
            vec![datafusion::logical_expr::col("float_part").is_null()],
            vec![
                "part-00003.parquet",
                "part-00004.parquet",
                "part-00005.parquet",
            ],
            vec!["float_part", "id"],
        ),
        (
            "double is not null",
            vec![datafusion::logical_expr::col("double_part").is_not_null()],
            vec![
                "part-00000.parquet",
                "part-00001.parquet",
                "part-00002.parquet",
            ],
            vec!["double_part", "id"],
        ),
        (
            "separate filters combine with and",
            vec![
                datafusion::logical_expr::col("float_part").eq(float_one.clone()),
                datafusion::logical_expr::col("double_part").eq(double_two.clone()),
            ],
            vec!["part-00000.parquet"],
            vec!["double_part", "float_part", "id"],
        ),
        (
            "whole and",
            vec![
                datafusion::logical_expr::col("float_part")
                    .eq(float_one.clone())
                    .and(datafusion::logical_expr::col("double_part").eq(double_two)),
            ],
            vec!["part-00000.parquet"],
            vec!["double_part", "float_part", "id"],
        ),
        (
            "whole or",
            vec![
                datafusion::logical_expr::col("float_part")
                    .eq(float_negative)
                    .or(datafusion::logical_expr::col("double_part").eq(double_four)),
            ],
            vec!["part-00001.parquet", "part-00002.parquet"],
            vec!["double_part", "float_part", "id"],
        ),
        (
            "not",
            vec![Expr::Not(Box::new(
                datafusion::logical_expr::col("float_part").eq(float_three),
            ))],
            vec!["part-00000.parquet", "part-00002.parquet"],
            vec!["float_part", "id"],
        ),
    ];

    for (name, filters, expected_paths, expected_kernel_names) in cases {
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let support = provider.supports_filters_pushdown(&filter_refs)?;
        assert_eq!(
            support,
            vec![TableProviderFilterPushDown::Exact; filters.len()],
            "{name}"
        );

        let plan = provider
            .scan(&state, Some(&vec![0]), &filters, None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;
        let scan_plan = scan.scan_plan();
        let mut kernel_names = scan_plan
            .kernel_scan()
            .kernel_schema()
            .fields()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        kernel_names.sort_unstable();

        assert_eq!(scan_plan.projected_schema.field(0).name(), "id", "{name}");
        assert_eq!(kernel_names, expected_kernel_names, "{name}");
        assert_eq!(scan_plan.pushed_filter_plan.exact_count, filters.len());
        assert_eq!(scan_plan.pushed_filter_plan.unsupported_count, 0);
        assert_eq!(scan_plan.pushed_filter_plan.residual_filter_count, 0);
        assert_eq!(
            scan_plan.pushed_filter_plan.pushed_filter_count,
            filters.len()
        );
        assert!(scan_plan.kernel_partition_predicate.is_some(), "{name}");
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn floating_partition_mixed_and_filter_uses_kernel_pruning()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "floating-partition-mixed-and-kernel-pruning",
        FLOATING_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["float_part","double_part"]"#,
        &[
            r#""partitionValues":{"float_part":"1.5","double_part":"2.25"}"#,
            r#""partitionValues":{"float_part":"3.0","double_part":"4.0"}"#,
            r#""partitionValues":{"float_part":"-1.5","double_part":"-2.25"}"#,
            r#""partitionValues":{"float_part":null,"double_part":null}"#,
            r#""partitionValues":{"float_part":"","double_part":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let float_one = Expr::Literal(ScalarValue::Float32(Some(1.5)), None);
    let filter = datafusion::logical_expr::col("float_part")
        .eq(float_one)
        .and(datafusion::logical_expr::col("id").gt(datafusion::logical_expr::lit(10)));

    assert_eq!(
        provider.supports_filters_pushdown(&[&filter])?,
        vec![TableProviderFilterPushDown::Inexact]
    );

    let plan = provider.scan(&state, None, &[filter], None).await?;
    let scan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;
    let scan_plan = scan.scan_plan();

    assert_eq!(scan_plan.pushed_filter_plan.exact_count, 0);
    assert_eq!(scan_plan.pushed_filter_plan.inexact_count, 1);
    assert_eq!(scan_plan.pushed_filter_plan.unsupported_count, 0);
    assert_eq!(scan_plan.pushed_filter_plan.pushed_filter_count, 1);
    assert_eq!(scan_plan.pushed_filter_plan.residual_filter_count, 1);
    assert!(scan_plan.kernel_partition_predicate.is_some());
    assert_eq!(scan_file_paths(scan)?, vec!["part-00000.parquet"]);

    Ok(())
}

#[tokio::test]
async fn timestamp_partition_mixed_and_filter_uses_kernel_pruning()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "timestamp-partition-mixed-and-kernel-pruning",
        TIMESTAMP_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_ts"]"#,
        &[
            r#""partitionValues":{"event_ts":"2026-01-01T00:00:00.123456Z"}"#,
            r#""partitionValues":{"event_ts":"2025-12-31T23:59:59.999999Z"}"#,
            r#""partitionValues":{"event_ts":"2026-01-01T00:00:00.123457Z"}"#,
            r#""partitionValues":{"event_ts":null}"#,
            r#""partitionValues":{"event_ts":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let timestamp = Expr::Literal(
        ScalarValue::TimestampMicrosecond(
            Some(1_767_225_600_123_456),
            Some(Arc::<str>::from("UTC")),
        ),
        None,
    );
    let filter = datafusion::logical_expr::col("event_ts")
        .eq(timestamp)
        .and(datafusion::logical_expr::col("id").gt(datafusion::logical_expr::lit(10)));

    assert_eq!(
        provider.supports_filters_pushdown(&[&filter])?,
        vec![TableProviderFilterPushDown::Inexact]
    );

    let plan = provider.scan(&state, None, &[filter], None).await?;
    let scan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;
    let scan_plan = scan.scan_plan();

    assert_eq!(scan_plan.pushed_filter_plan.exact_count, 0);
    assert_eq!(scan_plan.pushed_filter_plan.inexact_count, 1);
    assert_eq!(scan_plan.pushed_filter_plan.unsupported_count, 0);
    assert_eq!(scan_plan.pushed_filter_plan.pushed_filter_count, 1);
    assert_eq!(scan_plan.pushed_filter_plan.residual_filter_count, 1);
    assert!(scan_plan.kernel_partition_predicate.is_some());
    assert_eq!(scan_file_paths(scan)?, vec!["part-00000.parquet"]);

    Ok(())
}

#[tokio::test]
async fn timestamp_ntz_partition_mixed_and_filter_uses_kernel_pruning()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_protocol_and_adds(
        "timestamp-ntz-partition-mixed-and-kernel-pruning",
        TIMESTAMP_NTZ_PROTOCOL_JSON,
        TIMESTAMP_NTZ_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_ts_ntz"]"#,
        &[
            r#""partitionValues":{"event_ts_ntz":"2026-01-01 00:00:00.123456"}"#,
            r#""partitionValues":{"event_ts_ntz":"2025-12-31 23:59:59.999999"}"#,
            r#""partitionValues":{"event_ts_ntz":"2026-01-01 00:00:00.123457"}"#,
            r#""partitionValues":{"event_ts_ntz":null}"#,
            r#""partitionValues":{"event_ts_ntz":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let timestamp = Expr::Literal(
        ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_456), None),
        None,
    );
    let filter = datafusion::logical_expr::col("event_ts_ntz")
        .eq(timestamp)
        .and(datafusion::logical_expr::col("id").gt(datafusion::logical_expr::lit(10)));

    assert_eq!(
        provider.supports_filters_pushdown(&[&filter])?,
        vec![TableProviderFilterPushDown::Inexact]
    );

    let plan = provider.scan(&state, None, &[filter], None).await?;
    let scan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;
    let scan_plan = scan.scan_plan();

    assert_eq!(scan_plan.pushed_filter_plan.exact_count, 0);
    assert_eq!(scan_plan.pushed_filter_plan.inexact_count, 1);
    assert_eq!(scan_plan.pushed_filter_plan.unsupported_count, 0);
    assert_eq!(scan_plan.pushed_filter_plan.pushed_filter_count, 1);
    assert_eq!(scan_plan.pushed_filter_plan.residual_filter_count, 1);
    assert!(scan_plan.kernel_partition_predicate.is_some());
    assert_eq!(scan_file_paths(scan)?, vec!["part-00000.parquet"]);

    Ok(())
}

#[tokio::test]
async fn binary_partition_mixed_and_filter_uses_kernel_pruning()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "binary-partition-mixed-and-kernel-pruning",
        BINARY_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["payload"]"#,
        &[
            r#""partitionValues":{"payload":"hello"}"#,
            r#""partitionValues":{"payload":"world"}"#,
            r#""partitionValues":{"payload":"/=%"}"#,
            r#""partitionValues":{"payload":null}"#,
            r#""partitionValues":{"payload":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let hello = Expr::Literal(ScalarValue::Binary(Some(b"hello".to_vec())), None);
    let filter = datafusion::logical_expr::col("payload")
        .eq(hello)
        .and(datafusion::logical_expr::col("id").gt(datafusion::logical_expr::lit(10)));

    assert_eq!(
        provider.supports_filters_pushdown(&[&filter])?,
        vec![TableProviderFilterPushDown::Inexact]
    );

    let plan = provider.scan(&state, None, &[filter], None).await?;
    let scan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;
    let scan_plan = scan.scan_plan();

    assert_eq!(scan_plan.pushed_filter_plan.exact_count, 0);
    assert_eq!(scan_plan.pushed_filter_plan.inexact_count, 1);
    assert_eq!(scan_plan.pushed_filter_plan.unsupported_count, 0);
    assert_eq!(scan_plan.pushed_filter_plan.pushed_filter_count, 1);
    assert_eq!(scan_plan.pushed_filter_plan.residual_filter_count, 1);
    assert!(scan_plan.kernel_partition_predicate.is_some());
    assert_eq!(scan_file_paths(scan)?, vec!["part-00000.parquet"]);

    Ok(())
}

#[tokio::test]
async fn sql_floating_partition_null_checks_are_exact_kernel_pushdown()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema_and_adds(
        "sql-floating-partition-null-checks",
        FLOATING_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["float_part","double_part"]"#,
        &[
            r#""partitionValues":{"float_part":"1.5","double_part":"-2.25"}"#,
            r#""partitionValues":{"float_part":null,"double_part":"-2.25"}"#,
            r#""partitionValues":{"float_part":"1.5","double_part":null}"#,
            r#""partitionValues":{"float_part":null,"double_part":null}"#,
            r#""partitionValues":{"float_part":"","double_part":"8.0"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;
    let sql_cases = [
        (
            "float is null",
            "select id from orders where float_part is null",
        ),
        (
            "double is not null",
            "select id from orders where double_part is not null",
        ),
        (
            "null check or",
            "select id from orders where float_part is null or double_part is null",
        ),
        (
            "not null check",
            "select id from orders where not(float_part is null)",
        ),
    ];

    for (name, sql) in sql_cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(
            !plan_display.contains("FilterExec"),
            "{name} should not keep residual filter:\n{plan_display}"
        );
        assert_eq!(scans.len(), 1, "{name}: {plan_display}");
        assert_eq!(
            scans[0].scan_plan().pushed_filter_plan.exact_count,
            1,
            "{name}: {plan_display}"
        );
        assert_eq!(
            scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
            1,
            "{name}: {plan_display}"
        );
        assert_eq!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .residual_filter_count,
            0,
            "{name}: {plan_display}"
        );
        assert!(
            scans[0].scan_plan().kernel_partition_predicate.is_some(),
            "{name}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn sql_floating_partition_equality_and_membership_are_exact_kernel_pushdown()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema_and_adds(
        "sql-floating-partition-equality-membership",
        FLOATING_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["float_part","double_part"]"#,
        &[
            r#""partitionValues":{"float_part":"1.5","double_part":"-2.25"}"#,
            r#""partitionValues":{"float_part":"-0.0","double_part":"0.0"}"#,
            r#""partitionValues":{"float_part":"0.0","double_part":"-0.0"}"#,
            r#""partitionValues":{"float_part":null,"double_part":null}"#,
            r#""partitionValues":{"float_part":"","double_part":"8.0"}"#,
            r#""partitionValues":{"float_part":"NaN","double_part":"Infinity"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;
    let sql_cases = [
        (
            "float equality",
            "select id from orders where float_part = cast(1.5 as float)",
        ),
        (
            "float in list",
            "select id from orders where float_part in (cast(1.5 as float), cast(-1.5 as float))",
        ),
        (
            "float inequality",
            "select id from orders where float_part != cast(1.5 as float)",
        ),
        (
            "double equality",
            "select id from orders where double_part = -2.25",
        ),
        (
            "double not in",
            "select id from orders where double_part not in (-2.25)",
        ),
    ];

    for (name, sql) in sql_cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(
            !plan_display.contains("FilterExec"),
            "{name} should not keep residual filter:\n{plan_display}"
        );
        assert_eq!(scans.len(), 1, "{name}: {plan_display}");
        assert!(
            scans[0].scan_plan().pushed_filter_plan.exact_count > 0,
            "{name}: {plan_display}"
        );
        assert!(
            scans[0].scan_plan().pushed_filter_plan.pushed_filter_count > 0,
            "{name}: {plan_display}"
        );
        assert_eq!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .residual_filter_count,
            0,
            "{name}: {plan_display}"
        );
        assert!(
            scans[0].scan_plan().kernel_partition_predicate.is_some(),
            "{name}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn sql_floating_partition_comparisons_and_between_keep_residual_filter()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema_and_adds(
        "sql-floating-partition-comparisons-between-residuals",
        FLOATING_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["float_part","double_part"]"#,
        &[
            r#""partitionValues":{"float_part":"1.5","double_part":"-2.25"}"#,
            r#""partitionValues":{"float_part":"-0.0","double_part":"0.0"}"#,
            r#""partitionValues":{"float_part":"0.0","double_part":"1.0"}"#,
            r#""partitionValues":{"float_part":null,"double_part":null}"#,
            r#""partitionValues":{"float_part":"","double_part":"8.0"}"#,
            r#""partitionValues":{"float_part":"NaN","double_part":"Infinity"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;
    let sql_cases = [
        (
            "float less than",
            "select id from orders where float_part < cast(1.5 as float)",
        ),
        (
            "float between",
            "select id from orders where float_part between cast(-0.0 as float) and cast(1.5 as float)",
        ),
        (
            "float not between",
            "select id from orders where float_part not between cast(0.0 as float) and cast(1.5 as float)",
        ),
        (
            "double between",
            "select id from orders where double_part between -2.25 and 0.0",
        ),
        (
            "double not between",
            "select id from orders where double_part not between -2.25 and 0.0",
        ),
    ];

    for (name, sql) in sql_cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(
            plan_display.contains("FilterExec"),
            "{name} unexpectedly became exact:\n{plan_display}"
        );
        assert_eq!(scans.len(), 1, "{name}: {plan_display}");
        assert_eq!(
            scans[0].scan_plan().pushed_filter_plan.exact_count,
            0,
            "{name}: {plan_display}"
        );
        assert_eq!(
            scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
            0,
            "{name}: {plan_display}"
        );
        assert_eq!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .residual_filter_count,
            0,
            "{name}: {plan_display}"
        );
        assert!(
            scans[0].scan_plan().kernel_partition_predicate.is_none(),
            "{name}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn sql_floating_partition_unsafe_literal_filters_keep_residual_filter()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema_and_adds(
        "sql-floating-partition-unsafe-literal-residuals",
        FLOATING_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["float_part","double_part"]"#,
        &[
            r#""partitionValues":{"float_part":"1.5","double_part":"-2.25"}"#,
            r#""partitionValues":{"float_part":"-0.0","double_part":"0.0"}"#,
            r#""partitionValues":{"float_part":"0.0","double_part":"1.0"}"#,
            r#""partitionValues":{"float_part":null,"double_part":null}"#,
            r#""partitionValues":{"float_part":"","double_part":"8.0"}"#,
            r#""partitionValues":{"float_part":"NaN","double_part":"Infinity"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;
    let cases = [
        (
            "nan equality",
            "select id from orders where float_part = cast('NaN' as float)",
        ),
        (
            "infinity ordering",
            "select id from orders where double_part > cast('Infinity' as double)",
        ),
        (
            "null in list",
            "select id from orders where float_part in (cast(1.5 as float), cast(null as float))",
        ),
        (
            "wrong width equality",
            "select id from orders where float_part = cast(1.5 as double)",
        ),
    ];

    for (name, sql) in cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(
            plan_display.contains("FilterExec"),
            "{name} unexpectedly became exact:\n{plan_display}"
        );
        assert_eq!(scans.len(), 1, "{name}: {plan_display}");
        assert_eq!(
            scans[0].scan_plan().pushed_filter_plan.exact_count,
            0,
            "{name}: {plan_display}"
        );
        assert_eq!(
            scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
            0,
            "{name}: {plan_display}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn decimal_partition_comparisons_are_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "decimal-partition-comparisons-boundary",
        DECIMAL_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["amount"]"#,
        &[
            r#""partitionValues":{"amount":"123.45"}"#,
            r#""partitionValues":{"amount":"0.00"}"#,
            r#""partitionValues":{"amount":"-1.23"}"#,
            r#""partitionValues":{"amount":null}"#,
            r#""partitionValues":{"amount":""}"#,
            r#""partitionValues":{"amount":"999.99"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let amount = Expr::Literal(ScalarValue::Decimal128(Some(12_345), 10, 2), None);
    let zero = Expr::Literal(ScalarValue::Decimal128(Some(0), 10, 2), None);
    let negative = Expr::Literal(ScalarValue::Decimal128(Some(-123), 10, 2), None);
    let cases = [
        (
            "less than",
            datafusion::logical_expr::col("amount").lt(amount.clone()),
        ),
        (
            "less than or equal",
            datafusion::logical_expr::col("amount").lt_eq(negative),
        ),
        (
            "greater than",
            datafusion::logical_expr::col("amount").gt(zero.clone()),
        ),
        (
            "reversed greater than or equal",
            amount.lt_eq(datafusion::logical_expr::col("amount")),
        ),
    ];

    for (name, filter) in cases {
        let support = provider.supports_filters_pushdown(&[&filter])?;
        assert_eq!(support, vec![TableProviderFilterPushDown::Exact], "{name}");

        provider
            .scan(&state, Some(&vec![0]), &[filter], None)
            .await?;
    }

    Ok(())
}

#[tokio::test]
async fn decimal_partition_between_filters_are_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "decimal-partition-between-boundary",
        DECIMAL_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["amount"]"#,
        &[
            r#""partitionValues":{"amount":"123.45"}"#,
            r#""partitionValues":{"amount":"0.00"}"#,
            r#""partitionValues":{"amount":"-1.23"}"#,
            r#""partitionValues":{"amount":null}"#,
            r#""partitionValues":{"amount":""}"#,
            r#""partitionValues":{"amount":"999.99"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let amount = Expr::Literal(ScalarValue::Decimal128(Some(12_345), 10, 2), None);
    let zero = Expr::Literal(ScalarValue::Decimal128(Some(0), 10, 2), None);
    let cases = [
        (
            "between inclusive",
            datafusion::logical_expr::col("amount").between(zero.clone(), amount.clone()),
        ),
        (
            "not between",
            datafusion::logical_expr::col("amount").not_between(zero, amount),
        ),
    ];

    for (name, filter) in cases {
        let support = provider.supports_filters_pushdown(&[&filter])?;
        assert_eq!(support, vec![TableProviderFilterPushDown::Exact], "{name}");

        provider
            .scan(&state, Some(&vec![0]), &[filter], None)
            .await?;
    }

    Ok(())
}

#[tokio::test]
async fn decimal_partition_boolean_composition_and_projection_are_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "decimal-partition-boolean-composition-boundary",
        DECIMAL_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["amount"]"#,
        &[
            r#""partitionValues":{"amount":"123.45"}"#,
            r#""partitionValues":{"amount":"0.00"}"#,
            r#""partitionValues":{"amount":"-1.23"}"#,
            r#""partitionValues":{"amount":null}"#,
            r#""partitionValues":{"amount":""}"#,
            r#""partitionValues":{"amount":"999.99"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let amount = Expr::Literal(ScalarValue::Decimal128(Some(12_345), 10, 2), None);
    let zero = Expr::Literal(ScalarValue::Decimal128(Some(0), 10, 2), None);
    let negative = Expr::Literal(ScalarValue::Decimal128(Some(-123), 10, 2), None);
    let separate_and_filters = vec![
        datafusion::logical_expr::col("amount").gt_eq(zero.clone()),
        datafusion::logical_expr::col("amount").lt(amount.clone()),
    ];
    let whole_and_filter = datafusion::logical_expr::col("amount")
        .gt_eq(zero.clone())
        .and(datafusion::logical_expr::col("amount").lt(amount.clone()));
    let whole_or_filter = datafusion::logical_expr::col("amount")
        .eq(amount.clone())
        .or(datafusion::logical_expr::col("amount").eq(negative));
    let whole_not_filter = Expr::Not(Box::new(datafusion::logical_expr::col("amount").eq(amount)));
    let cases = [
        ("separate filters combine with and", separate_and_filters),
        ("whole and", vec![whole_and_filter]),
        ("whole or", vec![whole_or_filter]),
        ("whole not", vec![whole_not_filter]),
    ];

    for (name, filters) in cases {
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let support = provider.supports_filters_pushdown(&filter_refs)?;
        assert_eq!(
            support,
            vec![TableProviderFilterPushDown::Exact; filters.len()],
            "{name}"
        );

        provider
            .scan(&state, Some(&vec![0]), &filters, None)
            .await?;
    }

    Ok(())
}

#[tokio::test]
async fn decimal_partition_high_precision_values_are_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "decimal-partition-high-precision-boundary",
        HIGH_PRECISION_DECIMAL_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["amount"]"#,
        &[
            r#""partitionValues":{"amount":"1.230000000000000000"}"#,
            r#""partitionValues":{"amount":"12345678901234567890.123456789012345678"}"#,
            r#""partitionValues":{"amount":"-1.230000000000000000"}"#,
            r#""partitionValues":{"amount":null}"#,
            r#""partitionValues":{"amount":"999.990000000000000000"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let small_amount = Expr::Literal(
        ScalarValue::Decimal128(Some(1_230_000_000_000_000_000), 38, 18),
        None,
    );
    let large_amount = Expr::Literal(
        ScalarValue::Decimal128(
            Some(12_345_678_901_234_567_890_123_456_789_012_345_678),
            38,
            18,
        ),
        None,
    );
    let zero = Expr::Literal(ScalarValue::Decimal128(Some(0), 38, 18), None);
    let cases = [
        (
            "high precision equality",
            datafusion::logical_expr::col("amount").eq(large_amount.clone()),
        ),
        (
            "high precision ordering",
            datafusion::logical_expr::col("amount").gt(zero.clone()),
        ),
        (
            "high precision between",
            datafusion::logical_expr::col("amount").between(zero, large_amount),
        ),
        (
            "high precision not in",
            datafusion::logical_expr::col("amount").in_list(vec![small_amount], true),
        ),
    ];

    for (name, filter) in cases {
        let support = provider.supports_filters_pushdown(&[&filter])?;
        assert_eq!(support, vec![TableProviderFilterPushDown::Exact], "{name}");

        provider
            .scan(&state, Some(&vec![0]), &[filter], None)
            .await?;
    }

    Ok(())
}

#[tokio::test]
async fn decimal_partition_exponent_metadata_is_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "decimal-partition-exponent-metadata-boundary",
        HIGH_PRECISION_DECIMAL_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["amount"]"#,
        &[
            r#""partitionValues":{"amount":"0E-18"}"#,
            r#""partitionValues":{"amount":"1.23E-16"}"#,
            r#""partitionValues":{"amount":"999.990000000000000000"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let tiny_amount = Expr::Literal(ScalarValue::Decimal128(Some(123), 38, 18), None);
    let filter = datafusion::logical_expr::col("amount").eq(tiny_amount);

    let support = provider.supports_filters_pushdown(&[&filter])?;
    assert_eq!(support, vec![TableProviderFilterPushDown::Exact]);

    provider
        .scan(&state, Some(&vec![0]), &[filter], None)
        .await?;

    Ok(())
}

#[tokio::test]
async fn decimal_partition_equality_and_membership_are_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "decimal-partition-equality-membership-boundary",
        DECIMAL_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["amount"]"#,
        &[
            r#""partitionValues":{"amount":"123.45"}"#,
            r#""partitionValues":{"amount":"0.00"}"#,
            r#""partitionValues":{"amount":"-1.23"}"#,
            r#""partitionValues":{"amount":null}"#,
            r#""partitionValues":{"amount":""}"#,
            r#""partitionValues":{"amount":"999.99"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let amount = Expr::Literal(ScalarValue::Decimal128(Some(12_345), 10, 2), None);
    let zero = Expr::Literal(ScalarValue::Decimal128(Some(0), 10, 2), None);
    let negative = Expr::Literal(ScalarValue::Decimal128(Some(-123), 10, 2), None);
    let cases = [
        (
            "equality",
            datafusion::logical_expr::col("amount").eq(amount.clone()),
        ),
        (
            "reversed equality",
            negative.eq(datafusion::logical_expr::col("amount")),
        ),
        (
            "inequality",
            datafusion::logical_expr::col("amount").not_eq(amount.clone()),
        ),
        (
            "in list",
            datafusion::logical_expr::col("amount")
                .in_list(vec![amount.clone(), zero.clone(), amount.clone()], false),
        ),
        (
            "not in list",
            datafusion::logical_expr::col("amount").in_list(vec![amount.clone()], true),
        ),
    ];

    for (name, filter) in cases {
        let support = provider.supports_filters_pushdown(&[&filter])?;
        assert_eq!(support, vec![TableProviderFilterPushDown::Exact], "{name}");

        provider
            .scan(&state, Some(&vec![0]), &[filter], None)
            .await?;
    }

    Ok(())
}

#[tokio::test]
async fn decimal_partition_null_checks_are_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "decimal-partition-null-checks-boundary",
        DECIMAL_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["amount"]"#,
        &[
            r#""partitionValues":{"amount":"123.45"}"#,
            r#""partitionValues":{"amount":"0.00"}"#,
            r#""partitionValues":{"amount":"-1.23"}"#,
            r#""partitionValues":{"amount":null}"#,
            r#""partitionValues":{"amount":""}"#,
            r#""partitionValues":{"amount":"999.99"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let cases = [
        ("is null", datafusion::logical_expr::col("amount").is_null()),
        (
            "is not null",
            datafusion::logical_expr::col("amount").is_not_null(),
        ),
    ];

    for (name, filter) in cases {
        let support = provider.supports_filters_pushdown(&[&filter])?;
        assert_eq!(support, vec![TableProviderFilterPushDown::Exact], "{name}");

        provider
            .scan(&state, Some(&vec![0]), &[filter], None)
            .await?;
    }

    Ok(())
}

#[tokio::test]
async fn decimal_partition_exact_filters_prune_files_through_kernel_scan_plan()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "decimal-partition-kernel-scan-pruning",
        DECIMAL_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["amount"]"#,
        &[
            r#""partitionValues":{"amount":"123.45"}"#,
            r#""partitionValues":{"amount":"0.00"}"#,
            r#""partitionValues":{"amount":"-1.23"}"#,
            r#""partitionValues":{"amount":"2.00"}"#,
            r#""partitionValues":{"amount":"10.00"}"#,
            r#""partitionValues":{"amount":null}"#,
            r#""partitionValues":{"amount":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let amount_123 = Expr::Literal(ScalarValue::Decimal128(Some(12_345), 10, 2), None);
    let zero = Expr::Literal(ScalarValue::Decimal128(Some(0), 10, 2), None);
    let negative = Expr::Literal(ScalarValue::Decimal128(Some(-123), 10, 2), None);
    let two = Expr::Literal(ScalarValue::Decimal128(Some(200), 10, 2), None);
    let ten = Expr::Literal(ScalarValue::Decimal128(Some(1_000), 10, 2), None);
    let cases: Vec<(&str, Vec<Expr>, Vec<&str>)> = vec![
        (
            "equality",
            vec![datafusion::logical_expr::col("amount").eq(amount_123.clone())],
            vec!["part-00000.parquet"],
        ),
        (
            "reversed equality",
            vec![negative.clone().eq(datafusion::logical_expr::col("amount"))],
            vec!["part-00002.parquet"],
        ),
        (
            "inequality",
            vec![datafusion::logical_expr::col("amount").not_eq(amount_123.clone())],
            vec![
                "part-00001.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
                "part-00004.parquet",
            ],
        ),
        (
            "in list",
            vec![datafusion::logical_expr::col("amount").in_list(
                vec![amount_123.clone(), zero.clone(), amount_123.clone()],
                false,
            )],
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
        (
            "not in list",
            vec![datafusion::logical_expr::col("amount").in_list(vec![amount_123.clone()], true)],
            vec![
                "part-00001.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
                "part-00004.parquet",
            ],
        ),
        (
            "less than",
            vec![datafusion::logical_expr::col("amount").lt(ten.clone())],
            vec![
                "part-00001.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
            ],
        ),
        (
            "less than or equal",
            vec![datafusion::logical_expr::col("amount").lt_eq(negative.clone())],
            vec!["part-00002.parquet"],
        ),
        (
            "greater than",
            vec![datafusion::logical_expr::col("amount").gt(two.clone())],
            vec!["part-00000.parquet", "part-00004.parquet"],
        ),
        (
            "greater than or equal",
            vec![datafusion::logical_expr::col("amount").gt_eq(amount_123.clone())],
            vec!["part-00000.parquet"],
        ),
        (
            "between",
            vec![datafusion::logical_expr::col("amount").between(negative.clone(), two.clone())],
            vec![
                "part-00001.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
            ],
        ),
        (
            "not between",
            vec![
                datafusion::logical_expr::col("amount").not_between(negative.clone(), two.clone()),
            ],
            vec!["part-00000.parquet", "part-00004.parquet"],
        ),
        (
            "is null",
            vec![datafusion::logical_expr::col("amount").is_null()],
            vec![
                "part-00005.parquet",
                "part-00006.parquet",
                "part-00007.parquet",
            ],
        ),
        (
            "is not null",
            vec![datafusion::logical_expr::col("amount").is_not_null()],
            vec![
                "part-00000.parquet",
                "part-00001.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
                "part-00004.parquet",
            ],
        ),
        (
            "separate filters combine with and",
            vec![
                datafusion::logical_expr::col("amount").gt_eq(zero.clone()),
                datafusion::logical_expr::col("amount").lt(amount_123.clone()),
            ],
            vec![
                "part-00001.parquet",
                "part-00003.parquet",
                "part-00004.parquet",
            ],
        ),
        (
            "whole and",
            vec![
                datafusion::logical_expr::col("amount")
                    .gt_eq(zero.clone())
                    .and(datafusion::logical_expr::col("amount").lt(amount_123.clone())),
            ],
            vec![
                "part-00001.parquet",
                "part-00003.parquet",
                "part-00004.parquet",
            ],
        ),
        (
            "whole or",
            vec![
                datafusion::logical_expr::col("amount")
                    .eq(amount_123.clone())
                    .or(datafusion::logical_expr::col("amount").eq(negative)),
            ],
            vec!["part-00000.parquet", "part-00002.parquet"],
        ),
        (
            "not",
            vec![Expr::Not(Box::new(
                datafusion::logical_expr::col("amount").eq(two),
            ))],
            vec![
                "part-00000.parquet",
                "part-00001.parquet",
                "part-00002.parquet",
                "part-00004.parquet",
            ],
        ),
    ];

    for (name, filters, expected_paths) in cases {
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let support = provider.supports_filters_pushdown(&filter_refs)?;
        assert_eq!(
            support,
            vec![TableProviderFilterPushDown::Exact; filters.len()],
            "{name}"
        );

        let plan = provider
            .scan(&state, Some(&vec![0]), &filters, None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;
        let scan_plan = scan.scan_plan();
        let kernel_names = scan_plan
            .kernel_scan()
            .kernel_schema()
            .fields()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();

        assert_eq!(scan_plan.projected_schema.field(0).name(), "id", "{name}");
        assert_eq!(kernel_names, vec!["id", "amount"], "{name}");
        assert_eq!(scan_plan.pushed_filter_plan.exact_count, filters.len());
        assert_eq!(scan_plan.pushed_filter_plan.unsupported_count, 0);
        assert_eq!(scan_plan.pushed_filter_plan.residual_filter_count, 0);
        assert_eq!(
            scan_plan.pushed_filter_plan.pushed_filter_count,
            filters.len()
        );
        assert!(scan_plan.kernel_partition_predicate.is_some(), "{name}");
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn decimal_partition_mixed_and_filter_uses_kernel_pruning()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "decimal-partition-mixed-and-kernel-pruning",
        DECIMAL_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["amount"]"#,
        &[
            r#""partitionValues":{"amount":"123.45"}"#,
            r#""partitionValues":{"amount":"0.00"}"#,
            r#""partitionValues":{"amount":"-1.23"}"#,
            r#""partitionValues":{"amount":null}"#,
            r#""partitionValues":{"amount":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let zero = Expr::Literal(ScalarValue::Decimal128(Some(0), 10, 2), None);
    let filter = datafusion::logical_expr::col("amount")
        .gt_eq(zero)
        .and(datafusion::logical_expr::col("id").gt(datafusion::logical_expr::lit(10)));

    assert_eq!(
        provider.supports_filters_pushdown(&[&filter])?,
        vec![TableProviderFilterPushDown::Inexact]
    );

    let plan = provider.scan(&state, None, &[filter], None).await?;
    let scan = plan
        .as_any()
        .downcast_ref::<DeltaScanPlanningExec>()
        .ok_or("expected DeltaScanPlanningExec")?;
    let scan_plan = scan.scan_plan();

    assert_eq!(scan_plan.pushed_filter_plan.exact_count, 0);
    assert_eq!(scan_plan.pushed_filter_plan.inexact_count, 1);
    assert_eq!(scan_plan.pushed_filter_plan.unsupported_count, 0);
    assert_eq!(scan_plan.pushed_filter_plan.pushed_filter_count, 1);
    assert_eq!(scan_plan.pushed_filter_plan.residual_filter_count, 1);
    assert!(scan_plan.kernel_partition_predicate.is_some());
    assert_eq!(
        scan_file_paths(scan)?,
        vec!["part-00000.parquet", "part-00001.parquet"]
    );

    Ok(())
}

#[tokio::test]
async fn date_partition_comparisons_are_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "date-partition-comparisons-boundary",
        DATE_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_date"]"#,
        &[
            r#""partitionValues":{"event_date":"2026-01-01"}"#,
            r#""partitionValues":{"event_date":"2024-02-29"}"#,
            r#""partitionValues":{"event_date":"1969-12-31"}"#,
            r#""partitionValues":{"event_date":null}"#,
            r#""partitionValues":{"event_date":""}"#,
            r#""partitionValues":{"event_date":"2027-12-31"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let new_year_2026 = Expr::Literal(ScalarValue::Date32(Some(20_454)), None);
    let leap_day_2024 = Expr::Literal(ScalarValue::Date32(Some(19_782)), None);
    let pre_epoch_day = Expr::Literal(ScalarValue::Date32(Some(-1)), None);
    let cases = [
        (
            "less than",
            datafusion::logical_expr::col("event_date").lt(new_year_2026.clone()),
        ),
        (
            "less than or equal",
            datafusion::logical_expr::col("event_date").lt_eq(pre_epoch_day),
        ),
        (
            "greater than",
            datafusion::logical_expr::col("event_date").gt(leap_day_2024),
        ),
        (
            "greater than or equal",
            datafusion::logical_expr::col("event_date").gt_eq(new_year_2026.clone()),
        ),
        (
            "reversed less than",
            new_year_2026.gt(datafusion::logical_expr::col("event_date")),
        ),
    ];

    for (name, filter) in cases {
        let support = provider.supports_filters_pushdown(&[&filter])?;
        assert_eq!(support, vec![TableProviderFilterPushDown::Exact], "{name}");

        provider
            .scan(&state, Some(&vec![0]), &[filter], None)
            .await?;
    }

    Ok(())
}

#[tokio::test]
async fn date_partition_between_filters_are_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "date-partition-between-boundary",
        DATE_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_date"]"#,
        &[
            r#""partitionValues":{"event_date":"2026-01-01"}"#,
            r#""partitionValues":{"event_date":"2024-02-29"}"#,
            r#""partitionValues":{"event_date":"1969-12-31"}"#,
            r#""partitionValues":{"event_date":null}"#,
            r#""partitionValues":{"event_date":""}"#,
            r#""partitionValues":{"event_date":"2027-12-31"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let new_year_2026 = Expr::Literal(ScalarValue::Date32(Some(20_454)), None);
    let leap_day_2024 = Expr::Literal(ScalarValue::Date32(Some(19_782)), None);
    let cases = [
        (
            "between inclusive",
            datafusion::logical_expr::col("event_date")
                .between(leap_day_2024.clone(), new_year_2026.clone()),
        ),
        (
            "not between",
            datafusion::logical_expr::col("event_date")
                .not_between(leap_day_2024.clone(), new_year_2026.clone()),
        ),
        (
            "contradictory between",
            datafusion::logical_expr::col("event_date")
                .between(new_year_2026.clone(), leap_day_2024.clone()),
        ),
        (
            "contradictory not between",
            datafusion::logical_expr::col("event_date")
                .not_between(new_year_2026.clone(), leap_day_2024.clone()),
        ),
    ];

    for (name, filter) in cases {
        let support = provider.supports_filters_pushdown(&[&filter])?;
        assert_eq!(support, vec![TableProviderFilterPushDown::Exact], "{name}");

        provider
            .scan(&state, Some(&vec![0]), &[filter], None)
            .await?;
    }

    Ok(())
}

#[tokio::test]
async fn date_partition_boolean_composition_and_projection_are_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "date-partition-boolean-composition-boundary",
        DATE_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_date"]"#,
        &[
            r#""partitionValues":{"event_date":"2026-01-01"}"#,
            r#""partitionValues":{"event_date":"2024-02-29"}"#,
            r#""partitionValues":{"event_date":"1969-12-31"}"#,
            r#""partitionValues":{"event_date":null}"#,
            r#""partitionValues":{"event_date":""}"#,
            r#""partitionValues":{"event_date":"2027-12-31"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let new_year_2026 = Expr::Literal(ScalarValue::Date32(Some(20_454)), None);
    let next_day = Expr::Literal(ScalarValue::Date32(Some(20_455)), None);
    let leap_day_2024 = Expr::Literal(ScalarValue::Date32(Some(19_782)), None);
    let pre_epoch_day = Expr::Literal(ScalarValue::Date32(Some(-1)), None);
    let separate_and_filters = vec![
        datafusion::logical_expr::col("event_date").gt_eq(leap_day_2024.clone()),
        datafusion::logical_expr::col("event_date").lt(next_day.clone()),
    ];
    let whole_and_filter = datafusion::logical_expr::col("event_date")
        .gt_eq(leap_day_2024.clone())
        .and(datafusion::logical_expr::col("event_date").lt(next_day));
    let whole_or_filter = datafusion::logical_expr::col("event_date")
        .eq(new_year_2026.clone())
        .or(datafusion::logical_expr::col("event_date").eq(pre_epoch_day));
    let whole_not_filter = Expr::Not(Box::new(
        datafusion::logical_expr::col("event_date").eq(new_year_2026),
    ));
    let cases = [
        ("separate filters combine with and", separate_and_filters),
        ("whole and", vec![whole_and_filter]),
        ("whole or", vec![whole_or_filter]),
        ("whole not", vec![whole_not_filter]),
    ];

    for (name, filters) in cases {
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let support = provider.supports_filters_pushdown(&filter_refs)?;
        assert_eq!(
            support,
            vec![TableProviderFilterPushDown::Exact; filters.len()],
            "{name}"
        );

        provider
            .scan(&state, Some(&vec![0]), &filters, None)
            .await?;
    }

    Ok(())
}

#[tokio::test]
async fn date_partition_exact_filters_prune_files_through_kernel_scan_plan()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "date-partition-kernel-scan-pruning",
        DATE_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_date"]"#,
        &[
            r#""partitionValues":{"event_date":"2026-01-01"}"#,
            r#""partitionValues":{"event_date":"2024-02-29"}"#,
            r#""partitionValues":{"event_date":"1969-12-31"}"#,
            r#""partitionValues":{"event_date":"2026-01-02"}"#,
            r#""partitionValues":{"event_date":null}"#,
            r#""partitionValues":{"event_date":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let new_year_2026 = Expr::Literal(ScalarValue::Date32(Some(20_454)), None);
    let next_day = Expr::Literal(ScalarValue::Date32(Some(20_455)), None);
    let leap_day_2024 = Expr::Literal(ScalarValue::Date32(Some(19_782)), None);
    let pre_epoch_day = Expr::Literal(ScalarValue::Date32(Some(-1)), None);
    let cases: Vec<(&str, Vec<Expr>, Vec<&str>)> = vec![
        (
            "equality",
            vec![datafusion::logical_expr::col("event_date").eq(new_year_2026.clone())],
            vec!["part-00000.parquet"],
        ),
        (
            "reversed equality pre epoch",
            vec![
                pre_epoch_day
                    .clone()
                    .eq(datafusion::logical_expr::col("event_date")),
            ],
            vec!["part-00002.parquet"],
        ),
        (
            "inequality",
            vec![datafusion::logical_expr::col("event_date").not_eq(new_year_2026.clone())],
            vec![
                "part-00001.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
            ],
        ),
        (
            "in list",
            vec![datafusion::logical_expr::col("event_date").in_list(
                vec![
                    new_year_2026.clone(),
                    leap_day_2024.clone(),
                    new_year_2026.clone(),
                ],
                false,
            )],
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
        (
            "not in list",
            vec![
                datafusion::logical_expr::col("event_date")
                    .in_list(vec![new_year_2026.clone()], true),
            ],
            vec![
                "part-00001.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
            ],
        ),
        (
            "less than",
            vec![datafusion::logical_expr::col("event_date").lt(next_day.clone())],
            vec![
                "part-00000.parquet",
                "part-00001.parquet",
                "part-00002.parquet",
            ],
        ),
        (
            "less than or equal",
            vec![datafusion::logical_expr::col("event_date").lt_eq(pre_epoch_day.clone())],
            vec!["part-00002.parquet"],
        ),
        (
            "greater than",
            vec![datafusion::logical_expr::col("event_date").gt(leap_day_2024.clone())],
            vec!["part-00000.parquet", "part-00003.parquet"],
        ),
        (
            "greater than or equal",
            vec![datafusion::logical_expr::col("event_date").gt_eq(new_year_2026.clone())],
            vec!["part-00000.parquet", "part-00003.parquet"],
        ),
        (
            "between",
            vec![
                datafusion::logical_expr::col("event_date")
                    .between(leap_day_2024.clone(), new_year_2026.clone()),
            ],
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
        (
            "not between",
            vec![
                datafusion::logical_expr::col("event_date")
                    .not_between(leap_day_2024.clone(), new_year_2026.clone()),
            ],
            vec!["part-00002.parquet", "part-00003.parquet"],
        ),
        (
            "is null",
            vec![datafusion::logical_expr::col("event_date").is_null()],
            vec![
                "part-00004.parquet",
                "part-00005.parquet",
                "part-00006.parquet",
            ],
        ),
        (
            "is not null",
            vec![datafusion::logical_expr::col("event_date").is_not_null()],
            vec![
                "part-00000.parquet",
                "part-00001.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
            ],
        ),
        (
            "separate filters combine with and",
            vec![
                datafusion::logical_expr::col("event_date").gt_eq(leap_day_2024.clone()),
                datafusion::logical_expr::col("event_date").lt(next_day.clone()),
            ],
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
        (
            "whole and",
            vec![
                datafusion::logical_expr::col("event_date")
                    .gt_eq(leap_day_2024.clone())
                    .and(datafusion::logical_expr::col("event_date").lt(next_day)),
            ],
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
        (
            "whole or",
            vec![
                datafusion::logical_expr::col("event_date")
                    .eq(new_year_2026.clone())
                    .or(datafusion::logical_expr::col("event_date").eq(pre_epoch_day)),
            ],
            vec!["part-00000.parquet", "part-00002.parquet"],
        ),
        (
            "not",
            vec![Expr::Not(Box::new(
                datafusion::logical_expr::col("event_date").eq(leap_day_2024),
            ))],
            vec![
                "part-00000.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
            ],
        ),
    ];

    for (name, filters, expected_paths) in cases {
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let support = provider.supports_filters_pushdown(&filter_refs)?;
        assert_eq!(
            support,
            vec![TableProviderFilterPushDown::Exact; filters.len()],
            "{name}"
        );

        let plan = provider
            .scan(&state, Some(&vec![0]), &filters, None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;
        let scan_plan = scan.scan_plan();
        let kernel_names = scan_plan
            .kernel_scan()
            .kernel_schema()
            .fields()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();

        assert_eq!(scan_plan.projected_schema.field(0).name(), "id", "{name}");
        assert_eq!(kernel_names, vec!["id", "event_date"], "{name}");
        assert_eq!(scan_plan.pushed_filter_plan.exact_count, filters.len());
        assert_eq!(scan_plan.pushed_filter_plan.unsupported_count, 0);
        assert_eq!(scan_plan.pushed_filter_plan.residual_filter_count, 0);
        assert_eq!(
            scan_plan.pushed_filter_plan.pushed_filter_count,
            filters.len()
        );
        assert!(scan_plan.kernel_partition_predicate.is_some(), "{name}");
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn boolean_partition_exact_filters_prune_files_through_kernel_scan_plan()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "boolean-partition-kernel-scan-pruning",
        BOOLEAN_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["is_current"]"#,
        &[
            r#""partitionValues":{"is_current":"true"}"#,
            r#""partitionValues":{"is_current":"false"}"#,
            r#""partitionValues":{"is_current":null}"#,
            r#""partitionValues":{"is_current":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let cases: Vec<(&str, Vec<Expr>, Vec<&str>)> = vec![
        (
            "equality true",
            vec![
                datafusion::logical_expr::col("is_current").eq(datafusion::logical_expr::lit(true)),
            ],
            vec!["part-00000.parquet"],
        ),
        (
            "reversed equality false",
            vec![
                datafusion::logical_expr::lit(false)
                    .eq(datafusion::logical_expr::col("is_current")),
            ],
            vec!["part-00001.parquet"],
        ),
        (
            "inequality",
            vec![
                datafusion::logical_expr::col("is_current")
                    .not_eq(datafusion::logical_expr::lit(true)),
            ],
            vec!["part-00001.parquet"],
        ),
        (
            "in list",
            vec![datafusion::logical_expr::col("is_current").in_list(
                vec![
                    datafusion::logical_expr::lit(true),
                    datafusion::logical_expr::lit(false),
                    datafusion::logical_expr::lit(true),
                ],
                false,
            )],
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
        (
            "not in list",
            vec![
                datafusion::logical_expr::col("is_current")
                    .in_list(vec![datafusion::logical_expr::lit(true)], true),
            ],
            vec!["part-00001.parquet"],
        ),
        (
            "is null",
            vec![datafusion::logical_expr::col("is_current").is_null()],
            vec![
                "part-00002.parquet",
                "part-00003.parquet",
                "part-00004.parquet",
            ],
        ),
        (
            "is not null",
            vec![datafusion::logical_expr::col("is_current").is_not_null()],
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
        (
            "separate filters combine with and",
            vec![
                datafusion::logical_expr::col("is_current").is_not_null(),
                datafusion::logical_expr::col("is_current").eq(datafusion::logical_expr::lit(true)),
            ],
            vec!["part-00000.parquet"],
        ),
        (
            "whole and",
            vec![
                datafusion::logical_expr::col("is_current")
                    .eq(datafusion::logical_expr::lit(true))
                    .and(datafusion::logical_expr::col("is_current").is_not_null()),
            ],
            vec!["part-00000.parquet"],
        ),
        (
            "whole or",
            vec![
                datafusion::logical_expr::col("is_current")
                    .eq(datafusion::logical_expr::lit(true))
                    .or(datafusion::logical_expr::col("is_current").is_null()),
            ],
            vec![
                "part-00000.parquet",
                "part-00002.parquet",
                "part-00003.parquet",
                "part-00004.parquet",
            ],
        ),
        (
            "not",
            vec![Expr::Not(Box::new(
                datafusion::logical_expr::col("is_current").eq(datafusion::logical_expr::lit(true)),
            ))],
            vec!["part-00001.parquet"],
        ),
        (
            "shorthand",
            vec![datafusion::logical_expr::col("is_current")],
            vec!["part-00000.parquet"],
        ),
        (
            "not shorthand",
            vec![Expr::Not(Box::new(datafusion::logical_expr::col(
                "is_current",
            )))],
            vec!["part-00001.parquet"],
        ),
    ];

    for (name, filters, expected_paths) in cases {
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let support = provider.supports_filters_pushdown(&filter_refs)?;
        assert_eq!(
            support,
            vec![TableProviderFilterPushDown::Exact; filters.len()],
            "{name}"
        );

        let plan = provider
            .scan(&state, Some(&vec![0]), &filters, None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;
        let scan_plan = scan.scan_plan();
        let kernel_names = scan_plan
            .kernel_scan()
            .kernel_schema()
            .fields()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();

        assert_eq!(scan_plan.projected_schema.field(0).name(), "id", "{name}");
        assert_eq!(kernel_names, vec!["id", "is_current"], "{name}");
        assert_eq!(scan_plan.pushed_filter_plan.exact_count, filters.len());
        assert_eq!(scan_plan.pushed_filter_plan.unsupported_count, 0);
        assert_eq!(scan_plan.pushed_filter_plan.residual_filter_count, 0);
        assert_eq!(
            scan_plan.pushed_filter_plan.pushed_filter_count,
            filters.len()
        );
        assert!(scan_plan.kernel_partition_predicate.is_some(), "{name}");
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn boolean_partition_unsafe_literal_shapes_are_rejected_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema(
        "boolean-partition-unsafe-literal-shapes",
        BOOLEAN_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["is_current"]"#,
        r#""partitionValues":{"is_current":"true"}"#,
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let filters = vec![
        (
            "string literal equality",
            datafusion::logical_expr::col("is_current").eq(datafusion::logical_expr::lit("true")),
        ),
        (
            "null equality",
            datafusion::logical_expr::col("is_current")
                .eq(Expr::Literal(ScalarValue::Boolean(None), None)),
        ),
        (
            "null in",
            datafusion::logical_expr::col("is_current").in_list(
                vec![
                    datafusion::logical_expr::lit(true),
                    Expr::Literal(ScalarValue::Boolean(None), None),
                ],
                false,
            ),
        ),
        (
            "empty in",
            datafusion::logical_expr::col("is_current").in_list(Vec::<Expr>::new(), false),
        ),
        (
            "empty not in",
            datafusion::logical_expr::col("is_current").in_list(Vec::<Expr>::new(), true),
        ),
        (
            "mixed string boolean in",
            datafusion::logical_expr::col("is_current").in_list(
                vec![
                    datafusion::logical_expr::lit(true),
                    datafusion::logical_expr::lit("false"),
                ],
                false,
            ),
        ),
        (
            "non literal in",
            datafusion::logical_expr::col("is_current")
                .in_list(vec![datafusion::logical_expr::col("id")], false),
        ),
    ];
    let filter_refs = filters.iter().map(|(_, filter)| filter).collect::<Vec<_>>();

    let support = provider.supports_filters_pushdown(&filter_refs)?;
    assert_eq!(
        support,
        vec![TableProviderFilterPushDown::Unsupported; filters.len()]
    );

    for (name, filter) in filters {
        let result = provider
            .scan(&state, None, std::slice::from_ref(&filter), None)
            .await;

        assert!(
            matches!(result, Err(DataFusionError::External(error)) if error
                .to_string()
                .contains("pushed filters must be exact partition predicates")),
            "{name} should be rejected"
        );
    }

    Ok(())
}

#[tokio::test]
async fn boolean_partition_unsafe_ordering_shapes_are_rejected_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema(
        "boolean-partition-unsafe-ordering-shapes",
        BOOLEAN_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["is_current"]"#,
        r#""partitionValues":{"is_current":"true"}"#,
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let scalar_udf = create_udf(
        "boolean_identity_for_pushdown_boundary",
        vec![DataType::Boolean],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))),
    );
    let scalar_function =
        Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
            Arc::new(scalar_udf),
            vec![datafusion::logical_expr::col("is_current")],
        ));
    let filters = vec![
        (
            "less than",
            datafusion::logical_expr::col("is_current").lt(datafusion::logical_expr::lit(true)),
        ),
        (
            "greater than or equal",
            datafusion::logical_expr::col("is_current").gt_eq(datafusion::logical_expr::lit(false)),
        ),
        (
            "between",
            datafusion::logical_expr::col("is_current").between(
                datafusion::logical_expr::lit(false),
                datafusion::logical_expr::lit(true),
            ),
        ),
        (
            "not between",
            datafusion::logical_expr::col("is_current").not_between(
                datafusion::logical_expr::lit(false),
                datafusion::logical_expr::lit(true),
            ),
        ),
        (
            "cast operand",
            datafusion::logical_expr::col("is_current").eq(datafusion::logical_expr::cast(
                datafusion::logical_expr::lit(true),
                DataType::Boolean,
            )),
        ),
        (
            "scalar function operand",
            datafusion::logical_expr::col("is_current").eq(scalar_function),
        ),
    ];
    let filter_refs = filters.iter().map(|(_, filter)| filter).collect::<Vec<_>>();

    let support = provider.supports_filters_pushdown(&filter_refs)?;
    assert_eq!(
        support,
        vec![TableProviderFilterPushDown::Unsupported; filters.len()]
    );

    for (name, filter) in filters {
        let result = provider
            .scan(&state, None, std::slice::from_ref(&filter), None)
            .await;

        assert!(
            matches!(result, Err(DataFusionError::External(error)) if error
                .to_string()
                .contains("pushed filters must be exact partition predicates")),
            "{name} should be rejected"
        );
    }

    Ok(())
}

#[test]
fn integer_partition_uncoerced_literals_remain_unsupported()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema(
        "integer-partition-unsupported-boundary",
        INTEGER_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["byte_part","short_part","int_part","long_part"]"#,
        r#""partitionValues":{"byte_part":"7","short_part":"1024","int_part":"0","long_part":"9223372036854775807"}"#,
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let filters = [
        datafusion::logical_expr::col("int_part").eq(datafusion::logical_expr::lit("0")),
        datafusion::logical_expr::col("int_part").between(
            datafusion::logical_expr::lit("-10"),
            datafusion::logical_expr::lit("10"),
        ),
    ];
    let filter_refs = filters.iter().collect::<Vec<_>>();

    let support = provider.supports_filters_pushdown(&filter_refs)?;
    let plan = provider.plan_supports_filters_pushdown(&filter_refs);

    assert_eq!(
        support,
        vec![TableProviderFilterPushDown::Unsupported; filters.len()]
    );
    assert_eq!(plan.exact_count, 0);
    assert_eq!(plan.unsupported_count, filters.len());
    assert_eq!(plan.residual_filter_count, filters.len());

    Ok(())
}

#[tokio::test]
async fn integer_partition_unsafe_direct_shapes_are_rejected_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema(
        "integer-partition-unsafe-direct-shapes",
        INTEGER_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["long_part"]"#,
        r#""partitionValues":{"long_part":"7"}"#,
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let scalar_udf = create_udf(
        "integer_identity_for_pushdown_boundary",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(7))))),
    );
    let scalar_function =
        Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
            Arc::new(scalar_udf),
            vec![datafusion::logical_expr::col("long_part")],
        ));
    let filters = vec![
        (
            "null equality",
            datafusion::logical_expr::col("long_part")
                .eq(Expr::Literal(ScalarValue::Int64(None), None)),
        ),
        (
            "empty in",
            datafusion::logical_expr::col("long_part").in_list(Vec::<Expr>::new(), false),
        ),
        (
            "null in",
            datafusion::logical_expr::col("long_part").in_list(
                vec![
                    datafusion::logical_expr::lit(7_i64),
                    Expr::Literal(ScalarValue::Int64(None), None),
                ],
                false,
            ),
        ),
        (
            "mixed string numeric in",
            datafusion::logical_expr::col("long_part").in_list(
                vec![
                    datafusion::logical_expr::lit(7_i64),
                    datafusion::logical_expr::lit("7"),
                ],
                false,
            ),
        ),
        (
            "non literal in",
            datafusion::logical_expr::col("long_part")
                .in_list(vec![datafusion::logical_expr::col("id")], false),
        ),
        (
            "null between",
            datafusion::logical_expr::col("long_part").between(
                Expr::Literal(ScalarValue::Int64(None), None),
                datafusion::logical_expr::lit(10_i64),
            ),
        ),
        (
            "non literal between",
            datafusion::logical_expr::col("long_part").between(
                datafusion::logical_expr::col("id"),
                datafusion::logical_expr::lit(10_i64),
            ),
        ),
        (
            "cast operand",
            datafusion::logical_expr::col("long_part").eq(datafusion::logical_expr::cast(
                datafusion::logical_expr::lit(7_i64),
                DataType::Int64,
            )),
        ),
        (
            "scalar function operand",
            datafusion::logical_expr::col("long_part").eq(scalar_function),
        ),
    ];
    let filter_refs = filters.iter().map(|(_, filter)| filter).collect::<Vec<_>>();

    let support = provider.supports_filters_pushdown(&filter_refs)?;
    assert_eq!(
        support,
        vec![TableProviderFilterPushDown::Unsupported; filters.len()]
    );

    for (name, filter) in filters {
        let result = provider
            .scan(&state, None, std::slice::from_ref(&filter), None)
            .await;

        assert!(
            matches!(result, Err(DataFusionError::External(error)) if error
                .to_string()
                .contains("pushed filters must be exact partition predicates")),
            "{name} should be rejected"
        );
    }

    Ok(())
}

#[tokio::test]
async fn integer_partition_exact_filters_prune_files_through_kernel_scan_plan()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "integer-partition-kernel-scan-pruning",
        INTEGER_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["long_part"]"#,
        &[
            r#""partitionValues":{"long_part":"7"}"#,
            r#""partitionValues":{"long_part":"-1"}"#,
            r#""partitionValues":{"long_part":"20"}"#,
            r#""partitionValues":{"long_part":null}"#,
            r#""partitionValues":{"long_part":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let cases: Vec<(&str, Vec<Expr>, Vec<&str>)> = vec![
        (
            "equality",
            vec![
                datafusion::logical_expr::col("long_part").eq(datafusion::logical_expr::lit(7_i64)),
            ],
            vec!["part-00000.parquet"],
        ),
        (
            "inequality",
            vec![
                datafusion::logical_expr::col("long_part")
                    .not_eq(datafusion::logical_expr::lit(7_i64)),
            ],
            vec!["part-00001.parquet", "part-00002.parquet"],
        ),
        (
            "in list",
            vec![datafusion::logical_expr::col("long_part").in_list(
                vec![
                    datafusion::logical_expr::lit(7_i64),
                    datafusion::logical_expr::lit(-1_i64),
                ],
                false,
            )],
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
        (
            "not in list",
            vec![
                datafusion::logical_expr::col("long_part")
                    .in_list(vec![datafusion::logical_expr::lit(7_i64)], true),
            ],
            vec!["part-00001.parquet", "part-00002.parquet"],
        ),
        (
            "less than",
            vec![
                datafusion::logical_expr::col("long_part").lt(datafusion::logical_expr::lit(7_i64)),
            ],
            vec!["part-00001.parquet"],
        ),
        (
            "less than or equal",
            vec![
                datafusion::logical_expr::col("long_part")
                    .lt_eq(datafusion::logical_expr::lit(-1_i64)),
            ],
            vec!["part-00001.parquet"],
        ),
        (
            "greater than",
            vec![
                datafusion::logical_expr::col("long_part")
                    .gt(datafusion::logical_expr::lit(-1_i64)),
            ],
            vec!["part-00000.parquet", "part-00002.parquet"],
        ),
        (
            "greater than or equal",
            vec![
                datafusion::logical_expr::col("long_part")
                    .gt_eq(datafusion::logical_expr::lit(7_i64)),
            ],
            vec!["part-00000.parquet", "part-00002.parquet"],
        ),
        (
            "between",
            vec![datafusion::logical_expr::col("long_part").between(
                datafusion::logical_expr::lit(-1_i64),
                datafusion::logical_expr::lit(7_i64),
            )],
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
        (
            "not between",
            vec![datafusion::logical_expr::col("long_part").not_between(
                datafusion::logical_expr::lit(-1_i64),
                datafusion::logical_expr::lit(7_i64),
            )],
            vec!["part-00002.parquet"],
        ),
        (
            "is null",
            vec![datafusion::logical_expr::col("long_part").is_null()],
            vec![
                "part-00003.parquet",
                "part-00004.parquet",
                "part-00005.parquet",
            ],
        ),
        (
            "is not null",
            vec![datafusion::logical_expr::col("long_part").is_not_null()],
            vec![
                "part-00000.parquet",
                "part-00001.parquet",
                "part-00002.parquet",
            ],
        ),
        (
            "separate filters combine with and",
            vec![
                datafusion::logical_expr::col("long_part")
                    .gt_eq(datafusion::logical_expr::lit(-1_i64)),
                datafusion::logical_expr::col("long_part")
                    .lt(datafusion::logical_expr::lit(20_i64)),
            ],
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
        (
            "whole and",
            vec![
                datafusion::logical_expr::col("long_part")
                    .gt_eq(datafusion::logical_expr::lit(-1_i64))
                    .and(
                        datafusion::logical_expr::col("long_part")
                            .lt(datafusion::logical_expr::lit(20_i64)),
                    ),
            ],
            vec!["part-00000.parquet", "part-00001.parquet"],
        ),
        (
            "whole or",
            vec![
                datafusion::logical_expr::col("long_part")
                    .eq(datafusion::logical_expr::lit(7_i64))
                    .or(datafusion::logical_expr::col("long_part")
                        .eq(datafusion::logical_expr::lit(20_i64))),
            ],
            vec!["part-00000.parquet", "part-00002.parquet"],
        ),
        (
            "not",
            vec![Expr::Not(Box::new(
                datafusion::logical_expr::col("long_part").eq(datafusion::logical_expr::lit(7_i64)),
            ))],
            vec!["part-00001.parquet", "part-00002.parquet"],
        ),
    ];

    for (name, filters, expected_paths) in cases {
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let support = provider.supports_filters_pushdown(&filter_refs)?;
        assert_eq!(
            support,
            vec![TableProviderFilterPushDown::Exact; filters.len()],
            "{name}"
        );

        let plan = provider
            .scan(&state, Some(&vec![0]), &filters, None)
            .await?;
        let scan = plan
            .as_any()
            .downcast_ref::<DeltaScanPlanningExec>()
            .ok_or("expected DeltaScanPlanningExec")?;
        let scan_plan = scan.scan_plan();
        let kernel_names = scan_plan
            .kernel_scan()
            .kernel_schema()
            .fields()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();

        assert_eq!(scan_plan.projected_schema.field(0).name(), "id", "{name}");
        assert_eq!(kernel_names, vec!["id", "long_part"], "{name}");
        assert_eq!(scan_plan.pushed_filter_plan.exact_count, filters.len());
        assert_eq!(scan_plan.pushed_filter_plan.unsupported_count, 0);
        assert_eq!(scan_plan.pushed_filter_plan.residual_filter_count, 0);
        assert_eq!(
            scan_plan.pushed_filter_plan.pushed_filter_count,
            filters.len()
        );
        assert!(scan_plan.kernel_partition_predicate.is_some(), "{name}");
        assert_eq!(scan_file_paths(scan)?, expected_paths, "{name}");
    }

    Ok(())
}

#[tokio::test]
async fn integer_partition_between_filters_are_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "integer-partition-between-boundary",
        INTEGER_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["long_part"]"#,
        &[
            r#""partitionValues":{"long_part":"7"}"#,
            r#""partitionValues":{"long_part":"-1"}"#,
            r#""partitionValues":{"long_part":"20"}"#,
            r#""partitionValues":{"long_part":null}"#,
            r#""partitionValues":{"long_part":""}"#,
            r#""partitionValues":{"long_part":"999"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let cases = [
        (
            "between inclusive",
            datafusion::logical_expr::col("long_part").between(
                datafusion::logical_expr::lit(-1_i64),
                datafusion::logical_expr::lit(7_i64),
            ),
        ),
        (
            "not between",
            datafusion::logical_expr::col("long_part").not_between(
                datafusion::logical_expr::lit(-1_i64),
                datafusion::logical_expr::lit(7_i64),
            ),
        ),
        (
            "contradictory between",
            datafusion::logical_expr::col("long_part").between(
                datafusion::logical_expr::lit(10_i64),
                datafusion::logical_expr::lit(-10_i64),
            ),
        ),
        (
            "contradictory not between",
            datafusion::logical_expr::col("long_part").not_between(
                datafusion::logical_expr::lit(10_i64),
                datafusion::logical_expr::lit(-10_i64),
            ),
        ),
    ];

    for (name, filter) in cases {
        let support = provider.supports_filters_pushdown(&[&filter])?;
        assert_eq!(support, vec![TableProviderFilterPushDown::Exact], "{name}");

        provider
            .scan(&state, Some(&vec![0]), &[filter], None)
            .await?;
    }

    Ok(())
}

#[tokio::test]
async fn integer_partition_boolean_composition_and_projection_are_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "integer-partition-boolean-composition-boundary",
        INTEGER_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["long_part"]"#,
        &[
            r#""partitionValues":{"long_part":"7"}"#,
            r#""partitionValues":{"long_part":"-1"}"#,
            r#""partitionValues":{"long_part":"20"}"#,
            r#""partitionValues":{"long_part":null}"#,
            r#""partitionValues":{"long_part":""}"#,
            r#""partitionValues":{"long_part":"999"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let separate_and_filters = vec![
        datafusion::logical_expr::col("long_part").gt_eq(datafusion::logical_expr::lit(-1_i64)),
        datafusion::logical_expr::col("long_part").lt(datafusion::logical_expr::lit(20_i64)),
    ];
    let whole_and_filter = datafusion::logical_expr::col("long_part")
        .gt_eq(datafusion::logical_expr::lit(-1_i64))
        .and(datafusion::logical_expr::col("long_part").lt(datafusion::logical_expr::lit(20_i64)));
    let whole_or_filter = datafusion::logical_expr::col("long_part")
        .eq(datafusion::logical_expr::lit(7_i64))
        .or(datafusion::logical_expr::col("long_part").eq(datafusion::logical_expr::lit(20_i64)));
    let whole_not_filter = Expr::Not(Box::new(
        datafusion::logical_expr::col("long_part").eq(datafusion::logical_expr::lit(7_i64)),
    ));
    let cases = [
        ("separate filters combine with and", separate_and_filters),
        ("whole and", vec![whole_and_filter]),
        ("whole or", vec![whole_or_filter]),
        ("whole not", vec![whole_not_filter]),
    ];

    for (name, filters) in cases {
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let support = provider.supports_filters_pushdown(&filter_refs)?;
        assert_eq!(
            support,
            vec![TableProviderFilterPushDown::Exact; filters.len()],
            "{name}"
        );

        provider
            .scan(&state, Some(&vec![0]), &filters, None)
            .await?;
    }

    Ok(())
}

#[tokio::test]
async fn integer_partition_comparisons_are_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "integer-partition-comparisons-boundary",
        INTEGER_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["long_part"]"#,
        &[
            r#""partitionValues":{"long_part":"7"}"#,
            r#""partitionValues":{"long_part":"-1"}"#,
            r#""partitionValues":{"long_part":null}"#,
            r#""partitionValues":{"long_part":""}"#,
            r#""partitionValues":{"long_part":"999"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let cases = [
        (
            "less than",
            datafusion::logical_expr::col("long_part").lt(datafusion::logical_expr::lit(7_i64)),
        ),
        (
            "less than or equal",
            datafusion::logical_expr::col("long_part").lt_eq(datafusion::logical_expr::lit(-1_i64)),
        ),
        (
            "greater than",
            datafusion::logical_expr::col("long_part").gt(datafusion::logical_expr::lit(-1_i64)),
        ),
        (
            "greater than or equal",
            datafusion::logical_expr::col("long_part").gt_eq(datafusion::logical_expr::lit(7_i64)),
        ),
        (
            "reversed less than",
            datafusion::logical_expr::lit(7_i64).gt(datafusion::logical_expr::col("long_part")),
        ),
    ];

    for (name, filter) in cases {
        let support = provider.supports_filters_pushdown(&[&filter])?;
        assert_eq!(support, vec![TableProviderFilterPushDown::Exact], "{name}");

        provider
            .scan(&state, Some(&vec![0]), &[filter], None)
            .await?;
    }

    Ok(())
}

#[tokio::test]
async fn integer_partition_equality_and_membership_are_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "integer-partition-equality-membership-boundary",
        INTEGER_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["long_part"]"#,
        &[
            r#""partitionValues":{"long_part":"7"}"#,
            r#""partitionValues":{"long_part":"-1"}"#,
            r#""partitionValues":{"long_part":null}"#,
            r#""partitionValues":{"long_part":""}"#,
            r#""partitionValues":{"long_part":"999"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let cases = [
        (
            "equality",
            datafusion::logical_expr::col("long_part").eq(datafusion::logical_expr::lit(7_i64)),
        ),
        (
            "reversed equality",
            datafusion::logical_expr::lit(7_i64).eq(datafusion::logical_expr::col("long_part")),
        ),
        (
            "inequality",
            datafusion::logical_expr::col("long_part").not_eq(datafusion::logical_expr::lit(7_i64)),
        ),
        (
            "in list",
            datafusion::logical_expr::col("long_part").in_list(
                vec![
                    datafusion::logical_expr::lit(7_i64),
                    datafusion::logical_expr::lit(-1_i64),
                ],
                false,
            ),
        ),
        (
            "not in list",
            datafusion::logical_expr::col("long_part")
                .in_list(vec![datafusion::logical_expr::lit(7_i64)], true),
        ),
    ];

    for (name, filter) in cases {
        let support = provider.supports_filters_pushdown(&[&filter])?;
        assert_eq!(support, vec![TableProviderFilterPushDown::Exact], "{name}");

        provider
            .scan(&state, Some(&vec![0]), &[filter], None)
            .await?;
    }

    Ok(())
}

#[test]
fn integer_partition_width_bounds_remain_unsupported_for_direct_filters()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema(
        "integer-partition-width-boundaries",
        INTEGER_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["byte_part","short_part","int_part","long_part"]"#,
        r#""partitionValues":{"byte_part":"127","short_part":"32767","int_part":"2147483647","long_part":"9223372036854775807"}"#,
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let in_range_filters = [
        datafusion::logical_expr::col("byte_part").eq(datafusion::logical_expr::lit(127_i8)),
        datafusion::logical_expr::col("short_part").eq(datafusion::logical_expr::lit(32767_i16)),
        datafusion::logical_expr::col("int_part").eq(datafusion::logical_expr::lit(2147483647_i32)),
        datafusion::logical_expr::col("long_part")
            .eq(datafusion::logical_expr::lit(9223372036854775807_i64)),
        datafusion::logical_expr::col("byte_part").lt(datafusion::logical_expr::lit(127_i8)),
        datafusion::logical_expr::col("short_part").gt_eq(datafusion::logical_expr::lit(32767_i16)),
        datafusion::logical_expr::col("int_part").between(
            datafusion::logical_expr::lit(-2147483648_i32),
            datafusion::logical_expr::lit(2147483647_i32),
        ),
    ];
    let unsupported_filters = [
        datafusion::logical_expr::col("byte_part").eq(datafusion::logical_expr::lit(128_i16)),
        datafusion::logical_expr::col("short_part").eq(datafusion::logical_expr::lit(32768_i32)),
        datafusion::logical_expr::col("int_part").eq(datafusion::logical_expr::lit(2147483648_i64)),
        datafusion::logical_expr::col("byte_part").lt(datafusion::logical_expr::lit(128_i16)),
        datafusion::logical_expr::col("short_part").gt_eq(datafusion::logical_expr::lit(32768_i32)),
        datafusion::logical_expr::col("int_part").between(
            datafusion::logical_expr::lit(-2147483649_i64),
            datafusion::logical_expr::lit(2147483647_i32),
        ),
    ];
    let in_range_refs = in_range_filters.iter().collect::<Vec<_>>();
    let unsupported_refs = unsupported_filters.iter().collect::<Vec<_>>();

    let in_range_plan = provider.plan_supports_filters_pushdown(&in_range_refs);
    let unsupported_plan = provider.plan_supports_filters_pushdown(&unsupported_refs);

    assert_eq!(in_range_plan.exact_count, in_range_filters.len());
    assert_eq!(in_range_plan.unsupported_count, 0);
    assert_eq!(unsupported_plan.exact_count, 0);
    assert_eq!(
        unsupported_plan.unsupported_count,
        unsupported_filters.len()
    );

    Ok(())
}

#[tokio::test]
async fn integer_partition_null_checks_are_exact_at_scan_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema_and_adds(
        "integer-partition-null-checks-boundary",
        INTEGER_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["long_part"]"#,
        &[
            r#""partitionValues":{"long_part":"7"}"#,
            r#""partitionValues":{"long_part":"-1"}"#,
            r#""partitionValues":{"long_part":null}"#,
            r#""partitionValues":{"long_part":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    let provider = DeltaTableProvider::try_new(source, preflight)?;
    let state = SessionContext::new().state();
    let cases = [
        (
            "is null",
            datafusion::logical_expr::col("long_part").is_null(),
        ),
        (
            "is not null",
            datafusion::logical_expr::col("long_part").is_not_null(),
        ),
    ];

    for (name, filter) in cases {
        let support = provider.supports_filters_pushdown(&[&filter])?;
        assert_eq!(support, vec![TableProviderFilterPushDown::Exact], "{name}");

        provider
            .scan(&state, Some(&vec![0]), &[filter], None)
            .await?;
    }

    Ok(())
}

#[tokio::test]
async fn sql_integer_partition_null_checks_are_exact_kernel_pushdown()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema_and_adds(
        "sql-integer-partition-null-checks",
        INTEGER_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["long_part"]"#,
        &[
            r#""partitionValues":{"long_part":"7"}"#,
            r#""partitionValues":{"long_part":"-1"}"#,
            r#""partitionValues":{"long_part":null}"#,
            r#""partitionValues":{"long_part":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    let cases = [
        ("is null", "select id from orders where long_part is null"),
        (
            "is not null",
            "select id from orders where long_part is not null",
        ),
    ];

    for (name, sql) in cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(
            !plan_display.contains("FilterExec"),
            "{name} should not keep residual filter:\n{plan_display}"
        );
        assert_eq!(scans.len(), 1, "{name}: {plan_display}");
        assert_eq!(scans[0].scan_plan().pushed_filter_plan.exact_count, 1);
        assert_eq!(
            scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
            1
        );
        assert_eq!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .residual_filter_count,
            0
        );
        assert!(
            scans[0].scan_plan().kernel_partition_predicate.is_some(),
            "{name}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn sql_boolean_partition_null_checks_are_exact_kernel_pushdown()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema_and_adds(
        "sql-boolean-partition-null-checks",
        BOOLEAN_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["is_current"]"#,
        &[
            r#""partitionValues":{"is_current":"true"}"#,
            r#""partitionValues":{"is_current":"false"}"#,
            r#""partitionValues":{"is_current":null}"#,
            r#""partitionValues":{"is_current":""}"#,
            r#""partitionValues":{"is_current":"false"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    let cases = [
        ("is null", "select id from orders where is_current is null"),
        (
            "is not null",
            "select id from orders where is_current is not null",
        ),
    ];

    for (name, sql) in cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(
            !plan_display.contains("FilterExec"),
            "{name} should not keep residual filter:\n{plan_display}"
        );
        assert_eq!(scans.len(), 1, "{name}: {plan_display}");
        assert_eq!(scans[0].scan_plan().pushed_filter_plan.exact_count, 1);
        assert_eq!(
            scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
            1
        );
        assert_eq!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .residual_filter_count,
            0
        );
        assert!(
            scans[0].scan_plan().kernel_partition_predicate.is_some(),
            "{name}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn sql_binary_partition_null_checks_are_exact_kernel_pushdown()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema_and_adds(
        "sql-binary-partition-null-checks",
        BINARY_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["payload"]"#,
        &[
            r#""partitionValues":{"payload":"hello"}"#,
            r#""partitionValues":{"payload":"world"}"#,
            r#""partitionValues":{"payload":null}"#,
            r#""partitionValues":{"payload":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    let cases = [
        ("is null", "select id from orders where payload is null"),
        (
            "is not null",
            "select id from orders where payload is not null",
        ),
    ];

    for (name, sql) in cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(
            !plan_display.contains("FilterExec"),
            "{name} should not keep residual filter:\n{plan_display}"
        );
        assert_eq!(scans.len(), 1, "{name}: {plan_display}");
        assert_eq!(scans[0].scan_plan().pushed_filter_plan.exact_count, 1);
        assert_eq!(
            scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
            1
        );
        assert_eq!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .residual_filter_count,
            0
        );
        assert!(
            scans[0].scan_plan().kernel_partition_predicate.is_some(),
            "{name}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn sql_binary_partition_equality_and_membership_are_exact_kernel_pushdown()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema_and_adds(
        "sql-binary-partition-equality-membership",
        BINARY_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["payload"]"#,
        &[
            r#""partitionValues":{"payload":"hello"}"#,
            r#""partitionValues":{"payload":"world"}"#,
            r#""partitionValues":{"payload":"/=%"}"#,
            r#""partitionValues":{"payload":null}"#,
            r#""partitionValues":{"payload":""}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    let cases = [
        (
            "binary literal equality",
            "select id from orders where payload = X'68656C6C6F'",
        ),
        (
            "reversed binary literal equality",
            "select id from orders where X'2F3D25' = payload",
        ),
        (
            "binary literal inequality",
            "select id from orders where payload != X'68656C6C6F'",
        ),
        (
            "binary literal in list",
            "select id from orders where payload in (X'68656C6C6F', X'2F3D25', X'68656C6C6F')",
        ),
        (
            "binary literal not in list",
            "select id from orders where payload not in (X'776F726C64')",
        ),
    ];

    for (name, sql) in cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(
            !plan_display.contains("FilterExec"),
            "{name} should not keep residual filter:\n{plan_display}"
        );
        assert_eq!(scans.len(), 1, "{name}: {plan_display}");
        assert_eq!(
            scans[0].scan_plan().pushed_filter_plan.exact_count,
            1,
            "{name}: {plan_display}"
        );
        assert_eq!(
            scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
            1,
            "{name}: {plan_display}"
        );
        assert_eq!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .residual_filter_count,
            0
        );
        assert!(
            scans[0].scan_plan().kernel_partition_predicate.is_some(),
            "{name}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn sql_date_partition_equality_and_membership_are_exact_kernel_pushdown()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema_and_adds(
        "sql-date-partition-equality-membership",
        DATE_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_date"]"#,
        &[
            r#""partitionValues":{"event_date":"2026-01-01"}"#,
            r#""partitionValues":{"event_date":"2024-02-29"}"#,
            r#""partitionValues":{"event_date":"1969-12-31"}"#,
            r#""partitionValues":{"event_date":null}"#,
            r#""partitionValues":{"event_date":""}"#,
            r#""partitionValues":{"event_date":"2027-12-31"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;
    let cases = [
        (
            "date literal equality",
            "select id from orders where event_date = DATE '2026-01-01'",
        ),
        (
            "string literal equality coerced by datafusion",
            "select id from orders where event_date = '2026-01-01'",
        ),
        (
            "reversed date literal equality pre epoch",
            "select id from orders where DATE '1969-12-31' = event_date",
        ),
        (
            "date literal inequality",
            "select id from orders where event_date != DATE '2026-01-01'",
        ),
        (
            "date literal in list",
            "select id from orders where event_date in (DATE '2026-01-01', DATE '2024-02-29', DATE '2026-01-01')",
        ),
        (
            "date literal not in list",
            "select id from orders where event_date not in (DATE '2026-01-01')",
        ),
    ];

    for (name, sql) in cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(
            !plan_display.contains("FilterExec"),
            "{name} should not keep residual filter:\n{plan_display}"
        );
        assert_eq!(scans.len(), 1, "{name}: {plan_display}");
        assert!(
            scans[0].scan_plan().pushed_filter_plan.exact_count > 0,
            "{name}: {plan_display}"
        );
        assert!(
            scans[0].scan_plan().pushed_filter_plan.pushed_filter_count > 0,
            "{name}: {plan_display}"
        );
        assert_eq!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .residual_filter_count,
            0
        );
        assert!(
            scans[0].scan_plan().kernel_partition_predicate.is_some(),
            "{name}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn sql_decimal_partition_unsafe_literal_filters_keep_residual_filter()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema_and_adds(
        "sql-decimal-partition-unsafe-literal-residuals",
        DECIMAL_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["amount"]"#,
        &[
            r#""partitionValues":{"amount":"123.45"}"#,
            r#""partitionValues":{"amount":"0.00"}"#,
            r#""partitionValues":{"amount":"-1.23"}"#,
            r#""partitionValues":{"amount":null}"#,
            r#""partitionValues":{"amount":""}"#,
            r#""partitionValues":{"amount":"999.99"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    let cases = [(
        "string literal equality casts column to utf8",
        "select id from orders where amount = '123.45'",
    )];

    for (name, sql) in cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(
            plan_display.contains("FilterExec"),
            "{name} unexpectedly became exact:\n{plan_display}"
        );
        assert_eq!(scans.len(), 1, "{name}: {plan_display}");
        assert_eq!(
            scans[0].scan_plan().pushed_filter_plan.exact_count,
            0,
            "{name}: {plan_display}"
        );
        assert_eq!(
            scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
            0,
            "{name}: {plan_display}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn sql_decimal_partition_comparisons_are_exact_kernel_pushdown()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema_and_adds(
        "sql-decimal-partition-comparisons",
        DECIMAL_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["amount"]"#,
        &[
            r#""partitionValues":{"amount":"123.45"}"#,
            r#""partitionValues":{"amount":"0.00"}"#,
            r#""partitionValues":{"amount":"-1.23"}"#,
            r#""partitionValues":{"amount":null}"#,
            r#""partitionValues":{"amount":""}"#,
            r#""partitionValues":{"amount":"999.99"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    let cases = [
        (
            "decimal literal ordering",
            "select id from orders where amount < DECIMAL '123.45'",
        ),
        (
            "numeric literal ordering",
            "select id from orders where amount > 0.00",
        ),
        (
            "reversed decimal literal ordering different scale",
            "select id from orders where DECIMAL '-1.230' >= amount",
        ),
        (
            "decimal literal between",
            "select id from orders where amount between DECIMAL '0.00' and DECIMAL '123.45'",
        ),
        (
            "decimal literal not between",
            "select id from orders where amount not between DECIMAL '0.00' and DECIMAL '123.45'",
        ),
    ];

    for (name, sql) in cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(
            !plan_display.contains("FilterExec"),
            "{name} should not keep residual filter:\n{plan_display}"
        );
        assert_eq!(scans.len(), 1, "{name}: {plan_display}");
        assert!(
            scans[0].scan_plan().pushed_filter_plan.exact_count > 0,
            "{name}: {plan_display}"
        );
        assert!(
            scans[0].scan_plan().pushed_filter_plan.pushed_filter_count > 0,
            "{name}: {plan_display}"
        );
        assert_eq!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .residual_filter_count,
            0
        );
        assert!(
            scans[0].scan_plan().kernel_partition_predicate.is_some(),
            "{name}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn sql_decimal_partition_equality_and_membership_are_exact_kernel_pushdown()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema_and_adds(
        "sql-decimal-partition-equality-membership",
        DECIMAL_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["amount"]"#,
        &[
            r#""partitionValues":{"amount":"123.45"}"#,
            r#""partitionValues":{"amount":"0.00"}"#,
            r#""partitionValues":{"amount":"-1.23"}"#,
            r#""partitionValues":{"amount":null}"#,
            r#""partitionValues":{"amount":""}"#,
            r#""partitionValues":{"amount":"999.99"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    let cases = [
        (
            "decimal literal equality",
            "select id from orders where amount = DECIMAL '123.45'",
        ),
        (
            "numeric literal equality",
            "select id from orders where amount = 123.45",
        ),
        (
            "reversed decimal literal equality different scale",
            "select id from orders where DECIMAL '-1.230' = amount",
        ),
        (
            "decimal literal inequality",
            "select id from orders where amount != DECIMAL '123.45'",
        ),
        (
            "decimal literal in list",
            "select id from orders where amount in (DECIMAL '123.45', DECIMAL '0.00', DECIMAL '123.450')",
        ),
        (
            "decimal literal not in list",
            "select id from orders where amount not in (DECIMAL '123.45')",
        ),
    ];

    for (name, sql) in cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(
            !plan_display.contains("FilterExec"),
            "{name} should not keep residual filter:\n{plan_display}"
        );
        assert_eq!(scans.len(), 1, "{name}: {plan_display}");
        assert!(
            scans[0].scan_plan().pushed_filter_plan.exact_count > 0,
            "{name}: {plan_display}"
        );
        assert!(
            scans[0].scan_plan().pushed_filter_plan.pushed_filter_count > 0,
            "{name}: {plan_display}"
        );
        assert_eq!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .residual_filter_count,
            0
        );
        assert!(
            scans[0].scan_plan().kernel_partition_predicate.is_some(),
            "{name}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn sql_decimal_partition_null_checks_are_exact_kernel_pushdown()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema_and_adds(
        "sql-decimal-partition-null-checks",
        DECIMAL_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["amount"]"#,
        &[
            r#""partitionValues":{"amount":"123.45"}"#,
            r#""partitionValues":{"amount":"0.00"}"#,
            r#""partitionValues":{"amount":"-1.23"}"#,
            r#""partitionValues":{"amount":null}"#,
            r#""partitionValues":{"amount":""}"#,
            r#""partitionValues":{"amount":"999.99"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;
    let cases = [
        ("is null", "select id from orders where amount is null"),
        (
            "is not null",
            "select id from orders where amount is not null",
        ),
    ];

    for (name, sql) in cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(
            !plan_display.contains("FilterExec"),
            "{name} should not keep residual filter:\n{plan_display}"
        );
        assert_eq!(scans.len(), 1, "{name}: {plan_display}");
        assert_eq!(
            scans[0].scan_plan().pushed_filter_plan.exact_count,
            1,
            "{name}: {plan_display}"
        );
        assert_eq!(
            scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
            1,
            "{name}: {plan_display}"
        );
        assert_eq!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .residual_filter_count,
            0
        );
        assert!(
            scans[0].scan_plan().kernel_partition_predicate.is_some(),
            "{name}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn sql_date_partition_range_filters_are_exact_kernel_pushdown()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema_and_adds(
        "sql-date-partition-range-filters",
        DATE_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_date"]"#,
        &[
            r#""partitionValues":{"event_date":"2026-01-01"}"#,
            r#""partitionValues":{"event_date":"2024-02-29"}"#,
            r#""partitionValues":{"event_date":"1969-12-31"}"#,
            r#""partitionValues":{"event_date":"2026-01-02"}"#,
            r#""partitionValues":{"event_date":null}"#,
            r#""partitionValues":{"event_date":""}"#,
            r#""partitionValues":{"event_date":"2027-12-31"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;
    let cases = [
        (
            "date literal ordering",
            "select id from orders where event_date < DATE '2026-01-02'",
        ),
        (
            "reversed date literal ordering",
            "select id from orders where DATE '2026-01-01' > event_date",
        ),
        (
            "date literal between",
            "select id from orders where event_date between DATE '2026-01-01' and DATE '2026-01-02'",
        ),
        (
            "date literal not between",
            "select id from orders where event_date not between DATE '2024-02-29' and DATE '2026-01-01'",
        ),
        (
            "contradictory date literal between",
            "select id from orders where event_date between DATE '2026-01-01' and DATE '2024-02-29'",
        ),
        (
            "contradictory date literal not between",
            "select id from orders where event_date not between DATE '2026-01-01' and DATE '2024-02-29'",
        ),
    ];

    for (name, sql) in cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(
            !plan_display.contains("FilterExec"),
            "{name} should not keep residual filter:\n{plan_display}"
        );
        assert_eq!(scans.len(), 1, "{name}: {plan_display}");
        assert!(
            scans[0].scan_plan().pushed_filter_plan.exact_count > 0,
            "{name}: {plan_display}"
        );
        assert!(
            scans[0].scan_plan().pushed_filter_plan.pushed_filter_count > 0,
            "{name}: {plan_display}"
        );
        assert_eq!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .residual_filter_count,
            0
        );
        assert!(
            scans[0].scan_plan().kernel_partition_predicate.is_some(),
            "{name}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn sql_date_partition_null_checks_are_exact_kernel_pushdown()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema_and_adds(
        "sql-date-partition-null-checks",
        DATE_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["event_date"]"#,
        &[
            r#""partitionValues":{"event_date":"2026-01-01"}"#,
            r#""partitionValues":{"event_date":"1969-12-31"}"#,
            r#""partitionValues":{"event_date":null}"#,
            r#""partitionValues":{"event_date":""}"#,
            r#""partitionValues":{"event_date":"2027-12-31"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;
    let cases = [
        ("is null", "select id from orders where event_date is null"),
        (
            "is not null",
            "select id from orders where event_date is not null",
        ),
    ];

    for (name, sql) in cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display =
            datafusion::physical_plan::displayable(physical_plan.as_ref()).indent(true);
        let plan_display = plan_display.to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(
            !plan_display.contains("FilterExec"),
            "{name} should not keep residual filter:\n{plan_display}"
        );
        assert_eq!(scans.len(), 1, "{name}: {plan_display}");
        assert_eq!(
            scans[0].scan_plan().pushed_filter_plan.exact_count,
            1,
            "{name}: {plan_display}"
        );
        assert_eq!(
            scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
            1,
            "{name}: {plan_display}"
        );
        assert_eq!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .residual_filter_count,
            0
        );
        assert!(
            scans[0].scan_plan().kernel_partition_predicate.is_some(),
            "{name}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn sql_boolean_partition_literal_operators_are_exact_kernel_pushdown()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema_and_adds(
        "sql-boolean-partition-shorthand-rewrites",
        BOOLEAN_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["is_current"]"#,
        &[
            r#""partitionValues":{"is_current":"true"}"#,
            r#""partitionValues":{"is_current":"false"}"#,
            r#""partitionValues":{"is_current":null}"#,
            r#""partitionValues":{"is_current":""}"#,
            r#""partitionValues":{"is_current":"false"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    let cases = [
        ("shorthand", "select id from orders where is_current"),
        (
            "not shorthand",
            "select id from orders where not is_current",
        ),
        (
            "equality true rewrite",
            "select id from orders where is_current = true",
        ),
        (
            "inequality true rewrite",
            "select id from orders where is_current != true",
        ),
        (
            "reversed equality false rewrite",
            "select id from orders where false = is_current",
        ),
        (
            "in list rewrite",
            "select id from orders where is_current in (true, false, true)",
        ),
        (
            "not in list rewrite",
            "select id from orders where is_current not in (true)",
        ),
        (
            "whole and",
            "select id from orders where is_current = true and is_current is not null",
        ),
        (
            "whole or",
            "select id from orders where is_current = true or is_current is null",
        ),
        (
            "not equality",
            "select id from orders where not is_current = true",
        ),
    ];

    for (name, sql) in cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(
            !plan_display.contains("FilterExec"),
            "{name} should not keep residual filter:\n{plan_display}"
        );
        assert_eq!(scans.len(), 1, "{name}: {plan_display}");
        assert!(
            scans[0].scan_plan().pushed_filter_plan.exact_count > 0,
            "{name}: {plan_display}"
        );
        assert!(
            scans[0].scan_plan().pushed_filter_plan.pushed_filter_count > 0,
            "{name}: {plan_display}"
        );
        assert_eq!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .residual_filter_count,
            0
        );
        assert!(
            scans[0].scan_plan().kernel_partition_predicate.is_some(),
            "{name}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn sql_boolean_partition_ordering_filters_keep_residual_filter()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema_and_adds(
        "sql-boolean-partition-ordering-residuals",
        BOOLEAN_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["is_current"]"#,
        &[
            r#""partitionValues":{"is_current":"true"}"#,
            r#""partitionValues":{"is_current":"false"}"#,
            r#""partitionValues":{"is_current":null}"#,
            r#""partitionValues":{"is_current":""}"#,
            r#""partitionValues":{"is_current":"false"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    let cases = [
        ("less than", "select id from orders where is_current < true"),
        (
            "between",
            "select id from orders where is_current between false and true",
        ),
        (
            "not between",
            "select id from orders where is_current not between false and true",
        ),
    ];

    for (name, sql) in cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(
            plan_display.contains("FilterExec"),
            "{name} unexpectedly became exact:\n{plan_display}"
        );
        assert_eq!(scans.len(), 1, "{name}: {plan_display}");
        assert_eq!(scans[0].scan_plan().pushed_filter_plan.exact_count, 0);
        assert_eq!(
            scans[0].scan_plan().pushed_filter_plan.pushed_filter_count,
            0
        );
    }

    Ok(())
}

#[tokio::test]
async fn sql_integer_partition_literal_operators_are_exact_kernel_pushdown()
-> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = DeltaLogTable::new_with_schema_and_adds(
        "sql-integer-partition-equality-membership",
        INTEGER_PARTITION_SCHEMA_FIELDS_JSON,
        r#"["long_part"]"#,
        &[
            r#""partitionValues":{"long_part":"7"}"#,
            r#""partitionValues":{"long_part":"-1"}"#,
            r#""partitionValues":{"long_part":"20"}"#,
            r#""partitionValues":{"long_part":null}"#,
            r#""partitionValues":{"long_part":""}"#,
            r#""partitionValues":{"long_part":"999"}"#,
            r#""partitionValues":{}"#,
        ],
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;
    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;

    let cases = [
        ("equality", "select id from orders where long_part = 7"),
        (
            "reversed equality",
            "select id from orders where 7 = long_part",
        ),
        ("inequality", "select id from orders where long_part != 7"),
        (
            "in list",
            "select id from orders where long_part in (7, -1)",
        ),
        (
            "not in list",
            "select id from orders where long_part not in (7)",
        ),
        ("less than", "select id from orders where long_part < 7"),
        (
            "less than or equal",
            "select id from orders where long_part <= -1",
        ),
        ("greater than", "select id from orders where long_part > -1"),
        (
            "reversed greater than",
            "select id from orders where 7 > long_part",
        ),
        (
            "between inclusive",
            "select id from orders where long_part between -1 and 7",
        ),
        (
            "not between",
            "select id from orders where long_part not between -1 and 7",
        ),
        (
            "contradictory between",
            "select id from orders where long_part between 10 and -10",
        ),
        (
            "contradictory not between",
            "select id from orders where long_part not between 10 and -10",
        ),
        (
            "whole and",
            "select id from orders where long_part >= -1 and long_part < 20",
        ),
        (
            "whole or",
            "select id from orders where long_part = 7 or long_part = 20",
        ),
        ("not", "select id from orders where not long_part = 7"),
    ];

    for (name, sql) in cases {
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let plan_display = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
            .to_string();
        let mut scans = Vec::new();
        find_delta_scan_plans(physical_plan.as_ref(), &mut scans);

        assert!(
            !plan_display.contains("FilterExec"),
            "{name} should not keep residual filter:\n{plan_display}"
        );
        assert_eq!(scans.len(), 1, "{name}: {plan_display}");
        assert!(
            scans[0].scan_plan().pushed_filter_plan.exact_count > 0,
            "{name}: {plan_display}"
        );
        assert!(
            scans[0].scan_plan().pushed_filter_plan.pushed_filter_count > 0,
            "{name}: {plan_display}"
        );
        assert_eq!(
            scans[0]
                .scan_plan()
                .pushed_filter_plan
                .residual_filter_count,
            0
        );
        assert!(
            scans[0].scan_plan().kernel_partition_predicate.is_some(),
            "{name}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn sql_analysis_accepts_nested_source_columns_without_target_planning()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema(
        "nested-schema",
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
    let ctx = SessionContext::new();

    register_delta_sources(
        &ctx,
        vec![DeltaTableProviderConfig {
            source,
            protocol: preflight,
            scan_target_partitions: None,
        }],
    )?;
    let dataframe = ctx.sql("select id from orders").await?;
    let schema = dataframe.schema();

    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(0).data_type(), &DataType::Int32);

    Ok(())
}

#[test]
fn schema_conversion_failure_reports_source_and_field_context()
-> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaLogTable::new_with_schema(
        "schema-failure",
        INVALID_NESTED_IDS_SCHEMA_FIELDS_JSON,
        "[]",
        r#""partitionValues":{}"#,
    )?;
    let source = load_delta_source(DeltaSourceConfig {
        name: "orders".to_owned(),
        table_uri: table.path().to_string_lossy().to_string(),
        version: None,
    })?;
    let preflight = preflight_delta_protocol(&source)?;

    let result = DeltaTableProvider::try_new(source, preflight);

    assert!(matches!(
        result,
        Err(DeltaFunnelError::DeltaSourceSchema {
            source_name,
            reason,
            ..
        }) if source_name == "orders"
            && reason.contains("bad_array")
            && reason.contains("delta.columnMapping.nested.ids")
    ));

    Ok(())
}
