#![cfg(all(feature = "datafusion", feature = "native-async"))]

#[allow(dead_code)]
mod support;

use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow::{
    array::{Int32Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::{
    common::DataFusionError,
    datasource::{MemTable, TableProvider, TableType},
    logical_expr::{TableProviderFilterPushDown, col, lit},
    physical_plan::{ExecutionPlan, displayable},
    prelude::{SessionConfig, SessionContext},
};
use delta_arrow_reader::DeltaReaderBackend;
use delta_arrow_reader::{
    DeltaDataFusionScanOptions, DeltaReaderError, DeltaReaderExecutionOptions, DeltaReaderPhase,
    DeltaTableBuilder, DeltaTableProvider, collect_delta_datafusion_metrics, register_delta_table,
};
use futures_util::StreamExt;
use parquet::arrow::ArrowWriter;
use serde_json::{Value, json};

use support::RealParquetDeltaTable;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

struct TestTable(PathBuf);

impl TestTable {
    fn empty(name: &str) -> TestResult<Self> {
        let table = Self::new(name)?;
        table.write_log(&[protocol(1), metadata()])?;
        Ok(table)
    }

    fn partitioned(name: &str) -> TestResult<Self> {
        let table = Self::new(name)?;
        let west = table.write_parquet("west.parquet", &[1, 2])?;
        let east = table.write_parquet("east.parquet", &[3, 4])?;
        table.write_log(&[
            protocol(1),
            metadata(),
            add("west.parquet", west, "west", 1, 2),
            add("east.parquet", east, "east", 3, 4),
        ])?;
        Ok(table)
    }

    fn unsupported(name: &str) -> TestResult<Self> {
        let table = Self::new(name)?;
        table.write_log(&[protocol(4), metadata()])?;
        Ok(table)
    }

    fn new(name: &str) -> TestResult<Self> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let path = Path::new("target")
            .join("delta-arrow-reader-provider-tests")
            .join(format!("{}-{name}-{nanos}", std::process::id()));
        fs::create_dir_all(path.join("_delta_log"))?;
        Ok(Self(path))
    }

    fn uri(&self) -> String {
        self.0.to_string_lossy().into_owned()
    }

    fn write_parquet(&self, name: &str, ids: &[i32]) -> TestResult<u64> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(ids.to_vec()))],
        )?;
        let path = self.0.join(name);
        let mut writer = ArrowWriter::try_new(fs::File::create(&path)?, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(fs::metadata(path)?.len())
    }

    fn write_log(&self, actions: &[Value]) -> TestResult {
        let contents = actions
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(
            self.0.join("_delta_log/00000000000000000000.json"),
            format!("{contents}\n"),
        )?;
        Ok(())
    }
}

impl Drop for TestTable {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn protocol(min_reader_version: i32) -> Value {
    json!({
        "protocol": {
            "minReaderVersion": min_reader_version,
            "minWriterVersion": 2
        }
    })
}

fn metadata() -> Value {
    let schema = json!({
        "type": "struct",
        "fields": [
            {"name": "id", "type": "integer", "nullable": false, "metadata": {}},
            {"name": "region", "type": "string", "nullable": true, "metadata": {}}
        ]
    });
    json!({
        "metaData": {
            "id": "delta-arrow-reader-provider-test",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": schema.to_string(),
            "partitionColumns": ["region"],
            "configuration": {},
            "createdTime": 1587968585495_i64
        }
    })
}

fn add(path: &str, size: u64, region: &str, min_id: i32, max_id: i32) -> Value {
    let stats = json!({
        "numRecords": 2,
        "minValues": {"id": min_id},
        "maxValues": {"id": max_id},
        "nullCount": {"id": 0}
    });
    json!({
        "add": {
            "path": path,
            "partitionValues": {"region": region},
            "size": size,
            "modificationTime": 1587968586000_i64,
            "dataChange": true,
            "stats": stats.to_string()
        }
    })
}

fn ids(batches: &[RecordBatch]) -> Vec<i32> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(batch.schema().index_of("id").expect("id column"))
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32 id")
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect()
}

fn regions(batches: &[RecordBatch]) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(batch.schema().index_of("region").expect("region column"))
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Utf8 region")
                .iter()
                .map(|value| value.expect("non-null region").to_owned())
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn collect_plan(
    context: &SessionContext,
    plan: Arc<dyn ExecutionPlan>,
) -> TestResult<Vec<RecordBatch>> {
    Ok(datafusion::physical_plan::collect(plan, context.task_ctx()).await?)
}

fn register_fixture(
    context: &SessionContext,
    name: &str,
    fixture: &RealParquetDeltaTable,
    options: DeltaDataFusionScanOptions,
) -> TestResult {
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy().into_owned()).load()?;
    register_delta_table(context, name, table, options)?;
    Ok(())
}

fn register_allowed_regions(context: &SessionContext, regions: Vec<&str>) -> TestResult {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "region",
        DataType::Utf8,
        true,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(regions))],
    )?;
    context.register_table(
        "allowed_regions",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]])?),
    )?;
    Ok(())
}

fn external_reader_error(error: &DataFusionError) -> TestResult<&DeltaReaderError> {
    let DataFusionError::External(source) = error else {
        return Err("DataFusion error did not preserve an external reader error".into());
    };
    source
        .downcast_ref::<DeltaReaderError>()
        .ok_or_else(|| "external source was not DeltaReaderError".into())
}

#[tokio::test]
#[cfg(feature = "native-async")]
async fn options_protocol_schema_pushdown_and_debug_match_the_provider_contract() -> TestResult {
    let fixture = TestTable::partitioned("provider-contract")?;
    let table = DeltaTableBuilder::new(fixture.uri()).load()?;
    let defaults = DeltaDataFusionScanOptions::default();
    assert_eq!(
        defaults.execution_options,
        DeltaReaderExecutionOptions::default()
    );
    assert_eq!(defaults.target_partitions, None);

    let provider = DeltaTableProvider::try_new(table.clone(), defaults)?;
    assert_eq!(provider.schema(), table.schema().clone());
    assert_eq!(provider.table_type(), TableType::Base);
    let debug = format!("{provider:?}");
    assert!(!debug.contains(&fixture.uri()));
    assert!(!debug.contains("provider-contract"));

    let filters = [
        col("region").eq(lit("west")),
        col("id").gt(lit(1_i32)),
        col("id") + lit(1_i32),
    ];
    let filter_refs = filters.iter().collect::<Vec<_>>();
    assert_eq!(
        provider.supports_filters_pushdown(&filter_refs)?,
        [
            TableProviderFilterPushDown::Exact,
            TableProviderFilterPushDown::Exact,
            TableProviderFilterPushDown::Unsupported,
        ]
    );

    let context = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    let unsupported = provider
        .scan(&context.state(), None, &[col("id") + lit(1_i32)], None)
        .await
        .expect_err("unsupported pushed filter must fail");
    let datafusion::common::DataFusionError::External(source) = unsupported else {
        return Err("scan error did not preserve DeltaReaderError".into());
    };
    let reader = source
        .downcast_ref::<delta_arrow_reader::DeltaReaderError>()
        .ok_or("external source was not DeltaReaderError")?;
    assert_eq!(reader.phase(), DeltaReaderPhase::ScanPlanning);

    let full = provider.scan(&context.state(), None, &[], Some(1)).await?;
    assert_eq!(full.properties().output_partitioning().partition_count(), 2);
    assert_eq!(
        full.partition_statistics(None)?,
        Arc::new(datafusion::common::Statistics::new_unknown(&full.schema()))
    );
    let mut full_ids = ids(&collect_plan(&context, Arc::clone(&full)).await?);
    full_ids.sort_unstable();
    assert_eq!(full_ids, [1, 2, 3, 4]);
    let metrics = collect_delta_datafusion_metrics(full.as_ref());
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0].source_name(), None);

    let explicit_target = DeltaTableProvider::try_new(
        table.clone(),
        DeltaDataFusionScanOptions {
            target_partitions: Some(1),
            ..Default::default()
        },
    )?;
    let one_partition = explicit_target
        .scan(&context.state(), None, &[], None)
        .await?;
    assert_eq!(
        one_partition
            .properties()
            .output_partitioning()
            .partition_count(),
        1
    );

    for projection in [vec![2], vec![0, 0]] {
        let error = provider
            .scan(&context.state(), Some(&projection), &[], None)
            .await
            .expect_err("invalid projection must fail");
        let datafusion::common::DataFusionError::External(source) = error else {
            return Err("projection error did not preserve DeltaReaderError".into());
        };
        let reader = source
            .downcast_ref::<delta_arrow_reader::DeltaReaderError>()
            .ok_or("external source was not DeltaReaderError")?;
        assert_eq!(reader.phase(), DeltaReaderPhase::ScanPlanning);
    }

    let projection = vec![1, 0];
    let projected = provider
        .scan(&context.state(), Some(&projection), &[], None)
        .await?;
    let projected_batches = collect_plan(&context, projected).await?;
    let mut projected_regions = regions(&projected_batches);
    projected_regions.sort();
    let mut projected_ids = ids(&projected_batches);
    projected_ids.sort_unstable();
    assert_eq!(projected_regions, ["east", "east", "west", "west"]);
    assert_eq!(projected_ids, [1, 2, 3, 4]);

    let empty_projection = Vec::new();
    let empty = provider
        .scan(&context.state(), Some(&empty_projection), &[], None)
        .await?;
    let empty_batches = collect_plan(&context, empty).await?;
    assert!(empty_batches.iter().all(|batch| batch.num_columns() == 0));
    assert_eq!(
        empty_batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        4
    );

    let zero_target = DeltaTableProvider::try_new(
        table.clone(),
        DeltaDataFusionScanOptions {
            target_partitions: Some(0),
            ..Default::default()
        },
    )
    .expect_err("zero target must fail");
    assert_eq!(zero_target.phase(), DeltaReaderPhase::Configuration);

    #[cfg(not(feature = "official-kernel"))]
    {
        let execution_options = DeltaReaderExecutionOptions::new()
            .with_reader_backend(DeltaReaderBackend::OfficialKernel)?;
        let unavailable = DeltaTableProvider::try_new(
            table,
            DeltaDataFusionScanOptions {
                execution_options,
                target_partitions: None,
            },
        )
        .expect_err("disabled backend must fail");
        assert_eq!(unavailable.phase(), DeltaReaderPhase::Configuration);
    }

    let unsupported_fixture = TestTable::unsupported("unsupported-provider")?;
    let unsupported = DeltaTableBuilder::new(unsupported_fixture.uri()).load()?;
    let error = DeltaTableProvider::try_new(unsupported, Default::default())
        .expect_err("unsupported protocol must fail");
    assert_eq!(error.phase(), DeltaReaderPhase::Protocol);
    Ok(())
}

#[tokio::test]
#[cfg(feature = "native-async")]
async fn registration_sql_metrics_duplicates_and_repeated_scans_are_exact() -> TestResult {
    let fixture = TestTable::partitioned("registration")?;
    let table = DeltaTableBuilder::new(fixture.uri()).load()?;
    let context = SessionContext::new();

    for invalid in ["", "1orders", "line-items", "select"] {
        let error = register_delta_table(
            &context,
            invalid,
            table.clone(),
            DeltaDataFusionScanOptions::default(),
        )
        .expect_err("invalid name must fail");
        assert_eq!(error.phase(), DeltaReaderPhase::DataFusion);
    }

    let registered = register_delta_table(
        &context,
        "Orders",
        table.clone(),
        DeltaDataFusionScanOptions {
            target_partitions: Some(2),
            ..Default::default()
        },
    )?;
    assert_eq!(registered.name, "Orders");
    assert_eq!(registered.version, table.version());
    let registered_provider = context.table_provider("orders").await?;
    assert!(!format!("{registered_provider:?}").contains("Orders"));
    let duplicate = register_delta_table(
        &context,
        "orders",
        table,
        DeltaDataFusionScanOptions::default(),
    )
    .expect_err("duplicate registration must fail");
    assert_eq!(duplicate.phase(), DeltaReaderPhase::DataFusion);
    assert!(!duplicate.to_string().contains("Orders"));
    assert!(
        duplicate
            .source()
            .and_then(|source| {
                source.downcast_ref::<Box<datafusion::common::DataFusionError>>()
            })
            .is_some()
    );

    let mut dataframe_ids = ids(&context
        .table("orders")
        .await?
        .select_columns(&["id"])?
        .collect()
        .await?);
    dataframe_ids.sort_unstable();
    assert_eq!(dataframe_ids, [1, 2, 3, 4]);

    let first = context
        .sql("SELECT region, id FROM orders WHERE id > 1 ORDER BY id LIMIT 2")
        .await?
        .create_physical_plan()
        .await?;
    let first_handles = collect_delta_datafusion_metrics(first.as_ref());
    assert_eq!(first_handles.len(), 1);
    assert_eq!(first_handles[0].source_name(), Some("Orders"));
    assert_eq!(first_handles[0].snapshot().reader.files_started, 0);
    assert_eq!(first_handles[0].snapshot().reader.estimated_rows, Some(4));
    let first_batches = collect_plan(&context, first).await?;
    assert_eq!(ids(&first_batches), [2, 3]);
    assert_eq!(regions(&first_batches), ["west", "east"]);
    assert_eq!(first_handles[0].snapshot().reader.rows_produced, 3);

    let second = context
        .sql("SELECT id FROM orders WHERE region = 'west' ORDER BY id")
        .await?
        .create_physical_plan()
        .await?;
    let second_handles = collect_delta_datafusion_metrics(second.as_ref());
    assert_eq!(second_handles.len(), 1);
    assert_eq!(second_handles[0].source_name(), Some("Orders"));
    assert_eq!(second_handles[0].snapshot().reader.files_started, 0);
    assert_eq!(second_handles[0].snapshot().reader.estimated_rows, Some(2));
    assert_eq!(ids(&collect_plan(&context, second).await?), [1, 2]);
    assert_eq!(first_handles[0].snapshot().reader.rows_produced, 3);
    assert_eq!(second_handles[0].snapshot().reader.rows_produced, 2);

    assert_eq!(
        ids(&context
            .sql("SELECT id FROM orders WHERE region = 'west' AND id > 1")
            .await?
            .collect()
            .await?),
        [2]
    );
    assert_eq!(
        ids(&context
            .sql("SELECT id FROM orders WHERE id + 1 > 3 ORDER BY id")
            .await?
            .collect()
            .await?),
        [3, 4]
    );
    assert_eq!(
        ids(&context
            .sql("SELECT o.id FROM orders AS o WHERE o.region = 'east' ORDER BY o.id")
            .await?
            .collect()
            .await?),
        [3, 4]
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "native-async")]
async fn caller_runtime_owns_concurrent_dataframe_execution() -> TestResult {
    let fixture = TestTable::partitioned("caller-runtime")?;
    let table = DeltaTableBuilder::new(fixture.uri()).load_async().await?;
    let context = SessionContext::new();
    register_delta_table(
        &context,
        "orders",
        table,
        DeltaDataFusionScanOptions::default(),
    )?;

    let left = context.sql("SELECT id FROM orders WHERE id <= 2").await?;
    let right = context.sql("SELECT id FROM orders WHERE id > 2").await?;
    let (left, right) = tokio::try_join!(left.collect(), right.collect())?;
    assert_eq!(ids(&left), [1, 2]);
    assert_eq!(ids(&right), [3, 4]);
    Ok(())
}

#[tokio::test]
#[cfg(all(feature = "native-async", feature = "official-kernel"))]
async fn native_exact_and_official_residual_execution_return_the_same_rows() -> TestResult {
    let fixture = TestTable::partitioned("backend-parity")?;
    let table = DeltaTableBuilder::new(fixture.uri()).load()?;
    let mut outputs = Vec::new();

    for (name, backend) in [
        ("native_orders", DeltaReaderBackend::NativeAsync),
        ("official_orders", DeltaReaderBackend::OfficialKernel),
    ] {
        let context = SessionContext::new();
        let execution_options = DeltaReaderExecutionOptions::new().with_reader_backend(backend)?;
        let provider = DeltaTableProvider::try_new(
            table.clone(),
            DeltaDataFusionScanOptions {
                execution_options,
                target_partitions: Some(2),
            },
        )?;
        let data_filter = col("id").gt(lit(1_i32));
        assert_eq!(
            provider.supports_filters_pushdown(&[&data_filter])?,
            [match backend {
                DeltaReaderBackend::NativeAsync => TableProviderFilterPushDown::Exact,
                DeltaReaderBackend::OfficialKernel => TableProviderFilterPushDown::Inexact,
            }]
        );
        context.register_table(name, Arc::new(provider))?;
        let mut batches = context
            .sql(&format!("SELECT id FROM {name} WHERE id > 1"))
            .await?
            .collect()
            .await?;
        batches.sort_by_key(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32 id")
                .value(0)
        });
        outputs.push(ids(&batches));
    }

    assert_eq!(outputs[0], [2, 3, 4]);
    assert_eq!(outputs[1], outputs[0]);
    Ok(())
}

#[tokio::test]
#[cfg(feature = "native-async")]
async fn sql_join_dynamic_filter_prunes_before_file_admission() -> TestResult {
    let context = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(1)
            .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", true)
            .set_bool(
                "datafusion.optimizer.enable_join_dynamic_filter_pushdown",
                true,
            ),
    );
    register_allowed_regions(&context, vec!["us-west"])?;
    let fixture = RealParquetDeltaTable::new_with_two_partition_values("provider-dynamic-pruning")?;
    register_fixture(
        &context,
        "orders",
        &fixture,
        DeltaDataFusionScanOptions {
            target_partitions: Some(1),
            ..Default::default()
        },
    )?;

    let plan = context
        .sql(
            "SELECT o.id, o.customer_name, o.region \
             FROM allowed_regions r JOIN orders o ON r.region = o.region \
             ORDER BY o.id",
        )
        .await?
        .create_physical_plan()
        .await?;
    let display = displayable(plan.as_ref()).indent(true).to_string();
    assert!(display.contains("HashJoinExec"), "{display}");
    assert!(display.contains("DeltaDataFusionExec"), "{display}");

    let metrics = collect_delta_datafusion_metrics(plan.as_ref());
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0].source_name(), Some("orders"));
    assert_eq!(metrics[0].snapshot().reader.files_planned, 2);
    assert_eq!(metrics[0].snapshot().reader.files_started, 0);

    let batches = collect_plan(&context, plan).await?;
    assert_eq!(ids(&batches), [1, 2]);
    assert_eq!(regions(&batches), ["us-west", "us-west"]);
    let metrics = metrics[0].snapshot();
    assert_eq!(metrics.reader.files_started, 1);
    assert_eq!(metrics.reader.files_completed, 1);
    assert_eq!(metrics.dynamic_filters_received, 1);
    assert_eq!(metrics.dynamic_filters_accepted, 1);
    assert_eq!(metrics.dynamic_filters_unsupported, 0);
    assert_eq!(metrics.dynamic_filter_snapshots, 2);
    assert_eq!(metrics.dynamic_partition_files_pruned, 1);
    assert_eq!(metrics.dynamic_partition_files_kept, 1);
    assert_eq!(metrics.dynamic_files_not_pruned_missing_metadata, 0);
    assert_eq!(metrics.dynamic_files_not_pruned_unsupported_expression, 0);
    Ok(())
}

#[tokio::test]
#[cfg(feature = "native-async")]
async fn dynamic_join_kept_file_still_applies_deletion_vector() -> TestResult {
    let context = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(1)
            .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", true)
            .set_bool(
                "datafusion.optimizer.enable_join_dynamic_filter_pushdown",
                true,
            ),
    );
    register_allowed_regions(&context, vec!["us-west"])?;
    let fixture = RealParquetDeltaTable::new_with_partition_value_and_deletion_vector(
        "provider-dynamic-dv",
        "us-west",
        &[1],
    )?;
    register_fixture(
        &context,
        "orders",
        &fixture,
        DeltaDataFusionScanOptions {
            target_partitions: Some(1),
            ..Default::default()
        },
    )?;

    let plan = context
        .sql(
            "SELECT o.region, o.id \
             FROM allowed_regions r JOIN orders o ON r.region = o.region \
             ORDER BY o.id",
        )
        .await?
        .create_physical_plan()
        .await?;
    let metrics = collect_delta_datafusion_metrics(plan.as_ref());
    assert_eq!(metrics.len(), 1);
    let batches = collect_plan(&context, plan).await?;
    assert_eq!(ids(&batches), [1, 3]);
    assert_eq!(regions(&batches), ["us-west", "us-west"]);

    let metrics = metrics[0].snapshot();
    assert_eq!(metrics.dynamic_filters_received, 1);
    assert_eq!(metrics.dynamic_filters_accepted, 1);
    assert_eq!(metrics.dynamic_partition_files_pruned, 0);
    assert_eq!(metrics.dynamic_partition_files_kept, 1);
    assert_eq!(metrics.reader.deletion_vector_payloads_loaded, 1);
    assert_eq!(metrics.reader.deletion_vectors_applied, 1);
    assert_eq!(metrics.reader.deletion_vector_rows_deleted, 1);
    assert_eq!(metrics.reader.deletion_vector_failures, 0);
    assert_eq!(metrics.reader.deletion_vector_rejections, 0);
    Ok(())
}

#[tokio::test]
#[cfg(feature = "native-async")]
async fn native_exact_filter_applies_before_deletion_vector_masking() -> TestResult {
    let fixture = RealParquetDeltaTable::new_with_two_row_groups_and_deletion_vector(
        "provider-dv-predicate-pruning",
        3,
        &[4],
    )?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy().into_owned()).load()?;
    let provider = DeltaTableProvider::try_new(table, DeltaDataFusionScanOptions::default())?;
    let filter = col("id").gt(lit(3_i32));
    assert_eq!(
        provider.supports_filters_pushdown(&[&filter])?,
        [TableProviderFilterPushDown::Exact]
    );

    let context = SessionContext::new();
    context.register_table("orders", Arc::new(provider))?;
    let plan = context
        .sql("SELECT id FROM orders WHERE id > 3 ORDER BY id")
        .await?
        .create_physical_plan()
        .await?;
    let display = displayable(plan.as_ref()).indent(true).to_string();
    assert!(!display.contains("FilterExec"), "{display}");
    let metrics = collect_delta_datafusion_metrics(plan.as_ref());
    assert_eq!(ids(&collect_plan(&context, plan).await?), [4, 6]);
    let metrics = metrics[0].snapshot().reader;
    assert_eq!(metrics.deletion_vector_payloads_loaded, 1);
    assert_eq!(metrics.deletion_vectors_applied, 1);
    assert_eq!(metrics.deletion_vector_rows_deleted, 1);
    Ok(())
}

#[tokio::test]
#[cfg(feature = "native-async")]
async fn execution_records_batch_size_and_rejects_invalid_partition() -> TestResult {
    let fixture = RealParquetDeltaTable::new_default("provider-execution-options")?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy().into_owned()).load()?;
    let provider = DeltaTableProvider::try_new(
        table,
        DeltaDataFusionScanOptions {
            target_partitions: Some(1),
            ..Default::default()
        },
    )?;
    let context = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(1)
            .with_batch_size(13),
    );
    let plan = provider.scan(&context.state(), None, &[], None).await?;
    let metrics = collect_delta_datafusion_metrics(plan.as_ref());
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0].snapshot().output_batch_size, None);

    let error = plan
        .execute(1, context.task_ctx())
        .err()
        .ok_or("out-of-range partition unexpectedly executed")?;
    let reader = external_reader_error(&error)?;
    assert_eq!(reader.phase(), DeltaReaderPhase::DataFusion);
    assert!(
        reader
            .to_string()
            .contains("reason=scan_partition_index_out_of_range")
    );

    assert_eq!(ids(&collect_plan(&context, plan).await?), [1, 2, 3]);
    let metrics = metrics[0].snapshot();
    assert_eq!(metrics.output_batch_size, Some(13));
    assert_eq!(metrics.reader.scan_partitions_started, 1);
    assert_eq!(metrics.reader.scan_partitions_completed, 1);
    assert_eq!(metrics.reader.files_started, 1);
    assert_eq!(metrics.reader.files_completed, 1);
    Ok(())
}

#[tokio::test]
#[cfg(feature = "native-async")]
async fn empty_scan_has_no_partitions_rows_or_execution_metrics() -> TestResult {
    let fixture = TestTable::empty("provider-empty-scan")?;
    let table = DeltaTableBuilder::new(fixture.uri()).load()?;
    let context = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    let provider = DeltaTableProvider::try_new(table, DeltaDataFusionScanOptions::default())?;
    let plan = provider.scan(&context.state(), None, &[], None).await?;
    assert_eq!(plan.properties().output_partitioning().partition_count(), 0);
    assert!(
        displayable(plan.as_ref())
            .indent(true)
            .to_string()
            .contains("partitions=0")
    );

    let metrics = collect_delta_datafusion_metrics(plan.as_ref());
    assert_eq!(metrics.len(), 1);
    assert!(collect_plan(&context, plan).await?.is_empty());
    let metrics = metrics[0].snapshot().reader;
    assert_eq!(metrics.scan_partitions_planned, 0);
    assert_eq!(metrics.files_planned, 0);
    assert_eq!(metrics.estimated_rows, Some(0));
    assert_eq!(metrics.estimated_bytes, Some(0));
    assert_eq!(metrics.scan_partitions_started, 0);
    assert_eq!(metrics.scan_partitions_completed, 0);
    assert_eq!(metrics.files_started, 0);
    assert_eq!(metrics.files_completed, 0);
    assert_eq!(metrics.batches_produced, 0);
    assert_eq!(metrics.rows_produced, 0);
    assert_eq!(metrics.deletion_vector_payloads_loaded, 0);
    assert_eq!(metrics.deletion_vectors_applied, 0);
    assert_eq!(metrics.deletion_vector_rows_deleted, 0);
    assert_eq!(metrics.deletion_vector_failures, 0);
    assert_eq!(metrics.deletion_vector_rejections, 0);
    Ok(())
}

#[tokio::test]
#[cfg(feature = "native-async")]
async fn execution_error_preserves_reader_source_and_partial_metrics() -> TestResult {
    let fixture = RealParquetDeltaTable::new_default("provider-missing-file")?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy().into_owned()).load()?;
    fs::remove_file(fixture.path().join(fixture.data_file_path()))?;
    let provider = DeltaTableProvider::try_new(
        table,
        DeltaDataFusionScanOptions {
            target_partitions: Some(1),
            ..Default::default()
        },
    )?;
    let context = SessionContext::new();
    let plan = provider.scan(&context.state(), None, &[], None).await?;
    let metrics = collect_delta_datafusion_metrics(plan.as_ref());
    let mut stream = plan.execute(0, context.task_ctx())?;
    let error = stream
        .next()
        .await
        .ok_or("missing file returned no stream item")?
        .expect_err("missing file unexpectedly succeeded");
    let reader = external_reader_error(&error)?;
    assert_eq!(reader.phase(), DeltaReaderPhase::DataFileRead);
    assert_eq!(reader.as_str(), "data_file_read");
    assert!(reader.source().is_some());
    assert!(stream.next().await.is_none());

    let metrics = metrics[0].snapshot();
    assert_eq!(metrics.reader.files_started, 1);
    assert_eq!(metrics.reader.files_completed, 0);
    assert_eq!(metrics.reader.batches_produced, 0);
    assert_eq!(metrics.reader.rows_produced, 0);
    Ok(())
}

#[tokio::test]
#[cfg(feature = "native-async")]
async fn execution_stream_drop_preserves_bounded_partial_metrics() -> TestResult {
    let fixture = RealParquetDeltaTable::new_with_two_large_files("provider-stream-drop", 20_000)?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy().into_owned()).load()?;
    let execution_options = DeltaReaderExecutionOptions::new()
        .with_native_async_prefetch_file_count_per_partition(0)?
        .with_max_concurrent_file_reads_per_partition(1)?
        .with_max_concurrent_file_reads_per_scan(Some(1))?
        .with_output_buffer_capacity_per_partition(1)?;
    let provider = DeltaTableProvider::try_new(
        table,
        DeltaDataFusionScanOptions {
            execution_options,
            target_partitions: Some(1),
        },
    )?;
    let context = SessionContext::new();
    let projection = vec![0];
    let plan = provider
        .scan(&context.state(), Some(&projection), &[], None)
        .await?;
    let metrics = collect_delta_datafusion_metrics(plan.as_ref());
    let mut stream = plan.execute(0, context.task_ctx())?;
    let first = stream.next().await.ok_or("expected first batch")??;
    assert_eq!(ids(std::slice::from_ref(&first)).first().copied(), Some(1));
    drop(stream);

    for _ in 0..1000 {
        if metrics[0].snapshot().reader.batches_produced > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    let metrics = metrics[0].snapshot();
    assert_eq!(metrics.reader.scan_partitions_started, 1);
    assert_eq!(metrics.reader.scan_partitions_completed, 0);
    assert_eq!(metrics.reader.files_started, 1);
    assert_eq!(metrics.reader.files_completed, 0);
    assert!((1..=2).contains(&metrics.reader.batches_produced));
    assert!((1..=16_384).contains(&metrics.reader.rows_produced));
    Ok(())
}

#[tokio::test]
#[cfg(feature = "native-async")]
async fn native_metadata_hint_preserves_rows_and_request_fallback() -> TestResult {
    let fixture =
        RealParquetDeltaTable::new_with_two_large_files("provider-parquet-metadata-hint", 20_000)?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy().into_owned()).load()?;
    let mut outputs = Vec::new();
    let mut requests = Vec::new();

    for hint in [None, Some(64 * 1024), Some(9)] {
        let context =
            SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        let execution_options =
            DeltaReaderExecutionOptions::new().with_parquet_metadata_size_hint(hint)?;
        register_delta_table(
            &context,
            "orders",
            table.clone(),
            DeltaDataFusionScanOptions {
                execution_options,
                target_partitions: Some(1),
            },
        )?;
        let plan = context
            .sql("SELECT count(*) AS row_count, sum(id) AS id_sum FROM orders")
            .await?
            .create_physical_plan()
            .await?;
        let metrics = collect_delta_datafusion_metrics(plan.as_ref());
        let batches = collect_plan(&context, plan).await?;
        let batch = batches.first().ok_or("aggregate returned no batch")?;
        assert_eq!(batch.num_rows(), 1);
        let row_count = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("count was not Int64")?
            .value(0);
        let id_sum = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("sum was not Int64")?
            .value(0);
        outputs.push((row_count, id_sum));
        let snapshot = metrics[0].snapshot().reader;
        assert_eq!(snapshot.files_started, 2);
        requests.push(
            snapshot
                .parquet_data_file_range_get_operations
                .ok_or("missing NativeAsync range GET metric")?,
        );
    }

    assert_eq!(outputs[1], outputs[0]);
    assert_eq!(outputs[2], outputs[0]);
    assert_eq!(outputs[0], (40_000, 800_020_000));
    assert_eq!(requests[0].checked_sub(requests[1]), Some(2));
    assert_eq!(requests[2], requests[0]);
    Ok(())
}

#[tokio::test]
#[cfg(feature = "native-async")]
async fn dynamic_join_pruning_preserves_the_sql_residual() -> TestResult {
    let context = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(1)
            .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", true)
            .set_bool(
                "datafusion.optimizer.enable_join_dynamic_filter_pushdown",
                true,
            ),
    );
    register_allowed_regions(&context, vec!["us-west"])?;
    let fixture =
        RealParquetDeltaTable::new_with_two_partition_values("provider-dynamic-residual")?;
    register_fixture(
        &context,
        "orders",
        &fixture,
        DeltaDataFusionScanOptions {
            target_partitions: Some(1),
            ..Default::default()
        },
    )?;

    let plan = context
        .sql(
            "SELECT o.id, o.customer_name, o.region \
             FROM allowed_regions r JOIN orders o ON r.region = o.region \
             WHERE o.customer_name LIKE 'west-1%' ORDER BY o.id",
        )
        .await?
        .create_physical_plan()
        .await?;
    let display = displayable(plan.as_ref()).indent(true).to_string();
    assert!(display.contains("FilterExec"), "{display}");
    let metrics = collect_delta_datafusion_metrics(plan.as_ref());
    assert_eq!(ids(&collect_plan(&context, plan).await?), [1]);

    let metrics = metrics[0].snapshot();
    assert_eq!(metrics.reader.files_planned, 2);
    assert_eq!(metrics.reader.files_started, 1);
    assert_eq!(metrics.reader.files_completed, 1);
    assert_eq!(metrics.reader.rows_produced, 2);
    assert_eq!(metrics.dynamic_filters_received, 1);
    assert_eq!(metrics.dynamic_filters_accepted, 1);
    assert_eq!(metrics.dynamic_filters_unsupported, 0);
    assert_eq!(metrics.dynamic_partition_files_pruned, 1);
    assert_eq!(metrics.dynamic_partition_files_kept, 1);
    assert_eq!(
        metrics.reader.files_planned,
        metrics
            .reader
            .files_started
            .saturating_add(metrics.dynamic_partition_files_pruned)
    );
    Ok(())
}

#[tokio::test]
#[cfg(all(feature = "native-async", feature = "official-kernel"))]
async fn optimizer_keeps_limit_above_official_kernel_residual() -> TestResult {
    let fixture = RealParquetDeltaTable::new_default("provider-residual-limit")?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy().into_owned()).load()?;
    let execution_options = DeltaReaderExecutionOptions::new()
        .with_reader_backend(DeltaReaderBackend::OfficialKernel)?;
    let context = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    register_delta_table(
        &context,
        "orders",
        table,
        DeltaDataFusionScanOptions {
            execution_options,
            target_partitions: Some(1),
        },
    )?;

    let plan = context
        .sql("SELECT customer_name FROM orders WHERE id > 1 LIMIT 1")
        .await?
        .create_physical_plan()
        .await?;
    let display = displayable(plan.as_ref()).indent(true).to_string();
    assert!(display.contains("fetch=1"), "{display}");
    assert!(display.contains("FilterExec"), "{display}");
    assert!(display.contains("DeltaDataFusionExec"), "{display}");
    let filter = display.find("FilterExec").ok_or("missing FilterExec")?;
    let scan = display
        .find("DeltaDataFusionExec")
        .ok_or("missing DeltaDataFusionExec")?;
    assert!(filter < scan, "{display}");

    let metrics = collect_delta_datafusion_metrics(plan.as_ref());
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0].snapshot().reader.estimated_rows, Some(3));
    let batches = collect_plan(&context, plan).await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!(metrics[0].snapshot().reader.rows_produced, 3);
    Ok(())
}

#[tokio::test]
#[cfg(feature = "native-async")]
async fn joined_delta_scans_keep_distinct_metrics_and_limit_above_join() -> TestResult {
    let fixture = RealParquetDeltaTable::new_default("provider-joined-scans")?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy().into_owned()).load()?;
    let context = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    register_delta_table(
        &context,
        "orders",
        table.clone(),
        DeltaDataFusionScanOptions::default(),
    )?;
    register_delta_table(
        &context,
        "customers",
        table,
        DeltaDataFusionScanOptions::default(),
    )?;

    let star = context.sql("SELECT * FROM orders").await?;
    assert_eq!(star.schema().fields().len(), 2);
    assert_eq!(star.schema().field(0).name(), "id");
    assert_eq!(star.schema().field(0).data_type(), &DataType::Int32);
    assert_eq!(star.schema().field(1).name(), "customer_name");
    assert_eq!(star.schema().field(1).data_type(), &DataType::Utf8);
    let projected = context
        .sql("SELECT customer_name FROM orders")
        .await?
        .into_optimized_plan()?;
    assert_eq!(projected.schema().fields().len(), 1);
    assert_eq!(projected.schema().field(0).name(), "customer_name");
    assert_eq!(projected.schema().field(0).data_type(), &DataType::Utf8);

    let plan = context
        .sql(
            "SELECT orders.id FROM orders \
             JOIN customers ON orders.id = customers.id LIMIT 1",
        )
        .await?
        .create_physical_plan()
        .await?;
    let display = displayable(plan.as_ref()).indent(true).to_string();
    assert!(display.contains("HashJoinExec"), "{display}");
    assert!(display.contains("fetch=1"), "{display}");
    let metrics = collect_delta_datafusion_metrics(plan.as_ref());
    assert_eq!(metrics.len(), 2);
    let mut names = metrics
        .iter()
        .map(|metrics| metrics.source_name().unwrap_or_default())
        .collect::<Vec<_>>();
    names.sort_unstable();
    assert_eq!(names, ["customers", "orders"]);
    assert_eq!(ids(&collect_plan(&context, plan).await?), [1]);
    assert!(
        metrics
            .iter()
            .all(|metrics| metrics.snapshot().reader.files_started == 1)
    );
    Ok(())
}
