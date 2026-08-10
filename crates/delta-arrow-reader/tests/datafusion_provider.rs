#![cfg(all(
    feature = "datafusion",
    any(feature = "native-async", feature = "official-kernel")
))]

use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow::{
    array::{Int32Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::{
    datasource::{TableProvider, TableType},
    logical_expr::{TableProviderFilterPushDown, col, lit},
    physical_plan::ExecutionPlan,
    prelude::{SessionConfig, SessionContext},
};
#[cfg(feature = "official-kernel")]
use delta_arrow_reader::DeltaReaderBackend;
use delta_arrow_reader::{
    DeltaDataFusionScanOptions, DeltaReaderExecutionOptions, DeltaReaderPhase, DeltaTableBuilder,
    DeltaTableProvider, collect_delta_datafusion_metrics, register_delta_table,
};
use parquet::arrow::ArrowWriter;
use serde_json::{Value, json};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

struct TestTable(PathBuf);

impl TestTable {
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
        table,
        DeltaDataFusionScanOptions {
            target_partitions: Some(0),
            ..Default::default()
        },
    )
    .expect_err("zero target must fail");
    assert_eq!(zero_target.phase(), DeltaReaderPhase::Configuration);

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
