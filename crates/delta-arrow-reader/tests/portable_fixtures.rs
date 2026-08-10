mod support;

use std::{error::Error, path::Path};

#[cfg(feature = "native-async")]
use std::{fs, fs::File, sync::Arc};

#[cfg(feature = "native-async")]
use arrow::{
    array::Int32Array, compute::concat_batches, datatypes::SchemaRef, record_batch::RecordBatch,
    util::display::array_value_to_string,
};
#[cfg(feature = "native-async")]
use delta_arrow_reader::{
    DeltaBatchStream, DeltaComparison, DeltaPredicate, DeltaReadMetricsSnapshot,
    DeltaReaderBackend, DeltaReaderError, DeltaReaderExecutionOptions, DeltaReaderPhase,
    DeltaScalar, DeltaTableBuilder,
};
#[cfg(feature = "native-async")]
use futures_util::{StreamExt, TryStreamExt};
#[cfg(feature = "native-async")]
use parquet::file::{reader::FileReader, serialized_reader::SerializedFileReader};

use support::RealParquetDeltaTable;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[cfg(all(feature = "native-async", feature = "official-kernel"))]
struct BackendParityCase {
    name: &'static str,
    fixture: RealParquetDeltaTable,
    projection: &'static [&'static str],
    expected_rows: &'static [&'static str],
    expected_deleted_rows: Option<u64>,
}

#[cfg(all(feature = "native-async", feature = "official-kernel"))]
const COLUMN_MAPPING_ROWS: &[&str] = &["alice\t1", "bob\t2", "\t3"];
#[cfg(all(feature = "native-async", feature = "official-kernel"))]
const PARTITION_ROWS: &[&str] = &["us-west\t1", "us-west\t2", "us-west\t3"];
#[cfg(all(feature = "native-async", feature = "official-kernel"))]
const SUPPORTED_TYPE_ROWS: &[&str] = &[
    "1\talice\ttrue\t616c706861\t2024-01-01\t2024-01-01T00:00:00Z\t123.45\t1.25\t10.5\t{level: 1, label: low}\t[10, 20]",
    "2\tbob\tfalse\t62657461\t2024-01-02\t2024-01-02T00:00:00Z\t-67.89\t-2.5\t-20.25\t{level: 2, label: high}\t[30]",
    "3\t\t\t\t\t\t\t\t\t{level: , label: }\t",
];
#[cfg(all(feature = "native-async", feature = "official-kernel"))]
const TIMESTAMP_ROWS: &[&str] = &[
    "1\talice\t2024-01-01T00:00:00Z",
    "2\tbob\t2024-01-02T00:00:00Z",
    "3\t\t",
    "4\tcarol\t2024-01-03T00:00:00Z",
    "5\tdylan\t2024-01-04T00:00:00Z",
    "6\t\t",
];
#[cfg(all(feature = "native-async", feature = "official-kernel"))]
const NESTED_TIMESTAMP_ROWS: &[&str] = &[
    "1\t{event_ts: 2024-01-01T00:00:00Z}",
    "2\t{event_ts: 2024-01-02T00:00:00Z}",
    "3\t{event_ts: }",
    "4\t{event_ts: 2024-01-03T00:00:00Z}",
    "5\t{event_ts: 2024-01-04T00:00:00Z}",
    "6\t{event_ts: }",
];
#[cfg(all(feature = "native-async", feature = "official-kernel"))]
const NESTED_ROWS: &[&str] = &[
    "{age: 34, first_name: alice}\t1",
    "{age: 41, first_name: bob}\t2",
    "{age: , first_name: }\t3",
];
#[cfg(all(feature = "native-async", feature = "official-kernel"))]
const NESTED_MAPPING_ROWS: &[&str] = &[
    "{first_name: alice, age: 34}\talice\t1",
    "{first_name: bob, age: 41}\tbob\t2",
    "{first_name: , age: }\t\t3",
];
#[cfg(all(feature = "native-async", feature = "official-kernel"))]
const PROJECTED_NESTED_MAPPING_ROWS: &[&str] = &[
    "{first_name: alice, age: 34}",
    "{first_name: bob, age: 41}",
    "{first_name: , age: }",
];
#[cfg(all(feature = "native-async", feature = "official-kernel"))]
const ARRAY_ROWS: &[&str] = &[
    "[{zip: 94110, city: san francisco}, {zip: 10001, city: new york}]\t1",
    "\t2",
    "[{zip: , city: phoenix}]\t3",
];
#[cfg(all(feature = "native-async", feature = "official-kernel"))]
const MISSING_ARRAY_ROWS: &[&str] = &[
    "[{zip: 94110, city: san francisco, country: }, {zip: 10001, city: new york, country: }]\t1",
    "\t2",
    "[{zip: , city: phoenix, country: }]\t3",
];
#[cfg(all(feature = "native-async", feature = "official-kernel"))]
const ARRAY_MAPPING_ROWS: &[&str] = &[
    "[{city: san francisco, zip: 94110}, {city: new york, zip: 10001}]\talice\t1",
    "\tbob\t2",
    "[{city: phoenix, zip: }]\t\t3",
];
#[cfg(all(feature = "native-async", feature = "official-kernel"))]
const MAP_ROWS: &[&str] = &[
    "{home: {zip: 94110, city: san francisco}, work: {zip: 10001, city: new york}}\t1",
    "{}\t2",
    "{mailing: {zip: , city: phoenix}}\t3",
];
#[cfg(all(feature = "native-async", feature = "official-kernel"))]
const MISSING_MAP_ROWS: &[&str] = &[
    "{home: {zip: 94110, city: san francisco, country: }, work: {zip: 10001, city: new york, country: }}\t1",
    "{}\t2",
    "{mailing: {zip: , city: phoenix, country: }}\t3",
];
#[cfg(all(feature = "native-async", feature = "official-kernel"))]
const MAP_KEY_CAST_ROWS: &[&str] = &["{10: home, 20: work}\t1", "{}\t2", "{30: mailing}\t3"];
#[cfg(all(feature = "native-async", feature = "official-kernel"))]
const MAP_MAPPING_ROWS: &[&str] = &[
    "{home: {city: san francisco, zip: 94110}, work: {city: new york, zip: 10001}}\talice\t1",
    "{}\tbob\t2",
    "{mailing: {city: phoenix, zip: }}\t\t3",
];
#[cfg(all(feature = "native-async", feature = "official-kernel"))]
const MAP_KEY_VALUE_MAPPING_ROWS: &[&str] = &[
    "{{city: san francisco, zip: 94110}: {label: home, score: 7}, {city: new york, zip: 10001}: {label: work, score: 8}}\talice\t1",
    "{}\tbob\t2",
    "{{city: phoenix, zip: }: {label: mailing, score: 9}}\t\t3",
];
#[cfg(all(feature = "native-async", feature = "official-kernel"))]
const MAP_LIST_KEY_ROWS: &[&str] = &[
    "{[{zip: 94110, city: san francisco}, {zip: 10001, city: new york}]: home, []: work}\t1",
    "{}\t2",
    "{[{zip: , city: phoenix}]: mailing}\t3",
];
#[cfg(all(feature = "native-async", feature = "official-kernel"))]
const NESTED_MAP_KEY_ROWS: &[&str] = &[
    "{{{zip: 94110, city: san francisco}: 7, {zip: 10001, city: new york}: 8}: home, {}: work}\t1",
    "{}\t2",
    "{{{zip: , city: phoenix}: 9}: mailing}\t3",
];
#[cfg(all(feature = "native-async", feature = "official-kernel"))]
const MISSING_NESTED_ROWS: &[&str] = &[
    "{age: 34, first_name: alice, loyalty_tier: }\t1",
    "{age: 41, first_name: bob, loyalty_tier: }\t2",
    "{age: , first_name: , loyalty_tier: }\t3",
];
#[cfg(all(feature = "native-async", feature = "official-kernel"))]
const MISSING_COLUMN_ROWS: &[&str] = &["1\talice\t", "2\tbob\t", "3\t\t"];
#[cfg(feature = "native-async")]
const DEFAULT_ROWS: &[&str] = &["1\talice", "2\tbob", "3\t"];
#[cfg(all(feature = "native-async", feature = "official-kernel"))]
const DELETION_VECTOR_ROWS: &[&str] = &["1\talice", "3\t"];

#[cfg(all(feature = "native-async", feature = "official-kernel"))]
fn backend_parity_cases() -> TestResult<Vec<BackendParityCase>> {
    let case =
        |name, fixture, projection, expected_rows, expected_deleted_rows| BackendParityCase {
            name,
            fixture,
            projection,
            expected_rows,
            expected_deleted_rows,
        };

    Ok(vec![
        case(
            "column mapping",
            RealParquetDeltaTable::new_with_column_mapping("direct-backend-column-mapping")?,
            &["customer_name", "id"],
            COLUMN_MAPPING_ROWS,
            None,
        ),
        case(
            "partition transform",
            RealParquetDeltaTable::new_with_partition_value(
                "direct-backend-partition-transform",
                "us-west",
            )?,
            &["region", "id"],
            PARTITION_ROWS,
            None,
        ),
        case(
            "supported data types",
            RealParquetDeltaTable::new_with_supported_types("direct-backend-supported-types")?,
            &[
                "id",
                "customer_name",
                "active",
                "payload",
                "event_date",
                "event_ts",
                "amount",
                "score_f32",
                "score_f64",
                "attributes",
                "tags",
            ],
            SUPPORTED_TYPE_ROWS,
            None,
        ),
        case(
            "mixed timestamp physical types",
            RealParquetDeltaTable::new_with_mixed_timestamp_physical_types(
                "direct-backend-mixed-timestamps",
            )?,
            &["id", "customer_name", "event_ts"],
            TIMESTAMP_ROWS,
            None,
        ),
        case(
            "mixed UTC timestamp physical types",
            RealParquetDeltaTable::new_with_mixed_timestamp_physical_types_with_utc_nanoseconds(
                "direct-backend-mixed-utc-timestamps",
            )?,
            &["id", "customer_name", "event_ts"],
            TIMESTAMP_ROWS,
            None,
        ),
        case(
            "mixed nested timestamp physical types",
            RealParquetDeltaTable::new_with_mixed_nested_timestamp_physical_types(
                "direct-backend-mixed-nested-timestamps",
            )?,
            &["id", "profile"],
            NESTED_TIMESTAMP_ROWS,
            None,
        ),
        case(
            "nested struct name fallback",
            RealParquetDeltaTable::new_with_reordered_nested_struct_fields(
                "direct-backend-reordered-nested-struct",
            )?,
            &["profile", "id"],
            NESTED_ROWS,
            None,
        ),
        case(
            "nested column mapping",
            RealParquetDeltaTable::new_with_nested_column_mapping(
                "direct-backend-nested-column-mapping",
            )?,
            &["profile", "customer_name", "id"],
            NESTED_MAPPING_ROWS,
            None,
        ),
        case(
            "projected nested column mapping",
            RealParquetDeltaTable::new_with_nested_column_mapping(
                "direct-backend-projected-nested-column-mapping",
            )?,
            &["profile"],
            PROJECTED_NESTED_MAPPING_ROWS,
            None,
        ),
        case(
            "array struct name fallback",
            RealParquetDeltaTable::new_with_reordered_array_struct_fields(
                "direct-backend-reordered-array-struct",
            )?,
            &["addresses", "id"],
            ARRAY_ROWS,
            None,
        ),
        case(
            "missing nullable array struct field",
            RealParquetDeltaTable::new_with_missing_nullable_array_struct_field(
                "direct-backend-missing-nullable-array-field",
            )?,
            &["addresses", "id"],
            MISSING_ARRAY_ROWS,
            None,
        ),
        case(
            "array column mapping",
            RealParquetDeltaTable::new_with_array_column_mapping(
                "direct-backend-array-column-mapping",
            )?,
            &["addresses", "customer_name", "id"],
            ARRAY_MAPPING_ROWS,
            None,
        ),
        case(
            "array struct leaf cast",
            RealParquetDeltaTable::new_with_array_struct_long_zip_leaf_cast(
                "direct-backend-array-leaf-cast",
            )?,
            &["addresses", "id"],
            ARRAY_ROWS,
            None,
        ),
        case(
            "map value struct name fallback",
            RealParquetDeltaTable::new_with_reordered_map_value_struct_fields(
                "direct-backend-reordered-map-value",
            )?,
            &["attributes", "id"],
            MAP_ROWS,
            None,
        ),
        case(
            "missing nullable map value struct field",
            RealParquetDeltaTable::new_with_missing_nullable_map_value_struct_field(
                "direct-backend-missing-nullable-map-field",
            )?,
            &["attributes", "id"],
            MISSING_MAP_ROWS,
            None,
        ),
        case(
            "map key leaf cast",
            RealParquetDeltaTable::new_with_map_long_key_leaf_cast(
                "direct-backend-map-key-leaf-cast",
            )?,
            &["attributes", "id"],
            MAP_KEY_CAST_ROWS,
            None,
        ),
        case(
            "map column mapping",
            RealParquetDeltaTable::new_with_map_column_mapping(
                "direct-backend-map-column-mapping",
            )?,
            &["attributes", "customer_name", "id"],
            MAP_MAPPING_ROWS,
            None,
        ),
        case(
            "map key and value column mapping",
            RealParquetDeltaTable::new_with_map_key_value_column_mapping(
                "direct-backend-map-key-value-column-mapping",
            )?,
            &["attributes", "customer_name", "id"],
            MAP_KEY_VALUE_MAPPING_ROWS,
            None,
        ),
        case(
            "map list key struct name fallback",
            RealParquetDeltaTable::new_with_reordered_map_list_key_struct_fields(
                "direct-backend-reordered-map-list-key",
            )?,
            &["attributes", "id"],
            MAP_LIST_KEY_ROWS,
            None,
        ),
        case(
            "nested map key struct name fallback",
            RealParquetDeltaTable::new_with_reordered_nested_map_key_struct_fields(
                "direct-backend-reordered-nested-map-key",
            )?,
            &["attributes", "id"],
            NESTED_MAP_KEY_ROWS,
            None,
        ),
        case(
            "missing nullable nested struct field",
            RealParquetDeltaTable::new_with_missing_nullable_nested_struct_field(
                "direct-backend-missing-nullable-nested-field",
            )?,
            &["profile", "id"],
            MISSING_NESTED_ROWS,
            None,
        ),
        case(
            "missing nullable columns",
            RealParquetDeltaTable::new_with_missing_nullable_column(
                "direct-backend-missing-nullable-column",
            )?,
            &["id", "customer_name", "loyalty_tier"],
            MISSING_COLUMN_ROWS,
            None,
        ),
        case(
            "name fallback reordering",
            RealParquetDeltaTable::new_with_reordered_physical_columns(
                "direct-backend-reordered-physical-columns",
            )?,
            &["id", "customer_name"],
            DEFAULT_ROWS,
            None,
        ),
        case(
            "simple deletion vector",
            RealParquetDeltaTable::new_with_deletion_vector(
                "direct-backend-simple-deletion-vector",
                &[1],
            )?,
            &["id", "customer_name"],
            DELETION_VECTOR_ROWS,
            Some(1),
        ),
    ])
}

#[test]
fn every_frozen_real_parquet_fixture_is_portable() -> TestResult {
    let fixtures = [
        RealParquetDeltaTable::new_default("portable-default")?,
        RealParquetDeltaTable::new_with_rows("portable-rows", 5)?,
        RealParquetDeltaTable::new_with_rows_and_deletion_vector("portable-rows-dv", 5, &[1, 3])?,
        RealParquetDeltaTable::new_with_two_row_groups_and_deletion_vector(
            "portable-row-groups-dv",
            3,
            &[1, 4],
        )?,
        RealParquetDeltaTable::new_with_two_files("portable-two-files")?,
        RealParquetDeltaTable::new_with_two_files_and_deletion_vector(
            "portable-two-files-dv",
            &[1],
        )?,
        RealParquetDeltaTable::new_with_two_large_files("portable-large-files", 3)?,
        RealParquetDeltaTable::new_with_deletion_vector("portable-dv", &[1])?,
        RealParquetDeltaTable::new_with_partition_value("portable-partition", "us-west")?,
        RealParquetDeltaTable::new_with_two_partition_values("portable-two-partitions")?,
        RealParquetDeltaTable::new_with_null_partition_value("portable-null-partition")?,
        RealParquetDeltaTable::new_with_two_partition_columns("portable-two-columns")?,
        RealParquetDeltaTable::new_with_partition_value_and_deletion_vector(
            "portable-partition-dv",
            "us-west",
            &[1],
        )?,
        RealParquetDeltaTable::new_with_column_mapping("portable-column-mapping")?,
        RealParquetDeltaTable::new_with_supported_types("portable-supported-types")?,
        RealParquetDeltaTable::new_with_mixed_timestamp_physical_types(
            "portable-mixed-timestamps",
        )?,
        RealParquetDeltaTable::new_with_mixed_timestamp_physical_types_with_utc_nanoseconds(
            "portable-mixed-timestamps-utc",
        )?,
        RealParquetDeltaTable::new_with_mixed_nested_timestamp_physical_types(
            "portable-nested-timestamps",
        )?,
        RealParquetDeltaTable::new_with_reordered_nested_struct_fields(
            "portable-reordered-struct",
        )?,
        RealParquetDeltaTable::new_with_missing_nullable_nested_struct_field(
            "portable-missing-nullable-struct",
        )?,
        RealParquetDeltaTable::new_with_missing_non_nullable_nested_struct_field(
            "portable-missing-required-struct",
        )?,
        RealParquetDeltaTable::new_with_nested_column_mapping("portable-nested-column-mapping")?,
        RealParquetDeltaTable::new_with_reordered_array_struct_fields("portable-reordered-array")?,
        RealParquetDeltaTable::new_with_missing_nullable_array_struct_field(
            "portable-missing-nullable-array",
        )?,
        RealParquetDeltaTable::new_with_missing_non_nullable_array_struct_field(
            "portable-missing-required-array",
        )?,
        RealParquetDeltaTable::new_with_array_column_mapping("portable-array-mapping")?,
        RealParquetDeltaTable::new_with_array_struct_long_zip_leaf_cast(
            "portable-array-leaf-cast",
        )?,
        RealParquetDeltaTable::new_with_reordered_map_value_struct_fields(
            "portable-reordered-map-value",
        )?,
        RealParquetDeltaTable::new_with_missing_nullable_map_value_struct_field(
            "portable-missing-nullable-map-value",
        )?,
        RealParquetDeltaTable::new_with_missing_non_nullable_map_value_struct_field(
            "portable-missing-required-map-value",
        )?,
        RealParquetDeltaTable::new_with_map_column_mapping("portable-map-mapping")?,
        RealParquetDeltaTable::new_with_map_key_value_column_mapping(
            "portable-map-key-value-mapping",
        )?,
        RealParquetDeltaTable::new_with_map_long_key_leaf_cast("portable-map-key-cast")?,
        RealParquetDeltaTable::new_with_reordered_map_list_key_struct_fields(
            "portable-reordered-map-list-key",
        )?,
        RealParquetDeltaTable::new_with_reordered_nested_map_key_struct_fields(
            "portable-reordered-nested-map-key",
        )?,
        RealParquetDeltaTable::new_with_missing_nullable_column(
            "portable-missing-nullable-column",
        )?,
        RealParquetDeltaTable::new_with_missing_non_nullable_column(
            "portable-missing-required-column",
        )?,
        RealParquetDeltaTable::new_with_reordered_physical_columns("portable-reordered-columns")?,
    ];

    assert_eq!(fixtures.len(), 38);
    for fixture in fixtures {
        assert!(
            fixture
                .path()
                .join("_delta_log/00000000000000000000.json")
                .is_file()
        );
        assert!(
            fixture
                .path()
                .join("_delta_log/00000000000000000001.json")
                .is_file()
        );
        assert!(fixture.rows() > 0);
        assert!(fixture.data_file_size() > 0);
        assert!(!fixture.data_file_path().is_empty());
        assert!(fixture.path().starts_with(Path::new("target")));
    }
    Ok(())
}

#[cfg(feature = "native-async")]
enum ScanAttempt {
    Success {
        batch: RecordBatch,
        batch_count: usize,
        logical_schema: SchemaRef,
        metrics: DeltaReadMetricsSnapshot,
    },
    Failure {
        error: DeltaReaderError,
        metrics: DeltaReadMetricsSnapshot,
    },
}

#[cfg(feature = "native-async")]
fn runtime() -> TestResult<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?)
}

#[cfg(feature = "native-async")]
fn native_options(capacity: usize, prefetch: usize) -> TestResult<DeltaReaderExecutionOptions> {
    Ok(DeltaReaderExecutionOptions::new()
        .with_native_async_prefetch_file_count_per_partition(0)?
        .with_max_concurrent_file_reads_per_partition(capacity)?
        .with_max_concurrent_file_reads_per_scan(Some(capacity))?
        .with_output_buffer_capacity_per_partition(1)?
        .with_native_async_prefetch_file_count_per_partition(prefetch)?)
}

#[cfg(feature = "native-async")]
fn missing_data_file_fixture(
    name: &str,
    advertised_size: u64,
) -> TestResult<RealParquetDeltaTable> {
    let fixture = RealParquetDeltaTable::new_default(name)?;
    let add_path = fixture.path().join("_delta_log/00000000000000000001.json");
    let add = fs::read_to_string(&add_path)?;
    let old_size = format!(r#""size":{}"#, fixture.data_file_size());
    let new_size = format!(r#""size":{advertised_size}"#);
    let updated = add.replace(&old_size, &new_size);
    if updated == add {
        return Err("fixture add size was not found".into());
    }
    fs::write(add_path, updated)?;
    fs::remove_file(fixture.path().join(fixture.data_file_path()))?;
    Ok(fixture)
}

#[cfg(feature = "native-async")]
fn projection(names: &[&str]) -> Option<Vec<String>> {
    Some(names.iter().map(|name| (*name).to_owned()).collect())
}

#[cfg(feature = "native-async")]
async fn native_stream(
    fixture: &RealParquetDeltaTable,
    options: DeltaReaderExecutionOptions,
) -> TestResult<DeltaBatchStream> {
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy().into_owned())
        .with_execution_options(options)
        .load_async()
        .await?;
    let scan = table
        .scan()
        .with_projection(vec!["id".to_owned()])
        .with_target_partitions(1)?
        .build()
        .await?;
    assert_eq!(scan.partition_count(), 1);
    Ok(scan.execute().await?)
}

#[cfg(feature = "native-async")]
async fn scan_fixture(
    fixture: &RealParquetDeltaTable,
    backend: DeltaReaderBackend,
    projection: Option<Vec<String>>,
    predicate: Option<DeltaPredicate>,
) -> TestResult<ScanAttempt> {
    let options = DeltaReaderExecutionOptions::new().with_reader_backend(backend)?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy().into_owned())
        .with_execution_options(options)
        .load_async()
        .await?;
    let scan = table.scan().with_target_partitions(1)?;
    let scan = match projection {
        Some(projection) => scan.with_projection(projection),
        None => scan,
    };
    let scan = match predicate {
        Some(predicate) => scan.with_predicate(predicate),
        None => scan,
    }
    .build()
    .await?;
    let logical_schema = Arc::clone(scan.schema());
    let stream = scan.execute().await?;
    let metrics = stream.metrics();

    match stream.try_collect::<Vec<_>>().await {
        Ok(batches) => {
            let batch_count = batches.len();
            let batch = if batches.is_empty() {
                RecordBatch::new_empty(Arc::clone(&logical_schema))
            } else {
                concat_batches(&logical_schema, &batches)?
            };
            Ok(ScanAttempt::Success {
                batch,
                batch_count,
                logical_schema,
                metrics: metrics.snapshot(),
            })
        }
        Err(error) => Ok(ScanAttempt::Failure {
            error,
            metrics: metrics.snapshot(),
        }),
    }
}

#[cfg(feature = "native-async")]
fn batch_ids(batch: &RecordBatch) -> TestResult<Vec<i32>> {
    let index = batch.schema().index_of("id")?;
    let ids = batch
        .column(index)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or("id column must be Int32")?;
    Ok(ids.values().to_vec())
}

#[cfg(feature = "native-async")]
fn batch_rows_ordered_by_id(batch: &RecordBatch) -> TestResult<Vec<String>> {
    let mut rows = (0..batch.num_rows()).collect::<Vec<_>>();
    if let Ok(id_index) = batch.schema().index_of("id") {
        let ids = batch
            .column(id_index)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("id column must be Int32")?;
        rows.sort_unstable_by_key(|row| ids.value(*row));
    }

    rows.into_iter()
        .map(|row| {
            batch
                .columns()
                .iter()
                .map(|column| array_value_to_string(column.as_ref(), row))
                .collect::<Result<Vec<_>, _>>()
                .map(|values| values.join("\t"))
                .map_err(Into::into)
        })
        .collect()
}

#[cfg(feature = "native-async")]
fn assert_success(
    case_name: &str,
    backend: DeltaReaderBackend,
    attempt: ScanAttempt,
    expected_ids: &[i32],
) -> TestResult<(RecordBatch, DeltaReadMetricsSnapshot, usize)> {
    let (batch, batch_count, logical_schema, metrics) = match attempt {
        ScanAttempt::Success {
            batch,
            batch_count,
            logical_schema,
            metrics,
        } => (batch, batch_count, logical_schema, metrics),
        ScanAttempt::Failure { error, .. } => {
            let source = error.source().map(ToString::to_string);
            return Err(format!(
                "{case_name}: {backend:?} unexpectedly failed: {error}; source={source:?}"
            )
            .into());
        }
    };

    assert_eq!(
        batch.schema().fields(),
        logical_schema.fields(),
        "{case_name}"
    );
    assert_eq!(batch_ids(&batch)?, expected_ids, "{case_name}");
    assert_eq!(metrics.reader_backend, backend, "{case_name}");
    assert!(metrics.files_started > 0, "{case_name}");
    assert_eq!(
        metrics.files_completed, metrics.files_started,
        "{case_name}"
    );
    assert_eq!(metrics.scan_partitions_started, 1, "{case_name}");
    assert_eq!(metrics.scan_partitions_completed, 1, "{case_name}");
    Ok((batch, metrics, batch_count))
}

#[cfg(feature = "native-async")]
fn assert_missing_required(
    case_name: &str,
    expected_path: &str,
    attempt: ScanAttempt,
) -> TestResult {
    let ScanAttempt::Failure { error, metrics } = attempt else {
        return Err(format!("{case_name}: missing required field unexpectedly succeeded").into());
    };

    assert_eq!(error.phase(), DeltaReaderPhase::DataFileRead, "{case_name}");
    assert_eq!(error.as_str(), "data_file_read", "{case_name}");
    let display = error.to_string();
    assert!(
        display.contains("reason=parquet_schema_match_failed"),
        "{case_name}: {display}"
    );
    assert!(!display.contains(expected_path), "{case_name}");
    let mut source = error.source();
    let mut source_display = String::new();
    while let Some(current) = source {
        source_display.push_str(&current.to_string());
        source_display.push('\n');
        source = current.source();
    }
    assert!(
        source_display.contains("non-nullable provider field"),
        "{case_name}: {source_display}"
    );
    assert!(
        source_display.contains(expected_path),
        "{case_name}: missing {expected_path}: {source_display}"
    );
    assert!(
        source_display.contains("is missing from the Parquet file"),
        "{case_name}: {source_display}"
    );
    assert_eq!(metrics.reader_backend, DeltaReaderBackend::NativeAsync);
    assert_eq!(metrics.files_started, 1, "{case_name}");
    assert_eq!(metrics.files_completed, 0, "{case_name}");
    assert_eq!(metrics.batches_produced, 0, "{case_name}");
    assert_eq!(metrics.rows_produced, 0, "{case_name}");
    Ok(())
}

#[cfg(feature = "native-async")]
#[test]
fn native_missing_required_fields_preserve_errors_and_metrics() -> TestResult {
    struct Case {
        name: &'static str,
        fixture: RealParquetDeltaTable,
        path: &'static str,
        projection: &'static [&'static str],
    }

    runtime()?.block_on(async {
        let cases = vec![
            Case {
                name: "missing required array struct field",
                fixture: RealParquetDeltaTable::new_with_missing_non_nullable_array_struct_field(
                    "direct-native-missing-required-array",
                )?,
                path: "addresses.element.required_country",
                projection: &["addresses", "id"],
            },
            Case {
                name: "missing required map value struct field",
                fixture:
                    RealParquetDeltaTable::new_with_missing_non_nullable_map_value_struct_field(
                        "direct-native-missing-required-map-value",
                    )?,
                path: "attributes.value.required_country",
                projection: &["attributes", "id"],
            },
            Case {
                name: "missing required nested struct field",
                fixture: RealParquetDeltaTable::new_with_missing_non_nullable_nested_struct_field(
                    "direct-native-missing-required-nested",
                )?,
                path: "profile.required_code",
                projection: &["profile", "id"],
            },
            Case {
                name: "missing required column",
                fixture: RealParquetDeltaTable::new_with_missing_non_nullable_column(
                    "direct-native-missing-required-column",
                )?,
                path: "required_code",
                projection: &["id", "customer_name", "required_code"],
            },
        ];

        for case in cases {
            let attempt = scan_fixture(
                &case.fixture,
                DeltaReaderBackend::NativeAsync,
                projection(case.projection),
                None,
            )
            .await?;
            assert_missing_required(case.name, case.path, attempt)?;
        }
        Ok::<_, Box<dyn Error>>(())
    })
}

#[cfg(feature = "native-async")]
#[test]
fn native_exact_predicates_preserve_deletion_vector_row_indexes() -> TestResult {
    runtime()?.block_on(async {
        let fixture =
            RealParquetDeltaTable::new_with_deletion_vector("direct-native-dv-predicate", &[1])?;

        for (name, value, expected, rows_produced, rows_deleted) in [
            ("only deleted row", 2, Vec::new(), 0, 1),
            ("live row", 1, vec![1], 1, 0),
        ] {
            let predicate = DeltaPredicate::Compare {
                column: "id".into(),
                op: DeltaComparison::Eq,
                value: DeltaScalar::Int32(value),
            };
            let (_, metrics, _) = assert_success(
                name,
                DeltaReaderBackend::NativeAsync,
                scan_fixture(
                    &fixture,
                    DeltaReaderBackend::NativeAsync,
                    Some(vec!["id".to_owned()]),
                    Some(predicate),
                )
                .await?,
                &expected,
            )?;
            assert_eq!(metrics.rows_produced, rows_produced, "{name}");
            assert_eq!(metrics.deletion_vector_payloads_loaded, 1, "{name}");
            assert_eq!(metrics.deletion_vectors_applied, 1, "{name}");
            assert_eq!(metrics.deletion_vector_rows_deleted, rows_deleted, "{name}");
            assert_eq!(metrics.deletion_vector_failures, 0, "{name}");
            assert_eq!(metrics.deletion_vector_rejections, 0, "{name}");
        }

        let no_rows = scan_fixture(
            &fixture,
            DeltaReaderBackend::NativeAsync,
            Some(vec!["id".to_owned()]),
            Some(DeltaPredicate::Compare {
                column: "id".into(),
                op: DeltaComparison::Gt,
                value: DeltaScalar::Int32(99),
            }),
        )
        .await?;
        let ScanAttempt::Success { batch, metrics, .. } = no_rows else {
            return Err("predicate selecting no rows unexpectedly failed".into());
        };
        assert!(batch_ids(&batch)?.is_empty());
        assert_eq!(metrics.rows_produced, 0);
        assert_eq!(metrics.deletion_vector_failures, 0);
        assert_eq!(metrics.deletion_vector_rejections, 0);
        Ok::<_, Box<dyn Error>>(())
    })
}

#[cfg(feature = "native-async")]
#[test]
fn native_partial_pruning_predicate_remains_residual_only() -> TestResult {
    runtime()?.block_on(async {
        let fixture = RealParquetDeltaTable::new_with_supported_types(
            "direct-native-partial-pruning-predicate",
        )?;
        let predicate = DeltaPredicate::And(vec![
            DeltaPredicate::Compare {
                column: "id".into(),
                op: DeltaComparison::Gt,
                value: DeltaScalar::Int32(1),
            },
            DeltaPredicate::Compare {
                column: "score_f64".into(),
                op: DeltaComparison::NotEq,
                value: DeltaScalar::Float64(0.0),
            },
        ]);
        let (_, metrics, _) = assert_success(
            "partial pruning predicate",
            DeltaReaderBackend::NativeAsync,
            scan_fixture(
                &fixture,
                DeltaReaderBackend::NativeAsync,
                Some(vec!["id".to_owned()]),
                Some(predicate),
            )
            .await?,
            &[2],
        )?;

        assert_eq!(metrics.rows_produced, 3);
        Ok::<_, Box<dyn Error>>(())
    })
}

#[cfg(feature = "native-async")]
#[test]
fn native_deletion_vector_boundaries_preserve_rows_schema_and_metrics() -> TestResult {
    struct Case {
        name: &'static str,
        fixture: RealParquetDeltaTable,
        rows: i32,
        deleted: Vec<i32>,
        projection: &'static [&'static str],
        require_multiple_batches: bool,
        require_multiple_row_groups: bool,
        compare_official: bool,
        expected_batch_rows: Option<&'static [&'static str]>,
    }

    runtime()?.block_on(async {
        let cases = vec![
            Case {
                name: "multiple batches",
                fixture: RealParquetDeltaTable::new_with_rows_and_deletion_vector(
                    "direct-native-dv-multiple-batches",
                    9_000,
                    &[8_191, 8_192, 8_999],
                )?,
                rows: 9_000,
                deleted: vec![8_191, 8_192, 8_999],
                projection: &["id"],
                require_multiple_batches: true,
                require_multiple_row_groups: false,
                compare_official: true,
                expected_batch_rows: None,
            },
            Case {
                name: "multiple row groups",
                fixture: RealParquetDeltaTable::new_with_two_row_groups_and_deletion_vector(
                    "direct-native-dv-row-groups",
                    3_000,
                    &[2_999, 3_000, 5_999],
                )?,
                rows: 6_000,
                deleted: vec![2_999, 3_000, 5_999],
                projection: &["id"],
                require_multiple_batches: false,
                require_multiple_row_groups: true,
                compare_official: true,
                expected_batch_rows: None,
            },
            Case {
                name: "sparse indexes",
                fixture: RealParquetDeltaTable::new_with_rows_and_deletion_vector(
                    "direct-native-dv-sparse",
                    40_000,
                    &[0, 19_999, 39_999],
                )?,
                rows: 40_000,
                deleted: vec![0, 19_999, 39_999],
                projection: &["id"],
                require_multiple_batches: false,
                require_multiple_row_groups: false,
                compare_official: true,
                expected_batch_rows: None,
            },
            Case {
                name: "all rows live",
                fixture: RealParquetDeltaTable::new_with_deletion_vector(
                    "direct-native-dv-all-live",
                    &[],
                )?,
                rows: 3,
                deleted: Vec::new(),
                projection: &["id", "customer_name"],
                require_multiple_batches: false,
                require_multiple_row_groups: false,
                compare_official: true,
                expected_batch_rows: Some(DEFAULT_ROWS),
            },
            Case {
                name: "all rows deleted",
                fixture: RealParquetDeltaTable::new_with_deletion_vector(
                    "direct-native-dv-all-deleted",
                    &[0, 1, 2],
                )?,
                rows: 3,
                deleted: vec![0, 1, 2],
                projection: &["id", "customer_name"],
                require_multiple_batches: false,
                require_multiple_row_groups: false,
                compare_official: false,
                expected_batch_rows: Some(&[]),
            },
        ];

        for case in cases {
            if case.require_multiple_row_groups {
                let parquet = SerializedFileReader::new(File::open(
                    case.fixture.path().join(case.fixture.data_file_path()),
                )?)?;
                assert!(parquet.metadata().num_row_groups() > 1, "{}", case.name);
            }
            let expected = (0..case.rows)
                .filter(|index| !case.deleted.contains(index))
                .map(|index| index + 1)
                .collect::<Vec<_>>();
            let (batch, metrics, batch_count) = assert_success(
                case.name,
                DeltaReaderBackend::NativeAsync,
                scan_fixture(
                    &case.fixture,
                    DeltaReaderBackend::NativeAsync,
                    projection(case.projection),
                    None,
                )
                .await?,
                &expected,
            )?;

            assert_eq!(
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect::<Vec<_>>(),
                case.projection,
                "{}",
                case.name
            );
            assert_eq!(
                metrics.rows_produced,
                u64::try_from(expected.len())?,
                "{}",
                case.name
            );
            assert_eq!(metrics.deletion_vector_payloads_loaded, 1, "{}", case.name);
            assert_eq!(metrics.deletion_vectors_applied, 1, "{}", case.name);
            assert_eq!(
                metrics.deletion_vector_rows_deleted,
                u64::try_from(case.deleted.len())?,
                "{}",
                case.name
            );
            assert_eq!(metrics.deletion_vector_failures, 0, "{}", case.name);
            assert_eq!(metrics.deletion_vector_rejections, 0, "{}", case.name);
            if let Some(expected_batch_rows) = case.expected_batch_rows {
                assert_eq!(
                    batch_rows_ordered_by_id(&batch)?,
                    expected_batch_rows,
                    "{}",
                    case.name
                );
            }
            if case.require_multiple_batches {
                assert!(batch_count > 1, "{}", case.name);
                assert_eq!(metrics.batches_produced, u64::try_from(batch_count)?);
            }

            #[cfg(feature = "official-kernel")]
            if case.compare_official {
                let (official, _, _) = assert_success(
                    case.name,
                    DeltaReaderBackend::OfficialKernel,
                    scan_fixture(
                        &case.fixture,
                        DeltaReaderBackend::OfficialKernel,
                        projection(case.projection),
                        None,
                    )
                    .await?,
                    &expected,
                )?;
                assert_eq!(official, batch, "{}", case.name);
            }

            #[cfg(not(feature = "official-kernel"))]
            let _ = case.compare_official;
        }
        Ok::<_, Box<dyn Error>>(())
    })
}

#[cfg(feature = "native-async")]
#[test]
fn native_deletion_vector_payload_error_is_redacted_and_metered() -> TestResult {
    const RELATIVE_DV_FILE: &str = "deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin";

    runtime()?.block_on(async {
        let fixture = RealParquetDeltaTable::new_with_deletion_vector(
            "direct-native-dv-payload-error",
            &[1],
        )?;
        fs::remove_file(fixture.path().join(RELATIVE_DV_FILE))?;
        let ScanAttempt::Failure { error, metrics } = scan_fixture(
            &fixture,
            DeltaReaderBackend::NativeAsync,
            projection(&["id"]),
            None,
        )
        .await?
        else {
            return Err("missing deletion-vector payload unexpectedly succeeded".into());
        };

        assert_eq!(error.phase(), DeltaReaderPhase::DeletionVector);
        assert_eq!(error.as_str(), "deletion_vector_read");
        let display = error.to_string();
        assert!(
            display.contains("reason=deletion_vector_payload_read_failed"),
            "{display}"
        );
        assert!(!display.contains(RELATIVE_DV_FILE));
        assert_eq!(metrics.reader_backend, DeltaReaderBackend::NativeAsync);
        assert_eq!(metrics.files_started, 1);
        assert_eq!(metrics.files_completed, 0);
        assert_eq!(metrics.batches_produced, 0);
        assert_eq!(metrics.rows_produced, 0);
        assert_eq!(metrics.deletion_vector_payloads_loaded, 0);
        assert_eq!(metrics.deletion_vectors_applied, 0);
        assert_eq!(metrics.deletion_vector_rows_deleted, 0);
        assert_eq!(metrics.deletion_vector_failures, 1);
        assert_eq!(metrics.deletion_vector_rejections, 0);
        Ok::<_, Box<dyn Error>>(())
    })
}

#[cfg(feature = "native-async")]
#[test]
fn native_preserves_frozen_file_and_batch_order() -> TestResult {
    runtime()?.block_on(async {
        let cases = [
            (
                "two files",
                RealParquetDeltaTable::new_with_two_files("direct-native-file-order")?,
                (1..=4).collect::<Vec<_>>(),
                2,
                false,
            ),
            (
                "multiple batches",
                RealParquetDeltaTable::new_with_rows("direct-native-multiple-batch-order", 9_000)?,
                (1..=9_000).collect::<Vec<_>>(),
                1,
                true,
            ),
        ];

        for (name, fixture, expected, expected_files, require_multiple_batches) in cases {
            let (batch, metrics, batch_count) = assert_success(
                name,
                DeltaReaderBackend::NativeAsync,
                scan_fixture(
                    &fixture,
                    DeltaReaderBackend::NativeAsync,
                    projection(&["id"]),
                    None,
                )
                .await?,
                &expected,
            )?;

            assert_eq!(batch_ids(&batch)?, expected, "{name}");
            assert_eq!(metrics.files_started, expected_files, "{name}");
            assert_eq!(metrics.files_completed, expected_files, "{name}");
            assert_eq!(
                metrics.rows_produced,
                u64::try_from(expected.len())?,
                "{name}"
            );
            if require_multiple_batches {
                assert!(batch_count > 1, "{name}");
                assert_eq!(metrics.batches_produced, u64::try_from(batch_count)?);
            }
        }
        Ok::<_, Box<dyn Error>>(())
    })
}

#[cfg(feature = "native-async")]
#[test]
fn native_missing_file_preserves_read_error_and_metrics() -> TestResult {
    runtime()?.block_on(async {
        let fixture = missing_data_file_fixture("direct-native-missing-file", 123)?;
        let relative_path = fixture.data_file_path().to_owned();

        let mut stream = native_stream(&fixture, native_options(1, 0)?).await?;
        let metrics = stream.metrics();
        let error = stream
            .next()
            .await
            .ok_or("missing file produced no stream item")?
            .expect_err("missing file unexpectedly succeeded");

        assert_eq!(error.phase(), DeltaReaderPhase::DataFileRead);
        assert_eq!(error.as_str(), "data_file_read");
        assert!(
            error
                .to_string()
                .contains("reason=parquet_read_setup_failed")
        );
        assert!(!error.to_string().contains(&relative_path));
        assert!(error.source().is_some());
        assert!(stream.next().await.is_none());
        let metrics = metrics.snapshot();
        assert_eq!(metrics.files_started, 1);
        assert_eq!(metrics.files_completed, 0);
        assert_eq!(metrics.reader_backend, DeltaReaderBackend::NativeAsync);
        assert_eq!(metrics.parquet_data_file_opened_bytes, Some(123));
        assert!(
            metrics
                .parquet_data_file_range_get_operations
                .is_some_and(|operations| operations > 0)
        );
        assert_eq!(metrics.parquet_data_file_full_get_operations, Some(0));
        assert_eq!(metrics.parquet_data_file_bytes_received, Some(0));
        assert_eq!(metrics.batches_produced, 0);
        assert_eq!(metrics.rows_produced, 0);
        Ok::<_, Box<dyn Error>>(())
    })
}

#[cfg(feature = "native-async")]
#[test]
fn native_stream_drop_stops_future_file_scheduling() -> TestResult {
    runtime()?.block_on(async {
        let fixture = RealParquetDeltaTable::new_with_two_large_files(
            "direct-native-drop-scheduling",
            20_000,
        )?;
        let mut stream = native_stream(&fixture, native_options(1, 0)?).await?;
        let metrics = stream.metrics();
        let first = stream.next().await.ok_or("expected first batch")??;

        assert_eq!(batch_ids(&first)?.first().copied(), Some(1));
        drop(stream);
        tokio::task::yield_now().await;

        let metrics = metrics.snapshot();
        assert_eq!(metrics.reader_backend, DeltaReaderBackend::NativeAsync);
        assert_eq!(metrics.scan_partitions_started, 1);
        assert_eq!(metrics.scan_partitions_completed, 0);
        assert_eq!(metrics.files_started, 1);
        assert_eq!(metrics.files_completed, 0);
        assert!((1..=2).contains(&metrics.batches_produced));
        assert!((1..=16_384).contains(&metrics.rows_produced));
        Ok::<_, Box<dyn Error>>(())
    })
}

#[cfg(feature = "native-async")]
#[test]
fn native_deletion_vector_stream_drop_preserves_partial_metrics() -> TestResult {
    runtime()?.block_on(async {
        let fixture = RealParquetDeltaTable::new_with_rows_and_deletion_vector(
            "direct-native-dv-drop-partial-progress",
            20_000,
            &[0, 8_191, 8_192, 19_999],
        )?;
        let mut stream = native_stream(&fixture, native_options(1, 0)?).await?;
        let metrics = stream.metrics();
        let first = stream.next().await.ok_or("expected first batch")??;
        let first_ids = batch_ids(&first)?;

        assert_eq!(first_ids.first().copied(), Some(2));
        assert!(!first_ids.contains(&1));
        assert!(!first_ids.contains(&8_192));
        drop(stream);
        tokio::task::yield_now().await;

        let metrics = metrics.snapshot();
        assert_eq!(metrics.reader_backend, DeltaReaderBackend::NativeAsync);
        assert_eq!(metrics.scan_partitions_started, 1);
        assert_eq!(metrics.scan_partitions_completed, 0);
        assert_eq!(metrics.files_started, 1);
        assert_eq!(metrics.files_completed, 0);
        assert!((1..=2).contains(&metrics.batches_produced));
        assert!((1..=16_384).contains(&metrics.rows_produced));
        assert_eq!(metrics.deletion_vector_payloads_loaded, 1);
        assert_eq!(metrics.deletion_vectors_applied, 1);
        assert!((1..=3).contains(&metrics.deletion_vector_rows_deleted));
        assert_eq!(metrics.deletion_vector_failures, 0);
        assert_eq!(metrics.deletion_vector_rejections, 0);
        Ok::<_, Box<dyn Error>>(())
    })
}

#[cfg(feature = "native-async")]
#[test]
fn native_backpressure_bounds_future_file_scheduling() -> TestResult {
    runtime()?.block_on(async {
        let fixture = RealParquetDeltaTable::new_with_two_large_files(
            "direct-native-backpressure-scheduling",
            20_000,
        )?;
        let mut stream = native_stream(&fixture, native_options(1, 0)?).await?;
        let metrics = stream.metrics();
        let first = stream.next().await.ok_or("expected first batch")??;
        let mut ids = batch_ids(&first)?;
        let partial = metrics.snapshot();

        assert_eq!(ids.first().copied(), Some(1));
        assert_eq!(partial.files_started, 1);
        assert_eq!(partial.files_completed, 0);
        assert_eq!(partial.scan_partitions_completed, 0);

        for batch in stream.try_collect::<Vec<_>>().await? {
            ids.extend(batch_ids(&batch)?);
        }
        assert_eq!(ids, (1..=40_000).collect::<Vec<_>>());
        let complete = metrics.snapshot();
        assert_eq!(complete.reader_backend, DeltaReaderBackend::NativeAsync);
        assert_eq!(complete.scan_partitions_completed, 1);
        assert_eq!(complete.files_started, 2);
        assert_eq!(complete.files_completed, 2);
        assert_eq!(complete.rows_produced, 40_000);
        Ok::<_, Box<dyn Error>>(())
    })
}

#[cfg(feature = "native-async")]
#[test]
fn native_prefetch_preserves_file_order_and_completes() -> TestResult {
    runtime()?.block_on(async {
        let fixture =
            RealParquetDeltaTable::new_with_two_large_files("direct-native-prefetch-order", 9_000)?;
        let options = native_options(2, 1)?;
        assert_eq!(options.native_async_prefetch_file_count_per_partition(), 1);
        let stream = native_stream(&fixture, options).await?;
        let metrics = stream.metrics();
        let batches = stream.try_collect::<Vec<_>>().await?;
        let mut ids = Vec::new();
        for batch in &batches {
            ids.extend(batch_ids(batch)?);
        }

        assert_eq!(ids, (1..=18_000).collect::<Vec<_>>());
        let metrics = metrics.snapshot();
        assert_eq!(metrics.reader_backend, DeltaReaderBackend::NativeAsync);
        assert_eq!(metrics.scan_partitions_completed, 1);
        assert_eq!(metrics.files_started, 2);
        assert_eq!(metrics.files_completed, 2);
        assert_eq!(
            metrics.parquet_data_file_opened_bytes,
            metrics.estimated_bytes
        );
        assert_eq!(metrics.rows_produced, 18_000);
        Ok::<_, Box<dyn Error>>(())
    })
}

#[cfg(all(feature = "native-async", feature = "official-kernel"))]
fn assert_parity_success(
    case_name: &str,
    projected_columns: &[&str],
    expected_rows: &[&str],
    backend: DeltaReaderBackend,
    attempt: ScanAttempt,
) -> TestResult<(RecordBatch, DeltaReadMetricsSnapshot)> {
    let ScanAttempt::Success {
        batch,
        logical_schema,
        metrics,
        ..
    } = attempt
    else {
        return Err(format!("{case_name}: {backend:?} unexpectedly failed").into());
    };

    assert_eq!(
        batch.schema().fields(),
        logical_schema.fields(),
        "{case_name}"
    );
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        projected_columns,
        "{case_name}"
    );
    assert_eq!(
        batch_rows_ordered_by_id(&batch)?,
        expected_rows,
        "{case_name}"
    );
    assert_eq!(metrics.reader_backend, backend, "{case_name}");
    assert!(metrics.files_started > 0, "{case_name}");
    assert_eq!(
        metrics.files_completed, metrics.files_started,
        "{case_name}"
    );
    assert_eq!(metrics.scan_partitions_started, 1, "{case_name}");
    assert_eq!(metrics.scan_partitions_completed, 1, "{case_name}");
    Ok((batch, metrics))
}

#[cfg(all(feature = "native-async", feature = "official-kernel"))]
#[test]
fn official_kernel_matches_native_for_frozen_cases() -> TestResult {
    runtime()?.block_on(async {
        let cases = backend_parity_cases()?;
        assert_eq!(cases.len(), 24);

        for case in cases {
            let native = scan_fixture(
                &case.fixture,
                DeltaReaderBackend::NativeAsync,
                projection(case.projection),
                None,
            )
            .await?;
            let official = scan_fixture(
                &case.fixture,
                DeltaReaderBackend::OfficialKernel,
                projection(case.projection),
                None,
            )
            .await?;
            let (native, native_metrics) = assert_parity_success(
                case.name,
                case.projection,
                case.expected_rows,
                DeltaReaderBackend::NativeAsync,
                native,
            )?;
            let (official, _) = assert_parity_success(
                case.name,
                case.projection,
                case.expected_rows,
                DeltaReaderBackend::OfficialKernel,
                official,
            )?;

            assert_eq!(official, native, "{}", case.name);
            if let Some(deleted_rows) = case.expected_deleted_rows {
                assert_eq!(native_metrics.deletion_vector_payloads_loaded, 1);
                assert_eq!(native_metrics.deletion_vectors_applied, 1);
                assert_eq!(native_metrics.deletion_vector_rows_deleted, deleted_rows);
                assert_eq!(native_metrics.deletion_vector_failures, 0);
                assert_eq!(native_metrics.deletion_vector_rejections, 0);
            }
        }

        Ok::<_, Box<dyn Error>>(())
    })
}
