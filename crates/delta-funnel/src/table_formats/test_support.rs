//! Test fixtures for Delta Funnel workflows using local Delta tables.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use delta_kernel::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int32Array, ListArray, StringArray, StructArray, TimestampMicrosecondArray,
    TimestampNanosecondArray,
};
use delta_kernel::arrow::datatypes::{DataType, Field, Int32Type, Schema};
use delta_kernel::arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

const PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
const METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-real-parquet-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
const SUPPORTED_TYPES_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-real-parquet-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"active\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}},{\"name\":\"payload\",\"type\":\"binary\",\"nullable\":true,\"metadata\":{}},{\"name\":\"event_date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}},{\"name\":\"event_ts\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"amount\",\"type\":\"decimal(10,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"score_f32\",\"type\":\"float\",\"nullable\":true,\"metadata\":{}},{\"name\":\"score_f64\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"attributes\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"level\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"label\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}},{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"elementType\":\"integer\",\"containsNull\":true},\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
const DATA_FILE: &str = "part-00000.parquet";
const MODIFICATION_TIME_MS: i64 = 1_587_968_586_000;

/// Local Delta fixture used by Delta Funnel product tests.
pub(crate) struct RealParquetDeltaTable {
    path: PathBuf,
    rows: usize,
}

impl Drop for RealParquetDeltaTable {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

impl RealParquetDeltaTable {
    /// Creates a local Delta table with one real Parquet file.
    pub(crate) fn new_default(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_batch(
            name,
            default_batch()?,
            AddStats {
                rows: 3,
                max_id: 3,
                min_customer: "alice".to_owned(),
                max_customer: "bob".to_owned(),
                customer_null_count: 1,
            },
        )
    }

    /// Creates a local Delta table with two real Parquet files.
    pub(crate) fn new_with_two_files(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_file_batches(
            name,
            vec![
                file_batch(1, vec![(1, Some("file-a-1")), (2, Some("file-a-2"))])?,
                file_batch(2, vec![(3, Some("file-b-3")), (4, Some("file-b-4"))])?,
            ],
        )
    }

    /// Creates a local Delta table with two large real Parquet files.
    pub(crate) fn new_with_two_large_files(
        name: &str,
        rows_per_file: usize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        if rows_per_file == 0 {
            return Err("row count must be positive".into());
        }

        Self::new_with_file_batches(
            name,
            vec![
                sequential_file_batch(1, 1, rows_per_file, "file-a")?,
                sequential_file_batch(2, rows_per_file.saturating_add(1), rows_per_file, "file-b")?,
            ],
        )
    }

    /// Creates a local Delta table whose logical timestamp column is stored
    /// with different physical timestamp leaf types across Parquet files.
    pub(crate) fn new_with_mixed_timestamp_physical_types(
        name: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_metadata_file_batches(
            name,
            PROTOCOL_JSON,
            SUPPORTED_TYPES_METADATA_JSON,
            vec![
                RealParquetDataFile {
                    path: DATA_FILE.to_owned(),
                    batches: vec![supported_types_batch()?],
                    stats: AddStats {
                        rows: 3,
                        max_id: 3,
                        min_customer: "alice".to_owned(),
                        max_customer: "bob".to_owned(),
                        customer_null_count: 1,
                    },
                    partition_values_json: "{}".to_owned(),
                },
                RealParquetDataFile {
                    path: "part-00001.parquet".to_owned(),
                    batches: vec![supported_types_batch_with_nanosecond_event_ts(None)?],
                    stats: AddStats {
                        rows: 3,
                        max_id: 6,
                        min_customer: "carol".to_owned(),
                        max_customer: "dylan".to_owned(),
                        customer_null_count: 1,
                    },
                    partition_values_json: "{}".to_owned(),
                },
            ],
        )
    }

    fn new_with_batch(
        name: &str,
        batch: RecordBatch,
        stats: AddStats,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_file_batches(
            name,
            vec![RealParquetDataFile {
                path: DATA_FILE.to_owned(),
                batches: vec![batch],
                stats,
                partition_values_json: "{}".to_owned(),
            }],
        )
    }

    fn new_with_file_batches(
        name: &str,
        files: Vec<RealParquetDataFile>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_metadata_file_batches(name, PROTOCOL_JSON, METADATA_JSON, files)
    }

    fn new_with_protocol_metadata_file_batches(
        name: &str,
        protocol_json: &str,
        metadata_json: &str,
        files: Vec<RealParquetDataFile>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let path = Path::new("target")
            .join("delta-funnel-real-parquet-fixtures")
            .join(unique_name(name)?);
        let log_path = path.join("_delta_log");
        fs::create_dir_all(&log_path)?;

        let mut add_actions = Vec::with_capacity(files.len());
        let mut rows = 0_usize;

        for file in files {
            rows = rows.saturating_add(
                file.batches
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>(),
            );

            let first_batch = file
                .batches
                .first()
                .ok_or("data file must have at least one record batch")?;
            let max_row_group_size = file
                .batches
                .iter()
                .map(RecordBatch::num_rows)
                .min()
                .ok_or("data file must have at least one record batch")?;
            let writer_properties = WriterProperties::builder()
                .set_max_row_group_row_count(Some(max_row_group_size))
                .build();
            let mut writer = ArrowWriter::try_new(
                fs::File::create(path.join(&file.path))?,
                first_batch.schema(),
                Some(writer_properties),
            )?;
            for batch in &file.batches {
                writer.write(batch)?;
            }
            writer.close()?;

            let data_file_size = fs::metadata(path.join(&file.path))?.len();
            add_actions.push(add_json(
                &file.path,
                data_file_size,
                &file.stats,
                &file.partition_values_json,
            ));
        }

        fs::write(
            log_path.join("00000000000000000000.json"),
            format!("{protocol_json}\n{metadata_json}\n"),
        )?;
        fs::write(
            log_path.join("00000000000000000001.json"),
            format!("{}\n", add_actions.join("\n")),
        )?;

        Ok(Self { path, rows })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn rows(&self) -> usize {
        self.rows
    }
}

struct RealParquetDataFile {
    path: String,
    batches: Vec<RecordBatch>,
    stats: AddStats,
    partition_values_json: String,
}

struct AddStats {
    rows: usize,
    max_id: i32,
    min_customer: String,
    max_customer: String,
    customer_null_count: usize,
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("customer_name", DataType::Utf8, true),
    ]))
}

fn supported_types_schema_with_event_ts(event_ts_type: DataType) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("customer_name", DataType::Utf8, true),
        Field::new("active", DataType::Boolean, true),
        Field::new("payload", DataType::Binary, true),
        Field::new("event_date", DataType::Date32, true),
        Field::new("event_ts", event_ts_type, true),
        Field::new("amount", DataType::Decimal128(10, 2), true),
        Field::new("score_f32", DataType::Float32, true),
        Field::new("score_f64", DataType::Float64, true),
        Field::new(
            "attributes",
            DataType::Struct(
                vec![
                    Field::new("level", DataType::Int32, true),
                    Field::new("label", DataType::Utf8, true),
                ]
                .into(),
            ),
            true,
        ),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        ),
    ]))
}

fn default_batch() -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let columns = vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
        Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])) as Arc<dyn Array>,
    ];

    Ok(RecordBatch::try_new(schema(), columns)?)
}

fn supported_types_batch() -> Result<RecordBatch, Box<dyn std::error::Error>> {
    supported_types_batch_with_event_ts(
        [1, 2, 3],
        [Some("alice"), Some("bob"), None],
        Arc::new(
            TimestampMicrosecondArray::from(vec![
                Some(1_704_067_200_000_000),
                Some(1_704_153_600_000_000),
                None,
            ])
            .with_timezone("UTC"),
        ) as ArrayRef,
    )
}

fn supported_types_batch_with_nanosecond_event_ts(
    timezone: Option<&str>,
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let event_ts = match timezone {
        Some(timezone) => Arc::new(
            TimestampNanosecondArray::from(vec![
                Some(1_704_240_000_000_000_000),
                Some(1_704_326_400_000_000_000),
                None,
            ])
            .with_timezone(timezone),
        ) as ArrayRef,
        None => Arc::new(TimestampNanosecondArray::from(vec![
            Some(1_704_240_000_000_000_000),
            Some(1_704_326_400_000_000_000),
            None,
        ])) as ArrayRef,
    };

    supported_types_batch_with_event_ts([4, 5, 6], [Some("carol"), Some("dylan"), None], event_ts)
}

fn supported_types_batch_with_event_ts(
    ids: [i32; 3],
    customer_names: [Option<&str>; 3],
    event_ts: ArrayRef,
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let attributes = StructArray::from(vec![
        (
            Arc::new(Field::new("level", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![Some(1), Some(2), None])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("label", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("low"), Some("high"), None])) as ArrayRef,
        ),
    ]);
    let tags = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(10), Some(20)]),
        Some(vec![Some(30)]),
        None,
    ]);
    let event_ts_type = event_ts.data_type().clone();
    let columns = vec![
        Arc::new(Int32Array::from(ids.to_vec())) as Arc<dyn Array>,
        Arc::new(StringArray::from(
            customer_names
                .into_iter()
                .map(|name| name.map(str::to_owned))
                .collect::<Vec<_>>(),
        )) as Arc<dyn Array>,
        Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])) as Arc<dyn Array>,
        Arc::new(BinaryArray::from(vec![
            Some(b"alpha".as_ref()),
            Some(b"beta".as_ref()),
            None,
        ])) as Arc<dyn Array>,
        Arc::new(Date32Array::from(vec![Some(19_723), Some(19_724), None])) as Arc<dyn Array>,
        event_ts,
        Arc::new(
            Decimal128Array::from(vec![Some(12_345), Some(-6_789), None])
                .with_precision_and_scale(10, 2)?,
        ) as Arc<dyn Array>,
        Arc::new(Float32Array::from(vec![Some(1.25), Some(-2.5), None])) as Arc<dyn Array>,
        Arc::new(Float64Array::from(vec![Some(10.5), Some(-20.25), None])) as Arc<dyn Array>,
        Arc::new(attributes) as Arc<dyn Array>,
        Arc::new(tags) as Arc<dyn Array>,
    ];

    Ok(RecordBatch::try_new(
        supported_types_schema_with_event_ts(event_ts_type),
        columns,
    )?)
}

fn file_batch(
    index: usize,
    rows: Vec<(i32, Option<&str>)>,
) -> Result<RealParquetDataFile, Box<dyn std::error::Error>> {
    let row_count = rows.len();
    let path = format!("part-{index:05}.parquet");
    let ids = rows.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    let names = rows
        .into_iter()
        .map(|(_, name)| name.map(str::to_owned))
        .collect::<Vec<_>>();
    let max_id = ids.iter().copied().max().ok_or("file must have rows")?;
    let min_customer = names
        .iter()
        .flatten()
        .min()
        .ok_or("file must have a non-null customer")?
        .to_string();
    let max_customer = names
        .iter()
        .flatten()
        .max()
        .ok_or("file must have a non-null customer")?
        .to_string();
    let customer_null_count = names.iter().filter(|name| name.is_none()).count();
    let columns = vec![
        Arc::new(Int32Array::from(ids)) as Arc<dyn Array>,
        Arc::new(StringArray::from(names)) as Arc<dyn Array>,
    ];

    Ok(RealParquetDataFile {
        path,
        batches: vec![RecordBatch::try_new(schema(), columns)?],
        stats: AddStats {
            rows: row_count,
            max_id,
            min_customer,
            max_customer,
            customer_null_count,
        },
        partition_values_json: "{}".to_owned(),
    })
}

fn sequential_file_batch(
    index: usize,
    first_id: usize,
    rows: usize,
    customer_name: &str,
) -> Result<RealParquetDataFile, Box<dyn std::error::Error>> {
    let first_id = i32::try_from(first_id)?;
    let row_count = i32::try_from(rows)?;
    let ids = (first_id..first_id + row_count).collect::<Vec<_>>();
    let names = (0..rows)
        .map(|_| Some(customer_name.to_owned()))
        .collect::<Vec<_>>();
    let max_id = ids.iter().copied().max().ok_or("file must have rows")?;
    let columns = vec![
        Arc::new(Int32Array::from(ids)) as Arc<dyn Array>,
        Arc::new(StringArray::from(names)) as Arc<dyn Array>,
    ];

    Ok(RealParquetDataFile {
        path: format!("part-{index:05}.parquet"),
        batches: vec![RecordBatch::try_new(schema(), columns)?],
        stats: AddStats {
            rows,
            max_id,
            min_customer: customer_name.to_owned(),
            max_customer: customer_name.to_owned(),
            customer_null_count: 0,
        },
        partition_values_json: "{}".to_owned(),
    })
}

fn add_json(path: &str, size: u64, stats: &AddStats, partition_values_json: &str) -> String {
    let rows = stats.rows;
    let max_id = stats.max_id;
    let min_customer = &stats.min_customer;
    let max_customer = &stats.max_customer;
    let null_count = stats.customer_null_count;
    format!(
        r#"{{"add":{{"path":"{path}","partitionValues":{partition_values_json},"size":{size},"modificationTime":{MODIFICATION_TIME_MS},"dataChange":true,"stats":"{{\"numRecords\":{rows},\"minValues\":{{\"id\":1,\"customer_name\":\"{min_customer}\"}},\"maxValues\":{{\"id\":{max_id},\"customer_name\":\"{max_customer}\"}},\"nullCount\":{{\"id\":0,\"customer_name\":{null_count}}}}}"}}}}"#
    )
}

fn unique_name(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();

    Ok(format!("{}-{name}-{nanos}", std::process::id()))
}
