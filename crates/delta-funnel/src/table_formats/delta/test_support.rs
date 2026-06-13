//! Test fixtures for local Delta tables with real Parquet data files.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use delta_kernel::Engine as _;
use delta_kernel::arrow::array::{Array, Int32Array, StringArray};
use delta_kernel::arrow::datatypes::{DataType, Field, Schema};

use super::kernel;
use super::uri::normalize_delta_table_uri;

const PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
const METADATA_JSON: &str = r#"{"metaData":{"id":"delta-funnel-real-parquet-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
const DATA_FILE: &str = "part-00000.parquet";
const MODIFICATION_TIME_MS: i64 = 1_587_968_586_000;

/// Local Delta fixture with one real Parquet data file.
pub(crate) struct RealParquetDeltaTable {
    path: PathBuf,
    rows: usize,
    data_file_size: u64,
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

    /// Creates a local Delta table whose single Parquet file has sequential ids.
    pub(crate) fn new_with_rows(
        name: &str,
        rows: usize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        if rows == 0 {
            return Err("row count must be positive".into());
        }

        Self::new_with_batch(
            name,
            sequential_batch(rows)?,
            AddStats {
                rows,
                max_id: i32::try_from(rows)?,
                min_customer: "customer-1".to_owned(),
                max_customer: format!("customer-{rows}"),
                customer_null_count: 0,
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

    fn new_with_batch(
        name: &str,
        batch: kernel::RecordBatch,
        stats: AddStats,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_file_batches(
            name,
            vec![RealParquetDataFile {
                path: DATA_FILE.to_owned(),
                batch,
                stats,
            }],
        )
    }

    fn new_with_file_batches(
        name: &str,
        files: Vec<RealParquetDataFile>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let path = Path::new("target")
            .join("delta-funnel-real-parquet-fixtures")
            .join(unique_name(name)?);
        let log_path = path.join("_delta_log");
        fs::create_dir_all(&log_path)?;

        let table_uri = normalize_delta_table_uri(path.to_string_lossy())?;
        let table_url = kernel::try_parse_uri(&table_uri)?;
        let store = kernel::store_from_url_opts(&table_url, std::iter::empty::<(&str, &str)>())?;
        let engine = kernel::DefaultEngineBuilder::new(store).build();
        let mut add_actions = Vec::with_capacity(files.len());
        let mut rows = 0_usize;
        let mut total_data_file_size = 0_u64;

        for file in files {
            rows = rows.saturating_add(file.batch.num_rows());

            let data_url = table_url.join(&file.path)?;
            let engine_data: Box<dyn delta_kernel::EngineData> =
                Box::new(kernel::ArrowEngineData::new(file.batch));

            engine
                .parquet_handler()
                .write_parquet_file(data_url, Box::new(std::iter::once(Ok(engine_data))))?;

            let data_file_size = fs::metadata(path.join(&file.path))?.len();
            total_data_file_size = total_data_file_size.saturating_add(data_file_size);
            add_actions.push(add_json(&file.path, data_file_size, &file.stats));
        }

        fs::write(
            log_path.join("00000000000000000000.json"),
            format!("{PROTOCOL_JSON}\n{METADATA_JSON}\n"),
        )?;
        fs::write(
            log_path.join("00000000000000000001.json"),
            format!("{}\n", add_actions.join("\n")),
        )?;

        Ok(Self {
            path,
            rows,
            data_file_size: total_data_file_size,
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn data_file_path(&self) -> &'static str {
        DATA_FILE
    }

    pub(crate) fn data_file_size(&self) -> u64 {
        self.data_file_size
    }

    pub(crate) fn rows(&self) -> usize {
        self.rows
    }
}

struct RealParquetDataFile {
    path: String,
    batch: kernel::RecordBatch,
    stats: AddStats,
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

fn default_batch() -> Result<kernel::RecordBatch, Box<dyn std::error::Error>> {
    let columns = vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
        Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])) as Arc<dyn Array>,
    ];

    Ok(kernel::RecordBatch::try_new(schema(), columns)?)
}

fn sequential_batch(rows: usize) -> Result<kernel::RecordBatch, Box<dyn std::error::Error>> {
    let row_count = i32::try_from(rows)?;
    let ids = (1..=row_count).collect::<Vec<_>>();
    let names = (1..=row_count)
        .map(|id| Some(format!("customer-{id}")))
        .collect::<Vec<_>>();
    let columns = vec![
        Arc::new(Int32Array::from(ids)) as Arc<dyn Array>,
        Arc::new(StringArray::from(names)) as Arc<dyn Array>,
    ];

    Ok(kernel::RecordBatch::try_new(schema(), columns)?)
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
        batch: kernel::RecordBatch::try_new(schema(), columns)?,
        stats: AddStats {
            rows: row_count,
            max_id,
            min_customer,
            max_customer,
            customer_null_count,
        },
    })
}

fn add_json(path: &str, size: u64, stats: &AddStats) -> String {
    let rows = stats.rows;
    let max_id = stats.max_id;
    let min_customer = &stats.min_customer;
    let max_customer = &stats.max_customer;
    let null_count = stats.customer_null_count;
    format!(
        r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":{size},"modificationTime":{MODIFICATION_TIME_MS},"dataChange":true,"stats":"{{\"numRecords\":{rows},\"minValues\":{{\"id\":1,\"customer_name\":\"{min_customer}\"}},\"maxValues\":{{\"id\":{max_id},\"customer_name\":\"{max_customer}\"}},\"nullCount\":{{\"id\":0,\"customer_name\":{null_count}}}}}"}}}}"#
    )
}

fn unique_name(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();

    Ok(format!("{}-{name}-{nanos}", std::process::id()))
}
