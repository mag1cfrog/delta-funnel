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
        let path = Path::new("target")
            .join("delta-funnel-real-parquet-fixtures")
            .join(unique_name(name)?);
        let log_path = path.join("_delta_log");
        fs::create_dir_all(&log_path)?;

        let batch = default_batch()?;
        let rows = batch.num_rows();
        let table_uri = normalize_delta_table_uri(path.to_string_lossy())?;
        let table_url = kernel::try_parse_uri(&table_uri)?;
        let store = kernel::store_from_url_opts(&table_url, std::iter::empty::<(&str, &str)>())?;
        let engine = kernel::DefaultEngineBuilder::new(store).build();
        let data_url = table_url.join(DATA_FILE)?;
        let engine_data: Box<dyn delta_kernel::EngineData> =
            Box::new(kernel::ArrowEngineData::new(batch));

        engine
            .parquet_handler()
            .write_parquet_file(data_url, Box::new(std::iter::once(Ok(engine_data))))?;

        let data_file_size = fs::metadata(path.join(DATA_FILE))?.len();

        fs::write(
            log_path.join("00000000000000000000.json"),
            format!("{PROTOCOL_JSON}\n{METADATA_JSON}\n"),
        )?;
        fs::write(
            log_path.join("00000000000000000001.json"),
            format!("{}\n", add_json(DATA_FILE, data_file_size)),
        )?;

        Ok(Self {
            path,
            rows,
            data_file_size,
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

fn default_batch() -> Result<kernel::RecordBatch, Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("customer_name", DataType::Utf8, true),
    ]));
    let columns = vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
        Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])) as Arc<dyn Array>,
    ];

    Ok(kernel::RecordBatch::try_new(schema, columns)?)
}

fn add_json(path: &str, size: u64) -> String {
    format!(
        r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":{size},"modificationTime":{MODIFICATION_TIME_MS},"dataChange":true,"stats":"{{\"numRecords\":3,\"minValues\":{{\"id\":1,\"customer_name\":\"alice\"}},\"maxValues\":{{\"id\":3,\"customer_name\":\"bob\"}},\"nullCount\":{{\"id\":0,\"customer_name\":1}}}}"}}}}"#
    )
}

fn unique_name(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();

    Ok(format!("{}-{name}-{nanos}", std::process::id()))
}
