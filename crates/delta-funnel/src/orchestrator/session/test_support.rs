use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use datafusion::{
    arrow::{
        array::{ArrayRef, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    datasource::{MemTable, TableProvider},
};
use futures_util::StreamExt;

use crate::{
    DeltaFunnelError, LoadMode, MssqlConnectionConfig, MssqlOutputBatchStream, MssqlTargetConfig,
    MssqlTargetTable,
};

use super::{LazyTable, MssqlOutputTarget, OutputWritePlan, RunMode};

pub(super) struct DeltaLogTable {
    path: PathBuf,
}

impl Drop for DeltaLogTable {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

impl DeltaLogTable {
    pub(super) fn new(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_and_schema(name, PROTOCOL_JSON, DEFAULT_SCHEMA_FIELDS_JSON)
    }

    pub(super) fn new_with_protocol(
        name: &str,
        protocol_json: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_and_schema(name, protocol_json, DEFAULT_SCHEMA_FIELDS_JSON)
    }

    pub(super) fn new_with_schema(
        name: &str,
        schema_fields_json: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_protocol_and_schema(name, PROTOCOL_JSON, schema_fields_json)
    }

    fn new_with_protocol_and_schema(
        name: &str,
        protocol_json: &str,
        schema_fields_json: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let path = Path::new("target")
            .join("delta-funnel-orchestrator-tests")
            .join(unique_name(name)?);
        let log_path = path.join("_delta_log");
        fs::create_dir_all(&log_path)?;
        fs::write(
            log_path.join("00000000000000000000.json"),
            format!("{}\n{}\n", protocol_json, metadata_json(schema_fields_json)),
        )?;
        fs::write(
            log_path.join("00000000000000000001.json"),
            format!("{}\n", add_json("part-00000.parquet")),
        )?;

        Ok(Self { path })
    }

    pub(super) fn uri(&self) -> String {
        self.path.to_string_lossy().to_string()
    }

    pub(super) fn file_uri_with_secret_parts(&self) -> Result<String, Box<dyn std::error::Error>> {
        let path = fs::canonicalize(&self.path)?;

        Ok(format!(
            "file://{}?token=super-secret#debug-secret",
            path.to_string_lossy()
        ))
    }
}

const PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
const DEFAULT_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;
pub(super) const UNSUPPORTED_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":true},\"nullable\":true,\"metadata\":{}}]"#;

fn metadata_json(schema_fields_json: &str) -> String {
    format!(
        r#"{{"metaData":{{"id":"delta-funnel-test","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{{\"type\":\"struct\",\"fields\":{schema_fields_json}}}","partitionColumns":[],"configuration":{{}},"createdTime":1587968585495}}}}"#
    )
}

fn add_json(path: &str) -> String {
    format!(
        r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":0,"modificationTime":1587968586000,"dataChange":true}}}}"#
    )
}

fn unique_name(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(format!("{}-{name}-{nanos}", std::process::id()))
}

pub(super) fn marker_region_provider(
    marker: &str,
) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("marker", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![marker, marker])) as ArrayRef,
            Arc::new(StringArray::from(vec!["west", "east"])) as ArrayRef,
        ],
    )?;

    Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
}

pub(super) fn secret_connection() -> Result<MssqlConnectionConfig, DeltaFunnelError> {
    Ok(MssqlConnectionConfig::new(
        "server=tcp:sql.example.com;database=warehouse;user=admin;password=secret-token",
    )?
    .with_display_label("warehouse-primary"))
}

pub(super) fn override_connection() -> Result<MssqlConnectionConfig, DeltaFunnelError> {
    Ok(MssqlConnectionConfig::new(
        "server=tcp:override.example.com;database=warehouse;user=writer;password=override-secret",
    )?
    .with_display_label("warehouse-override"))
}

pub(super) fn output_request(
    table: LazyTable,
    output_name: &str,
    target_table: &str,
    load_mode: LoadMode,
) -> Result<OutputWritePlan, DeltaFunnelError> {
    output_request_with_run_mode(table, output_name, target_table, load_mode, RunMode::DryRun)
}

pub(super) fn execute_output_request(
    table: LazyTable,
    output_name: &str,
    target_table: &str,
    load_mode: LoadMode,
) -> Result<OutputWritePlan, DeltaFunnelError> {
    output_request_with_run_mode(
        table,
        output_name,
        target_table,
        load_mode,
        RunMode::Execute,
    )
}

fn output_request_with_run_mode(
    table: LazyTable,
    output_name: &str,
    target_table: &str,
    load_mode: LoadMode,
    run_mode: RunMode,
) -> Result<OutputWritePlan, DeltaFunnelError> {
    let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", target_table)?)
        .with_load_mode(load_mode);
    Ok(OutputWritePlan::new(
        table,
        MssqlOutputTarget::new(output_name, target_config, run_mode),
    ))
}

pub(super) async fn collect_stream_row_count(
    mut stream: MssqlOutputBatchStream,
) -> Result<usize, DeltaFunnelError> {
    let mut rows = 0_usize;

    while let Some(batch) = stream.next().await {
        rows = rows.saturating_add(batch?.num_rows());
    }

    Ok(rows)
}
