use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

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
