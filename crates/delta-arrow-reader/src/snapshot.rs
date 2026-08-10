//! Immutable Delta snapshot loading.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use snafu::ResultExt;

use crate::{
    DeltaProtocolInfo, DeltaReaderError, DeltaSnapshotSelection, DeltaStorageOptions,
    error::{SchemaConversionSnafu, SnapshotLoadSnafu, StorageInitializationSnafu},
    kernel::{DeltaKernelEngineContext, KernelSnapshot, snapshot_arrow_schema},
    uri::normalize_delta_table_uri,
};

const TRACING_TARGET: &str = "delta_arrow_reader";
const SNAPSHOT_LOAD_STARTED_EVENT: &str = "snapshot_load.started";
const SNAPSHOT_LOAD_COMPLETED_EVENT: &str = "snapshot_load.completed";
const SNAPSHOT_LOAD_FAILED_EVENT: &str = "snapshot_load.failed";

#[derive(Clone)]
pub(crate) struct LoadedDeltaTableSnapshot {
    snapshot: KernelSnapshot,
    protocol_info: DeltaProtocolInfo,
    schema: SchemaRef,
    engine_context: Arc<DeltaKernelEngineContext>,
}

#[allow(dead_code)]
impl LoadedDeltaTableSnapshot {
    pub(crate) fn table_uri(&self) -> &str {
        self.engine_context.table_url().as_str()
    }

    pub(crate) fn version(&self) -> u64 {
        self.snapshot.version()
    }

    pub(crate) fn protocol_info(&self) -> &DeltaProtocolInfo {
        &self.protocol_info
    }

    pub(crate) fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    pub(crate) fn schema_ref(&self) -> &SchemaRef {
        &self.schema
    }

    pub(crate) fn engine_context(&self) -> &Arc<DeltaKernelEngineContext> {
        &self.engine_context
    }

    pub(crate) fn kernel_snapshot(&self) -> &KernelSnapshot {
        &self.snapshot
    }
}

pub(crate) fn load_delta_table_snapshot_blocking(
    table_uri: &str,
    storage_options: &DeltaStorageOptions,
    selection: DeltaSnapshotSelection,
) -> Result<LoadedDeltaTableSnapshot, DeltaReaderError> {
    let selection_kind = snapshot_selection_kind(selection);
    trace_snapshot_load_started(selection_kind);
    let result = (|| {
        let table_url = normalize_delta_table_uri(table_uri)?;
        let s3_auth_mode_hint = s3_auth_mode_hint_for_source(&table_url, storage_options);
        let engine_context = DeltaKernelEngineContext::build(table_url, storage_options)
            .boxed()
            .context(StorageInitializationSnafu {
                reason: "storage_initialization_failed",
            })?;
        let engine_context = Arc::new(engine_context);
        let version = match selection {
            DeltaSnapshotSelection::Latest => None,
            DeltaSnapshotSelection::Version(version) => Some(version),
        };
        let snapshot =
            engine_context
                .load_snapshot(version)
                .boxed()
                .context(SnapshotLoadSnafu {
                    reason: snapshot_load_failed_reason(s3_auth_mode_hint),
                })?;
        let protocol_info = DeltaProtocolInfo::from_snapshot(&snapshot);
        let schema = snapshot_arrow_schema(&snapshot)
            .boxed()
            .context(SchemaConversionSnafu {
                reason: "schema_conversion_failed",
            })?;

        Ok(LoadedDeltaTableSnapshot {
            snapshot,
            protocol_info,
            schema,
            engine_context,
        })
    })();

    match &result {
        Ok(snapshot) => trace_snapshot_load_completed(selection_kind, snapshot),
        Err(error) => trace_snapshot_load_failed(selection_kind, error),
    }
    result
}

#[allow(dead_code)]
/// Offloads the one blocking Kernel load to the caller's Tokio runtime.
///
/// Dropping the returned future cancels result delivery. A blocking load that
/// already started may still finish before its owned context is dropped.
pub(crate) async fn load_delta_table_snapshot_async(
    table_uri: String,
    storage_options: DeltaStorageOptions,
    selection: DeltaSnapshotSelection,
) -> Result<LoadedDeltaTableSnapshot, DeltaReaderError> {
    let selection_kind = snapshot_selection_kind(selection);
    let result = tokio::task::spawn_blocking(move || {
        load_delta_table_snapshot_blocking(&table_uri, &storage_options, selection)
    })
    .await
    .boxed()
    .context(SnapshotLoadSnafu {
        reason: "snapshot_load_task_failed",
    });

    match result {
        Ok(result) => result,
        Err(error) => {
            trace_snapshot_load_failed(selection_kind, &error);
            Err(error)
        }
    }
}

const fn snapshot_selection_kind(selection: DeltaSnapshotSelection) -> &'static str {
    match selection {
        DeltaSnapshotSelection::Latest => "latest",
        DeltaSnapshotSelection::Version(_) => "version",
    }
}

fn trace_snapshot_load_started(selection: &'static str) {
    tracing::debug!(
        target: TRACING_TARGET,
        event = SNAPSHOT_LOAD_STARTED_EVENT,
        selection
    );
}

fn trace_snapshot_load_completed(selection: &'static str, snapshot: &LoadedDeltaTableSnapshot) {
    tracing::debug!(
        target: TRACING_TARGET,
        event = SNAPSHOT_LOAD_COMPLETED_EVENT,
        selection,
        snapshot_version = snapshot.version(),
        protocol_reader_version = snapshot.protocol_info().min_reader_version()
    );
}

fn trace_snapshot_load_failed(selection: &'static str, error: &DeltaReaderError) {
    tracing::debug!(
        target: TRACING_TARGET,
        event = SNAPSHOT_LOAD_FAILED_EVENT,
        selection,
        error_variant = error.as_str(),
        error_phase = error.phase().as_str()
    );
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum S3AuthModeHint {
    ExplicitStatic,
    ExplicitWebIdentity,
    ExplicitContainer,
    ImplicitProviderChain,
    OtherExplicit,
}

fn s3_auth_mode_hint_for_source(
    table_url: &url::Url,
    storage_options: &DeltaStorageOptions,
) -> Option<S3AuthModeHint> {
    is_s3_compatible(table_url).then(|| classify_s3_auth_mode(storage_options))
}

fn is_s3_compatible(table_url: &url::Url) -> bool {
    match (table_url.scheme(), table_url.host_str()) {
        ("s3" | "s3a", Some(_)) => true,
        ("https", Some(host)) => {
            let host = host.to_ascii_lowercase();
            host.ends_with("amazonaws.com") || host.ends_with("r2.cloudflarestorage.com")
        }
        _ => false,
    }
}

fn classify_s3_auth_mode(storage_options: &DeltaStorageOptions) -> S3AuthModeHint {
    let mut has_access_key_id = false;
    let mut has_secret_access_key = false;
    let mut has_web_identity_token_file = false;
    let mut has_role_arn = false;
    let mut has_container_credentials_relative_uri = false;
    let mut has_container_credentials_full_uri = false;
    let mut has_container_authorization_token_file = false;
    let mut has_auth_related_option = false;

    for key in storage_options.keys() {
        match key.to_ascii_lowercase().as_str() {
            "aws_access_key_id" | "access_key_id" => {
                has_access_key_id = true;
                has_auth_related_option = true;
            }
            "aws_secret_access_key" | "secret_access_key" => {
                has_secret_access_key = true;
                has_auth_related_option = true;
            }
            "aws_session_token" | "aws_token" | "session_token" | "token" => {
                has_auth_related_option = true;
            }
            "aws_web_identity_token_file" | "web_identity_token_file" => {
                has_web_identity_token_file = true;
                has_auth_related_option = true;
            }
            "aws_role_arn" | "role_arn" => {
                has_role_arn = true;
                has_auth_related_option = true;
            }
            "aws_role_session_name"
            | "role_session_name"
            | "aws_endpoint_url_sts"
            | "endpoint_url_sts" => {
                has_auth_related_option = true;
            }
            "aws_container_credentials_relative_uri" | "container_credentials_relative_uri" => {
                has_container_credentials_relative_uri = true;
                has_auth_related_option = true;
            }
            "aws_container_credentials_full_uri" | "container_credentials_full_uri" => {
                has_container_credentials_full_uri = true;
                has_auth_related_option = true;
            }
            "aws_container_authorization_token_file" | "container_authorization_token_file" => {
                has_container_authorization_token_file = true;
                has_auth_related_option = true;
            }
            "aws_imdsv1_fallback"
            | "imdsv1_fallback"
            | "aws_metadata_endpoint"
            | "metadata_endpoint"
            | "aws_unsigned_payload"
            | "unsigned_payload"
            | "aws_skip_signature"
            | "skip_signature" => {
                has_auth_related_option = true;
            }
            _ => {}
        }
    }

    if has_access_key_id && has_secret_access_key {
        S3AuthModeHint::ExplicitStatic
    } else if has_web_identity_token_file && has_role_arn {
        S3AuthModeHint::ExplicitWebIdentity
    } else if has_container_credentials_relative_uri
        || (has_container_credentials_full_uri && has_container_authorization_token_file)
    {
        S3AuthModeHint::ExplicitContainer
    } else if has_auth_related_option {
        S3AuthModeHint::OtherExplicit
    } else {
        S3AuthModeHint::ImplicitProviderChain
    }
}

fn snapshot_load_failed_reason(s3_auth_mode_hint: Option<S3AuthModeHint>) -> &'static str {
    if s3_auth_mode_hint == Some(S3AuthModeHint::ImplicitProviderChain) {
        "snapshot_load_failed_with_implicit_s3_credentials"
    } else {
        "snapshot_load_failed"
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        error::Error as _,
        fmt, fs,
        path::{Path, PathBuf},
        sync::{Arc, Mutex, Once},
        time::{SystemTime, UNIX_EPOCH},
    };

    use arrow::datatypes::{DataType, TimeUnit};
    use futures_util::future;
    use object_store::{ObjectStoreExt, memory::InMemory, path::Path as ObjectStorePath};
    use tracing::{
        Event, Level, Metadata, Subscriber,
        field::{Field, Visit},
        span::{Attributes, Id, Record},
        subscriber::Interest,
    };

    use super::{
        S3AuthModeHint, TRACING_TARGET, load_delta_table_snapshot_async,
        load_delta_table_snapshot_blocking, s3_auth_mode_hint_for_source,
        snapshot_load_failed_reason,
    };
    use crate::{
        DeltaReaderError, DeltaReaderPhase, DeltaSnapshotSelection, DeltaStorageOptions,
        kernel::{DeltaKernelEngineContext, insert_url_handler, is_kernel_error},
    };

    const PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
    const METADATA_JSON: &str = r#"{"metaData":{"id":"delta-arrow-reader-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
    const SUPPORTED_TYPES_PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["timestampNtz"],"writerFeatures":["timestampNtz"]}}"#;
    const SUPPORTED_TYPES_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-arrow-reader-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"profile\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"age\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"nickname\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}},{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"elementType\":\"integer\",\"containsNull\":false},\"nullable\":true,\"metadata\":{}},{\"name\":\"attributes\",\"type\":{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"long\",\"valueContainsNull\":false},\"nullable\":true,\"metadata\":{}},{\"name\":\"amount\",\"type\":\"decimal(10,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"event_ts\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"event_ts_ntz\",\"type\":\"timestamp_ntz\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
    const COLUMN_MAPPING_PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["columnMapping"],"writerFeatures":["columnMapping"]}}"#;
    const COLUMN_MAPPING_METADATA_JSON: &str = r#"{"metaData":{"id":"delta-arrow-reader-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{\"delta.columnMapping.id\":1,\"delta.columnMapping.physicalName\":\"phys_id\"}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":2,\"delta.columnMapping.physicalName\":\"phys_customer_name\"}}]}","partitionColumns":[],"configuration":{"delta.columnMapping.mode":"name","delta.columnMapping.maxColumnId":"2"},"createdTime":1587968585495}}"#;

    static TRACING_TEST_LOCK: Mutex<()> = Mutex::new(());
    static TRACING_TEST_GLOBAL_SUBSCRIBER: Once = Once::new();

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct CapturedEvent {
        target: &'static str,
        level: Level,
        fields: BTreeMap<String, String>,
    }

    #[derive(Clone, Default)]
    struct EventCollector(Option<Arc<Mutex<Vec<CapturedEvent>>>>);

    impl EventCollector {
        fn capturing() -> Self {
            Self(Some(Arc::new(Mutex::new(Vec::new()))))
        }

        fn events(&self) -> Vec<CapturedEvent> {
            self.0
                .as_ref()
                .and_then(|events| events.lock().ok().map(|events| events.clone()))
                .unwrap_or_default()
        }
    }

    impl Subscriber for EventCollector {
        fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
            if metadata.target() == TRACING_TARGET && *metadata.level() == Level::DEBUG {
                Interest::always()
            } else {
                Interest::sometimes()
            }
        }

        fn enabled(&self, metadata: &Metadata<'_>) -> bool {
            metadata.target() == TRACING_TARGET && *metadata.level() == Level::DEBUG
        }

        fn new_span(&self, _span: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }

        fn record(&self, _span: &Id, _values: &Record<'_>) {}

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

        fn event(&self, event: &Event<'_>) {
            let mut visitor = FieldVisitor::default();
            event.record(&mut visitor);
            if visitor
                .0
                .get("event")
                .is_some_and(|name| name.starts_with("snapshot_load."))
                && let Some(events) = &self.0
                && let Ok(mut events) = events.lock()
            {
                events.push(CapturedEvent {
                    target: event.metadata().target(),
                    level: *event.metadata().level(),
                    fields: visitor.0,
                });
            }
        }

        fn enter(&self, _span: &Id) {}

        fn exit(&self, _span: &Id) {}
    }

    #[derive(Default)]
    struct FieldVisitor(BTreeMap<String, String>);

    impl Visit for FieldVisitor {
        fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
            self.0.insert(field.name().to_owned(), format!("{value:?}"));
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            self.0.insert(field.name().to_owned(), value.to_owned());
        }

        fn record_u64(&mut self, field: &Field, value: u64) {
            self.0.insert(field.name().to_owned(), value.to_string());
        }

        fn record_i64(&mut self, field: &Field, value: i64) {
            self.0.insert(field.name().to_owned(), value.to_string());
        }
    }

    struct DeltaLogTable(PathBuf);

    impl DeltaLogTable {
        fn new(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
            Self::new_with_protocol_and_metadata(name, PROTOCOL_JSON, METADATA_JSON)
        }

        fn new_with_protocol_and_metadata(
            name: &str,
            protocol_json: &str,
            metadata_json: &str,
        ) -> Result<Self, Box<dyn std::error::Error>> {
            let path = Path::new("target")
                .join("delta-arrow-reader-snapshot-tests")
                .join(unique_name(name)?);
            let log_path = path.join("_delta_log");
            fs::create_dir_all(&log_path)?;
            fs::write(
                log_path.join("00000000000000000000.json"),
                format!("{protocol_json}\n{metadata_json}\n"),
            )?;
            fs::write(
                log_path.join("00000000000000000001.json"),
                r#"{"commitInfo":{"timestamp":1587968586000}}"#,
            )?;
            Ok(Self(path))
        }
    }

    impl Drop for DeltaLogTable {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn unique_name(name: &str) -> Result<String, Box<dyn std::error::Error>> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        Ok(format!("{}-{name}-{nanos}", std::process::id()))
    }

    fn storage_options(entries: &[(&str, &str)]) -> DeltaStorageOptions {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    fn empty_delta_table(name: &str) -> Result<DeltaLogTable, Box<dyn std::error::Error>> {
        let path = Path::new("target")
            .join("delta-arrow-reader-snapshot-tests")
            .join(unique_name(name)?);
        fs::create_dir_all(&path)?;
        Ok(DeltaLogTable(path))
    }

    #[test]
    fn loads_latest_and_fixed_snapshots_into_shared_immutable_state()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("blocking")?;
        let table_uri = table.0.to_string_lossy();
        let latest = load_delta_table_snapshot_blocking(
            &table_uri,
            &DeltaStorageOptions::new(),
            DeltaSnapshotSelection::Latest,
        )?;
        let fixed = load_delta_table_snapshot_blocking(
            &table_uri,
            &DeltaStorageOptions::new(),
            DeltaSnapshotSelection::Version(0),
        )?;
        let cloned = latest.clone();

        assert_eq!(latest.version(), 1);
        assert_eq!(fixed.version(), 0);
        assert!(latest.table_uri().starts_with("file://"));
        assert!(Arc::ptr_eq(
            latest.engine_context(),
            cloned.engine_context()
        ));
        assert!(Arc::ptr_eq(
            &latest.engine_context().object_store(),
            &cloned.engine_context().object_store()
        ));
        Ok(())
    }

    #[test]
    fn converts_and_reuses_the_logical_arrow_schema() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_protocol_and_metadata(
            "supported-schema",
            SUPPORTED_TYPES_PROTOCOL_JSON,
            SUPPORTED_TYPES_METADATA_JSON,
        )?;
        let loaded = load_delta_table_snapshot_blocking(
            &table.0.to_string_lossy(),
            &DeltaStorageOptions::new(),
            DeltaSnapshotSelection::Latest,
        )?;
        let schema = loaded.schema();
        let cached = loaded.schema();

        assert!(Arc::ptr_eq(&schema, &cached));
        assert_eq!(
            schema
                .fields()
                .iter()
                .map(|field| field.name())
                .collect::<Vec<_>>(),
            [
                "id",
                "profile",
                "tags",
                "attributes",
                "amount",
                "event_ts",
                "event_ts_ntz",
            ]
        );
        assert!(!schema.field_with_name("id")?.is_nullable());
        assert_eq!(
            schema.field_with_name("amount")?.data_type(),
            &DataType::Decimal128(10, 2)
        );
        assert_eq!(
            schema.field_with_name("event_ts")?.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(
            schema.field_with_name("event_ts_ntz")?.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );

        let DataType::Struct(profile) = schema.field_with_name("profile")?.data_type() else {
            panic!("profile must remain a struct");
        };
        assert_eq!(profile[0].name(), "age");
        assert!(!profile[0].is_nullable());
        assert_eq!(profile[1].name(), "nickname");
        assert!(profile[1].is_nullable());

        let DataType::List(element) = schema.field_with_name("tags")?.data_type() else {
            panic!("tags must remain a list");
        };
        assert_eq!(element.data_type(), &DataType::Int32);
        assert!(!element.is_nullable());

        let DataType::Map(entries, false) = schema.field_with_name("attributes")?.data_type()
        else {
            panic!("attributes must remain a map");
        };
        let DataType::Struct(key_value) = entries.data_type() else {
            panic!("map entries must remain a struct");
        };
        assert_eq!(key_value[0].data_type(), &DataType::Utf8);
        assert_eq!(key_value[1].data_type(), &DataType::Int64);
        assert!(!key_value[1].is_nullable());
        Ok(())
    }

    #[test]
    fn keeps_column_mapping_names_logical_and_metadata_available()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_protocol_and_metadata(
            "column-mapping-schema",
            COLUMN_MAPPING_PROTOCOL_JSON,
            COLUMN_MAPPING_METADATA_JSON,
        )?;
        let loaded = load_delta_table_snapshot_blocking(
            &table.0.to_string_lossy(),
            &DeltaStorageOptions::new(),
            DeltaSnapshotSelection::Latest,
        )?;
        let schema = loaded.schema();

        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "customer_name");
        assert_eq!(
            schema
                .field(0)
                .metadata()
                .get("delta.columnMapping.physicalName")
                .map(String::as_str),
            Some("phys_id")
        );
        assert_eq!(
            schema
                .field(1)
                .metadata()
                .get("delta.columnMapping.physicalName")
                .map(String::as_str),
            Some("phys_customer_name")
        );
        Ok(())
    }

    #[test]
    fn tracing_emits_only_the_bounded_load_fields() -> Result<(), Box<dyn std::error::Error>> {
        let _lock = TRACING_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let table = DeltaLogTable::new("tracing")?;
        let collector = EventCollector::capturing();
        let capture = collector.clone();
        let secret_uri = "ftp://user:password@example.com/table?token=secret";

        TRACING_TEST_GLOBAL_SUBSCRIBER.call_once(|| {
            let _ = tracing::subscriber::set_global_default(EventCollector::default());
        });
        tracing::subscriber::with_default(collector, || {
            tracing::callsite::rebuild_interest_cache();
            load_delta_table_snapshot_blocking(
                &table.0.to_string_lossy(),
                &DeltaStorageOptions::new(),
                DeltaSnapshotSelection::Latest,
            )?;
            assert!(
                load_delta_table_snapshot_blocking(
                    secret_uri,
                    &DeltaStorageOptions::new(),
                    DeltaSnapshotSelection::Version(9),
                )
                .is_err()
            );
            Ok::<_, DeltaReaderError>(())
        })?;
        tracing::callsite::rebuild_interest_cache();

        let events = capture.events();
        assert_eq!(events.len(), 4);
        assert!(events.iter().all(|event| {
            event.target == TRACING_TARGET
                && event.level == Level::DEBUG
                && !format!("{:?}", event.fields).contains("user")
                && !format!("{:?}", event.fields).contains("password")
                && !format!("{:?}", event.fields).contains("token")
                && !format!("{:?}", event.fields).contains("example.com")
        }));
        assert_eq!(
            events[0].fields,
            BTreeMap::from([
                ("event".to_owned(), "snapshot_load.started".to_owned()),
                ("selection".to_owned(), "latest".to_owned()),
            ])
        );
        assert_eq!(
            events[1].fields,
            BTreeMap::from([
                ("event".to_owned(), "snapshot_load.completed".to_owned()),
                ("protocol_reader_version".to_owned(), "1".to_owned()),
                ("selection".to_owned(), "latest".to_owned()),
                ("snapshot_version".to_owned(), "1".to_owned()),
            ])
        );
        assert_eq!(
            events[2].fields,
            BTreeMap::from([
                ("event".to_owned(), "snapshot_load.started".to_owned()),
                ("selection".to_owned(), "version".to_owned()),
            ])
        );
        assert_eq!(
            events[3].fields,
            BTreeMap::from([
                ("error_phase".to_owned(), "storage".to_owned()),
                (
                    "error_variant".to_owned(),
                    "storage_initialization".to_owned(),
                ),
                ("event".to_owned(), "snapshot_load.failed".to_owned()),
                ("selection".to_owned(), "version".to_owned()),
            ])
        );
        Ok(())
    }

    #[test]
    fn corrupt_snapshot_preserves_the_kernel_source_without_dependency_text()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("corrupt")?;
        fs::write(
            table.0.join("_delta_log/00000000000000000001.json"),
            "{not json\n",
        )?;
        let result = load_delta_table_snapshot_blocking(
            &table.0.to_string_lossy(),
            &DeltaStorageOptions::new(),
            DeltaSnapshotSelection::Latest,
        );
        let error = match result {
            Ok(_) => panic!("corrupt snapshot should fail"),
            Err(error) => error,
        };

        assert!(matches!(error, DeltaReaderError::SnapshotLoad { .. }));
        assert_eq!(
            error.to_string(),
            "delta reader error: phase=snapshot error=snapshot_load reason=snapshot_load_failed"
        );
        assert!(is_kernel_error(error.source().expect("Kernel source")));
        Ok(())
    }

    #[test]
    fn snapshot_failures_are_redacted_and_preserve_the_kernel_source()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("missing-version")?;
        let result = load_delta_table_snapshot_blocking(
            &table.0.to_string_lossy(),
            &DeltaStorageOptions::new(),
            DeltaSnapshotSelection::Version(2),
        );
        let error = match result {
            Ok(_) => panic!("missing version should fail"),
            Err(error) => error,
        };

        assert!(matches!(error, DeltaReaderError::SnapshotLoad { .. }));
        assert_eq!(error.phase(), DeltaReaderPhase::Snapshot);
        assert_eq!(error.as_str(), "snapshot_load");
        assert!(!error.to_string().contains(&table.0.to_string_lossy()[..]));
        assert!(is_kernel_error(error.source().expect("Kernel source")));
        Ok(())
    }

    #[test]
    fn empty_and_metadata_free_tables_fail_as_snapshot_loads()
    -> Result<(), Box<dyn std::error::Error>> {
        let empty = empty_delta_table("empty-table")?;
        let metadata_free = DeltaLogTable::new("metadata-free")?;
        fs::write(
            metadata_free.0.join("_delta_log/00000000000000000000.json"),
            r#"{"add":{"path":"part-00000.parquet","partitionValues":{},"size":0,"modificationTime":1587968586000,"dataChange":true}}"#,
        )?;

        for table in [&empty, &metadata_free] {
            let error = match load_delta_table_snapshot_blocking(
                &table.0.to_string_lossy(),
                &DeltaStorageOptions::new(),
                DeltaSnapshotSelection::Latest,
            ) {
                Ok(_) => panic!("invalid table must fail"),
                Err(error) => error,
            };
            assert!(matches!(error, DeltaReaderError::SnapshotLoad { .. }));
            assert_eq!(error.phase(), DeltaReaderPhase::Snapshot);
        }
        Ok(())
    }

    #[test]
    fn forwards_storage_options_to_exactly_one_store_construction()
    -> Result<(), Box<dyn std::error::Error>> {
        let scheme = format!(
            "darstorage{}{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        );
        let captured = Arc::new(Mutex::new(Vec::<DeltaStorageOptions>::new()));
        let handler_capture = Arc::clone(&captured);
        insert_url_handler(
            &scheme,
            Arc::new(move |_url, options| {
                handler_capture
                    .lock()
                    .map_err(|_| object_store::Error::Generic {
                        store: "capture",
                        source: std::io::Error::other("capture lock poisoned").into(),
                    })?
                    .push(options.into_iter().collect());
                Ok((Box::new(InMemory::new()), ObjectStorePath::from("")))
            }),
        )?;
        let options = storage_options(&[
            ("authorization", "secret-source-token"),
            ("region", "us-east-1"),
        ]);

        let error = match load_delta_table_snapshot_blocking(
            &format!("{scheme}://table/root"),
            &options,
            DeltaSnapshotSelection::Latest,
        ) {
            Ok(_) => panic!("empty in-memory store must fail snapshot loading"),
            Err(error) => error,
        };

        assert!(matches!(error, DeltaReaderError::SnapshotLoad { .. }));
        assert_eq!(
            captured
                .lock()
                .map(|options| options.clone())
                .unwrap_or_default(),
            vec![options]
        );
        assert!(!error.to_string().contains("secret-source-token"));
        assert!(!format!("{error:?}").contains("authorization"));
        Ok(())
    }

    #[test]
    fn s3_store_construction_accepts_implicit_and_documented_explicit_credentials()
    -> Result<(), Box<dyn std::error::Error>> {
        let table_url = url::Url::parse("s3://bucket/root")?;
        let _implicit =
            DeltaKernelEngineContext::build(table_url.clone(), &DeltaStorageOptions::new())?;
        for (access_key, secret_key, token_key, region_key) in [
            (
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
                "AWS_REGION",
            ),
            (
                "aws_access_key_id",
                "aws_secret_access_key",
                "aws_session_token",
                "aws_region",
            ),
            (
                "aws_access_key_id",
                "aws_secret_access_key",
                "aws_session_token",
                "region",
            ),
            (
                "aws_access_key_id",
                "aws_secret_access_key",
                "aws_session_token",
                "AWS_DEFAULT_REGION",
            ),
            (
                "aws_access_key_id",
                "aws_secret_access_key",
                "aws_session_token",
                "aws_default_region",
            ),
        ] {
            let options = storage_options(&[
                (access_key, "access"),
                (secret_key, "secret"),
                (token_key, "token"),
                (region_key, "us-east-1"),
            ]);
            let context = DeltaKernelEngineContext::build(table_url.clone(), &options)?;
            assert_eq!(context.table_url(), &table_url);
        }
        Ok(())
    }

    #[test]
    fn successful_load_forwards_options_once_without_retaining_them()
    -> Result<(), Box<dyn std::error::Error>> {
        let scheme = format!(
            "darstorage{}{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        );
        let store = InMemory::new();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        runtime.block_on(
            store.put(
                &ObjectStorePath::from("root/_delta_log/00000000000000000000.json"),
                format!("{PROTOCOL_JSON}\n{METADATA_JSON}\n")
                    .into_bytes()
                    .into(),
            ),
        )?;
        let captured = Arc::new(Mutex::new(Vec::<DeltaStorageOptions>::new()));
        let handler_capture = Arc::clone(&captured);
        insert_url_handler(
            &scheme,
            Arc::new(move |_url, options| {
                handler_capture
                    .lock()
                    .map_err(|_| object_store::Error::Generic {
                        store: "capture",
                        source: std::io::Error::other("capture lock poisoned").into(),
                    })?
                    .push(options.into_iter().collect());
                Ok((Box::new(store.clone()), ObjectStorePath::from("root")))
            }),
        )?;
        let options = storage_options(&[
            ("authorization", "secret-source-token"),
            ("region", "us-east-1"),
        ]);
        let loaded = load_delta_table_snapshot_blocking(
            &format!("{scheme}://table/root"),
            &options,
            DeltaSnapshotSelection::Latest,
        )?;

        assert_eq!(loaded.version(), 0);
        assert_eq!(
            captured
                .lock()
                .map(|options| options.clone())
                .unwrap_or_default(),
            vec![options]
        );
        assert_eq!(loaded.schema().field(0).name(), "id");
        Ok(())
    }

    #[test]
    fn unsupported_store_preserves_its_source_without_uri_disclosure() {
        let result = load_delta_table_snapshot_blocking(
            "ftp://user:password@example.com/table",
            &DeltaStorageOptions::new(),
            DeltaSnapshotSelection::Latest,
        );
        let error = match result {
            Ok(_) => panic!("unsupported store should fail"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            DeltaReaderError::StorageInitialization { .. }
        ));
        assert_eq!(error.phase(), DeltaReaderPhase::Storage);
        assert!(!error.to_string().contains("user"));
        assert!(!error.to_string().contains("password"));
        assert!(!error.to_string().contains("example.com"));
        assert!(is_kernel_error(error.source().expect("Kernel source")));
    }

    #[test]
    fn s3_auth_mode_hint_preserves_the_existing_classification()
    -> Result<(), Box<dyn std::error::Error>> {
        for table_uri in [
            "s3://bucket/table",
            "s3a://bucket/table",
            "https://s3.us-east-1.amazonaws.com/bucket/table",
            "https://bucket.s3.us-east-1.amazonaws.com/table",
            "https://ACCOUNT_ID.r2.cloudflarestorage.com/bucket/table",
        ] {
            assert_eq!(
                s3_auth_mode_hint_for_source(
                    &url::Url::parse(table_uri)?,
                    &DeltaStorageOptions::new()
                ),
                Some(S3AuthModeHint::ImplicitProviderChain),
                "{table_uri}"
            );
        }

        let table_url = url::Url::parse("s3://bucket/table")?;
        let cases = [
            (
                storage_options(&[
                    ("AWS_ACCESS_KEY_ID", "access"),
                    ("AWS_SECRET_ACCESS_KEY", "secret"),
                ]),
                S3AuthModeHint::ExplicitStatic,
            ),
            (
                storage_options(&[
                    ("aws_web_identity_token_file", "/token"),
                    ("aws_role_arn", "arn:aws:iam::123456789012:role/Test"),
                ]),
                S3AuthModeHint::ExplicitWebIdentity,
            ),
            (
                storage_options(&[("aws_container_credentials_relative_uri", "/credentials")]),
                S3AuthModeHint::ExplicitContainer,
            ),
            (
                storage_options(&[
                    ("aws_container_credentials_full_uri", "http://example.com"),
                    ("aws_container_authorization_token_file", "/token"),
                ]),
                S3AuthModeHint::ExplicitContainer,
            ),
            (
                storage_options(&[("aws_container_credentials_full_uri", "http://example.com")]),
                S3AuthModeHint::OtherExplicit,
            ),
            (
                storage_options(&[("AWS_METADATA_ENDPOINT", "http://169.254.169.254")]),
                S3AuthModeHint::OtherExplicit,
            ),
            (
                storage_options(&[("AWS_SESSION_TOKEN", "partial")]),
                S3AuthModeHint::OtherExplicit,
            ),
            (
                storage_options(&[("AWS_REGION", "us-east-1")]),
                S3AuthModeHint::ImplicitProviderChain,
            ),
        ];

        for (options, expected) in cases {
            assert_eq!(
                s3_auth_mode_hint_for_source(&table_url, &options),
                Some(expected)
            );
        }

        assert_eq!(
            s3_auth_mode_hint_for_source(
                &url::Url::parse("https://example.com/table")?,
                &DeltaStorageOptions::new()
            ),
            None
        );
        assert_eq!(
            snapshot_load_failed_reason(Some(S3AuthModeHint::ImplicitProviderChain)),
            "snapshot_load_failed_with_implicit_s3_credentials"
        );
        assert_eq!(
            snapshot_load_failed_reason(Some(S3AuthModeHint::ExplicitStatic)),
            "snapshot_load_failed"
        );
        Ok(())
    }

    #[test]
    fn async_loads_are_independent_and_use_the_same_blocking_path()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("async")?;
        let table_uri = table.0.to_string_lossy().into_owned();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        let (first, second) = runtime.block_on(future::join(
            load_delta_table_snapshot_async(
                table_uri.clone(),
                DeltaStorageOptions::new(),
                DeltaSnapshotSelection::Latest,
            ),
            load_delta_table_snapshot_async(
                table_uri,
                DeltaStorageOptions::new(),
                DeltaSnapshotSelection::Version(0),
            ),
        ));
        let first = first?;
        let second = second?;

        assert_eq!(first.version(), 1);
        assert_eq!(second.version(), 0);
        assert!(!Arc::ptr_eq(
            first.engine_context(),
            second.engine_context()
        ));
        Ok(())
    }
}
