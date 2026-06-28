use serde_json::{Value, json};

use crate::{
    DeltaProviderReaderBackend, DeltaSourceReport, FileCount, LazyTableKind, LoadMode,
    MssqlDryRunOutputFieldReport, MssqlDryRunOutputReport, MssqlDryRunSqlIdentityReport,
    MssqlDryRunWorkflowReport, MssqlTargetTable, OutputStatus, PhaseStatus, PhaseTimingReport,
    ReportReasonCode, RowCount, RunMode, ValidationStatus, WorkflowStatus,
};

impl RowCount {
    /// Returns a JSON-compatible shape that preserves count kind and value.
    #[must_use]
    pub fn to_json_value(self) -> Value {
        count_value(self.kind().as_str(), self.value())
    }
}

impl FileCount {
    /// Returns a JSON-compatible shape that preserves count kind and value.
    #[must_use]
    pub fn to_json_value(self) -> Value {
        count_value(self.kind().as_str(), self.value())
    }
}

impl ValidationStatus {
    /// Returns a JSON-compatible shape that preserves status kind and reason.
    #[must_use]
    pub fn to_json_value(self) -> Value {
        status_value(self.kind().as_str(), self.reason())
    }
}

impl PhaseStatus {
    /// Returns a JSON-compatible shape that preserves phase status kind and reason.
    #[must_use]
    pub fn to_json_value(self) -> Value {
        status_value(self.kind().as_str(), self.reason())
    }
}

impl OutputStatus {
    /// Returns a JSON-compatible shape that preserves output status semantics.
    #[must_use]
    pub fn to_json_value(self) -> Value {
        json!({
            "kind": self.kind().as_str(),
            "reason": reason_value(self.reason()),
            "validation": self.validation().map(ValidationStatus::to_json_value),
        })
    }
}

impl WorkflowStatus {
    /// Returns a JSON-compatible shape that preserves workflow status semantics.
    #[must_use]
    pub fn to_json_value(self) -> Value {
        status_value(self.kind().as_str(), self.reason())
    }
}

impl PhaseTimingReport {
    /// Returns a JSON-compatible shape with structured status and elapsed time.
    #[must_use]
    pub fn to_json_value(&self) -> Value {
        json!({
            "phase_name": self.phase_name(),
            "status": self.status().to_json_value(),
            "elapsed_micros": self.elapsed_micros(),
        })
    }
}

impl DeltaSourceReport {
    /// Returns the source report as a JSON-compatible Python shape.
    #[must_use]
    pub fn to_json_value(&self) -> Value {
        let protocol = self.protocol();
        let scheduling = self.scheduling();

        json!({
            "source_name": self.source_name(),
            "source_uri": self.source_uri(),
            "snapshot_version": self.snapshot_version(),
            "protocol": {
                "source_name": protocol.source_name,
                "table_uri": protocol.table_uri,
                "snapshot_version": protocol.snapshot_version,
                "min_reader_version": protocol.min_reader_version,
                "min_writer_version": protocol.min_writer_version,
                "reader_features": protocol.reader_features,
                "writer_features": protocol.writer_features,
            },
            "scheduling": {
                "query_target_partitions": scheduling.query_target_partitions(),
                "reader_backend": reader_backend(scheduling.reader_backend()),
                "max_concurrent_file_reads_per_scan": scheduling.max_concurrent_file_reads_per_scan(),
                "max_concurrent_file_reads_per_partition": scheduling.max_concurrent_file_reads_per_partition(),
                "output_buffer_capacity_per_partition": scheduling.output_buffer_capacity_per_partition(),
                "native_async_prefetch_file_count_per_partition": scheduling.native_async_prefetch_file_count_per_partition(),
            },
            "file_count": count_with_reason_value(
                self.file_count().kind().as_str(),
                self.file_count().value(),
                self.file_count_reason()
            ),
            "scan_metadata_exhausted": self.scan_metadata_exhausted(),
            "usage_status": self.usage_status().as_str(),
            "used_by_output_names": self.used_by_output_names(),
            "provider_read_stats_available": self.provider_read_stats().is_some(),
            "provider_stats_reason": reason_value(self.provider_stats_reason()),
            "phase_timings": phase_timings_value(self.phase_timings()),
        })
    }
}

impl MssqlDryRunOutputFieldReport {
    /// Returns the dry-run output field report as a JSON-compatible Python shape.
    #[must_use]
    pub fn to_json_value(&self) -> Value {
        json!({
            "index": self.index(),
            "name": self.name(),
            "arrow_type": self.arrow_type(),
            "nullable": self.nullable(),
        })
    }
}

impl MssqlDryRunSqlIdentityReport {
    /// Returns the dry-run SQL identity report as a JSON-compatible Python shape.
    #[must_use]
    pub fn to_json_value(&self) -> Value {
        json!({
            "state": self.state().as_str(),
            "hash": self.hash(),
            "reason": reason_value(self.reason()),
        })
    }
}

impl MssqlDryRunOutputReport {
    /// Returns the dry-run output report as a JSON-compatible Python shape.
    #[must_use]
    pub fn to_json_value(&self) -> Value {
        json!({
            "output_name": self.output_name(),
            "run_mode": run_mode(self.run_mode()),
            "status": self.status().to_json_value(),
            "table": {
                "id": self.table_id(),
                "kind": lazy_table_kind(self.table_kind()),
                "name": self.table_name(),
            },
            "target_table": target_table_value(self.target_table()),
            "load_mode": load_mode(self.load_mode()),
            "output_schema": self.output_schema()
                .iter()
                .map(MssqlDryRunOutputFieldReport::to_json_value)
                .collect::<Vec<_>>(),
            "target_schema_plan": {
                "output_field_count": self.target_schema_plan().mappings().len(),
                "diagnostic_count": self.target_schema_plan().diagnostic_reports().len(),
            },
            "target_ddl_plan": {
                "create_table_sql_present": self.target_ddl_plan().create_table_sql().is_some(),
            },
            "target_lifecycle_plan": {
                "create_table_sql_required": self.target_lifecycle_plan().create_table_sql_required(),
                "create_table_sql_present": self.target_lifecycle_plan().create_table_sql_present(),
                "executable_in_mvp": self.target_lifecycle_plan().executable_in_mvp(),
            },
            "sql_identity": self.sql_identity().to_json_value(),
            "source_usage_status": self.source_usage_status().as_str(),
            "used_source_names": self.used_source_names(),
            "output_row_count": count_with_reason_value(
                self.output_row_count().kind().as_str(),
                self.output_row_count().value(),
                self.output_row_count_reason()
            ),
            "validation_status": self.validation_status().to_json_value(),
            "phase_timings": phase_timings_value(self.phase_timings()),
            "dry_run": {
                "sql_server_contacted": self.sql_server_contacted(),
                "row_production_started": self.row_production_started(),
                "table_lifecycle_started": self.table_lifecycle_started(),
                "bulk_writer_started": self.bulk_writer_started(),
            },
        })
    }
}

impl MssqlDryRunWorkflowReport {
    /// Returns the dry-run workflow report as a JSON-compatible Python shape.
    #[must_use]
    pub fn to_json_value(&self) -> Value {
        json!({
            "run_mode": run_mode(self.run_mode()),
            "status": self.status().to_json_value(),
            "output_count": self.len(),
            "query_used_source_scan_metadata_exhausted": self.query_used_source_scan_metadata_exhausted(),
            "sources": self.sources()
                .iter()
                .map(DeltaSourceReport::to_json_value)
                .collect::<Vec<_>>(),
            "outputs": self.outputs()
                .iter()
                .map(MssqlDryRunOutputReport::to_json_value)
                .collect::<Vec<_>>(),
            "phase_timings": phase_timings_value(self.phase_timings()),
            "dry_run": {
                "sql_server_contacted": self.sql_server_contacted(),
                "row_production_started": self.row_production_started(),
                "table_lifecycle_started": self.table_lifecycle_started(),
                "bulk_writer_started": self.bulk_writer_started(),
            },
        })
    }
}

fn count_value(kind: &str, value: Option<u64>) -> Value {
    json!({
        "kind": kind,
        "value": value,
    })
}

fn count_with_reason_value(
    kind: &str,
    value: Option<u64>,
    reason: Option<ReportReasonCode>,
) -> Value {
    json!({
        "kind": kind,
        "value": value,
        "reason": reason_value(reason),
    })
}

fn phase_timings_value(timings: &[PhaseTimingReport]) -> Vec<Value> {
    timings
        .iter()
        .map(PhaseTimingReport::to_json_value)
        .collect()
}

fn status_value(kind: &str, reason: Option<ReportReasonCode>) -> Value {
    json!({
        "kind": kind,
        "reason": reason_value(reason),
    })
}

fn reason_value(reason: Option<ReportReasonCode>) -> Option<&'static str> {
    reason.map(ReportReasonCode::as_str)
}

fn run_mode(mode: RunMode) -> &'static str {
    match mode {
        RunMode::Execute => "execute",
        RunMode::DryRun => "dry_run",
    }
}

fn lazy_table_kind(kind: LazyTableKind) -> &'static str {
    match kind {
        LazyTableKind::DeltaSource => "delta_source",
        LazyTableKind::DerivedSql => "derived_sql",
    }
}

fn load_mode(mode: LoadMode) -> &'static str {
    match mode {
        LoadMode::AppendExisting => "append_existing",
        LoadMode::CreateAndLoad => "create_and_load",
        LoadMode::Replace => "replace",
    }
}

fn target_table_value(table: &MssqlTargetTable) -> Value {
    json!({
        "schema": table.schema(),
        "table": table.table(),
    })
}

fn reader_backend(backend: DeltaProviderReaderBackend) -> &'static str {
    match backend {
        DeltaProviderReaderBackend::OfficialKernel => "official_kernel",
        DeltaProviderReaderBackend::NativeAsync => "native_async",
    }
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        fs,
        path::PathBuf,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use serde_json::{Value, json};

    use super::*;
    use crate::{
        DeltaFunnelSession, DeltaSourceConfig, MssqlConnectionConfig, MssqlOutputTarget,
        MssqlTargetConfig, OutputWritePlan, SessionOptions,
    };

    type TestResult<T> = Result<T, Box<dyn Error + Send + Sync + 'static>>;

    struct DeltaLogFixture {
        path: PathBuf,
    }

    impl DeltaLogFixture {
        fn new(name: &str) -> TestResult<Self> {
            let path = env_unique_path(name)?;
            let log_dir = path.join("_delta_log");
            fs::create_dir_all(&log_dir)?;
            fs::write(
                log_dir.join("00000000000000000000.json"),
                format!(
                    "{}\n{}\n",
                    r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
                    metadata_json()
                ),
            )?;

            Ok(Self { path })
        }

        fn uri(&self) -> String {
            self.path.to_string_lossy().to_string()
        }
    }

    impl Drop for DeltaLogFixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn row_count_json_preserves_kind_and_value() {
        assert_eq!(
            RowCount::exact(3).to_json_value(),
            json!({"kind": "exact", "value": 3})
        );
        assert_eq!(
            RowCount::estimated(5).to_json_value(),
            json!({"kind": "estimated", "value": 5})
        );
        assert_eq!(
            RowCount::partial(2).to_json_value(),
            json!({"kind": "partial", "value": 2})
        );
        assert_eq!(
            RowCount::unavailable().to_json_value(),
            json!({"kind": "unavailable", "value": null})
        );
    }

    #[test]
    fn file_count_json_preserves_non_numeric_kinds() {
        assert_eq!(
            FileCount::skipped().to_json_value(),
            json!({"kind": "skipped", "value": null})
        );
        assert_eq!(
            FileCount::not_executed().to_json_value(),
            json!({"kind": "not_executed", "value": null})
        );
    }

    #[test]
    fn status_json_preserves_stable_kind_and_reason_strings() {
        assert_eq!(
            ValidationStatus::skipped(ReportReasonCode::DryRun).to_json_value(),
            json!({"kind": "skipped", "reason": "dry_run"})
        );
        assert_eq!(
            PhaseStatus::not_started(ReportReasonCode::NotExecuted).to_json_value(),
            json!({"kind": "not_started", "reason": "not_executed"})
        );
        assert_eq!(
            WorkflowStatus::no_op(ReportReasonCode::NotExecuted).to_json_value(),
            json!({"kind": "no_op", "reason": "not_executed"})
        );
    }

    #[test]
    fn output_status_json_preserves_nested_validation_status() {
        assert_eq!(
            OutputStatus::validation_failed(ValidationStatus::required_but_failed(
                ReportReasonCode::MissingExactOutputRows
            ))
            .to_json_value(),
            json!({
                "kind": "validation_failed",
                "reason": null,
                "validation": {
                    "kind": "required_but_failed",
                    "reason": "missing_exact_output_rows"
                }
            })
        );
    }

    #[test]
    fn phase_timing_json_is_json_round_trippable() -> Result<(), serde_json::Error> {
        let value =
            PhaseTimingReport::completed("load_sources", Duration::from_micros(42)).to_json_value();

        assert_eq!(
            value,
            json!({
                "phase_name": "load_sources",
                "status": {"kind": "completed", "reason": null},
                "elapsed_micros": 42
            })
        );
        serde_json::from_str::<Value>(&serde_json::to_string(&value)?).map(|_| ())
    }

    #[tokio::test]
    async fn dry_run_workflow_json_exposes_sources_outputs_and_safe_diagnostics() -> TestResult<()>
    {
        let orders = DeltaLogFixture::new("orders-json-report")?;
        let mut session = session_with_default_connection()?;
        session.delta_lake(DeltaSourceConfig::new("orders", orders.uri()))?;
        let selected_orders = session
            .table_from_sql("select id, region from orders")
            .await?;
        let output = OutputWritePlan::new(
            selected_orders,
            MssqlOutputTarget::new(
                "orders_output",
                MssqlTargetConfig::new(MssqlTargetTable::unqualified("orders_sink")?),
                RunMode::DryRun,
            ),
        );

        let value = session.dry_run_all_to_mssql(&[output])?.to_json_value();

        assert_eq!(value["run_mode"], "dry_run");
        assert_eq!(value["status"], json!({"kind": "success", "reason": null}));
        assert_eq!(value["output_count"], 1);
        assert_eq!(value["sources"][0]["source_name"], "orders");
        assert_eq!(
            value["sources"][0]["file_count"],
            json!({"kind": "unavailable", "value": null, "reason": "cost_avoidance"})
        );
        assert_eq!(value["sources"][0]["provider_read_stats_available"], false);
        assert_eq!(value["sources"][0]["provider_stats_reason"], "not_executed");
        assert_eq!(value["outputs"][0]["output_name"], "orders_output");
        assert_eq!(value["outputs"][0]["status"]["kind"], "dry_run_planned");
        assert_eq!(value["outputs"][0]["target_table"]["table"], "orders_sink");
        assert_eq!(value["outputs"][0]["output_schema"][0]["name"], "id");
        assert_eq!(
            value["outputs"][0]["output_row_count"],
            json!({"kind": "unavailable", "value": null, "reason": "not_executed"})
        );
        assert_eq!(
            value["outputs"][0]["validation_status"],
            json!({"kind": "skipped", "reason": "dry_run"})
        );
        assert_eq!(
            value["outputs"][0]["dry_run"]["sql_server_contacted"],
            false
        );
        assert_json_safe(&value)?;
        assert_no_secret_or_raw_sql_text(&value);

        Ok(())
    }

    fn session_with_default_connection() -> Result<DeltaFunnelSession, crate::DeltaFunnelError> {
        let connection = MssqlConnectionConfig::new(
            "server=tcp:sql.example.com;database=warehouse;user=admin;password=secret-token",
        )?
        .with_display_label("warehouse");
        DeltaFunnelSession::new(SessionOptions::new().with_default_mssql_connection(connection))
    }

    fn metadata_json() -> String {
        format!(
            r#"{{"metaData":{{"id":"delta-funnel-json-test","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{{\"type\":\"struct\",\"fields\":{SCHEMA_FIELDS_JSON}}}","partitionColumns":[],"configuration":{{}},"createdTime":1587968585495}}}}"#
        )
    }

    const SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;

    fn env_unique_path(name: &str) -> TestResult<PathBuf> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        Ok(std::env::temp_dir().join(format!(
            "delta-funnel-json-report-{}-{name}-{nanos}",
            std::process::id()
        )))
    }

    fn assert_json_safe(value: &Value) -> TestResult<()> {
        serde_json::from_str::<Value>(&serde_json::to_string(value)?)?;
        Ok(())
    }

    fn assert_no_secret_or_raw_sql_text(value: &Value) {
        let text = value.to_string();
        assert!(!text.contains("secret-token"));
        assert!(!text.contains("password"));
        assert!(!text.contains("server=tcp"));
        assert!(!text.contains("select id"));
    }
}
