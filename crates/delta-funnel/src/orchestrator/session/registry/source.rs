use std::fmt;

use datafusion::arrow::datatypes::SchemaRef;

use crate::{
    DeltaFunnelError, DeltaProtocolReport, DeltaSourceConfig, DeltaTableProviderConfig,
    PhaseTimingReport, RegisteredDeltaSource,
    progress::{ProgressEvent, ProgressOperation, ProgressPhase, ProgressReporter},
    query_engine::datafusion::{
        register_delta_source_with_scan_execution_options, reject_existing_delta_registration_name,
    },
    report::PhaseTimer,
    table_formats::{
        load_delta_source_with_tracing, preflight_delta_protocol_with_tracing,
        validate_table_source_names,
    },
};

use super::super::{DeltaFunnelSession, LazyTable};

const SOURCE_LOADING_PHASE: &str = "source_loading";
const PROTOCOL_PREFLIGHT_PHASE: &str = "protocol_preflight";
const DATAFUSION_REGISTRATION_PHASE: &str = "datafusion_registration";

/// Registered Delta source tracked by a query-load session.
#[derive(Clone, PartialEq, Eq)]
pub struct RegisteredSessionSource {
    table: LazyTable,
    source_uri: String,
    snapshot_version: u64,
    schema: SchemaRef,
    protocol: DeltaProtocolReport,
    phase_timings: Vec<PhaseTimingReport>,
}

impl RegisteredSessionSource {
    pub(super) fn from_registered(
        table: LazyTable,
        registered: RegisteredDeltaSource,
        phase_timings: Vec<PhaseTimingReport>,
    ) -> Self {
        Self {
            table,
            source_uri: registered.table_uri,
            snapshot_version: registered.snapshot_version,
            schema: registered.schema,
            protocol: registered.protocol,
            phase_timings,
        }
    }

    /// Returns the lazy table handle for this registered source.
    #[must_use]
    pub const fn table(&self) -> &LazyTable {
        &self.table
    }

    /// Returns the DataFusion table name for this source.
    #[must_use]
    pub fn name(&self) -> &str {
        self.table.name()
    }

    /// Returns the sanitized Delta source URI or display summary.
    #[must_use]
    pub fn source_uri(&self) -> &str {
        &self.source_uri
    }

    /// Returns the resolved Delta snapshot version.
    #[must_use]
    pub const fn snapshot_version(&self) -> u64 {
        self.snapshot_version
    }

    /// Returns the logical Arrow schema exposed to DataFusion.
    #[must_use]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns the sanitized protocol report captured before registration.
    #[must_use]
    pub const fn protocol(&self) -> &DeltaProtocolReport {
        &self.protocol
    }

    /// Returns durable phase timings captured while registering this source.
    #[must_use]
    pub fn phase_timings(&self) -> &[PhaseTimingReport] {
        &self.phase_timings
    }
}

impl fmt::Debug for RegisteredSessionSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredSessionSource")
            .field("table", &self.table)
            .field("source_uri", &self.source_uri)
            .field("snapshot_version", &self.snapshot_version)
            .field("schema", &self.schema)
            .field("protocol", &self.protocol)
            .field("phase_timings", &self.phase_timings)
            .finish()
    }
}

impl DeltaFunnelSession {
    /// Registers one Delta source and returns its lazy table handle.
    ///
    /// The method performs source setup only: Delta snapshot metadata loading,
    /// protocol preflight, and DataFusion table registration. It does not scan
    /// data files for row production, parse user SQL, contact SQL Server, or
    /// execute an output action.
    ///
    /// # Errors
    ///
    /// Returns the first Delta source loading, protocol preflight, duplicate
    /// alias, schema conversion, or DataFusion registration error. Session
    /// source state is updated only after the DataFusion registration succeeds.
    pub fn delta_lake(&mut self, source: DeltaSourceConfig) -> Result<LazyTable, DeltaFunnelError> {
        self.validate_delta_source_registration(&source)?;
        self.register_delta_source(source, None)
    }

    /// Registers one Delta source while reporting its live lifecycle.
    ///
    /// Name and catalog conflicts fail before progress starts. Once started,
    /// the reporter receives the source-loading phases and exactly one terminal
    /// event. Reporting does not change registration or rollback behavior.
    pub(crate) fn delta_lake_with_progress(
        &mut self,
        source: DeltaSourceConfig,
        reporter: ProgressReporter,
    ) -> Result<LazyTable, DeltaFunnelError> {
        self.validate_delta_source_registration(&source)?;
        reporter.emit(&ProgressEvent::started(
            ProgressOperation::RegisterDeltaSource,
        ));
        let result = self.register_delta_source(source, Some(&reporter));
        reporter.emit(&if result.is_ok() {
            ProgressEvent::completed()
        } else {
            ProgressEvent::failed()
        });
        result
    }

    /// Performs source loading and catalog registration after local checks.
    fn register_delta_source(
        &mut self,
        source: DeltaSourceConfig,
        reporter: Option<&ProgressReporter>,
    ) -> Result<LazyTable, DeltaFunnelError> {
        let mut phase_timings = Vec::new();

        emit_registration_phase(reporter, ProgressPhase::LoadingDeltaMetadata);
        let source_timer = PhaseTimer::start(SOURCE_LOADING_PHASE);
        let planned = match load_delta_source_with_tracing(source) {
            Ok(planned) => {
                phase_timings.push(source_timer.completed());
                planned
            }
            Err(error) => {
                phase_timings.push(source_timer.failed());
                return Err(error);
            }
        };

        emit_registration_phase(reporter, ProgressPhase::ValidatingDeltaProtocol);
        let preflight_timer = PhaseTimer::start(PROTOCOL_PREFLIGHT_PHASE);
        let preflight = match preflight_delta_protocol_with_tracing(&planned) {
            Ok(preflight) => {
                phase_timings.push(preflight_timer.completed());
                preflight
            }
            Err(error) => {
                phase_timings.push(preflight_timer.failed());
                return Err(error);
            }
        };

        let registration_timer = PhaseTimer::start(DATAFUSION_REGISTRATION_PHASE);
        let config = DeltaTableProviderConfig {
            source: planned,
            protocol: preflight,
            scan_target_partitions: None,
        };
        let registered = match register_delta_source_with_scan_execution_options(
            &self.context,
            config,
            self.options.provider_scan_options(),
            reporter,
        ) {
            Ok(registered) => {
                phase_timings.push(registration_timer.completed());
                registered
            }
            Err(error) => {
                phase_timings.push(registration_timer.failed());
                return Err(error);
            }
        };
        let table = self.allocate_delta_source_table(registered.name.clone());
        let session_source =
            RegisteredSessionSource::from_registered(table.clone(), registered, phase_timings);
        self.sources.push(session_source);
        Ok(table)
    }

    /// Runs checks that must not start source loading or progress rendering.
    fn validate_delta_source_registration(
        &self,
        source: &DeltaSourceConfig,
    ) -> Result<(), DeltaFunnelError> {
        validate_table_source_names([source.name.as_str()])?;
        self.reject_registered_alias_name(&source.name)?;
        reject_existing_delta_registration_name(&self.context, &source.name, &source.table_uri)
    }

    fn allocate_delta_source_table(&mut self, name: String) -> LazyTable {
        let id = self.next_table_id;
        self.next_table_id = self.next_table_id.saturating_add(1);
        LazyTable::delta_source(id, name)
    }
}

fn emit_registration_phase(reporter: Option<&ProgressReporter>, phase: ProgressPhase) {
    if let Some(reporter) = reporter {
        reporter.emit(&ProgressEvent::phase_changed(phase, None));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use datafusion::{arrow::datatypes::Schema, datasource::empty::EmptyTable};

    use super::{DATAFUSION_REGISTRATION_PHASE, PROTOCOL_PREFLIGHT_PHASE, SOURCE_LOADING_PHASE};

    use crate::{
        DeltaFunnelError, DeltaProviderReaderBackend, DeltaProviderScanExecutionOptions,
        DeltaSourceConfig, DeltaStorageOptions, QueryOptions,
        progress::{
            ProgressEvent, ProgressEventKind, ProgressOperation, ProgressPhase, ProgressReporter,
        },
        query_engine::datafusion::test_support::{
            FailsOnCustomersSchemaProvider, INVALID_NESTED_IDS_SCHEMA_FIELDS_JSON,
            SingleSchemaCatalogProvider,
        },
    };

    use super::super::super::{
        DeltaFunnelSession, LazyTableKind, SessionOptions, SourceUsageStatus,
        test_support::DeltaLogTable,
    };

    const UNSUPPORTED_PROTOCOL_JSON: &str =
        r#"{"protocol":{"minReaderVersion":99,"minWriterVersion":2}}"#;

    fn recording_reporter() -> (ProgressReporter, Arc<Mutex<Vec<ProgressEvent>>>) {
        let events = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&events);
        let reporter = ProgressReporter::new(move |event| {
            if let Ok(mut events) = recorded.lock() {
                events.push(event.clone());
            }
        });
        (reporter, events)
    }

    #[test]
    fn delta_lake_progress_reports_ordered_registration_lifecycle()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders-progress")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (reporter, events) = recording_reporter();

        session
            .delta_lake_with_progress(DeltaSourceConfig::new("orders", table.uri()), reporter)?;

        let events = events.lock().map_err(|_| "progress events lock poisoned")?;
        assert_eq!(events.len(), 6);
        assert_eq!(events[0].kind(), ProgressEventKind::Started);
        assert_eq!(
            events[0].operation(),
            Some(ProgressOperation::RegisterDeltaSource)
        );
        assert_eq!(
            events[1..5]
                .iter()
                .map(ProgressEvent::phase)
                .collect::<Vec<_>>(),
            vec![
                Some(ProgressPhase::LoadingDeltaMetadata),
                Some(ProgressPhase::ValidatingDeltaProtocol),
                Some(ProgressPhase::PreparingDeltaProvider),
                Some(ProgressPhase::RegisteringDeltaSource),
            ]
        );
        assert_eq!(events[5].kind(), ProgressEventKind::Completed);
        assert!(events.iter().all(|event| event.output_name().is_none()));
        assert!(events.iter().all(|event| event.files_total().is_none()));
        assert!(events.iter().all(|event| event.rows().is_none()));
        Ok(())
    }

    #[test]
    fn delta_lake_progress_fails_after_start_for_source_loading_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (reporter, events) = recording_reporter();

        let result =
            session.delta_lake_with_progress(DeltaSourceConfig::new("orders", ""), reporter);

        assert!(matches!(
            result,
            Err(DeltaFunnelError::InvalidSourceUri { .. })
        ));
        let events = events.lock().map_err(|_| "progress events lock poisoned")?;
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].kind(), ProgressEventKind::Started);
        assert_eq!(events[1].phase(), Some(ProgressPhase::LoadingDeltaMetadata));
        assert_eq!(events[2].kind(), ProgressEventKind::Failed);
        Ok(())
    }

    #[test]
    fn delta_lake_progress_reports_protocol_failure_after_validation_starts()
    -> Result<(), Box<dyn std::error::Error>> {
        let table =
            DeltaLogTable::new_with_protocol("progress-protocol", UNSUPPORTED_PROTOCOL_JSON)?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (reporter, events) = recording_reporter();

        let result = session
            .delta_lake_with_progress(DeltaSourceConfig::new("orders", table.uri()), reporter);

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DeltaProtocolCompatibility { .. })
        ));
        let events = events.lock().map_err(|_| "progress events lock poisoned")?;
        assert_eq!(events.len(), 4);
        assert_eq!(events[0].kind(), ProgressEventKind::Started);
        assert_eq!(events[1].phase(), Some(ProgressPhase::LoadingDeltaMetadata));
        assert_eq!(
            events[2].phase(),
            Some(ProgressPhase::ValidatingDeltaProtocol)
        );
        assert_eq!(events[3].kind(), ProgressEventKind::Failed);
        Ok(())
    }

    #[test]
    fn delta_lake_progress_reports_provider_preparation_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "progress-provider",
            INVALID_NESTED_IDS_SCHEMA_FIELDS_JSON,
        )?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (reporter, events) = recording_reporter();

        let result = session
            .delta_lake_with_progress(DeltaSourceConfig::new("orders", table.uri()), reporter);

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DeltaSourceSchema { .. })
        ));
        let events = events.lock().map_err(|_| "progress events lock poisoned")?;
        assert_eq!(events.len(), 5);
        assert_eq!(
            events[3].phase(),
            Some(ProgressPhase::PreparingDeltaProvider)
        );
        assert_eq!(events[4].kind(), ProgressEventKind::Failed);
        assert!(
            events
                .iter()
                .all(|event| event.phase() != Some(ProgressPhase::RegisteringDeltaSource))
        );
        Ok(())
    }

    #[test]
    fn delta_lake_progress_reports_catalog_registration_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("progress-catalog")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let failing_schema: Arc<dyn datafusion::catalog::SchemaProvider> =
            Arc::new(FailsOnCustomersSchemaProvider::default());
        session.context().register_catalog(
            "datafusion",
            Arc::new(SingleSchemaCatalogProvider::new(failing_schema)),
        );
        let (reporter, events) = recording_reporter();

        let result = session
            .delta_lake_with_progress(DeltaSourceConfig::new("customers", table.uri()), reporter);

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DataFusionRegistration { .. })
        ));
        let events = events.lock().map_err(|_| "progress events lock poisoned")?;
        assert_eq!(events.len(), 6);
        assert_eq!(
            events[4].phase(),
            Some(ProgressPhase::RegisteringDeltaSource)
        );
        assert_eq!(events[5].kind(), ProgressEventKind::Failed);
        assert!(session.sources().is_empty());
        assert_eq!(session.next_table_id(), 0);
        Ok(())
    }

    #[test]
    fn delta_lake_progress_does_not_start_for_local_registration_errors()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders-conflict")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        for source in [
            DeltaSourceConfig::new("select", ""),
            DeltaSourceConfig::new("ORDERS", ""),
        ] {
            let (reporter, events) = recording_reporter();
            assert!(session.delta_lake_with_progress(source, reporter).is_err());
            assert!(
                events
                    .lock()
                    .map_err(|_| "progress events lock poisoned")?
                    .is_empty()
            );
        }
        Ok(())
    }

    #[test]
    fn delta_lake_progress_does_not_start_for_datafusion_catalog_conflict()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.context().register_table(
            "ExistingOrders",
            Arc::new(EmptyTable::new(Arc::new(Schema::empty()))),
        )?;
        let (reporter, events) = recording_reporter();

        let result = session.delta_lake_with_progress(
            DeltaSourceConfig::new("existingorders", "secret://must-not-load"),
            reporter,
        );

        assert!(matches!(
            result,
            Err(DeltaFunnelError::DataFusionRegistration { .. })
        ));
        assert!(
            events
                .lock()
                .map_err(|_| "progress events lock poisoned")?
                .is_empty()
        );
        Ok(())
    }

    #[test]
    fn delta_lake_registers_source_and_returns_lazy_table() -> Result<(), Box<dyn std::error::Error>>
    {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        let lazy = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        assert_eq!(lazy.id(), 0);
        assert_eq!(lazy.kind(), LazyTableKind::DeltaSource);
        assert_eq!(lazy.name(), "orders");
        assert_eq!(session.next_table_id(), 1);
        assert_eq!(session.sources().len(), 1);
        let registered = session
            .registered_source("ORDERS")
            .ok_or("expected registered source")?;
        assert_eq!(registered.table(), &lazy);
        assert_eq!(registered.name(), "orders");
        assert!(registered.source_uri().starts_with("file://"));
        assert_eq!(registered.snapshot_version(), 1);
        assert_eq!(registered.protocol().source_name, "orders");
        assert_eq!(registered.schema().fields().len(), 2);
        let source_reports = session.source_reports();
        assert_eq!(source_reports.len(), 1);
        let report = &source_reports[0];
        assert_eq!(report.source_name(), "orders");
        assert_eq!(report.source_uri(), registered.source_uri());
        assert_eq!(report.snapshot_version(), 1);
        assert_eq!(report.protocol().source_name, "orders");
        assert_eq!(report.scheduling().query_target_partitions(), None);
        assert_eq!(
            report.scheduling().reader_backend(),
            DeltaProviderReaderBackend::NativeAsync
        );
        assert_eq!(
            report.scheduling().max_concurrent_file_reads_per_scan(),
            None
        );
        assert_eq!(report.file_count(), crate::FileCount::unavailable());
        assert_eq!(
            report.file_count_reason(),
            Some(crate::ReportReasonCode::CostAvoidance)
        );
        assert!(!report.scan_metadata_exhausted());
        assert_eq!(report.usage_status(), SourceUsageStatus::Unknown);
        assert!(report.used_by_output_names().is_empty());
        assert!(report.provider_read_stats().is_none());
        assert_eq!(
            report.provider_stats_reason(),
            Some(crate::ReportReasonCode::NotExecuted)
        );
        let phase_timings = report.phase_timings();
        assert_eq!(
            phase_timings
                .iter()
                .map(crate::PhaseTimingReport::phase_name)
                .collect::<Vec<_>>(),
            vec![
                SOURCE_LOADING_PHASE,
                PROTOCOL_PREFLIGHT_PHASE,
                DATAFUSION_REGISTRATION_PHASE
            ]
        );
        assert!(
            phase_timings
                .iter()
                .all(|timing| timing.status().is_completed())
        );
        assert!(
            phase_timings
                .iter()
                .all(|timing| timing.elapsed_micros().is_some())
        );

        Ok(())
    }

    #[test]
    fn delta_lake_registers_multiple_distinct_sources() -> Result<(), Box<dyn std::error::Error>> {
        let orders = DeltaLogTable::new("orders")?;
        let customers = DeltaLogTable::new("customers")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        let orders = session.delta_lake(DeltaSourceConfig::new("orders", orders.uri()))?;
        let customers = session.delta_lake(DeltaSourceConfig::new("customers", customers.uri()))?;

        assert_eq!(orders.id(), 0);
        assert_eq!(customers.id(), 1);
        assert_eq!(session.sources().len(), 2);
        assert!(session.registered_source("orders").is_some());
        assert!(session.registered_source("customers").is_some());
        Ok(())
    }

    #[test]
    fn duplicate_source_alias_fails_before_loading_second_source()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        let error = session.delta_lake(DeltaSourceConfig::new("ORDERS", ""));

        assert!(matches!(
            error,
            Err(DeltaFunnelError::DuplicateSourceName { name }) if name == "ORDERS"
        ));
        assert_eq!(session.sources().len(), 1);
        assert_eq!(session.next_table_id(), 1);
        Ok(())
    }

    #[test]
    fn invalid_source_alias_fails_before_registration() -> Result<(), DeltaFunnelError> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        let error = session.delta_lake(DeltaSourceConfig::new("select", ""));

        assert!(matches!(
            error,
            Err(DeltaFunnelError::InvalidSourceName { name, .. }) if name == "select"
        ));
        assert!(session.sources().is_empty());
        assert_eq!(session.next_table_id(), 0);
        Ok(())
    }

    #[test]
    fn protocol_preflight_failure_does_not_register_source()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_protocol("unsupported", UNSUPPORTED_PROTOCOL_JSON)?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        let error = session.delta_lake(DeltaSourceConfig::new("unsupported", table.uri()));

        let display = format!("{}", error.as_ref().err().ok_or("expected error")?);
        assert!(display.contains("unsupported"));
        assert!(display.contains("unsupported Delta minReaderVersion"));
        assert!(matches!(
            error,
            Err(DeltaFunnelError::DeltaProtocolCompatibility { .. })
        ));
        assert!(session.sources().is_empty());
        assert_eq!(session.next_table_id(), 0);
        Ok(())
    }

    #[test]
    fn protocol_preflight_failure_redacts_secret_uri_parts()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_protocol("unsupported", UNSUPPORTED_PROTOCOL_JSON)?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        let error = session
            .delta_lake(DeltaSourceConfig::new(
                "unsupported",
                table.file_uri_with_secret_parts()?,
            ))
            .map(|_| ())
            .map_err(|error| error.to_string());

        assert!(
            matches!(error, Err(display) if display.contains("unsupported")
            && display.contains("unsupported Delta minReaderVersion")
            && !display.contains("super-secret")
            && !display.contains("debug-secret")
            && !display.contains("token"))
        );
        assert!(session.sources().is_empty());
        assert_eq!(session.next_table_id(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn protocol_preflight_failure_does_not_leak_datafusion_alias()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_protocol("unsupported", UNSUPPORTED_PROTOCOL_JSON)?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        let error = session.delta_lake(DeltaSourceConfig::new("unsupported", table.uri()));

        assert!(matches!(
            error,
            Err(DeltaFunnelError::DeltaProtocolCompatibility { .. })
        ));
        assert!(session.context().table("unsupported").await.is_err());
        assert!(session.sources().is_empty());
        assert_eq!(session.next_table_id(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn registered_source_sql_analysis_does_not_read_data_files_for_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let dataframe = session
            .context()
            .sql("select id, customer_name from orders")
            .await?;
        let schema = dataframe.schema();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "customer_name");
        assert_eq!(session.sources().len(), 1);
        Ok(())
    }

    #[test]
    fn source_debug_does_not_expose_storage_option_values() -> Result<(), Box<dyn std::error::Error>>
    {
        let table = DeltaLogTable::new("storage-options")?;
        let mut storage_options = DeltaStorageOptions::new();
        storage_options.insert(
            "AWS_SECRET_ACCESS_KEY".to_owned(),
            "super-secret".to_owned(),
        );
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        session.delta_lake(
            DeltaSourceConfig::new("orders", table.uri()).with_storage_options(storage_options),
        )?;

        let debug = format!("{session:?}");
        assert!(debug.contains("orders"));
        assert!(!debug.contains("super-secret"));
        assert!(!debug.contains("AWS_SECRET_ACCESS_KEY"));
        let report_debug = format!("{:?}", session.source_reports());
        assert!(report_debug.contains("orders"));
        assert!(!report_debug.contains("super-secret"));
        assert!(!report_debug.contains("AWS_SECRET_ACCESS_KEY"));
        Ok(())
    }

    #[test]
    fn source_registration_honors_configured_provider_options()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("configured-provider")?;
        let provider_scan_options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::OfficialKernel,
            2,
            1,
        )?
        .with_output_buffer_capacity_per_partition(3)?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new()
                .with_query_options(QueryOptions {
                    target_partitions: Some(4),
                    output_batch_size: None,
                })
                .with_provider_scan_options(provider_scan_options),
        )?;

        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        assert_eq!(session.sources().len(), 1);
        assert!(session.registered_source("orders").is_some());
        let reports = session.source_reports();
        assert_eq!(reports.len(), 1);
        let scheduling = reports[0].scheduling();
        assert_eq!(scheduling.query_target_partitions(), Some(4));
        assert_eq!(
            scheduling.reader_backend(),
            DeltaProviderReaderBackend::OfficialKernel
        );
        assert_eq!(scheduling.max_concurrent_file_reads_per_scan(), Some(2));
        assert_eq!(scheduling.max_concurrent_file_reads_per_partition(), 1);
        assert_eq!(scheduling.output_buffer_capacity_per_partition(), 3);
        assert_eq!(
            scheduling.native_async_prefetch_file_count_per_partition(),
            0
        );
        Ok(())
    }
}
