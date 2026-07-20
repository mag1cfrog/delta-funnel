//! Converts typed reports into explicit JSON-compatible values.
//!
//! Rust callers use these values for structured diagnostics, and the Python
//! binding converts the same values into dictionaries. Keeping the mappings
//! here gives both APIs one report shape and makes the fields that are safe to
//! expose explicit instead of relying on generic serialization.

use std::{collections::BTreeMap, time::Duration};

use serde_json::{Value, json};

use crate::{
    DeltaProviderReadStatsSnapshot, DeltaProviderReaderBackend, DeltaSourceReport, FileCount,
    LazyTableKind, LoadMode, MssqlDryRunOutputFieldReport, MssqlDryRunOutputReport,
    MssqlDryRunSqlIdentityReport, MssqlDryRunWorkflowReport, MssqlOutputBatchValidationReport,
    MssqlOutputFieldReport, MssqlOutputWriteStatus, MssqlTargetCleanupStatus, MssqlTargetTable,
    MssqlWorkflowWriteReport, MssqlWriteFailureContext, MssqlWriteFailureReport, MssqlWritePhase,
    MssqlWriteReport, MssqlWriteSkippedReason, MssqlWriteSkippedReport, MssqlWriteStats,
    OperationTimeline, OutputStatus, PhaseStatus, PhaseTimingReport, QueryExecutionMetric,
    QueryExecutionMetricValue, QueryExecutionOperatorProfile, QueryExecutionProfile,
    ReportReasonCode, RowCount, RunMode, TablePreview, TimelineSpan, TimelineSpanStatus,
    TimelineSpanTimeSemantics, ValidationStatus, WorkflowStatus, WriteAllCacheAliasReport,
    WriteAllCacheAliasStatus, WriteAllCacheCandidateSkip, WriteAllCacheCandidateSkipReason,
    WriteAllCacheFailure, WriteAllCacheReport, WriteAllNoCacheReason, WriteAllReport,
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

impl OperationTimeline {
    /// Returns this wall-clock timeline as a stable JSON-compatible value.
    #[must_use]
    pub fn to_json_value(&self) -> Value {
        json!({
            "schema_version": Self::SCHEMA_VERSION,
            "name": self.name(),
            "status": self.status().as_str(),
            "total_duration_micros": self.total_duration_micros(),
            "spans": self
                .spans()
                .iter()
                .map(TimelineSpan::to_json_value)
                .collect::<Vec<_>>(),
        })
    }

    /// Returns a Chrome Trace Event JSON document for this wall-clock timeline.
    #[must_use]
    pub fn to_trace_event_json_value(&self) -> Value {
        // VizTracer and Perfetto reserve thread zero for their main-thread track.
        const ROOT_LANE: u64 = 1;
        let mut events = vec![
            json!({
                "name": "process_name",
                "cat": "__metadata",
                "ph": "M",
                "pid": 1,
                "args": {"name": format!("Delta Funnel {}", self.name())},
            }),
            trace_lane_metadata(ROOT_LANE, self.name()),
            json!({
                "name": self.name(),
                "cat": "delta_funnel.operation",
                "ph": "X",
                "pid": 1,
                "tid": ROOT_LANE,
                "ts": 0,
                "dur": self.total_duration_micros(),
                "args": {
                    "id": 0,
                    "parent_id": Value::Null,
                    "status": self.status().as_str(),
                    "time_semantics": "wall_clock",
                },
            }),
        ];

        let mut lanes = BTreeMap::new();
        for span in self.spans() {
            let next_lane = ROOT_LANE.saturating_add(super::usize_to_u64_saturating(
                lanes.len().saturating_add(1),
            ));
            let lane_key = (span.category().to_owned(), span.track_name().to_owned());
            let (lane, new_lane) = match lanes.entry(lane_key) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(next_lane);
                    (next_lane, true)
                }
                std::collections::btree_map::Entry::Occupied(entry) => (*entry.get(), false),
            };
            if new_lane {
                events.push(trace_lane_metadata(lane, span.track_name()));
            }
            events.push(json!({
                "name": span.name(),
                "cat": span.category(),
                "ph": "X",
                "pid": 1,
                "tid": lane,
                "ts": span.start_offset_micros(),
                "dur": span.duration_micros(),
                "args": {
                    "id": span.id(),
                    "parent_id": span.parent_id().unwrap_or(0),
                    "status": span.status().as_str(),
                    "time_semantics": span.time_semantics().as_str(),
                    "attributes": span.attributes(),
                },
            }));
        }

        json!({
            "traceEvents": events,
            "displayTimeUnit": "ms",
            "delta_funnel_timeline": self.to_json_value(),
        })
    }
}

impl TimelineSpan {
    fn to_json_value(&self) -> Value {
        json!({
            "id": self.id(),
            "parent_id": self.parent_id(),
            "name": self.name(),
            "track_name": self.track_name(),
            "category": self.category(),
            "start_offset_micros": self.start_offset_micros(),
            "duration_micros": self.duration_micros(),
            "status": self.status().as_str(),
            "time_semantics": self.time_semantics().as_str(),
            "attributes": self.attributes(),
        })
    }
}

fn trace_lane_metadata(lane: u64, name: &str) -> Value {
    json!({
        "name": "thread_name",
        "cat": "__metadata",
        "ph": "M",
        "pid": 1,
        "tid": lane,
        "args": {"name": name},
    })
}

impl QueryExecutionProfile {
    /// Returns this execution profile as a stable JSON-compatible value.
    #[must_use]
    pub fn to_json_value(&self) -> Value {
        json!({
            "scope": self.scope().as_str(),
            "outcome": self.outcome().as_str(),
            "partial": self.partial(),
            "delta_funnel_row_limit": self.delta_funnel_row_limit(),
            "operators": self
                .operators()
                .iter()
                .map(QueryExecutionOperatorProfile::to_json_value)
                .collect::<Vec<_>>(),
        })
    }

    pub(crate) fn operator_lifecycle_timeline_spans(
        &self,
        first_span_id: u64,
        wall_clock_origin_nanos: i128,
        total_duration_micros: u64,
    ) -> Vec<TimelineSpan> {
        let status = match self.outcome() {
            crate::QueryExecutionOutcome::Success => TimelineSpanStatus::Completed,
            crate::QueryExecutionOutcome::Error => TimelineSpanStatus::Failed,
            crate::QueryExecutionOutcome::Cancelled => TimelineSpanStatus::Cancelled,
        };

        trace_spans(self)
            .into_iter()
            .enumerate()
            .filter_map(|(index, span)| {
                let start_micros = timestamp_offset_micros(
                    span.start_nanos,
                    wall_clock_origin_nanos,
                    total_duration_micros,
                );
                let end_micros = timestamp_offset_micros(
                    span.end_nanos,
                    wall_clock_origin_nanos,
                    total_duration_micros,
                );
                let duration_micros = end_micros.checked_sub(start_micros)?;
                let id = first_span_id.saturating_add(super::usize_to_u64_saturating(index));

                Some(
                    TimelineSpan::new(
                        id,
                        None,
                        span.operator.operator_name(),
                        "datafusion.operator.lifecycle",
                        Duration::from_micros(start_micros),
                        Duration::from_micros(duration_micros),
                        status,
                        TimelineSpanTimeSemantics::Lifecycle,
                    )
                    .with_track_name(trace_lane_name(&span))
                    .with_attribute("node_id", json!(span.operator.node_id()))
                    .with_attribute("parent_node_id", json!(span.operator.parent_node_id()))
                    .with_attribute("partition", json!(span.partition))
                    .with_attribute(
                        "output_partition_count",
                        json!(span.operator.output_partition_count()),
                    )
                    .with_attribute(
                        "metrics",
                        Value::Array(
                            span.metrics
                                .iter()
                                .map(|metric| metric.to_json_value())
                                .collect(),
                        ),
                    ),
                )
            })
            .collect()
    }

    /// Returns a Chrome Trace Event JSON document for this execution profile.
    ///
    /// Each physical operator partition with terminal start and end timestamps
    /// becomes one complete event on its own synthetic track. Timestamps and
    /// durations use the trace format's standard microsecond unit and are
    /// relative to the first included operator start.
    #[must_use]
    pub fn to_trace_event_json_value(&self) -> Value {
        let spans = trace_spans(self);
        let origin_nanos = spans
            .iter()
            .map(|span| span.start_nanos)
            .min()
            .unwrap_or_default();
        let mut events = vec![json!({
            "name": "process_name",
            "cat": "__metadata",
            "ph": "M",
            "pid": 1,
            "args": {
                "name": format!("Delta Funnel {}", self.scope().as_str()),
            },
        })];

        for (lane, span) in spans.into_iter().enumerate() {
            let lane = super::usize_to_u64_saturating(lane.saturating_add(1));
            events.push(json!({
                "name": "thread_name",
                "cat": "__metadata",
                "ph": "M",
                "pid": 1,
                "tid": lane,
                "args": {
                    "name": trace_lane_name(&span),
                },
            }));
            events.push(json!({
                "name": span.operator.operator_name(),
                "cat": "datafusion.operator",
                "ph": "X",
                "pid": 1,
                "tid": lane,
                "ts": nanos_to_micros(span.start_nanos.abs_diff(origin_nanos)),
                "dur": nanos_to_micros(span.end_nanos.abs_diff(span.start_nanos)),
                "args": {
                    "node_id": span.operator.node_id(),
                    "parent_node_id": span.operator.parent_node_id(),
                    "partition": span.partition,
                    "output_partition_count": span.operator.output_partition_count(),
                    "metrics": span
                        .metrics
                        .iter()
                        .map(|metric| metric.to_json_value())
                        .collect::<Vec<_>>(),
                },
            }));
        }

        json!({
            "traceEvents": events,
            "delta_funnel_profile": self.to_json_value(),
        })
    }
}

struct TraceSpan<'a> {
    operator: &'a QueryExecutionOperatorProfile,
    partition: Option<u64>,
    start_nanos: i64,
    end_nanos: i64,
    metrics: Vec<&'a QueryExecutionMetric>,
}

#[derive(Default)]
struct TraceSpanMetrics<'a> {
    start_nanos: Option<i64>,
    end_nanos: Option<i64>,
    metrics: Vec<&'a QueryExecutionMetric>,
}

fn trace_spans(profile: &QueryExecutionProfile) -> Vec<TraceSpan<'_>> {
    let mut spans = Vec::new();

    for operator in profile.operators() {
        let mut partition_metrics = BTreeMap::new();
        for metric in operator.metrics() {
            let entry = partition_metrics
                .entry(metric.partition())
                .or_insert_with(TraceSpanMetrics::default);
            match (metric.name(), metric.value()) {
                (
                    "start_timestamp",
                    QueryExecutionMetricValue::TimestampNanoseconds(Some(value)),
                ) => {
                    entry.start_nanos = Some(
                        entry
                            .start_nanos
                            .map_or(*value, |current| current.min(*value)),
                    );
                }
                ("end_timestamp", QueryExecutionMetricValue::TimestampNanoseconds(Some(value))) => {
                    entry.end_nanos = Some(
                        entry
                            .end_nanos
                            .map_or(*value, |current| current.max(*value)),
                    );
                }
                _ => entry.metrics.push(metric),
            }
        }

        spans.extend(
            partition_metrics
                .into_iter()
                .filter_map(|(partition, metrics)| {
                    let start_nanos = metrics.start_nanos?;
                    let end_nanos = metrics.end_nanos?;
                    (end_nanos >= start_nanos).then_some(TraceSpan {
                        operator,
                        partition,
                        start_nanos,
                        end_nanos,
                        metrics: metrics.metrics,
                    })
                }),
        );
    }

    spans
}

fn trace_lane_name(span: &TraceSpan<'_>) -> String {
    let partition = span
        .partition
        .map_or_else(|| "global".to_owned(), |value| value.to_string());
    let mut name = format!(
        "{} [node {}, partition {partition}",
        span.operator.operator_name(),
        span.operator.node_id()
    );
    name.push(']');
    name
}

fn nanos_to_micros(nanos: u64) -> f64 {
    nanos as f64 / 1_000.0
}

fn timestamp_offset_micros(
    timestamp_nanos: i64,
    wall_clock_origin_nanos: i128,
    total_duration_micros: u64,
) -> u64 {
    let offset_nanos = i128::from(timestamp_nanos).saturating_sub(wall_clock_origin_nanos);
    if offset_nanos <= 0 {
        return 0;
    }
    let offset_micros = offset_nanos / 1_000;
    match u64::try_from(offset_micros) {
        Ok(value) => value.min(total_duration_micros),
        Err(_) => total_duration_micros,
    }
}

impl crate::PreviewFailureContext {
    /// Returns this preview failure context as a stable JSON-compatible value.
    #[must_use]
    pub fn to_json_value(&self) -> Value {
        json!({
            "failed_phase": self.failed_phase(),
            "phase_timings": self
                .phase_timings()
                .iter()
                .map(PhaseTimingReport::to_json_value)
                .collect::<Vec<_>>(),
            "execution_profile": self
                .execution_profile()
                .map(QueryExecutionProfile::to_json_value),
            "operation_timeline": self
                .operation_timeline()
                .map(OperationTimeline::to_json_value),
        })
    }
}

impl TablePreview {
    /// Returns a Chrome Trace Event JSON document for this profiled preview.
    ///
    /// Returns `None` for a legacy preview or when detailed execution
    /// profiling was disabled.
    #[must_use]
    pub fn to_trace_event_json_value(&self) -> Option<Value> {
        let profile = self.execution_profile()?;
        let mut trace = self.operation_timeline()?.to_trace_event_json_value();
        let Value::Object(document) = &mut trace else {
            return None;
        };
        document.insert("delta_funnel_profile".to_owned(), profile.to_json_value());
        Some(trace)
    }
}

impl QueryExecutionOperatorProfile {
    /// Returns this operator profile as a stable JSON-compatible value.
    #[must_use]
    pub fn to_json_value(&self) -> Value {
        json!({
            "node_id": self.node_id(),
            "parent_node_id": self.parent_node_id(),
            "operator_name": self.operator_name(),
            "output_partition_count": self.output_partition_count(),
            "metrics_available": self.metrics_available(),
            "aggregated_metrics": self
                .aggregated_metrics()
                .iter()
                .map(QueryExecutionMetric::to_json_value)
                .collect::<Vec<_>>(),
            "metrics": self
                .metrics()
                .iter()
                .map(QueryExecutionMetric::to_json_value)
                .collect::<Vec<_>>(),
            "delta_provider_read_stats": self
                .delta_provider_read_stats()
                .map(provider_read_stats_value),
        })
    }
}

impl QueryExecutionMetric {
    /// Returns this redacted metric as a stable JSON-compatible value.
    #[must_use]
    pub fn to_json_value(&self) -> Value {
        let (value, components) = execution_metric_value(self.value());

        json!({
            "name": self.name(),
            "category": self.category().as_str(),
            "partition": self.partition(),
            "output_partition": self.output_partition(),
            "value_kind": self.value().value_kind(),
            "value": value,
            "components": components,
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
            "provider_read_stats": self.provider_read_stats().map(provider_read_stats_value),
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
                "create_table_sql_present": self.target_ddl_plan().create_table_sql_present(),
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

impl MssqlOutputFieldReport {
    /// Returns the execute output field report as a JSON-compatible Python shape.
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

impl MssqlWriteStats {
    /// Returns SQL Server write stats as a JSON-compatible Python shape.
    #[must_use]
    pub fn to_json_value(&self) -> Value {
        json!({
            "output_name": self.output_name(),
            "rows_written": self.rows_written(),
            "batches_written": self.batches_written(),
            "elapsed_ms": self.elapsed_ms(),
        })
    }
}

impl MssqlWriteReport {
    /// Returns the execute output report as a JSON-compatible Python shape.
    #[must_use]
    pub fn to_json_value(&self) -> Value {
        json!({
            "output_name": self.output_name(),
            "run_mode": run_mode(RunMode::Execute),
            "target_table": target_table_value(self.target_table()),
            "load_mode": load_mode(self.load_mode()),
            "connection_source": connection_source(self.connection_source()),
            "connection": {
                "display_label": self.connection().display_label(),
            },
            "output_schema": self.output_schema()
                .iter()
                .map(MssqlOutputFieldReport::to_json_value)
                .collect::<Vec<_>>(),
            "output_row_count": self.output_row_count().to_json_value(),
            "target_row_count_before_write": self.target_row_count_before_write().to_json_value(),
            "target_row_count_after_write": self.target_row_count_after_write().to_json_value(),
            "target_row_count": self.target_row_count().to_json_value(),
            "validation_status": self.validation_status().to_json_value(),
            "batch_shaping": batch_shaping_value(self.batch_shaping()),
            "phase_timings": phase_timings_value(self.phase_timings()),
            "execution_profile": self
                .execution_profile()
                .map(QueryExecutionProfile::to_json_value),
            "operation_timeline": self
                .operation_timeline()
                .map(OperationTimeline::to_json_value),
            "write_stats": self.stats().to_json_value(),
            "partial_write_possible": self.partial_write_possible(),
            "cleanup": cleanup_status(self.cleanup()),
        })
    }

    /// Returns a Chrome Trace Event JSON document for this profiled write.
    #[must_use]
    pub fn to_trace_event_json_value(&self) -> Option<Value> {
        let profile = self.execution_profile()?;
        let mut trace = self.operation_timeline()?.to_trace_event_json_value();
        let Value::Object(document) = &mut trace else {
            return None;
        };
        document.insert("delta_funnel_profile".to_owned(), profile.to_json_value());
        Some(trace)
    }
}

impl MssqlOutputBatchValidationReport {
    /// Returns the output batch validation report as a JSON-compatible Python shape.
    #[must_use]
    pub fn to_json_value(&self) -> Value {
        json!({
            "output_name": self.output_name(),
            "target_table": target_table_value(self.target_table()),
            "load_mode": load_mode(self.load_mode()),
            "connection_source": connection_source(self.connection_source()),
            "connection": {
                "display_label": self.connection().display_label(),
            },
        })
    }
}

impl MssqlOutputWriteStatus {
    /// Returns one workflow output status as a JSON-compatible Python shape.
    #[must_use]
    pub fn to_json_value(&self) -> Value {
        match self {
            Self::Succeeded(report) => json!({
                "kind": "succeeded",
                "output_name": self.output_name(),
                "target_table": target_table_value(self.target_table()),
                "load_mode": load_mode(self.load_mode()),
                "connection_source": connection_source(self.connection_source()),
                "output_row_count": self.output_row_count().to_json_value(),
                "target_row_count": self.target_row_count().to_json_value(),
                "validation_status": self.validation_status().to_json_value(),
                "batch_shaping": batch_shaping_value(self.batch_shaping()),
                "phase_timings": phase_timings_value(self.phase_timings()),
                "report": report.to_json_value(),
            }),
            Self::Failed(report) => json!({
                "kind": "failed",
                "output_name": self.output_name(),
                "target_table": target_table_value(self.target_table()),
                "load_mode": load_mode(self.load_mode()),
                "connection_source": connection_source(self.connection_source()),
                "output_row_count": self.output_row_count().to_json_value(),
                "target_row_count": self.target_row_count().to_json_value(),
                "validation_status": self.validation_status().to_json_value(),
                "batch_shaping": batch_shaping_value(self.batch_shaping()),
                "phase_timings": phase_timings_value(self.phase_timings()),
                "failure": report.to_json_value(),
            }),
            Self::Skipped(report) => json!({
                "kind": "skipped",
                "output_name": self.output_name(),
                "target_table": target_table_value(self.target_table()),
                "load_mode": load_mode(self.load_mode()),
                "connection_source": connection_source(self.connection_source()),
                "output_row_count": self.output_row_count().to_json_value(),
                "target_row_count": self.target_row_count().to_json_value(),
                "validation_status": self.validation_status().to_json_value(),
                "batch_shaping": batch_shaping_value(self.batch_shaping()),
                "phase_timings": phase_timings_value(self.phase_timings()),
                "skipped": report.to_json_value(),
            }),
        }
    }
}

impl MssqlWorkflowWriteReport {
    /// Returns the SQL Server workflow report as a JSON-compatible Python shape.
    #[must_use]
    pub fn to_json_value(&self) -> Value {
        json!({
            "output_count": self.len(),
            "all_succeeded": self.all_succeeded(),
            "succeeded_count": self.succeeded_count(),
            "failed_count": self.failed_count(),
            "skipped_count": self.skipped_count(),
            "outputs": self.outputs()
                .iter()
                .map(MssqlOutputWriteStatus::to_json_value)
                .collect::<Vec<_>>(),
        })
    }
}

impl MssqlWriteFailureReport {
    /// Returns one failed workflow output report as a JSON-compatible Python shape.
    #[must_use]
    pub fn to_json_value(&self) -> Value {
        json!({
            "output_name": self.output_name(),
            "target_table": target_table_value(self.target().table()),
            "load_mode": load_mode(self.target().load_mode()),
            "connection_source": connection_source(self.target().connection_source()),
            "error": self.error(),
            "context": self.context().map(MssqlWriteFailureContext::to_json_value),
            "output_row_count": self.output_row_count().to_json_value(),
            "target_row_count": self.target_row_count().to_json_value(),
            "validation_status": self.validation_status().to_json_value(),
            "batch_shaping": batch_shaping_value(self.batch_shaping()),
            "phase_timings": phase_timings_value(self.phase_timings()),
        })
    }
}

impl MssqlWriteFailureContext {
    /// Returns phase-aware write failure context as a JSON-compatible Python shape.
    #[must_use]
    pub fn to_json_value(&self) -> Value {
        json!({
            "phase": write_phase(self.phase()),
            "output_name": self.output_name(),
            "target_table": target_table_value(self.target_table()),
            "load_mode": load_mode(self.load_mode()),
            "connection_source": connection_source(self.connection_source()),
            "connection": {
                "display_label": self.connection().display_label(),
            },
            "write_stats": self.stats().to_json_value(),
            "output_row_count": self.output_row_count().to_json_value(),
            "target_row_count_before_write": self.target_row_count_before_write().to_json_value(),
            "target_row_count_after_write": self.target_row_count_after_write().to_json_value(),
            "target_row_count": self.target_row_count().to_json_value(),
            "validation_status": self.validation_status().to_json_value(),
            "batch_shaping": batch_shaping_value(self.batch_shaping()),
            "partial_write_possible": self.partial_write_possible(),
            "cleanup": cleanup_status(self.cleanup()),
            "cleanup_error": self.cleanup_error(),
            "diagnostics": self.diagnostics()
                .iter()
                .map(write_diagnostic_value)
                .collect::<Vec<_>>(),
            "phase_timings": phase_timings_value(self.phase_timings()),
            "report": self.report().to_json_value(),
        })
    }
}

impl MssqlWriteSkippedReport {
    /// Returns one skipped workflow output report as a JSON-compatible Python shape.
    #[must_use]
    pub fn to_json_value(&self) -> Value {
        json!({
            "output_name": self.output_name(),
            "target_table": target_table_value(self.target().table()),
            "load_mode": load_mode(self.target().load_mode()),
            "connection_source": connection_source(self.target().connection_source()),
            "reason": skipped_reason_value(self.reason()),
            "output_row_count": self.output_row_count().to_json_value(),
            "target_row_count": self.target_row_count().to_json_value(),
            "validation_status": self.validation_status().to_json_value(),
            "batch_shaping": batch_shaping_value(self.batch_shaping()),
            "phase_timings": phase_timings_value(self.phase_timings()),
            "execution_profile": Value::Null,
        })
    }
}

impl WriteAllReport {
    /// Returns the write-all report as a JSON-compatible Python shape.
    #[must_use]
    pub fn to_json_value(&self) -> Value {
        json!({
            "workflow": self.workflow().to_json_value(),
            "cache": self.cache().to_json_value(),
            "sources": self.sources()
                .iter()
                .map(DeltaSourceReport::to_json_value)
                .collect::<Vec<_>>(),
            "phase_timings": phase_timings_value(self.phase_timings()),
            "operation_timeline": self
                .operation_timeline()
                .map(OperationTimeline::to_json_value),
            "output_count": self.len(),
            "all_succeeded": self.all_succeeded(),
            "succeeded_count": self.succeeded_count(),
            "failed_count": self.failed_count(),
            "skipped_count": self.skipped_count(),
        })
    }

    /// Returns a Chrome Trace Event JSON document for this profiled write-all call.
    #[must_use]
    pub fn to_trace_event_json_value(&self) -> Option<Value> {
        let mut trace = self.operation_timeline()?.to_trace_event_json_value();
        let Value::Object(document) = &mut trace else {
            return None;
        };
        document.insert(
            "delta_funnel_write_all_report".to_owned(),
            self.to_json_value(),
        );
        Some(trace)
    }
}

impl WriteAllCacheReport {
    /// Returns write-all cache metadata as a JSON-compatible Python shape.
    #[must_use]
    pub fn to_json_value(&self) -> Value {
        match self {
            Self::Disabled => json!({
                "kind": "disabled",
                "reason": null,
                "aliases": [],
                "skipped_candidates": [],
            }),
            Self::NoCache {
                reason,
                skipped_candidates,
            } => json!({
                "kind": "no_cache",
                "reason": no_cache_reason(*reason),
                "aliases": [],
                "skipped_candidates": skipped_candidates
                    .iter()
                    .map(WriteAllCacheCandidateSkip::to_json_value)
                    .collect::<Vec<_>>(),
            }),
            Self::CacheAliases {
                aliases,
                skipped_candidates,
            } => json!({
                "kind": "cache_aliases",
                "reason": null,
                "aliases": aliases
                    .iter()
                    .map(WriteAllCacheAliasReport::to_json_value)
                    .collect::<Vec<_>>(),
                "skipped_candidates": skipped_candidates
                    .iter()
                    .map(WriteAllCacheCandidateSkip::to_json_value)
                    .collect::<Vec<_>>(),
            }),
        }
    }
}

impl WriteAllCacheAliasReport {
    /// Returns one selected write-all cache alias as a JSON-compatible Python shape.
    #[must_use]
    pub fn to_json_value(&self) -> Value {
        match self.status() {
            WriteAllCacheAliasStatus::Selected => json!({
                "table_id": self.table_id(),
                "alias": self.alias(),
                "output_indexes": self.output_indexes(),
                "status": self.status().as_str(),
            }),
            WriteAllCacheAliasStatus::MaterializedAndRestored
            | WriteAllCacheAliasStatus::Failed => json!({
                "table_id": self.table_id(),
                "alias": self.alias(),
                "output_indexes": self.output_indexes(),
                "status": self.status().as_str(),
                "phase_timings": phase_timings_value(self.phase_timings()),
                "failed_phase": self.failed_phase(),
                "execution_profile": self
                    .execution_profile()
                    .map(QueryExecutionProfile::to_json_value),
            }),
        }
    }
}

impl WriteAllCacheFailure {
    /// Returns cache failure context as a JSON-compatible Python shape.
    #[must_use]
    pub fn to_json_value(&self) -> Value {
        json!({
            "aliases": self
                .aliases()
                .iter()
                .map(WriteAllCacheAliasReport::to_json_value)
                .collect::<Vec<_>>(),
            "primary_failed_alias_table_id": self.primary_failed_alias_table_id(),
            "workflow": self.workflow().map(MssqlWorkflowWriteReport::to_json_value),
            "operation_timeline": self
                .operation_timeline()
                .map(OperationTimeline::to_json_value),
        })
    }
}

impl WriteAllCacheCandidateSkip {
    /// Returns one skipped write-all cache candidate as a JSON-compatible Python shape.
    #[must_use]
    pub fn to_json_value(&self) -> Value {
        json!({
            "table_id": self.table_id(),
            "alias": self.alias(),
            "reason": cache_candidate_skip_reason(self.reason()),
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

fn connection_source(source: crate::MssqlConnectionSource) -> &'static str {
    match source {
        crate::MssqlConnectionSource::TargetOverride => "target_override",
        crate::MssqlConnectionSource::ContextDefault => "context_default",
    }
}

fn cleanup_status(status: MssqlTargetCleanupStatus) -> &'static str {
    match status {
        MssqlTargetCleanupStatus::NotApplicable => "not_applicable",
        MssqlTargetCleanupStatus::NotAttempted => "not_attempted",
        MssqlTargetCleanupStatus::Succeeded => "succeeded",
        MssqlTargetCleanupStatus::Failed => "failed",
    }
}

fn write_phase(phase: MssqlWritePhase) -> &'static str {
    match phase {
        MssqlWritePhase::QueryDataFramePlanning => "query_dataframe_planning",
        MssqlWritePhase::QueryPhysicalPlanning => "query_physical_planning",
        MssqlWritePhase::QueryStreamSetup => "query_stream_setup",
        MssqlWritePhase::Connect => "connect",
        MssqlWritePhase::PrepareTargetLifecycle => "prepare_target_lifecycle",
        MssqlWritePhase::InitializeWriter => "initialize_writer",
        MssqlWritePhase::PollBatchStream => "poll_batch_stream",
        MssqlWritePhase::ValidateBatchSchema => "validate_batch_schema",
        MssqlWritePhase::WriteBatch => "write_batch",
        MssqlWritePhase::Finalize => "finalize",
        MssqlWritePhase::Validation => "validation",
        MssqlWritePhase::SwapTarget => "swap_target",
        MssqlWritePhase::Cleanup => "cleanup",
    }
}

fn write_diagnostic_value(
    diagnostic: &crate::report::sql_server::write::MssqlWriteDiagnostic,
) -> Value {
    json!({
        "severity": diagnostic_severity(diagnostic.severity()),
        "code": diagnostic_code(diagnostic.code()),
        "message": diagnostic.message(),
        "field": diagnostic.field().map(|field| json!({
            "index": field.index(),
            "name": field.name(),
        })),
        "row": diagnostic.row(),
    })
}

fn diagnostic_severity(severity: arrow_tiberius::DiagnosticSeverity) -> &'static str {
    match severity {
        arrow_tiberius::DiagnosticSeverity::Warning => "warning",
        arrow_tiberius::DiagnosticSeverity::Error => "error",
    }
}

fn diagnostic_code(code: arrow_tiberius::DiagnosticCode) -> &'static str {
    match code {
        arrow_tiberius::DiagnosticCode::UnsupportedArrowType => "unsupported_arrow_type",
        arrow_tiberius::DiagnosticCode::LossyConversionRequiresPolicy => {
            "lossy_conversion_requires_policy"
        }
        arrow_tiberius::DiagnosticCode::PolicyApplied => "policy_applied",
        arrow_tiberius::DiagnosticCode::IdentifierInvalid => "identifier_invalid",
        arrow_tiberius::DiagnosticCode::IdentifierTooLong => "identifier_too_long",
        arrow_tiberius::DiagnosticCode::DecimalOutOfRange => "decimal_out_of_range",
        arrow_tiberius::DiagnosticCode::IntegerOutOfRange => "integer_out_of_range",
        arrow_tiberius::DiagnosticCode::TimestampOutOfRange => "timestamp_out_of_range",
        arrow_tiberius::DiagnosticCode::TimezoneUnsupported => "timezone_unsupported",
        arrow_tiberius::DiagnosticCode::SchemaMismatch => "schema_mismatch",
        arrow_tiberius::DiagnosticCode::BackendUnavailable => "backend_unavailable",
        arrow_tiberius::DiagnosticCode::ProfileDependentConversion => {
            "profile_dependent_conversion"
        }
        arrow_tiberius::DiagnosticCode::ObservedDataRequired => "observed_data_required",
        arrow_tiberius::DiagnosticCode::ValueConversionUnsupported => {
            "value_conversion_unsupported"
        }
        arrow_tiberius::DiagnosticCode::ValueTypeMismatch => "value_type_mismatch",
        arrow_tiberius::DiagnosticCode::NullInNonNullableColumn => "null_in_non_nullable_column",
        arrow_tiberius::DiagnosticCode::NonFiniteFloat => "non_finite_float",
        arrow_tiberius::DiagnosticCode::ValueTooLong => "value_too_long",
        arrow_tiberius::DiagnosticCode::RowIndexOutOfBounds => "row_index_out_of_bounds",
        arrow_tiberius::DiagnosticCode::DirectEncodingInvalidPayload => {
            "direct_encoding_invalid_payload"
        }
        arrow_tiberius::DiagnosticCode::DirectEncodingUnsupportedMapping => {
            "direct_encoding_unsupported_mapping"
        }
        arrow_tiberius::DiagnosticCode::DirectEncodingUnsupportedBatch => {
            "direct_encoding_unsupported_batch"
        }
        _ => "unknown",
    }
}

fn batch_shaping_value(report: crate::MssqlBatchShapingReport) -> Value {
    json!({
        "status": report.status().to_json_value(),
        "input_batches": report.input_batches(),
        "input_rows": report.input_rows(),
        "output_batches": report.output_batches(),
        "output_rows": report.output_rows(),
    })
}

fn provider_read_stats_value(stats: &DeltaProviderReadStatsSnapshot) -> Value {
    json!({
        "source_name": stats.source_name,
        "snapshot_version": stats.snapshot_version,
        "reader_backend": reader_backend(stats.reader_backend),
        "scan_metadata_exhausted": stats.scan_metadata_exhausted,
        "scan_partitions_planned": stats.scan_partitions_planned,
        "files_planned": stats.files_planned,
        "approximate_files_filtered_during_planning": stats.files_filtered_during_planning,
        "estimated_rows": stats.estimated_rows,
        "estimated_bytes": stats.estimated_bytes,
        "parquet_data_file_range_get_operations": stats.parquet_data_file_range_get_operations,
        "parquet_data_file_full_get_operations": stats.parquet_data_file_full_get_operations,
        "parquet_data_file_bytes_received": stats.parquet_data_file_bytes_received,
        "parquet_data_file_opened_bytes": stats.parquet_data_file_opened_bytes,
        "datafusion_output_batch_size": stats.datafusion_output_batch_size,
        "scan_partitions_started": stats.scan_partitions_started,
        "scan_partitions_completed": stats.scan_partitions_completed,
        "files_started": stats.files_started,
        "files_completed": stats.files_completed,
        "dynamic_partition_files_pruned": stats.dynamic_partition_files_pruned,
        "dynamic_partition_files_kept": stats.dynamic_partition_files_kept,
        "dynamic_filters_received": stats.dynamic_filters_received,
        "dynamic_filters_accepted": stats.dynamic_filters_accepted,
        "dynamic_filters_unsupported": stats.dynamic_filters_unsupported,
        "dynamic_filter_snapshots": stats.dynamic_filter_snapshots,
        "dynamic_partition_files_not_pruned_missing_metadata": stats.dynamic_partition_files_not_pruned_missing_metadata,
        "dynamic_partition_files_not_pruned_unsupported_expression": stats.dynamic_partition_files_not_pruned_unsupported_expression,
        "batches_produced": stats.batches_produced,
        "rows_produced": stats.rows_produced,
        "deletion_vector_payloads_loaded": stats.deletion_vector_payloads_loaded,
        "deletion_vectors_applied": stats.deletion_vectors_applied,
        "deletion_vector_rows_deleted": stats.deletion_vector_rows_deleted,
        "deletion_vector_failures": stats.deletion_vector_failures,
        "deletion_vector_rejections": stats.deletion_vector_rejections,
    })
}

fn execution_metric_value(value: &QueryExecutionMetricValue) -> (Value, Value) {
    let no_components = Value::Null;

    match value {
        QueryExecutionMetricValue::Count(value)
        | QueryExecutionMetricValue::Bytes(value)
        | QueryExecutionMetricValue::Nanoseconds(value)
        | QueryExecutionMetricValue::Gauge(value)
        | QueryExecutionMetricValue::Custom(value) => (json!(value), no_components),
        QueryExecutionMetricValue::TimestampNanoseconds(value) => (json!(value), no_components),
        QueryExecutionMetricValue::Pruning {
            pruned,
            matched,
            fully_matched,
        } => (
            Value::Null,
            json!({
                "pruned": pruned,
                "matched": matched,
                "fully_matched": fully_matched,
            }),
        ),
        QueryExecutionMetricValue::Ratio { part, total } => (
            Value::Null,
            json!({
                "part": part,
                "total": total,
            }),
        ),
    }
}

fn skipped_reason_value(reason: &MssqlWriteSkippedReason) -> Value {
    match reason {
        MssqlWriteSkippedReason::PreviousOutputFailed { failed_output_name } => json!({
            "kind": "previous_output_failed",
            "failed_output_name": failed_output_name,
        }),
    }
}

fn reader_backend(backend: DeltaProviderReaderBackend) -> &'static str {
    match backend {
        DeltaProviderReaderBackend::OfficialKernel => "official_kernel",
        DeltaProviderReaderBackend::NativeAsync => "native_async",
    }
}

fn no_cache_reason(reason: WriteAllNoCacheReason) -> &'static str {
    match reason {
        WriteAllNoCacheReason::FewerThanTwoOutputs => "fewer_than_two_outputs",
        WriteAllNoCacheReason::NoSharedRegisteredDerivedAlias => {
            "no_shared_registered_derived_alias"
        }
        WriteAllNoCacheReason::AmbiguousSharedDerivedAlias => "ambiguous_shared_derived_alias",
    }
}

fn cache_candidate_skip_reason(reason: &WriteAllCacheCandidateSkipReason) -> Value {
    match reason {
        WriteAllCacheCandidateSkipReason::NotShared { output_count } => json!({
            "kind": "not_shared",
            "output_count": output_count,
        }),
        WriteAllCacheCandidateSkipReason::MissingSqlText => json!({
            "kind": "missing_sql_text",
        }),
        WriteAllCacheCandidateSkipReason::IncompleteLineage => json!({
            "kind": "incomplete_lineage",
        }),
        WriteAllCacheCandidateSkipReason::CoveredByDeeperSharedAlias { selected_table_id } => {
            json!({
                "kind": "covered_by_deeper_shared_alias",
                "selected_table_id": selected_table_id,
            })
        }
        WriteAllCacheCandidateSkipReason::AmbiguousDepth => json!({
            "kind": "ambiguous_depth",
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        error::Error,
        fs,
        path::PathBuf,
        sync::Arc,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use async_trait::async_trait;
    use futures_util::stream;
    use serde_json::{Value, json};

    use super::*;
    use crate::MssqlWorkflowOutputWriter;
    use crate::{
        DeltaFunnelSession, DeltaProtocolReport, DeltaProviderScanExecutionOptions,
        DeltaProviderSchedulingReport, DeltaSourceConfig, MssqlConnectionConfig,
        MssqlOutputBatchStream, MssqlOutputTarget, MssqlOutputWriteJob, MssqlSchemaPlanOptions,
        MssqlTargetConfig, MssqlTargetOutputPlan, MssqlTargetResolutionContext,
        MssqlWorkflowWriteOptions, MssqlWriteBackend, OutputWritePlan, QueryOptions,
        ResolvedMssqlTarget, SessionOptions, ValidationOptions, plan_mssql_target_for_output,
        write_mssql_outputs_with_writer,
    };
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use arrow_tiberius::PlanOptions;

    type TestResult<T> = Result<T, Box<dyn Error + Send + Sync + 'static>>;

    struct FakeWorkflowWriter {
        outcomes: VecDeque<Result<MssqlWriteReport, crate::DeltaFunnelError>>,
    }

    impl FakeWorkflowWriter {
        fn new(outcomes: Vec<Result<MssqlWriteReport, crate::DeltaFunnelError>>) -> Self {
            Self {
                outcomes: outcomes.into(),
            }
        }
    }

    #[async_trait]
    impl MssqlWorkflowOutputWriter for FakeWorkflowWriter {
        async fn write_output(
            &mut self,
            _output_schema: SchemaRef,
            _resolved_target: ResolvedMssqlTarget,
            _schema_options: MssqlSchemaPlanOptions,
            _batches: MssqlOutputBatchStream,
            _write_backend: MssqlWriteBackend,
            _validation_options: ValidationOptions,
            _reporter: Option<&crate::progress::ProgressReporter>,
            _stage_context: crate::profiling::OperationStageContext<'_>,
        ) -> Result<MssqlWriteReport, crate::DeltaFunnelError> {
            self.outcomes.pop_front().ok_or_else(|| {
                crate::DeltaFunnelError::MssqlWorkflowPlanning {
                    message: "missing fake writer outcome".to_owned(),
                }
            })?
        }
    }

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

    #[test]
    fn operation_timeline_trace_uses_the_total_wall_clock_as_its_origin() {
        let execution = TimelineSpan::new(
            1,
            None,
            "preview_execute_collect",
            "delta_funnel.phase",
            Duration::from_micros(1_700_000),
            Duration::from_micros(6_200_000),
            crate::TimelineSpanStatus::Completed,
            crate::TimelineSpanTimeSemantics::WallClock,
        )
        .with_attribute("rows", json!(10_000));
        let timeline = OperationTimeline::new(
            "preview",
            crate::TimelineSpanStatus::Completed,
            Duration::from_micros(8_000_000),
            vec![execution],
        );

        let trace = timeline.to_trace_event_json_value();
        let events = trace["traceEvents"]
            .as_array()
            .expect("trace events should be an array");

        assert_eq!(events.len(), 5);
        assert!(
            events
                .iter()
                .filter_map(|event| event["tid"].as_u64())
                .all(|lane| lane > 0)
        );
        assert_eq!(events[2]["tid"], 1);
        assert_eq!(events[2]["name"], "preview");
        assert_eq!(events[2]["ts"], 0);
        assert_eq!(events[2]["dur"], 8_000_000);
        assert_eq!(events[4]["name"], "preview_execute_collect");
        assert_eq!(events[4]["tid"], 2);
        assert_eq!(events[4]["ts"], 1_700_000);
        assert_eq!(events[4]["dur"], 6_200_000);
        assert_eq!(events[4]["args"]["time_semantics"], "wall_clock");
        assert_eq!(events[4]["args"]["attributes"]["rows"], 10_000);
        assert_eq!(trace["delta_funnel_timeline"], timeline.to_json_value());
    }

    #[test]
    fn operation_timeline_trace_reuses_lanes_for_repeated_batch_steps() {
        let spans = [10_u64, 30]
            .into_iter()
            .enumerate()
            .map(|(index, start)| {
                TimelineSpan::new(
                    crate::usize_to_u64_saturating(index.saturating_add(1)),
                    None,
                    "Write batch",
                    "delta_funnel.write.batch",
                    Duration::from_micros(start),
                    Duration::from_micros(5),
                    crate::TimelineSpanStatus::Completed,
                    crate::TimelineSpanTimeSemantics::WallClock,
                )
                .with_track_name("SQL Server batch writes")
            })
            .collect();
        let timeline = OperationTimeline::new(
            "write",
            crate::TimelineSpanStatus::Completed,
            Duration::from_micros(50),
            spans,
        );

        let trace = timeline.to_trace_event_json_value();
        let events = trace["traceEvents"]
            .as_array()
            .expect("trace events should be an array");
        let lane_metadata = events
            .iter()
            .filter(|event| {
                event["name"] == "thread_name" && event["args"]["name"] == "SQL Server batch writes"
            })
            .count();
        let write_lanes = events
            .iter()
            .filter(|event| event["name"] == "Write batch")
            .filter_map(|event| event["tid"].as_u64())
            .collect::<Vec<_>>();

        assert_eq!(lane_metadata, 1);
        assert_eq!(write_lanes.len(), 2);
        assert_eq!(write_lanes[0], write_lanes[1]);
    }

    #[test]
    fn execution_profile_trace_json_exposes_partition_spans_and_embeds_profile() {
        let metric = |name, partition, value| {
            QueryExecutionMetric::new(
                name,
                crate::QueryExecutionMetricCategory::Summary,
                Some(partition),
                None,
                value,
            )
        };
        let operator = QueryExecutionOperatorProfile::new(
            4,
            Some(3),
            "FilterExec",
            2,
            true,
            Vec::new(),
            vec![
                metric(
                    "start_timestamp",
                    0,
                    QueryExecutionMetricValue::TimestampNanoseconds(Some(1_000_000)),
                ),
                metric(
                    "end_timestamp",
                    0,
                    QueryExecutionMetricValue::TimestampNanoseconds(Some(1_750_500)),
                ),
                metric("output_rows", 0, QueryExecutionMetricValue::Count(42)),
                metric(
                    "start_timestamp",
                    1,
                    QueryExecutionMetricValue::TimestampNanoseconds(Some(900_000)),
                ),
                metric(
                    "end_timestamp",
                    1,
                    QueryExecutionMetricValue::TimestampNanoseconds(Some(2_000_000)),
                ),
                metric("output_rows", 1, QueryExecutionMetricValue::Count(7)),
            ],
            None,
        );
        let profile = QueryExecutionProfile::preview(
            crate::QueryExecutionOutcome::Success,
            20,
            vec![operator],
        );

        let trace = profile.to_trace_event_json_value();
        let events = trace["traceEvents"]
            .as_array()
            .expect("trace events should be an array");

        assert_eq!(events.len(), 5);
        assert_eq!(events[0]["ph"], "M");
        assert_eq!(
            events[1]["args"]["name"],
            "FilterExec [node 4, partition 0]"
        );
        assert_eq!(events[2]["ph"], "X");
        assert_eq!(events[2]["ts"], 100.0);
        assert_eq!(events[2]["dur"], 750.5);
        assert_eq!(events[2]["args"]["metrics"][0]["name"], "output_rows");
        assert_eq!(
            events[3]["args"]["name"],
            "FilterExec [node 4, partition 1]"
        );
        assert_eq!(events[4]["ts"], 0.0);
        assert_eq!(events[4]["dur"], 1100.0);
        assert_eq!(trace["delta_funnel_profile"], profile.to_json_value());
    }

    #[test]
    fn execution_profile_trace_json_handles_timestamp_boundaries_and_invalid_spans() {
        let metric = |name, partition, value| {
            QueryExecutionMetric::new(
                name,
                crate::QueryExecutionMetricCategory::Summary,
                Some(partition),
                None,
                QueryExecutionMetricValue::TimestampNanoseconds(Some(value)),
            )
        };
        let operator = QueryExecutionOperatorProfile::new(
            0,
            None,
            "BoundaryExec",
            5,
            true,
            Vec::new(),
            vec![
                metric("start_timestamp", 0, i64::MIN),
                metric("end_timestamp", 0, i64::MIN),
                metric("start_timestamp", 1, i64::MIN),
                metric("end_timestamp", 1, i64::MAX),
                metric("start_timestamp", 2, i64::MAX),
                metric("end_timestamp", 2, i64::MAX),
                metric("start_timestamp", 3, 0),
                metric("start_timestamp", 4, 1),
                metric("end_timestamp", 4, 0),
            ],
            None,
        );
        let profile = QueryExecutionProfile::preview(
            crate::QueryExecutionOutcome::Success,
            20,
            vec![operator],
        );

        let trace = profile.to_trace_event_json_value();
        let complete_events = trace["traceEvents"]
            .as_array()
            .expect("trace events should be an array")
            .iter()
            .filter(|event| event["ph"] == "X")
            .collect::<Vec<_>>();
        let event_for_partition = |partition| {
            complete_events
                .iter()
                .find(|event| event["args"]["partition"] == partition)
                .copied()
                .expect("partition should have a complete event")
        };

        assert_eq!(complete_events.len(), 3);
        assert_eq!(event_for_partition(0)["ts"], 0.0);
        assert_eq!(event_for_partition(1)["dur"], nanos_to_micros(u64::MAX));
        assert_eq!(event_for_partition(2)["ts"], nanos_to_micros(u64::MAX));
    }

    #[test]
    fn operator_lifecycles_align_to_and_are_clamped_by_the_operation_wall_clock() {
        let metric = |name, value| {
            QueryExecutionMetric::new(
                name,
                crate::QueryExecutionMetricCategory::Summary,
                Some(3),
                None,
                value,
            )
        };
        let operator = QueryExecutionOperatorProfile::new(
            4,
            Some(2),
            "FilterExec",
            4,
            true,
            Vec::new(),
            vec![
                metric(
                    "start_timestamp",
                    QueryExecutionMetricValue::TimestampNanoseconds(Some(1_100_000)),
                ),
                metric(
                    "end_timestamp",
                    QueryExecutionMetricValue::TimestampNanoseconds(Some(2_000_000)),
                ),
                metric(
                    "elapsed_compute",
                    QueryExecutionMetricValue::Nanoseconds(42_000),
                ),
            ],
            None,
        );
        let profile =
            QueryExecutionProfile::preview(crate::QueryExecutionOutcome::Error, 20, vec![operator]);

        let spans = profile.operator_lifecycle_timeline_spans(7, 1_000_000, 800);

        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].id(), 7);
        assert_eq!(spans[0].name(), "FilterExec");
        assert_eq!(spans[0].track_name(), "FilterExec [node 4, partition 3]");
        assert_eq!(spans[0].start_offset_micros(), 100);
        assert_eq!(spans[0].duration_micros(), 700);
        assert_eq!(spans[0].status(), crate::TimelineSpanStatus::Failed);
        assert_eq!(
            spans[0].time_semantics(),
            crate::TimelineSpanTimeSemantics::Lifecycle
        );
        assert_eq!(spans[0].attributes()["node_id"], 4);
        assert_eq!(spans[0].attributes()["parent_node_id"], 2);
        assert_eq!(
            spans[0].attributes()["metrics"][0]["name"],
            "elapsed_compute"
        );
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
        assert!(value["outputs"][0].get("execution_profile").is_none());
        assert_json_safe(&value)?;
        assert_no_secret_or_raw_sql_text(&value);

        Ok(())
    }

    #[test]
    fn execute_write_report_json_exposes_stats_counts_and_validation() -> TestResult<()> {
        let output_plan = output_plan()?;
        let report = MssqlWriteReport::from_output_plan(
            &output_plan,
            42,
            3,
            125,
            false,
            MssqlTargetCleanupStatus::NotApplicable,
        )
        .with_target_delta_validation(
            RowCount::exact(10),
            RowCount::exact(52),
            ValidationStatus::passed(),
            PhaseTimingReport::completed("mssql_target_validation", Duration::from_micros(7)),
        );

        let value = report.to_json_value();

        assert_eq!(value["run_mode"], "execute");
        assert!(value.get("status").is_none());
        assert_eq!(value["output_name"], "orders_output");
        assert_eq!(value["target_table"]["schema"], "dbo");
        assert_eq!(value["target_table"]["table"], "orders");
        assert_eq!(value["connection_source"], "context_default");
        assert_eq!(value["connection"]["display_label"], "warehouse");
        assert_eq!(value["output_schema"][0]["name"], "id");
        assert_eq!(
            value["output_row_count"],
            json!({"kind": "exact", "value": 42})
        );
        assert_eq!(
            value["target_row_count_before_write"],
            json!({"kind": "exact", "value": 10})
        );
        assert_eq!(
            value["target_row_count_after_write"],
            json!({"kind": "exact", "value": 52})
        );
        assert_eq!(
            value["validation_status"],
            json!({"kind": "passed", "reason": null})
        );
        assert_eq!(value["batch_shaping"]["input_batches"], 3);
        assert_eq!(value["batch_shaping"]["input_rows"], 42);
        assert_eq!(value["write_stats"]["rows_written"], 42);
        assert_eq!(value["write_stats"]["batches_written"], 3);
        assert_eq!(value["write_stats"]["elapsed_ms"], 125);
        assert_eq!(report.execution_profile(), None);
        assert!(value["execution_profile"].is_null());
        assert_eq!(value["cleanup"], "not_applicable");
        assert_json_safe(&value)?;
        assert_no_secret_or_raw_sql_text(&value);

        Ok(())
    }

    #[test]
    fn batch_validation_report_json_exposes_safe_target_context() -> TestResult<()> {
        let value =
            MssqlOutputBatchValidationReport::from_output_plan(&output_plan()?).to_json_value();

        assert_eq!(value["output_name"], "orders_output");
        assert_eq!(
            value["target_table"],
            json!({"schema": "dbo", "table": "orders"})
        );
        assert_eq!(value["connection_source"], "context_default");
        assert_eq!(value["connection"]["display_label"], "warehouse");
        assert_json_safe(&value)?;
        assert_no_secret_or_raw_sql_text(&value);

        Ok(())
    }

    #[test]
    fn workflow_output_status_json_wraps_successful_write_report() -> TestResult<()> {
        let output_plan = output_plan()?;
        let report = MssqlWriteReport::from_output_plan(
            &output_plan,
            7,
            1,
            25,
            false,
            MssqlTargetCleanupStatus::NotApplicable,
        );

        let value = MssqlOutputWriteStatus::Succeeded(report).to_json_value();

        assert_eq!(value["kind"], "succeeded");
        assert_eq!(value["output_name"], "orders_output");
        assert_eq!(
            value["output_row_count"],
            json!({"kind": "exact", "value": 7})
        );
        assert_eq!(value["report"]["write_stats"]["rows_written"], 7);
        assert!(value.get("execution_profile").is_none());
        assert!(value["report"]["execution_profile"].is_null());
        assert_json_safe(&value)?;
        assert_no_secret_or_raw_sql_text(&value);

        Ok(())
    }

    #[test]
    fn failure_context_json_exposes_structured_context_without_success_status() -> TestResult<()> {
        let output_plan = output_plan()?;
        let context = MssqlWriteFailureContext::from_output_plan(
            &output_plan,
            MssqlWritePhase::WriteBatch,
            4,
            1,
            25,
            true,
            MssqlTargetCleanupStatus::Failed,
        );

        let value = context.to_json_value();

        assert_eq!(value["phase"], "write_batch");
        assert_eq!(
            value["output_row_count"],
            json!({"kind": "partial", "value": 4})
        );
        assert_eq!(value["partial_write_possible"], true);
        assert_eq!(value["cleanup"], "failed");
        assert!(value.get("execution_profile").is_none());
        assert!(value["report"]["execution_profile"].is_null());
        assert!(value["report"].get("status").is_none());
        assert_json_safe(&value)?;
        assert_no_secret_or_raw_sql_text(&value);

        Ok(())
    }

    #[tokio::test]
    async fn workflow_json_covers_real_failed_and_skipped_statuses() -> TestResult<()> {
        let first = output_plan_named("first_output")?;
        let second = output_plan_named("second_output")?;
        let third = output_plan_named("third_output")?;
        let first_report = MssqlWriteReport::from_output_plan(
            &first,
            7,
            1,
            25,
            false,
            MssqlTargetCleanupStatus::NotApplicable,
        );
        let failure_context = MssqlWriteFailureContext::from_output_plan(
            &second,
            MssqlWritePhase::WriteBatch,
            4,
            1,
            25,
            true,
            MssqlTargetCleanupStatus::NotApplicable,
        );
        let failure = crate::DeltaFunnelError::MssqlWritePhase {
            context: Box::new(failure_context),
            message: "failed to write output".to_owned(),
        };
        let writer = FakeWorkflowWriter::new(vec![Ok(first_report), Err(failure)]);

        let report = write_mssql_outputs_with_writer(
            vec![job(first)?, job(second)?, job(third)?],
            MssqlWorkflowWriteOptions::default(),
            writer,
        )
        .await?;

        let value = report.to_json_value();

        assert_eq!(value["succeeded_count"], 1);
        assert_eq!(value["failed_count"], 1);
        assert_eq!(value["skipped_count"], 1);
        assert_eq!(value["outputs"][0]["kind"], "succeeded");
        assert_eq!(value["outputs"][1]["kind"], "failed");
        assert_eq!(
            value["outputs"][1]["failure"]["context"]["phase"],
            "write_batch"
        );
        assert_eq!(
            value["outputs"][1]["failure"]["context"]["output_row_count"],
            json!({"kind": "partial", "value": 4})
        );
        assert_eq!(value["outputs"][2]["kind"], "skipped");
        assert!(value["outputs"][2].get("execution_profile").is_none());
        assert!(value["outputs"][2]["skipped"]["execution_profile"].is_null());
        assert_eq!(
            value["outputs"][2]["skipped"]["reason"],
            json!({
                "kind": "previous_output_failed",
                "failed_output_name": "second_output"
            })
        );
        assert_json_safe(&value)?;
        assert_no_secret_or_raw_sql_text(&value);

        Ok(())
    }

    #[test]
    fn write_all_cache_json_preserves_decision_aliases_and_skip_reasons() -> TestResult<()> {
        let value = WriteAllCacheReport::CacheAliases {
            aliases: vec![WriteAllCacheAliasReport::executed(
                9,
                "shared_orders",
                vec![0, 2],
                WriteAllCacheAliasStatus::MaterializedAndRestored,
                vec![PhaseTimingReport::completed(
                    "cache_alias_restore",
                    Duration::from_micros(3),
                )],
                None,
            )],
            skipped_candidates: vec![
                WriteAllCacheCandidateSkip::new(
                    7,
                    "lonely_orders",
                    WriteAllCacheCandidateSkipReason::NotShared { output_count: 1 },
                ),
                WriteAllCacheCandidateSkip::new(
                    8,
                    "missing_sql_orders",
                    WriteAllCacheCandidateSkipReason::MissingSqlText,
                ),
            ],
        }
        .to_json_value();

        assert_eq!(value["kind"], "cache_aliases");
        assert_eq!(value["aliases"][0]["alias"], "shared_orders");
        assert_eq!(value["aliases"][0]["status"], "materialized_and_restored");
        assert!(value["aliases"][0]["execution_profile"].is_null());
        assert_eq!(
            value["aliases"][0]["phase_timings"][0]["phase_name"],
            "cache_alias_restore"
        );
        assert!(value["aliases"][0]["failed_phase"].is_null());
        assert_eq!(
            value["skipped_candidates"][0]["reason"],
            json!({"kind": "not_shared", "output_count": 1})
        );
        assert_eq!(
            value["skipped_candidates"][1]["reason"],
            json!({"kind": "missing_sql_text"})
        );
        assert_json_safe(&value)?;
        assert_no_secret_or_raw_sql_text(&value);

        Ok(())
    }

    #[test]
    fn write_all_cache_alias_json_separates_plan_and_failed_execution_fields() {
        let selected = WriteAllCacheAliasReport::selected(9, "shared_orders", vec![0, 2]);
        let selected_value = selected.to_json_value();
        assert_eq!(selected.status().to_string(), "selected");
        assert!(selected.phase_timings().is_empty());
        assert_eq!(selected.failed_phase(), None);
        assert_eq!(selected.execution_profile(), None);
        assert!(selected_value.get("phase_timings").is_none());
        assert!(selected_value.get("failed_phase").is_none());
        assert!(selected_value.get("execution_profile").is_none());

        let failed = WriteAllCacheAliasReport::executed(
            9,
            "shared_orders",
            vec![0, 2],
            WriteAllCacheAliasStatus::Failed,
            vec![PhaseTimingReport::failed(
                "cache_alias_install",
                Duration::from_micros(4),
            )],
            Some("cache_alias_install"),
        );
        let failed_value = failed.to_json_value();
        assert_eq!(failed.status().to_string(), "failed");
        assert_eq!(failed_value["status"], "failed");
        assert_eq!(failed_value["failed_phase"], "cache_alias_install");
        assert!(failed_value["execution_profile"].is_null());
        assert_eq!(failed_value["phase_timings"][0]["status"]["kind"], "failed");
    }

    #[test]
    fn empty_write_all_cache_failure_has_the_exact_json_shape() {
        let failure = WriteAllCacheFailure::new(Vec::new(), None, None);

        assert_eq!(
            failure.to_json_value(),
            json!({
                "aliases": [],
                "primary_failed_alias_table_id": null,
                "workflow": null,
                "operation_timeline": null,
            })
        );
    }

    #[test]
    fn source_report_json_exposes_provider_read_stats_details() -> TestResult<()> {
        let source = DeltaSourceReport::metadata_only(
            "orders",
            "s3://user:password@example.com/tmp/orders?token=secret#debug",
            3,
            DeltaProtocolReport {
                source_name: "orders".to_owned(),
                table_uri: "s3://example.com/tmp/orders".to_owned(),
                snapshot_version: 3,
                min_reader_version: 1,
                min_writer_version: 2,
                reader_features: vec!["deletionVectors".to_owned()],
                writer_features: Vec::new(),
            },
            DeltaProviderSchedulingReport::from_options(
                QueryOptions {
                    target_partitions: Some(4),
                    output_batch_size: Some(128),
                },
                DeltaProviderScanExecutionOptions::default(),
            ),
        )
        .with_provider_read_stats(provider_read_stats_snapshot());

        let value = source.to_json_value();

        assert_eq!(value["source_uri"], "s3://example.com/tmp/orders");
        assert_eq!(
            value["protocol"]["table_uri"],
            "s3://example.com/tmp/orders"
        );
        assert_eq!(
            value["file_count"],
            json!({"kind": "exact", "value": 5, "reason": null})
        );
        assert_eq!(value["provider_read_stats_available"], true);
        assert_eq!(value["provider_stats_reason"], Value::Null);
        assert_eq!(
            value["provider_read_stats"]["reader_backend"],
            "native_async"
        );
        assert_eq!(value["provider_read_stats"]["files_planned"], 5);
        assert_eq!(
            value["provider_read_stats"]["parquet_data_file_range_get_operations"],
            4
        );
        assert_eq!(
            value["provider_read_stats"]["parquet_data_file_full_get_operations"],
            0
        );
        assert_eq!(
            value["provider_read_stats"]["parquet_data_file_bytes_received"],
            512
        );
        assert_eq!(
            value["provider_read_stats"]["parquet_data_file_opened_bytes"],
            2048
        );
        assert_eq!(
            value["provider_read_stats"]["approximate_files_filtered_during_planning"],
            8
        );
        assert_eq!(value["provider_read_stats"]["rows_produced"], 10);
        assert_eq!(
            value["provider_read_stats"]["dynamic_partition_files_pruned"],
            2
        );
        assert_json_safe(&value)?;
        assert_no_secret_or_raw_sql_text(&value);

        Ok(())
    }

    #[test]
    fn provider_read_stats_json_preserves_unavailable_parquet_io_metrics() {
        let mut stats = provider_read_stats_snapshot();
        stats.reader_backend = DeltaProviderReaderBackend::OfficialKernel;
        stats.parquet_data_file_range_get_operations = None;
        stats.parquet_data_file_full_get_operations = None;
        stats.parquet_data_file_bytes_received = None;
        stats.parquet_data_file_opened_bytes = None;

        let value = provider_read_stats_value(&stats);

        assert_eq!(value["parquet_data_file_range_get_operations"], Value::Null);
        assert_eq!(value["parquet_data_file_full_get_operations"], Value::Null);
        assert_eq!(value["parquet_data_file_bytes_received"], Value::Null);
        assert_eq!(value["parquet_data_file_opened_bytes"], Value::Null);
    }

    #[test]
    fn provider_read_stats_json_preserves_available_parquet_io_metric_zeros() {
        let mut stats = provider_read_stats_snapshot();
        stats.parquet_data_file_range_get_operations = Some(0);
        stats.parquet_data_file_full_get_operations = Some(0);
        stats.parquet_data_file_bytes_received = Some(0);
        stats.parquet_data_file_opened_bytes = Some(0);

        let value = provider_read_stats_value(&stats);

        assert_eq!(value["parquet_data_file_range_get_operations"], 0);
        assert_eq!(value["parquet_data_file_full_get_operations"], 0);
        assert_eq!(value["parquet_data_file_bytes_received"], 0);
        assert_eq!(value["parquet_data_file_opened_bytes"], 0);
    }

    fn session_with_default_connection() -> Result<DeltaFunnelSession, crate::DeltaFunnelError> {
        let connection = MssqlConnectionConfig::new(
            "server=tcp:sql.example.com;database=warehouse;user=admin;password=secret-token",
        )?
        .with_display_label("warehouse");
        DeltaFunnelSession::new(SessionOptions::new().with_default_mssql_connection(connection))
    }

    fn output_plan() -> Result<MssqlTargetOutputPlan, crate::DeltaFunnelError> {
        output_plan_with_table("orders_output", "orders")
    }

    fn output_plan_named(
        output_name: &str,
    ) -> Result<MssqlTargetOutputPlan, crate::DeltaFunnelError> {
        output_plan_with_table(output_name, format!("{output_name}_orders"))
    }

    fn output_plan_with_table(
        output_name: &str,
        table_name: impl Into<String>,
    ) -> Result<MssqlTargetOutputPlan, crate::DeltaFunnelError> {
        let connection = MssqlConnectionConfig::new(
            "server=tcp:sql.example.com;database=warehouse;user=admin;password=secret-token",
        )?
        .with_display_label("warehouse");
        let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", table_name)?);

        plan_mssql_target_for_output(
            orders_schema(),
            output_name,
            &target_config,
            Some(&connection),
            PlanOptions::default(),
        )
    }

    fn job(
        output_plan: MssqlTargetOutputPlan,
    ) -> Result<MssqlOutputWriteJob, crate::DeltaFunnelError> {
        Ok(MssqlOutputWriteJob::with_default_write_backend(
            orders_schema_ref(),
            resolved_target(output_plan)?,
            MssqlSchemaPlanOptions::default(),
            || async { Ok(stream::empty()) },
        ))
    }

    fn resolved_target(
        output_plan: MssqlTargetOutputPlan,
    ) -> Result<ResolvedMssqlTarget, crate::DeltaFunnelError> {
        let connection = MssqlConnectionConfig::new(
            "server=tcp:sql.example.com;database=warehouse;user=admin;password=secret-token",
        )?
        .with_display_label("warehouse");

        MssqlTargetConfig::new(output_plan.target_table().clone())
            .with_load_mode(output_plan.load_mode())
            .resolve(MssqlTargetResolutionContext {
                output_name: Some(output_plan.output_name()),
                default_connection: Some(&connection),
            })
    }

    fn orders_schema_ref() -> SchemaRef {
        Arc::new(orders_schema())
    }

    fn orders_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, true),
        ])
    }

    fn provider_read_stats_snapshot() -> DeltaProviderReadStatsSnapshot {
        DeltaProviderReadStatsSnapshot {
            source_name: "orders".to_owned(),
            snapshot_version: 3,
            reader_backend: DeltaProviderReaderBackend::NativeAsync,
            scan_metadata_exhausted: Some(true),
            scan_partitions_planned: 4,
            files_planned: 5,
            files_filtered_during_planning: Some(8),
            estimated_rows: Some(99),
            estimated_bytes: Some(2048),
            parquet_data_file_range_get_operations: Some(4),
            parquet_data_file_full_get_operations: Some(0),
            parquet_data_file_bytes_received: Some(512),
            parquet_data_file_opened_bytes: Some(2048),
            datafusion_output_batch_size: Some(128),
            scan_partitions_started: 4,
            scan_partitions_completed: 4,
            files_started: 5,
            files_completed: 5,
            dynamic_partition_files_pruned: 2,
            dynamic_partition_files_kept: 3,
            dynamic_filters_received: 1,
            dynamic_filters_accepted: 1,
            dynamic_filters_unsupported: 0,
            dynamic_filter_snapshots: 1,
            dynamic_partition_files_not_pruned_missing_metadata: 0,
            dynamic_partition_files_not_pruned_unsupported_expression: 0,
            batches_produced: 2,
            rows_produced: 10,
            deletion_vector_payloads_loaded: 1,
            deletion_vectors_applied: 1,
            deletion_vector_rows_deleted: 2,
            deletion_vector_failures: 0,
            deletion_vector_rejections: 0,
        }
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
