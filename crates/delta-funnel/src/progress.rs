//! Typed progress events for workspace host integrations.

use std::sync::Arc;

use crate::support::sanitize_text_for_display;

type ProgressCallback = dyn Fn(&ProgressEvent) + Send + Sync + 'static;

/// Top-level operation represented by a progress action.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ProgressOperation {
    /// Load and register one named Delta source.
    RegisterDeltaSource,
    /// Execute and format a bounded table preview.
    PreviewTable,
    /// Execute one SQL Server output write.
    WriteToMssql,
    /// Plan one SQL Server output without executing it.
    DryRunToMssql,
    /// Execute a multi-output SQL Server write workflow.
    WriteAllToMssql,
    /// Plan a multi-output SQL Server write workflow without executing it.
    DryRunAllToMssql,
}

/// Stable visible phase of a progress action.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ProgressPhase {
    /// Load Delta transaction-log metadata for a source.
    LoadingDeltaMetadata,
    /// Check that the loaded Delta protocol is supported.
    ValidatingDeltaProtocol,
    /// Build the DataFusion provider for a loaded Delta source.
    PreparingDeltaProvider,
    /// Add the prepared Delta provider to the DataFusion catalog.
    RegisteringDeltaSource,
    /// Prepare the bounded query used for a table preview.
    PreparingPreview,
    /// Execute and collect the bounded preview query.
    CollectingPreview,
    /// Format collected preview batches as text and HTML.
    FormattingPreview,
    /// Resolve and plan the selected output.
    PlanningOutput,
    /// Set up the selected output batch stream.
    SettingUpStream,
    /// Materialize shared data selected by write-all cache planning.
    MaterializingCache,
    /// Restore session aliases after shared cache execution.
    RestoringCache,
    /// Establish the SQL Server connection.
    Connecting,
    /// Prepare the SQL Server target table.
    PreparingTarget,
    /// Write the output batch stream as one visible phase.
    Writing,
    /// Validate the completed write.
    Validating,
    /// Swap a validated replace staging table into place.
    SwappingTarget,
    /// Clean up a target created by Delta Funnel after failure.
    CleaningUp,
    /// Build source reports after output work has finished.
    ReportingSources,
}

/// Kind of one typed progress event.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ProgressEventKind {
    /// The action started.
    Started,
    /// The action entered a different visible phase.
    PhaseChanged,
    /// Work advanced within the current visible phase.
    Progress,
    /// The action completed successfully.
    Completed,
    /// The action completed with one or more structured output failures.
    CompletedWithFailures,
    /// The action failed.
    Failed,
    /// The action was cancelled.
    Cancelled,
}

/// One immutable typed progress event emitted by the core crate.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgressEvent {
    state: ProgressEventState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProgressEventState {
    Started(ProgressOperation),
    PhaseChanged(ProgressSnapshot),
    Progress(ProgressSnapshot),
    Completed,
    CompletedWithFailures,
    Failed,
    #[allow(
        dead_code,
        reason = "reserved for host-reported cancellation in the progress contract"
    )]
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProgressSnapshot {
    phase: ProgressPhase,
    output_name: Option<String>,
    output_position: Option<ProgressOutputPosition>,
    file_progress: Option<DeltaFileProgress>,
    metrics: Option<ProgressMetrics>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProgressOutputPosition {
    index: u64,
    count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DeltaFileProgress {
    handled: u64,
    total: u64,
    runtime_pruned: u64,
    planning_pruned: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProgressMetrics {
    rows: u64,
    batches: u64,
}

impl ProgressEvent {
    pub(crate) const fn started(operation: ProgressOperation) -> Self {
        Self {
            state: ProgressEventState::Started(operation),
        }
    }

    pub(crate) fn phase_changed(phase: ProgressPhase, output_name: Option<&str>) -> Self {
        Self {
            state: ProgressEventState::PhaseChanged(ProgressSnapshot::new(phase, output_name)),
        }
    }

    pub(crate) fn progress(
        phase: ProgressPhase,
        output_name: Option<&str>,
        rows: u64,
        batches: u64,
    ) -> Self {
        let mut snapshot = ProgressSnapshot::new(phase, output_name);
        snapshot.metrics = Some(ProgressMetrics { rows, batches });
        Self {
            state: ProgressEventState::Progress(snapshot),
        }
    }

    #[cfg(test)]
    pub(crate) fn file_progress(
        phase: ProgressPhase,
        output_name: Option<&str>,
        files_handled: u64,
        files_total: u64,
    ) -> Option<Self> {
        let mut snapshot = ProgressSnapshot::new(phase, output_name);
        snapshot.file_progress = DeltaFileProgress::new(files_handled, files_total, 0, None);
        snapshot.file_progress.map(|_| Self {
            state: ProgressEventState::Progress(snapshot),
        })
    }

    pub(crate) fn file_progress_from_provider_stats(
        phase: ProgressPhase,
        output_name: Option<&str>,
        provider_stats: &[crate::DeltaProviderReadStatsSnapshot],
    ) -> Option<Self> {
        let mut snapshot = ProgressSnapshot::new(phase, output_name);
        snapshot.file_progress = DeltaFileProgress::from_provider_stats(provider_stats);
        snapshot.file_progress.map(|_| Self {
            state: ProgressEventState::Progress(snapshot),
        })
    }

    #[cfg(test)]
    pub(crate) fn progress_with_files(
        phase: ProgressPhase,
        output_name: Option<&str>,
        files_handled: u64,
        files_total: u64,
        rows: u64,
        batches: u64,
    ) -> Self {
        let mut snapshot = ProgressSnapshot::new(phase, output_name);
        snapshot.file_progress = DeltaFileProgress::new(files_handled, files_total, 0, None);
        snapshot.metrics = Some(ProgressMetrics { rows, batches });
        Self {
            state: ProgressEventState::Progress(snapshot),
        }
    }

    /// Adds a validated one-based output position to a phase or progress event.
    pub(crate) fn with_output_position(mut self, index: u64, count: u64) -> Option<Self> {
        let position = ProgressOutputPosition::new(index, count)?;
        match &mut self.state {
            ProgressEventState::PhaseChanged(snapshot) | ProgressEventState::Progress(snapshot) => {
                snapshot.output_position = Some(position);
                Some(self)
            }
            _ => None,
        }
    }

    pub(crate) const fn completed() -> Self {
        Self {
            state: ProgressEventState::Completed,
        }
    }

    pub(crate) const fn completed_with_failures() -> Self {
        Self {
            state: ProgressEventState::CompletedWithFailures,
        }
    }

    pub(crate) const fn failed() -> Self {
        Self {
            state: ProgressEventState::Failed,
        }
    }

    #[cfg(test)]
    pub(crate) const fn cancelled() -> Self {
        Self {
            state: ProgressEventState::Cancelled,
        }
    }

    /// Returns the event kind.
    #[doc(hidden)]
    #[must_use]
    pub const fn kind(&self) -> ProgressEventKind {
        match self.state {
            ProgressEventState::Started(_) => ProgressEventKind::Started,
            ProgressEventState::PhaseChanged(_) => ProgressEventKind::PhaseChanged,
            ProgressEventState::Progress(_) => ProgressEventKind::Progress,
            ProgressEventState::Completed => ProgressEventKind::Completed,
            ProgressEventState::CompletedWithFailures => ProgressEventKind::CompletedWithFailures,
            ProgressEventState::Failed => ProgressEventKind::Failed,
            ProgressEventState::Cancelled => ProgressEventKind::Cancelled,
        }
    }

    /// Returns the operation for a started event.
    #[doc(hidden)]
    #[must_use]
    pub const fn operation(&self) -> Option<ProgressOperation> {
        match self.state {
            ProgressEventState::Started(operation) => Some(operation),
            _ => None,
        }
    }

    /// Returns the visible phase for a phase or progress event.
    #[doc(hidden)]
    #[must_use]
    pub const fn phase(&self) -> Option<ProgressPhase> {
        match &self.state {
            ProgressEventState::PhaseChanged(snapshot) | ProgressEventState::Progress(snapshot) => {
                Some(snapshot.phase)
            }
            _ => None,
        }
    }

    /// Returns the sanitized logical output name when one is active.
    #[doc(hidden)]
    #[must_use]
    pub fn output_name(&self) -> Option<&str> {
        self.snapshot()
            .and_then(|snapshot| snapshot.output_name.as_deref())
    }

    /// Returns the one-based active output index when present.
    #[doc(hidden)]
    #[must_use]
    pub fn output_index(&self) -> Option<u64> {
        self.snapshot()
            .and_then(|snapshot| snapshot.output_position)
            .map(|position| position.index)
    }

    /// Returns the total output count when present.
    #[doc(hidden)]
    #[must_use]
    pub fn output_count(&self) -> Option<u64> {
        self.snapshot()
            .and_then(|snapshot| snapshot.output_position)
            .map(|position| position.count)
    }

    /// Returns the selected Delta files handled when determinate progress is active.
    #[doc(hidden)]
    #[must_use]
    pub fn files_handled(&self) -> Option<u64> {
        self.snapshot()
            .and_then(|snapshot| snapshot.file_progress)
            .map(|progress| progress.handled)
    }

    /// Returns the selected Delta file total when determinate progress is active.
    #[doc(hidden)]
    #[must_use]
    pub fn files_total(&self) -> Option<u64> {
        self.snapshot()
            .and_then(|snapshot| snapshot.file_progress)
            .map(|progress| progress.total)
    }

    /// Returns selected files skipped by dynamic partition pruning.
    #[doc(hidden)]
    #[must_use]
    pub fn files_runtime_pruned(&self) -> Option<u64> {
        self.snapshot()
            .and_then(|snapshot| snapshot.file_progress)
            .map(|progress| progress.runtime_pruned)
    }

    /// Returns the approximate files excluded during metadata planning.
    #[doc(hidden)]
    #[must_use]
    pub fn files_planning_pruned(&self) -> Option<u64> {
        self.snapshot()
            .and_then(|snapshot| snapshot.file_progress)
            .and_then(|progress| progress.planning_pruned)
    }

    /// Returns the accepted row count when present.
    #[doc(hidden)]
    #[must_use]
    pub fn rows(&self) -> Option<u64> {
        self.snapshot()
            .and_then(|snapshot| snapshot.metrics)
            .map(|metrics| metrics.rows)
    }

    /// Returns the accepted batch count when present.
    #[doc(hidden)]
    #[must_use]
    pub fn batches(&self) -> Option<u64> {
        self.snapshot()
            .and_then(|snapshot| snapshot.metrics)
            .map(|metrics| metrics.batches)
    }

    const fn snapshot(&self) -> Option<&ProgressSnapshot> {
        match &self.state {
            ProgressEventState::PhaseChanged(snapshot) | ProgressEventState::Progress(snapshot) => {
                Some(snapshot)
            }
            _ => None,
        }
    }
}

impl ProgressSnapshot {
    fn new(phase: ProgressPhase, output_name: Option<&str>) -> Self {
        Self {
            phase,
            output_name: output_name.map(sanitize_text_for_display),
            output_position: None,
            file_progress: None,
            metrics: None,
        }
    }
}

impl ProgressOutputPosition {
    /// Accepts only one-based positions inside the declared output count.
    const fn new(index: u64, count: u64) -> Option<Self> {
        if index == 0 || index > count {
            return None;
        }
        Some(Self { index, count })
    }
}

impl DeltaFileProgress {
    const fn new(
        handled: u64,
        total: u64,
        runtime_pruned: u64,
        planning_pruned: Option<u64>,
    ) -> Option<Self> {
        if total == 0 {
            return None;
        }

        let handled = if handled > total { total } else { handled };
        Some(Self {
            handled,
            total,
            runtime_pruned: if runtime_pruned > handled {
                handled
            } else {
                runtime_pruned
            },
            planning_pruned,
        })
    }

    fn from_provider_stats(
        provider_stats: &[crate::DeltaProviderReadStatsSnapshot],
    ) -> Option<Self> {
        if provider_stats.is_empty()
            || provider_stats
                .iter()
                .any(|stats| stats.scan_metadata_exhausted != Some(true))
        {
            return None;
        }

        let total = provider_stats
            .iter()
            .try_fold(0_u64, |total, stats| total.checked_add(stats.files_planned))?;
        let (completed, pruned) =
            provider_stats
                .iter()
                .fold((0_u64, 0_u64), |(completed, pruned), stats| {
                    (
                        completed.saturating_add(stats.files_completed),
                        pruned.saturating_add(stats.dynamic_partition_files_pruned),
                    )
                });
        let planning_pruned = provider_stats.iter().try_fold(0_u64, |total, stats| {
            total.checked_add(stats.files_filtered_during_planning?)
        });
        Self::new(
            completed.saturating_add(pruned),
            total,
            pruned,
            planning_pruned,
        )
    }
}

/// Cloneable per-action callback owner used by workspace host integrations.
#[doc(hidden)]
#[derive(Clone, Default)]
pub struct ProgressReporter {
    callback: Option<Arc<ProgressCallback>>,
    output_position: Option<ProgressOutputPosition>,
}

impl ProgressReporter {
    /// Creates a reporter that synchronously borrows each emitted event.
    #[doc(hidden)]
    pub fn new<F>(callback: F) -> Self
    where
        F: Fn(&ProgressEvent) + Send + Sync + 'static,
    {
        Self {
            callback: Some(Arc::new(callback)),
            output_position: None,
        }
    }

    pub(crate) fn emit(&self, event: &ProgressEvent) {
        let Some(callback) = &self.callback else {
            return;
        };
        if let Some(position) = self.output_position {
            if let Some(event) = event
                .clone()
                .with_output_position(position.index, position.count)
            {
                callback(&event);
            }
        } else {
            callback(event);
        }
    }

    /// Creates a reporter that adds one output position to phase and progress
    /// events before forwarding them to this reporter.
    ///
    /// Action-level start and terminal events are not forwarded because they
    /// belong to the surrounding multi-output workflow. Calling this on an
    /// already scoped reporter keeps its original output position.
    pub(crate) fn for_output(&self, index: u64, count: u64) -> Option<Self> {
        let output_position = ProgressOutputPosition::new(index, count)?;
        let output_position = self.output_position.unwrap_or(output_position);
        Some(Self {
            callback: self.callback.clone(),
            output_position: Some(output_position),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;
    use crate::{DeltaProviderReadStatsSnapshot, DeltaProviderReaderBackend};

    #[test]
    fn events_expose_only_the_payload_for_their_kind() {
        let started = ProgressEvent::started(ProgressOperation::WriteToMssql);
        assert_eq!(started.kind(), ProgressEventKind::Started);
        assert_eq!(started.operation(), Some(ProgressOperation::WriteToMssql));
        assert_eq!(started.phase(), None);
        assert_eq!(started.output_name(), None);
        assert_eq!(started.rows(), None);

        let phase =
            ProgressEvent::phase_changed(ProgressPhase::PlanningOutput, Some("orders\noutput"));
        assert_eq!(phase.kind(), ProgressEventKind::PhaseChanged);
        assert_eq!(phase.operation(), None);
        assert_eq!(phase.phase(), Some(ProgressPhase::PlanningOutput));
        assert_eq!(phase.output_name(), Some(r"orders\noutput"));
        assert_eq!(phase.output_index(), None);
        assert_eq!(phase.output_count(), None);
        assert_eq!(phase.files_handled(), None);
        assert_eq!(phase.files_total(), None);
        assert_eq!(phase.files_runtime_pruned(), None);
        assert_eq!(phase.files_planning_pruned(), None);
        assert_eq!(phase.rows(), None);
        assert_eq!(phase.batches(), None);

        let progress = ProgressEvent::progress(ProgressPhase::Writing, Some("orders"), 42, 3);
        assert_eq!(progress.kind(), ProgressEventKind::Progress);
        assert_eq!(progress.phase(), Some(ProgressPhase::Writing));
        assert_eq!(progress.output_name(), Some("orders"));
        assert_eq!(progress.files_handled(), None);
        assert_eq!(progress.files_total(), None);
        assert_eq!(progress.files_runtime_pruned(), None);
        assert_eq!(progress.files_planning_pruned(), None);
        assert_eq!(progress.rows(), Some(42));
        assert_eq!(progress.batches(), Some(3));
    }

    #[test]
    fn started_events_preserve_each_operation_identity() {
        for operation in [
            ProgressOperation::RegisterDeltaSource,
            ProgressOperation::PreviewTable,
            ProgressOperation::WriteToMssql,
            ProgressOperation::DryRunToMssql,
            ProgressOperation::WriteAllToMssql,
            ProgressOperation::DryRunAllToMssql,
        ] {
            assert_eq!(
                ProgressEvent::started(operation).operation(),
                Some(operation)
            );
        }
    }

    #[test]
    fn file_progress_is_paired_positive_and_capped() -> Result<(), Box<dyn std::error::Error>> {
        let progress =
            ProgressEvent::progress_with_files(ProgressPhase::Writing, Some("orders"), 7, 5, 42, 3);

        assert_eq!(progress.kind(), ProgressEventKind::Progress);
        assert_eq!(progress.phase(), Some(ProgressPhase::Writing));
        assert_eq!(progress.files_handled(), Some(5));
        assert_eq!(progress.files_total(), Some(5));
        assert_eq!(progress.files_runtime_pruned(), Some(0));
        assert_eq!(progress.files_planning_pruned(), None);
        assert_eq!(progress.rows(), Some(42));
        assert_eq!(progress.batches(), Some(3));
        assert!(
            ProgressEvent::file_progress(ProgressPhase::Writing, Some("orders"), 0, 0).is_none()
        );
        Ok(())
    }

    #[test]
    fn output_positions_are_one_based_and_belong_only_to_active_work_events()
    -> Result<(), Box<dyn std::error::Error>> {
        let Some(phase) =
            ProgressEvent::phase_changed(ProgressPhase::PlanningOutput, Some("orders\noutput"))
                .with_output_position(2, 3)
        else {
            return Err("valid phase output position was rejected".into());
        };
        let Some(progress) = ProgressEvent::progress(ProgressPhase::Writing, Some("orders"), 42, 3)
            .with_output_position(2, 3)
        else {
            return Err("valid progress output position was rejected".into());
        };

        for event in [&phase, &progress] {
            assert_eq!(event.output_index(), Some(2));
            assert_eq!(event.output_count(), Some(3));
        }
        assert_eq!(phase.output_name(), Some(r"orders\noutput"));

        for (index, count) in [(0, 3), (1, 0), (4, 3)] {
            assert!(
                ProgressEvent::phase_changed(ProgressPhase::PlanningOutput, None)
                    .with_output_position(index, count)
                    .is_none()
            );
        }
        assert!(
            ProgressEvent::completed()
                .with_output_position(1, 1)
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn output_reporter_adds_position_and_drops_action_events()
    -> Result<(), Box<dyn std::error::Error>> {
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = Arc::clone(&events);
        let reporter = ProgressReporter::new(move |event| {
            if let Ok(mut events) = recorded.lock() {
                events.push(event.clone());
            }
        });
        let Some(output_reporter) = reporter.for_output(2, 3) else {
            return Err("valid output position was rejected".into());
        };
        let cloned_output_reporter = output_reporter.clone();
        let Some(rescoped_output_reporter) = output_reporter.for_output(1, 3) else {
            return Err("valid nested output position was rejected".into());
        };

        output_reporter.emit(&ProgressEvent::started(ProgressOperation::WriteAllToMssql));
        cloned_output_reporter.emit(&ProgressEvent::phase_changed(
            ProgressPhase::Writing,
            Some("orders"),
        ));
        rescoped_output_reporter.emit(&ProgressEvent::progress(
            ProgressPhase::Writing,
            Some("orders"),
            42,
            3,
        ));
        output_reporter.emit(&ProgressEvent::completed());

        for (index, count) in [(0, 3), (1, 0), (4, 3)] {
            assert!(reporter.for_output(index, count).is_none());
        }

        let events = events.lock().map_err(|_| "progress event lock poisoned")?;
        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|event| event.output_index() == Some(2)));
        assert!(events.iter().all(|event| event.output_count() == Some(3)));
        Ok(())
    }

    #[test]
    fn provider_stats_sum_selected_and_handled_files_across_scans()
    -> Result<(), Box<dyn std::error::Error>> {
        let stats = [
            provider_stats(Some(true), 5, 2, 1),
            provider_stats(Some(true), 7, 1, 2),
        ];

        let Some(progress) = ProgressEvent::file_progress_from_provider_stats(
            ProgressPhase::Writing,
            Some("orders"),
            &stats,
        ) else {
            return Err("eligible provider stats did not produce file progress".into());
        };

        assert_eq!(progress.files_total(), Some(12));
        assert_eq!(progress.files_handled(), Some(6));
        assert_eq!(progress.files_runtime_pruned(), Some(3));
        assert_eq!(progress.files_planning_pruned(), Some(18));
        Ok(())
    }

    #[test]
    fn provider_stats_cap_handled_files_at_the_selected_total()
    -> Result<(), Box<dyn std::error::Error>> {
        let stats = [provider_stats(Some(true), 5, 4, 3)];

        let Some(progress) =
            ProgressEvent::file_progress_from_provider_stats(ProgressPhase::Writing, None, &stats)
        else {
            return Err("eligible provider stats did not produce file progress".into());
        };

        assert_eq!(progress.files_total(), Some(5));
        assert_eq!(progress.files_handled(), Some(5));
        assert_eq!(progress.files_runtime_pruned(), Some(3));
        assert_eq!(progress.files_planning_pruned(), Some(9));
        Ok(())
    }

    #[test]
    fn ineligible_provider_stats_stay_indeterminate() {
        assert!(
            ProgressEvent::file_progress_from_provider_stats(ProgressPhase::Writing, None, &[])
                .is_none()
        );

        for stats in [
            provider_stats(Some(false), 1, 0, 0),
            provider_stats(None, 1, 0, 0),
            provider_stats(Some(true), 0, 0, 0),
        ] {
            assert!(
                ProgressEvent::file_progress_from_provider_stats(
                    ProgressPhase::Writing,
                    None,
                    &[stats],
                )
                .is_none()
            );
        }
    }

    fn provider_stats(
        scan_metadata_exhausted: Option<bool>,
        files_planned: u64,
        files_completed: u64,
        dynamic_partition_files_pruned: u64,
    ) -> DeltaProviderReadStatsSnapshot {
        DeltaProviderReadStatsSnapshot {
            source_name: "orders".to_owned(),
            snapshot_version: 1,
            reader_backend: DeltaProviderReaderBackend::NativeAsync,
            scan_metadata_exhausted,
            scan_partitions_planned: 1,
            files_planned,
            files_filtered_during_planning: Some(9),
            estimated_rows: None,
            estimated_bytes: None,
            parquet_data_file_range_get_operations: Some(0),
            parquet_data_file_full_get_operations: Some(0),
            parquet_data_file_bytes_received: Some(0),
            parquet_data_file_opened_bytes: Some(0),
            datafusion_output_batch_size: None,
            scan_partitions_started: 0,
            scan_partitions_completed: 0,
            files_started: files_completed,
            files_completed,
            dynamic_partition_files_pruned,
            dynamic_partition_files_kept: 0,
            dynamic_filters_received: 0,
            dynamic_filters_accepted: 0,
            dynamic_filters_unsupported: 0,
            dynamic_filter_snapshots: 0,
            dynamic_partition_files_not_pruned_missing_metadata: 0,
            dynamic_partition_files_not_pruned_unsupported_expression: 0,
            batches_produced: 0,
            rows_produced: 0,
            deletion_vector_payloads_loaded: 0,
            deletion_vectors_applied: 0,
            deletion_vector_rows_deleted: 0,
            deletion_vector_failures: 0,
            deletion_vector_rejections: 0,
        }
    }

    #[test]
    fn terminal_events_have_no_action_payload() {
        let events = [
            ProgressEvent::completed(),
            ProgressEvent::completed_with_failures(),
            ProgressEvent::failed(),
            ProgressEvent::cancelled(),
        ];

        assert_eq!(
            events.iter().map(ProgressEvent::kind).collect::<Vec<_>>(),
            [
                ProgressEventKind::Completed,
                ProgressEventKind::CompletedWithFailures,
                ProgressEventKind::Failed,
                ProgressEventKind::Cancelled,
            ]
        );
        assert!(events.iter().all(|event| event.operation().is_none()));
        assert!(events.iter().all(|event| event.phase().is_none()));
        assert!(events.iter().all(|event| event.output_name().is_none()));
        assert!(events.iter().all(|event| event.files_handled().is_none()));
        assert!(events.iter().all(|event| event.files_total().is_none()));
        assert!(events.iter().all(|event| event.rows().is_none()));
    }

    #[test]
    fn reporter_clones_share_the_owned_borrowing_callback() {
        let deliveries = Arc::new(AtomicUsize::new(0));
        let callback_deliveries = Arc::clone(&deliveries);
        let reporter = ProgressReporter::new(move |event| {
            assert_eq!(event.kind(), ProgressEventKind::Started);
            callback_deliveries.fetch_add(1, Ordering::Relaxed);
        });
        let cloned = reporter.clone();
        let event = ProgressEvent::started(ProgressOperation::WriteToMssql);

        reporter.emit(&event);
        cloned.emit(&event);

        assert_eq!(deliveries.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn default_reporter_is_a_no_op() {
        ProgressReporter::default().emit(&ProgressEvent::failed());
    }

    #[test]
    fn reporter_is_send_and_sync() {
        const fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ProgressReporter>();
    }
}
