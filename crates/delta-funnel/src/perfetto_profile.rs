//! Opt-in Perfetto producer and semantic track adapter for Delta Funnel diagnostics.
//!
//! This module does not install a tracing subscriber or manage the external capture process.

use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::{OnceLock, atomic::AtomicU64};
use std::time::{Duration, Instant};

use perfetto_sdk::producer::{Backends, Producer, ProducerInitArgsBuilder};
use perfetto_sdk::protos::trace::track_event::track_descriptor::{
    TrackDescriptorChildTracksOrdering, TrackDescriptorFieldNumber,
    TrackDescriptorSiblingMergeBehavior,
};
use perfetto_sdk::track_event::{
    EventContext, TrackEvent, TrackEventProtoField, TrackEventProtoTrack, TrackEventTrack,
};
use perfetto_sdk::{
    track_event_begin, track_event_categories, track_event_category_enabled, track_event_end,
};

use crate::profiling::{allocate_id, in_operation_capture_scope};
use crate::query_engine::datafusion::initialize_datafusion_task_tracing;

mod profile_layer;
mod ranked_profile_artifact;
mod ranked_report;
mod report_aggregate;
mod report_cli;
mod report_health;
mod report_html;
mod report_terminal;
mod report_trace_processor;
mod report_trace_sanitizer;

pub use profile_layer::{PROFILE_TARGET, PerfettoProfileLayer, is_profile_target};
use ranked_profile_artifact::{
    has_ranked_profile_artifact_magic, read_ranked_profile_artifact, write_ranked_profile_artifact,
};
use ranked_report::RankedProfileDocument;
use report_aggregate::load_ranked_profile;
use report_cli::preflight_ranked_report_paths;
pub use report_cli::{
    RankedReportFailure, RankedReportFailurePhase, run_perfetto_diagnostics_cli,
    run_perfetto_diagnostics_cli_with_args,
};
use report_html::{ExistingOutputPolicy, render_ranked_profile_html, write_ranked_profile_html};
#[cfg(test)]
use report_terminal::render_terminal_view;

/// Reports whether two profile destinations resolve to the same file.
#[doc(hidden)]
pub fn output_paths_alias(left: &Path, right: &Path) -> io::Result<bool> {
    let left = resolve_output_identity(left)?;
    let right = resolve_output_identity(right)?;
    if left == right {
        return Ok(true);
    }
    match same_file::is_same_file(left, right) {
        Ok(alias) => Ok(alias),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

fn resolve_output_identity(path: &Path) -> io::Result<PathBuf> {
    let mut resolved = PathBuf::new();
    for component in std::path::absolute(path)?.components() {
        match component {
            Component::Prefix(prefix) => resolved.push(prefix.as_os_str()),
            Component::RootDir => resolved.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                resolved.pop();
            }
            Component::Normal(segment) => {
                resolved.push(segment);
                match resolved.canonicalize() {
                    Ok(canonical) => resolved = canonical,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
            }
        }
    }
    Ok(resolved)
}

/// Generates one self-contained ranked HTML report from a completed Perfetto trace or artifact.
///
/// # Errors
///
/// Returns a structured failure when the trace, Trace Processor query, aggregate,
/// serialization, or output write fails.
pub fn generate_ranked_profile_report(
    input: &Path,
    output: &Path,
) -> Result<PathBuf, RankedReportFailure> {
    generate_ranked_profile_report_for_scope(input, output, None, ExistingOutputPolicy::Replace)
}

/// Converts a completed Perfetto trace into a reusable ranked profile artifact.
///
/// # Errors
///
/// Returns a structured failure when the trace cannot be aggregated, validated,
/// serialized, or atomically persisted.
#[doc(hidden)]
pub fn generate_ranked_profile_artifact(
    input: &Path,
    output: &Path,
) -> Result<PathBuf, RankedReportFailure> {
    generate_ranked_profile_artifact_for_scope(input, output, None)
}

/// Converts one host-selected operation capture into a reusable ranked profile artifact.
///
/// # Errors
///
/// Returns a structured failure when the selected operation is unavailable or
/// the artifact cannot be validated, serialized, or atomically persisted.
#[doc(hidden)]
pub fn generate_operation_ranked_profile_artifact(
    input: &Path,
    output: &Path,
    capture_scope: &OperationCaptureScope,
) -> Result<PathBuf, RankedReportFailure> {
    generate_ranked_profile_artifact_for_scope(input, output, Some(capture_scope.id))
}

fn generate_ranked_profile_artifact_for_scope(
    input: &Path,
    output: &Path,
    capture_scope_id: Option<u64>,
) -> Result<PathBuf, RankedReportFailure> {
    let paths = preflight_ranked_report_paths(input, output).map_err(RankedReportFailure::from)?;
    let document = load_ranked_profile(&paths.input, capture_scope_id)?;
    write_ranked_profile_artifact(&paths.output, &document, ExistingOutputPolicy::Replace)?;
    Ok(paths.output)
}

/// Validates a ranked profile artifact without starting Trace Processor.
///
/// # Errors
///
/// Returns a structured failure when the artifact envelope, archive bytes, or
/// ranked profile invariants are invalid.
#[doc(hidden)]
pub fn validate_ranked_profile_artifact(input: &Path) -> Result<(), RankedReportFailure> {
    read_ranked_profile_artifact(input).map(|_| ())
}

fn generate_ranked_profile_report_without_clobber(
    input: &Path,
    output: &Path,
) -> Result<PathBuf, RankedReportFailure> {
    generate_ranked_profile_report_for_scope(input, output, None, ExistingOutputPolicy::Preserve)
}

/// Generates one ranked HTML report for a host-selected operation capture.
///
/// # Errors
///
/// Returns a structured failure when the selected operation is unavailable or
/// normal ranked report generation fails.
#[doc(hidden)]
pub fn generate_operation_ranked_profile_report(
    input: &Path,
    output: &Path,
    capture_scope: &OperationCaptureScope,
) -> Result<PathBuf, RankedReportFailure> {
    generate_ranked_profile_report_for_scope(
        input,
        output,
        Some(capture_scope.id),
        ExistingOutputPolicy::Replace,
    )
}

fn generate_ranked_profile_report_for_scope(
    input: &Path,
    output: &Path,
    capture_scope_id: Option<u64>,
    existing_output: ExistingOutputPolicy,
) -> Result<PathBuf, RankedReportFailure> {
    let paths = preflight_ranked_report_paths(input, output).map_err(RankedReportFailure::from)?;
    let document = match capture_scope_id {
        Some(capture_scope_id) => load_ranked_profile(&paths.input, Some(capture_scope_id))?,
        None => load_ranked_profile_input(&paths.input)?,
    };
    let html = render_ranked_profile_html(&document)?;
    write_ranked_profile_html(&paths.output, &html, existing_output)?;
    Ok(paths.output)
}

fn load_ranked_profile_input(input: &Path) -> Result<RankedProfileDocument, RankedReportFailure> {
    if has_ranked_profile_artifact_magic(input)? {
        read_ranked_profile_artifact(input)?.into_document()
    } else {
        load_ranked_profile(input, None)
    }
}

const CATEGORY: &str = "delta_funnel.profile";
const CAPTURE_POLL_INTERVAL: Duration = Duration::from_millis(10);
// Keep output tracks after the bounded worker-lane rank range.
const DELTA_SCAN_OUTPUT_SIBLING_ORDER_BASE: u64 = 1_000_000;
// Perfetto's 256 KiB default dropped semantic packets during canonical event
// bursts. This is the largest bounded hint accepted by Perfetto v57.2.
const PRODUCER_SHMEM_SIZE_HINT_KB: u32 = 32 * 1024;
static PERFETTO_INITIALIZATION: OnceLock<Result<(), String>> = OnceLock::new();
static NEXT_OPERATION_CAPTURE_SCOPE_ID: AtomicU64 = AtomicU64::new(1);

/// Unique correlation scope for one host-managed operation capture.
#[doc(hidden)]
#[derive(Debug)]
pub struct OperationCaptureScope {
    id: u64,
}

impl OperationCaptureScope {
    /// Allocates one process-unique operation capture scope.
    pub fn allocate() -> Option<Self> {
        allocate_id(&NEXT_OPERATION_CAPTURE_SCOPE_ID)
            .filter(|id| i64::try_from(*id).is_ok())
            .map(|id| Self { id })
    }

    /// Runs the operation entry point inside this capture scope.
    pub fn in_scope<T>(&self, operation: impl FnOnce() -> T) -> T {
        track_event_begin!(
            "delta_funnel.profile.context",
            "Delta Funnel operation capture scope",
            |context: &mut EventContext| {
                context.add_debug_arg(
                    "capture_scope_id",
                    perfetto_sdk::track_event::TrackEventDebugArg::Uint64(self.id),
                );
            }
        );
        let _context = OperationCaptureContext;
        in_operation_capture_scope(self.id, operation)
    }
}

struct OperationCaptureContext;

impl Drop for OperationCaptureContext {
    fn drop(&mut self) {
        track_event_end!("delta_funnel.profile.context");
    }
}

track_event_categories! {
    pub(crate) mod delta_funnel_perfetto {
        (
            "delta_funnel.profile",
            "Delta Funnel semantic profiling",
            []
        ),
        (
            "delta_funnel.profile.context",
            "Delta Funnel semantic execution context",
            []
        ),
    }
}

pub(crate) use delta_funnel_perfetto as perfetto_te_ns;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SemanticTrack {
    pub(crate) name: String,
    pub(crate) uuid: u64,
    pub(crate) parent_uuid: u64,
    pub(crate) sibling_order_rank: u64,
}

impl SemanticTrack {
    fn new(name: String, id: u64, parent_uuid: u64, sibling_order_rank: u64) -> Self {
        let uuid = TrackEventTrack::named_track_uuid(&name, id, parent_uuid);
        Self {
            name,
            uuid,
            parent_uuid,
            sibling_order_rank,
        }
    }

    pub(crate) fn set_on(&self, context: &mut EventContext) {
        let fields = [
            TrackEventProtoField::VarInt(
                TrackDescriptorFieldNumber::ParentUuid as u32,
                self.parent_uuid,
            ),
            TrackEventProtoField::Cstr(TrackDescriptorFieldNumber::Name as u32, &self.name),
            TrackEventProtoField::VarInt(
                TrackDescriptorFieldNumber::DisallowMergingWithSystemTracks as u32,
                1,
            ),
            TrackEventProtoField::VarInt(
                TrackDescriptorFieldNumber::ChildOrdering as u32,
                u64::from(u32::from(TrackDescriptorChildTracksOrdering::Explicit)),
            ),
            TrackEventProtoField::VarInt(
                TrackDescriptorFieldNumber::SiblingOrderRank as u32,
                self.sibling_order_rank,
            ),
            TrackEventProtoField::VarInt(
                TrackDescriptorFieldNumber::SiblingMergeBehavior as u32,
                u64::from(u32::from(
                    TrackDescriptorSiblingMergeBehavior::SiblingMergeBehaviorNone,
                )),
            ),
        ];
        context.set_proto_track(&TrackEventProtoTrack {
            uuid: self.uuid,
            fields: &fields,
        });
    }
}

pub(crate) fn diagnostics_track(process_uuid: u64) -> SemanticTrack {
    SemanticTrack::new("Delta Funnel diagnostics".to_owned(), 0, process_uuid, 10)
}

pub(crate) fn operation_track(operation_id: u64, diagnostics_uuid: u64) -> SemanticTrack {
    SemanticTrack::new(
        format!("Operation [{}]", operation_token(operation_id)),
        operation_id,
        diagnostics_uuid,
        operation_id,
    )
}

pub(crate) fn phase_track(operation_id: u64, operation_uuid: u64) -> SemanticTrack {
    SemanticTrack::new(
        format!("Operation [{}] / phases", operation_token(operation_id)),
        0,
        operation_uuid,
        10,
    )
}

pub(crate) fn planning_track(
    operation_id: u64,
    query_execution_id: u64,
    phases_uuid: u64,
) -> SemanticTrack {
    SemanticTrack::new(
        format!(
            "Operation [{}] / query [{}] / planning",
            operation_token(operation_id),
            query_token(query_execution_id)
        ),
        query_execution_id,
        phases_uuid,
        20_u64.saturating_add(query_execution_id),
    )
}

pub(crate) fn owner_track(operation_id: u64, owner_id: u64, operation_uuid: u64) -> SemanticTrack {
    SemanticTrack::new(
        format!(
            "Operation [{}] / owner [{}]",
            operation_token(operation_id),
            owner_token(owner_id)
        ),
        owner_id,
        operation_uuid,
        30_u64.saturating_add(owner_id),
    )
}

pub(crate) fn query_track(
    operation_id: u64,
    query_execution_id: u64,
    operation_uuid: u64,
) -> SemanticTrack {
    SemanticTrack::new(
        format!(
            "Operation [{}] / query [{}]",
            operation_token(operation_id),
            query_token(query_execution_id)
        ),
        query_execution_id,
        operation_uuid,
        20_u64.saturating_add(query_execution_id),
    )
}

pub(crate) fn worker_track(
    operation_id: u64,
    query_execution_id: u64,
    worker_lane_id: u64,
    query_uuid: u64,
    sibling_order_rank: u64,
) -> SemanticTrack {
    SemanticTrack::new(
        format!(
            "Operation [{}] / query [{}] / worker [{}]",
            operation_token(operation_id),
            query_token(query_execution_id),
            worker_token(worker_lane_id)
        ),
        worker_lane_id,
        query_uuid,
        sibling_order_rank,
    )
}

pub(crate) fn delta_scan_output_track(
    operation_id: u64,
    query_execution_id: u64,
    execution_stream_id: u64,
    query_uuid: u64,
) -> SemanticTrack {
    SemanticTrack::new(
        format!(
            "Operation [{}] / query [{}] / Delta scan output [{}]",
            operation_token(operation_id),
            query_token(query_execution_id),
            stream_token(execution_stream_id)
        ),
        execution_stream_id,
        query_uuid,
        DELTA_SCAN_OUTPUT_SIBLING_ORDER_BASE.saturating_add(execution_stream_id),
    )
}

pub(crate) fn operation_token(id: u64) -> String {
    format!("op-{id:020}")
}

pub(crate) fn query_token(id: u64) -> String {
    format!("q-{id:020}")
}

pub(crate) fn worker_token(id: u64) -> String {
    format!("w-{id:020}")
}

pub(crate) fn stream_token(id: u64) -> String {
    format!("s-{id:020}")
}

pub(crate) fn owner_token(id: u64) -> String {
    format!("o-{id:020}")
}

/// Initializes the system Perfetto producer and registers the profile category
/// once per process.
///
/// Repeated and concurrent calls reuse the first initialization result.
///
/// # Errors
///
/// Returns an error when the process-wide category or DataFusion task tracer
/// cannot be registered.
pub fn initialize_perfetto() -> io::Result<()> {
    match PERFETTO_INITIALIZATION.get_or_init(|| {
        let producer_args = ProducerInitArgsBuilder::new()
            .backends(Backends::SYSTEM)
            .shmem_size_hint_kb(PRODUCER_SHMEM_SIZE_HINT_KB);
        Producer::init(producer_args.build());
        TrackEvent::init();
        perfetto_te_ns::register()
            .map_err(|error| format!("failed to register Perfetto category: {error}"))?;
        initialize_datafusion_task_tracing()
            .map_err(|error| format!("failed to register DataFusion task tracer: {error}"))
    }) {
        Ok(()) => Ok(()),
        Err(message) => Err(io::Error::other(message.clone())),
    }
}

/// Returns whether an external capture currently accepts Delta Funnel profile events.
pub fn is_profile_capture_active() -> bool {
    track_event_category_enabled!("delta_funnel.profile")
}

/// Waits up to `timeout` for an external system capture to enable the profile category.
///
/// # Errors
///
/// Returns [`io::ErrorKind::TimedOut`] when no capture enables the category, or
/// [`io::ErrorKind::InvalidInput`] when `timeout` cannot be represented by the platform clock.
pub fn wait_for_capture(timeout: Duration) -> io::Result<()> {
    let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Perfetto capture wait timeout {timeout:?} is too large"),
        )
    })?;
    while !is_profile_capture_active() {
        let now = Instant::now();
        if now >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("Perfetto category {CATEGORY:?} was not enabled within {timeout:?}"),
            ));
        }
        std::thread::sleep(
            deadline
                .saturating_duration_since(now)
                .min(CAPTURE_POLL_INTERVAL),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::error::Error;
    use std::hash::Hasher;
    use std::hint::black_box;
    use std::io::Read;
    use std::sync::{Arc, Barrier, mpsc};
    use std::thread::{self, ThreadId};

    use perfetto_sdk::track_event::{EventContext, TrackEventDebugArg};
    use perfetto_sdk::{track_event_begin, track_event_end};

    use super::*;

    const RELEASE_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
    type DynError = Box<dyn Error + Send + Sync>;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct SemanticTracks {
        diagnostics: SemanticTrack,
        operation: SemanticTrack,
        phases: SemanticTrack,
        query: SemanticTrack,
        workers: [(u64, SemanticTrack); 2],
    }

    impl SemanticTracks {
        fn new(
            process_uuid: u64,
            operation_id: u64,
            query_execution_id: u64,
            mut worker_lane_ids: [u64; 2],
        ) -> Self {
            debug_assert_ne!(operation_id, 0);
            debug_assert_ne!(query_execution_id, 0);
            debug_assert!(worker_lane_ids.iter().all(|id| *id != 0));
            worker_lane_ids.sort_unstable();
            debug_assert_ne!(worker_lane_ids[0], worker_lane_ids[1]);

            let diagnostics = diagnostics_track(process_uuid);
            let operation = operation_track(operation_id, diagnostics.uuid);
            let phases = phase_track(operation_id, operation.uuid);
            let query = query_track(operation_id, query_execution_id, operation.uuid);
            let make_worker_track = |worker_lane_id, sibling_order_rank| {
                worker_track(
                    operation_id,
                    query_execution_id,
                    worker_lane_id,
                    query.uuid,
                    sibling_order_rank,
                )
            };
            let workers = [
                (
                    worker_lane_ids[0],
                    make_worker_track(worker_lane_ids[0], 10),
                ),
                (
                    worker_lane_ids[1],
                    make_worker_track(worker_lane_ids[1], 20),
                ),
            ];
            Self {
                diagnostics,
                operation,
                phases,
                query,
                workers,
            }
        }
    }

    #[derive(Debug)]
    struct WorkerMigrationEvidence {
        begin_thread: ThreadId,
        end_thread: ThreadId,
        parallel_thread: ThreadId,
    }

    #[test]
    fn semantic_tracks_are_deterministic_parented_ordered_and_exactly_filterable() {
        let tracks = SemanticTracks::new(42, 1, 1, [10, 1]);
        let duplicate = SemanticTracks::new(42, 1, 1, [1, 10]);
        assert_eq!(tracks, duplicate);

        let uuids = [
            tracks.diagnostics.uuid,
            tracks.operation.uuid,
            tracks.phases.uuid,
            tracks.query.uuid,
            tracks.workers[0].1.uuid,
            tracks.workers[1].1.uuid,
        ]
        .into_iter()
        .collect::<BTreeSet<_>>();
        assert_eq!(uuids.len(), 6);
        assert_eq!(tracks.operation.parent_uuid, tracks.diagnostics.uuid);
        assert_eq!(tracks.phases.parent_uuid, tracks.operation.uuid);
        assert_eq!(tracks.query.parent_uuid, tracks.operation.uuid);
        assert!(
            tracks
                .workers
                .iter()
                .all(|(_, worker)| worker.parent_uuid == tracks.query.uuid)
        );
        assert!(tracks.phases.sibling_order_rank < tracks.query.sibling_order_rank);
        assert!(tracks.workers[0].1.sibling_order_rank < tracks.workers[1].1.sibling_order_rank);

        let worker_1_filter = format!("worker [{}]", worker_token(1));
        assert!(tracks.workers[0].1.name.contains(&worker_1_filter));
        assert!(!tracks.workers[1].1.name.contains(&worker_1_filter));
        let query_1_filter = format!("query [{}]", query_token(1));
        let query_10 = SemanticTracks::new(42, 1, 10, [1, 10]);
        assert!(!query_10.query.name.contains(&query_1_filter));
        let operation_2 = SemanticTracks::new(42, 2, 1, [1, 10]);
        assert_ne!(tracks.operation.uuid, operation_2.operation.uuid);
        assert_ne!(tracks.query.uuid, operation_2.query.uuid);
        assert_ne!(tracks.workers[0].1.uuid, operation_2.workers[0].1.uuid);
    }

    #[test]
    fn producer_initialization_is_concurrent_and_retry_safe() -> io::Result<()> {
        let barrier = Arc::new(Barrier::new(8));
        let initializations = (0..8)
            .map(|_| {
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    initialize_perfetto()
                })
            })
            .collect::<Vec<_>>();
        for initialization in initializations {
            initialization
                .join()
                .expect("Perfetto initialization thread must not panic")?;
        }

        initialize_perfetto()?;
        let first_error = wait_for_capture(Duration::ZERO)
            .expect_err("an inactive category must time out immediately");
        initialize_perfetto()?;
        let second_error = wait_for_capture(Duration::ZERO)
            .expect_err("a repeated inactive wait must still time out");

        assert_eq!(first_error.kind(), io::ErrorKind::TimedOut);
        assert_eq!(second_error.kind(), io::ErrorKind::TimedOut);
        assert!(first_error.to_string().contains(CATEGORY));
        assert!(second_error.to_string().contains(CATEGORY));
        Ok(())
    }

    #[test]
    fn capture_wait_rejects_an_unrepresentable_timeout() {
        let error = wait_for_capture(Duration::MAX)
            .expect_err("an unrepresentable deadline must be rejected");

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn report_generation_rejects_an_input_alias_before_analysis() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let input = directory.path().join("capture.pftrace");
        std::fs::write(&input, "unchanged trace")?;

        let error = generate_ranked_profile_report(&input, &input)
            .expect_err("the report must never replace its input trace");
        assert_eq!(error.phase(), RankedReportFailurePhase::Output);
        assert_eq!(error.kind(), "aliases_input");
        assert_eq!(std::fs::read_to_string(input)?, "unchanged trace");
        Ok(())
    }

    #[test]
    #[ignore = "requires trace_processor_shell and a real raw trace"]
    fn generates_a_report_without_modifying_a_real_raw_trace() -> Result<(), DynError> {
        let trace = PathBuf::from(
            std::env::var_os("DELTA_FUNNEL_TEST_PERFETTO_TRACE")
                .ok_or("DELTA_FUNNEL_TEST_PERFETTO_TRACE is not set")?,
        );
        let before = file_fingerprint(&trace)?;
        let directory = tempfile::tempdir()?;
        let output = directory.path().join("capture.profile.html");

        assert_eq!(generate_ranked_profile_report(&trace, &output)?, output);
        assert_eq!(file_fingerprint(&trace)?, before);
        let html = std::fs::read_to_string(output)?;
        assert!(html.starts_with("<!doctype html>"));
        assert!(html.contains("id=\"profile-data\""));
        assert!(html.contains("Function metrics are sampled on-CPU observations"));
        assert!(!html.contains(trace.to_string_lossy().as_ref()));
        assert!(!html.contains("http://"));
        assert!(!html.contains("https://"));
        Ok(())
    }

    fn file_fingerprint(path: &Path) -> io::Result<(u64, u64)> {
        let mut file = std::fs::File::open(path)?;
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        let mut byte_count = 0_u64;
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let count = file.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            hasher.write(&buffer[..count]);
            byte_count = byte_count
                .checked_add(u64::try_from(count).map_err(io::Error::other)?)
                .ok_or_else(|| io::Error::other("input trace size overflowed"))?;
        }
        Ok((byte_count, hasher.finish()))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn logical_worker_track_survives_tokio_worker_migration() -> Result<(), DynError> {
        let tracks = SemanticTracks::new(42, 1, 1, [1, 10]);
        let worker_uuid = tracks.workers[0].1.uuid;

        let evidence = emit_parallel_workers(&tracks, Duration::from_millis(1)).await?;

        assert_ne!(evidence.begin_thread, evidence.end_thread);
        assert_eq!(evidence.parallel_thread, evidence.end_thread);
        assert_eq!(tracks.workers[0].1.uuid, worker_uuid);
        Ok(())
    }

    async fn emit_parallel_workers(
        tracks: &SemanticTracks,
        work_duration: Duration,
    ) -> Result<WorkerMigrationEvidence, DynError> {
        let (first_lane_id, first_track) = tracks.workers[0].clone();
        let (parallel_lane_id, parallel_track) = tracks.workers[1].clone();
        let (begun_tx, begun_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let begin_task = tokio::spawn(async move {
            let begin_thread = std::thread::current().id();
            let begin_worker = current_thread_name();
            track_event_begin!(
                "delta_funnel.profile",
                "Logical worker activity",
                |context: &mut EventContext| {
                    first_track.set_on(context);
                    context
                        .add_debug_arg("worker_lane_id", TrackEventDebugArg::Uint64(first_lane_id));
                    context.add_debug_arg(
                        "begin_tokio_worker",
                        TrackEventDebugArg::String(&begin_worker),
                    );
                }
            );
            sampled_cpu_work(work_duration / 2);
            begun_tx
                .send(begin_thread)
                .map_err(|_| "begin thread receiver closed")?;
            release_rx
                .recv_timeout(RELEASE_WAIT_TIMEOUT)
                .map_err(|error| format!("timed out waiting to release begin worker: {error}"))?;
            Ok::<_, DynError>(())
        });

        let begin_thread = begun_rx.await?;
        let parallel_task = tokio::spawn(async move {
            let parallel_thread = std::thread::current().id();
            let parallel_worker = current_thread_name();
            track_event_begin!(
                "delta_funnel.profile",
                "Logical worker activity",
                |context: &mut EventContext| {
                    parallel_track.set_on(context);
                    context.add_debug_arg(
                        "worker_lane_id",
                        TrackEventDebugArg::Uint64(parallel_lane_id),
                    );
                    context.add_debug_arg(
                        "tokio_worker",
                        TrackEventDebugArg::String(&parallel_worker),
                    );
                }
            );
            sampled_cpu_work(work_duration);
            track_event_end!("delta_funnel.profile", |context: &mut EventContext| {
                parallel_track.set_on(context)
            });
            Ok::<_, DynError>(parallel_thread)
        });
        let parallel_result = match parallel_task.await {
            Ok(result) => result,
            Err(error) => Err(error.into()),
        };
        let parallel_thread = match parallel_result {
            Ok(evidence) => evidence,
            Err(error) => {
                let _ = release_tx.send(());
                let _ = begin_task.await;
                return Err(error);
            }
        };

        let first_end_track = tracks.workers[0].1.clone();
        let end_task = tokio::spawn(async move {
            let end_thread = std::thread::current().id();
            let end_worker = current_thread_name();
            if end_thread == begin_thread {
                return Err("logical worker begin and end ran on the same Tokio worker".into());
            }
            sampled_cpu_work(work_duration / 2);
            track_event_end!("delta_funnel.profile", |context: &mut EventContext| {
                first_end_track.set_on(context);
                context.add_debug_arg("end_tokio_worker", TrackEventDebugArg::String(&end_worker));
            });
            Ok::<_, DynError>(end_thread)
        });

        let end_result = end_task.await;
        let _ = release_tx.send(());
        let end_thread = end_result??;
        begin_task.await??;
        Ok(WorkerMigrationEvidence {
            begin_thread,
            end_thread,
            parallel_thread,
        })
    }

    fn current_thread_name() -> String {
        std::thread::current()
            .name()
            .unwrap_or("unnamed-worker")
            .to_owned()
    }

    #[inline(never)]
    fn sampled_cpu_work(duration: Duration) {
        let deadline = Instant::now() + duration;
        let mut value = 1_u64;
        while Instant::now() < deadline {
            value = black_box(value.wrapping_mul(6364136223846793005).wrapping_add(1));
        }
        black_box(value);
    }
}
