//! Opt-in Perfetto system-backend capability harness.
//!
//! This binary is intentionally separate from Delta Funnel production paths.
//! It proves that deterministic parented semantic tracks remain independent
//! of Tokio worker threads while system tracing samples the Rust call stack.
//!
//! Start `tracebox` first with `tools/perfetto/capability-spike.pbtx` and
//! `--system-sockets`, then run this binary from the `profiling` build profile.

#![allow(
    missing_docs,
    reason = "the Perfetto SDK macro generates undocumented public helpers"
)]

use std::error::Error;
#[cfg(test)]
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(test)]
use std::sync::mpsc;
#[cfg(test)]
use std::thread::ThreadId;
use std::time::Duration;
#[cfg(test)]
use std::time::Instant;

use delta_funnel::{DeltaFunnelSession, SessionOptions};
#[cfg(test)]
use perfetto_sdk::track_event::EventContext;
#[cfg(test)]
use perfetto_sdk::track_event::TrackEventDebugArg;
#[cfg(test)]
use perfetto_sdk::{track_event_begin, track_event_end};
use tracing_subscriber::{Layer, filter::filter_fn, prelude::*};

#[path = "perfetto_profile/mod.rs"]
mod perfetto_profile;

use perfetto_profile::{
    PROFILE_TARGET, PerfettoProfileLayer, initialize_perfetto, wait_for_capture,
};
#[cfg(test)]
use perfetto_profile::{
    SemanticTrack, diagnostics_track, operation_track, perfetto_te_ns, phase_track, query_token,
    query_track, worker_token, worker_track,
};

const PROCESS_METADATA_GRACE_PERIOD: Duration = Duration::from_millis(250);
#[cfg(test)]
const RELEASE_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
static NEXT_WORKER_ID: AtomicUsize = AtomicUsize::new(1);

type DynError = Box<dyn Error + Send + Sync>;

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct SemanticTracks {
    diagnostics: SemanticTrack,
    operation: SemanticTrack,
    phases: SemanticTrack,
    query: SemanticTrack,
    workers: [(u64, SemanticTrack); 2],
}

#[cfg(test)]
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

#[cfg(test)]
#[derive(Debug)]
struct WorkerMigrationEvidence {
    begin_thread: ThreadId,
    end_thread: ThreadId,
    parallel_thread: ThreadId,
}

fn main() -> Result<(), DynError> {
    initialize_perfetto()?;
    wait_for_capture()?;

    let perfetto_layer =
        PerfettoProfileLayer.with_filter(filter_fn(|metadata| metadata.target() == PROFILE_TARGET));
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(perfetto_layer))?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_name_fn(|| {
            let worker_id = NEXT_WORKER_ID.fetch_add(1, Ordering::Relaxed);
            format!("perfetto-spike-worker-{worker_id}")
        })
        .build()?;
    runtime.block_on(emit_representative_preview())?;
    // Keep the short-lived harness available while linux.perf captures its maps.
    std::thread::sleep(PROCESS_METADATA_GRACE_PERIOD);

    Ok(())
}

async fn emit_representative_preview() -> Result<(), DynError> {
    let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
    let table = session
        .table_from_sql(
            "SELECT value % 1024 AS bucket, SUM(value) AS total \
             FROM generate_series(1, 13394789) AS series(value) \
             GROUP BY value % 1024 ORDER BY bucket",
        )
        .await?;
    let preview = session.preview_table(&table, 1024).await?;
    println!(
        "captured representative Delta Funnel preview: {} rendered bytes",
        preview.text().len()
    );
    Ok(())
}

#[cfg(test)]
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
            "delta_funnel.perfetto_spike",
            "Logical worker activity",
            |context: &mut EventContext| {
                first_track.set_on(context);
                context.add_debug_arg("worker_lane_id", TrackEventDebugArg::Uint64(first_lane_id));
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
            "delta_funnel.perfetto_spike",
            "Logical worker activity",
            |context: &mut EventContext| {
                parallel_track.set_on(context);
                context.add_debug_arg(
                    "worker_lane_id",
                    TrackEventDebugArg::Uint64(parallel_lane_id),
                );
                context.add_debug_arg("tokio_worker", TrackEventDebugArg::String(&parallel_worker));
            }
        );
        sampled_cpu_work(work_duration);
        track_event_end!(
            "delta_funnel.perfetto_spike",
            |context: &mut EventContext| {
                parallel_track.set_on(context);
            }
        );
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
        track_event_end!(
            "delta_funnel.perfetto_spike",
            |context: &mut EventContext| {
                first_end_track.set_on(context);
                context.add_debug_arg("end_tokio_worker", TrackEventDebugArg::String(&end_worker));
            }
        );
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

#[cfg(test)]
fn current_thread_name() -> String {
    std::thread::current()
        .name()
        .unwrap_or("unnamed-worker")
        .to_owned()
}

#[cfg(test)]
#[inline(never)]
fn sampled_cpu_work(duration: Duration) {
    let deadline = Instant::now() + duration;
    let mut value = 1_u64;
    while Instant::now() < deadline {
        value = black_box(value.wrapping_mul(6364136223846793005).wrapping_add(1));
    }
    black_box(value);
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

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
}
