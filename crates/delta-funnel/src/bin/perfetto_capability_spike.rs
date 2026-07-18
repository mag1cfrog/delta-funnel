//! Opt-in Perfetto system-backend capability harness.
//!
//! This binary is intentionally separate from Delta Funnel production paths.
//! It proves that one logical Perfetto slice can begin and end on different
//! Tokio worker threads while system tracing samples the Rust call stack.
//!
//! Start `tracebox` first with `tools/perfetto/capability-spike.pbtx` and
//! `--system-sockets`, then run this binary from the `profiling` build profile.

#![allow(
    missing_docs,
    reason = "the Perfetto SDK macro generates undocumented public helpers"
)]

use std::error::Error;
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use perfetto_sdk::producer::{Backends, Producer, ProducerInitArgsBuilder};
use perfetto_sdk::track_event::{EventContext, TrackEvent, TrackEventDebugArg, TrackEventTrack};
use perfetto_sdk::{
    track_event_begin, track_event_categories, track_event_category_enabled, track_event_end,
};

const CATEGORY: &str = "delta_funnel.perfetto_spike";
const CAPTURE_WAIT_TIMEOUT: Duration = Duration::from_secs(10);
const RELEASE_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
const SAMPLED_WORK_DURATION: Duration = Duration::from_millis(750);
static NEXT_WORKER_ID: AtomicUsize = AtomicUsize::new(1);

type DynError = Box<dyn Error + Send + Sync>;

track_event_categories! {
    pub mod delta_funnel_perfetto {
        (
            "delta_funnel.perfetto_spike",
            "Delta Funnel Perfetto capability spike",
            []
        ),
    }
}

use delta_funnel_perfetto as perfetto_te_ns;

fn main() -> Result<(), DynError> {
    initialize_perfetto()?;
    wait_for_capture()?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name_fn(|| {
            let worker_id = NEXT_WORKER_ID.fetch_add(1, Ordering::Relaxed);
            format!("perfetto-spike-worker-{worker_id}")
        })
        .build()?;
    runtime.block_on(emit_cross_worker_slice())?;

    Ok(())
}

fn initialize_perfetto() -> Result<(), DynError> {
    let producer_args = ProducerInitArgsBuilder::new().backends(Backends::SYSTEM);
    Producer::init(producer_args.build());
    TrackEvent::init();
    perfetto_te_ns::register()?;
    Ok(())
}

fn wait_for_capture() -> Result<(), DynError> {
    let deadline = Instant::now() + CAPTURE_WAIT_TIMEOUT;
    while !track_event_category_enabled!("delta_funnel.perfetto_spike") {
        if Instant::now() >= deadline {
            return Err(format!(
                "Perfetto category {CATEGORY:?} was not enabled within {CAPTURE_WAIT_TIMEOUT:?}"
            )
            .into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Ok(())
}

async fn emit_cross_worker_slice() -> Result<(), DynError> {
    let track = Arc::new(TrackEventTrack::register_named_track(
        "Delta Funnel capability track",
        0,
        TrackEventTrack::process_track_uuid(),
    )?);
    let (begun_tx, begun_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();

    let begin_track = Arc::clone(&track);
    let begin_task = tokio::spawn(async move {
        let begin_thread = std::thread::current().id();
        let begin_worker = current_thread_name();
        track_event_begin!(
            "delta_funnel.perfetto_spike",
            "Cross-worker logical operation",
            |ctx: &mut EventContext| {
                ctx.set_track(&begin_track);
                ctx.add_debug_arg(
                    "begin_tokio_worker",
                    TrackEventDebugArg::String(&begin_worker),
                );
            }
        );
        begun_tx
            .send((begin_thread, begin_worker))
            .map_err(|_| "begin thread receiver closed")?;
        release_rx
            .recv_timeout(RELEASE_WAIT_TIMEOUT)
            .map_err(|error| format!("timed out waiting to release begin worker: {error}"))?;
        Ok::<_, DynError>(())
    });

    let (begin_thread, begin_worker) = begun_rx.await?;
    let end_task = tokio::spawn(async move {
        let end_thread = std::thread::current().id();
        let end_worker = current_thread_name();
        if end_thread == begin_thread {
            return Err("slice begin and end ran on the same Tokio worker".into());
        }
        sampled_cpu_work(SAMPLED_WORK_DURATION);
        track_event_end!("delta_funnel.perfetto_spike", |ctx: &mut EventContext| {
            ctx.set_track(&track);
            ctx.add_debug_arg("end_tokio_worker", TrackEventDebugArg::String(&end_worker));
            ctx.set_flush();
        });
        Ok::<_, DynError>((end_thread, end_worker))
    });

    let end_result = end_task.await;
    let _ = release_tx.send(());
    let (end_thread, end_worker) = end_result??;
    begin_task.await??;
    println!(
        "captured logical slice across Tokio workers: begin={begin_worker}/{begin_thread:?}, end={end_worker}/{end_thread:?}"
    );
    Ok(())
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
