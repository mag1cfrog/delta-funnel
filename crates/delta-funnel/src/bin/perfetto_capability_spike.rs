//! Opt-in Perfetto system-backend capability harness.
//!
//! This binary is intentionally separate from Delta Funnel production paths.
//! It proves that deterministic parented semantic tracks remain independent
//! of Tokio worker threads while system tracing samples the Rust call stack.
//!
//! Start `tracebox` first with `tools/perfetto/capability-spike.pbtx` and
//! `--system-sockets`, then run this binary from the `profiling` build profile.

use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use delta_funnel::{
    DeltaFunnelSession, SessionOptions,
    perfetto_profile::{
        PROFILE_TARGET, PerfettoProfileLayer, initialize_perfetto, wait_for_capture,
    },
};
use tracing_subscriber::{Layer, filter::filter_fn, prelude::*};

const PROCESS_METADATA_GRACE_PERIOD: Duration = Duration::from_millis(250);
const CAPTURE_WAIT_TIMEOUT: Duration = Duration::from_secs(10);
static NEXT_WORKER_ID: AtomicUsize = AtomicUsize::new(1);

type DynError = Box<dyn Error + Send + Sync>;

fn main() -> Result<(), DynError> {
    initialize_perfetto()?;
    wait_for_capture(CAPTURE_WAIT_TIMEOUT)?;

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
