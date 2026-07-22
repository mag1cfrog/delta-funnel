//! Stages optional diagnostics assets for the Python wheel.

use std::env;
use std::fs;
use std::io;
use std::path::PathBuf;

const PERFETTO_ASSETS: [&str; 5] = [
    "capture-health",
    "capture-workload",
    "delta-funnel-deep-system.pbtx",
    "delta-funnel-standard-streaming.pbtx",
    "delta-funnel-standard.pbtx",
];

fn main() -> io::Result<()> {
    let python_version = env!("CARGO_PKG_VERSION").replace("-dev.", ".dev");
    println!("cargo:rustc-env=DELTAFUNNEL_PY_VERSION={python_version}");

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let source_dir = manifest_dir.join("../../tools/perfetto");
    let capture_health_sql =
        manifest_dir.join("../delta-funnel/src/perfetto_profile/sql/capture_health.sql");
    let out_dir =
        PathBuf::from(env::var_os("OUT_DIR").ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "Cargo did not provide OUT_DIR")
        })?);
    let destination_dir = out_dir.join("perfetto");

    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_PERFETTO_PROFILE");
    for asset in PERFETTO_ASSETS {
        println!(
            "cargo:rerun-if-changed={}",
            source_dir.join(asset).display()
        );
    }
    println!("cargo:rerun-if-changed={}", capture_health_sql.display());

    if destination_dir.exists() {
        fs::remove_dir_all(&destination_dir)?;
    }
    if env::var_os("CARGO_FEATURE_PERFETTO_PROFILE").is_none() {
        return Ok(());
    }

    fs::create_dir_all(&destination_dir)?;
    for asset in PERFETTO_ASSETS {
        fs::copy(source_dir.join(asset), destination_dir.join(asset))?;
    }
    fs::copy(
        capture_health_sql,
        destination_dir.join("capture-health.sql"),
    )?;

    Ok(())
}
