//! Minimal Rust backing flow for a Delta-to-MSSQL dry run.
//!
//! This example is a how-to for Rust integrators who want to see the same
//! public API shape that a future Python `Session` wrapper will call.
//!
//! Run it with a local Delta table path:
//!
//! `DELTA_FUNNEL_EXAMPLE_ORDERS_DELTA=/path/to/orders cargo run -p delta-funnel --example query_load_dry_run`

use std::env;

use delta_funnel::{
    DeltaFunnelRuntime, DeltaFunnelSession, DeltaSourceConfig, LoadMode, MssqlConnectionConfig,
    MssqlOutputTarget, MssqlTargetConfig, MssqlTargetTable, OutputWritePlan, RunMode,
    SessionOptions,
};

const ORDERS_DELTA_ENV: &str = "DELTA_FUNNEL_EXAMPLE_ORDERS_DELTA";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let Some(orders_delta_uri) = example_orders_delta_uri() else {
        println!("Set {ORDERS_DELTA_ENV} to a local Delta table path to run this dry-run example.");
        return Ok(());
    };

    let default_connection = MssqlConnectionConfig::new(
        "server=tcp:localhost,1433;database=warehouse;user=sa;password=example-password",
    )?
    .with_display_label("example-mssql");
    let mut session = DeltaFunnelSession::new(
        SessionOptions::new().with_default_mssql_connection(default_connection),
    )?;
    let runtime = DeltaFunnelRuntime::new()?;

    let _orders = session.delta_lake(DeltaSourceConfig::new("orders", orders_delta_uri))?;
    let selected_orders = runtime.table_from_sql(&mut session, "select * from orders")?;

    let target = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders_output")?)
        .with_load_mode(LoadMode::AppendExisting);
    let output = OutputWritePlan::new(
        selected_orders,
        MssqlOutputTarget::new("orders_output", target, RunMode::DryRun),
    );
    let report = runtime.dry_run_to_mssql(&session, &output)?;

    println!(
        "planned output {} in {:?}; SQL Server contacted: {}",
        report.output_name(),
        report.run_mode(),
        report.sql_server_contacted()
    );

    Ok(())
}

fn example_orders_delta_uri() -> Option<String> {
    env::var(ORDERS_DELTA_ENV)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}
