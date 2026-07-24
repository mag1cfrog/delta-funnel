# Rust Quickstart

This quickstart registers a Delta table, runs a DataFusion query, previews the
result, and writes it to SQL Server. Add the crate as described in
[Installation](install.md) before continuing.

## Before you start

You need:

- an accessible Delta table
- a SQL Server database and login
- permission to create a table in the target schema

The example uses `create_and_load`, so `[dbo].[daily_orders]` must not already
exist.

## Run the workflow

Replace the source path and connection values, then use this as `src/main.rs`:

```rust
use delta_funnel::{
    DeltaFunnelRuntime, DeltaFunnelSession, DeltaSourceConfig, LoadMode,
    MssqlConnectionConfig, MssqlOutputTarget, MssqlTargetConfig,
    MssqlTargetTable, OutputWritePlan, RunMode, SessionOptions,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connection_string = concat!(
        "server=tcp:localhost,1433;",
        "database=warehouse;",
        "User ID=etl_user;",
        "Password=REPLACE_ME;",
        "encrypt=true;",
        "TrustServerCertificate=yes",
    );

    let default_connection = MssqlConnectionConfig::new(connection_string)?
        .with_display_label("warehouse");
    let mut session = DeltaFunnelSession::new(
        SessionOptions::new().with_default_mssql_connection(default_connection),
    )?;
    let runtime = DeltaFunnelRuntime::new()?;

    let _orders = session.delta_lake(DeltaSourceConfig::new(
        "orders",
        "file:///path/to/orders-delta",
    ))?;

    let daily_orders = runtime.table_from_sql(
        &mut session,
        r#"
        select customer_id, order_date, total_amount
        from orders
        where order_date >= date '2026-01-01'
        "#,
    )?;

    let preview = runtime.preview_table(&session, &daily_orders, 20)?;
    println!("{}", preview.text());

    let target = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "daily_orders")?)
        .with_load_mode(LoadMode::CreateAndLoad);
    let output = OutputWritePlan::new(
        daily_orders,
        MssqlOutputTarget::new("daily_orders", target, RunMode::Execute),
    );
    let report = runtime.write_to_mssql(&session, &output)?;

    println!("wrote output {}", report.output_name());
    Ok(())
}
```

Run it with:

```bash
cargo run
```

The command prints a bounded preview, creates `[dbo].[daily_orders]`, writes the
query result, and finishes with `wrote output daily_orders`.

## Next steps

- [Core concepts](concepts.md) explains the workflow objects used above.
- [SQL Server writes](sql-server.md) explains connection and load-mode choices.
- [Dry runs and reports](dry-runs-reports.md) explains how to validate a plan
  before writing.
- [API reference](reference/api.md) links to the complete Rust API.
