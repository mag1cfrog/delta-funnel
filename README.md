# Delta Funnel

<h3 align="center">
  <strong>Move Delta Lake data into SQL Server without Spark or ODBC.</strong>
</h3>

<p align="center">
  A lightweight Rust and Python toolkit for reading Delta Lake tables,<br/>
  transforming them with SQL, and bulk-loading through a native TDS driver.
</p>

<p align="center">
  Built in Rust. Python API included. Local builds available today.
</p>

> [!NOTE]
> Delta Funnel is early project code. Local Rust and Python builds work, but
> PyPI, crates.io, and hosted documentation publishing are not configured yet.

## When To Use It

Use Delta Funnel when you need to:

- Read Delta Lake tables from local paths or object-store URIs.
- Transform rows with DataFusion SQL.
- Load one or more results into Microsoft SQL Server.
- Use native TDS bulk writes designed to be significantly faster than ODBC-based loads.
- Run the workflow from Rust or from a PyO3 native extension module in Python.
- Avoid standing up Spark for a focused Delta Lake to SQL Server pipeline.

## Install Or Build

Until package publishing is configured, build and smoke-test the Python wheel
for the `deltafunnel` package from the repository:

```bash
cargo xtask python-package-check
```

For Rust, use the `delta-funnel` workspace crate directly:

```bash
cargo check --workspace
```

## Python Quickstart

```python
from deltafunnel import Session

connection_string = (
    "server=tcp:localhost,1433;"
    "database=warehouse;"
    "user id=etl_user;"
    "password=REPLACE_ME;"
    "encrypt=true;"
    "TrustServerCertificate=yes"
)

session = Session(default_mssql_connection_string=connection_string)
orders = session.delta_lake("file:///path/to/orders-delta", name="orders")

daily_orders = session.table_from_sql("""
    select customer_id, order_date, total_amount
    from orders
    where order_date >= date '2026-01-01'
""")

report = daily_orders.write_to_mssql(
    schema="dbo",
    table="daily_orders",
    load_mode="create_and_load",
)
```

`session.delta_lake(..., name="orders")` registers a Delta source immediately.
`session.delta_lake(...)` without `name` returns a pending source; call
`.alias("orders")` before SQL references it.

Reports are plain Python `dict` values converted from Rust report types. Report
formatting is designed to avoid exposing connection strings, credentials, and
raw row values. See
[`docs/failure-reports-and-tracing.md`](docs/failure-reports-and-tracing.md)
for the failure-report and tracing rules.

## Dry Runs

Use `dry_run=True` on the same write methods to validate the plan without
writing rows:

```python
dry_run_report = daily_orders.write_to_mssql(
    schema="dbo",
    table="daily_orders",
    load_mode="create_and_load",
    dry_run=True,
)
```

There are no public Python `dry_run_*` methods.

## Multi-output Writes

`Table.to_mssql(...)` creates an output spec without writing. `Session.write_all`
writes the specs in one workflow.

```python
active_orders = session.table_from_sql("""
    select *
    from orders
    where status = 'active'
""").alias("active_orders")

west = session.table_from_sql("""
    select * from active_orders where region = 'west'
""")
east = session.table_from_sql("""
    select * from active_orders where region = 'east'
""")

outputs = [
    west.to_mssql(
        schema="dbo",
        table="active_orders_west",
        load_mode="append_existing",
        name="west_active_orders",
    ),
    east.to_mssql(
        schema="dbo",
        table="active_orders_east",
        load_mode="append_existing",
        name="east_active_orders",
    ),
]

dry_run_report = session.write_all(outputs, dry_run=True)
report = session.write_all(outputs, options={"cache_mode": "disabled"})
```

`options={"cache_mode": "auto"}` is the default execute behavior. It may cache
shared lazy SQL aliases during one `write_all` call. Use
`options={"cache_mode": "disabled"}` to force the baseline path.

> [!IMPORTANT]
> `options` is only accepted for execute `write_all` calls, not dry runs.

The first Python surface does not include persistent `cache`, `persist`,
or `materialize` APIs.

## Rust API

The Rust crate owns the workflow implementation and public report types. A
minimal dry-run example is available at
[`crates/delta-funnel/examples/query_load_dry_run.rs`](crates/delta-funnel/examples/query_load_dry_run.rs).

Run it with a local Delta table path:

```bash
DELTA_FUNNEL_EXAMPLE_ORDERS_DELTA=/path/to/orders \
  cargo run -p delta-funnel --example query_load_dry_run
```

Core Rust entry points include:

- `DeltaFunnelSession` for source registration and session state.
- `DeltaFunnelRuntime` for lazy SQL planning, dry runs, and writes.
- `OutputWritePlan` and `MssqlOutputTarget` for output planning.
- `WriteAllOptions` and `WriteAllCacheMode` for multi-output execution.

## Build And Test

```bash
cargo fmt --all --check
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

SQL Server integration tests are opt-in:

```bash
cargo xtask sqlserver-test
```

The xtask runner can start a local SQL Server container, run Rust and Python
write tests, and remove the container when it exits. See
[`docs/mssql-integration-tests.md`](docs/mssql-integration-tests.md).
