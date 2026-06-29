# DeltaFunnel

DeltaFunnel loads Delta Lake query results into Microsoft SQL Server.

It combines Delta metadata scanning, DataFusion SQL planning, Arrow batch
handoff, and SQL Server bulk writes behind a Rust API and a PyO3 native Python
package named `deltafunnel`.

> [!NOTE]
> DeltaFunnel is early project code. The Python package can be built and used
> locally, but PyPI publishing is not yet configured.

## What It Does

- Registers Delta Lake sources from local paths or object-store URIs.
- Builds lazy SQL-derived tables with DataFusion.
- Plans and executes one or many SQL Server outputs.
- Supports dry-run reports that plan sources, schemas, targets, and validation
  without writing rows.
- Produces JSON-safe report dictionaries for Python users and report values for
  Rust users.
- Provides an opt-in SQL Server integration test runner through `cargo xtask`.

## Python quickstart

The Python package is a PyO3 native extension module. It exposes the Rust
workflow API directly and does not add a pure Python wrapper layer.

Build and install a local wheel:

```bash
cargo xtask python-package-check
```

Create a session with a default SQL Server connection string. `Session()` uses
Rust defaults unless options are supplied.

```python
from deltafunnel import Session

connection_string = (
    "Driver={ODBC Driver 18 for SQL Server};"
    "Server=localhost;"
    "Database=warehouse;"
    "Trusted_Connection=yes;"
    "Encrypt=yes;"
    "TrustServerCertificate=yes"
)
source_uri = "file:///path/to/local/delta-table"

session = Session(default_mssql_connection_string=connection_string)
orders = session.delta_lake(source_uri, name="orders")

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

Use `dry_run=True` on the same action methods to plan without writing:

```python
dry_run_report = daily_orders.write_to_mssql(
    schema="dbo",
    table="daily_orders",
    load_mode="create_and_load",
    dry_run=True,
)
```

There are no public Python `dry_run_*` methods.

## Multi-output Python writes

`Table.to_mssql(...)` creates an output spec without writing. `Session.write_all`
writes the specs in one workflow. The default output name is the target `table`;
pass `name=...` to override it for reports and duplicate-name checks.

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
report = session.write_all(outputs, options={"cache_mode": "auto"})
```

`options={"cache_mode": "auto"}` is the default execute behavior. It may cache
shared lazy SQL aliases during one `write_all` call. Use
`options={"cache_mode": "disabled"}` to force the baseline path.

> [!IMPORTANT]
> `options` is only accepted for execute `write_all` calls, not dry runs.

Reports are plain Python `dict` values converted from Rust JSON-safe report
shapes. The first Python surface does not include persistent `cache`, `persist`,
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

Build and test the workspace:

```bash
cargo fmt --all --check
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

Build the Python wheel:

```bash
cd crates/delta-funnel-python
maturin build --skip-auditwheel
```

Build, install, and smoke-test the Python wheel in a clean temporary
virtualenv:

```bash
cargo xtask python-package-check
```

SQL Server integration tests are opt-in:

```bash
cargo xtask sqlserver-test
```

The xtask runner can start a local SQL Server container, run Rust and Python
write tests, and remove the container when it exits. See
[`docs/mssql-integration-tests.md`](docs/mssql-integration-tests.md).

## Documentation

The Delta DataFusion scan partition target policy is documented in
[`docs/scan-partition-target-policy.md`](docs/scan-partition-target-policy.md).

Failure-report collection, validation limits, and safe tracing setup are
documented in
[`docs/failure-reports-and-tracing.md`](docs/failure-reports-and-tracing.md).
