# Python API Walkthrough

This walkthrough uses the `deltafunnel` package. Install it from PyPI, or build
the local wheel first with `cargo xtask python-package-check` when developing
the repository.

## Create a session

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
```

Delta Funnel accepts a SQL Server ADO-style connection string. It does not
require an ODBC DSN.

## Register a Delta source

```python
orders = session.delta_lake("file:///path/to/orders-delta", name="orders")
```

Passing `name` registers the source immediately so SQL can reference it.
Calling `session.delta_lake(...)` without `name` returns a pending source; call
`.alias("orders")` before using it in SQL.

## Read a private S3 Delta table from a local shell

`storage_options` are forwarded to the underlying object-store builder that
Delta Funnel uses for S3 access. On the current S3 path, Delta Funnel does not
auto-load shell `AWS_*` variables, `AWS_PROFILE`, or shared AWS config and
credentials files.

For a private S3 Delta table from a local shell, pass explicit credentials and
region in `storage_options`:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_SESSION_TOKEN` as optional
- `AWS_REGION`

Delta Funnel also accepts these common lowercase aliases:

- `aws_access_key_id`
- `aws_secret_access_key`
- `aws_session_token`
- `aws_region`
- `region`

This works reliably:

```python
import os
from deltafunnel import Session

storage_options = {
    "AWS_REGION": "us-east-1",
    "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
    "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
}
if os.environ.get("AWS_SESSION_TOKEN"):
    storage_options["AWS_SESSION_TOKEN"] = os.environ["AWS_SESSION_TOKEN"]

source = Session().delta_lake(
    "s3://<private-bucket>/<delta-table>",
    storage_options=storage_options,
    name="source",
)
```

This is not enough by itself:

```python
Session().delta_lake(
    "s3://<private-bucket>/<delta-table>",
    storage_options={"region": "us-east-1"},
    name="source",
)
```

`region` is a supported key, but `region` alone only sets region. It does not
provide credentials.

If the same table works in `deltalake` but fails in `deltafunnel`, the likely
cause is a credential-discovery path mismatch, not a Delta snapshot or protocol
problem.

## Transform rows with SQL

```python
daily_orders = session.table_from_sql("""
    select customer_id, order_date, total_amount
    from orders
    where order_date >= date '2026-01-01'
""")
```

`table_from_sql` creates a lazy table. It does not execute rows until a
terminal action reads or writes the table.

## Preview rows

```python
preview = daily_orders.preview(limit=20)
daily_orders.show(limit=20)
```

`preview()` and `show()` execute the DataFusion query and read rows with the
limit applied before collection. They do not contact SQL Server or write rows.
`preview()` returns a `Preview` object with text and notebook HTML
representations. `show()` prints the text preview to Python stdout.

## Write to SQL Server

```python
report = daily_orders.write_to_mssql(
    schema="dbo",
    table="daily_orders",
    load_mode="create_and_load",
)
```

The returned report is a plain Python `dict` converted from Rust report types.
Report formatting is designed to avoid exposing connection strings,
credentials, and raw row values.

### Progress display

`progress=None`, the default, shows a Rich progress display when Rich detects
an interactive terminal or Jupyter. Use `progress=True` to force the display in
scripts or CI, or `progress=False` to disable it.

```python
report = daily_orders.write_to_mssql(
    schema="dbo",
    table="daily_orders",
    load_mode="create_and_load",
    progress=True,
)
```

The display starts without a percentage. When the query plan has complete
statistics for at least one selected Delta file, the same display becomes a
determinate bar. Its percentage measures selected Delta files handled. Files
removed by Delta metadata pruning are outside that selected total. File sizes
can differ, so the percentage is not an estimate of bytes read, elapsed time,
or whole-action completion.

The file line can show `Delta files 8/10 | pruned 3 at runtime, ~90 in
planning`. Runtime pruning is the exact number of selected files skipped while
the query ran. Planning pruning is an approximate count from Delta Kernel
metadata selection, so the display prefixes it with `~`.

The description also shows cumulative rows and batches after SQL Server accepts
each batch. Dry runs do not have write counters. Queries without eligible Delta
scan statistics, including queries with zero selected files, remain
indeterminate.

Rapid numeric updates are combined before rendering. Status changes and the
final result still appear immediately. If an action fails, the final display
keeps the latest actual file position and accepted write counters instead of
filling the bar.

Progress reuses statistics already maintained by the active query plan. It does
not run an extra count query, expand Delta metadata again, or make additional
object-store requests.
