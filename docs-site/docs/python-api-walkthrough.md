# Python Quickstart

This quickstart uses the `deltafunnel` package. Install it from PyPI as
described in [Installation](install.md) before continuing.

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

For a pending source, register the alias later:

```python
pending = session.delta_lake("file:///path/to/orders-delta")
orders = pending.alias("orders")
```

For progress modes and source-registration behavior, see
[Progress displays](progress.md).

For private S3 credentials and troubleshooting, see
[Private S3 sources](advanced/private-s3.md).

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

For preview progress and terminal or notebook rendering, see
[Progress displays](progress.md).

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

For write progress, file counters, and row counters, see
[Progress displays](progress.md).
