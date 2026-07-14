# Python Quickstart

This quickstart uses the `deltafunnel` package. Install it from PyPI as
described in [Installation](install.md) before continuing.

## Before you start

You need:

- an accessible Delta table with the columns used below, or a query adjusted
  to match your table
- a SQL Server database and login
- permission to create a table in the target schema

The example uses `create_and_load`, so `[dbo].[daily_orders]` must not already
exist.

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

print(report["output_name"])
print(report["write_stats"]["rows_written"])
print(report["validation_status"]["kind"])
```

The returned report is a plain Python `dict` converted from Rust report types.
Report formatting is designed to avoid exposing connection strings,
credentials, and raw row values.

A successful call returns without raising an exception and creates
`[dbo].[daily_orders]`. The first printed value is `daily_orders`; the row count
and validation status depend on the data and available target validation.

## Next steps

- [Core concepts](concepts.md) explains sessions, sources, tables, outputs, and
  reports.
- [SQL Server writes](sql-server.md) explains connection and load-mode choices.
- [Dry runs and reports](dry-runs-reports.md) explains how to validate this plan
  before writing.
- [Progress displays](progress.md) explains terminal and notebook progress.
- [Private S3 sources](advanced/private-s3.md) explains credentials and source
  access.
- [API references](reference/api.md) covers deferred source registration and
  the complete typed surface.
