# SQL Server

Delta Funnel writes to Microsoft SQL Server through a native TDS path. It does
not require Spark's SQL Server connector or an ODBC driver.

## Connection strings

Pass a SQL Server ADO-style connection string to `Session` or to a specific
output:

```python
connection_string = (
    "server=tcp:localhost,1433;"
    "database=warehouse;"
    "user id=etl_user;"
    "password=REPLACE_ME;"
    "encrypt=true;"
    "TrustServerCertificate=yes"
)
```

Use a per-output `connection_string` when different outputs write to different
targets. Otherwise, set `default_mssql_connection_string` on `Session`.

## Load modes

Python accepts these load modes:

- `append_existing`
- `create_and_load`
- `replace`

Choose `create_and_load` when the target must not already exist,
`append_existing` for appending to an existing table, and `replace` when the
target should exactly match the output rows. `replace` can rebuild an existing
target or create a missing one, including an empty target for empty output.

`replace` writes to a staging table, validates that staging table, then swaps
it into an existing final target or promotes it to a missing final target. The
replacement table is recreated from the DeltaFunnel-planned SQL Server schema,
so existing table metadata such as indexes, constraints, triggers, permissions,
and extended properties is not preserved.
