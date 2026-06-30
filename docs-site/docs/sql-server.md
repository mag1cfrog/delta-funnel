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

Python currently accepts these load modes:

- `append_existing`
- `create_and_load`
- `replace`

Choose `create_and_load` for a first load into a new table, `append_existing`
for appending to an existing table, and `replace` when the target should be
recreated.

## Integration tests

SQL Server tests are opt-in and managed by xtask:

```bash
cargo xtask sqlserver-test
```

The runner can start a local SQL Server container, create the test database,
run Rust and Python write tests, and remove the container when it exits.

See the detailed guide:
[SQL Server integration tests](https://github.com/mag1cfrog/delta-funnel/blob/main/docs/mssql-integration-tests.md).
