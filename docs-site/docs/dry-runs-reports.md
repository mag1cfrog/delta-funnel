# Dry Runs, Validation, And Reports

Use dry runs to validate a plan before writing rows to SQL Server.

The examples below continue from the [Python quickstart](python-api-walkthrough.md):
`daily_orders` is a lazy table created from SQL.

## Single-output dry run

```python
dry_run_report = daily_orders.write_to_mssql(
    schema="dbo",
    table="daily_orders",
    load_mode="create_and_load",
    dry_run=True,
)
```

Dry-run calls do not write rows. They are meant to check source planning,
target identity, lifecycle choices, and output shape.

## Execute reports

Execute calls return report dictionaries too:

```python
report = daily_orders.write_to_mssql(
    schema="dbo",
    table="daily_orders",
    load_mode="create_and_load",
)
```

For multi-output dry runs, shared caching, and partial failure reports, see
[Multiple outputs and shared caching](advanced/multiple-outputs.md).

For interpreting failures and collecting safe diagnostics, see
[Diagnose failed workflows](advanced/failure-diagnostics.md).

For application diagnostics, see [Python logging](advanced/python-logging.md).
