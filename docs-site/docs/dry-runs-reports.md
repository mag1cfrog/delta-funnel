# Dry Runs, Validation, And Reports

Use dry runs to validate a plan before writing rows to SQL Server.

The examples below continue from the [Python quickstart](python-api-walkthrough.md):
`session` is a `Session`, `daily_orders` is a lazy table, and `west` and `east`
are lazy tables created from SQL.

## Single-output dry run

```python
dry_run_report = daily_orders.write_to_mssql(
    schema="dbo",
    table="daily_orders",
    load_mode="create_and_load",
    dry_run=True,
)
```

## Multi-output dry run

```python
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
```

Dry-run calls do not write rows. They are meant to check source planning,
target identity, lifecycle choices, and output shape.

For consolidated progress across planning, shared cache work, and every output,
see [Progress displays](progress.md).

## Execute reports

Execute calls return report dictionaries too:

```python
report = session.write_all(outputs, options={"cache_mode": "auto"})
```

`options={"cache_mode": "auto"}` is the default execute behavior. Use
`options={"cache_mode": "disabled"}` to force the baseline path.

!!! important
    `options` is only accepted for execute `write_all` calls, not dry runs.

A report can contain failed or skipped outputs when top-level orchestration
completes. A top-level planning, cache, orchestration, or cache-restoration
error raises an exception instead. Cache restoration happens before the result
is delivered, so a restoration error supersedes any completed report.

For failure-report and tracing rules, see
[Failure Reports And Safe Tracing](https://github.com/mag1cfrog/delta-funnel/blob/main/docs/failure-reports-and-tracing.md).

## Python logging

For Python diagnostics, route DeltaFunnel events into standard-library
`logging` before running the workflow:

```python
import logging
import deltafunnel

logging.basicConfig(level=logging.INFO)
deltafunnel.init_logging()
```

Use `DELTAFUNNEL_LOG` or an explicit filter string such as
`delta_funnel=debug,delta_kernel=debug,object_store=debug` when you need more
detail. DeltaFunnel does not configure handlers or exporters; existing Datadog,
OpenTelemetry, JSON logging, file logging, pytest capture, and framework
integrations continue to own Python logging output.
