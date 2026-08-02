# Multiple Outputs and Shared Caching

Use `Session.write_all(...)` when one workflow writes several related lazy
tables to SQL Server. Shared lazy SQL dependencies can be cached so common
upstream work is not repeated for every output.

The examples below assume `west` and `east` are lazy tables created from SQL.

## Define the outputs

Create one output spec from each table:

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
```

## Dry-run every output

Validate the workflow without reading or writing rows:

```python
dry_run_report = session.write_all(outputs, dry_run=True)
```

The report describes source planning, target identity, lifecycle choices, and
output shape. The `options` argument is not accepted for dry runs.

## Execute with shared caching

Execute all outputs with the default `auto` cache mode:

```python
report = session.write_all(outputs)
```

To cache a known dependency chain, explicitly select its registered derived
aliases:

```python
report = session.write_all(
    outputs,
    options={
        "cache_mode": "explicit",
        "cache_aliases": ["prepared_orders", "classified_orders"],
    },
)
```

Explicit mode validates every selected alias before execution, then
materializes the aliases in dependency order. `cache_mode="explicit"` and
`cache_aliases` must be supplied together.

Use the baseline path when shared caching is not wanted:

```python
report = session.write_all(
    outputs,
    options={"cache_mode": "disabled"},
)
```

## Profile every attempted output

Profiling is optional and belongs to the Advanced path. See
[Inspect write-all profiles](execution-profiling.md#inspect-write-all-profiles)
to enable profiling, find each output's exact profile, or generate one ranked
report for the complete operation.

## Interpret failures

A report can contain failed or skipped outputs when top-level orchestration
completes. A top-level planning, cache, orchestration, or cache-restoration
error raises an exception instead. A cache failure retains every attempted
alias report. If output execution completed before cache restoration failed,
the failure also retains that completed workflow report. See the
[write-all cache diagnostics reference](../reference/diagnostics.md#inspect-returned-write-all-cache-diagnostics)
for phase boundaries and Python and Rust failure access.

For consolidated progress across planning, shared cache work, and every output,
see [Progress displays](../progress.md).
