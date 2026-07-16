# Multiple Outputs And Shared Caching

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

Use the baseline path when shared caching is not wanted:

```python
report = session.write_all(
    outputs,
    options={"cache_mode": "disabled"},
)
```

## Profile every attempted output

Enable detailed DataFusion profiling for each output query:

```python
report = session.write_all(outputs, options={"profile": True})
```

Export the complete workflow on one wall clock when phase and output ordering
matters:

```python
report = session.write_all(
    outputs,
    options={"profile": True},
    trace_path="write-all-trace.json",
)
```

Open the file with `vizviewer write-all-trace.json`, Perfetto, or another
Chrome Trace Event viewer. The root event is the total `write_all` duration.
Top-level planning, workflow execution, source reporting, sequential output
attempts, SQL Server work, and output-query operator lifecycles are positioned
relative to that same origin. The returned dictionary also exposes the model
under `report["operation_timeline"]`. Detailed output queries also record
wall-clock operator activity grouped into query and executor-worker tracks.

With automatic caching, the trace also positions each alias's resolution,
planning, execution, `MemTable` construction, installation, restoration, and
DataFusion operator lifecycles on labeled cache lanes.
Cache materialization queries also use query and executor-worker activity
tracks, so selecting one exact worker produces a top-down flame view.

Profiling works with both cache modes. For example, disable shared caching and
enable profiling in the same call:

```python
report = session.write_all(
    outputs,
    options={"cache_mode": "disabled", "profile": True},
)
```

Profiles stay with the output result that produced them:

```python
for output in report["workflow"]["outputs"]:
    if output["kind"] == "succeeded":
        profile = output["report"]["execution_profile"]
    elif output["kind"] == "failed":
        context = output["failure"]["context"]
        profile = None if context is None else context["report"]["execution_profile"]
    else:
        profile = output["skipped"]["execution_profile"]  # Always None.
```

An attempted output can still have `None` when it failed before a profile
observer could be installed. An output skipped after an earlier failure was
not attempted and always has `execution_profile=None`. The top-level report and
output status wrappers do not duplicate these fields.

Omitting `profile`, or passing `None` or `False`, disables profiling. Only the
actual Boolean `True` enables it. The `options` argument remains unavailable
for dry runs, and `trace_path` is execute-only. For exact Python and Rust
contracts, profile outcomes, and the profile schema, see
[API references](../reference/api.md#multi-output-sql-server-profiling).

## Interpret failures

A report can contain failed or skipped outputs when top-level orchestration
completes. A top-level planning, cache, orchestration, or cache-restoration
error raises an exception instead. A cache failure retains every attempted
alias report. If output execution completed before cache restoration failed,
the failure also retains that completed workflow report. See
[returned write-all cache diagnostics](tracing-and-diagnostics.md#inspect-returned-write-all-cache-diagnostics)
for phase boundaries and Python and Rust failure access.

For consolidated progress across planning, shared cache work, and every output,
see [Progress displays](../progress.md).
