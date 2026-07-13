# Python Progress

Delta Funnel can show live Rich progress for Python actions that load Delta
metadata, execute DataFusion queries, or write to SQL Server. Progress is a
presentation layer. It does not change the query, add cancellation, or replace
diagnostic logging.

## Choose when progress appears

Every supported action accepts the same keyword-only `progress` argument.

| Value | Behavior |
| --- | --- |
| `None` | Show progress when Rich detects an interactive terminal or Jupyter. Stay quiet in scripts, pipes, and CI. |
| `True` | Show progress even when the output is not interactive. |
| `False` | Disable progress for this call. |

Terminal progress uses stderr, so application output on stdout remains clean.
In Jupyter, Rich selects its notebook display path. Preview progress uses the
notebook output stream and finishes before the preview HTML or `show()` text is
emitted.

Delta Funnel does not install IPython, Jupyter, or widgets as package
dependencies. Most notebook environments already include them. If Rich warns
that Jupyter support needs `ipywidgets`, install it in the kernel environment:

```bash
python -m pip install ipywidgets
```

## Supported actions

| Action | Work shown |
| --- | --- |
| `Session.delta_lake(..., name=...)` | Delta metadata loading and source registration |
| `PendingDeltaSource.alias(...)` | The same deferred source registration lifecycle |
| `Table.preview(...)` | Bounded query execution and preview formatting |
| `Table.show(...)` | The same preview work before text is printed |
| `Table.write_to_mssql(...)` | Planning and SQL Server execution, or planning only for a dry run |
| `Session.write_all(...)` | One display across planning, shared cache work, and all outputs |

Creating an unnamed pending Delta source remains lazy and shows no progress.
Choose the mode later on `alias(...)`:

```python
orders = session.delta_lake(
    "file:///data/orders",
    name="orders",
    progress=None,
)

pending = session.delta_lake("file:///data/customers")
customers = pending.alias("customers", progress=True)
```

Preview and show use the same progress behavior:

```python
preview = daily_orders.preview(limit=20, progress=None)
daily_orders.show(limit=20, progress=False)
```

Enable or disable progress independently for a single write:

```python
report = daily_orders.write_to_mssql(
    schema="dbo",
    table="daily_orders",
    load_mode="create_and_load",
    progress=True,
)

dry_run_report = daily_orders.write_to_mssql(
    schema="dbo",
    table="daily_orders",
    load_mode="create_and_load",
    dry_run=True,
    progress=False,
)
```

For multiple outputs, pass `progress` to the top-level call:

```python
outputs = [
    west.to_mssql(
        schema="dbo",
        table="orders_west",
        load_mode="append_existing",
        name="west",
    ),
    east.to_mssql(
        schema="dbo",
        table="orders_east",
        load_mode="append_existing",
        name="east",
    ),
]

report = session.write_all(outputs, progress=True)
```

## Read the display

An action starts with its current phase and an indeterminate bar. For example,
it may show metadata loading, query planning, cache materialization, connecting,
writing, validation, or cleanup. An indeterminate bar means Delta Funnel does
not have a truthful total for the active work.

### Delta file progress

After planning, an eligible Delta scan can switch the same display to a file
percentage:

```text
80%  Delta files 8/10 | pruned 3 at runtime, ~90 in planning
```

The denominator is the number of Delta files selected for the active physical
plan. A selected file becomes handled when the scan reads it or runtime pruning
skips it. The percentage does not measure bytes, time, output rows, or the
whole action. Files can have very different sizes, and more phases may remain
after the scan reaches 100%.

`pruned 3 at runtime` is the exact number of selected files skipped while the
query executed. `~90 in planning` is an approximate number excluded earlier by
Delta metadata selection. Planning-pruned files are outside the selected total.

The display stays indeterminate when the active plan has no reliable Delta
file total. This includes non-Delta plans and plans with zero selected files.
A bounded preview may finish successfully before every selected file is
handled, because its limit can stop the query early. A failure also keeps the
last real position instead of filling the bar.

### Rows and batches

During an execute write, the description adds cumulative rows and batches after
SQL Server accepts each batch. Large row counts use two decimal places with a
`K` or `M` suffix, such as `12.50K rows` or `3.25M rows`.

Dry runs do not execute a query or write batches, so they show planning phases
without file, row, or batch counters.

### Multiple outputs and shared work

One `write_all` call owns one consolidated display. It labels active outputs as
`Output 1/2`, `Output 2/2`, and so on. Cache work and each output have separate
physical-plan scopes, so the file total and write counters reset when the
active scope changes. A new scope may return to an indeterminate bar.

Progress reads statistics from work that the workflow already performs. It
does not execute the query twice, run a count query, repeat shared cache work,
or make extra source requests.

## Understand completion and interruption

`Completed` means the top-level action succeeded. For `write_all`, `Completed
with failures` means orchestration completed and its report contains failed or
skipped outputs. A top-level planning, cache, restoration, or orchestration
error raises `DeltaFunnelError` and ends as `Failed`.

If Python interrupts Rich while an action is running, Delta Funnel finishes
required action cleanup before re-raising the same interruption. It makes a
best-effort attempt to attach:

- `deltafunnel_operation_status`, such as `completed`,
  `completed_with_failures`, `failed`, or `cancelled`.
- `deltafunnel_operation_error` when the action itself failed.
- `deltafunnel_operation_report` when `write_all` completed with structured
  output failures.

The interruption takes precedence over returning the Python result. Delta
Funnel also writes one short stderr notice with the final operation status.

## Keep progress separate from diagnostics

Progress shows stable phase names, sanitized logical output names, and numeric
counters. It does not display source locations, storage options, credentials,
connection strings, raw metadata, raw rows, or raw internal errors.

Logging and progress are independent and may be enabled together. See
[Python logging](advanced/python-logging.md) for diagnostic setup, the
[Python quickstart](python-api-walkthrough.md) for a complete workflow and
[Dry runs and reports](dry-runs-reports.md) for report semantics.
