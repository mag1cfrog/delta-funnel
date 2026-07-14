# API References

## Rust

The Rust crate owns the workflow implementation and public report types. The
published Rust API reference lives on
[docs.rs/delta-funnel](https://docs.rs/delta-funnel).

For local API docs, run:

```bash
cargo doc -p delta-funnel --open
```

## Python

The Python package name and import name are `deltafunnel`.

The current typed public surface is recorded in the package stub:

- [`deltafunnel.pyi`](https://github.com/mag1cfrog/delta-funnel/blob/main/crates/delta-funnel-python/deltafunnel.pyi)

Core Python entry points:

- `init_logging`
- `Session`
- `PendingDeltaSource`
- `Table`
- `Preview`
- `MssqlOutputSpec`
- `DeltaFunnelError`

For `init_logging` setup and filter behavior, see
[Python logging](../advanced/python-logging.md).

For progress modes and display behavior shared by the supported actions, see
[Progress displays](../progress.md).

`Session.delta_lake(source_uri, *, version=None, storage_options=None,
name=None, progress=None)` registers a named Delta source immediately when
`name` is present. Without `name`, it returns a lazy `PendingDeltaSource` and
does not load or register the source.

`PendingDeltaSource.alias(name, *, progress=None)` performs the deferred
registration. Progress is selected by the call that performs registration. A
value passed while creating an unnamed pending source is not reused by
`alias(...)`.

`Table.preview(limit=20, *, progress=None)` returns a `Preview` object.
`Table.show(limit=20, *, progress=None)` executes the same preview and prints
the text form to Python stdout. Both execute the DataFusion query with the limit
applied before collection, read rows, and do not contact or write to SQL Server.
`Preview.text` is the plain text table and `Preview.html` backs notebook
`_repr_html_()` display.

For Delta sources, `Session.delta_lake(..., storage_options=...)` accepts a
mapping of string keys and values and forwards them to the underlying
object-store builder used by Delta Funnel. For private S3 tables, see the
[Private S3 sources](../advanced/private-s3.md) guide for the exact
documented AWS keys, examples, and troubleshooting guidance.

## Execution Profile Model

The Rust crate exports one immutable execution-profile model for bounded
previews, one-output SQL Server writes, and selected `write_all` cache aliases.
This foundation also supplies the reusable terminal consumer and bounded
tracing summary. It does not by itself expose a profile option or attach the
immutable result to an operation report. Individual operation APIs own that
integration; the model itself does not change query execution.

`ExecutionProfileMode` defaults to `Disabled`. Its other value is `Detailed`.
The stable JSON spellings used by the remaining enums are:

| Enum | JSON values |
| --- | --- |
| `QueryExecutionScope` | `preview`, `mssql_output`, `write_all_cache_alias` |
| `QueryExecutionOutcome` | `success`, `error`, `cancelled` |
| `QueryExecutionMetricCategory` | `summary`, `dev` |

The public Rust model uses typed values and read-only accessors. JSON is an
explicit projection of that model, not its in-memory source of truth.

### Profile Schema

```json
{
  "scope": "preview",
  "outcome": "success",
  "partial": false,
  "delta_funnel_row_limit": 20,
  "operators": [
    {
      "node_id": 0,
      "parent_node_id": null,
      "operator_name": "GlobalLimitExec",
      "output_partition_count": 1,
      "metrics_available": true,
      "aggregated_metrics": [],
      "metrics": [],
      "delta_provider_read_stats": null
    }
  ]
}
```

`partial` is derived from `outcome`: it is `false` only for `success`.
`delta_funnel_row_limit` is the exact Delta Funnel preview limit, converted to
an unsigned 64-bit value with saturation. It is `null` for both write scopes.
A successful limited preview means the limited execution completed normally;
it does not describe an unbounded write.

Operators are the unique physical-plan nodes in deterministic first-seen
pre-order. IDs start at zero and are local to one profile. The root parent is
`null`. Repeated references to the exact same `Arc<dyn ExecutionPlan>` keep the
first node and first parent, while distinct nodes remain separate even when
their names and metadata match. `operator_name` is only DataFusion's short
`ExecutionPlan::name()` value. Plan display text is never collected.

`metrics_available=false` means DataFusion returned no metric set. An available
but empty set uses `metrics_available=true` with two empty metric arrays. Every
operator, including a zero-output-partition root, remains in the profile.

### Raw And Aggregated Metrics

Each operator has two views of the same terminal DataFusion metric set:

- `metrics` preserves original per-partition entries.
- `aggregated_metrics` uses DataFusion's `aggregate_by_name()` result and
  therefore sets `partition` and `output_partition` to `null`.

Both arrays are sorted by category, name, partition, output partition, value
kind, and typed value. Original metric position is used only to order otherwise
identical redacted entries. Collection reads each node's metric set once and
does not execute or poll the plan.

DataFusion operator metrics are cumulative counters and gauges. Partitions and
operators can execute concurrently, and parent and child work can overlap. Do
not sum operator compute durations and call the result query wall time. The
profile does not derive wall-time percentages.

Each metric has this envelope:

```json
{
  "name": "output_rows",
  "category": "summary",
  "partition": 0,
  "output_partition": null,
  "value_kind": "count",
  "value": 42,
  "components": null
}
```

DataFusion 53.1 values map as follows:

| DataFusion value | Profile value |
| --- | --- |
| Output rows, output batches, spill count, spilled rows, generic count | `count` with an unsigned scalar |
| Output bytes, spilled bytes, current memory usage | `bytes` with an unsigned scalar |
| Elapsed compute, generic time | `nanoseconds` with an unsigned scalar |
| Generic gauge | `gauge` with an unsigned scalar |
| Start or end timestamp | `timestamp_nanoseconds` with a signed Unix epoch scalar or `null` |
| Pruning metrics | `pruning` with `pruned`, `matched`, and `fully_matched` components |
| Ratio | `ratio` with unsigned `part` and `total` components |
| Custom | `custom` with its unsigned `as_usize()` value |

All non-timestamp `usize` conversions saturate to `u64`. A numeric zero is an
available measured value. Unavailable metrics use the relevant absence signal,
such as `metrics_available=false`, a `null` optional provider field, or a
`null` unset or out-of-range timestamp. Current memory is a terminal gauge, not
a promised peak. Ratios preserve their two integer components and are not
converted to floating-point percentages. Custom display text is not exposed.

### Labels And Redaction

The collector recognizes only an exact `outputPartition` label whose value is
a base-10 non-negative integer that fits in `u64`. It normalizes that value to
`output_partition`. Malformed values and every other label are dropped,
including `filename`, `expr`, and unknown future labels.

Profiles never include plan display text, expressions, SQL, schemas, literals,
URLs, paths, storage options, headers, credentials, or custom metric display
text. These redaction rules apply to both JSON and Rust `Debug` output.

### Delta Provider Snapshots

A `DeltaScanPlanningExec` operator can contain the existing
`DeltaProviderReadStatsSnapshot` under `delta_provider_read_stats`. The profile
reuses the established provider JSON mapping wholesale, so provider fields keep
their existing names and availability semantics.

Terminal consumers associate snapshots by exact read-stats `Arc` identity, not
source name or snapshot contents. They reuse the immutable snapshot captured at
the shared terminal transition and do not take a later live snapshot. A scan
missing from a supplied terminal set gets `null` provider stats and an internal
redacted diagnostic. A supplied snapshot with no matching scan is ignored.
The standalone internal collector can instead snapshot each unique handle once
when no terminal set is supplied.

This model is application-level query profiling. It does not collect syscall,
CPU stack, scheduler, Tokio, network-packet, `perf`, eBPF, or kernel profiles,
and it does not run `EXPLAIN ANALYZE` or execute the query a second time.
