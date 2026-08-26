# Delta Provider Read Scheduling

Delta Arrow Reader owns file-read concurrency, prefetching, Parquet reads,
dynamic partition pruning, backpressure, cancellation, schema transforms, and
deletion-vector masking.

For the execution flow, see
[Read scheduling](https://mag1cfrog.github.io/delta-arrow-reader/read-scheduling/).
For exact reader defaults and metric definitions, see
[Execution options](https://mag1cfrog.github.io/delta-arrow-reader/reference/execution-options/)
and [Scan metrics](https://mag1cfrog.github.io/delta-arrow-reader/reference/metrics/).

## Configure the reader through Delta Funnel

Python users pass a typed `ProviderScanOptions` object to `Session`. Rust users
set the corresponding options through `SessionOptions`. Delta Funnel validates
these values before it registers a table with the standalone reader.

Delta Funnel changes one standalone default: it uses ordinary Arrow `Utf8` and
`Binary` arrays because they perform better in its transformation-heavy
workflows. Set `use_view_types=True` for a measured scan-heavy workload that
benefits from Arrow view arrays.

See the [API reference](../reference/api.md#session-options) for the options and
their Delta Funnel defaults.

## Observe reads through Delta Funnel

Delta Funnel carries the reader's metrics into source reports, execution
profiles, and one terminal Parquet I/O tracing event. These surfaces preserve
the meaning of the underlying scan counters.

See the [diagnostics reference](../reference/diagnostics.md) for what appears in
reports and tracing events. Use the standalone reader's
[metrics reference](https://mag1cfrog.github.io/delta-arrow-reader/reference/metrics/)
for the meaning and measurement boundary of each counter.
