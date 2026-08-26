# Delta Scan Planning

Delta Arrow Reader owns Delta scan planning. It chooses a partition target,
selects files, groups whole-file tasks, and, when useful, lets DataFusion split
large files into ranged tasks.

For the planning algorithm and its resource caps, see
[Scan planning](https://mag1cfrog.github.io/delta-arrow-reader/scan-planning/)
in the Delta Arrow Reader documentation.

## What Delta Funnel configures

Delta Funnel supplies two planning choices:

- `Session.target_partitions` sets DataFusion's session-wide execution target.
  Delta Arrow Reader uses that value as an upper limit when it chooses an
  automatic scan target.
- `ProviderScanOptions.intra_file_repartitioning` controls whether DataFusion
  may split files only to fill missing parallelism or may also rebalance a plan
  that already has enough partitions.

Delta Funnel does not set a separate reader-specific partition target. See the
[Python API reference](../reference/api.md#session-options) for the public
options.

## What Delta Funnel records

Source reports record the DataFusion target and the selected reader backend.
Executed scans can also include the reader's planning and execution metrics.
See the [diagnostics reference](../reference/diagnostics.md) for Delta Funnel's
reporting and tracing boundaries.

The repository's
[Delta scan benchmark](../contributing/scan-benchmarks.md) remains the place to
measure these choices through a complete Delta Funnel workflow.
