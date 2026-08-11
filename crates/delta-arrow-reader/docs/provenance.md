# Provenance

## Staging source

- Source repository: <https://github.com/mag1cfrog/delta-funnel>
- Frozen reader source SHA:
  `e2650427ff5e2e1a7a4e5ef9eaf30969b217fec3`
- Baseline artifact: `docs/delta-arrow-reader-extraction-baseline.md`
- Staging branch base SHA:
  `89c7c87d6e2eaf27283eae581f9e2a11821fb5f3`
- Temporary branch: `refactor/delta-arrow-reader-staging`
- Temporary crate path: `crates/delta-arrow-reader`
- Identity and inventory check: `2026-08-07T02:00:13Z` UTC

The governing issues are
[#447](https://github.com/mag1cfrog/delta-funnel/issues/447),
[#459](https://github.com/mag1cfrog/delta-funnel/issues/459), and
[#460](https://github.com/mag1cfrog/delta-funnel/issues/460).

## Independent handoff

[Issue #486](https://github.com/mag1cfrog/delta-funnel/issues/486) will export
one later frozen staging SHA into the independent repository and become the
final repository and provenance authority. This scaffold does not reserve or
publish the candidate package.

## Path mapping

| Delta Funnel source path | Staged crate path | Owning issue |
| --- | --- | --- |
| `crates/delta-funnel/src/table_formats/delta/uri.rs` | `crates/delta-arrow-reader/src/uri.rs` | #462 |
| `crates/delta-funnel/src/table_formats/delta/kernel.rs` | `crates/delta-arrow-reader/src/kernel.rs` | #462 |
| `crates/delta-funnel/src/table_formats/delta/snapshot.rs` | `crates/delta-arrow-reader/src/snapshot.rs` | #462 |
| `crates/delta-funnel/src/table_formats/delta/protocol.rs` | `crates/delta-arrow-reader/src/protocol.rs` | #462 |
| Source-loading portions of `crates/delta-funnel/src/table_formats/delta.rs` | `crates/delta-arrow-reader/src/snapshot.rs` and `src/kernel.rs` | #462 |
| Snapshot/protocol/schema fixtures from `crates/delta-funnel/src/table_formats/delta/test_support.rs` | Crate-local tests in `src/uri.rs`, `src/snapshot.rs`, and `src/protocol.rs` | #462 |
| `crates/delta-funnel/src/table_formats/delta/deletion_vector.rs` | `crates/delta-arrow-reader/src/deletion_vector.rs` | #477 |
| DV metadata portions of `crates/delta-funnel/src/table_formats/delta.rs` | `crates/delta-arrow-reader/src/kernel.rs` and `src/deletion_vector.rs` | #477 |
| DV masking portions of `crates/delta-funnel/src/query_engine/datafusion/execution/file_reader.rs` | `crates/delta-arrow-reader/src/deletion_vector.rs` | #477 |
| DV coordinate portions of `crates/delta-funnel/src/query_engine/datafusion/execution/native_async_reader.rs` | `crates/delta-arrow-reader/src/deletion_vector.rs` | #477 |
| DV counter portions of `crates/delta-funnel/src/query_engine/datafusion/execution/read_stats.rs` | `crates/delta-arrow-reader/src/metrics.rs` | #477 |
| DataFusion predicate-adapter portions of `crates/delta-funnel/src/table_formats/delta/kernel.rs` | `crates/delta-arrow-reader/src/predicate.rs` and `src/kernel.rs` | #484 |
| Scan-metadata and transform portions of `crates/delta-funnel/src/table_formats/delta.rs` | `crates/delta-arrow-reader/src/planning.rs`, `src/kernel.rs`, and `src/transform.rs` | #463 |
| Scan/read-schema portions of `crates/delta-funnel/src/table_formats/delta/read.rs` | `crates/delta-arrow-reader/src/planning.rs` and `src/transform.rs` | #463 |
| Scan/schema/metadata portions of `crates/delta-funnel/src/table_formats/delta/kernel.rs` | `crates/delta-arrow-reader/src/kernel.rs` and `src/planning.rs` | #463 |
| `crates/delta-funnel/src/query_engine/datafusion/planning/file_task.rs` | `crates/delta-arrow-reader/src/planning.rs` | #463 |
| Metadata/task portions of `crates/delta-funnel/src/query_engine/datafusion/planning/scan_plan.rs` | `crates/delta-arrow-reader/src/planning.rs` | #463 |
| Scan-planning fixtures from `crates/delta-funnel/src/table_formats/delta/test_support.rs` | Crate-local tests in `src/planning.rs` | #463 |
| `crates/delta-funnel/src/query_engine/datafusion/planning/partition_target.rs` | `crates/delta-arrow-reader/src/partition_target.rs` | #478 |
| Host-diagnostic portions of `crates/delta-funnel/src/query_engine/datafusion/execution/environment.rs` | `crates/delta-arrow-reader/src/partition_target.rs` | #478 |
| `crates/delta-funnel/src/query_engine/datafusion/planning/file_task_partition.rs` | `crates/delta-arrow-reader/src/planning.rs` | #478 |
| Final partitioned-plan assembly portions of `crates/delta-funnel/src/query_engine/datafusion/planning/scan_plan.rs` | `crates/delta-arrow-reader/src/planning.rs` | #478 |
| Planning-metrics initialization portions of `crates/delta-funnel/src/query_engine/datafusion/execution/planning_exec.rs` | `crates/delta-arrow-reader/src/planning.rs` | #478 |
| `crates/delta-funnel/src/query_engine/datafusion/execution/async_scheduler.rs` | `crates/delta-arrow-reader/src/scheduling.rs` | #481 |
| Limiter portions of `crates/delta-funnel/src/query_engine/datafusion/execution/scheduling.rs` | `crates/delta-arrow-reader/src/config.rs` and `src/scheduling.rs` | #481 |
| Permit/environment portions of `crates/delta-funnel/src/query_engine/datafusion/execution/environment.rs` | `crates/delta-arrow-reader/src/scheduling.rs` | #481 |
| Handoff/cancellation portions of `crates/delta-funnel/src/query_engine/datafusion/execution/file_reader.rs` and `planning_exec.rs` | `crates/delta-arrow-reader/src/scheduling.rs` and `src/planning.rs` | #481 |
| Execution-counter portions of `crates/delta-funnel/src/query_engine/datafusion/execution/read_stats.rs` | `crates/delta-arrow-reader/src/metrics.rs` and `src/scheduling.rs` | #481 |
| `crates/delta-funnel/src/query_engine/datafusion/execution/native_async_reader.rs` | `crates/delta-arrow-reader/src/native_async_reader.rs` | #464 |
| `crates/delta-funnel/src/query_engine/datafusion/execution/native_async_row_group_pruning.rs` | `crates/delta-arrow-reader/src/native_async_row_group_pruning.rs` | #464 |
| `crates/delta-funnel/src/query_engine/datafusion/execution/metered_object_store.rs` | `crates/delta-arrow-reader/src/metered_object_store.rs` | #464 |
| NativeAsync file-producer and default-backend portions of `crates/delta-funnel/src/query_engine/datafusion/execution/file_reader.rs` and `reader_backend.rs` | `crates/delta-arrow-reader/src/native_async_reader.rs` and `src/config.rs` | #464 |
| NativeAsync Parquet I/O counter portions of `crates/delta-funnel/src/query_engine/datafusion/execution/read_stats.rs` | `crates/delta-arrow-reader/src/metered_object_store.rs`, `src/metrics.rs`, and `src/native_async_reader.rs` | #464 |
| OfficialKernel data-file portions of `crates/delta-funnel/src/table_formats/delta/read.rs` | `crates/delta-arrow-reader/src/official_kernel_reader.rs` and `src/kernel.rs` | #465 |
| OfficialKernel file-correctness portions of `crates/delta-funnel/src/query_engine/datafusion/execution/file_reader.rs` and `reader_backend.rs` | `crates/delta-arrow-reader/src/official_kernel_reader.rs`, `src/deletion_vector.rs`, and `src/kernel.rs` | #465 |
| OfficialKernel blocking-producer portions of `crates/delta-funnel/src/query_engine/datafusion/execution/scheduling.rs` and `planning_exec.rs` | `crates/delta-arrow-reader/src/official_kernel_reader.rs` over `src/scheduling.rs` | #465 |
| OfficialKernel metric-availability portions of `crates/delta-funnel/src/query_engine/datafusion/execution/read_stats.rs` | `crates/delta-arrow-reader/src/metrics.rs` | #465 |
| Direct table-loading, scan-building, and Arrow stream composition over the extracted services | `crates/delta-arrow-reader/src/direct.rs` | #466 |
| `crates/delta-funnel/src/query_engine/datafusion/planning/projection.rs` | `crates/delta-arrow-reader/src/datafusion_planning.rs` | #467 |
| `crates/delta-funnel/src/query_engine/datafusion/planning/filters.rs` | `crates/delta-arrow-reader/src/datafusion_planning.rs` | #467 |
| `crates/delta-funnel/src/query_engine/datafusion/planning/filters/analysis.rs` | `crates/delta-arrow-reader/src/datafusion_planning.rs` | #467 |
| `crates/delta-funnel/src/query_engine/datafusion/planning/filters/partition_pushdown.rs` and `stats_pushdown.rs` | `crates/delta-arrow-reader/src/datafusion_planning.rs` over the #484 predicate and Kernel-pruning boundary | #467 |
| Static filter-normalization and scan-planning portions of `crates/delta-funnel/src/query_engine/datafusion/catalog/provider.rs` | `crates/delta-arrow-reader/src/datafusion_planning.rs` | #467 |
| `crates/delta-funnel/src/query_engine/datafusion/planning/dynamic_filters.rs` | `crates/delta-arrow-reader/src/datafusion_dynamic_filters.rs` | #480 |
| `crates/delta-funnel/src/query_engine/datafusion/planning/dynamic_partition_pruning.rs` | `crates/delta-arrow-reader/src/datafusion_dynamic_partition_pruning.rs` | #480 |
| Dynamic-filter execution portions of `crates/delta-funnel/src/query_engine/datafusion/execution/planning_exec.rs` | `crates/delta-arrow-reader/src/datafusion_execution.rs` | #480 |
| Provider and physical-plan portions of `crates/delta-funnel/src/query_engine/datafusion/catalog/provider.rs` and `execution/planning_exec.rs` | `crates/delta-arrow-reader/src/datafusion_provider.rs` and `src/datafusion_execution.rs` | #468 |
| Single-table registration portions of `crates/delta-funnel/src/query_engine/datafusion/catalog/registration.rs` | `crates/delta-arrow-reader/src/datafusion_provider.rs` | #468 |
| Frozen public provider and execution fixtures | `crates/delta-arrow-reader/tests/support/real_parquet_delta_table.rs` | #469 |
| Applicable public assertions from `catalog/provider_tests.rs`, `catalog/registration.rs`, and `execution/planning_exec.rs` | `crates/delta-arrow-reader/tests/datafusion_provider.rs` | #483 |
| Controlled provider-exec cases, fixture generation, delayed storage, measurement, and schema-22 CSV portions of `crates/delta-funnel/src/bin/delta_scan_partition_bench.rs` | `crates/delta-arrow-reader/benches/reader.rs`, `docs/benchmark-parity.md`, and `docs/benchmark-parity-results.csv` | #470 |

## Test migration

- #462 ports URI normalization, latest/fixed snapshot loading, protocol and
  schema conversion, storage construction/context reuse, redaction, and
  unsupported-protocol scan-boundary assertions into the staged crate.
- #477 ports the focused deletion-vector metadata, payload, coordinate,
  masking, redaction, and metric assertions into the staged crate.
- #463 ports scan-metadata, ordered file-task, schema, and transform
  preservation. #478 ports target selection, host diagnostics, deterministic
  grouping, final partitioned-plan assembly, and planning-metrics
  initialization over those tasks.
- #481 ports limiter, lazy admission, ordered prefetch, bounded handoff,
  first-error, cancellation, cleanup, and execution-counter assertions into
  `src/scheduling.rs` and `src/planning.rs`.
- #464 ports the NativeAsync async Parquet reader, object-store metering,
  metadata prefetch, per-file buffering, physical projection and schema
  matching, conservative row-group pruning, original row indexes, DV and
  transform pipeline, cancellation, resource lifetime, errors, and focused
  scheduler integration assertions. Full public direct certification remains
  with #482, and DataFusion adaptation and certification remain with #483.
- #465 ports OfficialKernel data-file reads, the bounded blocking handoff,
  projection, predicates, transforms, ordered DV masking, capability fallback,
  cancellation-safe cleanup, errors, metrics availability, and focused parity
  assertions against NativeAsync. Full public direct certification remains with
  #482, and DataFusion adaptation and certification remain with #483.
- #484 adapts the focused comparison, scalar, Boolean, null, and Kernel
  conversion assertions into the staged crate, then adds schema-validation,
  three-valued residual, pruning-parity, concurrency, and redaction coverage.
- Empty binary values remain accepted, while negative-scale decimal and
  signed-zero float predicates fall back to residual-only when Kernel cannot
  preserve their logical meaning. These #484 contract differences are covered
  locally rather than retaining the old adapter outcomes.
- #467 ports and adapts ordered, empty, rejected-duplicate, invalid, hostile,
  accepted-predicate-column, and residual-column projection coverage. Its
  table-driven parity tests preserve the existing partition and data-statistics
  type/operator matrices, reversed comparisons, qualifier normalization,
  rejection policy, exactness capability, mixed-AND pruning, and three-valued
  partition `IN`/`BETWEEN` behavior. Equivalent Utf8/LargeUtf8,
  Binary/LargeBinary, and timestamp representations are normalized to the
  Arrow field type because #484 predicates require exact scalar types.
  DataFusion optimizer statistics stay unknown, and its advisory scan limit
  remains outside core planning.
- #466 adds external signature and deterministic end-to-end tests for the
  direct load, projection, predicate, limit, partition merge, backend parity,
  error, drop, redaction, and retained-metrics contracts.
- #482 carries the remaining frozen direct/backend contract through the public
  API. It preserves the 24 frozen NativeAsync/OfficialKernel equivalence cases
  against static expected rows, plus four additional backend comparisons among
  the nine deletion-vector predicate/boundary/failure cases. It also preserves
  four missing-required-field failures and seven ordering/error/resource cases
  from `planning_exec.rs`. The #469 portability test remains constructor-only;
  #482 executes only fixtures used by those frozen cases. Focused internal
  mechanics remain with their earlier #464, #465, #477, and #481 ports, while
  SQL plans, DataFusion error wrapping, provider statistics, and optimizer
  assertions remain with #483. Delta Funnel workflow and reporting consumers
  remain in Delta Funnel.
- #483 inventories all 626 tests in the frozen DataFusion tree, including the
  193 provider, 11 registration, and 102 physical-execution tests. Its external
  provider suite preserves the applicable public SQL, projection, static and
  dynamic filtering, residual, registration, option, metric, error,
  cancellation, backend, and repeated-execution assertions. The exact private
  projection and static-filter matrices remain mechanically represented by
  #467, dynamic classification and snapshot evaluation by #480, scheduler and
  reader mechanics by #481 and #482, and snapshot/schema behavior by #462.
  Atomic multi-source registration and rollback, custom catalog/schema routing,
  profiling, operator activity, session workflow, and reporting remain Delta
  Funnel-owned because the standalone public boundary has no corresponding
  orchestration API.
- #470 ports the 12 controlled provider-exec reader cases, four deterministic
  fixture recipes and fingerprints, delayed storage model, output validation,
  schema-22 measurements, and two-warm-up/five-measurement comparison method.
  The extracted public API matches every deterministic field and has no
  material timing regression. Product workflow and report benchmarks remain in
  Delta Funnel.
