# Delta Arrow Reader migration plan

This is the reviewed handoff from issue #474 to the atomic cutover in #475 and
the mechanical cleanup in #476. The frozen behavior and ownership reference is
[`delta-arrow-reader-extraction-baseline.md`](delta-arrow-reader-extraction-baseline.md).
This preparation change does not route production through the published crate.

## Published dependency and type compatibility

Delta Funnel depends on exact crates.io version `delta-arrow-reader = "=0.1.1"`
with its default `native-async` feature plus `datafusion` and `official-kernel`.
`Cargo.lock` records crates.io checksum
`6e75b1cda3e8a3450bd6653175abb83f272b0cdba5abda1fff62146f4d96385e`.
There is no path, Git, patch, or staging source.

The resolved graph has one copy of each compatibility-critical package:

| Boundary | Resolved package | Compatibility proof |
| --- | --- | --- |
| Arrow schemas and batches | Arrow/Parquet 58.3.0 | `DeltaTable::schema` and `DeltaBatchStream` compile directly as Delta Funnel's `SchemaRef` and `RecordBatch`. |
| DataFusion provider and plans | DataFusion 54.1.0 | Delta Funnel's `SessionContext` is accepted by `register_delta_table`; its plans are accepted by the standalone metrics collector. |
| Object storage | object_store 0.13.2 | One resolved package; its concrete types stay inside the reader boundary. |
| Delta metadata and engines | delta_kernel/default-engine 0.25.0 | One resolved package; concrete engine state stays inside `DeltaTable`. |
| Async execution | Tokio 1.52.3 and futures 0.3.32 | `load_async`, scan execution, and `TryStreamExt` use the caller's existing runtime and stream types. |

`tests/delta_reader_features.rs` supplies the focused shadow proof. It loads the
existing type-widening fixture through the published direct API, consumes its
Arrow stream, registers the same table in the existing DataFusion context, and
executes a query. The existing test in that file remains on the old production
route and continues to cover both reader backends.

## Current call-site map for #475

The mapping below reconciles the #459 public inventory with the current tree.
Rows marked "retain" are Delta Funnel product responsibilities, not duplicate
reader implementation.

| Current call site or responsibility | #475 treatment |
| --- | --- |
| `DeltaSourceConfig`, `load_delta_source*`, and `load_delta_sources` callers in the session, runtime, benchmark, docs, and tests | Retain `DeltaSourceConfig` as the product-owned source name and pre-load environment/authentication policy boundary. Move it out of the reader module, use the standalone `DeltaStorageOptions`, then construct `DeltaTableBuilder` with URI, storage options, snapshot selection, and execution options and call `load`. Repeat the load inside the retained multi-source loop. Preserve the existing source-loading telemetry and authentication-mode fields around the standalone call. |
| `PlannedDeltaSource` consumers | Replace reader state with `DeltaTable`. Keep the validated source name and sanitized/reporting context in Delta Funnel-owned session state. Use `DeltaTable::{table_uri,version,schema,protocol}` for canonical reader facts. |
| `preflight_delta_protocol*` and `preflight_delta_sources` | Use `DeltaTable::validate_protocol` and `DeltaTable::protocol`. Repeat validation inside retained multi-source orchestration. Build `DeltaProtocolReport` only at the Delta Funnel report boundary, and preserve the existing protocol-preflight telemetry around validation. |
| `delta_source_arrow_schema` | Use `DeltaTable::schema` directly; the shadow test proves the `SchemaRef` identity. |
| `DeltaProviderReaderBackend` | Replace with `DeltaReaderBackend`. |
| `DeltaProviderScanExecutionOptions` | Replace with `DeltaReaderExecutionOptions`; adapt the existing Rust/Python fields through its public builders and preserve the existing defaults and validation order. |
| `DeltaTableProviderConfig` and internal `DeltaTableProvider` construction | Use `DeltaDataFusionScanOptions` plus `DeltaTable`; use `DeltaTableProvider::try_new` only where direct provider ownership is required. |
| Single-source catalog registration | Use `register_delta_table` and its `RegisteredDeltaTable` result internally. Retain `RegisteredDeltaSource` as the product result containing sanitized URI, schema, and `DeltaProtocolReport`; populate it from the retained source context and a cheap `DeltaTable` clone. |
| `register_delta_sources*` and session registry registration | Retain the multi-source name checks, existing-catalog checks, progress and registration telemetry, all-or-nothing loop, rollback, report construction, and session bookkeeping. Each item calls `register_delta_table` exactly once. |
| Provider projection, filter, partition-pruning, scan planning, scheduling, both file backends, transformations, deletion vectors, and cancellation | Remove the internal call path and let `DeltaTableProvider` own it. Delta Funnel supplies only canonical scan options. |
| `DeltaProviderReadStatsSnapshot` and `collect_delta_provider_read_stats` | Use `collect_delta_datafusion_metrics`, `DeltaDataFusionMetricsSnapshot`, and nested `DeltaReadMetricsSnapshot`. Flatten fields only while serializing the existing Delta Funnel reports and progress events. |
| Partition-target diagnostic types and functions | Import the identically named standalone diagnostic types and functions directly. |
| Reader failures currently converted to `DeltaFunnelError` | Map `DeltaReaderError::{as_str,phase}` at the existing product boundary. Retain only validated source name, sanitized URI, report reason, and progress context. |
| `datafusion_session_config`, `datafusion_session_context`, and `datafusion_query_output_stream` | Retain. They own application session policy and output handoff. |
| `QueryOptions`, `DeltaSourceReport`, `DeltaProtocolReport`, `DeltaProviderSchedulingReport`, `RegisteredDeltaSources`, reports, progress, profiling, workflows, and Python adapters | Retain. They own product policy, orchestration, serialization, observability, or a language boundary. Consume standalone types instead of reader duplicates. |

Private reader calls under `table_formats/delta*`,
`query_engine/datafusion/catalog/provider.rs`,
`query_engine/datafusion/execution*`, and
`query_engine/datafusion/planning*` have no separate integration contract. They
are reached only through the old load/provider route, so replacing the load and
provider boundaries above replaces all of them without a facade or fallback.

## Atomic #475 cutover

1. Move `DeltaSourceConfig` to the Delta Funnel source-policy boundary without
   changing its public fields or builders. Change loaded session source state
   to hold the standalone `DeltaTable` plus Delta Funnel-owned source identity
   and reporting context.
2. Adapt source loading and protocol reporting to `DeltaTableBuilder` and
   `DeltaTable` while preserving current name validation, environment option
   precedence, authentication-mode derivation, error redaction, progress, and
   source/protocol telemetry events.
3. Adapt execution options once, then register each source through
   `register_delta_table` while preserving prevalidation, registration
   telemetry, product registration results, and rollback.
4. Replace provider metrics collection and report flattening with standalone
   metrics.
5. Replace diagnostic imports and remove old reader-owned public exports rather
   than aliasing them.
6. Map standalone errors only at existing Delta Funnel product boundaries.
7. Remove the old reader modules from production reachability in the same PR,
   but leave physical deletion to #476.
8. Run all existing affected reader, provider, report, progress, workflow,
   benchmark, Rust API, and Python tests. Port any newly discovered
   reader-only assertion to the standalone repository before deleting it.

Production must have one route after the cutover: Delta Funnel orchestration to
the published reader. It must not dual-read, fall back, or expose compatibility
aliases.

## Mechanical #476 cleanup

Delete these standalone-owned implementation files once #475 makes them
unreachable:

- `crates/delta-funnel/src/table_formats/delta.rs`
- `crates/delta-funnel/src/table_formats/delta/`
- `crates/delta-funnel/src/query_engine/datafusion/catalog/provider.rs`
- `crates/delta-funnel/src/query_engine/datafusion/execution.rs`
- `crates/delta-funnel/src/query_engine/datafusion/execution/`
- `crates/delta-funnel/src/query_engine/datafusion/planning.rs`
- `crates/delta-funnel/src/query_engine/datafusion/planning/`

Retain and simplify `table_formats.rs` for source-name/environment policy,
`catalog/registration.rs`, `query_engine/datafusion.rs`, the session, profiling,
report, progress, workflow, and Python modules. Split or delete reader-only
cases in `catalog/provider_tests.rs`; retain its product
registration, rollback, report, session, and query behavior cases. Delete
`delta_scan_partition_bench` and reader-only fixtures only after their retained
product coverage or standalone owner is confirmed.

After source deletion, inspect usage and remove the direct dependencies that
served only the duplicate reader: `delta_kernel_default_engine`, `object_store`,
and the Windows system dependency used by the old partition probe. Remove
`delta_kernel` and `parquet` too if deleting the reader benchmark leaves no
retained caller. `async-trait`, `futures-util`, Tokio, and `libc` have independent
Delta Funnel callers and are not cleanup candidates. Confirm the final list
with `cargo machete`, `cargo tree`, and source search instead of changing any
unrelated version.
