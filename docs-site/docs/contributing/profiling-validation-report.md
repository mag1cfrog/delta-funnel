# Profiling Validation Report

This report records the Linux profiling evidence used to close the unified
Perfetto diagnostics program. It compares the supported profiling modes on one
canonical generated workload and documents the production correctness matrix.
It is evidence from one development machine, not a universal performance or
file-size guarantee. Measurements were collected from 2026-07-16 through
2026-07-19.

For the user workflow, mode selection, capture lifecycle, and health-field
interpretation, use [Profile Delta Funnel Workloads](profiling.md). This report
does not replace that how-to.

## Decision

The production Perfetto path is suitable for opt-in, local diagnostics on
Linux:

- Short standard mode combines exact Delta Funnel semantic spans and sampled
  native stacks in one stock Perfetto trace.
- Streaming standard mode retained a complete 10-minute capture well below its
  configured 512 MiB file cap on the canonical workload.
- Deep-system mode adds useful scheduler evidence, but its additional volume
  and host-wide visibility make it appropriate only for short investigations.
- Stable semantic JSON and Samply remain useful, supported alternatives. They
  solve different problems and are not replaced by Perfetto.
- Default builds and published Python wheels remain Perfetto-free.

The measurements do not justify a claim of zero overhead. Short standard
Perfetto was about 8.6 percent slower at workflow p50 than its adjacent
feature-off control in the final short run. The 10-minute streaming run was
about 0.8 percent slower at operation p50 than the historical adjacent control,
but those runs were not randomized or interleaved. Treat both values as
directional.

## Scope and evidence lineage

The performance workload contains 13,394,789 generated rows in 1,204 local
Delta data files. It exercises the production DataFusion provider and
`write_all` stream path without opening SQL Server or writing target rows. All
runs used seed 0, the `native_async` backend, and the
`prefetch_2_parallel_buffer_1` scheduling profile.

Evidence came from these completed slices:

- #511 and #512 established the feature-off, stable semantic JSON, and Samply
  baselines.
- #522 measured short standard and short deep-system Perfetto at the final
  prototype architecture. #524 through #526 moved that architecture into the
  production core and Python paths without changing the canonical semantic
  identity model.
- #527 measured the production bounded streaming path for 10 minutes.
- #528 revalidated short production captures across preview, SQL Server
  `write`, and SQL Server `write_all`, including failure, concurrency,
  interruption, truncation, and reduced-buffer cases.

The short Perfetto performance rows were not rerun after the adapter moved from
the prototype binary into its production module. The production parity tests
verify behavior, hierarchy, and capture health, but not a narrow performance
equivalence bound. The rows remain useful for mode selection because the
emitted semantic model, 100 Hz sampler, and buffer design are the same.

## Test environment

The results were collected on the following shared development host:

| Component | Tested value |
| --- | --- |
| Operating system | Fedora Linux 43, Linux 6.19.14-200.fc43.x86_64 |
| CPU | AMD Ryzen 7 8845HS, 8 cores, 16 hardware threads |
| Available parallelism | 16 |
| Rust | rustc 1.97.0, Cargo 1.97.0 |
| Python | CPython 3.14.6 |
| Maturin | 1.11.5 |
| Samply | 0.13.1 |
| Perfetto | v57.2-da1d152cf |
| Trace Processor API | 14 |
| Perfetto UI | Stock v57.2 in Chrome for Testing 148 |
| Linux perf policy | `perf_event_paranoid=-1`, `kptr_restrict=0` |

The `profiling` Cargo profile was used for native sampling. It keeps release
optimizations and line-table debug information. The exact unstripped binary and
Cargo sources remained local for symbolization. Optimized and inlined Rust
frames can still be collapsed or attributed to a nearby source line.

## Runtime comparison

Each short-workload row contains three workload repetitions. The streaming row
contains 26 repetitions in one continuous capture. Peak RSS is the benchmark
process increase reported by the runner. It excludes the external Samply or
Perfetto service process.

| Mode | Repetitions | Workflow p50 | Workflow p95 | Observed range | Command wall | Peak RSS increase |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Feature-off control before | 3 | 22.362 s | 23.024 s | Not retained | 88.88 s | 14,800.9 MiB |
| Stable semantic JSON | 3 | 30.581 s | 31.497 s | Not retained | 127.05 s | 16,701.2 MiB |
| Samply at 1000 Hz | 3 | 22.021 s | 22.209 s | Not retained | 89.18 s | 14,744.7 MiB |
| Feature-off control after | 3 | 21.377 s | 22.058 s | Not retained | 86.23 s | 14,595.3 MiB |
| Short standard Perfetto | 3 | 24.014 s | 28.414 s | 23.041 to 28.414 s | 97.46 s | 14,553.3 MiB |
| Short deep-system Perfetto, run 1 | 3 | 23.402 s | 24.544 s | 22.914 to 24.544 s | 93.68 s | 14,644.1 MiB |
| Short deep-system Perfetto, run 2 | 3 | 24.428 s | 24.853 s | 22.474 to 24.853 s | 93.99 s | 14,984.8 MiB |
| Streaming standard Perfetto | 26 | 22.288 s | 22.930 s | 21.475 to 23.382 s | 601.50 s | 14,555.8 MiB |

The short standard and deep-system rows used an adjacent feature-off p50 of
22.107 s and command wall time of 89.03 s from the #522 measurement window.
Against that control:

- Short standard workflow p50 was 8.6 percent higher and command wall time was
  9.5 percent higher. One 28.414-second repetition dominates its p95.
- Deep-system workflow p50 was 5.9 to 10.5 percent higher across two runs.
  Command wall time was 5.2 to 5.6 percent higher.
- Streaming operation p50 was 0.8 percent higher. This compares repetitions
  inside one long capture with a historical adjacent short control, so it is
  not a strict long-run overhead estimate.

The bracketed feature-off controls around Samply differed by 4.6 percent. The
Samply p50 was 1.5 percent below the first and 3.0 percent above the second, so
no slowdown beyond host drift was measurable in that matrix.

Stable semantic JSON was 36.8 to 43.1 percent slower than the two feature-off
controls and increased benchmark peak RSS by 12.8 to 14.4 percent. That mode
retains a high-cardinality in-memory timeline and serializes a Chrome trace.
The result is specific to detailed profiling on this high-volume workload. It
does not imply the same overhead for a short operation with few events.

## Capture volume and sample quality

Configured sample frequency is per sampled thread. Observed aggregate sample
density divides target-process samples by trace or command duration across all
active threads. The aggregate value therefore changes with parallelism and is
not a replacement for the configured frequency.

| Mode | Semantic events | Target native samples | Sample quality | Observed aggregate density | Artifact size |
| --- | ---: | ---: | --- | ---: | ---: |
| Feature-off control | 0 | 0 | Not applicable | 0 | 0 |
| Stable semantic JSON | 190,149 Chrome events maximum per repetition | 0 | Exact semantic timing only | 0 | 338.3 MiB maximum per repetition |
| Samply at 1000 Hz | 0 | 287,020 | 119 lost, about 0.04 percent | 3,218 samples/s | 4.7 MiB for three repetitions |
| Short standard Perfetto | 300,036 slices | 28,745 | 7 skipped and 7 without call sites | 295 samples/s | 67.1 MiB for three repetitions |
| Short deep-system Perfetto, run 1 | 300,036 slices | 29,255 | 11 skipped | 312 samples/s | 70.9 MiB for three repetitions |
| Short deep-system Perfetto, run 2 | 300,036 slices | 28,759 | 7 skipped | 306 samples/s | 71.9 MiB for three repetitions |
| Streaming standard Perfetto | 2,600,000 operator slices plus lifecycle and 26 truncation markers | 229,341 | 23 skipped and 23 without call sites, about 0.010 percent | 381 samples/s | 47.7 MiB for 10 minutes |

The stable semantic row also contained at most 189,666 operation timeline
spans. Its 338.3 MiB value is one compact JSON export per repetition, not the
sum of all three repetitions. Median JSON export time was 5.310 s and was
recorded separately from workflow time.

Each short Perfetto operation reached the documented 100,000 high-cardinality
operator-activity budget. The three-operation traces therefore contain 300,000
operator slices and three visible truncation markers. The remaining slices are
operation, phase, query, and other canonical lifecycle spans. Truncation at the
documented semantic budget is not buffer loss.

The deep-system traces additionally contained 2,293,482 and 2,331,749
scheduler rows. Do not subtract their file sizes from the final short standard
file because those artifacts came from different adapter revisions. The
controlled #522 ablation attributed about 30 to 36 MiB to scheduler events.
Ftrace volume still depends on all host activity during the selected interval.

The production streaming trace lasted 601.639 s and saved 50,020,700 bytes.
Its two 64 MiB ring buffers wrote 491,270,144 semantic bytes and 32,559,104
sampling and process-metadata bytes before deflate compression. Neither buffer
overwrote unread data, and no sequence packet loss, writer packet loss,
data-source loss, or flush failure was reported. The saved file used 9.3
percent of its configured 512 MiB cap.

## Buffer and lifecycle comparison

The table below describes the current checked-in Perfetto configs. The #522
deep-system performance traces used an earlier 512 MiB scheduler buffer, for
704 MiB of service buffers in total. The production config reduced that
scheduler buffer to 256 MiB without changing the two scheduler event types.

| Perfetto mode | Service buffers | File behavior | Startup | Stop and finalization |
| --- | --- | --- | ---: | ---: |
| Short standard | 128 MiB semantic, 64 MiB samples, 4 MiB metadata, all `DISCARD` | Uncompressed short file | 0.13 s | 1.63 s |
| Short deep-system | Short standard plus 256 MiB compact scheduler buffer | Uncompressed short file | Not separately recorded | Not separately recorded |
| Streaming standard | 64 MiB semantic and 64 MiB samples plus metadata, both `RING_BUFFER` | Deflate, 5 s writes, 512 MiB cap | 76.2 ms | 122.1 ms |

Feature-off execution has no profiler startup or finalization. Stable semantic
JSON has no external startup and recorded a 5.310 s median JSON export after
the measured workflow. Samply startup and finalization were not isolated; its
89.18 s command wall time includes both. Deep-system tracebox startup and
finalization were not recorded separately.

The diagnostics-enabled target also requests a bounded 32 MiB producer
shared-memory buffer. Service and producer buffers are memory allocations, not
expected file sizes. The streaming run measured tracebox peak RSS at about 4.3
MiB, while the central buffers belong to the Perfetto tracing service rather
than that small client process.

`DISCARD` preserves the beginning of a short capture and drops later packets
after saturation. `RING_BUFFER` preserves recent packets between streaming
writes. The health result, not the process exit status or file size alone,
determines whether the full requested interval is complete.

## Stock-viewer validation

| Mode | Viewer result |
| --- | --- |
| Stable semantic JSON | VizTracer and Perfetto display the exact semantic hierarchy. No native samples are present. |
| Samply | Firefox Profiler displays native call trees, flame graphs, and source lines. Exact Delta Funnel semantic spans are not present. |
| Short standard Perfetto | Stock Perfetto displays operation, phase, query, worker, and operator tracks. Selecting an operation interval produces a symbolized native Top Down flame graph in the same trace. |
| Short deep-system Perfetto | The short standard views remain available, with scheduler and wakeup context added. |
| Streaming standard Perfetto | Stock Perfetto loaded the 47.7 MiB 10-minute trace and became interactive in 21.09 s. Exact worker filtering and selected-interval Top Down and Bottom Up native views worked. |

The standard short trace contained useful Delta Funnel, Delta Kernel,
DataFusion, Arrow, Parquet, object_store, Tokio, PyO3, libc, and kernel-related
frames. Missing call sites and skipped samples reduce statistical confidence,
but they do not change the exact timestamps of healthy semantic slices.

The exact worker filter
`worker [w-00000000000000000001]` did not match worker 10, worker 14, or other
prefixed identities. Parallel workers stayed on sibling tracks. The viewer did
not force unrelated workers into false nesting.

## Production correctness matrix

The #528 production matrix used the diagnostic Python extension, generated or
repository-owned data, SQL Server integration fixtures, the checked-in capture
configs, and the canonical `capture-health` command.

| Case | Operation result | Capture result |
| --- | --- | --- |
| Preview success | Returned the expected preview | Complete semantic hierarchy and useful native stacks |
| Single SQL Server write success | Returned the successful write report | Complete and isolated operation root |
| Multi-table `write_all` success | Returned successful per-table reports | Complete roots and stable output identities |
| Failure before database mutation | Returned the original planning or execution error | Error result preserved in the semantic root |
| SQL Server write failure | Returned the database write error | Error result preserved without rewriting the database outcome |
| Partial `write_all` failure | Preserved existing per-table status semantics | Successful and failed output results remained distinguishable |
| Two concurrent previews | Both completed with 1.079 s of measured overlap | Separate operation identities and no cross-operation nesting |
| Process interruption | Process exited with status 143 | `capture_complete=0`, `semantic_complete=0`, one incomplete root |
| 100,000-event limit | Operation completed successfully | Exactly 100,000 operator slices and one truncation marker |
| Reduced 64 KiB semantic buffer | Workload completed successfully | 3,442 buffer-loss events and both completion fields false |

The standard success trace had three operation roots, 2,013 operator slices,
and no missing canonical fields, crossing worker slices, or buffer loss. Exact
worker filtering retained only the requested fixed-width identity.

Every operation had exactly one deterministic root. Planning, execution,
result, and close boundaries remained chronological and inside that root.
Required operation, query, worker, node, partition, stream, activity, and timing
identities were present when each operator slice began. Terminal result fields
appeared only when the slice completed. Parent checks found no mismatched
nesting or unstable node-name groups.

Semantic payload inspection found no SQL text, credentials, row values, or
unrestricted workload paths. Native samples can contain local binary, symbol,
and source paths by design, which is why all trace files remain local and need
explicit review before upload.

The matrix also found one separate Python concurrency defect: mutating one
`Session` catalog concurrently can raise a Rust `PyBorrowMutError` panic.
GitHub issue #533 owns converting that condition into a stable Python error.
It does not invalidate the supported concurrent execution case, where tables
are built before two operations execute concurrently.

No capture finalization, health-query, symbolization, or viewer outcome changed
an operation result. A successful database write remained successful even if
later diagnostics were incomplete.

Default dependency isolation was checked with both core and Python package
graphs:

```bash
if cargo tree --locked -p delta-funnel -e normal | rg -i perfetto; then
  exit 1
fi
if cargo tree --locked -p delta-funnel-python -e normal | rg -i perfetto; then
  exit 1
fi
```

Both checks found zero Perfetto dependencies. Only an explicit
`perfetto-profile` feature build links the Perfetto SDK.

## Reproduce the canonical workload

Build the feature-off and diagnostics-enabled optimized binaries:

```bash
cargo build --locked --profile profiling \
  -p delta-funnel \
  --bin delta_scan_partition_bench

cp target/profiling/delta_scan_partition_bench \
  target/profiling/delta_scan_partition_bench-feature-off

cargo build --locked --profile profiling \
  -p delta-funnel \
  --features perfetto-profile \
  --bin delta_scan_partition_bench

ln -sf delta_scan_partition_bench \
  target/profiling/delta-funnel-perfetto-preview
```

Use this exact common argument set:

```bash
benchmark_args=(
  --mode provider-exec
  --seed 0
  --provider-exec-storage-profile local
  --provider-exec-workload provider_wide_event_export_13m
  --provider-exec-query write_all_exports
  --provider-exec-phase-aligned-workflow
  --provider-exec-backend native_async
  --provider-exec-scheduling-profile prefetch_2_parallel_buffer_1
)
```

The complete feature-off, stable semantic JSON, and Samply command matrix is in
[Run Delta Scan Benchmarks](scan-benchmarks.md#compare-samply-with-detailed-profiling).
It brackets the profiled runs with feature-off controls and records command
wall time with GNU `time`. When following it after the builds above, replace
each `target/profiling/delta_scan_partition_bench` path in that matrix with the
preserved `target/profiling/delta_scan_partition_bench-feature-off` path.

For a three-repetition short Perfetto measurement, use the readiness and
shutdown lifecycle from the canonical how-to with one of these configs:

Recommended short mode:

```bash
capture_config=tools/perfetto/delta-funnel-standard.pbtx
capture_path=target/perfetto-captures/benchmark-standard.pftrace
```

Optional scheduler investigation:

```bash
capture_config=tools/perfetto/delta-funnel-deep-system.pbtx
capture_path=target/perfetto-captures/benchmark-deep-system.pftrace
```

Run the diagnostics-enabled alias after tracebox reports readiness:

```bash
/usr/bin/time -f 'command_wall_seconds=%e command_max_rss_kb=%M' \
  target/profiling/delta-funnel-perfetto-preview \
  "${benchmark_args[@]}" \
  --provider-exec-repetitions 3 \
  --output target/perfetto-captures/benchmark.csv
```

For the 10-minute production measurement, select the streaming config and use
26 repetitions:

```bash
capture_config=tools/perfetto/delta-funnel-standard-streaming.pbtx
capture_path=target/perfetto-captures/benchmark-streaming.pftrace
configured_file_cap_bytes=536870912

/usr/bin/time -f 'command_wall_seconds=%e command_max_rss_kb=%M' \
  target/profiling/delta-funnel-perfetto-preview \
  "${benchmark_args[@]}" \
  --provider-exec-repetitions 26 \
  --output target/perfetto-captures/benchmark-streaming.csv
```

Always stop and wait for the external tracebox process separately from the
benchmark result. Then run the canonical health query:

```bash
if test -n "${configured_file_cap_bytes:-}"; then
  tools/perfetto/capture-health \
    "$capture_path" "$configured_file_cap_bytes"
else
  tools/perfetto/capture-health "$capture_path"
fi
```

Record the CSV, GNU `time` output, capture size, health row, tool versions,
config file, commit, and host policy. Reopen the trace in a fresh stock
Perfetto session before claiming viewer usability.

## Prototype cleanup audit

The final tracked tree was compared with the #522 prototype commit after
production parity had passed. No prototype-only runtime remains:

- `perfetto_capability_spike` and its `capability-spike.pbtx` config were
  removed. Production adapter unit tests, Python activation tests, the
  repository example, and the end-to-end matrix cover their supported
  properties.
- The adapter moved from the binary-private `src/bin/perfetto_profile` module
  to the single feature-gated `src/perfetto_profile` production module.
- `phase-aligned-write-all-standard.pbtx` became
  `delta-funnel-standard.pbtx`.
- `phase-aligned-write-all.pbtx` became `delta-funnel-deep-system.pbtx`.
- Bounded long capture uses the additional production
  `delta-funnel-standard-streaming.pbtx` config and canonical
  `capture-health` command.
- The `delta_scan_partition_bench` binary remains because it is the canonical
  generated correctness, performance, and volume workload. It is not a second
  adapter or a capability-only harness.
- Stable semantic JSON remains because it is a supported public diagnostic
  format. It does not duplicate Perfetto capture control or native sampling.
- The old root documentation path is a short compatibility redirect to the
  canonical how-to, not a second maintained guide.

A tracked-file search found no remaining spike-named binary, config, module,
custom Perfetto exporter, temporary trace parser, trace merger, or run diary.
Historical evidence remains in GitHub issue #522 and ignored local `target/`
artifacts rather than in production source.

## Final repository verification

The closeout branch passed the default and feature-enabled repository checks:

```bash
cargo fmt --all -- --check
cargo check --locked --workspace --all-targets
cargo test --locked --workspace --all-targets
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace --all-targets --all-features
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
RUSTDOCFLAGS='-D warnings' \
  cargo doc --locked --workspace --all-features --no-deps
cargo xtask python-package-check
python -m pip install -r docs-site/requirements.txt
python -m zensical build --strict -f docs-site/mkdocs.yml
git diff --check
```

The default dependency-tree checks shown above found no Perfetto dependency in
the core or Python package. The normal Python package check built an abi3 wheel,
verified its contents and metadata, installed it into a clean environment, and
passed terminal, minimum-Rich, and Jupyter progress smoke tests.

The diagnostic wheel passed a separate optimized build and clean-environment
import check:

```bash
maturin build --locked --profile profiling \
  --features perfetto-profile \
  --skip-auditwheel \
  --out target/python-perfetto-closeout-wheels \
  --manifest-path crates/delta-funnel-python/Cargo.toml
```

The installed diagnostic wheel exported `init_perfetto_diagnostics()` and
returned the structured `capture_timeout` kind when invoked without an active
capture. This distinguishes the feature-enabled wheel from the default wheel's
`not_available` result without starting an expensive workload.

The all-feature run passed 1,421 core tests and 167 Python binding tests, with
no failures. The default run passed 1,412 core tests and 160 Python binding
tests, with no failures. SQL Server tests that require an external target are
ignored by the ordinary workspace command; the #528 production matrix ran
those paths separately. Stock Perfetto short and streaming inspection was also
performed separately because it is an interactive acceptance check, not a
unit test.

## Interpretation limits

- The host was shared and the matrices were not randomized. Small differences
  can be host drift.
- Three short repetitions are enough for a development baseline, not a narrow
  confidence interval.
- The 10-minute row contains repeated operations in one capture, not repeated
  independent 10-minute captures.
- Generated local Delta data does not model a specific S3 service, private
  query, SQL Server target, or production contention pattern.
- A large number of short operations receives one semantic event budget per
  operation and can grow a trace faster than one long capped operation.
- Linux Perfetto and Samply measurements contain on-CPU samples only.
  Off-CPU time requires scheduler evidence and careful correlation.
- Deep-system ftrace can expose host-wide activity. Its volume, overhead, and
  privacy surface depend on other processes running during the capture.
- The `.pftrace` can contain process names, command lines, source paths, and
  symbols. Keep it local unless an explicit review and upload decision permits
  sharing.

No private SQL, credentials, paths, table names, row values, screenshots,
traces, or symbol bundles are committed with this report. Generated evidence
remains under ignored `target/` paths.

## Related issues

- [#510: Migrate profiling to a unified mature tracing architecture](https://github.com/mag1cfrog/delta-funnel/issues/510)
- [#511: Establish profiling overhead baselines](https://github.com/mag1cfrog/delta-funnel/issues/511)
- [#512: Establish the Samply reference baseline](https://github.com/mag1cfrog/delta-funnel/issues/512)
- [#522: Prototype unified Perfetto capture](https://github.com/mag1cfrog/delta-funnel/issues/522)
- [#527: Add bounded streaming and capture health](https://github.com/mag1cfrog/delta-funnel/issues/527)
- [#528: Validate and document production diagnostics](https://github.com/mag1cfrog/delta-funnel/issues/528)
- [#533: Return a Python error for concurrent Session catalog mutation](https://github.com/mag1cfrog/delta-funnel/issues/533)
