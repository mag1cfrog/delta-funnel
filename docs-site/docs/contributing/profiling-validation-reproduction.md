# Reproduce the Profiling Validation

This appendix contains the commands and repository checks behind the
[profiling validation report](profiling-validation-report.md). Use it when
reproducing or updating that evidence. It is not required for ordinary
workload profiling.

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
shutdown lifecycle from the
[Perfetto profiling how-to](profiling-perfetto.md) with one of these configs:

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

The default dependency-tree checks shown in the validation report found no
Perfetto dependency in the core or Python package. The normal Python package
check built an abi3 wheel, verified its contents and metadata, installed it
into a clean environment, and passed terminal, minimum-Rich, and Jupyter
progress smoke tests.

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
