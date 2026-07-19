# Capture Python Perfetto Diagnostics

This development workflow captures Delta Funnel's semantic operation hierarchy
and sampled native Rust call stacks in one Perfetto trace. It is intended for
occasional diagnosis on Linux. Published Python wheels do not include the
Perfetto producer.

## Prerequisites

Install these tools and make them available on `PATH`:

- `maturin`
- Perfetto `tracebox`

On Linux, `tracebox` also needs permission to sample performance events. This
temporary setting lasts until reboot:

```sh
echo '-1' | sudo tee /proc/sys/kernel/perf_event_paranoid
```

Use the least permissive setting that works in your environment. Systems with
stricter security requirements should use their normal performance-monitoring
policy instead.

## Build a diagnostics-enabled Python extension

From the repository root, create and activate a dedicated virtual environment:

```sh
python -m venv target/python-perfetto-venv
source target/python-perfetto-venv/bin/activate
maturin develop --locked --profile profiling \
  --features perfetto-profile \
  --manifest-path crates/delta-funnel-python/Cargo.toml
```

The `profiling` profile preserves information needed to symbolize native call
stacks. The `perfetto-profile` feature is opt-in so normal builds and published
wheels do not link the Perfetto SDK.

## Start the external capture

Start `tracebox` before activating diagnostics in Python:

```sh
mkdir -p target/perfetto-spike
trace_pid="$(
  tracebox --txt --system-sockets --background-wait \
    --config tools/perfetto/phase-aligned-write-all-standard.pbtx \
    --out target/perfetto-spike/python-preview.pftrace
)"
```

The standard configuration targets both the Rust diagnostic benchmark and
Python. It samples native call stacks at 100 Hz while preserving exact Delta
Funnel semantic spans on a separate buffer.

## Activate diagnostics and run a preview

Call `init_perfetto_diagnostics()` once, before `init_logging()` and before any
preview or write operation:

```sh
python - <<'PY'
import deltafunnel

installed = deltafunnel.init_perfetto_diagnostics(
    wait_timeout_seconds=10.0,
)
if not installed:
    raise RuntimeError(
        "another global tracing subscriber is already installed; "
        "start a fresh Python process and activate Perfetto diagnostics first"
    )

session = deltafunnel.Session()
table = session.table_from_sql(
    "SELECT SUM(LENGTH(REGEXP_REPLACE(CAST(value AS VARCHAR), "
    "'[0-9]', 'x', 'g'))) AS total "
    "FROM generate_series(1, 200000000) AS series(value)"
)
preview = table.preview(limit=1, progress=False)
print(preview.text)
PY
```

This example generates data in memory and is deliberately long enough to
produce a useful sampled call stack on a typical development machine. Adjust
the generated row count for the machine under test.

`DELTAFUNNEL_LOG` and the function's `filter` and `logger` arguments configure
the Python logging side of the combined subscriber. They do not disable the
`delta_funnel.profile` events sent to Perfetto.

## Stop and inspect the capture

After the Python process exits, stop the external capture cleanly:

```sh
kill -TERM "$trace_pid"
wait "$trace_pid"
```

Open `target/perfetto-spike/python-preview.pftrace` in
[Perfetto UI](https://ui.perfetto.dev/). The `Delta Funnel diagnostics` process
track contains this hierarchy:

```text
Operation
  Phases
  Query
    Worker
      Operator and lower-level activity
```

Select the operation's time range, then use the `Perf sample flamegraph` panel
in `Top Down` mode to move from sampled native entry points into deeper Rust
functions. Use an exact worker identity such as
`w-00000000000000000001]` in the track-name filter when isolating one logical
worker. The closing bracket prevents worker 1 from also matching worker 10 or
worker 14.

Perfetto may report a small number of skipped performance samples. That affects
sample density, not the exact semantic spans. Treat a high skipped-sample count
or lost semantic packets as an unhealthy capture.

## Activation results and errors

`init_perfetto_diagnostics()` has one-shot process-wide behavior:

- `True` means the combined Python logging and Perfetto subscriber was installed
  and an external capture was ready before the function returned.
- `False` means another global tracing subscriber was already installed. The
  function does not replace it. Start a fresh Python process and call
  `init_perfetto_diagnostics()` first.
- A `DeltaFunnelError` with phase `perfetto_diagnostics` reports an unavailable
  build, invalid argument, producer failure, capture timeout, or unavailable
  capture through its structured `kind` field.

The stable diagnostic error kinds are:

```text
not_available
invalid_logger
invalid_wait_timeout
producer_initialization_failed
capture_timeout
capture_unavailable
```

An invalid logging filter remains a configuration error with kind
`invalid_logging_filter`.

Python activates the producer and subscriber, but it does not start, stop, or
finalize `tracebox`. Once activation succeeds, later capture-service problems
cannot change a successful preview or database write into a Python operation
failure. Always stop and validate the external trace separately.
