# Python Logging

Use `deltafunnel.init_logging()` to route Delta Funnel's Rust tracing events
through standard-library `logging` before running a workflow.

## Enable the logging bridge

Configure Python logging first, then initialize the bridge:

```python
import logging
import deltafunnel

logging.basicConfig(level=logging.INFO)
installed = deltafunnel.init_logging()
```

`init_logging()` installs a process-global bridge to
`logging.getLogger("deltafunnel")`. It returns `True` when the bridge is
installed and `False` when a global Rust tracing subscriber is already
installed.

The first successful call selects the bridge configuration for the process.
Later calls that return `False` do not replace it. Pass `logger="name"` on the
first call to use a different Python logger.

## Select diagnostic events

Pass a tracing filter on the first call when you need more detail:

```python
deltafunnel.init_logging(
    "delta_funnel=debug,delta_kernel=debug,object_store=debug,arrow_tiberius=debug"
)
```

You can also set the `DELTAFUNNEL_LOG` environment variable instead of passing
a filter. An explicit filter argument takes precedence. When neither is set,
Delta Funnel uses `delta_funnel=info,arrow_tiberius=info`.

DEBUG events must pass both the Rust tracing filter and Python logging levels.
For example, this configuration enables terminal Parquet I/O and execution
profile summaries:

```python
import logging
import deltafunnel

handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)

logger = logging.getLogger("deltafunnel")
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)
logger.propagate = False

deltafunnel.init_logging("delta_funnel=debug")
```

`DELTAFUNNEL_LOG` and the filter passed to `init_logging()` control only the
Rust tracing filter. They do not lower the selected Python logger or handler
level. A Python INFO threshold still discards forwarded DEBUG records.

For event fields and terminal outcome rules, see
[Inspect terminal Parquet I/O](../reference/diagnostics.md#inspect-terminal-parquet-io)
and
[Inspect terminal execution profiles](../reference/diagnostics.md#inspect-terminal-execution-profiles).

## Keep Python in control

Delta Funnel does not configure Python handlers, formatters, levels, files, or
external exporters. Existing Datadog, OpenTelemetry, JSON logging, file
logging, pytest capture, notebook, and framework integrations continue to own
Python logging output.

For report fields and a safe bug-report checklist, see
[Troubleshoot a failed run](tracing-and-diagnostics.md).
