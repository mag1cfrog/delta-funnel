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
