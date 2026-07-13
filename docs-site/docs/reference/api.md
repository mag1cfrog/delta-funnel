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

- `Session`
- `PendingDeltaSource`
- `Table`
- `Preview`
- `MssqlOutputSpec`
- `DeltaFunnelError`

`Table.preview(limit=20, *, progress=None)` returns a `Preview` object.
`Table.show(limit=20, *, progress=None)` executes the same preview and prints
the text form to Python stdout. Both execute the DataFusion query with the limit
applied before collection, read rows, and do not contact or write to SQL Server.
`Preview.text` is the plain text table and `Preview.html` backs notebook
`_repr_html_()` display.

For both methods, `progress=None` enables automatic terminal and notebook
progress, `True` forces progress, and `False` disables it. Eligible Delta plans
show selected-file progress, which may remain partial when the limit ends the
query early. Other plans stay indeterminate. The limit is not used as a
progress total. Notebook progress finishes before preview output; terminal
progress uses stderr and leaves `show()` stdout table-only.

For Delta sources, `Session.delta_lake(..., storage_options=...)` accepts a
mapping of string keys and values and forwards them to the underlying
object-store builder used by Delta Funnel. For private S3 tables, see the
[Python API walkthrough](../python-api-walkthrough.md) for the exact
documented AWS keys, examples, and troubleshooting guidance.
