# DeltaFunnel

DeltaFunnel is an early Rust workspace for exporting Delta Lake tables into
Microsoft SQL Server efficiently.

The initial direction is:

- Read Delta table snapshots from object storage.
- Shape Arrow record batches for efficient bulk loading.
- Use `arrow-tiberius` as the SQL Server write path.
- Add Python bindings through PyO3 after the Rust API settles.

The workspace currently contains the core `delta-funnel` crate. More crates can
be added for Python bindings, CLI tools, or integration test harnesses as the
design hardens.

Foundation dependency and error-handling decisions are recorded in
[`docs/dependency-alignment.md`](docs/dependency-alignment.md).
