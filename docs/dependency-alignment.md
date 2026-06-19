# Dependency Alignment

This document records the foundation decisions for the first DeltaFunnel MVP.
It intentionally avoids defining future config or report shapes before the
owning feature work needs them.

## Selected Dependencies

- `snafu = "0.9"` is the Rust error-handling framework.
- `arrow-tiberius = "0.1.1"` is the Arrow to SQL Server planning and write
  path.
- `arrow-tiberius` brings in `tiberius-raw-bulk =0.12.3-raw-bulk.13`
  transitively. DeltaFunnel should add a direct `tiberius` dependency only when
  the SQL sink issue constructs a `tiberius::Client`; that direct dependency
  must use the `tiberius-raw-bulk` package identity.
- `delta_kernel = "0.23.0"` is used with Arrow 58, the default engine, and an
  explicit `internal-api` decision.

The public `RecordBatch` path should stay on Arrow 58 across `delta_kernel`,
`arrow-tiberius`, and DeltaFunnel. A second Arrow major version in that path is
a blocker unless a deliberate conversion boundary is added.

## Error Pattern

DeltaFunnel uses SNAFU for errors. The foundation crate currently exposes the
explicit `DeltaFunnelError` type and does not define a crate-level `Result`
alias. Later issues should add their own phase-specific variants when they
implement those phases.

Error display messages must be sanitized. Dependency debug output, SQL Server
connection strings, object-store credentials, access keys, secret keys, and
session tokens must not be copied into default `Display` messages or
Python-facing errors.

## Delta Kernel Boundary

Stability-sensitive `delta_kernel` APIs are kept behind the private
`table_formats::delta::kernel` module. The foundation tests compile against
the API symbols that later Delta source and reader issues depend on:

- `scan_metadata`
- `visit_scan_files`
- `try_parse_uri`
- `store_from_url_opts`
- `Snapshot::builder_for`
- `SnapshotBuilder::at_version`
- `get_selection_vector`
- `SelectionVector` through the deletion-vector selection API
- `transform_to_logical`
- Arrow engine-data conversion

If a future `delta_kernel` upgrade moves or hides these APIs, the adapter smoke
test should fail before feature code silently drops transforms or deletion
vectors.

## What This Foundation Does Not Own

The foundation issue does not define broad public API scaffolding. Types such
as `RunMode`, `ExportPlan`, `ExportReport`, validation options, row/file count
reports, schema-planning reports, dry-run reports, and table-lifecycle reports
belong to the first feature issue that needs them to compile.
