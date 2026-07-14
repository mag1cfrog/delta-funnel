# SQL Server integration tests

DeltaFunnel's SQL Server integration tests are opt-in through the xtask runner.
A normal `cargo test --workspace` run does not require SQL Server. The Rust
integration target skips database work when its environment variables are
absent, and the Python database tests are ignored unless explicitly selected.

Run the managed SQL Server test suite with:

```sh
cargo xtask sqlserver-test
```

The xtask runner starts a local SQL Server container, waits for readiness,
creates the test database, sets compatibility level 100, runs the Rust and
Python integration tests, and removes the container when the command exits.

## What the suite covers

The `mssql_direct_raw_bulk` integration test target exercises SQL Server writes
through the Rust public APIs. It covers direct raw bulk writes, the high-level
orchestrator runtime, append and create-and-load modes, and timestamp handling.

The Python test filters cover single-output and multi-output writes. They check
default and per-write connections, progress reporting, partial failures,
skipped outputs, and replace-mode behavior, including empty output to a missing
target.

All suites use unique table names, verify reports and persisted values, and
remove their test tables.

## Container runtime

The runner supports Docker-compatible runtimes such as Docker and Podman.
Runtime selection uses:

1. `--container-runtime`
2. `DELTA_FUNNEL_CONTAINER_RUNTIME`
3. `docker` on `PATH`
4. `podman` on `PATH`

Examples:

```sh
cargo xtask sqlserver-test --container-runtime podman
DELTA_FUNNEL_CONTAINER_RUNTIME=podman cargo xtask sqlserver-test
```

## Existing SQL Server

To use an existing SQL Server instead of a local container:

```sh
cargo xtask sqlserver-test \
  --connection-string 'server=tcp:127.0.0.1,1433;user id=sa;password=...;TrustServerCertificate=true' \
  --database delta_funnel_integration \
  --schema dbo
```

The schema must already exist and the configured SQL Server login must be able
to create and drop tables in that schema. The default schema is `dbo`.

The xtask runner passes these environment variables to the integration tests:

```text
DELTA_FUNNEL_MSSQL_TEST_CONNECTION_STRING
DELTA_FUNNEL_MSSQL_TEST_SCHEMA
```

The lower-level commands are only for CI or debugging when SQL Server is
already configured and those environment variables are set:

```sh
cargo test -p delta-funnel --test mssql_direct_raw_bulk -- --nocapture
cargo test -p delta-funnel-python table_write_to_mssql_execute_writes -- --ignored --nocapture
cargo test -p delta-funnel-python write_all_execute_writes -- --ignored --nocapture
```

Test table names include process and timestamp values to avoid collisions. The
raw connection string is only read from the environment and passed to
connection setup; test skip messages and table names do not include credential
material.
