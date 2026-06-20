# SQL Server integration tests

DeltaFunnel's SQL Server integration tests are opt-in through the xtask runner.
A normal `cargo test --workspace` run does not require SQL Server; tests that
need a database print a skip message and return successfully when the required
environment variables are absent.

Run the managed SQL Server test suite with:

```sh
cargo xtask sqlserver-test
```

The xtask runner starts a local SQL Server container, waits for readiness,
creates the test database, sets compatibility level 100, runs the SQL Server
integration tests, and removes the container when the command exits.

## DirectRawBulk sink test

The `mssql_direct_raw_bulk` integration test exercises the public
`write_output_batches_to_mssql` API with the default `WriteBackend::DirectRawBulk`
path. When configured, it creates a unique append-existing test table, writes two
Arrow record batches, checks the returned write stats, and drops the table.

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

The lower-level command is only for CI/debugging when SQL Server is already
configured and those environment variables are set:

```sh
cargo test -p delta-funnel --test mssql_direct_raw_bulk -- --nocapture
```

Test tables use the `df_mssql_it_` prefix plus process and timestamp values to
avoid collisions. The raw connection string is only read from the environment
and passed to connection setup; test skip messages and table names do not include
credential material.
