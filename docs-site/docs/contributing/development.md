# Local Development

Use this guide when developing Delta Funnel itself. Run all commands from the
repository root.

## Prerequisites

- Rust toolchain matching the workspace `rust-version`
- Python 3.10 or newer
- `maturin` when building the Python wheel

## Get the source

```bash
git clone https://github.com/mag1cfrog/delta-funnel.git
cd delta-funnel
```

Install `maturin` if it is not already available:

```bash
python -m pip install "maturin>=1.11,<2"
```

## Check the Rust workspace

```bash
cargo check --workspace
```

## Build and smoke-test the Python wheel

```bash
cargo xtask python-package-check
```

This command builds the `deltafunnel` wheel, checks the typing marker files,
installs the wheel into a clean virtual environment, imports `deltafunnel`, and
constructs `Session()`.

## Run the SQL Server integration tests

SQL Server tests are opt-in and managed by xtask:

```bash
cargo xtask sqlserver-test
```

The runner can start a local SQL Server container, create the test database,
run Rust and Python write tests, and remove the container when it exits.

See the detailed guide:
[SQL Server integration tests](https://github.com/mag1cfrog/delta-funnel/blob/main/docs/mssql-integration-tests.md).
