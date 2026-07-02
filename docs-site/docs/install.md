# Installation And Local Build

Install the Rust crate from crates.io or the Python package from PyPI.

## Prerequisites

- Rust toolchain matching the workspace `rust-version`
- Python 3.10 or newer for the Python extension
- `maturin` when building the Python wheel

## Rust crate

```bash
cargo add delta-funnel
```

## Python package

For Python projects managed by uv, add the `deltafunnel` package:

```bash
uv add deltafunnel
```

For an existing Python environment, install it with pip:

```bash
python -m pip install deltafunnel
```

## Local build

Use a local build when developing Delta Funnel itself.

Get the source:

```bash
git clone https://github.com/mag1cfrog/delta-funnel.git
cd delta-funnel
```

Run all commands below from the repository root.

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

## Private S3 note for Python users

If you are reading a private S3 Delta table from a local shell, see the
[Python API walkthrough](python-api-walkthrough.md#read-a-private-s3-delta-table-from-a-local-shell)
before debugging Delta snapshot behavior. On the current S3 path, Delta Funnel
expects explicit `storage_options` credentials for local shell usage.
