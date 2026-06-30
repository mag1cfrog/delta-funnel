# Installation And Local Build

Delta Funnel is not published to PyPI or crates.io yet. Build it from the
repository.

## Prerequisites

- Rust toolchain matching the workspace `rust-version`
- Python 3.10 or newer for the Python extension
- `maturin` when building the Python wheel

## Get the source

```bash
git clone https://github.com/mag1cfrog/delta-funnel.git
cd delta-funnel
```

Run all commands below from the repository root.

Install `maturin` if it is not already available:

```bash
python -m pip install "maturin>=1.11,<2"
```

## Build the Rust workspace

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
