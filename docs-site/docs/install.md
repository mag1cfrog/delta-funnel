# Installation

Install the Rust crate from crates.io or the Python package from PyPI.

## Rust crate

```bash
cargo add delta-funnel
```

## Python package

The Python package requires Python 3.10 or newer.

For Python projects managed by uv, add the `deltafunnel` package:

```bash
uv add deltafunnel
```

For an existing Python environment, install it with pip:

```bash
python -m pip install deltafunnel
```

## Private S3 note for Python users

If you are reading a private S3 Delta table from a local shell, see the
[Python API walkthrough](python-api-walkthrough.md#read-a-private-s3-delta-table-from-a-local-shell)
before debugging Delta snapshot behavior. On the current S3 path, Delta Funnel
expects explicit `storage_options` credentials for local shell usage.
