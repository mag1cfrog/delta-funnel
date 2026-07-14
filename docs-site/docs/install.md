# Installation

Install the Rust crate from crates.io or the Python package from PyPI.

## Rust crate

The Rust crate requires Rust 1.88 or newer.

```bash
cargo add delta-funnel
```

## Python package

The Python package requires Python 3.10 or newer. PyPI provides prebuilt
wheels for:

- Linux x86_64 with glibc 2.28 or newer
- Windows x86_64
- macOS arm64
- macOS x86_64

Delta Funnel does not currently publish a source distribution. `pip` cannot
install the package on other operating systems or architectures unless a
compatible wheel is added.

For Python projects managed by uv, add the `deltafunnel` package:

```bash
uv add deltafunnel
```

For an existing Python environment, install it with pip:

```bash
python -m pip install deltafunnel
```
