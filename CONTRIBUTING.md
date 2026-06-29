# Contributing

Thanks for helping improve Delta Funnel.

## Local Development

Install a current Rust toolchain with the workspace `rust-version` from
`Cargo.toml`. Python package work also needs Python 3.10 or newer and maturin.

Run the standard checks before opening a pull request:

```bash
cargo fmt --all --check
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

Build and smoke-test the Python wheel with:

```bash
cargo xtask python-package-check
```

SQL Server integration tests are opt-in:

```bash
cargo xtask sqlserver-test
```

## Pull Requests

Keep changes focused. Runtime behavior changes should include the smallest
runnable test that would fail without the change. Do not include secrets,
private hostnames, or real connection strings in issues, tests, docs, or logs.
