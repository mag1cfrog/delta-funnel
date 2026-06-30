# Development Workflow

Use the standard local checks before opening or updating a pull request.

```bash
cargo fmt --all --check
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
git diff --check
```

Run the Python package smoke path when changes affect the Python package or
public packaging behavior:

```bash
cargo xtask python-package-check
```

Run SQL Server integration tests only when SQL Server behavior changes:

```bash
cargo xtask sqlserver-test
```

The `SQL Server Integration` GitHub Actions workflow runs the same command on a
GitHub-hosted Linux runner. Run it manually before a release, and use the pull
request trigger to validate changes to the SQL Server write path.

Build the docs site with:

```bash
python -m pip install -r docs-site/requirements.txt && python -m mkdocs build --strict -f docs-site/mkdocs.yml
```

The pull request CI runs the core Rust and Python package checks for code
changes. SQL Server integration tests remain opt-in.
