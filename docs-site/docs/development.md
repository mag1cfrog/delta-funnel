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

## Rust releases

Release-plz manages Rust release PRs for the `delta-funnel` crate only.
`delta-funnel-python` and `xtask` are not published crates.

Before the first crates.io release, publish `delta-funnel` manually. After that,
configure crates.io trusted publishing for the `Release-plz` workflow and the
`crates-io-release` GitHub environment.

First crates.io publish:

```bash
cargo publish -p delta-funnel --dry-run
cargo publish -p delta-funnel
```

Repository setup:

- Allow GitHub Actions to create pull requests.
- Create the `crates-io-release` GitHub environment.
- Add a `RELEASE_PLZ_TOKEN` secret if release-plz PRs need to trigger required
  pull request checks automatically.
- Set the `RELEASE_PLZ_PR_ENABLED` repository variable to `true` after the first
  manual crates.io publish.

Normal Rust release flow:

1. Let the `Release-plz PR` job open or update the release PR after changes land
   on `main`.
2. Review and merge the release PR when the crate is ready to release.
3. Run the `Release-plz` workflow manually.
4. Approve the `crates-io-release` environment deployment.
