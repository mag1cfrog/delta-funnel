# Delta Funnel

Delta Funnel moves Delta Lake data into SQL Server without Spark or ODBC.

Use it when you need a focused single-node pipeline that can read Delta Lake
tables, transform rows with SQL, and bulk-load results into Microsoft SQL
Server through a native TDS driver.

!!! note "Project status"
    Delta Funnel is early project code. The Rust crate is available on
    crates.io, and the Python package is available on PyPI.

## Start here

- [Installation](install.md): add the Rust crate or Python package.
- [Python API walkthrough](python-api-walkthrough.md): register a Delta table, transform it, and write to SQL Server.
- [Concepts](concepts.md): learn the core objects: session, source, table, output, and report.
- [SQL Server](sql-server.md): configure SQL Server writes and run integration tests.

## What this site covers

This site is a navigable entry point for public users and contributors. It
links deeper engineering notes where those notes already exist instead of
duplicating them.

For the source repository, see
[mag1cfrog/delta-funnel](https://github.com/mag1cfrog/delta-funnel).
