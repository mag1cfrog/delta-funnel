---
title: "Delta Funnel: Delta Lake to SQL Server"
description: Delta Lake to SQL Server without Spark or JDBC/ODBC bottlenecks. Transform with DataFusion SQL and bulk-load through native TDS.
---

# Delta Funnel

![Surreal banner showing Delta Lake data flowing through a Rust-orange funnel into a database barrel.](https://raw.githubusercontent.com/mag1cfrog/delta-funnel/main/assets/delta-funnel-banner.jpg)

<h3 align="center">
  <strong>Delta Lake to SQL Server. No Spark. No JDBC/ODBC bottleneck.</strong>
</h3>

<p align="center">
  DataFusion SQL in.<br/>
  Native TDS bulk load out.
</p>

<p align="center">
  <strong>Observed:</strong> 13.4M rows in ~14 minutes vs. a ~2 hour Spark/JDBC path.
</p>

<p align="center">
  <a href="https://docs.rs/delta-funnel"><img alt="Rust docs" src="https://docs.rs/delta-funnel/badge.svg"></a>
  <a href="https://crates.io/crates/delta-funnel"><img alt="crates.io" src="https://img.shields.io/crates/v/delta-funnel.svg"></a>
  <a href="https://pypi.org/project/deltafunnel/"><img alt="PyPI" src="https://img.shields.io/pypi/v/deltafunnel.svg"></a>
  <a href="https://pypi.org/project/deltafunnel/"><img alt="Python 3.10+" src="https://img.shields.io/badge/python-3.10%2B-blue.svg"></a>
</p>

Project links: [GitHub](https://github.com/mag1cfrog/delta-funnel),
[PyPI](https://pypi.org/project/deltafunnel/),
[crates.io](https://crates.io/crates/delta-funnel),
[docs.rs](https://docs.rs/delta-funnel),
and [release notes](https://github.com/mag1cfrog/delta-funnel/releases).

## Why I Wrote This

People like to have the finalized golden-layer data ported into a relational database like MSSQL. I work at an on-prem Microsoft shop, which means the practical deployment target was a Windows VM, so I had to set up WSL + Spark just to do the job. And because [`sql-spark-connector`](https://github.com/microsoft/sql-spark-connector) is no longer maintained, I had to deal with slow plain JDBC writes as well.

One day I had enough of both, so I decided to pull together a native solution on top of [`delta-kernel-rs`](https://github.com/delta-io/delta-kernel-rs), [`tiberius`](https://github.com/prisma/tiberius), and [`datafusion`](https://github.com/apache/datafusion), without the overhead of JVM or JDBC/ODBC. It works unexpectedly well.

!!! note "Project status"
    Delta Funnel is early project code. The Rust crate is available on
    crates.io, and the Python package is available on PyPI.

## Start here

Python users can follow these pages in order:

1. [Installation](install.md): add the Rust crate or Python package.
2. [Python quickstart](python-api-walkthrough.md): register a Delta table,
   transform it, and write to SQL Server.
3. [Core concepts](concepts.md): understand sessions, sources, tables, outputs,
   and reports.

Rust users can follow the same path through [Installation](install.md), the
[Rust quickstart](rust-quickstart.md), and [Core concepts](concepts.md).

## Find what you need

- [SQL Server writes](sql-server.md): configure connections and load modes.
- [Dry runs and reports](dry-runs-reports.md): validate workflows and inspect
  structured results.
- [Multiple outputs and shared caching](advanced/multiple-outputs.md): write
  related outputs without repeating common upstream work.
- [Progress displays](progress.md): configure and interpret live Python
  progress.
- [Private S3 sources](advanced/private-s3.md): configure credentials and
  troubleshoot source access.
- [Python logging](advanced/python-logging.md): route diagnostic events through
  standard-library logging.
- [Failure diagnostics](advanced/failure-diagnostics.md): interpret reports and
  collect safe information for troubleshooting.
- [API references](reference/api.md): find the Rust and Python API entry
  points.
