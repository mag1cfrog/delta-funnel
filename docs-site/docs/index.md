---
title: "Delta Funnel: Delta Lake to SQL Server"
description: Delta Lake to SQL Server without Spark or JDBC/ODBC bottlenecks. Transform with DataFusion SQL and bulk-load through native TDS.
---

# Delta Funnel

Move Delta Lake data into SQL Server without Spark or an ODBC driver. Transform
rows with DataFusion SQL, then load them through native TDS bulk writes.

!!! note "Project status"
    Delta Funnel is early project code. The Rust crate is available on
    crates.io, and the Python package is available on PyPI.

## Start here

Follow these steps in order:

1. [Install Delta Funnel](install.md).
2. Choose one language:
    - [Python quickstart](python-api-walkthrough.md)
    - [Rust quickstart](rust-quickstart.md)
3. [Core concepts](concepts.md): understand sessions, sources, tables, outputs,
   and reports.

## Common tasks

- [SQL Server writes](sql-server.md): configure connections and load modes.
- [Dry runs and reports](dry-runs-reports.md): validate workflows and inspect
  structured results.
- [Multiple outputs and shared caching](advanced/multiple-outputs.md): write
  related outputs without repeating common upstream work.
- [Progress displays](progress.md): configure and interpret live Python
  progress.
- [Private S3 sources](advanced/private-s3.md): configure credentials and
  troubleshoot source access.

## Troubleshoot a run

- [Python logging](advanced/python-logging.md): route diagnostic events through
  standard-library logging.
- [Troubleshoot a failed run](advanced/tracing-and-diagnostics.md): inspect
  failure reports and collect safe troubleshooting information.

## Profile performance

- [Choose a profiling method](contributing/profiling.md): select a semantic
  trace, Samply, or Perfetto based on the question you need to answer.
- [Export execution profiles](advanced/execution-profiling.md): inspect preview
  and SQL Server operations on a shared wall-clock timeline.

## Reference

- [API references](reference/api.md): find the Rust and Python API entry
  points.
- [Diagnostics reference](reference/diagnostics.md): look up tracing events,
  operation phases, stream outcomes, and cache lifecycle fields.
- [Execution profile reference](reference/execution-profile.md): look up the
  returned profile schema, metrics, labels, and redaction rules.

## About Delta Funnel

![Surreal banner showing Delta Lake data flowing through a Rust-orange funnel into a database barrel.](https://raw.githubusercontent.com/mag1cfrog/delta-funnel/main/assets/delta-funnel-banner.jpg)

**Observed:** 13.4M rows in ~14 minutes vs. a ~2 hour Spark/JDBC path.

Project links: [GitHub](https://github.com/mag1cfrog/delta-funnel),
[PyPI](https://pypi.org/project/deltafunnel/),
[crates.io](https://crates.io/crates/delta-funnel),
[docs.rs](https://docs.rs/delta-funnel),
and [release notes](https://github.com/mag1cfrog/delta-funnel/releases).

### Why I wrote this

People like to have finalized golden-layer data ported into a relational
database such as SQL Server. I work at an on-prem Microsoft shop, which made
the practical deployment target a Windows VM. I had to set up WSL and Spark
just to do that job, then deal with slow JDBC writes because
[`sql-spark-connector`](https://github.com/microsoft/sql-spark-connector) is no
longer maintained.

I built a native solution on top of
[`delta-kernel-rs`](https://github.com/delta-io/delta-kernel-rs),
[`tiberius`](https://github.com/prisma/tiberius), and
[`datafusion`](https://github.com/apache/datafusion) without the overhead of
the JVM or JDBC/ODBC.
