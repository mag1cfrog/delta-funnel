---
title: "Delta Funnel: Delta Lake to SQL Server"
description: Lightweight Rust and Python toolkit for fast Delta Lake to SQL Server loads with DataFusion SQL and native TDS bulk writes, without Spark or ODBC.
---

# Delta Funnel

![Surreal banner showing Delta Lake data flowing through a Rust-orange funnel into a database barrel.](https://raw.githubusercontent.com/mag1cfrog/delta-funnel/main/assets/delta-funnel-banner.jpg)

<h3 align="center">
  <strong>Fast, lightweight Delta Lake to SQL Server loads without Spark or ODBC.</strong>
</h3>

<p align="center">
  A lightweight Rust and Python toolkit for reading Delta Lake tables,<br/>
  transforming them with DataFusion SQL, and writing through native TDS bulk loads.
</p>

<p align="center">
  <a href="https://docs.rs/delta-funnel"><img alt="Rust docs" src="https://docs.rs/delta-funnel/badge.svg"></a>
  <a href="https://crates.io/crates/delta-funnel"><img alt="crates.io" src="https://img.shields.io/crates/v/delta-funnel.svg"></a>
  <a href="https://pypi.org/project/deltafunnel/"><img alt="PyPI" src="https://img.shields.io/pypi/v/deltafunnel.svg"></a>
  <a href="https://pypi.org/project/deltafunnel/"><img alt="Python 3.10+" src="https://img.shields.io/badge/python-3.10%2B-blue.svg"></a>
</p>

Project links: [Delta Funnel GitHub repository](https://github.com/mag1cfrog/delta-funnel),
[deltafunnel Python package on PyPI](https://pypi.org/project/deltafunnel/),
[delta-funnel Rust crate on crates.io](https://crates.io/crates/delta-funnel),
[delta-funnel Rust API documentation on docs.rs](https://docs.rs/delta-funnel),
and [Delta Funnel release notes](https://github.com/mag1cfrog/delta-funnel/releases).

!!! note "Project status"
    Delta Funnel is early project code. The Rust crate is available on
    crates.io, and the Python package is available on PyPI.

## Install

For Rust:

```bash
cargo add delta-funnel
```

For Python:

```bash
uv add deltafunnel
```

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
