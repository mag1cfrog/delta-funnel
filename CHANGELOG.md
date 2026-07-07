# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0](https://github.com/mag1cfrog/delta-funnel/compare/delta-funnel-v0.1.6...delta-funnel-v0.2.0) - 2026-07-07

### Added

- replace SQL Server write options with direct backend selection

### Fixed

- match SQL Server datetime rounding when writing timestamp columns as datetime
- support non-nullable Timestamp(ns) columns mapped to SQL Server datetime
- keep partition filters out of native row predicates ([#413](https://github.com/mag1cfrog/delta-funnel/pull/413))

### Other

- update arrow-tiberius adapter to 0.2.0 ([#416](https://github.com/mag1cfrog/delta-funnel/pull/416))

## [0.1.6](https://github.com/mag1cfrog/delta-funnel/compare/delta-funnel-v0.1.5...delta-funnel-v0.1.6) - 2026-07-04

### Fixed

- expose SQL Server timestamp policy

## [0.1.5](https://github.com/mag1cfrog/delta-funnel/compare/delta-funnel-v0.1.4...delta-funnel-v0.1.5) - 2026-07-03

### Fixed

- allow SQL Server replace writes to create missing targets

## [0.1.4](https://github.com/mag1cfrog/delta-funnel/compare/delta-funnel-v0.1.3...delta-funnel-v0.1.4) - 2026-07-03

### Fixed

- accept Utf8View SQL Server string batches

## [0.1.3](https://github.com/mag1cfrog/delta-funnel/compare/delta-funnel-v0.1.2...delta-funnel-v0.1.3) - 2026-07-03

### Fixed

- expose SQL Server batch validation diagnostics in Python errors and reports

## [0.1.2](https://github.com/mag1cfrog/delta-funnel/compare/delta-funnel-v0.1.1...delta-funnel-v0.1.2) - 2026-07-02

### Added

- load AWS env for S3 sources ([#393](https://github.com/mag1cfrog/delta-funnel/pull/393))
- implement SQL Server replace mode

### Fixed

- add S3 credential diagnostics ([#389](https://github.com/mag1cfrog/delta-funnel/pull/389))
- support native async leaf casts

### Other

- lock down S3 storage options contract ([#385](https://github.com/mag1cfrog/delta-funnel/pull/385))
- clarify Python S3 credential contract ([#384](https://github.com/mag1cfrog/delta-funnel/pull/384))
- Seo/search indexing ([#376](https://github.com/mag1cfrog/delta-funnel/pull/376))

## [0.1.1](https://github.com/mag1cfrog/delta-funnel/compare/v0.1.0...v0.1.1) - 2026-07-02

### Added

- bridge python logging to tracing

### Fixed

- preserve delta snapshot load cause
- avoid python attach panic in logging bridge
- expose snapshot load cause to python

### Other

- polish public docs landing page ([#359](https://github.com/mag1cfrog/delta-funnel/pull/359))
- isolate python logging bridge global test
- harden python logging bridge
