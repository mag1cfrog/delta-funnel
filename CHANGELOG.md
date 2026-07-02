# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
