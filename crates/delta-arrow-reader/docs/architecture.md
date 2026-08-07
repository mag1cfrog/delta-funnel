# Architecture

## Purpose

This crate is a temporary validation boundary for extracting Delta Funnel's
read-only Delta Lake to Arrow implementation. It lets maintainers prove the
package, dependency, API, runtime, correctness, and performance boundaries
before creating the independent repository.

## Lifecycle

The extraction has three ownership phases:

1. Delta Funnel's internal reader on `main` remains production truth.
2. This crate receives extraction work only on
   `refactor/delta-arrow-reader-staging`.
3. [Issue #486](https://github.com/mag1cfrog/delta-funnel/issues/486) exports
   one frozen validated crate subtree and transfers canonical ownership.

The staging branch never merges into `main`, publishes a package, or becomes a
Delta Funnel release. Delta Funnel later adopts only the independently
published package.

## Current boundary

The #460 scaffold contains only package identity and version access. It adds no
reader behavior, dependencies, production routing, or compatibility facade.
Later [#447-family issues](https://github.com/mag1cfrog/delta-funnel/issues/447)
own those decisions.
