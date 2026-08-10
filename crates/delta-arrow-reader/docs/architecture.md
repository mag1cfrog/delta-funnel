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

Through #464, this crate owns reader configuration, errors, metrics, immutable
snapshot/protocol/schema loading, deletion-vector handling, exact logical
predicates, scan metadata and transforms, deterministic partition planning,
backend-neutral bounded scheduling, and the private NativeAsync Parquet file
executor. NativeAsync reuses the snapshot engine's object store, performs
projected async range reads or threshold-controlled per-file buffering, and
hands ordered logical batches to the scheduler without creating a runtime,
store, limiter, queue, or public stream.

The crate does not yet contain an OfficialKernel file reader, a public
table-loading or scan-stream API, DataFusion integration, production routing,
or a compatibility facade. Later
[#447-family issues](https://github.com/mag1cfrog/delta-funnel/issues/447) own
those remaining boundaries.
