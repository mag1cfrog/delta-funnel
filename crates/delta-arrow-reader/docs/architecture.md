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

Through #467, this crate owns reader configuration, errors, metrics, immutable
snapshot/protocol/schema loading, deletion-vector handling, exact logical
predicates, scan metadata and transforms, deterministic partition planning,
backend-neutral bounded scheduling, and the private NativeAsync and
OfficialKernel file executors. NativeAsync reuses the snapshot engine's object
store, performs projected async range reads or threshold-controlled per-file
buffering, and hands ordered logical batches to the scheduler without creating
a runtime, store, limiter, queue, or public stream. OfficialKernel reuses the
same engine, plan, transforms, DVs, limiter, scheduler, and metrics through one
bounded blocking handoff. It disables physical predicates for DV files that
lack original row indexes and leaves exact residual evaluation to the later
direct and DataFusion surfaces. Dropping a scan stops future scheduling and
closes the handoff deterministically, but already-running synchronous Kernel
dependency work can finish only at its next safe handoff boundary.

The public direct API composes those services into an immutable table handle,
a single-use scan plan, and a pull-driven Arrow batch stream. It applies exact
residual predicates, removes hidden predicate columns, enforces a global output
limit, merges partitions deterministically, and keeps scan metrics accessible
after stream drop.

The optional private DataFusion planning adapter preserves the frozen provider
policy: it validates projection indexes, rejects duplicates, expands physical
reads with accepted predicate columns, normalizes relation-qualified top-level
columns, and applies the existing partition and data-statistics filter matrices.
Inexact filters retain their complete DataFusion residual and require all
residual columns in the scan output. The adapter does not derive optimizer
statistics or apply DataFusion's advisory scan limit.

The crate does not yet contain a DataFusion provider or physical plan,
production routing, or a compatibility facade. Later
[#447-family issues](https://github.com/mag1cfrog/delta-funnel/issues/447) own
those remaining boundaries.
