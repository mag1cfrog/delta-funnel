# DataFusion And Delta Internals

Delta Funnel uses DataFusion for SQL planning and execution, and Delta Kernel
for Delta Lake metadata and protocol handling.

This page is the entry point for deeper implementation notes. The notes are
kept in the repository `docs/` directory because they are engineering records,
not introductory user guides.

## Delta scan planning

Delta scan planning turns active Delta files into DataFusion scan work. The
provider decides how many scan partitions to request before Delta scan metadata
is expanded.

- [Scan partition target policy](https://github.com/mag1cfrog/delta-funnel/blob/main/docs/scan-partition-target-policy.md)
- [Scan partition benchmark](https://github.com/mag1cfrog/delta-funnel/blob/main/docs/scan-partition-benchmark.md)

## Read scheduling

Provider reads are bounded so one scan cannot create unbounded file-read work.
The native async backend applies Delta transforms and deletion-vector masks
before rows reach DataFusion.

- [Delta provider read scheduling](https://github.com/mag1cfrog/delta-funnel/blob/main/docs/provider-read-scheduling.md)
- [Native async backend benchmark notes](https://github.com/mag1cfrog/delta-funnel/blob/main/docs/native-async-backend-benchmark-notes.md)

## Dynamic partition pruning

Dynamic partition pruning is tracked separately because it crosses DataFusion
physical optimization, Delta scan metadata, provider scheduling, metrics, and
tests.

- [Dynamic partition pruning investigation](https://github.com/mag1cfrog/delta-funnel/blob/main/docs/dynamic-partition-pruning-investigation.md)

## Dependency boundaries

The first release keeps dependency alignment explicit around Arrow, DataFusion,
Delta Kernel, and SQL Server writes.

- [Dependency alignment](https://github.com/mag1cfrog/delta-funnel/blob/main/docs/dependency-alignment.md)
