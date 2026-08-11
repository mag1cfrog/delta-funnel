# DataFusion and Delta Internals

Delta Funnel uses DataFusion for SQL planning and execution. The published
[`delta-arrow-reader`](https://docs.rs/delta-arrow-reader/0.1.2/delta_arrow_reader/)
crate owns Delta metadata loading, scan planning, file reads, transforms,
deletion vectors, and the DataFusion table provider.

Delta Funnel supplies source and execution policy to that provider, then owns
the surrounding session, workflow, report, progress, profiling, SQL Server,
and Python integration. It does not contain a second Delta reader.

## Delta scan planning

The standalone provider turns active Delta files into bounded DataFusion scan
work. Delta Funnel exposes the relevant source options and records the
resulting diagnostics.

- [Scan partition target policy](scan-partition-planning.md)

## Read scheduling and dynamic pruning

The standalone provider owns bounded reads, cancellation, transforms,
deletion-vector masking, and dynamic partition pruning. Delta Funnel consumes
its metrics in product reports and execution profiles.

- [Delta provider read scheduling](provider-read-scheduling.md)
