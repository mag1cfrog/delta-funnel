# DataFusion and Delta Internals

Delta Funnel uses DataFusion for SQL planning and execution. The published
[`delta-arrow-reader`](https://docs.rs/delta-arrow-reader/)
crate owns Delta metadata loading, scan planning, file reads, transforms,
deletion vectors, and the DataFusion table provider.

Delta Funnel supplies source and execution policy to that provider, then owns
the surrounding session, workflow, report, progress, profiling, SQL Server,
and Python integration. It does not contain a second Delta reader.

## Learn about the reader

The standalone reader documentation explains the complete read path:

- [Reader architecture](https://mag1cfrog.github.io/delta-arrow-reader/architecture/)
- [Scan planning](https://mag1cfrog.github.io/delta-arrow-reader/scan-planning/)
- [Read scheduling](https://mag1cfrog.github.io/delta-arrow-reader/read-scheduling/)

## Delta Funnel integration

These pages cover only the choices and observations that Delta Funnel adds:

- [Delta scan planning](scan-partition-planning.md)
- [Delta provider read scheduling](provider-read-scheduling.md)
