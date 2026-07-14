# DataFusion and Delta Internals

Delta Funnel uses DataFusion for SQL planning and execution, and Delta Kernel
for Delta Lake metadata and protocol handling.

The Delta provider connects those systems. It turns Delta scan metadata into
bounded DataFusion work, applies Delta transforms and deletion vectors, and
records read statistics for reports and progress displays.

## Delta scan planning

Delta scan planning turns active Delta files into DataFusion scan work. The
provider decides how many scan partitions to request before Delta scan metadata
is expanded.

- [Scan partition target policy](scan-partition-planning.md)

## Read scheduling and dynamic pruning

Provider reads are bounded so one scan cannot create unbounded file-read work.
The native async backend applies Delta transforms and deletion-vector masks
before rows reach DataFusion. Dynamic partition pruning can skip selected files
before that work starts.

- [Delta provider read scheduling](provider-read-scheduling.md)
