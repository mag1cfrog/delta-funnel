//! Metadata-only grouping for Delta scan file tasks.

use crate::{DeltaFunnelError, error::DeltaScanFileTaskPartitionPlanningSnafu};

use super::file_task::DeltaScanFileTask;

/// Caller request for metadata-only Delta scan file task partition planning.
#[allow(dead_code)]
pub(crate) struct DeltaScanFileTaskPartitionPlanRequest {
    /// DataFusion table name for this source.
    pub(crate) source_name: String,
    /// Normalized Delta table URI for this source.
    pub(crate) table_uri: String,
    /// Resolved Delta snapshot version that selected the file tasks.
    pub(crate) snapshot_version: u64,
    /// Whether the upstream scan metadata iterator was consumed to completion.
    pub(crate) scan_metadata_exhausted: bool,
    /// Delta-aware file tasks selected for this provider scan.
    pub(crate) file_tasks: Vec<DeltaScanFileTask>,
    /// Partition grouping options for this provider scan.
    pub(crate) options: DeltaScanFileTaskPartitionOptions,
}

/// Metadata-only partition plan for grouped Delta scan file tasks.
#[allow(dead_code)]
pub(crate) struct DeltaScanFileTaskPartitionPlan {
    /// DataFusion table name for this source.
    pub(crate) source_name: String,
    /// Normalized Delta table URI used by the file tasks.
    pub(crate) table_uri: String,
    /// Resolved Delta snapshot version that selected the file tasks.
    pub(crate) snapshot_version: u64,
    /// Provider scan partitions, each containing one or more physical file tasks.
    pub(crate) partitions: Vec<DeltaScanFileTaskPartition>,
    /// Whether upstream scan metadata was consumed to completion.
    pub(crate) scan_metadata_exhausted: bool,
    /// Total estimated bytes when every input file task has a known byte estimate.
    pub(crate) estimated_bytes: Option<u64>,
    /// Total estimated rows when every input file task has a known row estimate.
    pub(crate) estimated_rows: Option<u64>,
}

/// One provider scan partition containing whole Delta file tasks.
#[allow(dead_code)]
pub(crate) struct DeltaScanFileTaskPartition {
    /// Whole physical Delta file tasks assigned to this partition in scan order.
    pub(crate) file_tasks: Vec<DeltaScanFileTask>,
    /// Partition byte estimate when every task in this partition has known bytes.
    pub(crate) estimated_bytes: Option<u64>,
    /// Partition row estimate when every task in this partition has known rows.
    pub(crate) estimated_rows: Option<u64>,
}

/// Metadata-only file task partition planning options.
#[allow(dead_code)]
pub(crate) struct DeltaScanFileTaskPartitionOptions {
    /// Desired upper bound for output partitions when enough file tasks exist.
    pub(crate) target_partitions: usize,
}

impl DeltaScanFileTaskPartitionPlan {
    /// Groups Delta-aware file tasks into metadata-only provider scan partitions.
    ///
    /// The policy is deterministic and never splits a physical file. When every
    /// file task has a byte estimate, partitions are formed in scan order around
    /// a target byte budget. If any byte estimate is unknown, planning falls back
    /// to deterministic file-count balancing.
    #[allow(dead_code)]
    pub(crate) fn try_new(
        request: DeltaScanFileTaskPartitionPlanRequest,
    ) -> Result<Self, DeltaFunnelError> {
        let DeltaScanFileTaskPartitionPlanRequest {
            source_name,
            table_uri,
            snapshot_version,
            scan_metadata_exhausted,
            file_tasks,
            options,
        } = request;

        validate_partition_options(&source_name, &table_uri, snapshot_version, &options)?;
        validate_file_task_context(&source_name, &table_uri, snapshot_version, &file_tasks)?;

        let estimated_bytes = sum_task_estimate(
            &source_name,
            &table_uri,
            snapshot_version,
            "estimated bytes",
            file_tasks.iter().map(|file_task| file_task.estimated_bytes),
        )?;
        let estimated_rows = sum_task_estimate(
            &source_name,
            &table_uri,
            snapshot_version,
            "estimated rows",
            file_tasks.iter().map(|file_task| file_task.estimated_rows),
        )?;
        let partitions = if file_tasks.is_empty() {
            Vec::new()
        } else if estimated_bytes.is_some() {
            group_by_estimated_bytes(
                &source_name,
                &table_uri,
                snapshot_version,
                file_tasks,
                options.target_partitions,
            )?
        } else {
            group_by_file_count(
                &source_name,
                &table_uri,
                snapshot_version,
                file_tasks,
                options.target_partitions,
            )?
        };

        Ok(Self {
            source_name,
            table_uri,
            snapshot_version,
            partitions,
            scan_metadata_exhausted,
            estimated_bytes,
            estimated_rows,
        })
    }
}

/// Validates partition options before metadata expansion or grouping work.
///
/// The target partition count is the main caller-controlled scheduling input,
/// so zero is rejected before any scan planning state is consumed.
fn validate_partition_options(
    source_name: &str,
    table_uri: &str,
    snapshot_version: u64,
    options: &DeltaScanFileTaskPartitionOptions,
) -> Result<(), DeltaFunnelError> {
    if options.target_partitions == 0 {
        return Err(partition_planning_error(
            source_name,
            table_uri,
            snapshot_version,
            "target_partitions must be greater than zero",
        ));
    }

    Ok(())
}

/// Verifies that all file tasks belong to the same provider scan context.
///
/// Partition planning consumes tasks as one scan unit, so mixed sources,
/// tables, or snapshot versions would make later execution diagnostics and
/// recovery ambiguous.
fn validate_file_task_context(
    source_name: &str,
    table_uri: &str,
    snapshot_version: u64,
    file_tasks: &[DeltaScanFileTask],
) -> Result<(), DeltaFunnelError> {
    for file_task in file_tasks {
        if file_task.source_name != source_name {
            return Err(partition_planning_error(
                source_name,
                table_uri,
                snapshot_version,
                format!(
                    "file task `{}` belongs to source `{}`, not `{source_name}`",
                    file_task.path, file_task.source_name
                ),
            ));
        }
        if file_task.table_uri != table_uri {
            return Err(partition_planning_error(
                source_name,
                table_uri,
                snapshot_version,
                format!(
                    "file task `{}` belongs to table URI `{}`, not `{table_uri}`",
                    file_task.path, file_task.table_uri
                ),
            ));
        }
        if file_task.snapshot_version != snapshot_version {
            return Err(partition_planning_error(
                source_name,
                table_uri,
                snapshot_version,
                format!(
                    "file task `{}` belongs to snapshot version {}, not {snapshot_version}",
                    file_task.path, file_task.snapshot_version
                ),
            ));
        }
    }

    Ok(())
}

/// Groups known-size file tasks by a target byte budget without splitting files.
///
/// The policy preserves scan order and emits at most `target_partitions`
/// non-empty partitions. A single oversized physical file stays whole and may
/// exceed the computed target bytes for its partition.
fn group_by_estimated_bytes(
    source_name: &str,
    table_uri: &str,
    snapshot_version: u64,
    file_tasks: Vec<DeltaScanFileTask>,
    target_partitions: usize,
) -> Result<Vec<DeltaScanFileTaskPartition>, DeltaFunnelError> {
    let output_limit = target_partitions.min(file_tasks.len());
    let total_bytes = sum_task_estimate(
        source_name,
        table_uri,
        snapshot_version,
        "estimated bytes",
        file_tasks.iter().map(|file_task| file_task.estimated_bytes),
    )?
    .ok_or_else(|| {
        partition_planning_error(
            source_name,
            table_uri,
            snapshot_version,
            "known-size grouping requires every file task to have estimated bytes",
        )
    })?;
    let target_bytes = total_bytes.div_ceil(output_limit as u64);
    let mut partitions = Vec::new();
    let mut current_file_tasks = Vec::new();
    let mut current_bytes = 0_u64;

    for file_task in file_tasks {
        let file_bytes = file_task.estimated_bytes.ok_or_else(|| {
            partition_planning_error(
                source_name,
                table_uri,
                snapshot_version,
                "known-size grouping requires every file task to have estimated bytes",
            )
        })?;
        // Keep scan order stable and only start a new partition before adding a
        // file that would exceed the byte budget. Large files stay whole, so a
        // single oversized file may exceed the target by itself.
        let can_start_next_partition = !current_file_tasks.is_empty()
            && partitions.len() + 1 < output_limit
            && current_bytes.saturating_add(file_bytes) > target_bytes;

        if can_start_next_partition {
            partitions.push(build_partition(
                source_name,
                table_uri,
                snapshot_version,
                current_file_tasks,
            )?);
            current_file_tasks = Vec::new();
            current_bytes = 0;
        }

        current_bytes = current_bytes.checked_add(file_bytes).ok_or_else(|| {
            partition_planning_error(
                source_name,
                table_uri,
                snapshot_version,
                "partition estimated bytes overflowed u64",
            )
        })?;
        current_file_tasks.push(file_task);
    }

    if !current_file_tasks.is_empty() {
        partitions.push(build_partition(
            source_name,
            table_uri,
            snapshot_version,
            current_file_tasks,
        )?);
    }

    Ok(partitions)
}

/// Groups unknown-size file tasks by deterministic file-count balancing.
///
/// This fallback is used when any input task lacks a byte estimate. It preserves
/// scan order, emits at most `target_partitions` non-empty partitions, and does
/// not use row estimates to invent byte estimates.
fn group_by_file_count(
    source_name: &str,
    table_uri: &str,
    snapshot_version: u64,
    file_tasks: Vec<DeltaScanFileTask>,
    target_partitions: usize,
) -> Result<Vec<DeltaScanFileTaskPartition>, DeltaFunnelError> {
    let output_limit = target_partitions.min(file_tasks.len());
    let mut partitions = Vec::new();
    let mut file_tasks = file_tasks.into_iter();
    let mut remaining_files = file_tasks.len();

    for partition_index in 0..output_limit {
        let remaining_partitions = output_limit - partition_index;
        // Unknown-size tasks use deterministic file-count balancing. Earlier
        // partitions get the extra file when the count does not divide evenly.
        let take_count = remaining_files.div_ceil(remaining_partitions);
        let mut partition_file_tasks = Vec::with_capacity(take_count);

        for _ in 0..take_count {
            let Some(file_task) = file_tasks.next() else {
                return Err(partition_planning_error(
                    source_name,
                    table_uri,
                    snapshot_version,
                    "file-count grouping exhausted file tasks unexpectedly",
                ));
            };
            partition_file_tasks.push(file_task);
        }

        remaining_files -= take_count;
        partitions.push(build_partition(
            source_name,
            table_uri,
            snapshot_version,
            partition_file_tasks,
        )?);
    }

    Ok(partitions)
}

/// Builds one partition and derives aggregate estimates from its file tasks.
///
/// Aggregate estimates remain unknown when any task in the partition has an
/// unknown value for that estimate.
fn build_partition(
    source_name: &str,
    table_uri: &str,
    snapshot_version: u64,
    file_tasks: Vec<DeltaScanFileTask>,
) -> Result<DeltaScanFileTaskPartition, DeltaFunnelError> {
    let estimated_bytes = sum_task_estimate(
        source_name,
        table_uri,
        snapshot_version,
        "estimated bytes",
        file_tasks.iter().map(|file_task| file_task.estimated_bytes),
    )?;
    let estimated_rows = sum_task_estimate(
        source_name,
        table_uri,
        snapshot_version,
        "estimated rows",
        file_tasks.iter().map(|file_task| file_task.estimated_rows),
    )?;

    Ok(DeltaScanFileTaskPartition {
        file_tasks,
        estimated_bytes,
        estimated_rows,
    })
}

/// Sums an optional per-task estimate without inventing missing values.
///
/// Returns `None` as soon as any input estimate is unknown, and reports a
/// partition-planning error if known estimates overflow `u64`.
fn sum_task_estimate(
    source_name: &str,
    table_uri: &str,
    snapshot_version: u64,
    estimate_name: &str,
    estimates: impl IntoIterator<Item = Option<u64>>,
) -> Result<Option<u64>, DeltaFunnelError> {
    let mut total = 0_u64;

    for estimate in estimates {
        let Some(estimate) = estimate else {
            return Ok(None);
        };
        total = total.checked_add(estimate).ok_or_else(|| {
            partition_planning_error(
                source_name,
                table_uri,
                snapshot_version,
                format!("{estimate_name} overflowed u64"),
            )
        })?;
    }

    Ok(Some(total))
}

/// Creates the SNAFU-backed partition-planning error for this module.
fn partition_planning_error(
    source_name: &str,
    table_uri: &str,
    snapshot_version: u64,
    reason: impl Into<String>,
) -> DeltaFunnelError {
    DeltaScanFileTaskPartitionPlanningSnafu {
        source_name: source_name.to_owned(),
        table_uri: table_uri.to_owned(),
        snapshot_version,
        reason: reason.into(),
    }
    .build()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::table_formats::{
        KernelPhysicalToLogicalTransform, KernelScanDeletionVectorMetadata, KernelScanFileMetadata,
        KernelScanFileStats,
    };

    use super::*;

    fn plan_request(
        file_tasks: Vec<DeltaScanFileTask>,
        target_partitions: usize,
    ) -> DeltaScanFileTaskPartitionPlanRequest {
        DeltaScanFileTaskPartitionPlanRequest {
            source_name: "orders".to_owned(),
            table_uri: "file:///tmp/table".to_owned(),
            snapshot_version: 42,
            scan_metadata_exhausted: true,
            file_tasks,
            options: DeltaScanFileTaskPartitionOptions { target_partitions },
        }
    }

    fn file_task(
        path: &str,
        estimated_bytes: Option<u64>,
        estimated_rows: Option<u64>,
    ) -> Result<DeltaScanFileTask, DeltaFunnelError> {
        let mut task = DeltaScanFileTask::from_kernel_metadata(
            "orders",
            "file:///tmp/table",
            42,
            KernelScanFileMetadata {
                path: path.to_owned(),
                size: i64::try_from(estimated_bytes.unwrap_or(10)).map_err(|_| {
                    partition_planning_error(
                        "orders",
                        "file:///tmp/table",
                        42,
                        "test file size does not fit i64",
                    )
                })?,
                modification_time: 1587968586000,
                stats: Some(KernelScanFileStats {
                    num_records: estimated_rows.unwrap_or(1),
                }),
                deletion_vector: KernelScanDeletionVectorMetadata::NotPresent,
                physical_to_logical_transform: KernelPhysicalToLogicalTransform::NotRequired,
                partition_values: HashMap::from([("region".to_owned(), "us-west".to_owned())]),
            },
        )?;
        task.estimated_bytes = estimated_bytes;
        task.estimated_rows = estimated_rows;
        if estimated_rows.is_none() {
            task.stats = None;
        }

        Ok(task)
    }

    fn plan_partition_paths(plan: &DeltaScanFileTaskPartitionPlan) -> Vec<Vec<&str>> {
        plan.partitions
            .iter()
            .map(|partition| {
                partition
                    .file_tasks
                    .iter()
                    .map(|file_task| file_task.path.as_str())
                    .collect()
            })
            .collect()
    }

    #[test]
    fn partition_plan_rejects_zero_target_partitions() -> Result<(), Box<dyn std::error::Error>> {
        let error = match DeltaScanFileTaskPartitionPlan::try_new(plan_request(Vec::new(), 0)) {
            Ok(_) => return Err("expected target_partitions validation to fail".into()),
            Err(error) => error,
        };

        assert!(error.to_string().contains("target_partitions"));

        Ok(())
    }

    #[test]
    fn partition_plan_rejects_mismatched_file_task_context()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut mismatched_task = file_task("part-0.parquet", Some(10), Some(1))?;
        mismatched_task.snapshot_version = 43;

        let error =
            match DeltaScanFileTaskPartitionPlan::try_new(plan_request(vec![mismatched_task], 1)) {
                Ok(_) => return Err("expected file task context validation to fail".into()),
                Err(error) => error,
            };

        assert!(error.to_string().contains("snapshot version"));
        assert!(error.to_string().contains("part-0.parquet"));

        Ok(())
    }

    #[test]
    fn empty_file_task_list_returns_empty_partition_plan() -> Result<(), Box<dyn std::error::Error>>
    {
        let plan = DeltaScanFileTaskPartitionPlan::try_new(plan_request(Vec::new(), 4))?;

        assert_eq!(plan.source_name, "orders");
        assert_eq!(plan.table_uri, "file:///tmp/table");
        assert_eq!(plan.snapshot_version, 42);
        assert!(plan.scan_metadata_exhausted);
        assert!(plan.partitions.is_empty());
        assert_eq!(plan.estimated_bytes, Some(0));
        assert_eq!(plan.estimated_rows, Some(0));

        Ok(())
    }

    #[test]
    fn known_size_files_group_by_estimated_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let plan = DeltaScanFileTaskPartitionPlan::try_new(plan_request(
            vec![
                file_task("large.parquet", Some(90), Some(9))?,
                file_task("small-1.parquet", Some(10), Some(1))?,
                file_task("small-2.parquet", Some(10), Some(1))?,
                file_task("small-3.parquet", Some(10), Some(1))?,
            ],
            2,
        ))?;

        assert_eq!(
            plan_partition_paths(&plan),
            vec![
                vec!["large.parquet"],
                vec!["small-1.parquet", "small-2.parquet", "small-3.parquet"],
            ]
        );
        assert_eq!(
            plan.partitions
                .iter()
                .map(|partition| partition.estimated_bytes)
                .collect::<Vec<_>>(),
            vec![Some(90), Some(30)]
        );
        assert_eq!(plan.estimated_bytes, Some(120));
        assert_eq!(plan.estimated_rows, Some(12));

        Ok(())
    }

    #[test]
    fn known_size_grouping_can_emit_fewer_partitions_than_requested()
    -> Result<(), Box<dyn std::error::Error>> {
        let plan = DeltaScanFileTaskPartitionPlan::try_new(plan_request(
            vec![
                file_task("part-0.parquet", Some(10), Some(1))?,
                file_task("part-1.parquet", Some(10), Some(1))?,
            ],
            8,
        ))?;

        assert_eq!(plan.partitions.len(), 2);
        assert_eq!(
            plan_partition_paths(&plan),
            vec![vec!["part-0.parquet"], vec!["part-1.parquet"]]
        );

        Ok(())
    }

    #[test]
    fn unknown_size_files_fallback_to_file_count_balancing()
    -> Result<(), Box<dyn std::error::Error>> {
        let plan = DeltaScanFileTaskPartitionPlan::try_new(plan_request(
            vec![
                file_task("part-0.parquet", None, Some(1))?,
                file_task("part-1.parquet", Some(10), Some(1))?,
                file_task("part-2.parquet", Some(10), Some(1))?,
                file_task("part-3.parquet", Some(10), Some(1))?,
                file_task("part-4.parquet", Some(10), Some(1))?,
            ],
            2,
        ))?;

        assert_eq!(
            plan_partition_paths(&plan),
            vec![
                vec!["part-0.parquet", "part-1.parquet", "part-2.parquet"],
                vec!["part-3.parquet", "part-4.parquet"],
            ]
        );
        assert_eq!(plan.estimated_bytes, None);
        assert_eq!(plan.partitions[0].estimated_bytes, None);
        assert_eq!(plan.partitions[1].estimated_bytes, Some(20));

        Ok(())
    }

    #[test]
    fn zero_byte_files_are_preserved() -> Result<(), Box<dyn std::error::Error>> {
        let plan = DeltaScanFileTaskPartitionPlan::try_new(plan_request(
            vec![
                file_task("zero-0.parquet", Some(0), Some(0))?,
                file_task("zero-1.parquet", Some(0), Some(0))?,
            ],
            4,
        ))?;

        assert_eq!(
            plan_partition_paths(&plan),
            vec![vec!["zero-0.parquet", "zero-1.parquet"]]
        );
        assert_eq!(plan.estimated_bytes, Some(0));
        assert_eq!(plan.estimated_rows, Some(0));

        Ok(())
    }

    #[test]
    fn each_input_file_task_appears_exactly_once() -> Result<(), Box<dyn std::error::Error>> {
        let plan = DeltaScanFileTaskPartitionPlan::try_new(plan_request(
            vec![
                file_task("part-0.parquet", Some(10), Some(1))?,
                file_task("part-1.parquet", Some(10), Some(1))?,
                file_task("part-2.parquet", Some(10), Some(1))?,
                file_task("part-3.parquet", Some(10), Some(1))?,
            ],
            2,
        ))?;
        let flattened_paths = plan
            .partitions
            .iter()
            .flat_map(|partition| partition.file_tasks.iter())
            .map(|file_task| file_task.path.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            flattened_paths,
            vec![
                "part-0.parquet",
                "part-1.parquet",
                "part-2.parquet",
                "part-3.parquet",
            ]
        );

        Ok(())
    }

    #[test]
    fn unknown_rows_keep_row_estimates_unknown() -> Result<(), Box<dyn std::error::Error>> {
        let plan = DeltaScanFileTaskPartitionPlan::try_new(plan_request(
            vec![
                file_task("part-0.parquet", Some(10), None)?,
                file_task("part-1.parquet", Some(10), Some(1))?,
            ],
            1,
        ))?;

        assert_eq!(plan.estimated_rows, None);
        assert_eq!(plan.partitions[0].estimated_rows, None);

        Ok(())
    }
}
