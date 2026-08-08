//! Private scan planning models.

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use arrow::datatypes::{Schema, SchemaRef};
use snafu::ResultExt;

use crate::{
    DeltaReaderError, DeltaReaderExecutionOptions,
    deletion_vector::DeletionVectorMetadata,
    error::{InvalidProjectionSnafu, ScanPlanningSnafu},
    kernel::{
        DeltaKernelEngineContext, DeltaKernelPredicate, KernelPhysicalToLogicalTransform,
        KernelScan, KernelScanFileMetadata,
    },
    snapshot::LoadedDeltaTableSnapshot,
};

#[allow(dead_code)]
pub(crate) struct DeltaScanPlan {
    pub(crate) snapshot_version: u64,
    pub(crate) engine_context: Arc<DeltaKernelEngineContext>,
    pub(crate) logical_schema: SchemaRef,
    pub(crate) physical_schema: SchemaRef,
    pub(crate) projected_schema: SchemaRef,
    pub(crate) file_tasks: Vec<DeltaScanFileTask>,
    pub(crate) scan_metadata_exhausted: bool,
    pub(crate) files_filtered_during_planning: Option<u64>,
    pub(crate) estimated_bytes: Option<u64>,
    pub(crate) estimated_rows: Option<u64>,
    pub(crate) physical_predicate: Option<DeltaKernelPredicate>,
    pub(crate) execution_options: DeltaReaderExecutionOptions,
}

#[allow(dead_code)]
pub(crate) fn build_scan(
    snapshot: &LoadedDeltaTableSnapshot,
    projection: Option<&[String]>,
    predicate: Option<DeltaKernelPredicate>,
    include_stats: bool,
) -> Result<KernelScan, DeltaReaderError> {
    validate_projection(snapshot.schema().as_ref(), projection)?;
    snapshot
        .kernel_snapshot()
        .build_scan(projection, predicate, include_stats)
        .boxed()
        .context(ScanPlanningSnafu {
            reason: "kernel_scan_build_failed",
        })
}

#[allow(dead_code)]
pub(crate) fn plan_scan(
    snapshot: &LoadedDeltaTableSnapshot,
    projection: Option<&[String]>,
    hidden_columns: &[String],
    kernel_predicate: Option<DeltaKernelPredicate>,
    include_stats: bool,
    execution_options: DeltaReaderExecutionOptions,
) -> Result<DeltaScanPlan, DeltaReaderError> {
    execution_options.validate()?;
    let logical_projection =
        logical_projection(snapshot.schema().as_ref(), projection, hidden_columns)?;
    let scan = build_scan(
        snapshot,
        logical_projection.as_deref(),
        kernel_predicate.clone(),
        include_stats,
    )?;
    let metadata = scan
        .file_metadata(snapshot.engine_context())
        .boxed()
        .context(ScanPlanningSnafu {
            reason: "kernel_scan_metadata_failed",
        })?;
    let file_tasks = metadata
        .files
        .into_iter()
        .map(DeltaScanFileTask::try_from_kernel)
        .collect::<Result<Vec<_>, _>>()?;
    let estimated_bytes = exact_sum(file_tasks.iter().map(|task| task.estimated_bytes));
    let estimated_rows = exact_sum(file_tasks.iter().map(|task| task.estimated_rows));
    let logical_schema = scan.logical_schema();
    let physical_predicate = scan.physical_predicate();
    let projected_schema = match projection {
        None => Arc::clone(&logical_schema),
        Some(names) => Arc::new(Schema::new_with_metadata(
            logical_schema.fields()[..names.len()].to_vec(),
            logical_schema.metadata().clone(),
        )),
    };

    Ok(DeltaScanPlan {
        snapshot_version: snapshot.version(),
        engine_context: Arc::clone(snapshot.engine_context()),
        logical_schema,
        physical_schema: scan.physical_schema(),
        projected_schema,
        file_tasks,
        scan_metadata_exhausted: true,
        files_filtered_during_planning: metadata.files_filtered_during_planning,
        estimated_bytes,
        estimated_rows,
        physical_predicate,
        execution_options,
    })
}

fn logical_projection(
    schema: &Schema,
    projection: Option<&[String]>,
    hidden_columns: &[String],
) -> Result<Option<Vec<String>>, DeltaReaderError> {
    validate_projection(schema, projection)?;
    for name in hidden_columns {
        if schema.index_of(name).is_err() {
            return InvalidProjectionSnafu {
                reason: "column_not_found",
            }
            .fail();
        }
    }

    Ok(projection.map(|projection| {
        let mut logical = projection.to_vec();
        for name in hidden_columns {
            if !logical.contains(name) {
                logical.push(name.clone());
            }
        }
        logical
    }))
}

fn exact_sum(estimates: impl IntoIterator<Item = Option<u64>>) -> Option<u64> {
    estimates
        .into_iter()
        .try_fold(0_u64, |total, value| total.checked_add(value?))
}

fn validate_projection(
    schema: &Schema,
    projection: Option<&[String]>,
) -> Result<(), DeltaReaderError> {
    let Some(projection) = projection else {
        return Ok(());
    };
    let mut seen = HashSet::with_capacity(projection.len());

    for name in projection {
        if !seen.insert(name) {
            return InvalidProjectionSnafu {
                reason: "duplicate_column",
            }
            .fail();
        }
        if schema.index_of(name).is_err() {
            return InvalidProjectionSnafu {
                reason: "column_not_found",
            }
            .fail();
        }
    }

    Ok(())
}

#[allow(dead_code)]
pub(crate) struct DeltaScanFileTask {
    pub(crate) path: String,
    pub(crate) estimated_bytes: Option<u64>,
    pub(crate) estimated_rows: Option<u64>,
    pub(crate) modification_time_ms: Option<i64>,
    pub(crate) partition_values: BTreeMap<String, String>,
    pub(crate) deletion_vector: DeletionVectorMetadata,
    pub(crate) transform: KernelPhysicalToLogicalTransform,
}

#[allow(dead_code)]
impl DeltaScanFileTask {
    pub(crate) fn try_from_kernel(file: KernelScanFileMetadata) -> Result<Self, DeltaReaderError> {
        let estimated_bytes = u64::try_from(file.size)
            .boxed()
            .context(ScanPlanningSnafu {
                reason: "negative_file_size",
            })?;

        Ok(Self {
            path: file.path,
            estimated_bytes: Some(estimated_bytes),
            estimated_rows: file.estimated_rows,
            modification_time_ms: file.modification_time_ms,
            partition_values: file.partition_values,
            deletion_vector: DeletionVectorMetadata::from_kernel(file.deletion_vector),
            transform: file.transform,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        fs,
        path::{Path, PathBuf},
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    use delta_kernel::{
        actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType},
        expressions::{ColumnName, Expression},
        scan::state::{DvInfo, ScanFile, Stats},
    };

    use super::{DeltaScanFileTask, build_scan, exact_sum, plan_scan};
    use crate::{
        DeltaComparison, DeltaPredicate, DeltaReaderPhase, DeltaScalar, DeltaSnapshotSelection,
        DeltaStorageOptions,
        kernel::{KernelScanFileMetadata, delta_predicate_to_kernel_pruning},
        predicate::validate_predicate,
        snapshot::load_delta_table_snapshot_blocking,
    };

    const PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
    const METADATA_JSON: &str = r#"{"metaData":{"id":"scan-planning-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"label\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"hidden\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;

    struct DeltaLogTable(PathBuf);

    impl DeltaLogTable {
        fn new_with_adds(name: &str, adds: &[String]) -> Result<Self, Box<dyn std::error::Error>> {
            let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
            let path = Path::new("target")
                .join("delta-arrow-reader-planning-tests")
                .join(format!("{}-{name}-{nanos}", std::process::id()));
            let log_path = path.join("_delta_log");
            fs::create_dir_all(&log_path)?;
            fs::write(
                log_path.join("00000000000000000000.json"),
                format!("{PROTOCOL_JSON}\n{METADATA_JSON}\n{}", adds.join("\n")),
            )?;
            Ok(Self(path))
        }
    }

    impl Drop for DeltaLogTable {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn loaded_snapshot(
        name: &str,
    ) -> Result<
        (DeltaLogTable, crate::snapshot::LoadedDeltaTableSnapshot),
        Box<dyn std::error::Error>,
    > {
        loaded_snapshot_with_adds(name, &[])
    }

    fn loaded_snapshot_with_adds(
        name: &str,
        adds: &[String],
    ) -> Result<
        (DeltaLogTable, crate::snapshot::LoadedDeltaTableSnapshot),
        Box<dyn std::error::Error>,
    > {
        let table = DeltaLogTable::new_with_adds(name, adds)?;
        let snapshot = load_delta_table_snapshot_blocking(
            &table.0.to_string_lossy(),
            &DeltaStorageOptions::new(),
            DeltaSnapshotSelection::Latest,
        )?;
        Ok((table, snapshot))
    }

    fn add(path: &str, size: i64, rows: Option<u64>) -> String {
        let stats = rows.map(|rows| format!(r#"{{"numRecords":{rows}}}"#));
        add_with_stats(path, size, stats.as_deref())
    }

    fn add_with_stats(path: &str, size: i64, stats: Option<&str>) -> String {
        let stats = stats.map_or_else(String::new, |stats| {
            format!(
                ",\"stats\":{}",
                serde_json::to_string(stats).expect("stats string is serializable")
            )
        });
        format!(
            r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":{size},"modificationTime":1587968586000,"dataChange":true{stats}}}}}"#
        )
    }

    fn field_names(schema: &arrow::datatypes::SchemaRef) -> Vec<&str> {
        schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect()
    }

    fn kernel_file(path: &str) -> ScanFile {
        ScanFile {
            path: path.to_owned(),
            size: 123,
            modification_time: 1_587_968_586_000,
            stats: Some(Stats { num_records: 7 }),
            dv_info: DvInfo::default(),
            transform: None,
            partition_values: HashMap::from([
                ("region".to_owned(), "us-west".to_owned()),
                ("day".to_owned(), "2026-06-11".to_owned()),
            ]),
        }
    }

    fn task(file: ScanFile) -> Result<DeltaScanFileTask, crate::DeltaReaderError> {
        DeltaScanFileTask::try_from_kernel(KernelScanFileMetadata::from_scan_file(file))
    }

    #[test]
    fn file_task_preserves_kernel_metadata_without_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut file = kernel_file("part-00000.parquet");
        file.dv_info = DeletionVectorDescriptor::try_new(
            DeletionVectorStorageType::Inline,
            "inline-payload",
            None,
            14,
            2,
        )?
        .into();
        file.transform = Some(Arc::new(Expression::Column(ColumnName::new([
            "physical_id",
        ]))));

        let task = task(file)?;

        assert_eq!(task.path, "part-00000.parquet");
        assert_eq!(task.estimated_bytes, Some(123));
        assert_eq!(task.estimated_rows, Some(7));
        assert_eq!(task.modification_time_ms, Some(1_587_968_586_000));
        assert_eq!(
            task.partition_values.into_iter().collect::<Vec<_>>(),
            [
                ("day".to_owned(), "2026-06-11".to_owned()),
                ("region".to_owned(), "us-west".to_owned()),
            ]
        );
        assert!(task.deletion_vector.is_present());
        assert!(task.transform.is_required());
        assert!(task.transform.into_inner().is_some());

        Ok(())
    }

    #[test]
    fn file_task_preserves_zero_and_missing_estimates() -> Result<(), Box<dyn std::error::Error>> {
        let mut file = kernel_file("empty.parquet");
        file.size = 0;
        file.stats = None;

        let task = task(file)?;

        assert_eq!(task.estimated_bytes, Some(0));
        assert_eq!(task.estimated_rows, None);
        assert!(!task.deletion_vector.is_present());
        assert!(!task.transform.is_required());

        Ok(())
    }

    #[test]
    fn file_task_rejects_negative_size_without_disclosing_path() {
        let mut file = kernel_file("secret-file.parquet");
        file.size = -1;

        let error = match task(file) {
            Ok(_) => panic!("negative size must fail"),
            Err(error) => error,
        };
        let display = error.to_string();

        assert_eq!(error.as_str(), "scan_planning");
        assert_eq!(error.phase(), DeltaReaderPhase::ScanPlanning);
        assert!(!display.contains("secret-file"));
        assert!(!format!("{error:?}").contains("secret-file"));
    }

    #[test]
    fn file_task_planning_exhausts_empty_single_and_multi_batch_scans()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_empty_table, empty_snapshot) = loaded_snapshot("empty-files")?;
        let empty = plan_scan(&empty_snapshot, None, &[], None, true, Default::default())?;
        assert!(empty.file_tasks.is_empty());
        assert_eq!(empty.estimated_bytes, Some(0));
        assert_eq!(empty.estimated_rows, Some(0));

        let single_add = [add("single.parquet", 0, None)];
        let (_single_table, single_snapshot) =
            loaded_snapshot_with_adds("single-file", &single_add)?;
        let single = plan_scan(&single_snapshot, None, &[], None, true, Default::default())?;
        assert_eq!(single.file_tasks.len(), 1);
        assert_eq!(single.file_tasks[0].path, "single.parquet");
        assert_eq!(single.file_tasks[0].estimated_bytes, Some(0));
        assert_eq!(single.estimated_bytes, Some(0));
        assert_eq!(single.estimated_rows, None);

        let adds = (0_u32..1_001)
            .map(|index| {
                add(
                    &format!("part-{index:04}.parquet"),
                    i64::from(index),
                    (index % 2 == 0).then_some(u64::from(index)),
                )
            })
            .collect::<Vec<_>>();
        let (_many_table, many_snapshot) = loaded_snapshot_with_adds("many-files", &adds)?;
        let projection = ["label".to_owned(), "id".to_owned()];
        let many = plan_scan(
            &many_snapshot,
            Some(&projection),
            &[],
            None,
            true,
            Default::default(),
        )?;

        assert_eq!(many.file_tasks.len(), adds.len());
        assert_eq!(many.file_tasks[0].path, "part-0000.parquet");
        assert_eq!(many.file_tasks[1].estimated_rows, None);
        assert_eq!(many.file_tasks[1_000].path, "part-1000.parquet");
        assert_eq!(many.file_tasks[1_000].estimated_bytes, Some(1_000));
        assert_eq!(many.file_tasks[1_000].estimated_rows, Some(1_000));
        assert!(many.scan_metadata_exhausted);
        assert_eq!(many.files_filtered_during_planning, Some(0));
        assert_eq!(many.estimated_bytes, Some(500_500));
        assert_eq!(many.estimated_rows, None);
        assert!(Arc::ptr_eq(
            &many.engine_context,
            many_snapshot.engine_context()
        ));
        assert_eq!(many.snapshot_version, many_snapshot.version());
        assert_eq!(field_names(&many.logical_schema), ["label", "id"]);
        assert_eq!(field_names(&many.physical_schema), ["label", "id"]);
        assert_eq!(field_names(&many.projected_schema), ["label", "id"]);
        assert!(many.physical_predicate.is_none());
        assert_eq!(many.execution_options, Default::default());

        Ok(())
    }

    #[test]
    fn file_task_planning_returns_no_partial_result() -> Result<(), Box<dyn std::error::Error>> {
        let adds = [
            add("first.parquet", 1, Some(1)),
            add("secret-invalid.parquet", -1, Some(1)),
            add("last.parquet", 1, Some(1)),
        ];
        let (_table, snapshot) = loaded_snapshot_with_adds("all-or-error", &adds)?;

        let error = match plan_scan(&snapshot, None, &[], None, true, Default::default()) {
            Ok(_) => return Err("invalid task must fail the plan".into()),
            Err(error) => error,
        };

        assert_eq!(error.as_str(), "scan_planning");
        assert!(!error.to_string().contains("secret-invalid"));

        Ok(())
    }

    #[test]
    fn aggregate_estimates_are_exact_or_unknown() {
        assert_eq!(exact_sum([]), Some(0));
        assert_eq!(exact_sum([Some(2), Some(3)]), Some(5));
        assert_eq!(exact_sum([Some(2), None, Some(3)]), None);
        assert_eq!(exact_sum([Some(u64::MAX), Some(1)]), None);
    }

    #[test]
    fn scan_plan_keeps_hidden_columns_and_applies_static_stats_pruning()
    -> Result<(), Box<dyn std::error::Error>> {
        let adds = [
            add_with_stats(
                "impossible.parquet",
                10,
                Some(
                    r#"{"numRecords":2,"minValues":{"hidden":1},"maxValues":{"hidden":10},"nullCount":{"hidden":0}}"#,
                ),
            ),
            add_with_stats(
                "possible.parquet",
                20,
                Some(
                    r#"{"numRecords":3,"minValues":{"hidden":101},"maxValues":{"hidden":200},"nullCount":{"hidden":0}}"#,
                ),
            ),
            add("missing-stats.parquet", 30, None),
        ];
        let (_table, snapshot) = loaded_snapshot_with_adds("stats-pruning", &adds)?;
        let predicate = DeltaPredicate::Compare {
            column: "hidden".to_owned(),
            op: DeltaComparison::Gt,
            value: DeltaScalar::Int32(100),
        };
        validate_predicate(&predicate, snapshot.schema().as_ref())?;
        let kernel_predicate =
            delta_predicate_to_kernel_pruning(&predicate).ok_or("expected Kernel predicate")?;
        let projection = ["label".to_owned()];
        let hidden = ["hidden".to_owned()];

        let plan = plan_scan(
            &snapshot,
            Some(&projection),
            &hidden,
            Some(kernel_predicate),
            true,
            Default::default(),
        )?;

        assert_eq!(field_names(&plan.logical_schema), ["label", "hidden"]);
        assert_eq!(field_names(&plan.physical_schema), ["label", "hidden"]);
        assert_eq!(field_names(&plan.projected_schema), ["label"]);
        assert_eq!(
            plan.file_tasks
                .iter()
                .map(|task| task.path.as_str())
                .collect::<Vec<_>>(),
            ["possible.parquet", "missing-stats.parquet"]
        );
        assert_eq!(plan.files_filtered_during_planning, Some(1));
        assert_eq!(plan.estimated_bytes, Some(50));
        assert_eq!(plan.estimated_rows, None);
        assert!(plan.physical_predicate.is_some());

        let empty_projection = Vec::new();
        let empty = plan_scan(
            &snapshot,
            Some(&empty_projection),
            &hidden,
            None,
            false,
            Default::default(),
        )?;
        assert_eq!(field_names(&empty.logical_schema), ["hidden"]);
        assert_eq!(field_names(&empty.physical_schema), ["hidden"]);
        assert!(empty.projected_schema.fields().is_empty());

        Ok(())
    }

    #[test]
    fn scan_preserves_full_ordered_and_empty_projections() -> Result<(), Box<dyn std::error::Error>>
    {
        let (_table, snapshot) = loaded_snapshot("projections")?;

        let full = build_scan(&snapshot, None, None, false)?;
        assert_eq!(
            field_names(&full.logical_schema()),
            ["id", "label", "hidden"]
        );
        assert_eq!(
            field_names(&full.physical_schema()),
            ["id", "label", "hidden"]
        );

        let ordered_names = ["label".to_owned(), "id".to_owned()];
        let ordered = build_scan(&snapshot, Some(&ordered_names), None, false)?;
        assert_eq!(field_names(&ordered.logical_schema()), ["label", "id"]);
        assert_eq!(field_names(&ordered.physical_schema()), ["label", "id"]);

        let empty_names = Vec::<String>::new();
        let empty = build_scan(&snapshot, Some(&empty_names), None, false)?;
        assert!(empty.logical_schema().fields().is_empty());
        assert!(empty.physical_schema().fields().is_empty());

        Ok(())
    }

    #[test]
    fn scan_keeps_metadata_predicate_out_of_projected_schemas()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, snapshot) = loaded_snapshot("hidden-predicate")?;
        let predicate = DeltaPredicate::Compare {
            column: "hidden".to_owned(),
            op: DeltaComparison::Gt,
            value: DeltaScalar::Int32(1),
        };
        validate_predicate(&predicate, snapshot.schema().as_ref())?;
        let kernel_predicate =
            delta_predicate_to_kernel_pruning(&predicate).ok_or("expected Kernel predicate")?;
        let projection = ["label".to_owned()];

        let scan = build_scan(&snapshot, Some(&projection), Some(kernel_predicate), false)?;

        assert_eq!(field_names(&scan.logical_schema()), ["label"]);
        assert_eq!(field_names(&scan.physical_schema()), ["label"]);
        assert!(scan.has_physical_predicate());

        Ok(())
    }

    #[test]
    fn scan_rejects_missing_and_duplicate_projections_without_disclosure()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, snapshot) = loaded_snapshot("invalid-projection")?;

        for projection in [
            vec!["secret-missing".to_owned()],
            vec!["id".to_owned(), "id".to_owned()],
        ] {
            let error = match build_scan(&snapshot, Some(&projection), None, false) {
                Ok(_) => return Err("invalid projection must fail".into()),
                Err(error) => error,
            };
            let display = error.to_string();

            assert_eq!(error.as_str(), "invalid_projection");
            assert_eq!(error.phase(), DeltaReaderPhase::ScanPlanning);
            assert!(!display.contains("secret-missing"));
            assert!(!format!("{error:?}").contains("secret-missing"));
        }

        let projection = ["id".to_owned()];
        let hidden = ["secret-hidden".to_owned()];
        let error = match plan_scan(
            &snapshot,
            Some(&projection),
            &hidden,
            None,
            false,
            Default::default(),
        ) {
            Ok(_) => return Err("invalid hidden column must fail".into()),
            Err(error) => error,
        };
        assert_eq!(error.as_str(), "invalid_projection");
        assert!(!error.to_string().contains("secret-hidden"));

        Ok(())
    }
}
