//! Private scan planning models.

use std::collections::{BTreeMap, HashSet};

use arrow::datatypes::Schema;
use snafu::ResultExt;

use crate::{
    DeltaReaderError,
    deletion_vector::DeletionVectorMetadata,
    error::{InvalidProjectionSnafu, ScanPlanningSnafu},
    kernel::{
        DeltaKernelPredicate, KernelPhysicalToLogicalTransform, KernelScan, KernelScanFileMetadata,
    },
    snapshot::LoadedDeltaTableSnapshot,
};

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
pub(crate) fn plan_file_tasks(
    snapshot: &LoadedDeltaTableSnapshot,
    scan: &KernelScan,
) -> Result<Vec<DeltaScanFileTask>, DeltaReaderError> {
    scan.file_metadata(snapshot.engine_context())
        .boxed()
        .context(ScanPlanningSnafu {
            reason: "kernel_scan_metadata_failed",
        })?
        .into_iter()
        .map(DeltaScanFileTask::try_from_kernel)
        .collect()
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

    use super::{DeltaScanFileTask, build_scan, plan_file_tasks};
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
        let stats = rows.map_or_else(String::new, |rows| {
            format!(r#", "stats":"{{\"numRecords\":{rows}}}""#)
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
        let empty_scan = build_scan(&empty_snapshot, None, None, true)?;
        assert!(plan_file_tasks(&empty_snapshot, &empty_scan)?.is_empty());

        let single_add = [add("single.parquet", 0, None)];
        let (_single_table, single_snapshot) =
            loaded_snapshot_with_adds("single-file", &single_add)?;
        let single_scan = build_scan(&single_snapshot, None, None, true)?;
        let single = plan_file_tasks(&single_snapshot, &single_scan)?;
        assert_eq!(single.len(), 1);
        assert_eq!(single[0].path, "single.parquet");
        assert_eq!(single[0].estimated_bytes, Some(0));
        assert_eq!(single[0].estimated_rows, None);

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
        let many_scan = build_scan(&many_snapshot, None, None, true)?;
        let many = plan_file_tasks(&many_snapshot, &many_scan)?;

        assert_eq!(many.len(), adds.len());
        assert_eq!(many[0].path, "part-0000.parquet");
        assert_eq!(many[1].estimated_rows, None);
        assert_eq!(many[1_000].path, "part-1000.parquet");
        assert_eq!(many[1_000].estimated_bytes, Some(1_000));
        assert_eq!(many[1_000].estimated_rows, Some(1_000));

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
        let scan = build_scan(&snapshot, None, None, true)?;

        let error = match plan_file_tasks(&snapshot, &scan) {
            Ok(_) => return Err("invalid task must fail the plan".into()),
            Err(error) => error,
        };

        assert_eq!(error.as_str(), "scan_planning");
        assert!(!error.to_string().contains("secret-invalid"));

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

        Ok(())
    }
}
