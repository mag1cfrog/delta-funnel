//! Private scan planning models.

use std::collections::BTreeMap;

use snafu::ResultExt;

use crate::{
    DeltaReaderError,
    deletion_vector::DeletionVectorMetadata,
    error::ScanPlanningSnafu,
    kernel::{KernelPhysicalToLogicalTransform, KernelScanFileMetadata},
};

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
    use std::{collections::HashMap, sync::Arc};

    use delta_kernel::{
        actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType},
        expressions::{ColumnName, Expression},
        scan::state::{DvInfo, ScanFile, Stats},
    };

    use super::DeltaScanFileTask;
    use crate::{DeltaReaderPhase, kernel::KernelScanFileMetadata};

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
}
