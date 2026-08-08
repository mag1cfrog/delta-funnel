//! Private physical-to-logical transform service.

use arrow::record_batch::RecordBatch;
use snafu::ResultExt;

use crate::{
    DeltaReaderError,
    error::PhysicalToLogicalTransformSnafu,
    planning::{DeltaScanFileTask, DeltaScanPlan},
};

#[allow(dead_code)]
impl DeltaScanPlan {
    pub(crate) fn apply_transform(
        &self,
        task: &DeltaScanFileTask,
        batch: RecordBatch,
    ) -> Result<RecordBatch, DeltaReaderError> {
        let physical_rows = batch.num_rows();
        let batch = task
            .transform
            .apply(&self.engine_context, &self.kernel_schemas, batch)
            .boxed()
            .context(PhysicalToLogicalTransformSnafu {
                reason: "kernel_transform_failed",
            })?;

        if batch.num_rows() != physical_rows
            || batch.schema().as_ref() != self.logical_schema.as_ref()
        {
            return Err(delta_kernel::Error::generic("transform_output_mismatch"))
                .boxed()
                .context(PhysicalToLogicalTransformSnafu {
                    reason: "transform_output_mismatch",
                });
        }

        Ok(batch)
    }
}
