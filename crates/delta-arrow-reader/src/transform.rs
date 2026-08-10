//! Private physical-to-logical transform service.

use std::sync::Arc;

use arrow::{compute::cast, datatypes::SchemaRef, record_batch::RecordBatch};
use snafu::ResultExt;

use crate::{
    DeltaReaderError,
    error::{DataFileReadSnafu, PhysicalToLogicalTransformSnafu},
    planning::{DeltaScanFileTask, DeltaScanPlan},
};

pub(crate) fn align_batch_to_logical_schema(
    batch: RecordBatch,
    logical_schema: &SchemaRef,
    mismatch_message: &'static str,
) -> Result<RecordBatch, DeltaReaderError> {
    if batch.schema().as_ref() == logical_schema.as_ref() {
        return Ok(batch);
    }
    let compatible = batch.num_columns() == logical_schema.fields().len()
        && batch
            .schema()
            .fields()
            .iter()
            .zip(logical_schema.fields())
            .all(|(actual, expected)| {
                actual.name() == expected.name()
                    && actual.is_nullable() == expected.is_nullable()
                    && actual.data_type().equals_datatype(expected.data_type())
            });
    if !compatible {
        return Err(delta_kernel::Error::generic(mismatch_message))
            .boxed()
            .context(DataFileReadSnafu {
                reason: "backend_logical_schema_mismatch",
            });
    }
    let columns = batch
        .columns()
        .iter()
        .zip(logical_schema.fields())
        .map(|(column, field)| {
            if column.data_type() == field.data_type() {
                Ok(Arc::clone(column))
            } else {
                cast(column.as_ref(), field.data_type())
            }
        })
        .collect::<Result<Vec<_>, _>>()
        .boxed()
        .context(DataFileReadSnafu {
            reason: "backend_logical_schema_mismatch",
        })?;
    RecordBatch::try_new(Arc::clone(logical_schema), columns)
        .boxed()
        .context(DataFileReadSnafu {
            reason: "backend_logical_schema_mismatch",
        })
}

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
