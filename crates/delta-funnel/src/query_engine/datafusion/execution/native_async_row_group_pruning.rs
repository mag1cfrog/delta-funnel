//! Native parquet row-group pruning for physical scan predicates.
//!
//! This module uses Delta Kernel's public data-skipping evaluator trait, but
//! owns the parquet footer stats adapter because Delta Kernel's built-in
//! row-group adapter is crate-private. The safety rule is conservative: if a
//! row group's stats are missing or cannot be converted to the expected Delta
//! scalar type, keep the row group.

use std::cmp::Ordering;
use std::collections::HashMap;

use chrono::{DateTime, Days};
use delta_kernel::kernel_predicates::{
    DataSkippingPredicateEvaluator, KernelPredicateEvaluator, KernelPredicateEvaluatorDefaults,
};
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::statistics::Statistics;
use parquet::schema::types::ColumnDescPtr;

use crate::table_formats::{
    KernelColumnName, KernelDataType, KernelDecimalData, KernelPrimitiveType, KernelScalar,
    KernelScanReadSchema,
};

/// Computes the row groups that cannot be eliminated by footer statistics.
///
/// `None` means there is no physical predicate to use for row-group pruning.
/// `Some(Vec::new())` means every row group was proven impossible and the
/// parquet reader should return no rows.
#[allow(dead_code)]
pub(crate) fn native_async_pruned_row_groups(
    metadata: &ParquetMetaData,
    read_schema: &KernelScanReadSchema,
) -> Option<Vec<usize>> {
    let predicate = read_schema.physical_predicate()?;

    Some(
        metadata
            .row_groups()
            .iter()
            .enumerate()
            .filter_map(|(ordinal, row_group)| {
                NativeAsyncRowGroupStats::new(row_group)
                    .may_contain_matching_rows(predicate)
                    .then_some(ordinal)
            })
            .collect(),
    )
}

struct NativeAsyncRowGroupStats<'a> {
    row_group: &'a RowGroupMetaData,
    field_indices: HashMap<KernelColumnName, usize>,
}

impl<'a> NativeAsyncRowGroupStats<'a> {
    fn new(row_group: &'a RowGroupMetaData) -> Self {
        Self {
            row_group,
            field_indices: row_group_field_indices(row_group.schema_descr().columns()),
        }
    }

    fn may_contain_matching_rows(&self, predicate: &delta_kernel::PredicateRef) -> bool {
        self.eval_sql_where(predicate) != Some(false)
    }

    fn stats(&self, column: &KernelColumnName) -> Option<Option<&Statistics>> {
        self.field_indices
            .get(column)
            .map(|index| self.row_group.column(*index).statistics())
    }

    fn min_stat(
        &self,
        column: &KernelColumnName,
        data_type: &KernelDataType,
    ) -> Option<KernelScalar> {
        stat_min_scalar(data_type, self.stats(column)??)
    }

    fn max_stat(
        &self,
        column: &KernelColumnName,
        data_type: &KernelDataType,
    ) -> Option<KernelScalar> {
        stat_max_scalar(data_type, self.stats(column)??)
    }

    fn null_count_stat(&self, column: &KernelColumnName) -> Option<i64> {
        self.stats(column)??
            .null_count_opt()
            .map(|value| value as i64)
    }

    fn row_count_stat(&self) -> i64 {
        self.row_group.num_rows()
    }
}

impl DataSkippingPredicateEvaluator for NativeAsyncRowGroupStats<'_> {
    type Output = bool;
    type ColumnStat = KernelScalar;

    fn get_min_stat(
        &self,
        col: &KernelColumnName,
        data_type: &KernelDataType,
    ) -> Option<KernelScalar> {
        self.min_stat(col, data_type)
    }

    fn get_max_stat(
        &self,
        col: &KernelColumnName,
        data_type: &KernelDataType,
    ) -> Option<KernelScalar> {
        self.max_stat(col, data_type)
    }

    fn get_nullcount_stat(&self, col: &KernelColumnName) -> Option<KernelScalar> {
        self.null_count_stat(col).map(KernelScalar::from)
    }

    fn get_rowcount_stat(&self) -> Option<KernelScalar> {
        Some(KernelScalar::from(self.row_count_stat()))
    }

    fn eval_partial_cmp(
        &self,
        ord: Ordering,
        col: KernelScalar,
        val: &KernelScalar,
        inverted: bool,
    ) -> Option<bool> {
        KernelPredicateEvaluatorDefaults::partial_cmp_scalars(ord, &col, val, inverted)
    }

    fn eval_pred_scalar(&self, val: &KernelScalar, inverted: bool) -> Option<bool> {
        KernelPredicateEvaluatorDefaults::eval_pred_scalar(val, inverted)
    }

    fn eval_pred_scalar_is_null(&self, val: &KernelScalar, inverted: bool) -> Option<bool> {
        KernelPredicateEvaluatorDefaults::eval_pred_scalar_is_null(val, inverted)
    }

    fn eval_pred_is_null(&self, col: &KernelColumnName, inverted: bool) -> Option<bool> {
        let safe_to_skip = match inverted {
            true => self.get_rowcount_stat()?,
            false => KernelScalar::from(0_i64),
        };
        Some(self.get_nullcount_stat(col)? != safe_to_skip)
    }

    fn eval_pred_binary_scalars(
        &self,
        op: delta_kernel::expressions::BinaryPredicateOp,
        left: &KernelScalar,
        right: &KernelScalar,
        inverted: bool,
    ) -> Option<bool> {
        KernelPredicateEvaluatorDefaults::eval_pred_binary_scalars(op, left, right, inverted)
    }

    fn eval_pred_opaque(
        &self,
        op: &delta_kernel::expressions::OpaquePredicateOpRef,
        exprs: &[delta_kernel::Expression],
        inverted: bool,
    ) -> Option<bool> {
        op.eval_as_data_skipping_predicate(self, exprs, inverted)
    }

    fn finish_eval_pred_junction(
        &self,
        op: delta_kernel::expressions::JunctionPredicateOp,
        preds: &mut dyn Iterator<Item = Option<bool>>,
        inverted: bool,
    ) -> Option<bool> {
        KernelPredicateEvaluatorDefaults::finish_eval_pred_junction(op, preds, inverted)
    }
}

fn row_group_field_indices(columns: &[ColumnDescPtr]) -> HashMap<KernelColumnName, usize> {
    columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| {
            let name = column.path().parts().first()?.as_str();
            Some((KernelColumnName::new([name]), index))
        })
        .collect()
}

fn stat_min_scalar(data_type: &KernelDataType, stats: &Statistics) -> Option<KernelScalar> {
    use KernelPrimitiveType::*;

    match (data_type.as_primitive_opt()?, stats) {
        (String, Statistics::ByteArray(values)) => values.min_opt()?.as_utf8().ok().map(Into::into),
        (String, Statistics::FixedLenByteArray(values)) => {
            values.min_opt()?.as_utf8().ok().map(Into::into)
        }
        (Long, Statistics::Int64(values)) => values.min_opt().map(Into::into),
        (Long, Statistics::Int32(values)) => values.min_opt().map(|value| (*value as i64).into()),
        (Integer, Statistics::Int32(values)) => values.min_opt().map(Into::into),
        (Short, Statistics::Int32(values)) => values.min_opt().map(|value| (*value as i16).into()),
        (Byte, Statistics::Int32(values)) => values.min_opt().map(|value| (*value as i8).into()),
        (Float, Statistics::Float(values)) => values.min_opt().map(Into::into),
        (Double, Statistics::Double(values)) => values.min_opt().map(Into::into),
        (Double, Statistics::Float(values)) => values.min_opt().map(|value| (*value as f64).into()),
        (Boolean, Statistics::Boolean(values)) => values.min_opt().map(Into::into),
        (Binary, Statistics::ByteArray(values)) => {
            values.min_opt().map(|value| value.data().into())
        }
        (Binary, Statistics::FixedLenByteArray(values)) => {
            values.min_opt().map(|value| value.data().into())
        }
        (Date, Statistics::Int32(values)) => {
            values.min_opt().map(|value| KernelScalar::Date(*value))
        }
        (Timestamp, Statistics::Int64(values)) => values
            .min_opt()
            .map(|value| KernelScalar::Timestamp(*value)),
        (TimestampNtz, Statistics::Int64(values)) => values
            .min_opt()
            .map(|value| KernelScalar::TimestampNtz(*value)),
        (TimestampNtz, Statistics::Int32(values)) => timestamp_ntz_from_days(values.min_opt()),
        (Decimal(decimal_type), Statistics::Int32(values)) => values
            .min_opt()
            .and_then(|value| KernelDecimalData::try_new(*value, *decimal_type).ok())
            .map(Into::into),
        (Decimal(decimal_type), Statistics::Int64(values)) => values
            .min_opt()
            .and_then(|value| KernelDecimalData::try_new(*value, *decimal_type).ok())
            .map(Into::into),
        (Decimal(decimal_type), Statistics::FixedLenByteArray(values)) => values
            .min_opt()
            .and_then(|value| decimal_scalar_from_bytes(value.data(), *decimal_type)),
        _ => None,
    }
}

fn stat_max_scalar(data_type: &KernelDataType, stats: &Statistics) -> Option<KernelScalar> {
    use KernelPrimitiveType::*;

    match (data_type.as_primitive_opt()?, stats) {
        (String, Statistics::ByteArray(values)) => values.max_opt()?.as_utf8().ok().map(Into::into),
        (String, Statistics::FixedLenByteArray(values)) => {
            values.max_opt()?.as_utf8().ok().map(Into::into)
        }
        (Long, Statistics::Int64(values)) => values.max_opt().map(Into::into),
        (Long, Statistics::Int32(values)) => values.max_opt().map(|value| (*value as i64).into()),
        (Integer, Statistics::Int32(values)) => values.max_opt().map(Into::into),
        (Short, Statistics::Int32(values)) => values.max_opt().map(|value| (*value as i16).into()),
        (Byte, Statistics::Int32(values)) => values.max_opt().map(|value| (*value as i8).into()),
        (Float, Statistics::Float(values)) => values.max_opt().map(Into::into),
        (Double, Statistics::Double(values)) => values.max_opt().map(Into::into),
        (Double, Statistics::Float(values)) => values.max_opt().map(|value| (*value as f64).into()),
        (Boolean, Statistics::Boolean(values)) => values.max_opt().map(Into::into),
        (Binary, Statistics::ByteArray(values)) => {
            values.max_opt().map(|value| value.data().into())
        }
        (Binary, Statistics::FixedLenByteArray(values)) => {
            values.max_opt().map(|value| value.data().into())
        }
        (Date, Statistics::Int32(values)) => {
            values.max_opt().map(|value| KernelScalar::Date(*value))
        }
        (Timestamp, Statistics::Int64(values)) => values
            .max_opt()
            .map(|value| KernelScalar::Timestamp(*value)),
        (TimestampNtz, Statistics::Int64(values)) => values
            .max_opt()
            .map(|value| KernelScalar::TimestampNtz(*value)),
        (TimestampNtz, Statistics::Int32(values)) => timestamp_ntz_from_days(values.max_opt()),
        (Decimal(decimal_type), Statistics::Int32(values)) => values
            .max_opt()
            .and_then(|value| KernelDecimalData::try_new(*value, *decimal_type).ok())
            .map(Into::into),
        (Decimal(decimal_type), Statistics::Int64(values)) => values
            .max_opt()
            .and_then(|value| KernelDecimalData::try_new(*value, *decimal_type).ok())
            .map(Into::into),
        (Decimal(decimal_type), Statistics::FixedLenByteArray(values)) => values
            .max_opt()
            .and_then(|value| decimal_scalar_from_bytes(value.data(), *decimal_type)),
        _ => None,
    }
}

fn timestamp_ntz_from_days(days: Option<&i32>) -> Option<KernelScalar> {
    let days = u64::try_from(*days?).ok()?;
    let timestamp = DateTime::UNIX_EPOCH.checked_add_days(Days::new(days))?;
    let duration = timestamp.signed_duration_since(DateTime::UNIX_EPOCH);
    Some(KernelScalar::TimestampNtz(duration.num_microseconds()?))
}

fn decimal_scalar_from_bytes(
    bytes: &[u8],
    data_type: delta_kernel::schema::DecimalType,
) -> Option<KernelScalar> {
    if bytes.len() > 16 {
        return None;
    }

    // Parquet fixed-length decimal stats are stored as big-endian two's
    // complement bytes. Convert to little-endian i128 bytes and preserve the
    // sign when the encoded value is narrower than 16 bytes.
    let pad = bytes
        .first()
        .is_some_and(|byte| byte & 0x80 != 0)
        .then_some(0xff)
        .unwrap_or(0x00);
    let mut bytes = Vec::from(bytes);
    bytes.reverse();
    bytes.resize(16, pad);
    let bytes: [u8; 16] = bytes.try_into().ok()?;
    KernelDecimalData::try_new(i128::from_le_bytes(bytes), data_type)
        .ok()
        .map(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_scalar_from_fixed_len_bytes_sign_extends_negative_values()
    -> Result<(), Box<dyn std::error::Error>> {
        let decimal_type = delta_kernel::schema::DecimalType::try_new(10, 2)?;
        let negative_one = match decimal_scalar_from_bytes(&[0xff], decimal_type) {
            Some(KernelScalar::Decimal(value)) => value,
            other => return Err(format!("expected decimal scalar, got {other:?}").into()),
        };

        assert_eq!(negative_one.bits(), -1);

        Ok(())
    }

    #[test]
    fn decimal_scalar_from_fixed_len_bytes_preserves_positive_values()
    -> Result<(), Box<dyn std::error::Error>> {
        let decimal_type = delta_kernel::schema::DecimalType::try_new(10, 2)?;
        let positive_one = match decimal_scalar_from_bytes(&[0x01], decimal_type) {
            Some(KernelScalar::Decimal(value)) => value,
            other => return Err(format!("expected decimal scalar, got {other:?}").into()),
        };

        assert_eq!(positive_one.bits(), 1);

        Ok(())
    }
}
