use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
        Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        LargeBinaryArray, LargeStringArray, RecordBatch, Scalar as ArrowScalar, StringArray,
        TimestampMicrosecondArray,
        types::{Decimal128Type, DecimalType, validate_decimal_precision_and_scale},
    },
    compute::{
        filter_record_batch,
        kernels::{
            boolean::{and_kleene, is_not_null, is_null, not, or_kleene},
            cmp,
        },
    },
    datatypes::{DataType, Schema, TimeUnit},
    error::ArrowError,
};

use crate::{DeltaReaderError, error::UnsupportedPredicateSnafu};

/// Comparison operation in a Delta predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaComparison {
    /// Equal.
    Eq,
    /// Not equal.
    NotEq,
    /// Less than.
    Lt,
    /// Less than or equal.
    LtEq,
    /// Greater than.
    Gt,
    /// Greater than or equal.
    GtEq,
}

/// Non-null scalar value in a Delta predicate.
#[derive(Debug, Clone, PartialEq)]
pub enum DeltaScalar {
    /// Boolean value.
    Boolean(bool),
    /// Signed 8-bit integer value.
    Int8(i8),
    /// Signed 16-bit integer value.
    Int16(i16),
    /// Signed 32-bit integer value.
    Int32(i32),
    /// Signed 64-bit integer value.
    Int64(i64),
    /// 32-bit floating-point value.
    Float32(f32),
    /// 64-bit floating-point value.
    Float64(f64),
    /// Date value represented as days since the Unix epoch.
    Date32(i32),
    /// 128-bit decimal value.
    Decimal128 {
        /// Unscaled decimal value.
        value: i128,
        /// Decimal precision.
        precision: u8,
        /// Decimal scale.
        scale: i8,
    },
    /// UTF-8 string value.
    Utf8(String),
    /// Large UTF-8 string value.
    LargeUtf8(String),
    /// Binary value.
    Binary(Vec<u8>),
    /// Large binary value.
    LargeBinary(Vec<u8>),
    /// Fixed-size binary value.
    FixedSizeBinary {
        /// Required byte width.
        size: i32,
        /// Binary value.
        value: Vec<u8>,
    },
    /// Microsecond timestamp value.
    TimestampMicrosecond {
        /// Microseconds since the Unix epoch.
        value: i64,
        /// Optional timezone name.
        timezone: Option<String>,
    },
}

/// Query-engine-neutral Delta predicate.
#[derive(Debug, Clone, PartialEq)]
pub enum DeltaPredicate {
    /// Constant Boolean predicate.
    Boolean(bool),
    /// Compare a column with a non-null scalar value.
    Compare {
        /// Unqualified top-level logical column name.
        column: String,
        /// Comparison operation.
        op: DeltaComparison,
        /// Non-null scalar value.
        value: DeltaScalar,
    },
    /// Test whether a column value is null.
    IsNull {
        /// Unqualified top-level logical column name.
        column: String,
    },
    /// Test whether a column value is not null.
    IsNotNull {
        /// Unqualified top-level logical column name.
        column: String,
    },
    /// Logical conjunction.
    And(Vec<DeltaPredicate>),
    /// Logical disjunction.
    Or(Vec<DeltaPredicate>),
    /// Logical negation.
    Not(Box<DeltaPredicate>),
}

#[allow(dead_code)]
pub(crate) fn validate_predicate(
    predicate: &DeltaPredicate,
    schema: &Schema,
) -> Result<(), DeltaReaderError> {
    match predicate {
        DeltaPredicate::Boolean(_) => Ok(()),
        DeltaPredicate::Compare { column, value, .. } => {
            validate_scalar(column_data_type(schema, column)?, value)
        }
        DeltaPredicate::IsNull { column } | DeltaPredicate::IsNotNull { column } => {
            column_data_type(schema, column).map(|_| ())
        }
        DeltaPredicate::And(children) | DeltaPredicate::Or(children) => children
            .iter()
            .try_for_each(|child| validate_predicate(child, schema)),
        DeltaPredicate::Not(child) => validate_predicate(child, schema),
    }
}

#[allow(dead_code)]
pub(crate) fn evaluate_predicate(
    batch: &RecordBatch,
    predicate: &DeltaPredicate,
) -> Result<RecordBatch, DeltaReaderError> {
    predicate_selection(batch, predicate)
        .and_then(|selection| filter_record_batch(batch, &selection))
        .map_err(|_| unsupported_predicate("predicate_evaluation"))
}

pub(crate) fn referenced_columns(predicate: &DeltaPredicate) -> Vec<String> {
    fn visit(predicate: &DeltaPredicate, columns: &mut Vec<String>) {
        match predicate {
            DeltaPredicate::Boolean(_) => {}
            DeltaPredicate::Compare { column, .. }
            | DeltaPredicate::IsNull { column }
            | DeltaPredicate::IsNotNull { column } => {
                if !columns.contains(column) {
                    columns.push(column.clone());
                }
            }
            DeltaPredicate::And(children) | DeltaPredicate::Or(children) => {
                for child in children {
                    visit(child, columns);
                }
            }
            DeltaPredicate::Not(child) => visit(child, columns),
        }
    }

    let mut columns = Vec::new();
    visit(predicate, &mut columns);
    columns
}

fn predicate_selection(
    batch: &RecordBatch,
    predicate: &DeltaPredicate,
) -> Result<BooleanArray, ArrowError> {
    match predicate {
        DeltaPredicate::Boolean(value) => Ok(BooleanArray::from(vec![*value; batch.num_rows()])),
        DeltaPredicate::Compare { column, op, value } => {
            let column = batch.column(batch.schema().index_of(column)?);
            let scalar = ArrowScalar::new(scalar_array(value)?);
            match op {
                DeltaComparison::Eq => cmp::eq(column, &scalar),
                DeltaComparison::NotEq => cmp::neq(column, &scalar),
                DeltaComparison::Lt => cmp::lt(column, &scalar),
                DeltaComparison::LtEq => cmp::lt_eq(column, &scalar),
                DeltaComparison::Gt => cmp::gt(column, &scalar),
                DeltaComparison::GtEq => cmp::gt_eq(column, &scalar),
            }
        }
        DeltaPredicate::IsNull { column } => {
            is_null(batch.column(batch.schema().index_of(column)?).as_ref())
        }
        DeltaPredicate::IsNotNull { column } => {
            is_not_null(batch.column(batch.schema().index_of(column)?).as_ref())
        }
        DeltaPredicate::And(children) => combine_selections(batch, children, true, and_kleene),
        DeltaPredicate::Or(children) => combine_selections(batch, children, false, or_kleene),
        DeltaPredicate::Not(child) => not(&predicate_selection(batch, child)?),
    }
}

fn combine_selections(
    batch: &RecordBatch,
    predicates: &[DeltaPredicate],
    identity: bool,
    combine: fn(&BooleanArray, &BooleanArray) -> Result<BooleanArray, ArrowError>,
) -> Result<BooleanArray, ArrowError> {
    let mut predicates = predicates.iter();
    let Some(first) = predicates.next() else {
        return Ok(BooleanArray::from(vec![identity; batch.num_rows()]));
    };
    let first = predicate_selection(batch, first)?;

    predicates.try_fold(first, |selection, predicate| {
        combine(&selection, &predicate_selection(batch, predicate)?)
    })
}

fn scalar_array(scalar: &DeltaScalar) -> Result<ArrayRef, ArrowError> {
    let array: ArrayRef = match scalar {
        DeltaScalar::Boolean(value) => Arc::new(BooleanArray::from(vec![*value])),
        DeltaScalar::Int8(value) => Arc::new(Int8Array::from(vec![*value])),
        DeltaScalar::Int16(value) => Arc::new(Int16Array::from(vec![*value])),
        DeltaScalar::Int32(value) => Arc::new(Int32Array::from(vec![*value])),
        DeltaScalar::Int64(value) => Arc::new(Int64Array::from(vec![*value])),
        DeltaScalar::Float32(value) => Arc::new(Float32Array::from(vec![*value])),
        DeltaScalar::Float64(value) => Arc::new(Float64Array::from(vec![*value])),
        DeltaScalar::Date32(value) => Arc::new(Date32Array::from(vec![*value])),
        DeltaScalar::Decimal128 {
            value,
            precision,
            scale,
        } => Arc::new(
            Decimal128Array::from(vec![*value]).with_precision_and_scale(*precision, *scale)?,
        ),
        DeltaScalar::Utf8(value) => Arc::new(StringArray::from(vec![value.as_str()])),
        DeltaScalar::LargeUtf8(value) => Arc::new(LargeStringArray::from(vec![value.as_str()])),
        DeltaScalar::Binary(value) => Arc::new(BinaryArray::from(vec![value.as_slice()])),
        DeltaScalar::LargeBinary(value) => Arc::new(LargeBinaryArray::from(vec![value.as_slice()])),
        DeltaScalar::FixedSizeBinary { value, .. } => Arc::new(
            FixedSizeBinaryArray::try_from_iter(std::iter::once(value.as_slice()))?,
        ),
        DeltaScalar::TimestampMicrosecond { value, timezone } => Arc::new(
            TimestampMicrosecondArray::from(vec![*value]).with_timezone_opt(timezone.clone()),
        ),
    };
    Ok(array)
}

fn column_data_type<'a>(
    schema: &'a Schema,
    column: &str,
) -> Result<&'a DataType, DeltaReaderError> {
    if column.is_empty() || column.contains('.') {
        return Err(unsupported_predicate("invalid_column_reference"));
    }

    let mut matching_fields = schema
        .fields()
        .iter()
        .filter(|field| field.name() == column);
    let Some(field) = matching_fields.next() else {
        return Err(unsupported_predicate("column_not_found"));
    };

    if matching_fields.next().is_some() {
        return Err(unsupported_predicate("ambiguous_column"));
    }

    Ok(field.data_type())
}

fn validate_scalar(data_type: &DataType, scalar: &DeltaScalar) -> Result<(), DeltaReaderError> {
    let matches = match scalar {
        DeltaScalar::Boolean(_) => data_type == &DataType::Boolean,
        DeltaScalar::Int8(_) => data_type == &DataType::Int8,
        DeltaScalar::Int16(_) => data_type == &DataType::Int16,
        DeltaScalar::Int32(_) => data_type == &DataType::Int32,
        DeltaScalar::Int64(_) => data_type == &DataType::Int64,
        DeltaScalar::Float32(value) => {
            if !value.is_finite() {
                return Err(unsupported_predicate("non_finite_float"));
            }
            data_type == &DataType::Float32
        }
        DeltaScalar::Float64(value) => {
            if !value.is_finite() {
                return Err(unsupported_predicate("non_finite_float"));
            }
            data_type == &DataType::Float64
        }
        DeltaScalar::Date32(_) => data_type == &DataType::Date32,
        DeltaScalar::Decimal128 {
            value,
            precision,
            scale,
        } => {
            if validate_decimal_precision_and_scale::<Decimal128Type>(*precision, *scale).is_err()
                || Decimal128Type::validate_decimal_precision(*value, *precision, *scale).is_err()
            {
                return Err(unsupported_predicate("invalid_decimal"));
            }
            data_type == &DataType::Decimal128(*precision, *scale)
        }
        DeltaScalar::Utf8(_) => data_type == &DataType::Utf8,
        DeltaScalar::LargeUtf8(_) => data_type == &DataType::LargeUtf8,
        DeltaScalar::Binary(_) => data_type == &DataType::Binary,
        DeltaScalar::LargeBinary(_) => data_type == &DataType::LargeBinary,
        DeltaScalar::FixedSizeBinary { size, value } => {
            if usize::try_from(*size).ok() != Some(value.len()) || *size <= 0 {
                return Err(unsupported_predicate("invalid_fixed_size_binary"));
            }
            data_type == &DataType::FixedSizeBinary(*size)
        }
        DeltaScalar::TimestampMicrosecond { timezone, .. } => match timezone {
            Some(timezone) => {
                if timezone.is_empty() {
                    return Err(unsupported_predicate("invalid_timestamp_timezone"));
                }
                matches!(
                    data_type,
                    DataType::Timestamp(TimeUnit::Microsecond, Some(field_timezone))
                        if field_timezone.as_ref() == timezone
                )
            }
            None => data_type == &DataType::Timestamp(TimeUnit::Microsecond, None),
        },
    };

    if matches {
        Ok(())
    } else {
        Err(unsupported_predicate("scalar_type_mismatch"))
    }
}

fn unsupported_predicate(reason: &'static str) -> DeltaReaderError {
    UnsupportedPredicateSnafu { reason }.build()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{
            Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
            FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
            Int64Array, LargeBinaryArray, LargeStringArray, RecordBatch, StringArray,
            TimestampMicrosecondArray,
        },
        datatypes::{
            DataType, Field, Fields, IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode,
        },
    };

    use super::{
        DeltaComparison, DeltaPredicate, DeltaScalar, evaluate_predicate, predicate_selection,
        validate_predicate,
    };
    use crate::{DeltaReaderError, DeltaReaderPhase};

    fn compare(column: &str, value: DeltaScalar) -> DeltaPredicate {
        DeltaPredicate::Compare {
            column: column.into(),
            op: DeltaComparison::Eq,
            value,
        }
    }

    fn supported_schema() -> Schema {
        Schema::new(vec![
            Field::new("boolean", DataType::Boolean, true),
            Field::new("int8", DataType::Int8, true),
            Field::new("int16", DataType::Int16, true),
            Field::new("int32", DataType::Int32, true),
            Field::new("int64", DataType::Int64, true),
            Field::new("float32", DataType::Float32, true),
            Field::new("float64", DataType::Float64, true),
            Field::new("date32", DataType::Date32, true),
            Field::new("decimal", DataType::Decimal128(10, 2), true),
            Field::new("negative_scale", DataType::Decimal128(10, -2), true),
            Field::new("utf8", DataType::Utf8, true),
            Field::new("large_utf8", DataType::LargeUtf8, true),
            Field::new("binary", DataType::Binary, true),
            Field::new("large_binary", DataType::LargeBinary, true),
            Field::new("fixed_binary", DataType::FixedSizeBinary(3), true),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new(
                "timestamp_ntz",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new(
                "struct",
                DataType::Struct(Fields::from(vec![Field::new(
                    "nested",
                    DataType::Int32,
                    true,
                )])),
                true,
            ),
        ])
    }

    fn assert_unsupported(predicate: &DeltaPredicate, schema: &Schema) {
        let error = validate_predicate(predicate, schema).expect_err("predicate must be rejected");
        assert_eq!(error.as_str(), "unsupported_predicate");
        assert_eq!(error.phase(), DeltaReaderPhase::ScanPlanning);
    }

    fn selection_values(
        batch: &RecordBatch,
        predicate: &DeltaPredicate,
    ) -> Result<Vec<Option<bool>>, Box<dyn std::error::Error>> {
        validate_predicate(predicate, batch.schema().as_ref())?;
        Ok(predicate_selection(batch, predicate)?.iter().collect())
    }

    fn evaluate_validated(
        batch: &RecordBatch,
        predicate: &DeltaPredicate,
    ) -> Result<RecordBatch, DeltaReaderError> {
        validate_predicate(predicate, batch.schema().as_ref())?;
        evaluate_predicate(batch, predicate)
    }

    #[test]
    fn accepts_every_exact_scalar_shape_and_predicate_form() -> Result<(), DeltaReaderError> {
        let schema = supported_schema();
        let scalars = [
            ("boolean", DeltaScalar::Boolean(true)),
            ("int8", DeltaScalar::Int8(i8::MIN)),
            ("int16", DeltaScalar::Int16(i16::MAX)),
            ("int32", DeltaScalar::Int32(i32::MIN)),
            ("int64", DeltaScalar::Int64(i64::MAX)),
            ("float32", DeltaScalar::Float32(0.0)),
            ("float32", DeltaScalar::Float32(-0.0)),
            ("float64", DeltaScalar::Float64(0.0)),
            ("float64", DeltaScalar::Float64(-0.0)),
            ("date32", DeltaScalar::Date32(i32::MAX)),
            (
                "decimal",
                DeltaScalar::Decimal128 {
                    value: 9_999_999_999,
                    precision: 10,
                    scale: 2,
                },
            ),
            (
                "negative_scale",
                DeltaScalar::Decimal128 {
                    value: 123,
                    precision: 10,
                    scale: -2,
                },
            ),
            ("utf8", DeltaScalar::Utf8(String::new())),
            ("large_utf8", DeltaScalar::LargeUtf8(String::new())),
            ("binary", DeltaScalar::Binary(Vec::new())),
            ("large_binary", DeltaScalar::LargeBinary(Vec::new())),
            (
                "fixed_binary",
                DeltaScalar::FixedSizeBinary {
                    size: 3,
                    value: vec![0, 1, 2],
                },
            ),
            (
                "timestamp",
                DeltaScalar::TimestampMicrosecond {
                    value: i64::MAX,
                    timezone: Some("UTC".into()),
                },
            ),
            (
                "timestamp_ntz",
                DeltaScalar::TimestampMicrosecond {
                    value: i64::MIN,
                    timezone: None,
                },
            ),
        ];
        let comparisons = [
            DeltaComparison::Eq,
            DeltaComparison::NotEq,
            DeltaComparison::Lt,
            DeltaComparison::LtEq,
            DeltaComparison::Gt,
            DeltaComparison::GtEq,
        ];

        for op in comparisons {
            for (column, value) in &scalars {
                validate_predicate(
                    &DeltaPredicate::Compare {
                        column: (*column).into(),
                        op,
                        value: value.clone(),
                    },
                    &schema,
                )?;
            }
        }

        validate_predicate(&DeltaPredicate::Boolean(true), &schema)?;
        validate_predicate(
            &DeltaPredicate::And(vec![
                DeltaPredicate::IsNull {
                    column: "struct".into(),
                },
                DeltaPredicate::Or(Vec::new()),
                DeltaPredicate::Not(Box::new(DeltaPredicate::IsNotNull {
                    column: "int32".into(),
                })),
            ]),
            &schema,
        )?;
        validate_predicate(&DeltaPredicate::And(Vec::new()), &schema)?;

        Ok(())
    }

    #[test]
    fn rejects_invalid_missing_nested_and_ambiguous_columns_without_disclosure() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("duplicate", DataType::Int32, true),
            Field::new("duplicate", DataType::Int32, true),
        ]);

        for column in ["", "profile.secret", "missing-secret", "duplicate"] {
            let predicate = compare(column, DeltaScalar::Int32(7));
            let error = validate_predicate(&predicate, &schema)
                .expect_err("invalid column must be rejected");
            let display = error.to_string();
            assert_eq!(error.as_str(), "unsupported_predicate");
            assert_eq!(error.phase(), DeltaReaderPhase::ScanPlanning);
            assert!(!display.contains("profile"));
            assert!(!display.contains("missing"));
            assert!(!display.contains("duplicate"));
            assert!(!format!("{error:?}").contains("secret"));
        }

        let hostile_literal = compare("id", DeltaScalar::Utf8("sensitive-literal".into()));
        let error = validate_predicate(&hostile_literal, &schema)
            .expect_err("mismatched literal must be rejected");
        assert!(!error.to_string().contains("sensitive-literal"));
        assert!(!format!("{error:?}").contains("sensitive-literal"));

        for predicate in [
            DeltaPredicate::And(vec![
                DeltaPredicate::Boolean(false),
                compare("missing-secret", DeltaScalar::Int32(7)),
            ]),
            DeltaPredicate::Or(vec![
                DeltaPredicate::Boolean(true),
                compare("missing-secret", DeltaScalar::Int32(7)),
            ]),
        ] {
            assert_unsupported(&predicate, &schema);
        }
    }

    #[test]
    fn rejects_coercion_and_invalid_scalar_values() {
        let schema = supported_schema();
        let invalid = [
            compare("int32", DeltaScalar::Int64(7)),
            compare("large_utf8", DeltaScalar::Utf8("value".into())),
            compare("large_binary", DeltaScalar::Binary(vec![1])),
            compare(
                "decimal",
                DeltaScalar::Decimal128 {
                    value: 1,
                    precision: 11,
                    scale: 2,
                },
            ),
            compare(
                "decimal",
                DeltaScalar::Decimal128 {
                    value: 1,
                    precision: 10,
                    scale: 3,
                },
            ),
            compare(
                "decimal",
                DeltaScalar::Decimal128 {
                    value: 1,
                    precision: 0,
                    scale: 0,
                },
            ),
            compare(
                "decimal",
                DeltaScalar::Decimal128 {
                    value: 10_000_000_000,
                    precision: 10,
                    scale: 2,
                },
            ),
            compare("float32", DeltaScalar::Float32(f32::NAN)),
            compare("float32", DeltaScalar::Float32(f32::INFINITY)),
            compare("float64", DeltaScalar::Float64(f64::NEG_INFINITY)),
            compare(
                "fixed_binary",
                DeltaScalar::FixedSizeBinary {
                    size: 0,
                    value: Vec::new(),
                },
            ),
            compare(
                "fixed_binary",
                DeltaScalar::FixedSizeBinary {
                    size: 3,
                    value: vec![1, 2],
                },
            ),
            compare(
                "fixed_binary",
                DeltaScalar::FixedSizeBinary {
                    size: 2,
                    value: vec![1, 2],
                },
            ),
            compare(
                "timestamp",
                DeltaScalar::TimestampMicrosecond {
                    value: 1,
                    timezone: Some(String::new()),
                },
            ),
            compare(
                "timestamp",
                DeltaScalar::TimestampMicrosecond {
                    value: 1,
                    timezone: Some("America/Phoenix".into()),
                },
            ),
            compare(
                "timestamp",
                DeltaScalar::TimestampMicrosecond {
                    value: 1,
                    timezone: None,
                },
            ),
            compare(
                "timestamp_ntz",
                DeltaScalar::TimestampMicrosecond {
                    value: 1,
                    timezone: Some("UTC".into()),
                },
            ),
        ];

        for predicate in invalid {
            assert_unsupported(&predicate, &schema);
        }
    }

    #[test]
    fn rejects_every_unsupported_arrow_type() {
        let item = Arc::new(Field::new("item", DataType::Int32, true));
        let entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int32, true),
            ])),
            false,
        ));
        let unsupported = vec![
            DataType::Null,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float16,
            DataType::Date64,
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Time32(TimeUnit::Second),
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Duration(TimeUnit::Microsecond),
            DataType::Interval(IntervalUnit::MonthDayNano),
            DataType::Decimal32(9, 2),
            DataType::Decimal64(18, 2),
            DataType::Decimal256(38, 2),
            DataType::Utf8View,
            DataType::BinaryView,
            DataType::List(Arc::clone(&item)),
            DataType::Struct(Fields::from(vec![Field::new(
                "nested",
                DataType::Int32,
                true,
            )])),
            DataType::Map(entries, false),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            DataType::Union(UnionFields::empty(), UnionMode::Dense),
        ];

        for data_type in unsupported {
            let schema = Schema::new(vec![Field::new("value", data_type, true)]);
            assert_unsupported(&compare("value", DeltaScalar::Int32(7)), &schema);
        }
    }

    #[test]
    fn evaluates_complete_three_valued_truth_tables() -> Result<(), Box<dyn std::error::Error>> {
        let batch = RecordBatch::try_from_iter([
            (
                "a",
                Arc::new(Int32Array::from(vec![
                    Some(1),
                    Some(1),
                    Some(1),
                    Some(0),
                    Some(0),
                    Some(0),
                    None,
                    None,
                    None,
                ])) as ArrayRef,
            ),
            (
                "b",
                Arc::new(Int32Array::from(vec![
                    Some(1),
                    Some(0),
                    None,
                    Some(1),
                    Some(0),
                    None,
                    Some(1),
                    Some(0),
                    None,
                ])) as ArrayRef,
            ),
        ])?;
        let a = compare("a", DeltaScalar::Int32(1));
        let b = compare("b", DeltaScalar::Int32(1));

        assert_eq!(
            selection_values(&batch, &DeltaPredicate::Not(Box::new(a.clone())))?,
            vec![
                Some(false),
                Some(false),
                Some(false),
                Some(true),
                Some(true),
                Some(true),
                None,
                None,
                None,
            ]
        );
        assert_eq!(
            selection_values(&batch, &DeltaPredicate::And(vec![a.clone(), b.clone()]))?,
            vec![
                Some(true),
                Some(false),
                None,
                Some(false),
                Some(false),
                Some(false),
                None,
                Some(false),
                None,
            ]
        );
        assert_eq!(
            selection_values(&batch, &DeltaPredicate::Or(vec![a, b]))?,
            vec![
                Some(true),
                Some(true),
                Some(true),
                Some(true),
                Some(false),
                None,
                Some(true),
                None,
                None,
            ]
        );
        assert_eq!(
            selection_values(&batch, &DeltaPredicate::And(Vec::new()))?,
            vec![Some(true); 9]
        );
        assert_eq!(
            selection_values(&batch, &DeltaPredicate::Or(Vec::new()))?,
            vec![Some(false); 9]
        );
        assert_eq!(
            selection_values(&batch, &DeltaPredicate::IsNull { column: "a".into() },)?,
            vec![
                Some(false),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
                Some(true),
                Some(true),
                Some(true),
            ]
        );
        assert_eq!(
            selection_values(&batch, &DeltaPredicate::IsNotNull { column: "a".into() },)?,
            vec![
                Some(true),
                Some(true),
                Some(true),
                Some(true),
                Some(true),
                Some(true),
                Some(false),
                Some(false),
                Some(false),
            ]
        );

        Ok(())
    }

    #[test]
    fn evaluates_every_comparison_and_scalar_boundary() -> Result<(), Box<dyn std::error::Error>> {
        let integer_batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3), None])) as ArrayRef,
        )])?;
        for (op, expected) in [
            (
                DeltaComparison::Eq,
                vec![Some(false), Some(true), Some(false), None],
            ),
            (
                DeltaComparison::NotEq,
                vec![Some(true), Some(false), Some(true), None],
            ),
            (
                DeltaComparison::Lt,
                vec![Some(true), Some(false), Some(false), None],
            ),
            (
                DeltaComparison::LtEq,
                vec![Some(true), Some(true), Some(false), None],
            ),
            (
                DeltaComparison::Gt,
                vec![Some(false), Some(false), Some(true), None],
            ),
            (
                DeltaComparison::GtEq,
                vec![Some(false), Some(true), Some(true), None],
            ),
        ] {
            let predicate = DeltaPredicate::Compare {
                column: "value".into(),
                op,
                value: DeltaScalar::Int32(2),
            };
            assert_eq!(selection_values(&integer_batch, &predicate)?, expected);
        }

        let decimal = Decimal128Array::from(vec![123_i128]).with_precision_and_scale(10, 2)?;
        let negative_decimal =
            Decimal128Array::from(vec![123_i128]).with_precision_and_scale(10, -2)?;
        let fixed_binary = FixedSizeBinaryArray::try_from_iter(std::iter::once(b"abc".as_slice()))?;
        let scalar_batch = RecordBatch::try_from_iter([
            (
                "boolean",
                Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
            ),
            ("int8", Arc::new(Int8Array::from(vec![-8])) as ArrayRef),
            ("int16", Arc::new(Int16Array::from(vec![-16])) as ArrayRef),
            ("int32", Arc::new(Int32Array::from(vec![-32])) as ArrayRef),
            ("int64", Arc::new(Int64Array::from(vec![-64])) as ArrayRef),
            (
                "float32",
                Arc::new(Float32Array::from(vec![1.5])) as ArrayRef,
            ),
            (
                "float64",
                Arc::new(Float64Array::from(vec![-2.5])) as ArrayRef,
            ),
            (
                "date32",
                Arc::new(Date32Array::from(vec![20_000])) as ArrayRef,
            ),
            ("decimal", Arc::new(decimal) as ArrayRef),
            ("negative_decimal", Arc::new(negative_decimal) as ArrayRef),
            ("utf8", Arc::new(StringArray::from(vec![""])) as ArrayRef),
            (
                "large_utf8",
                Arc::new(LargeStringArray::from(vec![""])) as ArrayRef,
            ),
            (
                "binary",
                Arc::new(BinaryArray::from(vec![b"".as_slice()])) as ArrayRef,
            ),
            (
                "large_binary",
                Arc::new(LargeBinaryArray::from(vec![b"".as_slice()])) as ArrayRef,
            ),
            ("fixed_binary", Arc::new(fixed_binary) as ArrayRef),
            (
                "timestamp",
                Arc::new(TimestampMicrosecondArray::from(vec![1_234_567_i64]).with_timezone("UTC"))
                    as ArrayRef,
            ),
            (
                "timestamp_ntz",
                Arc::new(TimestampMicrosecondArray::from(vec![1_234_567_i64])) as ArrayRef,
            ),
        ])?;
        let scalars = [
            ("boolean", DeltaScalar::Boolean(true)),
            ("int8", DeltaScalar::Int8(-8)),
            ("int16", DeltaScalar::Int16(-16)),
            ("int32", DeltaScalar::Int32(-32)),
            ("int64", DeltaScalar::Int64(-64)),
            ("float32", DeltaScalar::Float32(1.5)),
            ("float64", DeltaScalar::Float64(-2.5)),
            ("date32", DeltaScalar::Date32(20_000)),
            (
                "decimal",
                DeltaScalar::Decimal128 {
                    value: 123,
                    precision: 10,
                    scale: 2,
                },
            ),
            (
                "negative_decimal",
                DeltaScalar::Decimal128 {
                    value: 123,
                    precision: 10,
                    scale: -2,
                },
            ),
            ("utf8", DeltaScalar::Utf8(String::new())),
            ("large_utf8", DeltaScalar::LargeUtf8(String::new())),
            ("binary", DeltaScalar::Binary(Vec::new())),
            ("large_binary", DeltaScalar::LargeBinary(Vec::new())),
            (
                "fixed_binary",
                DeltaScalar::FixedSizeBinary {
                    size: 3,
                    value: b"abc".to_vec(),
                },
            ),
            (
                "timestamp",
                DeltaScalar::TimestampMicrosecond {
                    value: 1_234_567,
                    timezone: Some("UTC".into()),
                },
            ),
            (
                "timestamp_ntz",
                DeltaScalar::TimestampMicrosecond {
                    value: 1_234_567,
                    timezone: None,
                },
            ),
        ];

        for (column, scalar) in scalars {
            assert_eq!(
                selection_values(&scalar_batch, &compare(column, scalar))?,
                vec![Some(true)]
            );
        }

        let zero_batch = RecordBatch::try_from_iter([
            (
                "float32",
                Arc::new(Float32Array::from(vec![0.0_f32, -0.0])) as ArrayRef,
            ),
            (
                "float64",
                Arc::new(Float64Array::from(vec![0.0_f64, -0.0])) as ArrayRef,
            ),
        ])?;
        assert_eq!(
            selection_values(&zero_batch, &compare("float32", DeltaScalar::Float32(0.0)))?,
            vec![Some(true), Some(false)]
        );
        assert_eq!(
            selection_values(&zero_batch, &compare("float64", DeltaScalar::Float64(-0.0)),)?,
            vec![Some(false), Some(true)]
        );

        Ok(())
    }

    #[test]
    fn filters_sliced_multi_batch_inputs_with_stable_schema_and_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let full = RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int32Array::from(vec![
                    Some(99),
                    None,
                    Some(3),
                    Some(1),
                    Some(4),
                    Some(2),
                    Some(88),
                ])) as ArrayRef,
            ),
            (
                "label",
                Arc::new(StringArray::from(vec!["x", "n", "c", "a", "d", "b", "y"])) as ArrayRef,
            ),
        ])?;
        let batch = full.slice(1, 5);
        let predicate = DeltaPredicate::Compare {
            column: "id".into(),
            op: DeltaComparison::Gt,
            value: DeltaScalar::Int32(2),
        };
        let filtered = evaluate_validated(&batch, &predicate)?;
        assert!(Arc::ptr_eq(batch.schema_ref(), filtered.schema_ref()));
        assert_eq!(
            filtered
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or("expected Int32 output")?,
            &Int32Array::from(vec![3, 4])
        );
        assert_eq!(
            filtered
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("expected Utf8 output")?,
            &StringArray::from(vec!["c", "d"])
        );

        let second = RecordBatch::try_from_iter([(
            "id",
            Arc::new(Int32Array::from(vec![5, 0])) as ArrayRef,
        )])?;
        let second_filtered = evaluate_validated(&second, &predicate)?;
        assert_eq!(second_filtered.num_rows(), 1);

        let no_survivors = evaluate_validated(&batch, &DeltaPredicate::Boolean(false))?;
        assert_eq!(no_survivors.num_rows(), 0);
        assert!(Arc::ptr_eq(batch.schema_ref(), no_survivors.schema_ref()));

        let empty = RecordBatch::new_empty(batch.schema());
        assert_eq!(evaluate_validated(&empty, &predicate)?.num_rows(), 0);

        Ok(())
    }

    #[test]
    fn evaluation_is_stateless_concurrent_and_redacts_failures()
    -> Result<(), Box<dyn std::error::Error>> {
        let batch = Arc::new(RecordBatch::try_from_iter([(
            "id",
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
        )])?);
        let predicate = Arc::new(DeltaPredicate::Compare {
            column: "id".into(),
            op: DeltaComparison::GtEq,
            value: DeltaScalar::Int32(2),
        });
        validate_predicate(predicate.as_ref(), batch.schema().as_ref())?;

        std::thread::scope(|scope| {
            let handles = (0..8)
                .map(|_| {
                    let batch = Arc::clone(&batch);
                    let predicate = Arc::clone(&predicate);
                    scope.spawn(move || evaluate_predicate(&batch, &predicate))
                })
                .collect::<Vec<_>>();

            for handle in handles {
                let result = handle.join();
                assert!(result.is_ok());
                assert_eq!(
                    result
                        .ok()
                        .and_then(Result::ok)
                        .map(|batch| batch.num_rows()),
                    Some(2)
                );
            }
        });

        let hostile = compare(
            "sensitive-column",
            DeltaScalar::Utf8("sensitive-literal".into()),
        );
        let error = evaluate_predicate(&batch, &hostile)
            .expect_err("unexpected evaluation failure must be mapped");
        let display = error.to_string();
        assert_eq!(error.as_str(), "unsupported_predicate");
        assert!(!display.contains("sensitive-column"));
        assert!(!display.contains("sensitive-literal"));
        assert!(!format!("{error:?}").contains("sensitive"));

        Ok(())
    }
}
