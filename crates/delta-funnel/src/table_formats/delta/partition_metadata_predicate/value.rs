use std::cmp::Ordering;

use datafusion::arrow::datatypes::DataType;

/// Logical value family used when evaluating serialized Delta partition metadata.
///
/// Delta stores partition values as text in add-file metadata. Exact provider
/// pruning still has to evaluate those raw strings through the column's logical
/// type. This enum is the small central switch that additional logical
/// partition metadata types should extend.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PartitionMetadataValueKind {
    String,
    SignedInteger { min: i64, max: i64 },
    Boolean,
}

impl PartitionMetadataValueKind {
    pub(super) fn from_supported_data_type(data_type: &DataType) -> Option<Self> {
        match data_type {
            DataType::Utf8 | DataType::LargeUtf8 => Some(Self::String),
            DataType::Int8 => Some(Self::SignedInteger {
                min: i64::from(i8::MIN),
                max: i64::from(i8::MAX),
            }),
            DataType::Int16 => Some(Self::SignedInteger {
                min: i64::from(i16::MIN),
                max: i64::from(i16::MAX),
            }),
            DataType::Int32 => Some(Self::SignedInteger {
                min: i64::from(i32::MIN),
                max: i64::from(i32::MAX),
            }),
            DataType::Int64 => Some(Self::SignedInteger {
                min: i64::MIN,
                max: i64::MAX,
            }),
            DataType::Boolean => Some(Self::Boolean),
            _ => None,
        }
    }

    pub(super) fn is_boolean(self) -> bool {
        matches!(self, Self::Boolean)
    }

    pub(super) fn parse_raw(self, raw_value: &str) -> Option<PartitionScalar> {
        match self {
            Self::String => Some(PartitionScalar::String(raw_value.to_owned())),
            Self::SignedInteger { min, max } => raw_value
                .parse::<i64>()
                .ok()
                .filter(|value| min <= *value && *value <= max)
                .map(PartitionScalar::SignedInteger),
            Self::Boolean => raw_value.parse::<bool>().ok().map(PartitionScalar::Boolean),
        }
    }
}

/// Typed literal or parsed raw partition metadata value.
///
/// The evaluator compares values only after both sides have been converted into
/// this representation, so raw string ordering is never reused for typed
/// partition values.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) enum PartitionScalar {
    String(String),
    SignedInteger(i64),
    Boolean(bool),
}

impl PartitionScalar {
    pub(super) fn compare(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::String(left), Self::String(right)) => Some(left.cmp(right)),
            (Self::SignedInteger(left), Self::SignedInteger(right)) => Some(left.cmp(right)),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::DataType;

    use super::*;

    #[test]
    fn value_kind_tracks_supported_logical_types() {
        assert_eq!(
            PartitionMetadataValueKind::from_supported_data_type(&DataType::Utf8),
            Some(PartitionMetadataValueKind::String)
        );
        assert_eq!(
            PartitionMetadataValueKind::from_supported_data_type(&DataType::LargeUtf8),
            Some(PartitionMetadataValueKind::String)
        );
        assert_eq!(
            PartitionMetadataValueKind::from_supported_data_type(&DataType::Int8),
            Some(PartitionMetadataValueKind::SignedInteger {
                min: i64::from(i8::MIN),
                max: i64::from(i8::MAX),
            })
        );
        assert_eq!(
            PartitionMetadataValueKind::from_supported_data_type(&DataType::Int16),
            Some(PartitionMetadataValueKind::SignedInteger {
                min: i64::from(i16::MIN),
                max: i64::from(i16::MAX),
            })
        );
        assert_eq!(
            PartitionMetadataValueKind::from_supported_data_type(&DataType::Int32),
            Some(PartitionMetadataValueKind::SignedInteger {
                min: i64::from(i32::MIN),
                max: i64::from(i32::MAX),
            })
        );
        assert_eq!(
            PartitionMetadataValueKind::from_supported_data_type(&DataType::Int64),
            Some(PartitionMetadataValueKind::SignedInteger {
                min: i64::MIN,
                max: i64::MAX,
            })
        );
        assert_eq!(
            PartitionMetadataValueKind::from_supported_data_type(&DataType::Boolean),
            Some(PartitionMetadataValueKind::Boolean)
        );
        assert_eq!(
            PartitionMetadataValueKind::from_supported_data_type(&DataType::Float64),
            None
        );
    }

    #[test]
    fn string_value_kind_preserves_raw_partition_text() {
        assert_eq!(
            PartitionMetadataValueKind::String.parse_raw(""),
            Some(PartitionScalar::String(String::new()))
        );
        assert_eq!(
            PartitionMetadataValueKind::String.parse_raw("us-west"),
            Some(PartitionScalar::String("us-west".to_owned()))
        );
    }

    #[test]
    fn integer_value_kind_parses_signed_base10_text_with_width_bounds() {
        let byte_kind = PartitionMetadataValueKind::SignedInteger {
            min: i64::from(i8::MIN),
            max: i64::from(i8::MAX),
        };
        let int_kind = PartitionMetadataValueKind::SignedInteger {
            min: i64::from(i32::MIN),
            max: i64::from(i32::MAX),
        };

        assert_eq!(
            byte_kind.parse_raw("-128"),
            Some(PartitionScalar::SignedInteger(-128))
        );
        assert_eq!(
            byte_kind.parse_raw("127"),
            Some(PartitionScalar::SignedInteger(127))
        );
        assert_eq!(byte_kind.parse_raw("-129"), None);
        assert_eq!(byte_kind.parse_raw("128"), None);
        assert_eq!(byte_kind.parse_raw(""), None);
        assert_eq!(byte_kind.parse_raw("not-an-integer"), None);
        assert_eq!(
            int_kind.parse_raw("2147483647"),
            Some(PartitionScalar::SignedInteger(2147483647))
        );
        assert_eq!(int_kind.parse_raw("2147483648"), None);
    }

    #[test]
    fn boolean_value_kind_parses_lowercase_delta_metadata_text() {
        assert_eq!(
            PartitionMetadataValueKind::Boolean.parse_raw("true"),
            Some(PartitionScalar::Boolean(true))
        );
        assert_eq!(
            PartitionMetadataValueKind::Boolean.parse_raw("false"),
            Some(PartitionScalar::Boolean(false))
        );
        assert_eq!(PartitionMetadataValueKind::Boolean.parse_raw("TRUE"), None);
        assert_eq!(PartitionMetadataValueKind::Boolean.parse_raw("False"), None);
        assert_eq!(PartitionMetadataValueKind::Boolean.parse_raw(""), None);
        assert_eq!(
            PartitionMetadataValueKind::Boolean.parse_raw("not-a-boolean"),
            None
        );
    }
}
