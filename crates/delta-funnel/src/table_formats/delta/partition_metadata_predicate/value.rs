use std::cmp::Ordering;

use datafusion::arrow::datatypes::DataType;

/// Logical value family used when evaluating serialized Delta partition metadata.
///
/// Delta stores partition values as text in add-file metadata. Exact provider
/// pruning still has to evaluate those raw strings through the column's logical
/// type. This enum is the small central switch that future integer, decimal,
/// boolean, date, and timestamp support should extend.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PartitionMetadataValueKind {
    String,
}

impl PartitionMetadataValueKind {
    pub(super) fn from_supported_data_type(data_type: &DataType) -> Option<Self> {
        match data_type {
            DataType::Utf8 | DataType::LargeUtf8 => Some(Self::String),
            _ => None,
        }
    }

    pub(super) fn parse_raw(self, raw_value: &str) -> Option<PartitionScalar> {
        match self {
            Self::String => Some(PartitionScalar::String(raw_value.to_owned())),
        }
    }
}

/// Typed literal or parsed raw partition metadata value.
///
/// The evaluator compares values only after both sides have been converted into
/// this representation. That avoids accidental raw string ordering when
/// non-string partition types are added.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) enum PartitionScalar {
    String(String),
}

impl PartitionScalar {
    pub(super) fn compare(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::String(left), Self::String(right)) => Some(left.cmp(right)),
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::DataType;

    use super::*;

    #[test]
    fn value_kind_tracks_current_supported_logical_types() {
        assert_eq!(
            PartitionMetadataValueKind::from_supported_data_type(&DataType::Utf8),
            Some(PartitionMetadataValueKind::String)
        );
        assert_eq!(
            PartitionMetadataValueKind::from_supported_data_type(&DataType::LargeUtf8),
            Some(PartitionMetadataValueKind::String)
        );
        assert_eq!(
            PartitionMetadataValueKind::from_supported_data_type(&DataType::Int64),
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
}
