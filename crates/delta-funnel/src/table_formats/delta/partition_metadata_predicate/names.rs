use std::collections::{HashMap, HashSet};

/// Logical-to-physical partition column names for Delta scan metadata.
///
/// Delta scan files expose partition values by physical column name. Most
/// currently supported tables use the logical name as the physical name, but
/// keeping the lookup explicit prevents future column-mapping support from
/// leaking into provider planning code.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct DeltaPartitionNameMap {
    logical_to_physical: HashMap<String, String>,
}

impl DeltaPartitionNameMap {
    /// Builds an identity lookup for tables where logical and physical
    /// partition names are the same.
    #[must_use]
    pub(crate) fn identity(partition_columns: &HashSet<String>) -> Self {
        Self {
            logical_to_physical: partition_columns
                .iter()
                .map(|name| (name.clone(), name.clone()))
                .collect(),
        }
    }

    #[cfg(test)]
    pub(super) fn new(logical_to_physical: impl IntoIterator<Item = (String, String)>) -> Self {
        Self {
            logical_to_physical: logical_to_physical.into_iter().collect(),
        }
    }

    pub(super) fn physical_name(&self, logical_name: &str) -> Option<&str> {
        self.logical_to_physical
            .get(logical_name)
            .map(String::as_str)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PhysicalPartitionColumn {
    physical_name: String,
}

impl PhysicalPartitionColumn {
    pub(super) fn new(physical_name: String) -> Self {
        Self { physical_name }
    }

    pub(super) fn value<'a>(
        &self,
        partition_values: &'a HashMap<String, String>,
    ) -> Option<&'a str> {
        partition_values
            .get(&self.physical_name)
            .map(String::as_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn partition_columns(names: &[&str]) -> HashSet<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    #[test]
    fn identity_map_uses_logical_names_as_physical_names() {
        let name_map = DeltaPartitionNameMap::identity(&partition_columns(&["region", "day"]));

        assert_eq!(name_map.physical_name("region"), Some("region"));
        assert_eq!(name_map.physical_name("day"), Some("day"));
        assert_eq!(name_map.physical_name("missing"), None);
    }

    #[test]
    fn physical_partition_column_reads_raw_metadata_values() {
        let column = PhysicalPartitionColumn::new("col-physical-region".to_owned());
        let values = HashMap::from([
            ("col-physical-region".to_owned(), "us-west".to_owned()),
            ("region".to_owned(), "us-east".to_owned()),
        ]);

        assert_eq!(column.value(&values), Some("us-west"));
        assert_eq!(
            PhysicalPartitionColumn::new("missing".to_owned()).value(&values),
            None
        );
    }
}
