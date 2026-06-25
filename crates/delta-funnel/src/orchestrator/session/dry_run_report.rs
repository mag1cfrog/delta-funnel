use std::fmt;

use crate::ReportReasonCode;

/// Output schema field included in an MSSQL dry-run report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlDryRunOutputFieldReport {
    index: u64,
    name: String,
    arrow_type: String,
    nullable: bool,
}

impl MssqlDryRunOutputFieldReport {
    pub(super) fn from_mapping(mapping: &arrow_tiberius::SchemaMapping) -> Self {
        Self {
            index: crate::usize_to_u64_saturating(mapping.arrow().index()),
            name: mapping.arrow().name().to_owned(),
            arrow_type: mapping.arrow().data_type().to_string(),
            nullable: mapping.arrow().nullable(),
        }
    }

    /// Returns the zero-based output field index.
    #[must_use]
    pub const fn index(&self) -> u64 {
        self.index
    }

    /// Returns the output field name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the Arrow data type as a stable display string.
    #[must_use]
    pub fn arrow_type(&self) -> &str {
        &self.arrow_type
    }

    /// Returns true when the output field is nullable.
    #[must_use]
    pub const fn nullable(&self) -> bool {
        self.nullable
    }
}

/// SQL identity state included in an MSSQL dry-run output report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MssqlDryRunSqlIdentityState {
    /// A stable SQL identity hash is available.
    Present,
    /// No SQL identity applies to the selected lazy table.
    Absent,
    /// A SQL identity applies, but could not be reported from available metadata.
    Unavailable,
}

impl MssqlDryRunSqlIdentityState {
    /// Returns a stable lower-snake-case code for report serialization.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Present => "present",
            Self::Absent => "absent",
            Self::Unavailable => "unavailable",
        }
    }
}

impl fmt::Display for MssqlDryRunSqlIdentityState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Redacted SQL identity included in an MSSQL dry-run output report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlDryRunSqlIdentityReport {
    state: MssqlDryRunSqlIdentityState,
    hash: Option<String>,
    reason: Option<ReportReasonCode>,
}

impl MssqlDryRunSqlIdentityReport {
    pub(super) fn present(hash: String) -> Self {
        Self {
            state: MssqlDryRunSqlIdentityState::Present,
            hash: Some(hash),
            reason: None,
        }
    }

    pub(super) fn absent() -> Self {
        Self {
            state: MssqlDryRunSqlIdentityState::Absent,
            hash: None,
            reason: None,
        }
    }

    pub(super) fn unavailable(reason: ReportReasonCode) -> Self {
        Self {
            state: MssqlDryRunSqlIdentityState::Unavailable,
            hash: None,
            reason: Some(reason),
        }
    }

    /// Returns whether a SQL identity hash is present, absent, or unavailable.
    #[must_use]
    pub const fn state(&self) -> MssqlDryRunSqlIdentityState {
        self.state
    }

    /// Returns the stable SQL identity hash when retained SQL is available.
    #[must_use]
    pub fn hash(&self) -> Option<&str> {
        self.hash.as_deref()
    }

    /// Returns the reason when SQL identity reporting is unavailable.
    #[must_use]
    pub const fn reason(&self) -> Option<ReportReasonCode> {
        self.reason
    }
}

pub(super) fn stable_sql_identity_hash(sql: &str) -> String {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET_BASIS;
    for byte in sql.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }

    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::{MssqlDryRunSqlIdentityState, stable_sql_identity_hash};

    #[test]
    fn sql_identity_status_and_hash_are_stable() {
        assert_eq!(MssqlDryRunSqlIdentityState::Present.as_str(), "present");
        assert_eq!(MssqlDryRunSqlIdentityState::Absent.to_string(), "absent");
        assert_eq!(
            MssqlDryRunSqlIdentityState::Unavailable.as_str(),
            "unavailable"
        );
        assert_eq!(
            stable_sql_identity_hash("select marker where region = 'west'"),
            "cbd6889e027b0f88"
        );
    }
}
