use delta_kernel::Version;

/// Protocol details for one named Delta source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaProtocolReport {
    /// DataFusion table name for this source.
    pub source_name: String,
    /// Sanitized normalized Delta table URI context.
    pub table_uri: String,
    /// Resolved Delta snapshot version.
    pub snapshot_version: Version,
    /// Delta minimum reader protocol version.
    pub min_reader_version: i32,
    /// Delta minimum writer protocol version.
    pub min_writer_version: i32,
    /// Delta reader table features required by this source.
    pub reader_features: Vec<String>,
    /// Delta writer table features advertised by this source.
    pub writer_features: Vec<String>,
}
