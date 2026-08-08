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
