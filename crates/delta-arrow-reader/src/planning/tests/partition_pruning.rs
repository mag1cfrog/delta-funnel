use delta_kernel::expressions::{ColumnName, Expression, Predicate, Scalar};
use serde_json::{Value, json};

use super::{DeltaLogTable, PROTOCOL_JSON, plan_scan, planned_tasks};
use crate::{
    DeltaComparison, DeltaPredicate, DeltaReaderError, DeltaReaderExecutionOptions,
    DeltaReaderPhase, DeltaScalar, DeltaSnapshotSelection, DeltaStorageOptions,
    kernel::{DeltaKernelPredicate, delta_predicate_to_kernel_pruning},
    predicate::validate_predicate,
    snapshot::{LoadedDeltaTableSnapshot, load_delta_table_snapshot_blocking},
};

const TIMESTAMP_NTZ_PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["timestampNtz"],"writerFeatures":["timestampNtz"]}}"#;

struct PartitionFixture {
    _table: DeltaLogTable,
    snapshot: LoadedDeltaTableSnapshot,
}

impl PartitionFixture {
    fn new(
        name: &str,
        protocol: &str,
        fields: Vec<Value>,
        partition_columns: &[&str],
        adds: Vec<String>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_protocol_metadata_and_adds(
            name,
            protocol,
            &metadata(fields, partition_columns),
            &adds,
        )?;
        let snapshot = load_delta_table_snapshot_blocking(
            &table.0.to_string_lossy(),
            &DeltaStorageOptions::new(),
            DeltaSnapshotSelection::Latest,
        )?;
        Ok(Self {
            _table: table,
            snapshot,
        })
    }

    fn all_paths(&self) -> Result<Vec<String>, DeltaReaderError> {
        self.kernel_paths(None)
    }

    fn selected_paths(&self, predicate: &DeltaPredicate) -> Result<Vec<String>, DeltaReaderError> {
        validate_predicate(predicate, self.snapshot.schema().as_ref())?;
        let predicate = delta_predicate_to_kernel_pruning(predicate)
            .expect("characterization predicate has an exact Kernel representation");
        self.kernel_paths(Some(predicate))
    }

    fn selected_kernel_paths(&self, predicate: Predicate) -> Result<Vec<String>, DeltaReaderError> {
        self.kernel_paths(Some(DeltaKernelPredicate::from_test_predicate(predicate)))
    }

    fn kernel_paths(
        &self,
        predicate: Option<DeltaKernelPredicate>,
    ) -> Result<Vec<String>, DeltaReaderError> {
        let plan = plan_scan(
            &self.snapshot,
            None,
            &[],
            predicate,
            false,
            DeltaReaderExecutionOptions::default(),
        )?;
        let mut paths = planned_tasks(&plan)
            .map(|task| task.path.clone())
            .collect::<Vec<_>>();
        paths.sort_unstable();
        Ok(paths)
    }
}

fn field(name: &str, data_type: &str) -> Value {
    json!({
        "name": name,
        "type": data_type,
        "nullable": true,
        "metadata": {}
    })
}

fn metadata(fields: Vec<Value>, partition_columns: &[&str]) -> String {
    let schema = json!({"type": "struct", "fields": fields}).to_string();
    json!({
        "metaData": {
            "id": "delta-arrow-reader-partition-parity",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": schema,
            "partitionColumns": partition_columns,
            "configuration": {},
            "createdTime": 1587968585495_i64
        }
    })
    .to_string()
}

fn add(path: &str, partition_values: Value) -> String {
    json!({
        "add": {
            "path": path,
            "partitionValues": partition_values,
            "size": 0,
            "modificationTime": 1587968586000_i64,
            "dataChange": true
        }
    })
    .to_string()
}

fn compare(column: &str, op: DeltaComparison, value: DeltaScalar) -> DeltaPredicate {
    DeltaPredicate::Compare {
        column: column.to_owned(),
        op,
        value,
    }
}

fn is_null(column: &str) -> DeltaPredicate {
    DeltaPredicate::IsNull {
        column: column.to_owned(),
    }
}

fn is_not_null(column: &str) -> DeltaPredicate {
    DeltaPredicate::IsNotNull {
        column: column.to_owned(),
    }
}

fn and(left: DeltaPredicate, right: DeltaPredicate) -> DeltaPredicate {
    DeltaPredicate::And(vec![left, right])
}

fn or(left: DeltaPredicate, right: DeltaPredicate) -> DeltaPredicate {
    DeltaPredicate::Or(vec![left, right])
}

fn not(predicate: DeltaPredicate) -> DeltaPredicate {
    DeltaPredicate::Not(Box::new(predicate))
}

fn in_list(column: &str, values: Vec<DeltaScalar>) -> DeltaPredicate {
    DeltaPredicate::Or(
        values
            .into_iter()
            .map(|value| compare(column, DeltaComparison::Eq, value))
            .collect(),
    )
}

fn not_in_list(column: &str, values: Vec<DeltaScalar>) -> DeltaPredicate {
    DeltaPredicate::And(
        values
            .into_iter()
            .map(|value| compare(column, DeltaComparison::NotEq, value))
            .collect(),
    )
}

fn between(column: &str, low: DeltaScalar, high: DeltaScalar) -> DeltaPredicate {
    and(
        compare(column, DeltaComparison::GtEq, low),
        compare(column, DeltaComparison::LtEq, high),
    )
}

fn not_between(column: &str, low: DeltaScalar, high: DeltaScalar) -> DeltaPredicate {
    or(
        compare(column, DeltaComparison::Lt, low),
        compare(column, DeltaComparison::Gt, high),
    )
}

fn decimal(value: i128) -> DeltaScalar {
    DeltaScalar::Decimal128 {
        value,
        precision: 10,
        scale: 2,
    }
}

fn timestamp(value: i64, timezone: Option<&str>) -> DeltaScalar {
    DeltaScalar::TimestampMicrosecond {
        value,
        timezone: timezone.map(str::to_owned),
    }
}

fn kernel_column(name: &str) -> Expression {
    Expression::Column(ColumnName::new([name]))
}

fn kernel_compare(column: &str, op: DeltaComparison, value: Scalar) -> Predicate {
    let column = kernel_column(column);
    let value = Expression::Literal(value);
    match op {
        DeltaComparison::Eq => Predicate::eq(column, value),
        DeltaComparison::NotEq => Predicate::ne(column, value),
        DeltaComparison::Lt => Predicate::lt(column, value),
        DeltaComparison::LtEq => Predicate::le(column, value),
        DeltaComparison::Gt => Predicate::gt(column, value),
        DeltaComparison::GtEq => Predicate::ge(column, value),
    }
}

fn assert_invalid_partition(error: DeltaReaderError, secret: &str) {
    assert_eq!(error.as_str(), "scan_planning");
    assert_eq!(error.phase(), DeltaReaderPhase::ScanPlanning);
    assert!(!error.to_string().contains(secret));
    assert!(!format!("{error:?}").contains(secret));
}

macro_rules! assert_paths {
    ($fixture:expr, $predicate:expr, [$($path:literal),* $(,)?]) => {
        let expected: Vec<String> = vec![$($path.to_owned()),*];
        assert_eq!(
            $fixture.selected_paths(&$predicate)?,
            expected
        );
    };
}

macro_rules! assert_kernel_paths {
    ($fixture:expr, $predicate:expr, [$($path:literal),* $(,)?]) => {
        assert_eq!(
            $fixture.selected_kernel_paths($predicate)?,
            vec![$($path.to_owned()),*]
        );
    };
}

#[test]
fn string_partition_pruning_matches_the_frozen_characterization()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = PartitionFixture::new(
        "string-partition-parity",
        PROTOCOL_JSON,
        vec![field("id", "integer"), field("region", "string")],
        &["region"],
        vec![
            add("region-us-west.parquet", json!({"region": "us-west"})),
            add("region-us-east.parquet", json!({"region": "us-east"})),
            add("region-null.parquet", json!({"region": null})),
            add("region-missing.parquet", json!({})),
            add("region-empty-string.parquet", json!({"region": ""})),
        ],
    )?;

    assert_eq!(
        fixture.all_paths()?,
        [
            "region-empty-string.parquet",
            "region-missing.parquet",
            "region-null.parquet",
            "region-us-east.parquet",
            "region-us-west.parquet",
        ]
    );
    assert_paths!(
        fixture,
        is_null("region"),
        [
            "region-empty-string.parquet",
            "region-missing.parquet",
            "region-null.parquet",
        ]
    );
    assert_paths!(
        fixture,
        is_not_null("region"),
        ["region-us-east.parquet", "region-us-west.parquet"]
    );
    assert_paths!(
        fixture,
        compare(
            "region",
            DeltaComparison::Eq,
            DeltaScalar::Utf8("us-west".to_owned())
        ),
        ["region-us-west.parquet"]
    );
    assert_paths!(
        fixture,
        compare(
            "region",
            DeltaComparison::NotEq,
            DeltaScalar::Utf8("us-west".to_owned())
        ),
        ["region-us-east.parquet"]
    );
    assert_paths!(
        fixture,
        compare(
            "region",
            DeltaComparison::Eq,
            DeltaScalar::Utf8(String::new())
        ),
        []
    );
    assert_paths!(
        fixture,
        compare(
            "region",
            DeltaComparison::NotEq,
            DeltaScalar::Utf8(String::new())
        ),
        ["region-us-east.parquet", "region-us-west.parquet"]
    );
    assert_paths!(
        fixture,
        in_list(
            "region",
            vec![
                DeltaScalar::Utf8("us-west".to_owned()),
                DeltaScalar::Utf8(String::new()),
            ]
        ),
        ["region-us-west.parquet"]
    );
    assert_paths!(
        fixture,
        not_in_list(
            "region",
            vec![
                DeltaScalar::Utf8("us-west".to_owned()),
                DeltaScalar::Utf8(String::new()),
            ]
        ),
        ["region-us-east.parquet"]
    );
    Ok(())
}

#[test]
fn boolean_partition_pruning_matches_the_frozen_characterization()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = PartitionFixture::new(
        "boolean-partition-parity",
        PROTOCOL_JSON,
        vec![field("id", "integer"), field("is_current", "boolean")],
        &["is_current"],
        vec![
            add("boolean-true.parquet", json!({"is_current": "true"})),
            add("boolean-false.parquet", json!({"is_current": "false"})),
            add("boolean-null.parquet", json!({"is_current": null})),
            add("boolean-empty.parquet", json!({"is_current": ""})),
            add("boolean-missing.parquet", json!({})),
        ],
    )?;

    assert_eq!(
        fixture.all_paths()?,
        [
            "boolean-empty.parquet",
            "boolean-false.parquet",
            "boolean-missing.parquet",
            "boolean-null.parquet",
            "boolean-true.parquet",
        ]
    );
    assert_paths!(
        fixture,
        is_null("is_current"),
        [
            "boolean-empty.parquet",
            "boolean-missing.parquet",
            "boolean-null.parquet",
        ]
    );
    assert_paths!(
        fixture,
        is_not_null("is_current"),
        ["boolean-false.parquet", "boolean-true.parquet"]
    );
    assert_paths!(
        fixture,
        compare(
            "is_current",
            DeltaComparison::Eq,
            DeltaScalar::Boolean(true)
        ),
        ["boolean-true.parquet"]
    );
    assert_paths!(
        fixture,
        compare(
            "is_current",
            DeltaComparison::Eq,
            DeltaScalar::Boolean(false)
        ),
        ["boolean-false.parquet"]
    );
    assert_paths!(
        fixture,
        compare(
            "is_current",
            DeltaComparison::NotEq,
            DeltaScalar::Boolean(true)
        ),
        ["boolean-false.parquet"]
    );
    assert_paths!(
        fixture,
        in_list(
            "is_current",
            vec![DeltaScalar::Boolean(true), DeltaScalar::Boolean(false)]
        ),
        ["boolean-false.parquet", "boolean-true.parquet"]
    );
    assert_paths!(
        fixture,
        not_in_list("is_current", vec![DeltaScalar::Boolean(true)]),
        ["boolean-false.parquet"]
    );
    assert_paths!(
        fixture,
        or(
            compare(
                "is_current",
                DeltaComparison::Eq,
                DeltaScalar::Boolean(true)
            ),
            is_null("is_current")
        ),
        [
            "boolean-empty.parquet",
            "boolean-missing.parquet",
            "boolean-null.parquet",
            "boolean-true.parquet",
        ]
    );
    assert_paths!(
        fixture,
        and(
            compare(
                "is_current",
                DeltaComparison::Eq,
                DeltaScalar::Boolean(true)
            ),
            is_not_null("is_current")
        ),
        ["boolean-true.parquet"]
    );
    assert_paths!(
        fixture,
        not(compare(
            "is_current",
            DeltaComparison::Eq,
            DeltaScalar::Boolean(true)
        )),
        ["boolean-false.parquet"]
    );

    let invalid = PartitionFixture::new(
        "invalid-boolean-partition-parity",
        PROTOCOL_JSON,
        vec![field("id", "integer"), field("is_current", "boolean")],
        &["is_current"],
        vec![
            add("boolean-valid.parquet", json!({"is_current": "true"})),
            add(
                "boolean-invalid.parquet",
                json!({"is_current": "not-a-boolean"}),
            ),
        ],
    )?;
    assert_invalid_partition(invalid.all_paths().unwrap_err(), "not-a-boolean");
    assert_invalid_partition(
        invalid
            .selected_paths(&compare(
                "is_current",
                DeltaComparison::Eq,
                DeltaScalar::Boolean(true),
            ))
            .unwrap_err(),
        "not-a-boolean",
    );
    Ok(())
}

#[test]
fn date_partition_pruning_matches_the_frozen_characterization()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = PartitionFixture::new(
        "date-partition-parity",
        PROTOCOL_JSON,
        vec![field("id", "integer"), field("event_date", "date")],
        &["event_date"],
        vec![
            add(
                "date-pre-epoch.parquet",
                json!({"event_date": "1969-12-31"}),
            ),
            add("date-epoch.parquet", json!({"event_date": "1970-01-01"})),
            add("date-leap-day.parquet", json!({"event_date": "2024-02-29"})),
            add("date-new-year.parquet", json!({"event_date": "2026-01-01"})),
            add("date-null.parquet", json!({"event_date": null})),
            add("date-empty.parquet", json!({"event_date": ""})),
            add("date-missing.parquet", json!({})),
        ],
    )?;

    assert_eq!(
        fixture.all_paths()?,
        [
            "date-empty.parquet",
            "date-epoch.parquet",
            "date-leap-day.parquet",
            "date-missing.parquet",
            "date-new-year.parquet",
            "date-null.parquet",
            "date-pre-epoch.parquet",
        ]
    );
    assert_paths!(
        fixture,
        compare(
            "event_date",
            DeltaComparison::Gt,
            DeltaScalar::Date32(19_782)
        ),
        ["date-new-year.parquet"]
    );
    assert_paths!(
        fixture,
        compare("event_date", DeltaComparison::LtEq, DeltaScalar::Date32(0)),
        ["date-epoch.parquet", "date-pre-epoch.parquet"]
    );
    assert_paths!(
        fixture,
        compare(
            "event_date",
            DeltaComparison::Lt,
            DeltaScalar::Date32(20_454)
        ),
        [
            "date-epoch.parquet",
            "date-leap-day.parquet",
            "date-pre-epoch.parquet",
        ]
    );
    assert_paths!(
        fixture,
        is_null("event_date"),
        [
            "date-empty.parquet",
            "date-missing.parquet",
            "date-null.parquet",
        ]
    );
    assert_paths!(
        fixture,
        is_not_null("event_date"),
        [
            "date-epoch.parquet",
            "date-leap-day.parquet",
            "date-new-year.parquet",
            "date-pre-epoch.parquet",
        ]
    );
    assert_paths!(
        fixture,
        compare(
            "event_date",
            DeltaComparison::Eq,
            DeltaScalar::Date32(20_454)
        ),
        ["date-new-year.parquet"]
    );
    assert_paths!(
        fixture,
        compare(
            "event_date",
            DeltaComparison::NotEq,
            DeltaScalar::Date32(20_454)
        ),
        [
            "date-epoch.parquet",
            "date-leap-day.parquet",
            "date-pre-epoch.parquet",
        ]
    );
    assert_paths!(
        fixture,
        in_list(
            "event_date",
            vec![DeltaScalar::Date32(-1), DeltaScalar::Date32(20_454)]
        ),
        ["date-new-year.parquet", "date-pre-epoch.parquet"]
    );
    assert_paths!(
        fixture,
        not_in_list("event_date", vec![DeltaScalar::Date32(19_782)]),
        [
            "date-epoch.parquet",
            "date-new-year.parquet",
            "date-pre-epoch.parquet",
        ]
    );
    assert_paths!(
        fixture,
        between(
            "event_date",
            DeltaScalar::Date32(-1),
            DeltaScalar::Date32(19_782)
        ),
        [
            "date-epoch.parquet",
            "date-leap-day.parquet",
            "date-pre-epoch.parquet",
        ]
    );
    assert_paths!(
        fixture,
        not_between(
            "event_date",
            DeltaScalar::Date32(-1),
            DeltaScalar::Date32(19_782)
        ),
        ["date-new-year.parquet"]
    );
    assert_paths!(
        fixture,
        and(
            compare("event_date", DeltaComparison::GtEq, DeltaScalar::Date32(0)),
            compare(
                "event_date",
                DeltaComparison::Lt,
                DeltaScalar::Date32(20_454)
            )
        ),
        ["date-epoch.parquet", "date-leap-day.parquet"]
    );
    assert_paths!(
        fixture,
        or(
            compare("event_date", DeltaComparison::Eq, DeltaScalar::Date32(-1)),
            compare(
                "event_date",
                DeltaComparison::Eq,
                DeltaScalar::Date32(20_454)
            )
        ),
        ["date-new-year.parquet", "date-pre-epoch.parquet"]
    );
    assert_paths!(
        fixture,
        not(compare(
            "event_date",
            DeltaComparison::Eq,
            DeltaScalar::Date32(19_782)
        )),
        [
            "date-epoch.parquet",
            "date-new-year.parquet",
            "date-pre-epoch.parquet",
        ]
    );

    let invalid = PartitionFixture::new(
        "invalid-date-partition-parity",
        PROTOCOL_JSON,
        vec![field("id", "integer"), field("event_date", "date")],
        &["event_date"],
        vec![
            add("date-valid.parquet", json!({"event_date": "2026-01-01"})),
            add("date-invalid.parquet", json!({"event_date": "not-a-date"})),
        ],
    )?;
    assert_invalid_partition(invalid.all_paths().unwrap_err(), "not-a-date");
    assert_invalid_partition(
        invalid
            .selected_paths(&compare(
                "event_date",
                DeltaComparison::Eq,
                DeltaScalar::Date32(20_454),
            ))
            .unwrap_err(),
        "not-a-date",
    );
    Ok(())
}

#[test]
fn decimal_partition_pruning_matches_the_frozen_characterization()
-> Result<(), Box<dyn std::error::Error>> {
    let fields = vec![field("id", "integer"), field("amount", "decimal(10,2)")];
    let fixture = PartitionFixture::new(
        "decimal-partition-parity",
        PROTOCOL_JSON,
        fields.clone(),
        &["amount"],
        vec![
            add("decimal-negative.parquet", json!({"amount": "-1.23"})),
            add("decimal-zero.parquet", json!({"amount": "0.00"})),
            add("decimal-two.parquet", json!({"amount": "2.00"})),
            add("decimal-ten.parquet", json!({"amount": "10.00"})),
            add("decimal-large.parquet", json!({"amount": "123.45"})),
            add("decimal-null.parquet", json!({"amount": null})),
            add("decimal-empty.parquet", json!({"amount": ""})),
            add("decimal-missing.parquet", json!({})),
        ],
    )?;

    assert_eq!(
        fixture.all_paths()?,
        [
            "decimal-empty.parquet",
            "decimal-large.parquet",
            "decimal-missing.parquet",
            "decimal-negative.parquet",
            "decimal-null.parquet",
            "decimal-ten.parquet",
            "decimal-two.parquet",
            "decimal-zero.parquet",
        ]
    );
    assert_paths!(
        fixture,
        compare("amount", DeltaComparison::Gt, decimal(200)),
        ["decimal-large.parquet", "decimal-ten.parquet"]
    );
    assert_paths!(
        fixture,
        compare("amount", DeltaComparison::Lt, decimal(1_000)),
        [
            "decimal-negative.parquet",
            "decimal-two.parquet",
            "decimal-zero.parquet",
        ]
    );
    assert_paths!(
        fixture,
        is_null("amount"),
        [
            "decimal-empty.parquet",
            "decimal-missing.parquet",
            "decimal-null.parquet",
        ]
    );
    assert_paths!(
        fixture,
        is_not_null("amount"),
        [
            "decimal-large.parquet",
            "decimal-negative.parquet",
            "decimal-ten.parquet",
            "decimal-two.parquet",
            "decimal-zero.parquet",
        ]
    );
    assert_paths!(
        fixture,
        compare("amount", DeltaComparison::Eq, decimal(12_345)),
        ["decimal-large.parquet"]
    );
    assert_paths!(
        fixture,
        compare("amount", DeltaComparison::NotEq, decimal(12_345)),
        [
            "decimal-negative.parquet",
            "decimal-ten.parquet",
            "decimal-two.parquet",
            "decimal-zero.parquet",
        ]
    );
    assert_paths!(
        fixture,
        in_list("amount", vec![decimal(-123), decimal(12_345)]),
        ["decimal-large.parquet", "decimal-negative.parquet"]
    );
    assert_paths!(
        fixture,
        not_in_list("amount", vec![decimal(200)]),
        [
            "decimal-large.parquet",
            "decimal-negative.parquet",
            "decimal-ten.parquet",
            "decimal-zero.parquet",
        ]
    );
    assert_paths!(
        fixture,
        between("amount", decimal(-123), decimal(200)),
        [
            "decimal-negative.parquet",
            "decimal-two.parquet",
            "decimal-zero.parquet",
        ]
    );
    assert_paths!(
        fixture,
        not_between("amount", decimal(-123), decimal(200)),
        ["decimal-large.parquet", "decimal-ten.parquet"]
    );
    assert_paths!(
        fixture,
        and(
            compare("amount", DeltaComparison::GtEq, decimal(0)),
            compare("amount", DeltaComparison::Lt, decimal(12_345))
        ),
        [
            "decimal-ten.parquet",
            "decimal-two.parquet",
            "decimal-zero.parquet",
        ]
    );
    assert_paths!(
        fixture,
        or(
            compare("amount", DeltaComparison::Eq, decimal(-123)),
            compare("amount", DeltaComparison::Eq, decimal(12_345))
        ),
        ["decimal-large.parquet", "decimal-negative.parquet"]
    );
    assert_paths!(
        fixture,
        not(compare("amount", DeltaComparison::Eq, decimal(200))),
        [
            "decimal-large.parquet",
            "decimal-negative.parquet",
            "decimal-ten.parquet",
            "decimal-zero.parquet",
        ]
    );

    for (name, invalid_value) in [
        ("invalid-decimal-partition-parity", "not-a-decimal"),
        ("invalid-decimal-scale-partition-parity", "123.450"),
    ] {
        let invalid = PartitionFixture::new(
            name,
            PROTOCOL_JSON,
            fields.clone(),
            &["amount"],
            vec![
                add("decimal-valid.parquet", json!({"amount": "123.45"})),
                add("decimal-invalid.parquet", json!({"amount": invalid_value})),
            ],
        )?;
        if invalid_value == "not-a-decimal" {
            assert_invalid_partition(invalid.all_paths().unwrap_err(), invalid_value);
        }
        assert_invalid_partition(
            invalid
                .selected_paths(&compare("amount", DeltaComparison::Eq, decimal(12_345)))
                .unwrap_err(),
            invalid_value,
        );
    }
    Ok(())
}

#[test]
fn floating_partition_pruning_matches_the_frozen_characterization()
-> Result<(), Box<dyn std::error::Error>> {
    let fields = vec![
        field("id", "integer"),
        field("float_part", "float"),
        field("double_part", "double"),
    ];
    let fixture = PartitionFixture::new(
        "floating-partition-parity",
        PROTOCOL_JSON,
        fields.clone(),
        &["float_part", "double_part"],
        vec![
            add(
                "floating-neg.parquet",
                json!({"float_part": "-1.5", "double_part": "-2.25"}),
            ),
            add(
                "floating-neg-zero.parquet",
                json!({"float_part": "-0.0", "double_part": "-0.0"}),
            ),
            add(
                "floating-pos-zero.parquet",
                json!({"float_part": "0.0", "double_part": "0.0"}),
            ),
            add(
                "floating-one.parquet",
                json!({"float_part": "1.5", "double_part": "2.25"}),
            ),
            add(
                "floating-ten.parquet",
                json!({"float_part": "10.0", "double_part": "10.0"}),
            ),
            add(
                "floating-null.parquet",
                json!({"float_part": null, "double_part": null}),
            ),
            add(
                "floating-empty.parquet",
                json!({"float_part": "", "double_part": ""}),
            ),
            add("floating-missing.parquet", json!({})),
        ],
    )?;

    assert_eq!(
        fixture.all_paths()?,
        [
            "floating-empty.parquet",
            "floating-missing.parquet",
            "floating-neg-zero.parquet",
            "floating-neg.parquet",
            "floating-null.parquet",
            "floating-one.parquet",
            "floating-pos-zero.parquet",
            "floating-ten.parquet",
        ]
    );
    assert_paths!(
        fixture,
        compare("float_part", DeltaComparison::Gt, DeltaScalar::Float32(1.5)),
        ["floating-ten.parquet"]
    );
    assert_kernel_paths!(
        fixture,
        kernel_compare("double_part", DeltaComparison::Lt, Scalar::Double(0.0)),
        ["floating-neg-zero.parquet", "floating-neg.parquet"]
    );
    assert_paths!(
        fixture,
        compare(
            "float_part",
            DeltaComparison::Lt,
            DeltaScalar::Float32(10.0)
        ),
        [
            "floating-neg-zero.parquet",
            "floating-neg.parquet",
            "floating-one.parquet",
            "floating-pos-zero.parquet",
        ]
    );
    assert_paths!(
        fixture,
        is_null("float_part"),
        [
            "floating-empty.parquet",
            "floating-missing.parquet",
            "floating-null.parquet",
        ]
    );
    assert_paths!(
        fixture,
        is_not_null("double_part"),
        [
            "floating-neg-zero.parquet",
            "floating-neg.parquet",
            "floating-one.parquet",
            "floating-pos-zero.parquet",
            "floating-ten.parquet",
        ]
    );
    assert_kernel_paths!(
        fixture,
        kernel_compare("float_part", DeltaComparison::Eq, Scalar::Float(-0.0)),
        ["floating-neg-zero.parquet"]
    );
    assert_kernel_paths!(
        fixture,
        kernel_compare("float_part", DeltaComparison::Eq, Scalar::Float(0.0)),
        ["floating-pos-zero.parquet"]
    );
    assert_kernel_paths!(
        fixture,
        kernel_compare("double_part", DeltaComparison::NotEq, Scalar::Double(0.0)),
        [
            "floating-neg-zero.parquet",
            "floating-neg.parquet",
            "floating-one.parquet",
            "floating-ten.parquet",
        ]
    );
    assert_paths!(
        fixture,
        in_list(
            "float_part",
            vec![DeltaScalar::Float32(-1.5), DeltaScalar::Float32(1.5)]
        ),
        ["floating-neg.parquet", "floating-one.parquet"]
    );
    assert_paths!(
        fixture,
        not_in_list("double_part", vec![DeltaScalar::Float64(2.25)]),
        [
            "floating-neg-zero.parquet",
            "floating-neg.parquet",
            "floating-pos-zero.parquet",
            "floating-ten.parquet",
        ]
    );
    assert_kernel_paths!(
        fixture,
        Predicate::and(
            kernel_compare("float_part", DeltaComparison::GtEq, Scalar::Float(-0.0)),
            kernel_compare("float_part", DeltaComparison::LtEq, Scalar::Float(1.5)),
        ),
        [
            "floating-neg-zero.parquet",
            "floating-one.parquet",
            "floating-pos-zero.parquet",
        ]
    );
    assert_kernel_paths!(
        fixture,
        Predicate::or(
            kernel_compare("double_part", DeltaComparison::Lt, Scalar::Double(0.0)),
            kernel_compare("double_part", DeltaComparison::Gt, Scalar::Double(2.25)),
        ),
        [
            "floating-neg-zero.parquet",
            "floating-neg.parquet",
            "floating-ten.parquet",
        ]
    );
    assert_kernel_paths!(
        fixture,
        Predicate::and(
            kernel_compare("float_part", DeltaComparison::GtEq, Scalar::Float(0.0)),
            kernel_compare("float_part", DeltaComparison::Lt, Scalar::Float(10.0)),
        ),
        ["floating-one.parquet", "floating-pos-zero.parquet"]
    );
    assert_paths!(
        fixture,
        or(
            compare(
                "double_part",
                DeltaComparison::Eq,
                DeltaScalar::Float64(-2.25)
            ),
            compare(
                "double_part",
                DeltaComparison::Eq,
                DeltaScalar::Float64(10.0)
            )
        ),
        ["floating-neg.parquet", "floating-ten.parquet"]
    );
    assert_paths!(
        fixture,
        not(compare(
            "float_part",
            DeltaComparison::Eq,
            DeltaScalar::Float32(1.5)
        )),
        [
            "floating-neg-zero.parquet",
            "floating-neg.parquet",
            "floating-pos-zero.parquet",
            "floating-ten.parquet",
        ]
    );

    let invalid = PartitionFixture::new(
        "invalid-floating-partition-parity",
        PROTOCOL_JSON,
        fields.clone(),
        &["float_part", "double_part"],
        vec![
            add(
                "floating-valid.parquet",
                json!({"float_part": "1.5", "double_part": "2.25"}),
            ),
            add(
                "floating-invalid.parquet",
                json!({"float_part": "not-a-float", "double_part": "not-a-double"}),
            ),
        ],
    )?;
    assert_invalid_partition(invalid.all_paths().unwrap_err(), "not-a-float");
    assert_invalid_partition(
        invalid
            .selected_paths(&compare(
                "float_part",
                DeltaComparison::Eq,
                DeltaScalar::Float32(1.5),
            ))
            .unwrap_err(),
        "not-a-float",
    );

    let nonfinite = PartitionFixture::new(
        "nonfinite-floating-partition-parity",
        PROTOCOL_JSON,
        fields,
        &["float_part", "double_part"],
        vec![
            add(
                "floating-valid.parquet",
                json!({"float_part": "1.5", "double_part": "2.25"}),
            ),
            add(
                "floating-nan.parquet",
                json!({"float_part": "NaN", "double_part": "NaN"}),
            ),
            add(
                "floating-inf.parquet",
                json!({"float_part": "Infinity", "double_part": "Infinity"}),
            ),
            add(
                "floating-neg-inf.parquet",
                json!({"float_part": "-Infinity", "double_part": "-Infinity"}),
            ),
        ],
    )?;
    assert_eq!(
        nonfinite.all_paths()?,
        [
            "floating-inf.parquet",
            "floating-nan.parquet",
            "floating-neg-inf.parquet",
            "floating-valid.parquet",
        ]
    );
    assert_kernel_paths!(
        nonfinite,
        kernel_compare("float_part", DeltaComparison::Gt, Scalar::Float(0.0)),
        [
            "floating-inf.parquet",
            "floating-nan.parquet",
            "floating-valid.parquet",
        ]
    );
    assert_kernel_paths!(
        nonfinite,
        kernel_compare("double_part", DeltaComparison::Lt, Scalar::Double(0.0)),
        ["floating-neg-inf.parquet"]
    );
    assert_kernel_paths!(
        nonfinite,
        kernel_compare("float_part", DeltaComparison::Eq, Scalar::Float(f32::NAN)),
        ["floating-nan.parquet"]
    );
    Ok(())
}

#[test]
fn binary_partition_pruning_matches_the_frozen_characterization()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = PartitionFixture::new(
        "binary-partition-parity",
        PROTOCOL_JSON,
        vec![field("id", "integer"), field("payload", "binary")],
        &["payload"],
        vec![
            add("binary-HELLO.parquet", json!({"payload": "HELLO"})),
            add("binary-hello.parquet", json!({"payload": "hello"})),
            add("binary-special.parquet", json!({"payload": "/=%"})),
            add("binary-null.parquet", json!({"payload": null})),
            add("binary-empty.parquet", json!({"payload": ""})),
            add("binary-missing.parquet", json!({})),
        ],
    )?;

    assert_eq!(
        fixture.all_paths()?,
        [
            "binary-HELLO.parquet",
            "binary-empty.parquet",
            "binary-hello.parquet",
            "binary-missing.parquet",
            "binary-null.parquet",
            "binary-special.parquet",
        ]
    );
    assert_paths!(
        fixture,
        is_null("payload"),
        [
            "binary-empty.parquet",
            "binary-missing.parquet",
            "binary-null.parquet",
        ]
    );
    assert_paths!(
        fixture,
        is_not_null("payload"),
        [
            "binary-HELLO.parquet",
            "binary-hello.parquet",
            "binary-special.parquet",
        ]
    );
    for (value, expected) in [
        (b"HELLO".to_vec(), vec!["binary-HELLO.parquet"]),
        (b"hello".to_vec(), vec!["binary-hello.parquet"]),
        (Vec::new(), vec![]),
    ] {
        assert_eq!(
            fixture.selected_paths(&compare(
                "payload",
                DeltaComparison::Eq,
                DeltaScalar::Binary(value),
            ))?,
            expected
        );
    }
    assert_paths!(
        fixture,
        compare(
            "payload",
            DeltaComparison::NotEq,
            DeltaScalar::Binary(b"hello".to_vec())
        ),
        ["binary-HELLO.parquet", "binary-special.parquet"]
    );
    assert_paths!(
        fixture,
        compare(
            "payload",
            DeltaComparison::NotEq,
            DeltaScalar::Binary(Vec::new())
        ),
        [
            "binary-HELLO.parquet",
            "binary-hello.parquet",
            "binary-special.parquet",
        ]
    );
    assert_paths!(
        fixture,
        in_list(
            "payload",
            vec![
                DeltaScalar::Binary(b"HELLO".to_vec()),
                DeltaScalar::Binary(b"/=%".to_vec()),
            ]
        ),
        ["binary-HELLO.parquet", "binary-special.parquet"]
    );
    assert_paths!(
        fixture,
        not_in_list(
            "payload",
            vec![
                DeltaScalar::Binary(b"hello".to_vec()),
                DeltaScalar::Binary(b"/=%".to_vec()),
            ]
        ),
        ["binary-HELLO.parquet"]
    );
    assert_paths!(
        fixture,
        and(
            is_not_null("payload"),
            compare(
                "payload",
                DeltaComparison::NotEq,
                DeltaScalar::Binary(b"HELLO".to_vec())
            )
        ),
        ["binary-hello.parquet", "binary-special.parquet"]
    );
    assert_paths!(
        fixture,
        or(
            compare(
                "payload",
                DeltaComparison::Eq,
                DeltaScalar::Binary(b"HELLO".to_vec())
            ),
            is_null("payload")
        ),
        [
            "binary-HELLO.parquet",
            "binary-empty.parquet",
            "binary-missing.parquet",
            "binary-null.parquet",
        ]
    );
    assert_paths!(
        fixture,
        not(compare(
            "payload",
            DeltaComparison::Eq,
            DeltaScalar::Binary(b"hello".to_vec())
        )),
        ["binary-HELLO.parquet", "binary-special.parquet"]
    );
    assert_paths!(
        fixture,
        compare(
            "payload",
            DeltaComparison::Eq,
            DeltaScalar::Binary(vec![0xDE, 0xAD, 0xBE, 0xEF])
        ),
        []
    );
    assert_paths!(
        fixture,
        compare(
            "payload",
            DeltaComparison::NotEq,
            DeltaScalar::Binary(vec![0xDE, 0xAD, 0xBE, 0xEF])
        ),
        [
            "binary-HELLO.parquet",
            "binary-hello.parquet",
            "binary-special.parquet",
        ]
    );
    Ok(())
}

fn assert_timestamp_characterization(
    fixture: &PartitionFixture,
    column: &str,
    timezone: Option<&str>,
    paths: [&str; 7],
) -> Result<(), DeltaReaderError> {
    let [
        empty,
        high_path,
        low_path,
        missing,
        null,
        pre_epoch_path,
        target_path,
    ] = paths;
    let pre_epoch = -1_i64;
    let low = 1_767_225_599_999_999_i64;
    let target = 1_767_225_600_123_456_i64;
    let high = 1_767_225_600_123_457_i64;

    assert_eq!(
        fixture.all_paths()?,
        [
            empty,
            high_path,
            low_path,
            missing,
            null,
            pre_epoch_path,
            target_path
        ]
    );
    assert_eq!(
        fixture.selected_paths(&compare(
            column,
            DeltaComparison::Lt,
            timestamp(target, timezone),
        ))?,
        [low_path, pre_epoch_path]
    );
    assert_eq!(
        fixture.selected_paths(&compare(
            column,
            DeltaComparison::GtEq,
            timestamp(target, timezone),
        ))?,
        [high_path, target_path]
    );
    assert_eq!(
        fixture.selected_paths(&compare(
            column,
            DeltaComparison::Lt,
            timestamp(high, timezone),
        ))?,
        [low_path, pre_epoch_path, target_path]
    );
    assert_eq!(
        fixture.selected_paths(&compare(
            column,
            DeltaComparison::Eq,
            timestamp(pre_epoch, timezone),
        ))?,
        [pre_epoch_path]
    );
    assert_eq!(
        fixture.selected_paths(&compare(
            column,
            DeltaComparison::Eq,
            timestamp(target, timezone),
        ))?,
        [target_path]
    );
    assert_eq!(
        fixture.selected_paths(&compare(
            column,
            DeltaComparison::NotEq,
            timestamp(target, timezone),
        ))?,
        [high_path, low_path, pre_epoch_path]
    );
    assert_eq!(
        fixture.selected_paths(&compare(
            column,
            DeltaComparison::Eq,
            timestamp(high, timezone),
        ))?,
        [high_path]
    );
    assert_eq!(
        fixture.selected_paths(&compare(
            column,
            DeltaComparison::Eq,
            timestamp(low, timezone),
        ))?,
        [low_path]
    );
    assert_eq!(
        fixture.selected_paths(&is_null(column))?,
        [empty, missing, null]
    );
    assert_eq!(
        fixture.selected_paths(&is_not_null(column))?,
        [high_path, low_path, pre_epoch_path, target_path]
    );
    assert_eq!(
        fixture.selected_paths(&in_list(
            column,
            vec![timestamp(low, timezone), timestamp(target, timezone)],
        ))?,
        [low_path, target_path]
    );
    assert_eq!(
        fixture.selected_paths(&not_in_list(
            column,
            vec![timestamp(low, timezone), timestamp(target, timezone)],
        ))?,
        [high_path, pre_epoch_path]
    );
    assert_eq!(
        fixture.selected_paths(&between(
            column,
            timestamp(low, timezone),
            timestamp(target, timezone),
        ))?,
        [low_path, target_path]
    );
    assert_eq!(
        fixture.selected_paths(&not_between(
            column,
            timestamp(low, timezone),
            timestamp(target, timezone),
        ))?,
        [high_path, pre_epoch_path]
    );
    assert_eq!(
        fixture.selected_paths(&and(
            compare(column, DeltaComparison::Gt, timestamp(low, timezone)),
            compare(column, DeltaComparison::LtEq, timestamp(high, timezone)),
        ))?,
        [high_path, target_path]
    );
    assert_eq!(
        fixture.selected_paths(&or(
            compare(column, DeltaComparison::Eq, timestamp(low, timezone)),
            is_null(column),
        ))?,
        [empty, low_path, missing, null]
    );
    assert_eq!(
        fixture.selected_paths(&not(compare(
            column,
            DeltaComparison::Eq,
            timestamp(target, timezone),
        )))?,
        [high_path, low_path, pre_epoch_path]
    );
    Ok(())
}

#[test]
fn timestamp_partition_pruning_matches_the_frozen_characterization()
-> Result<(), Box<dyn std::error::Error>> {
    let fields = vec![field("id", "integer"), field("event_ts", "timestamp")];
    let fixture = PartitionFixture::new(
        "timestamp-partition-parity",
        PROTOCOL_JSON,
        fields.clone(),
        &["event_ts"],
        vec![
            add(
                "timestamp-pre-epoch.parquet",
                json!({"event_ts": "1969-12-31T23:59:59.999999Z"}),
            ),
            add(
                "timestamp-low.parquet",
                json!({"event_ts": "2025-12-31T23:59:59.999999Z"}),
            ),
            add(
                "timestamp-target.parquet",
                json!({"event_ts": "2026-01-01T00:00:00.123456Z"}),
            ),
            add(
                "timestamp-high.parquet",
                json!({"event_ts": "2026-01-01T00:00:00.123457Z"}),
            ),
            add("timestamp-null.parquet", json!({"event_ts": null})),
            add("timestamp-empty.parquet", json!({"event_ts": ""})),
            add("timestamp-missing.parquet", json!({})),
        ],
    )?;
    assert_timestamp_characterization(
        &fixture,
        "event_ts",
        Some("UTC"),
        [
            "timestamp-empty.parquet",
            "timestamp-high.parquet",
            "timestamp-low.parquet",
            "timestamp-missing.parquet",
            "timestamp-null.parquet",
            "timestamp-pre-epoch.parquet",
            "timestamp-target.parquet",
        ],
    )?;

    let invalid = PartitionFixture::new(
        "invalid-timestamp-partition-parity",
        PROTOCOL_JSON,
        fields,
        &["event_ts"],
        vec![
            add(
                "timestamp-valid.parquet",
                json!({"event_ts": "2026-01-01T00:00:00.123456Z"}),
            ),
            add(
                "timestamp-invalid.parquet",
                json!({"event_ts": "not-a-timestamp"}),
            ),
        ],
    )?;
    assert_invalid_partition(invalid.all_paths().unwrap_err(), "not-a-timestamp");
    assert_invalid_partition(
        invalid
            .selected_paths(&compare(
                "event_ts",
                DeltaComparison::Eq,
                timestamp(1_767_225_600_123_456, Some("UTC")),
            ))
            .unwrap_err(),
        "not-a-timestamp",
    );
    Ok(())
}

#[test]
fn timestamp_ntz_partition_pruning_matches_the_frozen_characterization()
-> Result<(), Box<dyn std::error::Error>> {
    let fields = vec![
        field("id", "integer"),
        field("event_ts_ntz", "timestamp_ntz"),
    ];
    let fixture = PartitionFixture::new(
        "timestamp-ntz-partition-parity",
        TIMESTAMP_NTZ_PROTOCOL_JSON,
        fields.clone(),
        &["event_ts_ntz"],
        vec![
            add(
                "timestamp-ntz-pre-epoch.parquet",
                json!({"event_ts_ntz": "1969-12-31 23:59:59.999999"}),
            ),
            add(
                "timestamp-ntz-low-space.parquet",
                json!({"event_ts_ntz": "2025-12-31 23:59:59.999999"}),
            ),
            add(
                "timestamp-ntz-target-space.parquet",
                json!({"event_ts_ntz": "2026-01-01 00:00:00.123456"}),
            ),
            add(
                "timestamp-ntz-high.parquet",
                json!({"event_ts_ntz": "2026-01-01 00:00:00.123457"}),
            ),
            add("timestamp-ntz-null.parquet", json!({"event_ts_ntz": null})),
            add("timestamp-ntz-empty.parquet", json!({"event_ts_ntz": ""})),
            add("timestamp-ntz-missing.parquet", json!({})),
        ],
    )?;
    assert_timestamp_characterization(
        &fixture,
        "event_ts_ntz",
        None,
        [
            "timestamp-ntz-empty.parquet",
            "timestamp-ntz-high.parquet",
            "timestamp-ntz-low-space.parquet",
            "timestamp-ntz-missing.parquet",
            "timestamp-ntz-null.parquet",
            "timestamp-ntz-pre-epoch.parquet",
            "timestamp-ntz-target-space.parquet",
        ],
    )?;

    for (name, invalid_value) in [
        ("invalid-timestamp-ntz-partition-parity", "not-a-timestamp"),
        (
            "t-separator-timestamp-ntz-partition-parity",
            "2026-01-01T00:00:00.123456",
        ),
        (
            "zone-timestamp-ntz-partition-parity",
            "2026-01-01T00:00:00.123456Z",
        ),
    ] {
        let invalid = PartitionFixture::new(
            name,
            TIMESTAMP_NTZ_PROTOCOL_JSON,
            fields.clone(),
            &["event_ts_ntz"],
            vec![
                add(
                    "timestamp-ntz-valid.parquet",
                    json!({"event_ts_ntz": "2026-01-01 00:00:00.123456"}),
                ),
                add(
                    "timestamp-ntz-invalid.parquet",
                    json!({"event_ts_ntz": invalid_value}),
                ),
            ],
        )?;
        assert_invalid_partition(invalid.all_paths().unwrap_err(), invalid_value);
        assert_invalid_partition(
            invalid
                .selected_paths(&compare(
                    "event_ts_ntz",
                    DeltaComparison::Eq,
                    timestamp(1_767_225_600_123_456, None),
                ))
                .unwrap_err(),
            invalid_value,
        );
    }
    Ok(())
}

#[test]
fn integer_partition_pruning_matches_the_frozen_characterization()
-> Result<(), Box<dyn std::error::Error>> {
    let fields = vec![
        field("id", "integer"),
        field("byte_part", "byte"),
        field("short_part", "short"),
        field("int_part", "integer"),
        field("long_part", "long"),
    ];
    let integer_add = |path: &str, value: &str| {
        add(
            path,
            json!({
                "byte_part": value,
                "short_part": value,
                "int_part": value,
                "long_part": value,
            }),
        )
    };
    let fixture = PartitionFixture::new(
        "integer-partition-parity",
        PROTOCOL_JSON,
        fields.clone(),
        &["byte_part", "short_part", "int_part", "long_part"],
        vec![
            integer_add("integer--1.parquet", "-1"),
            integer_add("integer-2.parquet", "2"),
            integer_add("integer-10.parquet", "10"),
            add(
                "integer-null.parquet",
                json!({
                    "byte_part": null,
                    "short_part": null,
                    "int_part": null,
                    "long_part": null,
                }),
            ),
            integer_add("integer-empty.parquet", ""),
            add("integer-missing.parquet", json!({})),
        ],
    )?;

    assert_eq!(
        fixture.all_paths()?,
        [
            "integer--1.parquet",
            "integer-10.parquet",
            "integer-2.parquet",
            "integer-empty.parquet",
            "integer-missing.parquet",
            "integer-null.parquet",
        ]
    );
    assert_paths!(
        fixture,
        compare("long_part", DeltaComparison::Gt, DeltaScalar::Int64(2)),
        ["integer-10.parquet"]
    );
    assert_paths!(
        fixture,
        compare("long_part", DeltaComparison::Lt, DeltaScalar::Int64(10)),
        ["integer--1.parquet", "integer-2.parquet"]
    );
    assert_paths!(
        fixture,
        is_null("long_part"),
        [
            "integer-empty.parquet",
            "integer-missing.parquet",
            "integer-null.parquet",
        ]
    );
    assert_paths!(
        fixture,
        is_not_null("long_part"),
        [
            "integer--1.parquet",
            "integer-10.parquet",
            "integer-2.parquet",
        ]
    );
    assert_paths!(
        fixture,
        compare("long_part", DeltaComparison::Eq, DeltaScalar::Int64(2)),
        ["integer-2.parquet"]
    );
    assert_paths!(
        fixture,
        compare("long_part", DeltaComparison::NotEq, DeltaScalar::Int64(2)),
        ["integer--1.parquet", "integer-10.parquet"]
    );
    assert_paths!(
        fixture,
        in_list(
            "long_part",
            vec![DeltaScalar::Int64(-1), DeltaScalar::Int64(10)]
        ),
        ["integer--1.parquet", "integer-10.parquet"]
    );
    assert_paths!(
        fixture,
        not_in_list("long_part", vec![DeltaScalar::Int64(2)]),
        ["integer--1.parquet", "integer-10.parquet"]
    );
    assert_paths!(
        fixture,
        between("long_part", DeltaScalar::Int64(-1), DeltaScalar::Int64(2)),
        ["integer--1.parquet", "integer-2.parquet"]
    );
    assert_paths!(
        fixture,
        not_between("long_part", DeltaScalar::Int64(-1), DeltaScalar::Int64(2)),
        ["integer-10.parquet"]
    );
    assert_paths!(
        fixture,
        and(
            compare("long_part", DeltaComparison::GtEq, DeltaScalar::Int64(-1)),
            compare("long_part", DeltaComparison::Lt, DeltaScalar::Int64(10))
        ),
        ["integer--1.parquet", "integer-2.parquet"]
    );
    assert_paths!(
        fixture,
        or(
            compare("long_part", DeltaComparison::Eq, DeltaScalar::Int64(-1)),
            compare("long_part", DeltaComparison::Eq, DeltaScalar::Int64(10))
        ),
        ["integer--1.parquet", "integer-10.parquet"]
    );
    assert_paths!(
        fixture,
        not(compare(
            "long_part",
            DeltaComparison::Eq,
            DeltaScalar::Int64(2)
        )),
        ["integer--1.parquet", "integer-10.parquet"]
    );

    let widths = PartitionFixture::new(
        "integer-width-partition-parity",
        PROTOCOL_JSON,
        fields.clone(),
        &["byte_part", "short_part", "int_part", "long_part"],
        vec![
            add(
                "width-byte-min.parquet",
                json!({"byte_part": "-128", "short_part": "0", "int_part": "0", "long_part": "0"}),
            ),
            add(
                "width-short-max.parquet",
                json!({"byte_part": "0", "short_part": "32767", "int_part": "0", "long_part": "0"}),
            ),
            add(
                "width-int-max.parquet",
                json!({"byte_part": "0", "short_part": "0", "int_part": "2147483647", "long_part": "0"}),
            ),
            add(
                "width-long-max.parquet",
                json!({"byte_part": "0", "short_part": "0", "int_part": "0", "long_part": "9223372036854775807"}),
            ),
        ],
    )?;
    assert_paths!(
        widths,
        compare("byte_part", DeltaComparison::Eq, DeltaScalar::Int8(i8::MIN)),
        ["width-byte-min.parquet"]
    );
    assert_paths!(
        widths,
        compare(
            "short_part",
            DeltaComparison::Eq,
            DeltaScalar::Int16(i16::MAX)
        ),
        ["width-short-max.parquet"]
    );
    assert_paths!(
        widths,
        compare(
            "int_part",
            DeltaComparison::Eq,
            DeltaScalar::Int32(i32::MAX)
        ),
        ["width-int-max.parquet"]
    );
    assert_paths!(
        widths,
        compare(
            "long_part",
            DeltaComparison::Eq,
            DeltaScalar::Int64(i64::MAX)
        ),
        ["width-long-max.parquet"]
    );

    let invalid = PartitionFixture::new(
        "invalid-integer-partition-parity",
        PROTOCOL_JSON,
        fields,
        &["byte_part", "short_part", "int_part", "long_part"],
        vec![
            integer_add("integer-valid.parquet", "7"),
            add(
                "integer-invalid.parquet",
                json!({
                    "byte_part": "0",
                    "short_part": "0",
                    "int_part": "0",
                    "long_part": "not-an-integer",
                }),
            ),
        ],
    )?;
    assert_invalid_partition(invalid.all_paths().unwrap_err(), "not-an-integer");
    assert_invalid_partition(
        invalid
            .selected_paths(&compare(
                "long_part",
                DeltaComparison::Eq,
                DeltaScalar::Int64(7),
            ))
            .unwrap_err(),
        "not-an-integer",
    );
    Ok(())
}
