use serde_json::{Map, Value, json};

use super::{DeltaLogTable, PROTOCOL_JSON, plan_scan, planned_tasks};
use crate::{
    DeltaComparison, DeltaPredicate, DeltaReaderExecutionOptions, DeltaScalar,
    DeltaSnapshotSelection, DeltaStorageOptions,
    kernel::delta_predicate_to_kernel_pruning,
    predicate::validate_predicate,
    snapshot::{LoadedDeltaTableSnapshot, load_delta_table_snapshot_blocking},
};

const TIMESTAMP_NTZ_PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["timestampNtz"],"writerFeatures":["timestampNtz"]}}"#;

struct StatisticsFixture {
    _table: DeltaLogTable,
    snapshot: LoadedDeltaTableSnapshot,
}

impl StatisticsFixture {
    fn new(
        name: &str,
        protocol: &str,
        fields: Vec<Value>,
        adds: Vec<String>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_protocol_metadata_and_adds(
            name,
            protocol,
            &metadata(fields),
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

    fn selected_paths(
        &self,
        predicate: &DeltaPredicate,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        validate_predicate(predicate, self.snapshot.schema().as_ref())?;
        let kernel_predicate = delta_predicate_to_kernel_pruning(predicate)
            .ok_or("predicate has no safe Kernel pruning representation")?;
        let plan = plan_scan(
            &self.snapshot,
            None,
            &[],
            Some(kernel_predicate),
            true,
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

fn metadata(fields: Vec<Value>) -> String {
    let schema = json!({"type": "struct", "fields": fields}).to_string();
    json!({
        "metaData": {
            "id": "delta-arrow-reader-statistics-parity",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": schema,
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 1587968585495_i64
        }
    })
    .to_string()
}

fn stats_add(
    path: &str,
    min_values: Option<Value>,
    max_values: Option<Value>,
    null_count: Option<Value>,
) -> String {
    let mut stats = Map::from_iter([("numRecords".to_owned(), json!(10))]);
    if let Some(values) = min_values {
        stats.insert("minValues".to_owned(), values);
    }
    if let Some(values) = max_values {
        stats.insert("maxValues".to_owned(), values);
    }
    if let Some(values) = null_count {
        stats.insert("nullCount".to_owned(), values);
    }
    json!({
        "add": {
            "path": path,
            "partitionValues": {},
            "size": 10,
            "modificationTime": 1587968586000_i64,
            "dataChange": true,
            "stats": Value::Object(stats).to_string()
        }
    })
    .to_string()
}

fn missing_stats_add(path: &str) -> String {
    json!({
        "add": {
            "path": path,
            "partitionValues": {},
            "size": 10,
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

macro_rules! assert_paths {
    ($fixture:expr, $predicate:expr, [$($path:literal),* $(,)?]) => {
        let expected: Vec<String> = vec![$($path.to_owned()),*];
        assert_eq!(
            $fixture.selected_paths(&$predicate)?,
            expected
        );
    };
}

#[test]
fn integer_statistics_pruning_matches_the_frozen_selected_files()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = StatisticsFixture::new(
        "integer-statistics-parity",
        PROTOCOL_JSON,
        vec![field("id", "integer")],
        vec![
            stats_add(
                "id-impossible.parquet",
                Some(json!({"id": 1})),
                Some(json!({"id": 50})),
                Some(json!({"id": 0})),
            ),
            stats_add(
                "id-possible.parquet",
                Some(json!({"id": 101})),
                Some(json!({"id": 150})),
                Some(json!({"id": 0})),
            ),
            missing_stats_add("id-missing-stats.parquet"),
        ],
    )?;
    assert_paths!(
        fixture,
        compare("id", DeltaComparison::Gt, DeltaScalar::Int32(100)),
        ["id-missing-stats.parquet", "id-possible.parquet"]
    );

    let all_low = StatisticsFixture::new(
        "integer-all-impossible-parity",
        PROTOCOL_JSON,
        vec![field("id", "integer")],
        vec![
            stats_add(
                "id-low-a.parquet",
                Some(json!({"id": 1})),
                Some(json!({"id": 50})),
                Some(json!({"id": 0})),
            ),
            stats_add(
                "id-low-b.parquet",
                Some(json!({"id": 51})),
                Some(json!({"id": 100})),
                Some(json!({"id": 0})),
            ),
        ],
    )?;
    assert_paths!(
        all_low,
        compare("id", DeltaComparison::Gt, DeltaScalar::Int32(100)),
        []
    );
    Ok(())
}

#[test]
fn boolean_statistics_pruning_matches_complete_and_partial_frozen_boundaries()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = StatisticsFixture::new(
        "boolean-statistics-parity",
        PROTOCOL_JSON,
        vec![field("is_current", "boolean")],
        vec![
            stats_add(
                "boolean-false-only.parquet",
                Some(json!({"is_current": false})),
                Some(json!({"is_current": false})),
                Some(json!({"is_current": 0})),
            ),
            stats_add(
                "boolean-true-only.parquet",
                Some(json!({"is_current": true})),
                Some(json!({"is_current": true})),
                Some(json!({"is_current": 0})),
            ),
            stats_add(
                "boolean-mixed.parquet",
                Some(json!({"is_current": false})),
                Some(json!({"is_current": true})),
                Some(json!({"is_current": 0})),
            ),
            stats_add(
                "boolean-false-with-null.parquet",
                Some(json!({"is_current": false})),
                Some(json!({"is_current": false})),
                Some(json!({"is_current": 2})),
            ),
            stats_add(
                "boolean-true-with-null.parquet",
                Some(json!({"is_current": true})),
                Some(json!({"is_current": true})),
                Some(json!({"is_current": 2})),
            ),
            stats_add(
                "boolean-all-null.parquet",
                None,
                None,
                Some(json!({"is_current": 10})),
            ),
            missing_stats_add("boolean-missing-stats.parquet"),
        ],
    )?;
    let conservative = [
        "boolean-false-only.parquet",
        "boolean-false-with-null.parquet",
        "boolean-missing-stats.parquet",
        "boolean-mixed.parquet",
        "boolean-true-only.parquet",
        "boolean-true-with-null.parquet",
    ];
    for (op, value) in [
        (DeltaComparison::Eq, true),
        (DeltaComparison::Eq, false),
        (DeltaComparison::NotEq, true),
        (DeltaComparison::NotEq, false),
    ] {
        assert_eq!(
            fixture.selected_paths(&compare("is_current", op, DeltaScalar::Boolean(value),))?,
            conservative.map(str::to_owned)
        );
    }
    assert_paths!(
        fixture,
        is_null("is_current"),
        [
            "boolean-all-null.parquet",
            "boolean-false-with-null.parquet",
            "boolean-missing-stats.parquet",
            "boolean-true-with-null.parquet"
        ]
    );
    assert_eq!(
        fixture.selected_paths(&is_not_null("is_current"))?,
        conservative.map(str::to_owned)
    );

    let partial = StatisticsFixture::new(
        "boolean-partial-statistics-parity",
        PROTOCOL_JSON,
        vec![field("is_current", "boolean")],
        vec![
            stats_add(
                "boolean-min-only-false.parquet",
                Some(json!({"is_current": false})),
                None,
                Some(json!({"is_current": 0})),
            ),
            stats_add(
                "boolean-max-only-true.parquet",
                None,
                Some(json!({"is_current": true})),
                Some(json!({"is_current": 0})),
            ),
            stats_add(
                "boolean-counts-only.parquet",
                None,
                None,
                Some(json!({"is_current": 0})),
            ),
            stats_add(
                "boolean-missing-null-count.parquet",
                Some(json!({"is_current": false})),
                Some(json!({"is_current": true})),
                None,
            ),
            missing_stats_add("boolean-missing-stats.parquet"),
        ],
    )?;
    assert_paths!(
        partial,
        is_null("is_current"),
        [
            "boolean-missing-null-count.parquet",
            "boolean-missing-stats.parquet"
        ]
    );
    assert_paths!(
        partial,
        is_not_null("is_current"),
        [
            "boolean-counts-only.parquet",
            "boolean-max-only-true.parquet",
            "boolean-min-only-false.parquet",
            "boolean-missing-null-count.parquet",
            "boolean-missing-stats.parquet"
        ]
    );
    Ok(())
}

#[test]
fn date_statistics_pruning_matches_complete_and_partial_frozen_boundaries()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = StatisticsFixture::new(
        "date-statistics-parity",
        PROTOCOL_JSON,
        vec![field("event_date", "date")],
        vec![
            stats_add(
                "date-pre-epoch-only.parquet",
                Some(json!({"event_date": "1969-12-31"})),
                Some(json!({"event_date": "1969-12-31"})),
                Some(json!({"event_date": 0})),
            ),
            stats_add(
                "date-leap-only.parquet",
                Some(json!({"event_date": "2024-02-29"})),
                Some(json!({"event_date": "2024-02-29"})),
                Some(json!({"event_date": 0})),
            ),
            stats_add(
                "date-new-year-only.parquet",
                Some(json!({"event_date": "2026-01-01"})),
                Some(json!({"event_date": "2026-01-01"})),
                Some(json!({"event_date": 0})),
            ),
            stats_add(
                "date-range.parquet",
                Some(json!({"event_date": "2024-02-29"})),
                Some(json!({"event_date": "2026-01-01"})),
                Some(json!({"event_date": 0})),
            ),
            stats_add(
                "date-new-year-with-null.parquet",
                Some(json!({"event_date": "2026-01-01"})),
                Some(json!({"event_date": "2026-01-01"})),
                Some(json!({"event_date": 2})),
            ),
            stats_add(
                "date-all-null.parquet",
                None,
                None,
                Some(json!({"event_date": 10})),
            ),
            missing_stats_add("date-missing-stats.parquet"),
        ],
    )?;
    assert_paths!(
        fixture,
        compare(
            "event_date",
            DeltaComparison::Gt,
            DeltaScalar::Date32(19_782)
        ),
        [
            "date-missing-stats.parquet",
            "date-new-year-only.parquet",
            "date-new-year-with-null.parquet",
            "date-range.parquet"
        ]
    );
    assert_paths!(
        fixture,
        compare(
            "event_date",
            DeltaComparison::GtEq,
            DeltaScalar::Date32(20_454)
        ),
        [
            "date-missing-stats.parquet",
            "date-new-year-only.parquet",
            "date-new-year-with-null.parquet",
            "date-range.parquet"
        ]
    );
    assert_paths!(
        fixture,
        compare("event_date", DeltaComparison::LtEq, DeltaScalar::Date32(-1)),
        ["date-missing-stats.parquet", "date-pre-epoch-only.parquet"]
    );
    assert_paths!(
        fixture,
        compare(
            "event_date",
            DeltaComparison::Lt,
            DeltaScalar::Date32(20_454)
        ),
        [
            "date-leap-only.parquet",
            "date-missing-stats.parquet",
            "date-pre-epoch-only.parquet",
            "date-range.parquet"
        ]
    );
    assert_paths!(
        fixture,
        compare(
            "event_date",
            DeltaComparison::Eq,
            DeltaScalar::Date32(20_454)
        ),
        [
            "date-missing-stats.parquet",
            "date-new-year-only.parquet",
            "date-new-year-with-null.parquet",
            "date-range.parquet"
        ]
    );
    assert_paths!(
        fixture,
        compare(
            "event_date",
            DeltaComparison::NotEq,
            DeltaScalar::Date32(20_454)
        ),
        [
            "date-leap-only.parquet",
            "date-missing-stats.parquet",
            "date-pre-epoch-only.parquet",
            "date-range.parquet"
        ]
    );
    assert_paths!(
        fixture,
        is_null("event_date"),
        [
            "date-all-null.parquet",
            "date-missing-stats.parquet",
            "date-new-year-with-null.parquet"
        ]
    );
    assert_paths!(
        fixture,
        is_not_null("event_date"),
        [
            "date-leap-only.parquet",
            "date-missing-stats.parquet",
            "date-new-year-only.parquet",
            "date-new-year-with-null.parquet",
            "date-pre-epoch-only.parquet",
            "date-range.parquet"
        ]
    );

    let partial = StatisticsFixture::new(
        "date-partial-statistics-parity",
        PROTOCOL_JSON,
        vec![field("event_date", "date")],
        vec![
            stats_add(
                "date-min-only-high.parquet",
                Some(json!({"event_date": "2026-01-01"})),
                None,
                Some(json!({"event_date": 0})),
            ),
            stats_add(
                "date-max-only-low.parquet",
                None,
                Some(json!({"event_date": "2024-02-29"})),
                Some(json!({"event_date": 0})),
            ),
            stats_add(
                "date-counts-only.parquet",
                None,
                None,
                Some(json!({"event_date": 0})),
            ),
            stats_add(
                "date-missing-null-count.parquet",
                Some(json!({"event_date": "2024-02-29"})),
                Some(json!({"event_date": "2026-01-01"})),
                None,
            ),
            missing_stats_add("date-missing-stats.parquet"),
        ],
    )?;
    assert_paths!(
        partial,
        compare(
            "event_date",
            DeltaComparison::Gt,
            DeltaScalar::Date32(19_782)
        ),
        [
            "date-counts-only.parquet",
            "date-min-only-high.parquet",
            "date-missing-null-count.parquet",
            "date-missing-stats.parquet"
        ]
    );
    assert_paths!(
        partial,
        is_null("event_date"),
        [
            "date-missing-null-count.parquet",
            "date-missing-stats.parquet"
        ]
    );
    assert_paths!(
        partial,
        is_not_null("event_date"),
        [
            "date-counts-only.parquet",
            "date-max-only-low.parquet",
            "date-min-only-high.parquet",
            "date-missing-null-count.parquet",
            "date-missing-stats.parquet"
        ]
    );
    Ok(())
}

#[test]
fn decimal_statistics_pruning_matches_complete_and_partial_frozen_boundaries()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = StatisticsFixture::new(
        "decimal-statistics-parity",
        PROTOCOL_JSON,
        vec![field("amount", "decimal(10,2)")],
        vec![
            stats_add(
                "decimal-negative-only.parquet",
                Some(json!({"amount": "-1.23"})),
                Some(json!({"amount": "-1.23"})),
                Some(json!({"amount": 0})),
            ),
            stats_add(
                "decimal-zero-only.parquet",
                Some(json!({"amount": "0.00"})),
                Some(json!({"amount": "0.00"})),
                Some(json!({"amount": 0})),
            ),
            stats_add(
                "decimal-two-only.parquet",
                Some(json!({"amount": "2.00"})),
                Some(json!({"amount": "2.00"})),
                Some(json!({"amount": 0})),
            ),
            stats_add(
                "decimal-ten-only.parquet",
                Some(json!({"amount": "10.00"})),
                Some(json!({"amount": "10.00"})),
                Some(json!({"amount": 0})),
            ),
            stats_add(
                "decimal-large-only.parquet",
                Some(json!({"amount": "123.45"})),
                Some(json!({"amount": "123.45"})),
                Some(json!({"amount": 0})),
            ),
            stats_add(
                "decimal-range.parquet",
                Some(json!({"amount": "0.00"})),
                Some(json!({"amount": "10.00"})),
                Some(json!({"amount": 0})),
            ),
            stats_add(
                "decimal-two-with-null.parquet",
                Some(json!({"amount": "2.00"})),
                Some(json!({"amount": "2.00"})),
                Some(json!({"amount": 2})),
            ),
            stats_add(
                "decimal-all-null.parquet",
                None,
                None,
                Some(json!({"amount": 10})),
            ),
            missing_stats_add("decimal-missing-stats.parquet"),
        ],
    )?;
    assert_paths!(
        fixture,
        compare("amount", DeltaComparison::Gt, decimal(200)),
        [
            "decimal-large-only.parquet",
            "decimal-missing-stats.parquet",
            "decimal-range.parquet",
            "decimal-ten-only.parquet"
        ]
    );
    assert_paths!(
        fixture,
        compare("amount", DeltaComparison::GtEq, decimal(1_000)),
        [
            "decimal-large-only.parquet",
            "decimal-missing-stats.parquet",
            "decimal-range.parquet",
            "decimal-ten-only.parquet"
        ]
    );
    assert_paths!(
        fixture,
        compare("amount", DeltaComparison::LtEq, decimal(-123)),
        [
            "decimal-missing-stats.parquet",
            "decimal-negative-only.parquet"
        ]
    );
    assert_paths!(
        fixture,
        compare("amount", DeltaComparison::Lt, decimal(1_000)),
        [
            "decimal-missing-stats.parquet",
            "decimal-negative-only.parquet",
            "decimal-range.parquet",
            "decimal-two-only.parquet",
            "decimal-two-with-null.parquet",
            "decimal-zero-only.parquet"
        ]
    );
    assert_paths!(
        fixture,
        compare("amount", DeltaComparison::Eq, decimal(200)),
        [
            "decimal-missing-stats.parquet",
            "decimal-range.parquet",
            "decimal-two-only.parquet",
            "decimal-two-with-null.parquet"
        ]
    );
    assert_paths!(
        fixture,
        compare("amount", DeltaComparison::NotEq, decimal(200)),
        [
            "decimal-large-only.parquet",
            "decimal-missing-stats.parquet",
            "decimal-negative-only.parquet",
            "decimal-range.parquet",
            "decimal-ten-only.parquet",
            "decimal-zero-only.parquet"
        ]
    );
    assert_paths!(
        fixture,
        is_null("amount"),
        [
            "decimal-all-null.parquet",
            "decimal-missing-stats.parquet",
            "decimal-two-with-null.parquet"
        ]
    );
    assert_paths!(
        fixture,
        is_not_null("amount"),
        [
            "decimal-large-only.parquet",
            "decimal-missing-stats.parquet",
            "decimal-negative-only.parquet",
            "decimal-range.parquet",
            "decimal-ten-only.parquet",
            "decimal-two-only.parquet",
            "decimal-two-with-null.parquet",
            "decimal-zero-only.parquet"
        ]
    );

    let partial = StatisticsFixture::new(
        "decimal-partial-statistics-parity",
        PROTOCOL_JSON,
        vec![field("amount", "decimal(10,2)")],
        vec![
            stats_add(
                "decimal-min-only-high.parquet",
                Some(json!({"amount": "10.00"})),
                None,
                Some(json!({"amount": 0})),
            ),
            stats_add(
                "decimal-max-only-low.parquet",
                None,
                Some(json!({"amount": "0.00"})),
                Some(json!({"amount": 0})),
            ),
            stats_add(
                "decimal-counts-only.parquet",
                None,
                None,
                Some(json!({"amount": 0})),
            ),
            stats_add(
                "decimal-missing-null-count.parquet",
                Some(json!({"amount": "0.00"})),
                Some(json!({"amount": "10.00"})),
                None,
            ),
            missing_stats_add("decimal-missing-stats.parquet"),
        ],
    )?;
    assert_paths!(
        partial,
        compare("amount", DeltaComparison::Gt, decimal(200)),
        [
            "decimal-counts-only.parquet",
            "decimal-min-only-high.parquet",
            "decimal-missing-null-count.parquet",
            "decimal-missing-stats.parquet"
        ]
    );
    assert_paths!(
        partial,
        is_null("amount"),
        [
            "decimal-missing-null-count.parquet",
            "decimal-missing-stats.parquet"
        ]
    );
    assert_paths!(
        partial,
        is_not_null("amount"),
        [
            "decimal-counts-only.parquet",
            "decimal-max-only-low.parquet",
            "decimal-min-only-high.parquet",
            "decimal-missing-null-count.parquet",
            "decimal-missing-stats.parquet"
        ]
    );
    Ok(())
}

#[test]
fn binary_statistics_pruning_matches_complete_and_partial_frozen_boundaries()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = StatisticsFixture::new(
        "binary-statistics-parity",
        PROTOCOL_JSON,
        vec![field("payload", "binary")],
        vec![
            stats_add(
                "binary-HELLO.parquet",
                Some(json!({"payload": "HELLO"})),
                Some(json!({"payload": "HELLO"})),
                Some(json!({"payload": 0})),
            ),
            stats_add(
                "binary-empty.parquet",
                Some(json!({"payload": ""})),
                Some(json!({"payload": ""})),
                Some(json!({"payload": 0})),
            ),
            stats_add(
                "binary-hello.parquet",
                Some(json!({"payload": "hello"})),
                Some(json!({"payload": "hello"})),
                Some(json!({"payload": 0})),
            ),
            stats_add(
                "binary-range.parquet",
                Some(json!({"payload": "a"})),
                Some(json!({"payload": "z"})),
                Some(json!({"payload": 0})),
            ),
            stats_add(
                "binary-special.parquet",
                Some(json!({"payload": "/=%"})),
                Some(json!({"payload": "/=%"})),
                Some(json!({"payload": 0})),
            ),
            stats_add(
                "binary-with-null.parquet",
                Some(json!({"payload": "hello"})),
                Some(json!({"payload": "hello"})),
                Some(json!({"payload": 2})),
            ),
            stats_add(
                "binary-all-null.parquet",
                None,
                None,
                Some(json!({"payload": 10})),
            ),
            missing_stats_add("binary-missing-stats.parquet"),
        ],
    )?;
    let conservative = [
        "binary-HELLO.parquet",
        "binary-empty.parquet",
        "binary-hello.parquet",
        "binary-missing-stats.parquet",
        "binary-range.parquet",
        "binary-special.parquet",
        "binary-with-null.parquet",
    ];
    for (op, value) in [
        (DeltaComparison::Eq, b"hello".to_vec()),
        (DeltaComparison::NotEq, b"hello".to_vec()),
        (DeltaComparison::Gt, b"hello".to_vec()),
        (DeltaComparison::Lt, b"hello".to_vec()),
        (DeltaComparison::Eq, Vec::new()),
    ] {
        assert_eq!(
            fixture.selected_paths(&compare("payload", op, DeltaScalar::Binary(value)))?,
            conservative.map(str::to_owned)
        );
    }
    assert_paths!(
        fixture,
        is_null("payload"),
        [
            "binary-all-null.parquet",
            "binary-missing-stats.parquet",
            "binary-with-null.parquet"
        ]
    );
    assert_eq!(
        fixture.selected_paths(&is_not_null("payload"))?,
        conservative.map(str::to_owned)
    );

    let partial = StatisticsFixture::new(
        "binary-partial-statistics-parity",
        PROTOCOL_JSON,
        vec![field("payload", "binary")],
        vec![
            stats_add(
                "binary-min-only-high.parquet",
                Some(json!({"payload": "m"})),
                None,
                Some(json!({"payload": 0})),
            ),
            stats_add(
                "binary-max-only-low.parquet",
                None,
                Some(json!({"payload": "a"})),
                Some(json!({"payload": 0})),
            ),
            stats_add(
                "binary-counts-only.parquet",
                None,
                None,
                Some(json!({"payload": 0})),
            ),
            stats_add(
                "binary-missing-null-count.parquet",
                Some(json!({"payload": "a"})),
                Some(json!({"payload": "z"})),
                None,
            ),
            missing_stats_add("binary-missing-stats.parquet"),
        ],
    )?;
    assert_paths!(
        partial,
        compare(
            "payload",
            DeltaComparison::Gt,
            DeltaScalar::Binary(b"hello".to_vec())
        ),
        [
            "binary-counts-only.parquet",
            "binary-max-only-low.parquet",
            "binary-min-only-high.parquet",
            "binary-missing-null-count.parquet",
            "binary-missing-stats.parquet"
        ]
    );
    assert_paths!(
        partial,
        is_null("payload"),
        [
            "binary-missing-null-count.parquet",
            "binary-missing-stats.parquet"
        ]
    );
    assert_paths!(
        partial,
        is_not_null("payload"),
        [
            "binary-counts-only.parquet",
            "binary-max-only-low.parquet",
            "binary-min-only-high.parquet",
            "binary-missing-null-count.parquet",
            "binary-missing-stats.parquet"
        ]
    );
    Ok(())
}

#[test]
fn floating_statistics_pruning_matches_safe_frozen_boundaries()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = StatisticsFixture::new(
        "floating-statistics-parity",
        PROTOCOL_JSON,
        vec![
            field("float_score", "float"),
            field("double_score", "double"),
        ],
        vec![
            stats_add(
                "floating-neg.parquet",
                Some(json!({"float_score": -1.5, "double_score": -2.25})),
                Some(json!({"float_score": -1.5, "double_score": -2.25})),
                Some(json!({"float_score": 0, "double_score": 0})),
            ),
            stats_add(
                "floating-neg-zero.parquet",
                Some(json!({"float_score": -0.0, "double_score": -0.0})),
                Some(json!({"float_score": -0.0, "double_score": -0.0})),
                Some(json!({"float_score": 0, "double_score": 0})),
            ),
            stats_add(
                "floating-pos-zero.parquet",
                Some(json!({"float_score": 0.0, "double_score": 0.0})),
                Some(json!({"float_score": 0.0, "double_score": 0.0})),
                Some(json!({"float_score": 0, "double_score": 0})),
            ),
            stats_add(
                "floating-one.parquet",
                Some(json!({"float_score": 1.5, "double_score": 2.25})),
                Some(json!({"float_score": 1.5, "double_score": 2.25})),
                Some(json!({"float_score": 0, "double_score": 0})),
            ),
            stats_add(
                "floating-range.parquet",
                Some(json!({"float_score": -1.0, "double_score": -2.0})),
                Some(json!({"float_score": 2.0, "double_score": 3.0})),
                Some(json!({"float_score": 0, "double_score": 0})),
            ),
            stats_add(
                "floating-ten.parquet",
                Some(json!({"float_score": 10.0, "double_score": 10.0})),
                Some(json!({"float_score": 10.0, "double_score": 10.0})),
                Some(json!({"float_score": 0, "double_score": 0})),
            ),
            stats_add(
                "floating-one-with-null.parquet",
                Some(json!({"float_score": 1.5, "double_score": 2.25})),
                Some(json!({"float_score": 1.5, "double_score": 2.25})),
                Some(json!({"float_score": 2, "double_score": 2})),
            ),
            stats_add(
                "floating-all-null.parquet",
                None,
                None,
                Some(json!({"float_score": 10, "double_score": 10})),
            ),
            missing_stats_add("floating-missing-stats.parquet"),
        ],
    )?;
    assert_paths!(
        fixture,
        compare(
            "float_score",
            DeltaComparison::Gt,
            DeltaScalar::Float32(1.5)
        ),
        [
            "floating-missing-stats.parquet",
            "floating-range.parquet",
            "floating-ten.parquet"
        ]
    );
    assert_paths!(
        fixture,
        compare(
            "float_score",
            DeltaComparison::Lt,
            DeltaScalar::Float32(10.0)
        ),
        [
            "floating-missing-stats.parquet",
            "floating-neg-zero.parquet",
            "floating-neg.parquet",
            "floating-one-with-null.parquet",
            "floating-one.parquet",
            "floating-pos-zero.parquet",
            "floating-range.parquet"
        ]
    );
    assert_paths!(
        fixture,
        compare(
            "float_score",
            DeltaComparison::Eq,
            DeltaScalar::Float32(1.5)
        ),
        [
            "floating-missing-stats.parquet",
            "floating-one-with-null.parquet",
            "floating-one.parquet",
            "floating-range.parquet"
        ]
    );
    assert_paths!(
        fixture,
        compare(
            "float_score",
            DeltaComparison::NotEq,
            DeltaScalar::Float32(1.5)
        ),
        [
            "floating-missing-stats.parquet",
            "floating-neg-zero.parquet",
            "floating-neg.parquet",
            "floating-pos-zero.parquet",
            "floating-range.parquet",
            "floating-ten.parquet"
        ]
    );
    assert_paths!(
        fixture,
        is_null("float_score"),
        [
            "floating-all-null.parquet",
            "floating-missing-stats.parquet",
            "floating-one-with-null.parquet"
        ]
    );
    assert_paths!(
        fixture,
        is_not_null("double_score"),
        [
            "floating-missing-stats.parquet",
            "floating-neg-zero.parquet",
            "floating-neg.parquet",
            "floating-one-with-null.parquet",
            "floating-one.parquet",
            "floating-pos-zero.parquet",
            "floating-range.parquet",
            "floating-ten.parquet"
        ]
    );

    let partial = StatisticsFixture::new(
        "floating-partial-statistics-parity",
        PROTOCOL_JSON,
        vec![
            field("float_score", "float"),
            field("double_score", "double"),
        ],
        vec![
            stats_add(
                "floating-min-only-high.parquet",
                Some(json!({"float_score": 2.0, "double_score": 2.0})),
                None,
                Some(json!({"float_score": 0, "double_score": 0})),
            ),
            stats_add(
                "floating-max-only-low.parquet",
                None,
                Some(json!({"float_score": 0.0, "double_score": 0.0})),
                Some(json!({"float_score": 0, "double_score": 0})),
            ),
            stats_add(
                "floating-counts-only.parquet",
                None,
                None,
                Some(json!({"float_score": 0, "double_score": 0})),
            ),
            stats_add(
                "floating-missing-null-count.parquet",
                Some(json!({"float_score": -1.0, "double_score": -1.0})),
                Some(json!({"float_score": 2.0, "double_score": 2.0})),
                None,
            ),
            missing_stats_add("floating-missing-stats.parquet"),
        ],
    )?;
    assert_paths!(
        partial,
        compare(
            "float_score",
            DeltaComparison::Gt,
            DeltaScalar::Float32(1.0)
        ),
        [
            "floating-counts-only.parquet",
            "floating-min-only-high.parquet",
            "floating-missing-null-count.parquet",
            "floating-missing-stats.parquet"
        ]
    );
    assert_paths!(
        partial,
        is_null("float_score"),
        [
            "floating-missing-null-count.parquet",
            "floating-missing-stats.parquet"
        ]
    );
    assert_paths!(
        partial,
        is_not_null("float_score"),
        [
            "floating-counts-only.parquet",
            "floating-max-only-low.parquet",
            "floating-min-only-high.parquet",
            "floating-missing-null-count.parquet",
            "floating-missing-stats.parquet"
        ]
    );

    let nonfinite = StatisticsFixture::new(
        "floating-nonfinite-statistics-parity",
        PROTOCOL_JSON,
        vec![
            field("float_score", "float"),
            field("double_score", "double"),
        ],
        vec![
            stats_add(
                "floating-valid.parquet",
                Some(json!({"float_score": 1.5, "double_score": 2.25})),
                Some(json!({"float_score": 1.5, "double_score": 2.25})),
                Some(json!({"float_score": 0, "double_score": 0})),
            ),
            stats_add(
                "floating-nan.parquet",
                Some(json!({"float_score": "NaN", "double_score": "NaN"})),
                Some(json!({"float_score": "NaN", "double_score": "NaN"})),
                Some(json!({"float_score": 0, "double_score": 0})),
            ),
            stats_add(
                "floating-inf.parquet",
                Some(json!({"float_score": "Infinity", "double_score": "Infinity"})),
                Some(json!({"float_score": "Infinity", "double_score": "Infinity"})),
                Some(json!({"float_score": 0, "double_score": 0})),
            ),
            stats_add(
                "floating-neg-inf.parquet",
                Some(json!({"float_score": "-Infinity", "double_score": "-Infinity"})),
                Some(json!({"float_score": "-Infinity", "double_score": "-Infinity"})),
                Some(json!({"float_score": 0, "double_score": 0})),
            ),
            missing_stats_add("floating-missing-stats.parquet"),
        ],
    )?;
    assert_paths!(
        nonfinite,
        compare(
            "float_score",
            DeltaComparison::Gt,
            DeltaScalar::Float32(1.0)
        ),
        [
            "floating-inf.parquet",
            "floating-missing-stats.parquet",
            "floating-nan.parquet",
            "floating-valid.parquet"
        ]
    );
    assert_paths!(
        nonfinite,
        compare(
            "float_score",
            DeltaComparison::Eq,
            DeltaScalar::Float32(1.5)
        ),
        ["floating-missing-stats.parquet", "floating-valid.parquet"]
    );
    assert_paths!(
        nonfinite,
        compare(
            "float_score",
            DeltaComparison::NotEq,
            DeltaScalar::Float32(1.5)
        ),
        [
            "floating-inf.parquet",
            "floating-missing-stats.parquet",
            "floating-nan.parquet",
            "floating-neg-inf.parquet"
        ]
    );
    Ok(())
}

#[test]
fn string_statistics_pruning_matches_complete_partial_and_unicode_frozen_boundaries()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = StatisticsFixture::new(
        "string-statistics-parity",
        PROTOCOL_JSON,
        vec![field("customer_name", "string")],
        vec![
            stats_add(
                "string-empty-only.parquet",
                Some(json!({"customer_name": ""})),
                Some(json!({"customer_name": ""})),
                Some(json!({"customer_name": 0})),
            ),
            stats_add(
                "string-mixed-case-only.parquet",
                Some(json!({"customer_name": "Alice"})),
                Some(json!({"customer_name": "Alice"})),
                Some(json!({"customer_name": 0})),
            ),
            stats_add(
                "string-alice-only.parquet",
                Some(json!({"customer_name": "alice"})),
                Some(json!({"customer_name": "alice"})),
                Some(json!({"customer_name": 0})),
            ),
            stats_add(
                "string-bob-only.parquet",
                Some(json!({"customer_name": "bob"})),
                Some(json!({"customer_name": "bob"})),
                Some(json!({"customer_name": 0})),
            ),
            stats_add(
                "string-range.parquet",
                Some(json!({"customer_name": "alice"})),
                Some(json!({"customer_name": "morgan"})),
                Some(json!({"customer_name": 0})),
            ),
            stats_add(
                "string-zed-only.parquet",
                Some(json!({"customer_name": "zed"})),
                Some(json!({"customer_name": "zed"})),
                Some(json!({"customer_name": 0})),
            ),
            stats_add(
                "string-alice-with-null.parquet",
                Some(json!({"customer_name": "alice"})),
                Some(json!({"customer_name": "alice"})),
                Some(json!({"customer_name": 2})),
            ),
            stats_add(
                "string-all-null.parquet",
                None,
                None,
                Some(json!({"customer_name": 10})),
            ),
            missing_stats_add("string-missing-stats.parquet"),
        ],
    )?;
    assert_paths!(
        fixture,
        compare(
            "customer_name",
            DeltaComparison::Gt,
            DeltaScalar::Utf8("m".to_owned())
        ),
        [
            "string-missing-stats.parquet",
            "string-range.parquet",
            "string-zed-only.parquet"
        ]
    );
    assert_paths!(
        fixture,
        compare(
            "customer_name",
            DeltaComparison::GtEq,
            DeltaScalar::Utf8("morgan".to_owned())
        ),
        [
            "string-missing-stats.parquet",
            "string-range.parquet",
            "string-zed-only.parquet"
        ]
    );
    assert_paths!(
        fixture,
        compare(
            "customer_name",
            DeltaComparison::LtEq,
            DeltaScalar::Utf8("Alice".to_owned())
        ),
        [
            "string-empty-only.parquet",
            "string-missing-stats.parquet",
            "string-mixed-case-only.parquet"
        ]
    );
    assert_paths!(
        fixture,
        compare(
            "customer_name",
            DeltaComparison::Lt,
            DeltaScalar::Utf8("m".to_owned())
        ),
        [
            "string-alice-only.parquet",
            "string-alice-with-null.parquet",
            "string-bob-only.parquet",
            "string-empty-only.parquet",
            "string-missing-stats.parquet",
            "string-mixed-case-only.parquet",
            "string-range.parquet"
        ]
    );
    assert_paths!(
        fixture,
        compare(
            "customer_name",
            DeltaComparison::Eq,
            DeltaScalar::Utf8("alice".to_owned())
        ),
        [
            "string-alice-only.parquet",
            "string-alice-with-null.parquet",
            "string-missing-stats.parquet",
            "string-range.parquet"
        ]
    );
    assert_paths!(
        fixture,
        compare(
            "customer_name",
            DeltaComparison::NotEq,
            DeltaScalar::Utf8("alice".to_owned())
        ),
        [
            "string-bob-only.parquet",
            "string-empty-only.parquet",
            "string-missing-stats.parquet",
            "string-mixed-case-only.parquet",
            "string-range.parquet",
            "string-zed-only.parquet"
        ]
    );
    assert_paths!(
        fixture,
        is_null("customer_name"),
        [
            "string-alice-with-null.parquet",
            "string-all-null.parquet",
            "string-missing-stats.parquet"
        ]
    );
    assert_paths!(
        fixture,
        is_not_null("customer_name"),
        [
            "string-alice-only.parquet",
            "string-alice-with-null.parquet",
            "string-bob-only.parquet",
            "string-empty-only.parquet",
            "string-missing-stats.parquet",
            "string-mixed-case-only.parquet",
            "string-range.parquet",
            "string-zed-only.parquet"
        ]
    );

    let partial = StatisticsFixture::new(
        "string-partial-statistics-parity",
        PROTOCOL_JSON,
        vec![field("customer_name", "string")],
        vec![
            stats_add(
                "string-min-only-morgan.parquet",
                Some(json!({"customer_name": "morgan"})),
                None,
                Some(json!({"customer_name": 0})),
            ),
            stats_add(
                "string-max-only-alice.parquet",
                None,
                Some(json!({"customer_name": "alice"})),
                Some(json!({"customer_name": 0})),
            ),
            stats_add(
                "string-counts-only.parquet",
                None,
                None,
                Some(json!({"customer_name": 0})),
            ),
            stats_add(
                "string-missing-null-count.parquet",
                Some(json!({"customer_name": "alice"})),
                Some(json!({"customer_name": "morgan"})),
                None,
            ),
            missing_stats_add("string-missing-stats.parquet"),
        ],
    )?;
    assert_paths!(
        partial,
        compare(
            "customer_name",
            DeltaComparison::Gt,
            DeltaScalar::Utf8("m".to_owned())
        ),
        [
            "string-counts-only.parquet",
            "string-min-only-morgan.parquet",
            "string-missing-null-count.parquet",
            "string-missing-stats.parquet"
        ]
    );
    assert_paths!(
        partial,
        is_null("customer_name"),
        [
            "string-missing-null-count.parquet",
            "string-missing-stats.parquet"
        ]
    );
    assert_paths!(
        partial,
        is_not_null("customer_name"),
        [
            "string-counts-only.parquet",
            "string-max-only-alice.parquet",
            "string-min-only-morgan.parquet",
            "string-missing-null-count.parquet",
            "string-missing-stats.parquet"
        ]
    );

    let unicode = StatisticsFixture::new(
        "string-unicode-statistics-parity",
        PROTOCOL_JSON,
        vec![field("customer_name", "string")],
        vec![
            stats_add(
                "string-ascii-cafe.parquet",
                Some(json!({"customer_name": "cafe"})),
                Some(json!({"customer_name": "cafe"})),
                Some(json!({"customer_name": 0})),
            ),
            stats_add(
                "string-ascii-zulu.parquet",
                Some(json!({"customer_name": "zulu"})),
                Some(json!({"customer_name": "zulu"})),
                Some(json!({"customer_name": 0})),
            ),
            stats_add(
                "string-eclair.parquet",
                Some(json!({"customer_name": "\u{00e9}clair"})),
                Some(json!({"customer_name": "\u{00e9}clair"})),
                Some(json!({"customer_name": 0})),
            ),
            stats_add(
                "string-emile.parquet",
                Some(json!({"customer_name": "\u{00e9}mile"})),
                Some(json!({"customer_name": "\u{00e9}mile"})),
                Some(json!({"customer_name": 0})),
            ),
            missing_stats_add("string-missing-stats.parquet"),
        ],
    )?;
    assert_paths!(
        unicode,
        compare(
            "customer_name",
            DeltaComparison::Eq,
            DeltaScalar::Utf8("\u{00e9}clair".to_owned())
        ),
        ["string-eclair.parquet", "string-missing-stats.parquet"]
    );
    assert_paths!(
        unicode,
        compare(
            "customer_name",
            DeltaComparison::GtEq,
            DeltaScalar::Utf8("\u{00e9}clair".to_owned())
        ),
        [
            "string-eclair.parquet",
            "string-emile.parquet",
            "string-missing-stats.parquet"
        ]
    );
    assert_paths!(
        unicode,
        compare(
            "customer_name",
            DeltaComparison::Lt,
            DeltaScalar::Utf8("\u{00e9}clair".to_owned())
        ),
        [
            "string-ascii-cafe.parquet",
            "string-ascii-zulu.parquet",
            "string-missing-stats.parquet"
        ]
    );
    assert_paths!(
        unicode,
        compare(
            "customer_name",
            DeltaComparison::Gt,
            DeltaScalar::Utf8("\u{00e9}clair".to_owned())
        ),
        ["string-emile.parquet", "string-missing-stats.parquet"]
    );
    Ok(())
}

fn timestamp_add(
    path: &str,
    column: &str,
    min: Option<&str>,
    max: Option<&str>,
    null_count: Option<u64>,
) -> String {
    let min_values = min.map(|value| json!({column: value}));
    let max_values = max.map(|value| json!({column: value}));
    let null_count = null_count.map(|value| json!({column: value}));
    stats_add(path, min_values, max_values, null_count)
}

fn timestamp_fixture(
    name: &str,
    protocol: &str,
    column: &str,
    data_type: &str,
    prefix: &str,
) -> Result<StatisticsFixture, Box<dyn std::error::Error>> {
    let (pre_epoch, low, target, high) = if data_type == "timestamp" {
        (
            "1969-12-31T23:59:59.999999Z",
            "2025-12-31T23:59:59.999999Z",
            "2026-01-01T00:00:00.123456Z",
            "2026-01-01T00:00:00.123457Z",
        )
    } else {
        (
            "1969-12-31 23:59:59.999999",
            "2025-12-31 23:59:59.999999",
            "2026-01-01 00:00:00.123456",
            "2026-01-01 00:00:00.123457",
        )
    };
    StatisticsFixture::new(
        name,
        protocol,
        vec![field(column, data_type)],
        vec![
            timestamp_add(
                &format!("{prefix}-pre-epoch-only.parquet"),
                column,
                Some(pre_epoch),
                Some(pre_epoch),
                Some(0),
            ),
            timestamp_add(
                &format!("{prefix}-low-only.parquet"),
                column,
                Some(low),
                Some(low),
                Some(0),
            ),
            timestamp_add(
                &format!("{prefix}-target-only.parquet"),
                column,
                Some(target),
                Some(target),
                Some(0),
            ),
            timestamp_add(
                &format!("{prefix}-high-only.parquet"),
                column,
                Some(high),
                Some(high),
                Some(0),
            ),
            timestamp_add(
                &format!("{prefix}-range.parquet"),
                column,
                Some(low),
                Some(target),
                Some(0),
            ),
            timestamp_add(
                &format!("{prefix}-target-with-null.parquet"),
                column,
                Some(target),
                Some(target),
                Some(2),
            ),
            timestamp_add(
                &format!("{prefix}-all-null.parquet"),
                column,
                None,
                None,
                Some(10),
            ),
            missing_stats_add(&format!("{prefix}-missing-stats.parquet")),
        ],
    )
}

fn partial_timestamp_fixture(
    name: &str,
    protocol: &str,
    column: &str,
    data_type: &str,
    prefix: &str,
) -> Result<StatisticsFixture, Box<dyn std::error::Error>> {
    let (low, target) = if data_type == "timestamp" {
        ("2025-12-31T23:59:59.999999Z", "2026-01-01T00:00:00.123456Z")
    } else {
        ("2025-12-31 23:59:59.999999", "2026-01-01 00:00:00.123456")
    };
    StatisticsFixture::new(
        name,
        protocol,
        vec![field(column, data_type)],
        vec![
            timestamp_add(
                &format!("{prefix}-min-only-target.parquet"),
                column,
                Some(target),
                None,
                Some(0),
            ),
            timestamp_add(
                &format!("{prefix}-max-only-low.parquet"),
                column,
                None,
                Some(low),
                Some(0),
            ),
            timestamp_add(
                &format!("{prefix}-counts-only.parquet"),
                column,
                None,
                None,
                Some(0),
            ),
            timestamp_add(
                &format!("{prefix}-missing-null-count.parquet"),
                column,
                Some(low),
                Some(target),
                None,
            ),
            missing_stats_add(&format!("{prefix}-missing-stats.parquet")),
        ],
    )
}

fn assert_timestamp_statistics_matrix(
    fixture: &StatisticsFixture,
    partial: &StatisticsFixture,
    column: &str,
    prefix: &str,
    timezone: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let low = 1_767_225_599_999_999_i64;
    let target = 1_767_225_600_123_456_i64;
    let high = 1_767_225_600_123_457_i64;
    let paths = |names: &[&str]| {
        names
            .iter()
            .map(|name| format!("{prefix}-{name}.parquet"))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        fixture.selected_paths(&compare(
            column,
            DeltaComparison::Lt,
            timestamp(target, timezone)
        ))?,
        paths(&["low-only", "missing-stats", "pre-epoch-only", "range"])
    );
    assert_eq!(
        fixture.selected_paths(&compare(
            column,
            DeltaComparison::GtEq,
            timestamp(target, timezone)
        ))?,
        paths(&[
            "high-only",
            "missing-stats",
            "range",
            "target-only",
            "target-with-null"
        ])
    );
    assert_eq!(
        fixture.selected_paths(&compare(
            column,
            DeltaComparison::Lt,
            timestamp(high, timezone)
        ))?,
        paths(&[
            "low-only",
            "missing-stats",
            "pre-epoch-only",
            "range",
            "target-only",
            "target-with-null"
        ])
    );
    assert_eq!(
        fixture.selected_paths(&compare(
            column,
            DeltaComparison::Eq,
            timestamp(low, timezone)
        ))?,
        paths(&["low-only", "missing-stats", "range"])
    );
    assert_eq!(
        fixture.selected_paths(&compare(
            column,
            DeltaComparison::NotEq,
            timestamp(target, timezone)
        ))?,
        paths(&[
            "high-only",
            "low-only",
            "missing-stats",
            "pre-epoch-only",
            "range",
            "target-only",
            "target-with-null"
        ])
    );
    assert_eq!(
        fixture.selected_paths(&is_null(column))?,
        paths(&["all-null", "missing-stats", "target-with-null"])
    );
    assert_eq!(
        fixture.selected_paths(&is_not_null(column))?,
        paths(&[
            "high-only",
            "low-only",
            "missing-stats",
            "pre-epoch-only",
            "range",
            "target-only",
            "target-with-null"
        ])
    );
    assert_eq!(
        partial.selected_paths(&compare(
            column,
            DeltaComparison::Gt,
            timestamp(low, timezone)
        ))?,
        paths(&[
            "counts-only",
            "max-only-low",
            "min-only-target",
            "missing-null-count",
            "missing-stats"
        ])
    );
    assert_eq!(
        partial.selected_paths(&is_null(column))?,
        paths(&["missing-null-count", "missing-stats"])
    );
    assert_eq!(
        partial.selected_paths(&is_not_null(column))?,
        paths(&[
            "counts-only",
            "max-only-low",
            "min-only-target",
            "missing-null-count",
            "missing-stats"
        ])
    );
    Ok(())
}

#[test]
fn timestamp_statistics_pruning_matches_timezone_and_ntz_frozen_boundaries()
-> Result<(), Box<dyn std::error::Error>> {
    let timestamp_complete = timestamp_fixture(
        "timestamp-statistics-parity",
        PROTOCOL_JSON,
        "event_ts",
        "timestamp",
        "timestamp",
    )?;
    let timestamp_partial = partial_timestamp_fixture(
        "timestamp-partial-statistics-parity",
        PROTOCOL_JSON,
        "event_ts",
        "timestamp",
        "timestamp",
    )?;
    assert_timestamp_statistics_matrix(
        &timestamp_complete,
        &timestamp_partial,
        "event_ts",
        "timestamp",
        Some("UTC"),
    )?;

    let ntz_fixture = timestamp_fixture(
        "timestamp-ntz-statistics-parity",
        TIMESTAMP_NTZ_PROTOCOL_JSON,
        "event_ts_ntz",
        "timestamp_ntz",
        "timestamp-ntz",
    )?;
    let ntz_partial = partial_timestamp_fixture(
        "timestamp-ntz-partial-statistics-parity",
        TIMESTAMP_NTZ_PROTOCOL_JSON,
        "event_ts_ntz",
        "timestamp_ntz",
        "timestamp-ntz",
    )?;
    assert_timestamp_statistics_matrix(
        &ntz_fixture,
        &ntz_partial,
        "event_ts_ntz",
        "timestamp-ntz",
        None,
    )?;
    Ok(())
}
