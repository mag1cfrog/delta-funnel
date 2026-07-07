//! Environment-gated SQL Server integration tests.
//!
//! These tests skip when the SQL Server test connection environment variables
//! are absent, so default workspace test runs do not require SQL Server.

use std::{
    env,
    error::Error,
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow_tiberius::{TableName, connect_mssql_client_from_ado_string};
use datafusion::arrow::{
    array::{ArrayRef, Int32Array, Int64Array, TimestampNanosecondArray},
    record_batch::RecordBatch,
};
use delta_funnel::{
    DeltaFunnelError, DeltaFunnelRuntime, DeltaFunnelSession, LoadMode, MssqlConnectionConfig,
    MssqlOutputTarget, MssqlSchemaPlanOptions, MssqlTargetCleanupStatus, MssqlTargetConfig,
    MssqlTargetResolutionContext, MssqlTargetTable, MssqlTimestampPolicy, MssqlWriteBackend,
    OutputWritePlan, RowCount, RunMode, SessionOptions, ValidationStatus,
    default_mssql_write_backend, write_output_batches_to_mssql,
};
use futures_util::stream;

const CONNECTION_STRING_ENV: &str = "DELTA_FUNNEL_MSSQL_TEST_CONNECTION_STRING";
const SCHEMA_ENV: &str = "DELTA_FUNNEL_MSSQL_TEST_SCHEMA";
const APPEND_EXISTING_OUTPUT_NAME: &str = "mssql_direct_raw_bulk_append_orders";
const CREATE_AND_LOAD_OUTPUT_NAME: &str = "mssql_direct_raw_bulk_create_orders";
const ORCHESTRATOR_OUTPUT_NAME: &str = "mssql_orchestrator_runtime_orders";
const TIMESTAMP_NS_DATETIME_OUTPUT_NAME: &str = "mssql_direct_raw_bulk_timestamp_ns_datetime";
const EXPECTED_ORDER_IDS: &[i64] = &[101, 102, 103];
const APPEND_EXISTING_EXPECTED_ORDER_IDS: &[i64] = &[99, 101, 102, 103];
static NEXT_TABLE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

type TestError = Box<dyn Error + Send + Sync + 'static>;
type TestResult<T> = Result<T, TestError>;

struct MssqlIntegrationConfig {
    connection_string: String,
    schema: String,
}

enum MssqlIntegrationGate {
    Configured(MssqlIntegrationConfig),
    Skipped { missing: Vec<&'static str> },
}

impl MssqlIntegrationConfig {
    fn from_env() -> MssqlIntegrationGate {
        Self::from_values(
            env::var(CONNECTION_STRING_ENV).ok(),
            env::var(SCHEMA_ENV).ok(),
        )
    }

    fn from_values(
        connection_string: Option<String>,
        schema: Option<String>,
    ) -> MssqlIntegrationGate {
        let connection_string = non_empty_value(connection_string);
        let schema = non_empty_value(schema);
        let mut missing = Vec::new();

        if connection_string.is_none() {
            missing.push(CONNECTION_STRING_ENV);
        }
        if schema.is_none() {
            missing.push(SCHEMA_ENV);
        }

        match (connection_string, schema) {
            (Some(connection_string), Some(schema)) => {
                MssqlIntegrationGate::Configured(MssqlIntegrationConfig {
                    connection_string,
                    schema,
                })
            }
            _ => MssqlIntegrationGate::Skipped { missing },
        }
    }
}

#[test]
fn mssql_integration_config_reports_missing_env_names_without_secret_values() -> TestResult<()> {
    let gate = MssqlIntegrationConfig::from_values(
        Some("server=localhost;user=sa;password=secret".to_owned()),
        Some("   ".to_owned()),
    );

    let MssqlIntegrationGate::Skipped { missing } = gate else {
        return Err(test_error("expected integration config to be skipped"));
    };

    assert_eq!(missing, vec![SCHEMA_ENV]);

    Ok(())
}

#[tokio::test]
async fn mssql_direct_raw_bulk_append_existing_writes_two_batches_when_configured() -> TestResult<()>
{
    let Some(config) = configured_or_skip() else {
        return Ok(());
    };

    run_append_existing_direct_raw_bulk_test(config).await
}

#[tokio::test]
async fn mssql_direct_raw_bulk_create_and_load_writes_two_batches_when_configured() -> TestResult<()>
{
    let Some(config) = configured_or_skip() else {
        return Ok(());
    };

    run_create_and_load_direct_raw_bulk_test(config).await
}

#[tokio::test]
async fn mssql_direct_raw_bulk_round_trips_non_nullable_timestamp_ns_datetime_when_configured()
-> TestResult<()> {
    let Some(config) = configured_or_skip() else {
        return Ok(());
    };

    run_non_nullable_timestamp_ns_datetime_test(config).await
}

#[test]
fn mssql_orchestrator_runtime_create_and_load_writes_query_output_when_configured() -> TestResult<()>
{
    let Some(config) = configured_or_skip() else {
        return Ok(());
    };

    run_create_and_load_orchestrator_runtime_test(config)
}

async fn run_append_existing_direct_raw_bulk_test(
    config: MssqlIntegrationConfig,
) -> TestResult<()> {
    let table = unique_table_name(&config.schema)?;
    let mut admin = connect_mssql_client_from_ado_string(&config.connection_string).await?;

    drop_table_if_exists(&mut admin, &table).await?;
    let write_result = async {
        create_append_existing_table(&mut admin, &table).await?;
        if !admin.table_exists(&table).await? {
            return Err(test_error(format!(
                "created MSSQL test table is not visible: {}",
                table.quoted_sql()
            )));
        }

        let report = write_order_id_batches(
            &config,
            table.table().as_str(),
            LoadMode::AppendExisting,
            APPEND_EXISTING_OUTPUT_NAME,
        )
        .await?;
        assert_order_ids_persisted(&mut admin, &table, APPEND_EXISTING_EXPECTED_ORDER_IDS).await?;

        Ok(report)
    }
    .await;
    let cleanup_result = drop_table_if_exists(&mut admin, &table).await;

    match (write_result, cleanup_result) {
        (Ok(report), Ok(())) => {
            assert_eq!(report.stats().output_name(), APPEND_EXISTING_OUTPUT_NAME);
            assert_eq!(report.stats().rows_written(), 3);
            assert_eq!(report.stats().batches_written(), 2);
            assert_eq!(report.cleanup(), MssqlTargetCleanupStatus::NotApplicable);
            assert_eq!(report.output_row_count(), RowCount::exact(3));
            assert_eq!(report.target_row_count_before_write(), RowCount::exact(1));
            assert_eq!(report.target_row_count_after_write(), RowCount::exact(4));
            assert_eq!(report.target_row_count(), RowCount::exact(4));
            assert_eq!(report.validation_status(), ValidationStatus::passed());
            Ok(())
        }
        (Err(write_error), Ok(())) => Err(write_error),
        (Ok(report), Err(cleanup_error)) => Err(test_error(format!(
            "write succeeded with {}; cleanup failed: {cleanup_error}",
            write_report_summary(&report)
        ))),
        (Err(write_error), Err(cleanup_error)) => Err(test_error(format!(
            "write failed: {write_error}; cleanup failed: {cleanup_error}"
        ))),
    }
}

async fn run_create_and_load_direct_raw_bulk_test(
    config: MssqlIntegrationConfig,
) -> TestResult<()> {
    let table = unique_table_name(&config.schema)?;
    let mut admin = connect_mssql_client_from_ado_string(&config.connection_string).await?;

    drop_table_if_exists(&mut admin, &table).await?;
    let write_result = async {
        let report = write_order_id_batches(
            &config,
            table.table().as_str(),
            LoadMode::CreateAndLoad,
            CREATE_AND_LOAD_OUTPUT_NAME,
        )
        .await?;
        if !admin.table_exists(&table).await? {
            return Err(test_error(format!(
                "created MSSQL test table is not visible: {}",
                table.quoted_sql()
            )));
        }
        assert_order_ids_persisted(&mut admin, &table, EXPECTED_ORDER_IDS).await?;

        Ok(report)
    }
    .await;
    let cleanup_result = drop_table_if_exists(&mut admin, &table).await;

    match (write_result, cleanup_result) {
        (Ok(report), Ok(())) => {
            assert_eq!(report.stats().output_name(), CREATE_AND_LOAD_OUTPUT_NAME);
            assert_eq!(report.stats().rows_written(), 3);
            assert_eq!(report.stats().batches_written(), 2);
            assert_eq!(report.cleanup(), MssqlTargetCleanupStatus::NotAttempted);
            assert_eq!(report.output_row_count(), RowCount::exact(3));
            assert_eq!(report.target_row_count(), RowCount::exact(3));
            assert_eq!(report.validation_status(), ValidationStatus::passed());
            Ok(())
        }
        (Err(write_error), Ok(())) => Err(write_error),
        (Ok(report), Err(cleanup_error)) => Err(test_error(format!(
            "write succeeded with {}; cleanup failed: {cleanup_error}",
            write_report_summary(&report)
        ))),
        (Err(write_error), Err(cleanup_error)) => Err(test_error(format!(
            "write failed: {write_error}; cleanup failed: {cleanup_error}"
        ))),
    }
}

async fn run_non_nullable_timestamp_ns_datetime_test(
    config: MssqlIntegrationConfig,
) -> TestResult<()> {
    for backend in [
        MssqlWriteBackend::BaselineTokenRow,
        MssqlWriteBackend::DirectRawBulk,
    ] {
        let table = unique_table_name(&config.schema)?;
        let mut admin = connect_mssql_client_from_ado_string(&config.connection_string).await?;

        drop_table_if_exists(&mut admin, &table).await?;
        let write_result = async {
            let report = write_timestamp_ns_datetime_batch(
                &config,
                table.table().as_str(),
                backend,
                TIMESTAMP_NS_DATETIME_OUTPUT_NAME,
            )
            .await?;
            if !admin.table_exists(&table).await? {
                return Err(test_error(format!(
                    "created MSSQL test table is not visible: {}",
                    table.quoted_sql()
                )));
            }
            assert_timestamp_ns_datetime_values_persisted(&mut admin, &table).await?;

            Ok(report)
        }
        .await;
        let cleanup_result = drop_table_if_exists(&mut admin, &table).await;

        match (write_result, cleanup_result) {
            (Ok(report), Ok(())) => {
                assert_eq!(
                    report.stats().output_name(),
                    TIMESTAMP_NS_DATETIME_OUTPUT_NAME
                );
                assert_eq!(report.stats().rows_written(), 3);
                assert_eq!(report.stats().batches_written(), 1);
                assert_eq!(report.cleanup(), MssqlTargetCleanupStatus::NotAttempted);
                assert_eq!(report.output_row_count(), RowCount::exact(3));
                assert_eq!(report.target_row_count(), RowCount::exact(3));
                assert_eq!(report.validation_status(), ValidationStatus::passed());
            }
            (Err(write_error), Ok(())) => return Err(write_error),
            (Ok(report), Err(cleanup_error)) => {
                return Err(test_error(format!(
                    "write succeeded with {}; cleanup failed: {cleanup_error}",
                    write_report_summary(&report)
                )));
            }
            (Err(write_error), Err(cleanup_error)) => {
                return Err(test_error(format!(
                    "write failed: {write_error}; cleanup failed: {cleanup_error}"
                )));
            }
        }
    }

    Ok(())
}

fn run_create_and_load_orchestrator_runtime_test(config: MssqlIntegrationConfig) -> TestResult<()> {
    let table = unique_table_name(&config.schema)?;
    let admin_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    admin_runtime.block_on(async {
        let mut admin = connect_mssql_client_from_ado_string(&config.connection_string).await?;
        drop_table_if_exists(&mut admin, &table).await
    })?;

    let write_result = write_orchestrator_runtime_order_ids(&config, table.table().as_str());

    let cleanup_result = admin_runtime.block_on(async {
        let mut admin = connect_mssql_client_from_ado_string(&config.connection_string).await?;
        let assertion_result = async {
            if !admin.table_exists(&table).await? {
                return Err(test_error(format!(
                    "created MSSQL test table is not visible: {}",
                    table.quoted_sql()
                )));
            }
            assert_order_ids_persisted(&mut admin, &table, EXPECTED_ORDER_IDS).await
        }
        .await;
        let cleanup_result = drop_table_if_exists(&mut admin, &table).await;

        match (assertion_result, cleanup_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(assertion_error), Ok(())) => Err(assertion_error),
            (Ok(()), Err(cleanup_error)) => Err(cleanup_error),
            (Err(assertion_error), Err(cleanup_error)) => Err(test_error(format!(
                "assertion failed: {assertion_error}; cleanup failed: {cleanup_error}"
            ))),
        }
    });

    match (write_result, cleanup_result) {
        (Ok(report), Ok(())) => {
            assert_eq!(report.stats().output_name(), ORCHESTRATOR_OUTPUT_NAME);
            assert_eq!(report.stats().rows_written(), 3);
            assert!(report.stats().batches_written() >= 1);
            assert_eq!(report.cleanup(), MssqlTargetCleanupStatus::NotAttempted);
            assert_eq!(report.output_row_count(), RowCount::exact(3));
            assert_eq!(report.target_row_count(), RowCount::exact(3));
            assert_eq!(report.validation_status(), ValidationStatus::passed());
            Ok(())
        }
        (Err(write_error), Ok(())) => Err(write_error),
        (Ok(report), Err(cleanup_error)) => Err(test_error(format!(
            "write succeeded with {}; cleanup failed: {cleanup_error}",
            write_report_summary(&report)
        ))),
        (Err(write_error), Err(cleanup_error)) => Err(test_error(format!(
            "write failed: {write_error}; cleanup failed: {cleanup_error}"
        ))),
    }
}

async fn write_order_id_batches(
    config: &MssqlIntegrationConfig,
    table_name: &str,
    load_mode: LoadMode,
    output_name: &str,
) -> TestResult<delta_funnel::MssqlWriteReport> {
    let output_schema = order_id_schema();
    let connection = MssqlConnectionConfig::new(config.connection_string.clone())?
        .with_display_label("mssql-direct-raw-bulk-integration");
    let target_table = MssqlTargetTable::new(config.schema.clone(), table_name.to_owned())?;
    let target = MssqlTargetConfig::new(target_table)
        .with_load_mode(load_mode)
        .resolve(MssqlTargetResolutionContext {
            output_name: Some(output_name),
            default_connection: Some(&connection),
        })?;

    let first = order_id_batch(Arc::clone(&output_schema), vec![101, 102])?;
    let second = order_id_batch(Arc::clone(&output_schema), vec![103])?;
    let batches = stream::iter(vec![
        Ok::<RecordBatch, DeltaFunnelError>(first),
        Ok::<RecordBatch, DeltaFunnelError>(second),
    ]);

    Ok(write_output_batches_to_mssql(
        output_schema.as_ref(),
        target,
        MssqlSchemaPlanOptions::default(),
        batches,
        default_mssql_write_backend(),
    )
    .await?)
}

async fn write_timestamp_ns_datetime_batch(
    config: &MssqlIntegrationConfig,
    table_name: &str,
    backend: MssqlWriteBackend,
    output_name: &str,
) -> TestResult<delta_funnel::MssqlWriteReport> {
    let output_schema = timestamp_ns_datetime_schema();
    let connection = MssqlConnectionConfig::new(config.connection_string.clone())?
        .with_display_label("mssql-timestamp-ns-datetime-integration");
    let target_table = MssqlTargetTable::new(config.schema.clone(), table_name.to_owned())?;
    let target = MssqlTargetConfig::new(target_table)
        .with_load_mode(LoadMode::CreateAndLoad)
        .resolve(MssqlTargetResolutionContext {
            output_name: Some(output_name),
            default_connection: Some(&connection),
        })?;
    let schema_options = MssqlSchemaPlanOptions {
        timestamp_policy: MssqlTimestampPolicy::DateTime,
        ..MssqlSchemaPlanOptions::default()
    };
    let batch = timestamp_ns_datetime_batch(Arc::clone(&output_schema))?;
    let batches = stream::iter(vec![Ok::<RecordBatch, DeltaFunnelError>(batch)]);

    Ok(write_output_batches_to_mssql(
        output_schema.as_ref(),
        target,
        schema_options,
        batches,
        backend,
    )
    .await?)
}

fn write_orchestrator_runtime_order_ids(
    config: &MssqlIntegrationConfig,
    table_name: &str,
) -> TestResult<delta_funnel::MssqlWriteReport> {
    let connection = MssqlConnectionConfig::new(config.connection_string.clone())?
        .with_display_label("mssql-orchestrator-runtime-integration");
    let runtime = DeltaFunnelRuntime::new()?;
    let mut session =
        DeltaFunnelSession::new(SessionOptions::new().with_default_mssql_connection(connection))?;
    let orders = runtime.table_from_sql(
        &mut session,
        "\
select cast(101 as bigint) as order_id
union all select cast(102 as bigint) as order_id
union all select cast(103 as bigint) as order_id",
    )?;
    let target = MssqlTargetConfig::new(MssqlTargetTable::new(
        config.schema.clone(),
        table_name.to_owned(),
    )?)
    .with_load_mode(LoadMode::CreateAndLoad);
    let request = OutputWritePlan::new(
        orders,
        MssqlOutputTarget::new(ORCHESTRATOR_OUTPUT_NAME, target, RunMode::Execute),
    );

    Ok(runtime.write_to_mssql(&session, &request)?)
}

async fn create_append_existing_table(
    client: &mut arrow_tiberius::ConnectedMssqlClient,
    table: &TableName,
) -> TestResult<()> {
    client
        .execute_statement(&format!(
            "\
CREATE TABLE {} ([order_id] BIGINT NOT NULL);
INSERT INTO {} ([order_id]) VALUES (CAST(99 AS BIGINT));",
            table.quoted_sql(),
            table.quoted_sql()
        ))
        .await?;

    Ok(())
}

async fn assert_order_ids_persisted(
    client: &mut arrow_tiberius::ConnectedMssqlClient,
    table: &TableName,
    expected_order_ids: &[i64],
) -> TestResult<()> {
    client
        .execute_statement(&order_id_assertion_sql(table, expected_order_ids))
        .await?;

    Ok(())
}

async fn assert_timestamp_ns_datetime_values_persisted(
    client: &mut arrow_tiberius::ConnectedMssqlClient,
    table: &TableName,
) -> TestResult<()> {
    client
        .execute_statement(&timestamp_ns_datetime_assertion_sql(table))
        .await?;

    Ok(())
}

async fn drop_table_if_exists(
    client: &mut arrow_tiberius::ConnectedMssqlClient,
    table: &TableName,
) -> TestResult<()> {
    client
        .execute_statement(&format!("DROP TABLE IF EXISTS {};", table.quoted_sql()))
        .await?;

    Ok(())
}

fn unique_table_name(schema: &str) -> TestResult<TableName> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|source| test_error(format!("system clock is before UNIX_EPOCH: {source}")))?
        .as_nanos();
    let sequence = NEXT_TABLE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let table = format!(
        "df_mssql_it_{}_{}_{}",
        std::process::id(),
        timestamp,
        sequence
    );

    Ok(TableName::new(schema.to_owned(), table)?)
}

fn order_id_assertion_sql(table: &TableName, expected_order_ids: &[i64]) -> String {
    let expected_rows = expected_order_ids
        .iter()
        .map(|order_id| format!("(CAST({order_id} AS BIGINT))"))
        .collect::<Vec<_>>()
        .join(", ");
    let row_count = expected_order_ids.len();
    let table = table.quoted_sql();

    format!(
        "\
IF (SELECT COUNT_BIG(*) FROM {table}) <> {row_count}
BEGIN
    RAISERROR('unexpected DirectRawBulk row count', 16, 1);
    RETURN;
END;
IF EXISTS (
    SELECT [order_id] FROM {table}
    EXCEPT
    SELECT [expected].[order_id]
    FROM (VALUES {expected_rows}) AS [expected]([order_id])
)
OR EXISTS (
    SELECT [expected].[order_id]
    FROM (VALUES {expected_rows}) AS [expected]([order_id])
    EXCEPT
    SELECT [order_id] FROM {table}
)
BEGIN
    RAISERROR('unexpected DirectRawBulk order_id values', 16, 1);
    RETURN;
END;"
    )
}

fn timestamp_ns_datetime_assertion_sql(table: &TableName) -> String {
    let expected_rows = timestamp_ns_datetime_cases()
        .iter()
        .map(|(row_id, _nanos, literal)| {
            format!("({row_id}, CAST(CAST(N'{literal}' AS datetime2(6)) AS datetime))")
        })
        .collect::<Vec<_>>()
        .join(", ");
    let table = table.quoted_sql();

    format!(
        "\
IF (SELECT COUNT_BIG(*) FROM {table}) <> 3
BEGIN
    RAISERROR('unexpected timestamp datetime row count', 16, 1);
    RETURN;
END;
IF EXISTS (
    SELECT [id], [event_time] FROM {table}
    EXCEPT
    SELECT [expected].[id], [expected].[event_time]
    FROM (VALUES {expected_rows}) AS [expected]([id], [event_time])
)
OR EXISTS (
    SELECT [expected].[id], [expected].[event_time]
    FROM (VALUES {expected_rows}) AS [expected]([id], [event_time])
    EXCEPT
    SELECT [id], [event_time] FROM {table}
)
BEGIN
    RAISERROR('unexpected timestamp datetime values', 16, 1);
    RETURN;
END;"
    )
}

fn order_id_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "order_id",
        DataType::Int64,
        false,
    )]))
}

fn order_id_batch(schema: SchemaRef, values: Vec<i64>) -> TestResult<RecordBatch> {
    let order_ids: ArrayRef = Arc::new(Int64Array::from(values));

    Ok(RecordBatch::try_new(schema, vec![order_ids])?)
}

fn timestamp_ns_datetime_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]))
}

fn timestamp_ns_datetime_batch(schema: SchemaRef) -> TestResult<RecordBatch> {
    let cases = timestamp_ns_datetime_cases();
    let ids: ArrayRef = Arc::new(Int32Array::from(
        cases
            .iter()
            .map(|(row_id, _nanos, _literal)| *row_id)
            .collect::<Vec<_>>(),
    ));
    let event_times: ArrayRef = Arc::new(TimestampNanosecondArray::from(
        cases
            .iter()
            .map(|(_row_id, nanos, _literal)| *nanos)
            .collect::<Vec<_>>(),
    ));

    Ok(RecordBatch::try_new(schema, vec![ids, event_times])?)
}

fn timestamp_ns_datetime_cases() -> [(i32, i64, &'static str); 3] {
    [
        (1, 1_780_529_793_687_000_000, "2026-06-03T23:36:33.687000"),
        (2, 1_778_615_767_493_000_000, "2026-05-12T19:56:07.493000"),
        (3, 1_774_840_482_427_000_000, "2026-03-30T03:14:42.427000"),
    ]
}

fn non_empty_value(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let value = value.trim().to_owned();
        (!value.is_empty()).then_some(value)
    })
}

fn configured_or_skip() -> Option<MssqlIntegrationConfig> {
    match MssqlIntegrationConfig::from_env() {
        MssqlIntegrationGate::Configured(config) => Some(config),
        MssqlIntegrationGate::Skipped { missing } => {
            eprintln!(
                "skipping MSSQL DirectRawBulk integration test; missing {}",
                missing.join(", ")
            );
            None
        }
    }
}

fn write_report_summary(report: &delta_funnel::MssqlWriteReport) -> String {
    format!(
        "output `{}`, rows {}, batches {}, cleanup {}",
        report.stats().output_name(),
        report.stats().rows_written(),
        report.stats().batches_written(),
        report.cleanup()
    )
}

fn test_error(message: impl Into<String>) -> TestError {
    Box::new(std::io::Error::other(message.into()))
}
