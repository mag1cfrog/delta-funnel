//! End-to-end tests for the public Rust orchestrator API.

use std::{
    error::Error,
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use delta_funnel::{
    DeltaFunnelSession, DeltaSourceConfig, LoadMode, MssqlConnectionConfig, MssqlOutputTarget,
    MssqlTargetConfig, MssqlTargetTable, OutputWritePlan, RunMode, SessionOptions,
};

type TestError = Box<dyn Error + Send + Sync + 'static>;
type TestResult<T> = Result<T, TestError>;

struct DeltaLogFixture {
    path: PathBuf,
}

impl DeltaLogFixture {
    fn new(name: &str, schema_fields_json: &str) -> TestResult<Self> {
        let path = env_unique_path(name)?;
        let log_dir = path.join("_delta_log");
        fs::create_dir_all(&log_dir)?;
        fs::write(
            log_dir.join("00000000000000000000.json"),
            format!(
                "{}\n{}\n",
                r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
                metadata_json(schema_fields_json)
            ),
        )?;

        Ok(Self { path })
    }

    fn uri(&self) -> String {
        self.path.to_string_lossy().to_string()
    }
}

impl Drop for DeltaLogFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[tokio::test]
async fn dry_run_plans_one_source_one_output_through_public_api() -> TestResult<()> {
    let orders = DeltaLogFixture::new("orders", ORDERS_SCHEMA_FIELDS_JSON)?;
    let mut session = session_with_default_connection()?;
    let orders_table = session.delta_lake(DeltaSourceConfig::new("orders", orders.uri()))?;
    let selected_orders = session
        .table_from_sql("select id, region from orders")
        .await?;
    let output = output_request(
        selected_orders,
        "orders_output",
        "orders_sink",
        LoadMode::AppendExisting,
        RunMode::DryRun,
    )?;

    let report = session.dry_run_to_mssql(&output)?;

    assert_eq!(orders_table.name(), "orders");
    assert_eq!(report.output_name(), "orders_output");
    assert_eq!(report.run_mode(), RunMode::DryRun);
    assert_eq!(
        report
            .planned_output()
            .output_plan()
            .schema_mappings()
            .len(),
        2
    );
    assert!(!report.sql_server_contacted());
    assert!(!report.row_production_started());
    assert!(!report.table_lifecycle_started());
    assert!(!report.bulk_writer_started());
    Ok(())
}

#[tokio::test]
async fn dry_run_plans_multi_source_join_through_public_api() -> TestResult<()> {
    let orders = DeltaLogFixture::new("orders", ORDERS_SCHEMA_FIELDS_JSON)?;
    let customers = DeltaLogFixture::new("customers", CUSTOMERS_SCHEMA_FIELDS_JSON)?;
    let mut session = session_with_default_connection()?;
    session.delta_lake(DeltaSourceConfig::new("orders", orders.uri()))?;
    session.delta_lake(DeltaSourceConfig::new("customers", customers.uri()))?;
    let joined = session
        .table_from_sql(
            "select o.id, c.customer_name \
             from orders o \
             join customers c on o.customer_id = c.id",
        )
        .await?;
    let output = output_request(
        joined,
        "joined_output",
        "joined_sink",
        LoadMode::AppendExisting,
        RunMode::DryRun,
    )?;

    let report = session.dry_run_to_mssql(&output)?;

    assert_eq!(report.output_name(), "joined_output");
    assert_eq!(
        report
            .planned_output()
            .output_plan()
            .schema_mappings()
            .len(),
        2
    );
    assert!(!report.sql_server_contacted());
    assert!(!report.row_production_started());
    Ok(())
}

#[tokio::test]
async fn dry_run_plans_shared_derived_table_for_two_outputs() -> TestResult<()> {
    let orders = DeltaLogFixture::new("orders", ORDERS_SCHEMA_FIELDS_JSON)?;
    let mut session = session_with_default_connection()?;
    session.delta_lake(DeltaSourceConfig::new("orders", orders.uri()))?;
    let pending_big = session
        .table_from_sql("select id, region from orders")
        .await?;
    let _big = session.register_alias("big_orders", &pending_big)?;
    let west = session
        .table_from_sql("select id from big_orders where region = 'west'")
        .await?;
    let east = session
        .table_from_sql("select id from big_orders where region = 'east'")
        .await?;
    let west = output_request(
        west,
        "west_output",
        "west_orders",
        LoadMode::AppendExisting,
        RunMode::DryRun,
    )?;
    let east = output_request(
        east,
        "east_output",
        "east_orders",
        LoadMode::CreateAndLoad,
        RunMode::DryRun,
    )?;

    let report = session.dry_run_all_to_mssql(&[west, east])?;

    assert_eq!(report.len(), 2);
    assert_eq!(report.outputs()[0].output_name(), "west_output");
    assert_eq!(report.outputs()[1].output_name(), "east_output");
    assert_eq!(
        report.outputs()[0]
            .planned_output()
            .output_plan()
            .schema_mappings()
            .len(),
        1
    );
    assert_eq!(
        report.outputs()[1]
            .planned_output()
            .output_plan()
            .schema_mappings()
            .len(),
        1
    );
    assert!(!report.sql_server_contacted());
    assert!(!report.row_production_started());
    assert!(!report.table_lifecycle_started());
    assert!(!report.bulk_writer_started());
    Ok(())
}

fn session_with_default_connection() -> TestResult<DeltaFunnelSession> {
    let connection = MssqlConnectionConfig::new(
        "server=tcp:sql.example.com;database=warehouse;user=admin;password=secret-token",
    )?
    .with_display_label("integration-test");

    Ok(DeltaFunnelSession::new(
        SessionOptions::new().with_default_mssql_connection(connection),
    )?)
}

fn output_request(
    table: delta_funnel::LazyTable,
    output_name: &str,
    target_table: &str,
    load_mode: LoadMode,
    run_mode: RunMode,
) -> TestResult<OutputWritePlan> {
    let target = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", target_table)?)
        .with_load_mode(load_mode);

    Ok(OutputWritePlan::new(
        table,
        MssqlOutputTarget::new(output_name, target, run_mode),
    ))
}

fn metadata_json(schema_fields_json: &str) -> String {
    format!(
        r#"{{"metaData":{{"id":"delta-funnel-e2e-test","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{{\"type\":\"struct\",\"fields\":{schema_fields_json}}}","partitionColumns":[],"configuration":{{}},"createdTime":1587968585495}}}}"#
    )
}

fn env_unique_path(name: &str) -> TestResult<PathBuf> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(std::env::temp_dir().join(format!(
        "delta-funnel-e2e-{}-{name}-{nanos}",
        std::process::id()
    )))
}

const ORDERS_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;
const CUSTOMERS_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;
