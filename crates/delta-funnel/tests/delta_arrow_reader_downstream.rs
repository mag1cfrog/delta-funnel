//! Staging-only proof that Delta Funnel can consume the extracted reader.

use std::path::Path;

use datafusion::{arrow::record_batch::RecordBatch, assert_batches_eq, prelude::SessionContext};
use delta_arrow_reader::{DeltaDataFusionScanOptions, DeltaTableBuilder, register_delta_table};
use futures_util::TryStreamExt;

#[tokio::test]
async fn direct_and_datafusion_public_types_cross_the_crate_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let table_uri = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("type-widening")
        .to_string_lossy()
        .into_owned();
    let table = DeltaTableBuilder::new(table_uri).load_async().await?;

    let scan = table
        .scan()
        .with_projection(vec!["byte_long".into()])
        .build()
        .await?;
    let direct_batches: Vec<RecordBatch> = scan.execute().await?.try_collect().await?;
    assert_eq!(
        direct_batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        2
    );

    let context = SessionContext::new();
    register_delta_table(
        &context,
        "widened",
        table,
        DeltaDataFusionScanOptions::default(),
    )?;
    let query_batches = context
        .sql("select byte_long from widened order by byte_long")
        .await?
        .collect()
        .await?;
    assert_batches_eq!(
        [
            "+---------------------+",
            "| byte_long           |",
            "+---------------------+",
            "| 1                   |",
            "| 9223372036854775807 |",
            "+---------------------+",
        ],
        &query_batches
    );

    Ok(())
}
