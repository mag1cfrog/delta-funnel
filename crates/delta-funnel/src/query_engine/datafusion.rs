//! DataFusion integration.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::{TableProvider, TableType};
    use datafusion::prelude::SessionContext;

    #[test]
    fn datafusion_table_provider_api_symbols_are_available() -> datafusion::error::Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let table: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(Arc::clone(&schema)));
        let ctx = SessionContext::new();

        ctx.register_table("orders", Arc::clone(&table))?;

        assert_eq!(table.table_type(), TableType::Base);
        assert_eq!(table.schema().as_ref(), schema.as_ref());

        Ok(())
    }
}
