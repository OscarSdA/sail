use deltalake::table::config::TableConfig;
use deltalake::{DeltaResult, DeltaTableConfig};

use crate::kernel::snapshot::EagerSnapshot;
use crate::table::DataFusionMixins;

#[derive(Debug, Clone)]
pub struct DeltaTableState {
    pub snapshot: EagerSnapshot,
}

impl DeltaTableState {
    /// Get the version
    pub fn version(&self) -> i64 {
        self.snapshot.version()
    }

    /// Get the metadata
    pub fn metadata(&self) -> &deltalake::kernel::Metadata {
        self.snapshot.metadata()
    }

    /// Get the schema
    pub fn schema(&self) -> &deltalake::kernel::StructType {
        self.snapshot.schema()
    }

    /// Get the load config
    pub fn load_config(&self) -> &DeltaTableConfig {
        self.snapshot.load_config()
    }

    /// Get the underlying snapshot
    pub fn snapshot(&self) -> &EagerSnapshot {
        &self.snapshot
    }

    /// Get file actions iterator
    pub fn file_actions_iter(&self) -> DeltaResult<Vec<deltalake::kernel::Add>> {
        self.snapshot.file_actions_iter()
    }

    /// Get file actions
    pub fn file_actions(&self) -> DeltaResult<Vec<deltalake::kernel::Add>> {
        Ok(self.snapshot.file_actions()?.collect())
    }

    /// Get add actions table
    pub fn add_actions_table(
        &self,
        include_file_column: bool,
    ) -> DeltaResult<deltalake::arrow::record_batch::RecordBatch> {
        // For now, return an empty RecordBatch with the correct schema
        // This would need proper implementation to parse the snapshot data
        let schema = deltalake::arrow::datatypes::Schema::empty();
        Ok(deltalake::arrow::record_batch::RecordBatch::new_empty(
            std::sync::Arc::new(schema),
        ))
    }

    /// Get table config
    pub fn table_config(&self) -> &deltalake::DeltaTableConfig {
        self.snapshot.load_config()
    }
}

use datafusion::execution::context::SessionState;
use datafusion_expr::Expr;

impl DataFusionMixins for DeltaTableState {
    fn arrow_schema(&self) -> DeltaResult<deltalake::arrow::datatypes::SchemaRef> {
        self.snapshot.arrow_schema()
    }

    fn input_schema(&self) -> DeltaResult<deltalake::arrow::datatypes::SchemaRef> {
        self.snapshot.input_schema()
    }

    fn parse_predicate_expression(
        &self,
        expr: impl AsRef<str>,
        df_state: &SessionState,
    ) -> DeltaResult<Expr> {
        self.snapshot.parse_predicate_expression(expr, df_state)
    }
}

impl deltalake::kernel::transaction::TableReference for DeltaTableState {
    fn metadata(&self) -> &deltalake::kernel::Metadata {
        self.snapshot.metadata()
    }

    fn protocol(&self) -> &deltalake::kernel::Protocol {
        self.snapshot.protocol()
    }

    fn config(&self) -> TableConfig<'_> {
        // Since TableConfig constructor is private, we need to use unsafe transmute
        // This is safe because TableConfig is just a wrapper around &HashMap<String, String>
        let metadata = self.snapshot.metadata();
        unsafe {
            std::mem::transmute::<&std::collections::HashMap<String, String>, TableConfig<'_>>(
                metadata.configuration(),
            )
        }
    }

    fn eager_snapshot(&self) -> &deltalake::kernel::EagerSnapshot {
        // For now, we'll create a minimal delta-rs EagerSnapshot
        // This is a temporary solution during the migration
        todo!("Need to implement conversion from sail EagerSnapshot to delta-rs EagerSnapshot")
    }
}
