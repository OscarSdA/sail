use std::collections::HashMap;

use deltalake::TableProperty;

macro_rules! table_config {
    ($(($docs:literal, $key:expr, $name:ident, $ret:ty, $default:literal),)*) => {
        $(
            #[doc = $docs]
            pub fn $name(&self) -> $ret {
                self.0
                    .get($key.as_ref())
                    .and_then(|value| value.parse().ok())
                    .unwrap_or($default)
            }
        )*
    }
}

/// Well known delta table configuration
#[derive(Debug)]
pub struct TableConfig<'a>(pub(crate) &'a HashMap<String, String>);

/// Default num index cols
pub const DEFAULT_NUM_INDEX_COLS: i32 = 32;
/// Default target file size
pub const DEFAULT_TARGET_FILE_SIZE: i64 = 104857600;

impl TableConfig<'_> {
    table_config!(
        (
            "true for this Delta table to be append-only",
            TableProperty::AppendOnly,
            append_only,
            bool,
            false
        ),
        (
            "true for Delta Lake to write file statistics in checkpoints in JSON format for the stats column.",
            TableProperty::CheckpointWriteStatsAsJson,
            write_stats_as_json,
            bool,
            true
        ),
        (
            "true for Delta Lake to write file statistics to checkpoints in struct format",
            TableProperty::CheckpointWriteStatsAsStruct,
            write_stats_as_struct,
            bool,
            false
        ),
        (
            "true for Delta Lake to write checkpoint files using run length encoding (RLE)",
            TableProperty::CheckpointUseRunLengthEncoding,
            use_checkpoint_rle,
            bool,
            true
        ),
        (
            "The target file size in bytes or higher units for file tuning",
            TableProperty::TargetFileSize,
            target_file_size,
            i64,
            // Databricks / spark defaults to 104857600 (bytes) or 100mb
            104857600
        ),
        (
            "true to enable change data feed.",
            TableProperty::EnableChangeDataFeed,
            enable_change_data_feed,
            bool,
            false
        ),
        (
            "true to enable deletion vectors and predictive I/O for updates.",
            TableProperty::EnableDeletionVectors,
            enable_deletion_vectors,
            bool,
            // in databricks the default is dependent on the workspace settings and runtime version
            // https://learn.microsoft.com/en-us/azure/databricks/administration-guide/workspace-settings/deletion-vectors
            false
        ),
        (
            "The number of columns for Delta Lake to collect statistics about for data skipping.",
            TableProperty::DataSkippingNumIndexedCols,
            num_indexed_cols,
            i32,
            32
        ),
        (
            "whether to cleanup expired logs",
            TableProperty::EnableExpiredLogCleanup,
            enable_expired_log_cleanup,
            bool,
            true
        ),
        (
            "Interval (number of commits) after which a new checkpoint should be created",
            TableProperty::CheckpointInterval,
            checkpoint_interval,
            i32,
            100
        ),
    );
}
