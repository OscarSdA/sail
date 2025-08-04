use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::Duration;

use deltalake::table::config::IsolationLevel;
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

    pub fn isolation_level(&self) -> IsolationLevel {
        self.0
            .get(TableProperty::IsolationLevel.as_ref())
            .and_then(|v| v.parse().ok())
            .unwrap_or_default()
    }

    /// How long the history for a Delta table is kept.
    ///
    /// Each time a checkpoint is written, Delta Lake automatically cleans up log entries older
    /// than the retention interval. If you set this property to a large enough value, many log
    /// entries are retained. This should not impact performance as operations against the log are
    /// constant time. Operations on history are parallel but will become more expensive as the log size increases.
    pub fn log_retention_duration(&self) -> Duration {
        static DEFAULT_DURATION: LazyLock<Duration> =
            LazyLock::new(|| parse_interval("interval 30 days").unwrap());
        self.0
            .get(TableProperty::LogRetentionDuration.as_ref())
            .and_then(|v| parse_interval(v).ok())
            .unwrap_or_else(|| DEFAULT_DURATION.to_owned())
    }
}

const SECONDS_PER_MINUTE: u64 = 60;
const SECONDS_PER_HOUR: u64 = 60 * SECONDS_PER_MINUTE;
const SECONDS_PER_DAY: u64 = 24 * SECONDS_PER_HOUR;
const SECONDS_PER_WEEK: u64 = 7 * SECONDS_PER_DAY;

fn parse_interval(value: &str) -> Result<Duration, String> {
    let not_an_interval = || format!("'{value}' is not an interval");

    if !value.starts_with("interval ") {
        return Err(not_an_interval());
    }
    let mut it = value.split_whitespace();
    let _ = it.next(); // skip "interval"
    let number = it
        .next()
        .ok_or_else(not_an_interval)?
        .parse::<u64>()
        .map_err(|_| not_an_interval())?;

    let duration = match it.next().ok_or_else(not_an_interval)? {
        "nanosecond" | "nanoseconds" => Duration::from_nanos(number),
        "microsecond" | "microseconds" => Duration::from_micros(number),
        "millisecond" | "milliseconds" => Duration::from_millis(number),
        "second" | "seconds" => Duration::from_secs(number),
        "minute" | "minutes" => Duration::from_secs(number * SECONDS_PER_MINUTE),
        "hour" | "hours" => Duration::from_secs(number * SECONDS_PER_HOUR),
        "day" | "days" => Duration::from_secs(number * SECONDS_PER_DAY),
        "week" | "weeks" => Duration::from_secs(number * SECONDS_PER_WEEK),
        unit => {
            return Err(format!("Unknown unit '{unit}'"));
        }
    };

    Ok(duration)
}
