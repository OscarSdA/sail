//! Delta table snapshots
//!
//! A snapshot represents the state of a Delta Table at a given version.
//!
//! There are two types of snapshots:
//!
//! - [`Snapshot`] is a snapshot where most data is loaded on demand and only the
//!   bare minimum - [`Protocol`] and [`Metadata`] - is cached in memory.
//! - [`EagerSnapshot`] is a snapshot where much more log data is eagerly loaded into memory.
//!
//! The submodules provide structures and methods that aid in generating
//! and consuming snapshots.
//!
//! ## Reading the log
//!
//!

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Cursor};
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
// use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::path::{LogPathFileType, ParsedLogPath};
use delta_kernel::scan::scan_row_schema;
use delta_kernel::schema::SchemaRef;
use delta_kernel::snapshot::Snapshot as KernelSnapshot;
use delta_kernel::{PredicateRef, Version};
use deltalake::kernel::{Action, CommitInfo, Metadata, Protocol, Remove};
// #[cfg(test)]
// use deltalake::kernel::transaction::CommitData;
use deltalake::kernel::{ActionType, Add, StructType};
use deltalake::logstore::LogStore;
// use deltalake::logstore::LogStoreExt;  // This is private, we'll need to implement it
// use deltalake::table::config::TableConfig;
use deltalake::{DeltaResult, DeltaTableConfig, DeltaTableError};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use object_store::path::Path;
use object_store::ObjectStore;
use tokio::task::spawn_blocking;
use url::Url;

// use crate::kernel::snapshot::log_data::ex::{ScanExt, SnapshotExt};
use crate::kernel::models::fields::ActionTypeExt;
use crate::kernel::snapshot::log_data::SailLogDataHandler;
use crate::kernel::snapshot::parse::read_removes;

// Type aliases for compatibility
pub type SendableRBStream = BoxStream<'static, DeltaResult<RecordBatch>>;
pub type LogDataHandler<'a> = crate::kernel::snapshot::log_data::SailLogDataHandler;

// pub use stream::*;

pub mod log_data;
pub(crate) mod parse;
// pub(crate) mod replay;
// mod serde;
mod stream;

use stream::RecordBatchReceiverStreamBuilder;

pub(crate) static SCAN_ROW_ARROW_SCHEMA: LazyLock<arrow_schema::SchemaRef> =
    LazyLock::new(|| Arc::new(scan_row_schema().as_ref().try_into_arrow().unwrap()));

/// A snapshot of a Delta table
#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    /// Log segment containing all log files in the snapshot
    pub(crate) inner: Arc<KernelSnapshot>,
    /// Configuration for the current session
    config: DeltaTableConfig,
    /// Logical table schema
    schema: SchemaRef,
    /// Fully qualified URL of the table
    table_url: Url,
}

impl Snapshot {
    /// Create a new [`Snapshot`] instance
    pub async fn try_new(
        log_store: &dyn LogStore,
        config: DeltaTableConfig,
        version: Option<i64>,
    ) -> DeltaResult<Self> {
        // TODO: bundle operation_id with logstore ...
        let engine = log_store.engine(None);
        let mut table_root_url = log_store.config().location.clone();
        if !table_root_url.path().ends_with('/') {
            table_root_url.set_path(&format!("{}/", table_root_url.path()));
        }
        let version = version.map(|v| v as u64);

        let snapshot = match spawn_blocking(move || {
            KernelSnapshot::try_new(table_root_url, engine.as_ref(), version)
        })
        .await
        .map_err(|e| DeltaTableError::Generic(e.to_string()))?
        {
            Ok(snapshot) => snapshot,
            Err(e) => {
                // TODO: we should have more handling-friendly errors upstream in kernel.
                if e.to_string().contains("No files in log segment") {
                    return Err(DeltaTableError::NotATable(e.to_string()));
                } else {
                    return Err(e.into());
                }
            }
        };

        let schema = snapshot.table_configuration().schema();

        Ok(Self {
            inner: Arc::new(snapshot),
            config,
            schema,
            table_url: log_store.config().location.clone(),
        })
    }

    // Test method removed for simplification

    /// Update the snapshot to the given version
    pub async fn update(
        &mut self,
        log_store: &dyn LogStore,
        target_version: Option<u64>,
    ) -> DeltaResult<()> {
        if let Some(version) = target_version {
            if version == self.version() as u64 {
                return Ok(());
            }
            if version < self.version() as u64 {
                return Err(DeltaTableError::Generic("Cannot downgrade snapshot".into()));
            }
        }

        // TODO: bundle operation id with log store ...
        let engine = log_store.engine(None);
        let current = self.inner.clone();
        let snapshot = spawn_blocking(move || {
            KernelSnapshot::try_new_from(current, engine.as_ref(), target_version)
        })
        .await
        .map_err(|e| DeltaTableError::Generic(e.to_string()))??;

        self.inner = snapshot;
        self.schema = self.inner.table_configuration().schema();

        Ok(())
    }

    /// Get the table version of the snapshot
    pub fn version(&self) -> i64 {
        self.inner.version() as i64
    }

    /// Get the table schema of the snapshot
    pub fn schema(&self) -> &StructType {
        self.schema.as_ref()
    }

    /// Get the table metadata of the snapshot
    pub fn metadata(&self) -> &Metadata {
        self.inner.metadata()
    }

    /// Get the table protocol of the snapshot
    pub fn protocol(&self) -> &Protocol {
        self.inner.protocol()
    }

    /// Get the table config which is loaded with of the snapshot
    pub fn load_config(&self) -> &DeltaTableConfig {
        &self.config
    }

    /// Get the table root of the snapshot
    pub(crate) fn table_root_path(&self) -> DeltaResult<Path> {
        Ok(Path::from_url_path(self.table_url.path())?)
    }

    /// Well known table configuration
    pub fn table_config(&self) -> &std::collections::HashMap<String, String> {
        self.inner.metadata().configuration()
    }

    /// Get the files in the snapshot
    pub fn files(
        &self,
        log_store: &dyn LogStore,
        predicate: Option<PredicateRef>,
    ) -> SendableRBStream {
        let scan = match self
            .inner
            .clone()
            .scan_builder()
            .with_predicate(predicate)
            .build()
        {
            Ok(scan) => scan,
            Err(err) => {
                return Box::pin(futures::stream::once(async {
                    Err(DeltaTableError::KernelError(err))
                }))
            }
        };

        // TODO: which capacity to choose?
        let mut builder = RecordBatchReceiverStreamBuilder::new(100);
        let tx = builder.tx();
        // TODO: bundle operation id with log store ...
        let engine = log_store.engine(None);
        let _inner = self.inner.clone();

        builder.spawn_blocking(move || {
            let scan_iter = scan.scan_metadata(engine.as_ref())?;
            for res in scan_iter {
                let scan_metadata = res?;
                let batch: RecordBatch =
                    ArrowEngineData::try_from_engine_data(scan_metadata.scan_files.data)?.into();
                let batch = batch; // For now, skip stats parsing
                if tx.blocking_send(Ok(batch)).is_err() {
                    break;
                }
            }
            Ok(())
        });

        builder.build()
    }

    /// Get the commit infos in the snapshot
    pub(crate) async fn commit_infos(
        &self,
        log_store: &dyn LogStore,
        limit: Option<usize>,
    ) -> DeltaResult<BoxStream<'_, DeltaResult<Option<CommitInfo>>>> {
        let store = log_store.root_object_store(None);

        let log_root = self.table_root_path()?.child("_delta_log");
        let start_from = log_root.child(
            format!(
                "{:020}",
                limit
                    .map(|l| (self.version() - l as i64 + 1).max(0))
                    .unwrap_or(0)
            )
            .as_str(),
        );

        let dummy_url = url::Url::parse("memory:///").unwrap();
        let mut commit_files = Vec::new();
        for meta in store
            .list_with_offset(Some(&log_root), &start_from)
            .try_collect::<Vec<_>>()
            .await?
        {
            // safety: object store path are always valid urls paths.
            let dummy_path = dummy_url.join(meta.location.as_ref()).unwrap();
            if let Some(parsed_path) = ParsedLogPath::try_from(dummy_path)? {
                if matches!(parsed_path.file_type, LogPathFileType::Commit) {
                    commit_files.push(meta);
                }
            }
        }
        commit_files.sort_unstable_by(|a, b| b.location.cmp(&a.location));
        Ok(futures::stream::iter(commit_files)
            .map(move |meta| {
                let store = store.clone();
                async move {
                    let commit_log_bytes = store.get(&meta.location).await?.bytes().await?;
                    let reader = BufReader::new(Cursor::new(commit_log_bytes));
                    for line in reader.lines() {
                        let action: Action = serde_json::from_str(line?.as_str())?;
                        if let Action::CommitInfo(commit_info) = action {
                            return Ok::<_, DeltaTableError>(Some(commit_info));
                        }
                    }
                    Ok(None)
                }
            })
            .buffered(self.config.log_buffer_size)
            .boxed())
    }

    pub(crate) fn tombstones(
        &self,
        log_store: &dyn LogStore,
    ) -> BoxStream<'_, DeltaResult<Remove>> {
        static TOMBSTONE_SCHEMA: LazyLock<Arc<StructType>> = LazyLock::new(|| {
            Arc::new(StructType::new(vec![
                ActionType::Remove.schema_field().clone(),
                ActionType::Sidecar.schema_field().clone(),
            ]))
        });

        // TODO: which capacity to choose?
        let mut builder = RecordBatchReceiverStreamBuilder::new(100);
        let tx = builder.tx();

        // TODO: bundle operation id with log store ...
        let engine = log_store.engine(None);

        let remove_data = match self.inner.log_segment().read_actions(
            engine.as_ref(),
            TOMBSTONE_SCHEMA.clone(),
            TOMBSTONE_SCHEMA.clone(),
            None,
        ) {
            Ok(data) => data,
            Err(err) => {
                return Box::pin(futures::stream::once(async {
                    Err(DeltaTableError::KernelError(err))
                }))
            }
        };

        builder.spawn_blocking(move || {
            for res in remove_data {
                let batch: RecordBatch =
                    ArrowEngineData::try_from_engine_data(res?.actions)?.into();
                if tx.blocking_send(Ok(batch)).is_err() {
                    break;
                }
            }
            Ok(())
        });

        builder
            .build()
            .map(|maybe_batch| maybe_batch.and_then(|batch| read_removes(&batch)))
            .map_ok(|removes| {
                futures::stream::iter(removes.into_iter().map(Ok::<_, DeltaTableError>))
            })
            .try_flatten()
            .boxed()
    }

    async fn application_transaction_version(
        &self,
        log_store: &dyn LogStore,
        app_id: String,
    ) -> DeltaResult<Option<i64>> {
        // TODO: bundle operation id with log store ...
        let engine = log_store.engine(None);
        let inner = self.inner.clone();
        let version = spawn_blocking(move || inner.get_app_id_version(&app_id, engine.as_ref()))
            .await
            .map_err(|e| DeltaTableError::GenericError { source: e.into() })??;
        Ok(version)
    }
}

/// A snapshot of a Delta table that has been eagerly loaded into memory.
#[derive(Debug, Clone, PartialEq)]
pub struct EagerSnapshot {
    snapshot: Snapshot,
    // logical files in the snapshot
    pub(crate) files: RecordBatch,
}

impl EagerSnapshot {
    /// Create a new [`EagerSnapshot`] instance
    pub async fn try_new(
        log_store: &dyn LogStore,
        config: DeltaTableConfig,
        version: Option<i64>,
    ) -> DeltaResult<Self> {
        let snapshot = Snapshot::try_new(log_store, config.clone(), version).await?;

        let files = match config.require_files {
            true => snapshot.files(log_store, None).try_collect().await?,
            false => vec![],
        };

        let scan_row_schema = SCAN_ROW_ARROW_SCHEMA.clone();
        let files = concat_batches(&scan_row_schema, &files)?;

        Ok(Self { snapshot, files })
    }

    // #[cfg(test)]
    // pub async fn new_test<'a>(
    //     commits: impl IntoIterator<Item = &'a CommitData>,
    // ) -> DeltaResult<Self> {
    //     let (snapshot, log_store) = Snapshot::new_test(commits).await?;
    //     let files: Vec<_> = snapshot
    //         .files(log_store.as_ref(), None)
    //         .try_collect()
    //         .await?;
    //     let scan_row_schema = snapshot.inner.scan_row_parsed_schema_arrow()?;
    //     let files = concat_batches(&scan_row_schema, &files)?;

    //     Ok(Self { snapshot, files })
    // }

    /// Update the snapshot to the given version
    pub async fn update(
        &mut self,
        log_store: &dyn LogStore,
        target_version: Option<Version>,
    ) -> DeltaResult<()> {
        let current_version = self.version() as u64;
        if Some(current_version) == target_version {
            return Ok(());
        }

        self.snapshot.update(log_store, target_version).await?;

        let scan = self.snapshot.inner.clone().scan_builder().build()?;
        let engine = log_store.engine(None);
        let current_files = self.files.clone();
        let files: Vec<_> = spawn_blocking(move || {
            scan.scan_metadata_from(
                engine.as_ref(),
                current_version,
                Box::new(std::iter::once(
                    Box::new(ArrowEngineData::new(current_files))
                        as Box<dyn delta_kernel::EngineData>,
                )),
                None,
            )?
            .map_ok(|s| {
                ArrowEngineData::try_from_engine_data(s.scan_files.data)
                    .unwrap()
                    .into()
            })
            .try_collect()
        })
        .await
        .map_err(|e| DeltaTableError::Generic(e.to_string()))??;

        let files = concat_batches(&SCAN_ROW_ARROW_SCHEMA, &files)?;
        let files = files; // For now, skip stats parsing

        self.files = files;

        Ok(())
    }

    /// Get the underlying snapshot
    pub(crate) fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    /// Get the table version of the snapshot
    pub fn version(&self) -> i64 {
        self.snapshot.version()
    }

    /// Get the timestamp of the given version
    pub fn version_timestamp(&self, version: i64) -> Option<i64> {
        for path in &self.snapshot.inner.log_segment().ascending_commit_files {
            if path.version as i64 == version {
                return Some(path.location.last_modified);
            }
        }
        None
    }

    /// Get the table schema of the snapshot
    pub fn schema(&self) -> &StructType {
        self.snapshot.schema()
    }

    /// Get the table metadata of the snapshot
    pub fn metadata(&self) -> &Metadata {
        self.snapshot.metadata()
    }

    /// Get the table protocol of the snapshot
    pub fn protocol(&self) -> &Protocol {
        self.snapshot.protocol()
    }

    /// Get the table config which is loaded with of the snapshot
    pub fn load_config(&self) -> &DeltaTableConfig {
        self.snapshot.load_config()
    }

    /// Well known table configuration
    pub fn table_config(&self) -> &std::collections::HashMap<String, String> {
        self.snapshot.table_config()
    }

    /// Get a [`LogDataHandler`] for the snapshot to inspect the currently loaded state of the log.
    pub fn log_data(&self) -> SailLogDataHandler {
        SailLogDataHandler::from_data(
            vec![self.files.clone()],
            self.metadata().clone(),
            self.schema().clone(),
        )
    }

    /// Get the number of files in the snapshot
    pub fn files_count(&self) -> usize {
        self.files.num_rows()
    }

    /// Get the files in the snapshot
    pub fn file_actions(&self) -> DeltaResult<impl Iterator<Item = Add>> {
        let actions = self.file_actions_iter()?;
        Ok(actions.into_iter())
    }

    /// Get an iterator over file actions
    pub fn file_actions_iter(&self) -> DeltaResult<Vec<Add>> {
        self.parse_add_actions_from_batch()
    }

    /// Parse Add actions from the files RecordBatch
    fn parse_add_actions_from_batch(&self) -> DeltaResult<Vec<Add>> {
        use std::collections::HashMap;

        use datafusion::arrow::array::{Array, Int64Array, MapArray, StringArray};
        use datafusion::arrow::datatypes::DataType;

        let batch = &self.files;
        if batch.num_rows() == 0 {
            return Ok(vec![]);
        }

        // Extract required columns from the RecordBatch
        let path_array = batch
            .column_by_name("path")
            .ok_or_else(|| DeltaTableError::Generic("Missing 'path' column".to_string()))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DeltaTableError::Generic("Invalid 'path' column type".to_string()))?;

        let size_array = batch
            .column_by_name("size")
            .ok_or_else(|| DeltaTableError::Generic("Missing 'size' column".to_string()))?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DeltaTableError::Generic("Invalid 'size' column type".to_string()))?;

        let modification_time_array = batch
            .column_by_name("modificationTime")
            .ok_or_else(|| {
                DeltaTableError::Generic("Missing 'modificationTime' column".to_string())
            })?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DeltaTableError::Generic("Invalid 'modificationTime' column type".to_string())
            })?;

        // Try to get partition values - this might be in different locations depending on the schema
        let partition_values_array = batch
            .column_by_name("partitionValues")
            .or_else(|| batch.column_by_name("fileConstantValues.partitionValues"))
            .and_then(|col| match col.data_type() {
                DataType::Map(_, _) => col.as_any().downcast_ref::<MapArray>(),
                _ => None,
            });

        let mut actions = Vec::with_capacity(batch.num_rows());

        for i in 0..batch.num_rows() {
            let path = if path_array.is_null(i) {
                continue; // Skip null paths
            } else {
                path_array.value(i).to_string()
            };

            let size = if size_array.is_null(i) {
                0
            } else {
                size_array.value(i)
            };

            let modification_time = if modification_time_array.is_null(i) {
                0
            } else {
                modification_time_array.value(i)
            };

            // Parse partition values if available
            let partition_values = if let Some(pv_array) = partition_values_array {
                if pv_array.is_null(i) {
                    HashMap::new()
                } else {
                    self.parse_partition_values(pv_array, i)?
                }
            } else {
                HashMap::new()
            };

            actions.push(Add {
                path,
                partition_values,
                size,
                modification_time,
                data_change: true,
                stats: None, // TODO: Parse stats if needed
                tags: None,
                deletion_vector: None,
                base_row_id: None,
                default_row_commit_version: None,
                clustering_provider: None,
            });
        }

        Ok(actions)
    }

    /// Parse partition values from a MapArray at the given index
    fn parse_partition_values(
        &self,
        map_array: &datafusion::arrow::array::MapArray,
        index: usize,
    ) -> DeltaResult<HashMap<String, Option<String>>> {
        use datafusion::arrow::array::{Array, StringArray};

        let mut partition_values = HashMap::new();

        if map_array.is_null(index) {
            return Ok(partition_values);
        }

        let map_value = map_array.value(index);
        if map_value.num_columns() >= 2 {
            let keys = map_value
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DeltaTableError::Generic("Invalid partition keys type".to_string())
                })?;

            let values = map_value.column(1);

            for i in 0..keys.len() {
                let key = keys.value(i).to_string();
                let value = if values.is_null(i) {
                    None
                } else {
                    // Convert the value to string representation
                    Some(format!("{:?}", values.slice(i, 1)))
                };
                partition_values.insert(key, value);
            }
        }

        Ok(partition_values)
    }

    /// Iterate over all latest app transactions
    pub async fn transaction_version(
        &self,
        log_store: &dyn LogStore,
        app_id: impl AsRef<str>,
    ) -> DeltaResult<Option<i64>> {
        self.snapshot
            .application_transaction_version(log_store, app_id.as_ref().to_string())
            .await
    }
}

pub(crate) fn partitions_schema(
    schema: &StructType,
    partition_columns: &[String],
) -> DeltaResult<Option<StructType>> {
    if partition_columns.is_empty() {
        return Ok(None);
    }
    Ok(Some(StructType::new(
        partition_columns
            .iter()
            .map(|col| {
                schema.field(col).cloned().ok_or_else(|| {
                    DeltaTableError::Generic(format!("Partition column {col} not found in schema"))
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
    )))
}

// Tests removed for simplification
