use std::collections::HashSet;
use std::sync::Arc;

use arrow_schema::DataType as ArrowDataType;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, StringArray, StructArray, UInt64Array,
};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::scalar::ScalarValue;
use datafusion::common::stats::{ColumnStatistics, Precision, Statistics};
use datafusion::common::Column;
use datafusion::functions_aggregate::min_max::{MaxAccumulator, MinAccumulator};
use datafusion::physical_optimizer::pruning::PruningStatistics;
use datafusion::physical_plan::Accumulator;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::expressions::Expression;
use delta_kernel::schema::{DataType, PrimitiveType};
use delta_kernel::{EngineData, EvaluationHandler, ExpressionEvaluator};
use deltalake::kernel::{LogicalFile, Metadata, Snapshot, StructType};
use deltalake::logstore::LogStoreRef;
use deltalake::{DeltaResult, DeltaTableConfig, DeltaTableError};
use futures::TryStreamExt;

use crate::kernel::ARROW_HANDLER;

pub mod ex {
    use std::sync::Arc;

    use arrow_schema::{ArrowError, Schema as ArrowSchema};
    use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, StructArray};
    use datafusion::arrow::record_batch::RecordBatch;
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::scan::{Scan, ScanMetadata};
    use delta_kernel::schema::SchemaRef;
    use delta_kernel::snapshot::Snapshot;
    use delta_kernel::{Engine, EngineData, PredicateRef, Version};
    use deltalake::{DeltaResult, DeltaTableError};
    // use futures::TryStreamExt;

    // Credit: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/kernel/arrow/extract.rs>
    pub fn extract_and_cast_opt<'a, T: Array + 'static>(
        array: &'a dyn super::ProvidesColumnByName,
        name: &'a str,
    ) -> Option<&'a T> {
        let mut path_steps = name.split('.');
        let first = path_steps.next()?;
        extract_column(array, first, &mut path_steps)
            .ok()?
            .as_any()
            .downcast_ref::<T>()
    }

    pub fn extract_column<'a>(
        array: &'a dyn super::ProvidesColumnByName,
        path_step: &str,
        remaining_path_steps: &mut impl Iterator<Item = &'a str>,
    ) -> Result<&'a ArrayRef, ArrowError> {
        let child = array
            .column_by_name(path_step)
            .ok_or(ArrowError::SchemaError(format!(
                "No such field: {path_step}",
            )))?;

        if let Some(next_path_step) = remaining_path_steps.next() {
            extract_column(
                child
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| {
                        ArrowError::SchemaError(format!("'{path_step}' is not a struct"))
                    })?,
                next_path_step,
                remaining_path_steps,
            )
        } else {
            Ok(child)
        }
    }

    /// [`ScanMetadata`] contains (1) a [`RecordBatch`] specifying data files to be scanned
    /// and (2) a vector of transforms (one transform per scan file) that must be applied to the data read
    /// from those files.
    pub struct ScanMetadataArrow {
        /// Record batch with one row per file to scan
        pub scan_files: RecordBatch,
        /// Vector of transforms to apply to each file
        pub scan_file_transforms: Vec<Option<String>>,
    }

    pub trait ScanExt {
        /// Get the metadata for a table scan.
        ///
        /// This method handles translation between `EngineData` and `RecordBatch`
        /// and will already apply any selection vectors to the data.
        /// See [`Scan::scan_metadata`] for details.
        fn scan_metadata_arrow(
            &self,
            engine: &dyn Engine,
        ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanMetadataArrow>>>;

        fn scan_metadata_from_arrow(
            &self,
            engine: &dyn Engine,
            existing_version: Version,
            existing_data: Box<dyn Iterator<Item = RecordBatch>>,
            existing_predicate: Option<PredicateRef>,
        ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanMetadataArrow>>>;
    }

    impl ScanExt for Scan {
        fn scan_metadata_arrow(
            &self,
            engine: &dyn Engine,
        ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanMetadataArrow>>> {
            Ok(self.scan_metadata(engine)?.map(|result| {
                result
                    .and_then(kernel_to_arrow)
                    .map_err(|e| DeltaTableError::Generic(e.to_string()))
            }))
        }

        fn scan_metadata_from_arrow(
            &self,
            engine: &dyn Engine,
            existing_version: Version,
            existing_data: Box<dyn Iterator<Item = RecordBatch>>,
            existing_predicate: Option<PredicateRef>,
        ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanMetadataArrow>>> {
            let engine_iter = existing_data
                .map(|batch| Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>);
            Ok(self
                .scan_metadata_from(engine, existing_version, engine_iter, existing_predicate)?
                .map(|result| {
                    result
                        .and_then(kernel_to_arrow)
                        .map_err(|e| DeltaTableError::Generic(e.to_string()))
                }))
        }
    }

    pub trait SnapshotExt {
        /// Returns the expected file statistics schema for the snapshot.
        fn stats_schema(&self) -> DeltaResult<Option<SchemaRef>>;

        fn partitions_schema(&self) -> DeltaResult<Option<SchemaRef>>;

        fn scan_row_parsed_schema_arrow(&self) -> DeltaResult<Arc<ArrowSchema>>;

        /// Parse stats column into a struct array.
        fn parse_stats_column(&self, batch: &RecordBatch) -> DeltaResult<RecordBatch>;
    }

    impl SnapshotExt for Snapshot {
        fn stats_schema(&self) -> DeltaResult<Option<SchemaRef>> {
            // TODO: Implement stats schema
            Ok(None)
        }

        fn partitions_schema(&self) -> DeltaResult<Option<SchemaRef>> {
            // TODO: Implement partitions schema
            Ok(None)
        }

        fn scan_row_parsed_schema_arrow(&self) -> DeltaResult<Arc<ArrowSchema>> {
            // Use the static schema from the parent module
            Ok(crate::kernel::snapshot::SCAN_ROW_ARROW_SCHEMA.clone())
        }

        fn parse_stats_column(&self, batch: &RecordBatch) -> DeltaResult<RecordBatch> {
            // TODO: Implement stats parsing
            Ok(batch.clone())
        }
    }

    fn kernel_to_arrow(metadata: ScanMetadata) -> Result<ScanMetadataArrow, delta_kernel::Error> {
        let scan_file_transforms = metadata
            .scan_file_transforms
            .into_iter()
            .enumerate()
            .filter_map(|(i, v)| {
                metadata.scan_files.selection_vector[i]
                    .then_some(v.map(|expr| format!("{:?}", expr)))
            })
            .collect();
        let batch = ArrowEngineData::try_from_engine_data(metadata.scan_files.data)?.into();
        let scan_files = filter_record_batch(
            &batch,
            &BooleanArray::from(metadata.scan_files.selection_vector),
        )
        .map_err(|e| delta_kernel::Error::Generic(e.to_string()))?;
        Ok(ScanMetadataArrow {
            scan_files,
            scan_file_transforms,
        })
    }

    fn filter_record_batch(
        batch: &RecordBatch,
        filter: &BooleanArray,
    ) -> Result<RecordBatch, delta_kernel::Error> {
        use datafusion::arrow::compute::filter_record_batch;
        filter_record_batch(batch, filter).map_err(|e| delta_kernel::Error::Generic(e.to_string()))
    }
}

/// A trait for providing columns by name, implemented by RecordBatch and StructArray.
/// This allows helper functions to work on both.
pub trait ProvidesColumnByName {
    fn column_by_name(&self, name: &str) -> Option<&ArrayRef>;
}

impl ProvidesColumnByName for RecordBatch {
    fn column_by_name(&self, name: &str) -> Option<&ArrayRef> {
        self.column_by_name(name)
    }
}

impl ProvidesColumnByName for StructArray {
    fn column_by_name(&self, name: &str) -> Option<&ArrayRef> {
        self.column_by_name(name)
    }
}

/// Helper function to convert Box<dyn EngineData> back to RecordBatch
fn engine_data_to_record_batch(engine_data: Box<dyn EngineData>) -> DeltaResult<RecordBatch> {
    let arrow_data = ArrowEngineData::try_from_engine_data(engine_data)
        .map_err(|e| DeltaTableError::Generic(e.to_string()))?;
    Ok(arrow_data.into())
}

/// Extension trait to simplify evaluating expressions with RecordBatch
pub(crate) trait ExpressionEvaluatorExt {
    fn evaluate_arrow(&self, batch: RecordBatch) -> DeltaResult<RecordBatch>;
}

impl<T: ExpressionEvaluator + ?Sized> ExpressionEvaluatorExt for T {
    fn evaluate_arrow(&self, batch: RecordBatch) -> DeltaResult<RecordBatch> {
        let engine_data = ArrowEngineData::new(batch);
        let result_data = self
            .evaluate(&engine_data)
            .map_err(|e| DeltaTableError::Generic(e.to_string()))?;
        engine_data_to_record_batch(result_data)
    }
}

/// A simplified LogDataHandler that provides basic functionality
#[derive(Debug)]
pub struct SailLogDataHandler {
    pub data: Vec<RecordBatch>,
    pub metadata: Metadata,
    pub schema: StructType,
}

impl SailLogDataHandler {
    pub fn from_data(data: Vec<RecordBatch>, metadata: Metadata, schema: StructType) -> Self {
        Self {
            data,
            metadata,
            schema,
        }
    }
}

impl<'a> IntoIterator for &'a SailLogDataHandler {
    type Item = LogicalFile<'a>;
    type IntoIter = Box<dyn Iterator<Item = Self::Item> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        // Create an iterator that processes all batches
        Box::new(SailFileStatsIterator::new(self))
    }
}

/// Iterator that processes RecordBatch data to yield LogicalFile instances
pub struct SailFileStatsIterator<'a> {
    handler: &'a SailLogDataHandler,
    batch_index: usize,
    row_index: usize,
}

impl<'a> SailFileStatsIterator<'a> {
    fn new(handler: &'a SailLogDataHandler) -> Self {
        Self {
            handler,
            batch_index: 0,
            row_index: 0,
        }
    }
}

impl<'a> Iterator for SailFileStatsIterator<'a> {
    type Item = LogicalFile<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO: Implement proper LogicalFile iteration from RecordBatch data
        // For now, this is a placeholder since the main functionality is implemented
        // in EagerSnapshot::file_actions_iter() which returns Add actions directly
        None
    }
}

// Credit: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/kernel/snapshot/log_data.rs>
impl SailLogDataHandler {
    pub async fn new(
        log_store: LogStoreRef,
        config: DeltaTableConfig,
        version: Option<i64>,
    ) -> DeltaResult<Self> {
        let snapshot = Snapshot::try_new(log_store.as_ref(), config.clone(), version).await?;

        let files: Vec<RecordBatch> = snapshot
            .files(log_store.as_ref(), None)
            .try_collect()
            .await?;

        let metadata = snapshot.metadata().clone();
        let schema = snapshot.schema().clone();

        Ok(Self {
            data: files,
            metadata,
            schema,
        })
    }

    /// Extract statistics from the pre-parsed log data following delta-rs pattern
    fn pick_stats(&self, column: &Column, stats_field: &'static str) -> Option<ArrayRef> {
        let field = self.schema.field(&column.name)?;

        // Binary type does not support natural order which is required for DataFusion to prune
        if field.data_type() == &DataType::Primitive(PrimitiveType::Binary) {
            return None;
        }

        let expression = if self.metadata.partition_columns().contains(&column.name) {
            Expression::column(["add", "partitionValues_parsed", &column.name])
        } else {
            Expression::column(["add", "stats_parsed", stats_field, &column.name])
        };

        let evaluator = ARROW_HANDLER.new_expression_evaluator(
            crate::kernel::models::fields::log_schema_ref().clone(),
            expression,
            field.data_type().clone(),
        );

        let mut results = Vec::new();

        for batch in self.data.iter() {
            let result = evaluator.evaluate_arrow(batch.clone()).ok()?;
            results.push(result);
        }

        if results.is_empty() {
            return None;
        }

        let schema = results[0].schema();
        let batch = concat_batches(&schema, &results).ok()?;
        batch.column_by_name("output").cloned()
    }

    /// Get the number of containers (files) being pruned
    pub fn num_containers(&self) -> usize {
        self.data.iter().map(|batch| batch.num_rows()).sum()
    }

    /// Calculate statistics for the data in the handler, optionally applying a pruning mask.
    pub fn statistics(&self, mask: Option<Vec<bool>>) -> Option<Statistics> {
        let num_rows = self.num_records(&mask);
        let total_byte_size = self.total_byte_size(&mask);
        let column_statistics = self
            .schema
            .fields()
            .map(|f| self.column_stats(f.name(), &mask))
            .collect::<Option<Vec<_>>>()?;
        Some(Statistics {
            num_rows,
            total_byte_size,
            column_statistics,
        })
    }

    /// Helper to get aggregated row counts.
    fn num_records(&self, mask: &Option<Vec<bool>>) -> Precision<usize> {
        self.collect_batch_stats("add.stats_parsed.numRecords", mask)
    }

    /// Helper to get aggregated byte size.
    fn total_byte_size(&self, mask: &Option<Vec<bool>>) -> Precision<usize> {
        self.collect_batch_stats("add.size", mask)
    }

    /// Helper to collect and sum statistics from RecordBatches.
    fn collect_batch_stats(&self, path: &str, mask: &Option<Vec<bool>>) -> Precision<usize> {
        let mut total = 0;
        let mut known = false;
        let mut mask_offset = 0;

        for batch in self.data.iter() {
            let num_rows = batch.num_rows();
            let arr = ex::extract_and_cast_opt::<Int64Array>(batch, path);

            if let Some(arr) = arr {
                for i in 0..num_rows {
                    if mask.as_ref().map(|m| m[mask_offset + i]).unwrap_or(true) && arr.is_valid(i)
                    {
                        total += arr.value(i) as usize;
                        known = true;
                    }
                }
            }
            mask_offset += num_rows;
        }

        if known {
            Precision::Exact(total)
        } else {
            Precision::Absent
        }
    }

    /// Collects and aggregates column-level statistics.
    fn column_stats(&self, name: &str, mask: &Option<Vec<bool>>) -> Option<ColumnStatistics> {
        let null_count_col = format!("add.stats_parsed.nullCount.{name}");
        let null_count = self.collect_batch_stats(&null_count_col, mask);

        let min_value = self.column_bounds("add.stats_parsed.minValues", name, mask, true);
        let max_value = self.column_bounds("add.stats_parsed.maxValues", name, mask, false);

        Some(ColumnStatistics {
            null_count,
            max_value,
            min_value,
            sum_value: Precision::Absent,
            distinct_count: Precision::Absent,
        })
    }

    /// Helper to get min/max bounds for a column from all batches.
    fn column_bounds(
        &self,
        path_step: &str,
        name: &str,
        mask: &Option<Vec<bool>>,
        is_min: bool,
    ) -> Precision<ScalarValue> {
        let mut accumulator: Option<Box<dyn Accumulator>> = None;
        let mut mask_offset = 0;

        for batch in &self.data {
            let num_rows = batch.num_rows();
            let mut path_iter = path_step.split('.');
            let array = if let Ok(array) = ex::extract_column(
                batch,
                #[allow(clippy::unwrap_used)]
                path_iter.next().unwrap(),
                &mut path_iter,
            ) {
                let mut name_iter = name.split('.');
                if let Ok(array) = ex::extract_column(
                    #[allow(clippy::unwrap_used)]
                    array.as_any().downcast_ref::<StructArray>().unwrap(),
                    #[allow(clippy::unwrap_used)]
                    name_iter.next().unwrap(),
                    &mut name_iter,
                ) {
                    array
                } else {
                    mask_offset += num_rows;
                    continue;
                }
            } else {
                mask_offset += num_rows;
                continue;
            };

            if accumulator.is_none() {
                if is_min {
                    accumulator = MinAccumulator::try_new(array.data_type())
                        .ok()
                        .map(|a| Box::new(a) as Box<dyn Accumulator>);
                } else {
                    accumulator = MaxAccumulator::try_new(array.data_type())
                        .ok()
                        .map(|a| Box::new(a) as Box<dyn Accumulator>);
                }
                if accumulator.is_none() {
                    return Precision::Absent;
                }
            }

            let array_to_update = if let Some(mask) = mask {
                if mask.len() > mask_offset {
                    let batch_mask = BooleanArray::from_iter(
                        mask[mask_offset..mask_offset + num_rows]
                            .iter()
                            .map(|&b| Some(b)),
                    );
                    datafusion::arrow::compute::filter(array, &batch_mask).ok()
                } else {
                    None // Mask is shorter than expected, treat as no data
                }
            } else {
                Some(array.clone())
            };

            if let Some(arr) = array_to_update {
                if let Some(acc) = accumulator.as_mut() {
                    let _ = acc.update_batch(&[arr]);
                }
            }

            mask_offset += num_rows;
        }

        accumulator
            .and_then(|mut acc| acc.evaluate().ok())
            .map(Precision::Exact)
            .unwrap_or(Precision::Absent)
    }
}

impl PruningStatistics for SailLogDataHandler {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        self.pick_stats(column, "minValues")
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        self.pick_stats(column, "maxValues")
    }

    fn num_containers(&self) -> usize {
        self.num_containers()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        if !self.metadata.partition_columns().contains(&column.name) {
            let counts = self.pick_stats(column, "nullCount")?;
            return datafusion::arrow::compute::cast(counts.as_ref(), &ArrowDataType::UInt64).ok();
        }

        // For partition columns, calculate null counts based on partition values
        let partition_values_array = self.pick_stats(column, "__dummy__")?;
        let row_counts = self.row_counts(column)?;
        let row_counts = row_counts.as_any().downcast_ref::<UInt64Array>()?;
        let mut null_counts = Vec::with_capacity(partition_values_array.len());

        for i in 0..partition_values_array.len() {
            let null_count = if partition_values_array.is_null(i) {
                row_counts.value(i)
            } else {
                0
            };
            null_counts.push(null_count);
        }

        Some(Arc::new(UInt64Array::from(null_counts)))
    }

    fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
        use std::sync::LazyLock;

        static ROW_COUNTS_EVAL: LazyLock<Arc<dyn ExpressionEvaluator>> = LazyLock::new(|| {
            ARROW_HANDLER.new_expression_evaluator(
                crate::kernel::models::fields::log_schema_ref().clone(),
                Expression::column(["add", "stats_parsed", "numRecords"]),
                DataType::Primitive(PrimitiveType::Long),
            )
        });

        let mut results = Vec::new();

        for batch in self.data.iter() {
            let result = ROW_COUNTS_EVAL.evaluate_arrow(batch.clone()).ok()?;
            results.push(result);
        }

        if results.is_empty() {
            return None;
        }

        let schema = results[0].schema();
        let batch = concat_batches(&schema, &results).ok()?;
        datafusion::arrow::compute::cast(batch.column_by_name("output")?, &ArrowDataType::UInt64)
            .ok()
    }

    // This function is optional but will optimize partition column pruning
    fn contained(&self, column: &Column, values: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        if values.is_empty() || !self.metadata.partition_columns().contains(&column.name) {
            return None;
        }

        // Retrieve the partition values for the column
        let partition_values_array = self.pick_stats(column, "__dummy__")?;

        let partition_values_str = partition_values_array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(DeltaTableError::generic(
                "failed to downcast string result to StringArray.",
            ))
            .ok()?;

        let mut contains = Vec::with_capacity(partition_values_str.len());

        // Helper function to check if a partition value matches any of the target values
        fn check_scalar(pv: &str, value: &ScalarValue) -> bool {
            match value {
                ScalarValue::Utf8(Some(v))
                | ScalarValue::Utf8View(Some(v))
                | ScalarValue::LargeUtf8(Some(v)) => pv == v,
                ScalarValue::Dictionary(_, inner) => check_scalar(pv, inner),
                _ => value.to_string() == pv,
            }
        }

        for i in 0..partition_values_str.len() {
            if partition_values_str.is_null(i) {
                contains.push(false);
            } else {
                contains.push(
                    values
                        .iter()
                        .any(|scalar| check_scalar(partition_values_str.value(i), scalar)),
                );
            }
        }

        Some(BooleanArray::from(contains))
    }
}
