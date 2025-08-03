//! Utilities for converting Arrow arrays into Delta data structures.
use datafusion::arrow::array::{
    Array, BooleanArray, Int64Array, MapArray, StringArray, StructArray,
};
use deltalake::kernel::Remove;
use deltalake::{DeltaResult, DeltaTableError};
use percent_encoding::percent_decode_str;

use crate::kernel::snapshot::log_data::ex::extract_and_cast_opt;
use crate::kernel::snapshot::log_data::ProvidesColumnByName;

pub(super) fn read_removes(array: &dyn ProvidesColumnByName) -> DeltaResult<Vec<Remove>> {
    let mut result = Vec::new();

    if let Some(arr) = extract_and_cast_opt::<StructArray>(array, "remove") {
        // Stop early if all values are null
        if arr.null_count() == arr.len() {
            return Ok(result);
        }

        let path_arr = extract_and_cast_opt::<StringArray>(arr, "path")
            .ok_or_else(|| DeltaTableError::Generic("remove.path is required".to_string()))?;

        let deletion_timestamp_arr = extract_and_cast_opt::<Int64Array>(arr, "deletionTimestamp");
        let data_change_arr = extract_and_cast_opt::<BooleanArray>(arr, "dataChange")
            .ok_or_else(|| DeltaTableError::Generic("remove.dataChange is required".to_string()))?;
        let extended_file_metadata_arr =
            extract_and_cast_opt::<BooleanArray>(arr, "extendedFileMetadata");
        let partition_values_arr = extract_and_cast_opt::<MapArray>(arr, "partitionValues");
        let size_arr = extract_and_cast_opt::<Int64Array>(arr, "size");
        let tags_arr = extract_and_cast_opt::<MapArray>(arr, "tags");
        let deletion_vector_arr = extract_and_cast_opt::<StructArray>(arr, "deletionVector");
        let base_row_id_arr = extract_and_cast_opt::<Int64Array>(arr, "baseRowId");
        let default_row_commit_version_arr =
            extract_and_cast_opt::<Int64Array>(arr, "defaultRowCommitVersion");

        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }

            let path = path_arr.value(i);
            let path = percent_decode_str(path)
                .decode_utf8()
                .map_err(|e| DeltaTableError::Generic(format!("Failed to decode path: {}", e)))?
                .to_string();

            let deletion_timestamp = deletion_timestamp_arr.and_then(|arr| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i))
                }
            });

            let data_change = data_change_arr.value(i);

            let extended_file_metadata = extended_file_metadata_arr.and_then(|arr| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i))
                }
            });

            let partition_values = if let Some(arr) = partition_values_arr {
                if arr.is_null(i) {
                    None
                } else {
                    // For simplicity, we'll create an empty map for now
                    // In a full implementation, you'd parse the map array
                    Some(std::collections::HashMap::new())
                }
            } else {
                None
            };

            let size = size_arr.and_then(|arr| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i))
                }
            });

            let tags = if let Some(_arr) = tags_arr {
                // For simplicity, we'll create an empty map for now
                // In a full implementation, you'd parse the map array
                Some(std::collections::HashMap::new())
            } else {
                None
            };

            let deletion_vector = if let Some(_arr) = deletion_vector_arr {
                // For simplicity, we'll skip deletion vector parsing for now
                // In a full implementation, you'd parse the struct array
                None
            } else {
                None
            };

            let base_row_id = base_row_id_arr.and_then(|arr| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i))
                }
            });

            let default_row_commit_version = default_row_commit_version_arr.and_then(|arr| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i))
                }
            });

            result.push(Remove {
                path,
                deletion_timestamp,
                data_change,
                extended_file_metadata,
                partition_values,
                size,
                tags,
                deletion_vector,
                base_row_id,
                default_row_commit_version,
            });
        }
    }

    Ok(result)
}
