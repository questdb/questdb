use std::cmp;

use crate::parquet::error::ParquetResult;
use crate::parquet_write::file::WriteOptions;
use parquet2::schema::types::PhysicalType;
use qdb_core::col_type::{ColumnType, ColumnTypeTag};

/// Default cap on uncompressed data page size.
pub const DEFAULT_PAGE_SIZE: usize = 1024 * 1024;

/// Compute the number of rows per data page for a given primitive size.
#[inline]
pub fn rows_per_page(options: &WriteOptions, bytes_per_row: usize) -> usize {
    let max_page_size = options.data_page_size.unwrap_or(DEFAULT_PAGE_SIZE);
    cmp::max(max_page_size / bytes_per_row, 1)
}

#[inline]
fn bytes_per_primitive_type(physical_type: PhysicalType) -> usize {
    match physical_type {
        PhysicalType::Boolean => 1,
        PhysicalType::Int32 | PhysicalType::Float => 4,
        PhysicalType::Int96 => 12,
        _ => 8,
    }
}

#[inline]
fn bytes_per_group_type(column_type: ColumnType) -> ParquetResult<usize> {
    if column_type.tag() == ColumnTypeTag::Array {
        Ok((column_type.array_dimensionality()? as usize) * 8)
    } else {
        Ok(8)
    }
}

#[inline]
pub fn rows_per_primitive_page(options: &WriteOptions, physical_type: PhysicalType) -> usize {
    rows_per_page(options, bytes_per_primitive_type(physical_type))
}

#[inline]
pub fn rows_per_group_page(
    options: &WriteOptions,
    column_type: ColumnType,
) -> ParquetResult<usize> {
    Ok(rows_per_page(options, bytes_per_group_type(column_type)?))
}
