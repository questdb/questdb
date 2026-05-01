use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use parquet2::page::Page;

use crate::parquet::error::ParquetResult;
use crate::parquet_write::encoders::helpers::{
    column_chunk_row_count, lock_bloom_set, page_row_windows, PageRowWindow,
};
use crate::parquet_write::schema::Column;

mod fixed;
mod primitive;
#[cfg(test)]
mod tests;
mod varlen;

pub use fixed::{bytes_to_page, encode_fixed_len_bytes};
pub use primitive::{
    boolean_to_page, encode_boolean, encode_decimal, encode_int_notnull, encode_int_nullable,
    encode_simd,
};
pub use varlen::{
    binary_to_page, encode_binary, encode_string, encode_varchar, string_to_page, varchar_to_page,
};

/// Internal helper: split the selected column chunk into one or more data pages
/// using the configured `rows_per_page` heuristic. Locks the bloom set once for
/// the whole chunk.
fn encode_column_chunk<F>(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    rows_per_page: usize,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
    mut emit: F,
) -> ParquetResult<Vec<Page>>
where
    F: FnMut(PageRowWindow, Option<&mut HashSet<u64>>) -> ParquetResult<Page>,
{
    let total_rows = column_chunk_row_count(columns, first_partition_start, last_partition_end);
    if total_rows == 0 {
        return Ok(vec![]);
    }

    let mut bloom_guard = lock_bloom_set(bloom_set.as_ref())?;
    let mut bloom = bloom_guard.as_deref_mut();
    let mut pages = Vec::with_capacity(total_rows.div_ceil(rows_per_page));
    for window in page_row_windows(total_rows, rows_per_page) {
        pages.push(emit(window, bloom.as_deref_mut())?);
    }
    Ok(pages)
}
