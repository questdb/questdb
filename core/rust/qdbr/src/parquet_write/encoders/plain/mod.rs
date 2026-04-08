use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use parquet2::page::Page;

use crate::parquet::error::ParquetResult;
use crate::parquet_write::encoders::helpers::{
    lock_bloom_set, partition_slice_range, ChunkSlice, PartitionPageSlices,
};
use crate::parquet_write::schema::Column;

mod fixed;
mod primitive;
#[cfg(test)]
mod tests;
mod varlen;

pub use fixed::{bytes_to_dict_pages, bytes_to_page, encode_fixed_len_bytes};
pub use primitive::{
    boolean_to_page, encode_boolean, encode_decimal, encode_int_notnull, encode_int_nullable,
    encode_simd,
};
pub use varlen::{
    binary_to_dict_pages, binary_to_page, encode_binary, encode_string, encode_varchar,
    string_to_dict_pages, string_to_page, varchar_to_dict_pages, varchar_to_page,
};

/// Internal helper: iterate over each partition, splitting it into sub-pages
/// of `rows_per_page` rows each. Locks the bloom set once per emitted page.
fn encode_per_partition<F>(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    rows_per_page: usize,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
    mut emit: F,
) -> ParquetResult<Vec<Page>>
where
    F: FnMut(&Column, ChunkSlice, Option<&mut HashSet<u64>>) -> ParquetResult<Page>,
{
    let num_partitions = columns.len();
    let mut pages = Vec::with_capacity(num_partitions);
    for (part_idx, column) in columns.iter().enumerate() {
        let (chunk_offset, chunk_length) = partition_slice_range(
            part_idx,
            num_partitions,
            column.row_count,
            first_partition_start,
            last_partition_end,
        );
        for chunk in PartitionPageSlices::new(column, chunk_offset, chunk_length, rows_per_page) {
            let mut bloom_guard = lock_bloom_set(bloom_set.as_ref())?;
            let bloom = bloom_guard.as_deref_mut();
            let page = emit(column, chunk, bloom)?;
            pages.push(page);
        }
    }
    Ok(pages)
}
