use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use parquet2::page::Page;

use crate::parquet::error::ParquetResult;
use crate::parquet_write::encoders::helpers::{column_chunk_row_count, lock_bloom_set};
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

/// Internal helper: materialize the whole selected column chunk into a single
/// page. Locks the bloom set once for the chunk.
fn encode_column_chunk<F>(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
    mut emit: F,
) -> ParquetResult<Vec<Page>>
where
    F: FnMut(Option<&mut HashSet<u64>>) -> ParquetResult<Page>,
{
    if column_chunk_row_count(columns, first_partition_start, last_partition_end) == 0 {
        return Ok(vec![]);
    }

    let mut bloom_guard = lock_bloom_set(bloom_set.as_ref())?;
    let bloom = bloom_guard.as_deref_mut();
    Ok(vec![emit(bloom)?])
}
