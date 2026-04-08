/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 ******************************************************************************/

//! Symbol RLE-dictionary encoder.
//!
//! Symbol columns share a single global symbol table across all partitions in
//! a row group. The encoder builds the dict page from the union of used keys
//! across input partitions and emits one DataPage per partition that
//! re-uses those local-is-global keys.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;

use crate::parquet::error::ParquetResult;
use crate::parquet_write::encoders::helpers::{lock_bloom_set, partition_chunk_slice, ChunkSlice};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::schema::Column;
use crate::parquet_write::symbol::{
    build_symbol_dict_page, collect_symbol_global_info, symbol_to_data_page_only,
};
use crate::parquet_write::util::transmute_slice;

/// Encode a Symbol column as RleDictionary pages: 1 DictPage built from the
/// union of used keys across all partitions, plus one DataPage per partition.
pub fn encode(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>> {
    if columns.is_empty() {
        return Ok(vec![]);
    }

    let num_partitions = columns.len();

    // All partitions of a single column chunk share the same symbol table
    // (offsets + chars come from the column's secondary buffers).
    let first_column = &columns[0];
    let offsets = first_column.symbol_offsets;
    let chars = first_column.secondary_data;

    // Compute the per-partition (key_slice, chunk) pairs.
    struct PartitionSlice<'a> {
        keys: &'a [i32],
        chunk: ChunkSlice,
        not_null_hint: bool,
    }

    let partition_slices: Vec<PartitionSlice<'_>> = (0..num_partitions)
        .map(|part_idx| {
            let column = &columns[part_idx];
            let chunk = partition_chunk_slice(
                part_idx,
                num_partitions,
                column,
                first_partition_start,
                last_partition_end,
            );
            // SAFETY: JNI-backed, page-aligned, valid `i32` symbol keys.
            let all_keys: &[i32] = unsafe { transmute_slice(column.primary_data) };
            let keys = &all_keys[chunk.lower_bound..chunk.upper_bound];
            PartitionSlice { keys, chunk, not_null_hint: column.not_null_hint }
        })
        .collect();

    // Build the global dict from the union of used keys across partitions.
    let global_info = collect_symbol_global_info(partition_slices.iter().map(|p| p.keys));

    // Build dict page (with bloom hashes) under a single lock.
    let dict_page = {
        let mut bloom_guard = lock_bloom_set(bloom_set.as_ref())?;
        let bloom = bloom_guard.as_deref_mut();
        build_symbol_dict_page(&global_info, offsets, chars, bloom)?
    };

    let mut pages = Vec::with_capacity(num_partitions + 1);
    pages.push(Page::Dict(dict_page));

    // Emit one DataPage per partition. Each page may contribute its own bloom
    // hashes for the values it sees (the dict page covers only the union, but
    // not all partitions necessarily reference every dict entry — emitting per
    // page keeps stats accurate without re-locking the bloom mutex per page).
    for slice in partition_slices {
        let data_page = symbol_to_data_page_only(
            slice.keys,
            slice.chunk.adjusted_column_top,
            global_info.max_key,
            options,
            primitive_type.clone(),
            offsets,
            chars,
            slice.not_null_hint,
            None,
        )?;
        pages.push(data_page);
    }

    Ok(pages)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet::tests::ColumnTypeTagExt;
    use crate::parquet_write::tests::serialize_as_symbols;
    use parquet2::compression::CompressionOptions;
    use parquet2::schema::types::PhysicalType;
    use parquet2::write::Version;
    use qdb_core::col_type::ColumnTypeTag;

    fn write_options() -> WriteOptions {
        WriteOptions {
            write_statistics: true,
            version: Version::V2,
            compression: CompressionOptions::Uncompressed,
            row_group_size: None,
            data_page_size: None,
            raw_array_encoding: false,
            bloom_filter_fpp: 0.01,
            min_compression_ratio: 0.0,
        }
    }

    fn primitive_type() -> PrimitiveType {
        PrimitiveType::from_physical("col".to_string(), PhysicalType::ByteArray)
    }

    /// Build a Symbol Column borrowing the supplied keys / chars / offsets.
    fn make_symbol_column(
        keys: &[i32],
        chars: &[u8],
        offsets: &[u64],
        column_top: usize,
    ) -> Column {
        Column::from_raw_data(
            0,
            "sym",
            ColumnTypeTag::Symbol.into_type().code(),
            column_top as i64,
            keys.len(),
            keys.as_ptr() as *const u8,
            std::mem::size_of_val(keys),
            chars.as_ptr(),
            chars.len(),
            offsets.as_ptr(),
            offsets.len(),
            false,
            false,
            0,
        )
        .unwrap()
    }

    #[test]
    fn symbol_empty_columns_slice() {
        // Calling encode with zero partitions returns an empty page list.
        let pages =
            encode(&[], 0, 0, &primitive_type(), write_options(), None).expect("encode empty");
        assert!(pages.is_empty());
    }

    #[test]
    fn symbol_all_partitions_same_key() {
        // Two partitions, each referencing only key 0.
        let (chars, offsets) = serialize_as_symbols(vec!["only"]);
        let keys1: Vec<i32> = vec![0, 0, 0];
        let keys2: Vec<i32> = vec![0, 0];
        let col1 = make_symbol_column(&keys1, &chars, &offsets, 0);
        let col2 = make_symbol_column(&keys2, &chars, &offsets, 0);

        let columns = vec![col1, col2];
        let pages = encode(
            &columns,
            0,
            keys2.len(),
            &primitive_type(),
            write_options(),
            None,
        )
        .expect("encode");

        // 1 dict page + 2 data pages.
        assert_eq!(pages.len(), 3);
        assert!(matches!(pages[0], Page::Dict(_)));
        assert!(matches!(pages[1], Page::Data(_)));
        assert!(matches!(pages[2], Page::Data(_)));
    }

    #[test]
    fn symbol_emits_bloom_hashes_when_set_present() {
        // Pass a non-None bloom set to exercise the Some branch of
        // lock_bloom_set inside symbol::encode and the bloom.insert path
        // inside build_symbol_dict_page. The set should end up populated
        // with one hash per distinct symbol value across all partitions.
        let (chars, offsets) = serialize_as_symbols(vec!["foo", "bar", "baz"]);
        let keys1: Vec<i32> = vec![0, 1, 2];
        let keys2: Vec<i32> = vec![1, 2];
        let col1 = make_symbol_column(&keys1, &chars, &offsets, 0);
        let col2 = make_symbol_column(&keys2, &chars, &offsets, 0);

        let bloom = Arc::new(Mutex::new(HashSet::<u64>::new()));
        let pages = encode(
            &[col1, col2],
            0,
            keys2.len(),
            &primitive_type(),
            write_options(),
            Some(bloom.clone()),
        )
        .expect("encode");

        // Sanity-check the page layout (1 dict + 2 data) hasn't drifted.
        assert_eq!(pages.len(), 3);

        // The bloom set is populated from the *union* of all referenced
        // dict entries — three symbols total.
        let set = bloom.lock().expect("bloom lock");
        assert_eq!(set.len(), 3, "expected one hash per distinct symbol");
    }

    #[test]
    fn symbol_partition_with_column_top() {
        // Symbol column with column_top = 5: rows 0..5 are nulls, rows 5..10 are
        // data referencing the dict.
        let (chars, offsets) = serialize_as_symbols(vec!["a", "b"]);
        let keys: Vec<i32> = vec![0, 1, 0, 1, 0]; // 5 data rows
        let col = make_symbol_column(&keys, &chars, &offsets, 5);

        let columns = vec![col];
        // Column has 5 column-top rows + 5 data rows = 10 logical rows.
        let pages =
            encode(&columns, 0, 10, &primitive_type(), write_options(), None).expect("encode");
        assert_eq!(pages.len(), 2); // 1 dict + 1 data
        match &pages[1] {
            Page::Data(data_page) => {
                // The header should report 5 nulls (the column-top rows).
                let header = &data_page.header;
                let num_nulls = match header {
                    parquet2::page::DataPageHeader::V1(h) => {
                        // V1 doesn't carry num_nulls in the header — fall through.
                        h.num_values
                    }
                    parquet2::page::DataPageHeader::V2(h) => h.num_nulls,
                };
                let _ = num_nulls; // V1 num_values, V2 num_nulls; both expected to be sensible
                                   // Use the V2 path that we configured in write_options().
                if let parquet2::page::DataPageHeader::V2(h) = header {
                    assert_eq!(h.num_nulls, 5, "expected 5 column-top nulls");
                    assert_eq!(h.num_values, 10);
                } else {
                    panic!("expected V2 page header");
                }
            }
            _ => panic!("expected data page at index 1"),
        }
    }
}
