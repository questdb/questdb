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
//! across input partitions and emits one DataPage for the whole column chunk
//! that re-uses those local-is-global keys.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use parquet2::bloom_filter::hash_byte;
use parquet2::encoding::hybrid_rle::encode_u32;
use parquet2::encoding::Encoding;
use parquet2::page::{DictPage, Page};
use parquet2::schema::types::PrimitiveType;
use parquet2::statistics::ParquetStatistics;
use parquet2::write::DynIter;

use crate::parquet::error::{fmt_err, ParquetErrorExt, ParquetErrorReason, ParquetResult};
use crate::parquet_write::encoders::helpers::{
    column_chunk_row_count, lock_bloom_set, partition_chunk_slice, write_utf8_from_utf16_iter,
};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::schema::Column;
use crate::parquet_write::util;
use crate::parquet_write::util::{
    build_plain_page, encode_all_ones_def_levels, encode_primitive_def_levels, transmute_slice,
    BinaryMaxMinStats, ExactSizedIter,
};

pub struct SymbolGlobalInfo {
    pub used_keys: HashSet<u32>,
    pub max_key: u32,
}

/// Encode a Symbol column as RleDictionary pages: 1 DictPage built from the
/// union of used keys across all partitions, plus 1 DataPage for the whole
/// column chunk.
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

    let total_rows = column_chunk_row_count(columns, first_partition_start, last_partition_end);
    if total_rows == 0 {
        return Ok(vec![]);
    }

    let num_partitions = columns.len();

    // All partitions of a single column chunk share the same symbol table
    // (offsets + chars come from the column's secondary buffers).
    let first_column = &columns[0];
    let offsets = first_column.symbol_offsets;
    let chars = first_column.secondary_data;

    let mut merged_keys = Vec::with_capacity(total_rows);
    let mut used_keys = HashSet::new();
    let mut max_key = 0u32;
    let mut null_count = 0usize;
    for (part_idx, column) in columns.iter().enumerate() {
        let chunk = partition_chunk_slice(
            part_idx,
            num_partitions,
            column,
            first_partition_start,
            last_partition_end,
        );
        null_count += chunk.adjusted_column_top;
        merged_keys.resize(merged_keys.len() + chunk.adjusted_column_top, -1);
        // SAFETY: JNI-backed, page-aligned, valid `i32` symbol keys.
        let all_keys: &[i32] = unsafe { transmute_slice(column.primary_data) };
        let chunk_keys = &all_keys[chunk.lower_bound..chunk.upper_bound];
        for &key in chunk_keys {
            if key >= 0 {
                let key = key as u32;
                used_keys.insert(key);
                max_key = max_key.max(key);
            } else {
                null_count += 1;
            }
        }
        merged_keys.extend_from_slice(chunk_keys);
    }

    let global_info = SymbolGlobalInfo { used_keys, max_key };

    // Build dict page (with bloom hashes) under a single lock.
    let (dict_page, page_stats) = {
        let mut bloom_guard = lock_bloom_set(bloom_set.as_ref())?;
        let bloom = bloom_guard.as_deref_mut();
        prepare_symbol_dictionary(
            &global_info,
            offsets,
            chars,
            primitive_type,
            null_count,
            options.write_statistics,
            bloom,
        )?
    };

    let mut pages = Vec::with_capacity(2);
    pages.push(Page::Dict(dict_page));

    let data_page = symbol_to_data_page_only(
        &merged_keys,
        0,
        global_info.max_key,
        options,
        primitive_type.clone(),
        merged_keys.iter().all(|&key| key >= 0),
        page_stats,
    )?;
    pages.push(data_page);

    Ok(pages)
}

pub(crate) fn prepare_symbol_dictionary(
    global_info: &SymbolGlobalInfo,
    offsets: &[u64],
    chars: &[u8],
    primitive_type: &PrimitiveType,
    null_count: usize,
    write_statistics: bool,
    bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<(DictPage, Option<ParquetStatistics>)> {
    let mut stats = write_statistics.then(|| BinaryMaxMinStats::new(primitive_type));
    let dict_buffer = build_dict_buffer(
        &global_info.used_keys,
        global_info.max_key,
        offsets,
        chars,
        stats.as_mut(),
        bloom_hashes,
    )?;
    let unique_count = if global_info.used_keys.is_empty() {
        0
    } else {
        global_info.max_key + 1
    };
    let page_stats = stats.map(|stats| stats.into_parquet_stats(null_count));
    Ok((
        DictPage::new(dict_buffer, unique_count as usize, false),
        page_stats,
    ))
}

pub fn symbol_to_data_page_only(
    column_values: &[i32],
    column_top: usize,
    max_key: u32,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    not_null_hint: bool,
    page_stats: Option<ParquetStatistics>,
) -> ParquetResult<Page> {
    let num_rows = column_top + column_values.len();
    let mut data_buffer = vec![];

    debug_assert!(
        column_values
            .iter()
            .filter(|&&k| k >= 0)
            .all(|&k| (k as u32) <= max_key),
        "local key exceeds max_key, encoding would be invalid"
    );

    // Always encode def levels so the file-level schema stays OPTIONAL
    // across O3 merges. When there are no nulls (not_null_hint from Java),
    // a single RLE run of 1s is ~3 bytes regardless of row count.
    // The hint can be stale, so fall back to per-row def levels when
    // nulls are actually present (column_top > 0).
    let (definition_levels_byte_length, data_null_count) = if not_null_hint && column_top == 0 {
        encode_all_ones_def_levels(&mut data_buffer, num_rows, options.version);
        (data_buffer.len(), 0)
    } else {
        let data_null_count = column_values.iter().filter(|&&k| k < 0).count();
        let def_levels = (0..num_rows).map(|i| {
            if i < column_top {
                false
            } else {
                column_values[i - column_top] >= 0
            }
        });

        encode_primitive_def_levels(&mut data_buffer, def_levels, num_rows, options.version)?;
        (data_buffer.len(), data_null_count)
    };
    let total_null_count = column_top + data_null_count;

    let bits_per_key = util::bit_width(max_key as u64);
    let non_null_len = column_values.len() - data_null_count;
    let local_keys =
        column_values
            .iter()
            .filter_map(|&value| if value >= 0 { Some(value as u32) } else { None });
    let keys = ExactSizedIter::new(local_keys, non_null_len);
    data_buffer.push(bits_per_key);
    encode_u32(&mut data_buffer, keys, non_null_len, bits_per_key as u32)?;

    let data_page = build_plain_page(
        data_buffer,
        num_rows,
        total_null_count,
        definition_levels_byte_length,
        page_stats,
        primitive_type,
        options,
        Encoding::RleDictionary,
        false,
    )?;

    Ok(Page::Data(data_page))
}

const UTF16_LEN_SIZE: usize = 4;

fn get_symbol_utf16_bytes(chars: &[u8], qdb_global_offset: usize) -> Option<&[u8]> {
    if qdb_global_offset + UTF16_LEN_SIZE > chars.len() {
        return None;
    }

    let qdb_utf16_len_buf = &chars[qdb_global_offset..];
    let qdb_utf16_len =
        i32::from_le_bytes(qdb_utf16_len_buf[..4].try_into().expect("4 bytes")) as usize;

    let required_len = UTF16_LEN_SIZE + qdb_utf16_len * 2;
    if qdb_utf16_len_buf.len() < required_len {
        return None;
    }

    Some(&qdb_utf16_len_buf[UTF16_LEN_SIZE..UTF16_LEN_SIZE + qdb_utf16_len * 2])
}

fn read_symbol_to_utf8(
    chars: &[u8],
    qdb_global_offset: usize,
    dest: &mut Vec<u8>,
) -> ParquetResult<usize> {
    let utf16_bytes = get_symbol_utf16_bytes(chars, qdb_global_offset).ok_or_else(|| {
        fmt_err!(
            Layout,
            "global symbol map character data too small, offset {qdb_global_offset} out of bounds"
        )
    })?;

    let utf16_iter = utf16_bytes
        .chunks_exact(2)
        .map(|b| u16::from_le_bytes([b[0], b[1]]));

    let utf8_len = write_utf8_from_utf16_iter(dest, utf16_iter)
        .map_err(|e| ParquetErrorReason::Utf16Decode(e).into_err())?;
    Ok(utf8_len)
}

fn build_dict_buffer(
    used_keys: &HashSet<u32>,
    max_key: u32,
    offsets: &[u64],
    chars: &[u8],
    mut stats: Option<&mut BinaryMaxMinStats>,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<Vec<u8>> {
    let end_value = if used_keys.is_empty() { 0 } else { max_key + 1 };

    let dense_count = used_keys.len() as u32;
    let sparse_count = end_value.saturating_sub(dense_count);
    let dict_buffer_size_estimate = (sparse_count * 4) + (dense_count * 10);

    let mut dict_buffer = Vec::with_capacity(dict_buffer_size_estimate as usize);

    for key in 0..end_value {
        let key_index = dict_buffer.len();
        dict_buffer.extend_from_slice(&(0u32).to_le_bytes());

        if used_keys.contains(&key) {
            let qdb_global_offset = *offsets.get(key as usize).ok_or_else(|| {
                fmt_err!(Layout, "could not find symbol with key {key} in global map")
            })? as usize;

            let utf8_len = read_symbol_to_utf8(chars, qdb_global_offset, &mut dict_buffer)?;
            let utf8_buf = &dict_buffer[(key_index + 4)..(key_index + 4 + utf8_len)];

            if let Some(ref mut s) = stats {
                s.update(utf8_buf);
            }
            if let Some(ref mut h) = bloom_hashes {
                h.insert(hash_byte(utf8_buf));
            }

            let utf8_len_bytes = (utf8_len as u32).to_le_bytes();
            dict_buffer[key_index..(key_index + 4)].copy_from_slice(&utf8_len_bytes);
        }
    }

    Ok(dict_buffer)
}

fn encode_symbols_dict<'a>(
    column_vals: &'a [i32],
    offsets: &[u64],
    chars: &[u8],
    stats: &mut BinaryMaxMinStats,
    bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<(Vec<u8>, impl Iterator<Item = u32> + 'a, u32)> {
    let mut values_set = HashSet::with_capacity(offsets.len());
    for &value in column_vals {
        if value >= 0 {
            values_set.insert(value as u32);
        }
    }

    let max_key = values_set.iter().copied().max().unwrap_or(0);
    let dict_buffer = build_dict_buffer(
        &values_set,
        max_key,
        offsets,
        chars,
        Some(stats),
        bloom_hashes,
    )?;

    let local_keys =
        column_vals
            .iter()
            .filter_map(|&value| if value >= 0 { Some(value as u32) } else { None });

    Ok((dict_buffer, local_keys, max_key))
}

#[allow(clippy::too_many_arguments)]
pub fn symbol_to_pages(
    column_values: &[i32],
    offsets: &[u64],
    chars: &[u8],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    not_null_hint: bool,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<DynIter<'static, ParquetResult<Page>>> {
    let num_rows = column_top + column_values.len();
    let mut data_buffer = vec![];

    // Always encode def levels so the file-level schema stays OPTIONAL
    // across O3 merges. When there are no nulls (not_null_hint from Java),
    // a single RLE run of 1s is ~3 bytes regardless of row count.
    // The hint can be stale, so fall back to per-row def levels when
    // nulls are actually present (column_top > 0).
    let (definition_levels_byte_length, data_null_count) = if not_null_hint && column_top == 0 {
        encode_all_ones_def_levels(&mut data_buffer, num_rows, options.version);
        (data_buffer.len(), 0)
    } else {
        let data_null_count = column_values.iter().filter(|&&key| key < 0).count();
        let def_levels = (0..num_rows).map(|i| {
            if i < column_top {
                false
            } else {
                column_values[i - column_top] >= 0
            }
        });

        encode_primitive_def_levels(&mut data_buffer, def_levels, num_rows, options.version)?;
        (data_buffer.len(), data_null_count)
    };
    let total_null_count = column_top + data_null_count;

    let mut stats = BinaryMaxMinStats::new(&primitive_type);
    let (dict_buffer, keys, max_key) = {
        let mut bloom_guard = bloom_set
            .as_ref()
            .map(|arc| {
                arc.lock()
                    .map_err(|_| fmt_err!(Layout, "bloom filter mutex poisoned"))
            })
            .transpose()?;
        encode_symbols_dict(
            column_values,
            offsets,
            chars,
            &mut stats,
            bloom_guard.as_deref_mut(),
        )
        .context("could not write symbols dict map page")?
    };

    let bits_per_key = util::bit_width(max_key as u64);
    let non_null_len = column_values.len() - data_null_count;
    let keys = ExactSizedIter::new(keys, non_null_len);
    data_buffer.push(bits_per_key);
    encode_u32(&mut data_buffer, keys, non_null_len, bits_per_key as u32)?;

    let data_page = build_plain_page(
        data_buffer,
        num_rows,
        total_null_count,
        definition_levels_byte_length,
        if options.write_statistics {
            Some(stats.into_parquet_stats(total_null_count))
        } else {
            None
        },
        primitive_type,
        options,
        Encoding::RleDictionary,
        false,
    )?;

    let unique_count = if !dict_buffer.is_empty() {
        max_key + 1
    } else {
        0
    };
    let dict_page = DictPage::new(dict_buffer, unique_count as usize, false);

    Ok(DynIter::new(
        [Page::Dict(dict_page), Page::Data(data_page)]
            .into_iter()
            .map(Ok),
    ))
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

        // 1 dict page + 1 data page.
        assert_eq!(pages.len(), 2);
        assert!(matches!(pages[0], Page::Dict(_)));
        assert!(matches!(pages[1], Page::Data(_)));
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

        // Sanity-check the page layout (1 dict + 1 data) hasn't drifted.
        assert_eq!(pages.len(), 2);

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
