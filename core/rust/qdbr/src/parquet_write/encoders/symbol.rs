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
//! across input partitions and emits one or more DataPages for the logical
//! column chunk, re-using those local-is-global keys.

use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use parquet2::bloom_filter::hash_byte;
use parquet2::encoding::hybrid_rle::encode_u32;
use parquet2::encoding::Encoding;
use parquet2::page::{DictPage, Page};
use parquet2::schema::types::PrimitiveType;
use parquet2::write::DynIter;

use qdb_core::col_type::{ColumnType, ColumnTypeTag};

use crate::parquet::error::{fmt_err, ParquetErrorReason, ParquetResult};
use crate::parquet_write::encoders::helpers::{
    column_chunk_row_count, lock_bloom_set, page_row_windows, partition_chunk_slice,
    rows_per_primitive_page, write_utf8_from_utf16_iter, FlatValidity,
};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::schema::Column;
use crate::parquet_write::util;
use crate::parquet_write::util::{
    build_plain_page, transmute_slice, BinaryMaxMinStats, ExactSizedIter,
};

pub struct SymbolGlobalInfo {
    pub used_keys: HashSet<u32>,
    pub max_key: u32,
}

/// Encode a Symbol column as RleDictionary pages: 1 DictPage built from the
/// union of used keys across all partitions, plus one or more DataPages for
/// the whole column chunk depending on `data_page_size`.
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

    // The Java caller sets `not_null_hint` (bit 31 of the column type) when
    // it believes no -1 keys exist in column data. We treat this strictly
    // as a perf hint: when set, the per-page encoder speculatively writes
    // an all-ones def-level run and validates while encoding keys, falling
    // back to per-row def levels if any -1 is observed. A stale hint is
    // therefore safe (issue #7007 was caused by trusting the hint without
    // validation).
    let not_null_hint = columns.iter().all(|c| c.not_null_hint);

    let mut merged_keys = Vec::with_capacity(total_rows);
    let mut used_keys = HashSet::new();
    let mut max_key = 0u32;
    for (part_idx, column) in columns.iter().enumerate() {
        let chunk = partition_chunk_slice(
            part_idx,
            num_partitions,
            column,
            first_partition_start,
            last_partition_end,
        );
        merged_keys.resize(merged_keys.len() + chunk.adjusted_column_top, -1);
        // SAFETY: JNI-backed, page-aligned, valid `i32` symbol keys.
        let all_keys: &[i32] = unsafe { transmute_slice(column.primary_data) };
        let chunk_keys = &all_keys[chunk.lower_bound..chunk.upper_bound];
        for &key in chunk_keys {
            if key >= 0 {
                let key = key as u32;
                used_keys.insert(key);
                max_key = max_key.max(key);
            }
        }
        merged_keys.extend_from_slice(chunk_keys);
    }

    let global_info = SymbolGlobalInfo { used_keys, max_key };

    // Build dict page (with bloom hashes) under a single lock.
    let (dict_buffer, dict_entry_count, value_ranges) = {
        let mut bloom_guard = lock_bloom_set(bloom_set.as_ref())?;
        let bloom = bloom_guard.as_deref_mut();
        prepare_symbol_dictionary(&global_info, offsets, chars, bloom)?
    };

    let rows_per_page = rows_per_primitive_page(&options, primitive_type.physical_type);
    let mut data_pages = Vec::with_capacity(total_rows.div_ceil(rows_per_page));
    for window in page_row_windows(total_rows, rows_per_page) {
        let page_values = &merged_keys[window.row_offset..window.row_offset + window.row_count];
        let data_page = encode_symbol_data_page(
            page_values,
            window.row_count,
            global_info.max_key,
            options,
            primitive_type.clone(),
            &dict_buffer,
            &value_ranges,
            not_null_hint,
        )?;
        data_pages.push(data_page);
    }

    let mut pages = Vec::with_capacity(1 + data_pages.len());
    pages.push(Page::Dict(DictPage::new(
        dict_buffer,
        dict_entry_count,
        false,
    )));
    pages.extend(data_pages);
    Ok(pages)
}

pub(crate) fn prepare_symbol_dictionary(
    global_info: &SymbolGlobalInfo,
    offsets: &[u64],
    chars: &[u8],
    bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<(Vec<u8>, usize, Vec<Range<usize>>)> {
    let (dict_buffer, value_ranges) = build_dict_buffer(
        &global_info.used_keys,
        global_info.max_key,
        offsets,
        chars,
        bloom_hashes,
    )?;
    let dict_entry_count = if global_info.used_keys.is_empty() {
        0
    } else {
        (global_info.max_key + 1) as usize
    };
    Ok((dict_buffer, dict_entry_count, value_ranges))
}

/// Encodes a single Symbol data page.
///
/// When `not_null_hint` is true, the encoder speculatively writes an
/// all-ones def-level RLE run (~3 bytes regardless of row count) and
/// streams keys through `encode_u32` with a validating iterator that
/// flags any `-1`. `encode_u32` collects the iterator into a `Vec<u32>`
/// before writing any output bytes (parquet2 hybrid_rle/encoder.rs:37),
/// so the side effect runs to completion before commit; on `saw_null`
/// the speculative output is truncated and the page is re-encoded with a
/// proper validity bitmap and filtered keys.
///
/// Stats are accumulated during the same iteration over `page_values`
/// (speculative or slow path). They observe exactly the `v >= 0` entries
/// that the slow path would visit, so a speculation-then-restart leaves
/// stats correct without recomputation. Bloom hashes are populated once
/// per column chunk in `prepare_symbol_dictionary` and are not touched
/// here.
///
/// A stale hint is safe: at worst we pay the speculative pass + a
/// restart. The encoder's correctness no longer depends on the Java side
/// supplying a truthful hint.
#[allow(clippy::too_many_arguments)]
fn encode_symbol_data_page(
    page_values: &[i32],
    num_rows: usize,
    max_key: u32,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    dict_buffer: &[u8],
    value_ranges: &[Range<usize>],
    not_null_hint: bool,
) -> ParquetResult<Page> {
    debug_assert!(
        page_values
            .iter()
            .filter(|&&k| k >= 0)
            .all(|&k| (k as u32) <= max_key),
        "local key exceeds max_key, encoding would be invalid"
    );

    let bits_per_key = util::bit_width(max_key as u64);
    let mut data_buffer = vec![];

    // Stats live across speculative/slow attempts. They are updated
    // exactly once per `v >= 0` entry, regardless of which path commits.
    let stats = options
        .write_statistics
        .then(|| RefCell::new(BinaryMaxMinStats::new(&primitive_type)));

    let (definition_levels_byte_length, total_null_count) = if not_null_hint {
        // Speculative fast path.
        let dl_start = data_buffer.len();
        util::encode_all_ones_def_levels(&mut data_buffer, num_rows, options.version);
        let dl_speculative_byte_len = data_buffer.len() - dl_start;
        data_buffer.push(bits_per_key);

        let saw_null = Cell::new(false);
        let validating_keys = page_values.iter().map(|&v| {
            if v < 0 {
                saw_null.set(true);
                0u32
            } else {
                let k = v as u32;
                if let Some(s) = stats.as_ref() {
                    let range = &value_ranges[k as usize];
                    s.borrow_mut().update(&dict_buffer[range.clone()]);
                }
                k
            }
        });
        encode_u32(
            &mut data_buffer,
            validating_keys,
            num_rows,
            bits_per_key as u32,
        )?;

        if !saw_null.get() {
            (dl_speculative_byte_len, 0usize)
        } else {
            // Speculation invalidated. Discard everything we wrote for
            // this page and re-encode the slow path. Stats accumulated
            // during the speculative iterator already saw every v >= 0
            // entry, so we do NOT re-update them here.
            data_buffer.truncate(dl_start);
            let validity = build_symbol_validity(page_values, 0);
            let dl_meta = validity.encode_def_levels(&mut data_buffer, options.version)?;
            data_buffer.push(bits_per_key);
            let non_null_len = num_rows - dl_meta.null_count;
            let local_keys = page_values
                .iter()
                .filter_map(|&v| if v >= 0 { Some(v as u32) } else { None });
            let keys = ExactSizedIter::new(local_keys, non_null_len);
            encode_u32(&mut data_buffer, keys, non_null_len, bits_per_key as u32)?;
            (dl_meta.definition_levels_byte_length, dl_meta.null_count)
        }
    } else {
        // Slow path (caller already declared nulls present, no point
        // speculating). Build validity and accumulate stats in a single
        // pass over page_values.
        let mut validity = FlatValidity::new();
        validity.reset(num_rows);
        let mut non_null_len: usize = 0;
        for &v in page_values {
            if v < 0 {
                validity.push_null();
            } else {
                validity.push_present();
                non_null_len += 1;
                if let Some(s) = stats.as_ref() {
                    let range = &value_ranges[v as usize];
                    s.borrow_mut().update(&dict_buffer[range.clone()]);
                }
            }
        }
        let dl_meta = validity.encode_def_levels(&mut data_buffer, options.version)?;
        data_buffer.push(bits_per_key);
        let local_keys = page_values
            .iter()
            .filter_map(|&v| if v >= 0 { Some(v as u32) } else { None });
        let keys = ExactSizedIter::new(local_keys, non_null_len);
        encode_u32(&mut data_buffer, keys, non_null_len, bits_per_key as u32)?;
        (dl_meta.definition_levels_byte_length, dl_meta.null_count)
    };

    let page_stats = stats.map(|s| s.into_inner().into_parquet_stats(total_null_count));

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

fn build_symbol_validity(column_values: &[i32], column_top: usize) -> FlatValidity {
    let mut validity = FlatValidity::new();
    validity.reset(column_top + column_values.len());
    for _ in 0..column_top {
        validity.push_null();
    }
    for &value in column_values {
        if value >= 0 {
            validity.push_present();
        } else {
            validity.push_null();
        }
    }
    validity
}

const UTF16_LEN_SIZE: usize = 4;

fn get_symbol_utf16_bytes(chars: &[u8], qdb_global_offset: usize) -> Option<&[u8]> {
    // Use checked_add so a corrupt offset near usize::MAX can't wrap through the guard.
    if qdb_global_offset
        .checked_add(UTF16_LEN_SIZE)
        .is_none_or(|end| end > chars.len())
    {
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
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<(Vec<u8>, Vec<Range<usize>>)> {
    let end_value = if used_keys.is_empty() { 0 } else { max_key + 1 };

    let dense_count = used_keys.len() as u32;
    let sparse_count = end_value.saturating_sub(dense_count);
    let dict_buffer_size_estimate = (sparse_count * 4) + (dense_count * 10);

    let mut dict_buffer = Vec::with_capacity(dict_buffer_size_estimate as usize);
    let mut value_ranges = vec![0..0; end_value as usize];

    for key in 0..end_value {
        let key_index = dict_buffer.len();
        dict_buffer.extend_from_slice(&(0u32).to_le_bytes());

        if used_keys.contains(&key) {
            let qdb_global_offset = *offsets.get(key as usize).ok_or_else(|| {
                fmt_err!(Layout, "could not find symbol with key {key} in global map")
            })? as usize;

            let utf8_len = read_symbol_to_utf8(chars, qdb_global_offset, &mut dict_buffer)?;
            let value_range = (key_index + 4)..(key_index + 4 + utf8_len);
            let utf8_buf = &dict_buffer[value_range.clone()];

            if let Some(ref mut h) = bloom_hashes {
                h.insert(hash_byte(utf8_buf));
            }
            value_ranges[key as usize] = value_range;

            let utf8_len_bytes = (utf8_len as u32).to_le_bytes();
            dict_buffer[key_index..(key_index + 4)].copy_from_slice(&utf8_len_bytes);
        }
    }

    Ok((dict_buffer, value_ranges))
}

/// Legacy single-partition API kept for benchmarks.  Delegates to [`encode`].
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
    let primary_data = unsafe {
        std::slice::from_raw_parts(
            column_values.as_ptr() as *const u8,
            std::mem::size_of_val(column_values),
        )
    };
    // Forward the hint via bit 31 of the column type, matching the JNI
    // entry point's encoding (see Column::from_raw_data in schema.rs).
    let mut column_type_code = ColumnType::new(ColumnTypeTag::Symbol, 0).code();
    if not_null_hint {
        column_type_code |= i32::MIN;
    }
    let column = Column::from_raw_data(
        0,
        "symbol",
        column_type_code,
        column_top as i64,
        column_top + column_values.len(),
        primary_data.as_ptr(),
        primary_data.len(),
        chars.as_ptr(),
        chars.len(),
        offsets.as_ptr(),
        offsets.len(),
        false,
        false,
        0,
    )?;
    let row_count = column.row_count;
    let pages = encode(&[column], 0, row_count, &primitive_type, options, bloom_set)?;
    Ok(DynIter::new(pages.into_iter().map(Ok)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet::tests::ColumnTypeTagExt;
    use crate::parquet_write::tests::serialize_as_symbols;
    use parquet2::compression::CompressionOptions;
    use parquet2::page::DataPageHeader;
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

    fn page_v2_num_values(page: &Page) -> i32 {
        match page {
            Page::Data(data) => match &data.header {
                DataPageHeader::V2(h) => h.num_values,
                DataPageHeader::V1(_) => panic!("expected V2 header"),
            },
            _ => panic!("expected data page"),
        }
    }

    /// Build a Symbol Column borrowing the supplied keys / chars / offsets.
    fn make_symbol_column(
        keys: &[i32],
        chars: &[u8],
        offsets: &[u64],
        column_top: usize,
    ) -> Column {
        make_symbol_column_with_hint(keys, chars, offsets, column_top, false)
    }

    /// Like `make_symbol_column` but lets the test pass `not_null_hint`
    /// directly (encoded as bit 31 of the column type, matching the JNI
    /// boundary).
    fn make_symbol_column_with_hint(
        keys: &[i32],
        chars: &[u8],
        offsets: &[u64],
        column_top: usize,
        not_null_hint: bool,
    ) -> Column {
        let mut column_type_code = ColumnTypeTag::Symbol.into_type().code();
        if not_null_hint {
            column_type_code |= i32::MIN;
        }
        Column::from_raw_data(
            0,
            "sym",
            column_type_code,
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

    /// Extract V2 (num_values, num_nulls, definition_levels_byte_length)
    /// from a data page. Panics on V1.
    fn page_v2_metrics(page: &Page) -> (i32, i32, usize) {
        match page {
            Page::Data(data) => match &data.header {
                DataPageHeader::V2(h) => (
                    h.num_values,
                    h.num_nulls,
                    h.definition_levels_byte_length as usize,
                ),
                DataPageHeader::V1(_) => panic!("expected V2 header"),
            },
            _ => panic!("expected data page"),
        }
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
    fn symbol_honors_data_page_size() {
        let (chars, offsets) = serialize_as_symbols(vec!["foo", "bar", "baz"]);
        let keys: Vec<i32> = (0..20).map(|i| i % 3).collect();
        let col = make_symbol_column(&keys, &chars, &offsets, 0);
        let opts = WriteOptions { data_page_size: Some(64), ..write_options() };
        let pages = encode(&[col], 0, keys.len(), &primitive_type(), opts, None).expect("encode");

        assert_eq!(pages.len(), 4);
        assert!(matches!(pages[0], Page::Dict(_)));
        assert_eq!(page_v2_num_values(&pages[1]), 8);
        assert_eq!(page_v2_num_values(&pages[2]), 8);
        assert_eq!(page_v2_num_values(&pages[3]), 4);
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
    fn symbol_to_pages_legacy_api() {
        let (chars, offsets) = serialize_as_symbols(vec!["foo", "bar"]);
        let keys: Vec<i32> = vec![0, 1, -1, 0];
        let pages = symbol_to_pages(
            &keys,
            &offsets,
            &chars,
            0,
            write_options(),
            primitive_type(),
            false,
            None,
        )
        .expect("encode");
        let pages: Vec<_> = pages.map(|r| r.expect("page")).collect();
        assert_eq!(pages.len(), 2); // 1 dict + 1 data
        assert!(matches!(pages[0], Page::Dict(_)));
    }

    #[test]
    fn symbol_to_pages_legacy_with_column_top() {
        let (chars, offsets) = serialize_as_symbols(vec!["foo"]);
        let keys: Vec<i32> = vec![0, 0];
        let pages = symbol_to_pages(
            &keys,
            &offsets,
            &chars,
            3,
            write_options(),
            primitive_type(),
            false,
            None,
        )
        .expect("encode");
        let pages: Vec<_> = pages.map(|r| r.expect("page")).collect();
        assert_eq!(pages.len(), 2);
        // Data page should have 5 rows total (3 top + 2 data)
        assert_eq!(page_v2_num_values(&pages[1]), 5);
    }

    #[test]
    fn symbol_no_stats() {
        let (chars, offsets) = serialize_as_symbols(vec!["foo"]);
        let keys: Vec<i32> = vec![0, 0];
        let col = make_symbol_column(&keys, &chars, &offsets, 0);
        let opts = WriteOptions { write_statistics: false, ..write_options() };
        let pages = encode(&[col], 0, keys.len(), &primitive_type(), opts, None).expect("encode");
        assert_eq!(pages.len(), 2);
    }

    #[test]
    fn symbol_speculative_fast_path_commits_when_no_nulls() {
        // Hint asserts no nulls, and the data really has none. The encoder
        // must commit the speculative all-ones def-level run.
        let (chars, offsets) = serialize_as_symbols(vec!["foo", "bar"]);
        let keys: Vec<i32> = vec![0, 1, 0, 1, 0, 1, 0, 1, 0, 1];
        let col = make_symbol_column_with_hint(&keys, &chars, &offsets, 0, true);

        let pages = encode(
            &[col],
            0,
            keys.len(),
            &primitive_type(),
            write_options(),
            None,
        )
            .expect("encode");
        assert_eq!(pages.len(), 2);

        let (num_values, num_nulls, dl_byte_len) = page_v2_metrics(&pages[1]);
        assert_eq!(num_values, keys.len() as i32);
        assert_eq!(num_nulls, 0);

        // The all-ones fast path emits a tiny RLE run. Sanity-check by
        // re-deriving the expected length via the same util the encoder
        // uses on the fast path.
        let mut expected_dl = vec![];
        crate::parquet_write::util::encode_all_ones_def_levels(
            &mut expected_dl,
            keys.len(),
            write_options().version,
        );
        assert_eq!(dl_byte_len, expected_dl.len());
    }

    #[test]
    fn symbol_speculative_restart_on_stale_hint() {
        // Hint asserts no nulls but the data contains -1. Pre-#6949 this
        // crashed the encoder. Variant B's restart path must produce a
        // valid page with the correct null_count.
        let (chars, offsets) = serialize_as_symbols(vec!["foo", "bar"]);
        let keys: Vec<i32> = vec![0, 1, -1, 0, -1, 1, 0, -1];
        let expected_nulls = keys.iter().filter(|&&k| k < 0).count() as i32;
        let col = make_symbol_column_with_hint(&keys, &chars, &offsets, 0, true);

        let pages = encode(
            &[col],
            0,
            keys.len(),
            &primitive_type(),
            write_options(),
            None,
        )
            .expect("encode");
        assert_eq!(pages.len(), 2);

        let (num_values, num_nulls, _) = page_v2_metrics(&pages[1]);
        assert_eq!(num_values, keys.len() as i32);
        assert_eq!(num_nulls, expected_nulls);
    }

    #[test]
    fn symbol_speculative_matches_slow_path_for_same_data() {
        // Same input, both with and without the hint. The page-level
        // null counts must agree — speculation must not corrupt counts
        // even when it ends up restarting.
        let (chars, offsets) = serialize_as_symbols(vec!["foo", "bar", "baz"]);
        let keys: Vec<i32> = vec![0, -1, 1, -1, 2, 0, -1, 1, 2, -1];

        let col_hint = make_symbol_column_with_hint(&keys, &chars, &offsets, 0, true);
        let col_no_hint = make_symbol_column_with_hint(&keys, &chars, &offsets, 0, false);

        let pages_hint = encode(
            &[col_hint],
            0,
            keys.len(),
            &primitive_type(),
            write_options(),
            None,
        )
            .expect("encode hint");
        let pages_no_hint = encode(
            &[col_no_hint],
            0,
            keys.len(),
            &primitive_type(),
            write_options(),
            None,
        )
            .expect("encode no_hint");

        let m_hint = page_v2_metrics(&pages_hint[1]);
        let m_no_hint = page_v2_metrics(&pages_no_hint[1]);
        assert_eq!(m_hint.0, m_no_hint.0, "num_values disagree");
        assert_eq!(m_hint.1, m_no_hint.1, "num_nulls disagree");
        assert_eq!(
            m_hint.2, m_no_hint.2,
            "definition_levels_byte_length disagree"
        );
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
