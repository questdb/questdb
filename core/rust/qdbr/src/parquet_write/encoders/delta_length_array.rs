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

//! Delta-length-byte-array encoders for byte-array families (String, Binary,
//! Varchar).

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use parquet2::encoding::Encoding;
use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;

use crate::parquet::error::ParquetResult;
use crate::parquet_write::encoders::helpers::{
    lock_bloom_set, partition_slice_range, rows_per_primitive_page, ChunkSlice, PartitionPageSlices,
};
use crate::parquet_write::encoders::plain::{binary_to_page, string_to_page, varchar_to_page};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::schema::Column;
use crate::parquet_write::util::transmute_slice;

/// Encode a String column as DeltaLengthByteArray pages.
pub fn encode_string(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>> {
    let rpp = rows_per_primitive_page(&options, primitive_type.physical_type);
    encode_per_partition(
        columns,
        first_partition_start,
        last_partition_end,
        rpp,
        bloom_set,
        |column, chunk, bloom| {
            // SAFETY: Data originates from JNI/Java memory-mapped column data, which is
            // page-aligned. The byte content represents valid `i64` offsets.
            let aux: &[i64] = unsafe { transmute_slice(column.secondary_data) };
            let aux_slice = &aux[chunk.lower_bound..chunk.upper_bound];
            string_to_page(
                aux_slice,
                column.primary_data,
                chunk.adjusted_column_top,
                options,
                primitive_type.clone(),
                Encoding::DeltaLengthByteArray,
                bloom,
            )
        },
    )
}

/// Encode a Binary column as DeltaLengthByteArray pages.
pub fn encode_binary(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>> {
    let rpp = rows_per_primitive_page(&options, primitive_type.physical_type);
    encode_per_partition(
        columns,
        first_partition_start,
        last_partition_end,
        rpp,
        bloom_set,
        |column, chunk, bloom| {
            // SAFETY: Data originates from JNI/Java memory-mapped column data, which is
            // page-aligned. The byte content represents valid `i64` offsets.
            let aux: &[i64] = unsafe { transmute_slice(column.secondary_data) };
            let aux_slice = &aux[chunk.lower_bound..chunk.upper_bound];
            binary_to_page(
                aux_slice,
                column.primary_data,
                chunk.adjusted_column_top,
                options,
                primitive_type.clone(),
                Encoding::DeltaLengthByteArray,
                bloom,
            )
        },
    )
}

/// Encode a Varchar column as DeltaLengthByteArray pages.
pub fn encode_varchar(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>> {
    let rpp = rows_per_primitive_page(&options, primitive_type.physical_type);
    encode_per_partition(
        columns,
        first_partition_start,
        last_partition_end,
        rpp,
        bloom_set,
        |column, chunk, bloom| {
            // SAFETY: Data originates from JNI/Java memory-mapped column data, which is
            // page-aligned. The byte content represents valid `[u8; 16]` aux entries.
            let aux: &[[u8; 16]] = unsafe { transmute_slice(column.secondary_data) };
            let aux_slice = &aux[chunk.lower_bound..chunk.upper_bound];
            varchar_to_page(
                aux_slice,
                column.primary_data,
                chunk.adjusted_column_top,
                options,
                primitive_type.clone(),
                Encoding::DeltaLengthByteArray,
                bloom,
            )
        },
    )
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet::tests::ColumnTypeTagExt;
    use crate::parquet_write::schema::column_type_to_parquet_type;
    use parquet2::compression::CompressionOptions;
    use parquet2::page::DataPageHeader;
    use parquet2::schema::types::ParquetType;
    use parquet2::write::Version;
    use qdb_core::col_type::{ColumnType, ColumnTypeTag};
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};

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

    fn primitive_type_for(tag: ColumnTypeTag) -> PrimitiveType {
        let column_type = ColumnType::new(tag, 0);
        match column_type_to_parquet_type(0, "col", column_type, false, false).expect("type") {
            ParquetType::PrimitiveType(pt) => pt,
            _ => panic!("expected primitive type for {:?}", tag),
        }
    }

    fn v2_header(page: &Page) -> (i32, i32, i32) {
        match page {
            Page::Data(d) => match &d.header {
                DataPageHeader::V2(h) => (h.num_values, h.num_nulls, h.encoding.0),
                DataPageHeader::V1(_) => panic!("expected V2 header"),
            },
            _ => panic!("expected data page"),
        }
    }

    fn make_string_aux(strings: &[&str]) -> (Vec<u8>, Vec<i64>) {
        let mut data = Vec::new();
        let mut offsets = Vec::new();
        for s in strings {
            let utf16: Vec<u16> = s.encode_utf16().collect();
            offsets.push(data.len() as i64);
            data.extend_from_slice(&(utf16.len() as i32).to_le_bytes());
            let bytes: &[u8] = unsafe {
                std::slice::from_raw_parts(
                    utf16.as_ptr() as *const u8,
                    utf16.len() * std::mem::size_of::<u16>(),
                )
            };
            data.extend_from_slice(bytes);
        }
        (data, offsets)
    }

    fn make_binary_aux(slices: &[&[u8]]) -> (Vec<u8>, Vec<i64>) {
        let mut data = Vec::new();
        let mut offsets = Vec::new();
        for s in slices {
            offsets.push(data.len() as i64);
            data.extend_from_slice(&(s.len() as i64).to_le_bytes());
            data.extend_from_slice(s);
        }
        (data, offsets)
    }

    fn make_varchar_aux_inlined(value: &[u8]) -> [u8; 16] {
        assert!(value.len() <= 9);
        let mut entry = [0u8; 16];
        entry[0] = ((value.len() as u8) << 4) | 0b11;
        entry[1..1 + value.len()].copy_from_slice(value);
        entry
    }

    fn make_varchar_aux_split(value: &[u8], offset: usize) -> [u8; 16] {
        assert!(value.len() > 9);
        let mut entry = [0u8; 16];
        let header: u32 = ((value.len() as u32) << 4) | 0b10;
        entry[0..4].copy_from_slice(&header.to_le_bytes());
        entry[4..10].copy_from_slice(&value[..6]);
        entry[10..12].copy_from_slice(&(offset as u16).to_le_bytes());
        entry[12..16].copy_from_slice(&((offset >> 16) as u32).to_le_bytes());
        entry
    }

    #[test]
    fn encode_string_delta_round_trip() {
        let (data_buf, offsets) = make_string_aux(&["alpha", "beta", "gamma", "delta"]);
        let col = Column::from_raw_data(
            0,
            "col",
            ColumnTypeTag::String.into_type().code(),
            0,
            offsets.len(),
            data_buf.as_ptr(),
            data_buf.len(),
            offsets.as_ptr() as *const u8,
            std::mem::size_of_val(&offsets[..]),
            std::ptr::null(),
            0,
            false,
            false,
            0,
        )
        .unwrap();
        let pt = primitive_type_for(ColumnTypeTag::String);
        let pages =
            encode_string(&[col], 0, offsets.len(), &pt, write_options(), None).expect("encode");
        assert_eq!(pages.len(), 1);
        let (num_values, num_nulls, enc) = v2_header(&pages[0]);
        assert_eq!(num_values, 4);
        assert_eq!(num_nulls, 0);
        // DELTA_LENGTH_BYTE_ARRAY = 6
        assert_eq!(enc, 6);
    }

    #[test]
    fn encode_binary_delta_round_trip() {
        let (data_buf, offsets) = make_binary_aux(&[b"abc", b"defgh", b"ij"]);
        let col = Column::from_raw_data(
            0,
            "col",
            ColumnTypeTag::Binary.into_type().code(),
            0,
            offsets.len(),
            data_buf.as_ptr(),
            data_buf.len(),
            offsets.as_ptr() as *const u8,
            std::mem::size_of_val(&offsets[..]),
            std::ptr::null(),
            0,
            false,
            false,
            0,
        )
        .unwrap();
        let pt = primitive_type_for(ColumnTypeTag::Binary);
        let pages =
            encode_binary(&[col], 0, offsets.len(), &pt, write_options(), None).expect("encode");
        assert_eq!(pages.len(), 1);
        let (num_values, _, enc) = v2_header(&pages[0]);
        assert_eq!(num_values, 3);
        assert_eq!(enc, 6);
    }

    #[test]
    fn encode_varchar_delta_round_trip() {
        let aux = [
            make_varchar_aux_inlined(b"hi"),
            make_varchar_aux_split(b"hello world!!", 0),
            make_varchar_aux_inlined(b"bye"),
        ];
        let mut data = Vec::new();
        data.extend_from_slice(b"hello world!!");
        let col = Column::from_raw_data(
            0,
            "col",
            ColumnTypeTag::Varchar.into_type().code(),
            0,
            aux.len(),
            data.as_ptr(),
            data.len(),
            aux.as_ptr() as *const u8,
            aux.len() * 16,
            std::ptr::null(),
            0,
            false,
            false,
            0,
        )
        .unwrap();
        let pt = primitive_type_for(ColumnTypeTag::Varchar);
        let pages =
            encode_varchar(&[col], 0, aux.len(), &pt, write_options(), None).expect("encode");
        assert_eq!(pages.len(), 1);
        let (num_values, num_nulls, enc) = v2_header(&pages[0]);
        assert_eq!(num_values, 3);
        assert_eq!(num_nulls, 0);
        assert_eq!(enc, 6);
    }

    #[test]
    fn encode_string_delta_multi_partition_independent_pages() {
        // Two partitions of strings; verify each gets its own delta page.
        let (data_buf1, offsets1) = make_string_aux(&["a", "b", "c"]);
        let (data_buf2, offsets2) = make_string_aux(&["x", "y"]);
        let col1 = Column::from_raw_data(
            0,
            "col",
            ColumnTypeTag::String.into_type().code(),
            0,
            offsets1.len(),
            data_buf1.as_ptr(),
            data_buf1.len(),
            offsets1.as_ptr() as *const u8,
            offsets1.len() * std::mem::size_of::<i64>(),
            std::ptr::null(),
            0,
            false,
            false,
            0,
        )
        .unwrap();
        let col2 = Column::from_raw_data(
            0,
            "col",
            ColumnTypeTag::String.into_type().code(),
            0,
            offsets2.len(),
            data_buf2.as_ptr(),
            data_buf2.len(),
            offsets2.as_ptr() as *const u8,
            offsets2.len() * std::mem::size_of::<i64>(),
            std::ptr::null(),
            0,
            false,
            false,
            0,
        )
        .unwrap();
        let pt = primitive_type_for(ColumnTypeTag::String);
        let pages = encode_string(&[col1, col2], 0, offsets2.len(), &pt, write_options(), None)
            .expect("encode");
        assert_eq!(pages.len(), 2, "delta encoders emit one page per partition");
        let (n0, _, _) = v2_header(&pages[0]);
        let (n1, _, _) = v2_header(&pages[1]);
        assert_eq!(n0, 3);
        assert_eq!(n1, 2);
    }

    #[test]
    fn encode_varchar_delta_with_column_top() {
        // 10 column-top rows + 3 data rows = 13 logical rows.
        let aux = [
            make_varchar_aux_inlined(b"a"),
            make_varchar_aux_inlined(b"b"),
            make_varchar_aux_inlined(b"c"),
        ];
        let data: Vec<u8> = vec![];
        let col = Column::from_raw_data(
            0,
            "col",
            ColumnTypeTag::Varchar.into_type().code(),
            10, // column_top
            aux.len(),
            data.as_ptr(),
            data.len(),
            aux.as_ptr() as *const u8,
            aux.len() * 16,
            std::ptr::null(),
            0,
            false,
            false,
            0,
        )
        .unwrap();
        // Sanity: this Column has column_top=10 (set via from_raw_data) and 3 data rows.
        // We pass last_partition_end = 13 to cover top + data.
        let pt = primitive_type_for(ColumnTypeTag::Varchar);
        let pages = encode_varchar(&[col], 0, 13, &pt, write_options(), None).expect("encode");
        assert_eq!(pages.len(), 1);
        let (num_values, num_nulls, _) = v2_header(&pages[0]);
        assert_eq!(num_values, 13);
        assert_eq!(num_nulls, 10);
    }

    fn make_varchar_aux_null() -> [u8; 16] {
        let mut entry = [0u8; 16];
        entry[0] = 0b100;
        entry
    }

    fn make_null_string_aux(count: usize) -> (Vec<u8>, Vec<i64>) {
        let mut data = Vec::new();
        let mut offsets = Vec::new();
        for _ in 0..count {
            offsets.push(data.len() as i64);
            data.extend_from_slice(&(-1i32).to_le_bytes());
        }
        (data, offsets)
    }

    fn build_string_column(data: &[u8], offsets: &[i64]) -> Column {
        Column::from_raw_data(
            0,
            "col",
            ColumnTypeTag::String.into_type().code(),
            0,
            offsets.len(),
            data.as_ptr(),
            data.len(),
            offsets.as_ptr() as *const u8,
            std::mem::size_of_val(offsets),
            std::ptr::null(),
            0,
            false,
            false,
            0,
        )
        .unwrap()
    }

    fn build_string_column_with_top(data: &[u8], offsets: &[i64], column_top: usize) -> Column {
        Column::from_raw_data(
            0,
            "col",
            ColumnTypeTag::String.into_type().code(),
            column_top as i64,
            offsets.len(),
            data.as_ptr(),
            data.len(),
            offsets.as_ptr() as *const u8,
            std::mem::size_of_val(offsets),
            std::ptr::null(),
            0,
            false,
            false,
            0,
        )
        .unwrap()
    }

    fn build_binary_column(data: &[u8], offsets: &[i64]) -> Column {
        Column::from_raw_data(
            0,
            "col",
            ColumnTypeTag::Binary.into_type().code(),
            0,
            offsets.len(),
            data.as_ptr(),
            data.len(),
            offsets.as_ptr() as *const u8,
            std::mem::size_of_val(offsets),
            std::ptr::null(),
            0,
            false,
            false,
            0,
        )
        .unwrap()
    }

    fn build_binary_column_with_top(data: &[u8], offsets: &[i64], column_top: usize) -> Column {
        Column::from_raw_data(
            0,
            "col",
            ColumnTypeTag::Binary.into_type().code(),
            column_top as i64,
            offsets.len(),
            data.as_ptr(),
            data.len(),
            offsets.as_ptr() as *const u8,
            std::mem::size_of_val(offsets),
            std::ptr::null(),
            0,
            false,
            false,
            0,
        )
        .unwrap()
    }

    fn build_varchar_column(aux: &[[u8; 16]], data: &[u8]) -> Column {
        Column::from_raw_data(
            0,
            "col",
            ColumnTypeTag::Varchar.into_type().code(),
            0,
            aux.len(),
            data.as_ptr(),
            data.len(),
            aux.as_ptr() as *const u8,
            aux.len() * 16,
            std::ptr::null(),
            0,
            false,
            false,
            0,
        )
        .unwrap()
    }

    // ----- null handling -----

    #[test]
    fn encode_string_delta_with_null_values() {
        let mut data = Vec::new();
        let mut offsets = Vec::new();
        // Row 0: "hi"
        let (d, _) = make_string_aux(&["hi"]);
        offsets.push(data.len() as i64);
        data.extend_from_slice(&d);
        // Row 1: null (length header = -1)
        offsets.push(data.len() as i64);
        data.extend_from_slice(&(-1i32).to_le_bytes());
        // Row 2: "yo"
        let start = data.len();
        let (d2, _) = make_string_aux(&["yo"]);
        offsets.push(start as i64);
        data.extend_from_slice(&d2);

        let col = build_string_column(&data, &offsets);
        let pt = primitive_type_for(ColumnTypeTag::String);
        let pages =
            encode_string(&[col], 0, offsets.len(), &pt, write_options(), None).expect("encode");
        assert_eq!(pages.len(), 1);
        let (num_values, num_nulls, enc) = v2_header(&pages[0]);
        assert_eq!(num_values, 3);
        assert_eq!(num_nulls, 1);
        assert_eq!(enc, 6);
    }

    #[test]
    fn encode_binary_delta_with_null_values() {
        let mut data = Vec::new();
        let mut offsets = Vec::new();
        // Row 0: "abc"
        offsets.push(data.len() as i64);
        data.extend_from_slice(&3i64.to_le_bytes());
        data.extend_from_slice(b"abc");
        // Row 1: null (negative length)
        offsets.push(data.len() as i64);
        data.extend_from_slice(&(-1i64).to_le_bytes());
        // Row 2: "z"
        offsets.push(data.len() as i64);
        data.extend_from_slice(&1i64.to_le_bytes());
        data.extend_from_slice(b"z");

        let col = build_binary_column(&data, &offsets);
        let pt = primitive_type_for(ColumnTypeTag::Binary);
        let pages =
            encode_binary(&[col], 0, offsets.len(), &pt, write_options(), None).expect("encode");
        assert_eq!(pages.len(), 1);
        let (num_values, num_nulls, enc) = v2_header(&pages[0]);
        assert_eq!(num_values, 3);
        assert_eq!(num_nulls, 1);
        assert_eq!(enc, 6);
    }

    #[test]
    fn encode_varchar_delta_with_null_values() {
        let aux = [
            make_varchar_aux_inlined(b"hi"),
            make_varchar_aux_null(),
            make_varchar_aux_inlined(b"bye"),
            make_varchar_aux_null(),
        ];
        let data: Vec<u8> = vec![];
        let col = build_varchar_column(&aux, &data);
        let pt = primitive_type_for(ColumnTypeTag::Varchar);
        let pages =
            encode_varchar(&[col], 0, aux.len(), &pt, write_options(), None).expect("encode");
        assert_eq!(pages.len(), 1);
        let (num_values, num_nulls, enc) = v2_header(&pages[0]);
        assert_eq!(num_values, 4);
        assert_eq!(num_nulls, 2);
        assert_eq!(enc, 6);
    }

    // ----- empty and all-nulls edge cases -----

    #[test]
    fn encode_string_delta_empty_yields_zero_pages() {
        let data: Vec<u8> = vec![];
        let offsets: Vec<i64> = vec![];
        let col = build_string_column(&data, &offsets);
        let pt = primitive_type_for(ColumnTypeTag::String);
        let pages = encode_string(&[col], 0, 0, &pt, write_options(), None).expect("encode");
        assert!(pages.is_empty());
    }

    #[test]
    fn encode_binary_delta_empty_yields_zero_pages() {
        let data: Vec<u8> = vec![];
        let offsets: Vec<i64> = vec![];
        let col = build_binary_column(&data, &offsets);
        let pt = primitive_type_for(ColumnTypeTag::Binary);
        let pages = encode_binary(&[col], 0, 0, &pt, write_options(), None).expect("encode");
        assert!(pages.is_empty());
    }

    #[test]
    fn encode_string_delta_all_nulls() {
        let (data, offsets) = make_null_string_aux(5);
        let col = build_string_column(&data, &offsets);
        let pt = primitive_type_for(ColumnTypeTag::String);
        let pages =
            encode_string(&[col], 0, offsets.len(), &pt, write_options(), None).expect("encode");
        assert_eq!(pages.len(), 1);
        let (num_values, num_nulls, _) = v2_header(&pages[0]);
        assert_eq!(num_values, 5);
        assert_eq!(num_nulls, 5);
    }

    #[test]
    fn encode_varchar_delta_all_nulls() {
        let aux = [
            make_varchar_aux_null(),
            make_varchar_aux_null(),
            make_varchar_aux_null(),
        ];
        let data: Vec<u8> = vec![];
        let col = build_varchar_column(&aux, &data);
        let pt = primitive_type_for(ColumnTypeTag::Varchar);
        let pages =
            encode_varchar(&[col], 0, aux.len(), &pt, write_options(), None).expect("encode");
        assert_eq!(pages.len(), 1);
        let (num_values, num_nulls, _) = v2_header(&pages[0]);
        assert_eq!(num_values, 3);
        assert_eq!(num_nulls, 3);
    }

    // ----- column-top -----

    #[test]
    fn encode_string_delta_with_column_top() {
        let (data_buf, offsets) = make_string_aux(&["a", "b"]);
        let col = build_string_column_with_top(&data_buf, &offsets, 5);
        let pt = primitive_type_for(ColumnTypeTag::String);
        let pages = encode_string(&[col], 0, 7, &pt, write_options(), None).expect("encode");
        assert_eq!(pages.len(), 1);
        let (num_values, num_nulls, enc) = v2_header(&pages[0]);
        assert_eq!(num_values, 7);
        assert_eq!(num_nulls, 5, "column top rows should appear as nulls");
        assert_eq!(enc, 6);
    }

    #[test]
    fn encode_binary_delta_with_column_top() {
        let (data_buf, offsets) = make_binary_aux(&[b"abc", b"xy"]);
        let col = build_binary_column_with_top(&data_buf, &offsets, 3);
        let pt = primitive_type_for(ColumnTypeTag::Binary);
        let pages = encode_binary(&[col], 0, 5, &pt, write_options(), None).expect("encode");
        assert_eq!(pages.len(), 1);
        let (num_values, num_nulls, enc) = v2_header(&pages[0]);
        assert_eq!(num_values, 5);
        assert_eq!(num_nulls, 3, "column top rows should appear as nulls");
        assert_eq!(enc, 6);
    }

    // ----- bloom filter -----

    #[test]
    fn encode_string_delta_bloom_filter() {
        let (data_buf, offsets) = make_string_aux(&["alpha", "beta", "gamma"]);
        let col = build_string_column(&data_buf, &offsets);
        let pt = primitive_type_for(ColumnTypeTag::String);
        let bloom = Arc::new(Mutex::new(HashSet::new()));
        let _ = encode_string(
            &[col],
            0,
            offsets.len(),
            &pt,
            write_options(),
            Some(bloom.clone()),
        )
        .expect("encode");
        let set = bloom.lock().expect("lock");
        assert_eq!(set.len(), 3);
    }

    #[test]
    fn encode_binary_delta_bloom_filter() {
        let (data_buf, offsets) = make_binary_aux(&[b"abc", b"def", b"ghi", b"jkl"]);
        let col = build_binary_column(&data_buf, &offsets);
        let pt = primitive_type_for(ColumnTypeTag::Binary);
        let bloom = Arc::new(Mutex::new(HashSet::new()));
        let _ = encode_binary(
            &[col],
            0,
            offsets.len(),
            &pt,
            write_options(),
            Some(bloom.clone()),
        )
        .expect("encode");
        let set = bloom.lock().expect("lock");
        assert_eq!(set.len(), 4);
    }

    // ----- multi-partition with nulls (per-partition pages) -----

    #[test]
    fn encode_binary_delta_multi_partition_with_nulls() {
        // Partition 1: "abc", null
        let mut data1 = Vec::new();
        let mut offsets1 = Vec::new();
        offsets1.push(data1.len() as i64);
        data1.extend_from_slice(&3i64.to_le_bytes());
        data1.extend_from_slice(b"abc");
        offsets1.push(data1.len() as i64);
        data1.extend_from_slice(&(-1i64).to_le_bytes());

        // Partition 2: null, "xy"
        let mut data2 = Vec::new();
        let mut offsets2 = Vec::new();
        offsets2.push(data2.len() as i64);
        data2.extend_from_slice(&(-1i64).to_le_bytes());
        offsets2.push(data2.len() as i64);
        data2.extend_from_slice(&2i64.to_le_bytes());
        data2.extend_from_slice(b"xy");

        let col1 = build_binary_column(&data1, &offsets1);
        let col2 = build_binary_column(&data2, &offsets2);
        let pt = primitive_type_for(ColumnTypeTag::Binary);
        let pages = encode_binary(&[col1, col2], 0, offsets2.len(), &pt, write_options(), None)
            .expect("encode");
        assert_eq!(pages.len(), 2);
        let (nv0, nn0, _) = v2_header(&pages[0]);
        let (nv1, nn1, _) = v2_header(&pages[1]);
        assert_eq!(nv0, 2);
        assert_eq!(nn0, 1);
        assert_eq!(nv1, 2);
        assert_eq!(nn1, 1);
    }

    #[test]
    fn encode_varchar_delta_multi_partition_with_nulls() {
        let aux1 = [make_varchar_aux_inlined(b"a"), make_varchar_aux_null()];
        let aux2 = [make_varchar_aux_null(), make_varchar_aux_inlined(b"z")];
        let data: Vec<u8> = vec![];
        let col1 = build_varchar_column(&aux1, &data);
        let col2 = build_varchar_column(&aux2, &data);
        let pt = primitive_type_for(ColumnTypeTag::Varchar);
        let pages = encode_varchar(&[col1, col2], 0, aux2.len(), &pt, write_options(), None)
            .expect("encode");
        assert_eq!(pages.len(), 2);
        let (nv0, nn0, _) = v2_header(&pages[0]);
        let (nv1, nn1, _) = v2_header(&pages[1]);
        assert_eq!(nv0, 2);
        assert_eq!(nn0, 1);
        assert_eq!(nv1, 2);
        assert_eq!(nn1, 1);
    }
}
