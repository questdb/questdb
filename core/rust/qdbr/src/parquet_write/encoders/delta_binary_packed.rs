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

//! Delta-binary-packed encoders for integer families.

use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use parquet2::encoding::Encoding;
use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;
use parquet2::types::NativeType;

use crate::parquet::error::ParquetResult;
use crate::parquet_write::encoders::helpers::{
    lock_bloom_set, partition_slice_range, rows_per_primitive_page, ChunkSlice, PartitionPageSlices,
};
use crate::parquet_write::encoders::numeric::{self, SimdEncodable, StatsUpdater};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::schema::Column;
use crate::parquet_write::util::{transmute_slice, MaxMin};
use crate::parquet_write::Nullable;

/// Encode SIMD-encodable integer types (Int, Long, Date, Timestamp) as
/// DeltaBinaryPacked pages.
pub fn encode_simd<T>(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>>
where
    T: SimdEncodable,
{
    let rpp = rows_per_primitive_page(&options, primitive_type.physical_type);
    encode_per_partition(
        columns,
        first_partition_start,
        last_partition_end,
        rpp,
        bloom_set,
        |column, chunk, bloom| {
            // SAFETY: Data originates from JNI/Java memory-mapped column data, which is
            // page-aligned. The byte content represents valid `T` values.
            let data: &[T] = unsafe { transmute_slice(column.primary_data) };
            let slice = &data[chunk.lower_bound..chunk.upper_bound];
            numeric::slice_to_page_simd::<T>(
                slice,
                chunk.adjusted_column_top,
                options,
                primitive_type.clone(),
                Encoding::DeltaBinaryPacked,
                bloom,
            )
        },
    )
}

/// Encode notnull integer (Byte, Short, Char) as DeltaBinaryPacked pages.
pub fn encode_int_notnull<T, P>(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>>
where
    P: NativeType + num_traits::AsPrimitive<i64>,
    T: Default + num_traits::AsPrimitive<P> + Debug,
{
    let rpp = rows_per_primitive_page(&options, primitive_type.physical_type);
    encode_per_partition(
        columns,
        first_partition_start,
        last_partition_end,
        rpp,
        bloom_set,
        |column, chunk, bloom| {
            // SAFETY: Data originates from JNI/Java memory-mapped column data, which is
            // page-aligned. The byte content represents valid `T` values.
            let data: &[T] = unsafe { transmute_slice(column.primary_data) };
            let slice = &data[chunk.lower_bound..chunk.upper_bound];
            numeric::int_slice_to_page_notnull::<T, P>(
                slice,
                chunk.adjusted_column_top,
                options,
                primitive_type.clone(),
                Encoding::DeltaBinaryPacked,
                bloom,
            )
        },
    )
}

/// Encode nullable integer (IPv4, GeoByte/Short/Int/Long) as DeltaBinaryPacked
/// pages.
pub fn encode_int_nullable<T, P, const UNSIGNED_STATS: bool>(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>>
where
    P: NativeType + num_traits::AsPrimitive<i64>,
    T: Nullable + num_traits::AsPrimitive<P> + Debug,
    MaxMin<P>: StatsUpdater<P, UNSIGNED_STATS>,
{
    let rpp = rows_per_primitive_page(&options, primitive_type.physical_type);
    encode_per_partition(
        columns,
        first_partition_start,
        last_partition_end,
        rpp,
        bloom_set,
        |column, chunk, bloom| {
            // SAFETY: Data originates from JNI/Java memory-mapped column data, which is
            // page-aligned. The byte content represents valid `T` values.
            let data: &[T] = unsafe { transmute_slice(column.primary_data) };
            let slice = &data[chunk.lower_bound..chunk.upper_bound];
            numeric::int_slice_to_page_nullable::<T, P, UNSIGNED_STATS>(
                slice,
                chunk.adjusted_column_top,
                options,
                primitive_type.clone(),
                Encoding::DeltaBinaryPacked,
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
    use crate::parquet_write::schema::column_type_to_parquet_type;
    use crate::parquet_write::tests::make_column_with_top;
    use parquet2::compression::CompressionOptions;
    use parquet2::page::DataPageHeader;
    use parquet2::schema::types::ParquetType;
    use parquet2::statistics::PrimitiveStatistics;
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

    // ----- encode_simd: Int / Long -----

    #[test]
    fn encode_simd_int_delta_round_trip() {
        let data: Vec<i32> = (0..100i32).collect();
        let col = make_column_with_top("col", ColumnTypeTag::Int, &data, 0, 0);
        let pt = primitive_type_for(ColumnTypeTag::Int);
        let pages =
            encode_simd::<i32>(&[col], 0, data.len(), &pt, write_options(), None).expect("encode");
        assert_eq!(pages.len(), 1);
        let (num_values, num_nulls, enc) = v2_header(&pages[0]);
        assert_eq!(num_values, 100);
        assert_eq!(num_nulls, 0);
        // DELTA_BINARY_PACKED = 5
        assert_eq!(enc, 5);
    }

    #[test]
    fn encode_simd_long_delta_round_trip() {
        let data: Vec<i64> = (0..100i64).collect();
        let col = make_column_with_top("col", ColumnTypeTag::Long, &data, 0, 0);
        let pt = primitive_type_for(ColumnTypeTag::Long);
        let pages =
            encode_simd::<i64>(&[col], 0, data.len(), &pt, write_options(), None).expect("encode");
        assert_eq!(pages.len(), 1);
        let (_, _, enc) = v2_header(&pages[0]);
        assert_eq!(enc, 5);
    }

    #[test]
    fn encode_simd_long_descending_sequence() {
        // Strictly descending values exercise the negative-delta path.
        let data: Vec<i64> = (0..100i64).rev().collect();
        let col = make_column_with_top("col", ColumnTypeTag::Long, &data, 0, 0);
        let pt = primitive_type_for(ColumnTypeTag::Long);
        let pages =
            encode_simd::<i64>(&[col], 0, data.len(), &pt, write_options(), None).expect("encode");
        assert_eq!(pages.len(), 1);
        let (num_values, _, _) = v2_header(&pages[0]);
        assert_eq!(num_values, 100);
    }

    #[test]
    fn encode_simd_multi_partition_delta_independent_pages() {
        // 4 partitions × 25 rows i64 → 4 separate delta pages.
        let parts: Vec<Vec<i64>> = (0..4)
            .map(|p| ((p * 25)..(p * 25 + 25)).collect())
            .collect();
        let columns: Vec<Column> = parts
            .iter()
            .map(|d| make_column_with_top("col", ColumnTypeTag::Long, d, 0, 0))
            .collect();
        let pt = primitive_type_for(ColumnTypeTag::Long);
        let pages =
            encode_simd::<i64>(&columns, 0, 25, &pt, write_options(), None).expect("encode");
        assert_eq!(pages.len(), 4);
        for page in &pages {
            let (num_values, _, enc) = v2_header(page);
            assert_eq!(num_values, 25);
            assert_eq!(enc, 5);
        }
    }

    // ----- encode_int_notnull: Byte / Short / Char (currently uncovered) -----

    #[test]
    fn encode_int_notnull_byte_delta() {
        let data: Vec<i8> = (-50..50i8).collect();
        let col = make_column_with_top("col", ColumnTypeTag::Byte, &data, 0, 0);
        let pt = primitive_type_for(ColumnTypeTag::Byte);
        let pages =
            encode_int_notnull::<i8, i32>(&[col], 0, data.len(), &pt, write_options(), None)
                .expect("encode");
        assert_eq!(pages.len(), 1);
        let (num_values, num_nulls, enc) = v2_header(&pages[0]);
        assert_eq!(num_values, 100);
        assert_eq!(num_nulls, 0);
        assert_eq!(enc, 5);
    }

    #[test]
    fn encode_int_notnull_short_delta() {
        let data: Vec<i16> = (0..100i16).collect();
        let col = make_column_with_top("col", ColumnTypeTag::Short, &data, 0, 0);
        let pt = primitive_type_for(ColumnTypeTag::Short);
        let pages =
            encode_int_notnull::<i16, i32>(&[col], 0, data.len(), &pt, write_options(), None)
                .expect("encode");
        assert_eq!(pages.len(), 1);
        let (num_values, _, enc) = v2_header(&pages[0]);
        assert_eq!(num_values, 100);
        assert_eq!(enc, 5);
    }

    #[test]
    fn encode_int_notnull_char_delta() {
        let data: Vec<u16> = (0..100u16).collect();
        let col = make_column_with_top("col", ColumnTypeTag::Char, &data, 0, 0);
        let pt = primitive_type_for(ColumnTypeTag::Char);
        let pages =
            encode_int_notnull::<u16, i32>(&[col], 0, data.len(), &pt, write_options(), None)
                .expect("encode");
        assert_eq!(pages.len(), 1);
        let (num_values, _, enc) = v2_header(&pages[0]);
        assert_eq!(num_values, 100);
        assert_eq!(enc, 5);
    }

    // ----- encode_int_nullable: IPv4 / Geo* (currently uncovered) -----

    #[test]
    fn encode_int_nullable_ipv4_delta() {
        let data: Vec<i32> = vec![1, 2, 3, 0 /* null */, 4, 5];
        let col = make_column_with_top("col", ColumnTypeTag::IPv4, &data, 0, 0);
        let pt = primitive_type_for(ColumnTypeTag::IPv4);
        let pages = encode_int_nullable::<crate::parquet_write::IPv4, i32, true>(
            &[col],
            0,
            data.len(),
            &pt,
            write_options(),
            None,
        )
        .expect("encode");
        assert_eq!(pages.len(), 1);
        let (num_values, num_nulls, enc) = v2_header(&pages[0]);
        assert_eq!(num_values, 6);
        assert_eq!(num_nulls, 1);
        assert_eq!(enc, 5);
    }

    #[test]
    fn encode_int_nullable_geobyte_delta() {
        let data: Vec<i8> = vec![1, 2, -1 /* null */, 3];
        let col = make_column_with_top("col", ColumnTypeTag::GeoByte, &data, 0, 0);
        let pt = primitive_type_for(ColumnTypeTag::GeoByte);
        let pages = encode_int_nullable::<crate::parquet_write::GeoByte, i32, false>(
            &[col],
            0,
            data.len(),
            &pt,
            write_options(),
            None,
        )
        .expect("encode");
        let (num_values, num_nulls, _) = v2_header(&pages[0]);
        assert_eq!(num_values, 4);
        assert_eq!(num_nulls, 1);
    }

    #[test]
    fn encode_int_nullable_geoshort_delta() {
        let data: Vec<i16> = vec![100, 200, -1, 300];
        let col = make_column_with_top("col", ColumnTypeTag::GeoShort, &data, 0, 0);
        let pt = primitive_type_for(ColumnTypeTag::GeoShort);
        let pages = encode_int_nullable::<crate::parquet_write::GeoShort, i32, false>(
            &[col],
            0,
            data.len(),
            &pt,
            write_options(),
            None,
        )
        .expect("encode");
        let (num_values, num_nulls, _) = v2_header(&pages[0]);
        assert_eq!(num_values, 4);
        assert_eq!(num_nulls, 1);
    }

    #[test]
    fn encode_int_nullable_geoint_delta() {
        let data: Vec<i32> = vec![1000, 2000, -1, 3000, 4000];
        let col = make_column_with_top("col", ColumnTypeTag::GeoInt, &data, 0, 0);
        let pt = primitive_type_for(ColumnTypeTag::GeoInt);
        let pages = encode_int_nullable::<crate::parquet_write::GeoInt, i32, false>(
            &[col],
            0,
            data.len(),
            &pt,
            write_options(),
            None,
        )
        .expect("encode");
        let (num_values, num_nulls, _) = v2_header(&pages[0]);
        assert_eq!(num_values, 5);
        assert_eq!(num_nulls, 1);
    }

    #[test]
    fn encode_int_nullable_geolong_delta() {
        let data: Vec<i64> = vec![10_000, 20_000, -1, 30_000];
        let col = make_column_with_top("col", ColumnTypeTag::GeoLong, &data, 0, 0);
        let pt = primitive_type_for(ColumnTypeTag::GeoLong);
        let pages = encode_int_nullable::<crate::parquet_write::GeoLong, i64, false>(
            &[col],
            0,
            data.len(),
            &pt,
            write_options(),
            None,
        )
        .expect("encode");
        let (num_values, num_nulls, _) = v2_header(&pages[0]);
        assert_eq!(num_values, 4);
        assert_eq!(num_nulls, 1);
    }

    // ----- column-top, empty, all-nulls edge cases -----

    #[test]
    fn encode_simd_int_with_column_top() {
        let data: Vec<i32> = (0..100).collect();
        let col = make_column_with_top("col", ColumnTypeTag::Int, &data, 50, 0);
        let pt = primitive_type_for(ColumnTypeTag::Int);
        let pages = encode_simd::<i32>(&[col], 0, 150, &pt, write_options(), None).expect("encode");
        assert_eq!(pages.len(), 1);
        let (num_values, num_nulls, enc) = v2_header(&pages[0]);
        assert_eq!(num_values, 150);
        assert_eq!(num_nulls, 50, "column top rows should appear as nulls");
        assert_eq!(enc, 5);
    }

    #[test]
    fn encode_simd_empty_partition_yields_zero_pages() {
        let data: Vec<i32> = vec![];
        let col = make_column_with_top("col", ColumnTypeTag::Int, &data, 0, 0);
        let pt = primitive_type_for(ColumnTypeTag::Int);
        let pages = encode_simd::<i32>(&[col], 0, 0, &pt, write_options(), None).expect("encode");
        assert!(pages.is_empty());
    }

    #[test]
    fn encode_simd_all_nulls_partition() {
        let data: Vec<i32> = vec![i32::MIN; 50];
        let col = make_column_with_top("col", ColumnTypeTag::Int, &data, 0, 0);
        let pt = primitive_type_for(ColumnTypeTag::Int);
        let pages = encode_simd::<i32>(&[col], 0, 50, &pt, write_options(), None).expect("encode");
        assert_eq!(pages.len(), 1);
        let (num_values, num_nulls, _) = v2_header(&pages[0]);
        assert_eq!(num_values, 50);
        assert_eq!(num_nulls, 50);
    }

    #[test]
    fn encode_simd_single_value_repeated() {
        let data: Vec<i32> = vec![42; 100];
        let col = make_column_with_top("col", ColumnTypeTag::Int, &data, 0, 0);
        let pt = primitive_type_for(ColumnTypeTag::Int);
        let pages =
            encode_simd::<i32>(&[col], 0, data.len(), &pt, write_options(), None).expect("encode");
        assert_eq!(pages.len(), 1);
        let (num_values, num_nulls, _) = v2_header(&pages[0]);
        assert_eq!(num_values, 100);
        assert_eq!(num_nulls, 0);
    }

    // ----- statistics verification -----

    fn page_i32_min_max(page: &Page) -> (i32, i32) {
        match page {
            Page::Data(d) => {
                let arc = d.statistics().expect("statistics present").expect("ok");
                let stats = arc
                    .as_any()
                    .downcast_ref::<PrimitiveStatistics<i32>>()
                    .expect("PrimitiveStatistics<i32>");
                (stats.min_value.unwrap(), stats.max_value.unwrap())
            }
            _ => panic!("expected data page"),
        }
    }

    fn page_i64_min_max(page: &Page) -> (i64, i64) {
        match page {
            Page::Data(d) => {
                let arc = d.statistics().expect("statistics present").expect("ok");
                let stats = arc
                    .as_any()
                    .downcast_ref::<PrimitiveStatistics<i64>>()
                    .expect("PrimitiveStatistics<i64>");
                (stats.min_value.unwrap(), stats.max_value.unwrap())
            }
            _ => panic!("expected data page"),
        }
    }

    #[test]
    fn encode_simd_int_page_stats() {
        let data: Vec<i32> = vec![500, 100, 300, 200, 400];
        let col = make_column_with_top("col", ColumnTypeTag::Int, &data, 0, 0);
        let pt = primitive_type_for(ColumnTypeTag::Int);
        let pages =
            encode_simd::<i32>(&[col], 0, data.len(), &pt, write_options(), None).expect("encode");
        assert_eq!(pages.len(), 1);
        let (min, max) = page_i32_min_max(&pages[0]);
        assert_eq!((min, max), (100, 500));
    }

    #[test]
    fn encode_simd_long_page_stats() {
        let data: Vec<i64> = vec![500, 100, 300, 200, 400];
        let col = make_column_with_top("col", ColumnTypeTag::Long, &data, 0, 0);
        let pt = primitive_type_for(ColumnTypeTag::Long);
        let pages =
            encode_simd::<i64>(&[col], 0, data.len(), &pt, write_options(), None).expect("encode");
        let (min, max) = page_i64_min_max(&pages[0]);
        assert_eq!((min, max), (100, 500));
    }

    // ----- bloom filter -----

    #[test]
    fn encode_simd_int_bloom_hashes() {
        let data: Vec<i32> = vec![10, 20, 30, 40, 50, 60];
        let col = make_column_with_top("col", ColumnTypeTag::Int, &data, 0, 0);
        let pt = primitive_type_for(ColumnTypeTag::Int);
        let bloom = Arc::new(Mutex::new(HashSet::new()));
        let _ = encode_simd::<i32>(
            &[col],
            0,
            data.len(),
            &pt,
            write_options(),
            Some(bloom.clone()),
        )
        .expect("encode");
        let set = bloom.lock().expect("lock");
        assert_eq!(set.len(), 6);
    }

    // ----- multi-partition null counts (per-partition pages) -----

    #[test]
    fn encode_simd_multi_partition_null_counts() {
        // Each partition gets its own page; verify null counts per page.
        let parts: Vec<Vec<i32>> = vec![
            vec![1, 2, 3, 4, 5],                      // 0 nulls
            vec![1, i32::MIN, 2, i32::MIN, 3],        // 2 nulls
            vec![i32::MIN, i32::MIN, i32::MIN, 1, 2], // 3 nulls
        ];
        let columns: Vec<Column> = parts
            .iter()
            .map(|d| make_column_with_top("col", ColumnTypeTag::Int, d, 0, 0))
            .collect();
        let pt = primitive_type_for(ColumnTypeTag::Int);
        let pages = encode_simd::<i32>(&columns, 0, 5, &pt, write_options(), None).expect("encode");
        assert_eq!(pages.len(), 3);
        let (_, nulls0, _) = v2_header(&pages[0]);
        let (_, nulls1, _) = v2_header(&pages[1]);
        let (_, nulls2, _) = v2_header(&pages[2]);
        assert_eq!(nulls0, 0);
        assert_eq!(nulls1, 2);
        assert_eq!(nulls2, 3);
    }

    // ----- notnull: column-top, multi-partition -----

    #[test]
    fn encode_int_notnull_byte_with_column_top() {
        let data: Vec<i8> = vec![5, 6, 7];
        let col = make_column_with_top("col", ColumnTypeTag::Byte, &data, 4, 0);
        let pt = primitive_type_for(ColumnTypeTag::Byte);
        let pages = encode_int_notnull::<i8, i32>(&[col], 0, 7, &pt, write_options(), None)
            .expect("encode");
        assert_eq!(pages.len(), 1);
        let (num_values, num_nulls, enc) = v2_header(&pages[0]);
        assert_eq!(num_values, 7);
        // The notnull delta encoder fills column-top rows with default values
        // but still reports column_top as null_count in the page header.
        assert_eq!(num_nulls, 4);
        assert_eq!(enc, 5);
    }

    #[test]
    fn encode_int_notnull_short_multi_partition() {
        let parts: Vec<Vec<i16>> = vec![vec![10, 20, 30], vec![40, 50, 60]];
        let columns: Vec<Column> = parts
            .iter()
            .map(|d| make_column_with_top("col", ColumnTypeTag::Short, d, 0, 0))
            .collect();
        let pt = primitive_type_for(ColumnTypeTag::Short);
        let pages = encode_int_notnull::<i16, i32>(&columns, 0, 3, &pt, write_options(), None)
            .expect("encode");
        assert_eq!(pages.len(), 2);
        for page in &pages {
            let (num_values, num_nulls, enc) = v2_header(page);
            assert_eq!(num_values, 3);
            assert_eq!(num_nulls, 0);
            assert_eq!(enc, 5);
        }
    }

    // ----- nullable: all-nulls, column-top, multi-partition -----

    #[test]
    fn encode_int_nullable_ipv4_all_nulls() {
        let data: Vec<i32> = vec![0; 10]; // IPv4 null sentinel is 0
        let col = make_column_with_top("col", ColumnTypeTag::IPv4, &data, 0, 0);
        let pt = primitive_type_for(ColumnTypeTag::IPv4);
        let pages = encode_int_nullable::<crate::parquet_write::IPv4, i32, true>(
            &[col],
            0,
            data.len(),
            &pt,
            write_options(),
            None,
        )
        .expect("encode");
        assert_eq!(pages.len(), 1);
        let (num_values, num_nulls, _) = v2_header(&pages[0]);
        assert_eq!(num_values, 10);
        assert_eq!(num_nulls, 10);
    }

    #[test]
    fn encode_int_nullable_ipv4_with_column_top() {
        let data: Vec<i32> = vec![1, 2, 3];
        let col = make_column_with_top("col", ColumnTypeTag::IPv4, &data, 5, 0);
        let pt = primitive_type_for(ColumnTypeTag::IPv4);
        let pages = encode_int_nullable::<crate::parquet_write::IPv4, i32, true>(
            &[col],
            0,
            8, // 5 column-top + 3 data
            &pt,
            write_options(),
            None,
        )
        .expect("encode");
        assert_eq!(pages.len(), 1);
        let (num_values, num_nulls, _) = v2_header(&pages[0]);
        assert_eq!(num_values, 8);
        assert_eq!(num_nulls, 5, "column top rows should appear as nulls");
    }

    #[test]
    fn encode_int_nullable_multi_partition_null_counts() {
        // Each partition gets its own page.
        let parts: Vec<Vec<i32>> = vec![
            vec![1, 0, 3],    // 1 null (0 is IPv4 null)
            vec![0, 0, 0],    // 3 nulls
            vec![10, 20, 30], // 0 nulls
        ];
        let columns: Vec<Column> = parts
            .iter()
            .map(|d| make_column_with_top("col", ColumnTypeTag::IPv4, d, 0, 0))
            .collect();
        let pt = primitive_type_for(ColumnTypeTag::IPv4);
        let pages = encode_int_nullable::<crate::parquet_write::IPv4, i32, true>(
            &columns,
            0,
            3,
            &pt,
            write_options(),
            None,
        )
        .expect("encode");
        assert_eq!(pages.len(), 3);
        let (_, nulls0, _) = v2_header(&pages[0]);
        let (_, nulls1, _) = v2_header(&pages[1]);
        let (_, nulls2, _) = v2_header(&pages[2]);
        assert_eq!(nulls0, 1);
        assert_eq!(nulls1, 3);
        assert_eq!(nulls2, 0);
    }

    #[test]
    fn encode_int_nullable_geobyte_bloom_filter() {
        let data: Vec<i8> = vec![10, 20, -1, 30];
        let col = make_column_with_top("col", ColumnTypeTag::GeoByte, &data, 0, 0);
        let pt = primitive_type_for(ColumnTypeTag::GeoByte);
        let bloom = Arc::new(Mutex::new(HashSet::new()));
        let _ = encode_int_nullable::<crate::parquet_write::GeoByte, i32, false>(
            &[col],
            0,
            data.len(),
            &pt,
            write_options(),
            Some(bloom.clone()),
        )
        .expect("encode");
        let set = bloom.lock().expect("lock");
        // 3 non-null values should produce 3 bloom hashes
        assert_eq!(set.len(), 3);
    }
}
