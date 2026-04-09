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

//! Shared helpers used by the per-encoding encoder modules.
//!
//! Bloom-set locking convention: dict-encoding functions lock the bloom mutex
//! once during dict-page assembly and release it before per-partition data-page
//! emission. Plain encoders may lock per-partition since they have no shared
//! state.

use std::cmp;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::schema::Column;

/// Default cap on uncompressed data page size, mirroring the legacy
/// `DEFAULT_PAGE_SIZE` constant in `file.rs` before the rework.
#[allow(dead_code)]
pub const DEFAULT_PAGE_SIZE: usize = 1024 * 1024;

/// Compute the number of rows per data page for a given primitive size.
/// Mirrors the legacy `column_chunk_to_primitive_pages` formula.
#[inline]
#[allow(dead_code)]
pub fn rows_per_page(options: &WriteOptions, bytes_per_row: usize) -> usize {
    let max_page_size = options.data_page_size.unwrap_or(DEFAULT_PAGE_SIZE);
    cmp::max(max_page_size / bytes_per_row, 1)
}

/// Per-partition slice into a column chunk: byte range plus the number of
/// "column top" rows that fall inside the chunk (rows that have no backing
/// storage and read as null).
#[derive(Clone, Copy, Debug)]
pub struct ChunkSlice {
    pub lower_bound: usize,
    pub upper_bound: usize,
    pub adjusted_column_top: usize,
}

#[cfg(test)]
impl ChunkSlice {
    #[inline]
    pub fn len(&self) -> usize {
        self.upper_bound - self.lower_bound
    }

    /// Total number of rows this slice represents (data rows + adjusted top).
    #[inline]
    pub fn num_rows(&self) -> usize {
        self.adjusted_column_top + self.len()
    }
}

/// Compute the byte range and adjusted column top for a chunk that begins at
/// `chunk_offset` (in column rows) and is `chunk_length` rows long, given the
/// column's original column top.
///
/// `column_top` is the number of leading rows that have no backing data.
/// The returned `lower_bound`/`upper_bound` are indices into the data buffer
/// (after subtracting the column top).
#[inline]
pub fn compute_chunk_slice(
    column_top: usize,
    chunk_offset: usize,
    chunk_length: usize,
) -> ChunkSlice {
    let mut adjusted_column_top = 0;
    let lower_bound = if chunk_offset < column_top {
        adjusted_column_top = column_top - chunk_offset;
        0
    } else {
        chunk_offset - column_top
    };
    let upper_bound = if chunk_offset + chunk_length < column_top {
        adjusted_column_top = chunk_length;
        0
    } else {
        chunk_offset + chunk_length - column_top
    };
    ChunkSlice { lower_bound, upper_bound, adjusted_column_top }
}

/// Compute `(chunk_offset, chunk_length)` for the i-th partition in a
/// multi-partition row group write.
///
/// Single-partition (`num_partitions == 1`) uses the absolute bounds.
/// First partition starts at `first_partition_start` and runs to its end.
/// Last partition starts at 0 and ends at `last_partition_end`.
/// Middle partitions are taken in full.
#[inline]
pub fn partition_slice_range(
    part_idx: usize,
    num_partitions: usize,
    row_count: usize,
    first_partition_start: usize,
    last_partition_end: usize,
) -> (usize, usize) {
    if num_partitions == 1 {
        (
            first_partition_start,
            last_partition_end - first_partition_start,
        )
    } else if part_idx == 0 {
        (first_partition_start, row_count - first_partition_start)
    } else if part_idx == num_partitions - 1 {
        (0, last_partition_end)
    } else {
        (0, row_count)
    }
}

/// Compute the per-partition `ChunkSlice` for the i-th column in a row group.
#[inline]
pub fn partition_chunk_slice(
    part_idx: usize,
    num_partitions: usize,
    column: &Column,
    first_partition_start: usize,
    last_partition_end: usize,
) -> ChunkSlice {
    let (chunk_offset, chunk_length) = partition_slice_range(
        part_idx,
        num_partitions,
        column.row_count,
        first_partition_start,
        last_partition_end,
    );
    compute_chunk_slice(column.column_top, chunk_offset, chunk_length)
}

/// Total number of logical rows covered by the selected column chunk across
/// all input partitions.
#[inline]
pub fn column_chunk_row_count(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
) -> usize {
    let num_partitions = columns.len();
    columns
        .iter()
        .enumerate()
        .map(|(part_idx, column)| {
            partition_slice_range(
                part_idx,
                num_partitions,
                column.row_count,
                first_partition_start,
                last_partition_end,
            )
            .1
        })
        .sum()
}

/// Compute the logical chunk slice for each input partition.
pub fn column_chunk_slices<'a>(
    columns: &'a [Column],
    first_partition_start: usize,
    last_partition_end: usize,
) -> impl Iterator<Item = ChunkSlice> + 'a {
    let num_partitions = columns.len();
    columns.iter().enumerate().map(move |(part_idx, column)| {
        partition_chunk_slice(
            part_idx,
            num_partitions,
            column,
            first_partition_start,
            last_partition_end,
        )
    })
}

/// Borrowed typed view of a logical chunk segment from a single partition.
#[derive(Clone, Copy, Debug)]
pub struct TypedChunkSegment<'a, T> {
    pub adjusted_column_top: usize,
    pub slice: &'a [T],
}

impl<T> TypedChunkSegment<'_, T> {
    #[inline]
    pub fn num_rows(&self) -> usize {
        self.adjusted_column_top + self.slice.len()
    }
}

/// Build borrowed typed segments for the selected column chunk without
/// materializing a whole-chunk value buffer.
pub fn collect_typed_chunk_segments<'a, T, F>(
    columns: &'a [Column],
    first_partition_start: usize,
    last_partition_end: usize,
    mut transmuter: F,
) -> ParquetResult<Vec<TypedChunkSegment<'a, T>>>
where
    F: FnMut(&'a Column) -> ParquetResult<&'a [T]>,
{
    let mut segments = Vec::with_capacity(columns.len());

    for (column, chunk) in columns.iter().zip(column_chunk_slices(
        columns,
        first_partition_start,
        last_partition_end,
    )) {
        let typed = transmuter(column)?;
        segments.push(TypedChunkSegment {
            adjusted_column_top: chunk.adjusted_column_top,
            slice: &typed[chunk.lower_bound..chunk.upper_bound],
        });
    }

    Ok(segments)
}

/// Iterator over sub-chunks of a partition that respects `rows_per_page`.
/// Each yielded `ChunkSlice` covers at most `rows_per_page` rows.
#[derive(Clone)]
pub struct PartitionPageSlices {
    column_top: usize,
    chunk_offset: usize,
    chunk_length: usize,
    rows_per_page: usize,
    sub_offset: usize,
}

impl PartitionPageSlices {
    #[allow(dead_code)]
    pub fn new(
        column: &Column,
        chunk_offset: usize,
        chunk_length: usize,
        rows_per_page: usize,
    ) -> Self {
        Self::from_parts(column.column_top, chunk_offset, chunk_length, rows_per_page)
    }

    /// Construct directly from a `column_top` value, without holding a `Column`.
    /// Mostly used by tests; production callers prefer `new(column, ...)`.
    pub fn from_parts(
        column_top: usize,
        chunk_offset: usize,
        chunk_length: usize,
        rows_per_page: usize,
    ) -> Self {
        Self {
            column_top,
            chunk_offset,
            chunk_length,
            rows_per_page,
            sub_offset: 0,
        }
    }
}

impl Iterator for PartitionPageSlices {
    type Item = ChunkSlice;

    fn next(&mut self) -> Option<Self::Item> {
        if self.sub_offset >= self.chunk_length {
            return None;
        }
        let sub_length = cmp::min(self.rows_per_page, self.chunk_length - self.sub_offset);
        let slice = compute_chunk_slice(
            self.column_top,
            self.chunk_offset + self.sub_offset,
            sub_length,
        );
        self.sub_offset += sub_length;
        Some(slice)
    }
}

/// Lock the bloom set mutex if present, returning a guard that lives until the
/// caller drops it. Returns `None` if no bloom set was provided.
pub fn lock_bloom_set(
    bloom_set: Option<&Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Option<std::sync::MutexGuard<'_, HashSet<u64>>>> {
    bloom_set
        .map(|arc| {
            arc.lock()
                .map_err(|_| fmt_err!(Layout, "bloom filter mutex poisoned"))
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_chunk_slice_no_top() {
        let cs = compute_chunk_slice(0, 0, 10);
        assert_eq!(cs.lower_bound, 0);
        assert_eq!(cs.upper_bound, 10);
        assert_eq!(cs.adjusted_column_top, 0);
        assert_eq!(cs.num_rows(), 10);
    }

    #[test]
    fn compute_chunk_slice_top_partial() {
        // 3 leading null rows, chunk starts at row 1, length 5 -> two top rows
        // are inside the chunk, the rest (3 rows) come from data[0..3].
        let cs = compute_chunk_slice(3, 1, 5);
        assert_eq!(cs.adjusted_column_top, 2);
        assert_eq!(cs.lower_bound, 0);
        assert_eq!(cs.upper_bound, 3);
        assert_eq!(cs.num_rows(), 5);
    }

    #[test]
    fn compute_chunk_slice_top_entirely_inside_chunk() {
        // Chunk fully inside the column top region.
        let cs = compute_chunk_slice(10, 2, 5);
        assert_eq!(cs.adjusted_column_top, 5);
        assert_eq!(cs.lower_bound, 0);
        assert_eq!(cs.upper_bound, 0);
        assert_eq!(cs.num_rows(), 5);
    }

    #[test]
    fn compute_chunk_slice_no_top_overlap() {
        // Chunk past the column top region entirely.
        let cs = compute_chunk_slice(2, 5, 7);
        assert_eq!(cs.adjusted_column_top, 0);
        assert_eq!(cs.lower_bound, 3);
        assert_eq!(cs.upper_bound, 10);
        assert_eq!(cs.num_rows(), 7);
    }

    #[test]
    fn partition_slice_single_partition() {
        let (offset, length) = partition_slice_range(0, 1, 100, 10, 70);
        assert_eq!(offset, 10);
        assert_eq!(length, 60);
    }

    #[test]
    fn partition_slice_multi_first() {
        let (offset, length) = partition_slice_range(0, 3, 100, 25, 50);
        assert_eq!(offset, 25);
        assert_eq!(length, 75);
    }

    #[test]
    fn partition_slice_multi_middle() {
        let (offset, length) = partition_slice_range(1, 3, 100, 25, 50);
        assert_eq!(offset, 0);
        assert_eq!(length, 100);
    }

    #[test]
    fn partition_slice_multi_last() {
        let (offset, length) = partition_slice_range(2, 3, 100, 25, 50);
        assert_eq!(offset, 0);
        assert_eq!(length, 50);
    }

    #[test]
    fn partition_page_slices_single_chunk() {
        // 50 rows fits comfortably in one page of 100 rows.
        let mut iter = PartitionPageSlices::from_parts(0, 0, 50, 100);
        let first = iter.next().expect("first slice");
        assert_eq!(first.lower_bound, 0);
        assert_eq!(first.upper_bound, 50);
        assert_eq!(first.adjusted_column_top, 0);
        assert!(iter.next().is_none());
    }

    #[test]
    fn partition_page_slices_exact_multiple() {
        // 100 rows / 25 per page = 4 equal sub-pages, no remainder.
        let iter = PartitionPageSlices::from_parts(0, 0, 100, 25);
        let slices: Vec<_> = iter.collect();
        assert_eq!(slices.len(), 4);
        for (i, slice) in slices.iter().enumerate() {
            assert_eq!(slice.lower_bound, i * 25);
            assert_eq!(slice.upper_bound, (i + 1) * 25);
            assert_eq!(slice.adjusted_column_top, 0);
        }
    }

    #[test]
    fn partition_page_slices_with_remainder() {
        // 95 rows / 25 per page = 3 full pages of 25 + 1 page of 20.
        let iter = PartitionPageSlices::from_parts(0, 0, 95, 25);
        let slices: Vec<_> = iter.collect();
        assert_eq!(slices.len(), 4);
        assert_eq!(slices[3].len(), 20);
        let total: usize = slices.iter().map(|s| s.len()).sum();
        assert_eq!(total, 95);
    }

    #[test]
    fn partition_page_slices_with_column_top() {
        // column_top = 10 (rows 0..10 are null), 30 data rows -> 40 total rows.
        // 15 rows per page -> 3 pages.
        // Page 1 [rows 0..15]: 10 top + 5 data.
        // Page 2 [rows 15..30]: 0 top + 15 data.
        // Page 3 [rows 30..40]: 0 top + 10 data.
        let iter = PartitionPageSlices::from_parts(10, 0, 40, 15);
        let slices: Vec<_> = iter.collect();
        assert_eq!(slices.len(), 3);

        assert_eq!(slices[0].adjusted_column_top, 10);
        assert_eq!(slices[0].len(), 5);
        assert_eq!(slices[0].num_rows(), 15);

        assert_eq!(slices[1].adjusted_column_top, 0);
        assert_eq!(slices[1].len(), 15);
        assert_eq!(slices[1].num_rows(), 15);

        assert_eq!(slices[2].adjusted_column_top, 0);
        assert_eq!(slices[2].len(), 10);
        assert_eq!(slices[2].num_rows(), 10);

        let total_rows: usize = slices.iter().map(|s| s.num_rows()).sum();
        assert_eq!(total_rows, 40);
        let total_data: usize = slices.iter().map(|s| s.len()).sum();
        assert_eq!(total_data, 30);
        let total_top: usize = slices.iter().map(|s| s.adjusted_column_top).sum();
        assert_eq!(total_top, 10);
    }

    #[test]
    fn partition_page_slices_empty_partition() {
        // Zero-length chunk yields zero slices.
        let mut iter = PartitionPageSlices::from_parts(0, 0, 0, 100);
        assert!(iter.next().is_none());
    }

    #[test]
    fn rows_per_page_respects_data_page_size() {
        let mut opts = WriteOptions {
            write_statistics: true,
            version: parquet2::write::Version::V2,
            compression: parquet2::compression::CompressionOptions::Uncompressed,
            row_group_size: None,
            data_page_size: Some(256),
            raw_array_encoding: false,
            bloom_filter_fpp: 0.01,
            min_compression_ratio: 0.0,
        };
        // 256 bytes / 8 bytes per row = 32 rows per page.
        assert_eq!(rows_per_page(&opts, 8), 32);
        // 256 bytes / 4 bytes per row = 64 rows per page.
        assert_eq!(rows_per_page(&opts, 4), 64);

        // None falls back to DEFAULT_PAGE_SIZE (1 MiB).
        opts.data_page_size = None;
        assert_eq!(rows_per_page(&opts, 8), DEFAULT_PAGE_SIZE / 8);
    }

    #[test]
    fn rows_per_page_lower_bound() {
        // bytes_per_row > data_page_size returns 1 (never 0).
        let opts = WriteOptions {
            write_statistics: true,
            version: parquet2::write::Version::V2,
            compression: parquet2::compression::CompressionOptions::Uncompressed,
            row_group_size: None,
            data_page_size: Some(8),
            raw_array_encoding: false,
            bloom_filter_fpp: 0.01,
            min_compression_ratio: 0.0,
        };
        assert_eq!(rows_per_page(&opts, 32), 1);
        assert_eq!(rows_per_page(&opts, 1024), 1);
    }
}
