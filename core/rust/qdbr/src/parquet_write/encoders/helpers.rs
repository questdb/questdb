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

//! Shared encoder helpers organized by responsibility. Keep this module thin:
//! it re-exports the focused helper submodules so call sites can continue to
//! import from `encoders::helpers`.

mod bloom;
mod chunking;
mod page_sizing;
mod utf8;
mod validity;

pub use bloom::lock_bloom_set;
pub use chunking::{
    collect_varlen_segments, column_chunk_row_count, page_chunk_views, page_row_windows,
    partition_chunk_slice, partition_slice_range, slice_varlen_segments, ChunkSlice, PageRowWindow,
    PartitionChunkView, PartitionPageSlices, VarlenChunkSegment,
};
pub use page_sizing::{rows_per_group_page, rows_per_primitive_page};
pub use utf8::write_utf8_from_utf16_iter;
pub use validity::FlatValidity;

#[cfg(test)]
mod tests {
    use super::chunking::compute_chunk_slice;
    use super::page_sizing::{rows_per_page, DEFAULT_PAGE_SIZE};
    use super::*;
    use crate::parquet_write::file::WriteOptions;
    use parquet2::encoding::bitpacked;
    use parquet2::encoding::hybrid_rle::{Decoder, HybridEncoded};
    use parquet2::write::Version;

    fn decode_def_levels(buffer: &[u8], num_rows: usize, version: Version) -> Vec<u8> {
        let data = match version {
            Version::V1 => &buffer[4..],
            Version::V2 => buffer,
        };
        let decoder = Decoder::new(data, 1);
        let mut result = Vec::new();
        for run in decoder {
            match run.unwrap() {
                HybridEncoded::Bitpacked(values) => {
                    let remaining = num_rows - result.len();
                    result.extend(bitpacked::Decoder::<u8>::try_new(values, 1, remaining).unwrap());
                }
                HybridEncoded::Rle(value, count) => {
                    let bit = value[0] & 1;
                    result.extend(std::iter::repeat_n(bit, count));
                }
            }
        }
        result
    }

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

    #[test]
    fn flat_validity_reset_reuses_backing_bits() {
        let mut validity = FlatValidity::new();
        validity.reset(10);
        for _ in 0..10 {
            validity.push_present();
        }

        validity.reset(3);
        validity.push_null();
        validity.push_present();
        validity.push_null();

        let mut buffer = Vec::new();
        let meta = validity
            .encode_def_levels(&mut buffer, Version::V2)
            .unwrap();
        assert_eq!(meta.null_count, 2);
        assert_eq!(meta.definition_levels_byte_length, 2);
        assert_eq!(decode_def_levels(&buffer, 3, Version::V2), vec![0, 1, 0]);
    }

    #[test]
    fn flat_validity_crosses_byte_boundary() {
        let mut validity = FlatValidity::new();
        validity.reset(10);
        for idx in 0..10 {
            if idx % 3 == 0 {
                validity.push_null();
            } else {
                validity.push_present();
            }
        }

        let mut buffer = Vec::new();
        let meta = validity
            .encode_def_levels(&mut buffer, Version::V2)
            .unwrap();
        assert_eq!(meta.null_count, 4);
        assert_eq!(
            decode_def_levels(&buffer, 10, Version::V2),
            vec![0, 1, 1, 0, 1, 1, 0, 1, 1, 0]
        );
    }

    #[test]
    fn flat_validity_all_present_uses_rle() {
        let mut validity = FlatValidity::new();
        validity.reset(9);
        for _ in 0..9 {
            validity.push_present();
        }

        let mut buffer = Vec::new();
        let meta = validity
            .encode_def_levels(&mut buffer, Version::V2)
            .unwrap();
        assert_eq!(meta.null_count, 0);
        assert_eq!(meta.definition_levels_byte_length, 2);
        assert_eq!(decode_def_levels(&buffer, 9, Version::V2), vec![1; 9]);
    }

    #[test]
    fn flat_validity_all_null_uses_rle() {
        let mut validity = FlatValidity::new();
        validity.reset(9);
        for _ in 0..9 {
            validity.push_null();
        }

        let mut buffer = Vec::new();
        let meta = validity
            .encode_def_levels(&mut buffer, Version::V2)
            .unwrap();
        assert_eq!(meta.null_count, 9);
        assert_eq!(meta.definition_levels_byte_length, 2);
        assert_eq!(decode_def_levels(&buffer, 9, Version::V2), vec![0; 9]);
    }

    #[test]
    fn flat_validity_zero_rows_encodes_nothing() {
        let mut validity = FlatValidity::new();
        validity.reset(0);

        let mut buffer = vec![1, 2, 3];
        let meta = validity
            .encode_def_levels(&mut buffer, Version::V2)
            .unwrap();
        assert_eq!(meta.null_count, 0);
        assert_eq!(meta.definition_levels_byte_length, 0);
        assert_eq!(buffer, vec![1, 2, 3]);
    }
}
