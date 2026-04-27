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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

//! Row group filter pushdown against `_pm` metadata. This is qdbr-only
//! because it delegates comparison and bloom filter probing to
//! `crate::parquet_read::row_groups::ParquetDecoder`.

use std::slice;

use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_metadata::reader::ParquetMetaReader;
use crate::parquet_metadata::types::{decode_stat_sizes, StatFlags};
use crate::parquet_read::row_groups::ParquetDecoder;
use crate::parquet_read::{
    ColumnFilterPacked, ColumnFilterValues, FILTER_OP_BETWEEN, FILTER_OP_EQ, FILTER_OP_GE,
    FILTER_OP_GT, FILTER_OP_IS_NOT_NULL, FILTER_OP_IS_NULL, FILTER_OP_LE, FILTER_OP_LT,
};
use parquet2::schema::types::PhysicalType;
use qdb_core::col_type::ColumnTypeTag;

/// Row group filter pushdown using `_pm` metadata for both statistics and
/// bloom filters. Bloom filter bitsets are stored inline in the `_pm`
/// out-of-line region, so this routine reads everything from the `_pm`
/// slice and never touches the parquet data file.
pub fn can_skip_row_group(
    reader: &ParquetMetaReader<'_>,
    row_group_index: usize,
    filters: &[ColumnFilterPacked],
    filter_buf_end: u64,
) -> ParquetResult<bool> {
    if row_group_index >= reader.row_group_count() as usize {
        return Err(fmt_err!(
            InvalidType,
            "row group index {} out of range [0,{})",
            row_group_index,
            reader.row_group_count()
        ));
    }

    let rg_block = reader.row_group(row_group_index)?;
    let col_count = reader.column_count() as usize;

    for packed_filter in filters {
        let count = packed_filter.count();
        let op = packed_filter.operation_type();

        if count > 0 && packed_filter.ptr == 0 {
            return Err(fmt_err!(
                InvalidType,
                "invalid filter payload: null pointer with non-zero count"
            ));
        }
        let column_idx = packed_filter.column_index() as usize;
        if column_idx >= col_count {
            continue;
        }

        let chunk = rg_block.column_chunk(column_idx)?;
        let stat_flags = StatFlags(chunk.stat_flags);
        let null_count = if stat_flags.has_null_count() {
            Some(chunk.null_count as i64)
        } else {
            None
        };
        let num_values = Some(chunk.num_values as i64);

        if op == FILTER_OP_IS_NULL {
            if null_count == Some(0) {
                return Ok(true);
            }
            continue;
        }
        if op == FILTER_OP_IS_NOT_NULL {
            if let (Some(nc), Some(nv)) = (null_count, num_values) {
                if nc == nv {
                    return Ok(true);
                }
            }
            continue;
        }

        let filter_desc = ColumnFilterValues {
            count,
            ptr: packed_filter.ptr,
            buf_end: filter_buf_end,
        };

        let col_desc = reader.column_descriptor(column_idx)?;
        let physical_type = match col_desc.physical_type {
            0 => PhysicalType::Boolean,
            1 => PhysicalType::Int32,
            2 => PhysicalType::Int64,
            3 => PhysicalType::Int96,
            4 => PhysicalType::Float,
            5 => PhysicalType::Double,
            6 => PhysicalType::ByteArray,
            7 => PhysicalType::FixedLenByteArray(col_desc.fixed_byte_len as usize),
            _ => continue,
        };
        let has_nulls = null_count.is_none_or(|c| c > 0);
        let qdb_column_type = packed_filter.qdb_column_type();
        let col_type_tag = qdb_column_type & 0xFF;

        let is_decimal = matches!(
            col_type_tag,
            x if x == ColumnTypeTag::Decimal8 as i32
                || x == ColumnTypeTag::Decimal16 as i32
                || x == ColumnTypeTag::Decimal32 as i32
                || x == ColumnTypeTag::Decimal64 as i32
                || x == ColumnTypeTag::Decimal128 as i32
                || x == ColumnTypeTag::Decimal256 as i32
        );

        // Read inline stats at the physical type width, not the narrowed QDB
        // width. The _pm stores narrowed values (e.g., 1 byte for BYTE) but the
        // pruning comparison expects physical type width (4 bytes for INT32).
        let phys_size = match physical_type {
            PhysicalType::Boolean => 1,
            PhysicalType::Int32 | PhysicalType::Float => 4,
            PhysicalType::Int64 | PhysicalType::Double => 8,
            PhysicalType::Int96 => 0, // not inlined
            PhysicalType::ByteArray => 0,
            PhysicalType::FixedLenByteArray(len) => len,
        };

        let ool = rg_block.out_of_line_region();

        // Inline size: for types with known physical width (Int32, Int64, etc.),
        // use phys_size clamped to 8. For ByteArray (phys_size=0), use stat_sizes
        // nibble since the value width varies (e.g., Symbol stores short strings inline).
        let (min_stat_sz, max_stat_sz) = decode_stat_sizes(chunk.stat_sizes);
        let min_inline_size = if phys_size > 0 {
            phys_size.min(8)
        } else {
            (min_stat_sz as usize).min(8)
        };
        let max_inline_size = if phys_size > 0 {
            phys_size.min(8)
        } else {
            (max_stat_sz as usize).min(8)
        };

        // Decode an OOL stat reference: `(offset << 16) | length`
        // (offset = high 48 bits, length = low 16 bits).
        let decode_ool = |encoded: u64| -> Option<&[u8]> {
            let ool_off = (encoded >> 16) as usize;
            let ool_len = (encoded & 0xFFFF) as usize;
            if ool_len == 0 {
                return None;
            }
            ool.get(ool_off..ool_off + ool_len)
        };

        let min_bytes: Option<&[u8]> = if stat_flags.has_min_stat() {
            if stat_flags.is_min_inlined() && min_inline_size > 0 {
                Some(unsafe {
                    slice::from_raw_parts(
                        &chunk.min_stat as *const u64 as *const u8,
                        min_inline_size,
                    )
                })
            } else if !stat_flags.is_min_inlined() {
                decode_ool(chunk.min_stat)
            } else {
                None
            }
        } else {
            None
        };

        let max_bytes: Option<&[u8]> = if stat_flags.has_max_stat() {
            if stat_flags.is_max_inlined() && max_inline_size > 0 {
                Some(unsafe {
                    slice::from_raw_parts(
                        &chunk.max_stat as *const u64 as *const u8,
                        max_inline_size,
                    )
                })
            } else if !stat_flags.is_max_inlined() {
                decode_ool(chunk.max_stat)
            } else {
                None
            }
        } else {
            None
        };

        match op {
            FILTER_OP_EQ => {
                // Bloom filter bitset lookup via footer feature section.
                let bitset: &[u8] = if reader.has_bloom_filters() {
                    if let Some(pos) = reader.bloom_filter_position(column_idx as u32) {
                        if reader.has_bloom_filters_external() {
                            // External mode: bitsets are in the parquet file, not
                            // available from the _pm reader alone. Skip.
                            &[]
                        } else {
                            let off = reader
                                .bloom_filter_offset_in_pm(row_group_index, pos)
                                .unwrap_or(0);
                            if off > 0 && (off as usize) + 4 <= reader.data().len() {
                                let bf_data = &reader.data()[off as usize..];
                                let bf_len_raw =
                                    i32::from_le_bytes(bf_data[..4].try_into().unwrap_or_default());
                                if bf_len_raw <= 0 {
                                    &[]
                                } else {
                                    let bf_len = bf_len_raw as usize;
                                    bf_data.get(4..4 + bf_len).unwrap_or(&[])
                                }
                            } else {
                                &[]
                            }
                        }
                    } else {
                        &[]
                    }
                } else {
                    &[]
                };
                if !bitset.is_empty() {
                    let all_absent = ParquetDecoder::all_values_absent_from_bloom(
                        bitset,
                        &physical_type,
                        &filter_desc,
                        has_nulls,
                        is_decimal,
                        qdb_column_type,
                    )?;
                    if all_absent {
                        return Ok(true);
                    }
                }

                let is_ipv4 = col_type_tag == ColumnTypeTag::IPv4 as i32;
                let is_date = col_type_tag == ColumnTypeTag::Date as i32;
                let is_third_party_unsigned = false;
                if !is_third_party_unsigned {
                    if let (Some(min_b), Some(max_b)) = (min_bytes, max_bytes) {
                        if ParquetDecoder::all_values_outside_min_max_with_stats(
                            &physical_type,
                            &filter_desc,
                            has_nulls,
                            is_decimal,
                            is_ipv4,
                            is_date,
                            Some(min_b),
                            Some(max_b),
                        )? {
                            return Ok(true);
                        }
                    }
                }
            }
            FILTER_OP_LT | FILTER_OP_LE | FILTER_OP_GT | FILTER_OP_GE | FILTER_OP_BETWEEN => {
                let is_ipv4 = col_type_tag == ColumnTypeTag::IPv4 as i32;
                let is_date = col_type_tag == ColumnTypeTag::Date as i32;
                if let (Some(min_b), Some(max_b)) = (min_bytes, max_bytes) {
                    if ParquetDecoder::value_outside_range(
                        &physical_type,
                        &filter_desc,
                        is_decimal,
                        is_ipv4,
                        is_date,
                        op,
                        Some(min_b),
                        Some(max_b),
                    )? {
                        return Ok(true);
                    }
                }
            }
            _ => {}
        }
    }

    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet_metadata::column_chunk::ColumnChunkRaw;
    use crate::parquet_metadata::row_group::RowGroupBlockBuilder;
    use crate::parquet_metadata::types::{encode_stat_sizes, Codec, ColumnFlags, FieldRepetition};
    use crate::parquet_metadata::writer::ParquetMetaWriter;

    /// Physical type ordinal for Int64 in the `_pm` format.
    const PHYS_INT64: u8 = 2;

    /// Physical type ordinal for FixedLenByteArray in the `_pm` format.
    const PHYS_FLBA: u8 = 7;

    /// Build a `_pm` file with one Int64 column and configurable stats per row group.
    /// Each entry: `(num_rows, null_count, min, max, has_stats)`.
    fn build_long_parquet_meta(row_groups: &[(u64, u64, i64, i64, bool)]) -> ParquetResult<(Vec<u8>, u64)> {
        let mut writer = ParquetMetaWriter::new();
        writer
            .designated_timestamp(-1)
            .add_column(
                "val",
                0,
                ColumnTypeTag::Long as i32,
                ColumnFlags::new().with_repetition(FieldRepetition::Optional),
                0,
                PHYS_INT64,
                0,
                1, // max_def_level = 1 for optional
            )
            .parquet_footer(0, 0);

        for &(num_rows, null_count, min, max, has_stats) in row_groups {
            let mut rg = RowGroupBlockBuilder::new(1);
            rg.set_num_rows(num_rows);

            let mut chunk = ColumnChunkRaw::zeroed();
            chunk.codec = Codec::Uncompressed as u8;
            chunk.num_values = num_rows;
            chunk.null_count = null_count;

            if has_stats {
                chunk.stat_flags = StatFlags::new()
                    .with_min(true, true)
                    .with_max(true, true)
                    .with_null_count()
                    .0;
                chunk.stat_sizes = encode_stat_sizes(8, 8);
                chunk.min_stat = min as u64;
                chunk.max_stat = max as u64;
            } else {
                chunk.stat_flags = StatFlags::new().with_null_count().0;
            }
            rg.set_column_chunk(0, chunk)?;

            writer.add_row_group(rg);
        }

        Ok(writer.finish()?)
    }

    /// Build a `_pm` file with one FLBA(16) UUID column and OOL stats.
    fn build_uuid_parquet_meta(
        row_groups: &[(u64, u64, [u8; 16], [u8; 16])],
    ) -> ParquetResult<(Vec<u8>, u64)> {
        let mut writer = ParquetMetaWriter::new();
        writer
            .designated_timestamp(-1)
            .add_column(
                "val",
                0,
                ColumnTypeTag::Uuid as i32,
                ColumnFlags::new().with_repetition(FieldRepetition::Optional),
                16, // fixed_byte_len for FLBA(16)
                PHYS_FLBA,
                0,
                1, // max_def_level = 1 for optional
            )
            .parquet_footer(0, 0);

        for &(num_rows, null_count, ref min, ref max) in row_groups {
            let mut rg = RowGroupBlockBuilder::new(1);
            rg.set_num_rows(num_rows);

            let mut chunk = ColumnChunkRaw::zeroed();
            chunk.codec = Codec::Uncompressed as u8;
            chunk.num_values = num_rows;
            chunk.null_count = null_count;
            // OOL stats: is_min_inlined=false
            chunk.stat_flags = StatFlags::new()
                .with_min(false, true)
                .with_max(false, true)
                .with_null_count()
                .0;
            rg.set_column_chunk(0, chunk)?;
            rg.add_out_of_line_stat(0, true, min)?;
            rg.add_out_of_line_stat(0, false, max)?;

            writer.add_row_group(rg);
        }

        Ok(writer.finish()?)
    }

    fn make_filter(
        column_index: u32,
        count: u32,
        op: u8,
        ptr: u64,
        col_type: i32,
    ) -> ColumnFilterPacked {
        ColumnFilterPacked {
            col_idx_and_count: (column_index as u64)
                | (((count as u64) & 0x00FF_FFFF) << 32)
                | ((op as u64) << 56),
            ptr,
            column_type: col_type as u64,
        }
    }

    #[test]
    fn skip_is_null_when_no_nulls() -> ParquetResult<()> {
        //                  (rows, nulls, min, max, has_stats)
        let (parquet_meta, fo) = build_long_parquet_meta(&[(100, 0, 10, 200, true)])?;
        let reader = ParquetMetaReader::from_file_size(&parquet_meta, fo)?;

        let filter = make_filter(0, 0, FILTER_OP_IS_NULL, 0, ColumnTypeTag::Long as i32);
        assert!(can_skip_row_group(&reader, 0, &[filter], 0)?);
        Ok(())
    }

    #[test]
    fn no_skip_is_null_when_nulls_present() -> ParquetResult<()> {
        let (parquet_meta, fo) = build_long_parquet_meta(&[(100, 5, 10, 200, true)])?;
        let reader = ParquetMetaReader::from_file_size(&parquet_meta, fo)?;

        let filter = make_filter(0, 0, FILTER_OP_IS_NULL, 0, ColumnTypeTag::Long as i32);
        assert!(!can_skip_row_group(&reader, 0, &[filter], 0)?);
        Ok(())
    }

    #[test]
    fn skip_is_not_null_when_all_nulls() -> ParquetResult<()> {
        let (parquet_meta, fo) = build_long_parquet_meta(&[(100, 100, 0, 0, true)])?;
        let reader = ParquetMetaReader::from_file_size(&parquet_meta, fo)?;

        let filter = make_filter(0, 0, FILTER_OP_IS_NOT_NULL, 0, ColumnTypeTag::Long as i32);
        assert!(can_skip_row_group(&reader, 0, &[filter], 0)?);
        Ok(())
    }

    #[test]
    fn no_skip_is_not_null_when_some_non_nulls() -> ParquetResult<()> {
        let (parquet_meta, fo) = build_long_parquet_meta(&[(100, 50, 10, 200, true)])?;
        let reader = ParquetMetaReader::from_file_size(&parquet_meta, fo)?;

        let filter = make_filter(0, 0, FILTER_OP_IS_NOT_NULL, 0, ColumnTypeTag::Long as i32);
        assert!(!can_skip_row_group(&reader, 0, &[filter], 0)?);
        Ok(())
    }

    #[test]
    fn skip_eq_outside_min_max() -> ParquetResult<()> {
        // Range [100, 200], search for 300.
        let (parquet_meta, fo) = build_long_parquet_meta(&[(1000, 0, 100, 200, true)])?;
        let reader = ParquetMetaReader::from_file_size(&parquet_meta, fo)?;

        let value: i64 = 300;
        let filter = make_filter(
            0,
            1,
            FILTER_OP_EQ,
            &value as *const i64 as u64,
            ColumnTypeTag::Long as i32,
        );
        let buf_end = unsafe { (&value as *const i64).add(1) } as u64;
        assert!(can_skip_row_group(&reader, 0, &[filter], buf_end)?);
        Ok(())
    }

    #[test]
    fn no_skip_eq_inside_min_max() -> ParquetResult<()> {
        // Range [100, 200], search for 150.
        let (parquet_meta, fo) = build_long_parquet_meta(&[(1000, 0, 100, 200, true)])?;
        let reader = ParquetMetaReader::from_file_size(&parquet_meta, fo)?;

        let value: i64 = 150;
        let filter = make_filter(
            0,
            1,
            FILTER_OP_EQ,
            &value as *const i64 as u64,
            ColumnTypeTag::Long as i32,
        );
        let buf_end = unsafe { (&value as *const i64).add(1) } as u64;
        assert!(!can_skip_row_group(&reader, 0, &[filter], buf_end)?);
        Ok(())
    }

    #[test]
    fn skip_gt_above_max() -> ParquetResult<()> {
        // Range [100, 200], GT 200 → all values <= 200 so skip.
        let (parquet_meta, fo) = build_long_parquet_meta(&[(1000, 0, 100, 200, true)])?;
        let reader = ParquetMetaReader::from_file_size(&parquet_meta, fo)?;

        let value: i64 = 200;
        let filter = make_filter(
            0,
            1,
            FILTER_OP_GT,
            &value as *const i64 as u64,
            ColumnTypeTag::Long as i32,
        );
        let buf_end = unsafe { (&value as *const i64).add(1) } as u64;
        assert!(can_skip_row_group(&reader, 0, &[filter], buf_end)?);
        Ok(())
    }

    #[test]
    fn skip_lt_below_min() -> ParquetResult<()> {
        // Range [100, 200], LT 100 → all values >= 100 so skip.
        let (parquet_meta, fo) = build_long_parquet_meta(&[(1000, 0, 100, 200, true)])?;
        let reader = ParquetMetaReader::from_file_size(&parquet_meta, fo)?;

        let value: i64 = 100;
        let filter = make_filter(
            0,
            1,
            FILTER_OP_LT,
            &value as *const i64 as u64,
            ColumnTypeTag::Long as i32,
        );
        let buf_end = unsafe { (&value as *const i64).add(1) } as u64;
        assert!(can_skip_row_group(&reader, 0, &[filter], buf_end)?);
        Ok(())
    }

    #[test]
    fn no_skip_without_stats() -> ParquetResult<()> {
        let (parquet_meta, fo) = build_long_parquet_meta(&[(1000, 0, 0, 0, false)])?;
        let reader = ParquetMetaReader::from_file_size(&parquet_meta, fo)?;

        let value: i64 = 9999;
        let filter = make_filter(
            0,
            1,
            FILTER_OP_EQ,
            &value as *const i64 as u64,
            ColumnTypeTag::Long as i32,
        );
        let buf_end = unsafe { (&value as *const i64).add(1) } as u64;
        // No stats → conservative, cannot skip.
        assert!(!can_skip_row_group(&reader, 0, &[filter], buf_end)?);
        Ok(())
    }

    #[test]
    fn skip_rg_index_out_of_range() {
        let (parquet_meta, fo) = build_long_parquet_meta(&[(100, 0, 10, 200, true)]).unwrap();
        let reader = ParquetMetaReader::from_file_size(&parquet_meta, fo).unwrap();

        let err = can_skip_row_group(&reader, 5, &[], 0);
        assert!(err.is_err());
        assert!(err
            .unwrap_err()
            .to_string()
            .contains("row group index 5 out of range"));
    }

    #[test]
    fn skip_column_index_out_of_range_continues() -> ParquetResult<()> {
        let (parquet_meta, fo) = build_long_parquet_meta(&[(100, 0, 10, 200, true)])?;
        let reader = ParquetMetaReader::from_file_size(&parquet_meta, fo)?;

        // Column index 99 doesn't exist → filter is ignored, no skip.
        let filter = make_filter(99, 0, FILTER_OP_IS_NULL, 0, ColumnTypeTag::Long as i32);
        assert!(!can_skip_row_group(&reader, 0, &[filter], 0)?);
        Ok(())
    }

    #[test]
    fn skip_empty_filters() -> ParquetResult<()> {
        let (parquet_meta, fo) = build_long_parquet_meta(&[(100, 0, 10, 200, true)])?;
        let reader = ParquetMetaReader::from_file_size(&parquet_meta, fo)?;

        assert!(!can_skip_row_group(&reader, 0, &[], 0)?);
        Ok(())
    }

    #[test]
    fn skip_eq_outside_uuid_ool_stats() -> ParquetResult<()> {
        // UUID range [0x11..11, 0x33..33], search for 0xFF..FF → outside range.
        let min = [0x11u8; 16];
        let max = [0x33u8; 16];
        let (parquet_meta, fo) = build_uuid_parquet_meta(&[(100, 0, min, max)])?;
        let reader = ParquetMetaReader::from_file_size(&parquet_meta, fo)?;

        // Filter value: 0xFF..FF (16 bytes), outside the range.
        let filter_value = [0xFFu8; 16];
        let filter = make_filter(
            0,
            1,
            FILTER_OP_EQ,
            filter_value.as_ptr() as u64,
            ColumnTypeTag::Uuid as i32,
        );
        let buf_end = unsafe { filter_value.as_ptr().add(16) } as u64;
        assert!(
            can_skip_row_group(&reader, 0, &[filter], buf_end)?,
            "should skip row group when filter value is outside OOL stat range"
        );
        Ok(())
    }
}
