use std::slice;

use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet::qdb_metadata::{QdbMetaCol, QdbMetaColFormat};
use crate::parquet_metadata::reader::ParquetMetaReader;
use crate::parquet_metadata::types::{decode_stat_sizes, ColumnFlags, StatFlags};
use crate::parquet_read::decode_column::{
    decode_column_chunk_filtered_with_params, decode_column_chunk_with_params,
    reconstruct_descriptor,
};
use crate::parquet_read::row_groups::ParquetDecoder;
use crate::parquet_read::{
    ColumnFilterPacked, ColumnFilterValues, DecodeContext, RowGroupBuffers, FILTER_OP_BETWEEN,
    FILTER_OP_EQ, FILTER_OP_GE, FILTER_OP_GT, FILTER_OP_IS_NOT_NULL, FILTER_OP_IS_NULL,
    FILTER_OP_LE, FILTER_OP_LT,
};
use parquet2::schema::types::PhysicalType;
use parquet2::schema::Repetition;
use qdb_core::col_type::{ColumnType, ColumnTypeTag};

use crate::parquet_read::row_groups::ParquetColumnIndex;

/// Decode a row group using metadata from a `_pm` sidecar file.
///
/// Column types, byte ranges, codecs, and descriptors are read from the
/// `_pm` binary format via [`ParquetMetaReader`]. The `col_pairs` array
/// uses the same `[parquet_column_index, column_type]` pair format as
/// `PartitionDecoder` for compatibility with `PageFrameMemoryPool`.
/// The `column_type` from Java is used for Symbol->Varchar and
/// Varchar->VarcharSlice overrides; the base type comes from `_pm`.
#[allow(clippy::too_many_arguments)]
pub fn decode_row_group(
    ctx: &mut DecodeContext,
    row_group_bufs: &mut RowGroupBuffers,
    file_data: &[u8],
    parquet_meta_reader: &ParquetMetaReader,
    col_pairs: &[(ParquetColumnIndex, ColumnType)],
    row_group_index: usize,
    row_group_lo: usize,
    row_group_hi: usize,
) -> ParquetResult<usize> {
    let rg_count = parquet_meta_reader.row_group_count();
    if row_group_index >= rg_count as usize {
        return Err(fmt_err!(
            InvalidType,
            "row group index {} out of range [0,{})",
            row_group_index,
            rg_count
        ));
    }

    let rg_block = parquet_meta_reader.row_group(row_group_index)?;
    let col_count = parquet_meta_reader.column_count();

    row_group_bufs.ensure_n_columns(col_pairs.len())?;

    let mut decoded = 0usize;
    for (dest_col_idx, &(column_idx, to_column_type)) in col_pairs.iter().enumerate() {
        let column_idx = column_idx as usize;
        if column_idx >= col_count as usize {
            return Err(fmt_err!(
                InvalidType,
                "column index {} out of range [0,{})",
                column_idx,
                col_count
            ));
        }

        let col_desc = parquet_meta_reader.column_descriptor(column_idx)?;
        let col_type_code = col_desc.col_type;
        let mut column_type = ColumnType::new_raw(col_type_code)
            .ok_or_else(|| fmt_err!(InvalidType, "unknown column type code: {}", col_type_code))?;

        // Apply the same Symbol->Varchar and Varchar->VarcharSlice overrides
        // as ParquetDecoder::decode_row_group().
        if column_type.tag() == ColumnTypeTag::Symbol
            && (to_column_type.tag() == ColumnTypeTag::Varchar
                || to_column_type.tag() == ColumnTypeTag::VarcharSlice)
        {
            column_type = to_column_type;
        }
        if column_type.tag() == ColumnTypeTag::Varchar
            && to_column_type.tag() == ColumnTypeTag::VarcharSlice
        {
            column_type = to_column_type;
        }

        let flags = ColumnFlags(col_desc.flags);
        let field_rep = flags
            .repetition()
            .unwrap_or(crate::parquet_metadata::types::FieldRepetition::Optional);
        let repetition: Repetition = field_rep.into();

        let column_name = parquet_meta_reader
            .column_name(column_idx)
            .unwrap_or("<unknown>");

        let format = if flags.is_local_key_global() {
            Some(QdbMetaColFormat::LocalKeyIsGlobal)
        } else {
            None
        };
        let ascii = if flags.is_ascii() { Some(true) } else { None };

        let column_chunk_bufs = &mut row_group_bufs.column_bufs[dest_col_idx];
        let col_info = QdbMetaCol { column_type, column_top: 0, format, ascii };

        let chunk = rg_block.column_chunk(column_idx)?;
        let stat_flags = StatFlags(chunk.stat_flags);
        if stat_flags.has_null_count() && chunk.null_count == chunk.num_values {
            column_chunk_bufs.reset();
            decoded = row_group_hi.saturating_sub(row_group_lo);
            continue;
        }
        let col_start = chunk.byte_range_start as usize;
        let col_len = chunk.total_compressed as usize;
        let compression = chunk
            .codec()
            .map_err(|e| fmt_err!(InvalidType, "invalid codec: {}", e))?;
        let compression: parquet2::compression::Compression = compression.into();
        let num_values = chunk.num_values as i64;

        let descriptor = reconstruct_descriptor(
            col_desc.physical_type,
            col_desc.fixed_byte_len,
            col_desc.max_rep_level,
            col_desc.max_def_level,
            column_name,
            repetition,
        );

        match decode_column_chunk_with_params(
            ctx,
            column_chunk_bufs,
            file_data,
            col_start,
            col_len,
            compression,
            descriptor,
            num_values,
            col_info,
            row_group_lo,
            row_group_hi,
            column_name,
            row_group_index,
        ) {
            Ok(count) => decoded = count,
            Err(err) => return Err(err),
        }
    }

    Ok(decoded)
}

/// Decode a row group with row-level filtering using `_pm` metadata.
///
/// When `FILL_NULLS` is true, unfiltered rows are filled with nulls.
/// When false, unfiltered rows are skipped.
#[allow(clippy::too_many_arguments)]
pub fn decode_row_group_filtered<const FILL_NULLS: bool>(
    ctx: &mut DecodeContext,
    row_group_bufs: &mut RowGroupBuffers,
    file_data: &[u8],
    parquet_meta_reader: &ParquetMetaReader,
    column_offset: usize,
    col_pairs: &[(ParquetColumnIndex, ColumnType)],
    row_group_index: usize,
    row_group_lo: usize,
    row_group_hi: usize,
    filtered_rows: &[i64],
) -> ParquetResult<usize> {
    let rg_block = parquet_meta_reader.row_group(row_group_index)?;
    let col_count = parquet_meta_reader.column_count();

    row_group_bufs.ensure_n_columns(column_offset + col_pairs.len())?;

    let mut decoded = 0usize;
    for (dest_col_idx, &(column_idx, to_column_type)) in col_pairs.iter().enumerate() {
        let column_idx = column_idx as usize;
        if column_idx >= col_count as usize {
            return Err(fmt_err!(
                InvalidType,
                "column index {} out of range [0,{})",
                column_idx,
                col_count
            ));
        }

        let col_desc = parquet_meta_reader.column_descriptor(column_idx)?;
        let col_type_code = col_desc.col_type;
        let mut column_type = ColumnType::new_raw(col_type_code)
            .ok_or_else(|| fmt_err!(InvalidType, "unknown column type code: {}", col_type_code))?;

        if column_type.tag() == ColumnTypeTag::Symbol
            && (to_column_type.tag() == ColumnTypeTag::Varchar
                || to_column_type.tag() == ColumnTypeTag::VarcharSlice)
        {
            column_type = to_column_type;
        }
        if column_type.tag() == ColumnTypeTag::Varchar
            && to_column_type.tag() == ColumnTypeTag::VarcharSlice
        {
            column_type = to_column_type;
        }

        let flags = ColumnFlags(col_desc.flags);
        let field_rep = flags
            .repetition()
            .unwrap_or(crate::parquet_metadata::types::FieldRepetition::Optional);
        let repetition: Repetition = field_rep.into();
        let column_name = parquet_meta_reader
            .column_name(column_idx)
            .unwrap_or("<unknown>");
        let format = if flags.is_local_key_global() {
            Some(QdbMetaColFormat::LocalKeyIsGlobal)
        } else {
            None
        };
        let ascii = if flags.is_ascii() { Some(true) } else { None };

        let chunk = rg_block.column_chunk(column_idx)?;
        let buf_idx = column_offset + dest_col_idx;
        let column_chunk_bufs = &mut row_group_bufs.column_bufs[buf_idx];
        let col_info = QdbMetaCol { column_type, column_top: 0, format, ascii };
        let stat_flags = StatFlags(chunk.stat_flags);
        if stat_flags.has_null_count() && chunk.null_count == chunk.num_values {
            column_chunk_bufs.reset();
            decoded = if FILL_NULLS {
                row_group_hi.saturating_sub(row_group_lo)
            } else {
                filtered_rows.len()
            };
            continue;
        }
        let col_start = chunk.byte_range_start as usize;
        let col_len = chunk.total_compressed as usize;
        let compression: parquet2::compression::Compression = chunk
            .codec()
            .map_err(|e| fmt_err!(InvalidType, "invalid codec: {}", e))?
            .into();
        let num_values = chunk.num_values as i64;

        let descriptor = reconstruct_descriptor(
            col_desc.physical_type,
            col_desc.fixed_byte_len,
            col_desc.max_rep_level,
            col_desc.max_def_level,
            column_name,
            repetition,
        );

        match decode_column_chunk_filtered_with_params::<FILL_NULLS>(
            ctx,
            column_chunk_bufs,
            file_data,
            col_start,
            col_len,
            compression,
            descriptor,
            num_values,
            col_info,
            row_group_lo,
            row_group_hi,
            filtered_rows,
            column_name,
            row_group_index,
        ) {
            Ok(count) => decoded = count,
            Err(err) => return Err(err),
        }
    }

    Ok(decoded)
}

/// Find the row group containing the given timestamp using `_pm` metadata.
///
/// Reads min/max timestamp stats directly from `_pm` column chunks.
/// Falls back to `decode_ts(rg_idx, ts_col, row_lo, row_hi)` when inline
/// stats are unavailable (should not happen for QDB-written partitions).
#[allow(clippy::too_many_arguments)]
pub fn find_row_group_by_timestamp(
    parquet_meta_reader: &ParquetMetaReader,
    timestamp: i64,
    row_lo: usize,
    row_hi: usize,
    ts_col: usize,
    decode_ts: impl Fn(usize, usize, usize, usize) -> ParquetResult<i64>,
) -> ParquetResult<u64> {
    let row_group_count = parquet_meta_reader.row_group_count() as usize;
    let col_count = parquet_meta_reader.column_count() as usize;

    if ts_col >= col_count {
        return Err(fmt_err!(
            InvalidType,
            "timestamp column index {} out of range [0,{})",
            ts_col,
            col_count
        ));
    }

    let mut row_count = 0usize;
    for rg_idx in 0..row_group_count {
        let rg_block = parquet_meta_reader.row_group(rg_idx)?;
        let num_rows = rg_block.num_rows() as usize;

        if num_rows == 0 {
            continue;
        }
        if row_hi + 1 < row_count {
            break;
        }

        if row_lo < row_count + num_rows {
            let chunk = rg_block.column_chunk(ts_col)?;
            let stat_flags = StatFlags(chunk.stat_flags);

            let min_value = if stat_flags.has_min_stat() && stat_flags.is_min_inlined() {
                chunk.min_stat as i64
            } else {
                decode_ts(rg_idx, ts_col, 0, 1)?
            };

            if timestamp < min_value {
                return Ok((2 * rg_idx + 1) as u64);
            }

            let max_value = if stat_flags.has_max_stat() && stat_flags.is_max_inlined() {
                chunk.max_stat as i64
            } else {
                let num_vals = chunk.num_values as usize;
                decode_ts(rg_idx, ts_col, num_vals - 1, num_vals)?
            };

            if timestamp < max_value {
                return Ok(2 * (rg_idx + 1) as u64);
            }
        }
        row_count += num_rows;
    }

    Ok((2 * row_group_count + 1) as u64)
}

/// Row group filter pushdown using `_pm` metadata for both statistics and
/// bloom filters. Bloom filter bitsets are stored inline in the `_pm`
/// out-of-line region, so this function reads everything from the
/// `parquet_meta_reader` slice and never touches the parquet data file.
pub fn can_skip_row_group(
    parquet_meta_reader: &ParquetMetaReader,
    row_group_index: usize,
    filters: &[ColumnFilterPacked],
    filter_buf_end: u64,
) -> ParquetResult<bool> {
    if row_group_index >= parquet_meta_reader.row_group_count() as usize {
        return Err(fmt_err!(
            InvalidType,
            "row group index {} out of range [0,{})",
            row_group_index,
            parquet_meta_reader.row_group_count()
        ));
    }

    let rg_block = parquet_meta_reader.row_group(row_group_index)?;
    let col_count = parquet_meta_reader.column_count() as usize;

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

        let col_desc = parquet_meta_reader.column_descriptor(column_idx)?;
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
            min_stat_sz as usize
        };
        let max_inline_size = if phys_size > 0 {
            phys_size.min(8)
        } else {
            max_stat_sz as usize
        };

        // Decode an OOL stat reference: `(offset << 32) | length`.
        let decode_ool = |encoded: u64| -> Option<&[u8]> {
            let ool_off = (encoded >> 32) as usize;
            let ool_len = (encoded & 0xFFFF_FFFF) as usize;
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
                let bitset: &[u8] = if parquet_meta_reader.has_bloom_filters() {
                    if let Some(pos) = parquet_meta_reader.bloom_filter_position(column_idx as u32)
                    {
                        if parquet_meta_reader.has_bloom_filters_external() {
                            // External mode: bitsets are in the parquet file, not
                            // available from the _pm reader alone. Skip.
                            &[]
                        } else {
                            let off = parquet_meta_reader
                                .bloom_filter_offset_in_pm(row_group_index, pos)
                                .unwrap_or(0);
                            if off > 0 && (off as usize) + 4 <= parquet_meta_reader.data().len() {
                                let bf_data = &parquet_meta_reader.data()[off as usize..];
                                let bf_len =
                                    i32::from_le_bytes(bf_data[..4].try_into().unwrap_or_default())
                                        as usize;
                                bf_data.get(4..4 + bf_len).unwrap_or(&[])
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
    use crate::parquet::error::ParquetResult;
    use crate::parquet_metadata::column_chunk::ColumnChunkRaw;
    use crate::parquet_metadata::row_group::RowGroupBlockBuilder;
    use crate::parquet_metadata::types::{encode_stat_sizes, Codec, FieldRepetition};
    use crate::parquet_metadata::writer::ParquetMetaWriter;

    /// Physical type ordinal for Int64 in the `_pm` format.
    const PHYS_INT64: u8 = 2;

    /// Build a `_pm` file with one Timestamp column and the given row groups.
    /// Each row group entry is `(num_rows, min_ts, max_ts)`.
    fn build_ts_pm(row_groups: &[(u64, i64, i64)]) -> ParquetResult<(Vec<u8>, u64)> {
        let mut writer = ParquetMetaWriter::new();
        writer
            .designated_timestamp(0)
            .add_column(
                "ts",
                0,
                ColumnTypeTag::Timestamp as i32,
                ColumnFlags::new().with_repetition(FieldRepetition::Required),
                0,
                PHYS_INT64,
                0,
                0,
            )
            .parquet_footer(0, 0);

        for &(num_rows, min_ts, max_ts) in row_groups {
            let mut rg = RowGroupBlockBuilder::new(1);
            rg.set_num_rows(num_rows);

            let mut chunk = ColumnChunkRaw::zeroed();
            chunk.codec = Codec::Uncompressed as u8;
            chunk.num_values = num_rows;
            chunk.stat_flags = StatFlags::new()
                .with_min(true, true)
                .with_max(true, true)
                .with_null_count()
                .0;
            chunk.stat_sizes = encode_stat_sizes(8, 8);
            chunk.min_stat = min_ts as u64;
            chunk.max_stat = max_ts as u64;
            rg.set_column_chunk(0, chunk)?;

            writer.add_row_group(rg);
        }

        writer.finish()
    }

    /// Build a `_pm` file with one Int64 column and configurable stats per row group.
    /// Each entry: `(num_rows, null_count, min, max, has_stats)`.
    fn build_long_pm(row_groups: &[(u64, u64, i64, i64, bool)]) -> ParquetResult<(Vec<u8>, u64)> {
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

        writer.finish()
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

    // -----------------------------------------------------------------------
    // find_row_group_by_timestamp
    // -----------------------------------------------------------------------

    #[test]
    fn find_ts_before_all_data() -> ParquetResult<()> {
        let (pm, fo) = build_ts_pm(&[(100, 1000, 2000)])?;
        let reader = ParquetMetaReader::new(&pm, fo)?;

        // timestamp < min of first row group → 2*0+1 = 1
        let result = find_row_group_by_timestamp(&reader, 500, 0, 100, 0, |_, _, _, _| {
            panic!("decode should not be called when inline stats exist");
        })?;
        assert_eq!(result, 1);
        Ok(())
    }

    #[test]
    fn find_ts_within_row_group() -> ParquetResult<()> {
        let (pm, fo) = build_ts_pm(&[(100, 1000, 2000)])?;
        let reader = ParquetMetaReader::new(&pm, fo)?;

        // 1000 <= 1500 < 2000 → inside rg 0 → 2*(0+1) = 2
        let result = find_row_group_by_timestamp(&reader, 1500, 0, 100, 0, |_, _, _, _| {
            panic!("decode should not be called");
        })?;
        assert_eq!(result, 2);
        Ok(())
    }

    #[test]
    fn find_ts_after_all_data() -> ParquetResult<()> {
        let (pm, fo) = build_ts_pm(&[(100, 1000, 2000)])?;
        let reader = ParquetMetaReader::new(&pm, fo)?;

        // timestamp >= max → end marker: 2*1+1 = 3
        let result = find_row_group_by_timestamp(&reader, 3000, 0, 100, 0, |_, _, _, _| {
            panic!("decode should not be called");
        })?;
        assert_eq!(result, 3);
        Ok(())
    }

    #[test]
    fn find_ts_multiple_row_groups() -> ParquetResult<()> {
        let (pm, fo) = build_ts_pm(&[(100, 1000, 2000), (100, 2000, 3000), (100, 3000, 4000)])?;
        let reader = ParquetMetaReader::new(&pm, fo)?;

        // Before first → 1
        assert_eq!(
            find_row_group_by_timestamp(&reader, 500, 0, 300, 0, |_, _, _, _| unreachable!())?,
            1
        );

        // In second row group: 2000 <= 2500 < 3000 → 2*(1+1) = 4
        assert_eq!(
            find_row_group_by_timestamp(&reader, 2500, 0, 300, 0, |_, _, _, _| unreachable!())?,
            4
        );

        // In third row group: 3000 <= 3500 < 4000 → 2*(2+1) = 6
        assert_eq!(
            find_row_group_by_timestamp(&reader, 3500, 0, 300, 0, |_, _, _, _| unreachable!())?,
            6
        );

        // After all → 2*3+1 = 7
        assert_eq!(
            find_row_group_by_timestamp(&reader, 5000, 0, 300, 0, |_, _, _, _| unreachable!())?,
            7
        );

        Ok(())
    }

    #[test]
    fn find_ts_empty_row_group_skipped() -> ParquetResult<()> {
        let (pm, fo) = build_ts_pm(&[
            (0, 0, 0), // empty, skipped
            (100, 1000, 2000),
        ])?;
        let reader = ParquetMetaReader::new(&pm, fo)?;

        // Should skip rg 0 (empty) and find timestamp in rg 1.
        let result =
            find_row_group_by_timestamp(&reader, 1500, 0, 100, 0, |_, _, _, _| unreachable!())?;
        assert_eq!(result, 2 * (1 + 1) as u64);
        Ok(())
    }

    #[test]
    fn find_ts_decode_fallback_called() -> ParquetResult<()> {
        // Build _pm without inline stats on the timestamp column.
        let mut writer = ParquetMetaWriter::new();
        writer
            .designated_timestamp(0)
            .add_column(
                "ts",
                0,
                ColumnTypeTag::Timestamp as i32,
                ColumnFlags::new().with_repetition(FieldRepetition::Required),
                0,
                PHYS_INT64,
                0,
                0,
            )
            .parquet_footer(0, 0);

        let mut rg = RowGroupBlockBuilder::new(1);
        rg.set_num_rows(100);
        let mut chunk = ColumnChunkRaw::zeroed();
        chunk.codec = Codec::Uncompressed as u8;
        chunk.num_values = 100;
        // No stat flags set → fallback to decode.
        rg.set_column_chunk(0, chunk)?;
        writer.add_row_group(rg);

        let (pm, fo) = writer.finish()?;
        let reader = ParquetMetaReader::new(&pm, fo)?;

        // The closure returns min=1000 (row_lo=0, row_hi=1) and max=2000
        // (row_lo=99, row_hi=100).
        let result = find_row_group_by_timestamp(
            &reader,
            1500,
            0,
            100,
            0,
            |_rg_idx, _ts_col, row_lo, row_hi| {
                if row_lo == 0 && row_hi == 1 {
                    Ok(1000i64) // min
                } else {
                    Ok(2000i64) // max
                }
            },
        )?;
        // 1000 <= 1500 < 2000 → inside rg 0 → 2*(0+1) = 2
        assert_eq!(result, 2);
        Ok(())
    }

    #[test]
    fn find_ts_col_out_of_range() {
        let (pm, fo) = build_ts_pm(&[(100, 1000, 2000)]).unwrap();
        let reader = ParquetMetaReader::new(&pm, fo).unwrap();

        let err =
            find_row_group_by_timestamp(&reader, 1500, 0, 100, 99, |_, _, _, _| unreachable!());
        assert!(err.is_err());
        assert!(err
            .unwrap_err()
            .to_string()
            .contains("timestamp column index 99 out of range"));
    }

    // -----------------------------------------------------------------------
    // can_skip_row_group
    // -----------------------------------------------------------------------

    #[test]
    fn skip_is_null_when_no_nulls() -> ParquetResult<()> {
        //                  (rows, nulls, min, max, has_stats)
        let (pm, fo) = build_long_pm(&[(100, 0, 10, 200, true)])?;
        let reader = ParquetMetaReader::new(&pm, fo)?;

        let filter = make_filter(0, 0, FILTER_OP_IS_NULL, 0, ColumnTypeTag::Long as i32);
        assert!(can_skip_row_group(&reader, 0, &[filter], 0)?);
        Ok(())
    }

    #[test]
    fn no_skip_is_null_when_nulls_present() -> ParquetResult<()> {
        let (pm, fo) = build_long_pm(&[(100, 5, 10, 200, true)])?;
        let reader = ParquetMetaReader::new(&pm, fo)?;

        let filter = make_filter(0, 0, FILTER_OP_IS_NULL, 0, ColumnTypeTag::Long as i32);
        assert!(!can_skip_row_group(&reader, 0, &[filter], 0)?);
        Ok(())
    }

    #[test]
    fn skip_is_not_null_when_all_nulls() -> ParquetResult<()> {
        let (pm, fo) = build_long_pm(&[(100, 100, 0, 0, true)])?;
        let reader = ParquetMetaReader::new(&pm, fo)?;

        let filter = make_filter(0, 0, FILTER_OP_IS_NOT_NULL, 0, ColumnTypeTag::Long as i32);
        assert!(can_skip_row_group(&reader, 0, &[filter], 0)?);
        Ok(())
    }

    #[test]
    fn no_skip_is_not_null_when_some_non_nulls() -> ParquetResult<()> {
        let (pm, fo) = build_long_pm(&[(100, 50, 10, 200, true)])?;
        let reader = ParquetMetaReader::new(&pm, fo)?;

        let filter = make_filter(0, 0, FILTER_OP_IS_NOT_NULL, 0, ColumnTypeTag::Long as i32);
        assert!(!can_skip_row_group(&reader, 0, &[filter], 0)?);
        Ok(())
    }

    #[test]
    fn skip_eq_outside_min_max() -> ParquetResult<()> {
        // Range [100, 200], search for 300.
        let (pm, fo) = build_long_pm(&[(1000, 0, 100, 200, true)])?;
        let reader = ParquetMetaReader::new(&pm, fo)?;

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
        let (pm, fo) = build_long_pm(&[(1000, 0, 100, 200, true)])?;
        let reader = ParquetMetaReader::new(&pm, fo)?;

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
        let (pm, fo) = build_long_pm(&[(1000, 0, 100, 200, true)])?;
        let reader = ParquetMetaReader::new(&pm, fo)?;

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
        let (pm, fo) = build_long_pm(&[(1000, 0, 100, 200, true)])?;
        let reader = ParquetMetaReader::new(&pm, fo)?;

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
        let (pm, fo) = build_long_pm(&[(1000, 0, 0, 0, false)])?;
        let reader = ParquetMetaReader::new(&pm, fo)?;

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
        let (pm, fo) = build_long_pm(&[(100, 0, 10, 200, true)]).unwrap();
        let reader = ParquetMetaReader::new(&pm, fo).unwrap();

        let err = can_skip_row_group(&reader, 5, &[], 0);
        assert!(err.is_err());
        assert!(err
            .unwrap_err()
            .to_string()
            .contains("row group index 5 out of range"));
    }

    #[test]
    fn skip_column_index_out_of_range_continues() -> ParquetResult<()> {
        let (pm, fo) = build_long_pm(&[(100, 0, 10, 200, true)])?;
        let reader = ParquetMetaReader::new(&pm, fo)?;

        // Column index 99 doesn't exist → filter is ignored, no skip.
        let filter = make_filter(99, 0, FILTER_OP_IS_NULL, 0, ColumnTypeTag::Long as i32);
        assert!(!can_skip_row_group(&reader, 0, &[filter], 0)?);
        Ok(())
    }

    #[test]
    fn skip_empty_filters() -> ParquetResult<()> {
        let (pm, fo) = build_long_pm(&[(100, 0, 10, 200, true)])?;
        let reader = ParquetMetaReader::new(&pm, fo)?;

        assert!(!can_skip_row_group(&reader, 0, &[], 0)?);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // decode_row_group
    // -----------------------------------------------------------------------

    #[test]
    fn decode_single_timestamp_column() -> ParquetResult<()> {
        let (parquet_data, pm_bytes, pm_footer_offset) = build_matched_parquet_pm(10)?;

        let reader = ParquetMetaReader::new(&pm_bytes, pm_footer_offset)?;

        let tas = crate::allocator::TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut ctx = DecodeContext::new(parquet_data.as_ptr(), parquet_data.len() as u64);
        let mut bufs = RowGroupBuffers::new(allocator);

        let col_pairs = [(0i32, ColumnType::new(ColumnTypeTag::Timestamp, 0))];
        let decoded = decode_row_group(
            &mut ctx,
            &mut bufs,
            &parquet_data,
            &reader,
            &col_pairs,
            0,
            0,
            10,
        )?;
        assert_eq!(decoded, 10);

        // Verify decoded timestamp values: 0, 1, 2, ..., 9.
        let data = &bufs.column_bufs[0].data_vec;
        assert_eq!(data.len(), 10 * 8);
        for i in 0..10 {
            let val = i64::from_le_bytes(data[i * 8..(i + 1) * 8].try_into().unwrap());
            assert_eq!(val, i as i64, "timestamp at index {}", i);
        }

        Ok(())
    }

    #[test]
    fn decode_row_group_index_out_of_range() -> ParquetResult<()> {
        let (parquet_data, pm_bytes, pm_footer_offset) = build_matched_parquet_pm(10)?;
        let reader = ParquetMetaReader::new(&pm_bytes, pm_footer_offset)?;

        let tas = crate::allocator::TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut ctx = DecodeContext::new(parquet_data.as_ptr(), parquet_data.len() as u64);
        let mut bufs = RowGroupBuffers::new(allocator);

        let col_pairs = [(0i32, ColumnType::new(ColumnTypeTag::Timestamp, 0))];
        let err = decode_row_group(
            &mut ctx,
            &mut bufs,
            &parquet_data,
            &reader,
            &col_pairs,
            5,
            0,
            10,
        );
        assert!(err.is_err());
        assert!(err
            .unwrap_err()
            .to_string()
            .contains("row group index 5 out of range"));
        Ok(())
    }

    #[test]
    fn decode_column_index_out_of_range() -> ParquetResult<()> {
        let (parquet_data, pm_bytes, pm_footer_offset) = build_matched_parquet_pm(10)?;
        let reader = ParquetMetaReader::new(&pm_bytes, pm_footer_offset)?;

        let tas = crate::allocator::TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut ctx = DecodeContext::new(parquet_data.as_ptr(), parquet_data.len() as u64);
        let mut bufs = RowGroupBuffers::new(allocator);

        // Column 99 doesn't exist.
        let col_pairs = [(99i32, ColumnType::new(ColumnTypeTag::Timestamp, 0))];
        let err = decode_row_group(
            &mut ctx,
            &mut bufs,
            &parquet_data,
            &reader,
            &col_pairs,
            0,
            0,
            10,
        );
        assert!(err.is_err());
        assert!(err
            .unwrap_err()
            .to_string()
            .contains("column index 99 out of range"));
        Ok(())
    }

    #[test]
    fn decode_partial_row_range() -> ParquetResult<()> {
        let (parquet_data, pm_bytes, pm_footer_offset) = build_matched_parquet_pm(100)?;
        let reader = ParquetMetaReader::new(&pm_bytes, pm_footer_offset)?;

        let tas = crate::allocator::TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut ctx = DecodeContext::new(parquet_data.as_ptr(), parquet_data.len() as u64);
        let mut bufs = RowGroupBuffers::new(allocator);

        let col_pairs = [(0i32, ColumnType::new(ColumnTypeTag::Timestamp, 0))];
        decode_row_group(
            &mut ctx,
            &mut bufs,
            &parquet_data,
            &reader,
            &col_pairs,
            0,
            10,
            20,
        )?;

        // The data buffer contains only the requested 10-row slice.
        let data = &bufs.column_bufs[0].data_vec;
        assert_eq!(data.len(), 10 * 8);
        for i in 0..10 {
            let val = i64::from_le_bytes(data[i * 8..(i + 1) * 8].try_into().unwrap());
            assert_eq!(val, (i + 10) as i64);
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // decode_row_group_filtered
    // -----------------------------------------------------------------------

    #[test]
    fn decode_filtered_subset() -> ParquetResult<()> {
        let (parquet_data, pm_bytes, pm_footer_offset) = build_matched_parquet_pm(100)?;
        let reader = ParquetMetaReader::new(&pm_bytes, pm_footer_offset)?;

        let tas = crate::allocator::TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut ctx = DecodeContext::new(parquet_data.as_ptr(), parquet_data.len() as u64);
        let mut bufs = RowGroupBuffers::new(allocator);

        let col_pairs = [(0i32, ColumnType::new(ColumnTypeTag::Timestamp, 0))];
        let filtered_rows: Vec<i64> = vec![0, 5, 10, 50, 99];

        let decoded = decode_row_group_filtered::<false>(
            &mut ctx,
            &mut bufs,
            &parquet_data,
            &reader,
            0,
            &col_pairs,
            0,
            0,
            100,
            &filtered_rows,
        )?;
        assert_eq!(decoded, filtered_rows.len());

        let data = &bufs.column_bufs[0].data_vec;
        assert_eq!(data.len(), filtered_rows.len() * 8);
        for (i, &expected_row) in filtered_rows.iter().enumerate() {
            let val = i64::from_le_bytes(data[i * 8..(i + 1) * 8].try_into().unwrap());
            assert_eq!(val, expected_row, "filtered row at index {}", i);
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// Create matched parquet file + `_pm` bytes using `convert_from_parquet`.
    /// Returns `(parquet_bytes, pm_bytes, pm_footer_offset)`.
    fn build_matched_parquet_pm(row_count: usize) -> ParquetResult<(Vec<u8>, Vec<u8>, u64)> {
        use crate::parquet::qdb_metadata::QdbMeta;
        use crate::parquet::tests::ColumnTypeTagExt;
        use crate::parquet_metadata::convert::convert_from_parquet;
        use crate::parquet_write::file::ParquetWriter;
        use crate::parquet_write::schema::{Column, ParquetEncodingConfig, Partition};
        use parquet2::compression::CompressionOptions;
        use parquet2::read::read_metadata_with_size;
        use parquet2::write::Version;
        use std::io::Cursor;

        let col_data: Vec<i64> = (0..row_count as i64).collect();
        let data_bytes: &[u8] = unsafe {
            std::slice::from_raw_parts(col_data.as_ptr() as *const u8, col_data.len() * 8)
        };
        let data_static: &'static [u8] = Box::leak(data_bytes.to_vec().into_boxed_slice());

        let col = Column {
            name: "ts",
            data_type: ColumnTypeTag::Timestamp.into_type(),
            id: 0,
            row_count,
            primary_data: data_static,
            secondary_data: &[],
            symbol_offsets: &[],
            column_top: 0,
            designated_timestamp: true,
            not_null_hint: true,
            designated_timestamp_ascending: true,
            parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
        };

        let partition = Partition { table: "test".to_string(), columns: vec![col] };

        let mut parquet_buf = Vec::new();
        ParquetWriter::new(&mut parquet_buf)
            .with_statistics(true)
            .with_compression(CompressionOptions::Uncompressed)
            .with_version(Version::V1)
            .with_row_group_size(Some(row_count))
            .finish(partition)
            .unwrap();

        let mut cursor = Cursor::new(&parquet_buf);
        let metadata = read_metadata_with_size(&mut cursor, parquet_buf.len() as u64).unwrap();
        let qdb_meta = metadata
            .key_value_metadata
            .as_ref()
            .and_then(|kvs| {
                kvs.iter()
                    .find(|kv| kv.key == "questdb")
                    .and_then(|kv| kv.value.as_deref())
            })
            .map(|j| QdbMeta::deserialize(j).unwrap());

        let (pm_bytes, footer_offset) = convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0)?;

        Ok((parquet_buf, pm_bytes, footer_offset))
    }

    /// Physical type ordinal for FixedLenByteArray in the `_pm` format.
    const PHYS_FLBA: u8 = 7;

    /// Build a `_pm` file with one FLBA(16) UUID column and OOL stats.
    fn build_uuid_pm(
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

        writer.finish()
    }

    #[test]
    fn skip_eq_outside_uuid_ool_stats() -> ParquetResult<()> {
        // UUID range [0x11..11, 0x33..33], search for 0xFF..FF → outside range.
        let min = [0x11u8; 16];
        let max = [0x33u8; 16];
        let (pm, fo) = build_uuid_pm(&[(100, 0, min, max)])?;
        let reader = ParquetMetaReader::new(&pm, fo)?;

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
