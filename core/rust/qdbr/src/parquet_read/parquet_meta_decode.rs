use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet::qdb_metadata::{QdbMetaCol, QdbMetaColFormat};
use crate::parquet_metadata::reader::ParquetMetaReader;
use crate::parquet_metadata::types::{ColumnFlags, StatFlags};
use crate::parquet_read::decode_column::{
    decode_column_chunk_filtered_with_params, decode_column_chunk_with_params,
    reconstruct_descriptor,
};
use crate::parquet_read::{DecodeContext, RowGroupBuffers};
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
        let num_values = i64::try_from(chunk.num_values).map_err(|_| {
            fmt_err!(
                InvalidType,
                "num_values {} out of i64 range",
                chunk.num_values
            )
        })?;

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
        let num_values = i64::try_from(chunk.num_values).map_err(|_| {
            fmt_err!(
                InvalidType,
                "num_values {} out of i64 range",
                chunk.num_values
            )
        })?;

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
        // row_hi + 1 may overflow on a crafted call; saturating_add keeps
        // the comparison meaningful (row_count is bounded by file rows).
        if row_hi.saturating_add(1) < row_count {
            break;
        }

        let row_count_end = row_count.checked_add(num_rows).ok_or_else(|| {
            fmt_err!(
                InvalidType,
                "row count overflow: {} + {}",
                row_count,
                num_rows
            )
        })?;
        if row_lo < row_count_end {
            let chunk = rg_block.column_chunk(ts_col)?;
            let stat_flags = StatFlags(chunk.stat_flags);

            let min_value = if stat_flags.has_min_stat() && stat_flags.is_min_inlined() {
                chunk.min_stat as i64
            } else {
                decode_ts(rg_idx, ts_col, 0, 1)?
            };

            if timestamp < min_value {
                let marker = (rg_idx as u64)
                    .checked_mul(2)
                    .and_then(|v| v.checked_add(1))
                    .ok_or_else(|| {
                        fmt_err!(InvalidType, "row group marker overflow at rg {}", rg_idx)
                    })?;
                return Ok(marker);
            }

            let max_value = if stat_flags.has_max_stat() && stat_flags.is_max_inlined() {
                chunk.max_stat as i64
            } else {
                let num_vals = chunk.num_values as usize;
                if num_vals == 0 {
                    continue;
                }
                decode_ts(rg_idx, ts_col, num_vals - 1, num_vals)?
            };

            if timestamp < max_value {
                let marker = (rg_idx as u64)
                    .checked_add(1)
                    .and_then(|v| v.checked_mul(2))
                    .ok_or_else(|| {
                        fmt_err!(InvalidType, "row group marker overflow at rg {}", rg_idx)
                    })?;
                return Ok(marker);
            }
        }
        row_count = row_count_end;
    }

    let end_marker = (row_group_count as u64)
        .checked_mul(2)
        .and_then(|v| v.checked_add(1))
        .ok_or_else(|| {
            fmt_err!(
                InvalidType,
                "row group end marker overflow for count {}",
                row_group_count
            )
        })?;
    Ok(end_marker)
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
    fn build_ts_parquet_meta(row_groups: &[(u64, i64, i64)]) -> ParquetResult<(Vec<u8>, u64)> {
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

        Ok(writer.finish()?)
    }

    // -----------------------------------------------------------------------
    // find_row_group_by_timestamp
    // -----------------------------------------------------------------------

    #[test]
    fn find_ts_before_all_data() -> ParquetResult<()> {
        let (parquet_meta, fo) = build_ts_parquet_meta(&[(100, 1000, 2000)])?;
        let reader = ParquetMetaReader::from_file_size(&parquet_meta, fo)?;

        // timestamp < min of first row group → 2*0+1 = 1
        let result = find_row_group_by_timestamp(&reader, 500, 0, 100, 0, |_, _, _, _| {
            panic!("decode should not be called when inline stats exist");
        })?;
        assert_eq!(result, 1);
        Ok(())
    }

    #[test]
    fn find_ts_within_row_group() -> ParquetResult<()> {
        let (parquet_meta, fo) = build_ts_parquet_meta(&[(100, 1000, 2000)])?;
        let reader = ParquetMetaReader::from_file_size(&parquet_meta, fo)?;

        // 1000 <= 1500 < 2000 → inside rg 0 → 2*(0+1) = 2
        let result = find_row_group_by_timestamp(&reader, 1500, 0, 100, 0, |_, _, _, _| {
            panic!("decode should not be called");
        })?;
        assert_eq!(result, 2);
        Ok(())
    }

    #[test]
    fn find_ts_after_all_data() -> ParquetResult<()> {
        let (parquet_meta, fo) = build_ts_parquet_meta(&[(100, 1000, 2000)])?;
        let reader = ParquetMetaReader::from_file_size(&parquet_meta, fo)?;

        // timestamp >= max → end marker: 2*1+1 = 3
        let result = find_row_group_by_timestamp(&reader, 3000, 0, 100, 0, |_, _, _, _| {
            panic!("decode should not be called");
        })?;
        assert_eq!(result, 3);
        Ok(())
    }

    #[test]
    fn find_ts_multiple_row_groups() -> ParquetResult<()> {
        let (parquet_meta, fo) = build_ts_parquet_meta(&[(100, 1000, 2000), (100, 2000, 3000), (100, 3000, 4000)])?;
        let reader = ParquetMetaReader::from_file_size(&parquet_meta, fo)?;

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
        let (parquet_meta, fo) = build_ts_parquet_meta(&[
            (0, 0, 0), // empty, skipped
            (100, 1000, 2000),
        ])?;
        let reader = ParquetMetaReader::from_file_size(&parquet_meta, fo)?;

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

        let (parquet_meta, fo) = writer.finish()?;
        let reader = ParquetMetaReader::from_file_size(&parquet_meta, fo)?;

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
        let (parquet_meta, fo) = build_ts_parquet_meta(&[(100, 1000, 2000)]).unwrap();
        let reader = ParquetMetaReader::from_file_size(&parquet_meta, fo).unwrap();

        let err =
            find_row_group_by_timestamp(&reader, 1500, 0, 100, 99, |_, _, _, _| unreachable!());
        assert!(err.is_err());
        assert!(err
            .unwrap_err()
            .to_string()
            .contains("timestamp column index 99 out of range"));
    }

    // -----------------------------------------------------------------------
    // decode_row_group
    // -----------------------------------------------------------------------

    #[test]
    fn decode_single_timestamp_column() -> ParquetResult<()> {
        let (parquet_data, parquet_meta_bytes, parquet_meta_file_size) = build_matched_parquet_meta(10)?;

        let reader = ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size)?;

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
        let (parquet_data, parquet_meta_bytes, parquet_meta_file_size) = build_matched_parquet_meta(10)?;
        let reader = ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size)?;

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
        let (parquet_data, parquet_meta_bytes, parquet_meta_file_size) = build_matched_parquet_meta(10)?;
        let reader = ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size)?;

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
        let (parquet_data, parquet_meta_bytes, parquet_meta_file_size) = build_matched_parquet_meta(100)?;
        let reader = ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size)?;

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
        let (parquet_data, parquet_meta_bytes, parquet_meta_file_size) = build_matched_parquet_meta(100)?;
        let reader = ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size)?;

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
    /// Returns `(parquet_bytes, parquet_meta_bytes, parquet_meta_file_size)`.
    fn build_matched_parquet_meta(row_count: usize) -> ParquetResult<(Vec<u8>, Vec<u8>, u64)> {
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

        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0, None)?;

        Ok((parquet_buf, parquet_meta_bytes, parquet_meta_file_size))
    }
}
