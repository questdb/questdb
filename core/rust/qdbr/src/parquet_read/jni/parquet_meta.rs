use std::slice;

use crate::allocator::QdbAllocator;
use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_read::jni::validate_jni_column_types;
use crate::parquet_read::row_groups::ParquetColumnIndex;
use crate::parquet_read::{ColumnFilterPacked, DecodeContext, ParquetDecoder, RowGroupBuffers};
use jni::objects::JClass;
use jni::JNIEnv;
use qdb_core::col_type::ColumnType;

/// Decode a row group using metadata from a `_pm` sidecar file.
///
/// Column types, byte ranges, codecs, and descriptors are read from the
/// `_pm` binary format via [`ParquetMetaReader`]. The `columns` array
/// uses the same `[parquet_column_index, column_type]` pair format as
/// `PartitionDecoder` for compatibility with `PageFrameMemoryPool`.
/// The `column_type` from Java is used for Symbol→Varchar and
/// Varchar→VarcharSlice overrides; the base type comes from `_pm`.
#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_ParquetMetaPartitionDecoder_decodeRowGroup(
    mut env: JNIEnv,
    _class: JClass,
    allocator: *const QdbAllocator,
    ctx: *mut DecodeContext,
    parquet_file_ptr: *const u8,
    parquet_file_size: u64,
    parquet_meta_ptr: *const u8,
    parquet_meta_size: u64,
    row_group_bufs: *mut RowGroupBuffers,
    columns: *const (ParquetColumnIndex, ColumnType), // [index, type] pairs
    column_count: u32,
    row_group_index: u32,
    row_group_lo: u32,
    row_group_hi: u32,
) -> u32 {
    let res = pm_decode_row_group_impl(
        allocator,
        ctx,
        parquet_file_ptr,
        parquet_file_size,
        parquet_meta_ptr,
        parquet_meta_size,
        row_group_bufs,
        columns,
        column_count,
        row_group_index,
        row_group_lo,
        row_group_hi,
    );
    match res {
        Ok(count) => count as u32,
        Err(mut err) => {
            err.add_context("error in ParquetMetaPartitionDecoder.decodeRowGroup");
            err.into_cairo_exception().throw(&mut env)
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_ParquetMetaPartitionDecoder_decodeRowGroupWithRowFilter(
    mut env: JNIEnv,
    _class: JClass,
    allocator: *const QdbAllocator,
    ctx: *mut DecodeContext,
    parquet_file_ptr: *const u8,
    parquet_file_size: u64,
    parquet_meta_ptr: *const u8,
    parquet_meta_size: u64,
    row_group_bufs: *mut RowGroupBuffers,
    column_offset: u32,
    columns: *const (ParquetColumnIndex, ColumnType),
    column_count: u32,
    row_group_index: u32,
    row_group_lo: u32,
    row_group_hi: u32,
    filtered_rows_ptr: *const i64,
    filtered_rows_size: i64,
) {
    let filtered_rows_count = if filtered_rows_size < 0 {
        0usize
    } else {
        filtered_rows_size as usize
    };
    let res = pm_decode_row_group_filtered_impl::<false>(
        allocator,
        ctx,
        parquet_file_ptr,
        parquet_file_size,
        parquet_meta_ptr,
        parquet_meta_size,
        row_group_bufs,
        column_offset as usize,
        columns,
        column_count,
        row_group_index,
        row_group_lo,
        row_group_hi,
        filtered_rows_ptr,
        filtered_rows_count,
    );
    if let Err(mut err) = res {
        err.add_context("error in ParquetMetaPartitionDecoder.decodeRowGroupWithRowFilter");
        let _: () = err.into_cairo_exception().throw(&mut env);
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_ParquetMetaPartitionDecoder_decodeRowGroupWithRowFilterFillNulls(
    mut env: JNIEnv,
    _class: JClass,
    allocator: *const QdbAllocator,
    ctx: *mut DecodeContext,
    parquet_file_ptr: *const u8,
    parquet_file_size: u64,
    parquet_meta_ptr: *const u8,
    parquet_meta_size: u64,
    row_group_bufs: *mut RowGroupBuffers,
    column_offset: u32,
    columns: *const (ParquetColumnIndex, ColumnType),
    column_count: u32,
    row_group_index: u32,
    row_group_lo: u32,
    row_group_hi: u32,
    filtered_rows_ptr: *const i64,
    filtered_rows_size: i64,
) {
    let filtered_rows_count = if filtered_rows_size < 0 {
        0usize
    } else {
        filtered_rows_size as usize
    };
    let res = pm_decode_row_group_filtered_impl::<true>(
        allocator,
        ctx,
        parquet_file_ptr,
        parquet_file_size,
        parquet_meta_ptr,
        parquet_meta_size,
        row_group_bufs,
        column_offset as usize,
        columns,
        column_count,
        row_group_index,
        row_group_lo,
        row_group_hi,
        filtered_rows_ptr,
        filtered_rows_count,
    );
    if let Err(mut err) = res {
        err.add_context(
            "error in ParquetMetaPartitionDecoder.decodeRowGroupWithRowFilterFillNulls",
        );
        let _: () = err.into_cairo_exception().throw(&mut env);
    }
}

#[allow(clippy::too_many_arguments)]
fn pm_decode_row_group_filtered_impl<const FILL_NULLS: bool>(
    _allocator: *const QdbAllocator,
    ctx: *mut DecodeContext,
    parquet_file_ptr: *const u8,
    parquet_file_size: u64,
    parquet_meta_ptr: *const u8,
    parquet_meta_size: u64,
    row_group_bufs: *mut RowGroupBuffers,
    column_offset: usize,
    columns: *const (ParquetColumnIndex, ColumnType),
    column_count: u32,
    row_group_index: u32,
    row_group_lo: u32,
    row_group_hi: u32,
    filtered_rows_ptr: *const i64,
    filtered_rows_count: usize,
) -> ParquetResult<usize> {
    use crate::parquet::qdb_metadata::{QdbMetaCol, QdbMetaColFormat};
    use crate::parquet_metadata::reader::ParquetMetaReader;
    use crate::parquet_metadata::types::ColumnFlags;
    use crate::parquet_read::decode_column::{
        decode_column_chunk_filtered_with_params, reconstruct_descriptor,
    };
    use parquet2::schema::Repetition;
    use qdb_core::col_type::ColumnTypeTag;

    if ctx.is_null() {
        return Err(fmt_err!(InvalidType, "decode context pointer is null"));
    }
    if row_group_bufs.is_null() {
        return Err(fmt_err!(InvalidType, "row group buffers pointer is null"));
    }
    if columns.is_null() && column_count > 0 {
        return Err(fmt_err!(InvalidType, "columns pointer is null"));
    }
    if filtered_rows_ptr.is_null() && filtered_rows_count > 0 {
        return Err(fmt_err!(InvalidType, "filtered rows pointer is null"));
    }
    if parquet_meta_ptr.is_null() || parquet_meta_size == 0 {
        return Err(fmt_err!(
            InvalidType,
            "_pm pointer is null or size is zero"
        ));
    }
    if parquet_file_ptr.is_null() || parquet_file_size == 0 {
        return Err(fmt_err!(
            InvalidType,
            "parquet file pointer is null or size is zero"
        ));
    }

    let parquet_meta_data =
        unsafe { slice::from_raw_parts(parquet_meta_ptr, parquet_meta_size as usize) };
    let parquet_meta_reader =
        ParquetMetaReader::from_file_size(parquet_meta_data, parquet_meta_size)?;
    let file_data = unsafe { slice::from_raw_parts(parquet_file_ptr, parquet_file_size as usize) };
    let ctx = unsafe { &mut *ctx };
    let row_group_bufs = unsafe { &mut *row_group_bufs };
    let col_pairs = unsafe { slice::from_raw_parts(columns, column_count as usize) };
    let filtered_rows = if filtered_rows_ptr.is_null() {
        &[]
    } else {
        unsafe { slice::from_raw_parts(filtered_rows_ptr, filtered_rows_count) }
    };

    let rg_block = parquet_meta_reader.row_group(row_group_index as usize)?;
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

        let col_info = QdbMetaCol {
            column_type,
            column_top: parquet_meta_reader.column_top(column_idx)? as usize,
            format,
            ascii,
        };
        let buf_idx = column_offset + dest_col_idx;
        let column_chunk_bufs = &mut row_group_bufs.column_bufs[buf_idx];

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
            row_group_lo as usize,
            row_group_hi as usize,
            filtered_rows,
            column_name,
            row_group_index as usize,
        ) {
            Ok(count) => decoded = count,
            Err(err) => return Err(err),
        }
    }

    Ok(decoded)
}

#[allow(clippy::too_many_arguments)]
fn pm_decode_row_group_impl(
    _allocator: *const QdbAllocator,
    ctx: *mut DecodeContext,
    parquet_file_ptr: *const u8,
    parquet_file_size: u64,
    parquet_meta_ptr: *const u8,
    parquet_meta_size: u64,
    row_group_bufs: *mut RowGroupBuffers,
    columns: *const (ParquetColumnIndex, ColumnType),
    column_count: u32,
    row_group_index: u32,
    row_group_lo: u32,
    row_group_hi: u32,
) -> ParquetResult<usize> {
    use crate::parquet::qdb_metadata::{QdbMetaCol, QdbMetaColFormat};
    use crate::parquet_metadata::reader::ParquetMetaReader;
    use crate::parquet_metadata::types::ColumnFlags;
    use crate::parquet_read::decode_column::{
        decode_column_chunk_with_params, reconstruct_descriptor,
    };
    use parquet2::schema::Repetition;
    use qdb_core::col_type::ColumnTypeTag;

    if ctx.is_null() {
        return Err(fmt_err!(InvalidType, "decode context pointer is null"));
    }
    if row_group_bufs.is_null() {
        return Err(fmt_err!(InvalidType, "row group buffers pointer is null"));
    }
    if columns.is_null() && column_count > 0 {
        return Err(fmt_err!(InvalidType, "columns pointer is null"));
    }
    if column_count > 0 {
        let col_pairs = unsafe { slice::from_raw_parts(columns, column_count as usize) };
        validate_jni_column_types(col_pairs)?;
    }
    if parquet_meta_ptr.is_null() || parquet_meta_size == 0 {
        return Err(fmt_err!(
            InvalidType,
            "_pm pointer is null or size is zero"
        ));
    }
    if parquet_file_ptr.is_null() || parquet_file_size == 0 {
        return Err(fmt_err!(
            InvalidType,
            "parquet file pointer is null or size is zero"
        ));
    }

    let parquet_meta_data =
        unsafe { slice::from_raw_parts(parquet_meta_ptr, parquet_meta_size as usize) };
    let parquet_meta_reader =
        ParquetMetaReader::from_file_size(parquet_meta_data, parquet_meta_size)?;

    let rg_count = parquet_meta_reader.row_group_count();
    if row_group_index >= rg_count {
        return Err(fmt_err!(
            InvalidType,
            "row group index {} out of range [0,{})",
            row_group_index,
            rg_count
        ));
    }

    let file_data = unsafe { slice::from_raw_parts(parquet_file_ptr, parquet_file_size as usize) };
    let ctx = unsafe { &mut *ctx };
    let row_group_bufs = unsafe { &mut *row_group_bufs };
    let col_pairs = unsafe { slice::from_raw_parts(columns, column_count as usize) };

    let rg_block = parquet_meta_reader.row_group(row_group_index as usize)?;
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

        // Apply the same Symbol→Varchar and Varchar→VarcharSlice overrides
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

        let column_top = parquet_meta_reader.column_top(column_idx)? as usize;
        let accumulated_size = (0..row_group_index as usize).try_fold(0usize, |acc, rg| {
            parquet_meta_reader
                .row_group(rg)
                .map(|b| acc + b.num_rows() as usize)
        })?;

        let column_chunk_bufs = &mut row_group_bufs.column_bufs[dest_col_idx];

        if column_top >= row_group_hi as usize + accumulated_size {
            column_chunk_bufs.reset();
            continue;
        }

        let chunk = rg_block.column_chunk(column_idx)?;
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

        let col_info = QdbMetaCol { column_type, column_top, format, ascii };

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
            row_group_lo as usize,
            row_group_hi as usize,
            column_name,
            row_group_index as usize,
        ) {
            Ok(count) => decoded = count,
            Err(err) => return Err(err),
        }
    }

    Ok(decoded)
}

/// Find the row group containing the given timestamp using `_pm` metadata.
///
/// Reads min/max timestamp stats directly from the `_pm` file's column chunks.
/// Falls back to actual decode if stats are unavailable (should not happen for
/// QDB-written partitions).
#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_ParquetMetaPartitionDecoder_findRowGroupByTimestamp(
    mut env: JNIEnv,
    _class: JClass,
    allocator: *const QdbAllocator,
    parquet_file_ptr: *const u8,
    parquet_file_size: u64,
    parquet_meta_ptr: *const u8,
    parquet_meta_size: u64,
    timestamp: i64,
    row_lo: i64,
    row_hi: i64,
    timestamp_column_index: i32,
) -> i64 {
    let res = pm_find_row_group_by_timestamp_impl(
        allocator,
        parquet_file_ptr,
        parquet_file_size,
        parquet_meta_ptr,
        parquet_meta_size,
        timestamp,
        row_lo as usize,
        row_hi as usize,
        timestamp_column_index as usize,
    );
    match res {
        Ok(val) => val as i64,
        Err(mut err) => {
            err.add_context("error in ParquetMetaPartitionDecoder.findRowGroupByTimestamp");
            err.into_cairo_exception().throw(&mut env)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn pm_find_row_group_by_timestamp_impl(
    allocator: *const QdbAllocator,
    parquet_file_ptr: *const u8,
    parquet_file_size: u64,
    parquet_meta_ptr: *const u8,
    parquet_meta_size: u64,
    timestamp: i64,
    row_lo: usize,
    row_hi: usize,
    ts_col: usize,
) -> ParquetResult<u64> {
    use crate::parquet_metadata::reader::ParquetMetaReader;
    use crate::parquet_metadata::types::StatFlags;

    if parquet_meta_ptr.is_null() || parquet_meta_size == 0 {
        return Err(fmt_err!(
            InvalidType,
            "_pm pointer is null or size is zero"
        ));
    }

    let parquet_meta_data =
        unsafe { slice::from_raw_parts(parquet_meta_ptr, parquet_meta_size as usize) };
    let parquet_meta_reader =
        ParquetMetaReader::from_file_size(parquet_meta_data, parquet_meta_size)?;

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

            // Read min timestamp from inline stat.
            let min_value = if stat_flags.has_min_stat() && stat_flags.is_min_inlined() {
                chunk.min_stat as i64
            } else {
                // Fall back to decoding — this shouldn't happen for QDB-written files.
                decode_single_ts_from_pm(
                    allocator,
                    parquet_file_ptr,
                    parquet_file_size,
                    &parquet_meta_reader,
                    rg_idx,
                    ts_col,
                    0,
                    1,
                )?
            };

            if timestamp < min_value {
                return Ok((2 * rg_idx + 1) as u64);
            }

            // Read max timestamp from inline stat.
            let max_value = if stat_flags.has_max_stat() && stat_flags.is_max_inlined() {
                chunk.max_stat as i64
            } else {
                let num_vals = chunk.num_values as usize;
                decode_single_ts_from_pm(
                    allocator,
                    parquet_file_ptr,
                    parquet_file_size,
                    &parquet_meta_reader,
                    rg_idx,
                    ts_col,
                    num_vals - 1,
                    num_vals,
                )?
            };

            if timestamp < max_value {
                return Ok(2 * (rg_idx + 1) as u64);
            }
        }
        row_count += num_rows;
    }

    Ok((2 * row_group_count + 1) as u64)
}

/// Decode a single timestamp value from the parquet file using `_pm` metadata.
#[allow(clippy::too_many_arguments)]
fn decode_single_ts_from_pm(
    allocator: *const QdbAllocator,
    parquet_file_ptr: *const u8,
    parquet_file_size: u64,
    parquet_meta_reader: &crate::parquet_metadata::reader::ParquetMetaReader,
    rg_idx: usize,
    ts_col: usize,
    row_lo: usize,
    row_hi: usize,
) -> ParquetResult<i64> {
    use crate::parquet::qdb_metadata::QdbMetaCol;
    use crate::parquet_read::decode_column::{
        decode_column_chunk_with_params, reconstruct_descriptor,
    };
    use crate::parquet_read::ColumnChunkBuffers;

    let file_data = unsafe { slice::from_raw_parts(parquet_file_ptr, parquet_file_size as usize) };
    let col_desc = parquet_meta_reader.column_descriptor(ts_col)?;
    let rg_block = parquet_meta_reader.row_group(rg_idx)?;
    let chunk = rg_block.column_chunk(ts_col)?;

    let col_start = chunk.byte_range_start as usize;
    let col_len = chunk.total_compressed as usize;
    let compression: parquet2::compression::Compression = chunk
        .codec()
        .map_err(|e| fmt_err!(InvalidType, "invalid codec: {}", e))?
        .into();
    let num_values = chunk.num_values as i64;

    let flags = crate::parquet_metadata::types::ColumnFlags(col_desc.flags);
    let field_rep = flags
        .repetition()
        .unwrap_or(crate::parquet_metadata::types::FieldRepetition::Required);
    let column_name = parquet_meta_reader.column_name(ts_col).unwrap_or("<ts>");

    let descriptor = reconstruct_descriptor(
        col_desc.physical_type,
        col_desc.fixed_byte_len,
        col_desc.max_rep_level,
        col_desc.max_def_level,
        column_name,
        field_rep.into(),
    );

    let col_info = QdbMetaCol {
        column_type: ColumnType::new(qdb_core::col_type::ColumnTypeTag::Timestamp, 0),
        column_top: 0,
        format: None,
        ascii: None,
    };

    let mut ctx = DecodeContext::new(parquet_file_ptr, parquet_file_size);
    let alloc = unsafe { &*allocator }.clone();
    let mut bufs = ColumnChunkBuffers::new(alloc);

    decode_column_chunk_with_params(
        &mut ctx,
        &mut bufs,
        file_data,
        col_start,
        col_len,
        compression,
        descriptor,
        num_values,
        col_info,
        row_lo,
        row_hi,
        column_name,
        rg_idx,
    )?;

    if bufs.data_vec.len() < 8 {
        return Err(fmt_err!(
            InvalidType,
            "decoded timestamp buffer too small: {}",
            bufs.data_vec.len()
        ));
    }

    Ok(i64::from_le_bytes(bufs.data_vec[..8].try_into().unwrap()))
}

/// Check if all filter values are absent from a bloom filter at the given
/// offset in the parquet file. Returns `true` if the row group can be skipped.
#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_ParquetMetaPartitionDecoder_checkBloomFilter(
    mut env: JNIEnv,
    _class: JClass,
    parquet_file_ptr: *const u8,
    parquet_file_size: u64,
    bloom_filter_offset: u64,
    physical_type: u8,
    fixed_byte_len: i32,
    filter_values_ptr: u64,
    filter_count: u32,
    filter_buf_end: u64,
    has_nulls: bool,
    is_decimal: bool,
    qdb_column_type: i32,
) -> bool {
    let res = (|| -> ParquetResult<bool> {
        use crate::parquet_read::ColumnFilterValues;
        use parquet2::schema::types::PhysicalType;

        if parquet_file_ptr.is_null() || parquet_file_size == 0 {
            return Ok(false);
        }
        if bloom_filter_offset == 0 {
            return Ok(false);
        }

        let file_data =
            unsafe { slice::from_raw_parts(parquet_file_ptr, parquet_file_size as usize) };
        let bitset =
            parquet2::bloom_filter::read_from_slice_at_offset(bloom_filter_offset, file_data)
                .unwrap_or(&[]);
        if bitset.is_empty() {
            return Ok(false);
        }

        let pt = match physical_type {
            0 => PhysicalType::Boolean,
            1 => PhysicalType::Int32,
            2 => PhysicalType::Int64,
            3 => PhysicalType::Int96,
            4 => PhysicalType::Float,
            5 => PhysicalType::Double,
            6 => PhysicalType::ByteArray,
            7 => PhysicalType::FixedLenByteArray(fixed_byte_len as usize),
            _ => return Ok(false),
        };

        let filter_desc = ColumnFilterValues {
            count: filter_count,
            ptr: filter_values_ptr,
            buf_end: filter_buf_end,
        };

        ParquetDecoder::all_values_absent_from_bloom(
            bitset,
            &pt,
            &filter_desc,
            has_nulls,
            is_decimal,
            qdb_column_type,
        )
    })();

    match res {
        Ok(absent) => absent,
        Err(mut err) => {
            err.add_context("error in ParquetMetaPartitionDecoder.checkBloomFilter");
            err.into_cairo_exception().throw(&mut env)
        }
    }
}

/// Row group filter pushdown using `_pm` metadata for statistics and
/// the parquet file for bloom filters. Same filter list format as
/// `PartitionDecoder.canSkipRowGroup`.
#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_ParquetMetaPartitionDecoder_canSkipRowGroup(
    mut env: JNIEnv,
    _class: JClass,
    parquet_file_ptr: *const u8,
    parquet_file_size: u64,
    parquet_meta_ptr: *const u8,
    parquet_meta_size: u64,
    row_group_index: u32,
    filters_ptr: *const ColumnFilterPacked,
    filter_count: u32,
    filter_buf_end: u64,
) -> bool {
    let res = pm_can_skip_row_group_impl(
        parquet_file_ptr,
        parquet_file_size,
        parquet_meta_ptr,
        parquet_meta_size,
        row_group_index,
        filters_ptr,
        filter_count,
        filter_buf_end,
    );
    match res {
        Ok(skip) => skip,
        Err(mut err) => {
            err.add_context("error in ParquetMetaPartitionDecoder.canSkipRowGroup");
            err.into_cairo_exception().throw(&mut env)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn pm_can_skip_row_group_impl(
    parquet_file_ptr: *const u8,
    parquet_file_size: u64,
    parquet_meta_ptr: *const u8,
    parquet_meta_size: u64,
    row_group_index: u32,
    filters_ptr: *const ColumnFilterPacked,
    filter_count: u32,
    filter_buf_end: u64,
) -> ParquetResult<bool> {
    use crate::parquet_metadata::reader::ParquetMetaReader;
    use crate::parquet_metadata::types::StatFlags;
    use crate::parquet_read::{
        ColumnFilterValues, FILTER_OP_BETWEEN, FILTER_OP_EQ, FILTER_OP_GE, FILTER_OP_GT,
        FILTER_OP_IS_NOT_NULL, FILTER_OP_IS_NULL, FILTER_OP_LE, FILTER_OP_LT,
    };
    use parquet2::schema::types::PhysicalType;
    use qdb_core::col_type::ColumnTypeTag;

    if parquet_meta_ptr.is_null() || parquet_meta_size == 0 {
        return Ok(false);
    }

    let parquet_meta_data =
        unsafe { slice::from_raw_parts(parquet_meta_ptr, parquet_meta_size as usize) };
    let parquet_meta_reader =
        ParquetMetaReader::from_file_size(parquet_meta_data, parquet_meta_size)?;

    if row_group_index >= parquet_meta_reader.row_group_count() {
        return Err(fmt_err!(
            InvalidType,
            "row group index {} out of range [0,{})",
            row_group_index,
            parquet_meta_reader.row_group_count()
        ));
    }

    let rg_block = parquet_meta_reader.row_group(row_group_index as usize)?;
    let col_count = parquet_meta_reader.column_count() as usize;
    let filters = if filters_ptr.is_null() {
        &[]
    } else {
        unsafe { slice::from_raw_parts(filters_ptr, filter_count as usize) }
    };
    let file_data = if parquet_file_ptr.is_null() || parquet_file_size == 0 {
        &[] as &[u8]
    } else {
        unsafe { slice::from_raw_parts(parquet_file_ptr, parquet_file_size as usize) }
    };

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

        // Read inline min/max stats from _pm. These are in QDB format.
        // For types where QDB format matches parquet physical format at the
        // same byte width (Int32→Int32, Int64→Int64, Float, Double), the stats
        // work directly with the existing comparison code.
        let min_bytes: Option<&[u8]> = if stat_flags.has_min_stat() && stat_flags.is_min_inlined() {
            let size =
                crate::parquet_metadata::types::decode_stat_sizes(chunk.stat_sizes).0 as usize;
            if size > 0 && size <= 8 {
                Some(unsafe {
                    slice::from_raw_parts(&chunk.min_stat as *const u64 as *const u8, size)
                })
            } else {
                None
            }
        } else {
            None
        };

        let max_bytes: Option<&[u8]> = if stat_flags.has_max_stat() && stat_flags.is_max_inlined() {
            let size =
                crate::parquet_metadata::types::decode_stat_sizes(chunk.stat_sizes).1 as usize;
            if size > 0 && size <= 8 {
                Some(unsafe {
                    slice::from_raw_parts(&chunk.max_stat as *const u64 as *const u8, size)
                })
            } else {
                None
            }
        } else {
            None
        };

        match op {
            FILTER_OP_EQ => {
                // Bloom filter check
                let bloom_off = chunk.bloom_filter_off.byte_offset();
                if bloom_off > 0 {
                    let bitset =
                        parquet2::bloom_filter::read_from_slice_at_offset(bloom_off, file_data)
                            .unwrap_or(&[]);
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
                }

                // Min/max stat check
                let is_ipv4 = col_type_tag == ColumnTypeTag::IPv4 as i32;
                let is_date = col_type_tag == ColumnTypeTag::Date as i32;
                // For _pm path (QDB-written files), unsigned int types don't exist
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
