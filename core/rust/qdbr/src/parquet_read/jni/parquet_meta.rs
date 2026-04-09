use std::slice;

use crate::allocator::QdbAllocator;
use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_metadata::reader::ParquetMetaReader;
use crate::parquet_read::jni::validate_jni_column_types;
use crate::parquet_read::row_groups::ParquetColumnIndex;
use crate::parquet_read::{DecodeContext, RowGroupBuffers};
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
        return Err(fmt_err!(InvalidType, "_pm pointer is null or size is zero"));
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
    let col_pairs = if columns.is_null() {
        &[]
    } else {
        unsafe { slice::from_raw_parts(columns, column_count as usize) }
    };
    let filtered_rows = if filtered_rows_ptr.is_null() {
        &[]
    } else {
        unsafe { slice::from_raw_parts(filtered_rows_ptr, filtered_rows_count) }
    };

    crate::parquet_read::parquet_meta_decode::decode_row_group_filtered::<FILL_NULLS>(
        ctx,
        row_group_bufs,
        file_data,
        &parquet_meta_reader,
        column_offset,
        col_pairs,
        row_group_index as usize,
        row_group_lo as usize,
        row_group_hi as usize,
        filtered_rows,
    )
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
        return Err(fmt_err!(InvalidType, "_pm pointer is null or size is zero"));
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
    let col_pairs = if columns.is_null() {
        &[]
    } else {
        unsafe { slice::from_raw_parts(columns, column_count as usize) }
    };

    crate::parquet_read::parquet_meta_decode::decode_row_group(
        ctx,
        row_group_bufs,
        file_data,
        &parquet_meta_reader,
        col_pairs,
        row_group_index as usize,
        row_group_lo as usize,
        row_group_hi as usize,
    )
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
    if parquet_meta_ptr.is_null() || parquet_meta_size == 0 {
        return Err(fmt_err!(InvalidType, "_pm pointer is null or size is zero"));
    }

    let parquet_meta_data =
        unsafe { slice::from_raw_parts(parquet_meta_ptr, parquet_meta_size as usize) };
    let parquet_meta_reader =
        ParquetMetaReader::from_file_size(parquet_meta_data, parquet_meta_size)?;

    let decode_ts = |rg_idx, ts_col, row_lo, row_hi| {
        decode_single_ts_from_pm(
            allocator,
            parquet_file_ptr,
            parquet_file_size,
            &parquet_meta_reader,
            rg_idx,
            ts_col,
            row_lo,
            row_hi,
        )
    };

    crate::parquet_read::parquet_meta_decode::find_row_group_by_timestamp(
        &parquet_meta_reader,
        timestamp,
        row_lo,
        row_hi,
        ts_col,
        decode_ts,
    )
}

/// Decode a single timestamp value from the parquet file using `_pm` metadata.
#[allow(clippy::too_many_arguments)]
fn decode_single_ts_from_pm(
    allocator: *const QdbAllocator,
    parquet_file_ptr: *const u8,
    parquet_file_size: u64,
    parquet_meta_reader: &ParquetMetaReader,
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
