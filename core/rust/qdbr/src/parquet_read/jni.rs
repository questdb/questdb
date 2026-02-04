use std::io::Cursor;
use std::slice;

use crate::allocator::QdbAllocator;
use crate::parquet::error::{ParquetErrorExt, ParquetResult};
use crate::parquet::qdb_metadata::ParquetFieldId;
use crate::parquet_read::decode::ParquetColumnIndex;
use crate::parquet_read::{
    ColumnChunkBuffers, ColumnChunkStats, ColumnFilterPacked, ColumnMeta, DecodeContext,
    ParquetDecoder, RowGroupBuffers, RowGroupStatBuffers,
};
use jni::objects::JClass;
use jni::JNIEnv;
use qdb_core::col_type::ColumnType;
use std::mem::{offset_of, size_of};

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_create(
    mut env: JNIEnv,
    _class: JClass,
    allocator: *const QdbAllocator,
    file_ptr: *const u8, // mmapped file's address
    file_size: u64,      // mmapped file's size
) -> *mut ParquetDecoder {
    let buf = unsafe { slice::from_raw_parts(file_ptr, file_size as usize) };
    let mut reader: Cursor<&[u8]> = Cursor::new(buf);
    let allocator = unsafe { &*allocator }.clone();
    match ParquetDecoder::read(allocator, &mut reader, file_size) {
        Ok(decoder) => Box::into_raw(Box::new(decoder)),
        Err(mut err) => {
            err.add_context(format!(
                "could not read parquet file with read size {file_size}"
            ));
            err.add_context("error in PartitionDecoder.create");
            err.into_cairo_exception().throw(&mut env)
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_destroy(
    _env: JNIEnv,
    _class: JClass,
    decoder: *mut ParquetDecoder,
) {
    if decoder.is_null() {
        panic!("decoder pointer is null");
    }

    drop(unsafe { Box::from_raw(decoder) });
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_createDecodeContext(
    _env: JNIEnv,
    _class: JClass,
    file_ptr: *const u8,
    file_size: u64,
) -> *mut DecodeContext {
    Box::into_raw(Box::new(DecodeContext::new(file_ptr, file_size)))
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_destroyDecodeContext(
    _env: JNIEnv,
    _class: JClass,
    ctx: *mut DecodeContext,
) {
    if !ctx.is_null() {
        drop(unsafe { Box::from_raw(ctx) });
    }
}

fn validate_jni_column_types(columns: &[(ParquetFieldId, ColumnType)]) -> ParquetResult<()> {
    for &(_, java_column_type) in columns {
        let code = java_column_type.code();
        let _col_type: ColumnType = code
            .try_into()
            .context("invalid column type passed across JNI layer")?;
    }
    Ok(())
}

#[repr(u8)]
enum DecodeMode {
    NoFilter = 0,
    FilterSkip = 1,
    FilterFillNulls = 2,
}

#[allow(clippy::too_many_arguments)]
fn decode_row_group_impl<const MODE: u8>(
    env: &mut JNIEnv,
    decoder: *const ParquetDecoder,
    ctx: *mut DecodeContext,
    row_group_bufs: *mut RowGroupBuffers,
    column_offset: usize,
    columns: *const (ParquetColumnIndex, ColumnType),
    column_count: u32,
    row_group_index: u32,
    row_group_lo: u32,
    row_group_hi: u32,
    filtered_rows_ptr: *const i64,
    filtered_rows_count: usize,
) -> u32 {
    assert!(!decoder.is_null(), "decoder pointer is null");
    assert!(!ctx.is_null(), "decode context pointer is null");
    assert!(
        !row_group_bufs.is_null(),
        "row group buffers pointer is null"
    );
    assert!(!columns.is_null(), "columns pointer is null");

    match MODE {
        x if x == DecodeMode::NoFilter as u8 => {}
        _ => {
            assert!(
                !filtered_rows_ptr.is_null(),
                "filtered rows pointer is null"
            );
        }
    }

    let decoder = unsafe { &*decoder };
    let ctx = unsafe { &mut *ctx };
    let row_group_bufs = unsafe { &mut *row_group_bufs };
    let columns = unsafe { slice::from_raw_parts(columns, column_count as usize) };
    let filtered_rows = if filtered_rows_ptr.is_null() {
        &[]
    } else {
        unsafe { slice::from_raw_parts(filtered_rows_ptr, filtered_rows_count) }
    };

    let res = validate_jni_column_types(columns).and_then(|()| match MODE {
        x if x == DecodeMode::NoFilter as u8 => decoder.decode_row_group(
            ctx,
            row_group_bufs,
            columns,
            row_group_index,
            row_group_lo,
            row_group_hi,
        ),
        x if x == DecodeMode::FilterSkip as u8 => decoder.decode_row_group_filtered::<false>(
            ctx,
            row_group_bufs,
            column_offset,
            columns,
            row_group_index,
            row_group_lo,
            row_group_hi,
            filtered_rows,
        ),
        _ => decoder.decode_row_group_filtered::<true>(
            ctx,
            row_group_bufs,
            column_offset,
            columns,
            row_group_index,
            row_group_lo,
            row_group_hi,
            filtered_rows,
        ),
    });

    match res {
        Ok(row_count) => row_count as u32,
        Err(mut err) => {
            let (context_msg, method_name) = match MODE {
                x if x == DecodeMode::NoFilter as u8 => (
                    format!("could not decode row group {row_group_index}"),
                    "error in PartitionDecoder.decodeRowGroup",
                ),
                x if x == DecodeMode::FilterSkip as u8 => (
                    format!("could not decode row group {row_group_index} with row filter"),
                    "error in PartitionDecoder.decodeRowGroupWithRowFilter",
                ),
                _ => (
                    format!(
                        "could not decode row group {row_group_index} with row filter (fill nulls)"
                    ),
                    "error in PartitionDecoder.decodeRowGroupWithRowFilterFillNulls",
                ),
            };
            err.add_context(context_msg);
            err.add_context(method_name);
            err.into_cairo_exception().throw(env)
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_canSkipRowGroup(
    mut env: JNIEnv,
    _class: JClass,
    decoder: *const ParquetDecoder,
    row_group_index: u32,
    file_ptr: *const u8,
    file_size: u64,
    filters: *const ColumnFilterPacked,
    filter_count: u32,
) -> bool {
    assert!(!decoder.is_null(), "decoder pointer is null");
    assert!(!filters.is_null(), "filters pointer is null");
    assert!(!file_ptr.is_null(), "file_ptr is null");

    let decoder = unsafe { &*decoder };
    let filters = unsafe { slice::from_raw_parts(filters, filter_count as usize) };
    let file_data = unsafe { slice::from_raw_parts(file_ptr, file_size as usize) };

    match decoder.can_skip_row_group(row_group_index, file_data, filters) {
        Ok(can_skip) => can_skip,
        Err(mut err) => {
            err.add_context(format!(
                "could not check bloom filter for row group {row_group_index}"
            ));
            err.add_context("error in PartitionDecoder.canSkipRowGroup");
            err.into_cairo_exception().throw(&mut env)
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_decodeRowGroup(
    mut env: JNIEnv,
    _class: JClass,
    decoder: *const ParquetDecoder,
    ctx: *mut DecodeContext,
    row_group_bufs: *mut RowGroupBuffers,
    columns: *const (ParquetColumnIndex, ColumnType),
    column_count: u32,
    row_group_index: u32,
    row_group_lo: u32,
    row_group_hi: u32,
) -> u32 {
    decode_row_group_impl::<{ DecodeMode::NoFilter as u8 }>(
        &mut env,
        decoder,
        ctx,
        row_group_bufs,
        0,
        columns,
        column_count,
        row_group_index,
        row_group_lo,
        row_group_hi,
        std::ptr::null(),
        0,
    )
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_decodeRowGroupWithRowFilter(
    mut env: JNIEnv,
    _class: JClass,
    decoder: *const ParquetDecoder,
    ctx: *mut DecodeContext,
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
    decode_row_group_impl::<{ DecodeMode::FilterSkip as u8 }>(
        &mut env,
        decoder,
        ctx,
        row_group_bufs,
        column_offset as usize,
        columns,
        column_count,
        row_group_index,
        row_group_lo,
        row_group_hi,
        filtered_rows_ptr,
        filtered_rows_size as usize,
    );
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_decodeRowGroupWithRowFilterFillNulls(
    mut env: JNIEnv,
    _class: JClass,
    decoder: *const ParquetDecoder,
    ctx: *mut DecodeContext,
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
    decode_row_group_impl::<{ DecodeMode::FilterFillNulls as u8 }>(
        &mut env,
        decoder,
        ctx,
        row_group_bufs,
        column_offset as usize,
        columns,
        column_count,
        row_group_index,
        row_group_lo,
        row_group_hi,
        filtered_rows_ptr,
        filtered_rows_size as usize,
    );
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_readRowGroupStats(
    mut env: JNIEnv,
    _class: JClass,
    decoder: *const ParquetDecoder,
    row_group_stat_bufs: *mut RowGroupStatBuffers,
    columns: *const (ParquetColumnIndex, ColumnType),
    column_count: u32,
    row_group_index: u32,
) {
    assert!(!decoder.is_null(), "decoder pointer is null");
    assert!(
        !row_group_stat_bufs.is_null(),
        "row group stat buffers pointer is null"
    );
    assert!(!columns.is_null(), "columns pointer is null");

    let decoder = unsafe { &*decoder };
    let row_group_stat_bufs = unsafe { &mut *row_group_stat_bufs };
    let columns = unsafe { slice::from_raw_parts(columns, column_count as usize) };

    let res = validate_jni_column_types(columns).and_then(|()| {
        decoder.read_column_chunk_stats(row_group_stat_bufs, columns, row_group_index)
    });

    match res {
        Ok(_) => {}
        Err(mut err) => {
            err.add_context(format!(
                "could not get row group stats in row group {row_group_index}"
            ));
            err.add_context("error in PartitionDecoder.readRowGroupStats");
            err.into_cairo_exception().throw(&mut env)
        }
    }
}

// See PartitionDecoder for more info on the returned value format.
#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_findRowGroupByTimestamp(
    mut env: JNIEnv,
    _class: JClass,
    decoder: *const ParquetDecoder,
    timestamp: i64,
    row_lo: usize,
    row_hi: usize,
    timestamp_index: u32,
) -> u64 {
    assert!(!decoder.is_null(), "decoder pointer is null");

    let decoder = unsafe { &*decoder };

    match decoder.find_row_group_by_timestamp(timestamp, row_lo, row_hi, timestamp_index) {
        Ok(row_group_index) => row_group_index,
        Err(mut err) => {
            err.add_context(format!("could not find row group by timestamp {timestamp}"));
            err.add_context("error in PartitionDecoder.findRowGroupByTimestamp");
            err.into_cairo_exception().throw(&mut env)
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_columnCountOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ParquetDecoder, col_count)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_rowCountOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ParquetDecoder, row_count)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_rowGroupCountOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ParquetDecoder, row_group_count)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_rowGroupSizesPtrOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ParquetDecoder, row_group_sizes_ptr)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_timestampIndexOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ParquetDecoder, timestamp_index)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_columnsPtrOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ParquetDecoder, columns_ptr)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_columnRecordTypeOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ColumnMeta, column_type)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_columnRecordNamePtrOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ColumnMeta, name_ptr)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_columnRecordNameSizeOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ColumnMeta, name_size)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_columnIdsOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ColumnMeta, id)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_columnRecordSize(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    size_of::<ColumnMeta>()
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupBuffers_create(
    _env: JNIEnv,
    _class: JClass,
    allocator: *const QdbAllocator,
) -> *mut RowGroupBuffers {
    let allocator = unsafe { &*allocator }.clone();
    Box::into_raw(Box::new(RowGroupBuffers::new(allocator)))
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupBuffers_destroy(
    _env: JNIEnv,
    _class: JClass,
    buffers: *mut RowGroupBuffers,
) {
    if buffers.is_null() {
        panic!("row group buffers pointer is null");
    }

    unsafe {
        drop(Box::from_raw(buffers));
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupBuffers_columnChunkBuffersSize(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    size_of::<ColumnChunkBuffers>()
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupBuffers_columnBuffersPtrOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(RowGroupBuffers, column_bufs_ptr)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupBuffers_chunkDataPtrOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ColumnChunkBuffers, data_ptr)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupBuffers_chunkDataSizeOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ColumnChunkBuffers, data_size)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupBuffers_chunkAuxPtrOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ColumnChunkBuffers, aux_ptr)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupBuffers_chunkAuxSizeOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ColumnChunkBuffers, aux_size)
}

// RowGroupStatsBuffers
#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupStatBuffers_create(
    _env: JNIEnv,
    _class: JClass,
    allocator: *const QdbAllocator,
) -> *mut RowGroupStatBuffers {
    let allocator = unsafe { &*allocator }.clone();
    Box::into_raw(Box::new(RowGroupStatBuffers::new(allocator)))
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupStatBuffers_destroy(
    _env: JNIEnv,
    _class: JClass,
    stat_buffers: *mut RowGroupStatBuffers,
) {
    if stat_buffers.is_null() {
        panic!("row group stat buffers pointer is null");
    }

    unsafe {
        drop(Box::from_raw(stat_buffers));
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupStatBuffers_buffersPtrOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(RowGroupStatBuffers, column_chunk_stats_ptr)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupStatBuffers_buffersSize(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    size_of::<ColumnChunkStats>()
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupStatBuffers_minValuePtrOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ColumnChunkStats, min_value_ptr)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupStatBuffers_minValueSizeOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ColumnChunkStats, min_value_size)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupStatBuffers_maxValuePtrOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ColumnChunkStats, max_value_ptr)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupStatBuffers_maxValueSizeOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ColumnChunkStats, max_value_size)
}
