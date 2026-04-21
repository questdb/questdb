#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::io::Cursor;
use std::slice;

use crate::allocator::QdbAllocator;
use crate::parquet::error::{fmt_err, ParquetErrorExt, ParquetResult};
use crate::parquet::qdb_metadata::ParquetFieldId;
use crate::parquet_read::row_groups::ParquetColumnIndex;
use crate::parquet_read::{
    ColumnChunkBuffers, ColumnChunkStats, ColumnFilterPacked, ColumnMeta, DecodeContext,
    ParquetDecoder, RowGroupBuffers, RowGroupStatBuffers,
};
use jni::objects::JClass;
use jni::JNIEnv;
use qdb_core::col_type::ColumnType;
use std::mem::{offset_of, size_of};

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
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
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_destroy(
    _env: JNIEnv,
    _class: JClass,
    decoder: *mut ParquetDecoder,
) {
    if decoder.is_null() {
        return;
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
#[allow(clippy::not_unsafe_ptr_arg_deref)]
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
    let res = (|| -> ParquetResult<usize> {
        if decoder.is_null() {
            return Err(fmt_err!(InvalidLayout, "decoder pointer is null"));
        }
        if ctx.is_null() {
            return Err(fmt_err!(InvalidLayout, "decode context pointer is null"));
        }
        if row_group_bufs.is_null() {
            return Err(fmt_err!(InvalidLayout, "row group buffers pointer is null"));
        }
        if columns.is_null() {
            return Err(fmt_err!(InvalidLayout, "columns pointer is null"));
        }
        match MODE {
            x if x == DecodeMode::NoFilter as u8 => {}
            _ => {
                if filtered_rows_ptr.is_null() && filtered_rows_count > 0 {
                    return Err(fmt_err!(InvalidLayout, "filtered rows pointer is null"));
                }
                let row_group_span = row_group_hi.saturating_sub(row_group_lo) as usize;
                if filtered_rows_count > row_group_span {
                    return Err(fmt_err!(
                        InvalidLayout,
                        "filtered rows count {} exceeds row group span {}",
                        filtered_rows_count,
                        row_group_span
                    ));
                }
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

        validate_jni_column_types(columns).and_then(|()| match MODE {
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
        })
    })();

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
    filter_buf_end: u64,
) -> bool {
    let res = (|| -> ParquetResult<bool> {
        if decoder.is_null() {
            return Err(fmt_err!(InvalidLayout, "decoder pointer is null"));
        }
        if filters.is_null() && filter_count > 0 {
            return Err(fmt_err!(InvalidLayout, "filters pointer is null"));
        }
        if file_ptr.is_null() {
            return Err(fmt_err!(InvalidLayout, "file pointer is null"));
        }

        let decoder = unsafe { &*decoder };
        let filters = if filter_count == 0 {
            &[]
        } else {
            unsafe { slice::from_raw_parts(filters, filter_count as usize) }
        };
        let file_data = unsafe { slice::from_raw_parts(file_ptr, file_size as usize) };

        decoder.can_skip_row_group(row_group_index, file_data, filters, filter_buf_end)
    })();

    match res {
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

const QUESTDB_ENCODING_PLAIN: i32 = 1;
const QUESTDB_ENCODING_RLE_DICTIONARY: i32 = 2;
const QUESTDB_ENCODING_DELTA_LENGTH_BYTE_ARRAY: i32 = 3;
const QUESTDB_ENCODING_DELTA_BINARY_PACKED: i32 = 4;
const QUESTDB_ENCODING_BYTE_STREAM_SPLIT: i32 = 5;

// Parquet spec encoding ordinals (from parquet.thrift Encoding enum).
const PARQUET_ENCODING_PLAIN: i32 = 0;
const PARQUET_ENCODING_DELTA_BINARY_PACKED: i32 = 5;
const PARQUET_ENCODING_DELTA_LENGTH_BYTE_ARRAY: i32 = 6;
const PARQUET_ENCODING_RLE_DICTIONARY: i32 = 8;
const PARQUET_ENCODING_BYTE_STREAM_SPLIT: i32 = 9;

fn questdb_encoding_id_to_parquet_encoding(encoding_id: i32) -> ParquetResult<i32> {
    match encoding_id {
        QUESTDB_ENCODING_PLAIN => Ok(PARQUET_ENCODING_PLAIN),
        QUESTDB_ENCODING_RLE_DICTIONARY => Ok(PARQUET_ENCODING_RLE_DICTIONARY),
        QUESTDB_ENCODING_DELTA_LENGTH_BYTE_ARRAY => Ok(PARQUET_ENCODING_DELTA_LENGTH_BYTE_ARRAY),
        QUESTDB_ENCODING_DELTA_BINARY_PACKED => Ok(PARQUET_ENCODING_DELTA_BINARY_PACKED),
        QUESTDB_ENCODING_BYTE_STREAM_SPLIT => Ok(PARQUET_ENCODING_BYTE_STREAM_SPLIT),
        _ => Err(fmt_err!(
            InvalidType,
            "unsupported parquet encoding id {}",
            encoding_id
        )),
    }
}

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_rowGroupColumnHasEncoding(
    mut env: JNIEnv,
    _class: JClass,
    decoder: *const ParquetDecoder,
    row_group_index: u32,
    column_index: u32,
    encoding_id: i32,
) -> bool {
    let res = (|| -> ParquetResult<bool> {
        if decoder.is_null() {
            return Err(fmt_err!(InvalidLayout, "decoder pointer is null"));
        }

        let parquet_encoding = questdb_encoding_id_to_parquet_encoding(encoding_id)?;
        let decoder = unsafe { &*decoder };
        decoder.row_group_column_has_encoding(row_group_index, column_index, parquet_encoding)
    })();

    match res {
        Ok(has_encoding) => has_encoding,
        Err(mut err) => {
            err.add_context(format!(
                "could not read encodings for row group {}, column {}",
                row_group_index, column_index
            ));
            err.add_context("error in PartitionDecoder.rowGroupColumnHasEncoding");
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
    let filtered_rows_count = if filtered_rows_size < 0 {
        0usize
    } else {
        filtered_rows_size as usize
    };
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
        filtered_rows_count,
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
    let filtered_rows_count = if filtered_rows_size < 0 {
        0usize
    } else {
        filtered_rows_size as usize
    };
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
        filtered_rows_count,
    );
}

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_readRowGroupStats(
    mut env: JNIEnv,
    _class: JClass,
    decoder: *const ParquetDecoder,
    row_group_stat_bufs: *mut RowGroupStatBuffers,
    columns: *const (ParquetColumnIndex, ColumnType),
    column_count: u32,
    row_group_index: u32,
) {
    let res = (|| -> ParquetResult<()> {
        if decoder.is_null() {
            return Err(fmt_err!(InvalidLayout, "decoder pointer is null"));
        }
        if row_group_stat_bufs.is_null() {
            return Err(fmt_err!(
                InvalidLayout,
                "row group stat buffers pointer is null"
            ));
        }
        if columns.is_null() {
            return Err(fmt_err!(InvalidLayout, "columns pointer is null"));
        }

        let decoder = unsafe { &*decoder };
        let row_group_stat_bufs = unsafe { &mut *row_group_stat_bufs };
        let columns = unsafe { slice::from_raw_parts(columns, column_count as usize) };

        validate_jni_column_types(columns).and_then(|()| {
            decoder.read_column_chunk_stats(row_group_stat_bufs, columns, row_group_index)
        })
    })();

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

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_rowGroupMinTimestamp(
    mut env: JNIEnv,
    _class: JClass,
    decoder: *const ParquetDecoder,
    file_ptr: *const u8,
    file_size: u64,
    row_group_index: u32,
    timestamp_column_index: u32,
) -> i64 {
    let res = (|| -> ParquetResult<i64> {
        if decoder.is_null() {
            return Err(fmt_err!(InvalidLayout, "decoder pointer is null"));
        }
        if file_ptr.is_null() {
            return Err(fmt_err!(InvalidLayout, "file pointer is null"));
        }

        let decoder = unsafe { &*decoder };
        decoder.row_group_min_timestamp(
            file_ptr,
            file_size,
            row_group_index,
            timestamp_column_index,
        )
    })();

    match res {
        Ok(ts) => ts,
        Err(mut err) => {
            err.add_context(format!(
                "could not get min timestamp for row group {row_group_index}"
            ));
            err.add_context("error in PartitionDecoder.rowGroupMinTimestamp");
            err.into_cairo_exception().throw(&mut env)
        }
    }
}

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_rowGroupMaxTimestamp(
    mut env: JNIEnv,
    _class: JClass,
    decoder: *const ParquetDecoder,
    file_ptr: *const u8,
    file_size: u64,
    row_group_index: u32,
    timestamp_column_index: u32,
) -> i64 {
    let res = (|| -> ParquetResult<i64> {
        if decoder.is_null() {
            return Err(fmt_err!(InvalidLayout, "decoder pointer is null"));
        }
        if file_ptr.is_null() {
            return Err(fmt_err!(InvalidLayout, "file pointer is null"));
        }

        let decoder = unsafe { &*decoder };
        decoder.row_group_max_timestamp(
            file_ptr,
            file_size,
            row_group_index,
            timestamp_column_index,
        )
    })();

    match res {
        Ok(ts) => ts,
        Err(mut err) => {
            err.add_context(format!(
                "could not get max timestamp for row group {row_group_index}"
            ));
            err.add_context("error in PartitionDecoder.rowGroupMaxTimestamp");
            err.into_cairo_exception().throw(&mut env)
        }
    }
}

// See PartitionDecoder for more info on the returned value format.
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_findRowGroupByTimestamp(
    mut env: JNIEnv,
    _class: JClass,
    decoder: *const ParquetDecoder,
    file_ptr: *const u8,
    file_size: u64,
    timestamp: i64,
    row_lo: usize,
    row_hi: usize,
    timestamp_index: u32,
) -> u64 {
    let res = (|| -> ParquetResult<u64> {
        if decoder.is_null() {
            return Err(fmt_err!(InvalidLayout, "decoder pointer is null"));
        }
        if file_ptr.is_null() {
            return Err(fmt_err!(InvalidLayout, "file pointer is null"));
        }

        let decoder = unsafe { &*decoder };
        decoder.find_row_group_by_timestamp(
            file_ptr,
            file_size,
            timestamp,
            row_lo,
            row_hi,
            timestamp_index,
        )
    })();

    match res {
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
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_unusedBytesOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ParquetDecoder, unused_bytes)
}

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupBuffers_create(
    _env: JNIEnv,
    _class: JClass,
    allocator: *const QdbAllocator,
) -> *mut RowGroupBuffers {
    let allocator = unsafe { &*allocator }.clone();
    Box::into_raw(Box::new(RowGroupBuffers::new(allocator)))
}

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupBuffers_destroy(
    _env: JNIEnv,
    _class: JClass,
    buffers: *mut RowGroupBuffers,
) {
    if buffers.is_null() {
        return;
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
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupStatBuffers_create(
    _env: JNIEnv,
    _class: JClass,
    allocator: *const QdbAllocator,
) -> *mut RowGroupStatBuffers {
    let allocator = unsafe { &*allocator }.clone();
    Box::into_raw(Box::new(RowGroupStatBuffers::new(allocator)))
}

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupStatBuffers_destroy(
    _env: JNIEnv,
    _class: JClass,
    stat_buffers: *mut RowGroupStatBuffers,
) {
    if stat_buffers.is_null() {
        return;
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

/// Reads partition metadata (row_count and squash_tracker) from a parquet file's footer.
/// Writes row_count (i64) at dest_addr and squash_tracker (i64) at dest_addr+8.
/// Throws CairoException on invalid arguments or I/O errors.
#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_readPartitionMeta(
    mut env: JNIEnv,
    _class: JClass,
    file_path_ptr: *const u8,
    file_path_len: i32,
    dest_addr: i64,
) -> jni::sys::jboolean {
    // Validate arguments — invalid inputs throw Java exceptions.
    let res = (|| -> ParquetResult<()> {
        if file_path_ptr.is_null() {
            return Err(fmt_err!(InvalidLayout, "file_path_ptr is null"));
        }
        if file_path_len < 0 {
            return Err(fmt_err!(
                InvalidLayout,
                "file_path_len is negative: {}",
                file_path_len
            ));
        }
        if dest_addr == 0 {
            return Err(fmt_err!(InvalidLayout, "dest_addr is null"));
        }
        Ok(())
    })();

    if let Err(mut err) = res {
        err.add_context("error in PartitionDecoder.readPartitionMeta");
        return err.into_cairo_exception().throw(&mut env);
    }

    // Read file metadata — propagate I/O errors as CairoException to Java.
    let res = (|| -> ParquetResult<()> {
        let path_bytes = unsafe { slice::from_raw_parts(file_path_ptr, file_path_len as usize) };
        let path_str = std::str::from_utf8(path_bytes)
            .map_err(|e| fmt_err!(InvalidLayout, "invalid UTF-8 in file path: {}", e))?;
        let mut file = std::fs::File::open(path_str).map_err(|e| {
            fmt_err!(
                InvalidLayout,
                "cannot open parquet file \"{}\": {}",
                path_str,
                e
            )
        })?;
        let file_size = file
            .metadata()
            .map_err(|e| {
                fmt_err!(
                    InvalidLayout,
                    "cannot read file metadata \"{}\": {}",
                    path_str,
                    e
                )
            })?
            .len();
        let file_metadata =
            parquet2::read::read_metadata_with_size(&mut file, file_size).map_err(|e| {
                fmt_err!(
                    InvalidLayout,
                    "cannot read parquet footer \"{}\": {}",
                    path_str,
                    e
                )
            })?;

        let row_count = i64::try_from(file_metadata.num_rows).map_err(|_| {
            fmt_err!(
                InvalidLayout,
                "num_rows exceeds i64::MAX in \"{}\"",
                path_str
            )
        })?;
        let squash_tracker = match crate::parquet_read::meta::extract_qdb_meta(&file_metadata) {
            Ok(Some(meta)) => meta.squash_tracker,
            _ => -1i64,
        };

        let dest = dest_addr as *mut i64;
        unsafe {
            dest.write_unaligned(row_count);
            dest.add(1).write_unaligned(squash_tracker);
        }
        Ok(())
    })();

    if let Err(mut err) = res {
        err.add_context("error in PartitionDecoder.readPartitionMeta");
        return err.into_cairo_exception().throw(&mut env);
    }
    1 // JNI_TRUE
}

#[cfg(test)]
mod tests {
    use super::{
        questdb_encoding_id_to_parquet_encoding, PARQUET_ENCODING_BYTE_STREAM_SPLIT,
        PARQUET_ENCODING_DELTA_BINARY_PACKED, PARQUET_ENCODING_DELTA_LENGTH_BYTE_ARRAY,
        PARQUET_ENCODING_PLAIN, PARQUET_ENCODING_RLE_DICTIONARY,
        QUESTDB_ENCODING_BYTE_STREAM_SPLIT, QUESTDB_ENCODING_DELTA_BINARY_PACKED,
        QUESTDB_ENCODING_DELTA_LENGTH_BYTE_ARRAY, QUESTDB_ENCODING_PLAIN,
        QUESTDB_ENCODING_RLE_DICTIONARY,
    };
    use crate::parquet::error::ParquetErrorReason;

    #[test]
    fn encoding_id_to_parquet_encoding_maps_known_ids() {
        assert_eq!(
            questdb_encoding_id_to_parquet_encoding(QUESTDB_ENCODING_PLAIN).unwrap(),
            PARQUET_ENCODING_PLAIN,
        );
        assert_eq!(
            questdb_encoding_id_to_parquet_encoding(QUESTDB_ENCODING_RLE_DICTIONARY).unwrap(),
            PARQUET_ENCODING_RLE_DICTIONARY,
        );
        assert_eq!(
            questdb_encoding_id_to_parquet_encoding(QUESTDB_ENCODING_DELTA_LENGTH_BYTE_ARRAY)
                .unwrap(),
            PARQUET_ENCODING_DELTA_LENGTH_BYTE_ARRAY,
        );
        assert_eq!(
            questdb_encoding_id_to_parquet_encoding(QUESTDB_ENCODING_DELTA_BINARY_PACKED).unwrap(),
            PARQUET_ENCODING_DELTA_BINARY_PACKED,
        );
        assert_eq!(
            questdb_encoding_id_to_parquet_encoding(QUESTDB_ENCODING_BYTE_STREAM_SPLIT).unwrap(),
            PARQUET_ENCODING_BYTE_STREAM_SPLIT,
        );
    }

    #[test]
    fn encoding_id_to_parquet_encoding_rejects_unknown() {
        for bad_id in [0, 6, 99, -1, i32::MIN, i32::MAX] {
            let err = questdb_encoding_id_to_parquet_encoding(bad_id)
                .err()
                .unwrap_or_else(|| panic!("expected error for encoding id {bad_id}"));
            assert!(
                matches!(err.reason(), ParquetErrorReason::InvalidType),
                "expected InvalidType for encoding id {bad_id}, got {:?}",
                err.reason()
            );
            let msg = err.to_string();
            assert!(
                msg.contains("unsupported parquet encoding id"),
                "error message should mention 'unsupported parquet encoding id', got: {msg}"
            );
            assert!(
                msg.contains(&bad_id.to_string()),
                "error message should include the rejected id {bad_id}, got: {msg}"
            );
        }
    }
}
