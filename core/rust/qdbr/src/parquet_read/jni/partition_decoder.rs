use std::io::Cursor;
use std::slice;

use crate::allocator::QdbAllocator;
use crate::ffi_panic_guard::{ffi_guard, ffi_guard_void};
use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_read::jni::{validate_jni_column_types, DecodeMode};
use crate::parquet_read::row_groups::ParquetColumnIndex;
use crate::parquet_read::{
    ColumnFilterPacked, ColumnMeta, DecodeContext, ParquetDecoder, RowGroupBuffers,
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
    ffi_guard(
        &mut env,
        "PartitionDecoder.create",
        std::ptr::null_mut(),
        |env| {
            if allocator.is_null() {
                let err = fmt_err!(InvalidLayout, "allocator pointer is null");
                return err.into_cairo_exception().throw(env);
            }
            if file_ptr.is_null() {
                let err = fmt_err!(InvalidLayout, "file pointer is null");
                return err.into_cairo_exception().throw(env);
            }
            let file_size_usize = match usize::try_from(file_size) {
                Ok(v) => v,
                Err(_) => {
                    let err = fmt_err!(
                        InvalidLayout,
                        "file size {} exceeds addressable range",
                        file_size
                    );
                    return err.into_cairo_exception().throw(env);
                }
            };
            let buf = unsafe { slice::from_raw_parts(file_ptr, file_size_usize) };
            let mut reader: Cursor<&[u8]> = Cursor::new(buf);
            let allocator = unsafe { &*allocator }.clone();
            match ParquetDecoder::read(allocator, &mut reader, file_size) {
                Ok(decoder) => Box::into_raw(Box::new(decoder)),
                Err(mut err) => {
                    err.add_context(format!(
                        "could not read parquet file with read size {file_size}"
                    ));
                    err.add_context("error in PartitionDecoder.create");
                    err.into_cairo_exception().throw(env)
                }
            }
        },
    )
}

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_destroy(
    mut env: JNIEnv,
    _class: JClass,
    decoder: *mut ParquetDecoder,
) {
    ffi_guard_void(&mut env, "PartitionDecoder.destroy", |_env| {
        if decoder.is_null() {
            return;
        }

        drop(unsafe { Box::from_raw(decoder) });
    })
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_createDecodeContext(
    mut env: JNIEnv,
    _class: JClass,
    file_ptr: *const u8,
    file_size: u64,
) -> *mut DecodeContext {
    ffi_guard(
        &mut env,
        "PartitionDecoder.createDecodeContext",
        std::ptr::null_mut(),
        |_env| Box::into_raw(Box::new(DecodeContext::new(file_ptr, file_size))),
    )
}

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_destroyDecodeContext(
    mut env: JNIEnv,
    _class: JClass,
    ctx: *mut DecodeContext,
) {
    ffi_guard_void(&mut env, "PartitionDecoder.destroyDecodeContext", |_env| {
        if !ctx.is_null() {
            drop(unsafe { Box::from_raw(ctx) });
        }
    })
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
    ffi_guard(&mut env, "PartitionDecoder.canSkipRowGroup", false, |env| {
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
                err.into_cairo_exception().throw(env)
            }
        }
    })
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
    ffi_guard(
        &mut env,
        "PartitionDecoder.rowGroupColumnHasEncoding",
        false,
        |env| {
            let res = (|| -> ParquetResult<bool> {
                if decoder.is_null() {
                    return Err(fmt_err!(InvalidLayout, "decoder pointer is null"));
                }

                let parquet_encoding = questdb_encoding_id_to_parquet_encoding(encoding_id)?;
                let decoder = unsafe { &*decoder };
                decoder.row_group_column_has_encoding(
                    row_group_index,
                    column_index,
                    parquet_encoding,
                )
            })();

            match res {
                Ok(has_encoding) => has_encoding,
                Err(mut err) => {
                    err.add_context(format!(
                        "could not read encodings for row group {}, column {}",
                        row_group_index, column_index
                    ));
                    err.add_context("error in PartitionDecoder.rowGroupColumnHasEncoding");
                    err.into_cairo_exception().throw(env)
                }
            }
        },
    )
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
    ffi_guard(&mut env, "PartitionDecoder.decodeRowGroup", 0, |env| {
        decode_row_group_impl::<{ DecodeMode::NoFilter as u8 }>(
            env,
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
    })
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
    ffi_guard_void(
        &mut env,
        "PartitionDecoder.decodeRowGroupWithRowFilter",
        |env| {
            let filtered_rows_count = if filtered_rows_size < 0 {
                0usize
            } else {
                filtered_rows_size as usize
            };
            decode_row_group_impl::<{ DecodeMode::FilterSkip as u8 }>(
                env,
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
        },
    )
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
    ffi_guard_void(
        &mut env,
        "PartitionDecoder.decodeRowGroupWithRowFilterFillNulls",
        |env| {
            let filtered_rows_count = if filtered_rows_size < 0 {
                0usize
            } else {
                filtered_rows_size as usize
            };
            decode_row_group_impl::<{ DecodeMode::FilterFillNulls as u8 }>(
                env,
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
        },
    )
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
    ffi_guard(
        &mut env,
        "PartitionDecoder.rowGroupMinTimestamp",
        -1,
        |env| {
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
                    err.into_cairo_exception().throw(env)
                }
            }
        },
    )
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
    ffi_guard(
        &mut env,
        "PartitionDecoder.rowGroupMaxTimestamp",
        -1,
        |env| {
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
                    err.into_cairo_exception().throw(env)
                }
            }
        },
    )
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
    ffi_guard(
        &mut env,
        "PartitionDecoder.findRowGroupByTimestamp",
        0,
        |env| {
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
                    err.into_cairo_exception().throw(env)
                }
            }
        },
    )
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_columnCountOffset(
    mut env: JNIEnv,
    _class: JClass,
) -> usize {
    ffi_guard(&mut env, "PartitionDecoder.columnCountOffset", 0, |_env| {
        offset_of!(ParquetDecoder, col_count)
    })
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_rowCountOffset(
    mut env: JNIEnv,
    _class: JClass,
) -> usize {
    ffi_guard(&mut env, "PartitionDecoder.rowCountOffset", 0, |_env| {
        offset_of!(ParquetDecoder, row_count)
    })
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_rowGroupCountOffset(
    mut env: JNIEnv,
    _class: JClass,
) -> usize {
    ffi_guard(
        &mut env,
        "PartitionDecoder.rowGroupCountOffset",
        0,
        |_env| offset_of!(ParquetDecoder, row_group_count),
    )
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_rowGroupSizesPtrOffset(
    mut env: JNIEnv,
    _class: JClass,
) -> usize {
    ffi_guard(
        &mut env,
        "PartitionDecoder.rowGroupSizesPtrOffset",
        0,
        |_env| offset_of!(ParquetDecoder, row_group_sizes_ptr),
    )
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_timestampIndexOffset(
    mut env: JNIEnv,
    _class: JClass,
) -> usize {
    ffi_guard(
        &mut env,
        "PartitionDecoder.timestampIndexOffset",
        0,
        |_env| offset_of!(ParquetDecoder, timestamp_index),
    )
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_columnsPtrOffset(
    mut env: JNIEnv,
    _class: JClass,
) -> usize {
    ffi_guard(&mut env, "PartitionDecoder.columnsPtrOffset", 0, |_env| {
        offset_of!(ParquetDecoder, columns_ptr)
    })
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_columnRecordTypeOffset(
    mut env: JNIEnv,
    _class: JClass,
) -> usize {
    ffi_guard(
        &mut env,
        "PartitionDecoder.columnRecordTypeOffset",
        0,
        |_env| offset_of!(ColumnMeta, column_type),
    )
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_columnRecordNamePtrOffset(
    mut env: JNIEnv,
    _class: JClass,
) -> usize {
    ffi_guard(
        &mut env,
        "PartitionDecoder.columnRecordNamePtrOffset",
        0,
        |_env| offset_of!(ColumnMeta, name_ptr),
    )
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_columnRecordNameSizeOffset(
    mut env: JNIEnv,
    _class: JClass,
) -> usize {
    ffi_guard(
        &mut env,
        "PartitionDecoder.columnRecordNameSizeOffset",
        0,
        |_env| offset_of!(ColumnMeta, name_size),
    )
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_columnIdsOffset(
    mut env: JNIEnv,
    _class: JClass,
) -> usize {
    ffi_guard(&mut env, "PartitionDecoder.columnIdsOffset", 0, |_env| {
        offset_of!(ColumnMeta, id)
    })
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_columnRecordSize(
    mut env: JNIEnv,
    _class: JClass,
) -> usize {
    ffi_guard(&mut env, "PartitionDecoder.columnRecordSize", 0, |_env| {
        size_of::<ColumnMeta>()
    })
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_unusedBytesOffset(
    mut env: JNIEnv,
    _class: JClass,
) -> usize {
    ffi_guard(&mut env, "PartitionDecoder.unusedBytesOffset", 0, |_env| {
        offset_of!(ParquetDecoder, unused_bytes)
    })
}
