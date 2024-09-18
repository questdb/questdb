use std::mem::{offset_of, size_of};
use std::ptr;
use std::slice;

use jni::objects::JClass;
use jni::JNIEnv;

use crate::parquet::col_type::ColumnType;
use crate::parquet::error::ParquetResult;
use crate::parquet_read::io::NonOwningFile;
use crate::parquet_read::{ColumnChunkBuffers, ColumnMeta, ParquetDecoder, RowGroupBuffers};

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_create(
    mut env: JNIEnv,
    _class: JClass,
    raw_fd: i32,
) -> *mut ParquetDecoder<NonOwningFile> {
    let reader = NonOwningFile::from_raw_fd(raw_fd);
    match ParquetDecoder::read(reader) {
        Ok(decoder) => Box::into_raw(Box::new(decoder)),
        Err(err) => throw_java_ex(&mut env, "PartitionDecoder.create", &err, ptr::null_mut()),
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_destroy(
    _env: JNIEnv,
    _class: JClass,
    decoder: *mut ParquetDecoder<NonOwningFile>,
) {
    if decoder.is_null() {
        panic!("decoder pointer is null");
    }

    drop(unsafe { Box::from_raw(decoder) });
}

fn map_column_types_from_ints(
    to_column_types: *const i32,
    col_count: usize,
) -> ParquetResult<Vec<Option<ColumnType>>> {
    let to_column_types_ints = unsafe { slice::from_raw_parts(to_column_types, col_count) };
    let mut to_column_types = Vec::with_capacity(col_count);
    for &v in to_column_types_ints.iter() {
        if v <= 0 {
            to_column_types.push(None);
        } else {
            let column_type: ColumnType = v.try_into()?;
            to_column_types.push(Some(column_type));
        }
    }
    Ok(to_column_types)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_decodeRowGroup(
    mut env: JNIEnv,
    _class: JClass,
    decoder: *mut ParquetDecoder<NonOwningFile>,
    row_group_bufs: *mut RowGroupBuffers,
    to_column_types: *const i32, // negative numbers for columns to skip
    row_group_index: u32,
) -> usize {
    assert!(!decoder.is_null(), "decoder pointer is null");
    assert!(
        !row_group_bufs.is_null(),
        "row group buffers pointer is null"
    );
    assert!(!to_column_types.is_null(), "column type pointer is null");

    let decoder = unsafe { &mut *decoder };
    let col_count = decoder.col_count as usize;
    let row_group_bufs = unsafe { &mut *row_group_bufs };
    let to_column_types = match map_column_types_from_ints(to_column_types, col_count) {
        Ok(v) => v,
        Err(err) => {
            return throw_java_ex(&mut env, "decodeRowGroup", &err, 0);
        }
    };

    match decoder.decode_row_group(row_group_bufs, &to_column_types, row_group_index) {
        Ok(row_count) => row_count,
        Err(err) => throw_java_ex(&mut env, "decodeRowGroup", &err, 0),
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_columnCountOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ParquetDecoder<NonOwningFile>, col_count)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_rowCountOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ParquetDecoder<NonOwningFile>, row_count)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_rowGroupCountOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ParquetDecoder<NonOwningFile>, row_group_count)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_rowGroupSizesPtrOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ParquetDecoder<NonOwningFile>, row_group_sizes_ptr)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_columnsPtrOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ParquetDecoder<NonOwningFile>, columns_ptr)
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
) -> *mut RowGroupBuffers {
    Box::into_raw(Box::new(RowGroupBuffers::new()))
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

fn throw_java_ex<T>(
    env: &mut JNIEnv,
    method_name: &str,
    err: &impl std::fmt::Debug,
    default_value: T,
) -> T {
    let msg = format!("error in {}: {:?}", method_name, err);
    env.throw_new("java/lang/RuntimeException", msg)
        .expect("failed to throw exception");
    default_value
}
