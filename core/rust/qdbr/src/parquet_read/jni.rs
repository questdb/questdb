use jni::objects::JClass;
use jni::JNIEnv;
use std::mem::{offset_of, size_of};

use crate::parquet_read::{ColumnChunkBuffers, ColumnChunkStats, ColumnMeta, ParquetDecoder};
use crate::parquet_write::schema::ColumnType;
use crate::utils;

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_create(
    mut env: JNIEnv,
    _class: JClass,
    raw_fd: i32,
) -> *mut ParquetDecoder {
    match ParquetDecoder::read(utils::from_raw_file_descriptor(raw_fd)) {
        Ok(decoder) => Box::into_raw(Box::new(decoder)),
        Err(err) => utils::throw_java_ex(&mut env, "PartitionDecoder.create", &err),
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

    unsafe {
        drop(Box::from_raw(decoder));
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_decodeColumnChunk(
    mut env: JNIEnv,
    _class: JClass,
    decoder: *mut ParquetDecoder,
    row_group: usize,
    column: usize,
    to_column_type: i32,
) -> *const ColumnChunkBuffers {
    assert!(!decoder.is_null(), "decoder pointer is null");
    let decoder = unsafe { &mut *decoder };

    if column >= decoder.columns.len() {
        return utils::throw_java_ex(
            &mut env,
            "decodeColumnChunk",
            &format!(
                "column index {} out of range [0,{})",
                column,
                decoder.columns.len()
            ),
        );
    }

    let column_type = decoder.columns[column].typ;
    if Ok(column_type) != ColumnType::try_from(to_column_type) {
        return utils::throw_java_ex(
            &mut env,
            "decodeColumnChunk",
            &format!(
                "requested column type {} does not match file column type {:?}, column index: {}",
                to_column_type, column_type, column
            ),
        );
    } else {
        let column_file_index = decoder.columns[column].id;
        if let Err(err) =
            decoder.decode_column_chunk(row_group, column_file_index as usize, column, column_type)
        {
            return utils::throw_java_ex(&mut env, "decodeColumnChunk", &err);
        }
    }
    let buffer = &decoder.column_buffers[column];
    buffer as *const ColumnChunkBuffers
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_getColumnChunkStats(
    mut env: JNIEnv,
    _class: JClass,
    decoder: *mut ParquetDecoder,
    row_group: usize,
    column: usize,
) -> *const ColumnChunkStats {
    assert!(!decoder.is_null(), "decoder pointer is null");
    let decoder = unsafe { &mut *decoder };

    if column >= decoder.columns.len() {
        return utils::throw_java_ex(
            &mut env,
            "getColumnChunkStats",
            &format!(
                "column index {} out of range [0,{})",
                column,
                decoder.columns.len()
            ),
        );
    }

    let column_file_index = decoder.columns[column].id;
    decoder.update_column_chunk_stats(row_group, column_file_index as usize, column);
    let stats = &decoder.column_chunk_stats[column];
    stats as *const ColumnChunkStats
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
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_chunkDataPtrOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ColumnChunkBuffers, data_ptr)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_chunkAuxPtrOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ColumnChunkBuffers, aux_ptr)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_chunkRowGroupCountPtrOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ColumnChunkBuffers, row_count)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_chunkStatMinValuePtrOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ColumnChunkStats, min_value_ptr)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_chunkStatMinValueSizeOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ColumnChunkStats, min_value_size)
}
