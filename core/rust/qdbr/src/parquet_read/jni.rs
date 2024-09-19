use std::fs::File;
use std::mem::{offset_of, size_of};
use std::ptr;
use std::slice;

use jni::objects::JClass;
use jni::JNIEnv;

use crate::parquet_read::{ColumnChunkBuffers, ColumnMeta, ParquetDecoder, RowGroupBuffers};
use crate::parquet_write::schema::ColumnType;

fn from_raw_file_descriptor(raw: i32) -> File {
    unsafe {
        #[cfg(unix)]
        {
            use std::os::unix::io::{FromRawFd, RawFd};
            File::from_raw_fd(raw as RawFd)
        }

        #[cfg(windows)]
        {
            use std::os::windows::io::{FromRawHandle, RawHandle};
            File::from_raw_handle(raw as usize as RawHandle)
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_create(
    mut env: JNIEnv,
    _class: JClass,
    raw_fd: i32,
) -> *mut ParquetDecoder {
    match ParquetDecoder::read(from_raw_file_descriptor(raw_fd)) {
        Ok(decoder) => Box::into_raw(Box::new(decoder)),
        Err(err) => throw_java_ex(&mut env, "PartitionDecoder.create", &err, ptr::null_mut()),
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
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_decodeRowGroup(
    mut env: JNIEnv,
    _class: JClass,
    decoder: *mut ParquetDecoder,
    row_group_bufs: *mut RowGroupBuffers,
    // Contains [parquet_column_index, column_type] pairs.
    columns: *const i32,
    column_count: i32,
    row_group_index: i32,
) -> usize {
    assert!(!decoder.is_null(), "decoder pointer is null");
    assert!(
        !row_group_bufs.is_null(),
        "row group buffers pointer is null"
    );
    assert!(!columns.is_null(), "columns pointer is null");

    let decoder = unsafe { &mut *decoder };
    let row_group_bufs = unsafe { &mut *row_group_bufs };
    let column_count = column_count as usize;
    let columns = unsafe { slice::from_raw_parts(columns, 2 * column_count) };

    if row_group_index >= decoder.row_group_count {
        return throw_java_ex(
            &mut env,
            "decodeRowGroup",
            &format!(
                "row group index {} out of range [0,{})",
                row_group_index, decoder.row_group_count
            ),
            0,
        );
    }

    if row_group_bufs.column_bufs.len() < column_count {
        row_group_bufs
            .column_bufs
            .resize_with(column_count, ColumnChunkBuffers::new);
    }

    let mut row_group_size = 0;
    for i in 0..column_count {
        let parquet_column_idx = columns[2 * i] as usize;
        let to_column_type = columns[2 * i + 1];
        let column_type = decoder.columns[parquet_column_idx].typ;
        if Ok(column_type) != ColumnType::try_from(to_column_type) {
            return throw_java_ex(
                &mut env,
                "decodeRowGroup",
                &format!(
                    "requested column type {} does not match file column type {:?}, column index: {}",
                    to_column_type, column_type, parquet_column_idx
                ),
                0,
            );
        } else {
            let column_chunk_bufs = &mut row_group_bufs.column_bufs[i];
            let column_file_index = decoder.columns[parquet_column_idx].id;

            match decoder.decode_column_chunk(
                column_chunk_bufs,
                row_group_index as usize,
                column_file_index as usize,
                column_type,
            ) {
                Ok(column_chunk_size) => {
                    if row_group_size > 0 && row_group_size != column_chunk_size {
                        return throw_java_ex(
                            &mut env,
                            "decodeRowGroup",
                            &format!(
                                "column chunk size {} does not match previous size {}",
                                column_chunk_size, row_group_size
                            ),
                            0,
                        );
                    }
                    row_group_size = column_chunk_size;
                }
                Err(err) => {
                    return throw_java_ex(&mut env, "decodeRowGroup", &err, 0);
                }
            }
        }
    }

    row_group_bufs.refresh_ptrs();
    row_group_size
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
