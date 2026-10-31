use crate::allocator::QdbAllocator;
use crate::parquet_read::{ColumnChunkBuffers, ExternalColumnView, RowGroupBuffers};
use jni::objects::JClass;
use jni::JNIEnv;
use qdb_core::cairo::CairoException;
use std::mem::{offset_of, size_of};
use std::slice;

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupBuffers_create(
    mut env: JNIEnv,
    _class: JClass,
    allocator: *const QdbAllocator,
) -> *mut RowGroupBuffers {
    let env = &mut env;
    if allocator.is_null() {
        return CairoException::new("allocator pointer is null").throw(env);
    }
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
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupBuffers_installExternalColumnViews(
    mut env: JNIEnv,
    _class: JClass,
    buffers: *mut RowGroupBuffers,
    column_offset: i32,
    views_addr: i64,
    view_count: i32,
) {
    if buffers.is_null()
        || column_offset < 0
        || view_count < 0
        || (view_count > 0 && views_addr == 0)
        || !(views_addr as usize).is_multiple_of(std::mem::align_of::<ExternalColumnView>())
    {
        CairoException::new("invalid external column view arguments").throw::<()>(&mut env);
        return;
    }
    // SAFETY: Java supplies `view_count` four-long descriptors in a live direct
    // buffer for the duration of this call; alignment and null are checked.
    let views = unsafe {
        slice::from_raw_parts(views_addr as *const ExternalColumnView, view_count as usize)
    };
    // SAFETY: `buffers` is a live RowGroupBuffers pointer created by `create`;
    // Java serialises mutation of one instance.
    let buffers = unsafe { &mut *buffers };
    if let Err(error) = buffers.install_external_views(column_offset as usize, views) {
        CairoException::new(error.to_string()).throw::<()>(&mut env);
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
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupBuffers_chunkPageBuffersSizeOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ColumnChunkBuffers, page_buffers_size)
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

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupBuffers_chunkColumnTopOffset(
    _env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ColumnChunkBuffers, column_top)
}
