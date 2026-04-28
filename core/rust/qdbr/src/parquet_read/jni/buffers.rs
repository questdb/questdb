use crate::allocator::QdbAllocator;
use crate::ffi_panic_guard::{ffi_guard, ffi_guard_void};
use crate::parquet_read::{ColumnChunkBuffers, RowGroupBuffers};
use jni::objects::JClass;
use jni::JNIEnv;
use qdb_core::cairo::CairoException;
use std::mem::{offset_of, size_of};

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupBuffers_create(
    mut env: JNIEnv,
    _class: JClass,
    allocator: *const QdbAllocator,
) -> *mut RowGroupBuffers {
    ffi_guard(
        &mut env,
        "RowGroupBuffers.create",
        std::ptr::null_mut(),
        |env| {
            if allocator.is_null() {
                return CairoException::new("allocator pointer is null").throw(env);
            }
            let allocator = unsafe { &*allocator }.clone();
            Box::into_raw(Box::new(RowGroupBuffers::new(allocator)))
        },
    )
}

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupBuffers_destroy(
    mut env: JNIEnv,
    _class: JClass,
    buffers: *mut RowGroupBuffers,
) {
    ffi_guard_void(&mut env, "RowGroupBuffers.destroy", |_env| {
        if buffers.is_null() {
            return;
        }

        unsafe {
            drop(Box::from_raw(buffers));
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupBuffers_columnChunkBuffersSize(
    mut env: JNIEnv,
    _class: JClass,
) -> usize {
    ffi_guard(
        &mut env,
        "RowGroupBuffers.columnChunkBuffersSize",
        0,
        |_env| size_of::<ColumnChunkBuffers>(),
    )
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupBuffers_columnBuffersPtrOffset(
    mut env: JNIEnv,
    _class: JClass,
) -> usize {
    ffi_guard(
        &mut env,
        "RowGroupBuffers.columnBuffersPtrOffset",
        0,
        |_env| offset_of!(RowGroupBuffers, column_bufs_ptr),
    )
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupBuffers_chunkDataPtrOffset(
    mut env: JNIEnv,
    _class: JClass,
) -> usize {
    ffi_guard(&mut env, "RowGroupBuffers.chunkDataPtrOffset", 0, |_env| {
        offset_of!(ColumnChunkBuffers, data_ptr)
    })
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupBuffers_chunkDataSizeOffset(
    mut env: JNIEnv,
    _class: JClass,
) -> usize {
    ffi_guard(&mut env, "RowGroupBuffers.chunkDataSizeOffset", 0, |_env| {
        offset_of!(ColumnChunkBuffers, data_size)
    })
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupBuffers_chunkAuxPtrOffset(
    mut env: JNIEnv,
    _class: JClass,
) -> usize {
    ffi_guard(&mut env, "RowGroupBuffers.chunkAuxPtrOffset", 0, |_env| {
        offset_of!(ColumnChunkBuffers, aux_ptr)
    })
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_RowGroupBuffers_chunkAuxSizeOffset(
    mut env: JNIEnv,
    _class: JClass,
) -> usize {
    ffi_guard(&mut env, "RowGroupBuffers.chunkAuxSizeOffset", 0, |_env| {
        offset_of!(ColumnChunkBuffers, aux_size)
    })
}
