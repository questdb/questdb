use std::sync::Arc;

use qdb_core::{
    cairo::{CairoException, ResultToCairoException},
    types::{IdNumber, SegmentId, WalId},
    wal_lock,
};

macro_rules! get_wal_lock {
    ($env: ident, $ptr: expr) => {
        match unsafe { $ptr.as_ref() } {
            None => {
                return CairoException::new("WalLock pointer is null").throw(&mut $env);
            }
            Some(wl) => wl.as_ref(),
        }
    };
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_wal_QdbrWalLocker_create(
    _env: jni::JNIEnv,
    _class: jni::objects::JClass,
) -> *mut Arc<wal_lock::WalLock> {
    let wl = wal_lock::WalLock::new();
    let arc_wl = Arc::new(wl);
    Box::into_raw(Box::new(arc_wl))
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_wal_QdbrWalLocker_destroy(
    _env: jni::JNIEnv,
    _class: jni::objects::JClass,
    ptr: *mut Arc<wal_lock::WalLock>,
) {
    if ptr.is_null() {
        return;
    }
    // Safety: the caller is responsible for ensuring the pointer is valid.
    unsafe {
        let _ = Box::from_raw(ptr);
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_wal_QdbrWalLocker_isSegmentLocked0(
    mut env: jni::JNIEnv,
    _class: jni::objects::JClass,
    ptr: *mut Arc<wal_lock::WalLock>,
    table_dir_name_ptr: *const u8,
    table_dir_name_size: jni::sys::jint,
    wal_id: jni::sys::jint,
    segment_id: jni::sys::jint,
) -> jni::sys::jboolean {
    let wal_lock = get_wal_lock!(env, ptr);
    let table_dir_name = unsafe {
        str::from_utf8_unchecked(std::slice::from_raw_parts(
            table_dir_name_ptr,
            table_dir_name_size as usize,
        ))
    };
    let wal_id = WalId::new(wal_id as u32);
    let segment_id = SegmentId::new(segment_id);
    let is_locked = wal_lock.is_segment_locked(table_dir_name, wal_id, segment_id);
    if is_locked {
        jni::sys::JNI_TRUE
    } else {
        jni::sys::JNI_FALSE
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_wal_QdbrWalLocker_isWalLocked0(
    mut env: jni::JNIEnv,
    _class: jni::objects::JClass,
    ptr: *mut Arc<wal_lock::WalLock>,
    table_dir_name_ptr: *const u8,
    table_dir_name_size: jni::sys::jint,
    wal_id: jni::sys::jint,
) -> jni::sys::jboolean {
    let wal_lock = get_wal_lock!(env, ptr);
    let table_dir_name = unsafe {
        str::from_utf8_unchecked(std::slice::from_raw_parts(
            table_dir_name_ptr,
            table_dir_name_size as usize,
        ))
    };
    let wal_id = WalId::new(wal_id as u32);
    let is_locked = wal_lock.is_locked(table_dir_name, wal_id);
    if is_locked {
        jni::sys::JNI_TRUE
    } else {
        jni::sys::JNI_FALSE
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_wal_QdbrWalLocker_lockPurge0(
    mut env: jni::JNIEnv,
    _class: jni::objects::JClass,
    ptr: *mut Arc<wal_lock::WalLock>,
    table_dir_name_ptr: *const u8,
    table_dir_name_size: jni::sys::jint,
    wal_id: jni::sys::jint,
) -> jni::sys::jint {
    let wal_lock = get_wal_lock!(env, ptr);
    let table_dir_name = unsafe {
        str::from_utf8_unchecked(std::slice::from_raw_parts(
            table_dir_name_ptr,
            table_dir_name_size as usize,
        ))
    };
    let wal_id = WalId::new(wal_id as u32);
    wal_lock
        .lock_for_purge(table_dir_name, wal_id)
        .map(|min_segment_id| {
            min_segment_id.map_or(jni::sys::jint::MAX, |sid| sid.value() as jni::sys::jint)
        })
        .or_throw_to_java(&mut env)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_wal_QdbrWalLocker_unlockPurge0(
    mut env: jni::JNIEnv,
    _class: jni::objects::JClass,
    ptr: *mut Arc<wal_lock::WalLock>,
    table_dir_name_ptr: *const u8,
    table_dir_name_size: jni::sys::jint,
    wal_id: jni::sys::jint,
) {
    let wal_lock = get_wal_lock!(env, ptr);
    let table_dir_name = unsafe {
        str::from_utf8_unchecked(std::slice::from_raw_parts(
            table_dir_name_ptr,
            table_dir_name_size as usize,
        ))
    };
    let wal_id = WalId::new(wal_id as u32);
    wal_lock
        .unlock_purge(table_dir_name, wal_id)
        .or_throw_to_java(&mut env);
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_wal_QdbrWalLocker_lockWriter0(
    mut env: jni::JNIEnv,
    _class: jni::objects::JClass,
    ptr: *mut Arc<wal_lock::WalLock>,
    table_dir_name_ptr: *const u8,
    table_dir_name_size: jni::sys::jint,
    wal_id: jni::sys::jint,
    min_segment_id: jni::sys::jint,
) {
    let wal_lock = get_wal_lock!(env, ptr);
    let table_dir_name = unsafe {
        str::from_utf8_unchecked(std::slice::from_raw_parts(
            table_dir_name_ptr,
            table_dir_name_size as usize,
        ))
    };
    let wal_id = WalId::new(wal_id as u32);
    let min_segment_id = SegmentId::new(min_segment_id);
    wal_lock
        .lock_for_write(table_dir_name, wal_id, min_segment_id)
        .or_throw_to_java(&mut env);
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_wal_QdbrWalLocker_unlockWriter0(
    mut env: jni::JNIEnv,
    _class: jni::objects::JClass,
    ptr: *mut Arc<wal_lock::WalLock>,
    table_dir_name_ptr: *const u8,
    table_dir_name_size: jni::sys::jint,
    wal_id: jni::sys::jint,
) {
    let wal_lock = get_wal_lock!(env, ptr);
    let table_dir_name = unsafe {
        str::from_utf8_unchecked(std::slice::from_raw_parts(
            table_dir_name_ptr,
            table_dir_name_size as usize,
        ))
    };
    let wal_id = WalId::new(wal_id as u32);
    wal_lock
        .unlock_write(table_dir_name, wal_id)
        .or_throw_to_java(&mut env);
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_wal_QdbrWalLocker_setWalSegmentMinId0(
    mut env: jni::JNIEnv,
    _class: jni::objects::JClass,
    ptr: *mut Arc<wal_lock::WalLock>,
    table_dir_name_ptr: *const u8,
    table_dir_name_size: jni::sys::jint,
    wal_id: jni::sys::jint,
    min_segment_id: jni::sys::jint,
) {
    let wal_lock = get_wal_lock!(env, ptr);
    let table_dir_name = unsafe {
        str::from_utf8_unchecked(std::slice::from_raw_parts(
            table_dir_name_ptr,
            table_dir_name_size as usize,
        ))
    };
    let wal_id = WalId::new(wal_id as u32);
    let min_segment_id = SegmentId::new(min_segment_id);
    wal_lock
        .update_writer_min_segment_id(table_dir_name, wal_id, min_segment_id)
        .or_throw_to_java(&mut env);
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_wal_QdbrWalLocker_clearTable0(
    mut env: jni::JNIEnv,
    _class: jni::objects::JClass,
    ptr: *mut Arc<wal_lock::WalLock>,
    table_dir_name_ptr: *const u8,
    table_dir_name_size: jni::sys::jint,
) {
    let wal_lock = get_wal_lock!(env, ptr);
    let table_dir_name = unsafe {
        str::from_utf8_unchecked(std::slice::from_raw_parts(
            table_dir_name_ptr,
            table_dir_name_size as usize,
        ))
    };
    wal_lock.clear_table(table_dir_name);
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_wal_QdbrWalLocker_clear0(
    mut env: jni::JNIEnv,
    _class: jni::objects::JClass,
    ptr: *mut Arc<wal_lock::WalLock>,
) {
    let wal_lock = get_wal_lock!(env, ptr);
    wal_lock.clear();
}
