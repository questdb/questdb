//! JNI bindings for zstd compression used by the QWP egress protocol.
//!
//! Exposes a thin wrapper over zstd's context-based API:
//!   * ZSTD_CCtx per server-side egress connection, level fixed at create time.
//!   * ZSTD_DCtx per client IoThread.
//!
//! All entry points operate on raw native pointers so Java callers can pass
//! direct-buffer addresses with zero copies. Return values:
//!   * non-negative  -- compressed / decompressed byte count
//!   * negative      -- zstd error code, negated for transport across JNI

use jni::objects::JClass;
use jni::sys::{jint, jlong};
use jni::JNIEnv;
use zstd::zstd_safe;
use zstd::zstd_safe::{CCtx, CParameter, DCtx};

#[no_mangle]
pub extern "system" fn Java_io_questdb_std_Zstd_createCCtx(
    _env: JNIEnv,
    _class: JClass,
    level: jint,
) -> jlong {
    let mut cctx: CCtx<'static> = CCtx::create();
    if zstd_safe::CCtx::set_parameter(
        &mut cctx,
        CParameter::CompressionLevel(level.clamp(1, 22)),
    )
    .is_err()
    {
        return 0;
    }
    Box::into_raw(Box::new(cctx)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_std_Zstd_freeCCtx(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    if ptr == 0 {
        return;
    }
    unsafe {
        drop(Box::from_raw(ptr as *mut CCtx<'static>));
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_std_Zstd_compress(
    _env: JNIEnv,
    _class: JClass,
    ctx: jlong,
    src_addr: jlong,
    src_len: jlong,
    dst_addr: jlong,
    dst_cap: jlong,
) -> jlong {
    if ctx == 0 {
        return -1;
    }
    let cctx = unsafe { &mut *(ctx as *mut CCtx<'static>) };
    let src = unsafe { std::slice::from_raw_parts(src_addr as *const u8, src_len as usize) };
    let dst = unsafe { std::slice::from_raw_parts_mut(dst_addr as *mut u8, dst_cap as usize) };
    match cctx.compress2(dst, src) {
        Ok(n) => n as jlong,
        Err(code) => -(code as jlong),
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_std_Zstd_createDCtx(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let dctx: DCtx<'static> = DCtx::create();
    Box::into_raw(Box::new(dctx)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_std_Zstd_freeDCtx(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    if ptr == 0 {
        return;
    }
    unsafe {
        drop(Box::from_raw(ptr as *mut DCtx<'static>));
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_std_Zstd_decompress(
    _env: JNIEnv,
    _class: JClass,
    ctx: jlong,
    src_addr: jlong,
    src_len: jlong,
    dst_addr: jlong,
    dst_cap: jlong,
) -> jlong {
    if ctx == 0 {
        return -1;
    }
    let dctx = unsafe { &mut *(ctx as *mut DCtx<'static>) };
    let src = unsafe { std::slice::from_raw_parts(src_addr as *const u8, src_len as usize) };
    let dst = unsafe { std::slice::from_raw_parts_mut(dst_addr as *mut u8, dst_cap as usize) };
    match dctx.decompress(dst, src) {
        Ok(n) => n as jlong,
        Err(code) => -(code as jlong),
    }
}
