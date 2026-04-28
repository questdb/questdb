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
//!
//! The crate is compiled with `panic = "abort"`, so any panic in these entry
//! points aborts the JVM rather than crossing the FFI boundary.

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
    // Clamp to zstd's real range: negative levels are "fast" modes
    // (-131072..=-1) and positive levels cap at 22. Values outside this
    // range would be rejected by CParameter::set_parameter and leak the
    // half-built context; clamping first keeps the return contract
    // (0 = setup failure).
    let clamped = level.clamp(-131072, 22);
    // try_create() returns None when ZSTD_createCCtx returns NULL (allocator
    // failure under memory pressure). The non-fallible CCtx::create() would
    // panic in that case; the try_ variant avoids aborting the JVM under
    // memory pressure.
    let mut cctx: CCtx<'static> = match CCtx::try_create() {
        Some(c) => c,
        None => return 0,
    };
    if zstd_safe::CCtx::set_parameter(&mut cctx, CParameter::CompressionLevel(clamped)).is_err() {
        return 0;
    }
    Box::into_raw(Box::new(cctx)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_std_Zstd_freeCCtx(_env: JNIEnv, _class: JClass, ptr: jlong) {
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
    if ctx == 0 || !valid_slice_args(src_addr, src_len) || !valid_slice_args(dst_addr, dst_cap) {
        return -1;
    }
    let cctx = unsafe { &mut *(ctx as *mut CCtx<'static>) };
    let src = unsafe { make_slice(src_addr, src_len) };
    let dst = unsafe { make_slice_mut(dst_addr, dst_cap) };
    match cctx.compress2(dst, src) {
        Ok(n) => n as jlong,
        // zstd represents errors as usize values near usize::MAX; casting to
        // jlong yields a negative i64 that the Java side already interprets
        // as an error signal.
        Err(code) => code as jlong,
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_std_Zstd_createDCtx(_env: JNIEnv, _class: JClass) -> jlong {
    // See createCCtx for why try_create is preferred over create.
    let dctx: DCtx<'static> = match DCtx::try_create() {
        Some(c) => c,
        None => return 0,
    };
    Box::into_raw(Box::new(dctx)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_std_Zstd_freeDCtx(_env: JNIEnv, _class: JClass, ptr: jlong) {
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
    if ctx == 0 || !valid_slice_args(src_addr, src_len) || !valid_slice_args(dst_addr, dst_cap) {
        return -1;
    }
    let dctx = unsafe { &mut *(ctx as *mut DCtx<'static>) };
    let src = unsafe { make_slice(src_addr, src_len) };
    let dst = unsafe { make_slice_mut(dst_addr, dst_cap) };
    match dctx.decompress(dst, src) {
        Ok(n) => n as jlong,
        // See `Java_io_questdb_std_Zstd_compress` for the error-code encoding.
        Err(code) => code as jlong,
    }
}

/// Returns `true` iff `(addr, len)` can be passed to `from_raw_parts` safely:
/// non-negative length and a non-null address unless the length is zero.
/// Rejects lengths that would exceed `isize::MAX` (a documented UB precondition
/// for `slice::from_raw_parts`).
fn valid_slice_args(addr: jlong, len: jlong) -> bool {
    if len < 0 || len as u64 > isize::MAX as u64 {
        return false;
    }
    if len > 0 && addr == 0 {
        return false;
    }
    true
}

/// # Safety
/// Caller must have checked `valid_slice_args(addr, len)` beforehand and must
/// guarantee the memory region is valid for reads for the slice's lifetime.
unsafe fn make_slice<'a>(addr: jlong, len: jlong) -> &'a [u8] {
    if len == 0 {
        // Passing the addr through with len==0 would still require it to be
        // non-null per the `from_raw_parts` contract. A dangling aligned ptr
        // satisfies that precondition without dereferencing.
        return &[];
    }
    std::slice::from_raw_parts(addr as *const u8, len as usize)
}

/// # Safety
/// Caller must have checked `valid_slice_args(addr, len)` beforehand and must
/// guarantee the memory region is valid for writes for the slice's lifetime.
unsafe fn make_slice_mut<'a>(addr: jlong, len: jlong) -> &'a mut [u8] {
    if len == 0 {
        return &mut [];
    }
    std::slice::from_raw_parts_mut(addr as *mut u8, len as usize)
}
