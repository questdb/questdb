/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

//! Generic panic-guard helpers for `pub extern "system" fn` JNI entry points.
//!
//! Unwinding across an FFI boundary is undefined behaviour on the default
//! `panic = "unwind"` strategy; even "should never panic" sites are guarded
//! as defence in depth, because the global allocator and `parquet2`'s thrift
//! parser can both panic on resource exhaustion or malformed input.
//!
//! [`ffi_guard`] and [`ffi_guard_void`] catch the panic, log a tagged record
//! to stderr (the JVM forwards stderr to the QuestDB log), and raise a
//! `CairoException` on the JNI environment so the Java caller sees a thrown
//! exception rather than a sentinel return value that could collide with a
//! benign result (e.g. `false` from `canSkipRowGroup0`, `0` from offset
//! getters). The sentinel is still returned for type compatibility but is
//! ignored once the JVM observes the pending exception on JNI return.
//!
//! `qwp_zstd.rs` uses an in-module copy of this helper that predates the
//! cross-module need; new sites should call the generic version here.

use jni::JNIEnv;
use qdb_core::cairo::CairoException;
use std::panic::{catch_unwind, AssertUnwindSafe};

/// Catches any panic escaping `f`, logs a tagged record to stderr, raises a
/// `CairoException` on `env`, and returns `sentinel`. The Java caller sees
/// the pending exception on JNI return; the sentinel value is irrelevant in
/// that case but must be returned to satisfy the calling convention.
///
/// Use this for entry points returning a value (`*mut T`, `jlong`, `bool`,
/// `usize`, etc.). For void-returning entry points use [`ffi_guard_void`].
pub(crate) fn ffi_guard<R, F>(env: &mut JNIEnv, name: &'static str, sentinel: R, f: F) -> R
where
    F: FnOnce(&mut JNIEnv) -> R,
{
    // Reborrow `env` as a raw pointer so the inner closure can borrow it
    // mutably while running, and the panic-recovery branch can borrow it
    // mutably again after the closure has been dropped.
    let env_ptr: *mut JNIEnv = env;
    let result = catch_unwind(AssertUnwindSafe(move || {
        // SAFETY: `ffi_guard` holds the unique `&mut JNIEnv` for its whole
        // lifetime. The closure has exclusive use of it while running.
        let env_inner: &mut JNIEnv = unsafe { &mut *env_ptr };
        f(env_inner)
    }));
    match result {
        Ok(v) => v,
        Err(payload) => {
            // SAFETY: the closure has been dropped, so `env_ptr` is the
            // unique mutable alias again.
            let env_back: &mut JNIEnv = unsafe { &mut *env_ptr };
            handle_ffi_panic(env_back, name, &payload);
            sentinel
        }
    }
}

/// Void-returning sibling of [`ffi_guard`]. A panic is logged and converted
/// to a `CairoException` on `env`, matching the value-returning variant.
pub(crate) fn ffi_guard_void<F>(env: &mut JNIEnv, name: &'static str, f: F)
where
    F: FnOnce(&mut JNIEnv),
{
    let env_ptr: *mut JNIEnv = env;
    let result = catch_unwind(AssertUnwindSafe(move || {
        // SAFETY: see `ffi_guard`.
        let env_inner: &mut JNIEnv = unsafe { &mut *env_ptr };
        f(env_inner)
    }));
    if let Err(payload) = result {
        // SAFETY: see `ffi_guard`.
        let env_back: &mut JNIEnv = unsafe { &mut *env_ptr };
        handle_ffi_panic(env_back, name, &payload);
    }
}

fn handle_ffi_panic(env: &mut JNIEnv, name: &'static str, payload: &Box<dyn std::any::Any + Send>) {
    let msg = payload_message(payload);
    eprintln!("ERROR qdbr::{} panicked across JNI boundary: {}", name, msg);
    // If the panic site already raised a Java exception (e.g. via
    // `CairoException::throw`) before unwinding, don't stomp on it.
    if matches!(env.exception_check(), Ok(true)) {
        return;
    }
    let _: () = CairoException::new(format!("{} panicked: {}", name, msg)).throw(env);
}

/// Best-effort extraction of a human-readable message from a panic payload.
/// `panic!("msg")` produces `&'static str`; `panic!("{}", x)` produces
/// `String`. Anything else (e.g. a custom panic type) reads as a placeholder.
fn payload_message(payload: &Box<dyn std::any::Any + Send>) -> &str {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        s
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.as_str()
    } else {
        "<non-string panic payload>"
    }
}
