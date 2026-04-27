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
//! to stderr (the JVM forwards stderr to the QuestDB log), and return the
//! caller-supplied sentinel. The Java side then sees the same failure
//! signal it already handles for the corresponding business-logic error
//! path (e.g. `0` for `create*`, `null` for pointer returns, or a thrown
//! `CairoException` for value-returning entry points that already throw on
//! the error branch and read the sentinel as "no value produced").
//!
//! `qwp_zstd.rs` uses an in-module copy of this helper that predates the
//! cross-module need; new sites should call the generic version here.

use std::panic::{catch_unwind, AssertUnwindSafe};

/// Catches any panic escaping `f` and returns `sentinel` instead. The panic
/// payload is logged to stderr with `name` so an operator gets a tagged
/// record of the FFI failure even though the Java caller only sees a plain
/// sentinel return.
///
/// Use this for entry points returning a value (`*mut T`, `jlong`, `bool`,
/// `usize`, etc.). For void-returning entry points use [`ffi_guard_void`].
pub(crate) fn ffi_guard<R, F: FnOnce() -> R>(name: &'static str, sentinel: R, f: F) -> R {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(v) => v,
        Err(payload) => {
            eprintln!(
                "ERROR qdbr::{} panicked across JNI boundary, returning sentinel: {}",
                name,
                payload_message(&payload)
            );
            sentinel
        }
    }
}

/// Void-returning sibling of [`ffi_guard`]. A panic is logged and swallowed,
/// because there is no return-value channel for failure. The caller can rely
/// on side effects already being reverted by RAII for the `f` it passed.
pub(crate) fn ffi_guard_void<F: FnOnce()>(name: &'static str, f: F) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(f)) {
        eprintln!(
            "ERROR qdbr::{} panicked across JNI boundary: {}",
            name,
            payload_message(&payload)
        );
    }
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
