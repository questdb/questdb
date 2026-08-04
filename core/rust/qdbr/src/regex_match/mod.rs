/*+*****************************************************************************
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

//! Experimental native regex backend for the SQL `~` operator over VARCHAR columns.
//!
//! Exposed over a flat C ABI (not JNI) so the JVM side can bind it through the
//! JDK 25 Foreign Function & Memory API with low-overhead `critical` downcalls.
//! VARCHAR payloads are already UTF-8 in native memory, so matching runs against
//! raw byte slices with zero transcoding.
//!
//! The Rust `regex` crate is linear-time and intentionally does NOT support
//! backreferences or lookaround. `qdb_regex_compile` therefore returns 0 for any
//! pattern it cannot handle; the Java caller is expected to fall back to the JDK
//! `java.util.regex` engine in that case, so no SQL surface loses functionality.

use regex::bytes::Regex;
use std::slice;

/// Compiles a UTF-8 regex `pattern`. The pattern is passed as a raw native
/// address plus a length in bytes.
///
/// Returns an opaque handle (a leaked `Box<Regex>` pointer) as an `i64`, or `0`
/// when the pattern is not valid UTF-8 or is rejected by the engine. A non-zero
/// handle MUST be released exactly once via [`qdb_regex_free`].
#[no_mangle]
pub extern "C" fn qdb_regex_compile(pattern_ptr: i64, pattern_len: i64) -> i64 {
    if pattern_ptr == 0 || pattern_len < 0 {
        return 0;
    }
    let bytes = unsafe { slice::from_raw_parts(pattern_ptr as *const u8, pattern_len as usize) };
    let pattern = match std::str::from_utf8(bytes) {
        Ok(s) => s,
        Err(_) => return 0,
    };
    match Regex::new(pattern) {
        Ok(re) => Box::into_raw(Box::new(re)) as i64,
        Err(_) => 0,
    }
}

/// Unanchored search over `text`, mirroring `java.util.regex.Matcher#find()`:
/// returns `1` if the pattern matches anywhere in the input, `0` otherwise.
///
/// Designed to be invoked as a `critical` FFM downcall: it performs no
/// allocation, never calls back into the JVM, and returns promptly.
///
/// # Safety
/// `handle` must be a live value previously returned by [`qdb_regex_compile`]
/// and `text_ptr`/`text_len` must describe a readable region of `text_len` bytes.
#[no_mangle]
pub extern "C" fn qdb_regex_find(handle: i64, text_ptr: i64, text_len: i64) -> i32 {
    if handle == 0 || text_len < 0 {
        return 0;
    }
    let re = unsafe { &*(handle as *const Regex) };
    let text: &[u8] = if text_len == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(text_ptr as *const u8, text_len as usize) }
    };
    re.is_match(text) as i32
}

/// Releases a handle returned by [`qdb_regex_compile`]. Passing `0` is a no-op.
///
/// # Safety
/// `handle` must not be used after this call and must be freed at most once.
#[no_mangle]
pub extern "C" fn qdb_regex_free(handle: i64) {
    if handle != 0 {
        unsafe {
            drop(Box::from_raw(handle as *mut Regex));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compile(p: &str) -> i64 {
        qdb_regex_compile(p.as_ptr() as i64, p.len() as i64)
    }

    fn find(h: i64, t: &str) -> i32 {
        qdb_regex_find(h, t.as_ptr() as i64, t.len() as i64)
    }

    #[test]
    fn match_and_free() {
        let h = compile("a.c");
        assert_ne!(h, 0);
        assert_eq!(find(h, "xxabcyy"), 1);
        assert_eq!(find(h, "xxabdyy"), 0);
        assert_eq!(find(h, ""), 0);
        qdb_regex_free(h);
    }

    #[test]
    fn unicode_payload() {
        let h = compile("é+");
        assert_ne!(h, 0);
        assert_eq!(find(h, "caféé"), 1);
        qdb_regex_free(h);
    }

    #[test]
    fn unsupported_pattern_returns_zero() {
        // backreference: rejected by the linear-time engine -> caller falls back to JDK
        assert_eq!(compile(r"(a)\1"), 0);
        // invalid syntax
        assert_eq!(compile("a("), 0);
    }
}
