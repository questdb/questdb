/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

#![feature(allocator_api)]
extern crate core;
pub extern crate jni;

mod allocator;
mod cairo;
mod files;
mod parquet;
mod parquet_read;
mod parquet_write;

use jni::sys::jlong;
use jni::{objects::JClass, JNIEnv};
use once_cell::sync::Lazy;
use rayon::{ThreadPool, ThreadPoolBuilder};

pub static POOL: Lazy<ThreadPool> = Lazy::new(|| {
    let num_threads = std::env::var("QUESTDB_MAX_THREADS") // TODO: Use a proper config system
        .map(|s| s.parse::<usize>().expect("max_threads"))
        .unwrap_or_else(|_| {
            std::thread::available_parallelism()
                .unwrap_or(std::num::NonZeroUsize::new(1).unwrap())
                .get()
        });
    ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .thread_name(|i| format!("qdb-parq-{i}"))
        .build()
        .expect("could not spawn threads")
});

// Static size eq assertion: Ensures we can write pointers in place of jlong in our signatures.
const _: fn() = || {
    let _ = core::mem::transmute::<jlong, *const i32>;
};

#[no_mangle]
pub extern "system" fn Java_io_questdb_std_Os_initRust(_env: JNIEnv, _class: JClass) {
    if std::env::var("RUST_BACKTRACE").is_err() {
        std::env::set_var("RUST_BACKTRACE", "1");
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_std_Os_smokeTest(
    _env: JNIEnv,
    _class: JClass,
    a: i64,
    b: i64,
) -> i64 {
    let result = a + b;
    result
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_std_Os_isRustReleaseBuild(
    _env: JNIEnv,
    _class: JClass,
) -> bool {
    !cfg!(debug_assertions)
}
