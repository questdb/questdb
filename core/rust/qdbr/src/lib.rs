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

mod parquet_read;
mod parquet_write;
mod mem_utils;

pub extern crate jni;

use jni::sys::jlong;
use jni::{objects::JClass, JNIEnv};

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
pub extern "system" fn Java_io_questdb_std_Os_rustSmokeTest(
    _env: JNIEnv,
    _class: JClass,
    a: i64,
    b: i64,
) -> i64 {
    a + b
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_std_Os_isRustReleaseBuild(
    _env: JNIEnv,
    _class: JClass,
) -> bool {
    !cfg!(debug_assertions)
}
