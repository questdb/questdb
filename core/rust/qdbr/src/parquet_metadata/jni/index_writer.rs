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

//! JNI bindings for `IndexMetaFileWriter` (Java class `io.questdb.cairo.IndexMetaFileWriter`).
//!
//! These `extern "system"` functions are called from Java via JNI. Raw pointer
//! parameters are null-checked via the `check_not_null!` macro before
//! dereferencing, so the functions are safe in practice but cannot be marked
//! `unsafe` because they must match the JNI calling convention.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use crate::parquet::error::fmt_err;
use crate::parquet_metadata::index_meta::IndexMetaWriter;
use jni::objects::JClass;
use jni::sys::{jint, jlong};
use jni::JNIEnv;
use std::slice;

/// Holds the finished _im file bytes.
pub struct IndexMetaBuiltFile {
    data: Vec<u8>,
}

macro_rules! check_not_negative {
    ($env:expr, $count:expr, $name:expr) => {
        if $count < 0 {
            let err = fmt_err!(InvalidType, concat!($name, " count is negative"));
            return err.into_cairo_exception().throw($env);
        }
    };
}

macro_rules! check_not_null {
    ($env:expr, $ptr:expr, $name:expr) => {
        if $ptr.is_null() {
            let err = fmt_err!(InvalidType, concat!($name, " pointer is null"));
            return err.into_cairo_exception().throw($env);
        }
    };
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_addRowGroup(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut IndexMetaWriter,
    first_key: jint,
    row_id_min: jlong,
    row_id_max: jlong,
    col_ranges_ptr: *const u64,
    col_count: jint,
) {
    let env = &mut env;
    check_not_null!(env, ptr, "IndexMetaFileWriter");
    check_not_null!(env, col_ranges_ptr, "IndexMetaFileWriter col ranges");
    // A negative jint would become an enormous slice length below.
    check_not_negative!(env, col_count, "IndexMetaFileWriter col ranges");
    let writer = unsafe { &mut *ptr };
    let raw = unsafe { slice::from_raw_parts(col_ranges_ptr, (col_count as usize) * 2) };
    let ranges: Vec<(u64, u64)> = raw.chunks_exact(2).map(|c| (c[0], c[1])).collect();
    writer.add_row_group(first_key as u32, row_id_min, row_id_max, &ranges);
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_create(
    _env: JNIEnv,
    _class: JClass,
) -> *mut IndexMetaWriter {
    Box::into_raw(Box::new(IndexMetaWriter::new(0, 0)))
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_destroyResult(
    _env: JNIEnv,
    _class: JClass,
    ptr: *mut IndexMetaBuiltFile,
) {
    if !ptr.is_null() {
        drop(unsafe { Box::from_raw(ptr) });
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_destroyWriter(
    _env: JNIEnv,
    _class: JClass,
    ptr: *mut IndexMetaWriter,
) {
    if !ptr.is_null() {
        drop(unsafe { Box::from_raw(ptr) });
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_finish(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut IndexMetaWriter,
) -> *mut IndexMetaBuiltFile {
    let env = &mut env;
    check_not_null!(env, ptr, "IndexMetaFileWriter");
    let writer = unsafe { &*ptr };
    match writer.finish() {
        Ok(data) => Box::into_raw(Box::new(IndexMetaBuiltFile { data })),
        Err(err) => {
            let mut err: crate::parquet::error::ParquetError = err.into();
            err.add_context("error in IndexMetaFileWriter.finish");
            err.into_cairo_exception().throw(env)
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_resultDataLen(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *const IndexMetaBuiltFile,
) -> jlong {
    let env = &mut env;
    check_not_null!(env, ptr, "IndexMetaBuiltFile");
    let result = unsafe { &*ptr };
    result.data.len() as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_resultDataPtr(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *const IndexMetaBuiltFile,
) -> *const u8 {
    let env = &mut env;
    check_not_null!(env, ptr, "IndexMetaBuiltFile");
    let result = unsafe { &*ptr };
    result.data.as_ptr()
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_setDataRowGroupBoundaries(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut IndexMetaWriter,
    boundaries_ptr: *const i64,
    count: jint,
) {
    let env = &mut env;
    check_not_null!(env, ptr, "IndexMetaFileWriter");
    check_not_null!(env, boundaries_ptr, "IndexMetaFileWriter boundaries");
    // A negative jint would become an enormous slice length below.
    check_not_negative!(env, count, "IndexMetaFileWriter boundaries");
    let writer = unsafe { &mut *ptr };
    let boundaries = unsafe { slice::from_raw_parts(boundaries_ptr, count as usize) };
    writer.set_data_row_group_boundaries(boundaries);
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_setPayload(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut IndexMetaWriter,
    payload_kind: jint,
    key_count: jint,
) {
    let env = &mut env;
    check_not_null!(env, ptr, "IndexMetaFileWriter");
    let writer = unsafe { &mut *ptr };
    writer.set_payload(payload_kind as u32, key_count as u32);
}
