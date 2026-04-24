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

//! JNI bindings for `ParquetMetaFileWriter` (Java class `io.questdb.cairo.ParquetMetaFileWriter`).
//!
//! These `extern "system"` functions are called from Java via JNI. Raw pointer
//! parameters are null-checked via the `check_not_null!` macro before
//! dereferencing, so the functions are safe in practice but cannot be marked
//! `unsafe` because they must match the JNI calling convention.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use crate::parquet::error::{fmt_err, parquet_meta_err};
use crate::parquet_metadata::error::ParquetMetaErrorKind;
use crate::parquet_metadata::row_group::RowGroupBlockBuilder;
use crate::parquet_metadata::types::ColumnFlags;
use crate::parquet_metadata::writer::ParquetMetaWriter;
use jni::objects::JClass;
use jni::sys::jint;
use jni::JNIEnv;
use std::slice;

/// Holds the ParquetMetaWriter plus a column count tracker.
pub struct JniParquetMetaWriter {
    writer: ParquetMetaWriter,
    column_count: u32,
}

/// Holds the finished _pm file bytes and the committed parquet_meta_file_size.
pub struct ParquetMetaBuiltFile {
    data: Vec<u8>,
    parquet_meta_file_size: u64,
}

macro_rules! check_not_null {
    ($env:ident, $ptr:expr, $name:expr) => {
        if $ptr.is_null() {
            let err = fmt_err!(InvalidType, concat!($name, " pointer is null"));
            return err.into_cairo_exception().throw(&mut $env);
        }
    };
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_ParquetMetaFileWriter_create(
    _env: JNIEnv,
    _class: JClass,
) -> *mut JniParquetMetaWriter {
    let wrapper = JniParquetMetaWriter { writer: ParquetMetaWriter::new(), column_count: 0 };
    Box::into_raw(Box::new(wrapper))
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_ParquetMetaFileWriter_destroyWriter(
    _env: JNIEnv,
    _class: JClass,
    ptr: *mut JniParquetMetaWriter,
) {
    if !ptr.is_null() {
        drop(unsafe { Box::from_raw(ptr) });
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_ParquetMetaFileWriter_setDesignatedTimestamp(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut JniParquetMetaWriter,
    index: jint,
) {
    check_not_null!(env, ptr, "ParquetMetaFileWriter");
    let wrapper = unsafe { &mut *ptr };
    wrapper.writer.designated_timestamp(index);
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_ParquetMetaFileWriter_addColumn(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut JniParquetMetaWriter,
    name_ptr: *const u8,
    name_len: jint,
    id: jint,
    col_type: jint,
    flags: jint,
    fixed_byte_len: jint,
    physical_type: jint,
    max_rep_level: jint,
    max_def_level: jint,
) {
    check_not_null!(env, ptr, "ParquetMetaFileWriter");
    if name_ptr.is_null() || name_len < 0 {
        let err = fmt_err!(InvalidType, "invalid column name pointer or length");
        return err.into_cairo_exception().throw(&mut env);
    }
    debug_assert!(
        (name_len as usize) <= 1 << 16,
        "implausible column name length: {}",
        name_len
    );
    let wrapper = unsafe { &mut *ptr };
    let name_bytes = unsafe { slice::from_raw_parts(name_ptr, name_len as usize) };
    let name = match std::str::from_utf8(name_bytes) {
        Ok(s) => s,
        Err(e) => {
            let err = parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "invalid UTF-8 in column name: {}",
                e
            );
            return err.into_cairo_exception().throw(&mut env);
        }
    };
    wrapper.writer.add_column(
        name,
        id,
        col_type,
        ColumnFlags(flags),
        fixed_byte_len,
        physical_type as u8,
        max_rep_level as u8,
        max_def_level as u8,
    );
    wrapper.column_count += 1;
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_ParquetMetaFileWriter_addBloomFilter(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut JniParquetMetaWriter,
    col_index: jint,
    bitset_ptr: *const u8,
    bitset_len: jint,
) {
    check_not_null!(env, ptr, "ParquetMetaFileWriter");
    if bitset_ptr.is_null() || bitset_len < 0 {
        let err = parquet_meta_err!(
            ParquetMetaErrorKind::InvalidValue,
            "invalid bloom filter bitset pointer or length"
        );
        return err.into_cairo_exception().throw(&mut env);
    }
    debug_assert!(
        (bitset_len as usize) <= 1 << 30,
        "implausible bloom filter bitset length: {}",
        bitset_len
    );
    let wrapper = unsafe { &mut *ptr };
    let bitset = unsafe { slice::from_raw_parts(bitset_ptr, bitset_len as usize) };
    if let Err(err) = wrapper
        .writer
        .add_bloom_filter_to_last_row_group(col_index as usize, bitset)
    {
        let mut err: crate::parquet::error::ParquetError = err.into();
        err.add_context("error in ParquetMetaFileWriter.addBloomFilter");
        err.into_cairo_exception().throw::<()>(&mut env);
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_ParquetMetaFileWriter_addSortingColumn(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut JniParquetMetaWriter,
    index: jint,
) {
    check_not_null!(env, ptr, "ParquetMetaFileWriter");
    let wrapper = unsafe { &mut *ptr };
    wrapper.writer.add_sorting_column(index as u32);
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_ParquetMetaFileWriter_addRowGroup(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut JniParquetMetaWriter,
    num_rows: u64,
) {
    check_not_null!(env, ptr, "ParquetMetaFileWriter");
    let wrapper = unsafe { &mut *ptr };
    if wrapper.column_count == 0 {
        let err = parquet_meta_err!(
            ParquetMetaErrorKind::InvalidValue,
            "cannot add row group: no columns defined"
        );
        return err.into_cairo_exception().throw(&mut env);
    }
    let mut builder = RowGroupBlockBuilder::new(wrapper.column_count);
    builder.set_num_rows(num_rows);
    wrapper.writer.add_row_group(builder);
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_ParquetMetaFileWriter_setParquetFooter(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut JniParquetMetaWriter,
    offset: u64,
    length: jint,
) {
    check_not_null!(env, ptr, "ParquetMetaFileWriter");
    let wrapper = unsafe { &mut *ptr };
    wrapper.writer.parquet_footer(offset, length as u32);
}

/// Finishes building the _pm file. Borrows (does not consume) the writer.
/// The caller must still call `destroyWriter` to free the writer.
#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_ParquetMetaFileWriter_finish(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut JniParquetMetaWriter,
) -> *mut ParquetMetaBuiltFile {
    check_not_null!(env, ptr, "ParquetMetaFileWriter");
    let wrapper = unsafe { &mut *ptr };
    match wrapper.writer.finish() {
        Ok((data, parquet_meta_file_size)) => Box::into_raw(Box::new(ParquetMetaBuiltFile {
            data,
            parquet_meta_file_size,
        })),
        Err(err) => {
            let mut err: crate::parquet::error::ParquetError = err.into();
            err.add_context("error in ParquetMetaFileWriter.finish");
            err.into_cairo_exception().throw(&mut env)
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_ParquetMetaFileWriter_resultDataPtr(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *const ParquetMetaBuiltFile,
) -> *const u8 {
    check_not_null!(env, ptr, "ParquetMetaBuiltFile");
    let result = unsafe { &*ptr };
    result.data.as_ptr()
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_ParquetMetaFileWriter_resultDataLen(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *const ParquetMetaBuiltFile,
) -> u64 {
    check_not_null!(env, ptr, "ParquetMetaBuiltFile");
    let result = unsafe { &*ptr };
    result.data.len() as u64
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_ParquetMetaFileWriter_resultParquetMetaFileSize(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *const ParquetMetaBuiltFile,
) -> u64 {
    check_not_null!(env, ptr, "ParquetMetaBuiltFile");
    let result = unsafe { &*ptr };
    result.parquet_meta_file_size
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_ParquetMetaFileWriter_destroyResult(
    _env: JNIEnv,
    _class: JClass,
    ptr: *mut ParquetMetaBuiltFile,
) {
    if !ptr.is_null() {
        drop(unsafe { Box::from_raw(ptr) });
    }
}
