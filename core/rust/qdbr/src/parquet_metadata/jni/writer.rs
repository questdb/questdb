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
use crate::parquet_metadata::covering_index_entries;
use crate::parquet_metadata::error::ParquetMetaErrorKind;
use crate::parquet_metadata::row_group::RowGroupBlockBuilder;
use crate::parquet_metadata::types::ColumnFlags;
use crate::parquet_metadata::writer::ParquetMetaWriter;
use jni::objects::JClass;
use jni::sys::{jint, jlong};
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
    ($env:expr, $ptr:expr, $name:expr) => {
        if $ptr.is_null() {
            let err = fmt_err!(InvalidType, concat!($name, " pointer is null"));
            return err.into_cairo_exception().throw($env);
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
    let env = &mut env;
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
    let env = &mut env;
    check_not_null!(env, ptr, "ParquetMetaFileWriter");
    if name_ptr.is_null() || name_len < 0 {
        let err = fmt_err!(InvalidType, "invalid column name pointer or length");
        return err.into_cairo_exception().throw(env);
    }
    debug_assert!(
        (name_len as usize) <= 1 << 16,
        "implausible column name length: {}",
        name_len
    );
    let physical_type_u8 = match u8::try_from(physical_type) {
        Ok(v) => v,
        Err(_) => {
            let err = fmt_err!(
                InvalidType,
                "physical_type {} out of u8 range",
                physical_type
            );
            return err.into_cairo_exception().throw(env);
        }
    };
    let max_rep_level_u8 = match u8::try_from(max_rep_level) {
        Ok(v) => v,
        Err(_) => {
            let err = fmt_err!(
                InvalidType,
                "max_rep_level {} out of u8 range",
                max_rep_level
            );
            return err.into_cairo_exception().throw(env);
        }
    };
    let max_def_level_u8 = match u8::try_from(max_def_level) {
        Ok(v) => v,
        Err(_) => {
            let err = fmt_err!(
                InvalidType,
                "max_def_level {} out of u8 range",
                max_def_level
            );
            return err.into_cairo_exception().throw(env);
        }
    };
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
            return err.into_cairo_exception().throw(env);
        }
    };
    wrapper.writer.add_column(
        name,
        id,
        col_type,
        ColumnFlags(flags),
        fixed_byte_len,
        physical_type_u8,
        max_rep_level_u8,
        max_def_level_u8,
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
    let env = &mut env;
    check_not_null!(env, ptr, "ParquetMetaFileWriter");
    if bitset_ptr.is_null() || bitset_len < 0 {
        let err = parquet_meta_err!(
            ParquetMetaErrorKind::InvalidValue,
            "invalid bloom filter bitset pointer or length"
        );
        return err.into_cairo_exception().throw(env);
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
        err.into_cairo_exception().throw::<()>(env);
    }
}

/// Adds a covering-index entry `(column_id, index_txn, im_file_size)` to the
/// footer's `COVERING_INDEX` section. Unlike `addBloomFilter`, this is not
/// tied to the last row group -- one entry per indexed column, valid for
/// the whole footer.
#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_ParquetMetaFileWriter_addCoveringIndex(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut JniParquetMetaWriter,
    column_id: jint,
    index_txn: u64,
    im_file_size: u64,
) {
    let env = &mut env;
    check_not_null!(env, ptr, "ParquetMetaFileWriter");
    let column_id_u32 = match u32::try_from(column_id) {
        Ok(v) => v,
        Err(_) => {
            let err = fmt_err!(InvalidType, "column_id {} out of u32 range", column_id);
            return err.into_cairo_exception().throw(env);
        }
    };
    let wrapper = unsafe { &mut *ptr };
    wrapper
        .writer
        .add_covering_index(column_id_u32, index_txn, im_file_size);
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_ParquetMetaFileWriter_addSortingColumn(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut JniParquetMetaWriter,
    index: jint,
) {
    let env = &mut env;
    check_not_null!(env, ptr, "ParquetMetaFileWriter");
    let index_u32 = match u32::try_from(index) {
        Ok(v) => v,
        Err(_) => {
            let err = fmt_err!(
                InvalidType,
                "sorting column index {} out of u32 range",
                index
            );
            return err.into_cairo_exception().throw(env);
        }
    };
    let wrapper = unsafe { &mut *ptr };
    wrapper.writer.add_sorting_column(index_u32);
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_ParquetMetaFileWriter_addRowGroup(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut JniParquetMetaWriter,
    num_rows: u64,
) {
    let env = &mut env;
    check_not_null!(env, ptr, "ParquetMetaFileWriter");
    let wrapper = unsafe { &mut *ptr };
    if wrapper.column_count == 0 {
        let err = parquet_meta_err!(
            ParquetMetaErrorKind::InvalidValue,
            "cannot add row group: no columns defined"
        );
        return err.into_cairo_exception().throw(env);
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
    let env = &mut env;
    check_not_null!(env, ptr, "ParquetMetaFileWriter");
    let length_u32 = match u32::try_from(length) {
        Ok(v) => v,
        Err(_) => {
            let err = fmt_err!(
                InvalidType,
                "parquet footer length {} out of u32 range",
                length
            );
            return err.into_cairo_exception().throw(env);
        }
    };
    let wrapper = unsafe { &mut *ptr };
    wrapper.writer.parquet_footer(offset, length_u32);
}

/// Finishes building the _pm file. Borrows (does not consume) the writer.
/// The caller must still call `destroyWriter` to free the writer.
#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_ParquetMetaFileWriter_finish(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut JniParquetMetaWriter,
) -> *mut ParquetMetaBuiltFile {
    let env = &mut env;
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
            err.into_cairo_exception().throw(env)
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_ParquetMetaFileWriter_resultDataPtr(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *const ParquetMetaBuiltFile,
) -> *const u8 {
    let env = &mut env;
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
    let env = &mut env;
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
    let env = &mut env;
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

/// Builds an append-only `_pm` snapshot that restates the covering-index
/// section and changes nothing else: same row group offsets, same parquet
/// footer, same `unused_bytes`, and the prior footer's `seqTxn` explicitly
/// inherited. This is how a seal publishes its index token without rewriting
/// `data.parquet`.
///
/// `existing_ptr` must address `append_base` bytes of the `_pm` file.
/// `parse_anchor` is the committed `_pm` size the current `data.parquet` size
/// resolves to, and `append_base` is the `_pm` header at offset 0; they differ
/// only inside the crash window a rolled-back update leaves behind.
///
/// `entries_ptr` addresses `entries_size` bytes holding `entry_count` entries of
/// three `i64` each: `column_id`, `index_txn`, `im_file_size`. The byte length
/// is passed alongside the count and validated against it before any
/// dereference, so a count that disagrees with the allocation errors instead of
/// over-reading. The set is complete, not a delta; zero entries drops the
/// section.
///
/// Returns a `ParquetMetaBuiltFile` whose `data` the caller writes **at
/// `append_base`**, not at offset 0, and whose `parquet_meta_file_size` the
/// caller patches into the header as the last write of the sequence.
#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_ParquetMetaFileWriter_buildCoveringIndexAppend(
    mut env: JNIEnv,
    _class: JClass,
    existing_ptr: *const u8,
    parse_anchor: i64,
    append_base: i64,
    entries_ptr: *const i64,
    entries_size: jlong,
    entry_count: jint,
) -> *mut ParquetMetaBuiltFile {
    let env = &mut env;
    check_not_null!(env, existing_ptr, "_pm buffer");
    if parse_anchor <= 0 || append_base < parse_anchor {
        let err = fmt_err!(
            InvalidType,
            "_pm append base {append_base} out of range for parse anchor {parse_anchor}"
        );
        return err.into_cairo_exception().throw(env);
    }
    // SAFETY: the JNI caller guarantees `entries_size` readable bytes at
    // `entries_ptr` for the duration of the call; the helper validates the
    // count against that length before it dereferences anything.
    let entries = match unsafe { covering_index_entries(entries_ptr, entries_size, entry_count) } {
        Ok(entries) => entries,
        Err(msg) => {
            let err = fmt_err!(InvalidType, "{msg}");
            return err.into_cairo_exception().throw(env);
        }
    };

    // SAFETY: the caller guarantees `append_base` readable bytes at
    // `existing_ptr` for the duration of the call.
    let existing = unsafe { slice::from_raw_parts(existing_ptr, append_base as usize) };
    let build = || -> crate::parquet_metadata::error::ParquetMetaResult<(Vec<u8>, u64)> {
        let mut updater =
            qdb_parquet_meta::writer::ParquetMetaUpdateWriter::new(existing, parse_anchor as u64)?;
        // Nothing about the data changed, so the prior footer's seqTxn is the
        // right value and inheriting it is the explicit opt-in rather than a
        // forgotten setter.
        updater.inherit_seq_txn();
        updater.set_covering_index(entries);
        updater.finish_appending_at(append_base as u64)
    };
    match build() {
        Ok((data, parquet_meta_file_size)) => Box::into_raw(Box::new(ParquetMetaBuiltFile {
            data,
            parquet_meta_file_size,
        })),
        Err(err) => {
            let mut err: crate::parquet::error::ParquetError = err.into();
            err.add_context("error in ParquetMetaFileWriter.buildCoveringIndexAppend");
            err.into_cairo_exception().throw(env)
        }
    }
}
