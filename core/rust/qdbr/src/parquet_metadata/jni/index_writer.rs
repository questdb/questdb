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

//! JNI bindings for `IndexMetaFileWriter` (Java class `io.questdb.cairo.IndexMetaFileWriter`),
//! the `_im` covering-index metadata file writer, format version 2.
//!
//! The surface mirrors the `_pm` writer bindings in the sibling `writer`
//! module: create / populate / finish / destroy against a boxed
//! [`IndexMetaWriter`], with the finished bytes handed back through a second
//! boxed [`IndexMetaBuiltFile`].
//!
//! Populating a row group is one call per row group, not one per field: Java
//! lays out the `COLUMN_COUNT` 64-byte `ColumnChunkRaw` structures itself and
//! passes a single pointer, so a wide index schema does not cost a JNI
//! transition per chunk field. Stats too wide to inline are patched onto the
//! last row group afterwards, exactly as `_pm`'s `addBloomFilter` does.
//!
//! These `extern "system"` functions are called from Java via JNI. Raw pointer
//! parameters are null-checked via the `check_not_null!` macro before
//! dereferencing, so the functions are safe in practice but cannot be marked
//! `unsafe` because they must match the JNI calling convention.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use crate::parquet::error::{fmt_err, parquet_meta_err};
use crate::parquet_metadata::error::ParquetMetaErrorKind;
use crate::parquet_metadata::header::ColumnDescriptorRaw;
use crate::parquet_metadata::index_meta::IndexMetaWriter;
use crate::parquet_metadata::types::{ColumnFlags, COLUMN_CHUNK_SIZE};
use crate::parquet_metadata::{ColumnChunkRaw, RowGroupBlockBuilder};
use jni::objects::JClass;
use jni::sys::{jboolean, jint, jlong};
use jni::JNIEnv;
use std::slice;

/// Byte width of one `data.parquet` row group boundary, matching Java's
/// `Long.BYTES`.
const BOUNDARY_SIZE: usize = std::mem::size_of::<i64>();

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

/// Appends an index column: the `_pm` 32-byte descriptor plus its name. `id`
/// carries the covered column's QuestDB writer index, or `-1` for the
/// synthetic `key_id` / `row_id` columns.
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_addColumn(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut IndexMetaWriter,
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
    check_not_null!(env, ptr, "IndexMetaFileWriter");
    check_not_null!(env, name_ptr, "IndexMetaFileWriter column name");
    // A negative jint would become an enormous slice length below.
    check_not_negative!(env, name_len, "IndexMetaFileWriter column name");
    let physical_type = match u8::try_from(physical_type) {
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
    let max_rep_level = match u8::try_from(max_rep_level) {
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
    let max_def_level = match u8::try_from(max_def_level) {
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
    let name_bytes = unsafe { slice::from_raw_parts(name_ptr, name_len as usize) };
    let name = match std::str::from_utf8(name_bytes) {
        Ok(s) => s,
        Err(e) => {
            let err = parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "invalid UTF-8 in index column name: {}",
                e
            );
            return err.into_cairo_exception().throw(env);
        }
    };
    let writer = unsafe { &mut *ptr };
    // name_offset / name_length are owned by the writer and backpatched on
    // finish, so whatever is passed here would be discarded.
    writer.add_column(
        name,
        ColumnDescriptorRaw {
            name_offset: 0,
            id,
            col_type,
            flags: ColumnFlags(flags).0,
            fixed_byte_len,
            name_length: 0,
            physical_type,
            max_rep_level,
            max_def_level,
            _reserved: 0,
        },
    );
}

/// Patches a min or max stat of the most recently added row group's column
/// chunk into the block's out-of-line region. Used for covered columns whose
/// statistics exceed the 8 inline bytes: `UUID`, `LONG256`, `VARCHAR` and
/// friends.
#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_addOutOfLineStat(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut IndexMetaWriter,
    col_index: jint,
    is_min: jboolean,
    data_ptr: *const u8,
    data_len: jint,
) {
    let env = &mut env;
    check_not_null!(env, ptr, "IndexMetaFileWriter");
    check_not_null!(env, data_ptr, "IndexMetaFileWriter out-of-line stat");
    // A negative jint would become an enormous slice length below.
    check_not_negative!(env, data_len, "IndexMetaFileWriter out-of-line stat");
    check_not_negative!(
        env,
        col_index,
        "IndexMetaFileWriter out-of-line stat column"
    );
    let writer = unsafe { &mut *ptr };
    let data = unsafe { slice::from_raw_parts(data_ptr, data_len as usize) };
    if let Err(err) =
        writer.add_out_of_line_stat_to_last_row_group(col_index as usize, is_min != 0, data)
    {
        let mut err: crate::parquet::error::ParquetError = err.into();
        err.add_context("error in IndexMetaFileWriter.addOutOfLineStat");
        err.into_cairo_exception().throw::<()>(env);
    }
}

/// Appends one index row group: its first (smallest) key id, `NUM_ROWS`, and
/// `chunk_count` column chunks read from `chunks_ptr`. The buffer holds
/// `chunk_count` consecutive 64-byte `ColumnChunkRaw` structures in on-disk
/// layout, so a row group costs one JNI transition regardless of schema width.
///
/// `chunks_len` is the buffer's own byte length, and `chunk_count` chunks must
/// account for exactly that many bytes. Without it the count alone decides how
/// far the loop below reads, and a caller that miscounts produces an
/// out-of-bounds native read that nothing on either side can detect.
#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_addRowGroup(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut IndexMetaWriter,
    first_key: jint,
    num_rows: jlong,
    chunks_ptr: *const u8,
    chunks_len: jlong,
    chunk_count: jint,
) {
    let env = &mut env;
    check_not_null!(env, ptr, "IndexMetaFileWriter");
    check_not_null!(env, chunks_ptr, "IndexMetaFileWriter column chunks");
    // A negative jint would become an enormous chunk count below.
    check_not_negative!(env, chunk_count, "IndexMetaFileWriter column chunks");
    if num_rows < 0 {
        let err = parquet_meta_err!(
            ParquetMetaErrorKind::InvalidValue,
            "row group num rows {} is negative",
            num_rows
        );
        return err.into_cairo_exception().throw(env);
    }
    // chunk_count is non-negative by the check above, so this product is exact
    // in i64 and the comparison bounds the reads that follow by the buffer the
    // caller actually allocated.
    let expected_len = chunk_count as i64 * COLUMN_CHUNK_SIZE as i64;
    if chunks_len != expected_len {
        let err = parquet_meta_err!(
            ParquetMetaErrorKind::InvalidValue,
            "column chunk buffer length {} does not match {} chunks of {} bytes",
            chunks_len,
            chunk_count,
            COLUMN_CHUNK_SIZE
        );
        return err.into_cairo_exception().throw(env);
    }
    let mut block = RowGroupBlockBuilder::new(chunk_count as u32);
    block.set_num_rows(num_rows as u64);
    let chunks = chunks_ptr as *const ColumnChunkRaw;
    for i in 0..chunk_count as usize {
        // read_unaligned: the buffer comes from a Java-side allocation whose
        // alignment the JVM does not guarantee to be 8.
        let chunk = unsafe { std::ptr::read_unaligned(chunks.add(i)) };
        if let Err(err) = block.set_column_chunk(i, chunk) {
            let mut err: crate::parquet::error::ParquetError = err.into();
            err.add_context("error in IndexMetaFileWriter.addRowGroup");
            return err.into_cairo_exception().throw(env);
        }
    }
    let writer = unsafe { &mut *ptr };
    writer.add_row_group(first_key as u32, block);
}

/// Creates a writer. `key_id_column` and `row_id_column` are indices into the
/// columns added with `addColumn`; `row_id_column` is `-1` under the
/// row-per-key payload kind.
#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_create(
    _env: JNIEnv,
    _class: JClass,
    payload_kind: jint,
    key_count: jint,
    key_id_column: jint,
    row_id_column: jint,
) -> *mut IndexMetaWriter {
    Box::into_raw(Box::new(IndexMetaWriter::new(
        payload_kind as u32,
        key_count as u32,
        key_id_column,
        row_id_column,
    )))
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

/// Finishes building the _im file. Borrows (does not consume) the writer.
/// The caller must still call `destroyWriter` to free the writer.
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

/// Sets `data.parquet`'s cumulative row group boundaries from `count`
/// consecutive `i64`s read at `boundaries_ptr`.
///
/// `boundaries_len` is the buffer's own byte length, and `count` boundaries
/// must account for exactly that many bytes. Without it the count alone
/// decides how far the slice below reads, and a caller that miscounts produces
/// an out-of-bounds native read that nothing on either side can detect.
#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_setDataRowGroupBoundaries(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut IndexMetaWriter,
    boundaries_ptr: *const i64,
    boundaries_len: jlong,
    count: jint,
) {
    let env = &mut env;
    check_not_null!(env, ptr, "IndexMetaFileWriter");
    check_not_null!(env, boundaries_ptr, "IndexMetaFileWriter boundaries");
    // A negative jint would become an enormous slice length below.
    check_not_negative!(env, count, "IndexMetaFileWriter boundaries");
    // count is non-negative by the check above, so this product is exact in
    // i64 and the comparison bounds the read that follows by the buffer the
    // caller actually allocated.
    let expected_len = count as i64 * BOUNDARY_SIZE as i64;
    if boundaries_len != expected_len {
        let err = parquet_meta_err!(
            ParquetMetaErrorKind::InvalidValue,
            "boundary buffer length {} does not match {} boundaries of {} bytes",
            boundaries_len,
            count,
            BOUNDARY_SIZE
        );
        return err.into_cairo_exception().throw(env);
    }
    let writer = unsafe { &mut *ptr };
    let boundaries = unsafe { slice::from_raw_parts(boundaries_ptr, count as usize) };
    writer.set_data_row_group_boundaries(boundaries);
}

/// Overwrites the payload kind and key count passed to `create`, for callers
/// that only learn them once the index build has run.
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
