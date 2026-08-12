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
//! the `_im` covering-index metadata file writer, format version 3.
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
//! That surface is not how production writes an `_im`. `generateIndexMetadata`
//! is: it builds the whole file from a finished streaming parquet writer, whose
//! thrift metadata is the only place the per-chunk codec, encodings, byte
//! ranges, null counts and statistics exist. Java supplies what only it knows --
//! the key directory, the row-id zone maps and `data.parquet`'s row-group
//! boundaries -- and never the values it cannot see.
//!
//! These `extern "system"` functions are called from Java via JNI. Raw pointer
//! parameters are null-checked via the `check_not_null!` macro before
//! dereferencing, so the functions are safe in practice but cannot be marked
//! `unsafe` because they must match the JNI calling convention.
//!
//! Every entry point routes its body through [`ffi_guard`]. Unwinding out of an
//! `extern "system"` fn aborts the process, so without it any panic anywhere
//! under the writer -- today or after a future change -- takes the JVM down
//! instead of throwing. See [`ffi_guard`] for the reachability argument.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use crate::parquet::error::{fmt_err, parquet_meta_err};
use crate::parquet_metadata::error::ParquetMetaErrorKind;
use crate::parquet_metadata::header::ColumnDescriptorRaw;
use crate::parquet_metadata::index_meta::IndexMetaWriter;
use crate::parquet_metadata::types::{ColumnFlags, COLUMN_CHUNK_SIZE};
use crate::parquet_metadata::{ColumnChunkRaw, RowGroupBlockBuilder};
use crate::parquet_write::jni::StreamingParquetWriter;
use crate::qwp_zstd::payload_message;
use jni::objects::JClass;
use jni::sys::{jboolean, jint, jlong};
use jni::JNIEnv;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::slice;

/// Byte width of one `data.parquet` row group boundary, matching Java's
/// `Long.BYTES`.
const BOUNDARY_SIZE: usize = std::mem::size_of::<i64>();

/// Byte width of one `RG_FIRST_KEY` entry, matching Java's `Integer.BYTES`.
const FIRST_KEY_SIZE: usize = std::mem::size_of::<i32>();

/// Byte width of one `RG_ROW_ID_MIN` / `RG_ROW_ID_MAX` entry, matching Java's
/// `Long.BYTES`.
const ROW_ID_SIZE: usize = std::mem::size_of::<i64>();

/// Copies `count` elements out of a Java-side buffer, one unaligned read each.
///
/// `slice::from_raw_parts` cannot be used on these buffers: it requires the
/// pointer to be aligned to `T`, and nothing in the JNI contract guarantees a
/// Java-side allocation is aligned to 8. An unaligned `&[i64]` is undefined
/// behaviour the moment it is constructed, whether or not it is ever read, so
/// the reads are done element-wise exactly as [`addRowGroup`] already does for
/// its column chunks. The copy costs one pass over a few hundred bytes at seal
/// time and is off every hot path.
///
/// # Safety
/// `ptr` must address `count` consecutive readable `T` values. Alignment is the
/// one requirement this function lifts.
///
/// [`addRowGroup`]: Java_io_questdb_cairo_IndexMetaFileWriter_addRowGroup
unsafe fn copy_unaligned<T: Copy>(ptr: *const T, count: usize) -> Vec<T> {
    // `ptr.add` needs no alignment of its own; only `read_unaligned` touches
    // memory, and that is the read this exists to make sound.
    (0..count)
        .map(|i| unsafe { std::ptr::read_unaligned(ptr.add(i)) })
        .collect()
}

/// Holds the finished _im file bytes.
pub struct IndexMetaBuiltFile {
    data: Vec<u8>,
}

macro_rules! check_not_negative {
    ($env:expr, $value:expr, $name:expr) => {
        if $value < 0 {
            let err = fmt_err!(InvalidType, concat!($name, " is negative"));
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

/// Runs `f` and catches any panic escaping it, returning the panic's message.
///
/// Split out of [`ffi_guard`] so the catching half can be tested without a JVM:
/// the throwing half is `ParquetError::into_cairo_exception().throw()`, the
/// same call every error path in this file already makes and the Java tests
/// already cover.
///
/// The message is also logged to stderr (the JVM forwards stderr to the
/// QuestDB log), matching [`crate::qwp_zstd`]'s guards, so an operator gets a
/// tagged record of which entry point failed.
fn catch_ffi_panic<T, F: FnOnce() -> T>(name: &str, f: F) -> Result<T, String> {
    catch_unwind(AssertUnwindSafe(f)).map_err(|payload| {
        let message = format!(
            "{} panicked across the JNI boundary: {}",
            name,
            payload_message(&payload)
        );
        eprintln!("ERROR qdbr::parquet_metadata::jni::index_writer::{message}");
        message
    })
}

/// Wraps an entry point body so a panic becomes a thrown `CairoException`
/// rather than a process abort. Unwinding out of an `extern "system"` fn is an
/// abort, so an unguarded entry point turns any panic into a dead JVM.
///
/// A 300_000-sequence fuzz of the writer API reached no panic through these
/// entry points today, so this is defence in depth. It is not decoration: the
/// Rust `IndexMetaReader` gains a JNI binding of its own, and it panics on a
/// truncated file -- unguarded, that turns a corrupt `_im` sidecar into a JVM
/// abort rather than a query that fails.
///
/// Modelled on [`crate::qwp_zstd::Java_io_questdb_std_Zstd_decompress`]'s
/// `ffi_guard_jlong`, which returns a sentinel because the Zstd bindings have
/// no exception channel. These entry points do have one -- every error here is
/// already a `CairoException` -- so a panic is reported the same way, and the
/// sentinel is only the value returned after the throw is queued.
fn ffi_guard<T: Default, F: FnOnce(&mut JNIEnv) -> T>(
    env: &mut JNIEnv,
    name: &'static str,
    f: F,
) -> T {
    // The reborrow keeps `env` usable after the closure is dropped, which is
    // what lets the panic path throw.
    let caught = catch_ffi_panic(name, || f(&mut *env));
    match caught {
        Ok(value) => value,
        Err(message) => {
            let err = fmt_err!(InvalidType, "{}", message);
            err.into_cairo_exception().throw(env)
        }
    }
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
    ffi_guard(&mut env, "addColumn", |env| {
        check_not_null!(env, ptr, "IndexMetaFileWriter");
        check_not_null!(env, name_ptr, "IndexMetaFileWriter column name");
        // A negative jint would become an enormous slice length below.
        check_not_negative!(env, name_len, "IndexMetaFileWriter column name length");
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
    })
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
    ffi_guard(&mut env, "addOutOfLineStat", |env| {
        check_not_null!(env, ptr, "IndexMetaFileWriter");
        check_not_null!(env, data_ptr, "IndexMetaFileWriter out-of-line stat");
        // A negative jint would become an enormous slice length below.
        check_not_negative!(env, data_len, "IndexMetaFileWriter out-of-line stat length");
        check_not_negative!(
            env,
            col_index,
            "IndexMetaFileWriter out-of-line stat column index"
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
    })
}

/// Appends one index row group: its first (smallest) key id, the smallest and
/// largest row id it holds, `NUM_ROWS`, and `chunk_count` column chunks read
/// from `chunks_ptr`. The buffer holds `chunk_count` consecutive 64-byte
/// `ColumnChunkRaw` structures in on-disk layout, so a row group costs one JNI
/// transition regardless of schema width.
///
/// The row-id range crosses the boundary rather than being derived from the
/// `row_id` chunk because `RG_ROW_ID_MIN` / `RG_ROW_ID_MAX` are unconditional:
/// under the row-per-key payload there is no `row_id` column to take it from.
///
/// `chunks_len` is the buffer's own byte length, and `chunk_count` chunks must
/// account for exactly that many bytes. Without it the count alone decides how
/// far the loop below reads, and a caller that miscounts produces an
/// out-of-bounds native read that nothing on either side can detect.
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_addRowGroup(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut IndexMetaWriter,
    first_key: jint,
    row_id_min: jlong,
    row_id_max: jlong,
    num_rows: jlong,
    chunks_ptr: *const u8,
    chunks_len: jlong,
    chunk_count: jint,
) {
    ffi_guard(&mut env, "addRowGroup", |env| {
        check_not_null!(env, ptr, "IndexMetaFileWriter");
        check_not_null!(env, chunks_ptr, "IndexMetaFileWriter column chunks");
        // A negative jint would become an enormous chunk count below.
        check_not_negative!(env, chunk_count, "IndexMetaFileWriter column chunk count");
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
            // alignment the JVM does not guarantee to be 8. Every typed buffer
            // this file takes from Java is read the same way -- see
            // [`copy_unaligned`].
            let chunk = unsafe { std::ptr::read_unaligned(chunks.add(i)) };
            if let Err(err) = block.set_column_chunk(i, chunk) {
                let mut err: crate::parquet::error::ParquetError = err.into();
                err.add_context("error in IndexMetaFileWriter.addRowGroup");
                return err.into_cairo_exception().throw(env);
            }
        }
        let writer = unsafe { &mut *ptr };
        writer.add_row_group(first_key as u32, row_id_min, row_id_max, block);
    })
}

/// Creates a writer. `key_id_column` and `row_id_column` are indices into the
/// columns added with `addColumn`; `row_id_column` is `-1` under the
/// row-per-key payload kind.
///
/// `key_space_size` is the exclusive upper bound on key ids -- the native
/// reader's `keyCountIncludingNulls` -- not a count of distinct keys present.
/// `first_cover_column` is the descriptor index of cover slot 0: the synthetic
/// columns come first, then the covered columns in cover-slot order.
///
/// Both are rejected when negative: as a `u32` a negative `first_cover_column`
/// is a descriptor index four billion past the schema, and a negative
/// `key_space_size` is a key-space bound no key id can reach, which is the
/// silent-wrong-answer the bound exists to prevent.
#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_create(
    mut env: JNIEnv,
    _class: JClass,
    payload_kind: jint,
    key_space_size: jint,
    key_id_column: jint,
    row_id_column: jint,
    first_cover_column: jint,
) -> *mut IndexMetaWriter {
    ffi_guard(&mut env, "create", |env| {
        check_not_negative!(env, key_space_size, "IndexMetaFileWriter key space size");
        check_not_negative!(
            env,
            first_cover_column,
            "IndexMetaFileWriter first cover column"
        );
        Box::into_raw(Box::new(IndexMetaWriter::new(
            payload_kind as u32,
            key_space_size as u32,
            key_id_column,
            row_id_column,
            first_cover_column as u32,
        )))
    })
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_destroyResult(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut IndexMetaBuiltFile,
) {
    ffi_guard(&mut env, "destroyResult", |_env| {
        if !ptr.is_null() {
            drop(unsafe { Box::from_raw(ptr) });
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_destroyWriter(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut IndexMetaWriter,
) {
    ffi_guard(&mut env, "destroyWriter", |_env| {
        if !ptr.is_null() {
            drop(unsafe { Box::from_raw(ptr) });
        }
    })
}

/// Finishes building the _im file. Borrows (does not consume) the writer.
/// The caller must still call `destroyWriter` to free the writer.
#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_finish(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut IndexMetaWriter,
) -> *mut IndexMetaBuiltFile {
    ffi_guard(&mut env, "finish", |env| {
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
    })
}

/// Generates the complete `_im` for the covering-index parquet a streaming
/// parquet write has just finished, and hands it back as an
/// [`IndexMetaBuiltFile`] read with `resultDataPtr` / `resultDataLen` and freed
/// with `destroyResult`.
///
/// This is the production `_im` path. It does not go through the `create` /
/// `addColumn` / `addRowGroup` surface above because `_im` records, per (row
/// group, column), the codec, the encodings present, the byte range, the null
/// count and the min/max statistics -- values Java never sees, because the
/// parquet encoder produces them and they live only in the writer's own thrift
/// metadata. Java supplies what only it knows: the key directory, the row-id
/// zone maps and `data.parquet`'s row-group boundaries.
///
/// `writer_ptr` must be a writer whose `finishStreamingParquetWrite` has
/// already run: before that the parquet footer does not exist, and its zero
/// offset is rejected rather than recorded.
///
/// `count` is the index row-group count, and `first_keys_len`, `row_id_min_len`
/// and `row_id_max_len` are those buffers' own byte lengths, which must account
/// for exactly that many elements of their own width. `data_boundaries_len` is
/// likewise a byte length, and the boundary count is derived from it rather
/// than passed separately, so no count decides how far a read goes without its
/// buffer's length agreeing. Without that, a caller that miscounts produces an
/// out-of-bounds native read that nothing on either side can detect.
///
/// Every `_im` writer validation stays in force, including the key-alignment
/// invariant: an index whose row groups split a key across a group shared with
/// another key is refused here rather than written and discovered later.
/// Nothing at read time can detect that violation.
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_generateIndexMetadata(
    mut env: JNIEnv,
    _class: JClass,
    writer_ptr: *mut StreamingParquetWriter,
    first_keys_ptr: *const i32,
    first_keys_len: jlong,
    row_id_min_ptr: *const i64,
    row_id_min_len: jlong,
    row_id_max_ptr: *const i64,
    row_id_max_len: jlong,
    data_boundaries_ptr: *const i64,
    data_boundaries_len: jlong,
    count: jint,
    key_space_size: jint,
    key_id_column: jint,
    row_id_column: jint,
    first_cover_column: jint,
    payload_kind: jint,
) -> *mut IndexMetaBuiltFile {
    ffi_guard(&mut env, "generateIndexMetadata", |env| {
        check_not_null!(env, writer_ptr, "StreamingParquetWriter");
        check_not_null!(env, first_keys_ptr, "IndexMetaFileWriter first keys");
        check_not_null!(env, row_id_min_ptr, "IndexMetaFileWriter row id minima");
        check_not_null!(env, row_id_max_ptr, "IndexMetaFileWriter row id maxima");
        check_not_null!(env, data_boundaries_ptr, "IndexMetaFileWriter boundaries");
        // A negative jint would become an enormous slice length below.
        check_not_negative!(env, count, "IndexMetaFileWriter row group count");
        check_not_negative!(env, key_space_size, "IndexMetaFileWriter key space size");
        check_not_negative!(
            env,
            first_cover_column,
            "IndexMetaFileWriter first cover column"
        );
        // count is non-negative by the check above, so these products are exact
        // in i64 and the comparisons bound the reads that follow by the buffers
        // the caller actually allocated.
        for (name, len, element_size) in [
            ("first key", first_keys_len, FIRST_KEY_SIZE),
            ("row id min", row_id_min_len, ROW_ID_SIZE),
            ("row id max", row_id_max_len, ROW_ID_SIZE),
        ] {
            let expected_len = count as i64 * element_size as i64;
            if len != expected_len {
                let err = parquet_meta_err!(
                    ParquetMetaErrorKind::InvalidValue,
                    "{} buffer length {} does not match {} entries of {} bytes",
                    name,
                    len,
                    count,
                    element_size
                );
                return err.into_cairo_exception().throw(env);
            }
        }
        // The boundary count is whatever the buffer's own length accounts for,
        // so there is no second count to disagree with it.
        if data_boundaries_len <= 0 || data_boundaries_len % BOUNDARY_SIZE as i64 != 0 {
            let err = parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "boundary buffer length {} is not a positive multiple of {} bytes",
                data_boundaries_len,
                BOUNDARY_SIZE
            );
            return err.into_cairo_exception().throw(env);
        }
        let boundary_count = (data_boundaries_len / BOUNDARY_SIZE as i64) as usize;

        // SAFETY: the length checks above bound every one of these buffers by
        // the byte count the caller allocated, and `copy_unaligned` lifts the
        // alignment requirement a `&[i32]` / `&[i64]` would impose.
        let first_keys = unsafe { copy_unaligned(first_keys_ptr, count as usize) };
        // A negative key id would reach the writer as a key near u32::MAX and
        // trip the key-space bound with a diagnostic naming a key the caller
        // never passed. Refuse it while it is still recognisable.
        if let Some(negative) = first_keys.iter().find(|key| **key < 0) {
            let err = parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "row group first key {} is negative",
                negative
            );
            return err.into_cairo_exception().throw(env);
        }
        let first_keys: Vec<u32> = first_keys.iter().map(|key| *key as u32).collect();
        let row_id_mins = unsafe { copy_unaligned(row_id_min_ptr, count as usize) };
        let row_id_maxs = unsafe { copy_unaligned(row_id_max_ptr, count as usize) };
        let data_boundaries = unsafe { copy_unaligned(data_boundaries_ptr, boundary_count) };

        // SAFETY: the pointer comes from `Box::into_raw` in
        // `createStreamingParquetWriter`; single-threaded JNI access guarantees
        // no aliasing, and generation only reads the finished writer.
        let writer = unsafe { &*writer_ptr };
        match writer.generate_index_metadata(
            &first_keys,
            &row_id_mins,
            &row_id_maxs,
            &data_boundaries,
            key_space_size as u32,
            key_id_column,
            row_id_column,
            first_cover_column as u32,
            payload_kind as u32,
        ) {
            Ok(data) => Box::into_raw(Box::new(IndexMetaBuiltFile { data })),
            Err(mut err) => {
                err.add_context("error in IndexMetaFileWriter.generateIndexMetadata");
                err.into_cairo_exception().throw(env)
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_resultDataLen(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *const IndexMetaBuiltFile,
) -> jlong {
    ffi_guard(&mut env, "resultDataLen", |env| {
        check_not_null!(env, ptr, "IndexMetaBuiltFile");
        let result = unsafe { &*ptr };
        result.data.len() as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_resultDataPtr(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *const IndexMetaBuiltFile,
) -> *const u8 {
    ffi_guard(&mut env, "resultDataPtr", |env| {
        check_not_null!(env, ptr, "IndexMetaBuiltFile");
        let result = unsafe { &*ptr };
        result.data.as_ptr()
    })
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
    ffi_guard(&mut env, "setDataRowGroupBoundaries", |env| {
        check_not_null!(env, ptr, "IndexMetaFileWriter");
        check_not_null!(env, boundaries_ptr, "IndexMetaFileWriter boundaries");
        // A negative jint would become an enormous slice length below.
        check_not_negative!(env, count, "IndexMetaFileWriter boundary count");
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
        // SAFETY: the length check above bounds the buffer by the byte count the
        // caller allocated, and `copy_unaligned` lifts the alignment requirement
        // a `&[i64]` would impose on a pointer the JVM does not align to 8.
        let boundaries = unsafe { copy_unaligned(boundaries_ptr, count as usize) };
        writer.set_data_row_group_boundaries(&boundaries);
    })
}

/// Records where `<col>.pidx.<indexTxn>.parquet`'s own parquet footer starts
/// and how long it is. The index parquet's committed size follows as
/// `offset + length + 8`, which is what lets cold-storage upload and orphan
/// validation work without an `ff.length()` call.
///
/// Negative values are rejected here rather than wrapping into an enormous
/// `u64` / `u32`: the writer's own check only refuses zero, so a negative
/// `jlong` would reach disk as a plausible-looking footer position.
#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_setPidxFooter(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut IndexMetaWriter,
    footer_offset: jlong,
    footer_length: jint,
) {
    ffi_guard(&mut env, "setPidxFooter", |env| {
        check_not_null!(env, ptr, "IndexMetaFileWriter");
        check_not_negative!(env, footer_offset, "IndexMetaFileWriter pidx footer offset");
        check_not_negative!(env, footer_length, "IndexMetaFileWriter pidx footer length");
        let writer = unsafe { &mut *ptr };
        writer.set_pidx_footer(footer_offset as u64, footer_length as u32);
    })
}

/// Overwrites the payload kind and key space size passed to `create`, for
/// callers that only learn them once the index build has run.
#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_setPayload(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut IndexMetaWriter,
    payload_kind: jint,
    key_space_size: jint,
) {
    ffi_guard(&mut env, "setPayload", |env| {
        check_not_null!(env, ptr, "IndexMetaFileWriter");
        check_not_negative!(env, key_space_size, "IndexMetaFileWriter key space size");
        let writer = unsafe { &mut *ptr };
        writer.set_payload(payload_kind as u32, key_space_size as u32);
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The guard's throwing half is `into_cairo_exception().throw()`, which
    /// needs a JVM; its catching half is what stands between a panic and an
    /// abort, and that is what these assert. Without the catch, a panic in any
    /// entry point body unwinds out of an `extern "system"` fn and kills the
    /// process -- this test would take the test runner down with it rather
    /// than fail.
    #[test]
    fn catch_ffi_panic_catches_a_static_str_panic() {
        let caught: Result<(), String> =
            catch_ffi_panic("addRowGroup", || panic!("deliberate test panic"));
        assert_eq!(
            caught.unwrap_err(),
            "addRowGroup panicked across the JNI boundary: deliberate test panic"
        );
    }

    /// `panic!("{}", x)` produces a `String` payload rather than a
    /// `&'static str`, and an index-out-of-bounds panic -- the shape most
    /// likely to reach here from a future reader binding -- is one of those.
    #[test]
    fn catch_ffi_panic_catches_a_formatted_panic() {
        let boundaries: Vec<i64> = Vec::new();
        let caught: Result<i64, String> = catch_ffi_panic("finish", || boundaries[0]);
        let message = caught.unwrap_err();
        assert!(
            message.starts_with("finish panicked across the JNI boundary: "),
            "unexpected message: {message}"
        );
        assert!(
            message.contains("index out of bounds"),
            "unexpected message: {message}"
        );
    }

    #[test]
    fn catch_ffi_panic_passes_a_value_through() {
        let caught = catch_ffi_panic("resultDataLen", || 42i64);
        assert_eq!(caught.unwrap(), 42);
    }

    /// A guard on twelve of thirteen entry points is a guard on none of them:
    /// the thirteenth is the one a corrupt file reaches. Reading the source is the
    /// only way to assert the property over every entry point at once, and it
    /// costs nothing at runtime. The count is asserted too, so a new entry
    /// point has to come past this test.
    #[test]
    fn every_entry_point_routes_through_the_guard() {
        const MARKER: &str = "pub extern \"system\" fn Java_";
        let source = include_str!("index_writer.rs");
        // Everything below this module is the test's own text, including the
        // marker literal itself.
        let tests_at = source
            .find("\n#[cfg(test)]\nmod tests {")
            .expect("test module");
        let source = &source[..tests_at];
        let mut checked = 0;
        for (index, _) in source.match_indices(MARKER) {
            let body = &source[index..];
            let name_end = body.find('(').expect("entry point signature");
            let name = &body[..name_end];
            // The body runs to the next entry point, or to the end of file.
            let body_end = body[1..].find(MARKER).map(|i| i + 1).unwrap_or(body.len());
            assert!(
                body[..body_end].contains("ffi_guard("),
                "{name} does not route through ffi_guard; a panic in it aborts the JVM"
            );
            checked += 1;
        }
        assert_eq!(checked, 13, "entry point count changed");
    }
}
