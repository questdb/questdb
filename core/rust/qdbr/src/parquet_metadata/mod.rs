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

//! `_pm` parquet partition metadata.
//!
//! Format primitives (readers, writers, footer/header/row-group/column-chunk
//! types) live in the shared `qdb-parquet-meta` crate so any crate that
//! needs to parse `_pm` can link the format types without depending on the
//! qdbr write/read pipeline. This module retains qdbr-only pieces:
//! write-path conversion helpers (`convert`), JNI thunks (`jni`), and
//! row-group filter pushdown that delegates to
//! `crate::parquet_read::ParquetDecoder` (`skip`).
//!
//! The format specification lives in `docs/parquet-metadata.md`.

pub mod convert;
pub mod index_gen;
pub mod jni;
pub mod skip;

pub use qdb_parquet_meta::{
    column_chunk, error, footer, header, index_meta, reader, row_group, types, writer,
    ColumnChunkRaw, ColumnDescriptorRaw, FileHeader, FileHeaderBuilder, Footer, FooterBuilder,
    ParquetMetaReader, ParquetMetaUpdateWriter, ParquetMetaWriter, RowGroupBlockBuilder,
    RowGroupBlockReader,
};

pub use convert::{
    generate_parquet_metadata, physical_type_to_u8, update_parquet_metadata, ParquetMetaColumnInfo,
    ParquetMetaUpdateResult,
};

/// Bytes one covering-index entry occupies in a JNI entries buffer: three
/// `i64`s, `column_id`, `index_txn`, `im_file_size`.
pub const COVERING_INDEX_JNI_ENTRY_SIZE: i64 = 3 * 8;

/// Parses a JNI covering-index entries buffer into the form the `_pm` writers
/// take, validating the buffer before dereferencing any of it.
///
/// Both entry points that accept these entries take the byte length alongside
/// the count, and this is where the two are reconciled: `entry_count` is only
/// believed when `entry_count * COVERING_INDEX_JNI_ENTRY_SIZE` is exactly
/// `entries_size`. Without that a count derived on the Java side -- it is a
/// division there -- could exceed the allocation and read past it, which is a
/// JVM crash rather than an exception. Every field is range-checked too: a
/// negative `index_txn` or `im_file_size` would otherwise wrap into an enormous
/// `u64` and be written into the footer as one.
///
/// Returns the message to throw on any violation; the caller owns the exception
/// type and context.
///
/// # Safety
///
/// `entries_ptr` must either be null with `entry_count == 0`, or address at
/// least `entries_size` readable bytes that stay valid for the whole call.
pub unsafe fn covering_index_entries(
    entries_ptr: *const i64,
    entries_size: i64,
    entry_count: i32,
) -> Result<Vec<(u32, u64, u64)>, String> {
    if entry_count < 0 || entries_size < 0 {
        return Err(format!(
            "covering index buffer has a negative size or count [entriesSize={entries_size}, entryCount={entry_count}]"
        ));
    }
    let expected = entry_count as i64 * COVERING_INDEX_JNI_ENTRY_SIZE;
    if expected != entries_size {
        return Err(format!(
            "covering index buffer size does not match its entry count [entriesSize={entries_size}, entryCount={entry_count}, expectedSize={expected}]"
        ));
    }
    if entry_count > 0 && entries_ptr.is_null() {
        return Err(format!(
            "covering index buffer is null for {entry_count} entries"
        ));
    }
    let mut entries: Vec<(u32, u64, u64)> = Vec::with_capacity(entry_count as usize);
    for i in 0..entry_count as usize {
        // SAFETY: the checks above establish that `entries_ptr` addresses
        // exactly `entry_count` entries of three i64 each, and the caller
        // guarantees the buffer stays alive for the duration of the call. Read
        // unaligned: nothing in the JNI contract guarantees the allocation's
        // alignment, and an unaligned `&[i64]` is undefined behaviour the
        // moment it is constructed.
        let entry = unsafe { entries_ptr.add(i * 3) };
        let column_id = unsafe { entry.read_unaligned() };
        let index_txn = unsafe { entry.add(1).read_unaligned() };
        let im_file_size = unsafe { entry.add(2).read_unaligned() };
        let column_id = u32::try_from(column_id).map_err(|_| {
            format!("covering index entry {i} has out of range column id {column_id}")
        })?;
        let index_txn = u64::try_from(index_txn).map_err(|_| {
            format!("covering index entry {i} has out of range index txn {index_txn}")
        })?;
        let im_file_size = u64::try_from(im_file_size).map_err(|_| {
            format!("covering index entry {i} has out of range _im file size {im_file_size}")
        })?;
        entries.push((column_id, index_txn, im_file_size));
    }
    Ok(entries)
}

#[cfg(test)]
mod covering_index_entries_tests {
    use super::*;

    #[test]
    fn accepts_a_buffer_whose_length_matches_its_count() {
        let buf: Vec<i64> = vec![1, 7, 4096, 2, 8, 512];
        let entries =
            unsafe { covering_index_entries(buf.as_ptr(), (buf.len() * 8) as i64, 2).unwrap() };
        assert_eq!(entries, vec![(1u32, 7u64, 4096u64), (2u32, 8u64, 512u64)]);
    }

    #[test]
    fn accepts_an_empty_clear() {
        let entries = unsafe { covering_index_entries(std::ptr::null(), 0, 0).unwrap() };
        assert!(entries.is_empty());
    }

    #[test]
    fn rejects_a_count_that_over_reads_the_buffer() {
        // The Java side derives the count by division, so an odd long count
        // truncates and would otherwise read past the allocation. 5 longs is 40
        // bytes; a count of 1 claims 24 and a count of 2 claims 48 -- neither
        // matches, and the mismatch is the error rather than a read.
        let buf: Vec<i64> = vec![1, 7, 4096, 2, 8];
        let err = unsafe { covering_index_entries(buf.as_ptr(), (buf.len() * 8) as i64, 1) }
            .expect_err("a buffer length that does not match the count must be rejected");
        assert!(err.contains("does not match its entry count"), "{err}");
        let err = unsafe { covering_index_entries(buf.as_ptr(), (buf.len() * 8) as i64, 2) }
            .expect_err("a count that over-reads the buffer must be rejected");
        assert!(err.contains("does not match its entry count"), "{err}");
    }

    #[test]
    fn rejects_a_null_buffer_with_a_non_zero_count() {
        let err = unsafe { covering_index_entries(std::ptr::null(), 24, 1) }
            .expect_err("a null buffer with entries must be rejected");
        assert!(err.contains("is null for 1 entries"), "{err}");
    }

    #[test]
    fn rejects_out_of_range_fields() {
        let buf: Vec<i64> = vec![-1, 7, 4096];
        let err = unsafe { covering_index_entries(buf.as_ptr(), 24, 1) }
            .expect_err("a negative column id must be rejected");
        assert!(err.contains("out of range column id"), "{err}");

        let buf: Vec<i64> = vec![1, -7, 4096];
        let err = unsafe { covering_index_entries(buf.as_ptr(), 24, 1) }
            .expect_err("a negative index txn must be rejected");
        assert!(err.contains("out of range index txn"), "{err}");

        let buf: Vec<i64> = vec![1, 7, -4096];
        let err = unsafe { covering_index_entries(buf.as_ptr(), 24, 1) }
            .expect_err("a negative _im file size must be rejected");
        assert!(err.contains("out of range _im file size"), "{err}");
    }
}
