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

//! JNI bindings for `ParquetMetaFileReader` (Java class
//! `io.questdb.cairo.ParquetMetaFileReader`).
//!
//! These bindings let the Java reader cache a parsed [`ParquetMetaReader`]
//! across multiple `canSkipRowGroup` calls instead of re-parsing the `_pm`
//! header/footer on every row group. The Java side allocates the native
//! handle lazily on the first skip call and frees it via `clear()` /
//! `close()`.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_metadata::reader::ParquetMetaReader;
use crate::parquet_read::ColumnFilterPacked;
use jni::objects::JClass;
use jni::JNIEnv;
use std::slice;

/// Holds a parsed [`ParquetMetaReader`] whose backing slice is owned by the
/// Java mmap. The `'static` lifetime is a documented lie — the actual data
/// is owned by Java and the Java side guarantees the data outlives this
/// reader by calling `destroyNativeReader` before unmapping the file.
///
/// SAFETY: callers MUST destroy this struct before munmapping the
/// underlying `_pm` file. The reference pattern is
/// `ShowPartitionsRecordCursorFactory.closeParquetMeta()` in the Java tree:
/// `clear()` first, then `munmap`.
pub struct JniParquetMetaReader {
    inner: ParquetMetaReader<'static>,
}

impl JniParquetMetaReader {
    /// Builds a reader by interpreting `[data_ptr, data_ptr + data_size)`
    /// as a `_pm` file. Validation (header/footer parsing, format version
    /// check) happens once here and is amortized across every subsequent
    /// `canSkipRowGroup` call.
    ///
    /// SAFETY: `data_ptr` must point to at least `data_size` valid bytes
    /// that remain mapped for the lifetime of the returned reader. The
    /// caller (Java side) is responsible for upholding this contract.
    unsafe fn new(data_ptr: *const u8, data_size: u64) -> ParquetResult<Self> {
        let data_size_usize = usize::try_from(data_size).map_err(|_| {
            fmt_err!(
                InvalidLayout,
                "_pm file size {} exceeds addressable range",
                data_size
            )
        })?;
        let data: &[u8] = slice::from_raw_parts(data_ptr, data_size_usize);
        // Lifetime extension: the slice borrows from a Java-owned mmap. The
        // Java side guarantees the mmap outlives this reader.
        let data: &'static [u8] = std::mem::transmute::<&[u8], &'static [u8]>(data);
        let inner = ParquetMetaReader::from_file_size(data, data_size)?;
        Ok(Self { inner })
    }

    pub fn reader(&self) -> &ParquetMetaReader<'static> {
        &self.inner
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_ParquetMetaFileReader_createNativeReader(
    mut env: JNIEnv,
    _class: JClass,
    addr: *const u8,
    file_size: u64,
) -> *mut JniParquetMetaReader {
    if addr.is_null() {
        let err = fmt_err!(InvalidLayout, "_pm file pointer is null");
        return err.into_cairo_exception().throw(&mut env);
    }
    let res = unsafe { JniParquetMetaReader::new(addr, file_size) };
    match res {
        Ok(reader) => Box::into_raw(Box::new(reader)),
        Err(mut err) => {
            err.add_context("error in ParquetMetaFileReader.createNativeReader");
            err.into_cairo_exception().throw(&mut env)
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_ParquetMetaFileReader_destroyNativeReader(
    _env: JNIEnv,
    _class: JClass,
    ptr: *mut JniParquetMetaReader,
) {
    if !ptr.is_null() {
        drop(unsafe { Box::from_raw(ptr) });
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_ParquetMetaFileReader_canSkipRowGroup0(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *const JniParquetMetaReader,
    row_group_index: u32,
    filters_ptr: *const ColumnFilterPacked,
    filter_count: u32,
    filter_buf_end: u64,
) -> bool {
    let res = (|| -> ParquetResult<bool> {
        if ptr.is_null() {
            return Err(fmt_err!(
                InvalidLayout,
                "JniParquetMetaReader pointer is null"
            ));
        }
        if filters_ptr.is_null() && filter_count > 0 {
            return Err(fmt_err!(
                InvalidLayout,
                "filters pointer is null with non-zero filter count"
            ));
        }
        let jni_reader = unsafe { &*ptr };
        let filters: &[ColumnFilterPacked] = if filter_count == 0 {
            &[]
        } else {
            unsafe { slice::from_raw_parts(filters_ptr, filter_count as usize) }
        };
        jni_reader
            .reader()
            .can_skip_row_group(row_group_index as usize, filters, filter_buf_end)
    })();
    match res {
        Ok(skip) => skip,
        Err(mut err) => {
            err.add_context("error in ParquetMetaFileReader.canSkipRowGroup");
            err.into_cairo_exception().throw(&mut env)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet_metadata::column_chunk::ColumnChunkRaw;
    use crate::parquet_metadata::row_group::RowGroupBlockBuilder;
    use crate::parquet_metadata::types::{
        encode_stat_sizes, Codec, ColumnFlags, FieldRepetition, StatFlags,
    };
    use crate::parquet_metadata::writer::ParquetMetaWriter;
    use crate::parquet_read::{FILTER_OP_EQ, FILTER_OP_GT, FILTER_OP_IS_NULL, FILTER_OP_LT};
    use qdb_core::col_type::ColumnTypeTag;

    /// Physical type ordinal for Int64 in the `_pm` format.
    const PHYS_INT64: u8 = 2;

    fn make_filter(
        column_index: u32,
        count: u32,
        op: u8,
        ptr: u64,
        col_type: i32,
    ) -> ColumnFilterPacked {
        ColumnFilterPacked {
            col_idx_and_count: (column_index as u64)
                | (((count as u64) & 0x00FF_FFFF) << 32)
                | ((op as u64) << 56),
            ptr,
            column_type: col_type as u64,
        }
    }

    /// Builds a `_pm` file with one Long column carrying min/max stats for
    /// each row group entry. Each entry is `(num_rows, null_count, min, max)`.
    fn build_long_pm(row_groups: &[(u64, u64, i64, i64)]) -> Vec<u8> {
        let mut writer = ParquetMetaWriter::new();
        writer
            .designated_timestamp(-1)
            .add_column(
                "val",
                0,
                ColumnTypeTag::Long as i32,
                ColumnFlags::new().with_repetition(FieldRepetition::Optional),
                0,
                PHYS_INT64,
                0,
                1,
            )
            .parquet_footer(0, 0);

        for &(num_rows, null_count, min, max) in row_groups {
            let mut rg = RowGroupBlockBuilder::new(1);
            rg.set_num_rows(num_rows);

            let mut chunk = ColumnChunkRaw::zeroed();
            chunk.codec = Codec::Uncompressed as u8;
            chunk.num_values = num_rows;
            chunk.null_count = null_count;
            chunk.stat_flags = StatFlags::new()
                .with_min(true, true)
                .with_max(true, true)
                .with_null_count()
                .0;
            chunk.stat_sizes = encode_stat_sizes(8, 8);
            chunk.min_stat = min as u64;
            chunk.max_stat = max as u64;
            rg.set_column_chunk(0, chunk).unwrap();

            writer.add_row_group(rg);
        }

        let (bytes, _) = writer.finish().unwrap();
        bytes
    }

    #[test]
    fn create_destroy_no_use() {
        let bytes = build_long_pm(&[(100, 0, 10, 200)]);
        let reader = unsafe { JniParquetMetaReader::new(bytes.as_ptr(), bytes.len() as u64) }
            .expect("reader should construct from valid _pm file");
        assert_eq!(reader.reader().column_count(), 1);
        assert_eq!(reader.reader().row_group_count(), 1);
        // Drop simulates destroyNativeReader.
    }

    #[test]
    fn create_invalid_file_too_small() {
        let buf = [0u8; 3];
        let res = unsafe { JniParquetMetaReader::new(buf.as_ptr(), buf.len() as u64) };
        assert!(res.is_err(), "expected Err for 3-byte buffer");
    }

    #[test]
    fn create_invalid_format_version() {
        let mut bytes = build_long_pm(&[(100, 0, 10, 200)]);
        // Corrupt the format version field at byte 0.
        bytes[0] ^= 0xFF;
        let res = unsafe { JniParquetMetaReader::new(bytes.as_ptr(), bytes.len() as u64) };
        assert!(res.is_err(), "expected Err for corrupted format version");
    }

    #[test]
    fn create_invalid_footer_length() {
        // Trailer claims a footer length larger than the file.
        let mut buf = [0u8; 20];
        buf[16..20].copy_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
        let res = unsafe { JniParquetMetaReader::new(buf.as_ptr(), buf.len() as u64) };
        assert!(res.is_err(), "expected Err for invalid footer length");
    }

    #[test]
    fn can_skip_no_filters_returns_false() {
        let bytes = build_long_pm(&[(100, 0, 10, 200)]);
        let jni_reader =
            unsafe { JniParquetMetaReader::new(bytes.as_ptr(), bytes.len() as u64) }.unwrap();
        let result = jni_reader.reader().can_skip_row_group(0, &[], 0).unwrap();
        assert!(!result, "no filters → cannot skip");
    }

    #[test]
    fn can_skip_eq_outside_min_max_via_cached_reader() {
        // [10, 200], EQ 999 → outside range, skip.
        let bytes = build_long_pm(&[(100, 0, 10, 200)]);
        let jni_reader =
            unsafe { JniParquetMetaReader::new(bytes.as_ptr(), bytes.len() as u64) }.unwrap();
        let value: i64 = 999;
        let filter = make_filter(
            0,
            1,
            FILTER_OP_EQ,
            &value as *const i64 as u64,
            ColumnTypeTag::Long as i32,
        );
        let buf_end = unsafe { (&value as *const i64).add(1) } as u64;
        let result = jni_reader
            .reader()
            .can_skip_row_group(0, &[filter], buf_end)
            .unwrap();
        assert!(result, "EQ 999 outside [10,200] → skip");
    }

    #[test]
    fn can_skip_eq_inside_min_max_via_cached_reader() {
        let bytes = build_long_pm(&[(100, 0, 10, 200)]);
        let jni_reader =
            unsafe { JniParquetMetaReader::new(bytes.as_ptr(), bytes.len() as u64) }.unwrap();
        let value: i64 = 50;
        let filter = make_filter(
            0,
            1,
            FILTER_OP_EQ,
            &value as *const i64 as u64,
            ColumnTypeTag::Long as i32,
        );
        let buf_end = unsafe { (&value as *const i64).add(1) } as u64;
        let result = jni_reader
            .reader()
            .can_skip_row_group(0, &[filter], buf_end)
            .unwrap();
        assert!(!result, "EQ 50 inside [10,200] → cannot skip");
    }

    #[test]
    fn can_skip_lt_below_min_via_cached_reader() {
        let bytes = build_long_pm(&[(100, 0, 10, 200)]);
        let jni_reader =
            unsafe { JniParquetMetaReader::new(bytes.as_ptr(), bytes.len() as u64) }.unwrap();
        let value: i64 = 10;
        let filter = make_filter(
            0,
            1,
            FILTER_OP_LT,
            &value as *const i64 as u64,
            ColumnTypeTag::Long as i32,
        );
        let buf_end = unsafe { (&value as *const i64).add(1) } as u64;
        let result = jni_reader
            .reader()
            .can_skip_row_group(0, &[filter], buf_end)
            .unwrap();
        assert!(result, "LT 10 with min=10 → all values >= 10, skip");
    }

    #[test]
    fn can_skip_gt_above_max_via_cached_reader() {
        let bytes = build_long_pm(&[(100, 0, 10, 200)]);
        let jni_reader =
            unsafe { JniParquetMetaReader::new(bytes.as_ptr(), bytes.len() as u64) }.unwrap();
        let value: i64 = 200;
        let filter = make_filter(
            0,
            1,
            FILTER_OP_GT,
            &value as *const i64 as u64,
            ColumnTypeTag::Long as i32,
        );
        let buf_end = unsafe { (&value as *const i64).add(1) } as u64;
        let result = jni_reader
            .reader()
            .can_skip_row_group(0, &[filter], buf_end)
            .unwrap();
        assert!(result, "GT 200 with max=200 → all values <= 200, skip");
    }

    #[test]
    fn can_skip_is_null_when_no_nulls_via_cached_reader() {
        let bytes = build_long_pm(&[(100, 0, 10, 200)]);
        let jni_reader =
            unsafe { JniParquetMetaReader::new(bytes.as_ptr(), bytes.len() as u64) }.unwrap();
        let filter = make_filter(0, 0, FILTER_OP_IS_NULL, 0, ColumnTypeTag::Long as i32);
        let result = jni_reader
            .reader()
            .can_skip_row_group(0, &[filter], 0)
            .unwrap();
        assert!(result, "IS NULL with null_count=0 → skip");
    }

    #[test]
    fn can_skip_row_group_index_out_of_range() {
        let bytes = build_long_pm(&[(100, 0, 10, 200)]);
        let jni_reader =
            unsafe { JniParquetMetaReader::new(bytes.as_ptr(), bytes.len() as u64) }.unwrap();
        let res = jni_reader.reader().can_skip_row_group(5, &[], 0);
        assert!(res.is_err(), "row group index 5 out of range");
    }

    #[test]
    fn cached_reader_handles_multiple_row_groups() {
        // Three row groups with disjoint ranges. The same JniParquetMetaReader
        // instance is reused for all three checks — this exercises the
        // cached-reader path that the refactor was built for.
        let bytes = build_long_pm(&[(100, 0, 0, 99), (100, 0, 100, 199), (100, 0, 200, 299)]);
        let jni_reader =
            unsafe { JniParquetMetaReader::new(bytes.as_ptr(), bytes.len() as u64) }.unwrap();

        // EQ 50 should skip rg 1 and rg 2 but not rg 0.
        let value: i64 = 50;
        let filter = make_filter(
            0,
            1,
            FILTER_OP_EQ,
            &value as *const i64 as u64,
            ColumnTypeTag::Long as i32,
        );
        let buf_end = unsafe { (&value as *const i64).add(1) } as u64;

        let r0 = jni_reader
            .reader()
            .can_skip_row_group(0, &[filter], buf_end)
            .unwrap();
        let r1 = jni_reader
            .reader()
            .can_skip_row_group(1, &[filter], buf_end)
            .unwrap();
        let r2 = jni_reader
            .reader()
            .can_skip_row_group(2, &[filter], buf_end)
            .unwrap();
        assert!(!r0, "rg 0 [0..99] contains 50, cannot skip");
        assert!(r1, "rg 1 [100..199] does not contain 50, skip");
        assert!(r2, "rg 2 [200..299] does not contain 50, skip");
    }
}
