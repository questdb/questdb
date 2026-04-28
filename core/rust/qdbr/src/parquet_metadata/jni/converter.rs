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

//! JNI binding for generating a `_pm` metadata file from a parquet file.

use crate::allocator::QdbAllocator;
use crate::parquet::error::parquet_meta_err;
use crate::parquet::error::{fmt_err, ParquetError, ParquetErrorExt, ParquetResult};
use crate::parquet::io::FromRawFdI32Ext;
use crate::parquet::qdb_metadata::{QdbMeta, QDB_META_KEY};
use crate::parquet_metadata::convert::{
    convert_from_parquet, detect_designated_timestamp, extract_sorting_columns,
};
use crate::parquet_metadata::error::ParquetMetaErrorKind;
use crate::parquet_read::decode_column::{decode_single_timestamp_value, reconstruct_descriptor};
use jni::objects::JClass;
use jni::JNIEnv;
use memmap2::Mmap;
use parquet2::metadata::FileMetaData;
use parquet2::read::read_metadata_with_size;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::ManuallyDrop;

/// Reads parquet metadata, generates the `_pm` file, writes it, and returns
/// the parquet meta file size. On error, throws a `CairoException` and returns -1.
///
/// `allocator` is a `QdbAllocator` pointer (from `Unsafe.getNativeAllocator`)
/// used only when we have to backfill missing designated-timestamp statistics.
#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_ParquetMetadataWriter_generate(
    mut env: JNIEnv,
    _class: JClass,
    allocator: *const QdbAllocator,
    parquet_fd: i32,
    parquet_file_size: i64,
    parquet_meta_fd: i32,
) -> i64 {
    let env = &mut env;
    if parquet_file_size < 0 {
        let err = parquet_meta_err!(
            ParquetMetaErrorKind::InvalidValue,
            "negative parquet file size: {}",
            parquet_file_size
        );
        return err.into_cairo_exception().throw::<i64>(env);
    }
    match generate_parquet_meta(
        allocator,
        parquet_fd,
        parquet_file_size as u64,
        parquet_meta_fd,
    ) {
        Ok(parquet_meta_file_size) => parquet_meta_file_size as i64,
        Err(mut err) => {
            err.add_context("error in ParquetMetadataWriter.generate");
            err.into_cairo_exception().throw::<i64>(env)
        }
    }
}

#[allow(clippy::explicit_auto_deref)]
fn generate_parquet_meta(
    allocator: *const QdbAllocator,
    parquet_fd: i32,
    parquet_file_size: u64,
    parquet_meta_fd: i32,
) -> ParquetResult<u64> {
    // Wrap fds in ManuallyDrop so Rust never closes them — Java owns the fds.
    let mut parquet_file = ManuallyDrop::new(unsafe { File::from_raw_fd_i32(parquet_fd) });

    // Read parquet file metadata.
    let metadata = read_metadata_with_size(&mut *parquet_file, parquet_file_size)
        .context("could not read parquet metadata")?;

    // Extract optional QuestDB metadata.
    let qdb_meta = extract_qdb_meta(&metadata)?;

    // Compute parquet footer offset and length.
    // Parquet file layout: [data] [thrift footer] [footer_length: i32 LE] [PAR1: 4B]
    // So footer_length is at file_size - 8, and footer starts at file_size - 8 - footer_length.
    let footer_length = read_parquet_footer_length(&mut *parquet_file, parquet_file_size)
        .context("could not read parquet footer length")?;
    let parquet_footer_offset = parquet_file_size
        .checked_sub(8)
        .and_then(|s| s.checked_sub(footer_length as u64))
        .ok_or_else(|| {
            parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "parquet footer length {} exceeds file size {}",
                footer_length,
                parquet_file_size
            )
        })?;

    // If a designated timestamp is present, mmap the parquet so we can
    // backfill missing inline min/max stats for that column. mmap overhead is
    // negligible on the migration path. `convert_from_parquet` only invokes
    // the closure when a row group's ts chunk is missing inline stats, so for
    // well-formed QDB parquets the mmap is paid but never read.
    let sorting_cols = extract_sorting_columns(&metadata)?;
    let designated_ts = detect_designated_timestamp(&metadata, qdb_meta.as_ref(), &sorting_cols);

    let mmap = if designated_ts >= 0 {
        // Safety: we hold an exclusive ManuallyDrop borrow on `parquet_file`
        // for the duration of the mmap, and the file is not truncated or
        // written to concurrently because Mig940 opens it read-only.
        let m = unsafe { Mmap::map(&*parquet_file) }.map_err(|e| {
            parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "could not mmap parquet file: {}",
                e
            )
        })?;
        Some(m)
    } else {
        None
    };

    let (parquet_meta_bytes, _parquet_meta_footer_offset) = if let Some(ref file_data) = mmap {
        let ts_col = designated_ts as usize;
        let backfill = |rg_idx: usize, row_lo: usize, row_hi: usize| -> ParquetResult<i64> {
            decode_single_ts_value_from_parquet(
                allocator, file_data, &metadata, rg_idx, ts_col, row_lo, row_hi,
            )
        };
        convert_from_parquet(
            &metadata,
            qdb_meta.as_ref(),
            parquet_footer_offset,
            footer_length,
            Some(&backfill),
        )
    } else {
        convert_from_parquet(
            &metadata,
            qdb_meta.as_ref(),
            parquet_footer_offset,
            footer_length,
            None,
        )
    }
    .context("could not convert parquet metadata to parquet meta file")?;

    let parquet_meta_file_size = parquet_meta_bytes.len() as u64;

    // Write the _pm file. ManuallyDrop ensures the fd is never closed by Rust.
    let mut parquet_meta_file =
        ManuallyDrop::new(unsafe { File::from_raw_fd_i32(parquet_meta_fd) });
    parquet_meta_file
        .write_all(&parquet_meta_bytes)
        .map_err(ParquetError::from)
        .context("could not write parquet meta file")?;
    // Persist the new _pm before the JNI call returns. Java is expected to
    // fsync as well, but doing it here provides defence in depth: every
    // caller of this entry point gets a durable _pm regardless of the Java
    // close ordering.
    parquet_meta_file
        .sync_data()
        .map_err(ParquetError::from)
        .context("could not fsync parquet meta file")?;

    Ok(parquet_meta_file_size)
}

/// Decode a single i64 timestamp value from a parquet column chunk using
/// `FileMetaData`. Mirrors `decode_single_ts_from_pm` but reads from parquet
/// metadata directly, so it is callable from Mig940 before any `_pm` exists.
pub(crate) fn decode_single_ts_value_from_parquet(
    allocator: *const QdbAllocator,
    file_data: &[u8],
    metadata: &FileMetaData,
    rg_idx: usize,
    ts_col: usize,
    row_lo: usize,
    row_hi: usize,
) -> ParquetResult<i64> {
    let rg_meta = metadata.row_groups.get(rg_idx).ok_or_else(|| {
        fmt_err!(
            InvalidType,
            "row group index {} out of range [0, {})",
            rg_idx,
            metadata.row_groups.len()
        )
    })?;
    let cols = rg_meta.columns();
    let col_chunk = cols.get(ts_col).ok_or_else(|| {
        fmt_err!(
            InvalidType,
            "timestamp column index {} out of range [0, {})",
            ts_col,
            cols.len()
        )
    })?;
    let (byte_range_start, total_compressed) = col_chunk.byte_range();
    let col_desc = col_chunk.descriptor();
    let field_info = &col_desc.descriptor.primitive_type.field_info;
    let column_name = field_info.name.clone();
    let descriptor = reconstruct_descriptor(
        super::super::convert::physical_type_to_u8(
            col_desc.descriptor.primitive_type.physical_type,
        ),
        match col_desc.descriptor.primitive_type.physical_type {
            parquet2::schema::types::PhysicalType::FixedLenByteArray(len) => len as i32,
            _ => 0,
        },
        col_desc.descriptor.max_rep_level.try_into().unwrap_or(0),
        col_desc.descriptor.max_def_level.try_into().unwrap_or(0),
        &column_name,
        field_info.repetition,
    );
    decode_single_timestamp_value(
        allocator,
        file_data,
        byte_range_start as usize,
        total_compressed as usize,
        col_chunk.compression(),
        descriptor,
        col_chunk.num_values().max(0),
        &column_name,
        rg_idx,
        row_lo,
        row_hi,
    )
}

fn extract_qdb_meta(metadata: &parquet2::metadata::FileMetaData) -> ParquetResult<Option<QdbMeta>> {
    let Some(kvs) = metadata.key_value_metadata.as_ref() else {
        return Ok(None);
    };
    let Some(kv) = kvs.iter().find(|kv| kv.key == QDB_META_KEY) else {
        return Ok(None);
    };
    let Some(json) = kv.value.as_deref() else {
        return Ok(None);
    };
    let meta = QdbMeta::deserialize(json)?;
    Ok(Some(meta))
}

fn read_parquet_footer_length(file: &mut File, file_size: u64) -> ParquetResult<u32> {
    // The last 8 bytes of a parquet file are: [footer_length: i32 LE] [PAR1: 4B]
    let seek_pos = file_size.checked_sub(8).ok_or_else(|| {
        parquet_meta_err!(
            ParquetMetaErrorKind::Truncated,
            "parquet file too small: {} bytes",
            file_size
        )
    })?;
    file.seek(SeekFrom::Start(seek_pos))
        .map_err(ParquetError::from)?;
    let mut buf = [0u8; 4];
    file.read_exact(&mut buf).map_err(ParquetError::from)?;
    Ok(u32::from_le_bytes(buf))
}
