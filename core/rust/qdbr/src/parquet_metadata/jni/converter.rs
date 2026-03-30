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

use crate::parquet::error::{ParquetError, ParquetErrorExt, ParquetResult};
use crate::parquet::io::FromRawFdI32Ext;
use crate::parquet::qdb_metadata::{QdbMeta, QDB_META_KEY};
use crate::parquet_metadata::convert::convert_from_parquet;
use jni::objects::JClass;
use jni::JNIEnv;
use parquet2::read::read_metadata_with_size;
use crate::parquet_metadata::error::{parquet_meta_err, ParquetMetaErrorKind};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::ManuallyDrop;

/// Reads parquet metadata, generates the `_pm` file, writes it, and returns
/// the pm file size. On error, throws a `CairoException` and returns -1.
#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_ParquetMetadataWriter_generate(
    mut env: JNIEnv,
    _class: JClass,
    parquet_fd: i32,
    parquet_file_size: i64,
    pm_fd: i32,
) -> i64 {
    if parquet_file_size < 0 {
        let err = parquet_meta_err!(
            ParquetMetaErrorKind::InvalidValue,
            "negative parquet file size: {}",
            parquet_file_size
        );
        return err.into_cairo_exception().throw::<i64>(&mut env);
    }
    match generate_pm(parquet_fd, parquet_file_size as u64, pm_fd) {
        Ok(pm_file_size) => pm_file_size as i64,
        Err(mut err) => {
            err.add_context("error in ParquetMetadataWriter.generate");
            err.into_cairo_exception().throw::<i64>(&mut env)
        }
    }
}

#[allow(clippy::explicit_auto_deref)]
fn generate_pm(parquet_fd: i32, parquet_file_size: u64, pm_fd: i32) -> ParquetResult<u64> {
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

    // Convert to _pm format.
    let (pm_bytes, _pm_footer_offset) = convert_from_parquet(
        &metadata,
        qdb_meta.as_ref(),
        parquet_footer_offset,
        footer_length,
    )
    .context("could not convert parquet metadata to pm")?;

    let pm_file_size = pm_bytes.len() as u64;

    // Write the _pm file. ManuallyDrop ensures the fd is never closed by Rust.
    let mut pm_file = ManuallyDrop::new(unsafe { File::from_raw_fd_i32(pm_fd) });
    pm_file
        .write_all(&pm_bytes)
        .map_err(ParquetError::from)
        .context("could not write pm file")?;

    Ok(pm_file_size)
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
