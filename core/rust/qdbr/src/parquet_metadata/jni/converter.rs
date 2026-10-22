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

//! JNI bindings that generate a `_pm` metadata file from a parquet file and record its writer in an
//! existing one.

use crate::allocator::QdbAllocator;
use crate::parquet::error::parquet_meta_err;
use crate::parquet::error::{fmt_err, ParquetError, ParquetErrorExt, ParquetResult};
use crate::parquet::io::FromRawFdI32Ext;
use crate::parquet::qdb_metadata::extract_qdb_meta;
use crate::parquet_read::decode_column::{decode_single_timestamp_value, reconstruct_descriptor};
use crate::parquet_write::update::publish_parquet_meta_with_sync;
use jni::objects::JClass;
use jni::JNIEnv;
use memmap2::Mmap;
use parquet2::metadata::FileMetaData;
use parquet2::read::read_metadata_with_size;
use qdb_parquet_meta::convert::{
    convert_from_parquet, is_written_by_questdb, physical_type_to_u8, SliceBloomFilterSource,
    TsStatsBackfill,
};
use qdb_parquet_meta::error::{ParquetMetaError, ParquetMetaErrorKind};
use qdb_parquet_meta::reader::ParquetMetaReader;
use qdb_parquet_meta::writer::ParquetMetaUpdateWriter;
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

/// Records in an existing `_pm` that QuestDB's parquet writer wrote the partition, for a `_pm`
/// written before the record existed. Appends a footer that repeats the committed snapshot with
/// `WRITTEN_BY_QUESTDB_BIT` set and publishes it through the header, as an incremental update
/// does, so a crash before the header patch leaves the committed snapshot in place.
///
/// Returns the new `_pm` size, 0 when the committed footer already carries the bit, and -1 when
/// another writer wrote the parquet file or the committed footer does not describe
/// `parquet_file_size` (a stale `_pm` the table writer regenerates). On error, throws a
/// `CairoException`.
#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_ParquetMetadataWriter_markWrittenByQuestDB(
    mut env: JNIEnv,
    _class: JClass,
    parquet_fd: i32,
    parquet_file_size: i64,
    parquet_meta_fd: i32,
) -> i64 {
    let env = &mut env;
    let res = (|| -> ParquetResult<i64> {
        let parquet_file_size = u64::try_from(parquet_file_size).map_err(|_| {
            parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "negative parquet file size: {}",
                parquet_file_size
            )
        })?;
        // Java owns the fds.
        let mut parquet_file = ManuallyDrop::new(unsafe { File::from_raw_fd_i32(parquet_fd) });
        let mut parquet_meta_file =
            ManuallyDrop::new(unsafe { File::from_raw_fd_i32(parquet_meta_fd) });
        mark_written_by_questdb(&mut parquet_file, parquet_file_size, &mut parquet_meta_file)
    })();
    match res {
        Ok(result) => result,
        Err(mut err) => {
            err.add_context("error in ParquetMetadataWriter.markWrittenByQuestDB");
            err.into_cairo_exception().throw::<i64>(env)
        }
    }
}

pub(crate) fn mark_written_by_questdb(
    parquet_file: &mut File,
    parquet_file_size: u64,
    parquet_meta_file: &mut File,
) -> ParquetResult<i64> {
    let mut committed_size = [0u8; 8];
    parquet_meta_file.seek(SeekFrom::Start(0))?;
    parquet_meta_file.read_exact(&mut committed_size)?;
    let committed_size = u64::from_le_bytes(committed_size);
    let file_size = parquet_meta_file.metadata()?.len();
    if committed_size > file_size {
        return Err(parquet_meta_err!(
            ParquetMetaErrorKind::Truncated,
            "_pm header size {} exceeds file size {}",
            committed_size,
            file_size
        ));
    }
    let mut existing = vec![0u8; committed_size as usize];
    parquet_meta_file.seek(SeekFrom::Start(0))?;
    parquet_meta_file
        .read_exact(&mut existing)
        .context("could not read _pm file")?;

    let reader = ParquetMetaReader::from_file_size(&existing, committed_size)?;
    reader.verify_checksum()?;
    let footer = reader.footer();
    if footer.parquet_footer_offset() + footer.parquet_footer_length() as u64 + 8
        != parquet_file_size
    {
        return Ok(-1);
    }
    if footer.feature_flags().is_written_by_questdb() {
        return Ok(0);
    }
    let metadata = read_metadata_with_size(parquet_file, parquet_file_size)
        .context("could not read parquet metadata")?;
    if !is_written_by_questdb(metadata.created_by.as_deref()) {
        return Ok(-1);
    }

    let mut updater = ParquetMetaUpdateWriter::new(&existing, committed_size)?;
    updater.inherit_seq_txn();
    updater.written_by_questdb(true);
    let (append_bytes, new_size) = updater.finish_appending_at(committed_size)?;
    parquet_meta_file.seek(SeekFrom::Start(committed_size))?;
    parquet_meta_file
        .write_all(&append_bytes)
        .context("could not append _pm footer")?;
    publish_parquet_meta_with_sync(parquet_meta_file, new_size, true, true, File::sync_data)?;
    Ok(new_size as i64)
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

    // mmap the parquet file so the converter can: (1) inline parquet bloom
    // bitsets into `_pm`, and (2) backfill missing inline min/max ts stats
    // for the designated timestamp column. mmap overhead is negligible on
    // the migration path; both lookups are invoked only when needed.
    //
    // Safety: we hold an exclusive ManuallyDrop borrow on `parquet_file` for
    // the duration of the mmap, and the file is not truncated or written to
    // concurrently because Mig940/snapshot-restore open it read-only.
    let file_data = unsafe { Mmap::map(&*parquet_file) }.map_err(|e| {
        parquet_meta_err!(
            ParquetMetaErrorKind::InvalidValue,
            "could not mmap parquet file: {}",
            e
        )
    })?;
    let bloom_source = SliceBloomFilterSource::new(&file_data);

    let sorting_cols =
        qdb_parquet_meta::convert::resolve_sorting_columns(&metadata, qdb_meta.as_ref())
            .map_err(ParquetError::from)?;
    let designated_ts = qdb_parquet_meta::convert::detect_designated_timestamp(
        &metadata,
        qdb_meta.as_ref(),
        &sorting_cols,
    );

    let backfill: Option<Box<TsStatsBackfill<'_>>> = if designated_ts >= 0 {
        let ts_col = designated_ts as usize;
        let allocator_ptr = allocator;
        let metadata_ref = &metadata;
        let file_data_ref: &[u8] = &file_data;
        Some(Box::new(move |rg_idx, row_lo, row_hi| {
            decode_single_ts_value_from_parquet(
                allocator_ptr,
                file_data_ref,
                metadata_ref,
                rg_idx,
                ts_col,
                row_lo,
                row_hi,
            )
            .map_err(|e| {
                ParquetMetaError::with_descr(ParquetMetaErrorKind::Conversion, e.to_string())
            })
        }))
    } else {
        None
    };

    let (parquet_meta_bytes, _parquet_meta_footer_offset) = convert_from_parquet(
        &metadata,
        qdb_meta.as_ref(),
        parquet_footer_offset,
        footer_length,
        &bloom_source,
        backfill.as_deref(),
    )
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
/// metadata directly, so it is callable from Mig941 before any `_pm` exists.
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
        physical_type_to_u8(col_desc.descriptor.primitive_type.physical_type),
        match col_desc.descriptor.primitive_type.physical_type {
            parquet2::schema::types::PhysicalType::FixedLenByteArray(len) => len as i32,
            _ => 0,
        },
        col_desc.descriptor.max_rep_level.try_into().unwrap_or(0),
        col_desc.descriptor.max_def_level.try_into().unwrap_or(0),
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

#[cfg(test)]
mod tests {
    use super::mark_written_by_questdb;
    use crate::parquet::tests::ColumnTypeTagExt;
    use crate::parquet_write::file::ParquetWriter;
    use crate::parquet_write::schema::{Column, Partition};
    use parquet2::read::read_metadata_with_size;
    use qdb_core::col_type::ColumnTypeTag;
    use qdb_parquet_meta::convert::{convert_from_parquet, NoBloomFilterSource};
    use qdb_parquet_meta::reader::ParquetMetaReader;
    use std::fs::{File, OpenOptions};
    use std::io::{Cursor, Read, Seek, SeekFrom, Write};
    use tempfile::NamedTempFile;

    fn questdb_parquet(values: &[f64]) -> Vec<u8> {
        let data: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
        let column = Column::from_raw_data(
            0,
            "d",
            ColumnTypeTag::Double.into_type().code(),
            0,
            values.len(),
            data.as_ptr(),
            data.len(),
            std::ptr::null(),
            0,
            std::ptr::null(),
            0,
            false,
            false,
            0,
        )
        .unwrap();
        let mut buf = Cursor::new(Vec::new());
        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .finish(Partition { table: "t".to_string(), columns: vec![column] })
            .unwrap();
        buf.into_inner()
    }

    /// Writes the parquet bytes and a `_pm` built from them as a writer from before
    /// `WRITTEN_BY_QUESTDB_BIT` built it, or as it builds one for a file with `created_by`.
    fn files(parquet: &[u8], created_by: Option<&str>) -> (NamedTempFile, NamedTempFile) {
        let mut parquet_file = NamedTempFile::new().unwrap();
        parquet_file.write_all(parquet).unwrap();
        let mut metadata =
            read_metadata_with_size(&mut Cursor::new(parquet), parquet.len() as u64).unwrap();
        metadata.created_by = created_by.map(str::to_string);
        let footer_length = u32::from_le_bytes(
            parquet[parquet.len() - 8..parquet.len() - 4]
                .try_into()
                .unwrap(),
        );
        let footer_offset = (parquet.len() - 8 - footer_length as usize) as u64;
        let (pm, _) = convert_from_parquet(
            &metadata,
            None,
            footer_offset,
            footer_length,
            &NoBloomFilterSource,
            None,
        )
        .unwrap();
        let mut pm_file = NamedTempFile::new().unwrap();
        pm_file.write_all(&pm).unwrap();
        (parquet_file, pm_file)
    }

    fn mark(parquet: &NamedTempFile, parquet_size: u64, pm: &NamedTempFile) -> i64 {
        let mut parquet_file = File::open(parquet.path()).unwrap();
        let mut pm_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(pm.path())
            .unwrap();
        mark_written_by_questdb(&mut parquet_file, parquet_size, &mut pm_file).unwrap()
    }

    fn read_pm(pm: &NamedTempFile) -> Vec<u8> {
        let mut bytes = Vec::new();
        File::open(pm.path())
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();
        bytes
    }

    fn committed_footer_is_written_by_questdb(pm: &[u8]) -> bool {
        let size = u64::from_le_bytes(pm[..8].try_into().unwrap());
        let reader = ParquetMetaReader::from_file_size(pm, size).unwrap();
        reader.verify_checksum().unwrap();
        reader.footer().feature_flags().is_written_by_questdb()
    }

    #[test]
    fn mark_appends_a_footer_to_a_questdb_file_once() {
        let parquet = questdb_parquet(&[1.0, 2.0, 3.0]);
        let (parquet_file, pm_file) = files(&parquet, None);
        let before = read_pm(&pm_file);
        assert!(!committed_footer_is_written_by_questdb(&before));

        let new_size = mark(&parquet_file, parquet.len() as u64, &pm_file);
        assert!(new_size as usize > before.len());
        let after = read_pm(&pm_file);
        assert_eq!(new_size as usize, after.len());
        assert!(committed_footer_is_written_by_questdb(&after));
        // The committed snapshot stays byte for byte in place, and the new footer chains to it.
        assert_eq!(before[8..], after[8..before.len()]);
        let reader = ParquetMetaReader::from_file_size(&after, new_size as u64).unwrap();
        assert_eq!(
            reader.footer().prev_parquet_meta_file_size(),
            before.len() as u64
        );
        assert_eq!(reader.row_group_count(), 1);

        assert_eq!(0, mark(&parquet_file, parquet.len() as u64, &pm_file));
        assert_eq!(after, read_pm(&pm_file));
    }

    #[test]
    fn mark_leaves_another_writers_file_and_a_stale_pm_alone() {
        let mut parquet = questdb_parquet(&[1.0, 2.0, 3.0]);
        let created_by = b"QuestDB version 9.0";
        let at = parquet
            .windows(created_by.len())
            .position(|w| w == created_by)
            .unwrap();
        parquet[at] = b'X';
        let (parquet_file, pm_file) = files(&parquet, None);
        let before = read_pm(&pm_file);
        assert_eq!(-1, mark(&parquet_file, parquet.len() as u64, &pm_file));
        assert_eq!(before, read_pm(&pm_file));

        let parquet = questdb_parquet(&[1.0, 2.0, 3.0]);
        let (parquet_file, pm_file) = files(&parquet, None);
        let before = read_pm(&pm_file);
        assert_eq!(-1, mark(&parquet_file, parquet.len() as u64 + 1, &pm_file));
        assert_eq!(before, read_pm(&pm_file));
    }

    #[test]
    fn mark_fails_on_a_corrupt_pm_or_parquet_file() {
        let parquet = questdb_parquet(&[1.0, 2.0, 3.0]);
        let try_mark = |parquet: &[u8], pm: &[u8]| {
            let mut parquet_file = NamedTempFile::new().unwrap();
            parquet_file.write_all(parquet).unwrap();
            let mut pm_file = NamedTempFile::new().unwrap();
            pm_file.write_all(pm).unwrap();
            let mut parquet_file = File::open(parquet_file.path()).unwrap();
            let mut pm_file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(pm_file.path())
                .unwrap();
            mark_written_by_questdb(&mut parquet_file, parquet.len() as u64, &mut pm_file)
        };
        let (_, pm_file) = files(&parquet, None);
        let pm = read_pm(&pm_file);

        assert!(try_mark(&parquet, &[]).is_err());
        let mut oversized = pm.clone();
        oversized[..8].copy_from_slice(&(pm.len() as u64 + 1).to_le_bytes());
        assert!(try_mark(&parquet, &oversized).is_err());
        let mut truncated = pm.clone();
        truncated[..8].copy_from_slice(&16u64.to_le_bytes());
        assert!(try_mark(&parquet, &truncated).is_err());
        let mut corrupt = pm.clone();
        corrupt[40] ^= 0xFF;
        assert!(try_mark(&parquet, &corrupt).is_err());
        // The _pm describes the file, but its parquet footer does not parse.
        let mut unreadable = parquet.clone();
        let footer_length = u32::from_le_bytes(
            parquet[parquet.len() - 8..parquet.len() - 4]
                .try_into()
                .unwrap(),
        );
        let footer_offset = parquet.len() - 8 - footer_length as usize;
        unreadable[footer_offset..parquet.len() - 8].fill(0xFF);
        assert!(try_mark(&unreadable, &pm).is_err());
    }

    #[test]
    fn mark_overwrites_the_tail_a_crash_before_the_header_patch_left() {
        let parquet = questdb_parquet(&[1.0, 2.0, 3.0]);
        let (parquet_file, pm_file) = files(&parquet, None);
        let before = read_pm(&pm_file);
        {
            let mut file = OpenOptions::new()
                .append(true)
                .open(pm_file.path())
                .unwrap();
            file.write_all(&[0xAB; 37]).unwrap();
        }
        assert!(mark(&parquet_file, parquet.len() as u64, &pm_file) > before.len() as i64);
        let after = read_pm(&pm_file);
        let size = u64::from_le_bytes(after[..8].try_into().unwrap());
        assert!(committed_footer_is_written_by_questdb(
            &after[..size as usize]
        ));

        let mut header = [0u8; 8];
        let mut file = File::open(pm_file.path()).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.read_exact(&mut header).unwrap();
        assert_eq!(size, u64::from_le_bytes(header));
    }
}
