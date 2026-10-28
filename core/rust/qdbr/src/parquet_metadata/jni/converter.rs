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
use crate::parquet::qdb_metadata::extract_qdb_meta;
use crate::parquet_read::decode_column::{decode_single_timestamp_value, reconstruct_descriptor};
use jni::objects::JClass;
use jni::JNIEnv;
use memmap2::Mmap;
use parquet2::metadata::FileMetaData;
use parquet2::read::read_metadata_with_size;
use qdb_parquet_meta::convert::{
    convert_from_parquet, physical_type_to_u8, SliceBloomFilterSource, TsStatsBackfill,
};
use qdb_parquet_meta::error::{ParquetMetaError, ParquetMetaErrorKind};
use qdb_parquet_meta::types::{
    SeqTxn, FOOTER_CHECKSUM_SIZE, FOOTER_TRAILER_SIZE, HEADER_PARQUET_META_FILE_SIZE_OFF,
};
use qdb_parquet_meta::{ParquetMetaReader, ParquetMetaUpdateWriter};
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

/// Stamps `seq_txn` as the base seqTxn of the `_pm` footer that describes `parquet_file_size`,
/// appending a footer and committing it through the header. Throws a `CairoException` on error.
#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_ParquetMetadataWriter_setBaseSeqTxn(
    mut env: JNIEnv,
    _class: JClass,
    parquet_meta_fd: i32,
    parquet_file_size: i64,
    seq_txn: i64,
) {
    let env = &mut env;
    if parquet_file_size < 0 || seq_txn < 0 {
        let err = parquet_meta_err!(
            ParquetMetaErrorKind::InvalidValue,
            "invalid base seqTxn stamp [parquetFileSize={}, seqTxn={}]",
            parquet_file_size,
            seq_txn
        );
        return err.into_cairo_exception().throw::<()>(env);
    }
    // Java owns the fd: ManuallyDrop keeps Rust from closing it.
    let mut file = ManuallyDrop::new(unsafe { File::from_raw_fd_i32(parquet_meta_fd) });
    if let Err(mut err) = set_base_seq_txn(&mut file, parquet_file_size as u64, seq_txn) {
        err.add_context("error in ParquetMetadataWriter.setBaseSeqTxn");
        err.into_cairo_exception().throw::<()>(env)
    }
}

fn set_base_seq_txn(file: &mut File, parquet_file_size: u64, seq_txn: i64) -> ParquetResult<()> {
    // 1. Resolve the footer of `parquet_file_size` below the committed size the header holds.
    // SAFETY: Java holds the writer lock over the partition, and the map is dropped before any write.
    let mmap = unsafe { Mmap::map(&*file) }.map_err(ParquetError::from)?;
    let off = HEADER_PARQUET_META_FILE_SIZE_OFF;
    if mmap.len() < off + 8 {
        return Err(parquet_meta_err!(
            ParquetMetaErrorKind::Truncated,
            "_pm too small to hold its committed size: {} bytes",
            mmap.len()
        ));
    }
    let committed_size = u64::from_le_bytes(mmap[off..off + 8].try_into().expect("8 bytes"));
    let (offset, footer) =
        ParquetMetaReader::find_footer_for_parquet_size(&mmap, committed_size, parquet_file_size)?;
    let resolved_size =
        offset + footer.crc_offset() as u64 + (FOOTER_CHECKSUM_SIZE + FOOTER_TRAILER_SIZE) as u64;
    let reader = ParquetMetaReader::from_file_size(&mmap[..], resolved_size)?;
    reader.verify_checksum()?;

    // 2. A replay after the header write keeps the stamp it finds.
    let seq_txn = SeqTxn::new(seq_txn);
    if reader.seq_txn() == Some(seq_txn) {
        drop(mmap);
        file.sync_all().map_err(ParquetError::from)?;
        return Ok(());
    }

    // 3. Append a footer with the stamp, preserving any newer footer past the resolved one.
    let mut update = ParquetMetaUpdateWriter::new(&mmap[..], resolved_size)?;
    update.seq_txn(seq_txn);
    let (append_bytes, new_parquet_meta_file_size) = update.finish_appending_at(committed_size)?;
    drop(mmap);
    file.seek(SeekFrom::Start(committed_size))
        .map_err(ParquetError::from)?;
    file.write_all(&append_bytes).map_err(ParquetError::from)?;
    file.sync_data().map_err(ParquetError::from)?;

    // 4. Commit it: the header's size field points at the new footer last.
    file.seek(SeekFrom::Start(off as u64))
        .map_err(ParquetError::from)?;
    file.write_all(&new_parquet_meta_file_size.to_le_bytes())
        .map_err(ParquetError::from)?;
    file.sync_data().map_err(ParquetError::from)?;
    Ok(())
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
    use super::*;
    use qdb_parquet_meta::types::ColumnFlags;
    use qdb_parquet_meta::{ParquetMetaWriter, RowGroupBlockBuilder};

    // A `_pm` whose only footer describes a 4360-byte parquet file.
    fn write_pm(seq_txn: i64) -> (tempfile::NamedTempFile, u64) {
        let mut writer = ParquetMetaWriter::new();
        writer.designated_timestamp(0);
        writer.add_column("ts", 0, 8, ColumnFlags::new(), 0, 0, 0, 0);
        let mut row_group = RowGroupBlockBuilder::new(1);
        row_group.set_num_rows(10);
        writer.add_row_group(row_group);
        writer.parquet_footer(4096, 256);
        writer.seq_txn(SeqTxn::new(seq_txn));
        let (bytes, _) = writer.finish().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(&bytes).unwrap();
        (file, 4096 + 256 + 8)
    }

    fn stamp(file: &tempfile::NamedTempFile, parquet_size: u64) -> Option<SeqTxn> {
        let bytes = std::fs::read(file.path()).unwrap();
        let committed = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let (offset, footer) =
            ParquetMetaReader::find_footer_for_parquet_size(&bytes, committed, parquet_size)
                .unwrap();
        let size = offset + footer.crc_offset() as u64 + 8;
        ParquetMetaReader::from_file_size(&bytes, size)
            .unwrap()
            .seq_txn()
    }

    #[test]
    fn set_base_seq_txn_replaces_a_lower_or_higher_stamp() {
        for (old, new) in [(7, 42), (42, 7)] {
            let (pm, parquet_size) = write_pm(old);
            let mut file = File::options()
                .read(true)
                .write(true)
                .open(pm.path())
                .unwrap();
            set_base_seq_txn(&mut file, parquet_size, new).unwrap();
            assert_eq!(stamp(&pm, parquet_size), Some(SeqTxn::new(new)));
            // A replay finds its own stamp and appends nothing.
            let len = std::fs::metadata(pm.path()).unwrap().len();
            set_base_seq_txn(&mut file, parquet_size, new).unwrap();
            assert_eq!(std::fs::metadata(pm.path()).unwrap().len(), len);
        }
    }

    #[test]
    fn set_base_seq_txn_refuses_an_unknown_parquet_size() {
        let (pm, parquet_size) = write_pm(7);
        let mut file = File::options()
            .read(true)
            .write(true)
            .open(pm.path())
            .unwrap();
        assert!(set_base_seq_txn(&mut file, parquet_size + 1, 9).is_err());
        assert_eq!(stamp(&pm, parquet_size), Some(SeqTxn::new(7)));
    }
}
