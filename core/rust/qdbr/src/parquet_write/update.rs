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
use crate::allocator::QdbAllocator;
use crate::parquet::error::{
    fmt_err, ParquetError, ParquetErrorExt, ParquetErrorReason, ParquetResult,
};
use crate::parquet::qdb_metadata::{QdbMeta, QdbMetaCol, QdbMetaColFormat, QDB_META_KEY};
use crate::parquet_metadata::types::SeqTxn;
use crate::parquet_write::file::{create_row_group, WriteOptions};
use crate::parquet_write::schema::{
    to_compressions, to_encodings, to_parquet_schema, Column, Partition,
};
use parquet2::compression::CompressionOptions;
use parquet2::encoding::uleb128;
use parquet2::metadata::{FileMetaData, KeyValue, SchemaDescriptor, SortingColumn};
use parquet2::read::{read_metadata_with_footer_bytes, read_metadata_with_size};
use parquet2::schema::types::ParquetType;
use parquet2::schema::Repetition;
use parquet2::write;
use parquet2::write::footer_cache::FooterCache;
use parquet2::write::{
    write_row_group, ColumnOffsetsMetadata, CopiedColumnIndex, ParquetFile, Version,
};
use parquet_format_safe::thrift::protocol::{TCompactInputProtocol, TCompactOutputProtocol};
use parquet_format_safe::{
    ColumnChunk, ColumnMetaData, CompressionCodec, DataPageHeader, DictionaryPageHeader,
    Encoding as ThriftEncoding, OffsetIndex, PageEncodingStats, PageHeader, PageType, RowGroup,
    Type,
};
use qdb_core::col_type::{ColumnType, ColumnTypeTag};
use qdb_parquet_meta::convert::resolve_column_id;
use rapidhash::RapidHashMap;
use std::collections::HashSet;
use std::fs::File;
use std::io::{Read as _, Seek, SeekFrom, Write as _};

/// Computes the contiguous byte range [start, end) of a row group's column
/// data, including the last column's bloom filter when present.  Non-last
/// columns' bloom filters sit between column chunks and are already covered
/// by the byte ranges; only the last column's bloom filter can extend past
/// the final column chunk.
trait RowGroupByteRange {
    fn data_byte_range<R: std::io::Read + Seek>(&self, reader: &mut R)
        -> ParquetResult<(u64, u64)>;
}

impl RowGroupByteRange for parquet2::metadata::RowGroupMetaData {
    fn data_byte_range<R: std::io::Read + Seek>(
        &self,
        reader: &mut R,
    ) -> ParquetResult<(u64, u64)> {
        let columns = self.columns();
        if columns.is_empty() {
            return Ok((0, 0));
        }
        let mut rg_start = u64::MAX;
        let mut rg_end = 0u64;
        let mut last_col_idx = 0usize;
        for (i, col) in columns.iter().enumerate() {
            let (start, len) = col.byte_range();
            rg_start = rg_start.min(start);
            let end = start + len;
            if end >= rg_end {
                rg_end = end;
                last_col_idx = i;
            }
        }
        if columns[last_col_idx]
            .metadata()
            .bloom_filter_offset
            .is_some()
        {
            let bf_total = parquet2::bloom_filter::total_size(&columns[last_col_idx], reader)?;
            if bf_total > 0 {
                let bf_offset = columns[last_col_idx]
                    .metadata()
                    .bloom_filter_offset
                    .unwrap() as u64;
                rg_end = rg_end.max(bf_offset + bf_total);
            }
        }
        Ok((rg_start, rg_end))
    }
}

#[repr(C)]
pub struct ParquetUpdater {
    allocator: QdbAllocator,
    reader: File,
    read_file_size: u64,
    parquet_file: ParquetFile<File>,
    compression_options: CompressionOptions,
    row_group_size: Option<usize>,
    data_page_size: Option<usize>,
    raw_array_encoding: bool,
    bloom_filter_columns: HashSet<usize>,
    min_compression_ratio: f64,
    copy_buffer: Vec<u8>,
    file_metadata: FileMetaData,
    accumulated_unused_bytes: u64,
    old_footer_size: u64,
    is_rewrite: bool,
    symbol_schema_checked: bool,
    result_file_size: u64,
    result_unused_bytes: u64,
    target_qdb_meta: Option<QdbMeta>,
    target_col_id_to_pos: Option<RapidHashMap<i32, usize>>,
    /// Per-column ASCII flag from the old file's QDB metadata, keyed by parquet
    /// field_id. Used to skip `is_column_ascii()` scans when the old flag is
    /// already `false`. Keyed by id (not by schema position) so a partition
    /// whose column order differs from the file's schema still resolves correctly.
    old_ascii: RapidHashMap<i32, bool>,
    /// Per-column ASCII flag computed from written (inserted/replaced) row groups,
    /// keyed by parquet field_id (== Column::id). `true` = all written values are
    /// ASCII; `false` = at least one non-ASCII value; absent = column not written.
    /// Keying by id rather than by partition position guarantees end() looks up
    /// the right column even if the partition layout drifts across calls.
    written_ascii: RapidHashMap<i32, bool>,
    parquet_meta_fd: Option<File>,
    parquet_meta_file_size: u64,
    // The append base: the `_pm` offset-0 header (the reader's getFileSize()),
    // threaded in from Java parallel to the parse anchor (parquet_meta_file_size).
    // New incremental bytes land here, strictly past any orphaned dead footer a
    // rolled-back update left between the parse anchor and the append base.
    append_base: u64,
    // The existing parquet data-file size, used only as a first-time (`<= 0`) vs
    // incremental gate in end(). It is not a `_pm` size.
    existing_parquet_file_size: i64,
    result_parquet_meta_size: i64,
    /// Apply-time `seqTxn` for the new `_pm` snapshot.
    seq_txn: SeqTxn,
    // Per-VARCHAR-column "still all-ASCII" tracker, keyed by parquet field_id.
    // Seeded at construction from the old qdb_meta's ascii flag:
    //   old.ascii == Some(true)  -> initial value `true`  (scan new aux to verify)
    //   old.ascii == Some(false) -> initial value `false` (any copied old row
    //                               group already carries non-ASCII bytes, so
    //                               the final flag can only be `Some(false)`)
    //   old.ascii == None        -> initial value `false` (unknown about old,
    //                               so be conservative)
    // set_target_schema() additionally seeds fresh VARCHAR columns from
    // ADD COLUMN as `true`: existing rows backfill with nulls, which
    // is_column_ascii skips, so a new column is trivially all-ASCII until
    // a write proves otherwise. Each write call updates the tracker only
    // for columns whose value is still `true`, so the scan is skipped in
    // the common case where the column already contains a non-ASCII byte.
    // Columns missing from the map at end() (no old entry and no
    // set_target_schema seeding) resolve to `false` via unwrap_or(false).
    varchar_all_ascii: RapidHashMap<i32, bool>,
    /// Deterministic work counters used by resource/performance regression
    /// tests. They also make hybrid-rewrite behavior observable without a
    /// timing-sensitive wall-clock assertion.
    hybrid_copied_column_chunks: u64,
    hybrid_encoded_column_chunks: u64,
}

fn publish_parquet_meta_with_sync<S>(
    parquet_meta_file: &mut File,
    new_file_size: u64,
    sync: bool,
    sync_tail_before_publish: bool,
    mut sync_data: S,
) -> ParquetResult<()>
where
    S: FnMut(&File) -> std::io::Result<()>,
{
    if sync && sync_tail_before_publish {
        sync_data(parquet_meta_file)
            .map_err(ParquetError::from)
            .context("could not fsync appended _pm snapshot before publication")?;
    }
    parquet_meta_file
        .seek(SeekFrom::Start(
            crate::parquet_metadata::types::HEADER_PARQUET_META_FILE_SIZE_OFF as u64,
        ))
        .map_err(ParquetError::from)?;
    parquet_meta_file
        .write_all(&new_file_size.to_le_bytes())
        .map_err(ParquetError::from)
        .context("could not patch header parquet_meta_file_size in _pm file")?;
    if sync {
        sync_data(parquet_meta_file)
            .map_err(ParquetError::from)
            .context("could not fsync published _pm header")?;
    }
    Ok(())
}

impl ParquetUpdater {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        allocator: QdbAllocator,
        mut reader: File,
        read_file_size: u64,
        writer: File,
        write_file_size: u64,
        sorting_columns: Option<Vec<SortingColumn>>,
        write_statistics: bool,
        raw_array_encoding: bool,
        compression_options: CompressionOptions,
        row_group_size: Option<usize>,
        data_page_size: Option<usize>,
        bloom_filter_fpp: f64,
        min_compression_ratio: f64,
        parquet_meta_fd: Option<File>,
        parquet_meta_file_size: u64,
        append_base: u64,
        existing_parquet_file_size: i64,
        seq_txn: SeqTxn,
    ) -> ParquetResult<Self> {
        fn version_from(value: i32) -> ParquetResult<Version> {
            match value {
                1 => Ok(Version::V1),
                2 => Ok(Version::V2),
                _ => Err(fmt_err!(
                    InvalidLayout,
                    "invalid parquet version number: {value}"
                )),
            }
        }

        let is_rewrite = write_file_size == 0;

        let (metadata, footer_cache, old_footer_size) = if is_rewrite {
            let metadata = read_metadata_with_size(&mut reader, read_file_size)?;
            (metadata, None, 0u64)
        } else {
            // In update mode, also capture raw footer bytes for incremental serialization.
            let (metadata, raw_footer_bytes, footer_size) =
                read_metadata_with_footer_bytes(&mut reader, read_file_size)?;
            let cache = FooterCache::from_footer_bytes(raw_footer_bytes).map_err(|e| {
                ParquetError::with_descr(
                    ParquetErrorReason::Parquet2(parquet2::error::Error::oos(e.to_string())),
                    "could not build footer cache",
                )
            })?;
            (metadata, Some(cache), footer_size)
        };

        let file_metadata = metadata.clone();

        // Validate that the file was written by QuestDB and has consistent metadata.
        // O3 merge relies on QuestDB-specific metadata (column types, symbol tables,
        // unused_bytes tracking) that external Parquet writers don't produce.
        let num_parquet_cols = metadata.schema_descr.columns().len();
        let qdb_meta = metadata.key_value_metadata.as_ref().and_then(|kvs| {
            kvs.iter()
                .find(|kv| kv.key == QDB_META_KEY)
                .and_then(|kv| kv.value.as_ref())
        });
        let source_qdb_meta = match qdb_meta {
            None => {
                return Err(fmt_err!(
                    InvalidLayout,
                    "parquet file lacks '{}' metadata key; O3 merge requires files written by QuestDB",
                    QDB_META_KEY
                ));
            }
            Some(raw) => {
                let meta = QdbMeta::deserialize(raw)?;
                if meta.schema.len() != num_parquet_cols {
                    return Err(fmt_err!(
                        InvalidLayout,
                        "QuestDB metadata schema has {} columns but parquet schema has {}",
                        meta.schema.len(),
                        num_parquet_cols
                    ));
                }
                meta
            }
        };

        // The caller's `sorting_columns` use the raw timestamp slot, stale after a
        // DROP COLUMN; recompute from the dense designated position in the source
        // qdb_meta, keeping a sort column only when both the caller and source have
        // one. Schema-change rewrites recompute it again in set_target_schema.
        let sorting_columns = sorting_columns.and(designated_ts_sorting_columns(&source_qdb_meta));

        // Detect which columns had bloom filters in the original file.
        let bloom_filter_columns = if let Some(first_rg) = metadata.row_groups.first() {
            first_rg
                .columns()
                .iter()
                .enumerate()
                .filter_map(|(i, col)| {
                    if col.metadata().bloom_filter_offset.is_some() {
                        Some(i)
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            HashSet::new()
        };

        let version = version_from(metadata.version)?;
        let created_by = metadata.created_by.clone();
        let schema = metadata.schema_descr.clone();

        // Append/update matches the source's statistics setting so the file does
        // not end up with footer min/max on some row groups but not others: encode
        // them for the new groups iff the source carries any chunk statistics.
        // Read that from the statistics, not the page index. ADD COLUMN raw-copies
        // the data columns (keeping their stats) without a ColumnIndex and
        // backfills a stats-less null column, so a file can legitimately carry
        // statistics with no index; hence `any`, not `all` (the null column has
        // none). ColumnIndex uniformity is a separate decision, gated by
        // source_column_indexed in end(). A rewrite keeps the runtime config; an
        // empty source has nothing to match.
        let write_statistics = if is_rewrite || metadata.row_groups.is_empty() {
            write_statistics
        } else {
            metadata
                .row_groups
                .iter()
                .any(|rg| rg.columns().iter().any(|c| c.statistics().is_some()))
        };
        let options = write::WriteOptions { write_statistics, version, bloom_filter_fpp };

        // Seed the per-column "still all-ASCII" tracker from the old qdb_meta.
        // Columns whose old ascii was already Some(false) or None get `false`
        // here, which lets track_new_data_ascii short-circuit their per-write
        // aux scan: there is no path back to Some(true) once any old data has
        // been seen as non-ASCII (or as unknown), because copied row groups
        // carry that data through. Only columns starting at `true` are
        // scanned, and the scan downgrades them to `false` on the first
        // non-ASCII entry.
        let mut varchar_all_ascii: RapidHashMap<i32, bool> = RapidHashMap::default();
        if let Some(old_qdb) = metadata
            .key_value_metadata
            .as_ref()
            .and_then(|kvs| {
                kvs.iter()
                    .find(|kv| kv.key == QDB_META_KEY)
                    .and_then(|kv| kv.value.as_ref())
            })
            .map(|raw| QdbMeta::deserialize(raw))
            .transpose()?
        {
            let schema_cols = metadata.schema_descr.columns();
            for (i, col) in old_qdb.schema.iter().enumerate() {
                if col.column_type.tag() != ColumnTypeTag::Varchar {
                    continue;
                }
                if let Some(field_id) = schema_cols
                    .get(i)
                    .and_then(|c| c.base_type.get_field_info().id)
                {
                    varchar_all_ascii.insert(field_id, col.ascii == Some(true));
                }
            }
        }

        let (parquet_file, accumulated_unused_bytes) = if is_rewrite {
            // Rewrite mode: write to a fresh file
            let pf = ParquetFile::with_sorting_columns(
                writer,
                schema,
                options,
                created_by,
                sorting_columns,
            );
            (pf, 0u64)
        } else {
            // Update mode: append to existing file.
            // The upfront guard already validated QDB metadata exists and parses,
            // so unwrap_or(0) only covers the default for missing unused_bytes field.
            let accumulated_unused_bytes = metadata
                .key_value_metadata
                .as_ref()
                .and_then(|kvs| {
                    kvs.iter()
                        .find(|kv| kv.key == QDB_META_KEY)
                        .and_then(|kv| kv.value.as_ref())
                })
                .map(|raw| QdbMeta::deserialize(raw))
                .transpose()?
                .map(|m| m.unused_bytes)
                .unwrap_or(0);

            // Seek writer to end of file so new data is appended after existing content.
            // The reader and writer are separate fds; reading metadata only moves the reader cursor.
            let mut writer = writer;
            writer.seek(SeekFrom::Start(write_file_size))?;

            let pf = ParquetFile::new_updater(
                writer,
                write_file_size,
                schema,
                options,
                created_by,
                sorting_columns,
                metadata.into_thrift(),
                footer_cache,
            );
            (pf, accumulated_unused_bytes)
        };

        // Build an id-keyed map from the old file's QDB metadata: pair each
        // QdbMetaCol with the parquet field_id from the corresponding old schema
        // field at the same position. Entries without a known ascii flag are
        // omitted (track_ascii treats the absence as "no signal").
        let old_ascii: RapidHashMap<i32, bool> = file_metadata
            .key_value_metadata
            .as_ref()
            .and_then(|kvs| {
                kvs.iter()
                    .find(|kv| kv.key == QDB_META_KEY)
                    .and_then(|kv| kv.value.as_ref())
                    .and_then(|v| QdbMeta::deserialize(v).ok())
            })
            .map(|m| {
                let old_fields = file_metadata.schema_descr.fields();
                m.schema
                    .iter()
                    .zip(old_fields.iter())
                    .filter_map(|(col, field)| {
                        field
                            .get_field_info()
                            .id
                            .and_then(|id| col.ascii.map(|a| (id, a)))
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(ParquetUpdater {
            allocator,
            reader,
            read_file_size,
            parquet_file,
            compression_options,
            raw_array_encoding,
            row_group_size,
            data_page_size,
            bloom_filter_columns,
            min_compression_ratio,
            copy_buffer: Vec::new(),
            file_metadata,
            accumulated_unused_bytes,
            old_footer_size,
            is_rewrite,
            symbol_schema_checked: false,
            result_file_size: 0,
            result_unused_bytes: 0,
            target_qdb_meta: None,
            target_col_id_to_pos: None,
            old_ascii,
            written_ascii: RapidHashMap::default(),
            parquet_meta_fd,
            parquet_meta_file_size,
            append_base,
            existing_parquet_file_size,
            result_parquet_meta_size: -1,
            seq_txn,
            varchar_all_ascii,
            hybrid_copied_column_chunks: 0,
            hybrid_encoded_column_chunks: 0,
        })
    }

    /// Inspect VARCHAR aux for any column whose flag is still `true` in
    /// varchar_all_ascii and downgrade it to `false` if a non-ASCII entry is
    /// found. Columns whose flag is already `false` are skipped entirely:
    /// the result cannot return to `Some(true)` once any old or new data has
    /// been seen as non-ASCII (or as unknown), so there is no reason to
    /// re-scan their aux on every row-group write.
    fn track_new_data_ascii(&mut self, partition: &Partition) {
        use super::varchar::is_column_ascii;
        for col in &partition.columns {
            if col.data_type.tag() != ColumnTypeTag::Varchar {
                continue;
            }
            // Only scan when there is still a chance of staying all-ASCII.
            // Missing entries are treated as `false` (e.g. brand-new columns
            // from ADD COLUMN that are not yet in the tracker).
            if !self
                .varchar_all_ascii
                .get(&col.id)
                .copied()
                .unwrap_or(false)
            {
                continue;
            }
            if col.secondary_data.is_empty() {
                continue;
            }
            // SAFETY: aux originates from JNI/Java memory-mapped column data and is
            // page-aligned. Each entry is exactly 16 bytes.
            let aux: &[[u8; 16]] = unsafe { super::util::transmute_slice(col.secondary_data) };
            if !is_column_ascii(aux) {
                self.varchar_all_ascii.insert(col.id, false);
            }
        }
    }

    /// Computes the per-column ASCII flag from the partition's VARCHAR aux data
    /// and ANDs it into `written_ascii`, keyed by `Column::id` (== parquet field_id).
    /// Called for each inserted/replaced row group.
    ///
    /// Skips the aux scan when the result is already determined to be `false`
    /// (from a previous row group or from the old file's metadata in update mode).
    fn track_ascii(&mut self, partition: &Partition) {
        for col in partition.columns.iter() {
            if col.data_type.tag() != ColumnTypeTag::Varchar || col.secondary_data.is_empty() {
                continue;
            }
            // Already determined non-ASCII from a previous row group — skip scan.
            if matches!(self.written_ascii.get(&col.id), Some(false)) {
                continue;
            }
            // In update mode, if the old file's flag is already false the final
            // result (old AND written) will be false regardless — skip scan.
            if !self.is_rewrite && matches!(self.old_ascii.get(&col.id), Some(false)) {
                self.written_ascii.insert(col.id, false);
                continue;
            }
            // SAFETY: secondary_data contains native VARCHAR aux entries (16 bytes each).
            let aux: &[[u8; 16]] = unsafe { super::util::transmute_slice(col.secondary_data) };
            let is_ascii = super::varchar::is_column_ascii(aux);
            let merged = match self.written_ascii.get(&col.id) {
                Some(&prev) => prev && is_ascii,
                None => is_ascii,
            };
            self.written_ascii.insert(col.id, merged);
        }
    }

    pub fn replace_row_group(
        &mut self,
        partition: &Partition,
        row_group_id: i32,
    ) -> ParquetResult<()> {
        self.track_ascii(partition);
        self.ensure_schema_matches_columns(partition)?;
        self.track_new_data_ascii(partition);
        let row_count = partition
            .columns
            .first()
            .ok_or_else(|| fmt_err!(InvalidLayout, "replace_row_group: partition has no columns"))?
            .row_count;
        let options = self.row_group_options();
        let (row_group, bloom_hashes) = create_row_group(
            partition,
            0,
            row_count,
            self.parquet_file.schema().fields(),
            &to_encodings(partition),
            options,
            &to_compressions(partition),
            &self.bloom_filter_columns,
            false,
        )?;

        if self.is_rewrite {
            self.parquet_file
                .write(row_group, &bloom_hashes)
                .with_context(|_| {
                    format!("Failed to write row group {row_group_id} in rewrite mode")
                })
        } else {
            // Track the old row group's bytes that will become dead space.
            if row_group_id < 0 {
                return Err(fmt_err!(
                    InvalidLayout,
                    "replace_row_group: negative row_group_id: {}",
                    row_group_id
                ));
            }
            let rg_idx = row_group_id as usize;
            if rg_idx < self.file_metadata.row_groups.len() {
                let old_rg = &self.file_metadata.row_groups[rg_idx];

                let (rg_start, rg_end) =
                    old_rg.data_byte_range(&mut self.reader).with_context(|_| {
                        format!(
                            "replace_row_group: failed to compute byte range for rg {}",
                            rg_idx,
                        )
                    })?;
                if rg_start < rg_end {
                    self.accumulated_unused_bytes += rg_end - rg_start;
                }

                // Column/offset indexes are stored separately from row group data.
                for col in old_rg.columns() {
                    if let Some(len) = col.column_index_length() {
                        self.accumulated_unused_bytes += len as u64;
                    }
                    if let Some(len) = col.offset_index_length() {
                        self.accumulated_unused_bytes += len as u64;
                    }
                }
            }

            self.parquet_file
                .replace(row_group, Some(row_group_id), &bloom_hashes)
                .with_context(|_| format!("Failed to replace row group {row_group_id}"))
        }
    }

    pub fn insert_row_group(&mut self, partition: &Partition, position: i32) -> ParquetResult<()> {
        self.track_ascii(partition);
        self.ensure_schema_matches_columns(partition)?;
        self.track_new_data_ascii(partition);
        let row_count = partition
            .columns
            .first()
            .ok_or_else(|| fmt_err!(InvalidLayout, "insert_row_group: partition has no columns"))?
            .row_count;
        let options = self.row_group_options();
        let (row_group, bloom_hashes) = create_row_group(
            partition,
            0,
            row_count,
            self.parquet_file.schema().fields(),
            &to_encodings(partition),
            options,
            &to_compressions(partition),
            &self.bloom_filter_columns,
            false,
        )?;

        if self.is_rewrite {
            self.parquet_file
                .write(row_group, &bloom_hashes)
                .with_context(|_| {
                    format!("Failed to write row group at position {position} in rewrite mode")
                })
        } else {
            self.parquet_file
                .insert(row_group, position, &bloom_hashes)
                .with_context(|_| format!("Failed to insert row group at position {position}"))
        }
    }

    pub fn copy_row_group(&mut self, rg_index: i32) -> ParquetResult<()> {
        if rg_index < 0 {
            return Err(fmt_err!(
                InvalidLayout,
                "copy_row_group: negative rg_index: {}",
                rg_index
            ));
        }
        let rg_idx = rg_index as usize;
        if rg_idx >= self.file_metadata.row_groups.len() {
            return Err(fmt_err!(
                InvalidLayout,
                "copy_row_group: row group index {} out of range [0,{})",
                rg_idx,
                self.file_metadata.row_groups.len()
            ));
        }

        let old_rg = &self.file_metadata.row_groups[rg_idx];
        let row_count = old_rg.num_rows();

        let (rg_start, rg_end) = old_rg.data_byte_range(&mut self.reader).with_context(|_| {
            format!(
                "copy_row_group: failed to compute byte range for rg {}",
                rg_idx
            )
        })?;

        if rg_start >= rg_end {
            return Err(fmt_err!(
                InvalidLayout,
                "copy_row_group: empty byte range for row group {}",
                rg_idx
            ));
        }

        // Read the raw bytes from the reader file, reusing the buffer across copies.
        let raw_len = (rg_end - rg_start) as usize;
        self.copy_buffer.resize(raw_len, 0);
        self.reader.seek(SeekFrom::Start(rg_start))?;
        self.reader.read_exact(&mut self.copy_buffer)?;

        // Extract bloom filter bitsets from the copy buffer before taking columns.
        // The copy buffer contains data from rg_start..rg_end, which includes
        // bloom filters. The bloom filter offset is absolute in the old file;
        // subtract rg_start to get the offset within the copy buffer.
        let bloom_bitsets = extract_bloom_bitsets_from_buffer(
            &self.file_metadata.row_groups[rg_idx],
            &self.copy_buffer,
            rg_start,
        );

        // Ensure the PAR1 file header is written before computing offsets.
        self.parquet_file.ensure_started().map_err(|s| {
            ParquetError::with_descr(
                ParquetErrorReason::Parquet2(s),
                "Failed to write file header before raw copy",
            )
        })?;

        let new_offset = self.parquet_file.current_offset();
        let offset_delta = new_offset as i64 - rg_start as i64;

        // Take ownership of columns — each row group is processed exactly once.
        let mut columns: Vec<ColumnChunk> = self.file_metadata.row_groups[rg_idx]
            .take_columns()
            .into_iter()
            .map(|c| c.into_thrift())
            .collect();

        // Capture the source page index before adjust_column_chunk_offsets
        // clears the index pointers.
        let copied_page_index = build_copied_page_index(&columns, &mut self.reader, offset_delta)?;

        for col in &mut columns {
            adjust_column_chunk_offsets(col, offset_delta);
        }

        let sorting_columns = self.parquet_file.sorting_columns().map(<[_]>::to_vec);
        let thrift_rg = build_raw_row_group(columns, row_count, sorting_columns)?;

        self.parquet_file
            .write_raw_row_group_with_index(
                &self.copy_buffer,
                thrift_rg,
                bloom_bitsets,
                copied_page_index,
            )
            .map_err(|s| {
                ParquetError::with_descr(
                    ParquetErrorReason::Parquet2(s),
                    format!("Failed to raw-copy row group {rg_idx}"),
                )
            })
    }

    /// Sets the target schema for the output file, replacing the schema read
    /// from the input file. Use this when the table schema has changed (ADD/DROP
    /// COLUMN) since the parquet file was written. The target schema defines
    /// the column layout in the output footer.
    ///
    /// Also builds a target QdbMeta that `end()` uses instead of the old file's
    /// metadata. Format hints (e.g. `LocalKeyIsGlobal` for SYMBOL columns)
    /// are preserved from the old schema for columns that still exist.
    pub fn set_target_schema(&mut self, partition: &Partition) -> ParquetResult<()> {
        let (schema, _kv) =
            to_parquet_schema(partition, self.raw_array_encoding, -1, SeqTxn::UNSET)?;
        self.parquet_file.set_schema(schema);

        // Build column_id → old schema index from the old file's parquet field_ids.
        let old_fields = self.file_metadata.schema_descr.fields();
        let old_col_id_to_idx: RapidHashMap<i32, usize> = old_fields
            .iter()
            .enumerate()
            .filter_map(|(i, f)| f.get_field_info().id.map(|id| (id, i)))
            .collect();

        let old_qdb_meta = self
            .file_metadata
            .key_value_metadata
            .as_ref()
            .and_then(|kvs| {
                kvs.iter()
                    .find(|kv| kv.key == QDB_META_KEY)
                    .and_then(|kv| kv.value.as_ref())
            })
            .and_then(|v| QdbMeta::deserialize(v).ok());

        let mut qdb_meta = QdbMeta::new(partition.columns.len());
        for col in &partition.columns {
            let is_existing_col = old_col_id_to_idx.contains_key(&col.id);

            // Preserve format hint from old schema for existing columns.
            let format = old_col_id_to_idx
                .get(&col.id)
                .and_then(|&old_idx| {
                    old_qdb_meta
                        .as_ref()
                        .and_then(|m| m.schema.get(old_idx))
                        .and_then(|c| c.format)
                })
                .or_else(|| {
                    // New column: set format based on type.
                    if col.data_type.tag() == ColumnTypeTag::Symbol {
                        Some(QdbMetaColFormat::LocalKeyIsGlobal)
                    } else {
                        None
                    }
                });

            // Seed a fresh VARCHAR column added by ADD COLUMN as `true`:
            // existing rows backfill with nulls (which is_column_ascii
            // skips), so the column is trivially all-ASCII until a write
            // proves otherwise. track_new_data_ascii will downgrade it on
            // the first non-ASCII aux entry. Existing columns already have
            // a tracker entry from new() and must not be overwritten here.
            if !is_existing_col && col.data_type.tag() == ColumnTypeTag::Varchar {
                self.varchar_all_ascii.insert(col.id, true);
            }

            let column_type = if col.designated_timestamp {
                col.data_type
                    .into_designated_with_order(col.designated_timestamp_ascending)?
            } else {
                col.data_type
            };

            let ascii = old_col_id_to_idx
                .get(&col.id)
                .and_then(|&old_idx| old_qdb_meta.as_ref().and_then(|m| m.schema.get(old_idx)))
                .and_then(|old_col| old_col.ascii);

            qdb_meta.schema.push(QdbMetaCol {
                column_type,
                column_top: 0,
                format,
                ascii,
                id: Some(col.id),
            });
        }

        // Cache column_id → target schema position map for use during
        // copy_row_group_with_null_columns(). The target schema is invariant
        // across all row group copies, so building this once avoids a HashMap
        // allocation per row group.
        let target_fields = self.parquet_file.schema().fields();
        self.target_col_id_to_pos = Some(
            target_fields
                .iter()
                .enumerate()
                .filter_map(|(i, f)| f.get_field_info().id.map(|id| (id, i)))
                .collect(),
        );

        // Re-stamp the sort order from the dense target schema: ADD/DROP COLUMN
        // can shift the timestamp index. Must run before the copy/insert calls
        // that follow, which read sorting_columns().
        self.parquet_file
            .set_sorting_columns(designated_ts_sorting_columns(&qdb_meta));

        self.target_qdb_meta = Some(qdb_meta);
        Ok(())
    }

    /// Copies an existing row group from the input file and appends null column
    /// chunks for columns that are missing from the old schema but present in
    /// the target schema. `null_columns` contains `(target_schema_position,
    /// column_type)` pairs for each column that needs a null chunk.
    pub fn copy_row_group_with_null_columns(
        &mut self,
        rg_index: i32,
        null_columns: &[(usize, ColumnType)],
    ) -> ParquetResult<()> {
        if rg_index < 0 {
            return Err(fmt_err!(
                InvalidLayout,
                "copy_row_group_with_null_columns: negative rg_index: {}",
                rg_index
            ));
        }
        let rg_idx = rg_index as usize;
        if rg_idx >= self.file_metadata.row_groups.len() {
            return Err(fmt_err!(
                InvalidLayout,
                "copy_row_group_with_null_columns: row group index {} out of range [0,{})",
                rg_idx,
                self.file_metadata.row_groups.len()
            ));
        }

        let old_rg = &self.file_metadata.row_groups[rg_idx];
        let row_count = old_rg.num_rows();

        // Determine byte range of existing column chunk data, including
        // the last column's bloom filter when present.
        let (rg_start, rg_end) = old_rg.data_byte_range(&mut self.reader).with_context(|_| {
            format!(
                "copy_row_group_with_null_columns: failed to compute byte range for rg {}",
                rg_idx
            )
        })?;
        if rg_start >= rg_end {
            return Err(fmt_err!(
                InvalidLayout,
                "copy_row_group_with_null_columns: empty byte range for row group {}",
                rg_idx
            ));
        }

        // Read existing raw bytes, reusing the buffer across copies.
        let raw_len = (rg_end - rg_start) as usize;
        self.copy_buffer.resize(raw_len, 0);
        self.reader.seek(SeekFrom::Start(rg_start))?;
        self.reader.read_exact(&mut self.copy_buffer)?;

        // Extract bloom filter bitsets before taking columns. These are indexed
        // in source (pre-drop) column order; the remap loop below moves them into
        // target order in lockstep with merged_cols.
        let mut source_bloom_bitsets = extract_bloom_bitsets_from_buffer(
            &self.file_metadata.row_groups[rg_idx],
            &self.copy_buffer,
            rg_start,
        );

        self.parquet_file.ensure_started().map_err(|s| {
            ParquetError::with_descr(
                ParquetErrorReason::Parquet2(s),
                "Failed to write file header before raw copy with null columns",
            )
        })?;

        let new_offset = self.parquet_file.current_offset();
        let offset_delta = new_offset as i64 - rg_start as i64;

        // Use the cached column_id → target schema position map built in
        // set_target_schema(). This avoids a HashMap allocation per row group.
        let target_fields = self.parquet_file.schema().fields();
        let old_fields = self.file_metadata.schema_descr.fields();
        let target_col_id_to_pos = self.target_col_id_to_pos.as_ref().ok_or_else(|| {
            fmt_err!(
                InvalidLayout,
                "copy_row_group_with_null_columns: target schema not set"
            )
        })?;

        // Merge existing and null column chunks in target schema order. Each
        // survivor's bloom bitset moves to the same target slot: the bitsets were
        // extracted in source order, but a DROP COLUMN shifts surviving columns,
        // and the _pm writer indexes bitsets positionally against target columns.
        let target_col_count = target_fields.len();
        let mut merged_cols: Vec<Option<ColumnChunk>> = vec![None; target_col_count];
        let mut bloom_bitsets: Vec<Option<Vec<u8>>> = vec![None; target_col_count];

        // Take ownership of existing columns — each row group is processed exactly once.
        // NLL allows mutable access here because `old_rg` is no longer used.
        let existing_cols = self.file_metadata.row_groups[rg_idx].take_columns();
        for (old_pos, col_meta) in existing_cols.into_iter().enumerate() {
            let id = old_fields
                .get(old_pos)
                .and_then(|f: &ParquetType| f.get_field_info().id);
            if let Some(&target_pos) = id.and_then(|id| target_col_id_to_pos.get(&id)) {
                let mut col_chunk = col_meta.into_thrift();
                adjust_column_chunk_offsets(&mut col_chunk, offset_delta);
                merged_cols[target_pos] = Some(col_chunk);
                if let Some(bitset) = source_bloom_bitsets.get_mut(old_pos) {
                    bloom_bitsets[target_pos] = bitset.take();
                }
            }
            // else: dropped column — skip (dead bytes in raw copy)
        }

        // Generate null column chunk bytes for missing columns.
        // Null column bytes are appended after the existing raw data.
        let null_chunk_offset_base = new_offset + raw_len as u64;
        let mut null_bytes_buf: Vec<u8> = Vec::new();

        for &(target_pos, col_type) in null_columns {
            let field = target_fields.get(target_pos).ok_or_else(|| {
                fmt_err!(
                    InvalidLayout,
                    "null column target position {} out of target schema range {}",
                    target_pos,
                    target_fields.len()
                )
            })?;

            let col_offset = null_chunk_offset_base + null_bytes_buf.len() as u64;
            let (chunk_bytes, thrift_col) =
                generate_null_column_chunk_bytes(field, col_type, row_count, col_offset)?;
            null_bytes_buf.extend_from_slice(&chunk_bytes);
            merged_cols[target_pos] = Some(thrift_col);
        }

        // Collect into final column list; every slot must be filled.
        let columns: Vec<ColumnChunk> = merged_cols
            .into_iter()
            .enumerate()
            .map(|(i, slot)| {
                slot.ok_or_else(|| {
                    fmt_err!(
                        InvalidLayout,
                        "copy_row_group_with_null_columns: merged column slot {} is empty \
                         (target schema has {} columns)",
                        i,
                        target_col_count
                    )
                })
            })
            .collect::<ParquetResult<Vec<_>>>()?;

        let sorting_columns = self.parquet_file.sorting_columns().map(<[_]>::to_vec);
        let thrift_rg = build_raw_row_group(columns, row_count, sorting_columns)?;

        // Concatenate existing + null bytes and write as one raw row group.
        self.copy_buffer.extend_from_slice(&null_bytes_buf);

        self.parquet_file
            .write_raw_row_group_with_bloom(&self.copy_buffer, thrift_rg, bloom_bitsets)
            .map_err(|s| {
                ParquetError::with_descr(
                    ParquetErrorReason::Parquet2(s),
                    format!("Failed to raw-copy row group {rg_idx} with null columns"),
                )
            })
    }

    /// Rewrites only the columns supplied in `partition` and streams every
    /// other target-schema column chunk byte-for-byte from the source row
    /// group. The supplied columns are matched by stable parquet field id, so
    /// their order does not need to match the target schema.
    ///
    /// This operation is rewrite-only and requires `set_target_schema()` first.
    /// Chunk statistics, encodings, compression, bloom filters, and page
    /// indexes are preserved for copied columns; changed columns get freshly
    /// generated page indexes.
    pub fn rewrite_row_group_columns(
        &mut self,
        rg_index: i32,
        partition: &Partition,
    ) -> ParquetResult<()> {
        if !self.is_rewrite {
            return Err(fmt_err!(
                InvalidLayout,
                "rewrite_row_group_columns requires rewrite mode"
            ));
        }
        if rg_index < 0 {
            return Err(fmt_err!(
                InvalidLayout,
                "rewrite_row_group_columns: negative rg_index: {}",
                rg_index
            ));
        }
        let rg_idx = rg_index as usize;
        if rg_idx >= self.file_metadata.row_groups.len() {
            return Err(fmt_err!(
                InvalidLayout,
                "rewrite_row_group_columns: row group index {} out of range [0,{})",
                rg_idx,
                self.file_metadata.row_groups.len()
            ));
        }
        if partition.columns.is_empty() {
            return Err(fmt_err!(
                InvalidLayout,
                "rewrite_row_group_columns: no changed columns"
            ));
        }
        if self.target_col_id_to_pos.is_none() {
            return Err(fmt_err!(
                InvalidLayout,
                "rewrite_row_group_columns: target schema not set"
            ));
        }

        let row_count = self.file_metadata.row_groups[rg_idx].num_rows();
        if partition
            .columns
            .iter()
            .any(|col| col.row_count != row_count)
        {
            return Err(fmt_err!(
                InvalidLayout,
                "rewrite_row_group_columns: changed column row count does not match source row group {}",
                row_count
            ));
        }

        self.track_ascii(partition);
        self.track_new_data_ascii(partition);

        let target_fields = self.parquet_file.schema().fields().to_vec();
        let target_descriptors = self.parquet_file.schema().columns().to_vec();
        let target_col_count = target_fields.len();
        let target_col_id_to_pos = self.target_col_id_to_pos.as_ref().unwrap();
        let old_fields = self.file_metadata.schema_descr.fields().to_vec();
        let old_col_id_to_pos: RapidHashMap<i32, usize> = old_fields
            .iter()
            .enumerate()
            .filter_map(|(i, field)| field.get_field_info().id.map(|id| (id, i)))
            .collect();

        let mut changed_target_positions = Vec::with_capacity(partition.columns.len());
        let mut changed_ids = HashSet::with_capacity(partition.columns.len());
        for col in &partition.columns {
            let target_pos = target_col_id_to_pos.get(&col.id).copied().ok_or_else(|| {
                fmt_err!(
                    InvalidLayout,
                    "rewrite_row_group_columns: changed column id {} is absent from target schema",
                    col.id
                )
            })?;
            if !changed_ids.insert(col.id) {
                return Err(fmt_err!(
                    InvalidLayout,
                    "rewrite_row_group_columns: duplicate changed column id {}",
                    col.id
                ));
            }
            changed_target_positions.push(target_pos);
        }

        self.parquet_file.ensure_started().map_err(|e| {
            ParquetError::with_descr(
                ParquetErrorReason::Parquet2(e),
                "Failed to write file header before hybrid row-group rewrite",
            )
        })?;
        let output_start = self.parquet_file.current_offset();
        let mut bytes_written = 0u64;
        let mut columns: Vec<Option<ColumnChunk>> = vec![None; target_col_count];
        let mut bloom_bitsets: Vec<Option<Vec<u8>>> = vec![None; target_col_count];
        let mut copied_page_index = std::iter::repeat_with(|| None)
            .take(target_col_count)
            .collect::<Vec<_>>();
        let mut stream_buf = [0u8; 64 * 1024];

        // Stream unchanged compressed page bytes (and their serialized bloom
        // filters) directly from source to destination. Only the fixed-size
        // transfer buffer above is retained, independent of table width.
        for (target_pos, target_field) in target_fields.iter().enumerate() {
            let target_id = target_field.get_field_info().id.ok_or_else(|| {
                fmt_err!(
                    InvalidLayout,
                    "rewrite_row_group_columns: target column {} has no field id",
                    target_pos
                )
            })?;
            if changed_ids.contains(&target_id) {
                continue;
            }
            let source_pos = old_col_id_to_pos.get(&target_id).copied().ok_or_else(|| {
                fmt_err!(
                    InvalidLayout,
                    "rewrite_row_group_columns: unchanged target column id {} is absent from source",
                    target_id
                )
            })?;
            let source_col = self.file_metadata.row_groups[rg_idx].columns()[source_pos].clone();
            let source_descriptor = source_col.descriptor();
            let target_descriptor = &target_descriptors[target_pos];
            let source_primitive = &source_descriptor.descriptor.primitive_type;
            let target_primitive = &target_descriptor.descriptor.primitive_type;
            if source_primitive.physical_type != target_primitive.physical_type
                || source_primitive.logical_type != target_primitive.logical_type
                || source_primitive.converted_type != target_primitive.converted_type
                || source_descriptor.descriptor.max_def_level
                    != target_descriptor.descriptor.max_def_level
                || source_descriptor.descriptor.max_rep_level
                    != target_descriptor.descriptor.max_rep_level
            {
                return Err(fmt_err!(
                    InvalidLayout,
                    "rewrite_row_group_columns: unchanged column id {} has incompatible source and target parquet types",
                    target_id
                ));
            }

            let (source_data_offset, source_data_len) = source_col.byte_range();
            let target_data_offset = output_start + bytes_written;
            let offset_delta = target_data_offset as i64 - source_data_offset as i64;
            copied_page_index[target_pos] = build_copied_column_index(
                source_col.column_chunk(),
                &mut self.reader,
                offset_delta,
            )?;
            copy_file_range(
                &mut self.reader,
                self.parquet_file.writer_mut(),
                source_data_offset,
                source_data_len,
                &mut stream_buf,
            )?;
            bytes_written += source_data_len;

            let mut thrift_col = source_col.column_chunk().clone();
            adjust_column_chunk_offsets(&mut thrift_col, offset_delta);
            if let Some(source_bloom_offset) = source_col
                .metadata()
                .bloom_filter_offset
                .filter(|&offset| offset > 0)
            {
                let bloom_size = parquet2::bloom_filter::total_size(&source_col, &mut self.reader)?;
                if bloom_size > 0 {
                    let bloom_size_usize = usize::try_from(bloom_size).map_err(|_| {
                        fmt_err!(
                            InvalidLayout,
                            "rewrite_row_group_columns: bloom filter size {} does not fit usize",
                            bloom_size
                        )
                    })?;
                    let mut bloom_bytes = Vec::new();
                    bloom_bytes.try_reserve_exact(bloom_size_usize).map_err(|_| {
                        fmt_err!(
                            InvalidLayout,
                            "rewrite_row_group_columns: bloom filter size {} exceeds available memory",
                            bloom_size
                        )
                    })?;
                    bloom_bytes.resize(bloom_size_usize, 0);
                    self.reader
                        .seek(SeekFrom::Start(source_bloom_offset as u64))?;
                    self.reader.read_exact(&mut bloom_bytes)?;
                    let target_bloom_offset = output_start + bytes_written;
                    self.parquet_file.writer_mut().write_all(&bloom_bytes)?;
                    bytes_written += bloom_size as u64;
                    thrift_col.meta_data.as_mut().unwrap().bloom_filter_offset =
                        Some(target_bloom_offset as i64);
                    bloom_bitsets[target_pos] =
                        parquet2::bloom_filter::read_from_slice_at_offset(0, &bloom_bytes)
                            .ok()
                            .filter(|bitset| !bitset.is_empty())
                            .map(<[u8]>::to_vec);
                }
            }
            thrift_col.meta_data.as_mut().unwrap().path_in_schema =
                target_descriptor.path_in_schema.clone();
            columns[target_pos] = Some(thrift_col);
        }

        // Encode only changed columns. Their temporary schema is deliberately
        // narrow, so the encoder's CPU and working set scale with the number of
        // changes rather than with the full table width.
        let (changed_schema, _) =
            to_parquet_schema(partition, self.raw_array_encoding, -1, SeqTxn::UNSET)?;
        let changed_bloom_columns: HashSet<usize> = changed_target_positions
            .iter()
            .enumerate()
            .filter_map(|(changed_pos, target_pos)| {
                self.bloom_filter_columns
                    .contains(target_pos)
                    .then_some(changed_pos)
            })
            .collect();
        let options = self.row_group_options();
        let (changed_iter, changed_bloom_hashes) = create_row_group(
            partition,
            0,
            row_count,
            changed_schema.fields(),
            &to_encodings(partition),
            options,
            &to_compressions(partition),
            &changed_bloom_columns,
            false,
        )?;
        let changed_offset = output_start + bytes_written;
        let bloom_filter_fpp = self.parquet_file.options().bloom_filter_fpp;
        let (changed_rg, changed_page_specs, changed_size, changed_blooms) = write_row_group(
            self.parquet_file.writer_mut(),
            changed_offset,
            changed_schema.columns(),
            changed_iter,
            &None,
            rg_idx,
            bloom_filter_fpp,
            &changed_bloom_hashes,
        )?;
        bytes_written += changed_size;
        let mut page_specs = (0..target_col_count)
            .map(|_| Vec::new())
            .collect::<Vec<_>>();
        for (changed_pos, (mut changed_col, changed_specs)) in changed_rg
            .columns
            .into_iter()
            .zip(changed_page_specs)
            .enumerate()
        {
            let target_pos = changed_target_positions[changed_pos];
            changed_col.meta_data.as_mut().unwrap().path_in_schema =
                target_descriptors[target_pos].path_in_schema.clone();
            columns[target_pos] = Some(changed_col);
            bloom_bitsets[target_pos] = changed_blooms[changed_pos].clone();
            page_specs[target_pos] = changed_specs;
        }

        let columns = columns
            .into_iter()
            .enumerate()
            .map(|(position, col)| {
                col.ok_or_else(|| {
                    fmt_err!(
                        InvalidLayout,
                        "rewrite_row_group_columns: target column slot {} is empty",
                        position
                    )
                })
            })
            .collect::<ParquetResult<Vec<_>>>()?;
        let sorting_columns = self.parquet_file.sorting_columns().map(<[_]>::to_vec);
        let thrift_rg = build_raw_row_group(columns, row_count, sorting_columns)?;
        self.parquet_file.register_streamed_hybrid_row_group(
            bytes_written,
            thrift_rg,
            bloom_bitsets,
            page_specs,
            copied_page_index,
        );
        self.hybrid_copied_column_chunks += (target_col_count - partition.columns.len()) as u64;
        self.hybrid_encoded_column_chunks += partition.columns.len() as u64;
        Ok(())
    }

    pub fn end(&mut self, key_value_metadata: Option<Vec<KeyValue>>) -> ParquetResult<u64> {
        // Build updated QDB metadata with unused_bytes and pass it as KV metadata.
        // When a target schema was set (ADD/DROP COLUMN), use the pre-built
        // target_qdb_meta which already has the correct column list with
        // column_top=0. Otherwise fall back to the old file's QDB metadata.
        let mut qdb_meta = if let Some(mut target) = self.target_qdb_meta.take() {
            // Apply tracked ASCII flags from written row groups, keyed by field_id.
            let target_fields = self.parquet_file.schema().fields();
            for (i, col) in target.schema.iter_mut().enumerate() {
                if let Some(field_id) = target_fields.get(i).and_then(|f| f.get_field_info().id) {
                    if let Some(&written) = self.written_ascii.get(&field_id) {
                        col.ascii = Some(written);
                    }
                }
            }
            target
        } else {
            let mut meta = self
                .file_metadata
                .key_value_metadata
                .as_ref()
                .and_then(|kvs| {
                    kvs.iter()
                        .find(|kv| kv.key == QDB_META_KEY)
                        .and_then(|kv| kv.value.as_ref())
                        .and_then(|v| QdbMeta::deserialize(v).ok())
                })
                .unwrap_or_else(|| QdbMeta::new(0));

            // After an O3 merge, row group sizes change but the file-level
            // column_top values remain stale from the original file. The
            // decoder uses column_top together with the *new* accumulated row
            // group sizes to decide whether a row group is entirely before the
            // column top and can be skipped (returning null ptr). Stale
            // column_top values may cause the decoder to incorrectly skip row
            // groups that now contain actual data (from merged O3 rows).
            //
            // All merged/inserted row groups already embed null sentinels in
            // their data with column_top=0, and copied row groups preserve
            // their original null definitions in the page data. Zeroing the
            // file-level column_top is therefore safe: the decoder will read
            // the (null) pages instead of skipping them, which is correct
            // albeit slightly less optimal.
            // Update the ASCII flag from the actual data written in
            // inserted/replaced row groups. For a full rewrite, the
            // written_ascii value is the final answer. For an in-place
            // update, AND the old flag (covering copied row groups) with
            // the written flag (covering new row groups). written_ascii is
            // keyed by parquet field_id, so look up by id (not position).
            let existing_fields = self.parquet_file.schema().fields();
            for (i, col) in meta.schema.iter_mut().enumerate() {
                col.column_top = 0;
                let field_id = existing_fields.get(i).and_then(|f| f.get_field_info().id);
                if let Some(id) = field_id {
                    if let Some(&written) = self.written_ascii.get(&id) {
                        if self.is_rewrite {
                            col.ascii = Some(written);
                        } else {
                            col.ascii = col.ascii.map(|old| old && written);
                        }
                    }
                }
            }
            meta
        };

        qdb_meta.seq_txn = self.seq_txn.get();

        // Emit the VARCHAR column-level ascii flag from the tracker built
        // during writes. Each tracker entry started life as `true` for an
        // old column whose old.ascii was Some(true) or for a fresh ADD
        // COLUMN VARCHAR (existing rows backfill with nulls, which are
        // ASCII-compatible), and as `false` otherwise. track_new_data_ascii
        // flips it to `false` on the first non-ASCII aux entry. So the
        // tracker encodes "old data was all-ASCII (or absent) AND every new
        // write stayed all-ASCII". Columns missing from the tracker emit
        // Some(false), matching the conservative default.
        let schema_fields = self.parquet_file.schema().fields();
        for (i, col) in qdb_meta.schema.iter_mut().enumerate() {
            if col.column_type.tag() != ColumnTypeTag::Varchar {
                continue;
            }
            let field_id = schema_fields
                .get(i)
                .and_then(|f| f.get_field_info().id)
                .unwrap_or(-1);
            col.ascii = Some(
                self.varchar_all_ascii
                    .get(&field_id)
                    .copied()
                    .unwrap_or(false),
            );
        }

        if self.is_rewrite {
            qdb_meta.unused_bytes = 0;
        } else {
            // The old footer is now dead space.
            self.accumulated_unused_bytes += self.old_footer_size;
            qdb_meta.unused_bytes = self.accumulated_unused_bytes;
        }

        let qdb_meta_json = qdb_meta.serialize()?;
        let qdb_kv = KeyValue {
            key: QDB_META_KEY.to_string(),
            value: Some(qdb_meta_json),
        };

        self.result_unused_bytes = qdb_meta.unused_bytes;

        let mut kv = key_value_metadata.unwrap_or_default();
        kv.push(qdb_kv);

        let file_size = self.parquet_file.end(Some(kv)).map_err(|s| {
            ParquetError::with_descr(
                ParquetErrorReason::Parquet2(s),
                "could not update parquet file",
            )
        })?;
        self.result_file_size = file_size;

        // Generate _pm metadata from the in-memory thrift row groups.
        if let Some(ref mut parquet_meta_file) = self.parquet_meta_fd {
            let footer_offset = self.parquet_file.parquet_footer_offset();
            let footer_length = file_size
                .checked_sub(footer_offset)
                .and_then(|v| v.checked_sub(8))
                .ok_or_else(|| {
                    fmt_err!(
                        InvalidLayout,
                        "parquet footer offset {} exceeds file size {}",
                        footer_offset,
                        file_size
                    )
                })? as u32;
            let schema_columns = self.parquet_file.schema().columns().to_vec();

            // Use the parquet file's sorting columns (set at construction with
            // the target timestamp index), not the old file metadata which may
            // have stale column indices after set_target_schema().
            let sorting_cols: Vec<parquet2::metadata::SortingColumn> = self
                .parquet_file
                .sorting_columns()
                .unwrap_or_default()
                .to_vec();
            let col_infos =
                build_column_infos_from_qdb_meta(&qdb_meta, &schema_columns, &sorting_cols);
            let sorting_indices: Vec<u32> =
                sorting_cols.iter().map(|sc| sc.column_idx as u32).collect();
            let designated_ts = qdb_meta
                .schema
                .iter()
                .position(|cm| cm.column_type.is_designated())
                .map(|i| i as i32)
                .unwrap_or(-1);

            if self.is_rewrite || self.existing_parquet_file_size <= 0 {
                let thrift_row_groups = self.parquet_file.row_groups();
                let bloom_bitsets = self.parquet_file.bloom_bitsets();

                // Full create: rewrite or first-time generation.
                let (parquet_meta_bytes, _) = crate::parquet_metadata::generate_parquet_metadata(
                    &col_infos,
                    thrift_row_groups,
                    designated_ts,
                    &sorting_indices,
                    footer_offset,
                    footer_length,
                    bloom_bitsets,
                    self.result_unused_bytes,
                    qdb_meta.squash_tracker,
                    self.seq_txn,
                )?;
                self.result_parquet_meta_size = parquet_meta_bytes.len() as i64;
                parquet_meta_file
                    .write_all(&parquet_meta_bytes)
                    .map_err(ParquetError::from)
                    .context("could not write _pm file")?;
            } else {
                let thrift_row_groups = self.parquet_file.row_groups();
                let bloom_bitsets = self.parquet_file.bloom_bitsets();

                // Incremental update: read the committed _pm and append the new
                // snapshot. Two distinct offsets drive this, both threaded in
                // from Java:
                //  - parse anchor: the committed head resolved from `_txn`
                //    (`parquet_meta_file_size`). Drives which footer is parsed,
                //    the new footer's `prev`, and the reused row-group offsets.
                //  - append base: the `_pm` offset-0 header (the Java reader's
                //    getFileSize()). New bytes land there, strictly past any
                //    orphaned dead footer a rolled-back update left in
                //    [parse anchor, append base), so committed and reader-mapped
                //    bytes are never overwritten. The two coincide unless a prior
                //    update patched the header but crashed before its `_txn`
                //    commit (the crash window). The table write lock is held, so
                //    the header is stable between the Java read and this write.
                let parse_anchor = self.parquet_meta_file_size;
                let append_base = self.append_base;
                let mut existing_pm = vec![0u8; append_base as usize];
                parquet_meta_file
                    .seek(SeekFrom::Start(0))
                    .map_err(ParquetError::from)?;
                parquet_meta_file
                    .read_exact(&mut existing_pm)
                    .map_err(ParquetError::from)
                    .context("could not read existing _pm file")?;

                let result = crate::parquet_metadata::update_parquet_metadata(
                    &existing_pm,
                    parse_anchor,
                    append_base,
                    thrift_row_groups,
                    footer_offset,
                    footer_length,
                    bloom_bitsets,
                    self.result_unused_bytes,
                    self.seq_txn,
                )?;

                // Write the new snapshot at the append base. The header still
                // points at the committed footer here: commit_parquet_meta
                // patches it after the index build. So any failure before that
                // header patch lands leaves the committed header and footer
                // intact, with the new bytes an invisible dead tail past the
                // header. In sync modes commit_parquet_meta durably flushes this
                // tail before patching the header, then flushes the header before
                // the `_txn` commit. If that second flush throws, the tail is
                // already durable and the committed `_txn` size is unchanged, so
                // readers can walk the MVCC chain back to the committed footer.
                parquet_meta_file
                    .seek(SeekFrom::Start(append_base))
                    .map_err(ParquetError::from)?;
                parquet_meta_file
                    .write_all(&result.bytes)
                    .map_err(ParquetError::from)
                    .context("could not write _pm file")?;

                self.result_parquet_meta_size = result.new_file_size as i64;
            }
        }

        Ok(file_size)
    }

    pub fn result_unused_bytes(&self) -> u64 {
        self.result_unused_bytes
    }

    /// Publishes the new `_pm` snapshot. For an in-place incremental update in a
    /// sync mode, first flushes the appended snapshot while the old header is
    /// still authoritative, then patches the new `parquet_meta_file_size` into
    /// the header (the MVCC commit signal), and flushes again before the matching
    /// `_txn` commit. The first barrier prevents a power loss from exposing a
    /// header whose newest footer is torn; the second prevents `_txn` from
    /// outliving the new header. Full-create/rewrite paths write a non-authoritative
    /// file and need only the final barrier. NOSYNC deliberately skips both. A
    /// no-op when no `_pm` fd is attached.
    pub fn commit_parquet_meta(&mut self, sync: bool) -> ParquetResult<()> {
        self.commit_parquet_meta_with_sync_data(sync, File::sync_data)
    }

    fn commit_parquet_meta_with_sync_data<S>(
        &mut self,
        sync: bool,
        sync_data: S,
    ) -> ParquetResult<()>
    where
        S: FnMut(&File) -> std::io::Result<()>,
    {
        let new_file_size = self.result_parquet_meta_size;
        if let Some(ref mut parquet_meta_file) = self.parquet_meta_fd {
            debug_assert!(
                new_file_size > 0,
                "commit_parquet_meta called before end() wrote the _pm"
            );
            publish_parquet_meta_with_sync(
                parquet_meta_file,
                new_file_size as u64,
                sync,
                !self.is_rewrite && self.existing_parquet_file_size > 0,
                sync_data,
            )?;
        }
        Ok(())
    }

    /// Updates the file-level schema when any column's not_null_hint flag disagrees
    /// with the schema's repetition. This happens when an O3 merge introduces
    /// null values into a symbol column that was previously all-non-null
    /// (Required in the schema). Only safe in rewrite mode where all row groups
    /// are re-encoded; in update mode untouched row groups would have data
    /// encoded with the old schema.
    ///
    /// Handles two legacy edge cases during rewrite:
    /// 1. Symbol columns marked Required in old files (written before the
    ///    convention was established that symbols are always Optional). If the
    ///    current data contains nulls (`!col.not_null_hint`), the schema must be
    ///    downgraded to Optional so the rewritten pages include definition levels.
    /// 2. Boolean/Byte/Short/Char columns marked Required in files written
    ///    before commit 247cb447cd ("fix: known-bad null-sentinel behaviour").
    ///    Modern files always write these as Optional so column-top rows can be
    ///    materialised as NULL via def-level=0. The repetition-aware dispatch
    ///    in encode.rs preserves the Required path for these tags in update
    ///    mode, but during a rewrite the file is fully re-encoded, so the
    ///    schema is migrated to Optional unconditionally.
    ///
    /// New files always write Symbol and Boolean/Byte/Short/Char as Optional
    /// (see `column_type_to_parquet_type` in schema.rs). The `Column::not_null_hint`
    /// flag is only a write-time hint for the encoder to emit a fast all-ones
    /// RLE run for definition levels.
    fn ensure_schema_matches_columns(&mut self, partition: &Partition) -> ParquetResult<()> {
        if self.symbol_schema_checked || !self.is_rewrite {
            return Ok(());
        }
        self.symbol_schema_checked = true;
        let fields = self.parquet_file.schema().fields();
        if partition.columns.len() != fields.len() {
            return Err(fmt_err!(
                InvalidLayout,
                "ensure_schema_matches_columns: column count ({}) != schema field count ({})",
                partition.columns.len(),
                fields.len()
            ));
        }
        let needs_migration = |col: &Column, field: &ParquetType| -> bool {
            if field.get_field_info().repetition != Repetition::Required {
                return false;
            }
            match col.data_type.tag() {
                ColumnTypeTag::Symbol => !col.not_null_hint,
                ColumnTypeTag::Boolean
                | ColumnTypeTag::Byte
                | ColumnTypeTag::Short
                | ColumnTypeTag::Char => true,
                _ => false,
            }
        };
        let needs_update = partition
            .columns
            .iter()
            .zip(fields.iter())
            .any(|(col, field)| needs_migration(col, field));
        if !needs_update {
            return Ok(());
        }
        let mut new_fields: Vec<ParquetType> = fields.to_vec();
        for (col, field) in partition.columns.iter().zip(new_fields.iter_mut()) {
            if needs_migration(col, field) {
                if let ParquetType::PrimitiveType(ref mut pt) = field {
                    pt.field_info.repetition = Repetition::Optional;
                }
            }
        }
        let schema =
            SchemaDescriptor::new(self.parquet_file.schema().name().to_string(), new_fields);
        self.parquet_file.set_schema(schema);
        Ok(())
    }

    pub fn result_parquet_meta_size(&self) -> i64 {
        self.result_parquet_meta_size
    }

    fn row_group_options(&self) -> WriteOptions {
        WriteOptions {
            write_statistics: self.parquet_file.options().write_statistics,
            compression: self.compression_options,
            version: self.parquet_file.options().version,
            row_group_size: self.row_group_size,
            data_page_size: self.data_page_size,
            raw_array_encoding: self.raw_array_encoding,
            bloom_filter_fpp: self.parquet_file.options().bloom_filter_fpp,
            min_compression_ratio: self.min_compression_ratio,
        }
    }
}

fn copy_file_range<W: std::io::Write>(
    reader: &mut File,
    writer: &mut W,
    offset: u64,
    len: u64,
    buffer: &mut [u8],
) -> ParquetResult<()> {
    reader.seek(SeekFrom::Start(offset))?;
    let mut remaining = len;
    while remaining > 0 {
        let chunk = usize::try_from(remaining.min(buffer.len() as u64)).map_err(|_| {
            fmt_err!(
                InvalidLayout,
                "copy_file_range: chunk length does not fit usize"
            )
        })?;
        reader.read_exact(&mut buffer[..chunk])?;
        writer.write_all(&buffer[..chunk])?;
        remaining -= chunk as u64;
    }
    Ok(())
}

/// Extracts bloom filter bitsets from a raw byte buffer that contains a copied
/// row group's data. The buffer starts at `buf_file_offset` in the original file.
/// Returns one `Option<Vec<u8>>` per column.
fn extract_bloom_bitsets_from_buffer(
    rg: &parquet2::metadata::RowGroupMetaData,
    buffer: &[u8],
    buf_file_offset: u64,
) -> Vec<Option<Vec<u8>>> {
    rg.columns()
        .iter()
        .map(|col_meta| {
            let meta = col_meta.metadata();
            let bf_offset = meta.bloom_filter_offset.filter(|&o| o > 0)?;
            let rel_offset = (bf_offset as u64).checked_sub(buf_file_offset)? as usize;
            let slice = buffer.get(rel_offset..)?;
            parquet2::bloom_filter::read_from_slice_at_offset(0, slice)
                .ok()
                .filter(|bs| !bs.is_empty())
                .map(|bs| bs.to_vec())
        })
        .collect()
}

/// Shifts all offset fields in a `ColumnChunk` by `offset_delta` and clears
/// column/offset index references (they are not copied with raw row group data).
fn adjust_column_chunk_offsets(col: &mut ColumnChunk, offset_delta: i64) {
    if let Some(ref mut meta) = col.meta_data {
        meta.data_page_offset += offset_delta;
        if let Some(ref mut v) = meta.dictionary_page_offset {
            *v += offset_delta;
        }
        if let Some(ref mut v) = meta.index_page_offset {
            *v += offset_delta;
        }
        if let Some(ref mut v) = meta.bloom_filter_offset {
            *v += offset_delta;
        }
    }
    col.column_index_offset = None;
    col.column_index_length = None;
    col.offset_index_offset = None;
    col.offset_index_length = None;
}

/// Reads a copied row group's source page index, rebased for the output file:
/// the ColumnIndex is copied verbatim, the OffsetIndex page offsets are shifted
/// by `offset_delta`. `None` if any column lacks an OffsetIndex, which keeps the
/// whole output unindexed (a mixed file is rejected by strict readers). Call
/// before `adjust_column_chunk_offsets` clears `columns`' source index pointers.
fn build_copied_page_index(
    columns: &[ColumnChunk],
    reader: &mut File,
    offset_delta: i64,
) -> ParquetResult<Option<Vec<CopiedColumnIndex>>> {
    let mut result = Vec::with_capacity(columns.len());
    for col in columns {
        let Some(copied) = build_copied_column_index(col, reader, offset_delta)? else {
            return Ok(None);
        };
        result.push(copied);
    }
    Ok(Some(result))
}

fn build_copied_column_index(
    col: &ColumnChunk,
    reader: &mut File,
    offset_delta: i64,
) -> ParquetResult<Option<CopiedColumnIndex>> {
    let (Some(oi_offset), Some(oi_length)) = (col.offset_index_offset, col.offset_index_length)
    else {
        return Ok(None);
    };
    // A corrupt footer can encode a negative offset/length; reject it before casting
    // to usize/u64, which would request a usize::MAX allocation and abort the JVM.
    if oi_offset < 0 || oi_length < 0 {
        return Err(fmt_err!(
            InvalidLayout,
            "negative offset index location: offset={oi_offset}, length={oi_length}"
        ));
    }

    // Read and rebase the OffsetIndex. Reserve fallibly: a corrupt footer can
    // encode a huge positive length, and an infallible alloc would abort the JVM.
    let mut oi_bytes = Vec::new();
    oi_bytes
        .try_reserve_exact(oi_length as usize)
        .map_err(|_| {
            fmt_err!(
                InvalidLayout,
                "offset index length {oi_length} exceeds available memory"
            )
        })?;
    oi_bytes.resize(oi_length as usize, 0);
    reader.seek(SeekFrom::Start(oi_offset as u64))?;
    reader.read_exact(&mut oi_bytes)?;
    let mut offset_index = {
        // Generous byte cap, matching parquet2's reader.
        let max_bytes = oi_bytes.len() * 2 + 1024;
        let mut prot = TCompactInputProtocol::new(oi_bytes.as_slice(), max_bytes);
        OffsetIndex::read_from_in_protocol(&mut prot)
            .map_err(|e| fmt_err!(InvalidLayout, "could not read source offset index: {e}"))?
    };
    for location in &mut offset_index.page_locations {
        location.offset = location
            .offset
            .checked_add(offset_delta)
            .ok_or_else(|| fmt_err!(InvalidLayout, "offset index page location offset overflow"))?;
    }
    let mut rebased = Vec::with_capacity(oi_bytes.len());
    {
        let mut prot = TCompactOutputProtocol::new(&mut rebased);
        offset_index
            .write_to_out_protocol(&mut prot)
            .map_err(|e| fmt_err!(InvalidLayout, "could not write offset index: {e}"))?;
    }

    // ColumnIndex has no file offsets, so copy its bytes verbatim.
    let column_index = match (col.column_index_offset, col.column_index_length) {
        (Some(ci_offset), Some(ci_length)) => {
            if ci_offset < 0 || ci_length < 0 {
                return Err(fmt_err!(
                    InvalidLayout,
                    "negative column index location: offset={ci_offset}, length={ci_length}"
                ));
            }
            let mut ci_bytes = Vec::new();
            ci_bytes
                .try_reserve_exact(ci_length as usize)
                .map_err(|_| {
                    fmt_err!(
                        InvalidLayout,
                        "column index length {ci_length} exceeds available memory"
                    )
                })?;
            ci_bytes.resize(ci_length as usize, 0);
            reader.seek(SeekFrom::Start(ci_offset as u64))?;
            reader.read_exact(&mut ci_bytes)?;
            Some(ci_bytes)
        }
        _ => None,
    };

    Ok(Some(CopiedColumnIndex {
        column_index,
        offset_index: rebased,
    }))
}

/// File-level `sorting_columns` for a QuestDB parquet file: the designated
/// timestamp at its dense `qdb_meta` position (not the raw timestamp slot, which
/// goes stale after a DROP COLUMN), or `None` when there is none. The sort
/// direction follows the designated timestamp's order, matching
/// `designated_sorting_col` on the read path.
fn designated_ts_sorting_columns(qdb_meta: &QdbMeta) -> Option<Vec<SortingColumn>> {
    qdb_meta
        .schema
        .iter()
        .position(|col| col.column_type.is_designated())
        .map(|pos| {
            let descending = !qdb_meta.schema[pos]
                .column_type
                .is_designated_timestamp_ascending();
            vec![SortingColumn::new(pos as i32, descending, false)]
        })
}

/// Builds a thrift `RowGroup` from a list of `ColumnChunk`s, computing
/// `file_offset`, `total_byte_size`, and `total_compressed_size` from the
/// column metadata. `sorting_columns` carries the file-level target sort order
/// so copied row groups declare the same value as freshly written ones.
fn build_raw_row_group(
    columns: Vec<ColumnChunk>,
    num_rows: usize,
    sorting_columns: Option<Vec<SortingColumn>>,
) -> ParquetResult<RowGroup> {
    let mut total_byte_size = 0i64;
    let mut total_compressed_size = 0i64;
    let mut file_offset: Option<i64> = None;
    for (column_index, column) in columns.iter().enumerate() {
        let metadata = column.meta_data.as_ref().ok_or_else(|| {
            fmt_err!(
                InvalidLayout,
                "build_raw_row_group: column {} has no metadata",
                column_index
            )
        })?;
        total_byte_size = total_byte_size
            .checked_add(metadata.total_uncompressed_size)
            .ok_or_else(|| fmt_err!(InvalidLayout, "row-group uncompressed size overflow"))?;
        total_compressed_size = total_compressed_size
            .checked_add(metadata.total_compressed_size)
            .ok_or_else(|| fmt_err!(InvalidLayout, "row-group compressed size overflow"))?;

        let column_offset = ColumnOffsetsMetadata::from_column_chunk(column)
            .calc_row_group_file_offset()
            .ok_or_else(|| {
                fmt_err!(
                    InvalidLayout,
                    "build_raw_row_group: column {} has no page offset",
                    column_index
                )
            })?;
        if column_offset < 0 {
            return Err(fmt_err!(
                InvalidLayout,
                "build_raw_row_group: column {} has negative page offset {}",
                column_index,
                column_offset
            ));
        }
        file_offset = Some(file_offset.map_or(column_offset, |current| current.min(column_offset)));
    }
    // Hybrid rewrites stream unchanged chunks before freshly encoded chunks,
    // so physical order need not match schema order. The row-group start is
    // the minimum dictionary/data page offset across every column, not the
    // first schema column's offset.
    Ok(RowGroup {
        columns,
        total_byte_size,
        num_rows: i64::try_from(num_rows)
            .map_err(|_| fmt_err!(InvalidLayout, "row count {} exceeds i64", num_rows))?,
        sorting_columns,
        file_offset,
        total_compressed_size: Some(total_compressed_size),
        ordinal: None,
    })
}

/// Generates the raw bytes and thrift `ColumnChunk` for an all-NULL (or
/// all-zero for Required types) column chunk. The output is a single
/// uncompressed DataPageV1 containing RLE-encoded definition levels
/// (all zeros for Optional) or zero-filled values (for Required).
///
/// For nested GroupType columns (arrays encoded as nested LIST), the page
/// includes both repetition and definition levels with correct bit widths
/// matching the nested schema, and the leaf physical type and full path.
///
/// Returns `(raw_page_bytes, thrift_column_chunk)` where `raw_page_bytes`
/// must be written at `file_offset` in the output file, and the thrift
/// metadata references that offset.
fn generate_null_column_chunk_bytes(
    parquet_field: &ParquetType,
    column_type: ColumnType,
    row_count: usize,
    file_offset: u64,
) -> ParquetResult<(Vec<u8>, ColumnChunk)> {
    let field_info = parquet_field.get_field_info();
    let is_required = field_info.repetition == Repetition::Required;
    let is_symbol = column_type.tag() == ColumnTypeTag::Symbol;

    // Walk the type tree to collect the full root-to-leaf path, the
    // maximum repetition/definition levels, and the leaf physical type.
    let (path, max_rep_level, _max_def_level, (thrift_type, _type_length)) =
        collect_leaf_info(parquet_field);

    // Build page data.
    let mut page_data = if is_required {
        // Required column: no definition levels, all-zero values.
        generate_required_zero_page(parquet_field, column_type, row_count)?
    } else {
        // Optional/nested column: RLE rep+def levels = all zeros, no values.
        generate_optional_null_page(row_count, max_rep_level)
    };

    // Symbol columns use RleDictionary encoding. The decoder does not
    // support Plain encoding for symbols, so we emit an empty dictionary
    // page and mark the data page as RLE_DICTIONARY. The data page needs
    // a trailing bit-width byte (0) for the empty dictionary indices.
    if is_symbol {
        page_data.push(0x00); // bit_width = 0 (empty dictionary)
    }

    let mut chunk_bytes = Vec::new();

    // For Symbol columns, prepend an empty dictionary page.
    let dict_page_offset = if is_symbol {
        let dict_header = PageHeader {
            type_: PageType::DICTIONARY_PAGE,
            uncompressed_page_size: 0,
            compressed_page_size: 0,
            crc: None,
            data_page_header: None,
            index_page_header: None,
            dictionary_page_header: Some(DictionaryPageHeader {
                num_values: 0,
                encoding: ThriftEncoding::PLAIN,
                is_sorted: None,
            }),
            data_page_header_v2: None,
        };
        let mut protocol = TCompactOutputProtocol::new(&mut chunk_bytes);
        dict_header
            .write_to_out_protocol(&mut protocol)
            .map_err(|e| {
                ParquetError::with_descr(
                    ParquetErrorReason::Parquet2(parquet2::error::Error::oos(e.to_string())),
                    "Failed to serialize null column dictionary page header",
                )
            })?;
        // Dictionary page has no data (0 entries), only the header.
        Some(file_offset as i64)
    } else {
        None
    };

    // Serialize the data page header.
    let data_encoding = if is_symbol {
        ThriftEncoding::RLE_DICTIONARY
    } else {
        ThriftEncoding::PLAIN
    };
    let data_page_header = PageHeader {
        type_: PageType::DATA_PAGE,
        uncompressed_page_size: page_data.len() as i32,
        compressed_page_size: page_data.len() as i32,
        crc: None,
        data_page_header: Some(DataPageHeader {
            num_values: row_count as i32,
            encoding: data_encoding,
            definition_level_encoding: ThriftEncoding::RLE,
            repetition_level_encoding: ThriftEncoding::RLE,
            statistics: None,
        }),
        index_page_header: None,
        dictionary_page_header: None,
        data_page_header_v2: None,
    };
    let data_page_offset = file_offset as i64 + chunk_bytes.len() as i64;
    {
        let mut protocol = TCompactOutputProtocol::new(&mut chunk_bytes);
        data_page_header
            .write_to_out_protocol(&mut protocol)
            .map_err(|e| {
                ParquetError::with_descr(
                    ParquetErrorReason::Parquet2(parquet2::error::Error::oos(e.to_string())),
                    "Failed to serialize null column page header",
                )
            })?;
    }
    chunk_bytes.extend_from_slice(&page_data);

    let total_size = chunk_bytes.len() as i64;

    let encodings = if is_symbol {
        vec![
            ThriftEncoding::PLAIN,
            ThriftEncoding::RLE_DICTIONARY,
            ThriftEncoding::RLE,
        ]
    } else {
        vec![ThriftEncoding::PLAIN, ThriftEncoding::RLE]
    };
    let encoding_stats = if is_symbol {
        vec![
            PageEncodingStats {
                page_type: PageType::DICTIONARY_PAGE,
                encoding: ThriftEncoding::PLAIN,
                count: 1,
            },
            PageEncodingStats {
                page_type: PageType::DATA_PAGE,
                encoding: ThriftEncoding::RLE_DICTIONARY,
                count: 1,
            },
        ]
    } else {
        vec![PageEncodingStats {
            page_type: PageType::DATA_PAGE,
            encoding: ThriftEncoding::PLAIN,
            count: 1,
        }]
    };

    let metadata = ColumnMetaData {
        type_: thrift_type,
        encodings,
        path_in_schema: path,
        codec: CompressionCodec::UNCOMPRESSED,
        num_values: row_count as i64,
        total_uncompressed_size: total_size,
        total_compressed_size: total_size,
        key_value_metadata: None,
        data_page_offset,
        index_page_offset: None,
        dictionary_page_offset: dict_page_offset,
        statistics: None,
        encoding_stats: Some(encoding_stats),
        bloom_filter_offset: None,
        bloom_filter_length: None,
    };

    let column_chunk = ColumnChunk {
        file_path: None,
        file_offset: file_offset as i64 + total_size,
        meta_data: Some(metadata),
        offset_index_offset: None,
        offset_index_length: None,
        column_index_offset: None,
        column_index_length: None,
        crypto_metadata: None,
        encrypted_column_metadata: None,
    };

    Ok((chunk_bytes, column_chunk))
}

/// Collects the full root-to-leaf path for `path_in_schema` and computes
/// the leaf's max repetition/definition levels and physical type.
///
/// For primitive types the path is `["col_name"]` with levels derived from
/// the single node's repetition.  For nested LIST groups (arrays) this
/// walks down to the leaf, e.g. `["col_name", "list", "element"]`,
/// accumulating levels at each nesting step.
fn collect_leaf_info(parquet_type: &ParquetType) -> (Vec<String>, i16, i16, (Type, Option<i32>)) {
    let mut path = Vec::new();
    let mut max_rep_level: i16 = 0;
    let mut max_def_level: i16 = 0;
    let mut current = parquet_type;
    loop {
        let info = current.get_field_info();
        match info.repetition {
            Repetition::Optional => {
                max_def_level += 1;
            }
            Repetition::Repeated => {
                max_def_level += 1;
                max_rep_level += 1;
            }
            Repetition::Required => {}
        }
        path.push(info.name.clone());
        match current {
            ParquetType::PrimitiveType(pt) => {
                let thrift_type = pt.physical_type.into();
                return (path, max_rep_level, max_def_level, thrift_type);
            }
            ParquetType::GroupType { fields, .. } => {
                if let Some(child) = fields.first() {
                    current = child;
                } else {
                    // Empty group — should not happen for valid schemas.
                    return (path, max_rep_level, max_def_level, (Type::BYTE_ARRAY, None));
                }
            }
        }
    }
}

/// Generates page data for an Optional (or nested) column where all rows
/// are NULL.  For flat Optional columns `max_rep_level` is 0 and only
/// definition levels are emitted.  For nested types (e.g. LIST arrays)
/// `max_rep_level > 0` and the page contains repetition levels first,
/// then definition levels, matching the DataPageV1 wire format expected
/// by `split_buffer_v1`.
fn generate_optional_null_page(row_count: usize, max_rep_level: i16) -> Vec<u8> {
    // RLE encoding of `row_count` zeros: header = (count << 1) as varint,
    // followed by ceil(bit_width / 8) zero bytes for the value.  Since the
    // value is 0 regardless of bit_width, one 0x00 byte suffices for any
    // bit_width in [1, 8].
    let rle_all_zeros = {
        let mut buf = Vec::with_capacity(8);
        let mut varint_buf = [0u8; 10];
        let varint_len = uleb128::encode((row_count << 1) as u64, &mut varint_buf);
        buf.extend_from_slice(&varint_buf[..varint_len]);
        buf.push(0x00); // value byte: all zeros
        buf
    };

    let rle_section_len = rle_all_zeros.len() as u32;
    // Estimate: optional rep + def sections.
    let mut page_data = Vec::with_capacity(2 * (4 + rle_all_zeros.len()));

    // Repetition levels (only for nested types with max_rep_level > 0).
    if max_rep_level > 0 {
        page_data.extend_from_slice(&rle_section_len.to_le_bytes());
        page_data.extend_from_slice(&rle_all_zeros);
    }

    // Definition levels.
    page_data.extend_from_slice(&rle_section_len.to_le_bytes());
    page_data.extend_from_slice(&rle_all_zeros);
    page_data
}

/// Generates page data for a Required column where all values are zero/default.
/// No definition levels for Required columns — just the value bytes.
fn generate_required_zero_page(
    _parquet_field: &ParquetType,
    column_type: ColumnType,
    row_count: usize,
) -> ParquetResult<Vec<u8>> {
    let value_size = match column_type.tag() {
        ColumnTypeTag::Boolean => {
            // Boolean: packed bits, ceil(row_count / 8) bytes.
            return Ok(vec![0u8; row_count.div_ceil(8)]);
        }
        ColumnTypeTag::Byte | ColumnTypeTag::Short | ColumnTypeTag::Char => {
            // Stored as Int32 in Parquet.
            4
        }
        _ => {
            return Err(fmt_err!(
                InvalidLayout,
                "cannot generate null chunk for Required column type {:?}",
                column_type
            ));
        }
    };
    Ok(vec![0u8; row_count * value_size])
}

fn build_column_infos_from_qdb_meta<'a>(
    qdb_meta: &'a QdbMeta,
    schema_columns: &'a [parquet2::metadata::ColumnDescriptor],
    sorting_columns: &[SortingColumn],
) -> Vec<crate::parquet_metadata::ParquetMetaColumnInfo<'a>> {
    schema_columns
        .iter()
        .enumerate()
        .map(|(i, col_desc)| {
            let field_info = col_desc.base_type.get_field_info();
            let cm = qdb_meta.schema.get(i);
            let col_type_code = cm.map(|c| c.column_type.code()).unwrap_or_else(|| {
                crate::parquet_read::meta::infer_column_type(col_desc)
                    .map(|ct| ct.code())
                    .unwrap_or(-1)
            });
            let mut flags = crate::parquet_metadata::types::ColumnFlags::new();
            let repetition =
                crate::parquet_metadata::types::FieldRepetition::from(field_info.repetition);
            flags = flags.with_repetition(repetition);
            if let Some(c) = cm {
                if c.format == Some(QdbMetaColFormat::LocalKeyIsGlobal) {
                    flags = flags.with_local_key_is_global();
                }
                if c.ascii == Some(true) {
                    flags = flags.with_ascii();
                }
            }
            if let Some(sc) = sorting_columns.iter().find(|sc| sc.column_idx == i as i32) {
                if sc.descending {
                    flags = flags.with_descending();
                }
            }

            let phys_type = col_desc.descriptor.primitive_type.physical_type;
            crate::parquet_metadata::ParquetMetaColumnInfo {
                name: &field_info.name,
                col_type_code,
                // Prefer QuestDB's authoritative id from QdbMeta over the parquet
                // field_id, matching the convert path's `_pm` generation.
                id: resolve_column_id(cm.and_then(|c| c.id), field_info.id),
                flags,
                fixed_byte_len: match phys_type {
                    parquet2::schema::types::PhysicalType::FixedLenByteArray(len) => len as i32,
                    _ => 0,
                },
                physical_type: crate::parquet_metadata::physical_type_to_u8(phys_type),
                max_rep_level: col_desc.descriptor.max_rep_level as u8,
                max_def_level: col_desc.descriptor.max_def_level as u8,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::SeqTxn;
    use crate::parquet::tests::ColumnTypeTagExt;
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet2::compression::CompressionOptions;
    use parquet2::write::{ParquetFile, Version};
    use qdb_parquet_meta::NoBloomFilterSource;
    use std::cell::Cell;
    use std::collections::HashSet;
    use std::env;
    use std::error::Error;
    use std::fs::File;
    use std::io::{Cursor, Read, Seek, SeekFrom, Write};
    use std::ptr::null;

    use super::adjust_column_chunk_offsets;
    use crate::parquet_write::file::DEFAULT_BLOOM_FILTER_FPP;
    use crate::parquet_write::file::{create_row_group, ParquetWriter, WriteOptions};
    use crate::parquet_write::schema::{
        to_compressions, to_encodings, to_parquet_schema, Column, Partition,
    };

    use arrow::datatypes::ToByteSlice;
    use num_traits::float::FloatCore;
    use parquet2::compression::Compression;
    use parquet2::read::read_metadata_with_size;
    use parquet2::write;
    use qdb_core::col_type::{ColumnType, ColumnTypeTag};
    use tempfile::NamedTempFile;

    fn read_pm_header(file: &NamedTempFile) -> Result<u64, Box<dyn Error>> {
        let mut bytes = [0u8; size_of::<u64>()];
        file.reopen()?.read_exact(&mut bytes)?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn publish_test_updater(
        allocator: &crate::allocator::TestAllocatorState,
        pm: &NamedTempFile,
        is_rewrite: bool,
        existing_parquet_file_size: i64,
    ) -> Result<super::ParquetUpdater, Box<dyn Error>> {
        let (source, _partition) = write_initial_zstd_file()?;
        let source_len = source.as_file().metadata()?.len();
        let output = NamedTempFile::new()?;
        let writer = if is_rewrite {
            output.reopen()?
        } else {
            source.reopen()?
        };
        let write_file_size = if is_rewrite { 0 } else { source_len };
        let mut updater = super::ParquetUpdater::new(
            allocator.allocator(),
            source.reopen()?,
            source_len,
            writer,
            write_file_size,
            None,
            true,
            false,
            CompressionOptions::Zstd(None),
            None,
            None,
            DEFAULT_BLOOM_FILTER_FPP,
            0.0,
            Some(pm.reopen()?),
            0,
            0,
            existing_parquet_file_size,
            SeqTxn::UNSET,
        )?;
        updater.result_parquet_meta_size = 42;
        Ok(updater)
    }

    #[test]
    fn commit_parquet_meta_tail_sync_error_does_not_publish_header() -> Result<(), Box<dyn Error>> {
        let allocator = crate::allocator::TestAllocatorState::new();
        let pm = NamedTempFile::new()?;
        pm.as_file().set_len(64)?;
        let mut updater = publish_test_updater(&allocator, &pm, false, 1)?;
        let err = updater
            .commit_parquet_meta_with_sync_data(true, |_| {
                Err(std::io::Error::other("injected sync failure"))
            })
            .unwrap_err();

        assert_eq!(read_pm_header(&pm)?, 0);
        assert!(
            err.to_string()
                .contains("could not fsync appended _pm snapshot before publication"),
            "unexpected error: {err}"
        );
        assert!(
            err.to_string().contains("injected sync failure"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[test]
    fn commit_parquet_meta_sync_false_skips_sync() -> Result<(), Box<dyn Error>> {
        let allocator = crate::allocator::TestAllocatorState::new();
        let pm = NamedTempFile::new()?;
        pm.as_file().set_len(64)?;
        let mut updater = publish_test_updater(&allocator, &pm, false, 1)?;
        let sync_calls = Cell::new(0);
        updater.commit_parquet_meta_with_sync_data(false, |_| {
            sync_calls.set(sync_calls.get() + 1);
            Err(std::io::Error::other("sync must be skipped"))
        })?;

        assert_eq!(sync_calls.get(), 0);
        assert_eq!(read_pm_header(&pm)?, 42);
        Ok(())
    }

    #[test]
    fn commit_parquet_meta_sync_true_orders_footer_before_header_publication(
    ) -> Result<(), Box<dyn Error>> {
        let allocator = crate::allocator::TestAllocatorState::new();
        let pm = NamedTempFile::new()?;
        pm.as_file().set_len(64)?;
        let mut updater = publish_test_updater(&allocator, &pm, false, 1)?;
        let sync_calls = Cell::new(0);
        updater.commit_parquet_meta_with_sync_data(true, |file| {
            let sync_call = sync_calls.get();
            let mut published = file.try_clone()?;
            published.seek(SeekFrom::Start(0))?;
            let mut bytes = [0u8; size_of::<u64>()];
            published.read_exact(&mut bytes)?;
            let expected_header = if sync_call == 0 { 0 } else { 42 };
            assert_eq!(u64::from_le_bytes(bytes), expected_header);
            sync_calls.set(sync_call + 1);
            Ok(())
        })?;

        assert_eq!(sync_calls.get(), 2);
        assert_eq!(read_pm_header(&pm)?, 42);
        Ok(())
    }

    #[test]
    fn commit_parquet_meta_header_sync_error_is_propagated_after_publication(
    ) -> Result<(), Box<dyn Error>> {
        let allocator = crate::allocator::TestAllocatorState::new();
        let pm = NamedTempFile::new()?;
        pm.as_file().set_len(64)?;
        let mut updater = publish_test_updater(&allocator, &pm, false, 1)?;
        let sync_calls = Cell::new(0);
        let err = updater
            .commit_parquet_meta_with_sync_data(true, |_| {
                let sync_call = sync_calls.get();
                sync_calls.set(sync_call + 1);
                if sync_call == 0 {
                    Ok(())
                } else {
                    Err(std::io::Error::other("injected sync failure"))
                }
            })
            .unwrap_err();

        assert_eq!(sync_calls.get(), 2);
        assert_eq!(read_pm_header(&pm)?, 42);
        assert!(
            err.to_string()
                .contains("could not fsync published _pm header"),
            "unexpected error: {err}"
        );
        assert!(
            err.to_string().contains("injected sync failure"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[test]
    fn commit_parquet_meta_non_authoritative_files_sync_only_after_publication(
    ) -> Result<(), Box<dyn Error>> {
        for (mode, is_rewrite, existing_parquet_file_size) in
            [("first-create", false, -1), ("rewrite", true, 1)]
        {
            let allocator = crate::allocator::TestAllocatorState::new();
            let pm = NamedTempFile::new()?;
            pm.as_file().set_len(64)?;
            let mut updater =
                publish_test_updater(&allocator, &pm, is_rewrite, existing_parquet_file_size)?;
            let sync_calls = Cell::new(0);
            updater.commit_parquet_meta_with_sync_data(true, |file| {
                sync_calls.set(sync_calls.get() + 1);
                let mut published = file.try_clone()?;
                published.seek(SeekFrom::Start(0))?;
                let mut bytes = [0u8; size_of::<u64>()];
                published.read_exact(&mut bytes)?;
                assert_eq!(u64::from_le_bytes(bytes), 42, "mode={mode}");
                Ok(())
            })?;

            assert_eq!(sync_calls.get(), 1, "mode={mode}");
            assert_eq!(read_pm_header(&pm)?, 42, "mode={mode}");
        }
        Ok(())
    }

    fn save_to_file(bytes: &Bytes) {
        if let Ok(path) = env::var("OUT_PARQUET_FILE") {
            let mut file = File::create(path).expect("file create failed");
            file.write_all(bytes.to_byte_slice())
                .expect("file write failed");
        };
    }

    fn make_column<T>(name: &'static str, col_type: ColumnType, values: &[T]) -> Column {
        make_column_with_id(0, name, col_type, values)
    }

    fn make_column_with_id<T>(
        id: i32,
        name: &'static str,
        col_type: ColumnType,
        values: &[T],
    ) -> Column {
        Column::from_raw_data(
            id,
            name,
            col_type.code(),
            0,
            values.len(),
            values.as_ptr() as *const u8,
            std::mem::size_of_val(values),
            null(),
            0,
            null(),
            0,
            false,
            false,
            0,
        )
        .unwrap()
    }

    fn exercise_hybrid_rewrite(
        unchanged_count: usize,
        changed_position: usize,
    ) -> Result<std::time::Duration, Box<dyn Error>> {
        use crate::allocator::TestAllocatorState;
        use arrow::array::Int64Array;

        let total_column_count = unchanged_count + 1;
        assert!(changed_position < total_column_count);
        let row_count = 32usize;
        let source_changed: Vec<i32> = (0..row_count as i32).collect();
        let target_changed: Vec<i64> = source_changed
            .iter()
            .map(|value| i64::from(*value) * 10)
            .collect();
        let unchanged_values: Vec<Vec<i64>> = (0..unchanged_count)
            .map(|column| {
                (0..row_count)
                    .map(|row| (column as i64 + 1) * 10_000 + row as i64)
                    .collect()
            })
            .collect();

        let mut source_columns = Vec::with_capacity(total_column_count);
        let mut target_columns = Vec::with_capacity(total_column_count);
        let mut expected_target_bounds = Vec::with_capacity(total_column_count);
        let mut unchanged_index = 0usize;
        for column_position in 0..total_column_count {
            let column_id = column_position as i32;
            if column_position == changed_position {
                source_columns.push(make_column_with_id(
                    column_id,
                    "changed",
                    ColumnTypeTag::Int.into_type(),
                    &source_changed,
                ));
                target_columns.push(make_column_with_id(
                    column_id,
                    "changed",
                    ColumnTypeTag::Long.into_type(),
                    &target_changed,
                ));
                expected_target_bounds.push((target_changed[0], target_changed[row_count - 1]));
            } else {
                let name: &'static str =
                    Box::leak(format!("unchanged_{column_position}").into_boxed_str());
                let values = &unchanged_values[unchanged_index];
                source_columns.push(make_column_with_id(
                    column_id,
                    name,
                    ColumnTypeTag::Long.into_type(),
                    values,
                ));
                target_columns.push(make_column_with_id(
                    column_id,
                    name,
                    ColumnTypeTag::Long.into_type(),
                    values,
                ));
                expected_target_bounds.push((values[0], values[row_count - 1]));
                unchanged_index += 1;
            }
        }

        let source_partition = Partition {
            table: "hybrid".to_string(),
            columns: source_columns,
        };
        let target_partition = Partition {
            table: "hybrid".to_string(),
            columns: target_columns,
        };
        let changed_partition = Partition {
            table: "hybrid".to_string(),
            columns: vec![target_partition.columns[changed_position]],
        };

        let source = NamedTempFile::new()?;
        ParquetWriter::new(source.reopen()?)
            .with_compression(CompressionOptions::Zstd(None))
            .finish(source_partition)?;
        let source_len = source.as_file().metadata()?.len();
        let source_bytes = std::fs::read(source.path())?;
        let source_meta = read_metadata_with_size(&mut source.reopen()?, source_len)?;
        assert!(
            source_meta.row_groups[0]
                .columns()
                .iter()
                .all(|column| column.offset_index_offset().is_some()
                    && column.column_index_offset().is_some()),
            "source fixture must carry both page indexes"
        );
        let expected_unchanged: Vec<(usize, Vec<u8>)> = source_meta.row_groups[0]
            .columns()
            .iter()
            .enumerate()
            .filter(|(position, _)| *position != changed_position)
            .map(|(position, column)| {
                let (offset, len) = column.byte_range();
                (
                    position,
                    source_bytes[offset as usize..(offset + len) as usize].to_vec(),
                )
            })
            .collect();

        let output = NamedTempFile::new()?;
        let alloc_state = TestAllocatorState::new();
        let mut updater = super::ParquetUpdater::new(
            alloc_state.allocator(),
            source.reopen()?,
            source_len,
            output.reopen()?,
            0,
            None,
            true,
            false,
            CompressionOptions::Zstd(None),
            None,
            None,
            DEFAULT_BLOOM_FILTER_FPP,
            0.0,
            None,
            0,
            0,
            -1,
            SeqTxn::UNSET,
        )?;
        updater.set_target_schema(&target_partition)?;
        let started = std::time::Instant::now();
        updater.rewrite_row_group_columns(0, &changed_partition)?;
        let output_len = updater.end(None)?;
        let rewrite_elapsed = started.elapsed();
        assert_eq!(
            updater.copy_buffer.capacity(),
            0,
            "hybrid rewrite must not retain a row-group-sized copy buffer at width {unchanged_count}, changed position {changed_position}"
        );
        assert_eq!(
            updater.hybrid_copied_column_chunks, unchanged_count as u64,
            "every unchanged chunk must take the raw-copy path"
        );
        assert_eq!(
            updater.hybrid_encoded_column_chunks, 1,
            "encoding work must stay constant as unchanged width grows"
        );
        assert_eq!(
            alloc_state.rss_mem_used(),
            0,
            "hybrid updater retained memory in its native allocator at width {unchanged_count}, changed position {changed_position}"
        );
        drop(updater);

        let output_bytes = std::fs::read(output.path())?;
        let output_meta = read_metadata_with_size(&mut output.reopen()?, output_len)?;
        assert_eq!(
            output_meta.row_groups[0].columns().len(),
            total_column_count
        );
        assert!(
            output_meta.row_groups[0]
                .columns()
                .iter()
                .all(|column| column.offset_index_offset().is_some()
                    && column.column_index_offset().is_some()),
            "hybrid output must retain copied indexes and generate the changed column index"
        );
        for (position, expected) in expected_unchanged {
            let column = &output_meta.row_groups[0].columns()[position];
            let (offset, len) = column.byte_range();
            assert_eq!(
                &output_bytes[offset as usize..(offset + len) as usize],
                expected,
                "unchanged compressed chunk changed at width {unchanged_count}, changed position {changed_position}, column {position}"
            );
            assert_eq!(column.compression(), Compression::Zstd);
        }

        use parquet::arrow::arrow_reader::ArrowReaderOptions;
        use parquet::file::page_index::index::Index;

        let bytes: Bytes = output_bytes.into();
        let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
            bytes,
            ArrowReaderOptions::new().with_page_index(true),
        )?;
        {
            let metadata = builder.metadata();
            let column_index = metadata.column_index().expect("parsed mixed column index");
            let offset_index = metadata.offset_index().expect("parsed mixed offset index");
            for column_position in 0..total_column_count {
                let page_locations = offset_index[0][column_position].page_locations();
                assert_eq!(
                    page_locations[0].offset,
                    metadata.row_group(0).column(column_position).data_page_offset(),
                    "mixed offset index must point at the copied/encoded data page for column {column_position}"
                );
                match &column_index[0][column_position] {
                    Index::INT64(native) => {
                        let (expected_min, expected_max) =
                            expected_target_bounds[column_position];
                        assert_eq!(
                            native.indexes[0].min,
                            Some(expected_min),
                            "mixed column index min at column {column_position}"
                        );
                        assert_eq!(
                            native.indexes[0].max,
                            Some(expected_max),
                            "mixed column index max at column {column_position}"
                        );
                    }
                    other => panic!(
                        "expected INT64 mixed column index at column {column_position}, got {other:?}"
                    ),
                }
            }
        }
        let mut batches = builder.build()?;
        let batch = batches.next().transpose()?.expect("one output batch");
        let changed = batch
            .column(changed_position)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("changed column must be INT64");
        assert_eq!(
            changed.values().as_ref(),
            target_changed.as_slice(),
            "changed column values at width {unchanged_count}, changed position {changed_position}"
        );
        Ok(rewrite_elapsed)
    }

    #[test]
    fn hybrid_rewrite_raw_copies_unchanged_columns_across_widths() -> Result<(), Box<dyn Error>> {
        for unchanged_count in [1usize, 32, 256] {
            let total_column_count = unchanged_count + 1;
            let mut changed_positions = vec![0, total_column_count / 2, total_column_count - 1];
            changed_positions.sort_unstable();
            changed_positions.dedup();
            for changed_position in changed_positions {
                exercise_hybrid_rewrite(unchanged_count, changed_position)?;
            }
        }
        Ok(())
    }

    /// Manual CPU smoke benchmark for the hybrid rewrite's width scaling. Run with:
    /// `cargo test -p qdbr --lib hybrid_rewrite_width_sweep_benchmark -- --ignored --nocapture`.
    /// The shared helper asserts deterministic copied/encoded work counters;
    /// elapsed time is diagnostic rather than a timing-sensitive pass/fail oracle.
    #[test]
    #[ignore = "manual hybrid rewrite CPU width sweep"]
    fn hybrid_rewrite_width_sweep_benchmark() -> Result<(), Box<dyn Error>> {
        for unchanged_count in [1usize, 32, 256] {
            let changed_position = unchanged_count.div_ceil(2);
            let elapsed = exercise_hybrid_rewrite(unchanged_count, changed_position)?;
            eprintln!(
                "hybrid rewrite: unchanged_columns={unchanged_count}, encoded_columns=1, elapsed={elapsed:?}"
            );
        }
        Ok(())
    }

    /// Builds a designated (ascending) TIMESTAMP column. The encoder records the
    /// designated flag in `qdb_meta`, which is what the sorting-column derivation
    /// keys off, so sorting-column tests must use this rather than a plain
    /// TIMESTAMP column.
    fn make_designated_ts_with_id(id: i32, name: &'static str, values: &[i64]) -> Column {
        make_designated_ts_with_order(id, name, values, true)
    }

    /// Builds a designated TIMESTAMP column with an explicit sort direction.
    /// `ascending = false` records the descending order in `qdb_meta`, so the
    /// derived `sorting_columns` (and thus the ColumnIndex boundary order) are
    /// DESCENDING.
    fn make_designated_ts_with_order(
        id: i32,
        name: &'static str,
        values: &[i64],
        ascending: bool,
    ) -> Column {
        Column::from_raw_data(
            id,
            name,
            ColumnTypeTag::Timestamp.into_type().code(),
            0,
            values.len(),
            values.as_ptr() as *const u8,
            std::mem::size_of_val(values),
            null(),
            0,
            null(),
            0,
            true,
            ascending,
            0,
        )
        .unwrap()
    }

    #[test]
    fn generate_null_column_chunk_bytes_sets_encoding_stats() -> Result<(), Box<dyn Error>> {
        let int_type = ColumnTypeTag::Int.into_type();
        let symbol_type = ColumnTypeTag::Symbol.into_type();
        let int_partition = Partition {
            table: "t".to_string(),
            columns: vec![make_column_with_id(0, "i", int_type, &[1i32])],
        };
        let symbol_partition = Partition {
            table: "t".to_string(),
            columns: vec![make_column_with_id(0, "s", symbol_type, &[1i32])],
        };
        let (int_schema, _) = to_parquet_schema(&int_partition, false, -1, SeqTxn::UNSET)?;
        let (symbol_schema, _) = to_parquet_schema(&symbol_partition, false, -1, SeqTxn::UNSET)?;

        let (_, int_chunk) =
            super::generate_null_column_chunk_bytes(&int_schema.fields()[0], int_type, 10, 100)?;
        let (_, symbol_chunk) = super::generate_null_column_chunk_bytes(
            &symbol_schema.fields()[0],
            symbol_type,
            10,
            200,
        )?;

        assert_eq!(
            int_chunk.meta_data.as_ref().unwrap().encoding_stats,
            Some(vec![super::PageEncodingStats {
                page_type: super::PageType::DATA_PAGE,
                encoding: super::ThriftEncoding::PLAIN,
                count: 1,
            }])
        );
        assert_eq!(
            symbol_chunk.meta_data.as_ref().unwrap().encoding_stats,
            Some(vec![
                super::PageEncodingStats {
                    page_type: super::PageType::DICTIONARY_PAGE,
                    encoding: super::ThriftEncoding::PLAIN,
                    count: 1,
                },
                super::PageEncodingStats {
                    page_type: super::PageType::DATA_PAGE,
                    encoding: super::ThriftEncoding::RLE_DICTIONARY,
                    count: 1,
                },
            ])
        );
        Ok(())
    }

    #[test]
    fn append_replace_row_group() -> Result<(), Box<dyn Error>> {
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1 = [1i32, 2, i32::MIN, 3];
        let _expected1 = [Some(1i32), Some(2), None, Some(3)];
        let col2 = [0.5f32, 0.001, f32::nan(), 3.15];
        let _expected2 = [Some(0.5f32), Some(0.001), None, Some(3.15)];

        let col1_w = make_column("col1", ColumnTypeTag::Int.into_type(), &col1);
        let col2_w = make_column("col2", ColumnTypeTag::Float.into_type(), &col2);

        let partition = Partition {
            table: "test_table".to_string(),
            columns: [col1_w, col2_w].to_vec(),
        };

        ParquetWriter::new(&mut buf)
            .finish(partition)
            .expect("parquet writer");

        let col1_extra = [4, 5, i32::MIN];
        let extra_expected1 = [Some(4i32), Some(5), None];
        let col2_extra = [f32::nan(), 3.13, std::f32::consts::PI];
        let extra_expected2 = [None, Some(3.13), Some(std::f32::consts::PI)];

        let col1_extra_w = make_column("col1", ColumnTypeTag::Int.into_type(), &col1_extra);
        let col2_extra_w = make_column("col2", ColumnTypeTag::Float.into_type(), &col2_extra);

        let new_partition = Partition {
            table: "test_table".to_string(),
            columns: [col1_extra_w, col2_extra_w].to_vec(),
        };

        let orig_offset = buf.position();
        let metadata = read_metadata_with_size(&mut buf, orig_offset)?;

        let (schema, _) = to_parquet_schema(&new_partition, false, -1, SeqTxn::UNSET)?;

        let foptions = WriteOptions {
            write_statistics: true,
            compression: CompressionOptions::Uncompressed,
            version: Version::V1,
            row_group_size: None,
            data_page_size: None,
            raw_array_encoding: false,
            bloom_filter_fpp: DEFAULT_BLOOM_FILTER_FPP,
            min_compression_ratio: 0.0,
        };
        let bloom_filter_columns = HashSet::new();

        let options = write::WriteOptions {
            write_statistics: true,
            version: Version::V1,
            bloom_filter_fpp: DEFAULT_BLOOM_FILTER_FPP,
        };

        let (row_group, bloom_hashes) = create_row_group(
            &new_partition,
            0,
            col1_extra.len(),
            metadata.schema_descr.fields(),
            &to_encodings(&new_partition),
            foptions,
            &to_compressions(&new_partition),
            &bloom_filter_columns,
            false,
        )?;

        let (replace_row_group, replace_bloom_hashes) = create_row_group(
            &new_partition,
            0,
            col1_extra.len(),
            metadata.schema_descr.fields(),
            &to_encodings(&new_partition),
            foptions,
            &to_compressions(&new_partition),
            &bloom_filter_columns,
            false,
        )?;

        let orig_offset = buf.position();
        let metadata = read_metadata_with_size(&mut buf, orig_offset)?;
        let created_by = metadata.created_by.clone();

        let mut parquet_file = ParquetFile::new_updater(
            &mut buf,
            orig_offset,
            schema,
            options,
            created_by,
            None,
            metadata.into_thrift(),
            None,
        );
        parquet_file.append(row_group, &bloom_hashes)?;
        parquet_file.replace(replace_row_group, Some(0), &replace_bloom_hashes)?;
        parquet_file.end(None)?;

        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();
        save_to_file(&bytes);
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .expect("reader")
            .with_batch_size(8192)
            .build()
            .expect("builder");

        for batch in parquet_reader.flatten() {
            let i32array = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .expect("Failed to downcast");
            let collected: Vec<_> = i32array.iter().collect();
            assert_eq!(
                &collected,
                &extra_expected1
                    .iter()
                    .chain(extra_expected1.iter())
                    .cloned()
                    .collect::<Vec<_>>()
            );
            let f32array = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .expect("Failed to downcast");
            let collected: Vec<_> = f32array.iter().collect();
            assert_eq!(
                &collected,
                &extra_expected2
                    .iter()
                    .chain(extra_expected2.iter())
                    .cloned()
                    .collect::<Vec<_>>()
            );
        }
        Ok(())
    }

    /// Write an initial compressed parquet file to a temp file and return it.
    fn write_initial_zstd_file() -> Result<(NamedTempFile, Partition), Box<dyn Error>> {
        let col1 = [1i32, 2, i32::MIN, 3];
        let col2 = [0.5f32, 0.001, f32::nan(), 3.15];
        let col1_w = make_column("col1", ColumnTypeTag::Int.into_type(), &col1);
        let col2_w = make_column("col2", ColumnTypeTag::Float.into_type(), &col2);
        let partition = Partition {
            table: "test_table".to_string(),
            columns: [col1_w, col2_w].to_vec(),
        };

        let tmp = NamedTempFile::new()?;
        let file = tmp.reopen()?;
        ParquetWriter::new(file)
            .with_compression(CompressionOptions::Zstd(None))
            .finish(partition)?;

        // Build the partition for appending (same schema, fresh data).
        let col1_extra = [4, 5, i32::MIN];
        let col2_extra = [f32::nan(), 3.13, std::f32::consts::PI];
        let col1_extra_w = make_column("col1", ColumnTypeTag::Int.into_type(), &col1_extra);
        let col2_extra_w = make_column("col2", ColumnTypeTag::Float.into_type(), &col2_extra);
        let new_partition = Partition {
            table: "test_table".to_string(),
            columns: [col1_extra_w, col2_extra_w].to_vec(),
        };

        Ok((tmp, new_partition))
    }

    #[test]
    fn test_updater_with_min_compression_ratio() -> Result<(), Box<dyn Error>> {
        use crate::allocator::TestAllocatorState;

        // --- Case 1: very high min_compression_ratio forces fallback to uncompressed ---
        {
            let (tmp, new_partition) = write_initial_zstd_file()?;
            let file_len = tmp.as_file().metadata()?.len();
            let reader = tmp.reopen()?;
            let alloc_state = TestAllocatorState::new();

            let writer = tmp.reopen()?;
            let mut updater = super::ParquetUpdater::new(
                alloc_state.allocator(),
                reader,
                file_len,
                writer,
                file_len,                       // write_file_size: update (append) mode
                None,                           // sorting_columns
                true,                           // write_statistics
                false,                          // raw_array_encoding
                CompressionOptions::Zstd(None), // compression
                None,                           // row_group_size
                None,                           // data_page_size
                DEFAULT_BLOOM_FILTER_FPP,       // bloom_filter_fpp
                100.0,                          // min_compression_ratio (impossibly high)
                None,                           // parquet_meta_fd
                0,                              // parquet_meta_file_size
                0,                              // append_base
                -1,                             // existing_parquet_file_size
                SeqTxn::UNSET,                  // seq_txn
            )?;

            updater.insert_row_group(&new_partition, 1)?;
            updater.end(None)?;

            // Read back metadata and check the appended row group (index 1).
            let verify_file = tmp.reopen()?;
            let verify_len = verify_file.metadata()?.len();
            let metadata = read_metadata_with_size(&mut &verify_file, verify_len)?;
            assert_eq!(metadata.row_groups.len(), 2, "expected 2 row groups");

            // The appended row group should have fallen back to Uncompressed
            // because the ratio threshold (100.0) is impossibly high.
            let appended_rg = &metadata.row_groups[1];
            for col in appended_rg.columns() {
                assert_eq!(
                    col.compression(),
                    Compression::Uncompressed,
                    "expected uncompressed fallback for column {:?}",
                    col.descriptor().path_in_schema,
                );
            }

            // Original row group should still be Zstd (it was written before
            // the updater applied its ratio check).
            let original_rg = &metadata.row_groups[0];
            for col in original_rg.columns() {
                assert_eq!(
                    col.compression(),
                    Compression::Zstd,
                    "original row group column should remain Zstd",
                );
            }
        }

        // --- Case 2: low min_compression_ratio keeps compressed output ---
        {
            let (tmp, new_partition) = write_initial_zstd_file()?;
            let file_len = tmp.as_file().metadata()?.len();
            let reader = tmp.reopen()?;
            let alloc_state = TestAllocatorState::new();

            let writer = tmp.reopen()?;
            let mut updater = super::ParquetUpdater::new(
                alloc_state.allocator(),
                reader,
                file_len,
                writer,
                file_len, // write_file_size: update (append) mode
                None,
                true,
                false,
                CompressionOptions::Zstd(None),
                None,
                None,
                DEFAULT_BLOOM_FILTER_FPP,
                0.5,  // min_compression_ratio: ratio check active but easily met
                None, // parquet_meta_fd
                0,    // parquet_meta_file_size
                0,    // append_base
                -1,   // existing_parquet_file_size
                SeqTxn::UNSET,
            )?;

            updater.insert_row_group(&new_partition, 1)?;
            updater.end(None)?;

            let verify_file = tmp.reopen()?;
            let verify_len = verify_file.metadata()?.len();
            let metadata = read_metadata_with_size(&mut &verify_file, verify_len)?;
            assert_eq!(metadata.row_groups.len(), 2);

            // The appended row group should keep Zstd because the ratio
            // threshold (0.5) is trivially satisfied — it only requires
            // uncompressed/compressed >= 0.5.
            let appended_rg = &metadata.row_groups[1];
            for col in appended_rg.columns() {
                assert_eq!(
                    col.compression(),
                    Compression::Zstd,
                    "expected Zstd compression to be kept for column {:?}",
                    col.descriptor().path_in_schema,
                );
            }
        }

        Ok(())
    }

    /// Regression guard for the O3 update/append path. The updater seeds the new
    /// footer from the existing file via `FileMetaData::into_thrift()`, then merges
    /// row groups and writes it back unchanged through `end_file_incremental`. If
    /// `into_thrift()` drops `column_orders`, a partition loses its file-level
    /// column-order declaration on the first merge, and every spec-conformant
    /// reader (pyarrow, Spark, DuckDB, Trino, Iceberg) then treats the per-row-group
    /// min/max as undefined. Reads back with the independent Arrow reader, the way
    /// external tools observe the footer.
    #[test]
    fn appended_file_preserves_column_orders() -> Result<(), Box<dyn Error>> {
        use crate::allocator::TestAllocatorState;
        use std::io::Read as _;

        let (tmp, new_partition) = write_initial_zstd_file()?;
        let file_len = tmp.as_file().metadata()?.len();
        let reader = tmp.reopen()?;
        let writer = tmp.reopen()?;
        let alloc_state = TestAllocatorState::new();

        let mut updater = super::ParquetUpdater::new(
            alloc_state.allocator(),
            reader,
            file_len,
            writer,
            file_len, // write_file_size: update (append) mode
            None,     // sorting_columns
            true,     // write_statistics
            false,    // raw_array_encoding
            CompressionOptions::Zstd(None),
            None, // row_group_size
            None, // data_page_size
            DEFAULT_BLOOM_FILTER_FPP,
            0.0,  // min_compression_ratio
            None, // parquet_meta_fd
            0,    // parquet_meta_file_size
            0,
            -1, // existing_parquet_file_size
            SeqTxn::UNSET,
        )?;

        // Append a second row group, then re-read the footer.
        updater.insert_row_group(&new_partition, 1)?;
        updater.end(None)?;

        let mut bytes = Vec::new();
        tmp.reopen()?.read_to_end(&mut bytes)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))?;
        let file_meta = builder.metadata().file_metadata();
        let leaf_count = file_meta.schema_descr().num_columns();
        assert_eq!(leaf_count, 2, "expected 2 leaf columns after append");

        let orders = file_meta.column_orders().unwrap_or_else(|| {
            panic!(
                "appended file dropped column_orders; into_thrift() must regenerate them \
                 or external readers ignore min/max after the first O3 merge"
            )
        });
        assert_eq!(
            orders.len(),
            leaf_count,
            "column_orders must have one entry per leaf column"
        );
        for (i, order) in orders.iter().enumerate() {
            assert!(
                matches!(order, parquet::basic::ColumnOrder::TYPE_DEFINED_ORDER(_)),
                "leaf column {i} must declare TypeDefinedOrder, got {order:?}"
            );
        }
        Ok(())
    }

    /// Companion guard for the updater's rewrite mode (write_file_size == 0 -> a
    /// fresh output file), the path the O3 parquet merge uses. Rewrite mode
    /// finalizes through `ParquetFile::end` in `Mode::Write`, a different footer
    /// builder than both the plain writer and the append path, so it needs its own
    /// guard that `column_orders` is emitted. Reads back with the independent Arrow
    /// reader.
    #[test]
    fn rewritten_file_preserves_column_orders() -> Result<(), Box<dyn Error>> {
        use crate::allocator::TestAllocatorState;
        use parquet2::metadata::SortingColumn;
        use std::io::Read as _;

        let sorting = || Some(vec![SortingColumn::new(0, false, false)]);

        // Source file: designated timestamp at column 0, plus an int value column.
        let ts = [1i64, 2, 3, 4];
        let val = [10i32, 20, 30, 40];
        let partition = Partition {
            table: "t".to_string(),
            columns: vec![
                make_designated_ts_with_id(0, "ts", &ts),
                make_column("val", ColumnTypeTag::Int.into_type(), &val),
            ],
        };
        let src = NamedTempFile::new()?;
        ParquetWriter::new(src.reopen()?)
            .with_sorting_columns(sorting())
            .finish(partition)?;
        let src_len = src.as_file().metadata()?.len();

        // Rewrite into a fresh file: copy the existing row group, append a fresh one.
        let out = NamedTempFile::new()?;
        let alloc = TestAllocatorState::new();
        let mut updater = super::ParquetUpdater::new(
            alloc.allocator(),
            src.reopen()?, // reader: old file
            src_len,
            out.reopen()?, // writer: fresh file
            0,             // write_file_size == 0 -> rewrite
            sorting(),
            true,
            false,
            CompressionOptions::Uncompressed,
            None,
            None,
            DEFAULT_BLOOM_FILTER_FPP,
            0.0,
            None,
            0,
            0,
            -1,
            SeqTxn::UNSET,
        )?;
        let o3_ts = [5i64, 6, 7];
        let o3_val = [50i32, 60, 70];
        let o3 = Partition {
            table: "t".to_string(),
            columns: vec![
                make_designated_ts_with_id(0, "ts", &o3_ts),
                make_column("val", ColumnTypeTag::Int.into_type(), &o3_val),
            ],
        };
        updater.copy_row_group(0)?;
        updater.insert_row_group(&o3, 1)?;
        updater.end(None)?;

        let mut bytes = Vec::new();
        out.reopen()?.read_to_end(&mut bytes)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))?;
        let file_meta = builder.metadata().file_metadata();
        let leaf_count = file_meta.schema_descr().num_columns();
        assert_eq!(leaf_count, 2, "expected 2 leaf columns after rewrite");

        let orders = file_meta.column_orders().unwrap_or_else(|| {
            panic!("rewritten file dropped column_orders; the Mode::Write footer must declare them")
        });
        assert_eq!(
            orders.len(),
            leaf_count,
            "column_orders must have one entry per leaf column"
        );
        for (i, order) in orders.iter().enumerate() {
            assert!(
                matches!(order, parquet::basic::ColumnOrder::TYPE_DEFINED_ORDER(_)),
                "leaf column {i} must declare TypeDefinedOrder, got {order:?}"
            );
        }
        Ok(())
    }

    /// Two-column (designated TIMESTAMP + INT) partition with the given values.
    fn ts_val_partition(ts: &[i64], val: &[i32]) -> Partition {
        Partition {
            table: "t".to_string(),
            columns: vec![
                make_designated_ts_with_id(0, "ts", ts),
                make_column("val", ColumnTypeTag::Int.into_type(), val),
            ],
        }
    }

    /// Like `ts_val_partition` but the designated timestamp is descending, so the
    /// derived sorting column (and the timestamp ColumnIndex boundary order) are
    /// DESCENDING.
    fn ts_val_partition_desc(ts: &[i64], val: &[i32]) -> Partition {
        Partition {
            table: "t".to_string(),
            columns: vec![
                make_designated_ts_with_order(0, "ts", ts, false),
                make_column("val", ColumnTypeTag::Int.into_type(), val),
            ],
        }
    }

    /// Asserts every column carries both indexes, each OffsetIndex points at the
    /// column's data page, and the timestamp ColumnIndex matches `ts_bounds` with
    /// ASCENDING order. Loads with the page index Required (a mixed file rejects).
    fn assert_fully_indexed(bytes: &[u8], ts_bounds: &[(i64, i64)]) {
        use parquet::arrow::arrow_reader::ArrowReaderOptions;
        use parquet::file::page_index::index::Index;
        use parquet::format::BoundaryOrder;

        let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
            Bytes::from(bytes.to_vec()),
            ArrowReaderOptions::new().with_page_index(true),
        )
        .expect("a fully indexed file must load under the Required page-index policy");
        let md = builder.metadata();
        assert_eq!(md.num_row_groups(), ts_bounds.len());

        for rg in md.row_groups() {
            for col in rg.columns() {
                assert!(col.offset_index_offset().is_some(), "offset index offset");
                assert!(col.offset_index_length().is_some(), "offset index length");
                assert!(col.column_index_offset().is_some(), "column index offset");
                assert!(col.column_index_length().is_some(), "column index length");
            }
        }

        let column_index = md.column_index().expect("parsed column index");
        let offset_index = md.offset_index().expect("parsed offset index");
        assert_eq!(column_index.len(), md.num_row_groups());
        assert_eq!(offset_index.len(), md.num_row_groups());

        for (rg_i, rg) in md.row_groups().iter().enumerate() {
            for (col_i, col_oi) in offset_index[rg_i].iter().enumerate() {
                let locations = col_oi.page_locations();
                assert!(
                    !locations.is_empty(),
                    "rg{rg_i} col{col_i}: no page locations"
                );
                assert_eq!(
                    locations[0].offset,
                    rg.column(col_i).data_page_offset(),
                    "rg{rg_i} col{col_i}: offset index must point at the rebased data page",
                );
            }
            let (min, max) = ts_bounds[rg_i];
            match &column_index[rg_i][0] {
                Index::INT64(native) => {
                    assert_eq!(
                        native.boundary_order,
                        BoundaryOrder::ASCENDING,
                        "rg{rg_i}: sorted timestamp must declare ASCENDING"
                    );
                    assert_eq!(native.indexes[0].min, Some(min), "rg{rg_i} ts min");
                    assert_eq!(native.indexes[0].max, Some(max), "rg{rg_i} ts max");
                }
                other => panic!("rg{rg_i}: expected INT64 timestamp index, got {other:?}"),
            }
        }
    }

    /// Asserts no column carries a page index. Loads with the page index Required,
    /// which still succeeds since a fully unindexed file short-circuits cleanly.
    fn assert_unindexed(bytes: &[u8]) {
        use parquet::arrow::arrow_reader::ArrowReaderOptions;

        let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
            Bytes::from(bytes.to_vec()),
            ArrowReaderOptions::new().with_page_index(true),
        )
        .expect("an unindexed file must still load under the Required page-index policy");
        let md = builder.metadata();
        for rg in md.row_groups() {
            for col in rg.columns() {
                assert!(
                    col.offset_index_offset().is_none(),
                    "expected no offset index"
                );
                assert!(
                    col.column_index_offset().is_none(),
                    "expected no column index"
                );
            }
        }
    }

    /// Reads the int `val` column (leaf 1) across all row groups, proving the
    /// data is reachable from the (rebased) column offsets.
    fn read_val_column(bytes: &[u8]) -> Vec<Option<i32>> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes.to_vec()))
            .unwrap()
            .build()
            .unwrap();
        let mut out = Vec::new();
        for batch in reader {
            let arr = batch
                .unwrap()
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>();
            out.extend(arr);
        }
        out
    }

    /// Clears every column's page-index pointer, modelling a legacy file. The old
    /// index bytes stay as dead space, no longer referenced by the footer.
    fn strip_page_index(bytes: &[u8]) -> Result<Vec<u8>, Box<dyn Error>> {
        let len = bytes.len();
        let meta_len = i32::from_le_bytes(bytes[len - 8..len - 4].try_into()?) as usize;
        let meta_start = len - 8 - meta_len;
        let mut cur = Cursor::new(bytes);
        let metadata = read_metadata_with_size(&mut cur, len as u64)?;
        let mut thrift = metadata.into_thrift();
        for rg in &mut thrift.row_groups {
            for col in &mut rg.columns {
                col.column_index_offset = None;
                col.column_index_length = None;
                col.offset_index_offset = None;
                col.offset_index_length = None;
            }
        }
        let mut out = bytes[..meta_start].to_vec();
        parquet2::write::end_file(&mut out, &thrift)?;
        Ok(out)
    }

    /// Rewrite (copy one group, freshly encode another): the output is fully
    /// page-indexed, copied groups replaying their rebased source index.
    #[test]
    fn rewritten_file_writes_full_page_index() -> Result<(), Box<dyn Error>> {
        use crate::allocator::TestAllocatorState;
        use parquet2::metadata::SortingColumn;
        use std::io::Read as _;

        let sorting = || Some(vec![SortingColumn::new(0, false, false)]);
        let src = NamedTempFile::new()?;
        ParquetWriter::new(src.reopen()?)
            .with_sorting_columns(sorting())
            .finish(ts_val_partition(&[1, 2, 3, 4], &[10, 20, 30, 40]))?;
        let src_len = src.as_file().metadata()?.len();

        let out = NamedTempFile::new()?;
        let alloc = TestAllocatorState::new();
        let mut updater = super::ParquetUpdater::new(
            alloc.allocator(),
            src.reopen()?,
            src_len,
            out.reopen()?,
            0, // write_file_size == 0 -> rewrite
            sorting(),
            true,
            false,
            CompressionOptions::Uncompressed,
            None,
            None,
            DEFAULT_BLOOM_FILTER_FPP,
            0.0,
            None,
            0,
            0,
            -1,
            SeqTxn::UNSET,
        )?;
        updater.copy_row_group(0)?; // raw-copied: source index rebased
        updater.insert_row_group(&ts_val_partition(&[5, 6, 7], &[50, 60, 70]), 1)?; // fresh
        updater.end(None)?;

        let mut bytes = Vec::new();
        out.reopen()?.read_to_end(&mut bytes)?;

        assert_fully_indexed(&bytes, &[(1, 4), (5, 7)]);
        assert_eq!(
            read_val_column(&bytes),
            vec![
                Some(10),
                Some(20),
                Some(30),
                Some(40),
                Some(50),
                Some(60),
                Some(70)
            ],
        );
        Ok(())
    }

    /// A copied row group replays its source ColumnIndex bytes verbatim, so a
    /// DESCENDING source boundary order must survive a rewrite. Guards against the
    /// fresh-encode path (boundary order from sorting_columns) and the copy path
    /// disagreeing on direction.
    #[test]
    fn rewrite_preserves_descending_boundary_order_in_copied_group() -> Result<(), Box<dyn Error>> {
        use crate::allocator::TestAllocatorState;
        use parquet::arrow::arrow_reader::ArrowReaderOptions;
        use parquet::file::page_index::index::Index;
        use parquet::format::BoundaryOrder;
        use parquet2::metadata::SortingColumn;
        use std::io::Read as _;

        // Source: a descending designated timestamp -> ColumnIndex declares DESCENDING.
        let sorting = || Some(vec![SortingColumn::new(0, true, false)]); // descending
        let src = NamedTempFile::new()?;
        ParquetWriter::new(src.reopen()?)
            .with_sorting_columns(sorting())
            .finish(ts_val_partition_desc(&[40, 30, 20, 10], &[4, 3, 2, 1]))?;
        let src_len = src.as_file().metadata()?.len();

        let out = NamedTempFile::new()?;
        let alloc = TestAllocatorState::new();
        let mut updater = super::ParquetUpdater::new(
            alloc.allocator(),
            src.reopen()?,
            src_len,
            out.reopen()?,
            0, // write_file_size == 0 -> rewrite
            sorting(),
            true,
            false,
            CompressionOptions::Uncompressed,
            None,
            None,
            DEFAULT_BLOOM_FILTER_FPP,
            0.0,
            None,
            0,
            0,
            -1,
            SeqTxn::UNSET,
        )?;
        updater.copy_row_group(0)?; // raw-copied: source DESCENDING index rebased verbatim
        updater.end(None)?;

        let mut bytes = Vec::new();
        out.reopen()?.read_to_end(&mut bytes)?;

        let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
            Bytes::from(bytes.clone()),
            ArrowReaderOptions::new().with_page_index(true),
        )?;
        let md = builder.metadata();
        let column_index = md.column_index().expect("parsed column index");
        match &column_index[0][0] {
            Index::INT64(native) => assert_eq!(
                native.boundary_order,
                BoundaryOrder::DESCENDING,
                "copied group must preserve the source DESCENDING boundary order"
            ),
            other => panic!("expected INT64 timestamp index, got {other:?}"),
        }
        Ok(())
    }

    /// Append: the new group is indexed to match the cached group's kept index,
    /// so the file stays uniformly indexed.
    #[test]
    fn appended_file_writes_full_page_index() -> Result<(), Box<dyn Error>> {
        use crate::allocator::TestAllocatorState;
        use parquet2::metadata::SortingColumn;
        use std::io::Read as _;

        let sorting = || Some(vec![SortingColumn::new(0, false, false)]);
        let tmp = NamedTempFile::new()?;
        ParquetWriter::new(tmp.reopen()?)
            .with_sorting_columns(sorting())
            .finish(ts_val_partition(&[1, 2, 3, 4], &[10, 20, 30, 40]))?;
        let file_len = tmp.as_file().metadata()?.len();

        let alloc = TestAllocatorState::new();
        let mut updater = super::ParquetUpdater::new(
            alloc.allocator(),
            tmp.reopen()?,
            file_len,
            tmp.reopen()?,
            file_len, // update (append) mode
            sorting(),
            true,
            false,
            CompressionOptions::Uncompressed,
            None,
            None,
            DEFAULT_BLOOM_FILTER_FPP,
            0.0,
            None,
            0,
            0,
            -1,
            SeqTxn::UNSET,
        )?;
        updater.insert_row_group(&ts_val_partition(&[5, 6, 7], &[50, 60, 70]), 1)?;
        updater.end(None)?;

        let mut bytes = Vec::new();
        tmp.reopen()?.read_to_end(&mut bytes)?;

        assert_fully_indexed(&bytes, &[(1, 4), (5, 7)]);
        assert_eq!(
            read_val_column(&bytes),
            vec![
                Some(10),
                Some(20),
                Some(30),
                Some(40),
                Some(50),
                Some(60),
                Some(70)
            ],
        );
        Ok(())
    }

    /// Rewriting a source with no page index (a legacy file) leaves the whole
    /// output unindexed rather than mixed, since copied groups can't be indexed.
    #[test]
    fn rewrite_of_unindexed_source_stays_unindexed() -> Result<(), Box<dyn Error>> {
        use crate::allocator::TestAllocatorState;
        use parquet2::metadata::SortingColumn;
        use std::io::{Read as _, Write as _};

        let sorting = || Some(vec![SortingColumn::new(0, false, false)]);
        let indexed = NamedTempFile::new()?;
        ParquetWriter::new(indexed.reopen()?)
            .with_sorting_columns(sorting())
            .finish(ts_val_partition(&[1, 2, 3, 4], &[10, 20, 30, 40]))?;
        let mut indexed_bytes = Vec::new();
        indexed.reopen()?.read_to_end(&mut indexed_bytes)?;

        // Model a legacy, page-index-less source file.
        let stripped_bytes = strip_page_index(&indexed_bytes)?;
        assert_unindexed(&stripped_bytes);
        let src = NamedTempFile::new()?;
        src.reopen()?.write_all(&stripped_bytes)?;
        let src_len = stripped_bytes.len() as u64;

        let out = NamedTempFile::new()?;
        let alloc = TestAllocatorState::new();
        let mut updater = super::ParquetUpdater::new(
            alloc.allocator(),
            src.reopen()?,
            src_len,
            out.reopen()?,
            0, // rewrite
            sorting(),
            true,
            false,
            CompressionOptions::Uncompressed,
            None,
            None,
            DEFAULT_BLOOM_FILTER_FPP,
            0.0,
            None,
            0,
            0,
            -1,
            SeqTxn::UNSET,
        )?;
        updater.copy_row_group(0)?; // source has no index -> not indexable
        updater.insert_row_group(&ts_val_partition(&[5, 6, 7], &[50, 60, 70]), 1)?;
        updater.end(None)?;

        let mut bytes = Vec::new();
        out.reopen()?.read_to_end(&mut bytes)?;

        // Unindexed: the copied group can't be indexed, so neither is the fresh one.
        assert_unindexed(&bytes);
        assert_eq!(
            read_val_column(&bytes),
            vec![
                Some(10),
                Some(20),
                Some(30),
                Some(40),
                Some(50),
                Some(60),
                Some(70)
            ],
        );
        Ok(())
    }

    /// Appending to a source with no page index leaves the appended group
    /// unindexed too, so the file does not become mixed.
    #[test]
    fn append_to_unindexed_source_stays_unindexed() -> Result<(), Box<dyn Error>> {
        use crate::allocator::TestAllocatorState;
        use parquet2::metadata::SortingColumn;
        use std::io::{Read as _, Write as _};

        let sorting = || Some(vec![SortingColumn::new(0, false, false)]);
        let indexed = NamedTempFile::new()?;
        ParquetWriter::new(indexed.reopen()?)
            .with_sorting_columns(sorting())
            .finish(ts_val_partition(&[1, 2, 3, 4], &[10, 20, 30, 40]))?;
        let mut indexed_bytes = Vec::new();
        indexed.reopen()?.read_to_end(&mut indexed_bytes)?;

        // Model a legacy, page-index-less source and append to it in place.
        let stripped_bytes = strip_page_index(&indexed_bytes)?;
        let tmp = NamedTempFile::new()?;
        tmp.reopen()?.write_all(&stripped_bytes)?;
        let file_len = stripped_bytes.len() as u64;

        let alloc = TestAllocatorState::new();
        let mut updater = super::ParquetUpdater::new(
            alloc.allocator(),
            tmp.reopen()?,
            file_len,
            tmp.reopen()?,
            file_len, // update (append) mode
            sorting(),
            true,
            false,
            CompressionOptions::Uncompressed,
            None,
            None,
            DEFAULT_BLOOM_FILTER_FPP,
            0.0,
            None,
            0,
            0,
            -1,
            SeqTxn::UNSET,
        )?;
        updater.insert_row_group(&ts_val_partition(&[5, 6, 7], &[50, 60, 70]), 1)?;
        updater.end(None)?;

        let mut bytes = Vec::new();
        tmp.reopen()?.read_to_end(&mut bytes)?;

        assert_unindexed(&bytes);
        assert_eq!(
            read_val_column(&bytes),
            vec![
                Some(10),
                Some(20),
                Some(30),
                Some(40),
                Some(50),
                Some(60),
                Some(70)
            ],
        );
        Ok(())
    }

    /// With statistics off the writer emits an OffsetIndex but no ColumnIndex; a
    /// rewrite keeps the output uniformly offset-indexed (the copied-OffsetIndex-
    /// without-ColumnIndex path).
    #[test]
    fn rewrite_preserves_offset_only_index_when_statistics_disabled() -> Result<(), Box<dyn Error>>
    {
        use crate::allocator::TestAllocatorState;
        use parquet::arrow::arrow_reader::ArrowReaderOptions;
        use parquet2::metadata::SortingColumn;
        use std::io::Read as _;

        let sorting = || Some(vec![SortingColumn::new(0, false, false)]);
        let src = NamedTempFile::new()?;
        ParquetWriter::new(src.reopen()?)
            .with_sorting_columns(sorting())
            .with_statistics(false)
            .finish(ts_val_partition(&[1, 2, 3, 4], &[10, 20, 30, 40]))?;
        let src_len = src.as_file().metadata()?.len();

        let out = NamedTempFile::new()?;
        let alloc = TestAllocatorState::new();
        let mut updater = super::ParquetUpdater::new(
            alloc.allocator(),
            src.reopen()?,
            src_len,
            out.reopen()?,
            0, // rewrite
            sorting(),
            false, // statistics disabled
            false,
            CompressionOptions::Uncompressed,
            None,
            None,
            DEFAULT_BLOOM_FILTER_FPP,
            0.0,
            None,
            0,
            0,
            -1,
            SeqTxn::UNSET,
        )?;
        updater.copy_row_group(0)?; // source has an OffsetIndex but no ColumnIndex
        updater.insert_row_group(&ts_val_partition(&[5, 6, 7], &[50, 60, 70]), 1)?;
        updater.end(None)?;

        let mut bytes = Vec::new();
        out.reopen()?.read_to_end(&mut bytes)?;

        let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
            Bytes::from(bytes.clone()),
            ArrowReaderOptions::new().with_page_index(true),
        )?;
        let md = builder.metadata();
        assert_eq!(md.num_row_groups(), 2);
        for rg in md.row_groups() {
            for col in rg.columns() {
                assert!(col.offset_index_offset().is_some(), "offset index present");
                assert!(
                    col.column_index_offset().is_none(),
                    "no column index when statistics are disabled"
                );
            }
        }
        assert_eq!(
            read_val_column(&bytes),
            vec![
                Some(10),
                Some(20),
                Some(30),
                Some(40),
                Some(50),
                Some(60),
                Some(70)
            ],
        );
        Ok(())
    }

    /// Rewriting a source written without a ColumnIndex (OffsetIndex only) while
    /// runtime statistics are ON: the copied group cannot supply a ColumnIndex, so
    /// write_page_index drops it from the freshly encoded group too, keeping the
    /// output uniformly offset-only. Without that gate the copied group would lack a
    /// ColumnIndex while the fresh group carried one -- a mixed page index.
    #[test]
    fn rewrite_with_statistics_on_matches_offset_only_source() -> Result<(), Box<dyn Error>> {
        use crate::allocator::TestAllocatorState;
        use parquet::arrow::arrow_reader::ArrowReaderOptions;
        use parquet2::metadata::SortingColumn;
        use std::io::Read as _;

        let sorting = || Some(vec![SortingColumn::new(0, false, false)]);
        let src = NamedTempFile::new()?;
        ParquetWriter::new(src.reopen()?)
            .with_sorting_columns(sorting())
            .with_statistics(false) // source: OffsetIndex but no ColumnIndex
            .finish(ts_val_partition(&[1, 2, 3, 4], &[10, 20, 30, 40]))?;
        let src_len = src.as_file().metadata()?.len();

        let out = NamedTempFile::new()?;
        let alloc = TestAllocatorState::new();
        let mut updater = super::ParquetUpdater::new(
            alloc.allocator(),
            src.reopen()?,
            src_len,
            out.reopen()?,
            0, // rewrite
            sorting(),
            true, // runtime statistics on: the fresh group must still match the source
            false,
            CompressionOptions::Uncompressed,
            None,
            None,
            DEFAULT_BLOOM_FILTER_FPP,
            0.0,
            None,
            0,
            0,
            -1,
            SeqTxn::UNSET,
        )?;
        updater.copy_row_group(0)?; // copied group has an OffsetIndex but no ColumnIndex
        updater.insert_row_group(&ts_val_partition(&[5, 6, 7], &[50, 60, 70]), 1)?;
        updater.end(None)?;

        let mut bytes = Vec::new();
        out.reopen()?.read_to_end(&mut bytes)?;

        let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
            Bytes::from(bytes.clone()),
            ArrowReaderOptions::new().with_page_index(true),
        )?;
        let md = builder.metadata();
        assert_eq!(md.num_row_groups(), 2);
        for rg in md.row_groups() {
            for col in rg.columns() {
                assert!(
                    col.offset_index_offset().is_some(),
                    "offset index present on every row group"
                );
                assert!(
                    col.column_index_offset().is_none(),
                    "no column index on any row group: the offset-only copied group \
                     forces the fresh group to drop its ColumnIndex"
                );
            }
        }
        assert_eq!(
            read_val_column(&bytes),
            vec![
                Some(10),
                Some(20),
                Some(30),
                Some(40),
                Some(50),
                Some(60),
                Some(70)
            ],
        );
        Ok(())
    }

    /// Appending with statistics disabled to a source written with a ColumnIndex:
    /// the appended group adopts the source's ColumnIndex choice instead of the
    /// runtime flag, so the output is uniformly indexed rather than mixed.
    #[test]
    fn append_with_statistics_off_matches_indexed_source() -> Result<(), Box<dyn Error>> {
        use crate::allocator::TestAllocatorState;
        use parquet2::metadata::SortingColumn;
        use std::io::Read as _;

        let sorting = || Some(vec![SortingColumn::new(0, false, false)]);
        let tmp = NamedTempFile::new()?;
        ParquetWriter::new(tmp.reopen()?)
            .with_sorting_columns(sorting())
            .with_statistics(true) // source carries a ColumnIndex
            .finish(ts_val_partition(&[1, 2, 3, 4], &[10, 20, 30, 40]))?;
        let file_len = tmp.as_file().metadata()?.len();

        let alloc = TestAllocatorState::new();
        let mut updater = super::ParquetUpdater::new(
            alloc.allocator(),
            tmp.reopen()?,
            file_len,
            tmp.reopen()?,
            file_len, // update (append) mode
            sorting(),
            false, // runtime statistics off: must be overridden to match the source
            false,
            CompressionOptions::Uncompressed,
            None,
            None,
            DEFAULT_BLOOM_FILTER_FPP,
            0.0,
            None,
            0,
            0,
            -1,
            SeqTxn::UNSET,
        )?;
        updater.insert_row_group(&ts_val_partition(&[5, 6, 7], &[50, 60, 70]), 1)?;
        updater.end(None)?;

        let mut bytes = Vec::new();
        tmp.reopen()?.read_to_end(&mut bytes)?;

        assert_fully_indexed(&bytes, &[(1, 4), (5, 7)]);
        assert_eq!(
            read_val_column(&bytes),
            vec![
                Some(10),
                Some(20),
                Some(30),
                Some(40),
                Some(50),
                Some(60),
                Some(70)
            ],
        );
        Ok(())
    }

    /// Appending with statistics enabled to a source written without a ColumnIndex
    /// (OffsetIndex only): the appended group drops its ColumnIndex to match the
    /// source, so the file stays uniformly offset-only rather than mixed.
    #[test]
    fn append_with_statistics_on_matches_offset_only_source() -> Result<(), Box<dyn Error>> {
        use crate::allocator::TestAllocatorState;
        use parquet::arrow::arrow_reader::ArrowReaderOptions;
        use parquet2::metadata::SortingColumn;
        use std::io::Read as _;

        let sorting = || Some(vec![SortingColumn::new(0, false, false)]);
        let tmp = NamedTempFile::new()?;
        ParquetWriter::new(tmp.reopen()?)
            .with_sorting_columns(sorting())
            .with_statistics(false) // source: OffsetIndex but no ColumnIndex
            .finish(ts_val_partition(&[1, 2, 3, 4], &[10, 20, 30, 40]))?;
        let file_len = tmp.as_file().metadata()?.len();

        let alloc = TestAllocatorState::new();
        let mut updater = super::ParquetUpdater::new(
            alloc.allocator(),
            tmp.reopen()?,
            file_len,
            tmp.reopen()?,
            file_len, // update (append) mode
            sorting(),
            true, // runtime statistics on: must be overridden to match the source
            false,
            CompressionOptions::Uncompressed,
            None,
            None,
            DEFAULT_BLOOM_FILTER_FPP,
            0.0,
            None,
            0,
            0,
            -1,
            SeqTxn::UNSET,
        )?;
        updater.insert_row_group(&ts_val_partition(&[5, 6, 7], &[50, 60, 70]), 1)?;
        updater.end(None)?;

        let mut bytes = Vec::new();
        tmp.reopen()?.read_to_end(&mut bytes)?;

        let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
            Bytes::from(bytes.clone()),
            ArrowReaderOptions::new().with_page_index(true),
        )?;
        let md = builder.metadata();
        assert_eq!(md.num_row_groups(), 2);
        for rg in md.row_groups() {
            for col in rg.columns() {
                assert!(
                    col.offset_index_offset().is_some(),
                    "offset index present on every row group"
                );
                assert!(
                    col.column_index_offset().is_none(),
                    "no column index on any row group"
                );
            }
        }
        assert_eq!(
            read_val_column(&bytes),
            vec![
                Some(10),
                Some(20),
                Some(30),
                Some(40),
                Some(50),
                Some(60),
                Some(70)
            ],
        );
        Ok(())
    }

    /// Appending an opaque-Binary row group whose page carries an unbounded
    /// (max-less) max to a fully ColumnIndexed source. The appended group cannot
    /// supply a ColumnIndex, so the writer must strip the cached source group's
    /// ColumnIndex too, leaving the file uniformly offset-only rather than
    /// advertising a ColumnIndex on the cached group but not the appended one.
    #[test]
    fn append_unbounded_max_binary_strips_cached_column_index() -> Result<(), Box<dyn Error>> {
        use crate::allocator::TestAllocatorState;
        use parquet::arrow::arrow_reader::ArrowReaderOptions;
        use std::io::Read as _;
        use std::ptr::null;

        // QuestDB opaque-Binary column layout: a [len i64][bytes] primary buffer and
        // one i64 offset per value.
        fn qdb_binary(values: &[&[u8]]) -> (Vec<u8>, Vec<i64>) {
            let mut primary = Vec::new();
            let mut offsets = Vec::with_capacity(values.len());
            for &value in values {
                offsets.push(primary.len() as i64);
                primary.extend_from_slice(&(value.len() as i64).to_le_bytes());
                primary.extend_from_slice(value);
            }
            (primary, offsets)
        }

        let binary_code = ColumnType::new(ColumnTypeTag::Binary, 0).code();

        // Source: designated ts + a short-valued opaque-Binary column, statistics on,
        // so every column carries a ColumnIndex (fully indexed).
        let src_ts = [1i64, 2, 3];
        let src_values: Vec<&[u8]> = vec![&[0x01], &[0x02], &[0x03]];
        let (src_primary, src_offsets) = qdb_binary(&src_values);
        let src_blob = Column::from_raw_data(
            1,
            "b",
            binary_code,
            0,
            src_values.len(),
            src_primary.as_ptr(),
            src_primary.len(),
            src_offsets.as_ptr() as *const u8,
            std::mem::size_of_val(src_offsets.as_slice()),
            null(),
            0,
            false,
            false,
            0,
        )?;
        let src_partition = Partition {
            table: "t".to_string(),
            columns: vec![make_designated_ts_with_id(0, "ts", &src_ts), src_blob],
        };

        let src = NamedTempFile::new()?;
        ParquetWriter::new(src.reopen()?)
            .with_statistics(true)
            .finish(src_partition)?;
        let src_len = src.as_file().metadata()?.len();

        // Appended group: a value whose clamped 9-byte prefix is all 0xFF and exceeds
        // 9 bytes has no short upper bound, so its page emits no max (unbounded) and the
        // new group cannot carry a ColumnIndex.
        let app_ts = [4i64, 5];
        let unbounded = vec![0xFFu8; 12];
        let app_values: Vec<&[u8]> = vec![&[0x04], &unbounded];
        let (app_primary, app_offsets) = qdb_binary(&app_values);
        let app_blob = Column::from_raw_data(
            1,
            "b",
            binary_code,
            0,
            app_values.len(),
            app_primary.as_ptr(),
            app_primary.len(),
            app_offsets.as_ptr() as *const u8,
            std::mem::size_of_val(app_offsets.as_slice()),
            null(),
            0,
            false,
            false,
            0,
        )?;
        let app_partition = Partition {
            table: "t".to_string(),
            columns: vec![make_designated_ts_with_id(0, "ts", &app_ts), app_blob],
        };

        let alloc = TestAllocatorState::new();
        let mut updater = super::ParquetUpdater::new(
            alloc.allocator(),
            src.reopen()?,
            src_len,
            src.reopen()?,
            src_len, // update (append) mode
            None,
            true, // runtime statistics on; the source is indexed regardless
            false,
            CompressionOptions::Uncompressed,
            None,
            None,
            DEFAULT_BLOOM_FILTER_FPP,
            0.0,
            None,
            0,  // parquet_meta_file_size
            0,  // append_base
            -1, // existing_parquet_file_size
            SeqTxn::UNSET,
        )?;
        updater.insert_row_group(&app_partition, 1)?;
        updater.end(None)?;

        let mut bytes = Vec::new();
        src.reopen()?.read_to_end(&mut bytes)?;

        let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
            Bytes::from(bytes.clone()),
            ArrowReaderOptions::new().with_page_index(true),
        )?;
        let md = builder.metadata();
        assert_eq!(md.num_row_groups(), 2);
        assert_eq!(md.file_metadata().num_rows(), 5);
        for rg in md.row_groups() {
            for col in rg.columns() {
                assert!(
                    col.offset_index_offset().is_some(),
                    "offset index present on every row group"
                );
                assert!(
                    col.column_index_offset().is_none(),
                    "no column index on any row group: the appended unbounded-max Binary \
                     group forces the cached source group to drop its ColumnIndex"
                );
            }
        }
        Ok(())
    }

    /// Append after ADD COLUMN must keep footer statistics. ADD COLUMN raw-copies
    /// the existing row group (stats kept) and backfills a stats-less null column,
    /// emitting no page index. A later append in update mode must still read the
    /// source as statistics-bearing (from the copied columns) and keep computing
    /// footer min/max, not infer statistics-off from the missing page index.
    #[test]
    fn append_after_add_column_keeps_footer_statistics() -> Result<(), Box<dyn Error>> {
        use crate::allocator::TestAllocatorState;

        // 1. Source with statistics on: [ts(id=0), val(id=1)].
        let ts = [1i64, 2, 3, 4];
        let val = [10i32, 20, 30, 40];
        let src_partition = Partition {
            table: "t".to_string(),
            columns: vec![
                make_designated_ts_with_id(0, "ts", &ts),
                make_column_with_id(1, "val", ColumnTypeTag::Int.into_type(), &val),
            ],
        };
        let src = NamedTempFile::new()?;
        ParquetWriter::new(src.reopen()?)
            .with_statistics(true)
            .finish(src_partition)?;
        let src_len = src.as_file().metadata()?.len();

        // 2. ADD COLUMN rewrite to [ts, val, added]: raw-copies the source row
        //    group (stats kept, no page index) and backfills `added` with nulls.
        let added = ColumnTypeTag::Int.into_type();
        let added_file = NamedTempFile::new()?;
        let alloc = TestAllocatorState::new();
        let mut rewriter = super::ParquetUpdater::new(
            alloc.allocator(),
            src.reopen()?,
            src_len,
            added_file.reopen()?,
            0, // rewrite
            None,
            true, // statistics on
            false,
            CompressionOptions::Uncompressed,
            None,
            None,
            DEFAULT_BLOOM_FILTER_FPP,
            0.0,
            None,
            0,
            0,
            -1,
            SeqTxn::UNSET,
        )?;
        let target = Partition {
            table: "t".to_string(),
            columns: vec![
                make_designated_ts_with_id(0, "ts", &ts),
                make_column_with_id(1, "val", ColumnTypeTag::Int.into_type(), &val),
                make_column_with_id(2, "added", added, &val),
            ],
        };
        rewriter.set_target_schema(&target)?;
        rewriter.copy_row_group_with_null_columns(0, &[(2, added)])?;
        rewriter.end(None)?;

        // The rewrite must establish the F1 precondition: stats present on the
        // copied data columns, but no ColumnIndex anywhere.
        let added_len = added_file.as_file().metadata()?.len();
        {
            let f = added_file.reopen()?;
            let md = read_metadata_with_size(&mut &f, added_len)?;
            assert_eq!(md.row_groups.len(), 1);
            let cols = md.row_groups[0].columns();
            assert!(cols[0].statistics().is_some(), "ts keeps stats after copy");
            assert!(cols[1].statistics().is_some(), "val keeps stats after copy");
            assert!(
                cols.iter().all(|c| c.column_index_offset().is_none()),
                "ADD COLUMN output carries no ColumnIndex"
            );
        }

        // 3. O3 append into the rewritten file (update mode).
        let app_ts = [5i64, 6, 7];
        let app_val = [50i32, 60, 70];
        let app_added = [1i32, 2, 3];
        let app = Partition {
            table: "t".to_string(),
            columns: vec![
                make_designated_ts_with_id(0, "ts", &app_ts),
                make_column_with_id(1, "val", ColumnTypeTag::Int.into_type(), &app_val),
                make_column_with_id(2, "added", added, &app_added),
            ],
        };
        let alloc2 = TestAllocatorState::new();
        let mut updater = super::ParquetUpdater::new(
            alloc2.allocator(),
            added_file.reopen()?,
            added_len,
            added_file.reopen()?,
            added_len, // update (append) mode
            None,
            true,
            false,
            CompressionOptions::Uncompressed,
            None,
            None,
            DEFAULT_BLOOM_FILTER_FPP,
            0.0,
            None,
            0,
            0,
            -1,
            SeqTxn::UNSET,
        )?;
        updater.insert_row_group(&app, 1)?;
        updater.end(None)?;

        // 4. The appended group must carry footer chunk statistics: the writer
        //    kept computing min/max because the source's copied columns still
        //    bear stats, rather than dropping them over the absent page index.
        let final_len = added_file.as_file().metadata()?.len();
        let f = added_file.reopen()?;
        let md = read_metadata_with_size(&mut &f, final_len)?;
        assert_eq!(md.row_groups.len(), 2);
        let appended = md.row_groups[1].columns();
        assert!(
            appended.iter().all(|c| c.statistics().is_some()),
            "every appended column must keep footer min/max after append-to-ADD-COLUMN"
        );
        Ok(())
    }

    /// After copy_row_group with a non-zero offset shift, the bloom filter
    /// on the last column must still be readable and contain the original
    /// values. This requires either copying the bloom bytes into the new
    /// file or clearing the offset to None.
    #[test]
    fn copy_row_group_preserves_last_col_bloom_filter() -> Result<(), Box<dyn Error>> {
        use std::io::{Read as _, Seek, SeekFrom};

        // Create a parquet file with 2 row groups.
        // Bloom filter on col1 (index 1) -- the LAST column.
        let col0_rg0 = [1i32, 2, 3, 4];
        let col1_rg0 = [0.5f32, 0.001, 3.15, 2.72];
        let col0_rg1 = [5i32, 6, 7, 8];
        let col1_rg1 = [1.1f32, 2.2, 3.3, 4.4];

        let partition_rg0 = Partition {
            table: "test_table".to_string(),
            columns: vec![
                make_column("col0", ColumnTypeTag::Int.into_type(), &col0_rg0),
                make_column("col1", ColumnTypeTag::Float.into_type(), &col1_rg0),
            ],
        };
        let partition_rg1 = Partition {
            table: "test_table".to_string(),
            columns: vec![
                make_column("col0", ColumnTypeTag::Int.into_type(), &col0_rg1),
                make_column("col1", ColumnTypeTag::Float.into_type(), &col1_rg1),
            ],
        };

        let (schema, _) = crate::parquet_write::schema::to_parquet_schema(
            &partition_rg0,
            false,
            -1,
            SeqTxn::UNSET,
        )?;
        let encodings = to_encodings(&partition_rg0);

        let mut bloom_cols = HashSet::new();
        bloom_cols.insert(1usize); // bloom filter on last column

        let foptions = WriteOptions {
            write_statistics: true,
            compression: CompressionOptions::Uncompressed,
            version: Version::V1,
            row_group_size: None,
            data_page_size: None,
            raw_array_encoding: false,
            bloom_filter_fpp: DEFAULT_BLOOM_FILTER_FPP,
            min_compression_ratio: 0.0,
        };

        let options = write::WriteOptions {
            write_statistics: true,
            version: Version::V1,
            bloom_filter_fpp: DEFAULT_BLOOM_FILTER_FPP,
        };

        let compressions_rg0 = to_compressions(&partition_rg0);
        let (rg0, bloom0) = create_row_group(
            &partition_rg0,
            0,
            col0_rg0.len(),
            schema.fields(),
            &encodings,
            foptions,
            &compressions_rg0,
            &bloom_cols,
            false,
        )?;
        let compressions_rg1 = to_compressions(&partition_rg1);
        let (rg1, bloom1) = create_row_group(
            &partition_rg1,
            0,
            col0_rg1.len(),
            schema.fields(),
            &encodings,
            foptions,
            &compressions_rg1,
            &bloom_cols,
            false,
        )?;

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let mut pf = ParquetFile::with_sorting_columns(
            &mut buf,
            schema.clone(),
            options,
            Some("test".to_string()),
            None,
        );
        pf.write(rg0, &bloom0)?;
        pf.write(rg1, &bloom1)?;

        let mut qdb_meta = crate::parquet::qdb_metadata::QdbMeta::new(2);
        qdb_meta
            .schema
            .push(crate::parquet::qdb_metadata::QdbMetaCol {
                column_type: ColumnTypeTag::Int.into_type(),
                column_top: 0,
                format: None,
                ascii: None,
                id: None,
            });
        qdb_meta
            .schema
            .push(crate::parquet::qdb_metadata::QdbMetaCol {
                column_type: ColumnTypeTag::Float.into_type(),
                column_top: 0,
                format: None,
                ascii: None,
                id: None,
            });
        let qdb_json = qdb_meta.serialize().expect("serialize qdb meta");
        let kv = parquet2::metadata::KeyValue {
            key: crate::parquet::qdb_metadata::QDB_META_KEY.to_string(),
            value: Some(qdb_json),
        };
        pf.end(Some(vec![kv]))?;

        let orig_data = buf.into_inner();
        let orig_len = orig_data.len() as u64;

        // Read metadata, verify bloom filter exists on col1 (last column).
        let mut reader_cursor = Cursor::new(&orig_data[..]);
        let metadata = read_metadata_with_size(&mut reader_cursor, orig_len)?;
        assert_eq!(metadata.row_groups.len(), 2);

        let old_rg1 = &metadata.row_groups[1];
        assert!(
            old_rg1.columns()[0]
                .metadata()
                .bloom_filter_offset
                .is_none(),
            "col0 should NOT have bloom filter"
        );
        assert!(
            old_rg1.columns()[1]
                .metadata()
                .bloom_filter_offset
                .is_some(),
            "col1 (last col) should have bloom filter"
        );

        // Verify the bloom filter is readable and correct in the original file.
        let orig_bf_bitset =
            parquet2::bloom_filter::read_from_slice(&old_rg1.columns()[1], &orig_data)?;
        assert!(
            !orig_bf_bitset.is_empty(),
            "original bloom filter should be non-empty"
        );
        for &val in &col1_rg1 {
            let hash = parquet2::bloom_filter::hash_native(val);
            assert!(
                parquet2::bloom_filter::is_in_set(orig_bf_bitset, hash),
                "original bloom filter should contain {val}"
            );
        }

        // Simulate copy_row_group with offset shift.
        // Write a differently-sized row group first to create a non-zero offset_delta.
        let col0_new = [100i32, 200, 300, 400, 500]; // 5 values, different size
        let col1_new = [9.9f32, 8.8, 7.7, 6.6, 5.5];
        let partition_new = Partition {
            table: "test_table".to_string(),
            columns: vec![
                make_column("col0", ColumnTypeTag::Int.into_type(), &col0_new),
                make_column("col1", ColumnTypeTag::Float.into_type(), &col1_new),
            ],
        };
        let compressions_new = to_compressions(&partition_new);
        let (rg_new, bloom_new) = create_row_group(
            &partition_new,
            0,
            col0_new.len(),
            schema.fields(),
            &to_encodings(&partition_new),
            foptions,
            &compressions_new,
            &bloom_cols,
            false,
        )?;

        let mut new_buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let mut new_pf = ParquetFile::with_sorting_columns(
            &mut new_buf,
            schema.clone(),
            options,
            Some("test".to_string()),
            None,
        );
        new_pf.write(rg_new, &bloom_new)?;

        // Replicate the production copy_row_group logic (from update.rs:307-374).
        new_pf.ensure_started()?;
        let columns_meta = old_rg1.columns();

        let mut rg_start = u64::MAX;
        let mut rg_end = 0u64;
        let mut last_col_idx = 0usize;
        for (i, col) in columns_meta.iter().enumerate() {
            let (start, len) = col.byte_range();
            rg_start = rg_start.min(start);
            let end = start + len;
            if end >= rg_end {
                rg_end = end;
                last_col_idx = i;
            }
        }

        // Extend rg_end to cover the last column's bloom filter (mirrors production code).
        if columns_meta[last_col_idx]
            .metadata()
            .bloom_filter_offset
            .is_some()
        {
            let mut bf_reader = Cursor::new(&orig_data[..]);
            let bf_total =
                parquet2::bloom_filter::total_size(&columns_meta[last_col_idx], &mut bf_reader)
                    .expect("read bloom filter total size");
            if bf_total > 0 {
                let bf_offset = columns_meta[last_col_idx]
                    .metadata()
                    .bloom_filter_offset
                    .unwrap() as u64;
                rg_end = rg_end.max(bf_offset + bf_total);
            }
        }

        let raw_len = (rg_end - rg_start) as usize;
        let mut raw_bytes = vec![0u8; raw_len];
        let mut reader_cursor2 = Cursor::new(&orig_data[..]);
        reader_cursor2.seek(SeekFrom::Start(rg_start))?;
        reader_cursor2.read_exact(&mut raw_bytes)?;

        let new_offset = new_pf.current_offset();
        let offset_delta = new_offset as i64 - rg_start as i64;
        assert_ne!(
            offset_delta, 0,
            "offset_delta must be non-zero for this test"
        );

        // Apply the SAME offset adjustments as the production code.
        let mut thrift_rg = old_rg1.clone().into_thrift();
        for col_chunk in &mut thrift_rg.columns {
            adjust_column_chunk_offsets(col_chunk, offset_delta);
        }

        new_pf.write_raw_row_group(&raw_bytes, thrift_rg)?;
        new_pf.end(None)?;

        // Verify the copied bloom filter is still correct in the new file.
        let new_data = new_buf.into_inner();
        let new_len = new_data.len() as u64;
        let mut new_reader = Cursor::new(&new_data[..]);
        let new_metadata = read_metadata_with_size(&mut new_reader, new_len)?;

        let copied_rg = &new_metadata.row_groups[1];
        assert!(
            copied_rg.columns()[1]
                .metadata()
                .bloom_filter_offset
                .is_some(),
            "copied column should preserve bloom filter offset"
        );

        // The bloom filter must be readable and contain the original values.
        let new_bf_bitset =
            parquet2::bloom_filter::read_from_slice(&copied_rg.columns()[1], &new_data)
                .expect("bloom filter at adjusted offset should be readable");
        assert!(
            !new_bf_bitset.is_empty(),
            "copied bloom filter should be non-empty"
        );
        for &val in &col1_rg1 {
            let hash = parquet2::bloom_filter::hash_native(val);
            assert!(
                parquet2::bloom_filter::is_in_set(new_bf_bitset, hash),
                "copied bloom filter should contain {val}"
            );
        }

        Ok(())
    }

    /// Files written before commit 247cb447cd carry `Repetition::Required` for
    /// BYTE/SHORT/CHAR. That commit and follow-up c65b523964 flipped both the
    /// schema and the encoder dispatch to Optional for new files. In update
    /// mode the legacy schema is preserved (`ensure_schema_matches_columns`
    /// returns early when `!is_rewrite`), so without a repetition-aware
    /// dispatch the encoder would assert Optional against a Required schema
    /// and abort the JVM. The dispatch in `encode.rs` now picks
    /// `encode_int_notnull` when the schema field is Required and
    /// `encode_int_nullable` when it is Optional, for all three encodings
    /// (Plain, DeltaBinaryPacked, RleDictionary).
    ///
    /// This test exercises the success path of both branches by calling
    /// `create_row_group` (the same function `insert_row_group` /
    /// `replace_row_group` delegate to) with a hand-built schema field that
    /// pins repetition. The Required arm verifies the legacy-file fix; the
    /// Optional arm verifies the modern-file behaviour is untouched.
    #[test]
    fn create_row_group_handles_both_repetitions_for_byte_short_char() {
        use parquet2::encoding::Encoding;
        use parquet2::schema::types::{
            IntegerType, ParquetType, PhysicalType, PrimitiveConvertedType, PrimitiveLogicalType,
        };
        use parquet2::schema::Repetition;
        use parquet2::write::Version;

        fn build_field(tag: ColumnTypeTag, repetition: Repetition, id: i32) -> ParquetType {
            // Mirrors the field definitions schema.rs builds for these tags
            // (Byte=Int8, Short=Int16, Char=Uint16); only the repetition is
            // a parameter so the test can pin both legacy and modern shapes.
            let (converted, logical) = match tag {
                ColumnTypeTag::Byte => (
                    PrimitiveConvertedType::Int8,
                    PrimitiveLogicalType::Integer(IntegerType::Int8),
                ),
                ColumnTypeTag::Short => (
                    PrimitiveConvertedType::Int16,
                    PrimitiveLogicalType::Integer(IntegerType::Int16),
                ),
                ColumnTypeTag::Char => (
                    PrimitiveConvertedType::Uint16,
                    PrimitiveLogicalType::Integer(IntegerType::UInt16),
                ),
                _ => unreachable!(),
            };
            ParquetType::try_from_primitive(
                "col".to_string(),
                PhysicalType::Int32,
                repetition,
                Some(converted),
                Some(logical),
                Some(id),
            )
            .unwrap()
        }

        let options = WriteOptions {
            write_statistics: true,
            compression: CompressionOptions::Uncompressed,
            version: Version::V1,
            row_group_size: None,
            data_page_size: None,
            raw_array_encoding: false,
            bloom_filter_fpp: DEFAULT_BLOOM_FILTER_FPP,
            min_compression_ratio: 0.0,
        };
        let bloom_filter_columns: HashSet<usize> = HashSet::new();
        let compressions: Vec<Option<CompressionOptions>> = vec![None];

        // Helper: run create_row_group for the given partition+field+encoding
        // under panic-catch (the bug pre-fix manifested as panic, not Err).
        fn run_one(
            partition: &Partition,
            field: &ParquetType,
            encoding: Encoding,
            row_count: usize,
            options: WriteOptions,
            compressions: &[Option<CompressionOptions>],
            bloom_filter_columns: &HashSet<usize>,
        ) -> Result<crate::parquet::error::ParquetResult<()>, Box<dyn std::any::Any + Send>>
        {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                create_row_group(
                    partition,
                    0,
                    row_count,
                    std::slice::from_ref(field),
                    &[encoding],
                    options,
                    compressions,
                    bloom_filter_columns,
                    false,
                )
                .map(|_| ())
            }))
        }

        for &encoding in &[
            Encoding::Plain,
            Encoding::DeltaBinaryPacked,
            Encoding::RleDictionary,
        ] {
            for &repetition in &[Repetition::Required, Repetition::Optional] {
                // Each tag has distinct backing storage; build the Partition
                // inline so the slice borrow lives across the call.
                let byte_data: [i8; 4] = [1, 2, 3, 4];
                let short_data: [i16; 4] = [-1, 0, 1, 2];
                let char_data: [u16; 4] = [b'a' as u16, b'b' as u16, b'c' as u16, b'd' as u16];

                let cases: [(ColumnTypeTag, Partition, usize); 3] = [
                    (
                        ColumnTypeTag::Byte,
                        Partition {
                            table: "test".to_string(),
                            columns: vec![make_column(
                                "col",
                                ColumnTypeTag::Byte.into_type(),
                                &byte_data,
                            )],
                        },
                        byte_data.len(),
                    ),
                    (
                        ColumnTypeTag::Short,
                        Partition {
                            table: "test".to_string(),
                            columns: vec![make_column(
                                "col",
                                ColumnTypeTag::Short.into_type(),
                                &short_data,
                            )],
                        },
                        short_data.len(),
                    ),
                    (
                        ColumnTypeTag::Char,
                        Partition {
                            table: "test".to_string(),
                            columns: vec![make_column(
                                "col",
                                ColumnTypeTag::Char.into_type(),
                                &char_data,
                            )],
                        },
                        char_data.len(),
                    ),
                ];

                for (tag, partition, row_count) in cases {
                    let field = build_field(tag, repetition, 0);
                    let result = run_one(
                        &partition,
                        &field,
                        encoding,
                        row_count,
                        options,
                        &compressions,
                        &bloom_filter_columns,
                    );
                    let inner = result.unwrap_or_else(|_| {
                        panic!(
                            "encoder panicked for tag {:?} encoding {:?} repetition {:?}; \
                             the repetition-aware dispatch in encode.rs must route Required \
                             schemas to encode_int_notnull and Optional to encode_int_nullable",
                            tag, encoding, repetition
                        )
                    });
                    inner.unwrap_or_else(|e| {
                        panic!(
                            "create_row_group failed for tag {:?} encoding {:?} repetition {:?}: {:?}",
                            tag, encoding, repetition, e
                        )
                    });
                }
            }
        }
    }

    /// Round-trip: build a legacy-shaped file with Required BYTE in the schema,
    /// open it via ParquetUpdater in rewrite mode, append a row group, and
    /// verify the resulting file's schema has been migrated to Optional.
    /// Update mode is not covered here because, by design, it preserves the
    /// legacy schema (validated by the success of the dispatch test above).
    #[test]
    fn rewrite_mode_migrates_legacy_required_byte_to_optional() -> Result<(), Box<dyn Error>> {
        use crate::allocator::TestAllocatorState;
        use crate::parquet_write::file::ParquetWriter;
        use crate::parquet_write::schema::to_parquet_schema;
        use parquet2::schema::Repetition;

        let initial = [1i8, 2, 3, 4];
        let initial_col = make_column("b", ColumnTypeTag::Byte.into_type(), &initial);
        let initial_partition = Partition { table: "t".to_string(), columns: vec![initial_col] };

        // Build the modern schema, then patch BYTE -> Required so the written
        // file matches the pre-247cb447cd layout. The repetition-aware
        // dispatch in encode.rs picks encode_int_notnull for this field, so
        // the pages on disk are also in the legacy shape (no def-levels).
        let (modern_schema, additional_meta) =
            to_parquet_schema(&initial_partition, false, -1, SeqTxn::UNSET)?;
        let mut legacy_fields = modern_schema.fields().to_vec();
        for field in legacy_fields.iter_mut() {
            if let parquet2::schema::types::ParquetType::PrimitiveType(ref mut pt) = field {
                pt.field_info.repetition = Repetition::Required;
            }
        }
        let legacy_schema = parquet2::metadata::SchemaDescriptor::new(
            modern_schema.name().to_string(),
            legacy_fields,
        );

        let tmp = NamedTempFile::new()?;
        {
            let writer = tmp.reopen()?;
            let mut chunked = ParquetWriter::new(writer)
                .chunked(legacy_schema.clone(), to_encodings(&initial_partition))?;
            chunked.write_chunk(&initial_partition)?;
            chunked.finish(additional_meta)?;
        }

        // Sanity: the legacy file really did write BYTE as Required.
        {
            let mut reader = tmp.reopen()?;
            let len = reader.metadata()?.len();
            let metadata = read_metadata_with_size(&mut reader, len)?;
            assert_eq!(
                metadata.schema_descr.fields()[0]
                    .get_field_info()
                    .repetition,
                Repetition::Required,
                "legacy file must carry Required BYTE so the migration is exercised"
            );
        }

        // Rewrite-mode updater: write_file_size = 0 routes through the
        // rewrite path; the new file goes to a fresh temp.
        let out_tmp = NamedTempFile::new()?;
        let alloc_state = TestAllocatorState::new();
        let mut updater = super::ParquetUpdater::new(
            alloc_state.allocator(),
            tmp.reopen()?,
            tmp.as_file().metadata()?.len(),
            out_tmp.reopen()?,
            0, // write_file_size = 0 => rewrite mode
            None,
            true,
            false,
            CompressionOptions::Uncompressed,
            None,
            None,
            DEFAULT_BLOOM_FILTER_FPP,
            0.0,
            None,
            0,  // parquet_meta_file_size
            0,  // append_base
            -1, // existing_parquet_file_size
            SeqTxn::UNSET,
        )?;

        let extra = [5i8, 6, 7, 8];
        let extra_col = make_column("b", ColumnTypeTag::Byte.into_type(), &extra);
        let extra_partition = Partition { table: "t".to_string(), columns: vec![extra_col] };
        updater.insert_row_group(&extra_partition, 0)?;
        updater.end(None)?;

        // The rewritten file's schema must be Optional for BYTE, regardless
        // of what the input file declared.
        let mut verify = out_tmp.reopen()?;
        let verify_len = verify.metadata()?.len();
        let new_metadata = read_metadata_with_size(&mut verify, verify_len)?;
        assert_eq!(
            new_metadata.schema_descr.fields()[0]
                .get_field_info()
                .repetition,
            Repetition::Optional,
            "ensure_schema_matches_columns must migrate legacy Required BYTE to Optional in rewrite mode"
        );

        Ok(())
    }

    fn make_column_chunk(
        data_page_offset: i64,
        dictionary_page_offset: Option<i64>,
    ) -> super::ColumnChunk {
        super::ColumnChunk {
            file_path: None,
            file_offset: 0,
            meta_data: Some(super::ColumnMetaData {
                type_: super::Type::INT64,
                encodings: vec![],
                path_in_schema: vec!["c".to_string()],
                codec: super::CompressionCodec::UNCOMPRESSED,
                num_values: 0,
                total_uncompressed_size: 500,
                total_compressed_size: 250,
                key_value_metadata: None,
                data_page_offset,
                index_page_offset: None,
                dictionary_page_offset,
                statistics: None,
                encoding_stats: None,
                bloom_filter_offset: None,
                bloom_filter_length: None,
            }),
            offset_index_offset: None,
            offset_index_length: None,
            column_index_offset: None,
            column_index_length: None,
            crypto_metadata: None,
            encrypted_column_metadata: None,
        }
    }

    #[test]
    fn build_raw_row_group_uses_dict_offset_when_first_column_has_dict() {
        // Regression: build_raw_row_group used to read only data_page_offset
        // for RowGroup.file_offset. With the new (spec-correct) page offsets,
        // a dict-encoded first column has data_page_offset past the dict
        // page; using it would overshoot the row group's start byte by
        // dict_bytes_written.
        let dict_offset: i64 = 1000;
        let data_offset: i64 = 1090;
        let columns = vec![
            make_column_chunk(data_offset, Some(dict_offset)),
            make_column_chunk(2000, None),
        ];

        let rg = super::build_raw_row_group(columns, 10, None).unwrap();
        assert_eq!(
            rg.file_offset,
            Some(dict_offset),
            "row group file_offset must point at the dict page, not the data page"
        );
    }

    #[test]
    fn build_raw_row_group_uses_data_offset_when_first_column_has_no_dict() {
        let data_offset: i64 = 1000;
        let columns = vec![make_column_chunk(data_offset, None)];

        let rg = super::build_raw_row_group(columns, 10, None).unwrap();
        assert_eq!(rg.file_offset, Some(data_offset));
    }

    #[test]
    fn build_raw_row_group_falls_back_to_data_offset_when_dict_offset_is_zero() {
        // calc_row_group_file_offset filters dict_offset > 0; a sentinel 0
        // should be ignored and the data_page_offset used instead.
        let data_offset: i64 = 1000;
        let columns = vec![make_column_chunk(data_offset, Some(0))];

        let rg = super::build_raw_row_group(columns, 10, None).unwrap();
        assert_eq!(rg.file_offset, Some(data_offset));
    }

    #[test]
    fn build_raw_row_group_uses_minimum_physical_column_offset() {
        let columns = vec![
            make_column_chunk(3000, None),
            make_column_chunk(1000, Some(900)),
            make_column_chunk(2000, None),
        ];

        let rg = super::build_raw_row_group(columns, 10, None).unwrap();
        assert_eq!(
            rg.file_offset,
            Some(900),
            "schema order may differ from streamed physical order"
        );
    }

    /// Regression for the no-sorting-columns case (a table with no designated
    /// timestamp): when the file declares no sort order, copy_row_group must
    /// leave the copied group without sorting columns and the freshly written
    /// group must agree, so the footer stays internally consistent and
    /// extract_sorting_columns returns an empty set rather than erroring. The
    /// other copy_row_group tests only cover the WITH-sorting-column path.
    #[test]
    fn copy_row_group_preserves_absent_sorting_columns() -> Result<(), Box<dyn Error>> {
        use crate::allocator::TestAllocatorState;
        use crate::parquet_metadata::convert::extract_sorting_columns;

        // 1. Initial single-row-group file WITHOUT sorting columns.
        let k = [1i32, 2, 3, 4];
        let val = [10i32, 20, 30, 40];
        let partition = Partition {
            table: "t".to_string(),
            columns: vec![
                make_column("k", ColumnTypeTag::Int.into_type(), &k),
                make_column("val", ColumnTypeTag::Int.into_type(), &val),
            ],
        };
        let src = NamedTempFile::new()?;
        ParquetWriter::new(src.reopen()?)
            .with_sorting_columns(None)
            .finish(partition)?;
        let src_len = src.as_file().metadata()?.len();

        // 2. Rewrite-mode updater with NO target sorting columns.
        let out = NamedTempFile::new()?;
        let alloc = TestAllocatorState::new();
        let mut updater = super::ParquetUpdater::new(
            alloc.allocator(),
            src.reopen()?,
            src_len,
            out.reopen()?,
            0,    // write_file_size == 0 -> rewrite
            None, // target sorting columns: none
            true,
            false,
            CompressionOptions::Uncompressed,
            None,
            None,
            DEFAULT_BLOOM_FILTER_FPP,
            0.0,
            None,
            0,
            0,
            -1,
            SeqTxn::UNSET,
        )?;

        // 3. Copy the existing row group, then append a fresh O3 row group.
        let o3_k = [5i32, 6, 7];
        let o3_val = [50i32, 60, 70];
        let o3 = Partition {
            table: "t".to_string(),
            columns: vec![
                make_column("k", ColumnTypeTag::Int.into_type(), &o3_k),
                make_column("val", ColumnTypeTag::Int.into_type(), &o3_val),
            ],
        };
        updater.copy_row_group(0)?; // copied group: must stay without sorting cols
        updater.insert_row_group(&o3, 1)?; // fresh group: also without sorting cols
        updater.end(None)?;

        // 4. Neither row group declares sorting columns, and they agree.
        let f = out.reopen()?;
        let len = f.metadata()?.len();
        let md = read_metadata_with_size(&mut &f, len)?;
        assert_eq!(md.row_groups.len(), 2);
        assert!(
            md.row_groups[0].sorting_columns().is_none(),
            "copied group of a no-sort file must declare no sorting columns",
        );
        assert_eq!(
            md.row_groups[0].sorting_columns(),
            md.row_groups[1].sorting_columns(),
            "copied and appended row groups must agree (both: no sorting columns)",
        );
        // The native call Mig941 makes must accept it and return an empty set.
        let cols = extract_sorting_columns(&md).expect("a file without sorting columns is valid");
        assert!(
            cols.is_empty(),
            "no row group declares sorting columns -> empty result",
        );
        Ok(())
    }

    /// Regression for the O3-merge sorting-columns inconsistency: copy_row_group
    /// must stamp the file-level target sorting columns onto the copied group
    /// rather than leaving it None. The rewrite flow copies the unchanged row
    /// group and then appends a fresh O3 group; before the fix the copied group
    /// declared 0 sorting columns while the fresh group declared the timestamp
    /// sort column, producing a footer the strict _pm validator (Mig941 path)
    /// rejects with "rg 0 has 0 but rg 1 has 1". After the fix every row group
    /// declares identical sorting columns.
    #[test]
    fn copy_row_group_preserves_sorting_columns() -> Result<(), Box<dyn Error>> {
        use crate::allocator::TestAllocatorState;
        use crate::parquet_metadata::convert::extract_sorting_columns;
        use parquet2::metadata::SortingColumn;

        // Designated timestamp at column 0, ascending.
        let sorting = || Some(vec![SortingColumn::new(0, false, false)]);

        // 1. Initial single-row-group file WITH sorting columns -> a freshly
        //    converted parquet partition. The timestamp is designated, so the
        //    encoder records the designated flag in qdb_meta at column 0.
        let ts = [1i64, 2, 3, 4];
        let val = [10i32, 20, 30, 40];
        let partition = Partition {
            table: "t".to_string(),
            columns: vec![
                make_designated_ts_with_id(0, "ts", &ts),
                make_column("val", ColumnTypeTag::Int.into_type(), &val),
            ],
        };
        let src = NamedTempFile::new()?;
        ParquetWriter::new(src.reopen()?)
            .with_sorting_columns(sorting())
            .finish(partition)?;
        let src_len = src.as_file().metadata()?.len();

        // 2. Rewrite-mode updater (write_file_size == 0 -> fresh output file),
        //    the same mode the O3 parquet merge uses.
        let out = NamedTempFile::new()?;
        let alloc = TestAllocatorState::new();
        let mut updater = super::ParquetUpdater::new(
            alloc.allocator(),
            src.reopen()?, // reader: old file
            src_len,
            out.reopen()?, // writer: fresh file
            0,             // write_file_size == 0 -> rewrite
            sorting(),     // target sorting columns
            true,
            false,
            CompressionOptions::Uncompressed,
            None,
            None,
            DEFAULT_BLOOM_FILTER_FPP,
            0.0,
            None,
            0,
            0,
            -1,
            SeqTxn::UNSET,
        )?;

        // 3. Copy the existing row group, then append a fresh O3 row group.
        let o3_ts = [5i64, 6, 7];
        let o3_val = [50i32, 60, 70];
        let o3 = Partition {
            table: "t".to_string(),
            columns: vec![
                make_designated_ts_with_id(0, "ts", &o3_ts),
                make_column("val", ColumnTypeTag::Int.into_type(), &o3_val),
            ],
        };
        updater.copy_row_group(0)?; // copied group: must keep sorting cols
        updater.insert_row_group(&o3, 1)?; // fresh group: written with sorting cols
        updater.end(None)?;

        // 4. Every row group must declare identical sorting columns, so Mig941 /
        //    convert_from_parquet accepts the file, AND the declared index is the
        //    dense designated-timestamp position (0).
        let f = out.reopen()?;
        let len = f.metadata()?.len();
        let md = read_metadata_with_size(&mut &f, len)?;
        assert_eq!(md.row_groups.len(), 2);
        assert_eq!(
            md.row_groups[0].sorting_columns(),
            md.row_groups[1].sorting_columns(),
            "copied and appended row groups must agree on sorting columns",
        );
        assert_eq!(
            md.row_groups[0].sorting_columns(),
            &sorting(),
            "every row group must declare the dense designated-timestamp index (0)",
        );
        // Sanity check: the native call Mig941 makes still accepts the file.
        extract_sorting_columns(&md).expect("sorting columns must be consistent across row groups");
        Ok(())
    }

    /// The ADD/DROP COLUMN variant of copy_row_group_preserves_sorting_columns,
    /// exercising a timestamp-index shift: old schema
    /// `[drop_me(id=5), ts(id=0), val(id=1)]` (ts at index 1) becomes target
    /// `[ts(id=0), val(id=1), added(id=2)]` (ts at index 0). set_target_schema
    /// must re-stamp the dense target index (0), derived from the target schema's
    /// designated flag, on every row group.
    #[test]
    fn copy_row_group_with_null_columns_preserves_sorting_columns() -> Result<(), Box<dyn Error>> {
        use crate::allocator::TestAllocatorState;
        use crate::parquet_metadata::convert::extract_sorting_columns;
        use parquet2::metadata::SortingColumn;

        // 1. Initial file with the OLD schema [drop_me(id=5), ts(id=0),
        //    val(id=1)]. The designated timestamp sits at column index 1, so the
        //    OLD footer's sort column points at index 1.
        let old_sorting = || Some(vec![SortingColumn::new(1, false, false)]);
        let drop_me = [100i32, 200, 300, 400];
        let ts = [1i64, 2, 3, 4];
        let val = [10i32, 20, 30, 40];
        let partition = Partition {
            table: "t".to_string(),
            columns: vec![
                make_column_with_id(5, "drop_me", ColumnTypeTag::Int.into_type(), &drop_me),
                make_designated_ts_with_id(0, "ts", &ts),
                make_column_with_id(1, "val", ColumnTypeTag::Int.into_type(), &val),
            ],
        };
        let src = NamedTempFile::new()?;
        ParquetWriter::new(src.reopen()?)
            .with_sorting_columns(old_sorting())
            .finish(partition)?;
        let src_len = src.as_file().metadata()?.len();

        // Rewrite-mode updater. The constructor already reduces the caller's index
        // to the source's designated position (1) via designated_ts_sorting_columns;
        // the test's job is to prove set_target_schema then recomputes it to the
        // dense target position (0) rather than keeping the source's 1.
        let target_sorting = || Some(vec![SortingColumn::new(0, false, false)]);
        let out = NamedTempFile::new()?;
        let alloc = TestAllocatorState::new();
        let mut updater = super::ParquetUpdater::new(
            alloc.allocator(),
            src.reopen()?,
            src_len,
            out.reopen()?,
            0, // rewrite
            Some(vec![SortingColumn::new(7, false, false)]),
            true,
            false,
            CompressionOptions::Uncompressed,
            None,
            None,
            DEFAULT_BLOOM_FILTER_FPP,
            0.0,
            None,
            0,
            0,
            -1,
            SeqTxn::UNSET,
        )?;

        // 3. Target schema drops the leading column and adds a trailing one:
        //    [ts(id=0), val(id=1), added(id=2)]. This shifts the timestamp from
        //    old index 1 to target index 0.
        let added = ColumnTypeTag::Int.into_type();
        let target = Partition {
            table: "t".to_string(),
            columns: vec![
                make_designated_ts_with_id(0, "ts", &ts),
                make_column_with_id(1, "val", ColumnTypeTag::Int.into_type(), &val),
                make_column_with_id(2, "added", added, &val),
            ],
        };
        updater.set_target_schema(&target)?;

        // 4. Copy the old row group: drop_me (id=5) is dropped, ts and val are
        //    remapped, and added (target pos 2) is backfilled with nulls. Then
        //    append a fresh O3 group in the new 3-column schema.
        updater.copy_row_group_with_null_columns(0, &[(2, added)])?;
        let o3_ts = [5i64, 6, 7];
        let o3_val = [50i32, 60, 70];
        let o3_added = [1i32, 2, 3];
        let o3 = Partition {
            table: "t".to_string(),
            columns: vec![
                make_designated_ts_with_id(0, "ts", &o3_ts),
                make_column_with_id(1, "val", ColumnTypeTag::Int.into_type(), &o3_val),
                make_column_with_id(2, "added", added, &o3_added),
            ],
        };
        updater.insert_row_group(&o3, 1)?;
        updater.end(None)?;

        // Both row groups must declare the dense TARGET timestamp position (0),
        // proving set_target_schema re-derived it rather than keeping the source's 1.
        let f = out.reopen()?;
        let len = f.metadata()?.len();
        let md = read_metadata_with_size(&mut &f, len)?;
        assert_eq!(md.row_groups.len(), 2);
        assert_eq!(
            md.row_groups[0].sorting_columns(),
            md.row_groups[1].sorting_columns(),
            "copied (with null column) and appended row groups must agree on sorting columns",
        );
        assert_eq!(
            md.row_groups[0].sorting_columns(),
            &target_sorting(),
            "copied group must stamp the dense TARGET sort index (0), not the source's 1",
        );
        extract_sorting_columns(&md).expect("sorting columns must be consistent across row groups");
        Ok(())
    }

    /// Regression for the stale raw-slot bug in the no-schema-change rewrite
    /// path. A leading column was dropped BEFORE the partition was converted to
    /// parquet, so the file is already dense `[ts(0), val(1)]` with its footer
    /// correctly sorted on column 0. The table's raw timestamp slot, however, is
    /// still 1 (the dropped column keeps a tombstone), and the caller passes
    /// that stale slot into the updater. With no schema change set_target_schema
    /// is never called, so the construction-time derivation is the only line of
    /// defense: it must ignore the stale slot and re-derive the dense index (0)
    /// from the source qdb_meta's designated flag. Before the fix every row
    /// group would have been stamped with the out-of-range index 1.
    #[test]
    fn rewrite_ignores_stale_raw_timestamp_slot() -> Result<(), Box<dyn Error>> {
        use crate::allocator::TestAllocatorState;
        use crate::parquet_metadata::convert::extract_sorting_columns;
        use parquet2::metadata::SortingColumn;

        // 1. Already-dense file [ts(id=0), val(id=1)]: the designated timestamp
        //    is at column 0, and the encoder writes the footer sort column at 0.
        let ts = [1i64, 2, 3, 4];
        let val = [10i32, 20, 30, 40];
        let partition = Partition {
            table: "t".to_string(),
            columns: vec![
                make_designated_ts_with_id(0, "ts", &ts),
                make_column_with_id(1, "val", ColumnTypeTag::Int.into_type(), &val),
            ],
        };
        let src = NamedTempFile::new()?;
        ParquetWriter::new(src.reopen()?)
            .with_sorting_columns(Some(vec![SortingColumn::new(0, false, false)]))
            .finish(partition)?;
        let src_len = src.as_file().metadata()?.len();

        // 2. Rewrite-mode updater fed the STALE raw timestamp slot (1), as
        //    O3PartitionJob would after a pre-conversion DROP COLUMN left a
        //    tombstone in the raw layout. No set_target_schema call follows.
        let out = NamedTempFile::new()?;
        let alloc = TestAllocatorState::new();
        let mut updater = super::ParquetUpdater::new(
            alloc.allocator(),
            src.reopen()?,
            src_len,
            out.reopen()?,
            0, // rewrite
            Some(vec![SortingColumn::new(1, false, false)]),
            true,
            false,
            CompressionOptions::Uncompressed,
            None,
            None,
            DEFAULT_BLOOM_FILTER_FPP,
            0.0,
            None,
            0,
            0,
            -1,
            SeqTxn::UNSET,
        )?;

        // 3. Copy the existing row group and append a fresh O3 group, same schema.
        let o3_ts = [5i64, 6, 7];
        let o3_val = [50i32, 60, 70];
        let o3 = Partition {
            table: "t".to_string(),
            columns: vec![
                make_designated_ts_with_id(0, "ts", &o3_ts),
                make_column_with_id(1, "val", ColumnTypeTag::Int.into_type(), &o3_val),
            ],
        };
        updater.copy_row_group(0)?;
        updater.insert_row_group(&o3, 1)?;
        updater.end(None)?;

        // 4. Every row group must declare the dense designated-timestamp index
        //    (0), NOT the stale raw slot (1) the caller passed in.
        let f = out.reopen()?;
        let len = f.metadata()?.len();
        let md = read_metadata_with_size(&mut &f, len)?;
        assert_eq!(md.row_groups.len(), 2);
        assert_eq!(
            md.row_groups[0].sorting_columns(),
            md.row_groups[1].sorting_columns(),
            "copied and appended row groups must agree on sorting columns",
        );
        assert_eq!(
            md.row_groups[0].sorting_columns(),
            &Some(vec![SortingColumn::new(0, false, false)]),
            "rewrite must declare the dense designated-timestamp index (0), not the stale raw slot (1)",
        );
        extract_sorting_columns(&md).expect("sorting columns must be consistent across row groups");
        Ok(())
    }

    /// regression through a real update-mode merge (the rewrite-mode tests
    /// above don't cover it). The cached legacy group keeps its stale footer index
    /// (1) while the appended group gets the corrected dense index (0), so the
    /// footer conflicts -- yet the migration must tolerate it via qdb_meta.
    #[test]
    fn update_mode_append_tolerated_by_migration_via_qdb_meta() -> Result<(), Box<dyn Error>> {
        use crate::allocator::TestAllocatorState;
        use crate::parquet::qdb_metadata::{QdbMeta, QDB_META_KEY};
        use crate::parquet_metadata::convert::{convert_from_parquet, extract_sorting_columns};
        use crate::parquet_metadata::reader::ParquetMetaReader;
        use parquet2::metadata::SortingColumn;

        // Legacy file: qdb_meta designates ts at dense column 0, but the footer's
        // row group carries the stale raw-slot index 1 (as a pre-fix binary wrote).
        let ts = [1i64, 2, 3, 4];
        let val = [10i32, 20, 30, 40];
        let partition = Partition {
            table: "t".to_string(),
            columns: vec![
                make_designated_ts_with_id(0, "ts", &ts),
                make_column_with_id(1, "val", ColumnTypeTag::Int.into_type(), &val),
            ],
        };
        let tmp = NamedTempFile::new()?;
        ParquetWriter::new(tmp.reopen()?)
            .with_sorting_columns(Some(vec![SortingColumn::new(1, false, false)]))
            .finish(partition)?;
        let file_len = tmp.as_file().metadata()?.len();

        // In-place update mode (write_file_size == file_len). The caller passes the
        // stale slot 1; the constructor re-derives the dense index 0 from qdb_meta.
        let alloc = TestAllocatorState::new();
        let mut updater = super::ParquetUpdater::new(
            alloc.allocator(),
            tmp.reopen()?,
            file_len,
            tmp.reopen()?,
            file_len, // update (append) mode
            Some(vec![SortingColumn::new(1, false, false)]),
            true,
            false,
            CompressionOptions::Uncompressed,
            None,
            None,
            DEFAULT_BLOOM_FILTER_FPP,
            0.0,
            None,
            0,
            0,
            -1,
            SeqTxn::UNSET,
        )?;

        // Append a fresh O3 row group, then finish.
        let o3_ts = [5i64, 6, 7];
        let o3_val = [50i32, 60, 70];
        let o3 = Partition {
            table: "t".to_string(),
            columns: vec![
                make_designated_ts_with_id(0, "ts", &o3_ts),
                make_column_with_id(1, "val", ColumnTypeTag::Int.into_type(), &o3_val),
            ],
        };
        updater.insert_row_group(&o3, 1)?;
        updater.end(None)?;

        // The footer now conflicts: cached group keeps 1, appended group carries 0.
        let f = tmp.reopen()?;
        let len = f.metadata()?.len();
        let md = read_metadata_with_size(&mut &f, len)?;
        assert_eq!(md.row_groups.len(), 2);
        assert_eq!(
            md.row_groups[0].sorting_columns(),
            &Some(vec![SortingColumn::new(1, false, false)]),
        );
        assert_eq!(
            md.row_groups[1].sorting_columns(),
            &Some(vec![SortingColumn::new(0, false, false)]),
        );
        assert!(extract_sorting_columns(&md).is_err());

        // The migration must still succeed via qdb_meta, recording dense position 0.
        let qdb_raw = md
            .key_value_metadata
            .as_ref()
            .and_then(|kvs| kvs.iter().find(|kv| kv.key == QDB_META_KEY))
            .and_then(|kv| kv.value.as_ref())
            .expect("updated file must carry qdb_meta");
        let qdb_meta = QdbMeta::deserialize(qdb_raw)?;
        let (pm_bytes, pm_size) =
            convert_from_parquet(&md, Some(&qdb_meta), 0, 0, &NoBloomFilterSource, None)
                .expect("migration must tolerate the update-mode footer via qdb_meta");
        let reader = ParquetMetaReader::from_file_size(&pm_bytes, pm_size).unwrap();
        assert_eq!(reader.designated_timestamp(), Some(0));
        assert_eq!(reader.sorting_column_count(), 1);
        assert_eq!(reader.sorting_column(0).unwrap(), 0);
        Ok(())
    }

    /// Rewrite copying TWO source row groups. Fresh groups of differing sizes
    /// precede each copy, so the copied groups land at distinct, non-zero
    /// offset_deltas -- exercising the per-group page-index rebase that a
    /// single-group copy can't. assert_fully_indexed verifies each copied
    /// group's OffsetIndex points at its own rebased data page.
    #[test]
    fn rewrite_copies_multiple_row_groups_with_distinct_offset_deltas() -> Result<(), Box<dyn Error>>
    {
        use crate::allocator::TestAllocatorState;
        use parquet2::metadata::SortingColumn;
        use std::io::Read as _;

        let sorting = || Some(vec![SortingColumn::new(0, false, false)]);

        // Source with two row groups (row_group_size 2 over 4 rows).
        let src = NamedTempFile::new()?;
        ParquetWriter::new(src.reopen()?)
            .with_sorting_columns(sorting())
            .with_row_group_size(Some(2))
            .finish(ts_val_partition(&[10, 20, 30, 40], &[100, 200, 300, 400]))?;
        let src_len = src.as_file().metadata()?.len();
        {
            // Sanity: the source really has two groups to copy.
            let md = read_metadata_with_size(&mut src.reopen()?, src_len)?;
            assert_eq!(md.row_groups.len(), 2);
        }

        let out = NamedTempFile::new()?;
        let alloc = TestAllocatorState::new();
        let mut updater = super::ParquetUpdater::new(
            alloc.allocator(),
            src.reopen()?,
            src_len,
            out.reopen()?,
            0, // rewrite
            sorting(),
            true,
            false,
            CompressionOptions::Uncompressed,
            None,
            None,
            DEFAULT_BLOOM_FILTER_FPP,
            0.0,
            None,
            0,
            0,
            -1,
            SeqTxn::UNSET,
        )?;

        // Physical output order (rewrite ignores the insert position):
        //   freshA | copied rg0 | freshB | copied rg1
        // freshA has 3 rows vs rg0's 2, so the two copied groups land at
        // different shifts -> distinct, non-zero offset_delta per copied group.
        updater.insert_row_group(&ts_val_partition(&[1, 2, 3], &[1, 2, 3]), 0)?;
        updater.copy_row_group(0)?;
        updater.insert_row_group(&ts_val_partition(&[25, 26], &[250, 260]), 2)?;
        updater.copy_row_group(1)?;
        updater.end(None)?;

        let mut bytes = Vec::new();
        out.reopen()?.read_to_end(&mut bytes)?;

        assert_fully_indexed(&bytes, &[(1, 3), (10, 20), (25, 26), (30, 40)]);
        assert_eq!(
            read_val_column(&bytes),
            vec![
                Some(1),
                Some(2),
                Some(3),
                Some(100),
                Some(200),
                Some(250),
                Some(260),
                Some(300),
                Some(400),
            ],
        );
        Ok(())
    }

    /// The rewrite _pm generation path resolves column ids via
    /// build_column_infos_from_qdb_meta -> resolve_column_id. Every other
    /// update.rs test passes None for parquet_meta_fd, so this drives a real
    /// _pm file and asserts the resolved (QuestDB) ids land in the metadata.
    #[test]
    fn rewrite_generates_pm_with_resolved_column_ids() -> Result<(), Box<dyn Error>> {
        use crate::allocator::TestAllocatorState;
        use crate::parquet_metadata::reader::ParquetMetaReader;
        use parquet2::metadata::SortingColumn;
        use std::io::Read as _;

        // Source columns carry explicit, distinct QuestDB ids (10, 20). The
        // writer embeds them in qdb_meta; resolve_column_id must surface them.
        let ts = [1i64, 2, 3, 4];
        let val = [10i32, 20, 30, 40];
        let partition = Partition {
            table: "t".to_string(),
            columns: vec![
                make_designated_ts_with_id(10, "ts", &ts),
                make_column_with_id(20, "val", ColumnTypeTag::Int.into_type(), &val),
            ],
        };
        let src = NamedTempFile::new()?;
        ParquetWriter::new(src.reopen()?)
            .with_sorting_columns(Some(vec![SortingColumn::new(0, false, false)]))
            .finish(partition)?;
        let src_len = src.as_file().metadata()?.len();

        let out = NamedTempFile::new()?;
        let pm = NamedTempFile::new()?;
        let alloc = TestAllocatorState::new();
        let mut updater = super::ParquetUpdater::new(
            alloc.allocator(),
            src.reopen()?,
            src_len,
            out.reopen()?,
            0, // rewrite
            Some(vec![SortingColumn::new(0, false, false)]),
            true,
            false,
            CompressionOptions::Uncompressed,
            None,
            None,
            DEFAULT_BLOOM_FILTER_FPP,
            0.0,
            Some(pm.reopen()?), // real _pm fd exercises the id-resolution path
            0,
            0,
            -1, // no existing _pm -> full-create branch
            SeqTxn::UNSET,
        )?;
        updater.copy_row_group(0)?;
        updater.end(None)?;

        let pm_size = updater.result_parquet_meta_size();
        assert!(pm_size > 0, "rewrite must produce a _pm file");

        let mut pm_bytes = Vec::new();
        pm.reopen()?.read_to_end(&mut pm_bytes)?;
        let reader = ParquetMetaReader::from_file_size(&pm_bytes, pm_size as u64).unwrap();
        assert_eq!(reader.column_count(), 2);
        assert_eq!(reader.column_descriptor(0).unwrap().id, 10);
        assert_eq!(reader.column_descriptor(1).unwrap().id, 20);
        Ok(())
    }

    /// Regression: after a DROP COLUMN shifts the surviving columns,
    /// copy_row_group_with_null_columns must inline each survivor's OWN bloom
    /// filter into the _pm sidecar, not its source-order predecessor's. The
    /// bitsets are extracted in source order but the columns are rewritten in
    /// target order; without the remap the _pm writer's positional indexing
    /// attached column a's bloom to b, b's to c, and c's to the bloom-less
    /// timestamp, so an equality filter probed the wrong column's bloom and
    /// could silently skip a matching row group. Source [a,b,c,ts] with blooms
    /// on a/b/c, drop a, then assert the _pm carries b's and c's own blooms at
    /// their new positions and no bloom on ts.
    #[test]
    fn copy_row_group_with_null_columns_remaps_bloom_to_target_order() -> Result<(), Box<dyn Error>>
    {
        use crate::allocator::TestAllocatorState;
        use crate::parquet_metadata::reader::ParquetMetaReader;
        use parquet2::metadata::SortingColumn;
        use std::io::Read as _;

        // Distinct value ranges make the three blooms distinct, so a
        // misattributed bloom is caught by an exact byte comparison.
        let a: Vec<i32> = (1000..1016).collect();
        let b: Vec<i32> = (2000..2016).collect();
        let c: Vec<i32> = (3000..3016).collect();
        let ts: Vec<i64> = (1..17).collect();
        let src_partition = Partition {
            table: "t".to_string(),
            columns: vec![
                make_column_with_id(10, "a", ColumnTypeTag::Int.into_type(), &a),
                make_column_with_id(11, "b", ColumnTypeTag::Int.into_type(), &b),
                make_column_with_id(12, "c", ColumnTypeTag::Int.into_type(), &c),
                make_designated_ts_with_id(0, "ts", &ts),
            ],
        };
        let mut bloom_cols = HashSet::new();
        bloom_cols.insert(0);
        bloom_cols.insert(1);
        bloom_cols.insert(2);
        let src = NamedTempFile::new()?;
        ParquetWriter::new(src.reopen()?)
            .with_statistics(true)
            .with_sorting_columns(Some(vec![SortingColumn::new(3, false, false)]))
            .with_bloom_filter_columns(bloom_cols)
            .finish(src_partition)?;
        let src_len = src.as_file().metadata()?.len();

        // The raw copy preserves each column's bloom bytes verbatim, so the
        // source data-file blooms are the expected _pm blooms for the survivors.
        let mut src_bytes = Vec::new();
        src.reopen()?.read_to_end(&mut src_bytes)?;
        let src_md =
            read_metadata_with_size(&mut Cursor::new(&src_bytes[..]), src_bytes.len() as u64)?;
        let src_rg = &src_md.row_groups[0];
        assert!(
            src_rg.columns()[3].metadata().bloom_filter_offset.is_none(),
            "ts must carry no source bloom"
        );
        let bloom_a =
            parquet2::bloom_filter::read_from_slice(&src_rg.columns()[0], &src_bytes)?.to_vec();
        let bloom_b =
            parquet2::bloom_filter::read_from_slice(&src_rg.columns()[1], &src_bytes)?.to_vec();
        let bloom_c =
            parquet2::bloom_filter::read_from_slice(&src_rg.columns()[2], &src_bytes)?.to_vec();
        assert_ne!(
            bloom_a, bloom_b,
            "distinct values must yield distinct blooms"
        );
        assert_ne!(
            bloom_b, bloom_c,
            "distinct values must yield distinct blooms"
        );

        // Rewrite dropping `a`: survivors b, c, ts each shift down one position.
        let out = NamedTempFile::new()?;
        let pm = NamedTempFile::new()?;
        let alloc = TestAllocatorState::new();
        let mut updater = super::ParquetUpdater::new(
            alloc.allocator(),
            src.reopen()?,
            src_len,
            out.reopen()?,
            0, // rewrite
            Some(vec![SortingColumn::new(3, false, false)]),
            true,
            false,
            CompressionOptions::Uncompressed,
            None,
            None,
            DEFAULT_BLOOM_FILTER_FPP,
            0.0,
            Some(pm.reopen()?),
            0,
            0,
            -1,
            SeqTxn::UNSET,
        )?;
        let target = Partition {
            table: "t".to_string(),
            columns: vec![
                make_column_with_id(11, "b", ColumnTypeTag::Int.into_type(), &b),
                make_column_with_id(12, "c", ColumnTypeTag::Int.into_type(), &c),
                make_designated_ts_with_id(0, "ts", &ts),
            ],
        };
        updater.set_target_schema(&target)?;
        updater.copy_row_group_with_null_columns(0, &[])?;
        updater.end(None)?;

        // Post-fix only b (target 0) and c (target 1) carry blooms; pre-fix the
        // shift also inlined a stray bloom onto ts (target 2).
        let pm_size = updater.result_parquet_meta_size();
        let mut pm_bytes = Vec::new();
        pm.reopen()?.read_to_end(&mut pm_bytes)?;
        let reader = ParquetMetaReader::from_file_size(&pm_bytes, pm_size as u64).unwrap();
        assert_eq!(
            reader.bloom_filter_columns(),
            vec![0, 1],
            "only the two surviving bloomed columns may carry a _pm bloom"
        );
        assert_eq!(
            reader.bloom_filter_position(2),
            None,
            "ts must carry no bloom"
        );

        let read_inlined = |col_idx: u32| -> Vec<u8> {
            let pos = reader
                .bloom_filter_position(col_idx)
                .expect("surviving column has a _pm bloom");
            let off = reader
                .bloom_filter_offset_in_pm(0, pos)
                .expect("bloom offset") as usize;
            let len = i32::from_le_bytes(pm_bytes[off..off + 4].try_into().unwrap()) as usize;
            pm_bytes[off + 4..off + 4 + len].to_vec()
        };

        // b moved 1 -> 0 and c moved 2 -> 1; each must carry its own source
        // bloom, not the source-order neighbor the pre-fix code attached.
        assert_eq!(
            read_inlined(0),
            bloom_b,
            "target col 0 must carry b's bloom"
        );
        assert_ne!(
            read_inlined(0),
            bloom_a,
            "target col 0 must not carry dropped a's bloom"
        );
        assert_eq!(
            read_inlined(1),
            bloom_c,
            "target col 1 must carry c's bloom"
        );
        assert_ne!(
            read_inlined(1),
            bloom_b,
            "target col 1 must not carry b's bloom"
        );
        Ok(())
    }
}
