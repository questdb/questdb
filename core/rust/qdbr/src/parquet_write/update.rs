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
use std::collections::HashSet;

use rapidhash::RapidHashMap;
use std::fs::File;
use std::io::{Read as _, Seek, SeekFrom};

use parquet2::compression::CompressionOptions;
use parquet2::encoding::uleb128;
use parquet2::metadata::{FileMetaData, KeyValue, SchemaDescriptor, SortingColumn};
use parquet2::read::{read_metadata_with_footer_bytes, read_metadata_with_size};
use parquet2::schema::types::ParquetType;
use parquet2::schema::Repetition;
use parquet2::write;
use parquet2::write::footer_cache::FooterCache;
use parquet2::write::{ParquetFile, Version};
use parquet_format_safe::thrift::protocol::TCompactOutputProtocol;
use parquet_format_safe::{
    ColumnChunk, ColumnMetaData, CompressionCodec, DataPageHeader, DictionaryPageHeader,
    Encoding as ThriftEncoding, PageHeader, PageType, RowGroup, Type,
};
use qdb_core::col_type::{ColumnType, ColumnTypeTag};

use crate::allocator::QdbAllocator;
use crate::parquet::error::{
    fmt_err, ParquetError, ParquetErrorExt, ParquetErrorReason, ParquetResult,
};
use crate::parquet::qdb_metadata::{QdbMeta, QdbMetaCol, QdbMetaColFormat, QDB_META_KEY};
use crate::parquet_write::file::{create_row_group, WriteOptions};
use crate::parquet_write::schema::{to_compressions, to_encodings, to_parquet_schema, Partition};

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
    /// Per-column ASCII flag from the old file's QDB metadata, read at construction.
    /// Used to skip `is_column_ascii()` scans when the old flag is already `false`.
    old_ascii: Vec<Option<bool>>,
    /// Per-column ASCII flag computed from written (inserted/replaced) row groups.
    /// `Some(true)` = all written values are ASCII, `Some(false)` = at least one
    /// non-ASCII value, `None` = column not written (e.g., non-VARCHAR or no row
    /// groups inserted/replaced).
    written_ascii: Vec<Option<bool>>,
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
        match qdb_meta {
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
            }
        }

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
        let options = write::WriteOptions { write_statistics, version, bloom_filter_fpp };

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

        let old_ascii = file_metadata
            .key_value_metadata
            .as_ref()
            .and_then(|kvs| {
                kvs.iter()
                    .find(|kv| kv.key == QDB_META_KEY)
                    .and_then(|kv| kv.value.as_ref())
                    .and_then(|v| QdbMeta::deserialize(v).ok())
            })
            .map(|m| m.schema.iter().map(|c| c.ascii).collect::<Vec<_>>())
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
            written_ascii: Vec::new(),
        })
    }

    /// Computes the per-column ASCII flag from the partition's VARCHAR aux data
    /// and ANDs it into `written_ascii`. Called for each inserted/replaced row group.
    ///
    /// Skips the aux scan when the result is already determined to be `false`
    /// (from a previous row group or from the old file's metadata in update mode).
    fn track_ascii(&mut self, partition: &Partition) {
        let n = partition.columns.len();
        if self.written_ascii.len() < n {
            self.written_ascii.resize(n, None);
        }
        for (i, col) in partition.columns.iter().enumerate() {
            if col.data_type.tag() != ColumnTypeTag::Varchar || col.secondary_data.is_empty() {
                continue;
            }
            // Already determined non-ASCII from a previous row group — skip scan.
            if self.written_ascii[i] == Some(false) {
                continue;
            }
            // In update mode, if the old file's flag is already false the final
            // result (old AND written) will be false regardless — skip scan.
            if !self.is_rewrite {
                if let Some(Some(false)) = self.old_ascii.get(i) {
                    self.written_ascii[i] = Some(false);
                    continue;
                }
            }
            // SAFETY: secondary_data contains native VARCHAR aux entries (16 bytes each).
            let aux: &[[u8; 16]] =
                unsafe { super::util::transmute_slice(col.secondary_data) };
            let is_ascii = super::varchar::is_column_ascii(aux);
            self.written_ascii[i] = Some(match self.written_ascii[i] {
                Some(prev) => prev && is_ascii,
                None => is_ascii,
            });
        }
    }

    pub fn replace_row_group(
        &mut self,
        partition: &Partition,
        row_group_id: i32,
    ) -> ParquetResult<()> {
        self.track_ascii(partition);
        self.ensure_schema_matches_columns(partition)?;
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

        // Ensure the PAR1 file header is written before computing offsets.
        // Without this, current_offset() returns 0, but write_raw_row_group()
        // would then write the 4-byte PAR1 header first, making the actual data
        // start at offset 4 while metadata offsets point to 0.
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
        for col in &mut columns {
            adjust_column_chunk_offsets(col, offset_delta);
        }

        let thrift_rg = build_raw_row_group(columns, row_count);

        self.parquet_file
            .write_raw_row_group(&self.copy_buffer, thrift_rg)
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
        let (schema, _kv) = to_parquet_schema(partition, self.raw_array_encoding, -1)?;
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

            let column_type = if col.designated_timestamp {
                col.data_type
                    .into_designated_with_order(col.designated_timestamp_ascending)?
            } else {
                col.data_type
            };

            qdb_meta
                .schema
                .push(QdbMetaCol { column_type, column_top: 0, format, ascii: None });
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

        // Merge existing and null column chunks in target schema order.
        let target_col_count = target_fields.len();
        let mut merged_cols: Vec<Option<ColumnChunk>> = vec![None; target_col_count];

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

        let thrift_rg = build_raw_row_group(columns, row_count);

        // Concatenate existing + null bytes and write as one raw row group.
        self.copy_buffer.extend_from_slice(&null_bytes_buf);

        self.parquet_file
            .write_raw_row_group(&self.copy_buffer, thrift_rg)
            .map_err(|s| {
                ParquetError::with_descr(
                    ParquetErrorReason::Parquet2(s),
                    format!("Failed to raw-copy row group {rg_idx} with null columns"),
                )
            })
    }

    pub fn end(&mut self, key_value_metadata: Option<Vec<KeyValue>>) -> ParquetResult<u64> {
        // Build updated QDB metadata with unused_bytes and pass it as KV metadata.
        // When a target schema was set (ADD/DROP COLUMN), use the pre-built
        // target_qdb_meta which already has the correct column list with
        // column_top=0. Otherwise fall back to the old file's QDB metadata.
        let mut qdb_meta = if let Some(mut target) = self.target_qdb_meta.take() {
            // Apply tracked ASCII flags from written row groups.
            for (i, col) in target.schema.iter_mut().enumerate() {
                if let Some(&Some(written)) = self.written_ascii.get(i) {
                    col.ascii = Some(written);
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
            for (i, col) in meta.schema.iter_mut().enumerate() {
                col.column_top = 0; // Update the ASCII flag from the actual data written in
                // inserted/replaced row groups. For a full rewrite, the
                // written_ascii value is the final answer. For an in-place
                // update, AND the old flag (covering copied row groups) with
                // the written flag (covering new row groups).
                if let Some(&Some(written)) = self.written_ascii.get(i) {
                    if self.is_rewrite {
                        col.ascii = Some(written);
                    } else {
                        col.ascii = col.ascii.map(|old| old && written);
                    }
                }
            }
            meta
        };

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
        Ok(file_size)
    }

    pub fn result_unused_bytes(&self) -> u64 {
        self.result_unused_bytes
    }

    /// Updates the file-level schema when any column's not_null_hint flag disagrees
    /// with the schema's repetition. This happens when an O3 merge introduces
    /// null values into a symbol column that was previously all-non-null
    /// (Required in the schema). Only safe in rewrite mode where all row groups
    /// are re-encoded; in update mode untouched row groups would have data
    /// encoded with the old schema.
    /// Handles a legacy edge case during rewrite: old parquet files may have
    /// Symbol columns marked as Required in the schema (written before the
    /// convention was established that symbols are always Optional). If the
    /// current data now contains nulls (`!col.not_null_hint`), the schema must be
    /// downgraded to Optional so the rewritten pages include definition levels.
    ///
    /// New files always write symbols as Optional (see `column_type_to_parquet_type`
    /// in schema.rs). The `Column::not_null_hint` flag is only a write-time hint for
    /// the encoder to emit a fast all-ones RLE run for definition levels.
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
        let needs_update = partition
            .columns
            .iter()
            .zip(fields.iter())
            .any(|(col, field)| {
                col.data_type.tag() == ColumnTypeTag::Symbol
                    && !col.not_null_hint
                    && field.get_field_info().repetition == Repetition::Required
            });
        if !needs_update {
            return Ok(());
        }
        let mut new_fields: Vec<ParquetType> = fields.to_vec();
        for (col, field) in partition.columns.iter().zip(new_fields.iter_mut()) {
            if col.data_type.tag() == ColumnTypeTag::Symbol && !col.not_null_hint {
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

/// Builds a thrift `RowGroup` from a list of `ColumnChunk`s, computing
/// `file_offset`, `total_byte_size`, and `total_compressed_size` from the
/// column metadata.
fn build_raw_row_group(columns: Vec<ColumnChunk>, num_rows: usize) -> RowGroup {
    let total_byte_size: i64 = columns
        .iter()
        .filter_map(|c| c.meta_data.as_ref())
        .map(|m| m.total_uncompressed_size)
        .sum();
    let total_compressed_size: i64 = columns
        .iter()
        .filter_map(|c| c.meta_data.as_ref())
        .map(|m| m.total_compressed_size)
        .sum();
    let file_offset = columns
        .first()
        .and_then(|c| c.meta_data.as_ref())
        .map(|m| m.data_page_offset);

    RowGroup {
        columns,
        total_byte_size,
        num_rows: num_rows as i64,
        sorting_columns: None,
        file_offset,
        total_compressed_size: Some(total_compressed_size),
        ordinal: None,
    }
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
        encoding_stats: None,
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

#[cfg(test)]
mod tests {
    use crate::parquet::tests::ColumnTypeTagExt;
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet2::compression::CompressionOptions;
    use parquet2::write::{ParquetFile, Version};
    use std::collections::HashSet;
    use std::env;
    use std::error::Error;
    use std::fs::File;
    use std::io::Cursor;
    use std::io::Write;
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

    fn save_to_file(bytes: &Bytes) {
        if let Ok(path) = env::var("OUT_PARQUET_FILE") {
            let mut file = File::create(path).expect("file create failed");
            file.write_all(bytes.to_byte_slice())
                .expect("file write failed");
        };
    }

    fn make_column<T>(name: &'static str, col_type: ColumnType, values: &[T]) -> Column {
        Column::from_raw_data(
            0,
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
            column_structure_version: -1,
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
            column_structure_version: -1,
        };

        let orig_offset = buf.position();
        let metadata = read_metadata_with_size(&mut buf, orig_offset)?;

        let (schema, _) = to_parquet_schema(&new_partition, false, -1)?;

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
                0.5, // min_compression_ratio: ratio check active but easily met
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

    /// After copy_row_group with a non-zero offset shift, the bloom filter
    /// on the last column must still be readable and contain the original
    /// values. This requires either copying the bloom bytes into the new
    /// file or clearing the offset to None.
    #[test]
    fn copy_row_group_preserves_last_col_bloom_filter() -> Result<(), Box<dyn Error>> {
        use std::io::{Read as _, Seek, SeekFrom};

        // Create a parquet file with 2 row groups.
        // Bloom filter on col1 (index 1) — the LAST column.
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
            column_structure_version: -1,
        };
        let partition_rg1 = Partition {
            table: "test_table".to_string(),
            columns: vec![
                make_column("col0", ColumnTypeTag::Int.into_type(), &col0_rg1),
                make_column("col1", ColumnTypeTag::Float.into_type(), &col1_rg1),
            ],
            column_structure_version: -1,
        };

        let (schema, _) =
            crate::parquet_write::schema::to_parquet_schema(&partition_rg0, false, -1)?;
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
            });
        qdb_meta
            .schema
            .push(crate::parquet::qdb_metadata::QdbMetaCol {
                column_type: ColumnTypeTag::Float.into_type(),
                column_top: 0,
                format: None,
                ascii: None,
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
            column_structure_version: -1,
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
}
