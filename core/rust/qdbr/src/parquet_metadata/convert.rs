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

//! Conversion from parquet2 `FileMetaData` (+ optional `QdbMeta`) to `_pm` format.

use crate::parquet::error::{parquet_meta_err, ParquetResult};
use crate::parquet::qdb_metadata::{QdbMeta, QdbMetaColFormat};
use crate::parquet_metadata::column_chunk::ColumnChunkRaw;
use crate::parquet_metadata::error::ParquetMetaErrorKind;
use crate::parquet_metadata::row_group::RowGroupBlockBuilder;
use crate::parquet_metadata::types::{
    encode_stat_sizes, Codec, ColumnFlags, EncodingMask, FieldRepetition, StatFlags,
};
use crate::parquet_metadata::writer::ParquetMetaWriter;
use crate::parquet_read::meta::resolve_column_id;
use parquet2::metadata::FileMetaData;
use parquet2::metadata::SortingColumn;
use parquet2::schema::types::{PhysicalType, PrimitiveLogicalType};
use qdb_core::col_type::ColumnTypeTag;

/// Maps a parquet2 `PhysicalType` enum to its ordinal `u8` encoding.
pub fn physical_type_to_u8(pt: PhysicalType) -> u8 {
    match pt {
        PhysicalType::Boolean => 0,
        PhysicalType::Int32 => 1,
        PhysicalType::Int64 => 2,
        PhysicalType::Int96 => 3,
        PhysicalType::Float => 4,
        PhysicalType::Double => 5,
        PhysicalType::ByteArray => 6,
        PhysicalType::FixedLenByteArray(_) => 7,
    }
}

/// Decodes a timestamp for row_group_index, row_lo, row_hi. The converter
/// invokes this to backfill missing min and max statistics on the designated
/// timestamp column.
pub type TsStatsBackfill<'a> = dyn Fn(usize, usize, usize) -> ParquetResult<i64> + 'a;

/// Converts a parquet file's metadata into a `_pm` binary representation.
///
/// # Arguments
/// - `file_metadata` - Parquet file metadata from `read_metadata_with_size()`.
/// - `qdb_meta` - Optional QuestDB-specific metadata (from the parquet footer's
///   `"questdb"` key-value pair). If `None`, column types are inferred from the
///   parquet schema and tops default to 0.
/// - `parquet_footer_offset` - Byte offset of the parquet footer in the parquet file.
/// - `parquet_footer_length` - Length of the parquet footer in bytes.
/// - `ts_stats_backfill` - Optional callback used when a row group's designated
///   timestamp column lacks inline min/max stats. When provided, the converter
///   invokes it with `(rg_idx, 0, 1)` for min and `(rg_idx, num_values - 1,
///   num_values)` for max, then writes the results as inline stats.
/// - `parquet_file_data` - Optional view of the parquet file's bytes (mmap or
///   buffer). When provided, any column chunk whose parquet footer carries a
///   `bloom_filter_offset` has its bitset read from this slice and inlined
///   into the `_pm` out-of-line region. When `None`, bloom filters are not
///   inlined and readers fall back to reading the bitset from the parquet
///   file at query time.
///
/// # Errors
/// - If any column chunk references an external `file_path` (not supported).
/// - If the footer's row groups declare conflicting sorting columns and
///   `qdb_meta` has no designated timestamp to fall back on.
/// - If `qdb_meta` is present but its schema length doesn't match the parquet column count.
pub fn convert_from_parquet(
    file_metadata: &FileMetaData,
    qdb_meta: Option<&QdbMeta>,
    parquet_footer_offset: u64,
    parquet_footer_length: u32,
    ts_stats_backfill: Option<&TsStatsBackfill<'_>>,
    parquet_file_data: Option<&[u8]>,
) -> ParquetResult<(Vec<u8>, u64)> {
    let columns = file_metadata.schema_descr.columns();
    let col_count = columns.len();

    // Validate QdbMeta schema length matches.
    if let Some(meta) = qdb_meta {
        if meta.schema.len() != col_count {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::SchemaMismatch,
                "QdbMeta schema has {} columns but parquet has {}",
                meta.schema.len(),
                col_count
            ));
        }
    }

    validate_file_paths(file_metadata)?;
    let sorting_cols = resolve_sorting_columns(file_metadata, qdb_meta)?;

    // Detect designated timestamp.
    let designated_ts = detect_designated_timestamp(file_metadata, qdb_meta, &sorting_cols);

    // Build the writer.
    let mut writer = ParquetMetaWriter::new();
    writer.designated_timestamp(designated_ts);
    writer.parquet_footer(parquet_footer_offset, parquet_footer_length);
    if let Some(meta) = qdb_meta {
        writer.squash_tracker(meta.squash_tracker);
    }

    // Add sorting columns.
    for sc in &sorting_cols {
        writer.add_sorting_column(sc.column_idx as u32);
    }

    // Add column descriptors.
    for (i, col_desc) in columns.iter().enumerate() {
        let field_info = col_desc.base_type.get_field_info();
        let name = &field_info.name;
        // Prefer QuestDB's authoritative id from QdbMeta over the parquet
        // field_id, so a `_pm` rebuilt from a file with no field_id still
        // recovers the column id.
        let id = resolve_column_id(qdb_meta.and_then(|m| m.schema[i].id), field_info.id);

        let col_type_code = if let Some(meta) = qdb_meta {
            let col_meta = &meta.schema[i];
            col_meta.column_type.code()
        } else {
            // Without QdbMeta, infer the QDB type from the parquet schema.
            let inferred = crate::parquet_read::meta::infer_column_type(col_desc);
            inferred.map(|t| t.code()).unwrap_or(-1)
        };

        let mut flags = ColumnFlags::new();

        // Set repetition from parquet schema.
        let repetition = FieldRepetition::from(field_info.repetition);
        flags = flags.with_repetition(repetition);

        // Set QdbMeta-derived flags.
        if let Some(meta) = qdb_meta {
            let col_meta = &meta.schema[i];
            if col_meta.format == Some(QdbMetaColFormat::LocalKeyIsGlobal) {
                flags = flags.with_local_key_is_global();
            }
            if col_meta.ascii == Some(true) {
                flags = flags.with_ascii();
            }
        }

        // Set descending from sorting columns.
        if let Some(sc) = sorting_cols.iter().find(|sc| sc.column_idx == i as i32) {
            if sc.descending {
                flags = flags.with_descending();
            }
        }

        let phys_type = col_desc.descriptor.primitive_type.physical_type;
        let physical_type = physical_type_to_u8(phys_type);
        let fixed_byte_len = match phys_type {
            PhysicalType::FixedLenByteArray(len) => len as i32,
            _ => 0,
        };
        let max_rep_level: u8 = col_desc.descriptor.max_rep_level.try_into().map_err(|_| {
            parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "max_rep_level {} does not fit in u8",
                col_desc.descriptor.max_rep_level
            )
        })?;
        let max_def_level: u8 = col_desc.descriptor.max_def_level.try_into().map_err(|_| {
            parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "max_def_level {} does not fit in u8",
                col_desc.descriptor.max_def_level
            )
        })?;
        writer.add_column(
            name,
            id,
            col_type_code,
            flags,
            fixed_byte_len,
            physical_type,
            max_rep_level,
            max_def_level,
        );
    }

    // Add row groups.
    for (rg_idx, rg) in file_metadata.row_groups.iter().enumerate() {
        let rg_columns = rg.columns();
        if rg_columns.len() != col_count {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::SchemaMismatch,
                "row group has {} columns but schema has {}",
                rg_columns.len(),
                col_count
            ));
        }

        let mut rg_builder = RowGroupBlockBuilder::new(col_count as u32);
        rg_builder.set_num_rows(rg.num_rows().max(0) as u64);

        for (col_idx, col_chunk) in rg_columns.iter().enumerate() {
            let col_type_tag = qdb_meta
                .and_then(|m| {
                    let code = m.schema[col_idx].column_type.tag() as u8;
                    ColumnTypeTag::try_from(code).ok()
                })
                .or_else(|| {
                    crate::parquet_read::meta::infer_column_type(&columns[col_idx])
                        .map(|ct| ct.tag())
                });

            let mut chunk = build_column_chunk(col_chunk)?;

            // Backfill inline min/max stats for the designated timestamp column
            // when the source parquet lacks them. Without this the `_pm` would
            // force readers onto the decode fallback path, defeating the
            // "`_pm` is authoritative" invariant. The closure is only invoked
            // when the chunk has non-zero values and at least one of the
            // min/max stats is missing or not inlined.
            if col_idx as i32 == designated_ts
                && col_type_tag == Some(ColumnTypeTag::Timestamp)
                && chunk.raw.num_values > 0
            {
                if let Some(backfill) = ts_stats_backfill {
                    let stat_flags = StatFlags(chunk.raw.stat_flags);
                    let has_min_inlined = stat_flags.has_min_stat() && stat_flags.is_min_inlined();
                    let has_max_inlined = stat_flags.has_max_stat() && stat_flags.is_max_inlined();
                    if !has_min_inlined || !has_max_inlined {
                        let num_values = chunk.raw.num_values as usize;
                        let min_ts = backfill(rg_idx, 0, 1)?;
                        let max_ts = backfill(rg_idx, num_values - 1, num_values)?;
                        chunk.raw.min_stat = min_ts as u64;
                        chunk.raw.max_stat = max_ts as u64;
                        chunk.raw.stat_flags =
                            stat_flags.with_min(true, true).with_max(true, true).0;
                        chunk.raw.stat_sizes = encode_stat_sizes(8, 8);
                        chunk.ool_min = None;
                        chunk.ool_max = None;
                    }
                }
            }

            rg_builder.set_column_chunk(col_idx, chunk.raw)?;

            // Add out-of-line stats if any.
            if let Some(ref min_bytes) = chunk.ool_min {
                rg_builder.add_out_of_line_stat(col_idx, true, min_bytes)?;
            }
            if let Some(ref max_bytes) = chunk.ool_max {
                rg_builder.add_out_of_line_stat(col_idx, false, max_bytes)?;
            }

            // Inline the bloom-filter bitset into `_pm` when the caller gave
            // us a view of the parquet file. The migration and snapshot
            // restore paths must pass `parquet_file_data` so that the
            // resulting `_pm` matches what the write path produces; without
            // this, readers fall back to reading the bitset from the parquet
            // file on every query.
            if let (Some((offset, _len)), Some(file_data)) =
                (chunk.bloom_filter_parquet, parquet_file_data)
            {
                // A bloom filter the footer claims must be readable. If the bitset cannot be
                // read (corruption, or a bloom-filter header parquet2 does not support), abort
                // the conversion rather than silently producing a _pm without it -- the caller
                // (Mig941, snapshot restore, attach) surfaces the failure with the table path.
                let bitset = parquet2::bloom_filter::read_from_slice_at_offset(offset, file_data)
                    .map_err(|err| {
                    parquet_meta_err!(
                        ParquetMetaErrorKind::Conversion,
                        "could not read parquet bloom filter at offset {}: {}",
                        offset,
                        err
                    )
                })?;
                if !bitset.is_empty() {
                    rg_builder.add_bloom_filter(col_idx, bitset)?;
                }
            }
        }

        writer.add_row_group(rg_builder);
    }

    Ok(writer.finish()?)
}

struct BuiltChunk {
    raw: ColumnChunkRaw,
    ool_min: Option<Vec<u8>>,
    ool_max: Option<Vec<u8>>,
    /// Bloom filter location in the parquet file (offset, length).
    bloom_filter_parquet: Option<(u64, u32)>,
}

fn build_column_chunk(
    col_chunk: &parquet2::metadata::ColumnChunkMetaData,
) -> ParquetResult<BuiltChunk> {
    let (byte_range_start, total_compressed) = col_chunk.byte_range();
    let codec = Codec::from(col_chunk.compression());
    // column_encoding() returns parquet2::thrift_format's Encoding; convert to parquet2's.
    let p2_encodings: Vec<parquet2::encoding::Encoding> = col_chunk
        .column_encoding()
        .iter()
        .filter_map(|e| parquet2::encoding::Encoding::try_from(*e).ok())
        .collect();
    let encodings = EncodingMask::from(p2_encodings.as_slice());

    let m = col_chunk.metadata();
    let bloom_filter_parquet = match (m.bloom_filter_offset, m.bloom_filter_length) {
        (Some(off), Some(len)) if off > 0 && len > 0 => Some((off.max(0) as u64, len as u32)),
        _ => None,
    };

    let mut raw = ColumnChunkRaw::zeroed();
    raw.codec = codec as u8;
    raw.encodings = encodings.0;
    raw.num_values = col_chunk.num_values().max(0) as u64;
    raw.byte_range_start = byte_range_start;
    raw.total_compressed = total_compressed;

    let (ool_min, ool_max) = apply_thrift_stats(&mut raw, m.statistics.as_ref());

    Ok(BuiltChunk { raw, ool_min, ool_max, bloom_filter_parquet })
}

/// Reads min/max/null/distinct counts straight from parquet's thrift
/// statistics and writes them into `raw`, returning any out-of-line bytes.
///
/// Inline vs OOL is gated purely by stat byte width (1..=8 bytes inline,
/// longer goes OOL): the QuestDB column type doesn't constrain placement,
/// because the read path (`can_skip_row_group`, `find_row_group_by_timestamp`)
/// already interprets the slot at parquet physical width and applies any
/// parquet-aware overlay (e.g., `is_date * MILLIS_PER_DAY`) on its own.
/// Stats bytes are passed through verbatim — no typed deserialization, no
/// re-serialization at convert time.
fn apply_thrift_stats(
    raw: &mut ColumnChunkRaw,
    stats: Option<&parquet2::thrift_format::Statistics>,
) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    let Some(stats) = stats else {
        return (None, None);
    };

    let mut stat_flags = StatFlags::new();
    let mut ool_min: Option<Vec<u8>> = None;
    let mut ool_max: Option<Vec<u8>> = None;

    if let Some(nc) = stats.null_count {
        stat_flags = stat_flags.with_null_count();
        raw.null_count = nc.max(0) as u64;
    }
    if let Some(dc) = stats.distinct_count {
        stat_flags = stat_flags.with_distinct_count();
        raw.distinct_count = dc.max(0) as u64;
    }

    // Parquet treats an absent is_*_value_exact as exact: the field post-dates the
    // statistic, so a writer that never widens a bound leaves it unset. Only an
    // explicit Some(false) -- emitted when UTF-8/opaque-Binary min/max truncation
    // widens the bound -- means inexact. Carry that through so the _pm exact bit
    // matches the parquet footer instead of always claiming exact.
    let is_min_exact = stats.is_min_value_exact != Some(false);
    let is_max_exact = stats.is_max_value_exact != Some(false);

    if let Some(min_val) = stats.min_value.as_deref() {
        if !min_val.is_empty() {
            if min_val.len() <= 8 {
                stat_flags = stat_flags.with_min(true, is_min_exact);
                let mut buf = [0u8; 8];
                buf[..min_val.len()].copy_from_slice(min_val);
                raw.min_stat = u64::from_le_bytes(buf);
            } else {
                stat_flags = stat_flags.with_min(false, is_min_exact);
                ool_min = Some(min_val.to_vec());
            }
        }
    }

    if let Some(max_val) = stats.max_value.as_deref() {
        if !max_val.is_empty() {
            if max_val.len() <= 8 {
                stat_flags = stat_flags.with_max(true, is_max_exact);
                let mut buf = [0u8; 8];
                buf[..max_val.len()].copy_from_slice(max_val);
                raw.max_stat = u64::from_le_bytes(buf);
            } else {
                stat_flags = stat_flags.with_max(false, is_max_exact);
                ool_max = Some(max_val.to_vec());
            }
        }
    }

    let min_size = if stat_flags.is_min_inlined() {
        stats.min_value.as_ref().map(|v| v.len() as u8).unwrap_or(0)
    } else {
        0
    };
    let max_size = if stat_flags.is_max_inlined() {
        stats.max_value.as_ref().map(|v| v.len() as u8).unwrap_or(0)
    } else {
        0
    };
    if min_size > 0 || max_size > 0 {
        raw.stat_sizes = encode_stat_sizes(min_size, max_size);
    }
    raw.stat_flags = stat_flags.0;

    (ool_min, ool_max)
}

fn build_column_chunk_from_thrift(
    meta: &parquet2::thrift_format::ColumnMetaData,
) -> ParquetResult<BuiltChunk> {
    // byte_range_start: prefer dictionary_page_offset if present.
    let byte_range_start = meta
        .dictionary_page_offset
        .unwrap_or(meta.data_page_offset)
        .max(0) as u64;
    let total_compressed = meta.total_compressed_size.max(0) as u64;

    // Codec: thrift CompressionCodec → parquet2 Compression → our Codec enum.
    let codec = parquet2::compression::Compression::try_from(meta.codec)
        .map(Codec::from)
        .map_err(|_| {
            parquet_meta_err!(
                ParquetMetaErrorKind::Conversion,
                "unsupported compression codec: {:?}",
                meta.codec
            )
        })?;

    // Encodings: convert thrift Encoding values to parquet2 Encoding, then to EncodingMask.
    let p2_encodings: Vec<parquet2::encoding::Encoding> = meta
        .encodings
        .iter()
        .filter_map(|e| parquet2::encoding::Encoding::try_from(*e).ok())
        .collect();
    let encodings = EncodingMask::from(p2_encodings.as_slice());

    // Bloom filter: capture parquet-file location so the caller can read the
    // bitset and store it in the _pm out-of-line region.
    let bloom_filter_parquet = match (meta.bloom_filter_offset, meta.bloom_filter_length) {
        (Some(off), Some(len)) if off > 0 && len > 0 => Some((off.max(0) as u64, len as u32)),
        _ => None,
    };

    let mut raw = ColumnChunkRaw::zeroed();
    raw.codec = codec as u8;
    raw.encodings = encodings.0;
    raw.num_values = meta.num_values.max(0) as u64;
    raw.byte_range_start = byte_range_start;
    raw.total_compressed = total_compressed;

    let (ool_min, ool_max) = apply_thrift_stats(&mut raw, meta.statistics.as_ref());

    Ok(BuiltChunk { raw, ool_min, ool_max, bloom_filter_parquet })
}

/// Column metadata needed to build a `_pm` header.
///
/// Callers construct this from their own types (`Partition`, `QdbMeta`, etc.)
/// so that `parquet_metadata` doesn't depend on `parquet_write`.
pub struct ParquetMetaColumnInfo<'a> {
    pub name: &'a str,
    pub col_type_code: i32,
    pub id: i32,
    pub flags: ColumnFlags,
    pub fixed_byte_len: i32,
    pub physical_type: u8,
    pub max_rep_level: u8,
    pub max_def_level: u8,
}

/// Result of an incremental `_pm` update.
///
/// `bytes` is always append-only: the caller seeks to the existing file size
/// and writes them after the previous trailer. The previous bytes (including
/// the previous trailer) are left untouched, preserving the stale-reader
/// invariant that any earlier committed `parquet_meta_file_size` continues to resolve
/// to a consistent older snapshot.
///
/// After appending `bytes`, the caller must patch `new_file_size` into the
/// header at `HEADER_PARQUET_META_FILE_SIZE_OFF` as the last step — this is the MVCC
/// commit signal that publishes the new snapshot.
#[derive(Debug)]
pub struct ParquetMetaUpdateResult {
    /// Bytes to append at the existing file size.
    pub bytes: Vec<u8>,
    /// Total `_pm` file size after the append. Also the value the caller
    /// must patch into the header at `HEADER_PARQUET_META_FILE_SIZE_OFF`.
    pub new_file_size: u64,
}

/// Generates a complete `_pm` file from scratch.
///
/// Used by the batch encode path and the O3 rewrite path.
#[allow(clippy::too_many_arguments)]
pub fn generate_parquet_metadata(
    columns: &[ParquetMetaColumnInfo],
    thrift_row_groups: &[parquet2::thrift_format::RowGroup],
    designated_timestamp: i32,
    sorting_columns: &[u32],
    parquet_footer_offset: u64,
    parquet_footer_length: u32,
    bloom_bitsets: &[Vec<Option<Vec<u8>>>],
    unused_bytes: u64,
    squash_tracker: i64,
) -> ParquetResult<(Vec<u8>, u64)> {
    let mut writer = ParquetMetaWriter::new();
    writer.designated_timestamp(designated_timestamp);
    writer.parquet_footer(parquet_footer_offset, parquet_footer_length);
    writer.unused_bytes(unused_bytes);
    writer.squash_tracker(squash_tracker);

    for &sc_idx in sorting_columns {
        writer.add_sorting_column(sc_idx);
    }

    for col in columns {
        writer.add_column(
            col.name,
            col.id,
            col.col_type_code,
            col.flags,
            col.fixed_byte_len,
            col.physical_type,
            col.max_rep_level,
            col.max_def_level,
        );
    }

    for (rg_idx, thrift_rg) in thrift_row_groups.iter().enumerate() {
        let mut block = build_row_group_block_from_thrift_with_types(thrift_rg, &[])?;
        // Add bloom filter bitsets captured during the parquet write.
        if let Some(rg_bitsets) = bloom_bitsets.get(rg_idx) {
            for (col_idx, bf) in rg_bitsets.iter().enumerate() {
                if let Some(bitset) = bf {
                    if !bitset.is_empty() {
                        block.add_bloom_filter(col_idx, bitset)?;
                    }
                }
            }
        }
        writer.add_row_group(block);
    }

    Ok(writer.finish()?)
}

/// Updates an existing `_pm` file incrementally (append-only).
///
/// Compares the new parquet row groups against the committed `_pm`: unchanged
/// row groups keep their original offsets (no data rewritten); only new/changed
/// blocks and a new footer are appended at `append_base`, leaving the committed
/// bytes -- and any orphaned dead-footer tail in [parse anchor, append_base) --
/// intact, so any older committed `parquetMetaFileSize` still resolves to a
/// consistent snapshot.
///
/// `existing_parquet_meta_file_size` is the committed parse anchor (drives which
/// footer is parsed, the new footer's `prev`, and the reused offsets);
/// `append_base` (the published header, `>=` the parse anchor) is where the new
/// bytes land. `existing_parquet_meta` must span at least `append_base` bytes.
///
/// Returns the bytes to append at `append_base` and the resulting file size.
/// Errors when the new row group count is smaller than the existing one
/// (compaction is not supported in the in-place path).
#[allow(clippy::too_many_arguments)]
pub fn update_parquet_metadata(
    existing_parquet_meta: &[u8],
    existing_parquet_meta_file_size: u64,
    append_base: u64,
    thrift_row_groups: &[parquet2::thrift_format::RowGroup],
    parquet_footer_offset: u64,
    parquet_footer_length: u32,
    bloom_bitsets: &[Vec<Option<Vec<u8>>>],
    unused_bytes: u64,
) -> ParquetResult<ParquetMetaUpdateResult> {
    // append_base (the published header, >= the parse anchor) bounds the new
    // footer's position; the buffer must reach it so finish folds any dead
    // footer in [parse anchor, append_base) into the cumulative CRC. Guarded
    // here because append_base comes from the on-disk header: a corrupt value
    // must error, not panic via JNI.
    let append_base_len = usize::try_from(append_base).map_err(|_| {
        parquet_meta_err!(
            ParquetMetaErrorKind::Truncated,
            "_pm append base {} exceeds addressable range",
            append_base
        )
    })?;
    if append_base < existing_parquet_meta_file_size
        || existing_parquet_meta.len() < append_base_len
    {
        return Err(parquet_meta_err!(
            ParquetMetaErrorKind::InvalidValue,
            "_pm append base {} out of range [parse anchor {}, available {}]",
            append_base,
            existing_parquet_meta_file_size,
            existing_parquet_meta.len()
        ));
    }

    // Read the existing _pm to get row group fingerprints (byte_range_start of first column).
    let existing_reader = crate::parquet_metadata::reader::ParquetMetaReader::from_file_size(
        existing_parquet_meta,
        existing_parquet_meta_file_size,
    )?;
    let existing_rg_count = existing_reader.row_group_count() as usize;

    // Collect fingerprints: first column's byte_range_start for each existing row group.
    let mut existing_fingerprints: Vec<Option<u64>> = Vec::with_capacity(existing_rg_count);
    for i in 0..existing_rg_count {
        let rg = existing_reader.row_group(i)?;
        let fp = if existing_reader.column_count() > 0 {
            rg.column_chunk(0).map(|c| c.byte_range_start).ok()
        } else {
            None
        };
        existing_fingerprints.push(fp);
    }

    // Compaction is conceptually representable as an append (write a new
    // footer that references fewer row groups, leaving the dropped blocks
    // as dead space), but the current `ParquetMetaUpdateWriter` exposes
    // only replace/add — it has no remove operation, and the loop below
    // only iterates `thrift_row_groups`, so existing entries beyond the
    // new length would leak into the new footer as stale references. None
    // of the Java callers in `O3PartitionJob` shrink the row group count
    // today, so this is a defensive guard rather than a load-bearing
    // check; it stays so that any future caller that violates the
    // invariant fails loudly instead of silently corrupting the file.
    if thrift_row_groups.len() < existing_rg_count {
        return Err(parquet_meta_err!(
            ParquetMetaErrorKind::InvalidValue,
            "_pm in-place update cannot shrink row group count ({} -> {}); caller must escalate to rewrite mode (new nameTxn directory)",
            existing_rg_count,
            thrift_row_groups.len()
        ));
    }

    // Build the update writer from existing data. The updater takes the
    // committed parquet_meta_file_size that we were handed; its internal reader
    // duplicates the work of `existing_reader` above but keeps the public
    // API simple.
    let mut updater = crate::parquet_metadata::writer::ParquetMetaUpdateWriter::new(
        existing_parquet_meta,
        existing_parquet_meta_file_size,
    )?;

    for (i, thrift_rg) in thrift_row_groups.iter().enumerate() {
        // Fingerprint the new row group: first column's byte_range_start.
        let new_fp: Option<u64> = thrift_rg
            .columns
            .first()
            .and_then(|c| c.meta_data.as_ref())
            .map(|m| m.dictionary_page_offset.unwrap_or(m.data_page_offset) as u64);

        if i < existing_rg_count && existing_fingerprints[i] == new_fp {
            // Unchanged row group — keep existing block.
            continue;
        }

        let mut block = build_row_group_block_from_thrift_with_types(thrift_rg, &[])?;
        if let Some(rg_bitsets) = bloom_bitsets.get(i) {
            for (col_idx, bf) in rg_bitsets.iter().enumerate() {
                if let Some(bitset) = bf {
                    if !bitset.is_empty() {
                        block.add_bloom_filter(col_idx, bitset)?;
                    }
                }
            }
        }

        if i < existing_rg_count {
            // Changed row group — replace.
            updater.replace_row_group(i, block)?;
        } else {
            // New row group — append.
            updater.add_row_group(block);
        }
    }

    updater.parquet_footer(parquet_footer_offset, parquet_footer_length);
    updater.unused_bytes(unused_bytes);
    let (append_bytes, new_file_size) = updater.finish_appending_at(append_base)?;
    debug_assert_eq!(new_file_size, append_base + append_bytes.len() as u64);

    Ok(ParquetMetaUpdateResult { bytes: append_bytes, new_file_size })
}

/// Builds a `RowGroupBlockBuilder` directly from a thrift `RowGroup` struct.
/// Used by `generate_parquet_metadata` and `update_parquet_metadata`.
///
/// Inline vs OOL placement of column statistics is decided by stat byte
/// width inside `apply_thrift_stats`, so this function does not need the
/// QuestDB column type tags. `parquet_data` is the mmapped/read parquet
/// file bytes, used to extract bloom filter bitsets into the _pm
/// out-of-line region.
pub fn build_row_group_block_from_thrift_with_types(
    rg: &parquet2::thrift_format::RowGroup,
    parquet_data: &[u8],
) -> ParquetResult<RowGroupBlockBuilder> {
    let col_count = rg.columns.len();
    let mut builder = RowGroupBlockBuilder::new(col_count as u32);
    builder.set_num_rows(rg.num_rows.max(0) as u64);

    for (col_idx, col_chunk) in rg.columns.iter().enumerate() {
        let meta = col_chunk.meta_data.as_ref().ok_or_else(|| {
            parquet_meta_err!(
                ParquetMetaErrorKind::Conversion,
                "column chunk at col={} has no metadata",
                col_idx
            )
        })?;

        let chunk = build_column_chunk_from_thrift(meta)?;
        builder.set_column_chunk(col_idx, chunk.raw)?;

        if let Some(ref min_bytes) = chunk.ool_min {
            builder.add_out_of_line_stat(col_idx, true, min_bytes)?;
        }
        if let Some(ref max_bytes) = chunk.ool_max {
            builder.add_out_of_line_stat(col_idx, false, max_bytes)?;
        }

        // Copy bloom filter bitset from the parquet file into the _pm OOL region.
        if let Some((pq_offset, _pq_length)) = chunk.bloom_filter_parquet {
            if let Ok(bitset) =
                parquet2::bloom_filter::read_from_slice_at_offset(pq_offset, parquet_data)
            {
                if !bitset.is_empty() {
                    builder.add_bloom_filter(col_idx, bitset)?;
                }
            }
        }
    }

    Ok(builder)
}

fn validate_file_paths(file_metadata: &FileMetaData) -> ParquetResult<()> {
    for (rg_idx, rg) in file_metadata.row_groups.iter().enumerate() {
        for (col_idx, col) in rg.columns().iter().enumerate() {
            if col.file_path().is_some() {
                return Err(parquet_meta_err!(
                    ParquetMetaErrorKind::Conversion,
                    "column chunk at rg={}, col={} references an external file_path (not supported)",
                    rg_idx,
                    col_idx
                ));
            }
        }
    }
    Ok(())
}

pub(crate) struct SortingCol {
    column_idx: i32,
    descending: bool,
}

/// The dense position and order of qdb_meta's designated timestamp, or `None`
/// when there is no designated timestamp.
fn designated_sorting_col(qdb_meta: &QdbMeta) -> Option<SortingCol> {
    qdb_meta
        .schema
        .iter()
        .position(|col| col.column_type.is_designated())
        .map(|pos| SortingCol {
            column_idx: pos as i32,
            descending: !qdb_meta.schema[pos]
                .column_type
                .is_designated_timestamp_ascending(),
        })
}

pub(crate) fn resolve_sorting_columns(
    file_metadata: &FileMetaData,
    qdb_meta: Option<&QdbMeta>,
) -> ParquetResult<Vec<SortingCol>> {
    match qdb_meta.and_then(designated_sorting_col) {
        Some(sc) => Ok(vec![sc]),
        None => extract_sorting_columns(file_metadata),
    }
}

/// Sort columns declared in the parquet footer. Row groups that declare none are
/// skipped -- a legacy O3 merge left copied groups unstamped while fresh ones
/// carried the timestamp sort column -- so only the declaring groups must agree;
/// genuinely conflicting orders are rejected.
pub(crate) fn extract_sorting_columns(
    file_metadata: &FileMetaData,
) -> ParquetResult<Vec<SortingCol>> {
    let mut reference: Option<(usize, &[SortingColumn])> = None;

    for (rg_idx, rg) in file_metadata.row_groups.iter().enumerate() {
        let current: &[SortingColumn] = match rg.sorting_columns() {
            Some(cols) if !cols.is_empty() => cols.as_slice(),
            _ => continue, // no sorting columns -> ignore this row group
        };

        let (ref_idx, prev) = match reference {
            Some(r) => r,
            None => {
                reference = Some((rg_idx, current));
                continue;
            }
        };

        if prev.len() != current.len() {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::SchemaMismatch,
                "sorting columns differ between row groups: rg {} has {} but rg {} has {}",
                ref_idx,
                prev.len(),
                rg_idx,
                current.len()
            ));
        }
        for (i, (p, c)) in prev.iter().zip(current.iter()).enumerate() {
            if p.column_idx != c.column_idx || p.descending != c.descending {
                return Err(parquet_meta_err!(
                    ParquetMetaErrorKind::SchemaMismatch,
                    "sorting column {} differs between row groups {} and {}",
                    i,
                    ref_idx,
                    rg_idx
                ));
            }
        }
    }

    Ok(reference
        .map(|(_, cols)| {
            cols.iter()
                .map(|sc| SortingCol {
                    column_idx: sc.column_idx,
                    descending: sc.descending,
                })
                .collect()
        })
        .unwrap_or_default())
}

pub(crate) fn detect_designated_timestamp(
    file_metadata: &FileMetaData,
    qdb_meta: Option<&QdbMeta>,
    sorting_cols: &[SortingCol],
) -> i32 {
    // Check QdbMeta first: the designated timestamp column has a type flag.
    if let Some(meta) = qdb_meta {
        for (i, col) in meta.schema.iter().enumerate() {
            if col.column_type.is_designated() {
                return i as i32;
            }
        }
    }

    // Fall back to heuristic: first ascending sorting column that is a
    // required timestamp.
    if let Some(first_sc) = sorting_cols.first() {
        if !first_sc.descending {
            let col_idx = first_sc.column_idx as usize;
            if let Some(col_desc) = file_metadata.schema_descr.columns().get(col_idx) {
                let info = &col_desc.descriptor.primitive_type.field_info;
                if info.repetition == parquet2::schema::Repetition::Required {
                    let is_timestamp = if let Some(meta) = qdb_meta {
                        // Check if QdbMeta says it's a timestamp.
                        meta.schema
                            .get(col_idx)
                            .is_some_and(|cm| cm.column_type.tag() == ColumnTypeTag::Timestamp)
                    } else {
                        // No QdbMeta: check the parquet logical type.
                        matches!(
                            col_desc.descriptor.primitive_type.logical_type,
                            Some(PrimitiveLogicalType::Timestamp { .. })
                        )
                    };
                    if is_timestamp {
                        return col_idx as i32;
                    }
                }
            }
        }
    }

    -1
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet::tests::ColumnTypeTagExt;
    use crate::parquet_metadata::reader::ParquetMetaReader;
    use crate::parquet_metadata::types::Codec;
    use crate::parquet_write::file::ParquetWriter;
    use crate::parquet_write::schema::{Column, ParquetEncodingConfig, Partition};
    use parquet2::compression::CompressionOptions;
    use parquet2::read::read_metadata_with_size;
    use parquet2::write::Version;
    use qdb_core::col_type::ColumnTypeTag;
    use std::io::Cursor;

    fn write_test_parquet(row_count: usize, compression: CompressionOptions) -> Vec<u8> {
        let col_data: Vec<i64> = (0..row_count as i64).collect();
        let data_bytes: &[u8] = unsafe {
            std::slice::from_raw_parts(col_data.as_ptr() as *const u8, col_data.len() * 8)
        };
        // Leak to get a 'static reference (only in tests).
        let data_static: &'static [u8] = Box::leak(data_bytes.to_vec().into_boxed_slice());

        let col = Column {
            name: "ts",
            data_type: ColumnTypeTag::Timestamp.into_type(),
            id: 0,
            row_count,
            primary_data: data_static,
            secondary_data: &[],
            symbol_offsets: &[],
            column_top: 0,
            designated_timestamp: true,
            not_null_hint: true,
            strided_timestamp_16: false,
            designated_timestamp_ascending: true,
            parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
        };

        let partition = Partition { table: "test".to_string(), columns: vec![col] };

        let mut buf = Vec::new();
        let writer = ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_compression(compression)
            .with_version(Version::V1)
            .with_row_group_size(Some(row_count));

        writer.finish(partition).unwrap();
        buf
    }

    fn extract_qdb_meta_from(metadata: &FileMetaData) -> Option<QdbMeta> {
        metadata
            .key_value_metadata
            .as_ref()
            .and_then(|kvs| {
                kvs.iter()
                    .find(|kv| kv.key == "questdb")
                    .and_then(|kv| kv.value.as_deref())
            })
            .map(|j| QdbMeta::deserialize(j).unwrap())
    }

    #[test]
    fn convert_simple_parquet() {
        let parquet_data = write_test_parquet(100, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);

        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0, None, None).unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        assert_eq!(reader.column_count(), 1);
        assert_eq!(reader.row_group_count(), 1);
        assert_eq!(reader.column_name(0).unwrap(), "ts");

        let rg = reader.row_group(0).unwrap();
        assert_eq!(rg.num_rows(), 100);

        let chunk = rg.column_chunk(0).unwrap();
        assert_eq!(chunk.codec().unwrap(), Codec::Uncompressed);
        assert!(chunk.byte_range_start > 0);
        assert!(chunk.total_compressed > 0);
        assert_eq!(chunk.num_values, 100);
    }

    #[test]
    fn convert_prefers_qdb_meta_id_over_field_id() {
        let parquet_data = write_test_parquet(10, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        // The written parquet has field_id == QdbMeta id == 0 for its single
        // column. Diverge the QdbMeta id; the _pm must then carry the QdbMeta id,
        // proving it is preferred over the parquet field_id.
        let mut qdb_meta = extract_qdb_meta_from(&metadata).expect("test parquet has qdb meta");
        assert_eq!(qdb_meta.schema.len(), 1);
        qdb_meta.schema[0].id = Some(77);
        let (bytes, size) =
            convert_from_parquet(&metadata, Some(&qdb_meta), 0, 0, None, None).unwrap();
        let reader = ParquetMetaReader::from_file_size(&bytes, size).unwrap();
        assert_eq!(reader.column_descriptor(0).unwrap().id, 77);

        // An absent QdbMeta id falls back to the field_id (0).
        qdb_meta.schema[0].id = None;
        let (bytes, size) =
            convert_from_parquet(&metadata, Some(&qdb_meta), 0, 0, None, None).unwrap();
        let reader = ParquetMetaReader::from_file_size(&bytes, size).unwrap();
        assert_eq!(reader.column_descriptor(0).unwrap().id, 0);

        // Without QdbMeta, the id comes straight from the parquet field_id (0).
        let (bytes, size) = convert_from_parquet(&metadata, None, 0, 0, None, None).unwrap();
        let reader = ParquetMetaReader::from_file_size(&bytes, size).unwrap();
        assert_eq!(reader.column_descriptor(0).unwrap().id, 0);
    }

    #[test]
    fn convert_with_compression() {
        let parquet_data = write_test_parquet(1000, CompressionOptions::Snappy);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, None, 0, 0, None, None).unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        let chunk = reader.row_group(0).unwrap().column_chunk(0).unwrap();
        assert_eq!(chunk.codec().unwrap(), Codec::Snappy);
    }

    #[test]
    fn convert_without_qdb_meta() {
        let parquet_data = write_test_parquet(50, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, None, 0, 0, None, None).unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        assert_eq!(reader.column_count(), 1);
        let desc = reader.column_descriptor(0).unwrap();
        // Without QdbMeta, the type is inferred from the parquet schema.
        // The test parquet has a Timestamp(Micros) column → ColumnTypeTag::Timestamp (8).
        assert_eq!(desc.col_type, ColumnTypeTag::Timestamp as i32);
    }

    #[test]
    fn convert_propagates_squash_tracker_from_qdb_meta() {
        // When QdbMeta.squash_tracker != -1, the _pm header carries it.
        let parquet_data = write_test_parquet(10, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        let mut qdb_meta = extract_qdb_meta_from(&metadata).expect("test parquet has qdb meta");
        qdb_meta.squash_tracker = 42;

        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, Some(&qdb_meta), 0, 0, None, None).unwrap();
        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        assert!(reader.feature_flags().has_squash_tracker());
        assert_eq!(reader.squash_tracker(), Some(42));
    }

    #[test]
    fn convert_omits_squash_tracker_when_neg_one() {
        let parquet_data = write_test_parquet(10, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        let mut qdb_meta = extract_qdb_meta_from(&metadata).expect("test parquet has qdb meta");
        qdb_meta.squash_tracker = -1;

        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, Some(&qdb_meta), 0, 0, None, None).unwrap();
        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        assert!(!reader.feature_flags().has_squash_tracker());
        assert_eq!(reader.squash_tracker(), None);
    }

    #[test]
    fn convert_without_qdb_meta_omits_squash_tracker() {
        let parquet_data = write_test_parquet(10, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, None, 0, 0, None, None).unwrap();
        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        assert!(!reader.feature_flags().has_squash_tracker());
        assert_eq!(reader.squash_tracker(), None);
    }

    #[test]
    fn convert_rejects_mismatched_schema() {
        let parquet_data = write_test_parquet(10, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        let mut bad_meta = QdbMeta::new(0);
        bad_meta
            .schema
            .push(crate::parquet::qdb_metadata::QdbMetaCol {
                column_type: ColumnTypeTag::Int.into_type(),
                column_top: 0,
                format: None,
                ascii: None,
                id: None,
            });
        bad_meta
            .schema
            .push(crate::parquet::qdb_metadata::QdbMetaCol {
                column_type: ColumnTypeTag::Long.into_type(),
                column_top: 0,
                format: None,
                ascii: None,
                id: None,
            });

        let result = convert_from_parquet(&metadata, Some(&bad_meta), 0, 0, None, None);
        assert!(result.is_err());
    }

    /// Regression: an O3 merge copies unchanged row groups without re-stamping
    /// the partition's designated-timestamp sort column, so a converted-then-
    /// merged partition ends up with rg 0 declaring no sorting columns and rg 1
    /// declaring the timestamp sort column. This is the exact shape Mig941 reads
    /// from the on-disk footer; extract_sorting_columns used to reject it with
    /// "rg 0 has 0 but rg 1 has 1" and crash replica bootstrap. It must now
    /// tolerate the mix while still rejecting genuinely conflicting sort orders.
    #[test]
    fn extract_sorting_columns_tolerates_groups_without_sorting() {
        use parquet2::metadata::{RowGroupMetaData, SortingColumn};

        // Parse a real file to get a valid FileMetaData shell, then overwrite
        // its row groups to mimic an O3-merged partition.
        let parquet_data = write_test_parquet(10, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let mut metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        let ts_sort = vec![SortingColumn::new(0, false, false)];
        metadata.row_groups = vec![
            // copied: no sort cols
            RowGroupMetaData::with_sorting_columns(vec![], 10, None, 0),
            // fresh: ts sort
            RowGroupMetaData::with_sorting_columns(vec![], 10, Some(ts_sort.clone()), 0),
        ];

        // Used to error with "rg 0 has 0 but rg 1 has 1"; now tolerated, adopting
        // the sort columns of the only row group that declares any.
        let cols = extract_sorting_columns(&metadata)
            .expect("a mix of empty and non-empty sorting columns must be tolerated");
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].column_idx, 0);
        assert!(!cols[0].descending);

        // Reverse order: rg0 declares the sort column, rg1 declares none -> still tolerated.
        metadata.row_groups = vec![
            // fresh: ts sort
            RowGroupMetaData::with_sorting_columns(vec![], 10, Some(ts_sort.clone()), 0),
            // copied: no sort cols
            RowGroupMetaData::with_sorting_columns(vec![], 10, None, 0),
        ];
        let cols = extract_sorting_columns(&metadata)
            .expect("a mix of empty and non-empty sorting columns must be tolerated");
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].column_idx, 0);
        assert!(!cols[0].descending);

        // Two row groups sorted on DIFFERENT columns is a real conflict -> still rejected.
        metadata.row_groups = vec![
            RowGroupMetaData::with_sorting_columns(
                vec![],
                10,
                Some(vec![SortingColumn::new(0, false, false)]),
                0,
            ),
            RowGroupMetaData::with_sorting_columns(
                vec![],
                10,
                Some(vec![SortingColumn::new(1, false, false)]),
                0,
            ),
        ];
        assert!(extract_sorting_columns(&metadata).is_err());

        // Two row groups that declare a DIFFERENT number of sorting columns is
        // also a real conflict -> still rejected.
        metadata.row_groups = vec![
            RowGroupMetaData::with_sorting_columns(
                vec![],
                10,
                Some(vec![SortingColumn::new(0, false, false)]),
                0,
            ),
            RowGroupMetaData::with_sorting_columns(
                vec![],
                10,
                Some(vec![
                    SortingColumn::new(0, false, false),
                    SortingColumn::new(1, false, false),
                ]),
                0,
            ),
        ];
        assert!(extract_sorting_columns(&metadata).is_err());

        // Same column but OPPOSITE sort direction is a real conflict -> still rejected.
        metadata.row_groups = vec![
            RowGroupMetaData::with_sorting_columns(
                vec![],
                10,
                Some(vec![SortingColumn::new(0, false, false)]),
                0,
            ),
            RowGroupMetaData::with_sorting_columns(
                vec![],
                10,
                Some(vec![SortingColumn::new(0, true, false)]),
                0,
            ),
        ];
        assert!(extract_sorting_columns(&metadata).is_err());
    }

    /// Regression: extract_sorting_columns must stay correct with MORE than two
    /// row groups, where non-declaring (no-sort-column) groups are interleaved
    /// among declaring ones. A partition that is converted and then O3-merged
    /// repeatedly accumulates several copied groups (no sorting columns) around
    /// freshly written ones (the timestamp sort column). The two-row-group tests
    /// never adopt the reference from a non-first group and then hit a conflict
    /// on a later, non-adjacent group; this one does.
    #[test]
    fn extract_sorting_columns_handles_many_row_groups() {
        use parquet2::metadata::{RowGroupMetaData, SortingColumn};

        let parquet_data = write_test_parquet(10, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let mut metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        let ts_sort = || Some(vec![SortingColumn::new(0, false, false)]);

        // Four groups; sorting columns declared only on the inner two, with empty
        // groups before, between, and after -> tolerated, adopting [ts].
        metadata.row_groups = vec![
            RowGroupMetaData::with_sorting_columns(vec![], 10, None, 0),
            RowGroupMetaData::with_sorting_columns(vec![], 10, ts_sort(), 0),
            RowGroupMetaData::with_sorting_columns(vec![], 10, None, 0),
            RowGroupMetaData::with_sorting_columns(vec![], 10, ts_sort(), 0),
        ];
        let cols = extract_sorting_columns(&metadata)
            .expect("interleaved empty and matching groups must be tolerated");
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].column_idx, 0);
        assert!(!cols[0].descending);

        // Reference adopted from a non-first group (rg1), conflict on a later,
        // non-adjacent group (rg3) -> rejected. rg0 and rg2 are skipped.
        metadata.row_groups = vec![
            RowGroupMetaData::with_sorting_columns(vec![], 10, None, 0),
            RowGroupMetaData::with_sorting_columns(
                vec![],
                10,
                Some(vec![SortingColumn::new(0, false, false)]),
                0,
            ),
            RowGroupMetaData::with_sorting_columns(vec![], 10, None, 0),
            RowGroupMetaData::with_sorting_columns(
                vec![],
                10,
                Some(vec![SortingColumn::new(1, false, false)]),
                0,
            ),
        ];
        assert!(
            extract_sorting_columns(&metadata).is_err(),
            "a conflicting sort column on a later group must still be rejected"
        );

        // Conflict expressed as a differing sort-column COUNT on a later group.
        metadata.row_groups = vec![
            RowGroupMetaData::with_sorting_columns(vec![], 10, None, 0),
            RowGroupMetaData::with_sorting_columns(
                vec![],
                10,
                Some(vec![SortingColumn::new(0, false, false)]),
                0,
            ),
            RowGroupMetaData::with_sorting_columns(
                vec![],
                10,
                Some(vec![
                    SortingColumn::new(0, false, false),
                    SortingColumn::new(1, false, false),
                ]),
                0,
            ),
        ];
        assert!(
            extract_sorting_columns(&metadata).is_err(),
            "a differing sort-column count on a later group must still be rejected"
        );
    }

    /// End-to-end: the full Mig941 conversion (resolve_sorting_columns ->
    /// detect_designated_timestamp -> _pm writer -> ParquetMetaReader), not just
    /// extract_sorting_columns in isolation, must tolerate an O3-merged footer
    /// that mixes a copied row group (no sort cols) with a fresh one (the ts
    /// sort col) and still record the designated timestamp. This is the exact
    /// path that threw "rg 0 has 0 but rg 1 has 1" and crashed replica bootstrap.
    #[test]
    fn convert_from_parquet_tolerates_mixed_sorting_columns() {
        use parquet2::metadata::{RowGroupMetaData, SortingColumn};

        // A real, valid file (real column chunks + QdbMeta), ts sort column at col 0.
        let parquet_data = write_test_parquet(10, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let mut metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata).expect("test parquet has qdb meta");

        // Reshape into the O3-merged footer shape, carrying the REAL column chunks
        // so convert_from_parquet's per-row-group column walk runs for real: rg0
        // copied (no sort cols), rg1 fresh (ts sort col).
        let cols = metadata.row_groups[0].columns().to_vec();
        let n = metadata.row_groups[0].num_rows();
        metadata.row_groups = vec![
            RowGroupMetaData::with_sorting_columns(cols.clone(), n, None, 0),
            RowGroupMetaData::with_sorting_columns(
                cols,
                n,
                Some(vec![SortingColumn::new(0, false, false)]),
                1,
            ),
        ];

        // The full conversion must succeed and still record the designated ts.
        let (pm_bytes, pm_size) =
            convert_from_parquet(&metadata, Some(&qdb_meta), 0, 0, None, None)
                .expect("convert_from_parquet must tolerate the mixed O3 footer");
        let reader = ParquetMetaReader::from_file_size(&pm_bytes, pm_size).unwrap();
        assert_eq!(reader.designated_timestamp(), Some(0));
        assert_eq!(reader.sorting_column_count(), 1);
        assert_eq!(reader.sorting_column(0).unwrap(), 0);
    }

    /// C1 regression: across a version upgrade an update-mode O3 merge can leave a
    /// footer whose declaring row groups disagree -- a cached group with the stale
    /// index 1, a freshly appended group with the corrected index 0. The footer
    /// reader rejects that, but the migration must trust qdb_meta and not abort.
    #[test]
    fn convert_from_parquet_tolerates_conflicting_sort_indices_via_qdb_meta() {
        use parquet2::metadata::{RowGroupMetaData, SortingColumn};

        // Real, valid file with qdb_meta designating the timestamp at column 0.
        let parquet_data = write_test_parquet(10, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let mut metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata).expect("test parquet has qdb meta");

        // Footer shape: copied group (no sort col), legacy stale [1], corrected [0].
        let cols = metadata.row_groups[0].columns().to_vec();
        let n = metadata.row_groups[0].num_rows();
        metadata.row_groups = vec![
            RowGroupMetaData::with_sorting_columns(cols.clone(), n, None, 0),
            RowGroupMetaData::with_sorting_columns(
                cols.clone(),
                n,
                Some(vec![SortingColumn::new(1, false, false)]),
                1,
            ),
            RowGroupMetaData::with_sorting_columns(
                cols,
                n,
                Some(vec![SortingColumn::new(0, false, false)]),
                2,
            ),
        ];

        // The footer reader alone rejects this; the migration must not.
        assert!(extract_sorting_columns(&metadata).is_err());

        let (pm_bytes, pm_size) =
            convert_from_parquet(&metadata, Some(&qdb_meta), 0, 0, None, None)
                .expect("convert_from_parquet must resolve sorting from qdb_meta, not the footer");
        let reader = ParquetMetaReader::from_file_size(&pm_bytes, pm_size).unwrap();
        assert_eq!(reader.designated_timestamp(), Some(0));
        assert_eq!(reader.sorting_column_count(), 1);
        // The dense designated position (0), not the stale footer index 1.
        assert_eq!(reader.sorting_column(0).unwrap(), 0);

        // Scoped to QuestDB files: without qdb_meta the conflict still aborts.
        assert!(convert_from_parquet(&metadata, None, 0, 0, None, None).is_err());
    }

    /// extract_sorting_columns returns an empty set for a footer with no row groups.
    #[test]
    fn extract_sorting_columns_handles_zero_row_groups() {
        let parquet_data = write_test_parquet(10, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let mut metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        metadata.row_groups = vec![];
        let cols = extract_sorting_columns(&metadata).expect("zero row groups is valid");
        assert!(cols.is_empty());
    }

    /// A present-but-empty sorting vector is skipped like None, not treated as a
    /// conflict against a sibling group that declares a real sort column.
    #[test]
    fn extract_sorting_columns_skips_empty_present_sorting_columns() {
        use parquet2::metadata::{RowGroupMetaData, SortingColumn};

        let parquet_data = write_test_parquet(10, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let mut metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        metadata.row_groups = vec![
            // present but empty -> treated as "no sorting columns", skipped.
            RowGroupMetaData::with_sorting_columns(vec![], 10, Some(vec![]), 0),
            RowGroupMetaData::with_sorting_columns(
                vec![],
                10,
                Some(vec![SortingColumn::new(0, false, false)]),
                0,
            ),
        ];
        let cols = extract_sorting_columns(&metadata)
            .expect("an empty-but-present sorting vector must be skipped, not conflict");
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].column_idx, 0);
    }

    fn leak_bytes(data: &[u8]) -> &'static [u8] {
        Box::leak(data.to_vec().into_boxed_slice())
    }

    fn write_multi_column_parquet(row_count: usize) -> Vec<u8> {
        // Timestamp column (i64).
        let ts_data: Vec<i64> = (0..row_count as i64).collect();
        let ts_bytes = leak_bytes(unsafe {
            std::slice::from_raw_parts(ts_data.as_ptr() as *const u8, ts_data.len() * 8)
        });

        // Int column (i32).
        let int_data: Vec<i32> = (0..row_count as i32).collect();
        let int_bytes = leak_bytes(unsafe {
            std::slice::from_raw_parts(int_data.as_ptr() as *const u8, int_data.len() * 4)
        });

        // Double column (f64).
        let dbl_data: Vec<f64> = (0..row_count).map(|i| i as f64 * 1.5).collect();
        let dbl_bytes = leak_bytes(unsafe {
            std::slice::from_raw_parts(dbl_data.as_ptr() as *const u8, dbl_data.len() * 8)
        });

        // Boolean column (u8, 1 byte per value).
        let bool_data: Vec<u8> = (0..row_count).map(|i| (i % 2) as u8).collect();
        let bool_bytes = leak_bytes(&bool_data);

        let cols = vec![
            Column {
                name: "ts",
                data_type: ColumnTypeTag::Timestamp.into_type(),
                id: 0,
                row_count,
                primary_data: ts_bytes,
                secondary_data: &[],
                symbol_offsets: &[],
                column_top: 0,
                designated_timestamp: true,
                not_null_hint: true,
                strided_timestamp_16: false,
                designated_timestamp_ascending: true,
                parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
            },
            Column {
                name: "int_val",
                data_type: ColumnTypeTag::Int.into_type(),
                id: 1,
                row_count,
                primary_data: int_bytes,
                secondary_data: &[],
                symbol_offsets: &[],
                column_top: 0,
                designated_timestamp: false,
                not_null_hint: false,
                strided_timestamp_16: false,
                designated_timestamp_ascending: false,
                parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
            },
            Column {
                name: "dbl_val",
                data_type: ColumnTypeTag::Double.into_type(),
                id: 2,
                row_count,
                primary_data: dbl_bytes,
                secondary_data: &[],
                symbol_offsets: &[],
                column_top: 0,
                designated_timestamp: false,
                not_null_hint: false,
                strided_timestamp_16: false,
                designated_timestamp_ascending: false,
                parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
            },
            Column {
                name: "bool_val",
                data_type: ColumnTypeTag::Boolean.into_type(),
                id: 3,
                row_count,
                primary_data: bool_bytes,
                secondary_data: &[],
                symbol_offsets: &[],
                column_top: 0,
                designated_timestamp: false,
                not_null_hint: false,
                strided_timestamp_16: false,
                designated_timestamp_ascending: false,
                parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
            },
        ];

        let partition = Partition { table: "test_multi".to_string(), columns: cols };

        let mut buf = Vec::new();
        let writer = ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_compression(CompressionOptions::Uncompressed)
            .with_version(Version::V1)
            .with_row_group_size(Some(row_count));

        writer.finish(partition).unwrap();
        buf
    }

    #[test]
    fn convert_multi_column_with_stats() {
        let parquet_data = write_multi_column_parquet(100);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);

        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 1024, 200, None, None).unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        assert_eq!(reader.column_count(), 4);
        assert_eq!(reader.row_group_count(), 1);
        assert_eq!(reader.parquet_footer_offset(), 1024);
        assert_eq!(reader.parquet_footer_length(), 200);

        assert_eq!(reader.column_name(0).unwrap(), "ts");
        assert_eq!(reader.column_name(1).unwrap(), "int_val");
        assert_eq!(reader.column_name(2).unwrap(), "dbl_val");
        assert_eq!(reader.column_name(3).unwrap(), "bool_val");

        let rg = reader.row_group(0).unwrap();
        assert_eq!(rg.num_rows(), 100);

        // Timestamp column (Int64 physical type) - stats should be inlined.
        let ts_chunk = rg.column_chunk(0).unwrap();
        let ts_flags = ts_chunk.stat_flags();
        assert!(ts_flags.has_min_stat());
        assert!(ts_flags.has_max_stat());
        assert!(ts_flags.has_null_count());
        assert_eq!(ts_chunk.num_values, 100);

        // Int column (Int32 physical type) - inline stats.
        let int_chunk = rg.column_chunk(1).unwrap();
        let int_flags = int_chunk.stat_flags();
        assert!(int_flags.has_min_stat());
        assert!(int_flags.has_max_stat());

        // Double column (Double physical type).
        let dbl_chunk = rg.column_chunk(2).unwrap();
        let dbl_flags = dbl_chunk.stat_flags();
        assert!(dbl_flags.has_min_stat());
        assert!(dbl_flags.has_max_stat());

        // Boolean column - inline stats.
        let bool_chunk = rg.column_chunk(3).unwrap();
        let bool_flags = bool_chunk.stat_flags();
        assert!(bool_flags.has_min_stat());
        assert!(bool_flags.has_max_stat());
    }

    /// Regression: a SHORT column with negative values must round-trip through
    /// the inline u64 stat slot and read back as the correct i32 at parquet
    /// physical width (Int32). Earlier the convert path narrowed to 2 bytes,
    /// the inline slot zero-padded to 8, and the skip path's 4-byte i32 read
    /// turned a -74 i16 into 65462, dropping every row group whose true min
    /// was negative.
    #[test]
    fn convert_short_negative_min_round_trips_at_int32_width() {
        let row_count = 100;
        let short_data: Vec<i16> = (0..row_count as i16).map(|i| i - 50).collect();
        let short_bytes = leak_bytes(unsafe {
            std::slice::from_raw_parts(short_data.as_ptr() as *const u8, short_data.len() * 2)
        });

        let ts_data: Vec<i64> = (0..row_count as i64).collect();
        let ts_bytes = leak_bytes(unsafe {
            std::slice::from_raw_parts(ts_data.as_ptr() as *const u8, ts_data.len() * 8)
        });

        let cols = vec![
            Column {
                name: "ts",
                data_type: ColumnTypeTag::Timestamp.into_type(),
                id: 0,
                row_count,
                primary_data: ts_bytes,
                secondary_data: &[],
                symbol_offsets: &[],
                column_top: 0,
                designated_timestamp: true,
                not_null_hint: true,
                strided_timestamp_16: false,
                designated_timestamp_ascending: true,
                parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
            },
            Column {
                name: "val",
                data_type: ColumnTypeTag::Short.into_type(),
                id: 1,
                row_count,
                primary_data: short_bytes,
                secondary_data: &[],
                symbol_offsets: &[],
                column_top: 0,
                designated_timestamp: false,
                not_null_hint: false,
                strided_timestamp_16: false,
                designated_timestamp_ascending: false,
                parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
            },
        ];

        let partition = Partition { table: "test_short_neg".to_string(), columns: cols };

        let mut buf = Vec::new();
        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_compression(CompressionOptions::Uncompressed)
            .with_version(Version::V1)
            .with_row_group_size(Some(row_count))
            .finish(partition)
            .unwrap();

        let mut cursor = Cursor::new(&buf);
        let metadata = read_metadata_with_size(&mut cursor, buf.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);

        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0, None, None).unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        let chunk = reader.row_group(0).unwrap().column_chunk(1).unwrap();

        // Read the low 4 bytes of the inline slot as i32 — exactly what
        // can_skip_row_group does for Int32 physical width.
        let bytes = chunk.min_stat.to_le_bytes();
        let min_i32 = i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        assert_eq!(
            min_i32, -50,
            "inline min_stat must read back as -50 at Int32 width"
        );

        let bytes = chunk.max_stat.to_le_bytes();
        let max_i32 = i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        assert_eq!(
            max_i32, 49,
            "inline max_stat must read back as 49 at Int32 width"
        );
    }

    /// Regression: an external INT32 + Date logical-type parquet must keep its
    /// stats as i32 days at parquet physical width when ingested through the
    /// inline path. The earlier convert_stat_to_qdb widened days to i64 millis,
    /// so the skip path's 4-byte INT32 read interpreted the low 4 bytes of
    /// millis as days and produced wildly wrong bounds. QuestDB itself writes
    /// Date as INT64+millis, so this case only surfaces for external Int32-Date
    /// parquets converted through `build_column_chunk_from_thrift`.
    #[test]
    fn convert_int32_date_round_trips_at_int32_width() {
        use parquet2::thrift_format::{
            ColumnChunk, ColumnMetaData, CompressionCodec, Encoding as ThriftEncoding, RowGroup,
            Statistics as ThriftStatistics, Type,
        };

        let min_days: i32 = -100; // ~1969-09-23
        let max_days: i32 = 365; //  1971-01-01

        let stats = ThriftStatistics {
            max: None,
            min: None,
            null_count: Some(0),
            distinct_count: None,
            max_value: Some(max_days.to_le_bytes().to_vec()),
            min_value: Some(min_days.to_le_bytes().to_vec()),
            is_max_value_exact: None,
            is_min_value_exact: None,
        };
        let meta = ColumnMetaData {
            type_: Type::INT32,
            encodings: vec![ThriftEncoding::PLAIN],
            path_in_schema: vec!["d".to_string()],
            codec: CompressionCodec::UNCOMPRESSED,
            num_values: 100,
            total_uncompressed_size: 400,
            total_compressed_size: 400,
            key_value_metadata: None,
            data_page_offset: 4,
            index_page_offset: None,
            dictionary_page_offset: None,
            statistics: Some(stats),
            encoding_stats: None,
            bloom_filter_offset: None,
            bloom_filter_length: None,
        };
        let column_chunk = ColumnChunk {
            file_path: None,
            file_offset: 0,
            meta_data: Some(meta),
            offset_index_offset: None,
            offset_index_length: None,
            column_index_offset: None,
            column_index_length: None,
            crypto_metadata: None,
            encrypted_column_metadata: None,
        };
        let row_group = RowGroup {
            columns: vec![column_chunk],
            total_byte_size: 400,
            num_rows: 100,
            sorting_columns: None,
            file_offset: None,
            total_compressed_size: None,
            ordinal: None,
        };

        let block = build_row_group_block_from_thrift_with_types(&row_group, &[]).unwrap();

        let chunk = block.column_chunk_raw(0);
        let stat_flags = StatFlags(chunk.stat_flags);
        assert!(stat_flags.has_min_stat());
        assert!(stat_flags.is_min_inlined());
        assert!(stat_flags.has_max_stat());
        assert!(stat_flags.is_max_inlined());

        // The slot must hold i32 days at parquet physical width (4 bytes).
        // Reading the low 4 bytes as i32 is exactly what `can_skip_row_group`
        // does for INT32 physical type before applying `MILLIS_PER_DAY`.
        let min_bytes = chunk.min_stat.to_le_bytes();
        let min_i32 = i32::from_le_bytes([min_bytes[0], min_bytes[1], min_bytes[2], min_bytes[3]]);
        assert_eq!(
            min_i32, min_days,
            "INT32-Date inline min must read back as i32 days, not i64 millis"
        );
        let max_bytes = chunk.max_stat.to_le_bytes();
        let max_i32 = i32::from_le_bytes([max_bytes[0], max_bytes[1], max_bytes[2], max_bytes[3]]);
        assert_eq!(
            max_i32, max_days,
            "INT32-Date inline max must read back as i32 days, not i64 millis"
        );

        // High 4 bytes must be zero. This pins the contract: the convert path
        // did not widen i32 days to i64 millis before writing the slot.
        assert_eq!(&min_bytes[4..], &[0u8; 4]);
        assert_eq!(&max_bytes[4..], &[0u8; 4]);
    }

    /// Run `apply_thrift_stats` over a thrift `Statistics` carrying the given
    /// min/max values and exactness flags, returning the resulting `_pm` raw plus
    /// any out-of-line stat bytes.
    fn apply_thrift_stats_for(
        min_value: Option<Vec<u8>>,
        max_value: Option<Vec<u8>>,
        is_min_value_exact: Option<bool>,
        is_max_value_exact: Option<bool>,
    ) -> (ColumnChunkRaw, Option<Vec<u8>>, Option<Vec<u8>>) {
        let stats = parquet2::thrift_format::Statistics {
            max: None,
            min: None,
            null_count: None,
            distinct_count: None,
            max_value,
            min_value,
            is_max_value_exact,
            is_min_value_exact,
        };
        let mut raw = ColumnChunkRaw::zeroed();
        let (ool_min, ool_max) = apply_thrift_stats(&mut raw, Some(&stats));
        (raw, ool_min, ool_max)
    }

    #[test]
    fn pm_exact_bit_set_when_thrift_flag_absent() {
        // An absent is_*_value_exact is Parquet's "exact" default: short inline
        // bounds with no flag must mark _pm exact.
        let (raw, _, _) = apply_thrift_stats_for(Some(vec![1, 2]), Some(vec![9, 9]), None, None);
        let sf = raw.stat_flags();
        assert!(sf.has_min_stat() && sf.is_min_inlined() && sf.is_min_exact());
        assert!(sf.has_max_stat() && sf.is_max_inlined() && sf.is_max_exact());
    }

    #[test]
    fn pm_exact_bit_set_when_thrift_flag_explicitly_true() {
        let (raw, _, _) =
            apply_thrift_stats_for(Some(vec![1, 2]), Some(vec![9, 9]), Some(true), Some(true));
        let sf = raw.stat_flags();
        assert!(sf.is_min_exact());
        assert!(sf.is_max_exact());
    }

    #[test]
    fn pm_exact_bit_cleared_when_thrift_flag_false_inline() {
        // The core fix: a bound the writer marked inexact (Some(false)) must clear
        // the _pm exact bit even when it is short enough to inline -- e.g. an
        // external parquet that truncated a min/max to a short prefix.
        let (raw, _, _) =
            apply_thrift_stats_for(Some(vec![1, 2]), Some(vec![9, 9]), Some(false), Some(false));
        let sf = raw.stat_flags();
        assert!(sf.has_min_stat() && sf.is_min_inlined() && !sf.is_min_exact());
        assert!(sf.has_max_stat() && sf.is_max_inlined() && !sf.is_max_exact());
    }

    #[test]
    fn pm_exact_bit_cleared_for_truncated_ool_bounds() {
        // The realistic UTF-8/opaque-Binary truncation case: a 64-byte bound goes
        // out-of-line and is flagged inexact; _pm must record OOL + not-exact and
        // carry the full bytes out of line.
        let long_min = vec![b'a'; 64];
        let long_max = vec![b'z'; 64];
        let (raw, ool_min, ool_max) = apply_thrift_stats_for(
            Some(long_min.clone()),
            Some(long_max.clone()),
            Some(false),
            Some(false),
        );
        let sf = raw.stat_flags();
        assert!(sf.has_min_stat() && !sf.is_min_inlined() && !sf.is_min_exact());
        assert!(sf.has_max_stat() && !sf.is_max_inlined() && !sf.is_max_exact());
        assert_eq!(ool_min.as_deref(), Some(long_min.as_slice()));
        assert_eq!(ool_max.as_deref(), Some(long_max.as_slice()));
    }

    #[test]
    fn pm_exact_bit_kept_for_exact_ool_bounds() {
        // A long bound is not automatically inexact: fixed-length byte arrays
        // (Uuid/Long256/Decimal) store exact bounds verbatim. A 16-byte exact min
        // must stay OOL and exact.
        let exact_min = vec![7u8; 16];
        let (raw, ool_min, _) = apply_thrift_stats_for(Some(exact_min.clone()), None, None, None);
        let sf = raw.stat_flags();
        assert!(sf.has_min_stat() && !sf.is_min_inlined() && sf.is_min_exact());
        assert_eq!(ool_min.as_deref(), Some(exact_min.as_slice()));
    }

    #[test]
    fn pm_min_and_max_exactness_are_independent() {
        // An inexact min must not drag the max's exact bit down, and vice versa.
        let (raw, _, _) =
            apply_thrift_stats_for(Some(vec![1, 2]), Some(vec![9, 9]), Some(false), None);
        let sf = raw.stat_flags();
        assert!(!sf.is_min_exact());
        assert!(sf.is_max_exact());

        let (raw, _, _) =
            apply_thrift_stats_for(Some(vec![1, 2]), Some(vec![9, 9]), None, Some(false));
        let sf = raw.stat_flags();
        assert!(sf.is_min_exact());
        assert!(!sf.is_max_exact());
    }

    #[test]
    fn pm_no_stat_bits_for_absent_or_empty_value() {
        // An absent or empty bound sets no present/inlined/exact bit at all: the
        // exactness flag is irrelevant when there is no value to qualify.
        let (raw, ool_min, ool_max) =
            apply_thrift_stats_for(None, Some(Vec::new()), Some(false), Some(false));
        let sf = raw.stat_flags();
        assert!(!sf.has_min_stat() && !sf.is_min_exact());
        assert!(!sf.has_max_stat() && !sf.is_max_exact());
        assert!(ool_min.is_none() && ool_max.is_none());
    }

    #[test]
    fn build_column_chunk_from_thrift_carries_inexact_bit() {
        // End-to-end through the thrift column builder: a truncated (inexact) min
        // on a BYTE_ARRAY column must surface as a not-exact _pm bit, proving the
        // exactness survives the full convert path rather than just the helper.
        use parquet2::thrift_format::{
            ColumnMetaData, CompressionCodec, Encoding as ThriftEncoding, Statistics, Type,
        };
        let long_min = vec![b'a'; 64];
        let stats = Statistics {
            max: None,
            min: None,
            null_count: Some(0),
            distinct_count: None,
            max_value: None,
            min_value: Some(long_min.clone()),
            is_max_value_exact: None,
            is_min_value_exact: Some(false),
        };
        let meta = ColumnMetaData {
            type_: Type::BYTE_ARRAY,
            encodings: vec![ThriftEncoding::PLAIN],
            path_in_schema: vec!["s".to_string()],
            codec: CompressionCodec::UNCOMPRESSED,
            num_values: 10,
            total_uncompressed_size: 100,
            total_compressed_size: 100,
            key_value_metadata: None,
            data_page_offset: 4,
            index_page_offset: None,
            dictionary_page_offset: None,
            statistics: Some(stats),
            encoding_stats: None,
            bloom_filter_offset: None,
            bloom_filter_length: None,
        };
        let built = build_column_chunk_from_thrift(&meta).unwrap();
        let sf = built.raw.stat_flags();
        assert!(sf.has_min_stat() && !sf.is_min_inlined() && !sf.is_min_exact());
        assert_eq!(built.ool_min.as_deref(), Some(long_min.as_slice()));
    }

    #[test]
    fn convert_with_qdb_meta_flags() {
        let parquet_data = write_test_parquet(10, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        // Build QdbMeta with LocalKeyIsGlobal and ascii flags.
        let mut meta = QdbMeta::new(1);
        meta.schema.push(crate::parquet::qdb_metadata::QdbMetaCol {
            column_type: ColumnTypeTag::Symbol.into_type(),
            column_top: 42,
            format: Some(QdbMetaColFormat::LocalKeyIsGlobal),
            ascii: Some(true),
            id: None,
        });

        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, Some(&meta), 0, 0, None, None).unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        let desc = reader.column_descriptor(0).unwrap();
        let flags = desc.flags();
        assert!(flags.is_local_key_global());
        assert!(flags.is_ascii());
        // `top` is no longer part of the column descriptor.
    }

    #[test]
    fn convert_sorting_columns_propagated() {
        // Write a parquet file with a designated timestamp. qdb_meta marks the
        // designated column, which resolve_sorting_columns treats as
        // authoritative for the _pm sort column -- so it is recorded at its
        // dense position even when the footer's row groups omit sorting columns.
        let parquet_data = write_test_parquet(100, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        let qdb_meta = extract_qdb_meta_from(&metadata);
        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0, None, None).unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();

        // The designated timestamp (from qdb_meta) is the sole sort column, at
        // its dense position (0).
        assert!(reader.designated_timestamp().is_some());
        assert_eq!(reader.sorting_column_count(), 1);
        assert_eq!(reader.sorting_column(0).unwrap(), 0);
    }

    #[test]
    fn convert_multi_row_groups() {
        // Write a parquet file with multiple row groups.
        let row_count = 200;
        let col_data: Vec<i64> = (0..row_count as i64).collect();
        let data_bytes = leak_bytes(unsafe {
            std::slice::from_raw_parts(col_data.as_ptr() as *const u8, col_data.len() * 8)
        });

        let col = Column {
            name: "ts",
            data_type: ColumnTypeTag::Timestamp.into_type(),
            id: 0,
            row_count,
            primary_data: data_bytes,
            secondary_data: &[],
            symbol_offsets: &[],
            column_top: 0,
            designated_timestamp: true,
            not_null_hint: true,
            strided_timestamp_16: false,
            designated_timestamp_ascending: true,
            parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
        };

        let partition = Partition { table: "test".to_string(), columns: vec![col] };

        let mut buf = Vec::new();
        // Use small row group size to get multiple row groups.
        let writer = ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_compression(CompressionOptions::Uncompressed)
            .with_version(Version::V1)
            .with_row_group_size(Some(75));

        writer.finish(partition).unwrap();

        let mut cursor = Cursor::new(&buf);
        let metadata = read_metadata_with_size(&mut cursor, buf.len() as u64).unwrap();

        assert!(metadata.row_groups.len() >= 2);

        let qdb_meta = extract_qdb_meta_from(&metadata);
        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0, None, None).unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        assert_eq!(reader.row_group_count(), metadata.row_groups.len() as u32);

        // Verify each row group has correct num_rows.
        for i in 0..reader.row_group_count() as usize {
            let rg = reader.row_group(i).unwrap();
            assert_eq!(rg.num_rows(), metadata.row_groups[i].num_rows() as u64);
        }
    }

    fn write_float_parquet(row_count: usize) -> Vec<u8> {
        let float_data: Vec<f32> = (0..row_count).map(|i| i as f32 * 0.5).collect();
        let float_bytes = leak_bytes(unsafe {
            std::slice::from_raw_parts(float_data.as_ptr() as *const u8, float_data.len() * 4)
        });

        let col = Column {
            name: "float_val",
            data_type: ColumnTypeTag::Float.into_type(),
            id: 0,
            row_count,
            primary_data: float_bytes,
            secondary_data: &[],
            symbol_offsets: &[],
            column_top: 0,
            designated_timestamp: false,
            not_null_hint: false,
            strided_timestamp_16: false,
            designated_timestamp_ascending: false,
            parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
        };

        let partition = Partition {
            table: "test_float".to_string(),
            columns: vec![col],
        };

        let mut buf = Vec::new();
        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_compression(CompressionOptions::Uncompressed)
            .with_version(Version::V1)
            .with_row_group_size(Some(row_count))
            .finish(partition)
            .unwrap();
        buf
    }

    #[test]
    fn convert_float_column_stats() {
        let parquet_data = write_float_parquet(50);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);

        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0, None, None).unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        let rg = reader.row_group(0).unwrap();
        let chunk = rg.column_chunk(0).unwrap();
        let flags = chunk.stat_flags();
        assert!(flags.has_min_stat());
        assert!(flags.has_max_stat());
        // Float is 4 bytes, should be inlined.
        assert!(flags.is_min_inlined());
        assert!(flags.is_max_inlined());
    }

    #[test]
    fn convert_distinct_count_present() {
        // Write a parquet file with statistics enabled.
        let parquet_data = write_multi_column_parquet(50);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);

        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0, None, None).unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        let rg = reader.row_group(0).unwrap();

        // Check that at least one column has distinct_count or null_count set.
        let mut has_null_count = false;
        for i in 0..reader.column_count() as usize {
            let chunk = rg.column_chunk(i).unwrap();
            if chunk.stat_flags().has_null_count() {
                has_null_count = true;
            }
        }
        assert!(has_null_count);
    }

    // ── Tests for build_row_group_block_from_thrift_with_types ─────────

    /// Test helper: thin wrapper around the public function.
    fn build_row_group_block_from_thrift(
        rg: &parquet2::thrift_format::RowGroup,
    ) -> crate::parquet::error::ParquetResult<RowGroupBlockBuilder> {
        build_row_group_block_from_thrift_with_types(rg, &[])
    }

    #[test]
    fn thrift_round_trip_matches_convert_from_parquet() {
        // Write a multi-column parquet, then compare:
        // 1. convert_from_parquet (high-level FileMetaData path)
        // 2. build_row_group_block_from_thrift (thrift RowGroup path)
        // Both should produce identical _pm row group blocks.
        let parquet_data = write_multi_column_parquet(200);
        let mut cursor = Cursor::new(&parquet_data);
        let file_size = parquet_data.len() as u64;
        let metadata = read_metadata_with_size(&mut cursor, file_size).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);

        // Path 1: convert_from_parquet
        let (parquet_meta_bytes_from_meta, parquet_meta_file_size) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0, None, None).unwrap();
        let reader1 = ParquetMetaReader::from_file_size(
            &parquet_meta_bytes_from_meta,
            parquet_meta_file_size,
        )
        .unwrap();

        // Path 2: build from thrift via the writer's into_inner_and_metadata()
        // We need the ThriftFileMetaData. Write again using ChunkedWriter to get it.
        let parquet_data2 = write_multi_column_parquet(200);
        let mut cursor2 = Cursor::new(&parquet_data2);
        let metadata2 = read_metadata_with_size(&mut cursor2, parquet_data2.len() as u64).unwrap();
        let thrift_meta = metadata2.into_thrift();

        // Build row group blocks from thrift row groups.
        for (rg_idx, thrift_rg) in thrift_meta.row_groups.iter().enumerate() {
            let block = build_row_group_block_from_thrift(thrift_rg).unwrap();

            // Compare against the convert_from_parquet result.
            let rg1 = reader1.row_group(rg_idx).unwrap();
            assert_eq!(
                block.num_rows(),
                rg1.num_rows(),
                "num_rows mismatch at rg {rg_idx}"
            );

            for col_idx in 0..reader1.column_count() as usize {
                let chunk1 = rg1.column_chunk(col_idx).unwrap();
                let chunk2 = block.column_chunk_raw(col_idx);

                assert_eq!(
                    chunk1.codec, chunk2.codec,
                    "codec mismatch rg={rg_idx} col={col_idx}"
                );
                assert_eq!(
                    chunk1.encodings, chunk2.encodings,
                    "encodings mismatch rg={rg_idx} col={col_idx}"
                );
                assert_eq!(
                    chunk1.byte_range_start, chunk2.byte_range_start,
                    "byte_range mismatch rg={rg_idx} col={col_idx}"
                );
                assert_eq!(
                    chunk1.total_compressed, chunk2.total_compressed,
                    "total_compressed mismatch rg={rg_idx} col={col_idx}"
                );
                assert_eq!(
                    chunk1.num_values, chunk2.num_values,
                    "num_values mismatch rg={rg_idx} col={col_idx}"
                );
                assert_eq!(
                    chunk1.null_count, chunk2.null_count,
                    "null_count mismatch rg={rg_idx} col={col_idx}"
                );
                assert_eq!(
                    chunk1.stat_flags, chunk2.stat_flags,
                    "stat_flags mismatch rg={rg_idx} col={col_idx}"
                );
                assert_eq!(
                    chunk1.min_stat, chunk2.min_stat,
                    "min_stat mismatch rg={rg_idx} col={col_idx}"
                );
                assert_eq!(
                    chunk1.max_stat, chunk2.max_stat,
                    "max_stat mismatch rg={rg_idx} col={col_idx}"
                );
            }
        }
    }

    #[test]
    fn thrift_multi_column_with_stats() {
        // write_multi_column_parquet produces: ts(Timestamp), int_val(Int), dbl_val(Double), flag(Boolean)
        let parquet_data = write_multi_column_parquet(50);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let thrift_meta = metadata.into_thrift();

        let block = build_row_group_block_from_thrift(&thrift_meta.row_groups[0]).unwrap();

        assert_eq!(block.num_rows(), 50);

        // ts column (index 0): Timestamp = 8 bytes, stats should be inlined.
        let ts_chunk = block.column_chunk_raw(0);
        let ts_flags = StatFlags(ts_chunk.stat_flags);
        assert!(ts_flags.has_min_stat(), "ts should have min stat");
        assert!(ts_flags.is_min_inlined(), "ts min should be inlined");
        assert!(ts_flags.has_max_stat(), "ts should have max stat");
        assert!(ts_flags.is_max_inlined(), "ts max should be inlined");
        assert!(ts_flags.has_null_count(), "ts should have null count");
        assert_eq!(ts_chunk.null_count, 0);
        // min=0, max=49 (microseconds)
        assert_eq!(ts_chunk.min_stat, 0);
        assert_eq!(ts_chunk.max_stat, 49);

        // int_val column (index 1): Int = 4 bytes, stats inlined. Data: 0..49.
        let int_chunk = block.column_chunk_raw(1);
        let int_flags = StatFlags(int_chunk.stat_flags);
        assert!(int_flags.has_min_stat());
        assert!(int_flags.is_min_inlined());
        assert_eq!(int_chunk.min_stat as i32, 0);
        assert_eq!(int_chunk.max_stat as i32, 49);

        // dbl_val column (index 2): Double = 8 bytes, stats inlined.
        let dbl_chunk = block.column_chunk_raw(2);
        let dbl_flags = StatFlags(dbl_chunk.stat_flags);
        assert!(dbl_flags.has_min_stat());
        assert!(dbl_flags.is_min_inlined());
        // Double 0.0 and 49*1.5=73.5 as bits
        assert_eq!(f64::from_le_bytes(dbl_chunk.min_stat.to_le_bytes()), 0.0);
        assert_eq!(f64::from_le_bytes(dbl_chunk.max_stat.to_le_bytes()), 73.5);

        // flag column (index 3): Boolean = 1 byte, stats inlined.
        let flag_chunk = block.column_chunk_raw(3);
        let flag_flags = StatFlags(flag_chunk.stat_flags);
        assert!(flag_flags.has_min_stat());
        assert!(flag_flags.is_min_inlined());
    }

    #[test]
    fn thrift_with_compression() {
        let parquet_data = write_test_parquet(100, CompressionOptions::Snappy);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let thrift_meta = metadata.into_thrift();

        let block = build_row_group_block_from_thrift(&thrift_meta.row_groups[0]).unwrap();

        let chunk = block.column_chunk_raw(0);
        assert_eq!(chunk.codec().unwrap(), Codec::Snappy);
    }

    #[test]
    fn thrift_without_qdb_meta_infers_types() {
        // Without QdbMeta, the convert path passes parquet stat bytes through
        // verbatim regardless. write_multi_column_parquet: ts(Timestamp),
        // int_val(Int), dbl_val(Double), flag(Boolean).
        let parquet_data = write_multi_column_parquet(30);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let thrift_meta = metadata.into_thrift();

        let block = build_row_group_block_from_thrift(&thrift_meta.row_groups[0]).unwrap();

        // Stats should still be inlined for fixed-size types.
        let ts_chunk = block.column_chunk_raw(0);
        let ts_flags = StatFlags(ts_chunk.stat_flags);
        assert!(ts_flags.has_min_stat(), "inferred ts should have min stat");
        assert!(
            ts_flags.is_min_inlined(),
            "inferred ts min should be inlined"
        );

        // int_val (index 1): inferred as Int, 4 bytes, should inline.
        let int_chunk = block.column_chunk_raw(1);
        let int_flags = StatFlags(int_chunk.stat_flags);
        assert!(
            int_flags.has_min_stat(),
            "inferred int should have min stat"
        );
        assert!(
            int_flags.is_min_inlined(),
            "inferred int min should be inlined"
        );

        // dbl_val (index 2): inferred as Double, 8 bytes, should inline.
        let dbl_chunk = block.column_chunk_raw(2);
        let dbl_flags = StatFlags(dbl_chunk.stat_flags);
        assert!(
            dbl_flags.has_min_stat(),
            "inferred double should have min stat"
        );
        assert!(
            dbl_flags.is_min_inlined(),
            "inferred double min should be inlined"
        );

        // flag (index 3): inferred as Boolean, 1 byte, should inline.
        let flag_chunk = block.column_chunk_raw(3);
        let flag_flags = StatFlags(flag_chunk.stat_flags);
        assert!(
            flag_flags.has_min_stat(),
            "inferred bool should have min stat"
        );
        assert!(
            flag_flags.is_min_inlined(),
            "inferred bool min should be inlined"
        );
    }

    #[test]
    fn thrift_multiple_row_groups() {
        // Small row_group_size to force multiple row groups.
        let ts_data: Vec<i64> = (0..100i64).collect();
        let ts_bytes: &'static [u8] = Box::leak(
            unsafe { std::slice::from_raw_parts(ts_data.as_ptr() as *const u8, ts_data.len() * 8) }
                .to_vec()
                .into_boxed_slice(),
        );

        let partition = Partition {
            table: "test".to_string(),
            columns: vec![Column {
                name: "ts",
                data_type: ColumnTypeTag::Timestamp.into_type(),
                id: 0,
                row_count: 100,
                primary_data: ts_bytes,
                secondary_data: &[],
                symbol_offsets: &[],
                column_top: 0,
                designated_timestamp: true,
                not_null_hint: true,
                strided_timestamp_16: false,
                designated_timestamp_ascending: true,
                parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
            }],
        };

        let mut buf = Vec::new();
        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_version(Version::V1)
            .with_row_group_size(Some(30)) // 100 rows / 30 = 4 row groups
            .finish(partition)
            .unwrap();

        let mut cursor = Cursor::new(&buf);
        let metadata = read_metadata_with_size(&mut cursor, buf.len() as u64).unwrap();
        let thrift_meta = metadata.into_thrift();

        assert_eq!(thrift_meta.row_groups.len(), 4);
        let expected_rows = [30u64, 30, 30, 10];

        for (i, thrift_rg) in thrift_meta.row_groups.iter().enumerate() {
            let block = build_row_group_block_from_thrift(thrift_rg).unwrap();
            assert_eq!(
                block.num_rows(),
                expected_rows[i],
                "row count mismatch at rg {i}"
            );
        }
    }

    #[test]
    fn generate_parquet_metadata_produces_valid_pm() {
        let parquet_data = write_multi_column_parquet(80);
        let file_size = parquet_data.len() as u64;
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, file_size).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);
        let thrift_meta = metadata.into_thrift();

        // Reload metadata for schema_columns.
        let mut cursor2 = Cursor::new(&parquet_data);
        let metadata2 = read_metadata_with_size(&mut cursor2, parquet_data.len() as u64).unwrap();
        let schema_columns = metadata2.schema_descr.columns();

        // Build column infos from QdbMeta (simulating what update.rs does).
        let col_infos: Vec<ParquetMetaColumnInfo> = schema_columns
            .iter()
            .enumerate()
            .map(|(i, col_desc)| {
                let field_info = col_desc.base_type.get_field_info();
                let cm = qdb_meta.as_ref().and_then(|m| m.schema.get(i));
                let col_type_code = cm.map(|c| c.column_type.code()).unwrap_or_else(|| {
                    crate::parquet_read::meta::infer_column_type(col_desc)
                        .map(|ct| ct.code())
                        .unwrap_or(-1)
                });
                let mut flags = ColumnFlags::new();
                flags = flags.with_repetition(FieldRepetition::from(field_info.repetition));
                ParquetMetaColumnInfo {
                    name: &field_info.name,
                    col_type_code,
                    id: field_info.id.unwrap_or(-1),
                    flags,
                    fixed_byte_len: match col_desc.descriptor.primitive_type.physical_type {
                        PhysicalType::FixedLenByteArray(len) => len as i32,
                        _ => 0,
                    },
                    physical_type: physical_type_to_u8(
                        col_desc.descriptor.primitive_type.physical_type,
                    ),
                    max_rep_level: col_desc.descriptor.max_rep_level as u8,
                    max_def_level: col_desc.descriptor.max_def_level as u8,
                }
            })
            .collect();

        let parquet_footer_offset = 100u64;
        let parquet_footer_length = 50u32;

        let (parquet_meta_bytes, parquet_meta_file_size) = generate_parquet_metadata(
            &col_infos,
            &thrift_meta.row_groups,
            0,    // designated_timestamp = column 0
            &[0], // sorting on column 0
            parquet_footer_offset,
            parquet_footer_length,
            &[],
            0,
            -1,
        )
        .unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        assert_eq!(reader.column_count(), 4);
        assert_eq!(reader.row_group_count(), 1);
        assert_eq!(reader.designated_timestamp(), Some(0));
        assert_eq!(reader.sorting_column_count(), 1);
        assert_eq!(reader.parquet_footer_offset(), parquet_footer_offset);
        assert_eq!(reader.parquet_footer_length(), parquet_footer_length);
        assert_eq!(reader.column_name(0).unwrap(), "ts");
        assert_eq!(reader.column_name(1).unwrap(), "int_val");

        let rg = reader.row_group(0).unwrap();
        assert_eq!(rg.num_rows(), 80);
        assert!(reader.verify_checksum().is_ok());
        assert_eq!(parquet_meta_file_size, parquet_meta_bytes.len() as u64);
    }

    #[test]
    fn update_parquet_metadata_appends_new_row_group() {
        // Create initial _pm with 1 row group.
        let parquet_data = write_multi_column_parquet(80);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);
        let thrift_meta = metadata.into_thrift();

        let mut cursor2 = Cursor::new(&parquet_data);
        let metadata2 = read_metadata_with_size(&mut cursor2, parquet_data.len() as u64).unwrap();
        let schema_columns = metadata2.schema_descr.columns();

        let col_infos: Vec<ParquetMetaColumnInfo> = schema_columns
            .iter()
            .enumerate()
            .map(|(i, col_desc)| {
                let field_info = col_desc.base_type.get_field_info();
                let cm = qdb_meta.as_ref().and_then(|m| m.schema.get(i));
                let mut flags = ColumnFlags::new();
                flags = flags.with_repetition(FieldRepetition::from(field_info.repetition));
                ParquetMetaColumnInfo {
                    name: &field_info.name,
                    col_type_code: cm.map(|c| c.column_type.code()).unwrap_or(-1),
                    id: field_info.id.unwrap_or(-1),
                    flags,
                    fixed_byte_len: match col_desc.descriptor.primitive_type.physical_type {
                        PhysicalType::FixedLenByteArray(len) => len as i32,
                        _ => 0,
                    },
                    physical_type: physical_type_to_u8(
                        col_desc.descriptor.primitive_type.physical_type,
                    ),
                    max_rep_level: col_desc.descriptor.max_rep_level as u8,
                    max_def_level: col_desc.descriptor.max_def_level as u8,
                }
            })
            .collect();

        let (initial_pm, _) = generate_parquet_metadata(
            &col_infos,
            &thrift_meta.row_groups,
            0,
            &[0],
            100,
            50,
            &[],
            0,
            -1,
        )
        .unwrap();

        let initial_size = initial_pm.len() as u64;
        let initial_reader = ParquetMetaReader::from_file_size(&initial_pm, initial_size).unwrap();
        assert_eq!(initial_reader.row_group_count(), 1);

        // Now simulate adding a second row group (same columns, different data offset).
        // Clone the first row group and change its data_page_offset to simulate a new one.
        let mut extended_rgs = thrift_meta.row_groups.clone();
        let mut new_rg = extended_rgs[0].clone();
        for col in &mut new_rg.columns {
            if let Some(ref mut meta) = col.meta_data {
                meta.data_page_offset += 10_000; // Different fingerprint.
            }
        }
        extended_rgs.push(new_rg);

        let result = update_parquet_metadata(
            &initial_pm,
            initial_size,
            initial_size,
            &extended_rgs,
            200,
            60,
            &[],
            0,
        )
        .unwrap();

        assert!(!result.bytes.is_empty(), "should have append bytes");

        // Build the full file: initial + append. Mimic the Java side by
        // patching the header's parquet_meta_file_size last — this is the MVCC
        // commit signal that publishes the new snapshot.
        let mut full_file = initial_pm.clone();
        full_file.extend_from_slice(&result.bytes);
        assert_eq!(full_file.len() as u64, result.new_file_size);
        full_file[crate::parquet_metadata::types::HEADER_PARQUET_META_FILE_SIZE_OFF
            ..crate::parquet_metadata::types::HEADER_PARQUET_META_FILE_SIZE_OFF + 8]
            .copy_from_slice(&result.new_file_size.to_le_bytes());

        // New reader should see 2 row groups.
        let new_reader =
            ParquetMetaReader::from_file_size(&full_file, result.new_file_size).unwrap();
        assert_eq!(new_reader.row_group_count(), 2);
        assert!(new_reader.verify_checksum().is_ok());

        // Old reader pinned to the old size must still see 1 row group.
        // MVCC on the old snapshot uses the initial slice (with the
        // original parquet_meta_file_size still in the header), which the caller
        // obtained before the new snapshot was published.
        let old_reader = ParquetMetaReader::from_file_size(&initial_pm, initial_size).unwrap();
        assert_eq!(old_reader.row_group_count(), 1);
    }

    #[test]
    fn update_parquet_metadata_appends_past_dead_footer() {
        let parquet_data = write_multi_column_parquet(80);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);
        let thrift_meta = metadata.into_thrift();

        let mut cursor2 = Cursor::new(&parquet_data);
        let metadata2 = read_metadata_with_size(&mut cursor2, parquet_data.len() as u64).unwrap();
        let col_infos = col_infos_from_schema(metadata2.schema_descr.columns(), qdb_meta.as_ref());

        let (initial_pm, _) = generate_parquet_metadata(
            &col_infos,
            &thrift_meta.row_groups,
            0,
            &[0],
            100,
            50,
            &[],
            0,
            -1,
        )
        .unwrap();
        let s0 = initial_pm.len() as u64;

        // Two-row-group target: the original row group (unchanged) plus a new one.
        let mut extended_rgs = thrift_meta.row_groups.clone();
        let mut new_rg = extended_rgs[0].clone();
        for col in &mut new_rg.columns {
            if let Some(ref mut meta) = col.meta_data {
                meta.data_page_offset += 10_000; // different fingerprint -> treated as new
            }
        }
        extended_rgs.push(new_rg);

        // Crash window: a prior update published a footer at [s0, s1) (header
        // patched to s1) then crashed before its `_txn` commit. The committed
        // head stays s0 (the dead footer never matched `_txn`).
        let dead = update_parquet_metadata(&initial_pm, s0, s0, &extended_rgs, 200, 60, &[], 0)
            .unwrap()
            .bytes;
        let mut physical_after_fail = initial_pm.clone();
        physical_after_fail.extend_from_slice(&dead);
        let s1 = physical_after_fail.len() as u64;
        assert!(s1 > s0, "dead footer must extend the physical file");

        // The next successful update parses the committed head (s0) and appends
        // past the dead footer at the append base (the dirty-ahead header, s1).
        let result =
            update_parquet_metadata(&physical_after_fail, s0, s1, &extended_rgs, 300, 70, &[], 0)
                .unwrap();
        assert_eq!(result.new_file_size, s1 + result.bytes.len() as u64);

        let mut full = physical_after_fail.clone();
        full.extend_from_slice(&result.bytes);
        full[crate::parquet_metadata::types::HEADER_PARQUET_META_FILE_SIZE_OFF
            ..crate::parquet_metadata::types::HEADER_PARQUET_META_FILE_SIZE_OFF + 8]
            .copy_from_slice(&result.new_file_size.to_le_bytes());

        // The writer appended only past s1: committed + dead bytes untouched
        // (bar the header size field), and the dead footer is byte-identical.
        assert_eq!(&full[8..s1 as usize], &physical_after_fail[8..s1 as usize]);
        assert_eq!(&full[s0 as usize..s1 as usize], &dead[..]);

        // A reader at the new committed size verifies the cumulative CRC, which
        // now spans the dead region, and the new footer chains onto s0 --
        // orphaning the dead footer out of the MVCC chain.
        let new_reader = ParquetMetaReader::from_file_size(&full, result.new_file_size).unwrap();
        new_reader.verify_checksum().unwrap();
        assert_eq!(new_reader.row_group_count(), 2);
        assert_eq!(new_reader.prev_parquet_meta_file_size(), s0);

        // The committed snapshot still resolves independently at s0.
        let old_reader = ParquetMetaReader::from_file_size(&initial_pm, s0).unwrap();
        assert_eq!(old_reader.row_group_count(), 1);
        old_reader.verify_checksum().unwrap();
    }

    /// Builds a `Vec<ParquetMetaColumnInfo>` from the given parquet schema and
    /// optional `QdbMeta`, with `top == 0` for every column. The caller can
    /// stamp non-zero tops onto specific entries afterwards.
    fn col_infos_from_schema<'a>(
        schema_columns: &'a [parquet2::metadata::ColumnDescriptor],
        qdb_meta: Option<&'a QdbMeta>,
    ) -> Vec<ParquetMetaColumnInfo<'a>> {
        schema_columns
            .iter()
            .enumerate()
            .map(|(i, col_desc)| {
                let field_info = col_desc.base_type.get_field_info();
                let cm = qdb_meta.and_then(|m| m.schema.get(i));
                let mut flags = ColumnFlags::new();
                flags = flags.with_repetition(FieldRepetition::from(field_info.repetition));
                ParquetMetaColumnInfo {
                    name: &field_info.name,
                    col_type_code: cm.map(|c| c.column_type.code()).unwrap_or(-1),
                    id: field_info.id.unwrap_or(-1),
                    flags,
                    fixed_byte_len: match col_desc.descriptor.primitive_type.physical_type {
                        PhysicalType::FixedLenByteArray(len) => len as i32,
                        _ => 0,
                    },
                    physical_type: physical_type_to_u8(
                        col_desc.descriptor.primitive_type.physical_type,
                    ),
                    max_rep_level: col_desc.descriptor.max_rep_level as u8,
                    max_def_level: col_desc.descriptor.max_def_level as u8,
                }
            })
            .collect()
    }

    #[test]
    fn update_with_decreasing_row_group_count_returns_error() {
        // Build an initial `_pm` with 3 row groups, then call
        // `update_parquet_metadata` with only 2. The updater can grow or
        // replace row groups but cannot drop one, so it must error.
        let parquet_data = write_multi_column_parquet(80);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);
        let thrift_meta = metadata.into_thrift();

        let mut cursor2 = Cursor::new(&parquet_data);
        let metadata2 = read_metadata_with_size(&mut cursor2, parquet_data.len() as u64).unwrap();
        let schema_columns = metadata2.schema_descr.columns();

        let col_infos = col_infos_from_schema(schema_columns, qdb_meta.as_ref());

        // Build 3 row groups by cloning the first one with shifted page offsets.
        let mut three_rgs = thrift_meta.row_groups.clone();
        let mut second_rg = three_rgs[0].clone();
        for col in &mut second_rg.columns {
            if let Some(ref mut meta) = col.meta_data {
                meta.data_page_offset += 10_000;
            }
        }
        let mut third_rg = three_rgs[0].clone();
        for col in &mut third_rg.columns {
            if let Some(ref mut meta) = col.meta_data {
                meta.data_page_offset += 20_000;
            }
        }
        three_rgs.push(second_rg);
        three_rgs.push(third_rg);
        assert_eq!(three_rgs.len(), 3);

        let (initial_pm, _) =
            generate_parquet_metadata(&col_infos, &three_rgs, 0, &[0], 100, 50, &[], 0, -1)
                .unwrap();
        let initial_size = initial_pm.len() as u64;

        let initial_reader = ParquetMetaReader::from_file_size(&initial_pm, initial_size).unwrap();
        assert_eq!(initial_reader.row_group_count(), 3);

        // Drop the third row group and ask for an update with only 2.
        let two_rgs = three_rgs[..2].to_vec();
        let result = update_parquet_metadata(
            &initial_pm,
            initial_size,
            initial_size,
            &two_rgs,
            100,
            50,
            &[],
            0,
        );

        let err = match result {
            Ok(_) => panic!("update should fail when row groups shrink"),
            Err(e) => e,
        };
        let msg = format!("{err}");
        assert!(
            msg.contains("cannot shrink row group count (3 -> 2)"),
            "expected 'cannot shrink row group count (3 -> 2)' in error, got: {msg}"
        );
        assert!(
            msg.contains("escalate to rewrite mode"),
            "expected escalation hint in error, got: {msg}"
        );
    }

    #[test]
    fn update_rejects_invalid_append_base() {
        // The append base comes from the on-disk `_pm` header; a corrupt value
        // must error (not panic via JNI) before any parsing. Build a valid
        // 1-row-group `_pm` and a 2-row-group target that would otherwise
        // append cleanly, then drive the guard with three bad append bases.
        let parquet_data = write_multi_column_parquet(80);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);
        let thrift_meta = metadata.into_thrift();

        let mut cursor2 = Cursor::new(&parquet_data);
        let metadata2 = read_metadata_with_size(&mut cursor2, parquet_data.len() as u64).unwrap();
        let col_infos = col_infos_from_schema(metadata2.schema_descr.columns(), qdb_meta.as_ref());

        let (initial_pm, _) = generate_parquet_metadata(
            &col_infos,
            &thrift_meta.row_groups,
            0,
            &[0],
            100,
            50,
            &[],
            0,
            -1,
        )
        .unwrap();
        let parse_anchor = initial_pm.len() as u64;

        // A 2-row-group target so the update is well-formed apart from the base.
        let mut extended_rgs = thrift_meta.row_groups.clone();
        let mut new_rg = extended_rgs[0].clone();
        for col in &mut new_rg.columns {
            if let Some(ref mut meta) = col.meta_data {
                meta.data_page_offset += 10_000;
            }
        }
        extended_rgs.push(new_rg);

        // parse_anchor - 1: below the committed head; parse_anchor + 1: one byte
        // past the buffer end; u64::MAX: still addressable as usize on 64-bit, so
        // it trips the buffer-length check rather than the try_from overflow.
        for &bad_base in &[parse_anchor - 1, parse_anchor + 1, u64::MAX] {
            let result = update_parquet_metadata(
                &initial_pm,
                parse_anchor,
                bad_base,
                &extended_rgs,
                200,
                60,
                &[],
                0,
            );
            let err = match result {
                Ok(_) => panic!("append base {bad_base} must be rejected"),
                Err(e) => e,
            };
            let msg = format!("{err}");
            assert!(
                msg.contains("append base") && msg.contains("out of range"),
                "append base {bad_base}: expected an 'out of range' guard error, got: {msg}"
            );
        }
    }

    #[test]
    fn thrift_missing_column_metadata_errors() {
        // A ColumnChunk with meta_data = None should produce a clear error.
        let rg = parquet2::thrift_format::RowGroup {
            columns: vec![parquet2::thrift_format::ColumnChunk {
                file_path: None,
                file_offset: 0,
                meta_data: None,
                offset_index_offset: None,
                offset_index_length: None,
                column_index_offset: None,
                column_index_length: None,
                crypto_metadata: None,
                encrypted_column_metadata: None,
            }],
            total_byte_size: 0,
            num_rows: 10,
            sorting_columns: None,
            file_offset: None,
            total_compressed_size: None,
            ordinal: None,
        };

        let result = build_row_group_block_from_thrift(&rg);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("no metadata"),
            "expected 'no metadata' error, got: {err_msg}"
        );
    }

    #[test]
    fn bloom_filter_extracted_from_parquet_into_pm() {
        // Write a parquet file with bloom filters enabled on column 0.
        let row_count = 100;
        let col_data: Vec<i64> = (0..row_count as i64).collect();
        let data_bytes: &[u8] = unsafe {
            std::slice::from_raw_parts(col_data.as_ptr() as *const u8, col_data.len() * 8)
        };
        let data_static: &'static [u8] = Box::leak(data_bytes.to_vec().into_boxed_slice());

        let col = Column {
            name: "ts",
            data_type: ColumnTypeTag::Timestamp.into_type(),
            id: 0,
            row_count,
            primary_data: data_static,
            secondary_data: &[],
            symbol_offsets: &[],
            column_top: 0,
            designated_timestamp: true,
            not_null_hint: true,
            strided_timestamp_16: false,
            designated_timestamp_ascending: true,
            parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
        };

        let partition = Partition {
            table: "test_bloom".to_string(),
            columns: vec![col],
        };
        let mut buf = Vec::new();
        let writer = ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_version(Version::V1)
            .with_bloom_filter_columns([0].into_iter().collect())
            .with_bloom_filter_fpp(0.01)
            .with_row_group_size(Some(row_count));

        // Use chunked API to capture bloom filter bitsets.
        let (schema, additional_meta) =
            crate::parquet_write::schema::to_parquet_schema(&partition, false, -1).unwrap();
        let encodings = crate::parquet_write::schema::to_encodings(&partition);
        let compressions = crate::parquet_write::schema::to_compressions(&partition);
        let mut chunked = writer
            .chunked_with_compressions(schema, encodings, compressions)
            .unwrap();
        chunked.write_chunk(&partition).unwrap();
        chunked.finish(additional_meta).unwrap();

        let bloom_bitsets = chunked.bloom_bitsets();
        assert!(
            bloom_bitsets.len() == 1 && bloom_bitsets[0][0].is_some(),
            "should have captured bloom filter bitset"
        );

        let col_infos = vec![ParquetMetaColumnInfo {
            name: "ts",
            col_type_code: ColumnTypeTag::Timestamp.into_type().code(),
            id: 0,
            flags: ColumnFlags::new().with_repetition(FieldRepetition::Required),
            fixed_byte_len: 0,
            physical_type: physical_type_to_u8(PhysicalType::Int64),
            max_rep_level: 0,
            max_def_level: 0,
        }];

        // Generate _pm with captured bloom filter bitsets.
        let (parquet_meta_bytes, parquet_meta_file_size) = generate_parquet_metadata(
            &col_infos,
            chunked.row_groups(),
            0,
            &[0],
            100,
            50,
            bloom_bitsets,
            0,
            -1,
        )
        .unwrap();

        // Verify the _pm has a bloom filter via the feature section.
        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        assert_eq!(reader.row_group_count(), 1);
        assert!(
            reader.has_bloom_filters(),
            "bloom filter feature flag should be set"
        );
        assert_eq!(reader.bloom_filter_position(0), Some(0));

        // Read the inlined bloom filter offset from the footer section.
        let bf_abs = reader.bloom_filter_offset_in_pm(0, 0).unwrap() as usize;
        assert_ne!(bf_abs, 0, "bloom filter offset should be non-zero");
        assert_eq!(
            bf_abs % 8,
            0,
            "bloom filter offset should be 8-byte aligned"
        );
        assert!(
            bf_abs + 4 <= parquet_meta_bytes.len(),
            "bloom filter offset should be within _pm bounds"
        );

        // Read the [i32 len][bitset] at the absolute offset.
        let bf_data = &parquet_meta_bytes[bf_abs..];
        let bf_len = i32::from_le_bytes(bf_data[..4].try_into().unwrap()) as usize;
        assert!(
            bf_len >= 32,
            "bloom filter bitset should be at least 32 bytes"
        );
        assert!(
            bf_abs + 4 + bf_len <= parquet_meta_bytes.len(),
            "bloom filter bitset should fit within _pm"
        );

        // The bitset should not be all zeros (we inserted 100 values).
        let bitset = &bf_data[4..4 + bf_len];
        assert!(
            bitset.iter().any(|&b| b != 0),
            "bloom filter bitset should contain set bits"
        );
    }

    /// Writes a parquet file with a bloom filter on the `id` column. Returns
    /// the raw parquet bytes; the migration path mmaps these bytes and hands
    /// them to `convert_from_parquet` as `parquet_file_data`.
    fn write_parquet_with_bloom_filter() -> Vec<u8> {
        let row_count = 100usize;
        let ts_data: Vec<i64> = (0..row_count as i64).collect();
        let ts_bytes: &'static [u8] = Box::leak(
            unsafe { std::slice::from_raw_parts(ts_data.as_ptr() as *const u8, ts_data.len() * 8) }
                .to_vec()
                .into_boxed_slice(),
        );
        let id_data: Vec<i32> = (0..row_count as i32).collect();
        let id_bytes: &'static [u8] = Box::leak(
            unsafe { std::slice::from_raw_parts(id_data.as_ptr() as *const u8, id_data.len() * 4) }
                .to_vec()
                .into_boxed_slice(),
        );

        let cols = vec![
            Column {
                name: "ts",
                data_type: ColumnTypeTag::Timestamp.into_type(),
                id: 0,
                row_count,
                primary_data: ts_bytes,
                secondary_data: &[],
                symbol_offsets: &[],
                column_top: 0,
                designated_timestamp: true,
                not_null_hint: true,
                strided_timestamp_16: false,
                designated_timestamp_ascending: true,
                parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
            },
            Column {
                name: "id",
                data_type: ColumnTypeTag::Int.into_type(),
                id: 1,
                row_count,
                primary_data: id_bytes,
                secondary_data: &[],
                symbol_offsets: &[],
                column_top: 0,
                designated_timestamp: false,
                not_null_hint: true,
                strided_timestamp_16: false,
                designated_timestamp_ascending: false,
                parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
            },
        ];
        let partition = Partition { table: "bloom".to_string(), columns: cols };

        let mut buf = Vec::new();
        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_version(Version::V1)
            .with_row_group_size(Some(row_count))
            .with_bloom_filter_columns([1].into_iter().collect())
            .with_bloom_filter_fpp(0.01)
            .finish(partition)
            .unwrap();
        buf
    }

    /// Migration path: when the caller passes the parquet bytes, the bloom
    /// filter bitset is read out of the parquet footer and inlined into the
    /// `_pm` out-of-line region. Covers the new `parquet_file_data: Some(...)`
    /// branch in `convert_from_parquet`.
    #[test]
    fn convert_from_parquet_inlines_bloom_filter_from_slice() {
        let parquet_data = write_parquet_with_bloom_filter();

        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);

        // Sanity: the parquet footer carries a bloom_filter_offset for `id`.
        let id_meta = metadata.row_groups[0].columns()[1].metadata();
        assert!(
            id_meta.bloom_filter_offset.is_some_and(|o| o > 0),
            "test parquet should carry a bloom_filter_offset on the id column"
        );

        let (pm_bytes, pm_file_size) = convert_from_parquet(
            &metadata,
            qdb_meta.as_ref(),
            0,
            0,
            None,
            Some(parquet_data.as_slice()),
        )
        .unwrap();

        let reader = ParquetMetaReader::from_file_size(&pm_bytes, pm_file_size).unwrap();
        assert!(
            reader.has_bloom_filters(),
            "BLOOM_FILTERS feature flag should be set when bloom filters are inlined"
        );
        // Column 0 (ts) has no bloom filter; column 1 (id) does. The footer
        // section is one entry deep.
        assert_eq!(reader.bloom_filter_position(0), None);
        let id_pos = reader
            .bloom_filter_position(1)
            .expect("id column should have a bloom filter footer entry");
        let bf_abs = reader.bloom_filter_offset_in_pm(0, id_pos).unwrap() as usize;
        assert_ne!(bf_abs, 0, "inlined bloom filter offset should be non-zero");
        let bf_len = i32::from_le_bytes(pm_bytes[bf_abs..bf_abs + 4].try_into().unwrap()) as usize;
        assert!(bf_len >= 32, "inlined bitset must be at least 32 bytes");
        let inlined_bitset = &pm_bytes[bf_abs + 4..bf_abs + 4 + bf_len];
        assert!(
            inlined_bitset.iter().any(|&b| b != 0),
            "inlined bloom bitset should have at least one bit set for 100 values"
        );
    }

    /// Migration path: when the caller does NOT pass parquet bytes (i.e. it
    /// has not mmapped the file), bloom filters are not inlined and `_pm`
    /// does not carry the BLOOM_FILTERS feature flag, even though the parquet
    /// footer would have allowed it. Anchors the `parquet_file_data: None`
    /// branch so a future change that flips it on by default fails the test.
    #[test]
    fn convert_from_parquet_skips_bloom_inline_without_parquet_data() {
        let parquet_data = write_parquet_with_bloom_filter();

        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);

        let (pm_bytes, pm_file_size) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0, None, None).unwrap();

        let reader = ParquetMetaReader::from_file_size(&pm_bytes, pm_file_size).unwrap();
        assert!(
            !reader.has_bloom_filters(),
            "BLOOM_FILTERS flag must stay off when parquet_file_data is None"
        );
    }

    /// Migration path error case: the caller passes a truncated view of the
    /// parquet file that does not extend to the bloom_filter_offset recorded
    /// in the footer. `parquet2::bloom_filter::read_from_slice_at_offset`
    /// rejects this, the converter wraps the error with a Conversion kind,
    /// and the failure surfaces to the caller rather than silently producing
    /// a `_pm` without the bloom filter the footer claims. Covers the
    /// `map_err` branch in the bloom-inline block.
    #[test]
    fn convert_from_parquet_propagates_bloom_read_error_on_truncated_slice() {
        let parquet_data = write_parquet_with_bloom_filter();

        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);

        let bloom_offset = metadata.row_groups[0].columns()[1]
            .metadata()
            .bloom_filter_offset
            .expect("test parquet should carry a bloom_filter_offset")
            as usize;
        // Truncate the parquet bytes before the bloom-filter region so the
        // converter's read of the bitset at `bloom_offset` fails.
        let truncated = &parquet_data[..bloom_offset.saturating_sub(1)];

        let err = convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0, None, Some(truncated))
            .expect_err("truncated parquet_file_data should fail bloom-filter read");

        let msg = format!("{err}");
        assert!(
            msg.contains("could not read parquet bloom filter at offset"),
            "error message should mention bloom filter read failure, got: {msg}"
        );
    }

    /// Migration path: a parquet with two bloom-filtered columns split across
    /// two row groups must inline a distinct bitset for every (row group,
    /// column) pair. Guards against a regression that copies one bitset to all
    /// row groups, drops all but the last, or overlaps per-column bitsets in
    /// the `_pm` out-of-line region.
    #[test]
    fn convert_from_parquet_inlines_bloom_filters_across_row_groups_and_columns() {
        let parquet_data = write_parquet_with_two_bloom_columns_two_row_groups();

        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);
        assert_eq!(
            metadata.row_groups.len(),
            2,
            "test parquet should have two row groups"
        );

        let (pm_bytes, pm_file_size) = convert_from_parquet(
            &metadata,
            qdb_meta.as_ref(),
            0,
            0,
            None,
            Some(&parquet_data),
        )
        .unwrap();

        let reader = ParquetMetaReader::from_file_size(&pm_bytes, pm_file_size).unwrap();
        assert!(reader.has_bloom_filters());
        assert_eq!(
            reader.bloom_filter_position(0),
            None,
            "ts column carries no bloom filter"
        );
        let pos_a = reader
            .bloom_filter_position(1)
            .expect("column 1 should have a bloom filter");
        let pos_b = reader
            .bloom_filter_position(2)
            .expect("column 2 should have a bloom filter");

        let mut offsets = Vec::with_capacity(4);
        for rg in 0..2usize {
            for pos in [pos_a, pos_b] {
                let off = reader.bloom_filter_offset_in_pm(rg, pos).unwrap() as usize;
                assert_ne!(off, 0, "each (row group, column) pair must inline a bitset");
                let len = i32::from_le_bytes(pm_bytes[off..off + 4].try_into().unwrap()) as usize;
                assert!(len >= 32, "inlined bitset must be at least 32 bytes");
                let bitset = &pm_bytes[off + 4..off + 4 + len];
                assert!(
                    bitset.iter().any(|&b| b != 0),
                    "inlined bloom bitset should have at least one bit set"
                );
                offsets.push(off);
            }
        }
        let mut distinct = offsets.clone();
        distinct.sort_unstable();
        distinct.dedup();
        assert_eq!(
            distinct.len(),
            offsets.len(),
            "every (row group, column) bloom bitset must occupy a distinct _pm offset"
        );
    }

    fn write_parquet_with_two_bloom_columns_two_row_groups() -> Vec<u8> {
        let row_count = 100usize;
        let ts_data: Vec<i64> = (0..row_count as i64).collect();
        let ts_bytes: &'static [u8] = Box::leak(
            unsafe { std::slice::from_raw_parts(ts_data.as_ptr() as *const u8, ts_data.len() * 8) }
                .to_vec()
                .into_boxed_slice(),
        );
        let a_data: Vec<i32> = (0..row_count as i32).collect();
        let a_bytes: &'static [u8] = Box::leak(
            unsafe { std::slice::from_raw_parts(a_data.as_ptr() as *const u8, a_data.len() * 4) }
                .to_vec()
                .into_boxed_slice(),
        );
        let b_data: Vec<i32> = (0..row_count as i32)
            .map(|x| x.wrapping_mul(7) + 3)
            .collect();
        let b_bytes: &'static [u8] = Box::leak(
            unsafe { std::slice::from_raw_parts(b_data.as_ptr() as *const u8, b_data.len() * 4) }
                .to_vec()
                .into_boxed_slice(),
        );

        let cols = vec![
            Column {
                name: "ts",
                data_type: ColumnTypeTag::Timestamp.into_type(),
                id: 0,
                row_count,
                primary_data: ts_bytes,
                secondary_data: &[],
                symbol_offsets: &[],
                column_top: 0,
                designated_timestamp: true,
                not_null_hint: true,
                strided_timestamp_16: false,
                designated_timestamp_ascending: true,
                parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
            },
            Column {
                name: "a",
                data_type: ColumnTypeTag::Int.into_type(),
                id: 1,
                row_count,
                primary_data: a_bytes,
                secondary_data: &[],
                symbol_offsets: &[],
                column_top: 0,
                designated_timestamp: false,
                not_null_hint: true,
                strided_timestamp_16: false,
                designated_timestamp_ascending: false,
                parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
            },
            Column {
                name: "b",
                data_type: ColumnTypeTag::Int.into_type(),
                id: 2,
                row_count,
                primary_data: b_bytes,
                secondary_data: &[],
                symbol_offsets: &[],
                column_top: 0,
                designated_timestamp: false,
                not_null_hint: true,
                strided_timestamp_16: false,
                designated_timestamp_ascending: false,
                parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
            },
        ];
        let partition = Partition { table: "bloom".to_string(), columns: cols };

        let mut buf = Vec::new();
        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_version(Version::V1)
            // Half the rows per group yields two row groups.
            .with_row_group_size(Some(row_count / 2))
            .with_bloom_filter_columns([1, 2].into_iter().collect())
            .with_bloom_filter_fpp(0.01)
            .finish(partition)
            .unwrap();
        buf
    }

    #[test]
    fn uuid_ool_stats_round_trip() {
        // Write a parquet file with a UUID column + timestamp column.
        let row_count = 10usize;
        // UUID data: 16 bytes per value. [hi: u64, lo: u64] in LE.
        let uuid_data: Vec<[u8; 16]> = (0..row_count)
            .map(|i| {
                let mut buf = [0u8; 16];
                // Store as (hi=0, lo=i+1) — simple ascending UUIDs.
                buf[0..8].copy_from_slice(&((i as u64 + 1) * 0x1111).to_le_bytes());
                buf[8..16].copy_from_slice(&0u64.to_le_bytes());
                buf
            })
            .collect();
        let uuid_bytes = leak_bytes(unsafe {
            std::slice::from_raw_parts(uuid_data.as_ptr() as *const u8, row_count * 16)
        });

        let ts_data: Vec<i64> = (0..row_count as i64).collect();
        let ts_bytes = leak_bytes(unsafe {
            std::slice::from_raw_parts(ts_data.as_ptr() as *const u8, ts_data.len() * 8)
        });

        let cols = vec![
            Column {
                name: "ts",
                data_type: ColumnTypeTag::Timestamp.into_type(),
                id: 0,
                row_count,
                primary_data: ts_bytes,
                secondary_data: &[],
                symbol_offsets: &[],
                column_top: 0,
                designated_timestamp: true,
                not_null_hint: true,
                strided_timestamp_16: false,
                designated_timestamp_ascending: true,
                parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
            },
            Column {
                name: "val",
                data_type: ColumnTypeTag::Uuid.into_type(),
                id: 1,
                row_count,
                primary_data: uuid_bytes,
                secondary_data: &[],
                symbol_offsets: &[],
                column_top: 0,
                designated_timestamp: false,
                not_null_hint: false,
                strided_timestamp_16: false,
                designated_timestamp_ascending: false,
                parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
            },
        ];
        let partition = Partition { table: "test".to_string(), columns: cols };
        let mut buf = Vec::new();
        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_version(Version::V1)
            .with_row_group_size(Some(row_count))
            .finish(partition)
            .unwrap();

        // Read parquet metadata.
        let mut cursor = Cursor::new(&buf);
        let metadata = read_metadata_with_size(&mut cursor, buf.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);
        let thrift_meta = metadata.into_thrift();

        let mut cursor2 = Cursor::new(&buf);
        let metadata2 = read_metadata_with_size(&mut cursor2, buf.len() as u64).unwrap();
        let schema_columns = metadata2.schema_descr.columns();

        let col_infos: Vec<ParquetMetaColumnInfo> = schema_columns
            .iter()
            .enumerate()
            .map(|(i, col_desc)| {
                let field_info = col_desc.base_type.get_field_info();
                let cm = qdb_meta.as_ref().and_then(|m| m.schema.get(i));
                let col_type_code = cm.map(|c| c.column_type.code()).unwrap_or(-1);
                let mut flags = ColumnFlags::new();
                flags = flags.with_repetition(FieldRepetition::from(field_info.repetition));
                ParquetMetaColumnInfo {
                    name: &field_info.name,
                    col_type_code,
                    id: field_info.id.unwrap_or(-1),
                    flags,
                    fixed_byte_len: match col_desc.descriptor.primitive_type.physical_type {
                        PhysicalType::FixedLenByteArray(len) => len as i32,
                        _ => 0,
                    },
                    physical_type: physical_type_to_u8(
                        col_desc.descriptor.primitive_type.physical_type,
                    ),
                    max_rep_level: col_desc.descriptor.max_rep_level as u8,
                    max_def_level: col_desc.descriptor.max_def_level as u8,
                }
            })
            .collect();

        let (parquet_meta_bytes, _) = generate_parquet_metadata(
            &col_infos,
            &thrift_meta.row_groups,
            0,
            &[0],
            100,
            50,
            &[],
            0,
            -1,
        )
        .unwrap();

        // Read _pm and check UUID column stats.
        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_bytes.len() as u64)
                .unwrap();
        assert_eq!(reader.column_count(), 2);
        assert_eq!(reader.column_name(1).unwrap(), "val");

        let rg = reader.row_group(0).unwrap();
        let chunk = rg.column_chunk(1).unwrap(); // UUID column at index 1
        let stat_flags = StatFlags(chunk.stat_flags);

        let ool = rg.out_of_line_region();

        assert!(
            stat_flags.has_min_stat(),
            "UUID column should have min stat"
        );
        assert!(
            !stat_flags.is_min_inlined(),
            "UUID stat should be OOL (not inlined)"
        );
        assert!(
            stat_flags.has_max_stat(),
            "UUID column should have max stat"
        );
        assert!(
            !stat_flags.is_max_inlined(),
            "UUID stat should be OOL (not inlined)"
        );
        assert!(!ool.is_empty(), "OOL region should contain stat data");

        // Read OOL stat bytes. Encoding: (offset << 16) | length.
        let min_off = (chunk.min_stat >> 16) as usize;
        let min_len = (chunk.min_stat & 0xFFFF) as usize;
        let max_off = (chunk.max_stat >> 16) as usize;
        let max_len = (chunk.max_stat & 0xFFFF) as usize;
        assert_eq!(min_len, 16, "UUID OOL min stat should be 16 bytes");
        assert_eq!(max_len, 16, "UUID OOL max stat should be 16 bytes");
        let min_bytes = &ool[min_off..min_off + min_len];
        let max_bytes = &ool[max_off..max_off + max_len];
        assert_ne!(min_bytes, max_bytes, "min and max OOL stats should differ");
    }

    // Backfill tests below drive the `ts_stats_backfill` parameter of
    // `convert_from_parquet` with stub closures. A matching QdbMeta marks the
    // ts column as designated so `detect_designated_timestamp` picks it up
    // regardless of the parquet's own `designated_timestamp` flag.

    /// Writes a parquet file with a single i64 "ts" column holding no stats
    /// and a hand-built QdbMeta that marks col 0 as the designated timestamp.
    /// The column has `designated_timestamp: false` so the writer's hard-coded
    /// "designated ts always gets stats" override at `parquet_write/file.rs`
    /// does not fire; `with_statistics(false)` then suppresses all stats. The
    /// QdbMeta is injected via `ChunkedWriter::finish(additional_meta)`.
    fn write_parquet_without_ts_stats(
        row_count: usize,
        rows_per_group: usize,
    ) -> (Vec<u8>, QdbMeta) {
        use crate::parquet::qdb_metadata::{QdbMetaCol, QDB_META_KEY};
        use crate::parquet_write::schema::{to_compressions, to_encodings, to_parquet_schema};
        use parquet2::metadata::KeyValue;

        let col_data: Vec<i64> = (0..row_count as i64).collect();
        let data_bytes: &[u8] = unsafe {
            std::slice::from_raw_parts(col_data.as_ptr() as *const u8, col_data.len() * 8)
        };
        let data_static: &'static [u8] = Box::leak(data_bytes.to_vec().into_boxed_slice());

        let col = Column {
            id: 0,
            name: "ts",
            data_type: ColumnTypeTag::Timestamp.into_type(),
            row_count,
            primary_data: data_static,
            secondary_data: &[],
            symbol_offsets: &[],
            column_top: 0,
            designated_timestamp: false,
            not_null_hint: false,
            strided_timestamp_16: false,
            designated_timestamp_ascending: true,
            parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
        };
        let partition = Partition { table: "test".to_string(), columns: vec![col] };

        let (schema, _empty_meta) = to_parquet_schema(&partition, false, -1).unwrap();
        let encodings = to_encodings(&partition);
        let compressions = to_compressions(&partition);

        let mut parquet_buf = Vec::new();
        let mut chunked = ParquetWriter::new(&mut parquet_buf)
            .with_statistics(false)
            .with_compression(CompressionOptions::Uncompressed)
            .with_version(Version::V1)
            .with_row_group_size(Some(rows_per_group))
            .chunked_with_compressions(schema, encodings, compressions)
            .unwrap();
        chunked.write_chunk(&partition).unwrap();

        let mut qdb_meta = QdbMeta::new(1);
        qdb_meta.schema.push(QdbMetaCol {
            column_type: ColumnTypeTag::Timestamp
                .into_type()
                .into_designated()
                .unwrap(),
            column_top: 0,
            format: None,
            ascii: None,
            id: None,
        });
        let qdb_meta_json = qdb_meta.serialize().unwrap();
        chunked
            .finish(vec![KeyValue {
                key: QDB_META_KEY.to_string(),
                value: Some(qdb_meta_json),
            }])
            .unwrap();
        (parquet_buf, qdb_meta)
    }

    /// Regenerates the committed test fixture consumed by
    /// `Mig941Test#testMigrateBackfillsMissingTsStats`. Run with
    /// `cargo test emit_mig941_ts_no_stats_fixture -- --ignored` after
    /// changing the parquet write path in a way that affects the fixture.
    #[test]
    #[ignore]
    fn emit_mig941_ts_no_stats_fixture() {
        let (bytes, _qdb_meta) = write_parquet_without_ts_stats(20, 10);
        let out = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../src/test/resources/mig941/ts_no_stats.parquet");
        std::fs::create_dir_all(out.parent().unwrap()).unwrap();
        std::fs::write(&out, &bytes).unwrap();
        eprintln!("wrote {} bytes to {}", bytes.len(), out.display());
    }

    #[test]
    fn backfill_fills_stat_flags_and_values() {
        let (parquet_bytes, qdb_meta) = write_parquet_without_ts_stats(10, 10);
        let mut cursor = Cursor::new(&parquet_bytes);
        let metadata = read_metadata_with_size(&mut cursor, parquet_bytes.len() as u64).unwrap();

        let backfill = |_rg: usize, _lo: usize, _hi: usize| -> ParquetResult<i64> { Ok(42) };
        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, Some(&qdb_meta), 0, 0, Some(&backfill), None).unwrap();
        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        let chunk = reader.row_group(0).unwrap().column_chunk(0).unwrap();
        let flags = StatFlags(chunk.stat_flags);
        assert!(flags.has_min_stat() && flags.is_min_inlined());
        assert!(flags.has_max_stat() && flags.is_max_inlined());
        assert_eq!(chunk.min_stat as i64, 42);
        assert_eq!(chunk.max_stat as i64, 42);
    }

    #[test]
    fn backfill_skipped_when_inline_stats_already_present() {
        // Write parquet the normal way (designated_timestamp: true forces
        // stats on) and confirm the backfill closure is never called.
        let parquet_bytes = write_test_parquet(10, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_bytes);
        let metadata = read_metadata_with_size(&mut cursor, parquet_bytes.len() as u64).unwrap();
        let qdb_meta = metadata
            .key_value_metadata
            .as_ref()
            .and_then(|kvs| {
                kvs.iter()
                    .find(|kv| kv.key == crate::parquet::qdb_metadata::QDB_META_KEY)
                    .and_then(|kv| kv.value.as_deref())
            })
            .map(|json| QdbMeta::deserialize(json).unwrap());

        let backfill = |_rg: usize, _lo: usize, _hi: usize| -> ParquetResult<i64> {
            panic!("backfill must not be called when inline stats exist");
        };
        let (_parquet_meta_bytes, _parquet_meta_file_size) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0, Some(&backfill), None)
                .unwrap();
    }
}
