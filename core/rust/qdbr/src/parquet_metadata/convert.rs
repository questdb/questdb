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
use crate::parquet_read::decoders::int32::DayToMillisConverter;
use crate::parquet_read::decoders::int96::Int96ToTimestampConverter;
use parquet2::metadata::FileMetaData;
use parquet2::schema::types::{PhysicalType, PrimitiveLogicalType, TimeUnit};
use parquet2::statistics::{
    BinaryStatistics, BooleanStatistics, FixedLenStatistics, PrimitiveStatistics, Statistics,
};
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
///
/// # Errors
/// - If any column chunk references an external `file_path` (not supported).
/// - If sorting columns differ between row groups.
/// - If `qdb_meta` is present but its schema length doesn't match the parquet column count.
pub fn convert_from_parquet(
    file_metadata: &FileMetaData,
    qdb_meta: Option<&QdbMeta>,
    parquet_footer_offset: u64,
    parquet_footer_length: u32,
    ts_stats_backfill: Option<&TsStatsBackfill<'_>>,
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

    // Validate no file_path references and extract/validate sorting columns.
    validate_file_paths(file_metadata)?;
    let sorting_cols = extract_sorting_columns(file_metadata)?;

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
        let id = field_info.id.unwrap_or(-1);

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

            let mut chunk = build_column_chunk(col_chunk, col_type_tag)?;

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
    col_type_tag: Option<ColumnTypeTag>,
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

    let bloom_filter_parquet = {
        let m = col_chunk.metadata();
        match (m.bloom_filter_offset, m.bloom_filter_length) {
            (Some(off), Some(len)) if off > 0 && len > 0 => Some((off.max(0) as u64, len as u32)),
            _ => None,
        }
    };

    let mut raw = ColumnChunkRaw::zeroed();
    raw.codec = codec as u8;
    raw.encodings = encodings.0;
    raw.num_values = col_chunk.num_values().max(0) as u64;
    raw.byte_range_start = byte_range_start;
    raw.total_compressed = total_compressed;

    let mut ool_min: Option<Vec<u8>> = None;
    let mut ool_max: Option<Vec<u8>> = None;

    // Determine if stats should be inline or out-of-line.
    let fixed_size = col_type_tag.and_then(|t| t.fixed_size());
    let is_inline = fixed_size.is_some_and(|s| s <= 8);

    // Extract statistics.
    if let Some(Ok(stats)) = col_chunk.statistics() {
        {
            let mut stat_flags = StatFlags::new();

            // null_count
            if let Some(nc) = stats.null_count() {
                stat_flags = stat_flags.with_null_count();
                raw.null_count = nc.max(0) as u64;
            }

            // Extract raw min/max from parquet stats, then convert to QuestDB representation.
            let (raw_min, raw_max, distinct_count) =
                extract_stat_values(stats.as_ref(), col_chunk.physical_type());

            let logical_type = col_chunk
                .descriptor()
                .descriptor
                .primitive_type
                .logical_type
                .as_ref();

            let min_bytes = raw_min.and_then(|v| {
                convert_stat_to_qdb(&v, col_chunk.physical_type(), logical_type, col_type_tag)
            });
            let max_bytes = raw_max.and_then(|v| {
                convert_stat_to_qdb(&v, col_chunk.physical_type(), logical_type, col_type_tag)
            });

            if let Some(dc) = distinct_count {
                stat_flags = stat_flags.with_distinct_count();
                raw.distinct_count = dc.max(0) as u64;
            }

            if let Some(ref min_val) = min_bytes {
                if is_inline && min_val.len() <= 8 {
                    stat_flags = stat_flags.with_min(true, true);
                    let mut buf = [0u8; 8];
                    buf[..min_val.len()].copy_from_slice(min_val);
                    raw.min_stat = u64::from_le_bytes(buf);
                } else if !min_val.is_empty() {
                    stat_flags = stat_flags.with_min(false, true);
                    ool_min = Some(min_val.clone());
                }
            }

            if let Some(ref max_val) = max_bytes {
                if is_inline && max_val.len() <= 8 {
                    stat_flags = stat_flags.with_max(true, true);
                    let mut buf = [0u8; 8];
                    buf[..max_val.len()].copy_from_slice(max_val);
                    raw.max_stat = u64::from_le_bytes(buf);
                } else if !max_val.is_empty() {
                    stat_flags = stat_flags.with_max(false, true);
                    ool_max = Some(max_val.clone());
                }
            }

            // Encode stat sizes for inline stats.
            if is_inline {
                let min_size = min_bytes
                    .as_ref()
                    .filter(|_| stat_flags.is_min_inlined())
                    .map(|v| v.len() as u8)
                    .unwrap_or(0);
                let max_size = max_bytes
                    .as_ref()
                    .filter(|_| stat_flags.is_max_inlined())
                    .map(|v| v.len() as u8)
                    .unwrap_or(0);
                raw.stat_sizes = encode_stat_sizes(min_size, max_size);
            }

            raw.stat_flags = stat_flags.0;
        }
    }

    Ok(BuiltChunk { raw, ool_min, ool_max, bloom_filter_parquet })
}

type RawStatValues = (Option<Vec<u8>>, Option<Vec<u8>>, Option<i64>);

/// Extracts min/max stat values as raw LE bytes and distinct_count.
fn extract_stat_values(stats: &dyn Statistics, physical_type: PhysicalType) -> RawStatValues {
    let none = (None, None, None);
    match physical_type {
        PhysicalType::Boolean => {
            let Some(s) = stats.as_any().downcast_ref::<BooleanStatistics>() else {
                return none;
            };
            let min = s.min_value.map(|v| vec![v as u8]);
            let max = s.max_value.map(|v| vec![v as u8]);
            (min, max, s.distinct_count)
        }
        PhysicalType::Int32 => {
            let Some(s) = stats.as_any().downcast_ref::<PrimitiveStatistics<i32>>() else {
                return none;
            };
            let min = s.min_value.map(|v| v.to_le_bytes().to_vec());
            let max = s.max_value.map(|v| v.to_le_bytes().to_vec());
            (min, max, s.distinct_count)
        }
        PhysicalType::Int64 => {
            let Some(s) = stats.as_any().downcast_ref::<PrimitiveStatistics<i64>>() else {
                return none;
            };
            let min = s.min_value.map(|v| v.to_le_bytes().to_vec());
            let max = s.max_value.map(|v| v.to_le_bytes().to_vec());
            (min, max, s.distinct_count)
        }
        PhysicalType::Float => {
            let Some(s) = stats.as_any().downcast_ref::<PrimitiveStatistics<f32>>() else {
                return none;
            };
            let min = s.min_value.map(|v| v.to_le_bytes().to_vec());
            let max = s.max_value.map(|v| v.to_le_bytes().to_vec());
            (min, max, s.distinct_count)
        }
        PhysicalType::Double => {
            let Some(s) = stats.as_any().downcast_ref::<PrimitiveStatistics<f64>>() else {
                return none;
            };
            let min = s.min_value.map(|v| v.to_le_bytes().to_vec());
            let max = s.max_value.map(|v| v.to_le_bytes().to_vec());
            (min, max, s.distinct_count)
        }
        PhysicalType::ByteArray => {
            let Some(s) = stats.as_any().downcast_ref::<BinaryStatistics>() else {
                return none;
            };
            (s.min_value.clone(), s.max_value.clone(), s.distinct_count)
        }
        PhysicalType::FixedLenByteArray(_) => {
            let Some(s) = stats.as_any().downcast_ref::<FixedLenStatistics>() else {
                return none;
            };
            (s.min_value.clone(), s.max_value.clone(), s.distinct_count)
        }
        PhysicalType::Int96 => {
            let Some(s) = stats
                .as_any()
                .downcast_ref::<PrimitiveStatistics<[u32; 3]>>()
            else {
                return none;
            };
            let min = s.min_value.map(|v| {
                let mut buf = Vec::with_capacity(12);
                for word in v {
                    buf.extend_from_slice(&word.to_le_bytes());
                }
                buf
            });
            let max = s.max_value.map(|v| {
                let mut buf = Vec::with_capacity(12);
                for word in v {
                    buf.extend_from_slice(&word.to_le_bytes());
                }
                buf
            });
            (min, max, s.distinct_count)
        }
    }
}

/// Converts a parquet stat value (raw LE bytes) to QuestDB's internal representation.
///
/// This is the single source of truth for the mapping. Handles:
/// - **Narrowing**: INT32-backed types (Byte, Short, Char, GeoByte, GeoShort) are
///   truncated from 4 bytes to their QuestDB fixed size.
/// - **Date days->millis**: INT32 Date (days since epoch) -> i64 millis.
/// - **Timestamp millis->micros**: INT64 Timestamp(Millis) with QDB Timestamp type -> i64 micros.
/// - **INT96->nanos**: 12-byte Julian day + nanos-of-day -> i64 epoch nanos.
///
/// Returns `None` if the input is empty.
fn convert_stat_to_qdb(
    raw: &[u8],
    physical_type: PhysicalType,
    logical_type: Option<&PrimitiveLogicalType>,
    col_type_tag: Option<ColumnTypeTag>,
) -> Option<Vec<u8>> {
    if raw.is_empty() {
        return None;
    }

    match (physical_type, col_type_tag) {
        // Narrowing: INT32 -> 1 byte for Byte/GeoByte.
        (PhysicalType::Int32, Some(ColumnTypeTag::Byte | ColumnTypeTag::GeoByte))
            if !raw.is_empty() =>
        {
            Some(vec![raw[0]])
        }

        // Narrowing: INT32 -> 2 bytes for Short/Char/GeoShort.
        (
            PhysicalType::Int32,
            Some(ColumnTypeTag::Short | ColumnTypeTag::Char | ColumnTypeTag::GeoShort),
        ) if raw.len() >= 2 => Some(vec![raw[0], raw[1]]),

        // Date stored as INT32 days -> i64 millis.
        // i32 * 86_400_000 always fits in i64 (max ≈ 1.86e17 < 9.22e18).
        (PhysicalType::Int32, Some(ColumnTypeTag::Date)) if raw.len() == 4 => {
            // Safety: we just checked the length is 4 bytes.
            let days = i32::from_le_bytes(unsafe { raw[..4].try_into().unwrap_unchecked() });
            let millis = DayToMillisConverter::convert(days);
            Some(millis.to_le_bytes().to_vec())
        }

        // External parquet Date (INT32 + Date logical type) without QdbMeta.
        (PhysicalType::Int32, None) if raw.len() == 4 => {
            if matches!(logical_type, Some(PrimitiveLogicalType::Date)) {
                // Safety: we just checked the length is 4 bytes.
                let days = i32::from_le_bytes(unsafe { raw[..4].try_into().unwrap_unchecked() });
                let millis = DayToMillisConverter::convert(days);
                Some(millis.to_le_bytes().to_vec())
            } else {
                Some(raw.to_vec())
            }
        }

        // Timestamp(Millis) -> micros, only for QDB Timestamp (not Date).
        (PhysicalType::Int64, Some(ColumnTypeTag::Timestamp)) if raw.len() == 8 => {
            if matches!(
                logical_type,
                Some(PrimitiveLogicalType::Timestamp { unit: TimeUnit::Milliseconds, .. })
            ) {
                // Safety: we just checked the length is 8 bytes.
                let millis = i64::from_le_bytes(unsafe { raw[..8].try_into().unwrap_unchecked() });
                let micros = millis.checked_mul(1000).unwrap_or(if millis >= 0 {
                    i64::MAX
                } else {
                    i64::MIN
                });
                Some(micros.to_le_bytes().to_vec())
            } else {
                // Micros or Nanos: pass through (Nanos stays nanos with NS flag).
                Some(raw.to_vec())
            }
        }

        // INT96 -> epoch nanos.
        (PhysicalType::Int96, _) if raw.len() == 12 => {
            let mut arr = [0u8; 12];
            arr.copy_from_slice(&raw[..12]);
            let nanos = Int96ToTimestampConverter::convert(arr.into());
            Some(nanos.to_le_bytes().to_vec())
        }

        // Pass-through for everything else.
        _ => Some(raw.to_vec()),
    }
}

fn build_column_chunk_from_thrift(
    meta: &parquet2::thrift_format::ColumnMetaData,
    schema_col: Option<&parquet2::metadata::ColumnDescriptor>,
    col_type_tag: Option<ColumnTypeTag>,
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

    let mut ool_min: Option<Vec<u8>> = None;
    let mut ool_max: Option<Vec<u8>> = None;

    // Determine if stats should be inline or out-of-line.
    let fixed_size = col_type_tag.and_then(|t| t.fixed_size());
    let is_inline = fixed_size.is_some_and(|s| s <= 8);

    // Deserialize and process statistics.
    if let (Some(ref thrift_stats), Some(col_desc)) = (&meta.statistics, schema_col) {
        let primitive_type = col_desc.descriptor.primitive_type.clone();
        if let Ok(stats) =
            parquet2::statistics::deserialize_statistics(thrift_stats, primitive_type)
        {
            let mut stat_flags = StatFlags::new();

            if let Some(nc) = stats.null_count() {
                stat_flags = stat_flags.with_null_count();
                raw.null_count = nc.max(0) as u64;
            }

            let physical_type = col_desc.descriptor.primitive_type.physical_type;
            let (raw_min, raw_max, distinct_count) =
                extract_stat_values(stats.as_ref(), physical_type);

            let logical_type = col_desc.descriptor.primitive_type.logical_type.as_ref();

            let min_bytes = raw_min
                .and_then(|v| convert_stat_to_qdb(&v, physical_type, logical_type, col_type_tag));
            let max_bytes = raw_max
                .and_then(|v| convert_stat_to_qdb(&v, physical_type, logical_type, col_type_tag));

            if let Some(dc) = distinct_count {
                stat_flags = stat_flags.with_distinct_count();
                raw.distinct_count = dc.max(0) as u64;
            }

            if let Some(ref min_val) = min_bytes {
                if is_inline && min_val.len() <= 8 {
                    stat_flags = stat_flags.with_min(true, true);
                    let mut buf = [0u8; 8];
                    buf[..min_val.len()].copy_from_slice(min_val);
                    raw.min_stat = u64::from_le_bytes(buf);
                } else if !min_val.is_empty() {
                    stat_flags = stat_flags.with_min(false, true);
                    ool_min = Some(min_val.clone());
                }
            }

            if let Some(ref max_val) = max_bytes {
                if is_inline && max_val.len() <= 8 {
                    stat_flags = stat_flags.with_max(true, true);
                    let mut buf = [0u8; 8];
                    buf[..max_val.len()].copy_from_slice(max_val);
                    raw.max_stat = u64::from_le_bytes(buf);
                } else if !max_val.is_empty() {
                    stat_flags = stat_flags.with_max(false, true);
                    ool_max = Some(max_val.clone());
                }
            }

            if is_inline {
                let min_size = min_bytes
                    .as_ref()
                    .filter(|_| stat_flags.is_min_inlined())
                    .map(|v| v.len() as u8)
                    .unwrap_or(0);
                let max_size = max_bytes
                    .as_ref()
                    .filter(|_| stat_flags.is_max_inlined())
                    .map(|v| v.len() as u8)
                    .unwrap_or(0);
                raw.stat_sizes = encode_stat_sizes(min_size, max_size);
            }

            raw.stat_flags = stat_flags.0;
        }
    }

    Ok(BuiltChunk { raw, ool_min, ool_max, bloom_filter_parquet })
}

/// Column metadata needed to build a `_pm` header.
///
/// Callers construct this from their own types (`Partition`, `QdbMeta`, etc.)
/// so that `parquet_metadata` doesn't depend on `parquet_write`.
pub struct ParquetMetaColumnInfo<'a> {
    pub name: &'a str,
    pub col_type_code: i32,
    pub col_type_tag: Option<ColumnTypeTag>,
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
    schema_columns: &[parquet2::metadata::ColumnDescriptor],
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

    let col_type_tags: Vec<Option<ColumnTypeTag>> =
        columns.iter().map(|c| c.col_type_tag).collect();

    for (rg_idx, thrift_rg) in thrift_row_groups.iter().enumerate() {
        let mut block = build_row_group_block_from_thrift_with_types(
            thrift_rg,
            schema_columns,
            &col_type_tags,
            &[],
        )?;
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
/// Compares the new parquet row groups against the existing `_pm` to determine
/// which row groups are unchanged, changed, or new. Unchanged row groups keep
/// their original offsets (no data rewritten). Only new/changed blocks and a
/// new footer are appended after the existing trailer, leaving the previous
/// bytes intact so that any older committed `parquetMetaFileSize` continues
/// to resolve to a consistent older snapshot.
///
/// Returns the bytes the caller must append at `existing_parquet_meta_file_size` and
/// the resulting file size. Returns an error when the new row group count is
/// smaller than the existing one (compaction is not supported in the in-place
/// path; the writer's row-group entry list cannot drop existing references).
#[allow(clippy::too_many_arguments)]
pub fn update_parquet_metadata(
    existing_pm: &[u8],
    existing_parquet_meta_file_size: u64,
    columns: &[ParquetMetaColumnInfo],
    schema_columns: &[parquet2::metadata::ColumnDescriptor],
    thrift_row_groups: &[parquet2::thrift_format::RowGroup],
    parquet_footer_offset: u64,
    parquet_footer_length: u32,
    bloom_bitsets: &[Vec<Option<Vec<u8>>>],
    unused_bytes: u64,
) -> ParquetResult<ParquetMetaUpdateResult> {
    let existing_pm_len = usize::try_from(existing_parquet_meta_file_size).map_err(|_| {
        parquet_meta_err!(
            ParquetMetaErrorKind::Truncated,
            "_pm file size {} exceeds addressable range",
            existing_parquet_meta_file_size
        )
    })?;
    let existing_pm = existing_pm.get(..existing_pm_len).ok_or_else(|| {
        parquet_meta_err!(
            ParquetMetaErrorKind::Truncated,
            "_pm file size {} exceeds available data {}",
            existing_parquet_meta_file_size,
            existing_pm.len()
        )
    })?;

    // Read the existing _pm to get row group fingerprints (byte_range_start of first column).
    let existing_reader = crate::parquet_metadata::reader::ParquetMetaReader::from_file_size(
        existing_pm,
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
        existing_pm,
        existing_parquet_meta_file_size,
    )?;

    let col_type_tags: Vec<Option<ColumnTypeTag>> =
        columns.iter().map(|c| c.col_type_tag).collect();

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

        let mut block = build_row_group_block_from_thrift_with_types(
            thrift_rg,
            schema_columns,
            &col_type_tags,
            &[],
        )?;
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
    let (append_bytes, new_file_size) = updater.finish()?;
    debug_assert_eq!(
        new_file_size,
        existing_parquet_meta_file_size + append_bytes.len() as u64
    );

    Ok(ParquetMetaUpdateResult { bytes: append_bytes, new_file_size })
}

/// Builds a `RowGroupBlockBuilder` directly from a thrift `RowGroup` struct
/// with pre-resolved column type tags. Used by `generate_parquet_metadata`
/// and `update_parquet_metadata` where the caller already knows the types.
///
/// `parquet_data` is the mmapped/read parquet file bytes, used to extract
/// bloom filter bitsets into the _pm out-of-line region.
pub fn build_row_group_block_from_thrift_with_types(
    rg: &parquet2::thrift_format::RowGroup,
    schema_columns: &[parquet2::metadata::ColumnDescriptor],
    col_type_tags: &[Option<ColumnTypeTag>],
    parquet_data: &[u8],
) -> ParquetResult<RowGroupBlockBuilder> {
    let col_count = rg.columns.len();
    let mut builder = RowGroupBlockBuilder::new(col_count as u32);
    builder.set_num_rows(rg.num_rows.max(0) as u64);

    for (col_idx, col_chunk) in rg.columns.iter().enumerate() {
        let col_type_tag = col_type_tags.get(col_idx).copied().flatten();

        let meta = col_chunk.meta_data.as_ref().ok_or_else(|| {
            parquet_meta_err!(
                ParquetMetaErrorKind::Conversion,
                "column chunk at col={} has no metadata",
                col_idx
            )
        })?;

        let chunk =
            build_column_chunk_from_thrift(meta, schema_columns.get(col_idx), col_type_tag)?;
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

pub(crate) fn extract_sorting_columns(
    file_metadata: &FileMetaData,
) -> ParquetResult<Vec<SortingCol>> {
    let mut result: Option<Vec<SortingCol>> = None;

    for (rg_idx, rg) in file_metadata.row_groups.iter().enumerate() {
        let current = match rg.sorting_columns() {
            Some(cols) => cols
                .iter()
                .map(|sc| SortingCol {
                    column_idx: sc.column_idx,
                    descending: sc.descending,
                })
                .collect::<Vec<_>>(),
            None => Vec::new(),
        };

        if let Some(ref prev) = result {
            // Validate consistency.
            if prev.len() != current.len() {
                return Err(parquet_meta_err!(
                    ParquetMetaErrorKind::SchemaMismatch,
                    "sorting columns differ between row groups: rg 0 has {} but rg {} has {}",
                    prev.len(),
                    rg_idx,
                    current.len()
                ));
            }
            for (i, (p, c)) in prev.iter().zip(current.iter()).enumerate() {
                if p.column_idx != c.column_idx || p.descending != c.descending {
                    return Err(parquet_meta_err!(
                        ParquetMetaErrorKind::SchemaMismatch,
                        "sorting column {} differs between row groups 0 and {}",
                        i,
                        rg_idx
                    ));
                }
            }
        } else {
            result = Some(current);
        }
    }

    Ok(result.unwrap_or_default())
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

        let (pm_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0, None).unwrap();

        let reader = ParquetMetaReader::from_file_size(&pm_bytes, parquet_meta_file_size).unwrap();
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
    fn convert_with_compression() {
        let parquet_data = write_test_parquet(1000, CompressionOptions::Snappy);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        let (pm_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, None, 0, 0, None).unwrap();

        let reader = ParquetMetaReader::from_file_size(&pm_bytes, parquet_meta_file_size).unwrap();
        let chunk = reader.row_group(0).unwrap().column_chunk(0).unwrap();
        assert_eq!(chunk.codec().unwrap(), Codec::Snappy);
    }

    #[test]
    fn convert_without_qdb_meta() {
        let parquet_data = write_test_parquet(50, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        let (pm_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, None, 0, 0, None).unwrap();

        let reader = ParquetMetaReader::from_file_size(&pm_bytes, parquet_meta_file_size).unwrap();
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

        let (pm_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, Some(&qdb_meta), 0, 0, None).unwrap();
        let reader = ParquetMetaReader::from_file_size(&pm_bytes, parquet_meta_file_size).unwrap();
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

        let (pm_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, Some(&qdb_meta), 0, 0, None).unwrap();
        let reader = ParquetMetaReader::from_file_size(&pm_bytes, parquet_meta_file_size).unwrap();
        assert!(!reader.feature_flags().has_squash_tracker());
        assert_eq!(reader.squash_tracker(), None);
    }

    #[test]
    fn convert_without_qdb_meta_omits_squash_tracker() {
        let parquet_data = write_test_parquet(10, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        let (pm_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, None, 0, 0, None).unwrap();
        let reader = ParquetMetaReader::from_file_size(&pm_bytes, parquet_meta_file_size).unwrap();
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
            });
        bad_meta
            .schema
            .push(crate::parquet::qdb_metadata::QdbMetaCol {
                column_type: ColumnTypeTag::Long.into_type(),
                column_top: 0,
                format: None,
                ascii: None,
            });

        let result = convert_from_parquet(&metadata, Some(&bad_meta), 0, 0, None);
        assert!(result.is_err());
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

        let (pm_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 1024, 200, None).unwrap();

        let reader = ParquetMetaReader::from_file_size(&pm_bytes, parquet_meta_file_size).unwrap();
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
        });

        let (pm_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, Some(&meta), 0, 0, None).unwrap();

        let reader = ParquetMetaReader::from_file_size(&pm_bytes, parquet_meta_file_size).unwrap();
        let desc = reader.column_descriptor(0).unwrap();
        let flags = desc.flags();
        assert!(flags.is_local_key_global());
        assert!(flags.is_ascii());
        // `top` is no longer part of the column descriptor.
    }

    #[test]
    fn convert_sorting_columns_propagated() {
        // Write a parquet file with a designated timestamp - ParquetWriter
        // should set sorting columns in the row group metadata.
        let parquet_data = write_test_parquet(100, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        // Check if sorting columns are actually present in the parquet metadata.
        let has_sorting = metadata.row_groups.iter().any(|rg| {
            rg.sorting_columns()
                .as_ref()
                .is_some_and(|sc| !sc.is_empty())
        });

        let qdb_meta = extract_qdb_meta_from(&metadata);
        let (pm_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0, None).unwrap();

        let reader = ParquetMetaReader::from_file_size(&pm_bytes, parquet_meta_file_size).unwrap();

        if has_sorting {
            assert!(reader.sorting_column_count() > 0);
            assert_eq!(reader.sorting_column(0).unwrap(), 0);
        } else {
            // If the writer doesn't set sorting columns, they won't appear.
            assert_eq!(reader.sorting_column_count(), 0);
        }

        // Designated timestamp should still be detected from QdbMeta.
        assert!(reader.designated_timestamp().is_some());
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
        let (pm_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0, None).unwrap();

        let reader = ParquetMetaReader::from_file_size(&pm_bytes, parquet_meta_file_size).unwrap();
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

        let (pm_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0, None).unwrap();

        let reader = ParquetMetaReader::from_file_size(&pm_bytes, parquet_meta_file_size).unwrap();
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
    fn extract_stat_values_boolean() {
        let stats = BooleanStatistics {
            null_count: Some(2),
            distinct_count: Some(2),
            min_value: Some(false),
            max_value: Some(true),
        };
        let (min, max, dc) = extract_stat_values(&stats, PhysicalType::Boolean);
        assert_eq!(min, Some(vec![0u8]));
        assert_eq!(max, Some(vec![1u8]));
        assert_eq!(dc, Some(2));
    }

    #[test]
    fn extract_stat_values_int32() {
        use parquet2::schema::types::PrimitiveType;
        let pt = PrimitiveType::from_physical("x".to_string(), PhysicalType::Int32);
        let stats = PrimitiveStatistics::<i32> {
            primitive_type: pt,
            null_count: Some(0),
            distinct_count: Some(10),
            min_value: Some(-5),
            max_value: Some(100),
        };
        let (min, max, dc) = extract_stat_values(&stats, PhysicalType::Int32);
        assert_eq!(min, Some((-5i32).to_le_bytes().to_vec()));
        assert_eq!(max, Some(100i32.to_le_bytes().to_vec()));
        assert_eq!(dc, Some(10));
    }

    #[test]
    fn extract_stat_values_float() {
        use parquet2::schema::types::PrimitiveType;
        let pt = PrimitiveType::from_physical("x".to_string(), PhysicalType::Float);
        let stats = PrimitiveStatistics::<f32> {
            primitive_type: pt,
            null_count: None,
            distinct_count: None,
            min_value: Some(1.5f32),
            max_value: Some(99.9f32),
        };
        let (min, max, dc) = extract_stat_values(&stats, PhysicalType::Float);
        assert_eq!(min, Some(1.5f32.to_le_bytes().to_vec()));
        assert_eq!(max, Some(99.9f32.to_le_bytes().to_vec()));
        assert_eq!(dc, None);
    }

    #[test]
    fn extract_stat_values_double() {
        use parquet2::schema::types::PrimitiveType;
        let pt = PrimitiveType::from_physical("x".to_string(), PhysicalType::Double);
        let stats = PrimitiveStatistics::<f64> {
            primitive_type: pt,
            null_count: None,
            distinct_count: None,
            min_value: Some(-1.0),
            max_value: Some(1.0),
        };
        let (min, max, dc) = extract_stat_values(&stats, PhysicalType::Double);
        assert_eq!(min, Some((-1.0f64).to_le_bytes().to_vec()));
        assert_eq!(max, Some(1.0f64.to_le_bytes().to_vec()));
        assert_eq!(dc, None);
    }

    #[test]
    fn extract_stat_values_binary() {
        use parquet2::schema::types::PrimitiveType;
        let pt = PrimitiveType::from_physical("x".to_string(), PhysicalType::ByteArray);
        let stats = BinaryStatistics {
            primitive_type: pt,
            null_count: Some(1),
            distinct_count: Some(5),
            min_value: Some(vec![0x01, 0x02]),
            max_value: Some(vec![0xFF, 0xFE]),
        };
        let (min, max, dc) = extract_stat_values(&stats, PhysicalType::ByteArray);
        assert_eq!(min, Some(vec![0x01, 0x02]));
        assert_eq!(max, Some(vec![0xFF, 0xFE]));
        assert_eq!(dc, Some(5));
    }

    #[test]
    fn extract_stat_values_fixed_len_byte_array() {
        use parquet2::schema::types::PrimitiveType;
        let pt = PrimitiveType::from_physical("x".to_string(), PhysicalType::FixedLenByteArray(16));
        let stats = FixedLenStatistics {
            primitive_type: pt,
            null_count: None,
            distinct_count: None,
            min_value: Some(vec![0u8; 16]),
            max_value: Some(vec![0xFFu8; 16]),
        };
        let (min, max, _) = extract_stat_values(&stats, PhysicalType::FixedLenByteArray(16));
        assert_eq!(min.as_ref().map(|v| v.len()), Some(16));
        assert_eq!(max.as_ref().map(|v| v.len()), Some(16));
    }

    #[test]
    fn extract_stat_values_int96() {
        use parquet2::schema::types::PrimitiveType;
        let pt = PrimitiveType::from_physical("x".to_string(), PhysicalType::Int96);
        let stats = PrimitiveStatistics::<[u32; 3]> {
            primitive_type: pt,
            null_count: None,
            distinct_count: None,
            min_value: Some([1, 2, 3]),
            max_value: Some([4, 5, 6]),
        };
        let (min, max, _) = extract_stat_values(&stats, PhysicalType::Int96);
        let min = min.unwrap();
        assert_eq!(min.len(), 12);
        assert_eq!(&min[0..4], &1u32.to_le_bytes());
        assert_eq!(&min[4..8], &2u32.to_le_bytes());
        assert_eq!(&min[8..12], &3u32.to_le_bytes());
        let max = max.unwrap();
        assert_eq!(&max[0..4], &4u32.to_le_bytes());
    }

    #[test]
    fn convert_distinct_count_present() {
        // Write a parquet file with statistics enabled.
        let parquet_data = write_multi_column_parquet(50);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);

        let (pm_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0, None).unwrap();

        let reader = ParquetMetaReader::from_file_size(&pm_bytes, parquet_meta_file_size).unwrap();
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

    // ── convert_stat_to_qdb tests ─────────────────────────────────────

    #[test]
    fn convert_stat_narrowing_byte() {
        let raw = 42i32.to_le_bytes().to_vec();
        let result =
            convert_stat_to_qdb(&raw, PhysicalType::Int32, None, Some(ColumnTypeTag::Byte));
        assert_eq!(result, Some(vec![42u8]));
    }

    #[test]
    fn convert_stat_narrowing_byte_negative() {
        let raw = (-3i32).to_le_bytes().to_vec();
        let result =
            convert_stat_to_qdb(&raw, PhysicalType::Int32, None, Some(ColumnTypeTag::Byte));
        // -3 as i8 is 0xFD, which is the low byte of -3i32 in LE
        assert_eq!(result, Some(vec![(-3i8) as u8]));
    }

    #[test]
    fn convert_stat_narrowing_geobyte() {
        let raw = 7i32.to_le_bytes().to_vec();
        let result = convert_stat_to_qdb(
            &raw,
            PhysicalType::Int32,
            None,
            Some(ColumnTypeTag::GeoByte),
        );
        assert_eq!(result, Some(vec![7u8]));
    }

    #[test]
    fn convert_stat_narrowing_short() {
        let raw = 1000i32.to_le_bytes().to_vec();
        let result =
            convert_stat_to_qdb(&raw, PhysicalType::Int32, None, Some(ColumnTypeTag::Short));
        assert_eq!(result, Some(1000i16.to_le_bytes().to_vec()));
    }

    #[test]
    fn convert_stat_narrowing_char() {
        let raw = 0x0041i32.to_le_bytes().to_vec(); // 'A' as u16
        let result =
            convert_stat_to_qdb(&raw, PhysicalType::Int32, None, Some(ColumnTypeTag::Char));
        assert_eq!(result, Some(0x0041u16.to_le_bytes().to_vec()));
    }

    #[test]
    fn convert_stat_narrowing_geoshort() {
        let raw = 255i32.to_le_bytes().to_vec();
        let result = convert_stat_to_qdb(
            &raw,
            PhysicalType::Int32,
            None,
            Some(ColumnTypeTag::GeoShort),
        );
        assert_eq!(result, Some(255i16.to_le_bytes().to_vec()));
    }

    #[test]
    fn convert_stat_date_days_to_millis_with_qdb_meta() {
        // 1 day = 86_400_000 millis
        let raw = 10i32.to_le_bytes().to_vec(); // 10 days
        let result =
            convert_stat_to_qdb(&raw, PhysicalType::Int32, None, Some(ColumnTypeTag::Date));
        let expected = (10i64 * 86_400_000).to_le_bytes().to_vec();
        assert_eq!(result, Some(expected));
    }

    #[test]
    fn convert_stat_date_days_to_millis_without_qdb_meta() {
        let raw = 5i32.to_le_bytes().to_vec();
        let result = convert_stat_to_qdb(
            &raw,
            PhysicalType::Int32,
            Some(&PrimitiveLogicalType::Date),
            None,
        );
        let expected = (5i64 * 86_400_000).to_le_bytes().to_vec();
        assert_eq!(result, Some(expected));
    }

    #[test]
    fn convert_stat_date_negative_days() {
        // Days before epoch
        let raw = (-100i32).to_le_bytes().to_vec();
        let result =
            convert_stat_to_qdb(&raw, PhysicalType::Int32, None, Some(ColumnTypeTag::Date));
        let expected = (-100i64 * 86_400_000).to_le_bytes().to_vec();
        assert_eq!(result, Some(expected));
    }

    #[test]
    fn convert_stat_int32_no_qdb_meta_no_logical_type() {
        // Plain INT32 without QdbMeta or logical type: pass through
        let raw = 42i32.to_le_bytes().to_vec();
        let result = convert_stat_to_qdb(&raw, PhysicalType::Int32, None, None);
        assert_eq!(result, Some(raw));
    }

    #[test]
    fn convert_stat_timestamp_millis_to_micros() {
        let millis = 1_000_000i64; // 1000 seconds
        let raw = millis.to_le_bytes().to_vec();
        let logical = PrimitiveLogicalType::Timestamp {
            unit: TimeUnit::Milliseconds,
            is_adjusted_to_utc: true,
        };
        let result = convert_stat_to_qdb(
            &raw,
            PhysicalType::Int64,
            Some(&logical),
            Some(ColumnTypeTag::Timestamp),
        );
        let expected = (millis * 1000).to_le_bytes().to_vec();
        assert_eq!(result, Some(expected));
    }

    #[test]
    fn convert_stat_timestamp_micros_passthrough() {
        let micros = 1_000_000_000i64;
        let raw = micros.to_le_bytes().to_vec();
        let logical = PrimitiveLogicalType::Timestamp {
            unit: TimeUnit::Microseconds,
            is_adjusted_to_utc: true,
        };
        let result = convert_stat_to_qdb(
            &raw,
            PhysicalType::Int64,
            Some(&logical),
            Some(ColumnTypeTag::Timestamp),
        );
        assert_eq!(result, Some(raw));
    }

    #[test]
    fn convert_stat_timestamp_nanos_passthrough() {
        let nanos = 1_000_000_000_000i64;
        let raw = nanos.to_le_bytes().to_vec();
        let logical = PrimitiveLogicalType::Timestamp {
            unit: TimeUnit::Nanoseconds,
            is_adjusted_to_utc: true,
        };
        let result = convert_stat_to_qdb(
            &raw,
            PhysicalType::Int64,
            Some(&logical),
            Some(ColumnTypeTag::Timestamp),
        );
        assert_eq!(result, Some(raw));
    }

    #[test]
    fn convert_stat_int96_to_epoch_nanos() {
        // Construct an INT96 for 1970-01-02 00:00:00.000000001 UTC
        // Julian day for 1970-01-02 = 2_440_589
        // nanos_of_day = 1
        let nanos_of_day: u64 = 1;
        let julian_day: u32 = 2_440_589;
        let mut raw = Vec::with_capacity(12);
        raw.extend_from_slice(&(nanos_of_day as u32).to_le_bytes()); // low 32
        raw.extend_from_slice(&((nanos_of_day >> 32) as u32).to_le_bytes()); // high 32
        raw.extend_from_slice(&julian_day.to_le_bytes());

        let result = convert_stat_to_qdb(&raw, PhysicalType::Int96, None, None);

        // 1 day = 86_400_000_000_000 nanos, plus 1
        let expected_nanos: i64 = 86_400_000_000_000 + 1;
        assert_eq!(result, Some(expected_nanos.to_le_bytes().to_vec()));
    }

    #[test]
    fn convert_stat_int96_epoch() {
        // Julian day for 1970-01-01 = 2_440_588, nanos_of_day = 0
        let mut raw = Vec::with_capacity(12);
        raw.extend_from_slice(&0u32.to_le_bytes());
        raw.extend_from_slice(&0u32.to_le_bytes());
        raw.extend_from_slice(&2_440_588u32.to_le_bytes());

        let result = convert_stat_to_qdb(&raw, PhysicalType::Int96, None, None);
        assert_eq!(result, Some(0i64.to_le_bytes().to_vec()));
    }

    #[test]
    fn convert_stat_empty_returns_none() {
        let result = convert_stat_to_qdb(&[], PhysicalType::Int32, None, Some(ColumnTypeTag::Int));
        assert_eq!(result, None);
    }

    #[test]
    fn convert_stat_int_passthrough() {
        let raw = 42i32.to_le_bytes().to_vec();
        let result = convert_stat_to_qdb(&raw, PhysicalType::Int32, None, Some(ColumnTypeTag::Int));
        assert_eq!(result, Some(raw));
    }

    #[test]
    fn convert_stat_long_passthrough() {
        let raw = 123456789i64.to_le_bytes().to_vec();
        let result =
            convert_stat_to_qdb(&raw, PhysicalType::Int64, None, Some(ColumnTypeTag::Long));
        assert_eq!(result, Some(raw));
    }

    #[test]
    fn convert_stat_double_passthrough() {
        let raw = 42.5_f64.to_le_bytes().to_vec();
        let result = convert_stat_to_qdb(
            &raw,
            PhysicalType::Double,
            None,
            Some(ColumnTypeTag::Double),
        );
        assert_eq!(result, Some(raw));
    }

    #[test]
    fn convert_stat_float_passthrough() {
        let raw = 2.5f32.to_le_bytes().to_vec();
        let result =
            convert_stat_to_qdb(&raw, PhysicalType::Float, None, Some(ColumnTypeTag::Float));
        assert_eq!(result, Some(raw));
    }

    #[test]
    fn convert_stat_byte_array_passthrough() {
        let raw = b"hello".to_vec();
        let result = convert_stat_to_qdb(
            &raw,
            PhysicalType::ByteArray,
            None,
            Some(ColumnTypeTag::Varchar),
        );
        assert_eq!(result, Some(raw));
    }

    // ── Saturation on extreme values ──────────────────────────────────

    #[test]
    fn convert_stat_timestamp_millis_overflow_saturates() {
        let raw = i64::MAX.to_le_bytes().to_vec();
        let logical = PrimitiveLogicalType::Timestamp {
            unit: TimeUnit::Milliseconds,
            is_adjusted_to_utc: true,
        };
        let result = convert_stat_to_qdb(
            &raw,
            PhysicalType::Int64,
            Some(&logical),
            Some(ColumnTypeTag::Timestamp),
        );
        let val = i64::from_le_bytes(result.unwrap().try_into().unwrap());
        assert_eq!(val, i64::MAX);
    }

    #[test]
    fn convert_stat_timestamp_millis_negative_overflow_saturates() {
        let raw = i64::MIN.to_le_bytes().to_vec();
        let logical = PrimitiveLogicalType::Timestamp {
            unit: TimeUnit::Milliseconds,
            is_adjusted_to_utc: true,
        };
        let result = convert_stat_to_qdb(
            &raw,
            PhysicalType::Int64,
            Some(&logical),
            Some(ColumnTypeTag::Timestamp),
        );
        let val = i64::from_le_bytes(result.unwrap().try_into().unwrap());
        assert_eq!(val, i64::MIN);
    }

    // ── Mismatched-length stats fall through to passthrough ───────────

    #[test]
    fn convert_stat_short_int32_passes_through() {
        // 2 bytes with Int32+Date: guard `raw.len() == 4` fails, hits passthrough.
        let raw = vec![0u8; 2];
        let result =
            convert_stat_to_qdb(&raw, PhysicalType::Int32, None, Some(ColumnTypeTag::Date));
        assert_eq!(result, Some(raw));
    }

    #[test]
    fn convert_stat_short_int96_passes_through() {
        // 8 bytes with Int96: guard `raw.len() == 12` fails, hits passthrough.
        let raw = vec![0u8; 8];
        let result = convert_stat_to_qdb(&raw, PhysicalType::Int96, None, None);
        assert_eq!(result, Some(raw));
    }

    // ── Tests for build_row_group_block_from_thrift_with_types ─────────

    /// Test helper: resolve col_type_tags from QdbMeta + schema, then delegate.
    fn build_row_group_block_from_thrift(
        rg: &parquet2::thrift_format::RowGroup,
        schema_columns: &[parquet2::metadata::ColumnDescriptor],
        qdb_meta: Option<&QdbMeta>,
    ) -> crate::parquet::error::ParquetResult<RowGroupBlockBuilder> {
        let col_type_tags: Vec<Option<ColumnTypeTag>> = (0..rg.columns.len())
            .map(|i| {
                qdb_meta
                    .and_then(|m| {
                        let code = m.schema.get(i)?.column_type.tag() as u8;
                        ColumnTypeTag::try_from(code).ok()
                    })
                    .or_else(|| {
                        schema_columns
                            .get(i)
                            .and_then(crate::parquet_read::meta::infer_column_type)
                            .map(|ct| ct.tag())
                    })
            })
            .collect();
        build_row_group_block_from_thrift_with_types(rg, schema_columns, &col_type_tags, &[])
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
        let (pm_bytes_from_meta, parquet_meta_file_size) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0, None).unwrap();
        let reader1 =
            ParquetMetaReader::from_file_size(&pm_bytes_from_meta, parquet_meta_file_size).unwrap();

        // Path 2: build from thrift via the writer's into_inner_and_metadata()
        // We need the ThriftFileMetaData. Write again using ChunkedWriter to get it.
        let parquet_data2 = write_multi_column_parquet(200);
        let mut cursor2 = Cursor::new(&parquet_data2);
        let metadata2 = read_metadata_with_size(&mut cursor2, parquet_data2.len() as u64).unwrap();
        let thrift_meta = metadata2.into_thrift();

        // Build row group blocks from thrift row groups.
        let schema_columns = metadata.schema_descr.columns();
        for (rg_idx, thrift_rg) in thrift_meta.row_groups.iter().enumerate() {
            let block =
                build_row_group_block_from_thrift(thrift_rg, schema_columns, qdb_meta.as_ref())
                    .unwrap();

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
        let qdb_meta = extract_qdb_meta_from(&metadata);
        let thrift_meta = metadata.into_thrift();

        // Reload metadata for schema_columns (into_thrift consumed it).
        let mut cursor2 = Cursor::new(&parquet_data);
        let metadata2 = read_metadata_with_size(&mut cursor2, parquet_data.len() as u64).unwrap();
        let schema_columns = metadata2.schema_descr.columns();

        let block = build_row_group_block_from_thrift(
            &thrift_meta.row_groups[0],
            schema_columns,
            qdb_meta.as_ref(),
        )
        .unwrap();

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
        let qdb_meta = extract_qdb_meta_from(&metadata);
        let thrift_meta = metadata.into_thrift();

        let mut cursor2 = Cursor::new(&parquet_data);
        let metadata2 = read_metadata_with_size(&mut cursor2, parquet_data.len() as u64).unwrap();

        let block = build_row_group_block_from_thrift(
            &thrift_meta.row_groups[0],
            metadata2.schema_descr.columns(),
            qdb_meta.as_ref(),
        )
        .unwrap();

        let chunk = block.column_chunk_raw(0);
        assert_eq!(chunk.codec().unwrap(), Codec::Snappy);
    }

    #[test]
    fn thrift_without_qdb_meta_infers_types() {
        // Without QdbMeta, types should be inferred from parquet schema.
        // write_multi_column_parquet: ts(Timestamp), int_val(Int), dbl_val(Double), flag(Boolean)
        let parquet_data = write_multi_column_parquet(30);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let thrift_meta = metadata.into_thrift();

        let mut cursor2 = Cursor::new(&parquet_data);
        let metadata2 = read_metadata_with_size(&mut cursor2, parquet_data.len() as u64).unwrap();
        let schema_columns = metadata2.schema_descr.columns();

        // Pass None for QdbMeta — types should be inferred.
        let block =
            build_row_group_block_from_thrift(&thrift_meta.row_groups[0], schema_columns, None)
                .unwrap();

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
        let qdb_meta = extract_qdb_meta_from(&metadata);
        let thrift_meta = metadata.into_thrift();

        let mut cursor2 = Cursor::new(&buf);
        let metadata2 = read_metadata_with_size(&mut cursor2, buf.len() as u64).unwrap();

        assert_eq!(thrift_meta.row_groups.len(), 4);
        let expected_rows = [30u64, 30, 30, 10];

        for (i, thrift_rg) in thrift_meta.row_groups.iter().enumerate() {
            let block = build_row_group_block_from_thrift(
                thrift_rg,
                metadata2.schema_descr.columns(),
                qdb_meta.as_ref(),
            )
            .unwrap();
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
                let col_type_tag = cm.map(|c| c.column_type.tag()).or_else(|| {
                    crate::parquet_read::meta::infer_column_type(col_desc).map(|ct| ct.tag())
                });
                let mut flags = ColumnFlags::new();
                flags = flags.with_repetition(FieldRepetition::from(field_info.repetition));
                ParquetMetaColumnInfo {
                    name: &field_info.name,
                    col_type_code,
                    col_type_tag,
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

        let (pm_bytes, parquet_meta_file_size) = generate_parquet_metadata(
            &col_infos,
            schema_columns,
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

        let reader = ParquetMetaReader::from_file_size(&pm_bytes, parquet_meta_file_size).unwrap();
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
        assert_eq!(parquet_meta_file_size, pm_bytes.len() as u64);
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
                let col_type_tag = cm.map(|c| c.column_type.tag()).or_else(|| {
                    crate::parquet_read::meta::infer_column_type(col_desc).map(|ct| ct.tag())
                });
                let mut flags = ColumnFlags::new();
                flags = flags.with_repetition(FieldRepetition::from(field_info.repetition));
                ParquetMetaColumnInfo {
                    name: &field_info.name,
                    col_type_code: cm.map(|c| c.column_type.code()).unwrap_or(-1),
                    col_type_tag,
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
            schema_columns,
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
            &col_infos,
            schema_columns,
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
                let col_type_tag = cm.map(|c| c.column_type.tag()).or_else(|| {
                    crate::parquet_read::meta::infer_column_type(col_desc).map(|ct| ct.tag())
                });
                let mut flags = ColumnFlags::new();
                flags = flags.with_repetition(FieldRepetition::from(field_info.repetition));
                ParquetMetaColumnInfo {
                    name: &field_info.name,
                    col_type_code: cm.map(|c| c.column_type.code()).unwrap_or(-1),
                    col_type_tag,
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

        let (initial_pm, _) = generate_parquet_metadata(
            &col_infos,
            schema_columns,
            &three_rgs,
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
        assert_eq!(initial_reader.row_group_count(), 3);

        // Drop the third row group and ask for an update with only 2.
        let two_rgs = three_rgs[..2].to_vec();
        let result = update_parquet_metadata(
            &initial_pm,
            initial_size,
            &col_infos,
            schema_columns,
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

        let result = build_row_group_block_from_thrift(&rg, &[], None);
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
            col_type_tag: Some(ColumnTypeTag::Timestamp),
            id: 0,
            flags: ColumnFlags::new().with_repetition(FieldRepetition::Required),
            fixed_byte_len: 0,
            physical_type: physical_type_to_u8(PhysicalType::Int64),
            max_rep_level: 0,
            max_def_level: 0,
        }];

        // Generate _pm with captured bloom filter bitsets.
        let (pm_bytes, parquet_meta_file_size) = generate_parquet_metadata(
            &col_infos,
            chunked.schema().columns(),
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
        let reader = ParquetMetaReader::from_file_size(&pm_bytes, parquet_meta_file_size).unwrap();
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
            bf_abs + 4 <= pm_bytes.len(),
            "bloom filter offset should be within _pm bounds"
        );

        // Read the [i32 len][bitset] at the absolute offset.
        let bf_data = &pm_bytes[bf_abs..];
        let bf_len = i32::from_le_bytes(bf_data[..4].try_into().unwrap()) as usize;
        assert!(
            bf_len >= 32,
            "bloom filter bitset should be at least 32 bytes"
        );
        assert!(
            bf_abs + 4 + bf_len <= pm_bytes.len(),
            "bloom filter bitset should fit within _pm"
        );

        // The bitset should not be all zeros (we inserted 100 values).
        let bitset = &bf_data[4..4 + bf_len];
        assert!(
            bitset.iter().any(|&b| b != 0),
            "bloom filter bitset should contain set bits"
        );
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
                let col_type_tag = cm.map(|c| c.column_type.tag());
                let mut flags = ColumnFlags::new();
                flags = flags.with_repetition(FieldRepetition::from(field_info.repetition));
                ParquetMetaColumnInfo {
                    name: &field_info.name,
                    col_type_code,
                    col_type_tag,
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

        let (pm_bytes, _) = generate_parquet_metadata(
            &col_infos,
            schema_columns,
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
        let reader = ParquetMetaReader::from_file_size(&pm_bytes, pm_bytes.len() as u64).unwrap();
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
    /// `Mig940Test#testMigrateBackfillsMissingTsStats`. Run with
    /// `cargo test emit_mig940_ts_no_stats_fixture -- --ignored` after
    /// changing the parquet write path in a way that affects the fixture.
    #[test]
    #[ignore]
    fn emit_mig940_ts_no_stats_fixture() {
        let (bytes, _qdb_meta) = write_parquet_without_ts_stats(20, 10);
        let out = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../src/test/resources/mig940/ts_no_stats.parquet");
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
            convert_from_parquet(&metadata, Some(&qdb_meta), 0, 0, Some(&backfill)).unwrap();
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
        let (_pm_bytes, _pm_file_size) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0, Some(&backfill)).unwrap();
    }
}
