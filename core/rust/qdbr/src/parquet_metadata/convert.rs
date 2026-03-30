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

use crate::parquet::error::ParquetResult;
use crate::parquet::qdb_metadata::{QdbMeta, QdbMetaColFormat};
use crate::parquet_metadata::column_chunk::ColumnChunkRaw;
use crate::parquet_metadata::error::{parquet_meta_err, ParquetMetaErrorKind};
use crate::parquet_metadata::row_group::RowGroupBlockBuilder;
use crate::parquet_metadata::types::{
    encode_stat_sizes, BlockAlignedOffset, Codec, ColumnFlags, EncodingMask, FieldRepetition,
    StatFlags,
};
use crate::parquet_metadata::writer::ParquetMetaWriter;
use crate::parquet_read::decoders::int32::DayToMillisConverter;
use crate::parquet_read::decoders::int96::Int96ToTimestampConverter;
use parquet2::metadata::FileMetaData;
use parquet2::schema::types::{PhysicalType, PrimitiveLogicalType, TimeUnit};
use parquet2::statistics::{BinaryStatistics, BooleanStatistics, PrimitiveStatistics, Statistics};
use qdb_core::col_type::ColumnTypeTag;

/// Converts a parquet file's metadata into a `_pm` binary representation.
///
/// # Arguments
/// - `file_metadata` - Parquet file metadata from `read_metadata_with_size()`.
/// - `qdb_meta` - Optional QuestDB-specific metadata (from the parquet footer's
///   `"questdb"` key-value pair). If `None`, column types are inferred from the
///   parquet schema and tops default to 0.
/// - `parquet_footer_offset` - Byte offset of the parquet footer in the parquet file.
/// - `parquet_footer_length` - Length of the parquet footer in bytes.
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

    // Add sorting columns.
    for sc in &sorting_cols {
        writer.add_sorting_column(sc.column_idx as u32);
    }

    // Add column descriptors.
    for (i, col_desc) in columns.iter().enumerate() {
        let field_info = col_desc.base_type.get_field_info();
        let name = &field_info.name;
        let id = field_info.id.unwrap_or(-1);

        let (col_type_code, top) = if let Some(meta) = qdb_meta {
            let col_meta = &meta.schema[i];
            (col_meta.column_type.code(), col_meta.column_top as u64)
        } else {
            // Without QdbMeta, we store -1 as type (unknown) and 0 as top.
            (-1i32, 0u64)
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

        writer.add_column(top, name, id, col_type_code, flags);
    }

    // Add row groups.
    for rg in &file_metadata.row_groups {
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
            let col_type_tag = qdb_meta.and_then(|m| {
                let code = m.schema[col_idx].column_type.tag() as u8;
                ColumnTypeTag::try_from(code).ok()
            });

            let chunk = build_column_chunk(col_chunk, col_type_tag)?;
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

    writer.finish()
}

struct BuiltChunk {
    raw: ColumnChunkRaw,
    ool_min: Option<Vec<u8>>,
    ool_max: Option<Vec<u8>>,
}

fn build_column_chunk(
    col_chunk: &parquet2::metadata::ColumnChunkMetaData,
    col_type_tag: Option<ColumnTypeTag>,
) -> ParquetResult<BuiltChunk> {
    let (byte_range_start, total_compressed) = col_chunk.byte_range();
    let codec = Codec::from(col_chunk.compression());
    // column_encoding() returns parquet_format_safe's Encoding; convert to parquet2's.
    let p2_encodings: Vec<parquet2::encoding::Encoding> = col_chunk
        .column_encoding()
        .iter()
        .filter_map(|e| parquet2::encoding::Encoding::try_from(*e).ok())
        .collect();
    let encodings = EncodingMask::from(p2_encodings.as_slice());

    let bloom_filter_off = col_chunk
        .metadata()
        .bloom_filter_offset
        .map(|off| BlockAlignedOffset::from_byte_offset(off as u64))
        .transpose()?
        .unwrap_or(BlockAlignedOffset::ZERO);

    let mut raw = ColumnChunkRaw::zeroed();
    raw.codec = codec as u8;
    raw.encodings = encodings.0;
    raw.bloom_filter_off = bloom_filter_off;
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

    Ok(BuiltChunk { raw, ool_min, ool_max })
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
        PhysicalType::ByteArray | PhysicalType::FixedLenByteArray(_) => {
            let Some(s) = stats.as_any().downcast_ref::<BinaryStatistics>() else {
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

struct SortingCol {
    column_idx: i32,
    descending: bool,
}

fn extract_sorting_columns(file_metadata: &FileMetaData) -> ParquetResult<Vec<SortingCol>> {
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

fn detect_designated_timestamp(
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
            required: true,
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

        let (pm_bytes, footer_offset) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0).unwrap();

        let reader = ParquetMetaReader::new(&pm_bytes, footer_offset).unwrap();
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

        let (pm_bytes, footer_offset) = convert_from_parquet(&metadata, None, 0, 0).unwrap();

        let reader = ParquetMetaReader::new(&pm_bytes, footer_offset).unwrap();
        let chunk = reader.row_group(0).unwrap().column_chunk(0).unwrap();
        assert_eq!(chunk.codec().unwrap(), Codec::Snappy);
    }

    #[test]
    fn convert_without_qdb_meta() {
        let parquet_data = write_test_parquet(50, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        let (pm_bytes, footer_offset) = convert_from_parquet(&metadata, None, 0, 0).unwrap();

        let reader = ParquetMetaReader::new(&pm_bytes, footer_offset).unwrap();
        assert_eq!(reader.column_count(), 1);
        let desc = reader.column_descriptor(0).unwrap();
        assert_eq!(desc.col_type, -1); // Unknown without QdbMeta.
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

        let result = convert_from_parquet(&metadata, Some(&bad_meta), 0, 0);
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
                required: true,
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
                required: false,
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
                required: false,
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
                required: false,
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

        let (pm_bytes, footer_offset) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 1024, 200).unwrap();

        let reader = ParquetMetaReader::new(&pm_bytes, footer_offset).unwrap();
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

        let (pm_bytes, footer_offset) = convert_from_parquet(&metadata, Some(&meta), 0, 0).unwrap();

        let reader = ParquetMetaReader::new(&pm_bytes, footer_offset).unwrap();
        let desc = reader.column_descriptor(0).unwrap();
        let flags = desc.flags();
        assert!(flags.is_local_key_global());
        assert!(flags.is_ascii());
        assert_eq!(desc.top, 42);
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
        let (pm_bytes, footer_offset) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0).unwrap();

        let reader = ParquetMetaReader::new(&pm_bytes, footer_offset).unwrap();

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
            required: true,
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
        let (pm_bytes, footer_offset) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0).unwrap();

        let reader = ParquetMetaReader::new(&pm_bytes, footer_offset).unwrap();
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
            required: false,
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

        let (pm_bytes, footer_offset) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0).unwrap();

        let reader = ParquetMetaReader::new(&pm_bytes, footer_offset).unwrap();
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
        let stats = BinaryStatistics {
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

        let (pm_bytes, footer_offset) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0).unwrap();

        let reader = ParquetMetaReader::new(&pm_bytes, footer_offset).unwrap();
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
}
