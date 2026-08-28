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

//! `_im` generation from a finished streaming parquet write.
//!
//! `_im` records, per (row group, column), the codec, the encodings present,
//! the byte range, the null count and the min/max statistics. None of those
//! values exist Java-side: the parquet encoder produces them and they live
//! only in the writer's own thrift metadata. Production `_im` is therefore
//! built here rather than through `IndexMetaFileWriter`'s per-row-group JNI
//! surface, for the same reason production `_pm` is built by
//! [`crate::parquet_metadata::generate_parquet_metadata`] rather than by
//! `ParquetMetaFileWriter`.
//!
//! Every check the `_im` writer performs stays in force here -- in particular
//! the key-alignment invariant, which rejects an index whose row groups split
//! a key across a group shared with another key. Nothing at read time can
//! detect that violation, so the rejection is allowed to propagate to the
//! caller rather than being caught or worked around.
//!
//! The format specification lives in `docs/index-metadata.md`.

use std::io::Write;

use parquet2::metadata::ColumnDescriptor;
use parquet2::schema::types::PhysicalType;

use crate::parquet::error::{fmt_err, ParquetError, ParquetResult};
use crate::parquet_metadata::physical_type_to_u8;
use crate::parquet_write::file::ChunkedWriter;
use qdb_parquet_meta::convert::build_row_group_block;
use qdb_parquet_meta::index_meta::IndexMetaWriter;
use qdb_parquet_meta::types::{ColumnFlags, FieldRepetition};
use qdb_parquet_meta::{
    infer_column_type, ColumnDescriptorRaw, NoBloomFilterSource, QdbMeta, QdbMetaCol,
    QdbMetaColFormat, QDB_META_KEY,
};

/// Bytes a parquet file carries after its footer: the 4-byte footer length and
/// the `PAR1` magic. `_im` records the footer's offset and length, and the
/// index parquet's committed size follows as `offset + length + 8` -- the same
/// derivation `_pm` makes for `data.parquet`.
const PARQUET_FOOTER_TRAILER_LEN: u64 = 8;

/// Builds the complete `_im` for a covering-index parquet the streaming writer
/// has just finished.
///
/// `first_keys`, `row_id_mins` and `row_id_maxs` carry one entry per index row
/// group, in row-group order, and must all have the writer's row-group count.
/// `data_boundaries` holds `data.parquet`'s cumulative row counts, one more
/// entry than it has row groups, starting at `0`.
///
/// `key_space_size` is the **exclusive upper bound on key ids** -- the native
/// reader's `keyCountIncludingNulls` -- not a count of distinct keys present.
/// Occupancy is sparse, and a distinct-key count here would make every key
/// above the first report as absent.
///
/// `key_id_column` and `row_id_column` are descriptor indices of the synthetic
/// columns (`row_id_column` is `-1` under the row-per-key payload), and
/// `first_cover_column` is the descriptor index of cover slot 0: the synthetic
/// columns come first, then the covered columns in cover-slot order.
///
/// # Errors
/// Any violation of the `_im` writer's checks, including the key-alignment
/// invariant, is returned rather than written out.
#[allow(clippy::too_many_arguments)]
pub fn generate_index_metadata<W: Write>(
    written: &ChunkedWriter<W>,
    pidx_file_size: u64,
    first_keys: &[u32],
    row_id_mins: &[i64],
    row_id_maxs: &[i64],
    data_boundaries: &[i64],
    key_dirs: &[Vec<u32>],
    key_space_size: u32,
    key_id_column: i32,
    row_id_column: i32,
    first_cover_column: u32,
    payload_kind: u32,
    logical_row_counts: &[i64],
) -> ParquetResult<Vec<u8>> {
    let row_groups = written.row_groups();
    // Under a payload kind that stores a whole row group's postings as ONE
    // parquet row, the parquet metadata's row count is 1 and says nothing about
    // how many postings the group holds. The _im must record the POSTING count,
    // because that is what a reader iterates. So the logical counts are an
    // INPUT here rather than something read back off the footer.
    if !logical_row_counts.is_empty() && logical_row_counts.len() != row_groups.len() {
        return Err(fmt_err!(
            InvalidLayout,
            "index metadata has {} logical row counts but {} row groups",
            logical_row_counts.len(),
            row_groups.len()
        ));
    }
    if !key_dirs.is_empty() && key_dirs.len() != row_groups.len() {
        return Err(fmt_err!(
            InvalidLayout,
            "index metadata has {} key directories but {} row groups",
            key_dirs.len(),
            row_groups.len()
        ));
    }
    if first_keys.len() != row_groups.len()
        || row_id_mins.len() != row_groups.len()
        || row_id_maxs.len() != row_groups.len()
    {
        return Err(fmt_err!(
            InvalidLayout,
            "index row group directory has {} first keys, {} row id minima and {} row id maxima \
             for {} row groups",
            first_keys.len(),
            row_id_mins.len(),
            row_id_maxs.len(),
            row_groups.len()
        ));
    }

    // The parquet footer's position is what lets a reader decode index bytes
    // pulled from cold storage without reading the footer, and its length is
    // what makes the index parquet's committed size derivable without an
    // `ff.length()` call. Both subtractions are checked: `parquet_footer_offset`
    // is zero until the write is finished, and a caller that generates `_im`
    // too early must get an error rather than a plausible-looking length.
    let pidx_footer_offset = written.parquet_footer_offset();
    let pidx_footer_length = pidx_file_size
        .checked_sub(pidx_footer_offset)
        .and_then(|tail| tail.checked_sub(PARQUET_FOOTER_TRAILER_LEN))
        .and_then(|length| u32::try_from(length).ok())
        .ok_or_else(|| {
            fmt_err!(
                InvalidLayout,
                "index parquet footer at offset {} does not fit in a file of {} bytes",
                pidx_footer_offset,
                pidx_file_size
            )
        })?;

    let mut writer = IndexMetaWriter::new(
        payload_kind,
        key_space_size,
        key_id_column,
        row_id_column,
        first_cover_column,
    );
    writer.set_pidx_footer(pidx_footer_offset, pidx_footer_length);
    writer.set_data_row_group_boundaries(data_boundaries);

    let columns = written.schema().columns();
    let qdb_meta = extract_written_qdb_meta(written)?;
    if let Some(meta) = qdb_meta.as_ref() {
        if meta.schema.len() != columns.len() {
            return Err(fmt_err!(
                InvalidLayout,
                "index parquet QuestDB metadata has {} columns but its schema has {}",
                meta.schema.len(),
                columns.len()
            ));
        }
    }
    for (i, column) in columns.iter().enumerate() {
        let qdb_col = qdb_meta.as_ref().map(|meta| &meta.schema[i]);
        let (name, descriptor) = index_column_descriptor(column, qdb_col)?;
        writer.add_column(name, descriptor);
    }

    for (i, row_group) in row_groups.iter().enumerate() {
        // `_im` has no bloom filter section -- its extra sections are the key
        // directory and the data row-group boundaries -- so no bitset is
        // resolved here.
        let mut block = build_row_group_block(row_group, i, &NoBloomFilterSource)?;
        if !logical_row_counts.is_empty() {
            block.set_num_rows(logical_row_counts[i].max(0) as u64);
        }
        let key_dir: &[u32] = key_dirs.get(i).map(|d| d.as_slice()).unwrap_or(&[]);
        writer.add_row_group(first_keys[i], row_id_mins[i], row_id_maxs[i], key_dir, block);
    }

    writer.finish().map_err(ParquetError::from)
}

/// Reads the `"questdb"` key-value entry from the finished write's own footer
/// metadata. Returns `Ok(None)` before the write is finished, or when the
/// entry is absent -- the descriptors then fall back to the parquet schema.
fn extract_written_qdb_meta<W: Write>(
    written: &ChunkedWriter<W>,
) -> ParquetResult<Option<QdbMeta>> {
    let Some(key_values) = written.key_value_metadata() else {
        return Ok(None);
    };
    let Some(kv) = key_values.iter().find(|kv| kv.key == QDB_META_KEY) else {
        return Ok(None);
    };
    let Some(json) = kv.value.as_deref() else {
        return Ok(None);
    };
    Ok(Some(
        QdbMeta::deserialize(json).map_err(ParquetError::from)?,
    ))
}

/// Builds one `_im` column descriptor -- `_pm`'s 32-byte structure -- from the
/// index parquet's schema, preferring QuestDB's own metadata where it has it.
///
/// `ID` is the covered column's QuestDB writer index, or `-1` for the synthetic
/// `key_id` / `row_id` columns; the writer rejects a descriptor order that puts
/// either on the wrong side of `FIRST_COVER_COLUMN`. `TYPE` comes from
/// `QdbMeta` when present because inference cannot distinguish QuestDB types
/// that share a parquet physical type, and a covered column's descriptor type
/// is what a query decodes its values with.
fn index_column_descriptor<'a>(
    column: &'a ColumnDescriptor,
    qdb_col: Option<&QdbMetaCol>,
) -> ParquetResult<(&'a str, ColumnDescriptorRaw)> {
    let field_info = column.base_type.get_field_info();
    let primitive = &column.descriptor.primitive_type;

    let mut flags =
        ColumnFlags::new().with_repetition(FieldRepetition::from(field_info.repetition));
    if let Some(qdb_col) = qdb_col {
        if qdb_col.format == Some(QdbMetaColFormat::LocalKeyIsGlobal) {
            flags = flags.with_local_key_is_global();
        }
        if qdb_col.ascii == Some(true) {
            flags = flags.with_ascii();
        }
    }

    let col_type = qdb_col
        .map(|qdb_col| qdb_col.column_type.code())
        .unwrap_or_else(|| infer_column_type(column).map(|ct| ct.code()).unwrap_or(-1));
    // QuestDB stamps the writer index into both the `QdbMeta` id and the
    // parquet `field_id`, and the synthetic columns carry -1 in both.
    let id = qdb_col
        .and_then(|qdb_col| qdb_col.id)
        .or(field_info.id)
        .unwrap_or(-1);
    let fixed_byte_len = match primitive.physical_type {
        PhysicalType::FixedLenByteArray(len) => len as i32,
        _ => 0,
    };
    let max_rep_level = u8::try_from(column.descriptor.max_rep_level).map_err(|_| {
        fmt_err!(
            InvalidLayout,
            "max_rep_level {} does not fit in u8",
            column.descriptor.max_rep_level
        )
    })?;
    let max_def_level = u8::try_from(column.descriptor.max_def_level).map_err(|_| {
        fmt_err!(
            InvalidLayout,
            "max_def_level {} does not fit in u8",
            column.descriptor.max_def_level
        )
    })?;

    Ok((
        field_info.name.as_str(),
        ColumnDescriptorRaw {
            // The writer owns name_offset / name_length and backpatches both.
            name_offset: 0,
            id,
            col_type,
            flags: flags.0,
            fixed_byte_len,
            name_length: 0,
            physical_type: physical_type_to_u8(primitive.physical_type),
            max_rep_level,
            max_def_level,
            _reserved: 0,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::parquet::tests::ColumnTypeTagExt;
    use crate::parquet_write::file::ParquetWriter;
    use crate::parquet_write::schema::{
        to_compressions, to_encodings, to_parquet_schema, Column, ParquetEncodingConfig, Partition,
    };
    use parquet2::write::Version;
    use qdb_core::col_type::ColumnTypeTag;
    use qdb_parquet_meta::{IndexMetaReader, SeqTxn};

    /// One test row group's rows: `key` repeated over `row_ids`.
    fn key_run(key: i32, row_ids: std::ops::Range<i64>) -> Vec<(i32, i64)> {
        row_ids.map(|row_id| (key, row_id)).collect()
    }

    /// Leaks `values` and returns the bytes backing them. `Column` borrows
    /// `&'static [u8]` because production data is memory-mapped; leaking the
    /// typed vector (rather than a byte copy) keeps the allocation aligned for
    /// the element type the encoder transmutes it back to.
    fn leak_as_bytes<T: Copy + 'static>(values: Vec<T>) -> &'static [u8] {
        let leaked: &'static [T] = Vec::leak(values);
        // SAFETY: `leaked` is a live `&'static [T]`; any `T: Copy` is readable
        // as its own bytes, and the length is taken from the same slice.
        unsafe {
            std::slice::from_raw_parts(leaked.as_ptr() as *const u8, std::mem::size_of_val(leaked))
        }
    }

    /// Writes a covering-index parquet -- `(key_id INT, row_id LONG, price
    /// DOUBLE)` -- one row group per entry in `row_groups`, and returns the
    /// finished writer plus the parquet file size.
    ///
    /// Each row group is closed at an explicit row boundary rather than by the
    /// fixed row-group threshold, which is what a key-aligned index write does.
    fn write_test_index_parquet(row_groups: &[Vec<(i32, i64)>]) -> (ChunkedWriter<Vec<u8>>, u64) {
        let rows: Vec<(i32, i64)> = row_groups.iter().flatten().copied().collect();
        let row_count = rows.len();
        let key_ids = leak_as_bytes(rows.iter().map(|(key, _)| *key).collect::<Vec<i32>>());
        let row_ids = leak_as_bytes(rows.iter().map(|(_, row_id)| *row_id).collect::<Vec<i64>>());
        let prices = leak_as_bytes(
            rows.iter()
                .map(|(_, row_id)| *row_id as f64 / 4.0)
                .collect::<Vec<f64>>(),
        );

        let column =
            |id: i32, name: &'static str, tag: ColumnTypeTag, data: &'static [u8]| Column {
                id,
                name,
                data_type: tag.into_type(),
                row_count,
                column_top: 0,
                primary_data: data,
                secondary_data: &[],
                symbol_offsets: &[],
                designated_timestamp: false,
                not_null_hint: false,
                strided_timestamp_16: false,
                designated_timestamp_ascending: false,
                parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
            };

        // The synthetic columns carry id -1 and come first; the covered column
        // carries its QuestDB writer index. `_im` requires exactly that order.
        let partition = Partition {
            table: "pidx".to_string(),
            columns: vec![
                column(-1, "key_id", ColumnTypeTag::Int, key_ids),
                column(-1, "row_id", ColumnTypeTag::Long, row_ids),
                column(7, "price", ColumnTypeTag::Double, prices),
            ],
        };

        let (schema, additional_meta) =
            to_parquet_schema(&partition, false, -1, SeqTxn::UNSET).unwrap();
        let encodings = to_encodings(&partition);
        let compressions = to_compressions(&partition);
        let mut chunked = ParquetWriter::new(Vec::new())
            .with_statistics(true)
            .with_version(Version::V1)
            .chunked_with_compressions(schema, encodings, compressions)
            .unwrap();

        let mut offset = 0usize;
        for group in row_groups {
            chunked
                .write_row_group_from_partitions(&[&partition], offset, offset + group.len())
                .unwrap();
            offset += group.len();
        }
        let file_size = chunked.finish(additional_meta).unwrap();
        (chunked, file_size)
    }

    #[test]
    fn test_generates_an_im_matching_the_written_row_groups() {
        // Three row groups whose first keys are 0, 5 and 5 -- the last two a
        // dedicated run for key 5, which the `_im` key-alignment invariant
        // permits and which is the hot-key layout the format exists to express.
        let (written, file_size) =
            write_test_index_parquet(&[key_run(0, 0..10), key_run(5, 10..20), key_run(5, 20..30)]);
        let im = generate_index_metadata(
            &written,
            file_size,
            &[0, 5, 5],
            &[0, 10, 20],
            &[9, 19, 29],
            &[0, 15, 30],
            &[],
            10,
            0,
            1,
            2,
            0,
            &[],
        )
        .unwrap();

        let reader = IndexMetaReader::new(&im).unwrap();
        assert_eq!(reader.index_row_group_count(), 3);
        assert_eq!(reader.key_space_size(), 10);
        assert_eq!(reader.row_group_range_for_key(5), Some((1, 2)));
        assert_eq!(reader.row_group_row_id_min(1).unwrap(), 10);
        assert_eq!(reader.row_group_row_id_max(2).unwrap(), 29);
        assert_eq!(reader.data_row_group_boundary(2).unwrap(), 30);
        assert!(reader.pidx_footer_offset() > 0);
        assert_eq!(
            reader.pidx_file_size().unwrap(),
            file_size,
            "the committed index parquet size must be derivable from the recorded footer"
        );

        // Descriptor order is fixed and positional: the synthetic columns come
        // first with ID -1, then the covered columns in cover-slot order, so
        // cover slot 0 resolves to descriptor FIRST_COVER_COLUMN.
        assert_eq!(reader.column_count(), 3);
        assert_eq!(reader.column_name(0).unwrap(), "key_id");
        assert_eq!(reader.column_name(1).unwrap(), "row_id");
        assert_eq!(reader.column_name(2).unwrap(), "price");
        assert_eq!(reader.column_descriptor(0).unwrap().id, -1);
        assert_eq!(reader.column_descriptor(1).unwrap().id, -1);
        assert_eq!(reader.cover_column_index(0).unwrap(), 2);
        let price = reader.column_descriptor(2).unwrap();
        assert_eq!(price.id, 7, "a covered column's ID is its writer index");
        assert_eq!(
            price.col_type,
            ColumnTypeTag::Double.into_type().code(),
            "a covered column's TYPE is its QuestDB column type"
        );

        // The values that exist only inside the writer's thrift metadata: a
        // real byte range and a real compressed size per column chunk. These
        // are what make `_im` sufficient to decode index bytes on its own.
        let block = reader.row_group_block(1).unwrap();
        assert_eq!(block.num_rows(), 10);
        let key_chunk = block.column_chunk(0).unwrap();
        assert!(key_chunk.byte_range_start > 0);
        assert!(key_chunk.total_compressed > 0);
        assert_eq!(key_chunk.num_values, 10);
        assert_eq!(key_chunk.min_stat, 5);
        assert_eq!(key_chunk.max_stat, 5);
    }

    /// The key-alignment invariant is the single most important check the `_im`
    /// writer performs: the reader's `rg_lo` resolves an exact match to the
    /// *first* `RG_FIRST_KEY` entry equal to the key, so a key that is both the
    /// last key of a packed row group and the first key of the next has its
    /// postings in the packed group silently dropped. No reader can detect it.
    /// This asserts the rejection reaches the production generation path rather
    /// than living only in the `_im` writer's own unit tests.
    #[test]
    fn test_rejects_a_key_split_across_a_shared_row_group() {
        // Row group 1 holds keys 5 and 6, and row group 2 starts at key 6: key
        // 6 is split across a group it shares with key 5.
        let mut shared = key_run(5, 10..15);
        shared.extend(key_run(6, 15..20));
        let (written, file_size) =
            write_test_index_parquet(&[key_run(0, 0..10), shared, key_run(6, 20..30)]);

        let err = generate_index_metadata(
            &written,
            file_size,
            &[0, 5, 6],
            &[0, 10, 20],
            &[9, 19, 29],
            &[0, 15, 30],
            &[],
            10,
            0,
            1,
            2,
            0,
            &[],
        )
        .unwrap_err();

        let message = format!("{err}");
        assert!(
            message
                .contains("a key must not be split across a row group it shares with another key"),
            "expected a key-alignment rejection, got: {message}"
        );
    }
}
