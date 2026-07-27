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

//! qdbr-side write path adapters around the shared parquet-to-`_pm` converter.
//!
//! The actual conversion logic lives in `qdb_parquet_meta::convert`. This
//! module re-exports the shared API under the historical `convert::*` path
//! and adds two qdbr-only wrappers that take pre-captured bloom-filter
//! bitsets (`&[Vec<Option<Vec<u8>>>]`) instead of a `BloomFilterSource`,
//! preserving the existing write-path callers in `parquet_write/jni.rs` and
//! `parquet_write/update.rs`.

use parquet2::thrift_format::{ColumnMetaData, RowGroup};

use crate::parquet::error::{parquet_meta_err, ParquetError, ParquetResult};
use qdb_parquet_meta::convert::{build_row_group_block, BloomFilterSource};
use qdb_parquet_meta::error::{ParquetMetaErrorKind, ParquetMetaResult};
use qdb_parquet_meta::types::SeqTxn;

pub use qdb_parquet_meta::convert::{
    convert_from_parquet, detect_designated_timestamp, extract_sorting_columns,
    physical_type_to_u8, validate_file_paths, BloomFilterSource as SharedBloomFilterSource,
    NoBloomFilterSource, ParquetMetaColumnInfo, SliceBloomFilterSource, SortingCol,
    TsStatsBackfill,
};

/// Bloom-filter source backed by pre-captured bitsets indexed by
/// `(row_group, column)`. The parquet write path captures these while writing
/// each row group; the shared converter then inlines them through this view.
pub struct VecBloomFilterSource<'a> {
    bitsets: &'a [Vec<Option<Vec<u8>>>],
}

impl<'a> VecBloomFilterSource<'a> {
    pub fn new(bitsets: &'a [Vec<Option<Vec<u8>>>]) -> Self {
        Self { bitsets }
    }
}

impl BloomFilterSource for VecBloomFilterSource<'_> {
    fn bitset<'a>(
        &'a self,
        row_group: usize,
        column: usize,
        _meta: &ColumnMetaData,
    ) -> ParquetMetaResult<Option<&'a [u8]>> {
        let Some(rg) = self.bitsets.get(row_group) else {
            return Ok(None);
        };
        let Some(cell) = rg.get(column) else {
            return Ok(None);
        };
        Ok(cell.as_deref())
    }
}

/// Result of an incremental `_pm` update.
///
/// `bytes` is always append-only: the caller seeks to the existing file size
/// and writes them after the previous trailer. The previous bytes (including
/// the previous trailer) are left untouched, preserving the stale-reader
/// invariant that any earlier committed `parquet_meta_file_size` continues to
/// resolve to a consistent older snapshot.
///
/// After appending `bytes`, the caller must patch `new_file_size` into the
/// header at `HEADER_PARQUET_META_FILE_SIZE_OFF` as the last step — this is
/// the MVCC commit signal that publishes the new snapshot.
#[derive(Debug)]
pub struct ParquetMetaUpdateResult {
    /// Bytes to append at the existing file size.
    pub bytes: Vec<u8>,
    /// Total `_pm` file size after the append. Also the value the caller must
    /// patch into the header at `HEADER_PARQUET_META_FILE_SIZE_OFF`.
    pub new_file_size: u64,
}

/// Generates a complete `_pm` file from pre-built column descriptors and raw
/// thrift row groups, inlining the bloom bitsets captured during the parquet
/// write.
#[allow(clippy::too_many_arguments)]
pub fn generate_parquet_metadata(
    columns: &[ParquetMetaColumnInfo<'_>],
    thrift_row_groups: &[RowGroup],
    designated_timestamp: i32,
    sorting_columns: &[u32],
    parquet_footer_offset: u64,
    parquet_footer_length: u32,
    bloom_bitsets: &[Vec<Option<Vec<u8>>>],
    unused_bytes: u64,
    squash_tracker: i64,
    seq_txn: SeqTxn,
) -> ParquetResult<(Vec<u8>, u64)> {
    let bloom_source = VecBloomFilterSource::new(bloom_bitsets);
    qdb_parquet_meta::convert::generate_parquet_metadata(
        columns,
        thrift_row_groups,
        designated_timestamp,
        sorting_columns,
        parquet_footer_offset,
        parquet_footer_length,
        unused_bytes,
        squash_tracker,
        seq_txn,
        &bloom_source,
    )
    .map_err(ParquetError::from)
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
/// Returns an error when the new row group count is smaller than the existing
/// one: the writer's row-group entry list cannot drop existing references, so
/// the caller must escalate to a full rewrite.
#[allow(clippy::too_many_arguments)]
pub fn update_parquet_metadata(
    existing_parquet_meta: &[u8],
    existing_parquet_meta_file_size: u64,
    append_base: u64,
    thrift_row_groups: &[RowGroup],
    parquet_footer_offset: u64,
    parquet_footer_length: u32,
    bloom_bitsets: &[Vec<Option<Vec<u8>>>],
    unused_bytes: u64,
    seq_txn: SeqTxn,
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

    let existing_reader = qdb_parquet_meta::reader::ParquetMetaReader::from_file_size(
        existing_parquet_meta,
        existing_parquet_meta_file_size,
    )?;
    let existing_rg_count = existing_reader.row_group_count() as usize;

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
    // footer that references fewer row groups, leaving the dropped blocks as
    // dead space), but the current `ParquetMetaUpdateWriter` exposes only
    // replace/add — it has no remove operation, and the loop below only
    // iterates `thrift_row_groups`, so existing entries beyond the new length
    // would leak into the new footer as stale references. None of the Java
    // callers in `O3PartitionJob` shrink the row group count today, so this
    // is a defensive guard rather than a load-bearing check; it stays so any
    // future caller that violates the invariant fails loudly instead of
    // silently corrupting the file.
    if thrift_row_groups.len() < existing_rg_count {
        return Err(parquet_meta_err!(
            ParquetMetaErrorKind::InvalidValue,
            "_pm in-place update cannot shrink row group count ({} -> {}); caller must escalate to rewrite mode (new nameTxn directory)",
            existing_rg_count,
            thrift_row_groups.len()
        ));
    }

    let mut updater = qdb_parquet_meta::writer::ParquetMetaUpdateWriter::new(
        existing_parquet_meta,
        existing_parquet_meta_file_size,
    )?;

    let bloom_source = VecBloomFilterSource::new(bloom_bitsets);
    for (i, thrift_rg) in thrift_row_groups.iter().enumerate() {
        let new_fp: Option<u64> = thrift_rg
            .columns
            .first()
            .and_then(|c| c.meta_data.as_ref())
            .map(|m| m.dictionary_page_offset.unwrap_or(m.data_page_offset) as u64);

        if i < existing_rg_count && existing_fingerprints[i] == new_fp {
            continue;
        }

        let block = build_row_group_block(thrift_rg, i, &bloom_source)?;

        if i < existing_rg_count {
            updater.replace_row_group(i, block)?;
        } else {
            updater.add_row_group(block);
        }
    }

    updater.parquet_footer(parquet_footer_offset, parquet_footer_length);
    updater.unused_bytes(unused_bytes);
    updater.seq_txn(seq_txn);
    let (append_bytes, new_file_size) = updater.finish_appending_at(append_base)?;
    debug_assert_eq!(new_file_size, append_base + append_bytes.len() as u64);

    Ok(ParquetMetaUpdateResult { bytes: append_bytes, new_file_size })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet::qdb_metadata::{QdbMeta, QdbMetaColFormat};
    use crate::parquet::tests::ColumnTypeTagExt;
    use crate::parquet_write::file::ParquetWriter;
    use crate::parquet_write::schema::{Column, ParquetEncodingConfig, Partition};
    use parquet2::compression::CompressionOptions;
    use parquet2::metadata::FileMetaData;
    use parquet2::read::read_metadata_with_size;
    use parquet2::schema::types::PhysicalType;
    use parquet2::write::Version;
    use qdb_core::col_type::ColumnTypeTag;
    use qdb_parquet_meta::reader::ParquetMetaReader;
    use qdb_parquet_meta::types::{Codec, ColumnFlags, FieldRepetition, StatFlags};
    use std::io::Cursor;

    fn write_test_parquet(row_count: usize, compression: CompressionOptions) -> Vec<u8> {
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

        let (parquet_meta_bytes, parquet_meta_file_size) = convert_from_parquet(
            &metadata,
            qdb_meta.as_ref(),
            0,
            0,
            &NoBloomFilterSource,
            None,
        )
        .unwrap();

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
    fn convert_with_compression() {
        let parquet_data = write_test_parquet(1000, CompressionOptions::Snappy);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, None, 0, 0, &NoBloomFilterSource, None).unwrap();

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
            convert_from_parquet(&metadata, None, 0, 0, &NoBloomFilterSource, None).unwrap();

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
        let parquet_data = write_test_parquet(10, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        let mut qdb_meta = extract_qdb_meta_from(&metadata).expect("test parquet has qdb meta");
        qdb_meta.squash_tracker = 42;

        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, Some(&qdb_meta), 0, 0, &NoBloomFilterSource, None)
                .unwrap();
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
            convert_from_parquet(&metadata, Some(&qdb_meta), 0, 0, &NoBloomFilterSource, None)
                .unwrap();
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
            convert_from_parquet(&metadata, None, 0, 0, &NoBloomFilterSource, None).unwrap();
        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        assert!(!reader.feature_flags().has_squash_tracker());
        assert_eq!(reader.squash_tracker(), None);
    }

    #[test]
    fn convert_propagates_seq_txn_from_qdb_meta() {
        let parquet_data = write_test_parquet(10, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        let mut qdb_meta = extract_qdb_meta_from(&metadata).expect("test parquet has qdb meta");
        qdb_meta.seq_txn = 77;

        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, Some(&qdb_meta), 0, 0, &NoBloomFilterSource, None)
                .unwrap();
        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        assert!(reader.footer_feature_flags().has_seq_txn());
        assert_eq!(reader.seq_txn(), Some(SeqTxn::new(77)));
    }

    #[test]
    fn convert_omits_seq_txn_when_neg_one() {
        let parquet_data = write_test_parquet(10, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        let mut qdb_meta = extract_qdb_meta_from(&metadata).expect("test parquet has qdb meta");
        qdb_meta.seq_txn = -1;

        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, Some(&qdb_meta), 0, 0, &NoBloomFilterSource, None)
                .unwrap();
        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        assert!(!reader.footer_feature_flags().has_seq_txn());
        assert_eq!(reader.seq_txn(), None);
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

        let result =
            convert_from_parquet(&metadata, Some(&bad_meta), 0, 0, &NoBloomFilterSource, None);
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
            convert_from_parquet(&metadata, Some(&qdb_meta), 0, 0, &NoBloomFilterSource, None)
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
            convert_from_parquet(&metadata, Some(&qdb_meta), 0, 0, &NoBloomFilterSource, None)
                .expect("convert_from_parquet must resolve sorting from qdb_meta, not the footer");
        let reader = ParquetMetaReader::from_file_size(&pm_bytes, pm_size).unwrap();
        assert_eq!(reader.designated_timestamp(), Some(0));
        assert_eq!(reader.sorting_column_count(), 1);
        // The dense designated position (0), not the stale footer index 1.
        assert_eq!(reader.sorting_column(0).unwrap(), 0);

        // Scoped to QuestDB files: without qdb_meta the conflict still aborts.
        assert!(convert_from_parquet(&metadata, None, 0, 0, &NoBloomFilterSource, None).is_err());
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

        let (parquet_meta_bytes, parquet_meta_file_size) = convert_from_parquet(
            &metadata,
            qdb_meta.as_ref(),
            1024,
            200,
            &NoBloomFilterSource,
            None,
        )
        .unwrap();

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

        let ts_chunk = rg.column_chunk(0).unwrap();
        let ts_flags = ts_chunk.stat_flags();
        assert!(ts_flags.has_min_stat());
        assert!(ts_flags.has_max_stat());
        assert!(ts_flags.has_null_count());
        assert_eq!(ts_chunk.num_values, 100);

        let int_chunk = rg.column_chunk(1).unwrap();
        let int_flags = int_chunk.stat_flags();
        assert!(int_flags.has_min_stat());
        assert!(int_flags.has_max_stat());

        let dbl_chunk = rg.column_chunk(2).unwrap();
        let dbl_flags = dbl_chunk.stat_flags();
        assert!(dbl_flags.has_min_stat());
        assert!(dbl_flags.has_max_stat());

        let bool_chunk = rg.column_chunk(3).unwrap();
        let bool_flags = bool_chunk.stat_flags();
        assert!(bool_flags.has_min_stat());
        assert!(bool_flags.has_max_stat());
    }

    /// Regression: a SHORT column with negative values must round-trip
    /// through the inline u64 stat slot and read back as the correct i32 at
    /// parquet physical width (Int32). Earlier the convert path narrowed to 2
    /// bytes, the inline slot zero-padded to 8, and the skip path's 4-byte
    /// i32 read turned a -74 i16 into 65462, dropping every row group whose
    /// true min was negative.
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

        let (parquet_meta_bytes, parquet_meta_file_size) = convert_from_parquet(
            &metadata,
            qdb_meta.as_ref(),
            0,
            0,
            &NoBloomFilterSource,
            None,
        )
        .unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        let chunk = reader.row_group(0).unwrap().column_chunk(1).unwrap();

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

    /// Regression: an external INT32 + Date logical-type parquet must keep
    /// its stats as i32 days at parquet physical width when ingested through
    /// the inline path.
    #[test]
    fn convert_int32_date_round_trips_at_int32_width() {
        use parquet2::thrift_format::{
            ColumnChunk, ColumnMetaData, CompressionCodec, Encoding as ThriftEncoding,
            RowGroup as ThriftRowGroup, Statistics as ThriftStatistics, Type,
        };

        let min_days: i32 = -100;
        let max_days: i32 = 365;

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
        let row_group = ThriftRowGroup {
            columns: vec![column_chunk],
            total_byte_size: 400,
            num_rows: 100,
            sorting_columns: None,
            file_offset: None,
            total_compressed_size: None,
            ordinal: None,
        };

        let block = build_row_group_block(&row_group, 0, &NoBloomFilterSource).unwrap();

        let chunk = block.column_chunk_raw(0);
        let stat_flags = StatFlags(chunk.stat_flags);
        assert!(stat_flags.has_min_stat());
        assert!(stat_flags.is_min_inlined());
        assert!(stat_flags.has_max_stat());
        assert!(stat_flags.is_max_inlined());

        let min_bytes = chunk.min_stat.to_le_bytes();
        let min_i32 = i32::from_le_bytes([min_bytes[0], min_bytes[1], min_bytes[2], min_bytes[3]]);
        assert_eq!(min_i32, min_days);
        let max_bytes = chunk.max_stat.to_le_bytes();
        let max_i32 = i32::from_le_bytes([max_bytes[0], max_bytes[1], max_bytes[2], max_bytes[3]]);
        assert_eq!(max_i32, max_days);

        assert_eq!(&min_bytes[4..], &[0u8; 4]);
        assert_eq!(&max_bytes[4..], &[0u8; 4]);
    }

    #[test]
    fn convert_with_qdb_meta_flags() {
        let parquet_data = write_test_parquet(10, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        let mut meta = QdbMeta::new(1);
        meta.schema.push(crate::parquet::qdb_metadata::QdbMetaCol {
            column_type: ColumnTypeTag::Symbol.into_type(),
            column_top: 42,
            format: Some(QdbMetaColFormat::LocalKeyIsGlobal),
            ascii: Some(true),
            id: None,
        });

        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, Some(&meta), 0, 0, &NoBloomFilterSource, None).unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        let desc = reader.column_descriptor(0).unwrap();
        let flags = desc.flags();
        assert!(flags.is_local_key_global());
        assert!(flags.is_ascii());
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
        let (parquet_meta_bytes, parquet_meta_file_size) = convert_from_parquet(
            &metadata,
            qdb_meta.as_ref(),
            0,
            0,
            &NoBloomFilterSource,
            None,
        )
        .unwrap();

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
        let (parquet_meta_bytes, parquet_meta_file_size) = convert_from_parquet(
            &metadata,
            qdb_meta.as_ref(),
            0,
            0,
            &NoBloomFilterSource,
            None,
        )
        .unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        assert_eq!(reader.row_group_count(), metadata.row_groups.len() as u32);

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

        let (parquet_meta_bytes, parquet_meta_file_size) = convert_from_parquet(
            &metadata,
            qdb_meta.as_ref(),
            0,
            0,
            &NoBloomFilterSource,
            None,
        )
        .unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        let rg = reader.row_group(0).unwrap();
        let chunk = rg.column_chunk(0).unwrap();
        let flags = chunk.stat_flags();
        assert!(flags.has_min_stat());
        assert!(flags.has_max_stat());
        assert!(flags.is_min_inlined());
        assert!(flags.is_max_inlined());
    }

    #[test]
    fn convert_distinct_count_present() {
        let parquet_data = write_multi_column_parquet(50);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);

        let (parquet_meta_bytes, parquet_meta_file_size) = convert_from_parquet(
            &metadata,
            qdb_meta.as_ref(),
            0,
            0,
            &NoBloomFilterSource,
            None,
        )
        .unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        let rg = reader.row_group(0).unwrap();

        let mut has_null_count = false;
        for i in 0..reader.column_count() as usize {
            let chunk = rg.column_chunk(i).unwrap();
            if chunk.stat_flags().has_null_count() {
                has_null_count = true;
            }
        }
        assert!(has_null_count);
    }

    /// Verifies that the FileMetaData path (`convert_from_parquet`) and the
    /// raw thrift path (`build_row_group_block`) produce identical row-group
    /// blocks for the same parquet input.
    #[test]
    fn thrift_round_trip_matches_convert_from_parquet() {
        let parquet_data = write_multi_column_parquet(200);
        let mut cursor = Cursor::new(&parquet_data);
        let file_size = parquet_data.len() as u64;
        let metadata = read_metadata_with_size(&mut cursor, file_size).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);

        let (parquet_meta_bytes_from_meta, parquet_meta_file_size) = convert_from_parquet(
            &metadata,
            qdb_meta.as_ref(),
            0,
            0,
            &NoBloomFilterSource,
            None,
        )
        .unwrap();
        let reader1 = ParquetMetaReader::from_file_size(
            &parquet_meta_bytes_from_meta,
            parquet_meta_file_size,
        )
        .unwrap();

        let parquet_data2 = write_multi_column_parquet(200);
        let mut cursor2 = Cursor::new(&parquet_data2);
        let metadata2 = read_metadata_with_size(&mut cursor2, parquet_data2.len() as u64).unwrap();
        let thrift_meta = metadata2.into_thrift();

        for (rg_idx, thrift_rg) in thrift_meta.row_groups.iter().enumerate() {
            let block = build_row_group_block(thrift_rg, rg_idx, &NoBloomFilterSource).unwrap();

            let rg1 = reader1.row_group(rg_idx).unwrap();
            assert_eq!(block.num_rows(), rg1.num_rows());

            for col_idx in 0..reader1.column_count() as usize {
                let chunk1 = rg1.column_chunk(col_idx).unwrap();
                let chunk2 = block.column_chunk_raw(col_idx);

                assert_eq!(chunk1.codec, chunk2.codec);
                assert_eq!(chunk1.encodings, chunk2.encodings);
                assert_eq!(chunk1.byte_range_start, chunk2.byte_range_start);
                assert_eq!(chunk1.total_compressed, chunk2.total_compressed);
                assert_eq!(chunk1.num_values, chunk2.num_values);
                assert_eq!(chunk1.null_count, chunk2.null_count);
                assert_eq!(chunk1.stat_flags, chunk2.stat_flags);
                assert_eq!(chunk1.min_stat, chunk2.min_stat);
                assert_eq!(chunk1.max_stat, chunk2.max_stat);
            }
        }
    }

    #[test]
    fn thrift_multi_column_with_stats() {
        let parquet_data = write_multi_column_parquet(50);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let thrift_meta = metadata.into_thrift();

        let block =
            build_row_group_block(&thrift_meta.row_groups[0], 0, &NoBloomFilterSource).unwrap();

        assert_eq!(block.num_rows(), 50);

        let ts_chunk = block.column_chunk_raw(0);
        let ts_flags = StatFlags(ts_chunk.stat_flags);
        assert!(ts_flags.has_min_stat());
        assert!(ts_flags.is_min_inlined());
        assert!(ts_flags.has_max_stat());
        assert!(ts_flags.is_max_inlined());
        assert!(ts_flags.has_null_count());
        assert_eq!(ts_chunk.null_count, 0);
        assert_eq!(ts_chunk.min_stat, 0);
        assert_eq!(ts_chunk.max_stat, 49);

        let int_chunk = block.column_chunk_raw(1);
        let int_flags = StatFlags(int_chunk.stat_flags);
        assert!(int_flags.has_min_stat());
        assert!(int_flags.is_min_inlined());
        assert_eq!(int_chunk.min_stat as i32, 0);
        assert_eq!(int_chunk.max_stat as i32, 49);

        let dbl_chunk = block.column_chunk_raw(2);
        let dbl_flags = StatFlags(dbl_chunk.stat_flags);
        assert!(dbl_flags.has_min_stat());
        assert!(dbl_flags.is_min_inlined());
        assert_eq!(f64::from_le_bytes(dbl_chunk.min_stat.to_le_bytes()), 0.0);
        assert_eq!(f64::from_le_bytes(dbl_chunk.max_stat.to_le_bytes()), 73.5);

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

        let block =
            build_row_group_block(&thrift_meta.row_groups[0], 0, &NoBloomFilterSource).unwrap();

        let chunk = block.column_chunk_raw(0);
        assert_eq!(chunk.codec().unwrap(), Codec::Snappy);
    }

    #[test]
    fn thrift_without_qdb_meta_infers_types() {
        let parquet_data = write_multi_column_parquet(30);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let thrift_meta = metadata.into_thrift();

        let block =
            build_row_group_block(&thrift_meta.row_groups[0], 0, &NoBloomFilterSource).unwrap();

        let ts_chunk = block.column_chunk_raw(0);
        let ts_flags = StatFlags(ts_chunk.stat_flags);
        assert!(ts_flags.has_min_stat());
        assert!(ts_flags.is_min_inlined());

        let int_chunk = block.column_chunk_raw(1);
        let int_flags = StatFlags(int_chunk.stat_flags);
        assert!(int_flags.has_min_stat());
        assert!(int_flags.is_min_inlined());

        let dbl_chunk = block.column_chunk_raw(2);
        let dbl_flags = StatFlags(dbl_chunk.stat_flags);
        assert!(dbl_flags.has_min_stat());
        assert!(dbl_flags.is_min_inlined());

        let flag_chunk = block.column_chunk_raw(3);
        let flag_flags = StatFlags(flag_chunk.stat_flags);
        assert!(flag_flags.has_min_stat());
        assert!(flag_flags.is_min_inlined());
    }

    #[test]
    fn thrift_multiple_row_groups() {
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
            .with_row_group_size(Some(30))
            .finish(partition)
            .unwrap();

        let mut cursor = Cursor::new(&buf);
        let metadata = read_metadata_with_size(&mut cursor, buf.len() as u64).unwrap();
        let thrift_meta = metadata.into_thrift();

        assert_eq!(thrift_meta.row_groups.len(), 4);
        let expected_rows = [30u64, 30, 30, 10];

        for (i, thrift_rg) in thrift_meta.row_groups.iter().enumerate() {
            let block = build_row_group_block(thrift_rg, i, &NoBloomFilterSource).unwrap();
            assert_eq!(block.num_rows(), expected_rows[i]);
        }
    }

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
                    col_type_code: cm.map(|c| c.column_type.code()).unwrap_or_else(|| {
                        crate::parquet_read::meta::infer_column_type(col_desc)
                            .map(|ct| ct.code())
                            .unwrap_or(-1)
                    }),
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
    fn generate_parquet_metadata_produces_valid_pm() {
        let parquet_data = write_multi_column_parquet(80);
        let file_size = parquet_data.len() as u64;
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, file_size).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);
        let thrift_meta = metadata.into_thrift();

        let mut cursor2 = Cursor::new(&parquet_data);
        let metadata2 = read_metadata_with_size(&mut cursor2, parquet_data.len() as u64).unwrap();
        let schema_columns = metadata2.schema_descr.columns();

        let col_infos = col_infos_from_schema(schema_columns, qdb_meta.as_ref());

        let parquet_footer_offset = 100u64;
        let parquet_footer_length = 50u32;

        let (parquet_meta_bytes, parquet_meta_file_size) = generate_parquet_metadata(
            &col_infos,
            &thrift_meta.row_groups,
            0,
            &[0],
            parquet_footer_offset,
            parquet_footer_length,
            &[],
            0,
            -1,
            SeqTxn::UNSET,
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
        let parquet_data = write_multi_column_parquet(80);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);
        let thrift_meta = metadata.into_thrift();

        let mut cursor2 = Cursor::new(&parquet_data);
        let metadata2 = read_metadata_with_size(&mut cursor2, parquet_data.len() as u64).unwrap();
        let schema_columns = metadata2.schema_descr.columns();

        let col_infos = col_infos_from_schema(schema_columns, qdb_meta.as_ref());

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
            SeqTxn::UNSET,
        )
        .unwrap();

        let initial_size = initial_pm.len() as u64;
        let initial_reader = ParquetMetaReader::from_file_size(&initial_pm, initial_size).unwrap();
        assert_eq!(initial_reader.row_group_count(), 1);

        let mut extended_rgs = thrift_meta.row_groups.clone();
        let mut new_rg = extended_rgs[0].clone();
        for col in &mut new_rg.columns {
            if let Some(ref mut meta) = col.meta_data {
                meta.data_page_offset += 10_000;
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
            SeqTxn::UNSET,
        )
        .unwrap();

        assert!(!result.bytes.is_empty());

        let mut full_file = initial_pm.clone();
        full_file.extend_from_slice(&result.bytes);
        assert_eq!(full_file.len() as u64, result.new_file_size);
        full_file[qdb_parquet_meta::types::HEADER_PARQUET_META_FILE_SIZE_OFF
            ..qdb_parquet_meta::types::HEADER_PARQUET_META_FILE_SIZE_OFF + 8]
            .copy_from_slice(&result.new_file_size.to_le_bytes());

        let new_reader =
            ParquetMetaReader::from_file_size(&full_file, result.new_file_size).unwrap();
        assert_eq!(new_reader.row_group_count(), 2);
        assert!(new_reader.verify_checksum().is_ok());

        let old_reader = ParquetMetaReader::from_file_size(&initial_pm, initial_size).unwrap();
        assert_eq!(old_reader.row_group_count(), 1);
    }

    #[test]
    fn update_with_decreasing_row_group_count_returns_error() {
        let parquet_data = write_multi_column_parquet(80);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);
        let thrift_meta = metadata.into_thrift();

        let mut cursor2 = Cursor::new(&parquet_data);
        let metadata2 = read_metadata_with_size(&mut cursor2, parquet_data.len() as u64).unwrap();
        let schema_columns = metadata2.schema_descr.columns();

        let col_infos = col_infos_from_schema(schema_columns, qdb_meta.as_ref());

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
            &three_rgs,
            0,
            &[0],
            100,
            50,
            &[],
            0,
            -1,
            SeqTxn::UNSET,
        )
        .unwrap();
        let initial_size = initial_pm.len() as u64;

        let initial_reader = ParquetMetaReader::from_file_size(&initial_pm, initial_size).unwrap();
        assert_eq!(initial_reader.row_group_count(), 3);

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
            SeqTxn::UNSET,
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
            SeqTxn::UNSET,
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
                SeqTxn::UNSET,
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

        let result = build_row_group_block(&rg, 0, &NoBloomFilterSource);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("no metadata"), "got: {err_msg}");
    }

    #[test]
    fn bloom_filter_extracted_from_parquet_into_pm() {
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

        let (schema, additional_meta) =
            crate::parquet_write::schema::to_parquet_schema(&partition, false, -1, SeqTxn::UNSET)
                .unwrap();
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
            SeqTxn::UNSET,
        )
        .unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        assert_eq!(reader.row_group_count(), 1);
        assert!(reader.has_bloom_filters());
        assert_eq!(reader.bloom_filter_position(0), Some(0));

        let bf_abs = reader.bloom_filter_offset_in_pm(0, 0).unwrap() as usize;
        assert_ne!(bf_abs, 0);
        assert_eq!(bf_abs % 8, 0);
        assert!(bf_abs + 4 <= parquet_meta_bytes.len());

        let bf_data = &parquet_meta_bytes[bf_abs..];
        let bf_len = i32::from_le_bytes(bf_data[..4].try_into().unwrap()) as usize;
        assert!(bf_len >= 32);
        assert!(bf_abs + 4 + bf_len <= parquet_meta_bytes.len());

        let inlined = &bf_data[4..4 + bf_len];
        let captured = bloom_bitsets[0][0].as_ref().unwrap();
        assert_eq!(
            inlined,
            captured.as_slice(),
            "inlined `_pm` bloom bitset must equal the bitset captured during parquet write"
        );
    }

    #[test]
    fn uuid_ool_stats_round_trip() {
        let row_count = 10usize;
        let uuid_data: Vec<[u8; 16]> = (0..row_count)
            .map(|i| {
                let mut buf = [0u8; 16];
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

        let mut cursor = Cursor::new(&buf);
        let metadata = read_metadata_with_size(&mut cursor, buf.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);
        let thrift_meta = metadata.into_thrift();

        let mut cursor2 = Cursor::new(&buf);
        let metadata2 = read_metadata_with_size(&mut cursor2, buf.len() as u64).unwrap();
        let schema_columns = metadata2.schema_descr.columns();

        let col_infos = col_infos_from_schema(schema_columns, qdb_meta.as_ref());

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
            SeqTxn::UNSET,
        )
        .unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_bytes.len() as u64)
                .unwrap();
        assert_eq!(reader.column_count(), 2);
        assert_eq!(reader.column_name(1).unwrap(), "val");

        let rg = reader.row_group(0).unwrap();
        let chunk = rg.column_chunk(1).unwrap();
        let stat_flags = StatFlags(chunk.stat_flags);

        let ool = rg.out_of_line_region();

        assert!(stat_flags.has_min_stat());
        assert!(!stat_flags.is_min_inlined());
        assert!(stat_flags.has_max_stat());
        assert!(!stat_flags.is_max_inlined());
        assert!(!ool.is_empty());

        let min_off = (chunk.min_stat >> 16) as usize;
        let min_len = (chunk.min_stat & 0xFFFF) as usize;
        let max_off = (chunk.max_stat >> 16) as usize;
        let max_len = (chunk.max_stat & 0xFFFF) as usize;
        assert_eq!(min_len, 16);
        assert_eq!(max_len, 16);
        let min_bytes = &ool[min_off..min_off + min_len];
        let max_bytes = &ool[max_off..max_off + max_len];
        assert_ne!(min_bytes, max_bytes);
    }

    /// Writes a parquet file with a single i64 "ts" column holding no stats
    /// and a hand-built QdbMeta that marks col 0 as the designated timestamp.
    /// The column has `designated_timestamp: false` so the writer's hard-coded
    /// "designated ts always gets stats" override at `parquet_write/file.rs`
    /// does not fire; `with_statistics(false)` then suppresses all stats.
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

        let (schema, _empty_meta) =
            to_parquet_schema(&partition, false, -1, SeqTxn::UNSET).unwrap();
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

        let backfill = |_rg: usize, _lo: usize, _hi: usize| -> ParquetMetaResult<i64> { Ok(42) };
        let (parquet_meta_bytes, parquet_meta_file_size) = convert_from_parquet(
            &metadata,
            Some(&qdb_meta),
            0,
            0,
            &NoBloomFilterSource,
            Some(&backfill),
        )
        .unwrap();
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

        let backfill = |_rg: usize, _lo: usize, _hi: usize| -> ParquetMetaResult<i64> {
            panic!("backfill must not be called when inline stats exist");
        };
        let (_parquet_meta_bytes, _parquet_meta_file_size) = convert_from_parquet(
            &metadata,
            qdb_meta.as_ref(),
            0,
            0,
            &NoBloomFilterSource,
            Some(&backfill),
        )
        .unwrap();
    }
}
