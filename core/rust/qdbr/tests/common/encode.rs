use std::collections::HashSet;
use std::io::Cursor;
use std::ptr::null;

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use questdbr::parquet_write::{
    schema::{Column, Partition},
    ParquetWriter,
};

use crate::common::Encoding;

/// Packed encoding config values for Column::parquet_encoding_config.
/// Bit 0-7: encoding enum, bit 24: isExplicitlySet flag.
pub const PLAIN_CONFIG: i32 = 1 | (1 << 24);
pub const RLE_DICT_CONFIG: i32 = 2 | (1 << 24);
pub const DELTA_LENGTH_BYTE_ARRAY_CONFIG: i32 = 3 | (1 << 24);
pub const DELTA_BINARY_PACKED_CONFIG: i32 = 4 | (1 << 24);
pub const ALL_BLOOM_FILTER_STATES: [bool; 2] = [false, true];

impl Encoding {
    pub fn config(self) -> i32 {
        match self {
            Encoding::Plain => PLAIN_CONFIG,
            Encoding::RleDictionary => RLE_DICT_CONFIG,
            Encoding::DeltaBinaryPacked => DELTA_BINARY_PACKED_CONFIG,
            Encoding::DeltaLengthByteArray => DELTA_LENGTH_BYTE_ARRAY_CONFIG,
            _ => panic!("unsupported encoding for config: {self:?}"),
        }
    }
}

/// Null pattern for encode tests.
#[derive(Debug, Clone, Copy)]
pub enum NullPattern {
    None,
    Sparse,
    Dense,
}

pub const ALL_NULL_PATTERNS: [NullPattern; 3] =
    [NullPattern::None, NullPattern::Sparse, NullPattern::Dense];

/// Create a Column for a fixed-width primitive type.
///
/// SAFETY: The caller must ensure `data` outlives the returned Column.
/// The Column holds a `'static` reference to the data, so the data must
/// remain valid for the duration of the Column's use.
pub fn make_primitive_column(
    name: &'static str,
    column_type: i32,
    data: *const u8,
    data_size: usize,
    row_count: usize,
    encoding_config: i32,
) -> Column {
    Column::from_raw_data(
        0,
        name,
        column_type,
        0,
        row_count,
        data,
        data_size,
        null(),
        0,
        null(),
        0,
        false,
        false,
        encoding_config,
    )
    .expect("Column::from_raw_data")
}

/// Create a Column for a String or Binary type (with secondary_data = offsets).
///
/// SAFETY: The caller must ensure `primary_data` and `secondary_data` outlive the returned Column.
#[allow(clippy::too_many_arguments)]
pub fn make_string_column(
    name: &'static str,
    column_type: i32,
    primary_data: *const u8,
    primary_data_size: usize,
    secondary_data: *const u8,
    secondary_data_size: usize,
    row_count: usize,
    encoding_config: i32,
) -> Column {
    Column::from_raw_data(
        0,
        name,
        column_type,
        0,
        row_count,
        primary_data,
        primary_data_size,
        secondary_data,
        secondary_data_size,
        null(),
        0,
        false,
        false,
        encoding_config,
    )
    .expect("Column::from_raw_data")
}

/// Create a Column for a Varchar type (primary_data = overflow data, secondary_data = aux entries).
///
/// SAFETY: The caller must ensure `primary_data` and `secondary_data` outlive the returned Column.
#[allow(clippy::too_many_arguments)]
pub fn make_varchar_column(
    name: &'static str,
    column_type: i32,
    primary_data: *const u8,
    primary_data_size: usize,
    secondary_data: *const u8,
    secondary_data_size: usize,
    row_count: usize,
    encoding_config: i32,
) -> Column {
    Column::from_raw_data(
        0,
        name,
        column_type,
        0,
        row_count,
        primary_data,
        primary_data_size,
        secondary_data,
        secondary_data_size,
        null(),
        0,
        false,
        false,
        encoding_config,
    )
    .expect("Column::from_raw_data")
}

/// Create a Column for a Symbol type (primary_data = keys, secondary_data = dictionary,
/// symbol_offsets = offsets into dictionary).
///
/// SAFETY: The caller must ensure all data outlive the returned Column.
#[allow(clippy::too_many_arguments)]
pub fn make_symbol_column(
    name: &'static str,
    column_type: i32,
    primary_data: *const u8,
    primary_data_size: usize,
    secondary_data: *const u8,
    secondary_data_size: usize,
    symbol_offsets: *const u64,
    symbol_offsets_count: usize,
    row_count: usize,
    encoding_config: i32,
) -> Column {
    Column::from_raw_data(
        0,
        name,
        column_type,
        0,
        row_count,
        primary_data,
        primary_data_size,
        secondary_data,
        secondary_data_size,
        symbol_offsets,
        symbol_offsets_count,
        false,
        false,
        encoding_config,
    )
    .expect("Column::from_raw_data")
}

fn finish_parquet(partition: Partition, statistics: bool, bloom_filter_enabled: bool) -> Vec<u8> {
    let mut buf = Cursor::new(Vec::new());
    let mut writer = ParquetWriter::new(&mut buf).with_statistics(statistics);
    if bloom_filter_enabled {
        writer = writer
            .with_bloom_filter_columns(HashSet::from([0usize]))
            .with_bloom_filter_fpp(0.01);
    }
    writer.finish(partition).expect("ParquetWriter::finish");
    buf.into_inner()
}

/// Write a Partition through ParquetWriter and return the Parquet bytes.
pub fn write_parquet(partition: Partition) -> Vec<u8> {
    finish_parquet(partition, true, false)
}

/// Write a single-column Partition with bloom filters enabled and return the Parquet bytes.
pub fn write_parquet_with_bloom(partition: Partition) -> Vec<u8> {
    finish_parquet(partition, true, true)
}

/// Write a Partition through ParquetWriter without statistics and return the Parquet bytes.
pub fn write_parquet_no_stats(partition: Partition) -> Vec<u8> {
    finish_parquet(partition, false, false)
}

/// Write a single-column Partition without statistics and with bloom filters enabled.
pub fn write_parquet_no_stats_with_bloom(partition: Partition) -> Vec<u8> {
    finish_parquet(partition, false, true)
}

/// Read Parquet bytes back using Arrow's reader and return all RecordBatches.
pub fn read_parquet_batches(data: &[u8]) -> Vec<RecordBatch> {
    let bytes: Bytes = data.to_vec().into();
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .expect("ParquetRecordBatchReaderBuilder")
        .with_batch_size(1_000_000)
        .build()
        .expect("build reader");
    reader.flatten().collect()
}

/// Assert whether the first column in every row group has bloom filter metadata.
pub fn assert_single_column_bloom_metadata(data: &[u8], expected_present: bool) {
    let metadata =
        parquet2::read::read_metadata(&mut Cursor::new(data)).expect("read parquet metadata");
    for (row_group_idx, row_group) in metadata.row_groups.iter().enumerate() {
        let column = row_group.columns()[0].metadata();
        if expected_present {
            assert!(
                column.bloom_filter_offset.is_some(),
                "expected bloom filter offset in row group {row_group_idx}"
            );
            let length = column
                .bloom_filter_length
                .expect("expected bloom filter length when bloom filter is enabled");
            assert!(
                length > 0,
                "expected positive bloom filter length in row group {row_group_idx}"
            );
        } else {
            assert!(
                column.bloom_filter_offset.is_none(),
                "unexpected bloom filter offset in row group {row_group_idx}"
            );
            assert!(
                column.bloom_filter_length.is_none(),
                "unexpected bloom filter length in row group {row_group_idx}"
            );
        }
    }
}

/// Build QDB-format string data (primary + offsets) from a list of string values
/// and null indicators.
///
/// Returns (primary_data, offsets) where:
/// - primary_data: [i32 char_count | u16 chars...]* per row (char_count = -1 for null)
/// - offsets: i64 byte offset into primary_data per row
pub fn build_qdb_string_data(values: &[&str], nulls: &[bool]) -> (Vec<u8>, Vec<i64>) {
    assert_eq!(values.len(), nulls.len());
    let mut primary = Vec::new();
    let mut offsets = Vec::with_capacity(values.len());

    for (i, &value) in values.iter().enumerate() {
        offsets.push(primary.len() as i64);
        if nulls[i] {
            primary.extend_from_slice(&(-1i32).to_le_bytes());
        } else {
            let utf16: Vec<u16> = value.encode_utf16().collect();
            primary.extend_from_slice(&(utf16.len() as i32).to_le_bytes());
            for ch in &utf16 {
                primary.extend_from_slice(&ch.to_le_bytes());
            }
        }
    }
    (primary, offsets)
}

/// Build QDB-format binary data (primary + offsets) from a list of byte slices
/// and null indicators.
///
/// Returns (primary_data, offsets) where:
/// - primary_data: [i64 byte_count | u8 bytes...]* per row (byte_count = -1 for null)
/// - offsets: i64 byte offset into primary_data per row
pub fn build_qdb_binary_data(values: &[&[u8]], nulls: &[bool]) -> (Vec<u8>, Vec<i64>) {
    assert_eq!(values.len(), nulls.len());
    let mut primary = Vec::new();
    let mut offsets = Vec::with_capacity(values.len());

    for (i, &value) in values.iter().enumerate() {
        offsets.push(primary.len() as i64);
        if nulls[i] {
            primary.extend_from_slice(&(-1i64).to_le_bytes());
        } else {
            primary.extend_from_slice(&(value.len() as i64).to_le_bytes());
            primary.extend_from_slice(value);
        }
    }
    (primary, offsets)
}

/// Build QDB-format varchar aux data from a list of string values and null indicators.
///
/// Returns (primary_data, aux_data) where:
/// - primary_data: overflow data for strings > 9 bytes
/// - aux_data: [u8; 16]* per row (header + inline/offset data)
pub fn build_qdb_varchar_data(values: &[&str], nulls: &[bool]) -> (Vec<u8>, Vec<u8>) {
    assert_eq!(values.len(), nulls.len());
    let mut overflow_data = Vec::new();
    let mut aux = Vec::new();

    const FLAG_INLINED: u8 = 1;
    const FLAG_NULL: u8 = 4;
    const FLAGS_WIDTH: u32 = 4;

    for (i, &value) in values.iter().enumerate() {
        let mut entry = [0u8; 16];
        if nulls[i] {
            entry[0] = FLAG_NULL;
        } else {
            let bytes = value.as_bytes();
            let len = bytes.len();
            if len <= 9 {
                // Inlined
                entry[0] = ((len as u8) << FLAGS_WIDTH as u8) | FLAG_INLINED;
                entry[1..1 + len].copy_from_slice(bytes);
            } else {
                // Split/overflow (no INLINED flag)
                let header = (len as u32) << FLAGS_WIDTH;
                entry[0..4].copy_from_slice(&header.to_le_bytes());
                // Prefix (first 6 bytes)
                entry[4..10].copy_from_slice(&bytes[..6]);
                // Offset into overflow data
                let offset = overflow_data.len();
                let offset_lo = (offset & 0xFFFF) as u16;
                let offset_hi = (offset >> 16) as u32;
                entry[10..12].copy_from_slice(&offset_lo.to_le_bytes());
                entry[12..16].copy_from_slice(&offset_hi.to_le_bytes());
                overflow_data.extend_from_slice(bytes);
            }
        }
        aux.extend_from_slice(&entry);
    }
    (overflow_data, aux)
}

/// Serialize symbol labels into QDB format.
///
/// Returns (chars_data, offsets) where:
/// - chars_data: UTF-16 dictionary entries, each prefixed with u32 char count
/// - offsets: u64 byte offsets into chars_data for each symbol
pub fn serialize_as_symbols(labels: &[&str]) -> (Vec<u8>, Vec<u64>) {
    let mut chars = vec![];
    let mut offsets = vec![];

    for &s in labels {
        let sym_chars: Vec<u16> = s.encode_utf16().collect();
        offsets.push(chars.len() as u64);
        chars.extend_from_slice(&(sym_chars.len() as u32).to_le_bytes());
        for ch in &sym_chars {
            chars.extend_from_slice(&ch.to_le_bytes());
        }
    }

    (chars, offsets)
}

/// Generate null indicators for a given count and pattern.
pub fn generate_nulls(count: usize, pattern: NullPattern) -> Vec<bool> {
    match pattern {
        NullPattern::None => vec![false; count],
        NullPattern::Dense => (0..count).map(|i| i % 2 == 0).collect(),
        NullPattern::Sparse => (0..count).map(|i| i % 10 == 0).collect(),
    }
}
