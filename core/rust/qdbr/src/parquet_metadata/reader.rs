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

//! Zero-copy reader for `.qdbp` metadata files.

use crate::parquet::error::ParquetResult;
use crate::parquet_metadata::error::{qdbp_err, QdbpErrorKind};
use crate::parquet_metadata::footer::Footer;
use crate::parquet_metadata::header::{ColumnDescriptorRaw, FileHeader};
use crate::parquet_metadata::row_group::RowGroupBlockReader;
use crate::parquet_metadata::types::FOOTER_CHECKSUM_SIZE;

/// Main reader for a `.qdbp` metadata file.
///
/// Operates directly over a `&[u8]` slice (typically from an mmap).
/// Validates the format version and CRC32 checksum on construction.
pub struct QdbpReader<'a> {
    data: &'a [u8],
    header: FileHeader<'a>,
    footer: Footer<'a>,
    footer_offset: u64,
}

impl<'a> QdbpReader<'a> {
    /// Creates a reader over the given byte slice.
    ///
    /// `footer_offset` is the absolute byte offset of the footer within the file
    /// (obtained from `_txn`).
    ///
    /// Validates format version and footer bounds but does **not** verify the
    /// CRC32 checksum. Call [`verify_checksum`](Self::verify_checksum) separately
    /// when integrity verification is needed.
    #[must_use = "returns the reader"]
    pub fn new(data: &'a [u8], footer_offset: u64) -> ParquetResult<Self> {
        let header = FileHeader::new(data)?;

        let footer_usize = usize::try_from(footer_offset).map_err(|_| {
            qdbp_err!(
                QdbpErrorKind::Truncated,
                "footer offset {} exceeds addressable range",
                footer_offset
            )
        })?;
        let footer_data = data.get(footer_usize..).ok_or_else(|| {
            qdbp_err!(
                QdbpErrorKind::Truncated,
                "footer offset {} out of bounds",
                footer_offset
            )
        })?;
        let footer = Footer::new(footer_data)?;

        Ok(Self { data, header, footer, footer_offset })
    }

    /// Verifies the CRC32 checksum stored in the footer against the file contents.
    pub fn verify_checksum(&self) -> ParquetResult<()> {
        let footer_usize = self.footer_offset as usize;
        let footer_total = Footer::total_size(self.footer.row_group_count())?;
        let checksum_abs = footer_usize
            .checked_add(footer_total)
            .and_then(|s| s.checked_sub(FOOTER_CHECKSUM_SIZE))
            .ok_or_else(|| qdbp_err!(QdbpErrorKind::Truncated, "checksum offset overflow"))?;
        if checksum_abs > self.data.len() {
            return Err(qdbp_err!(
                QdbpErrorKind::Truncated,
                "checksum field out of bounds"
            ));
        }
        let stored_crc = self.footer.checksum()?;
        let computed_crc = crc32fast::hash(&self.data[..checksum_abs]);
        if stored_crc != computed_crc {
            return Err(qdbp_err!(QdbpErrorKind::ChecksumMismatch {
                stored: stored_crc,
                computed: computed_crc,
            }));
        }
        Ok(())
    }

    pub fn column_count(&self) -> u32 {
        self.header.column_count()
    }

    pub fn row_group_count(&self) -> u32 {
        self.footer.row_group_count()
    }

    /// Returns the designated timestamp column index, or `None` if not set.
    pub fn designated_timestamp(&self) -> Option<u32> {
        self.header.designated_timestamp()
    }

    /// Returns the column descriptor at `index`.
    pub fn column_descriptor(&self, index: usize) -> ParquetResult<&'a ColumnDescriptorRaw> {
        self.header.column_descriptor(index)
    }

    /// Returns the UTF-8 name of the column at `index`.
    pub fn column_name(&self, index: usize) -> ParquetResult<&'a str> {
        let desc = self.header.column_descriptor(index)?;
        self.header.column_name(desc)
    }

    /// Returns the sorting column index at position `i`.
    pub fn sorting_column(&self, i: usize) -> ParquetResult<u32> {
        self.header.sorting_column(i)
    }

    pub fn sorting_column_count(&self) -> u32 {
        self.header.sorting_column_count()
    }

    /// Returns a zero-copy reader for the row group at `index`.
    pub fn row_group(&self, index: usize) -> ParquetResult<RowGroupBlockReader<'a>> {
        let block_offset_u64 = self.footer.row_group_block_offset(index)?;
        let block_offset = usize::try_from(block_offset_u64).map_err(|_| {
            qdbp_err!(
                QdbpErrorKind::Truncated,
                "row group block offset {} exceeds addressable range",
                block_offset_u64
            )
        })?;
        let block_data = self.data.get(block_offset..).ok_or_else(|| {
            qdbp_err!(
                QdbpErrorKind::Truncated,
                "row group block offset {} out of bounds",
                block_offset
            )
        })?;
        RowGroupBlockReader::new(block_data, self.header.column_count())
    }

    pub fn parquet_footer_offset(&self) -> u64 {
        self.footer.parquet_footer_offset()
    }

    pub fn parquet_footer_length(&self) -> u32 {
        self.footer.parquet_footer_length()
    }

    /// Returns the footer offset within this file.
    pub fn footer_offset(&self) -> u64 {
        self.footer_offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet_metadata::column_chunk::ColumnChunkRaw;
    use crate::parquet_metadata::row_group::RowGroupBlockBuilder;
    use crate::parquet_metadata::types::{
        encode_stat_sizes, ColumnFlags, FieldRepetition, StatFlags,
    };
    use crate::parquet_metadata::writer::QdbpWriter;

    #[test]
    fn round_trip_all_column_types() {
        let types: &[(i32, &str)] = &[
            (1, "bool_col"),
            (2, "byte_col"),
            (3, "short_col"),
            (5, "int_col"),
            (6, "long_col"),
            (8, "ts_col"),
            (10, "double_col"),
            (12, "sym_col"),
            (19, "uuid_col"),
            (26, "varchar_col"),
        ];

        let mut w = QdbpWriter::new();
        w.designated_timestamp(5); // ts_col
        for (type_code, name) in types {
            w.add_column(0, name, -1, *type_code, ColumnFlags::new());
        }
        let (bytes, footer_offset) = w.finish().unwrap();

        let reader = QdbpReader::new(&bytes, footer_offset).unwrap();
        assert_eq!(reader.column_count(), types.len() as u32);
        assert_eq!(reader.designated_timestamp(), Some(5));

        for (i, (type_code, name)) in types.iter().enumerate() {
            assert_eq!(reader.column_name(i).unwrap(), *name);
            let desc = reader.column_descriptor(i).unwrap();
            assert_eq!(desc.col_type, *type_code);
        }
    }

    #[test]
    fn crc_corruption_detected() {
        let mut w = QdbpWriter::new();
        w.add_column(0, "x", 0, 5, ColumnFlags::new());
        let (mut bytes, footer_offset) = w.finish().unwrap();

        // Corrupt one byte in the header.
        bytes[0] ^= 0xFF;
        // Should fail on version check (new() no longer checks CRC).
        assert!(QdbpReader::new(&bytes, footer_offset).is_err());
    }

    #[test]
    fn crc_corrupted_in_body() {
        let mut w = QdbpWriter::new();
        w.add_column(0, "x", 0, 5, ColumnFlags::new());
        let mut rg = RowGroupBlockBuilder::new(1);
        rg.set_num_rows(42);
        w.add_row_group(rg);
        let (mut bytes, footer_offset) = w.finish().unwrap();

        // Corrupt a byte in the row group block area.
        let mid = footer_offset as usize / 2;
        if mid < bytes.len() {
            bytes[mid] ^= 0xFF;
        }
        let reader = QdbpReader::new(&bytes, footer_offset).unwrap();
        assert!(reader.verify_checksum().is_err());
    }

    #[test]
    fn verify_checksum_passes_on_valid_file() {
        let mut w = QdbpWriter::new();
        w.add_column(0, "x", 0, 5, ColumnFlags::new());
        let (bytes, footer_offset) = w.finish().unwrap();

        let reader = QdbpReader::new(&bytes, footer_offset).unwrap();
        reader.verify_checksum().unwrap();
    }

    #[test]
    fn multiple_row_groups_with_stats() {
        let mut w = QdbpWriter::new();
        w.add_column(
            0,
            "ts",
            0,
            8,
            ColumnFlags::new().with_repetition(FieldRepetition::Required),
        );
        w.add_sorting_column(0);

        for i in 0..5u64 {
            let mut rg = RowGroupBlockBuilder::new(1);
            rg.set_num_rows((i + 1) * 100);
            let mut c = ColumnChunkRaw::zeroed();
            c.stat_flags = StatFlags::new().with_min(true, true).with_max(true, true).0;
            c.stat_sizes = encode_stat_sizes(8, 8);
            c.min_stat = i * 1000;
            c.max_stat = (i + 1) * 1000 - 1;
            c.num_values = (i + 1) * 100;
            rg.set_column_chunk(0, c).unwrap();
            w.add_row_group(rg);
        }

        let (bytes, footer_offset) = w.finish().unwrap();
        let reader = QdbpReader::new(&bytes, footer_offset).unwrap();

        assert_eq!(reader.row_group_count(), 5);
        for i in 0..5 {
            let rg = reader.row_group(i).unwrap();
            assert_eq!(rg.num_rows(), ((i as u64) + 1) * 100);
            let c = rg.column_chunk(0).unwrap();
            assert_eq!(c.min_stat, (i as u64) * 1000);
        }
    }

    #[test]
    fn footer_offset_accessor() {
        let mut w = QdbpWriter::new();
        w.add_column(0, "x", 0, 5, ColumnFlags::new());
        let (bytes, footer_offset) = w.finish().unwrap();

        let reader = QdbpReader::new(&bytes, footer_offset).unwrap();
        assert_eq!(reader.footer_offset(), footer_offset);
    }

    #[test]
    fn row_group_block_offset_out_of_bounds() {
        // Build a valid file, then manually corrupt a row group entry
        // to point past the end of the file.
        let mut w = QdbpWriter::new();
        w.add_column(0, "x", 0, 5, ColumnFlags::new());
        let mut rg = RowGroupBlockBuilder::new(1);
        rg.set_num_rows(10);
        w.add_row_group(rg);
        let (mut bytes, footer_offset) = w.finish().unwrap();

        // The first row group entry is at footer_offset + 16 (after the fixed fields).
        let entry_offset = footer_offset as usize + 16;
        // Set the block offset to a huge value that, when shifted, exceeds the file.
        let bad_offset = 0xFFFF_FFFFu32;
        bytes[entry_offset..entry_offset + 4].copy_from_slice(&bad_offset.to_le_bytes());

        // Fix the CRC so the reader doesn't reject on checksum mismatch.
        let crc_offset = bytes.len() - 4;
        let crc = crc32fast::hash(&bytes[..crc_offset]);
        bytes[crc_offset..].copy_from_slice(&crc.to_le_bytes());

        let reader = QdbpReader::new(&bytes, footer_offset).unwrap();
        assert!(reader.row_group(0).is_err());
    }

    #[test]
    fn checksum_field_out_of_bounds() {
        // Construct a file where the footer claims many row groups but the data
        // is truncated so the checksum field falls outside the slice.
        // Simplest: just truncate a valid file at the footer.
        let mut w = QdbpWriter::new();
        w.add_column(0, "x", 0, 5, ColumnFlags::new());
        let (bytes, footer_offset) = w.finish().unwrap();

        // Truncate the file to cut off the last byte (the CRC).
        let truncated = &bytes[..bytes.len() - 1];
        // This should fail because the footer's checksum field is out of bounds.
        assert!(QdbpReader::new(truncated, footer_offset).is_err());
    }

    #[test]
    fn footer_offset_out_of_bounds() {
        let mut w = QdbpWriter::new();
        w.add_column(0, "x", 0, 5, ColumnFlags::new());
        let (bytes, _) = w.finish().unwrap();

        // Use a footer offset past the end of the file.
        assert!(QdbpReader::new(&bytes, bytes.len() as u64 + 100).is_err());
    }

    #[test]
    fn sorting_columns_round_trip() {
        let mut w = QdbpWriter::new();
        w.designated_timestamp(0);
        w.add_column(0, "ts", 0, 8, ColumnFlags::new());
        w.add_column(0, "key", 1, 12, ColumnFlags::new());
        w.add_sorting_column(0);
        w.add_sorting_column(1);

        let (bytes, footer_offset) = w.finish().unwrap();
        let reader = QdbpReader::new(&bytes, footer_offset).unwrap();

        assert_eq!(reader.sorting_column_count(), 2);
        assert_eq!(reader.sorting_column(0).unwrap(), 0);
        assert_eq!(reader.sorting_column(1).unwrap(), 1);
    }
}
