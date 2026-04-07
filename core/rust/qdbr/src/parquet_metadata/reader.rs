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

//! Zero-copy reader for `_pm` metadata files.

use crate::parquet::error::ParquetResult;
use crate::parquet_metadata::error::{parquet_meta_err, ParquetMetaErrorKind};
use crate::parquet_metadata::footer::Footer;
use crate::parquet_metadata::header::{ColumnDescriptorRaw, FileHeader};
use crate::parquet_metadata::row_group::RowGroupBlockReader;
use crate::parquet_metadata::types::{FOOTER_CHECKSUM_SIZE, FOOTER_TRAILER_SIZE};

/// Main reader for a `_pm` metadata file.
///
/// Operates directly over a `&[u8]` slice (typically from an mmap).
/// Validates the format version and CRC32 checksum on construction.
pub struct ParquetMetaReader<'a> {
    data: &'a [u8],
    header: FileHeader<'a>,
    footer: Footer<'a>,
    footer_offset: u64,
}

impl<'a> ParquetMetaReader<'a> {
    /// Creates a reader by reading the footer length trailer at the end of the file.
    ///
    /// `file_size` is the total size of the `_pm` file. The last 4 bytes store
    /// the footer length (from footer start through CRC, inclusive). The footer
    /// offset is derived as `file_size - 4 - footer_length`.
    #[must_use = "returns the reader"]
    pub fn from_file_size(data: &'a [u8], file_size: u64) -> ParquetResult<Self> {
        let file_size_usize = usize::try_from(file_size).map_err(|_| {
            parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "file size {} exceeds addressable range",
                file_size
            )
        })?;
        if file_size_usize < FOOTER_TRAILER_SIZE {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "file too small for footer trailer: {} bytes",
                file_size
            ));
        }
        let trailer = data
            .get(file_size_usize - FOOTER_TRAILER_SIZE..file_size_usize)
            .ok_or_else(|| {
                parquet_meta_err!(
                    ParquetMetaErrorKind::Truncated,
                    "footer trailer out of bounds at file size {}",
                    file_size
                )
            })?;
        let footer_length =
            u32::from_le_bytes(trailer.try_into().expect("slice is 4 bytes")) as u64;
        let footer_offset = file_size
            .checked_sub(FOOTER_TRAILER_SIZE as u64)
            .and_then(|s| s.checked_sub(footer_length))
            .ok_or_else(|| {
                parquet_meta_err!(
                    ParquetMetaErrorKind::Truncated,
                    "footer length {} exceeds file size {}",
                    footer_length,
                    file_size
                )
            })?;
        Self::new(data, footer_offset)
    }

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
            parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "footer offset {} exceeds addressable range",
                footer_offset
            )
        })?;
        let footer_data = data.get(footer_usize..).ok_or_else(|| {
            parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "footer offset {} out of bounds",
                footer_offset
            )
        })?;

        // Read footer_length from the trailer at the end of the footer region.
        if footer_data.len() < FOOTER_TRAILER_SIZE {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "footer region too small for trailer"
            ));
        }
        let trailer_start = footer_data.len() - FOOTER_TRAILER_SIZE;
        let footer_length = u32::from_le_bytes(
            footer_data[trailer_start..trailer_start + FOOTER_TRAILER_SIZE]
                .try_into()
                .expect("slice is 4 bytes"),
        );

        let footer = Footer::new(footer_data, footer_length)?;

        Ok(Self { data, header, footer, footer_offset })
    }

    /// Verifies the CRC32 checksum stored in the footer against the file contents.
    /// Verifies the CRC32 checksum stored in the footer against the file contents.
    ///
    /// The CRC covers bytes `[0, crc_field_offset)` of the entire file.
    /// The CRC field offset is determined from `footer_length` via the trailer,
    /// which handles unknown footer feature sections.
    pub fn verify_checksum(&self) -> ParquetResult<()> {
        let footer_usize = self.footer_offset as usize;
        let crc_rel_offset = self.footer.crc_offset();
        let checksum_abs = footer_usize + crc_rel_offset;
        if checksum_abs + FOOTER_CHECKSUM_SIZE > self.data.len() {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "checksum field out of bounds"
            ));
        }
        let stored_crc = self.footer.checksum()?;
        let computed_crc = crc32fast::hash(&self.data[..checksum_abs]);
        if stored_crc != computed_crc {
            return Err(parquet_meta_err!(ParquetMetaErrorKind::ChecksumMismatch {
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

    /// Returns the column top for the column at `index`.
    /// Returns 0 when the FEATURE_COLUMN_TOPS flag is not set.
    pub fn column_top(&self, index: usize) -> ParquetResult<u64> {
        self.header.column_top(index)
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
            parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "row group block offset {} exceeds addressable range",
                block_offset_u64
            )
        })?;
        let block_data = self.data.get(block_offset..).ok_or_else(|| {
            parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
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

    pub fn unused_bytes(&self) -> u64 {
        self.footer.unused_bytes()
    }

    /// Returns the raw file data slice.
    pub fn data(&self) -> &'a [u8] {
        self.data
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
    use crate::parquet_metadata::writer::ParquetMetaWriter;

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

        let mut w = ParquetMetaWriter::new();
        w.designated_timestamp(5); // ts_col
        for (type_code, name) in types {
            w.add_column(name, -1, *type_code, ColumnFlags::new(), 0, 0, 0, 0);
        }
        let (bytes, footer_offset) = w.finish().unwrap();

        let reader = ParquetMetaReader::new(&bytes, footer_offset).unwrap();
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
        let mut w = ParquetMetaWriter::new();
        w.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let (mut bytes, footer_offset) = w.finish().unwrap();

        // Corrupt one byte in the header.
        bytes[0] ^= 0xFF;
        // Should fail on version check (new() no longer checks CRC).
        assert!(ParquetMetaReader::new(&bytes, footer_offset).is_err());
    }

    #[test]
    fn crc_corrupted_in_body() {
        let mut w = ParquetMetaWriter::new();
        w.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let mut rg = RowGroupBlockBuilder::new(1);
        rg.set_num_rows(42);
        w.add_row_group(rg);
        let (mut bytes, footer_offset) = w.finish().unwrap();

        // Corrupt a byte in the row group block area.
        let mid = footer_offset as usize / 2;
        if mid < bytes.len() {
            bytes[mid] ^= 0xFF;
        }
        let reader = ParquetMetaReader::new(&bytes, footer_offset).unwrap();
        assert!(reader.verify_checksum().is_err());
    }

    #[test]
    fn verify_checksum_passes_on_valid_file() {
        let mut w = ParquetMetaWriter::new();
        w.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let (bytes, footer_offset) = w.finish().unwrap();

        let reader = ParquetMetaReader::new(&bytes, footer_offset).unwrap();
        reader.verify_checksum().unwrap();
    }

    #[test]
    fn multiple_row_groups_with_stats() {
        let mut w = ParquetMetaWriter::new();
        w.add_column(
            "ts",
            0,
            8,
            ColumnFlags::new().with_repetition(FieldRepetition::Required),
            0,
            0,
            0,
            0,
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
        let reader = ParquetMetaReader::new(&bytes, footer_offset).unwrap();

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
        let mut w = ParquetMetaWriter::new();
        w.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let (bytes, footer_offset) = w.finish().unwrap();

        let reader = ParquetMetaReader::new(&bytes, footer_offset).unwrap();
        assert_eq!(reader.footer_offset(), footer_offset);
    }

    #[test]
    fn row_group_block_offset_out_of_bounds() {
        // Build a valid file, then manually corrupt a row group entry
        // to point past the end of the file.
        let mut w = ParquetMetaWriter::new();
        w.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let mut rg = RowGroupBlockBuilder::new(1);
        rg.set_num_rows(10);
        w.add_row_group(rg);
        let (mut bytes, footer_offset) = w.finish().unwrap();

        // The first row group entry is at footer_offset + FOOTER_FIXED_SIZE (after the fixed fields).
        let entry_offset =
            footer_offset as usize + crate::parquet_metadata::types::FOOTER_FIXED_SIZE;
        // Set the block offset to a huge value that, when shifted, exceeds the file.
        let bad_offset = 0xFFFF_FFFFu32;
        bytes[entry_offset..entry_offset + 4].copy_from_slice(&bad_offset.to_le_bytes());

        // Fix the CRC so the reader doesn't reject on checksum mismatch.
        // CRC is at len - TRAILER(4) - CRC(4).
        let crc_offset = bytes.len() - 8;
        let crc = crc32fast::hash(&bytes[..crc_offset]);
        bytes[crc_offset..crc_offset + 4].copy_from_slice(&crc.to_le_bytes());

        let reader = ParquetMetaReader::new(&bytes, footer_offset).unwrap();
        assert!(reader.row_group(0).is_err());
    }

    #[test]
    fn checksum_field_out_of_bounds() {
        // Construct a file where the footer claims many row groups but the data
        // is truncated so the checksum field falls outside the slice.
        // Simplest: just truncate a valid file at the footer.
        let mut w = ParquetMetaWriter::new();
        w.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let (bytes, footer_offset) = w.finish().unwrap();

        // Truncate the file to cut off the last byte (the CRC).
        let truncated = &bytes[..bytes.len() - 1];
        // This should fail because the footer's checksum field is out of bounds.
        assert!(ParquetMetaReader::new(truncated, footer_offset).is_err());
    }

    #[test]
    fn footer_offset_out_of_bounds() {
        let mut w = ParquetMetaWriter::new();
        w.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let (bytes, _) = w.finish().unwrap();

        // Use a footer offset past the end of the file.
        assert!(ParquetMetaReader::new(&bytes, bytes.len() as u64 + 100).is_err());
    }

    #[test]
    fn from_file_size_round_trip() {
        let mut w = ParquetMetaWriter::new();
        w.designated_timestamp(0);
        w.add_column("ts", 0, 8, ColumnFlags::new(), 0, 0, 0, 0);
        let mut rg = RowGroupBlockBuilder::new(1);
        rg.set_num_rows(42);
        w.add_row_group(rg);
        w.parquet_footer(4096, 256);
        let (bytes, footer_offset) = w.finish().unwrap();

        // from_file_size should derive the same footer offset from the trailer.
        let reader = ParquetMetaReader::from_file_size(&bytes, bytes.len() as u64).unwrap();
        assert_eq!(reader.footer_offset(), footer_offset);
        assert_eq!(reader.column_count(), 1);
        assert_eq!(reader.row_group_count(), 1);
        assert_eq!(reader.parquet_footer_offset(), 4096);
        assert_eq!(reader.parquet_footer_length(), 256);
        assert_eq!(reader.row_group(0).unwrap().num_rows(), 42);
    }

    #[test]
    fn from_file_size_too_small() {
        assert!(ParquetMetaReader::from_file_size(&[0u8; 3], 3).is_err());
    }

    #[test]
    fn from_file_size_bad_trailer() {
        // Trailer claims footer length larger than the file.
        let mut buf = vec![0u8; 20];
        buf[16..20].copy_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
        assert!(ParquetMetaReader::from_file_size(&buf, buf.len() as u64).is_err());
    }

    #[test]
    fn sorting_columns_round_trip() {
        let mut w = ParquetMetaWriter::new();
        w.designated_timestamp(0);
        w.add_column("ts", 0, 8, ColumnFlags::new(), 0, 0, 0, 0);
        w.add_column("key", 1, 12, ColumnFlags::new(), 0, 0, 0, 0);
        w.add_sorting_column(0);
        w.add_sorting_column(1);

        let (bytes, footer_offset) = w.finish().unwrap();
        let reader = ParquetMetaReader::new(&bytes, footer_offset).unwrap();

        assert_eq!(reader.sorting_column_count(), 2);
        assert_eq!(reader.sorting_column(0).unwrap(), 0);
        assert_eq!(reader.sorting_column(1).unwrap(), 1);
    }

    #[test]
    fn column_tops_through_reader() {
        let mut w = ParquetMetaWriter::new();
        w.add_column("a", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        w.add_column("b", 1, 6, ColumnFlags::new(), 0, 0, 0, 0);
        w.set_column_top(0, 256);

        let (bytes, footer_offset) = w.finish().unwrap();
        let reader = ParquetMetaReader::new(&bytes, footer_offset).unwrap();
        reader.verify_checksum().unwrap();

        assert_eq!(reader.column_top(0).unwrap(), 256);
        assert_eq!(reader.column_top(1).unwrap(), 0);
    }

    #[test]
    fn crc_covers_column_tops_section() {
        let mut w = ParquetMetaWriter::new();
        w.add_column("a", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        w.set_column_top(0, 42);

        let (mut bytes, footer_offset) = w.finish().unwrap();
        let reader = ParquetMetaReader::new(&bytes, footer_offset).unwrap();
        reader.verify_checksum().unwrap();

        // Corrupt a byte in the column tops section.
        let top_bytes = 42u64.to_le_bytes();
        let pos = bytes
            .windows(8)
            .position(|w| w == top_bytes)
            .expect("should find top value in file");
        bytes[pos] ^= 0xFF;

        let reader2 = ParquetMetaReader::new(&bytes, footer_offset).unwrap();
        assert!(reader2.verify_checksum().is_err());
    }
}
