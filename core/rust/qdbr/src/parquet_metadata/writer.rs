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

//! Writer for `.qdbp` metadata files (create and update modes).

use crate::parquet::error::ParquetResult;
use crate::parquet_metadata::error::{qdbp_err, QdbpErrorKind};
use crate::parquet_metadata::footer::{Footer, FooterBuilder};
use crate::parquet_metadata::header::FileHeaderBuilder;
use crate::parquet_metadata::row_group::RowGroupBlockBuilder;
use crate::parquet_metadata::types::{ColumnFlags, BLOCK_ALIGNMENT, FOOTER_CHECKSUM_SIZE};

// ── QdbpWriter (create mode) ───────────────────────────────────────────

/// Builds a complete `.qdbp` metadata file from scratch.
///
/// Usage:
/// ```ignore
/// let bytes = QdbpWriter::new()
///     .designated_timestamp(0)
///     .add_column(0, "ts", 0, 8, ColumnFlags::new())
///     .add_row_group(rg_builder)
///     .parquet_footer(offset, length)
///     .finish()?;
/// ```
pub struct QdbpWriter {
    header_builder: FileHeaderBuilder,
    row_groups: Vec<RowGroupBlockBuilder>,
    parquet_footer_offset: u64,
    parquet_footer_length: u32,
}

impl Default for QdbpWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl QdbpWriter {
    pub fn new() -> Self {
        Self {
            header_builder: FileHeaderBuilder::new(-1),
            row_groups: Vec::new(),
            parquet_footer_offset: 0,
            parquet_footer_length: 0,
        }
    }

    pub fn designated_timestamp(&mut self, index: i32) -> &mut Self {
        self.header_builder = FileHeaderBuilder::new(index);
        self
    }

    pub fn add_column(
        &mut self,
        top: u64,
        name: &str,
        id: i32,
        col_type: i32,
        flags: ColumnFlags,
    ) -> &mut Self {
        self.header_builder
            .add_column(top, name, id, col_type, flags);
        self
    }

    pub fn add_sorting_column(&mut self, index: u32) -> &mut Self {
        self.header_builder.add_sorting_column(index);
        self
    }

    pub fn add_row_group(&mut self, builder: RowGroupBlockBuilder) -> &mut Self {
        self.row_groups.push(builder);
        self
    }

    pub fn parquet_footer(&mut self, offset: u64, length: u32) -> &mut Self {
        self.parquet_footer_offset = offset;
        self.parquet_footer_length = length;
        self
    }

    /// Finishes writing and returns the complete `.qdbp` file bytes.
    ///
    /// Returns `(bytes, footer_offset)` where `footer_offset` is the byte
    /// offset of the footer within the file (to store in `_txn`).
    #[must_use = "returns the file bytes and footer offset"]
    pub fn finish(&self) -> ParquetResult<(Vec<u8>, u64)> {
        let mut buf = Vec::new();

        // Write header (includes descriptors, sorting columns, name strings).
        self.header_builder.write_to(&mut buf);

        // Write row group blocks (8-byte aligned).
        let mut block_offsets: Vec<u64> = Vec::with_capacity(self.row_groups.len());
        for rg in &self.row_groups {
            let offset = rg.write_to(&mut buf);
            block_offsets.push(offset as u64);
        }

        // Write footer.
        let mut fb = FooterBuilder::new(self.parquet_footer_offset, self.parquet_footer_length);
        for &offset in &block_offsets {
            fb.add_row_group_offset(offset)?;
        }
        let footer_offset = fb.write_to(&mut buf) as u64;

        // Compute and write CRC32 over [0, checksum_field_offset).
        let checksum_field_offset = buf.len() - FOOTER_CHECKSUM_SIZE;
        let crc = crc32fast::hash(&buf[..checksum_field_offset]);
        buf[checksum_field_offset..].copy_from_slice(&crc.to_le_bytes());

        Ok((buf, footer_offset))
    }
}

// ── QdbpUpdateWriter (update mode) ─────────────────────────────────────

/// Produces bytes to append to an existing `.qdbp` file for an incremental
/// update (new/changed row group blocks + new footer).
///
/// Unchanged row groups keep their original offsets in the new footer.
pub struct QdbpUpdateWriter<'a> {
    existing: &'a [u8],
    existing_footer_offset: u64,
    #[allow(dead_code)]
    column_count: u32,
    /// (original_offset | None for new/replaced, builder)
    entries: Vec<RowGroupEntry>,
    parquet_footer_offset: u64,
    parquet_footer_length: u32,
}

enum RowGroupEntry {
    /// Reuse an existing block at this offset.
    Existing(u64),
    /// Write a new block.
    New(RowGroupBlockBuilder),
}

impl<'a> QdbpUpdateWriter<'a> {
    /// Creates an update writer from the existing file data and its footer offset.
    pub fn new(existing: &'a [u8], existing_footer_offset: u64) -> ParquetResult<Self> {
        let footer_usize = usize::try_from(existing_footer_offset).map_err(|_| {
            qdbp_err!(
                QdbpErrorKind::Truncated,
                "footer offset {} exceeds addressable range",
                existing_footer_offset
            )
        })?;
        let footer_data = existing
            .get(footer_usize..)
            .ok_or_else(|| qdbp_err!(QdbpErrorKind::Truncated, "footer offset out of bounds"))?;
        let footer = Footer::new(footer_data)?;

        let header_data = existing;
        let hdr = crate::parquet_metadata::header::FileHeader::new(header_data)?;
        let column_count = hdr.column_count();

        // Initialize entries with existing row group offsets.
        let mut entries = Vec::with_capacity(footer.row_group_count() as usize);
        for i in 0..footer.row_group_count() as usize {
            entries.push(RowGroupEntry::Existing(footer.row_group_block_offset(i)?));
        }

        Ok(Self {
            existing,
            existing_footer_offset,
            column_count,
            entries,
            parquet_footer_offset: footer.parquet_footer_offset(),
            parquet_footer_length: footer.parquet_footer_length(),
        })
    }

    /// Replaces a row group at `index` with a new block.
    pub fn replace_row_group(
        &mut self,
        index: usize,
        builder: RowGroupBlockBuilder,
    ) -> ParquetResult<&mut Self> {
        let len = self.entries.len();
        let slot = self.entries.get_mut(index).ok_or_else(|| {
            qdbp_err!(
                QdbpErrorKind::InvalidValue,
                "row group index {} out of range [0, {})",
                index,
                len
            )
        })?;
        *slot = RowGroupEntry::New(builder);
        Ok(self)
    }

    /// Appends a new row group.
    pub fn add_row_group(&mut self, builder: RowGroupBlockBuilder) -> &mut Self {
        self.entries.push(RowGroupEntry::New(builder));
        self
    }

    pub fn parquet_footer(&mut self, offset: u64, length: u32) -> &mut Self {
        self.parquet_footer_offset = offset;
        self.parquet_footer_length = length;
        self
    }

    /// Finishes the update.
    ///
    /// Returns `(append_bytes, new_footer_offset)`:
    /// - `append_bytes`: bytes to append to the file after the old footer
    /// - `new_footer_offset`: the absolute offset of the new footer in the file
    ///
    /// The caller must also write the CRC32 at the end of the new footer.
    /// The CRC covers `[0, checksum_field_offset)` of the entire file
    /// (existing + appended).
    #[must_use = "returns the append bytes and new footer offset"]
    pub fn finish(&self) -> ParquetResult<(Vec<u8>, u64)> {
        // The new data starts right after the old footer.
        let footer_usize = self.existing_footer_offset as usize;
        let old_footer = Footer::new(&self.existing[footer_usize..])?;
        let old_footer_total = Footer::total_size(old_footer.row_group_count())?;
        let append_start = footer_usize + old_footer_total;

        let mut append_buf = Vec::new();

        // Write new/replaced row group blocks and collect final offsets.
        let mut final_offsets: Vec<u64> = Vec::with_capacity(self.entries.len());
        for entry in &self.entries {
            match entry {
                RowGroupEntry::Existing(offset) => {
                    final_offsets.push(*offset);
                }
                RowGroupEntry::New(builder) => {
                    // Pad relative to the absolute file position.
                    let abs_len = append_start + append_buf.len();
                    let padding = (BLOCK_ALIGNMENT - (abs_len % BLOCK_ALIGNMENT)) % BLOCK_ALIGNMENT;
                    append_buf.extend(std::iter::repeat_n(0u8, padding));

                    let abs_offset = append_start + append_buf.len();
                    // Write the block content directly (no extra alignment since
                    // we already padded).
                    append_buf.extend_from_slice(&builder.num_rows.to_le_bytes());
                    for chunk in &builder.chunks {
                        let bytes: &[u8; 64] = unsafe {
                            &*(chunk as *const super::column_chunk::ColumnChunkRaw
                                as *const [u8; 64])
                        };
                        append_buf.extend_from_slice(bytes);
                    }
                    append_buf.extend_from_slice(&builder.out_of_line);

                    final_offsets.push(abs_offset as u64);
                }
            }
        }

        // Write the new footer.
        let mut fb = FooterBuilder::new(self.parquet_footer_offset, self.parquet_footer_length);
        for &offset in &final_offsets {
            fb.add_row_group_offset(offset)?;
        }
        let footer_rel_start = fb.write_to(&mut append_buf);
        let new_footer_offset = (append_start + footer_rel_start) as u64;

        // Compute CRC32 over the entire file [0, checksum_field).
        let checksum_field_abs = append_start + append_buf.len() - FOOTER_CHECKSUM_SIZE;
        let mut hasher = crc32fast::Hasher::new();
        // Hash existing bytes up to append_start.
        hasher.update(&self.existing[..append_start]);
        // Hash new bytes up to (but not including) the checksum field.
        let new_bytes_before_crc = checksum_field_abs - append_start;
        hasher.update(&append_buf[..new_bytes_before_crc]);
        let crc = hasher.finalize();

        // Write CRC into the append buffer.
        let crc_offset_in_buf = append_buf.len() - FOOTER_CHECKSUM_SIZE;
        append_buf[crc_offset_in_buf..].copy_from_slice(&crc.to_le_bytes());

        Ok((append_buf, new_footer_offset))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet_metadata::column_chunk::ColumnChunkRaw;
    use crate::parquet_metadata::reader::QdbpReader;
    use crate::parquet_metadata::types::{Codec, FieldRepetition};

    fn make_simple_file() -> (Vec<u8>, u64) {
        let mut w = QdbpWriter::new();
        w.designated_timestamp(0);
        w.add_column(
            0,
            "ts",
            0,
            8,
            ColumnFlags::new().with_repetition(FieldRepetition::Required),
        );
        w.add_column(0, "val", 1, 10, ColumnFlags::new());
        w.add_sorting_column(0);

        let mut rg = RowGroupBlockBuilder::new(2);
        rg.set_num_rows(1000);
        let mut c0 = ColumnChunkRaw::zeroed();
        c0.codec = Codec::Snappy as u8;
        c0.num_values = 1000;
        rg.set_column_chunk(0, c0).unwrap();
        w.add_row_group(rg);

        w.parquet_footer(4096, 256);
        w.finish().unwrap()
    }

    #[test]
    fn create_and_read_back() {
        let (bytes, footer_offset) = make_simple_file();
        let reader = QdbpReader::new(&bytes, footer_offset).unwrap();

        assert_eq!(reader.column_count(), 2);
        assert_eq!(reader.row_group_count(), 1);
        assert_eq!(reader.designated_timestamp(), Some(0));
        assert_eq!(reader.parquet_footer_offset(), 4096);
        assert_eq!(reader.parquet_footer_length(), 256);

        let rg = reader.row_group(0).unwrap();
        assert_eq!(rg.num_rows(), 1000);
        let c = rg.column_chunk(0).unwrap();
        assert_eq!(c.codec().unwrap(), Codec::Snappy);
    }

    #[test]
    fn create_empty() {
        let mut w = QdbpWriter::new();
        w.add_column(0, "x", 0, 5, ColumnFlags::new());
        let (bytes, footer_offset) = w.finish().unwrap();

        let reader = QdbpReader::new(&bytes, footer_offset).unwrap();
        assert_eq!(reader.column_count(), 1);
        assert_eq!(reader.row_group_count(), 0);
    }

    #[test]
    fn update_append_row_group() {
        let (original, footer_offset) = make_simple_file();

        let mut updater = QdbpUpdateWriter::new(&original, footer_offset).unwrap();

        let mut rg = RowGroupBlockBuilder::new(2);
        rg.set_num_rows(500);
        let mut c = ColumnChunkRaw::zeroed();
        c.codec = Codec::Zstd as u8;
        c.num_values = 500;
        rg.set_column_chunk(0, c).unwrap();
        updater.add_row_group(rg);
        updater.parquet_footer(8192, 512);

        let (append_bytes, new_footer_offset) = updater.finish().unwrap();

        // Construct the full updated file.
        let old_footer = Footer::new(&original[footer_offset as usize..]).unwrap();
        let old_end =
            footer_offset as usize + Footer::total_size(old_footer.row_group_count()).unwrap();
        let mut full = original[..old_end].to_vec();
        full.extend_from_slice(&append_bytes);

        let reader = QdbpReader::new(&full, new_footer_offset).unwrap();
        assert_eq!(reader.row_group_count(), 2);
        assert_eq!(reader.parquet_footer_offset(), 8192);

        // Original row group still accessible.
        let rg0 = reader.row_group(0).unwrap();
        assert_eq!(rg0.num_rows(), 1000);

        // New row group.
        let rg1 = reader.row_group(1).unwrap();
        assert_eq!(rg1.num_rows(), 500);
        assert_eq!(rg1.column_chunk(0).unwrap().codec().unwrap(), Codec::Zstd);
    }

    #[test]
    fn update_replace_row_group() {
        // Build a file with 2 row groups.
        let mut w = QdbpWriter::new();
        w.add_column(0, "x", 0, 5, ColumnFlags::new());

        let mut rg0 = RowGroupBlockBuilder::new(1);
        rg0.set_num_rows(100);
        w.add_row_group(rg0);

        let mut rg1 = RowGroupBlockBuilder::new(1);
        rg1.set_num_rows(200);
        w.add_row_group(rg1);

        let (original, footer_offset) = w.finish().unwrap();

        // Replace row group 1.
        let mut updater = QdbpUpdateWriter::new(&original, footer_offset).unwrap();
        let mut new_rg1 = RowGroupBlockBuilder::new(1);
        new_rg1.set_num_rows(999);
        updater.replace_row_group(1, new_rg1).unwrap();

        let (append_bytes, new_footer_offset) = updater.finish().unwrap();

        let old_footer = Footer::new(&original[footer_offset as usize..]).unwrap();
        let old_end =
            footer_offset as usize + Footer::total_size(old_footer.row_group_count()).unwrap();
        let mut full = original[..old_end].to_vec();
        full.extend_from_slice(&append_bytes);

        let reader = QdbpReader::new(&full, new_footer_offset).unwrap();
        assert_eq!(reader.row_group_count(), 2);

        // Row group 0 unchanged.
        assert_eq!(reader.row_group(0).unwrap().num_rows(), 100);
        // Row group 1 replaced.
        assert_eq!(reader.row_group(1).unwrap().num_rows(), 999);
    }

    #[test]
    fn replace_row_group_out_of_range() {
        let (original, footer_offset) = make_simple_file();
        let mut updater = QdbpUpdateWriter::new(&original, footer_offset).unwrap();
        let rg = RowGroupBlockBuilder::new(2);
        // Only 1 row group exists (index 0), so index 5 is out of range.
        assert!(updater.replace_row_group(5, rg).is_err());
    }

    #[test]
    fn default_creates_same_as_new() {
        let mut w = QdbpWriter::default();
        w.add_column(0, "x", 0, 5, ColumnFlags::new());
        let (bytes, footer_offset) = w.finish().unwrap();
        let reader = QdbpReader::new(&bytes, footer_offset).unwrap();
        assert_eq!(reader.column_count(), 1);
        assert_eq!(reader.designated_timestamp(), None);
    }
}
