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

//! Footer reader and builder.

use crate::parquet::error::ParquetResult;
use crate::parquet_metadata::error::{parquet_meta_err, ParquetMetaErrorKind};
use crate::parquet_metadata::types::{
    BlockAlignedOffset, BLOCK_ALIGNMENT_SHIFT, FOOTER_CHECKSUM_SIZE, FOOTER_FIXED_SIZE,
    FOOTER_TRAILER_SIZE, ROW_GROUP_ENTRY_SIZE,
};

// ── On-disk footer fixed portion (16 bytes) ─────────────────────────────

/// Parsed fixed portion of the footer (16 bytes on disk, read field-by-field).
#[derive(Debug, Copy, Clone)]
pub struct FooterRaw {
    pub parquet_footer_offset: u64,
    pub parquet_footer_length: u32,
    pub row_group_count: u32,
}

// ── Footer (zero-copy reader) ──────────────────────────────────────────

/// Reader over the footer of a `_pm` file.
///
/// The footer starts at the offset stored in `_txn` and contains:
/// PARQUET_FOOTER_OFFSET(u64), PARQUET_FOOTER_LENGTH(u32),
/// ROW_GROUP_COUNT(u32), ROW_GROUP_ENTRIES(4B each), CHECKSUM(u32).
///
/// The footer is NOT guaranteed to be 8-byte aligned (it follows variable-length
/// out-of-line stat data), so the fixed portion is read via `read_unaligned`.
pub struct Footer<'a> {
    raw: FooterRaw,
    data: &'a [u8],
}

impl<'a> Footer<'a> {
    /// Creates a footer reader over the byte slice starting at the footer offset.
    pub fn new(data: &'a [u8]) -> ParquetResult<Self> {
        if data.len() < FOOTER_FIXED_SIZE + FOOTER_CHECKSUM_SIZE {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "footer too small"
            ));
        }
        // Read fields individually to avoid repr(C) padding issues.
        let raw = FooterRaw {
            parquet_footer_offset: u64::from_le_bytes(data[0..8].try_into().unwrap()),
            parquet_footer_length: u32::from_le_bytes(data[8..12].try_into().unwrap()),
            row_group_count: u32::from_le_bytes(data[12..16].try_into().unwrap()),
        };

        let required = Self::total_size(raw.row_group_count)?;
        if data.len() < required {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "footer too small for {} row groups: need {} bytes, have {}",
                raw.row_group_count,
                required,
                data.len()
            ));
        }
        Ok(Self { raw, data })
    }

    /// Total byte size of the footer including entries, checksum, and trailer.
    pub fn total_size(row_group_count: u32) -> ParquetResult<usize> {
        Self::size_through_crc(row_group_count)?
            .checked_add(FOOTER_TRAILER_SIZE)
            .ok_or_else(|| {
                parquet_meta_err!(ParquetMetaErrorKind::Truncated, "footer size overflow")
            })
    }

    /// Byte size of the footer from its start through the CRC (inclusive),
    /// excluding the trailer. This is the value stored in the trailer.
    pub fn size_through_crc(row_group_count: u32) -> ParquetResult<usize> {
        let rg_entries = (row_group_count as usize)
            .checked_mul(ROW_GROUP_ENTRY_SIZE)
            .ok_or_else(|| {
                parquet_meta_err!(ParquetMetaErrorKind::Truncated, "row_group_count overflow")
            })?;
        FOOTER_FIXED_SIZE
            .checked_add(rg_entries)
            .and_then(|s| s.checked_add(FOOTER_CHECKSUM_SIZE))
            .ok_or_else(|| {
                parquet_meta_err!(ParquetMetaErrorKind::Truncated, "footer size overflow")
            })
    }

    /// Byte offset in the parquet file where the parquet footer starts.
    pub fn parquet_footer_offset(&self) -> u64 {
        self.raw.parquet_footer_offset
    }

    /// Length of the parquet footer in bytes.
    pub fn parquet_footer_length(&self) -> u32 {
        self.raw.parquet_footer_length
    }

    pub fn row_group_count(&self) -> u32 {
        self.raw.row_group_count
    }

    /// Returns the actual byte offset of the row group block at `index`.
    /// The stored value is right-shifted by [`BLOCK_ALIGNMENT_SHIFT`].
    pub fn row_group_block_offset(&self, index: usize) -> ParquetResult<u64> {
        if index >= self.raw.row_group_count as usize {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "row group entry index {} out of range [0, {})",
                index,
                self.raw.row_group_count
            ));
        }
        let o = FOOTER_FIXED_SIZE + index * ROW_GROUP_ENTRY_SIZE;
        let entry_data = self.data.get(o..o + 4).ok_or_else(|| {
            parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "row group entry out of bounds"
            )
        })?;
        // Safety: .get(o..o+4) returned Some, so entry_data is exactly 4 bytes.
        let stored = u32::from_le_bytes(entry_data.try_into().expect("slice is 4 bytes"));
        Ok((stored as u64) << BLOCK_ALIGNMENT_SHIFT)
    }

    /// Returns the CRC32 checksum stored at the end of the footer.
    pub fn checksum(&self) -> ParquetResult<u32> {
        let o = FOOTER_FIXED_SIZE + (self.raw.row_group_count as usize) * ROW_GROUP_ENTRY_SIZE;
        let crc_data = self.data.get(o..o + 4).ok_or_else(|| {
            parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "checksum field out of bounds"
            )
        })?;
        // Safety: .get(o..o+4) returned Some, so crc_data is exactly 4 bytes.
        Ok(u32::from_le_bytes(
            crc_data.try_into().expect("slice is 4 bytes"),
        ))
    }
}

// ── FooterBuilder ──────────────────────────────────────────────────────

/// Builds a `_pm` footer into a `Vec<u8>`.
pub struct FooterBuilder {
    parquet_footer_offset: u64,
    parquet_footer_length: u32,
    row_group_offsets: Vec<u64>,
}

impl FooterBuilder {
    pub fn new(parquet_footer_offset: u64, parquet_footer_length: u32) -> Self {
        Self {
            parquet_footer_offset,
            parquet_footer_length,
            row_group_offsets: Vec::new(),
        }
    }

    /// Adds a row group block offset. The offset must be 8-byte aligned
    /// and representable as a block-aligned u32.
    pub fn add_row_group_offset(&mut self, offset: u64) -> ParquetResult<&mut Self> {
        // Validates alignment AND that the shifted value fits in u32.
        let _ = BlockAlignedOffset::from_byte_offset(offset)?;
        self.row_group_offsets.push(offset);
        Ok(self)
    }

    /// Writes the footer to `buf` (fixed fields + entries + CRC placeholder + trailer).
    /// The CRC placeholder is written as 0 and must be filled in by the caller.
    /// The trailer stores the footer length (from start through CRC, inclusive).
    /// Returns the byte offset within `buf` where the footer starts.
    pub fn write_to(&self, buf: &mut Vec<u8>) -> usize {
        let footer_start = buf.len();

        buf.extend_from_slice(&self.parquet_footer_offset.to_le_bytes());
        buf.extend_from_slice(&self.parquet_footer_length.to_le_bytes());
        buf.extend_from_slice(&(self.row_group_offsets.len() as u32).to_le_bytes());

        for &offset in &self.row_group_offsets {
            let stored = (offset >> BLOCK_ALIGNMENT_SHIFT) as u32;
            buf.extend_from_slice(&stored.to_le_bytes());
        }

        // CRC32 placeholder (filled by the top-level writer).
        buf.extend_from_slice(&0u32.to_le_bytes());

        // Footer length trailer: total bytes from footer start through CRC (inclusive).
        let footer_length_through_crc = (buf.len() - footer_start) as u32;
        buf.extend_from_slice(&footer_length_through_crc.to_le_bytes());

        footer_start
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_empty() {
        let fb = FooterBuilder::new(1024, 512);
        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        let footer = Footer::new(&buf[start..]).unwrap();
        assert_eq!(footer.parquet_footer_offset(), 1024);
        assert_eq!(footer.parquet_footer_length(), 512);
        assert_eq!(footer.row_group_count(), 0);
        assert_eq!(footer.checksum().unwrap(), 0); // placeholder
    }

    #[test]
    fn round_trip_with_entries() {
        let mut fb = FooterBuilder::new(0, 0);
        fb.add_row_group_offset(0).unwrap();
        fb.add_row_group_offset(64).unwrap();
        fb.add_row_group_offset(128).unwrap();

        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        let footer = Footer::new(&buf[start..]).unwrap();
        assert_eq!(footer.row_group_count(), 3);
        assert_eq!(footer.row_group_block_offset(0).unwrap(), 0);
        assert_eq!(footer.row_group_block_offset(1).unwrap(), 64);
        assert_eq!(footer.row_group_block_offset(2).unwrap(), 128);
    }

    #[test]
    fn reject_misaligned_offset() {
        let mut fb = FooterBuilder::new(0, 0);
        assert!(fb.add_row_group_offset(7).is_err());
        assert!(fb.add_row_group_offset(1).is_err());
    }

    #[test]
    fn reject_offset_exceeding_u32_range() {
        let mut fb = FooterBuilder::new(0, 0);
        // (u32::MAX as u64 + 1) << 3 exceeds the representable block-aligned range.
        let huge = ((u32::MAX as u64) + 1) << super::BLOCK_ALIGNMENT_SHIFT;
        assert!(fb.add_row_group_offset(huge).is_err());
    }

    #[test]
    fn footer_too_small() {
        assert!(Footer::new(&[0u8; 4]).is_err());
    }

    #[test]
    fn entry_out_of_range() {
        let fb = FooterBuilder::new(0, 0);
        let mut buf = Vec::new();
        fb.write_to(&mut buf);

        let footer = Footer::new(&buf).unwrap();
        assert!(footer.row_group_block_offset(0).is_err());
    }

    #[test]
    fn footer_truncated_for_entries() {
        // Create a footer with a valid header claiming 5 row groups but
        // only provide enough bytes for the fixed header + checksum (no entries).
        let mut buf = Vec::new();
        buf.extend_from_slice(&0u64.to_le_bytes()); // parquet_footer_offset
        buf.extend_from_slice(&0u32.to_le_bytes()); // parquet_footer_length
        buf.extend_from_slice(&5u32.to_le_bytes()); // row_group_count = 5
                                                    // Need 16 + 5*4 + 4 = 40 bytes, but only have 16.
        assert!(Footer::new(&buf).is_err());
    }

    #[test]
    fn many_row_groups() {
        let mut fb = FooterBuilder::new(100, 50);
        for i in 0..100u64 {
            fb.add_row_group_offset(i * 8).unwrap();
        }

        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        let footer = Footer::new(&buf[start..]).unwrap();
        assert_eq!(footer.row_group_count(), 100);
        for i in 0..100 {
            assert_eq!(footer.row_group_block_offset(i).unwrap(), (i as u64) * 8);
        }
    }
}
