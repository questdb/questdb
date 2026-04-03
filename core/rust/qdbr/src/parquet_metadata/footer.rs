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
    BlockAlignedOffset, FooterFeatureFlags, BLOCK_ALIGNMENT_SHIFT, FOOTER_CHECKSUM_SIZE,
    FOOTER_FIXED_SIZE, FOOTER_TRAILER_SIZE, ROW_GROUP_ENTRY_SIZE,
};

// ── On-disk footer fixed portion (24 bytes) ─────────────────────────────

/// On-disk layout of the fixed portion of the footer (24 bytes, read field-by-field).
#[derive(Debug, Copy, Clone)]
pub struct FooterRaw {
    pub parquet_footer_offset: u64,
    pub parquet_footer_length: u32,
    pub row_group_count: u32,
    pub footer_feature_flags: FooterFeatureFlags,
    pub unused_bytes: u64,
}

// ── Footer (zero-copy reader) ──────────────────────────────────────────

/// Reader over the footer of a `_pm` file.
///
/// The footer starts at the offset stored in `_txn` and contains:
/// PARQUET_FOOTER_OFFSET(u64), PARQUET_FOOTER_LENGTH(u32),
/// ROW_GROUP_COUNT(u32), FOOTER_FEATURE_FLAGS(u64),
/// ROW_GROUP_ENTRIES(4B each), [footer feature sections], CHECKSUM(u32).
///
/// CRC is located via `footer_length` from the trailer: `CRC offset =
/// footer_length - 4` relative to footer start. This handles unknown
/// footer feature sections between the entries and CRC.
pub struct Footer<'a> {
    raw: FooterRaw,
    data: &'a [u8],
    footer_length_through_crc: u32,
}

impl<'a> Footer<'a> {
    /// Creates a footer reader over the byte slice starting at the footer offset.
    ///
    /// `footer_length_through_crc` is the value from the trailer (bytes from
    /// footer start through CRC, inclusive).
    pub fn new(data: &'a [u8], footer_length_through_crc: u32) -> ParquetResult<Self> {
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
            footer_feature_flags: FooterFeatureFlags::from_le_bytes(
                data[16..24].try_into().unwrap(),
            ),
            unused_bytes: u64::from_le_bytes(data[24..32].try_into().unwrap()),
        };

        let unknown_required = raw.footer_feature_flags.unknown_required(0);
        if unknown_required != 0 {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::UnsupportedFeature { flags: unknown_required }
            ));
        }

        // Validate that the footer data is large enough for base entries + CRC.
        let min_size = Self::min_size(raw.row_group_count)?;
        if data.len() < min_size {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "footer too small for {} row groups: need {} bytes, have {}",
                raw.row_group_count,
                min_size,
                data.len()
            ));
        }

        // Validate footer_length_through_crc covers at least the base footer.
        let base_through_crc = Self::base_size_through_crc(raw.row_group_count)?;
        if (footer_length_through_crc as usize) < base_through_crc {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "footer_length {} too small for base footer size {}",
                footer_length_through_crc,
                base_through_crc
            ));
        }

        Ok(Self { raw, data, footer_length_through_crc })
    }

    /// Minimum byte size for the footer (fixed + base entries + CRC + trailer).
    /// Does not account for footer feature sections.
    pub fn min_size(row_group_count: u32) -> ParquetResult<usize> {
        Self::base_size_through_crc(row_group_count)?
            .checked_add(FOOTER_TRAILER_SIZE)
            .ok_or_else(|| {
                parquet_meta_err!(ParquetMetaErrorKind::Truncated, "footer size overflow")
            })
    }

    /// Byte size from footer start through CRC (inclusive), for the base
    /// footer without any footer feature sections.
    pub fn base_size_through_crc(row_group_count: u32) -> ParquetResult<usize> {
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

    /// Byte offset of the CRC32 field relative to footer start.
    /// Uses `footer_length_through_crc` from the trailer, which handles
    /// unknown footer feature sections between entries and CRC.
    pub fn crc_offset(&self) -> usize {
        self.footer_length_through_crc as usize - FOOTER_CHECKSUM_SIZE
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

    pub fn footer_feature_flags(&self) -> FooterFeatureFlags {
        self.raw.footer_feature_flags
    }

    pub fn unused_bytes(&self) -> u64 {
        self.raw.unused_bytes
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

    /// Returns the CRC32 checksum stored in the footer.
    pub fn checksum(&self) -> ParquetResult<u32> {
        let o = self.crc_offset();
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
    unused_bytes: u64,
    row_group_offsets: Vec<u64>,
}

impl FooterBuilder {
    pub fn new(parquet_footer_offset: u64, parquet_footer_length: u32) -> Self {
        Self {
            parquet_footer_offset,
            parquet_footer_length,
            unused_bytes: 0,
            row_group_offsets: Vec::new(),
        }
    }

    pub fn unused_bytes(&mut self, unused_bytes: u64) -> &mut Self {
        self.unused_bytes = unused_bytes;
        self
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
        buf.extend_from_slice(&FooterFeatureFlags::new().0.to_le_bytes());
        buf.extend_from_slice(&self.unused_bytes.to_le_bytes());

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

        let footer_length = u32::from_le_bytes(buf[buf.len() - 4..].try_into().unwrap());
        let footer = Footer::new(&buf[start..], footer_length).unwrap();
        assert_eq!(footer.parquet_footer_offset(), 1024);
        assert_eq!(footer.parquet_footer_length(), 512);
        assert_eq!(footer.row_group_count(), 0);
        assert_eq!(footer.footer_feature_flags(), FooterFeatureFlags::new());
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

        let footer_length = u32::from_le_bytes(buf[buf.len() - 4..].try_into().unwrap());
        let footer = Footer::new(&buf[start..], footer_length).unwrap();
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
        assert!(Footer::new(&[0u8; 4], 4).is_err());
    }

    #[test]
    fn entry_out_of_range() {
        let fb = FooterBuilder::new(0, 0);
        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        let footer_length = u32::from_le_bytes(buf[buf.len() - 4..].try_into().unwrap());
        let footer = Footer::new(&buf[start..], footer_length).unwrap();
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
        buf.extend_from_slice(&0u64.to_le_bytes()); // footer_feature_flags
                                                    // Need 24 + 5*4 + 4 = 48 bytes, but only have 24.
        assert!(Footer::new(&buf, 24).is_err());
    }

    #[test]
    fn many_row_groups() {
        let mut fb = FooterBuilder::new(100, 50);
        for i in 0..100u64 {
            fb.add_row_group_offset(i * 8).unwrap();
        }

        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        let footer_length = u32::from_le_bytes(buf[buf.len() - 4..].try_into().unwrap());
        let footer = Footer::new(&buf[start..], footer_length).unwrap();
        assert_eq!(footer.row_group_count(), 100);
        for i in 0..100 {
            assert_eq!(footer.row_group_block_offset(i).unwrap(), (i as u64) * 8);
        }
    }

    #[test]
    fn unknown_optional_footer_flags_ignored() {
        let fb = FooterBuilder::new(0, 0);
        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        // Set unknown optional flag bits in the footer.
        let flags_offset = start + 16;
        let flags = 0x0000_0000_FFFF_FFFEu64;
        buf[flags_offset..flags_offset + 8].copy_from_slice(&flags.to_le_bytes());

        let footer_length = u32::from_le_bytes(buf[buf.len() - 4..].try_into().unwrap());
        // Reader should accept (CRC located via footer_length, not computed).
        let footer = Footer::new(&buf[start..], footer_length).unwrap();
        assert_eq!(footer.row_group_count(), 0);
    }

    #[test]
    fn unknown_required_footer_flags_rejected() {
        let fb = FooterBuilder::new(0, 0);
        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        // Set an unknown required flag (bit 32).
        let flags_offset = start + 16;
        let flags = 1u64 << 32;
        buf[flags_offset..flags_offset + 8].copy_from_slice(&flags.to_le_bytes());

        let footer_length = u32::from_le_bytes(buf[buf.len() - 4..].try_into().unwrap());
        assert!(Footer::new(&buf[start..], footer_length).is_err());
    }
}
