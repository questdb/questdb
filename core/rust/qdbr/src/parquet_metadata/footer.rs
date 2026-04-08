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
    BlockAlignedOffset, HeaderFeatureFlags, BLOCK_ALIGNMENT_SHIFT, FOOTER_CHECKSUM_SIZE,
    FOOTER_FIXED_SIZE, FOOTER_TRAILER_SIZE, ROW_GROUP_ENTRY_SIZE,
};

// ── On-disk footer fixed portion (24 bytes) ─────────────────────────────

/// On-disk layout of the fixed portion of the footer (24 bytes, read field-by-field).
#[derive(Debug, Copy, Clone)]
pub struct FooterRaw {
    pub parquet_footer_offset: u64,
    pub parquet_footer_length: u32,
    pub row_group_count: u32,
    pub unused_bytes: u64,
}

// ── Footer (zero-copy reader) ──────────────────────────────────────────

/// Reader over the footer of a `_pm` file.
///
/// The footer starts at the offset derived from the trailer and contains:
/// PARQUET_FOOTER_OFFSET(u64), PARQUET_FOOTER_LENGTH(u32),
/// ROW_GROUP_COUNT(u32), UNUSED_BYTES(u64), ROW_GROUP_ENTRIES(4B each),
/// [feature sections gated by header feature flags], CHECKSUM(u32).
///
/// CRC is located via `footer_length` from the trailer: `CRC offset =
/// footer_length - 4` relative to footer start. This handles unknown
/// feature sections between the entries and CRC.
pub struct Footer<'a> {
    raw: FooterRaw,
    data: &'a [u8],
    footer_length_through_crc: u32,
    column_tops: Option<&'a [u8]>,
}

impl<'a> Footer<'a> {
    /// Creates a footer reader over the byte slice starting at the footer offset.
    ///
    /// `footer_length_through_crc` is the value from the trailer (bytes from
    /// footer start through CRC, inclusive).
    pub fn new(
        data: &'a [u8],
        footer_length_through_crc: u32,
        feature_flags: HeaderFeatureFlags,
        column_count: u32,
    ) -> ParquetResult<Self> {
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
            unused_bytes: u64::from_le_bytes(data[16..24].try_into().unwrap()),
        };

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

        let footer_total = (footer_length_through_crc as usize)
            .checked_add(FOOTER_TRAILER_SIZE)
            .ok_or_else(|| {
                parquet_meta_err!(ParquetMetaErrorKind::Truncated, "footer size overflow")
            })?;
        if data.len() < footer_total {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "footer data too small for footer length {}",
                footer_length_through_crc
            ));
        }

        let mut column_tops = None;
        if feature_flags.has_column_tops() {
            let column_tops_size = (column_count as usize).checked_mul(8).ok_or_else(|| {
                parquet_meta_err!(ParquetMetaErrorKind::Truncated, "column_tops size overflow")
            })?;
            let column_tops_start = FOOTER_FIXED_SIZE
                .checked_add((raw.row_group_count as usize) * ROW_GROUP_ENTRY_SIZE)
                .ok_or_else(|| {
                    parquet_meta_err!(ParquetMetaErrorKind::Truncated, "footer size overflow")
                })?;
            let available = (footer_length_through_crc as usize)
                .checked_sub(base_through_crc)
                .ok_or_else(|| {
                    parquet_meta_err!(ParquetMetaErrorKind::Truncated, "footer size underflow")
                })?;
            if available != 0 {
                if available < column_tops_size {
                    return Err(parquet_meta_err!(
                        ParquetMetaErrorKind::Truncated,
                        "footer column_tops section truncated: need {} bytes, have {}",
                        column_tops_size,
                        available
                    ));
                }
                let column_tops_end =
                    column_tops_start
                        .checked_add(column_tops_size)
                        .ok_or_else(|| {
                            parquet_meta_err!(
                                ParquetMetaErrorKind::Truncated,
                                "footer column_tops section overflow"
                            )
                        })?;
                if column_tops_end > data.len() {
                    return Err(parquet_meta_err!(
                        ParquetMetaErrorKind::Truncated,
                        "footer column_tops section out of bounds"
                    ));
                }
                column_tops = Some(&data[column_tops_start..column_tops_end]);
            }
        }

        Ok(Self { raw, data, footer_length_through_crc, column_tops })
    }

    /// Minimum byte size for the footer (fixed + base entries + CRC + trailer).
    /// Does not account for feature sections.
    pub fn min_size(row_group_count: u32) -> ParquetResult<usize> {
        Self::base_size_through_crc(row_group_count)?
            .checked_add(FOOTER_TRAILER_SIZE)
            .ok_or_else(|| {
                parquet_meta_err!(ParquetMetaErrorKind::Truncated, "footer size overflow")
            })
    }

    /// Byte size from footer start through CRC (inclusive), for the base
    /// footer without any feature sections.
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
    /// unknown feature sections between entries and CRC.
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

    pub fn has_column_tops_override(&self) -> bool {
        self.column_tops.is_some()
    }

    pub fn column_top(&self, index: usize) -> ParquetResult<Option<u64>> {
        match self.column_tops {
            Some(tops_data) => {
                let column_count = tops_data.len() / 8;
                if index >= column_count {
                    return Err(parquet_meta_err!(
                        ParquetMetaErrorKind::InvalidValue,
                        "footer column top index {} out of range [0, {})",
                        index,
                        column_count
                    ));
                }
                let offset = index * 8;
                Ok(Some(u64::from_le_bytes(
                    tops_data[offset..offset + 8]
                        .try_into()
                        .expect("slice is 8 bytes"),
                )))
            }
            None => Ok(None),
        }
    }
}

// ── FooterBuilder ──────────────────────────────────────────────────────

/// Builds a `_pm` footer into a `Vec<u8>`.
pub struct FooterBuilder {
    parquet_footer_offset: u64,
    parquet_footer_length: u32,
    unused_bytes: u64,
    row_group_offsets: Vec<u64>,
    column_tops: Option<Vec<u64>>,
}

impl FooterBuilder {
    pub fn new(parquet_footer_offset: u64, parquet_footer_length: u32) -> Self {
        Self {
            parquet_footer_offset,
            parquet_footer_length,
            unused_bytes: 0,
            row_group_offsets: Vec::new(),
            column_tops: None,
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

    pub fn set_column_tops(&mut self, column_tops: &[u64]) -> &mut Self {
        self.column_tops = Some(column_tops.to_vec());
        self
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
        buf.extend_from_slice(&self.unused_bytes.to_le_bytes());

        for &offset in &self.row_group_offsets {
            let stored = (offset >> BLOCK_ALIGNMENT_SHIFT) as u32;
            buf.extend_from_slice(&stored.to_le_bytes());
        }

        if let Some(column_tops) = &self.column_tops {
            for &top in column_tops {
                buf.extend_from_slice(&top.to_le_bytes());
            }
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

    fn parse_footer(
        buf: &[u8],
        start: usize,
        feature_flags: HeaderFeatureFlags,
        column_count: u32,
    ) -> Footer<'_> {
        let footer_length = u32::from_le_bytes(buf[buf.len() - 4..].try_into().unwrap());
        Footer::new(&buf[start..], footer_length, feature_flags, column_count).unwrap()
    }

    #[test]
    fn round_trip_empty() {
        let fb = FooterBuilder::new(1024, 512);
        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        let footer = parse_footer(&buf, start, HeaderFeatureFlags::new(), 0);
        assert_eq!(footer.parquet_footer_offset(), 1024);
        assert_eq!(footer.parquet_footer_length(), 512);
        assert_eq!(footer.row_group_count(), 0);
        assert_eq!(footer.unused_bytes(), 0);
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

        let footer = parse_footer(&buf, start, HeaderFeatureFlags::new(), 0);
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
        assert!(Footer::new(&[0u8; 4], 4, HeaderFeatureFlags::new(), 0).is_err());
    }

    #[test]
    fn entry_out_of_range() {
        let fb = FooterBuilder::new(0, 0);
        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        let footer = parse_footer(&buf, start, HeaderFeatureFlags::new(), 0);
        assert!(footer.row_group_block_offset(0).is_err());
    }

    #[test]
    fn footer_truncated_for_entries() {
        // Create a footer with a valid fixed portion claiming 5 row groups but
        // only provide enough bytes for the fixed portion (no entries, no CRC).
        let mut buf = Vec::new();
        buf.extend_from_slice(&0u64.to_le_bytes()); // parquet_footer_offset
        buf.extend_from_slice(&0u32.to_le_bytes()); // parquet_footer_length
        buf.extend_from_slice(&5u32.to_le_bytes()); // row_group_count = 5
        buf.extend_from_slice(&0u64.to_le_bytes()); // unused_bytes
                                                    // Need 24 + 5*4 + 4 = 48 bytes, but only have 24.
        assert!(Footer::new(&buf, 24, HeaderFeatureFlags::new(), 0).is_err());
    }

    #[test]
    fn many_row_groups() {
        let mut fb = FooterBuilder::new(100, 50);
        for i in 0..100u64 {
            fb.add_row_group_offset(i * 8).unwrap();
        }

        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        let footer = parse_footer(&buf, start, HeaderFeatureFlags::new(), 0);
        assert_eq!(footer.row_group_count(), 100);
        for i in 0..100 {
            assert_eq!(footer.row_group_block_offset(i).unwrap(), (i as u64) * 8);
        }
    }

    #[test]
    fn unused_bytes_round_trip() {
        let mut fb = FooterBuilder::new(100, 50);
        fb.unused_bytes(8192);

        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        let footer = parse_footer(&buf, start, HeaderFeatureFlags::new(), 0);
        assert_eq!(footer.unused_bytes(), 8192);
    }

    #[test]
    fn footer_length_too_small_for_base() {
        // Build a valid empty footer, then call Footer::new with a
        // footer_length_through_crc that is smaller than base_size_through_crc.
        // base_through_crc for 0 row groups = FOOTER_FIXED_SIZE(24) + FOOTER_CHECKSUM_SIZE(4) = 28.
        let fb = FooterBuilder::new(0, 0);
        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        // Pass a footer_length_through_crc smaller than the required base size.
        let too_small = (FOOTER_FIXED_SIZE + FOOTER_CHECKSUM_SIZE - 1) as u32;
        assert!(Footer::new(&buf[start..], too_small, HeaderFeatureFlags::new(), 0).is_err());
    }

    #[test]
    fn footer_length_exactly_base_size() {
        // Boundary: footer_length_through_crc exactly equals base_through_crc must succeed.
        let fb = FooterBuilder::new(0, 0);
        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        let exact = (FOOTER_FIXED_SIZE + FOOTER_CHECKSUM_SIZE) as u32;
        Footer::new(&buf[start..], exact, HeaderFeatureFlags::new(), 0).unwrap();
    }

    #[test]
    fn round_trip_with_footer_column_tops_override() {
        let mut fb = FooterBuilder::new(0, 0);
        fb.add_row_group_offset(64).unwrap();
        fb.set_column_tops(&[42, 0, 7]);

        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        let footer = parse_footer(&buf, start, HeaderFeatureFlags::new().with_column_tops(), 3);
        assert!(footer.has_column_tops_override());
        assert_eq!(footer.column_top(0).unwrap(), Some(42));
        assert_eq!(footer.column_top(1).unwrap(), Some(0));
        assert_eq!(footer.column_top(2).unwrap(), Some(7));
    }

    #[test]
    fn footer_override_requires_enough_bytes() {
        let mut fb = FooterBuilder::new(0, 0);
        fb.add_row_group_offset(64).unwrap();
        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        let footer_length = u32::from_le_bytes(buf[buf.len() - 4..].try_into().unwrap());
        let too_long = footer_length + 8;
        assert!(Footer::new(
            &buf[start..],
            too_long,
            HeaderFeatureFlags::new().with_column_tops(),
            2
        )
        .is_err());
    }

    #[test]
    fn footer_override_ignores_unknown_trailing_sections() {
        let mut fb = FooterBuilder::new(0, 0);
        fb.add_row_group_offset(64).unwrap();
        fb.set_column_tops(&[11]);

        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);
        let old_footer_length =
            u32::from_le_bytes(buf[buf.len() - FOOTER_TRAILER_SIZE..].try_into().unwrap());
        let trailer_start = buf.len() - FOOTER_TRAILER_SIZE;
        let crc_start = trailer_start - FOOTER_CHECKSUM_SIZE;

        buf.splice(crc_start..crc_start, [0xAAu8; 16]);
        let footer_length = old_footer_length + 16;
        let trailer_offset = buf.len() - FOOTER_TRAILER_SIZE;
        buf[trailer_offset..].copy_from_slice(&footer_length.to_le_bytes());

        let footer = parse_footer(&buf, start, HeaderFeatureFlags::new().with_column_tops(), 1);
        assert_eq!(footer.column_top(0).unwrap(), Some(11));
    }
}
