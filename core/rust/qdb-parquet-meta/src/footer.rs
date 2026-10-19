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

use crate::error::ParquetMetaErrorKind;
use crate::error::ParquetMetaResult;
use crate::parquet_meta_err;
use crate::types::{
    BlockAlignedOffset, FooterFeatureFlags, SeqTxn, BLOCK_ALIGNMENT_SHIFT, FOOTER_CHECKSUM_SIZE,
    FOOTER_FIXED_SIZE, FOOTER_TRAILER_SIZE, MAX_SCRATCHPAD_SIZE, ROW_GROUP_ENTRY_SIZE,
    SUPPORTED_FOOTER_SECTIONS,
};

/// Index of the `SEQ_TXN_BIT` section in the footer-flag offsets array.
pub const SEQ_TXN_SECTION_IDX: usize = 0;
const _: () = assert!(SEQ_TXN_SECTION_IDX < SUPPORTED_FOOTER_SECTIONS);

/// Index of the `SCRATCHPAD_BIT` section in the footer-flag offsets array.
pub const SCRATCHPAD_SECTION_IDX: usize = 1;
const _: () = assert!(SCRATCHPAD_SECTION_IDX < SUPPORTED_FOOTER_SECTIONS);

/// Walks set footer-flag bits forward from `sections_start` (= end of the
/// bloom filter section, or `entries_end` when bloom is absent), stamping
/// each known section's offset and advancing the cursor by its on-disk
/// size. `data` is the full footer byte slice (used for the variable-size
/// scratchpad branch); `end` bounds the walk (= CRC offset). Returns
/// `(offsets, sections_end)`. Add one branch per new known bit.
pub fn compute_section_offsets(
    feature_flags: FooterFeatureFlags,
    sections_start: usize,
    data: &[u8],
    end: usize,
) -> ParquetMetaResult<([u32; SUPPORTED_FOOTER_SECTIONS], usize)> {
    let mut offsets = [0u32; SUPPORTED_FOOTER_SECTIONS];
    let mut cursor = sections_start;
    if feature_flags.has_seq_txn() {
        offsets[SEQ_TXN_SECTION_IDX] = cursor as u32;
        cursor += 8;
        if cursor > end {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "seq_txn section exceeds CRC offset {}",
                end
            ));
        }
    }
    if feature_flags.has_scratchpad() {
        offsets[SCRATCHPAD_SECTION_IDX] = cursor as u32;
        cursor += parse_scratchpad_size(data, cursor, end)?;
    }
    Ok((offsets, cursor))
}

/// Parses the scratchpad payload (`[entry_count u32]` + per-entry
/// `[code u32, length u32, content]`) starting at `cursor` and returns its
/// total on-disk size in bytes. Bounds-checks every read against `end`.
/// `cursor` and `end` are bounded by the u32 footer length, so plain usize
/// arithmetic cannot overflow on a 64-bit target.
fn parse_scratchpad_size(data: &[u8], cursor: usize, end: usize) -> ParquetMetaResult<usize> {
    let count_end = cursor + 4;
    if count_end > end {
        return Err(parquet_meta_err!(
            ParquetMetaErrorKind::Truncated,
            "scratchpad entry_count exceeds CRC offset"
        ));
    }
    let entry_count = u32::from_le_bytes(data[cursor..count_end].try_into().unwrap()) as usize;
    if entry_count == 0 {
        return Err(parquet_meta_err!(
            ParquetMetaErrorKind::InvalidValue,
            "scratchpad bit set but entry_count is 0"
        ));
    }
    let mut p = count_end;
    for _ in 0..entry_count {
        let hdr_end = p + 8;
        if hdr_end > end {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "scratchpad entry header exceeds CRC offset"
            ));
        }
        let length = u32::from_le_bytes(data[p + 4..p + 8].try_into().unwrap()) as usize;
        let content_end = hdr_end + length;
        if content_end > end {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "scratchpad entry content exceeds CRC offset"
            ));
        }
        p = content_end;
    }
    let total = p - cursor;
    if total > MAX_SCRATCHPAD_SIZE {
        return Err(parquet_meta_err!(
            ParquetMetaErrorKind::InvalidValue,
            "scratchpad payload {} exceeds MAX_SCRATCHPAD_SIZE {}",
            total,
            MAX_SCRATCHPAD_SIZE
        ));
    }
    Ok(total)
}

/// Iterator over scratchpad entries: `(code, content)` pairs in
/// insertion order. The slice ranges have been validated by
/// [`compute_section_offsets`], so direct indexing is safe.
pub struct ScratchpadIter<'a> {
    data: &'a [u8],
    cursor: usize,
    remaining: u32,
}

impl<'a> Iterator for ScratchpadIter<'a> {
    type Item = (u32, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let code = u32::from_le_bytes(
            self.data[self.cursor..self.cursor + 4]
                .try_into()
                .expect("slice is 4 bytes"),
        );
        let length = u32::from_le_bytes(
            self.data[self.cursor + 4..self.cursor + 8]
                .try_into()
                .expect("slice is 4 bytes"),
        ) as usize;
        let content_start = self.cursor + 8;
        let content = &self.data[content_start..content_start + length];
        self.cursor = content_start + length;
        self.remaining -= 1;
        Some((code, content))
    }
}

// ── On-disk footer fixed portion (40 bytes) ─────────────────────────────

/// On-disk layout of the fixed portion of the footer (40 bytes, read field-by-field).
#[derive(Debug, Copy, Clone)]
pub struct FooterRaw {
    pub parquet_footer_offset: u64,
    pub parquet_footer_length: u32,
    pub row_group_count: u32,
    pub unused_bytes: u64,
    /// Committed `_pm` file size at the time of the previous snapshot, or 0
    /// for the first snapshot in the chain. The previous footer is located
    /// by reading the trailer at `prev_parquet_meta_file_size - 4`.
    pub prev_parquet_meta_file_size: u64,
    pub feature_flags: FooterFeatureFlags,
}

// ── Footer (zero-copy reader) ──────────────────────────────────────────

/// Reader over the footer of a `_pm` file.
///
/// The footer starts at the offset derived from the trailer and contains:
/// PARQUET_FOOTER_OFFSET(u64), PARQUET_FOOTER_LENGTH(u32),
/// ROW_GROUP_COUNT(u32), UNUSED_BYTES(u64),
/// PREV_PARQUET_META_FILE_SIZE(u64), FOOTER_FEATURE_FLAGS(u64),
/// ROW_GROUP_ENTRIES(4B each),
/// [feature sections gated by header or footer feature flags],
/// CHECKSUM(u32).
///
/// Feature sections sit between row group entries and CRC, in this order:
/// header-flag-gated sections first (currently just the bloom filter
/// section at `BLOOM_FILTERS_BIT`, parsed by the higher-level reader),
/// then footer-flag-gated sections in ascending bit order. Each
/// footer-flag section has a hardcoded byte size determined by its bit
/// position (see [`compute_section_offsets`]).
///
/// Header-first ordering is the forward-compat contract: a reader that
/// doesn't know any footer-flag bits still finds bloom at the unchanged
/// offset and treats trailing footer-flag bytes as opaque (still
/// CRC-covered).
///
/// `Footer::new` stamps the offset of recognized footer-flag sections
/// into `section_offsets`; payload bytes are parsed lazily by accessors
/// (`seq_txn()`, etc.).
///
/// CRC is located via `footer_length` from the trailer: `CRC offset =
/// footer_length - 4` relative to footer start.
pub struct Footer<'a> {
    raw: FooterRaw,
    data: &'a [u8],
    footer_length_through_crc: u32,
    /// Byte offset of each footer-flag-gated section's payload start.
    /// Entry `i` is meaningful only when bit `i` is set in
    /// `raw.feature_flags`; otherwise it is zero.
    section_offsets: [u32; SUPPORTED_FOOTER_SECTIONS],
    /// Byte offset where the bloom filter section ends (= where the
    /// footer-flag-gated sections begin).
    bloom_section_end: u32,
}

impl<'a> Footer<'a> {
    /// Creates a footer reader over the byte slice starting at the footer offset.
    ///
    /// `footer_length_through_crc` is the value from the trailer.
    /// `bloom_section_size` is the byte size of the bloom filter section
    /// (computed by the caller from header info; pass `0` when bloom is
    /// absent). Footer-flag sections sit immediately after the bloom
    /// section.
    pub fn new(
        data: &'a [u8],
        footer_length_through_crc: u32,
        bloom_section_size: usize,
    ) -> ParquetMetaResult<Self> {
        if data.len() < FOOTER_FIXED_SIZE + FOOTER_CHECKSUM_SIZE {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "footer too small"
            ));
        }
        // Read fields individually to avoid repr(C) padding issues.
        // Unwraps: data.len() >= FOOTER_FIXED_SIZE (40) + FOOTER_CHECKSUM_SIZE
        // checked above, so all fixed-width slices are exactly the right length.
        let raw = FooterRaw {
            parquet_footer_offset: u64::from_le_bytes(data[0..8].try_into().unwrap()),
            parquet_footer_length: u32::from_le_bytes(data[8..12].try_into().unwrap()),
            row_group_count: u32::from_le_bytes(data[12..16].try_into().unwrap()),
            unused_bytes: u64::from_le_bytes(data[16..24].try_into().unwrap()),
            prev_parquet_meta_file_size: u64::from_le_bytes(data[24..32].try_into().unwrap()),
            feature_flags: FooterFeatureFlags::from_le_bytes(data[32..40].try_into().unwrap()),
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

        let entries_end = FOOTER_FIXED_SIZE + (raw.row_group_count as usize) * ROW_GROUP_ENTRY_SIZE;
        let crc_offset = (footer_length_through_crc as usize) - FOOTER_CHECKSUM_SIZE;
        let bloom_section_end = entries_end.checked_add(bloom_section_size).ok_or_else(|| {
            parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "bloom section size {} overflows footer",
                bloom_section_size
            )
        })?;
        if bloom_section_end > crc_offset {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "bloom section ({} bytes) exceeds CRC offset",
                bloom_section_size
            ));
        }
        let (section_offsets, sections_end) =
            compute_section_offsets(raw.feature_flags, bloom_section_end, data, crc_offset)?;
        if sections_end > crc_offset {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "footer-flag sections end {} exceeds CRC offset {}",
                sections_end,
                crc_offset
            ));
        }

        Ok(Self {
            raw,
            data,
            footer_length_through_crc,
            section_offsets,
            bloom_section_end: bloom_section_end as u32,
        })
    }

    /// Minimum byte size for the footer (fixed + base entries + CRC + trailer).
    /// Does not account for feature sections.
    pub fn min_size(row_group_count: u32) -> ParquetMetaResult<usize> {
        Self::base_size_through_crc(row_group_count)?
            .checked_add(FOOTER_TRAILER_SIZE)
            .ok_or_else(|| {
                parquet_meta_err!(ParquetMetaErrorKind::Truncated, "footer size overflow")
            })
    }

    /// Byte size from footer start through CRC (inclusive), for the base
    /// footer without any feature sections.
    pub fn base_size_through_crc(row_group_count: u32) -> ParquetMetaResult<usize> {
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

    /// Returns the committed `_pm` file size at the time of the previous
    /// snapshot (0 if this is the first snapshot in the chain). Walk back
    /// by reading the trailer at `prev_parquet_meta_file_size - 4` to
    /// locate the previous footer.
    pub fn prev_parquet_meta_file_size(&self) -> u64 {
        self.raw.prev_parquet_meta_file_size
    }

    /// Returns the feature flags stored in this footer.
    pub fn feature_flags(&self) -> FooterFeatureFlags {
        self.raw.feature_flags
    }

    /// Per-footer `seqTxn`, or `None` when `SEQ_TXN_BIT` is unset.
    /// Consumed by the enterprise build; OSS does not read it.
    pub fn seq_txn(&self) -> Option<SeqTxn> {
        if !self.raw.feature_flags.has_seq_txn() {
            return None;
        }
        let off = self.section_offsets[SEQ_TXN_SECTION_IDX] as usize;
        // Unwrap: Footer::new validated `off + 8 <= crc_offset`.
        let raw = i64::from_le_bytes(self.data[off..off + 8].try_into().unwrap());
        Some(SeqTxn::new(raw))
    }

    /// Iterator over scratchpad entries in insertion order. Yields nothing
    /// when `SCRATCHPAD_BIT` is unset.
    pub fn scratchpad_entries(&self) -> ScratchpadIter<'a> {
        if !self.raw.feature_flags.has_scratchpad() {
            return ScratchpadIter {
                data: self.data,
                cursor: 0,
                remaining: 0,
            };
        }
        let off = self.section_offsets[SCRATCHPAD_SECTION_IDX] as usize;
        // Unwrap: Footer::new validated the scratchpad payload, including
        // entry_count.
        let entry_count = u32::from_le_bytes(self.data[off..off + 4].try_into().unwrap());
        ScratchpadIter {
            data: self.data,
            cursor: off + 4,
            remaining: entry_count,
        }
    }

    /// First scratchpad entry matching `code`, or `None`. O(n) — callers
    /// typically read a handful of codes per partition open.
    pub fn scratchpad_entry(&self, code: u32) -> Option<&'a [u8]> {
        self.scratchpad_entries()
            .find(|(c, _)| *c == code)
            .map(|(_, content)| content)
    }

    /// Byte offset (relative to footer start) where the bloom filter
    /// section ends. Higher-level readers use this as the end bound when
    /// slicing the bloom section from `entries_end..bloom_section_end()`.
    pub fn bloom_section_end(&self) -> usize {
        self.bloom_section_end as usize
    }

    /// Returns the actual byte offset of the row group block at `index`.
    /// The stored value is right-shifted by [`BLOCK_ALIGNMENT_SHIFT`].
    pub fn row_group_block_offset(&self, index: usize) -> ParquetMetaResult<u64> {
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
    pub fn checksum(&self) -> ParquetMetaResult<u32> {
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
    prev_parquet_meta_file_size: u64,
    feature_flags: FooterFeatureFlags,
    row_group_offsets: Vec<u64>,
    /// Optional bloom filter footer feature section bytes, written before
    /// footer-flag-gated sections (seq_txn, scratchpad).
    bloom_filter_section: Vec<u8>,
    seq_txn: Option<SeqTxn>,
    scratchpad: Vec<(u32, Vec<u8>)>,
}

impl FooterBuilder {
    pub fn new(parquet_footer_offset: u64, parquet_footer_length: u32) -> Self {
        Self {
            parquet_footer_offset,
            parquet_footer_length,
            unused_bytes: 0,
            prev_parquet_meta_file_size: 0,
            feature_flags: FooterFeatureFlags::new(),
            row_group_offsets: Vec::new(),
            bloom_filter_section: Vec::new(),
            seq_txn: None,
            scratchpad: Vec::new(),
        }
    }

    pub fn unused_bytes(&mut self, unused_bytes: u64) -> &mut Self {
        self.unused_bytes = unused_bytes;
        self
    }

    /// Sets the committed `_pm` file size at the time of the previous
    /// snapshot. Zero means "no previous snapshot" (first commit).
    pub fn prev_parquet_meta_file_size(&mut self, prev_size: u64) -> &mut Self {
        self.prev_parquet_meta_file_size = prev_size;
        self
    }

    /// Sets the per-footer feature flags written alongside the fixed fields.
    pub fn feature_flags(&mut self, feature_flags: FooterFeatureFlags) -> &mut Self {
        self.feature_flags = feature_flags;
        self
    }

    /// Sets the bloom filter footer feature section bytes. Written between
    /// the seq_txn payload and CRC.
    pub fn set_bloom_filter_section(&mut self, section: Vec<u8>) -> &mut Self {
        self.bloom_filter_section = section;
        self
    }

    /// Sets the per-footer `seqTxn`. `SeqTxn::UNSET` omits the section.
    pub fn set_seq_txn(&mut self, value: SeqTxn) -> &mut Self {
        self.seq_txn = if value.is_set() { Some(value) } else { None };
        self
    }

    /// Replaces the scratchpad entries. An empty `Vec` clears the section.
    pub fn set_scratchpad_entries(&mut self, entries: Vec<(u32, Vec<u8>)>) -> &mut Self {
        self.scratchpad = entries;
        self
    }

    /// Adds a row group block offset. The offset must be 8-byte aligned
    /// and representable as a block-aligned u32.
    pub fn add_row_group_offset(&mut self, offset: u64) -> ParquetMetaResult<&mut Self> {
        // Validates alignment AND that the shifted value fits in u32.
        let _ = BlockAlignedOffset::from_byte_offset(offset)?;
        self.row_group_offsets.push(offset);
        Ok(self)
    }

    /// Rejects a scratchpad the reader would refuse: a payload larger than
    /// `MAX_SCRATCHPAD_SIZE` (`parse_scratchpad_size` hard-errors on it) or an entry
    /// whose length overflows the `u32` length prefix. The sibling `debug_assert!` in
    /// `write_to` guards the same invariant only in debug builds, so release-mode
    /// writers call this at the fallible `finish` boundary to stay symmetric with the
    /// reader instead of emitting a footer that this crate then cannot re-open.
    pub fn validate_scratchpad(&self) -> ParquetMetaResult<()> {
        if self.scratchpad.is_empty() {
            return Ok(());
        }
        let mut payload_size: usize = 4;
        for (_, content) in &self.scratchpad {
            if content.len() > u32::MAX as usize {
                return Err(parquet_meta_err!(
                    ParquetMetaErrorKind::InvalidValue,
                    "scratchpad entry length {} exceeds u32::MAX",
                    content.len()
                ));
            }
            payload_size = payload_size.saturating_add(8 + content.len());
        }
        if payload_size > crate::types::MAX_SCRATCHPAD_SIZE {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "scratchpad payload {} exceeds MAX_SCRATCHPAD_SIZE {}",
                payload_size,
                crate::types::MAX_SCRATCHPAD_SIZE
            ));
        }
        Ok(())
    }

    /// Writes the footer to `buf` (fixed fields + entries + CRC placeholder + trailer).
    /// The CRC placeholder is written as 0 and must be filled in by the caller.
    /// The trailer stores the footer length (from start through CRC, inclusive).
    /// Returns the byte offset within `buf` where the footer starts.
    pub fn write_to(&self, buf: &mut Vec<u8>) -> usize {
        let footer_start = buf.len();

        // Force feature bits to match section presence so a caller-set
        // feature_flags() can't desync from the actual payload.
        let known_section_bits =
            FooterFeatureFlags::SEQ_TXN_BIT | FooterFeatureFlags::SCRATCHPAD_BIT;
        let mut effective_flags = FooterFeatureFlags(self.feature_flags.0 & !known_section_bits);
        if self.seq_txn.is_some() {
            effective_flags = effective_flags.with_seq_txn();
        }
        if !self.scratchpad.is_empty() {
            effective_flags = effective_flags.with_scratchpad();
        }

        buf.extend_from_slice(&self.parquet_footer_offset.to_le_bytes());
        buf.extend_from_slice(&self.parquet_footer_length.to_le_bytes());
        buf.extend_from_slice(&(self.row_group_offsets.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.unused_bytes.to_le_bytes());
        buf.extend_from_slice(&self.prev_parquet_meta_file_size.to_le_bytes());
        buf.extend_from_slice(&effective_flags.to_le_bytes());

        for &offset in &self.row_group_offsets {
            let stored = (offset >> BLOCK_ALIGNMENT_SHIFT) as u32;
            buf.extend_from_slice(&stored.to_le_bytes());
        }

        // Header-flag sections first (bloom), then footer-flag sections
        // in ascending bit order: seq_txn (bit 0), scratchpad (bit 1).
        if !self.bloom_filter_section.is_empty() {
            buf.extend_from_slice(&self.bloom_filter_section);
        }
        if let Some(value) = self.seq_txn {
            buf.extend_from_slice(&value.get().to_le_bytes());
        }
        if !self.scratchpad.is_empty() {
            let payload_size = 4 + self
                .scratchpad
                .iter()
                .map(|(_, c)| 8 + c.len())
                .sum::<usize>();
            debug_assert!(
                payload_size <= crate::types::MAX_SCRATCHPAD_SIZE,
                "scratchpad payload {} exceeds MAX_SCRATCHPAD_SIZE {}",
                payload_size,
                crate::types::MAX_SCRATCHPAD_SIZE
            );
            buf.extend_from_slice(&(self.scratchpad.len() as u32).to_le_bytes());
            for (code, content) in &self.scratchpad {
                buf.extend_from_slice(&code.to_le_bytes());
                buf.extend_from_slice(&(content.len() as u32).to_le_bytes());
                buf.extend_from_slice(content);
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

    fn parse_footer(buf: &[u8], start: usize) -> Footer<'_> {
        parse_footer_with_bloom(buf, start, 0)
    }

    fn parse_footer_with_bloom(buf: &[u8], start: usize, bloom_section_size: usize) -> Footer<'_> {
        let footer_length = u32::from_le_bytes(buf[buf.len() - 4..].try_into().unwrap());
        Footer::new(&buf[start..], footer_length, bloom_section_size).unwrap()
    }

    #[test]
    fn round_trip_empty() {
        let fb = FooterBuilder::new(1024, 512);
        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        let footer = parse_footer(&buf, start);
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

        let footer = parse_footer(&buf, start);
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
        assert!(Footer::new(&[0u8; 4], 4, 0).is_err());
    }

    #[test]
    fn entry_out_of_range() {
        let fb = FooterBuilder::new(0, 0);
        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        let footer = parse_footer(&buf, start);
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
        buf.extend_from_slice(&0u64.to_le_bytes()); // prev_parquet_meta_file_size
        buf.extend_from_slice(&0u64.to_le_bytes()); // footer_feature_flags
                                                    // Need 40 + 5*4 + 4 = 64 bytes, but only have 40.
        assert!(Footer::new(&buf, 40, 0).is_err());
    }

    #[test]
    fn many_row_groups() {
        let mut fb = FooterBuilder::new(100, 50);
        for i in 0..100u64 {
            fb.add_row_group_offset(i * 8).unwrap();
        }

        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        let footer = parse_footer(&buf, start);
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

        let footer = parse_footer(&buf, start);
        assert_eq!(footer.unused_bytes(), 8192);
    }

    #[test]
    fn footer_length_too_small_for_base() {
        // Build a valid empty footer, then call Footer::new with a
        // footer_length_through_crc that is smaller than base_size_through_crc.
        // base_through_crc for 0 row groups = FOOTER_FIXED_SIZE(40) + FOOTER_CHECKSUM_SIZE(4) = 44.
        let fb = FooterBuilder::new(0, 0);
        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        // Pass a footer_length_through_crc smaller than the required base size.
        let too_small = (FOOTER_FIXED_SIZE + FOOTER_CHECKSUM_SIZE - 1) as u32;
        assert!(Footer::new(&buf[start..], too_small, 0).is_err());
    }

    #[test]
    fn feature_flags_default_zero() {
        use crate::types::FooterFeatureFlags;

        let fb = FooterBuilder::new(0, 0);
        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        let footer = parse_footer(&buf, start);
        assert_eq!(footer.feature_flags(), FooterFeatureFlags::new());
    }

    #[test]
    fn caller_flags_cannot_claim_absent_known_sections() {
        use crate::types::FooterFeatureFlags;

        let unknown_optional_bit = 1 << 5;
        let caller_flags = FooterFeatureFlags(
            FooterFeatureFlags::SEQ_TXN_BIT
                | FooterFeatureFlags::SCRATCHPAD_BIT
                | unknown_optional_bit,
        );
        let mut fb = FooterBuilder::new(0, 0);
        fb.feature_flags(caller_flags);
        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        let footer = parse_footer(&buf, start);
        assert_eq!(footer.feature_flags().0, unknown_optional_bit);
        assert_eq!(footer.seq_txn(), None);
        assert_eq!(footer.scratchpad_entries().count(), 0);
    }

    #[test]
    fn footer_length_exactly_base_size() {
        // Boundary: footer_length_through_crc exactly equals base_through_crc must succeed.
        let fb = FooterBuilder::new(0, 0);
        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        let exact = (FOOTER_FIXED_SIZE + FOOTER_CHECKSUM_SIZE) as u32;
        Footer::new(&buf[start..], exact, 0).unwrap();
    }

    #[test]
    fn seq_txn_round_trip() {
        let mut fb = FooterBuilder::new(0, 0);
        fb.set_seq_txn(SeqTxn::new(42));
        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        let footer = parse_footer(&buf, start);
        assert!(footer.feature_flags().has_seq_txn());
        assert_eq!(footer.seq_txn(), Some(SeqTxn::new(42)));
    }

    #[test]
    fn seq_txn_absent_when_unset() {
        let fb = FooterBuilder::new(0, 0);
        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        let footer = parse_footer(&buf, start);
        assert!(!footer.feature_flags().has_seq_txn());
        assert_eq!(footer.seq_txn(), None);
    }

    #[test]
    fn seq_txn_unset_sentinel_omits_section() {
        let fb_none = FooterBuilder::new(0, 0);
        let mut buf_none = Vec::new();
        fb_none.write_to(&mut buf_none);

        let mut fb_unset = FooterBuilder::new(0, 0);
        fb_unset.set_seq_txn(SeqTxn::UNSET);
        let mut buf_unset = Vec::new();
        fb_unset.write_to(&mut buf_unset);

        assert_eq!(buf_none, buf_unset);
    }

    #[test]
    fn seq_txn_negative_values_round_trip() {
        // i64::MIN must survive — only `SeqTxn::UNSET` (-1) is a sentinel.
        let mut fb = FooterBuilder::new(0, 0);
        fb.set_seq_txn(SeqTxn::new(i64::MIN));
        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        let footer = parse_footer(&buf, start);
        assert_eq!(footer.seq_txn(), Some(SeqTxn::new(i64::MIN)));
    }

    #[test]
    fn seq_txn_truncated_payload_rejected() {
        // Flip SEQ_TXN_BIT without appending the 8-byte payload.
        let fb = FooterBuilder::new(0, 0);
        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        let flags = crate::types::FooterFeatureFlags::SEQ_TXN_BIT;
        let flags_off = start + crate::types::FOOTER_FEATURE_FLAGS_OFF;
        buf[flags_off..flags_off + 8].copy_from_slice(&flags.to_le_bytes());

        let trailer = u32::from_le_bytes(buf[buf.len() - 4..].try_into().unwrap());
        // SEQ_TXN_BIT set but no payload follows the row group entries.
        assert!(Footer::new(&buf[start..], trailer, 0).is_err());
    }

    #[test]
    fn scratchpad_round_trip() {
        let mut fb = FooterBuilder::new(0, 0);
        fb.set_scratchpad_entries(vec![(0xCAFE_BABE, vec![1, 2, 3, 4, 5])]);
        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        let footer = parse_footer(&buf, start);
        assert!(footer.feature_flags().has_scratchpad());
        assert_eq!(
            footer.scratchpad_entry(0xCAFE_BABE),
            Some(&[1, 2, 3, 4, 5][..])
        );
        let entries: Vec<_> = footer.scratchpad_entries().collect();
        assert_eq!(entries, vec![(0xCAFE_BABE, &[1, 2, 3, 4, 5][..])]);
    }

    #[test]
    fn scratchpad_multiple_entries() {
        let mut fb = FooterBuilder::new(0, 0);
        fb.set_scratchpad_entries(vec![
            (1, vec![0xAA; 4]),
            (2, vec![0xBB; 16]),
            (3, vec![0xCC; 32]),
        ]);
        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        let footer = parse_footer(&buf, start);
        let entries: Vec<_> = footer.scratchpad_entries().collect();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0], (1u32, &[0xAA; 4][..]));
        assert_eq!(entries[1], (2u32, &[0xBB; 16][..]));
        assert_eq!(entries[2], (3u32, &[0xCC; 32][..]));
        assert_eq!(footer.scratchpad_entry(2), Some(&[0xBB; 16][..]));
        assert_eq!(footer.scratchpad_entry(99), None);
    }

    #[test]
    fn scratchpad_empty_omits_bit() {
        let fb_none = FooterBuilder::new(0, 0);
        let mut buf_none = Vec::new();
        fb_none.write_to(&mut buf_none);

        let mut fb_empty = FooterBuilder::new(0, 0);
        fb_empty.set_scratchpad_entries(vec![]);
        let mut buf_empty = Vec::new();
        fb_empty.write_to(&mut buf_empty);

        assert_eq!(buf_none, buf_empty);
    }

    #[test]
    fn scratchpad_truncated_payload_rejected() {
        // Flip SCRATCHPAD_BIT without appending the count.
        let fb = FooterBuilder::new(0, 0);
        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        let flags = crate::types::FooterFeatureFlags::SCRATCHPAD_BIT;
        let flags_off = start + crate::types::FOOTER_FEATURE_FLAGS_OFF;
        buf[flags_off..flags_off + 8].copy_from_slice(&flags.to_le_bytes());

        let trailer = u32::from_le_bytes(buf[buf.len() - 4..].try_into().unwrap());
        assert!(Footer::new(&buf[start..], trailer, 0).is_err());
    }

    #[test]
    fn scratchpad_zero_count_with_bit_rejected() {
        // Hand-craft a footer with SCRATCHPAD_BIT set and entry_count=0 on disk.
        let mut buf = Vec::new();
        buf.extend_from_slice(&0u64.to_le_bytes()); // parquet_footer_offset
        buf.extend_from_slice(&0u32.to_le_bytes()); // parquet_footer_length
        buf.extend_from_slice(&0u32.to_le_bytes()); // row_group_count
        buf.extend_from_slice(&0u64.to_le_bytes()); // unused_bytes
        buf.extend_from_slice(&0u64.to_le_bytes()); // prev_parquet_meta_file_size
        buf.extend_from_slice(&crate::types::FooterFeatureFlags::SCRATCHPAD_BIT.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes()); // entry_count = 0
        buf.extend_from_slice(&0u32.to_le_bytes()); // CRC placeholder
        let footer_len = buf.len() as u32;
        buf.extend_from_slice(&footer_len.to_le_bytes());

        let err = match Footer::new(&buf, footer_len, 0) {
            Err(e) => e,
            Ok(_) => panic!("expected zero-count rejection"),
        };
        assert_eq!(err.kind, ParquetMetaErrorKind::InvalidValue);
    }

    #[test]
    fn scratchpad_entry_header_exceeds_crc_rejected() {
        // entry_count > 0 but no room left for even the first 8-byte header.
        let mut buf = Vec::new();
        buf.extend_from_slice(&0u64.to_le_bytes()); // parquet_footer_offset
        buf.extend_from_slice(&0u32.to_le_bytes()); // parquet_footer_length
        buf.extend_from_slice(&0u32.to_le_bytes()); // row_group_count
        buf.extend_from_slice(&0u64.to_le_bytes()); // unused_bytes
        buf.extend_from_slice(&0u64.to_le_bytes()); // prev_parquet_meta_file_size
        buf.extend_from_slice(&crate::types::FooterFeatureFlags::SCRATCHPAD_BIT.to_le_bytes());
        buf.extend_from_slice(&1u32.to_le_bytes()); // entry_count = 1
                                                    // No entry header bytes follow; CRC sits right here.
        buf.extend_from_slice(&0u32.to_le_bytes()); // CRC placeholder
        let footer_len = buf.len() as u32;
        buf.extend_from_slice(&footer_len.to_le_bytes());

        let err = match Footer::new(&buf, footer_len, 0) {
            Err(e) => e,
            Ok(_) => panic!("expected entry-header-exceeds-CRC rejection"),
        };
        assert_eq!(err.kind, ParquetMetaErrorKind::Truncated);
    }

    #[test]
    fn scratchpad_entry_length_overflow_rejected() {
        // Build a valid 1-entry scratchpad, then bump the on-disk length so the
        // declared content extends past the CRC offset.
        let mut fb = FooterBuilder::new(0, 0);
        fb.set_scratchpad_entries(vec![(1, vec![0xAA; 4])]);
        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        // Locate the entry's length field: footer_start + FOOTER_FIXED_SIZE
        // (no row groups, no bloom, no seq_txn) + 4 (entry_count) + 4 (code).
        let length_off = start + FOOTER_FIXED_SIZE + 4 + 4;
        buf[length_off..length_off + 4].copy_from_slice(&u32::MAX.to_le_bytes());

        let trailer = u32::from_le_bytes(buf[buf.len() - 4..].try_into().unwrap());
        let err = match Footer::new(&buf[start..], trailer, 0) {
            Err(e) => e,
            Ok(_) => panic!("expected length-overflow rejection"),
        };
        assert_eq!(err.kind, ParquetMetaErrorKind::Truncated);
    }

    #[test]
    fn scratchpad_exceeds_max_size_rejected() {
        use crate::types::MAX_SCRATCHPAD_SIZE;

        // Hand-craft a footer whose scratchpad declares one entry larger
        // than MAX_SCRATCHPAD_SIZE but truncated by CRC.
        let mut buf = Vec::new();
        buf.extend_from_slice(&0u64.to_le_bytes()); // parquet_footer_offset
        buf.extend_from_slice(&0u32.to_le_bytes()); // parquet_footer_length
        buf.extend_from_slice(&0u32.to_le_bytes()); // row_group_count
        buf.extend_from_slice(&0u64.to_le_bytes()); // unused_bytes
        buf.extend_from_slice(&0u64.to_le_bytes()); // prev_parquet_meta_file_size
        buf.extend_from_slice(&crate::types::FooterFeatureFlags::SCRATCHPAD_BIT.to_le_bytes());
        // Scratchpad: count=1, code=0, length=MAX_SCRATCHPAD_SIZE, content=MAX_SCRATCHPAD_SIZE bytes
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(&(MAX_SCRATCHPAD_SIZE as u32).to_le_bytes());
        buf.extend(std::iter::repeat_n(0u8, MAX_SCRATCHPAD_SIZE));
        buf.extend_from_slice(&0u32.to_le_bytes()); // CRC placeholder
        let footer_len = buf.len() as u32;
        buf.extend_from_slice(&footer_len.to_le_bytes());

        // Payload size = 4 (count) + 8 (header) + MAX = MAX + 12 > MAX, rejected.
        let err = match Footer::new(&buf, footer_len, 0) {
            Err(e) => e,
            Ok(_) => panic!("expected MAX_SCRATCHPAD_SIZE rejection"),
        };
        assert_eq!(err.kind, ParquetMetaErrorKind::InvalidValue);
    }

    #[test]
    fn validate_scratchpad_enforces_reader_cap() {
        use crate::types::MAX_SCRATCHPAD_SIZE;

        // payload_size = 4 (entry count) + 8 (per-entry code+len) + content.len(),
        // so a single entry of MAX-12 lands the payload exactly on the cap. That is
        // accepted; one byte over is rejected with the same error the reader raises,
        // keeping the release-mode writer symmetric with parse_scratchpad_size.
        let at_limit_content = MAX_SCRATCHPAD_SIZE - 12;

        let mut fb = FooterBuilder::new(0, 0);
        fb.set_scratchpad_entries(vec![(0, vec![0u8; at_limit_content])]);
        assert!(
            fb.validate_scratchpad().is_ok(),
            "a payload equal to MAX_SCRATCHPAD_SIZE must be accepted"
        );

        let mut fb = FooterBuilder::new(0, 0);
        fb.set_scratchpad_entries(vec![(0, vec![0u8; at_limit_content + 1])]);
        let err = fb
            .validate_scratchpad()
            .expect_err("a payload one byte over MAX_SCRATCHPAD_SIZE must be rejected");
        assert_eq!(err.kind, ParquetMetaErrorKind::InvalidValue);

        // An empty scratchpad is a no-op.
        assert!(FooterBuilder::new(0, 0).validate_scratchpad().is_ok());
    }

    #[test]
    fn scratchpad_coexists_with_seq_txn_and_bloom() {
        let mut fb = FooterBuilder::new(0, 0);
        fb.add_row_group_offset(0).unwrap();
        fb.set_bloom_filter_section(vec![0xAA, 0xBB, 0xCC, 0xDD]);
        fb.set_seq_txn(SeqTxn::new(7));
        fb.set_scratchpad_entries(vec![(42, vec![0xEE, 0xFF])]);
        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        let footer = parse_footer_with_bloom(&buf, start, 4);
        assert!(footer.feature_flags().has_seq_txn());
        assert!(footer.feature_flags().has_scratchpad());

        // Layout: row_group_entries(4) → bloom(4) → seq_txn(8) → scratchpad(...) → CRC.
        let entries_end = FOOTER_FIXED_SIZE + ROW_GROUP_ENTRY_SIZE;
        assert_eq!(
            &buf[start + entries_end..start + entries_end + 4],
            &[0xAA, 0xBB, 0xCC, 0xDD]
        );
        let bloom_end = start + footer.bloom_section_end();
        assert_eq!(&buf[bloom_end..bloom_end + 8], &7i64.to_le_bytes());

        let scratchpad_start = bloom_end + 8;
        // entry_count = 1
        assert_eq!(
            u32::from_le_bytes(
                buf[scratchpad_start..scratchpad_start + 4]
                    .try_into()
                    .unwrap()
            ),
            1
        );
        // code = 42
        assert_eq!(
            u32::from_le_bytes(
                buf[scratchpad_start + 4..scratchpad_start + 8]
                    .try_into()
                    .unwrap()
            ),
            42
        );
        // length = 2
        assert_eq!(
            u32::from_le_bytes(
                buf[scratchpad_start + 8..scratchpad_start + 12]
                    .try_into()
                    .unwrap()
            ),
            2
        );
        assert_eq!(
            &buf[scratchpad_start + 12..scratchpad_start + 14],
            &[0xEE, 0xFF]
        );
        assert_eq!(scratchpad_start + 14, start + footer.crc_offset());

        assert_eq!(footer.scratchpad_entry(42), Some(&[0xEE, 0xFF][..]));
        assert_eq!(footer.seq_txn(), Some(SeqTxn::new(7)));
    }

    #[test]
    fn scratchpad_lookup_returns_first_match() {
        let mut fb = FooterBuilder::new(0, 0);
        fb.set_scratchpad_entries(vec![(7, vec![0x11]), (7, vec![0x22])]);
        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        let footer = parse_footer(&buf, start);
        assert_eq!(footer.scratchpad_entry(7), Some(&[0x11][..]));
    }

    #[test]
    fn seq_txn_coexists_with_bloom_filter_section() {
        // Asserts on-disk order: bloom section first, then seq_txn (8 bytes).
        let mut fb = FooterBuilder::new(0, 0);
        fb.add_row_group_offset(0).unwrap();
        fb.set_seq_txn(SeqTxn::new(7));
        fb.set_bloom_filter_section(vec![0xAA, 0xBB, 0xCC, 0xDD]);
        let mut buf = Vec::new();
        let start = fb.write_to(&mut buf);

        let footer = parse_footer_with_bloom(&buf, start, 4);
        assert!(footer.feature_flags().has_seq_txn());
        assert_eq!(footer.seq_txn(), Some(SeqTxn::new(7)));

        let entries_end = FOOTER_FIXED_SIZE + ROW_GROUP_ENTRY_SIZE;
        assert_eq!(
            &buf[start + entries_end..start + entries_end + 4],
            &[0xAA, 0xBB, 0xCC, 0xDD]
        );
        let bloom_end = start + footer.bloom_section_end();
        assert_eq!(&buf[bloom_end..bloom_end + 8], &7i64.to_le_bytes());
        assert_eq!(bloom_end + 8, start + footer.crc_offset());
    }

    #[test]
    fn unknown_optional_flag_with_trailing_payload_accepted() {
        // Forward-compat: a newer writer set an unknown optional footer bit and
        // appended its section after the known ones. This reader skips the
        // unknown bytes and accepts the footer -- `Footer::new` tolerates
        // `sections_end < crc_offset`.
        let unknown_bit: u64 = 1 << 5;
        let trailing_payload = [0xABu8; 12];

        let mut buf = Vec::new();
        buf.extend_from_slice(&0u64.to_le_bytes()); // parquet_footer_offset
        buf.extend_from_slice(&0u32.to_le_bytes()); // parquet_footer_length
        buf.extend_from_slice(&0u32.to_le_bytes()); // row_group_count
        buf.extend_from_slice(&0u64.to_le_bytes()); // unused_bytes
        buf.extend_from_slice(&0u64.to_le_bytes()); // prev_parquet_meta_file_size
        let flags = FooterFeatureFlags::SEQ_TXN_BIT | unknown_bit;
        buf.extend_from_slice(&flags.to_le_bytes());
        // Known section: seq_txn (bit 0), 8 bytes.
        buf.extend_from_slice(&7i64.to_le_bytes());
        // Unknown optional section (bit 5): opaque payload this reader skips.
        let known_sections_end = buf.len();
        buf.extend_from_slice(&trailing_payload);
        buf.extend_from_slice(&0u32.to_le_bytes()); // CRC placeholder
        let footer_len = buf.len() as u32;
        buf.extend_from_slice(&footer_len.to_le_bytes());

        let footer = Footer::new(&buf, footer_len, 0).unwrap();

        // The unknown bit is preserved; the known accessors ignore it.
        assert_eq!(footer.feature_flags().0, flags);
        assert!(footer.feature_flags().has_seq_txn());
        assert_eq!(footer.seq_txn(), Some(SeqTxn::new(7)));
        assert_eq!(footer.scratchpad_entries().count(), 0);

        // Trailing payload sits between the known sections and the CRC:
        // `sections_end < crc_offset` is tolerated, not rejected.
        assert_eq!(
            footer.crc_offset(),
            known_sections_end + trailing_payload.len()
        );
        assert!(footer.crc_offset() > known_sections_end);
    }
}
