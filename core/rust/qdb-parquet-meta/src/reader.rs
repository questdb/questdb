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

use crate::error::ParquetMetaErrorKind;
use crate::error::ParquetMetaResult;
use crate::footer::Footer;
use crate::header::{ColumnDescriptorRaw, FileHeader};
use crate::parquet_meta_err;
use crate::row_group::RowGroupBlockReader;
use crate::types::{
    FooterFeatureFlags, HeaderFeatureFlags, FOOTER_CHECKSUM_SIZE, FOOTER_FIXED_SIZE,
    FOOTER_TRAILER_SIZE, HEADER_CRC_AREA_OFF, ROW_GROUP_ENTRY_SIZE,
};

/// Main reader for a `_pm` metadata file.
///
/// Operates directly over a `&[u8]` slice (typically from an mmap).
/// Validates the format version and CRC32 checksum on construction.
pub struct ParquetMetaReader<'a> {
    data: &'a [u8],
    header: FileHeader<'a>,
    footer: Footer<'a>,
    footer_offset: u64,
    /// Bloom filter footer section data slice, if present.
    bloom_filter_section: Option<&'a [u8]>,
}

impl<'a> ParquetMetaReader<'a> {
    /// Creates a reader from the committed `_pm` file size.
    ///
    /// `file_size` is the total committed size of the `_pm` file — the value
    /// stored in the header at `HEADER_PARQUET_META_FILE_SIZE_OFF`. The caller is
    /// expected to have read that value from the header before calling here
    /// (and before sizing any mmap). The last 4 bytes of the committed view
    /// store the footer length, from which this method derives the footer
    /// offset as `file_size - 4 - footer_length`.
    ///
    /// The resulting reader is bound to the first `file_size` bytes of `data`.
    /// Any trailing bytes (e.g. from an in-progress append that hasn't been
    /// published yet) are ignored.
    #[must_use = "returns the reader"]
    pub fn from_file_size(data: &'a [u8], file_size: u64) -> ParquetMetaResult<Self> {
        let file_size_usize = usize::try_from(file_size).map_err(|_| {
            parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "file size {} exceeds addressable range",
                file_size
            )
        })?;
        let file_data = data.get(..file_size_usize).ok_or_else(|| {
            parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "file size {} exceeds available data {}",
                file_size,
                data.len()
            )
        })?;
        if file_size_usize < FOOTER_TRAILER_SIZE {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "file too small for footer trailer: {} bytes",
                file_size
            ));
        }
        let trailer = file_data
            .get(file_size_usize - FOOTER_TRAILER_SIZE..file_size_usize)
            .ok_or_else(|| {
                parquet_meta_err!(
                    ParquetMetaErrorKind::Truncated,
                    "footer trailer out of bounds at file size {}",
                    file_size
                )
            })?;
        // Expect: .get() returned a FOOTER_TRAILER_SIZE (4) byte slice.
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
        Self::new(file_data, footer_offset)
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
    pub fn new(data: &'a [u8], footer_offset: u64) -> ParquetMetaResult<Self> {
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
        // Expect: footer_data.len() >= FOOTER_TRAILER_SIZE checked above,
        // so the slice is exactly FOOTER_TRAILER_SIZE (4) bytes.
        let trailer_start = footer_data.len() - FOOTER_TRAILER_SIZE;
        let footer_length = u32::from_le_bytes(
            footer_data[trailer_start..trailer_start + FOOTER_TRAILER_SIZE]
                .try_into()
                .expect("slice is 4 bytes"),
        );

        let footer = Footer::new(footer_data, footer_length)?;

        // Reject footers that carry unknown required flag bits. The currently
        // selected footer may be newer than this reader understands, in which
        // case reading it would silently miss a required per-footer section.
        let unknown_required = footer.feature_flags().unknown_required(0);
        if unknown_required != 0 {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "unsupported required footer feature flags [flags=0x{:X}]",
                unknown_required
            ));
        }

        // Parse bloom filter footer section if the feature flag is set.
        let bloom_filter_section = if header.feature_flags().has_bloom_filters() {
            let bloom_col_count = header.bloom_filter_column_count() as usize;
            let is_external = header.feature_flags().has_bloom_filters_external();
            let entry_size = if is_external { 16 } else { 4 };
            let rg_count = footer.row_group_count() as usize;
            let section_size = rg_count
                .checked_mul(bloom_col_count)
                .and_then(|n| n.checked_mul(entry_size))
                .ok_or_else(|| {
                    parquet_meta_err!(
                        ParquetMetaErrorKind::Truncated,
                        "bloom filter footer section size overflow"
                    )
                })?;
            let section_start = FOOTER_FIXED_SIZE + rg_count * ROW_GROUP_ENTRY_SIZE;
            let section_end = section_start + section_size;
            if section_end > footer.crc_offset() {
                return Err(parquet_meta_err!(
                    ParquetMetaErrorKind::Truncated,
                    "bloom filter footer section exceeds CRC offset: {} > {}",
                    section_end,
                    footer.crc_offset()
                ));
            }
            if section_end > footer_data.len() {
                return Err(parquet_meta_err!(
                    ParquetMetaErrorKind::Truncated,
                    "bloom filter footer section exceeds footer data"
                ));
            }
            Some(&footer_data[section_start..section_end])
        } else {
            None
        };

        Ok(Self {
            data,
            header,
            footer,
            footer_offset,
            bloom_filter_section,
        })
    }

    /// Verifies the CRC32 checksum stored in the footer.
    ///
    /// The CRC covers bytes `[HEADER_CRC_AREA_OFF, crc_field_offset)` — everything
    /// after the `footer_offset` field (which is mutable on update). This protects
    /// feature flags, column descriptors, row group blocks, and all footer versions.
    pub fn verify_checksum(&self) -> ParquetMetaResult<()> {
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
        let computed_crc = crc32fast::hash(&self.data[HEADER_CRC_AREA_OFF..checksum_abs]);
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
    pub fn column_descriptor(&self, index: usize) -> ParquetMetaResult<&'a ColumnDescriptorRaw> {
        self.header.column_descriptor(index)
    }

    /// Returns the UTF-8 name of the column at `index`.
    pub fn column_name(&self, index: usize) -> ParquetMetaResult<&'a str> {
        let desc = self.header.column_descriptor(index)?;
        self.header.column_name(desc)
    }

    /// Returns the sorting column index at position `i`.
    pub fn sorting_column(&self, i: usize) -> ParquetMetaResult<u32> {
        self.header.sorting_column(i)
    }

    pub fn sorting_column_count(&self) -> u32 {
        self.header.sorting_column_count()
    }

    /// Returns a zero-copy reader for the row group at `index`.
    pub fn row_group(&self, index: usize) -> ParquetMetaResult<RowGroupBlockReader<'a>> {
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

    pub fn feature_flags(&self) -> HeaderFeatureFlags {
        self.header.feature_flags()
    }

    /// Returns the feature flags stored in the currently selected footer.
    pub fn footer_feature_flags(&self) -> FooterFeatureFlags {
        self.footer.feature_flags()
    }

    /// Partition squash tracker, or `None` when the SQUASH_TRACKER bit is not
    /// set. Consumed by the enterprise build; OSS does not read it today.
    pub fn squash_tracker(&self) -> Option<i64> {
        self.header.squash_tracker()
    }

    /// Returns the raw file data slice.
    pub fn data(&self) -> &'a [u8] {
        self.data
    }

    /// Returns the total committed `_pm` file size as encoded in the header.
    /// For a well-formed file this matches `data().len()`.
    pub fn parquet_meta_file_size(&self) -> u64 {
        self.header.parquet_meta_file_size()
    }

    /// Returns the footer offset within this file.
    pub fn footer_offset(&self) -> u64 {
        self.footer_offset
    }

    /// Returns the committed `_pm` file size at the time of the previous
    /// snapshot (0 if this is the first snapshot in the chain).
    pub fn prev_parquet_meta_file_size(&self) -> u64 {
        self.footer.prev_parquet_meta_file_size()
    }

    /// Walks the MVCC chain starting from the latest snapshot to find the
    /// footer whose parquet file size matches `target_parquet_size`. Each
    /// step reads `prev_parquet_meta_file_size` from the current footer,
    /// then derives the previous footer location from the trailer at
    /// `prev_size - 4` — the same size-then-trailer indirection the header
    /// uses for the latest footer.
    ///
    /// Parquet file size is derived from each footer as:
    /// `parquet_footer_offset + parquet_footer_length + 8`.
    ///
    /// Returns `(footer_offset, Footer)` for the matching footer.
    pub fn find_footer_for_parquet_size(
        data: &'a [u8],
        parquet_meta_file_size: u64,
        target_parquet_size: u64,
    ) -> ParquetMetaResult<(u64, Footer<'a>)> {
        let mut current_size = parquet_meta_file_size;
        loop {
            if current_size < (FOOTER_TRAILER_SIZE as u64) {
                return Err(parquet_meta_err!(
                    ParquetMetaErrorKind::Truncated,
                    "chain walk: _pm size {} too small for trailer",
                    current_size
                ));
            }
            let current_size_usize = usize::try_from(current_size).map_err(|_| {
                parquet_meta_err!(
                    ParquetMetaErrorKind::Truncated,
                    "_pm size {} exceeds addressable range",
                    current_size
                )
            })?;
            let trailer_abs = current_size_usize - FOOTER_TRAILER_SIZE;
            let trailer = data
                .get(trailer_abs..trailer_abs + FOOTER_TRAILER_SIZE)
                .ok_or_else(|| {
                    parquet_meta_err!(
                        ParquetMetaErrorKind::Truncated,
                        "trailer at {} out of bounds",
                        trailer_abs
                    )
                })?;
            // Safety: .get() returned a 4-byte slice.
            let footer_length = u32::from_le_bytes(trailer.try_into().expect("slice is 4 bytes"));
            let current_offset = current_size
                .checked_sub(FOOTER_TRAILER_SIZE as u64)
                .and_then(|s| s.checked_sub(footer_length as u64))
                .ok_or_else(|| {
                    parquet_meta_err!(
                        ParquetMetaErrorKind::Truncated,
                        "footer length {} exceeds _pm size {}",
                        footer_length,
                        current_size
                    )
                })?;
            let current_offset_usize = current_offset as usize;
            let footer_data = data
                .get(current_offset_usize..current_size_usize)
                .ok_or_else(|| {
                    parquet_meta_err!(
                        ParquetMetaErrorKind::Truncated,
                        "footer at {}..{} out of bounds",
                        current_offset,
                        current_size
                    )
                })?;
            let footer = Footer::new(footer_data, footer_length)?;

            let pq_size =
                footer.parquet_footer_offset() + footer.parquet_footer_length() as u64 + 8;
            if pq_size == target_parquet_size {
                return Ok((current_offset, footer));
            }

            let prev_size = footer.prev_parquet_meta_file_size();
            if prev_size == 0 || prev_size >= current_size {
                return Err(parquet_meta_err!(
                    ParquetMetaErrorKind::InvalidValue,
                    "no footer found for parquet size {}",
                    target_parquet_size
                ));
            }
            current_size = prev_size;
        }
    }

    /// Returns true if the bloom filter feature is enabled.
    pub fn has_bloom_filters(&self) -> bool {
        self.header.feature_flags().has_bloom_filters()
    }

    /// Returns true if bloom filter bitsets are stored in the parquet file.
    pub fn has_bloom_filters_external(&self) -> bool {
        self.header.feature_flags().has_bloom_filters_external()
    }

    /// Delegates to the header's binary search for the bloom filter column position.
    pub fn bloom_filter_position(&self, col_idx: u32) -> Option<usize> {
        self.header.bloom_filter_position(col_idx)
    }

    /// Returns an iterator over bloom filter column indices.
    pub fn bloom_filter_columns(&self) -> Vec<u32> {
        let count = self.header.bloom_filter_column_count();
        // Unwrap: pos < count, so bloom_filter_column cannot fail.
        (0..count as usize)
            .map(|pos| self.header.bloom_filter_column(pos).unwrap())
            .collect()
    }

    /// Returns the inlined bloom filter offset in the _pm file for the given
    /// `(rg_idx, pos)` pair. `pos` is the bloom filter column position (from
    /// `bloom_filter_position`). Returns 0 if absent.
    ///
    /// The stored value is `absolute_byte_offset >> 3`. This method returns
    /// the actual byte offset.
    pub fn bloom_filter_offset_in_pm(&self, rg_idx: usize, pos: usize) -> ParquetMetaResult<u64> {
        let section = self.bloom_filter_section.ok_or_else(|| {
            parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "no bloom filter section"
            )
        })?;
        let bloom_col_count = self.header.bloom_filter_column_count() as usize;
        let idx = rg_idx * bloom_col_count + pos;
        let off = idx * 4;
        if off + 4 > section.len() {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "bloom filter offset out of bounds: rg={}, pos={}",
                rg_idx,
                pos
            ));
        }
        // Unwrap: off + 4 <= section.len() checked above.
        let stored = u32::from_le_bytes(section[off..off + 4].try_into().unwrap());
        Ok((stored as u64) << crate::types::BLOCK_ALIGNMENT_SHIFT)
    }

    /// Returns the `(parquet_offset, parquet_length)` for an external bloom
    /// filter at `(rg_idx, pos)`. Returns `(0, 0)` if absent.
    pub fn bloom_filter_parquet_ref(
        &self,
        rg_idx: usize,
        pos: usize,
    ) -> ParquetMetaResult<(u64, u64)> {
        let section = self.bloom_filter_section.ok_or_else(|| {
            parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "no bloom filter section"
            )
        })?;
        let bloom_col_count = self.header.bloom_filter_column_count() as usize;
        let idx = rg_idx * bloom_col_count + pos;
        let off = idx * 16;
        if off + 16 > section.len() {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "bloom filter parquet ref out of bounds: rg={}, pos={}",
                rg_idx,
                pos
            ));
        }
        // Unwraps: off + 16 <= section.len() checked above.
        let parquet_offset = u64::from_le_bytes(section[off..off + 8].try_into().unwrap());
        let parquet_length = u64::from_le_bytes(section[off + 8..off + 16].try_into().unwrap());
        Ok((parquet_offset, parquet_length))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_chunk::ColumnChunkRaw;
    use crate::row_group::RowGroupBlockBuilder;
    use crate::types::{encode_stat_sizes, ColumnFlags, FieldRepetition, StatFlags};
    use crate::writer::ParquetMetaWriter;

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
        let (bytes, parquet_meta_file_size) = w.finish().unwrap();

        let reader = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();
        assert_eq!(reader.column_count(), types.len() as u32);
        assert_eq!(reader.designated_timestamp(), Some(5));

        for (i, (type_code, name)) in types.iter().enumerate() {
            assert_eq!(reader.column_name(i).unwrap(), *name);
            let desc = reader.column_descriptor(i).unwrap();
            assert_eq!(desc.col_type, *type_code);
        }
    }

    /// Derives the footer offset of a freshly finished file by reading the
    /// trailer. Only needed by tests that want to corrupt specific footer
    /// bytes before re-opening.
    fn footer_offset_of(bytes: &[u8], parquet_meta_file_size: u64) -> u64 {
        let end = parquet_meta_file_size as usize;
        let trailer = &bytes[end - FOOTER_TRAILER_SIZE..end];
        let footer_length =
            u32::from_le_bytes(trailer.try_into().expect("slice is 4 bytes")) as u64;
        parquet_meta_file_size - FOOTER_TRAILER_SIZE as u64 - footer_length
    }

    #[test]
    fn crc_corruption_detected() {
        let mut w = ParquetMetaWriter::new();
        w.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let mut rg = RowGroupBlockBuilder::new(1);
        rg.set_num_rows(42);
        w.add_row_group(rg);
        let (mut bytes, parquet_meta_file_size) = w.finish().unwrap();

        // Corrupt a byte in the CRC-covered area (column descriptors, after offset 8).
        let target = crate::types::HEADER_CRC_AREA_OFF + 1;
        bytes[target] ^= 0xFF;
        let reader = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();
        assert!(reader.verify_checksum().is_err());
    }

    #[test]
    fn crc_corrupted_in_footer() {
        let mut w = ParquetMetaWriter::new();
        w.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let mut rg = RowGroupBlockBuilder::new(1);
        rg.set_num_rows(42);
        w.add_row_group(rg);
        let (mut bytes, parquet_meta_file_size) = w.finish().unwrap();

        // Corrupt a byte inside the footer (first byte after the footer start).
        let footer_offset = footer_offset_of(&bytes, parquet_meta_file_size);
        let target = footer_offset as usize;
        if target < bytes.len() {
            bytes[target] ^= 0xFF;
        }
        let reader = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();
        assert!(reader.verify_checksum().is_err());
    }

    #[test]
    fn verify_checksum_passes_on_valid_file() {
        let mut w = ParquetMetaWriter::new();
        w.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let (bytes, parquet_meta_file_size) = w.finish().unwrap();

        let reader = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();
        reader.verify_checksum().unwrap();
    }

    #[test]
    fn unknown_required_footer_flags_rejected() {
        use crate::types::FOOTER_FEATURE_FLAGS_OFF;

        let mut w = ParquetMetaWriter::new();
        w.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let (mut bytes, parquet_meta_file_size) = w.finish().unwrap();

        // Patch footer feature flags to set an unknown required bit (bit 32).
        let footer_offset = footer_offset_of(&bytes, parquet_meta_file_size);
        let flags_off = footer_offset as usize + FOOTER_FEATURE_FLAGS_OFF;
        let required_bit: u64 = 1 << 32;
        bytes[flags_off..flags_off + 8].copy_from_slice(&required_bit.to_le_bytes());

        let err = match ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size) {
            Ok(_) => panic!("expected error for required footer flag"),
            Err(e) => e,
        };
        assert!(
            format!("{err}").contains("unsupported required footer feature flags"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn unknown_optional_footer_flags_accepted() {
        use crate::types::FOOTER_FEATURE_FLAGS_OFF;

        let mut w = ParquetMetaWriter::new();
        w.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let (mut bytes, parquet_meta_file_size) = w.finish().unwrap();

        // Patch footer feature flags to set an unknown optional bit (bit 5).
        // Readers must accept the file and ignore the unknown bit.
        let footer_offset = footer_offset_of(&bytes, parquet_meta_file_size);
        let flags_off = footer_offset as usize + FOOTER_FEATURE_FLAGS_OFF;
        let optional_bit: u64 = 1 << 5;
        bytes[flags_off..flags_off + 8].copy_from_slice(&optional_bit.to_le_bytes());

        let reader = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();
        assert_eq!(reader.footer_feature_flags().0, optional_bit);
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

        let (bytes, parquet_meta_file_size) = w.finish().unwrap();
        let reader = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();

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
        let (bytes, parquet_meta_file_size) = w.finish().unwrap();

        let expected_footer_offset = footer_offset_of(&bytes, parquet_meta_file_size);
        let reader = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();
        assert_eq!(reader.footer_offset(), expected_footer_offset);
    }

    #[test]
    fn parquet_meta_file_size_matches_slice_len() {
        // The header field patched at finish() must equal bytes.len().
        let mut w = ParquetMetaWriter::new();
        w.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let (bytes, parquet_meta_file_size) = w.finish().unwrap();
        assert_eq!(parquet_meta_file_size, bytes.len() as u64);

        let reader = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();
        assert_eq!(reader.parquet_meta_file_size(), parquet_meta_file_size);
        assert_eq!(reader.data().len(), bytes.len());
    }

    #[test]
    fn from_file_size_ignores_trailing_bytes() {
        // Simulate an in-progress append: actual slice is larger than the
        // committed header-reported size. Reader must behave as if only the
        // committed prefix exists.
        let mut w = ParquetMetaWriter::new();
        w.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let (mut bytes, parquet_meta_file_size) = w.finish().unwrap();
        bytes.extend_from_slice(&[0xAA_u8; 32]);

        let reader = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();
        reader.verify_checksum().unwrap();
        assert_eq!(reader.data().len(), parquet_meta_file_size as usize);
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
        let (mut bytes, parquet_meta_file_size) = w.finish().unwrap();

        // The first row group entry is at footer_offset + FOOTER_FIXED_SIZE (after the fixed fields).
        let footer_offset = footer_offset_of(&bytes, parquet_meta_file_size);
        let entry_offset = footer_offset as usize + crate::types::FOOTER_FIXED_SIZE;
        // Set the block offset to a huge value that, when shifted, exceeds the file.
        let bad_offset = 0xFFFF_FFFFu32;
        bytes[entry_offset..entry_offset + 4].copy_from_slice(&bad_offset.to_le_bytes());

        // Fix the CRC so the reader doesn't reject on checksum mismatch.
        // CRC is at len - TRAILER(4) - CRC(4).
        let crc_offset = bytes.len() - 8;
        let crc = crc32fast::hash(&bytes[HEADER_CRC_AREA_OFF..crc_offset]);
        bytes[crc_offset..crc_offset + 4].copy_from_slice(&crc.to_le_bytes());

        let reader = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();
        assert!(reader.row_group(0).is_err());
    }

    #[test]
    fn checksum_field_out_of_bounds() {
        // Construct a file where the footer claims many row groups but the data
        // is truncated so the checksum field falls outside the slice.
        // Simplest: just truncate a valid file at the footer.
        let mut w = ParquetMetaWriter::new();
        w.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let (bytes, parquet_meta_file_size) = w.finish().unwrap();

        // Truncate the file to cut off the last byte (the CRC). The
        // caller-declared parquet_meta_file_size still points past the truncation,
        // so the reader must reject.
        let truncated = &bytes[..bytes.len() - 1];
        assert!(ParquetMetaReader::from_file_size(truncated, parquet_meta_file_size).is_err());
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
        let (bytes, parquet_meta_file_size) = w.finish().unwrap();

        // from_file_size should derive the same footer offset from the trailer.
        let reader = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();
        let expected_footer_offset = footer_offset_of(&bytes, parquet_meta_file_size);
        assert_eq!(reader.footer_offset(), expected_footer_offset);
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

        let (bytes, parquet_meta_file_size) = w.finish().unwrap();
        let reader = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();

        assert_eq!(reader.sorting_column_count(), 2);
        assert_eq!(reader.sorting_column(0).unwrap(), 0);
        assert_eq!(reader.sorting_column(1).unwrap(), 1);
    }

    #[test]
    fn unused_bytes_through_reader() {
        let mut w = ParquetMetaWriter::new();
        w.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        w.unused_bytes(4096);
        let (bytes, parquet_meta_file_size) = w.finish().unwrap();

        let reader = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();
        assert_eq!(reader.unused_bytes(), 4096);
    }

    #[test]
    fn data_accessor_returns_full_slice() {
        let mut w = ParquetMetaWriter::new();
        w.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let (bytes, parquet_meta_file_size) = w.finish().unwrap();

        let reader = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();
        assert_eq!(reader.data().len(), bytes.len());
        assert_eq!(reader.data().as_ptr(), bytes.as_ptr());
    }

    #[test]
    fn footer_region_too_small_for_trailer() {
        // Build a valid file, then call new() with a footer_offset so close to
        // the end that the remaining bytes can't hold the 4-byte trailer.
        let mut w = ParquetMetaWriter::new();
        w.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let (bytes, _) = w.finish().unwrap();

        // Pass a footer offset that leaves only 3 bytes after it (less than FOOTER_TRAILER_SIZE).
        let bad_offset = (bytes.len() - 3) as u64;
        assert!(ParquetMetaReader::new(&bytes, bad_offset).is_err());
    }

    #[test]
    fn bloom_filter_inlined_round_trip() {
        let mut w = ParquetMetaWriter::new();
        w.add_column("a", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        w.add_column("b", 1, 6, ColumnFlags::new(), 0, 0, 0, 0);

        let bitset = vec![0xAA_u8; 64];
        let mut rg = RowGroupBlockBuilder::new(2);
        rg.set_num_rows(100);
        rg.add_bloom_filter(1, &bitset).unwrap();
        w.add_row_group(rg);

        let (bytes, parquet_meta_file_size) = w.finish().unwrap();
        let reader = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();
        reader.verify_checksum().unwrap();

        assert!(reader.has_bloom_filters());
        assert!(!reader.has_bloom_filters_external());
        assert_eq!(reader.bloom_filter_columns(), vec![1]);
        assert_eq!(reader.bloom_filter_position(0), None);
        assert_eq!(reader.bloom_filter_position(1), Some(0));

        // Read inlined offset.
        let off = reader.bloom_filter_offset_in_pm(0, 0).unwrap();
        assert_ne!(off, 0);
        assert_eq!(off % 8, 0);

        // Read [i32 len][bitset] at the offset.
        let bf_data = &bytes[off as usize..];
        let bf_len = i32::from_le_bytes(bf_data[..4].try_into().unwrap()) as usize;
        assert_eq!(bf_len, 64);
        assert_eq!(&bf_data[4..4 + bf_len], &bitset);
    }

    #[test]
    fn bloom_filter_external_round_trip() {
        let mut w = ParquetMetaWriter::new();
        w.add_column("a", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        w.set_bloom_filters_external(true);

        let mut rg = RowGroupBlockBuilder::new(1);
        rg.set_num_rows(100);
        rg.add_external_bloom_filter(0, 4096, 512).unwrap();
        w.add_row_group(rg);

        let (bytes, parquet_meta_file_size) = w.finish().unwrap();
        let reader = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();
        reader.verify_checksum().unwrap();

        assert!(reader.has_bloom_filters());
        assert!(reader.has_bloom_filters_external());
        assert_eq!(reader.bloom_filter_columns(), vec![0]);

        let (off, len) = reader.bloom_filter_parquet_ref(0, 0).unwrap();
        assert_eq!(off, 4096);
        assert_eq!(len, 512);
    }

    #[test]
    fn bloom_filter_absent_sentinel() {
        // Two row groups, only the second has a bloom filter.
        let mut w = ParquetMetaWriter::new();
        w.add_column("a", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        w.set_bloom_filter_columns(&[0]);

        let rg0 = RowGroupBlockBuilder::new(1);
        w.add_row_group(rg0);

        let mut rg1 = RowGroupBlockBuilder::new(1);
        rg1.set_num_rows(50);
        rg1.add_bloom_filter(0, &[0xBBu8; 32]).unwrap();
        w.add_row_group(rg1);

        let (bytes, parquet_meta_file_size) = w.finish().unwrap();
        let reader = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();
        reader.verify_checksum().unwrap();

        // Row group 0 has sentinel 0 (absent).
        let off0 = reader.bloom_filter_offset_in_pm(0, 0).unwrap();
        assert_eq!(off0, 0);

        // Row group 1 has a real offset.
        let off1 = reader.bloom_filter_offset_in_pm(1, 0).unwrap();
        assert_ne!(off1, 0);
    }

    #[test]
    fn no_bloom_filters_has_no_section() {
        let mut w = ParquetMetaWriter::new();
        w.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let mut rg = RowGroupBlockBuilder::new(1);
        rg.set_num_rows(10);
        w.add_row_group(rg);

        let (bytes, parquet_meta_file_size) = w.finish().unwrap();
        let reader = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();
        reader.verify_checksum().unwrap();

        assert!(!reader.has_bloom_filters());
        assert!(reader.bloom_filter_position(0).is_none());
    }

    #[test]
    fn bloom_filter_crc_covers_section() {
        let mut w = ParquetMetaWriter::new();
        w.add_column("a", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);

        let mut rg = RowGroupBlockBuilder::new(1);
        rg.set_num_rows(100);
        rg.add_bloom_filter(0, &[0xCC_u8; 32]).unwrap();
        w.add_row_group(rg);

        let (mut bytes, parquet_meta_file_size) = w.finish().unwrap();
        let reader = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();
        reader.verify_checksum().unwrap();

        // Corrupt a byte in the bloom filter footer section.
        // The footer section is between the row group entries and CRC.
        let footer_offset = footer_offset_of(&bytes, parquet_meta_file_size);
        let footer_start = footer_offset as usize;
        // Footer: FOOTER_FIXED_SIZE (40) + 4 (1 rg entry) + bloom section + CRC + trailer
        let bloom_section_start = footer_start + FOOTER_FIXED_SIZE + 4;
        bytes[bloom_section_start] ^= 0xFF;

        let reader2 = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();
        assert!(reader2.verify_checksum().is_err());
    }
}
