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

use std::slice;

use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_metadata::error::{parquet_meta_err, ParquetMetaErrorKind};
use crate::parquet_metadata::footer::Footer;
use crate::parquet_metadata::header::{ColumnDescriptorRaw, FileHeader};
use crate::parquet_metadata::row_group::RowGroupBlockReader;
use crate::parquet_metadata::types::{
    decode_stat_sizes, FooterFeatureFlags, HeaderFeatureFlags, StatFlags, FOOTER_CHECKSUM_SIZE,
    FOOTER_FIXED_SIZE, FOOTER_TRAILER_SIZE, HEADER_CRC_AREA_OFF, ROW_GROUP_ENTRY_SIZE,
};
use crate::parquet_read::row_groups::ParquetDecoder;
use crate::parquet_read::{
    ColumnFilterPacked, ColumnFilterValues, FILTER_OP_BETWEEN, FILTER_OP_EQ, FILTER_OP_GE,
    FILTER_OP_GT, FILTER_OP_IS_NOT_NULL, FILTER_OP_IS_NULL, FILTER_OP_LE, FILTER_OP_LT,
};
use parquet2::schema::types::PhysicalType;
use qdb_core::col_type::ColumnTypeTag;

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
    pub fn from_file_size(data: &'a [u8], file_size: u64) -> ParquetResult<Self> {
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
    ) -> ParquetResult<(u64, Footer<'a>)> {
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
    pub fn bloom_filter_offset_in_pm(&self, rg_idx: usize, pos: usize) -> ParquetResult<u64> {
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
        Ok((stored as u64) << crate::parquet_metadata::types::BLOCK_ALIGNMENT_SHIFT)
    }

    /// Returns the `(parquet_offset, parquet_length)` for an external bloom
    /// filter at `(rg_idx, pos)`. Returns `(0, 0)` if absent.
    pub fn bloom_filter_parquet_ref(&self, rg_idx: usize, pos: usize) -> ParquetResult<(u64, u64)> {
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

    /// Row group filter pushdown using `_pm` metadata for both statistics and
    /// bloom filters. Bloom filter bitsets are stored inline in the `_pm`
    /// out-of-line region, so this method reads everything from the `_pm`
    /// slice and never touches the parquet data file.
    pub fn can_skip_row_group(
        &self,
        row_group_index: usize,
        filters: &[ColumnFilterPacked],
        filter_buf_end: u64,
    ) -> ParquetResult<bool> {
        if row_group_index >= self.row_group_count() as usize {
            return Err(fmt_err!(
                InvalidType,
                "row group index {} out of range [0,{})",
                row_group_index,
                self.row_group_count()
            ));
        }

        let rg_block = self.row_group(row_group_index)?;
        let col_count = self.column_count() as usize;

        for packed_filter in filters {
            let count = packed_filter.count();
            let op = packed_filter.operation_type();

            if count > 0 && packed_filter.ptr == 0 {
                return Err(fmt_err!(
                    InvalidType,
                    "invalid filter payload: null pointer with non-zero count"
                ));
            }
            let column_idx = packed_filter.column_index() as usize;
            if column_idx >= col_count {
                continue;
            }

            let chunk = rg_block.column_chunk(column_idx)?;
            let stat_flags = StatFlags(chunk.stat_flags);
            let null_count = if stat_flags.has_null_count() {
                Some(chunk.null_count as i64)
            } else {
                None
            };
            let num_values = Some(chunk.num_values as i64);

            if op == FILTER_OP_IS_NULL {
                if null_count == Some(0) {
                    return Ok(true);
                }
                continue;
            }
            if op == FILTER_OP_IS_NOT_NULL {
                if let (Some(nc), Some(nv)) = (null_count, num_values) {
                    if nc == nv {
                        return Ok(true);
                    }
                }
                continue;
            }

            let filter_desc = ColumnFilterValues {
                count,
                ptr: packed_filter.ptr,
                buf_end: filter_buf_end,
            };

            let col_desc = self.column_descriptor(column_idx)?;
            let physical_type = match col_desc.physical_type {
                0 => PhysicalType::Boolean,
                1 => PhysicalType::Int32,
                2 => PhysicalType::Int64,
                3 => PhysicalType::Int96,
                4 => PhysicalType::Float,
                5 => PhysicalType::Double,
                6 => PhysicalType::ByteArray,
                7 => PhysicalType::FixedLenByteArray(col_desc.fixed_byte_len as usize),
                _ => continue,
            };
            let has_nulls = null_count.is_none_or(|c| c > 0);
            let qdb_column_type = packed_filter.qdb_column_type();
            let col_type_tag = qdb_column_type & 0xFF;

            let is_decimal = matches!(
                col_type_tag,
                x if x == ColumnTypeTag::Decimal8 as i32
                    || x == ColumnTypeTag::Decimal16 as i32
                    || x == ColumnTypeTag::Decimal32 as i32
                    || x == ColumnTypeTag::Decimal64 as i32
                    || x == ColumnTypeTag::Decimal128 as i32
                    || x == ColumnTypeTag::Decimal256 as i32
            );

            // Read inline stats at the physical type width, not the narrowed QDB
            // width. The _pm stores narrowed values (e.g., 1 byte for BYTE) but the
            // pruning comparison expects physical type width (4 bytes for INT32).
            let phys_size = match physical_type {
                PhysicalType::Boolean => 1,
                PhysicalType::Int32 | PhysicalType::Float => 4,
                PhysicalType::Int64 | PhysicalType::Double => 8,
                PhysicalType::Int96 => 0, // not inlined
                PhysicalType::ByteArray => 0,
                PhysicalType::FixedLenByteArray(len) => len,
            };

            let ool = rg_block.out_of_line_region();

            // Inline size: for types with known physical width (Int32, Int64, etc.),
            // use phys_size clamped to 8. For ByteArray (phys_size=0), use stat_sizes
            // nibble since the value width varies (e.g., Symbol stores short strings inline).
            let (min_stat_sz, max_stat_sz) = decode_stat_sizes(chunk.stat_sizes);
            let min_inline_size = if phys_size > 0 {
                phys_size.min(8)
            } else {
                (min_stat_sz as usize).min(8)
            };
            let max_inline_size = if phys_size > 0 {
                phys_size.min(8)
            } else {
                (max_stat_sz as usize).min(8)
            };

            // Decode an OOL stat reference: `(offset << 16) | length`
            // (offset = high 48 bits, length = low 16 bits).
            let decode_ool = |encoded: u64| -> Option<&[u8]> {
                let ool_off = (encoded >> 16) as usize;
                let ool_len = (encoded & 0xFFFF) as usize;
                if ool_len == 0 {
                    return None;
                }
                ool.get(ool_off..ool_off + ool_len)
            };

            let min_bytes: Option<&[u8]> = if stat_flags.has_min_stat() {
                if stat_flags.is_min_inlined() && min_inline_size > 0 {
                    Some(unsafe {
                        slice::from_raw_parts(
                            &chunk.min_stat as *const u64 as *const u8,
                            min_inline_size,
                        )
                    })
                } else if !stat_flags.is_min_inlined() {
                    decode_ool(chunk.min_stat)
                } else {
                    None
                }
            } else {
                None
            };

            let max_bytes: Option<&[u8]> = if stat_flags.has_max_stat() {
                if stat_flags.is_max_inlined() && max_inline_size > 0 {
                    Some(unsafe {
                        slice::from_raw_parts(
                            &chunk.max_stat as *const u64 as *const u8,
                            max_inline_size,
                        )
                    })
                } else if !stat_flags.is_max_inlined() {
                    decode_ool(chunk.max_stat)
                } else {
                    None
                }
            } else {
                None
            };

            match op {
                FILTER_OP_EQ => {
                    // Bloom filter bitset lookup via footer feature section.
                    let bitset: &[u8] = if self.has_bloom_filters() {
                        if let Some(pos) = self.bloom_filter_position(column_idx as u32) {
                            if self.has_bloom_filters_external() {
                                // External mode: bitsets are in the parquet file, not
                                // available from the _pm reader alone. Skip.
                                &[]
                            } else {
                                let off = self
                                    .bloom_filter_offset_in_pm(row_group_index, pos)
                                    .unwrap_or(0);
                                if off > 0 && (off as usize) + 4 <= self.data().len() {
                                    let bf_data = &self.data()[off as usize..];
                                    let bf_len_raw = i32::from_le_bytes(
                                        bf_data[..4].try_into().unwrap_or_default(),
                                    );
                                    if bf_len_raw <= 0 {
                                        &[]
                                    } else {
                                        let bf_len = bf_len_raw as usize;
                                        bf_data.get(4..4 + bf_len).unwrap_or(&[])
                                    }
                                } else {
                                    &[]
                                }
                            }
                        } else {
                            &[]
                        }
                    } else {
                        &[]
                    };
                    if !bitset.is_empty() {
                        let all_absent = ParquetDecoder::all_values_absent_from_bloom(
                            bitset,
                            &physical_type,
                            &filter_desc,
                            has_nulls,
                            is_decimal,
                            qdb_column_type,
                        )?;
                        if all_absent {
                            return Ok(true);
                        }
                    }

                    let is_ipv4 = col_type_tag == ColumnTypeTag::IPv4 as i32;
                    let is_date = col_type_tag == ColumnTypeTag::Date as i32;
                    let is_third_party_unsigned = false;
                    if !is_third_party_unsigned {
                        if let (Some(min_b), Some(max_b)) = (min_bytes, max_bytes) {
                            if ParquetDecoder::all_values_outside_min_max_with_stats(
                                &physical_type,
                                &filter_desc,
                                has_nulls,
                                is_decimal,
                                is_ipv4,
                                is_date,
                                Some(min_b),
                                Some(max_b),
                            )? {
                                return Ok(true);
                            }
                        }
                    }
                }
                FILTER_OP_LT | FILTER_OP_LE | FILTER_OP_GT | FILTER_OP_GE | FILTER_OP_BETWEEN => {
                    let is_ipv4 = col_type_tag == ColumnTypeTag::IPv4 as i32;
                    let is_date = col_type_tag == ColumnTypeTag::Date as i32;
                    if let (Some(min_b), Some(max_b)) = (min_bytes, max_bytes) {
                        if ParquetDecoder::value_outside_range(
                            &physical_type,
                            &filter_desc,
                            is_decimal,
                            is_ipv4,
                            is_date,
                            op,
                            Some(min_b),
                            Some(max_b),
                        )? {
                            return Ok(true);
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(false)
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
        let target = crate::parquet_metadata::types::HEADER_CRC_AREA_OFF + 1;
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
        use crate::parquet_metadata::types::FOOTER_FEATURE_FLAGS_OFF;

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
        use crate::parquet_metadata::types::FOOTER_FEATURE_FLAGS_OFF;

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
        let entry_offset =
            footer_offset as usize + crate::parquet_metadata::types::FOOTER_FIXED_SIZE;
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

    // -----------------------------------------------------------------------
    // can_skip_row_group
    // -----------------------------------------------------------------------

    use crate::parquet::error::ParquetResult as TestParquetResult;
    use crate::parquet_metadata::types::Codec;
    use crate::parquet_read::{
        ColumnFilterPacked, FILTER_OP_EQ, FILTER_OP_GT, FILTER_OP_IS_NOT_NULL, FILTER_OP_IS_NULL,
        FILTER_OP_LT,
    };
    use qdb_core::col_type::ColumnTypeTag;

    /// Physical type ordinal for Int64 in the `_pm` format.
    const PHYS_INT64: u8 = 2;

    /// Physical type ordinal for FixedLenByteArray in the `_pm` format.
    const PHYS_FLBA: u8 = 7;

    /// Build a `_pm` file with one Int64 column and configurable stats per row group.
    /// Each entry: `(num_rows, null_count, min, max, has_stats)`.
    fn build_long_pm(
        row_groups: &[(u64, u64, i64, i64, bool)],
    ) -> TestParquetResult<(Vec<u8>, u64)> {
        let mut writer = ParquetMetaWriter::new();
        writer
            .designated_timestamp(-1)
            .add_column(
                "val",
                0,
                ColumnTypeTag::Long as i32,
                ColumnFlags::new().with_repetition(FieldRepetition::Optional),
                0,
                PHYS_INT64,
                0,
                1, // max_def_level = 1 for optional
            )
            .parquet_footer(0, 0);

        for &(num_rows, null_count, min, max, has_stats) in row_groups {
            let mut rg = RowGroupBlockBuilder::new(1);
            rg.set_num_rows(num_rows);

            let mut chunk = ColumnChunkRaw::zeroed();
            chunk.codec = Codec::Uncompressed as u8;
            chunk.num_values = num_rows;
            chunk.null_count = null_count;

            if has_stats {
                chunk.stat_flags = StatFlags::new()
                    .with_min(true, true)
                    .with_max(true, true)
                    .with_null_count()
                    .0;
                chunk.stat_sizes = encode_stat_sizes(8, 8);
                chunk.min_stat = min as u64;
                chunk.max_stat = max as u64;
            } else {
                chunk.stat_flags = StatFlags::new().with_null_count().0;
            }
            rg.set_column_chunk(0, chunk)?;

            writer.add_row_group(rg);
        }

        writer.finish()
    }

    /// Build a `_pm` file with one FLBA(16) UUID column and OOL stats.
    fn build_uuid_pm(
        row_groups: &[(u64, u64, [u8; 16], [u8; 16])],
    ) -> TestParquetResult<(Vec<u8>, u64)> {
        let mut writer = ParquetMetaWriter::new();
        writer
            .designated_timestamp(-1)
            .add_column(
                "val",
                0,
                ColumnTypeTag::Uuid as i32,
                ColumnFlags::new().with_repetition(FieldRepetition::Optional),
                16, // fixed_byte_len for FLBA(16)
                PHYS_FLBA,
                0,
                1, // max_def_level = 1 for optional
            )
            .parquet_footer(0, 0);

        for &(num_rows, null_count, ref min, ref max) in row_groups {
            let mut rg = RowGroupBlockBuilder::new(1);
            rg.set_num_rows(num_rows);

            let mut chunk = ColumnChunkRaw::zeroed();
            chunk.codec = Codec::Uncompressed as u8;
            chunk.num_values = num_rows;
            chunk.null_count = null_count;
            // OOL stats: is_min_inlined=false
            chunk.stat_flags = StatFlags::new()
                .with_min(false, true)
                .with_max(false, true)
                .with_null_count()
                .0;
            rg.set_column_chunk(0, chunk)?;
            rg.add_out_of_line_stat(0, true, min)?;
            rg.add_out_of_line_stat(0, false, max)?;

            writer.add_row_group(rg);
        }

        writer.finish()
    }

    fn make_filter(
        column_index: u32,
        count: u32,
        op: u8,
        ptr: u64,
        col_type: i32,
    ) -> ColumnFilterPacked {
        ColumnFilterPacked {
            col_idx_and_count: (column_index as u64)
                | (((count as u64) & 0x00FF_FFFF) << 32)
                | ((op as u64) << 56),
            ptr,
            column_type: col_type as u64,
        }
    }

    #[test]
    fn skip_is_null_when_no_nulls() -> TestParquetResult<()> {
        //                  (rows, nulls, min, max, has_stats)
        let (pm, fo) = build_long_pm(&[(100, 0, 10, 200, true)])?;
        let reader = ParquetMetaReader::from_file_size(&pm, fo)?;

        let filter = make_filter(0, 0, FILTER_OP_IS_NULL, 0, ColumnTypeTag::Long as i32);
        assert!(reader.can_skip_row_group(0, &[filter], 0)?);
        Ok(())
    }

    #[test]
    fn no_skip_is_null_when_nulls_present() -> TestParquetResult<()> {
        let (pm, fo) = build_long_pm(&[(100, 5, 10, 200, true)])?;
        let reader = ParquetMetaReader::from_file_size(&pm, fo)?;

        let filter = make_filter(0, 0, FILTER_OP_IS_NULL, 0, ColumnTypeTag::Long as i32);
        assert!(!reader.can_skip_row_group(0, &[filter], 0)?);
        Ok(())
    }

    #[test]
    fn skip_is_not_null_when_all_nulls() -> TestParquetResult<()> {
        let (pm, fo) = build_long_pm(&[(100, 100, 0, 0, true)])?;
        let reader = ParquetMetaReader::from_file_size(&pm, fo)?;

        let filter = make_filter(0, 0, FILTER_OP_IS_NOT_NULL, 0, ColumnTypeTag::Long as i32);
        assert!(reader.can_skip_row_group(0, &[filter], 0)?);
        Ok(())
    }

    #[test]
    fn no_skip_is_not_null_when_some_non_nulls() -> TestParquetResult<()> {
        let (pm, fo) = build_long_pm(&[(100, 50, 10, 200, true)])?;
        let reader = ParquetMetaReader::from_file_size(&pm, fo)?;

        let filter = make_filter(0, 0, FILTER_OP_IS_NOT_NULL, 0, ColumnTypeTag::Long as i32);
        assert!(!reader.can_skip_row_group(0, &[filter], 0)?);
        Ok(())
    }

    #[test]
    fn skip_eq_outside_min_max() -> TestParquetResult<()> {
        // Range [100, 200], search for 300.
        let (pm, fo) = build_long_pm(&[(1000, 0, 100, 200, true)])?;
        let reader = ParquetMetaReader::from_file_size(&pm, fo)?;

        let value: i64 = 300;
        let filter = make_filter(
            0,
            1,
            FILTER_OP_EQ,
            &value as *const i64 as u64,
            ColumnTypeTag::Long as i32,
        );
        let buf_end = unsafe { (&value as *const i64).add(1) } as u64;
        assert!(reader.can_skip_row_group(0, &[filter], buf_end)?);
        Ok(())
    }

    #[test]
    fn no_skip_eq_inside_min_max() -> TestParquetResult<()> {
        // Range [100, 200], search for 150.
        let (pm, fo) = build_long_pm(&[(1000, 0, 100, 200, true)])?;
        let reader = ParquetMetaReader::from_file_size(&pm, fo)?;

        let value: i64 = 150;
        let filter = make_filter(
            0,
            1,
            FILTER_OP_EQ,
            &value as *const i64 as u64,
            ColumnTypeTag::Long as i32,
        );
        let buf_end = unsafe { (&value as *const i64).add(1) } as u64;
        assert!(!reader.can_skip_row_group(0, &[filter], buf_end)?);
        Ok(())
    }

    #[test]
    fn skip_gt_above_max() -> TestParquetResult<()> {
        // Range [100, 200], GT 200 → all values <= 200 so skip.
        let (pm, fo) = build_long_pm(&[(1000, 0, 100, 200, true)])?;
        let reader = ParquetMetaReader::from_file_size(&pm, fo)?;

        let value: i64 = 200;
        let filter = make_filter(
            0,
            1,
            FILTER_OP_GT,
            &value as *const i64 as u64,
            ColumnTypeTag::Long as i32,
        );
        let buf_end = unsafe { (&value as *const i64).add(1) } as u64;
        assert!(reader.can_skip_row_group(0, &[filter], buf_end)?);
        Ok(())
    }

    #[test]
    fn skip_lt_below_min() -> TestParquetResult<()> {
        // Range [100, 200], LT 100 → all values >= 100 so skip.
        let (pm, fo) = build_long_pm(&[(1000, 0, 100, 200, true)])?;
        let reader = ParquetMetaReader::from_file_size(&pm, fo)?;

        let value: i64 = 100;
        let filter = make_filter(
            0,
            1,
            FILTER_OP_LT,
            &value as *const i64 as u64,
            ColumnTypeTag::Long as i32,
        );
        let buf_end = unsafe { (&value as *const i64).add(1) } as u64;
        assert!(reader.can_skip_row_group(0, &[filter], buf_end)?);
        Ok(())
    }

    #[test]
    fn no_skip_without_stats() -> TestParquetResult<()> {
        let (pm, fo) = build_long_pm(&[(1000, 0, 0, 0, false)])?;
        let reader = ParquetMetaReader::from_file_size(&pm, fo)?;

        let value: i64 = 9999;
        let filter = make_filter(
            0,
            1,
            FILTER_OP_EQ,
            &value as *const i64 as u64,
            ColumnTypeTag::Long as i32,
        );
        let buf_end = unsafe { (&value as *const i64).add(1) } as u64;
        // No stats → conservative, cannot skip.
        assert!(!reader.can_skip_row_group(0, &[filter], buf_end)?);
        Ok(())
    }

    #[test]
    fn skip_rg_index_out_of_range() {
        let (pm, fo) = build_long_pm(&[(100, 0, 10, 200, true)]).unwrap();
        let reader = ParquetMetaReader::from_file_size(&pm, fo).unwrap();

        let err = reader.can_skip_row_group(5, &[], 0);
        assert!(err.is_err());
        assert!(err
            .unwrap_err()
            .to_string()
            .contains("row group index 5 out of range"));
    }

    #[test]
    fn skip_column_index_out_of_range_continues() -> TestParquetResult<()> {
        let (pm, fo) = build_long_pm(&[(100, 0, 10, 200, true)])?;
        let reader = ParquetMetaReader::from_file_size(&pm, fo)?;

        // Column index 99 doesn't exist → filter is ignored, no skip.
        let filter = make_filter(99, 0, FILTER_OP_IS_NULL, 0, ColumnTypeTag::Long as i32);
        assert!(!reader.can_skip_row_group(0, &[filter], 0)?);
        Ok(())
    }

    #[test]
    fn skip_empty_filters() -> TestParquetResult<()> {
        let (pm, fo) = build_long_pm(&[(100, 0, 10, 200, true)])?;
        let reader = ParquetMetaReader::from_file_size(&pm, fo)?;

        assert!(!reader.can_skip_row_group(0, &[], 0)?);
        Ok(())
    }

    #[test]
    fn skip_eq_outside_uuid_ool_stats() -> TestParquetResult<()> {
        // UUID range [0x11..11, 0x33..33], search for 0xFF..FF → outside range.
        let min = [0x11u8; 16];
        let max = [0x33u8; 16];
        let (pm, fo) = build_uuid_pm(&[(100, 0, min, max)])?;
        let reader = ParquetMetaReader::from_file_size(&pm, fo)?;

        // Filter value: 0xFF..FF (16 bytes), outside the range.
        let filter_value = [0xFFu8; 16];
        let filter = make_filter(
            0,
            1,
            FILTER_OP_EQ,
            filter_value.as_ptr() as u64,
            ColumnTypeTag::Uuid as i32,
        );
        let buf_end = unsafe { filter_value.as_ptr().add(16) } as u64;
        assert!(
            reader.can_skip_row_group(0, &[filter], buf_end)?,
            "should skip row group when filter value is outside OOL stat range"
        );
        Ok(())
    }
}
