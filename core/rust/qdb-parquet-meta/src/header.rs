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

//! File header, column descriptors, and sorting columns.

use crate::error::ParquetMetaErrorKind;
use crate::error::ParquetMetaResult;
use crate::parquet_meta_err;
use crate::types::{ColumnFlags, HeaderFeatureFlags, COLUMN_DESCRIPTOR_SIZE, HEADER_FIXED_SIZE};

// ── On-disk column descriptor (32 bytes) ───────────────────────────────

/// On-disk layout of a column descriptor (32 bytes).
///
/// The column name is stored externally as a length-prefixed string at
/// `name_offset` points to `[utf8 bytes][padding to 4-byte alignment]`.
#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub struct ColumnDescriptorRaw {
    pub name_offset: u64,
    pub id: i32,
    pub col_type: i32,
    pub flags: i32,
    pub fixed_byte_len: i32,
    pub name_length: u32,
    pub physical_type: u8,
    pub max_rep_level: u8,
    pub max_def_level: u8,
    pub _reserved: u8,
}

const _: () = assert!(size_of::<ColumnDescriptorRaw>() == COLUMN_DESCRIPTOR_SIZE);

impl ColumnDescriptorRaw {
    pub const fn flags(&self) -> ColumnFlags {
        ColumnFlags(self.flags)
    }
}

// ── On-disk header fixed portion (32 bytes) ────────────────────────────

/// On-disk layout of the fixed portion of the file header (32 bytes).
///
/// All u64 fields are naturally 8-byte aligned.
#[derive(Debug, Copy, Clone)]
pub struct FileHeaderRaw {
    pub parquet_meta_file_size: u64,
    pub feature_flags: HeaderFeatureFlags,
    pub designated_timestamp: i32,
    pub sorting_column_count: u32,
    pub column_count: u32,
}

// ── FileHeader (zero-copy reader) ──────────────────────────────────────

/// Zero-copy reader over the file header region of a `_pm` file.
pub struct FileHeader<'a> {
    raw: FileHeaderRaw,
    data: &'a [u8],
    /// Bloom filter column indices (raw bytes of u32 array), if BLOOM_FILTERS bit is set.
    bloom_filter_columns: Option<&'a [u8]>,
    /// Partition squash tracker, if SQUASH_TRACKER bit is set.
    squash_tracker: Option<i64>,
}

impl<'a> FileHeader<'a> {
    /// Creates a `FileHeader` reader over the given byte slice.
    ///
    /// The slice must start at byte 0 of the `_pm` file and be large enough
    /// to contain the full header (fixed fields + descriptors + sorting columns
    /// + name strings + feature sections).
    pub fn new(data: &'a [u8]) -> ParquetMetaResult<Self> {
        if data.len() < HEADER_FIXED_SIZE {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "file too small for header"
            ));
        }

        // Parse fixed header fields individually (feature_flags at offset 8
        // would not be 8-byte aligned in a repr(C) struct-pointer cast given
        // the leading u64, so we read by byte slice).
        // Unwraps: data.len() >= HEADER_FIXED_SIZE (32) checked above, so all
        // fixed-width slices are exactly the right length.
        let raw = FileHeaderRaw {
            parquet_meta_file_size: u64::from_le_bytes(data[0..8].try_into().unwrap()),
            feature_flags: HeaderFeatureFlags::from_le_bytes(data[8..16].try_into().unwrap()),
            designated_timestamp: i32::from_le_bytes(data[16..20].try_into().unwrap()),
            sorting_column_count: u32::from_le_bytes(data[20..24].try_into().unwrap()),
            column_count: u32::from_le_bytes(data[24..28].try_into().unwrap()),
        };

        let unknown_required = raw.feature_flags.unknown_required(0);
        if unknown_required != 0 {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::UnsupportedFeature {
                    flags: unknown_required
                }
            ));
        }

        let min_size = Self::min_size(raw.column_count, raw.sorting_column_count)?;
        if data.len() < min_size {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "file too small for {} columns and {} sorting columns",
                raw.column_count,
                raw.sorting_column_count
            ));
        }

        // Validate bit dependencies.
        raw.feature_flags.validate_bit_dependencies()?;

        if raw.feature_flags.has_sorting_is_dts_asc() && raw.designated_timestamp < 0 {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "SORTING_IS_DTS_ASC requires designated_timestamp >= 0, got {}",
                raw.designated_timestamp
            ));
        }

        // Validate that name strings are in bounds.
        let names_end = Self::compute_names_area_end(data, raw.column_count)?;

        // Parse header feature sections (after name strings, in bit order).
        let mut cursor = names_end;

        // Bit 0: BLOOM_FILTERS header section.
        let bloom_filter_columns = if raw.feature_flags.has_bloom_filters() {
            // Read bloom_col_count: u32.
            if cursor + 4 > data.len() {
                return Err(parquet_meta_err!(
                    ParquetMetaErrorKind::Truncated,
                    "bloom filter header section: missing bloom_col_count"
                ));
            }
            // Unwrap: cursor + 4 <= data.len() checked above.
            let bloom_col_count = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap());
            cursor += 4;

            if bloom_col_count == 0 {
                return Err(parquet_meta_err!(
                    ParquetMetaErrorKind::InvalidValue,
                    "bloom filter header section: bloom_col_count is 0 but bit 0 is set"
                ));
            }
            if bloom_col_count > raw.column_count {
                return Err(parquet_meta_err!(
                    ParquetMetaErrorKind::InvalidValue,
                    "bloom filter header section: bloom_col_count {} exceeds column_count {}",
                    bloom_col_count,
                    raw.column_count
                ));
            }

            let indices_bytes = (bloom_col_count as usize) * 4;
            if cursor + indices_bytes > data.len() {
                return Err(parquet_meta_err!(
                    ParquetMetaErrorKind::Truncated,
                    "bloom filter header section: indices truncated"
                ));
            }
            let indices_slice = &data[cursor..cursor + indices_bytes];
            cursor += indices_bytes;

            // Validate: each index < column_count, sorted ascending, unique.
            let mut prev: Option<u32> = None;
            for i in 0..bloom_col_count as usize {
                let off = i * 4;
                // Unwrap: off + 4 <= indices_bytes, indices_slice length checked above.
                let idx = u32::from_le_bytes(indices_slice[off..off + 4].try_into().unwrap());
                if idx >= raw.column_count {
                    return Err(parquet_meta_err!(
                        ParquetMetaErrorKind::InvalidValue,
                        "bloom filter column index {} >= column_count {}",
                        idx,
                        raw.column_count
                    ));
                }
                if let Some(p) = prev {
                    if idx <= p {
                        return Err(parquet_meta_err!(
                            ParquetMetaErrorKind::InvalidValue,
                            "bloom filter column indices not sorted ascending and unique: {} after {}",
                            idx,
                            p
                        ));
                    }
                }
                prev = Some(idx);
            }

            Some(indices_slice)
        } else {
            None
        };

        // Bit 3: SQUASH_TRACKER header section (single i64).
        let squash_tracker = if raw.feature_flags.has_squash_tracker() {
            if cursor + 8 > data.len() {
                return Err(parquet_meta_err!(
                    ParquetMetaErrorKind::Truncated,
                    "squash tracker header section: missing i64 payload"
                ));
            }
            // Unwrap: cursor + 8 <= data.len() checked above.
            let value = i64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
            Some(value)
        } else {
            None
        };

        Ok(Self {
            raw,
            data,
            bloom_filter_columns,
            squash_tracker,
        })
    }

    /// Minimum byte size required for the header with the given column counts
    /// (not including name string data or feature sections).
    fn min_size(column_count: u32, sorting_column_count: u32) -> ParquetMetaResult<usize> {
        HEADER_FIXED_SIZE
            .checked_add(
                (column_count as usize)
                    .checked_mul(COLUMN_DESCRIPTOR_SIZE)
                    .ok_or_else(|| {
                        parquet_meta_err!(ParquetMetaErrorKind::Truncated, "column_count overflow")
                    })?,
            )
            .and_then(|s| s.checked_add((sorting_column_count as usize).saturating_mul(4)))
            .ok_or_else(|| {
                parquet_meta_err!(ParquetMetaErrorKind::Truncated, "header size overflow")
            })
    }

    /// Computes the byte offset past the last name string entry.
    fn compute_names_area_end(data: &[u8], column_count: u32) -> ParquetMetaResult<usize> {
        let mut end = HEADER_FIXED_SIZE + (column_count as usize) * COLUMN_DESCRIPTOR_SIZE;
        // We don't know sorting_column_count here from just column_count,
        // but name_offsets are absolute, so we just find the max end.
        for i in 0..column_count as usize {
            let offset = HEADER_FIXED_SIZE + i * COLUMN_DESCRIPTOR_SIZE;
            let ptr = data[offset..].as_ptr();
            debug_assert_eq!(ptr.align_offset(align_of::<ColumnDescriptorRaw>()), 0);
            let desc = unsafe { &*(ptr as *const ColumnDescriptorRaw) };
            let entry_size = desc.name_length as usize;
            let entry_end = (desc.name_offset as usize)
                .checked_add(entry_size)
                .ok_or_else(|| {
                    parquet_meta_err!(
                        ParquetMetaErrorKind::Truncated,
                        "name entry overflow at column {}",
                        i
                    )
                })?;
            end = end.max(entry_end);
        }
        Ok(end)
    }

    pub fn feature_flags(&self) -> HeaderFeatureFlags {
        self.raw.feature_flags
    }

    /// Returns the total committed `_pm` file size stored in the header.
    /// The writer patches this last, so observing a value here is the MVCC
    /// commit signal for the latest snapshot.
    pub fn parquet_meta_file_size(&self) -> u64 {
        self.raw.parquet_meta_file_size
    }

    /// Index of the designated timestamp column, or `None` if not set (-1).
    pub fn designated_timestamp(&self) -> Option<u32> {
        let v = self.raw.designated_timestamp;
        if v < 0 {
            None
        } else {
            Some(v as u32)
        }
    }

    /// Partition squash tracker, or `None` when the SQUASH_TRACKER feature bit
    /// is not set. Consumed by the enterprise build; OSS does not read it.
    pub fn squash_tracker(&self) -> Option<i64> {
        self.squash_tracker
    }

    /// Returns the effective sorting column count. When SORTING_IS_DTS_ASC is
    /// set, this returns 1 even though the on-disk count is 0.
    pub fn sorting_column_count(&self) -> u32 {
        if self.raw.feature_flags.has_sorting_is_dts_asc() {
            1
        } else {
            self.raw.sorting_column_count
        }
    }

    pub fn column_count(&self) -> u32 {
        self.raw.column_count
    }

    /// Returns a zero-copy reference to the column descriptor at `index`.
    pub fn column_descriptor(&self, index: usize) -> ParquetMetaResult<&'a ColumnDescriptorRaw> {
        if index >= self.raw.column_count as usize {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "column descriptor index {} out of range [0, {})",
                index,
                self.raw.column_count
            ));
        }
        let offset = HEADER_FIXED_SIZE + index * COLUMN_DESCRIPTOR_SIZE;
        let ptr = self.data[offset..].as_ptr();
        // Safety: ColumnDescriptorRaw is #[repr(C)] with 8-byte natural alignment.
        // offset = 24 + index * 32, which is always 8-byte aligned.
        debug_assert_eq!(ptr.align_offset(align_of::<ColumnDescriptorRaw>()), 0);
        Ok(unsafe { &*(ptr as *const ColumnDescriptorRaw) })
    }

    /// Returns the UTF-8 column name for the given descriptor.
    /// Reads `name_length` bytes at `name_offset`.
    pub fn column_name(&self, desc: &ColumnDescriptorRaw) -> ParquetMetaResult<&'a str> {
        let start = desc.name_offset as usize;
        let len = desc.name_length as usize;
        let end = start.checked_add(len).ok_or_else(|| {
            parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "column name offset {}+{} overflows",
                start,
                len
            )
        })?;
        if end > self.data.len() {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "column name at offset {} length {} exceeds file size {}",
                start,
                len,
                self.data.len()
            ));
        }
        std::str::from_utf8(&self.data[start..end]).map_err(|e| {
            parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "invalid UTF-8 in column name at offset {}: {}",
                start,
                e
            )
        })
    }

    /// Returns a zero-copy slice of all sorting column indices.
    ///
    /// The required bounds were validated in [`FileHeader::new`].
    pub fn sorting_columns(&self) -> &'a [u32] {
        if self.raw.sorting_column_count == 0 {
            return &[];
        }
        // These multiplications cannot overflow: min_size() in new() already
        // verified them with checked arithmetic and validated data.len() >= result.
        let offset = HEADER_FIXED_SIZE + (self.raw.column_count as usize) * COLUMN_DESCRIPTOR_SIZE;
        let end = offset + (self.raw.sorting_column_count as usize) * 4;
        debug_assert!(end <= self.data.len());
        let ptr = self.data[offset..end].as_ptr() as *const u32;
        // Safety: u32 requires 4-byte alignment. offset = 24 + n*32 is always 4-byte aligned.
        // Bounds checked: min_size() validated offset + sorting_column_count * 4 <= data.len().
        debug_assert_eq!(ptr.align_offset(align_of::<u32>()), 0);
        unsafe { std::slice::from_raw_parts(ptr, self.raw.sorting_column_count as usize) }
    }

    /// Returns the effective sorting column index at position `index`.
    /// When SORTING_IS_DTS_ASC is set, index 0 returns the designated timestamp.
    pub fn sorting_column(&self, index: usize) -> ParquetMetaResult<u32> {
        if self.raw.feature_flags.has_sorting_is_dts_asc() {
            if index == 0 {
                return Ok(self.raw.designated_timestamp as u32);
            }
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "sorting column index {} out of range [0, 1)",
                index
            ));
        }
        let cols = self.sorting_columns();
        if index >= cols.len() {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "sorting column index {} out of range [0, {})",
                index,
                cols.len()
            ));
        }
        Ok(cols[index])
    }

    /// Byte offset past the last sorting column entry (start of name strings area).
    ///
    /// Safe: min_size() validated this arithmetic in [`FileHeader::new`].
    pub fn sorting_columns_end_offset(&self) -> usize {
        HEADER_FIXED_SIZE
            + (self.raw.column_count as usize) * COLUMN_DESCRIPTOR_SIZE
            + (self.raw.sorting_column_count as usize) * 4
    }

    /// Number of columns with bloom filters. Returns 0 if the bloom filter
    /// feature is not enabled.
    pub fn bloom_filter_column_count(&self) -> u32 {
        match self.bloom_filter_columns {
            Some(slice) => (slice.len() / 4) as u32,
            None => 0,
        }
    }

    /// Returns the column index at position `pos` in the bloom filter column list.
    pub fn bloom_filter_column(&self, pos: usize) -> ParquetMetaResult<u32> {
        let slice = self.bloom_filter_columns.ok_or_else(|| {
            parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "no bloom filter columns"
            )
        })?;
        let off = pos * 4;
        if off + 4 > slice.len() {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "bloom filter column position {} out of range [0, {})",
                pos,
                slice.len() / 4
            ));
        }
        // Unwrap: off + 4 <= slice.len() checked above.
        Ok(u32::from_le_bytes(slice[off..off + 4].try_into().unwrap()))
    }

    /// Binary search for `col_idx` in the sorted bloom filter column list.
    /// Returns the position if found.
    pub fn bloom_filter_position(&self, col_idx: u32) -> Option<usize> {
        let slice = self.bloom_filter_columns?;
        let count = slice.len() / 4;
        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let off = mid * 4;
            let idx = u32::from_le_bytes(slice[off..off + 4].try_into().ok()?);
            match idx.cmp(&col_idx) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(mid),
            }
        }
        None
    }
}

// ── FileHeaderBuilder ──────────────────────────────────────────────────

struct ColumnEntry {
    name: String,
    id: i32,
    col_type: i32,
    flags: ColumnFlags,
    fixed_byte_len: i32,
    physical_type: u8,
    max_rep_level: u8,
    max_def_level: u8,
}

/// Builds a `_pm` file header into a `Vec<u8>`.
pub struct FileHeaderBuilder {
    designated_timestamp: i32,
    columns: Vec<ColumnEntry>,
    sorting_columns: Vec<u32>,
    pub(crate) bloom_filter_columns: Vec<u32>,
    pub(crate) bloom_filters_external: bool,
    squash_tracker: Option<i64>,
}

impl FileHeaderBuilder {
    pub fn new(designated_timestamp: i32) -> Self {
        Self {
            designated_timestamp,
            columns: Vec::new(),
            sorting_columns: Vec::new(),
            bloom_filter_columns: Vec::new(),
            bloom_filters_external: false,
            squash_tracker: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_column(
        &mut self,
        name: &str,
        id: i32,
        col_type: i32,
        flags: ColumnFlags,
        fixed_byte_len: i32,
        physical_type: u8,
        max_rep_level: u8,
        max_def_level: u8,
    ) -> &mut Self {
        self.columns.push(ColumnEntry {
            name: name.to_owned(),
            id,
            col_type,
            flags,
            fixed_byte_len,
            physical_type,
            max_rep_level,
            max_def_level,
        });
        self
    }

    pub fn add_sorting_column(&mut self, index: u32) -> &mut Self {
        self.sorting_columns.push(index);
        self
    }

    /// Sets the bloom filter column indices. The list is sorted and deduped.
    pub fn set_bloom_filter_columns(&mut self, indices: &[u32]) -> &mut Self {
        self.bloom_filter_columns = indices.to_vec();
        self.bloom_filter_columns.sort_unstable();
        self.bloom_filter_columns.dedup();
        self
    }

    pub fn set_bloom_filters_external(&mut self, value: bool) -> &mut Self {
        self.bloom_filters_external = value;
        self
    }

    /// Sets the partition squash tracker value. Passing `-1` clears it (the
    /// flag bit and 8-byte payload are omitted on write).
    pub fn set_squash_tracker(&mut self, value: i64) -> &mut Self {
        self.squash_tracker = if value == -1 { None } else { Some(value) };
        self
    }

    /// Returns true when sorting can be encoded as the SORTING_IS_DTS_ASC flag
    /// instead of writing explicit sorting column entries.
    fn can_use_sorting_is_dts_asc(&self) -> bool {
        let dts = self.designated_timestamp;
        if dts < 0 {
            return false;
        }
        let dts_idx = dts as usize;
        if dts_idx >= self.columns.len() {
            return false;
        }
        self.sorting_columns.len() == 1
            && self.sorting_columns[0] == dts_idx as u32
            && !self.columns[dts_idx].flags.is_descending()
    }

    /// Serializes the header into `buf`. Returns the byte offset past the end
    /// of the header.
    pub fn write_to(&self, buf: &mut Vec<u8>) -> usize {
        let column_count = self.columns.len() as u32;
        let use_dts_asc = self.can_use_sorting_is_dts_asc();

        // Compute feature flags.
        let mut flags = HeaderFeatureFlags::new();
        if !self.bloom_filter_columns.is_empty() {
            flags = flags.with_bloom_filters();
            if self.bloom_filters_external {
                flags = flags.with_bloom_filters_external();
            }
        }
        let sorting_count;
        if use_dts_asc {
            flags = flags.with_sorting_is_dts_asc();
            sorting_count = 0u32;
        } else {
            sorting_count = self.sorting_columns.len() as u32;
        }
        if self.squash_tracker.is_some() {
            flags = flags.with_squash_tracker();
        }

        // Fixed header fields (32 bytes).
        // parquet_meta_file_size placeholder (patched last by the top-level writer once
        // the full file size is known — acts as the MVCC commit signal).
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&flags.0.to_le_bytes());
        buf.extend_from_slice(&self.designated_timestamp.to_le_bytes());
        buf.extend_from_slice(&sorting_count.to_le_bytes());
        buf.extend_from_slice(&column_count.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes()); // reserved (alignment padding)

        // Reserve space for column descriptors (backpatched later with name offsets).
        let descriptors_start = buf.len();
        let descriptors_bytes = self.columns.len() * COLUMN_DESCRIPTOR_SIZE;
        buf.resize(buf.len() + descriptors_bytes, 0);

        // Sorting columns (omitted when SORTING_IS_DTS_ASC is set).
        if !use_dts_asc {
            for &idx in &self.sorting_columns {
                buf.extend_from_slice(&idx.to_le_bytes());
            }
        }

        // Name strings area: each name is [utf8 bytes].
        // The length is stored in the column descriptor's name_length field.
        let mut name_offsets: Vec<u64> = Vec::with_capacity(self.columns.len());
        for col in &self.columns {
            let offset = buf.len() as u64;
            let name_bytes = col.name.as_bytes();
            buf.extend_from_slice(name_bytes);
            name_offsets.push(offset);
        }

        // Backpatch column descriptors.
        for (i, col) in self.columns.iter().enumerate() {
            let name_offset = name_offsets[i];
            let desc_offset = descriptors_start + i * COLUMN_DESCRIPTOR_SIZE;
            let desc = ColumnDescriptorRaw {
                name_offset,
                id: col.id,
                col_type: col.col_type,
                flags: col.flags.0,
                fixed_byte_len: col.fixed_byte_len,
                name_length: col.name.len() as u32,
                physical_type: col.physical_type,
                max_rep_level: col.max_rep_level,
                max_def_level: col.max_def_level,
                _reserved: 0,
            };
            // Safety: ColumnDescriptorRaw is #[repr(C)] and fully initialized.
            let bytes: &[u8; COLUMN_DESCRIPTOR_SIZE] = unsafe {
                &*(&desc as *const ColumnDescriptorRaw as *const [u8; COLUMN_DESCRIPTOR_SIZE])
            };
            buf[desc_offset..desc_offset + COLUMN_DESCRIPTOR_SIZE].copy_from_slice(bytes);
        }

        // Header feature sections (in bit order, after name strings).
        // Bit 0: BLOOM_FILTERS header section.
        if !self.bloom_filter_columns.is_empty() {
            let bloom_col_count = self.bloom_filter_columns.len() as u32;
            buf.extend_from_slice(&bloom_col_count.to_le_bytes());
            for &idx in &self.bloom_filter_columns {
                buf.extend_from_slice(&idx.to_le_bytes());
            }
        }

        // Bit 3: SQUASH_TRACKER header section (single i64).
        if let Some(value) = self.squash_tracker {
            buf.extend_from_slice(&value.to_le_bytes());
        }

        buf.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::FieldRepetition;

    #[test]
    fn descriptor_size_is_32() {
        assert_eq!(size_of::<ColumnDescriptorRaw>(), 32);
    }

    #[test]
    fn header_round_trip_empty() {
        let builder = FileHeaderBuilder::new(-1);
        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        let hdr = FileHeader::new(&buf).unwrap();
        assert_eq!(hdr.feature_flags(), HeaderFeatureFlags::new());
        assert_eq!(hdr.designated_timestamp(), None);
        assert_eq!(hdr.column_count(), 0);
        assert_eq!(hdr.sorting_column_count(), 0);
    }

    #[test]
    fn header_round_trip_with_columns() {
        let mut builder = FileHeaderBuilder::new(0);
        builder.add_column(
            "timestamp",
            0,
            8, // ColumnTypeTag::Timestamp
            ColumnFlags::new().with_repetition(FieldRepetition::Required),
            0,
            0,
            0,
            0,
        );
        builder.add_column(
            "value",
            1,
            10, // ColumnTypeTag::Double
            ColumnFlags::new().with_repetition(FieldRepetition::Optional),
            0,
            0,
            0,
            0,
        );
        builder.add_sorting_column(0);

        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        let hdr = FileHeader::new(&buf).unwrap();
        assert_eq!(hdr.column_count(), 2);
        assert_eq!(hdr.designated_timestamp(), Some(0));
        assert_eq!(hdr.sorting_column_count(), 1);
        assert_eq!(hdr.sorting_column(0).unwrap(), 0);

        // Column 0.
        let desc0 = hdr.column_descriptor(0).unwrap();
        assert_eq!(desc0.id, 0);
        assert_eq!(desc0.col_type, 8);
        assert_eq!(hdr.column_name(desc0).unwrap(), "timestamp");
        assert_eq!(
            desc0.flags().repetition().unwrap(),
            FieldRepetition::Required
        );

        // Column 1.
        let desc1 = hdr.column_descriptor(1).unwrap();
        assert_eq!(desc1.id, 1);
        assert_eq!(desc1.col_type, 10);
        assert_eq!(hdr.column_name(desc1).unwrap(), "value");
        assert_eq!(
            desc1.flags().repetition().unwrap(),
            FieldRepetition::Optional
        );
    }

    #[test]
    fn header_many_columns() {
        let mut builder = FileHeaderBuilder::new(-1);
        for i in 0..10 {
            builder.add_column(
                &format!("col_{i}"),
                i,
                5, // Int
                ColumnFlags::new(),
                0,
                0,
                0,
                0,
            );
        }

        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        let hdr = FileHeader::new(&buf).unwrap();
        assert_eq!(hdr.column_count(), 10);
        for i in 0..10 {
            let desc = hdr.column_descriptor(i).unwrap();
            assert_eq!(desc.id, i as i32);
            assert_eq!(hdr.column_name(desc).unwrap(), format!("col_{i}"));
        }
    }

    #[test]
    fn header_truncated_errors() {
        assert!(FileHeader::new(&[0u8; 4]).is_err());
    }

    #[test]
    fn descriptor_out_of_range() {
        let builder = FileHeaderBuilder::new(-1);
        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        let hdr = FileHeader::new(&buf).unwrap();
        assert!(hdr.column_descriptor(0).is_err());
    }

    #[test]
    fn sorting_column_out_of_range() {
        let builder = FileHeaderBuilder::new(-1);
        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        let hdr = FileHeader::new(&buf).unwrap();
        assert!(hdr.sorting_column(0).is_err());
    }

    #[test]
    fn header_too_small_for_columns() {
        // Write a header claiming 10 columns but truncate the buffer.
        let mut buf = vec![0u8; HEADER_FIXED_SIZE];
        buf[0..8].copy_from_slice(&0u64.to_le_bytes()); // parquet_meta_file_size
        buf[8..16].copy_from_slice(&0u64.to_le_bytes()); // feature_flags
        buf[16..20].copy_from_slice(&(-1i32).to_le_bytes()); // designated_ts
        buf[20..24].copy_from_slice(&0u32.to_le_bytes()); // sorting count
        buf[24..28].copy_from_slice(&10u32.to_le_bytes()); // claim 10 columns
                                                           // Buffer is only 32 bytes but needs 32 + 10*32 = 352.
        assert!(FileHeader::new(&buf).is_err());
    }

    #[test]
    fn column_name_out_of_bounds() {
        let mut builder = FileHeaderBuilder::new(-1);
        builder.add_column("ok", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        // Corrupt name_offset of the first descriptor to point far beyond file.
        let desc_offset = HEADER_FIXED_SIZE;
        let bad_name_offset = 99999u64;
        buf[desc_offset..desc_offset + 8].copy_from_slice(&bad_name_offset.to_le_bytes());

        let hdr = FileHeader::new(&buf).unwrap();
        let desc = hdr.column_descriptor(0).unwrap();
        assert!(hdr.column_name(desc).is_err());
    }

    #[test]
    fn column_name_invalid_utf8() {
        let mut builder = FileHeaderBuilder::new(-1);
        builder.add_column("ab", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        let hdr = FileHeader::new(&buf).unwrap();
        let desc = hdr.column_descriptor(0).unwrap();
        let name_start = desc.name_offset as usize;
        // Overwrite string bytes with invalid UTF-8.
        buf[name_start] = 0xFF;
        buf[name_start + 1] = 0xFE;

        // Re-parse (the header struct references the same buffer).
        let hdr = FileHeader::new(&buf).unwrap();
        let desc = hdr.column_descriptor(0).unwrap();
        assert!(hdr.column_name(desc).is_err());
    }

    #[test]
    fn sorting_columns_end_offset_value() {
        let mut builder = FileHeaderBuilder::new(-1);
        builder.add_column("a", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        builder.add_column("b", 1, 6, ColumnFlags::new(), 0, 0, 0, 0);
        builder.add_sorting_column(0);
        builder.add_sorting_column(1);
        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        let hdr = FileHeader::new(&buf).unwrap();
        let expected = HEADER_FIXED_SIZE + 2 * COLUMN_DESCRIPTOR_SIZE + 2 * 4;
        assert_eq!(hdr.sorting_columns_end_offset(), expected);
    }

    #[test]
    fn unknown_optional_header_flags_ignored() {
        let mut builder = FileHeaderBuilder::new(-1);
        builder.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        // Set unknown optional flag bits (bits 3-31). Bits 0-2 are now defined
        // (bloom filters + sorting_is_dts_asc), so we skip them.
        let flags_with_unknown = 0x0000_0000_FFFF_FFF8u64;
        buf[4..12].copy_from_slice(&flags_with_unknown.to_le_bytes());

        // Reader should accept the file (ignores unknown optional flags).
        let hdr = FileHeader::new(&buf).unwrap();
        assert_eq!(hdr.column_count(), 1);
    }

    #[test]
    fn unknown_required_header_flags_rejected() {
        let mut builder = FileHeaderBuilder::new(-1);
        builder.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        // Set an unknown required flag (bit 32).
        let flags_with_required = 1u64 << 32;
        buf[4..12].copy_from_slice(&flags_with_required.to_le_bytes());

        assert!(FileHeader::new(&buf).is_err());
    }

    #[test]
    fn bloom_filter_columns_round_trip() {
        let mut builder = FileHeaderBuilder::new(-1);
        builder.add_column("a", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        builder.add_column("b", 1, 6, ColumnFlags::new(), 0, 0, 0, 0);
        builder.add_column("c", 2, 7, ColumnFlags::new(), 0, 0, 0, 0);
        builder.set_bloom_filter_columns(&[0, 2]);

        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        let hdr = FileHeader::new(&buf).unwrap();
        assert!(hdr.feature_flags().has_bloom_filters());
        assert!(!hdr.feature_flags().has_bloom_filters_external());
        assert_eq!(hdr.bloom_filter_column_count(), 2);
        assert_eq!(hdr.bloom_filter_column(0).unwrap(), 0);
        assert_eq!(hdr.bloom_filter_column(1).unwrap(), 2);
    }

    #[test]
    fn bloom_filter_position_binary_search() {
        let mut builder = FileHeaderBuilder::new(-1);
        for i in 0..5 {
            builder.add_column(&format!("c{i}"), i, 5, ColumnFlags::new(), 0, 0, 0, 0);
        }
        builder.set_bloom_filter_columns(&[1, 3]);

        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        let hdr = FileHeader::new(&buf).unwrap();
        assert_eq!(hdr.bloom_filter_position(0), None);
        assert_eq!(hdr.bloom_filter_position(1), Some(0));
        assert_eq!(hdr.bloom_filter_position(2), None);
        assert_eq!(hdr.bloom_filter_position(3), Some(1));
        assert_eq!(hdr.bloom_filter_position(4), None);
    }

    #[test]
    fn bloom_filter_bit1_without_bit0_rejected() {
        let mut builder = FileHeaderBuilder::new(-1);
        builder.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        // Manually set only BLOOM_FILTERS_EXTERNAL (bit 1) without BLOOM_FILTERS (bit 0).
        let flags = HeaderFeatureFlags::BLOOM_FILTERS_EXTERNAL_BIT;
        buf[8..16].copy_from_slice(&flags.to_le_bytes());

        assert!(FileHeader::new(&buf).is_err());
    }

    #[test]
    fn bloom_filter_index_exceeds_column_count_rejected() {
        let mut builder = FileHeaderBuilder::new(-1);
        builder.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        // Column index 5 exceeds column_count=1.
        builder.set_bloom_filter_columns(&[5]);

        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        assert!(FileHeader::new(&buf).is_err());
    }

    #[test]
    fn bloom_filter_not_sorted_rejected() {
        let mut builder = FileHeaderBuilder::new(-1);
        builder.add_column("a", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        builder.add_column("b", 1, 6, ColumnFlags::new(), 0, 0, 0, 0);
        builder.add_column("c", 2, 7, ColumnFlags::new(), 0, 0, 0, 0);
        // set_bloom_filter_columns sorts, so write manually to test unsorted.
        builder.bloom_filter_columns = vec![2, 0]; // not sorted

        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        assert!(FileHeader::new(&buf).is_err());
    }

    #[test]
    fn bloom_filter_zero_count_with_bit0_rejected() {
        let mut builder = FileHeaderBuilder::new(-1);
        builder.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        // Set bit 0 manually but provide zero bloom_col_count.
        let flags = HeaderFeatureFlags::BLOOM_FILTERS_BIT;
        buf[8..16].copy_from_slice(&flags.to_le_bytes());
        // After name strings, write bloom_col_count = 0.
        let names_end = buf.len();
        buf.extend_from_slice(&0u32.to_le_bytes()); // bloom_col_count = 0
                                                    // This would be at names_end, which is where the section starts.
                                                    // But the header was already built without the section. We need to
                                                    // inject it. Actually, the reader reads the count from after names.
                                                    // The existing buf has names at the end with no section bytes. Adding
                                                    // 4 bytes is enough since the reader parses from names_end.
        let _ = names_end; // suppress unused
                           // Re-parse: this should fail because bloom_col_count == 0 with bit 0 set.
        assert!(FileHeader::new(&buf).is_err());
    }

    #[test]
    fn no_bloom_filters_means_no_section() {
        let mut builder = FileHeaderBuilder::new(-1);
        builder.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        // Don't set any bloom filter columns.
        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        let hdr = FileHeader::new(&buf).unwrap();
        assert!(!hdr.feature_flags().has_bloom_filters());
        assert_eq!(hdr.bloom_filter_column_count(), 0);
    }

    // ── SQUASH_TRACKER tests ─────────────────────────────────────────

    #[test]
    fn squash_tracker_round_trip() {
        let mut builder = FileHeaderBuilder::new(-1);
        builder.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        builder.set_squash_tracker(42);

        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        let hdr = FileHeader::new(&buf).unwrap();
        assert!(hdr.feature_flags().has_squash_tracker());
        assert_eq!(hdr.squash_tracker(), Some(42));
    }

    #[test]
    fn squash_tracker_absent_when_unset() {
        let mut builder = FileHeaderBuilder::new(-1);
        builder.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        // Do not call set_squash_tracker at all.

        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        let hdr = FileHeader::new(&buf).unwrap();
        assert!(!hdr.feature_flags().has_squash_tracker());
        assert_eq!(hdr.squash_tracker(), None);
    }

    #[test]
    fn squash_tracker_neg_one_is_omitted() {
        // -1 is the "unset" sentinel; writer must omit the flag and payload.
        let mut builder_none = FileHeaderBuilder::new(-1);
        builder_none.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let mut buf_none = Vec::new();
        builder_none.write_to(&mut buf_none);

        let mut builder_neg_one = FileHeaderBuilder::new(-1);
        builder_neg_one.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        builder_neg_one.set_squash_tracker(-1);
        let mut buf_neg_one = Vec::new();
        builder_neg_one.write_to(&mut buf_neg_one);

        assert_eq!(buf_none, buf_neg_one);
        let hdr = FileHeader::new(&buf_neg_one).unwrap();
        assert!(!hdr.feature_flags().has_squash_tracker());
        assert_eq!(hdr.squash_tracker(), None);
    }

    #[test]
    fn squash_tracker_coexists_with_bloom_filters() {
        // Sequential navigation: BLOOM_FILTERS (bit 0) then SQUASH_TRACKER (bit 3).
        let mut builder = FileHeaderBuilder::new(-1);
        builder.add_column("a", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        builder.add_column("b", 1, 6, ColumnFlags::new(), 0, 0, 0, 0);
        builder.add_column("c", 2, 7, ColumnFlags::new(), 0, 0, 0, 0);
        builder.set_bloom_filter_columns(&[0, 2]);
        builder.set_squash_tracker(1234);

        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        let hdr = FileHeader::new(&buf).unwrap();
        assert!(hdr.feature_flags().has_bloom_filters());
        assert!(hdr.feature_flags().has_squash_tracker());
        assert_eq!(hdr.bloom_filter_column_count(), 2);
        assert_eq!(hdr.bloom_filter_column(0).unwrap(), 0);
        assert_eq!(hdr.bloom_filter_column(1).unwrap(), 2);
        assert_eq!(hdr.squash_tracker(), Some(1234));
    }

    #[test]
    fn squash_tracker_negative_values_round_trip() {
        // Any non-(-1) negative value must round-trip verbatim — -1 is the only sentinel.
        let mut builder = FileHeaderBuilder::new(-1);
        builder.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        builder.set_squash_tracker(i64::MIN);

        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        let hdr = FileHeader::new(&buf).unwrap();
        assert_eq!(hdr.squash_tracker(), Some(i64::MIN));
    }

    #[test]
    fn squash_tracker_truncated_payload_rejected() {
        // Build a header without squash_tracker, then set the flag manually
        // without appending the 8-byte payload. Reader must reject as Truncated.
        let mut builder = FileHeaderBuilder::new(-1);
        builder.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        let flags = HeaderFeatureFlags::SQUASH_TRACKER_BIT;
        buf[8..16].copy_from_slice(&flags.to_le_bytes());

        assert!(FileHeader::new(&buf).is_err());
    }

    // ── SORTING_IS_DTS_ASC tests ─────────────────────────────────────

    #[test]
    fn sorting_is_dts_asc_applied() {
        // dts=0, single sorting column [0], not descending → flag set.
        let mut builder = FileHeaderBuilder::new(0);
        builder.add_column(
            "ts",
            0,
            8,
            ColumnFlags::new().with_repetition(FieldRepetition::Required),
            0,
            0,
            0,
            0,
        );
        builder.add_column("val", 1, 10, ColumnFlags::new(), 0, 0, 0, 0);
        builder.add_sorting_column(0);

        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        let hdr = FileHeader::new(&buf).unwrap();
        assert!(hdr.feature_flags().has_sorting_is_dts_asc());
        // On-disk sorting_column_count is 0, but effective is 1.
        assert_eq!(hdr.raw.sorting_column_count, 0);
        assert_eq!(hdr.sorting_column_count(), 1);
        assert_eq!(hdr.sorting_column(0).unwrap(), 0);
        assert!(hdr.sorting_column(1).is_err());
    }

    #[test]
    fn sorting_is_dts_asc_not_applied_no_dts() {
        // dts=-1 → flag NOT set, sorting stored normally.
        let mut builder = FileHeaderBuilder::new(-1);
        builder.add_column("a", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        builder.add_sorting_column(0);

        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        let hdr = FileHeader::new(&buf).unwrap();
        assert!(!hdr.feature_flags().has_sorting_is_dts_asc());
        assert_eq!(hdr.sorting_column_count(), 1);
        assert_eq!(hdr.sorting_column(0).unwrap(), 0);
    }

    #[test]
    fn sorting_is_dts_asc_not_applied_multiple_sorting_cols() {
        // dts=0, sorting=[0,1] → flag NOT set.
        let mut builder = FileHeaderBuilder::new(0);
        builder.add_column("ts", 0, 8, ColumnFlags::new(), 0, 0, 0, 0);
        builder.add_column("key", 1, 12, ColumnFlags::new(), 0, 0, 0, 0);
        builder.add_sorting_column(0);
        builder.add_sorting_column(1);

        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        let hdr = FileHeader::new(&buf).unwrap();
        assert!(!hdr.feature_flags().has_sorting_is_dts_asc());
        assert_eq!(hdr.sorting_column_count(), 2);
        assert_eq!(hdr.sorting_column(0).unwrap(), 0);
        assert_eq!(hdr.sorting_column(1).unwrap(), 1);
    }

    #[test]
    fn sorting_is_dts_asc_not_applied_sorting_ne_dts() {
        // dts=0, sorting=[1] → flag NOT set.
        let mut builder = FileHeaderBuilder::new(0);
        builder.add_column("ts", 0, 8, ColumnFlags::new(), 0, 0, 0, 0);
        builder.add_column("key", 1, 12, ColumnFlags::new(), 0, 0, 0, 0);
        builder.add_sorting_column(1);

        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        let hdr = FileHeader::new(&buf).unwrap();
        assert!(!hdr.feature_flags().has_sorting_is_dts_asc());
        assert_eq!(hdr.sorting_column_count(), 1);
        assert_eq!(hdr.sorting_column(0).unwrap(), 1);
    }

    #[test]
    fn sorting_is_dts_asc_not_applied_descending() {
        // dts=0, sorting=[0] but DESCENDING → flag NOT set.
        let mut builder = FileHeaderBuilder::new(0);
        builder.add_column("ts", 0, 8, ColumnFlags::new().with_descending(), 0, 0, 0, 0);
        builder.add_sorting_column(0);

        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        let hdr = FileHeader::new(&buf).unwrap();
        assert!(!hdr.feature_flags().has_sorting_is_dts_asc());
        assert_eq!(hdr.sorting_column_count(), 1);
        assert_eq!(hdr.sorting_column(0).unwrap(), 0);
    }

    #[test]
    fn sorting_is_dts_asc_saves_4_bytes() {
        // Same file with and without the optimization — 4 byte difference.
        let mut with_opt = FileHeaderBuilder::new(0);
        with_opt.add_column("ts", 0, 8, ColumnFlags::new(), 0, 0, 0, 0);
        with_opt.add_sorting_column(0);
        let mut buf_with = Vec::new();
        with_opt.write_to(&mut buf_with);

        // Build the same file but force no optimization by using dts=-1
        // (sorting column still at index 0, but not matching dts).
        let mut without_opt = FileHeaderBuilder::new(-1);
        without_opt.add_column("ts", 0, 8, ColumnFlags::new(), 0, 0, 0, 0);
        without_opt.add_sorting_column(0);
        let mut buf_without = Vec::new();
        without_opt.write_to(&mut buf_without);

        assert_eq!(buf_with.len() + 4, buf_without.len());
    }

    #[test]
    fn sorting_is_dts_asc_with_negative_dts_rejected() {
        let mut builder = FileHeaderBuilder::new(-1);
        builder.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        // Manually set SORTING_IS_DTS_ASC flag while designated_timestamp remains -1.
        let flags = HeaderFeatureFlags::SORTING_IS_DTS_ASC_BIT;
        buf[8..16].copy_from_slice(&flags.to_le_bytes());

        assert!(FileHeader::new(&buf).is_err());
    }
}
