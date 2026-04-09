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

use crate::parquet::error::ParquetResult;
use crate::parquet_metadata::error::{parquet_meta_err, ParquetMetaErrorKind};
use crate::parquet_metadata::types::{
    ColumnFlags, HeaderFeatureFlags, COLUMN_DESCRIPTOR_SIZE, FILE_FORMAT_VERSION, HEADER_FIXED_SIZE,
};

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

// ── On-disk header fixed portion (24 bytes) ────────────────────────────

/// On-disk layout of the fixed portion of the file header (24 bytes).
///
/// Read field-by-field rather than via pointer cast because
/// `feature_flags: u64` at offset 4 is not 8-byte aligned.
#[derive(Debug, Copy, Clone)]
pub struct FileHeaderRaw {
    pub format_version: u32,
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
}

impl<'a> FileHeader<'a> {
    /// Creates a `FileHeader` reader over the given byte slice.
    ///
    /// The slice must start at byte 0 of the `_pm` file and be large enough
    /// to contain the full header (fixed fields + descriptors + sorting columns
    /// + name strings + feature sections).
    pub fn new(data: &'a [u8]) -> ParquetResult<Self> {
        if data.len() < HEADER_FIXED_SIZE {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "file too small for header"
            ));
        }

        // Parse fixed header fields individually (feature_flags at offset 4
        // is not 8-byte aligned, so we cannot use a repr(C) pointer cast).
        let raw = FileHeaderRaw {
            format_version: u32::from_le_bytes(data[0..4].try_into().unwrap()),
            feature_flags: HeaderFeatureFlags::from_le_bytes(data[4..12].try_into().unwrap()),
            designated_timestamp: i32::from_le_bytes(data[12..16].try_into().unwrap()),
            sorting_column_count: u32::from_le_bytes(data[16..20].try_into().unwrap()),
            column_count: u32::from_le_bytes(data[20..24].try_into().unwrap()),
        };

        if raw.format_version != FILE_FORMAT_VERSION {
            return Err(parquet_meta_err!(ParquetMetaErrorKind::VersionMismatch {
                found: raw.format_version,
                expected: FILE_FORMAT_VERSION,
            }));
        }

        let unknown_required = raw.feature_flags.unknown_required(0);
        if unknown_required != 0 {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::UnsupportedFeature { flags: unknown_required }
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

        // Validate that name strings are in bounds. There are currently no
        // header feature sections beyond the names area.
        let _ = Self::compute_names_area_end(data, raw.column_count)?;

        Ok(Self { raw, data })
    }

    /// Minimum byte size required for the header with the given column counts
    /// (not including name string data or feature sections).
    fn min_size(column_count: u32, sorting_column_count: u32) -> ParquetResult<usize> {
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
    fn compute_names_area_end(data: &[u8], column_count: u32) -> ParquetResult<usize> {
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

    pub fn format_version(&self) -> u32 {
        self.raw.format_version
    }

    pub fn feature_flags(&self) -> HeaderFeatureFlags {
        self.raw.feature_flags
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

    pub fn sorting_column_count(&self) -> u32 {
        self.raw.sorting_column_count
    }

    pub fn column_count(&self) -> u32 {
        self.raw.column_count
    }

    /// Returns a zero-copy reference to the column descriptor at `index`.
    pub fn column_descriptor(&self, index: usize) -> ParquetResult<&'a ColumnDescriptorRaw> {
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
    pub fn column_name(&self, desc: &ColumnDescriptorRaw) -> ParquetResult<&'a str> {
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

    /// Returns the sorting column index at position `index`.
    pub fn sorting_column(&self, index: usize) -> ParquetResult<u32> {
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
}

impl FileHeaderBuilder {
    pub fn new(designated_timestamp: i32) -> Self {
        Self {
            designated_timestamp,
            columns: Vec::new(),
            sorting_columns: Vec::new(),
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

    /// Serializes the header into `buf`. Returns the byte offset past the end
    /// of the header.
    pub fn write_to(&self, buf: &mut Vec<u8>) -> usize {
        let column_count = self.columns.len() as u32;
        let sorting_count = self.sorting_columns.len() as u32;

        // Fixed header fields (24 bytes, field-by-field for alignment safety).
        buf.extend_from_slice(&FILE_FORMAT_VERSION.to_le_bytes());
        buf.extend_from_slice(&HeaderFeatureFlags::new().0.to_le_bytes());
        buf.extend_from_slice(&self.designated_timestamp.to_le_bytes());
        buf.extend_from_slice(&sorting_count.to_le_bytes());
        buf.extend_from_slice(&column_count.to_le_bytes());

        // Reserve space for column descriptors (backpatched later with name offsets).
        let descriptors_start = buf.len();
        let descriptors_bytes = self.columns.len() * COLUMN_DESCRIPTOR_SIZE;
        buf.resize(buf.len() + descriptors_bytes, 0);

        // Sorting columns.
        for &idx in &self.sorting_columns {
            buf.extend_from_slice(&idx.to_le_bytes());
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

        buf.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet_metadata::types::FieldRepetition;

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
        assert_eq!(hdr.format_version(), FILE_FORMAT_VERSION);
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
    fn header_bad_version() {
        let mut buf = vec![0u8; HEADER_FIXED_SIZE];
        // Write version 99.
        buf[0..4].copy_from_slice(&99u32.to_le_bytes());
        assert!(FileHeader::new(&buf).is_err());
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
        buf[0..4].copy_from_slice(&FILE_FORMAT_VERSION.to_le_bytes());
        buf[4..12].copy_from_slice(&0u64.to_le_bytes()); // feature_flags
        buf[12..16].copy_from_slice(&(-1i32).to_le_bytes()); // designated_ts
        buf[16..20].copy_from_slice(&0u32.to_le_bytes()); // sorting count
        buf[20..24].copy_from_slice(&10u32.to_le_bytes()); // claim 10 columns
                                                           // Buffer is only 24 bytes but needs 24 + 10*32 = 344.
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

        // Set unknown optional flag bits (bits 1-31).
        let flags_with_unknown = 0x0000_0000_FFFF_FFFEu64;
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
}
