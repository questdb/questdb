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
use crate::parquet_metadata::error::{qdbp_err, QdbpErrorKind};
use crate::parquet_metadata::types::{
    ColumnFlags, COLUMN_DESCRIPTOR_SIZE, FILE_FORMAT_VERSION, HEADER_FIXED_SIZE,
};

// ── On-disk column descriptor (32 bytes) ───────────────────────────────

/// On-disk layout of a column descriptor (32 bytes).
#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub struct ColumnDescriptorRaw {
    pub top: u64,
    pub name_offset: u64,
    pub name_length: u32,
    pub id: i32,
    pub col_type: i32,
    pub flags: i32,
}

const _: () = assert!(size_of::<ColumnDescriptorRaw>() == COLUMN_DESCRIPTOR_SIZE);

impl ColumnDescriptorRaw {
    pub const fn flags(&self) -> ColumnFlags {
        ColumnFlags(self.flags)
    }
}

// ── On-disk header fixed portion (16 bytes) ─────────────────────────────

/// On-disk layout of the fixed portion of the file header (16 bytes).
#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub struct FileHeaderRaw {
    pub format_version: u32,
    pub designated_timestamp: i32,
    pub sorting_column_count: u32,
    pub column_count: u32,
}

const _: () = assert!(size_of::<FileHeaderRaw>() == HEADER_FIXED_SIZE);

// ── FileHeader (zero-copy reader) ──────────────────────────────────────

/// Zero-copy reader over the file header region of a `.qdbp` file.
pub struct FileHeader<'a> {
    raw: &'a FileHeaderRaw,
    data: &'a [u8],
}

impl<'a> FileHeader<'a> {
    /// Creates a `FileHeader` reader over the given byte slice.
    ///
    /// The slice must start at byte 0 of the `.qdbp` file and be large enough
    /// to contain the full header (fixed fields + descriptors + sorting columns).
    pub fn new(data: &'a [u8]) -> ParquetResult<Self> {
        if data.len() < HEADER_FIXED_SIZE {
            return Err(qdbp_err!(
                QdbpErrorKind::Truncated,
                "file too small for header"
            ));
        }
        let ptr = data.as_ptr() as *const FileHeaderRaw;
        // Safety: FileHeaderRaw is #[repr(C)] with 4-byte max alignment.
        // The slice starts at file offset 0 (page-aligned from mmap).
        debug_assert_eq!(ptr.align_offset(align_of::<FileHeaderRaw>()), 0);
        let raw = unsafe { &*ptr };

        if raw.format_version != FILE_FORMAT_VERSION {
            return Err(qdbp_err!(QdbpErrorKind::VersionMismatch {
                found: raw.format_version,
                expected: FILE_FORMAT_VERSION,
            }));
        }

        let min_size = Self::min_size(raw.column_count, raw.sorting_column_count)?;
        if data.len() < min_size {
            return Err(qdbp_err!(
                QdbpErrorKind::Truncated,
                "file too small for {} columns and {} sorting columns",
                raw.column_count,
                raw.sorting_column_count
            ));
        }
        Ok(Self { raw, data })
    }

    /// Minimum byte size required for the header with the given column counts
    /// (not including name string data).
    fn min_size(column_count: u32, sorting_column_count: u32) -> ParquetResult<usize> {
        HEADER_FIXED_SIZE
            .checked_add(
                (column_count as usize)
                    .checked_mul(COLUMN_DESCRIPTOR_SIZE)
                    .ok_or_else(|| qdbp_err!(QdbpErrorKind::Truncated, "column_count overflow"))?,
            )
            .and_then(|s| s.checked_add((sorting_column_count as usize).saturating_mul(4)))
            .ok_or_else(|| qdbp_err!(QdbpErrorKind::Truncated, "header size overflow"))
    }

    pub fn format_version(&self) -> u32 {
        self.raw.format_version
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
            return Err(qdbp_err!(
                QdbpErrorKind::InvalidValue,
                "column descriptor index {} out of range [0, {})",
                index,
                self.raw.column_count
            ));
        }
        let offset = HEADER_FIXED_SIZE + index * COLUMN_DESCRIPTOR_SIZE;
        let ptr = self.data[offset..].as_ptr();
        // Safety: ColumnDescriptorRaw is #[repr(C)] with 8-byte natural alignment.
        // offset = 16 + index * 32, which is always 8-byte aligned.
        debug_assert_eq!(ptr.align_offset(align_of::<ColumnDescriptorRaw>()), 0);
        Ok(unsafe { &*(ptr as *const ColumnDescriptorRaw) })
    }

    /// Returns the UTF-8 column name for the given descriptor.
    pub fn column_name(&self, desc: &ColumnDescriptorRaw) -> ParquetResult<&'a str> {
        let start = desc.name_offset as usize;
        let len = desc.name_length as usize;
        let end = start.checked_add(len).ok_or_else(|| {
            qdbp_err!(
                QdbpErrorKind::Truncated,
                "column name offset {}+{} overflows",
                start,
                len
            )
        })?;
        if end > self.data.len() {
            return Err(qdbp_err!(
                QdbpErrorKind::Truncated,
                "column name offset {}+{} exceeds file size {}",
                start,
                len,
                self.data.len()
            ));
        }
        std::str::from_utf8(&self.data[start..start + len]).map_err(|e| {
            qdbp_err!(
                QdbpErrorKind::InvalidValue,
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
        // Safety: u32 requires 4-byte alignment. offset = 16 + n*32 is always 4-byte aligned.
        // Bounds checked: min_size() validated offset + sorting_column_count * 4 <= data.len().
        debug_assert_eq!(ptr.align_offset(align_of::<u32>()), 0);
        unsafe { std::slice::from_raw_parts(ptr, self.raw.sorting_column_count as usize) }
    }

    /// Returns the sorting column index at position `index`.
    pub fn sorting_column(&self, index: usize) -> ParquetResult<u32> {
        let cols = self.sorting_columns();
        if index >= cols.len() {
            return Err(qdbp_err!(
                QdbpErrorKind::InvalidValue,
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
    top: u64,
    name: String,
    id: i32,
    col_type: i32,
    flags: ColumnFlags,
}

/// Builds a `.qdbp` file header into a `Vec<u8>`.
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

    pub fn add_column(
        &mut self,
        top: u64,
        name: &str,
        id: i32,
        col_type: i32,
        flags: ColumnFlags,
    ) -> &mut Self {
        self.columns
            .push(ColumnEntry { top, name: name.to_owned(), id, col_type, flags });
        self
    }

    pub fn add_sorting_column(&mut self, index: u32) -> &mut Self {
        self.sorting_columns.push(index);
        self
    }

    /// Serializes the header into `buf`. Returns the byte offset past the end
    /// of the header (including name strings).
    pub fn write_to(&self, buf: &mut Vec<u8>) -> usize {
        let column_count = self.columns.len() as u32;
        let sorting_count = self.sorting_columns.len() as u32;

        // Fixed header fields.
        buf.extend_from_slice(&FILE_FORMAT_VERSION.to_le_bytes());
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

        // Name strings area.
        let mut name_offsets: Vec<(u64, u32)> = Vec::with_capacity(self.columns.len());
        for col in &self.columns {
            let offset = buf.len() as u64;
            let len = col.name.len() as u32;
            buf.extend_from_slice(col.name.as_bytes());
            name_offsets.push((offset, len));
        }

        // Backpatch column descriptors.
        for (i, col) in self.columns.iter().enumerate() {
            let (name_offset, name_length) = name_offsets[i];
            let desc_offset = descriptors_start + i * COLUMN_DESCRIPTOR_SIZE;
            let desc = ColumnDescriptorRaw {
                top: col.top,
                name_offset,
                name_length,
                id: col.id,
                col_type: col.col_type,
                flags: col.flags.0,
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
        assert_eq!(hdr.designated_timestamp(), None);
        assert_eq!(hdr.column_count(), 0);
        assert_eq!(hdr.sorting_column_count(), 0);
    }

    #[test]
    fn header_round_trip_with_columns() {
        let mut builder = FileHeaderBuilder::new(0);
        builder.add_column(
            100,
            "timestamp",
            0,
            8, // ColumnTypeTag::Timestamp
            ColumnFlags::new().with_repetition(FieldRepetition::Required),
        );
        builder.add_column(
            0,
            "value",
            1,
            10, // ColumnTypeTag::Double
            ColumnFlags::new().with_repetition(FieldRepetition::Optional),
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
        assert_eq!(desc0.top, 100);
        assert_eq!(desc0.id, 0);
        assert_eq!(desc0.col_type, 8);
        assert_eq!(hdr.column_name(desc0).unwrap(), "timestamp");
        assert_eq!(
            desc0.flags().repetition().unwrap(),
            FieldRepetition::Required
        );

        // Column 1.
        let desc1 = hdr.column_descriptor(1).unwrap();
        assert_eq!(desc1.top, 0);
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
                i as u64,
                &format!("col_{i}"),
                i,
                5, // Int
                ColumnFlags::new(),
            );
        }

        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        let hdr = FileHeader::new(&buf).unwrap();
        assert_eq!(hdr.column_count(), 10);
        for i in 0..10 {
            let desc = hdr.column_descriptor(i).unwrap();
            assert_eq!(desc.top, i as u64);
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
        buf[4..8].copy_from_slice(&(-1i32).to_le_bytes()); // designated_ts
        buf[8..12].copy_from_slice(&0u32.to_le_bytes()); // sorting count
        buf[12..16].copy_from_slice(&10u32.to_le_bytes()); // claim 10 columns
                                                           // Buffer is only 16 bytes but needs 16 + 10*32 = 336.
        assert!(FileHeader::new(&buf).is_err());
    }

    #[test]
    fn column_name_out_of_bounds() {
        // Build a valid header, then manually corrupt a name_offset to point
        // past the end of the buffer.
        let mut builder = FileHeaderBuilder::new(-1);
        builder.add_column(0, "ok", 0, 5, ColumnFlags::new());
        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        // Corrupt name_offset of the first descriptor to point far beyond file.
        let desc_offset = HEADER_FIXED_SIZE;
        let bad_name_offset = 99999u64;
        buf[desc_offset + 8..desc_offset + 16].copy_from_slice(&bad_name_offset.to_le_bytes());

        let hdr = FileHeader::new(&buf).unwrap();
        let desc = hdr.column_descriptor(0).unwrap();
        assert!(hdr.column_name(desc).is_err());
    }

    #[test]
    fn column_name_invalid_utf8() {
        // Build a header, then overwrite the name bytes with invalid UTF-8.
        let mut builder = FileHeaderBuilder::new(-1);
        builder.add_column(0, "ab", 0, 5, ColumnFlags::new());
        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        let hdr = FileHeader::new(&buf).unwrap();
        let desc = hdr.column_descriptor(0).unwrap();
        let name_start = desc.name_offset as usize;
        // Overwrite with invalid UTF-8.
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
        builder.add_column(0, "a", 0, 5, ColumnFlags::new());
        builder.add_column(0, "b", 1, 6, ColumnFlags::new());
        builder.add_sorting_column(0);
        builder.add_sorting_column(1);
        let mut buf = Vec::new();
        builder.write_to(&mut buf);

        let hdr = FileHeader::new(&buf).unwrap();
        let expected = HEADER_FIXED_SIZE + 2 * COLUMN_DESCRIPTOR_SIZE + 2 * 4;
        assert_eq!(hdr.sorting_columns_end_offset(), expected);
    }
}
