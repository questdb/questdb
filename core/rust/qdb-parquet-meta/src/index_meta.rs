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

//! `_im` covering-index metadata file format.
//!
//! Sidecar to `<col>.pidx.parquet`, mirroring the role `_pm` plays for
//! `data.parquet`: it carries the byte ranges needed to fetch index row
//! groups without reading the Parquet footer, the key directory used to
//! locate a key's row groups, and the zone maps used to prune them.
//!
//! The format specification lives in
//! `docs/superpowers/specs/2026-08-10-covering-index-parquet-design.md`.

use crate::error::{ParquetMetaErrorKind, ParquetMetaResult};
use crate::parquet_meta_err;

#[cfg(not(target_endian = "little"))]
compile_error!("index meta format requires a little-endian target");

/// Fixed portion of the `_im` header.
pub const IM_HEADER_SIZE: usize = 48;
/// Current `_im` format version.
pub const IM_FORMAT_VERSION: u32 = 1;
/// Size of the CRC trailer at the end of the file.
pub const IM_TRAILER_SIZE: usize = 4;
/// First byte covered by the CRC; `IM_FILE_SIZE` at offset 0 is excluded
/// because the writer patches it last as the commit signal.
pub const IM_CRC_AREA_OFF: usize = 8;

const OFF_IM_FILE_SIZE: usize = 0;
const OFF_FEATURE_FLAGS: usize = 8;
const OFF_FORMAT_VERSION: usize = 16;
const OFF_PAYLOAD_KIND: usize = 20;
const OFF_INDEX_RG_COUNT: usize = 24;
const OFF_DATA_RG_COUNT: usize = 28;
const OFF_INDEX_COLUMN_COUNT: usize = 32;
const OFF_KEY_COUNT: usize = 36;
const OFF_RESERVED: usize = 40;

struct RowGroupMeta {
    first_key: u32,
    row_id_min: i64,
    row_id_max: i64,
    col_ranges: Vec<(u64, u64)>,
}

/// Builds a complete `_im` file in memory.
pub struct IndexMetaWriter {
    payload_kind: u32,
    key_count: u32,
    row_groups: Vec<RowGroupMeta>,
    data_boundaries: Vec<i64>,
}

impl IndexMetaWriter {
    pub fn new(payload_kind: u32, key_count: u32) -> Self {
        Self {
            payload_kind,
            key_count,
            row_groups: Vec::new(),
            data_boundaries: Vec::new(),
        }
    }

    /// Appends one index row group. `col_ranges` is one `(offset, length)`
    /// pair per index column, in schema order; every row group must supply
    /// the same number of pairs.
    pub fn add_row_group(
        &mut self,
        first_key: u32,
        row_id_min: i64,
        row_id_max: i64,
        col_ranges: &[(u64, u64)],
    ) {
        self.row_groups.push(RowGroupMeta {
            first_key,
            row_id_min,
            row_id_max,
            col_ranges: col_ranges.to_vec(),
        });
    }

    pub fn set_data_row_group_boundaries(&mut self, boundaries: &[i64]) {
        self.data_boundaries = boundaries.to_vec();
    }

    /// Overwrites the payload kind and key count set at construction. The JNI
    /// layer creates the writer before Java knows either value, so it calls
    /// this once the index build has determined them.
    pub fn set_payload(&mut self, payload_kind: u32, key_count: u32) {
        self.payload_kind = payload_kind;
        self.key_count = key_count;
    }

    /// Serialises the complete `_im` file. Takes `&self` — matching
    /// `ParquetMetaUpdateWriter::finish` — so the JNI layer can build the
    /// buffer without consuming the boxed writer that Java still owns and will
    /// later hand to `destroyWriter`.
    pub fn finish(&self) -> ParquetMetaResult<Vec<u8>> {
        if self.data_boundaries.is_empty() {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "data row group boundaries not set"
            ));
        }
        let rg_count = self.row_groups.len();
        let col_count = self.row_groups.first().map_or(0, |rg| rg.col_ranges.len());
        for (i, rg) in self.row_groups.iter().enumerate() {
            if rg.col_ranges.len() != col_count {
                return Err(parquet_meta_err!(
                    ParquetMetaErrorKind::SchemaMismatch,
                    "row group {i} has {} column ranges, expected {col_count}",
                    rg.col_ranges.len()
                ));
            }
            if i > 0 && rg.first_key < self.row_groups[i - 1].first_key {
                return Err(parquet_meta_err!(
                    ParquetMetaErrorKind::InvalidValue,
                    "row group first keys must be non-decreasing at index {i}"
                ));
            }
        }
        // Both readers answer "absent" for key >= KEY_COUNT, so a first key at
        // or above it would make every posting in that row group unreachable:
        // the query returns zero rows and nothing reports an error. First keys
        // are non-decreasing by the check above, so the last one bounds them all.
        if let Some(last) = self.row_groups.last() {
            if last.first_key >= self.key_count {
                return Err(parquet_meta_err!(
                    ParquetMetaErrorKind::InvalidValue,
                    "row group first key {} must be below key count {}",
                    last.first_key,
                    self.key_count
                ));
            }
        }
        if self.data_boundaries[0] != 0 {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "first data row group boundary must be 0, got {}",
                self.data_boundaries[0]
            ));
        }
        // A binary search over a non-monotone boundary array maps row ids to the
        // wrong data row group without failing.
        for i in 1..self.data_boundaries.len() {
            if self.data_boundaries[i] < self.data_boundaries[i - 1] {
                return Err(parquet_meta_err!(
                    ParquetMetaErrorKind::InvalidValue,
                    "data row group boundaries must be non-decreasing at index {i}"
                ));
            }
        }

        let mut buf = vec![0u8; IM_HEADER_SIZE];
        buf[OFF_FORMAT_VERSION..OFF_FORMAT_VERSION + 4]
            .copy_from_slice(&IM_FORMAT_VERSION.to_le_bytes());
        buf[OFF_PAYLOAD_KIND..OFF_PAYLOAD_KIND + 4]
            .copy_from_slice(&self.payload_kind.to_le_bytes());
        buf[OFF_INDEX_RG_COUNT..OFF_INDEX_RG_COUNT + 4]
            .copy_from_slice(&(rg_count as u32).to_le_bytes());
        buf[OFF_DATA_RG_COUNT..OFF_DATA_RG_COUNT + 4]
            .copy_from_slice(&((self.data_boundaries.len() - 1) as u32).to_le_bytes());
        buf[OFF_INDEX_COLUMN_COUNT..OFF_INDEX_COLUMN_COUNT + 4]
            .copy_from_slice(&(col_count as u32).to_le_bytes());
        buf[OFF_KEY_COUNT..OFF_KEY_COUNT + 4].copy_from_slice(&self.key_count.to_le_bytes());
        buf[OFF_FEATURE_FLAGS..OFF_FEATURE_FLAGS + 8].copy_from_slice(&0u64.to_le_bytes());
        buf[OFF_RESERVED..OFF_RESERVED + 8].copy_from_slice(&0u64.to_le_bytes());

        for rg in &self.row_groups {
            buf.extend_from_slice(&rg.first_key.to_le_bytes());
        }
        buf.extend_from_slice(&self.key_count.to_le_bytes());
        align_to_8(&mut buf);

        for rg in &self.row_groups {
            buf.extend_from_slice(&rg.row_id_min.to_le_bytes());
        }
        for rg in &self.row_groups {
            buf.extend_from_slice(&rg.row_id_max.to_le_bytes());
        }
        for b in &self.data_boundaries {
            buf.extend_from_slice(&b.to_le_bytes());
        }
        for rg in &self.row_groups {
            for (offset, length) in &rg.col_ranges {
                buf.extend_from_slice(&offset.to_le_bytes());
                buf.extend_from_slice(&length.to_le_bytes());
            }
        }

        let crc_end = buf.len();
        buf.extend_from_slice(&0u32.to_le_bytes());
        let total = buf.len() as u64;
        buf[OFF_IM_FILE_SIZE..OFF_IM_FILE_SIZE + 8].copy_from_slice(&total.to_le_bytes());
        let crc = crc32fast::hash(&buf[IM_CRC_AREA_OFF..crc_end]);
        buf[crc_end..crc_end + 4].copy_from_slice(&crc.to_le_bytes());
        Ok(buf)
    }
}

fn align_to_8(buf: &mut Vec<u8>) {
    while !buf.len().is_multiple_of(8) {
        buf.push(0);
    }
}

/// Zero-copy reader over a complete, committed `_im` buffer.
#[derive(Debug)]
pub struct IndexMetaReader<'a> {
    data: &'a [u8],
    rg_first_key_off: usize,
    row_id_min_off: usize,
    row_id_max_off: usize,
    data_boundary_off: usize,
    col_range_off: usize,
    index_rg_count: usize,
    data_rg_count: usize,
    index_column_count: usize,
    key_count: u32,
    payload_kind: u32,
    im_file_size: u64,
}

impl<'a> IndexMetaReader<'a> {
    pub fn new(data: &'a [u8]) -> ParquetMetaResult<Self> {
        if data.len() < IM_HEADER_SIZE + IM_TRAILER_SIZE {
            return Err(parquet_meta_err!(ParquetMetaErrorKind::Truncated));
        }
        let im_file_size = read_u64(data, OFF_IM_FILE_SIZE);
        if im_file_size as usize > data.len()
            || (im_file_size as usize) < IM_HEADER_SIZE + IM_TRAILER_SIZE
        {
            return Err(parquet_meta_err!(ParquetMetaErrorKind::Truncated));
        }
        let version = read_u32(data, OFF_FORMAT_VERSION);
        if version != IM_FORMAT_VERSION {
            return Err(parquet_meta_err!(ParquetMetaErrorKind::VersionMismatch {
                found: version,
                expected: IM_FORMAT_VERSION,
            }));
        }
        let required = read_u64(data, OFF_FEATURE_FLAGS) & 0xFFFF_FFFF_0000_0000;
        if required != 0 {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::UnsupportedFeature { flags: required }
            ));
        }
        let end = im_file_size as usize;
        let crc_end = end - IM_TRAILER_SIZE;
        let stored = read_u32(data, crc_end);
        let computed = crc32fast::hash(&data[IM_CRC_AREA_OFF..crc_end]);
        if stored != computed {
            return Err(parquet_meta_err!(ParquetMetaErrorKind::ChecksumMismatch {
                stored,
                computed
            }));
        }

        let index_rg_count = read_u32(data, OFF_INDEX_RG_COUNT) as usize;
        let data_rg_count = read_u32(data, OFF_DATA_RG_COUNT) as usize;
        let index_column_count = read_u32(data, OFF_INDEX_COLUMN_COUNT) as usize;

        let rg_first_key_off = IM_HEADER_SIZE;
        let after_keys = rg_first_key_off + (index_rg_count + 1) * 4;
        let row_id_min_off = after_keys.next_multiple_of(8);
        let row_id_max_off = row_id_min_off + index_rg_count * 8;
        let data_boundary_off = row_id_max_off + index_rg_count * 8;
        let col_range_off = data_boundary_off + (data_rg_count + 1) * 8;
        // The counts come straight off the header, so on a crafted file the
        // product overflows usize and `needed` wraps below `end`: the file would
        // pass this check and panic later in `column_byte_range`. The Java reader
        // rearranges the same test into a division for the same reason; see
        // IndexMetaFileReader.of.
        let needed = index_rg_count
            .checked_mul(index_column_count)
            .and_then(|entries| entries.checked_mul(16))
            .and_then(|bytes| bytes.checked_add(col_range_off))
            .and_then(|off| off.checked_add(IM_TRAILER_SIZE));
        match needed {
            Some(needed) if needed <= end => {}
            _ => return Err(parquet_meta_err!(ParquetMetaErrorKind::Truncated)),
        }

        Ok(Self {
            data,
            rg_first_key_off,
            row_id_min_off,
            row_id_max_off,
            data_boundary_off,
            col_range_off,
            index_rg_count,
            data_rg_count,
            index_column_count,
            key_count: read_u32(data, OFF_KEY_COUNT),
            payload_kind: read_u32(data, OFF_PAYLOAD_KIND),
            im_file_size,
        })
    }

    pub fn column_byte_range(&self, row_group: usize, column: usize) -> (u64, u64) {
        let base = self.col_range_off + (row_group * self.index_column_count + column) * 16;
        (read_u64(self.data, base), read_u64(self.data, base + 8))
    }

    pub fn data_row_group_boundary(&self, i: usize) -> i64 {
        read_u64(self.data, self.data_boundary_off + i * 8) as i64
    }

    pub fn data_row_group_count(&self) -> usize {
        self.data_rg_count
    }

    pub fn im_file_size(&self) -> u64 {
        self.im_file_size
    }

    pub fn index_column_count(&self) -> usize {
        self.index_column_count
    }

    pub fn index_row_group_count(&self) -> usize {
        self.index_rg_count
    }

    pub fn key_count(&self) -> u32 {
        self.key_count
    }

    pub fn payload_kind(&self) -> u32 {
        self.payload_kind
    }

    /// Inclusive `(rg_lo, rg_hi)` range of index row groups holding `key`,
    /// or `None` when `key` is outside the covered key space.
    pub fn row_group_range_for_key(&self, key: u32) -> Option<(usize, usize)> {
        if key >= self.key_count || self.index_rg_count == 0 {
            return None;
        }
        let first_key = |i: usize| read_u32(self.data, self.rg_first_key_off + i * 4);
        let n = self.index_rg_count;

        let mut lo = 0usize;
        let mut hi = n;
        while lo < hi {
            let mid = (lo + hi) / 2;
            if first_key(mid) < key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let rg_lo = if lo < n && first_key(lo) == key {
            lo
        } else if lo == 0 {
            return None;
        } else {
            lo - 1
        };

        let mut ulo = 0usize;
        let mut uhi = n;
        while ulo < uhi {
            let mid = (ulo + uhi) / 2;
            if first_key(mid) <= key {
                ulo = mid + 1;
            } else {
                uhi = mid;
            }
        }
        Some((rg_lo, ulo - 1))
    }

    pub fn row_id_max(&self, row_group: usize) -> i64 {
        read_u64(self.data, self.row_id_max_off + row_group * 8) as i64
    }

    pub fn row_id_min(&self, row_group: usize) -> i64 {
        read_u64(self.data, self.row_id_min_off + row_group * 8) as i64
    }
}

fn read_u32(data: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(data[off..off + 4].try_into().unwrap())
}

fn read_u64(data: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(data[off..off + 8].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_sample() -> Vec<u8> {
        let mut w = IndexMetaWriter::new(0, 11_405);
        w.add_row_group(0, 0, 99_999, &[(4, 100), (104, 200)]);
        w.add_row_group(11_403, 100_000, 157_999, &[(304, 50), (354, 60)]);
        w.add_row_group(11_403, 158_000, 240_000, &[(414, 70), (484, 80)]);
        w.add_row_group(11_404, 240_001, 999_999, &[(564, 90), (654, 10)]);
        w.set_data_row_group_boundaries(&[0, 500_000, 1_000_000]);
        w.finish().unwrap()
    }

    /// 3 index row groups end the key directory at `48 + 4 * 4 = 64`, which is
    /// already 8-aligned, so `align_to_8` adds nothing. Every other sample uses
    /// an even count and therefore only ever exercises the padded case.
    fn build_odd_row_group_sample() -> Vec<u8> {
        let mut w = IndexMetaWriter::new(0, 900);
        w.add_row_group(0, 0, 99, &[(4, 100), (104, 200)]);
        w.add_row_group(300, 100, 199, &[(304, 50), (354, 60)]);
        w.add_row_group(700, 200, 299, &[(414, 70), (484, 80)]);
        w.set_data_row_group_boundaries(&[0, 150, 300]);
        w.finish().unwrap()
    }

    /// Overwrites a header `u32` and repairs the CRC trailer, so the reader
    /// reaches the section arithmetic instead of failing the checksum first.
    fn patch_header_u32(bytes: &mut [u8], off: usize, value: u32) {
        bytes[off..off + 4].copy_from_slice(&value.to_le_bytes());
        let crc_end = bytes.len() - IM_TRAILER_SIZE;
        let crc = crc32fast::hash(&bytes[IM_CRC_AREA_OFF..crc_end]);
        bytes[crc_end..crc_end + 4].copy_from_slice(&crc.to_le_bytes());
    }

    #[test]
    fn test_round_trip_header_fields() {
        let bytes = build_sample();
        let r = IndexMetaReader::new(&bytes).unwrap();
        assert_eq!(r.im_file_size(), bytes.len() as u64);
        assert_eq!(r.payload_kind(), 0);
        assert_eq!(r.key_count(), 11_405);
        assert_eq!(r.index_row_group_count(), 4);
        assert_eq!(r.data_row_group_count(), 2);
        assert_eq!(r.index_column_count(), 2);
    }

    #[test]
    fn test_key_spanning_multiple_row_groups() {
        let bytes = build_sample();
        let r = IndexMetaReader::new(&bytes).unwrap();
        assert_eq!(r.row_group_range_for_key(11_403), Some((1, 2)));
    }

    #[test]
    fn test_key_packed_into_shared_row_group() {
        let bytes = build_sample();
        let r = IndexMetaReader::new(&bytes).unwrap();
        assert_eq!(r.row_group_range_for_key(5), Some((0, 0)));
        assert_eq!(r.row_group_range_for_key(0), Some((0, 0)));
        assert_eq!(r.row_group_range_for_key(11_404), Some((3, 3)));
    }

    #[test]
    fn test_key_out_of_range() {
        let bytes = build_sample();
        let r = IndexMetaReader::new(&bytes).unwrap();
        assert_eq!(r.row_group_range_for_key(11_405), None);
        assert_eq!(r.row_group_range_for_key(u32::MAX), None);
    }

    #[test]
    fn test_zone_maps_and_byte_ranges() {
        let bytes = build_sample();
        let r = IndexMetaReader::new(&bytes).unwrap();
        assert_eq!(r.row_id_min(1), 100_000);
        assert_eq!(r.row_id_max(1), 157_999);
        assert_eq!(r.column_byte_range(2, 1), (484, 80));
        assert_eq!(r.data_row_group_boundary(0), 0);
        assert_eq!(r.data_row_group_boundary(2), 1_000_000);
    }

    #[test]
    fn test_checksum_mismatch_is_rejected() {
        let mut bytes = build_sample();
        let victim = 48;
        bytes[victim] ^= 0xFF;
        let err = IndexMetaReader::new(&bytes).unwrap_err();
        assert!(matches!(
            err.kind,
            ParquetMetaErrorKind::ChecksumMismatch { .. }
        ));
    }

    #[test]
    fn test_truncated_file_is_rejected() {
        let bytes = build_sample();
        let err = IndexMetaReader::new(&bytes[..40]).unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::Truncated));
    }

    #[test]
    fn test_first_key_at_or_above_key_count_is_rejected() {
        let mut w = IndexMetaWriter::new(0, 10);
        w.add_row_group(0, 0, 99, &[(4, 100)]);
        w.add_row_group(10, 100, 199, &[(104, 100)]);
        w.set_data_row_group_boundaries(&[0, 200]);
        let err = w.finish().unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::InvalidValue));
        assert!(err.msg.contains("must be below key count"), "{}", err.msg);
    }

    #[test]
    fn test_first_data_boundary_must_be_zero() {
        let mut w = IndexMetaWriter::new(0, 10);
        w.add_row_group(0, 0, 99, &[(4, 100)]);
        w.set_data_row_group_boundaries(&[1, 200]);
        let err = w.finish().unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::InvalidValue));
        assert!(
            err.msg.contains("first data row group boundary must be 0"),
            "{}",
            err.msg
        );
    }

    #[test]
    fn test_data_boundaries_must_be_non_decreasing() {
        let mut w = IndexMetaWriter::new(0, 10);
        w.add_row_group(0, 0, 99, &[(4, 100)]);
        w.set_data_row_group_boundaries(&[0, 200, 150]);
        let err = w.finish().unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::InvalidValue));
        assert!(err.msg.contains("non-decreasing at index 2"), "{}", err.msg);
    }

    /// A header claiming `u32::MAX` row groups and `u32::MAX` columns makes the
    /// col-range size product overflow usize. Unchecked, `needed` wraps below
    /// `end`, the file passes validation, and `column_byte_range` panics later.
    #[test]
    fn test_col_range_size_overflow_is_rejected() {
        let mut bytes = build_sample();
        patch_header_u32(&mut bytes, OFF_INDEX_RG_COUNT, u32::MAX);
        patch_header_u32(&mut bytes, OFF_INDEX_COLUMN_COUNT, u32::MAX);
        let err = IndexMetaReader::new(&bytes).unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::Truncated));
    }

    #[test]
    fn test_odd_row_group_count_leaves_key_directory_unpadded() {
        let bytes = build_odd_row_group_sample();
        assert_eq!(bytes.len(), 236);
        assert_eq!(read_u32(&bytes, 48), 0);
        assert_eq!(read_u32(&bytes, 52), 300);
        assert_eq!(read_u32(&bytes, 56), 700);
        // Key count sentinel ends the directory at 64, so RG_ROW_ID_MIN starts
        // there with no padding byte in between.
        assert_eq!(read_u32(&bytes, 60), 900);
        assert_eq!(read_u64(&bytes, 64), 0);
        assert_eq!(read_u64(&bytes, 88), 99);
        assert_eq!(read_u64(&bytes, 112), 0);
        assert_eq!(read_u64(&bytes, 216), 484);
        assert_eq!(read_u64(&bytes, 224), 80);

        let r = IndexMetaReader::new(&bytes).unwrap();
        assert_eq!(r.index_row_group_count(), 3);
        assert_eq!(r.index_column_count(), 2);
        assert_eq!(r.data_row_group_count(), 2);
        assert_eq!(r.row_id_min(0), 0);
        assert_eq!(r.row_id_max(2), 299);
        assert_eq!(r.data_row_group_boundary(2), 300);
        assert_eq!(r.column_byte_range(2, 1), (484, 80));
        assert_eq!(r.row_group_range_for_key(700), Some((2, 2)));
    }

    #[test]
    fn test_version_mismatch_is_rejected() {
        let mut bytes = build_sample();
        bytes[16..20].copy_from_slice(&99u32.to_le_bytes());
        let err = IndexMetaReader::new(&bytes).unwrap_err();
        assert!(matches!(
            err.kind,
            ParquetMetaErrorKind::VersionMismatch { .. }
        ));
    }
}
