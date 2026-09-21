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

//! Row group block reader and builder.

use crate::column_chunk::ColumnChunkRaw;
use crate::error::ParquetMetaErrorKind;
use crate::error::ParquetMetaResult;
use crate::parquet_meta_err;
use crate::types::{BLOCK_ALIGNMENT, COLUMN_CHUNK_SIZE};

// ── RowGroupBlockReader (zero-copy) ────────────────────────────────────

/// Zero-copy reader over a single row group block.
///
/// Layout: `NUM_ROWS(u64)` followed by `column_count` column chunks (64B each),
/// followed by optional out-of-line stat data.
pub struct RowGroupBlockReader<'a> {
    data: &'a [u8],
    column_count: u32,
    num_rows: u64,
}

impl<'a> RowGroupBlockReader<'a> {
    /// Creates a reader over the byte slice starting at a row group block.
    pub fn new(data: &'a [u8], column_count: u32) -> ParquetMetaResult<Self> {
        let min_size = Self::min_block_size(column_count)?;
        if data.len() < min_size {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "row group block too small: {} bytes, need at least {}",
                data.len(),
                min_size
            ));
        }
        let num_rows = u64::from_le_bytes(data[0..8].try_into().unwrap());
        Ok(Self {
            data,
            column_count,
            num_rows,
        })
    }

    /// Minimum byte size of a row group block (NUM_ROWS + chunks, no out-of-line).
    pub fn min_block_size(column_count: u32) -> ParquetMetaResult<usize> {
        (column_count as usize)
            .checked_mul(COLUMN_CHUNK_SIZE)
            .and_then(|s| s.checked_add(8))
            .ok_or_else(|| {
                parquet_meta_err!(
                    ParquetMetaErrorKind::Truncated,
                    "column_count overflow in row group block size"
                )
            })
    }

    /// Number of rows in this row group.
    pub fn num_rows(&self) -> u64 {
        self.num_rows
    }

    /// Returns a zero-copy reference to the column chunk at `index`.
    pub fn column_chunk(&self, index: usize) -> ParquetMetaResult<&'a ColumnChunkRaw> {
        if index >= self.column_count as usize {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "column chunk index {} out of range [0, {})",
                index,
                self.column_count
            ));
        }
        let offset = 8 + index * COLUMN_CHUNK_SIZE;
        let ptr = self.data[offset..].as_ptr();
        // Safety: ColumnChunkRaw is #[repr(C)] with 8-byte max alignment.
        // Row group blocks are 8-byte aligned per spec, and offset = 8 + n*64
        // is always 8-byte aligned.
        debug_assert_eq!(ptr.align_offset(align_of::<ColumnChunkRaw>()), 0);
        Ok(unsafe { &*(ptr as *const ColumnChunkRaw) })
    }

    /// Returns the out-of-line stats region (bytes after the last column chunk).
    ///
    /// Note: the returned slice may extend beyond this row group's out-of-line data
    /// since the block's total size is not encoded. Callers must use stat offsets
    /// relative to this slice and not assume the slice length equals the OOL data size.
    pub fn out_of_line_region(&self) -> &'a [u8] {
        // Safe: column_count was validated in new(), so this cannot overflow.
        let start = 8 + (self.column_count as usize) * COLUMN_CHUNK_SIZE;
        if start >= self.data.len() {
            return &[];
        }
        &self.data[start..]
    }
}

// ── RowGroupBlockBuilder ───────────────────────────────────────────────

/// Builds a row group block into a `Vec<u8>`.
#[derive(Debug)]
pub struct RowGroupBlockBuilder {
    pub(crate) num_rows: u64,
    pub(crate) chunks: Vec<ColumnChunkRaw>,
    pub(crate) out_of_line: Vec<u8>,
    /// (col_index, ool_offset) for inlined bloom filters.
    bloom_filters: Vec<(usize, usize)>,
    /// (col_index, parquet_offset, parquet_length) for external bloom filters.
    external_bloom_filters: Vec<(usize, u64, u64)>,
}

impl RowGroupBlockBuilder {
    /// Creates a builder for a row group block with `column_count` columns.
    pub fn new(column_count: u32) -> Self {
        Self {
            num_rows: 0,
            chunks: vec![ColumnChunkRaw::zeroed(); column_count as usize],
            out_of_line: Vec::new(),
            bloom_filters: Vec::new(),
            external_bloom_filters: Vec::new(),
        }
    }

    pub fn num_rows(&self) -> u64 {
        self.num_rows
    }

    pub fn column_chunk_raw(&self, index: usize) -> &ColumnChunkRaw {
        &self.chunks[index]
    }

    pub fn set_num_rows(&mut self, rows: u64) -> &mut Self {
        self.num_rows = rows;
        self
    }

    pub fn set_column_chunk(
        &mut self,
        index: usize,
        chunk: ColumnChunkRaw,
    ) -> ParquetMetaResult<&mut Self> {
        let len = self.chunks.len();
        let slot = self.chunks.get_mut(index).ok_or_else(|| {
            parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "column chunk index {} out of range [0, {})",
                index,
                len
            )
        })?;
        *slot = chunk;
        Ok(self)
    }

    /// Appends out-of-line stat data and patches the column chunk's min or max
    /// stat field to point into this region.
    ///
    /// `is_min`: if true, patches `min_stat`; otherwise patches `max_stat`.
    /// Appends an out-of-line stat value and records its location in the
    /// column chunk. The `min_stat` / `max_stat` field is encoded as
    /// `(offset << 16) | length`: offset occupies the high 48 bits (byte
    /// offset within this row group block's out-of-line region) and length
    /// occupies the low 16 bits. Parquet truncates stats well below 64KB;
    /// oversized values are rejected.
    pub fn add_out_of_line_stat(
        &mut self,
        col_index: usize,
        is_min: bool,
        data: &[u8],
    ) -> ParquetMetaResult<&mut Self> {
        let len = self.chunks.len();
        let chunk = self.chunks.get_mut(col_index).ok_or_else(|| {
            parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "column chunk index {} out of range [0, {})",
                col_index,
                len
            )
        })?;
        if data.len() > u16::MAX as usize {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "out-of-line stat length {} exceeds u16 max {}",
                data.len(),
                u16::MAX
            ));
        }
        let offset = self.out_of_line.len() as u64;
        debug_assert!(
            offset < (1u64 << 48),
            "out-of-line stat offset {} exceeds u48 max",
            offset
        );
        let data_len = data.len() as u64;
        self.out_of_line.extend_from_slice(data);
        let encoded = (offset << 16) | (data_len & 0xFFFF);
        if is_min {
            chunk.min_stat = encoded;
        } else {
            chunk.max_stat = encoded;
        }
        Ok(self)
    }

    /// Appends a bloom filter bitset to the out-of-line region (8-byte aligned)
    /// for the given column. The bitset is stored as `[i32 length][bitset]`.
    /// The absolute _pm file offset is computed by the writer when building
    /// the bloom filter footer section.
    pub fn add_bloom_filter(
        &mut self,
        col_index: usize,
        bitset: &[u8],
    ) -> ParquetMetaResult<&mut Self> {
        let len = self.chunks.len();
        if col_index >= len {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "bloom filter column index {} out of range [0, {})",
                col_index,
                len
            ));
        }
        let bitset_len = i32::try_from(bitset.len()).map_err(|_| {
            parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "bloom filter bitset too large: {} bytes, max {}",
                bitset.len(),
                i32::MAX
            )
        })?;

        // Pad to 8-byte alignment within the OOL region.
        let padding =
            (BLOCK_ALIGNMENT - (self.out_of_line.len() % BLOCK_ALIGNMENT)) % BLOCK_ALIGNMENT;
        self.out_of_line.extend(std::iter::repeat_n(0u8, padding));

        let ool_offset = self.out_of_line.len();
        self.out_of_line
            .extend_from_slice(&bitset_len.to_le_bytes());
        self.out_of_line.extend_from_slice(bitset);
        self.bloom_filters.push((col_index, ool_offset));
        Ok(self)
    }

    /// Returns the inlined bloom filter entries: `(col_idx, ool_offset)` pairs.
    pub fn bloom_filter_inlined_entries(&self) -> &[(usize, usize)] {
        &self.bloom_filters
    }

    /// Records an external bloom filter reference for the given column.
    pub fn add_external_bloom_filter(
        &mut self,
        col_index: usize,
        parquet_offset: u64,
        parquet_length: u64,
    ) -> ParquetMetaResult<&mut Self> {
        let len = self.chunks.len();
        if col_index >= len {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "bloom filter column index {} out of range [0, {})",
                col_index,
                len
            ));
        }
        self.external_bloom_filters
            .push((col_index, parquet_offset, parquet_length));
        Ok(self)
    }

    /// Returns the external bloom filter entries: `(col_idx, parquet_offset, parquet_length)`.
    pub fn bloom_filter_external_entries(&self) -> &[(usize, u64, u64)] {
        &self.external_bloom_filters
    }

    /// Writes the row group block to `buf`, padding to 8-byte alignment.
    /// Returns the byte offset within `buf` where this block starts.
    pub fn write_to(&self, buf: &mut Vec<u8>) -> usize {
        // Pad to 8-byte alignment before writing the block.
        let padding = (BLOCK_ALIGNMENT - (buf.len() % BLOCK_ALIGNMENT)) % BLOCK_ALIGNMENT;
        buf.extend(std::iter::repeat_n(0u8, padding));

        let block_start = buf.len();

        // NUM_ROWS.
        buf.extend_from_slice(&self.num_rows.to_le_bytes());

        // Column chunks.
        for chunk in &self.chunks {
            // Safety: ColumnChunkRaw is #[repr(C)], Copy, and 64 bytes.
            let bytes: &[u8; COLUMN_CHUNK_SIZE] =
                unsafe { &*(chunk as *const ColumnChunkRaw as *const [u8; COLUMN_CHUNK_SIZE]) };
            buf.extend_from_slice(bytes);
        }

        // Out-of-line data (stats + bloom filters).
        buf.extend_from_slice(&self.out_of_line);

        block_start
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{encode_stat_sizes, Codec, StatFlags};

    #[test]
    fn round_trip_simple() {
        let mut builder = RowGroupBlockBuilder::new(2);
        builder.set_num_rows(1000);

        let mut chunk0 = ColumnChunkRaw::zeroed();
        chunk0.codec = Codec::Snappy as u8;
        chunk0.num_values = 1000;
        chunk0.byte_range_start = 4096;
        chunk0.total_compressed = 8192;
        builder.set_column_chunk(0, chunk0).unwrap();

        let mut chunk1 = ColumnChunkRaw::zeroed();
        chunk1.codec = Codec::Zstd as u8;
        chunk1.num_values = 1000;
        builder.set_column_chunk(1, chunk1).unwrap();

        let mut buf = Vec::new();
        let offset = builder.write_to(&mut buf);

        let reader = RowGroupBlockReader::new(&buf[offset..], 2).unwrap();
        assert_eq!(reader.num_rows(), 1000);

        let c0 = reader.column_chunk(0).unwrap();
        assert_eq!(c0.codec().unwrap(), Codec::Snappy);
        assert_eq!(c0.num_values, 1000);
        assert_eq!(c0.byte_range_start, 4096);
        assert_eq!(c0.total_compressed, 8192);

        let c1 = reader.column_chunk(1).unwrap();
        assert_eq!(c1.codec().unwrap(), Codec::Zstd);
    }

    #[test]
    fn out_of_line_stats() {
        let mut builder = RowGroupBlockBuilder::new(1);
        builder.set_num_rows(100);

        let mut chunk = ColumnChunkRaw::zeroed();
        chunk.stat_flags = StatFlags::new()
            .with_min(false, true) // not inlined
            .with_max(false, true)
            .0;
        chunk.stat_sizes = encode_stat_sizes(0, 0); // inline sizes irrelevant
        builder.set_column_chunk(0, chunk).unwrap();

        // 16-byte out-of-line min stat (e.g., UUID).
        let min_data = [1u8; 16];
        builder.add_out_of_line_stat(0, true, &min_data).unwrap();
        // 16-byte out-of-line max stat.
        let max_data = [0xFFu8; 16];
        builder.add_out_of_line_stat(0, false, &max_data).unwrap();

        let mut buf = Vec::new();
        let offset = builder.write_to(&mut buf);

        let reader = RowGroupBlockReader::new(&buf[offset..], 1).unwrap();
        let c = reader.column_chunk(0).unwrap();

        let ool = reader.out_of_line_region();
        // OOL stats encode (offset << 16) | length.
        let min_off = (c.min_stat >> 16) as usize;
        let min_len = (c.min_stat & 0xFFFF) as usize;
        assert_eq!(min_len, 16);
        assert_eq!(&ool[min_off..min_off + min_len], &min_data);
        let max_off = (c.max_stat >> 16) as usize;
        let max_len = (c.max_stat & 0xFFFF) as usize;
        assert_eq!(max_len, 16);
        assert_eq!(&ool[max_off..max_off + max_len], &max_data);
    }

    #[test]
    fn alignment_padding() {
        let mut buf = vec![0u8; 5]; // Not 8-byte aligned.
        let builder = RowGroupBlockBuilder::new(1);
        let offset = builder.write_to(&mut buf);
        assert_eq!(offset % BLOCK_ALIGNMENT, 0);
    }

    #[test]
    fn block_too_small() {
        assert!(RowGroupBlockReader::new(&[0u8; 4], 1).is_err());
    }

    #[test]
    fn chunk_index_out_of_range() {
        let mut buf = Vec::new();
        RowGroupBlockBuilder::new(1).write_to(&mut buf);
        let reader = RowGroupBlockReader::new(&buf, 1).unwrap();
        assert!(reader.column_chunk(1).is_err());
    }

    #[test]
    fn set_column_chunk_out_of_range() {
        let mut builder = RowGroupBlockBuilder::new(1);
        assert!(builder
            .set_column_chunk(1, ColumnChunkRaw::zeroed())
            .is_err());
    }

    #[test]
    fn add_out_of_line_stat_out_of_range() {
        let mut builder = RowGroupBlockBuilder::new(1);
        assert!(builder.add_out_of_line_stat(1, true, &[0u8; 8]).is_err());
    }

    #[test]
    fn add_out_of_line_stat_length_overflows_u16() {
        let mut builder = RowGroupBlockBuilder::new(1);
        let oversized = vec![0u8; (u16::MAX as usize) + 1];
        assert!(builder.add_out_of_line_stat(0, true, &oversized).is_err());
    }

    #[test]
    fn out_of_line_region_empty_when_no_ool_data() {
        let mut buf = Vec::new();
        let mut builder = RowGroupBlockBuilder::new(1);
        builder.set_num_rows(10);
        builder.write_to(&mut buf);
        // Block is exactly min_block_size, so out-of-line region is empty.
        let reader = RowGroupBlockReader::new(&buf, 1).unwrap();
        assert!(reader.out_of_line_region().is_empty());
    }

    #[test]
    fn bloom_filter_round_trip() {
        let bitset = vec![0xAA_u8; 64]; // 64-byte bloom filter bitset
        let mut builder = RowGroupBlockBuilder::new(2);
        builder.set_num_rows(500);
        builder
            .set_column_chunk(0, ColumnChunkRaw::zeroed())
            .unwrap();
        builder
            .set_column_chunk(1, ColumnChunkRaw::zeroed())
            .unwrap();
        builder.add_bloom_filter(1, &bitset).unwrap();

        // Verify inlined entries accessor.
        let entries = builder.bloom_filter_inlined_entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, 1); // col_index

        let mut buf = Vec::new();
        let block_start = builder.write_to(&mut buf);

        let reader = RowGroupBlockReader::new(&buf[block_start..], 2).unwrap();
        // _reserved field stays zero on both columns.
        assert_eq!(reader.column_chunk(0).unwrap()._reserved, 0);
        assert_eq!(reader.column_chunk(1).unwrap()._reserved, 0);

        // Read [i32 len][bitset] from the OOL region at the recorded offset.
        let ool_start = block_start + 8 + 2 * COLUMN_CHUNK_SIZE;
        let ool_offset = entries[0].1;
        let bf_data = &buf[ool_start + ool_offset..];
        let bf_len = i32::from_le_bytes(bf_data[..4].try_into().unwrap()) as usize;
        assert_eq!(bf_len, 64);
        assert_eq!(&bf_data[4..4 + bf_len], &bitset);
    }

    #[test]
    fn bloom_filter_with_ool_stats() {
        // Ensure bloom filters work alongside out-of-line stats.
        let mut builder = RowGroupBlockBuilder::new(1);
        builder.set_num_rows(100);
        builder
            .set_column_chunk(0, ColumnChunkRaw::zeroed())
            .unwrap();

        // Add OOL stat first (variable length, may misalign).
        let stat_data = [0x11_u8; 13]; // 13 bytes, not 8-byte aligned
        builder.add_out_of_line_stat(0, true, &stat_data).unwrap();

        // Add bloom filter — should be padded to 8-byte alignment.
        let bitset = vec![0xBB_u8; 32];
        builder.add_bloom_filter(0, &bitset).unwrap();

        let mut buf = Vec::new();
        let block_start = builder.write_to(&mut buf);

        let reader = RowGroupBlockReader::new(&buf[block_start..], 1).unwrap();
        let c = reader.column_chunk(0).unwrap();

        // OOL stat still readable. Encoding: (offset << 16) | length.
        let ool = reader.out_of_line_region();
        let min_off = (c.min_stat >> 16) as usize;
        let min_len = (c.min_stat & 0xFFFF) as usize;
        assert_eq!(min_len, 13);
        assert_eq!(&ool[min_off..min_off + min_len], &stat_data);

        // Bloom filter readable from OOL via the builder entries.
        let entries = builder.bloom_filter_inlined_entries();
        assert_eq!(entries.len(), 1);
        let ool_start = block_start + 8 + COLUMN_CHUNK_SIZE;
        let ool_offset = entries[0].1;
        let bf_abs = ool_start + ool_offset;
        assert_eq!(bf_abs % BLOCK_ALIGNMENT, 0);
        let bf_data = &buf[bf_abs..];
        let bf_len = i32::from_le_bytes(bf_data[..4].try_into().unwrap()) as usize;
        assert_eq!(bf_len, 32);
        assert_eq!(&bf_data[4..4 + bf_len], &bitset);
    }

    #[test]
    fn bloom_filter_multiple_columns() {
        let mut builder = RowGroupBlockBuilder::new(3);
        builder.set_num_rows(200);
        for i in 0..3 {
            builder
                .set_column_chunk(i, ColumnChunkRaw::zeroed())
                .unwrap();
        }
        let bf0 = vec![0x11_u8; 32];
        let bf2 = vec![0x22_u8; 64];
        builder.add_bloom_filter(0, &bf0).unwrap();
        builder.add_bloom_filter(2, &bf2).unwrap();

        let entries = builder.bloom_filter_inlined_entries();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, 0); // col 0
        assert_eq!(entries[1].0, 2); // col 2

        let mut buf = Vec::new();
        let block_start = builder.write_to(&mut buf);

        let reader = RowGroupBlockReader::new(&buf[block_start..], 3).unwrap();
        // _reserved stays zero on all columns.
        for i in 0..3 {
            assert_eq!(reader.column_chunk(i).unwrap()._reserved, 0);
        }

        // Verify OOL bitsets via builder entries.
        let ool_start = block_start + 8 + 3 * COLUMN_CHUNK_SIZE;
        for (entry, expected_bitset) in entries.iter().zip([&bf0[..], &bf2[..]]) {
            let bf_data = &buf[ool_start + entry.1..];
            let bf_len = i32::from_le_bytes(bf_data[..4].try_into().unwrap()) as usize;
            assert_eq!(&bf_data[4..4 + bf_len], expected_bitset);
        }
    }

    #[test]
    fn bloom_filter_external_round_trip() {
        let mut builder = RowGroupBlockBuilder::new(2);
        builder.set_num_rows(100);
        builder.add_external_bloom_filter(0, 1024, 256).unwrap();
        builder.add_external_bloom_filter(1, 2048, 512).unwrap();

        let entries = builder.bloom_filter_external_entries();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], (0, 1024, 256));
        assert_eq!(entries[1], (1, 2048, 512));
    }

    #[test]
    fn bloom_filter_col_index_out_of_range() {
        let mut builder = RowGroupBlockBuilder::new(1);
        builder
            .set_column_chunk(0, ColumnChunkRaw::zeroed())
            .unwrap();
        assert!(builder.add_bloom_filter(1, &[0u8; 32]).is_err());
    }
}
