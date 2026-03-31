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

use crate::parquet::error::ParquetResult;
use crate::parquet_metadata::column_chunk::ColumnChunkRaw;
use crate::parquet_metadata::error::{parquet_meta_err, ParquetMetaErrorKind};
use crate::parquet_metadata::types::{BLOCK_ALIGNMENT, COLUMN_CHUNK_SIZE};

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
    pub fn new(data: &'a [u8], column_count: u32) -> ParquetResult<Self> {
        let min_size = Self::min_block_size(column_count)?;
        if data.len() < min_size {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "row group block too small: {} bytes, need at least {}",
                data.len(),
                min_size
            ));
        }
        // Safety: Row group blocks are 8-byte aligned per spec, so the u64 read is aligned.
        let num_rows = unsafe { *(data.as_ptr() as *const u64) };
        Ok(Self { data, column_count, num_rows })
    }

    /// Minimum byte size of a row group block (NUM_ROWS + chunks, no out-of-line).
    pub fn min_block_size(column_count: u32) -> ParquetResult<usize> {
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
    pub fn column_chunk(&self, index: usize) -> ParquetResult<&'a ColumnChunkRaw> {
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
}

impl RowGroupBlockBuilder {
    /// Creates a builder for a row group block with `column_count` columns.
    pub fn new(column_count: u32) -> Self {
        Self {
            num_rows: 0,
            chunks: vec![ColumnChunkRaw::zeroed(); column_count as usize],
            out_of_line: Vec::new(),
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
    ) -> ParquetResult<&mut Self> {
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
    pub fn add_out_of_line_stat(
        &mut self,
        col_index: usize,
        is_min: bool,
        data: &[u8],
    ) -> ParquetResult<&mut Self> {
        let len = self.chunks.len();
        let chunk = self.chunks.get_mut(col_index).ok_or_else(|| {
            parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "column chunk index {} out of range [0, {})",
                col_index,
                len
            )
        })?;
        let offset = self.out_of_line.len() as u64;
        self.out_of_line.extend_from_slice(data);
        if is_min {
            chunk.min_stat = offset;
        } else {
            chunk.max_stat = offset;
        }
        Ok(self)
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

        // Out-of-line stat data.
        buf.extend_from_slice(&self.out_of_line);

        block_start
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet_metadata::types::{encode_stat_sizes, Codec, StatFlags};

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
        let min_off = c.min_stat as usize;
        assert_eq!(&ool[min_off..min_off + 16], &min_data);
        let max_off = c.max_stat as usize;
        assert_eq!(&ool[max_off..max_off + 16], &max_data);
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
    fn out_of_line_region_empty_when_no_ool_data() {
        let mut buf = Vec::new();
        let mut builder = RowGroupBlockBuilder::new(1);
        builder.set_num_rows(10);
        builder.write_to(&mut buf);
        // Block is exactly min_block_size, so out-of-line region is empty.
        let reader = RowGroupBlockReader::new(&buf, 1).unwrap();
        assert!(reader.out_of_line_region().is_empty());
    }
}
