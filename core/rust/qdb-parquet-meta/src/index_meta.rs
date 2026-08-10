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

//! `_im` covering-index metadata file format, version 2.
//!
//! Sidecar to `<col>.pidx.parquet`, doing for the covering index exactly what
//! `_pm` does for `data.parquet`: it carries the column descriptors that map
//! index columns back to QuestDB columns, per-row-group column chunks (byte
//! ranges, codec, encodings, null counts and statistics) so index data pulled
//! from cold storage can be decoded without reading the parquet footer, the
//! key directory used to locate a key's row groups, and the `data.parquet`
//! row-group boundaries used to turn a row id into a data row group.
//!
//! The format deliberately reuses `_pm`'s [`ColumnDescriptorRaw`],
//! [`RowGroupBlockBuilder`] / [`RowGroupBlockReader`] and
//! [`ColumnChunkRaw`](crate::column_chunk::ColumnChunkRaw) structures
//! byte-for-byte; only the header and the index-specific sections differ.
//!
//! The format specification lives in `docs/index-metadata.md`.

use crate::error::{ParquetMetaErrorKind, ParquetMetaResult};
use crate::header::ColumnDescriptorRaw;
use crate::parquet_meta_err;
use crate::row_group::{RowGroupBlockBuilder, RowGroupBlockReader};
use crate::types::{BLOCK_ALIGNMENT, BLOCK_ALIGNMENT_SHIFT, COLUMN_DESCRIPTOR_SIZE};

#[cfg(not(target_endian = "little"))]
compile_error!("index meta format requires a little-endian target");

/// Size of the fixed `_im` header.
pub const IM_HEADER_SIZE: usize = 64;
/// Current `_im` format version. Version 1 is not readable; see the spec's
/// "Versioning" section.
pub const IM_FORMAT_VERSION: u32 = 2;
/// `_im` magic at offset 8: the bytes `QDBIDX\0\x02`. It disambiguates `_im`
/// from `_pm`, which carries `FEATURE_FLAGS` at the same offset.
pub const IM_MAGIC: u64 = 0x0200_5844_4942_4451;
/// Size of the CRC trailer at the end of the file.
pub const IM_TRAILER_SIZE: usize = 4;
/// First byte covered by the CRC; `IM_FILE_SIZE` at offset 0 is excluded
/// because the writer patches it last as the commit signal.
pub const IM_CRC_AREA_OFF: usize = 8;

/// Row-per-posting payload: one index row per posting, with a `row_id` column.
pub const IM_PAYLOAD_ROW_PER_POSTING: u32 = 0;
/// Row-per-key payload: one index row per key, with no `row_id` column.
pub const IM_PAYLOAD_ROW_PER_KEY: u32 = 1;

const OFF_IM_FILE_SIZE: usize = 0;
const OFF_IM_MAGIC: usize = 8;
const OFF_FEATURE_FLAGS: usize = 16;
const OFF_FORMAT_VERSION: usize = 24;
const OFF_PAYLOAD_KIND: usize = 28;
const OFF_COLUMN_COUNT: usize = 32;
const OFF_INDEX_RG_COUNT: usize = 36;
const OFF_DATA_RG_COUNT: usize = 40;
const OFF_KEY_COUNT: usize = 44;
const OFF_KEY_ID_COLUMN: usize = 48;
const OFF_ROW_ID_COLUMN: usize = 52;
const OFF_INDEX_SECTIONS_OFFSET: usize = 56;

/// Field offsets inside `_pm`'s 32-byte column descriptor, used to bound the
/// name strings without materialising a descriptor before the layout has been
/// validated.
const DESC_OFF_NAME_OFFSET: usize = std::mem::offset_of!(ColumnDescriptorRaw, name_offset);
const DESC_OFF_NAME_LENGTH: usize = std::mem::offset_of!(ColumnDescriptorRaw, name_length);

/// Bits 32-63 of `FEATURE_FLAGS` are required: a reader that does not know
/// them must reject the file.
const REQUIRED_FEATURE_MASK: u64 = 0xFFFF_FFFF_0000_0000;

// ── IndexMetaWriter ────────────────────────────────────────────────────

/// A column of the index schema: the `_pm` descriptor plus its name. The
/// writer owns `name_offset` and `name_length` and backpatches both.
struct IndexColumn {
    name: String,
    descriptor: ColumnDescriptorRaw,
}

/// Builds a complete `_im` file in memory.
pub struct IndexMetaWriter {
    payload_kind: u32,
    key_count: u32,
    key_id_column: i32,
    row_id_column: i32,
    columns: Vec<IndexColumn>,
    row_groups: Vec<(u32, RowGroupBlockBuilder)>,
    data_boundaries: Vec<i64>,
}

impl IndexMetaWriter {
    /// Creates a writer. `key_id_column` and `row_id_column` are indices into
    /// the columns added with [`IndexMetaWriter::add_column`]; `row_id_column`
    /// is `-1` under [`IM_PAYLOAD_ROW_PER_KEY`].
    pub fn new(payload_kind: u32, key_count: u32, key_id_column: i32, row_id_column: i32) -> Self {
        Self {
            payload_kind,
            key_count,
            key_id_column,
            row_id_column,
            columns: Vec::new(),
            row_groups: Vec::new(),
            data_boundaries: Vec::new(),
        }
    }

    /// Appends an index column. The descriptor is `_pm`'s 32-byte structure;
    /// `name_offset` and `name_length` are ignored and computed on write.
    ///
    /// `ID` carries the covered column's QuestDB writer index, or `-1` for the
    /// synthetic `key_id` / `row_id` columns.
    pub fn add_column(&mut self, name: &str, descriptor: ColumnDescriptorRaw) -> &mut Self {
        self.columns.push(IndexColumn {
            name: name.to_owned(),
            descriptor,
        });
        self
    }

    /// Appends one index row group: its first (smallest) key id and the block
    /// carrying one column chunk per index column.
    pub fn add_row_group(&mut self, first_key: u32, block: RowGroupBlockBuilder) -> &mut Self {
        self.row_groups.push((first_key, block));
        self
    }

    /// Appends an out-of-line stat to the most recently added row group's
    /// block, patching that column chunk's min or max stat into an
    /// `(offset << 16) | length` reference.
    ///
    /// Mirrors [`ParquetMetaWriter::add_bloom_filter_to_last_row_group`]: the
    /// JNI layer hands over a row group's 64-byte chunks in one call and then
    /// patches the stats that do not fit inline, which must happen after the
    /// chunk itself is in place.
    ///
    /// [`ParquetMetaWriter::add_bloom_filter_to_last_row_group`]: crate::writer::ParquetMetaWriter::add_bloom_filter_to_last_row_group
    pub fn add_out_of_line_stat_to_last_row_group(
        &mut self,
        col_index: usize,
        is_min: bool,
        data: &[u8],
    ) -> ParquetMetaResult<&mut Self> {
        let (_, block) = self.row_groups.last_mut().ok_or_else(|| {
            parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "no row group to add an out-of-line stat to"
            )
        })?;
        block.add_out_of_line_stat(col_index, is_min, data)?;
        Ok(self)
    }

    /// Sets `data.parquet`'s cumulative row-group boundaries. The array has
    /// `DATA_RG_COUNT + 1` entries, starts at `0` and is non-decreasing.
    pub fn set_data_row_group_boundaries(&mut self, boundaries: &[i64]) -> &mut Self {
        self.data_boundaries = boundaries.to_vec();
        self
    }

    /// Overwrites the payload kind and key count set at construction. The JNI
    /// layer creates the writer before Java knows either value, so it calls
    /// this once the index build has determined them.
    pub fn set_payload(&mut self, payload_kind: u32, key_count: u32) -> &mut Self {
        self.payload_kind = payload_kind;
        self.key_count = key_count;
        self
    }

    /// Runs the checks listed under "Validation the writer performs" in the
    /// spec. All of them are cheap here and produce silent wrong answers if
    /// they reach disk, so the writer refuses rather than trusting callers.
    fn validate(&self) -> ParquetMetaResult<()> {
        let column_count = self.columns.len();
        if column_count == 0 {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "index schema has no columns"
            ));
        }
        if self.payload_kind != IM_PAYLOAD_ROW_PER_POSTING
            && self.payload_kind != IM_PAYLOAD_ROW_PER_KEY
        {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "unknown payload kind {}",
                self.payload_kind
            ));
        }
        let key_id_column = usize::try_from(self.key_id_column)
            .ok()
            .filter(|i| *i < column_count)
            .ok_or_else(|| {
                parquet_meta_err!(
                    ParquetMetaErrorKind::InvalidValue,
                    "key id column {} out of range [0, {})",
                    self.key_id_column,
                    column_count
                )
            })?;
        // row_id is mandatory under row-per-posting: pruning by time reads its
        // chunk stats. Row-per-key has no row id at all and stores -1.
        if self.row_id_column < 0 {
            if self.payload_kind != IM_PAYLOAD_ROW_PER_KEY {
                return Err(parquet_meta_err!(
                    ParquetMetaErrorKind::InvalidValue,
                    "row id column may only be -1 under payload kind {}",
                    IM_PAYLOAD_ROW_PER_KEY
                ));
            }
        } else if self.row_id_column as usize >= column_count {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "row id column {} out of range [0, {})",
                self.row_id_column,
                column_count
            ));
        }

        if self.data_boundaries.is_empty() {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "data row group boundaries not set"
            ));
        }
        if self.data_boundaries[0] != 0 {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "first data row group boundary must be 0, got {}",
                self.data_boundaries[0]
            ));
        }
        // A binary search over a non-monotone boundary array maps row ids to
        // the wrong data row group without failing.
        for i in 1..self.data_boundaries.len() {
            if self.data_boundaries[i] < self.data_boundaries[i - 1] {
                return Err(parquet_meta_err!(
                    ParquetMetaErrorKind::InvalidValue,
                    "data row group boundaries must be non-decreasing at index {i}"
                ));
            }
        }

        for (i, (first_key, block)) in self.row_groups.iter().enumerate() {
            if block.chunks.len() != column_count {
                return Err(parquet_meta_err!(
                    ParquetMetaErrorKind::SchemaMismatch,
                    "row group {i} has {} column chunks, expected {column_count}",
                    block.chunks.len()
                ));
            }
            // A zero-row parquet row group is treated as corruption.
            if block.num_rows() == 0 {
                return Err(parquet_meta_err!(
                    ParquetMetaErrorKind::InvalidValue,
                    "row group {i} has zero rows"
                ));
            }
            if i > 0 && *first_key < self.row_groups[i - 1].0 {
                return Err(parquet_meta_err!(
                    ParquetMetaErrorKind::InvalidValue,
                    "row group first keys must be non-decreasing at index {i}"
                ));
            }
            // The dense key directory exists only because striding 64-byte
            // chunks on the lookup hot path is cache-hostile; it must agree
            // with the chunk stats it duplicates, or the fast path and the
            // slow path answer differently.
            let key_chunk = block.column_chunk_raw(key_id_column);
            // The comparison is only meaningful against an inline stat: an
            // out-of-line stat is `(offset << 16) | length`, which for a short
            // out-of-line payload near the start of the region is a small
            // integer and could collide with a small key id.
            let stat_flags = key_chunk.stat_flags();
            if !stat_flags.has_min_stat() || !stat_flags.is_min_inlined() {
                return Err(parquet_meta_err!(
                    ParquetMetaErrorKind::InvalidValue,
                    "row group {i} key id chunk min stat must be present and inline"
                ));
            }
            let min_stat = key_chunk.min_stat;
            if min_stat != *first_key as u64 {
                return Err(parquet_meta_err!(
                    ParquetMetaErrorKind::InvalidValue,
                    "row group {i} first key {} does not match key id chunk min stat {}",
                    first_key,
                    min_stat
                ));
            }
        }
        // Both readers answer "absent" for key >= KEY_COUNT, so a first key at
        // or above it would make every posting in that row group unreachable:
        // the query returns zero rows and nothing reports an error. First keys
        // are non-decreasing by the check above, so the last one bounds them all.
        if let Some((last_key, _)) = self.row_groups.last() {
            if *last_key >= self.key_count {
                return Err(parquet_meta_err!(
                    ParquetMetaErrorKind::InvalidValue,
                    "row group first key {} must be below key count {}",
                    last_key,
                    self.key_count
                ));
            }
        }
        Ok(())
    }

    /// Serialises the complete `_im` file. Takes `&self` — matching
    /// `ParquetMetaUpdateWriter::finish` — so the JNI layer can build the
    /// buffer without consuming the boxed writer that Java still owns and will
    /// later hand to `destroyWriter`.
    pub fn finish(&self) -> ParquetMetaResult<Vec<u8>> {
        self.validate()?;

        let column_count = self.columns.len() as u32;
        let index_rg_count = self.row_groups.len() as u32;
        let data_rg_count = (self.data_boundaries.len() - 1) as u32;

        let mut buf = Vec::new();
        // IM_FILE_SIZE placeholder, patched last as the commit signal.
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&IM_MAGIC.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes()); // FEATURE_FLAGS
        buf.extend_from_slice(&IM_FORMAT_VERSION.to_le_bytes());
        buf.extend_from_slice(&self.payload_kind.to_le_bytes());
        buf.extend_from_slice(&column_count.to_le_bytes());
        buf.extend_from_slice(&index_rg_count.to_le_bytes());
        buf.extend_from_slice(&data_rg_count.to_le_bytes());
        buf.extend_from_slice(&self.key_count.to_le_bytes());
        buf.extend_from_slice(&self.key_id_column.to_le_bytes());
        buf.extend_from_slice(&self.row_id_column.to_le_bytes());
        // INDEX_SECTIONS_OFFSET placeholder, backpatched once the sections are
        // laid out. Readers use it rather than deriving the position.
        buf.extend_from_slice(&0u64.to_le_bytes());
        debug_assert_eq!(buf.len(), IM_HEADER_SIZE);

        // Column descriptors, backpatched once the name offsets are known.
        let descriptors_start = buf.len();
        buf.resize(
            descriptors_start + self.columns.len() * COLUMN_DESCRIPTOR_SIZE,
            0,
        );

        // Name strings, then padding to an 8-byte boundary.
        let mut name_offsets = Vec::with_capacity(self.columns.len());
        for col in &self.columns {
            name_offsets.push(buf.len() as u64);
            buf.extend_from_slice(col.name.as_bytes());
        }
        align_to_8(&mut buf);

        for (i, col) in self.columns.iter().enumerate() {
            let mut desc = col.descriptor;
            desc.name_offset = name_offsets[i];
            desc.name_length = col.name.len() as u32;
            let desc_offset = descriptors_start + i * COLUMN_DESCRIPTOR_SIZE;
            // Safety: ColumnDescriptorRaw is #[repr(C)] and fully initialized.
            let bytes: &[u8; COLUMN_DESCRIPTOR_SIZE] = unsafe {
                &*(&desc as *const ColumnDescriptorRaw as *const [u8; COLUMN_DESCRIPTOR_SIZE])
            };
            buf[desc_offset..desc_offset + COLUMN_DESCRIPTOR_SIZE].copy_from_slice(bytes);
        }

        // Row group blocks, in `_pm`'s layout. `write_to` aligns each block to
        // 8 bytes, so the offset always survives the `>> 3` in RG_BLOCK_OFFSET.
        let mut block_offsets = Vec::with_capacity(self.row_groups.len());
        for (_, block) in &self.row_groups {
            let start = block.write_to(&mut buf);
            let shifted = u32::try_from(start >> BLOCK_ALIGNMENT_SHIFT).map_err(|_| {
                parquet_meta_err!(
                    ParquetMetaErrorKind::InvalidValue,
                    "row group block offset {} exceeds the u32 shifted range",
                    start
                )
            })?;
            block_offsets.push(shifted);
        }

        // Index sections, each 8-byte aligned. The offset of the first one is
        // recorded in the header: it cannot be derived forwards, because a
        // block's out-of-line stat region has no recorded length.
        align_to_8(&mut buf);
        let index_sections_offset = buf.len() as u64;
        buf[OFF_INDEX_SECTIONS_OFFSET..OFF_INDEX_SECTIONS_OFFSET + 8]
            .copy_from_slice(&index_sections_offset.to_le_bytes());
        for offset in &block_offsets {
            buf.extend_from_slice(&offset.to_le_bytes());
        }
        align_to_8(&mut buf);
        for (first_key, _) in &self.row_groups {
            buf.extend_from_slice(&first_key.to_le_bytes());
        }
        buf.extend_from_slice(&self.key_count.to_le_bytes()); // sentinel
        align_to_8(&mut buf);
        for boundary in &self.data_boundaries {
            buf.extend_from_slice(&boundary.to_le_bytes());
        }

        let crc_end = buf.len();
        buf.extend_from_slice(&0u32.to_le_bytes());
        let total = buf.len() as u64;
        buf[OFF_IM_FILE_SIZE..OFF_IM_FILE_SIZE + 8].copy_from_slice(&total.to_le_bytes());
        // The CRC starts at 8, so IM_FILE_SIZE is deliberately outside it: the
        // commit patch must not invalidate the checksum.
        let crc = crc32fast::hash(&buf[IM_CRC_AREA_OFF..crc_end]);
        buf[crc_end..crc_end + 4].copy_from_slice(&crc.to_le_bytes());
        Ok(buf)
    }
}

fn align_to_8(buf: &mut Vec<u8>) {
    let padding = (BLOCK_ALIGNMENT - (buf.len() % BLOCK_ALIGNMENT)) % BLOCK_ALIGNMENT;
    buf.extend(std::iter::repeat_n(0u8, padding));
}

/// Byte footprint of a section of `size` bytes that starts 8-byte aligned and
/// is followed by another 8-byte aligned section.
fn aligned_footprint(size: usize) -> Option<usize> {
    size.checked_add(BLOCK_ALIGNMENT - 1)
        .map(|v| v & !(BLOCK_ALIGNMENT - 1))
}

// ── IndexMetaReader ────────────────────────────────────────────────────

/// Zero-copy reader over a complete, committed `_im` buffer.
///
/// Callers map exactly `IM_FILE_SIZE` bytes: the on-disk length may include
/// bytes from an in-progress write and is not a commit boundary.
#[derive(Debug)]
pub struct IndexMetaReader<'a> {
    /// Truncated to `IM_FILE_SIZE`; nothing past the committed size is visible.
    data: &'a [u8],
    im_file_size: u64,
    feature_flags: u64,
    payload_kind: u32,
    column_count: u32,
    index_rg_count: usize,
    data_rg_count: usize,
    key_count: u32,
    key_id_column: i32,
    row_id_column: i32,
    names_start: usize,
    /// The header's `INDEX_SECTIONS_OFFSET`, validated at construction. It
    /// doubles as the exclusive upper bound of the row group block region.
    rg_block_offset_off: usize,
    rg_first_key_off: usize,
    data_boundary_off: usize,
}

impl<'a> IndexMetaReader<'a> {
    pub fn new(data: &'a [u8]) -> ParquetMetaResult<Self> {
        if data.len() < IM_HEADER_SIZE + IM_TRAILER_SIZE {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "buffer of {} bytes is too small for an _im header",
                data.len()
            ));
        }
        let im_file_size = read_u64(data, OFF_IM_FILE_SIZE);
        let end = usize::try_from(im_file_size).unwrap_or(usize::MAX);
        if end > data.len() || end < IM_HEADER_SIZE + IM_TRAILER_SIZE {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "IM_FILE_SIZE {} is outside the {}-byte buffer",
                im_file_size,
                data.len()
            ));
        }
        let data = &data[..end];

        let magic = read_u64(data, OFF_IM_MAGIC);
        if magic != IM_MAGIC {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "bad _im magic: found 0x{magic:016X}, expected 0x{IM_MAGIC:016X}"
            ));
        }
        let version = read_u32(data, OFF_FORMAT_VERSION);
        if version != IM_FORMAT_VERSION {
            return Err(parquet_meta_err!(ParquetMetaErrorKind::VersionMismatch {
                found: version,
                expected: IM_FORMAT_VERSION,
            }));
        }
        let feature_flags = read_u64(data, OFF_FEATURE_FLAGS);
        let unknown_required = feature_flags & REQUIRED_FEATURE_MASK;
        if unknown_required != 0 {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::UnsupportedFeature {
                    flags: unknown_required
                }
            ));
        }

        // Nothing below this point may be trusted until the CRC agrees.
        let crc_off = end - IM_TRAILER_SIZE;
        let stored = read_u32(data, crc_off);
        let computed = crc32fast::hash(&data[IM_CRC_AREA_OFF..crc_off]);
        if stored != computed {
            return Err(parquet_meta_err!(ParquetMetaErrorKind::ChecksumMismatch {
                stored,
                computed
            }));
        }

        let column_count = read_u32(data, OFF_COLUMN_COUNT);
        let index_rg_count = read_u32(data, OFF_INDEX_RG_COUNT) as usize;
        let data_rg_count = read_u32(data, OFF_DATA_RG_COUNT) as usize;

        // The header records where the index sections start; a reader never
        // derives it. What it must do is validate the value against everything
        // else the header claims, with every step checked: the counts and the
        // offset come straight off a file that may be crafted, so an unchecked
        // product or sum wraps and the sections land inside the header or past
        // the end.
        let index_sections_offset = read_u64(data, OFF_INDEX_SECTIONS_OFFSET);
        let truncated = || {
            parquet_meta_err!(ParquetMetaErrorKind::Truncated, "_im sections at offset {} do not fit: column_count {}, index_rg_count {}, data_rg_count {}, size {}", index_sections_offset, column_count, index_rg_count, data_rg_count, end)
        };
        // Each section starts 8-byte aligned, so the first one must be too.
        if !index_sections_offset.is_multiple_of(BLOCK_ALIGNMENT as u64) {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Alignment,
                "index sections offset {} is not 8-byte aligned",
                index_sections_offset
            ));
        }
        let rg_block_offset_off =
            usize::try_from(index_sections_offset).map_err(|_| truncated())?;

        // The sections start at or after the column descriptors and the name
        // strings they point at.
        let names_start = (column_count as usize)
            .checked_mul(COLUMN_DESCRIPTOR_SIZE)
            .and_then(|n| n.checked_add(IM_HEADER_SIZE))
            .ok_or_else(truncated)?;
        if names_start > rg_block_offset_off {
            return Err(truncated());
        }
        // Descriptors are in bounds now, so their name entries can be read to
        // bound the end of the name blob.
        for i in 0..column_count as usize {
            let desc_off = IM_HEADER_SIZE + i * COLUMN_DESCRIPTOR_SIZE;
            let name_off = usize::try_from(read_u64(data, desc_off + DESC_OFF_NAME_OFFSET))
                .unwrap_or(usize::MAX);
            let name_end = name_off
                .checked_add(read_u32(data, desc_off + DESC_OFF_NAME_LENGTH) as usize)
                .ok_or_else(truncated)?;
            if name_off < names_start || name_end > rg_block_offset_off {
                return Err(truncated());
            }
        }

        // The three sections, sized from the header counts, each padded up so
        // the next starts 8-byte aligned, must fit ahead of the CRC.
        let block_offset_bytes = index_rg_count
            .checked_mul(4)
            .and_then(aligned_footprint)
            .ok_or_else(truncated)?;
        let rg_first_key_off = rg_block_offset_off
            .checked_add(block_offset_bytes)
            .ok_or_else(truncated)?;
        let first_key_bytes = index_rg_count
            .checked_add(1)
            .and_then(|n| n.checked_mul(4))
            .and_then(aligned_footprint)
            .ok_or_else(truncated)?;
        let data_boundary_off = rg_first_key_off
            .checked_add(first_key_bytes)
            .ok_or_else(truncated)?;
        let boundary_bytes = data_rg_count
            .checked_add(1)
            .and_then(|n| n.checked_mul(8))
            .ok_or_else(truncated)?;
        let sections_end = data_boundary_off
            .checked_add(boundary_bytes)
            .ok_or_else(truncated)?;
        if sections_end > crc_off {
            return Err(truncated());
        }

        // The header's column selectors are trusted all the way to an address
        // computation: `KEY_ID_COLUMN` is the only sanctioned route to the
        // synthetic `key_id` column, so a caller hands it straight to a column
        // chunk accessor. Validating them here, at open, is what keeps that
        // call safe - and the Java reader, where an unchecked selector is a
        // wild address rather than an error, validates them at the same point.
        let payload_kind = read_u32(data, OFF_PAYLOAD_KIND);
        if payload_kind != IM_PAYLOAD_ROW_PER_POSTING && payload_kind != IM_PAYLOAD_ROW_PER_KEY {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "unknown _im payload kind {payload_kind}"
            ));
        }
        let key_id_column = read_u32(data, OFF_KEY_ID_COLUMN) as i32;
        if key_id_column < 0 || key_id_column as u32 >= column_count {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "_im key id column {} out of range [0, {})",
                key_id_column,
                column_count
            ));
        }
        // `ROW_ID_COLUMN` is `-1` exactly under row-per-key: that payload has
        // no row id column at all, and row-per-posting prunes by time through
        // the chunk stats of the column this names. Any other negative value
        // is rejected under both kinds - it is neither the sentinel nor an
        // index.
        let row_id_column = read_u32(data, OFF_ROW_ID_COLUMN) as i32;
        let row_id_column_valid = if payload_kind == IM_PAYLOAD_ROW_PER_KEY {
            row_id_column == -1
        } else {
            row_id_column >= 0 && (row_id_column as u32) < column_count
        };
        if !row_id_column_valid {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "_im row id column {} is invalid under payload kind {}, column count {}",
                row_id_column,
                payload_kind,
                column_count
            ));
        }

        // A block's extent comes from the next entry of RG_BLOCK_OFFSET, so
        // the array must ascend: an entry that does not leaves a block with an
        // empty or inverted extent and makes every out-of-line stat bound
        // derived from it meaningless. Rejecting here rather than on first
        // access is what makes every later extent computation trustworthy, and
        // both reader implementations reject the same files.
        let mut previous = -1i64;
        for i in 0..index_rg_count {
            let entry = read_u32(data, rg_block_offset_off + i * 4) as i64;
            if entry <= previous {
                return Err(parquet_meta_err!(
                    ParquetMetaErrorKind::InvalidValue,
                    "_im RG_BLOCK_OFFSET entries must ascend: row group {i} entry {entry} is not above {previous}"
                ));
            }
            previous = entry;
        }

        Ok(Self {
            data,
            im_file_size,
            feature_flags,
            payload_kind,
            column_count,
            index_rg_count,
            data_rg_count,
            key_count: read_u32(data, OFF_KEY_COUNT),
            key_id_column,
            row_id_column,
            names_start,
            rg_block_offset_off,
            rg_first_key_off,
            data_boundary_off,
        })
    }

    /// Total committed file size, as patched by the writer.
    pub fn im_file_size(&self) -> u64 {
        self.im_file_size
    }

    pub fn feature_flags(&self) -> u64 {
        self.feature_flags
    }

    /// `0` = row-per-posting, `1` = row-per-key.
    pub fn payload_kind(&self) -> u32 {
        self.payload_kind
    }

    pub fn column_count(&self) -> u32 {
        self.column_count
    }

    pub fn index_row_group_count(&self) -> usize {
        self.index_rg_count
    }

    pub fn data_row_group_count(&self) -> usize {
        self.data_rg_count
    }

    pub fn key_count(&self) -> u32 {
        self.key_count
    }

    /// Index of the synthetic `key_id` column in the descriptors.
    pub fn key_id_column(&self) -> i32 {
        self.key_id_column
    }

    /// Index of the synthetic `row_id` column, or `-1` under payload kind 1.
    pub fn row_id_column(&self) -> i32 {
        self.row_id_column
    }

    /// Absolute file offset of the first index section (`RG_BLOCK_OFFSET`), as
    /// recorded in the header and validated at construction.
    pub fn index_sections_offset(&self) -> u64 {
        self.rg_block_offset_off as u64
    }

    /// Returns the column descriptor at `index`, zero-copy.
    pub fn column_descriptor(&self, index: usize) -> ParquetMetaResult<&'a ColumnDescriptorRaw> {
        if index >= self.column_count as usize {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "column index {} out of range [0, {})",
                index,
                self.column_count
            ));
        }
        let offset = IM_HEADER_SIZE + index * COLUMN_DESCRIPTOR_SIZE;
        let ptr = self.data[offset..].as_ptr();
        // Safety: ColumnDescriptorRaw is #[repr(C)] with 8-byte max alignment,
        // the header is 64 bytes and descriptors are 32 bytes, so every
        // descriptor offset is 8-byte aligned. The bounds were checked above
        // and again against `names_start` in `new`.
        debug_assert_eq!(ptr.align_offset(align_of::<ColumnDescriptorRaw>()), 0);
        Ok(unsafe { &*(ptr as *const ColumnDescriptorRaw) })
    }

    /// Returns the UTF-8 name of the column at `index`.
    pub fn column_name(&self, index: usize) -> ParquetMetaResult<&'a str> {
        let desc = self.column_descriptor(index)?;
        let start = usize::try_from(desc.name_offset).map_err(|_| {
            parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "name offset exceeds addressable range at column {}",
                index
            )
        })?;
        let end = start
            .checked_add(desc.name_length as usize)
            .ok_or_else(|| {
                parquet_meta_err!(
                    ParquetMetaErrorKind::Truncated,
                    "name entry overflow at column {}",
                    index
                )
            })?;
        if start < self.names_start || end > self.rg_block_offset_off {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "name entry out of bounds at column {}: offset {} length {}",
                index,
                start,
                desc.name_length
            ));
        }
        std::str::from_utf8(&self.data[start..end]).map_err(|_| {
            parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "column {} name is not valid UTF-8",
                index
            )
        })
    }

    /// Returns the index of the column whose descriptor `ID` matches the given
    /// QuestDB writer index, which is how `requiredCoverColumns` becomes a
    /// parquet column projection.
    ///
    /// A negative `ID` is not a lookup key and always misses: `-1` is the
    /// descriptor sentinel for the synthetic `key_id` and `row_id` columns,
    /// and matching it here would hand back the first synthetic column instead.
    /// Those two are reached through the header's `KEY_ID_COLUMN` and
    /// `ROW_ID_COLUMN`, which is the only sanctioned route. This matches
    /// `ParquetMetaFileReader.getColumnIndexById`.
    pub fn column_index_by_id(&self, id: i32) -> ParquetMetaResult<Option<usize>> {
        if id < 0 {
            return Ok(None);
        }
        for i in 0..self.column_count as usize {
            if self.column_descriptor(i)?.id == id {
                return Ok(Some(i));
            }
        }
        Ok(None)
    }

    /// Half-open byte extent `[start, end)` of the row group block at `index`.
    ///
    /// Block `i` runs from `RG_BLOCK_OFFSET[i]` to `RG_BLOCK_OFFSET[i + 1]`,
    /// and the last block runs to `INDEX_SECTIONS_OFFSET`. The entries ascend,
    /// which `new` rejected the file for otherwise, so `start < end` holds for
    /// every block but the last, whose end is checked here.
    fn row_group_block_extent(&self, index: usize) -> ParquetMetaResult<(usize, usize)> {
        if index >= self.index_rg_count {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "row group index {} out of range [0, {})",
                index,
                self.index_rg_count
            ));
        }
        let block_offset = |i: usize| {
            (read_u32(self.data, self.rg_block_offset_off + i * 4) as u64) << BLOCK_ALIGNMENT_SHIFT
        };
        let bound = self.rg_block_offset_off as u64;
        let start = block_offset(index);
        let end = if index + 1 < self.index_rg_count {
            block_offset(index + 1)
        } else {
            bound
        };
        if start < self.names_start as u64 || end > bound || start > end {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "row group {} block extent [{}, {}) is outside the block region [{}, {})",
                index,
                start,
                end,
                self.names_start,
                self.rg_block_offset_off
            ));
        }
        Ok((start as usize, end as usize))
    }

    /// Returns a reader over the row group block at `index`, located through
    /// `RG_BLOCK_OFFSET` and bounded by the block's own extent, so a block can
    /// never read the bytes of the one after it.
    pub fn row_group_block(&self, index: usize) -> ParquetMetaResult<RowGroupBlockReader<'a>> {
        let (start, end) = self.row_group_block_extent(index)?;
        RowGroupBlockReader::new(&self.data[start..end], self.column_count)
    }

    /// Resolves a column chunk's out-of-line min or max statistic, the
    /// `(offset << 16) | length` reference the chunk carries when the payload
    /// exceeds the 8 inline bytes, to the bytes it addresses.
    ///
    /// The reference is relative to the block's out-of-line region and is
    /// **bounded by the block's own extent**: a reference reaching into
    /// another row group's block, or past the index sections, is rejected
    /// rather than resolved to a silently wrong stat value. Stats drive query
    /// pruning, so a wrong one is a wrong answer, not a decode failure.
    pub fn out_of_line_stat(
        &self,
        row_group: usize,
        column: usize,
        is_min: bool,
    ) -> ParquetMetaResult<&'a [u8]> {
        let block = self.row_group_block(row_group)?;
        let chunk = block.column_chunk(column)?;
        let stat_flags = chunk.stat_flags();
        debug_assert!(
            if is_min {
                stat_flags.has_min_stat() && !stat_flags.is_min_inlined()
            } else {
                stat_flags.has_max_stat() && !stat_flags.is_max_inlined()
            },
            "stat absent or inlined for row group {row_group}, column {column}"
        );
        let encoded = if is_min {
            chunk.min_stat
        } else {
            chunk.max_stat
        };
        let region = block.out_of_line_region();
        let region_size = region.len() as u64;
        let stat_offset = encoded >> 16;
        let stat_length = encoded & 0xFFFF;
        if stat_offset > region_size || stat_length > region_size - stat_offset {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "_im out of line stat out of bounds: row group {}, column {}, offset {}, length {}, region size {}",
                row_group,
                column,
                stat_offset,
                stat_length,
                region_size
            ));
        }
        let start = stat_offset as usize;
        Ok(&region[start..start + stat_length as usize])
    }

    /// The smallest key id present in row group `index`. Index
    /// `INDEX_RG_COUNT` is the sentinel and equals `KEY_COUNT`.
    pub fn row_group_first_key(&self, index: usize) -> ParquetMetaResult<u32> {
        if index > self.index_rg_count {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "first key index {} out of range [0, {}]",
                index,
                self.index_rg_count
            ));
        }
        Ok(read_u32(self.data, self.rg_first_key_off + index * 4))
    }

    /// Cumulative row count at `data.parquet` row group boundary `index`.
    /// The array has `DATA_RG_COUNT + 1` entries.
    pub fn data_row_group_boundary(&self, index: usize) -> ParquetMetaResult<i64> {
        if index > self.data_rg_count {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "data boundary index {} out of range [0, {}]",
                index,
                self.data_rg_count
            ));
        }
        Ok(read_u64(self.data, self.data_boundary_off + index * 8) as i64)
    }

    /// Inclusive `(rg_lo, rg_hi)` range of index row groups holding `key`, or
    /// `None` when `key` is outside the covered key space. The range is
    /// contiguous, so a key's postings are one byte range per column.
    pub fn row_group_range_for_key(&self, key: u32) -> Option<(usize, usize)> {
        if key >= self.key_count || self.index_rg_count == 0 {
            return None;
        }
        // Bounded at INDEX_RG_COUNT: the sentinel is never read by the search.
        let n = self.index_rg_count;
        let first_key = |i: usize| read_u32(self.data, self.rg_first_key_off + i * 4);

        let mut lo = 0usize;
        let mut hi = n;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if first_key(mid) < key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let rg_lo = if lo < n && first_key(lo) == key {
            lo
        } else if lo == 0 {
            // The key sorts below the first row group's first key, so no row
            // group can hold it.
            return None;
        } else {
            lo - 1
        };

        let mut ulo = 0usize;
        let mut uhi = n;
        while ulo < uhi {
            let mid = ulo + (uhi - ulo) / 2;
            if first_key(mid) <= key {
                ulo = mid + 1;
            } else {
                uhi = mid;
            }
        }
        Some((rg_lo, ulo - 1))
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

    /// Encodes an out-of-line stat reference the way a column chunk carries it:
    /// `(offset << 16) | length`, relative to the row group block. Written as a
    /// helper rather than inline arithmetic so an offset of `0` still reads as a
    /// reference rather than collapsing to a bare length.
    fn ool_ref(offset: u64, length: u64) -> u64 {
        (offset << 16) | length
    }
    use crate::column_chunk::ColumnChunkRaw;
    use crate::types::{encode_stat_sizes, Codec, ColumnFlags, EncodingMask, StatFlags};

    // Absolute layout of `build_two_block_out_of_line_sample`, pinned by
    // `test_two_block_out_of_line_sample_layout` so the crafted offsets below
    // keep addressing what they are meant to address.
    const TWO_BLOCK_0_OFF: usize = 176;
    const TWO_BLOCK_1_OFF: usize = TWO_BLOCK_0_OFF + TWO_BLOCK_SIZE;
    const TWO_BLOCK_FILE_LEN: usize = 684;
    const TWO_BLOCK_MAX_FILL: [u8; 2] = [0xEE, 0xDD];
    const TWO_BLOCK_MIN_FILL: [u8; 2] = [0x11, 0x22];
    /// Out-of-line region of each block: a 16-byte min followed by a 16-byte
    /// max, the second of which ends exactly at the block's end.
    const TWO_BLOCK_OOL_SIZE: u64 = 32;
    const TWO_BLOCK_SECTIONS_OFF: usize = TWO_BLOCK_1_OFF + TWO_BLOCK_SIZE;
    /// 8 NUM_ROWS + 3 chunks of 64 + a 32-byte out-of-line region.
    const TWO_BLOCK_SIZE: usize = 8 + 3 * 64 + 32;
    /// MAX_STAT of the `uid` chunk, relative to the block start: past
    /// NUM_ROWS and the two preceding chunks, then 56 into the chunk.
    const TWO_BLOCK_UID_MAX_STAT: usize = 8 + 2 * 64 + 56;

    // QuestDB column type tags, spelled out so the fixtures do not depend on
    // qdb-core's enum ordering.
    const TYPE_INT: i32 = 5;
    const TYPE_LONG: i32 = 6;
    const TYPE_DOUBLE: i32 = 10;
    const TYPE_UUID: i32 = 19;

    fn descriptor(id: i32, col_type: i32) -> ColumnDescriptorRaw {
        ColumnDescriptorRaw {
            name_offset: 0,
            id,
            col_type,
            flags: ColumnFlags::new().0,
            fixed_byte_len: 0,
            name_length: 0,
            physical_type: 1,
            max_rep_level: 0,
            max_def_level: 1,
            _reserved: 0,
        }
    }

    /// A `key_id` chunk whose MIN_STAT is the row group's first key, as the
    /// redundancy invariant requires.
    fn key_id_chunk(first_key: u32, last_key: u32, rows: u64) -> ColumnChunkRaw {
        let mut c = ColumnChunkRaw::zeroed();
        c.codec = Codec::Zstd as u8;
        c.encodings = EncodingMask::RLE_DICTIONARY;
        c.stat_flags = StatFlags::new()
            .with_min(true, true)
            .with_max(true, true)
            .with_null_count()
            .0;
        c.stat_sizes = encode_stat_sizes(4, 4);
        c.num_values = rows;
        c.min_stat = first_key as u64;
        c.max_stat = last_key as u64;
        c
    }

    fn row_id_chunk(min: i64, max: i64, rows: u64) -> ColumnChunkRaw {
        let mut c = ColumnChunkRaw::zeroed();
        c.codec = Codec::Zstd as u8;
        c.encodings = EncodingMask::DELTA_BINARY_PACKED;
        c.stat_flags = StatFlags::new().with_min(true, true).with_max(true, true).0;
        c.stat_sizes = encode_stat_sizes(8, 8);
        c.num_values = rows;
        c.min_stat = min as u64;
        c.max_stat = max as u64;
        c
    }

    /// The fixture the absolute byte-offset assertions pin.
    ///
    /// 3 columns with names totalling 17 bytes, so the name section needs 7
    /// bytes of padding, and 4 row groups, so RG_BLOCK_OFFSET is already
    /// 8-aligned while RG_FIRST_KEY needs 4 bytes of padding.
    fn sample_writer() -> IndexMetaWriter {
        let mut w = IndexMetaWriter::new(IM_PAYLOAD_ROW_PER_POSTING, 11_405, 0, 1);
        w.add_column("key_id", descriptor(-1, TYPE_INT));
        w.add_column("row_id", descriptor(-1, TYPE_LONG));
        w.add_column("price", descriptor(7, TYPE_DOUBLE));

        let specs: [(u32, u32, i64, i64, u64); 4] = [
            (0, 11_402, 0, 99_999, 100_000),
            (11_403, 11_403, 100_000, 157_999, 58_000),
            (11_403, 11_403, 158_000, 240_000, 82_001),
            (11_404, 11_404, 240_001, 999_999, 759_999),
        ];
        for (i, (first_key, last_key, row_min, row_max, rows)) in specs.iter().enumerate() {
            let mut block = RowGroupBlockBuilder::new(3);
            block.set_num_rows(*rows);
            block
                .set_column_chunk(0, key_id_chunk(*first_key, *last_key, *rows))
                .unwrap();
            block
                .set_column_chunk(1, row_id_chunk(*row_min, *row_max, *rows))
                .unwrap();

            // Fully populated covered-column chunk: every field carries a
            // distinct value so the round trip pins all of them.
            let mut price = ColumnChunkRaw::zeroed();
            price.codec = Codec::Snappy as u8;
            price.encodings = EncodingMask::PLAIN | EncodingMask::BYTE_STREAM_SPLIT;
            price.stat_flags = StatFlags::new()
                .with_min(true, true)
                .with_max(true, false)
                .with_null_count()
                .with_distinct_count()
                .0;
            price.stat_sizes = encode_stat_sizes(8, 8);
            price.num_values = *rows;
            price.byte_range_start = 4_096 + i as u64 * 1_000;
            price.total_compressed = 512 + i as u64;
            price.null_count = i as u64;
            price.distinct_count = 40 + i as u64;
            price.min_stat = 100 + i as u64;
            price.max_stat = 900 + i as u64;
            block.set_column_chunk(2, price).unwrap();

            w.add_row_group(*first_key, block);
        }
        w.set_data_row_group_boundaries(&[0, 500_000, 1_000_000]);
        w
    }

    fn build_sample() -> Vec<u8> {
        sample_writer().finish().unwrap()
    }

    /// The complementary alignment case: names total 16 bytes so the name
    /// section needs no padding, and 3 row groups make RG_BLOCK_OFFSET the
    /// padded section and RG_FIRST_KEY the aligned one.
    fn build_aligned_sample() -> Vec<u8> {
        let mut w = IndexMetaWriter::new(IM_PAYLOAD_ROW_PER_POSTING, 900, 0, 1);
        w.add_column("key_id", descriptor(-1, TYPE_INT));
        w.add_column("row_id", descriptor(-1, TYPE_LONG));
        w.add_column("pxpx", descriptor(3, TYPE_DOUBLE));
        for (i, first_key) in [0u32, 300, 700].iter().enumerate() {
            let mut block = RowGroupBlockBuilder::new(3);
            block.set_num_rows(100);
            block
                .set_column_chunk(0, key_id_chunk(*first_key, *first_key + 99, 100))
                .unwrap();
            block
                .set_column_chunk(1, row_id_chunk(i as i64 * 100, i as i64 * 100 + 99, 100))
                .unwrap();
            block.set_column_chunk(2, ColumnChunkRaw::zeroed()).unwrap();
            w.add_row_group(*first_key, block);
        }
        w.set_data_row_group_boundaries(&[0, 150, 300]);
        w.finish().unwrap()
    }

    /// Two row groups, each carrying a 16-byte out-of-line min and a 16-byte
    /// out-of-line max for a covered UUID column, so every block has a
    /// 32-byte out-of-line region and the last stat of a block ends exactly at
    /// that block's end. That is what makes the per-block bound testable: an
    /// off-by-one loosening lets block 0 address block 1.
    ///
    /// Layout, pinned by `TWO_BLOCK_*` below: header 64, descriptors 96, the
    /// names "key_idrow_iduid" 15 bytes padded to 16, then two blocks of
    /// 8 + 3 * 64 + 32 = 232 bytes each, then the index sections.
    fn build_two_block_out_of_line_sample() -> Vec<u8> {
        let mut w = IndexMetaWriter::new(IM_PAYLOAD_ROW_PER_POSTING, 50, 0, 1);
        w.add_column("key_id", descriptor(-1, TYPE_INT));
        w.add_column("row_id", descriptor(-1, TYPE_LONG));
        w.add_column("uid", descriptor(4, TYPE_UUID));
        for (i, first_key) in [7u32, 20].iter().enumerate() {
            let mut block = RowGroupBlockBuilder::new(3);
            block.set_num_rows(64);
            block
                .set_column_chunk(0, key_id_chunk(*first_key, *first_key, 64))
                .unwrap();
            block
                .set_column_chunk(1, row_id_chunk(i as i64 * 64, i as i64 * 64 + 63, 64))
                .unwrap();
            let mut uid = ColumnChunkRaw::zeroed();
            uid.codec = Codec::Zstd as u8;
            uid.stat_flags = StatFlags::new()
                .with_min(false, true)
                .with_max(false, true)
                .0;
            uid.num_values = 64;
            block.set_column_chunk(2, uid).unwrap();
            block
                .add_out_of_line_stat(2, true, &[TWO_BLOCK_MIN_FILL[i]; 16])
                .unwrap();
            block
                .add_out_of_line_stat(2, false, &[TWO_BLOCK_MAX_FILL[i]; 16])
                .unwrap();
            w.add_row_group(*first_key, block);
        }
        w.set_data_row_group_boundaries(&[0, 128]);
        w.finish().unwrap()
    }

    /// A minimal valid writer used by the validation tests, which then break
    /// exactly one invariant each.
    fn minimal_writer() -> IndexMetaWriter {
        let mut w = IndexMetaWriter::new(IM_PAYLOAD_ROW_PER_POSTING, 100, 0, 1);
        w.add_column("key_id", descriptor(-1, TYPE_INT));
        w.add_column("row_id", descriptor(-1, TYPE_LONG));
        w.set_data_row_group_boundaries(&[0, 200]);
        w
    }

    fn minimal_block(first_key: u32, rows: u64) -> RowGroupBlockBuilder {
        let mut block = RowGroupBlockBuilder::new(2);
        block.set_num_rows(rows);
        block
            .set_column_chunk(0, key_id_chunk(first_key, first_key, rows))
            .unwrap();
        block
            .set_column_chunk(1, row_id_chunk(0, 99, rows))
            .unwrap();
        block
    }

    /// Overwrites a field and repairs the CRC trailer, so the reader reaches
    /// the check under test instead of failing the checksum first.
    fn patch_u32(bytes: &mut [u8], off: usize, value: u32) {
        bytes[off..off + 4].copy_from_slice(&value.to_le_bytes());
        repair_crc(bytes);
    }

    fn patch_u64(bytes: &mut [u8], off: usize, value: u64) {
        bytes[off..off + 8].copy_from_slice(&value.to_le_bytes());
        repair_crc(bytes);
    }

    fn repair_crc(bytes: &mut [u8]) {
        let crc_end = bytes.len() - IM_TRAILER_SIZE;
        let crc = crc32fast::hash(&bytes[IM_CRC_AREA_OFF..crc_end]);
        bytes[crc_end..crc_end + 4].copy_from_slice(&crc.to_le_bytes());
    }

    // ── Round trip ─────────────────────────────────────────────────────

    #[test]
    fn test_round_trip_header_fields() {
        let bytes = build_sample();
        let r = IndexMetaReader::new(&bytes).unwrap();
        assert_eq!(r.im_file_size(), bytes.len() as u64);
        assert_eq!(read_u64(&bytes, OFF_IM_MAGIC), IM_MAGIC);
        assert_eq!(read_u32(&bytes, OFF_FORMAT_VERSION), IM_FORMAT_VERSION);
        assert_eq!(r.feature_flags(), 0);
        assert_eq!(r.payload_kind(), IM_PAYLOAD_ROW_PER_POSTING);
        assert_eq!(r.column_count(), 3);
        assert_eq!(r.index_row_group_count(), 4);
        assert_eq!(r.data_row_group_count(), 2);
        assert_eq!(r.key_count(), 11_405);
        assert_eq!(r.key_id_column(), 0);
        assert_eq!(r.row_id_column(), 1);
        // The header points at the sections; nothing is inferred.
        assert_eq!(r.index_sections_offset(), 984);
        assert_eq!(read_u64(&bytes, OFF_INDEX_SECTIONS_OFFSET), 984);
    }

    #[test]
    fn test_round_trip_descriptors_and_names() {
        let bytes = build_sample();
        let r = IndexMetaReader::new(&bytes).unwrap();

        let key_id = r.column_descriptor(0).unwrap();
        assert_eq!(key_id.id, -1);
        assert_eq!(key_id.col_type, TYPE_INT);
        assert_eq!(key_id.name_length, 6);
        assert_eq!(key_id.physical_type, 1);
        assert_eq!(key_id.max_rep_level, 0);
        assert_eq!(key_id.max_def_level, 1);
        assert_eq!(key_id.fixed_byte_len, 0);
        assert_eq!(key_id.flags, 0);
        assert_eq!(key_id._reserved, 0);
        assert_eq!(r.column_name(0).unwrap(), "key_id");

        assert_eq!(r.column_descriptor(1).unwrap().col_type, TYPE_LONG);
        assert_eq!(r.column_name(1).unwrap(), "row_id");

        let price = r.column_descriptor(2).unwrap();
        assert_eq!(price.id, 7);
        assert_eq!(price.col_type, TYPE_DOUBLE);
        assert_eq!(r.column_name(2).unwrap(), "price");

        // The covered column's writer index is what maps a required cover
        // column to a parquet projection.
        assert_eq!(r.column_index_by_id(7).unwrap(), Some(2));
        assert_eq!(r.column_index_by_id(99).unwrap(), None);
        // -1 is the synthetic columns' sentinel, not a lookup key: it must
        // miss rather than return the first of them. key_id and row_id are
        // reached through the header instead.
        assert_eq!(r.column_index_by_id(-1).unwrap(), None);
        assert_eq!(r.column_index_by_id(i32::MIN).unwrap(), None);
        assert_eq!(r.key_id_column(), 0);
        assert_eq!(r.row_id_column(), 1);
        assert!(r.column_descriptor(3).is_err());
        assert!(r.column_name(3).is_err());
    }

    #[test]
    fn test_round_trip_column_chunks() {
        let bytes = build_sample();
        let r = IndexMetaReader::new(&bytes).unwrap();

        let block = r.row_group_block(2).unwrap();
        assert_eq!(block.num_rows(), 82_001);

        let key_id = block.column_chunk(0).unwrap();
        assert_eq!(key_id.codec().unwrap(), Codec::Zstd);
        assert!(key_id.encodings().has_rle_dictionary());
        assert_eq!(key_id.stat_sizes(), (4, 4));
        assert_eq!(key_id.min_stat, 11_403);
        assert_eq!(key_id.max_stat, 11_403);

        let row_id = block.column_chunk(1).unwrap();
        assert!(row_id.encodings().has_delta_binary_packed());
        assert_eq!(row_id.min_stat as i64, 158_000);
        assert_eq!(row_id.max_stat as i64, 240_000);

        let price = block.column_chunk(2).unwrap();
        assert_eq!(price.codec().unwrap(), Codec::Snappy);
        assert!(price.encodings().has_plain());
        assert!(price.encodings().has_byte_stream_split());
        let flags = price.stat_flags();
        assert!(flags.has_min_stat() && flags.is_min_inlined() && flags.is_min_exact());
        assert!(flags.has_max_stat() && flags.is_max_inlined() && !flags.is_max_exact());
        assert!(flags.has_null_count() && flags.has_distinct_count());
        assert_eq!(price.stat_sizes(), (8, 8));
        assert_eq!(price.num_values, 82_001);
        assert_eq!(price.byte_range_start, 6_096);
        assert_eq!(price.total_compressed, 514);
        assert_eq!(price.null_count, 2);
        assert_eq!(price.distinct_count, 42);
        assert_eq!(price.min_stat, 102);
        assert_eq!(price.max_stat, 902);
        assert_eq!(price._reserved, 0);

        assert!(r.row_group_block(4).is_err());
    }

    #[test]
    fn test_round_trip_index_sections() {
        let bytes = build_sample();
        let r = IndexMetaReader::new(&bytes).unwrap();

        // RG_BLOCK_OFFSET resolves each block, and every block's key id chunk
        // agrees with the key directory.
        for i in 0..r.index_row_group_count() {
            let first_key = r.row_group_first_key(i).unwrap();
            let block = r.row_group_block(i).unwrap();
            assert_eq!(block.column_chunk(0).unwrap().min_stat, first_key as u64);
        }
        assert_eq!(r.row_group_first_key(0).unwrap(), 0);
        assert_eq!(r.row_group_first_key(1).unwrap(), 11_403);
        assert_eq!(r.row_group_first_key(2).unwrap(), 11_403);
        assert_eq!(r.row_group_first_key(3).unwrap(), 11_404);
        // The sentinel is KEY_COUNT.
        assert_eq!(r.row_group_first_key(4).unwrap(), 11_405);
        assert!(r.row_group_first_key(5).is_err());

        assert_eq!(r.data_row_group_boundary(0).unwrap(), 0);
        assert_eq!(r.data_row_group_boundary(1).unwrap(), 500_000);
        assert_eq!(r.data_row_group_boundary(2).unwrap(), 1_000_000);
        assert!(r.data_row_group_boundary(3).is_err());
    }

    // ── Absolute byte layout ───────────────────────────────────────────

    /// Pins every section's absolute offset so a future edit cannot shift one
    /// undetected. Names total 17 bytes (padded to 24) and the row group count
    /// is even, so RG_BLOCK_OFFSET lands 8-aligned and RG_FIRST_KEY is padded.
    #[test]
    fn test_absolute_byte_layout_with_padded_name_section() {
        let bytes = build_sample();
        assert_eq!(bytes.len(), 1_052);
        assert_eq!(read_u64(&bytes, 0), 1_052); // IM_FILE_SIZE
        assert_eq!(read_u64(&bytes, 8), IM_MAGIC);
        assert_eq!(read_u64(&bytes, 16), 0); // FEATURE_FLAGS
        assert_eq!(read_u32(&bytes, 24), 2); // FORMAT_VERSION
        assert_eq!(read_u32(&bytes, 28), 0); // PAYLOAD_KIND
        assert_eq!(read_u32(&bytes, 32), 3); // COLUMN_COUNT
        assert_eq!(read_u32(&bytes, 36), 4); // INDEX_RG_COUNT
        assert_eq!(read_u32(&bytes, 40), 2); // DATA_RG_COUNT
        assert_eq!(read_u32(&bytes, 44), 11_405); // KEY_COUNT
        assert_eq!(read_u32(&bytes, 48), 0); // KEY_ID_COLUMN
        assert_eq!(read_u32(&bytes, 52), 1); // ROW_ID_COLUMN
        assert_eq!(read_u64(&bytes, 56), 984); // INDEX_SECTIONS_OFFSET

        // Descriptors: 64 + 3 * 32 = 160.
        assert_eq!(read_u64(&bytes, 64), 160); // col 0 name offset
        assert_eq!(read_u32(&bytes, 88), 6); // col 0 name length
        assert_eq!(read_u64(&bytes, 96), 166); // col 1 name offset
        assert_eq!(read_u64(&bytes, 128), 172); // col 2 name offset
        assert_eq!(read_u32(&bytes, 152), 5); // col 2 name length

        // Names: 160..177, then 7 bytes of padding to 184.
        assert_eq!(&bytes[160..177], b"key_idrow_idprice");
        assert_eq!(&bytes[177..184], &[0u8; 7]);

        // Blocks: 8 + 3 * 64 = 200 bytes each, from 184.
        assert_eq!(read_u64(&bytes, 184), 100_000); // block 0 NUM_ROWS
        assert_eq!(read_u64(&bytes, 384), 58_000); // block 1 NUM_ROWS
        assert_eq!(read_u64(&bytes, 584), 82_001); // block 2 NUM_ROWS
        assert_eq!(read_u64(&bytes, 784), 759_999); // block 3 NUM_ROWS
                                                    // block 3, column 2 (price): NUM_ROWS + 2 chunks + 8-byte prefix.
        assert_eq!(read_u64(&bytes, 784 + 8 + 2 * 64 + 8), 759_999); // num_values
        assert_eq!(read_u64(&bytes, 784 + 8 + 2 * 64 + 16), 7_096); // byte_range_start

        // RG_BLOCK_OFFSET at 984: 4 entries, no padding needed afterwards.
        assert_eq!(read_u32(&bytes, 984), 184 >> 3);
        assert_eq!(read_u32(&bytes, 988), 384 >> 3);
        assert_eq!(read_u32(&bytes, 992), 584 >> 3);
        assert_eq!(read_u32(&bytes, 996), 784 >> 3);

        // RG_FIRST_KEY at 1000: 5 entries (20 bytes) then 4 bytes of padding.
        assert_eq!(read_u32(&bytes, 1_000), 0);
        assert_eq!(read_u32(&bytes, 1_004), 11_403);
        assert_eq!(read_u32(&bytes, 1_008), 11_403);
        assert_eq!(read_u32(&bytes, 1_012), 11_404);
        assert_eq!(read_u32(&bytes, 1_016), 11_405); // sentinel
        assert_eq!(&bytes[1_020..1_024], &[0u8; 4]);

        // DATA_RG_BOUNDARY at 1024, CRC at 1048.
        assert_eq!(read_u64(&bytes, 1_024) as i64, 0);
        assert_eq!(read_u64(&bytes, 1_032) as i64, 500_000);
        assert_eq!(read_u64(&bytes, 1_040) as i64, 1_000_000);
        assert_eq!(read_u32(&bytes, 1_048), crc32fast::hash(&bytes[8..1_048]));
    }

    /// The complementary alignment case: names are already 8-aligned, so the
    /// name section adds no padding, and the odd row group count moves the
    /// padding from RG_FIRST_KEY to RG_BLOCK_OFFSET. v1's tests only ever
    /// exercised one of the two, which is how an alignment bug could hide.
    #[test]
    fn test_absolute_byte_layout_with_aligned_name_section() {
        let bytes = build_aligned_sample();
        assert_eq!(bytes.len(), 836);
        assert_eq!(read_u32(&bytes, 36), 3); // INDEX_RG_COUNT
        assert_eq!(read_u64(&bytes, 56), 776); // INDEX_SECTIONS_OFFSET

        // Names: 16 bytes at 160..176, no padding.
        assert_eq!(&bytes[160..176], b"key_idrow_idpxpx");

        // Blocks start immediately at 176.
        assert_eq!(read_u64(&bytes, 176), 100);
        assert_eq!(read_u64(&bytes, 376), 100);
        assert_eq!(read_u64(&bytes, 576), 100);

        // RG_BLOCK_OFFSET at 776: 3 entries (12 bytes) then 4 bytes of padding.
        assert_eq!(read_u32(&bytes, 776), 176 >> 3);
        assert_eq!(read_u32(&bytes, 780), 376 >> 3);
        assert_eq!(read_u32(&bytes, 784), 576 >> 3);
        assert_eq!(&bytes[788..792], &[0u8; 4]);

        // RG_FIRST_KEY at 792: 4 entries (16 bytes), already aligned.
        assert_eq!(read_u32(&bytes, 792), 0);
        assert_eq!(read_u32(&bytes, 796), 300);
        assert_eq!(read_u32(&bytes, 800), 700);
        assert_eq!(read_u32(&bytes, 804), 900); // sentinel

        // DATA_RG_BOUNDARY at 808, CRC at 832.
        assert_eq!(read_u64(&bytes, 808) as i64, 0);
        assert_eq!(read_u64(&bytes, 824) as i64, 300);

        let r = IndexMetaReader::new(&bytes).unwrap();
        assert_eq!(r.index_sections_offset(), 776);
        assert_eq!(r.column_name(2).unwrap(), "pxpx");
        assert_eq!(r.row_group_block(2).unwrap().num_rows(), 100);
        assert_eq!(r.row_group_range_for_key(700), Some((2, 2)));
        assert_eq!(r.data_row_group_boundary(2).unwrap(), 300);
    }

    // ── Key lookup ─────────────────────────────────────────────────────

    /// The worked example from the spec: `RG_FIRST_KEY = [0, 11_403, 11_403,
    /// 11_404, KEY_COUNT]`.
    #[test]
    fn test_key_lookup_worked_example() {
        let bytes = build_sample();
        let r = IndexMetaReader::new(&bytes).unwrap();
        assert_eq!(r.row_group_range_for_key(0), Some((0, 0)));
        assert_eq!(r.row_group_range_for_key(5), Some((0, 0)));
        assert_eq!(r.row_group_range_for_key(11_403), Some((1, 2)));
        assert_eq!(r.row_group_range_for_key(11_404), Some((3, 3)));
        assert_eq!(r.row_group_range_for_key(11_405), None); // KEY_COUNT
        assert_eq!(r.row_group_range_for_key(u32::MAX), None);
    }

    #[test]
    fn test_key_below_first_entry_is_absent() {
        let mut w = IndexMetaWriter::new(IM_PAYLOAD_ROW_PER_POSTING, 100, 0, 1);
        w.add_column("key_id", descriptor(-1, TYPE_INT));
        w.add_column("row_id", descriptor(-1, TYPE_LONG));
        w.add_row_group(5, minimal_block(5, 10));
        w.add_row_group(9, minimal_block(9, 10));
        w.set_data_row_group_boundaries(&[0, 20]);
        let bytes = w.finish().unwrap();

        let r = IndexMetaReader::new(&bytes).unwrap();
        assert_eq!(r.row_group_range_for_key(0), None);
        assert_eq!(r.row_group_range_for_key(4), None);
        assert_eq!(r.row_group_range_for_key(5), Some((0, 0)));
        assert_eq!(r.row_group_range_for_key(7), Some((0, 0)));
        assert_eq!(r.row_group_range_for_key(9), Some((1, 1)));
        assert_eq!(r.row_group_range_for_key(50), Some((1, 1)));
    }

    #[test]
    fn test_zero_row_groups_is_absent() {
        let mut w = IndexMetaWriter::new(IM_PAYLOAD_ROW_PER_POSTING, 100, 0, 1);
        w.add_column("key_id", descriptor(-1, TYPE_INT));
        w.add_column("row_id", descriptor(-1, TYPE_LONG));
        w.set_data_row_group_boundaries(&[0, 20]);
        let bytes = w.finish().unwrap();

        let r = IndexMetaReader::new(&bytes).unwrap();
        assert_eq!(r.index_row_group_count(), 0);
        assert_eq!(r.row_group_range_for_key(0), None);
        assert_eq!(r.row_group_range_for_key(50), None);
        // Only the sentinel is present.
        assert_eq!(r.row_group_first_key(0).unwrap(), 100);
        assert!(r.row_group_block(0).is_err());
    }

    // ── Out-of-line stats ──────────────────────────────────────────────

    /// A covered UUID column's min/max exceed 8 bytes, so the stats go to the
    /// block's out-of-line region as `(offset << 16) | length`.
    #[test]
    fn test_out_of_line_stats_for_wide_covered_column() {
        let min_uuid = [0x11u8; 16];
        let max_uuid = [0xEEu8; 16];

        let mut w = IndexMetaWriter::new(IM_PAYLOAD_ROW_PER_POSTING, 50, 0, 1);
        w.add_column("key_id", descriptor(-1, TYPE_INT));
        w.add_column("row_id", descriptor(-1, TYPE_LONG));
        w.add_column("uid", descriptor(4, TYPE_UUID));

        let mut block = RowGroupBlockBuilder::new(3);
        block.set_num_rows(64);
        block.set_column_chunk(0, key_id_chunk(7, 7, 64)).unwrap();
        block.set_column_chunk(1, row_id_chunk(0, 63, 64)).unwrap();
        let mut uid = ColumnChunkRaw::zeroed();
        uid.codec = Codec::Zstd as u8;
        uid.stat_flags = StatFlags::new()
            .with_min(false, true)
            .with_max(false, true)
            .0;
        uid.num_values = 64;
        block.set_column_chunk(2, uid).unwrap();
        block.add_out_of_line_stat(2, true, &min_uuid).unwrap();
        block.add_out_of_line_stat(2, false, &max_uuid).unwrap();
        w.add_row_group(7, block);
        w.set_data_row_group_boundaries(&[0, 64]);
        let bytes = w.finish().unwrap();

        let r = IndexMetaReader::new(&bytes).unwrap();
        let block = r.row_group_block(0).unwrap();
        let chunk = block.column_chunk(2).unwrap();
        assert!(!chunk.stat_flags().is_min_inlined());
        assert!(!chunk.stat_flags().is_max_inlined());

        let ool = block.out_of_line_region();
        let min_off = (chunk.min_stat >> 16) as usize;
        let min_len = (chunk.min_stat & 0xFFFF) as usize;
        assert_eq!(min_len, 16);
        assert_eq!(&ool[min_off..min_off + min_len], &min_uuid);
        let max_off = (chunk.max_stat >> 16) as usize;
        let max_len = (chunk.max_stat & 0xFFFF) as usize;
        assert_eq!(max_len, 16);
        assert_eq!(&ool[max_off..max_off + max_len], &max_uuid);

        // Header 64, descriptors 96, names "key_idrow_iduid" padded 15 -> 16,
        // one block of 8 + 3 * 64 plus 32 out-of-line bytes, then the index
        // sections: RG_BLOCK_OFFSET 4 padded to 8, RG_FIRST_KEY 8,
        // DATA_RG_BOUNDARY 16, CRC 4.
        assert_eq!(bytes.len(), 64 + 96 + 16 + (8 + 192 + 32) + 8 + 8 + 16 + 4);
        assert_eq!(bytes.len(), 444);
    }

    /// The JNI layer cannot borrow the block it just handed over, so it
    /// patches wide stats through the writer afterwards. The bytes must come
    /// out identical to patching the block before it was added.
    #[test]
    fn test_out_of_line_stat_added_through_the_last_row_group() {
        let min_uuid = [0x11u8; 16];
        let max_uuid = [0xEEu8; 16];

        let mut w = IndexMetaWriter::new(IM_PAYLOAD_ROW_PER_POSTING, 50, 0, 1);
        w.add_column("key_id", descriptor(-1, TYPE_INT));
        w.add_column("row_id", descriptor(-1, TYPE_LONG));
        w.add_column("uid", descriptor(4, TYPE_UUID));

        let mut block = RowGroupBlockBuilder::new(3);
        block.set_num_rows(64);
        block.set_column_chunk(0, key_id_chunk(7, 7, 64)).unwrap();
        block.set_column_chunk(1, row_id_chunk(0, 63, 64)).unwrap();
        let mut uid = ColumnChunkRaw::zeroed();
        uid.codec = Codec::Zstd as u8;
        uid.stat_flags = StatFlags::new()
            .with_min(false, true)
            .with_max(false, true)
            .0;
        uid.num_values = 64;
        block.set_column_chunk(2, uid).unwrap();
        w.add_row_group(7, block);
        w.add_out_of_line_stat_to_last_row_group(2, true, &min_uuid)
            .unwrap();
        w.add_out_of_line_stat_to_last_row_group(2, false, &max_uuid)
            .unwrap();
        w.set_data_row_group_boundaries(&[0, 64]);

        let bytes = w.finish().unwrap();
        let r = IndexMetaReader::new(&bytes).unwrap();
        let block = r.row_group_block(0).unwrap();
        let chunk = block.column_chunk(2).unwrap();
        let ool = block.out_of_line_region();
        let min_off = (chunk.min_stat >> 16) as usize;
        assert_eq!(&ool[min_off..min_off + 16], &min_uuid);
        let max_off = (chunk.max_stat >> 16) as usize;
        assert_eq!(&ool[max_off..max_off + 16], &max_uuid);
        // Byte-identical to the block-patched fixture above.
        assert_eq!(bytes.len(), 444);
    }

    /// Pins the fixture the crafted out-of-line references below patch, so a
    /// layout change cannot quietly turn them into harmless offsets.
    #[test]
    fn test_two_block_out_of_line_sample_layout() {
        let bytes = build_two_block_out_of_line_sample();
        assert_eq!(bytes.len(), TWO_BLOCK_FILE_LEN);
        assert_eq!(
            read_u64(&bytes, OFF_INDEX_SECTIONS_OFFSET),
            TWO_BLOCK_SECTIONS_OFF as u64
        );
        assert_eq!(
            read_u32(&bytes, TWO_BLOCK_SECTIONS_OFF),
            (TWO_BLOCK_0_OFF >> BLOCK_ALIGNMENT_SHIFT) as u32
        );
        assert_eq!(
            read_u32(&bytes, TWO_BLOCK_SECTIONS_OFF + 4),
            (TWO_BLOCK_1_OFF >> BLOCK_ALIGNMENT_SHIFT) as u32
        );
        // Block 1 is the last one, so its extent ends at the index sections.
        assert_eq!(TWO_BLOCK_1_OFF + TWO_BLOCK_SIZE, TWO_BLOCK_SECTIONS_OFF);
    }

    /// The legitimate case the bound must not break: each block's max stat
    /// occupies the last 16 bytes of its own out-of-line region, so
    /// `offset + length` lands exactly on the block's end. A bound written
    /// with `>=` instead of `>` rejects this.
    #[test]
    fn test_out_of_line_stat_at_the_block_end_is_accepted() {
        let bytes = build_two_block_out_of_line_sample();
        let r = IndexMetaReader::new(&bytes).unwrap();
        for rg in 0..2 {
            let chunk = r.row_group_block(rg).unwrap().column_chunk(2).unwrap();
            assert_eq!(chunk.min_stat, ool_ref(0, 16));
            // The max stat ends exactly at the end of the block's region.
            assert_eq!(chunk.max_stat, ool_ref(16, 16));
            assert_eq!(
                (chunk.max_stat >> 16) + (chunk.max_stat & 0xFFFF),
                TWO_BLOCK_OOL_SIZE
            );

            assert_eq!(
                r.out_of_line_stat(rg, 2, true).unwrap(),
                &[TWO_BLOCK_MIN_FILL[rg]; 16]
            );
            assert_eq!(
                r.out_of_line_stat(rg, 2, false).unwrap(),
                &[TWO_BLOCK_MAX_FILL[rg]; 16]
            );
        }
    }

    /// Row group 0's max stat is repointed just past its own out-of-line
    /// region, which is where row group 1's block begins. Bounded only by the
    /// end of the whole row group region this resolves happily and hands back
    /// another row group's bytes as this one's statistic - a silently wrong
    /// stat, and stats drive query pruning.
    #[test]
    fn test_out_of_line_stat_reaching_into_the_next_block_is_rejected() {
        // Exactly the first 16 bytes of block 1.
        let mut bytes = build_two_block_out_of_line_sample();
        patch_u64(
            &mut bytes,
            TWO_BLOCK_0_OFF + TWO_BLOCK_UID_MAX_STAT,
            ool_ref(TWO_BLOCK_OOL_SIZE, 16),
        );
        let r = IndexMetaReader::new(&bytes).unwrap();
        let err = r.out_of_line_stat(0, 2, false).unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::Truncated));
        assert!(
            err.msg.contains("out of line stat out of bounds"),
            "{}",
            err.msg
        );
        // Block 0's own stats are untouched, so the file is not simply broken.
        assert_eq!(
            r.out_of_line_stat(0, 2, true).unwrap(),
            &[TWO_BLOCK_MIN_FILL[0]; 16]
        );

        // Straddling the boundary is rejected too: the first 8 bytes are this
        // block's, the last 8 belong to the next one.
        let mut bytes = build_two_block_out_of_line_sample();
        patch_u64(
            &mut bytes,
            TWO_BLOCK_0_OFF + TWO_BLOCK_UID_MAX_STAT,
            ool_ref(TWO_BLOCK_OOL_SIZE - 8, 16),
        );
        let r = IndexMetaReader::new(&bytes).unwrap();
        assert!(r.out_of_line_stat(0, 2, false).is_err());

        // One byte past the block's end is the off-by-one case.
        let mut bytes = build_two_block_out_of_line_sample();
        patch_u64(
            &mut bytes,
            TWO_BLOCK_0_OFF + TWO_BLOCK_UID_MAX_STAT,
            ool_ref(TWO_BLOCK_OOL_SIZE, 1),
        );
        let r = IndexMetaReader::new(&bytes).unwrap();
        assert!(r.out_of_line_stat(0, 2, false).is_err());
    }

    /// The last block's extent ends at `INDEX_SECTIONS_OFFSET`, so a reference
    /// past its own region would address the key directory.
    #[test]
    fn test_out_of_line_stat_past_the_index_sections_is_rejected() {
        let mut bytes = build_two_block_out_of_line_sample();
        patch_u64(
            &mut bytes,
            TWO_BLOCK_1_OFF + TWO_BLOCK_UID_MAX_STAT,
            ool_ref(TWO_BLOCK_OOL_SIZE, 16),
        );
        let r = IndexMetaReader::new(&bytes).unwrap();
        let err = r.out_of_line_stat(1, 2, false).unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::Truncated));
        assert!(
            err.msg.contains("out of line stat out of bounds"),
            "{}",
            err.msg
        );

        // An offset large enough to overflow a naive `offset + length` sum is
        // rejected by the same comparison.
        let mut bytes = build_two_block_out_of_line_sample();
        patch_u64(
            &mut bytes,
            TWO_BLOCK_1_OFF + TWO_BLOCK_UID_MAX_STAT,
            u64::MAX,
        );
        let r = IndexMetaReader::new(&bytes).unwrap();
        assert!(r.out_of_line_stat(1, 2, false).is_err());
    }

    #[test]
    fn test_out_of_line_stat_without_a_row_group_is_rejected() {
        let mut w = minimal_writer();
        // `unwrap_err` would require the writer itself to be Debug, so match.
        let err = match w.add_out_of_line_stat_to_last_row_group(0, true, &[0u8; 16]) {
            Ok(_) => panic!("expected an error"),
            Err(err) => err,
        };
        assert!(matches!(err.kind, ParquetMetaErrorKind::InvalidValue));
        assert!(err.msg.contains("no row group"), "{}", err.msg);
    }

    // ── Writer validation ──────────────────────────────────────────────

    #[test]
    fn test_first_keys_must_be_non_decreasing() {
        let mut w = minimal_writer();
        w.add_row_group(10, minimal_block(10, 5));
        w.add_row_group(4, minimal_block(4, 5));
        let err = w.finish().unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::InvalidValue));
        assert!(err.msg.contains("non-decreasing at index 1"), "{}", err.msg);
    }

    #[test]
    fn test_last_first_key_must_be_below_key_count() {
        let mut w = minimal_writer();
        w.add_row_group(0, minimal_block(0, 5));
        w.add_row_group(100, minimal_block(100, 5));
        let err = w.finish().unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::InvalidValue));
        assert!(err.msg.contains("must be below key count"), "{}", err.msg);
    }

    #[test]
    fn test_first_key_must_match_key_id_chunk_min_stat() {
        let mut w = minimal_writer();
        let mut block = minimal_block(3, 5);
        // Directory says 3, the chunk stat says 4: the fast path and the slow
        // path would disagree.
        block.set_column_chunk(0, key_id_chunk(4, 4, 5)).unwrap();
        w.add_row_group(3, block);
        let err = w.finish().unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::InvalidValue));
        assert!(
            err.msg.contains("does not match key id chunk min stat"),
            "{}",
            err.msg
        );
    }

    /// An out-of-line stat is `(offset << 16) | length`, so a 16-byte payload
    /// at offset 0 encodes as 16 and would silently satisfy the cross-check for
    /// a row group whose first key is 16. Key ids are 4-byte ints, so requiring
    /// the stat to be inline costs nothing and closes the hole.
    #[test]
    fn test_out_of_line_key_id_min_stat_is_rejected() {
        let mut w = minimal_writer();
        let mut block = minimal_block(16, 5);
        let mut key_id = key_id_chunk(16, 16, 5);
        key_id.stat_flags = StatFlags::new()
            .with_min(false, true)
            .with_max(true, true)
            .0;
        block.set_column_chunk(0, key_id).unwrap();
        block.add_out_of_line_stat(0, true, &[0u8; 16]).unwrap();
        // The reference encodes as (offset 0 << 16) | length 16, which is the
        // collision the check has to catch.
        assert_eq!(block.column_chunk_raw(0).min_stat, 16);
        w.add_row_group(16, block);
        let err = w.finish().unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::InvalidValue));
        assert!(
            err.msg.contains("min stat must be present and inline"),
            "{}",
            err.msg
        );

        // A missing min stat is rejected by the same check.
        let mut w = minimal_writer();
        let mut block = minimal_block(0, 5);
        let mut key_id = key_id_chunk(0, 0, 5);
        key_id.stat_flags = StatFlags::new().with_max(true, true).0;
        block.set_column_chunk(0, key_id).unwrap();
        w.add_row_group(0, block);
        let err = w.finish().unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::InvalidValue));
        assert!(
            err.msg.contains("min stat must be present and inline"),
            "{}",
            err.msg
        );
    }

    #[test]
    fn test_first_data_boundary_must_be_zero() {
        let mut w = minimal_writer();
        w.add_row_group(0, minimal_block(0, 5));
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
        let mut w = minimal_writer();
        w.add_row_group(0, minimal_block(0, 5));
        w.set_data_row_group_boundaries(&[0, 200, 150]);
        let err = w.finish().unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::InvalidValue));
        assert!(err.msg.contains("non-decreasing at index 2"), "{}", err.msg);
    }

    #[test]
    fn test_block_must_carry_one_chunk_per_column() {
        let mut w = minimal_writer();
        // The schema has 2 columns; this block was built for 1.
        let mut block = RowGroupBlockBuilder::new(1);
        block.set_num_rows(5);
        block.set_column_chunk(0, key_id_chunk(0, 0, 5)).unwrap();
        w.add_row_group(0, block);
        let err = w.finish().unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::SchemaMismatch));
        assert!(
            err.msg.contains("has 1 column chunks, expected 2"),
            "{}",
            err.msg
        );
    }

    #[test]
    fn test_zero_row_block_is_rejected() {
        let mut w = minimal_writer();
        w.add_row_group(0, minimal_block(0, 0));
        let err = w.finish().unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::InvalidValue));
        assert!(err.msg.contains("has zero rows"), "{}", err.msg);
    }

    #[test]
    fn test_key_id_column_must_be_in_range() {
        let mut w = IndexMetaWriter::new(IM_PAYLOAD_ROW_PER_POSTING, 100, 5, 1);
        w.add_column("key_id", descriptor(-1, TYPE_INT));
        w.add_column("row_id", descriptor(-1, TYPE_LONG));
        w.set_data_row_group_boundaries(&[0, 20]);
        let err = w.finish().unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::InvalidValue));
        assert!(err.msg.contains("key id column 5"), "{}", err.msg);
    }

    #[test]
    fn test_row_id_column_may_only_be_absent_for_row_per_key() {
        let mut w = IndexMetaWriter::new(IM_PAYLOAD_ROW_PER_POSTING, 100, 0, -1);
        w.add_column("key_id", descriptor(-1, TYPE_INT));
        w.set_data_row_group_boundaries(&[0, 20]);
        let err = w.finish().unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::InvalidValue));
        assert!(err.msg.contains("may only be -1"), "{}", err.msg);

        // The same schema is valid as a row-per-key payload.
        let mut w = IndexMetaWriter::new(IM_PAYLOAD_ROW_PER_KEY, 100, 0, -1);
        w.add_column("key_id", descriptor(-1, TYPE_INT));
        w.set_data_row_group_boundaries(&[0, 20]);
        let bytes = w.finish().unwrap();
        let r = IndexMetaReader::new(&bytes).unwrap();
        assert_eq!(r.payload_kind(), IM_PAYLOAD_ROW_PER_KEY);
        assert_eq!(r.row_id_column(), -1);
    }

    // ── Reader rejection ───────────────────────────────────────────────

    #[test]
    fn test_checksum_mismatch_is_rejected() {
        let mut bytes = build_sample();
        bytes[200] ^= 0xFF;
        let err = IndexMetaReader::new(&bytes).unwrap_err();
        assert!(matches!(
            err.kind,
            ParquetMetaErrorKind::ChecksumMismatch { .. }
        ));
    }

    #[test]
    fn test_truncated_file_is_rejected() {
        let bytes = build_sample();
        // Below the fixed header.
        let err = IndexMetaReader::new(&bytes[..40]).unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::Truncated));
        // Header intact but IM_FILE_SIZE beyond the buffer.
        let err = IndexMetaReader::new(&bytes[..600]).unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::Truncated));
    }

    #[test]
    fn test_wrong_magic_is_rejected() {
        let mut bytes = build_sample();
        // A `_pm` file carries FEATURE_FLAGS where `_im` carries the magic.
        patch_u64(&mut bytes, OFF_IM_MAGIC, 0);
        let err = IndexMetaReader::new(&bytes).unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::InvalidValue));
        assert!(err.msg.contains("bad _im magic"), "{}", err.msg);
    }

    #[test]
    fn test_version_mismatch_is_rejected() {
        let mut bytes = build_sample();
        // Version 1 is the interim layout and is not readable.
        patch_u32(&mut bytes, OFF_FORMAT_VERSION, 1);
        let err = IndexMetaReader::new(&bytes).unwrap_err();
        assert!(matches!(
            err.kind,
            ParquetMetaErrorKind::VersionMismatch {
                found: 1,
                expected: 2
            }
        ));
    }

    #[test]
    fn test_unknown_required_feature_bit_is_rejected() {
        let mut bytes = build_sample();
        patch_u64(&mut bytes, OFF_FEATURE_FLAGS, 1 << 32);
        let err = IndexMetaReader::new(&bytes).unwrap_err();
        assert!(matches!(
            err.kind,
            ParquetMetaErrorKind::UnsupportedFeature {
                flags: 0x0000_0001_0000_0000
            }
        ));

        // An unknown optional bit is ignored.
        let mut bytes = build_sample();
        patch_u64(&mut bytes, OFF_FEATURE_FLAGS, 1 << 7);
        let r = IndexMetaReader::new(&bytes).unwrap();
        assert_eq!(r.feature_flags(), 1 << 7);
    }

    /// A header claiming `u32::MAX` row groups and columns makes every section
    /// size product enormous. Unchecked, the sums wrap and the section offsets
    /// land inside the header, so the reader would hand out garbage or panic
    /// instead of rejecting the file.
    #[test]
    fn test_crafted_counts_are_rejected_by_checked_arithmetic() {
        let mut bytes = build_sample();
        patch_u32(&mut bytes, OFF_INDEX_RG_COUNT, u32::MAX);
        patch_u32(&mut bytes, OFF_COLUMN_COUNT, u32::MAX);
        let err = IndexMetaReader::new(&bytes).unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::Truncated));

        let mut bytes = build_sample();
        patch_u32(&mut bytes, OFF_DATA_RG_COUNT, u32::MAX);
        let err = IndexMetaReader::new(&bytes).unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::Truncated));

        // Descriptors alone overrunning the index sections is also rejected.
        let mut bytes = build_sample();
        patch_u32(&mut bytes, OFF_COLUMN_COUNT, 1_000);
        let err = IndexMetaReader::new(&bytes).unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::Truncated));
    }

    /// Every section starts 8-byte aligned, and `row_group_block` hands out a
    /// `#[repr(C)]` view of the bytes the first one addresses.
    #[test]
    fn test_misaligned_index_sections_offset_is_rejected() {
        let mut bytes = build_sample();
        patch_u64(&mut bytes, OFF_INDEX_SECTIONS_OFFSET, 985);
        let err = IndexMetaReader::new(&bytes).unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::Alignment));
        assert!(err.msg.contains("not 8-byte aligned"), "{}", err.msg);
    }

    /// The names run 160..177 in the fixture, so 168 is 8-aligned and past the
    /// descriptors but still inside the name blob: the sections would overlap
    /// the strings the descriptors point at.
    #[test]
    fn test_index_sections_offset_inside_name_strings_is_rejected() {
        let mut bytes = build_sample();
        patch_u64(&mut bytes, OFF_INDEX_SECTIONS_OFFSET, 168);
        let err = IndexMetaReader::new(&bytes).unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::Truncated));

        // Inside the descriptors themselves is rejected by the same bound.
        let mut bytes = build_sample();
        patch_u64(&mut bytes, OFF_INDEX_SECTIONS_OFFSET, 128);
        let err = IndexMetaReader::new(&bytes).unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::Truncated));
    }

    /// The fixture's sections need 64 bytes; pointing at the CRC, or anywhere
    /// with less than that ahead of it, leaves them nowhere to fit.
    #[test]
    fn test_index_sections_offset_leaving_no_room_is_rejected() {
        let mut bytes = build_sample();
        patch_u64(&mut bytes, OFF_INDEX_SECTIONS_OFFSET, 1_048); // at the CRC
        let err = IndexMetaReader::new(&bytes).unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::Truncated));

        // 8 bytes short of the 64 the three sections occupy.
        let mut bytes = build_sample();
        patch_u64(&mut bytes, OFF_INDEX_SECTIONS_OFFSET, 992);
        let err = IndexMetaReader::new(&bytes).unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::Truncated));

        // Past the committed size entirely.
        let mut bytes = build_sample();
        patch_u64(&mut bytes, OFF_INDEX_SECTIONS_OFFSET, 2_048);
        let err = IndexMetaReader::new(&bytes).unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::Truncated));
    }

    /// An 8-aligned offset one section-size below `u64::MAX`: added to the
    /// section sizes unchecked it wraps to a small value that passes every
    /// bound, so the reader would resolve the sections inside the header.
    #[test]
    fn test_index_sections_offset_overflowing_the_size_sum_is_rejected() {
        let mut bytes = build_sample();
        patch_u64(&mut bytes, OFF_INDEX_SECTIONS_OFFSET, u64::MAX - 7);
        let err = IndexMetaReader::new(&bytes).unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::Truncated));
    }

    /// `KEY_ID_COLUMN` is the only sanctioned route to the synthetic `key_id`
    /// column, so a caller hands it straight to a column chunk accessor. An
    /// unvalidated one indexes past the block - an error here, a wild address
    /// in the Java reader - so both readers reject the file at open instead.
    #[test]
    fn test_crafted_key_id_column_is_rejected_at_open() {
        let mut bytes = build_sample();
        patch_u32(&mut bytes, OFF_KEY_ID_COLUMN, 10_000_000);
        let err = IndexMetaReader::new(&bytes).unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::InvalidValue));
        assert!(err.msg.contains("key id column"), "{}", err.msg);

        // -1 is the descriptor sentinel for a synthetic column, never an index.
        let mut bytes = build_sample();
        patch_u32(&mut bytes, OFF_KEY_ID_COLUMN, u32::MAX);
        let err = IndexMetaReader::new(&bytes).unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::InvalidValue));
        assert!(err.msg.contains("key id column"), "{}", err.msg);

        // The fixture has 3 columns, so the last valid index is accepted.
        let mut bytes = build_sample();
        patch_u32(&mut bytes, OFF_KEY_ID_COLUMN, 2);
        assert_eq!(IndexMetaReader::new(&bytes).unwrap().key_id_column(), 2);
    }

    /// `PAYLOAD_KIND` decides whether `ROW_ID_COLUMN` may be absent, so a kind
    /// neither reader knows leaves that rule undecidable.
    #[test]
    fn test_crafted_payload_kind_is_rejected_at_open() {
        let mut bytes = build_sample();
        patch_u32(&mut bytes, OFF_PAYLOAD_KIND, 2);
        let err = IndexMetaReader::new(&bytes).unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::InvalidValue));
        assert!(err.msg.contains("payload kind"), "{}", err.msg);
    }

    /// `ROW_ID_COLUMN` is `-1` exactly under row-per-key, and in range
    /// otherwise: it reaches the same address computation as `KEY_ID_COLUMN`,
    /// and pruning by time reads the chunk it names.
    #[test]
    fn test_crafted_row_id_column_is_rejected_at_open() {
        let mut bytes = build_sample();
        patch_u32(&mut bytes, OFF_ROW_ID_COLUMN, 10_000_000);
        let err = IndexMetaReader::new(&bytes).unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::InvalidValue));
        assert!(err.msg.contains("row id column"), "{}", err.msg);

        // -1 says "no row id column at all", which only row-per-key may say,
        // and the fixture is row-per-posting.
        let mut bytes = build_sample();
        patch_u32(&mut bytes, OFF_ROW_ID_COLUMN, u32::MAX);
        let err = IndexMetaReader::new(&bytes).unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::InvalidValue));
        assert!(err.msg.contains("row id column"), "{}", err.msg);

        // The converse: a row-per-key file must not name a row id column.
        let mut bytes = build_sample();
        patch_u32(&mut bytes, OFF_PAYLOAD_KIND, IM_PAYLOAD_ROW_PER_KEY);
        let err = IndexMetaReader::new(&bytes).unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::InvalidValue));
        assert!(err.msg.contains("row id column"), "{}", err.msg);
    }

    #[test]
    fn test_crafted_block_offset_is_rejected() {
        let mut bytes = build_sample();
        // Point row group 0 at the header. The entries still ascend, so the
        // file opens and the bound is applied when the block is resolved.
        patch_u32(&mut bytes, 984, 0);
        let r = IndexMetaReader::new(&bytes).unwrap();
        assert!(r.row_group_block(0).is_err());

        let mut bytes = build_sample();
        // Point the last row group past the end of the block region.
        patch_u32(&mut bytes, 996, u32::MAX);
        let r = IndexMetaReader::new(&bytes).unwrap();
        assert!(r.row_group_block(3).is_err());
    }

    /// A block's extent is `[RG_BLOCK_OFFSET[i], RG_BLOCK_OFFSET[i + 1])`, so
    /// an entry that does not ascend leaves a block with an empty or inverted
    /// extent and no meaningful bound for its out-of-line stats. Rejecting the
    /// file at open time - rather than per block on first access - is the
    /// judgement call both readers make, because it is what lets every later
    /// extent computation be trusted.
    #[test]
    fn test_non_ascending_block_offset_is_rejected_at_open() {
        // Entry 1 below entry 0.
        let mut bytes = build_sample();
        patch_u32(&mut bytes, 988, (184 >> BLOCK_ALIGNMENT_SHIFT) - 1);
        let err = IndexMetaReader::new(&bytes).unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::InvalidValue));
        assert!(
            err.msg.contains("RG_BLOCK_OFFSET entries must ascend"),
            "{}",
            err.msg
        );

        // Two blocks sharing an offset are rejected by the same check: the
        // first of them would have an empty extent.
        let mut bytes = build_sample();
        patch_u32(&mut bytes, 988, 184 >> BLOCK_ALIGNMENT_SHIFT);
        let err = IndexMetaReader::new(&bytes).unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::InvalidValue));

        // A huge entry in front of the others is non-ascending too, so it no
        // longer has to be caught later by the per-block bound.
        let mut bytes = build_sample();
        patch_u32(&mut bytes, 984, u32::MAX);
        let err = IndexMetaReader::new(&bytes).unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::InvalidValue));
    }

    #[test]
    fn test_crafted_name_offset_is_rejected() {
        // Push the name entry past the block region; the sum overflows, which
        // the construction-time bound on the name blob catches.
        let mut bytes = build_sample();
        bytes[64..72].copy_from_slice(&u64::MAX.to_le_bytes());
        repair_crc(&mut bytes);
        let err = IndexMetaReader::new(&bytes).unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::Truncated));

        // A name starting inside the descriptors is rejected the same way.
        let mut bytes = build_sample();
        bytes[64..72].copy_from_slice(&64u64.to_le_bytes());
        repair_crc(&mut bytes);
        let err = IndexMetaReader::new(&bytes).unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::Truncated));
    }

    /// The committed size is the only boundary: bytes from a later,
    /// unpublished write must not be visible.
    #[test]
    fn test_reader_ignores_bytes_past_committed_size() {
        let mut bytes = build_sample();
        let committed = bytes.len();
        bytes.extend_from_slice(&[0xAAu8; 128]);
        let r = IndexMetaReader::new(&bytes).unwrap();
        assert_eq!(r.im_file_size(), committed as u64);
        assert_eq!(r.data_row_group_boundary(2).unwrap(), 1_000_000);
    }
}
