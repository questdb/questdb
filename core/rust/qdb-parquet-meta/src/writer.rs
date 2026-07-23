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

//! Writer for `_pm` metadata files (create and update modes).

use crate::error::ParquetMetaErrorKind;
use crate::error::ParquetMetaResult;
use crate::footer::FooterBuilder;
use crate::header::FileHeaderBuilder;
use crate::parquet_meta_err;
use crate::reader::ParquetMetaReader;
use crate::row_group::RowGroupBlockBuilder;
use crate::types::{
    ColumnFlags, SeqTxn, BLOCK_ALIGNMENT, BLOCK_ALIGNMENT_SHIFT, COLUMN_CHUNK_SIZE,
    FOOTER_CHECKSUM_SIZE, FOOTER_TRAILER_SIZE, HEADER_CRC_AREA_OFF,
    HEADER_PARQUET_META_FILE_SIZE_OFF,
};

// ── ParquetMetaWriter (create mode) ───────────────────────────────────────────

/// Builds a complete `_pm` metadata file from scratch.
///
/// Usage:
/// ```ignore
/// let bytes = ParquetMetaWriter::new()
///     .designated_timestamp(0)
///     .add_column(0, "ts", 0, 8, ColumnFlags::new(), 0, 0, 0, 0)
///     .add_row_group(rg_builder)
///     .parquet_footer(offset, length)
///     .finish()?;
/// ```
pub struct ParquetMetaWriter {
    header_builder: FileHeaderBuilder,
    row_groups: Vec<RowGroupBlockBuilder>,
    parquet_footer_offset: u64,
    parquet_footer_length: u32,
    unused_bytes: u64,
    squash_tracker: i64,
    seq_txn: SeqTxn,
    scratchpad: Vec<(u32, Vec<u8>)>,
}

impl Default for ParquetMetaWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl ParquetMetaWriter {
    pub fn new() -> Self {
        Self {
            header_builder: FileHeaderBuilder::new(-1),
            row_groups: Vec::new(),
            parquet_footer_offset: 0,
            parquet_footer_length: 0,
            unused_bytes: 0,
            squash_tracker: -1,
            seq_txn: SeqTxn::UNSET,
            scratchpad: Vec::new(),
        }
    }

    pub fn designated_timestamp(&mut self, index: i32) -> &mut Self {
        self.header_builder = FileHeaderBuilder::new(index);
        self
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
        self.header_builder.add_column(
            name,
            id,
            col_type,
            flags,
            fixed_byte_len,
            physical_type,
            max_rep_level,
            max_def_level,
        );
        self
    }

    pub fn add_sorting_column(&mut self, index: u32) -> &mut Self {
        self.header_builder.add_sorting_column(index);
        self
    }

    pub fn add_row_group(&mut self, builder: RowGroupBlockBuilder) -> &mut Self {
        self.row_groups.push(builder);
        self
    }

    pub fn parquet_footer(&mut self, offset: u64, length: u32) -> &mut Self {
        self.parquet_footer_offset = offset;
        self.parquet_footer_length = length;
        self
    }

    pub fn unused_bytes(&mut self, unused_bytes: u64) -> &mut Self {
        self.unused_bytes = unused_bytes;
        self
    }

    /// Sets the partition squash tracker. The value is applied at `finish()`
    /// time to survive any subsequent `designated_timestamp()` call (which
    /// recreates the header builder). Passing `-1` omits the section.
    pub fn squash_tracker(&mut self, value: i64) -> &mut Self {
        self.squash_tracker = value;
        self
    }

    /// Sets the per-footer `seqTxn`. `SeqTxn::UNSET` omits the section.
    pub fn seq_txn(&mut self, value: SeqTxn) -> &mut Self {
        self.seq_txn = value;
        self
    }

    /// Replaces the opaque scratchpad entries written into the footer.
    /// An empty `Vec` omits the section.
    pub fn set_scratchpad_entries(&mut self, entries: Vec<(u32, Vec<u8>)>) -> &mut Self {
        self.scratchpad = entries;
        self
    }

    /// Adds a bloom filter bitset to the last row group for the given column.
    pub fn add_bloom_filter_to_last_row_group(
        &mut self,
        col_index: usize,
        bitset: &[u8],
    ) -> ParquetMetaResult<&mut Self> {
        let rg = self.row_groups.last_mut().ok_or_else(|| {
            parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "no row group to add bloom filter to"
            )
        })?;
        rg.add_bloom_filter(col_index, bitset)?;
        Ok(self)
    }

    /// Sets bloom filter column indices (delegates to header builder).
    pub fn set_bloom_filter_columns(&mut self, indices: &[u32]) -> &mut Self {
        self.header_builder.set_bloom_filter_columns(indices);
        self
    }

    /// Sets whether bloom filters are stored externally in the parquet file.
    pub fn set_bloom_filters_external(&mut self, value: bool) -> &mut Self {
        self.header_builder.set_bloom_filters_external(value);
        self
    }

    /// Finishes writing and returns the complete `_pm` file bytes.
    ///
    /// Returns `(bytes, parquet_meta_file_size)` where `parquet_meta_file_size` is the total
    /// committed file size — the same value that is patched into the header
    /// at `HEADER_PARQUET_META_FILE_SIZE_OFF` and matches `bytes.len() as u64`.
    #[must_use = "returns the file bytes and parquet_meta_file_size"]
    pub fn finish(&mut self) -> ParquetMetaResult<(Vec<u8>, u64)> {
        // Auto-derive bloom filter columns from row group contents if not set.
        let is_external = self.header_builder.bloom_filters_external;
        if self.header_builder.bloom_filter_columns.is_empty() {
            let mut col_set = std::collections::BTreeSet::new();
            for rg in &self.row_groups {
                if is_external {
                    for &(col_idx, _, _) in rg.bloom_filter_external_entries() {
                        col_set.insert(col_idx as u32);
                    }
                } else {
                    for &(col_idx, _) in rg.bloom_filter_inlined_entries() {
                        col_set.insert(col_idx as u32);
                    }
                }
            }
            if !col_set.is_empty() {
                let indices: Vec<u32> = col_set.into_iter().collect();
                self.header_builder.set_bloom_filter_columns(&indices);
            }
        }

        let bloom_filter_columns = self.header_builder.bloom_filter_columns.clone();
        let bloom_col_count = bloom_filter_columns.len();

        // Apply the squash tracker (stored on the writer) just before we serialize
        // the header — the header_builder can be recreated by designated_timestamp(),
        // so we thread the value through at finish time.
        self.header_builder.set_squash_tracker(self.squash_tracker);

        let mut buf = Vec::new();

        // Write header (includes descriptors, sorting columns, name strings,
        // and bloom filter header section if applicable).
        self.header_builder.write_to(&mut buf);

        // Write row group blocks (8-byte aligned).
        let mut block_offsets: Vec<u64> = Vec::with_capacity(self.row_groups.len());
        for rg in &self.row_groups {
            let offset = rg.write_to(&mut buf);
            block_offsets.push(offset as u64);
        }

        // Build the bloom filter footer section if applicable.
        let bloom_section = if bloom_col_count > 0 {
            build_bloom_filter_footer_section(
                &self.row_groups,
                &block_offsets,
                &bloom_filter_columns,
                is_external,
            )
        } else {
            Vec::new()
        };

        // Write footer.
        let mut fb = FooterBuilder::new(self.parquet_footer_offset, self.parquet_footer_length);
        fb.unused_bytes(self.unused_bytes);
        for &offset in &block_offsets {
            fb.add_row_group_offset(offset)?;
        }
        fb.set_bloom_filter_section(bloom_section);
        fb.set_seq_txn(self.seq_txn);
        fb.set_scratchpad_entries(self.scratchpad.clone());
        fb.validate_scratchpad()?;
        fb.write_to(&mut buf);

        // Compute and write CRC32 over [HEADER_CRC_AREA_OFF, checksum_field_offset).
        // The CRC covers everything after the mutable parquet_meta_file_size field at
        // offset 0: feature flags, column descriptors, row group blocks, and
        // footer.
        let checksum_field_offset = buf.len() - FOOTER_TRAILER_SIZE - FOOTER_CHECKSUM_SIZE;
        let crc = crc32fast::hash(&buf[HEADER_CRC_AREA_OFF..checksum_field_offset]);
        buf[checksum_field_offset..checksum_field_offset + FOOTER_CHECKSUM_SIZE]
            .copy_from_slice(&crc.to_le_bytes());

        // Patch the total committed file size into the header last. Readers
        // treat this as the MVCC commit signal — the file is only consistent
        // once this field agrees with the on-disk length through the trailer.
        let parquet_meta_file_size = buf.len() as u64;
        buf[HEADER_PARQUET_META_FILE_SIZE_OFF..HEADER_PARQUET_META_FILE_SIZE_OFF + 8]
            .copy_from_slice(&parquet_meta_file_size.to_le_bytes());

        Ok((buf, parquet_meta_file_size))
    }
}

/// Builds the dense bloom filter footer section from row group builders.
fn build_bloom_filter_footer_section(
    row_groups: &[RowGroupBlockBuilder],
    block_offsets: &[u64],
    bloom_filter_columns: &[u32],
    is_external: bool,
) -> Vec<u8> {
    let bloom_col_count = bloom_filter_columns.len();
    let rg_count = row_groups.len();
    let entry_size = if is_external { 16 } else { 4 };
    let mut section = vec![0u8; rg_count * bloom_col_count * entry_size];

    for (rg_idx, rg) in row_groups.iter().enumerate() {
        if is_external {
            for &(col_idx, pq_offset, pq_length) in rg.bloom_filter_external_entries() {
                if let Ok(pos) = bloom_filter_columns.binary_search(&(col_idx as u32)) {
                    let idx = rg_idx * bloom_col_count + pos;
                    let off = idx * 16;
                    section[off..off + 8].copy_from_slice(&pq_offset.to_le_bytes());
                    section[off + 8..off + 16].copy_from_slice(&pq_length.to_le_bytes());
                }
            }
        } else {
            let block_offset = block_offsets[rg_idx] as usize;
            let col_count = rg.chunks.len();
            let ool_start = block_offset + 8 + col_count * COLUMN_CHUNK_SIZE;
            for &(col_idx, ool_offset) in rg.bloom_filter_inlined_entries() {
                if let Ok(pos) = bloom_filter_columns.binary_search(&(col_idx as u32)) {
                    let abs_offset = ool_start + ool_offset;
                    let shifted = (abs_offset >> BLOCK_ALIGNMENT_SHIFT) as u32;
                    let idx = rg_idx * bloom_col_count + pos;
                    let off = idx * 4;
                    section[off..off + 4].copy_from_slice(&shifted.to_le_bytes());
                }
            }
        }
    }

    section
}

// ── ParquetMetaUpdateWriter (update mode) ─────────────────────────────────────

/// Produces bytes to append to an existing `_pm` file for an incremental
/// update (new/changed row group blocks + new footer).
///
/// Unchanged row groups keep their original offsets in the new footer.
pub struct ParquetMetaUpdateWriter<'a> {
    /// The committed `_pm` plus any bytes up to the append base. Spans at least
    /// `existing_parquet_meta_file_size` (the parse anchor) and, when an orphaned
    /// dead footer precedes the append base, the dead tail too -- the cumulative
    /// CRC in `finish_appending_at` folds those bytes in.
    existing: &'a [u8],
    existing_parquet_meta_file_size: u64,
    existing_footer_offset: u64,
    existing_footer_length: u32,
    /// (original_offset | None for new/replaced, builder)
    entries: Vec<RowGroupEntry>,
    parquet_footer_offset: u64,
    parquet_footer_length: u32,
    unused_bytes: u64,
    /// Bloom filter column indices from the existing header (empty if no bloom filters).
    bloom_filter_columns: Vec<u32>,
    /// Whether bloom filters are external in the existing file.
    is_bloom_external: bool,
    /// Existing bloom filter footer section bytes (per existing row group).
    /// For inlined: each entry is a Vec<u32> of shifted offsets, one per bloom column.
    /// For external: each entry is a Vec<(u64, u64)> of (offset, length) pairs.
    existing_bloom_inlined: Vec<Vec<u32>>,
    existing_bloom_external: Vec<Vec<(u64, u64)>>,
    /// Caller-set `seqTxn` for the new footer; `None` means "inherit from
    /// `prior_seq_txn`" (fires `debug_assert!` — see `finish()`).
    seq_txn: Option<SeqTxn>,
    prior_seq_txn: Option<SeqTxn>,
    /// Caller-set scratchpad for the new footer; `None` silently inherits
    /// `prior_scratchpad`. Unlike seq_txn, missing the setter is not a
    /// silent-state-divergence bug: a stale etag surfaces loudly as 412
    /// on the next chunk GET.
    scratchpad: Option<Vec<(u32, Vec<u8>)>>,
    prior_scratchpad: Vec<(u32, Vec<u8>)>,
}

enum RowGroupEntry {
    /// Reuse an existing block at this offset.
    Existing(u64),
    /// Write a new block.
    New(RowGroupBlockBuilder),
}

impl<'a> ParquetMetaUpdateWriter<'a> {
    /// Creates an update writer from the existing file slice and the committed
    /// `_pm` file size (the parse anchor). The slice must cover at least
    /// `existing_parquet_meta_file_size` bytes; it may extend further to an
    /// append base (see [`Self::finish_appending_at`]), which folds the bytes in
    /// between into the cumulative CRC. Bytes beyond the append base are unused.
    pub fn new(
        existing: &'a [u8],
        existing_parquet_meta_file_size: u64,
    ) -> ParquetMetaResult<Self> {
        // `existing` may span past the parse anchor (a dead-footer tail; see the
        // `existing` field). Parse against the committed parse anchor -- which
        // bounds from_file_size's own view -- and keep the full slice so
        // finish_appending_at can fold the tail into the cumulative CRC.
        let reader = ParquetMetaReader::from_file_size(existing, existing_parquet_meta_file_size)?;
        let existing_footer_offset = reader.footer_offset();
        let existing_footer_length =
            Self::read_trailer_footer_length(existing, existing_parquet_meta_file_size)?;
        let footer = reader.footer();
        let rg_count = footer.row_group_count() as usize;

        // Initialize entries with existing row group offsets.
        let mut entries = Vec::with_capacity(rg_count);
        for i in 0..rg_count {
            entries.push(RowGroupEntry::Existing(footer.row_group_block_offset(i)?));
        }

        // Parse existing bloom filter data.
        let bloom_filter_columns = reader.bloom_filter_columns();
        let is_bloom_external = reader.has_bloom_filters_external();
        let bloom_col_count = bloom_filter_columns.len();
        let mut existing_bloom_inlined = Vec::new();
        let mut existing_bloom_external = Vec::new();

        if reader.has_bloom_filters() {
            for rg_idx in 0..rg_count {
                if is_bloom_external {
                    let mut ext_entries = Vec::with_capacity(bloom_col_count);
                    for pos in 0..bloom_col_count {
                        let (off, len) = reader.bloom_filter_parquet_ref(rg_idx, pos)?;
                        ext_entries.push((off, len));
                    }
                    existing_bloom_external.push(ext_entries);
                } else {
                    let mut inl_entries = Vec::with_capacity(bloom_col_count);
                    for pos in 0..bloom_col_count {
                        let abs_off = reader.bloom_filter_offset_in_pm(rg_idx, pos)?;
                        // Store as shifted value (>>3).
                        let shifted = (abs_off >> BLOCK_ALIGNMENT_SHIFT) as u32;
                        inl_entries.push(shifted);
                    }
                    existing_bloom_inlined.push(inl_entries);
                }
            }
        }

        let prior_seq_txn = footer.seq_txn();
        let prior_scratchpad: Vec<(u32, Vec<u8>)> = footer
            .scratchpad_entries()
            .map(|(code, content)| (code, content.to_vec()))
            .collect();

        Ok(Self {
            existing,
            existing_parquet_meta_file_size,
            existing_footer_offset,
            existing_footer_length,
            entries,
            parquet_footer_offset: footer.parquet_footer_offset(),
            parquet_footer_length: footer.parquet_footer_length(),
            unused_bytes: footer.unused_bytes(),
            bloom_filter_columns,
            is_bloom_external,
            existing_bloom_inlined,
            existing_bloom_external,
            seq_txn: None,
            prior_seq_txn,
            scratchpad: None,
            prior_scratchpad,
        })
    }

    pub fn unused_bytes(&mut self, unused_bytes: u64) -> &mut Self {
        self.unused_bytes = unused_bytes;
        self
    }

    /// Sets the per-footer `seqTxn` for the new footer. Production paths
    /// must call this on every append: a forgotten setter silently
    /// desynchronizes downstream HEAD-and-skip checks. Missing it triggers
    /// `debug_assert!` in `finish()` and falls back to the prior footer's
    /// value.
    pub fn seq_txn(&mut self, value: SeqTxn) -> &mut Self {
        self.seq_txn = Some(value);
        self
    }

    /// Explicit opt-in to keep `prior_seq_txn` unchanged on `finish()`.
    pub fn inherit_seq_txn(&mut self) -> &mut Self {
        self.seq_txn = self.prior_seq_txn;
        self
    }

    /// Replaces the scratchpad on the new footer. Empty `Vec` clears it.
    /// Skipping the setter silently inherits the prior footer's scratchpad.
    pub fn set_scratchpad_entries(&mut self, entries: Vec<(u32, Vec<u8>)>) -> &mut Self {
        self.scratchpad = Some(entries);
        self
    }

    /// Replaces a row group at `index` with a new block.
    pub fn replace_row_group(
        &mut self,
        index: usize,
        builder: RowGroupBlockBuilder,
    ) -> ParquetMetaResult<&mut Self> {
        let len = self.entries.len();
        let slot = self.entries.get_mut(index).ok_or_else(|| {
            parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "row group index {} out of range [0, {})",
                index,
                len
            )
        })?;
        *slot = RowGroupEntry::New(builder);
        Ok(self)
    }

    /// Appends a new row group.
    pub fn add_row_group(&mut self, builder: RowGroupBlockBuilder) -> &mut Self {
        self.entries.push(RowGroupEntry::New(builder));
        self
    }

    pub fn parquet_footer(&mut self, offset: u64, length: u32) -> &mut Self {
        self.parquet_footer_offset = offset;
        self.parquet_footer_length = length;
        self
    }

    /// Reads the footer_length_through_crc value from the trailer at
    /// `parquet_meta_file_size - FOOTER_TRAILER_SIZE`. The trailer's position is
    /// governed by the committed `parquet_meta_file_size`, not the slice length —
    /// callers may pass a slice longer than the committed view (e.g. an
    /// mmap that includes trailing bytes from an in-progress append).
    fn read_trailer_footer_length(
        data: &[u8],
        parquet_meta_file_size: u64,
    ) -> ParquetMetaResult<u32> {
        let parquet_meta_file_size_usize =
            usize::try_from(parquet_meta_file_size).map_err(|_| {
                parquet_meta_err!(
                    ParquetMetaErrorKind::Truncated,
                    "_pm file size {} exceeds addressable range",
                    parquet_meta_file_size
                )
            })?;
        if parquet_meta_file_size_usize < FOOTER_TRAILER_SIZE
            || data.len() < parquet_meta_file_size_usize
        {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "data too small for footer trailer at _pm file size {}",
                parquet_meta_file_size
            ));
        }
        let trailer_start = parquet_meta_file_size_usize - FOOTER_TRAILER_SIZE;
        Ok(u32::from_le_bytes(
            data[trailer_start..trailer_start + FOOTER_TRAILER_SIZE]
                .try_into()
                .expect("slice is 4 bytes"),
        ))
    }

    /// Finishes the update with `append_base == parse anchor` -- the common case,
    /// no dead-footer tail before the new snapshot. See
    /// [`Self::finish_appending_at`].
    #[must_use = "returns the append bytes and new parquet_meta_file_size"]
    pub fn finish(&self) -> ParquetMetaResult<(Vec<u8>, u64)> {
        self.finish_appending_at(self.existing_parquet_meta_file_size)
    }

    /// Finishes the update, writing the new snapshot starting at `append_base`.
    ///
    /// `append_base` is where the appended bytes land. It equals the parse anchor
    /// (`existing_parquet_meta_file_size`) except in the crash window: a prior
    /// update published a footer at `[parse anchor, append_base)` by patching the
    /// header to `append_base`, then crashed before its `_txn` commit. New blocks
    /// and the new footer land at/after `append_base`, so the writer never
    /// overwrites that dead footer or any byte a stale reader mapped; the new
    /// footer's `prev` still points at the parse anchor, orphaning the dead footer
    /// out of the MVCC chain.
    ///
    /// Returns `(append_bytes, new_parquet_meta_file_size)`. The cumulative CRC
    /// inside `append_bytes` covers the whole file through the new CRC field --
    /// including any dead tail in `[parse anchor, append_base)`, since a reader
    /// resolving the new footer re-hashes every physical byte up to it. The
    /// caller publishes the snapshot by patching `new_parquet_meta_file_size` into
    /// the header (offset 0) as the last write.
    ///
    /// The caller guarantees `parse anchor <= append_base <= existing.len()`;
    /// `update_parquet_metadata` validates the header-derived `append_base`.
    #[must_use = "returns the append bytes and new parquet_meta_file_size"]
    pub fn finish_appending_at(&self, append_base: u64) -> ParquetMetaResult<(Vec<u8>, u64)> {
        let append_start = append_base as usize;
        debug_assert!(
            append_base >= self.existing_parquet_meta_file_size,
            "append base precedes parse anchor"
        );
        debug_assert!(
            append_start <= self.existing.len(),
            "retained slice shorter than append base"
        );

        let mut append_buf = Vec::new();

        // Write new/replaced row group blocks and collect final offsets.
        let mut final_offsets: Vec<u64> = Vec::with_capacity(self.entries.len());
        for entry in &self.entries {
            match entry {
                RowGroupEntry::Existing(offset) => {
                    final_offsets.push(*offset);
                }
                RowGroupEntry::New(builder) => {
                    // Pad relative to the absolute file position.
                    let abs_len = append_start + append_buf.len();
                    let padding = (BLOCK_ALIGNMENT - (abs_len % BLOCK_ALIGNMENT)) % BLOCK_ALIGNMENT;
                    append_buf.extend(std::iter::repeat_n(0u8, padding));

                    let abs_offset = append_start + append_buf.len();
                    // Write the block content directly (no extra alignment since
                    // we already padded).
                    append_buf.extend_from_slice(&builder.num_rows.to_le_bytes());
                    for chunk in &builder.chunks {
                        let bytes: &[u8; 64] = unsafe {
                            &*(chunk as *const super::column_chunk::ColumnChunkRaw
                                as *const [u8; 64])
                        };
                        append_buf.extend_from_slice(bytes);
                    }
                    append_buf.extend_from_slice(&builder.out_of_line);

                    final_offsets.push(abs_offset as u64);
                }
            }
        }

        // Build bloom filter footer section for all row groups (existing + new).
        let bloom_col_count = self.bloom_filter_columns.len();
        let bloom_section = if bloom_col_count > 0 {
            let entry_size = if self.is_bloom_external { 16 } else { 4 };
            let total_rg = self.entries.len();
            let mut section = vec![0u8; total_rg * bloom_col_count * entry_size];

            for (rg_idx, entry) in self.entries.iter().enumerate() {
                match entry {
                    RowGroupEntry::Existing(_) => {
                        // Copy through existing bloom filter entries.
                        // Index by rg_idx (original position), not a running counter,
                        // because existing_bloom_* is indexed by original row group position.
                        if self.is_bloom_external {
                            if let Some(ext) = self.existing_bloom_external.get(rg_idx) {
                                for (pos, &(off, len)) in ext.iter().enumerate() {
                                    let idx = rg_idx * bloom_col_count + pos;
                                    let o = idx * 16;
                                    section[o..o + 8].copy_from_slice(&off.to_le_bytes());
                                    section[o + 8..o + 16].copy_from_slice(&len.to_le_bytes());
                                }
                            }
                        } else if let Some(inl) = self.existing_bloom_inlined.get(rg_idx) {
                            for (pos, &shifted) in inl.iter().enumerate() {
                                let idx = rg_idx * bloom_col_count + pos;
                                let o = idx * 4;
                                section[o..o + 4].copy_from_slice(&shifted.to_le_bytes());
                            }
                        }
                    }
                    RowGroupEntry::New(builder) => {
                        let block_offset = final_offsets[rg_idx] as usize;
                        let col_count = builder.chunks.len();
                        if self.is_bloom_external {
                            for &(col_idx, pq_offset, pq_length) in
                                builder.bloom_filter_external_entries()
                            {
                                if let Ok(pos) =
                                    self.bloom_filter_columns.binary_search(&(col_idx as u32))
                                {
                                    let idx = rg_idx * bloom_col_count + pos;
                                    let o = idx * 16;
                                    section[o..o + 8].copy_from_slice(&pq_offset.to_le_bytes());
                                    section[o + 8..o + 16]
                                        .copy_from_slice(&pq_length.to_le_bytes());
                                }
                            }
                        } else {
                            let ool_start = block_offset + 8 + col_count * COLUMN_CHUNK_SIZE;
                            for &(col_idx, ool_offset) in builder.bloom_filter_inlined_entries() {
                                if let Ok(pos) =
                                    self.bloom_filter_columns.binary_search(&(col_idx as u32))
                                {
                                    let abs_offset = ool_start + ool_offset;
                                    let shifted = (abs_offset >> BLOCK_ALIGNMENT_SHIFT) as u32;
                                    let idx = rg_idx * bloom_col_count + pos;
                                    let o = idx * 4;
                                    section[o..o + 4].copy_from_slice(&shifted.to_le_bytes());
                                }
                            }
                        }
                    }
                }
            }
            section
        } else {
            Vec::new()
        };

        // Write the new footer. The MVCC chain walks back via the committed
        // parquet_meta_file_size at each step — not via a direct footer
        // offset — so store the existing committed size here. A reader
        // walking back derives the old footer location from the trailer
        // at `existing_parquet_meta_file_size - 4`.
        let mut fb = FooterBuilder::new(self.parquet_footer_offset, self.parquet_footer_length);
        fb.unused_bytes(self.unused_bytes);
        fb.prev_parquet_meta_file_size(self.existing_parquet_meta_file_size);
        for &offset in &final_offsets {
            fb.add_row_group_offset(offset)?;
        }
        fb.set_bloom_filter_section(bloom_section);
        let effective_seq_txn = match self.seq_txn {
            Some(value) => value,
            None => {
                debug_assert!(
                    self.prior_seq_txn.is_none(),
                    "ParquetMetaUpdateWriter.finish: seq_txn not set but prior footer had SEQ_TXN_BIT={:?}; production paths must call .seq_txn(new) to refresh it or .inherit_seq_txn() to keep the prior value",
                    self.prior_seq_txn
                );
                self.prior_seq_txn.unwrap_or(SeqTxn::UNSET)
            }
        };
        fb.set_seq_txn(effective_seq_txn);
        let effective_scratchpad = match &self.scratchpad {
            Some(entries) => entries.clone(),
            None => self.prior_scratchpad.clone(),
        };
        fb.set_scratchpad_entries(effective_scratchpad);
        fb.validate_scratchpad()?;
        fb.write_to(&mut append_buf);

        // Resume CRC32 from the committed footer's checksum. The old CRC covers
        // [HEADER_CRC_AREA_OFF, old_crc_field) of the committed bytes; continue
        // over the committed CRC field + trailer, then any dead-footer tail in
        // [parse anchor, append_base), then the new append bytes up to the new
        // CRC field -- the full physical range a reader re-hashes when resolving
        // the new footer. from_file_size validated the committed footer, so its
        // offset/length are in bounds.
        let footer_usize = self.existing_footer_offset as usize;
        let old_crc_field_offset =
            footer_usize + self.existing_footer_length as usize - FOOTER_CHECKSUM_SIZE;
        let old_crc = u32::from_le_bytes(
            self.existing[old_crc_field_offset..old_crc_field_offset + FOOTER_CHECKSUM_SIZE]
                .try_into()
                .expect("slice is 4 bytes"),
        );
        let checksum_field_abs =
            append_start + append_buf.len() - FOOTER_TRAILER_SIZE - FOOTER_CHECKSUM_SIZE;
        let mut hasher = crc32fast::Hasher::new_with_initial(old_crc);
        // Committed CRC field + trailer + any dead-footer tail: [old_crc_field, append_base).
        hasher.update(&self.existing[old_crc_field_offset..append_start]);
        // New bytes up to (but not including) the new checksum field.
        let new_bytes_before_crc = checksum_field_abs - append_start;
        hasher.update(&append_buf[..new_bytes_before_crc]);
        let crc = hasher.finalize();

        // Write CRC into the append buffer.
        let crc_offset_in_buf = append_buf.len() - FOOTER_TRAILER_SIZE - FOOTER_CHECKSUM_SIZE;
        append_buf[crc_offset_in_buf..crc_offset_in_buf + FOOTER_CHECKSUM_SIZE]
            .copy_from_slice(&crc.to_le_bytes());

        let new_parquet_meta_file_size = (append_start + append_buf.len()) as u64;
        Ok((append_buf, new_parquet_meta_file_size))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_chunk::ColumnChunkRaw;
    use crate::reader::ParquetMetaReader;
    use crate::types::{Codec, FieldRepetition};

    fn make_simple_file() -> (Vec<u8>, u64) {
        let mut w = ParquetMetaWriter::new();
        w.designated_timestamp(0);
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
        w.add_column("val", 1, 10, ColumnFlags::new(), 0, 0, 0, 0);
        w.add_sorting_column(0);

        let mut rg = RowGroupBlockBuilder::new(2);
        rg.set_num_rows(1000);
        let mut c0 = ColumnChunkRaw::zeroed();
        c0.codec = Codec::Snappy as u8;
        c0.num_values = 1000;
        rg.set_column_chunk(0, c0).unwrap();
        w.add_row_group(rg);

        w.parquet_footer(4096, 256);
        w.finish().unwrap()
    }

    #[test]
    fn create_and_read_back() {
        let (bytes, parquet_meta_file_size) = make_simple_file();
        let reader = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();

        assert_eq!(reader.column_count(), 2);
        assert_eq!(reader.row_group_count(), 1);
        assert_eq!(reader.designated_timestamp(), Some(0));
        assert_eq!(reader.parquet_footer_offset(), 4096);
        assert_eq!(reader.parquet_footer_length(), 256);

        let rg = reader.row_group(0).unwrap();
        assert_eq!(rg.num_rows(), 1000);
        let c = rg.column_chunk(0).unwrap();
        assert_eq!(c.codec().unwrap(), Codec::Snappy);
    }

    #[test]
    fn create_empty() {
        let mut w = ParquetMetaWriter::new();
        w.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let (bytes, parquet_meta_file_size) = w.finish().unwrap();

        let reader = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();
        assert_eq!(reader.column_count(), 1);
        assert_eq!(reader.row_group_count(), 0);
    }

    #[test]
    fn writer_round_trips_squash_tracker() {
        let mut w = ParquetMetaWriter::new();
        w.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        w.squash_tracker(99);
        let (bytes, parquet_meta_file_size) = w.finish().unwrap();

        let reader = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();
        assert!(reader.feature_flags().has_squash_tracker());
        assert_eq!(reader.squash_tracker(), Some(99));
    }

    #[test]
    fn writer_squash_tracker_default_omitted() {
        // Never calling squash_tracker() must produce a file with the bit clear.
        let mut w = ParquetMetaWriter::new();
        w.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let (bytes, parquet_meta_file_size) = w.finish().unwrap();

        let reader = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();
        assert!(!reader.feature_flags().has_squash_tracker());
        assert_eq!(reader.squash_tracker(), None);
    }

    #[test]
    fn writer_squash_tracker_survives_designated_timestamp_reset() {
        // designated_timestamp() rebuilds the header_builder; squash_tracker
        // is stored on the writer and re-applied at finish() time.
        let mut w = ParquetMetaWriter::new();
        w.squash_tracker(7);
        w.designated_timestamp(-1);
        w.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let (bytes, parquet_meta_file_size) = w.finish().unwrap();

        let reader = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();
        assert_eq!(reader.squash_tracker(), Some(7));
    }

    #[test]
    fn update_append_row_group() {
        let (original, existing_parquet_meta_file_size) = make_simple_file();

        let mut updater =
            ParquetMetaUpdateWriter::new(&original, existing_parquet_meta_file_size).unwrap();

        let mut rg = RowGroupBlockBuilder::new(2);
        rg.set_num_rows(500);
        let mut c = ColumnChunkRaw::zeroed();
        c.codec = Codec::Zstd as u8;
        c.num_values = 500;
        rg.set_column_chunk(0, c).unwrap();
        updater.add_row_group(rg);
        updater.parquet_footer(8192, 512);

        let (append_bytes, new_parquet_meta_file_size) = updater.finish().unwrap();

        // Construct the full updated file and patch the header's parquet_meta_file_size
        // to publish the new snapshot — mirrors what the Java writer does.
        let mut full = original.to_vec();
        full.extend_from_slice(&append_bytes);
        full[super::HEADER_PARQUET_META_FILE_SIZE_OFF
            ..super::HEADER_PARQUET_META_FILE_SIZE_OFF + 8]
            .copy_from_slice(&new_parquet_meta_file_size.to_le_bytes());

        let reader = ParquetMetaReader::from_file_size(&full, new_parquet_meta_file_size).unwrap();
        reader.verify_checksum().unwrap();
        assert_eq!(reader.row_group_count(), 2);
        assert_eq!(reader.parquet_footer_offset(), 8192);

        // Original row group still accessible.
        let rg0 = reader.row_group(0).unwrap();
        assert_eq!(rg0.num_rows(), 1000);

        // New row group.
        let rg1 = reader.row_group(1).unwrap();
        assert_eq!(rg1.num_rows(), 500);
        assert_eq!(rg1.column_chunk(0).unwrap().codec().unwrap(), Codec::Zstd);
    }

    #[test]
    fn update_appends_past_dead_footer() {
        // Committed snapshot: 1 row group, size s0 (the parse anchor).
        let (committed, s0) = make_simple_file();

        // Crash window: a prior update appended a footer at [s0, s1) and
        // published the header at s1, then crashed before its `_txn` commit. The
        // committed head stays s0 (the dead footer never matched `_txn`); the
        // header (the append base) is the dirty-ahead s1.
        let dead = {
            let mut u = ParquetMetaUpdateWriter::new(&committed, s0).unwrap();
            let mut rg = RowGroupBlockBuilder::new(2);
            rg.set_num_rows(111);
            let mut c = ColumnChunkRaw::zeroed();
            c.codec = Codec::Zstd as u8;
            c.num_values = 111;
            rg.set_column_chunk(0, c).unwrap();
            u.add_row_group(rg);
            u.parquet_footer(8192, 512);
            u.finish().unwrap().0
        };
        let mut physical_after_fail = committed.clone();
        physical_after_fail.extend_from_slice(&dead);
        let s1 = physical_after_fail.len() as u64;
        assert!(s1 > s0, "dead footer must extend the physical file");

        // Next successful in-place update: parse the committed head (s0), append
        // PAST the dead footer at the append base (the dirty-ahead header, s1).
        let mut updater = ParquetMetaUpdateWriter::new(&physical_after_fail, s0).unwrap();
        let mut rg = RowGroupBlockBuilder::new(2);
        rg.set_num_rows(222);
        let mut c = ColumnChunkRaw::zeroed();
        c.codec = Codec::Snappy as u8;
        c.num_values = 222;
        rg.set_column_chunk(0, c).unwrap();
        updater.add_row_group(rg);
        updater.parquet_footer(9000, 256);
        let (append_bytes, s2) = updater.finish_appending_at(s1).unwrap();

        // The new bytes land at the append base (s1), not the committed head.
        assert_eq!(s2, s1 + append_bytes.len() as u64);

        let mut full = physical_after_fail.clone();
        full.extend_from_slice(&append_bytes);
        full[super::HEADER_PARQUET_META_FILE_SIZE_OFF
            ..super::HEADER_PARQUET_META_FILE_SIZE_OFF + 8]
            .copy_from_slice(&s2.to_le_bytes());

        // The writer appended only past s1: every committed and dead byte
        // (besides the header's patched size field at offset 0) is untouched.
        assert_eq!(
            &full[8..s1 as usize],
            &physical_after_fail[8..s1 as usize],
            "committed + dead region must be byte-identical (writer only appends)"
        );
        assert_eq!(
            &full[s0 as usize..s1 as usize],
            &dead[..],
            "the orphaned dead footer must be left untouched"
        );

        // A reader resolving the new committed size validates the cumulative
        // CRC -- which now spans the dead region -- and sees the committed row
        // group plus the new one.
        let reader = ParquetMetaReader::from_file_size(&full, s2).unwrap();
        reader.verify_checksum().unwrap();
        assert_eq!(reader.row_group_count(), 2);
        // The new footer chains onto the committed head, orphaning the dead
        // footer out of the MVCC chain.
        assert_eq!(reader.prev_parquet_meta_file_size(), s0);
        assert_eq!(reader.row_group(0).unwrap().num_rows(), 1000); // reused committed RG
        assert_eq!(reader.row_group(1).unwrap().num_rows(), 222); // new RG past the dead footer

        // The committed snapshot still resolves independently at s0.
        let old = ParquetMetaReader::from_file_size(&committed, s0).unwrap();
        assert_eq!(old.row_group_count(), 1);
        old.verify_checksum().unwrap();
    }

    #[test]
    fn update_replace_row_group() {
        // Build a file with 2 row groups.
        let mut w = ParquetMetaWriter::new();
        w.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);

        let mut rg0 = RowGroupBlockBuilder::new(1);
        rg0.set_num_rows(100);
        w.add_row_group(rg0);

        let mut rg1 = RowGroupBlockBuilder::new(1);
        rg1.set_num_rows(200);
        w.add_row_group(rg1);

        let (original, existing_parquet_meta_file_size) = w.finish().unwrap();

        // Replace row group 1.
        let mut updater =
            ParquetMetaUpdateWriter::new(&original, existing_parquet_meta_file_size).unwrap();
        let mut new_rg1 = RowGroupBlockBuilder::new(1);
        new_rg1.set_num_rows(999);
        updater.replace_row_group(1, new_rg1).unwrap();

        let (append_bytes, new_parquet_meta_file_size) = updater.finish().unwrap();

        let mut full = original.to_vec();
        full.extend_from_slice(&append_bytes);
        full[super::HEADER_PARQUET_META_FILE_SIZE_OFF
            ..super::HEADER_PARQUET_META_FILE_SIZE_OFF + 8]
            .copy_from_slice(&new_parquet_meta_file_size.to_le_bytes());

        let reader = ParquetMetaReader::from_file_size(&full, new_parquet_meta_file_size).unwrap();
        reader.verify_checksum().unwrap();
        assert_eq!(reader.row_group_count(), 2);

        // Row group 0 unchanged.
        assert_eq!(reader.row_group(0).unwrap().num_rows(), 100);
        // Row group 1 replaced.
        assert_eq!(reader.row_group(1).unwrap().num_rows(), 999);
    }

    #[test]
    fn replace_row_group_out_of_range() {
        let (original, existing_parquet_meta_file_size) = make_simple_file();
        let mut updater =
            ParquetMetaUpdateWriter::new(&original, existing_parquet_meta_file_size).unwrap();
        let rg = RowGroupBlockBuilder::new(2);
        // Only 1 row group exists (index 0), so index 5 is out of range.
        assert!(updater.replace_row_group(5, rg).is_err());
    }

    #[test]
    fn update_replace_row_group_with_bloom_filters() {
        // Build a file with 3 row groups, each with a distinct bloom filter on column 0.
        let mut w = ParquetMetaWriter::new();
        w.add_column("a", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);

        let bf0 = vec![0xAA_u8; 64];
        let bf1 = vec![0xBB_u8; 64];
        let bf2 = vec![0xCC_u8; 64];

        let mut rg0 = RowGroupBlockBuilder::new(1);
        rg0.set_num_rows(100);
        rg0.add_bloom_filter(0, &bf0).unwrap();
        w.add_row_group(rg0);

        let mut rg1 = RowGroupBlockBuilder::new(1);
        rg1.set_num_rows(200);
        rg1.add_bloom_filter(0, &bf1).unwrap();
        w.add_row_group(rg1);

        let mut rg2 = RowGroupBlockBuilder::new(1);
        rg2.set_num_rows(300);
        rg2.add_bloom_filter(0, &bf2).unwrap();
        w.add_row_group(rg2);

        let (original, existing_parquet_meta_file_size) = w.finish().unwrap();

        // Replace row group 1 with a new bloom filter.
        let bf_new = vec![0xDD_u8; 64];
        let mut updater =
            ParquetMetaUpdateWriter::new(&original, existing_parquet_meta_file_size).unwrap();
        let mut new_rg1 = RowGroupBlockBuilder::new(1);
        new_rg1.set_num_rows(999);
        new_rg1.add_bloom_filter(0, &bf_new).unwrap();
        updater.replace_row_group(1, new_rg1).unwrap();

        let (append_bytes, new_parquet_meta_file_size) = updater.finish().unwrap();

        let mut full = original.to_vec();
        full.extend_from_slice(&append_bytes);
        full[super::HEADER_PARQUET_META_FILE_SIZE_OFF
            ..super::HEADER_PARQUET_META_FILE_SIZE_OFF + 8]
            .copy_from_slice(&new_parquet_meta_file_size.to_le_bytes());

        let reader = ParquetMetaReader::from_file_size(&full, new_parquet_meta_file_size).unwrap();
        reader.verify_checksum().unwrap();
        assert_eq!(reader.row_group_count(), 3);

        // Verify bloom filter data for each row group.
        for rg_idx in 0..3 {
            let off = reader.bloom_filter_offset_in_pm(rg_idx, 0).unwrap();
            assert_ne!(off, 0, "RG{rg_idx} bloom offset should not be absent");
            let bf_data = &full[off as usize..];
            let bf_len = i32::from_le_bytes(bf_data[..4].try_into().unwrap()) as usize;
            assert_eq!(bf_len, 64);

            let expected = match rg_idx {
                0 => &bf0,
                1 => &bf_new,
                2 => &bf2,
                _ => unreachable!(),
            };
            assert_eq!(
                &bf_data[4..4 + bf_len],
                expected.as_slice(),
                "RG{rg_idx} bloom filter data mismatch"
            );
        }
    }

    #[test]
    fn default_creates_same_as_new() {
        let mut w = ParquetMetaWriter::default();
        w.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let (bytes, parquet_meta_file_size) = w.finish().unwrap();
        let reader = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();
        assert_eq!(reader.column_count(), 1);
        assert_eq!(reader.designated_timestamp(), None);
    }

    fn make_file_with_seq_txn(seq_txn: SeqTxn) -> (Vec<u8>, u64) {
        let mut w = ParquetMetaWriter::new();
        w.designated_timestamp(0);
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
        let mut rg = RowGroupBlockBuilder::new(1);
        rg.set_num_rows(1);
        w.add_row_group(rg);
        w.parquet_footer(4096, 256);
        w.seq_txn(seq_txn);
        w.finish().unwrap()
    }

    #[test]
    fn writer_round_trips_seq_txn() {
        let mut w = ParquetMetaWriter::new();
        w.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        w.seq_txn(SeqTxn::new(123));
        let (bytes, parquet_meta_file_size) = w.finish().unwrap();

        let reader = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();
        assert!(reader.footer_feature_flags().has_seq_txn());
        assert_eq!(reader.seq_txn(), Some(SeqTxn::new(123)));
    }

    #[test]
    fn writer_seq_txn_default_omitted() {
        let mut w = ParquetMetaWriter::new();
        w.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        let (bytes, parquet_meta_file_size) = w.finish().unwrap();

        let reader = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();
        assert!(!reader.footer_feature_flags().has_seq_txn());
        assert_eq!(reader.seq_txn(), None);
    }

    #[test]
    fn update_writer_carries_seq_txn() {
        let (original, existing_size) = make_file_with_seq_txn(SeqTxn::new(1));

        let mut updater = ParquetMetaUpdateWriter::new(&original, existing_size).unwrap();
        updater.seq_txn(SeqTxn::new(2));
        let mut rg = RowGroupBlockBuilder::new(1);
        rg.set_num_rows(7);
        updater.add_row_group(rg);
        let (append_bytes, new_size) = updater.finish().unwrap();

        let mut full = original.clone();
        full.extend_from_slice(&append_bytes);
        full[super::HEADER_PARQUET_META_FILE_SIZE_OFF
            ..super::HEADER_PARQUET_META_FILE_SIZE_OFF + 8]
            .copy_from_slice(&new_size.to_le_bytes());

        let reader = ParquetMetaReader::from_file_size(&full, new_size).unwrap();
        assert_eq!(reader.seq_txn(), Some(SeqTxn::new(2)));
    }

    #[test]
    fn update_writer_inherits_seq_txn_when_unset() {
        // Release-only: the inherit-from-prior fallback. Debug builds hit
        // the debug_assert! covered by the test below.
        if cfg!(debug_assertions) {
            return;
        }
        let (original, existing_size) = make_file_with_seq_txn(SeqTxn::new(5));

        let mut updater = ParquetMetaUpdateWriter::new(&original, existing_size).unwrap();
        let mut rg = RowGroupBlockBuilder::new(1);
        rg.set_num_rows(7);
        updater.add_row_group(rg);
        let (append_bytes, new_size) = updater.finish().unwrap();

        let mut full = original.clone();
        full.extend_from_slice(&append_bytes);
        full[super::HEADER_PARQUET_META_FILE_SIZE_OFF
            ..super::HEADER_PARQUET_META_FILE_SIZE_OFF + 8]
            .copy_from_slice(&new_size.to_le_bytes());

        let reader = ParquetMetaReader::from_file_size(&full, new_size).unwrap();
        assert_eq!(reader.seq_txn(), Some(SeqTxn::new(5)));
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "seq_txn not set")]
    fn update_writer_debug_asserts_when_unset_and_prior_present() {
        let (original, existing_size) = make_file_with_seq_txn(SeqTxn::new(5));
        let mut updater = ParquetMetaUpdateWriter::new(&original, existing_size).unwrap();
        let mut rg = RowGroupBlockBuilder::new(1);
        rg.set_num_rows(7);
        updater.add_row_group(rg);
        let _ = updater.finish();
    }

    #[test]
    fn update_writer_inherits_seq_txn_when_opt_in_set() {
        // Callers that legitimately keep the prior seq_txn (e.g. a
        // scratchpad-only patch) call .inherit_seq_txn() to silence
        // the debug_assert while still landing the prior value on the
        // new footer.
        let (original, existing_size) = make_file_with_seq_txn(SeqTxn::new(11));

        let mut updater = ParquetMetaUpdateWriter::new(&original, existing_size).unwrap();
        updater.inherit_seq_txn();
        let mut rg = RowGroupBlockBuilder::new(1);
        rg.set_num_rows(7);
        updater.add_row_group(rg);
        let (append_bytes, new_size) = updater.finish().unwrap();

        let mut full = original.clone();
        full.extend_from_slice(&append_bytes);
        full[super::HEADER_PARQUET_META_FILE_SIZE_OFF
            ..super::HEADER_PARQUET_META_FILE_SIZE_OFF + 8]
            .copy_from_slice(&new_size.to_le_bytes());

        let reader = ParquetMetaReader::from_file_size(&full, new_size).unwrap();
        assert_eq!(reader.seq_txn(), Some(SeqTxn::new(11)));
    }

    #[test]
    fn update_writer_inherit_seq_txn_on_empty_prior_lands_unset() {
        // No prior seq_txn -> inherit opt-in is a no-op; the new
        // footer omits the seq_txn section just as it would without
        // the opt-in.
        let (original, existing_size) = make_simple_file();
        let mut updater = ParquetMetaUpdateWriter::new(&original, existing_size).unwrap();
        updater.inherit_seq_txn();
        let mut rg = RowGroupBlockBuilder::new(1);
        rg.set_num_rows(7);
        updater.add_row_group(rg);
        let (append_bytes, new_size) = updater.finish().unwrap();

        let mut full = original.clone();
        full.extend_from_slice(&append_bytes);
        full[super::HEADER_PARQUET_META_FILE_SIZE_OFF
            ..super::HEADER_PARQUET_META_FILE_SIZE_OFF + 8]
            .copy_from_slice(&new_size.to_le_bytes());

        let reader = ParquetMetaReader::from_file_size(&full, new_size).unwrap();
        assert_eq!(reader.seq_txn(), None);
    }

    fn make_file_with_scratchpad(entries: Vec<(u32, Vec<u8>)>) -> (Vec<u8>, u64) {
        let mut w = ParquetMetaWriter::new();
        w.designated_timestamp(0);
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
        let mut rg = RowGroupBlockBuilder::new(1);
        rg.set_num_rows(1);
        w.add_row_group(rg);
        w.parquet_footer(4096, 256);
        w.seq_txn(SeqTxn::new(1));
        w.set_scratchpad_entries(entries);
        w.finish().unwrap()
    }

    #[test]
    fn writer_round_trips_scratchpad() {
        let mut w = ParquetMetaWriter::new();
        w.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        w.set_scratchpad_entries(vec![(0xDEAD_BEEF, b"hello".to_vec())]);
        let (bytes, parquet_meta_file_size) = w.finish().unwrap();

        let reader = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();
        assert_eq!(reader.scratchpad_entry(0xDEAD_BEEF), Some(&b"hello"[..]));
    }

    #[test]
    fn finish_rejects_oversized_scratchpad_instead_of_writing_unreadable_footer() {
        // A scratchpad past MAX_SCRATCHPAD_SIZE would serialize a footer this crate's
        // reader then refuses to open. finish() must reject it up front (release builds
        // have no debug_assert), returning the same error the reader would raise --
        // never emitting the unreadable footer. Payload = 4 + 8 + MAX = MAX + 12 > MAX.
        let mut w = ParquetMetaWriter::new();
        w.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        w.set_scratchpad_entries(vec![(0, vec![0u8; crate::types::MAX_SCRATCHPAD_SIZE])]);
        let err = w
            .finish()
            .expect_err("finish must reject an oversized scratchpad");
        assert_eq!(err.kind, ParquetMetaErrorKind::InvalidValue);
    }

    #[test]
    fn update_writer_carries_explicit_scratchpad() {
        let (original, existing_size) = make_file_with_scratchpad(vec![(1, b"first".to_vec())]);

        let mut updater = ParquetMetaUpdateWriter::new(&original, existing_size).unwrap();
        updater.seq_txn(SeqTxn::new(2));
        updater.set_scratchpad_entries(vec![(1, b"second".to_vec())]);
        let mut rg = RowGroupBlockBuilder::new(1);
        rg.set_num_rows(7);
        updater.add_row_group(rg);
        let (append_bytes, new_size) = updater.finish().unwrap();

        let mut full = original.clone();
        full.extend_from_slice(&append_bytes);
        full[super::HEADER_PARQUET_META_FILE_SIZE_OFF
            ..super::HEADER_PARQUET_META_FILE_SIZE_OFF + 8]
            .copy_from_slice(&new_size.to_le_bytes());

        let reader = ParquetMetaReader::from_file_size(&full, new_size).unwrap();
        assert_eq!(reader.scratchpad_entry(1), Some(&b"second"[..]));
    }

    #[test]
    fn update_writer_inherits_scratchpad_silently_when_unset() {
        let (original, existing_size) =
            make_file_with_scratchpad(vec![(9, b"inherit-me".to_vec())]);

        let mut updater = ParquetMetaUpdateWriter::new(&original, existing_size).unwrap();
        // Set only seq_txn to avoid its own debug_assert; leave scratchpad
        // untouched — silent inherit is the intended behavior.
        updater.seq_txn(SeqTxn::new(2));
        let mut rg = RowGroupBlockBuilder::new(1);
        rg.set_num_rows(7);
        updater.add_row_group(rg);
        let (append_bytes, new_size) = updater.finish().unwrap();

        let mut full = original.clone();
        full.extend_from_slice(&append_bytes);
        full[super::HEADER_PARQUET_META_FILE_SIZE_OFF
            ..super::HEADER_PARQUET_META_FILE_SIZE_OFF + 8]
            .copy_from_slice(&new_size.to_le_bytes());

        let reader = ParquetMetaReader::from_file_size(&full, new_size).unwrap();
        assert_eq!(reader.scratchpad_entry(9), Some(&b"inherit-me"[..]));
    }

    #[test]
    fn update_writer_clears_scratchpad_when_set_to_empty() {
        let (original, existing_size) =
            make_file_with_scratchpad(vec![(5, b"will-vanish".to_vec())]);
        let original_parquet_size = 4096u64 + 256 + 8;

        let mut updater = ParquetMetaUpdateWriter::new(&original, existing_size).unwrap();
        updater.seq_txn(SeqTxn::new(2));
        updater.set_scratchpad_entries(vec![]);
        updater.parquet_footer(8192, 256);
        let mut rg = RowGroupBlockBuilder::new(1);
        rg.set_num_rows(7);
        updater.add_row_group(rg);
        let (append_bytes, new_size) = updater.finish().unwrap();

        let mut full = original.clone();
        full.extend_from_slice(&append_bytes);
        full[super::HEADER_PARQUET_META_FILE_SIZE_OFF
            ..super::HEADER_PARQUET_META_FILE_SIZE_OFF + 8]
            .copy_from_slice(&new_size.to_le_bytes());

        let latest = ParquetMetaReader::from_file_size(&full, new_size).unwrap();
        assert_eq!(latest.scratchpad_entry(5), None);
        assert!(!latest.footer_feature_flags().has_scratchpad());

        // Old footer still has the entry via MVCC chain walk.
        let (_offset, prior_footer) =
            ParquetMetaReader::find_footer_for_parquet_size(&full, new_size, original_parquet_size)
                .unwrap();
        assert_eq!(prior_footer.scratchpad_entry(5), Some(&b"will-vanish"[..]));
    }

    #[test]
    fn update_writer_scratchpad_independent_per_footer() {
        let (original, existing_size) = make_file_with_scratchpad(vec![(1, b"A".to_vec())]);
        let original_parquet_size = 4096u64 + 256 + 8;

        let mut updater = ParquetMetaUpdateWriter::new(&original, existing_size).unwrap();
        updater.seq_txn(SeqTxn::new(2));
        updater.set_scratchpad_entries(vec![(1, b"B".to_vec())]);
        updater.parquet_footer(8192, 256);
        let mut rg = RowGroupBlockBuilder::new(1);
        rg.set_num_rows(3);
        updater.add_row_group(rg);
        let (append_bytes, new_size) = updater.finish().unwrap();

        let mut full = original.clone();
        full.extend_from_slice(&append_bytes);
        full[super::HEADER_PARQUET_META_FILE_SIZE_OFF
            ..super::HEADER_PARQUET_META_FILE_SIZE_OFF + 8]
            .copy_from_slice(&new_size.to_le_bytes());

        let latest = ParquetMetaReader::from_file_size(&full, new_size).unwrap();
        assert_eq!(latest.scratchpad_entry(1), Some(&b"B"[..]));

        let (_offset, prior_footer) =
            ParquetMetaReader::find_footer_for_parquet_size(&full, new_size, original_parquet_size)
                .unwrap();
        assert_eq!(prior_footer.scratchpad_entry(1), Some(&b"A"[..]));
    }

    #[test]
    fn scratchpad_crc_covers_payload() {
        let mut w = ParquetMetaWriter::new();
        w.add_column("x", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
        w.set_scratchpad_entries(vec![(0xCAFE, vec![0x11, 0x22, 0x33, 0x44])]);
        let (mut bytes, parquet_meta_file_size) = w.finish().unwrap();

        let reader = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();
        reader.verify_checksum().unwrap();

        // The scratchpad's first byte sits at: footer + FOOTER_FIXED_SIZE +
        // 0 row group entries + 0 bloom bytes + 0 seq_txn bytes (unset). The
        // very next 4 bytes are entry_count; flip a content byte.
        let end = parquet_meta_file_size as usize;
        let trailer = &bytes[end - FOOTER_TRAILER_SIZE..end];
        let footer_length = u32::from_le_bytes(trailer.try_into().unwrap()) as u64;
        let footer_off =
            (parquet_meta_file_size - FOOTER_TRAILER_SIZE as u64 - footer_length) as usize;
        let scratchpad_off = footer_off + crate::types::FOOTER_FIXED_SIZE;
        // Skip count(4) + code(4) + length(4) = 12 bytes; corrupt the first content byte.
        bytes[scratchpad_off + 12] ^= 0xFF;

        let reader2 = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();
        assert!(reader2.verify_checksum().is_err());
    }

    #[test]
    fn seq_txn_crc_covers_payload() {
        let (mut bytes, parquet_meta_file_size) = make_file_with_seq_txn(SeqTxn::new(0x1122_3344));

        let reader = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();
        reader.verify_checksum().unwrap();

        // seq_txn is the first footer-flag section (bit 0), so it sits right
        // after FOOTER_FIXED_SIZE + the row group entries + 0 bloom bytes.
        // make_file_with_seq_txn adds one row group, so account for it.
        let end = parquet_meta_file_size as usize;
        let trailer = &bytes[end - FOOTER_TRAILER_SIZE..end];
        let footer_length = u32::from_le_bytes(trailer.try_into().unwrap()) as u64;
        let footer_off =
            (parquet_meta_file_size - FOOTER_TRAILER_SIZE as u64 - footer_length) as usize;
        let seq_txn_off =
            footer_off + crate::types::FOOTER_FIXED_SIZE + crate::types::ROW_GROUP_ENTRY_SIZE;
        // The seq_txn payload is an i64 (8 bytes); corrupt one of them.
        bytes[seq_txn_off] ^= 0xFF;

        let reader2 = ParquetMetaReader::from_file_size(&bytes, parquet_meta_file_size).unwrap();
        assert!(reader2.verify_checksum().is_err());
    }

    #[test]
    fn update_writer_seq_txn_independent_per_footer() {
        // Old footers must keep their own seq_txn after a new append.
        let (original, existing_size) = make_file_with_seq_txn(SeqTxn::new(10));
        let original_parquet_size = 4096u64 + 256 + 8;

        let mut updater = ParquetMetaUpdateWriter::new(&original, existing_size).unwrap();
        updater.seq_txn(SeqTxn::new(20));
        updater.parquet_footer(8192, 256);
        let mut rg = RowGroupBlockBuilder::new(1);
        rg.set_num_rows(3);
        updater.add_row_group(rg);
        let (append_bytes, new_size) = updater.finish().unwrap();

        let mut full = original.clone();
        full.extend_from_slice(&append_bytes);
        full[super::HEADER_PARQUET_META_FILE_SIZE_OFF
            ..super::HEADER_PARQUET_META_FILE_SIZE_OFF + 8]
            .copy_from_slice(&new_size.to_le_bytes());

        let latest = ParquetMetaReader::from_file_size(&full, new_size).unwrap();
        assert_eq!(latest.seq_txn(), Some(SeqTxn::new(20)));

        let (_offset, prior_footer) =
            ParquetMetaReader::find_footer_for_parquet_size(&full, new_size, original_parquet_size)
                .unwrap();
        assert_eq!(prior_footer.seq_txn(), Some(SeqTxn::new(10)));
    }
}
