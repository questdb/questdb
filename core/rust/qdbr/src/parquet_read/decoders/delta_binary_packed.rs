//! Decoder for Parquet `DELTA_BINARY_PACKED` encoded primitive data.
//!
//! This adapter wraps `parquet2`'s delta decoder and implements the `Pushable`
//! interface so row-group decode can stream values directly into
//! `ColumnChunkBuffers`.

#![allow(clippy::manual_is_multiple_of)]

use std::fmt::Debug;

use num_traits::{AsPrimitive, WrappingAdd};
use parquet2::encoding::{uleb128, zigzag_leb128};

use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_read::column_sink::Pushable;
use crate::parquet_read::decoders::unpack::{unpack32, unpack64};
use crate::parquet_read::ColumnChunkBuffers;

#[derive(Debug, Default)]
pub(crate) struct Miniblock<'a> {
    pub(crate) data: &'a [u8],
    pub(crate) num_bits: u8,
}

pub(crate) struct MiniblockIterator<'a, U> {
    page_data: &'a [u8],
    miniblocks_per_block: usize,
    // Number of blocks remaining to be read
    blocks_remaining: u64,
    // The page header's declared value count (including the first value). Only
    // get_end_pointer reads it: the last block writes a bit-width byte for every
    // miniblock but a body only for the miniblocks that actually hold values, so
    // the end-of-stream walk must stop at ceil(values_in_block / miniblock_size)
    // miniblocks per block. Counting all miniblocks_per_block overshoots the
    // length stream on a foreign file that declares more than one miniblock per
    // block (QuestDB's own writer always emits one).
    value_count: u64,
    // offset in data for the bidwidths field of the current/next block depending on the state of the iterator
    block_bitwidths_offset: usize,
    // miniblock size in number of values, this is constant across all blocks
    pub(crate) miniblock_size: usize,
    // offset of the next miniblock to decode
    miniblock_offset: usize,
    // index of the next miniblock to decode
    miniblock_index: usize,
    pub(crate) min_delta: U,
    // Whether the page declares at least one value (value_count >= 1). When
    // false -- an empty values buffer or a value_count=0 header -- the page's
    // first value is a phantom default; the decoder must reject a value request
    // rather than emit it.
    pub(crate) has_values: bool,
}

impl<'a, U> MiniblockIterator<'a, U>
where
    U: Default + WrappingAdd + Copy + 'static,
    i64: AsPrimitive<U>,
{
    pub fn try_new(page_data: &'a [u8]) -> ParquetResult<(Self, U)> {
        if page_data.is_empty() {
            // A DELTA_BINARY_PACKED page with no non-null values can be written
            // as an empty values buffer (no header at all). QuestDB's current
            // writer does not do this -- since the all-null fix it emits a
            // self-describing value_count=0 header, which the normal path below
            // parses -- but pre-fix QuestDB files and foreign encoders can, so
            // this branch keeps them readable. The page's definition levels drive
            // null filling, so the value decoder is only asked for nulls and
            // never reads this iterator. `has_values` is false: if a value IS
            // requested anyway (a corrupt/truncated page whose definition levels
            // claim a non-null), the decoder rejects it on the first request with
            // "not enough values to iterate" rather than silently decoding the
            // phantom first value as 0.
            let iterator = Self {
                page_data,
                miniblocks_per_block: 0,
                blocks_remaining: 0,
                value_count: 0,
                block_bitwidths_offset: 0,
                miniblock_size: 0,
                miniblock_offset: 0,
                miniblock_index: 0,
                min_delta: U::default(),
                has_values: false,
            };
            return Ok((iterator, U::default()));
        }
        let (block_size, offset) = uleb128::decode(page_data)
            .map_err(|_| fmt_err!(Layout, "failed to decode block size"))?;
        if block_size == 0 {
            return Err(fmt_err!(
                Layout,
                "delta binary packed block size must be greater than zero"
            ));
        }
        // The parquet spec requires the block size to be a multiple of 128, and the
        // vendored parquet2 length decoder enforces it. MiniblockIterator computes
        // the value-bytes offset on the same page the parquet2 decoder reads the
        // lengths from, so it must agree on which headers are valid; without this
        // the two parsers could accept/reject a foreign page differently.
        if block_size % 128 != 0 {
            return Err(fmt_err!(
                Layout,
                "delta binary packed block size {block_size} must be a multiple of 128"
            ));
        }
        let page_data = &page_data[offset..];
        let (miniblocks_per_block, offset) = uleb128::decode(page_data)
            .map_err(|_| fmt_err!(Layout, "failed to decode miniblock count"))?;
        if miniblocks_per_block == 0 {
            return Err(fmt_err!(
                Layout,
                "delta binary packed miniblocks-per-block must be greater than zero"
            ));
        }
        if block_size % miniblocks_per_block != 0 {
            return Err(fmt_err!(
                Layout,
                "delta binary packed block size {block_size} is not divisible by miniblock count {miniblocks_per_block}"
            ));
        }
        let miniblock_size: usize = (block_size / miniblocks_per_block)
            .try_into()
            .map_err(|_| fmt_err!(Layout, "delta binary packed miniblock size overflow"))?;
        if miniblock_size == 0 || !miniblock_size.is_multiple_of(32) {
            return Err(fmt_err!(
                Layout,
                "delta binary packed miniblock size {miniblock_size} must be a non-zero multiple of 32 values"
            ));
        }
        let miniblocks_per_block: usize = miniblocks_per_block
            .try_into()
            .map_err(|_| fmt_err!(Layout, "delta binary packed miniblock count overflow"))?;
        let page_data = &page_data[offset..];
        let (value_count, offset) = uleb128::decode(page_data)
            .map_err(|_| fmt_err!(Layout, "failed to decode value count"))?;
        let page_data = &page_data[offset..];
        let (first_value, offset) = zigzag_leb128::decode(page_data)
            .map_err(|_| fmt_err!(Layout, "failed to decode first value"))?;
        let page_data = &page_data[offset..];

        let blocks_remaining = if value_count > 1 {
            let blocks_values = value_count - 1; // the first value is not included in the blocks
            let blocks_values = blocks_values
                .checked_add(block_size - 1)
                .ok_or_else(|| fmt_err!(Layout, "delta binary packed block count overflow"))?; // add padding values to be able to decode the last block
            blocks_values / block_size
        } else {
            0
        };

        let mut s = Self {
            page_data,
            miniblocks_per_block,
            blocks_remaining,
            value_count,
            block_bitwidths_offset: 0,
            miniblock_size,
            miniblock_offset: 0,
            miniblock_index: if blocks_remaining > 0 {
                0
            } else {
                miniblocks_per_block
            }, // if there are no blocks, we want next_miniblock to return None, so we set the index to the end.
            min_delta: U::default(),
            // A value_count=0 header still parses, but its first_value is a
            // phantom default rather than a real value.
            has_values: value_count > 0,
        };
        s.advance_block(0)?;
        Ok((s, first_value.as_()))
    }

    pub(crate) fn get_end_pointer(&self) -> ParquetResult<*const u8> {
        // No block was initialized (e.g. value_count <= 1 in DELTA_BINARY_PACKED header).
        if self.blocks_remaining == 0 && self.miniblock_index == 0 && self.miniblock_offset == 0 {
            return Ok(self.page_data.as_ptr());
        }

        let mut blocks_remaining = self.blocks_remaining;
        let mut page_data = self.page_data;
        let miniblocks_per_block = self.miniblocks_per_block;
        let mut miniblock_index = self.miniblock_index;
        let block_bitwidths_offset = self.block_bitwidths_offset;
        let mut miniblock_offset = self.miniblock_offset;
        let miniblock_size = self.miniblock_size;

        // The values are laid out [first_value][block 0][block 1]... The first
        // value lives in the header, so the blocks hold value_count - 1 values.
        // Every block writes a bit-width byte for each of its miniblocks_per_block
        // miniblocks (already covered by miniblock_offset, which starts past the
        // whole bit-width array), but it writes a body only for the miniblocks that
        // actually hold values. Only the LAST block can be partial; every earlier
        // block fills all miniblocks_per_block. The trailing miniblocks of the last
        // block hold no values and carry no body bytes -- the spec leaves their
        // bit-width bytes arbitrary -- so bounding the last block's walk to
        // ceil(values_in_last_block / miniblock_size) miniblocks is what keeps this
        // from overshooting the length stream on a foreign page with more than one
        // miniblock per block (QuestDB's own writer always emits one).
        let block_size_values = miniblocks_per_block
            .checked_mul(miniblock_size)
            .ok_or_else(|| fmt_err!(Layout, "delta binary packed block size overflow"))?;
        // Guard the divisors below: miniblock_size (hence block_size_values) is a
        // validated non-zero multiple of 32 for any iterator that reaches here (the
        // empty-buffer iterator short-circuits above), but a div-by-zero would
        // panic and abort the JVM.
        if block_size_values == 0 {
            return Ok(self.page_data.as_ptr());
        }
        // Locate the last block and its populated-miniblock count from the header,
        // independent of how far the iterator has been advanced. blocks_remaining
        // identifies the last block correctly whether get_end_pointer is called on
        // a fresh iterator or mid-decode.
        let total_block_values = (self.value_count as usize).saturating_sub(1);
        let total_blocks = total_block_values.div_ceil(block_size_values);
        let last_block_values =
            total_block_values.saturating_sub(total_blocks.saturating_sub(1) * block_size_values);
        let last_block_miniblocks = last_block_values.div_ceil(miniblock_size);

        // Skip the populated miniblocks of the current block. It is the last block
        // when no full blocks follow it (blocks_remaining == 0).
        let current_block_miniblocks = if blocks_remaining == 0 {
            last_block_miniblocks
        } else {
            miniblocks_per_block
        };
        while miniblock_index < current_block_miniblocks {
            let bitwidth_idx = block_bitwidths_offset
                .checked_add(miniblock_index)
                .ok_or_else(|| fmt_err!(Layout, "delta binary packed bit width index overflow"))?;
            let num_bits = *page_data
                .get(bitwidth_idx)
                .ok_or_else(|| fmt_err!(Layout, "delta binary packed bit width out of bounds"))?;
            let miniblock_bytes =
                miniblock_size
                    .checked_mul(num_bits as usize)
                    .ok_or_else(|| {
                        fmt_err!(Layout, "delta binary packed miniblock byte size overflow")
                    })?
                    / 8;
            miniblock_offset = miniblock_offset
                .checked_add(miniblock_bytes)
                .ok_or_else(|| fmt_err!(Layout, "delta binary packed miniblock offset overflow"))?;
            miniblock_index += 1;
        }

        // Skip the remaining blocks
        while blocks_remaining > 0 {
            page_data = page_data.get(miniblock_offset..).ok_or_else(|| {
                fmt_err!(Layout, "delta binary packed block offset out of bounds")
            })?;
            let (_min_delta, offset) = zigzag_leb128::decode(page_data)
                .map_err(|_| fmt_err!(Layout, "failed to decode min delta"))?;
            page_data = &page_data[offset..];
            miniblock_offset = miniblocks_per_block;
            blocks_remaining -= 1;

            // This block is the last one when none remain after it.
            let block_miniblocks = if blocks_remaining == 0 {
                last_block_miniblocks
            } else {
                miniblocks_per_block
            };
            for i in 0..block_miniblocks {
                let num_bits = *page_data.get(i).ok_or_else(|| {
                    fmt_err!(Layout, "delta binary packed bit width out of bounds")
                })?;
                let miniblock_bytes =
                    miniblock_size
                        .checked_mul(num_bits as usize)
                        .ok_or_else(|| {
                            fmt_err!(Layout, "delta binary packed miniblock byte size overflow")
                        })?
                        / 8;
                miniblock_offset =
                    miniblock_offset
                        .checked_add(miniblock_bytes)
                        .ok_or_else(|| {
                            fmt_err!(Layout, "delta binary packed miniblock offset overflow")
                        })?;
            }
        }

        let end = page_data
            .get(miniblock_offset..)
            .ok_or_else(|| fmt_err!(Layout, "delta binary packed block exceeds page size"))?;
        Ok(end.as_ptr())
    }

    #[inline]
    pub fn advance_block(&mut self, offset: usize) -> ParquetResult<()> {
        if self.blocks_remaining == 0 {
            return Ok(());
        }

        self.page_data = self
            .page_data
            .get(offset..)
            .ok_or_else(|| fmt_err!(Layout, "delta binary packed block offset out of bounds"))?;
        let (min_delta, offset) = zigzag_leb128::decode(self.page_data)
            .map_err(|_| fmt_err!(Layout, "failed to decode min delta"))?;
        self.min_delta = min_delta.as_();
        self.block_bitwidths_offset = offset;
        self.miniblock_offset = offset
            .checked_add(self.miniblocks_per_block)
            .ok_or_else(|| fmt_err!(Layout, "delta binary packed miniblock offset overflow"))?;
        if self.miniblock_offset > self.page_data.len() {
            return Err(fmt_err!(
                Layout,
                "delta binary packed block header exceeds page size"
            ));
        }
        self.miniblock_index = 0;
        self.blocks_remaining -= 1;
        Ok(())
    }

    #[inline]
    pub(crate) fn next_miniblock(&mut self) -> ParquetResult<Option<Miniblock<'a>>> {
        if self.miniblock_index == self.miniblocks_per_block {
            // The current block is exhausted. If there are no more blocks, we're done.
            if self.blocks_remaining == 0 {
                return Ok(None);
            }
            // Advance lazily so the last miniblock in the previous block is decoded
            // with its own min_delta.
            self.advance_block(self.miniblock_offset)?;
        }

        let bitwidth_idx = self
            .block_bitwidths_offset
            .checked_add(self.miniblock_index)
            .ok_or_else(|| fmt_err!(Layout, "delta binary packed bit width index overflow"))?;
        let num_bits = *self
            .page_data
            .get(bitwidth_idx)
            .ok_or_else(|| fmt_err!(Layout, "delta binary packed bit width out of bounds"))?;
        let miniblock_bytes = self
            .miniblock_size
            .checked_mul(num_bits as usize)
            .ok_or_else(|| fmt_err!(Layout, "delta binary packed miniblock byte size overflow"))?
            / 8;
        let next_miniblock_offset = self
            .miniblock_offset
            .checked_add(miniblock_bytes)
            .ok_or_else(|| fmt_err!(Layout, "delta binary packed miniblock offset overflow"))?;
        let current = Miniblock {
            data: self
                .page_data
                .get(self.miniblock_offset..next_miniblock_offset)
                .ok_or_else(|| {
                    fmt_err!(Layout, "delta binary packed miniblock exceeds page size")
                })?,
            num_bits,
        };

        self.miniblock_index += 1;
        self.miniblock_offset = next_miniblock_offset;

        Ok(Some(current))
    }
}

/// A decoder for primitive types with delta-binary packed encoding that decodes
/// values in blocks.
/// T is the destination type.
/// We split each miniblock into packs of 32 values, and decode one pack at a time.
/// This allows to amortize the cost of decoding the bitwidths over multiple values, while
/// keeping the memory usage of the decoder low. Also, parquet spec requires that the miniblock size is a multiple of 32,
/// so we can be sure that each miniblock contains an integer number of packs.
pub struct DeltaBinaryPackedDecoder<'a, T, U>
where
    T: Copy + Debug + 'static,
    U: AsPrimitive<T>,
{
    buffers: &'a mut ColumnChunkBuffers,
    buffers_ptr: *mut T,
    buffers_offset: usize,
    iterator: MiniblockIterator<'a, U>,
    null_value: T,
    current_value: U,
    consumed_initial_value: bool,
    values: [U; 32],
    value_index: usize,
    miniblock: Miniblock<'a>,
    miniblock_pack_index: usize,
    packs_per_miniblock: usize,
}

impl<'a, T, U> DeltaBinaryPackedDecoder<'a, T, U>
where
    T: Copy + Debug + 'static,
    U: AsPrimitive<T> + Default + WrappingAdd + Copy + 'static,
    i64: AsPrimitive<U>,
{
    pub fn try_new(
        data: &'a [u8],
        buffers: &'a mut ColumnChunkBuffers,
        null_value: T,
    ) -> ParquetResult<Self> {
        let (mut miniblock_iterator, first_value) = MiniblockIterator::try_new(data)?;

        let packs_per_miniblock = miniblock_iterator.miniblock_size / 32;
        let (miniblock, miniblock_pack_index) = match miniblock_iterator.next_miniblock()? {
            Some(miniblock) => (miniblock, 0),
            None => {
                // No miniblocks to decode, use a default miniblock that requires to be skipped.
                (Miniblock::default(), packs_per_miniblock)
            }
        };

        // A zero-value page (empty values buffer or a value_count=0 header) has
        // no real first value -- `current_value` is a phantom default. Mark it
        // already consumed so the first value request goes straight to the empty
        // miniblock iterator and fails with "not enough values to iterate"
        // instead of silently decoding 0. All-null pages are unaffected: their
        // definition levels drive only push_nulls.
        let consumed_initial_value = !miniblock_iterator.has_values;
        Ok(Self {
            buffers_ptr: buffers.data_vec.as_mut_ptr().cast(),
            buffers_offset: buffers.data_vec.len() / std::mem::size_of::<T>(),
            buffers,
            null_value,
            current_value: first_value,
            values: [U::default(); 32],
            iterator: miniblock_iterator,
            consumed_initial_value,
            value_index: 32,
            miniblock,
            // Index of the current pack in the miniblock
            miniblock_pack_index,
            packs_per_miniblock,
        })
    }

    #[inline(always)]
    fn unpack_next(&mut self) -> ParquetResult<()> {
        // We need to decode the next pack of values in the miniblock
        if self.miniblock_pack_index == self.packs_per_miniblock {
            // We need to advance to the next miniblock
            match self.iterator.next_miniblock()? {
                Some(miniblock) => {
                    self.miniblock = miniblock;
                    self.miniblock_pack_index = 0;
                }
                None => {
                    // No more miniblocks to decode, this means that we have consumed all values in the page.
                    return Err(fmt_err!(Layout, "not enough values to iterate"));
                }
            }
        }

        let num_bits = self.miniblock.num_bits as usize;
        let max_num_bits = std::mem::size_of::<U>() * 8;
        if num_bits > max_num_bits {
            return Err(fmt_err!(
                Layout,
                "delta binary packed bit width {} exceeds {}-bit target width",
                num_bits,
                max_num_bits
            ));
        }
        let pack_size = 32 * num_bits / 8; // division will be optimized by compiler since we're multiplying by 32.
        let offset = self
            .miniblock_pack_index
            .checked_mul(pack_size)
            .ok_or_else(|| fmt_err!(Layout, "delta binary packed pack offset overflow"))?;
        let end = offset
            .checked_add(pack_size)
            .ok_or_else(|| fmt_err!(Layout, "delta binary packed pack end overflow"))?;
        let data =
            self.miniblock.data.get(offset..end).ok_or_else(|| {
                fmt_err!(Layout, "delta binary packed pack exceeds miniblock size")
            })?;
        self.miniblock_pack_index += 1;

        let values = self.values.as_mut_ptr();
        match std::mem::size_of::<U>() {
            4 => unpack32(data, values.cast(), num_bits),
            8 => unpack64(data, values.cast(), num_bits),
            _ => unreachable!("unsupported size"),
        }

        Ok(())
    }
}

impl<'a, T, U> Pushable for DeltaBinaryPackedDecoder<'a, T, U>
where
    T: Copy + Debug + WrappingAdd<Output = T> + 'static,
    U: AsPrimitive<T> + Default + WrappingAdd + Copy + 'static,
    i64: AsPrimitive<U>,
{
    #[inline]
    fn push(&mut self) -> ParquetResult<()> {
        if !self.consumed_initial_value {
            // SAFETY: destination pointer stays in-bounds because decode paths reserve output upfront.
            unsafe {
                let out = self.buffers_ptr.add(self.buffers_offset);
                *out = self.current_value.as_();
            }
            self.buffers_offset += 1;
            self.consumed_initial_value = true;
            return Ok(());
        }

        if self.value_index == 32 {
            self.unpack_next()?;
            self.value_index = 0;
        }

        self.current_value = self.current_value.wrapping_add(
            &self
                .iterator
                .min_delta
                .wrapping_add(&self.values[self.value_index]),
        );
        // SAFETY: destination pointer stays in-bounds because decode paths reserve output upfront.
        unsafe {
            self.buffers_ptr
                .add(self.buffers_offset)
                .write(self.current_value.as_());
        }
        self.buffers_offset += 1;
        self.value_index += 1;

        Ok(())
    }

    #[inline]
    fn push_null(&mut self) -> ParquetResult<()> {
        // SAFETY: destination pointer stays in-bounds because decode paths reserve output upfront.
        unsafe {
            *self.buffers_ptr.add(self.buffers_offset) = self.null_value;
        }
        self.buffers_offset += 1;
        Ok(())
    }

    #[inline(always)]
    fn push_nulls(&mut self, count: usize) -> ParquetResult<()> {
        // SAFETY: destination pointer stays in-bounds because decode paths reserve output upfront.
        let out = unsafe { self.buffers_ptr.add(self.buffers_offset) };
        for i in 0..count {
            // SAFETY: `out` points to reserved output space and `i < count`.
            unsafe {
                *out.add(i) = self.null_value;
            }
        }
        self.buffers_offset += count;
        Ok(())
    }

    #[inline(always)]
    fn push_slice(&mut self, mut count: usize) -> ParquetResult<()> {
        if count == 0 {
            return Ok(());
        }

        let mut current_value = self.current_value;
        let mut value_index = self.value_index;
        let mut buffers_offset = self.buffers_offset;
        let buffers_ptr = self.buffers_ptr;

        if !self.consumed_initial_value {
            // SAFETY: destination pointer stays in-bounds because decode paths reserve output upfront.
            unsafe {
                *buffers_ptr.add(buffers_offset) = current_value.as_();
            }
            buffers_offset += 1;
            self.consumed_initial_value = true;
            count -= 1;
        }

        while count > 0 && value_index != 32 {
            current_value = current_value.wrapping_add(
                &self
                    .iterator
                    .min_delta
                    .wrapping_add(&self.values[value_index]),
            );
            // SAFETY: destination pointer stays in-bounds because decode paths reserve output upfront.
            unsafe {
                *buffers_ptr.add(buffers_offset) = current_value.as_();
            }
            buffers_offset += 1;
            value_index += 1;
            count -= 1;
        }

        while count >= 32 {
            self.unpack_next()?;
            let min_delta = self.iterator.min_delta;
            for i in 0..32 {
                current_value =
                    current_value.wrapping_add(&min_delta.wrapping_add(&self.values[i]));
                // SAFETY: destination pointer stays in-bounds because decode paths reserve output upfront.
                unsafe {
                    *buffers_ptr.add(buffers_offset + i) = current_value.as_();
                }
            }
            buffers_offset += 32;
            count -= 32;
        }

        while count > 0 {
            if value_index == 32 {
                self.unpack_next()?;
                value_index = 0;
            }

            let to_write = count.min(32 - value_index);
            // SAFETY: destination pointer stays in-bounds because decode paths reserve output upfront.
            unsafe {
                for i in 0..to_write {
                    current_value = current_value.wrapping_add(
                        &self
                            .iterator
                            .min_delta
                            .wrapping_add(&self.values[value_index + i]),
                    );
                    *buffers_ptr.add(buffers_offset + i) = current_value.as_();
                }
            }
            buffers_offset += to_write;
            value_index += to_write;
            count -= to_write;
        }

        self.current_value = current_value;
        self.value_index = value_index;
        self.buffers_offset = buffers_offset;

        Ok(())
    }

    #[inline]
    fn reserve(&mut self, count: usize) -> ParquetResult<()> {
        let needed = (self.buffers_offset + count) * std::mem::size_of::<T>();
        if self.buffers.data_vec.len() < needed {
            let additional = needed - self.buffers.data_vec.len();
            self.buffers.data_vec.reserve(additional)?;
            // SAFETY: `needed <= capacity` after reserve; values are initialized before read.
            unsafe {
                self.buffers.data_vec.set_len(needed);
            }
        }
        self.buffers_ptr = self.buffers.data_vec.as_mut_ptr().cast();
        Ok(())
    }

    #[inline]
    fn skip(&mut self, mut count: usize) -> ParquetResult<()> {
        if count == 0 {
            return Ok(());
        }

        if !self.consumed_initial_value {
            self.consumed_initial_value = true;
            count -= 1;
        }

        while count > 0 {
            if self.value_index == 32 {
                self.unpack_next()?;
                self.value_index = 0;
            }

            let to_write = count.min(32 - self.value_index);
            for i in 0..to_write {
                self.current_value = self.current_value.wrapping_add(
                    &self
                        .iterator
                        .min_delta
                        .wrapping_add(&self.values[self.value_index + i]),
                );
            }
            self.value_index += to_write;
            count -= to_write;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use parquet2::encoding::{uleb128, zigzag_leb128};

    use crate::allocator::{AcVec, TestAllocatorState};
    use crate::parquet_read::column_sink::Pushable;
    use crate::parquet_read::decoders::delta_binary_packed::{
        DeltaBinaryPackedDecoder, MiniblockIterator,
    };
    use crate::parquet_read::ColumnChunkBuffers;

    fn create_buffers(allocator: &crate::allocator::QdbAllocator) -> ColumnChunkBuffers {
        ColumnChunkBuffers {
            data_size: 0,
            data_ptr: ptr::null_mut(),
            data_vec: AcVec::new_in(allocator.clone()),
            aux_size: 0,
            aux_ptr: ptr::null_mut(),
            aux_vec: AcVec::new_in(allocator.clone()),
            page_buffers_size: 0,
            page_buffers: Vec::new(),
            column_top: 0,
            page_buffers_charged: 0,
            page_buffers_counted: 0,
        }
    }

    fn write_uleb128(buf: &mut Vec<u8>, value: u64) {
        let mut tmp = [0u8; 10];
        let len = uleb128::encode(value, &mut tmp);
        buf.extend_from_slice(&tmp[..len]);
    }

    fn write_zigzag(buf: &mut Vec<u8>, value: i64) {
        let (tmp, len) = zigzag_leb128::encode(value);
        buf.extend_from_slice(&tmp[..len]);
    }

    /// Bit-pack 32 values at the given bit width (little-endian, LSB first).
    fn bitpack32(values: &[u64], num_bits: u8) -> Vec<u8> {
        assert_eq!(values.len(), 32);
        if num_bits == 0 {
            return vec![];
        }
        let total_bits = 32 * num_bits as usize;
        let mut bytes = vec![0u8; total_bits / 8];
        for (i, &v) in values.iter().enumerate() {
            let bit_offset = i * num_bits as usize;
            for b in 0..num_bits as usize {
                if v & (1 << b) != 0 {
                    let pos = bit_offset + b;
                    bytes[pos / 8] |= 1 << (pos % 8);
                }
            }
        }
        bytes
    }

    /// Encode a slice of i64 values into DELTA_BINARY_PACKED format.
    /// Uses block_size=128, miniblocks_per_block=4 (miniblock_size=32).
    fn encode_delta_binary_packed(values: &[i64]) -> Vec<u8> {
        let block_size: u64 = 128;
        let miniblocks_per_block: u64 = 4;
        let miniblock_size = 32usize;
        let value_count = values.len() as u64;

        let mut buf = Vec::new();
        write_uleb128(&mut buf, block_size);
        write_uleb128(&mut buf, miniblocks_per_block);
        write_uleb128(&mut buf, value_count);

        if values.is_empty() {
            write_zigzag(&mut buf, 0); // first_value placeholder
            return buf;
        }

        write_zigzag(&mut buf, values[0]);

        if values.len() <= 1 {
            return buf;
        }

        // Compute deltas (wrapping to handle extreme values)
        let deltas: Vec<i64> = values.windows(2).map(|w| w[1].wrapping_sub(w[0])).collect();

        // Process in blocks of block_size values (128 deltas)
        for block_start in (0..deltas.len()).step_by(block_size as usize) {
            let block_end = (block_start + block_size as usize).min(deltas.len());
            let block_deltas = &deltas[block_start..block_end];
            let min_delta = *block_deltas.iter().min().unwrap();

            write_zigzag(&mut buf, min_delta);

            // Compute relative deltas (subtract min_delta)
            let mut relative: Vec<u64> = block_deltas
                .iter()
                .map(|&d| d.wrapping_sub(min_delta) as u64)
                .collect();
            // Pad to full block size with zeros
            relative.resize(block_size as usize, 0);

            // Compute bitwidths per miniblock
            let mut bitwidths = Vec::new();
            for mb in 0..miniblocks_per_block as usize {
                let start = mb * miniblock_size;
                let end = start + miniblock_size;
                let max_val = *relative[start..end].iter().max().unwrap();
                let bw = if max_val == 0 {
                    0u8
                } else {
                    (64 - max_val.leading_zeros()) as u8
                };
                bitwidths.push(bw);
            }
            buf.extend_from_slice(&bitwidths);

            // Bit-pack each miniblock
            for (mb, bitwidth) in bitwidths
                .iter()
                .enumerate()
                .take(miniblocks_per_block as usize)
            {
                let start = mb * miniblock_size;
                let end = start + miniblock_size;
                let packed = bitpack32(&relative[start..end], *bitwidth);
                buf.extend_from_slice(&packed);
            }
        }

        buf
    }

    /// Helper to decode i32 values using the decoder.
    fn decode_i32_push_slice(data: &[u8], count: usize) -> Vec<i32> {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_buffers(&allocator);
        {
            let mut decoder =
                DeltaBinaryPackedDecoder::<i32, i32>::try_new(data, &mut buffers, i32::MIN)
                    .unwrap();
            decoder.reserve(count).unwrap();
            decoder.push_slice(count).unwrap();
        }
        let out: &[i32] =
            unsafe { std::slice::from_raw_parts(buffers.data_vec.as_ptr().cast(), count) };
        out.to_vec()
    }

    /// Helper to decode i64 values using the decoder.
    fn decode_i64_push_slice(data: &[u8], count: usize) -> Vec<i64> {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_buffers(&allocator);
        {
            let mut decoder =
                DeltaBinaryPackedDecoder::<i64, i64>::try_new(data, &mut buffers, i64::MIN)
                    .unwrap();
            decoder.reserve(count).unwrap();
            decoder.push_slice(count).unwrap();
        }
        let out: &[i64] =
            unsafe { std::slice::from_raw_parts(buffers.data_vec.as_ptr().cast(), count) };
        out.to_vec()
    }

    /// Helper to decode i32 values one-by-one using push().
    fn decode_i32_push_one_by_one(data: &[u8], count: usize) -> Vec<i32> {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_buffers(&allocator);
        {
            let mut decoder =
                DeltaBinaryPackedDecoder::<i32, i32>::try_new(data, &mut buffers, i32::MIN)
                    .unwrap();
            decoder.reserve(count).unwrap();
            for _ in 0..count {
                decoder.push().unwrap();
            }
        }
        let out: &[i32] =
            unsafe { std::slice::from_raw_parts(buffers.data_vec.as_ptr().cast(), count) };
        out.to_vec()
    }

    // ─── Happy path tests ───

    #[test]
    fn single_value_i32() {
        let data = encode_delta_binary_packed(&[42]);
        let result = decode_i32_push_slice(&data, 1);
        assert_eq!(result, vec![42]);
    }

    #[test]
    fn single_value_i64() {
        let data = encode_delta_binary_packed(&[42]);
        let result = decode_i64_push_slice(&data, 1);
        assert_eq!(result, vec![42i64]);
    }

    #[test]
    fn single_value_negative() {
        let data = encode_delta_binary_packed(&[-100]);
        let result = decode_i64_push_slice(&data, 1);
        assert_eq!(result, vec![-100i64]);
    }

    #[test]
    fn ascending_sequence_push() {
        let values: Vec<i64> = (100..133).collect(); // 33 values
        let data = encode_delta_binary_packed(&values);
        let result = decode_i32_push_one_by_one(&data, values.len());
        let expected: Vec<i32> = values.iter().map(|&v| v as i32).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn ascending_sequence_push_slice() {
        let values: Vec<i64> = (100..133).collect();
        let data = encode_delta_binary_packed(&values);
        let result = decode_i32_push_slice(&data, values.len());
        let expected: Vec<i32> = values.iter().map(|&v| v as i32).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn descending_sequence() {
        let values: Vec<i64> = (0..33).rev().collect(); // 32, 31, ..., 0
        let data = encode_delta_binary_packed(&values);
        let result = decode_i64_push_slice(&data, values.len());
        assert_eq!(result, values);
    }

    #[test]
    fn constant_values() {
        let values = vec![7i64; 33];
        let data = encode_delta_binary_packed(&values);
        let result = decode_i64_push_slice(&data, values.len());
        assert_eq!(result, values);
    }

    #[test]
    fn variable_deltas() {
        // Deltas: 1, 3, 2, 5, 1, 4, ...
        let values: Vec<i64> = vec![
            10, 11, 14, 16, 21, 22, 26, 30, 31, 35, 40, 41, 45, 50, 51, 55, 60, 61, 65, 70, 71, 75,
            80, 81, 85, 90, 91, 95, 100, 101, 105, 110, 111,
        ];
        let data = encode_delta_binary_packed(&values);
        let result = decode_i64_push_slice(&data, values.len());
        assert_eq!(result, values);
    }

    #[test]
    fn multiple_miniblocks() {
        // 97 values = 96 deltas = 3 miniblocks of 32
        let values: Vec<i64> = (0..97).map(|i| i * 2).collect();
        let data = encode_delta_binary_packed(&values);
        let result = decode_i64_push_slice(&data, values.len());
        assert_eq!(result, values);
    }

    #[test]
    fn multiple_blocks() {
        // 130 values = 129 deltas, which exceeds block_size=128
        let values: Vec<i64> = (0..130).map(|i| i * 3 + 1).collect();
        let data = encode_delta_binary_packed(&values);
        let result = decode_i64_push_slice(&data, values.len());
        assert_eq!(result, values);
    }

    #[test]
    fn push_null_and_push_nulls() {
        let values: Vec<i64> = (0..5).collect();
        let data = encode_delta_binary_packed(&values);
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_buffers(&allocator);
        let null_val: i64 = i64::MIN;
        let total = 8; // 5 real + 3 nulls
        {
            let mut decoder =
                DeltaBinaryPackedDecoder::<i64, i64>::try_new(&data, &mut buffers, null_val)
                    .unwrap();
            decoder.reserve(total).unwrap();
            // push 2 values, 1 null, 3 values, 2 nulls
            decoder.push_slice(2).unwrap();
            decoder.push_null().unwrap();
            decoder.push_slice(3).unwrap();
            decoder.push_nulls(2).unwrap();
        }
        let out: &[i64] =
            unsafe { std::slice::from_raw_parts(buffers.data_vec.as_ptr().cast(), total) };
        assert_eq!(out, &[0, 1, null_val, 2, 3, 4, null_val, null_val]);
    }

    #[test]
    fn skip_values() {
        // [0, 1, 2, 3, 4, 5, 6, 7, ...] — skip first 3, then read next 5
        let values: Vec<i64> = (0..33).collect();
        let data = encode_delta_binary_packed(&values);
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_buffers(&allocator);
        let count = 5;
        {
            let mut decoder =
                DeltaBinaryPackedDecoder::<i64, i64>::try_new(&data, &mut buffers, i64::MIN)
                    .unwrap();
            decoder.reserve(count).unwrap();
            decoder.skip(3).unwrap();
            decoder.push_slice(count).unwrap();
        }
        let out: &[i64] =
            unsafe { std::slice::from_raw_parts(buffers.data_vec.as_ptr().cast(), count) };
        assert_eq!(out, &[3, 4, 5, 6, 7]);
    }

    #[test]
    fn mixed_push_skip_null() {
        let values: Vec<i64> = (10..43).collect(); // 33 values: 10..42
        let data = encode_delta_binary_packed(&values);
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_buffers(&allocator);
        let null_val: i64 = -1;
        // push 1, skip 2, null 1, push 3
        let total = 5;
        {
            let mut decoder =
                DeltaBinaryPackedDecoder::<i64, i64>::try_new(&data, &mut buffers, null_val)
                    .unwrap();
            decoder.reserve(total).unwrap();
            decoder.push().unwrap(); // 10
            decoder.skip(2).unwrap(); // skip 11, 12
            decoder.push_null().unwrap(); // null
            decoder.push_slice(3).unwrap(); // 13, 14, 15
        }
        let out: &[i64] =
            unsafe { std::slice::from_raw_parts(buffers.data_vec.as_ptr().cast(), total) };
        assert_eq!(out, &[10, null_val, 13, 14, 15]);
    }

    #[test]
    fn push_slice_zero_is_noop() {
        let data = encode_delta_binary_packed(&[42]);
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_buffers(&allocator);
        {
            let mut decoder =
                DeltaBinaryPackedDecoder::<i64, i64>::try_new(&data, &mut buffers, i64::MIN)
                    .unwrap();
            decoder.reserve(1).unwrap();
            decoder.push_slice(0).unwrap();
            decoder.push_slice(1).unwrap();
        }
        let out: &[i64] =
            unsafe { std::slice::from_raw_parts(buffers.data_vec.as_ptr().cast(), 1) };
        assert_eq!(out, &[42]);
    }

    #[test]
    fn skip_zero_is_noop() {
        let data = encode_delta_binary_packed(&[42]);
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_buffers(&allocator);
        {
            let mut decoder =
                DeltaBinaryPackedDecoder::<i64, i64>::try_new(&data, &mut buffers, i64::MIN)
                    .unwrap();
            decoder.reserve(1).unwrap();
            decoder.skip(0).unwrap();
            decoder.push_slice(1).unwrap();
        }
        let out: &[i64] =
            unsafe { std::slice::from_raw_parts(buffers.data_vec.as_ptr().cast(), 1) };
        assert_eq!(out, &[42]);
    }

    #[test]
    fn large_values_i32() {
        // Values near i32 boundaries with small deltas (stay within 32-bit delta range).
        let values: Vec<i64> = vec![
            i32::MAX as i64 - 2,
            i32::MAX as i64 - 1,
            i32::MAX as i64,
            i32::MAX as i64 - 5,
            i32::MAX as i64 - 10,
        ];
        let data = encode_delta_binary_packed(&values);
        let result = decode_i32_push_slice(&data, values.len());
        let expected: Vec<i32> = values.iter().map(|&v| v as i32).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn large_values_i64() {
        // Values near i64 boundaries with small deltas.
        let values = vec![
            i64::MAX - 10,
            i64::MAX - 5,
            i64::MAX - 2,
            i64::MAX - 1,
            i64::MAX,
        ];
        let data = encode_delta_binary_packed(&values);
        let result = decode_i64_push_slice(&data, values.len());
        assert_eq!(result, values);
    }

    #[test]
    fn get_end_pointer_single_value() {
        let data = encode_delta_binary_packed(&[42]);
        let (iter, _first) = MiniblockIterator::<i64>::try_new(&data).unwrap();
        let end = iter.get_end_pointer().unwrap();
        // Single value: no blocks, end pointer should equal page_data start
        let page_data_ptr = iter.page_data.as_ptr();
        assert_eq!(end, page_data_ptr);
    }

    #[test]
    fn get_end_pointer_with_blocks() {
        let values: Vec<i64> = (0..33).collect();
        let data = encode_delta_binary_packed(&values);
        let (iter, _first) = MiniblockIterator::<i64>::try_new(&data).unwrap();
        let end = iter.get_end_pointer().unwrap();
        let walk = end as usize - data.as_ptr() as usize;

        // Exact offset: get_end_pointer must equal the trusted parquet2 decoder's
        // byte count after a full decode -- the true end of the length stream --
        // not merely fall somewhere inside the buffer. encode_delta_binary_packed
        // emits only the delta stream, so that end is the whole buffer.
        let mut dec = parquet2::encoding::delta_bitpacked::Decoder::try_new(&data).unwrap();
        let decoded = dec.by_ref().count();
        assert_eq!(decoded, values.len());
        assert_eq!(walk, dec.consumed_bytes());
        assert_eq!(walk, data.len());
    }

    // Builds a single-block DELTA_BINARY_PACKED stream with miniblocks_per_block=4
    // -- a foreign shape, since QuestDB's writer always emits 1. The block holds
    // value_count-1 deltas, populating ceil((value_count-1)/32) of its 4
    // miniblocks. The UNUSED trailing miniblocks get a non-zero garbage bit-width
    // byte (the spec permits any value there) and NO body bytes. Requires
    // value_count-1 <= 128 (a single block).
    fn build_multi_miniblock_stream(value_count: u64) -> Vec<u8> {
        const MINIBLOCKS: usize = 4;
        const MINIBLOCK_SIZE: usize = 32;
        let deltas = (value_count as usize).saturating_sub(1);
        assert!(
            deltas <= MINIBLOCKS * MINIBLOCK_SIZE,
            "test builds one block"
        );
        let populated = deltas.div_ceil(MINIBLOCK_SIZE);

        let mut data = Vec::new();
        write_uleb128(&mut data, (MINIBLOCKS * MINIBLOCK_SIZE) as u64); // block_size = 128
        write_uleb128(&mut data, MINIBLOCKS as u64);
        write_uleb128(&mut data, value_count);
        write_zigzag(&mut data, 0); // first_value
        write_zigzag(&mut data, 0); // block 0 min_delta
        for i in 0..MINIBLOCKS {
            // bit width 1 for a populated miniblock (each gets a 4-byte body), a
            // non-zero garbage value for the unused trailing ones (no body follows).
            data.push(if i < populated { 1 } else { 7 });
        }
        for _ in 0..populated {
            // 1-bit body: MINIBLOCK_SIZE bits = 4 bytes, all ones (delta == 1).
            data.extend_from_slice(&[0xFF; MINIBLOCK_SIZE / 8]);
        }
        data
    }

    #[test]
    fn get_end_pointer_ignores_unused_trailing_miniblocks() {
        // Regression: get_end_pointer used to add a body for EVERY miniblock of a
        // block, including the unused trailing ones of the last block, whose
        // bit-width bytes are arbitrary per spec. On a foreign page with
        // miniblocks_per_block > 1 that overshot the length stream, shifting every
        // value on a partial range read (silent corruption) or tripping a spurious
        // "exceeds page size" error. The trusted parquet2 decoder's consumed_bytes()
        // after a full decode is the true stream end; get_end_pointer must equal it.
        // Cases: 1 populated miniblock + 3 unused; 2 populated (last one partial) +
        // 2 unused; all 4 populated (no unused -- a sanity check).
        for value_count in [33u64, 40, 129] {
            let data = build_multi_miniblock_stream(value_count);

            let mut dec = parquet2::encoding::delta_bitpacked::Decoder::try_new(&data).unwrap();
            let decoded = dec.by_ref().count();
            assert_eq!(decoded, value_count as usize, "value_count={value_count}");
            let true_end = dec.consumed_bytes();

            let (iter, _): (MiniblockIterator<i32>, _) = MiniblockIterator::try_new(&data).unwrap();
            let end = iter.get_end_pointer().unwrap();
            let walk = end as usize - data.as_ptr() as usize;
            assert_eq!(
                walk, true_end,
                "get_end_pointer must equal parquet2 consumed_bytes (value_count={value_count})"
            );
        }
    }

    // Builds a MULTI-block DELTA_BINARY_PACKED stream with miniblocks_per_block=4 --
    // a foreign shape, since QuestDB's writer always emits 1. Every block but the
    // last is fully populated (all 4 miniblocks carry a body); the last block is
    // partial and its UNUSED trailing miniblocks carry a non-zero garbage bit-width
    // byte (the spec permits any value there) and NO body bytes. Unlike
    // build_multi_miniblock_stream this spans more than one block. Requires
    // value_count-1 > 128.
    fn build_multi_block_multi_miniblock_stream(value_count: u64) -> Vec<u8> {
        const MINIBLOCKS: usize = 4;
        const MINIBLOCK_SIZE: usize = 32;
        const BLOCK_SIZE: usize = MINIBLOCKS * MINIBLOCK_SIZE; // 128
        let total_deltas = (value_count as usize).saturating_sub(1);
        let total_blocks = total_deltas.div_ceil(BLOCK_SIZE);
        assert!(total_blocks >= 2, "test builds more than one block");

        let mut data = Vec::new();
        write_uleb128(&mut data, BLOCK_SIZE as u64); // block_size = 128
        write_uleb128(&mut data, MINIBLOCKS as u64);
        write_uleb128(&mut data, value_count);
        write_zigzag(&mut data, 0); // first_value

        let mut remaining = total_deltas;
        for _ in 0..total_blocks {
            // Only the last block can be partial; every earlier block fills all
            // MINIBLOCKS miniblocks.
            let block_deltas = remaining.min(BLOCK_SIZE);
            let populated = block_deltas.div_ceil(MINIBLOCK_SIZE);
            write_zigzag(&mut data, 0); // min_delta
            for i in 0..MINIBLOCKS {
                // bit width 1 for a populated miniblock (a 4-byte body follows), a
                // non-zero garbage value for the unused trailing ones (no body).
                data.push(if i < populated { 1 } else { 7 });
            }
            for _ in 0..populated {
                // 1-bit body: MINIBLOCK_SIZE bits = 4 bytes, all ones (delta == 1).
                data.extend_from_slice(&[0xFF; MINIBLOCK_SIZE / 8]);
            }
            remaining -= block_deltas;
        }
        data
    }

    #[test]
    fn get_end_pointer_multi_block_partial_last_block() {
        // The single-block get_end_pointer_ignores_unused_trailing_miniblocks only
        // reaches get_end_pointer's FIRST miniblock walk (the current block). With
        // miniblocks_per_block > 1 AND more than one block AND a partial last block,
        // get_end_pointer must also bound the last block INSIDE its remaining-blocks
        // loop to last_block_miniblocks -- otherwise it adds phantom bodies for the
        // unused trailing miniblocks (whose bit-width bytes are arbitrary per spec)
        // and overshoots the length stream, shifting every value on a partial range
        // read (silent corruption) or tripping a spurious "exceeds page size" error.
        // get_end_pointer_multiple_remaining_blocks does span multiple blocks but
        // uses the well-formed encoder (no garbage trailing miniblocks), so old and
        // new code agree there; only a foreign multi-block page with garbage trailing
        // bit-widths exercises this branch. QuestDB's own writer never emits this
        // shape (always 1 miniblock/block), so it is a foreign-input path reachable
        // from read_parquet(). The trusted parquet2 decoder's consumed_bytes() after
        // a full decode is the true stream end; get_end_pointer must equal it.
        //
        // Cases: 2 blocks, last block 1 delta (1 populated + 3 garbage); 2 blocks,
        // last block 40 deltas (2 populated + 2 garbage); 2 blocks, last block FULL
        // (4 populated, no garbage -- a multi-block sanity check); 3 blocks, last
        // block 40 deltas (the loop walks two full blocks before the partial one).
        for value_count in [130u64, 169, 257, 297] {
            let data = build_multi_block_multi_miniblock_stream(value_count);

            let mut dec = parquet2::encoding::delta_bitpacked::Decoder::try_new(&data).unwrap();
            let decoded = dec.by_ref().count();
            assert_eq!(decoded, value_count as usize, "value_count={value_count}");
            let true_end = dec.consumed_bytes();

            let (iter, _): (MiniblockIterator<i32>, _) = MiniblockIterator::try_new(&data).unwrap();
            let end = iter.get_end_pointer().unwrap();
            let walk = end as usize - data.as_ptr() as usize;
            assert_eq!(
                walk, true_end,
                "get_end_pointer must equal parquet2 consumed_bytes (value_count={value_count})"
            );
            // The builder emits only the delta stream, so the true end is the whole buffer.
            assert_eq!(walk, data.len(), "value_count={value_count}");
        }
    }

    // ─── Unhappy path tests ───

    #[test]
    fn empty_data_yields_no_values() {
        // An empty values buffer (an all-null data page) is treated as zero
        // values, not an error: try_new succeeds and yields no miniblocks.
        let (mut iter, first) = MiniblockIterator::<i64>::try_new(&[]).unwrap();
        assert_eq!(first, 0);
        assert!(!iter.has_values, "empty buffer declares no values");
        assert!(iter.next_miniblock().unwrap().is_none());
        let end = iter.get_end_pointer().unwrap();
        assert_eq!(end, iter.page_data.as_ptr());
    }

    #[test]
    fn all_null_page_pushes_nulls() {
        // Simulates decoding an all-null data page: the values buffer is empty
        // and the decoder is only ever asked to emit nulls (definition levels
        // are all zero). Regression for the suspended-table fuzz failure.
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_buffers(&allocator);
        let null_val: i64 = i64::MIN;
        let count = 10;
        {
            let mut decoder =
                DeltaBinaryPackedDecoder::<i64, i64>::try_new(&[], &mut buffers, null_val).unwrap();
            decoder.reserve(count).unwrap();
            decoder.push_nulls(count).unwrap();
        }
        let out: &[i64] =
            unsafe { std::slice::from_raw_parts(buffers.data_vec.as_ptr().cast(), count) };
        assert_eq!(out, &[null_val; 10]);
    }

    #[test]
    fn empty_page_value_request_errors() {
        // Companion to `all_null_page_pushes_nulls`: an all-null page only ever
        // fills nulls, but if a value is requested anyway (a corrupt page whose
        // definition levels claim a non-null), the FIRST request fails with
        // "not enough values to iterate" and writes nothing -- the phantom first
        // value of 0 is never emitted. The value_count=0 header variant is
        // covered by `value_count_zero_header_value_request_errors`.
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        // push(): the first value request errors.
        let mut buffers = create_buffers(&allocator);
        let err = {
            let mut decoder =
                DeltaBinaryPackedDecoder::<i64, i64>::try_new(&[], &mut buffers, i64::MIN).unwrap();
            decoder.reserve(2).unwrap();
            decoder.push().unwrap_err()
        };
        assert!(
            format!("{err}").contains("not enough values to iterate"),
            "got: {err}"
        );

        // push_slice(1): same contract via the batched path.
        let mut buffers = create_buffers(&allocator);
        let err = {
            let mut decoder =
                DeltaBinaryPackedDecoder::<i64, i64>::try_new(&[], &mut buffers, i64::MIN).unwrap();
            decoder.reserve(2).unwrap();
            decoder.push_slice(1).unwrap_err()
        };
        assert!(
            format!("{err}").contains("not enough values to iterate"),
            "got: {err}"
        );
    }

    #[test]
    fn value_count_zero_header_value_request_errors() {
        // A real value_count=0 header (what QuestDB's writer now emits for an
        // all-null page) behaves like the empty buffer: a value request fails
        // rather than decoding the phantom first value as 0.
        let mut header = Vec::new();
        parquet2::encoding::delta_bitpacked::encode(std::iter::empty::<i64>(), &mut header);
        assert!(!header.is_empty(), "value_count=0 header must carry bytes");

        let (iter, first) = MiniblockIterator::<i64>::try_new(&header).unwrap();
        assert_eq!(first, 0);
        assert!(!iter.has_values, "value_count=0 header has no values");

        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_buffers(&allocator);
        let err = {
            let mut decoder =
                DeltaBinaryPackedDecoder::<i64, i64>::try_new(&header, &mut buffers, i64::MIN)
                    .unwrap();
            decoder.reserve(1).unwrap();
            decoder.push().unwrap_err()
        };
        assert!(
            format!("{err}").contains("not enough values to iterate"),
            "got: {err}"
        );
    }

    #[test]
    fn block_size_zero() {
        let mut data = Vec::new();
        write_uleb128(&mut data, 0); // block_size = 0
        write_uleb128(&mut data, 4); // miniblocks_per_block
        write_uleb128(&mut data, 1); // value_count
        write_zigzag(&mut data, 0); // first_value

        let err = MiniblockIterator::<i64>::try_new(&data).err().unwrap();
        let msg = format!("{err}");
        assert!(
            msg.contains("block size must be greater than zero"),
            "got: {msg}"
        );
    }

    #[test]
    fn miniblocks_per_block_zero() {
        let mut data = Vec::new();
        write_uleb128(&mut data, 128); // block_size
        write_uleb128(&mut data, 0); // miniblocks_per_block = 0
        write_uleb128(&mut data, 1); // value_count
        write_zigzag(&mut data, 0); // first_value

        let err = MiniblockIterator::<i64>::try_new(&data).err().unwrap();
        let msg = format!("{err}");
        assert!(
            msg.contains("miniblocks-per-block must be greater than zero"),
            "got: {msg}"
        );
    }

    #[test]
    fn block_size_not_multiple_of_128() {
        let mut data = Vec::new();
        write_uleb128(&mut data, 64); // block_size = 64, not a multiple of 128
        write_uleb128(&mut data, 1); // miniblocks_per_block
        write_uleb128(&mut data, 1); // value_count
        write_zigzag(&mut data, 0); // first_value

        let err = MiniblockIterator::<i64>::try_new(&data).err().unwrap();
        let msg = format!("{err}");
        assert!(msg.contains("must be a multiple of 128"), "got: {msg}");
    }

    #[test]
    fn block_size_not_divisible_by_miniblock_count() {
        let mut data = Vec::new();
        write_uleb128(&mut data, 128); // block_size = 128 (multiple of 128), not divisible by 3
        write_uleb128(&mut data, 3); // miniblocks_per_block
        write_uleb128(&mut data, 1); // value_count
        write_zigzag(&mut data, 0); // first_value

        let err = MiniblockIterator::<i64>::try_new(&data).err().unwrap();
        let msg = format!("{err}");
        assert!(
            msg.contains("not divisible by miniblock count"),
            "got: {msg}"
        );
    }

    #[test]
    fn miniblock_size_not_multiple_of_32() {
        let mut data = Vec::new();
        write_uleb128(&mut data, 128); // block_size = 128 (multiple of 128)
        write_uleb128(&mut data, 8); // miniblocks_per_block = 8 → miniblock_size = 16
        write_uleb128(&mut data, 1); // value_count
        write_zigzag(&mut data, 0); // first_value

        let err = MiniblockIterator::<i64>::try_new(&data).err().unwrap();
        let msg = format!("{err}");
        assert!(
            msg.contains("must be a non-zero multiple of 32"),
            "got: {msg}"
        );
    }

    #[test]
    fn truncated_after_block_size() {
        // block_size=128 [0x80, 0x01], then only a single byte left.
        // ULEB128 decode is tolerant, but the decoded values will be nonsensical
        // and downstream validation catches the error.
        let data = vec![0x80, 0x01]; // block_size=128, miniblocks_per_block will decode as 0
        let err = MiniblockIterator::<i64>::try_new(&data).err().unwrap();
        let msg = format!("{err}");
        // miniblocks_per_block decodes as 0, which triggers the zero check
        assert!(
            msg.contains("miniblocks-per-block must be greater than zero"),
            "got: {msg}"
        );
    }

    #[test]
    fn truncated_block_missing_min_delta() {
        // Valid header indicating 33 values (requiring blocks), but no block data at all.
        let mut data = Vec::new();
        write_uleb128(&mut data, 128); // block_size
        write_uleb128(&mut data, 4); // miniblocks_per_block
        write_uleb128(&mut data, 33); // value_count > 1, needs blocks
        write_zigzag(&mut data, 0); // first_value
                                    // Block needs min_delta + bitwidths + packed data, but nothing follows.

        let err = MiniblockIterator::<i64>::try_new(&data).err().unwrap();
        let msg = format!("{err}");
        assert!(
            msg.contains("block header exceeds page size")
                || msg.contains("block offset out of bounds"),
            "got: {msg}"
        );
    }

    #[test]
    fn header_only_no_block_data() {
        // Valid header with value_count=5 but no block data after header.
        // The decoder should fail when trying to advance to the first block.
        let mut data = Vec::new();
        write_uleb128(&mut data, 128);
        write_uleb128(&mut data, 4);
        write_uleb128(&mut data, 5);
        write_zigzag(&mut data, 100); // first_value
                                      // No block data follows

        let err = MiniblockIterator::<i64>::try_new(&data).err().unwrap();
        let msg = format!("{err}");
        assert!(
            msg.contains("block header exceeds page size")
                || msg.contains("failed to decode min delta")
                || msg.contains("block offset out of bounds"),
            "got: {msg}"
        );
    }

    #[test]
    fn push_beyond_available_values() {
        // Encode only 2 values but try to decode 5
        let data = encode_delta_binary_packed(&[1, 2]);
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_buffers(&allocator);
        let result = {
            let mut decoder =
                DeltaBinaryPackedDecoder::<i64, i64>::try_new(&data, &mut buffers, i64::MIN)
                    .unwrap();
            // We can decode up to block_size + 1 values (padded), but eventually run out of miniblocks.
            // Requesting far more values than available triggers "not enough values to iterate".
            decoder.reserve(200).unwrap();
            decoder.push_slice(200)
        };
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("not enough values"), "got: {msg}");
    }

    #[test]
    fn get_end_pointer_multiple_remaining_blocks() {
        // 400+ values => 4+ blocks of 128 values each
        let values: Vec<i64> = (0..401).map(|i| i * 5 + 3).collect();
        let data = encode_delta_binary_packed(&values);
        let (mut iter, _first) = MiniblockIterator::<i64>::try_new(&data).unwrap();

        // Consume only 2 miniblocks from the first block
        let _mb1 = iter.next_miniblock().unwrap().unwrap();
        let _mb2 = iter.next_miniblock().unwrap().unwrap();

        let end = iter.get_end_pointer().unwrap();
        let walk = end as usize - data.as_ptr() as usize;

        // get_end_pointer returns the absolute end of the length stream regardless
        // of how far the iterator has advanced, so even mid-decode it must equal the
        // trusted parquet2 decoder's full-decode byte count (here the whole buffer).
        let mut dec = parquet2::encoding::delta_bitpacked::Decoder::try_new(&data).unwrap();
        let decoded = dec.by_ref().count();
        assert_eq!(decoded, values.len());
        assert_eq!(walk, dec.consumed_bytes());
        assert_eq!(walk, data.len());
    }

    #[test]
    fn push_slice_partial_pack_tail() {
        // 51 values: 1 initial + 50 deltas = 1 full pack of 32 + 18 in tail loop
        let values: Vec<i64> = (0..51).map(|i| i * 7 + 10).collect();
        let data = encode_delta_binary_packed(&values);
        let result = decode_i64_push_slice(&data, values.len());
        assert_eq!(result, values);
    }

    #[test]
    fn bit_width_exceeds_target_width_i32() {
        // Manually craft a page where a miniblock's bitwidth is 33 (exceeds 32 for i32).
        let mut data = Vec::new();
        write_uleb128(&mut data, 128); // block_size
        write_uleb128(&mut data, 4); // miniblocks_per_block
        write_uleb128(&mut data, 33); // value_count (needs 1 block)
        write_zigzag(&mut data, 0); // first_value
                                    // Block: min_delta
        write_zigzag(&mut data, 0);
        // Bitwidths: first miniblock has bitwidth 33 (invalid for i32), rest are 0
        data.push(33);
        data.push(0);
        data.push(0);
        data.push(0);
        // Provide enough packed data for a 33-bit miniblock (33*32/8 = 132 bytes)
        data.extend_from_slice(&[0u8; 132]);

        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_buffers(&allocator);
        let result = {
            let mut decoder =
                DeltaBinaryPackedDecoder::<i32, i32>::try_new(&data, &mut buffers, i32::MIN)
                    .unwrap();
            decoder.reserve(33).unwrap();
            decoder.push_slice(33)
        };
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(
            msg.contains("bit width") && msg.contains("exceeds"),
            "got: {msg}"
        );
    }

    #[test]
    fn truncated_block_data() {
        // Build a valid header for multiple values but truncate the block data.
        let mut data = Vec::new();
        write_uleb128(&mut data, 128); // block_size
        write_uleb128(&mut data, 4); // miniblocks_per_block
        write_uleb128(&mut data, 33); // value_count (needs blocks)
        write_zigzag(&mut data, 0); // first_value
                                    // Block header needs min_delta + 4 bitwidth bytes + packed data, but we provide nothing.

        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_buffers(&allocator);
        let result = DeltaBinaryPackedDecoder::<i64, i64>::try_new(&data, &mut buffers, i64::MIN);
        assert!(result.is_err());
    }
}
