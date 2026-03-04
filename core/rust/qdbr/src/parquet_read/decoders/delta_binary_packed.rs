//! Decoder for Parquet `DELTA_BINARY_PACKED` encoded primitive data.
//!
//! This adapter wraps `parquet2`'s delta decoder and implements the `Pushable`
//! interface so row-group decode can stream values directly into
//! `ColumnChunkBuffers`.

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
    // offset in data for the bidwidths field of the current/next block depending on the state of the iterator
    block_bitwidths_offset: usize,
    // miniblock size in number of values, this is constant across all blocks
    pub(crate) miniblock_size: usize,
    // offset of the next miniblock to decode
    miniblock_offset: usize,
    // index of the next miniblock to decode
    miniblock_index: usize,
    pub(crate) min_delta: U,
}

impl<'a, U> MiniblockIterator<'a, U>
where
    U: Default + WrappingAdd + Copy + 'static,
    i64: AsPrimitive<U>,
{
    pub fn try_new(page_data: &'a [u8]) -> ParquetResult<(Self, U)> {
        let (block_size, offset) = uleb128::decode(page_data)
            .map_err(|_| fmt_err!(Layout, "failed to decode block size"))?;
        if block_size == 0 {
            return Err(fmt_err!(
                Layout,
                "delta binary packed block size must be greater than zero"
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
        if miniblock_size == 0 || miniblock_size % 32 != 0 {
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
            block_bitwidths_offset: 0,
            miniblock_size,
            miniblock_offset: 0,
            miniblock_index: 0,
            min_delta: U::default(),
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

        // Skip the remaining miniblocks in the current block
        while miniblock_index < miniblocks_per_block {
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

            for i in 0..miniblocks_per_block {
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

        Ok(Self {
            buffers_ptr: buffers.data_vec.as_mut_ptr().cast(),
            buffers_offset: buffers.data_vec.len() / std::mem::size_of::<T>(),
            buffers,
            null_value,
            current_value: first_value,
            values: [U::default(); 32],
            iterator: miniblock_iterator,
            consumed_initial_value: false,
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

    fn result(&self) -> ParquetResult<()> {
        Ok(())
    }
}
