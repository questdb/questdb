//! Decoder for Parquet `DELTA_LENGTH_BYTE_ARRAY` encoded VarcharSlice data.
//!
//! Combines delta-binary-packed length decoding with VarcharSlice aux entry
//! writing into a single `Pushable` implementation. Lengths are decoded lazily
//! 32 at a time via `MiniblockIterator` + `unpack32`, and `push_slice` batches
//! the work to avoid per-value function call overhead.

use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_read::column_sink::Pushable;
use crate::parquet_read::decoders::delta_binary_packed::{Miniblock, MiniblockIterator};
use crate::parquet_read::decoders::unpack::unpack32;
use crate::parquet_read::ColumnChunkBuffers;

const AUX_ENTRY_SIZE: usize = 16;

/// A VarcharSlice decoder for DELTA_LENGTH_BYTE_ARRAY pages.
///
/// Instead of going through a slicer + column sink, this struct directly
/// decodes delta-packed lengths and writes VarcharSlice aux entries (len, flags, ptr),
/// following the same pattern as `DeltaBinaryPackedDecoder`.
pub struct DeltaLAVarcharSliceDecoder<'a> {
    buffers: &'a mut ColumnChunkBuffers,
    data: *const u8,
    data_len: usize,
    pos: usize,
    ascii_flags: u64,
    // Delta decoder state
    iterator: MiniblockIterator<'a, i32>,
    current_value: i32,
    consumed_initial: bool,
    first_value: i32,
    values: [i32; 32],
    value_index: usize,
    packs_per_miniblock: usize,
    miniblock: Miniblock<'a>,
    miniblock_pack_index: usize,
}

impl<'a> DeltaLAVarcharSliceDecoder<'a> {
    pub fn try_new(
        data: &'a [u8],
        buffers: &'a mut ColumnChunkBuffers,
        ascii: bool,
    ) -> ParquetResult<Self> {
        let (mut iterator, first_value): (MiniblockIterator<i32>, _) =
            MiniblockIterator::try_new(data)?;

        // Scan pass: find where the string bytes start.
        let end_ptr = iterator.get_end_pointer()?;
        let data_offset = end_ptr as usize - data.as_ptr() as usize;

        let packs_per_miniblock = iterator.miniblock_size / 32;
        let (miniblock, miniblock_pack_index) = match iterator.next_miniblock()? {
            Some(mb) => (mb, 0),
            None => (Miniblock::default(), packs_per_miniblock),
        };

        let ascii_flags: u64 = if ascii { 1u64 << 32 } else { 0 };

        Ok(Self {
            data: unsafe { data.as_ptr().add(data_offset) },
            data_len: data.len() - data_offset,
            pos: 0,
            buffers,
            ascii_flags,
            iterator,
            current_value: first_value,
            consumed_initial: false,
            first_value,
            values: [0i32; 32],
            value_index: 32,
            packs_per_miniblock,
            miniblock,
            miniblock_pack_index,
        })
    }

    #[inline(always)]
    fn decode_next_length(&mut self) -> ParquetResult<i32> {
        if !self.consumed_initial {
            self.consumed_initial = true;
            return Ok(self.first_value);
        }
        if self.value_index == 32 {
            self.unpack_next()?;
            self.value_index = 0;
        }
        self.current_value = self.current_value.wrapping_add(
            self.iterator
                .min_delta
                .wrapping_add(self.values[self.value_index]),
        );
        self.value_index += 1;
        Ok(self.current_value)
    }

    #[inline(always)]
    fn unpack_next(&mut self) -> ParquetResult<()> {
        if self.miniblock_pack_index == self.packs_per_miniblock {
            match self.iterator.next_miniblock()? {
                Some(miniblock) => {
                    self.miniblock = miniblock;
                    self.miniblock_pack_index = 0;
                }
                None => {
                    return Err(fmt_err!(Layout, "not enough values to iterate"));
                }
            }
        }

        let num_bits = self.miniblock.num_bits;
        let pack_size = 32 * num_bits as usize / 8;
        let offset = self.miniblock_pack_index * pack_size;
        let data_ptr = self.miniblock.data.as_ptr();
        let data = unsafe { std::slice::from_raw_parts(data_ptr.add(offset), pack_size) };
        self.miniblock_pack_index += 1;

        unpack32(data, self.values.as_mut_ptr().cast(), num_bits as usize);

        Ok(())
    }

    /// Validates and advances the data pointer by `len` bytes, returning
    /// a pointer to the start of the string value.
    #[inline(always)]
    fn advance_data(&mut self, len: i32) -> ParquetResult<*const u8> {
        if len < 0 {
            return Err(fmt_err!(
                Layout,
                "negative string length in DELTA_LENGTH_BYTE_ARRAY page"
            ));
        }
        let len_usize = len as usize;
        if self.pos + len_usize > self.data_len {
            return Err(fmt_err!(Layout, "string data extends beyond page boundary"));
        }
        let value_ptr = unsafe { self.data.add(self.pos) };
        self.pos += len_usize;
        Ok(value_ptr)
    }

    /// Write a single VarcharSlice aux entry: (len as i32, flags as u32, ptr as u64)
    #[inline(always)]
    fn write_aux_entry(&self, aux_ptr: *mut u8, value_ptr: *const u8, len: i32) {
        unsafe {
            let addr = aux_ptr.cast::<u64>();
            std::ptr::write_unaligned(addr, self.ascii_flags | (len as u32 as u64));
            std::ptr::write_unaligned(addr.add(1), value_ptr as u64);
        }
    }

    /// Write a null VarcharSlice aux entry: (-1i32, 0u32, 0u64)
    #[inline(always)]
    fn write_null_entry(aux_ptr: *mut u8) {
        unsafe {
            let addr = aux_ptr.cast::<u64>();
            std::ptr::write_unaligned(addr, -1i32 as u32 as u64);
            std::ptr::write_unaligned(addr.add(1), 0u64);
        }
    }
}

impl Pushable for DeltaLAVarcharSliceDecoder<'_> {
    fn reserve(&mut self, count: usize) -> ParquetResult<()> {
        self.buffers.aux_vec.reserve(count * AUX_ENTRY_SIZE)?;
        Ok(())
    }

    #[inline]
    fn push(&mut self) -> ParquetResult<()> {
        let len = self.decode_next_length()?;
        let value_ptr = self.advance_data(len)?;
        let aux_ptr = unsafe {
            self.buffers
                .aux_vec
                .as_mut_ptr()
                .add(self.buffers.aux_vec.len())
        };
        self.write_aux_entry(aux_ptr, value_ptr, len);
        unsafe {
            self.buffers
                .aux_vec
                .set_len(self.buffers.aux_vec.len() + AUX_ENTRY_SIZE);
        }
        Ok(())
    }

    #[inline]
    fn push_null(&mut self) -> ParquetResult<()> {
        let aux_ptr = unsafe {
            self.buffers
                .aux_vec
                .as_mut_ptr()
                .add(self.buffers.aux_vec.len())
        };
        Self::write_null_entry(aux_ptr);
        unsafe {
            self.buffers
                .aux_vec
                .set_len(self.buffers.aux_vec.len() + AUX_ENTRY_SIZE);
        }
        Ok(())
    }

    #[inline(always)]
    fn push_nulls(&mut self, count: usize) -> ParquetResult<()> {
        let aux_ptr = unsafe {
            self.buffers
                .aux_vec
                .as_mut_ptr()
                .add(self.buffers.aux_vec.len())
        };
        for i in 0..count {
            Self::write_null_entry(unsafe { aux_ptr.add(i * AUX_ENTRY_SIZE) });
        }
        unsafe {
            self.buffers
                .aux_vec
                .set_len(self.buffers.aux_vec.len() + count * AUX_ENTRY_SIZE);
        }
        Ok(())
    }

    #[inline(always)]
    fn push_slice(&mut self, mut count: usize) -> ParquetResult<()> {
        if count == 0 {
            return Ok(());
        }

        let total_count = count;
        let mut current_value = self.current_value;
        let mut value_index = self.value_index;
        let mut pos = self.pos;
        let data = self.data;
        let data_len = self.data_len;
        let ascii_flags = self.ascii_flags;

        // Inline bounds check macro for the hot loop
        macro_rules! checked_advance {
            ($len:expr, $pos:expr) => {{
                let len = $len;
                if len < 0 || $pos + len as usize > data_len {
                    return Err(fmt_err!(Layout, "string data extends beyond page boundary"));
                }
                let ptr = unsafe { data.add($pos) };
                $pos += len as usize;
                ptr
            }};
        }

        let mut aux_ptr = unsafe {
            self.buffers
                .aux_vec
                .as_mut_ptr()
                .add(self.buffers.aux_vec.len())
        };

        if !self.consumed_initial {
            let len = self.first_value;
            let value_ptr = checked_advance!(len, pos);
            self.write_aux_entry(aux_ptr, value_ptr, len);
            aux_ptr = unsafe { aux_ptr.add(AUX_ENTRY_SIZE) };
            self.consumed_initial = true;
            count -= 1;
        }

        // Drain remaining values from the current pack
        while count > 0 && value_index != 32 {
            current_value = current_value.wrapping_add(
                self.iterator
                    .min_delta
                    .wrapping_add(self.values[value_index]),
            );
            let len = current_value;
            let value_ptr = checked_advance!(len, pos);
            self.write_aux_entry(aux_ptr, value_ptr, len);
            aux_ptr = unsafe { aux_ptr.add(AUX_ENTRY_SIZE) };
            value_index += 1;
            count -= 1;
        }

        // Full packs of 32
        while count >= 32 {
            self.unpack_next()?;
            let min_delta = self.iterator.min_delta;
            for i in 0..32 {
                current_value = current_value.wrapping_add(min_delta.wrapping_add(self.values[i]));
                let len = current_value;
                let value_ptr = checked_advance!(len, pos);
                unsafe {
                    let addr = aux_ptr.add(i * AUX_ENTRY_SIZE).cast::<u64>();
                    std::ptr::write_unaligned(addr, ascii_flags | (len as u32 as u64));
                    std::ptr::write_unaligned(addr.add(1), value_ptr as u64);
                }
            }
            count -= 32;
            aux_ptr = unsafe { aux_ptr.add(32 * AUX_ENTRY_SIZE) };
        }

        // Remaining values
        while count > 0 {
            if value_index == 32 {
                self.unpack_next()?;
                value_index = 0;
            }

            let to_write = count.min(32 - value_index);
            for i in 0..to_write {
                current_value = current_value.wrapping_add(
                    self.iterator
                        .min_delta
                        .wrapping_add(self.values[value_index + i]),
                );
                let len = current_value;
                let value_ptr = checked_advance!(len, pos);
                self.write_aux_entry(aux_ptr, value_ptr, len);
                aux_ptr = unsafe { aux_ptr.add(AUX_ENTRY_SIZE) };
            }
            value_index += to_write;
            count -= to_write;
        }

        self.current_value = current_value;
        self.value_index = value_index;
        self.pos = pos;

        unsafe {
            self.buffers
                .aux_vec
                .set_len(self.buffers.aux_vec.len() + total_count * AUX_ENTRY_SIZE);
        }

        Ok(())
    }

    #[inline]
    fn skip(&mut self, mut count: usize) -> ParquetResult<()> {
        if count == 0 {
            return Ok(());
        }

        if !self.consumed_initial {
            let len = self.first_value;
            if len < 0 || self.pos + len as usize > self.data_len {
                return Err(fmt_err!(Layout, "string data extends beyond page boundary"));
            }
            self.pos += len as usize;
            self.consumed_initial = true;
            count -= 1;
        }

        while count > 0 {
            if self.value_index == 32 {
                self.unpack_next()?;
                self.value_index = 0;
            }

            let to_skip = count.min(32 - self.value_index);
            for i in 0..to_skip {
                self.current_value = self.current_value.wrapping_add(
                    self.iterator
                        .min_delta
                        .wrapping_add(self.values[self.value_index + i]),
                );
                let len = self.current_value;
                if len < 0 || self.pos + len as usize > self.data_len {
                    return Err(fmt_err!(Layout, "string data extends beyond page boundary"));
                }
                self.pos += len as usize;
            }
            self.value_index += to_skip;
            count -= to_skip;
        }

        Ok(())
    }

    fn result(&self) -> ParquetResult<()> {
        Ok(())
    }
}
