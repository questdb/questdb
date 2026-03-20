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
use crate::parquet_write::varchar::SLICE_NULL_HEADER;

const AUX_ENTRY_SIZE: usize = 16;
const MAX_VARCHAR_LENGTH: i32 = (1 << 28) - 1; // 28 bits for length in header

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
    ascii: bool,
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

        Ok(Self {
            data: unsafe { data.as_ptr().add(data_offset) },
            data_len: data.len() - data_offset,
            pos: 0,
            buffers,
            ascii,
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
        if !(0..=MAX_VARCHAR_LENGTH).contains(&len) {
            return Err(fmt_err!(
                Layout,
                "invalid string length in DELTA_LENGTH_BYTE_ARRAY page: {}",
                len,
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

    /// Write a single VarcharSlice aux entry using VARCHAR-compatible header format.
    #[inline(always)]
    fn write_aux_entry(&self, aux_ptr: *mut u8, value_ptr: *const u8, len: i32) {
        let len_u = len as u32;
        let header: u32 = (len_u << 4) | if self.ascii || len_u == 0 { 3 } else { 1 };
        unsafe {
            let addr = aux_ptr.cast::<u64>();
            std::ptr::write_unaligned(addr, header as u64);
            std::ptr::write_unaligned(addr.add(1), value_ptr as u64);
        }
    }

    /// Write a null VarcharSlice aux entry: header=4 (VARCHAR_HEADER_FLAG_NULL), ptr=0.
    #[inline(always)]
    fn write_null_entry(aux_ptr: *mut u8) {
        unsafe {
            let addr = aux_ptr.cast::<u64>();
            std::ptr::write_unaligned(addr, SLICE_NULL_HEADER as u64);
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
        let ascii = self.ascii;

        // Inline bounds check macro for the hot loop
        macro_rules! checked_advance {
            ($len:expr, $pos:expr) => {{
                let len = $len;
                if !(0..=MAX_VARCHAR_LENGTH).contains(&len) || $pos + len as usize > data_len {
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
                let len_u = len as u32;
                let header: u32 = (len_u << 4) | if ascii || len_u == 0 { 3 } else { 1 };
                unsafe {
                    let addr = aux_ptr.add(i * AUX_ENTRY_SIZE).cast::<u64>();
                    std::ptr::write_unaligned(addr, header as u64);
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
                if !(0..=MAX_VARCHAR_LENGTH).contains(&len)
                    || self.pos + len as usize > self.data_len
                {
                    return Err(fmt_err!(Layout, "string data extends beyond page boundary"));
                }
                self.pos += len as usize;
            }
            self.value_index += to_skip;
            count -= to_skip;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::{AcVec, TestAllocatorState};
    use crate::parquet_read::column_sink::Pushable;
    use crate::parquet_read::ColumnChunkBuffers;
    use std::ptr;

    fn create_test_buffers(allocator: &crate::allocator::QdbAllocator) -> ColumnChunkBuffers {
        ColumnChunkBuffers {
            data_size: 0,
            data_ptr: ptr::null_mut(),
            data_vec: AcVec::new_in(allocator.clone()),
            aux_size: 0,
            aux_ptr: ptr::null_mut(),
            aux_vec: AcVec::new_in(allocator.clone()),
            page_buffers: Vec::new(),
        }
    }

    /// Encode ULEB128
    fn encode_uleb128(mut value: u64, buf: &mut Vec<u8>) {
        loop {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            buf.push(byte);
            if value == 0 {
                break;
            }
        }
    }

    /// Encode zigzag LEB128
    fn encode_zigzag_leb128(value: i64, buf: &mut Vec<u8>) {
        let zigzag = ((value << 1) ^ (value >> 63)) as u64;
        encode_uleb128(zigzag, buf);
    }

    /// Bit-pack 32 values at the given bit width, little-endian
    fn bitpack32(values: &[i32], num_bits: u8) -> Vec<u8> {
        assert_eq!(values.len(), 32);
        let total_bytes = 32 * num_bits as usize / 8;
        let mut buf = vec![0u8; total_bytes];
        if num_bits == 0 {
            return buf;
        }
        let mut bit_offset = 0usize;
        for &v in values {
            let v = v as u32;
            for b in 0..num_bits as usize {
                if (v >> b) & 1 == 1 {
                    let byte_idx = bit_offset / 8;
                    let bit_idx = bit_offset % 8;
                    buf[byte_idx] |= 1 << bit_idx;
                }
                bit_offset += 1;
            }
        }
        buf
    }

    /// Encode a DELTA_LENGTH_BYTE_ARRAY page from a list of strings.
    ///
    /// Uses block_size=128, miniblocks_per_block=1 (so miniblock_size=128, 4 packs of 32).
    fn encode_delta_length_byte_array(strings: &[&str]) -> Vec<u8> {
        let lengths: Vec<i32> = strings.iter().map(|s| s.len() as i32).collect();
        let mut buf = Vec::new();

        // Delta-binary-packed header for lengths
        let block_size: u64 = 128;
        let miniblocks_per_block: u64 = 1;
        let value_count = lengths.len() as u64;
        let first_value = if lengths.is_empty() { 0 } else { lengths[0] };

        encode_uleb128(block_size, &mut buf);
        encode_uleb128(miniblocks_per_block, &mut buf);
        encode_uleb128(value_count, &mut buf);
        encode_zigzag_leb128(first_value as i64, &mut buf);

        // Encode remaining values (deltas from previous) in blocks of 128
        if lengths.len() > 1 {
            let deltas: Vec<i32> = lengths.windows(2).map(|w| w[1] - w[0]).collect();

            // Process in blocks of 128 deltas
            let mut i = 0;
            while i < deltas.len() {
                let block_end = (i + 128).min(deltas.len());
                let block_deltas = &deltas[i..block_end];

                let min_delta = *block_deltas.iter().min().unwrap();
                encode_zigzag_leb128(min_delta as i64, &mut buf);

                // Compute relative deltas (delta - min_delta)
                let mut padded = vec![0i32; 128];
                for (j, &d) in block_deltas.iter().enumerate() {
                    padded[j] = d - min_delta;
                }

                // Determine bit width needed for the miniblock (all 128 values)
                let max_val = *padded.iter().max().unwrap() as u32;
                let num_bits = if max_val == 0 {
                    0u8
                } else {
                    32 - max_val.leading_zeros() as u8
                };

                // Bitwidths array (1 byte per miniblock, we have 1 miniblock)
                buf.push(num_bits);

                // Pack 4 groups of 32
                for pack in 0..4 {
                    let start = pack * 32;
                    let pack_data = &padded[start..start + 32];
                    buf.extend_from_slice(&bitpack32(pack_data, num_bits));
                }

                i += 128;
            }
        }

        // Append concatenated string bytes
        for s in strings {
            buf.extend_from_slice(s.as_bytes());
        }

        buf
    }

    /// Encode with raw i32 lengths (for negative length tests).
    fn encode_delta_length_byte_array_raw(lengths: &[i32], string_data: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();

        let block_size: u64 = 128;
        let miniblocks_per_block: u64 = 1;
        let value_count = lengths.len() as u64;
        let first_value = if lengths.is_empty() { 0 } else { lengths[0] };

        encode_uleb128(block_size, &mut buf);
        encode_uleb128(miniblocks_per_block, &mut buf);
        encode_uleb128(value_count, &mut buf);
        encode_zigzag_leb128(first_value as i64, &mut buf);

        if lengths.len() > 1 {
            let deltas: Vec<i32> = lengths.windows(2).map(|w| w[1] - w[0]).collect();

            let mut i = 0;
            while i < deltas.len() {
                let block_end = (i + 128).min(deltas.len());
                let block_deltas = &deltas[i..block_end];

                let min_delta = *block_deltas.iter().min().unwrap();
                encode_zigzag_leb128(min_delta as i64, &mut buf);

                let mut padded = vec![0i32; 128];
                for (j, &d) in block_deltas.iter().enumerate() {
                    padded[j] = d - min_delta;
                }

                let max_val = *padded.iter().max().unwrap() as u32;
                let num_bits = if max_val == 0 {
                    0u8
                } else {
                    32 - max_val.leading_zeros() as u8
                };

                buf.push(num_bits);
                for pack in 0..4 {
                    let start = pack * 32;
                    buf.extend_from_slice(&bitpack32(&padded[start..start + 32], num_bits));
                }

                i += 128;
            }
        }

        buf.extend_from_slice(string_data);
        buf
    }

    /// Read aux entries as (header_u64, pointer_u64) pairs.
    fn read_aux_entries(buffers: &ColumnChunkBuffers) -> Vec<(u64, u64)> {
        assert_eq!(buffers.aux_vec.len() % 16, 0);
        let count = buffers.aux_vec.len() / 16;
        let mut entries = Vec::with_capacity(count);
        for i in 0..count {
            let offset = i * 16;
            let header =
                u64::from_le_bytes(buffers.aux_vec[offset..offset + 8].try_into().unwrap());
            let pointer =
                u64::from_le_bytes(buffers.aux_vec[offset + 8..offset + 16].try_into().unwrap());
            entries.push((header, pointer));
        }
        entries
    }

    /// Extract string from an aux entry by reading `len` bytes from the pointer.
    unsafe fn read_string_from_aux(header: u64, pointer: u64) -> String {
        let len = (header >> 4) as usize;
        if len == 0 {
            return String::new();
        }
        let ptr = pointer as *const u8;
        let slice = std::slice::from_raw_parts(ptr, len);
        String::from_utf8(slice.to_vec()).unwrap()
    }

    fn is_null_entry(header: u64, pointer: u64) -> bool {
        header == SLICE_NULL_HEADER as u64 && pointer == 0
    }

    // --- Basic push ---

    #[test]
    fn test_push_single_values() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let data = encode_delta_length_byte_array(&["foo", "bar", "baz"]);

        let mut decoder = DeltaLAVarcharSliceDecoder::try_new(&data, &mut buffers, true).unwrap();
        decoder.reserve(3).unwrap();

        decoder.push().unwrap();
        decoder.push().unwrap();
        decoder.push().unwrap();

        let entries = read_aux_entries(&buffers);
        assert_eq!(entries.len(), 3);
        unsafe {
            assert_eq!(read_string_from_aux(entries[0].0, entries[0].1), "foo");
            assert_eq!(read_string_from_aux(entries[1].0, entries[1].1), "bar");
            assert_eq!(read_string_from_aux(entries[2].0, entries[2].1), "baz");
        }
        // ASCII flag=3: header = (len << 4) | 3
        assert_eq!(entries[0].0, (3 << 4) | 3);
        assert_eq!(entries[1].0, (3 << 4) | 3);
        assert_eq!(entries[2].0, (3 << 4) | 3);
    }

    // --- push_slice ---

    #[test]
    fn test_push_slice_basic() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let strings = &["hello", "world", "test", "data"];
        let data = encode_delta_length_byte_array(strings);

        let mut decoder = DeltaLAVarcharSliceDecoder::try_new(&data, &mut buffers, true).unwrap();
        decoder.reserve(4).unwrap();
        decoder.push_slice(4).unwrap();

        let entries = read_aux_entries(&buffers);
        assert_eq!(entries.len(), 4);
        unsafe {
            assert_eq!(read_string_from_aux(entries[0].0, entries[0].1), "hello");
            assert_eq!(read_string_from_aux(entries[1].0, entries[1].1), "world");
            assert_eq!(read_string_from_aux(entries[2].0, entries[2].1), "test");
            assert_eq!(read_string_from_aux(entries[3].0, entries[3].1), "data");
        }
    }

    #[test]
    fn test_push_slice_zero() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let data = encode_delta_length_byte_array(&["abc"]);

        let mut decoder = DeltaLAVarcharSliceDecoder::try_new(&data, &mut buffers, true).unwrap();
        decoder.push_slice(0).unwrap();
        assert_eq!(buffers.aux_vec.len(), 0);
    }

    // --- push_null ---

    #[test]
    fn test_push_null() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let data = encode_delta_length_byte_array(&["abc"]);

        let mut decoder = DeltaLAVarcharSliceDecoder::try_new(&data, &mut buffers, true).unwrap();
        decoder.reserve(1).unwrap();
        decoder.push_null().unwrap();

        let entries = read_aux_entries(&buffers);
        assert_eq!(entries.len(), 1);
        assert!(is_null_entry(entries[0].0, entries[0].1));
    }

    // --- push_nulls ---

    #[test]
    fn test_push_nulls() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let data = encode_delta_length_byte_array(&["abc"]);

        let mut decoder = DeltaLAVarcharSliceDecoder::try_new(&data, &mut buffers, true).unwrap();
        decoder.reserve(5).unwrap();
        decoder.push_nulls(5).unwrap();

        let entries = read_aux_entries(&buffers);
        assert_eq!(entries.len(), 5);
        for e in &entries {
            assert!(is_null_entry(e.0, e.1));
        }
    }

    #[test]
    fn test_push_nulls_zero() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let data = encode_delta_length_byte_array(&["abc"]);

        let mut decoder = DeltaLAVarcharSliceDecoder::try_new(&data, &mut buffers, true).unwrap();
        decoder.push_nulls(0).unwrap();
        assert_eq!(buffers.aux_vec.len(), 0);
    }

    // --- skip ---

    #[test]
    fn test_skip_then_push() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let data = encode_delta_length_byte_array(&["aaa", "bbb", "ccc", "ddd"]);

        let mut decoder = DeltaLAVarcharSliceDecoder::try_new(&data, &mut buffers, true).unwrap();
        decoder.reserve(2).unwrap();
        decoder.skip(2).unwrap();
        decoder.push_slice(2).unwrap();

        let entries = read_aux_entries(&buffers);
        assert_eq!(entries.len(), 2);
        unsafe {
            assert_eq!(read_string_from_aux(entries[0].0, entries[0].1), "ccc");
            assert_eq!(read_string_from_aux(entries[1].0, entries[1].1), "ddd");
        }
    }

    #[test]
    fn test_skip_zero() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let data = encode_delta_length_byte_array(&["xyz"]);

        let mut decoder = DeltaLAVarcharSliceDecoder::try_new(&data, &mut buffers, true).unwrap();
        decoder.reserve(1).unwrap();
        decoder.skip(0).unwrap();
        decoder.push().unwrap();

        let entries = read_aux_entries(&buffers);
        assert_eq!(entries.len(), 1);
        unsafe {
            assert_eq!(read_string_from_aux(entries[0].0, entries[0].1), "xyz");
        }
    }

    #[test]
    fn test_skip_all() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let data = encode_delta_length_byte_array(&["aa", "bb", "cc"]);

        let mut decoder = DeltaLAVarcharSliceDecoder::try_new(&data, &mut buffers, true).unwrap();
        decoder.skip(3).unwrap();
        assert_eq!(buffers.aux_vec.len(), 0);
    }

    // --- Mixed operations ---

    #[test]
    fn test_mixed_push_null_skip() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let data = encode_delta_length_byte_array(&["aaa", "bbb", "ccc", "ddd", "eee", "fff"]);

        let mut decoder = DeltaLAVarcharSliceDecoder::try_new(&data, &mut buffers, true).unwrap();
        decoder.reserve(5).unwrap();

        decoder.push().unwrap(); // "aaa"
        decoder.push_null().unwrap(); // null
        decoder.skip(2).unwrap(); // skip "bbb", "ccc"
        decoder.push_slice(2).unwrap(); // "ddd", "eee"
                                        // skip "fff" not consumed

        let entries = read_aux_entries(&buffers);
        assert_eq!(entries.len(), 4);
        unsafe {
            assert_eq!(read_string_from_aux(entries[0].0, entries[0].1), "aaa");
            assert!(is_null_entry(entries[1].0, entries[1].1));
            assert_eq!(read_string_from_aux(entries[2].0, entries[2].1), "ddd");
            assert_eq!(read_string_from_aux(entries[3].0, entries[3].1), "eee");
        }
    }

    // --- Consistency: push vs push_slice ---

    #[test]
    fn test_push_vs_push_slice_consistency() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let strings: Vec<&str> = vec!["alpha", "beta", "gamma", "delta", "epsilon"];
        let data = encode_delta_length_byte_array(&strings);

        // Method 1: push one-by-one
        let mut buffers1 = create_test_buffers(&allocator);
        {
            let mut decoder =
                DeltaLAVarcharSliceDecoder::try_new(&data, &mut buffers1, true).unwrap();
            decoder.reserve(5).unwrap();
            for _ in 0..5 {
                decoder.push().unwrap();
            }
        }

        // Method 2: push_slice all at once
        let mut buffers2 = create_test_buffers(&allocator);
        {
            let mut decoder =
                DeltaLAVarcharSliceDecoder::try_new(&data, &mut buffers2, true).unwrap();
            decoder.reserve(5).unwrap();
            decoder.push_slice(5).unwrap();
        }

        let entries1 = read_aux_entries(&buffers1);
        let entries2 = read_aux_entries(&buffers2);
        assert_eq!(entries1.len(), entries2.len());
        for i in 0..entries1.len() {
            assert_eq!(entries1[i].0, entries2[i].0, "header mismatch at {i}");
            unsafe {
                assert_eq!(
                    read_string_from_aux(entries1[i].0, entries1[i].1),
                    read_string_from_aux(entries2[i].0, entries2[i].1),
                    "string mismatch at {i}"
                );
            }
        }
    }

    // --- Empty strings ---

    #[test]
    fn test_empty_strings() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let data = encode_delta_length_byte_array(&["", "", ""]);

        let mut decoder = DeltaLAVarcharSliceDecoder::try_new(&data, &mut buffers, true).unwrap();
        decoder.reserve(3).unwrap();
        decoder.push_slice(3).unwrap();

        let entries = read_aux_entries(&buffers);
        assert_eq!(entries.len(), 3);
        for e in &entries {
            // Empty string: len=0, flags=3 → header = (0 << 4) | 3 = 3
            assert_eq!(e.0, 3);
        }
    }

    // --- UTF-8 flag ---

    #[test]
    fn test_utf8_flag() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let data = encode_delta_length_byte_array(&["hello", "world"]);

        // ascii=false
        let mut decoder = DeltaLAVarcharSliceDecoder::try_new(&data, &mut buffers, false).unwrap();
        decoder.reserve(2).unwrap();
        decoder.push_slice(2).unwrap();

        let entries = read_aux_entries(&buffers);
        // Non-empty with ascii=false: flags=1 → header = (5 << 4) | 1 = 81
        assert_eq!(entries[0].0, (5 << 4) | 1);
        assert_eq!(entries[1].0, (5 << 4) | 1);
    }

    #[test]
    fn test_utf8_flag_empty_string_still_has_flag_3() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let data = encode_delta_length_byte_array(&["", "hi"]);

        // ascii=false, but empty strings still get flags=3
        let mut decoder = DeltaLAVarcharSliceDecoder::try_new(&data, &mut buffers, false).unwrap();
        decoder.reserve(2).unwrap();
        decoder.push_slice(2).unwrap();

        let entries = read_aux_entries(&buffers);
        assert_eq!(entries[0].0, 3); // empty → flags=3
        assert_eq!(entries[1].0, (2 << 4) | 1); // non-empty, ascii=false → flags=1
    }

    // --- Single value ---

    #[test]
    fn test_single_value() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let data = encode_delta_length_byte_array(&["only"]);

        let mut decoder = DeltaLAVarcharSliceDecoder::try_new(&data, &mut buffers, true).unwrap();
        decoder.reserve(1).unwrap();
        decoder.push().unwrap();

        let entries = read_aux_entries(&buffers);
        assert_eq!(entries.len(), 1);
        unsafe {
            assert_eq!(read_string_from_aux(entries[0].0, entries[0].1), "only");
        }
    }

    // --- Large data spanning multiple packs ---

    #[test]
    fn test_large_spanning_multiple_packs() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let strings: Vec<String> = (0..100).map(|i| format!("str{i:03}")).collect();
        let str_refs: Vec<&str> = strings.iter().map(|s| s.as_str()).collect();
        let data = encode_delta_length_byte_array(&str_refs);

        let mut decoder = DeltaLAVarcharSliceDecoder::try_new(&data, &mut buffers, true).unwrap();
        decoder.reserve(100).unwrap();
        decoder.push_slice(100).unwrap();

        let entries = read_aux_entries(&buffers);
        assert_eq!(entries.len(), 100);
        for (i, e) in entries.iter().enumerate() {
            let expected = format!("str{i:03}");
            unsafe {
                assert_eq!(read_string_from_aux(e.0, e.1), expected, "mismatch at {i}");
            }
        }
    }

    #[test]
    fn test_push_slice_crossing_pack_boundaries() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let strings: Vec<String> = (0..70).map(|i| format!("v{i:02}")).collect();
        let str_refs: Vec<&str> = strings.iter().map(|s| s.as_str()).collect();
        let data = encode_delta_length_byte_array(&str_refs);

        let mut decoder = DeltaLAVarcharSliceDecoder::try_new(&data, &mut buffers, true).unwrap();
        decoder.reserve(70).unwrap();

        // Non-aligned chunks: 5, 13, 25, 7, 20 = 70
        decoder.push_slice(5).unwrap();
        decoder.push_slice(13).unwrap();
        decoder.push_slice(25).unwrap();
        decoder.push_slice(7).unwrap();
        decoder.push_slice(20).unwrap();

        let entries = read_aux_entries(&buffers);
        assert_eq!(entries.len(), 70);
        for (i, e) in entries.iter().enumerate() {
            let expected = format!("v{i:02}");
            unsafe {
                assert_eq!(read_string_from_aux(e.0, e.1), expected, "mismatch at {i}");
            }
        }
    }

    // --- Unhappy paths ---

    #[test]
    fn test_negative_length_error() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        // lengths: [3, -1] → first=3, delta=-4
        let data = encode_delta_length_byte_array_raw(&[3, -1], b"abc");

        let mut decoder = DeltaLAVarcharSliceDecoder::try_new(&data, &mut buffers, true).unwrap();
        decoder.reserve(2).unwrap();
        decoder.push().unwrap(); // first value (3) is OK
        let err = decoder.push().err();
        assert!(err.is_some(), "expected error for negative length");
    }

    #[test]
    fn test_length_exceeds_max_varchar() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let too_long = 1 << 28; // MAX_VARCHAR_LENGTH + 1
        let data = encode_delta_length_byte_array_raw(&[too_long], &[]);

        let mut decoder = DeltaLAVarcharSliceDecoder::try_new(&data, &mut buffers, true).unwrap();
        decoder.reserve(1).unwrap();
        let err = decoder.push().err();
        assert!(err.is_some(), "expected error for length exceeding max");
    }

    #[test]
    fn test_string_data_beyond_page_boundary() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        // Claim length=10 but only provide 3 bytes of string data
        let data = encode_delta_length_byte_array_raw(&[10], b"abc");

        let mut decoder = DeltaLAVarcharSliceDecoder::try_new(&data, &mut buffers, true).unwrap();
        decoder.reserve(1).unwrap();
        let err = decoder.push().err();
        assert!(
            err.is_some(),
            "expected error for data beyond page boundary"
        );
    }

    #[test]
    fn test_skip_beyond_page_boundary() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        // Claim length=10 but only provide 3 bytes
        let data = encode_delta_length_byte_array_raw(&[10], b"abc");

        let mut decoder = DeltaLAVarcharSliceDecoder::try_new(&data, &mut buffers, true).unwrap();
        let err = decoder.skip(1).err();
        assert!(err.is_some(), "expected error for skip beyond boundary");
    }

    // --- Cross block boundary (>129 values) ---

    #[test]
    fn test_push_slice_crossing_block_boundary() {
        // 150 values: 1 first_value + 128 deltas (block 1) + 21 deltas (block 2)
        // This forces unpack_next to fetch a new miniblock from iterator.next_miniblock()
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let strings: Vec<String> = (0..150).map(|i| format!("s{i:03}")).collect();
        let str_refs: Vec<&str> = strings.iter().map(|s| s.as_str()).collect();
        let data = encode_delta_length_byte_array(&str_refs);

        let mut decoder = DeltaLAVarcharSliceDecoder::try_new(&data, &mut buffers, true).unwrap();
        decoder.reserve(150).unwrap();
        decoder.push_slice(150).unwrap();

        let entries = read_aux_entries(&buffers);
        assert_eq!(entries.len(), 150);
        for (i, e) in entries.iter().enumerate() {
            let expected = format!("s{i:03}");
            unsafe {
                assert_eq!(read_string_from_aux(e.0, e.1), expected, "mismatch at {i}");
            }
        }
    }

    #[test]
    fn test_push_slice_remainder_after_block_boundary() {
        // Use 140 values: after consuming initial value, 139 deltas.
        // Full packs: 4 packs of 32 = 128, then remaining 11 values need new block.
        // This hits the "Remaining values" path (line ~307-310) where value_index == 32
        // triggers unpack_next across a block boundary.
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let strings: Vec<String> = (0..140).map(|i| format!("x{i:03}")).collect();
        let str_refs: Vec<&str> = strings.iter().map(|s| s.as_str()).collect();
        let data = encode_delta_length_byte_array(&str_refs);

        let mut decoder = DeltaLAVarcharSliceDecoder::try_new(&data, &mut buffers, true).unwrap();
        decoder.reserve(140).unwrap();
        // Push in a way that leaves remainder after full packs cross the block boundary
        decoder.push_slice(140).unwrap();

        let entries = read_aux_entries(&buffers);
        assert_eq!(entries.len(), 140);
        unsafe {
            assert_eq!(read_string_from_aux(entries[0].0, entries[0].1), "x000");
            assert_eq!(read_string_from_aux(entries[139].0, entries[139].1), "x139");
        }
    }

    #[test]
    fn test_push_one_by_one_crossing_block_boundary() {
        // push() one-by-one through 150 values, crossing block boundary
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let strings: Vec<String> = (0..150).map(|i| format!("p{i:03}")).collect();
        let str_refs: Vec<&str> = strings.iter().map(|s| s.as_str()).collect();
        let data = encode_delta_length_byte_array(&str_refs);

        let mut decoder = DeltaLAVarcharSliceDecoder::try_new(&data, &mut buffers, true).unwrap();
        decoder.reserve(150).unwrap();
        for _ in 0..150 {
            decoder.push().unwrap();
        }

        let entries = read_aux_entries(&buffers);
        assert_eq!(entries.len(), 150);
        unsafe {
            assert_eq!(read_string_from_aux(entries[0].0, entries[0].1), "p000");
            assert_eq!(read_string_from_aux(entries[149].0, entries[149].1), "p149");
        }
    }

    #[test]
    fn test_skip_crossing_block_boundary() {
        // Skip 135 values (crossing block boundary), then push remaining
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let strings: Vec<String> = (0..150).map(|i| format!("k{i:03}")).collect();
        let str_refs: Vec<&str> = strings.iter().map(|s| s.as_str()).collect();
        let data = encode_delta_length_byte_array(&str_refs);

        let mut decoder = DeltaLAVarcharSliceDecoder::try_new(&data, &mut buffers, true).unwrap();
        decoder.reserve(15).unwrap();
        decoder.skip(135).unwrap();
        decoder.push_slice(15).unwrap();

        let entries = read_aux_entries(&buffers);
        assert_eq!(entries.len(), 15);
        for (i, e) in entries.iter().enumerate() {
            let expected = format!("k{:03}", i + 135);
            unsafe {
                assert_eq!(read_string_from_aux(e.0, e.1), expected, "mismatch at {i}");
            }
        }
    }

    #[test]
    fn test_push_slice_small_chunks_crossing_block_boundary() {
        // Push in small chunks (< 32) that cross both pack and block boundaries.
        // This exercises the "Remaining values" tail path repeatedly, including
        // when value_index wraps at 32 forcing unpack_next.
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let n = 200;
        let strings: Vec<String> = (0..n).map(|i| format!("r{i:03}")).collect();
        let str_refs: Vec<&str> = strings.iter().map(|s| s.as_str()).collect();
        let data = encode_delta_length_byte_array(&str_refs);

        let mut decoder = DeltaLAVarcharSliceDecoder::try_new(&data, &mut buffers, true).unwrap();
        decoder.reserve(n).unwrap();

        // Push in chunks of 7 to repeatedly cross pack boundaries in the tail
        let mut pushed = 0;
        while pushed < n {
            let chunk = 7.min(n - pushed);
            decoder.push_slice(chunk).unwrap();
            pushed += chunk;
        }

        let entries = read_aux_entries(&buffers);
        assert_eq!(entries.len(), n);
        for (i, e) in entries.iter().enumerate() {
            let expected = format!("r{i:03}");
            unsafe {
                assert_eq!(read_string_from_aux(e.0, e.1), expected, "mismatch at {i}");
            }
        }
    }

    #[test]
    fn test_skip_in_chunks_crossing_block_boundary() {
        // Skip in small chunks to hit the skip loop's unpack_next at pack boundaries
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let n = 200;
        let strings: Vec<String> = (0..n).map(|i| format!("q{i:03}")).collect();
        let str_refs: Vec<&str> = strings.iter().map(|s| s.as_str()).collect();
        let data = encode_delta_length_byte_array(&str_refs);

        let mut decoder = DeltaLAVarcharSliceDecoder::try_new(&data, &mut buffers, true).unwrap();
        decoder.reserve(5).unwrap();

        // Skip in chunks of 11 to cross pack and block boundaries
        let mut skipped = 0;
        let skip_total = n - 5;
        while skipped < skip_total {
            let chunk = 11.min(skip_total - skipped);
            decoder.skip(chunk).unwrap();
            skipped += chunk;
        }
        decoder.push_slice(5).unwrap();

        let entries = read_aux_entries(&buffers);
        assert_eq!(entries.len(), 5);
        for (i, e) in entries.iter().enumerate() {
            let expected = format!("q{:03}", i + skip_total);
            unsafe {
                assert_eq!(read_string_from_aux(e.0, e.1), expected, "mismatch at {i}");
            }
        }
    }

    #[test]
    fn test_push_beyond_encoded_values() {
        // Encode 3 empty strings (length 0). The delta encoding pads the miniblock
        // to 128 delta slots, all zero. We can consume 1 first_value + 128 padding
        // values = 129 total before exhausting the single block. The 130th push
        // triggers unpack_next → next_miniblock() → None → error.
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let data = encode_delta_length_byte_array(&["", "", ""]);

        let mut decoder = DeltaLAVarcharSliceDecoder::try_new(&data, &mut buffers, true).unwrap();
        decoder.reserve(200).unwrap();

        // Consume 129 values: 1 first_value + 128 from 4 packs of 32
        for _ in 0..129 {
            decoder.push().unwrap();
        }
        // 130th push: unpack_next finds no more miniblocks
        let err = decoder.push().err();
        assert!(err.is_some(), "expected error when miniblocks exhausted");
    }

    #[test]
    fn test_skip_error_in_inner_loop() {
        // Construct data where the 2nd value (not first) has a length that causes
        // data to exceed page boundary, triggered during skip's inner for-loop.
        // lengths: [3, 3, 100] with only 6 bytes of string data
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let data = encode_delta_length_byte_array_raw(&[3, 3, 100], b"abcdef");

        let mut decoder = DeltaLAVarcharSliceDecoder::try_new(&data, &mut buffers, true).unwrap();
        // Skip all 3: first=3 OK, second=3 OK, third=100 exceeds 6-byte boundary
        let err = decoder.skip(3).err();
        assert!(
            err.is_some(),
            "expected error for skip hitting bad length in inner loop"
        );
    }

    #[test]
    fn test_invalid_header() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        // Empty data → can't decode ULEB128 header
        let data: &[u8] = &[];
        let result = DeltaLAVarcharSliceDecoder::try_new(data, &mut buffers, true);
        assert!(result.is_err(), "expected error for empty header");
    }
}
