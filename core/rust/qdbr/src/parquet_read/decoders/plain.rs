//! Decoder for Parquet `PLAIN` encoded primitive values.
//!
//! The decoder reads unaligned primitive payloads directly from page memory and
//! writes converted values into `ColumnChunkBuffers`.

use num_traits::AsPrimitive;

use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_read::column_sink::Pushable;
use crate::parquet_read::decoders::{Converter, PrimitiveConverter};
use crate::parquet_read::ColumnChunkBuffers;
use std::any::TypeId;
use std::mem::size_of;
use std::ptr;

/// Lookup table: maps each source byte to 8 unpacked boolean bytes (LSB-first).
const BOOLEAN_BITMAP_LUT: [[u8; 8]; 256] = {
    let mut lut = [[0u8; 8]; 256];
    let mut i = 0u16;
    while i < 256 {
        let b = i as u8;
        lut[i as usize] = [
            b & 1,
            (b >> 1) & 1,
            (b >> 2) & 1,
            (b >> 3) & 1,
            (b >> 4) & 1,
            (b >> 5) & 1,
            (b >> 6) & 1,
            (b >> 7) & 1,
        ];
        i += 1;
    }
    lut
};

/// A decoder for primitive types with plain encoding
/// T is the source type
/// U is the destination type
pub struct PlainPrimitiveDecoder<'a, T, U = T, V = PrimitiveConverter<T, U>>
where
    V: Converter<T, U>,
{
    values: *const T,
    values_offset: usize,
    buffers: &'a mut ColumnChunkBuffers,
    buffers_ptr: *mut U,
    buffers_offset: usize,
    null_value: U,
    converter: V,
}

/// Decoder for Parquet `PLAIN` booleans (bit-packed LSB-first).
pub struct PlainBooleanDecoder<'a> {
    values: &'a [u8],
    bit_offset: usize,
    total_bits: usize,
    buffers: &'a mut ColumnChunkBuffers,
    buffers_ptr: *mut u8,
    buffers_offset: usize,
    null_value: u8,
    error: ParquetResult<()>,
}

impl<'a> PlainBooleanDecoder<'a> {
    pub fn new(
        values: &'a [u8],
        row_count: usize,
        buffers: &'a mut ColumnChunkBuffers,
        null_value: u8,
    ) -> Self {
        let buffers_offset = buffers.data_vec.len();
        Self {
            values,
            bit_offset: 0,
            total_bits: row_count,
            buffers_ptr: buffers.data_vec.as_mut_ptr(),
            buffers,
            buffers_offset,
            null_value,
            error: Ok(()),
        }
    }

    #[inline]
    fn has_error(&self) -> bool {
        self.error.is_err()
    }

    #[inline]
    fn set_layout_error(&mut self, message: &str) {
        if self.error.is_ok() {
            self.error = Err(fmt_err!(Layout, "{}", message));
        }
    }

    #[inline]
    fn can_read_bits(&mut self, count: usize) -> bool {
        if self.has_error() {
            return false;
        }

        let Some(next_bit_offset) = self.bit_offset.checked_add(count) else {
            self.set_layout_error("plain boolean bit offset overflow");
            return false;
        };

        if next_bit_offset > self.total_bits {
            self.set_layout_error("not enough plain boolean values");
            return false;
        }
        true
    }

    #[inline]
    fn read_bit(&self, bit_offset: usize) -> u8 {
        let byte = self.values[bit_offset >> 3];
        (byte >> (bit_offset & 7)) & 1
    }
}

impl<'a, T, U, V> PlainPrimitiveDecoder<'a, T, U, V>
where
    V: Converter<T, U>,
{
    pub fn new_with(
        values: &'a [u8],
        buffers: &'a mut ColumnChunkBuffers,
        null_value: U,
        converter: V,
    ) -> Self {
        let existing = buffers.data_vec.len();
        debug_assert_eq!(
            existing % std::mem::size_of::<U>(),
            0,
            "data_vec length is not aligned to element size"
        );
        let buffers_offset = existing / std::mem::size_of::<U>();
        Self {
            values: values.as_ptr().cast(),
            values_offset: 0,
            buffers_ptr: buffers.data_vec.as_mut_ptr().cast(),
            buffers,
            buffers_offset,
            null_value,
            converter,
        }
    }
}

impl<'a, T, U> PlainPrimitiveDecoder<'a, T, U, PrimitiveConverter<T, U>>
where
    U: 'static + Copy,
    T: AsPrimitive<U>,
{
    pub fn new(values: &'a [u8], buffers: &'a mut ColumnChunkBuffers, null_value: U) -> Self {
        Self::new_with(
            values,
            buffers,
            null_value,
            PrimitiveConverter::<T, U>::new(),
        )
    }
}

impl<'a, T, U, V> Pushable for PlainPrimitiveDecoder<'a, T, U, V>
where
    T: Copy + 'static,
    U: Copy + 'static,
    V: Converter<T, U>,
{
    fn push(&mut self) -> ParquetResult<()> {
        unsafe {
            *self.buffers_ptr.add(self.buffers_offset) = self
                .converter
                .convert(self.values.add(self.values_offset).read_unaligned());
            self.buffers_offset += 1;
            self.values_offset += 1;
        }
        Ok(())
    }

    fn push_null(&mut self) -> ParquetResult<()> {
        unsafe {
            *self.buffers_ptr.add(self.buffers_offset) = self.null_value;
            self.buffers_offset += 1;
        }
        Ok(())
    }

    fn push_nulls(&mut self, count: usize) -> ParquetResult<()> {
        let out = unsafe { self.buffers_ptr.add(self.buffers_offset) };
        for i in 0..count {
            unsafe {
                *out.add(i) = self.null_value;
            }
        }
        self.buffers_offset += count;
        Ok(())
    }

    fn push_slice(&mut self, count: usize) -> ParquetResult<()> {
        if size_of::<T>() == size_of::<U>() && TypeId::of::<T>() == TypeId::of::<U>() && V::IDENTITY
        {
            // Same type, same layout: bulk copy (also handles unaligned source)
            unsafe {
                ptr::copy_nonoverlapping(
                    self.values.add(self.values_offset) as *const u8,
                    self.buffers_ptr.add(self.buffers_offset) as *mut u8,
                    count * size_of::<T>(),
                );
            }
        } else {
            let out = unsafe { self.buffers_ptr.add(self.buffers_offset) };
            for i in 0..count {
                unsafe {
                    *out.add(i) = self
                        .converter
                        .convert(self.values.add(self.values_offset + i).read_unaligned());
                }
            }
        }
        self.buffers_offset += count;
        self.values_offset += count;
        Ok(())
    }

    fn reserve(&mut self, count: usize) -> ParquetResult<()> {
        let needed = (self.buffers_offset + count) * std::mem::size_of::<U>();
        if self.buffers.data_vec.len() < needed {
            let additional = needed - self.buffers.data_vec.len();
            self.buffers.data_vec.reserve(additional)?;
            unsafe {
                self.buffers.data_vec.set_len(needed);
            }
        }
        self.buffers_ptr = self.buffers.data_vec.as_mut_ptr().cast();
        Ok(())
    }

    fn skip(&mut self, count: usize) {
        self.values_offset += count;
    }

    fn result(&self) -> ParquetResult<()> {
        Ok(())
    }
}

impl Pushable for PlainBooleanDecoder<'_> {
    fn push(&mut self) -> ParquetResult<()> {
        if !self.can_read_bits(1) {
            return Ok(());
        }

        unsafe {
            *self.buffers_ptr.add(self.buffers_offset) = self.read_bit(self.bit_offset);
        }
        self.buffers_offset += 1;
        self.bit_offset += 1;
        Ok(())
    }

    fn push_null(&mut self) -> ParquetResult<()> {
        if self.has_error() {
            return Ok(());
        }
        unsafe {
            *self.buffers_ptr.add(self.buffers_offset) = self.null_value;
        }
        self.buffers_offset += 1;
        Ok(())
    }

    fn push_nulls(&mut self, count: usize) -> ParquetResult<()> {
        if self.has_error() || count == 0 {
            return Ok(());
        }

        let out = unsafe { self.buffers_ptr.add(self.buffers_offset) };
        if self.null_value == 0 {
            unsafe {
                ptr::write_bytes(out, 0, count);
            }
        } else {
            for i in 0..count {
                unsafe {
                    *out.add(i) = self.null_value;
                }
            }
        }
        self.buffers_offset += count;
        Ok(())
    }

    fn push_slice(&mut self, count: usize) -> ParquetResult<()> {
        if count == 0 {
            return Ok(());
        }
        if !self.can_read_bits(count) {
            return Ok(());
        }

        let out = unsafe { self.buffers_ptr.add(self.buffers_offset) };
        let mut written = 0usize;
        let mut remaining = count;

        let bit_idx = self.bit_offset & 7;
        if bit_idx != 0 {
            let bits_in_first_byte = (8 - bit_idx).min(remaining);
            let byte = self.values[self.bit_offset >> 3] >> bit_idx;
            for i in 0..bits_in_first_byte {
                unsafe {
                    *out.add(written + i) = (byte >> i) & 1;
                }
            }
            self.bit_offset += bits_in_first_byte;
            written += bits_in_first_byte;
            remaining -= bits_in_first_byte;
        }

        let start_byte = self.bit_offset >> 3;
        let full_bytes = remaining >> 3;
        for i in 0..full_bytes {
            unsafe {
                ptr::copy_nonoverlapping(
                    BOOLEAN_BITMAP_LUT[self.values[start_byte + i] as usize].as_ptr(),
                    out.add(written),
                    8,
                );
            }
            written += 8;
        }
        self.bit_offset += full_bytes << 3;
        remaining -= full_bytes << 3;

        if remaining > 0 {
            unsafe {
                ptr::copy_nonoverlapping(
                    BOOLEAN_BITMAP_LUT[self.values[self.bit_offset >> 3] as usize].as_ptr(),
                    out.add(written),
                    remaining,
                );
            }
            self.bit_offset += remaining;
            written += remaining;
        }

        self.buffers_offset += written;
        Ok(())
    }

    fn reserve(&mut self, count: usize) -> ParquetResult<()> {
        let needed = self.buffers_offset + count;
        if self.buffers.data_vec.len() < needed {
            let additional = needed - self.buffers.data_vec.len();
            self.buffers.data_vec.reserve(additional)?;
            unsafe {
                self.buffers.data_vec.set_len(needed);
            }
        }
        self.buffers_ptr = self.buffers.data_vec.as_mut_ptr();
        Ok(())
    }

    fn skip(&mut self, count: usize) {
        if !self.can_read_bits(count) {
            return;
        }
        self.bit_offset += count;
    }

    fn result(&self) -> ParquetResult<()> {
        self.error.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::allocator::{AcVec, TestAllocatorState};
    use crate::parquet_read::column_sink::Pushable;
    use crate::parquet_read::decoders::plain::PlainBooleanDecoder;
    use crate::parquet_read::ColumnChunkBuffers;
    use std::ptr;

    fn create_buffers(allocator: &crate::allocator::QdbAllocator) -> ColumnChunkBuffers {
        ColumnChunkBuffers {
            data_size: 0,
            data_ptr: ptr::null_mut(),
            data_vec: AcVec::new_in(allocator.clone()),
            aux_size: 0,
            aux_ptr: ptr::null_mut(),
            aux_vec: AcVec::new_in(allocator.clone()),
        }
    }

    #[test]
    fn test_plain_boolean_decoder_push_slice() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_buffers(&allocator);

        // Bits (LSB-first): [1,0,1,1,0,0,1,0,1,1]
        let values = [0x4D, 0x03];
        let result = {
            let mut decoder = PlainBooleanDecoder::new(&values, 10, &mut buffers, 0);
            decoder.reserve(10).unwrap();
            decoder.push_slice(10).unwrap();
            decoder.result()
        };

        assert_eq!(&buffers.data_vec[..], &[1, 0, 1, 1, 0, 0, 1, 0, 1, 1]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_plain_boolean_decoder_skip_and_nulls() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_buffers(&allocator);

        // Bits (LSB-first): [0,1,1,0,1,0]
        let values = [0x16];
        let result = {
            let mut decoder = PlainBooleanDecoder::new(&values, 6, &mut buffers, 0);
            decoder.reserve(6).unwrap();
            decoder.skip(1);
            decoder.push_slice(3).unwrap();
            decoder.push_nulls(2).unwrap();
            decoder.push().unwrap();
            decoder.result()
        };

        assert_eq!(&buffers.data_vec[..], &[1, 1, 0, 0, 0, 1]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_plain_boolean_decoder_reports_layout_error() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_buffers(&allocator);

        let values = [0x05];
        let result = {
            let mut decoder = PlainBooleanDecoder::new(&values, 3, &mut buffers, 0);
            decoder.reserve(4).unwrap();
            decoder.push_slice(2).unwrap();
            decoder.push_slice(2).unwrap();
            decoder.result()
        };

        assert_eq!(&buffers.data_vec[..2], &[1, 0]);
        assert!(result.is_err());
    }
}
