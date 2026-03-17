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
use std::marker::PhantomData;
use std::mem::size_of;
use std::ptr;

/// Lookup table: maps each source byte to 8 unpacked boolean bytes (LSB-first).
pub(crate) const BOOLEAN_BITMAP_LUT: [[u8; 8]; 256] = {
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
    values_len: usize,
    values_offset: usize,
    write_offset: usize,
    null_value: U,
    converter: V,
    _marker: PhantomData<&'a [u8]>,
}

/// Decoder for Parquet `PLAIN` booleans (bit-packed LSB-first).
pub struct PlainBooleanDecoder<'a> {
    values: &'a [u8],
    bit_offset: usize,
    total_bits: usize,
    write_offset: usize,
    null_value: u8,
}

impl<'a> PlainBooleanDecoder<'a> {
    pub fn new(values: &'a [u8], bufs: &ColumnChunkBuffers, null_value: u8) -> Self {
        Self {
            values,
            bit_offset: 0,
            total_bits: values.len() * 8,
            write_offset: bufs.data_vec.len(),
            null_value,
        }
    }

    #[inline]
    fn can_read_bits(&mut self, count: usize) -> ParquetResult<()> {
        let Some(next_bit_offset) = self.bit_offset.checked_add(count) else {
            return Err(fmt_err!(Layout, "plain boolean bit offset overflow"));
        };
        if next_bit_offset > self.total_bits {
            return Err(fmt_err!(Layout, "not enough plain boolean values"));
        }
        Ok(())
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
        bufs: &ColumnChunkBuffers,
        null_value: U,
        converter: V,
    ) -> Self {
        let existing = bufs.data_vec.len();
        debug_assert_eq!(
            existing % std::mem::size_of::<U>(),
            0,
            "data_vec length is not aligned to element size"
        );
        let write_offset = existing / std::mem::size_of::<U>();
        debug_assert_eq!(
            values.len() % size_of::<T>(),
            0,
            "plain source buffer is not element-aligned"
        );
        Self {
            values: values.as_ptr().cast(),
            values_len: values.len() / size_of::<T>(),
            values_offset: 0,
            write_offset,
            null_value,
            converter,
            _marker: PhantomData,
        }
    }
}

impl<'a, T, U> PlainPrimitiveDecoder<'a, T, U, PrimitiveConverter<T, U>>
where
    U: 'static + Copy,
    T: AsPrimitive<U>,
{
    pub fn new(values: &'a [u8], bufs: &ColumnChunkBuffers, null_value: U) -> Self {
        Self::new_with(
            values,
            bufs,
            null_value,
            PrimitiveConverter::<T, U>::new(),
        )
    }
}

impl<'a, T, U, V> Pushable<ColumnChunkBuffers> for PlainPrimitiveDecoder<'a, T, U, V>
where
    T: Copy + 'static,
    U: Copy + 'static,
    V: Converter<T, U>,
{
    fn push(&mut self, sink: &mut ColumnChunkBuffers) -> ParquetResult<()> {
        if self.values_offset >= self.values_len {
            return Err(fmt_err!(
                Layout,
                "not enough plain values: offset {} >= length {}",
                self.values_offset,
                self.values_len
            ));
        }
        let out_ptr: *mut U = sink.data_vec.as_mut_ptr().cast();
        // SAFETY: destination pointer stays in-bounds because decode paths reserve output upfront.
        unsafe {
            *out_ptr.add(self.write_offset) = self
                .converter
                .convert(self.values.add(self.values_offset).read_unaligned());
            self.write_offset += 1;
            self.values_offset += 1;
        }
        Ok(())
    }

    fn push_null(&mut self, sink: &mut ColumnChunkBuffers) -> ParquetResult<()> {
        let out_ptr: *mut U = sink.data_vec.as_mut_ptr().cast();
        // SAFETY: destination pointer stays in-bounds because decode paths reserve output upfront.
        unsafe {
            *out_ptr.add(self.write_offset) = self.null_value;
            self.write_offset += 1;
        }
        Ok(())
    }

    fn push_nulls(&mut self, sink: &mut ColumnChunkBuffers, count: usize) -> ParquetResult<()> {
        let out_ptr: *mut U = sink.data_vec.as_mut_ptr().cast();
        // SAFETY: destination pointer stays in-bounds because decode paths reserve output upfront.
        let out = unsafe { out_ptr.add(self.write_offset) };
        for i in 0..count {
            // SAFETY: `out` points to reserved output space and `i < count`.
            unsafe {
                *out.add(i) = self.null_value;
            }
        }
        self.write_offset += count;
        Ok(())
    }

    fn push_slice(&mut self, sink: &mut ColumnChunkBuffers, count: usize) -> ParquetResult<()> {
        if self.values_offset + count > self.values_len {
            return Err(fmt_err!(
                Layout,
                "not enough plain values for slice: offset {} + count {} > length {}",
                self.values_offset,
                count,
                self.values_len
            ));
        }

        let out_ptr: *mut U = sink.data_vec.as_mut_ptr().cast();
        if size_of::<T>() == size_of::<U>() && TypeId::of::<T>() == TypeId::of::<U>() && V::IDENTITY
        {
            // Same type, same layout: bulk copy (also handles unaligned source)
            // SAFETY: destination bytes are in-bounds because decode paths reserve output upfront.
            // We rely on trusted parquet metadata/level streams to keep source in-bounds.
            unsafe {
                ptr::copy_nonoverlapping(
                    self.values.add(self.values_offset) as *const u8,
                    out_ptr.add(self.write_offset) as *mut u8,
                    count * size_of::<T>(),
                );
            }
        } else {
            // SAFETY: destination pointer stays in-bounds because decode paths reserve output upfront.
            let out = unsafe { out_ptr.add(self.write_offset) };
            for i in 0..count {
                // SAFETY: destination element `write_offset + i` is within reserved output.
                // We rely on trusted parquet metadata/level streams to keep source in-bounds.
                unsafe {
                    *out.add(i) = self
                        .converter
                        .convert(self.values.add(self.values_offset + i).read_unaligned());
                }
            }
        }
        self.write_offset += count;
        self.values_offset += count;
        Ok(())
    }

    fn reserve(&mut self, sink: &mut ColumnChunkBuffers, count: usize) -> ParquetResult<()> {
        let needed = (self.write_offset + count) * std::mem::size_of::<U>();
        if sink.data_vec.len() < needed {
            let additional = needed - sink.data_vec.len();
            sink.data_vec.reserve(additional)?;
            // SAFETY: `needed <= capacity` after reserve; we only expose initialized bytes
            // after writes in push/push_slice paths.
            unsafe {
                sink.data_vec.set_len(needed);
            }
        }
        Ok(())
    }

    fn skip(&mut self, count: usize) -> ParquetResult<()> {
        self.values_offset += count;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Generic PrimitiveSink impl — enables aggregation and filter pushdown
// with the same decoder that handles materialization.
// ---------------------------------------------------------------------------

use crate::parquet_read::decode::vectorized::{PrimitiveSink, PrimitiveSinkWrapper};

impl<'a, T, U> PlainPrimitiveDecoder<'a, T, U, PrimitiveConverter<T, U>>
where
    U: 'static + Copy,
    T: AsPrimitive<U>,
{
    /// Construct a decoder targeting a generic `PrimitiveSink` instead of
    /// `ColumnChunkBuffers`. The sink manages its own write position.
    pub fn new_for_sink<S: PrimitiveSink<U>>(
        values: &'a [u8],
        _sink: &mut S,
        null_value: U,
    ) -> Self {
        debug_assert_eq!(
            values.len() % size_of::<T>(),
            0,
            "plain source buffer is not element-aligned"
        );
        Self {
            values: values.as_ptr().cast(),
            values_len: values.len() / size_of::<T>(),
            values_offset: 0,
            write_offset: 0, // unused for PrimitiveSink path
            null_value,
            converter: PrimitiveConverter::<T, U>::new(),
            _marker: PhantomData,
        }
    }
}

/// `Pushable` impl for the `PrimitiveSinkWrapper` newtype.
/// Uses `PrimitiveSink` methods instead of raw pointer writes into `ColumnChunkBuffers`.
/// This avoids coherence conflicts with the existing `Pushable<ColumnChunkBuffers>` impl.
impl<'a, T, U, V, S> Pushable<PrimitiveSinkWrapper<S>> for PlainPrimitiveDecoder<'a, T, U, V>
where
    T: Copy + 'static,
    U: Copy + 'static,
    V: Converter<T, U>,
    S: PrimitiveSink<U>,
{
    fn reserve(&mut self, sink: &mut PrimitiveSinkWrapper<S>, count: usize) -> ParquetResult<()> {
        sink.0.reserve(count)
    }

    #[inline]
    fn push(&mut self, sink: &mut PrimitiveSinkWrapper<S>) -> ParquetResult<()> {
        if self.values_offset >= self.values_len {
            return Err(fmt_err!(
                Layout,
                "not enough plain values: offset {} >= length {}",
                self.values_offset,
                self.values_len
            ));
        }
        let value = unsafe {
            self.converter
                .convert(self.values.add(self.values_offset).read_unaligned())
        };
        self.values_offset += 1;
        sink.0.write_value(value);
        Ok(())
    }

    #[inline]
    fn push_null(&mut self, sink: &mut PrimitiveSinkWrapper<S>) -> ParquetResult<()> {
        sink.0.write_null();
        Ok(())
    }

    #[inline]
    fn push_nulls(&mut self, sink: &mut PrimitiveSinkWrapper<S>, count: usize) -> ParquetResult<()> {
        sink.0.write_nulls(count);
        Ok(())
    }

    #[inline]
    fn push_slice(&mut self, sink: &mut PrimitiveSinkWrapper<S>, count: usize) -> ParquetResult<()> {
        if self.values_offset + count > self.values_len {
            return Err(fmt_err!(
                Layout,
                "not enough plain values for slice: offset {} + count {} > length {}",
                self.values_offset,
                count,
                self.values_len
            ));
        }
        for _ in 0..count {
            let value = unsafe {
                self.converter
                    .convert(self.values.add(self.values_offset).read_unaligned())
            };
            self.values_offset += 1;
            sink.0.write_value(value);
        }
        Ok(())
    }

    fn skip(&mut self, count: usize) -> ParquetResult<()> {
        self.values_offset += count;
        Ok(())
    }
}

impl Pushable<ColumnChunkBuffers> for PlainBooleanDecoder<'_> {
    fn push(&mut self, sink: &mut ColumnChunkBuffers) -> ParquetResult<()> {
        self.can_read_bits(1)?;
        let out_ptr = sink.data_vec.as_mut_ptr();
        unsafe {
            *out_ptr.add(self.write_offset) = self.read_bit(self.bit_offset);
        }
        self.write_offset += 1;
        self.bit_offset += 1;
        Ok(())
    }

    fn push_null(&mut self, sink: &mut ColumnChunkBuffers) -> ParquetResult<()> {
        let out_ptr = sink.data_vec.as_mut_ptr();
        unsafe {
            *out_ptr.add(self.write_offset) = self.null_value;
        }
        self.write_offset += 1;
        Ok(())
    }

    fn push_nulls(&mut self, sink: &mut ColumnChunkBuffers, count: usize) -> ParquetResult<()> {
        if count == 0 {
            return Ok(());
        }

        let out = unsafe { sink.data_vec.as_mut_ptr().add(self.write_offset) };
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
        self.write_offset += count;
        Ok(())
    }

    fn push_slice(&mut self, sink: &mut ColumnChunkBuffers, count: usize) -> ParquetResult<()> {
        if count == 0 {
            return Ok(());
        }
        self.can_read_bits(count)?;

        let out = unsafe { sink.data_vec.as_mut_ptr().add(self.write_offset) };
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

        self.write_offset += written;
        Ok(())
    }

    fn reserve(&mut self, sink: &mut ColumnChunkBuffers, count: usize) -> ParquetResult<()> {
        let needed = self.write_offset + count;
        if sink.data_vec.len() < needed {
            let additional = needed - sink.data_vec.len();
            sink.data_vec.reserve(additional)?;
            unsafe {
                sink.data_vec.set_len(needed);
            }
        }
        Ok(())
    }

    fn skip(&mut self, count: usize) -> ParquetResult<()> {
        self.can_read_bits(count)?;
        self.bit_offset += count;
        Ok(())
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
            page_buffers: Vec::new(),
        }
    }

    #[test]
    fn test_plain_boolean_decoder_push_slice() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_buffers(&allocator);

        // Bits (LSB-first): [1,0,1,1,0,0,1,0,1,1]
        let values = [0x4D, 0x03];
        let mut decoder = PlainBooleanDecoder::new(&values, &buffers, 0);
        decoder.reserve(&mut buffers, 10).unwrap();
        decoder.push_slice(&mut buffers, 10).unwrap();

        assert_eq!(&buffers.data_vec[..], &[1, 0, 1, 1, 0, 0, 1, 0, 1, 1]);
    }

    #[test]
    fn test_plain_boolean_decoder_skip_and_nulls() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_buffers(&allocator);

        // Bits (LSB-first): [0,1,1,0,1,0]
        let values = [0x16];
        let mut decoder = PlainBooleanDecoder::new(&values, &buffers, 0);
        decoder.reserve(&mut buffers, 6).unwrap();
        decoder.skip(1).unwrap();
        decoder.push_slice(&mut buffers, 3).unwrap();
        decoder.push_nulls(&mut buffers, 2).unwrap();
        decoder.push(&mut buffers).unwrap();

        assert_eq!(&buffers.data_vec[..], &[1, 1, 0, 0, 0, 1]);
    }

    #[test]
    fn test_plain_boolean_decoder_reports_layout_error() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_buffers(&allocator);

        let values = [0x05];
        let mut decoder = PlainBooleanDecoder::new(&values, &buffers, 0);
        decoder.reserve(&mut buffers, 9).unwrap();
        decoder.push_slice(&mut buffers, 4).unwrap();
        decoder.push_slice(&mut buffers, 4).unwrap();
        decoder.push_slice(&mut buffers, 1).unwrap_err(); // only 1 bit left

        assert_eq!(&buffers.data_vec[..2], &[1, 0]);
    }
}
