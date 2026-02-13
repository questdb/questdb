//! Small iterator primitives used by hybrid RLE dictionary decoding.
//!
//! `RleDictionaryDecoder` switches between these iterator shapes when it moves
//! across run boundaries in a hybrid-RLE stream.

use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_read::column_sink::Pushable;
use crate::parquet_read::ColumnChunkBuffers;
use parquet2::encoding::bitpacked;
use parquet2::encoding::hybrid_rle::{Decoder as HybridRunDecoder, HybridEncoded as HybridRun};
use std::ptr;

/// Custom repeat iterator that supports efficient skipping.
pub struct RepeatN {
    pub value: u32,
    pub remaining: usize,
}

impl RepeatN {
    #[inline]
    pub fn new(value: u32, count: usize) -> Self {
        Self { value, remaining: count }
    }

    #[inline]
    pub fn next(&mut self) -> Option<u32> {
        if self.remaining > 0 {
            self.remaining -= 1;
            Some(self.value)
        } else {
            None
        }
    }

    #[inline]
    pub fn skip(&mut self, n: usize) -> usize {
        let skipped = n.min(self.remaining);
        self.remaining -= skipped;
        skipped
    }
}

type BitpackedIterator<'a> = bitpacked::Decoder<'a, u32>;

pub enum RleIterator<'a> {
    Bitpacked(BitpackedIterator<'a>),
    Rle(RepeatN),
    /// Fast path for 8-bit bitpacked data: each byte is a dict index.
    /// Avoids the unpack step entirely.
    ByteIndices {
        data: &'a [u8],
        pos: usize,
    },
}

impl RleIterator<'_> {
    #[inline(always)]
    pub fn next(&mut self) -> Option<u32> {
        match self {
            RleIterator::Bitpacked(iter) => iter.next(),
            RleIterator::Rle(iter) => iter.next(),
            RleIterator::ByteIndices { data, pos } => {
                if *pos < data.len() {
                    let val = data[*pos] as u32;
                    *pos += 1;
                    Some(val)
                } else {
                    None
                }
            }
        }
    }

    #[inline]
    pub fn skip(&mut self, n: usize) -> usize {
        match self {
            RleIterator::Bitpacked(iter) => iter.advance(n),
            RleIterator::Rle(iter) => iter.skip(n),
            RleIterator::ByteIndices { data, pos } => {
                let avail = data.len() - *pos;
                let skipped = n.min(avail);
                *pos += skipped;
                skipped
            }
        }
    }
}

/// Lookup table: maps each encoded byte to 8 unpacked boolean bytes (LSB-first).
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

// Active run state for the current hybrid-RLE chunk.
// We keep this explicit state to avoid calling a per-value decoder API.
enum RleBooleanRun<'a> {
    None,
    Bitpacked {
        data: &'a [u8],
        byte_offset: usize,
        bit_offset: u8,
        remaining: usize,
    },
    Rle {
        value: u8,
        remaining: usize,
    },
}

impl RleBooleanRun<'_> {
    #[inline]
    fn remaining(&self) -> usize {
        match self {
            RleBooleanRun::None => 0,
            RleBooleanRun::Bitpacked { remaining, .. } => *remaining,
            RleBooleanRun::Rle { remaining, .. } => *remaining,
        }
    }
}

/// Decoder for Parquet boolean values encoded with `RLE` (hybrid RLE/bit-packed, bit-width=1).
pub struct RleBooleanDecoder<'a> {
    decoder: HybridRunDecoder<'a>,
    run: RleBooleanRun<'a>,
    remaining_values: usize,
    buffers: &'a mut ColumnChunkBuffers,
    buffers_ptr: *mut u8,
    buffers_offset: usize,
    null_value: u8,
    error: ParquetResult<()>,
}

impl<'a> RleBooleanDecoder<'a> {
    pub fn try_new(
        values: &'a [u8],
        row_count: usize,
        buffers: &'a mut ColumnChunkBuffers,
        null_value: u8,
    ) -> ParquetResult<Self> {
        // Parquet boolean RLE payload is prefixed with a 4-byte LE length.
        if values.len() < 4 {
            return Err(fmt_err!(
                Layout,
                "boolean RLE buffer too short: {} bytes",
                values.len()
            ));
        }

        let _payload_len = u32::from_le_bytes(values[..4].try_into().unwrap()) as usize;
        // Decode by runs (bitpacked / repeated), then expand in bulk.
        let decoder = HybridRunDecoder::new(&values[4..], 1);

        let buffers_offset = buffers.data_vec.len();
        Ok(Self {
            decoder,
            run: RleBooleanRun::None,
            remaining_values: row_count,
            buffers_ptr: buffers.data_vec.as_mut_ptr(),
            buffers,
            buffers_offset,
            null_value,
            error: Ok(()),
        })
    }

    #[inline]
    fn set_not_enough_values_error(&mut self) {
        if self.error.is_ok() {
            self.error = Err(fmt_err!(Layout, "not enough RLE boolean values"));
        }
    }

    #[inline]
    fn load_next_run(&mut self) -> bool {
        match self.decoder.next() {
            Some(Ok(HybridRun::Bitpacked(data))) => {
                // Each byte carries 8 booleans (LSB-first).
                self.run = RleBooleanRun::Bitpacked {
                    data,
                    byte_offset: 0,
                    bit_offset: 0,
                    remaining: data.len() * 8,
                };
                true
            }
            Some(Ok(HybridRun::Rle(data, run_len))) => {
                let value = data.first().copied().unwrap_or(0) & 1;
                self.run = RleBooleanRun::Rle { value, remaining: run_len };
                true
            }
            Some(Err(e)) => {
                if self.error.is_ok() {
                    self.error = Err(e.into());
                }
                self.run = RleBooleanRun::None;
                false
            }
            None => {
                self.set_not_enough_values_error();
                self.run = RleBooleanRun::None;
                false
            }
        }
    }

    #[inline]
    fn ensure_run(&mut self) -> bool {
        // Reuse current run while it has values; otherwise pull the next run.
        self.run.remaining() > 0 || self.load_next_run()
    }

    #[inline]
    fn decode_bitpacked_into(
        data: &[u8],
        byte_offset: &mut usize,
        bit_offset: &mut u8,
        out: *mut u8,
        count: usize,
    ) {
        let mut written = 0usize;
        let mut remaining = count;

        // First align to byte boundary if we are in the middle of a source byte.
        if *bit_offset != 0 {
            let bits_in_first_byte = ((8 - *bit_offset as usize).min(remaining)) as usize;
            let byte = data[*byte_offset] >> *bit_offset;
            for i in 0..bits_in_first_byte {
                unsafe {
                    *out.add(written + i) = (byte >> i) & 1;
                }
            }
            written += bits_in_first_byte;
            remaining -= bits_in_first_byte;
            *bit_offset += bits_in_first_byte as u8;
            if *bit_offset == 8 {
                *bit_offset = 0;
                *byte_offset += 1;
            }
        }

        let full_bytes = remaining >> 3;
        if full_bytes > 0 {
            // Expand full source bytes via LUT: 1 byte -> 8 decoded values.
            let mut dst = unsafe { out.add(written) };
            for i in 0..full_bytes {
                unsafe {
                    ptr::copy_nonoverlapping(
                        BOOLEAN_BITMAP_LUT[data[*byte_offset + i] as usize].as_ptr(),
                        dst,
                        8,
                    );
                    dst = dst.add(8);
                }
            }
            *byte_offset += full_bytes;
            let copied = full_bytes << 3;
            written += copied;
            remaining -= copied;
        }

        if remaining > 0 {
            // Tail (1..7 bits) from the next byte.
            unsafe {
                ptr::copy_nonoverlapping(
                    BOOLEAN_BITMAP_LUT[data[*byte_offset] as usize].as_ptr(),
                    out.add(written),
                    remaining,
                );
            }
            *bit_offset = remaining as u8;
        }
    }

    #[inline]
    fn skip_bitpacked(byte_offset: &mut usize, bit_offset: &mut u8, count: usize) {
        // Advance bit cursor arithmetically instead of consuming values one by one.
        let total = *bit_offset as usize + count;
        *byte_offset += total >> 3;
        *bit_offset = (total & 7) as u8;
    }

    fn decode_values_into(&mut self, mut out: *mut u8, mut count: usize) {
        while count > 0 {
            if self.remaining_values == 0 {
                self.set_not_enough_values_error();
                // Keep output deterministic after reserve() even on malformed input.
                unsafe {
                    ptr::write_bytes(out, 0, count);
                }
                return;
            }

            if !self.ensure_run() {
                unsafe {
                    ptr::write_bytes(out, 0, count);
                }
                return;
            }

            let run_available = self.run.remaining().min(self.remaining_values);
            if run_available == 0 {
                self.run = RleBooleanRun::None;
                continue;
            }
            let take = count.min(run_available);

            match &mut self.run {
                RleBooleanRun::Rle { value, remaining } => {
                    // Repeated run: fill output in one memset-style operation.
                    unsafe {
                        ptr::write_bytes(out, *value, take);
                    }
                    *remaining -= take;
                }
                RleBooleanRun::Bitpacked { data, byte_offset, bit_offset, remaining } => {
                    // Bitpacked run: expand packed bits into byte-per-bool output.
                    Self::decode_bitpacked_into(*data, byte_offset, bit_offset, out, take);
                    *remaining -= take;
                }
                RleBooleanRun::None => unreachable!(),
            }

            self.remaining_values -= take;
            count -= take;
            out = unsafe { out.add(take) };
        }
    }

    fn skip_values(&mut self, mut count: usize) {
        while count > 0 {
            if self.remaining_values == 0 {
                self.set_not_enough_values_error();
                return;
            }

            if !self.ensure_run() {
                return;
            }

            let run_available = self.run.remaining().min(self.remaining_values);
            if run_available == 0 {
                self.run = RleBooleanRun::None;
                continue;
            }
            let take = count.min(run_available);

            match &mut self.run {
                RleBooleanRun::Rle { remaining, .. } => {
                    // Repeated run skip is just counter arithmetic.
                    *remaining -= take;
                }
                RleBooleanRun::Bitpacked { byte_offset, bit_offset, remaining, .. } => {
                    // Bitpacked run skip advances bit/byte cursors.
                    Self::skip_bitpacked(byte_offset, bit_offset, take);
                    *remaining -= take;
                }
                RleBooleanRun::None => unreachable!(),
            }

            self.remaining_values -= take;
            count -= take;
        }
    }
}

impl Pushable for RleBooleanDecoder<'_> {
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

    fn push(&mut self) -> ParquetResult<()> {
        self.push_slice(1)?;
        Ok(())
    }

    fn push_slice(&mut self, count: usize) -> ParquetResult<()> {
        if count > 0 {
            let out = unsafe { self.buffers_ptr.add(self.buffers_offset) };
            self.decode_values_into(out, count);
            self.buffers_offset += count;
        }
        Ok(())
    }

    fn push_null(&mut self) -> ParquetResult<()> {
        unsafe {
            *self.buffers_ptr.add(self.buffers_offset) = self.null_value;
        }
        self.buffers_offset += 1;
        Ok(())
    }

    fn push_nulls(&mut self, count: usize) -> ParquetResult<()> {
        if count == 0 {
            return Ok(());
        }
        let out = unsafe { self.buffers_ptr.add(self.buffers_offset) };
        if self.null_value == 0 {
            unsafe {
                std::ptr::write_bytes(out, 0, count);
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

    fn skip(&mut self, count: usize) {
        if count > 0 {
            self.skip_values(count);
        }
    }

    fn result(&self) -> ParquetResult<()> {
        self.error.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::allocator::{AcVec, TestAllocatorState};
    use crate::parquet_read::column_sink::Pushable;
    use crate::parquet_read::decoders::rle::RleBooleanDecoder;
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

    fn encode_rle_bools(values: &[bool]) -> Vec<u8> {
        let mut payload = Vec::new();
        parquet2::encoding::hybrid_rle::encode_bool(
            &mut payload,
            values.iter().copied(),
            values.len(),
        )
        .unwrap();

        let mut data = Vec::with_capacity(payload.len() + 4);
        data.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        data.extend_from_slice(&payload);
        data
    }

    #[test]
    fn test_rle_boolean_decoder_push_slice() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_buffers(&allocator);

        let values = [
            true, false, true, true, false, false, true, false, true, true,
        ];
        let encoded = encode_rle_bools(&values);
        let result = {
            let mut decoder =
                RleBooleanDecoder::try_new(&encoded, values.len(), &mut buffers, 0).unwrap();
            decoder.reserve(values.len()).unwrap();
            decoder.push_slice(values.len()).unwrap();
            decoder.result()
        };

        let expected: Vec<u8> = values.iter().map(|&v| v as u8).collect();
        assert_eq!(&buffers.data_vec[..], expected.as_slice());
        assert!(result.is_ok());
    }

    #[test]
    fn test_rle_boolean_decoder_skip_and_nulls() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_buffers(&allocator);

        let values = [true, false, true, true, false, true];
        let encoded = encode_rle_bools(&values);
        let result = {
            let mut decoder =
                RleBooleanDecoder::try_new(&encoded, values.len(), &mut buffers, 9).unwrap();
            decoder.reserve(6).unwrap();
            decoder.skip(1);
            decoder.push_slice(3).unwrap();
            decoder.push_null().unwrap();
            decoder.push().unwrap();
            decoder.push_nulls(1).unwrap();
            decoder.result()
        };

        assert_eq!(&buffers.data_vec[..], &[0, 1, 1, 9, 0, 9]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_rle_boolean_decoder_reports_layout_error() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_buffers(&allocator);

        let values = [true, false, true];
        let encoded = encode_rle_bools(&values);
        let result = {
            let mut decoder = RleBooleanDecoder::try_new(&encoded, 3, &mut buffers, 0).unwrap();
            decoder.reserve(4).unwrap();
            decoder.push_slice(2).unwrap();
            decoder.push_slice(2).unwrap();
            decoder.result()
        };

        assert_eq!(&buffers.data_vec[..], &[1, 0, 1, 0]);
        assert!(result.is_err());
    }

    #[test]
    fn test_rle_boolean_decoder_rejects_short_buffer() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_buffers(&allocator);

        let err = RleBooleanDecoder::try_new(&[1, 2, 3], 3, &mut buffers, 0).err();
        assert!(err.is_some());
    }
}
