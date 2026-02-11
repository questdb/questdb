pub mod dict_decoder;
pub mod rle;

#[cfg(test)]
mod tests;

use crate::allocator::{AcVec, AllocFailure};
use crate::parquet::error::{fmt_err, ParquetResult};
use parquet2::encoding::delta_bitpacked;

use std::mem::size_of;
use std::ptr;

/// Trait for types that can receive bytes (used by DataPageSlicer)
pub trait ByteSink {
    fn extend_from_slice(&mut self, data: &[u8]) -> ParquetResult<()>;
    fn extend_from_slice_safe(&mut self, data: &[u8]) -> ParquetResult<()>;
}

pub struct SliceSink<'a>(pub &'a mut [u8]);

impl ByteSink for SliceSink<'_> {
    #[inline]
    fn extend_from_slice(&mut self, data: &[u8]) -> ParquetResult<()> {
        self.0[..data.len()].copy_from_slice(data);
        Ok(())
    }

    #[inline]
    fn extend_from_slice_safe(&mut self, data: &[u8]) -> ParquetResult<()> {
        self.0[..data.len()].copy_from_slice(data);
        Ok(())
    }
}

pub trait Converter<const N: usize> {
    fn convert<S: ByteSink>(input: &[u8], output: &mut S) -> ParquetResult<()>;
}

impl ByteSink for AcVec<u8> {
    #[inline(always)]
    fn extend_from_slice(&mut self, data: &[u8]) -> ParquetResult<()> {
        unsafe {
            let len = self.len();
            ptr::copy_nonoverlapping(data.as_ptr(), self.as_mut_ptr().add(len), data.len());
            self.set_len(len + data.len());
        }
        Ok(())
    }

    #[inline]
    fn extend_from_slice_safe(&mut self, data: &[u8]) -> ParquetResult<()> {
        AcVec::extend_from_slice(self, data).map_err(|_| {
            fmt_err!(
                OutOfMemory(Some(AllocFailure::OutOfMemory {
                    requested_size: data.len()
                })),
                "AcVec capacity overflow"
            )
        })
    }
}

impl ByteSink for Vec<u8> {
    #[inline]
    fn extend_from_slice(&mut self, data: &[u8]) -> ParquetResult<()> {
        // SAFETY: Caller ensures capacity via reserve()
        unsafe {
            let len = self.len();
            ptr::copy_nonoverlapping(data.as_ptr(), self.as_mut_ptr().add(len), data.len());
            self.set_len(len + data.len());
        }
        Ok(())
    }

    #[inline]
    fn extend_from_slice_safe(&mut self, data: &[u8]) -> ParquetResult<()> {
        Vec::extend_from_slice(self, data);
        Ok(())
    }
}

pub trait DataPageSlicer {
    fn next(&mut self) -> &[u8];
    fn next_into<S: ByteSink>(&mut self, dest: &mut S) -> ParquetResult<()>;
    /// Only called by fixed-size column decoders.
    fn next_slice_into<S: ByteSink>(&mut self, count: usize, dest: &mut S) -> ParquetResult<()>;
    fn skip(&mut self, count: usize);
    fn count(&self) -> usize;
    fn data_size(&self) -> usize;
    fn result(&self) -> ParquetResult<()>;
}

pub struct DataPageFixedSlicer<'a, const N: usize> {
    data: &'a [u8],
    pos: usize,
    sliced_row_count: usize,
}

impl<const N: usize> DataPageSlicer for DataPageFixedSlicer<'_, N> {
    #[inline]
    fn next(&mut self) -> &[u8] {
        let res = &self.data[self.pos..self.pos + N];
        self.pos += N;
        res
    }

    #[inline]
    fn next_into<S: ByteSink>(&mut self, dest: &mut S) -> ParquetResult<()> {
        let res = &self.data[self.pos..self.pos + N];
        self.pos += N;
        dest.extend_from_slice(res)
    }

    #[inline]
    fn next_slice_into<S: ByteSink>(&mut self, count: usize, dest: &mut S) -> ParquetResult<()> {
        let len = N * count;
        let res = &self.data[self.pos..self.pos + len];
        self.pos += len;
        dest.extend_from_slice(res)
    }

    #[inline]
    fn skip(&mut self, count: usize) {
        self.pos += N * count;
    }

    fn count(&self) -> usize {
        self.sliced_row_count
    }

    fn data_size(&self) -> usize {
        self.sliced_row_count * N
    }

    fn result(&self) -> ParquetResult<()> {
        Ok(())
    }
}

impl<'a, const N: usize> DataPageFixedSlicer<'a, N> {
    pub fn new(data: &'a [u8], row_count: usize) -> Self {
        Self { data, pos: 0, sliced_row_count: row_count }
    }
}

pub struct DeltaLengthArraySlicer<'a> {
    data: &'a [u8],
    sliced_row_count: usize,
    index: usize,
    lengths: Vec<i32>,
    pos: usize,
}

impl DataPageSlicer for DeltaLengthArraySlicer<'_> {
    #[inline]
    fn next(&mut self) -> &[u8] {
        let len = self.lengths[self.index] as usize;
        let res = &self.data[self.pos..self.pos + len];
        self.pos += len;
        self.index += 1;
        res
    }

    #[inline]
    fn next_into<S: ByteSink>(&mut self, dest: &mut S) -> ParquetResult<()> {
        let len = self.lengths[self.index] as usize;
        let res = &self.data[self.pos..self.pos + len];
        self.pos += len;
        self.index += 1;
        dest.extend_from_slice(res)
    }

    #[inline]
    fn next_slice_into<S: ByteSink>(&mut self, count: usize, dest: &mut S) -> ParquetResult<()> {
        for _ in 0..count {
            self.next_into(dest)?;
        }
        Ok(())
    }

    #[inline]
    fn skip(&mut self, count: usize) {
        let skip_bytes: usize = self.lengths[self.index..self.index + count]
            .iter()
            .map(|&len| len as usize)
            .sum();
        self.pos += skip_bytes;
        self.index += count;
    }

    fn count(&self) -> usize {
        self.sliced_row_count
    }

    fn data_size(&self) -> usize {
        self.data.len()
    }

    fn result(&self) -> ParquetResult<()> {
        Ok(())
    }
}

impl<'a> DeltaLengthArraySlicer<'a> {
    pub fn try_new(
        data: &'a [u8],
        row_count: usize,
        sliced_row_count: usize,
    ) -> ParquetResult<Self> {
        let mut decoder = delta_bitpacked::Decoder::try_new(data)?;
        let lengths: Vec<_> = decoder
            .by_ref()
            .take(row_count)
            .map(|r| r.map(|v| v as i32).unwrap())
            .collect::<Vec<_>>();

        let data_offset = decoder.consumed_bytes();
        Ok(Self {
            data: &data[data_offset..],
            sliced_row_count,
            index: 0,
            lengths,
            pos: 0,
        })
    }
}

pub struct DeltaBytesArraySlicer<'a> {
    prefix: std::vec::IntoIter<i32>,
    suffix: std::vec::IntoIter<i32>,
    data: &'a [u8],
    data_offset: usize,
    sliced_row_count: usize,
    last_value: Vec<u8>,
    error: ParquetResult<()>,
}

impl<'a> DataPageSlicer for DeltaBytesArraySlicer<'a> {
    fn next(&mut self) -> &[u8] {
        if self.error.is_err() {
            return &[];
        }
        match self.prefix.next() {
            Some(prefix_len) => {
                let prefix_len = prefix_len as usize;
                match self.suffix.next() {
                    Some(suffix_len) => {
                        let suffix_len = suffix_len as usize;
                        self.last_value.truncate(prefix_len);
                        self.last_value.extend_from_slice(
                            &self.data[self.data_offset..self.data_offset + suffix_len],
                        );
                        self.data_offset += suffix_len;
                        // SAFETY: we extend lifetime to 'a because last_value lives as long as self
                        unsafe {
                            std::mem::transmute::<&[u8], &'a [u8]>(self.last_value.as_slice())
                        }
                    }
                    None => {
                        self.error = Err(fmt_err!(Layout, "not enough suffix values to iterate"));
                        &[]
                    }
                }
            }
            None => {
                self.error = Err(fmt_err!(Layout, "not enough prefix values to iterate"));
                &[]
            }
        }
    }

    fn next_into<S: ByteSink>(&mut self, dest: &mut S) -> ParquetResult<()> {
        if self.error.is_err() {
            return Ok(());
        }
        match self.prefix.next() {
            Some(prefix_len) => {
                let prefix_len = prefix_len as usize;
                match self.suffix.next() {
                    Some(suffix_len) => {
                        let suffix_len = suffix_len as usize;
                        self.last_value.truncate(prefix_len);
                        self.last_value.extend_from_slice(
                            &self.data[self.data_offset..self.data_offset + suffix_len],
                        );
                        self.data_offset += suffix_len;
                        dest.extend_from_slice_safe(&self.last_value)
                    }
                    None => {
                        self.error = Err(fmt_err!(Layout, "not enough suffix values to iterate"));
                        Ok(())
                    }
                }
            }
            None => {
                self.error = Err(fmt_err!(Layout, "not enough prefix values to iterate"));
                Ok(())
            }
        }
    }

    #[inline]
    fn next_slice_into<S: ByteSink>(&mut self, count: usize, dest: &mut S) -> ParquetResult<()> {
        for _ in 0..count {
            self.next_into(dest)?;
        }
        Ok(())
    }

    fn skip(&mut self, count: usize) {
        for _ in 0..count {
            self.next();
        }
    }

    fn count(&self) -> usize {
        self.sliced_row_count
    }

    fn data_size(&self) -> usize {
        self.data.len()
    }

    fn result(&self) -> ParquetResult<()> {
        self.error.clone()
    }
}

impl<'a> DeltaBytesArraySlicer<'a> {
    pub fn try_new(
        data: &'a [u8],
        row_count: usize,
        sliced_row_count: usize,
    ) -> ParquetResult<Self> {
        let values = data;
        let mut decoder = delta_bitpacked::Decoder::try_new(values)?;
        let prefix = (&mut decoder)
            .take(row_count)
            .map(|r| r.map(|v| v as i32).unwrap())
            .collect::<Vec<_>>();

        let mut data_offset = decoder.consumed_bytes();
        let mut decoder = delta_bitpacked::Decoder::try_new(&values[decoder.consumed_bytes()..])?;
        let suffix = (&mut decoder)
            .map(|r| r.map(|v| v as i32).unwrap())
            .collect::<Vec<_>>();
        data_offset += decoder.consumed_bytes();

        Ok(Self {
            prefix: prefix.into_iter(),
            suffix: suffix.into_iter(),
            data: values,
            data_offset,
            sliced_row_count,
            last_value: vec![],
            error: Ok(()),
        })
    }
}

pub struct PlainVarSlicer<'a> {
    data: &'a [u8],
    pos: usize,
    sliced_row_count: usize,
}

impl DataPageSlicer for PlainVarSlicer<'_> {
    #[inline]
    fn next(&mut self) -> &[u8] {
        let len =
            unsafe { ptr::read_unaligned(self.data.as_ptr().add(self.pos) as *const u32) } as usize;
        self.pos += size_of::<u32>();
        let res = &self.data[self.pos..self.pos + len];
        self.pos += len;
        res
    }

    #[inline]
    fn next_into<S: ByteSink>(&mut self, dest: &mut S) -> ParquetResult<()> {
        let len =
            unsafe { ptr::read_unaligned(self.data.as_ptr().add(self.pos) as *const u32) } as usize;
        self.pos += size_of::<u32>();
        let res = &self.data[self.pos..self.pos + len];
        self.pos += len;
        dest.extend_from_slice(res)
    }

    #[inline]
    fn next_slice_into<S: ByteSink>(&mut self, count: usize, dest: &mut S) -> ParquetResult<()> {
        for _ in 0..count {
            self.next_into(dest)?;
        }
        Ok(())
    }

    #[inline]
    fn skip(&mut self, count: usize) {
        for _ in 0..count {
            let len =
                unsafe { ptr::read_unaligned(self.data.as_ptr().add(self.pos) as *const u32) };
            self.pos += len as usize + size_of::<u32>();
        }
    }

    #[inline]
    fn count(&self) -> usize {
        self.sliced_row_count
    }

    #[inline]
    fn data_size(&self) -> usize {
        self.data.len()
    }

    fn result(&self) -> ParquetResult<()> {
        Ok(())
    }
}

impl<'a> PlainVarSlicer<'a> {
    pub fn new(data: &'a [u8], sliced_row_count: usize) -> Self {
        Self { data, pos: 0, sliced_row_count }
    }
}

/// Lookup table: maps each byte to 8 expanded bits (LSB-first order).
const BITMAP_LUT: [[u8; 8]; 256] = {
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

pub struct BooleanBitmapSlicer<'a> {
    data: &'a [u8],
    bit_offset: usize,
    total_bits: usize,
    sliced_row_count: usize,
    error: ParquetResult<()>,
}

const BOOL_TRUE: [u8; 1] = [1];
const BOOL_FALSE: [u8; 1] = [0];

impl DataPageSlicer for BooleanBitmapSlicer<'_> {
    #[inline]
    fn next(&mut self) -> &[u8] {
        if self.bit_offset >= self.total_bits {
            self.error = Err(fmt_err!(Layout, "not enough bitmap values to iterate"));
            return &BOOL_FALSE;
        }
        let byte_idx = self.bit_offset >> 3;
        let bit_idx = self.bit_offset & 7;
        self.bit_offset += 1;
        if (self.data[byte_idx] >> bit_idx) & 1 == 1 {
            &BOOL_TRUE
        } else {
            &BOOL_FALSE
        }
    }

    #[inline]
    fn next_into<S: ByteSink>(&mut self, dest: &mut S) -> ParquetResult<()> {
        if self.bit_offset >= self.total_bits {
            self.error = Err(fmt_err!(Layout, "not enough bitmap values to iterate"));
            return dest.extend_from_slice(&BOOL_FALSE);
        }
        let byte_idx = self.bit_offset >> 3;
        let bit_idx = self.bit_offset & 7;
        self.bit_offset += 1;
        if (self.data[byte_idx] >> bit_idx) & 1 == 1 {
            dest.extend_from_slice(&BOOL_TRUE)
        } else {
            dest.extend_from_slice(&BOOL_FALSE)
        }
    }

    fn next_slice_into<S: ByteSink>(&mut self, count: usize, dest: &mut S) -> ParquetResult<()> {
        if count == 0 {
            return Ok(());
        }

        if self.bit_offset + count > self.total_bits {
            self.error = Err(fmt_err!(Layout, "not enough bitmap values to iterate"));
            return Ok(());
        }

        let mut remaining = count;

        // Handle unaligned start bits
        let bit_idx = self.bit_offset & 7;
        if bit_idx != 0 {
            let bits_in_first_byte = (8 - bit_idx).min(remaining);
            let byte = self.data[self.bit_offset >> 3] >> bit_idx;
            let mut buf = [0u8; 8];
            for i in 0..bits_in_first_byte {
                buf[i] = (byte >> i) & 1;
            }
            dest.extend_from_slice(&buf[..bits_in_first_byte])?;
            self.bit_offset += bits_in_first_byte;
            remaining -= bits_in_first_byte;
        }

        // Process full bytes using lookup table (8 bits -> 8 bytes at a time)
        let start_byte = self.bit_offset >> 3;
        let full_bytes = remaining >> 3;
        for i in 0..full_bytes {
            dest.extend_from_slice(&BITMAP_LUT[self.data[start_byte + i] as usize])?;
        }
        self.bit_offset += full_bytes << 3;
        remaining -= full_bytes << 3;

        // Handle remaining bits
        if remaining > 0 {
            let byte = self.data[self.bit_offset >> 3];
            let expanded = &BITMAP_LUT[byte as usize];
            dest.extend_from_slice(&expanded[..remaining])?;
            self.bit_offset += remaining;
        }

        Ok(())
    }

    fn skip(&mut self, count: usize) {
        self.bit_offset += count;
    }

    fn count(&self) -> usize {
        self.sliced_row_count
    }

    fn data_size(&self) -> usize {
        self.sliced_row_count
    }

    fn result(&self) -> ParquetResult<()> {
        self.error.clone()
    }
}

impl<'a> BooleanBitmapSlicer<'a> {
    pub fn new(data: &'a [u8], row_count: usize, sliced_row_count: usize) -> Self {
        Self {
            data,
            bit_offset: 0,
            total_bits: row_count,
            sliced_row_count,
            error: Ok(()),
        }
    }
}

/// Slicer for RLE/Bit-Packed Hybrid encoded boolean values.
///
/// The Parquet RLE encoding for booleans uses bit_width=1 with the standard
/// RLE/Bit-Packed Hybrid format, prefixed by a 4-byte little-endian length.
pub struct BooleanRleSlicer<'a> {
    decoder: parquet2::encoding::hybrid_rle::HybridRleDecoder<'a>,
    sliced_row_count: usize,
    error: ParquetResult<()>,
}

impl DataPageSlicer for BooleanRleSlicer<'_> {
    #[inline]
    fn next(&mut self) -> &[u8] {
        match self.decoder.next() {
            Some(Ok(val)) => {
                if val != 0 {
                    &BOOL_TRUE
                } else {
                    &BOOL_FALSE
                }
            }
            Some(Err(e)) => {
                self.error = Err(e.into());
                &BOOL_FALSE
            }
            None => {
                self.error = Err(fmt_err!(Layout, "not enough RLE boolean values"));
                &BOOL_FALSE
            }
        }
    }

    #[inline]
    fn next_into<S: ByteSink>(&mut self, dest: &mut S) -> ParquetResult<()> {
        let val = self.next();
        dest.extend_from_slice(val)
    }

    fn next_slice_into<S: ByteSink>(&mut self, count: usize, dest: &mut S) -> ParquetResult<()> {
        for _ in 0..count {
            let val = self.next();
            dest.extend_from_slice(val)?;
        }
        Ok(())
    }

    fn skip(&mut self, count: usize) {
        for _ in 0..count {
            let _ = self.decoder.next();
        }
    }

    fn count(&self) -> usize {
        self.sliced_row_count
    }

    fn data_size(&self) -> usize {
        self.sliced_row_count
    }

    fn result(&self) -> ParquetResult<()> {
        self.error.clone()
    }
}

impl<'a> BooleanRleSlicer<'a> {
    pub fn try_new(
        data: &'a [u8],
        row_count: usize,
        sliced_row_count: usize,
    ) -> ParquetResult<Self> {
        // RLE boolean values are prefixed with a 4-byte LE length
        if data.len() < 4 {
            return Err(fmt_err!(
                Layout,
                "boolean RLE buffer too short: {} bytes",
                data.len()
            ));
        }
        let _length = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
        let rle_data = &data[4..];
        let decoder =
            parquet2::encoding::hybrid_rle::HybridRleDecoder::try_new(rle_data, 1, row_count)?;
        Ok(Self {
            decoder,
            sliced_row_count,
            error: Ok(()),
        })
    }
}
