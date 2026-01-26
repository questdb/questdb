pub mod dict_decoder;
pub mod dict_slicer;
pub mod rle;

#[cfg(test)]
mod tests;

use crate::allocator::{AcVec, AcVecSetLen, AllocFailure};
use crate::parquet::error::{fmt_err, ParquetResult};
use parquet2::encoding::delta_bitpacked;
use parquet2::encoding::hybrid_rle::BitmapIter;
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
    fn convert<S: ByteSink>(input: &[u8], output: &mut S);
}

pub struct DaysToMillisConverter;

impl Converter<8> for DaysToMillisConverter {
    #[inline]
    fn convert<S: ByteSink>(input: &[u8], output: &mut S) {
        let days_since_epoch = unsafe { ptr::read_unaligned(input.as_ptr() as *const i32) };
        let date = days_since_epoch as i64 * 24 * 60 * 60 * 1000;
        let _ = output.extend_from_slice(&date.to_le_bytes());
    }
}

impl ByteSink for AcVec<u8> {
    #[inline]
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

pub struct DeltaBinaryPackedSlicer<'a, const N: usize> {
    decoder: delta_bitpacked::Decoder<'a>,
    sliced_row_count: usize,
    error: ParquetResult<()>,
    error_value: [u8; N],
    buffer: [u8; N],
}

impl<const N: usize> DataPageSlicer for DeltaBinaryPackedSlicer<'_, N> {
    #[inline]
    fn next(&mut self) -> &[u8] {
        let res = self.decoder.next();
        match res {
            Some(val) => match val {
                Ok(val) => {
                    let bytes = val.to_le_bytes();
                    self.buffer[..N].copy_from_slice(&bytes[..N]);
                    &self.buffer
                }
                Err(_) => {
                    // TODO(amunra): Clean-up, this is _not_ a layout error!
                    self.error = Err(fmt_err!(Layout, "not enough values to iterate"));
                    &self.error_value
                }
            },
            None => {
                // TODO(amunra): Clean-up, this is _not_ a layout error!
                self.error = Err(fmt_err!(Layout, "not enough values to iterate"));
                &self.error_value
            }
        }
    }

    #[inline]
    fn next_into<S: ByteSink>(&mut self, dest: &mut S) -> ParquetResult<()> {
        match self.decoder.next() {
            Some(Ok(val)) => {
                let bytes = val.to_le_bytes();
                dest.extend_from_slice(&bytes[..N])
            }
            Some(Err(_)) => {
                self.error = Err(fmt_err!(Layout, "not enough values to iterate"));
                dest.extend_from_slice(&self.error_value)
            }
            None => {
                self.error = Err(fmt_err!(Layout, "not enough values to iterate"));
                dest.extend_from_slice(&self.error_value)
            }
        }
    }

    fn next_slice_into<S: ByteSink>(&mut self, count: usize, dest: &mut S) -> ParquetResult<()> {
        for _ in 0..count {
            match self.decoder.next() {
                Some(Ok(val)) => {
                    let bytes = val.to_le_bytes();
                    dest.extend_from_slice(&bytes[..N])?;
                }
                Some(Err(_)) | None => {
                    self.error = Err(fmt_err!(Layout, "not enough values to iterate"));
                    dest.extend_from_slice(&self.error_value)?;
                }
            }
        }
        Ok(())
    }

    fn skip(&mut self, count: usize) {
        for _ in 0..count {
            self.decoder.next();
        }
    }

    fn count(&self) -> usize {
        self.sliced_row_count
    }

    fn data_size(&self) -> usize {
        self.sliced_row_count * N
    }

    fn result(&self) -> ParquetResult<()> {
        self.error.clone()
    }
}

impl<'a, const N: usize> DeltaBinaryPackedSlicer<'a, N> {
    pub fn try_new(data: &'a [u8], row_count: usize) -> ParquetResult<Self> {
        let decoder = delta_bitpacked::Decoder::try_new(data)?;
        Ok(Self {
            decoder,
            sliced_row_count: row_count,
            error: Ok(()),
            error_value: [0; N],
            buffer: [0; N],
        })
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

    fn skip(&mut self, count: usize) {
        for _ in 0..count {
            self.pos += self.lengths[self.index] as usize;
            self.index += 1;
        }
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

pub struct BooleanBitmapSlicer<'a> {
    bitmap_iter: BitmapIter<'a>,
    sliced_row_count: usize,
    error: ParquetResult<()>,
}

const BOOL_TRUE: [u8; 1] = [1];
const BOOL_FALSE: [u8; 1] = [0];

impl DataPageSlicer for BooleanBitmapSlicer<'_> {
    #[inline]
    fn next(&mut self) -> &[u8] {
        if let Some(val) = self.bitmap_iter.next() {
            if val {
                return &BOOL_TRUE;
            }
            return &BOOL_FALSE;
        }
        self.error = Err(fmt_err!(Layout, "not enough bitmap values to iterate"));
        &BOOL_FALSE
    }

    #[inline]
    fn next_into<S: ByteSink>(&mut self, dest: &mut S) -> ParquetResult<()> {
        if let Some(val) = self.bitmap_iter.next() {
            if val {
                return dest.extend_from_slice(&BOOL_TRUE);
            }
            return dest.extend_from_slice(&BOOL_FALSE);
        }
        self.error = Err(fmt_err!(Layout, "not enough bitmap values to iterate"));
        dest.extend_from_slice(&BOOL_FALSE)
    }

    fn next_slice_into<S: ByteSink>(&mut self, count: usize, dest: &mut S) -> ParquetResult<()> {
        if count == 0 {
            return Ok(());
        }

        const BATCH: usize = 1024;
        let mut buf = [0u8; BATCH];
        let mut remaining = count;

        while remaining > 0 {
            let n = remaining.min(BATCH);
            for slot in buf.iter_mut().take(n) {
                *slot = if let Some(val) = self.bitmap_iter.next() {
                    val as u8
                } else {
                    self.error = Err(fmt_err!(Layout, "not enough bitmap values to iterate"));
                    0
                };
            }
            dest.extend_from_slice(&buf[..n])?;
            remaining -= n;
        }
        Ok(())
    }

    fn skip(&mut self, count: usize) {
        for _ in 0..count {
            self.bitmap_iter.next();
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

impl<'a> BooleanBitmapSlicer<'a> {
    pub fn new(data: &'a [u8], row_count: usize, sliced_row_count: usize) -> Self {
        let bitmap_iter = BitmapIter::new(data, 0, row_count);
        Self { bitmap_iter, sliced_row_count, error: Ok(()) }
    }
}

pub struct ValueConvertSlicer<const N: usize, T: DataPageSlicer, C: Converter<N>> {
    inner_slicer: T,
    buffer: [u8; N],
    _converter: std::marker::PhantomData<C>,
}

impl<const N: usize, T: DataPageSlicer, C: Converter<N>> DataPageSlicer
    for ValueConvertSlicer<N, T, C>
{
    #[inline]
    fn next(&mut self) -> &[u8] {
        let slice = self.inner_slicer.next();
        C::convert(slice, &mut SliceSink(&mut self.buffer));
        &self.buffer
    }

    #[inline]
    fn next_into<S: ByteSink>(&mut self, dest: &mut S) -> ParquetResult<()> {
        let slice = self.inner_slicer.next();
        C::convert(slice, dest);
        Ok(())
    }

    #[inline]
    fn next_slice_into<S: ByteSink>(&mut self, count: usize, dest: &mut S) -> ParquetResult<()> {
        for _ in 0..count {
            self.next_into(dest)?;
        }
        Ok(())
    }

    fn skip(&mut self, count: usize) {
        self.inner_slicer.skip(count);
    }

    fn count(&self) -> usize {
        self.inner_slicer.count()
    }

    fn data_size(&self) -> usize {
        self.inner_slicer.count() * N
    }

    fn result(&self) -> ParquetResult<()> {
        self.inner_slicer.result()
    }
}

impl<const N: usize, T: DataPageSlicer, C: Converter<N>> ValueConvertSlicer<N, T, C> {
    pub fn new(inner_slicer: T) -> Self {
        Self {
            inner_slicer,
            buffer: [0; N],
            _converter: std::marker::PhantomData,
        }
    }
}
