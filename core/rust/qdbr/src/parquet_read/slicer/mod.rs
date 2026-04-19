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

pub trait Converter<const N: usize> {
    fn convert<S: ByteSink>(input: &[u8], output: &mut S) -> ParquetResult<()>;
}

impl ByteSink for AcVec<u8> {
    #[inline(always)]
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
    fn next(&mut self) -> ParquetResult<&[u8]>;
    fn next_into<S: ByteSink>(&mut self, dest: &mut S) -> ParquetResult<()>;
    /// Only called by fixed-size column decoders.
    fn next_slice_into<S: ByteSink>(&mut self, count: usize, dest: &mut S) -> ParquetResult<()>;
    /// Returns a contiguous chunk of `count` fixed-size values if available.
    /// Implementations that cannot provide borrowed contiguous slices return `None`.
    fn next_raw_slice(&mut self, _count: usize) -> Option<&[u8]> {
        None
    }
    fn skip(&mut self, count: usize) -> ParquetResult<()>;
    fn count(&self) -> usize;
    fn data_size(&self) -> usize;
}

pub struct DataPageFixedSlicer<'a, const N: usize> {
    data: &'a [u8],
    pos: usize,
    sliced_row_count: usize,
}

impl<const N: usize> DataPageSlicer for DataPageFixedSlicer<'_, N> {
    #[inline]
    fn next(&mut self) -> ParquetResult<&[u8]> {
        let res = &self.data[self.pos..self.pos + N];
        self.pos += N;
        Ok(res)
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
    fn next_raw_slice(&mut self, count: usize) -> Option<&[u8]> {
        let len = N * count;
        let res = &self.data[self.pos..self.pos + len];
        self.pos += len;
        Some(res)
    }

    #[inline]
    fn skip(&mut self, count: usize) -> ParquetResult<()> {
        self.pos += N * count;
        Ok(())
    }

    fn count(&self) -> usize {
        self.sliced_row_count
    }

    fn data_size(&self) -> usize {
        self.sliced_row_count * N
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
    buffer: [u8; N],
}

impl<const N: usize> DataPageSlicer for DeltaBinaryPackedSlicer<'_, N> {
    #[inline]
    fn next(&mut self) -> ParquetResult<&[u8]> {
        let res = self.decoder.next();
        match res {
            Some(val) => match val {
                Ok(val) => {
                    let bytes = val.to_le_bytes();
                    self.buffer[..N].copy_from_slice(&bytes[..N]);
                    Ok(&self.buffer)
                }
                Err(_) => {
                    // TODO(amunra): Clean-up, this is _not_ a layout error!
                    Err(fmt_err!(Layout, "not enough values to iterate"))
                }
            },
            None => {
                // TODO(amunra): Clean-up, this is _not_ a layout error!
                Err(fmt_err!(Layout, "not enough values to iterate"))
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
            Some(Err(_)) | None => Err(fmt_err!(Layout, "not enough values to iterate")),
        }
    }

    fn next_slice_into<S: ByteSink>(&mut self, count: usize, dest: &mut S) -> ParquetResult<()> {
        for _ in 0..count {
            self.next_into::<S>(dest)?;
        }
        Ok(())
    }

    fn skip(&mut self, count: usize) -> ParquetResult<()> {
        for _ in 0..count {
            self.decoder.next();
        }
        Ok(())
    }

    fn count(&self) -> usize {
        self.sliced_row_count
    }

    fn data_size(&self) -> usize {
        self.sliced_row_count * N
    }
}

impl<'a, const N: usize> DeltaBinaryPackedSlicer<'a, N> {
    pub fn try_new(data: &'a [u8], row_count: usize) -> ParquetResult<Self> {
        let decoder = delta_bitpacked::Decoder::try_new(data)?;
        Ok(Self {
            decoder,
            sliced_row_count: row_count,
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
    fn next(&mut self) -> ParquetResult<&[u8]> {
        let len = self.lengths[self.index] as usize;
        let res = &self.data[self.pos..self.pos + len];
        self.pos += len;
        self.index += 1;
        Ok(res)
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
    fn skip(&mut self, count: usize) -> ParquetResult<()> {
        let skip_bytes: usize = self.lengths[self.index..self.index + count]
            .iter()
            .map(|&len| len as usize)
            .sum();
        self.pos += skip_bytes;
        self.index += count;
        Ok(())
    }

    fn count(&self) -> usize {
        self.sliced_row_count
    }

    fn data_size(&self) -> usize {
        self.data.len()
    }
}

impl<'a> DeltaLengthArraySlicer<'a> {
    pub fn try_new(
        data: &'a [u8],
        row_count: usize,
        sliced_row_count: usize,
    ) -> ParquetResult<Self> {
        let mut decoder = delta_bitpacked::Decoder::try_new(data)?;
        let lengths: Vec<i32> = decoder
            .by_ref()
            .take(row_count)
            .map(|r| {
                r.map(|v| v as i32)
                    .map_err(|_| fmt_err!(Layout, "not enough length values to iterate"))
            })
            .collect::<Result<Vec<_>, _>>()?;

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
}

impl<'a> DataPageSlicer for DeltaBytesArraySlicer<'a> {
    fn next(&mut self) -> ParquetResult<&[u8]> {
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
                        Ok(self.last_value.as_slice())
                    }
                    None => Err(fmt_err!(Layout, "not enough suffix values to iterate")),
                }
            }
            None => Err(fmt_err!(Layout, "not enough prefix values to iterate")),
        }
    }

    fn next_into<S: ByteSink>(&mut self, dest: &mut S) -> ParquetResult<()> {
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
                    None => Err(fmt_err!(Layout, "not enough suffix values to iterate")),
                }
            }
            None => Err(fmt_err!(Layout, "not enough prefix values to iterate")),
        }
    }

    #[inline]
    fn next_slice_into<S: ByteSink>(&mut self, count: usize, dest: &mut S) -> ParquetResult<()> {
        for _ in 0..count {
            self.next_into(dest)?;
        }
        Ok(())
    }

    fn skip(&mut self, count: usize) -> ParquetResult<()> {
        for _ in 0..count {
            self.next()?;
        }
        Ok(())
    }

    fn count(&self) -> usize {
        self.sliced_row_count
    }

    fn data_size(&self) -> usize {
        self.data.len()
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
        let prefix: Vec<i32> = (&mut decoder)
            .take(row_count)
            .map(|r| {
                r.map(|v| v as i32)
                    .map_err(|_| fmt_err!(Layout, "not enough prefix values to iterate"))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut data_offset = decoder.consumed_bytes();
        let mut decoder = delta_bitpacked::Decoder::try_new(&values[decoder.consumed_bytes()..])?;
        let suffix = (&mut decoder)
            .map(|r| {
                r.map(|v| v as i32)
                    .map_err(|_| fmt_err!(Layout, "not enough suffix values to iterate"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        data_offset += decoder.consumed_bytes();

        Ok(Self {
            prefix: prefix.into_iter(),
            suffix: suffix.into_iter(),
            data: values,
            data_offset,
            sliced_row_count,
            last_value: vec![],
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
    fn next(&mut self) -> ParquetResult<&[u8]> {
        if self.pos + size_of::<u32>() > self.data.len() {
            return Err(fmt_err!(Layout, "not enough data to read length prefix"));
        }
        let len =
            unsafe { ptr::read_unaligned(self.data.as_ptr().add(self.pos) as *const u32) } as usize;
        self.pos += size_of::<u32>();
        let res = &self.data[self.pos..self.pos + len];
        self.pos += len;
        Ok(res)
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
    fn skip(&mut self, count: usize) -> ParquetResult<()> {
        for _ in 0..count {
            let len =
                unsafe { ptr::read_unaligned(self.data.as_ptr().add(self.pos) as *const u32) };
            self.pos += len as usize + size_of::<u32>();
        }
        Ok(())
    }

    #[inline]
    fn count(&self) -> usize {
        self.sliced_row_count
    }

    #[inline]
    fn data_size(&self) -> usize {
        self.data.len()
    }
}

impl<'a> PlainVarSlicer<'a> {
    pub fn new(data: &'a [u8], sliced_row_count: usize) -> Self {
        Self { data, pos: 0, sliced_row_count }
    }
}
