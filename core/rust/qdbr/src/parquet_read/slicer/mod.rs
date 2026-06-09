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

/// Validates a delta-decoded byte length from a (possibly foreign) page. Lengths
/// are byte counts, so reject negative or out-of-i32-range values up front rather
/// than letting `as usize` wrap them into a huge slice bound that panics. The
/// remaining in-range value is still bounds-checked against the actual buffer at
/// use, which rejects an oversized-but-positive length.
#[inline]
fn checked_len(value: i64) -> ParquetResult<i32> {
    let len = i32::try_from(value).map_err(|_| fmt_err!(Layout, "delta length out of range"))?;
    if len < 0 {
        return Err(fmt_err!(Layout, "negative delta length"));
    }
    Ok(len)
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
        // Bounds-check the length index: an empty-buffer page (lengths is empty)
        // or a page whose definition levels over-claim non-nulls would otherwise
        // index out of bounds and panic, aborting the JVM on foreign input.
        let len = self
            .lengths
            .get(self.index)
            .copied()
            .ok_or_else(|| fmt_err!(Layout, "not enough length values to iterate"))?
            as usize;
        // Bounds-check the slice: a length that exceeds the remaining values
        // buffer (a corrupt/foreign page) must not index out of bounds and panic.
        let res = self
            .data
            .get(self.pos..self.pos + len)
            .ok_or_else(|| fmt_err!(Layout, "delta length exceeds values buffer"))?;
        self.pos += len;
        self.index += 1;
        Ok(res)
    }

    #[inline]
    fn next_into<S: ByteSink>(&mut self, dest: &mut S) -> ParquetResult<()> {
        let len = self
            .lengths
            .get(self.index)
            .copied()
            .ok_or_else(|| fmt_err!(Layout, "not enough length values to iterate"))?
            as usize;
        let res = self
            .data
            .get(self.pos..self.pos + len)
            .ok_or_else(|| fmt_err!(Layout, "delta length exceeds values buffer"))?;
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
        let skip_bytes: usize = self
            .lengths
            .get(self.index..self.index + count)
            .ok_or_else(|| fmt_err!(Layout, "not enough length values to skip"))?
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
        if data.is_empty() {
            // An all-null DELTA_LENGTH_BYTE_ARRAY page can arrive with an empty
            // values buffer (no delta header) from a foreign encoder via
            // read_parquet(). There are no lengths to decode; the page's
            // definition levels drive push_null, so next() is never called.
            // Constructing the vendored parquet2 delta decoder on an empty buffer
            // instead divides block_size/num_mini_blocks = 0/0 and panics, which
            // aborts the JVM across the JNI boundary, so short-circuit here. This
            // mirrors MiniblockIterator::try_new's empty-buffer branch.
            return Ok(Self {
                data,
                sliced_row_count,
                index: 0,
                lengths: Vec::new(),
                pos: 0,
            });
        }
        let mut decoder = delta_bitpacked::Decoder::try_new(data)?;
        let lengths: Vec<i32> = decoder
            .by_ref()
            .take(row_count)
            .map(|r| {
                let v = r.map_err(|_| fmt_err!(Layout, "not enough length values to iterate"))?;
                checked_len(v)
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
        let prefix_len = self
            .prefix
            .next()
            .ok_or_else(|| fmt_err!(Layout, "not enough prefix values to iterate"))?
            as usize;
        let suffix_len = self
            .suffix
            .next()
            .ok_or_else(|| fmt_err!(Layout, "not enough suffix values to iterate"))?
            as usize;
        // Bounds-check the suffix slice: an oversized suffix length (a corrupt or
        // foreign page) must not index out of bounds and panic.
        let data = self.data;
        let suffix = data
            .get(self.data_offset..self.data_offset + suffix_len)
            .ok_or_else(|| fmt_err!(Layout, "suffix length exceeds values buffer"))?;
        self.last_value.truncate(prefix_len);
        self.last_value.extend_from_slice(suffix);
        self.data_offset += suffix_len;
        Ok(self.last_value.as_slice())
    }

    fn next_into<S: ByteSink>(&mut self, dest: &mut S) -> ParquetResult<()> {
        let prefix_len = self
            .prefix
            .next()
            .ok_or_else(|| fmt_err!(Layout, "not enough prefix values to iterate"))?
            as usize;
        let suffix_len = self
            .suffix
            .next()
            .ok_or_else(|| fmt_err!(Layout, "not enough suffix values to iterate"))?
            as usize;
        let data = self.data;
        let suffix = data
            .get(self.data_offset..self.data_offset + suffix_len)
            .ok_or_else(|| fmt_err!(Layout, "suffix length exceeds values buffer"))?;
        self.last_value.truncate(prefix_len);
        self.last_value.extend_from_slice(suffix);
        self.data_offset += suffix_len;
        dest.extend_from_slice_safe(&self.last_value)
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
        if data.is_empty() {
            // All-null DELTA_BYTE_ARRAY page with an empty values buffer (no delta
            // header): there are no prefix/suffix lengths to decode, the page's
            // definition levels drive push_null, and next() is never called. Avoids
            // the vendored parquet2 delta decoder's 0/0 panic on an empty buffer,
            // mirroring MiniblockIterator::try_new's empty-buffer branch.
            return Ok(Self {
                prefix: Vec::new().into_iter(),
                suffix: Vec::new().into_iter(),
                data,
                data_offset: 0,
                sliced_row_count,
                last_value: vec![],
            });
        }
        let values = data;
        let mut decoder = delta_bitpacked::Decoder::try_new(values)?;
        let prefix: Vec<i32> = (&mut decoder)
            .take(row_count)
            .map(|r| {
                let v = r.map_err(|_| fmt_err!(Layout, "not enough prefix values to iterate"))?;
                checked_len(v)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut data_offset = decoder.consumed_bytes();
        let mut decoder = delta_bitpacked::Decoder::try_new(&values[decoder.consumed_bytes()..])?;
        let suffix = (&mut decoder)
            .map(|r| {
                let v = r.map_err(|_| fmt_err!(Layout, "not enough suffix values to iterate"))?;
                checked_len(v)
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
        // SAFETY: the check above guarantees self.pos + 4 <= self.data.len().
        let len =
            unsafe { ptr::read_unaligned(self.data.as_ptr().add(self.pos) as *const u32) } as usize;
        self.pos += size_of::<u32>();
        // Bounds-check the slice: an oversized length (a corrupt/foreign page)
        // must not index out of bounds and panic.
        let res = self
            .data
            .get(self.pos..self.pos + len)
            .ok_or_else(|| fmt_err!(Layout, "plain length exceeds values buffer"))?;
        self.pos += len;
        Ok(res)
    }

    #[inline]
    fn next_into<S: ByteSink>(&mut self, dest: &mut S) -> ParquetResult<()> {
        if self.pos + size_of::<u32>() > self.data.len() {
            return Err(fmt_err!(Layout, "not enough data to read length prefix"));
        }
        // SAFETY: the check above guarantees self.pos + 4 <= self.data.len().
        let len =
            unsafe { ptr::read_unaligned(self.data.as_ptr().add(self.pos) as *const u32) } as usize;
        self.pos += size_of::<u32>();
        let res = self
            .data
            .get(self.pos..self.pos + len)
            .ok_or_else(|| fmt_err!(Layout, "plain length exceeds values buffer"))?;
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
            if self.pos + size_of::<u32>() > self.data.len() {
                return Err(fmt_err!(Layout, "not enough data to read length prefix"));
            }
            // SAFETY: the check above guarantees self.pos + 4 <= self.data.len().
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
