pub mod rle;

#[cfg(test)]
mod tests;

use crate::allocator::{AcVec, AllocFailure};
use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_read::decoders::MiniblockIterator;
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
        // Bounds-check the slice: a foreign or corrupt page whose element count
        // (driven by the definition/repetition levels) exceeds the values buffer
        // must not index out of bounds and panic, aborting the JVM across JNI.
        let res = self
            .data
            .get(self.pos..self.pos + N)
            .ok_or_else(|| fmt_err!(Layout, "fixed value exceeds values buffer"))?;
        self.pos += N;
        Ok(res)
    }

    #[inline]
    fn next_into<S: ByteSink>(&mut self, dest: &mut S) -> ParquetResult<()> {
        let res = self
            .data
            .get(self.pos..self.pos + N)
            .ok_or_else(|| fmt_err!(Layout, "fixed value exceeds values buffer"))?;
        self.pos += N;
        dest.extend_from_slice(res)
    }

    #[inline]
    fn next_slice_into<S: ByteSink>(&mut self, count: usize, dest: &mut S) -> ParquetResult<()> {
        let len = N * count;
        let res = self
            .data
            .get(self.pos..self.pos + len)
            .ok_or_else(|| fmt_err!(Layout, "fixed value slice exceeds values buffer"))?;
        self.pos += len;
        dest.extend_from_slice(res)
    }

    #[inline]
    fn next_raw_slice(&mut self, count: usize) -> Option<&[u8]> {
        let len = N * count;
        // Returns None (the trait's "cannot provide a borrowed slice" signal)
        // rather than indexing out of bounds and panicking on a foreign/corrupt
        // page.
        let res = self.data.get(self.pos..self.pos + len)?;
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

/// Collects up to `limit` delta-encoded lengths into a `Vec<i32>`, reserving the
/// buffer fallibly so an oversized or amplified foreign page surfaces a
/// recoverable `OutOfMemory` error instead of aborting the JVM.
///
/// The DELTA length/prefix/suffix streams decode up front into a `Vec`. `limit`
/// is the slicer's row count -- the page's `num_values`, an attacker-controlled
/// header field up to `i32::MAX`. A bit-width-0 delta miniblock expands to
/// `values_per_mini_block` entries while consuming no buffer bytes, so a tiny
/// page can declare billions of values; `take(limit)` caps the iteration at the
/// page's row count and `try_reserve_exact(limit)` makes the up-front sizing
/// fallible. A plain `collect` reserves the same capacity infallibly and aborts
/// the process when the allocator refuses the resulting multi-gigabyte request.
/// `checked_len` validates each value's range, not the count, so this reservation
/// is the only guard against the allocation. Classified `OutOfMemory` (not
/// `Layout`) so a parquet merge under `ApplyWal2TableJob` backs off and retries
/// on transient memory pressure rather than suspending the table.
fn collect_checked_lengths<E>(
    iter: impl Iterator<Item = Result<i64, E>>,
    limit: usize,
    what: &'static str,
) -> ParquetResult<Vec<i32>> {
    let mut lengths: Vec<i32> = Vec::new();
    lengths.try_reserve_exact(limit).map_err(|_| {
        fmt_err!(
            OutOfMemory(None),
            "cannot allocate {limit} delta {what} values"
        )
    })?;
    for value in iter.take(limit) {
        let value = value.map_err(|_| fmt_err!(Layout, "not enough {what} values to iterate"))?;
        lengths.push(checked_len(value)?);
    }
    Ok(lengths)
}

/// Returns the byte length of a DELTA_BINARY_PACKED length stream, i.e. the
/// offset at which the concatenated value bytes that follow it begin.
///
/// The slicers must skip the WHOLE length stream to reach the data region.
/// Deriving the offset from `delta_bitpacked::Decoder::consumed_bytes()` after a
/// `take(row_count)` is wrong for a partial range read (`row_count` < the page's
/// `num_values`): `take` stops before entering the later delta blocks, so
/// `consumed_bytes()` omits their bytes and the data slice starts inside the
/// length stream, shifting every value (silent corruption) or tripping a
/// "length exceeds values buffer" error on a valid page.
///
/// `MiniblockIterator::get_end_pointer` instead steps over the whole block/
/// miniblock structure, so the result does not depend on how many values a
/// caller later reads. The walk is bounded by the buffer size -- every block
/// consumes at least its header bytes -- not by the attacker-controlled declared
/// value count, so it cannot be amplified into an unbounded loop by a foreign
/// page. This is the same primitive the VarcharSlice decoder uses to locate its
/// data region.
fn delta_stream_byte_len(data: &[u8]) -> ParquetResult<usize> {
    let (iter, _): (MiniblockIterator<i32>, _) = MiniblockIterator::try_new(data)?;
    let end = iter.get_end_pointer()?;
    Ok(end as usize - data.as_ptr() as usize)
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
        let decoder = delta_bitpacked::Decoder::try_new(data)?;
        let lengths = collect_checked_lengths(decoder, row_count, "length")?;

        // Skip the entire length stream, not just the delta blocks the truncated
        // take(row_count) in collect_checked_lengths entered. See
        // delta_stream_byte_len: consumed_bytes() here would under-count on a
        // partial range read and start the data slice inside the length stream.
        let data_offset = delta_stream_byte_len(data)?;
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

        // A DELTA_BYTE_ARRAY page lays out [prefix lengths][suffix lengths][bytes].
        // Locate both stream boundaries by walking the full block structure
        // (delta_stream_byte_len) so a partial range read still finds the value
        // bytes; reading consumed_bytes() after the truncated take(row_count)
        // below under-counts the blocks it never entered, and using it to start
        // the suffix stream would compound the error. The walk also fixes the
        // suffix stream's start offset, not just the final data offset.
        let prefix_len = delta_stream_byte_len(values)?;
        let suffix_buf = &values[prefix_len..];
        let data_offset = prefix_len + delta_stream_byte_len(suffix_buf)?;

        // Materialize only the first row_count prefix/suffix lengths for the
        // reads; collect_checked_lengths bounds the allocation by row_count. The
        // suffix collect is bounded by row_count as well -- previously it
        // collected every value the delta stream declared (its own
        // attacker-controlled total_count), independent of the page's row count.
        let prefix_decoder = delta_bitpacked::Decoder::try_new(values)?;
        let prefix = collect_checked_lengths(prefix_decoder, row_count, "prefix")?;
        let suffix_decoder = delta_bitpacked::Decoder::try_new(suffix_buf)?;
        let suffix = collect_checked_lengths(suffix_decoder, row_count, "suffix")?;

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
