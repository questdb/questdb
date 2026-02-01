use crate::parquet::error::{fmt_err, ParquetError, ParquetResult};
use crate::parquet_read::slicer::dict_decoder::DictDecoder;
use crate::parquet_read::slicer::{ByteSink, DataPageSlicer};
use parquet2::encoding::bitpacked;
use parquet2::encoding::hybrid_rle::{Decoder, HybridEncoded};
use std::mem::size_of;

/// Custom repeat iterator that supports efficient skipping.
struct RepeatN {
    value: u32,
    remaining: usize,
}

impl RepeatN {
    #[inline]
    fn new(value: u32, count: usize) -> Self {
        Self { value, remaining: count }
    }

    #[inline]
    fn next(&mut self) -> Option<u32> {
        if self.remaining > 0 {
            self.remaining -= 1;
            Some(self.value)
        } else {
            None
        }
    }

    #[inline]
    fn skip(&mut self, n: usize) -> usize {
        let skipped = n.min(self.remaining);
        self.remaining -= skipped;
        skipped
    }
}

type BitpackedIterator<'a> = bitpacked::Decoder<'a, u32>;

enum RleIterator<'a> {
    Bitpacked(BitpackedIterator<'a>),
    Rle(RepeatN),
}

impl RleIterator<'_> {
    #[inline(always)]
    pub fn next(&mut self) -> Option<u32> {
        match self {
            RleIterator::Bitpacked(iter) => iter.next(),
            RleIterator::Rle(iter) => iter.next(),
        }
    }

    #[inline]
    pub fn skip(&mut self, n: usize) -> usize {
        match self {
            RleIterator::Bitpacked(iter) => iter.advance(n),
            RleIterator::Rle(iter) => iter.skip(n),
        }
    }
}

struct SlicerInner<'a, 'b> {
    decoder: Option<Decoder<'a>>,
    data: RleIterator<'a>,
    sliced_row_count: usize,
    error: Option<ParquetError>,

    // TODO(amunra): Clean this up -- non-idiomatic Rust code.
    //               Use the type system instead of magic values.
    error_value: &'b [u8],
}

impl<'a, 'b> SlicerInner<'a, 'b> {
    fn new(
        decoder: Option<Decoder<'a>>,
        iter: RleIterator<'a>,
        sliced_row_count: usize,
        error_value: &'b [u8],
    ) -> Self {
        Self {
            decoder,
            data: iter,
            sliced_row_count,
            error: None,
            error_value,
        }
    }

    fn decode(&mut self) -> ParquetResult<()> {
        let iter_err = || Err(fmt_err!(Layout, "Unexpected end of rle iterator"));

        let Some(ref mut decoder) = self.decoder else {
            return iter_err();
        };

        let Some(run) = decoder.next() else {
            return iter_err();
        };

        let num_bits = decoder.num_bits();
        match run? {
            HybridEncoded::Bitpacked(values) => {
                let count = values.len() * 8 / num_bits;
                let inner_decoder = bitpacked::Decoder::<u32>::try_new(values, num_bits, count)?;
                self.data = RleIterator::Bitpacked(inner_decoder)
            }
            HybridEncoded::Rle(pack, repeat) => {
                let mut bytes = [0u8; std::mem::size_of::<u32>()];
                pack.iter().zip(bytes.iter_mut()).for_each(|(src, dst)| {
                    *dst = *src;
                });
                let value = u32::from_le_bytes(bytes);
                let iterator = RepeatN::new(value, repeat);
                self.data = RleIterator::Rle(iterator);
            }
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn next_slice(&mut self, _count: usize) -> Option<&[u8]> {
        // Not supported
        None
    }

    fn skip(&mut self, count: usize) {
        if self.error.is_some() {
            return;
        }

        let mut remaining = count;
        while remaining > 0 {
            let skipped = self.data.skip(remaining);
            remaining -= skipped;

            if remaining > 0 {
                match self.decode() {
                    Ok(()) => {}
                    Err(err) => {
                        self.error = Some(err);
                        return;
                    }
                }
            }
        }
    }

    fn count(&self) -> usize {
        self.sliced_row_count
    }

    fn result(&self) -> ParquetResult<()> {
        match &self.error {
            Some(err) => Err(err.clone()),
            None => Ok(()),
        }
    }
}

pub struct RleDictionarySlicer<'a, 'b, T: DictDecoder> {
    dict: T,
    inner: SlicerInner<'a, 'b>,
}

impl<T: DictDecoder> DataPageSlicer for RleDictionarySlicer<'_, '_, T> {
    // TODO(amunra): Clean this up -- non-idiomatic Rust code -- Should this just be a
    //               fn next(&mut self) -> Option<Result<&[u8], ParquetReadError>> ?
    fn next(&mut self) -> &[u8] {
        if self.inner.error.is_some() {
            return self.inner.error_value;
        }

        if let Some(idx) = self.inner.data.next() {
            if idx < self.dict.len() {
                self.dict.get_dict_value(idx)
            } else {
                self.inner.error = Some(fmt_err!(
                    Layout,
                    "index {} is out of dict bounds {}",
                    idx,
                    self.dict.len()
                ));
                self.inner.error_value
            }
        } else {
            // This recursive is safe, it cannot go deeper than 1 level down.
            // After a successful call to `self.inner.decode()` there will be a value in `self.inner.data.next()`
            match self.decode() {
                Ok(()) => self.next(),
                Err(err) => {
                    self.inner.error = Some(err);
                    self.inner.error_value
                }
            }
        }
    }

    fn next_into<S: ByteSink>(&mut self, dest: &mut S) -> ParquetResult<()> {
        if self.inner.error.is_some() {
            return dest.extend_from_slice(self.inner.error_value);
        }

        if let Some(idx) = self.inner.data.next() {
            if idx < self.dict.len() {
                dest.extend_from_slice(self.dict.get_dict_value(idx))
            } else {
                self.inner.error = Some(fmt_err!(
                    Layout,
                    "index {} is out of dict bounds {}",
                    idx,
                    self.dict.len()
                ));
                dest.extend_from_slice(self.inner.error_value)
            }
        } else {
            match self.decode() {
                Ok(()) => self.next_into(dest),
                Err(err) => {
                    self.inner.error = Some(err);
                    dest.extend_from_slice(self.inner.error_value)
                }
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
        self.inner.skip(count);
    }

    fn count(&self) -> usize {
        self.inner.count()
    }

    fn data_size(&self) -> usize {
        (self.inner.sliced_row_count as f32 * self.dict.avg_key_len()) as usize
    }

    fn result(&self) -> ParquetResult<()> {
        self.inner.result()
    }
}

impl<'a, 'b, T: DictDecoder> RleDictionarySlicer<'a, 'b, T> {
    pub fn try_new(
        mut buffer: &'a [u8],
        dict: T,
        row_count: usize,
        sliced_row_count: usize,
        error_value: &'b [u8],
    ) -> ParquetResult<Self> {
        let num_bits = buffer[0];
        if num_bits > 0 {
            buffer = &buffer[1..];
            let decoder = Decoder::new(buffer, num_bits as usize);
            let mut res = Self {
                dict,
                inner: SlicerInner::new(
                    Some(decoder),
                    RleIterator::Rle(RepeatN::new(0, 0)),
                    sliced_row_count,
                    error_value,
                ),
            };
            res.decode()?;
            Ok(res)
        } else {
            Ok(Self {
                dict,
                inner: SlicerInner::new(
                    None,
                    RleIterator::Rle(RepeatN::new(0, row_count)),
                    sliced_row_count,
                    error_value,
                ),
            })
        }
    }

    fn decode(&mut self) -> ParquetResult<()> {
        self.inner.decode()
    }
}

/// A decoder for a symbol column where the local dictionary keys,
/// i.e. the numeric keys in the parquet data page, match the global keys in the `.o` file
/// of the global `SymbolMapReader`.
/// This is an optimisation for specially-encoded columns that skips mapping the parquet
/// symbol keys to the global keys.
pub struct RleLocalIsGlobalSymbolDecoder<'a, 'b> {
    next_value: [u8; 4],
    inner: SlicerInner<'a, 'b>,
}

impl DataPageSlicer for RleLocalIsGlobalSymbolDecoder<'_, '_> {
    fn next(&mut self) -> &[u8] {
        if self.inner.error.is_some() {
            return self.inner.error_value;
        }

        if let Some(idx) = self.inner.data.next() {
            self.next_value.copy_from_slice(&idx.to_le_bytes());
            &self.next_value
        } else {
            match self.decode() {
                Ok(()) => self.next(),
                Err(err) => {
                    self.inner.error = Some(err);
                    self.inner.error_value
                }
            }
        }
    }

    fn next_into<S: ByteSink>(&mut self, dest: &mut S) -> ParquetResult<()> {
        if self.inner.error.is_some() {
            return dest.extend_from_slice(self.inner.error_value);
        }

        if let Some(idx) = self.inner.data.next() {
            dest.extend_from_slice(&idx.to_le_bytes())
        } else {
            match self.decode() {
                Ok(()) => self.next_into(dest),
                Err(err) => {
                    self.inner.error = Some(err);
                    dest.extend_from_slice(self.inner.error_value)
                }
            }
        }
    }

    #[cfg(target_endian = "little")]
    fn next_slice_into<S: ByteSink>(&mut self, count: usize, dest: &mut S) -> ParquetResult<()> {
        const BATCH_SIZE: usize = 256; // 10% performance improvement
        let mut buffer = [0u32; BATCH_SIZE];
        let mut remaining = count;

        while remaining > 0 {
            if self.inner.error.is_some() {
                for _ in 0..remaining {
                    dest.extend_from_slice(self.inner.error_value)?;
                }
                return Ok(());
            }

            let batch = remaining.min(BATCH_SIZE);
            let mut written = 0;

            while written < batch {
                if let Some(idx) = self.inner.data.next() {
                    buffer[written] = idx;
                    written += 1;
                } else {
                    match self.decode() {
                        Ok(()) => {}
                        Err(err) => {
                            self.inner.error = Some(err);
                            break;
                        }
                    }
                }
            }

            if written > 0 {
                let bytes = unsafe {
                    std::slice::from_raw_parts(
                        buffer.as_ptr() as *const u8,
                        written * size_of::<u32>(),
                    )
                };
                dest.extend_from_slice(bytes)?;
                remaining -= written;
            }
        }
        Ok(())
    }

    #[cfg(target_endian = "big")]
    fn next_slice_into<S: ByteSink>(&mut self, count: usize, dest: &mut S) -> ParquetResult<()> {
        for _ in 0..count {
            self.next_into(dest)?;
        }
        Ok(())
    }

    fn skip(&mut self, count: usize) {
        self.inner.skip(count);
    }

    fn count(&self) -> usize {
        self.inner.count()
    }

    fn data_size(&self) -> usize {
        self.inner.sliced_row_count * size_of::<u32>()
    }

    fn result(&self) -> ParquetResult<()> {
        self.inner.result()
    }
}

impl<'a, 'b> RleLocalIsGlobalSymbolDecoder<'a, 'b> {
    pub fn try_new(
        mut buffer: &'a [u8],
        row_count: usize,
        sliced_row_count: usize,
        error_value: &'b [u8],
    ) -> ParquetResult<Self> {
        let num_bits = buffer[0];
        if num_bits > 0 {
            buffer = &buffer[1..];
            let decoder = Decoder::new(buffer, num_bits as usize);
            let mut res = Self {
                next_value: [0; 4],
                inner: SlicerInner::new(
                    Some(decoder),
                    RleIterator::Rle(RepeatN::new(0, 0)),
                    sliced_row_count,
                    error_value,
                ),
            };
            res.decode()?;
            Ok(res)
        } else {
            Ok(Self {
                next_value: [0; 4],
                inner: SlicerInner::new(
                    None,
                    RleIterator::Rle(RepeatN::new(0, row_count)),
                    sliced_row_count,
                    error_value,
                ),
            })
        }
    }

    fn decode(&mut self) -> ParquetResult<()> {
        self.inner.decode()
    }
}
