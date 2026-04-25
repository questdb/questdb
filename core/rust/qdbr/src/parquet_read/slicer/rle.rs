use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_read::decoders::{RepeatN, RleIterator, VarDictDecoder};
use crate::parquet_read::slicer::{ByteSink, DataPageSlicer};
use parquet2::encoding::bitpacked;
use parquet2::encoding::hybrid_rle::{Decoder, HybridEncoded};

struct SlicerInner<'a> {
    decoder: Option<Decoder<'a>>,
    data: RleIterator<'a>,
    sliced_row_count: usize,
}

impl<'a> SlicerInner<'a> {
    fn new(decoder: Option<Decoder<'a>>, iter: RleIterator<'a>, sliced_row_count: usize) -> Self {
        Self { decoder, data: iter, sliced_row_count }
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

    fn skip(&mut self, count: usize) -> ParquetResult<()> {
        let mut remaining = count;
        while remaining > 0 {
            let skipped = self.data.skip(remaining);
            remaining -= skipped;

            if remaining > 0 {
                self.decode()?;
            }
        }
        Ok(())
    }

    fn count(&self) -> usize {
        self.sliced_row_count
    }
}

pub struct RleDictionarySlicer<'a, T: VarDictDecoder> {
    dict: T,
    inner: SlicerInner<'a>,
}

impl<T: VarDictDecoder> DataPageSlicer for RleDictionarySlicer<'_, T> {
    // TODO(amunra): Clean this up -- non-idiomatic Rust code -- Should this just be a
    //               fn next(&mut self) -> Option<Result<&[u8], ParquetReadError>> ?
    fn next(&mut self) -> ParquetResult<&[u8]> {
        if let Some(idx) = self.inner.data.next() {
            if idx < self.dict.len() {
                Ok(self.dict.get_dict_value(idx))
            } else {
                Err(fmt_err!(
                    Layout,
                    "index {} is out of dict bounds {}",
                    idx,
                    self.dict.len()
                ))
            }
        } else {
            // This recursive is safe, it cannot go deeper than 1 level down.
            // After a successful call to `self.inner.decode()` there will be a value in `self.inner.data.next()`
            self.decode()?;
            self.next()
        }
    }

    fn next_into<S: ByteSink>(&mut self, dest: &mut S) -> ParquetResult<()> {
        if let Some(idx) = self.inner.data.next() {
            if idx < self.dict.len() {
                dest.extend_from_slice(self.dict.get_dict_value(idx))
            } else {
                Err(fmt_err!(
                    Layout,
                    "index {} is out of dict bounds {}",
                    idx,
                    self.dict.len()
                ))
            }
        } else {
            self.decode()?;
            self.next_into(dest)
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
        self.inner.skip(count)?;
        Ok(())
    }

    fn count(&self) -> usize {
        self.inner.count()
    }

    fn data_size(&self) -> usize {
        (self.inner.sliced_row_count as f32 * self.dict.avg_key_len()) as usize
    }
}

impl<'a, T: VarDictDecoder> RleDictionarySlicer<'a, T> {
    pub fn try_new(
        mut buffer: &'a [u8],
        dict: T,
        row_count: usize,
        sliced_row_count: usize,
    ) -> ParquetResult<Self> {
        let num_bits = buffer[0];
        if num_bits > 0 {
            buffer = &buffer[1..];
            // Do not decode the first run eagerly — see the matching
            // comment in ``RleDictVarcharSliceDecoder::try_new``. A
            // bitpacked header declaring zero groups (``[0x01]``),
            // which the writer emits for pages with
            // ``non_null_count == 0`` but a non-zero global
            // ``bits_per_key``, would make an eager
            // ``Decoder::next()`` return ``None`` and the call site
            // would surface "Unexpected end of rle iterator" even
            // when the def-level iterator never actually pulls a
            // value. ``next``/``skip`` already lazy-decode.
            let decoder = Decoder::new(buffer, num_bits as usize);
            Ok(Self {
                dict,
                inner: SlicerInner::new(
                    Some(decoder),
                    RleIterator::Rle(RepeatN::new(0, 0)),
                    sliced_row_count,
                ),
            })
        } else {
            Ok(Self {
                dict,
                inner: SlicerInner::new(
                    None,
                    RleIterator::Rle(RepeatN::new(0, row_count)),
                    sliced_row_count,
                ),
            })
        }
    }

    fn decode(&mut self) -> ParquetResult<()> {
        self.inner.decode()
    }
}
