use crate::parquet::error::{fmt_err, ParquetError, ParquetResult};
use crate::parquet_read::slicer::dict_decoder::DictDecoder;
use crate::parquet_read::slicer::DataPageSlicer;
use parquet2::encoding::bitpacked;
use parquet2::encoding::hybrid_rle::{Decoder, HybridEncoded};

type RepeatIterator = std::iter::Take<std::iter::Repeat<u32>>;
type BitbackIterator<'a> = bitpacked::Decoder<'a, u32>;

enum RleIterator<'a> {
    Bitpacked(BitbackIterator<'a>),
    Rle(RepeatIterator),
}

impl RleIterator<'_> {
    pub fn next(&mut self) -> Option<u32> {
        match self {
            RleIterator::Bitpacked(iter) => iter.next(),
            RleIterator::Rle(iter) => iter.next(),
        }
    }
}

pub struct RleDictionarySlicer<'a, 'b, T: DictDecoder> {
    decoder: Option<Decoder<'a>>,
    data: RleIterator<'a>,
    row_count: usize,
    dict: T,
    error: Option<ParquetError>,

    // TODO(amunra): Clean this up -- non-idiomatic Rust code.
    //               Use the type system instead of magic values.
    error_value: &'b [u8],
}

impl<T: DictDecoder> DataPageSlicer for RleDictionarySlicer<'_, '_, T> {
    // TODO(amunra): Clean this up -- non-idiomatic Rust code -- Should this just be a
    //               fn next(&mut self) -> Option<Result<&[u8], ParquetReadError>> ?
    fn next(&mut self) -> &[u8] {
        if self.error.is_some() {
            return self.error_value;
        }

        if let Some(idx) = self.data.next() {
            if idx < self.dict.len() {
                self.dict.get_dict_value(idx)
            } else {
                self.error = Some(fmt_err!(
                    Layout,
                    "index {} is out of dict bounds {}",
                    idx,
                    self.dict.len()
                ));
                self.error_value
            }
        } else {
            match self.decode() {
                Ok(()) => self.next(),
                Err(err) => {
                    self.error = Some(err);
                    self.error_value
                }
            }
        }
    }

    fn next_slice(&mut self, _count: usize) -> Option<&[u8]> {
        // Not supported
        None
    }

    fn skip(&mut self, count: usize) {
        for _ in 0..count {
            self.next();
        }
    }

    fn count(&self) -> usize {
        self.row_count
    }

    fn data_size(&self) -> usize {
        (self.row_count as f32 * self.dict.avg_key_len()) as usize
    }

    fn result(&self) -> ParquetResult<()> {
        match &self.error {
            Some(err) => Err(err.clone()),
            None => Ok(()),
        }
    }
}

impl<'a, 'b, T: DictDecoder> RleDictionarySlicer<'a, 'b, T> {
    pub fn try_new(
        mut buffer: &'a [u8],
        dict: T,
        row_count: usize,
        error_value: &'b [u8],
    ) -> ParquetResult<Self> {
        let num_bits = buffer[0];
        if num_bits > 0 {
            buffer = &buffer[1..];
            let decoder = Decoder::new(buffer, num_bits as usize);
            let mut res = Self {
                decoder: Some(decoder),
                data: RleIterator::Rle(std::iter::repeat(0).take(0)),
                row_count,
                dict,
                error: None,
                error_value,
            };
            res.decode()?;
            Ok(res)
        } else {
            Ok(Self {
                decoder: None,
                data: RleIterator::Rle(std::iter::repeat(0).take(row_count)),
                row_count,
                dict,
                error: None,
                error_value,
            })
        }
    }

    fn decode(&mut self) -> ParquetResult<()> {
        if let Some(ref mut decoder) = self.decoder {
            if let Some(run) = decoder.next() {
                let encoded = run?;
                if let HybridEncoded::Bitpacked(values) = encoded {
                    let count = values.len() * 8 / decoder.num_bits();
                    let inner_decoder =
                        bitpacked::Decoder::<u32>::try_new(values, decoder.num_bits(), count)?;
                    self.data = RleIterator::Bitpacked(inner_decoder);
                    return Ok(());
                } else if let HybridEncoded::Rle(pack, repeat) = encoded {
                    let mut bytes = [0u8; std::mem::size_of::<u32>()];
                    pack.iter().zip(bytes.iter_mut()).for_each(|(src, dst)| {
                        *dst = *src;
                    });
                    let value = u32::from_le_bytes(bytes);
                    let iterator = std::iter::repeat(value).take(repeat);
                    self.data = RleIterator::Rle(iterator);
                    return Ok(());
                } else {
                    return Err(fmt_err!(Unsupported, "encoding not supported: {encoded:?}"));
                }
            }
        }
        // TODO(amunra): Not a layout error. This is just an iterator pattern gone wrong, needs cleaning up.
        Err(fmt_err!(Layout, "Unexpected end of rle iterator"))
    }
}

/// A decoder for a symbol column where the local dictionary keys,
/// i.e. the numeric keys in the parquet data page, match the global keys in the `.o` file
/// of the global `SymbolMapReader`.
/// This is an optimisation for specially-encoded columns that skips mapping the parquet
/// symbol keys to the global keys.
pub struct RleLocalIsGlobalSymbolDecoder<'a, 'b> {
    decoder: Option<Decoder<'a>>,
    data: RleIterator<'a>,
    next_value: [u8; 4],
    sliced_row_count: usize,
    error: Option<ParquetError>,

    // TODO(amunra): Clean this up -- non-idiomatic Rust code.
    //               Use the type system instead of magic values.
    error_value: &'b [u8],
}

impl DataPageSlicer for RleLocalIsGlobalSymbolDecoder<'_, '_> {
    // TODO(amunra): Deduplicate this code.
    fn next(&mut self) -> &[u8] {
        if self.error.is_some() {
            return self.error_value;
        }

        if let Some(idx) = self.data.next() {
            self.next_value.copy_from_slice(&idx.to_le_bytes());
            &self.next_value
        } else {
            match self.decode() {
                Ok(()) => self.next(),
                Err(err) => {
                    self.error = Some(err);
                    self.error_value
                }
            }
        }
    }

    fn next_slice(&mut self, _count: usize) -> Option<&[u8]> {
        // Not supported
        None
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
        self.sliced_row_count * std::mem::size_of::<u32>()
    }

    fn result(&self) -> ParquetResult<()> {
        match &self.error {
            Some(err) => Err(err.clone()),
            None => Ok(()),
        }
    }
}

impl<'a, 'b> RleLocalIsGlobalSymbolDecoder<'a, 'b> {
    pub fn try_new(
        mut buffer: &'a [u8],
        row_count: usize,
        sliced_row_count: usize,
        error_value: &'b [u8],
    ) -> ParquetResult<Self> {
        // TODO(amunra): Deduplicate this code.
        let num_bits = buffer[0];
        if num_bits > 0 {
            buffer = &buffer[1..];
            let decoder = Decoder::new(buffer, num_bits as usize);
            let mut res = Self {
                decoder: Some(decoder),
                data: RleIterator::Rle(std::iter::repeat(0).take(0)),
                next_value: [0; 4],
                sliced_row_count,
                error: None,
                error_value,
            };
            res.decode()?;
            Ok(res)
        } else {
            Ok(Self {
                decoder: None,
                data: RleIterator::Rle(std::iter::repeat(0).take(row_count)),
                next_value: [0; 4],
                sliced_row_count,
                error: None,
                error_value,
            })
        }
    }

    fn decode(&mut self) -> ParquetResult<()> {
        // TODO(amunra): Deduplicate this code.
        let iter_err = || Err(fmt_err!(Layout, "Unexpected end of rle iterator"));

        let Some(ref mut decoder) = self.decoder else {
            return iter_err();
        };

        let Some(run) = decoder.next() else {
            return iter_err();
        };

        match run? {
            HybridEncoded::Bitpacked(values) => {
                let count = values.len() * 8 / decoder.num_bits();
                let inner_decoder =
                    bitpacked::Decoder::<u32>::try_new(values, decoder.num_bits(), count)?;
                self.data = RleIterator::Bitpacked(inner_decoder);
                Ok(())
            }
            HybridEncoded::Rle(pack, repeat) => {
                let mut bytes = [0u8; std::mem::size_of::<u32>()];
                pack.iter().zip(bytes.iter_mut()).for_each(|(src, dst)| {
                    *dst = *src;
                });
                let value = u32::from_le_bytes(bytes);
                let iterator = std::iter::repeat(value).take(repeat);
                self.data = RleIterator::Rle(iterator);
                Ok(())
            }
        }
    }
}
