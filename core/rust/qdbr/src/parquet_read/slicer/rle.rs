use crate::parquet_read::slicer::dict_decoder::DictDecoder;
use crate::parquet_read::slicer::DataPageSlicer;
use crate::parquet_write::ParquetResult;
use parquet2::encoding::bitpacked;
use parquet2::encoding::hybrid_rle::{Decoder, HybridEncoded};
use parquet2::error::Error;

type RepeatIterator = std::iter::Take<std::iter::Repeat<u32>>;
type BitbackIterator<'a> = bitpacked::Decoder<'a, u32>;

enum RleIterator<'a> {
    Bitpacked(BitbackIterator<'a>),
    Rle(RepeatIterator),
}

pub struct RleDictionarySlicer<'a, 'b, T: DictDecoder> {
    decoder: Option<Decoder<'a>>,
    data: RleIterator<'a>,
    row_count: usize,
    dict: T,
    error: ParquetResult<()>,
    error_value: &'b [u8],
}

impl<T: DictDecoder> DataPageSlicer for RleDictionarySlicer<'_, '_, T> {
    fn next(&mut self) -> &[u8] {
        if self.error.is_ok() {
            let idx = match self.data {
                RleIterator::Bitpacked(ref mut iter) => iter.next(),
                RleIterator::Rle(ref mut iter) => iter.next(),
            };

            if let Some(idx) = idx {
                if idx < self.dict.len() {
                    return self.dict.get_dict_value(idx);
                } else {
                    self.error = Err(Error::OutOfSpec(format!(
                        "index {} is out of dict bounds {}",
                        idx,
                        self.dict.len()
                    )));
                }
            } else {
                return match self.decode() {
                    Ok(()) => self.next(),
                    Err(err) => {
                        self.error = Err(err);
                        self.error_value
                    }
                };
            }
        }
        self.error_value
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
        self.error.clone()
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
                error: Ok(()),
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
                error: Ok(()),
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
                    return Err(Error::FeatureNotSupported(format!(
                        "encoding not supported: {:?}",
                        encoded
                    )));
                }
            }
        }
        Err(Error::OutOfSpec(
            "Unexpected end of rle iterator".to_string(),
        ))
    }
}
