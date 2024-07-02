use crate::parquet_read::slicer::RleIterator::Rle;
use crate::parquet_write::ParquetResult;
use parquet2::encoding::hybrid_rle::HybridEncoded;
use parquet2::encoding::{bitpacked, delta_bitpacked, hybrid_rle};
use parquet2::error::Error;
use parquet2::page::DictPage;
use std::marker::PhantomData;
use std::mem::size_of;
use std::ptr;

pub trait DataPageSlicer {
    fn next(&mut self) -> &[u8];
    fn next_slice(&mut self, count: usize) -> Option<&[u8]>;
    fn skip(&mut self, count: usize);
    fn count(&self) -> usize;
    fn data_size(&self) -> usize;
}

pub struct DataPageFixedSlicer<'a, const N: usize> {
    data: &'a [u8],
    pos: usize,
    row_count: usize,
}

impl<const N: usize> DataPageSlicer for DataPageFixedSlicer<'_, N> {
    fn next(&mut self) -> &[u8] {
        let res = &self.data[self.pos..self.pos + N];
        self.pos += N;
        res
    }

    fn next_slice(&mut self, count: usize) -> Option<&[u8]> {
        let res = &self.data[self.pos..self.pos + N * count];
        self.pos += N * count;
        Some(res)
    }

    fn skip(&mut self, count: usize) {
        self.pos += N * count;
    }

    fn count(&self) -> usize {
        self.row_count
    }

    fn data_size(&self) -> usize {
        self.row_count * N
    }
}

impl<'a, const N: usize> DataPageFixedSlicer<'a, N> {
    pub fn new(data: &'a [u8], row_count: usize) -> Self {
        Self { data, pos: 0, row_count }
    }
}

pub struct DeltaLengthArraySlicer<'a> {
    data: &'a [u8],
    row_count: usize,
    index: usize,
    lengths: Vec<i64>,
    pos: usize,
}

impl DataPageSlicer for DeltaLengthArraySlicer<'_> {
    fn next(&mut self) -> &[u8] {
        let len = self.lengths[self.index] as usize;
        let res = &self.data[self.pos..self.pos + len];
        self.pos += len;
        self.index += 1;
        res
    }

    fn next_slice(&mut self, _count: usize) -> Option<&[u8]> {
        None
    }

    fn skip(&mut self, count: usize) {
        for _ in 0..count {
            self.pos += self.lengths[self.index] as usize;
            self.index += 1;
        }
    }

    fn count(&self) -> usize {
        self.row_count
    }

    fn data_size(&self) -> usize {
        self.data.len()
    }
}

impl<'a> DeltaLengthArraySlicer<'a> {
    pub fn try_new(data: &'a [u8], row_count: usize) -> ParquetResult<Self> {
        let mut decoder = delta_bitpacked::Decoder::try_new(data)?;

        let lengths: Vec<i64> = decoder.by_ref().collect::<Result<Vec<i64>, Error>>()?;

        let data_offset = decoder.consumed_bytes();
        Ok(Self {
            data: &data[data_offset..],
            row_count,
            index: 0,
            lengths,
            pos: 0,
        })
    }
}

pub struct RleDictionarySlicer<'a, 'b, I>
where
    I: Iterator<Item = u32>,
{
    data: I,
    row_count: usize,
    dict: Vec<&'b [u8]>,
    error: ParquetResult<()>,
    avg_key_len: f32,
    _data: PhantomData<&'a u8>,
}

impl<I: Iterator<Item = u32>> DataPageSlicer for RleDictionarySlicer<'_, '_, I> {
    fn next(&mut self) -> &[u8] {
        if self.error.is_ok() {
            let idx = self.data.next();
            if let Some(idx) = idx {
                return self.dict[idx as usize];
            } else {
                return &[];
            }
        }
        &[]
    }

    fn next_slice(&mut self, _count: usize) -> Option<&[u8]> {
        // Not supported
        None
    }

    fn skip(&mut self, count: usize) {
        for _ in 0..count {
            self.data.next();
        }
    }

    fn count(&self) -> usize {
        self.row_count
    }

    fn data_size(&self) -> usize {
        (self.row_count as f32 * self.avg_key_len) as usize
    }
}

type RepeatIterator = std::iter::Take<std::iter::Repeat<u32>>;
type BitbackIterator<'a> = bitpacked::Decoder<'a, u32>;

enum RleIterator<'a> {
    Bitpacked(BitbackIterator<'a>),
    Rle(RepeatIterator),
}

pub enum DictionarySlicer<'a, 'b> {
    Bitpacked(RleDictionarySlicer<'a, 'b, BitbackIterator<'a>>),
    Rle(RleDictionarySlicer<'a, 'b, RepeatIterator>),
}

pub fn decode_rle<'a, 'b>(
    data: &'a [u8],
    dict_page: &'b DictPage,
    row_count: usize,
) -> ParquetResult<DictionarySlicer<'a, 'b>> {
    let mut dict_values: Vec<&[u8]> = Vec::with_capacity(dict_page.num_values);
    let mut offset = 0usize;
    let dict_data = &dict_page.buffer;

    let mut total_key_len = 0;
    for i in 0..dict_page.num_values {
        if offset + size_of::<u32>() > dict_data.len() {
            return Err(Error::OutOfSpec(format!(
                "dictionary data page is too short to read value length {}",
                i
            )));
        }

        let str_len =
            unsafe { ptr::read_unaligned(dict_data.as_ptr().add(offset) as *const u32) } as usize;
        offset += size_of::<u32>();

        if offset + str_len > dict_data.len() {
            return Err(Error::OutOfSpec(format!(
                "dictionary data page is too short to read value {}",
                i
            )));
        }

        let str_slice = &dict_data[offset..offset + str_len];
        debug_assert!(std::str::from_utf8(str_slice).is_ok());

        dict_values.push(str_slice);
        offset += str_len;
        total_key_len += str_len;
    }

    let bits_per_entry = data[0] as usize;
    let rle_decoder = decode_rle_v2(&data[1..], bits_per_entry)?;

    match rle_decoder {
        RleIterator::Bitpacked(bitpacked_iter) => {
            let bitpacked_slicer = RleDictionarySlicer {
                data: bitpacked_iter,
                row_count,
                dict: dict_values,
                error: Ok(()),
                avg_key_len: total_key_len as f32 / dict_page.num_values as f32,
                _data: PhantomData,
            };
            return Ok(DictionarySlicer::Bitpacked(bitpacked_slicer));
        }
        Rle(rle_iter) => {
            let rle_slicer = RleDictionarySlicer {
                data: rle_iter,
                row_count,
                dict: dict_values,
                error: Ok(()),
                avg_key_len: total_key_len as f32 / dict_page.num_values as f32,
                _data: PhantomData,
            };
            return Ok(DictionarySlicer::Rle(rle_slicer));
        }
    }
}

fn decode_rle_v2(buffer: &[u8], num_bits: usize) -> ParquetResult<RleIterator> {
    if num_bits > 0 {
        let mut decoder = hybrid_rle::Decoder::new(buffer, num_bits);
        if let Some(run) = decoder.next()  {
            let encoded = run?;
            if let HybridEncoded::Bitpacked(values) = encoded {
                let count = values.len() * 8 / num_bits;
                let inner_decoder = bitpacked::Decoder::<u32>::try_new(values, num_bits, count)?;
                return Ok(RleIterator::Bitpacked(inner_decoder));
            } else if let HybridEncoded::Rle(pack, repeat) = encoded {
                let value = unsafe { ptr::read_unaligned(pack.as_ptr() as *const u32) };
                let iterator = std::iter::repeat(value).take(repeat);
                return Ok(RleIterator::Rle(iterator));
            } else {
                return Err(Error::FeatureNotSupported(format!(
                    "encoding not supported: {:?}",
                    encoded
                )));
            }
        }
        Err(Error::OutOfSpec(
            "Hybrid rle not contain any bitpack or rle runs".to_string(),
        ))
    } else {
        let decoder = bitpacked::Decoder::<u32>::try_new(&[], 1, 0)?;
        Ok(RleIterator::Bitpacked(decoder))
    }
}
