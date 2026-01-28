use std::{cmp, io, mem, slice};

use crate::parquet::error::ParquetResult;
use crate::parquet_write::file::WriteOptions;
use parquet2::compression::CompressionOptions;
use parquet2::encoding::hybrid_rle::{encode_bool, encode_u32};
use parquet2::encoding::Encoding;
use parquet2::metadata::Descriptor;
use parquet2::page::{DataPage, DataPageHeader, DataPageHeaderV1, DataPageHeaderV2};
use parquet2::schema::types::{PhysicalType, PrimitiveType};
use parquet2::statistics::{serialize_statistics, BinaryStatistics, ParquetStatistics, Statistics};
use parquet2::types::NativeType;
use parquet2::write::Version;

#[derive(Debug)]
pub struct MaxMin<T> {
    pub max: Option<T>,
    pub min: Option<T>,
}

impl<T: Copy + NativeType> MaxMin<T> {
    pub fn new() -> Self {
        MaxMin { max: None, min: None }
    }
    pub fn update(&mut self, x: T) {
        self.max = Some(if let Some(max) = self.max {
            cmp::max_by(max, x, |x, y| x.ord(y))
        } else {
            x
        });
        self.min = Some(if let Some(min) = self.min {
            cmp::min_by(min, x, |x, y| x.ord(y))
        } else {
            x
        });
    }
}

pub struct BinaryMaxMinStats {
    primitive_type: PrimitiveType,
    max_value: Option<Vec<u8>>,
    min_value: Option<Vec<u8>>,
}

const SIZEOF_I64: usize = mem::size_of::<i64>();

impl BinaryMaxMinStats {
    pub fn new(primitive_type: &PrimitiveType) -> Self {
        Self {
            primitive_type: primitive_type.clone(),
            max_value: None,
            min_value: None,
        }
    }

    pub fn update(&mut self, value: &[u8]) {
        let val = if is_binary_column_type(&self.primitive_type) {
            &value[..value.len().min(SIZEOF_I64 + 1)]
        } else {
            value
        };
        match &mut self.max_value {
            None => {
                self.max_value = Some(val.to_vec());
            }
            Some(max) => {
                if val > max.as_slice() {
                    *max = val.to_vec();
                }
            }
        }
        match &mut self.min_value {
            None => {
                self.min_value = Some(val.to_vec());
            }
            Some(min) => {
                if val < min.as_slice() {
                    *min = val.to_vec();
                }
            }
        }
    }

    pub fn into_parquet_stats(self, null_count: usize) -> ParquetStatistics {
        let max_value = if is_binary_column_type(&self.primitive_type) {
            self.max_value.map(|max_value| {
                if max_value.len() <= SIZEOF_I64 {
                    max_value
                } else {
                    binary_upper_bound(max_value)
                }
            })
        } else {
            self.max_value
        };

        let stats = &BinaryStatistics {
            primitive_type: self.primitive_type,
            null_count: Some(null_count as i64),
            distinct_count: None,
            max_value,
            min_value: self.min_value,
        } as &dyn Statistics;
        serialize_statistics(stats)
    }
}

fn is_binary_column_type(primitive_type: &PrimitiveType) -> bool {
    primitive_type.physical_type == PhysicalType::ByteArray && primitive_type.logical_type.is_none()
}

fn binary_upper_bound(max_value: Vec<u8>) -> Vec<u8> {
    // We only keep 8 initial bytes for the min and max values.
    // Semantics of these Parquet fields are "lower and upper bound".
    // If max_value is longer than 8 bytes, we must choose an 8-byte value that
    // comes just after actual max_value in sort order. We achieve this by
    // converting to integer, incrementing, and converting back to bytes.
    // TODO: if first 8 bytes are all 0xFFs, it can't be incremented and the
    //       upper bound will be slightly off!
    let val_slice_be: [u8; SIZEOF_I64] = max_value[..SIZEOF_I64].try_into().unwrap();
    let upper_bound = i64::from_be_bytes(val_slice_be).saturating_add(1);
    upper_bound.to_be_bytes().to_vec()
}

pub struct ArrayStats {
    null_count: usize,
}

impl ArrayStats {
    pub fn new(null_count: usize) -> Self {
        Self { null_count }
    }

    pub fn into_parquet_stats(self) -> ParquetStatistics {
        ParquetStatistics {
            null_count: Some(self.null_count as i64),
            distinct_count: None,
            max_value: None,
            min_value: None,
            min: None,
            max: None,
        }
    }
}

pub struct ExactSizedIter<T, I: Iterator<Item = T>> {
    iter: I,
    remaining: usize,
}

impl<T, I: Iterator<Item = T> + Clone> Clone for ExactSizedIter<T, I> {
    fn clone(&self) -> Self {
        Self { iter: self.iter.clone(), remaining: self.remaining }
    }
}

impl<T, I: Iterator<Item = T>> ExactSizedIter<T, I> {
    pub fn new(iter: I, length: usize) -> Self {
        Self { iter, remaining: length }
    }
}

impl<T, I: Iterator<Item = T>> Iterator for ExactSizedIter<T, I> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().inspect(|_x| {
            self.remaining -= 1;
        })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

fn encode_primitive_def_levels_v1<I: Iterator<Item = bool>>(
    buffer: &mut Vec<u8>,
    iter: I,
    length: usize,
) -> io::Result<()> {
    buffer.extend_from_slice(&[0; 4]);
    let start = buffer.len();
    encode_bool(buffer, iter, length)?;
    let end = buffer.len();
    let length_bytes = end - start;

    // write the first 4 bytes as length
    let length_bytes = (length_bytes as i32).to_le_bytes();
    (0..4).for_each(|i| buffer[start - 4 + i] = length_bytes[i]);
    Ok(())
}

fn encode_primitive_def_levels_v2<I: Iterator<Item = bool>>(
    buffer: &mut Vec<u8>,
    iter: I,
    length: usize,
) -> io::Result<()> {
    encode_bool(buffer, iter, length)
}

pub fn encode_primitive_def_levels<I: Iterator<Item = bool>>(
    buffer: &mut Vec<u8>,
    iter: I,
    length: usize,
    version: Version,
) -> io::Result<()> {
    match version {
        Version::V1 => encode_primitive_def_levels_v1(buffer, iter, length),
        Version::V2 => encode_primitive_def_levels_v2(buffer, iter, length),
    }
}

fn encode_group_levels_v1<I: Iterator<Item = u32>>(
    buffer: &mut Vec<u8>,
    iter: I,
    length: usize,
    num_bits: u32,
) -> io::Result<()> {
    buffer.extend_from_slice(&[0; 4]);
    let start = buffer.len();
    encode_u32(buffer, iter, length, num_bits)?;
    let end = buffer.len();
    let length_bytes = end - start;

    // write the first 4 bytes as length
    let length_bytes = (length_bytes as i32).to_le_bytes();
    (0..4).for_each(|i| buffer[start - 4 + i] = length_bytes[i]);
    Ok(())
}

fn encode_group_levels_v2<I: Iterator<Item = u32>>(
    buffer: &mut Vec<u8>,
    iter: I,
    length: usize,
    num_bits: u32,
) -> io::Result<()> {
    encode_u32(buffer, iter, length, num_bits)
}

pub fn encode_group_levels<I: Iterator<Item = u32>>(
    buffer: &mut Vec<u8>,
    iter: I,
    length: usize,
    max_level: u32,
    version: Version,
) -> io::Result<()> {
    let num_bits = bit_width(max_level as u64);
    match version {
        Version::V1 => encode_group_levels_v1(buffer, iter, length, num_bits.into()),
        Version::V2 => encode_group_levels_v2(buffer, iter, length, num_bits.into()),
    }
}

#[inline]
pub fn bit_width(max: u64) -> u8 {
    (64 - max.leading_zeros()) as u8
}

#[allow(clippy::too_many_arguments)]
pub fn build_plain_page(
    buffer: Vec<u8>,
    num_rows: usize,
    null_count: usize,
    definition_levels_byte_length: usize,
    statistics: Option<ParquetStatistics>,
    primitive_type: PrimitiveType,
    options: WriteOptions,
    encoding: Encoding,
    required: bool,
) -> ParquetResult<DataPage> {
    let header = match options.version {
        Version::V1 => DataPageHeader::V1(DataPageHeaderV1 {
            num_values: num_rows as i32,
            encoding: encoding.into(),
            definition_level_encoding: Encoding::Rle.into(),
            repetition_level_encoding: Encoding::Rle.into(),
            statistics,
        }),
        Version::V2 => DataPageHeader::V2(DataPageHeaderV2 {
            num_values: num_rows as i32,
            encoding: encoding.into(),
            num_nulls: null_count as i32,
            num_rows: num_rows as i32,
            definition_levels_byte_length: definition_levels_byte_length as i32,
            repetition_levels_byte_length: 0,
            is_compressed: Some(options.compression != CompressionOptions::Uncompressed),
            statistics,
        }),
    };
    Ok(DataPage::new(
        header,
        buffer,
        Descriptor {
            primitive_type,
            max_def_level: if required { 0 } else { 1 },
            max_rep_level: 0,
        },
        Some(num_rows),
    ))
}

pub unsafe fn transmute_slice<T>(slice: &[u8]) -> &[T] {
    let sizeof_t = mem::size_of::<T>();
    assert_eq!(slice.len() % sizeof_t, 0);
    if slice.is_empty() {
        &[]
    } else {
        slice::from_raw_parts(slice.as_ptr() as *const T, slice.len() / sizeof_t)
    }
}

#[cfg(test)]
mod tests {
    use parquet2::encoding::bitpacked;
    use parquet2::encoding::hybrid_rle::{Decoder, HybridEncoded};

    use crate::parquet_write::util::encode_primitive_def_levels;

    #[test]
    fn decode_bitmap_v2() {
        let bit_width = 1;
        let expected = &[
            false, false, true, false, true, false, true, false, true, false, false, false, true,
            true,
        ];
        let expectedu8 = expected
            .iter()
            .map(|x| if *x { 1u8 } else { 0u8 })
            .collect::<Vec<_>>();
        let mut buff = vec![];
        encode_primitive_def_levels(
            &mut buff,
            expected.iter().cloned(),
            expected.len(),
            parquet2::write::Version::V2,
        )
        .unwrap();

        let mut decoder = Decoder::new(buff.as_slice(), 1);
        let run = decoder.next().unwrap();

        if let HybridEncoded::Bitpacked(values) = run.unwrap() {
            let result = bitpacked::Decoder::<u8>::try_new(values, bit_width, expected.len())
                .unwrap()
                .collect::<Vec<_>>();
            assert_eq!(result, expectedu8);
        } else {
            panic!()
        };
    }

    #[test]
    fn decode_bitmap_v1() {
        let bit_width = 1;
        let expected = &[
            false, false, true, false, true, false, true, false, true, false, false, false, true,
            true,
        ];
        let expectedu8 = expected
            .iter()
            .map(|x| if *x { 1u8 } else { 0u8 })
            .collect::<Vec<_>>();
        let mut buff = vec![];
        encode_primitive_def_levels(
            &mut buff,
            expected.iter().cloned(),
            expected.len(),
            parquet2::write::Version::V1,
        )
        .unwrap();

        let length = i32::from_le_bytes(buff[..4].try_into().unwrap()) as usize;
        assert_eq!(length, buff.len() - 4);

        let mut decoder = Decoder::new(&buff[4..], 1);
        let run = decoder.next().unwrap();

        if let HybridEncoded::Bitpacked(values) = run.unwrap() {
            let result = bitpacked::Decoder::<u8>::try_new(values, bit_width, expected.len())
                .unwrap()
                .collect::<Vec<_>>();
            assert_eq!(result, expectedu8);
        } else {
            panic!()
        };
    }
}
