use std::{cmp, io};

use parquet2::compression::CompressionOptions;
use parquet2::encoding::Encoding;
use parquet2::encoding::hybrid_rle::encode_bool;
use parquet2::metadata::Descriptor;
use parquet2::page::{DataPage, DataPageHeader, DataPageHeaderV1, DataPageHeaderV2};
use parquet2::schema::types::PrimitiveType;
use parquet2::statistics::ParquetStatistics;
use parquet2::types::NativeType;
use parquet2::write::Version;

use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::ParquetResult;

#[derive(Debug)]
pub struct MaxMin<T> {
    max: T,
    min: T,
    is_updated: bool,
}

impl<T: Copy + NativeType + num_traits::Bounded> MaxMin<T> {
    pub fn new() -> Self {
        MaxMin {
            max: T::min_value(),
            min: T::max_value(),
            is_updated: false,
        }
    }
    pub fn update(&mut self, x: T) {
        if !self.is_updated {
            self.max = x;
            self.min = x;
            self.is_updated = true;
        } else {
            self.max = cmp::max_by(self.max, x, |x, y| x.ord(y));
            self.min = cmp::min_by(self.min, x, |x, y| x.ord(y));
        }
    }

    pub fn get_current_values(&self) -> (Option<T>, Option<T>) {
        if self.is_updated {
            (Some(self.max), Some(self.min))
        } else {
            (None, None)
        }
    }
}

pub struct ExactSizedIter<T, I: Iterator<Item=T>> {
    iter: I,
    remaining: usize,
}

impl<T, I: Iterator<Item=T> + Clone> Clone for ExactSizedIter<T, I> {
    fn clone(&self) -> Self {
        Self { iter: self.iter.clone(), remaining: self.remaining }
    }
}

impl<T, I: Iterator<Item=T>> ExactSizedIter<T, I> {
    pub fn new(iter: I, length: usize) -> Self {
        Self { iter, remaining: length }
    }
}

impl<T, I: Iterator<Item=T>> Iterator for ExactSizedIter<T, I> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|x| {
            self.remaining -= 1;
            x
        })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

fn encode_iter_v1<I: Iterator<Item=bool>>(buffer: &mut Vec<u8>, iter: I) -> io::Result<()> {
    buffer.extend_from_slice(&[0; 4]);
    let start = buffer.len();
    encode_bool(buffer, iter)?;
    let end = buffer.len();
    let length = end - start;

    // write the first 4 bytes as length
    let length = (length as i32).to_le_bytes();
    (0..4).for_each(|i| buffer[start - 4 + i] = length[i]);
    Ok(())
}

fn encode_iter_v2<I: Iterator<Item=bool>>(writer: &mut Vec<u8>, iter: I) -> io::Result<()> {
    encode_bool(writer, iter)
}

pub fn encode_bool_iter<I: Iterator<Item=bool>>(
    writer: &mut Vec<u8>,
    iter: I,
    version: Version,
) -> io::Result<()> {
    match version {
        Version::V1 => encode_iter_v1(writer, iter),
        Version::V2 => encode_iter_v2(writer, iter),
    }
}

#[inline]
pub fn get_bit_width(max: u64) -> u32 {
    64 - max.leading_zeros()
}

#[allow(clippy::too_many_arguments)]
pub fn build_plain_page(
    buffer: Vec<u8>,
    num_values: usize,
    num_rows: usize,
    null_count: usize,
    repetition_levels_byte_length: usize,
    definition_levels_byte_length: usize,
    statistics: Option<ParquetStatistics>,
    type_: PrimitiveType,
    options: WriteOptions,
    encoding: Encoding,
) -> ParquetResult<DataPage> {
    let header = match options.version {
        Version::V1 => DataPageHeader::V1(DataPageHeaderV1 {
            num_values: num_values as i32,
            encoding: encoding.into(),
            definition_level_encoding: Encoding::Rle.into(),
            repetition_level_encoding: Encoding::Rle.into(),
            statistics,
        }),
        Version::V2 => DataPageHeader::V2(DataPageHeaderV2 {
            num_values: num_values as i32,
            encoding: encoding.into(),
            num_nulls: null_count as i32,
            num_rows: num_rows as i32,
            definition_levels_byte_length: definition_levels_byte_length as i32,
            repetition_levels_byte_length: repetition_levels_byte_length as i32,
            is_compressed: Some(options.compression != CompressionOptions::Uncompressed),
            statistics,
        }),
    };
    Ok(DataPage::new(
        header,
        buffer,
        Descriptor {
            primitive_type: type_,
            max_def_level: 0,
            max_rep_level: 0,
        },
        Some(num_rows),
    ))
}
