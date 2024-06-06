use std::{cmp, io, mem, slice};

use parquet2::compression::CompressionOptions;
use parquet2::encoding::hybrid_rle::encode_bool;
use parquet2::encoding::Encoding;
use parquet2::metadata::Descriptor;
use parquet2::page::{DataPage, DataPageHeader, DataPageHeaderV1, DataPageHeaderV2};
use parquet2::schema::types::{PhysicalType, PrimitiveType};
use parquet2::statistics::{serialize_statistics, BinaryStatistics, ParquetStatistics, Statistics};
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

pub struct BinaryMaxMin {
    primitive_type: PrimitiveType,
    max_value: Option<Vec<u8>>,
    min_value: Option<Vec<u8>>,
}

const SIZEOF_I64: usize = mem::size_of::<i64>();

impl BinaryMaxMin {
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

fn encode_iter_v1<I: Iterator<Item = bool>>(buffer: &mut Vec<u8>, iter: I) -> io::Result<()> {
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

fn encode_iter_v2<I: Iterator<Item = bool>>(buffer: &mut Vec<u8>, iter: I) -> io::Result<()> {
    encode_bool(buffer, iter)
}

pub fn encode_bool_iter<I: Iterator<Item = bool>>(
    buffer: &mut Vec<u8>,
    iter: I,
    version: Version,
) -> io::Result<()> {
    match version {
        Version::V1 => encode_iter_v1(buffer, iter),
        Version::V2 => encode_iter_v2(buffer, iter),
    }
}

#[inline]
pub fn get_bit_width(max: u64) -> u8 {
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
        Descriptor { primitive_type, max_def_level: 1, max_rep_level: 0 },
        Some(num_rows),
    ))
}

pub unsafe fn transmute_slice<T>(slice: &[u8]) -> &[T] {
    let sizeof_t = mem::size_of::<T>();
    assert!(
        slice.len() % sizeof_t == 0,
        "slice.len() {} % sizeof_t {} != 0",
        slice.len(),
        sizeof_t
    );
    slice::from_raw_parts(slice.as_ptr() as *const T, slice.len() / sizeof_t)
}
