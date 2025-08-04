/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

use std::mem;

use parquet2::compression::CompressionOptions;
use parquet2::metadata::Descriptor;
use parquet2::page::{DataPage, DataPageHeader, DataPageHeaderV1, DataPageHeaderV2};
use parquet2::statistics::ParquetStatistics;
use parquet2::write::Version;
use qdb_core::col_driver::ArrayAuxEntry;

use crate::parquet_write::util;
use parquet2::encoding::{delta_bitpacked, Encoding};
use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;

use super::util::{ArrayStats, BinaryMaxMinStats};
use crate::allocator::AcVec;
use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util::{
    build_plain_page, encode_group_levels, encode_primitive_deflevels, ExactSizedIter,
};

const HEADER_SIZE_NULL: [u8; 8] = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
// must be kept in sync with Java's ColumnType#ARRAY_NDIMS_LIMIT
const ARRAY_NDIMS_LIMIT: usize = 32;

#[derive(Clone, Copy)]
struct RepLevelsIterator<'a> {
    shape: &'a [i32],
    data_len: usize,
    flat_index: usize,
    nd_indexes: [i32; ARRAY_NDIMS_LIMIT],
}

impl<'a> RepLevelsIterator<'a> {
    pub fn new(shape: &'a [i32], data_len: usize) -> Self {
        RepLevelsIterator {
            shape,
            data_len,
            flat_index: 0,
            nd_indexes: [0_i32; ARRAY_NDIMS_LIMIT],
        }
    }

    // returns single zero item
    pub fn new_single() -> Self {
        RepLevelsIterator {
            shape: &[],
            data_len: 1,
            flat_index: 0,
            nd_indexes: [0_i32; ARRAY_NDIMS_LIMIT],
        }
    }

    /// Calculates repetition level of the next element.
    /// To do that, increments nd_indexes and returns the index of the lowest dimension
    /// where nd_indexes values had an overflow.
    ///
    /// E.g. for the shapes of [2, 3] and initial nd_indexes of [0, 2],
    /// the incremented nd_indexes value will be [1, 0] and the returned value will be 0.
    fn next_replevel(&mut self) -> u32 {
        for i in (0..self.shape.len()).rev() {
            self.nd_indexes[i] += 1;
            if self.nd_indexes[i] < self.shape[i] {
                return (i + 1) as u32;
            } else {
                // we've got an overflow in i-th dimension,
                // so carry on to the previous dimension
                self.nd_indexes[i] = 0;
            }
        }
        return 0;
    }
}

impl Iterator for RepLevelsIterator<'_> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.flat_index >= self.data_len {
            return None;
        }

        let replevel = if self.flat_index == 0 {
            0 // First element always has repetition level 0
        } else {
            self.next_replevel()
        };
        self.flat_index += 1;

        Some(replevel)
    }
}

const DEF_LEVELS_STUB_DATA: [f64; 1] = [42.0];

#[derive(Clone, Copy)]
struct DefLevelsIterator<'a> {
    max: u32,
    data: &'a [f64],
    flat_index: usize,
}

impl<'a> DefLevelsIterator<'a> {
    pub fn new(data: &'a [f64], max: u32) -> Self {
        DefLevelsIterator { max, data, flat_index: 0 }
    }

    // returns single zero item
    pub fn new_single(max: u32) -> Self {
        DefLevelsIterator { max, data: &DEF_LEVELS_STUB_DATA, flat_index: 0 }
    }
}

impl Iterator for DefLevelsIterator<'_> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.flat_index >= self.data.len() {
            return None;
        }

        let deflevel = if self.data[self.flat_index].is_nan() {
            self.max - 1
        } else {
            self.max
        };
        self.flat_index += 1;

        Some(deflevel)
    }
}

// encodes array as nested lists
pub fn array_to_page(
    // inner-most type of the array group field
    primitive_type: PrimitiveType,
    dim: usize,
    aux: &[[u8; 16]],
    data: &[u8],
    column_top: usize,
    options: WriteOptions,
    encoding: Encoding,
) -> ParquetResult<Page> {
    assert!(
        mem::size_of::<ArrayAuxEntry>() == 16,
        "size_of(ArrayAuxEntry) is not 16"
    );

    if dim > ARRAY_NDIMS_LIMIT {
        return Err(fmt_err!(
            Unsupported,
            "too large number of array dimensions {dim}"
        ));
    }

    let num_rows = column_top + aux.len();
    let mut buffer = vec![];
    let mut null_count = 0usize;

    let aux: &[ArrayAuxEntry] = unsafe { mem::transmute(aux) };

    let shape_size = 4 * dim;
    // data offset is aligned to 8 bytes
    let data_offset = (shape_size + 7) & !0x7;
    let arr_slices: Vec<Option<(&[i32], &[f64])>> = aux
        .iter()
        .map(|entry| {
            let size = entry.size() as usize;
            let offset = entry.offset() as usize;
            if size > 0 {
                assert!(
                    offset + size <= data.len(),
                    "Data corruption in ARRAY column"
                );
                let arr = &data[offset..][..size];
                let shape: &[i32] = unsafe { util::transmute_slice(&arr[..shape_size]) };
                let data: &[f64] = unsafe { util::transmute_slice(&arr[data_offset..]) };
                Some((shape, data))
            } else {
                null_count += 1;
                None
            }
        })
        .collect();

    // calculate lengths of rep and def levels ahead of time
    let num_values = arr_slices
        .iter()
        .map(|arr| match arr {
            Some(arr) => {
                let data = arr.1;
                std::cmp::max(data.len(), 1)
            }
            None => 1,
        })
        .sum::<usize>()
        + column_top;

    let replevels_iter = (0..num_rows).flat_map(|i| {
        if i < column_top {
            RepLevelsIterator::new_single()
        } else {
            match arr_slices[i - column_top] {
                Some(arr) => {
                    let shape = arr.0;
                    let data = arr.1;
                    if data.len() == 0 {
                        RepLevelsIterator::new_single()
                    } else {
                        RepLevelsIterator::new(shape, data.len())
                    }
                }
                None => RepLevelsIterator::new_single(),
            }
        }
    });
    encode_group_levels(
        &mut buffer,
        replevels_iter,
        num_values,
        dim as u32,
        options.version,
    )?;

    let repetition_levels_byte_length = buffer.len();

    let deflevels_iter = (0..num_rows).flat_map(|i| {
        if i < column_top {
            DefLevelsIterator::new_single(0)
        } else {
            match arr_slices[i - column_top] {
                Some(arr) => {
                    let data = arr.1;
                    if data.len() == 0 {
                        DefLevelsIterator::new_single(1)
                    } else {
                        // max def level is the number of dimensions plus two optional fields
                        DefLevelsIterator::new(data, (dim + 2) as u32)
                    }
                }
                None => DefLevelsIterator::new_single(0),
            }
        }
    });
    encode_group_levels(
        &mut buffer,
        deflevels_iter,
        num_values,
        (dim + 1) as u32,
        options.version,
    )?;

    let definition_levels_byte_length = buffer.len() - repetition_levels_byte_length;

    match encoding {
        Encoding::Plain => {
            encode_data_plain(&arr_slices, &mut buffer);
        }
        _ => {
            return Err(fmt_err!(
                Unsupported,
                "unsupported encoding {encoding:?} while writing an array column"
            ));
        }
    };

    let null_count = column_top + null_count;
    let stats = ArrayStats::new(null_count);
    build_array_page(
        primitive_type,
        buffer,
        num_values,
        num_rows,
        null_count,
        repetition_levels_byte_length,
        definition_levels_byte_length,
        if options.write_statistics {
            Some(stats.into_parquet_stats())
        } else {
            None
        },
        options,
        encoding,
    )
    .map(Page::Data)
}

fn encode_data_plain(arr_slices: &[Option<(&[i32], &[f64])>], buffer: &mut Vec<u8>) {
    for (_shape, data) in arr_slices.iter().filter_map(|&option| option) {
        data.iter().for_each(|v| {
            if !v.is_nan() {
                buffer.extend_from_slice(&v.to_le_bytes());
            }
        });
    }
}

#[allow(clippy::too_many_arguments)]
fn build_array_page(
    // inner-most type of array group field
    primitive_type: PrimitiveType,
    buffer: Vec<u8>,
    num_values: usize,
    num_rows: usize,
    null_count: usize,
    repetition_levels_byte_length: usize,
    definition_levels_byte_length: usize,
    statistics: Option<ParquetStatistics>,
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
            primitive_type,
            // max level values are ignored and recalculated for group types
            max_def_level: 0,
            max_rep_level: 0,
        },
        Some(num_rows),
    ))
}

// encodes in native QDB format
pub fn array_to_raw_page(
    aux: &[[u8; 16]],
    data: &[u8],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    encoding: Encoding,
) -> ParquetResult<Page> {
    assert!(
        mem::size_of::<ArrayAuxEntry>() == 16,
        "size_of(ArrayAuxEntry) is not 16"
    );

    let num_rows = column_top + aux.len();
    let mut buffer = vec![];
    let mut null_count = 0usize;

    let aux: &[ArrayAuxEntry] = unsafe { mem::transmute(aux) };

    let arr_slices: Vec<Option<&[u8]>> = aux
        .iter()
        .map(|entry| {
            let size = entry.size() as usize;
            let offset = entry.offset() as usize;
            if size > 0 {
                assert!(
                    offset + size <= data.len(),
                    "Data corruption in ARRAY column"
                );
                Some(&data[offset..][..size])
            } else {
                null_count += 1;
                None
            }
        })
        .collect();

    let deflevels_iter =
        (0..num_rows).map(|i| i >= column_top && arr_slices[i - column_top].is_some());
    encode_primitive_deflevels(&mut buffer, deflevels_iter, num_rows, options.version)?;

    let definition_levels_byte_length = buffer.len();

    let mut stats = BinaryMaxMinStats::new(&primitive_type);

    match encoding {
        Encoding::Plain => {
            encode_raw_plain(&arr_slices, &mut buffer, &mut stats);
        }
        Encoding::DeltaLengthByteArray => {
            encode_raw_delta(&arr_slices, null_count, &mut buffer, &mut stats);
        }
        _ => {
            return Err(fmt_err!(
                Unsupported,
                "unsupported encoding {encoding:?} while writing an array column"
            ));
        }
    };

    let null_count = column_top + null_count;
    build_plain_page(
        buffer,
        num_rows,
        null_count,
        definition_levels_byte_length,
        if options.write_statistics {
            Some(stats.into_parquet_stats(null_count))
        } else {
            None
        },
        primitive_type,
        options,
        encoding,
    )
    .map(Page::Data)
}

fn encode_raw_plain(
    arr_slices: &[Option<&[u8]>],
    buffer: &mut Vec<u8>,
    stats: &mut BinaryMaxMinStats,
) {
    for arr in arr_slices.iter().filter_map(|&option| option) {
        let len = (arr.len() as u32).to_le_bytes();
        buffer.extend_from_slice(&len);
        buffer.extend_from_slice(arr);
        stats.update(arr);
    }
}

fn encode_raw_delta(
    arr_slices: &[Option<&[u8]>],
    null_count: usize,
    buffer: &mut Vec<u8>,
    stats: &mut BinaryMaxMinStats,
) {
    let lengths = arr_slices
        .iter()
        .filter_map(|&option| option)
        .map(|arr| arr.len() as i64);
    let lengths = ExactSizedIter::new(lengths, arr_slices.len() - null_count);
    delta_bitpacked::encode(lengths, buffer);
    for arr in arr_slices.iter().filter_map(|&option| option) {
        buffer.extend_from_slice(arr);
        stats.update(arr);
    }
}

pub fn append_raw_array(
    aux_mem: &mut AcVec<u8>,
    data_mem: &mut AcVec<u8>,
    value: &[u8],
) -> ParquetResult<()> {
    let value_size = value.len();
    aux_mem.extend_from_slice(&data_mem.len().to_le_bytes())?;
    aux_mem.extend_from_slice(&value_size.to_le_bytes())?;
    data_mem.extend_from_slice(value)?;
    Ok(())
}

pub fn append_raw_array_null(aux_mem: &mut AcVec<u8>, data_mem: &[u8]) -> ParquetResult<()> {
    aux_mem.extend_from_slice(&data_mem.len().to_le_bytes())?;
    aux_mem.extend_from_slice(&HEADER_SIZE_NULL)?;
    Ok(())
}

pub fn append_raw_array_nulls(
    aux_mem: &mut AcVec<u8>,
    data_mem: &[u8],
    count: usize,
) -> ParquetResult<()> {
    for _ in 0..count {
        append_raw_array_null(aux_mem, data_mem)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_or_empty() {
        let rep_levels: Vec<u32> = RepLevelsIterator::new_single().collect();
        assert_eq!(rep_levels, vec![0]);
        let def_levels: Vec<u32> = DefLevelsIterator::new_single(0).collect();
        assert_eq!(def_levels, vec![0]);
        let def_levels: Vec<u32> = DefLevelsIterator::new_single(1).collect();
        assert_eq!(def_levels, vec![1]);
    }

    #[test]
    fn test_single_element_1d_array() {
        // 1d array, e.g. [42]
        let shape = vec![1];
        let data = vec![42.0];
        let rep_levels: Vec<u32> = RepLevelsIterator::new(shape.as_slice(), 1).collect();
        assert_eq!(rep_levels, vec![0]);
        let def_levels: Vec<u32> = DefLevelsIterator::new(data.as_slice(), 3).collect();
        assert_eq!(def_levels, vec![3]);
    }

    #[test]
    fn test_1d_array() {
        // 1d array
        let shape = vec![4];
        let data = vec![1.0, 2.0, 3.0, 4.0];
        let rep_levels: Vec<u32> = RepLevelsIterator::new(shape.as_slice(), 4).collect();
        assert_eq!(rep_levels, vec![0, 1, 1, 1]);
        let def_levels: Vec<u32> = DefLevelsIterator::new(data.as_slice(), 3).collect();
        assert_eq!(def_levels, vec![3, 3, 3, 3]);
    }

    #[test]
    fn test_1d_array_with_nulls() {
        // 1d array
        let shape = vec![4];
        let data = vec![std::f64::NAN, 2.0, std::f64::NAN, 4.0];
        let rep_levels: Vec<u32> = RepLevelsIterator::new(shape.as_slice(), 4).collect();
        assert_eq!(rep_levels, vec![0, 1, 1, 1]);
        let def_levels: Vec<u32> = DefLevelsIterator::new(data.as_slice(), 3).collect();
        assert_eq!(def_levels, vec![2, 3, 2, 3]);
    }

    #[test]
    fn test_2d_array() {
        // 2x3 array
        let shape = vec![2, 3];
        // [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
        let rep_levels: Vec<u32> = RepLevelsIterator::new(shape.as_slice(), 6).collect();
        assert_eq!(rep_levels, vec![0, 2, 2, 1, 2, 2]);
        let def_levels: Vec<u32> = DefLevelsIterator::new(data.as_slice(), 4).collect();
        assert_eq!(def_levels, vec![4, 4, 4, 4, 4, 4]);
    }

    #[test]
    fn test_3d_array() {
        // 2x2x2 array
        let shape = vec![2, 2, 2];
        // [[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0], [7.0, 8.0]]]
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let rep_levels: Vec<u32> = RepLevelsIterator::new(shape.as_slice(), 8).collect();
        assert_eq!(rep_levels, vec![0, 3, 2, 3, 1, 3, 2, 3]);
        let def_levels: Vec<u32> = DefLevelsIterator::new(data.as_slice(), 5).collect();
        assert_eq!(def_levels, vec![5, 5, 5, 5, 5, 5, 5, 5]);
    }

    #[test]
    fn test_3d_array_with_nulls() {
        // 2x2x2 array
        let shape = vec![2, 2, 2];
        // [[[1.0, null], [null, 4.0]], [[5.0, 6.0], [7.0, 8.0]]]
        let data = vec![1.0, std::f64::NAN, std::f64::NAN, 4.0, 5.0, 6.0, 7.0, 8.0];
        let rep_levels: Vec<u32> = RepLevelsIterator::new(shape.as_slice(), 8).collect();
        assert_eq!(rep_levels, vec![0, 3, 2, 3, 1, 3, 2, 3]);
        let def_levels: Vec<u32> = DefLevelsIterator::new(data.as_slice(), 5).collect();
        assert_eq!(def_levels, vec![5, 4, 4, 5, 5, 5, 5, 5]);
    }
}
