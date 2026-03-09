/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

use crate::parquet::util::{align8b, ARRAY_NDIMS_LIMIT};
use parquet2::compression::CompressionOptions;
use parquet2::encoding::hybrid_rle::HybridRleDecoder;
use parquet2::metadata::Descriptor;
use parquet2::page::{DataPage, DataPageHeader, DataPageHeaderV1, DataPageHeaderV2};
use parquet2::read::levels::get_bit_width;
use parquet2::statistics::ParquetStatistics;
use parquet2::write::Version;
use qdb_core::col_driver::ArrayAuxEntry;
use std::mem;

use crate::parquet_write::util;
use parquet2::encoding::{delta_bitpacked, Encoding};
use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;

use super::util::ArrayStats;
use crate::allocator::AcVec;
use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util::{
    build_plain_page, encode_group_levels, encode_primitive_def_levels, ExactSizedIter,
};

const HEADER_SIZE_NULL: [u8; 8] = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];

// Helper struct for array data access
struct ArrayDataParser<'a> {
    data: &'a [u8],
    shape_size: usize,
    data_offset: usize,
}

impl<'a> ArrayDataParser<'a> {
    fn get_array_data(&self, entry: &ArrayAuxEntry) -> Option<(&'a [i32], &'a [f64])> {
        let size = entry.size() as usize;
        let offset = entry.offset() as usize;
        if size > 0 {
            assert!(
                offset + size <= self.data.len(),
                "Data corruption in ARRAY column"
            );
            let arr = &self.data[offset..][..size];
            let shape: &[i32] = unsafe { util::transmute_slice(&arr[..self.shape_size]) };
            let data: &[f64] = unsafe { util::transmute_slice(&arr[self.data_offset..]) };
            Some((shape, data))
        } else {
            None
        }
    }
}

// Helper struct for raw array data access
struct RawArrayDataParser<'a> {
    data: &'a [u8],
}

impl<'a> RawArrayDataParser<'a> {
    fn get_raw_array_data(&self, entry: &ArrayAuxEntry) -> Option<&'a [u8]> {
        let size = entry.size() as usize;
        let offset = entry.offset() as usize;
        if size > 0 {
            assert!(
                offset + size <= self.data.len(),
                "Data corruption in ARRAY column"
            );
            Some(&self.data[offset..][..size])
        } else {
            None
        }
    }
}

// generates rep levels values for the given array shape and number of elements
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
    fn next_rep_level(&mut self) -> u32 {
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
        0
    }
}

impl Iterator for RepLevelsIterator<'_> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.flat_index >= self.data_len {
            return None;
        }

        let rep_level = if self.flat_index == 0 {
            0 // First element always has repetition level 0
        } else {
            self.next_rep_level()
        };
        self.flat_index += 1;

        Some(rep_level)
    }
}

const DEF_LEVELS_STUB_DATA: [f64; 1] = [42.0];

// generates def levels values for the given array elements
#[derive(Clone, Copy)]
struct DefLevelsIterator<'a> {
    max_def_level: u32,
    data: &'a [f64],
    flat_index: usize,
}

impl<'a> DefLevelsIterator<'a> {
    pub fn new(data: &'a [f64], max_def_level: u32) -> Self {
        DefLevelsIterator { max_def_level, data, flat_index: 0 }
    }

    // returns single zero item
    pub fn new_single(max_def_level: u32) -> Self {
        DefLevelsIterator {
            max_def_level,
            data: &DEF_LEVELS_STUB_DATA,
            flat_index: 0,
        }
    }
}

impl Iterator for DefLevelsIterator<'_> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.flat_index >= self.data.len() {
            return None;
        }

        let def_level = if self.data[self.flat_index].is_nan() {
            self.max_def_level - 1
        } else {
            self.max_def_level
        };
        self.flat_index += 1;

        Some(def_level)
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
    assert_eq!(
        mem::size_of::<ArrayAuxEntry>(),
        16,
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
    let data_offset = align8b(shape_size);

    let parser = ArrayDataParser { data, shape_size, data_offset };

    // calculate lengths of rep and def levels ahead of time (streaming approach)
    let mut num_values = column_top;
    for entry in aux.iter() {
        if let Some((_shape, arr_data)) = parser.get_array_data(entry) {
            num_values += std::cmp::max(arr_data.len(), 1);
        } else {
            null_count += 1;
            num_values += 1;
        }
    }

    let max_rep_level = dim as u32;
    let rep_levels_iter = (0..num_rows).flat_map(|i| {
        if i < column_top {
            RepLevelsIterator::new_single()
        } else {
            match parser.get_array_data(&aux[i - column_top]) {
                Some((shape, data)) => {
                    if data.is_empty() {
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
        rep_levels_iter,
        num_values,
        max_rep_level,
        options.version,
    )?;

    let repetition_levels_byte_length = buffer.len();

    // max def level is the number of dimensions plus two optional fields
    let max_def_level = (dim + 2) as u32;
    let def_levels_iter = (0..num_rows).flat_map(|i| {
        if i < column_top {
            DefLevelsIterator::new_single(0)
        } else {
            match parser.get_array_data(&aux[i - column_top]) {
                Some((_shape, data)) => {
                    if data.is_empty() {
                        DefLevelsIterator::new_single(1)
                    } else {
                        DefLevelsIterator::new(data, max_def_level)
                    }
                }
                None => DefLevelsIterator::new_single(0),
            }
        }
    });
    encode_group_levels(
        &mut buffer,
        def_levels_iter,
        num_values,
        max_def_level,
        options.version,
    )?;

    let definition_levels_byte_length = buffer.len() - repetition_levels_byte_length;

    match encoding {
        Encoding::Plain => {
            encode_data_plain_streaming(aux, &parser, &mut buffer);
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

// Streaming version that processes arrays on-demand without intermediate allocation
fn encode_data_plain_streaming(
    aux: &[ArrayAuxEntry],
    parser: &ArrayDataParser,
    buffer: &mut Vec<u8>,
) {
    for entry in aux.iter() {
        if let Some((_shape, data)) = parser.get_array_data(entry) {
            data.iter().for_each(|v| {
                if !v.is_nan() {
                    buffer.extend_from_slice(&v.to_le_bytes());
                }
            });
        }
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
    assert_eq!(
        mem::size_of::<ArrayAuxEntry>(),
        16,
        "size_of(ArrayAuxEntry) is not 16"
    );

    let num_rows = column_top + aux.len();
    let mut buffer = vec![];
    let mut null_count = 0usize;

    let aux: &[ArrayAuxEntry] = unsafe { mem::transmute(aux) };

    let raw_parser = RawArrayDataParser { data };

    // Count nulls during streaming
    for entry in aux.iter() {
        if raw_parser.get_raw_array_data(entry).is_none() {
            null_count += 1;
        }
    }

    let def_levels_iter = (0..num_rows).map(|i| {
        i >= column_top
            && raw_parser
                .get_raw_array_data(&aux[i - column_top])
                .is_some()
    });
    encode_primitive_def_levels(&mut buffer, def_levels_iter, num_rows, options.version)?;

    let definition_levels_byte_length = buffer.len();

    match encoding {
        Encoding::Plain => {
            encode_raw_plain_streaming(aux, &raw_parser, &mut buffer);
        }
        Encoding::DeltaLengthByteArray => {
            encode_raw_delta_streaming(aux, &raw_parser, null_count, &mut buffer);
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
    build_plain_page(
        buffer,
        num_rows,
        null_count,
        definition_levels_byte_length,
        if options.write_statistics {
            Some(stats.into_parquet_stats())
        } else {
            None
        },
        primitive_type,
        options,
        encoding,
        false,
    )
    .map(Page::Data)
}

// Streaming versions that process arrays on-demand
fn encode_raw_plain_streaming(
    aux: &[ArrayAuxEntry],
    raw_parser: &RawArrayDataParser,
    buffer: &mut Vec<u8>,
) {
    for entry in aux.iter() {
        if let Some(arr) = raw_parser.get_raw_array_data(entry) {
            let len = (arr.len() as u32).to_le_bytes();
            buffer.extend_from_slice(&len);
            buffer.extend_from_slice(arr);
        }
    }
}

fn encode_raw_delta_streaming(
    aux: &[ArrayAuxEntry],
    raw_parser: &RawArrayDataParser,
    null_count: usize,
    buffer: &mut Vec<u8>,
) {
    // First pass: collect lengths
    let lengths = aux
        .iter()
        .filter_map(|entry| raw_parser.get_raw_array_data(entry))
        .map(|arr| arr.len() as i64);
    let lengths = ExactSizedIter::new(lengths, aux.len() - null_count);
    delta_bitpacked::encode(lengths, buffer);

    // Second pass: write data
    for entry in aux.iter() {
        if let Some(arr) = raw_parser.get_raw_array_data(entry) {
            buffer.extend_from_slice(arr);
        }
    }
}

// iterates through the given encoded rep and def levels;
// does not implement Iterator since levels iterator returns an owned struct
pub struct LevelsIterator<'a> {
    rep_levels_iter: HybridRleDecoder<'a>,
    def_levels_iter: HybridRleDecoder<'a>,
    levels: Levels,
    last_rep_level: i64,
    last_def_level: i64,
    clear_pending: bool,
}

impl<'a> LevelsIterator<'a> {
    pub fn try_new(
        num_values: usize,
        max_rep_level: i16,
        max_def_level: i16,
        rep_levels: &'a [u8],
        def_levels: &'a [u8],
    ) -> ParquetResult<Self> {
        let rep_levels_iter: HybridRleDecoder<'_> =
            HybridRleDecoder::try_new(rep_levels, get_bit_width(max_rep_level), num_values)?;
        let def_levels_iter =
            HybridRleDecoder::try_new(def_levels, get_bit_width(max_def_level), num_values)?;

        let rep_levels_len = rep_levels_iter.size_hint().0;
        let def_levels_len = def_levels_iter.size_hint().0;
        if rep_levels_len != def_levels_len {
            return Err(fmt_err!(
                Unsupported,
                "repetition and definition levels sizes don't match for array column: {rep_levels_len}, {def_levels_len}"
            ));
        }

        Ok(LevelsIterator {
            rep_levels_iter,
            def_levels_iter,
            levels: Levels { rep_levels: vec![], def_levels: vec![] },
            last_rep_level: -1,
            last_def_level: -1,
            clear_pending: false,
        })
    }

    #[inline]
    pub fn has_lookahead(&self) -> bool {
        self.last_rep_level > -1
    }

    #[inline]
    pub fn take_lookahead(&mut self) -> (u32, u32) {
        debug_assert!(self.has_lookahead());
        let rep = self.last_rep_level as u32;
        let def = self.last_def_level as u32;
        self.last_rep_level = -1;
        self.last_def_level = -1;
        self.levels.clear();
        self.clear_pending = false;
        (rep, def)
    }

    #[inline]
    pub fn set_lookahead(&mut self, rep: u32, def: u32) {
        self.last_rep_level = rep as i64;
        self.last_def_level = def as i64;
    }

    #[inline]
    pub fn next_rep_def(&mut self) -> Option<ParquetResult<(u32, u32)>> {
        let rep = self.rep_levels_iter.next();
        let def = self.def_levels_iter.next();
        if rep.is_none() || def.is_none() {
            return None;
        }
        let rep = match rep.unwrap() {
            Ok(v) => v,
            Err(e) => return Some(Err(e.into())),
        };
        let def = match def.unwrap() {
            Ok(v) => v,
            Err(e) => return Some(Err(e.into())),
        };
        Some(Ok((rep, def)))
    }

    pub fn skip_rows(&mut self, n: usize, max_def_level: u32) -> ParquetResult<usize> {
        if n == 0 {
            return Ok(0);
        }

        let mut rows_skipped = 0usize;
        let mut non_null_count = 0usize;
        let mut has_elements = false;

        if self.last_rep_level > -1 {
            if self.last_def_level as u32 == max_def_level {
                non_null_count += 1;
            }
            self.last_rep_level = -1;
            self.last_def_level = -1;
            has_elements = true;
        }
        self.levels.clear();
        self.clear_pending = false;

        loop {
            match self.next_rep_def() {
                None => {
                    break;
                }
                Some(Err(e)) => return Err(e),
                Some(Ok((rep, def))) => {
                    if rep == 0 && has_elements {
                        rows_skipped += 1;
                        if rows_skipped >= n {
                            self.last_rep_level = rep as i64;
                            self.last_def_level = def as i64;
                            break;
                        }
                    }
                    has_elements = true;
                    if def == max_def_level {
                        non_null_count += 1;
                    }
                }
            }
        }

        Ok(non_null_count)
    }

    pub fn next_levels(&mut self) -> Option<ParquetResult<&Levels>> {
        if self.clear_pending {
            self.levels.clear();
            self.clear_pending = false;
        }

        loop {
            if self.last_rep_level > -1 {
                self.levels.rep_levels.push(self.last_rep_level as u32);
                self.last_rep_level = -1;
                self.levels.def_levels.push(self.last_def_level as u32);
                self.last_def_level = -1;
            }

            let rep_level = self.rep_levels_iter.next();
            let def_level = self.def_levels_iter.next();
            if rep_level.is_none() || def_level.is_none() {
                if !self.levels.is_empty() {
                    self.clear_pending = true;
                    return Some(Ok(&self.levels));
                }
                return None;
            }

            self.last_rep_level = match rep_level.unwrap() {
                Ok(level) => level as i64,
                Err(e) => return Some(Err(e.into())),
            };
            self.last_def_level = match def_level.unwrap() {
                Ok(level) => level as i64,
                Err(e) => return Some(Err(e.into())),
            };

            if self.last_rep_level == 0 && !self.levels.is_empty() {
                self.clear_pending = true;
                return Some(Ok(&self.levels));
            }
        }
    }
}

pub struct Levels {
    pub rep_levels: Vec<u32>,
    pub def_levels: Vec<u32>,
}

impl Levels {
    fn is_empty(&self) -> bool {
        self.rep_levels.is_empty()
    }

    fn clear(&mut self) {
        self.rep_levels.clear();
        self.def_levels.clear();
    }
}

#[allow(clippy::needless_range_loop)]
pub fn calculate_array_shape(
    shape: &mut [u32; ARRAY_NDIMS_LIMIT],
    max_rep_level: u32,
    rep_levels: &[u32],
) {
    if rep_levels.is_empty() {
        return;
    }

    let max_rep_level = max_rep_level as usize;

    let mut counts = [0_u32; ARRAY_NDIMS_LIMIT];

    match max_rep_level {
        // Common case optimization for small dimensions - unrolled loops
        1 => {
            shape[0] = rep_levels.len() as u32;
        }
        2 => {
            for &rep_level in rep_levels {
                let rep_level = rep_level.max(1) as usize;

                if rep_level < 2 {
                    counts[0] += 1;
                    counts[1] = 1;
                } else {
                    counts[1] += 1;
                }

                shape[1] = shape[1].max(counts[1]);
            }
            shape[0] = shape[0].max(counts[0]);
        }
        3 => {
            for &rep_level in rep_levels {
                let rep_level = rep_level.max(1) as usize;

                if rep_level < 2 {
                    counts[0] += 1;
                    counts[1] = 0;
                }
                if rep_level < 3 {
                    counts[1] += 1;
                    counts[2] = 1;
                } else {
                    counts[2] += 1;
                }

                shape[1] = shape[1].max(counts[1]);
                shape[2] = shape[2].max(counts[2]);
            }
            shape[0] = shape[0].max(counts[0]);
        }
        4 => {
            for &rep_level in rep_levels {
                let rep_level = rep_level.max(1) as usize;

                if rep_level < 2 {
                    counts[0] += 1;
                    counts[1] = 0;
                }
                if rep_level < 3 {
                    counts[1] += 1;
                    counts[2] = 0;
                }
                if rep_level < 4 {
                    counts[2] += 1;
                    counts[3] = 1;
                } else {
                    counts[3] += 1;
                }

                shape[1] = shape[1].max(counts[1]);
                shape[2] = shape[2].max(counts[2]);
                shape[3] = shape[3].max(counts[3]);
            }
            shape[0] = shape[0].max(counts[0]);
        }
        _ => {
            // General case for higher dimensions
            let mut last_rep_level = max_rep_level;
            for dim in 0..max_rep_level {
                shape[dim] = 1;
            }
            for &rep_level in rep_levels {
                let rep_level = rep_level.max(1) as usize;
                // Increment count at the deepest level (where actual values are)
                counts[rep_level - 1] += 1;
                shape[rep_level - 1] = shape[rep_level - 1].max(counts[rep_level - 1]);
                for dim in rep_level..last_rep_level {
                    // Reset counts for dimensions deeper than repetition level
                    counts[dim] = 1;
                }
                last_rep_level = rep_level;
            }
        }
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

pub fn append_array_null(aux_mem: &mut AcVec<u8>, data_mem: &[u8]) -> ParquetResult<()> {
    aux_mem.extend_from_slice(&data_mem.len().to_le_bytes())?;
    aux_mem.extend_from_slice(&HEADER_SIZE_NULL)?;
    Ok(())
}

pub fn append_array_nulls(
    aux_mem: &mut AcVec<u8>,
    data_mem: &[u8],
    count: usize,
) -> ParquetResult<()> {
    match count {
        0 => Ok(()),
        1 => append_array_null(aux_mem, data_mem),
        2 => {
            append_array_null(aux_mem, data_mem)?;
            append_array_null(aux_mem, data_mem)
        }
        3 => {
            append_array_null(aux_mem, data_mem)?;
            append_array_null(aux_mem, data_mem)?;
            append_array_null(aux_mem, data_mem)
        }
        4 => {
            append_array_null(aux_mem, data_mem)?;
            append_array_null(aux_mem, data_mem)?;
            append_array_null(aux_mem, data_mem)?;
            append_array_null(aux_mem, data_mem)
        }
        _ => {
            const ENTRY_SIZE: usize = 16; // 8 bytes offset + 8 bytes header

            let mut null_entry = [0u8; ENTRY_SIZE];
            null_entry[..8].copy_from_slice(&data_mem.len().to_le_bytes());
            let base = aux_mem.len();
            let total_bytes = count
                .checked_mul(ENTRY_SIZE)
                .ok_or_else(|| fmt_err!(Layout, "append_array_nulls overflow"))?;

            aux_mem.reserve(total_bytes)?;
            unsafe {
                let ptr = aux_mem.as_mut_ptr().add(base);
                for i in 0..count {
                    std::ptr::copy_nonoverlapping(
                        null_entry.as_ptr(),
                        ptr.add(i * ENTRY_SIZE),
                        ENTRY_SIZE,
                    );
                }
                let new_len = base
                    .checked_add(total_bytes)
                    .ok_or_else(|| fmt_err!(Layout, "append_array_nulls overflow"))?;
                aux_mem.set_len(new_len);
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::TestAllocatorState;

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
        let data = vec![f64::NAN, 2.0, f64::NAN, 4.0];
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
        let data = vec![1.0, f64::NAN, f64::NAN, 4.0, 5.0, 6.0, 7.0, 8.0];
        let rep_levels: Vec<u32> = RepLevelsIterator::new(shape.as_slice(), 8).collect();
        assert_eq!(rep_levels, vec![0, 3, 2, 3, 1, 3, 2, 3]);
        let def_levels: Vec<u32> = DefLevelsIterator::new(data.as_slice(), 5).collect();
        assert_eq!(def_levels, vec![5, 4, 4, 5, 5, 5, 5, 5]);
    }

    #[test]
    fn test_levels_iter_empty() -> ParquetResult<()> {
        let mut iter = LevelsIterator::try_new(0, 1, 1, &[], &[])?;
        assert_eq!(collect_levels(&mut iter)?, vec![]);
        Ok(())
    }

    #[test]
    fn test_levels_iter_1d_array() -> ParquetResult<()> {
        let rep_levels = vec![0_u32, 1, 1];
        let def_levels = vec![2_u32, 2, 2];
        let expected: Vec<(Vec<u32>, Vec<u32>)> = vec![(vec![0, 1, 1], vec![2, 2, 2])];
        assert_levels_iter(&rep_levels, &def_levels, &expected)?;
        Ok(())
    }

    #[test]
    fn test_levels_iter_1d_array2() -> ParquetResult<()> {
        let rep_levels = vec![0_u32, 1, 1, 0, 0];
        let def_levels = vec![2_u32, 2, 2, 2, 2];
        let expected: Vec<(Vec<u32>, Vec<u32>)> = vec![
            (vec![0, 1, 1], vec![2, 2, 2]),
            (vec![0], vec![2]),
            (vec![0], vec![2]),
        ];
        assert_levels_iter(&rep_levels, &def_levels, &expected)?;
        Ok(())
    }

    #[test]
    fn test_levels_iter_2d_array() -> ParquetResult<()> {
        let rep_levels = vec![0_u32, 2, 1, 2, 0];
        let def_levels = vec![3_u32, 2, 3, 2, 3];
        let expected: Vec<(Vec<u32>, Vec<u32>)> =
            vec![(vec![0, 2, 1, 2], vec![3, 2, 3, 2]), (vec![0], vec![3])];
        assert_levels_iter(&rep_levels, &def_levels, &expected)?;
        Ok(())
    }

    #[test]
    fn test_calculate_array_shape_empty() {
        let mut shape = [0_u32; ARRAY_NDIMS_LIMIT];
        let rep_levels = vec![];
        calculate_array_shape(&mut shape, 0, &rep_levels);
        assert_eq!(shape, [0_u32; ARRAY_NDIMS_LIMIT]);
    }

    #[test]
    fn test_calculate_array_shape_1d() {
        let mut shape = [0_u32; ARRAY_NDIMS_LIMIT];
        let rep_levels = vec![0, 1, 1, 1];
        calculate_array_shape(&mut shape, 1, &rep_levels);
        assert_eq!(shape[0..1], [4_u32]);
    }

    #[test]
    fn test_calculate_array_shape_2d() {
        let mut shape = [0_u32; ARRAY_NDIMS_LIMIT];
        let rep_levels = vec![0, 2, 2, 1, 2, 2, 1, 2, 2];
        calculate_array_shape(&mut shape, 2, &rep_levels);
        assert_eq!(shape[0..2], [3_u32, 3]);
    }

    #[test]
    fn test_calculate_array_shape_2d2() {
        let mut shape = [0_u32; ARRAY_NDIMS_LIMIT];
        let rep_levels = vec![0, 2, 2, 1, 2, 2];
        calculate_array_shape(&mut shape, 2, &rep_levels);
        assert_eq!(shape[0..2], [2_u32, 3]);
    }

    #[test]
    fn test_calculate_array_shape_3d() {
        let mut shape = [0_u32; ARRAY_NDIMS_LIMIT];
        let rep_levels = vec![0, 3, 3, 2, 3, 3, 1, 3, 3, 2, 3, 3];
        calculate_array_shape(&mut shape, 3, &rep_levels);
        assert_eq!(shape[0..3], [2_u32, 2, 3]);
    }

    #[test]
    fn test_calculate_array_shape_3d2() {
        let mut shape = [0_u32; ARRAY_NDIMS_LIMIT];
        let rep_levels = vec![0, 3, 3, 2, 3, 3];
        calculate_array_shape(&mut shape, 3, &rep_levels);
        assert_eq!(shape[0..3], [1_u32, 2, 3]);
    }

    #[test]
    fn test_calculate_array_shape_4d() {
        let mut shape = [0_u32; ARRAY_NDIMS_LIMIT];
        let rep_levels = vec![0, 4, 4, 3, 4, 4, 1, 4, 4, 3, 4, 4];
        calculate_array_shape(&mut shape, 4, &rep_levels);
        assert_eq!(shape[0..4], [2_u32, 1, 2, 3]);
    }

    #[test]
    fn test_calculate_array_shape_5d() {
        let mut shape = [0_u32; ARRAY_NDIMS_LIMIT];
        let rep_levels = vec![0, 4, 4, 3, 4, 4, 1, 4, 4, 3, 4, 4];
        calculate_array_shape(&mut shape, 5, &rep_levels);
        assert_eq!(shape[0..5], [2_u32, 1, 2, 3, 1]);
    }

    #[test]
    fn test_calculate_array_shape_edge_cases() {
        // Test single element array
        let mut shape = [0_u32; ARRAY_NDIMS_LIMIT];
        calculate_array_shape(&mut shape, 1, &[0]);
        assert_eq!(shape[0], 1);

        // Test jagged 2D array - [[1,2,3], [4,5]]
        // Jagged arrays are not yet supported by QDB query engine,
        // but calculate_array_shape() understands them
        let mut shape = [0_u32; ARRAY_NDIMS_LIMIT];
        let rep_levels = vec![0, 2, 2, 1, 2];
        calculate_array_shape(&mut shape, 2, &rep_levels);
        assert_eq!(shape[0], 2); // 2 sub-arrays
        assert_eq!(shape[1], 3); // max 3 elements in a sub-array

        // Test with all zeros (multiple new records)
        let mut shape = [0_u32; ARRAY_NDIMS_LIMIT];
        let rep_levels = vec![0, 0, 0];
        calculate_array_shape(&mut shape, 1, &rep_levels);
        assert_eq!(shape[0], 3); // three separate single-element arrays, max size is 3

        // Test large repetition levels
        let mut shape = [0_u32; ARRAY_NDIMS_LIMIT];
        let rep_levels = vec![0, 5, 5, 5, 5];
        calculate_array_shape(&mut shape, 5, &rep_levels);
        assert_eq!(shape[0..5], [1_u32, 1, 1, 1, 5]);
    }

    #[test]
    fn test_calculate_array_shape_large_array() {
        // Test performance with larger array (over CHUNK_SIZE)
        let mut shape = [0_u32; ARRAY_NDIMS_LIMIT];
        let mut rep_levels = vec![0]; // start with new record
        rep_levels.extend(std::iter::repeat_n(1, 100)); // continue in same array
        calculate_array_shape(&mut shape, 1, &rep_levels);
        assert_eq!(shape[0], 101);

        // Test 2D array with multiple chunks
        let mut shape = [0_u32; ARRAY_NDIMS_LIMIT];
        let mut rep_levels = vec![];
        for i in 0..10 {
            if i > 0 {
                rep_levels.push(1); // new sub-array
            } else {
                rep_levels.push(0); // new record
            }
            rep_levels.extend(std::iter::repeat_n(2, 15)); // elements in sub-array
        }
        calculate_array_shape(&mut shape, 2, &rep_levels);
        assert_eq!(shape[0], 10); // 10 sub-arrays
        assert_eq!(shape[1], 16); // 16 elements per sub-array (including first)
    }

    #[test]
    fn test_calculate_array_shape_max_dimensions() {
        // Test near maximum dimensionality
        let mut shape = [0_u32; ARRAY_NDIMS_LIMIT];
        let max_dim = 10_u32; // use 10 dimensions for testing
        let mut rep_levels = vec![0]; // new record

        // Add elements at deepest level
        rep_levels.extend(std::iter::repeat_n(max_dim, 5));
        calculate_array_shape(&mut shape, max_dim, &rep_levels);

        // Should have shape [1,1,1,...,1,6] with 6 at the deepest level
        for (i, &value) in shape[0..(max_dim as usize - 1)].iter().enumerate() {
            assert_eq!(value, 1, "dimension {} should be 1", i);
        }
        assert_eq!(shape[max_dim as usize - 1], 6);
    }

    fn assert_levels_iter(
        rep_levels: &[u32],
        def_levels: &[u32],
        expected: &Vec<(Vec<u32>, Vec<u32>)>,
    ) -> ParquetResult<()> {
        let max_rep_level = *rep_levels.iter().max().unwrap();
        let mut rep_levels_buf: Vec<u8> = vec![];
        encode_group_levels(
            &mut rep_levels_buf,
            rep_levels.iter().copied(),
            rep_levels.len(),
            max_rep_level,
            Version::V2,
        )?;

        let max_def_level = *def_levels.iter().max().unwrap();
        let mut def_levels_buf: Vec<u8> = vec![];
        encode_group_levels(
            &mut def_levels_buf,
            def_levels.iter().copied(),
            def_levels.len(),
            max_def_level,
            Version::V2,
        )?;

        let mut iter = LevelsIterator::try_new(
            rep_levels.len(),
            max_rep_level as i16,
            max_def_level as i16,
            &rep_levels_buf,
            &def_levels_buf,
        )?;
        assert_eq!(collect_levels(&mut iter)?, expected.as_slice());
        Ok(())
    }

    fn collect_levels(iter: &mut LevelsIterator) -> ParquetResult<Vec<(Vec<u32>, Vec<u32>)>> {
        let mut level_pairs: Vec<(Vec<u32>, Vec<u32>)> = vec![];
        while let Some(levels) = iter.next_levels() {
            let levels = levels?;
            level_pairs.push((levels.rep_levels.clone(), levels.def_levels.clone()));
        }
        Ok(level_pairs)
    }

    #[test]
    fn test_append_array_nulls() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let data_mem: &[u8] = &[1, 2, 3, 4]; // data_mem.len() = 4

        let mut aux = AcVec::new_in(allocator.clone());
        append_array_nulls(&mut aux, data_mem, 0).unwrap();
        assert_eq!(aux.len(), 0);

        // Test count = 1..=4 (fast path)
        for count in 1..=4 {
            let mut aux = AcVec::new_in(allocator.clone());
            append_array_nulls(&mut aux, data_mem, count).unwrap();
            assert_eq!(aux.len(), count * 16, "count={}", count);

            for i in 0..count {
                let offset = i * 16;
                let stored_offset = u64::from_le_bytes(aux[offset..offset + 8].try_into().unwrap());
                let stored_header =
                    u64::from_le_bytes(aux[offset + 8..offset + 16].try_into().unwrap());
                assert_eq!(stored_offset, 4, "count={}, i={}", count, i); // data_mem.len()
                assert_eq!(stored_header, 0, "count={}, i={}", count, i); // null header
            }
        }

        // Test count > 4 (general path)
        for count in [5, 10, 100] {
            let mut aux = AcVec::new_in(allocator.clone());
            append_array_nulls(&mut aux, data_mem, count).unwrap();
            assert_eq!(aux.len(), count * 16, "count={}", count);

            for i in 0..count {
                let offset = i * 16;
                let stored_offset = u64::from_le_bytes(aux[offset..offset + 8].try_into().unwrap());
                let stored_header =
                    u64::from_le_bytes(aux[offset + 8..offset + 16].try_into().unwrap());
                assert_eq!(stored_offset, 4, "count={}, i={}", count, i);
                assert_eq!(stored_header, 0, "count={}, i={}", count, i);
            }
        }

        let data_mem_large: Vec<u8> = vec![0; 1000];
        let mut aux = AcVec::new_in(allocator.clone());
        append_array_nulls(&mut aux, &data_mem_large, 3).unwrap();
        assert_eq!(aux.len(), 48);
        for i in 0..3 {
            let offset = i * 16;
            let stored_offset = u64::from_le_bytes(aux[offset..offset + 8].try_into().unwrap());
            assert_eq!(stored_offset, 1000);
        }
    }
}
