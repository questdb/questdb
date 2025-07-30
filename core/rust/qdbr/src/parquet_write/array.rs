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
use parquet2::write::Version;
use qdb_core::col_driver::ArrayAuxEntry;

use crate::parquet_write::util;
use parquet2::encoding::{delta_bitpacked, Encoding};
use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;

use super::util::BinaryMaxMin;
use crate::allocator::AcVec;
use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util::{
    build_plain_page, encode_bool_iter, encode_levels_iter, ExactSizedIter,
};
use parquet2::schema::types::PhysicalType;
use std::slice;

const HEADER_SIZE_NULL: [u8; 8] = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];

// encodes array as nested lists
pub fn array_to_page(
    dim: i32,
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

    let num_rows = column_top + aux.len();
    let mut buffer = vec![];
    let mut null_count = 0usize;

    let aux: &[ArrayAuxEntry] = unsafe { mem::transmute(aux) };

    let shape_size = 4 * dim as usize;
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

    let num_elements = 0usize;

    let replevels_iter = arr_slices
        .iter()
        .filter(|arr| arr.is_some())
        .map(|arr| arr.unwrap())
        .flat_map(|arr| {
            // TODO(puzpuzpuz): proper rep levels mapping instead of 1d array
            let _shape = arr.0;
            let data = arr.1;
            (0..data.len()).map(|i| if i == 0 { 0 } else { 1 })
        });
    encode_levels_iter(
        &mut buffer,
        replevels_iter,
        num_elements,
        dim as u32,
        options.version,
    )?;

    let repetition_levels_byte_length = buffer.len();

    let deflevels_iter =
        (0..num_rows).map(|i| i >= column_top && arr_slices[i - column_top].is_some());
    let deflevels_len = deflevels_iter.size_hint().1.unwrap();
    encode_bool_iter(&mut buffer, deflevels_iter, deflevels_len, options.version)?;

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
    build_page(
        dim,
        buffer,
        num_rows,
        null_count,
        repetition_levels_byte_length,
        definition_levels_byte_length,
        options,
        encoding,
    )
    .map(Page::Data)
}

fn encode_data_plain(arr_slices: &[Option<(&[i32], &[f64])>], buffer: &mut Vec<u8>) {
    for (_shape, data) in arr_slices.iter().filter_map(|&option| option) {
        let data: &[u8] = unsafe { transmute_slice_f64(data) };
        buffer.extend_from_slice(data);
    }
}

pub unsafe fn transmute_slice_f64(slice: &[f64]) -> &[u8] {
    if slice.is_empty() {
        &[]
    } else {
        slice::from_raw_parts(
            slice.as_ptr() as *const u8,
            slice.len() * mem::size_of::<f64>(),
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn build_page(
    dim: i32,
    buffer: Vec<u8>,
    num_rows: usize,
    null_count: usize,
    repetition_levels_byte_length: usize,
    definition_levels_byte_length: usize,
    options: WriteOptions,
    encoding: Encoding,
) -> ParquetResult<DataPage> {
    let header = match options.version {
        Version::V1 => DataPageHeader::V1(DataPageHeaderV1 {
            num_values: num_rows as i32,
            encoding: encoding.into(),
            definition_level_encoding: Encoding::Rle.into(),
            repetition_level_encoding: Encoding::Rle.into(),
            statistics: None,
        }),
        Version::V2 => DataPageHeader::V2(DataPageHeaderV2 {
            num_values: num_rows as i32,
            encoding: encoding.into(),
            num_nulls: null_count as i32,
            num_rows: num_rows as i32,
            definition_levels_byte_length: definition_levels_byte_length as i32,
            repetition_levels_byte_length: repetition_levels_byte_length as i32,
            is_compressed: Some(options.compression != CompressionOptions::Uncompressed),
            statistics: None,
        }),
    };
    // TODO(puzpuzpuz): how to avoid primitive type here????
    let t = PrimitiveType::from_physical("fdfd".to_string(), PhysicalType::Double);
    Ok(DataPage::new(
        header,
        buffer,
        Descriptor {
            primitive_type: t,
            max_def_level: 1,
            max_rep_level: dim as i16,
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
    let deflevels_len = deflevels_iter.size_hint().1.unwrap();
    encode_bool_iter(&mut buffer, deflevels_iter, deflevels_len, options.version)?;

    let definition_levels_byte_length = buffer.len();

    let mut stats = BinaryMaxMin::new(&primitive_type);

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

fn encode_raw_plain(arr_slices: &[Option<&[u8]>], buffer: &mut Vec<u8>, stats: &mut BinaryMaxMin) {
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
    stats: &mut BinaryMaxMin,
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
