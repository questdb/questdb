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

use parquet2::encoding::{delta_bitpacked, Encoding};
use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;

use super::util::BinaryMaxMin;
use crate::allocator::AcVec;
use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util::{build_plain_page, encode_bool_iter, ExactSizedIter};

const HEADER_SIZE_NULL: [u8; 8] = [
    0u8,
    0u8,
    0u8,
    0u8,
    0u8,
    0u8,
    0u8,
    0u8,
];

#[repr(C, packed)]
struct ArrayAuxEntry {
    offset: u64,
    size: u64,
}

pub fn array_to_page(
    aux: &[i64],
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
            let size = entry.size as usize;
            let offset = entry.offset as usize;
            if size > 0 {
                assert!(
                    offset + size <= data.len(),
                    "Data corruption in ARRAY column"
                );
                Some(&data[offset..][..size])
            } else { // null                
                null_count += 1;
                None
            }
        })
        .collect();

    let deflevels_iter =
        (0..num_rows).map(|i| i >= column_top && arr_slices[i - column_top].is_some());
    encode_bool_iter(&mut buffer, deflevels_iter, options.version)?;

    let definition_levels_byte_length = buffer.len();

    let mut stats = BinaryMaxMin::new(&primitive_type);

    match encoding {
        Encoding::Plain => {
            encode_plain(&arr_slices, &mut buffer, &mut stats);
        }
        Encoding::DeltaLengthByteArray => {
            encode_delta(&arr_slices, null_count, &mut buffer, &mut stats);
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

fn encode_plain(arr_slices: &[Option<&[u8]>], buffer: &mut Vec<u8>, stats: &mut BinaryMaxMin) {
    for arr in arr_slices.iter().filter_map(|&option| option) {
        let len = (arr.len() as u32).to_le_bytes();
        buffer.extend_from_slice(&len);
        buffer.extend_from_slice(arr);
        stats.update(arr);
    }
}

fn encode_delta(
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

pub fn append_array(
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
    for _ in 0..count {
        append_array_null(aux_mem, data_mem)?;
    }
    Ok(())
}
