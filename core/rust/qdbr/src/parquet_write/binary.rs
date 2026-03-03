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

use std::mem::size_of;

use parquet2::encoding::hybrid_rle::encode_u32;
use parquet2::encoding::{delta_bitpacked, Encoding};
use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;
use parquet2::types;
use parquet2::write::DynIter;
use rapidhash::RapidHashMap;

use super::util::BinaryMaxMinStats;
use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util::{
    build_plain_page, dict_pages_iter, encode_primitive_def_levels, ExactSizedIter,
};

pub fn binary_to_page(
    offsets: &[i64],
    data: &[u8],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    encoding: Encoding,
) -> ParquetResult<Page> {
    let num_rows = column_top + offsets.len();
    let mut buffer = vec![];
    let mut null_count = 0;

    let deflevels_iter = (0..num_rows).map(|i| {
        let len = if i < column_top {
            -1
        } else {
            let offset = offsets[i - column_top] as usize;
            let len = types::decode::<i64>(&data[offset..offset + size_of::<i64>()]);
            if len < 0 {
                null_count += 1;
            }
            len
        };
        len >= 0
    });
    encode_primitive_def_levels(&mut buffer, deflevels_iter, num_rows, options.version)?;

    let definition_levels_byte_length = buffer.len();

    let mut stats = BinaryMaxMinStats::new(&primitive_type);
    match encoding {
        Encoding::Plain => {
            encode_plain(offsets, data, &mut buffer, &mut stats);
            Ok(())
        }
        Encoding::DeltaLengthByteArray => {
            encode_delta(offsets, data, null_count, &mut buffer, &mut stats);
            Ok(())
        }
        _ => Err(fmt_err!(
            Unsupported,
            "unsupported encoding {encoding:?} while writing a binary column"
        )),
    }?;

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
        false,
    )
    .map(Page::Data)
}

fn encode_plain(
    offsets: &[i64],
    values: &[u8],
    buffer: &mut Vec<u8>,
    stats: &mut BinaryMaxMinStats,
) {
    let size_of_header = size_of::<i64>();

    for offset in offsets {
        let offset = usize::try_from(*offset).expect("invalid offset value in binary aux column");
        let len = types::decode::<i64>(&values[offset..offset + size_of_header]);
        if len < 0 {
            continue;
        }
        let value_offset = offset + size_of_header;
        let value = &values[value_offset..value_offset + len as usize];
        let encoded_len = (len as u32).to_le_bytes();
        buffer.extend_from_slice(&encoded_len);
        buffer.extend_from_slice(value);
        stats.update(value);
    }
}

fn encode_delta(
    offsets: &[i64],
    values: &[u8],
    null_count: usize,
    buffer: &mut Vec<u8>,
    stats: &mut BinaryMaxMinStats,
) {
    let size_of_header = size_of::<i64>();
    let row_count = offsets.len();

    if row_count == 0 {
        delta_bitpacked::encode(std::iter::empty(), buffer);
        return;
    }

    // Reserve buffer capacity for performance reasons only. No effect on correctness.
    {
        let last_offset = offsets[row_count - 1] as usize;
        let last_size = types::decode::<i64>(&values[last_offset..last_offset + size_of_header]);
        let last_size = if last_size > 0 { last_size } else { 0 };
        let capacity = (offsets[row_count - 1] - offsets[0] + last_size) as usize
            - ((row_count - 1) * size_of_header);
        buffer.reserve(capacity);
    }

    let lengths = offsets
        .iter()
        .map(|offset| {
            let offset = *offset as usize;
            types::decode::<i64>(&values[offset..offset + size_of_header])
        })
        .filter(|len| *len >= 0);
    let lengths = ExactSizedIter::new(lengths, row_count - null_count);

    delta_bitpacked::encode(lengths, buffer);

    for offset in offsets {
        let offset = *offset as usize;
        let len = types::decode::<i64>(&values[offset..offset + size_of_header]);
        if len < 0 {
            continue;
        }
        let value_offset = offset + size_of_header;
        let value = &values[value_offset..value_offset + len as usize];
        buffer.extend_from_slice(value);
        stats.update(value);
    }
}

pub fn binary_to_dict_pages(
    offsets: &[i64],
    data: &[u8],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
) -> ParquetResult<DynIter<'static, ParquetResult<Page>>> {
    let num_rows = column_top + offsets.len();
    let size_of_header = size_of::<i64>();
    let mut null_count = 0;

    // Collect byte slices
    let byte_slices: Vec<Option<&[u8]>> = offsets
        .iter()
        .map(|offset| {
            let offset =
                usize::try_from(*offset).expect("invalid offset value in binary aux column");
            let len = types::decode::<i64>(&data[offset..offset + size_of_header]);
            if len < 0 {
                null_count += 1;
                None
            } else {
                let value_offset = offset + size_of_header;
                Some(&data[value_offset..value_offset + len as usize])
            }
        })
        .collect();

    // Build dictionary
    let mut dict_map: RapidHashMap<&[u8], u32> = RapidHashMap::default();
    let mut dict_entries: Vec<&[u8]> = Vec::new();
    let mut keys: Vec<u32> = Vec::with_capacity(offsets.len());
    let mut total_keys_bytes = 0usize;

    for slice in &byte_slices {
        if let Some(s) = slice {
            let next_id = dict_entries.len() as u32;
            let key = *dict_map.entry(s).or_insert_with(|| {
                total_keys_bytes += 4 + s.len();
                dict_entries.push(s);
                next_id
            });
            keys.push(key);
        }
    }

    // Build dict buffer (length-prefixed bytes)
    let mut dict_buffer = Vec::with_capacity(total_keys_bytes);
    let mut stats = if options.write_statistics {
        Some(BinaryMaxMinStats::new(&primitive_type))
    } else {
        None
    };
    for &entry in &dict_entries {
        dict_buffer.extend_from_slice(&(entry.len() as u32).to_le_bytes());
        dict_buffer.extend_from_slice(entry);
        if let Some(ref mut s) = stats {
            s.update(entry);
        }
    }

    // Encode data page
    let total_null_count = column_top + null_count;
    let mut data_buffer = Vec::new();

    let def_levels = (0..num_rows).map(|i| {
        if i < column_top {
            false
        } else {
            byte_slices[i - column_top].is_some()
        }
    });
    encode_primitive_def_levels(&mut data_buffer, def_levels, num_rows, options.version)?;
    let definition_levels_byte_length = data_buffer.len();

    let max_key = if dict_entries.is_empty() {
        0u32
    } else {
        (dict_entries.len() - 1) as u32
    };
    let bits_per_key = super::util::bit_width(max_key as u64);
    let non_null_len = offsets.len() - null_count;
    data_buffer.push(bits_per_key);
    encode_u32(
        &mut data_buffer,
        keys.into_iter(),
        non_null_len,
        bits_per_key as u32,
    )?;

    let data_page = build_plain_page(
        data_buffer,
        num_rows,
        total_null_count,
        definition_levels_byte_length,
        stats.map(|s| s.into_parquet_stats(total_null_count)),
        primitive_type,
        options,
        Encoding::RleDictionary,
        false,
    )?;

    let unique_count = if dict_buffer.is_empty() {
        0
    } else {
        dict_entries.len()
    };
    Ok(dict_pages_iter(dict_buffer, unique_count, data_page))
}
