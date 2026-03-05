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

use super::util::BinaryMaxMinStats;
use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util::{
    build_plain_page, encode_dict_rle_pages, encode_primitive_def_levels, transmute_slice,
    ExactSizedIter,
};
use parquet2::encoding::{delta_bitpacked, Encoding};
use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;
use parquet2::types;
use parquet2::write::DynIter;
use rapidhash::RapidHashMap;

const SIZE_OF_HEADER: usize = std::mem::size_of::<i32>();

pub fn string_to_page(
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

    let utf16_slices: Vec<Option<&[u16]>> = offsets
        .iter()
        .map(|offset| {
            let offset =
                usize::try_from(*offset).expect("invalid offset value in string aux column");
            let maybe_utf16 = get_utf16(&data[offset..]);
            if maybe_utf16.is_none() {
                null_count += 1;
            }
            maybe_utf16
        })
        .collect();

    let deflevels_iter =
        (0..num_rows).map(|i| i >= column_top && utf16_slices[i - column_top].is_some());
    encode_primitive_def_levels(&mut buffer, deflevels_iter, num_rows, options.version)?;

    let definition_levels_byte_length = buffer.len();

    let mut stats = BinaryMaxMinStats::new(&primitive_type);

    match encoding {
        Encoding::Plain => {
            encode_plain(&utf16_slices, &mut buffer, &mut stats);
        }
        Encoding::DeltaLengthByteArray => {
            encode_delta(&utf16_slices, null_count, &mut buffer, &mut stats);
        }
        _ => {
            return Err(fmt_err!(
                Unsupported,
                "unsupported encoding {encoding:?} while writing a string column"
            ))
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
        false,
    )
    .map(Page::Data)
}

pub fn string_to_dict_pages(
    offsets: &[i64],
    data: &[u8],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
) -> ParquetResult<DynIter<'static, ParquetResult<Page>>> {
    let num_rows = column_top + offsets.len();
    let mut null_count = 0;

    // Deduplicate on raw UTF-16 slices (zero-copy from data buffer),
    // then convert only unique entries to UTF-8.
    let mut dict_map: RapidHashMap<&[u16], u32> = RapidHashMap::default();
    let mut dict_entries: Vec<&[u16]> = Vec::new();
    let mut keys: Vec<u32> = Vec::with_capacity(offsets.len());
    let mut is_not_null: Vec<bool> = Vec::with_capacity(offsets.len());

    for offset in offsets {
        let offset =
            usize::try_from(*offset).expect("invalid offset value in string aux column");
        match get_utf16(&data[offset..]) {
            Some(utf16) => {
                let next_id = dict_entries.len() as u32;
                let key = *dict_map.entry(utf16).or_insert_with(|| {
                    dict_entries.push(utf16);
                    next_id
                });
                keys.push(key);
                is_not_null.push(true);
            }
            None => {
                null_count += 1;
                is_not_null.push(false);
            }
        }
    }

    // Convert only unique dict entries to UTF-8 and build dict buffer
    let mut dict_buffer = Vec::new();
    let mut stats = if options.write_statistics {
        Some(BinaryMaxMinStats::new(&primitive_type))
    } else {
        None
    };
    for utf16 in &dict_entries {
        let utf8 = String::from_utf16(utf16).expect("utf16 string");
        let utf8_bytes = utf8.as_bytes();
        dict_buffer.extend_from_slice(&(utf8_bytes.len() as u32).to_le_bytes());
        dict_buffer.extend_from_slice(utf8_bytes);
        if let Some(ref mut s) = stats {
            s.update(utf8_bytes);
        }
    }

    // Encode data page
    let total_null_count = column_top + null_count;
    let mut data_buffer = Vec::new();

    let def_levels = (0..num_rows).map(|i| i >= column_top && is_not_null[i - column_top]);
    encode_primitive_def_levels(&mut data_buffer, def_levels, num_rows, options.version)?;
    let definition_levels_byte_length = data_buffer.len();

    let non_null_len = offsets.len() - null_count;
    let statistics = stats.map(|s| s.into_parquet_stats(total_null_count));

    encode_dict_rle_pages(
        dict_buffer,
        dict_entries.len(),
        keys,
        non_null_len,
        data_buffer,
        definition_levels_byte_length,
        num_rows,
        total_null_count,
        statistics,
        primitive_type,
        options,
        false,
    )
}

fn encode_plain(
    utf16_slices: &[Option<&[u16]>],
    buffer: &mut Vec<u8>,
    stats: &mut BinaryMaxMinStats,
) {
    for utf16 in utf16_slices.iter().filter_map(|&option| option) {
        let utf8 = String::from_utf16(utf16).expect("utf16 string");
        // BYTE_ARRAY: first 4 bytes denote length in little-endian.
        let encoded_len = (utf8.len() as u32).to_le_bytes();
        buffer.extend_from_slice(&encoded_len);
        let value = utf8.as_bytes();
        buffer.extend_from_slice(value);
        stats.update(value);
    }
}

fn encode_delta(
    utf16_slices: &[Option<&[u16]>],
    null_count: usize,
    buffer: &mut Vec<u8>,
    stats: &mut BinaryMaxMinStats,
) {
    let lengths = utf16_slices
        .iter()
        .filter_map(|&option| option)
        .map(|utf16| compute_utf8_length(utf16) as i64);
    let lengths = ExactSizedIter::new(lengths, utf16_slices.len() - null_count);
    delta_bitpacked::encode(lengths, buffer);
    for utf16 in utf16_slices.iter().filter_map(|&option| option) {
        let utf8 = String::from_utf16(utf16).expect("utf16 string");
        let value = utf8.as_bytes();
        buffer.extend_from_slice(value);
        stats.update(value);
    }
}

fn get_utf16(entry_tail: &[u8]) -> Option<&[u16]> {
    let (header, value_tail) = entry_tail.split_at(SIZE_OF_HEADER);
    let len_raw = types::decode::<i32>(header);
    if len_raw < 0 {
        return None;
    }
    // SAFETY: Data originates from JNI/Java memory-mapped column data, which is page-aligned.
    // The byte content represents valid `u16` values.
    let utf16_tail: &[u16] = unsafe { transmute_slice(value_tail) };
    let char_count = len_raw as usize;
    Some(&utf16_tail[..char_count])
}

fn compute_utf8_length(utf16: &[u16]) -> usize {
    utf16
        .iter()
        // Filter out low surrogates
        .filter(|&char| !(0xDC00..=0xDFFF).contains(char))
        .fold(0, |len, &char| {
            len + if char <= 0x7F {
                1 // ASCII char
            } else if char <= 0x7FF {
                2 // Two-byte UTF-8
            } else if !(0xD800..=0xDBFF).contains(&char) {
                3 // Not a high surrogate, so must be a three-byte UTF-8
            } else {
                4 // High surrogate
            }
        })
}
