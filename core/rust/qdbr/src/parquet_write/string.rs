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

use parquet2::encoding::{delta_bitpacked, Encoding};
use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;
use parquet2::types;

use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util::{
    build_plain_page, encode_bool_iter, transmute_slice, ExactSizedIter,
};
use crate::parquet_write::{ParquetError, ParquetResult};

use super::util::BinaryMaxMin;

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

    encode_bool_iter(&mut buffer, deflevels_iter, options.version)?;

    let definition_levels_byte_length = buffer.len();

    let mut stats = BinaryMaxMin::new(&primitive_type);

    match encoding {
        Encoding::Plain => {
            encode_plain(&utf16_slices, &mut buffer, &mut stats);
            Ok(())
        }
        Encoding::DeltaLengthByteArray => {
            encode_delta(&utf16_slices, null_count, &mut buffer, &mut stats);
            Ok(())
        }
        other => Err(ParquetError::OutOfSpec(format!(
            "Encoding string as {:?}",
            other
        ))),
    }?;

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

fn encode_plain(utf16_slices: &[Option<&[u16]>], buffer: &mut Vec<u8>, stats: &mut BinaryMaxMin) {
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
    stats: &mut BinaryMaxMin,
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
