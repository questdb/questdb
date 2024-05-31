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

use std::mem::{size_of, transmute};

use parquet2::encoding::{delta_bitpacked, Encoding};
use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;
use parquet2::types;

use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util::{build_plain_page, encode_bool_iter, ExactSizedIter};
use crate::parquet_write::{ParquetError, ParquetResult};

pub fn string_to_page(
    offsets: &[i64],
    data: &[u8],
    options: WriteOptions,
    type_: PrimitiveType,
    encoding: Encoding,
) -> ParquetResult<Page> {
    let mut buffer = vec![];
    let mut null_count = 0;

    let nulls_iterator = offsets.iter().map(|offset| {
        let offset = *offset as usize;
        let len = types::decode::<i32>(&data[offset..offset + size_of::<i32>()]);
        if len < 0 {
            null_count += 1;
            false
        } else {
            true
        }
    });

    encode_bool_iter(&mut buffer, nulls_iterator, options.version)?;

    let definition_levels_byte_length = buffer.len();

    match encoding {
        Encoding::Plain => encode_plain(offsets, data, null_count, &mut buffer),
        Encoding::DeltaLengthByteArray => encode_delta(offsets, data, null_count, &mut buffer),
        other => Err(ParquetError::OutOfSpec(format!(
            "Encoding string as {:?}",
            other
        )))?,
    }

    build_plain_page(
        buffer,
        offsets.len(),
        offsets.len(),
        null_count,
        definition_levels_byte_length,
        None, // do we really want a binary statistics?
        type_,
        options,
        encoding,
    )
    .map(Page::Data)
}

fn encode_plain(offsets: &[i64], values: &[u8], null_count: usize, buffer: &mut Vec<u8>) {
    println!("\n\nstring::encode_plain");
    let size_of_header = size_of::<i32>();
    for offset in offsets {
        let offset = usize::try_from(*offset).expect("invalid offset value in string aux column");
        let len_raw = types::decode::<i32>(&values[offset..offset + size_of_header]);
        println!("len_raw {len_raw}");
        if len_raw < 0 {
            continue;
        }
        let len = len_raw as usize;
        let value_tail: &[u16] = unsafe { transmute(&values[offset + size_of_header..]) };
        let value = &value_tail[..len];
        let utf8 = String::from_utf16(value).expect("utf16 string");
        println!("utf8 {utf8}");
        // BYTE_ARRAY: first 4 bytes denote length in little-endian.
        let encoded_len = (utf8.len() as u32).to_le_bytes();
        buffer.extend_from_slice(&encoded_len);
        buffer.extend_from_slice(utf8.as_bytes());
    }
    println!("string::encode_plain done\n");
}

fn encode_delta(offsets: &[i64], values: &[u8], null_count: usize, buffer: &mut Vec<u8>) {
    let size_of_header = size_of::<i32>();
    let row_count = offsets.len();

    if row_count == 0 {
        delta_bitpacked::encode(std::iter::empty(), buffer);
        return;
    }

    // Reserve buffer capacity for performance reasons only. No effect on correctness.
    {
        let last_offset = offsets[row_count - 1] as usize;
        let last_size = types::decode::<i32>(&values[last_offset..last_offset + size_of_header]);
        let last_size = if last_size > 0 { 2 * last_size } else { 0 } as i64;
        let capacity = (offsets[row_count - 1] - offsets[0] + last_size) as usize
            - (row_count * size_of_header);
        buffer.reserve(capacity);
    }

    // TODO: lengths are broken because they are UTF-16 lengths and the written data is UTF-8
    let lengths = offsets
        .iter()
        .map(|offset| {
            let offset = *offset as usize;
            2 * types::decode::<i32>(&values[offset..offset + size_of_header]) as i64
        })
        .filter(|len| *len >= 0);
    let non_null_count = row_count - null_count;
    let lengths = ExactSizedIter::new(lengths, non_null_count);
    delta_bitpacked::encode(lengths, buffer);

    for offset in offsets {
        let offset = *offset as usize;
        let len_raw = types::decode::<i32>(&values[offset..offset + size_of_header]);
        if len_raw < 0 {
            continue;
        }
        let value_offset = offset + size_of_header;
        let len = (len_raw as usize) * size_of::<u16>();
        let data = &values[value_offset..value_offset + len];
        let data: &[u16] = unsafe { transmute(data) };
        let utf8 = String::from_utf16(data).expect("utf16 string");
        buffer.extend_from_slice(utf8.as_bytes());
    }
}
