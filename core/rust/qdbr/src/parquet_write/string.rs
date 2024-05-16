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
use crate::util::{build_plain_page, encode_bool_iter, ExactSizedIter};

pub fn string_to_page(
    offsets: &[i64],
    data: &[u8],
    options: WriteOptions,
    type_: PrimitiveType,
    encoding: Encoding,
) -> parquet2::error::Result<Page> {
    let mut buffer = vec![];
    let mut null_count = 0;

    let lengths = offsets.iter().map(|offset| types::decode::<i32>( &data[*offset as usize..]));
    let nulls_iterator = lengths.clone().map(|len| {
        if len < 0 {
            null_count += 1;
            false
        } else {
            true
        }
    }) ;

    encode_bool_iter(&mut buffer, nulls_iterator, options.version)?;

    let definition_levels_byte_length = buffer.len();

    match encoding {
        Encoding::Plain => encode_plain(offsets, data, null_count, &mut buffer),
        Encoding::DeltaLengthByteArray => encode_delta(offsets, data, null_count, &mut buffer),
        other => Err(parquet2::error::Error::OutOfSpec(format!(
            "Encoding binary as {:?}",
            other
        )))?,
    }

    build_plain_page(
        buffer,
        offsets.len(),
        offsets.len(),
        null_count,
        0,
        definition_levels_byte_length,
        None, // do we really want a binary statistics?
        type_,
        options,
        encoding,
    ).map(Page::Data)
}

fn encode_non_null_values<'a, I: Iterator<Item = &'a [u8]>>(
    iter: I,
    buffer: &mut Vec<u8>,
) {
    iter.for_each(|x| {
        // BYTE_ARRAY: first 4 bytes denote length in littleendian.
        let len = (x.len() as u32).to_le_bytes();
        buffer.extend_from_slice(&len);
        buffer.extend_from_slice(x);
    })
}

fn encode_plain(offsets: &[i64], values: &[u8], null_count: usize, buffer: &mut Vec<u8>) {
    let len_before = buffer.len();
    let length = offsets.len() - null_count;
    let capacity = (offsets[offsets.len() - 1] - offsets[0]) as usize - (length * size_of::<i32>());
    buffer.reserve(capacity);

    let non_null_values_iter = offsets.iter().map(|offset| (*offset, types::decode::<i32>( &values[*offset as usize..])))
        .filter(|(offset, len)| *len >= 0)
        .map(|(offset, len)| &values[offset as usize + size_of::<i32>() .. (offset + len as i64) as usize]);
    encode_non_null_values(non_null_values_iter, buffer);
    // Ensure we allocated properly.
    debug_assert_eq!(buffer.len() - len_before, capacity);
}

fn encode_delta(offsets: &[i64], values: &[u8], null_count: usize, buffer: &mut Vec<u8>) {
    let lengths = offsets.iter().map(|offset| types::decode::<i32>( &values[*offset as usize..]) as i64)
        .filter(|len| *len >= 0);
    let length = offsets.len() - null_count;
    let lengths = ExactSizedIter::new(lengths, length);

    delta_bitpacked::encode(lengths, buffer);

    if offsets.len() > 0 {
        let capacity = (offsets[offsets.len() - 1] - offsets[0]) as usize - (length * size_of::<i32>());
        buffer.reserve(capacity);
    }
    for row in 0..offsets.len() {
        let offset = offsets[row];
        let len = types::decode::<i64>( &values[offset as usize..]);
        let data = &values[offset as usize + size_of::<i32>() .. (offset + len) as usize];
        let data: &[u16] = unsafe { transmute(data) };
        let utf8 = String::from_utf16(data).expect("utf16 string");
        buffer.extend_from_slice(utf8.as_bytes());
    }
}
