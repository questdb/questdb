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

    let lengths = offsets.iter().map(|offset| {
        let offset = *offset as usize;
        types::decode::<i32>(&data[offset..offset + size_of::<i32>()])
    });
    let nulls_iterator = lengths.clone().map(|len| {
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

fn encode_non_null_values<'a, I: Iterator<Item = &'a [u8]>>(iter: I, buffer: &mut Vec<u8>) {
    iter.for_each(|x| {
        let data: &[u16] = unsafe { transmute(x) };
        let utf8 = String::from_utf16(data).expect("utf16 string");
        // BYTE_ARRAY: first 4 bytes denote length in littleendian.
        let len = (utf8.len() as u32).to_le_bytes();
        buffer.extend_from_slice(&len);
        buffer.extend_from_slice(utf8.as_bytes());
    })
}

fn encode_plain(offsets: &[i64], values: &[u8], null_count: usize, buffer: &mut Vec<u8>) {
    let non_null_values_iter = offsets
        .iter()
        .map(|offset| {
            let offset = *offset as usize;
            (
                offset,
                types::decode::<i32>(&values[offset..offset + size_of::<i32>()]),
            )
        })
        .filter(|(_offset, len)| *len >= 0)
        .map(|(offset, len)| {
            &values[offset + size_of::<i32>()..offset + size_of::<i32>() + len as usize]
        });

    encode_non_null_values(non_null_values_iter, buffer);
}

fn encode_delta(offsets: &[i64], values: &[u8], null_count: usize, buffer: &mut Vec<u8>) {
    let lengths = offsets
        .iter()
        .map(|offset| {
            let offset = *offset as usize;
            types::decode::<i32>(&values[offset..offset + size_of::<i32>()]) as i64
        })
        .filter(|len| *len >= 0);
    let length = offsets.len() - null_count;
    let lengths = ExactSizedIter::new(lengths, length);

    delta_bitpacked::encode(lengths, buffer);

    if !offsets.is_empty() {
        let capacity =
            (offsets[offsets.len() - 1] - offsets[0]) as usize - (length * size_of::<i32>());
        buffer.reserve(capacity);
    }
    for offset in offsets {
        let offset = *offset as usize;
        let len = types::decode::<i32>(&values[offset..offset + size_of::<i32>()]);
        let data = &values[offset + size_of::<i32>()..offset + len as usize];
        let data: &[u16] = unsafe { transmute(data) };
        let utf8 = String::from_utf16(data).expect("utf16 string");
        buffer.extend_from_slice(utf8.as_bytes());
    }
}
