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

use std::mem::size_of;
use parquet2::encoding::{delta_bitpacked, Encoding};
use parquet2::page::DataPage;
use parquet2::schema::types::PrimitiveType;
use parquet2::types;
use crate::parquet_write::file::WriteOptions;
use crate::util::{build_plain_page, encode_bool_iter, ExactSizedIter};

pub fn binary_to_page(
    offsets: &[i64],
    data: &[u8],
    options: WriteOptions,
    type_: PrimitiveType,
    encoding: Encoding,
) -> parquet2::error::Result<DataPage> {
    let mut buffer = vec![];
    let mut null_count = 0;

    let lengths = offsets.iter().map(|offset| types::decode::<i64>( &data[*offset as usize..]));
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
    )
}

fn encode_delta(offsets: &[i64], values: &[u8], null_count: usize, buffer: &mut Vec<u8>) {
    let lengths = offsets.iter().map(|offset| types::decode::<i64>( &values[*offset as usize..]))
        .filter(|len| *len >= 0);
    let length = offsets.len() - null_count;
    let lengths = ExactSizedIter::new(lengths, length);

    delta_bitpacked::encode(lengths, buffer);

    if offsets.len() > 0 {
        let capacity = (offsets[offsets.len() - 1] - offsets[0]) as usize - (length * size_of::<i64>());
        buffer.reserve(capacity);
    }
    for row in 0..offsets.len() {
        let offset = offsets[row];
        let len = types::decode::<i64>( &values[offset as usize..]);
        let data = &values[offset as usize + size_of::<i64>() .. (offset + len) as usize];
        buffer.extend_from_slice(data);
    }
}