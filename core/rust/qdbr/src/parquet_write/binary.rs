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

use std::collections::HashSet;
use std::mem::size_of;

use parquet2::bloom_filter::hash_byte;
use parquet2::encoding::{delta_bitpacked, Encoding};
use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;
use parquet2::types;

use super::util::BinaryMaxMinStats;
use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util::{build_plain_page, encode_primitive_def_levels, ExactSizedIter};

pub fn binary_to_page(
    offsets: &[i64],
    data: &[u8],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    encoding: Encoding,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
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
            encode_plain(
                offsets,
                data,
                &mut buffer,
                &mut stats,
                bloom_hashes.as_deref_mut(),
            );
            Ok(())
        }
        Encoding::DeltaLengthByteArray => {
            encode_delta(
                offsets,
                data,
                null_count,
                &mut buffer,
                &mut stats,
                bloom_hashes,
            );
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
    mut bloom_hashes: Option<&mut HashSet<u64>>,
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
        if let Some(ref mut h) = bloom_hashes {
            h.insert(hash_byte(value));
        }
    }
}

fn encode_delta(
    offsets: &[i64],
    values: &[u8],
    null_count: usize,
    buffer: &mut Vec<u8>,
    stats: &mut BinaryMaxMinStats,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
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
        if let Some(ref mut h) = bloom_hashes {
            h.insert(hash_byte(value));
        }
    }
}
