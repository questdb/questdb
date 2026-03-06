use std::collections::HashSet;

use crate::parquet::error::ParquetResult;
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util::{
    build_plain_page, encode_dict_rle_pages, encode_primitive_def_levels,
};
use parquet2::bloom_filter::hash_byte;
use parquet2::encoding::Encoding;
use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;
use parquet2::write::DynIter;
use rapidhash::RapidHashMap;

use super::util::BinaryMaxMinStats;

fn encode_plain_be<const N: usize>(
    data: &[[u8; N]],
    buffer: &mut Vec<u8>,
    null_value: [u8; N],
    stats: &mut BinaryMaxMinStats,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) {
    for x in data.iter().filter(|&&x| x != null_value) {
        let mut reversed = [0u8; N];
        for (i, &b) in x.iter().rev().enumerate() {
            reversed[i] = b;
        }
        buffer.extend_from_slice(&reversed);
        stats.update(&reversed);
        if let Some(ref mut h) = bloom_hashes {
            h.insert(hash_byte(reversed));
        }
    }
}

fn encode_plain<const N: usize>(
    data: &[[u8; N]],
    buffer: &mut Vec<u8>,
    null_value: [u8; N],
    stats: &mut BinaryMaxMinStats,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) {
    for x in data.iter().filter(|&&x| x != null_value) {
        buffer.extend_from_slice(x);
        stats.update(x);
        if let Some(ref mut h) = bloom_hashes {
            h.insert(hash_byte(x));
        }
    }
}

pub fn bytes_to_page<const N: usize>(
    data: &[[u8; N]],
    reverse: bool,
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<Page> {
    let num_rows = column_top + data.len();
    let null_value = {
        let mut null_value = [0u8; N];
        let long_as_bytes = i64::MIN.to_le_bytes();
        for i in 0..N {
            null_value[i] = long_as_bytes[i % long_as_bytes.len()];
        }
        null_value
    };
    let mut buffer = vec![];
    let mut null_count = 0;

    let deflevels_iter = (0..num_rows).map(|i| {
        if i < column_top {
            false
        } else if data[i - column_top] == null_value {
            null_count += 1;
            false
        } else {
            true
        }
    });
    encode_primitive_def_levels(&mut buffer, deflevels_iter, num_rows, options.version)?;

    let definition_levels_byte_length = buffer.len();

    let mut stats = BinaryMaxMinStats::new(&primitive_type);
    if reverse {
        encode_plain_be(data, &mut buffer, null_value, &mut stats, bloom_hashes);
    } else {
        encode_plain(data, &mut buffer, null_value, &mut stats, bloom_hashes);
    }

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
        Encoding::Plain,
        false,
    )
    .map(Page::Data)
}

pub fn bytes_to_dict_pages<const N: usize>(
    data: &[[u8; N]],
    reverse: bool,
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
) -> ParquetResult<DynIter<'static, ParquetResult<Page>>> {
    let num_rows = column_top + data.len();
    let null_value = {
        let mut null_value = [0u8; N];
        let long_as_bytes = i64::MIN.to_le_bytes();
        for i in 0..N {
            null_value[i] = long_as_bytes[i % long_as_bytes.len()];
        }
        null_value
    };
    let mut null_count = 0;

    // Build dictionary
    let mut dict_map: RapidHashMap<[u8; N], u32> = RapidHashMap::default();
    let mut dict_entries: Vec<[u8; N]> = Vec::new();
    let mut keys: Vec<u32> = Vec::with_capacity(data.len());
    let mut stats = BinaryMaxMinStats::new(&primitive_type);

    for &value in data {
        if value == null_value {
            null_count += 1;
        } else {
            // For dictionary, store the value as it will appear in the dict buffer
            let stored = if reverse {
                let mut r = value;
                r.reverse();
                r
            } else {
                value
            };
            let next_id = dict_entries.len() as u32;
            let key = *dict_map.entry(stored).or_insert_with(|| {
                dict_entries.push(stored);
                next_id
            });
            keys.push(key);
            if !reverse {
                stats.update(&value);
            }
        }
    }

    // Build dict buffer: N raw bytes per entry (FixedLenByteArray format)
    let mut dict_buffer = Vec::with_capacity(dict_entries.len() * N);
    for entry in &dict_entries {
        dict_buffer.extend_from_slice(entry);
    }

    // Encode data page
    let total_null_count = column_top + null_count;
    let mut data_buffer = Vec::new();

    let def_levels = (0..num_rows).map(|i| {
        if i < column_top {
            false
        } else {
            data[i - column_top] != null_value
        }
    });
    encode_primitive_def_levels(&mut data_buffer, def_levels, num_rows, options.version)?;
    let definition_levels_byte_length = data_buffer.len();

    let non_null_len = data.len() - null_count;
    let statistics = if options.write_statistics {
        Some(stats.into_parquet_stats(total_null_count))
    } else {
        None
    };

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
