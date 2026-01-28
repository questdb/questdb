use super::util::BinaryMaxMinStats;
use crate::parquet::error::{fmt_err, ParquetErrorExt, ParquetErrorReason, ParquetResult};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util;
use crate::parquet_write::util::{build_plain_page, encode_primitive_def_levels, ExactSizedIter};
use parquet2::encoding::hybrid_rle::encode_u32;
use parquet2::encoding::Encoding;
use parquet2::page::{DictPage, Page};
use parquet2::schema::types::PrimitiveType;
use parquet2::write::DynIter;
use std::char::DecodeUtf16Error;
use std::cmp::max;
use std::collections::HashSet;

pub struct SymbolGlobalInfo {
    pub used_keys: HashSet<u32>,
    pub max_key: u32,
}

pub fn collect_symbol_global_info(
    partition_data: &[(&[i32], usize, usize)], // (keys_slice, column_top, num_rows)
) -> SymbolGlobalInfo {
    let mut used_keys = HashSet::new();
    let mut max_key = 0u32;

    for &(keys, column_top, num_rows) in partition_data {
        let data_rows = num_rows.saturating_sub(column_top);
        let keys_slice = &keys[..data_rows.min(keys.len())];

        for &key in keys_slice {
            if key >= 0 {
                let k = key as u32;
                max_key = max(max_key, k);
                used_keys.insert(k);
            }
        }
    }

    SymbolGlobalInfo { used_keys, max_key }
}

pub fn build_symbol_dict_page(
    global_info: &SymbolGlobalInfo,
    offsets: &[u64],
    chars: &[u8],
    primitive_type: &PrimitiveType,
    write_statistics: bool,
) -> ParquetResult<(DictPage, Option<BinaryMaxMinStats>)> {
    let mut stats = if write_statistics {
        Some(BinaryMaxMinStats::new(primitive_type))
    } else {
        None
    };

    let dict_buffer = build_dict_buffer(
        &global_info.used_keys,
        global_info.max_key,
        offsets,
        chars,
        stats.as_mut(),
    )?;

    let uniq_vals = if global_info.used_keys.is_empty() {
        0
    } else {
        global_info.max_key + 1
    };

    Ok((DictPage::new(dict_buffer, uniq_vals as usize, false), stats))
}

#[allow(clippy::too_many_arguments)]
pub fn symbol_to_data_page_only(
    column_values: &[i32],
    column_top: usize,
    global_max_key: u32,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    offsets: &[u64],
    chars: &[u8],
    required: bool,
) -> ParquetResult<Page> {
    let num_rows = column_top + column_values.len();
    let mut null_count = 0;
    let mut data_buffer = vec![];

    let definition_levels_byte_length = if required {
        debug_assert!(column_top == 0);
        0
    } else {
        let def_levels: Vec<bool> = (0..num_rows)
            .map(|i| {
                if i < column_top {
                    false
                } else {
                    let key = column_values[i - column_top];
                    if key >= 0 {
                        true
                    } else {
                        null_count += 1;
                        false
                    }
                }
            })
            .collect();

        encode_primitive_def_levels(
            &mut data_buffer,
            def_levels.into_iter(),
            num_rows,
            options.version,
        )?;
        data_buffer.len()
    };

    let page_stats = if options.write_statistics {
        let mut stats = BinaryMaxMinStats::new(&primitive_type);
        update_stats_for_partition(column_values, offsets, chars, &mut stats)?;
        Some(stats.into_parquet_stats(null_count))
    } else {
        None
    };

    let bits_per_key = util::bit_width(global_max_key as u64);
    let local_keys = column_values
        .iter()
        .filter_map(|&v| if v >= 0 { Some(v as u32) } else { None });
    let non_null_len = column_values.len() - null_count;
    let keys = ExactSizedIter::new(local_keys, non_null_len);
    data_buffer.push(bits_per_key);
    encode_u32(&mut data_buffer, keys, non_null_len, bits_per_key as u32)?;

    let data_page = build_plain_page(
        data_buffer,
        num_rows,
        null_count,
        definition_levels_byte_length,
        page_stats,
        primitive_type,
        options,
        Encoding::RleDictionary,
        required,
    )?;

    Ok(Page::Data(data_page))
}

fn update_stats_for_partition(
    column_values: &[i32],
    offsets: &[u64],
    chars: &[u8],
    stats: &mut BinaryMaxMinStats,
) -> ParquetResult<()> {
    for &key in column_values {
        if key >= 0 {
            let k = key as usize;
            if let Some(&offset) = offsets.get(k) {
                if let Some(utf8_buf) = read_symbol_as_utf8(chars, offset as usize)? {
                    stats.update(&utf8_buf);
                }
            }
        }
    }
    Ok(())
}

/// Reads a symbol from the QuestDB global symbol table and converts it to UTF-8.
/// Returns None if the offset is out of bounds.
fn read_symbol_as_utf8(chars: &[u8], qdb_global_offset: usize) -> ParquetResult<Option<Vec<u8>>> {
    const UTF16_LEN_SIZE: usize = 4;

    if qdb_global_offset + UTF16_LEN_SIZE > chars.len() {
        return Ok(None);
    }

    let qdb_utf16_len_buf = &chars[qdb_global_offset..];
    let qdb_utf16_len =
        i32::from_le_bytes(qdb_utf16_len_buf[..4].try_into().expect("4 bytes")) as usize;

    let required_len = UTF16_LEN_SIZE + qdb_utf16_len * 2;
    if qdb_utf16_len_buf.len() < required_len {
        return Ok(None);
    }

    // Safe UTF-16 reading without alignment issues
    let utf16_bytes = &qdb_utf16_len_buf[UTF16_LEN_SIZE..UTF16_LEN_SIZE + qdb_utf16_len * 2];
    let utf16_iter = utf16_bytes
        .chunks_exact(2)
        .map(|b| u16::from_le_bytes([b[0], b[1]]));

    let mut utf8_buf = Vec::new();
    write_utf8_from_utf16_iter(&mut utf8_buf, utf16_iter)
        .map_err(|e| ParquetErrorReason::Utf16Decode(e).into_err())?;
    Ok(Some(utf8_buf))
}

fn read_symbol_to_utf8(
    chars: &[u8],
    qdb_global_offset: usize,
    dest: &mut Vec<u8>,
) -> ParquetResult<usize> {
    const UTF16_LEN_SIZE: usize = 4;

    if qdb_global_offset + UTF16_LEN_SIZE > chars.len() {
        return Err(fmt_err!(
            Layout,
            "global symbol map character data too small, begin offset {qdb_global_offset} out of bounds"
        ));
    }

    let qdb_utf16_len_buf = &chars[qdb_global_offset..];
    let qdb_utf16_len =
        i32::from_le_bytes(qdb_utf16_len_buf[..4].try_into().expect("4 bytes")) as usize;

    let required_len = UTF16_LEN_SIZE + qdb_utf16_len * 2;
    if qdb_utf16_len_buf.len() < required_len {
        return Err(fmt_err!(
            Layout,
            "global symbol map character data too small, end offset {} out of bounds",
            qdb_global_offset + qdb_utf16_len * 2
        ));
    }
    let utf16_bytes = &qdb_utf16_len_buf[UTF16_LEN_SIZE..UTF16_LEN_SIZE + qdb_utf16_len * 2];
    let utf16_iter = utf16_bytes
        .chunks_exact(2)
        .map(|b| u16::from_le_bytes([b[0], b[1]]));

    let utf8_len = write_utf8_from_utf16_iter(dest, utf16_iter)
        .map_err(|e| ParquetErrorReason::Utf16Decode(e).into_err())?;
    Ok(utf8_len)
}

fn build_dict_buffer(
    used_keys: &HashSet<u32>,
    max_key: u32,
    offsets: &[u64],
    chars: &[u8],
    mut stats: Option<&mut BinaryMaxMinStats>,
) -> ParquetResult<Vec<u8>> {
    let end_value = if used_keys.is_empty() { 0 } else { max_key + 1 };

    let dense_count = used_keys.len() as u32;
    let sparse_count = end_value.saturating_sub(dense_count);
    let dict_buffer_size_estimate = (sparse_count * 4) + (dense_count * 10);

    let mut dict_buffer = Vec::with_capacity(dict_buffer_size_estimate as usize);

    for key in 0..end_value {
        let key_index = dict_buffer.len();
        dict_buffer.extend_from_slice(&(0u32).to_le_bytes());

        if used_keys.contains(&key) {
            let qdb_global_offset = *offsets.get(key as usize).ok_or_else(|| {
                fmt_err!(Layout, "could not find symbol with key {key} in global map")
            })? as usize;

            let utf8_len = read_symbol_to_utf8(chars, qdb_global_offset, &mut dict_buffer)?;
            let utf8_buf = &dict_buffer[(key_index + 4)..(key_index + 4 + utf8_len)];

            if let Some(ref mut s) = stats {
                s.update(utf8_buf);
            }

            let utf8_len_bytes = (utf8_len as u32).to_le_bytes();
            dict_buffer[key_index..(key_index + 4)].copy_from_slice(&utf8_len_bytes);
        }
    }

    Ok(dict_buffer)
}

/// Encode the QuestDB symbols to Parquet.
///
/// The resulting tuple consists of:
///   * The parquet dictionary buffer, which is a sequence of the 4-byte-len-prefixed utf8 strings.
///   * The local keys, which are the indexes into the dictionary buffer.
///   * The largest key value used, or 0 if no keys were used.
///
/// The first element of the first tuple argument returned (parquet dict buffer) is encoded in a
/// specific way to be compatible with QuestDB with zero-read overhead during queries.
///
/// The aim is to preserve the same numeric values in the column as the original QuestDB column.
/// In other words, the "local" keys will always match the "global" symbol keys.
///
/// The easiest way to achieve this would be to encode the whole dictionary every time.
/// E.g. if the dict has symbols:
///
/// 0: "abc"
/// 1: "defg"
/// 2: "hi"
/// 3: "jklmn"
///
/// And the column has key values:
///
/// 0, 2, 2  -- i.e, "abc", "hi", "hi"
///
/// We could encode the parquet dict buffer as so:
/// [3, 0, 0, 0, 'a', 'b', 'c',
///  4, 0, 0, 0, 'd', 'e', 'f', 'g',
///  2, 0, 0, 0, 'h', 'i',
///  5, 0, 0, 0, 'j', 'k', 'l', 'm', 'n']
///
/// But this would be unnecessarily wasteful.
/// Instead, we employ two strategies to reduce the size of the dictionary:
///   * The parquet dict is truncated to exclude symbols past the last used key.
///   * Intermediate unused keys are encoded as an empty string.
///
/// For the example above, the encoded parquet dict buffer would be:
///
/// [3, 0, 0, 0, 'a', 'b', 'c',
///  0, 0, 0, 0,
///  2, 0, 0, 0, 'h', 'i']
///
/// This strategy leads to two benefits:
///   * During querying, the dict keys can be used directly as the column values - no lookups!
///   * The resulting parquet file is still compatible with other readers.
///
/// The downsides are:
///   * The dictionary is inflated with empty strings.
///   * This is a reasonable tradeoff if most row groups end use a large subset of the global symbols.
///   * This trades faster query performance for slightly higher memory usage during ingestion.
///
fn encode_symbols_dict<'a>(
    column_vals: &'a [i32],
    offsets: &[u64],
    chars: &[u8],
    stats: &mut BinaryMaxMinStats,
) -> ParquetResult<(Vec<u8>, impl Iterator<Item = u32> + 'a, u32)> {
    let local_keys = column_vals
        .iter()
        .filter_map(|&v| if v >= 0 { Some(v as u32) } else { None });

    let local_keys2 = local_keys.clone(); // returned later

    let mut max_key = 0;
    // Collect the set of unique values in the column.
    let values_set: HashSet<u32> = local_keys
        .inspect(|&n| {
            max_key = max(max_key, n);
        })
        .collect();

    let dict_buffer = build_dict_buffer(&values_set, max_key, offsets, chars, Some(stats))?;

    Ok((dict_buffer, local_keys2, max_key))
}

fn write_utf8_from_utf16_iter(
    dest: &mut Vec<u8>,
    src: impl Iterator<Item = u16>,
) -> Result<usize, DecodeUtf16Error> {
    let start_count = dest.len();
    for c in char::decode_utf16(src) {
        let c = c?;
        match c.len_utf8() {
            1 => dest.push(c as u8),
            _ => dest.extend_from_slice(c.encode_utf8(&mut [0; 4]).as_bytes()),
        }
    }
    Ok(dest.len() - start_count)
}

pub fn symbol_to_pages(
    column_values: &[i32],
    offsets: &[u64],
    chars: &[u8],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    required: bool,
) -> ParquetResult<DynIter<'static, ParquetResult<Page>>> {
    let num_rows = column_top + column_values.len();
    let mut null_count = 0;
    let mut data_buffer = vec![];

    let definition_levels_byte_length = if required {
        debug_assert!(column_top == 0);
        0
    } else {
        // Build def levels and count nulls in a single pass
        let def_levels: Vec<bool> = (0..num_rows)
            .map(|i| {
                if i < column_top {
                    false
                } else {
                    let key = column_values[i - column_top];
                    if key >= 0 {
                        true
                    } else {
                        null_count += 1;
                        false
                    }
                }
            })
            .collect();

        encode_primitive_def_levels(
            &mut data_buffer,
            def_levels.into_iter(),
            num_rows,
            options.version,
        )?;
        data_buffer.len()
    };

    let mut stats = BinaryMaxMinStats::new(&primitive_type);
    let (dict_buffer, keys, max_key) =
        encode_symbols_dict(column_values, offsets, chars, &mut stats)
            .context("could not write symbols dict map page")?;
    let bits_per_key = util::bit_width(max_key as u64);

    let non_null_len = column_values.len() - null_count;
    let keys = ExactSizedIter::new(keys, non_null_len);
    // bits_per_key as a single byte...
    data_buffer.push(bits_per_key);
    // followed by the encoded keys.
    encode_u32(&mut data_buffer, keys, non_null_len, bits_per_key as u32)?;

    let data_page = build_plain_page(
        data_buffer,
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
        Encoding::RleDictionary,
        required,
    )?;

    let uniq_vals = if !dict_buffer.is_empty() {
        max_key + 1
    } else {
        0
    };
    let dict_page = DictPage::new(dict_buffer, uniq_vals as usize, false);

    Ok(DynIter::new(
        [Page::Dict(dict_page), Page::Data(data_page)]
            .into_iter()
            .map(Ok),
    ))
}
