use super::util::BinaryMaxMin;
use crate::parquet::error::{
    fmt_err, ParquetError, ParquetErrorCause, ParquetErrorExt, ParquetResult,
};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util;
use crate::parquet_write::util::{build_plain_page, encode_bool_iter, ExactSizedIter};
use parquet2::encoding::hybrid_rle::encode_u32;
use parquet2::encoding::Encoding;
use parquet2::page::{DictPage, Page};
use parquet2::schema::types::PrimitiveType;
use parquet2::write::DynIter;
use std::char::DecodeUtf16Error;
use std::cmp::max;
use std::collections::HashSet;

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
    column_vals: &'a [i32], // The QuestDB symbol column indices (i.e. numeric values).
    offsets: &'a [u64],     // Memory-mapped offsets into the QuestDB global symbol table.
    chars: &'a [u8], // Memory-mapped global symbol table. Sequence of 4-code-unit-len-prefixed utf16 strings.
    stats: &'a mut BinaryMaxMin,
) -> ParquetResult<(Vec<u8>, impl Iterator<Item = u32> + 'a, u32)> {
    let local_keys = column_vals
        .iter()
        .filter_map(|&v| if v >= 0 { Some(v as u32) } else { None });

    let local_keys2 = local_keys.clone(); // returned later

    let mut max_key = 0;
    let mut end_value = None;
    // TODO(amunra): Reuse (cache allocation of) the `values_set` across multiple calls.
    // Collect the set of unique values in the column.
    let values_set: HashSet<u32> = local_keys
        .inspect(|&n| {
            max_key = max(max_key, n);
            end_value = max(end_value, Some(n + 1));
        })
        .collect();
    let end_value = end_value.unwrap_or(0);

    // Compute an initial buffer capacity estimate for the dictionary buffer.
    // We know that skipped values will use up exactly 4 bytes, and we expect
    // other symbols to require 6 bytes per symbol in string length + 4 bytes len prefix.
    let dense_count = values_set.len() as u32;
    let sparse_count = end_value - dense_count;
    let dict_buffer_size_estimate = (sparse_count * 4) + (dense_count * 10);

    let mut dict_buffer = Vec::with_capacity(dict_buffer_size_estimate as usize);

    // Walk each key up to `last_value` and encode it into the `dict_buffer`.
    // Unused values are encoded as empty strings.
    for key in 0..end_value {
        // Always encode a zero-length. This is then overwritten with the actual length.
        // This is to avoid double-buffering into a temporary `String`.
        let key_index = dict_buffer.len();
        dict_buffer.extend_from_slice(&(0u32).to_le_bytes());

        if values_set.contains(&key) {
            let qdb_global_offset = *offsets.get(key as usize).ok_or_else(|| {
                fmt_err!(Layout, "could not find symbol with key {key} in global map")
            })? as usize;
            const UTF16_LEN_SIZE: usize = 4;
            if (qdb_global_offset + UTF16_LEN_SIZE) > chars.len() {
                return Err(fmt_err!(Layout,"global symbol map character data too small, begin offset {qdb_global_offset} out of bounds"));
            }
            let qdb_utf16_len_buf = &chars[qdb_global_offset..];
            let (qdb_utf16_len, qdb_utf16_buf) = qdb_utf16_len_buf.split_at(UTF16_LEN_SIZE);

            let qdb_utf16_len =
                i32::from_le_bytes(qdb_utf16_len.try_into().expect("4 bytes sliced")) as usize;

            // In the `.c` (chars) file, the length is stored as a little-endian 32-bit integer of
            // code unit counts. We multiply by 2 to get the byte length of the UTF-16 string.
            if qdb_utf16_buf.len() < (qdb_utf16_len * 2) {
                return Err(fmt_err!(
                    Layout,
                    "global symbol map character data too small, end offset {} out of bounds",
                    qdb_global_offset + qdb_utf16_len * 2
                ));
            }
            let qdb_utf16_buf: &[u16] = unsafe { std::mem::transmute(qdb_utf16_buf) };
            let qdb_utf16_buf = &qdb_utf16_buf[..qdb_utf16_len];
            let utf8_len = write_utf8_from_utf16(&mut dict_buffer, qdb_utf16_buf)
                .map_err(|e| ParquetErrorCause::Utf16Decode(e).into_err())?;
            let utf8_buf = &dict_buffer[(key_index + 4)..(key_index + 4 + utf8_len)];

            // Update the page's min/max statistics for the referenced UTF-8 strings.
            stats.update(utf8_buf);

            // Go back and overwrite the zero-length with the actual length.
            let utf8_len_bytes = (utf8_len as u32).to_le_bytes();
            dict_buffer[key_index..(key_index + 4)].copy_from_slice(&utf8_len_bytes);
        }
    }
    Ok((dict_buffer, local_keys2, max_key))
}

fn write_utf8_from_utf16(dest: &mut Vec<u8>, src: &[u16]) -> Result<usize, DecodeUtf16Error> {
    let start_count = dest.len();
    for c in char::decode_utf16(src.iter().cloned()) {
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
) -> ParquetResult<DynIter<'static, ParquetResult<Page>>> {
    let num_rows = column_top + column_values.len();
    let mut null_count = 0;

    // TODO(amunra): Optimize if there's no column top.
    let deflevels_iter = (0..num_rows).map(|i| {
        if i < column_top {
            false
        } else {
            let key = column_values[i - column_top];
            // negative denotes a null value
            if key > -1 {
                true
            } else {
                null_count += 1;
                false
            }
        }
    });
    let mut data_buffer = vec![];
    encode_bool_iter(&mut data_buffer, deflevels_iter, options.version)?;
    let definition_levels_byte_length = data_buffer.len();

    let mut stats = BinaryMaxMin::new(&primitive_type);
    let (dict_buffer, keys, max_key) =
        encode_symbols_dict(column_values, offsets, chars, &mut stats)
            .context("could not write symbols dict map page")?;
    let bits_per_key = util::get_bit_width(max_key as u64);

    let non_null_len = column_values.len() - null_count;
    let keys = ExactSizedIter::new(keys, non_null_len);
    // bits_per_key as a single byte...
    data_buffer.push(bits_per_key);
    // followed by the encoded keys.
    encode_u32(&mut data_buffer, keys, bits_per_key as u32)?;

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
