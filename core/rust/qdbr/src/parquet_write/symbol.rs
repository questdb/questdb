use std::collections::HashMap;
use std::mem::{self, size_of};

use parquet2::encoding::hybrid_rle::encode_u32;
use parquet2::encoding::Encoding;
use parquet2::page::{DictPage, Page};
use parquet2::schema::types::PrimitiveType;
use parquet2::write::DynIter;

use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util::{build_plain_page, encode_bool_iter, ExactSizedIter};
use crate::parquet_write::{util, ParquetResult};

use super::util::BinaryMaxMin;

fn encode_dict(
    column_vals: &[i32],
    offsets: &[u64],
    chars: &[u8],
    stats: &mut BinaryMaxMin,
) -> (Vec<u8>, Vec<u32>, u32) {
    let mut dict_buffer = vec![];
    let mut local_keys: Vec<u32> = Vec::new();
    let mut keys_to_local = HashMap::new();
    let mut serialized_count = 0;

    for column_value in column_vals {
        if *column_value <= -1 {
            continue;
        }
        let u32_size = size_of::<u32>();
        let local_key = *keys_to_local.entry(*column_value).or_insert_with(|| {
            let offset = offsets[*column_value as usize] as usize;
            let (size_header, data_tail) = chars[offset..].split_at(u32_size);
            let data_size = i32::from_le_bytes(size_header.try_into().unwrap()) as usize;
            let data_u8 = &data_tail[..data_size];
            let data_u16: &[u16] = unsafe { mem::transmute(data_u8) };
            let str_value = String::from_utf16(data_u16).expect("utf16 string");

            let local_key = serialized_count;
            dict_buffer.reserve(u32_size + str_value.len());
            dict_buffer.extend_from_slice(&(str_value.len() as u32).to_le_bytes());
            let value = str_value.as_bytes();
            dict_buffer.extend_from_slice(value);
            serialized_count += 1;
            stats.update(value);
            local_key
        });
        local_keys.push(local_key);
    }
    if serialized_count == 0 {
        // No symbol value used in the column data block, all were nulls
        return (dict_buffer, local_keys, 0);
    }
    (dict_buffer, local_keys, serialized_count - 1)
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
    let (dict_buffer, keys, max_key) = encode_dict(column_values, offsets, chars, &mut stats);
    let bits_per_key = util::get_bit_width(max_key as u64);

    let non_null_len = column_values.len() - null_count;
    let keys = ExactSizedIter::new(keys.into_iter(), non_null_len);
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
