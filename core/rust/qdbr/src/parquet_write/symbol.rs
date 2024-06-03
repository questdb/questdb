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

fn encode_dict(column_vals: &[i32], offsets: &[u64], chars: &[u8]) -> (Vec<u8>, Vec<u32>, u32) {
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
            let value = String::from_utf16(data_u16).expect("utf16 string");

            let local_key = serialized_count;
            dict_buffer.reserve(u32_size + value.len());
            dict_buffer.extend_from_slice(&(value.len() as u32).to_le_bytes());
            dict_buffer.extend_from_slice(value.as_bytes());
            serialized_count += 1;
            local_key
        });
        local_keys.push(local_key);
    }
    if serialized_count == 0 {
        // No symbol value used in the column data block, all were nulls
        return (dict_buffer, local_keys, 0);
    }
    (dict_buffer, local_keys, (serialized_count - 1) as u32)
}

pub fn symbol_to_pages(
    column_values: &[i32],
    offsets: &[u64],
    chars: &[u8],
    options: WriteOptions,
    type_: PrimitiveType,
) -> ParquetResult<DynIter<'static, ParquetResult<Page>>> {
    let mut null_count = 0;
    let deflevels_iter = column_values.iter().map(|key| {
        // -1 denotes a null value
        if *key > -1 {
            true
        } else {
            null_count += 1;
            false
        }
    });
    let mut data_buffer = vec![];
    let length = deflevels_iter.len();
    encode_bool_iter(&mut data_buffer, deflevels_iter, options.version)?;
    let definition_levels_byte_length = data_buffer.len();

    let (dict_buffer, keys, max_key) = encode_dict(column_values, offsets, chars);
    let num_bits = util::get_bit_width(max_key as u64);

    // print!("column:{}, keys: {}, offsets: {}, null_count: {}\n", column.name, keys.len(), offsets.len(), null_count);
    let non_null_len = column_values.len() - null_count;
    let keys = ExactSizedIter::new(keys.into_iter(), non_null_len);
    // num_bits as a single byte
    data_buffer.push(num_bits);
    // followed by the encoded indices.
    encode_u32(&mut data_buffer, keys, num_bits as u32)?;

    let data_page = build_plain_page(
        data_buffer,
        length,
        length,
        null_count,
        definition_levels_byte_length,
        None,
        type_,
        options,
        Encoding::RleDictionary,
    )?;

    let uniq_vals = if dict_buffer.len() > 0 {
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
