use std::collections::HashMap;
use std::mem;

use parquet2::encoding::Encoding;
use parquet2::encoding::hybrid_rle::encode_u32;
use parquet2::page::{DictPage, Page};
use parquet2::schema::types::PrimitiveType;
use parquet2::write::DynIter;

use crate::parquet_write::{ParquetResult, util};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util::{build_plain_page, encode_bool_iter, ExactSizedIter};

fn encode_dict(keys: &[i32], offsets: &[u64], data: &[u8], page: &mut Vec<u8>) -> (Vec<u32>, u32) {
    let mut indices: Vec<u32> = Vec::new();
    let mut keys_to_local = HashMap::new();
    let mut serialised = 0;
    for key in keys {
        if *key > 0 {
            let local_key = *keys_to_local.entry(*key).or_insert_with(|| {
                let offset = offsets[*key as usize] as usize;
                let size = i32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
                let data_slice: &[u16] =
                    unsafe { mem::transmute(&data[offset + 4..offset + 4 + size as usize]) };
                let value = String::from_utf16(data_slice).expect("utf16 string");

                let local_key = serialised;
                page.reserve(4 + value.len());
                page.extend_from_slice(&(value.len() as u32).to_le_bytes());
                page.extend_from_slice(value.as_bytes());
                serialised += 1;
                local_key
            });
            indices.push(local_key);
        }
    }
    (indices, serialised - 1)
}

pub fn symbol_to_pages(
    keys: &[i32],
    offsets: &[u64],
    data: &[u8],
    options: WriteOptions,
    type_: PrimitiveType,
) -> ParquetResult<DynIter<'static, ParquetResult<Page>>> {
    let offsets = &offsets[8..]; // ignore the header of (64 bytes or 8 u64s)

    let mut dict_buffer = vec![];
    let (keys, max_key) = encode_dict(keys, offsets, data, &mut dict_buffer);
    let mut null_count = 0;
    let nulls_iterator = keys.iter().map(|key| {
        if *key > 0 {
            // key == -1, is null encoding
            true
        } else {
            null_count += 1;
            false
        }
    });

    let mut data_buffer = vec![];
    let length = nulls_iterator.len();

    encode_bool_iter(&mut data_buffer, nulls_iterator, options.version)?;
    let definition_levels_byte_length = data_buffer.len();

    let num_bits = util::get_bit_width(max_key as u64);
    let non_null_len = keys.len() - null_count;
    let keys = ExactSizedIter::new(keys.into_iter(), non_null_len);
    // num_bits as a single byte
    data_buffer.push(num_bits as u8);
    // followed by the encoded indices.
    encode_u32(&mut data_buffer, keys, num_bits)?;

    let dict_page = DictPage::new(dict_buffer, length, false);

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

    Ok(DynIter::new(
        [Page::Dict(dict_page), Page::Data(data_page)]
            .into_iter()
            .map(Ok),
    ))
}
