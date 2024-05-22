use parquet2::encoding::Encoding;
use parquet2::page::DataPage;
use parquet2::schema::types::PrimitiveType;
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util::{build_plain_page, encode_bool_iter};

const HEADER_FLAG_INLINED: u32 = 1 << 0;
const HEADER_FLAG_ASCII: u32 = 1 << 1;
const HEADER_FLAG_NULL: u32 = 1 << 2;

const HEADER_FLAGS_WIDTH: u32 = 4;
const INLINED_LENGTH_MASK: u32 = (1 << 4) - 1;

const LENGTH_LIMIT_BYTES: u32 = 1 << 28;
const DATA_LENGTH_MASK: u32 = LENGTH_LIMIT_BYTES - 1;

fn encode_plain(aux: &[u8], data: &[u8], buffer: &mut Vec<u8>) {
    // append the non-null values
    aux.chunks(16).for_each(|bytes| {
        let raw = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        if !is_null(raw) {
            if is_inlined(raw) {
                let size = ((raw >> HEADER_FLAGS_WIDTH) & INLINED_LENGTH_MASK) as usize;
                buffer.extend_from_slice(bytes[1..size].try_into().unwrap());
            } else {
                let offset= (u64::from_le_bytes(bytes[8..].try_into().unwrap()) >> 16) as usize;
                let size = ((raw >> HEADER_FLAGS_WIDTH) & DATA_LENGTH_MASK) as usize;
                buffer.extend_from_slice(data[offset..offset + size].try_into().unwrap());
            }
        }
    })
}

#[inline(always)]
fn is_null(raw: u32) -> bool {
    (raw & HEADER_FLAG_NULL) == HEADER_FLAG_NULL
}

#[inline(always)]
fn is_inlined(raw: u32) -> bool {
    (raw & HEADER_FLAG_INLINED) == HEADER_FLAG_INLINED
}

pub fn varchar_to_page(
    aux: &[u8],
    data: &[u8],
    options: WriteOptions,
    type_: PrimitiveType,
) -> parquet2::error::Result<DataPage> {
    let mut buffer = vec![];
    let mut null_count = 0;

    let nulls_iterator = aux.chunks(16).map(|bytes| {
        let raw = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        if is_null(raw) {
            null_count += 1;
            false
        } else {
            true
        }
    });

    let length = nulls_iterator.len();
    encode_bool_iter(&mut buffer, nulls_iterator, options.version)?;
    let definition_levels_byte_length = buffer.len();
    encode_plain(aux, data, &mut buffer);
    build_plain_page(
        buffer,
        length,
        length,
        null_count,
        0,
        definition_levels_byte_length,
        None, // do we really want a varchar statistics?
        type_,
        options,
        Encoding::Plain,
    )
}

