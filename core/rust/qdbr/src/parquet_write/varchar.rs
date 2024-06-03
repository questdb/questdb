use parquet2::encoding::Encoding;
use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;

use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util::{build_plain_page, encode_bool_iter};
use crate::parquet_write::ParquetResult;

const HEADER_FLAG_INLINED: u32 = 1 << 0;
const HEADER_FLAG_ASCII: u32 = 1 << 1;
const HEADER_FLAG_NULL: u32 = 1 << 2;

const HEADER_FLAGS_WIDTH: u32 = 4;
const INLINED_LENGTH_MASK: u32 = (1 << 4) - 1;

const FULLY_INLINED_STRING_OFFSET: usize = 1;
const LENGTH_LIMIT_BYTES: u32 = 1 << 28;
const DATA_LENGTH_MASK: u32 = LENGTH_LIMIT_BYTES - 1;

fn encode_plain(aux: &[u8], data: &[u8], buffer: &mut Vec<u8>) {
    // append the non-null values
    aux.chunks(16).for_each(|bytes| {
        let header_raw = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        if !is_null(header_raw) {
            if is_inlined(header_raw) {
                let size = ((header_raw >> HEADER_FLAGS_WIDTH) & INLINED_LENGTH_MASK) as usize;
                let utf8_slice = &bytes[FULLY_INLINED_STRING_OFFSET..size];
                let len = (utf8_slice.len() as u32).to_le_bytes();
                buffer.extend_from_slice(&len);
                buffer.extend_from_slice(utf8_slice);
            } else {
                let offset = (u64::from_le_bytes(bytes[8..16].try_into().unwrap()) >> 16) as usize;
                let size = ((header_raw >> HEADER_FLAGS_WIDTH) & DATA_LENGTH_MASK) as usize;
                let utf8_slice = &data[offset..offset + size];
                let len = (utf8_slice.len() as u32).to_le_bytes();
                buffer.extend_from_slice(&len);
                buffer.extend_from_slice(utf8_slice);
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
    primitive_type: PrimitiveType,
) -> ParquetResult<Page> {
    let mut buffer = vec![];
    let mut null_count = 0;

    let deflevels_iter = aux.chunks(16).map(|bytes| {
        let raw = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        if is_null(raw) {
            null_count += 1;
            false
        } else {
            true
        }
    });

    let length = deflevels_iter.len();
    encode_bool_iter(&mut buffer, deflevels_iter, options.version)?;
    let definition_levels_byte_length = buffer.len();
    encode_plain(aux, data, &mut buffer);
    build_plain_page(
        buffer,
        length,
        length,
        null_count,
        definition_levels_byte_length,
        None, // do we really want a varchar statistics?
        primitive_type,
        options,
        Encoding::Plain,
    )
    .map(Page::Data)
}
