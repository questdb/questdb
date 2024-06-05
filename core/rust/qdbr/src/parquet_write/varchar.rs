use std::mem;
use std::slice;

use parquet2::encoding::Encoding;
use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;

use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util::{build_plain_page, encode_bool_iter};
use crate::parquet_write::ParquetResult;

const HEADER_FLAG_INLINED: u8 = 1 << 0;
const _HEADER_FLAG_ASCII: u8 = 1 << 1;
const HEADER_FLAG_NULL: u8 = 1 << 2;

const HEADER_FLAGS_WIDTH: u32 = 4;

#[repr(C, packed(16))]
struct AuxEntryInlined {
    header: u8,
    chars: [u8; 9],
    _offset: [u8; 6],
}

#[repr(C, packed(16))]
struct AuxEntrySplit {
    header: u32,
    chars: [u8; 6],
    offset_lo: u16,
    offset_hi: u32,
}

fn encode_plain(aux: &[AuxEntryInlined], data: &[u8], buffer: &mut Vec<u8>) {
    for entry in aux.iter().filter(|entry| !is_null(entry.header)) {
        if is_inlined(entry.header) {
            let size = (entry.header >> HEADER_FLAGS_WIDTH) as usize;
            let utf8_slice = &entry.chars[..size];
            let len = (utf8_slice.len() as u32).to_le_bytes();
            buffer.extend_from_slice(&len);
            buffer.extend_from_slice(utf8_slice);
        } else {
            let entry: &AuxEntrySplit = unsafe { mem::transmute(entry) };
            let header = entry.header;
            let size = (header >> HEADER_FLAGS_WIDTH) as usize;
            let offset = entry.offset_lo as usize + ((entry.offset_hi as usize) << 16);
            let utf8_slice = &data[offset..][..size];
            let len = (size as u32).to_le_bytes();
            buffer.extend_from_slice(&len);
            buffer.extend_from_slice(utf8_slice);
        }
    }
}

#[inline(always)]
fn is_null(raw: u8) -> bool {
    (raw & HEADER_FLAG_NULL) == HEADER_FLAG_NULL
}

#[inline(always)]
fn is_inlined(raw: u8) -> bool {
    (raw & HEADER_FLAG_INLINED) == HEADER_FLAG_INLINED
}

pub fn varchar_to_page(
    aux: &[u8],
    data: &[u8],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
) -> ParquetResult<Page> {
    let sizeof_entry = mem::size_of::<AuxEntryInlined>();
    assert!(
        aux.len() % sizeof_entry == 0,
        "aux.len() {} % sizeof_entry {} != 0",
        aux.len(),
        sizeof_entry
    );
    let aux: &[AuxEntryInlined] = unsafe {
        slice::from_raw_parts(
            aux.as_ptr() as *const AuxEntryInlined,
            aux.len() / sizeof_entry,
        )
    };
    let num_rows = column_top + aux.len();
    let mut buffer = vec![];
    let mut null_count = 0;

    let deflevels_iter = (0..num_rows).map(|i| {
        if i < column_top {
            null_count += 1;
            false
        } else {
            let header = aux[i - column_top].header;
            if is_null(header) {
                null_count += 1;
                false
            } else {
                true
            }
        }
    });

    encode_bool_iter(&mut buffer, deflevels_iter, options.version)?;
    let definition_levels_byte_length = buffer.len();
    encode_plain(aux, data, &mut buffer);
    build_plain_page(
        buffer,
        num_rows,
        null_count,
        definition_levels_byte_length,
        None, // TODO: implement statistics
        primitive_type,
        options,
        Encoding::Plain,
    )
    .map(Page::Data)
}
