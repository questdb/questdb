use std::mem;

use parquet2::encoding::Encoding;
use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;

use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util::{build_plain_page, encode_bool_iter, BinaryMaxMin};
use crate::parquet_write::ParquetResult;

const HEADER_FLAG_INLINED: u8 = 1 << 0;
const _HEADER_FLAG_ASCII: u8 = 1 << 1;
const HEADER_FLAG_NULL: u8 = 1 << 2;

const HEADER_FLAGS_WIDTH: u32 = 4;

#[repr(C, packed)]
struct AuxEntryInlined {
    header: u8,
    chars: [u8; 9],
    _offset: [u8; 6],
}

#[repr(C, packed)]
struct AuxEntrySplit {
    header: u32,
    chars: [u8; 6],
    offset_lo: u16,
    offset_hi: u32,
}

fn encode_plain(aux: &[[u8; 16]], data: &[u8], buffer: &mut Vec<u8>) -> BinaryMaxMin {
    assert!(
        mem::size_of::<AuxEntryInlined>() == 16 && mem::size_of::<AuxEntrySplit>() == 16,
        "size_of(AuxEntryInlined) or size_of(AuxEntrySplit) is not 16"
    );
    let aux: &[AuxEntryInlined] = unsafe { mem::transmute(aux) };
    let mut stats = BinaryMaxMin::new();

    for entry in aux.iter().filter(|entry| !is_null(entry.header)) {
        let utf8_slice = if is_inlined(entry.header) {
            let size = (entry.header >> HEADER_FLAGS_WIDTH) as usize;
            &entry.chars[..size]
        } else {
            let entry: &AuxEntrySplit = unsafe { mem::transmute(entry) };
            let header = entry.header;
            let size = (header >> HEADER_FLAGS_WIDTH) as usize;
            let offset = entry.offset_lo as usize | (entry.offset_hi as usize) << 16;
            &data[offset..][..size]
        };
        let len = (utf8_slice.len() as u32).to_le_bytes();
        buffer.extend_from_slice(&len);
        buffer.extend_from_slice(utf8_slice);
        stats.update(utf8_slice);
    }
    stats
}

#[inline(always)]
fn is_null(header: u8) -> bool {
    (header & HEADER_FLAG_NULL) == HEADER_FLAG_NULL
}

#[inline(always)]
fn is_inlined(header: u8) -> bool {
    (header & HEADER_FLAG_INLINED) == HEADER_FLAG_INLINED
}

pub fn varchar_to_page(
    aux: &[[u8; 16]],
    data: &[u8],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
) -> ParquetResult<Page> {
    let num_rows = column_top + aux.len();
    let mut buffer = vec![];
    let mut null_count = 0usize;

    let deflevels_iter = (0..num_rows).map(|i| {
        if i < column_top {
            null_count += 1;
            false
        } else {
            let entry = &aux[i - column_top];
            let header = entry[0];
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
    let stats = encode_plain(aux, data, &mut buffer);
    build_plain_page(
        buffer,
        num_rows,
        null_count,
        definition_levels_byte_length,
        if options.write_statistics {
            Some(stats.into_parquet_stats(null_count, &primitive_type))
        } else {
            None
        },
        primitive_type,
        options,
        Encoding::Plain,
    )
    .map(Page::Data)
}
