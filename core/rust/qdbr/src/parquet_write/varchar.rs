use std::mem;

use parquet2::encoding::{delta_bitpacked, Encoding};
use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;

use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util::{build_plain_page, encode_bool_iter, BinaryMaxMin};
use crate::parquet_write::{ParquetError, ParquetResult};

use super::util::ExactSizedIter;

const HEADER_FLAG_INLINED: u8 = 1 << 0;
const HEADER_FLAG_ASCII: u8 = 1 << 1;
const HEADER_FLAG_ASCII_32: u32 = 1 << 1;
const HEADER_FLAG_NULL: u8 = 1 << 2;
const HEADER_FLAGS_WIDTH: u32 = 4;
const VARCHAR_MAX_BYTES_FULLY_INLINED: usize = 9;
const VARCHAR_INLINED_PREFIX_BYTES: usize = 6;
const LENGTH_LIMIT_BYTES: usize = 1usize << 28;
const VARCHAR_MAX_COLUMN_SIZE: usize = 1usize << 48;
const VARCHAR_HEADER_FLAG_NULL: [u8; 10] = [
    HEADER_FLAG_NULL,
    0u8,
    0u8,
    0u8,
    0u8,
    0u8,
    0u8,
    0u8,
    0u8,
    0u8,
];

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

pub fn varchar_to_page(
    aux: &[[u8; 16]],
    data: &[u8],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    encoding: Encoding,
) -> ParquetResult<Page> {
    assert!(
        mem::size_of::<AuxEntryInlined>() == 16 && mem::size_of::<AuxEntrySplit>() == 16,
        "size_of(AuxEntryInlined) or size_of(AuxEntrySplit) is not 16"
    );

    let num_rows = column_top + aux.len();
    let mut buffer = vec![];
    let mut null_count = 0usize;

    let aux: &[AuxEntryInlined] = unsafe { mem::transmute(aux) };

    let utf8_slices: Vec<Option<&[u8]>> = aux
        .iter()
        .map(|entry| {
            if is_null(entry.header) {
                null_count += 1;
                None
            } else if is_inlined(entry.header) {
                let size = (entry.header >> HEADER_FLAGS_WIDTH) as usize;
                Some(&entry.chars[..size])
            } else {
                let entry: &AuxEntrySplit = unsafe { mem::transmute(entry) };
                let header = entry.header;
                let size = (header >> HEADER_FLAGS_WIDTH) as usize;
                let offset = entry.offset_lo as usize | (entry.offset_hi as usize) << 16;
                assert!(
                    offset + size <= data.len(),
                    "Data corruption in VARCHAR column"
                );
                Some(&data[offset..][..size])
            }
        })
        .collect();

    let deflevels_iter =
        (0..num_rows).map(|i| i >= column_top && utf8_slices[i - column_top].is_some());
    encode_bool_iter(&mut buffer, deflevels_iter, options.version)?;

    let definition_levels_byte_length = buffer.len();

    let mut stats = BinaryMaxMin::new(&primitive_type);

    match encoding {
        Encoding::Plain => {
            encode_plain(&utf8_slices, &mut buffer, &mut stats);
            Ok(())
        }
        Encoding::DeltaLengthByteArray => {
            encode_delta(&utf8_slices, null_count, &mut buffer, &mut stats);
            Ok(())
        }
        other => Err(ParquetError::OutOfSpec(format!(
            "Encoding string as {:?}",
            other
        ))),
    }?;

    let null_count = column_top + null_count;
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
        encoding,
    )
    .map(Page::Data)
}

fn encode_plain(utf8_slices: &[Option<&[u8]>], buffer: &mut Vec<u8>, stats: &mut BinaryMaxMin) {
    for utf8 in utf8_slices.iter().filter_map(|&option| option) {
        let len = (utf8.len() as u32).to_le_bytes();
        buffer.extend_from_slice(&len);
        buffer.extend_from_slice(utf8);
        stats.update(utf8);
    }
}

fn encode_delta(
    utf8_slices: &[Option<&[u8]>],
    null_count: usize,
    buffer: &mut Vec<u8>,
    stats: &mut BinaryMaxMin,
) {
    let lengths = utf8_slices
        .iter()
        .filter_map(|&option| option)
        .map(|utf8| utf8.len() as i64);
    let lengths = ExactSizedIter::new(lengths, utf8_slices.len() - null_count);
    delta_bitpacked::encode(lengths, buffer);
    for utf8 in utf8_slices.iter().filter_map(|&option| option) {
        buffer.extend_from_slice(utf8);
        stats.update(utf8);
    }
}

#[inline(always)]
fn is_null(header: u8) -> bool {
    (header & HEADER_FLAG_NULL) == HEADER_FLAG_NULL
}

#[inline(always)]
fn is_inlined(header: u8) -> bool {
    (header & HEADER_FLAG_INLINED) == HEADER_FLAG_INLINED
}

pub fn append_varchar(aux_mem: &mut Vec<u8>, data_mem: &mut Vec<u8>, value: &[u8]) {
    let value_size = value.len();
    if value_size <= VARCHAR_MAX_BYTES_FULLY_INLINED {
        let flags = HEADER_FLAG_INLINED | is_ascii_inlined(value);
        let header = (value_size << HEADER_FLAGS_WIDTH) as u8 | flags;
        aux_mem.push(header);
        let len_before_value = aux_mem.len();
        aux_mem.extend_from_slice(value);
        // Add zeroes to align to 16 bytes.
        aux_mem.resize(len_before_value + VARCHAR_MAX_BYTES_FULLY_INLINED, 0u8);
        append_offset(aux_mem, data_mem.len());
    } else {
        assert!(value_size <= LENGTH_LIMIT_BYTES);
        let header = (value_size as u32) << HEADER_FLAGS_WIDTH | is_ascii(value);
        aux_mem.extend_from_slice(&header.to_le_bytes());
        aux_mem.extend_from_slice(&value[0..VARCHAR_INLINED_PREFIX_BYTES]);
        data_mem.extend_from_slice(value);
        append_offset(aux_mem, data_mem.len() - value_size);
    }
}

fn is_ascii_inlined(value: &[u8]) -> u8 {
    for &c in value {
        if c > 127 {
            return 0u8;
        }
    }
    HEADER_FLAG_ASCII
}

fn is_ascii(value: &[u8]) -> u32 {
    for &c in value {
        if c > 127 {
            return 0u32;
        }
    }
    HEADER_FLAG_ASCII_32
}

pub fn append_varchar_null(aux_mem: &mut Vec<u8>, data_mem: &[u8]) {
    aux_mem.extend_from_slice(&VARCHAR_HEADER_FLAG_NULL);
    append_offset(aux_mem, data_mem.len());
}

fn append_offset(aux_mem: &mut Vec<u8>, offset: usize) {
    assert!(offset < VARCHAR_MAX_COLUMN_SIZE);
    aux_mem.extend_from_slice(&(offset as u16).to_le_bytes());
    aux_mem.extend_from_slice(&((offset >> 16) as u32).to_le_bytes());
}

pub fn append_varchar_nulls(aux_mem: &mut Vec<u8>, data_mem: &[u8], count: usize) {
    // TODO: optimize, inserting same values
    for _ in 0..count {
        append_varchar_null(aux_mem, data_mem);
    }
}
