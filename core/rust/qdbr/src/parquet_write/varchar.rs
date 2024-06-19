use std::mem;

use parquet2::encoding::{delta_bitpacked, Encoding};
use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;

use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util::{build_plain_page, encode_bool_iter, BinaryMaxMin};
use crate::parquet_write::{ParquetError, ParquetResult};

use super::util::ExactSizedIter;

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
