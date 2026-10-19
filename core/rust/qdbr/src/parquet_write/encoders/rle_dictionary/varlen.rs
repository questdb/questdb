use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;
use parquet2::types;
use rapidhash::RapidHashMap;

use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::schema::Column;
use crate::parquet_write::util::{transmute_slice, BinaryMaxMinStats};

use super::{
    build_varlen_dict_pages, column_chunk_row_count, partition_chunk_slice,
    write_utf8_from_utf16_iter, ColumnChunkDictState, Repetition,
};

type ByteSliceIter<'a> = Box<dyn Iterator<Item = ParquetResult<Option<&'a [u8]>>> + 'a>;

/// Encode a String column (UTF-16 source) as RleDictionary pages.
pub fn encode_string(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>> {
    let num_partitions = columns.len();
    let total_rows = column_chunk_row_count(columns, first_partition_start, last_partition_end);
    if total_rows == 0 {
        return Ok(vec![]);
    }
    let mut dict_map: RapidHashMap<&[u16], u32> = RapidHashMap::default();
    let mut dict_entries: Vec<Vec<u8>> = Vec::new();
    let mut total_dict_bytes = 0usize;
    let mut state = ColumnChunkDictState::<BinaryMaxMinStats>::new(
        Repetition::Optional,
        total_rows,
        options
            .write_statistics
            .then(|| BinaryMaxMinStats::new(primitive_type)),
    );

    // Pass 1: dedup and compute total dict buffer size.
    for (part_idx, column) in columns.iter().enumerate() {
        let chunk = partition_chunk_slice(
            part_idx,
            num_partitions,
            column,
            first_partition_start,
            last_partition_end,
        );
        let aux: &[i64] = unsafe { transmute_slice(column.secondary_data) };
        let aux_slice = &aux[chunk.lower_bound..chunk.upper_bound];
        let data = column.primary_data;

        state.extend_optional_nulls(chunk.adjusted_column_top)?;

        for offset in aux_slice {
            let offset = usize::try_from(*offset).map_err(|_| {
                fmt_err!(
                    Layout,
                    "invalid offset value in string aux column: {offset}"
                )
            })?;
            let entry_tail = data.get(offset..).ok_or_else(|| {
                fmt_err!(
                    Layout,
                    "offset value {offset} is out of bounds for string aux column data"
                )
            })?;
            match read_utf16(entry_tail)? {
                Some(utf16) => {
                    let key = if let Some(&id) = dict_map.get(&utf16) {
                        id
                    } else {
                        let mut utf8 = Vec::with_capacity(utf16.len() * 3);
                        let utf8_len = write_utf8_from_utf16_iter(&mut utf8, utf16.iter().copied())
                            .map_err(|e| {
                                fmt_err!(Layout, "invalid UTF-16 data in string column: {e}")
                            })?;
                        total_dict_bytes += 4 + utf8_len;
                        let id = u32::try_from(dict_entries.len())
                            .map_err(|_| fmt_err!(Layout, "dictionary exceeds u32::MAX entries"))?;
                        dict_map.insert(utf16, id);
                        if let Some(ref mut stats) = state.stats {
                            stats.update(&utf8);
                        }
                        dict_entries.push(utf8);
                        id
                    };
                    state.push_optional_value(key)?;
                }
                None => {
                    state.push_optional_null()?;
                }
            }
        }
    }

    build_varlen_dict_pages(
        dict_entries.iter().map(|e| e.as_slice()),
        total_dict_bytes,
        state,
        bloom_set.as_ref(),
        primitive_type,
        options,
    )
}

/// Encode a Binary column as RleDictionary pages.
pub fn encode_binary(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>> {
    encode_byte_slices(
        columns,
        first_partition_start,
        last_partition_end,
        primitive_type,
        options,
        bloom_set,
        |column, chunk| {
            let aux: &[i64] = unsafe { transmute_slice(column.secondary_data) };
            let aux_slice = &aux[chunk.lower_bound..chunk.upper_bound];
            let data = column.primary_data;
            let size_of_header = std::mem::size_of::<i64>();
            Ok(Box::new(aux_slice.iter().map(move |offset| {
                let offset = usize::try_from(*offset).map_err(|_| {
                    fmt_err!(
                        Layout,
                        "invalid offset value in binary aux column: {offset}"
                    )
                })?;
                let header_bytes = data.get(offset..offset + size_of_header).ok_or_else(|| {
                    fmt_err!(
                        Layout,
                        "invalid offset value in binary aux column: {offset}"
                    )
                })?;
                let len = types::decode::<i64>(header_bytes);
                if len < 0 {
                    Ok(None)
                } else {
                    let value_offset = offset + size_of_header;
                    let value_len = len as usize;
                    if value_offset + value_len > data.len() {
                        return Err(fmt_err!(
                            Layout,
                            "invalid offset and length in binary aux column: offset {offset}, length {len}"
                        ));
                    }
                    Ok(Some(&data[value_offset..value_offset + value_len]))
                }
            })))
        },
    )
}

/// Encode a Varchar column as RleDictionary pages.
pub fn encode_varchar(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>> {
    encode_byte_slices(
        columns,
        first_partition_start,
        last_partition_end,
        primitive_type,
        options,
        bloom_set,
        visit_varchar_aux_byte_slices,
    )
}

fn encode_byte_slices<F>(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
    decode: F,
) -> ParquetResult<Vec<Page>>
where
    F: for<'a> Fn(
        &'a Column,
        crate::parquet_write::encoders::helpers::ChunkSlice,
    ) -> ParquetResult<ByteSliceIter<'a>>,
{
    let num_partitions = columns.len();
    let total_rows = column_chunk_row_count(columns, first_partition_start, last_partition_end);
    if total_rows == 0 {
        return Ok(vec![]);
    }
    let mut dict_map: RapidHashMap<&[u8], u32> = RapidHashMap::default();
    let mut dict_entries: Vec<&[u8]> = Vec::new();
    let mut total_keys_bytes = 0usize;
    let mut state = ColumnChunkDictState::<BinaryMaxMinStats>::new(
        Repetition::Optional,
        total_rows,
        options
            .write_statistics
            .then(|| BinaryMaxMinStats::new(primitive_type)),
    );

    for (part_idx, column) in columns.iter().enumerate() {
        let chunk = partition_chunk_slice(
            part_idx,
            num_partitions,
            column,
            first_partition_start,
            last_partition_end,
        );
        state.extend_optional_nulls(chunk.adjusted_column_top)?;
        for slice_opt in decode(column, chunk)? {
            match slice_opt? {
                Some(s) => {
                    let key = if let Some(&id) = dict_map.get(&s) {
                        id
                    } else {
                        let id = u32::try_from(dict_entries.len())
                            .map_err(|_| fmt_err!(Layout, "dictionary exceeds u32::MAX entries"))?;
                        dict_map.insert(s, id);
                        dict_entries.push(s);
                        total_keys_bytes += 4 + s.len();
                        if let Some(ref mut stats) = state.stats {
                            stats.update(s);
                        }
                        id
                    };
                    state.push_optional_value(key)?;
                }
                None => {
                    state.push_optional_null()?;
                }
            }
        }
    }

    build_varlen_dict_pages(
        dict_entries.iter().copied(),
        total_keys_bytes,
        state,
        bloom_set.as_ref(),
        primitive_type,
        options,
    )
}

const HEADER_FLAG_INLINED: u8 = 1 << 0;
const HEADER_FLAG_NULL: u8 = 1 << 2;
const HEADER_FLAGS_WIDTH: u32 = 4;

#[repr(C, packed)]
struct VarcharAuxInlined {
    header: u8,
    chars: [u8; 9],
    _offset: [u8; 6],
}

#[repr(C, packed)]
struct VarcharAuxSplit {
    header: u32,
    chars: [u8; 6],
    offset_lo: u16,
    offset_hi: u32,
}

fn visit_varchar_aux_byte_slices<'a>(
    column: &'a Column,
    chunk: crate::parquet_write::encoders::helpers::ChunkSlice,
) -> ParquetResult<ByteSliceIter<'a>> {
    let aux: &[[u8; 16]] = unsafe { transmute_slice(column.secondary_data) };
    let aux_slice = &aux[chunk.lower_bound..chunk.upper_bound];
    let aux_inlined: &[VarcharAuxInlined] = unsafe { std::mem::transmute(aux_slice) };
    let data = column.primary_data;

    Ok(Box::new(aux_inlined.iter().map(move |entry| {
        if (entry.header & HEADER_FLAG_NULL) != 0 {
            Ok(None)
        } else if (entry.header & HEADER_FLAG_INLINED) != 0 {
            let size = (entry.header >> HEADER_FLAGS_WIDTH) as usize;
            Ok(Some(&entry.chars[..size]))
        } else {
            let split: &VarcharAuxSplit = unsafe { std::mem::transmute(entry) };
            let header = split.header;
            let size = (header >> HEADER_FLAGS_WIDTH) as usize;
            let offset = split.offset_lo as usize | ((split.offset_hi as usize) << 16);
            if offset + size > data.len() {
                return Err(fmt_err!(
                    Layout,
                    "data corruption in VARCHAR column: offset {} + size {} exceeds data length {}",
                    offset,
                    size,
                    data.len()
                ));
            }
            Ok(Some(&data[offset..][..size]))
        }
    })))
}

fn read_utf16(entry_tail: &[u8]) -> ParquetResult<Option<&[u16]>> {
    const SIZE_OF_HEADER: usize = std::mem::size_of::<i32>();
    let (header, value_tail) = entry_tail
        .split_at_checked(SIZE_OF_HEADER)
        .ok_or_else(|| fmt_err!(Layout, "not enough bytes for string header"))?;
    let len_raw = types::decode::<i32>(header);
    if len_raw < 0 {
        return Ok(None);
    }
    let utf16_tail: &[u16] = unsafe { transmute_slice(value_tail) };
    let char_count = len_raw as usize;
    let chars = utf16_tail
        .get(..char_count)
        .ok_or_else(|| fmt_err!(Layout, "not enough bytes for string value"))?;
    Ok(Some(chars))
}
