use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use parquet2::bloom_filter::hash_byte;
use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;
use parquet2::types;
use rapidhash::RapidHashMap;

use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::schema::Column;
use crate::parquet_write::util::{transmute_slice, BinaryMaxMinStats};

use super::{
    build_dict_page, build_var_dict_data_page, lock_bloom_set, partition_chunk_slice,
    PartitionDictState, Repetition,
};

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
    let mut dict_map: RapidHashMap<Vec<u16>, u32> = RapidHashMap::default();
    let mut dict_entries: Vec<Vec<u16>> = Vec::new();

    let mut per_partition: Vec<PartitionDictState<BinaryMaxMinStats>> =
        Vec::with_capacity(num_partitions);

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

        let mut state = PartitionDictState::<BinaryMaxMinStats>::new(
            chunk,
            options
                .write_statistics
                .then(|| BinaryMaxMinStats::new(primitive_type)),
        );

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
                    let next_id = u32::try_from(dict_entries.len())
                        .map_err(|_| fmt_err!(Layout, "dictionary exceeds u32::MAX entries"))?;
                    let key = if let Some(&id) = dict_map.get(utf16) {
                        id
                    } else {
                        let owned: Vec<u16> = utf16.to_vec();
                        let id = next_id;
                        dict_map.insert(owned.clone(), id);
                        dict_entries.push(owned);
                        id
                    };
                    state.keys.push(key);
                    state.is_not_null.push(true);
                    if let Some(ref mut stats) = state.stats {
                        let utf8 = String::from_utf16(utf16).map_err(|e| {
                            fmt_err!(Layout, "invalid UTF-16 data in string column: {e}")
                        })?;
                        stats.update(utf8.as_bytes());
                    }
                }
                None => {
                    state.is_not_null.push(false);
                    state.partition_null_count += 1;
                }
            }
        }

        per_partition.push(state);
    }

    let mut dict_buffer = Vec::new();
    {
        let mut bloom_guard = lock_bloom_set(bloom_set.as_ref())?;
        let mut bloom = bloom_guard.as_deref_mut();
        for utf16 in &dict_entries {
            let utf8 = String::from_utf16(utf16)
                .map_err(|e| fmt_err!(Layout, "invalid UTF-16 in dictionary entry: {e}"))?;
            let bytes = utf8.as_bytes();
            dict_buffer.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            dict_buffer.extend_from_slice(bytes);
            if let Some(ref mut h) = bloom {
                h.insert(hash_byte(bytes));
            }
        }
    }

    let dict_entry_count = dict_entries.len();
    let mut pages = Vec::with_capacity(num_partitions + 1);
    pages.push(Page::Dict(build_dict_page(dict_buffer, dict_entry_count)));

    for state in per_partition {
        let stats = state.stats.map(|s| {
            s.into_parquet_stats(state.chunk.adjusted_column_top + state.partition_null_count)
        });
        let data_page = build_var_dict_data_page(
            &state.keys,
            &state.is_not_null,
            state.chunk,
            state.partition_null_count,
            dict_entry_count,
            stats,
            primitive_type,
            options,
            Repetition::Optional,
        )?;
        pages.push(Page::Data(data_page));
    }

    Ok(pages)
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
            let mut out: Vec<Option<&[u8]>> = Vec::with_capacity(aux_slice.len());
            for offset in aux_slice {
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
                    out.push(None);
                } else {
                    let value_offset = offset + size_of_header;
                    let value_len = len as usize;
                    if value_offset + value_len > data.len() {
                        return Err(fmt_err!(
                            Layout,
                            "invalid offset and length in binary aux column: offset {offset}, length {len}"
                        ));
                    }
                    out.push(Some(&data[value_offset..value_offset + value_len]));
                }
            }
            Ok(out)
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
        varchar_aux_to_byte_slices,
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
    F: Fn(
        &Column,
        crate::parquet_write::encoders::helpers::ChunkSlice,
    ) -> ParquetResult<Vec<Option<&[u8]>>>,
{
    let num_partitions = columns.len();

    struct OwnedSlice<'a> {
        chunk: crate::parquet_write::encoders::helpers::ChunkSlice,
        slices: Vec<Option<&'a [u8]>>,
    }

    let decoded: Vec<OwnedSlice<'_>> = (0..num_partitions)
        .map(|part_idx| -> ParquetResult<OwnedSlice<'_>> {
            let column = &columns[part_idx];
            let chunk = partition_chunk_slice(
                part_idx,
                num_partitions,
                column,
                first_partition_start,
                last_partition_end,
            );
            let slices = decode(column, chunk)?;
            Ok(OwnedSlice { chunk, slices })
        })
        .collect::<ParquetResult<Vec<_>>>()?;

    let mut dict_map: RapidHashMap<&[u8], u32> = RapidHashMap::default();
    let mut dict_entries: Vec<&[u8]> = Vec::new();
    let mut total_keys_bytes = 0usize;
    let mut per_partition: Vec<PartitionDictState<BinaryMaxMinStats>> =
        Vec::with_capacity(num_partitions);

    for owned in &decoded {
        let mut state = PartitionDictState::<BinaryMaxMinStats>::new(
            owned.chunk,
            options
                .write_statistics
                .then(|| BinaryMaxMinStats::new(primitive_type)),
        );

        for slice_opt in &owned.slices {
            match slice_opt {
                Some(s) => {
                    let next_id = u32::try_from(dict_entries.len())
                        .map_err(|_| fmt_err!(Layout, "dictionary exceeds u32::MAX entries"))?;
                    let key = if let Some(&id) = dict_map.get(s) {
                        id
                    } else {
                        let id = next_id;
                        dict_map.insert(*s, id);
                        dict_entries.push(*s);
                        total_keys_bytes += 4 + s.len();
                        id
                    };
                    state.keys.push(key);
                    state.is_not_null.push(true);
                    if let Some(ref mut stats) = state.stats {
                        stats.update(s);
                    }
                }
                None => {
                    state.is_not_null.push(false);
                    state.partition_null_count += 1;
                }
            }
        }

        per_partition.push(state);
    }

    let mut dict_buffer = Vec::with_capacity(total_keys_bytes);
    {
        let mut bloom_guard = lock_bloom_set(bloom_set.as_ref())?;
        let mut bloom = bloom_guard.as_deref_mut();
        for entry in &dict_entries {
            dict_buffer.extend_from_slice(&(entry.len() as u32).to_le_bytes());
            dict_buffer.extend_from_slice(entry);
            if let Some(ref mut h) = bloom {
                h.insert(hash_byte(entry));
            }
        }
    }

    let dict_entry_count = dict_entries.len();
    let mut pages = Vec::with_capacity(num_partitions + 1);
    pages.push(Page::Dict(build_dict_page(dict_buffer, dict_entry_count)));

    for state in per_partition {
        let stats = state.stats.map(|s| {
            s.into_parquet_stats(state.chunk.adjusted_column_top + state.partition_null_count)
        });
        let data_page = build_var_dict_data_page(
            &state.keys,
            &state.is_not_null,
            state.chunk,
            state.partition_null_count,
            dict_entry_count,
            stats,
            primitive_type,
            options,
            Repetition::Optional,
        )?;
        pages.push(Page::Data(data_page));
    }

    Ok(pages)
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

fn varchar_aux_to_byte_slices<'a>(
    column: &'a Column,
    chunk: crate::parquet_write::encoders::helpers::ChunkSlice,
) -> ParquetResult<Vec<Option<&'a [u8]>>> {
    let aux: &[[u8; 16]] = unsafe { transmute_slice(column.secondary_data) };
    let aux_slice = &aux[chunk.lower_bound..chunk.upper_bound];
    let aux_inlined: &[VarcharAuxInlined] = unsafe { std::mem::transmute(aux_slice) };
    let data = column.primary_data;

    let mut out: Vec<Option<&'a [u8]>> = Vec::with_capacity(aux_inlined.len());
    for entry in aux_inlined {
        if (entry.header & HEADER_FLAG_NULL) != 0 {
            out.push(None);
        } else if (entry.header & HEADER_FLAG_INLINED) != 0 {
            let size = (entry.header >> HEADER_FLAGS_WIDTH) as usize;
            out.push(Some(&entry.chars[..size]));
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
            out.push(Some(&data[offset..][..size]));
        }
    }
    Ok(out)
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
