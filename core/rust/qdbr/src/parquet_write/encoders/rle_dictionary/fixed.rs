use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use parquet2::bloom_filter::{hash_byte, hash_native};
use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;
use parquet2::statistics::{
    serialize_statistics, FixedLenStatistics, ParquetStatistics, Statistics,
};
use parquet2::types::NativeType;
use rapidhash::RapidHashMap;

use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::schema::Column;
use crate::parquet_write::util::{transmute_slice, BinaryMaxMinStats, MaxMin};
use crate::parquet_write::Nullable;

use super::{
    build_dict_page, build_primitive_dict_data_page, column_chunk_row_count, lock_bloom_set,
    partition_chunk_slice, upsert_dict_entry, ColumnChunkDictState, Repetition,
};

/// Encode a Decimal{8,16,32,64,128,256} column as RleDictionary pages.
pub fn encode_decimal<T>(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>>
where
    T: Nullable + NativeType + Debug,
    T::Bytes: Eq + Hash,
{
    encode_decimal_inner::<T>(
        columns,
        first_partition_start,
        last_partition_end,
        primitive_type,
        options,
        bloom_set,
    )
}

/// Encode a FixedLenByteArray column (Long128, Long256, UUID, Decimal FLBA)
/// as RleDictionary pages. `reverse` swaps endianness for UUID columns.
pub fn encode_fixed_len_bytes<const N: usize>(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    reverse: bool,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>> {
    let num_partitions = columns.len();
    let total_rows = column_chunk_row_count(columns, first_partition_start, last_partition_end);
    if total_rows == 0 {
        return Ok(vec![]);
    }
    let null_value = fixed_len_null_value::<N>();

    let mut dict_map: RapidHashMap<[u8; N], u32> = RapidHashMap::default();
    let mut dict_entries: Vec<[u8; N]> = Vec::new();
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
        let data: &[[u8; N]] = unsafe { transmute_slice(column.primary_data) };
        let slice = &data[chunk.lower_bound..chunk.upper_bound];

        state.extend_optional_nulls(chunk.adjusted_column_top)?;

        for &value in slice {
            if value == null_value {
                state.push_optional_null()?;
            } else {
                let stored = if reverse {
                    let mut r = value;
                    r.reverse();
                    r
                } else {
                    value
                };
                let next_id = u32::try_from(dict_entries.len())
                    .map_err(|_| fmt_err!(Layout, "dictionary exceeds u32::MAX entries"))?;
                let key = *dict_map.entry(stored).or_insert_with(|| {
                    dict_entries.push(stored);
                    if let Some(ref mut stats) = state.stats {
                        stats.update(&stored);
                    }
                    next_id
                });
                state.push_optional_value(key)?;
            }
        }
    }

    let mut dict_buffer = Vec::with_capacity(dict_entries.len() * N);
    {
        let mut bloom_guard = lock_bloom_set(bloom_set.as_ref())?;
        let mut bloom = bloom_guard.as_deref_mut();
        for entry in &dict_entries {
            dict_buffer.extend_from_slice(entry);
            if let Some(ref mut h) = bloom {
                h.insert(hash_byte(entry));
            }
        }
    }

    let dict_entry_count = dict_entries.len();
    let stats = state.stats.map(|s| s.into_parquet_stats(state.null_count));
    let data_page = build_primitive_dict_data_page(
        &state.keys,
        state.validity.as_ref(),
        state.num_rows,
        state.null_count,
        dict_entry_count,
        stats,
        primitive_type,
        options,
        Repetition::Optional,
    )?;
    Ok(vec![
        Page::Dict(build_dict_page(dict_buffer, dict_entry_count)),
        Page::Data(data_page),
    ])
}

fn encode_decimal_inner<T>(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>>
where
    T: Nullable + NativeType + Debug,
    T::Bytes: Eq + Hash,
{
    let num_partitions = columns.len();
    let total_rows = column_chunk_row_count(columns, first_partition_start, last_partition_end);
    if total_rows == 0 {
        return Ok(vec![]);
    }
    let mut dict_map: RapidHashMap<T::Bytes, u32> = RapidHashMap::default();
    let mut dict_entries: Vec<T> = Vec::new();
    let mut state = ColumnChunkDictState::<MaxMin<T>>::new(
        Repetition::Optional,
        total_rows,
        options.write_statistics.then(MaxMin::<T>::new),
    );

    for (part_idx, column) in columns.iter().enumerate() {
        let chunk = partition_chunk_slice(
            part_idx,
            num_partitions,
            column,
            first_partition_start,
            last_partition_end,
        );
        let data: &[T] = unsafe { transmute_slice(column.primary_data) };
        let slice = &data[chunk.lower_bound..chunk.upper_bound];

        state.extend_optional_nulls(chunk.adjusted_column_top)?;

        for &value in slice {
            if value.is_null() {
                state.push_optional_null()?;
            } else {
                let key = upsert_dict_entry(
                    &mut dict_map,
                    &mut dict_entries,
                    value,
                    state.stats.as_mut(),
                )?;
                state.push_optional_value(key)?;
            }
        }
    }

    let mut dict_buffer = Vec::with_capacity(dict_entries.len() * std::mem::size_of::<T>());
    {
        let mut bloom_guard = lock_bloom_set(bloom_set.as_ref())?;
        let mut bloom = bloom_guard.as_deref_mut();
        for entry in &dict_entries {
            dict_buffer.extend_from_slice(entry.to_bytes().as_ref());
            if let Some(ref mut h) = bloom {
                h.insert(hash_native(*entry));
            }
        }
    }

    let dict_entry_count = dict_entries.len();
    let stats = state.stats.map(|s| {
        build_decimal_stats::<T>(Some(state.null_count as i64), s, primitive_type.clone())
    });
    let data_page = build_primitive_dict_data_page(
        &state.keys,
        state.validity.as_ref(),
        state.num_rows,
        state.null_count,
        dict_entry_count,
        stats,
        primitive_type,
        options,
        Repetition::Optional,
    )?;
    Ok(vec![
        Page::Dict(build_dict_page(dict_buffer, dict_entry_count)),
        Page::Data(data_page),
    ])
}

fn build_decimal_stats<T: NativeType>(
    null_count: Option<i64>,
    statistics: MaxMin<T>,
    primitive_type: PrimitiveType,
) -> ParquetStatistics {
    let stats = &FixedLenStatistics {
        primitive_type,
        null_count,
        distinct_count: None,
        max_value: statistics.max.map(|x| x.to_bytes().as_ref().to_vec()),
        min_value: statistics.min.map(|x| x.to_bytes().as_ref().to_vec()),
    } as &dyn Statistics;
    serialize_statistics(stats)
}

fn fixed_len_null_value<const N: usize>() -> [u8; N] {
    let mut null_value = [0u8; N];
    let long_as_bytes = i64::MIN.to_le_bytes();
    for i in 0..N {
        null_value[i] = long_as_bytes[i % long_as_bytes.len()];
    }
    null_value
}
