use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use parquet2::bloom_filter::hash_native;
use parquet2::encoding::hybrid_rle::encode_u32;
use parquet2::encoding::Encoding;
use parquet2::page::DictPage;
use parquet2::schema::types::PrimitiveType;
use parquet2::statistics::{
    serialize_statistics, ParquetStatistics, PrimitiveStatistics, Statistics,
};
use parquet2::types::NativeType;
use rapidhash::RapidHashMap;

use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_write::encoders::helpers::{lock_bloom_set, partition_chunk_slice, ChunkSlice};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::schema::Column;
use crate::parquet_write::util::{
    bit_width, build_plain_page, encode_primitive_def_levels, MaxMin,
};

mod fixed;
mod primitive;
#[cfg(test)]
mod tests;
mod varlen;

pub use fixed::{encode_decimal, encode_fixed_len_bytes};
pub use primitive::{encode_int_notnull, encode_int_nullable, encode_simd};
pub use varlen::{encode_binary, encode_string, encode_varchar};

#[derive(Clone, Copy, PartialEq)]
enum Repetition {
    Required,
    Optional,
}

impl Repetition {
    #[inline]
    fn is_required(self) -> bool {
        matches!(self, Repetition::Required)
    }
}

struct PartitionDictState<S> {
    chunk: ChunkSlice,
    keys: Vec<u32>,
    is_not_null: Vec<bool>,
    partition_null_count: usize,
    stats: Option<S>,
}

impl<S> PartitionDictState<S> {
    fn new(chunk: ChunkSlice, stats: Option<S>) -> Self {
        Self {
            chunk,
            keys: Vec::new(),
            is_not_null: Vec::new(),
            partition_null_count: 0,
            stats,
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn encode_primitive<T, P, F, G>(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
    repetition: Repetition,
    column_top_default: Option<P>,
    transmuter: impl Fn(&Column) -> ParquetResult<&[T]>,
    is_null: F,
    project: G,
) -> ParquetResult<Vec<parquet2::page::Page>>
where
    T: Copy + Debug,
    P: NativeType,
    P::Bytes: Eq + Hash,
    F: Fn(T) -> bool,
    G: Fn(T) -> P,
{
    let num_partitions = columns.len();
    let mut dict_map: RapidHashMap<P::Bytes, u32> = RapidHashMap::default();
    let mut dict_entries: Vec<P> = Vec::new();
    let mut per_partition: Vec<PartitionDictState<MaxMin<P>>> = Vec::with_capacity(num_partitions);

    for (part_idx, column) in columns.iter().enumerate() {
        let chunk = partition_chunk_slice(
            part_idx,
            num_partitions,
            column,
            first_partition_start,
            last_partition_end,
        );
        let typed = transmuter(column)?;
        let slice = &typed[chunk.lower_bound..chunk.upper_bound];

        let mut state = PartitionDictState::<MaxMin<P>>::new(
            chunk,
            options.write_statistics.then(MaxMin::<P>::new),
        );

        if repetition.is_required() && chunk.adjusted_column_top > 0 {
            let default_p = column_top_default.ok_or_else(|| {
                fmt_err!(
                    Layout,
                    "encode_primitive: Required column has column top but no default supplied"
                )
            })?;
            let default_key = upsert_dict_entry(&mut dict_map, &mut dict_entries, default_p)?;
            for _ in 0..chunk.adjusted_column_top {
                state.keys.push(default_key);
            }
            if let Some(ref mut stats) = state.stats {
                stats.update(default_p);
            }
        }

        for &value in slice {
            if is_null(value) {
                if repetition.is_required() {
                    return Err(fmt_err!(
                        Layout,
                        "encountered null value in Required column"
                    ));
                }
                state.is_not_null.push(false);
                state.partition_null_count += 1;
            } else {
                let p = project(value);
                let key = upsert_dict_entry(&mut dict_map, &mut dict_entries, p)?;
                state.keys.push(key);
                if !repetition.is_required() {
                    state.is_not_null.push(true);
                }
                if let Some(ref mut stats) = state.stats {
                    stats.update(p);
                }
            }
        }

        per_partition.push(state);
    }

    let mut dict_buffer = Vec::with_capacity(dict_entries.len() * std::mem::size_of::<P>());
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
    let mut pages = Vec::with_capacity(num_partitions + 1);
    pages.push(parquet2::page::Page::Dict(build_dict_page(
        dict_buffer,
        dict_entry_count,
    )));

    for state in per_partition {
        let stats = state.stats.map(|s| {
            build_primitive_stats(
                Some((state.chunk.adjusted_column_top + state.partition_null_count) as i64),
                s,
                primitive_type.clone(),
            )
        });
        let data_page = build_primitive_dict_data_page(
            &state.keys,
            &state.is_not_null,
            state.chunk,
            state.partition_null_count,
            dict_entry_count,
            stats,
            primitive_type,
            options,
            repetition,
        )?;
        pages.push(parquet2::page::Page::Data(data_page));
    }

    Ok(pages)
}

fn upsert_dict_entry<P>(
    dict_map: &mut RapidHashMap<P::Bytes, u32>,
    dict_entries: &mut Vec<P>,
    value: P,
) -> ParquetResult<u32>
where
    P: NativeType,
    P::Bytes: Eq + Hash,
{
    let bytes = value.to_bytes();
    if let Some(&id) = dict_map.get(&bytes) {
        return Ok(id);
    }
    let id = u32::try_from(dict_entries.len())
        .map_err(|_| fmt_err!(Layout, "dictionary exceeds u32::MAX entries"))?;
    dict_map.insert(bytes, id);
    dict_entries.push(value);
    Ok(id)
}

fn build_dict_page(dict_buffer: Vec<u8>, dict_entry_count: usize) -> DictPage {
    let unique_count = if dict_buffer.is_empty() {
        0
    } else {
        dict_entry_count
    };
    DictPage::new(dict_buffer, unique_count, false)
}

fn build_primitive_stats<P: NativeType>(
    null_count: Option<i64>,
    statistics: MaxMin<P>,
    primitive_type: PrimitiveType,
) -> ParquetStatistics {
    let stats = &PrimitiveStatistics::<P> {
        primitive_type,
        null_count,
        distinct_count: None,
        max_value: statistics.max,
        min_value: statistics.min,
    } as &dyn Statistics;
    serialize_statistics(stats)
}

#[allow(clippy::too_many_arguments)]
fn build_primitive_dict_data_page(
    keys: &[u32],
    is_not_null: &[bool],
    chunk: ChunkSlice,
    partition_null_count: usize,
    dict_entry_count: usize,
    statistics: Option<ParquetStatistics>,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    repetition: Repetition,
) -> ParquetResult<parquet2::page::DataPage> {
    let num_rows = chunk.num_rows();
    let required = repetition.is_required();
    let total_null_count = if required {
        0
    } else {
        chunk.adjusted_column_top + partition_null_count
    };
    let non_null_count = if required { num_rows } else { keys.len() };

    let mut buffer: Vec<u8> = Vec::new();

    if !required {
        let column_top = chunk.adjusted_column_top;
        let def_levels = (0..num_rows).map(|i| {
            if i < column_top {
                false
            } else {
                is_not_null[i - column_top]
            }
        });
        encode_primitive_def_levels(&mut buffer, def_levels, num_rows, options.version)?;
    }
    let definition_levels_byte_length = buffer.len();

    let max_key = if dict_entry_count == 0 {
        0u32
    } else {
        (dict_entry_count - 1) as u32
    };
    let bits_per_key = bit_width(max_key as u64);
    buffer.push(bits_per_key);
    encode_u32(
        &mut buffer,
        keys.iter().copied(),
        non_null_count,
        bits_per_key as u32,
    )?;

    build_plain_page(
        buffer,
        num_rows,
        total_null_count,
        definition_levels_byte_length,
        statistics,
        primitive_type.clone(),
        options,
        Encoding::RleDictionary,
        required,
    )
}

#[allow(clippy::too_many_arguments)]
fn build_var_dict_data_page(
    keys: &[u32],
    is_not_null: &[bool],
    chunk: ChunkSlice,
    partition_null_count: usize,
    dict_entry_count: usize,
    statistics: Option<ParquetStatistics>,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    repetition: Repetition,
) -> ParquetResult<parquet2::page::DataPage> {
    build_primitive_dict_data_page(
        keys,
        is_not_null,
        chunk,
        partition_null_count,
        dict_entry_count,
        statistics,
        primitive_type,
        options,
        repetition,
    )
}
