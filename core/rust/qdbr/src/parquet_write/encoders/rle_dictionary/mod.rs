use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use parquet2::bloom_filter::{hash_byte, hash_native};
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
use crate::parquet_write::encoders::helpers::{
    column_chunk_row_count, lock_bloom_set, partition_chunk_slice, write_utf8_from_utf16_iter,
    FlatValidity,
};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::schema::Column;
use crate::parquet_write::util::{bit_width, build_plain_page, BinaryMaxMinStats, MaxMin};

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

struct ColumnChunkDictState<S> {
    num_rows: usize,
    keys: Vec<u32>,
    validity: Option<FlatValidity>,
    null_count: usize,
    stats: Option<S>,
}

impl<S> ColumnChunkDictState<S> {
    fn new(repetition: Repetition, num_rows: usize, stats: Option<S>) -> Self {
        let mut validity = (!repetition.is_required()).then(FlatValidity::new);
        if let Some(validity) = validity.as_mut() {
            validity.reset(num_rows);
        }
        Self {
            num_rows: 0,
            keys: Vec::with_capacity(num_rows),
            validity,
            null_count: 0,
            stats,
        }
    }

    #[inline]
    fn push_optional_null(&mut self) -> ParquetResult<()> {
        self.num_rows += 1;
        self.null_count += 1;
        self.validity
            .as_mut()
            .ok_or_else(|| fmt_err!(Layout, "optional dictionary state must track validity"))?
            .push_null();
        Ok(())
    }

    #[inline]
    fn push_optional_value(&mut self, key: u32) -> ParquetResult<()> {
        self.num_rows += 1;
        self.keys.push(key);
        self.validity
            .as_mut()
            .ok_or_else(|| fmt_err!(Layout, "optional dictionary state must track validity"))?
            .push_present();
        Ok(())
    }

    #[inline]
    fn push_required_value(&mut self, key: u32) {
        self.num_rows += 1;
        self.keys.push(key);
    }

    #[inline]
    fn extend_optional_nulls(&mut self, count: usize) -> ParquetResult<()> {
        for _ in 0..count {
            self.push_optional_null()?;
        }
        Ok(())
    }

    #[inline]
    fn extend_required_values(&mut self, key: u32, count: usize) {
        self.num_rows += count;
        self.keys.resize(self.keys.len() + count, key);
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
    let total_rows = column_chunk_row_count(columns, first_partition_start, last_partition_end);
    if total_rows == 0 {
        return Ok(vec![]);
    }
    let mut dict_map: RapidHashMap<P::Bytes, u32> = RapidHashMap::default();
    let mut dict_entries: Vec<P> = Vec::new();
    let mut state = ColumnChunkDictState::<MaxMin<P>>::new(
        repetition,
        total_rows,
        options.write_statistics.then(MaxMin::<P>::new),
    );

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

        if repetition.is_required() && chunk.adjusted_column_top > 0 {
            let default_p = column_top_default.ok_or_else(|| {
                fmt_err!(
                    Layout,
                    "encode_primitive: Required column has column top but no default supplied"
                )
            })?;
            let default_key = upsert_dict_entry(
                &mut dict_map,
                &mut dict_entries,
                default_p,
                state.stats.as_mut(),
            )?;
            state.extend_required_values(default_key, chunk.adjusted_column_top);
        } else if chunk.adjusted_column_top > 0 {
            state.extend_optional_nulls(chunk.adjusted_column_top)?;
        }

        for &value in slice {
            if is_null(value) {
                if repetition.is_required() {
                    return Err(fmt_err!(
                        Layout,
                        "encountered null value in Required column"
                    ));
                }
                state.push_optional_null()?;
            } else {
                let p = project(value);
                let key =
                    upsert_dict_entry(&mut dict_map, &mut dict_entries, p, state.stats.as_mut())?;
                if repetition.is_required() {
                    state.push_required_value(key);
                } else {
                    state.push_optional_value(key)?;
                }
            }
        }
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
    let stats = state
        .stats
        .map(|s| build_primitive_stats(Some(state.null_count as i64), s, primitive_type.clone()));
    let data_page = build_primitive_dict_data_page(
        &state.keys,
        state.validity.as_ref(),
        state.num_rows,
        state.null_count,
        dict_entry_count,
        stats,
        primitive_type,
        options,
        repetition,
    )?;
    Ok(vec![
        parquet2::page::Page::Dict(build_dict_page(dict_buffer, dict_entry_count)),
        parquet2::page::Page::Data(data_page),
    ])
}

#[inline]
fn upsert_dict_entry<P>(
    dict_map: &mut RapidHashMap<P::Bytes, u32>,
    dict_entries: &mut Vec<P>,
    value: P,
    stats: Option<&mut MaxMin<P>>,
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
    if let Some(stats) = stats {
        stats.update(value);
    }
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

/// Serialize variable-length dict entries into the dict buffer, compute bloom hashes,
/// build the dict + data pages, and return the final page vector.
#[allow(clippy::too_many_arguments)]
fn build_varlen_dict_pages<'a>(
    dict_entries: impl ExactSizeIterator<Item = &'a [u8]>,
    total_dict_bytes: usize,
    state: ColumnChunkDictState<BinaryMaxMinStats>,
    bloom_set: Option<&Arc<Mutex<HashSet<u64>>>>,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
) -> ParquetResult<Vec<parquet2::page::Page>> {
    let dict_entry_count = dict_entries.len();
    let mut dict_buffer = Vec::with_capacity(total_dict_bytes);
    {
        let mut bloom_guard = lock_bloom_set(bloom_set)?;
        let mut bloom = bloom_guard.as_deref_mut();
        for entry in dict_entries {
            dict_buffer.extend_from_slice(&(entry.len() as u32).to_le_bytes());
            dict_buffer.extend_from_slice(entry);
            if let Some(ref mut h) = bloom {
                h.insert(hash_byte(entry));
            }
        }
    }
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
        parquet2::page::Page::Dict(build_dict_page(dict_buffer, dict_entry_count)),
        parquet2::page::Page::Data(data_page),
    ])
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
    validity: Option<&FlatValidity>,
    num_rows: usize,
    null_count: usize,
    dict_entry_count: usize,
    statistics: Option<ParquetStatistics>,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    repetition: Repetition,
) -> ParquetResult<parquet2::page::DataPage> {
    let required = repetition.is_required();
    let total_null_count = if required { 0 } else { null_count };
    let non_null_count = if required { num_rows } else { keys.len() };

    let mut buffer: Vec<u8> = Vec::new();

    if !required {
        validity
            .ok_or_else(|| fmt_err!(Layout, "optional dictionary data page must have validity"))?
            .encode_def_levels(&mut buffer, options.version)?;
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
