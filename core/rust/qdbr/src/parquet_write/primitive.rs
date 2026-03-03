use std::fmt::Debug;
use std::hash::Hash;

use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util::{
    build_plain_page, dict_pages_iter, encode_primitive_def_levels, ExactSizedIter, MaxMin,
};
use crate::parquet_write::Nullable;
use parquet2::encoding::delta_bitpacked::encode;
use parquet2::encoding::hybrid_rle::encode_u32;
use parquet2::encoding::Encoding;
use parquet2::page::{DataPage, Page};
use parquet2::schema::types::PrimitiveType;
use parquet2::schema::Repetition;
use parquet2::statistics::{
    serialize_statistics, FixedLenStatistics, ParquetStatistics, PrimitiveStatistics,
};
use parquet2::types::NativeType;
use parquet2::write::DynIter;
use rapidhash::RapidHashMap;

pub fn decimal_slice_to_page_plain<T>(
    slice: &[T],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
) -> ParquetResult<Page>
where
    T: Nullable + NativeType + Debug,
{
    assert!(primitive_type.field_info.repetition == Repetition::Optional);
    let num_rows = column_top + slice.len();
    let mut null_count = 0;
    let mut statistics = MaxMin::new();

    let deflevels_iter = (0..num_rows).map(|i| {
        if i < column_top {
            false
        } else {
            let value = slice[i - column_top];
            if value.is_null() {
                null_count += 1;
                false
            } else {
                statistics.update(value);
                true
            }
        }
    });
    let mut buffer = vec![];
    encode_primitive_def_levels(&mut buffer, deflevels_iter, num_rows, options.version)?;

    let definition_levels_byte_length = buffer.len();
    let buffer = encode_plain_nullable_direct(slice, null_count, buffer);

    let statistics = if options.write_statistics {
        let s = &FixedLenStatistics {
            primitive_type: primitive_type.clone(),
            null_count: Some((column_top + null_count) as i64),
            distinct_count: None,
            max_value: statistics.max.map(|x| x.to_bytes().as_ref().to_vec()),
            min_value: statistics.min.map(|x| x.to_bytes().as_ref().to_vec()),
        } as &dyn parquet2::statistics::Statistics;
        Some(serialize_statistics(s))
    } else {
        None
    };

    build_plain_page(
        buffer,
        num_rows,
        column_top + null_count,
        definition_levels_byte_length,
        statistics,
        primitive_type,
        options,
        Encoding::Plain,
        false,
    )
    .map(Page::Data)
}

pub fn int_slice_to_page_nullable<T, P>(
    slice: &[T],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    encoding: Encoding,
) -> ParquetResult<Page>
where
    P: NativeType + num_traits::AsPrimitive<i64>,
    T: Nullable + num_traits::AsPrimitive<P> + Debug,
{
    match encoding {
        Encoding::Plain => slice_to_page_nullable(
            slice,
            column_top,
            options,
            primitive_type,
            encoding,
            encode_plain_nullable,
        ),
        Encoding::DeltaBinaryPacked => slice_to_page_nullable(
            slice,
            column_top,
            options,
            primitive_type,
            encoding,
            encode_delta_nullable,
        ),
        other => {
            return Err(fmt_err!(
                Unsupported,
                "unsupported encoding {other:?} while writing an int column"
            ))
        }
    }
    .map(Page::Data)
}

pub fn int_slice_to_page_notnull<T, P>(
    slice: &[T],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    encoding: Encoding,
) -> ParquetResult<Page>
where
    P: NativeType + num_traits::AsPrimitive<i64>,
    T: Default + num_traits::AsPrimitive<P> + Debug,
{
    match encoding {
        Encoding::Plain => slice_to_page_notnull(
            slice,
            column_top,
            options,
            primitive_type,
            encoding,
            encode_plain_notnull,
        ),
        Encoding::DeltaBinaryPacked => slice_to_page_notnull(
            slice,
            column_top,
            options,
            primitive_type,
            encoding,
            encode_delta_notnull,
        ),
        other => {
            return Err(fmt_err!(
                Unsupported,
                "unsupported encoding {other:?} while writing an int column"
            ))
        }
    }
    .map(Page::Data)
}

fn slice_to_page_notnull<T, P, F: Fn(&[T], usize) -> Vec<u8>>(
    slice: &[T],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    encoding: Encoding,
    encode_fn: F,
) -> ParquetResult<DataPage>
where
    P: NativeType,
    T: Default + num_traits::AsPrimitive<P> + Debug,
{
    assert_eq!(primitive_type.field_info.repetition, Repetition::Required);
    let statistics = if options.write_statistics {
        let mut statistics = MaxMin::new();
        for value in slice {
            statistics.update(value.as_());
        }
        Some(build_statistics(
            Some(column_top as i64),
            statistics,
            primitive_type.clone(),
        ))
    } else {
        None
    };

    build_plain_page(
        encode_fn(slice, column_top),
        column_top + slice.len(),
        column_top,
        0,
        statistics,
        primitive_type,
        options,
        encoding,
        true,
    )
}

fn slice_to_page_nullable<T, P, F>(
    slice: &[T],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    encoding: Encoding,
    encode_fn: F,
) -> ParquetResult<DataPage>
where
    P: NativeType,
    T: Nullable + num_traits::AsPrimitive<P> + Debug,
    F: Fn(&[T], usize, Vec<u8>) -> Vec<u8>,
{
    assert_eq!(primitive_type.field_info.repetition, Repetition::Optional);
    let num_rows = column_top + slice.len();
    let mut null_count = 0;
    let mut statistics = MaxMin::new();

    let def_levels_iter = (0..num_rows).map(|i| {
        if i < column_top {
            false
        } else {
            let value = slice[i - column_top];
            if value.is_null() {
                null_count += 1;
                false
            } else {
                let v: P = value.as_();
                statistics.update(v);
                true
            }
        }
    });
    let mut buffer = vec![];
    encode_primitive_def_levels(&mut buffer, def_levels_iter, num_rows, options.version)?;

    let definition_levels_byte_length = buffer.len();
    let buffer = encode_fn(slice, null_count, buffer);

    let statistics = if options.write_statistics {
        Some(build_statistics(
            Some((column_top + null_count) as i64),
            statistics,
            primitive_type.clone(),
        ))
    } else {
        None
    };

    build_plain_page(
        buffer,
        num_rows,
        column_top + null_count,
        definition_levels_byte_length,
        statistics,
        primitive_type,
        options,
        encoding,
        false,
    )
}

fn encode_plain_notnull<T, P>(slice: &[T], column_top: usize) -> Vec<u8>
where
    P: NativeType,
    T: Default + num_traits::AsPrimitive<P>,
{
    let mut buffer = Vec::with_capacity(size_of::<P>() * (column_top + slice.len()));
    for i in 0..column_top + slice.len() {
        let x = if i < column_top {
            T::default()
        } else {
            slice[i - column_top]
        };
        let parquet_native: P = x.as_();
        buffer.extend_from_slice(parquet_native.to_bytes().as_ref())
    }
    buffer
}

fn encode_delta_notnull<T, P>(slice: &[T], column_top: usize) -> Vec<u8>
where
    P: NativeType + num_traits::AsPrimitive<i64>,
    T: Default + num_traits::AsPrimitive<P>,
{
    let iterator = (0..column_top + slice.len()).map(|i| {
        let x = if i < column_top {
            T::default()
        } else {
            slice[i - column_top]
        };
        let parquet_native: P = x.as_();
        let integer: i64 = parquet_native.as_();
        integer
    });
    let mut buffer = vec![];
    encode(iterator, &mut buffer);
    buffer
}

fn encode_plain_nullable<T, P>(slice: &[T], null_count: usize, mut buffer: Vec<u8>) -> Vec<u8>
where
    P: NativeType,
    T: Nullable + num_traits::AsPrimitive<P>,
{
    buffer.reserve(size_of::<P>() * (slice.len() - null_count));
    for x in slice.iter().filter(|x| !x.is_null()) {
        let parquet_native: P = x.as_();
        buffer.extend_from_slice(parquet_native.to_bytes().as_ref())
    }
    buffer
}

fn encode_plain_nullable_direct<T>(slice: &[T], null_count: usize, mut buffer: Vec<u8>) -> Vec<u8>
where
    T: Nullable + NativeType,
{
    buffer.reserve(std::mem::size_of::<T>() * (slice.len() - null_count));
    for x in slice.iter().filter(|x| !x.is_null()) {
        buffer.extend_from_slice(x.to_bytes().as_ref())
    }
    buffer
}

fn encode_delta_nullable<T, P>(slice: &[T], null_count: usize, mut buffer: Vec<u8>) -> Vec<u8>
where
    P: NativeType + num_traits::AsPrimitive<i64>,
    T: Nullable + num_traits::AsPrimitive<P>,
{
    let iterator = slice.iter().filter(|x| !x.is_null()).map(|x| {
        let parquet_native: P = x.as_();
        let integer: i64 = parquet_native.as_();
        integer
    });
    let iterator = ExactSizedIter::new(iterator, slice.len() - null_count);
    encode(iterator, &mut buffer);
    buffer
}

fn build_statistics<P: NativeType>(
    null_count: Option<i64>,
    statistics: MaxMin<P>,
    primitive_type: PrimitiveType,
) -> ParquetStatistics {
    let statistics = &PrimitiveStatistics::<P> {
        primitive_type,
        null_count,
        distinct_count: None,
        max_value: statistics.max,
        min_value: statistics.min,
    } as &dyn parquet2::statistics::Statistics;
    serialize_statistics(statistics)
}

// =============================================================================
// SIMD-optimized functions for common types
// =============================================================================

use crate::parquet_write::simd::{
    encode_f32_def_levels, encode_f64_def_levels, encode_i32_def_levels, encode_i64_def_levels,
    DefLevelResult,
};

/// Trait for types that can be SIMD-encoded in Parquet pages.
pub trait SimdEncodable: NativeType {
    /// Encode definition levels using SIMD, returns null count and optional stats.
    fn encode_def_levels(
        buffer: &mut Vec<u8>,
        slice: &[Self],
        column_top: usize,
        compute_stats: bool,
    ) -> std::io::Result<DefLevelResult<Self>>;

    /// Check if a value represents null.
    fn is_null(&self) -> bool;

    /// Encode data with delta encoding. Override for types that support it.
    fn encode_delta(_slice: &[Self], _non_null_count: usize, _buffer: &mut Vec<u8>) -> bool {
        false // Not supported by default
    }

    /// Encode data values, dispatching to Plain or Delta based on encoding.
    fn encode_data(
        slice: &[Self],
        null_count: usize,
        encoding: Encoding,
        mut buffer: Vec<u8>,
    ) -> ParquetResult<Vec<u8>> {
        let non_null_count = slice.len() - null_count;
        if non_null_count == 0 {
            return Ok(buffer);
        }

        match encoding {
            Encoding::Plain => {
                buffer.reserve(size_of::<Self>() * non_null_count);
                if null_count == 0 {
                    // Fast path: no nulls, memcpy entire slice
                    let bytes = unsafe {
                        std::slice::from_raw_parts(slice.as_ptr() as *const u8, size_of_val(slice))
                    };
                    buffer.extend_from_slice(bytes);
                } else {
                    // Slow path: filter out nulls
                    for x in slice.iter().filter(|x| !x.is_null()) {
                        buffer.extend_from_slice(x.to_bytes().as_ref());
                    }
                }
                Ok(buffer)
            }
            Encoding::DeltaBinaryPacked => {
                if Self::encode_delta(slice, non_null_count, &mut buffer) {
                    Ok(buffer)
                } else {
                    Err(fmt_err!(
                        Unsupported,
                        "delta encoding not supported for this type"
                    ))
                }
            }
            other => Err(fmt_err!(Unsupported, "unsupported encoding {other:?}")),
        }
    }
}

impl SimdEncodable for i64 {
    fn encode_def_levels(
        buffer: &mut Vec<u8>,
        slice: &[Self],
        column_top: usize,
        compute_stats: bool,
    ) -> std::io::Result<DefLevelResult<Self>> {
        encode_i64_def_levels(buffer, slice, column_top, compute_stats)
    }

    #[inline(always)]
    fn is_null(&self) -> bool {
        *self == i64::MIN
    }

    fn encode_delta(slice: &[Self], non_null_count: usize, buffer: &mut Vec<u8>) -> bool {
        let iterator = slice.iter().filter(|&&x| x != i64::MIN).copied();
        let iterator = ExactSizedIter::new(iterator, non_null_count);
        encode(iterator, buffer);
        true
    }
}

impl SimdEncodable for i32 {
    fn encode_def_levels(
        buffer: &mut Vec<u8>,
        slice: &[Self],
        column_top: usize,
        compute_stats: bool,
    ) -> std::io::Result<DefLevelResult<Self>> {
        encode_i32_def_levels(buffer, slice, column_top, compute_stats)
    }

    #[inline(always)]
    fn is_null(&self) -> bool {
        *self == i32::MIN
    }

    fn encode_delta(slice: &[Self], non_null_count: usize, buffer: &mut Vec<u8>) -> bool {
        let iterator = slice.iter().filter(|&&x| x != i32::MIN).map(|&x| x as i64);
        let iterator = ExactSizedIter::new(iterator, non_null_count);
        encode(iterator, buffer);
        true
    }
}

impl SimdEncodable for f64 {
    fn encode_def_levels(
        buffer: &mut Vec<u8>,
        slice: &[Self],
        column_top: usize,
        compute_stats: bool,
    ) -> std::io::Result<DefLevelResult<Self>> {
        encode_f64_def_levels(buffer, slice, column_top, compute_stats)
    }

    #[inline(always)]
    fn is_null(&self) -> bool {
        self.is_nan()
    }
}

impl SimdEncodable for f32 {
    fn encode_def_levels(
        buffer: &mut Vec<u8>,
        slice: &[Self],
        column_top: usize,
        compute_stats: bool,
    ) -> std::io::Result<DefLevelResult<Self>> {
        encode_f32_def_levels(buffer, slice, column_top, compute_stats)
    }

    #[inline(always)]
    fn is_null(&self) -> bool {
        self.is_nan()
    }
}

/// Generic SIMD-optimized page encoder for nullable columns.
pub fn slice_to_page_simd<T: SimdEncodable>(
    slice: &[T],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    encoding: Encoding,
) -> ParquetResult<Page> {
    assert_eq!(primitive_type.field_info.repetition, Repetition::Optional);
    let num_rows = column_top + slice.len();

    let mut buffer = vec![];

    // For V1, write a 4-byte length prefix placeholder
    let def_levels_start = if matches!(options.version, parquet2::write::Version::V1) {
        buffer.extend_from_slice(&[0; 4]);
        4
    } else {
        0
    };

    let result = T::encode_def_levels(&mut buffer, slice, column_top, options.write_statistics)
        .map_err(|e| {
            fmt_err!(
                Io(std::sync::Arc::new(e)),
                "failed to encode definition levels"
            )
        })?;

    // For V1, write the definition levels length
    if matches!(options.version, parquet2::write::Version::V1) {
        let def_levels_len = (buffer.len() - def_levels_start) as i32;
        buffer[0..4].copy_from_slice(&def_levels_len.to_le_bytes());
    }

    let definition_levels_byte_length = buffer.len();

    let buffer = T::encode_data(slice, result.null_count, encoding, buffer)?;

    let statistics = if options.write_statistics {
        Some(build_statistics(
            Some((column_top + result.null_count) as i64),
            MaxMin { max: result.max, min: result.min },
            primitive_type.clone(),
        ))
    } else {
        None
    };

    build_plain_page(
        buffer,
        num_rows,
        column_top + result.null_count,
        definition_levels_byte_length,
        statistics,
        primitive_type,
        options,
        encoding,
        false,
    )
    .map(Page::Data)
}

// =============================================================================
// Dictionary encoding functions
// =============================================================================

/// Dictionary encoding for SIMD-encodable nullable types (i32, i64, f32, f64).
/// Uses the byte representation as HashMap key to avoid float Eq/Hash issues.
pub fn slice_to_dict_pages_simd<T: SimdEncodable>(
    slice: &[T],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
) -> ParquetResult<DynIter<'static, ParquetResult<Page>>>
where
    T::Bytes: Eq + Hash,
{
    assert_eq!(primitive_type.field_info.repetition, Repetition::Optional);
    let num_rows = column_top + slice.len();
    let value_size = size_of::<T>();

    // Build dictionary: use byte representation as key
    let mut dict_map: RapidHashMap<T::Bytes, u32> = RapidHashMap::default();
    let mut dict_entries: Vec<T> = Vec::new();
    let mut keys: Vec<u32> = Vec::with_capacity(slice.len());
    let mut null_count = 0usize;
    let mut statistics = MaxMin::new();

    for &value in slice {
        if value.is_null() {
            null_count += 1;
        } else {
            statistics.update(value);
            let bytes = value.to_bytes();
            let next_id = dict_entries.len() as u32;
            let key = *dict_map.entry(bytes).or_insert_with(|| {
                dict_entries.push(value);
                next_id
            });
            keys.push(key);
        }
    }

    // Build dict buffer: raw native bytes per unique value
    let mut dict_buffer = Vec::with_capacity(dict_entries.len() * value_size);
    for &entry in &dict_entries {
        dict_buffer.extend_from_slice(entry.to_bytes().as_ref());
    }

    // Encode data page: def levels + bit_width + RLE-encoded keys
    let total_null_count = column_top + null_count;
    let mut data_buffer = Vec::new();

    let def_levels = (0..num_rows).map(|i| {
        if i < column_top {
            false
        } else {
            !slice[i - column_top].is_null()
        }
    });
    encode_primitive_def_levels(&mut data_buffer, def_levels, num_rows, options.version)?;
    let definition_levels_byte_length = data_buffer.len();

    let max_key = if dict_entries.is_empty() {
        0u32
    } else {
        (dict_entries.len() - 1) as u32
    };
    let bits_per_key = super::util::bit_width(max_key as u64);
    let non_null_len = slice.len() - null_count;
    data_buffer.push(bits_per_key);
    encode_u32(
        &mut data_buffer,
        keys.into_iter(),
        non_null_len,
        bits_per_key as u32,
    )?;

    let stats = if options.write_statistics {
        Some(build_statistics(
            Some(total_null_count as i64),
            statistics,
            primitive_type.clone(),
        ))
    } else {
        None
    };

    let data_page = build_plain_page(
        data_buffer,
        num_rows,
        total_null_count,
        definition_levels_byte_length,
        stats,
        primitive_type,
        options,
        Encoding::RleDictionary,
        false,
    )?;

    let unique_count = if dict_buffer.is_empty() {
        0
    } else {
        dict_entries.len()
    };
    Ok(dict_pages_iter(dict_buffer, unique_count, data_page))
}

/// Dictionary encoding for nullable int types that need type conversion (Geo*, IPv4).
pub fn int_slice_to_dict_pages_nullable<T, P>(
    slice: &[T],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
) -> ParquetResult<DynIter<'static, ParquetResult<Page>>>
where
    P: NativeType,
    P::Bytes: Eq + Hash,
    T: Nullable + num_traits::AsPrimitive<P> + Copy + Debug,
{
    assert_eq!(primitive_type.field_info.repetition, Repetition::Optional);
    let num_rows = column_top + slice.len();
    let value_size = size_of::<P>();

    let mut dict_map: RapidHashMap<P::Bytes, u32> = RapidHashMap::default();
    let mut dict_entries: Vec<P> = Vec::new();
    let mut keys: Vec<u32> = Vec::with_capacity(slice.len());
    let mut null_count = 0usize;
    let mut statistics = MaxMin::new();

    for &value in slice {
        if value.is_null() {
            null_count += 1;
        } else {
            let p: P = value.as_();
            statistics.update(p);
            let bytes = p.to_bytes();
            let next_id = dict_entries.len() as u32;
            let key = *dict_map.entry(bytes).or_insert_with(|| {
                dict_entries.push(p);
                next_id
            });
            keys.push(key);
        }
    }

    let mut dict_buffer = Vec::with_capacity(dict_entries.len() * value_size);
    for &entry in &dict_entries {
        dict_buffer.extend_from_slice(entry.to_bytes().as_ref());
    }

    let total_null_count = column_top + null_count;
    let mut data_buffer = Vec::new();

    let def_levels = (0..num_rows).map(|i| {
        if i < column_top {
            false
        } else {
            !slice[i - column_top].is_null()
        }
    });
    encode_primitive_def_levels(&mut data_buffer, def_levels, num_rows, options.version)?;
    let definition_levels_byte_length = data_buffer.len();

    let max_key = if dict_entries.is_empty() {
        0u32
    } else {
        (dict_entries.len() - 1) as u32
    };
    let bits_per_key = super::util::bit_width(max_key as u64);
    let non_null_len = slice.len() - null_count;
    data_buffer.push(bits_per_key);
    encode_u32(
        &mut data_buffer,
        keys.into_iter(),
        non_null_len,
        bits_per_key as u32,
    )?;

    let stats = if options.write_statistics {
        Some(build_statistics(
            Some(total_null_count as i64),
            statistics,
            primitive_type.clone(),
        ))
    } else {
        None
    };

    let data_page = build_plain_page(
        data_buffer,
        num_rows,
        total_null_count,
        definition_levels_byte_length,
        stats,
        primitive_type,
        options,
        Encoding::RleDictionary,
        false,
    )?;

    let unique_count = if dict_buffer.is_empty() {
        0
    } else {
        dict_entries.len()
    };
    Ok(dict_pages_iter(dict_buffer, unique_count, data_page))
}

/// Dictionary encoding for not-null int types (Byte, Short, Char) — Required repetition,
/// no def levels. Column top rows use T::default() as the dict value.
pub fn int_slice_to_dict_pages_notnull<T, P>(
    slice: &[T],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
) -> ParquetResult<DynIter<'static, ParquetResult<Page>>>
where
    P: NativeType,
    P::Bytes: Eq + Hash,
    T: Default + num_traits::AsPrimitive<P> + Copy + Debug,
{
    assert_eq!(primitive_type.field_info.repetition, Repetition::Required);
    let num_rows = column_top + slice.len();
    let value_size = size_of::<P>();

    let mut dict_map: RapidHashMap<P::Bytes, u32> = RapidHashMap::default();
    let mut dict_entries: Vec<P> = Vec::new();
    let mut keys: Vec<u32> = Vec::with_capacity(num_rows);
    let mut statistics = MaxMin::new();

    // Column top rows use the default value
    if column_top > 0 {
        let default_p: P = T::default().as_();
        let bytes = default_p.to_bytes();
        let next_id = dict_entries.len() as u32;
        let key = *dict_map.entry(bytes).or_insert_with(|| {
            dict_entries.push(default_p);
            next_id
        });
        for _ in 0..column_top {
            keys.push(key);
        }
    }

    for &value in slice {
        let p: P = value.as_();
        statistics.update(p);
        let bytes = p.to_bytes();
        let next_id = dict_entries.len() as u32;
        let key = *dict_map.entry(bytes).or_insert_with(|| {
            dict_entries.push(p);
            next_id
        });
        keys.push(key);
    }

    let mut dict_buffer = Vec::with_capacity(dict_entries.len() * value_size);
    for &entry in &dict_entries {
        dict_buffer.extend_from_slice(entry.to_bytes().as_ref());
    }

    let mut data_buffer = Vec::new();
    // No def levels for Required columns

    let max_key = if dict_entries.is_empty() {
        0u32
    } else {
        (dict_entries.len() - 1) as u32
    };
    let bits_per_key = super::util::bit_width(max_key as u64);
    data_buffer.push(bits_per_key);
    encode_u32(
        &mut data_buffer,
        keys.into_iter(),
        num_rows,
        bits_per_key as u32,
    )?;

    let stats = if options.write_statistics {
        Some(build_statistics(
            Some(column_top as i64),
            statistics,
            primitive_type.clone(),
        ))
    } else {
        None
    };

    let data_page = build_plain_page(
        data_buffer,
        num_rows,
        column_top,
        0, // no def levels
        stats,
        primitive_type,
        options,
        Encoding::RleDictionary,
        true,
    )?;

    let unique_count = if dict_buffer.is_empty() {
        0
    } else {
        dict_entries.len()
    };
    Ok(dict_pages_iter(dict_buffer, unique_count, data_page))
}

/// Dictionary encoding for Decimal types (FixedLenByteArray).
pub fn decimal_slice_to_dict_pages<T>(
    slice: &[T],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
) -> ParquetResult<DynIter<'static, ParquetResult<Page>>>
where
    T: Nullable + NativeType + Debug,
    T::Bytes: Eq + Hash,
{
    assert_eq!(primitive_type.field_info.repetition, Repetition::Optional);
    let num_rows = column_top + slice.len();

    let mut dict_map: RapidHashMap<T::Bytes, u32> = RapidHashMap::default();
    let mut dict_entries: Vec<T> = Vec::new();
    let mut keys: Vec<u32> = Vec::with_capacity(slice.len());
    let mut null_count = 0usize;
    let mut statistics = MaxMin::new();

    for &value in slice {
        if value.is_null() {
            null_count += 1;
        } else {
            statistics.update(value);
            let bytes = value.to_bytes();
            let next_id = dict_entries.len() as u32;
            let key = *dict_map.entry(bytes).or_insert_with(|| {
                dict_entries.push(value);
                next_id
            });
            keys.push(key);
        }
    }

    let mut dict_buffer = Vec::with_capacity(dict_entries.len() * size_of::<T>());
    for &entry in &dict_entries {
        dict_buffer.extend_from_slice(entry.to_bytes().as_ref());
    }

    let total_null_count = column_top + null_count;
    let mut data_buffer = Vec::new();

    let def_levels = (0..num_rows).map(|i| {
        if i < column_top {
            false
        } else {
            !slice[i - column_top].is_null()
        }
    });
    encode_primitive_def_levels(&mut data_buffer, def_levels, num_rows, options.version)?;
    let definition_levels_byte_length = data_buffer.len();

    let max_key = if dict_entries.is_empty() {
        0u32
    } else {
        (dict_entries.len() - 1) as u32
    };
    let bits_per_key = super::util::bit_width(max_key as u64);
    let non_null_len = slice.len() - null_count;
    data_buffer.push(bits_per_key);
    encode_u32(
        &mut data_buffer,
        keys.into_iter(),
        non_null_len,
        bits_per_key as u32,
    )?;

    let stats = if options.write_statistics {
        let s = &PrimitiveStatistics::<T> {
            primitive_type: primitive_type.clone(),
            null_count: Some(total_null_count as i64),
            distinct_count: None,
            max_value: statistics.max,
            min_value: statistics.min,
        } as &dyn parquet2::statistics::Statistics;
        Some(serialize_statistics(s))
    } else {
        None
    };

    let data_page = build_plain_page(
        data_buffer,
        num_rows,
        total_null_count,
        definition_levels_byte_length,
        stats,
        primitive_type,
        options,
        Encoding::RleDictionary,
        false,
    )?;

    let unique_count = if dict_buffer.is_empty() {
        0
    } else {
        dict_entries.len()
    };
    Ok(dict_pages_iter(dict_buffer, unique_count, data_page))
}
