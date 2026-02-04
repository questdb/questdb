use std::fmt::Debug;

use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util::{
    build_plain_page, encode_primitive_def_levels, ExactSizedIter, MaxMin,
};
use crate::parquet_write::Nullable;
use parquet2::encoding::delta_bitpacked::encode;
use parquet2::encoding::Encoding;
use parquet2::page::{DataPage, Page};
use parquet2::schema::types::PrimitiveType;
use parquet2::schema::Repetition;
use parquet2::statistics::{serialize_statistics, ParquetStatistics, PrimitiveStatistics};
use parquet2::types::NativeType;

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
    assert!(primitive_type.field_info.repetition == Repetition::Required);
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
                let v: P = value.as_();
                statistics.update(v);
                true
            }
        }
    });
    let mut buffer = vec![];
    encode_primitive_def_levels(&mut buffer, deflevels_iter, num_rows, options.version)?;

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
    let mut buffer = Vec::with_capacity(std::mem::size_of::<P>() * (column_top + slice.len()));
    for i in 0..column_top + slice.len() {
        let x = if i < column_top {
            T::default()
        } else {
            slice[i - column_top]
        };
        let parquet_native: P = x.as_();
        buffer.extend_from_slice(parquet_native.to_le_bytes().as_ref())
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
    buffer.reserve(std::mem::size_of::<P>() * (slice.len() - null_count));
    for x in slice.iter().filter(|x| !x.is_null()) {
        let parquet_native: P = x.as_();
        buffer.extend_from_slice(parquet_native.to_le_bytes().as_ref())
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
};

/// SIMD-optimized version for i64 slices (Long, Timestamp, Date columns).
pub fn i64_slice_to_page_simd(
    slice: &[i64],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    encoding: Encoding,
) -> ParquetResult<Page> {
    assert!(primitive_type.field_info.repetition == Repetition::Optional);
    let num_rows = column_top + slice.len();

    // Use SIMD to encode definition levels and compute statistics in one pass
    let mut buffer = vec![];

    // For V1, we need to write a 4-byte length prefix placeholder
    let def_levels_start = if matches!(options.version, parquet2::write::Version::V1) {
        buffer.extend_from_slice(&[0; 4]);
        4
    } else {
        0
    };

    let result = encode_i64_def_levels(&mut buffer, slice, column_top, options.write_statistics)
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

    // Encode the actual data
    let buffer = match encoding {
        Encoding::Plain => encode_i64_plain(slice, result.null_count, buffer),
        Encoding::DeltaBinaryPacked => encode_i64_delta(slice, result.null_count, buffer),
        other => {
            return Err(fmt_err!(
                Unsupported,
                "unsupported encoding {other:?} for i64 column"
            ))
        }
    };

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

fn encode_i64_plain(slice: &[i64], null_count: usize, mut buffer: Vec<u8>) -> Vec<u8> {
    let non_null_count = slice.len() - null_count;
    if non_null_count == 0 {
        return buffer;
    }
    buffer.reserve(std::mem::size_of::<i64>() * non_null_count);

    if null_count == 0 {
        // Fast path: no nulls, use memcpy
        // SAFETY: i64 slice can be safely viewed as bytes, and Parquet uses little-endian
        // which matches x86/ARM-LE native byte order
        let bytes =
            unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 8) };
        buffer.extend_from_slice(bytes);
    } else {
        // Slow path: filter out nulls
        for &x in slice.iter().filter(|&&x| x != i64::MIN) {
            buffer.extend_from_slice(&x.to_le_bytes());
        }
    }
    buffer
}

fn encode_i64_delta(slice: &[i64], null_count: usize, mut buffer: Vec<u8>) -> Vec<u8> {
    let non_null_count = slice.len() - null_count;
    if non_null_count == 0 {
        return buffer;
    }
    let iterator = slice.iter().filter(|&&x| x != i64::MIN).copied();
    let iterator = ExactSizedIter::new(iterator, non_null_count);
    encode(iterator, &mut buffer);
    buffer
}

/// SIMD-optimized version for i32 slices (Int columns).
pub fn i32_slice_to_page_simd(
    slice: &[i32],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    encoding: Encoding,
) -> ParquetResult<Page> {
    assert!(primitive_type.field_info.repetition == Repetition::Optional);
    let num_rows = column_top + slice.len();

    let mut buffer = vec![];

    let def_levels_start = if matches!(options.version, parquet2::write::Version::V1) {
        buffer.extend_from_slice(&[0; 4]);
        4
    } else {
        0
    };

    let result = encode_i32_def_levels(&mut buffer, slice, column_top, options.write_statistics)
        .map_err(|e| {
            fmt_err!(
                Io(std::sync::Arc::new(e)),
                "failed to encode definition levels"
            )
        })?;

    if matches!(options.version, parquet2::write::Version::V1) {
        let def_levels_len = (buffer.len() - def_levels_start) as i32;
        buffer[0..4].copy_from_slice(&def_levels_len.to_le_bytes());
    }

    let definition_levels_byte_length = buffer.len();

    let buffer = match encoding {
        Encoding::Plain => encode_i32_plain(slice, result.null_count, buffer),
        Encoding::DeltaBinaryPacked => encode_i32_delta(slice, result.null_count, buffer),
        other => {
            return Err(fmt_err!(
                Unsupported,
                "unsupported encoding {other:?} for i32 column"
            ))
        }
    };

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

fn encode_i32_plain(slice: &[i32], null_count: usize, mut buffer: Vec<u8>) -> Vec<u8> {
    let non_null_count = slice.len() - null_count;
    if non_null_count == 0 {
        return buffer;
    }
    buffer.reserve(std::mem::size_of::<i32>() * non_null_count);

    if null_count == 0 {
        // Fast path: no nulls, use memcpy
        let bytes =
            unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 4) };
        buffer.extend_from_slice(bytes);
    } else {
        // Slow path: filter out nulls
        for &x in slice.iter().filter(|&&x| x != i32::MIN) {
            buffer.extend_from_slice(&x.to_le_bytes());
        }
    }
    buffer
}

fn encode_i32_delta(slice: &[i32], null_count: usize, mut buffer: Vec<u8>) -> Vec<u8> {
    let non_null_count = slice.len() - null_count;
    if non_null_count == 0 {
        return buffer;
    }
    let iterator = slice.iter().filter(|&&x| x != i32::MIN).map(|&x| x as i64);
    let iterator = ExactSizedIter::new(iterator, non_null_count);
    encode(iterator, &mut buffer);
    buffer
}

/// SIMD-optimized version for f64 slices (Double columns).
pub fn f64_slice_to_page_simd(
    slice: &[f64],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
) -> ParquetResult<Page> {
    assert!(primitive_type.field_info.repetition == Repetition::Optional);
    let num_rows = column_top + slice.len();

    let mut buffer = vec![];

    let def_levels_start = if matches!(options.version, parquet2::write::Version::V1) {
        buffer.extend_from_slice(&[0; 4]);
        4
    } else {
        0
    };

    let result = encode_f64_def_levels(&mut buffer, slice, column_top, options.write_statistics)
        .map_err(|e| {
            fmt_err!(
                Io(std::sync::Arc::new(e)),
                "failed to encode definition levels"
            )
        })?;

    if matches!(options.version, parquet2::write::Version::V1) {
        let def_levels_len = (buffer.len() - def_levels_start) as i32;
        buffer[0..4].copy_from_slice(&def_levels_len.to_le_bytes());
    }

    let definition_levels_byte_length = buffer.len();

    // Encode the actual data (Plain encoding only for floats)
    let non_null_count = slice.len() - result.null_count;
    if non_null_count > 0 {
        buffer.reserve(std::mem::size_of::<f64>() * non_null_count);

        if result.null_count == 0 {
            // Fast path: no nulls (NaNs), use memcpy
            let bytes =
                unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 8) };
            buffer.extend_from_slice(bytes);
        } else {
            // Slow path: filter out NaNs
            for &x in slice.iter().filter(|x| !x.is_nan()) {
                buffer.extend_from_slice(&x.to_le_bytes());
            }
        }
    }

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
        Encoding::Plain,
        false,
    )
    .map(Page::Data)
}

/// SIMD-optimized version for f32 slices (Float columns).
pub fn f32_slice_to_page_simd(
    slice: &[f32],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
) -> ParquetResult<Page> {
    assert!(primitive_type.field_info.repetition == Repetition::Optional);
    let num_rows = column_top + slice.len();

    let mut buffer = vec![];

    let def_levels_start = if matches!(options.version, parquet2::write::Version::V1) {
        buffer.extend_from_slice(&[0; 4]);
        4
    } else {
        0
    };

    let result = encode_f32_def_levels(&mut buffer, slice, column_top, options.write_statistics)
        .map_err(|e| {
            fmt_err!(
                Io(std::sync::Arc::new(e)),
                "failed to encode definition levels"
            )
        })?;

    if matches!(options.version, parquet2::write::Version::V1) {
        let def_levels_len = (buffer.len() - def_levels_start) as i32;
        buffer[0..4].copy_from_slice(&def_levels_len.to_le_bytes());
    }

    let definition_levels_byte_length = buffer.len();

    // Encode the actual data (Plain encoding only for floats)
    let non_null_count = slice.len() - result.null_count;
    if non_null_count > 0 {
        buffer.reserve(std::mem::size_of::<f32>() * non_null_count);

        if result.null_count == 0 {
            // Fast path: no nulls (NaNs), use memcpy
            let bytes =
                unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 4) };
            buffer.extend_from_slice(bytes);
        } else {
            // Slow path: filter out NaNs
            for &x in slice.iter().filter(|x| !x.is_nan()) {
                buffer.extend_from_slice(&x.to_le_bytes());
            }
        }
    }

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
        Encoding::Plain,
        false,
    )
    .map(Page::Data)
}
