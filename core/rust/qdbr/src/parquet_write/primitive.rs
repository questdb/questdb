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
    buffer.reserve(size_of::<P>() * (slice.len() - null_count));
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
                        buffer.extend_from_slice(x.to_le_bytes().as_ref());
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
