use std::collections::HashSet;
use std::fmt::Debug;

use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util::{
    build_plain_page, encode_primitive_def_levels, ExactSizedIter, MaxMin,
};
use crate::parquet_write::Nullable;
use parquet2::bloom_filter::hash_native;
use parquet2::encoding::delta_bitpacked::encode;
use parquet2::encoding::Encoding;
use parquet2::page::{DataPage, Page};
use parquet2::schema::types::PrimitiveType;
use parquet2::schema::Repetition;
use parquet2::statistics::{serialize_statistics, ParquetStatistics, PrimitiveStatistics};
use parquet2::types::NativeType;

pub fn float_slice_to_page_plain<T, P>(
    slice: &[T],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<Page>
where
    P: NativeType,
    T: Nullable + num_traits::AsPrimitive<P> + num_traits::Float + From<i8> + Debug,
{
    slice_to_page_nullable(
        slice,
        column_top,
        options,
        primitive_type,
        Encoding::Plain,
        encode_plain_nullable,
        bloom_hashes,
    )
    .map(Page::Data)
}

pub fn int_slice_to_page_nullable<T, P>(
    slice: &[T],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    encoding: Encoding,
    bloom_hashes: Option<&mut HashSet<u64>>,
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
            bloom_hashes,
        ),
        Encoding::DeltaBinaryPacked => slice_to_page_nullable(
            slice,
            column_top,
            options,
            primitive_type,
            encoding,
            encode_delta_nullable,
            bloom_hashes,
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
    bloom_hashes: Option<&mut HashSet<u64>>,
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
            bloom_hashes,
        ),
        Encoding::DeltaBinaryPacked => slice_to_page_notnull(
            slice,
            column_top,
            options,
            primitive_type,
            encoding,
            encode_delta_notnull,
            bloom_hashes,
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
    bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<DataPage>
where
    P: NativeType,
    T: Default + num_traits::AsPrimitive<P> + Debug,
{
    assert!(primitive_type.field_info.repetition == Repetition::Required);

    let statistics = match (options.write_statistics, bloom_hashes) {
        (true, Some(h)) => {
            let mut statistics = MaxMin::new();
            for value in slice {
                let v: P = value.as_();
                statistics.update(v);
                h.insert(hash_native(v));
            }
            Some(build_statistics(
                Some(column_top as i64),
                statistics,
                primitive_type.clone(),
            ))
        }
        (true, None) => {
            let mut statistics = MaxMin::new();
            for value in slice {
                statistics.update(value.as_());
            }
            Some(build_statistics(
                Some(column_top as i64),
                statistics,
                primitive_type.clone(),
            ))
        }
        (false, Some(h)) => {
            for value in slice {
                h.insert(hash_native(value.as_()));
            }
            None
        }
        (false, None) => None,
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
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<DataPage>
where
    P: NativeType,
    T: Nullable + num_traits::AsPrimitive<P> + Debug,
    F: Fn(&[T], usize, Vec<u8>) -> Vec<u8>,
{
    assert!(primitive_type.field_info.repetition == Repetition::Optional);
    let num_rows = column_top + slice.len();
    let mut null_count = 0;
    let write_stats = options.write_statistics;
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
                if write_stats {
                    statistics.update(v);
                }
                if let Some(ref mut h) = bloom_hashes {
                    h.insert(hash_native(v));
                }
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
