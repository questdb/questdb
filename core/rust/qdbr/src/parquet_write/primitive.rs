use std::fmt::Debug;

use parquet2::encoding::delta_bitpacked::encode;
use parquet2::encoding::Encoding;
use parquet2::page::{DataPage, Page};
use parquet2::schema::types::PrimitiveType;
use parquet2::schema::Repetition;
use parquet2::statistics::{serialize_statistics, ParquetStatistics, PrimitiveStatistics};
use parquet2::types::NativeType;

use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util::{build_plain_page, encode_bool_iter, ExactSizedIter, MaxMin};
use crate::parquet_write::{Nullable, ParquetError, ParquetResult};

pub fn float_slice_to_page_plain<T, P>(
    slice: &[T],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
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
        other => Err(ParquetError::OutOfSpec(format!(
            "Encoding integer as {:?}",
            other
        )))?,
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
        other => Err(ParquetError::OutOfSpec(format!(
            "Encoding integer as {:?}",
            other
        )))?,
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
    encode_bool_iter(&mut buffer, deflevels_iter, options.version)?;

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
