use std::fmt::Debug;

use num_traits::Bounded;
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

fn encode_plain<T: From<i8> + Debug, P>(
    slice: &[T],
    column_top: usize,
    is_nullable: bool,
    null_count: usize,
    mut buffer: Vec<u8>,
) -> Vec<u8>
where
    P: NativeType,
    T: num_traits::AsPrimitive<P> + Nullable,
{
    if is_nullable {
        buffer.reserve(std::mem::size_of::<P>() * (slice.len() - null_count));
        // append the non-null values
        for x in slice.iter().filter(|x| !x.is_null()) {
            let parquet_native: P = x.as_();
            buffer.extend_from_slice(parquet_native.to_le_bytes().as_ref())
        }
    } else {
        buffer.reserve(std::mem::size_of::<P>() * (column_top + slice.len()));
        // append all values
        for i in 0..column_top + slice.len() {
            let x = if i < column_top {
                T::from(0i8)
            } else {
                slice[i - column_top]
            };
            let parquet_native: P = x.as_();
            buffer.extend_from_slice(parquet_native.to_le_bytes().as_ref())
        }
    }
    buffer
}

fn encode_delta<T: From<i8>, P>(
    slice: &[T],
    column_top: usize,
    is_nullable: bool,
    null_count: usize,
    mut buffer: Vec<u8>,
) -> Vec<u8>
where
    P: NativeType,
    T: num_traits::AsPrimitive<P> + Nullable,
    P: num_traits::AsPrimitive<i64>,
{
    if is_nullable {
        // append the non-null values
        let iterator = slice.iter().filter(|x| !x.is_null()).map(|x| {
            let parquet_native: P = x.as_();
            let integer: i64 = parquet_native.as_();
            integer
        });
        let iterator = ExactSizedIter::new(iterator, slice.len() - null_count);
        encode(iterator, &mut buffer)
    } else {
        // append all values
        let iterator = (0..column_top + slice.len()).map(|i| {
            let x = if i < column_top {
                T::from(0i8)
            } else {
                slice[i - column_top]
            };
            let parquet_native: P = x.as_();
            let integer: i64 = parquet_native.as_();
            integer
        });
        encode(iterator, &mut buffer)
    }
    buffer
}

// floats encoding
pub fn float_slice_to_page_plain<T: From<i8> + Debug, P>(
    slice: &[T],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
) -> ParquetResult<Page>
where
    P: NativeType + Bounded,
    T: num_traits::AsPrimitive<P> + Nullable + num_traits::Float + Bounded,
{
    let is_nullable = primitive_type.field_info.repetition == Repetition::Optional;
    slice_to_page(
        slice,
        column_top,
        is_nullable,
        options,
        primitive_type,
        Encoding::Plain,
        encode_plain,
    )
    .map(Page::Data)
}

pub fn int_slice_to_page<T: From<i8>, P>(
    slice: &[T],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    encoding: Encoding,
) -> ParquetResult<Page>
where
    P: NativeType + Bounded,
    T: num_traits::AsPrimitive<P> + Bounded + Nullable + Debug,
    P: num_traits::AsPrimitive<i64>,
{
    let is_nullable = primitive_type.field_info.repetition == Repetition::Optional;
    match encoding {
        Encoding::Plain => slice_to_page(
            slice,
            column_top,
            is_nullable,
            options,
            primitive_type,
            encoding,
            encode_plain,
        ),
        Encoding::DeltaBinaryPacked => slice_to_page(
            slice,
            column_top,
            is_nullable,
            options,
            primitive_type,
            encoding,
            encode_delta,
        ),
        other => Err(ParquetError::OutOfSpec(format!(
            "Encoding integer as {:?}",
            other
        )))?,
    }
    .map(Page::Data)
}

pub fn slice_to_page<T, P, F: Fn(&[T], usize, bool, usize, Vec<u8>) -> Vec<u8>>(
    slice: &[T],
    column_top: usize,
    is_nullable: bool,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    encoding: Encoding,
    encode_fn: F,
) -> ParquetResult<DataPage>
where
    P: NativeType + Bounded,
    T: num_traits::AsPrimitive<P> + Nullable + Bounded + Debug,
{
    let mut buffer = vec![];
    let mut null_count = 0;
    let mut statistics = MaxMin::new();
    if is_nullable {
        let deflevels_iter = (0..column_top + slice.len()).map(|i| {
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
        encode_bool_iter(&mut buffer, deflevels_iter, options.version)?;
    };

    let definition_levels_byte_length = buffer.len();
    let buffer = encode_fn(slice, column_top, is_nullable, null_count, buffer);

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
        slice.len(),
        slice.len(),
        column_top + null_count,
        definition_levels_byte_length,
        statistics,
        primitive_type,
        options,
        encoding,
    )
}

fn build_statistics<P>(
    null_count: Option<i64>,
    statistics: MaxMin<P>,
    primitive_type: PrimitiveType,
) -> ParquetStatistics
where
    P: NativeType + Bounded,
{
    let (max, min) = statistics.get_current_values();
    let statistics = &PrimitiveStatistics::<P> {
        primitive_type,
        // null_count: if null_count == 0 {None} else {Some(null_count as i64)},
        null_count,
        distinct_count: None,
        max_value: max,
        min_value: min,
    } as &dyn parquet2::statistics::Statistics;
    serialize_statistics(statistics)
}
