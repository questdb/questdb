use num_traits::Bounded;
use parquet2::encoding::delta_bitpacked::encode;
use parquet2::encoding::Encoding;
use parquet2::page::{DataPage, Page};
use parquet2::schema::Repetition;
use parquet2::schema::types::PrimitiveType;
use parquet2::statistics::{ParquetStatistics, PrimitiveStatistics, serialize_statistics};
use parquet2::types::NativeType;

use crate::parquet_write::{Nullable, ParquetError, ParquetResult};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util::{build_plain_page, encode_bool_iter, ExactSizedIter, MaxMin};

fn encode_plain<T, P>(
    slice: &[T],
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
        buffer.reserve(std::mem::size_of::<P>() * slice.len());
        // append all values
        slice.iter().for_each(|x| {
            let parquet_native: P = x.as_();
            buffer.extend_from_slice(parquet_native.to_le_bytes().as_ref())
        });
    }
    buffer
}

fn encode_delta<T, P>(
    slice: &[T],
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
        let iterator = slice.iter().map(|x| {
            let parquet_native: P = x.as_();
            let integer: i64 = parquet_native.as_();
            integer
        });
        encode(iterator, &mut buffer)
    }
    buffer
}

// floats encoding
pub fn float_slice_to_page_plain<T, P>(
    slice: &[T],
    options: WriteOptions,
    type_: PrimitiveType,
) -> ParquetResult<Page>
    where
        P: NativeType + Bounded,
        T: num_traits::AsPrimitive<P> + Nullable + num_traits::Float + Bounded,
{
    let is_nullable = type_.field_info.repetition == Repetition::Optional;
    slice_to_page(
        slice,
        is_nullable,
        options,
        type_,
        Encoding::Plain,
        encode_plain,
    )
        .map(Page::Data)
}

pub fn int_slice_to_page<T, P>(
    slice: &[T],
    options: WriteOptions,
    type_: PrimitiveType,
    encoding: Encoding,
) -> ParquetResult<Page>
    where
        P: NativeType + Bounded,
        T: num_traits::AsPrimitive<P> + Bounded + Nullable,
        P: num_traits::AsPrimitive<i64>,
{
    let is_nullable = type_.field_info.repetition == Repetition::Optional;
    match encoding {
        Encoding::Plain => {
            slice_to_page(slice, is_nullable, options, type_, encoding, encode_plain)
        }
        Encoding::DeltaBinaryPacked => {
            slice_to_page(slice, is_nullable, options, type_, encoding, encode_delta)
        }
        other => Err(ParquetError::OutOfSpec(format!(
            "Encoding integer as {:?}",
            other
        )))?,
    }
        .map(Page::Data)
}

pub fn slice_to_page<T, P, F: Fn(&[T], bool, usize, Vec<u8>) -> Vec<u8>>(
    slice: &[T],
    is_nullable: bool,
    options: WriteOptions,
    type_: PrimitiveType,
    encoding: Encoding,
    encode_fn: F,
) -> ParquetResult<DataPage>
    where
        P: NativeType + Bounded,
        T: num_traits::AsPrimitive<P> + Nullable + Bounded,
{
    let mut buffer = vec![];
    let mut null_count = 0;
    let mut statistics = MaxMin::new();
    if is_nullable {
        let nulls_iterator = slice.iter().map(|v| {
            let value = *v;
            if value.is_null() {
                null_count += 1;
                false
            } else {
                let v: P = value.as_();
                statistics.update(v);
                true
            }
        });
        encode_bool_iter(&mut buffer, nulls_iterator, options.version)?;
    };

    let definition_levels_byte_length = buffer.len();
    let buffer = encode_fn(slice, is_nullable, null_count, buffer);

    let statistics = if options.write_statistics {
        Some(build_statistics(
            Some(null_count as i64),
            statistics,
            type_.clone(),
        ))
    } else {
        None
    };

    build_plain_page(
        buffer,
        slice.len(),
        slice.len(),
        null_count,
        0,
        definition_levels_byte_length,
        statistics,
        type_,
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
