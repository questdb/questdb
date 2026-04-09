use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use parquet2::encoding::hybrid_rle::bitpacked_encode;
use parquet2::encoding::Encoding;
use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;
use parquet2::statistics::{
    serialize_statistics, BooleanStatistics, ParquetStatistics, Statistics,
};
use parquet2::types::NativeType;

use crate::parquet::error::ParquetResult;
use crate::parquet_write::encoders::helpers::{collect_typed_chunk_segments, TypedChunkSegment};
use crate::parquet_write::encoders::numeric::{self, SimdEncodable, StatsUpdater};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::schema::Column;
use crate::parquet_write::util::{build_plain_page, transmute_slice, MaxMin};
use crate::parquet_write::Nullable;

use super::encode_column_chunk;

/// Encode a SIMD-encodable primitive type (Int, Long, Float, Double, Date,
/// Timestamp) as Plain pages.
pub fn encode_simd<T>(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>>
where
    T: SimdEncodable,
{
    let segments = collect_typed_chunk_segments(
        columns,
        first_partition_start,
        last_partition_end,
        |column| {
            // SAFETY: Data originates from JNI/Java memory-mapped column data, which is
            // page-aligned. The byte content represents valid `T` values.
            Ok(unsafe { transmute_slice(column.primary_data) })
        },
    )?;
    encode_column_chunk(
        columns,
        first_partition_start,
        last_partition_end,
        bloom_set,
        |bloom| {
            numeric::slice_segments_to_page_simd::<T>(
                &segments,
                options,
                primitive_type.clone(),
                Encoding::Plain,
                bloom,
            )
        },
    )
}

/// Encode a notnull integer (Byte, Short, Char) as Plain pages.
pub fn encode_int_notnull<T, P>(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>>
where
    P: NativeType + num_traits::AsPrimitive<i64>,
    T: Default + num_traits::AsPrimitive<P> + Debug + Copy,
{
    let segments = collect_typed_chunk_segments(
        columns,
        first_partition_start,
        last_partition_end,
        |column| {
            // SAFETY: Data originates from JNI/Java memory-mapped column data, which is
            // page-aligned. The byte content represents valid `T` values.
            Ok(unsafe { transmute_slice(column.primary_data) })
        },
    )?;
    encode_column_chunk(
        columns,
        first_partition_start,
        last_partition_end,
        bloom_set,
        |bloom| {
            numeric::int_segments_to_page_notnull::<T, P>(
                &segments,
                options,
                primitive_type.clone(),
                Encoding::Plain,
                bloom,
            )
        },
    )
}

/// Encode a nullable integer (IPv4, GeoByte/Short/Int/Long) as Plain pages.
pub fn encode_int_nullable<T, P, const UNSIGNED_STATS: bool>(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>>
where
    P: NativeType + num_traits::AsPrimitive<i64>,
    T: Nullable + num_traits::AsPrimitive<P> + Debug + Copy,
    MaxMin<P>: StatsUpdater<P, UNSIGNED_STATS>,
{
    let segments = collect_typed_chunk_segments(
        columns,
        first_partition_start,
        last_partition_end,
        |column| {
            // SAFETY: Data originates from JNI/Java memory-mapped column data, which is
            // page-aligned. The byte content represents valid `T` values.
            Ok(unsafe { transmute_slice(column.primary_data) })
        },
    )?;
    encode_column_chunk(
        columns,
        first_partition_start,
        last_partition_end,
        bloom_set,
        |bloom| {
            numeric::int_segments_to_page_nullable::<T, P, UNSIGNED_STATS>(
                &segments,
                options,
                primitive_type.clone(),
                Encoding::Plain,
                bloom,
            )
        },
    )
}

/// Encode a Decimal type (FixedLenByteArray-backed) as Plain pages.
pub fn encode_decimal<T>(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>>
where
    T: Nullable + NativeType + Debug + Copy,
{
    let segments = collect_typed_chunk_segments(
        columns,
        first_partition_start,
        last_partition_end,
        |column| {
            // SAFETY: Data originates from JNI/Java memory-mapped column data, which is
            // page-aligned. The byte content represents valid `T` values.
            Ok(unsafe { transmute_slice(column.primary_data) })
        },
    )?;
    encode_column_chunk(
        columns,
        first_partition_start,
        last_partition_end,
        bloom_set,
        |bloom| {
            numeric::decimal_segments_to_page_plain::<T>(
                &segments,
                options,
                primitive_type.clone(),
                bloom,
            )
        },
    )
}

/// Encode boolean Plain pages.
pub fn encode_boolean(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    _bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>> {
    let segments = collect_typed_chunk_segments(
        columns,
        first_partition_start,
        last_partition_end,
        |column| Ok(column.primary_data),
    )?;
    encode_column_chunk(
        columns,
        first_partition_start,
        last_partition_end,
        None,
        |_bloom| boolean_segments_to_page(&segments, options, primitive_type.clone()),
    )
}

fn boolean_segments_to_page(
    segments: &[TypedChunkSegment<'_, u8>],
    options: WriteOptions,
    primitive_type: PrimitiveType,
) -> ParquetResult<Page> {
    let num_rows: usize = segments.iter().map(TypedChunkSegment::num_rows).sum();
    let mut buffer = vec![];
    let mut stats = MaxMin::new();
    let iter = segments
        .iter()
        .flat_map(|segment| {
            std::iter::repeat_n(0u8, segment.adjusted_column_top)
                .chain(segment.slice.iter().copied())
        })
        .map(|value| {
            stats.update(value as i32);
            value != 0
        });
    bitpacked_encode(&mut buffer, iter, num_rows)?;

    let statistics = if options.write_statistics {
        Some(boolean_statistics(stats))
    } else {
        None
    };

    build_plain_page(
        buffer,
        num_rows,
        0,
        0,
        statistics,
        primitive_type,
        options,
        Encoding::Plain,
        true,
    )
    .map(Page::Data)
}

/// Encode a slice of booleans as a single Parquet Plain data page.
/// Column-top rows are encoded as `false` (matching the legacy semantics).
pub fn boolean_to_page(
    slice: &[u8],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
) -> ParquetResult<Page> {
    let num_rows = column_top + slice.len();
    let mut buffer = vec![];
    let mut stats = MaxMin::new();

    let iter = (0..num_rows).map(|i| {
        let x = if i < column_top {
            0
        } else {
            slice[i - column_top]
        };
        stats.update(x as i32);
        x != 0
    });
    bitpacked_encode(&mut buffer, iter, num_rows)?;

    let statistics = if options.write_statistics {
        Some(boolean_statistics(stats))
    } else {
        None
    };

    build_plain_page(
        buffer,
        num_rows,
        0,
        0,
        statistics,
        primitive_type,
        options,
        Encoding::Plain,
        true,
    )
    .map(Page::Data)
}

fn boolean_statistics(bool_stats: MaxMin<i32>) -> ParquetStatistics {
    let statistics = &BooleanStatistics {
        null_count: Some(0),
        distinct_count: None,
        max_value: bool_stats.max.map(|x| x != 0),
        min_value: bool_stats.min.map(|x| x != 0),
    } as &dyn Statistics;
    serialize_statistics(statistics)
}
