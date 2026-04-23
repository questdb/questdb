use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use parquet2::bloom_filter::hash_native;
use parquet2::encoding::hybrid_rle::bitpacked_encode;
use parquet2::encoding::Encoding;
use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;
use parquet2::schema::Repetition;
use parquet2::statistics::{
    serialize_statistics, BooleanStatistics, FixedLenStatistics, ParquetStatistics, Statistics,
};
use parquet2::types::NativeType;

use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_write::encoders::helpers::{
    page_chunk_views, rows_per_primitive_page, FlatValidity, PageRowWindow, PartitionChunkView,
};
use crate::parquet_write::encoders::numeric::{build_statistics, SimdEncodable, StatsUpdater};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::schema::Column;
use crate::parquet_write::util::{build_plain_page, MaxMin, SimdMaxMin};
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
    let rows_per_page = rows_per_primitive_page(&options, primitive_type.physical_type);
    encode_column_chunk(
        columns,
        first_partition_start,
        last_partition_end,
        rows_per_page,
        bloom_set,
        |window, bloom| {
            simd_segments_to_page::<T>(
                columns,
                first_partition_start,
                last_partition_end,
                window,
                options,
                primitive_type.clone(),
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
    let rows_per_page = rows_per_primitive_page(&options, primitive_type.physical_type);
    encode_column_chunk(
        columns,
        first_partition_start,
        last_partition_end,
        rows_per_page,
        bloom_set,
        |window, bloom| {
            int_notnull_segments_to_page::<T, P>(
                columns,
                first_partition_start,
                last_partition_end,
                window,
                options,
                primitive_type.clone(),
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
    let rows_per_page = rows_per_primitive_page(&options, primitive_type.physical_type);
    encode_column_chunk(
        columns,
        first_partition_start,
        last_partition_end,
        rows_per_page,
        bloom_set,
        |window, bloom| {
            int_nullable_segments_to_page::<T, P, UNSIGNED_STATS>(
                columns,
                first_partition_start,
                last_partition_end,
                window,
                options,
                primitive_type.clone(),
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
    let rows_per_page = rows_per_primitive_page(&options, primitive_type.physical_type);
    encode_column_chunk(
        columns,
        first_partition_start,
        last_partition_end,
        rows_per_page,
        bloom_set,
        |window, bloom| {
            decimal_segments_to_page::<T>(
                columns,
                first_partition_start,
                last_partition_end,
                window,
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
    let rows_per_page = rows_per_primitive_page(&options, primitive_type.physical_type);
    encode_column_chunk(
        columns,
        first_partition_start,
        last_partition_end,
        rows_per_page,
        None,
        |window, _bloom| {
            boolean_segments_to_page(
                columns,
                first_partition_start,
                last_partition_end,
                window,
                options,
                primitive_type.clone(),
            )
        },
    )
}

fn simd_segments_to_page<T: SimdEncodable>(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    window: PageRowWindow,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<Page> {
    assert_eq!(primitive_type.field_info.repetition, Repetition::Optional);

    // SAFETY: Column data originates from JNI/Java memory-mapped buffers.
    let mut views = unsafe {
        page_chunk_views::<T>(columns, first_partition_start, last_partition_end, window)
    };
    let first = views.next().unwrap();

    match views.next() {
        None => {
            // Single view: use SIMD-accelerated path (fused def levels + stats + bloom).
            simd_single_view_page(first, options, primitive_type, bloom_hashes)
        }
        Some(second) => {
            // Multiple views: scalar single-pass fallback.
            simd_multi_view_page(
                first,
                std::iter::once(second).chain(views),
                window,
                options,
                primitive_type,
                bloom_hashes,
            )
        }
    }
}

/// SIMD fast path for single-partition pages.
fn simd_single_view_page<T: SimdEncodable>(
    view: PartitionChunkView<'_, T>,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<Page> {
    let num_rows = view.num_rows();
    let mut buffer = Vec::new();

    // V1 needs a 4-byte length prefix placeholder for def levels.
    let def_levels_start = if matches!(options.version, parquet2::write::Version::V1) {
        buffer.extend_from_slice(&[0; 4]);
        4
    } else {
        0
    };

    let result = T::encode_def_levels(
        &mut buffer,
        view.slice,
        view.adjusted_column_top,
        options.write_statistics,
        bloom_hashes,
    )
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
    let null_count = view.adjusted_column_top + result.null_count;

    let buffer = T::encode_data(view.slice, result.null_count, Encoding::Plain, buffer)?;

    let statistics = if options.write_statistics {
        Some(build_statistics(
            Some(null_count as i64),
            MaxMin { max: result.max, min: result.min },
            primitive_type.clone(),
        ))
    } else {
        None
    };

    build_plain_page(
        buffer,
        num_rows,
        null_count,
        definition_levels_byte_length,
        statistics,
        primitive_type,
        options,
        Encoding::Plain,
        false,
    )
    .map(Page::Data)
}

/// Scalar fallback for multi-partition pages.
fn simd_multi_view_page<'a, T: SimdEncodable>(
    first: PartitionChunkView<'a, T>,
    remaining: impl Iterator<Item = PartitionChunkView<'a, T>>,
    window: PageRowWindow,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<Page> {
    let num_rows = window.row_count;
    let mut validity = FlatValidity::new();
    validity.reset(num_rows);
    let mut statistics = SimdMaxMin::new();

    // Materialize views up front so we can walk them twice without re-running
    // the unsafe page_chunk_views iterator.
    let views: Vec<PartitionChunkView<'a, T>> =
        std::iter::once(first).chain(remaining).collect();

    // Pass 1: build validity bitmap only (no data writes).
    for view in &views {
        for _ in 0..view.adjusted_column_top {
            validity.push_null();
        }
        for &value in view.slice {
            if value.is_null() {
                validity.push_null();
            } else {
                validity.push_present();
            }
        }
    }

    // Encode def levels directly into `buffer` first, then append values.
    // Avoids an O(page-size) Vec::splice(0..0, ..) memmove after data writes.
    let mut buffer = Vec::with_capacity(size_of::<T>() * num_rows + 64);
    let def_levels = validity.encode_def_levels(&mut buffer, options.version)?;
    let null_count = def_levels.null_count;
    let definition_levels_byte_length = def_levels.definition_levels_byte_length;

    // Pass 2: append present values, updating stats/bloom.
    for view in &views {
        for &value in view.slice {
            if !value.is_null() {
                if options.write_statistics {
                    statistics.update(value);
                }
                if let Some(ref mut h) = bloom_hashes {
                    h.insert(hash_native(value));
                }
                buffer.extend_from_slice(value.to_bytes().as_ref());
            }
        }
    }

    let stats = if options.write_statistics {
        Some(build_statistics(
            Some(null_count as i64),
            statistics.to_minmax_stats(null_count != num_rows),
            primitive_type.clone(),
        ))
    } else {
        None
    };

    build_plain_page(
        buffer,
        num_rows,
        null_count,
        definition_levels_byte_length,
        stats,
        primitive_type,
        options,
        Encoding::Plain,
        false,
    )
    .map(Page::Data)
}

fn int_notnull_segments_to_page<T, P>(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    window: PageRowWindow,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<Page>
where
    P: NativeType + num_traits::AsPrimitive<i64>,
    T: Default + num_traits::AsPrimitive<P> + Debug + 'static,
{
    assert_eq!(primitive_type.field_info.repetition, Repetition::Required);

    let num_rows = window.row_count;
    let default_bytes = {
        let pv: P = T::default().as_();
        pv.to_bytes().as_ref().to_vec()
    };
    let mut buffer = Vec::with_capacity(size_of::<P>() * num_rows);
    let mut statistics = MaxMin::new();
    let mut column_top = 0;

    // SAFETY: Column data originates from JNI/Java memory-mapped buffers.
    let views = unsafe {
        page_chunk_views::<T>(columns, first_partition_start, last_partition_end, window)
    };
    for view in views {
        column_top += view.adjusted_column_top;
        for _ in 0..view.adjusted_column_top {
            buffer.extend_from_slice(&default_bytes);
        }
        for value in view.slice {
            let pv: P = value.as_();
            buffer.extend_from_slice(pv.to_bytes().as_ref());
            if options.write_statistics {
                statistics.update(pv);
            }
            if let Some(ref mut h) = bloom_hashes {
                h.insert(hash_native(pv));
            }
        }
    }

    let stats = if options.write_statistics {
        Some(build_statistics(
            Some(column_top as i64),
            statistics,
            primitive_type.clone(),
        ))
    } else {
        None
    };

    build_plain_page(
        buffer,
        num_rows,
        column_top,
        0,
        stats,
        primitive_type,
        options,
        Encoding::Plain,
        true,
    )
    .map(Page::Data)
}

fn int_nullable_segments_to_page<T, P, const UNSIGNED_STATS: bool>(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    window: PageRowWindow,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<Page>
where
    P: NativeType + num_traits::AsPrimitive<i64>,
    T: Nullable + num_traits::AsPrimitive<P> + Debug + 'static,
    MaxMin<P>: StatsUpdater<P, UNSIGNED_STATS>,
{
    assert_eq!(primitive_type.field_info.repetition, Repetition::Optional);

    let num_rows = window.row_count;
    let mut validity = FlatValidity::new();
    validity.reset(num_rows);
    let mut statistics = MaxMin::new();

    // Pass 1: build validity bitmap only (no data writes).
    // SAFETY: Column data originates from JNI/Java memory-mapped buffers.
    let views_pass1 = unsafe {
        page_chunk_views::<T>(columns, first_partition_start, last_partition_end, window)
    };
    for view in views_pass1 {
        for _ in 0..view.adjusted_column_top {
            validity.push_null();
        }
        for &value in view.slice {
            if value.is_null() {
                validity.push_null();
            } else {
                validity.push_present();
            }
        }
    }

    // Encode def levels directly into `buffer` first, then append values.
    // Avoids an O(page-size) Vec::splice(0..0, ..) memmove after data writes.
    let mut buffer = Vec::with_capacity(size_of::<P>() * num_rows + 64);
    let def_levels = validity.encode_def_levels(&mut buffer, options.version)?;
    let null_count = def_levels.null_count;
    let definition_levels_byte_length = def_levels.definition_levels_byte_length;

    // Pass 2: append present values, updating stats/bloom.
    // SAFETY: Column data originates from JNI/Java memory-mapped buffers.
    let views_pass2 = unsafe {
        page_chunk_views::<T>(columns, first_partition_start, last_partition_end, window)
    };
    for view in views_pass2 {
        for &value in view.slice {
            if !value.is_null() {
                let pv: P = value.as_();
                if options.write_statistics {
                    statistics.update_stats(pv);
                }
                if let Some(ref mut h) = bloom_hashes {
                    h.insert(hash_native(pv));
                }
                buffer.extend_from_slice(pv.to_bytes().as_ref());
            }
        }
    }

    let stats = if options.write_statistics {
        Some(build_statistics(
            Some(null_count as i64),
            statistics,
            primitive_type.clone(),
        ))
    } else {
        None
    };

    build_plain_page(
        buffer,
        num_rows,
        null_count,
        definition_levels_byte_length,
        stats,
        primitive_type,
        options,
        Encoding::Plain,
        false,
    )
    .map(Page::Data)
}

fn decimal_segments_to_page<T>(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    window: PageRowWindow,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<Page>
where
    T: Nullable + NativeType + Debug + 'static,
{
    assert_eq!(primitive_type.field_info.repetition, Repetition::Optional);

    let num_rows = window.row_count;
    let mut validity = FlatValidity::new();
    validity.reset(num_rows);
    let mut statistics = MaxMin::new();

    // Pass 1: build validity bitmap only (no data writes).
    // SAFETY: Column data originates from JNI/Java memory-mapped buffers.
    let views_pass1 = unsafe {
        page_chunk_views::<T>(columns, first_partition_start, last_partition_end, window)
    };
    for view in views_pass1 {
        for _ in 0..view.adjusted_column_top {
            validity.push_null();
        }
        for &value in view.slice {
            if value.is_null() {
                validity.push_null();
            } else {
                validity.push_present();
            }
        }
    }

    // Encode def levels directly into `buffer` first, then append values.
    // Avoids an O(page-size) Vec::splice(0..0, ..) memmove after data writes.
    let mut buffer = Vec::with_capacity(size_of::<T>() * num_rows + 64);
    let def_levels = validity.encode_def_levels(&mut buffer, options.version)?;
    let null_count = def_levels.null_count;
    let definition_levels_byte_length = def_levels.definition_levels_byte_length;

    // Pass 2: append present values, updating stats/bloom.
    // SAFETY: Column data originates from JNI/Java memory-mapped buffers.
    let views_pass2 = unsafe {
        page_chunk_views::<T>(columns, first_partition_start, last_partition_end, window)
    };
    for view in views_pass2 {
        for &value in view.slice {
            if !value.is_null() {
                if options.write_statistics {
                    statistics.update(value);
                }
                if let Some(ref mut h) = bloom_hashes {
                    h.insert(hash_native(value));
                }
                buffer.extend_from_slice(value.to_bytes().as_ref());
            }
        }
    }

    let statistics = if options.write_statistics {
        let s = &FixedLenStatistics {
            primitive_type: primitive_type.clone(),
            null_count: Some(null_count as i64),
            distinct_count: None,
            max_value: statistics.max.map(|x| x.to_bytes().as_ref().to_vec()),
            min_value: statistics.min.map(|x| x.to_bytes().as_ref().to_vec()),
        } as &dyn Statistics;
        Some(serialize_statistics(s))
    } else {
        None
    };

    build_plain_page(
        buffer,
        num_rows,
        null_count,
        definition_levels_byte_length,
        statistics,
        primitive_type,
        options,
        Encoding::Plain,
        false,
    )
    .map(Page::Data)
}

fn boolean_segments_to_page(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    window: PageRowWindow,
    options: WriteOptions,
    primitive_type: PrimitiveType,
) -> ParquetResult<Page> {
    let num_rows = window.row_count;
    let mut buffer = vec![];
    let mut stats = MaxMin::new();
    // SAFETY: Column data originates from JNI/Java memory-mapped buffers.
    let views = unsafe {
        page_chunk_views::<u8>(columns, first_partition_start, last_partition_end, window)
    };
    let iter = views
        .flat_map(|view| {
            std::iter::repeat_n(0u8, view.adjusted_column_top).chain(view.slice.iter().copied())
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
