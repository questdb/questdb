use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use parquet2::bloom_filter::hash_byte;
use parquet2::encoding::Encoding;
use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;
use parquet2::write::DynIter;
use rapidhash::RapidHashMap;

use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_write::encoders::helpers::{
    collect_partition_chunk_views, rows_per_primitive_page, slice_partition_chunk_views,
    FlatValidity, PartitionChunkView,
};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::schema::Column;
use crate::parquet_write::util::{
    build_plain_page, encode_dict_rle_pages, encode_primitive_def_levels, transmute_slice,
    BinaryMaxMinStats,
};

use super::encode_column_chunk;

/// Encode a FixedLenByteArray column (UUID, Long128, Long256, Decimal FLBA)
/// as Plain pages. `reverse` swaps endianness for UUID columns.
pub fn encode_fixed_len_bytes<const N: usize>(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    reverse: bool,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>> {
    let rows_per_page = rows_per_primitive_page(&options, primitive_type.physical_type);
    let segments = collect_partition_chunk_views(
        columns,
        first_partition_start,
        last_partition_end,
        |column| {
            // SAFETY: Data originates from JNI/Java memory-mapped column data, which is
            // page-aligned. The byte content represents valid `[u8; N]` values.
            Ok(unsafe { transmute_slice(column.primary_data) })
        },
    )?;
    encode_column_chunk(
        columns,
        first_partition_start,
        last_partition_end,
        rows_per_page,
        bloom_set,
        |window, bloom| {
            let page_segments = slice_partition_chunk_views(&segments, window);
            bytes_segments_to_page::<N>(
                &page_segments,
                reverse,
                options,
                primitive_type.clone(),
                bloom,
            )
        },
    )
}

fn fixed_null_value<const N: usize>() -> [u8; N] {
    let mut null_value = [0u8; N];
    let long_as_bytes = i64::MIN.to_le_bytes();
    for i in 0..N {
        null_value[i] = long_as_bytes[i % long_as_bytes.len()];
    }
    null_value
}

fn encode_fixed_plain_be<const N: usize>(
    data: &[[u8; N]],
    buffer: &mut Vec<u8>,
    null_value: [u8; N],
    stats: &mut BinaryMaxMinStats,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) {
    for x in data.iter().filter(|&&x| x != null_value) {
        let mut reversed = [0u8; N];
        for (i, &b) in x.iter().rev().enumerate() {
            reversed[i] = b;
        }
        buffer.extend_from_slice(&reversed);
        stats.update(&reversed);
        if let Some(ref mut h) = bloom_hashes {
            h.insert(hash_byte(reversed));
        }
    }
}

fn encode_fixed_plain<const N: usize>(
    data: &[[u8; N]],
    buffer: &mut Vec<u8>,
    null_value: [u8; N],
    stats: &mut BinaryMaxMinStats,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) {
    for x in data.iter().filter(|&&x| x != null_value) {
        buffer.extend_from_slice(x);
        stats.update(x);
        if let Some(ref mut h) = bloom_hashes {
            h.insert(hash_byte(x));
        }
    }
}

fn bytes_segments_to_page<const N: usize>(
    segments: &[PartitionChunkView<'_, [u8; N]>],
    reverse: bool,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<Page> {
    let num_rows: usize = segments.iter().map(PartitionChunkView::num_rows).sum();
    let null_value = fixed_null_value::<N>();
    let mut buffer = vec![];
    let mut validity = FlatValidity::new();
    validity.reset(num_rows);
    for segment in segments {
        for _ in 0..segment.adjusted_column_top {
            validity.push_null();
        }
        for &value in segment.slice {
            if value == null_value {
                validity.push_null();
            } else {
                validity.push_present();
            }
        }
    }
    let def_levels = validity.encode_def_levels(&mut buffer, options.version)?;
    let definition_levels_byte_length = def_levels.definition_levels_byte_length;
    let null_count = def_levels.null_count;
    let mut stats = BinaryMaxMinStats::new(&primitive_type);
    if reverse {
        for segment in segments {
            encode_fixed_plain_be(
                segment.slice,
                &mut buffer,
                null_value,
                &mut stats,
                bloom_hashes.as_deref_mut(),
            );
        }
    } else {
        for segment in segments {
            encode_fixed_plain(
                segment.slice,
                &mut buffer,
                null_value,
                &mut stats,
                bloom_hashes.as_deref_mut(),
            );
        }
    }

    build_plain_page(
        buffer,
        num_rows,
        null_count,
        definition_levels_byte_length,
        if options.write_statistics {
            Some(stats.into_parquet_stats(null_count))
        } else {
            None
        },
        primitive_type,
        options,
        Encoding::Plain,
        false,
    )
    .map(Page::Data)
}

/// Encode a slice of `[u8; N]` values as a single FixedLenByteArray Plain
/// data page. `reverse` toggles big-endian byte order (used for UUID).
pub fn bytes_to_page<const N: usize>(
    data: &[[u8; N]],
    reverse: bool,
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<Page> {
    let num_rows = column_top + data.len();
    let null_value = fixed_null_value::<N>();
    let mut buffer = vec![];
    let mut null_count = 0;

    let deflevels_iter = (0..num_rows).map(|i| {
        if i < column_top {
            false
        } else if data[i - column_top] == null_value {
            null_count += 1;
            false
        } else {
            true
        }
    });
    encode_primitive_def_levels(&mut buffer, deflevels_iter, num_rows, options.version)?;

    let definition_levels_byte_length = buffer.len();

    let mut stats = BinaryMaxMinStats::new(&primitive_type);
    if reverse {
        encode_fixed_plain_be(data, &mut buffer, null_value, &mut stats, bloom_hashes);
    } else {
        encode_fixed_plain(data, &mut buffer, null_value, &mut stats, bloom_hashes);
    }

    let null_count = column_top + null_count;
    build_plain_page(
        buffer,
        num_rows,
        null_count,
        definition_levels_byte_length,
        if options.write_statistics {
            Some(stats.into_parquet_stats(null_count))
        } else {
            None
        },
        primitive_type,
        options,
        Encoding::Plain,
        false,
    )
    .map(Page::Data)
}

/// Single-partition fixed-length-bytes dict encoder. Production code goes
/// through `encoders::rle_dictionary::encode_fixed_len_bytes`; this function
/// remains for the bench module's micro-benchmarks.
pub fn bytes_to_dict_pages<const N: usize>(
    data: &[[u8; N]],
    reverse: bool,
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<DynIter<'static, ParquetResult<Page>>> {
    let num_rows = column_top + data.len();
    let null_value = fixed_null_value::<N>();
    let mut null_count = 0;

    let mut dict_map: RapidHashMap<[u8; N], u32> = RapidHashMap::default();
    let mut dict_entries: Vec<[u8; N]> = Vec::new();
    let mut keys: Vec<u32> = Vec::with_capacity(data.len());
    let mut stats = BinaryMaxMinStats::new(&primitive_type);

    for &value in data {
        if value == null_value {
            null_count += 1;
        } else {
            let stored = if reverse {
                let mut r = value;
                r.reverse();
                r
            } else {
                value
            };
            let next_id = u32::try_from(dict_entries.len())
                .map_err(|_| fmt_err!(Layout, "dictionary exceeds u32::MAX entries"))?;
            let key = *dict_map.entry(stored).or_insert_with(|| {
                dict_entries.push(stored);
                next_id
            });
            keys.push(key);
            stats.update(&stored);
        }
    }

    let mut dict_buffer = Vec::with_capacity(dict_entries.len() * N);
    for entry in &dict_entries {
        dict_buffer.extend_from_slice(entry);
        if let Some(ref mut h) = bloom_hashes {
            h.insert(hash_byte(entry));
        }
    }

    let total_null_count = column_top + null_count;
    let mut data_buffer = Vec::new();

    let def_levels = (0..num_rows).map(|i| {
        if i < column_top {
            false
        } else {
            data[i - column_top] != null_value
        }
    });
    encode_primitive_def_levels(&mut data_buffer, def_levels, num_rows, options.version)?;
    let definition_levels_byte_length = data_buffer.len();

    let non_null_len = data.len() - null_count;
    let statistics = if options.write_statistics {
        Some(stats.into_parquet_stats(total_null_count))
    } else {
        None
    };

    encode_dict_rle_pages(
        dict_buffer,
        dict_entries.len(),
        keys,
        non_null_len,
        data_buffer,
        definition_levels_byte_length,
        num_rows,
        total_null_count,
        statistics,
        primitive_type,
        options,
        false,
    )
}
