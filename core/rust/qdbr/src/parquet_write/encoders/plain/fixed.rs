use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crate::parquet::error::ParquetResult;
use crate::parquet_write::encoders::helpers::{
    page_chunk_views, rows_per_primitive_page, FlatValidity, PageRowWindow,
};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::schema::Column;
use crate::parquet_write::util::{
    build_plain_page, encode_primitive_def_levels, BinaryMaxMinStats,
};
use parquet2::bloom_filter::hash_byte;
use parquet2::encoding::Encoding;
use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;

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
    encode_column_chunk(
        columns,
        first_partition_start,
        last_partition_end,
        rows_per_page,
        bloom_set,
        |window, bloom| {
            bytes_segments_to_page::<N>(
                columns,
                first_partition_start,
                last_partition_end,
                window,
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

#[allow(clippy::too_many_arguments)]
fn bytes_segments_to_page<const N: usize>(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    window: PageRowWindow,
    reverse: bool,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<Page> {
    let null_value = fixed_null_value::<N>();
    let num_rows = window.row_count;
    let mut validity = FlatValidity::new();
    validity.reset(num_rows);
    let mut buffer = Vec::with_capacity(N * num_rows);
    let mut stats = BinaryMaxMinStats::new(&primitive_type);

    // SAFETY: Column data originates from JNI/Java memory-mapped buffers which are page-aligned.
    let views = unsafe {
        page_chunk_views::<[u8; N]>(columns, first_partition_start, last_partition_end, window)
    };

    // Single pass: build validity bitmap, stats, bloom, and encoded values together.
    for view in views {
        for _ in 0..view.adjusted_column_top {
            validity.push_null();
        }
        for &value in view.slice {
            if value == null_value {
                validity.push_null();
            } else {
                validity.push_present();
                if reverse {
                    let mut reversed = [0u8; N];
                    for (i, &b) in value.iter().rev().enumerate() {
                        reversed[i] = b;
                    }
                    buffer.extend_from_slice(&reversed);
                    stats.update(&reversed);
                    if let Some(ref mut h) = bloom_hashes {
                        h.insert(hash_byte(reversed));
                    }
                } else {
                    buffer.extend_from_slice(&value);
                    stats.update(&value);
                    if let Some(ref mut h) = bloom_hashes {
                        h.insert(hash_byte(value));
                    }
                }
            }
        }
    }

    // Encode def levels into the reserved prefix, then close any gap.
    let mut def_buf = Vec::new();
    let def_levels = validity.encode_def_levels(&mut def_buf, options.version)?;
    let null_count = def_levels.null_count;
    let definition_levels_byte_length = def_levels.definition_levels_byte_length;
    buffer.splice(0..0, def_buf);

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
