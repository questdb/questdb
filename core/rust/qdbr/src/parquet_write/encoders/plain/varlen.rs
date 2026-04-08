use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use parquet2::bloom_filter::hash_byte;
use parquet2::encoding::{delta_bitpacked, Encoding};
use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;
use parquet2::types;
use parquet2::write::DynIter;
use rapidhash::RapidHashMap;

use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_write::encoders::helpers::rows_per_page;
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::schema::Column;
use crate::parquet_write::util::{
    build_plain_page, encode_dict_rle_pages, encode_primitive_def_levels, transmute_slice,
    BinaryMaxMinStats, ExactSizedIter,
};

use super::encode_per_partition;

/// Encode a String column (UTF-16 source) as Plain pages.
pub fn encode_string(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>> {
    let rpp = rows_per_page(&options, 8);
    encode_per_partition(
        columns,
        first_partition_start,
        last_partition_end,
        rpp,
        bloom_set,
        |column, chunk, bloom| {
            // SAFETY: Data originates from JNI/Java memory-mapped column data, which is
            // page-aligned. The byte content represents valid `i64` offsets.
            let aux: &[i64] = unsafe { transmute_slice(column.secondary_data) };
            let aux_slice = &aux[chunk.lower_bound..chunk.upper_bound];
            string_to_page(
                aux_slice,
                column.primary_data,
                chunk.adjusted_column_top,
                options,
                primitive_type.clone(),
                Encoding::Plain,
                bloom,
            )
        },
    )
}

/// Encode a Binary column as Plain pages.
pub fn encode_binary(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>> {
    let rpp = rows_per_page(&options, 8);
    encode_per_partition(
        columns,
        first_partition_start,
        last_partition_end,
        rpp,
        bloom_set,
        |column, chunk, bloom| {
            // SAFETY: Data originates from JNI/Java memory-mapped column data, which is
            // page-aligned. The byte content represents valid `i64` offsets.
            let aux: &[i64] = unsafe { transmute_slice(column.secondary_data) };
            let aux_slice = &aux[chunk.lower_bound..chunk.upper_bound];
            binary_to_page(
                aux_slice,
                column.primary_data,
                chunk.adjusted_column_top,
                options,
                primitive_type.clone(),
                Encoding::Plain,
                bloom,
            )
        },
    )
}

/// Encode a Varchar column as Plain pages.
pub fn encode_varchar(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>> {
    let rpp = rows_per_page(&options, 8);
    encode_per_partition(
        columns,
        first_partition_start,
        last_partition_end,
        rpp,
        bloom_set,
        |column, chunk, bloom| {
            // SAFETY: Data originates from JNI/Java memory-mapped column data, which is
            // page-aligned. The byte content represents valid `[u8; 16]` aux entries.
            let aux: &[[u8; 16]] = unsafe { transmute_slice(column.secondary_data) };
            let aux_slice = &aux[chunk.lower_bound..chunk.upper_bound];
            varchar_to_page(
                aux_slice,
                column.primary_data,
                chunk.adjusted_column_top,
                options,
                primitive_type.clone(),
                Encoding::Plain,
                bloom,
            )
        },
    )
}

const BINARY_HEADER_SIZE: usize = std::mem::size_of::<i64>();

/// Encode a Binary column as a single Plain or DeltaLengthByteArray data page.
pub fn binary_to_page(
    offsets: &[i64],
    data: &[u8],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    encoding: Encoding,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<Page> {
    // Validate offsets upfront so the deflevels iterator below can safely
    // cast each `i64` offset to `usize`. The iterator can't return an error,
    // and a negative offset would otherwise overflow the usize add downstream.
    for &off in offsets {
        if off < 0 {
            return Err(fmt_err!(
                Layout,
                "invalid offset value in binary aux column: {off}"
            ));
        }
    }

    let num_rows = column_top + offsets.len();
    let mut buffer = vec![];
    let mut null_count = 0;

    let deflevels_iter = (0..num_rows).map(|i| {
        let len = if i < column_top {
            -1
        } else {
            let offset = offsets[i - column_top] as usize;
            let len = types::decode::<i64>(&data[offset..offset + BINARY_HEADER_SIZE]);
            if len < 0 {
                null_count += 1;
            }
            len
        };
        len >= 0
    });
    encode_primitive_def_levels(&mut buffer, deflevels_iter, num_rows, options.version)?;

    let definition_levels_byte_length = buffer.len();

    let mut stats = BinaryMaxMinStats::new(&primitive_type);
    match encoding {
        Encoding::Plain => {
            encode_binary_plain(
                offsets,
                data,
                &mut buffer,
                &mut stats,
                bloom_hashes.as_deref_mut(),
            )?;
        }
        Encoding::DeltaLengthByteArray => {
            encode_binary_delta(
                offsets,
                data,
                null_count,
                &mut buffer,
                &mut stats,
                bloom_hashes,
            )?;
        }
        _ => {
            return Err(fmt_err!(
                Unsupported,
                "unsupported encoding {encoding:?} while writing a binary column"
            ));
        }
    };

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
        encoding,
        false,
    )
    .map(Page::Data)
}

fn encode_binary_plain(
    offsets: &[i64],
    values: &[u8],
    buffer: &mut Vec<u8>,
    stats: &mut BinaryMaxMinStats,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<()> {
    for offset in offsets {
        let offset = usize::try_from(*offset).map_err(|_| {
            fmt_err!(
                Layout,
                "invalid offset value in binary aux column: {offset}"
            )
        })?;
        let len = types::decode::<i64>(&values[offset..offset + BINARY_HEADER_SIZE]);
        if len < 0 {
            continue;
        }
        let value_offset = offset + BINARY_HEADER_SIZE;
        let value = &values[value_offset..value_offset + len as usize];
        let encoded_len = (len as u32).to_le_bytes();
        buffer.extend_from_slice(&encoded_len);
        buffer.extend_from_slice(value);
        stats.update(value);
        if let Some(ref mut h) = bloom_hashes {
            h.insert(hash_byte(value));
        }
    }
    Ok(())
}

fn encode_binary_delta(
    offsets: &[i64],
    values: &[u8],
    null_count: usize,
    buffer: &mut Vec<u8>,
    stats: &mut BinaryMaxMinStats,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<()> {
    let row_count = offsets.len();

    if row_count == 0 {
        delta_bitpacked::encode(std::iter::empty(), buffer);
        return Ok(());
    }

    for &off in offsets {
        if off < 0 {
            return Err(fmt_err!(
                Layout,
                "invalid offset value in binary aux column: {off}"
            ));
        }
    }

    {
        let last_offset = offsets[row_count - 1] as usize;
        let last_size =
            types::decode::<i64>(&values[last_offset..last_offset + BINARY_HEADER_SIZE]);
        let last_size = if last_size > 0 { last_size } else { 0 };
        let capacity = (offsets[row_count - 1] - offsets[0] + last_size) as usize
            - ((row_count - 1) * BINARY_HEADER_SIZE);
        buffer.reserve(capacity);
    }

    let lengths = offsets
        .iter()
        .map(|offset| {
            let offset = *offset as usize;
            types::decode::<i64>(&values[offset..offset + BINARY_HEADER_SIZE])
        })
        .filter(|len| *len >= 0);
    let lengths = ExactSizedIter::new(lengths, row_count - null_count);

    delta_bitpacked::encode(lengths, buffer);

    for offset in offsets {
        let offset = *offset as usize;
        let len = types::decode::<i64>(&values[offset..offset + BINARY_HEADER_SIZE]);
        if len < 0 {
            continue;
        }
        let value_offset = offset + BINARY_HEADER_SIZE;
        let value = &values[value_offset..value_offset + len as usize];
        buffer.extend_from_slice(value);
        stats.update(value);
        if let Some(ref mut h) = bloom_hashes {
            h.insert(hash_byte(value));
        }
    }
    Ok(())
}

/// Single-partition Binary dict encoder. Production code goes through
/// `encoders::rle_dictionary::encode_binary`; this remains for the bench
/// module's micro-benchmarks.
pub fn binary_to_dict_pages(
    offsets: &[i64],
    data: &[u8],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<DynIter<'static, ParquetResult<Page>>> {
    let num_rows = column_top + offsets.len();
    let mut null_count = 0;

    let byte_slices: Vec<Option<&[u8]>> = offsets
        .iter()
        .map(|offset| {
            let offset = usize::try_from(*offset).map_err(|_| {
                fmt_err!(
                    Layout,
                    "invalid offset value in binary aux column: {offset}"
                )
            })?;
            let len = data.get(offset..offset + BINARY_HEADER_SIZE).ok_or_else(|| {
                fmt_err!(
                    Layout,
                    "invalid offset value in binary aux column: {offset}"
                )
            })?;
            let len = types::decode::<i64>(len);
            if len < 0 {
                null_count += 1;
                Ok(None)
            } else {
                let value_offset = offset + BINARY_HEADER_SIZE;
                if value_offset + len as usize > data.len() {
                    return Err(fmt_err!(
                        Layout,
                        "invalid offset and length in binary aux column: offset {offset}, length {len}"
                    ));
                }
                Ok(Some(&data[value_offset..value_offset + len as usize]))
            }
        })
        .collect::<ParquetResult<Vec<_>>>()?;

    let mut dict_map: RapidHashMap<&[u8], u32> = RapidHashMap::default();
    let mut dict_entries: Vec<&[u8]> = Vec::new();
    let mut keys: Vec<u32> = Vec::with_capacity(offsets.len());
    let mut total_keys_bytes = 0usize;

    for s in byte_slices.iter().flatten() {
        let next_id = u32::try_from(dict_entries.len())
            .map_err(|_| fmt_err!(Layout, "dictionary exceeds u32::MAX entries"))?;
        let key = *dict_map.entry(s).or_insert_with(|| {
            total_keys_bytes += 4 + s.len();
            dict_entries.push(s);
            next_id
        });
        keys.push(key);
    }

    let mut dict_buffer = Vec::with_capacity(total_keys_bytes);
    let mut stats = if options.write_statistics {
        Some(BinaryMaxMinStats::new(&primitive_type))
    } else {
        None
    };
    for &entry in &dict_entries {
        dict_buffer.extend_from_slice(&(entry.len() as u32).to_le_bytes());
        dict_buffer.extend_from_slice(entry);
        if let Some(ref mut s) = stats {
            s.update(entry);
        }
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
            byte_slices[i - column_top].is_some()
        }
    });
    encode_primitive_def_levels(&mut data_buffer, def_levels, num_rows, options.version)?;
    let definition_levels_byte_length = data_buffer.len();

    let non_null_len = offsets.len() - null_count;
    let statistics = stats.map(|s| s.into_parquet_stats(total_null_count));

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

const STRING_HEADER_SIZE: usize = std::mem::size_of::<i32>();

/// Encode a String column (UTF-16 source) as a single Plain or
/// DeltaLengthByteArray data page.
pub fn string_to_page(
    offsets: &[i64],
    data: &[u8],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    encoding: Encoding,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<Page> {
    let num_rows = column_top + offsets.len();
    let mut buffer = vec![];
    let mut null_count = 0;

    let utf16_slices: Vec<Option<&[u16]>> = offsets
        .iter()
        .map(|offset| {
            let offset = usize::try_from(*offset).map_err(|_| {
                fmt_err!(
                    Layout,
                    "invalid offset value in string aux column: {offset}"
                )
            })?;
            let data = data.get(offset..).ok_or_else(|| {
                fmt_err!(
                    Layout,
                    "offset value {offset} is out of bounds for string aux column data"
                )
            })?;
            let maybe_utf16 = string_get_utf16(data)?;
            if maybe_utf16.is_none() {
                null_count += 1;
            }
            Ok(maybe_utf16)
        })
        .collect::<ParquetResult<Vec<_>>>()?;

    let deflevels_iter =
        (0..num_rows).map(|i| i >= column_top && utf16_slices[i - column_top].is_some());
    encode_primitive_def_levels(&mut buffer, deflevels_iter, num_rows, options.version)?;

    let definition_levels_byte_length = buffer.len();

    let mut stats = BinaryMaxMinStats::new(&primitive_type);

    match encoding {
        Encoding::Plain => {
            encode_string_plain(
                &utf16_slices,
                &mut buffer,
                &mut stats,
                bloom_hashes.as_deref_mut(),
            )?;
        }
        Encoding::DeltaLengthByteArray => {
            encode_string_delta(
                &utf16_slices,
                null_count,
                &mut buffer,
                &mut stats,
                bloom_hashes,
            )?;
        }
        _ => {
            return Err(fmt_err!(
                Unsupported,
                "unsupported encoding {encoding:?} while writing a string column"
            ));
        }
    };

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
        encoding,
        false,
    )
    .map(Page::Data)
}

/// Single-partition String dict encoder (legacy bench surface).
pub fn string_to_dict_pages(
    offsets: &[i64],
    data: &[u8],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<DynIter<'static, ParquetResult<Page>>> {
    let num_rows = column_top + offsets.len();
    let mut null_count = 0;

    let mut dict_map: RapidHashMap<&[u16], u32> = RapidHashMap::default();
    let mut dict_entries: Vec<&[u16]> = Vec::new();
    let mut keys: Vec<u32> = Vec::with_capacity(offsets.len());
    let mut is_not_null: Vec<bool> = Vec::with_capacity(offsets.len());

    for offset in offsets {
        let offset = usize::try_from(*offset).map_err(|_| {
            fmt_err!(
                Layout,
                "invalid offset value in string aux column: {offset}"
            )
        })?;
        let data = data.get(offset..).ok_or_else(|| {
            fmt_err!(
                Layout,
                "offset value {offset} is out of bounds for string aux column data"
            )
        })?;
        match string_get_utf16(data)? {
            Some(utf16) => {
                let next_id = u32::try_from(dict_entries.len())
                    .map_err(|_| fmt_err!(Layout, "dictionary exceeds u32::MAX entries"))?;
                let key = *dict_map.entry(utf16).or_insert_with(|| {
                    dict_entries.push(utf16);
                    next_id
                });
                keys.push(key);
                is_not_null.push(true);
            }
            None => {
                null_count += 1;
                is_not_null.push(false);
            }
        }
    }

    let mut dict_buffer = Vec::new();
    let mut stats = if options.write_statistics {
        Some(BinaryMaxMinStats::new(&primitive_type))
    } else {
        None
    };
    for utf16 in &dict_entries {
        let utf8 = String::from_utf16(utf16)
            .map_err(|e| fmt_err!(Layout, "invalid UTF-16 in dictionary entry: {e}"))?;
        let utf8_bytes = utf8.as_bytes();
        dict_buffer.extend_from_slice(&(utf8_bytes.len() as u32).to_le_bytes());
        dict_buffer.extend_from_slice(utf8_bytes);
        if let Some(ref mut s) = stats {
            s.update(utf8_bytes);
        }
        if let Some(ref mut h) = bloom_hashes {
            h.insert(hash_byte(utf8_bytes));
        }
    }

    let total_null_count = column_top + null_count;
    let mut data_buffer = Vec::new();

    let def_levels = (0..num_rows).map(|i| i >= column_top && is_not_null[i - column_top]);
    encode_primitive_def_levels(&mut data_buffer, def_levels, num_rows, options.version)?;
    let definition_levels_byte_length = data_buffer.len();

    let non_null_len = offsets.len() - null_count;
    let statistics = stats.map(|s| s.into_parquet_stats(total_null_count));

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

fn encode_string_plain(
    utf16_slices: &[Option<&[u16]>],
    buffer: &mut Vec<u8>,
    stats: &mut BinaryMaxMinStats,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<()> {
    for utf16 in utf16_slices.iter().filter_map(|&option| option) {
        let utf8 = String::from_utf16(utf16)
            .map_err(|e| fmt_err!(Layout, "invalid UTF-16 data in string column: {e}"))?;
        let encoded_len = (utf8.len() as u32).to_le_bytes();
        buffer.extend_from_slice(&encoded_len);
        let value = utf8.as_bytes();
        buffer.extend_from_slice(value);
        stats.update(value);
        if let Some(ref mut h) = bloom_hashes {
            h.insert(hash_byte(value));
        }
    }
    Ok(())
}

fn encode_string_delta(
    utf16_slices: &[Option<&[u16]>],
    null_count: usize,
    buffer: &mut Vec<u8>,
    stats: &mut BinaryMaxMinStats,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<()> {
    let lengths = utf16_slices
        .iter()
        .filter_map(|&option| option)
        .map(|utf16| compute_utf8_length(utf16) as i64);
    let lengths = ExactSizedIter::new(lengths, utf16_slices.len() - null_count);
    delta_bitpacked::encode(lengths, buffer);
    for utf16 in utf16_slices.iter().filter_map(|&option| option) {
        let utf8 = String::from_utf16(utf16)
            .map_err(|e| fmt_err!(Layout, "invalid UTF-16 data in string column: {e}"))?;
        let value = utf8.as_bytes();
        buffer.extend_from_slice(value);
        stats.update(value);
        if let Some(ref mut h) = bloom_hashes {
            h.insert(hash_byte(value));
        }
    }
    Ok(())
}

fn string_get_utf16(entry_tail: &[u8]) -> ParquetResult<Option<&[u16]>> {
    let (header, value_tail) = entry_tail
        .split_at_checked(STRING_HEADER_SIZE)
        .ok_or_else(|| fmt_err!(Layout, "not enough bytes for string header"))?;
    let len_raw = types::decode::<i32>(header);
    if len_raw < 0 {
        return Ok(None);
    }
    // SAFETY: Data originates from JNI/Java memory-mapped column data, which is page-aligned.
    // The byte content represents valid `u16` values.
    let utf16_tail: &[u16] = unsafe { transmute_slice(value_tail) };
    let char_count = len_raw as usize;
    let chars = utf16_tail
        .get(..char_count)
        .ok_or_else(|| fmt_err!(Layout, "not enough bytes for string value"))?;
    Ok(Some(chars))
}

fn compute_utf8_length(utf16: &[u16]) -> usize {
    utf16
        .iter()
        .filter(|&char| !(0xDC00..=0xDFFF).contains(char))
        .fold(0, |len, &char| {
            len + if char <= 0x7F {
                1
            } else if char <= 0x7FF {
                2
            } else if !(0xD800..=0xDBFF).contains(&char) {
                3
            } else {
                4
            }
        })
}

const VARCHAR_HEADER_FLAG_INLINED: u8 = 1 << 0;
const VARCHAR_HEADER_FLAG_NULL: u8 = 1 << 2;
const VARCHAR_HEADER_FLAGS_WIDTH: u32 = 4;

#[repr(C, packed)]
struct VarcharAuxInlined {
    header: u8,
    chars: [u8; 9],
    _offset: [u8; 6],
}

#[repr(C, packed)]
struct VarcharAuxSplit {
    header: u32,
    chars: [u8; 6],
    offset_lo: u16,
    offset_hi: u32,
}

#[inline(always)]
fn varchar_is_null(header: u8) -> bool {
    (header & VARCHAR_HEADER_FLAG_NULL) == VARCHAR_HEADER_FLAG_NULL
}

#[inline(always)]
fn varchar_is_inlined(header: u8) -> bool {
    (header & VARCHAR_HEADER_FLAG_INLINED) == VARCHAR_HEADER_FLAG_INLINED
}

/// Encode a Varchar column as a single Plain or DeltaLengthByteArray data page.
pub fn varchar_to_page(
    aux: &[[u8; 16]],
    data: &[u8],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    encoding: Encoding,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<Page> {
    debug_assert_eq!(std::mem::size_of::<VarcharAuxInlined>(), 16);
    debug_assert_eq!(std::mem::size_of::<VarcharAuxSplit>(), 16);

    let num_rows = column_top + aux.len();
    let mut buffer = vec![];
    let mut null_count = 0usize;

    // SAFETY: `VarcharAuxInlined` is `#[repr(C, packed)]` and exactly 16 bytes.
    // The source `&[[u8; 16]]` has compatible size and alignment 1.
    let aux: &[VarcharAuxInlined] = unsafe { std::mem::transmute(aux) };

    let utf8_slices: Vec<Option<&[u8]>> = aux
        .iter()
        .map(|entry| {
            if varchar_is_null(entry.header) {
                null_count += 1;
                Ok(None)
            } else if varchar_is_inlined(entry.header) {
                let size = (entry.header >> VARCHAR_HEADER_FLAGS_WIDTH) as usize;
                Ok(Some(&entry.chars[..size]))
            } else {
                // SAFETY: Both `VarcharAuxInlined` and `VarcharAuxSplit` are
                // `#[repr(C, packed)]` and 16 bytes. The header flag check
                // determines which interpretation is valid.
                let entry: &VarcharAuxSplit = unsafe { std::mem::transmute(entry) };
                let header = entry.header;
                let size = (header >> VARCHAR_HEADER_FLAGS_WIDTH) as usize;
                let offset = entry.offset_lo as usize | ((entry.offset_hi as usize) << 16);
                if offset + size > data.len() {
                    return Err(fmt_err!(
                        Layout,
                        "data corruption in VARCHAR column: offset {} + size {} exceeds data length {}",
                        offset,
                        size,
                        data.len()
                    ));
                }
                Ok(Some(&data[offset..][..size]))
            }
        })
        .collect::<ParquetResult<Vec<_>>>()?;

    let deflevels_iter =
        (0..num_rows).map(|i| i >= column_top && utf8_slices[i - column_top].is_some());
    encode_primitive_def_levels(&mut buffer, deflevels_iter, num_rows, options.version)?;

    let definition_levels_byte_length = buffer.len();

    let mut stats = BinaryMaxMinStats::new(&primitive_type);

    match encoding {
        Encoding::Plain => {
            encode_varchar_plain(
                &utf8_slices,
                &mut buffer,
                &mut stats,
                bloom_hashes.as_deref_mut(),
            );
        }
        Encoding::DeltaLengthByteArray => {
            encode_varchar_delta(
                &utf8_slices,
                null_count,
                &mut buffer,
                &mut stats,
                bloom_hashes,
            );
        }
        _ => {
            return Err(fmt_err!(
                Unsupported,
                "unsupported encoding {encoding:?} while writing a varchar column"
            ));
        }
    };

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
        encoding,
        false,
    )
    .map(Page::Data)
}

/// Single-partition Varchar dict encoder (legacy bench surface).
pub fn varchar_to_dict_pages(
    aux: &[[u8; 16]],
    data: &[u8],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<DynIter<'static, ParquetResult<Page>>> {
    let num_rows = column_top + aux.len();
    let aux: &[VarcharAuxInlined] = unsafe { std::mem::transmute(aux) };

    let mut null_count = 0usize;
    let utf8_slices: Vec<Option<&[u8]>> = aux
        .iter()
        .map(|entry| {
            if varchar_is_null(entry.header) {
                null_count += 1;
                Ok(None)
            } else if varchar_is_inlined(entry.header) {
                let size = (entry.header >> VARCHAR_HEADER_FLAGS_WIDTH) as usize;
                Ok(Some(&entry.chars[..size]))
            } else {
                let entry: &VarcharAuxSplit = unsafe { std::mem::transmute(entry) };
                let size = (entry.header >> VARCHAR_HEADER_FLAGS_WIDTH) as usize;
                let offset = entry.offset_lo as usize | ((entry.offset_hi as usize) << 16);
                if offset + size > data.len() {
                    return Err(fmt_err!(
                        Layout,
                        "data corruption in VARCHAR column: offset {} + size {} exceeds data length {}",
                        offset,
                        size,
                        data.len()
                    ));
                }
                Ok(Some(&data[offset..][..size]))
            }
        })
        .collect::<ParquetResult<Vec<_>>>()?;

    let mut dict_map: RapidHashMap<&[u8], u32> = RapidHashMap::default();
    let mut dict_entries: Vec<&[u8]> = Vec::new();
    let mut keys = Vec::with_capacity(utf8_slices.len() - null_count);
    let mut total_keys_bytes = 0usize;
    for s in utf8_slices.iter().flatten() {
        let next_id = u32::try_from(dict_entries.len())
            .map_err(|_| fmt_err!(Layout, "dictionary exceeds u32::MAX entries"))?;
        let key = *dict_map.entry(s).or_insert_with(|| {
            dict_entries.push(s);
            total_keys_bytes += 4 + s.len();
            next_id
        });
        keys.push(key);
    }

    let mut dict_buffer = Vec::with_capacity(total_keys_bytes);
    let mut stats = if options.write_statistics {
        Some(BinaryMaxMinStats::new(&primitive_type))
    } else {
        None
    };
    for &entry in &dict_entries {
        dict_buffer.extend_from_slice(&(entry.len() as u32).to_le_bytes());
        dict_buffer.extend_from_slice(entry);
        if let Some(ref mut stats) = stats {
            stats.update(entry);
        }
        if let Some(ref mut h) = bloom_hashes {
            h.insert(hash_byte(entry));
        }
    }

    let mut data_buffer = Vec::with_capacity(num_rows / 4);
    let total_null_count = column_top + null_count;

    let def_levels =
        (0..num_rows).map(|i| i >= column_top && utf8_slices[i - column_top].is_some());
    encode_primitive_def_levels(&mut data_buffer, def_levels, num_rows, options.version)?;
    let definition_levels_byte_length = data_buffer.len();

    let non_null_len = aux.len() - null_count;
    let statistics = stats.map(|s| s.into_parquet_stats(total_null_count));

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

fn encode_varchar_plain(
    utf8_slices: &[Option<&[u8]>],
    buffer: &mut Vec<u8>,
    stats: &mut BinaryMaxMinStats,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) {
    for utf8 in utf8_slices.iter().filter_map(|&option| option) {
        let len = (utf8.len() as u32).to_le_bytes();
        buffer.extend_from_slice(&len);
        buffer.extend_from_slice(utf8);
        stats.update(utf8);
        if let Some(ref mut h) = bloom_hashes {
            h.insert(hash_byte(utf8));
        }
    }
}

fn encode_varchar_delta(
    utf8_slices: &[Option<&[u8]>],
    null_count: usize,
    buffer: &mut Vec<u8>,
    stats: &mut BinaryMaxMinStats,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) {
    let lengths = utf8_slices
        .iter()
        .filter_map(|&option| option)
        .map(|utf8| utf8.len() as i64);
    let lengths = ExactSizedIter::new(lengths, utf8_slices.len() - null_count);
    delta_bitpacked::encode(lengths, buffer);
    for utf8 in utf8_slices.iter().filter_map(|&option| option) {
        buffer.extend_from_slice(utf8);
        stats.update(utf8);
        if let Some(ref mut h) = bloom_hashes {
            h.insert(hash_byte(utf8));
        }
    }
}
