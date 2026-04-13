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
use crate::parquet_write::encoders::helpers::{
    partition_chunk_slice, rows_per_primitive_page, FlatValidity, PageRowWindow,
};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::schema::Column;
use crate::parquet_write::util::{
    build_plain_page, encode_dict_rle_pages, encode_primitive_def_levels, transmute_slice,
    BinaryMaxMinStats, ExactSizedIter,
};

use super::encode_column_chunk;

#[derive(Clone, Copy)]
struct BinaryChunkSegment<'a> {
    adjusted_column_top: usize,
    offsets: &'a [i64],
    data: &'a [u8],
}

impl BinaryChunkSegment<'_> {
    #[inline]
    fn num_rows(&self) -> usize {
        self.adjusted_column_top + self.offsets.len()
    }
}

#[derive(Clone, Copy)]
struct StringChunkSegment<'a> {
    adjusted_column_top: usize,
    offsets: &'a [i64],
    data: &'a [u8],
}

impl StringChunkSegment<'_> {
    #[inline]
    fn num_rows(&self) -> usize {
        self.adjusted_column_top + self.offsets.len()
    }
}

#[derive(Clone, Copy)]
struct VarcharChunkSegment<'a> {
    adjusted_column_top: usize,
    aux: &'a [[u8; 16]],
    data: &'a [u8],
}

impl VarcharChunkSegment<'_> {
    #[inline]
    fn num_rows(&self) -> usize {
        self.adjusted_column_top + self.aux.len()
    }
}

fn slice_binary_segments<'a>(
    segments: &[BinaryChunkSegment<'a>],
    window: PageRowWindow,
) -> Vec<BinaryChunkSegment<'a>> {
    let mut remaining_offset = window.row_offset;
    let mut remaining_rows = window.row_count;
    let mut page_segments = Vec::with_capacity(segments.len());

    for segment in segments {
        let segment_rows = segment.num_rows();
        if remaining_offset >= segment_rows {
            remaining_offset -= segment_rows;
            continue;
        }
        if remaining_rows == 0 {
            break;
        }

        let rows_in_segment = std::cmp::min(segment_rows - remaining_offset, remaining_rows);
        let skip_data_rows = remaining_offset.saturating_sub(segment.adjusted_column_top);
        let available_top_rows = segment.adjusted_column_top.saturating_sub(remaining_offset);
        let top_rows = std::cmp::min(available_top_rows, rows_in_segment);
        let data_rows = rows_in_segment - top_rows;

        page_segments.push(BinaryChunkSegment {
            adjusted_column_top: top_rows,
            offsets: &segment.offsets[skip_data_rows..skip_data_rows + data_rows],
            data: segment.data,
        });

        remaining_rows -= rows_in_segment;
        remaining_offset = 0;
    }

    page_segments
}

fn slice_string_segments<'a>(
    segments: &[StringChunkSegment<'a>],
    window: PageRowWindow,
) -> Vec<StringChunkSegment<'a>> {
    let mut remaining_offset = window.row_offset;
    let mut remaining_rows = window.row_count;
    let mut page_segments = Vec::with_capacity(segments.len());

    for segment in segments {
        let segment_rows = segment.num_rows();
        if remaining_offset >= segment_rows {
            remaining_offset -= segment_rows;
            continue;
        }
        if remaining_rows == 0 {
            break;
        }

        let rows_in_segment = std::cmp::min(segment_rows - remaining_offset, remaining_rows);
        let skip_data_rows = remaining_offset.saturating_sub(segment.adjusted_column_top);
        let available_top_rows = segment.adjusted_column_top.saturating_sub(remaining_offset);
        let top_rows = std::cmp::min(available_top_rows, rows_in_segment);
        let data_rows = rows_in_segment - top_rows;

        page_segments.push(StringChunkSegment {
            adjusted_column_top: top_rows,
            offsets: &segment.offsets[skip_data_rows..skip_data_rows + data_rows],
            data: segment.data,
        });

        remaining_rows -= rows_in_segment;
        remaining_offset = 0;
    }

    page_segments
}

fn slice_varchar_segments<'a>(
    segments: &[VarcharChunkSegment<'a>],
    window: PageRowWindow,
) -> Vec<VarcharChunkSegment<'a>> {
    let mut remaining_offset = window.row_offset;
    let mut remaining_rows = window.row_count;
    let mut page_segments = Vec::with_capacity(segments.len());

    for segment in segments {
        let segment_rows = segment.num_rows();
        if remaining_offset >= segment_rows {
            remaining_offset -= segment_rows;
            continue;
        }
        if remaining_rows == 0 {
            break;
        }

        let rows_in_segment = std::cmp::min(segment_rows - remaining_offset, remaining_rows);
        let skip_data_rows = remaining_offset.saturating_sub(segment.adjusted_column_top);
        let available_top_rows = segment.adjusted_column_top.saturating_sub(remaining_offset);
        let top_rows = std::cmp::min(available_top_rows, rows_in_segment);
        let data_rows = rows_in_segment - top_rows;

        page_segments.push(VarcharChunkSegment {
            adjusted_column_top: top_rows,
            aux: &segment.aux[skip_data_rows..skip_data_rows + data_rows],
            data: segment.data,
        });

        remaining_rows -= rows_in_segment;
        remaining_offset = 0;
    }

    page_segments
}

fn collect_binary_segments<'a>(
    columns: &'a [Column],
    first_partition_start: usize,
    last_partition_end: usize,
) -> Vec<BinaryChunkSegment<'a>> {
    let num_partitions = columns.len();
    columns
        .iter()
        .enumerate()
        .map(|(part_idx, column)| {
            let chunk = partition_chunk_slice(
                part_idx,
                num_partitions,
                column,
                first_partition_start,
                last_partition_end,
            );
            // SAFETY: Data originates from JNI/Java memory-mapped column data, which is
            // page-aligned. The byte content represents valid `i64` offsets.
            let aux: &[i64] = unsafe { transmute_slice(column.secondary_data) };
            BinaryChunkSegment {
                adjusted_column_top: chunk.adjusted_column_top,
                offsets: &aux[chunk.lower_bound..chunk.upper_bound],
                data: column.primary_data,
            }
        })
        .collect()
}

fn collect_string_segments<'a>(
    columns: &'a [Column],
    first_partition_start: usize,
    last_partition_end: usize,
) -> Vec<StringChunkSegment<'a>> {
    let num_partitions = columns.len();
    columns
        .iter()
        .enumerate()
        .map(|(part_idx, column)| {
            let chunk = partition_chunk_slice(
                part_idx,
                num_partitions,
                column,
                first_partition_start,
                last_partition_end,
            );
            // SAFETY: Data originates from JNI/Java memory-mapped column data, which is
            // page-aligned. The byte content represents valid `i64` offsets.
            let aux: &[i64] = unsafe { transmute_slice(column.secondary_data) };
            StringChunkSegment {
                adjusted_column_top: chunk.adjusted_column_top,
                offsets: &aux[chunk.lower_bound..chunk.upper_bound],
                data: column.primary_data,
            }
        })
        .collect()
}

fn collect_varchar_segments<'a>(
    columns: &'a [Column],
    first_partition_start: usize,
    last_partition_end: usize,
) -> Vec<VarcharChunkSegment<'a>> {
    let num_partitions = columns.len();
    columns
        .iter()
        .enumerate()
        .map(|(part_idx, column)| {
            let chunk = partition_chunk_slice(
                part_idx,
                num_partitions,
                column,
                first_partition_start,
                last_partition_end,
            );
            // SAFETY: Data originates from JNI/Java memory-mapped column data, which is
            // page-aligned. The byte content represents valid `[u8; 16]` aux entries.
            let aux: &[[u8; 16]] = unsafe { transmute_slice(column.secondary_data) };
            VarcharChunkSegment {
                adjusted_column_top: chunk.adjusted_column_top,
                aux: &aux[chunk.lower_bound..chunk.upper_bound],
                data: column.primary_data,
            }
        })
        .collect()
}

/// Encode a String column (UTF-16 source) as Plain pages.
pub fn encode_string(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>> {
    let rows_per_page = rows_per_primitive_page(&options, primitive_type.physical_type);
    let segments = collect_string_segments(columns, first_partition_start, last_partition_end);
    encode_column_chunk(
        columns,
        first_partition_start,
        last_partition_end,
        rows_per_page,
        bloom_set,
        |window, bloom| {
            let page_segments = slice_string_segments(&segments, window);
            string_segments_to_page(
                &page_segments,
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
    let rows_per_page = rows_per_primitive_page(&options, primitive_type.physical_type);
    let segments = collect_binary_segments(columns, first_partition_start, last_partition_end);
    encode_column_chunk(
        columns,
        first_partition_start,
        last_partition_end,
        rows_per_page,
        bloom_set,
        |window, bloom| {
            let page_segments = slice_binary_segments(&segments, window);
            binary_segments_to_page(
                &page_segments,
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
    let rows_per_page = rows_per_primitive_page(&options, primitive_type.physical_type);
    let segments = collect_varchar_segments(columns, first_partition_start, last_partition_end);
    encode_column_chunk(
        columns,
        first_partition_start,
        last_partition_end,
        rows_per_page,
        bloom_set,
        |window, bloom| {
            let page_segments = slice_varchar_segments(&segments, window);
            varchar_segments_to_page(
                &page_segments,
                options,
                primitive_type.clone(),
                Encoding::Plain,
                bloom,
            )
        },
    )
}

fn binary_segments_to_page(
    segments: &[BinaryChunkSegment<'_>],
    options: WriteOptions,
    primitive_type: PrimitiveType,
    encoding: Encoding,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<Page> {
    let num_rows: usize = segments.iter().map(BinaryChunkSegment::num_rows).sum();
    let mut buffer = vec![];
    let mut validity = FlatValidity::new();
    validity.reset(num_rows);
    for segment in segments {
        for _ in 0..segment.adjusted_column_top {
            validity.push_null();
        }
        visit_binary_entries(segment.offsets, segment.data, |entry| {
            if entry.is_some() {
                validity.push_present();
            } else {
                validity.push_null();
            }
            Ok(())
        })?;
    }
    let def_levels = validity.encode_def_levels(&mut buffer, options.version)?;
    let definition_levels_byte_length = def_levels.definition_levels_byte_length;
    let null_count = def_levels.null_count;
    let mut stats = BinaryMaxMinStats::new(&primitive_type);
    match encoding {
        Encoding::Plain => {
            for segment in segments {
                for &offset in segment.offsets {
                    if let Some(value) = binary_get_slice_validated(segment.data, offset) {
                        let len = value.len();
                        buffer.extend_from_slice(&(len as u32).to_le_bytes());
                        buffer.extend_from_slice(value);
                        stats.update(value);
                        if let Some(ref mut h) = bloom_hashes {
                            h.insert(hash_byte(value));
                        }
                    }
                }
            }
        }
        Encoding::DeltaLengthByteArray => {
            let non_null_count = num_rows - null_count;
            let lengths = segments.iter().flat_map(|segment| {
                segment
                    .offsets
                    .iter()
                    .filter_map(|&offset| binary_get_slice_validated(segment.data, offset))
                    .map(|value| value.len() as i64)
            });
            let lengths = ExactSizedIter::new(lengths, non_null_count);
            delta_bitpacked::encode(lengths, &mut buffer);

            for segment in segments {
                for &offset in segment.offsets {
                    if let Some(value) = binary_get_slice_validated(segment.data, offset) {
                        buffer.extend_from_slice(value);
                        stats.update(value);
                        if let Some(ref mut h) = bloom_hashes {
                            h.insert(hash_byte(value));
                        }
                    }
                }
            }
        }
        _ => {
            return Err(fmt_err!(
                Unsupported,
                "unsupported encoding {encoding:?} while writing a binary column"
            ))
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
        encoding,
        false,
    )
    .map(Page::Data)
}

fn string_segments_to_page(
    segments: &[StringChunkSegment<'_>],
    options: WriteOptions,
    primitive_type: PrimitiveType,
    encoding: Encoding,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<Page> {
    let num_rows: usize = segments.iter().map(StringChunkSegment::num_rows).sum();
    let mut buffer = vec![];
    let mut validity = FlatValidity::new();
    validity.reset(num_rows);
    for segment in segments {
        for _ in 0..segment.adjusted_column_top {
            validity.push_null();
        }
        visit_string_entries(segment.offsets, segment.data, |entry| {
            if entry.is_some() {
                validity.push_present();
            } else {
                validity.push_null();
            }
            Ok(())
        })?;
    }
    let def_levels = validity.encode_def_levels(&mut buffer, options.version)?;
    let definition_levels_byte_length = def_levels.definition_levels_byte_length;
    let null_count = def_levels.null_count;
    let mut stats = BinaryMaxMinStats::new(&primitive_type);
    match encoding {
        Encoding::Plain => {
            for segment in segments {
                for &offset in segment.offsets {
                    if let Some(utf16) = string_get_utf16_validated(segment.data, offset) {
                        let len_offset = buffer.len();
                        buffer.extend_from_slice(&[0; 4]);
                        let utf8_start = buffer.len();
                        let utf8_len = append_utf8_from_utf16(&mut buffer, utf16)?;
                        buffer[len_offset..utf8_start]
                            .copy_from_slice(&(utf8_len as u32).to_le_bytes());
                        let value = &buffer[utf8_start..utf8_start + utf8_len];
                        stats.update(value);
                        if let Some(ref mut h) = bloom_hashes {
                            h.insert(hash_byte(value));
                        }
                    }
                }
            }
        }
        Encoding::DeltaLengthByteArray => {
            let non_null_count = num_rows - null_count;
            let lengths = segments.iter().flat_map(|segment| {
                segment
                    .offsets
                    .iter()
                    .filter_map(|&offset| string_get_utf16_validated(segment.data, offset))
                    .map(|utf16| compute_utf8_length(utf16) as i64)
            });
            let lengths = ExactSizedIter::new(lengths, non_null_count);
            delta_bitpacked::encode(lengths, &mut buffer);

            for segment in segments {
                for &offset in segment.offsets {
                    if let Some(utf16) = string_get_utf16_validated(segment.data, offset) {
                        let utf8_start = buffer.len();
                        let utf8_len = append_utf8_from_utf16(&mut buffer, utf16)?;
                        let value = &buffer[utf8_start..utf8_start + utf8_len];
                        stats.update(value);
                        if let Some(ref mut h) = bloom_hashes {
                            h.insert(hash_byte(value));
                        }
                    }
                }
            }
        }
        _ => {
            return Err(fmt_err!(
                Unsupported,
                "unsupported encoding {encoding:?} while writing a string column"
            ))
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
        encoding,
        false,
    )
    .map(Page::Data)
}

fn varchar_segments_to_page(
    segments: &[VarcharChunkSegment<'_>],
    options: WriteOptions,
    primitive_type: PrimitiveType,
    encoding: Encoding,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<Page> {
    let num_rows: usize = segments.iter().map(VarcharChunkSegment::num_rows).sum();
    let mut buffer = vec![];
    let mut validity = FlatValidity::new();
    validity.reset(num_rows);
    for segment in segments {
        for _ in 0..segment.adjusted_column_top {
            validity.push_null();
        }
        visit_varchar_entries(segment.aux, segment.data, |entry| {
            if entry.is_some() {
                validity.push_present();
            } else {
                validity.push_null();
            }
            Ok(())
        })?;
    }
    let def_levels = validity.encode_def_levels(&mut buffer, options.version)?;
    let definition_levels_byte_length = def_levels.definition_levels_byte_length;
    let null_count = def_levels.null_count;
    let mut stats = BinaryMaxMinStats::new(&primitive_type);
    match encoding {
        Encoding::Plain => {
            for segment in segments {
                for entry in segment.aux {
                    if let Some(value) = varchar_get_slice_validated(entry, segment.data) {
                        let len = value.len();
                        buffer.extend_from_slice(&(len as u32).to_le_bytes());
                        buffer.extend_from_slice(value);
                        stats.update(value);
                        if let Some(ref mut h) = bloom_hashes {
                            h.insert(hash_byte(value));
                        }
                    }
                }
            }
        }
        Encoding::DeltaLengthByteArray => {
            let non_null_count = num_rows - null_count;
            let lengths = segments.iter().flat_map(|segment| {
                segment
                    .aux
                    .iter()
                    .filter_map(|entry| varchar_get_slice_validated(entry, segment.data))
                    .map(|value| value.len() as i64)
            });
            let lengths = ExactSizedIter::new(lengths, non_null_count);
            delta_bitpacked::encode(lengths, &mut buffer);

            for segment in segments {
                for entry in segment.aux {
                    if let Some(value) = varchar_get_slice_validated(entry, segment.data) {
                        buffer.extend_from_slice(value);
                        stats.update(value);
                        if let Some(ref mut h) = bloom_hashes {
                            h.insert(hash_byte(value));
                        }
                    }
                }
            }
        }
        _ => {
            return Err(fmt_err!(
                Unsupported,
                "unsupported encoding {encoding:?} while writing a varchar column"
            ))
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
        encoding,
        false,
    )
    .map(Page::Data)
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
    bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<Page> {
    let mut byte_slices = Vec::with_capacity(column_top + offsets.len());
    byte_slices.resize(column_top, None);
    extend_binary_slices(&mut byte_slices, offsets, data)?;
    binary_slices_to_page(
        &byte_slices,
        options,
        primitive_type,
        encoding,
        bloom_hashes,
    )
}

pub fn binary_slices_to_page(
    byte_slices: &[Option<&[u8]>],
    options: WriteOptions,
    primitive_type: PrimitiveType,
    encoding: Encoding,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<Page> {
    let num_rows = byte_slices.len();
    let null_count = byte_slices.iter().filter(|entry| entry.is_none()).count();
    let mut buffer = vec![];

    let deflevels_iter = byte_slices.iter().map(|entry| entry.is_some());
    encode_primitive_def_levels(&mut buffer, deflevels_iter, num_rows, options.version)?;
    let definition_levels_byte_length = buffer.len();

    let mut stats = BinaryMaxMinStats::new(&primitive_type);
    match encoding {
        Encoding::Plain => {
            encode_binary_plain(
                byte_slices,
                &mut buffer,
                &mut stats,
                bloom_hashes.as_deref_mut(),
            );
        }
        Encoding::DeltaLengthByteArray => {
            encode_binary_delta(
                byte_slices,
                null_count,
                &mut buffer,
                &mut stats,
                bloom_hashes,
            );
        }
        _ => {
            return Err(fmt_err!(
                Unsupported,
                "unsupported encoding {encoding:?} while writing a binary column"
            ));
        }
    };

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

fn binary_get_slice(values: &[u8], offset: i64) -> ParquetResult<Option<&[u8]>> {
    let offset = usize::try_from(offset).map_err(|_| {
        fmt_err!(
            Layout,
            "invalid offset value in binary aux column: {offset}"
        )
    })?;
    let len = values
        .get(offset..offset + BINARY_HEADER_SIZE)
        .ok_or_else(|| {
            fmt_err!(
                Layout,
                "invalid offset value in binary aux column: {offset}"
            )
        })?;
    let len = types::decode::<i64>(len);
    if len < 0 {
        return Ok(None);
    }
    let value_offset = offset + BINARY_HEADER_SIZE;
    if value_offset + len as usize > values.len() {
        return Err(fmt_err!(
            Layout,
            "invalid offset and length in binary aux column: offset {offset}, length {len}"
        ));
    }
    Ok(Some(&values[value_offset..value_offset + len as usize]))
}

fn binary_get_slice_validated(values: &[u8], offset: i64) -> Option<&[u8]> {
    binary_get_slice(values, offset).expect("binary segment was validated before encoding")
}

fn visit_binary_entries<'a, F>(offsets: &[i64], values: &'a [u8], mut visit: F) -> ParquetResult<()>
where
    F: FnMut(Option<&'a [u8]>) -> ParquetResult<()>,
{
    for &offset in offsets {
        visit(binary_get_slice(values, offset)?)?;
    }
    Ok(())
}

pub(crate) fn extend_binary_slices<'a>(
    byte_slices: &mut Vec<Option<&'a [u8]>>,
    offsets: &[i64],
    values: &'a [u8],
) -> ParquetResult<()> {
    visit_binary_entries(offsets, values, |entry| {
        byte_slices.push(entry);
        Ok(())
    })?;
    Ok(())
}

fn encode_binary_plain(
    byte_slices: &[Option<&[u8]>],
    buffer: &mut Vec<u8>,
    stats: &mut BinaryMaxMinStats,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) {
    for value in byte_slices.iter().filter_map(|&entry| entry) {
        let len = value.len();
        let encoded_len = (len as u32).to_le_bytes();
        buffer.extend_from_slice(&encoded_len);
        buffer.extend_from_slice(value);
        stats.update(value);
        if let Some(ref mut h) = bloom_hashes {
            h.insert(hash_byte(value));
        }
    }
}

fn encode_binary_delta(
    byte_slices: &[Option<&[u8]>],
    null_count: usize,
    buffer: &mut Vec<u8>,
    stats: &mut BinaryMaxMinStats,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) {
    let row_count = byte_slices.len();
    let non_null_count = row_count - null_count;

    if non_null_count == 0 {
        delta_bitpacked::encode(std::iter::empty(), buffer);
        return;
    }

    {
        let payload_bytes = byte_slices
            .iter()
            .flatten()
            .map(|value| value.len())
            .sum::<usize>();
        let capacity =
            payload_bytes + (non_null_count.saturating_sub(1) * std::mem::size_of::<i64>());
        buffer.reserve(capacity);
    }

    let lengths = byte_slices
        .iter()
        .filter_map(|&entry| entry)
        .map(|value| value.len() as i64);
    let lengths = ExactSizedIter::new(lengths, non_null_count);

    delta_bitpacked::encode(lengths, buffer);

    for value in byte_slices.iter().filter_map(|&entry| entry) {
        buffer.extend_from_slice(value);
        stats.update(value);
        if let Some(ref mut h) = bloom_hashes {
            h.insert(hash_byte(value));
        }
    }
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
    bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<Page> {
    let mut utf16_slices = Vec::with_capacity(column_top + offsets.len());
    utf16_slices.resize(column_top, None);
    extend_string_slices(&mut utf16_slices, offsets, data)?;
    string_slices_to_page(
        &utf16_slices,
        options,
        primitive_type,
        encoding,
        bloom_hashes,
    )
}

pub fn string_slices_to_page(
    utf16_slices: &[Option<&[u16]>],
    options: WriteOptions,
    primitive_type: PrimitiveType,
    encoding: Encoding,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<Page> {
    let num_rows = utf16_slices.len();
    let null_count = utf16_slices.iter().filter(|entry| entry.is_none()).count();
    let mut buffer = vec![];

    let deflevels_iter = utf16_slices.iter().map(|entry| entry.is_some());
    encode_primitive_def_levels(&mut buffer, deflevels_iter, num_rows, options.version)?;

    let definition_levels_byte_length = buffer.len();
    let mut stats = BinaryMaxMinStats::new(&primitive_type);

    match encoding {
        Encoding::Plain => {
            encode_string_plain(
                utf16_slices,
                &mut buffer,
                &mut stats,
                bloom_hashes.as_deref_mut(),
            )?;
        }
        Encoding::DeltaLengthByteArray => {
            encode_string_delta(
                utf16_slices,
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

pub(crate) fn extend_string_slices<'a>(
    utf16_slices: &mut Vec<Option<&'a [u16]>>,
    offsets: &[i64],
    data: &'a [u8],
) -> ParquetResult<()> {
    visit_string_entries(offsets, data, |entry| {
        utf16_slices.push(entry);
        Ok(())
    })?;
    Ok(())
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
        let len_offset = buffer.len();
        buffer.extend_from_slice(&[0; 4]);
        let utf8_start = buffer.len();
        let utf8_len = append_utf8_from_utf16(buffer, utf16)?;
        buffer[len_offset..utf8_start].copy_from_slice(&(utf8_len as u32).to_le_bytes());
        let value = &buffer[utf8_start..utf8_start + utf8_len];
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
        let utf8_start = buffer.len();
        let utf8_len = append_utf8_from_utf16(buffer, utf16)?;
        let value = &buffer[utf8_start..utf8_start + utf8_len];
        stats.update(value);
        if let Some(ref mut h) = bloom_hashes {
            h.insert(hash_byte(value));
        }
    }
    Ok(())
}

fn append_utf8_from_utf16(buffer: &mut Vec<u8>, utf16: &[u16]) -> ParquetResult<usize> {
    let start = buffer.len();
    for c in char::decode_utf16(utf16.iter().copied()) {
        let c = c.map_err(|e| fmt_err!(Layout, "invalid UTF-16 data in string column: {e}"))?;
        let mut tmp = [0; 4];
        buffer.extend_from_slice(c.encode_utf8(&mut tmp).as_bytes());
    }
    Ok(buffer.len() - start)
}

fn string_get_utf16_at_offset(data: &[u8], offset: i64) -> ParquetResult<Option<&[u16]>> {
    let offset = usize::try_from(offset).map_err(|_| {
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
    string_get_utf16(data)
}

fn string_get_utf16_validated(data: &[u8], offset: i64) -> Option<&[u16]> {
    string_get_utf16_at_offset(data, offset).expect("string segment was validated before encoding")
}

fn visit_string_entries<'a, F>(offsets: &[i64], data: &'a [u8], mut visit: F) -> ParquetResult<()>
where
    F: FnMut(Option<&'a [u16]>) -> ParquetResult<()>,
{
    for &offset in offsets {
        visit(string_get_utf16_at_offset(data, offset)?)?;
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
    bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<Page> {
    let mut utf8_slices = Vec::with_capacity(column_top + aux.len());
    utf8_slices.resize(column_top, None);
    extend_varchar_slices(&mut utf8_slices, aux, data)?;
    varchar_slices_to_page(
        &utf8_slices,
        options,
        primitive_type,
        encoding,
        bloom_hashes,
    )
}

pub fn varchar_slices_to_page(
    utf8_slices: &[Option<&[u8]>],
    options: WriteOptions,
    primitive_type: PrimitiveType,
    encoding: Encoding,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<Page> {
    let num_rows = utf8_slices.len();
    let null_count = utf8_slices.iter().filter(|entry| entry.is_none()).count();
    let mut buffer = vec![];

    let deflevels_iter = utf8_slices.iter().map(|entry| entry.is_some());
    encode_primitive_def_levels(&mut buffer, deflevels_iter, num_rows, options.version)?;

    let definition_levels_byte_length = buffer.len();
    let mut stats = BinaryMaxMinStats::new(&primitive_type);

    match encoding {
        Encoding::Plain => {
            encode_varchar_plain(
                utf8_slices,
                &mut buffer,
                &mut stats,
                bloom_hashes.as_deref_mut(),
            );
        }
        Encoding::DeltaLengthByteArray => {
            encode_varchar_delta(
                utf8_slices,
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

pub(crate) fn extend_varchar_slices<'a>(
    utf8_slices: &mut Vec<Option<&'a [u8]>>,
    aux: &'a [[u8; 16]],
    data: &'a [u8],
) -> ParquetResult<()> {
    visit_varchar_entries(aux, data, |entry| {
        utf8_slices.push(entry);
        Ok(())
    })?;
    Ok(())
}

fn varchar_get_slice<'a>(entry: &'a [u8; 16], data: &'a [u8]) -> ParquetResult<Option<&'a [u8]>> {
    debug_assert_eq!(std::mem::size_of::<VarcharAuxInlined>(), 16);
    debug_assert_eq!(std::mem::size_of::<VarcharAuxSplit>(), 16);

    // SAFETY: `VarcharAuxInlined` is `#[repr(C, packed)]` and exactly 16 bytes.
    // The source `&[[u8; 16]]` has compatible size and alignment 1.
    let entry: &VarcharAuxInlined = unsafe { std::mem::transmute(entry) };
    if varchar_is_null(entry.header) {
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
}

fn varchar_get_slice_validated<'a>(entry: &'a [u8; 16], data: &'a [u8]) -> Option<&'a [u8]> {
    varchar_get_slice(entry, data).expect("varchar segment was validated before encoding")
}

fn visit_varchar_entries<'a, F>(
    aux: &'a [[u8; 16]],
    data: &'a [u8],
    mut visit: F,
) -> ParquetResult<()>
where
    F: FnMut(Option<&'a [u8]>) -> ParquetResult<()>,
{
    for entry in aux {
        visit(varchar_get_slice(entry, data)?)?;
    }
    Ok(())
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
