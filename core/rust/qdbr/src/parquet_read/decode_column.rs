/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

//! Standalone column chunk decode functions that accept explicit metadata
//! parameters instead of reading from a `FileMetaData` struct. These are
//! the building blocks for the `_pm` decode path where metadata comes from
//! the `_pm` sidecar file rather than the parquet footer.

use std::cmp;

use parquet2::compression::Compression;
use parquet2::metadata::Descriptor;
use parquet2::read::{SlicePageReader, SlicedPage};
use parquet2::schema::types::{FieldInfo, PhysicalType, PrimitiveType};
use parquet2::schema::Repetition;
use qdb_core::col_type::{ColumnType, ColumnTypeTag};

use crate::allocator::QdbAllocator;
use crate::parquet::error::{fmt_err, ParquetErrorExt, ParquetResult};
use crate::parquet::qdb_metadata::QdbMetaCol;
use crate::parquet_read::column_sink::var::fixup_varchar_slice_spill_pointers;
use crate::parquet_read::decode::{
    decode_page, decode_page_filtered, decompress_sliced_data, decompress_sliced_dict,
    page_row_count, sliced_page_row_count,
};
use crate::parquet_read::row_groups::{
    decompress_varchar_slice_data, decompress_varchar_slice_dict,
};
use crate::parquet_read::{ColumnChunkBuffers, DecodeContext};

/// Decode a single i64 timestamp value from a column chunk. Both the `_pm`
/// read path (`decode_single_ts_from_pm`) and the Mig940 backfill path
/// (`decode_single_ts_value_from_parquet`) call this after resolving their
/// own descriptors and byte ranges.
///
/// # Safety
/// `allocator` must point to a valid `QdbAllocator` that outlives this call.
#[allow(clippy::too_many_arguments, clippy::not_unsafe_ptr_arg_deref)]
pub fn decode_single_timestamp_value(
    allocator: *const QdbAllocator,
    file_data: &[u8],
    col_start: usize,
    col_len: usize,
    compression: Compression,
    descriptor: Descriptor,
    num_values: i64,
    column_name: &str,
    row_group_index: usize,
    row_lo: usize,
    row_hi: usize,
) -> ParquetResult<i64> {
    let col_info = QdbMetaCol {
        column_type: ColumnType::new(ColumnTypeTag::Timestamp, 0),
        column_top: 0,
        format: None,
        ascii: None,
    };
    let mut ctx = DecodeContext::new(file_data.as_ptr(), file_data.len() as u64);
    // Safety: caller guarantees `allocator` points to a valid QdbAllocator
    // for the duration of this call.
    let alloc = unsafe { &*allocator }.clone();
    let mut bufs = ColumnChunkBuffers::new(alloc);
    let col_data = &file_data[col_start..col_start + col_len];
    decode_column_chunk_with_params(
        &mut ctx,
        &mut bufs,
        col_data,
        compression,
        descriptor,
        num_values,
        col_info,
        row_lo,
        row_hi,
        column_name,
        row_group_index,
    )?;
    if bufs.data_vec.len() < 8 {
        return Err(fmt_err!(
            InvalidType,
            "decoded timestamp buffer too small: {}",
            bufs.data_vec.len()
        ));
    }
    Ok(i64::from_le_bytes(bufs.data_vec[..8].try_into().unwrap()))
}

/// Builds a [`Descriptor`] from `_pm` column descriptor fields.
///
/// `logical_type` and `converted_type` are set to `None` — the decode
/// dispatch (Phase 1C) no longer depends on them.
pub fn reconstruct_descriptor(
    physical_type_u8: u8,
    fixed_byte_len: i32,
    max_rep_level: u8,
    max_def_level: u8,
    name: &str,
    repetition: Repetition,
) -> Descriptor {
    let physical_type = match physical_type_u8 {
        0 => PhysicalType::Boolean,
        1 => PhysicalType::Int32,
        2 => PhysicalType::Int64,
        3 => PhysicalType::Int96,
        4 => PhysicalType::Float,
        5 => PhysicalType::Double,
        6 => PhysicalType::ByteArray,
        7 => PhysicalType::FixedLenByteArray(fixed_byte_len as usize),
        _ => PhysicalType::Int64, // fallback; will fail at decode dispatch
    };
    Descriptor {
        primitive_type: PrimitiveType {
            field_info: FieldInfo { name: name.to_string(), repetition, id: None },
            logical_type: None,
            converted_type: None,
            physical_type,
        },
        max_def_level: max_def_level as i16,
        max_rep_level: max_rep_level as i16,
    }
}

/// Decode a column chunk given explicit metadata parameters.
///
/// This is the core decode loop extracted from `ParquetDecoder::decode_column_chunk()`.
/// It creates a `SlicePageReader` from the given byte range and iterates pages.
#[allow(clippy::too_many_arguments)]
pub fn decode_column_chunk_with_params(
    ctx: &mut DecodeContext,
    bufs: &mut ColumnChunkBuffers,
    col_data: &[u8],
    compression: Compression,
    descriptor: Descriptor,
    num_values: i64,
    col_info: QdbMetaCol,
    row_lo: usize,
    row_hi: usize,
    column_name: &str,
    row_group_index: usize,
) -> ParquetResult<usize> {
    let col_len = col_data.len();
    let page_reader = SlicePageReader::from_parts(
        col_data,
        0,
        col_len,
        compression,
        descriptor,
        num_values,
        col_len,
    )?;

    let mut dict = None;
    let mut row_count = 0usize;
    let is_varchar_slice = col_info.column_type.tag() == ColumnTypeTag::VarcharSlice;

    let DecodeContext {
        decompress_buffer,
        dict_decompress_buffer,
        varchar_slice_buf_pool,
        varchar_slice_dict_bufs_scratch: varchar_slice_dict_bufs,
        ..
    } = ctx;

    varchar_slice_buf_pool.append(&mut bufs.page_buffers);
    bufs.reset();

    let mut varchar_slice_page_bufs: Vec<Vec<u8>> = Vec::new();
    varchar_slice_dict_bufs.clear();

    for maybe_page in page_reader {
        let sliced_page = maybe_page?;

        match sliced_page {
            SlicedPage::Dict(dict_page) => {
                let page = if is_varchar_slice {
                    decompress_varchar_slice_dict(
                        dict_page,
                        varchar_slice_dict_bufs,
                        varchar_slice_buf_pool,
                    )?
                } else {
                    decompress_sliced_dict(dict_page, dict_decompress_buffer)?
                };
                dict = Some(page);
            }
            SlicedPage::Data(page) => {
                let page_row_count_opt = sliced_page_row_count(&page, col_info.column_type);

                if let Some(page_row_count) = page_row_count_opt {
                    let row_count_end = row_count.checked_add(page_row_count).ok_or_else(|| {
                        fmt_err!(
                            InvalidLayout,
                            "row count overflow: {} + page rows {}",
                            row_count,
                            page_row_count
                        )
                    })?;
                    if row_lo < row_count_end && row_hi > row_count {
                        let page = if is_varchar_slice {
                            decompress_varchar_slice_data(
                                &page,
                                decompress_buffer,
                                &mut varchar_slice_page_bufs,
                                varchar_slice_buf_pool,
                            )?
                        } else {
                            decompress_sliced_data(&page, decompress_buffer)?
                        };
                        decode_page(
                            &page,
                            dict.as_ref(),
                            bufs,
                            col_info,
                            row_lo.saturating_sub(row_count),
                            cmp::min(page_row_count, row_hi - row_count),
                        )
                        .with_context(|_| {
                            format!(
                                "could not decode page for column {:?} in row group {}",
                                column_name, row_group_index,
                            )
                        })?;
                    }
                    row_count = row_count_end;
                } else {
                    let page = if is_varchar_slice {
                        decompress_varchar_slice_data(
                            &page,
                            decompress_buffer,
                            &mut varchar_slice_page_bufs,
                            varchar_slice_buf_pool,
                        )?
                    } else {
                        decompress_sliced_data(&page, decompress_buffer)?
                    };
                    let page_row_count = page_row_count(&page, col_info.column_type)?;
                    let row_count_end = row_count.checked_add(page_row_count).ok_or_else(|| {
                        fmt_err!(
                            InvalidLayout,
                            "row count overflow: {} + page rows {}",
                            row_count,
                            page_row_count
                        )
                    })?;

                    if row_lo < row_count_end && row_hi > row_count {
                        decode_page(
                            &page,
                            dict.as_ref(),
                            bufs,
                            col_info,
                            row_lo.saturating_sub(row_count),
                            cmp::min(page_row_count, row_hi - row_count),
                        )
                        .with_context(|_| {
                            format!(
                                "could not decode page for column {:?} in row group {}",
                                column_name, row_group_index,
                            )
                        })?;
                    }
                    row_count = row_count_end;
                }
            }
        };
    }

    finish_varchar_slice(
        is_varchar_slice,
        bufs,
        &mut varchar_slice_page_bufs,
        varchar_slice_dict_bufs,
        varchar_slice_buf_pool,
    );
    bufs.refresh_ptrs();
    Ok(row_count)
}

/// Decode a column chunk with row filtering given explicit metadata parameters.
#[allow(clippy::too_many_arguments)]
pub fn decode_column_chunk_filtered_with_params<const FILL_NULLS: bool>(
    ctx: &mut DecodeContext,
    bufs: &mut ColumnChunkBuffers,
    col_data: &[u8],
    compression: Compression,
    descriptor: Descriptor,
    num_values: i64,
    col_info: QdbMetaCol,
    row_lo: usize,
    row_hi: usize,
    rows_filter: &[i64],
    column_name: &str,
    row_group_index: usize,
) -> ParquetResult<usize> {
    let col_len = col_data.len();
    let page_reader = SlicePageReader::from_parts(
        col_data,
        0,
        col_len,
        compression,
        descriptor,
        num_values,
        col_len,
    )?;

    let mut dict = None;
    let mut page_row_start = 0usize;
    let mut filter_idx = 0usize;
    let filter_count = rows_filter.len();
    let is_varchar_slice = col_info.column_type.tag() == ColumnTypeTag::VarcharSlice;

    let DecodeContext {
        decompress_buffer,
        dict_decompress_buffer,
        varchar_slice_buf_pool,
        varchar_slice_dict_bufs_scratch: varchar_slice_dict_bufs,
        ..
    } = ctx;

    varchar_slice_buf_pool.append(&mut bufs.page_buffers);
    bufs.reset();

    let mut varchar_slice_page_bufs: Vec<Vec<u8>> = Vec::new();
    varchar_slice_dict_bufs.clear();

    for maybe_page in page_reader {
        let sliced_page = maybe_page?;

        match sliced_page {
            SlicedPage::Dict(dict_page) => {
                let page = if is_varchar_slice {
                    decompress_varchar_slice_dict(
                        dict_page,
                        varchar_slice_dict_bufs,
                        varchar_slice_buf_pool,
                    )?
                } else {
                    decompress_sliced_dict(dict_page, dict_decompress_buffer)?
                };
                dict = Some(page);
            }
            SlicedPage::Data(page) => {
                let page_row_count_opt = sliced_page_row_count(&page, col_info.column_type);

                if let Some(page_row_count) = page_row_count_opt {
                    let page_end = page_row_start.checked_add(page_row_count).ok_or_else(|| {
                        fmt_err!(
                            InvalidLayout,
                            "page_end overflow: {} + {}",
                            page_row_start,
                            page_row_count
                        )
                    })?;
                    if page_end <= row_lo {
                        page_row_start = page_end;
                        continue;
                    }
                    if page_row_start >= row_hi {
                        break;
                    }

                    let page_filter_start = filter_idx;
                    if filter_count - filter_idx <= 64 {
                        while filter_idx < filter_count
                            && (rows_filter[filter_idx] as usize).saturating_add(row_lo) < page_end
                        {
                            filter_idx += 1;
                        }
                    } else {
                        filter_idx += rows_filter[filter_idx..]
                            .partition_point(|&r| (r as usize).saturating_add(row_lo) < page_end);
                    }

                    if FILL_NULLS {
                        let lo = row_lo.saturating_sub(page_row_start);
                        let hi = (row_hi - page_row_start).min(page_row_count);
                        let page = decompress_data_page(
                            is_varchar_slice,
                            &page,
                            decompress_buffer,
                            &mut varchar_slice_page_bufs,
                            varchar_slice_buf_pool,
                        )?;
                        decode_page_filtered::<true>(
                            &page,
                            dict.as_ref(),
                            bufs,
                            col_info,
                            page_row_start,
                            page_row_count,
                            row_lo,
                            lo,
                            hi,
                            &rows_filter[page_filter_start..filter_idx],
                        )
                        .with_context(|_| {
                            format!(
                                "could not decode page for column {:?} in row group {}",
                                column_name, row_group_index
                            )
                        })?;
                    } else if page_filter_start < filter_idx {
                        let page = decompress_data_page(
                            is_varchar_slice,
                            &page,
                            decompress_buffer,
                            &mut varchar_slice_page_bufs,
                            varchar_slice_buf_pool,
                        )?;
                        decode_page_filtered::<false>(
                            &page,
                            dict.as_ref(),
                            bufs,
                            col_info,
                            page_row_start,
                            page_row_count,
                            row_lo,
                            0,
                            0,
                            &rows_filter[page_filter_start..filter_idx],
                        )
                        .with_context(|_| {
                            format!(
                                "could not decode page for column {:?} in row group {}",
                                column_name, row_group_index
                            )
                        })?;
                    }
                    page_row_start = page_end;
                } else {
                    if page_row_start >= row_hi {
                        break;
                    }

                    let page = decompress_data_page(
                        is_varchar_slice,
                        &page,
                        decompress_buffer,
                        &mut varchar_slice_page_bufs,
                        varchar_slice_buf_pool,
                    )?;
                    let page_row_count = page_row_count(&page, col_info.column_type)?;
                    let page_end = page_row_start.checked_add(page_row_count).ok_or_else(|| {
                        fmt_err!(
                            InvalidLayout,
                            "page_end overflow: {} + {}",
                            page_row_start,
                            page_row_count
                        )
                    })?;

                    if page_end <= row_lo {
                        page_row_start = page_end;
                        continue;
                    }

                    let page_filter_start = filter_idx;
                    if filter_count - filter_idx <= 64 {
                        while filter_idx < filter_count
                            && (rows_filter[filter_idx] as usize).saturating_add(row_lo) < page_end
                        {
                            filter_idx += 1;
                        }
                    } else {
                        filter_idx += rows_filter[filter_idx..]
                            .partition_point(|&r| (r as usize).saturating_add(row_lo) < page_end);
                    }

                    if FILL_NULLS {
                        let lo = row_lo.saturating_sub(page_row_start);
                        let hi = (row_hi - page_row_start).min(page_row_count);
                        decode_page_filtered::<true>(
                            &page,
                            dict.as_ref(),
                            bufs,
                            col_info,
                            page_row_start,
                            page_row_count,
                            row_lo,
                            lo,
                            hi,
                            &rows_filter[page_filter_start..filter_idx],
                        )
                        .with_context(|_| {
                            format!(
                                "could not decode page for column {:?} in row group {}",
                                column_name, row_group_index
                            )
                        })?;
                    } else if page_filter_start < filter_idx {
                        decode_page_filtered::<false>(
                            &page,
                            dict.as_ref(),
                            bufs,
                            col_info,
                            page_row_start,
                            page_row_count,
                            row_lo,
                            0,
                            0,
                            &rows_filter[page_filter_start..filter_idx],
                        )
                        .with_context(|_| {
                            format!(
                                "could not decode page for column {:?} in row group {}",
                                column_name, row_group_index
                            )
                        })?;
                    }
                    page_row_start = page_end;
                }
            }
        };
    }

    finish_varchar_slice(
        is_varchar_slice,
        bufs,
        &mut varchar_slice_page_bufs,
        varchar_slice_dict_bufs,
        varchar_slice_buf_pool,
    );
    bufs.refresh_ptrs();
    if FILL_NULLS {
        Ok(row_hi - row_lo)
    } else {
        Ok(filter_count)
    }
}

fn decompress_data_page<'a>(
    is_varchar_slice: bool,
    page: &'a parquet2::read::SlicedDataPage<'a>,
    decompress_buffer: &'a mut Vec<u8>,
    varchar_slice_page_bufs: &'a mut Vec<Vec<u8>>,
    varchar_slice_buf_pool: &mut Vec<Vec<u8>>,
) -> ParquetResult<crate::parquet_read::page::DataPage<'a>> {
    if is_varchar_slice {
        decompress_varchar_slice_data(
            page,
            decompress_buffer,
            varchar_slice_page_bufs,
            varchar_slice_buf_pool,
        )
    } else {
        decompress_sliced_data(page, decompress_buffer)
    }
}

fn finish_varchar_slice(
    is_varchar_slice: bool,
    bufs: &mut ColumnChunkBuffers,
    varchar_slice_page_bufs: &mut Vec<Vec<u8>>,
    varchar_slice_dict_bufs: &mut Vec<Vec<u8>>,
    _varchar_slice_buf_pool: &mut Vec<Vec<u8>>,
) {
    if is_varchar_slice {
        if !bufs.data_vec.is_empty() {
            fixup_varchar_slice_spill_pointers(bufs);
        }
        // Move dict buffers in too — aux entries from
        // RleDictVarcharSliceDecoder hold raw pointers into them and
        // require them to outlive the column chunk decode.
        varchar_slice_page_bufs.append(varchar_slice_dict_bufs);
        bufs.page_buffers.append(varchar_slice_page_bufs);
    }
}

#[cfg(test)]
mod tests {
    use super::reconstruct_descriptor;
    use parquet2::schema::types::PhysicalType;
    use parquet2::schema::Repetition;

    use std::io::Cursor;
    use std::sync::Arc;

    use parquet::basic::Repetition as P1Repetition;
    use parquet::data_type::Int64Type;
    use parquet::file::properties::{WriterProperties, WriterVersion};
    use parquet::file::writer::SerializedFileWriter;
    use parquet::format::KeyValue;
    use parquet::schema::types::Type;
    use parquet2::read::read_metadata_with_size;
    use qdb_core::col_type::{ColumnType, ColumnTypeTag};

    use crate::allocator::TestAllocatorState;
    use crate::parquet::qdb_metadata::QdbMetaCol;
    use crate::parquet_read::{ColumnChunkBuffers, DecodeContext};

    /// Write a parquet file containing a single REQUIRED INT64 column
    /// with values `0, 1, 2, ..., row_count - 1`.
    fn write_i64_parquet(row_count: usize) -> Vec<u8> {
        let data: Vec<i64> = (0..row_count as i64).collect();
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(
                    Type::primitive_type_builder("col", parquet::basic::Type::INT64)
                        .with_repetition(P1Repetition::REQUIRED)
                        .build()
                        .unwrap(),
                )])
                .build()
                .unwrap(),
        );
        let props = Arc::new(
            WriterProperties::builder()
                .set_writer_version(WriterVersion::PARQUET_2_0)
                .set_dictionary_enabled(false)
                .set_key_value_metadata(Some(vec![KeyValue::new(
                    "questdb".to_string(),
                    format!(
                        r#"{{"version":1,"schema":[{{"column_type":{},"column_top":0}}]}}"#,
                        ColumnTypeTag::Long as u8
                    ),
                )]))
                .build(),
        );

        let mut cursor = Cursor::new(Vec::new());
        let mut writer = SerializedFileWriter::new(&mut cursor, schema, props).unwrap();
        let mut rg = writer.next_row_group().unwrap();
        if let Some(mut col) = rg.next_column().unwrap() {
            col.typed::<Int64Type>()
                .write_batch(&data, None, None)
                .unwrap();
            col.close().unwrap();
        }
        rg.close().unwrap();
        writer.close().unwrap();
        cursor.into_inner()
    }

    /// Decode a row range `[row_lo, row_hi)` from the first (and only) column
    /// of the given parquet buffer. Returns the raw decoded data bytes.
    fn decode_row_range(buf: &[u8], row_lo: usize, row_hi: usize) -> Vec<u8> {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let buf_len = buf.len() as u64;

        let mut cursor = Cursor::new(buf);
        let metadata = read_metadata_with_size(&mut cursor, buf_len).unwrap();
        let schema_col = &metadata.schema_descr.columns()[0];
        let desc = &schema_col.descriptor;
        let prim = &desc.primitive_type;

        let descriptor = super::reconstruct_descriptor(
            2, // Int64
            0,
            desc.max_rep_level as u8,
            desc.max_def_level as u8,
            "col",
            prim.field_info.repetition,
        );

        let rg = &metadata.row_groups[0];
        let chunk = &rg.columns()[0];
        let (col_start, col_len) = chunk.byte_range();
        let compression = chunk.compression();
        let num_values = chunk.num_values();

        let col_info = QdbMetaCol {
            column_type: ColumnType::new(ColumnTypeTag::Long, 0),
            column_top: 0,
            format: None,
            ascii: None,
        };

        let mut ctx = DecodeContext::new(buf.as_ptr(), buf_len);
        let mut bufs = ColumnChunkBuffers::new(allocator);

        let col_start = col_start as usize;
        let col_len = col_len as usize;
        super::decode_column_chunk_with_params(
            &mut ctx,
            &mut bufs,
            &buf[col_start..col_start + col_len],
            compression,
            descriptor,
            num_values,
            col_info,
            row_lo,
            row_hi,
            "col",
            0,
        )
        .unwrap();

        bufs.data_vec.to_vec()
    }

    /// Interpret a byte slice as a sequence of little-endian i64 values.
    fn read_i64s(data: &[u8]) -> Vec<i64> {
        assert_eq!(data.len() % 8, 0, "data length must be a multiple of 8");
        data.chunks_exact(8)
            .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
            .collect()
    }

    #[test]
    fn reconstruct_boolean() {
        let desc = reconstruct_descriptor(0, 0, 0, 1, "flag", Repetition::Optional);
        assert_eq!(desc.primitive_type.physical_type, PhysicalType::Boolean);
    }

    #[test]
    fn reconstruct_int32() {
        let desc = reconstruct_descriptor(1, 0, 0, 1, "count", Repetition::Required);
        assert_eq!(desc.primitive_type.physical_type, PhysicalType::Int32);
    }

    #[test]
    fn reconstruct_int64() {
        let desc = reconstruct_descriptor(2, 0, 0, 0, "big_count", Repetition::Required);
        assert_eq!(desc.primitive_type.physical_type, PhysicalType::Int64);
    }

    #[test]
    fn reconstruct_int96() {
        let desc = reconstruct_descriptor(3, 0, 0, 1, "nano_ts", Repetition::Optional);
        assert_eq!(desc.primitive_type.physical_type, PhysicalType::Int96);
    }

    #[test]
    fn reconstruct_float() {
        let desc = reconstruct_descriptor(4, 0, 0, 1, "temperature", Repetition::Optional);
        assert_eq!(desc.primitive_type.physical_type, PhysicalType::Float);
    }

    #[test]
    fn reconstruct_double() {
        let desc = reconstruct_descriptor(5, 0, 0, 0, "price", Repetition::Required);
        assert_eq!(desc.primitive_type.physical_type, PhysicalType::Double);
    }

    #[test]
    fn reconstruct_byte_array() {
        let desc = reconstruct_descriptor(6, 0, 0, 1, "payload", Repetition::Optional);
        assert_eq!(desc.primitive_type.physical_type, PhysicalType::ByteArray);
    }

    #[test]
    fn reconstruct_flba_16() {
        let desc = reconstruct_descriptor(7, 16, 0, 1, "uuid", Repetition::Optional);
        assert_eq!(
            desc.primitive_type.physical_type,
            PhysicalType::FixedLenByteArray(16)
        );
    }

    #[test]
    fn reconstruct_flba_32() {
        let desc = reconstruct_descriptor(7, 32, 0, 1, "hash256", Repetition::Optional);
        assert_eq!(
            desc.primitive_type.physical_type,
            PhysicalType::FixedLenByteArray(32)
        );
    }

    #[test]
    fn reconstruct_flba_arbitrary() {
        let desc = reconstruct_descriptor(7, 5, 0, 0, "short_fixed", Repetition::Required);
        assert_eq!(
            desc.primitive_type.physical_type,
            PhysicalType::FixedLenByteArray(5)
        );
    }

    #[test]
    fn reconstruct_invalid_phys_fallback() {
        let desc = reconstruct_descriptor(99, 0, 0, 0, "unknown", Repetition::Required);
        assert_eq!(desc.primitive_type.physical_type, PhysicalType::Int64);
    }

    #[test]
    fn reconstruct_logical_converted_always_none() {
        let type_ids: &[(u8, PhysicalType)] = &[
            (0, PhysicalType::Boolean),
            (1, PhysicalType::Int32),
            (2, PhysicalType::Int64),
            (3, PhysicalType::Int96),
            (4, PhysicalType::Float),
            (5, PhysicalType::Double),
            (6, PhysicalType::ByteArray),
            (7, PhysicalType::FixedLenByteArray(12)),
        ];
        for &(phys_id, ref expected_phys) in type_ids {
            let flba_len = if phys_id == 7 { 12 } else { 0 };
            let desc = reconstruct_descriptor(phys_id, flba_len, 0, 0, "col", Repetition::Required);
            assert_eq!(&desc.primitive_type.physical_type, expected_phys);
            assert!(
                desc.primitive_type.logical_type.is_none(),
                "logical_type should be None for phys_id={}",
                phys_id
            );
            assert!(
                desc.primitive_type.converted_type.is_none(),
                "converted_type should be None for phys_id={}",
                phys_id
            );
        }
    }

    #[test]
    fn reconstruct_preserves_rep_def_name() {
        // rep=2, def=3
        let desc = reconstruct_descriptor(2, 0, 2, 3, "nested_val", Repetition::Repeated);
        assert_eq!(desc.max_rep_level, 2);
        assert_eq!(desc.max_def_level, 3);
        assert_eq!(desc.primitive_type.field_info.name, "nested_val");
        assert_eq!(
            desc.primitive_type.field_info.repetition,
            Repetition::Repeated
        );

        // rep=0, def=0, Required
        let desc = reconstruct_descriptor(1, 0, 0, 0, "flat_req", Repetition::Required);
        assert_eq!(desc.max_rep_level, 0);
        assert_eq!(desc.max_def_level, 0);
        assert_eq!(desc.primitive_type.field_info.name, "flat_req");
        assert_eq!(
            desc.primitive_type.field_info.repetition,
            Repetition::Required
        );

        // rep=1, def=1, Optional
        let desc = reconstruct_descriptor(5, 0, 1, 1, "opt_col", Repetition::Optional);
        assert_eq!(desc.max_rep_level, 1);
        assert_eq!(desc.max_def_level, 1);
        assert_eq!(desc.primitive_type.field_info.name, "opt_col");
        assert_eq!(
            desc.primitive_type.field_info.repetition,
            Repetition::Optional
        );

        // field_info.id is always None
        assert!(desc.primitive_type.field_info.id.is_none());
    }

    #[test]
    fn reconstruct_flba_zero_length() {
        let desc = reconstruct_descriptor(7, 0, 0, 0, "empty_flba", Repetition::Required);
        assert_eq!(
            desc.primitive_type.physical_type,
            PhysicalType::FixedLenByteArray(0)
        );
    }

    #[test]
    fn row_range_single_first_row() {
        let buf = write_i64_parquet(10);
        let data = decode_row_range(&buf, 0, 1);
        assert_eq!(data.len(), 8, "expected exactly 1 i64 (8 bytes)");
        let values = read_i64s(&data);
        assert_eq!(values, vec![0i64]);
    }

    #[test]
    fn row_range_single_last_row() {
        let row_count = 10usize;
        let buf = write_i64_parquet(row_count);
        let data = decode_row_range(&buf, row_count - 1, row_count);
        assert_eq!(data.len(), 8, "expected exactly 1 i64 (8 bytes)");
        let values = read_i64s(&data);
        assert_eq!(values, vec![(row_count - 1) as i64]);
    }

    #[test]
    fn row_range_middle_slice() {
        let buf = write_i64_parquet(20);
        let data = decode_row_range(&buf, 5, 10);
        assert_eq!(data.len(), 5 * 8, "expected 5 i64 values (40 bytes)");
        let values = read_i64s(&data);
        assert_eq!(values, vec![5, 6, 7, 8, 9]);
    }

    #[test]
    fn truncated_column_chunk_returns_error() {
        let buf = write_i64_parquet(5);
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let buf_len = buf.len() as u64;

        let mut cursor = Cursor::new(&*buf);
        let metadata = read_metadata_with_size(&mut cursor, buf_len).unwrap();
        let schema_col = &metadata.schema_descr.columns()[0];
        let desc = &schema_col.descriptor;
        let prim = &desc.primitive_type;

        let descriptor = super::reconstruct_descriptor(
            2,
            0,
            desc.max_rep_level as u8,
            desc.max_def_level as u8,
            "col",
            prim.field_info.repetition,
        );

        let rg = &metadata.row_groups[0];
        let chunk = &rg.columns()[0];
        let (col_start, col_len) = chunk.byte_range();
        let compression = chunk.compression();
        let num_values = chunk.num_values();

        let col_info = QdbMetaCol {
            column_type: ColumnType::new(ColumnTypeTag::Long, 0),
            column_top: 0,
            format: None,
            ascii: None,
        };

        // Truncate the column chunk so the page reader can't parse a full page.
        let full = &buf[col_start as usize..col_start as usize + col_len as usize];
        let truncated = &full[..full.len() / 2];
        let mut ctx = DecodeContext::new(buf.as_ptr(), buf_len);
        let mut bufs = ColumnChunkBuffers::new(allocator);

        let result = super::decode_column_chunk_with_params(
            &mut ctx,
            &mut bufs,
            truncated,
            compression,
            descriptor,
            num_values,
            col_info,
            0,
            5,
            "col",
            0,
        );
        assert!(result.is_err(), "expected error for truncated column chunk");
    }

    #[test]
    fn column_type_new_raw_tests() {
        // code 0 should return None (undefined type)
        assert!(
            ColumnType::new_raw(0).is_none(),
            "ColumnType::new_raw(0) should be None"
        );

        // valid type codes should return Some
        let long_code = ColumnTypeTag::Long as i32; // 6
        let ct = ColumnType::new_raw(long_code);
        assert!(
            ct.is_some(),
            "ColumnType::new_raw({}) should be Some",
            long_code
        );
        let ct = ct.unwrap();
        assert_eq!(ct.tag(), ColumnTypeTag::Long);
        assert_eq!(ct.code(), long_code);

        // negative codes are non-zero, so new_raw succeeds
        let neg = ColumnType::new_raw(-1);
        assert!(
            neg.is_some(),
            "ColumnType::new_raw(-1) should be Some (non-zero)"
        );
    }
}
