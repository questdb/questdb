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
use qdb_core::col_type::ColumnTypeTag;

use crate::parquet::error::{ParquetErrorExt, ParquetResult};
use crate::parquet::qdb_metadata::QdbMetaCol;
use crate::parquet_read::column_sink::var::fixup_varchar_slice_spill_pointers;
use crate::parquet_read::decode::{
    decode_page, decode_page_filtered, decompress_sliced_data, decompress_sliced_dict,
    page_row_count, sliced_page_row_count,
};
use crate::parquet_read::row_groups::decompress_varchar_slice_data;
use crate::parquet_read::{ColumnChunkBuffers, DecodeContext};

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
    file_data: &[u8],
    col_start: usize,
    col_len: usize,
    compression: Compression,
    descriptor: Descriptor,
    num_values: i64,
    col_info: QdbMetaCol,
    row_lo: usize,
    row_hi: usize,
    column_name: &str,
    row_group_index: usize,
) -> ParquetResult<usize> {
    let page_reader = SlicePageReader::from_parts(
        file_data,
        col_start,
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
        varchar_slice_dict_buf,
        ..
    } = ctx;

    varchar_slice_buf_pool.append(&mut bufs.page_buffers);
    bufs.reset();

    let mut varchar_slice_page_bufs: Vec<Vec<u8>> = Vec::new();

    for maybe_page in page_reader {
        let sliced_page = maybe_page?;

        match sliced_page {
            SlicedPage::Dict(dict_page) => {
                let page = if is_varchar_slice {
                    decompress_sliced_dict(dict_page, varchar_slice_dict_buf)?
                } else {
                    decompress_sliced_dict(dict_page, dict_decompress_buffer)?
                };
                dict = Some(page);
            }
            SlicedPage::Data(page) => {
                let page_row_count_opt = sliced_page_row_count(&page, col_info.column_type);

                if let Some(page_row_count) = page_row_count_opt {
                    if row_lo < row_count + page_row_count && row_hi > row_count {
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
                    row_count += page_row_count;
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

                    if row_lo < row_count + page_row_count && row_hi > row_count {
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
                    row_count += page_row_count;
                }
            }
        };
    }

    finish_varchar_slice(
        is_varchar_slice,
        bufs,
        &mut varchar_slice_page_bufs,
        varchar_slice_dict_buf,
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
    file_data: &[u8],
    col_start: usize,
    col_len: usize,
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
    let page_reader = SlicePageReader::from_parts(
        file_data,
        col_start,
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
        varchar_slice_dict_buf,
        ..
    } = ctx;

    varchar_slice_buf_pool.append(&mut bufs.page_buffers);
    bufs.reset();

    let mut varchar_slice_page_bufs: Vec<Vec<u8>> = Vec::new();

    for maybe_page in page_reader {
        let sliced_page = maybe_page?;

        match sliced_page {
            SlicedPage::Dict(dict_page) => {
                let page = if is_varchar_slice {
                    decompress_sliced_dict(dict_page, varchar_slice_dict_buf)?
                } else {
                    decompress_sliced_dict(dict_page, dict_decompress_buffer)?
                };
                dict = Some(page);
            }
            SlicedPage::Data(page) => {
                let page_row_count_opt = sliced_page_row_count(&page, col_info.column_type);

                if let Some(page_row_count) = page_row_count_opt {
                    let page_end = page_row_start + page_row_count;
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
                            && (rows_filter[filter_idx] as usize + row_lo) < page_end
                        {
                            filter_idx += 1;
                        }
                    } else {
                        filter_idx += rows_filter[filter_idx..]
                            .partition_point(|&r| (r as usize + row_lo) < page_end);
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
                    let page_end = page_row_start + page_row_count;

                    if page_end <= row_lo {
                        page_row_start = page_end;
                        continue;
                    }

                    let page_filter_start = filter_idx;
                    if filter_count - filter_idx <= 64 {
                        while filter_idx < filter_count
                            && (rows_filter[filter_idx] as usize + row_lo) < page_end
                        {
                            filter_idx += 1;
                        }
                    } else {
                        filter_idx += rows_filter[filter_idx..]
                            .partition_point(|&r| (r as usize + row_lo) < page_end);
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
        varchar_slice_dict_buf,
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
    varchar_slice_dict_buf: &mut Vec<u8>,
    varchar_slice_buf_pool: &mut Vec<Vec<u8>>,
) {
    if is_varchar_slice {
        if !bufs.data_vec.is_empty() {
            fixup_varchar_slice_spill_pointers(bufs);
        }
        bufs.page_buffers = std::mem::take(varchar_slice_page_bufs);
        if !varchar_slice_dict_buf.is_empty() {
            let replacement = varchar_slice_buf_pool.pop().unwrap_or_default();
            bufs.page_buffers
                .push(std::mem::replace(varchar_slice_dict_buf, replacement));
        }
    }
}
