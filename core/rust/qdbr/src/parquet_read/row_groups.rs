use crate::allocator::{AcVec, QdbAllocator};
use crate::parquet::error::{fmt_err, ParquetErrorExt, ParquetResult};
use crate::parquet::qdb_metadata::{QdbMeta, QdbMetaCol};
use crate::parquet_read::decode::{
    decode_page, decode_page_filtered, decompress_sliced_data, decompress_sliced_dict,
    page_row_count, sliced_page_row_count,
};
use crate::parquet_read::{
    ColumnChunkBuffers, ColumnFilterPacked, ColumnFilterValues, ColumnMeta, DecodeContext,
    RowGroupStatBuffers, FILTER_OP_EQ, FILTER_OP_GE, FILTER_OP_GT, FILTER_OP_IS_NOT_NULL,
    FILTER_OP_IS_NULL, FILTER_OP_LE, FILTER_OP_LT, MILLIS_PER_DAY,
};
use nonmax::NonMaxU32;
use parquet2::metadata::FileMetaData;
use parquet2::read::{SlicePageReader, SlicedPage};
use parquet2::schema::types::{PhysicalType, PrimitiveConvertedType, PrimitiveLogicalType};
use qdb_core::col_type::{ColumnType, ColumnTypeTag};
use std::{cmp, ptr, slice};

// The metadata fields are accessed from Java.
// This struct contains only immutable metadata.
// The reader is passed as a parameter to decode methods.
#[repr(C)]
pub struct ParquetDecoder {
    pub allocator: QdbAllocator,
    pub col_count: u32,
    pub row_count: usize,
    pub row_group_count: u32,
    pub row_group_sizes_ptr: *const u32,
    pub row_group_sizes: AcVec<u32>,
    // None (stored as zero, which is equal to ~u32::MAX) means no designated timestamp
    pub timestamp_index: Option<NonMaxU32>,
    pub columns_ptr: *const ColumnMeta,
    pub columns: AcVec<ColumnMeta>,
    pub metadata: FileMetaData,
    pub qdb_meta: Option<QdbMeta>,
    pub row_group_sizes_acc: AcVec<usize>,
}

/// The local positional index as it is stored in parquet.
/// Not to be confused with the field_id in the parquet metadata.
pub type ParquetColumnIndex = i32;

// The fields are accessed from Java.
#[repr(C)]
pub struct RowGroupBuffers {
    pub(super) column_bufs_ptr: *const ColumnChunkBuffers,
    pub(super) column_bufs: AcVec<ColumnChunkBuffers>,
}

impl RowGroupBuffers {
    pub fn new(allocator: QdbAllocator) -> Self {
        Self {
            column_bufs_ptr: ptr::null_mut(),
            column_bufs: AcVec::new_in(allocator),
        }
    }

    pub fn ensure_n_columns(&mut self, required_cols: usize) -> ParquetResult<()> {
        if self.column_bufs.len() < required_cols {
            let allocator = self.column_bufs.allocator().clone();
            self.column_bufs
                .resize_with(required_cols, || ColumnChunkBuffers::new(allocator.clone()))?;
            self.column_bufs_ptr = self.column_bufs.as_mut_ptr();
        }
        Ok(())
    }

    pub fn column_buffers(&self) -> &AcVec<ColumnChunkBuffers> {
        &self.column_bufs
    }
}

impl ParquetDecoder {
    pub fn decode_row_group(
        &self,
        ctx: &mut DecodeContext,
        row_group_bufs: &mut RowGroupBuffers,
        columns: &[(ParquetColumnIndex, ColumnType)],
        row_group_index: u32,
        row_group_lo: u32,
        row_group_hi: u32,
    ) -> ParquetResult<usize> {
        if row_group_index >= self.row_group_count {
            return Err(fmt_err!(
                InvalidLayout,
                "row group index {} out of range [0,{})",
                row_group_index,
                self.row_group_count
            ));
        }

        let accumulated_size = self.row_group_sizes_acc[row_group_index as usize];
        row_group_bufs.ensure_n_columns(columns.len())?;

        let mut decoded = 0usize;
        for (dest_col_idx, &(column_idx, to_column_type)) in columns.iter().enumerate() {
            let column_idx = column_idx as usize;
            let mut column_type = self.columns[column_idx].column_type.ok_or_else(|| {
                fmt_err!(
                    InvalidType,
                    "unknown column type, column index: {}",
                    column_idx
                )
            })?;

            // Special case for handling symbol columns in QuestDB-created Parquet files.
            // The `read_parquet` function does not support symbol columns,
            // so this workaround allows them to be read as varchar columns.
            if column_type.tag() == ColumnTypeTag::Symbol
                && to_column_type.tag() == ColumnTypeTag::Varchar
            {
                column_type = to_column_type;
            }

            if column_type != to_column_type {
                return Err(fmt_err!(
                    InvalidType,
                    "requested column type {} does not match file column type {}, column index: {}",
                    to_column_type,
                    column_type,
                    column_idx
                ));
            }

            let column_chunk_bufs = &mut row_group_bufs.column_bufs[dest_col_idx];

            // Get the column's format from the "questdb" key-value metadata stored in the file.
            let (column_top, format) = self
                .qdb_meta
                .as_ref()
                .and_then(|m| m.schema.get(column_idx))
                .map(|c| (c.column_top, c.format))
                .unwrap_or((0, None));

            if column_top >= row_group_hi as usize + accumulated_size {
                column_chunk_bufs.reset();
                continue;
            }

            let col_info = QdbMetaCol { column_type, column_top, format };
            match self.decode_column_chunk(
                ctx,
                column_chunk_bufs,
                row_group_index as usize,
                row_group_lo as usize,
                row_group_hi as usize,
                column_idx,
                col_info,
            ) {
                Ok(column_chunk_decoded) => {
                    if decoded > 0 && decoded != column_chunk_decoded {
                        return Err(fmt_err!(
                            InvalidLayout,
                            "column chunk size {column_chunk_decoded} does not match previous size {decoded}",
                        ));
                    }
                    decoded = column_chunk_decoded;
                }
                Err(err) => {
                    return Err(err);
                }
            }
        }

        Ok(decoded)
    }

    /// Decode only specific rows from a row group.
    /// The `rows_filter` contains the row indices (relative to the row group) to decode.
    /// For example, if rows_filter = [2, 3, 4, 5, 6, 9], only those rows will be decoded.
    #[allow(clippy::too_many_arguments)]
    pub fn decode_row_group_filtered<const FILL_NULLS: bool>(
        &self,
        ctx: &mut DecodeContext,
        row_group_bufs: &mut RowGroupBuffers,
        dest_col_offset: usize,
        columns: &[(ParquetColumnIndex, ColumnType)],
        row_group_index: u32,
        row_group_lo: u32,
        row_group_hi: u32,
        rows_filter: &[i64],
    ) -> ParquetResult<usize> {
        if row_group_index >= self.row_group_count {
            return Err(fmt_err!(
                InvalidLayout,
                "row group index {} out of range [0,{})",
                row_group_index,
                self.row_group_count
            ));
        }

        let output_count = if FILL_NULLS {
            (row_group_hi - row_group_lo) as usize
        } else {
            rows_filter.len()
        };

        if !FILL_NULLS && rows_filter.is_empty() {
            // No rows to decode
            row_group_bufs.ensure_n_columns(dest_col_offset + columns.len())?;
            for i in 0..columns.len() {
                let column_chunk_bufs = &mut row_group_bufs.column_bufs[dest_col_offset + i];
                column_chunk_bufs.reset();
            }
            return Ok(0);
        }

        let accumulated_size = self.row_group_sizes_acc[row_group_index as usize];
        row_group_bufs.ensure_n_columns(dest_col_offset + columns.len())?;

        let mut decoded = 0usize;

        for (i, &(column_idx, to_column_type)) in columns.iter().enumerate() {
            let dest_col_idx = dest_col_offset + i;
            let column_idx = column_idx as usize;
            let mut column_type = self.columns[column_idx].column_type.ok_or_else(|| {
                fmt_err!(
                    InvalidType,
                    "unknown column type, column index: {}",
                    column_idx
                )
            })?;

            // Special case for handling symbol columns in QuestDB-created Parquet files.
            if column_type.tag() == ColumnTypeTag::Symbol
                && to_column_type.tag() == ColumnTypeTag::Varchar
            {
                column_type = to_column_type;
            }

            if column_type != to_column_type {
                return Err(fmt_err!(
                    InvalidType,
                    "requested column type {} does not match file column type {}, column index: {}",
                    to_column_type,
                    column_type,
                    column_idx
                ));
            }

            let column_chunk_bufs = &mut row_group_bufs.column_bufs[dest_col_idx];

            // Get the column's format from the "questdb" key-value metadata stored in the file.
            let (column_top, format) = self
                .qdb_meta
                .as_ref()
                .and_then(|m| m.schema.get(column_idx))
                .map(|c| (c.column_top, c.format))
                .unwrap_or((0, None));

            if column_top >= row_group_hi as usize + accumulated_size {
                column_chunk_bufs.reset();
                continue;
            }

            let col_info = QdbMetaCol { column_type, column_top, format };

            // Decode the column chunk with row filter
            match self.decode_column_chunk_filtered::<FILL_NULLS>(
                ctx,
                column_chunk_bufs,
                row_group_index as usize,
                row_group_lo as usize,
                row_group_hi as usize,
                column_idx,
                col_info,
                rows_filter,
            ) {
                Ok(column_chunk_decoded) => {
                    if decoded > 0 && decoded != column_chunk_decoded {
                        return Err(fmt_err!(
                            InvalidLayout,
                            "column chunk size {column_chunk_decoded} does not match previous size {decoded}",
                        ));
                    }
                    decoded = column_chunk_decoded;
                }
                Err(err) => {
                    return Err(err);
                }
            }
        }

        Ok(output_count)
    }

    #[allow(clippy::too_many_arguments)]
    fn decode_column_chunk_filtered<const FILL_NULLS: bool>(
        &self,
        ctx: &mut DecodeContext,
        column_chunk_bufs: &mut ColumnChunkBuffers,
        row_group_index: usize,
        row_group_lo: usize,
        row_group_hi: usize,
        column_index: usize,
        col_info: QdbMetaCol,
        rows_filter: &[i64],
    ) -> ParquetResult<usize> {
        let columns = self.metadata.row_groups[row_group_index].columns();
        let column_metadata = &columns[column_index];

        let chunk_size = column_metadata.compressed_size();
        let chunk_size = chunk_size
            .try_into()
            .map_err(|_| fmt_err!(Layout, "column chunk size overflow, size: {chunk_size}"))?;

        // SAFETY: `DecodeContext` is created from a caller-owned mmap region and guarantees
        // `file_ptr` is valid for `file_size` bytes for the lifetime of this decode call.
        let buf = unsafe { slice::from_raw_parts(ctx.file_ptr, ctx.file_size as usize) };
        let page_reader = SlicePageReader::new(buf, column_metadata, chunk_size)?;

        match self.metadata.version {
            1 | 2 => Ok(()),
            ver => Err(fmt_err!(Unsupported, "unsupported parquet version: {ver}")),
        }?;

        let mut dict = None;
        let mut page_row_start = 0usize;
        let mut filter_idx = 0usize;
        let filter_count = rows_filter.len();

        column_chunk_bufs.reset();

        let dict_decompress_buffer = &mut ctx.dict_decompress_buffer;
        let decompress_buffer = &mut ctx.decompress_buffer;

        for maybe_page in page_reader {
            let sliced_page = maybe_page?;

            match sliced_page {
                SlicedPage::Dict(dict_page) => {
                    let page = decompress_sliced_dict(dict_page, dict_decompress_buffer)?;
                    dict = Some(page);
                }
                SlicedPage::Data(page) => {
                    let page_row_count_opt = sliced_page_row_count(&page, col_info.column_type);

                    if let Some(page_row_count) = page_row_count_opt {
                        let page_end = page_row_start + page_row_count;
                        if page_end <= row_group_lo {
                            page_row_start = page_end;
                            continue;
                        }
                        if page_row_start >= row_group_hi {
                            break;
                        }

                        let page_filter_start = filter_idx;
                        if filter_count - filter_idx <= 64 {
                            while filter_idx < filter_count
                                && (rows_filter[filter_idx] as usize + row_group_lo) < page_end
                            {
                                filter_idx += 1;
                            }
                        } else {
                            filter_idx += rows_filter[filter_idx..]
                                .partition_point(|&r| (r as usize + row_group_lo) < page_end);
                        }

                        if FILL_NULLS {
                            let row_lo = row_group_lo.saturating_sub(page_row_start);
                            let row_hi = (row_group_hi - page_row_start).min(page_row_count);
                            let page = decompress_sliced_data(&page, decompress_buffer)?;
                            decode_page_filtered::<true>(
                                &page,
                                dict.as_ref(),
                                column_chunk_bufs,
                                col_info,
                                page_row_start,
                                page_row_count,
                                row_group_lo,
                                row_lo,
                                row_hi,
                                &rows_filter[page_filter_start..filter_idx],
                            )
                            .with_context(|_| {
                                format!(
                                    "could not decode page for column {:?} in row group {}",
                                    self.metadata.schema_descr.columns()[column_index]
                                        .descriptor
                                        .primitive_type
                                        .field_info
                                        .name,
                                    row_group_index,
                                )
                            })?;
                        } else if page_filter_start < filter_idx {
                            let page = decompress_sliced_data(&page, decompress_buffer)?;
                            decode_page_filtered::<false>(
                                &page,
                                dict.as_ref(),
                                column_chunk_bufs,
                                col_info,
                                page_row_start,
                                page_row_count,
                                row_group_lo,
                                0,
                                0,
                                &rows_filter[page_filter_start..filter_idx],
                            )
                            .with_context(|_| {
                                format!(
                                    "could not decode page for column {:?} in row group {}",
                                    self.metadata.schema_descr.columns()[column_index]
                                        .descriptor
                                        .primitive_type
                                        .field_info
                                        .name,
                                    row_group_index,
                                )
                            })?;
                        }
                        page_row_start = page_end;
                    } else {
                        if page_row_start >= row_group_hi {
                            break;
                        }

                        let page = decompress_sliced_data(&page, decompress_buffer)?;
                        let page_row_count = page_row_count(&page, col_info.column_type)?;
                        let page_end = page_row_start + page_row_count;

                        if page_end <= row_group_lo {
                            page_row_start = page_end;
                            continue;
                        }

                        let page_filter_start = filter_idx;
                        if filter_count - filter_idx <= 64 {
                            while filter_idx < filter_count
                                && (rows_filter[filter_idx] as usize + row_group_lo) < page_end
                            {
                                filter_idx += 1;
                            }
                        } else {
                            filter_idx += rows_filter[filter_idx..]
                                .partition_point(|&r| (r as usize + row_group_lo) < page_end);
                        }

                        if FILL_NULLS {
                            let row_lo = row_group_lo.saturating_sub(page_row_start);
                            let row_hi = (row_group_hi - page_row_start).min(page_row_count);

                            decode_page_filtered::<true>(
                                &page,
                                dict.as_ref(),
                                column_chunk_bufs,
                                col_info,
                                page_row_start,
                                page_row_count,
                                row_group_lo,
                                row_lo,
                                row_hi,
                                &rows_filter[page_filter_start..filter_idx],
                            )
                            .with_context(|_| {
                                format!(
                                    "could not decode page for column {:?} in row group {}",
                                    self.metadata.schema_descr.columns()[column_index]
                                        .descriptor
                                        .primitive_type
                                        .field_info
                                        .name,
                                    row_group_index,
                                )
                            })?;
                        } else if page_filter_start < filter_idx {
                            decode_page_filtered::<false>(
                                &page,
                                dict.as_ref(),
                                column_chunk_bufs,
                                col_info,
                                page_row_start,
                                page_row_count,
                                row_group_lo,
                                0,
                                0,
                                &rows_filter[page_filter_start..filter_idx],
                            )
                            .with_context(|_| {
                                format!(
                                    "could not decode page for column {:?} in row group {}",
                                    self.metadata.schema_descr.columns()[column_index]
                                        .descriptor
                                        .primitive_type
                                        .field_info
                                        .name,
                                    row_group_index,
                                )
                            })?;
                        }
                        page_row_start = page_end;
                    }
                }
            };
        }

        column_chunk_bufs.refresh_ptrs();
        if FILL_NULLS {
            Ok(row_group_hi - row_group_lo)
        } else {
            Ok(filter_count)
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn decode_column_chunk(
        &self,
        ctx: &mut DecodeContext,
        column_chunk_bufs: &mut ColumnChunkBuffers,
        row_group_index: usize,
        row_group_lo: usize,
        row_group_hi: usize,
        column_index: usize,
        col_info: QdbMetaCol,
    ) -> ParquetResult<usize> {
        let columns = self.metadata.row_groups[row_group_index].columns();
        let column_metadata = &columns[column_index];

        let chunk_size = column_metadata.compressed_size();
        let chunk_size = chunk_size
            .try_into()
            .map_err(|_| fmt_err!(Layout, "column chunk size overflow, size: {chunk_size}"))?;

        // SAFETY: `DecodeContext` is created from a caller-owned mmap region and guarantees
        // `file_ptr` is valid for `file_size` bytes for the lifetime of this decode call.
        let buf = unsafe { slice::from_raw_parts(ctx.file_ptr, ctx.file_size as usize) };
        let page_reader = SlicePageReader::new(buf, column_metadata, chunk_size)?;

        match self.metadata.version {
            1 | 2 => Ok(()),
            ver => Err(fmt_err!(Unsupported, "unsupported parquet version: {ver}")),
        }?;

        let mut dict = None;
        let mut row_count = 0usize;

        column_chunk_bufs.reset();

        let dict_decompress_buffer = &mut ctx.dict_decompress_buffer;
        let decompress_buffer = &mut ctx.decompress_buffer;

        for maybe_page in page_reader {
            let sliced_page = maybe_page?;

            match sliced_page {
                SlicedPage::Dict(dict_page) => {
                    let page = decompress_sliced_dict(dict_page, dict_decompress_buffer)?;
                    dict = Some(page);
                }
                SlicedPage::Data(page) => {
                    let page_row_count_opt = sliced_page_row_count(&page, col_info.column_type);

                    if let Some(page_row_count) = page_row_count_opt {
                        if row_group_lo < row_count + page_row_count && row_group_hi > row_count {
                            let page = decompress_sliced_data(&page, decompress_buffer)?;
                            decode_page(
                                &page,
                                dict.as_ref(),
                                column_chunk_bufs,
                                col_info,
                                row_group_lo.saturating_sub(row_count),
                                cmp::min(page_row_count, row_group_hi - row_count),
                            )
                            .with_context(|_| {
                                format!(
                                    "could not decode page for column {:?} in row group {}",
                                    self.metadata.schema_descr.columns()[column_index]
                                        .descriptor
                                        .primitive_type
                                        .field_info
                                        .name,
                                    row_group_index,
                                )
                            })?;
                        }
                        row_count += page_row_count;
                    } else {
                        let page = decompress_sliced_data(&page, decompress_buffer)?;
                        let page_row_count = page_row_count(&page, col_info.column_type)?;

                        if row_group_lo < row_count + page_row_count && row_group_hi > row_count {
                            decode_page(
                                &page,
                                dict.as_ref(),
                                column_chunk_bufs,
                                col_info,
                                row_group_lo.saturating_sub(row_count),
                                cmp::min(page_row_count, row_group_hi - row_count),
                            )
                            .with_context(|_| {
                                format!(
                                    "could not decode page for column {:?} in row group {}",
                                    self.metadata.schema_descr.columns()[column_index]
                                        .descriptor
                                        .primitive_type
                                        .field_info
                                        .name,
                                    row_group_index,
                                )
                            })?;
                        }
                        row_count += page_row_count;
                    }
                }
            };
        }

        column_chunk_bufs.refresh_ptrs();
        Ok(row_count)
    }

    pub fn read_column_chunk_stats(
        &self,
        row_group_stat_buffers: &mut RowGroupStatBuffers,
        columns: &[(ParquetColumnIndex, ColumnType)],
        row_group_index: u32,
    ) -> ParquetResult<()> {
        if row_group_index >= self.row_group_count {
            return Err(fmt_err!(
                InvalidLayout,
                "row group index {} out of range [0,{})",
                row_group_index,
                self.row_group_count
            ));
        }

        row_group_stat_buffers.ensure_n_columns(columns.len())?;
        let row_group_index = row_group_index as usize;
        for (dest_col_idx, &(column_idx, to_column_type)) in columns.iter().enumerate() {
            let column_idx = column_idx as usize;
            let column_type = self.columns[column_idx].column_type.ok_or_else(|| {
                fmt_err!(
                    InvalidType,
                    "unknown column type, column index: {}",
                    column_idx
                )
            })?;
            if column_type != to_column_type {
                return Err(fmt_err!(
                    InvalidType,
                    "requested column type {} does not match file column type {}, column index: {}",
                    to_column_type,
                    column_type,
                    column_idx
                ));
            }

            let columns_meta = self.metadata.row_groups[row_group_index].columns();
            let column_metadata = &columns_meta[column_idx];
            let column_chunk = column_metadata.column_chunk();
            let stats = &mut row_group_stat_buffers.column_chunk_stats[dest_col_idx];

            stats.min_value.clear();
            stats.max_value.clear();

            if let Some(meta_data) = &column_chunk.meta_data {
                if let Some(statistics) = &meta_data.statistics {
                    if let Some(min) = statistics.min_value.as_ref() {
                        stats.min_value.extend_from_slice(min)?;
                    }
                    if let Some(max) = statistics.max_value.as_ref() {
                        stats.max_value.extend_from_slice(max)?;
                    }
                }
            }

            stats.min_value_ptr = stats.min_value.as_mut_ptr();
            stats.min_value_size = stats.min_value.len();
            stats.max_value_ptr = stats.max_value.as_mut_ptr();
            stats.max_value_size = stats.max_value.len();
        }
        Ok(())
    }

    pub fn can_skip_row_group(
        &self,
        row_group_index: u32,
        file_data: &[u8],
        filters: &[ColumnFilterPacked],
    ) -> ParquetResult<bool> {
        if row_group_index >= self.row_group_count {
            return Err(fmt_err!(
                InvalidLayout,
                "row group index {} out of range [0,{})",
                row_group_index,
                self.row_group_count
            ));
        }

        let row_group_index = row_group_index as usize;
        let columns_meta = self.metadata.row_groups[row_group_index].columns();
        let column_count = columns_meta.len();
        for packed_filter in filters {
            let count = packed_filter.count();
            let op = packed_filter.operation_type();

            if count > 0 && packed_filter.ptr == 0 {
                return Err(fmt_err!(
                    InvalidLayout,
                    "invalid filter payload: null pointer with non-zero count, column index: {}",
                    packed_filter.column_index()
                ));
            }
            let column_idx = packed_filter.column_index() as usize;
            if column_idx >= column_count {
                continue;
            }

            let column_metadata = &columns_meta[column_idx];
            let column_chunk_meta = column_metadata.column_chunk().meta_data.as_ref();
            let statistics = column_chunk_meta.and_then(|m| m.statistics.as_ref());
            let null_count = statistics.and_then(|s| s.null_count);
            let num_values = column_chunk_meta.map(|m| m.num_values);

            if op == FILTER_OP_IS_NULL {
                if null_count == Some(0) {
                    return Ok(true);
                }
                continue;
            }
            if op == FILTER_OP_IS_NOT_NULL {
                if let (Some(nc), Some(nv)) = (null_count, num_values) {
                    if nc == nv {
                        return Ok(true);
                    }
                }
                continue;
            }

            let filter_desc = ColumnFilterValues { count, ptr: packed_filter.ptr };
            let physical_type = column_metadata.physical_type();
            let has_nulls = null_count.is_none_or(|c| c > 0);

            let (min_bytes, max_bytes) = statistics
                .map(|s| {
                    let min = s.min_value.as_deref().or(s.min.as_deref());
                    let max = s.max_value.as_deref().or(s.max.as_deref());
                    (min, max)
                })
                .unwrap_or((None, None));

            match op {
                FILTER_OP_EQ => {
                    let is_decimal = Self::is_decimal_type(column_metadata);
                    let qdb_column_type = packed_filter.qdb_column_type();

                    let bitset =
                        parquet2::bloom_filter::read_from_slice(column_metadata, file_data)
                            .unwrap_or(&[]);
                    if !bitset.is_empty() {
                        let all_absent = Self::all_values_absent_from_bloom(
                            bitset,
                            &physical_type,
                            &filter_desc,
                            has_nulls,
                            is_decimal,
                            qdb_column_type,
                        );
                        if all_absent {
                            return Ok(true);
                        }
                    }

                    let col_type_tag = qdb_column_type & 0xFF;
                    let is_ipv4 = col_type_tag == ColumnTypeTag::IPv4 as i32;
                    let is_date = col_type_tag == ColumnTypeTag::Date as i32;
                    let is_qdb_unsigned = is_ipv4 || col_type_tag == ColumnTypeTag::Char as i32;
                    // Skip min/max filtering for third-party unsigned types (not IPv4 or Char).
                    // QuestDB doesn't support unsigned integers, so filter values are signed
                    // but third-party Parquet statistics are unsigned - comparison would be incorrect.
                    let is_third_party_unsigned =
                        !is_qdb_unsigned && Self::is_unsigned_int_type(column_metadata);
                    if !is_third_party_unsigned
                        && Self::all_values_outside_min_max_with_stats(
                            &physical_type,
                            &filter_desc,
                            has_nulls,
                            is_decimal,
                            is_ipv4,
                            is_date,
                            min_bytes,
                            max_bytes,
                        )
                    {
                        return Ok(true);
                    }
                }
                FILTER_OP_LT | FILTER_OP_LE | FILTER_OP_GT | FILTER_OP_GE => {
                    let is_decimal = Self::is_decimal_type(column_metadata);
                    let qdb_column_type = packed_filter.qdb_column_type();
                    let col_type_tag = qdb_column_type & 0xFF;
                    let is_ipv4 = col_type_tag == ColumnTypeTag::IPv4 as i32;
                    let is_date = col_type_tag == ColumnTypeTag::Date as i32;
                    let is_qdb_unsigned = is_ipv4 || col_type_tag == ColumnTypeTag::Char as i32;
                    // Skip min/max filtering for third-party unsigned types (not IPv4 or Char).
                    let is_third_party_unsigned =
                        !is_qdb_unsigned && Self::is_unsigned_int_type(column_metadata);

                    if !is_third_party_unsigned
                        && Self::value_outside_range(
                            &physical_type,
                            &filter_desc,
                            is_decimal,
                            is_ipv4,
                            is_date,
                            op,
                            min_bytes,
                            max_bytes,
                        )
                    {
                        return Ok(true);
                    }
                }
                _ => {}
            }
        }

        Ok(false)
    }

    #[inline]
    fn is_decimal_type(column_metadata: &parquet2::metadata::ColumnChunkMetaData) -> bool {
        let primitive_type = &column_metadata.descriptor().descriptor.primitive_type;
        matches!(
            primitive_type.logical_type,
            Some(PrimitiveLogicalType::Decimal(_, _))
        ) || matches!(
            primitive_type.converted_type,
            Some(PrimitiveConvertedType::Decimal(_, _))
        )
    }

    #[inline]
    fn is_unsigned_int_type(column_metadata: &parquet2::metadata::ColumnChunkMetaData) -> bool {
        use parquet2::schema::types::IntegerType;
        let primitive_type = &column_metadata.descriptor().descriptor.primitive_type;
        matches!(
            primitive_type.logical_type,
            Some(PrimitiveLogicalType::Integer(
                IntegerType::UInt8
                    | IntegerType::UInt16
                    | IntegerType::UInt32
                    | IntegerType::UInt64
            ))
        ) || matches!(
            primitive_type.converted_type,
            Some(
                PrimitiveConvertedType::Uint8
                    | PrimitiveConvertedType::Uint16
                    | PrimitiveConvertedType::Uint32
                    | PrimitiveConvertedType::Uint64
            )
        )
    }

    fn all_values_absent_from_bloom(
        bitset: &[u8],
        physical_type: &PhysicalType,
        filter_desc: &ColumnFilterValues,
        has_nulls: bool,
        is_decimal: bool,
        qdb_column_type: i32,
    ) -> bool {
        let count = filter_desc.count as usize;
        if count == 0 {
            return false;
        }

        let ptr = filter_desc.ptr as *const u8;
        match physical_type {
            PhysicalType::Int32 => {
                let col_type_tag = qdb_column_type & 0xFF;
                let is_ipv4 = col_type_tag == ColumnTypeTag::IPv4 as i32;
                let is_date = col_type_tag == ColumnTypeTag::Date as i32;
                if is_date {
                    for i in 0..count {
                        let millis = unsafe { (ptr as *const i64).add(i).read_unaligned() };
                        if millis == i64::MIN {
                            if has_nulls {
                                return false;
                            }
                        } else {
                            let days = (millis / MILLIS_PER_DAY) as i32;
                            if parquet2::bloom_filter::is_in_set(
                                bitset,
                                parquet2::bloom_filter::hash_native(days),
                            ) {
                                return false;
                            }
                        }
                    }
                    true
                } else {
                    for i in 0..count {
                        let v = unsafe { (ptr as *const i32).add(i).read_unaligned() };
                        let is_null = if is_ipv4 { v == 0 } else { v == i32::MIN };
                        if is_null {
                            if has_nulls {
                                return false;
                            }
                        } else if parquet2::bloom_filter::is_in_set(
                            bitset,
                            parquet2::bloom_filter::hash_native(v),
                        ) {
                            return false;
                        }
                    }
                    true
                }
            }
            PhysicalType::Int64 => {
                for i in 0..count {
                    let v = unsafe { (ptr as *const i64).add(i).read_unaligned() };
                    if v == i64::MIN {
                        if has_nulls {
                            return false;
                        }
                    } else if parquet2::bloom_filter::is_in_set(
                        bitset,
                        parquet2::bloom_filter::hash_native(v),
                    ) {
                        return false;
                    }
                }
                true
            }
            PhysicalType::Float => {
                for i in 0..count {
                    let v = unsafe { (ptr as *const f32).add(i).read_unaligned() };
                    if v.is_nan() {
                        if has_nulls {
                            return false;
                        }
                    } else {
                        // Canonicalize -0.0 to +0.0
                        let normalized = if v == 0.0 { 0.0f32 } else { v };
                        if parquet2::bloom_filter::is_in_set(
                            bitset,
                            parquet2::bloom_filter::hash_native(normalized),
                        ) {
                            return false;
                        }
                    }
                }
                true
            }
            PhysicalType::Double => {
                for i in 0..count {
                    let v = unsafe { (ptr as *const f64).add(i).read_unaligned() };
                    if v.is_nan() {
                        if has_nulls {
                            return false;
                        }
                    } else {
                        // Canonicalize -0.0 to +0.0
                        let normalized = if v == 0.0 { 0.0f64 } else { v };
                        if parquet2::bloom_filter::is_in_set(
                            bitset,
                            parquet2::bloom_filter::hash_native(normalized),
                        ) {
                            return false;
                        }
                    }
                }
                true
            }
            PhysicalType::ByteArray => {
                let mut offset = 0usize;
                for _ in 0..count {
                    let len = unsafe { (ptr.add(offset) as *const i32).read_unaligned() };
                    offset += size_of::<i32>();
                    if len < 0 {
                        if has_nulls {
                            return false;
                        }
                    } else {
                        let len = len as usize;
                        let bytes = unsafe { slice::from_raw_parts(ptr.add(offset), len) };
                        offset += len;
                        if parquet2::bloom_filter::is_in_set(
                            bitset,
                            parquet2::bloom_filter::hash_byte(bytes),
                        ) {
                            return false;
                        }
                    }
                }
                true
            }
            PhysicalType::FixedLenByteArray(size) => {
                let size = *size;
                let null_check = if is_decimal {
                    is_fixed_len_null_be
                } else {
                    is_fixed_len_null
                };
                for i in 0..count {
                    let bytes = unsafe { slice::from_raw_parts(ptr.add(i * size), size) };
                    if null_check(bytes) {
                        if has_nulls {
                            return false;
                        }
                    } else if parquet2::bloom_filter::is_in_set(
                        bitset,
                        parquet2::bloom_filter::hash_byte(bytes),
                    ) {
                        return false;
                    }
                }
                true
            }
            _ => false,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn all_values_outside_min_max_with_stats(
        physical_type: &PhysicalType,
        filter_desc: &ColumnFilterValues,
        has_nulls: bool,
        is_decimal: bool,
        is_ipv4: bool,
        is_date: bool,
        min_bytes: Option<&[u8]>,
        max_bytes: Option<&[u8]>,
    ) -> bool {
        let count = filter_desc.count as usize;
        if count == 0 {
            return false;
        }

        let ptr = filter_desc.ptr as *const u8;
        match physical_type {
            PhysicalType::Int32 if is_ipv4 => {
                let min_max = match (min_bytes, max_bytes) {
                    (Some(min_b), Some(max_b)) if min_b.len() == 4 && max_b.len() == 4 => Some((
                        u32::from_le_bytes(min_b.try_into().unwrap()),
                        u32::from_le_bytes(max_b.try_into().unwrap()),
                    )),
                    _ => None,
                };
                for i in 0..count {
                    let v = unsafe { (ptr as *const u32).add(i).read_unaligned() };
                    if v == 0 {
                        if has_nulls {
                            return false;
                        }
                    } else if let Some((min_val, max_val)) = min_max {
                        if v >= min_val && v <= max_val {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                true
            }
            // Signed Int32 (Byte, Short, Char, Int, Date).
            // DATE: filter values are i64 millis, converted to i32 days.
            // Others: filter values are i32, NULL = i32::MIN.
            PhysicalType::Int32 => {
                let min_max = match (min_bytes, max_bytes) {
                    (Some(min_b), Some(max_b)) if min_b.len() == 4 && max_b.len() == 4 => Some((
                        i32::from_le_bytes(min_b.try_into().unwrap()),
                        i32::from_le_bytes(max_b.try_into().unwrap()),
                    )),
                    _ => None,
                };
                for i in 0..count {
                    let (v, is_null) = if is_date {
                        let millis = unsafe { (ptr as *const i64).add(i).read_unaligned() };
                        if millis == i64::MIN {
                            (0, true)
                        } else {
                            ((millis / MILLIS_PER_DAY) as i32, false)
                        }
                    } else {
                        let v = unsafe { (ptr as *const i32).add(i).read_unaligned() };
                        (v, v == i32::MIN)
                    };
                    if is_null {
                        if has_nulls {
                            return false;
                        }
                    } else if let Some((min_val, max_val)) = min_max {
                        if v >= min_val && v <= max_val {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                true
            }
            // Signed Int64 (Long, Timestamp, Date): NULL = i64::MIN
            PhysicalType::Int64 => {
                let min_max = match (min_bytes, max_bytes) {
                    (Some(min_b), Some(max_b)) if min_b.len() == 8 && max_b.len() == 8 => Some((
                        i64::from_le_bytes(min_b.try_into().unwrap()),
                        i64::from_le_bytes(max_b.try_into().unwrap()),
                    )),
                    _ => None,
                };
                for i in 0..count {
                    let v = unsafe { (ptr as *const i64).add(i).read_unaligned() };
                    if v == i64::MIN {
                        if has_nulls {
                            return false;
                        }
                    } else if let Some((min_val, max_val)) = min_max {
                        if v >= min_val && v <= max_val {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                true
            }
            PhysicalType::Float => {
                let min_max = match (min_bytes, max_bytes) {
                    (Some(min_b), Some(max_b)) if min_b.len() == 4 && max_b.len() == 4 => {
                        let min_val = f32::from_le_bytes(min_b.try_into().unwrap());
                        let max_val = f32::from_le_bytes(max_b.try_into().unwrap());
                        if min_val.is_nan() || max_val.is_nan() {
                            None
                        } else {
                            Some((min_val, max_val))
                        }
                    }
                    _ => None,
                };
                for i in 0..count {
                    let v = unsafe { (ptr as *const f32).add(i).read_unaligned() };
                    if v.is_nan() {
                        if has_nulls {
                            return false;
                        }
                    } else if let Some((min_val, max_val)) = min_max {
                        if v >= min_val && v <= max_val {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                true
            }
            PhysicalType::Double => {
                let min_max = match (min_bytes, max_bytes) {
                    (Some(min_b), Some(max_b)) if min_b.len() == 8 && max_b.len() == 8 => {
                        let min_val = f64::from_le_bytes(min_b.try_into().unwrap());
                        let max_val = f64::from_le_bytes(max_b.try_into().unwrap());
                        if min_val.is_nan() || max_val.is_nan() {
                            None
                        } else {
                            Some((min_val, max_val))
                        }
                    }
                    _ => None,
                };
                for i in 0..count {
                    let v = unsafe { (ptr as *const f64).add(i).read_unaligned() };
                    if v.is_nan() {
                        if has_nulls {
                            return false;
                        }
                    } else if let Some((min_val, max_val)) = min_max {
                        if v >= min_val && v <= max_val {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                true
            }
            PhysicalType::ByteArray => {
                let (min_b, max_b) = match (min_bytes, max_bytes) {
                    (Some(min_b), Some(max_b)) => (min_b, max_b),
                    _ => return false,
                };
                let mut offset = 0usize;
                for _ in 0..count {
                    let len = unsafe { (ptr.add(offset) as *const i32).read_unaligned() };
                    offset += size_of::<i32>();
                    if len < 0 {
                        if has_nulls {
                            return false;
                        }
                    } else {
                        let len = len as usize;
                        let bytes = unsafe { slice::from_raw_parts(ptr.add(offset), len) };
                        offset += len;
                        let in_range = if is_decimal {
                            compare_signed_be_varlen(bytes, min_b) != cmp::Ordering::Less
                                && compare_signed_be_varlen(bytes, max_b) != cmp::Ordering::Greater
                        } else {
                            bytes >= min_b && bytes <= max_b
                        };
                        if in_range {
                            return false;
                        }
                    }
                }
                true
            }
            PhysicalType::FixedLenByteArray(size) => {
                let size = *size;
                let min_max = match (min_bytes, max_bytes) {
                    (Some(min_b), Some(max_b)) if min_b.len() == size && max_b.len() == size => {
                        Some((min_b, max_b))
                    }
                    _ => None,
                };
                let null_check = if is_decimal {
                    is_fixed_len_null_be
                } else {
                    is_fixed_len_null
                };
                for i in 0..count {
                    let bytes = unsafe { slice::from_raw_parts(ptr.add(i * size), size) };
                    if null_check(bytes) {
                        if has_nulls {
                            return false;
                        }
                    } else if let Some((min_b, max_b)) = min_max {
                        if is_decimal {
                            if compare_signed_be(bytes, min_b) != cmp::Ordering::Less
                                && compare_signed_be(bytes, max_b) != cmp::Ordering::Greater
                            {
                                return false;
                            }
                        } else if bytes >= min_b && bytes <= max_b {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                true
            }
            _ => false,
        }
    }

    /// Check if a range filter value proves that the row group can be skipped.
    /// For a single filter value and a given comparison op, checks:
    ///   LT (col < val): skip if min >= val
    ///   LE (col <= val): skip if min > val
    ///   GT (col > val): skip if max <= val
    ///   GE (col >= val): skip if max < val
    #[allow(clippy::too_many_arguments)]
    fn value_outside_range(
        physical_type: &PhysicalType,
        filter_desc: &ColumnFilterValues,
        is_decimal: bool,
        is_ipv4: bool,
        is_date: bool,
        op: u8,
        min_bytes: Option<&[u8]>,
        max_bytes: Option<&[u8]>,
    ) -> bool {
        let count = filter_desc.count as usize;
        // Range operations (LT/LE/GT/GE) expect exactly one value.
        // If not, conservatively don't skip.
        if count != 1 {
            return false;
        }
        let ptr = filter_desc.ptr as *const u8;

        match physical_type {
            PhysicalType::Int32 if is_ipv4 => {
                let (min_val, max_val) = match (min_bytes, max_bytes) {
                    (Some(min_b), Some(max_b)) if min_b.len() == 4 && max_b.len() == 4 => (
                        u32::from_le_bytes(min_b.try_into().unwrap()),
                        u32::from_le_bytes(max_b.try_into().unwrap()),
                    ),
                    _ => return false,
                };
                let v = unsafe { (ptr as *const u32).read_unaligned() };
                if v == 0 {
                    return false;
                }
                match op {
                    FILTER_OP_LT => min_val >= v,
                    FILTER_OP_LE => min_val > v,
                    FILTER_OP_GT => max_val <= v,
                    FILTER_OP_GE => max_val < v,
                    _ => false,
                }
            }
            // Signed Int32 (Byte, Short, Char, Int, Date).
            // DATE: filter value is i64 millis, converted to i32 days.
            // Others: filter value is i32, NULL = i32::MIN.
            PhysicalType::Int32 => {
                let (min_val, max_val) = match (min_bytes, max_bytes) {
                    (Some(min_b), Some(max_b)) if min_b.len() == 4 && max_b.len() == 4 => (
                        i32::from_le_bytes(min_b.try_into().unwrap()),
                        i32::from_le_bytes(max_b.try_into().unwrap()),
                    ),
                    _ => return false,
                };
                let (v, is_null) = if is_date {
                    let millis = unsafe { (ptr as *const i64).read_unaligned() };
                    if millis == i64::MIN {
                        (0, true)
                    } else {
                        ((millis / MILLIS_PER_DAY) as i32, false)
                    }
                } else {
                    let v = unsafe { (ptr as *const i32).read_unaligned() };
                    (v, v == i32::MIN)
                };
                if is_null {
                    return false;
                }
                match op {
                    FILTER_OP_LT => min_val >= v,
                    FILTER_OP_LE => min_val > v,
                    FILTER_OP_GT => max_val <= v,
                    FILTER_OP_GE => max_val < v,
                    _ => false,
                }
            }
            // Signed Int64 (Long, Timestamp, Date): NULL = i64::MIN
            PhysicalType::Int64 => {
                let (min_val, max_val) = match (min_bytes, max_bytes) {
                    (Some(min_b), Some(max_b)) if min_b.len() == 8 && max_b.len() == 8 => (
                        i64::from_le_bytes(min_b.try_into().unwrap()),
                        i64::from_le_bytes(max_b.try_into().unwrap()),
                    ),
                    _ => return false,
                };
                let v = unsafe { (ptr as *const i64).read_unaligned() };
                if v == i64::MIN {
                    return false;
                }
                match op {
                    FILTER_OP_LT => min_val >= v,
                    FILTER_OP_LE => min_val > v,
                    FILTER_OP_GT => max_val <= v,
                    FILTER_OP_GE => max_val < v,
                    _ => false,
                }
            }
            PhysicalType::Float => {
                let (min_val, max_val) = match (min_bytes, max_bytes) {
                    (Some(min_b), Some(max_b)) if min_b.len() == 4 && max_b.len() == 4 => {
                        let min_val = f32::from_le_bytes(min_b.try_into().unwrap());
                        let max_val = f32::from_le_bytes(max_b.try_into().unwrap());
                        if min_val.is_nan() || max_val.is_nan() {
                            return false;
                        }
                        (min_val, max_val)
                    }
                    _ => return false,
                };
                let v = unsafe { (ptr as *const f32).read_unaligned() };
                if v.is_nan() {
                    return false;
                }
                match op {
                    FILTER_OP_LT => min_val >= v,
                    FILTER_OP_LE => min_val > v,
                    FILTER_OP_GT => max_val <= v,
                    FILTER_OP_GE => max_val < v,
                    _ => false,
                }
            }
            PhysicalType::Double => {
                let (min_val, max_val) = match (min_bytes, max_bytes) {
                    (Some(min_b), Some(max_b)) if min_b.len() == 8 && max_b.len() == 8 => {
                        let min_val = f64::from_le_bytes(min_b.try_into().unwrap());
                        let max_val = f64::from_le_bytes(max_b.try_into().unwrap());
                        if min_val.is_nan() || max_val.is_nan() {
                            return false;
                        }
                        (min_val, max_val)
                    }
                    _ => return false,
                };
                let v = unsafe { (ptr as *const f64).read_unaligned() };
                if v.is_nan() {
                    return false;
                }
                match op {
                    FILTER_OP_LT => min_val >= v,
                    FILTER_OP_LE => min_val > v,
                    FILTER_OP_GT => max_val <= v,
                    FILTER_OP_GE => max_val < v,
                    _ => false,
                }
            }
            PhysicalType::ByteArray => {
                let (min_b, max_b) = match (min_bytes, max_bytes) {
                    (Some(min_b), Some(max_b)) => (min_b, max_b),
                    _ => return false,
                };
                let len = unsafe { (ptr as *const i32).read_unaligned() };
                if len < 0 {
                    return false;
                }
                let len = len as usize;
                let bytes = unsafe { slice::from_raw_parts(ptr.add(size_of::<i32>()), len) };

                if is_decimal {
                    let cmp_min = compare_signed_be_varlen(min_b, bytes);
                    let cmp_max = compare_signed_be_varlen(max_b, bytes);
                    match op {
                        FILTER_OP_LT => cmp_min != cmp::Ordering::Less, // min >= val
                        FILTER_OP_LE => cmp_min == cmp::Ordering::Greater, // min > val
                        FILTER_OP_GT => cmp_max != cmp::Ordering::Greater, // max <= val
                        FILTER_OP_GE => cmp_max == cmp::Ordering::Less, // max < val
                        _ => false,
                    }
                } else {
                    match op {
                        FILTER_OP_LT => min_b >= bytes,
                        FILTER_OP_LE => min_b > bytes,
                        FILTER_OP_GT => max_b <= bytes,
                        FILTER_OP_GE => max_b < bytes,
                        _ => false,
                    }
                }
            }
            PhysicalType::FixedLenByteArray(size) => {
                let size = *size;
                let (min_b, max_b) = match (min_bytes, max_bytes) {
                    (Some(min_b), Some(max_b)) if min_b.len() == size && max_b.len() == size => {
                        (min_b, max_b)
                    }
                    _ => return false,
                };
                let bytes = unsafe { slice::from_raw_parts(ptr, size) };
                let null_check = if is_decimal {
                    is_fixed_len_null_be
                } else {
                    is_fixed_len_null
                };
                if null_check(bytes) {
                    return false;
                }

                if is_decimal {
                    let cmp_min = compare_signed_be(min_b, bytes);
                    let cmp_max = compare_signed_be(max_b, bytes);
                    match op {
                        FILTER_OP_LT => cmp_min != cmp::Ordering::Less,
                        FILTER_OP_LE => cmp_min == cmp::Ordering::Greater,
                        FILTER_OP_GT => cmp_max != cmp::Ordering::Greater,
                        FILTER_OP_GE => cmp_max == cmp::Ordering::Less,
                        _ => false,
                    }
                } else {
                    match op {
                        FILTER_OP_LT => min_b >= bytes,
                        FILTER_OP_LE => min_b > bytes,
                        FILTER_OP_GT => max_b <= bytes,
                        FILTER_OP_GE => max_b < bytes,
                        _ => false,
                    }
                }
            }
            _ => false,
        }
    }

    pub fn find_row_group_by_timestamp(
        &self,
        file_ptr: *const u8,
        file_size: u64,
        timestamp: i64,
        row_lo: usize,
        row_hi: usize,
        timestamp_column_index: u32,
    ) -> ParquetResult<u64> {
        if timestamp_column_index >= self.col_count {
            return Err(fmt_err!(
                InvalidLayout,
                "timestamp column index {} out of range [0,{})",
                timestamp_column_index,
                self.col_count
            ));
        }

        let ts = timestamp_column_index as usize;
        self.validate_timestamp_column(ts)?;

        let row_group_count = self.row_group_count;
        let mut row_count = 0usize;
        let mut sorting_key_validated = false;
        for (row_group_idx, row_group_meta) in self.metadata.row_groups.iter().enumerate() {
            let columns_meta = row_group_meta.columns();
            let column_metadata = &columns_meta[ts];
            let column_chunk = column_metadata.column_chunk();
            let column_chunk_meta = column_chunk.meta_data.as_ref().ok_or_else(|| {
                fmt_err!(
                    InvalidType,
                    "metadata not found for timestamp column, column index: {}",
                    ts
                )
            })?;

            let column_chunk_size = column_chunk_meta.num_values as usize;
            if column_chunk_size == 0 {
                continue;
            }
            if row_hi + 1 < row_count {
                break;
            }
            if row_lo < row_count + column_chunk_size {
                let min_value = match self.row_group_timestamp_stat::<false>(
                    row_group_idx,
                    ts,
                    Some(column_metadata),
                )? {
                    Some(val) => val,
                    None => {
                        if !sorting_key_validated {
                            self.validate_timestamp_sorting_key(ts)?;
                            sorting_key_validated = true;
                        }
                        self.decode_single_timestamp(file_ptr, file_size, row_group_idx, ts, 0, 1)?
                    }
                };

                // Our overall scan direction is Vect#BIN_SEARCH_SCAN_DOWN (increasing
                // scan direction) and we're iterating over row groups left-to-right,
                // so as soon as we find the matching timestamp, we're done.
                //
                // The returned value includes the row group index shifted by +1,
                // as well as a flag to tell the caller that the timestamp is at the
                // right boundary of a row group or in a gap between two row groups
                // and, thus, row group decoding is not needed.

                // The value is to the left of the row group.
                if timestamp < min_value {
                    // We don't need to decode the row group (odd value).
                    return Ok((2 * row_group_idx + 1) as u64);
                }

                let max_value = match self.row_group_timestamp_stat::<true>(
                    row_group_idx,
                    ts,
                    Some(column_metadata),
                )? {
                    Some(val) => val,
                    None => {
                        if !sorting_key_validated {
                            self.validate_timestamp_sorting_key(ts)?;
                            sorting_key_validated = true;
                        }
                        self.decode_single_timestamp(
                            file_ptr,
                            file_size,
                            row_group_idx,
                            ts,
                            column_chunk_size - 1,
                            column_chunk_size,
                        )?
                    }
                };

                if timestamp < max_value {
                    return Ok(2 * (row_group_idx + 1) as u64);
                }
            }
            row_count += column_chunk_size;
        }

        // The value is to the right of the last row group, no need to decode (odd value).
        Ok((2 * row_group_count + 1) as u64)
    }

    fn row_group_timestamp_stat<const IS_MAX: bool>(
        &self,
        row_group_index: usize,
        timestamp_column_index: usize,
        column_chunk_meta: Option<&parquet2::metadata::ColumnChunkMetaData>,
    ) -> ParquetResult<Option<i64>> {
        let owned;
        let chunk_meta = match column_chunk_meta {
            Some(m) => m,
            None => {
                let columns_meta = self.metadata.row_groups[row_group_index].columns();
                owned = &columns_meta[timestamp_column_index];
                owned
            }
        };
        let meta_data = match &chunk_meta.column_chunk().meta_data {
            Some(m) => m,
            None => return Ok(None),
        };
        let statistics = match &meta_data.statistics {
            Some(s) => s,
            None => return Ok(None),
        };
        let value = if IS_MAX {
            &statistics.max_value
        } else {
            &statistics.min_value
        };
        match value {
            Some(v) if v.len() == 8 => Ok(Some(i64::from_le_bytes(
                v[0..8].try_into().expect("unexpected vec length"),
            ))),
            Some(v) => Err(fmt_err!(
                InvalidLayout,
                "unexpected timestamp stat byte array size of {}",
                v.len()
            )),
            None => Ok(None),
        }
    }

    fn validate_timestamp_column(&self, ts: usize) -> ParquetResult<()> {
        let column_type = self.columns[ts].column_type.ok_or_else(|| {
            fmt_err!(
                InvalidType,
                "unknown timestamp column type, column index: {}",
                ts
            )
        })?;
        if column_type.tag() != ColumnTypeTag::Timestamp {
            return Err(fmt_err!(
                InvalidType,
                "expected timestamp column, but got {}, column index: {}",
                column_type,
                ts
            ));
        }
        Ok(())
    }

    fn validate_timestamp_sorting_key(&self, timestamp_column_index: usize) -> ParquetResult<()> {
        if let Some(ts_idx) = self.timestamp_index {
            if ts_idx.get() as usize == timestamp_column_index {
                return Ok(());
            }
        }

        Err(fmt_err!(
            InvalidLayout,
            "timestamp column {} is not an ascending sorting key, \
             cannot determine min/max without statistics",
            timestamp_column_index
        ))
    }

    fn decode_single_timestamp(
        &self,
        file_ptr: *const u8,
        file_size: u64,
        row_group_index: usize,
        timestamp_column_index: usize,
        row_lo: usize,
        row_hi: usize,
    ) -> ParquetResult<i64> {
        let mut ctx = DecodeContext::new(file_ptr, file_size);
        let mut bufs = ColumnChunkBuffers::new(self.allocator.clone());
        let col_info = QdbMetaCol {
            column_type: ColumnType::new(ColumnTypeTag::Timestamp, 0),
            column_top: 0,
            format: None,
        };
        self.decode_column_chunk(
            &mut ctx,
            &mut bufs,
            row_group_index,
            row_lo,
            row_hi,
            timestamp_column_index,
            col_info,
        )?;
        let data = &bufs.data_vec;
        if data.len() < std::mem::size_of::<i64>() {
            return Err(fmt_err!(
                InvalidLayout,
                "decoded timestamp buffer too short: expected at least 8 bytes, got {}",
                data.len()
            ));
        }
        Ok(i64::from_le_bytes(data[..8].try_into().unwrap()))
    }

    pub fn row_group_min_timestamp(
        &self,
        file_ptr: *const u8,
        file_size: u64,
        row_group_index: u32,
        timestamp_column_index: u32,
    ) -> ParquetResult<i64> {
        if row_group_index >= self.row_group_count {
            return Err(fmt_err!(
                InvalidLayout,
                "row group index {} out of range [0,{})",
                row_group_index,
                self.row_group_count
            ));
        }
        if timestamp_column_index >= self.col_count {
            return Err(fmt_err!(
                InvalidLayout,
                "timestamp column index {} out of range [0,{})",
                timestamp_column_index,
                self.col_count
            ));
        }

        let rg = row_group_index as usize;
        let ts = timestamp_column_index as usize;
        self.validate_timestamp_column(ts)?;

        // Try statistics first
        if let Some(val) = self.row_group_timestamp_stat::<false>(rg, ts, None)? {
            return Ok(val);
        }

        self.validate_timestamp_sorting_key(ts)?;
        self.decode_single_timestamp(file_ptr, file_size, rg, ts, 0, 1)
    }

    pub fn row_group_max_timestamp(
        &self,
        file_ptr: *const u8,
        file_size: u64,
        row_group_index: u32,
        timestamp_column_index: u32,
    ) -> ParquetResult<i64> {
        if row_group_index >= self.row_group_count {
            return Err(fmt_err!(
                InvalidLayout,
                "row group index {} out of range [0,{})",
                row_group_index,
                self.row_group_count
            ));
        }
        if timestamp_column_index >= self.col_count {
            return Err(fmt_err!(
                InvalidLayout,
                "timestamp column index {} out of range [0,{})",
                timestamp_column_index,
                self.col_count
            ));
        }

        let rg = row_group_index as usize;
        let ts = timestamp_column_index as usize;
        self.validate_timestamp_column(ts)?;
        if let Some(val) = self.row_group_timestamp_stat::<true>(rg, ts, None)? {
            return Ok(val);
        }

        self.validate_timestamp_sorting_key(ts)?;
        let row_group_size = self.row_group_sizes[rg] as usize;
        if row_group_size == 0 {
            return Err(fmt_err!(
                InvalidLayout,
                "row group {} has zero rows for timestamp column {}",
                row_group_index,
                timestamp_column_index
            ));
        }
        self.decode_single_timestamp(
            file_ptr,
            file_size,
            rg,
            ts,
            row_group_size - 1,
            row_group_size,
        )
    }
}

/// Check if a FixedLenByteArray value is the null sentinel (LE format).
/// Matches the write path null_value: `i64::MIN` LE bytes repeated.
/// Used for UUID and LONG128.
fn is_fixed_len_null(bytes: &[u8]) -> bool {
    let le_null = i64::MIN.to_le_bytes();
    bytes.iter().enumerate().all(|(i, &b)| b == le_null[i % 8])
}

/// Check if a big-endian FixedLenByteArray value is the decimal null sentinel.
/// Decimal nulls are stored as the MIN value of the underlying integer type,
/// which in big-endian is [0x80, 0x00, ...].
fn is_fixed_len_null_be(bytes: &[u8]) -> bool {
    !bytes.is_empty() && bytes[0] == 0x80 && bytes[1..].iter().all(|&b| b == 0x00)
}

fn compare_signed_be(a: &[u8], b: &[u8]) -> cmp::Ordering {
    debug_assert_eq!(a.len(), b.len());
    match (a[0] as i8).cmp(&(b[0] as i8)) {
        cmp::Ordering::Equal => a[1..].cmp(&b[1..]),
        other => other,
    }
}

/// Compare two big-endian signed integers of potentially different lengths.
fn compare_signed_be_varlen(a: &[u8], b: &[u8]) -> cmp::Ordering {
    let a_sign = (a.first().copied().unwrap_or(0) as i8).is_negative();
    let b_sign = (b.first().copied().unwrap_or(0) as i8).is_negative();

    if a_sign != b_sign {
        return if a_sign {
            cmp::Ordering::Less
        } else {
            cmp::Ordering::Greater
        };
    }

    // Compare with virtual sign-extension: the shorter operand is logically
    // prefixed with sign bytes (0xFF for negative, 0x00 for non-negative).
    let max_len = a.len().max(b.len());
    let a_pad = max_len - a.len();
    let b_pad = max_len - b.len();
    let a_fill: u8 = if a_sign { 0xFF } else { 0x00 };
    let b_fill: u8 = if b_sign { 0xFF } else { 0x00 };

    for i in 0..max_len {
        let a_byte = if i < a_pad { a_fill } else { a[i - a_pad] };
        let b_byte = if i < b_pad { b_fill } else { b[i - b_pad] };
        let ord = if i == 0 {
            (a_byte as i8).cmp(&(b_byte as i8))
        } else {
            a_byte.cmp(&b_byte)
        };
        if ord != cmp::Ordering::Equal {
            return ord;
        }
    }
    cmp::Ordering::Equal
}
