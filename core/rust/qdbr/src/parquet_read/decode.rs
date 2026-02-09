use crate::allocator::{AcVec, QdbAllocator};
use crate::parquet::error::{fmt_err, ParquetErrorExt, ParquetResult};
use crate::parquet::qdb_metadata::{QdbMetaCol, QdbMetaColFormat};
use crate::parquet::util::{align8b, ARRAY_NDIMS_LIMIT};
use crate::parquet_read::column_sink::fixed::{
    Day, DeltaBinaryPackedPrimitiveDecoder, FixedBooleanColumnSink, FixedDoubleColumnSink,
    FixedFloatColumnSink, FixedInt2ByteColumnSink, FixedInt2ShortColumnSink, FixedIntColumnSink,
    FixedLong128ColumnSink, FixedLong256ColumnSink, FixedLongColumnSink, IntDecimalColumnSink,
    Millis, NanoTimestampColumnSink, PlainPrimitiveDecoder, ReverseFixedColumnSink,
};
use crate::parquet_read::column_sink::var::ARRAY_AUX_SIZE;
use crate::parquet_read::column_sink::var::{
    BinaryColumnSink, RawArrayColumnSink, StringColumnSink, VarcharColumnSink,
};
use crate::parquet_read::column_sink::Pushable;
use crate::parquet_read::slicer::dict_decoder::{FixedDictDecoder, VarDictDecoder};
use crate::parquet_read::slicer::rle::{RleDictionarySlicer, RleLocalIsGlobalSymbolDecoder};
use crate::parquet_read::slicer::{
    BooleanBitmapSlicer, DataPageFixedSlicer, DataPageSlicer, DeltaBytesArraySlicer,
    DeltaLengthArraySlicer, PlainVarSlicer,
};
use crate::parquet_read::{
    ColumnChunkBuffers, ColumnChunkStats, DecodeContext, ParquetDecoder, RowGroupBuffers,
    RowGroupStatBuffers,
};
use crate::parquet_write::array::{
    append_array_null, append_array_nulls, calculate_array_shape, LevelsIterator,
};
use parquet2::deserialize::{HybridDecoderBitmapIter, HybridEncoded};
use parquet2::encoding::hybrid_rle::BitmapIter;
use parquet2::encoding::hybrid_rle::HybridRleDecoder;
use parquet2::encoding::{hybrid_rle, Encoding};
use parquet2::page::DataPageHeader;
use parquet2::page::{split_buffer, DataPage, DictPage};
use parquet2::read::levels::get_bit_width;
use parquet2::read::{SlicePageReader, SlicedDataPage, SlicedDictPage, SlicedPage};
use parquet2::schema::types::{PhysicalType, PrimitiveConvertedType, PrimitiveLogicalType};
use qdb_core::col_type::{ColumnType, ColumnTypeTag};
use std::cmp::min;
use std::ptr;
use std::slice;
use std::{cmp, i32};

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
}

impl RowGroupStatBuffers {
    pub fn new(allocator: QdbAllocator) -> Self {
        Self {
            column_chunk_stats_ptr: ptr::null_mut(),
            column_chunk_stats: AcVec::new_in(allocator),
        }
    }

    pub fn ensure_n_columns(&mut self, required_cols: usize) -> ParquetResult<()> {
        if self.column_chunk_stats.len() < required_cols {
            let allocator = self.column_chunk_stats.allocator().clone();
            self.column_chunk_stats
                .resize_with(required_cols, || ColumnChunkStats::new(allocator.clone()))?;
            self.column_chunk_stats_ptr = self.column_chunk_stats.as_mut_ptr();
        }
        Ok(())
    }
}

impl ColumnChunkBuffers {
    pub fn new(allocator: QdbAllocator) -> Self {
        Self {
            data_vec: AcVec::new_in(allocator.clone()),
            data_ptr: ptr::null_mut(),
            data_size: 0,
            aux_vec: AcVec::new_in(allocator),
            aux_ptr: ptr::null_mut(),
            aux_size: 0,
        }
    }

    pub fn refresh_ptrs(&mut self) {
        self.data_size = self.data_vec.len();
        self.data_ptr = self.data_vec.as_mut_ptr();

        self.aux_size = self.aux_vec.len();
        self.aux_ptr = self.aux_vec.as_mut_ptr();
    }
}

impl ColumnChunkStats {
    pub fn new(allocator: QdbAllocator) -> Self {
        Self {
            min_value_ptr: ptr::null_mut(),
            min_value_size: 0,
            min_value: AcVec::new_in(allocator.clone()),
            max_value_ptr: ptr::null_mut(),
            max_value_size: 0,
            max_value: AcVec::new_in(allocator),
        }
    }
}

const UUID_NULL: [u8; 16] = unsafe { std::mem::transmute([i64::MIN; 2]) };
const LONG256_NULL: [u8; 32] = unsafe { std::mem::transmute([i64::MIN; 4]) };
const BYTE_NULL: [u8; 1] = [0u8];
const INT_NULL: [u8; 4] = i32::MIN.to_le_bytes();
const SHORT_NULL: [u8; 2] = 0i16.to_le_bytes();
const SYMBOL_NULL: [u8; 4] = i32::MIN.to_le_bytes();
const LONG_NULL: [u8; 8] = i64::MIN.to_le_bytes();
const DOUBLE_NULL: [u8; 8] = unsafe { std::mem::transmute([f64::NAN]) };
const FLOAT_NULL: [u8; 4] = unsafe { std::mem::transmute([f32::NAN]) };
const TIMESTAMP_96_EMPTY: [u8; 12] = [0; 12];

/// The local positional index as it is stored in parquet.
/// Not to be confused with the field_id in the parquet metadata.
pub type ParquetColumnIndex = i32;

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
        if row_group_index > self.row_group_count {
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
                column_chunk_bufs.data_vec.clear();
                column_chunk_bufs.data_size = 0;
                column_chunk_bufs.data_ptr = ptr::null_mut();
                column_chunk_bufs.aux_vec.clear();
                column_chunk_bufs.aux_size = 0;
                column_chunk_bufs.aux_ptr = ptr::null_mut();
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
        if row_group_index > self.row_group_count {
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
                column_chunk_bufs.data_vec.clear();
                column_chunk_bufs.data_size = 0;
                column_chunk_bufs.data_ptr = ptr::null_mut();
                column_chunk_bufs.aux_vec.clear();
                column_chunk_bufs.aux_size = 0;
                column_chunk_bufs.aux_ptr = ptr::null_mut();
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
                column_chunk_bufs.data_vec.clear();
                column_chunk_bufs.data_size = 0;
                column_chunk_bufs.data_ptr = ptr::null_mut();
                column_chunk_bufs.aux_vec.clear();
                column_chunk_bufs.aux_size = 0;
                column_chunk_bufs.aux_ptr = ptr::null_mut();
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

        column_chunk_bufs.aux_vec.clear();
        column_chunk_bufs.data_vec.clear();

        for maybe_page in page_reader {
            let sliced_page = maybe_page?;

            match sliced_page {
                SlicedPage::Dict(dict_page) => {
                    let page = decompress_sliced_dict(dict_page, &mut ctx.decompress_buffer)?;
                    dict = Some(page);
                }
                SlicedPage::Data(data_page) => {
                    let page_row_count_opt =
                        sliced_page_row_count(&data_page, col_info.column_type);

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
                            let mut page =
                                decompress_sliced_data(&data_page, &mut ctx.decompress_buffer)?;
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
                            ctx.decompress_buffer = std::mem::take(page.buffer_mut());
                        } else if page_filter_start < filter_idx {
                            let mut page =
                                decompress_sliced_data(&data_page, &mut ctx.decompress_buffer)?;
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
                            ctx.decompress_buffer = std::mem::take(page.buffer_mut());
                        }
                        page_row_start = page_end;
                    } else {
                        if page_row_start >= row_group_hi {
                            break;
                        }

                        let mut page =
                            decompress_sliced_data(&data_page, &mut ctx.decompress_buffer)?;
                        let page_row_count = page_row_count(&page, col_info.column_type)?;
                        let page_end = page_row_start + page_row_count;

                        if page_end <= row_group_lo {
                            ctx.decompress_buffer = std::mem::take(page.buffer_mut());
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
                        ctx.decompress_buffer = std::mem::take(page.buffer_mut());
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

        let buf = unsafe { slice::from_raw_parts(ctx.file_ptr, ctx.file_size as usize) };
        let page_reader = SlicePageReader::new(buf, column_metadata, chunk_size)?;

        match self.metadata.version {
            1 | 2 => Ok(()),
            ver => Err(fmt_err!(Unsupported, "unsupported parquet version: {ver}")),
        }?;

        let mut dict = None;
        let mut row_count = 0usize;
        column_chunk_bufs.aux_vec.clear();
        column_chunk_bufs.data_vec.clear();
        for maybe_page in page_reader {
            let sliced_page = maybe_page?;

            match sliced_page {
                SlicedPage::Dict(dict_page) => {
                    let page = decompress_sliced_dict(dict_page, &mut ctx.decompress_buffer)?;
                    dict = Some(page);
                }
                SlicedPage::Data(data_page) => {
                    let page_row_count_opt =
                        sliced_page_row_count(&data_page, col_info.column_type);

                    if let Some(page_row_count) = page_row_count_opt {
                        if row_group_lo < row_count + page_row_count && row_group_hi > row_count {
                            let mut page =
                                decompress_sliced_data(&data_page, &mut ctx.decompress_buffer)?;
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
                            ctx.decompress_buffer = std::mem::take(page.buffer_mut());
                        }
                        row_count += page_row_count;
                    } else {
                        let mut page =
                            decompress_sliced_data(&data_page, &mut ctx.decompress_buffer)?;
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
                        ctx.decompress_buffer = std::mem::take(page.buffer_mut());
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

    pub fn find_row_group_by_timestamp(
        &self,
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

        let timestamp_column_index = timestamp_column_index as usize;
        let column_type = self.columns[timestamp_column_index]
            .column_type
            .ok_or_else(|| {
                fmt_err!(
                    InvalidType,
                    "unknown timestamp column type, column index: {}",
                    timestamp_column_index
                )
            })?;
        if column_type.tag() != ColumnTypeTag::Timestamp {
            return Err(fmt_err!(
                InvalidType,
                "expected timestamp column, but got {}, column index: {}",
                column_type,
                timestamp_column_index
            ));
        }

        let row_group_count = self.row_group_count;
        let mut row_count = 0usize;
        for (row_group_idx, row_group_meta) in self.metadata.row_groups.iter().enumerate() {
            let columns_meta = row_group_meta.columns();
            let column_metadata = &columns_meta[timestamp_column_index];
            let column_chunk = column_metadata.column_chunk();
            let column_chunk_meta = column_chunk.meta_data.as_ref().ok_or_else(|| {
                fmt_err!(
                    InvalidType,
                    "metadata not found for timestamp column, column index: {}",
                    timestamp_column_index
                )
            })?;

            let column_chunk_size = column_chunk_meta.num_values as usize;
            if row_hi + 1 < row_count {
                break;
            }
            if row_lo < row_count + column_chunk_size {
                let column_chunk_stats =
                    column_chunk_meta.statistics.as_ref().ok_or_else(|| {
                        fmt_err!(
                            InvalidLayout,
                            "statistics not found for timestamp column, column index: {}",
                            timestamp_column_index
                        )
                    })?;

                let min_value = long_stat_value(&column_chunk_stats.min_value)?;
                let max_value = long_stat_value(&column_chunk_stats.max_value)?;

                // Our overall scan direction is Vect#BIN_SEARCH_SCAN_DOWN (increasing
                // scan direction) and we're iterating over row groups left-to-right,
                // so as soon as we find the matching timestamp, we're done.
                //
                // The returned value includes the row group index shifted by +1,
                // as well as a flag to tell the caller that the timestamp is at the
                // right boundary of a row group or in a gap between two row groups
                // and, thus, row group decoding is not needed.

                // Check if we're at the left boundary or within the row group.
                if timestamp >= min_value && timestamp < max_value {
                    // We'll have to decode the group and search in it (even value).
                    return Ok(2 * (row_group_idx + 1) as u64);
                }
                // The value is to the left of the row group.
                // It must be either the right boundary of the previous row group
                // or a gap between the previous and the current row groups.
                if timestamp < min_value {
                    // We don't need to decode the row group (odd value).
                    return Ok((2 * row_group_idx + 1) as u64);
                }
            }
            row_count += column_chunk_size;
        }

        // The value is to the right of the last row group, no need to decode (odd value).
        Ok((2 * row_group_count + 1) as u64)
    }
}

/// Decode a filtered data page.
/// - `FILL_NULLS = false`: skip rows not in filter
/// - `FILL_NULLS = true`: fill nulls for rows not in filter
#[allow(clippy::too_many_arguments)]
pub fn decode_page_filtered<const FILL_NULLS: bool>(
    page: &DataPage,
    dict: Option<&DictPage>,
    bufs: &mut ColumnChunkBuffers,
    col_info: QdbMetaCol,
    page_row_start: usize,
    page_row_count: usize,
    row_group_lo: usize,
    row_lo: usize,
    row_hi: usize,
    rows_filter: &[i64],
) -> ParquetResult<()> {
    if !FILL_NULLS && rows_filter.is_empty() {
        return Ok(());
    }

    let (_rep_levels, _, values_buffer) = split_buffer(page)?;
    let column_type = col_info.column_type;

    let encoding_error = true;
    let decoding_result = match (
        page.descriptor.primitive_type.physical_type,
        page.descriptor.primitive_type.logical_type,
        page.descriptor.primitive_type.converted_type,
    ) {
        (PhysicalType::Int32, logical_type, converted_type) => {
            match (page.encoding(), dict, logical_type, column_type.tag()) {
                (
                    Encoding::Plain,
                    _,
                    _,
                    ColumnTypeTag::Short | ColumnTypeTag::Char | ColumnTypeTag::GeoShort,
                ) => {
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut PlainPrimitiveDecoder::<i32, i16>::new(values_buffer, bufs, 0),
                    )?;
                    Ok(())
                }
                (
                    Encoding::DeltaBinaryPacked,
                    _,
                    _,
                    ColumnTypeTag::Short | ColumnTypeTag::Char | ColumnTypeTag::GeoShort,
                ) => {
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut DeltaBinaryPackedPrimitiveDecoder::<i16>::try_new(
                            values_buffer,
                            bufs,
                            0,
                        )?,
                    )?;
                    Ok(())
                }
                (Encoding::Plain, _, _, ColumnTypeTag::Byte | ColumnTypeTag::GeoByte) => {
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut PlainPrimitiveDecoder::<i32, i8>::new(values_buffer, bufs, 0),
                    )?;
                    Ok(())
                }
                (
                    Encoding::DeltaBinaryPacked,
                    _,
                    _,
                    ColumnTypeTag::Byte | ColumnTypeTag::GeoByte,
                ) => {
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut DeltaBinaryPackedPrimitiveDecoder::<i8>::try_new(
                            values_buffer,
                            bufs,
                            0,
                        )?,
                    )?;
                    Ok(())
                }
                (
                    Encoding::Plain,
                    _,
                    _,
                    ColumnTypeTag::Int | ColumnTypeTag::GeoInt | ColumnTypeTag::IPv4,
                ) => {
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut PlainPrimitiveDecoder::<i32, i32>::new(values_buffer, bufs, i32::MIN),
                    )?;
                    Ok(())
                }
                (Encoding::Plain, _, _, ColumnTypeTag::Date) => {
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut PlainPrimitiveDecoder::<Day, Millis>::new(
                            values_buffer,
                            bufs,
                            Millis::NULL_VALUE,
                        ),
                    )?;
                    Ok(())
                }
                (
                    Encoding::DeltaBinaryPacked,
                    _,
                    _,
                    ColumnTypeTag::Int | ColumnTypeTag::GeoInt | ColumnTypeTag::IPv4,
                ) => {
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut DeltaBinaryPackedPrimitiveDecoder::<i32>::try_new(
                            values_buffer,
                            bufs,
                            i32::MIN,
                        )?,
                    )?;
                    Ok(())
                }
                (
                    Encoding::RleDictionary | Encoding::PlainDictionary,
                    Some(dict_page),
                    _,
                    ColumnTypeTag::Int | ColumnTypeTag::GeoInt | ColumnTypeTag::IPv4,
                ) => {
                    let dict_decoder = FixedDictDecoder::<4>::try_new(dict_page)?;
                    let mut slicer = RleDictionarySlicer::try_new(
                        values_buffer,
                        dict_decoder,
                        page_row_count,
                        page_row_count,
                        &INT_NULL,
                    )?;
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut FixedIntColumnSink::new(&mut slicer, bufs, &INT_NULL),
                    )?;
                    Ok(())
                }
                (encoding, dict, logical_type, ColumnTypeTag::Double) => {
                    let scale = match logical_type {
                        Some(PrimitiveLogicalType::Decimal(_, scale)) => scale,
                        _ => match converted_type {
                            Some(PrimitiveConvertedType::Decimal(_, scale)) => scale,
                            _ => 0,
                        },
                    };

                    match (encoding, dict) {
                        (Encoding::RleDictionary | Encoding::PlainDictionary, Some(dict_page)) => {
                            let dict_decoder = FixedDictDecoder::<4>::try_new(dict_page)?;
                            let mut slicer = RleDictionarySlicer::try_new(
                                values_buffer,
                                dict_decoder,
                                page_row_count,
                                page_row_count,
                                &INT_NULL,
                            )?;
                            decode_page0_filtered::<_, FILL_NULLS>(
                                page,
                                page_row_start,
                                page_row_count,
                                row_group_lo,
                                row_lo,
                                row_hi,
                                rows_filter,
                                &mut IntDecimalColumnSink::new(
                                    &mut slicer,
                                    bufs,
                                    &DOUBLE_NULL,
                                    scale as i32,
                                ),
                            )?;
                            Ok(())
                        }
                        (Encoding::Plain, _) => {
                            decode_page0_filtered::<_, FILL_NULLS>(
                                page,
                                page_row_start,
                                page_row_count,
                                row_group_lo,
                                row_lo,
                                row_hi,
                                rows_filter,
                                &mut IntDecimalColumnSink::new(
                                    &mut DataPageFixedSlicer::<4>::new(
                                        values_buffer,
                                        page_row_count,
                                    ),
                                    bufs,
                                    &DOUBLE_NULL,
                                    scale as i32,
                                ),
                            )?;
                            Ok(())
                        }
                        _ => Err(encoding_error),
                    }
                }
                (
                    Encoding::RleDictionary | Encoding::PlainDictionary,
                    Some(dict_page),
                    _,
                    ColumnTypeTag::Short | ColumnTypeTag::Char | ColumnTypeTag::GeoShort,
                ) => {
                    let dict_decoder = FixedDictDecoder::<4>::try_new(dict_page)?;
                    let mut slicer = RleDictionarySlicer::try_new(
                        values_buffer,
                        dict_decoder,
                        page_row_count,
                        page_row_count,
                        &INT_NULL,
                    )?;
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut FixedInt2ShortColumnSink::new(&mut slicer, bufs, &SHORT_NULL),
                    )?;
                    Ok(())
                }
                (
                    Encoding::RleDictionary | Encoding::PlainDictionary,
                    Some(dict_page),
                    _,
                    ColumnTypeTag::Byte,
                ) => {
                    let dict_decoder = FixedDictDecoder::<4>::try_new(dict_page)?;
                    let mut slicer = RleDictionarySlicer::try_new(
                        values_buffer,
                        dict_decoder,
                        page_row_count,
                        page_row_count,
                        &INT_NULL,
                    )?;
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut FixedInt2ByteColumnSink::new(&mut slicer, bufs, &BYTE_NULL),
                    )?;
                    Ok(())
                }
                _ => Err(encoding_error),
            }
        }
        (PhysicalType::Int64, logical_type, _) => {
            match (page.encoding(), dict, logical_type, column_type.tag()) {
                (
                    Encoding::Plain,
                    _,
                    _,
                    ColumnTypeTag::Long
                    | ColumnTypeTag::Date
                    | ColumnTypeTag::GeoLong
                    | ColumnTypeTag::Timestamp,
                ) => {
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut PlainPrimitiveDecoder::<i64, i64>::new(values_buffer, bufs, i64::MIN),
                    )?;
                    Ok(())
                }
                (
                    Encoding::DeltaBinaryPacked,
                    _,
                    _,
                    ColumnTypeTag::Long
                    | ColumnTypeTag::Timestamp
                    | ColumnTypeTag::Date
                    | ColumnTypeTag::GeoLong,
                ) => {
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut DeltaBinaryPackedPrimitiveDecoder::<i64>::try_new(
                            values_buffer,
                            bufs,
                            i64::MIN,
                        )?,
                    )?;
                    Ok(())
                }
                (
                    Encoding::RleDictionary | Encoding::PlainDictionary,
                    Some(dict_page),
                    _,
                    ColumnTypeTag::Long
                    | ColumnTypeTag::Timestamp
                    | ColumnTypeTag::Date
                    | ColumnTypeTag::GeoLong,
                ) => {
                    let dict_decoder = FixedDictDecoder::<8>::try_new(dict_page)?;
                    let mut slicer = RleDictionarySlicer::try_new(
                        values_buffer,
                        dict_decoder,
                        page_row_count,
                        page_row_count,
                        &LONG_NULL,
                    )?;
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut FixedLongColumnSink::new(&mut slicer, bufs, &LONG_NULL),
                    )?;
                    Ok(())
                }
                _ => Err(encoding_error),
            }
        }
        (PhysicalType::FixedLenByteArray(16), Some(PrimitiveLogicalType::Uuid), _) => {
            match (page.encoding(), column_type.tag()) {
                (Encoding::Plain, ColumnTypeTag::Uuid) => {
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut ReverseFixedColumnSink::new(
                            &mut DataPageFixedSlicer::<16>::new(values_buffer, page_row_count),
                            bufs,
                            UUID_NULL,
                        ),
                    )?;
                    Ok(())
                }
                _ => Err(encoding_error),
            }
        }
        (PhysicalType::FixedLenByteArray(16), _logical_type, _) => {
            match (page.encoding(), column_type.tag()) {
                (Encoding::Plain, ColumnTypeTag::Long128) => {
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut FixedLong128ColumnSink::new(
                            &mut DataPageFixedSlicer::<16>::new(values_buffer, page_row_count),
                            bufs,
                            &UUID_NULL,
                        ),
                    )?;
                    Ok(())
                }
                _ => Err(encoding_error),
            }
        }
        (PhysicalType::FixedLenByteArray(32), _logical_type, _) => {
            match (page.encoding(), column_type.tag()) {
                (Encoding::Plain, ColumnTypeTag::Long256) => {
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut FixedLong256ColumnSink::new(
                            &mut DataPageFixedSlicer::<32>::new(values_buffer, page_row_count),
                            bufs,
                            &LONG256_NULL,
                        ),
                    )?;
                    Ok(())
                }
                _ => Err(encoding_error),
            }
        }
        (PhysicalType::ByteArray, Some(PrimitiveLogicalType::String), _)
        | (PhysicalType::ByteArray, _, Some(PrimitiveConvertedType::Utf8)) => {
            let encoding = page.encoding();
            match (encoding, dict, column_type.tag()) {
                (Encoding::DeltaLengthByteArray, _, ColumnTypeTag::String) => {
                    let mut slicer = DeltaLengthArraySlicer::try_new(
                        values_buffer,
                        page_row_count,
                        page_row_count,
                    )?;
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut StringColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(())
                }
                (Encoding::DeltaLengthByteArray, _, ColumnTypeTag::Varchar) => {
                    let mut slicer = DeltaLengthArraySlicer::try_new(
                        values_buffer,
                        page_row_count,
                        page_row_count,
                    )?;
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut VarcharColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(())
                }
                (
                    Encoding::RleDictionary | Encoding::PlainDictionary,
                    Some(dict_page),
                    ColumnTypeTag::Varchar,
                ) => {
                    let dict_decoder = VarDictDecoder::try_new(dict_page, true)?;
                    let mut slicer = RleDictionarySlicer::try_new(
                        values_buffer,
                        dict_decoder,
                        page_row_count,
                        page_row_count,
                        &LONG256_NULL,
                    )?;
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut VarcharColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(())
                }
                (Encoding::Plain, _, ColumnTypeTag::String) => {
                    let mut slicer = PlainVarSlicer::new(values_buffer, page_row_count);
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut StringColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(())
                }
                (Encoding::Plain, _, ColumnTypeTag::Varchar) => {
                    let mut slicer = PlainVarSlicer::new(values_buffer, page_row_count);
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut VarcharColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(())
                }
                (Encoding::DeltaByteArray, _, ColumnTypeTag::Varchar) => {
                    let mut slicer = DeltaBytesArraySlicer::try_new(
                        values_buffer,
                        page_row_count,
                        page_row_count,
                    )?;
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut VarcharColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(())
                }
                (Encoding::RleDictionary, Some(_dict_page), ColumnTypeTag::Symbol) => {
                    if col_info.format != Some(QdbMetaColFormat::LocalKeyIsGlobal) {
                        return Err(fmt_err!(
                            Unsupported,
                            "only special LocalKeyIsGlobal-encoded symbol columns are supported",
                        ));
                    }
                    let mut slicer = RleLocalIsGlobalSymbolDecoder::try_new(
                        values_buffer,
                        page_row_count,
                        page_row_count,
                        &SYMBOL_NULL,
                    )?;
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut FixedIntColumnSink::new(&mut slicer, bufs, &SYMBOL_NULL),
                    )?;
                    Ok(())
                }
                _ => Err(encoding_error),
            }
        }
        (PhysicalType::ByteArray, _, _) => {
            let encoding = page.encoding();
            match (encoding, dict, column_type.tag()) {
                (Encoding::Plain, _, ColumnTypeTag::Binary) => {
                    let mut slicer = PlainVarSlicer::new(values_buffer, page_row_count);
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut BinaryColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(())
                }
                (Encoding::DeltaLengthByteArray, _, ColumnTypeTag::Binary) => {
                    let mut slicer = DeltaLengthArraySlicer::try_new(
                        values_buffer,
                        page_row_count,
                        page_row_count,
                    )?;
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut BinaryColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(())
                }
                (
                    Encoding::RleDictionary | Encoding::PlainDictionary,
                    Some(dict_page),
                    ColumnTypeTag::Binary,
                ) => {
                    let dict_decoder = VarDictDecoder::try_new(dict_page, false)?;
                    let mut slicer = RleDictionarySlicer::try_new(
                        values_buffer,
                        dict_decoder,
                        page_row_count,
                        page_row_count,
                        &[],
                    )?;
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut BinaryColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(())
                }
                (Encoding::Plain, _, ColumnTypeTag::Array) => {
                    // raw array encoding
                    let mut slicer = PlainVarSlicer::new(values_buffer, page_row_count);
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut RawArrayColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(())
                }
                (Encoding::DeltaLengthByteArray, _, ColumnTypeTag::Array) => {
                    let mut slicer = DeltaLengthArraySlicer::try_new(
                        values_buffer,
                        page_row_count,
                        page_row_count,
                    )?;
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut RawArrayColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(())
                }
                _ => Err(encoding_error),
            }
        }
        (PhysicalType::Int96, logical_type, _) => {
            // Int96 is used for nano timestamps
            match (page.encoding(), dict, logical_type, column_type.tag()) {
                (Encoding::Plain, _, _, ColumnTypeTag::Timestamp) => {
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut NanoTimestampColumnSink::new(
                            &mut DataPageFixedSlicer::<12>::new(values_buffer, page_row_count),
                            bufs,
                            &LONG_NULL,
                        ),
                    )?;
                    Ok(())
                }
                (
                    Encoding::PlainDictionary | Encoding::RleDictionary,
                    Some(dict_page),
                    _,
                    ColumnTypeTag::Timestamp,
                ) => {
                    let dict_decoder = FixedDictDecoder::<12>::try_new(dict_page)?;
                    let mut slicer = RleDictionarySlicer::try_new(
                        values_buffer,
                        dict_decoder,
                        page_row_count,
                        page_row_count,
                        &TIMESTAMP_96_EMPTY,
                    )?;
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut NanoTimestampColumnSink::new(&mut slicer, bufs, &LONG_NULL),
                    )?;
                    Ok(())
                }
                _ => Err(encoding_error),
            }
        }
        (PhysicalType::Double, _, _) => match (page.encoding(), dict, column_type.tag()) {
            (Encoding::Plain, _, ColumnTypeTag::Double) => {
                bufs.aux_vec.clear();
                bufs.aux_ptr = ptr::null_mut();

                decode_page0_filtered::<_, FILL_NULLS>(
                    page,
                    page_row_start,
                    page_row_count,
                    row_group_lo,
                    row_lo,
                    row_hi,
                    rows_filter,
                    &mut PlainPrimitiveDecoder::<f64, f64>::new(values_buffer, bufs, f64::NAN),
                )?;
                Ok(())
            }
            (
                Encoding::RleDictionary | Encoding::PlainDictionary,
                Some(dict_page),
                ColumnTypeTag::Double,
            ) => {
                bufs.aux_vec.clear();
                bufs.aux_ptr = ptr::null_mut();

                let dict_decoder = FixedDictDecoder::<8>::try_new(dict_page)?;
                let mut slicer = RleDictionarySlicer::try_new(
                    values_buffer,
                    dict_decoder,
                    page_row_count,
                    page_row_count,
                    &DOUBLE_NULL,
                )?;
                decode_page0_filtered::<_, FILL_NULLS>(
                    page,
                    page_row_start,
                    page_row_count,
                    row_group_lo,
                    row_lo,
                    row_hi,
                    rows_filter,
                    &mut FixedDoubleColumnSink::new(&mut slicer, bufs, &DOUBLE_NULL),
                )?;
                Ok(())
            }
            (Encoding::Plain, _, ColumnTypeTag::Array) => {
                let mut slicer = DataPageFixedSlicer::<8>::new(values_buffer, page_row_count);
                decode_array_page_filtered::<_, FILL_NULLS>(
                    page,
                    page_row_start,
                    page_row_count,
                    row_group_lo,
                    row_lo,
                    row_hi,
                    rows_filter,
                    &mut slicer,
                    bufs,
                )?;
                Ok(())
            }
            (
                Encoding::RleDictionary | Encoding::PlainDictionary,
                Some(dict_page),
                ColumnTypeTag::Array,
            ) => {
                let dict_decoder = FixedDictDecoder::<8>::try_new(dict_page)?;
                let mut slicer = RleDictionarySlicer::try_new(
                    values_buffer,
                    dict_decoder,
                    page_row_count,
                    page_row_count,
                    &DOUBLE_NULL,
                )?;
                decode_array_page_filtered::<_, FILL_NULLS>(
                    page,
                    page_row_start,
                    page_row_count,
                    row_group_lo,
                    row_lo,
                    row_hi,
                    rows_filter,
                    &mut slicer,
                    bufs,
                )?;
                Ok(())
            }
            _ => Err(encoding_error),
        },
        // check remaining fixed-size types
        (typ, _, _) => {
            bufs.aux_vec.clear();
            bufs.aux_ptr = ptr::null_mut();

            match (page.encoding(), dict, typ, column_type.tag()) {
                (
                    Encoding::RleDictionary | Encoding::PlainDictionary,
                    Some(dict_page),
                    PhysicalType::Float,
                    ColumnTypeTag::Float,
                ) => {
                    let dict_decoder = FixedDictDecoder::<4>::try_new(dict_page)?;
                    let mut slicer = RleDictionarySlicer::try_new(
                        values_buffer,
                        dict_decoder,
                        page_row_count,
                        page_row_count,
                        &FLOAT_NULL,
                    )?;
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut FixedFloatColumnSink::new(&mut slicer, bufs, &FLOAT_NULL),
                    )?;
                    Ok(())
                }
                (Encoding::Plain, _, PhysicalType::Float, ColumnTypeTag::Float) => {
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut PlainPrimitiveDecoder::<f32, f32>::new(values_buffer, bufs, f32::NAN),
                    )?;
                    Ok(())
                }
                (Encoding::Plain, _, PhysicalType::Boolean, ColumnTypeTag::Boolean) => {
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut FixedBooleanColumnSink::new(
                            &mut BooleanBitmapSlicer::new(
                                values_buffer,
                                page_row_count,
                                page_row_count,
                            ),
                            bufs,
                            &[0],
                        ),
                    )?;
                    Ok(())
                }
                (Encoding::Rle, _, PhysicalType::Boolean, ColumnTypeTag::Boolean) => {
                    decode_page0_filtered::<_, FILL_NULLS>(
                        page,
                        page_row_start,
                        page_row_count,
                        row_group_lo,
                        row_lo,
                        row_hi,
                        rows_filter,
                        &mut FixedBooleanColumnSink::new(
                            &mut BooleanBitmapSlicer::new(
                                values_buffer,
                                page_row_count,
                                page_row_count,
                            ),
                            bufs,
                            &[0],
                        ),
                    )?;
                    Ok(())
                }
                _ => Err(encoding_error),
            }
        }
    };

    match decoding_result {
        Ok(()) => Ok(()),
        Err(_) => Err(fmt_err!(
            Unsupported,
            "encoding not supported for filtered decode, physical type: {:?}, \
                encoding {:?}, \
                logical type {:?}, \
                converted type: {:?}, \
                column type {:?}",
            page.descriptor.primitive_type.physical_type,
            page.encoding(),
            page.descriptor.primitive_type.logical_type,
            page.descriptor.primitive_type.converted_type,
            column_type,
        )),
    }
}

pub fn decode_page(
    page: &DataPage,
    dict: Option<&DictPage>,
    bufs: &mut ColumnChunkBuffers,
    col_info: QdbMetaCol,
    row_lo: usize,
    row_hi: usize,
) -> ParquetResult<()> {
    let (_rep_levels, _, values_buffer) = split_buffer(page)?;
    let column_type = col_info.column_type;
    let row_count = row_hi - row_lo;

    let encoding_error = true;
    let decoding_result = match (
        page.descriptor.primitive_type.physical_type,
        page.descriptor.primitive_type.logical_type,
        page.descriptor.primitive_type.converted_type,
    ) {
        (PhysicalType::Int32, logical_type, converted_type) => {
            match (page.encoding(), dict, logical_type, column_type.tag()) {
                (
                    Encoding::Plain,
                    _,
                    _,
                    ColumnTypeTag::Short | ColumnTypeTag::Char | ColumnTypeTag::GeoShort,
                ) => {
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut PlainPrimitiveDecoder::<i32, i16>::new(values_buffer, bufs, 0),
                    )?;
                    Ok(())
                }
                (
                    Encoding::DeltaBinaryPacked,
                    _,
                    _,
                    ColumnTypeTag::Short | ColumnTypeTag::Char | ColumnTypeTag::GeoShort,
                ) => {
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut DeltaBinaryPackedPrimitiveDecoder::<i16>::try_new(
                            values_buffer,
                            bufs,
                            0,
                        )?,
                    )?;
                    Ok(())
                }
                (Encoding::Plain, _, _, ColumnTypeTag::Byte | ColumnTypeTag::GeoByte) => {
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut PlainPrimitiveDecoder::<i32, i8>::new(values_buffer, bufs, 0),
                    )?;
                    Ok(())
                }
                (
                    Encoding::DeltaBinaryPacked,
                    _,
                    _,
                    ColumnTypeTag::Byte | ColumnTypeTag::GeoByte,
                ) => {
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut DeltaBinaryPackedPrimitiveDecoder::<i8>::try_new(
                            values_buffer,
                            bufs,
                            0,
                        )?,
                    )?;
                    Ok(())
                }
                (
                    Encoding::Plain,
                    _,
                    _,
                    ColumnTypeTag::Int | ColumnTypeTag::GeoInt | ColumnTypeTag::IPv4,
                ) => {
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut PlainPrimitiveDecoder::<i32, i32>::new(values_buffer, bufs, i32::MIN),
                    )?;
                    Ok(())
                }
                (Encoding::Plain, _, _, ColumnTypeTag::Date) => {
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut PlainPrimitiveDecoder::<Day, Millis>::new(
                            values_buffer,
                            bufs,
                            Millis::NULL_VALUE,
                        ),
                    )?;
                    Ok(())
                }
                (
                    Encoding::DeltaBinaryPacked,
                    _,
                    _,
                    ColumnTypeTag::Int | ColumnTypeTag::GeoInt | ColumnTypeTag::IPv4,
                ) => {
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut DeltaBinaryPackedPrimitiveDecoder::<i32>::try_new(
                            values_buffer,
                            bufs,
                            i32::MIN,
                        )?,
                    )?;
                    Ok(())
                }
                (
                    Encoding::RleDictionary | Encoding::PlainDictionary,
                    Some(dict_page),
                    _,
                    ColumnTypeTag::Int | ColumnTypeTag::GeoInt | ColumnTypeTag::IPv4,
                ) => {
                    let dict_decoder = FixedDictDecoder::<4>::try_new(dict_page)?;
                    let mut slicer = RleDictionarySlicer::try_new(
                        values_buffer,
                        dict_decoder,
                        row_hi,
                        row_count,
                        &INT_NULL,
                    )?;
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut FixedIntColumnSink::new(&mut slicer, bufs, &INT_NULL),
                    )?;
                    Ok(())
                }
                (encoding, dict, logical_type, ColumnTypeTag::Double) => {
                    let scale = match logical_type {
                        Some(PrimitiveLogicalType::Decimal(_, scale)) => scale,
                        _ => match converted_type {
                            Some(PrimitiveConvertedType::Decimal(_, scale)) => scale,
                            _ => 0,
                        },
                    };

                    match (encoding, dict) {
                        (Encoding::RleDictionary | Encoding::PlainDictionary, Some(dict_page)) => {
                            let dict_decoder = FixedDictDecoder::<4>::try_new(dict_page)?;
                            let mut slicer = RleDictionarySlicer::try_new(
                                values_buffer,
                                dict_decoder,
                                row_hi,
                                row_count,
                                &INT_NULL,
                            )?;
                            decode_page0(
                                page,
                                row_lo,
                                row_hi,
                                &mut IntDecimalColumnSink::new(
                                    &mut slicer,
                                    bufs,
                                    &DOUBLE_NULL,
                                    scale as i32,
                                ),
                            )?;
                            Ok(())
                        }
                        (Encoding::Plain, _) => {
                            decode_page0(
                                page,
                                row_lo,
                                row_hi,
                                &mut IntDecimalColumnSink::new(
                                    &mut DataPageFixedSlicer::<4>::new(values_buffer, row_count),
                                    bufs,
                                    &DOUBLE_NULL,
                                    scale as i32,
                                ),
                            )?;
                            Ok(())
                        }
                        _ => Err(encoding_error),
                    }
                }
                (
                    Encoding::RleDictionary | Encoding::PlainDictionary,
                    Some(dict_page),
                    _,
                    ColumnTypeTag::Short | ColumnTypeTag::Char | ColumnTypeTag::GeoShort,
                ) => {
                    let dict_decoder = FixedDictDecoder::<4>::try_new(dict_page)?;
                    let mut slicer = RleDictionarySlicer::try_new(
                        values_buffer,
                        dict_decoder,
                        row_hi,
                        row_count,
                        &INT_NULL,
                    )?;
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut FixedInt2ShortColumnSink::new(&mut slicer, bufs, &SHORT_NULL),
                    )?;
                    Ok(())
                }
                (
                    Encoding::RleDictionary | Encoding::PlainDictionary,
                    Some(dict_page),
                    _,
                    ColumnTypeTag::Byte,
                ) => {
                    let dict_decoder = FixedDictDecoder::<4>::try_new(dict_page)?;
                    let mut slicer = RleDictionarySlicer::try_new(
                        values_buffer,
                        dict_decoder,
                        row_hi,
                        row_count,
                        &INT_NULL,
                    )?;
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut FixedInt2ByteColumnSink::new(&mut slicer, bufs, &BYTE_NULL),
                    )?;
                    Ok(())
                }
                _ => Err(encoding_error),
            }
        }
        (PhysicalType::Int64, logical_type, _) => {
            match (page.encoding(), dict, logical_type, column_type.tag()) {
                (
                    Encoding::Plain,
                    _,
                    _,
                    ColumnTypeTag::Long
                    | ColumnTypeTag::Date
                    | ColumnTypeTag::GeoLong
                    | ColumnTypeTag::Timestamp,
                ) => {
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut PlainPrimitiveDecoder::<i64, i64>::new(values_buffer, bufs, i64::MIN),
                    )?;
                    Ok(())
                }
                (
                    Encoding::DeltaBinaryPacked,
                    _,
                    _,
                    ColumnTypeTag::Long
                    | ColumnTypeTag::Timestamp
                    | ColumnTypeTag::Date
                    | ColumnTypeTag::GeoLong,
                ) => {
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut DeltaBinaryPackedPrimitiveDecoder::<i64>::try_new(
                            values_buffer,
                            bufs,
                            i64::MIN,
                        )?,
                    )?;
                    Ok(())
                }
                (
                    Encoding::RleDictionary | Encoding::PlainDictionary,
                    Some(dict_page),
                    _,
                    ColumnTypeTag::Long
                    | ColumnTypeTag::Timestamp
                    | ColumnTypeTag::Date
                    | ColumnTypeTag::GeoLong,
                ) => {
                    let dict_decoder = FixedDictDecoder::<8>::try_new(dict_page)?;
                    let mut slicer = RleDictionarySlicer::try_new(
                        values_buffer,
                        dict_decoder,
                        row_hi,
                        row_count,
                        &LONG_NULL,
                    )?;
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut FixedLongColumnSink::new(&mut slicer, bufs, &LONG_NULL),
                    )?;
                    Ok(())
                }
                _ => Err(encoding_error),
            }
        }
        (PhysicalType::FixedLenByteArray(16), Some(PrimitiveLogicalType::Uuid), _) => {
            match (page.encoding(), column_type.tag()) {
                (Encoding::Plain, ColumnTypeTag::Uuid) => {
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut ReverseFixedColumnSink::new(
                            &mut DataPageFixedSlicer::<16>::new(values_buffer, row_count),
                            bufs,
                            UUID_NULL,
                        ),
                    )?;
                    Ok(())
                }
                _ => Err(encoding_error),
            }
        }
        (PhysicalType::FixedLenByteArray(16), _logical_type, _) => {
            match (page.encoding(), column_type.tag()) {
                (Encoding::Plain, ColumnTypeTag::Long128) => {
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut PlainPrimitiveDecoder::<i128, i128>::new(values_buffer, bufs, 0),
                    )?;
                    Ok(())
                }
                _ => Err(encoding_error),
            }
        }
        (PhysicalType::FixedLenByteArray(32), _logical_type, _) => {
            match (page.encoding(), column_type.tag()) {
                (Encoding::Plain, ColumnTypeTag::Long256) => {
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut FixedLong256ColumnSink::new(
                            &mut DataPageFixedSlicer::<32>::new(values_buffer, row_count),
                            bufs,
                            &LONG256_NULL,
                        ),
                    )?;
                    Ok(())
                }
                _ => Err(encoding_error),
            }
        }
        (PhysicalType::ByteArray, Some(PrimitiveLogicalType::String), _)
        | (PhysicalType::ByteArray, _, Some(PrimitiveConvertedType::Utf8)) => {
            let encoding = page.encoding();
            match (encoding, dict, column_type.tag()) {
                (Encoding::DeltaLengthByteArray, _, ColumnTypeTag::String) => {
                    let mut slicer =
                        DeltaLengthArraySlicer::try_new(values_buffer, row_hi, row_count)?;
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut StringColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(())
                }
                (Encoding::DeltaLengthByteArray, _, ColumnTypeTag::Varchar) => {
                    let mut slicer =
                        DeltaLengthArraySlicer::try_new(values_buffer, row_hi, row_count)?;
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut VarcharColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(())
                }
                (
                    Encoding::RleDictionary | Encoding::PlainDictionary,
                    Some(dict_page),
                    ColumnTypeTag::Varchar,
                ) => {
                    let dict_decoder = VarDictDecoder::try_new(dict_page, true)?;
                    let mut slicer = RleDictionarySlicer::try_new(
                        values_buffer,
                        dict_decoder,
                        row_hi,
                        row_count,
                        &LONG256_NULL,
                    )?;
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut VarcharColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(())
                }
                (Encoding::Plain, _, ColumnTypeTag::String) => {
                    let mut slicer = PlainVarSlicer::new(values_buffer, row_count);
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut StringColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(())
                }
                (Encoding::Plain, _, ColumnTypeTag::Varchar) => {
                    let mut slicer = PlainVarSlicer::new(values_buffer, row_count);
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut VarcharColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(())
                }
                (Encoding::DeltaByteArray, _, ColumnTypeTag::Varchar) => {
                    let mut slicer =
                        DeltaBytesArraySlicer::try_new(values_buffer, row_hi, row_count)?;
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut VarcharColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(())
                }
                (Encoding::RleDictionary, Some(_dict_page), ColumnTypeTag::Symbol) => {
                    if col_info.format != Some(QdbMetaColFormat::LocalKeyIsGlobal) {
                        return Err(fmt_err!(
                            Unsupported,
                            "only special LocalKeyIsGlobal-encoded symbol columns are supported",
                        ));
                    }
                    let mut slicer = RleLocalIsGlobalSymbolDecoder::try_new(
                        values_buffer,
                        row_hi,
                        row_count,
                        &SYMBOL_NULL,
                    )?;
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut FixedIntColumnSink::new(&mut slicer, bufs, &SYMBOL_NULL),
                    )?;
                    Ok(())
                }
                _ => Err(encoding_error),
            }
        }
        (PhysicalType::ByteArray, _, _) => {
            let encoding = page.encoding();
            match (encoding, dict, column_type.tag()) {
                (Encoding::Plain, _, ColumnTypeTag::Binary) => {
                    let mut slicer = PlainVarSlicer::new(values_buffer, row_count);
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut BinaryColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(())
                }
                (Encoding::DeltaLengthByteArray, _, ColumnTypeTag::Binary) => {
                    let mut slicer =
                        DeltaLengthArraySlicer::try_new(values_buffer, row_hi, row_count)?;
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut BinaryColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(())
                }
                (
                    Encoding::RleDictionary | Encoding::PlainDictionary,
                    Some(dict_page),
                    ColumnTypeTag::Binary,
                ) => {
                    let dict_decoder = VarDictDecoder::try_new(dict_page, false)?;
                    let mut slicer = RleDictionarySlicer::try_new(
                        values_buffer,
                        dict_decoder,
                        row_hi,
                        row_count,
                        &[],
                    )?;
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut BinaryColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(())
                }
                (Encoding::Plain, _, ColumnTypeTag::Array) => {
                    // raw array encoding
                    let mut slicer = PlainVarSlicer::new(values_buffer, row_count);
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut RawArrayColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(())
                }
                (Encoding::DeltaLengthByteArray, _, ColumnTypeTag::Array) => {
                    let mut slicer =
                        DeltaLengthArraySlicer::try_new(values_buffer, row_hi, row_count)?;
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut RawArrayColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(())
                }
                _ => Err(encoding_error),
            }
        }
        (PhysicalType::Int96, logical_type, _) => {
            // Int96 is used for nano timestamps
            match (page.encoding(), dict, logical_type, column_type.tag()) {
                (Encoding::Plain, _, _, ColumnTypeTag::Timestamp) => {
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut NanoTimestampColumnSink::new(
                            &mut DataPageFixedSlicer::<12>::new(values_buffer, row_count),
                            bufs,
                            &LONG_NULL,
                        ),
                    )?;
                    Ok(())
                }
                (
                    Encoding::PlainDictionary | Encoding::RleDictionary,
                    Some(dict_page),
                    _,
                    ColumnTypeTag::Timestamp,
                ) => {
                    let dict_decoder = FixedDictDecoder::<12>::try_new(dict_page)?;
                    let mut slicer = RleDictionarySlicer::try_new(
                        values_buffer,
                        dict_decoder,
                        row_hi,
                        row_count,
                        &TIMESTAMP_96_EMPTY,
                    )?;
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut NanoTimestampColumnSink::new(&mut slicer, bufs, &LONG_NULL),
                    )?;
                    Ok(())
                }
                _ => Err(encoding_error),
            }
        }
        (PhysicalType::Double, _, _) => match (page.encoding(), dict, column_type.tag()) {
            (Encoding::Plain, _, ColumnTypeTag::Double) => {
                bufs.aux_vec.clear();
                bufs.aux_ptr = ptr::null_mut();

                decode_page0(
                    page,
                    row_lo,
                    row_hi,
                    &mut PlainPrimitiveDecoder::<f64, f64>::new(values_buffer, bufs, f64::NAN),
                )?;
                Ok(())
            }
            (
                Encoding::RleDictionary | Encoding::PlainDictionary,
                Some(dict_page),
                ColumnTypeTag::Double,
            ) => {
                bufs.aux_vec.clear();
                bufs.aux_ptr = ptr::null_mut();

                let dict_decoder = FixedDictDecoder::<8>::try_new(dict_page)?;
                let mut slicer = RleDictionarySlicer::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    row_count,
                    &DOUBLE_NULL,
                )?;
                decode_page0(
                    page,
                    row_lo,
                    row_hi,
                    &mut FixedDoubleColumnSink::new(&mut slicer, bufs, &DOUBLE_NULL),
                )?;
                Ok(())
            }
            (Encoding::Plain, _, ColumnTypeTag::Array) => {
                let mut slicer = DataPageFixedSlicer::<8>::new(values_buffer, row_count);
                decode_array_page(page, row_lo, row_hi, &mut slicer, bufs)?;
                Ok(())
            }
            (
                Encoding::RleDictionary | Encoding::PlainDictionary,
                Some(dict_page),
                ColumnTypeTag::Array,
            ) => {
                let dict_decoder = FixedDictDecoder::<8>::try_new(dict_page)?;
                let mut slicer = RleDictionarySlicer::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    row_count,
                    &DOUBLE_NULL,
                )?;
                decode_array_page(page, row_lo, row_hi, &mut slicer, bufs)?;
                Ok(())
            }
            _ => Err(encoding_error),
        },
        // check remaining fixed-size types
        (typ, _, _) => {
            bufs.aux_vec.clear();
            bufs.aux_ptr = ptr::null_mut();

            match (page.encoding(), dict, typ, column_type.tag()) {
                (
                    Encoding::RleDictionary | Encoding::PlainDictionary,
                    Some(dict_page),
                    PhysicalType::Float,
                    ColumnTypeTag::Float,
                ) => {
                    let dict_decoder = FixedDictDecoder::<4>::try_new(dict_page)?;
                    let mut slicer = RleDictionarySlicer::try_new(
                        values_buffer,
                        dict_decoder,
                        row_hi,
                        row_count,
                        &FLOAT_NULL,
                    )?;
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut FixedFloatColumnSink::new(&mut slicer, bufs, &FLOAT_NULL),
                    )?;
                    Ok(())
                }
                (Encoding::Plain, _, PhysicalType::Float, ColumnTypeTag::Float) => {
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut PlainPrimitiveDecoder::<f32, f32>::new(values_buffer, bufs, f32::NAN),
                    )?;
                    Ok(())
                }
                (Encoding::Plain, _, PhysicalType::Boolean, ColumnTypeTag::Boolean) => {
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut FixedBooleanColumnSink::new(
                            &mut BooleanBitmapSlicer::new(values_buffer, row_hi, row_count),
                            bufs,
                            &[0],
                        ),
                    )?;
                    Ok(())
                }
                (Encoding::Rle, _, PhysicalType::Boolean, ColumnTypeTag::Boolean) => {
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut FixedBooleanColumnSink::new(
                            &mut BooleanBitmapSlicer::new(values_buffer, row_hi, row_count),
                            bufs,
                            &[0],
                        ),
                    )?;
                    Ok(())
                }
                _ => Err(encoding_error),
            }
        }
    };

    match decoding_result {
        Ok(row_count) => Ok(row_count),
        Err(_) => Err(fmt_err!(
            Unsupported,
            "encoding not supported, physical type: {:?}, \
                encoding {:?}, \
                logical type {:?}, \
                converted type: {:?}, \
                column type {:?}",
            page.descriptor.primitive_type.physical_type,
            page.encoding(),
            page.descriptor.primitive_type.logical_type,
            page.descriptor.primitive_type.converted_type,
            column_type,
        )),
    }
}

#[allow(clippy::while_let_on_iterator)]
fn decode_page0<T: Pushable>(
    page: &DataPage,
    row_lo: usize,
    row_hi: usize,
    sink: &mut T,
) -> ParquetResult<()> {
    sink.reserve(row_hi - row_lo)?;
    let iter = decode_null_bitmap(page, row_hi)?;
    if let Some(iter) = iter {
        let mut skip_count = row_lo;
        for run in iter {
            let run = run?;
            match run {
                HybridEncoded::Bitmap(values, length) => {
                    // consume `length` items
                    let mut iter = BitmapIter::new(values, 0, length);
                    // first, scan values to skip, if any
                    let local_skip_count = min(skip_count, length);
                    skip_count -= local_skip_count;
                    let mut to_skip = 0usize;
                    for _ in 0..local_skip_count {
                        if let Some(item) = iter.next() {
                            to_skip += item as usize;
                        }
                    }
                    sink.skip(to_skip);
                    // next, copy the remaining values, if any
                    // consecutive true/false values together
                    let mut consecutive_true = 0usize;
                    let mut consecutive_false = 0usize;
                    while let Some(item) = iter.next() {
                        if item {
                            if consecutive_false > 0 {
                                sink.push_nulls(consecutive_false)?;
                                consecutive_false = 0;
                            }
                            consecutive_true += 1;
                        } else {
                            if consecutive_true > 0 {
                                sink.push_slice(consecutive_true)?;
                                consecutive_true = 0;
                            }
                            consecutive_false += 1;
                        }
                    }

                    if consecutive_true > 0 {
                        sink.push_slice(consecutive_true)?;
                    }
                    if consecutive_false > 0 {
                        sink.push_nulls(consecutive_false)?;
                    }
                }
                HybridEncoded::Repeated(is_set, length) => {
                    let local_skip_count = min(skip_count, length);
                    let local_push_count = length - local_skip_count;
                    skip_count -= local_skip_count;
                    if is_set {
                        if local_skip_count > 0 {
                            sink.skip(local_skip_count);
                        }
                        if local_push_count > 0 {
                            sink.push_slice(local_push_count)?;
                        }
                    } else if local_push_count > 0 {
                        sink.push_nulls(local_push_count)?;
                    }
                }
            };
        }
    } else {
        sink.skip(row_lo);
        sink.push_slice(row_hi - row_lo)?;
    }
    sink.result()
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::while_let_on_iterator)]
fn decode_page0_filtered<T: Pushable, const FILL_NULLS: bool>(
    page: &DataPage,
    page_row_start: usize,
    page_row_count: usize,
    row_group_lo: usize,
    row_lo: usize,
    row_hi: usize,
    rows_filter: &[i64],
    sink: &mut T,
) -> ParquetResult<()> {
    if FILL_NULLS {
        let output_count = row_hi - row_lo;
        sink.reserve(output_count)?;

        if rows_filter.is_empty() {
            sink.push_nulls(output_count)?;
            return sink.result();
        }
    } else {
        if rows_filter.is_empty() {
            return Ok(());
        }
        sink.reserve(rows_filter.len())?;
    }

    let mut filter_idx = 0usize;
    let filter_len = rows_filter.len();
    let mut output_row = row_lo;

    let iter = decode_null_bitmap(page, page_row_count)?;
    if let Some(iter) = iter {
        let mut current_row = 0usize;

        for run in iter {
            let run = run?;
            match run {
                HybridEncoded::Bitmap(values, length) => {
                    let run_start_pos = page_row_start + current_row;
                    let run_end_in_page = current_row + length;

                    if FILL_NULLS {
                        if run_end_in_page <= row_lo {
                            sink.skip(count_ones_in_bitmap(values, 0, length));
                            current_row += length;
                            continue;
                        }
                        if current_row >= row_hi {
                            break;
                        }
                    } else {
                        if filter_idx >= filter_len {
                            return sink.result();
                        }
                        let run_end_pos = run_start_pos + length;
                        if (rows_filter[filter_idx] as usize + row_group_lo) >= run_end_pos {
                            sink.skip(count_ones_in_bitmap(values, 0, length));
                            current_row += length;
                            continue;
                        }
                    }

                    let mut bit_offset = if FILL_NULLS && current_row < row_lo {
                        let skip_bits = row_lo - current_row;
                        sink.skip(count_ones_in_bitmap(values, 0, skip_bits));
                        skip_bits
                    } else {
                        0usize
                    };

                    if FILL_NULLS {
                        while output_row < row_hi && (current_row + bit_offset) < run_end_in_page {
                            let abs_row = page_row_start + current_row + bit_offset;
                            let in_filter = filter_idx < filter_len
                                && (rows_filter[filter_idx] as usize + row_group_lo) == abs_row;

                            if in_filter {
                                if get_bit_at(values, bit_offset) {
                                    sink.push()?;
                                } else {
                                    sink.push_null()?;
                                }
                                filter_idx += 1;
                            } else {
                                if get_bit_at(values, bit_offset) {
                                    sink.skip(1);
                                }
                                sink.push_null()?;
                            }
                            bit_offset += 1;
                            output_row += 1;
                        }
                    } else {
                        while filter_idx < filter_len {
                            let target_offset =
                                (rows_filter[filter_idx] as usize + row_group_lo) - run_start_pos;
                            if target_offset >= length {
                                break;
                            }

                            if bit_offset < target_offset {
                                sink.skip(count_ones_in_bitmap(
                                    values,
                                    bit_offset,
                                    target_offset - bit_offset,
                                ));
                                bit_offset = target_offset;
                            }

                            if get_bit_at(values, bit_offset) {
                                sink.push()?;
                            } else {
                                sink.push_null()?;
                            }
                            filter_idx += 1;
                            bit_offset += 1;
                        }

                        if filter_idx >= filter_len {
                            return sink.result();
                        }
                    }

                    if bit_offset < length {
                        sink.skip(count_ones_in_bitmap(
                            values,
                            bit_offset,
                            length - bit_offset,
                        ));
                    }

                    current_row += length;
                }
                HybridEncoded::Repeated(is_set, length) => {
                    let run_start_pos = page_row_start + current_row;
                    let run_end_in_page = current_row + length;

                    if FILL_NULLS {
                        if run_end_in_page <= row_lo {
                            if is_set {
                                sink.skip(length);
                            }
                            current_row += length;
                            continue;
                        }
                        if current_row >= row_hi {
                            break;
                        }
                    } else {
                        if filter_idx >= filter_len {
                            return sink.result();
                        }
                        let run_end_pos = run_start_pos + length;
                        if (rows_filter[filter_idx] as usize + row_group_lo) >= run_end_pos {
                            if is_set {
                                sink.skip(length);
                            }
                            current_row += length;
                            continue;
                        }
                    }

                    let mut row_offset = if FILL_NULLS && current_row < row_lo {
                        let skip_rows = row_lo - current_row;
                        if is_set {
                            sink.skip(skip_rows);
                        }
                        skip_rows
                    } else {
                        0usize
                    };

                    if FILL_NULLS {
                        while output_row < row_hi && (current_row + row_offset) < run_end_in_page {
                            let abs_row = page_row_start + current_row + row_offset;
                            let in_filter = filter_idx < filter_len
                                && (rows_filter[filter_idx] as usize + row_group_lo) == abs_row;

                            if in_filter {
                                if is_set {
                                    sink.push()?;
                                } else {
                                    sink.push_null()?;
                                }
                                filter_idx += 1;
                            } else {
                                if is_set {
                                    sink.skip(1);
                                }
                                sink.push_null()?;
                            }
                            row_offset += 1;
                            output_row += 1;
                        }
                    } else {
                        while filter_idx < filter_len {
                            let target_offset =
                                (rows_filter[filter_idx] as usize + row_group_lo) - run_start_pos;

                            if target_offset >= length {
                                break;
                            }

                            if is_set && row_offset < target_offset {
                                sink.skip(target_offset - row_offset);
                            }
                            row_offset = target_offset;

                            if is_set {
                                sink.push()?;
                            } else {
                                sink.push_null()?;
                            }
                            filter_idx += 1;
                            row_offset += 1;
                        }

                        if filter_idx >= filter_len {
                            return sink.result();
                        }
                    }

                    if is_set && row_offset < length {
                        sink.skip(length - row_offset);
                    }

                    current_row += length;
                }
            };
        }
    } else {
        // No null bitmap - all values are non-null
        if FILL_NULLS {
            let mut page_row = row_lo;
            sink.skip(row_lo);

            while output_row < row_hi {
                let abs_row = page_row_start + page_row;
                let in_filter = filter_idx < filter_len
                    && (rows_filter[filter_idx] as usize + row_group_lo) == abs_row;

                if in_filter {
                    sink.push()?;
                    filter_idx += 1;
                } else {
                    sink.skip(1);
                    sink.push_null()?;
                }
                page_row += 1;
                output_row += 1;
            }

            if page_row < page_row_count {
                sink.skip(page_row_count - page_row);
            }
        } else {
            let mut i = 0usize;
            let mut prev_row_end = 0usize;

            while i < filter_len {
                let first_row = rows_filter[i] as usize + row_group_lo - page_row_start;
                if first_row >= page_row_count {
                    break;
                }

                let mut consecutive = 1usize;
                while i + consecutive < filter_len {
                    let curr = rows_filter[i + consecutive - 1] as usize;
                    let next = rows_filter[i + consecutive] as usize;
                    if next != curr + 1 {
                        break;
                    }
                    let next_row = next + row_group_lo - page_row_start;
                    if next_row >= page_row_count {
                        break;
                    }
                    consecutive += 1;
                }

                sink.skip(first_row - prev_row_end);
                sink.push_slice(consecutive)?;
                prev_row_end = first_row + consecutive;
                i += consecutive;
            }

            if prev_row_end < page_row_count {
                sink.skip(page_row_count - prev_row_end);
            }
        }
    }

    sink.result()
}

#[inline]
fn count_ones_in_bitmap(values: &[u8], offset: usize, length: usize) -> usize {
    let byte_idx = offset >> 3;
    let start_bit = offset & 7;

    match length {
        0 => 0,
        1 => {
            let byte = unsafe { *values.get_unchecked(byte_idx) };
            ((byte >> start_bit) & 1) as usize
        }
        2 => {
            let byte = unsafe { *values.get_unchecked(byte_idx) };
            if start_bit <= 6 {
                let two_bits = (byte >> start_bit) & 0b11;
                ((two_bits & 1) + (two_bits >> 1)) as usize
            } else {
                let first = (byte >> 7) & 1;
                let second = unsafe { *values.get_unchecked(byte_idx + 1) } & 1;
                (first + second) as usize
            }
        }
        3..=8 => {
            let byte = unsafe { *values.get_unchecked(byte_idx) };
            let end_bit = start_bit + length;
            if end_bit <= 8 {
                let mask = ((1u16 << length) - 1) << start_bit;
                (byte & mask as u8).count_ones() as usize
            } else {
                let first_mask = 0xFFu8 << start_bit;
                let mut count = (byte & first_mask).count_ones() as usize;
                let second_bits = length - (8 - start_bit);
                let second_mask = (1u8 << second_bits) - 1;
                count += (unsafe { *values.get_unchecked(byte_idx + 1) } & second_mask).count_ones()
                    as usize;
                count
            }
        }
        _ => {
            let mut count = 0usize;
            let mut bit_pos = offset;
            let end_pos = offset + length;

            // Handle unaligned start
            if start_bit != 0 {
                let bits_in_first_byte = 8 - start_bit;
                let mask = 0xFFu8 << start_bit;
                count += (unsafe { *values.get_unchecked(byte_idx) } & mask).count_ones() as usize;
                bit_pos += bits_in_first_byte;
            }

            // Handle full bytes
            let full_byte_start = bit_pos >> 3;
            let full_byte_end = end_pos >> 3;
            for &b in &values[full_byte_start..full_byte_end] {
                count += b.count_ones() as usize;
            }

            // Handle remaining bits
            let remaining = end_pos & 7;
            if remaining > 0 {
                let mask = (1u8 << remaining) - 1;
                count +=
                    (unsafe { *values.get_unchecked(full_byte_end) } & mask).count_ones() as usize;
            }

            count
        }
    }
}

#[inline]
fn get_bit_at(values: &[u8], bit_offset: usize) -> bool {
    let byte_idx = bit_offset >> 3;
    let bit_idx = bit_offset & 7;
    unsafe { (values.get_unchecked(byte_idx) >> bit_idx) & 1 == 1 }
}

fn decode_null_bitmap(
    page: &DataPage,
    count: usize,
) -> ParquetResult<Option<HybridDecoderBitmapIter<'_>>> {
    let nc = page.header().null_count();
    if nc == Some(0) {
        return Ok(None);
    }

    let def_levels = split_buffer(page)?.1;
    if def_levels.is_empty() {
        return Ok(None);
    }

    let iter = hybrid_rle::Decoder::new(def_levels, 1);
    let iter = HybridDecoderBitmapIter::new(iter, count);
    Ok(Some(iter))
}

#[allow(clippy::while_let_on_iterator)]
#[allow(clippy::too_many_arguments)]
fn decode_array_page_filtered<T: DataPageSlicer, const FILL_NULLS: bool>(
    page: &DataPage,
    page_row_start: usize,
    page_row_count: usize,
    row_group_lo: usize,
    row_lo: usize,
    row_hi: usize,
    rows_filter: &[i64],
    slicer: &mut T,
    buffers: &mut ColumnChunkBuffers,
) -> ParquetResult<()> {
    if FILL_NULLS {
        let output_count = row_hi - row_lo;
        buffers.aux_vec.reserve(output_count * ARRAY_AUX_SIZE)?;
        if rows_filter.is_empty() {
            append_array_nulls(&mut buffers.aux_vec, &buffers.data_vec, output_count)?;
            return Ok(());
        }
    } else {
        if rows_filter.is_empty() {
            return Ok(());
        }
        buffers
            .aux_vec
            .reserve(rows_filter.len() * ARRAY_AUX_SIZE)?;
    }

    let (rep_levels, def_levels, _) = split_buffer(page)?;

    let max_rep_level = page.descriptor.max_rep_level;
    let max_def_level = page.descriptor.max_def_level;

    if max_rep_level > ARRAY_NDIMS_LIMIT as i16 {
        return Err(fmt_err!(
            Unsupported,
            "too large number of array dimensions {max_rep_level}"
        ));
    }

    let mut levels_iter = LevelsIterator::try_new(
        page.num_values(),
        max_rep_level,
        max_def_level,
        rep_levels,
        def_levels,
    )?;

    match max_rep_level {
        1 => decode_array_filtered_loop::<T, FILL_NULLS, 1>(
            &mut levels_iter,
            max_rep_level,
            max_def_level,
            page_row_start,
            page_row_count,
            row_group_lo,
            row_lo,
            row_hi,
            rows_filter,
            slicer,
            buffers,
        ),
        2 => decode_array_filtered_loop::<T, FILL_NULLS, 2>(
            &mut levels_iter,
            max_rep_level,
            max_def_level,
            page_row_start,
            page_row_count,
            row_group_lo,
            row_lo,
            row_hi,
            rows_filter,
            slicer,
            buffers,
        ),
        _ => decode_array_filtered_loop::<T, FILL_NULLS, 0>(
            &mut levels_iter,
            max_rep_level,
            max_def_level,
            page_row_start,
            page_row_count,
            row_group_lo,
            row_lo,
            row_hi,
            rows_filter,
            slicer,
            buffers,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn decode_array_filtered_loop<T: DataPageSlicer, const FILL_NULLS: bool, const REP_LEVEL: i16>(
    levels_iter: &mut LevelsIterator,
    max_rep_level: i16,
    max_def_level: i16,
    page_row_start: usize,
    page_row_count: usize,
    row_group_lo: usize,
    row_lo: usize,
    row_hi: usize,
    rows_filter: &[i64],
    slicer: &mut T,
    buffers: &mut ColumnChunkBuffers,
) -> ParquetResult<()> {
    let mut current_row = 0usize;
    let mut filter_idx = 0usize;
    let filter_len = rows_filter.len();
    let mut def_scratch = Vec::new();

    if FILL_NULLS && row_lo > 0 {
        let non_null_skipped = levels_iter.skip_rows(row_lo, max_def_level as u32)?;
        slicer.skip(non_null_skipped);
        current_row = row_lo;
    }

    while current_row < page_row_count {
        if FILL_NULLS && current_row >= row_hi {
            break;
        }
        if !FILL_NULLS && filter_idx >= filter_len {
            break;
        }

        let row_pos = page_row_start + current_row;
        let in_filter =
            filter_idx < filter_len && (rows_filter[filter_idx] as usize + row_group_lo) == row_pos;

        if in_filter {
            let result = if REP_LEVEL == 1 {
                read_and_append_one_row_1d(
                    levels_iter,
                    max_def_level as u32,
                    slicer,
                    buffers,
                    &mut def_scratch,
                )?
            } else if REP_LEVEL == 2 {
                read_and_append_one_row_2d(
                    levels_iter,
                    max_def_level as u32,
                    slicer,
                    buffers,
                    &mut def_scratch,
                )?
            } else {
                read_and_append_one_row_generic(
                    levels_iter,
                    max_rep_level as u32,
                    max_def_level as u32,
                    slicer,
                    buffers,
                )?
            };
            let Some(first_vs) = result else {
                break;
            };

            filter_idx += 1;
            current_row += 1;
            if filter_idx == 1 && first_vs > 0 && filter_len > 1 {
                // estimate total size
                buffers.data_vec.reserve(first_vs * (filter_len - 1))?;
            }
        } else {
            let next_match_row = if filter_idx < filter_len {
                let abs_row = rows_filter[filter_idx] as usize + row_group_lo;
                abs_row.saturating_sub(page_row_start)
            } else {
                debug_assert!(FILL_NULLS);
                row_hi
            };
            let skip_count = next_match_row.saturating_sub(current_row);

            if skip_count > 0 {
                let non_null_skipped = levels_iter.skip_rows(skip_count, max_def_level as u32)?;
                slicer.skip(non_null_skipped);
                if FILL_NULLS {
                    append_array_nulls(&mut buffers.aux_vec, &buffers.data_vec, skip_count)?;
                }
                current_row += skip_count;
            }
        }
    }

    if FILL_NULLS && current_row < row_hi {
        let remaining = row_hi - current_row;
        append_array_nulls(&mut buffers.aux_vec, &buffers.data_vec, remaining)?;
    }

    Ok(())
}

fn decode_array_page<T: DataPageSlicer>(
    page: &DataPage,
    row_lo: usize,
    row_hi: usize,
    slicer: &mut T,
    buffers: &mut ColumnChunkBuffers,
) -> ParquetResult<()> {
    let (rep_levels, def_levels, _) = split_buffer(page)?;

    let max_rep_level = page.descriptor.max_rep_level;
    let max_def_level = page.descriptor.max_def_level;

    if max_rep_level > ARRAY_NDIMS_LIMIT as i16 {
        return Err(fmt_err!(
            Unsupported,
            "too large number of array dimensions {max_rep_level}"
        ));
    }

    buffers
        .aux_vec
        .reserve((row_hi - row_lo) * ARRAY_AUX_SIZE)?;

    let mut levels_iter = LevelsIterator::try_new(
        page.num_values(),
        max_rep_level,
        max_def_level,
        rep_levels,
        def_levels,
    )?;

    match max_rep_level {
        1 => decode_array_rows_1d(
            &mut levels_iter,
            max_def_level as u32,
            row_lo,
            row_hi,
            slicer,
            buffers,
        ),
        2 => decode_array_rows_2d(
            &mut levels_iter,
            max_def_level as u32,
            row_lo,
            row_hi,
            slicer,
            buffers,
        ),
        _ => decode_array_rows_generic(
            &mut levels_iter,
            max_rep_level as u32,
            max_def_level as u32,
            row_lo,
            row_hi,
            slicer,
            buffers,
        ),
    }
}

fn decode_array_rows_1d<T: DataPageSlicer>(
    levels_iter: &mut LevelsIterator,
    max_def_level: u32,
    row_lo: usize,
    row_hi: usize,
    slicer: &mut T,
    buffers: &mut ColumnChunkBuffers,
) -> ParquetResult<()> {
    if row_lo > 0 {
        let non_null_skipped = levels_iter.skip_rows(row_lo, max_def_level)?;
        slicer.skip(non_null_skipped);
    }

    let target_rows = row_hi - row_lo;
    if target_rows == 0 {
        return Ok(());
    }

    let mut def_scratch = Vec::new();

    let Some(first_vs) = read_and_append_one_row_1d(
        levels_iter,
        max_def_level,
        slicer,
        buffers,
        &mut def_scratch,
    )?
    else {
        return Ok(());
    };
    if target_rows > 1 && first_vs > 0 {
        // estimate total size
        buffers.data_vec.reserve(first_vs * (target_rows - 1))?;
    }
    for _ in 1..target_rows {
        if read_and_append_one_row_1d(
            levels_iter,
            max_def_level,
            slicer,
            buffers,
            &mut def_scratch,
        )?
        .is_none()
        {
            break;
        }
    }
    Ok(())
}

fn decode_array_rows_2d<T: DataPageSlicer>(
    levels_iter: &mut LevelsIterator,
    max_def_level: u32,
    row_lo: usize,
    row_hi: usize,
    slicer: &mut T,
    buffers: &mut ColumnChunkBuffers,
) -> ParquetResult<()> {
    if row_lo > 0 {
        let non_null_skipped = levels_iter.skip_rows(row_lo, max_def_level)?;
        slicer.skip(non_null_skipped);
    }

    let target_rows = row_hi - row_lo;
    if target_rows == 0 {
        return Ok(());
    }

    let mut def_scratch = Vec::new();

    let Some(first_vs) = read_and_append_one_row_2d(
        levels_iter,
        max_def_level,
        slicer,
        buffers,
        &mut def_scratch,
    )?
    else {
        return Ok(());
    };
    if target_rows > 1 && first_vs > 0 {
        // estimate total size
        buffers.data_vec.reserve(first_vs * (target_rows - 1))?;
    }
    for _ in 1..target_rows {
        if read_and_append_one_row_2d(
            levels_iter,
            max_def_level,
            slicer,
            buffers,
            &mut def_scratch,
        )?
        .is_none()
        {
            break;
        }
    }
    Ok(())
}

#[inline]
fn read_and_append_one_row_1d<T: DataPageSlicer>(
    levels_iter: &mut LevelsIterator,
    max_def_level: u32,
    slicer: &mut T,
    buffers: &mut ColumnChunkBuffers,
    def_scratch: &mut Vec<u32>,
) -> ParquetResult<Option<usize>> {
    let first_def = if levels_iter.has_lookahead() {
        let (_, def) = levels_iter.take_lookahead();
        Some(def)
    } else {
        match levels_iter.next_rep_def() {
            Some(Ok((_, def))) => Some(def),
            Some(Err(e)) => return Err(e),
            None => None,
        }
    };

    let Some(first_def) = first_def else {
        return Ok(None);
    };

    if first_def == 0 {
        append_array_null(&mut buffers.aux_vec, &buffers.data_vec)?;
        return Ok(Some(0));
    }

    let mut element_count = 1usize;
    let mut non_null_count: usize = if first_def == max_def_level { 1 } else { 0 };
    let mut has_nulls = first_def != max_def_level;

    def_scratch.clear();
    if has_nulls {
        def_scratch.push(first_def);
    }

    loop {
        match levels_iter.next_rep_def() {
            None => break,
            Some(Err(e)) => return Err(e),
            Some(Ok((rep, def))) => {
                if rep == 0 {
                    levels_iter.set_lookahead(rep, def);
                    break;
                }
                if def == max_def_level {
                    non_null_count += 1;
                } else if !has_nulls {
                    has_nulls = true;
                    def_scratch.reserve(element_count + 1);
                    def_scratch.resize(element_count, max_def_level);
                }
                if has_nulls {
                    def_scratch.push(def);
                }
                element_count += 1;
            }
        }
    }

    // 8 bytes shape ([element_count: u32, pad: u32]) + 8 bytes per f64 element.
    // Currently arrays only support f64 elements.
    let value_size = 8 + 8 * element_count;
    let data_start = buffers.data_vec.len();
    buffers.data_vec.reserve(value_size)?;

    buffers
        .aux_vec
        .extend_from_slice(&data_start.to_le_bytes())?;
    buffers
        .aux_vec
        .extend_from_slice(&value_size.to_le_bytes())?;

    buffers
        .data_vec
        .extend_from_slice(&(element_count as u32).to_le_bytes())?;
    buffers.data_vec.extend_from_slice(&[0u8; 4])?;

    if non_null_count == element_count {
        slicer.next_slice_into(element_count, &mut buffers.data_vec)?;
    } else {
        for &def in def_scratch.iter() {
            if def == max_def_level {
                slicer.next_into(&mut buffers.data_vec)?;
            } else {
                buffers
                    .data_vec
                    .extend_from_slice(&f64::NAN.to_le_bytes())?;
            }
        }
    }

    Ok(Some(value_size))
}

#[inline]
fn read_and_append_one_row_2d<T: DataPageSlicer>(
    levels_iter: &mut LevelsIterator,
    max_def_level: u32,
    slicer: &mut T,
    buffers: &mut ColumnChunkBuffers,
    def_scratch: &mut Vec<u32>,
) -> ParquetResult<Option<usize>> {
    let first_def = if levels_iter.has_lookahead() {
        let (_, def) = levels_iter.take_lookahead();
        Some(def)
    } else {
        match levels_iter.next_rep_def() {
            Some(Ok((_, def))) => Some(def),
            Some(Err(e)) => return Err(e),
            None => None,
        }
    };

    let Some(first_def) = first_def else {
        return Ok(None);
    };

    if first_def == 0 {
        append_array_null(&mut buffers.aux_vec, &buffers.data_vec)?;
        return Ok(Some(0));
    }

    let mut dim0 = 1u32;
    let mut cur_dim1 = 1u32;
    let mut max_dim1 = 0u32;
    let mut total_elements = 1usize;
    let mut non_null_count: usize = if first_def == max_def_level { 1 } else { 0 };
    let mut has_nulls = first_def != max_def_level;

    def_scratch.clear();
    if has_nulls {
        def_scratch.push(first_def);
    }

    loop {
        match levels_iter.next_rep_def() {
            None => break,
            Some(Err(e)) => return Err(e),
            Some(Ok((rep, def))) => {
                if rep == 0 {
                    levels_iter.set_lookahead(rep, def);
                    break;
                }
                if rep == 1 {
                    max_dim1 = max_dim1.max(cur_dim1);
                    cur_dim1 = 1;
                    dim0 += 1;
                } else {
                    cur_dim1 += 1;
                }
                if def == max_def_level {
                    non_null_count += 1;
                } else if !has_nulls {
                    has_nulls = true;
                    def_scratch.reserve(total_elements + 1);
                    def_scratch.resize(total_elements, max_def_level);
                }
                if has_nulls {
                    def_scratch.push(def);
                }
                total_elements += 1;
            }
        }
    }
    max_dim1 = max_dim1.max(cur_dim1);

    // 8 bytes shape ([dim0: u32, max_dim1: u32]) + 8 bytes per f64 element.
    // Currently arrays only support f64 elements.
    let value_size = 8 + 8 * total_elements;
    let data_start = buffers.data_vec.len();
    buffers.data_vec.reserve(value_size)?;

    buffers
        .aux_vec
        .extend_from_slice(&data_start.to_le_bytes())?;
    buffers
        .aux_vec
        .extend_from_slice(&value_size.to_le_bytes())?;

    buffers.data_vec.extend_from_slice(&dim0.to_le_bytes())?;
    buffers
        .data_vec
        .extend_from_slice(&max_dim1.to_le_bytes())?;

    if non_null_count == total_elements {
        slicer.next_slice_into(total_elements, &mut buffers.data_vec)?;
    } else {
        for &def in def_scratch.iter() {
            if def == max_def_level {
                slicer.next_into(&mut buffers.data_vec)?;
            } else {
                buffers
                    .data_vec
                    .extend_from_slice(&f64::NAN.to_le_bytes())?;
            }
        }
    }

    Ok(Some(value_size))
}

#[inline]
fn read_and_append_one_row_generic<T: DataPageSlicer>(
    levels_iter: &mut LevelsIterator,
    max_rep_level: u32,
    max_def_level: u32,
    slicer: &mut T,
    buffers: &mut ColumnChunkBuffers,
) -> ParquetResult<Option<usize>> {
    match levels_iter.next_levels() {
        Some(Ok(levels)) => {
            let vs = append_array(
                &mut buffers.aux_vec,
                &mut buffers.data_vec,
                max_rep_level,
                max_def_level,
                &levels.rep_levels,
                &levels.def_levels,
                slicer,
            )?;
            Ok(Some(vs))
        }
        Some(Err(e)) => Err(e),
        None => Ok(None),
    }
}

fn decode_array_rows_generic<T: DataPageSlicer>(
    levels_iter: &mut LevelsIterator,
    max_rep_level: u32,
    max_def_level: u32,
    row_lo: usize,
    row_hi: usize,
    slicer: &mut T,
    buffers: &mut ColumnChunkBuffers,
) -> ParquetResult<()> {
    if row_lo > 0 {
        let non_null_skipped = levels_iter.skip_rows(row_lo, max_def_level)?;
        slicer.skip(non_null_skipped);
    }

    let target_rows = row_hi - row_lo;
    if target_rows == 0 {
        return Ok(());
    }

    let Some(first_vs) = read_and_append_one_row_generic(
        levels_iter,
        max_rep_level,
        max_def_level,
        slicer,
        buffers,
    )?
    else {
        return Ok(());
    };
    if target_rows > 1 && first_vs > 0 {
        buffers.data_vec.reserve(first_vs * (target_rows - 1))?;
    }
    for _ in 1..target_rows {
        if read_and_append_one_row_generic(
            levels_iter,
            max_rep_level,
            max_def_level,
            slicer,
            buffers,
        )?
        .is_none()
        {
            break;
        }
    }
    Ok(())
}

fn append_array<T: DataPageSlicer>(
    aux_mem: &mut AcVec<u8>,
    data_mem: &mut AcVec<u8>,
    max_rep_level: u32,
    max_def_level: u32,
    rep_levels: &[u32],
    def_levels: &[u32],
    slicer: &mut T,
) -> ParquetResult<usize> {
    if def_levels.len() == 1 && def_levels[0] == 0 {
        append_array_null(aux_mem, data_mem)?;
        return Ok(0);
    }

    let shape_size = align8b(4 * max_rep_level as usize);
    let value_size = shape_size + 8 * rep_levels.len();

    aux_mem.extend_from_slice(&data_mem.len().to_le_bytes())?;
    aux_mem.extend_from_slice(&value_size.to_le_bytes())?;

    // first, calculate and write shape
    let mut shape = [0_u32; ARRAY_NDIMS_LIMIT];
    calculate_array_shape(&mut shape, max_rep_level, rep_levels);
    data_mem.reserve(value_size)?;
    let mut num_elements: usize = 1;
    for &dim in shape.iter().take(max_rep_level as usize) {
        num_elements *= dim as usize;
        data_mem.extend_from_slice(&dim.to_le_bytes())?;
    }
    if num_elements != def_levels.len() {
        return Err(fmt_err!(
            InvalidLayout,
            "incomplete array structure: expected {} elements, present {}",
            num_elements,
            def_levels.len(),
        ));
    }
    // add an optional padding
    if !max_rep_level.is_multiple_of(2) {
        data_mem.extend_from_slice(&0_u32.to_le_bytes())?;
    }

    // next, copy elements
    let non_null_count = def_levels.iter().filter(|&&d| d == max_def_level).count();
    if non_null_count == def_levels.len() {
        // All non-null: batch copy.
        slicer.next_slice_into(def_levels.len(), data_mem)?;
    } else {
        for &def_level in def_levels {
            if def_level == max_def_level {
                slicer.next_into(data_mem)?;
            } else {
                data_mem.extend_from_slice(&f64::NAN.to_le_bytes())?;
            }
        }
    }
    Ok(value_size)
}

fn decompress_sliced_dict(page: SlicedDictPage, buffer: &mut Vec<u8>) -> ParquetResult<DictPage> {
    let buf = if page.compression != parquet2::compression::Compression::Uncompressed {
        let read_size = page.uncompressed_size;
        buffer.resize(read_size, 0);
        parquet2::compression::decompress(page.compression, page.buffer, buffer)?;
        std::mem::take(buffer)
    } else {
        page.buffer.to_vec()
    };
    Ok(DictPage::new(buf, page.num_values, page.is_sorted))
}

fn decompress_sliced_data(
    page: &SlicedDataPage<'_>,
    decompress_buffer: &mut Vec<u8>,
) -> ParquetResult<DataPage> {
    if page.compression != parquet2::compression::Compression::Uncompressed {
        match &page.header {
            DataPageHeader::V1(_) => {
                let read_size = page.uncompressed_size;
                decompress_buffer.resize(read_size, 0);
                parquet2::compression::decompress(
                    page.compression,
                    page.buffer,
                    decompress_buffer,
                )?;
            }
            DataPageHeader::V2(header) => {
                let read_size = page.uncompressed_size;
                decompress_buffer.resize(read_size, 0);
                let offset = (header.definition_levels_byte_length
                    + header.repetition_levels_byte_length) as usize;
                let can_decompress = header.is_compressed.unwrap_or(true);
                if can_decompress {
                    if offset > decompress_buffer.len() || offset > page.buffer.len() {
                        return Err(fmt_err!(
                            Layout,
                            "V2 Page Header reported incorrect offset to compressed data"
                        ));
                    }
                    decompress_buffer[..offset].copy_from_slice(&page.buffer[..offset]);
                    parquet2::compression::decompress(
                        page.compression,
                        &page.buffer[offset..],
                        &mut decompress_buffer[offset..],
                    )?;
                } else {
                    if decompress_buffer.len() != page.buffer.len() {
                        return Err(fmt_err!(
                            Layout,
                            "V2 Page Header reported incorrect decompressed size"
                        ));
                    }
                    decompress_buffer.copy_from_slice(page.buffer);
                }
            }
        }
    } else {
        decompress_buffer.clear();
        decompress_buffer.extend_from_slice(page.buffer);
    }
    Ok(DataPage::new(
        page.header.clone(),
        std::mem::take(decompress_buffer),
        page.descriptor.clone(),
        None,
    ))
}

fn sliced_page_row_count(page: &SlicedDataPage, column_type: ColumnType) -> Option<usize> {
    match &page.header {
        DataPageHeader::V2(header) => Some(header.num_rows as usize),
        DataPageHeader::V1(header) => match column_type.tag() {
            ColumnTypeTag::Array => {
                if page.descriptor.primitive_type.physical_type == PhysicalType::ByteArray {
                    Some(header.num_values as usize)
                } else {
                    None
                }
            }
            _ => Some(header.num_values as usize),
        },
    }
}

fn page_row_count(page: &DataPage, column_type: ColumnType) -> ParquetResult<usize> {
    match page.header() {
        // V2 has explicit number of rows in the header.
        DataPageHeader::V2(header) => Ok(header.num_rows as usize),
        // V1 is more tricky in case of group types.
        DataPageHeader::V1(header) => {
            match column_type.tag() {
                ColumnTypeTag::Array => {
                    if page.descriptor.primitive_type.physical_type == PhysicalType::ByteArray {
                        // It's native array encoding, so we can just use the number of values.
                        Ok(header.num_values as usize)
                    } else {
                        // Slow path: array as LIST encoding + V1 format.
                        // We have to calculate the number of rows based on the repetition levels.
                        let (rep_levels, _, _) = split_buffer(page)?;

                        let num_rows = HybridRleDecoder::try_new(
                            rep_levels,
                            get_bit_width(page.descriptor.max_rep_level),
                            header.num_values as usize,
                        )?
                        .filter_map(|rep_level| match rep_level {
                            Ok(rep_level) => {
                                if rep_level == 0 {
                                    Some(())
                                } else {
                                    None
                                }
                            }
                            _ => None,
                        })
                        .count();
                        Ok(num_rows)
                    }
                }
                // For primitive types number of rows matches the number of values.
                _ => Ok(header.num_values as usize),
            }
        }
    }
}

fn long_stat_value(value: &Option<Vec<u8>>) -> ParquetResult<i64> {
    let value = value
        .as_ref()
        .ok_or_else(|| fmt_err!(InvalidLayout, "missing statistics value"))?;
    if value.len() != 8 {
        return Err(fmt_err!(
            InvalidLayout,
            "unexpected value byte array size of {}",
            value.len()
        ));
    }
    Ok(i64::from_le_bytes(
        value[0..8].try_into().expect("unexpected vec length"),
    ))
}

#[cfg(test)]
mod tests {
    use crate::allocator::{AcVec, TestAllocatorState};
    use crate::parquet::qdb_metadata::{QdbMetaCol, QdbMetaColFormat};
    use crate::parquet::tests::ColumnTypeTagExt;
    use crate::parquet_read::decode::{INT_NULL, LONG_NULL, UUID_NULL};
    use crate::parquet_read::{ColumnChunkBuffers, DecodeContext, ParquetDecoder, RowGroupBuffers};
    use crate::parquet_write::array::{append_array_null, append_raw_array};
    use crate::parquet_write::file::ParquetWriter;
    use crate::parquet_write::schema::{Column, Partition};
    use crate::parquet_write::varchar::{append_varchar, append_varchar_null};
    use arrow::datatypes::ToByteSlice;
    use parquet2::write::Version;
    use qdb_core::col_type::{encode_array_type, ColumnType, ColumnTypeTag};
    use rand::Rng;
    use std::io::Cursor;
    use std::mem::size_of;
    use std::ptr::null;

    #[test]
    fn test_decode_int_column_v2_nulls() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let row_count = 10;
        let row_group_size = 50;
        let data_page_size = 50;
        let version = Version::V2;
        let expected_buff =
            create_col_data_buff::<i32, 4, _>(row_count, INT_NULL, |int| int.to_le_bytes());
        let column_count = 1;
        let file = write_parquet_file(
            row_count,
            row_group_size,
            data_page_size,
            version,
            expected_buff.data_vec.as_ref(),
        );

        let file_len = file.len() as u64;
        let mut reader = Cursor::new(&file);
        let decoder = ParquetDecoder::read(allocator.clone(), &mut reader, file_len).unwrap();
        assert_eq!(decoder.columns.len(), column_count);
        assert_eq!(decoder.row_count, row_count);
        let row_group_count = decoder.row_group_count as usize;
        let bufs = &mut ColumnChunkBuffers::new(allocator.clone());
        let mut ctx = DecodeContext::new(file.as_ptr(), file_len);

        for column_index in 0..column_count {
            let column_type = decoder.columns[column_index].column_type.unwrap();
            let col_info = QdbMetaCol { column_type, column_top: 0, format: None };
            for row_group_index in 0..row_group_count {
                decoder
                    .decode_column_chunk(
                        &mut ctx,
                        bufs,
                        row_group_index,
                        0,
                        row_group_size,
                        column_index,
                        col_info,
                    )
                    .unwrap();

                assert_eq!(bufs.data_size, expected_buff.data_vec.len());
                assert_eq!(bufs.aux_size, 0);
                assert_eq!(bufs.data_vec, expected_buff.data_vec);
            }
        }
    }

    #[test]
    fn test_decode_int_column_v2_partial_decode() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let row_count = 100;
        let row_group_size = 10;
        let data_page_size = 5;
        let version = Version::V2;
        let expected_buff =
            create_col_data_buff::<i32, 4, _>(row_count, INT_NULL, |int| int.to_le_bytes());
        let column_count = 1;
        let file = write_parquet_file(
            row_count,
            row_group_size,
            data_page_size,
            version,
            expected_buff.data_vec.as_ref(),
        );

        let file_len = file.len() as u64;
        let mut reader = Cursor::new(&file);
        let decoder = ParquetDecoder::read(allocator.clone(), &mut reader, file_len).unwrap();
        assert_eq!(decoder.columns.len(), column_count);
        assert_eq!(decoder.row_count, row_count);
        let row_group_count = decoder.row_group_count as usize;
        let bufs = &mut ColumnChunkBuffers::new(allocator);
        let mut ctx = DecodeContext::new(file.as_ptr(), file_len);

        for row_lo in 0..row_group_size - 1 {
            for row_hi in row_lo + 1..row_group_size {
                for column_index in 0..column_count {
                    let column_type = decoder.columns[column_index].column_type.unwrap();
                    let col_info = QdbMetaCol { column_type, column_top: 0, format: None };
                    for row_group_index in 0..row_group_count {
                        decoder
                            .decode_column_chunk(
                                &mut ctx,
                                bufs,
                                row_group_index,
                                row_lo,
                                row_hi,
                                column_index,
                                col_info,
                            )
                            .unwrap();

                        assert_eq!(bufs.data_size, 4 * (row_hi - row_lo));
                        assert_eq!(bufs.aux_size, 0);
                        let row_group_offset = 4 * row_group_index * row_group_size;
                        assert_eq!(
                            bufs.data_vec,
                            expected_buff.data_vec
                                [row_group_offset + 4 * row_lo..row_group_offset + 4 * row_hi]
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn test_decode_int_long_column_v2_nulls_multi_groups() {
        let row_count = 10000;
        let row_group_size = 1000;
        let data_page_size = 1000;
        let version = Version::V1;
        let mut columns = Vec::new();

        let mut expected_buffs: Vec<(ColumnBuffers, ColumnType)> = Vec::new();
        let expected_int_buff =
            create_col_data_buff::<i32, 4, _>(row_count, INT_NULL, |int| int.to_le_bytes());
        columns.push(create_fix_column(
            columns.len() as i32,
            row_count,
            "int_col",
            expected_int_buff.data_vec.as_ref(),
            ColumnTypeTag::Int.into_type(),
        ));
        expected_buffs.push((expected_int_buff, ColumnTypeTag::Int.into_type()));

        let expected_long_buff =
            create_col_data_buff::<i64, 8, _>(row_count, LONG_NULL, |int| int.to_le_bytes());
        columns.push(create_fix_column(
            columns.len() as i32,
            row_count,
            "long_col",
            expected_long_buff.data_vec.as_ref(),
            ColumnTypeTag::Long.into_type(),
        ));
        expected_buffs.push((expected_long_buff, ColumnTypeTag::Long.into_type()));

        let string_buffers = create_col_data_buff_string(row_count, 3);
        columns.push(create_var_column(
            columns.len() as i32,
            row_count,
            "string_col",
            string_buffers.data_vec.as_ref(),
            string_buffers.aux_vec.as_ref().unwrap(),
            ColumnTypeTag::String.into_type(),
        ));
        expected_buffs.push((string_buffers, ColumnTypeTag::String.into_type()));

        let varchar_buffers = create_col_data_buff_varchar(row_count, 3);
        columns.push(create_var_column(
            columns.len() as i32,
            row_count,
            "varchar_col",
            varchar_buffers.data_vec.as_ref(),
            varchar_buffers.aux_vec.as_ref().unwrap(),
            ColumnTypeTag::Varchar.into_type(),
        ));
        expected_buffs.push((varchar_buffers, ColumnTypeTag::Varchar.into_type()));

        let symbol_buffs = create_col_data_buff_symbol(row_count, 10);
        columns.push(create_symbol_column(
            columns.len() as i32,
            row_count,
            "symbol_col",
            symbol_buffs.data_vec.as_ref(),
            symbol_buffs.sym_chars.as_ref().unwrap(),
            symbol_buffs.sym_offsets.as_ref().unwrap(),
            ColumnTypeTag::Symbol.into_type(),
        ));
        expected_buffs.push((symbol_buffs, ColumnTypeTag::Varchar.into_type()));

        let array_buffers = create_col_data_buff_array(row_count, 3);
        let array_type = encode_array_type(ColumnTypeTag::Double, 1).unwrap();
        columns.push(create_var_column(
            columns.len() as i32,
            row_count,
            "array_col",
            array_buffers.data_vec.as_ref(),
            array_buffers.aux_vec.as_ref().unwrap(),
            array_type,
        ));
        expected_buffs.push((array_buffers, array_type));

        assert_columns(
            row_count,
            row_group_size,
            data_page_size,
            version,
            columns,
            &expected_buffs,
        );
    }

    #[test]
    fn test_decode_column_type2() {
        let row_count = 10000;
        let row_group_size = 1000;
        let data_page_size = 1000;
        let version = Version::V2;
        let mut columns = Vec::new();
        let mut expected_buffs: Vec<(ColumnBuffers, ColumnType)> = Vec::new();

        let expected_bool_buff = create_col_data_buff_bool(row_count);
        columns.push(create_fix_column(
            columns.len() as i32,
            row_count,
            "bool_col",
            expected_bool_buff.data_vec.as_ref(),
            ColumnTypeTag::Boolean.into_type(),
        ));
        expected_buffs.push((expected_bool_buff, ColumnTypeTag::Boolean.into_type()));

        let expected_col_buff =
            create_col_data_buff::<i16, 2, _>(row_count, i16::MIN.to_le_bytes(), |short| {
                short.to_le_bytes()
            });
        columns.push(create_fix_column(
            columns.len() as i32,
            row_count,
            "bool_short",
            expected_col_buff.data_vec.as_ref(),
            ColumnTypeTag::Short.into_type(),
        ));
        expected_buffs.push((expected_col_buff, ColumnTypeTag::Short.into_type()));

        let expected_bool_buff =
            create_col_data_buff::<i16, 2, _>(row_count, i16::MIN.to_le_bytes(), |short| {
                short.to_le_bytes()
            });
        columns.push(create_fix_column(
            columns.len() as i32,
            row_count,
            "bool_char",
            expected_bool_buff.data_vec.as_ref(),
            ColumnTypeTag::Char.into_type(),
        ));
        expected_buffs.push((expected_bool_buff, ColumnTypeTag::Char.into_type()));

        let expected_uuid_buff =
            create_col_data_buff::<i128, 16, _>(row_count, UUID_NULL, |uuid| uuid.to_le_bytes());
        columns.push(create_fix_column(
            columns.len() as i32,
            row_count,
            "bool_char",
            expected_uuid_buff.data_vec.as_ref(),
            ColumnTypeTag::Uuid.into_type(),
        ));
        expected_buffs.push((expected_uuid_buff, ColumnTypeTag::Uuid.into_type()));

        assert_columns(
            row_count,
            row_group_size,
            data_page_size,
            version,
            columns,
            &expected_buffs,
        );
    }

    fn assert_columns(
        row_count: usize,
        row_group_size: usize,
        data_page_size: usize,
        version: Version,
        columns: Vec<Column>,
        expected_buffs: &[(ColumnBuffers, ColumnType)],
    ) {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let column_count = columns.len();
        let file = write_cols_to_parquet_file(row_group_size, data_page_size, version, columns);

        let file_len = file.len() as u64;
        let mut reader = Cursor::new(&file);
        let decoder = ParquetDecoder::read(allocator.clone(), &mut reader, file_len).unwrap();
        assert_eq!(decoder.columns.len(), column_count);
        assert_eq!(decoder.row_count, row_count);
        let row_group_count = decoder.row_group_count as usize;
        let bufs = &mut ColumnChunkBuffers::new(allocator.clone());
        let mut ctx = DecodeContext::new(file.as_ptr(), file_len);

        for (column_index, (column_buffs, column_type)) in expected_buffs.iter().enumerate() {
            let column_type = *column_type;
            let format = if column_type.tag() == ColumnTypeTag::Symbol {
                Some(QdbMetaColFormat::LocalKeyIsGlobal)
            } else {
                None
            };
            let mut data_offset = 0usize;
            let mut col_row_count = 0usize;
            let expected = column_buffs
                .expected_data_buff
                .as_ref()
                .unwrap_or(column_buffs.data_vec.as_ref());
            let expected_aux = column_buffs
                .expected_aux_buff
                .as_ref()
                .or(column_buffs.aux_vec.as_ref());

            for row_group_index in 0..row_group_count {
                let row_count = decoder
                    .decode_column_chunk(
                        &mut ctx,
                        bufs,
                        row_group_index,
                        0,
                        row_group_size,
                        column_index,
                        QdbMetaCol { column_type, column_top: 0, format },
                    )
                    .unwrap();

                assert_eq!(bufs.data_vec.len(), bufs.data_size);

                assert!(
                    data_offset + bufs.data_size <= expected.len(),
                    "Assertion failed: {} + {} < {}, where read_row_offset = {}, bufs.data_size = {}, expected.len() = {}",
                    data_offset, bufs.data_size, expected.len(), data_offset, bufs.data_size, expected.len()
                );

                assert_eq!(
                    expected[data_offset..data_offset + bufs.data_size],
                    bufs.data_vec
                );

                if let Some(expected_aux_data) = expected_aux {
                    if col_row_count == 0 {
                        assert_eq!(&expected_aux_data[0..bufs.aux_size], bufs.aux_vec);
                    } else if column_type.tag() == ColumnTypeTag::String {
                        let mut expected_aux_data_slice = AcVec::new_in(allocator.clone());
                        let vec_i64_ref = unsafe {
                            std::slice::from_raw_parts(
                                expected_aux_data.as_ptr() as *const i64,
                                expected_aux_data.len() / size_of::<i64>(),
                            )
                        };
                        expected_aux_data_slice
                            .extend_from_slice(&0u64.to_le_bytes())
                            .unwrap();
                        for i in 0..row_count {
                            let row_data_offset = vec_i64_ref[col_row_count + 1 + i];
                            expected_aux_data_slice
                                .extend_from_slice(
                                    &(row_data_offset - data_offset as i64).to_le_bytes(),
                                )
                                .unwrap();
                        }
                        assert_eq!(expected_aux_data_slice, bufs.aux_vec);
                    }
                } else {
                    assert_eq!(bufs.aux_size, 0);
                }
                col_row_count += row_count;
                data_offset += bufs.data_vec.len();
            }

            assert_eq!(expected.len(), data_offset);
            assert_eq!(row_count, col_row_count);
        }
    }

    fn write_parquet_file(
        row_count: usize,
        row_group_size: usize,
        data_page_size: usize,
        version: Version,
        expected_buff: &[u8],
    ) -> Vec<u8> {
        let columns = vec![create_fix_column(
            0,
            row_count,
            "int_col",
            expected_buff,
            ColumnTypeTag::Int.into_type(),
        )];

        write_cols_to_parquet_file(row_group_size, data_page_size, version, columns)
    }

    fn write_cols_to_parquet_file(
        row_group_size: usize,
        data_page_size: usize,
        version: Version,
        columns: Vec<Column>,
    ) -> Vec<u8> {
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let partition = Partition { table: "test_table".to_string(), columns };
        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_raw_array_encoding(true)
            .with_row_group_size(Some(row_group_size))
            .with_data_page_size(Some(data_page_size))
            .with_version(version)
            .finish(partition)
            .expect("parquet writer");

        buf.into_inner()
    }

    fn create_col_data_buff_bool(row_count: usize) -> ColumnBuffers {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let value_size = 1;
        let mut buff = AcVec::new_in(allocator);
        buff.extend_with(row_count * value_size, 0u8).unwrap();
        for i in 0..row_count {
            let value = i % 3 == 0;
            let offset = i * value_size;
            let bval = if value { 1u8 } else { 0u8 };
            buff[offset] = bval;
        }
        ColumnBuffers {
            data_vec: buff,
            aux_vec: None,
            sym_offsets: None,
            sym_chars: None,
            expected_data_buff: None,
            expected_aux_buff: None,
        }
    }

    fn create_col_data_buff<T, const N: usize, F>(
        row_count: usize,
        null_value: [u8; N],
        to_le_bytes: F,
    ) -> ColumnBuffers
    where
        T: From<i16> + Copy,
        F: Fn(T) -> [u8; N],
    {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let value_size = N;
        let mut buff = AcVec::new_in(allocator);
        buff.extend_with(row_count * value_size, 0u8).unwrap();
        for i in 0..row_count.div_ceil(2) {
            let value = T::from(i as i16);
            let offset = 2 * i * value_size;
            buff[offset..offset + value_size].copy_from_slice(&to_le_bytes(value));

            if offset + 2 * value_size <= buff.len() {
                buff[offset + value_size..offset + 2 * value_size].copy_from_slice(&null_value);
            }
        }
        ColumnBuffers {
            data_vec: buff,
            aux_vec: None,
            sym_offsets: None,
            sym_chars: None,
            expected_data_buff: None,
            expected_aux_buff: None,
        }
    }

    fn generate_random_unicode_string(len: usize) -> String {
        let mut rng = rand::rng();

        let len = 1 + rng.random_range(0..len - 1);

        // 0x00A0..0xD7FF generates a random Unicode scalar value in a range that includes non-ASCII characters
        let range = if rng.random_bool(0.5) {
            0x00A0..0xD7FF
        } else {
            33..126
        };

        let random_string: String = (0..len)
            .map(|_| {
                let c = rng.random_range(range.start..range.end);
                char::from_u32(c).unwrap_or('�') // Use a replacement character for invalid values
            })
            .collect();

        random_string
    }

    fn generate_random_binary(len: usize) -> Vec<u8> {
        let mut rng = rand::rng();

        let len = 1 + rng.random_range(0..len - 1);

        let random_bin: Vec<u8> = (0..len)
            .map(|_| {
                let u: u8 = rng.random();
                u
            })
            .collect();

        random_bin
    }

    struct ColumnBuffers {
        data_vec: AcVec<u8>,
        aux_vec: Option<AcVec<u8>>,
        sym_offsets: Option<AcVec<u64>>,
        sym_chars: Option<AcVec<u8>>,
        expected_data_buff: Option<AcVec<u8>>,
        expected_aux_buff: Option<AcVec<u8>>,
    }

    fn create_col_data_buff_symbol(row_count: usize, distinct_values: usize) -> ColumnBuffers {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let mut symbol_data_buff = AcVec::new_in(allocator.clone());
        let mut expected_aux_buff = AcVec::new_in(allocator.clone());
        let mut expected_data_buff = AcVec::new_in(allocator);

        let str_values: Vec<String> = (0..distinct_values)
            .map(|_| generate_random_unicode_string(10))
            .collect();

        let (symbol_chars_buff, symbol_offsets_buff) = serialize_as_symbols(&str_values);

        let mut i = 0;
        let null_sym_value = i32::MIN.to_le_bytes();
        while i < row_count {
            let symbol_value = i % distinct_values;
            symbol_data_buff
                .extend_from_slice(&(symbol_value as i32).to_le_bytes())
                .unwrap();

            let str_value = &str_values[i % distinct_values];
            append_varchar(
                &mut expected_aux_buff,
                &mut expected_data_buff,
                str_value.as_bytes(),
            )
            .unwrap();
            i += 1;

            if i < row_count {
                symbol_data_buff.extend_from_slice(&null_sym_value).unwrap();
                append_varchar_null(&mut expected_aux_buff, &expected_data_buff).unwrap();
                i += 1;
            }
        }

        ColumnBuffers {
            data_vec: symbol_data_buff,
            aux_vec: None,
            sym_offsets: Some(symbol_offsets_buff),
            sym_chars: Some(symbol_chars_buff),
            expected_data_buff: Some(expected_data_buff),
            expected_aux_buff: Some(expected_aux_buff),
        }
    }

    fn serialize_as_symbols(symbol_chars: &[String]) -> (AcVec<u8>, AcVec<u64>) {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let mut chars = AcVec::new_in(allocator.clone());
        let mut offsets = AcVec::new_in(allocator);

        for s in symbol_chars {
            let sym_chars: Vec<_> = s.encode_utf16().collect();
            let len = sym_chars.len();
            offsets.push(chars.len() as u64).unwrap();
            chars
                .extend_from_slice(&(len as u32).to_le_bytes())
                .unwrap();
            let encoded: &[u8] = unsafe {
                std::slice::from_raw_parts(
                    sym_chars.as_ptr() as *const u8,
                    sym_chars.len() * size_of::<u16>(),
                )
            };
            chars.extend_from_slice(encoded).unwrap();
        }

        (chars, offsets)
    }

    fn create_col_data_buff_varchar(row_count: usize, distinct_values: usize) -> ColumnBuffers {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let mut aux_buff = AcVec::new_in(allocator.clone());
        let mut data_buff = AcVec::new_in(allocator);

        let str_values: Vec<String> = (0..distinct_values)
            .map(|_| generate_random_unicode_string(10))
            .collect();

        let mut i = 0;
        while i < row_count {
            let str_value = &str_values[i % distinct_values];
            append_varchar(&mut aux_buff, &mut data_buff, str_value.as_bytes()).unwrap();
            i += 1;

            if i < row_count {
                append_varchar_null(&mut aux_buff, &data_buff).unwrap();
                i += 1;
            }
        }
        ColumnBuffers {
            data_vec: data_buff,
            aux_vec: Some(aux_buff),
            sym_offsets: None,
            sym_chars: None,
            expected_data_buff: None,
            expected_aux_buff: None,
        }
    }

    fn create_col_data_buff_string(row_count: usize, distinct_values: usize) -> ColumnBuffers {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let value_size = size_of::<i64>();
        let mut aux_buff = AcVec::new_in(allocator.clone());
        aux_buff.extend_with(value_size, 0u8).unwrap();
        let mut data_buff = AcVec::new_in(allocator);

        let str_values: Vec<Vec<u16>> = (0..distinct_values)
            .map(|_| generate_random_unicode_string(10).encode_utf16().collect())
            .collect();

        let mut i = 0;
        while i < row_count {
            let str_value = &str_values[i % distinct_values];
            data_buff
                .extend_from_slice(&(str_value.len() as i32).to_le_bytes())
                .unwrap();
            data_buff
                .extend_from_slice(str_value.to_byte_slice())
                .unwrap();
            aux_buff
                .extend_from_slice(&data_buff.len().to_le_bytes())
                .unwrap();
            i += 1;

            if i < row_count {
                data_buff.extend_from_slice(&(-1i32).to_le_bytes()).unwrap();
                aux_buff
                    .extend_from_slice(&data_buff.len().to_le_bytes())
                    .unwrap();
                i += 1;
            }
        }
        ColumnBuffers {
            data_vec: data_buff,
            aux_vec: Some(aux_buff),
            sym_offsets: None,
            sym_chars: None,
            expected_data_buff: None,
            expected_aux_buff: None,
        }
    }

    fn create_col_data_buff_array(row_count: usize, distinct_values: usize) -> ColumnBuffers {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let mut aux_buff = AcVec::new_in(allocator.clone());
        let mut data_buff = AcVec::new_in(allocator);

        let arr_values: Vec<Vec<u8>> = (0..distinct_values)
            .map(|_| generate_random_binary(10))
            .collect();

        let mut i = 0;
        while i < row_count {
            let arr_value = &arr_values[i % distinct_values];
            append_raw_array(&mut aux_buff, &mut data_buff, arr_value).unwrap();
            i += 1;

            if i < row_count {
                append_array_null(&mut aux_buff, &data_buff).unwrap();
                i += 1;
            }
        }
        ColumnBuffers {
            data_vec: data_buff,
            aux_vec: Some(aux_buff),
            sym_offsets: None,
            sym_chars: None,
            expected_data_buff: None,
            expected_aux_buff: None,
        }
    }

    fn create_fix_column(
        id: i32,
        row_count: usize,
        name: &'static str,
        primary_data: &[u8],
        col_type: ColumnType,
    ) -> Column {
        Column::from_raw_data(
            id,
            name,
            col_type.code(),
            0,
            row_count,
            primary_data.as_ptr(),
            primary_data.len(),
            null(),
            0,
            null(),
            0,
            false,
            false,
        )
        .unwrap()
    }

    fn create_var_column(
        id: i32,
        row_count: usize,
        name: &'static str,
        primary_data: &[u8],
        aux_data: &[u8],
        col_type: ColumnType,
    ) -> Column {
        Column::from_raw_data(
            id,
            name,
            col_type.code(),
            0,
            row_count,
            primary_data.as_ptr(),
            primary_data.len(),
            aux_data.as_ptr(),
            aux_data.len(),
            null(),
            0,
            false,
            false,
        )
        .unwrap()
    }

    fn create_symbol_column(
        id: i32,
        row_count: usize,
        name: &'static str,
        primary_data: &[u8],
        chars_data: &[u8],
        offsets: &[u64],
        col_type: ColumnType,
    ) -> Column {
        Column::from_raw_data(
            id,
            name,
            col_type.code(),
            0,
            row_count,
            primary_data.as_ptr(),
            primary_data.len(),
            chars_data.as_ptr(),
            chars_data.len(),
            offsets.as_ptr(),
            offsets.len(),
            false,
            false,
        )
        .unwrap()
    }

    #[test]
    fn test_decode_row_group_filtered_empty_filter() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let row_count = 10;
        let row_group_size = 10;
        let data_page_size = 5;
        let version = Version::V2;
        let expected_buff =
            create_col_data_buff::<i32, 4, _>(row_count, INT_NULL, |int| int.to_le_bytes());
        let file = write_parquet_file(
            row_count,
            row_group_size,
            data_page_size,
            version,
            expected_buff.data_vec.as_ref(),
        );

        let file_len = file.len() as u64;
        let mut reader = Cursor::new(&file);
        let decoder = ParquetDecoder::read(allocator.clone(), &mut reader, file_len).unwrap();
        let mut rgb = RowGroupBuffers::new(allocator);
        let mut ctx = DecodeContext::new(file.as_ptr(), file_len);
        let columns = vec![(0i32, ColumnTypeTag::Int.into_type())];
        let rows_filter: Vec<i64> = vec![];

        let count = decoder
            .decode_row_group_filtered::<false>(
                &mut ctx,
                &mut rgb,
                0,
                &columns,
                0,
                0,
                row_group_size as u32,
                &rows_filter,
            )
            .unwrap();

        assert_eq!(count, 0);
        assert_eq!(rgb.column_bufs[0].data_vec.len(), 0);
    }

    #[test]
    fn test_decode_row_group_filtered_single_row() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let row_count = 10;
        let row_group_size = 10;
        let data_page_size = 5;
        let version = Version::V2;
        let expected_buff =
            create_col_data_buff::<i32, 4, _>(row_count, INT_NULL, |int| int.to_le_bytes());
        let file = write_parquet_file(
            row_count,
            row_group_size,
            data_page_size,
            version,
            expected_buff.data_vec.as_ref(),
        );

        let file_len = file.len() as u64;
        let mut reader = Cursor::new(&file);
        let decoder = ParquetDecoder::read(allocator.clone(), &mut reader, file_len).unwrap();
        let mut rgb = RowGroupBuffers::new(allocator);
        let mut ctx = DecodeContext::new(file.as_ptr(), file_len);
        let columns = vec![(0i32, ColumnTypeTag::Int.into_type())];
        let rows_filter: Vec<i64> = vec![3];

        let count = decoder
            .decode_row_group_filtered::<false>(
                &mut ctx,
                &mut rgb,
                0,
                &columns,
                0,
                0,
                row_group_size as u32,
                &rows_filter,
            )
            .unwrap();

        assert_eq!(count, 1);
        assert_eq!(rgb.column_bufs[0].data_vec.len(), 4);
        assert_eq!(
            rgb.column_bufs[0].data_vec.as_slice(),
            &expected_buff.data_vec[12..16]
        );
    }

    #[test]
    fn test_decode_row_group_filtered_consecutive_rows() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let row_count = 20;
        let row_group_size = 20;
        let data_page_size = 5;
        let version = Version::V2;
        let expected_buff =
            create_col_data_buff::<i32, 4, _>(row_count, INT_NULL, |int| int.to_le_bytes());
        let file = write_parquet_file(
            row_count,
            row_group_size,
            data_page_size,
            version,
            expected_buff.data_vec.as_ref(),
        );

        let file_len = file.len() as u64;
        let mut reader = Cursor::new(&file);
        let decoder = ParquetDecoder::read(allocator.clone(), &mut reader, file_len).unwrap();
        let mut rgb = RowGroupBuffers::new(allocator);
        let mut ctx = DecodeContext::new(file.as_ptr(), file_len);

        let columns = vec![(0i32, ColumnTypeTag::Int.into_type())];
        let rows_filter: Vec<i64> = vec![5, 6, 7, 8];

        let count = decoder
            .decode_row_group_filtered::<false>(
                &mut ctx,
                &mut rgb,
                0,
                &columns,
                0,
                0,
                row_group_size as u32,
                &rows_filter,
            )
            .unwrap();

        assert_eq!(count, 4);
        assert_eq!(rgb.column_bufs[0].data_vec.len(), 16);
        assert_eq!(
            rgb.column_bufs[0].data_vec.as_slice(),
            &expected_buff.data_vec[20..36]
        );
    }

    #[test]
    fn test_decode_row_group_filtered_non_consecutive_rows() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let row_count = 20;
        let row_group_size = 20;
        let data_page_size = 5;
        let version = Version::V2;
        let expected_buff =
            create_col_data_buff::<i32, 4, _>(row_count, INT_NULL, |int| int.to_le_bytes());
        let file = write_parquet_file(
            row_count,
            row_group_size,
            data_page_size,
            version,
            expected_buff.data_vec.as_ref(),
        );

        let file_len = file.len() as u64;
        let mut reader = Cursor::new(&file);
        let decoder = ParquetDecoder::read(allocator.clone(), &mut reader, file_len).unwrap();
        let mut rgb = RowGroupBuffers::new(allocator);
        let mut ctx = DecodeContext::new(file.as_ptr(), file_len);
        let columns = vec![(0i32, ColumnTypeTag::Int.into_type())];
        let rows_filter: Vec<i64> = vec![2, 5, 10, 15];

        let count = decoder
            .decode_row_group_filtered::<false>(
                &mut ctx,
                &mut rgb,
                0,
                &columns,
                0,
                0,
                row_group_size as u32,
                &rows_filter,
            )
            .unwrap();

        assert_eq!(count, 4);
        assert_eq!(rgb.column_bufs[0].data_vec.len(), 16);

        let result: Vec<i32> = rgb.column_bufs[0]
            .data_vec
            .chunks(4)
            .map(|c| i32::from_le_bytes(c.try_into().unwrap()))
            .collect();
        let expected: Vec<i32> = [2, 5, 10, 15]
            .iter()
            .map(|&i| {
                i32::from_le_bytes(
                    expected_buff.data_vec[i * 4..(i + 1) * 4]
                        .try_into()
                        .unwrap(),
                )
            })
            .collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_decode_row_group_filtered_fill_nulls_true() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let row_count = 10;
        let row_group_size = 10;
        let data_page_size = 5;
        let version = Version::V2;
        let expected_buff =
            create_col_data_buff::<i32, 4, _>(row_count, INT_NULL, |int| int.to_le_bytes());
        let file = write_parquet_file(
            row_count,
            row_group_size,
            data_page_size,
            version,
            expected_buff.data_vec.as_ref(),
        );

        let file_len = file.len() as u64;
        let mut reader = Cursor::new(&file);
        let decoder = ParquetDecoder::read(allocator.clone(), &mut reader, file_len).unwrap();
        let mut rgb = RowGroupBuffers::new(allocator);
        let mut ctx = DecodeContext::new(file.as_ptr(), file_len);
        let columns = vec![(0i32, ColumnTypeTag::Int.into_type())];
        let rows_filter: Vec<i64> = vec![1, 3, 5];

        let count = decoder
            .decode_row_group_filtered::<true>(
                &mut ctx,
                &mut rgb,
                0,
                &columns,
                0,
                0,
                row_group_size as u32,
                &rows_filter,
            )
            .unwrap();

        assert_eq!(count, 10);
        assert_eq!(rgb.column_bufs[0].data_vec.len(), 40);
        let result: Vec<i32> = rgb.column_bufs[0]
            .data_vec
            .chunks(4)
            .map(|c| i32::from_le_bytes(c.try_into().unwrap()))
            .collect();

        for (i, &val) in result.iter().enumerate() {
            if rows_filter.contains(&(i as i64)) {
                let expected_val = i32::from_le_bytes(
                    expected_buff.data_vec[i * 4..(i + 1) * 4]
                        .try_into()
                        .unwrap(),
                );
                assert_eq!(val, expected_val, "Row {} should have value", i);
            } else {
                assert_eq!(val, i32::MIN, "Row {} should be NULL", i);
            }
        }
    }

    #[test]
    fn test_decode_row_group_filtered_across_pages() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let row_count = 100;
        let row_group_size = 100;
        let data_page_size = 10; // 10 pages of 10 rows each
        let version = Version::V2;
        let expected_buff =
            create_col_data_buff::<i32, 4, _>(row_count, INT_NULL, |int| int.to_le_bytes());
        let file = write_parquet_file(
            row_count,
            row_group_size,
            data_page_size,
            version,
            expected_buff.data_vec.as_ref(),
        );

        let file_len = file.len() as u64;
        let mut reader = Cursor::new(&file);
        let decoder = ParquetDecoder::read(allocator.clone(), &mut reader, file_len).unwrap();
        let mut rgb = RowGroupBuffers::new(allocator);
        let mut ctx = DecodeContext::new(file.as_ptr(), file_len);
        let columns = vec![(0i32, ColumnTypeTag::Int.into_type())];
        let rows_filter: Vec<i64> = vec![5, 25, 55, 95];

        let count = decoder
            .decode_row_group_filtered::<false>(
                &mut ctx,
                &mut rgb,
                0,
                &columns,
                0,
                0,
                row_group_size as u32,
                &rows_filter,
            )
            .unwrap();

        assert_eq!(count, 4);
        let result: Vec<i32> = rgb.column_bufs[0]
            .data_vec
            .chunks(4)
            .map(|c| i32::from_le_bytes(c.try_into().unwrap()))
            .collect();
        let expected: Vec<i32> = [5, 25, 55, 95]
            .iter()
            .map(|&i| {
                i32::from_le_bytes(
                    expected_buff.data_vec[i * 4..(i + 1) * 4]
                        .try_into()
                        .unwrap(),
                )
            })
            .collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_decode_row_group_filtered_with_row_range() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let row_count = 20;
        let row_group_size = 20;
        let data_page_size = 5;
        let version = Version::V2;
        let expected_buff =
            create_col_data_buff::<i32, 4, _>(row_count, INT_NULL, |int| int.to_le_bytes());
        let file = write_parquet_file(
            row_count,
            row_group_size,
            data_page_size,
            version,
            expected_buff.data_vec.as_ref(),
        );

        let file_len = file.len() as u64;
        let mut reader = Cursor::new(&file);
        let decoder = ParquetDecoder::read(allocator.clone(), &mut reader, file_len).unwrap();
        let mut rgb = RowGroupBuffers::new(allocator);
        let mut ctx = DecodeContext::new(file.as_ptr(), file_len);

        let columns = vec![(0i32, ColumnTypeTag::Int.into_type())];
        let rows_filter: Vec<i64> = vec![0, 2, 4, 6, 8];

        let count = decoder
            .decode_row_group_filtered::<false>(
                &mut ctx,
                &mut rgb,
                0,
                &columns,
                0,
                5,
                15,
                &rows_filter,
            )
            .unwrap();

        assert_eq!(count, 5);

        let result: Vec<i32> = rgb.column_bufs[0]
            .data_vec
            .chunks(4)
            .map(|c| i32::from_le_bytes(c.try_into().unwrap()))
            .collect();

        let expected: Vec<i32> = [5, 7, 9, 11, 13]
            .iter()
            .map(|&i| {
                i32::from_le_bytes(
                    expected_buff.data_vec[i * 4..(i + 1) * 4]
                        .try_into()
                        .unwrap(),
                )
            })
            .collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_decode_row_group_filtered_long_column() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let row_count = 20;
        let row_group_size = 20;
        let data_page_size = 10;
        let version = Version::V2;
        let expected_buff =
            create_col_data_buff::<i64, 8, _>(row_count, LONG_NULL, |long| long.to_le_bytes());
        let columns = vec![create_fix_column(
            0,
            row_count,
            "long_col",
            expected_buff.data_vec.as_ref(),
            ColumnTypeTag::Long.into_type(),
        )];
        let file = write_cols_to_parquet_file(row_group_size, data_page_size, version, columns);

        let file_len = file.len() as u64;
        let mut reader = Cursor::new(&file);
        let decoder = ParquetDecoder::read(allocator.clone(), &mut reader, file_len).unwrap();
        let mut rgb = RowGroupBuffers::new(allocator);
        let mut ctx = DecodeContext::new(file.as_ptr(), file_len);
        let columns = vec![(0i32, ColumnTypeTag::Long.into_type())];
        let rows_filter: Vec<i64> = vec![1, 5, 10, 15, 19];

        let count = decoder
            .decode_row_group_filtered::<false>(
                &mut ctx,
                &mut rgb,
                0,
                &columns,
                0,
                0,
                row_group_size as u32,
                &rows_filter,
            )
            .unwrap();

        assert_eq!(count, 5);
        assert_eq!(rgb.column_bufs[0].data_vec.len(), 40);

        let result: Vec<i64> = rgb.column_bufs[0]
            .data_vec
            .chunks(8)
            .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
            .collect();
        let expected: Vec<i64> = [1, 5, 10, 15, 19]
            .iter()
            .map(|&i| {
                i64::from_le_bytes(
                    expected_buff.data_vec[i * 8..(i + 1) * 8]
                        .try_into()
                        .unwrap(),
                )
            })
            .collect();
        assert_eq!(result, expected);
    }
}
