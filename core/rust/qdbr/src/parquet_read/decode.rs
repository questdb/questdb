use crate::allocator::{AcVec, QdbAllocator};
use crate::parquet::error::{fmt_err, ParquetErrorExt, ParquetResult};
use crate::parquet::qdb_metadata::{QdbMetaCol, QdbMetaColFormat};
use crate::parquet::util::{align8b, ARRAY_NDIMS_LIMIT};
use crate::parquet_read::column_sink::fixed::{
    FixedBooleanColumnSink, FixedDoubleColumnSink, FixedFloatColumnSink, FixedInt2ByteColumnSink,
    FixedInt2ShortColumnSink, FixedIntColumnSink, FixedLong128ColumnSink, FixedLong256ColumnSink,
    FixedLongColumnSink, IntDecimalColumnSink, NanoTimestampColumnSink, ReverseFixedColumnSink,
};
use crate::parquet_read::column_sink::var::ARRAY_AUX_SIZE;
use crate::parquet_read::column_sink::var::{
    BinaryColumnSink, RawArrayColumnSink, StringColumnSink, VarcharColumnSink,
};
use crate::parquet_read::column_sink::Pushable;
use crate::parquet_read::slicer::dict_decoder::{FixedDictDecoder, VarDictDecoder};
use crate::parquet_read::slicer::rle::{RleDictionarySlicer, RleLocalIsGlobalSymbolDecoder};
use crate::parquet_read::slicer::{
    BooleanBitmapSlicer, DataPageFixedSlicer, DataPageSlicer, DeltaBinaryPackedSlicer,
    DeltaBytesArraySlicer, DeltaLengthArraySlicer, PlainVarSlicer, ValueConvertSlicer,
};
use crate::parquet_read::{
    ColumnChunkBuffers, ColumnChunkStats, ParquetDecoder, RowGroupBuffers, RowGroupStatBuffers,
};
use crate::parquet_write::array::{append_array_null, calculate_array_shape, LevelsIterator};
use parquet2::deserialize::{HybridDecoderBitmapIter, HybridEncoded};
use parquet2::encoding::hybrid_rle::BitmapIter;
use parquet2::encoding::hybrid_rle::HybridRleDecoder;
use parquet2::encoding::{hybrid_rle, Encoding};
use parquet2::page::DataPageHeader;
use parquet2::page::{split_buffer, DataPage, DictPage, Page};
use parquet2::read::levels::get_bit_width;
use parquet2::read::{decompress, get_page_iterator};
use parquet2::schema::types::{PhysicalType, PrimitiveConvertedType, PrimitiveLogicalType};
use qdb_core::col_type::{ColumnType, ColumnTypeTag};
use std::cmp;
use std::cmp::min;
use std::io::{Read, Seek};
use std::ptr;

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

impl<R: Read + Seek> ParquetDecoder<R> {
    pub fn decode_row_group(
        &mut self,
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
            let mut column_type = self.columns[column_idx].column_type;

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

    pub fn decode_column_chunk(
        &mut self,
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

        let page_reader =
            get_page_iterator(column_metadata, &mut self.reader, None, vec![], chunk_size)?;

        match self.metadata.version {
            1 | 2 => Ok(()),
            ver => Err(fmt_err!(Unsupported, "unsupported parquet version: {ver}")),
        }?;

        let mut dict = None;
        let mut row_count = 0usize;
        column_chunk_bufs.aux_vec.clear();
        column_chunk_bufs.data_vec.clear();
        for maybe_page in page_reader {
            let page = maybe_page?;
            let page = decompress(page, &mut self.decompress_buffer)?;

            match page {
                Page::Dict(page) => {
                    dict = Some(page);
                }
                Page::Data(page) => {
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
            let column_type = self.columns[column_idx].column_type;
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
        let column_type = self.columns[timestamp_column_index].column_type;
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
                        &mut FixedInt2ShortColumnSink::new(
                            &mut DataPageFixedSlicer::<4>::new(values_buffer, row_count),
                            bufs,
                            &SHORT_NULL,
                        ),
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
                        &mut FixedInt2ShortColumnSink::new(
                            &mut DeltaBinaryPackedSlicer::<2>::try_new(values_buffer, row_count)?,
                            bufs,
                            &SHORT_NULL,
                        ),
                    )?;
                    Ok(())
                }
                (Encoding::Plain, _, _, ColumnTypeTag::Byte | ColumnTypeTag::GeoByte) => {
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut FixedInt2ByteColumnSink::new(
                            &mut DataPageFixedSlicer::<4>::new(values_buffer, row_count),
                            bufs,
                            &BYTE_NULL,
                        ),
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
                        &mut FixedInt2ByteColumnSink::new(
                            &mut DeltaBinaryPackedSlicer::<1>::try_new(values_buffer, row_count)?,
                            bufs,
                            &BYTE_NULL,
                        ),
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
                        &mut FixedIntColumnSink::new(
                            &mut DataPageFixedSlicer::<4>::new(values_buffer, row_count),
                            bufs,
                            &INT_NULL,
                        ),
                    )?;
                    Ok(())
                }
                (Encoding::Plain, _, _, ColumnTypeTag::Date) => {
                    decode_page0(
                        page,
                        row_lo,
                        row_hi,
                        &mut FixedLongColumnSink::new(
                            &mut ValueConvertSlicer::<8, _, _>::new(
                                DataPageFixedSlicer::<4>::new(values_buffer, row_count),
                                |int_val: &[u8], buff: &mut [u8; 8]| {
                                    let days_since_epoch = unsafe {
                                        ptr::read_unaligned(int_val.as_ptr() as *const i32)
                                    };
                                    let date = days_since_epoch as i64 * 24 * 60 * 60 * 1000;
                                    buff.copy_from_slice(&date.to_le_bytes());
                                },
                            ),
                            bufs,
                            &LONG_NULL,
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
                        &mut FixedIntColumnSink::new(
                            &mut DeltaBinaryPackedSlicer::<4>::try_new(values_buffer, row_count)?,
                            bufs,
                            &INT_NULL,
                        ),
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
                        &mut FixedLongColumnSink::new(
                            &mut DataPageFixedSlicer::<8>::new(values_buffer, row_count),
                            bufs,
                            &LONG_NULL,
                        ),
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
                        &mut FixedLongColumnSink::new(
                            &mut DeltaBinaryPackedSlicer::<8>::try_new(values_buffer, row_count)?,
                            bufs,
                            &LONG_NULL,
                        ),
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
                        &mut FixedLong128ColumnSink::new(
                            &mut DataPageFixedSlicer::<16>::new(values_buffer, row_count),
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
                    &mut FixedDoubleColumnSink::new(
                        &mut DataPageFixedSlicer::<8>::new(values_buffer, row_count),
                        bufs,
                        &DOUBLE_NULL,
                    ),
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
                        &mut FixedFloatColumnSink::new(
                            &mut DataPageFixedSlicer::<4>::new(values_buffer, row_count),
                            bufs,
                            &FLOAT_NULL,
                        ),
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
    sink.reserve()?;
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
                    while let Some(item) = iter.next() {
                        if item {
                            sink.push()?;
                        } else {
                            sink.push_null()?;
                        }
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

fn decode_null_bitmap(
    page: &DataPage,
    count: usize,
) -> ParquetResult<Option<HybridDecoderBitmapIter<'_>>> {
    let def_levels = split_buffer(page)?.1;
    if def_levels.is_empty() {
        return Ok(None);
    }

    let iter = hybrid_rle::Decoder::new(def_levels, 1);
    let iter = HybridDecoderBitmapIter::new(iter, count);
    Ok(Some(iter))
}

#[allow(clippy::while_let_on_iterator)]
fn decode_array_page<T: DataPageSlicer>(
    page: &DataPage,
    row_lo: usize,
    row_hi: usize,
    slicer: &mut T,
    buffers: &mut ColumnChunkBuffers,
) -> ParquetResult<()> {
    let count = slicer.count();
    buffers.aux_vec.reserve(count * ARRAY_AUX_SIZE)?;
    buffers.data_vec.reserve(slicer.data_size())?;

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
    let mut row = 0;
    let mut skip_count = row_lo;
    while let Some(levels) = levels_iter.next_levels() {
        let levels = levels?;
        // process levels accumulated for an array
        if skip_count == 0 {
            append_array(
                &mut buffers.aux_vec,
                &mut buffers.data_vec,
                max_rep_level as u32,
                max_def_level as u32,
                &levels.rep_levels,
                &levels.def_levels,
                slicer,
            )?;
        } else {
            skip_array(slicer, max_def_level as u32, &levels.def_levels);
            skip_count -= 1;
        }
        row += 1;
        if row >= row_hi {
            return Ok(());
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
) -> ParquetResult<()> {
    if def_levels.len() == 1 && def_levels[0] == 0 {
        append_array_null(aux_mem, data_mem)?;
    } else {
        let shape_size = align8b(4 * max_rep_level as usize);
        let value_size = shape_size + 8 * rep_levels.len();

        aux_mem.extend_from_slice(&data_mem.len().to_le_bytes())?;
        aux_mem.extend_from_slice(&value_size.to_le_bytes())?;

        // first, calculate and write shape
        let mut shape = [0_u32; ARRAY_NDIMS_LIMIT];
        calculate_array_shape(&mut shape, max_rep_level, rep_levels);
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
        if max_rep_level % 2 != 0 {
            data_mem.extend_from_slice(&0_u32.to_le_bytes())?;
        }

        // next, copy elements
        data_mem.reserve(value_size)?;
        for &def_level in def_levels {
            if def_level == max_def_level {
                data_mem.extend_from_slice(slicer.next())?;
            } else {
                data_mem.extend_from_slice(&f64::NAN.to_le_bytes())?;
            }
        }
    }
    Ok(())
}

fn skip_array<T: DataPageSlicer>(slicer: &mut T, max_def_level: u32, def_levels: &[u32]) {
    for &def_level in def_levels {
        if def_level == max_def_level {
            slicer.next();
        }
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
    use crate::parquet_read::{ColumnChunkBuffers, ParquetDecoder};
    use crate::parquet_write::array::{append_array_null, append_raw_array};
    use crate::parquet_write::file::ParquetWriter;
    use crate::parquet_write::schema::{Column, Partition};
    use crate::parquet_write::varchar::{append_varchar, append_varchar_null};
    use arrow::datatypes::ToByteSlice;
    use bytes::Bytes;
    use parquet::file::reader::Length;
    use parquet2::write::Version;
    use qdb_core::col_type::{encode_array_type, ColumnType, ColumnTypeTag};
    use rand::Rng;
    use std::fs::File;
    use std::io::{Cursor, Write};
    use std::mem::size_of;
    use std::path::Path;
    use std::ptr::null;
    use tempfile::NamedTempFile;

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

        let file_len = file.len();
        let mut decoder = ParquetDecoder::read(allocator.clone(), file, file_len).unwrap();
        assert_eq!(decoder.columns.len(), column_count);
        assert_eq!(decoder.row_count, row_count);
        let row_group_count = decoder.row_group_count as usize;
        let bufs = &mut ColumnChunkBuffers::new(allocator.clone());

        for column_index in 0..column_count {
            let column_type = decoder.columns[column_index].column_type;
            let col_info = QdbMetaCol { column_type, column_top: 0, format: None };
            for row_group_index in 0..row_group_count {
                decoder
                    .decode_column_chunk(
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

        let file_len = file.len();
        let mut decoder = ParquetDecoder::read(allocator.clone(), file, file_len).unwrap();
        assert_eq!(decoder.columns.len(), column_count);
        assert_eq!(decoder.row_count, row_count);
        let row_group_count = decoder.row_group_count as usize;
        let bufs = &mut ColumnChunkBuffers::new(allocator);

        for row_lo in 0..row_group_size - 1 {
            for row_hi in row_lo + 1..row_group_size {
                for column_index in 0..column_count {
                    let column_type = decoder.columns[column_index].column_type;
                    let col_info = QdbMetaCol { column_type, column_top: 0, format: None };
                    for row_group_index in 0..row_group_count {
                        decoder
                            .decode_column_chunk(
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

        let file_len = file.len();
        let mut decoder = ParquetDecoder::read(allocator.clone(), file, file_len).unwrap();
        assert_eq!(decoder.columns.len(), column_count);
        assert_eq!(decoder.row_count, row_count);
        let row_group_count = decoder.row_group_count as usize;
        let bufs = &mut ColumnChunkBuffers::new(allocator.clone());

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
    ) -> File {
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
    ) -> File {
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

        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();
        let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");
        temp_file
            .write_all(bytes.to_byte_slice())
            .expect("Failed to write to temp file");

        let path = temp_file.path().to_str().unwrap();
        let file = File::open(Path::new(path)).unwrap();
        file
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
        let mut rng = rand::thread_rng();

        let len = 1 + rng.gen_range(0..len - 1);

        // 0x00A0..0xD7FF generates a random Unicode scalar value in a range that includes non-ASCII characters
        let range = if rng.gen_bool(0.5) {
            0x00A0..0xD7FF
        } else {
            33..126
        };

        let random_string: String = (0..len)
            .map(|_| {
                let c = rng.gen_range(range.clone());
                char::from_u32(c).unwrap_or('�') // Use a replacement character for invalid values
            })
            .collect();

        random_string
    }

    fn generate_random_binary(len: usize) -> Vec<u8> {
        let mut rng = rand::thread_rng();

        let len = 1 + rng.gen_range(0..len - 1);

        let random_bin: Vec<u8> = (0..len)
            .map(|_| {
                let u: u8 = rng.gen();
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
        )
        .unwrap()
    }
}
