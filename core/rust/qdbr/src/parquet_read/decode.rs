use crate::allocator::{AcVec, QdbAllocator};
use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet::qdb_metadata::{QdbMetaCol, QdbMetaColFormat};
use crate::parquet_read::column_sink::var::{
    BinaryColumnSink, RawArrayColumnSink, StringColumnSink, VarcharColumnSink,
    VarcharSliceColumnSink, VarcharSliceSpillSink,
};
use crate::parquet_read::column_sink::Pushable;
use crate::parquet_read::decode::decimal::{
    decode_byte_array_decimal_dict_mode, decode_byte_array_decimal_mode,
    decode_fixed_decimal_dict_mode, decode_fixed_decimal_mode,
};
use crate::parquet_read::decoders::int128::Int128ToUuidConverter;
use crate::parquet_read::decoders::int96::{Int96Timestamp, Int96ToTimestampConverter};
use crate::parquet_read::decoders::{
    int32::DayToMillisConverter, int32::Int32ToDoubleConverter, BasePrimitiveDictDecoder,
    BaseVarDictDecoder, ConvertablePrimitiveDictDecoder, DeltaBinaryPackedDecoder,
    DeltaLAVarcharSliceDecoder, FixedDictDecoder, FloatToIntRangeCheckConverter,
    PlainBooleanDecoder, PlainPrimitiveDecoder, PrimitiveConverter, RleBooleanDecoder,
    RleDictVarcharSliceDecoder, RleDictionaryDecoder, RleLocalIsGlobalSymbolDictDecoder,
    F32_MAX_SAFE_FOR_I32, F32_MAX_SAFE_FOR_I64, F64_MAX_SAFE_FOR_I64,
};
use crate::parquet_read::page::{split_buffer, DataPage, DictPage};
use crate::parquet_read::slicer::rle::RleDictionarySlicer;
use crate::parquet_read::slicer::{
    DataPageFixedSlicer, DataPageSlicer, DeltaBytesArraySlicer, DeltaLengthArraySlicer,
    PlainVarSlicer,
};
use crate::parquet_read::ColumnChunkBuffers;
use parquet2::deserialize::{HybridDecoderBitmapIter, HybridEncoded};

use parquet2::encoding::hybrid_rle::HybridRleDecoder;
use parquet2::encoding::{hybrid_rle, Encoding};
use parquet2::page::DataPageHeader;
use parquet2::read::levels::get_bit_width;
use parquet2::read::{SlicedDataPage, SlicedDictPage};
use parquet2::schema::types::{PhysicalType, PrimitiveConvertedType, PrimitiveLogicalType};
use qdb_core::col_type::{nulls, ColumnType, ColumnTypeTag, Long128, Long256};
use std::cmp::min;
use std::ptr;

mod array;
mod decimal;

use self::array::{decode_array_page, decode_array_page_filtered};

impl ColumnChunkBuffers {
    pub fn new(allocator: QdbAllocator) -> Self {
        Self {
            data_vec: AcVec::new_in(allocator.clone()),
            data_ptr: ptr::null_mut(),
            data_size: 0,
            aux_vec: AcVec::new_in(allocator),
            aux_ptr: ptr::null_mut(),
            aux_size: 0,
            page_buffers_size: 0,
            page_buffers: Vec::new(),
            column_top: 0,
            page_buffers_charged: 0,
            page_buffers_counted: 0,
        }
    }

    // Unconditional re-read so a hypothetical second call between resets cannot keep
    // a stale ptr after a Vec reallocation. Returns an error when charging the
    // retained VarcharSlice page bytes against the per-query memory tracker
    // crosses the configured limit.
    pub fn refresh_ptrs(&mut self) -> ParquetResult<()> {
        // Always recompute the exposed pointer/size from the backing vectors.
        // Appending additional column chunks into the same buffer (see
        // decode_row_group_range, which decodes a run of row groups without
        // resetting between them) can reallocate data_vec/aux_vec, moving the
        // allocation and invalidating any pointer cached by an earlier refresh.
        // The vectors are the single source of truth, so recomputing every time
        // is correct and yields identical results for the single-chunk callers.
        self.data_size = self.data_vec.len();
        self.data_ptr = self.data_vec.as_mut_ptr();
        self.aux_size = self.aux_vec.len();
        self.aux_ptr = self.aux_vec.as_mut_ptr();

        // Sum of decompressed page/dict buffer bytes referenced by VarcharSlice aux entries.
        // Appends only grow page_buffers (a multi-row-group decode concatenates into the same
        // buffers without resetting), and already-counted entries are immutable, so add only
        // the buffers appended since the last refresh instead of re-summing the whole vector
        // -- otherwise a run of N row groups costs O(N^2). A shrink (truncate/partial drain)
        // falls back to a full recompute; reset() zeroes both fields.
        if self.page_buffers.len() < self.page_buffers_counted {
            self.page_buffers_size = self.page_buffers.iter().map(Vec::len).sum();
        } else {
            for buf in &self.page_buffers[self.page_buffers_counted..] {
                self.page_buffers_size += buf.len();
            }
        }
        self.page_buffers_counted = self.page_buffers.len();

        // Reconcile the per-query tracker charge to the retained payload. A net
        // growth is checked against the limit and may breach; a net shrink is
        // always credited. See `page_buffers_charged`.
        self.reconcile_page_buffers_charge()
    }

    // Charges or credits the per-query tracker so that exactly
    // `page_buffers_size` bytes stay reserved for this chunk's retained payload.
    // The growth path can breach the limit and return an error; the shrink path
    // never fails. On a breach `page_buffers_charged` is left at the prior value
    // so `reset`/`Drop` later credit only what was actually charged.
    fn reconcile_page_buffers_charge(&mut self) -> ParquetResult<()> {
        let target = self.page_buffers_size;
        if target > self.page_buffers_charged {
            let delta = target - self.page_buffers_charged;
            self.data_vec.allocator().charge_tracked(delta)?;
            self.page_buffers_charged = target;
        } else if target < self.page_buffers_charged {
            let delta = self.page_buffers_charged - target;
            self.data_vec.allocator().credit_tracked(delta);
            self.page_buffers_charged = target;
        }
        Ok(())
    }

    // Releases the whole per-query tracker charge held for `page_buffers`. Used
    // by `reset` and `Drop`, both of which empty (or hand off to the reuse pool)
    // the retained payload.
    fn release_page_buffers_charge(&mut self) {
        if self.page_buffers_charged > 0 {
            self.data_vec
                .allocator()
                .credit_tracked(self.page_buffers_charged);
            self.page_buffers_charged = 0;
        }
    }

    // Callers drain `page_buffers` (into a reuse pool) before invoking; this only clears
    // the outer Vec and the inner data/aux vectors. The inner Vecs keep their capacity
    // so the next decode can grow into them via realloc only when the new chunk exceeds
    // the buffer's historical peak.
    pub fn reset(&mut self) {
        self.data_vec.clear();
        self.data_size = 0;
        self.data_ptr = ptr::null_mut();

        self.aux_vec.clear();
        self.aux_size = 0;
        self.aux_ptr = ptr::null_mut();

        self.release_page_buffers_charge();
        self.page_buffers_size = 0;
        self.page_buffers.clear();
        self.page_buffers_counted = 0;
        self.column_top = 0;
    }
}

impl Drop for ColumnChunkBuffers {
    // The `data_vec` / `aux_vec` `AcVec`s credit the per-query tracker through
    // their own allocator on drop; `page_buffers` is a system-allocated `Vec`,
    // so its charge must be credited explicitly here to keep the tracker
    // balanced when a cursor closes without a final `reset`.
    fn drop(&mut self) {
        self.release_page_buffers_charge();
    }
}

#[derive(Clone, Copy)]
struct FilterDecodeContext<'a> {
    page_row_start: usize,
    page_row_count: usize,
    row_group_lo: usize,
    rows_filter: &'a [i64],
}

#[derive(Clone, Copy)]
struct DecodeModeContext<'a> {
    row_lo: usize,
    row_hi: usize,
    filter: Option<FilterDecodeContext<'a>>,
}

impl<'a> DecodeModeContext<'a> {
    #[inline]
    fn unfiltered(row_lo: usize, row_hi: usize) -> Self {
        Self { row_lo, row_hi, filter: None }
    }

    #[inline]
    fn filtered(row_lo: usize, row_hi: usize, filter: FilterDecodeContext<'a>) -> Self {
        Self { row_lo, row_hi, filter: Some(filter) }
    }

    #[inline]
    fn output_row_count(self) -> usize {
        self.row_hi - self.row_lo
    }

    #[inline]
    fn source_row_count(self) -> usize {
        match self.filter {
            Some(filter) => filter.page_row_count,
            None => self.row_hi,
        }
    }

    #[inline]
    fn sliced_row_count(self) -> usize {
        match self.filter {
            Some(filter) => filter.page_row_count,
            None => self.output_row_count(),
        }
    }

    #[inline]
    fn filtered_context(self) -> FilterDecodeContext<'a> {
        self.filter
            .expect("filtered decode context missing for filtered mode")
    }
}

#[inline(always)]
fn clear_aux_buffers(bufs: &mut ColumnChunkBuffers) {
    bufs.aux_vec.clear();
    bufs.aux_ptr = ptr::null_mut();
}

#[inline(always)]
fn decode_page0_mode<T: Pushable, const FILTERED: bool, const FILL_NULLS: bool>(
    page: &DataPage,
    mode: DecodeModeContext<'_>,
    sink: &mut T,
) -> ParquetResult<()> {
    if FILTERED {
        let filter = mode.filtered_context();
        decode_page0_filtered::<_, FILL_NULLS>(
            page,
            filter.page_row_start,
            filter.page_row_count,
            filter.row_group_lo,
            mode.row_lo,
            mode.row_hi,
            filter.rows_filter,
            sink,
        )
    } else {
        decode_page0(page, mode.row_lo, mode.row_hi, sink)
    }
}

#[inline(always)]
fn decode_array_page_mode<T: DataPageSlicer, const FILTERED: bool, const FILL_NULLS: bool>(
    page: &DataPage,
    mode: DecodeModeContext<'_>,
    slicer: &mut T,
    bufs: &mut ColumnChunkBuffers,
) -> ParquetResult<()> {
    if FILTERED {
        let filter = mode.filtered_context();
        decode_array_page_filtered::<_, FILL_NULLS>(
            page,
            filter.page_row_start,
            filter.page_row_count,
            filter.row_group_lo,
            mode.row_lo,
            mode.row_hi,
            filter.rows_filter,
            slicer,
            bufs,
        )
    } else {
        decode_array_page(page, mode.row_lo, mode.row_hi, slicer, bufs)
    }
}

/// Decode a filtered data page.
/// - `FILL_NULLS = false`: skip rows not in filter
/// - `FILL_NULLS = true`: fill nulls for rows not in filter
#[allow(clippy::too_many_arguments)]
pub(super) fn decode_page_filtered<const FILL_NULLS: bool>(
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

    let mode = DecodeModeContext::filtered(
        row_lo,
        row_hi,
        FilterDecodeContext {
            page_row_start,
            page_row_count,
            row_group_lo,
            rows_filter,
        },
    );
    decode_page_dispatch::<true, FILL_NULLS>(page, dict, bufs, col_info, mode)
}

pub fn decode_page(
    page: &DataPage,
    dict: Option<&DictPage>,
    bufs: &mut ColumnChunkBuffers,
    col_info: QdbMetaCol,
    row_lo: usize,
    row_hi: usize,
) -> ParquetResult<()> {
    let mode = DecodeModeContext::unfiltered(row_lo, row_hi);
    decode_page_dispatch::<false, false>(page, dict, bufs, col_info, mode)
}

fn decode_page_dispatch<const FILTERED: bool, const FILL_NULLS: bool>(
    page: &DataPage,
    dict: Option<&DictPage>,
    bufs: &mut ColumnChunkBuffers,
    col_info: QdbMetaCol,
    mode: DecodeModeContext<'_>,
) -> ParquetResult<()> {
    let (_rep_levels, _, values_buffer) = split_buffer(page)?;
    let column_type = col_info.column_type;

    let primitive_type = &page.descriptor.primitive_type;
    let supported = match primitive_type.physical_type {
        PhysicalType::Int32 => decode_int32_dispatch::<FILTERED, FILL_NULLS>(
            page,
            dict,
            values_buffer,
            bufs,
            column_type,
            mode,
        ),
        PhysicalType::Int64 => decode_int64_dispatch::<FILTERED, FILL_NULLS>(
            page,
            dict,
            values_buffer,
            bufs,
            column_type,
            mode,
        ),
        PhysicalType::FixedLenByteArray(len) => decode_fixed_len_dispatch::<FILTERED, FILL_NULLS>(
            page,
            dict,
            values_buffer,
            bufs,
            column_type,
            len,
            mode,
        ),
        PhysicalType::ByteArray => decode_byte_array_dispatch::<FILTERED, FILL_NULLS>(
            page,
            dict,
            values_buffer,
            bufs,
            col_info,
            column_type,
            mode,
        ),
        PhysicalType::Int96 => decode_int96_dispatch::<FILTERED, FILL_NULLS>(
            page,
            dict,
            values_buffer,
            bufs,
            column_type,
            mode,
        ),
        PhysicalType::Double => decode_double_dispatch::<FILTERED, FILL_NULLS>(
            page,
            dict,
            values_buffer,
            bufs,
            column_type,
            mode,
        ),
        typ => decode_other_fixed_dispatch::<FILTERED, FILL_NULLS>(
            page,
            dict,
            values_buffer,
            bufs,
            column_type,
            typ,
            mode,
        ),
    }?;

    if supported {
        Ok(())
    } else if FILTERED {
        Err(fmt_err!(
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
        ))
    } else {
        Err(fmt_err!(
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
        ))
    }
}

fn decode_int32_dispatch<const FILTERED: bool, const FILL_NULLS: bool>(
    page: &DataPage,
    dict: Option<&DictPage>,
    values_buffer: &[u8],
    bufs: &mut ColumnChunkBuffers,
    column_type: ColumnType,
    mode: DecodeModeContext<'_>,
) -> ParquetResult<bool> {
    let row_hi = mode.source_row_count();
    let logical_type = page.descriptor.primitive_type.logical_type;
    let converted_type = page.descriptor.primitive_type.converted_type;
    match (page.encoding(), dict, column_type.tag()) {
        (Encoding::Plain, _, ColumnTypeTag::Byte) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::<i32, i8>::new(values_buffer, bufs, nulls::BYTE),
            )?;
            Ok(true)
        }
        (Encoding::Plain, _, ColumnTypeTag::GeoByte) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::<i32, i8>::new(
                    values_buffer,
                    bufs,
                    nulls::GEOHASH_BYTE,
                ),
            )?;
            Ok(true)
        }
        (Encoding::Plain, _, ColumnTypeTag::Decimal8) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::<i32, i8>::new(values_buffer, bufs, nulls::DECIMAL8),
            )?;
            Ok(true)
        }
        (Encoding::DeltaBinaryPacked, _, ColumnTypeTag::Byte) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut DeltaBinaryPackedDecoder::<i8, i32>::try_new(
                    values_buffer,
                    bufs,
                    nulls::BYTE,
                )?,
            )?;
            Ok(true)
        }
        (Encoding::DeltaBinaryPacked, _, ColumnTypeTag::GeoByte) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut DeltaBinaryPackedDecoder::<i8, i32>::try_new(
                    values_buffer,
                    bufs,
                    nulls::GEOHASH_BYTE,
                )?,
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            ColumnTypeTag::Byte,
        ) => {
            let dict_decoder = BasePrimitiveDictDecoder::<i32, i8>::try_new(dict_page)?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::BYTE,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            ColumnTypeTag::GeoByte,
        ) => {
            let dict_decoder = BasePrimitiveDictDecoder::<i32, i8>::try_new(dict_page)?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::GEOHASH_BYTE,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            ColumnTypeTag::Decimal8,
        ) => {
            let dict_decoder = BasePrimitiveDictDecoder::<i32, i8>::try_new(dict_page)?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::DECIMAL8,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        (Encoding::Plain, _, ColumnTypeTag::Short | ColumnTypeTag::Char) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::<i32, i16>::new(values_buffer, bufs, nulls::SHORT),
            )?;
            Ok(true)
        }
        (Encoding::Plain, _, ColumnTypeTag::GeoShort) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::<i32, i16>::new(
                    values_buffer,
                    bufs,
                    nulls::GEOHASH_SHORT,
                ),
            )?;
            Ok(true)
        }
        (Encoding::Plain, _, ColumnTypeTag::Decimal16) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::<i32, i16>::new(values_buffer, bufs, nulls::DECIMAL16),
            )?;
            Ok(true)
        }
        (Encoding::DeltaBinaryPacked, _, ColumnTypeTag::Short | ColumnTypeTag::Char) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut DeltaBinaryPackedDecoder::<i16, i32>::try_new(
                    values_buffer,
                    bufs,
                    nulls::SHORT,
                )?,
            )?;
            Ok(true)
        }
        (Encoding::DeltaBinaryPacked, _, ColumnTypeTag::GeoShort) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut DeltaBinaryPackedDecoder::<i16, i32>::try_new(
                    values_buffer,
                    bufs,
                    nulls::GEOHASH_SHORT,
                )?,
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            ColumnTypeTag::Short | ColumnTypeTag::Char,
        ) => {
            let dict_decoder = BasePrimitiveDictDecoder::<i32, i16>::try_new(dict_page)?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::SHORT,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            ColumnTypeTag::GeoShort,
        ) => {
            let dict_decoder = BasePrimitiveDictDecoder::<i32, i16>::try_new(dict_page)?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::GEOHASH_SHORT,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            ColumnTypeTag::Decimal16,
        ) => {
            let dict_decoder = BasePrimitiveDictDecoder::<i32, i16>::try_new(dict_page)?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::DECIMAL16,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        (Encoding::Plain, _, ColumnTypeTag::Int) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::<i32>::new(values_buffer, bufs, nulls::INT),
            )?;
            Ok(true)
        }
        (Encoding::Plain, _, ColumnTypeTag::IPv4) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::<i32>::new(values_buffer, bufs, nulls::IPV4),
            )?;
            Ok(true)
        }
        (Encoding::Plain, _, ColumnTypeTag::GeoInt) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::<i32>::new(values_buffer, bufs, nulls::GEOHASH_INT),
            )?;
            Ok(true)
        }
        (Encoding::Plain, _, ColumnTypeTag::Decimal32) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::<i32>::new(values_buffer, bufs, nulls::DECIMAL32),
            )?;
            Ok(true)
        }
        (Encoding::DeltaBinaryPacked, _, ColumnTypeTag::Int) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut DeltaBinaryPackedDecoder::<i32, i32>::try_new(
                    values_buffer,
                    bufs,
                    nulls::INT,
                )?,
            )?;
            Ok(true)
        }
        (Encoding::DeltaBinaryPacked, _, ColumnTypeTag::IPv4) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut DeltaBinaryPackedDecoder::<i32, i32>::try_new(
                    values_buffer,
                    bufs,
                    nulls::IPV4,
                )?,
            )?;
            Ok(true)
        }
        (Encoding::DeltaBinaryPacked, _, ColumnTypeTag::GeoInt) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut DeltaBinaryPackedDecoder::<i32, i32>::try_new(
                    values_buffer,
                    bufs,
                    nulls::GEOHASH_INT,
                )?,
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            ColumnTypeTag::Int,
        ) => {
            let dict_decoder = BasePrimitiveDictDecoder::<i32, i32>::try_new(dict_page)?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::INT,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            ColumnTypeTag::IPv4,
        ) => {
            let dict_decoder = BasePrimitiveDictDecoder::<i32, i32>::try_new(dict_page)?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::IPV4,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            ColumnTypeTag::GeoInt,
        ) => {
            let dict_decoder = BasePrimitiveDictDecoder::<i32, i32>::try_new(dict_page)?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::GEOHASH_INT,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            ColumnTypeTag::Decimal32,
        ) => {
            let dict_decoder = BasePrimitiveDictDecoder::<i32, i32>::try_new(dict_page)?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::DECIMAL32,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        (Encoding::Plain, _, ColumnTypeTag::Date)
            if matches!(logical_type, Some(PrimitiveLogicalType::Date))
                || matches!(converted_type, Some(PrimitiveConvertedType::Date)) =>
        {
            // Parquet-native DATE column: Int32 stores days since epoch, convert to millis.
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::<i32, i64, DayToMillisConverter>::new_with(
                    values_buffer,
                    bufs,
                    nulls::TIMESTAMP,
                    DayToMillisConverter::new(),
                ),
            )?;
            Ok(true)
        }
        (Encoding::Plain, _, ColumnTypeTag::Date) => {
            // Type conversion (e.g. INT to DATE): plain i32 to i64 widening.
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::<i32, i64>::new(values_buffer, bufs, nulls::TIMESTAMP),
            )?;
            Ok(true)
        }
        (Encoding::RleDictionary | Encoding::PlainDictionary, Some(_), ColumnTypeTag::Date)
            if matches!(logical_type, Some(PrimitiveLogicalType::Date))
                || matches!(converted_type, Some(PrimitiveConvertedType::Date)) =>
        {
            let dict_page = dict.unwrap();
            // Parquet-native DATE column with dictionary encoding.
            let dict_decoder =
                ConvertablePrimitiveDictDecoder::try_new(dict_page, DayToMillisConverter::new())?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::TIMESTAMP,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            ColumnTypeTag::Date,
        ) => {
            // Type conversion with dictionary encoding: plain i32→i64 widening.
            let dict_decoder = ConvertablePrimitiveDictDecoder::<i32, i64, _>::try_new(
                dict_page,
                PrimitiveConverter::new(),
            )?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::TIMESTAMP,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        (encoding, dict, ColumnTypeTag::Double) => {
            let scale = match logical_type {
                Some(PrimitiveLogicalType::Decimal(_, scale)) => scale,
                _ => match converted_type {
                    Some(PrimitiveConvertedType::Decimal(_, scale)) => scale,
                    _ => 0,
                },
            };

            match (encoding, dict) {
                (Encoding::RleDictionary | Encoding::PlainDictionary, Some(dict_page)) => {
                    let dict_decoder = ConvertablePrimitiveDictDecoder::try_new(
                        dict_page,
                        Int32ToDoubleConverter::try_new(scale)?,
                    )?;
                    decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                        page,
                        mode,
                        &mut RleDictionaryDecoder::try_new(
                            values_buffer,
                            dict_decoder,
                            row_hi,
                            nulls::DOUBLE,
                            bufs,
                        )?,
                    )?;
                    Ok(true)
                }
                (Encoding::Plain, _) => {
                    decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                        page,
                        mode,
                        &mut PlainPrimitiveDecoder::new_with(
                            values_buffer,
                            bufs,
                            nulls::DOUBLE,
                            Int32ToDoubleConverter::try_new(scale)?,
                        ),
                    )?;
                    Ok(true)
                }
                _ => Ok(false),
            }
        }
        // -- Type conversion arms: Int32 physical to wider/different QDB type --
        (Encoding::Plain, _, ColumnTypeTag::Long | ColumnTypeTag::Timestamp) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::<i32, i64>::new(values_buffer, bufs, nulls::LONG),
            )?;
            Ok(true)
        }
        (Encoding::DeltaBinaryPacked, _, ColumnTypeTag::Long | ColumnTypeTag::Timestamp) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut DeltaBinaryPackedDecoder::<i64, i32>::try_new(
                    values_buffer,
                    bufs,
                    nulls::LONG,
                )?,
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            ColumnTypeTag::Long | ColumnTypeTag::Timestamp,
        ) => {
            let dict_decoder = ConvertablePrimitiveDictDecoder::<i32, i64, _>::try_new(
                dict_page,
                PrimitiveConverter::new(),
            )?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::LONG,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        (Encoding::Plain, _, ColumnTypeTag::Float) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::<i32, f32>::new(values_buffer, bufs, nulls::FLOAT),
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            ColumnTypeTag::Float,
        ) => {
            let dict_decoder = ConvertablePrimitiveDictDecoder::<i32, f32, _>::try_new(
                dict_page,
                PrimitiveConverter::new(),
            )?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::FLOAT,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        // -- Type conversion: Int32 to Boolean (truncate to i8) --
        (Encoding::Plain, _, ColumnTypeTag::Boolean) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::<i32, i8>::new(values_buffer, bufs, nulls::BYTE),
            )?;
            Ok(true)
        }
        (Encoding::DeltaBinaryPacked, _, ColumnTypeTag::Boolean) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut DeltaBinaryPackedDecoder::<i8, i32>::try_new(
                    values_buffer,
                    bufs,
                    nulls::BYTE,
                )?,
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            ColumnTypeTag::Boolean,
        ) => {
            let dict_decoder = BasePrimitiveDictDecoder::<i32, i8>::try_new(dict_page)?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::BYTE,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn decode_int64_dispatch<const FILTERED: bool, const FILL_NULLS: bool>(
    page: &DataPage,
    dict: Option<&DictPage>,
    values_buffer: &[u8],
    bufs: &mut ColumnChunkBuffers,
    column_type: ColumnType,
    mode: DecodeModeContext<'_>,
) -> ParquetResult<bool> {
    let row_hi = mode.source_row_count();
    match (page.encoding(), dict, column_type.tag()) {
        (Encoding::Plain, _, ColumnTypeTag::Decimal64) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::<i64>::new(values_buffer, bufs, nulls::DECIMAL64),
            )?;
            Ok(true)
        }
        (
            Encoding::Plain,
            _,
            ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp,
        ) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::<i64>::new(values_buffer, bufs, nulls::LONG),
            )?;
            Ok(true)
        }
        (Encoding::Plain, _, ColumnTypeTag::GeoLong) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::<i64>::new(values_buffer, bufs, nulls::GEOHASH_LONG),
            )?;
            Ok(true)
        }
        (Encoding::DeltaBinaryPacked, _, ColumnTypeTag::Decimal64) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut DeltaBinaryPackedDecoder::<i64, i64>::try_new(
                    values_buffer,
                    bufs,
                    nulls::DECIMAL64,
                )?,
            )?;
            Ok(true)
        }
        (
            Encoding::DeltaBinaryPacked,
            _,
            ColumnTypeTag::Long | ColumnTypeTag::Timestamp | ColumnTypeTag::Date,
        ) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut DeltaBinaryPackedDecoder::<i64, i64>::try_new(
                    values_buffer,
                    bufs,
                    nulls::LONG,
                )?,
            )?;
            Ok(true)
        }
        (Encoding::DeltaBinaryPacked, _, ColumnTypeTag::GeoLong) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut DeltaBinaryPackedDecoder::<i64, i64>::try_new(
                    values_buffer,
                    bufs,
                    nulls::GEOHASH_LONG,
                )?,
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            ColumnTypeTag::Decimal64,
        ) => {
            let dict_decoder = BasePrimitiveDictDecoder::<i64, i64>::try_new(dict_page)?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::DECIMAL64,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            ColumnTypeTag::Long | ColumnTypeTag::Timestamp | ColumnTypeTag::Date,
        ) => {
            let dict_decoder = BasePrimitiveDictDecoder::<i64, i64>::try_new(dict_page)?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::LONG,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            ColumnTypeTag::GeoLong,
        ) => {
            let dict_decoder = BasePrimitiveDictDecoder::<i64, i64>::try_new(dict_page)?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::GEOHASH_LONG,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        // -- Type conversion arms: Int64 physical to narrower/different QDB type --
        (Encoding::Plain, _, ColumnTypeTag::Int) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::<i64, i32>::new(values_buffer, bufs, nulls::INT),
            )?;
            Ok(true)
        }
        (Encoding::DeltaBinaryPacked, _, ColumnTypeTag::Int) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut DeltaBinaryPackedDecoder::<i32, i64>::try_new(
                    values_buffer,
                    bufs,
                    nulls::INT,
                )?,
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            ColumnTypeTag::Int,
        ) => {
            let dict_decoder = BasePrimitiveDictDecoder::<i64, i32>::try_new(dict_page)?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::INT,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        (Encoding::Plain, _, ColumnTypeTag::Short) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::<i64, i16>::new(values_buffer, bufs, nulls::SHORT),
            )?;
            Ok(true)
        }
        (Encoding::DeltaBinaryPacked, _, ColumnTypeTag::Short) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut DeltaBinaryPackedDecoder::<i16, i64>::try_new(
                    values_buffer,
                    bufs,
                    nulls::SHORT,
                )?,
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            ColumnTypeTag::Short,
        ) => {
            let dict_decoder = BasePrimitiveDictDecoder::<i64, i16>::try_new(dict_page)?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::SHORT,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        (Encoding::Plain, _, ColumnTypeTag::Byte | ColumnTypeTag::Boolean) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::<i64, i8>::new(values_buffer, bufs, nulls::BYTE),
            )?;
            Ok(true)
        }
        (Encoding::DeltaBinaryPacked, _, ColumnTypeTag::Byte | ColumnTypeTag::Boolean) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut DeltaBinaryPackedDecoder::<i8, i64>::try_new(
                    values_buffer,
                    bufs,
                    nulls::BYTE,
                )?,
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            ColumnTypeTag::Byte | ColumnTypeTag::Boolean,
        ) => {
            let dict_decoder = BasePrimitiveDictDecoder::<i64, i8>::try_new(dict_page)?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::BYTE,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        (Encoding::Plain, _, ColumnTypeTag::Double) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::<i64, f64>::new(values_buffer, bufs, nulls::DOUBLE),
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            ColumnTypeTag::Double,
        ) => {
            let dict_decoder = ConvertablePrimitiveDictDecoder::<i64, f64, _>::try_new(
                dict_page,
                PrimitiveConverter::new(),
            )?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::DOUBLE,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        // -- Type conversion: Int64 to Float (lossy widening) --
        (Encoding::Plain, _, ColumnTypeTag::Float) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::<i64, f32>::new(values_buffer, bufs, nulls::FLOAT),
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            ColumnTypeTag::Float,
        ) => {
            let dict_decoder = ConvertablePrimitiveDictDecoder::<i64, f32, _>::try_new(
                dict_page,
                PrimitiveConverter::new(),
            )?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::FLOAT,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn decode_fixed_len_dispatch<const FILTERED: bool, const FILL_NULLS: bool>(
    page: &DataPage,
    dict: Option<&DictPage>,
    values_buffer: &[u8],
    bufs: &mut ColumnChunkBuffers,
    column_type: ColumnType,
    len: usize,
    mode: DecodeModeContext<'_>,
) -> ParquetResult<bool> {
    match (len, column_type.tag()) {
        (16, ColumnTypeTag::Uuid) => match page.encoding() {
            Encoding::Plain => {
                decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                    page,
                    mode,
                    &mut PlainPrimitiveDecoder::<u128, u128, Int128ToUuidConverter>::new_with(
                        values_buffer,
                        bufs,
                        nulls::UUID,
                        Int128ToUuidConverter::new(),
                    ),
                )?;
                Ok(true)
            }
            Encoding::RleDictionary | Encoding::PlainDictionary => {
                let dict_decoder = ConvertablePrimitiveDictDecoder::try_new(
                    dict.ok_or_else(|| {
                        fmt_err!(
                            Unsupported,
                            "dictionary page required for dictionary encoding"
                        )
                    })?,
                    Int128ToUuidConverter::new(),
                )?;
                decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                    page,
                    mode,
                    &mut RleDictionaryDecoder::try_new(
                        values_buffer,
                        dict_decoder,
                        mode.source_row_count(),
                        nulls::UUID,
                        bufs,
                    )?,
                )?;
                Ok(true)
            }
            _ => Ok(false),
        },
        (
            src_len,
            ColumnTypeTag::Decimal8
            | ColumnTypeTag::Decimal16
            | ColumnTypeTag::Decimal32
            | ColumnTypeTag::Decimal64
            | ColumnTypeTag::Decimal128
            | ColumnTypeTag::Decimal256,
        ) => {
            match (page.encoding(), dict) {
                (Encoding::Plain, _) => decode_fixed_decimal_mode::<FILTERED, FILL_NULLS>(
                    page,
                    bufs,
                    values_buffer,
                    mode,
                    src_len,
                    column_type.tag(),
                )?,
                (Encoding::RleDictionary | Encoding::PlainDictionary, Some(dict_page)) => {
                    decode_fixed_decimal_dict_mode::<FILTERED, FILL_NULLS>(
                        page,
                        dict_page,
                        bufs,
                        values_buffer,
                        mode,
                        src_len,
                        column_type.tag(),
                    )?
                }
                _ => {
                    return Err(fmt_err!(
                        Unsupported,
                        "only Plain and dictionary encodings supported for FixedLenByteArray decimals, got {:?}",
                        page.encoding()
                    ))
                }
            }
            Ok(true)
        }
        _ => match (page.encoding(), len, column_type.tag()) {
            (Encoding::Plain, 16, ColumnTypeTag::Long128) => {
                decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                    page,
                    mode,
                    &mut PlainPrimitiveDecoder::<Long128, Long128>::new(
                        values_buffer,
                        bufs,
                        Long128::NULL,
                    ),
                )?;
                Ok(true)
            }
            (Encoding::RleDictionary | Encoding::PlainDictionary, 16, ColumnTypeTag::Long128) => {
                let dict_decoder = BasePrimitiveDictDecoder::<Long128, Long128>::try_new(
                    dict.ok_or_else(|| {
                        fmt_err!(
                            Unsupported,
                            "dictionary page required for dictionary encoding"
                        )
                    })?,
                )?;
                decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                    page,
                    mode,
                    &mut RleDictionaryDecoder::try_new(
                        values_buffer,
                        dict_decoder,
                        mode.source_row_count(),
                        Long128::NULL,
                        bufs,
                    )?,
                )?;
                Ok(true)
            }
            (Encoding::Plain, 32, ColumnTypeTag::Long256) => {
                decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                    page,
                    mode,
                    &mut PlainPrimitiveDecoder::<Long256, Long256>::new(
                        values_buffer,
                        bufs,
                        Long256::NULL,
                    ),
                )?;
                Ok(true)
            }
            (Encoding::RleDictionary | Encoding::PlainDictionary, 32, ColumnTypeTag::Long256) => {
                let dict_decoder = BasePrimitiveDictDecoder::<Long256, Long256>::try_new(
                    dict.ok_or_else(|| {
                        fmt_err!(
                            Unsupported,
                            "dictionary page required for dictionary encoding"
                        )
                    })?,
                )?;
                decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                    page,
                    mode,
                    &mut RleDictionaryDecoder::try_new(
                        values_buffer,
                        dict_decoder,
                        mode.source_row_count(),
                        Long256::NULL,
                        bufs,
                    )?,
                )?;
                Ok(true)
            }
            _ => Ok(false),
        },
    }
}

fn decode_byte_array_dispatch<const FILTERED: bool, const FILL_NULLS: bool>(
    page: &DataPage,
    dict: Option<&DictPage>,
    values_buffer: &[u8],
    bufs: &mut ColumnChunkBuffers,
    col_info: QdbMetaCol,
    column_type: ColumnType,
    mode: DecodeModeContext<'_>,
) -> ParquetResult<bool> {
    let row_hi = mode.source_row_count();
    let row_count = mode.sliced_row_count();

    match column_type.tag() {
        ColumnTypeTag::Decimal8
        | ColumnTypeTag::Decimal16
        | ColumnTypeTag::Decimal32
        | ColumnTypeTag::Decimal64
        | ColumnTypeTag::Decimal128
        | ColumnTypeTag::Decimal256 => {
            match (page.encoding(), dict) {
                (Encoding::Plain, _) => decode_byte_array_decimal_mode::<FILTERED, FILL_NULLS>(
                    page,
                    bufs,
                    values_buffer,
                    mode,
                    column_type.tag(),
                )?,
                (Encoding::RleDictionary | Encoding::PlainDictionary, Some(dict_page)) => {
                    decode_byte_array_decimal_dict_mode::<FILTERED, FILL_NULLS>(
                        page,
                        dict_page,
                        bufs,
                        values_buffer,
                        mode,
                        column_type.tag(),
                    )?
                }
                _ => {
                    return Err(fmt_err!(
            Unsupported,
            "only Plain and dictionary encodings supported for ByteArray decimals, got {:?}",
            page.encoding()
        ))
                }
            }
            Ok(true)
        }
        ColumnTypeTag::String
        | ColumnTypeTag::Varchar
        | ColumnTypeTag::VarcharSlice
        | ColumnTypeTag::Symbol => {
            let encoding = page.encoding();
            match (encoding, dict, column_type.tag()) {
                (Encoding::DeltaLengthByteArray, _, ColumnTypeTag::String) => {
                    let mut slicer =
                        DeltaLengthArraySlicer::try_new(values_buffer, row_hi, row_count)?;
                    decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                        page,
                        mode,
                        &mut StringColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(true)
                }
                (Encoding::DeltaLengthByteArray, _, ColumnTypeTag::Varchar) => {
                    let mut slicer =
                        DeltaLengthArraySlicer::try_new(values_buffer, row_hi, row_count)?;
                    decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                        page,
                        mode,
                        &mut VarcharColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(true)
                }
                (
                    Encoding::RleDictionary | Encoding::PlainDictionary,
                    Some(dict_page),
                    ColumnTypeTag::Varchar,
                ) => {
                    let dict_decoder = BaseVarDictDecoder::try_new(dict_page)?;
                    let mut slicer = RleDictionarySlicer::try_new(
                        values_buffer,
                        dict_decoder,
                        row_hi,
                        row_count,
                    )?;
                    decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                        page,
                        mode,
                        &mut VarcharColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(true)
                }
                (
                    Encoding::RleDictionary | Encoding::PlainDictionary,
                    dict_page,
                    ColumnTypeTag::String,
                ) => {
                    let dict_page = dict_page.ok_or_else(|| {
                        fmt_err!(
                            Unsupported,
                            "dictionary page required for dictionary encoding"
                        )
                    })?;
                    let dict_decoder = BaseVarDictDecoder::try_new(dict_page)?;
                    let mut slicer = RleDictionarySlicer::try_new(
                        values_buffer,
                        dict_decoder,
                        row_hi,
                        row_count,
                    )?;
                    decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                        page,
                        mode,
                        &mut StringColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(true)
                }
                (Encoding::Plain, _, ColumnTypeTag::String) => {
                    let mut slicer = PlainVarSlicer::new(values_buffer, row_count);
                    decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                        page,
                        mode,
                        &mut StringColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(true)
                }
                (Encoding::Plain, _, ColumnTypeTag::Varchar) => {
                    let mut slicer = PlainVarSlicer::new(values_buffer, row_count);
                    decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                        page,
                        mode,
                        &mut VarcharColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(true)
                }
                (Encoding::DeltaByteArray, _, ColumnTypeTag::Varchar) => {
                    let mut slicer =
                        DeltaBytesArraySlicer::try_new(values_buffer, row_hi, row_count)?;
                    decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                        page,
                        mode,
                        &mut VarcharColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(true)
                }
                (Encoding::DeltaLengthByteArray, _, ColumnTypeTag::VarcharSlice) => {
                    decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                        page,
                        mode,
                        &mut DeltaLAVarcharSliceDecoder::try_new(
                            values_buffer,
                            bufs,
                            col_info.ascii.unwrap_or(false),
                        )?,
                    )?;
                    Ok(true)
                }
                (
                    Encoding::RleDictionary | Encoding::PlainDictionary,
                    Some(dict_page),
                    ColumnTypeTag::VarcharSlice,
                ) => {
                    decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                        page,
                        mode,
                        &mut RleDictVarcharSliceDecoder::try_new(
                            values_buffer,
                            dict_page,
                            bufs,
                            col_info.ascii.unwrap_or(false),
                        )?,
                    )?;
                    Ok(true)
                }
                (Encoding::Plain, _, ColumnTypeTag::VarcharSlice) => {
                    let mut slicer = PlainVarSlicer::new(values_buffer, row_count);
                    decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                        page,
                        mode,
                        &mut VarcharSliceColumnSink::new(
                            &mut slicer,
                            bufs,
                            col_info.ascii.unwrap_or(false),
                        ),
                    )?;
                    Ok(true)
                }
                (Encoding::DeltaByteArray, _, ColumnTypeTag::VarcharSlice) => {
                    let mut slicer =
                        DeltaBytesArraySlicer::try_new(values_buffer, row_hi, row_count)?;
                    let mut spill_sink = VarcharSliceSpillSink::new(
                        &mut slicer,
                        bufs,
                        col_info.ascii.unwrap_or(false),
                    );
                    decode_page0_mode::<_, FILTERED, FILL_NULLS>(page, mode, &mut spill_sink)?;
                    // fixup_pointers deferred to end-of-chunk (see decode_column_chunk)
                    Ok(true)
                }
                (Encoding::RleDictionary, Some(dict_page), ColumnTypeTag::Symbol) => {
                    if col_info.format != Some(QdbMetaColFormat::LocalKeyIsGlobal) {
                        return Err(fmt_err!(
                            Unsupported,
                            "only special LocalKeyIsGlobal-encoded symbol columns are supported",
                        ));
                    }
                    let dict_decoder = RleLocalIsGlobalSymbolDictDecoder::new(dict_page);
                    decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                        page,
                        mode,
                        &mut RleDictionaryDecoder::try_new(
                            values_buffer,
                            dict_decoder,
                            row_hi,
                            nulls::SYMBOL,
                            bufs,
                        )?,
                    )?;
                    Ok(true)
                }
                _ => Ok(false),
            }
        }
        ColumnTypeTag::Binary | ColumnTypeTag::Array => {
            let encoding = page.encoding();
            match (encoding, dict, column_type.tag()) {
                (Encoding::Plain, _, ColumnTypeTag::Binary) => {
                    let mut slicer = PlainVarSlicer::new(values_buffer, row_count);
                    decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                        page,
                        mode,
                        &mut BinaryColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(true)
                }
                (Encoding::DeltaLengthByteArray, _, ColumnTypeTag::Binary) => {
                    let mut slicer =
                        DeltaLengthArraySlicer::try_new(values_buffer, row_hi, row_count)?;
                    decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                        page,
                        mode,
                        &mut BinaryColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(true)
                }
                (
                    Encoding::RleDictionary | Encoding::PlainDictionary,
                    Some(dict_page),
                    ColumnTypeTag::Binary,
                ) => {
                    let dict_decoder = BaseVarDictDecoder::try_new(dict_page)?;
                    let mut slicer = RleDictionarySlicer::try_new(
                        values_buffer,
                        dict_decoder,
                        row_hi,
                        row_count,
                    )?;
                    decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                        page,
                        mode,
                        &mut BinaryColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(true)
                }
                (Encoding::Plain, _, ColumnTypeTag::Array) => {
                    // raw array encoding
                    let mut slicer = PlainVarSlicer::new(values_buffer, row_count);
                    decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                        page,
                        mode,
                        &mut RawArrayColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(true)
                }
                (Encoding::DeltaLengthByteArray, _, ColumnTypeTag::Array) => {
                    let mut slicer =
                        DeltaLengthArraySlicer::try_new(values_buffer, row_hi, row_count)?;
                    decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                        page,
                        mode,
                        &mut RawArrayColumnSink::new(&mut slicer, bufs),
                    )?;
                    Ok(true)
                }
                _ => Ok(false),
            }
        }
        _ => Ok(false),
    }
}

fn decode_int96_dispatch<const FILTERED: bool, const FILL_NULLS: bool>(
    page: &DataPage,
    dict: Option<&DictPage>,
    values_buffer: &[u8],
    bufs: &mut ColumnChunkBuffers,
    column_type: ColumnType,
    mode: DecodeModeContext<'_>,
) -> ParquetResult<bool> {
    let row_hi = mode.source_row_count();
    match (page.encoding(), dict, column_type.tag()) {
        (Encoding::Plain, _, ColumnTypeTag::Timestamp) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::new_with(
                    values_buffer,
                    bufs,
                    nulls::TIMESTAMP,
                    Int96ToTimestampConverter::new(),
                ),
            )?;
            Ok(true)
        }
        (
            Encoding::PlainDictionary | Encoding::RleDictionary,
            Some(dict_page),
            ColumnTypeTag::Timestamp,
        ) => {
            let dict_decoder = ConvertablePrimitiveDictDecoder::<
                Int96Timestamp,
                i64,
                Int96ToTimestampConverter,
            >::try_new(dict_page, Int96ToTimestampConverter::new())?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::TIMESTAMP,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn decode_double_dispatch<const FILTERED: bool, const FILL_NULLS: bool>(
    page: &DataPage,
    dict: Option<&DictPage>,
    values_buffer: &[u8],
    bufs: &mut ColumnChunkBuffers,
    column_type: ColumnType,
    mode: DecodeModeContext<'_>,
) -> ParquetResult<bool> {
    let row_hi = mode.source_row_count();
    let row_count = mode.sliced_row_count();

    match (page.encoding(), dict, column_type.tag()) {
        (Encoding::Plain, _, ColumnTypeTag::Double) => {
            clear_aux_buffers(bufs);

            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::<f64>::new(values_buffer, bufs, f64::NAN),
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            ColumnTypeTag::Double,
        ) => {
            clear_aux_buffers(bufs);

            let dict_decoder = BasePrimitiveDictDecoder::<f64, f64>::try_new(dict_page)?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    f64::NAN,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        (Encoding::Plain, _, ColumnTypeTag::Array) => {
            let mut slicer = DataPageFixedSlicer::<8>::new(values_buffer, row_count);
            decode_array_page_mode::<_, FILTERED, FILL_NULLS>(page, mode, &mut slicer, bufs)?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            ColumnTypeTag::Array,
        ) => {
            let dict_decoder = FixedDictDecoder::<8>::try_new(dict_page)?;
            let mut slicer =
                RleDictionarySlicer::try_new(values_buffer, dict_decoder, row_hi, row_count)?;
            decode_array_page_mode::<_, FILTERED, FILL_NULLS>(page, mode, &mut slicer, bufs)?;
            Ok(true)
        }
        // -- Type conversion arms: Double physical → narrower QDB type --
        (Encoding::Plain, _, ColumnTypeTag::Float) => {
            clear_aux_buffers(bufs);
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::<f64, f32>::new(values_buffer, bufs, nulls::FLOAT),
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            ColumnTypeTag::Float,
        ) => {
            clear_aux_buffers(bufs);
            let dict_decoder = ConvertablePrimitiveDictDecoder::<f64, f32, _>::try_new(
                dict_page,
                PrimitiveConverter::new(),
            )?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::FLOAT,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        (
            Encoding::Plain,
            _,
            ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp,
        ) => {
            clear_aux_buffers(bufs);
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::new_with(
                    values_buffer,
                    bufs,
                    nulls::LONG,
                    FloatToIntRangeCheckConverter::<f64, i64, true>::new(
                        nulls::LONG,
                        F64_MAX_SAFE_FOR_I64,
                        i64::MIN as f64,
                    ),
                ),
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp,
        ) => {
            clear_aux_buffers(bufs);
            let dict_decoder = ConvertablePrimitiveDictDecoder::try_new(
                dict_page,
                FloatToIntRangeCheckConverter::<f64, i64, true>::new(
                    nulls::LONG,
                    F64_MAX_SAFE_FOR_I64,
                    i64::MIN as f64,
                ),
            )?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::LONG,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        (Encoding::Plain, _, ColumnTypeTag::Int) => {
            clear_aux_buffers(bufs);
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::new_with(
                    values_buffer,
                    bufs,
                    nulls::INT,
                    FloatToIntRangeCheckConverter::<f64, i32, true>::new(
                        nulls::INT,
                        i32::MAX as f64,
                        i32::MIN as f64,
                    ),
                ),
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            ColumnTypeTag::Int,
        ) => {
            clear_aux_buffers(bufs);
            let dict_decoder = ConvertablePrimitiveDictDecoder::try_new(
                dict_page,
                FloatToIntRangeCheckConverter::<f64, i32, true>::new(
                    nulls::INT,
                    i32::MAX as f64,
                    i32::MIN as f64,
                ),
            )?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::INT,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        // -- Type conversion: Double → Short (range-checked) --
        (Encoding::Plain, _, ColumnTypeTag::Short) => {
            clear_aux_buffers(bufs);
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::new_with(
                    values_buffer,
                    bufs,
                    nulls::SHORT,
                    FloatToIntRangeCheckConverter::<f64, i16>::new(
                        nulls::SHORT,
                        i16::MAX as f64,
                        i16::MIN as f64,
                    ),
                ),
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            ColumnTypeTag::Short,
        ) => {
            clear_aux_buffers(bufs);
            let dict_decoder = ConvertablePrimitiveDictDecoder::try_new(
                dict_page,
                FloatToIntRangeCheckConverter::<f64, i16>::new(
                    nulls::SHORT,
                    i16::MAX as f64,
                    i16::MIN as f64,
                ),
            )?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::SHORT,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        // -- Type conversion: Double → Byte/Boolean (range-checked) --
        (Encoding::Plain, _, ColumnTypeTag::Byte | ColumnTypeTag::Boolean) => {
            clear_aux_buffers(bufs);
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::new_with(
                    values_buffer,
                    bufs,
                    nulls::BYTE,
                    FloatToIntRangeCheckConverter::<f64, i8>::new(
                        nulls::BYTE,
                        i8::MAX as f64,
                        i8::MIN as f64,
                    ),
                ),
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            ColumnTypeTag::Byte | ColumnTypeTag::Boolean,
        ) => {
            clear_aux_buffers(bufs);
            let dict_decoder = ConvertablePrimitiveDictDecoder::try_new(
                dict_page,
                FloatToIntRangeCheckConverter::<f64, i8>::new(
                    nulls::BYTE,
                    i8::MAX as f64,
                    i8::MIN as f64,
                ),
            )?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::BYTE,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn decode_other_fixed_dispatch<const FILTERED: bool, const FILL_NULLS: bool>(
    page: &DataPage,
    dict: Option<&DictPage>,
    values_buffer: &[u8],
    bufs: &mut ColumnChunkBuffers,
    column_type: ColumnType,
    typ: PhysicalType,
    mode: DecodeModeContext<'_>,
) -> ParquetResult<bool> {
    let row_hi = mode.source_row_count();
    clear_aux_buffers(bufs);
    match (page.encoding(), dict, typ, column_type.tag()) {
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            PhysicalType::Float,
            ColumnTypeTag::Float,
        ) => {
            let dict_decoder = BasePrimitiveDictDecoder::<f32, f32>::try_new(dict_page)?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    f32::NAN,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        (Encoding::Plain, _, PhysicalType::Float, ColumnTypeTag::Float) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::<f32>::new(values_buffer, bufs, f32::NAN),
            )?;
            Ok(true)
        }
        (Encoding::Plain, _, PhysicalType::Boolean, ColumnTypeTag::Boolean) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainBooleanDecoder::new(values_buffer, bufs, 0),
            )?;
            Ok(true)
        }
        (Encoding::Rle, _, PhysicalType::Boolean, ColumnTypeTag::Boolean) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleBooleanDecoder::try_new(values_buffer, row_hi, bufs, 0)?,
            )?;
            Ok(true)
        }
        // -- Type conversion arms: Float physical → wider/different QDB type --
        (Encoding::Plain, _, PhysicalType::Float, ColumnTypeTag::Double) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::<f32, f64>::new(values_buffer, bufs, nulls::DOUBLE),
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            PhysicalType::Float,
            ColumnTypeTag::Double,
        ) => {
            let dict_decoder = ConvertablePrimitiveDictDecoder::<f32, f64, _>::try_new(
                dict_page,
                PrimitiveConverter::new(),
            )?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::DOUBLE,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        // -- Type conversion: Float → Int (range-checked) --
        (Encoding::Plain, _, PhysicalType::Float, ColumnTypeTag::Int) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::new_with(
                    values_buffer,
                    bufs,
                    nulls::INT,
                    FloatToIntRangeCheckConverter::<f32, i32, true>::new(
                        nulls::INT,
                        F32_MAX_SAFE_FOR_I32,
                        i32::MIN as f32,
                    ),
                ),
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            PhysicalType::Float,
            ColumnTypeTag::Int,
        ) => {
            let dict_decoder = ConvertablePrimitiveDictDecoder::try_new(
                dict_page,
                FloatToIntRangeCheckConverter::<f32, i32, true>::new(
                    nulls::INT,
                    F32_MAX_SAFE_FOR_I32,
                    i32::MIN as f32,
                ),
            )?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::INT,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        // -- Type conversion: Float → Long/Date/Timestamp (range-checked) --
        (
            Encoding::Plain,
            _,
            PhysicalType::Float,
            ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp,
        ) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::new_with(
                    values_buffer,
                    bufs,
                    nulls::LONG,
                    FloatToIntRangeCheckConverter::<f32, i64, true>::new(
                        nulls::LONG,
                        F32_MAX_SAFE_FOR_I64,
                        i64::MIN as f32,
                    ),
                ),
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            PhysicalType::Float,
            ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp,
        ) => {
            let dict_decoder = ConvertablePrimitiveDictDecoder::try_new(
                dict_page,
                FloatToIntRangeCheckConverter::<f32, i64, true>::new(
                    nulls::LONG,
                    F32_MAX_SAFE_FOR_I64,
                    i64::MIN as f32,
                ),
            )?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::LONG,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        // -- Type conversion: Float → Short (range-checked) --
        (Encoding::Plain, _, PhysicalType::Float, ColumnTypeTag::Short) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::new_with(
                    values_buffer,
                    bufs,
                    nulls::SHORT,
                    FloatToIntRangeCheckConverter::<f32, i16>::new(
                        nulls::SHORT,
                        i16::MAX as f32,
                        i16::MIN as f32,
                    ),
                ),
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            PhysicalType::Float,
            ColumnTypeTag::Short,
        ) => {
            let dict_decoder = ConvertablePrimitiveDictDecoder::try_new(
                dict_page,
                FloatToIntRangeCheckConverter::<f32, i16>::new(
                    nulls::SHORT,
                    i16::MAX as f32,
                    i16::MIN as f32,
                ),
            )?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::SHORT,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        // -- Type conversion: Float → Byte/Boolean (range-checked) --
        (Encoding::Plain, _, PhysicalType::Float, ColumnTypeTag::Byte | ColumnTypeTag::Boolean) => {
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut PlainPrimitiveDecoder::new_with(
                    values_buffer,
                    bufs,
                    nulls::BYTE,
                    FloatToIntRangeCheckConverter::<f32, i8>::new(
                        nulls::BYTE,
                        i8::MAX as f32,
                        i8::MIN as f32,
                    ),
                ),
            )?;
            Ok(true)
        }
        (
            Encoding::RleDictionary | Encoding::PlainDictionary,
            Some(dict_page),
            PhysicalType::Float,
            ColumnTypeTag::Byte | ColumnTypeTag::Boolean,
        ) => {
            let dict_decoder = ConvertablePrimitiveDictDecoder::try_new(
                dict_page,
                FloatToIntRangeCheckConverter::<f32, i8>::new(
                    nulls::BYTE,
                    i8::MAX as f32,
                    i8::MIN as f32,
                ),
            )?;
            decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict_decoder,
                    row_hi,
                    nulls::BYTE,
                    bufs,
                )?,
            )?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

#[allow(clippy::while_let_on_iterator)]
pub(super) fn decode_page0<T: Pushable>(
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
                    // Handle skip phase using popcnt for fast counting
                    let local_skip_count = min(skip_count, length);
                    skip_count -= local_skip_count;
                    let mut bit_offset = 0usize;

                    if local_skip_count > 0 {
                        let to_skip = count_ones_in_bitmap(values, 0, local_skip_count);
                        sink.skip(to_skip)?;
                        bit_offset = local_skip_count;
                    }

                    // Process remaining bits using word-at-a-time approach
                    let remaining = length - bit_offset;
                    if remaining > 0 {
                        decode_bitmap_runs(values, bit_offset, remaining, sink)?;
                    }
                }
                HybridEncoded::Repeated(is_set, length) => {
                    let local_skip_count = min(skip_count, length);
                    let local_push_count = length - local_skip_count;
                    skip_count -= local_skip_count;
                    if is_set {
                        if local_skip_count > 0 {
                            sink.skip(local_skip_count)?;
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
        sink.skip(row_lo)?;
        sink.push_slice(row_hi - row_lo)?;
    }
    Ok(())
}

/// Process bitmap runs using word-at-a-time approach with trailing_ones/trailing_zeros.
#[inline]
fn decode_bitmap_runs<T: Pushable>(
    values: &[u8],
    bit_offset: usize,
    count: usize,
    sink: &mut T,
) -> ParquetResult<()> {
    let mut remaining = count;
    let mut pos = bit_offset;
    let mut consecutive_true = 0usize;
    let mut consecutive_false = 0usize;

    // Handle unaligned start bits to reach byte boundary
    let start_bit = pos & 7;
    if start_bit != 0 {
        let bits_in_byte = (8 - start_bit).min(remaining);
        let byte = values[pos >> 3] >> start_bit;
        for i in 0..bits_in_byte {
            if (byte >> i) & 1 == 1 {
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
        pos += bits_in_byte;
        remaining -= bits_in_byte;
    }

    // Process 8 bytes (64 bits) at a time
    while remaining >= 64 && (pos >> 3) + 8 <= values.len() {
        let word = unsafe { (values.as_ptr().add(pos >> 3) as *const u64).read_unaligned() };

        if word == u64::MAX {
            // All 64 bits set
            if consecutive_false > 0 {
                sink.push_nulls(consecutive_false)?;
                consecutive_false = 0;
            }
            consecutive_true += 64;
        } else if word == 0 {
            // All 64 bits clear
            if consecutive_true > 0 {
                sink.push_slice(consecutive_true)?;
                consecutive_true = 0;
            }
            consecutive_false += 64;
        } else {
            // Mixed: scan runs using trailing_ones/trailing_zeros
            let mut w = word;
            let mut bits_left = 64usize;
            while bits_left > 0 {
                if w & 1 == 1 {
                    let ones = (w.trailing_ones() as usize).min(bits_left);
                    if consecutive_false > 0 {
                        sink.push_nulls(consecutive_false)?;
                        consecutive_false = 0;
                    }
                    consecutive_true += ones;
                    w >>= ones;
                    bits_left -= ones;
                } else {
                    let zeros = if w == 0 {
                        bits_left
                    } else {
                        (w.trailing_zeros() as usize).min(bits_left)
                    };
                    if consecutive_true > 0 {
                        sink.push_slice(consecutive_true)?;
                        consecutive_true = 0;
                    }
                    consecutive_false += zeros;
                    w >>= zeros;
                    bits_left -= zeros;
                }
            }
        }
        pos += 64;
        remaining -= 64;
    }

    // Process remaining full bytes
    while remaining >= 8 {
        let byte = values[pos >> 3];
        if byte == 0xFF {
            if consecutive_false > 0 {
                sink.push_nulls(consecutive_false)?;
                consecutive_false = 0;
            }
            consecutive_true += 8;
        } else if byte == 0 {
            if consecutive_true > 0 {
                sink.push_slice(consecutive_true)?;
                consecutive_true = 0;
            }
            consecutive_false += 8;
        } else {
            for i in 0..8 {
                if (byte >> i) & 1 == 1 {
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
        }
        pos += 8;
        remaining -= 8;
    }

    // Handle remaining bits
    if remaining > 0 {
        let byte = values[pos >> 3];
        for i in 0..remaining {
            if (byte >> i) & 1 == 1 {
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
    }

    // Flush remaining runs
    if consecutive_true > 0 {
        sink.push_slice(consecutive_true)?;
    }
    if consecutive_false > 0 {
        sink.push_nulls(consecutive_false)?;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::while_let_on_iterator)]
pub(super) fn decode_page0_filtered<T: Pushable, const FILL_NULLS: bool>(
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
            return Ok(());
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
                            sink.skip(count_ones_in_bitmap(values, 0, length))?;
                            current_row += length;
                            continue;
                        }
                        if current_row >= row_hi {
                            break;
                        }
                    } else {
                        if filter_idx >= filter_len {
                            return Ok(());
                        }
                        let run_end_pos = run_start_pos + length;
                        if (rows_filter[filter_idx] as usize + row_group_lo) >= run_end_pos {
                            sink.skip(count_ones_in_bitmap(values, 0, length))?;
                            current_row += length;
                            continue;
                        }
                    }

                    let mut bit_offset = if FILL_NULLS && current_row < row_lo {
                        let skip_bits = row_lo - current_row;
                        sink.skip(count_ones_in_bitmap(values, 0, skip_bits))?;
                        skip_bits
                    } else {
                        0usize
                    };

                    if FILL_NULLS {
                        while output_row < row_hi && (current_row + bit_offset) < run_end_in_page {
                            let abs_row = page_row_start + current_row + bit_offset;
                            while filter_idx < filter_len
                                && (rows_filter[filter_idx] as usize + row_group_lo) < abs_row
                            {
                                filter_idx += 1;
                            }
                            let gap = if filter_idx < filter_len {
                                (rows_filter[filter_idx] as usize + row_group_lo) - abs_row
                            } else {
                                row_hi - output_row
                            };
                            let bulk = gap
                                .min(row_hi - output_row)
                                .min(run_end_in_page - (current_row + bit_offset));
                            if bulk > 0 {
                                sink.skip(count_ones_in_bitmap(values, bit_offset, bulk))?;
                                sink.push_nulls(bulk)?;
                                bit_offset += bulk;
                                output_row += bulk;
                                continue;
                            }
                            if get_bit_at(values, bit_offset) {
                                sink.push()?;
                            } else {
                                sink.push_null()?;
                            }
                            filter_idx += 1;
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
                                ))?;
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
                            return Ok(());
                        }
                    }

                    if bit_offset < length {
                        sink.skip(count_ones_in_bitmap(
                            values,
                            bit_offset,
                            length - bit_offset,
                        ))?;
                    }

                    current_row += length;
                }
                HybridEncoded::Repeated(is_set, length) => {
                    let run_start_pos = page_row_start + current_row;
                    let run_end_in_page = current_row + length;

                    if FILL_NULLS {
                        if run_end_in_page <= row_lo {
                            if is_set {
                                sink.skip(length)?;
                            }
                            current_row += length;
                            continue;
                        }
                        if current_row >= row_hi {
                            break;
                        }
                    } else {
                        if filter_idx >= filter_len {
                            return Ok(());
                        }
                        let run_end_pos = run_start_pos + length;
                        if (rows_filter[filter_idx] as usize + row_group_lo) >= run_end_pos {
                            if is_set {
                                sink.skip(length)?;
                            }
                            current_row += length;
                            continue;
                        }
                    }

                    let mut row_offset = if FILL_NULLS && current_row < row_lo {
                        let skip_rows = row_lo - current_row;
                        if is_set {
                            sink.skip(skip_rows)?;
                        }
                        skip_rows
                    } else {
                        0usize
                    };

                    if FILL_NULLS {
                        while output_row < row_hi && (current_row + row_offset) < run_end_in_page {
                            let abs_row = page_row_start + current_row + row_offset;
                            while filter_idx < filter_len
                                && (rows_filter[filter_idx] as usize + row_group_lo) < abs_row
                            {
                                filter_idx += 1;
                            }
                            let gap = if filter_idx < filter_len {
                                (rows_filter[filter_idx] as usize + row_group_lo) - abs_row
                            } else {
                                row_hi - output_row
                            };
                            let bulk = gap
                                .min(row_hi - output_row)
                                .min(run_end_in_page - (current_row + row_offset));
                            if bulk > 0 {
                                if is_set {
                                    sink.skip(bulk)?;
                                }
                                sink.push_nulls(bulk)?;
                                row_offset += bulk;
                                output_row += bulk;
                                continue;
                            }
                            if is_set {
                                sink.push()?;
                            } else {
                                sink.push_null()?;
                            }
                            filter_idx += 1;
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
                                sink.skip(target_offset - row_offset)?;
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
                            return Ok(());
                        }
                    }

                    if is_set && row_offset < length {
                        sink.skip(length - row_offset)?;
                    }

                    current_row += length;
                }
            };
        }
    } else {
        // No null bitmap - all values are non-null
        if FILL_NULLS {
            let mut page_row = row_lo;
            sink.skip(row_lo)?;

            while output_row < row_hi {
                let abs_row = page_row_start + page_row;
                while filter_idx < filter_len
                    && (rows_filter[filter_idx] as usize + row_group_lo) < abs_row
                {
                    filter_idx += 1;
                }
                let gap = if filter_idx < filter_len {
                    (rows_filter[filter_idx] as usize + row_group_lo) - abs_row
                } else {
                    row_hi - output_row
                };
                let bulk = gap.min(row_hi - output_row);
                if bulk > 0 {
                    sink.skip(bulk)?;
                    sink.push_nulls(bulk)?;
                    page_row += bulk;
                    output_row += bulk;
                    continue;
                }
                sink.push()?;
                filter_idx += 1;
                page_row += 1;
                output_row += 1;
            }

            if page_row < page_row_count {
                sink.skip(page_row_count - page_row)?;
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

                sink.skip(first_row - prev_row_end)?;
                sink.push_slice(consecutive)?;
                prev_row_end = first_row + consecutive;
                i += consecutive;
            }

            if prev_row_end < page_row_count {
                sink.skip(page_row_count - prev_row_end)?;
            }
        }
    }
    Ok(())
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
    debug_assert!(byte_idx < values.len());
    values
        .get(byte_idx)
        .is_some_and(|byte| (byte >> bit_idx) & 1 == 1)
}

fn decode_null_bitmap<'a>(
    page: &DataPage<'a>,
    count: usize,
) -> ParquetResult<Option<HybridDecoderBitmapIter<'a>>> {
    let nc = page.header.null_count();
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

/// Sizes `buffer` to hold a `size`-byte decompressed page using a fallible
/// reservation, returning a clean error instead of aborting the process when the
/// allocation fails.
///
/// `size` is the page header's `uncompressed_page_size`. It is attacker-controlled
/// up to `i32::MAX` and, unlike `compressed_page_size`, is NOT bounded by
/// `max_page_size` on the read path: `max_page_size` is the compressed
/// column-chunk size, and a highly compressible page can legitimately decompress
/// to more than that, so the size cannot be range-checked up front. A plain
/// `Vec::resize` aborts the JVM over JNI on allocation failure; `try_reserve`
/// turns a decompression-bomb header into a recoverable out-of-memory error while
/// still decoding any page that genuinely fits in memory. Every reachable `size`
/// is `<= i32::MAX`, so a failure here is always an allocation shortfall, never a
/// malformed layout: classifying it `OutOfMemory` lets a caller retry on transient
/// memory pressure instead of treating the page as corrupt.
///
/// The sizing is grow-only and deliberately does not `clear()`: callers thread one
/// buffer through every page of a file, so clearing would force `resize` to memset
/// the whole page on every call only for the decompressor to overwrite it. Growing
/// in place zeros just the delta a larger page adds beyond the previous one. A
/// malformed page whose codec under-fills the buffer (Snappy/LZ4 do not check the
/// fill) thus keeps stale bytes from an earlier page of the same file in its tail;
/// a caller needing a zeroed tail must `clear()` first (see
/// `decompress_varchar_slice_dict`).
pub(super) fn resize_decompress_buffer(buffer: &mut Vec<u8>, size: usize) -> ParquetResult<()> {
    if size > buffer.len() {
        buffer.try_reserve(size - buffer.len()).map_err(|_| {
            fmt_err!(
                OutOfMemory(None),
                "cannot allocate {} bytes for a decompressed page",
                size
            )
        })?;
    }
    buffer.resize(size, 0);
    Ok(())
}

pub(super) fn decompress_sliced_dict<'a>(
    page: SlicedDictPage<'a>,
    buffer: &'a mut Vec<u8>,
) -> ParquetResult<DictPage<'a>> {
    let buf = if page.compression != parquet2::compression::Compression::Uncompressed {
        resize_decompress_buffer(buffer, page.uncompressed_size)?;
        parquet2::compression::decompress(page.compression, page.buffer, buffer)?;
        buffer
    } else {
        page.buffer
    };
    Ok(DictPage {
        buffer: buf,
        num_values: page.num_values,
        is_sorted: page.is_sorted,
    })
}

pub(super) fn decompress_sliced_data<'a>(
    page: &'a SlicedDataPage<'a>,
    decompress_buffer: &'a mut Vec<u8>,
) -> ParquetResult<DataPage<'a>> {
    let buffer = if page.compression != parquet2::compression::Compression::Uncompressed {
        match &page.header {
            DataPageHeader::V1(_) => {
                resize_decompress_buffer(decompress_buffer, page.uncompressed_size)?;
                parquet2::compression::decompress(
                    page.compression,
                    page.buffer,
                    decompress_buffer,
                )?;
                decompress_buffer
            }
            DataPageHeader::V2(header) => {
                let offset = (header.definition_levels_byte_length
                    + header.repetition_levels_byte_length) as usize;
                let can_decompress = header.is_compressed.unwrap_or(true);
                if can_decompress {
                    resize_decompress_buffer(decompress_buffer, page.uncompressed_size)?;
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
                    decompress_buffer
                } else {
                    // is_compressed=false: the page body is already uncompressed and
                    // returned as-is, so the decompress buffer is never read. Compare
                    // the header's uncompressed_size against the actual body directly
                    // -- sizing (and zeroing) an i32::MAX-capable buffer only to read
                    // back its length is pure waste with no decompression to amortize.
                    if page.uncompressed_size != page.buffer.len() {
                        return Err(fmt_err!(
                            Layout,
                            "V2 Page Header reported incorrect decompressed size"
                        ));
                    }
                    page.buffer
                }
            }
        }
    } else {
        page.buffer
    };
    Ok(DataPage {
        buffer,
        header: &page.header,
        descriptor: &page.descriptor,
    })
}

pub(super) fn sliced_page_row_count(
    page: &SlicedDataPage,
    column_type: ColumnType,
) -> Option<usize> {
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

pub(super) fn page_row_count(page: &DataPage, column_type: ColumnType) -> ParquetResult<usize> {
    match &page.header {
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
                        .fold(
                            Ok(0usize) as ParquetResult<_>,
                            |acc, rep_level| {
                                acc.and_then(|count| {
                                    if rep_level? == 0 {
                                        Ok(count + 1)
                                    } else {
                                        Ok(count)
                                    }
                                })
                            },
                        )?;
                        Ok(num_rows)
                    }
                }
                // For primitive types number of rows matches the number of values.
                _ => Ok(header.num_values as usize),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        decode_page, decode_page_filtered, decompress_sliced_data, decompress_sliced_dict,
        resize_decompress_buffer,
    };
    use crate::allocator::{AcVec, TestAllocatorState};
    use crate::parquet::error::ParquetErrorReason;
    use crate::parquet::qdb_metadata::{QdbMetaCol, QdbMetaColFormat};
    use crate::parquet::tests::ColumnTypeTagExt;
    use crate::parquet_read::page::{DataPage, DictPage};
    use crate::parquet_read::{ColumnChunkBuffers, DecodeContext, ParquetDecoder, RowGroupBuffers};
    use crate::parquet_write::array::{append_array_null, append_raw_array};
    use crate::parquet_write::decimal::{
        DECIMAL16_NULL, DECIMAL32_NULL, DECIMAL64_NULL, DECIMAL8_NULL,
    };
    use crate::parquet_write::file::ParquetWriter;
    use crate::parquet_write::schema::{Column, Partition};
    use crate::parquet_write::varchar::{append_varchar, append_varchar_null};
    use arrow::datatypes::ToByteSlice;
    use parquet2::compression::{compress, Compression, CompressionOptions};
    use parquet2::encoding::hybrid_rle::encode_u32;
    use parquet2::encoding::Encoding;
    use parquet2::metadata::Descriptor;
    use parquet2::page::{DataPageHeader, DataPageHeaderV1, DataPageHeaderV2};
    use parquet2::read::levels::get_bit_width;
    use parquet2::read::{SlicedDataPage, SlicedDictPage};
    use parquet2::schema::types::{FieldInfo, PhysicalType, PrimitiveLogicalType, PrimitiveType};
    use parquet2::schema::Repetition;
    use parquet2::write::Version;
    use qdb_core::col_type::{encode_array_type, ColumnType, ColumnTypeTag};
    use rand::RngExt;
    use std::io::Cursor;
    use std::mem::size_of;
    use std::ptr::null;

    const INT_NULL: [u8; 4] = i32::MIN.to_le_bytes();
    const LONG_NULL: [u8; 8] = i64::MIN.to_le_bytes();
    const UUID_NULL: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 0, 128];

    #[test]
    fn page_buffers_size_tracks_retained_page_bytes() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut bufs = ColumnChunkBuffers::new(allocator);

        // Fresh buffer: nothing retained.
        bufs.refresh_ptrs().unwrap();
        assert_eq!(bufs.page_buffers_size, 0);

        // A VarcharSlice decode retains decompressed page/dict buffers here, with the
        // aux pointers referencing them. refresh_ptrs must sum their lengths so the
        // Java decode-cache budget counts the string bytes.
        bufs.page_buffers.push(vec![0u8; 100]);
        bufs.page_buffers.push(vec![0u8; 56]);
        bufs.refresh_ptrs().unwrap();
        assert_eq!(bufs.page_buffers_size, 156);

        // reset() must zero it so a reused buffer does not carry stale bytes.
        bufs.reset();
        assert_eq!(bufs.page_buffers_size, 0);
        assert!(bufs.page_buffers.is_empty());
    }

    #[test]
    fn page_buffers_charge_tracks_per_query_memory() {
        let tas = TestAllocatorState::new().with_memory_tracker();
        let allocator = tas.allocator();
        let mut bufs = ColumnChunkBuffers::new(allocator);

        // The system-allocated page_buffers are otherwise invisible to the
        // per-query tracker; refresh_ptrs charges their retained bytes so a wide
        // VarcharSlice payload still counts against the limit.
        bufs.page_buffers.push(vec![0u8; 1000]);
        bufs.page_buffers.push(vec![0u8; 24]);
        bufs.refresh_ptrs().unwrap();
        assert_eq!(bufs.page_buffers_size, 1024);
        assert_eq!(tas.tracker_used(), 1024);

        // A subsequent decode that retains fewer bytes credits the difference.
        bufs.page_buffers.truncate(1);
        bufs.refresh_ptrs().unwrap();
        assert_eq!(bufs.page_buffers_size, 1000);
        assert_eq!(tas.tracker_used(), 1000);

        // reset() releases the whole charge back to the tracker.
        bufs.reset();
        assert_eq!(tas.tracker_used(), 0);

        // A retained charge still outstanding at drop is credited too.
        bufs.page_buffers.push(vec![0u8; 512]);
        bufs.refresh_ptrs().unwrap();
        assert_eq!(tas.tracker_used(), 512);
        drop(bufs);
        assert_eq!(tas.tracker_used(), 0);
    }

    #[test]
    fn page_buffers_charge_breaches_per_query_limit() {
        let tas = TestAllocatorState::new().with_memory_tracker();
        let allocator = tas.allocator();
        let mut bufs = ColumnChunkBuffers::new(allocator);
        tas.set_tracker_limit(4096);

        // Under the limit: charged and accepted.
        bufs.page_buffers.push(vec![0u8; 2048]);
        bufs.refresh_ptrs().unwrap();
        assert_eq!(tas.tracker_used(), 2048);

        // A wide payload pushes the retained bytes over the limit: refresh_ptrs
        // breaches at the per-query scope and leaves the charge at its prior value.
        // The "query" scope label in the message distinguishes a per-query breach
        // from a global-RSS one.
        bufs.page_buffers.push(vec![0u8; 8192]);
        let err = bufs.refresh_ptrs().unwrap_err();
        assert!(
            err.to_string().contains("query memory limit exceeded"),
            "unexpected error: {err}"
        );
        assert_eq!(bufs.page_buffers_charged, 2048);

        // The uncharged excess never reached the tracker; drop credits only the
        // 2048 bytes that were charged, returning the counter to zero.
        drop(bufs);
        assert_eq!(tas.tracker_used(), 0);
    }

    #[test]
    fn decode_row_group_clears_varchar_slice_reuse_pool() {
        // The VarcharSlice page-buffer reuse pool lives on the DecodeContext, which outlives
        // individual decode-cache entries. Spare buffers left in it are uncounted by the Java
        // byte budget and would otherwise survive cache eviction and releaseParquetBuffers(),
        // letting the budget read zero while RSS still held varchar page allocations. A
        // row-group decode must drop the pool at the end. The live buffers a decode produces
        // are held in ColumnChunkBuffers::page_buffers (counted), not the pool.
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let row_count = 10;
        let row_group_size = 50;
        let data_page_size = 50;
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
        let mut rgb = RowGroupBuffers::new(allocator.clone());
        let mut ctx = DecodeContext::new(file.as_ptr(), file_len);

        // Simulate spare page buffers retained from a prior column-chunk decode.
        ctx.varchar_slice_buf_pool.push(vec![0u8; 4096]);
        ctx.varchar_slice_buf_pool.push(vec![0u8; 4096]);

        let column_type = decoder.columns[0].column_type.unwrap();
        decoder
            .decode_row_group(
                &mut ctx,
                &mut rgb,
                &[(0, column_type)],
                0,
                0,
                row_count as u32,
            )
            .unwrap();

        assert!(
            ctx.varchar_slice_buf_pool.is_empty(),
            "row-group decode must release the varchar-slice reuse pool"
        );
        assert!(
            ctx.varchar_slice_page_bufs_scratch.is_empty(),
            "row-group decode must leave the page-buffer scratch empty"
        );
        assert!(
            ctx.varchar_slice_dict_bufs_scratch.is_empty(),
            "row-group decode must leave the dict-buffer scratch empty"
        );
    }

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
            let col_info = QdbMetaCol {
                column_type,
                column_top: 0,
                format: None,
                ascii: None,
                id: None,
            };
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
        #[cfg(miri)]
        let (row_count, row_group_size, data_page_size) = (30, 6, 3);
        #[cfg(not(miri))]
        let (row_count, row_group_size, data_page_size) = (100, 10, 5);
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
                    let col_info = QdbMetaCol {
                        column_type,
                        column_top: 0,
                        format: None,
                        ascii: None,
                        id: None,
                    };
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
    fn test_decode_boolean_column_v2_partial_decode() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        #[cfg(miri)]
        let (row_count, row_group_size, data_page_size) = (30, 6, 3);
        #[cfg(not(miri))]
        let (row_count, row_group_size, data_page_size) = (100, 10, 5);
        let version = Version::V2;
        let expected_buff = create_col_data_buff_bool(row_count);
        let columns = vec![create_fix_column(
            0,
            row_count,
            "bool_col",
            expected_buff.data_vec.as_ref(),
            ColumnTypeTag::Boolean.into_type(),
        )];
        let file = write_cols_to_parquet_file(row_group_size, data_page_size, version, columns);

        let file_len = file.len() as u64;
        let mut reader = Cursor::new(&file);
        let decoder = ParquetDecoder::read(allocator.clone(), &mut reader, file_len).unwrap();
        assert_eq!(decoder.columns.len(), 1);
        assert_eq!(decoder.row_count, row_count);
        let row_group_count = decoder.row_group_count as usize;
        let bufs = &mut ColumnChunkBuffers::new(allocator);
        let mut ctx = DecodeContext::new(file.as_ptr(), file_len);

        for row_lo in 0..row_group_size - 1 {
            for row_hi in row_lo + 1..row_group_size {
                for row_group_index in 0..row_group_count {
                    decoder
                        .decode_column_chunk(
                            &mut ctx,
                            bufs,
                            row_group_index,
                            row_lo,
                            row_hi,
                            0,
                            QdbMetaCol {
                                column_type: ColumnTypeTag::Boolean.into_type(),
                                column_top: 0,
                                format: None,
                                ascii: None,
                                id: None,
                            },
                        )
                        .unwrap();

                    assert_eq!(bufs.data_size, row_hi - row_lo);
                    assert_eq!(bufs.aux_size, 0);
                    let row_group_offset = row_group_index * row_group_size;
                    assert_eq!(
                        bufs.data_vec,
                        expected_buff.data_vec
                            [row_group_offset + row_lo..row_group_offset + row_hi]
                    );
                }
            }
        }
    }

    #[test]
    fn test_decode_int_long_column_v2_nulls_multi_groups() {
        #[cfg(miri)]
        let (row_count, row_group_size, data_page_size) = (100, 10, 10);
        #[cfg(not(miri))]
        let (row_count, row_group_size, data_page_size) = (10000, 1000, 1000);
        let version = Version::V1;
        let array_type = encode_array_type(ColumnTypeTag::Double, 1).unwrap();

        let expected_buffs: Vec<(ColumnBuffers, ColumnType)> = vec![
            (
                create_col_data_buff::<i32, 4, _>(row_count, INT_NULL, |int| int.to_le_bytes()),
                ColumnTypeTag::Int.into_type(),
            ),
            (
                create_col_data_buff::<i64, 8, _>(row_count, LONG_NULL, |int| int.to_le_bytes()),
                ColumnTypeTag::Long.into_type(),
            ),
            (
                create_col_data_buff_string(row_count, 3),
                ColumnTypeTag::String.into_type(),
            ),
            (
                create_col_data_buff_varchar(row_count, 3),
                ColumnTypeTag::Varchar.into_type(),
            ),
            (
                create_col_data_buff_symbol(row_count, 10),
                ColumnTypeTag::Varchar.into_type(),
            ),
            (create_col_data_buff_array(row_count, 3), array_type),
        ];

        let columns = vec![
            create_fix_column(
                0,
                row_count,
                "int_col",
                expected_buffs[0].0.data_vec.as_ref(),
                ColumnTypeTag::Int.into_type(),
            ),
            create_fix_column(
                1,
                row_count,
                "long_col",
                expected_buffs[1].0.data_vec.as_ref(),
                ColumnTypeTag::Long.into_type(),
            ),
            create_var_column(
                2,
                row_count,
                "string_col",
                expected_buffs[2].0.data_vec.as_ref(),
                expected_buffs[2].0.aux_vec.as_ref().unwrap(),
                ColumnTypeTag::String.into_type(),
            ),
            create_var_column(
                3,
                row_count,
                "varchar_col",
                expected_buffs[3].0.data_vec.as_ref(),
                expected_buffs[3].0.aux_vec.as_ref().unwrap(),
                ColumnTypeTag::Varchar.into_type(),
            ),
            create_symbol_column(
                4,
                row_count,
                "symbol_col",
                expected_buffs[4].0.data_vec.as_ref(),
                expected_buffs[4].0.sym_chars.as_ref().unwrap(),
                expected_buffs[4].0.sym_offsets.as_ref().unwrap(),
                ColumnTypeTag::Symbol.into_type(),
            ),
            create_var_column(
                5,
                row_count,
                "array_col",
                expected_buffs[5].0.data_vec.as_ref(),
                expected_buffs[5].0.aux_vec.as_ref().unwrap(),
                array_type,
            ),
        ];

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
        #[cfg(miri)]
        let (row_count, row_group_size, data_page_size) = (100, 10, 10);
        #[cfg(not(miri))]
        let (row_count, row_group_size, data_page_size) = (10000, 1000, 1000);
        let version = Version::V2;

        let expected_buffs: Vec<(ColumnBuffers, ColumnType)> = vec![
            (
                create_col_data_buff_bool(row_count),
                ColumnTypeTag::Boolean.into_type(),
            ),
            (
                create_col_data_buff::<i16, 2, _>(row_count, i16::MIN.to_le_bytes(), |short| {
                    short.to_le_bytes()
                }),
                ColumnTypeTag::Short.into_type(),
            ),
            (
                create_col_data_buff::<i16, 2, _>(row_count, i16::MIN.to_le_bytes(), |short| {
                    short.to_le_bytes()
                }),
                ColumnTypeTag::Char.into_type(),
            ),
            (
                create_col_data_buff::<i128, 16, _>(row_count, UUID_NULL, |uuid| {
                    uuid.to_le_bytes()
                }),
                ColumnTypeTag::Uuid.into_type(),
            ),
        ];

        let columns = vec![
            create_fix_column(
                0,
                row_count,
                "bool_col",
                expected_buffs[0].0.data_vec.as_ref(),
                ColumnTypeTag::Boolean.into_type(),
            ),
            create_fix_column(
                1,
                row_count,
                "short_col",
                expected_buffs[1].0.data_vec.as_ref(),
                ColumnTypeTag::Short.into_type(),
            ),
            create_fix_column(
                2,
                row_count,
                "char_col",
                expected_buffs[2].0.data_vec.as_ref(),
                ColumnTypeTag::Char.into_type(),
            ),
            create_fix_column(
                3,
                row_count,
                "uuid_col",
                expected_buffs[3].0.data_vec.as_ref(),
                ColumnTypeTag::Uuid.into_type(),
            ),
        ];

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
                        QdbMetaCol {
                            column_type,
                            column_top: 0,
                            format,
                            ascii: None,
                            id: None,
                        },
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
                        assert_eq!(expected_aux_data.len() % size_of::<i64>(), 0);
                        let read_i64 = |index: usize| {
                            let start = index * size_of::<i64>();
                            let end = start + size_of::<i64>();
                            i64::from_le_bytes(expected_aux_data[start..end].try_into().unwrap())
                        };
                        expected_aux_data_slice
                            .extend_from_slice(&0u64.to_le_bytes())
                            .unwrap();
                        for i in 0..row_count {
                            let row_data_offset = read_i64(col_row_count + 1 + i);
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

    struct TestDataPage {
        header: DataPageHeader,
        descriptor: Descriptor,
        buffer: Vec<u8>,
    }

    impl TestDataPage {
        fn as_page(&self) -> DataPage<'_> {
            DataPage {
                header: &self.header,
                descriptor: &self.descriptor,
                buffer: &self.buffer,
            }
        }
    }

    struct TestDictPage {
        buffer: Vec<u8>,
        num_values: usize,
        is_sorted: bool,
    }

    impl TestDictPage {
        fn as_page(&self) -> DictPage<'_> {
            DictPage {
                buffer: &self.buffer,
                num_values: self.num_values,
                is_sorted: self.is_sorted,
            }
        }
    }

    fn make_required_page(
        primitive_type: PrimitiveType,
        encoding: Encoding,
        values: Vec<u8>,
        num_values: usize,
    ) -> TestDataPage {
        TestDataPage {
            header: DataPageHeader::V1(DataPageHeaderV1 {
                num_values: num_values as i32,
                encoding: encoding.into(),
                definition_level_encoding: Encoding::Rle.into(),
                repetition_level_encoding: Encoding::Rle.into(),
                statistics: None,
            }),
            descriptor: Descriptor { primitive_type, max_def_level: 0, max_rep_level: 0 },
            buffer: values,
        }
    }

    fn make_decimal_flba_type(len: usize, precision: usize, scale: usize) -> PrimitiveType {
        PrimitiveType {
            field_info: FieldInfo {
                name: "dec_col".to_string(),
                repetition: Repetition::Required,
                id: None,
            },
            logical_type: Some(PrimitiveLogicalType::Decimal(precision, scale)),
            converted_type: None,
            physical_type: PhysicalType::FixedLenByteArray(len),
        }
    }

    fn make_decimal_ba_type(precision: usize, scale: usize) -> PrimitiveType {
        PrimitiveType {
            field_info: FieldInfo {
                name: "dec_col".to_string(),
                repetition: Repetition::Required,
                id: None,
            },
            logical_type: Some(PrimitiveLogicalType::Decimal(precision, scale)),
            converted_type: None,
            physical_type: PhysicalType::ByteArray,
        }
    }

    fn make_byte_array_type() -> PrimitiveType {
        PrimitiveType {
            field_info: FieldInfo {
                name: "str_col".to_string(),
                repetition: Repetition::Required,
                id: None,
            },
            logical_type: None,
            converted_type: None,
            physical_type: PhysicalType::ByteArray,
        }
    }

    fn make_int32_type() -> PrimitiveType {
        PrimitiveType {
            field_info: FieldInfo {
                name: "int_col".to_string(),
                repetition: Repetition::Required,
                id: None,
            },
            logical_type: None,
            converted_type: None,
            physical_type: PhysicalType::Int32,
        }
    }

    fn make_dict_page_i32(values: &[i32]) -> TestDictPage {
        let mut buf = Vec::with_capacity(values.len() * 4);
        for v in values {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        TestDictPage {
            buffer: buf,
            num_values: values.len(),
            is_sorted: false,
        }
    }

    fn make_dict_page_fixed<const N: usize>(values: &[[u8; N]]) -> TestDictPage {
        let mut buf = Vec::with_capacity(values.len() * N);
        for value in values {
            buf.extend_from_slice(value);
        }
        TestDictPage {
            buffer: buf,
            num_values: values.len(),
            is_sorted: false,
        }
    }

    fn make_dict_page_var(values: &[Vec<u8>]) -> TestDictPage {
        TestDictPage {
            buffer: encode_plain_byte_array(values),
            num_values: values.len(),
            is_sorted: false,
        }
    }

    fn make_dict_data_page(
        primitive_type: PrimitiveType,
        encoding: Encoding,
        indices: &[u32],
    ) -> TestDataPage {
        let mut buf = Vec::new();
        let max_index = indices.iter().copied().max().unwrap_or(0);
        let bit_width = get_bit_width(max_index as i16);
        buf.push(bit_width as u8);
        encode_u32(&mut buf, indices.iter().copied(), indices.len(), bit_width).unwrap();
        make_required_page(primitive_type, encoding, buf, indices.len())
    }

    fn encode_plain_byte_array(values: &[Vec<u8>]) -> Vec<u8> {
        let total_len: usize = values.iter().map(|v| 4 + v.len()).sum();
        let mut out = Vec::with_capacity(total_len);
        for value in values {
            out.extend_from_slice(&(value.len() as u32).to_le_bytes());
            out.extend_from_slice(value);
        }
        out
    }

    fn decimal_target_cases() -> [(ColumnTypeTag, usize); 6] {
        [
            (ColumnTypeTag::Decimal8, 1),
            (ColumnTypeTag::Decimal16, 2),
            (ColumnTypeTag::Decimal32, 4),
            (ColumnTypeTag::Decimal64, 8),
            (ColumnTypeTag::Decimal128, 16),
            (ColumnTypeTag::Decimal256, 32),
        ]
    }

    fn be_to_le_truncate(src: &[u8], target: usize) -> Vec<u8> {
        let mut src = src;
        let mut src_len = src.len();
        if src_len > target {
            let trunc = src_len - target;
            src = &src[trunc..];
            src_len = target;
        }
        let sign_byte = if src[0] & 0x80 != 0 { 0xFF } else { 0x00 };
        let mut out = vec![0u8; target];
        for i in 0..src_len {
            out[i] = src[src_len - 1 - i];
        }
        for byte in out.iter_mut().take(target).skip(src_len) {
            *byte = sign_byte;
        }
        out
    }

    fn be_to_qdb_decimal(src: &[u8], target: usize) -> Vec<u8> {
        let mut src = src;
        let mut src_len = src.len();
        if src_len > target {
            let sign_byte = if src[0] & 0x80 != 0 { 0xFF } else { 0x00 };
            let trunc = src_len - target;
            assert!(src[..trunc].iter().all(|b| *b == sign_byte));
            src = &src[trunc..];
            src_len = target;
        }

        let sign_byte = if src[0] & 0x80 != 0 { 0xFF } else { 0x00 };
        if target <= 8 {
            let mut out = vec![sign_byte; target];
            for i in 0..src_len {
                out[i] = src[src_len - 1 - i];
            }
            return out;
        }

        let mut out = vec![0u8; target];
        let words = target / 8;
        let sign_prefix = target - src_len;
        for w in 0..words {
            for i in 0..8 {
                let extended_pos = w * 8 + 7 - i;
                out[w * 8 + i] = if extended_pos < sign_prefix {
                    sign_byte
                } else {
                    src[extended_pos - sign_prefix]
                };
            }
        }
        out
    }

    fn expected_from_i32<const N: usize>(values: &[i32]) -> Vec<u8> {
        let mut out = Vec::with_capacity(values.len() * N);
        for v in values {
            let bytes = v.to_le_bytes();
            out.extend_from_slice(&bytes[..N]);
        }
        out
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
            0,
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
            0,
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
            0,
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
    fn test_decode_row_group_filtered_symbol_column_top_with_row_range() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let column_top = 8usize;
        let symbol_count = 4usize;
        let data_row_count = 20usize;
        let total_row_count = column_top + data_row_count;
        let row_group_size = total_row_count;
        let data_page_size = 10;
        let version = Version::V2;

        let data_values: Vec<i32> = (0..data_row_count)
            .map(|i| (i % symbol_count) as i32)
            .collect();
        let symbol_values: Vec<String> = (0..symbol_count).map(|i| format!("s{i}")).collect();
        let (symbol_chars, symbol_offsets) = serialize_as_symbols(&symbol_values);

        let column = Column::from_raw_data(
            0,
            "symbol_col",
            ColumnTypeTag::Symbol.into_type().code(),
            column_top as i64,
            total_row_count,
            data_values.as_ptr() as *const u8,
            std::mem::size_of_val(data_values.as_slice()),
            symbol_chars.as_ptr(),
            symbol_chars.len(),
            symbol_offsets.as_ptr(),
            symbol_offsets.len(),
            false,
            false,
            0,
        )
        .unwrap();

        let file =
            write_cols_to_parquet_file(row_group_size, data_page_size, version, vec![column]);
        let file_len = file.len() as u64;
        let mut reader = Cursor::new(&file);
        let decoder = ParquetDecoder::read(allocator.clone(), &mut reader, file_len).unwrap();
        let mut rgb = RowGroupBuffers::new(allocator);
        let mut ctx = DecodeContext::new(file.as_ptr(), file_len);
        let columns = vec![(0i32, ColumnTypeTag::Symbol.into_type())];

        let row_group_lo = 5u32;
        let row_group_hi = 23u32;
        let rows_filter: Vec<i64> = vec![0, 1, 2, 3, 5, 8, 10, 14, 17];

        let count = decoder
            .decode_row_group_filtered::<false>(
                &mut ctx,
                &mut rgb,
                0,
                &columns,
                0,
                row_group_lo,
                row_group_hi,
                &rows_filter,
            )
            .unwrap();

        assert_eq!(count, rows_filter.len());
        let result: Vec<i32> = rgb.column_bufs[0]
            .data_vec
            .chunks(std::mem::size_of::<i32>())
            .map(|c| i32::from_le_bytes(c.try_into().unwrap()))
            .collect();
        let expected: Vec<i32> = rows_filter
            .iter()
            .map(|&row| {
                let abs_row = row_group_lo as usize + row as usize;
                if abs_row < column_top {
                    i32::MIN
                } else {
                    data_values[abs_row - column_top]
                }
            })
            .collect();
        assert_eq!(
            rgb.column_bufs[0].data_vec.len(),
            rows_filter.len() * std::mem::size_of::<i32>(),
            "result={result:?}, expected={expected:?}"
        );
        assert_eq!(result, expected);
    }

    // No-sentinel source (SHORT) widened to a sentinel target (LONG) on the filtered decode
    // path: column-top rows that pass the filter must materialize as the target NULL sentinel
    // (i64::MIN), not 0. Covers FILL_NULLS=false (compacted) and true (full scan order), with a
    // partial window (row_group_lo > 0). Regression for the M1 "leading_nulls = 0" follow-up.
    #[test]
    fn test_decode_row_group_filtered_short_to_long_column_top() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let column_top = 8usize;
        let data_row_count = 20usize;
        let total_row_count = column_top + data_row_count;
        let row_group_size = total_row_count;
        let data_page_size = 10;
        let version = Version::V2;

        let data_values: Vec<i16> = (0..data_row_count).map(|i| (i as i16) * 10 + 1).collect();

        let column = Column::from_raw_data(
            0,
            "short_col",
            ColumnTypeTag::Short.into_type().code(),
            column_top as i64,
            total_row_count,
            data_values.as_ptr() as *const u8,
            std::mem::size_of_val(data_values.as_slice()),
            null(),
            0,
            null(),
            0,
            false,
            false,
            0,
        )
        .unwrap();

        let file =
            write_cols_to_parquet_file(row_group_size, data_page_size, version, vec![column]);
        let file_len = file.len() as u64;
        let mut reader = Cursor::new(&file);
        let decoder = ParquetDecoder::read(allocator.clone(), &mut reader, file_len).unwrap();
        let mut ctx = DecodeContext::new(file.as_ptr(), file_len);
        // Decode the SHORT parquet column as LONG (lazy widening conversion).
        let columns = vec![(0i32, ColumnTypeTag::Long.into_type())];

        let row_group_lo = 5u32;
        let row_group_hi = 23u32;
        // Window-relative, ascending. abs_row = row_group_lo + entry; null iff abs_row < column_top.
        let rows_filter: Vec<i64> = vec![0, 1, 2, 3, 5, 8, 10, 14, 17];

        // FILL_NULLS=false: compacted output, one i64 per matched row.
        let mut rgb = RowGroupBuffers::new(allocator.clone());
        let count = decoder
            .decode_row_group_filtered::<false>(
                &mut ctx,
                &mut rgb,
                0,
                &columns,
                0,
                row_group_lo,
                row_group_hi,
                &rows_filter,
            )
            .unwrap();
        assert_eq!(count, rows_filter.len());
        let result: Vec<i64> = rgb.column_bufs[0]
            .data_vec
            .chunks(std::mem::size_of::<i64>())
            .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
            .collect();
        let expected: Vec<i64> = rows_filter
            .iter()
            .map(|&row| {
                let abs_row = row_group_lo as usize + row as usize;
                if abs_row < column_top {
                    i64::MIN
                } else {
                    data_values[abs_row - column_top] as i64
                }
            })
            .collect();
        assert_eq!(result, expected, "FILL_NULLS=false");

        // FILL_NULLS=true: full scan order over [row_group_lo, row_group_hi). The late-
        // materialization consumer reads ONLY matched positions (record.setRowIndex(rows.get(i))
        // in AsyncWindowJoinRecordCursorFactory), so we assert just those. Unmatched positions
        // are placeholders the consumer never reads; for a no-sentinel source they keep the
        // source-null fill (0), which is irrelevant. Matched column-top rows must be i64::MIN.
        let mut rgb_fill = RowGroupBuffers::new(allocator);
        let count_fill = decoder
            .decode_row_group_filtered::<true>(
                &mut ctx,
                &mut rgb_fill,
                0,
                &columns,
                0,
                row_group_lo,
                row_group_hi,
                &rows_filter,
            )
            .unwrap();
        assert_eq!(count_fill, (row_group_hi - row_group_lo) as usize);
        let result_fill: Vec<i64> = rgb_fill.column_bufs[0]
            .data_vec
            .chunks(std::mem::size_of::<i64>())
            .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
            .collect();
        for &row in &rows_filter {
            let j = row as usize; // FILL_NULLS=true output is indexed by window-relative row
            let abs_row = row_group_lo as usize + j;
            let want = if abs_row < column_top {
                i64::MIN
            } else {
                data_values[abs_row - column_top] as i64
            };
            assert_eq!(result_fill[j], want, "FILL_NULLS=true matched row {j}");
        }
    }

    #[test]
    fn test_decode_flba_decimal_sign_extended_unfiltered() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let src_len = 16;
        let values = [
            // +123 as 16-byte BE
            [
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x7B,
            ],
            // -1 as 16-byte BE
            [0xFF; 16],
        ];
        let mut buffer = Vec::new();
        for v in values {
            buffer.extend_from_slice(&v);
        }

        let page = make_required_page(
            make_decimal_flba_type(src_len, 10, 2),
            Encoding::Plain,
            buffer,
            values.len(),
        );
        let page = page.as_page();

        let mut bufs = ColumnChunkBuffers::new(allocator);
        let col_info = QdbMetaCol {
            column_type: ColumnTypeTag::Decimal64.into_type(),
            column_top: 0,
            format: None,
            ascii: None,
            id: None,
        };

        decode_page(&page, None, &mut bufs, col_info, 0, values.len()).unwrap();

        let mut expected = Vec::new();
        expected.extend_from_slice(&be_to_le_truncate(&values[0], 8));
        expected.extend_from_slice(&be_to_le_truncate(&values[1], 8));
        assert_eq!(bufs.data_vec.as_slice(), expected.as_slice());
    }

    // Decode an all-null V1 page whose DELTA_BINARY_PACKED values buffer is EMPTY
    // (no header) -- the shape pre-fix QuestDB and some foreign encoders produce
    // -- through the real decode_page path, asserting every row is the given
    // Int64-width null sentinel. Every all-null integration test goes through the
    // current header-emitting writer, so this is the only coverage of
    // MiniblockIterator::try_new's empty-buffer branch via decode_page. Reverting
    // just the reader fix makes this fail (try_new rejects the empty buffer)
    // while the integration round-trips still pass.
    fn assert_empty_delta_int64_page_all_null(column_type: ColumnType, null_sentinel: i64) {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let n = 10usize;

        // Definition levels: n rows, all null (level 0), RLE-encoded (bit width 1).
        let mut def_levels = Vec::new();
        encode_u32(&mut def_levels, std::iter::repeat_n(0u32, n), n, 1).unwrap();

        // V1 optional page layout: [u32 def-levels length][def-levels][values].
        // The values buffer is left empty -- the shape this branch must tolerate.
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&(def_levels.len() as u32).to_le_bytes());
        buffer.extend_from_slice(&def_levels);

        let page = TestDataPage {
            header: DataPageHeader::V1(DataPageHeaderV1 {
                num_values: n as i32,
                encoding: Encoding::DeltaBinaryPacked.into(),
                definition_level_encoding: Encoding::Rle.into(),
                repetition_level_encoding: Encoding::Rle.into(),
                statistics: None,
            }),
            descriptor: Descriptor {
                primitive_type: PrimitiveType {
                    field_info: FieldInfo {
                        name: "col".to_string(),
                        repetition: Repetition::Optional,
                        id: None,
                    },
                    logical_type: None,
                    converted_type: None,
                    physical_type: PhysicalType::Int64,
                },
                max_def_level: 1,
                max_rep_level: 0,
            },
            buffer,
        };
        let page = page.as_page();

        let mut bufs = ColumnChunkBuffers::new(allocator);
        let col_info = QdbMetaCol {
            column_type,
            column_top: 0,
            format: None,
            ascii: None,
            id: None,
        };

        decode_page(&page, None, &mut bufs, col_info, 0, n).unwrap();

        assert_eq!(bufs.data_vec.len(), n * size_of::<i64>());
        let out: &[i64] = unsafe { std::slice::from_raw_parts(bufs.data_vec.as_ptr().cast(), n) };
        assert_eq!(out, &[null_sentinel; 10]);
    }

    #[test]
    fn decode_page_empty_delta_buffer_decodes_all_null() {
        // LONG: the common all-null delta column type.
        assert_empty_delta_int64_page_all_null(ColumnTypeTag::Long.into_type(), i64::MIN);
    }

    #[test]
    fn decode_page_empty_delta_buffer_decimal64_all_null() {
        // Decimal64 (Int64 physical type) also dispatches DELTA pages, with its
        // own null sentinel. QuestDB's writer never emits DELTA for decimals, so
        // this reader-side path is only reachable from foreign files; covers the
        // distinct decode.rs dispatch arm.
        assert_empty_delta_int64_page_all_null(ColumnTypeTag::Decimal64.into_type(), i64::MIN);
    }

    // Decode an all-null V1 ByteArray page (n rows, every definition level 0) whose
    // values buffer is exactly `values`, through the real decode_page dispatch, and
    // return its (data_vec, aux_vec). With `values` empty this is the header-less
    // shape a foreign encoder can emit for an all-null DELTA varlen column and that
    // read_parquet() hands straight to the slicers.
    fn decode_all_null_varlen_page(
        column_type: ColumnType,
        encoding: Encoding,
        values: &[u8],
    ) -> (Vec<u8>, Vec<u8>) {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let n = 10usize;

        // Definition levels: n rows, all null (level 0), RLE-encoded (bit width 1).
        let mut def_levels = Vec::new();
        encode_u32(&mut def_levels, std::iter::repeat_n(0u32, n), n, 1).unwrap();

        // V1 optional page layout: [u32 def-levels length][def-levels][values].
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&(def_levels.len() as u32).to_le_bytes());
        buffer.extend_from_slice(&def_levels);
        buffer.extend_from_slice(values);

        let page = TestDataPage {
            header: DataPageHeader::V1(DataPageHeaderV1 {
                num_values: n as i32,
                encoding: encoding.into(),
                definition_level_encoding: Encoding::Rle.into(),
                repetition_level_encoding: Encoding::Rle.into(),
                statistics: None,
            }),
            descriptor: Descriptor {
                primitive_type: PrimitiveType {
                    field_info: FieldInfo {
                        name: "col".to_string(),
                        repetition: Repetition::Optional,
                        id: None,
                    },
                    logical_type: None,
                    converted_type: None,
                    physical_type: PhysicalType::ByteArray,
                },
                max_def_level: 1,
                max_rep_level: 0,
            },
            buffer,
        };
        let page = page.as_page();

        let mut bufs = ColumnChunkBuffers::new(allocator);
        let col_info = QdbMetaCol {
            column_type,
            column_top: 0,
            format: None,
            ascii: None,
            id: None,
        };

        decode_page(&page, None, &mut bufs, col_info, 0, n).unwrap();
        (
            bufs.data_vec.as_slice().to_vec(),
            bufs.aux_vec.as_slice().to_vec(),
        )
    }

    // A compliant zero-value delta header for `encoding` -- the self-describing
    // shape a spec-following encoder emits for an all-null page. It decodes cleanly
    // today (block_size/num_mini_blocks are non-zero), so it serves as the golden
    // reference for what the header-less empty-buffer page must decode to.
    fn compliant_zero_value_delta_values(encoding: Encoding) -> Vec<u8> {
        let mut values = Vec::new();
        match encoding {
            Encoding::DeltaLengthByteArray => {
                parquet2::encoding::delta_length_byte_array::encode(
                    std::iter::empty::<&[u8]>(),
                    &mut values,
                );
            }
            Encoding::DeltaByteArray => {
                parquet2::encoding::delta_byte_array::encode(
                    std::iter::empty::<&[u8]>(),
                    &mut values,
                );
            }
            other => panic!("unsupported delta encoding {other:?}"),
        }
        assert!(
            !values.is_empty(),
            "reference zero-value header must be non-empty"
        );
        values
    }

    // An all-null varlen page whose values buffer is EMPTY (no delta header) must
    // decode byte-identically to the same page carrying a compliant zero-value
    // delta header. For an all-null page the values buffer is never read, so the
    // two agree -- but before the slicer empty-buffer guard, the empty buffer drove
    // the vendored parquet2 delta decoder into a 0/0 division that panicked and
    // aborted the JVM across the JNI boundary.
    fn assert_empty_delta_varlen_page_all_null(column_type: ColumnType, encoding: Encoding) {
        let reference = compliant_zero_value_delta_values(encoding);
        let (empty_data, empty_aux) = decode_all_null_varlen_page(column_type, encoding, &[]);
        let (ref_data, ref_aux) = decode_all_null_varlen_page(column_type, encoding, &reference);
        assert_eq!(
            empty_data, ref_data,
            "data_vec from empty buffer must match the compliant zero-value page"
        );
        assert_eq!(
            empty_aux, ref_aux,
            "aux_vec from empty buffer must match the compliant zero-value page"
        );
    }

    #[test]
    fn decode_page_empty_delta_length_buffer_string_all_null() {
        assert_empty_delta_varlen_page_all_null(
            ColumnTypeTag::String.into_type(),
            Encoding::DeltaLengthByteArray,
        );
    }

    #[test]
    fn decode_page_empty_delta_length_buffer_varchar_all_null() {
        assert_empty_delta_varlen_page_all_null(
            ColumnTypeTag::Varchar.into_type(),
            Encoding::DeltaLengthByteArray,
        );
    }

    #[test]
    fn decode_page_empty_delta_length_buffer_binary_all_null() {
        assert_empty_delta_varlen_page_all_null(
            ColumnTypeTag::Binary.into_type(),
            Encoding::DeltaLengthByteArray,
        );
    }

    #[test]
    fn decode_page_empty_delta_length_buffer_array_all_null() {
        assert_empty_delta_varlen_page_all_null(
            encode_array_type(ColumnTypeTag::Double, 1).unwrap(),
            Encoding::DeltaLengthByteArray,
        );
    }

    #[test]
    fn decode_page_empty_delta_byte_array_buffer_varchar_all_null() {
        assert_empty_delta_varlen_page_all_null(
            ColumnTypeTag::Varchar.into_type(),
            Encoding::DeltaByteArray,
        );
    }

    #[test]
    fn decode_page_empty_delta_byte_array_buffer_varchar_slice_all_null() {
        assert_empty_delta_varlen_page_all_null(
            ColumnTypeTag::VarcharSlice.into_type(),
            Encoding::DeltaByteArray,
        );
    }

    #[test]
    fn decode_page_empty_delta_length_buffer_string_null_layout() {
        // Anchors the golden-reference checks above: confirm the all-null result is
        // genuine, not merely a non-panicking artifact. A QuestDB String column
        // materializes each null row as a -1 length marker (4 bytes), no payload.
        let (data, _aux) = decode_all_null_varlen_page(
            ColumnTypeTag::String.into_type(),
            Encoding::DeltaLengthByteArray,
            &[],
        );
        assert_eq!(data.len(), 10 * size_of::<i32>());
        let lengths: &[i32] = unsafe { std::slice::from_raw_parts(data.as_ptr().cast(), 10) };
        assert_eq!(lengths, &[-1i32; 10]);
    }

    #[test]
    fn decode_page_empty_delta_length_buffer_partial_null_errors_not_panics() {
        // A corrupt page that claims a non-null (definition level 1) yet provides
        // an EMPTY values buffer must surface a clean decode error, never abort the
        // JVM. The empty-buffer try_new guard alone would relocate the parquet2 0/0
        // panic into DeltaLengthArraySlicer::next()'s length-index lookup; the
        // bounds check there turns it into a Layout error instead.
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let n = 10usize;

        // One non-null followed by nine nulls, RLE-encoded (bit width 1).
        let levels = [1u32, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let mut def_levels = Vec::new();
        encode_u32(&mut def_levels, levels.into_iter(), n, 1).unwrap();

        let mut buffer = Vec::new();
        buffer.extend_from_slice(&(def_levels.len() as u32).to_le_bytes());
        buffer.extend_from_slice(&def_levels);
        // values buffer intentionally empty -- contradicts the claimed non-null.

        let page = TestDataPage {
            header: DataPageHeader::V1(DataPageHeaderV1 {
                num_values: n as i32,
                encoding: Encoding::DeltaLengthByteArray.into(),
                definition_level_encoding: Encoding::Rle.into(),
                repetition_level_encoding: Encoding::Rle.into(),
                statistics: None,
            }),
            descriptor: Descriptor {
                primitive_type: PrimitiveType {
                    field_info: FieldInfo {
                        name: "col".to_string(),
                        repetition: Repetition::Optional,
                        id: None,
                    },
                    logical_type: None,
                    converted_type: None,
                    physical_type: PhysicalType::ByteArray,
                },
                max_def_level: 1,
                max_rep_level: 0,
            },
            buffer,
        };
        let page = page.as_page();

        let mut bufs = ColumnChunkBuffers::new(allocator);
        let col_info = QdbMetaCol {
            column_type: ColumnTypeTag::String.into_type(),
            column_top: 0,
            format: None,
            ascii: None,
            id: None,
        };

        let err = decode_page(&page, None, &mut bufs, col_info, 0, n)
            .expect_err("a non-null claim over an empty values buffer must be a decode error");
        assert!(
            err.to_string().contains("not enough length values"),
            "unexpected error: {err}"
        );
    }

    // Decode an all-non-null V1 ByteArray page of `strings` under `encoding` through
    // the real decode_page dispatch, reading only rows [0, row_hi). Returns its
    // (data_vec, aux_vec). With row_hi < strings.len() this is the partial-range read
    // that regressed C1: decode_byte_array_dispatch hands row_hi straight to the
    // DELTA slicer, so the value-bytes offset must be computed over the whole length
    // stream, not the blocks take(row_hi) entered.
    fn decode_delta_varlen_page_partial(
        column_type: ColumnType,
        encoding: Encoding,
        strings: &[&[u8]],
        row_hi: usize,
    ) -> (Vec<u8>, Vec<u8>) {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let n = strings.len();

        let mut values = Vec::new();
        match encoding {
            Encoding::DeltaLengthByteArray => {
                parquet2::encoding::delta_length_byte_array::encode(
                    strings.iter().copied(),
                    &mut values,
                );
            }
            Encoding::DeltaByteArray => {
                parquet2::encoding::delta_byte_array::encode(strings.iter().copied(), &mut values);
            }
            other => panic!("unsupported delta encoding {other:?}"),
        }

        // Definition levels: n rows, all non-null (level 1), RLE-encoded (bit width 1).
        let mut def_levels = Vec::new();
        encode_u32(&mut def_levels, std::iter::repeat_n(1u32, n), n, 1).unwrap();

        // V1 optional page layout: [u32 def-levels length][def-levels][values].
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&(def_levels.len() as u32).to_le_bytes());
        buffer.extend_from_slice(&def_levels);
        buffer.extend_from_slice(&values);

        let page = TestDataPage {
            header: DataPageHeader::V1(DataPageHeaderV1 {
                num_values: n as i32,
                encoding: encoding.into(),
                definition_level_encoding: Encoding::Rle.into(),
                repetition_level_encoding: Encoding::Rle.into(),
                statistics: None,
            }),
            descriptor: Descriptor {
                primitive_type: PrimitiveType {
                    field_info: FieldInfo {
                        name: "col".to_string(),
                        repetition: Repetition::Optional,
                        id: None,
                    },
                    logical_type: None,
                    converted_type: None,
                    physical_type: PhysicalType::ByteArray,
                },
                max_def_level: 1,
                max_rep_level: 0,
            },
            buffer,
        };
        let page = page.as_page();

        let mut bufs = ColumnChunkBuffers::new(allocator);
        let col_info = QdbMetaCol {
            column_type,
            column_top: 0,
            format: None,
            ascii: None,
            id: None,
        };

        decode_page(&page, None, &mut bufs, col_info, 0, row_hi).unwrap();
        (
            bufs.data_vec.as_slice().to_vec(),
            bufs.aux_vec.as_slice().to_vec(),
        )
    }

    // Reading the first `row_hi` values of a multi-block DELTA page must yield
    // exactly what fully decoding a page of just those `row_hi` values yields -- the
    // decoded prefix cannot depend on how many more values follow it. Before the
    // offset fix the partial read of the 300-value page (length stream spanning
    // three 128-value blocks) began inside the length stream and diverged from the
    // reference. 300 distinct varying-length values exercise non-zero-bit-width
    // later blocks, whose miniblock bytes the buggy offset also dropped.
    fn assert_delta_varlen_partial_matches_prefix(column_type: ColumnType, encoding: Encoding) {
        let owned: Vec<String> = (0..300).map(|i| format!("v{i}")).collect();
        let all: Vec<&[u8]> = owned.iter().map(|s| s.as_bytes()).collect();
        let row_hi = 5;

        let (partial_data, partial_aux) =
            decode_delta_varlen_page_partial(column_type, encoding, &all, row_hi);
        let (ref_data, ref_aux) =
            decode_delta_varlen_page_partial(column_type, encoding, &all[..row_hi], row_hi);

        assert_eq!(
            partial_data, ref_data,
            "partial multi-block decode data_vec must match the prefix-only decode"
        );
        assert_eq!(
            partial_aux, ref_aux,
            "partial multi-block decode aux_vec must match the prefix-only decode"
        );
    }

    #[test]
    fn decode_page_delta_length_string_partial_range_multi_block() {
        assert_delta_varlen_partial_matches_prefix(
            ColumnTypeTag::String.into_type(),
            Encoding::DeltaLengthByteArray,
        );
    }

    #[test]
    fn decode_page_delta_length_varchar_partial_range_multi_block() {
        assert_delta_varlen_partial_matches_prefix(
            ColumnTypeTag::Varchar.into_type(),
            Encoding::DeltaLengthByteArray,
        );
    }

    #[test]
    fn decode_page_delta_byte_array_varchar_partial_range_multi_block() {
        // The DeltaByteArray path (prefix + suffix streams, DeltaBytesArraySlicer) is
        // foreign-only today, but its offset carries the same latent bug, so guard
        // its partial-range read too.
        assert_delta_varlen_partial_matches_prefix(
            ColumnTypeTag::Varchar.into_type(),
            Encoding::DeltaByteArray,
        );
    }

    // Like decode_delta_varlen_page_partial but with an explicit null pattern:
    // def_levels[i] == 1 marks a non-null row, 0 a null row, and non_null_strings
    // holds only the non-null values (its length must equal the number of 1s). The
    // DELTA_LENGTH stream encodes only the non-null lengths, so this exercises the
    // interaction between the ROW count (row_hi, which bounds the slicer's take())
    // and the smaller NON-NULL count the slicer actually yields.
    fn decode_delta_string_with_nulls_partial(
        def_levels: &[u32],
        non_null_strings: &[&[u8]],
        row_hi: usize,
    ) -> (Vec<u8>, Vec<u8>) {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let n = def_levels.len();
        assert_eq!(
            def_levels.iter().filter(|&&d| d == 1).count(),
            non_null_strings.len(),
            "non_null_strings must match the number of non-null def levels"
        );

        let mut values = Vec::new();
        parquet2::encoding::delta_length_byte_array::encode(
            non_null_strings.iter().copied(),
            &mut values,
        );

        let mut def_buf = Vec::new();
        encode_u32(&mut def_buf, def_levels.iter().copied(), n, 1).unwrap();

        // V1 optional page layout: [u32 def-levels length][def-levels][values].
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&(def_buf.len() as u32).to_le_bytes());
        buffer.extend_from_slice(&def_buf);
        buffer.extend_from_slice(&values);

        let page = TestDataPage {
            header: DataPageHeader::V1(DataPageHeaderV1 {
                num_values: n as i32,
                encoding: Encoding::DeltaLengthByteArray.into(),
                definition_level_encoding: Encoding::Rle.into(),
                repetition_level_encoding: Encoding::Rle.into(),
                statistics: None,
            }),
            descriptor: Descriptor {
                primitive_type: PrimitiveType {
                    field_info: FieldInfo {
                        name: "col".to_string(),
                        repetition: Repetition::Optional,
                        id: None,
                    },
                    logical_type: None,
                    converted_type: None,
                    physical_type: PhysicalType::ByteArray,
                },
                max_def_level: 1,
                max_rep_level: 0,
            },
            buffer,
        };
        let page = page.as_page();

        let mut bufs = ColumnChunkBuffers::new(allocator);
        let col_info = QdbMetaCol {
            column_type: ColumnTypeTag::String.into_type(),
            column_top: 0,
            format: None,
            ascii: None,
            id: None,
        };

        decode_page(&page, None, &mut bufs, col_info, 0, row_hi).unwrap();
        (
            bufs.data_vec.as_slice().to_vec(),
            bufs.aux_vec.as_slice().to_vec(),
        )
    }

    #[test]
    fn decode_page_delta_length_string_partial_range_multi_block_with_nulls() {
        // The partial-range multi-block DELTA decode, but with NULLs in the cut
        // range. row_hi is a ROW count and bounds the slicer's take(), while the
        // DELTA length stream holds only the NON-NULL lengths -- a smaller count. A
        // partial read must still locate the value bytes correctly and place
        // nulls/values in the right rows. 300 rows with a null every 7th row give
        // 258 non-null values (a 3-block length stream); cutting at row 137 lands
        // inside the stream with 19 nulls before the cut. Reading [0, 137) from the
        // full page must match decoding a page of just those first 137 rows.
        let n = 300usize;
        let row_hi = 137usize;
        let def_levels: Vec<u32> = (0..n).map(|i| if i % 7 == 6 { 0 } else { 1 }).collect();
        // Varying-length non-null values so the delta blocks carry non-zero bit
        // widths (real miniblock bytes the offset walk must skip).
        let owned: Vec<String> = (0..n)
            .filter(|i| def_levels[*i] == 1)
            .map(|i| "ab".repeat(1 + (i % 9)))
            .collect();
        let non_null: Vec<&[u8]> = owned.iter().map(|s| s.as_bytes()).collect();

        let (partial_data, partial_aux) =
            decode_delta_string_with_nulls_partial(&def_levels, &non_null, row_hi);

        // Reference: a page of just the first row_hi rows (same null pattern, same
        // non-null values among them), decoded over the same range.
        let ref_def: Vec<u32> = def_levels[..row_hi].to_vec();
        let ref_non_null_count = ref_def.iter().filter(|&&d| d == 1).count();
        let ref_non_null: Vec<&[u8]> = non_null[..ref_non_null_count].to_vec();
        let (ref_data, ref_aux) =
            decode_delta_string_with_nulls_partial(&ref_def, &ref_non_null, row_hi);

        assert_eq!(
            partial_data, ref_data,
            "partial multi-block decode with nulls must match the prefix-only decode"
        );
        assert_eq!(
            partial_aux, ref_aux,
            "partial multi-block decode with nulls must match the prefix-only decode"
        );
    }

    #[test]
    fn decode_page_dict_num_values_over_buffer_errors_not_panics() {
        // End-to-end through decode_page: a dictionary-encoded Varchar column
        // whose dictionary page header claims far more values (i32::MAX) than its
        // buffer can hold. decode_page builds BaseVarDictDecoder, whose num_values
        // guard must surface a clean Layout error before it reaches the
        // dictionary-value reservation (try_reserve_dict_values), whose
        // tens-of-gigabytes request would otherwise abort the JVM when the
        // allocator refuses it. Proves the guard propagates through the full
        // decode dispatch, not just the direct constructor. The reservation
        // allocates eagerly, but whether such a request actually aborts is
        // allocator/overcommit dependent, so this test pins propagation of the
        // guard's rejection, not the abort.
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        // A valid 1-entry var-width dictionary buffer, but a lying header.
        let mut dict_page = make_dict_page_var(&[b"aaa".to_vec()]);
        dict_page.num_values = i32::MAX as usize;
        let dict_page = dict_page.as_page();

        let indices = [0u32];
        let page = make_dict_data_page(make_byte_array_type(), Encoding::RleDictionary, &indices);
        let page = page.as_page();

        let mut bufs = ColumnChunkBuffers::new(allocator);
        let col_info = QdbMetaCol {
            column_type: ColumnTypeTag::Varchar.into_type(),
            column_top: 0,
            format: None,
            ascii: None,
            id: None,
        };

        let err = decode_page(
            &page,
            Some(&dict_page),
            &mut bufs,
            col_info,
            0,
            indices.len(),
        )
        .expect_err("an oversized dictionary num_values must error, not abort");
        assert!(
            err.to_string().contains("too short to hold"),
            "unexpected error: {err}"
        );
        // The rejected decode must free everything it allocated on the way to the
        // error: no leak across JNI on the abort-class path it replaces.
        drop(bufs);
        assert_eq!(tas.rss_mem_used(), 0, "decode error path leaked memory");
    }

    #[test]
    fn decode_page_dict_index_bitwidth_over_32_errors_not_panics() {
        // End-to-end through decode_page: a dictionary-encoded String column whose
        // index bit width (40) exceeds the 32-bit u32 the indices unpack into. The
        // RLE_DICTIONARY index decode reaches bitpacked::Decoder::<u32>::try_new,
        // whose num_bits guard must surface a clean error rather than reaching
        // unreachable!("invalid num_bits 40") and aborting the JVM. Proves the
        // guard propagates through the full decode dispatch. Values-buffer wire
        // format: [40 = bit width, 0x03 = bitpacked indicator (1 group of 8), then
        // 40 data bytes = 8 * 40 bits].
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        // A valid var-width dictionary so the only fault is the index bit width.
        let dict_page = make_dict_page_var(&[b"aaa".to_vec(), b"bbb".to_vec(), b"ccc".to_vec()]);
        let dict_page = dict_page.as_page();

        let mut values = vec![40u8, 0x03];
        values.extend(std::iter::repeat_n(0u8, 40));
        let page = make_required_page(make_byte_array_type(), Encoding::RleDictionary, values, 8);
        let page = page.as_page();

        let mut bufs = ColumnChunkBuffers::new(allocator);
        let col_info = QdbMetaCol {
            column_type: ColumnTypeTag::String.into_type(),
            column_top: 0,
            format: None,
            ascii: None,
            id: None,
        };

        let err = decode_page(&page, Some(&dict_page), &mut bufs, col_info, 0, 8)
            .expect_err("an index bit width over 32 must error, not abort");
        assert!(
            err.to_string().contains("exceeds"),
            "unexpected error: {err}"
        );
        // The rejected decode must free everything it allocated on the way to the
        // error: no leak across JNI on the abort-class path it replaces.
        drop(bufs);
        assert_eq!(tas.rss_mem_used(), 0, "decode error path leaked memory");
    }

    #[test]
    fn resize_decompress_buffer_rejects_unsatisfiable_size_instead_of_aborting() {
        // uncompressed_page_size is attacker-controlled and drives the up-front
        // decompression-buffer allocation. Unlike compressed_page_size it is not
        // bounded by max_page_size on the read path (a highly compressible page
        // can legitimately decompress to more than the compressed chunk), so it
        // cannot be range-checked; the buffer sizing must instead be fallible. A
        // genuine i32::MAX (~2 GiB) request may succeed on a large host, so to pin
        // the fallible path deterministically we ask for usize::MAX: try_reserve
        // fails with CapacityOverflow without attempting (and aborting on) a real
        // allocation. Proves the sizing surfaces a clean error rather than the
        // process-aborting Vec::resize.
        let mut buf = Vec::new();
        let err = resize_decompress_buffer(&mut buf, usize::MAX)
            .expect_err("an unsatisfiable decompressed size must error, not abort");
        assert!(
            err.to_string().contains("cannot allocate"),
            "unexpected error: {err}"
        );
        // The failure must be classified OutOfMemory, not Layout: on the write
        // path (parquet merges under ApplyWal2TableJob) a Layout error suspends
        // the table, whereas OutOfMemory backs off and retries -- the correct
        // response to transient memory pressure.
        assert!(
            matches!(err.reason(), ParquetErrorReason::OutOfMemory(_)),
            "allocation failure must be classified OutOfMemory, not Layout: {err:?}"
        );
    }

    #[test]
    fn resize_decompress_buffer_grows_in_place_without_clearing() {
        // The helper threads one buffer through every page of a file. It must
        // size in place without clear()ing -- a clear() would make resize memset
        // the whole uncompressed_page_size on every page (a per-page regression)
        // only for the decompressor to overwrite it. Pin the observable contract:
        // growing preserves the bytes already present (zeroing only the added
        // tail), and shrinking truncates. A clear()-first implementation would
        // zero byte 0 here and fail this assertion.
        let mut buf = vec![0xAB_u8; 4];
        resize_decompress_buffer(&mut buf, 8).unwrap();
        assert_eq!(buf.len(), 8);
        assert_eq!(&buf[..4], &[0xAB; 4], "grow must not memset existing bytes");
        assert_eq!(&buf[4..], &[0; 4], "grown tail must be zeroed");

        resize_decompress_buffer(&mut buf, 2).unwrap();
        assert_eq!(buf.as_slice(), &[0xAB; 2], "shrink must truncate in place");
    }

    #[test]
    fn decompress_sliced_data_v2_uncompressed_does_not_size_buffer() {
        fn v2_uncompressed_page(body: &[u8], uncompressed_size: usize) -> SlicedDataPage<'_> {
            SlicedDataPage {
                header: DataPageHeader::V2(DataPageHeaderV2 {
                    num_values: 1,
                    num_nulls: 0,
                    num_rows: 1,
                    encoding: Encoding::Plain.into(),
                    definition_levels_byte_length: 0,
                    repetition_levels_byte_length: 0,
                    is_compressed: Some(false),
                    statistics: None,
                }),
                buffer: body,
                compression: Compression::Snappy,
                uncompressed_size,
                descriptor: Descriptor {
                    primitive_type: PrimitiveType::from_physical(
                        "c".to_string(),
                        PhysicalType::Int32,
                    ),
                    max_def_level: 0,
                    max_rep_level: 0,
                },
            }
        }

        // A V2 page with is_compressed=false is already uncompressed: the body is
        // returned as-is and the decompress buffer is never read. It must not be
        // sized -- the pre-hoist code resized it to the attacker-controlled
        // uncompressed_size (up to i32::MAX, unbounded by max_page_size) purely to
        // compare its length, memsetting a buffer with no decompression to amortize.
        let body = [10u8, 20, 30, 40];
        let page = v2_uncompressed_page(&body, body.len());
        let mut decompress_buffer = Vec::new();
        {
            let data_page = decompress_sliced_data(&page, &mut decompress_buffer).unwrap();
            assert_eq!(data_page.buffer, &body[..]);
            assert_eq!(
                data_page.buffer.as_ptr(),
                body.as_ptr(),
                "must return the page body itself, not a decompressed copy"
            );
        }
        assert_eq!(
            decompress_buffer.capacity(),
            0,
            "is_compressed=false must not allocate or size the decompress buffer"
        );

        // The length check is preserved, now compared directly against the body:
        // a header whose uncompressed_size disagrees with the body still errors.
        let bad = v2_uncompressed_page(&body, body.len() + 1);
        let mut scratch = Vec::new();
        let err = decompress_sliced_data(&bad, &mut scratch).unwrap_err();
        assert!(
            err.to_string().contains("incorrect decompressed size"),
            "unexpected error: {err}"
        );
    }

    fn snappy_compress(bytes: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        compress(CompressionOptions::Snappy, bytes, &mut out).unwrap();
        out
    }

    fn int32_descriptor() -> Descriptor {
        Descriptor {
            primitive_type: PrimitiveType::from_physical("c".to_string(), PhysicalType::Int32),
            max_def_level: 0,
            max_rep_level: 0,
        }
    }

    #[test]
    fn decompress_sliced_data_v1_compressed_round_trips() {
        // V1 compressed arm: resize_decompress_buffer sizes the reused buffer to
        // uncompressed_size, then the whole page body is decompressed into it.
        let values: Vec<u8> = (0..64u8).collect();
        let compressed = snappy_compress(&values);
        let page = SlicedDataPage {
            header: DataPageHeader::V1(DataPageHeaderV1 {
                num_values: values.len() as i32,
                encoding: Encoding::Plain.into(),
                definition_level_encoding: Encoding::Rle.into(),
                repetition_level_encoding: Encoding::Rle.into(),
                statistics: None,
            }),
            buffer: &compressed,
            compression: Compression::Snappy,
            uncompressed_size: values.len(),
            descriptor: int32_descriptor(),
        };
        let mut decompress_buffer = Vec::new();
        let data_page = decompress_sliced_data(&page, &mut decompress_buffer).unwrap();
        assert_eq!(data_page.buffer, &values[..]);
    }

    #[test]
    fn decompress_sliced_data_v2_compressed_round_trips() {
        // V2 compressed arm (can_decompress=true): the first `offset` bytes are
        // uncompressed levels copied verbatim, and only the values after them are
        // compressed and decompressed into decompress_buffer[offset..].
        let levels = [0xAAu8, 0xBB, 0xCC];
        let values: Vec<u8> = (0..40u8).collect();
        let mut body = levels.to_vec();
        body.extend_from_slice(&snappy_compress(&values));
        let page = SlicedDataPage {
            header: DataPageHeader::V2(DataPageHeaderV2 {
                num_values: values.len() as i32,
                num_nulls: 0,
                num_rows: values.len() as i32,
                encoding: Encoding::Plain.into(),
                definition_levels_byte_length: levels.len() as i32,
                repetition_levels_byte_length: 0,
                is_compressed: Some(true),
                statistics: None,
            }),
            buffer: &body,
            compression: Compression::Snappy,
            uncompressed_size: levels.len() + values.len(),
            descriptor: int32_descriptor(),
        };
        let mut decompress_buffer = Vec::new();
        let data_page = decompress_sliced_data(&page, &mut decompress_buffer).unwrap();
        let mut expected = levels.to_vec();
        expected.extend_from_slice(&values);
        assert_eq!(data_page.buffer, &expected[..]);
    }

    #[test]
    fn decompress_sliced_dict_compressed_round_trips() {
        // decompress_sliced_dict sizes the buffer to uncompressed_size and
        // decompresses the whole dict page body into it.
        let values: Vec<u8> = (0..48u8).collect();
        let compressed = snappy_compress(&values);
        let page = SlicedDictPage {
            buffer: &compressed,
            compression: Compression::Snappy,
            uncompressed_size: values.len(),
            num_values: 12,
            is_sorted: false,
        };
        let mut buffer = Vec::new();
        let dict = decompress_sliced_dict(page, &mut buffer).unwrap();
        assert_eq!(dict.buffer, &values[..]);
        assert_eq!(dict.num_values, 12);
    }

    #[test]
    fn decompress_sliced_data_compressed_lying_size_errors() {
        // A compressed page whose uncompressed_size under-claims the real
        // decompressed length must surface a clean error, not panic or abort:
        // resize_decompress_buffer sizes the output too small, so the codec
        // cannot fill it and returns an error that propagates out.
        let values: Vec<u8> = (0..64u8).collect();
        let compressed = snappy_compress(&values);
        let page = SlicedDataPage {
            header: DataPageHeader::V1(DataPageHeaderV1 {
                num_values: values.len() as i32,
                encoding: Encoding::Plain.into(),
                definition_level_encoding: Encoding::Rle.into(),
                repetition_level_encoding: Encoding::Rle.into(),
                statistics: None,
            }),
            buffer: &compressed,
            compression: Compression::Snappy,
            uncompressed_size: values.len() - 1, // lies: one byte short of the real output
            descriptor: int32_descriptor(),
        };
        let mut decompress_buffer = Vec::new();
        assert!(
            decompress_sliced_data(&page, &mut decompress_buffer).is_err(),
            "a compressed page whose uncompressed_size under-claims the body must error"
        );
    }

    #[test]
    fn decompress_sliced_data_v2_compressed_lying_size_errors() {
        // V2 counterpart of the V1 lying-size test: a V2 compressed page whose
        // uncompressed_size under-claims the real decompressed length (levels +
        // values) must surface a clean error, not panic or abort. The PR hoisted
        // the buffer sizing into the can_decompress arm, so resize_decompress_buffer
        // sizes decompress_buffer one byte short of levels+values, leaving its
        // [offset..] values region too small for the codec to fill -- the error must
        // propagate out rather than reach an unwritten tail.
        let levels = [0xAAu8, 0xBB, 0xCC];
        let values: Vec<u8> = (0..40u8).collect();
        let mut body = levels.to_vec();
        body.extend_from_slice(&snappy_compress(&values));
        let page = SlicedDataPage {
            header: DataPageHeader::V2(DataPageHeaderV2 {
                num_values: values.len() as i32,
                num_nulls: 0,
                num_rows: values.len() as i32,
                encoding: Encoding::Plain.into(),
                definition_levels_byte_length: levels.len() as i32,
                repetition_levels_byte_length: 0,
                is_compressed: Some(true),
                statistics: None,
            }),
            buffer: &body,
            compression: Compression::Snappy,
            uncompressed_size: levels.len() + values.len() - 1, // lies: one byte short
            descriptor: int32_descriptor(),
        };
        let mut decompress_buffer = Vec::new();
        assert!(
            decompress_sliced_data(&page, &mut decompress_buffer).is_err(),
            "a V2 compressed page whose uncompressed_size under-claims the body must error"
        );
    }

    // Regenerates the committed end-to-end fixture
    // core/src/test/resources/sqllogictest/data/parquet-testing/broken/rle_dict_index_bitwidth_over_32.parquet
    // read by the Java `ReadParquetFunctionTest`. Ignored by default; run with
    // `cargo test -p qdbr -- --ignored generate_rle_dict_index_bitwidth_fixture`
    // after the on-disk page layout changes. It writes a valid dictionary-encoded
    // parquet, locates the RLE_DICTIONARY index bit-width byte (the first byte of
    // a required column's data page) by pointer offset, patches it to 40 (> 32),
    // and asserts the patched bytes trip the guard with a clean "exceeds" error
    // (not a JVM abort) before committing them.
    #[test]
    #[ignore = "regenerates a committed test fixture on disk"]
    fn generate_rle_dict_index_bitwidth_fixture() {
        // Low cardinality (2 distinct values) so the writer dictionary-encodes it.
        let values: Vec<i32> = vec![7, 7, 7, 9, 9, 7, 9, 7];

        let col = std::sync::Arc::new(
            parquet::schema::types::Type::primitive_type_builder("v", parquet::basic::Type::INT32)
                .with_id(Some(0))
                .with_repetition(parquet::basic::Repetition::REQUIRED)
                .build()
                .unwrap(),
        );
        let schema = std::sync::Arc::new(
            parquet::schema::types::Type::group_type_builder("schema")
                .with_fields(vec![col])
                .build()
                .unwrap(),
        );
        let props = std::sync::Arc::new(
            parquet::file::properties::WriterProperties::builder()
                .set_dictionary_enabled(true)
                .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_1_0)
                .build(),
        );

        let mut cursor = Cursor::new(Vec::new());
        {
            let mut file_writer =
                parquet::file::writer::SerializedFileWriter::new(&mut cursor, schema, props)
                    .unwrap();
            let mut row_group_writer = file_writer.next_row_group().unwrap();
            let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
            col_writer
                .typed::<parquet::data_type::Int32Type>()
                .write_batch(&values, None, None)
                .unwrap();
            col_writer.close().unwrap();
            row_group_writer.close().unwrap();
            file_writer.close().unwrap();
        }
        let mut file = cursor.into_inner();

        // Locate the data page's first byte (the dict-index bit width) by reading
        // the column chunk's pages as slices into `file` and taking the pointer
        // offset of the first data page (a required column carries no levels, so
        // the bit-width byte is buffer[0]).
        let bitwidth_offset = {
            let metadata =
                parquet2::read::read_metadata(&mut Cursor::new(file.as_slice())).unwrap();
            let column = &metadata.row_groups[0].columns()[0];
            let reader = parquet2::read::SlicePageReader::new(&file, column, 1 << 20).unwrap();
            let mut saw_dict = false;
            let mut offset = None;
            for page in reader {
                match page.unwrap() {
                    parquet2::read::SlicedPage::Dict(_) => saw_dict = true,
                    parquet2::read::SlicedPage::Data(data) => {
                        offset = Some((data.buffer.as_ptr() as usize) - (file.as_ptr() as usize));
                        break;
                    }
                }
            }
            assert!(
                saw_dict,
                "column was not dictionary-encoded; adjust the values"
            );
            offset.expect("expected a data page after the dictionary page")
        };

        let original = file[bitwidth_offset];
        assert!(
            original <= 32,
            "expected a small real bit width at the located offset, got {original}"
        );
        file[bitwidth_offset] = 40; // > 32: trips bitpacked::Decoder::<u32>::try_new

        // The patched bytes must trip the guard with a clean error, not abort.
        {
            let tas = TestAllocatorState::new();
            let allocator = tas.allocator();
            let buf_len = file.len() as u64;
            let mut reader = Cursor::new(&file);
            let decoder = ParquetDecoder::read(allocator.clone(), &mut reader, buf_len).unwrap();
            let mut rgb = RowGroupBuffers::new(allocator);
            let mut ctx = DecodeContext::new(file.as_ptr(), buf_len);
            let err = decoder
                .decode_row_group(
                    &mut ctx,
                    &mut rgb,
                    &[(0, ColumnTypeTag::Int.into_type())],
                    0,
                    0,
                    values.len() as u32,
                )
                .expect_err("the patched fixture must error, not abort");
            assert!(
                err.to_string().contains("exceeds"),
                "fixture must trip the bit-width guard, got: {err}"
            );
        }

        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../src/test/resources/sqllogictest/data/parquet-testing/broken/rle_dict_index_bitwidth_over_32.parquet"
        );
        std::fs::write(path, &file).unwrap();
    }

    // Regenerates the committed end-to-end fixture
    // core/src/test/resources/sqllogictest/data/parquet-testing/broken/dict_num_values_over_buffer.parquet
    // read by the Java `ReadParquetFunctionTest`. Ignored by default; run with
    // `cargo test -p qdbr -- --ignored generate_dict_num_values_over_buffer_fixture`
    // after the on-disk page layout changes. It writes a valid dictionary-encoded
    // BYTE_ARRAY column, then patches the dictionary page header's num_values to a
    // count the dict buffer cannot hold (each value needs at least a 4-byte length
    // prefix). BaseVarDictDecoder::try_new now rejects that up front with "too short
    // to hold"; before the guard it reserved a Vec sized by the attacker-controlled
    // num_values (up to ~2.1 billion entries), and the allocator refusing that
    // multi-gigabyte request aborted the JVM over JNI. Asserts the patched bytes trip
    // the guard with a clean error before committing them. The magnitude that
    // actually drives the abort (i32::MAX) is covered by the Rust unit tests; this
    // fixture pins that the guard's rejection crosses the JNI boundary as a clean
    // CairoException rather than an abort.
    #[test]
    #[ignore = "regenerates a committed test fixture on disk"]
    fn generate_dict_num_values_over_buffer_fixture() {
        // Low cardinality (4 distinct short values) so the writer dictionary-encodes
        // it; UNCOMPRESSED so the on-disk dict page buffer is the raw dict bytes the
        // num_values guard measures against.
        let strings = ["aa", "bb", "cc", "dd", "aa", "cc", "bb", "dd"];
        let values: Vec<parquet::data_type::ByteArray> = strings
            .iter()
            .map(|s| parquet::data_type::ByteArray::from(s.as_bytes().to_vec()))
            .collect();

        let col = std::sync::Arc::new(
            parquet::schema::types::Type::primitive_type_builder(
                "v",
                parquet::basic::Type::BYTE_ARRAY,
            )
            .with_id(Some(0))
            .with_repetition(parquet::basic::Repetition::REQUIRED)
            // STRING logical type so QuestDB infers VARCHAR (the common foreign-string
            // case), whose dictionary decode routes through BaseVarDictDecoder.
            .with_logical_type(Some(parquet::basic::LogicalType::String))
            .build()
            .unwrap(),
        );
        let schema = std::sync::Arc::new(
            parquet::schema::types::Type::group_type_builder("schema")
                .with_fields(vec![col])
                .build()
                .unwrap(),
        );
        let props = std::sync::Arc::new(
            parquet::file::properties::WriterProperties::builder()
                .set_dictionary_enabled(true)
                .set_compression(parquet::basic::Compression::UNCOMPRESSED)
                .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_1_0)
                .build(),
        );

        let mut cursor = Cursor::new(Vec::new());
        {
            let mut file_writer =
                parquet::file::writer::SerializedFileWriter::new(&mut cursor, schema, props)
                    .unwrap();
            let mut row_group_writer = file_writer.next_row_group().unwrap();
            let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
            col_writer
                .typed::<parquet::data_type::ByteArrayType>()
                .write_batch(&values, None, None)
                .unwrap();
            col_writer.close().unwrap();
            row_group_writer.close().unwrap();
            file_writer.close().unwrap();
        }
        let mut file = cursor.into_inner();

        // Locate the dictionary page header (from dictionary_page_offset to the dict
        // page body the reader hands back) and its real num_values.
        let (dict_offset, body_start, dict_num_values, dict_buf_len) = {
            let metadata =
                parquet2::read::read_metadata(&mut Cursor::new(file.as_slice())).unwrap();
            let column = &metadata.row_groups[0].columns()[0];
            let dict_offset = column
                .dictionary_page_offset()
                .expect("column was not dictionary-encoded; adjust the values")
                as usize;
            let reader = parquet2::read::SlicePageReader::new(&file, column, 1 << 20).unwrap();
            let mut found = None;
            for page in reader {
                if let parquet2::read::SlicedPage::Dict(dict) = page.unwrap() {
                    let body_start = (dict.buffer.as_ptr() as usize) - (file.as_ptr() as usize);
                    found = Some((body_start, dict.num_values, dict.buffer.len()));
                    break;
                }
            }
            let (body_start, num_values, buf_len) = found.expect("expected a dictionary page");
            (dict_offset, body_start, num_values, buf_len)
        };

        // The original count must fit the buffer (a valid page) and encode in a
        // single zigzag byte so the in-place patch preserves the header length: for a
        // small non-negative i32 the thrift-compact zigzag varint is one byte equal
        // to num_values * 2.
        assert!(
            dict_num_values <= dict_buf_len / 4,
            "expected a valid dict (num_values {dict_num_values} <= buffer/4 {})",
            dict_buf_len / 4
        );
        assert!(
            dict_num_values < 64,
            "num_values must be a 1-byte zigzag varint"
        );

        // A patched count that overflows the buffer guard yet still fits one zigzag
        // byte. 63 * 4 bytes per value far exceeds the few-dozen-byte dict buffer.
        const PATCHED: usize = 63;
        assert!(
            PATCHED > dict_buf_len / 4,
            "patched num_values {PATCHED} must exceed buffer/4 {}",
            dict_buf_len / 4
        );

        // Find the num_values byte: in thrift-compact the PageHeader encodes the
        // DictionaryPageHeader as a struct field (low nibble 0xC), whose first field
        // is num_values (i32 field header 0x15) followed by its zigzag byte.
        let header = &file[dict_offset..body_start];
        let expect_byte = (dict_num_values as u8) * 2;
        let mut hits = header
            .windows(3)
            .enumerate()
            .filter(|(_, w)| (w[0] & 0x0F) == 0x0C && w[1] == 0x15 && w[2] == expect_byte);
        let (rel, _) = hits
            .next()
            .expect("could not locate the dictionary num_values field in the page header");
        assert!(
            hits.next().is_none(),
            "ambiguous num_values field location; tighten the search"
        );
        let num_values_byte = dict_offset + rel + 2;
        assert_eq!(file[num_values_byte], expect_byte);
        file[num_values_byte] = (PATCHED as u8) * 2;

        // The patched bytes must trip the num_values guard with a clean error, not abort.
        {
            let tas = TestAllocatorState::new();
            let allocator = tas.allocator();
            let buf_len = file.len() as u64;
            let mut reader = Cursor::new(&file);
            let decoder = ParquetDecoder::read(allocator.clone(), &mut reader, buf_len).unwrap();
            let mut rgb = RowGroupBuffers::new(allocator);
            let mut ctx = DecodeContext::new(file.as_ptr(), buf_len);
            let err = decoder
                .decode_row_group(
                    &mut ctx,
                    &mut rgb,
                    &[(0, ColumnTypeTag::Varchar.into_type())],
                    0,
                    0,
                    strings.len() as u32,
                )
                .expect_err("the patched fixture must error, not abort");
            assert!(
                err.to_string().contains("too short to hold"),
                "fixture must trip the num_values guard, got: {err}"
            );
        }

        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../src/test/resources/sqllogictest/data/parquet-testing/broken/dict_num_values_over_buffer.parquet"
        );
        std::fs::write(path, &file).unwrap();
    }

    #[test]
    fn test_decode_flba_decimal_sign_extended_filtered() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let src_len = 16;
        let values = [
            [
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x7B,
            ],
            [0xFF; 16],
        ];
        let mut buffer = Vec::new();
        for v in values {
            buffer.extend_from_slice(&v);
        }

        let page = make_required_page(
            make_decimal_flba_type(src_len, 10, 2),
            Encoding::Plain,
            buffer,
            values.len(),
        );
        let page = page.as_page();

        let mut bufs = ColumnChunkBuffers::new(allocator);
        let col_info = QdbMetaCol {
            column_type: ColumnTypeTag::Decimal64.into_type(),
            column_top: 0,
            format: None,
            ascii: None,
            id: None,
        };

        let rows_filter = vec![1i64];
        decode_page_filtered::<true>(
            &page,
            None,
            &mut bufs,
            col_info,
            0,
            values.len(),
            0,
            0,
            values.len(),
            &rows_filter,
        )
        .unwrap();

        let mut expected = Vec::new();
        expected.extend_from_slice(&DECIMAL64_NULL);
        expected.extend_from_slice(&be_to_le_truncate(&values[1], 8));
        assert_eq!(bufs.data_vec.as_slice(), expected.as_slice());
    }

    #[test]
    fn test_decode_flba_decimal_truncates_non_sign_extended() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let src_len = 16;
        let bad_value = [
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x01,
        ];
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&bad_value);

        let page = make_required_page(
            make_decimal_flba_type(src_len, 10, 2),
            Encoding::Plain,
            buffer,
            1,
        );
        let page = page.as_page();

        let mut bufs = ColumnChunkBuffers::new(allocator);
        let col_info = QdbMetaCol {
            column_type: ColumnTypeTag::Decimal64.into_type(),
            column_top: 0,
            format: None,
            ascii: None,
            id: None,
        };

        decode_page(&page, None, &mut bufs, col_info, 0, 1).unwrap();

        let expected = be_to_le_truncate(&bad_value, 8);
        assert_eq!(bufs.data_vec.as_slice(), expected.as_slice());
    }

    #[test]
    fn test_decode_flba_decimal_dict_unfiltered() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let src_len = 16;
        let dict_values = [
            [
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x7B,
            ], // +123
            [0xFF; 16], // -1
            [
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x02,
            ], // +2
        ];
        let dict_page = make_dict_page_fixed(&dict_values);
        let dict_page = dict_page.as_page();
        let indices = [0u32, 1, 2, 1, 0];
        let primitive_type = make_decimal_flba_type(src_len, 10, 2);

        let mut expected = Vec::new();
        for idx in indices {
            expected.extend_from_slice(&be_to_le_truncate(&dict_values[idx as usize], 8));
        }

        for encoding in [Encoding::RleDictionary, Encoding::PlainDictionary] {
            let page = make_dict_data_page(primitive_type.clone(), encoding, &indices);
            let page = page.as_page();
            let mut bufs = ColumnChunkBuffers::new(allocator.clone());
            let col_info = QdbMetaCol {
                column_type: ColumnType::new(ColumnTypeTag::Decimal64, 0),
                column_top: 0,
                format: None,
                ascii: None,
                id: None,
            };
            decode_page(
                &page,
                Some(&dict_page),
                &mut bufs,
                col_info,
                0,
                indices.len(),
            )
            .unwrap();
            assert_eq!(bufs.data_vec.as_slice(), expected.as_slice());
        }
    }

    #[test]
    fn test_decode_flba_decimal_dict_filtered_fill_nulls() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let src_len = 16;
        let dict_values = [
            [
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x7B,
            ], // +123
            [0xFF; 16], // -1
            [
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x02,
            ], // +2
        ];
        let dict_page = make_dict_page_fixed(&dict_values);
        let dict_page = dict_page.as_page();
        let indices = [0u32, 1, 2, 1, 0];
        let primitive_type = make_decimal_flba_type(src_len, 10, 2);
        let rows_filter = vec![1i64, 3];

        let mut expected = Vec::new();
        for row in 0..indices.len() {
            if rows_filter.contains(&(row as i64)) {
                expected
                    .extend_from_slice(&be_to_le_truncate(&dict_values[indices[row] as usize], 8));
            } else {
                expected.extend_from_slice(&DECIMAL64_NULL);
            }
        }

        for encoding in [Encoding::RleDictionary, Encoding::PlainDictionary] {
            let page = make_dict_data_page(primitive_type.clone(), encoding, &indices);
            let page = page.as_page();
            let mut bufs = ColumnChunkBuffers::new(allocator.clone());
            let col_info = QdbMetaCol {
                column_type: ColumnType::new(ColumnTypeTag::Decimal64, 0),
                column_top: 0,
                format: None,
                ascii: None,
                id: None,
            };
            decode_page_filtered::<true>(
                &page,
                Some(&dict_page),
                &mut bufs,
                col_info,
                0,
                indices.len(),
                0,
                0,
                indices.len(),
                &rows_filter,
            )
            .unwrap();
            assert_eq!(bufs.data_vec.as_slice(), expected.as_slice());
        }
    }

    #[test]
    fn test_decode_ba_decimal_plain_unfiltered() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let values = vec![
            vec![0x7B],                                                 // +123
            vec![0xFF],                                                 // -1
            vec![0x00, 0x80],                                           // +128
            vec![0xFF, 0x7F],                                           // -129
            vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02], // +2, sign-extended to 9 bytes
            vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE], // -2, sign-extended to 9 bytes
        ];

        let page = make_required_page(
            make_decimal_ba_type(10, 2),
            Encoding::Plain,
            encode_plain_byte_array(&values),
            values.len(),
        );
        let page = page.as_page();

        let mut bufs = ColumnChunkBuffers::new(allocator);
        let col_info = QdbMetaCol {
            column_type: ColumnType::new(ColumnTypeTag::Decimal64, 0),
            column_top: 0,
            format: None,
            ascii: None,
            id: None,
        };

        decode_page(&page, None, &mut bufs, col_info, 0, values.len()).unwrap();

        let mut expected = Vec::new();
        for value in &values {
            expected.extend_from_slice(&be_to_le_truncate(value, 8));
        }
        assert_eq!(bufs.data_vec.as_slice(), expected.as_slice());
    }

    #[test]
    fn test_decode_ba_decimal_dict_filtered_fill_nulls() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let dict_values = vec![
            vec![0x7B],                                           // +123
            vec![0xFF],                                           // -1
            vec![0x00, 0x80],                                     // +128
            vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xF6], // -10
        ];
        let dict_page = make_dict_page_var(&dict_values);
        let dict_page = dict_page.as_page();
        let indices = [0u32, 1, 2, 3, 0];
        let primitive_type = make_decimal_ba_type(10, 2);
        let rows_filter = vec![1i64, 3];

        let mut expected = Vec::new();
        for row in 0..indices.len() {
            if rows_filter.contains(&(row as i64)) {
                expected
                    .extend_from_slice(&be_to_le_truncate(&dict_values[indices[row] as usize], 8));
            } else {
                expected.extend_from_slice(&DECIMAL64_NULL);
            }
        }

        for encoding in [Encoding::RleDictionary, Encoding::PlainDictionary] {
            let page = make_dict_data_page(primitive_type.clone(), encoding, &indices);
            let page = page.as_page();
            let mut bufs = ColumnChunkBuffers::new(allocator.clone());
            let col_info = QdbMetaCol {
                column_type: ColumnType::new(ColumnTypeTag::Decimal64, 0),
                column_top: 0,
                format: None,
                ascii: None,
                id: None,
            };
            decode_page_filtered::<true>(
                &page,
                Some(&dict_page),
                &mut bufs,
                col_info,
                0,
                indices.len(),
                0,
                0,
                indices.len(),
                &rows_filter,
            )
            .unwrap();
            assert_eq!(bufs.data_vec.as_slice(), expected.as_slice());
        }
    }

    #[test]
    fn test_decode_ba_decimal_truncates_non_sign_extended() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let values = vec![vec![0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]];
        let page = make_required_page(
            make_decimal_ba_type(10, 2),
            Encoding::Plain,
            encode_plain_byte_array(&values),
            values.len(),
        );
        let page = page.as_page();

        let mut bufs = ColumnChunkBuffers::new(allocator);
        let col_info = QdbMetaCol {
            column_type: ColumnType::new(ColumnTypeTag::Decimal64, 0),
            column_top: 0,
            format: None,
            ascii: None,
            id: None,
        };

        decode_page(&page, None, &mut bufs, col_info, 0, values.len()).unwrap();

        let mut expected = Vec::new();
        for value in &values {
            expected.extend_from_slice(&be_to_le_truncate(value, 8));
        }
        assert_eq!(bufs.data_vec.as_slice(), expected.as_slice());
    }

    #[test]
    fn test_decode_ba_decimal_plain_all_target_sizes_unfiltered() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let values = vec![
            vec![0x7B],       // +123
            vec![0xFF],       // -1
            vec![0x00, 0x7F], // +127 with sign-extension prefix
            vec![0xFF, 0x80], // -128 with sign-extension prefix
            vec![0x00, 0x00, 0x00],
            vec![0xFF, 0xFF, 0xFE], // -2
        ];
        let page = make_required_page(
            make_decimal_ba_type(10, 2),
            Encoding::Plain,
            encode_plain_byte_array(&values),
            values.len(),
        );
        let page = page.as_page();

        for (tag, target_size) in decimal_target_cases() {
            let mut bufs = ColumnChunkBuffers::new(allocator.clone());
            let col_info = QdbMetaCol {
                column_type: ColumnType::new(tag, 0),
                column_top: 0,
                format: None,
                ascii: None,
                id: None,
            };

            decode_page(&page, None, &mut bufs, col_info, 0, values.len()).unwrap();

            let mut expected = Vec::new();
            for value in &values {
                expected.extend_from_slice(&be_to_qdb_decimal(value, target_size));
            }
            assert_eq!(bufs.data_vec.as_slice(), expected.as_slice());
        }
    }

    #[test]
    fn test_decode_ba_decimal_dict_all_target_sizes_filtered_no_fill_nulls() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let dict_values = vec![
            vec![0x7B],                         // +123
            vec![0xFF],                         // -1
            vec![0x00, 0x7F],                   // +127
            vec![0xFF, 0x80],                   // -128
            vec![0x00, 0x00, 0x00, 0x00, 0x05], // +5
            vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFB], // -5
            vec![0x00],                         // 0
        ];
        let dict_page = make_dict_page_var(&dict_values);
        let dict_page = dict_page.as_page();
        let indices = [0u32, 1, 2, 3, 4, 5, 6, 1];
        let rows_filter = vec![1i64, 3, 5, 6];
        let primitive_type = make_decimal_ba_type(20, 4);

        for (tag, target_size) in decimal_target_cases() {
            let mut expected = Vec::new();
            for row in rows_filter.iter().copied() {
                expected.extend_from_slice(&be_to_qdb_decimal(
                    &dict_values[indices[row as usize] as usize],
                    target_size,
                ));
            }

            for encoding in [Encoding::RleDictionary, Encoding::PlainDictionary] {
                let page = make_dict_data_page(primitive_type.clone(), encoding, &indices);
                let page = page.as_page();
                let mut bufs = ColumnChunkBuffers::new(allocator.clone());
                let col_info = QdbMetaCol {
                    column_type: ColumnType::new(tag, 0),
                    column_top: 0,
                    format: None,
                    ascii: None,
                    id: None,
                };

                decode_page_filtered::<false>(
                    &page,
                    Some(&dict_page),
                    &mut bufs,
                    col_info,
                    0,
                    indices.len(),
                    0,
                    0,
                    indices.len(),
                    &rows_filter,
                )
                .unwrap();

                assert_eq!(bufs.data_vec.as_slice(), expected.as_slice());
            }
        }
    }

    #[test]
    fn test_decode_int32_decimal_dict_unfiltered() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let dict_values = [10, -20, 30];
        let dict_page = make_dict_page_i32(&dict_values);
        let dict_page = dict_page.as_page();
        let indices = [0u32, 1, 2, 1, 0];
        let primitive_type = make_int32_type();

        let cases = [
            (ColumnTypeTag::Decimal8, 1usize),
            (ColumnTypeTag::Decimal16, 2usize),
            (ColumnTypeTag::Decimal32, 4usize),
        ];

        for (tag, size) in cases {
            let expected_all = expected_from_i32::<4>(&dict_values);
            let expected = indices
                .iter()
                .flat_map(|&idx| expected_all[(idx as usize) * 4..(idx as usize + 1) * 4].to_vec())
                .collect::<Vec<u8>>();
            let expected = expected
                .chunks(4)
                .flat_map(|c| c[..size].to_vec())
                .collect::<Vec<u8>>();

            for encoding in [Encoding::RleDictionary, Encoding::PlainDictionary] {
                let page = make_dict_data_page(primitive_type.clone(), encoding, &indices);
                let page = page.as_page();
                let mut bufs = ColumnChunkBuffers::new(allocator.clone());
                let col_info = QdbMetaCol {
                    column_type: ColumnType::new(tag, 0),
                    column_top: 0,
                    format: None,
                    ascii: None,
                    id: None,
                };
                decode_page(
                    &page,
                    Some(&dict_page),
                    &mut bufs,
                    col_info,
                    0,
                    indices.len(),
                )
                .unwrap();
                assert_eq!(bufs.data_vec.as_slice(), expected.as_slice());
            }
        }
    }

    #[test]
    fn test_decode_int32_decimal_dict_filtered_fill_nulls() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let dict_values = [10, -20, 30];
        let dict_page = make_dict_page_i32(&dict_values);
        let dict_page = dict_page.as_page();
        let indices = [0u32, 1, 2, 1, 0];
        let primitive_type = make_int32_type();
        let rows_filter = vec![1i64, 3];

        let cases = [
            (ColumnTypeTag::Decimal8, 1usize, DECIMAL8_NULL.as_slice()),
            (ColumnTypeTag::Decimal16, 2usize, DECIMAL16_NULL.as_slice()),
            (ColumnTypeTag::Decimal32, 4usize, DECIMAL32_NULL.as_slice()),
        ];

        for (tag, size, null_bytes) in cases {
            let expected_all = expected_from_i32::<4>(&dict_values);
            let mut expected = Vec::new();
            for (row, &idx_raw) in indices.iter().enumerate() {
                if rows_filter.contains(&(row as i64)) {
                    let idx = idx_raw as usize;
                    expected.extend_from_slice(&expected_all[idx * 4..idx * 4 + size]);
                } else {
                    expected.extend_from_slice(&null_bytes[..size]);
                }
            }

            for encoding in [Encoding::RleDictionary, Encoding::PlainDictionary] {
                let page = make_dict_data_page(primitive_type.clone(), encoding, &indices);
                let page = page.as_page();
                let mut bufs = ColumnChunkBuffers::new(allocator.clone());
                let col_info = QdbMetaCol {
                    column_type: ColumnType::new(tag, 0),
                    column_top: 0,
                    format: None,
                    ascii: None,
                    id: None,
                };
                decode_page_filtered::<true>(
                    &page,
                    Some(&dict_page),
                    &mut bufs,
                    col_info,
                    0,
                    indices.len(),
                    0,
                    0,
                    indices.len(),
                    &rows_filter,
                )
                .unwrap();
                assert_eq!(bufs.data_vec.as_slice(), expected.as_slice());
            }
        }
    }

    fn make_date_type() -> PrimitiveType {
        PrimitiveType {
            field_info: FieldInfo {
                name: "date_col".to_string(),
                repetition: Repetition::Optional,
                id: None,
            },
            logical_type: Some(PrimitiveLogicalType::Date),
            converted_type: None,
            physical_type: PhysicalType::Int32,
        }
    }

    const MILLIS_PER_DAY: i64 = 86_400_000;
    const DATE_NULL: [u8; 8] = i64::MIN.to_le_bytes();

    #[test]
    fn test_decode_date_dict_unfiltered() {
        // Date dictionary stores INT32 days, decoder converts to i64 millis.
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let dict_days = [100i32, 200, 365];
        let dict_page = make_dict_page_i32(&dict_days);
        let dict_page = dict_page.as_page();

        let indices = [0u32, 1, 2, 1, 0, 2];
        let primitive_type = make_date_type();

        let mut expected = Vec::new();
        for &idx in &indices {
            let day = dict_days[idx as usize];
            let millis = (day as i64) * MILLIS_PER_DAY;
            expected.extend_from_slice(&millis.to_le_bytes());
        }

        for encoding in [Encoding::RleDictionary, Encoding::PlainDictionary] {
            let page = make_dict_data_page(primitive_type.clone(), encoding, &indices);
            let page = page.as_page();
            let mut bufs = ColumnChunkBuffers::new(allocator.clone());
            let col_info = QdbMetaCol {
                column_type: ColumnTypeTag::Date.into_type(),
                column_top: 0,
                format: None,
                ascii: None,
                id: None,
            };

            decode_page(
                &page,
                Some(&dict_page),
                &mut bufs,
                col_info,
                0,
                indices.len(),
            )
            .unwrap();

            assert_eq!(
                bufs.data_vec.as_slice(),
                expected.as_slice(),
                "Date dict decode mismatch for encoding {:?}",
                encoding
            );
            assert_eq!(bufs.aux_size, 0, "aux_size should be 0 for Date column");
        }
    }

    #[test]
    fn test_decode_date_dict_filtered_no_fill_nulls() {
        // Filtered decode without filling nulls for non-matching rows.
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let dict_days = [10i32, 50, 100];
        let dict_page = make_dict_page_i32(&dict_days);
        let dict_page = dict_page.as_page();
        let indices = [0u32, 1, 2, 1, 0];
        let primitive_type = make_date_type();
        let rows_filter = vec![1i64, 3]; // Select rows 1 and 3

        let mut expected = Vec::new();
        for &row in &rows_filter {
            let idx = indices[row as usize];
            let millis = (dict_days[idx as usize] as i64) * MILLIS_PER_DAY;
            expected.extend_from_slice(&millis.to_le_bytes());
        }

        for encoding in [Encoding::RleDictionary, Encoding::PlainDictionary] {
            let page = make_dict_data_page(primitive_type.clone(), encoding, &indices);
            let page = page.as_page();
            let mut bufs = ColumnChunkBuffers::new(allocator.clone());
            let col_info = QdbMetaCol {
                column_type: ColumnTypeTag::Date.into_type(),
                column_top: 0,
                format: None,
                ascii: None,
                id: None,
            };

            decode_page_filtered::<false>(
                &page,
                Some(&dict_page),
                &mut bufs,
                col_info,
                0,
                indices.len(),
                0,
                0,
                indices.len(),
                &rows_filter,
            )
            .unwrap();

            assert_eq!(
                bufs.data_vec.as_slice(),
                expected.as_slice(),
                "Date dict filtered decode mismatch for encoding {:?}",
                encoding
            );
            assert_eq!(bufs.aux_size, 0, "aux_size should be 0 for Date column");
        }
    }

    #[test]
    fn test_decode_date_dict_filtered_fill_nulls() {
        // Filtered decode with nulls filled for non-matching rows.
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let dict_days = [7i32, 30, 365];
        let dict_page = make_dict_page_i32(&dict_days);
        let dict_page = dict_page.as_page();
        let indices = [0u32, 1, 2, 1, 0];
        let primitive_type = make_date_type();
        let rows_filter = vec![0i64, 2, 4]; // Select rows 0, 2, 4

        let mut expected = Vec::new();
        for (row, &idx) in indices.iter().enumerate() {
            if rows_filter.contains(&(row as i64)) {
                let millis = (dict_days[idx as usize] as i64) * MILLIS_PER_DAY;
                expected.extend_from_slice(&millis.to_le_bytes());
            } else {
                expected.extend_from_slice(&DATE_NULL);
            }
        }

        for encoding in [Encoding::RleDictionary, Encoding::PlainDictionary] {
            let page = make_dict_data_page(primitive_type.clone(), encoding, &indices);
            let page = page.as_page();
            let mut bufs = ColumnChunkBuffers::new(allocator.clone());
            let col_info = QdbMetaCol {
                column_type: ColumnTypeTag::Date.into_type(),
                column_top: 0,
                format: None,
                ascii: None,
                id: None,
            };

            decode_page_filtered::<true>(
                &page,
                Some(&dict_page),
                &mut bufs,
                col_info,
                0,
                indices.len(),
                0,
                0,
                indices.len(),
                &rows_filter,
            )
            .unwrap();

            assert_eq!(
                bufs.data_vec.as_slice(),
                expected.as_slice(),
                "Date dict filtered fill_nulls decode mismatch for encoding {:?}",
                encoding
            );
            assert_eq!(bufs.aux_size, 0, "aux_size should be 0 for Date column");
        }
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

    use super::decode_bitmap_runs;
    use crate::parquet_read::column_sink::Pushable;

    /// Records push_slice / push_nulls calls for verification.
    struct MockPushable {
        /// Reconstructed bit pattern: true = set (push_slice), false = null.
        bits: Vec<bool>,
    }

    impl MockPushable {
        fn new() -> Self {
            Self { bits: Vec::new() }
        }
    }

    impl Pushable for MockPushable {
        fn reserve(
            &mut self,
            _count: usize,
        ) -> super::super::super::parquet::error::ParquetResult<()> {
            Ok(())
        }
        fn push(&mut self) -> super::super::super::parquet::error::ParquetResult<()> {
            self.bits.push(true);
            Ok(())
        }
        fn push_slice(
            &mut self,
            count: usize,
        ) -> super::super::super::parquet::error::ParquetResult<()> {
            self.bits.extend(std::iter::repeat_n(true, count));
            Ok(())
        }
        fn push_null(&mut self) -> super::super::super::parquet::error::ParquetResult<()> {
            self.bits.push(false);
            Ok(())
        }
        fn push_nulls(
            &mut self,
            count: usize,
        ) -> super::super::super::parquet::error::ParquetResult<()> {
            self.bits.extend(std::iter::repeat_n(false, count));
            Ok(())
        }
        fn skip(
            &mut self,
            _count: usize,
        ) -> super::super::super::parquet::error::ParquetResult<()> {
            Ok(())
        }
    }

    /// Reference implementation: read bits one at a time from a bitmap.
    fn expected_bits(values: &[u8], bit_offset: usize, count: usize) -> Vec<bool> {
        (0..count)
            .map(|i| {
                let pos = bit_offset + i;
                (values[pos >> 3] >> (pos & 7)) & 1 == 1
            })
            .collect()
    }

    fn run_bitmap_test(values: &[u8], bit_offset: usize, count: usize) {
        let mut sink = MockPushable::new();
        decode_bitmap_runs(values, bit_offset, count, &mut sink).unwrap();
        let expected = expected_bits(values, bit_offset, count);
        assert_eq!(
            sink.bits, expected,
            "mismatch at bit_offset={bit_offset}, count={count}"
        );
    }

    #[test]
    fn bitmap_runs_empty() {
        run_bitmap_test(&[0xFF], 0, 0);
    }

    #[test]
    fn bitmap_runs_all_ones_small() {
        // 5 bits, all set
        run_bitmap_test(&[0xFF], 0, 5);
    }

    #[test]
    fn bitmap_runs_all_zeros_small() {
        // 5 bits, all clear
        run_bitmap_test(&[0x00], 0, 5);
    }

    #[test]
    fn bitmap_runs_all_ones_one_byte() {
        run_bitmap_test(&[0xFF], 0, 8);
    }

    #[test]
    fn bitmap_runs_all_zeros_one_byte() {
        run_bitmap_test(&[0x00], 0, 8);
    }

    #[test]
    fn bitmap_runs_mixed_byte() {
        // 0b10101010 = alternating 0,1,0,1,0,1,0,1
        run_bitmap_test(&[0xAA], 0, 8);
    }

    #[test]
    fn bitmap_runs_unaligned_start() {
        // Start at bit 3 within 0xFF, read 5 bits → all ones
        run_bitmap_test(&[0xFF], 3, 5);
    }

    #[test]
    fn bitmap_runs_unaligned_start_mixed() {
        // 0b11001010 = bits: 0,1,0,1,0,0,1,1
        // Start at bit 2, read 4 bits → 0,1,0,0
        run_bitmap_test(&[0xCA], 2, 4);
    }

    #[test]
    fn bitmap_runs_unaligned_start_spans_bytes() {
        // Start at bit 5 of first byte, read 10 bits spanning two bytes
        run_bitmap_test(&[0xFF, 0x0F], 5, 10);
    }

    #[test]
    fn bitmap_runs_exactly_64_all_ones() {
        let values = [0xFFu8; 8];
        run_bitmap_test(&values, 0, 64);
    }

    #[test]
    fn bitmap_runs_exactly_64_all_zeros() {
        let values = [0x00u8; 8];
        run_bitmap_test(&values, 0, 64);
    }

    #[test]
    fn bitmap_runs_exactly_64_mixed() {
        // First 32 bits set, last 32 bits clear
        let mut values = [0u8; 8];
        values[0..4].fill(0xFF);
        run_bitmap_test(&values, 0, 64);
    }

    #[test]
    fn bitmap_runs_64bit_trailing_ones_zeros() {
        // Pattern: 7 ones, 3 zeros, 5 ones, 1 zero, rest ones
        // This exercises the trailing_ones/trailing_zeros inner loop
        // 0b01111111 0b11111_000 0b1_0000000 ...
        let mut values = [0u8; 8];
        // Bit 0..6: ones (7 ones)
        values[0] = 0x7F; // 0b01111111
                          // Bit 7..9: zeros (3 zeros, bit 7 already 0 from 0x7F)
                          // Pattern: bits 0..6 = 1, bits 7..9 = 0, bits 10..63 = 1.
                          // byte0 = 0x7F, byte1 = 0xFC.
        values[0] = 0x7F;
        values[1] = 0xFC;
        values[2..8].fill(0xFF);
        run_bitmap_test(&values, 0, 64);
    }

    #[test]
    fn bitmap_runs_unaligned_into_64bit_path() {
        // 3 bits unaligned, then 64 bits via word path, then 5 remaining bits
        // Total: 3 + 64 + 5 = 72 bits, starting at bit_offset=5
        let values = [0xFFu8; 10]; // 80 bits available
        run_bitmap_test(&values, 5, 72);
    }

    #[test]
    fn bitmap_runs_unaligned_into_64bit_zeros() {
        let values = [0x00u8; 10];
        run_bitmap_test(&values, 3, 72);
    }

    #[test]
    fn bitmap_runs_remaining_full_bytes_all_ones() {
        // 24 bits (3 bytes), no 64-bit word path
        let values = [0xFFu8; 3];
        run_bitmap_test(&values, 0, 24);
    }

    #[test]
    fn bitmap_runs_remaining_full_bytes_all_zeros() {
        let values = [0x00u8; 3];
        run_bitmap_test(&values, 0, 24);
    }

    #[test]
    fn bitmap_runs_remaining_full_bytes_mixed() {
        // 0xFF, 0x00, 0xAA → 8 ones, 8 zeros, alternating
        let values = [0xFF, 0x00, 0xAA];
        run_bitmap_test(&values, 0, 24);
    }

    #[test]
    fn bitmap_runs_trailing_bits() {
        // 11 bits total: 8 full + 3 remaining
        run_bitmap_test(&[0xFF, 0x05], 0, 11);
    }

    #[test]
    fn bitmap_runs_only_trailing_bits() {
        // Less than 8 bits, no full byte processing
        run_bitmap_test(&[0b00110101], 0, 6);
    }

    #[test]
    fn bitmap_runs_cross_word_run() {
        // A run of ones that spans from one 64-bit word into the next
        // 128 bits all set
        let values = [0xFFu8; 16];
        run_bitmap_test(&values, 0, 128);
    }

    #[test]
    fn bitmap_runs_cross_word_run_with_transition() {
        // 128 bits: first 60 ones, then 8 zeros, then 60 ones
        // This tests run accumulation across the word boundary
        let mut values = [0xFFu8; 16];
        // Clear bits 60-67
        // byte 7: bits 56-63 → clear bits 60-63 → byte7 = 0x0F
        values[7] = 0x0F;
        // byte 8: bits 64-71 → clear bits 64-67 → byte8 = 0xF0
        values[8] = 0xF0;
        run_bitmap_test(&values, 0, 128);
    }

    #[test]
    fn bitmap_runs_large_alternating() {
        // 256 bits of alternating 0xAA pattern
        let values = [0xAAu8; 32];
        run_bitmap_test(&values, 0, 256);
    }

    #[test]
    fn bitmap_runs_large_with_offset() {
        // 200 bits starting at offset 7 with mixed pattern
        let mut values = [0u8; 30];
        for (i, v) in values.iter_mut().enumerate() {
            *v = if i % 3 == 0 {
                0xFF
            } else if i % 3 == 1 {
                0x00
            } else {
                0xAA
            };
        }
        run_bitmap_test(&values, 7, 200);
    }

    #[test]
    fn bitmap_runs_single_bit_true() {
        run_bitmap_test(&[0x01], 0, 1);
    }

    #[test]
    fn bitmap_runs_single_bit_false() {
        run_bitmap_test(&[0x00], 0, 1);
    }

    #[test]
    fn bitmap_runs_flush_trailing_true() {
        // Ends with ones: 4 zeros then 4 ones
        run_bitmap_test(&[0xF0], 0, 8);
    }

    #[test]
    fn bitmap_runs_flush_trailing_false() {
        // Ends with zeros: 4 ones then 4 zeros
        run_bitmap_test(&[0x0F], 0, 8);
    }

    #[test]
    fn bitmap_runs_count_less_than_byte_unaligned() {
        // bit_offset=3, count=3 → only unaligned path, no full bytes or words
        run_bitmap_test(&[0b11010110], 3, 3);
    }

    #[test]
    fn bitmap_runs_64bit_word_starts_with_zeros() {
        // Mixed word starting with zeros exercises the else branch first
        let mut values = [0u8; 8];
        values[0] = 0x00; // 8 zeros
        values[1] = 0xFF; // 8 ones
        values[2..8].fill(0xAA);
        run_bitmap_test(&values, 0, 64);
    }

    #[test]
    fn bitmap_runs_64bit_word_w_becomes_zero() {
        // Pattern where w becomes 0 mid-loop (all remaining bits are zeros)
        // First 16 ones, then 48 zeros
        let mut values = [0u8; 8];
        values[0] = 0xFF;
        values[1] = 0xFF;
        // bytes 2-7 already zero
        run_bitmap_test(&values, 0, 64);
    }
}
