use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_read::column_sink::Pushable;
use crate::parquet_read::decode::{decode_page0, decode_page0_filtered};
use crate::parquet_read::decoders::{BaseVarDictDecoder, VarDictDecoder};
use crate::parquet_read::page::{DataPage, DictPage};
use crate::parquet_read::slicer::rle::RleDictionarySlicer;
use crate::parquet_read::slicer::{DataPageDynSlicer, DataPageSlicer, PlainVarSlicer};
use crate::parquet_read::ColumnChunkBuffers;
use crate::parquet_write::decimal::{
    DECIMAL128_NULL, DECIMAL16_NULL, DECIMAL256_NULL, DECIMAL32_NULL, DECIMAL64_NULL, DECIMAL8_NULL,
};
use qdb_core::col_type::ColumnTypeTag;
use std::ptr;

/// Decode a FixedLenByteArray with Decimal logical type to a QuestDB decimal column.
/// Handles all source sizes (1-32 bytes) and target decimal types (Decimal8-Decimal256).
#[allow(clippy::too_many_arguments)]
pub(crate) fn decode_fixed_decimal(
    page: &DataPage,
    bufs: &mut ColumnChunkBuffers,
    values_buffer: &[u8],
    row_lo: usize,
    row_hi: usize,
    row_count: usize,
    src_len: usize,
    target_tag: ColumnTypeTag,
) -> ParquetResult<()> {
    let target_size = match target_tag {
        ColumnTypeTag::Decimal8 => 1,
        ColumnTypeTag::Decimal16 => 2,
        ColumnTypeTag::Decimal32 => 4,
        ColumnTypeTag::Decimal64 => 8,
        ColumnTypeTag::Decimal128 => 16,
        ColumnTypeTag::Decimal256 => 32,
        _ => {
            return Err(fmt_err!(
                Unsupported,
                "unsupported target column type {:?} for FixedLenByteArray decimal",
                target_tag
            ))
        }
    };

    if src_len == 0 || src_len > 32 {
        return Err(fmt_err!(
            Unsupported,
            "FixedLenByteArray({}) decimal cannot be decoded to {:?} (target size {} bytes)",
            src_len,
            target_tag,
            target_size
        ));
    }

    match target_tag {
        ColumnTypeTag::Decimal8 => decode_fixed_decimal_1(
            page,
            bufs,
            values_buffer,
            row_lo,
            row_hi,
            row_count,
            src_len,
        ),
        ColumnTypeTag::Decimal16 => decode_fixed_decimal_2(
            page,
            bufs,
            values_buffer,
            row_lo,
            row_hi,
            row_count,
            src_len,
        ),
        ColumnTypeTag::Decimal32 => decode_fixed_decimal_4(
            page,
            bufs,
            values_buffer,
            row_lo,
            row_hi,
            row_count,
            src_len,
        ),
        ColumnTypeTag::Decimal64 => decode_fixed_decimal_8(
            page,
            bufs,
            values_buffer,
            row_lo,
            row_hi,
            row_count,
            src_len,
        ),
        ColumnTypeTag::Decimal128 => decode_fixed_decimal_16(
            page,
            bufs,
            values_buffer,
            row_lo,
            row_hi,
            row_count,
            src_len,
        ),
        ColumnTypeTag::Decimal256 => decode_fixed_decimal_32(
            page,
            bufs,
            values_buffer,
            row_lo,
            row_hi,
            row_count,
            src_len,
        ),
        _ => Err(fmt_err!(
            Unsupported,
            "unsupported target column type {:?} for FixedLenByteArray decimal",
            target_tag
        )),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn decode_fixed_decimal_dict(
    page: &DataPage<'_>,
    dict_page: &DictPage<'_>,
    bufs: &mut ColumnChunkBuffers,
    values_buffer: &[u8],
    row_lo: usize,
    row_hi: usize,
    row_count: usize,
    src_len: usize,
    target_tag: ColumnTypeTag,
) -> ParquetResult<()> {
    let dict_decoder = RuntimeFixedDictDecoder::try_new(dict_page, src_len)?;
    let error_value = vec![0u8; src_len];
    let mut slicer = RleDictionarySlicer::try_new(
        values_buffer,
        dict_decoder,
        row_hi,
        row_count,
        error_value.as_slice(),
    )?;
    decode_fixed_decimal_with_slicer(page, bufs, &mut slicer, row_lo, row_hi, src_len, target_tag)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn decode_byte_array_decimal(
    page: &DataPage,
    bufs: &mut ColumnChunkBuffers,
    values_buffer: &[u8],
    row_lo: usize,
    row_hi: usize,
    row_count: usize,
    target_tag: ColumnTypeTag,
) -> ParquetResult<()> {
    let mut slicer = PlainVarSlicer::new(values_buffer, row_count);
    decode_byte_array_decimal_with_slicer(page, bufs, &mut slicer, row_lo, row_hi, target_tag)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn decode_byte_array_decimal_dict(
    page: &DataPage,
    dict_page: &DictPage,
    bufs: &mut ColumnChunkBuffers,
    values_buffer: &[u8],
    row_lo: usize,
    row_hi: usize,
    row_count: usize,
    target_tag: ColumnTypeTag,
) -> ParquetResult<()> {
    let dict_decoder = BaseVarDictDecoder::try_new(dict_page, false)?;
    let mut slicer = RleDictionarySlicer::try_new(
        values_buffer,
        dict_decoder,
        row_hi,
        row_count,
        &DECIMAL_DICT_ERROR_VALUE,
    )?;
    decode_byte_array_decimal_with_slicer(page, bufs, &mut slicer, row_lo, row_hi, target_tag)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn decode_byte_array_decimal_filtered<const FILL_NULLS: bool>(
    page: &DataPage,
    bufs: &mut ColumnChunkBuffers,
    values_buffer: &[u8],
    page_row_start: usize,
    page_row_count: usize,
    row_group_lo: usize,
    row_lo: usize,
    row_hi: usize,
    rows_filter: &[i64],
    target_tag: ColumnTypeTag,
) -> ParquetResult<()> {
    let mut slicer = PlainVarSlicer::new(values_buffer, page_row_count);
    decode_byte_array_decimal_filtered_with_slicer::<FILL_NULLS, _>(
        page,
        bufs,
        &mut slicer,
        page_row_start,
        page_row_count,
        row_group_lo,
        row_lo,
        row_hi,
        rows_filter,
        target_tag,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn decode_byte_array_decimal_filtered_dict<const FILL_NULLS: bool>(
    page: &DataPage,
    dict_page: &DictPage,
    bufs: &mut ColumnChunkBuffers,
    values_buffer: &[u8],
    page_row_start: usize,
    page_row_count: usize,
    row_group_lo: usize,
    row_lo: usize,
    row_hi: usize,
    rows_filter: &[i64],
    target_tag: ColumnTypeTag,
) -> ParquetResult<()> {
    let dict_decoder = BaseVarDictDecoder::try_new(dict_page, false)?;
    let mut slicer = RleDictionarySlicer::try_new(
        values_buffer,
        dict_decoder,
        page_row_count,
        page_row_count,
        &DECIMAL_DICT_ERROR_VALUE,
    )?;
    decode_byte_array_decimal_filtered_with_slicer::<FILL_NULLS, _>(
        page,
        bufs,
        &mut slicer,
        page_row_start,
        page_row_count,
        row_group_lo,
        row_lo,
        row_hi,
        rows_filter,
        target_tag,
    )
}

const DECIMAL_DICT_ERROR_VALUE: [u8; 1] = [0u8];

pub struct ReverseDecimalColumnSink<'a, const N: usize, T: DataPageSlicer> {
    slicer: &'a mut T,
    buffers: &'a mut ColumnChunkBuffers,
    null_value: [u8; N],
}

impl<const N: usize, T: DataPageSlicer> Pushable for ReverseDecimalColumnSink<'_, N, T> {
    fn reserve(&mut self, count: usize) -> ParquetResult<()> {
        self.buffers.data_vec.reserve(count * N)?;
        Ok(())
    }

    #[inline]
    fn push(&mut self) -> ParquetResult<()> {
        let slice = self.slicer.next();
        let base = self.buffers.data_vec.len();
        debug_assert!(base + N <= self.buffers.data_vec.capacity());

        unsafe {
            let ptr = self.buffers.data_vec.as_mut_ptr().add(base);
            for i in 0..N {
                *ptr.add(i) = slice[N - i - 1];
            }
            self.buffers.data_vec.set_len(base + N);
        }
        Ok(())
    }

    #[inline]
    fn push_slice(&mut self, count: usize) -> ParquetResult<()> {
        match count {
            0 => Ok(()),
            1 => self.push(),
            2 => {
                self.push()?;
                self.push()
            }
            3 => {
                self.push()?;
                self.push()?;
                self.push()
            }
            4 => {
                self.push()?;
                self.push()?;
                self.push()?;
                self.push()
            }
            _ => {
                let base = self.buffers.data_vec.len();
                let total_bytes = count * N;
                debug_assert!(base + total_bytes <= self.buffers.data_vec.capacity());

                unsafe {
                    let ptr = self.buffers.data_vec.as_mut_ptr().add(base);
                    for c in 0..count {
                        let slice = self.slicer.next();
                        let dest = ptr.add(c * N);
                        for i in 0..N {
                            *dest.add(i) = slice[N - i - 1];
                        }
                    }
                    self.buffers.data_vec.set_len(base + total_bytes);
                }
                Ok(())
            }
        }
    }

    #[inline]
    fn push_null(&mut self) -> ParquetResult<()> {
        self.buffers.data_vec.extend_from_slice(&self.null_value)?;
        Ok(())
    }

    #[inline]
    fn push_nulls(&mut self, count: usize) -> ParquetResult<()> {
        match count {
            0 => Ok(()),
            1 => self.push_null(),
            2 => {
                self.push_null()?;
                self.push_null()
            }
            3 => {
                self.push_null()?;
                self.push_null()?;
                self.push_null()
            }
            4 => {
                self.push_null()?;
                self.push_null()?;
                self.push_null()?;
                self.push_null()
            }
            _ => {
                let base = self.buffers.data_vec.len();
                let total_bytes = count * N;
                debug_assert!(base + total_bytes <= self.buffers.data_vec.capacity());

                unsafe {
                    let ptr = self.buffers.data_vec.as_mut_ptr().add(base);
                    for i in 0..count {
                        ptr::copy_nonoverlapping(self.null_value.as_ptr(), ptr.add(i * N), N);
                    }
                    self.buffers.data_vec.set_len(base + total_bytes);
                }
                Ok(())
            }
        }
    }

    #[inline]
    fn skip(&mut self, count: usize) -> ParquetResult<()> {
        self.slicer.skip(count);
        Ok(())
    }

    fn result(&self) -> ParquetResult<()> {
        self.slicer.result()
    }
}

impl<'a, const N: usize, T: DataPageSlicer> ReverseDecimalColumnSink<'a, N, T> {
    pub fn new(
        slicer: &'a mut T,
        buffers: &'a mut ColumnChunkBuffers,
        null_value: [u8; N],
    ) -> Self {
        Self { slicer, buffers, null_value }
    }
}

/// A sink for Decimal128/256 types that swaps bytes within each 8-byte word.
/// Parquet stores decimals as big-endian byte arrays, but QuestDB stores
/// Decimal128/256 as multiple i64 values in little-endian order.
/// N is the total size (16 for Decimal128, 32 for Decimal256).
/// WORDS is the number of 8-byte words (2 for Decimal128, 4 for Decimal256).
pub struct WordSwapDecimalColumnSink<'a, const N: usize, const WORDS: usize, T: DataPageSlicer> {
    slicer: &'a mut T,
    buffers: &'a mut ColumnChunkBuffers,
    null_value: [u8; N],
}

impl<const N: usize, const WORDS: usize, T: DataPageSlicer> Pushable
    for WordSwapDecimalColumnSink<'_, N, WORDS, T>
{
    fn reserve(&mut self, count: usize) -> ParquetResult<()> {
        self.buffers.data_vec.reserve(count * N)?;
        Ok(())
    }

    #[inline]
    fn push(&mut self) -> ParquetResult<()> {
        let slice = self.slicer.next();
        let base = self.buffers.data_vec.len();
        debug_assert!(base + N <= self.buffers.data_vec.capacity());

        unsafe {
            let ptr = self.buffers.data_vec.as_mut_ptr().add(base);
            for w in 0..WORDS {
                let src_offset = w * 8;
                let dst_offset = w * 8;
                for i in 0..8 {
                    *ptr.add(dst_offset + i) = slice[src_offset + 7 - i];
                }
            }
            self.buffers.data_vec.set_len(base + N);
        }
        Ok(())
    }

    #[inline]
    fn push_slice(&mut self, count: usize) -> ParquetResult<()> {
        match count {
            0 => Ok(()),
            1 => self.push(),
            2 => {
                self.push()?;
                self.push()
            }
            3 => {
                self.push()?;
                self.push()?;
                self.push()
            }
            4 => {
                self.push()?;
                self.push()?;
                self.push()?;
                self.push()
            }
            _ => {
                let base = self.buffers.data_vec.len();
                let total_bytes = count * N;
                debug_assert!(base + total_bytes <= self.buffers.data_vec.capacity());

                unsafe {
                    let ptr = self.buffers.data_vec.as_mut_ptr().add(base);
                    for c in 0..count {
                        let slice = self.slicer.next();
                        let dest = ptr.add(c * N);
                        for w in 0..WORDS {
                            let src_offset = w * 8;
                            let dst_offset = w * 8;
                            for i in 0..8 {
                                *dest.add(dst_offset + i) = slice[src_offset + 7 - i];
                            }
                        }
                    }
                    self.buffers.data_vec.set_len(base + total_bytes);
                }
                Ok(())
            }
        }
    }

    #[inline]
    fn push_null(&mut self) -> ParquetResult<()> {
        self.buffers.data_vec.extend_from_slice(&self.null_value)?;
        Ok(())
    }

    #[inline]
    fn push_nulls(&mut self, count: usize) -> ParquetResult<()> {
        match count {
            0 => Ok(()),
            1 => self.push_null(),
            2 => {
                self.push_null()?;
                self.push_null()
            }
            3 => {
                self.push_null()?;
                self.push_null()?;
                self.push_null()
            }
            4 => {
                self.push_null()?;
                self.push_null()?;
                self.push_null()?;
                self.push_null()
            }
            _ => {
                let base = self.buffers.data_vec.len();
                let total_bytes = count * N;
                debug_assert!(base + total_bytes <= self.buffers.data_vec.capacity());

                unsafe {
                    let ptr = self.buffers.data_vec.as_mut_ptr().add(base);
                    for i in 0..count {
                        ptr::copy_nonoverlapping(self.null_value.as_ptr(), ptr.add(i * N), N);
                    }
                    self.buffers.data_vec.set_len(base + total_bytes);
                }
                Ok(())
            }
        }
    }

    #[inline]
    fn skip(&mut self, count: usize) -> ParquetResult<()> {
        self.slicer.skip(count);
        Ok(())
    }

    fn result(&self) -> ParquetResult<()> {
        self.slicer.result()
    }
}

impl<'a, const N: usize, const WORDS: usize, T: DataPageSlicer>
    WordSwapDecimalColumnSink<'a, N, WORDS, T>
{
    pub fn new(
        slicer: &'a mut T,
        buffers: &'a mut ColumnChunkBuffers,
        null_value: [u8; N],
    ) -> Self {
        Self { slicer, buffers, null_value }
    }
}

/// A sink for decimal types that sign-extends from a smaller source size to a larger target size.
/// Parquet stores decimals as big-endian byte arrays, and QuestDB stores them as little-endian.
///
/// N is the target size in bytes (1, 2, 4, 8, 16, or 32).
/// `src_len` is the source size in bytes from the Parquet file.
/// If `src_len` > N, the value must be sign-extended (only leading sign bytes may be truncated).
///
/// For simple decimals (N <= 8): sign-extend and reverse all bytes.
/// For multi-word decimals (N = 16 or 32): sign-extend and swap bytes within each 8-byte word.
pub struct SignExtendDecimalColumnSink<'a, const N: usize, T: DataPageSlicer> {
    slicer: &'a mut T,
    buffers: &'a mut ColumnChunkBuffers,
    null_value: [u8; N],
    src_len: usize,
}

impl<const N: usize, T: DataPageSlicer> Pushable for SignExtendDecimalColumnSink<'_, N, T> {
    fn reserve(&mut self, count: usize) -> ParquetResult<()> {
        self.buffers.data_vec.reserve(count * N)?;
        Ok(())
    }

    #[inline]
    fn push(&mut self) -> ParquetResult<()> {
        let slice = self.slicer.next();
        let base = self.buffers.data_vec.len();
        debug_assert!(base + N <= self.buffers.data_vec.capacity());

        unsafe {
            let ptr = self.buffers.data_vec.as_mut_ptr().add(base);
            Self::sign_extend_and_convert(slice, ptr, self.src_len)?;
            self.buffers.data_vec.set_len(base + N);
        }
        Ok(())
    }

    #[inline]
    fn push_slice(&mut self, count: usize) -> ParquetResult<()> {
        let base = self.buffers.data_vec.len();
        let total_bytes = count * N;
        debug_assert!(base + total_bytes <= self.buffers.data_vec.capacity());

        unsafe {
            let ptr = self.buffers.data_vec.as_mut_ptr().add(base);
            for c in 0..count {
                let slice = self.slicer.next();
                let dest = ptr.add(c * N);
                Self::sign_extend_and_convert(slice, dest, self.src_len)?;
            }
            self.buffers.data_vec.set_len(base + total_bytes);
        }
        Ok(())
    }

    #[inline]
    fn push_null(&mut self) -> ParquetResult<()> {
        self.buffers.data_vec.extend_from_slice(&self.null_value)?;
        Ok(())
    }

    #[inline]
    fn push_nulls(&mut self, count: usize) -> ParquetResult<()> {
        let base = self.buffers.data_vec.len();
        let total_bytes = count * N;
        debug_assert!(base + total_bytes <= self.buffers.data_vec.capacity());

        unsafe {
            let ptr = self.buffers.data_vec.as_mut_ptr().add(base);
            for i in 0..count {
                ptr::copy_nonoverlapping(self.null_value.as_ptr(), ptr.add(i * N), N);
            }
            self.buffers.data_vec.set_len(base + total_bytes);
        }
        Ok(())
    }

    #[inline]
    fn skip(&mut self, count: usize) -> ParquetResult<()> {
        self.slicer.skip(count);
        Ok(())
    }

    fn result(&self) -> ParquetResult<()> {
        self.slicer.result()
    }
}

impl<'a, const N: usize, T: DataPageSlicer> SignExtendDecimalColumnSink<'a, N, T> {
    pub fn new(
        slicer: &'a mut T,
        buffers: &'a mut ColumnChunkBuffers,
        null_value: [u8; N],
        src_len: usize,
    ) -> Self {
        Self { slicer, buffers, null_value, src_len }
    }

    #[inline]
    unsafe fn sign_extend_and_convert(
        src: &[u8],
        dest: *mut u8,
        src_len: usize,
    ) -> ParquetResult<()> {
        debug_assert!(src.len() == src_len);
        if src_len == 0 {
            return Err(fmt_err!(
                Unsupported,
                "invalid decimal source length 0 for target size {}",
                N
            ));
        }
        let mut src = src;
        let mut src_len = src_len;
        if src_len > N {
            let sign_byte = if src[0] & 0x80 != 0 { 0xFF } else { 0x00 };
            let trunc = src_len - N;
            if src[..trunc].iter().any(|b| *b != sign_byte) {
                return Err(fmt_err!(
                    Unsupported,
                    "FixedLenByteArray({}) decimal cannot be decoded to target size {} bytes: \
                     source is larger than target and not sign-extended",
                    src_len,
                    N
                ));
            }
            let msb = src[trunc];
            if (msb & 0x80) != (sign_byte & 0x80) {
                return Err(fmt_err!(
                    Unsupported,
                    "FixedLenByteArray({}) decimal cannot be decoded to target size {} bytes: \
                     source is larger than target and would truncate significant digits",
                    src_len,
                    N
                ));
            }
            src = &src[trunc..];
            src_len = N;
        }

        let sign_byte = if src[0] & 0x80 != 0 { 0xFF } else { 0x00 };

        if N <= 8 {
            for i in 0..src_len {
                *dest.add(i) = src[src_len - 1 - i];
            }
            for i in src_len..N {
                *dest.add(i) = sign_byte;
            }
        } else {
            let words = N / 8;
            let sign_prefix = N - src_len;

            for w in 0..words {
                let word_start_in_extended = w * 8;
                let word_dest = dest.add(w * 8);

                for i in 0..8 {
                    let extended_pos = word_start_in_extended + 7 - i;
                    let byte = if extended_pos < sign_prefix {
                        sign_byte
                    } else {
                        src[extended_pos - sign_prefix]
                    };
                    *word_dest.add(i) = byte;
                }
            }
        }
        Ok(())
    }
}

fn decode_byte_array_decimal_with_slicer<T: DataPageSlicer>(
    page: &DataPage,
    bufs: &mut ColumnChunkBuffers,
    slicer: &mut T,
    row_lo: usize,
    row_hi: usize,
    target_tag: ColumnTypeTag,
) -> ParquetResult<()> {
    match target_tag {
        ColumnTypeTag::Decimal8 => decode_page0(
            page,
            row_lo,
            row_hi,
            &mut ByteArrayDecimalColumnSink::<1, _>::new(slicer, bufs, DECIMAL8_NULL),
        ),
        ColumnTypeTag::Decimal16 => decode_page0(
            page,
            row_lo,
            row_hi,
            &mut ByteArrayDecimalColumnSink::<2, _>::new(slicer, bufs, DECIMAL16_NULL),
        ),
        ColumnTypeTag::Decimal32 => decode_page0(
            page,
            row_lo,
            row_hi,
            &mut ByteArrayDecimalColumnSink::<4, _>::new(slicer, bufs, DECIMAL32_NULL),
        ),
        ColumnTypeTag::Decimal64 => decode_page0(
            page,
            row_lo,
            row_hi,
            &mut ByteArrayDecimalColumnSink::<8, _>::new(slicer, bufs, DECIMAL64_NULL),
        ),
        ColumnTypeTag::Decimal128 => decode_page0(
            page,
            row_lo,
            row_hi,
            &mut ByteArrayDecimalColumnSink::<16, _>::new(slicer, bufs, DECIMAL128_NULL),
        ),
        ColumnTypeTag::Decimal256 => decode_page0(
            page,
            row_lo,
            row_hi,
            &mut ByteArrayDecimalColumnSink::<32, _>::new(slicer, bufs, DECIMAL256_NULL),
        ),
        _ => Err(fmt_err!(
            Unsupported,
            "unsupported target column type {:?} for ByteArray decimal",
            target_tag
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn decode_byte_array_decimal_filtered_with_slicer<const FILL_NULLS: bool, T: DataPageSlicer>(
    page: &DataPage,
    bufs: &mut ColumnChunkBuffers,
    slicer: &mut T,
    page_row_start: usize,
    page_row_count: usize,
    row_group_lo: usize,
    row_lo: usize,
    row_hi: usize,
    rows_filter: &[i64],
    target_tag: ColumnTypeTag,
) -> ParquetResult<()> {
    match target_tag {
        ColumnTypeTag::Decimal8 => decode_page0_filtered::<_, FILL_NULLS>(
            page,
            page_row_start,
            page_row_count,
            row_group_lo,
            row_lo,
            row_hi,
            rows_filter,
            &mut ByteArrayDecimalColumnSink::<1, _>::new(slicer, bufs, DECIMAL8_NULL),
        ),
        ColumnTypeTag::Decimal16 => decode_page0_filtered::<_, FILL_NULLS>(
            page,
            page_row_start,
            page_row_count,
            row_group_lo,
            row_lo,
            row_hi,
            rows_filter,
            &mut ByteArrayDecimalColumnSink::<2, _>::new(slicer, bufs, DECIMAL16_NULL),
        ),
        ColumnTypeTag::Decimal32 => decode_page0_filtered::<_, FILL_NULLS>(
            page,
            page_row_start,
            page_row_count,
            row_group_lo,
            row_lo,
            row_hi,
            rows_filter,
            &mut ByteArrayDecimalColumnSink::<4, _>::new(slicer, bufs, DECIMAL32_NULL),
        ),
        ColumnTypeTag::Decimal64 => decode_page0_filtered::<_, FILL_NULLS>(
            page,
            page_row_start,
            page_row_count,
            row_group_lo,
            row_lo,
            row_hi,
            rows_filter,
            &mut ByteArrayDecimalColumnSink::<8, _>::new(slicer, bufs, DECIMAL64_NULL),
        ),
        ColumnTypeTag::Decimal128 => decode_page0_filtered::<_, FILL_NULLS>(
            page,
            page_row_start,
            page_row_count,
            row_group_lo,
            row_lo,
            row_hi,
            rows_filter,
            &mut ByteArrayDecimalColumnSink::<16, _>::new(slicer, bufs, DECIMAL128_NULL),
        ),
        ColumnTypeTag::Decimal256 => decode_page0_filtered::<_, FILL_NULLS>(
            page,
            page_row_start,
            page_row_count,
            row_group_lo,
            row_lo,
            row_hi,
            rows_filter,
            &mut ByteArrayDecimalColumnSink::<32, _>::new(slicer, bufs, DECIMAL256_NULL),
        ),
        _ => Err(fmt_err!(
            Unsupported,
            "unsupported target column type {:?} for ByteArray decimal",
            target_tag
        )),
    }
}

struct ByteArrayDecimalColumnSink<'a, const N: usize, T: DataPageSlicer> {
    slicer: &'a mut T,
    buffers: &'a mut ColumnChunkBuffers,
    null_value: [u8; N],
}

impl<const N: usize, T: DataPageSlicer> Pushable for ByteArrayDecimalColumnSink<'_, N, T> {
    fn reserve(&mut self, count: usize) -> ParquetResult<()> {
        self.buffers.data_vec.reserve(count * N)?;
        Ok(())
    }

    #[inline]
    fn push(&mut self) -> ParquetResult<()> {
        let src = self.slicer.next();
        let base = self.buffers.data_vec.len();
        debug_assert!(base + N <= self.buffers.data_vec.capacity());

        unsafe {
            let ptr = self.buffers.data_vec.as_mut_ptr().add(base);
            Self::convert_decimal(src, ptr)?;
            self.buffers.data_vec.set_len(base + N);
        }
        Ok(())
    }

    #[inline]
    fn push_slice(&mut self, count: usize) -> ParquetResult<()> {
        let base = self.buffers.data_vec.len();
        let total_bytes = count * N;
        debug_assert!(base + total_bytes <= self.buffers.data_vec.capacity());

        unsafe {
            let ptr = self.buffers.data_vec.as_mut_ptr().add(base);
            for c in 0..count {
                let src = self.slicer.next();
                Self::convert_decimal(src, ptr.add(c * N))?;
            }
            self.buffers.data_vec.set_len(base + total_bytes);
        }
        Ok(())
    }

    #[inline]
    fn push_null(&mut self) -> ParquetResult<()> {
        self.buffers.data_vec.extend_from_slice(&self.null_value)?;
        Ok(())
    }

    #[inline]
    fn push_nulls(&mut self, count: usize) -> ParquetResult<()> {
        let base = self.buffers.data_vec.len();
        let total_bytes = count * N;
        debug_assert!(base + total_bytes <= self.buffers.data_vec.capacity());

        unsafe {
            let ptr = self.buffers.data_vec.as_mut_ptr().add(base);
            for i in 0..count {
                ptr::copy_nonoverlapping(self.null_value.as_ptr(), ptr.add(i * N), N);
            }
            self.buffers.data_vec.set_len(base + total_bytes);
        }
        Ok(())
    }

    #[inline]
    fn skip(&mut self, count: usize) -> ParquetResult<()> {
        self.slicer.skip(count);
        Ok(())
    }

    fn result(&self) -> ParquetResult<()> {
        self.slicer.result()
    }
}

impl<'a, const N: usize, T: DataPageSlicer> ByteArrayDecimalColumnSink<'a, N, T> {
    fn new(slicer: &'a mut T, buffers: &'a mut ColumnChunkBuffers, null_value: [u8; N]) -> Self {
        Self { slicer, buffers, null_value }
    }

    #[inline]
    unsafe fn convert_decimal(src: &[u8], dest: *mut u8) -> ParquetResult<()> {
        let mut src = src;
        let mut src_len = src.len();
        if src_len == 0 {
            return Err(fmt_err!(
                Unsupported,
                "invalid ByteArray decimal source length 0 for target size {}",
                N
            ));
        }

        if src_len > N {
            let sign_byte = if src[0] & 0x80 != 0 { 0xFF } else { 0x00 };
            let trunc = src_len - N;
            if src[..trunc].iter().any(|b| *b != sign_byte) {
                return Err(fmt_err!(
                    Unsupported,
                    "ByteArray({}) decimal cannot be decoded to target size {} bytes: \
                     source is larger than target and not sign-extended",
                    src_len,
                    N
                ));
            }
            let msb = src[trunc];
            if (msb & 0x80) != (sign_byte & 0x80) {
                return Err(fmt_err!(
                    Unsupported,
                    "ByteArray({}) decimal cannot be decoded to target size {} bytes: \
                     source is larger than target and would truncate significant digits",
                    src_len,
                    N
                ));
            }
            src = &src[trunc..];
            src_len = N;
        }

        let sign_byte = if src[0] & 0x80 != 0 { 0xFF } else { 0x00 };
        if N <= 8 {
            for i in 0..src_len {
                *dest.add(i) = src[src_len - 1 - i];
            }
            for i in src_len..N {
                *dest.add(i) = sign_byte;
            }
        } else {
            let words = N / 8;
            let sign_prefix = N - src_len;
            for w in 0..words {
                let word_dest = dest.add(w * 8);
                for i in 0..8 {
                    let extended_pos = w * 8 + 7 - i;
                    let byte = if extended_pos < sign_prefix {
                        sign_byte
                    } else {
                        src[extended_pos - sign_prefix]
                    };
                    *word_dest.add(i) = byte;
                }
            }
        }
        Ok(())
    }
}

struct RuntimeFixedDictDecoder<'a> {
    dict_page: &'a [u8],
    value_size: usize,
}

impl<'a> RuntimeFixedDictDecoder<'a> {
    fn try_new(dict_page: &'a DictPage, value_size: usize) -> ParquetResult<Self> {
        if value_size == 0 {
            return Err(fmt_err!(Layout, "dictionary fixed value size must be > 0"));
        }
        if value_size * dict_page.num_values != dict_page.buffer.len() {
            return Err(fmt_err!(
                Layout,
                "dictionary data page size is not multiple of {value_size}"
            ));
        }
        Ok(Self { dict_page: dict_page.buffer.as_ref(), value_size })
    }

    #[inline]
    fn get_dict_value(&self, index: u32) -> &[u8] {
        let start = index as usize * self.value_size;
        let end = start + self.value_size;
        self.dict_page[start..end].as_ref()
    }

    #[inline]
    fn avg_key_len(&self) -> f32 {
        self.value_size as f32
    }

    #[inline]
    fn len(&self) -> u32 {
        (self.dict_page.len() / self.value_size) as u32
    }
}

impl VarDictDecoder for RuntimeFixedDictDecoder<'_> {
    #[inline]
    fn get_dict_value(&self, index: u32) -> &[u8] {
        RuntimeFixedDictDecoder::get_dict_value(self, index)
    }

    #[inline]
    fn avg_key_len(&self) -> f32 {
        RuntimeFixedDictDecoder::avg_key_len(self)
    }

    #[inline]
    fn len(&self) -> u32 {
        RuntimeFixedDictDecoder::len(self)
    }
}

fn decode_fixed_decimal_with_slicer<T: DataPageSlicer>(
    page: &DataPage,
    bufs: &mut ColumnChunkBuffers,
    slicer: &mut T,
    row_lo: usize,
    row_hi: usize,
    src_len: usize,
    target_tag: ColumnTypeTag,
) -> ParquetResult<()> {
    let target_size = match target_tag {
        ColumnTypeTag::Decimal8 => 1,
        ColumnTypeTag::Decimal16 => 2,
        ColumnTypeTag::Decimal32 => 4,
        ColumnTypeTag::Decimal64 => 8,
        ColumnTypeTag::Decimal128 => 16,
        ColumnTypeTag::Decimal256 => 32,
        _ => {
            return Err(fmt_err!(
                Unsupported,
                "unsupported target column type {:?} for FixedLenByteArray decimal",
                target_tag
            ))
        }
    };
    if src_len == 0 || src_len > 32 {
        return Err(fmt_err!(
            Unsupported,
            "FixedLenByteArray({}) decimal cannot be decoded to {:?} (target size {} bytes)",
            src_len,
            target_tag,
            target_size
        ));
    }

    match target_tag {
        ColumnTypeTag::Decimal8 => {
            if src_len == 1 {
                decode_page0(
                    page,
                    row_lo,
                    row_hi,
                    &mut ReverseDecimalColumnSink::<1, _>::new(slicer, bufs, DECIMAL8_NULL),
                )
            } else {
                decode_page0(
                    page,
                    row_lo,
                    row_hi,
                    &mut SignExtendDecimalColumnSink::<1, _>::new(
                        slicer,
                        bufs,
                        DECIMAL8_NULL,
                        src_len,
                    ),
                )
            }
        }
        ColumnTypeTag::Decimal16 => {
            if src_len == 2 {
                decode_page0(
                    page,
                    row_lo,
                    row_hi,
                    &mut ReverseDecimalColumnSink::<2, _>::new(slicer, bufs, DECIMAL16_NULL),
                )
            } else {
                decode_page0(
                    page,
                    row_lo,
                    row_hi,
                    &mut SignExtendDecimalColumnSink::<2, _>::new(
                        slicer,
                        bufs,
                        DECIMAL16_NULL,
                        src_len,
                    ),
                )
            }
        }
        ColumnTypeTag::Decimal32 => {
            if src_len == 4 {
                decode_page0(
                    page,
                    row_lo,
                    row_hi,
                    &mut ReverseDecimalColumnSink::<4, _>::new(slicer, bufs, DECIMAL32_NULL),
                )
            } else {
                decode_page0(
                    page,
                    row_lo,
                    row_hi,
                    &mut SignExtendDecimalColumnSink::<4, _>::new(
                        slicer,
                        bufs,
                        DECIMAL32_NULL,
                        src_len,
                    ),
                )
            }
        }
        ColumnTypeTag::Decimal64 => {
            if src_len == 8 {
                decode_page0(
                    page,
                    row_lo,
                    row_hi,
                    &mut ReverseDecimalColumnSink::<8, _>::new(slicer, bufs, DECIMAL64_NULL),
                )
            } else {
                decode_page0(
                    page,
                    row_lo,
                    row_hi,
                    &mut SignExtendDecimalColumnSink::<8, _>::new(
                        slicer,
                        bufs,
                        DECIMAL64_NULL,
                        src_len,
                    ),
                )
            }
        }
        ColumnTypeTag::Decimal128 => {
            if src_len == 16 {
                decode_page0(
                    page,
                    row_lo,
                    row_hi,
                    &mut WordSwapDecimalColumnSink::<16, 2, _>::new(slicer, bufs, DECIMAL128_NULL),
                )
            } else {
                decode_page0(
                    page,
                    row_lo,
                    row_hi,
                    &mut SignExtendDecimalColumnSink::<16, _>::new(
                        slicer,
                        bufs,
                        DECIMAL128_NULL,
                        src_len,
                    ),
                )
            }
        }
        ColumnTypeTag::Decimal256 => {
            if src_len == 32 {
                decode_page0(
                    page,
                    row_lo,
                    row_hi,
                    &mut WordSwapDecimalColumnSink::<32, 4, _>::new(slicer, bufs, DECIMAL256_NULL),
                )
            } else {
                decode_page0(
                    page,
                    row_lo,
                    row_hi,
                    &mut SignExtendDecimalColumnSink::<32, _>::new(
                        slicer,
                        bufs,
                        DECIMAL256_NULL,
                        src_len,
                    ),
                )
            }
        }
        _ => Err(fmt_err!(
            Unsupported,
            "unsupported target column type {:?} for FixedLenByteArray decimal",
            target_tag
        )),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn decode_fixed_decimal_filtered<const FILL_NULLS: bool>(
    page: &DataPage<'_>,
    bufs: &mut ColumnChunkBuffers,
    values_buffer: &[u8],
    page_row_start: usize,
    page_row_count: usize,
    row_group_lo: usize,
    row_lo: usize,
    row_hi: usize,
    rows_filter: &[i64],
    src_len: usize,
    target_tag: ColumnTypeTag,
) -> ParquetResult<()> {
    let target_size = match target_tag {
        ColumnTypeTag::Decimal8 => 1,
        ColumnTypeTag::Decimal16 => 2,
        ColumnTypeTag::Decimal32 => 4,
        ColumnTypeTag::Decimal64 => 8,
        ColumnTypeTag::Decimal128 => 16,
        ColumnTypeTag::Decimal256 => 32,
        _ => {
            return Err(fmt_err!(
                Unsupported,
                "unsupported target column type {:?} for FixedLenByteArray decimal",
                target_tag
            ))
        }
    };
    if src_len == 0 || src_len > 32 {
        return Err(fmt_err!(
            Unsupported,
            "FixedLenByteArray({}) decimal cannot be decoded to {:?} (target size {} bytes)",
            src_len,
            target_tag,
            target_size
        ));
    }

    match target_tag {
        ColumnTypeTag::Decimal8 => decode_fixed_decimal_filtered_1::<FILL_NULLS>(
            page,
            bufs,
            values_buffer,
            page_row_start,
            page_row_count,
            row_group_lo,
            row_lo,
            row_hi,
            rows_filter,
            src_len,
        ),
        ColumnTypeTag::Decimal16 => decode_fixed_decimal_filtered_2::<FILL_NULLS>(
            page,
            bufs,
            values_buffer,
            page_row_start,
            page_row_count,
            row_group_lo,
            row_lo,
            row_hi,
            rows_filter,
            src_len,
        ),
        ColumnTypeTag::Decimal32 => decode_fixed_decimal_filtered_4::<FILL_NULLS>(
            page,
            bufs,
            values_buffer,
            page_row_start,
            page_row_count,
            row_group_lo,
            row_lo,
            row_hi,
            rows_filter,
            src_len,
        ),
        ColumnTypeTag::Decimal64 => decode_fixed_decimal_filtered_8::<FILL_NULLS>(
            page,
            bufs,
            values_buffer,
            page_row_start,
            page_row_count,
            row_group_lo,
            row_lo,
            row_hi,
            rows_filter,
            src_len,
        ),
        ColumnTypeTag::Decimal128 => decode_fixed_decimal_filtered_16::<FILL_NULLS>(
            page,
            bufs,
            values_buffer,
            page_row_start,
            page_row_count,
            row_group_lo,
            row_lo,
            row_hi,
            rows_filter,
            src_len,
        ),
        ColumnTypeTag::Decimal256 => decode_fixed_decimal_filtered_32::<FILL_NULLS>(
            page,
            bufs,
            values_buffer,
            page_row_start,
            page_row_count,
            row_group_lo,
            row_lo,
            row_hi,
            rows_filter,
            src_len,
        ),
        _ => Err(fmt_err!(
            Unsupported,
            "unsupported target column type {:?} for FixedLenByteArray decimal",
            target_tag
        )),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn decode_fixed_decimal_filtered_dict<const FILL_NULLS: bool>(
    page: &DataPage,
    dict_page: &DictPage,
    bufs: &mut ColumnChunkBuffers,
    values_buffer: &[u8],
    page_row_start: usize,
    page_row_count: usize,
    row_group_lo: usize,
    row_lo: usize,
    row_hi: usize,
    rows_filter: &[i64],
    src_len: usize,
    target_tag: ColumnTypeTag,
) -> ParquetResult<()> {
    let dict_decoder = RuntimeFixedDictDecoder::try_new(dict_page, src_len)?;
    let error_value = vec![0u8; src_len];
    let mut slicer = RleDictionarySlicer::try_new(
        values_buffer,
        dict_decoder,
        page_row_count,
        page_row_count,
        error_value.as_slice(),
    )?;
    decode_fixed_decimal_filtered_with_slicer::<FILL_NULLS, _>(
        page,
        bufs,
        &mut slicer,
        page_row_start,
        page_row_count,
        row_group_lo,
        row_lo,
        row_hi,
        rows_filter,
        src_len,
        target_tag,
    )
}

#[allow(clippy::too_many_arguments)]
fn decode_fixed_decimal_filtered_with_slicer<const FILL_NULLS: bool, T: DataPageSlicer>(
    page: &DataPage,
    bufs: &mut ColumnChunkBuffers,
    slicer: &mut T,
    page_row_start: usize,
    page_row_count: usize,
    row_group_lo: usize,
    row_lo: usize,
    row_hi: usize,
    rows_filter: &[i64],
    src_len: usize,
    target_tag: ColumnTypeTag,
) -> ParquetResult<()> {
    let target_size = match target_tag {
        ColumnTypeTag::Decimal8 => 1,
        ColumnTypeTag::Decimal16 => 2,
        ColumnTypeTag::Decimal32 => 4,
        ColumnTypeTag::Decimal64 => 8,
        ColumnTypeTag::Decimal128 => 16,
        ColumnTypeTag::Decimal256 => 32,
        _ => {
            return Err(fmt_err!(
                Unsupported,
                "unsupported target column type {:?} for FixedLenByteArray decimal",
                target_tag
            ))
        }
    };
    if src_len == 0 || src_len > 32 {
        return Err(fmt_err!(
            Unsupported,
            "FixedLenByteArray({}) decimal cannot be decoded to {:?} (target size {} bytes)",
            src_len,
            target_tag,
            target_size
        ));
    }

    match target_tag {
        ColumnTypeTag::Decimal8 => {
            if src_len == 1 {
                decode_page0_filtered::<_, FILL_NULLS>(
                    page,
                    page_row_start,
                    page_row_count,
                    row_group_lo,
                    row_lo,
                    row_hi,
                    rows_filter,
                    &mut ReverseDecimalColumnSink::<1, _>::new(slicer, bufs, DECIMAL8_NULL),
                )
            } else {
                decode_page0_filtered::<_, FILL_NULLS>(
                    page,
                    page_row_start,
                    page_row_count,
                    row_group_lo,
                    row_lo,
                    row_hi,
                    rows_filter,
                    &mut SignExtendDecimalColumnSink::<1, _>::new(
                        slicer,
                        bufs,
                        DECIMAL8_NULL,
                        src_len,
                    ),
                )
            }
        }
        ColumnTypeTag::Decimal16 => {
            if src_len == 2 {
                decode_page0_filtered::<_, FILL_NULLS>(
                    page,
                    page_row_start,
                    page_row_count,
                    row_group_lo,
                    row_lo,
                    row_hi,
                    rows_filter,
                    &mut ReverseDecimalColumnSink::<2, _>::new(slicer, bufs, DECIMAL16_NULL),
                )
            } else {
                decode_page0_filtered::<_, FILL_NULLS>(
                    page,
                    page_row_start,
                    page_row_count,
                    row_group_lo,
                    row_lo,
                    row_hi,
                    rows_filter,
                    &mut SignExtendDecimalColumnSink::<2, _>::new(
                        slicer,
                        bufs,
                        DECIMAL16_NULL,
                        src_len,
                    ),
                )
            }
        }
        ColumnTypeTag::Decimal32 => {
            if src_len == 4 {
                decode_page0_filtered::<_, FILL_NULLS>(
                    page,
                    page_row_start,
                    page_row_count,
                    row_group_lo,
                    row_lo,
                    row_hi,
                    rows_filter,
                    &mut ReverseDecimalColumnSink::<4, _>::new(slicer, bufs, DECIMAL32_NULL),
                )
            } else {
                decode_page0_filtered::<_, FILL_NULLS>(
                    page,
                    page_row_start,
                    page_row_count,
                    row_group_lo,
                    row_lo,
                    row_hi,
                    rows_filter,
                    &mut SignExtendDecimalColumnSink::<4, _>::new(
                        slicer,
                        bufs,
                        DECIMAL32_NULL,
                        src_len,
                    ),
                )
            }
        }
        ColumnTypeTag::Decimal64 => {
            if src_len == 8 {
                decode_page0_filtered::<_, FILL_NULLS>(
                    page,
                    page_row_start,
                    page_row_count,
                    row_group_lo,
                    row_lo,
                    row_hi,
                    rows_filter,
                    &mut ReverseDecimalColumnSink::<8, _>::new(slicer, bufs, DECIMAL64_NULL),
                )
            } else {
                decode_page0_filtered::<_, FILL_NULLS>(
                    page,
                    page_row_start,
                    page_row_count,
                    row_group_lo,
                    row_lo,
                    row_hi,
                    rows_filter,
                    &mut SignExtendDecimalColumnSink::<8, _>::new(
                        slicer,
                        bufs,
                        DECIMAL64_NULL,
                        src_len,
                    ),
                )
            }
        }
        ColumnTypeTag::Decimal128 => {
            if src_len == 16 {
                decode_page0_filtered::<_, FILL_NULLS>(
                    page,
                    page_row_start,
                    page_row_count,
                    row_group_lo,
                    row_lo,
                    row_hi,
                    rows_filter,
                    &mut WordSwapDecimalColumnSink::<16, 2, _>::new(slicer, bufs, DECIMAL128_NULL),
                )
            } else {
                decode_page0_filtered::<_, FILL_NULLS>(
                    page,
                    page_row_start,
                    page_row_count,
                    row_group_lo,
                    row_lo,
                    row_hi,
                    rows_filter,
                    &mut SignExtendDecimalColumnSink::<16, _>::new(
                        slicer,
                        bufs,
                        DECIMAL128_NULL,
                        src_len,
                    ),
                )
            }
        }
        ColumnTypeTag::Decimal256 => {
            if src_len == 32 {
                decode_page0_filtered::<_, FILL_NULLS>(
                    page,
                    page_row_start,
                    page_row_count,
                    row_group_lo,
                    row_lo,
                    row_hi,
                    rows_filter,
                    &mut WordSwapDecimalColumnSink::<32, 4, _>::new(slicer, bufs, DECIMAL256_NULL),
                )
            } else {
                decode_page0_filtered::<_, FILL_NULLS>(
                    page,
                    page_row_start,
                    page_row_count,
                    row_group_lo,
                    row_lo,
                    row_hi,
                    rows_filter,
                    &mut SignExtendDecimalColumnSink::<32, _>::new(
                        slicer,
                        bufs,
                        DECIMAL256_NULL,
                        src_len,
                    ),
                )
            }
        }
        _ => Err(fmt_err!(
            Unsupported,
            "unsupported target column type {:?} for FixedLenByteArray decimal",
            target_tag
        )),
    }
}

macro_rules! decode_fixed_decimal_impl {
    (unfiltered simple $fn_name:ident, $target_size:expr, $null_value:expr, $target_name:expr) => {
        fn $fn_name(
            page: &DataPage,
            bufs: &mut ColumnChunkBuffers,
            values_buffer: &[u8],
            row_lo: usize,
            row_hi: usize,
            row_count: usize,
            src_len: usize,
        ) -> ParquetResult<()> {
            if src_len == 0 {
                return Err(fmt_err!(
                    Unsupported,
                    "unsupported FixedLenByteArray({}) source size for {}, valid sizes are 1-{}",
                    src_len,
                    $target_name,
                    $target_size
                ));
            }
            if src_len == $target_size {
                let mut slicer = DataPageDynSlicer::new(values_buffer, row_count, src_len);
                decode_page0(
                    page,
                    row_lo,
                    row_hi,
                    &mut ReverseDecimalColumnSink::<$target_size, _>::new(
                        &mut slicer,
                        bufs,
                        $null_value,
                    ),
                )?;
            } else {
                let mut slicer = DataPageDynSlicer::new(values_buffer, row_count, src_len);
                decode_page0(
                    page,
                    row_lo,
                    row_hi,
                    &mut SignExtendDecimalColumnSink::<$target_size, _>::new(
                        &mut slicer,
                        bufs,
                        $null_value,
                        src_len,
                    ),
                )?;
            }
            Ok(())
        }
    };
    (unfiltered multiword $fn_name:ident, $target_size:expr, $words:expr, $null_value:expr, $target_name:expr) => {
        fn $fn_name(
            page: &DataPage,
            bufs: &mut ColumnChunkBuffers,
            values_buffer: &[u8],
            row_lo: usize,
            row_hi: usize,
            row_count: usize,
            src_len: usize,
        ) -> ParquetResult<()> {
            if src_len == 0 {
                return Err(fmt_err!(
                    Unsupported,
                    "unsupported FixedLenByteArray({}) source size for {}, valid sizes are 1-{}",
                    src_len,
                    $target_name,
                    $target_size
                ));
            }
            if src_len == $target_size {
                let mut slicer = DataPageDynSlicer::new(values_buffer, row_count, src_len);
                decode_page0(
                    page,
                    row_lo,
                    row_hi,
                    &mut WordSwapDecimalColumnSink::<$target_size, $words, _>::new(
                        &mut slicer,
                        bufs,
                        $null_value,
                    ),
                )?;
            } else {
                let mut slicer = DataPageDynSlicer::new(values_buffer, row_count, src_len);
                decode_page0(
                    page,
                    row_lo,
                    row_hi,
                    &mut SignExtendDecimalColumnSink::<$target_size, _>::new(
                        &mut slicer,
                        bufs,
                        $null_value,
                        src_len,
                    ),
                )?;
            }
            Ok(())
        }
    };
    (filtered simple $fn_name:ident, $target_size:expr, $null_value:expr, $target_name:expr) => {
        #[allow(clippy::too_many_arguments)]
        fn $fn_name<const FILL_NULLS: bool>(
            page: &DataPage,
            bufs: &mut ColumnChunkBuffers,
            values_buffer: &[u8],
            page_row_start: usize,
            page_row_count: usize,
            row_group_lo: usize,
            row_lo: usize,
            row_hi: usize,
            rows_filter: &[i64],
            src_len: usize,
        ) -> ParquetResult<()> {
            if src_len == 0 {
                return Err(fmt_err!(
                    Unsupported,
                    "unsupported FixedLenByteArray({}) source size for {}, valid sizes are 1-{}",
                    src_len,
                    $target_name,
                    $target_size
                ));
            }
            if src_len == $target_size {
                let mut slicer = DataPageDynSlicer::new(values_buffer, page_row_count, src_len);
                decode_page0_filtered::<_, FILL_NULLS>(
                    page,
                    page_row_start,
                    page_row_count,
                    row_group_lo,
                    row_lo,
                    row_hi,
                    rows_filter,
                    &mut ReverseDecimalColumnSink::<$target_size, _>::new(
                        &mut slicer,
                        bufs,
                        $null_value,
                    ),
                )?;
            } else {
                let mut slicer = DataPageDynSlicer::new(values_buffer, page_row_count, src_len);
                decode_page0_filtered::<_, FILL_NULLS>(
                    page,
                    page_row_start,
                    page_row_count,
                    row_group_lo,
                    row_lo,
                    row_hi,
                    rows_filter,
                    &mut SignExtendDecimalColumnSink::<$target_size, _>::new(
                        &mut slicer,
                        bufs,
                        $null_value,
                        src_len,
                    ),
                )?;
            }
            Ok(())
        }
    };
    (filtered multiword $fn_name:ident, $target_size:expr, $words:expr, $null_value:expr, $target_name:expr) => {
        #[allow(clippy::too_many_arguments)]
        fn $fn_name<const FILL_NULLS: bool>(
            page: &DataPage,
            bufs: &mut ColumnChunkBuffers,
            values_buffer: &[u8],
            page_row_start: usize,
            page_row_count: usize,
            row_group_lo: usize,
            row_lo: usize,
            row_hi: usize,
            rows_filter: &[i64],
            src_len: usize,
        ) -> ParquetResult<()> {
            if src_len == 0 {
                return Err(fmt_err!(
                    Unsupported,
                    "unsupported FixedLenByteArray({}) source size for {}, valid sizes are 1-{}",
                    src_len,
                    $target_name,
                    $target_size
                ));
            }
            if src_len == $target_size {
                let mut slicer = DataPageDynSlicer::new(values_buffer, page_row_count, src_len);
                decode_page0_filtered::<_, FILL_NULLS>(
                    page,
                    page_row_start,
                    page_row_count,
                    row_group_lo,
                    row_lo,
                    row_hi,
                    rows_filter,
                    &mut WordSwapDecimalColumnSink::<$target_size, $words, _>::new(
                        &mut slicer,
                        bufs,
                        $null_value,
                    ),
                )?;
            } else {
                let mut slicer = DataPageDynSlicer::new(values_buffer, page_row_count, src_len);
                decode_page0_filtered::<_, FILL_NULLS>(
                    page,
                    page_row_start,
                    page_row_count,
                    row_group_lo,
                    row_lo,
                    row_hi,
                    rows_filter,
                    &mut SignExtendDecimalColumnSink::<$target_size, _>::new(
                        &mut slicer,
                        bufs,
                        $null_value,
                        src_len,
                    ),
                )?;
            }
            Ok(())
        }
    };
}

decode_fixed_decimal_impl!(unfiltered simple decode_fixed_decimal_1, 1, DECIMAL8_NULL, "Decimal8");
decode_fixed_decimal_impl!(unfiltered simple decode_fixed_decimal_2, 2, DECIMAL16_NULL, "Decimal16");
decode_fixed_decimal_impl!(unfiltered simple decode_fixed_decimal_4, 4, DECIMAL32_NULL, "Decimal32");
decode_fixed_decimal_impl!(unfiltered simple decode_fixed_decimal_8, 8, DECIMAL64_NULL, "Decimal64");
decode_fixed_decimal_impl!(
    unfiltered multiword decode_fixed_decimal_16,
    16,
    2,
    DECIMAL128_NULL,
    "Decimal128"
);
decode_fixed_decimal_impl!(
    unfiltered multiword decode_fixed_decimal_32,
    32,
    4,
    DECIMAL256_NULL,
    "Decimal256"
);
decode_fixed_decimal_impl!(filtered simple decode_fixed_decimal_filtered_1, 1, DECIMAL8_NULL, "Decimal8");
decode_fixed_decimal_impl!(filtered simple decode_fixed_decimal_filtered_2, 2, DECIMAL16_NULL, "Decimal16");
decode_fixed_decimal_impl!(filtered simple decode_fixed_decimal_filtered_4, 4, DECIMAL32_NULL, "Decimal32");
decode_fixed_decimal_impl!(filtered simple decode_fixed_decimal_filtered_8, 8, DECIMAL64_NULL, "Decimal64");
decode_fixed_decimal_impl!(
    filtered multiword decode_fixed_decimal_filtered_16,
    16,
    2,
    DECIMAL128_NULL,
    "Decimal128"
);
decode_fixed_decimal_impl!(
    filtered multiword decode_fixed_decimal_filtered_32,
    32,
    4,
    DECIMAL256_NULL,
    "Decimal256"
);
