use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_read::column_sink::Pushable;
use crate::parquet_read::decoders::{
    BaseVarDictDecoder, PrimitiveDictDecoder, RleDictionaryDecoder,
};
use crate::parquet_read::page::{DataPage, DictPage};
use crate::parquet_read::slicer::rle::RleDictionarySlicer;
use crate::parquet_read::slicer::{DataPageDynSlicer, DataPageSlicer, PlainVarSlicer, SliceSink};
use crate::parquet_read::ColumnChunkBuffers;
use crate::parquet_write::decimal::{
    Decimal128, Decimal16, Decimal256, Decimal32, Decimal64, Decimal8, DECIMAL128_NULL,
    DECIMAL128_NULL_VAL, DECIMAL16_NULL, DECIMAL16_NULL_VAL, DECIMAL256_NULL, DECIMAL256_NULL_VAL,
    DECIMAL32_NULL, DECIMAL32_NULL_VAL, DECIMAL64_NULL, DECIMAL64_NULL_VAL, DECIMAL8_NULL,
    DECIMAL8_NULL_VAL,
};
use qdb_core::col_type::ColumnTypeTag;
use std::ptr;

#[inline(always)]
pub(super) fn decode_fixed_decimal_mode<const FILTERED: bool, const FILL_NULLS: bool>(
    page: &DataPage,
    bufs: &mut ColumnChunkBuffers,
    values_buffer: &[u8],
    mode: super::DecodeModeContext<'_>,
    src_len: usize,
    target_tag: ColumnTypeTag,
) -> ParquetResult<()> {
    let mut slicer = DataPageDynSlicer::new(values_buffer, mode.sliced_row_count(), src_len);
    decode_fixed_decimal_with_slicer_mode::<FILTERED, FILL_NULLS, _>(
        page,
        bufs,
        &mut slicer,
        mode,
        src_len,
        target_tag,
    )
}

#[inline(always)]
pub(super) fn decode_fixed_decimal_dict_mode<const FILTERED: bool, const FILL_NULLS: bool>(
    page: &DataPage<'_>,
    dict_page: &DictPage<'_>,
    bufs: &mut ColumnChunkBuffers,
    values_buffer: &[u8],
    mode: super::DecodeModeContext<'_>,
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

    let row_hi = mode.source_row_count();
    match target_tag {
        ColumnTypeTag::Decimal8 => {
            let dict = PreconvertedFlbaDecimalDict::try_new_decimal8(dict_page, src_len)?;
            super::decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict,
                    row_hi,
                    DECIMAL8_NULL_VAL,
                    bufs,
                )?,
            )
        }
        ColumnTypeTag::Decimal16 => {
            let dict = PreconvertedFlbaDecimalDict::try_new_decimal16(dict_page, src_len)?;
            super::decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict,
                    row_hi,
                    DECIMAL16_NULL_VAL,
                    bufs,
                )?,
            )
        }
        ColumnTypeTag::Decimal32 => {
            let dict = PreconvertedFlbaDecimalDict::try_new_decimal32(dict_page, src_len)?;
            super::decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict,
                    row_hi,
                    DECIMAL32_NULL_VAL,
                    bufs,
                )?,
            )
        }
        ColumnTypeTag::Decimal64 => {
            let dict = PreconvertedFlbaDecimalDict::try_new_decimal64(dict_page, src_len)?;
            super::decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict,
                    row_hi,
                    DECIMAL64_NULL_VAL,
                    bufs,
                )?,
            )
        }
        ColumnTypeTag::Decimal128 => {
            let dict = PreconvertedFlbaDecimalDict::try_new_decimal128(dict_page, src_len)?;
            super::decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict,
                    row_hi,
                    DECIMAL128_NULL_VAL,
                    bufs,
                )?,
            )
        }
        ColumnTypeTag::Decimal256 => {
            let dict = PreconvertedFlbaDecimalDict::try_new_decimal256(dict_page, src_len)?;
            super::decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                page,
                mode,
                &mut RleDictionaryDecoder::try_new(
                    values_buffer,
                    dict,
                    row_hi,
                    DECIMAL256_NULL_VAL,
                    bufs,
                )?,
            )
        }
        _ => Err(fmt_err!(
            Unsupported,
            "unsupported target column type {:?} for FixedLenByteArray decimal",
            target_tag
        )),
    }
}

#[inline(always)]
pub(super) fn decode_byte_array_decimal_mode<const FILTERED: bool, const FILL_NULLS: bool>(
    page: &DataPage,
    bufs: &mut ColumnChunkBuffers,
    values_buffer: &[u8],
    mode: super::DecodeModeContext<'_>,
    target_tag: ColumnTypeTag,
) -> ParquetResult<()> {
    let mut slicer = PlainVarSlicer::new(values_buffer, mode.sliced_row_count());
    decode_byte_array_decimal_with_slicer_mode::<FILTERED, FILL_NULLS, _>(
        page,
        bufs,
        &mut slicer,
        mode,
        target_tag,
    )
}

#[inline(always)]
pub(super) fn decode_byte_array_decimal_dict_mode<const FILTERED: bool, const FILL_NULLS: bool>(
    page: &DataPage,
    dict_page: &DictPage,
    bufs: &mut ColumnChunkBuffers,
    values_buffer: &[u8],
    mode: super::DecodeModeContext<'_>,
    target_tag: ColumnTypeTag,
) -> ParquetResult<()> {
    let dict_decoder = BaseVarDictDecoder::try_new(dict_page, false)?;
    let mut slicer = RleDictionarySlicer::try_new(
        values_buffer,
        dict_decoder,
        mode.source_row_count(),
        mode.sliced_row_count(),
        &DECIMAL_DICT_ERROR_VALUE,
    )?;
    decode_byte_array_decimal_with_slicer_mode::<FILTERED, FILL_NULLS, _>(
        page,
        bufs,
        &mut slicer,
        mode,
        target_tag,
    )
}

const DECIMAL_DICT_ERROR_VALUE: [u8; 1] = [0u8];

#[inline]
unsafe fn reverse_exact<const N: usize>(src: &[u8], dest: *mut u8) {
    debug_assert!(src.len() == N);

    if N == 1 {
        *dest = src[0];
    } else if N == 2 {
        let v = (src.as_ptr() as *const u16).read_unaligned().swap_bytes();
        (dest as *mut u16).write_unaligned(v);
    } else if N == 4 {
        let v = (src.as_ptr() as *const u32).read_unaligned().swap_bytes();
        (dest as *mut u32).write_unaligned(v);
    } else if N == 8 {
        let v = (src.as_ptr() as *const u64).read_unaligned().swap_bytes();
        (dest as *mut u64).write_unaligned(v);
    } else {
        for i in 0..N {
            *dest.add(i) = src[N - i - 1];
        }
    }
}

#[inline]
unsafe fn word_swap_exact<const WORDS: usize>(src: &[u8], dest: *mut u8) {
    debug_assert!(src.len() == WORDS * 8);
    for w in 0..WORDS {
        let src_word = (src.as_ptr().add(w * 8) as *const u64)
            .read_unaligned()
            .swap_bytes();
        (dest.add(w * 8) as *mut u64).write_unaligned(src_word);
    }
}

#[inline]
unsafe fn load_be_u64_by_len(src: *const u8, src_len: usize) -> u64 {
    match src_len {
        1 => *src as u64,
        2 => u16::from_be((src as *const u16).read_unaligned()) as u64,
        3 => {
            ((u16::from_be((src as *const u16).read_unaligned()) as u64) << 8) | *src.add(2) as u64
        }
        4 => u32::from_be((src as *const u32).read_unaligned()) as u64,
        5 => {
            ((u32::from_be((src as *const u32).read_unaligned()) as u64) << 8) | *src.add(4) as u64
        }
        6 => {
            ((u32::from_be((src as *const u32).read_unaligned()) as u64) << 16)
                | u16::from_be((src.add(4) as *const u16).read_unaligned()) as u64
        }
        7 => {
            ((u32::from_be((src as *const u32).read_unaligned()) as u64) << 24)
                | ((u16::from_be((src.add(4) as *const u16).read_unaligned()) as u64) << 8)
                | *src.add(6) as u64
        }
        8 => u64::from_be((src as *const u64).read_unaligned()),
        _ => unreachable!("source len for Decimal64 fast path must be in 1..=8"),
    }
}

#[inline]
unsafe fn decode_decimal64_from_be(src: *const u8, src_len: usize) -> i64 {
    if src_len <= 8 {
        let raw = load_be_u64_by_len(src, src_len);
        let shift = (8 - src_len) * 8;
        (raw << shift) as i64 >> shift
    } else {
        let trunc = src_len - 8;
        u64::from_be((src.add(trunc) as *const u64).read_unaligned()) as i64
    }
}

#[inline]
unsafe fn load_be_u64_signext_window(
    src: *const u8,
    src_len: usize,
    total_len: usize,
    window_start: usize,
    sign_byte: u8,
) -> u64 {
    debug_assert!(total_len >= src_len);
    debug_assert!(window_start + 8 <= total_len);

    let sign_prefix = total_len - src_len;
    if window_start + 8 <= sign_prefix {
        return if sign_byte == 0xFF { u64::MAX } else { 0 };
    }

    let prefix_len = sign_prefix.saturating_sub(window_start).min(8);
    let src_start = window_start.saturating_sub(sign_prefix);
    let available = src_len.saturating_sub(src_start);
    let take = (8 - prefix_len).min(available);

    if take == 0 {
        return if sign_byte == 0xFF { u64::MAX } else { 0 };
    }

    let fragment = load_be_u64_by_len(src.add(src_start), take);
    if sign_byte == 0xFF {
        if take == 8 {
            fragment
        } else {
            let sign_mask = (!0u64) << (take * 8);
            sign_mask | fragment
        }
    } else {
        fragment
    }
}

#[inline]
unsafe fn convert_decimal_to_target<const N: usize>(
    mut src: &[u8],
    dest: *mut u8,
    _source_name: &'static str,
) -> ParquetResult<()> {
    let mut src_len = src.len();

    if src_len > N {
        let trunc = src_len - N;
        src = &src[trunc..];
        src_len = N;
    }

    if N <= 8 {
        let raw = load_be_u64_by_len(src.as_ptr(), src_len);
        let shift = (8 - src_len) * 8;
        let signed = (raw << shift) as i64 >> shift;
        if N == 1 {
            *dest = signed as i8 as u8;
        } else if N == 2 {
            (dest as *mut i16).write_unaligned((signed as i16).to_le());
        } else if N == 4 {
            (dest as *mut i32).write_unaligned((signed as i32).to_le());
        } else if N == 8 {
            (dest as *mut i64).write_unaligned(signed.to_le());
        } else {
            unreachable!("unsupported small decimal target size {}", N);
        }
        return Ok(());
    }

    if src_len == N {
        if N == 16 {
            word_swap_exact::<2>(src, dest);
            return Ok(());
        }
        if N == 32 {
            word_swap_exact::<4>(src, dest);
            return Ok(());
        }
    }

    let sign_byte = if src[0] & 0x80 != 0 { 0xFF } else { 0x00 };
    if N > 8 {
        let words = N / 8;
        for w in 0..words {
            let word_start = w * 8;
            let word = load_be_u64_signext_window(src.as_ptr(), src_len, N, word_start, sign_byte);
            (dest.add(word_start) as *mut u64).write_unaligned(word);
        }
    }

    Ok(())
}

#[inline]
unsafe fn convert_fixed_decimal_to_target<const N: usize>(
    src: &[u8],
    dest: *mut u8,
) -> ParquetResult<()> {
    if src.is_empty() {
        return Err(fmt_err!(
            Unsupported,
            "invalid decimal source length 0 for target size {}",
            N
        ));
    }
    convert_decimal_to_target::<N>(src, dest, "FixedLenByteArray")
}

#[inline]
unsafe fn convert_byte_array_decimal_to_target<const N: usize>(
    src: &[u8],
    dest: *mut u8,
) -> ParquetResult<()> {
    if src.is_empty() {
        return Err(fmt_err!(
            Unsupported,
            "invalid ByteArray decimal source length 0 for target size {}",
            N
        ));
    }
    convert_decimal_to_target::<N>(src, dest, "ByteArray")
}

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
            reverse_exact::<N>(slice, ptr);
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
                    {
                        let dst = std::slice::from_raw_parts_mut(ptr, total_bytes);
                        let mut sink = SliceSink(dst);
                        self.slicer.next_slice_into(count, &mut sink)?;
                    }
                    if N == 2 {
                        for c in 0..count {
                            let p = ptr.add(c * 2) as *mut u16;
                            p.write_unaligned(p.read_unaligned().swap_bytes());
                        }
                    } else if N == 4 {
                        for c in 0..count {
                            let p = ptr.add(c * 4) as *mut u32;
                            p.write_unaligned(p.read_unaligned().swap_bytes());
                        }
                    } else if N == 8 {
                        for c in 0..count {
                            let p = ptr.add(c * 8) as *mut u64;
                            p.write_unaligned(p.read_unaligned().swap_bytes());
                        }
                    } else {
                        for c in 0..count {
                            let dest = ptr.add(c * N);
                            let mut i = 0usize;
                            while i < N / 2 {
                                let a = *dest.add(i);
                                let b = *dest.add(N - i - 1);
                                *dest.add(i) = b;
                                *dest.add(N - i - 1) = a;
                                i += 1;
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
            word_swap_exact::<WORDS>(slice, ptr);
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
                    {
                        let dst = std::slice::from_raw_parts_mut(ptr, total_bytes);
                        let mut sink = SliceSink(dst);
                        self.slicer.next_slice_into(count, &mut sink)?;
                    }
                    for c in 0..count {
                        let value_ptr = ptr.add(c * N);
                        for w in 0..WORDS {
                            let p = value_ptr.add(w * 8) as *mut u64;
                            p.write_unaligned(p.read_unaligned().swap_bytes());
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
/// If `src_len` > N, high-order bytes are truncated and low-order bytes are decoded.
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
            if N == 8 {
                const CHUNK_VALUES: usize = 256;
                let mut tmp = [0u8; CHUNK_VALUES * 32];
                let mut out_idx = 0usize;
                while out_idx < count {
                    let chunk = (count - out_idx).min(CHUNK_VALUES);
                    if let Some(raw_chunk) = self.slicer.next_raw_slice(chunk) {
                        for c in 0..chunk {
                            let src_ptr = raw_chunk.as_ptr().add(c * self.src_len);
                            let signed = decode_decimal64_from_be(src_ptr, self.src_len);
                            (ptr.add((out_idx + c) * 8) as *mut i64)
                                .write_unaligned(signed.to_le());
                        }
                    } else {
                        let src_bytes = chunk * self.src_len;
                        {
                            let mut sink = SliceSink(&mut tmp[..src_bytes]);
                            self.slicer.next_slice_into(chunk, &mut sink)?;
                        }

                        for c in 0..chunk {
                            let src_ptr = tmp.as_ptr().add(c * self.src_len);
                            let signed = decode_decimal64_from_be(src_ptr, self.src_len);
                            (ptr.add((out_idx + c) * 8) as *mut i64)
                                .write_unaligned(signed.to_le());
                        }
                    }
                    out_idx += chunk;
                }
            } else {
                for c in 0..count {
                    let slice = self.slicer.next();
                    let dest = ptr.add(c * N);
                    Self::sign_extend_and_convert(slice, dest, self.src_len)?;
                }
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
        convert_fixed_decimal_to_target::<N>(src, dest)
    }
}

fn decode_byte_array_decimal_with_slicer_mode<
    const FILTERED: bool,
    const FILL_NULLS: bool,
    T: DataPageSlicer,
>(
    page: &DataPage,
    bufs: &mut ColumnChunkBuffers,
    slicer: &mut T,
    mode: super::DecodeModeContext<'_>,
    target_tag: ColumnTypeTag,
) -> ParquetResult<()> {
    match target_tag {
        ColumnTypeTag::Decimal8 => super::decode_page0_mode::<_, FILTERED, FILL_NULLS>(
            page,
            mode,
            &mut ByteArrayDecimalColumnSink::<1, _>::new(slicer, bufs, DECIMAL8_NULL),
        ),
        ColumnTypeTag::Decimal16 => super::decode_page0_mode::<_, FILTERED, FILL_NULLS>(
            page,
            mode,
            &mut ByteArrayDecimalColumnSink::<2, _>::new(slicer, bufs, DECIMAL16_NULL),
        ),
        ColumnTypeTag::Decimal32 => super::decode_page0_mode::<_, FILTERED, FILL_NULLS>(
            page,
            mode,
            &mut ByteArrayDecimalColumnSink::<4, _>::new(slicer, bufs, DECIMAL32_NULL),
        ),
        ColumnTypeTag::Decimal64 => super::decode_page0_mode::<_, FILTERED, FILL_NULLS>(
            page,
            mode,
            &mut ByteArrayDecimalColumnSink::<8, _>::new(slicer, bufs, DECIMAL64_NULL),
        ),
        ColumnTypeTag::Decimal128 => super::decode_page0_mode::<_, FILTERED, FILL_NULLS>(
            page,
            mode,
            &mut ByteArrayDecimalColumnSink::<16, _>::new(slicer, bufs, DECIMAL128_NULL),
        ),
        ColumnTypeTag::Decimal256 => super::decode_page0_mode::<_, FILTERED, FILL_NULLS>(
            page,
            mode,
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
        convert_byte_array_decimal_to_target::<N>(src, dest)
    }
}

struct PreconvertedFlbaDecimalDict<T: Copy> {
    values: Vec<T>,
}

impl<T: Copy> PrimitiveDictDecoder<T> for PreconvertedFlbaDecimalDict<T> {
    #[inline]
    fn get_dict_value(&self, index: u32) -> T {
        self.values[index as usize]
    }

    #[inline]
    fn len(&self) -> u32 {
        self.values.len() as u32
    }
}

impl PreconvertedFlbaDecimalDict<Decimal8> {
    fn try_new_decimal8(dict_page: &DictPage, src_len: usize) -> ParquetResult<Self> {
        validate_flba_dict(dict_page, src_len)?;
        let mut values = Vec::with_capacity(dict_page.num_values);
        for i in 0..dict_page.num_values {
            let src = &dict_page.buffer[i * src_len..(i + 1) * src_len];
            let mut buf = [0u8; 1];
            unsafe { convert_fixed_decimal_to_target::<1>(src, buf.as_mut_ptr())? };
            values.push(Decimal8(buf[0] as i8));
        }
        Ok(Self { values })
    }
}

impl PreconvertedFlbaDecimalDict<Decimal16> {
    fn try_new_decimal16(dict_page: &DictPage, src_len: usize) -> ParquetResult<Self> {
        validate_flba_dict(dict_page, src_len)?;
        let mut values = Vec::with_capacity(dict_page.num_values);
        for i in 0..dict_page.num_values {
            let src = &dict_page.buffer[i * src_len..(i + 1) * src_len];
            let mut buf = [0u8; 2];
            unsafe { convert_fixed_decimal_to_target::<2>(src, buf.as_mut_ptr())? };
            values.push(Decimal16(i16::from_le_bytes(buf)));
        }
        Ok(Self { values })
    }
}

impl PreconvertedFlbaDecimalDict<Decimal32> {
    fn try_new_decimal32(dict_page: &DictPage, src_len: usize) -> ParquetResult<Self> {
        validate_flba_dict(dict_page, src_len)?;
        let mut values = Vec::with_capacity(dict_page.num_values);
        for i in 0..dict_page.num_values {
            let src = &dict_page.buffer[i * src_len..(i + 1) * src_len];
            let mut buf = [0u8; 4];
            unsafe { convert_fixed_decimal_to_target::<4>(src, buf.as_mut_ptr())? };
            values.push(Decimal32(i32::from_le_bytes(buf)));
        }
        Ok(Self { values })
    }
}

impl PreconvertedFlbaDecimalDict<Decimal64> {
    fn try_new_decimal64(dict_page: &DictPage, src_len: usize) -> ParquetResult<Self> {
        validate_flba_dict(dict_page, src_len)?;
        let mut values = Vec::with_capacity(dict_page.num_values);
        for i in 0..dict_page.num_values {
            let src = &dict_page.buffer[i * src_len..(i + 1) * src_len];
            let mut buf = [0u8; 8];
            unsafe { convert_fixed_decimal_to_target::<8>(src, buf.as_mut_ptr())? };
            values.push(Decimal64(i64::from_le_bytes(buf)));
        }
        Ok(Self { values })
    }
}

impl PreconvertedFlbaDecimalDict<Decimal128> {
    fn try_new_decimal128(dict_page: &DictPage, src_len: usize) -> ParquetResult<Self> {
        validate_flba_dict(dict_page, src_len)?;
        let mut values = Vec::with_capacity(dict_page.num_values);
        for i in 0..dict_page.num_values {
            let src = &dict_page.buffer[i * src_len..(i + 1) * src_len];
            let mut buf = [0u8; 16];
            unsafe { convert_fixed_decimal_to_target::<16>(src, buf.as_mut_ptr())? };
            let hi = i64::from_le_bytes(buf[0..8].try_into().unwrap());
            let lo = u64::from_le_bytes(buf[8..16].try_into().unwrap());
            values.push(Decimal128(hi, lo));
        }
        Ok(Self { values })
    }
}

impl PreconvertedFlbaDecimalDict<Decimal256> {
    fn try_new_decimal256(dict_page: &DictPage, src_len: usize) -> ParquetResult<Self> {
        validate_flba_dict(dict_page, src_len)?;
        let mut values = Vec::with_capacity(dict_page.num_values);
        for i in 0..dict_page.num_values {
            let src = &dict_page.buffer[i * src_len..(i + 1) * src_len];
            let mut buf = [0u8; 32];
            unsafe { convert_fixed_decimal_to_target::<32>(src, buf.as_mut_ptr())? };
            let w0 = i64::from_le_bytes(buf[0..8].try_into().unwrap());
            let w1 = u64::from_le_bytes(buf[8..16].try_into().unwrap());
            let w2 = u64::from_le_bytes(buf[16..24].try_into().unwrap());
            let w3 = u64::from_le_bytes(buf[24..32].try_into().unwrap());
            values.push(Decimal256(w0, w1, w2, w3));
        }
        Ok(Self { values })
    }
}

fn validate_flba_dict(dict_page: &DictPage, src_len: usize) -> ParquetResult<()> {
    if src_len == 0 {
        return Err(fmt_err!(Layout, "dictionary fixed value size must be > 0"));
    }
    if src_len * dict_page.num_values != dict_page.buffer.len() {
        return Err(fmt_err!(
            Layout,
            "dictionary data page size is not multiple of {src_len}"
        ));
    }
    Ok(())
}

fn decode_fixed_decimal_with_slicer_mode<
    const FILTERED: bool,
    const FILL_NULLS: bool,
    T: DataPageSlicer,
>(
    page: &DataPage,
    bufs: &mut ColumnChunkBuffers,
    slicer: &mut T,
    mode: super::DecodeModeContext<'_>,
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
                super::decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                    page,
                    mode,
                    &mut ReverseDecimalColumnSink::<1, _>::new(slicer, bufs, DECIMAL8_NULL),
                )
            } else {
                super::decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                    page,
                    mode,
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
                super::decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                    page,
                    mode,
                    &mut ReverseDecimalColumnSink::<2, _>::new(slicer, bufs, DECIMAL16_NULL),
                )
            } else {
                super::decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                    page,
                    mode,
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
                super::decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                    page,
                    mode,
                    &mut ReverseDecimalColumnSink::<4, _>::new(slicer, bufs, DECIMAL32_NULL),
                )
            } else {
                super::decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                    page,
                    mode,
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
                super::decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                    page,
                    mode,
                    &mut ReverseDecimalColumnSink::<8, _>::new(slicer, bufs, DECIMAL64_NULL),
                )
            } else {
                super::decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                    page,
                    mode,
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
                super::decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                    page,
                    mode,
                    &mut WordSwapDecimalColumnSink::<16, 2, _>::new(slicer, bufs, DECIMAL128_NULL),
                )
            } else {
                super::decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                    page,
                    mode,
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
                super::decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                    page,
                    mode,
                    &mut WordSwapDecimalColumnSink::<32, 4, _>::new(slicer, bufs, DECIMAL256_NULL),
                )
            } else {
                super::decode_page0_mode::<_, FILTERED, FILL_NULLS>(
                    page,
                    mode,
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
