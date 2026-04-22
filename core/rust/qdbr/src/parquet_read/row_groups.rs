use crate::allocator::{AcVec, QdbAllocator};
use crate::parquet::error::{fmt_err, ParquetErrorExt, ParquetResult};
use crate::parquet::qdb_metadata::{QdbMeta, QdbMetaCol};
use crate::parquet_read::column_sink::var::fixup_varchar_slice_spill_pointers;
use crate::parquet_read::decode::{
    decode_page, decode_page_filtered, decompress_sliced_data, decompress_sliced_dict,
    page_row_count, sliced_page_row_count,
};
use crate::parquet_read::page::{DataPage, DictPage};
use crate::parquet_read::{
    ColumnChunkBuffers, ColumnFilterPacked, ColumnFilterValues, ColumnMeta, DecodeContext,
    RowGroupStatBuffers, FILTER_OP_BETWEEN, FILTER_OP_EQ, FILTER_OP_GE, FILTER_OP_GT,
    FILTER_OP_IS_NOT_NULL, FILTER_OP_IS_NULL, FILTER_OP_LE, FILTER_OP_LT, MILLIS_PER_DAY,
};
use nonmax::NonMaxU32;
use parquet2::encoding::Encoding;
use parquet2::metadata::FileMetaData;
use parquet2::read::{SlicePageReader, SlicedDataPage, SlicedDictPage, SlicedPage};
use parquet2::schema::types::{PhysicalType, PrimitiveConvertedType, PrimitiveLogicalType};
use qdb_core::col_type::{nulls, ColumnType, ColumnTypeTag, QDB_TIMESTAMP_NS_COLUMN_TYPE_FLAG};
use std::{cmp, mem::size_of, ptr, slice};

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
    pub unused_bytes: u64,
    pub column_structure_version: i32,
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

/// Decompress a varchar_slice data page, choosing the buffer strategy based on encoding.
///
/// For dictionary and DeltaByteArray encodings, aux entries don't reference
/// the data page buffer (they point to the dict buffer or `data_vec`), so
/// the buffer can be reused. For other encodings (Plain, DeltaLengthByteArray),
/// aux entries point directly into the page buffer, so it must persist.
fn decompress_varchar_slice_data<'a>(
    page: &'a SlicedDataPage<'a>,
    reusable_buf: &'a mut Vec<u8>,
    persistent_bufs: &'a mut Vec<Vec<u8>>,
    buf_pool: &mut Vec<Vec<u8>>,
) -> ParquetResult<DataPage<'a>> {
    match page.encoding() {
        Encoding::RleDictionary | Encoding::PlainDictionary | Encoding::DeltaByteArray => {
            decompress_sliced_data(page, reusable_buf)
        }
        _ => {
            let mut buf = buf_pool.pop().unwrap_or_default();
            buf.clear();
            persistent_bufs.push(buf);
            decompress_sliced_data(page, persistent_bufs.last_mut().unwrap())
        }
    }
}

/// Apply post-decode conversions that cannot be handled by the per-page decode dispatch,
/// typically because the source and target share the same physical representation.
fn post_convert(
    from_type: ColumnType,
    to_type: ColumnType,
    bufs: &mut ColumnChunkBuffers,
) -> ParquetResult<()> {
    let src_tag = from_type.tag();
    let dst_tag = to_type.tag();
    match (src_tag, dst_tag) {
        (ColumnTypeTag::Boolean, ColumnTypeTag::Byte) => {
            // Same physical size (1 byte), no expansion needed.
        }
        (ColumnTypeTag::Boolean, ColumnTypeTag::Short) => {
            expand_bool::<i16>(&mut bufs.data_vec)?;
        }
        (ColumnTypeTag::Boolean, ColumnTypeTag::Int) => {
            expand_bool::<i32>(&mut bufs.data_vec)?;
        }
        (ColumnTypeTag::Boolean, ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp) => {
            expand_bool::<i64>(&mut bufs.data_vec)?;
        }
        (ColumnTypeTag::Boolean, ColumnTypeTag::Float) => {
            expand_bool::<f32>(&mut bufs.data_vec)?;
        }
        (ColumnTypeTag::Boolean, ColumnTypeTag::Double) => {
            expand_bool::<f64>(&mut bufs.data_vec)?;
        }
        // Fixed → Boolean: contract decoded source-sized values to 1-byte booleans.
        // Null sentinels (i32::MIN, i64::MIN, NaN) map to 0 (false), not 1.
        (ColumnTypeTag::Byte, ColumnTypeTag::Boolean) => {
            contract_to_bool::<i8>(&mut bufs.data_vec, |_| false);
        }
        (ColumnTypeTag::Short | ColumnTypeTag::Char, ColumnTypeTag::Boolean) => {
            contract_to_bool::<i16>(&mut bufs.data_vec, |_| false);
        }
        (ColumnTypeTag::Int | ColumnTypeTag::IPv4, ColumnTypeTag::Boolean) => {
            contract_to_bool::<i32>(&mut bufs.data_vec, |v| v == nulls::INT);
        }
        (ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp, ColumnTypeTag::Boolean) => {
            contract_to_bool::<i64>(&mut bufs.data_vec, |v| v == nulls::LONG);
        }
        (ColumnTypeTag::Float, ColumnTypeTag::Boolean) => {
            contract_to_bool::<f32>(&mut bufs.data_vec, |v| v.is_nan());
        }
        (ColumnTypeTag::Double, ColumnTypeTag::Boolean) => {
            contract_to_bool::<f64>(&mut bufs.data_vec, |v| v.is_nan());
        }
        (ColumnTypeTag::Date, ColumnTypeTag::Timestamp) => {
            // Date milliseconds → Timestamp microseconds: multiply by 1000.
            scale_i64_in_place(&mut bufs.data_vec, 1000, false);
        }
        (ColumnTypeTag::Timestamp, ColumnTypeTag::Date) => {
            // Timestamp microseconds → Date milliseconds: divide by 1000.
            scale_i64_in_place(&mut bufs.data_vec, 1000, true);
        }
        // Fixed → Varchar: Java handles batch conversion after decode.
        (src, ColumnTypeTag::Varchar) if is_fixed_to_var_source(src) => {}
        // Fixed → String: Java handles batch conversion after decode.
        (src, ColumnTypeTag::String) if is_fixed_to_var_source(src) => {}
        // Var → fixed: Java handles batch conversion after decode.
        (ColumnTypeTag::Varchar | ColumnTypeTag::String, dst) if is_var_to_fixed_target(dst) => {}
        // Decimal → Decimal: rescale if source and target have different scales.
        (src, dst) if is_decimal_tag(src) && is_decimal_tag(dst) => {
            let src_scale = from_type.decimal_scale();
            let dst_scale = to_type.decimal_scale();
            if src_scale != dst_scale {
                rescale_decimal_in_place(&mut bufs.data_vec, dst, src_scale, dst_scale)?;
            }
        }
        // Fixed integer → Decimal: widen to target size and scale by 10^(target_scale).
        (ColumnTypeTag::Byte | ColumnTypeTag::Short | ColumnTypeTag::Int | ColumnTypeTag::Long,
         dst) if is_decimal_tag(dst) => {
            convert_fixed_to_decimal(&mut bufs.data_vec, src_tag, dst, to_type.decimal_scale())?;
        }
        _ => return Ok(()),
    }
    bufs.data_ptr = bufs.data_vec.as_mut_ptr();
    bufs.data_size = bufs.data_vec.len();
    Ok(())
}

/// Expand 1-byte boolean values (0/1) to wider `T` values in place.
/// Iterates backwards so wider writes don't overwrite unread bytes.
fn expand_bool<T: From<u8> + Copy>(data: &mut AcVec<u8>) -> ParquetResult<()> {
    let n = data.len();
    if n == 0 {
        return Ok(());
    }
    let needed = n * size_of::<T>();
    data.reserve(needed - n)?;
    unsafe { data.set_len(needed) };
    let ptr = data.as_mut_ptr();
    for i in (0..n).rev() {
        let val = T::from(unsafe { *ptr.add(i) });
        unsafe { (ptr.add(i * size_of::<T>()) as *mut T).write_unaligned(val) };
    }
    Ok(())
}

/// Contract wider fixed-size values to 1-byte booleans in place.
/// Non-zero, non-null values become 1; zero and null-sentinel values become 0.
/// The `is_null` predicate identifies the source type's null sentinel (e.g. `i32::MIN`
/// for INT, `NaN` for FLOAT/DOUBLE) so that NULL maps to `false` rather than `true`.
/// Iterates forward because the destination (1 byte) is always <= the source size.
fn contract_to_bool<T: Default + PartialEq + Copy>(
    data: &mut AcVec<u8>,
    is_null: impl Fn(T) -> bool,
) {
    let elem_size = size_of::<T>();
    let n = data.len() / elem_size;
    if n == 0 {
        return;
    }
    let ptr = data.as_mut_ptr();
    let zero = T::default();
    for i in 0..n {
        let val: T = unsafe { (ptr.add(i * elem_size) as *const T).read_unaligned() };
        unsafe { *ptr.add(i) = if val != zero && !is_null(val) { 1 } else { 0 } };
    }
    unsafe { data.set_len(n) };
}

/// Multiply or divide every non-null i64 in the buffer by `factor`.
fn scale_i64_in_place(data: &mut AcVec<u8>, factor: i64, divide: bool) {
    let count = data.len() / size_of::<i64>();
    let ptr = data.as_mut_ptr() as *mut i64;
    for i in 0..count {
        let val = unsafe { ptr.add(i).read_unaligned() };
        let converted = if val == qdb_core::col_type::nulls::LONG {
            qdb_core::col_type::nulls::LONG
        } else if divide {
            val / factor
        } else {
            val * factor
        };
        unsafe { ptr.add(i).write_unaligned(converted) };
    }
}

/// Returns true for decimal type tags.
fn is_decimal_tag(tag: ColumnTypeTag) -> bool {
    matches!(
        tag,
        ColumnTypeTag::Decimal8
            | ColumnTypeTag::Decimal16
            | ColumnTypeTag::Decimal32
            | ColumnTypeTag::Decimal64
            | ColumnTypeTag::Decimal128
            | ColumnTypeTag::Decimal256
    )
}

/// Rescale decoded decimal values in place by multiplying or dividing by 10^|scale_diff|.
/// Called when a Decimal column's scale changed after the parquet partition was written.
fn rescale_decimal_in_place(
    data: &mut AcVec<u8>,
    target_tag: ColumnTypeTag,
    src_scale: u8,
    dst_scale: u8,
) -> ParquetResult<()> {
    let abs_diff = (dst_scale as i32 - src_scale as i32).unsigned_abs();
    let divide = dst_scale < src_scale;
    match target_tag {
        ColumnTypeTag::Decimal8 => rescale_fixed::<1>(data, abs_diff, divide),
        ColumnTypeTag::Decimal16 => rescale_fixed::<2>(data, abs_diff, divide),
        ColumnTypeTag::Decimal32 => rescale_fixed::<4>(data, abs_diff, divide),
        ColumnTypeTag::Decimal64 => rescale_fixed::<8>(data, abs_diff, divide),
        ColumnTypeTag::Decimal128 => rescale_i128(data, abs_diff, divide),
        ColumnTypeTag::Decimal256 => rescale_i256(data, abs_diff, divide),
        _ => {}
    }
    Ok(())
}

/// Rescale Decimal8/16/32/64 values (N = 1, 2, 4, or 8 bytes) in place.
fn rescale_fixed<const N: usize>(data: &mut AcVec<u8>, scale_diff: u32, divide: bool) {
    debug_assert!(N == 1 || N == 2 || N == 4 || N == 8);
    let count = data.len() / N;
    let ptr = data.as_mut_ptr();
    // For N <= 8, 10^max_scale fits in the native integer (max scales: 2, 4, 9, 18).
    // Use i64 arithmetic for all sizes to keep things simple.
    let factor = 10i64.wrapping_pow(scale_diff);
    for i in 0..count {
        let offset = i * N;
        unsafe {
            let val = read_le_i64::<N>(ptr.add(offset));
            // Null sentinel is MIN for each width.
            let null = match N {
                1 => i8::MIN as i64,
                2 => i16::MIN as i64,
                4 => i32::MIN as i64,
                _ => i64::MIN,
            };
            if val == null {
                continue;
            }
            let scaled = if divide { val / factor } else { val.wrapping_mul(factor) };
            write_le_i64::<N>(ptr.add(offset), scaled);
        }
    }
}

#[inline]
unsafe fn read_le_i64<const N: usize>(ptr: *const u8) -> i64 {
    match N {
        1 => *(ptr as *const i8) as i64,
        2 => (ptr as *const i16).read_unaligned() as i64,
        4 => (ptr as *const i32).read_unaligned() as i64,
        _ => (ptr as *const i64).read_unaligned(),
    }
}

#[inline]
unsafe fn write_le_i64<const N: usize>(ptr: *mut u8, val: i64) {
    match N {
        1 => *(ptr as *mut i8) = val as i8,
        2 => (ptr as *mut i16).write_unaligned(val as i16),
        4 => (ptr as *mut i32).write_unaligned(val as i32),
        _ => (ptr as *mut i64).write_unaligned(val),
    }
}

/// Rescale Decimal128 values in place. Layout: [hi: i64 LE, lo: u64 LE].
fn rescale_i128(data: &mut AcVec<u8>, scale_diff: u32, divide: bool) {
    let factor = 10i128.wrapping_pow(scale_diff);
    let count = data.len() / 16;
    let ptr = data.as_mut_ptr();
    for i in 0..count {
        let offset = i * 16;
        let hi = unsafe { (ptr.add(offset) as *const i64).read_unaligned() };
        let lo = unsafe { (ptr.add(offset + 8) as *const u64).read_unaligned() };
        if hi == i64::MIN && lo == 0 {
            continue;
        }
        let val = ((hi as i128) << 64) | (lo as i128);
        let scaled = if divide { val / factor } else { val.wrapping_mul(factor) };
        unsafe {
            (ptr.add(offset) as *mut i64).write_unaligned((scaled >> 64) as i64);
            (ptr.add(offset + 8) as *mut u64).write_unaligned(scaled as u64);
        }
    }
}

/// Rescale Decimal256 values in place. Layout: [w0(hi): i64 LE, w1: u64 LE, w2: u64 LE, w3(lo): u64 LE].
/// Value = w0 * 2^192 + w1 * 2^128 + w2 * 2^64 + w3.
fn rescale_i256(data: &mut AcVec<u8>, scale_diff: u32, divide: bool) {
    let count = data.len() / 32;
    let ptr = data.as_mut_ptr();
    for i in 0..count {
        let offset = i * 32;
        let w0 = unsafe { (ptr.add(offset) as *const i64).read_unaligned() };
        let w1 = unsafe { (ptr.add(offset + 8) as *const u64).read_unaligned() };
        let w2 = unsafe { (ptr.add(offset + 16) as *const u64).read_unaligned() };
        let w3 = unsafe { (ptr.add(offset + 24) as *const u64).read_unaligned() };
        if w0 == i64::MIN && w1 == 0 && w2 == 0 && w3 == 0 {
            continue;
        }
        let (nw0, nw1, nw2, nw3) = if divide {
            div_i256_pow10(w0, w1, w2, w3, scale_diff)
        } else {
            mul_i256_pow10(w0, w1, w2, w3, scale_diff)
        };
        unsafe {
            (ptr.add(offset) as *mut i64).write_unaligned(nw0);
            (ptr.add(offset + 8) as *mut u64).write_unaligned(nw1);
            (ptr.add(offset + 16) as *mut u64).write_unaligned(nw2);
            (ptr.add(offset + 24) as *mut u64).write_unaligned(nw3);
        }
    }
}

fn mul_i256_pow10(w0: i64, w1: u64, w2: u64, w3: u64, scale_diff: u32) -> (i64, u64, u64, u64) {
    let mut r = (w0, w1, w2, w3);
    let mut remaining = scale_diff;
    while remaining > 0 {
        let step = remaining.min(18); // 10^18 fits in u64
        r = mul_i256_u64(r.0, r.1, r.2, r.3, 10u64.pow(step));
        remaining -= step;
    }
    r
}

/// Multiply a 256-bit two's complement integer by a u64 factor.
fn mul_i256_u64(w0: i64, w1: u64, w2: u64, w3: u64, factor: u64) -> (i64, u64, u64, u64) {
    let f = factor as u128;
    let p3 = w3 as u128 * f;
    let p2 = w2 as u128 * f + (p3 >> 64);
    let p1 = w1 as u128 * f + (p2 >> 64);
    let p0 = w0 as i128 * f as i128 + (p1 >> 64) as i128;
    (p0 as i64, p1 as u64, p2 as u64, p3 as u64)
}

fn div_i256_pow10(w0: i64, w1: u64, w2: u64, w3: u64, scale_diff: u32) -> (i64, u64, u64, u64) {
    let mut r = (w0, w1, w2, w3);
    let mut remaining = scale_diff;
    while remaining > 0 {
        let step = remaining.min(18);
        r = div_i256_u64(r.0, r.1, r.2, r.3, 10u64.pow(step));
        remaining -= step;
    }
    r
}

/// Divide a 256-bit two's complement integer by a u64 divisor (truncation toward zero).
fn div_i256_u64(w0: i64, w1: u64, w2: u64, w3: u64, divisor: u64) -> (i64, u64, u64, u64) {
    let neg = w0 < 0;
    let (aw0, aw1, aw2, aw3) = if neg { negate_i256(w0, w1, w2, w3) } else { (w0, w1, w2, w3) };
    let d = divisor as u128;
    let p0 = aw0 as u64 as u128;
    let q0 = (p0 / d) as u64;
    let p1 = ((p0 % d) << 64) | aw1 as u128;
    let q1 = (p1 / d) as u64;
    let p2 = ((p1 % d) << 64) | aw2 as u128;
    let q2 = (p2 / d) as u64;
    let p3 = ((p2 % d) << 64) | aw3 as u128;
    let q3 = (p3 / d) as u64;
    if neg { negate_i256(q0 as i64, q1, q2, q3) } else { (q0 as i64, q1, q2, q3) }
}

fn negate_i256(w0: i64, w1: u64, w2: u64, w3: u64) -> (i64, u64, u64, u64) {
    let (n3, c3) = (!w3).overflowing_add(1);
    let (n2, c2) = (!w2).overflowing_add(c3 as u64);
    let (n1, c1) = (!w1).overflowing_add(c2 as u64);
    let n0 = (!(w0 as u64)).wrapping_add(c1 as u64) as i64;
    (n0, n1, n2, n3)
}

/// Convert decoded fixed integer values (BYTE/SHORT/INT/LONG) to a target decimal type.
/// Widens each value from the source size to the target decimal size, then multiplies
/// by 10^scale. Iterates backwards when the target is wider to avoid overwriting unread data.
fn convert_fixed_to_decimal(
    data: &mut AcVec<u8>,
    src_tag: ColumnTypeTag,
    dst_tag: ColumnTypeTag,
    dst_scale: u8,
) -> ParquetResult<()> {
    let src_size = fixed_tag_size(src_tag);
    let dst_size = decimal_tag_size(dst_tag);
    let count = data.len() / src_size;
    if count == 0 {
        return Ok(());
    }

    // Grow the buffer if target is wider.
    let needed = count * dst_size;
    if needed > data.len() {
        data.reserve(needed - data.len())?;
    }
    unsafe { data.set_len(needed) };
    let ptr = data.as_mut_ptr();

    match dst_tag {
        // Target Decimal8..Decimal64: use i64 arithmetic.
        tag if decimal_tag_size(tag) <= 8 => {
            let factor = 10i64.wrapping_pow(dst_scale as u32);
            if dst_size >= src_size {
                for i in (0..count).rev() {
                    let val = unsafe { read_le_i64_at(ptr, i, src_size) };
                    let scaled = if is_int_null(val, src_tag) {
                        null_i64_for_decimal(dst_tag)
                    } else {
                        val.wrapping_mul(factor)
                    };
                    unsafe { write_le_i64_at(ptr, i, dst_size, scaled) };
                }
            } else {
                for i in 0..count {
                    let val = unsafe { read_le_i64_at(ptr, i, src_size) };
                    let scaled = if is_int_null(val, src_tag) {
                        null_i64_for_decimal(dst_tag)
                    } else {
                        val.wrapping_mul(factor)
                    };
                    unsafe { write_le_i64_at(ptr, i, dst_size, scaled) };
                }
            }
        }
        // Target Decimal128: widen to i128, scale, write as (hi, lo).
        ColumnTypeTag::Decimal128 => {
            let factor = 10i128.wrapping_pow(dst_scale as u32);
            for i in (0..count).rev() {
                let val = unsafe { read_le_i64_at(ptr, i, src_size) };
                let offset = i * 16;
                if is_int_null(val, src_tag) {
                    unsafe {
                        (ptr.add(offset) as *mut i64).write_unaligned(i64::MIN);
                        (ptr.add(offset + 8) as *mut u64).write_unaligned(0);
                    }
                } else {
                    let scaled = (val as i128).wrapping_mul(factor);
                    unsafe {
                        (ptr.add(offset) as *mut i64).write_unaligned((scaled >> 64) as i64);
                        (ptr.add(offset + 8) as *mut u64).write_unaligned(scaled as u64);
                    }
                }
            }
        }
        // Target Decimal256: widen to 256-bit, scale, write as (w0, w1, w2, w3).
        ColumnTypeTag::Decimal256 => {
            for i in (0..count).rev() {
                let val = unsafe { read_le_i64_at(ptr, i, src_size) };
                let offset = i * 32;
                if is_int_null(val, src_tag) {
                    unsafe {
                        (ptr.add(offset) as *mut i64).write_unaligned(i64::MIN);
                        (ptr.add(offset + 8) as *mut u64).write_unaligned(0);
                        (ptr.add(offset + 16) as *mut u64).write_unaligned(0);
                        (ptr.add(offset + 24) as *mut u64).write_unaligned(0);
                    }
                } else {
                    // Sign-extend to 256-bit: (sign, sign, sign, val)
                    let sign = if val < 0 { u64::MAX } else { 0 };
                    let (w0, w1, w2, w3) = mul_i256_pow10(
                        sign as i64, sign, sign, val as u64, dst_scale as u32,
                    );
                    unsafe {
                        (ptr.add(offset) as *mut i64).write_unaligned(w0);
                        (ptr.add(offset + 8) as *mut u64).write_unaligned(w1);
                        (ptr.add(offset + 16) as *mut u64).write_unaligned(w2);
                        (ptr.add(offset + 24) as *mut u64).write_unaligned(w3);
                    }
                }
            }
        }
        _ => {}
    }
    Ok(())
}

#[inline]
unsafe fn read_le_i64_at(ptr: *const u8, idx: usize, elem_size: usize) -> i64 {
    let p = ptr.add(idx * elem_size);
    match elem_size {
        1 => *(p as *const i8) as i64,
        2 => (p as *const i16).read_unaligned() as i64,
        4 => (p as *const i32).read_unaligned() as i64,
        _ => (p as *const i64).read_unaligned(),
    }
}

#[inline]
unsafe fn write_le_i64_at(ptr: *mut u8, idx: usize, elem_size: usize, val: i64) {
    let p = ptr.add(idx * elem_size);
    match elem_size {
        1 => *(p as *mut i8) = val as i8,
        2 => (p as *mut i16).write_unaligned(val as i16),
        4 => (p as *mut i32).write_unaligned(val as i32),
        _ => (p as *mut i64).write_unaligned(val),
    }
}

/// Returns true if the value is a null sentinel for the given integer source type.
#[inline]
fn is_int_null(val: i64, src_tag: ColumnTypeTag) -> bool {
    match src_tag {
        // BYTE and SHORT have no null sentinel in QuestDB.
        ColumnTypeTag::Byte | ColumnTypeTag::Short => false,
        ColumnTypeTag::Int => val == i32::MIN as i64,
        _ => val == i64::MIN,
    }
}

/// Returns the null sentinel as i64 for a small decimal target (size <= 8).
#[inline]
fn null_i64_for_decimal(tag: ColumnTypeTag) -> i64 {
    match tag {
        ColumnTypeTag::Decimal8 => i8::MIN as i64,
        ColumnTypeTag::Decimal16 => i16::MIN as i64,
        ColumnTypeTag::Decimal32 => i32::MIN as i64,
        _ => i64::MIN,
    }
}

fn fixed_tag_size(tag: ColumnTypeTag) -> usize {
    match tag {
        ColumnTypeTag::Byte | ColumnTypeTag::Boolean => 1,
        ColumnTypeTag::Short | ColumnTypeTag::Char => 2,
        ColumnTypeTag::Int | ColumnTypeTag::IPv4 | ColumnTypeTag::Float => 4,
        ColumnTypeTag::Long | ColumnTypeTag::Double | ColumnTypeTag::Date | ColumnTypeTag::Timestamp => 8,
        _ => 8,
    }
}

fn decimal_tag_size(tag: ColumnTypeTag) -> usize {
    match tag {
        ColumnTypeTag::Decimal8 => 1,
        ColumnTypeTag::Decimal16 => 2,
        ColumnTypeTag::Decimal32 => 4,
        ColumnTypeTag::Decimal64 => 8,
        ColumnTypeTag::Decimal128 => 16,
        ColumnTypeTag::Decimal256 => 32,
        _ => 8,
    }
}

/// Returns true for source types supported in fixed-to-var conversion.
fn is_fixed_to_var_source(tag: ColumnTypeTag) -> bool {
    matches!(
        tag,
        ColumnTypeTag::Boolean
            | ColumnTypeTag::Byte
            | ColumnTypeTag::Short
            | ColumnTypeTag::Char
            | ColumnTypeTag::Int
            | ColumnTypeTag::Long
            | ColumnTypeTag::Float
            | ColumnTypeTag::Double
            | ColumnTypeTag::Date
            | ColumnTypeTag::Timestamp
            | ColumnTypeTag::IPv4
            | ColumnTypeTag::Uuid
    )
}

/// Returns true for target types supported in var-to-fixed conversion.
fn is_var_to_fixed_target(tag: ColumnTypeTag) -> bool {
    matches!(
        tag,
        ColumnTypeTag::Boolean
            | ColumnTypeTag::Byte
            | ColumnTypeTag::Short
            | ColumnTypeTag::Char
            | ColumnTypeTag::Int
            | ColumnTypeTag::Long
            | ColumnTypeTag::Float
            | ColumnTypeTag::Double
            | ColumnTypeTag::Date
            | ColumnTypeTag::Timestamp
            | ColumnTypeTag::IPv4
            | ColumnTypeTag::Uuid
    )
}

/// Decompress a varchar_slice dictionary page into a fresh buffer drawn from `buf_pool`, then
/// move that buffer into `persistent_bufs` so it lives for the full column-chunk decode.
///
/// `RleDictVarcharSliceDecoder` writes raw pointers from the dict buffer into the persistent
/// `aux_vec`. If multiple dict pages in the same column chunk shared a single backing buffer,
/// each new dict would overwrite the previous one and invalidate aux entries written by the
/// data pages decoded against the earlier dict. Allocating a fresh buffer per dict page keeps
/// every previously-decoded aux pointer valid for the lifetime of the column-chunk decode.
///
/// For uncompressed dict pages the bytes already live in the caller-owned mmap region for the
/// duration of the decode call, so we reuse the existing slice instead of allocating.
///
/// # Safety
/// Pushing a `Vec<u8>` into the outer `persistent_bufs` only moves the `(ptr, len, cap)`
/// triple; the heap allocation that the inner pointer references stays at the same address.
/// `persistent_bufs` is moved into `column_chunk_bufs.page_buffers` at the end of the
/// column-chunk loop, so the returned slice is valid for the full column-chunk decode.
fn decompress_varchar_slice_dict<'bufs>(
    dict_page: SlicedDictPage<'_>,
    persistent_bufs: &'bufs mut Vec<Vec<u8>>,
    buf_pool: &mut Vec<Vec<u8>>,
) -> ParquetResult<DictPage<'bufs>> {
    let num_values = dict_page.num_values;
    let is_sorted = dict_page.is_sorted;
    let (ptr, len) = if dict_page.compression == parquet2::compression::Compression::Uncompressed {
        (dict_page.buffer.as_ptr(), dict_page.buffer.len())
    } else {
        let mut buf = buf_pool.pop().unwrap_or_default();
        buf.clear();
        buf.resize(dict_page.uncompressed_size, 0);
        parquet2::compression::decompress(dict_page.compression, dict_page.buffer, &mut buf)?;
        let ptr = buf.as_ptr();
        let len = buf.len();
        persistent_bufs.push(buf);
        (ptr, len)
    };
    // SAFETY: see function-level doc comment.
    let buffer: &'bufs [u8] = unsafe { std::slice::from_raw_parts(ptr, len) };
    Ok(DictPage { buffer, num_values, is_sorted })
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
                && (to_column_type.tag() == ColumnTypeTag::Varchar
                    || to_column_type.tag() == ColumnTypeTag::VarcharSlice
                    || to_column_type.tag() == ColumnTypeTag::String)
            {
                column_type = to_column_type;
            }

            // Allow requesting VarcharSlice when the file stores Varchar or String.
            // VarcharSlice is a zero-copy decode format; parquet stores both as UTF-8.
            if (column_type.tag() == ColumnTypeTag::Varchar
                || column_type.tag() == ColumnTypeTag::String)
                && to_column_type.tag() == ColumnTypeTag::VarcharSlice
            {
                column_type = to_column_type;
            }

            let original_column_type = column_type;
            let src_tag = column_type.tag();
            if column_type != to_column_type {
                // Allow fixed-to-fixed type conversions (ALTER COLUMN TYPE).
                // The output buffer is sized for the target type. The decode dispatch
                // reads the source physical type from parquet and converts on the fly.
                match (src_tag, to_column_type.tag()) {
                    // Integer widening (Timestamp/Date share i64 representation with Long)
                    (ColumnTypeTag::Byte, ColumnTypeTag::Short | ColumnTypeTag::Int | ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp) |
                    (ColumnTypeTag::Short, ColumnTypeTag::Int | ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp) |
                    (ColumnTypeTag::Int, ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp) |
                    // Integer/float widening
                    (ColumnTypeTag::Byte | ColumnTypeTag::Short | ColumnTypeTag::Int, ColumnTypeTag::Float | ColumnTypeTag::Double) |
                    (ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp, ColumnTypeTag::Float | ColumnTypeTag::Double) |
                    (ColumnTypeTag::Float, ColumnTypeTag::Double) |
                    // Integer narrowing
                    (ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp, ColumnTypeTag::Int | ColumnTypeTag::Short | ColumnTypeTag::Byte) |
                    (ColumnTypeTag::Int, ColumnTypeTag::Short | ColumnTypeTag::Byte) |
                    (ColumnTypeTag::Short, ColumnTypeTag::Byte) |
                    // Float/double narrowing to integer
                    (ColumnTypeTag::Double, ColumnTypeTag::Float | ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp | ColumnTypeTag::Int | ColumnTypeTag::Short | ColumnTypeTag::Byte) |
                    (ColumnTypeTag::Float, ColumnTypeTag::Int | ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp | ColumnTypeTag::Short | ColumnTypeTag::Byte) |
                    // Timestamp/Date ↔ Long (same i64 representation)
                    (ColumnTypeTag::Timestamp, ColumnTypeTag::Long) |
                    (ColumnTypeTag::Long, ColumnTypeTag::Timestamp) |
                    (ColumnTypeTag::Date, ColumnTypeTag::Long) |
                    (ColumnTypeTag::Long, ColumnTypeTag::Date) |
                    // Date ↔ Timestamp (needs post-decode scaling)
                    (ColumnTypeTag::Date, ColumnTypeTag::Timestamp) |
                    (ColumnTypeTag::Timestamp, ColumnTypeTag::Date) |
                    // Timestamp nano ↔ micro (needs post-decode scaling)
                    (ColumnTypeTag::Timestamp, ColumnTypeTag::Timestamp) |
                    // Fixed → Boolean: keep source type for decode so each
                    // value is decoded at full width; post_convert contracts
                    // to 1-byte booleans (non-zero → 1, zero → 0).
                    (ColumnTypeTag::Byte | ColumnTypeTag::Short | ColumnTypeTag::Int |
                     ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp |
                     ColumnTypeTag::Float | ColumnTypeTag::Double,
                     ColumnTypeTag::Boolean) => {
                        // Keep column_type as source type.
                    }
                    // Boolean → Int: decode as boolean, post-expand to i32
                    (ColumnTypeTag::Boolean, ColumnTypeTag::Int) => {
                        // Keep column_type as Boolean for decode; post-processing
                        // expands the 1-byte boolean values to 4-byte integers.
                    }
                    // Boolean → other fixed types: decode as boolean, post-expand
                    (ColumnTypeTag::Boolean,
                     ColumnTypeTag::Byte | ColumnTypeTag::Short | ColumnTypeTag::Long |
                     ColumnTypeTag::Float | ColumnTypeTag::Double |
                     ColumnTypeTag::Date | ColumnTypeTag::Timestamp) => {
                        // Keep column_type as Boolean for decode; post_convert expands.
                    }
                    // Fixed → var-size (VARCHAR, STRING): decode as source fixed type,
                    // post_convert produces the target var-size format.
                    (ColumnTypeTag::Byte | ColumnTypeTag::Short | ColumnTypeTag::Int |
                     ColumnTypeTag::Long | ColumnTypeTag::Float | ColumnTypeTag::Double |
                     ColumnTypeTag::Date | ColumnTypeTag::Timestamp |
                     ColumnTypeTag::Boolean | ColumnTypeTag::IPv4 | ColumnTypeTag::Uuid |
                     ColumnTypeTag::Char,
                     ColumnTypeTag::Varchar | ColumnTypeTag::String) => {
                        // Keep column_type as source; post_convert produces var-size output.
                    }
                    // Var → fixed-size: decode as source var type.
                    // Java handles batch conversion after decode.
                    (ColumnTypeTag::Varchar | ColumnTypeTag::String, dst)
                        if is_var_to_fixed_target(dst) =>
                    {
                        // Keep column_type as source var type; Java converts.
                    }
                    // String ↔ Varchar ↔ Symbol: parquet stores all three as
                    // BYTE_ARRAY (UTF-8), so we just remap to the target type
                    // and decode directly.
                    (ColumnTypeTag::String | ColumnTypeTag::Varchar | ColumnTypeTag::Symbol,
                     ColumnTypeTag::String | ColumnTypeTag::Varchar) => {
                        column_type = to_column_type;
                    }
                    // Array pass-through: same element type and dimensions.
                    (src_t, dst_t) if src_t == dst_t
                        && src_t == ColumnTypeTag::Array => {
                        column_type = to_column_type;
                    }
                    // Decimal → Decimal: different precision/scale.
                    // The decoder sign-extends or truncates to the target size.
                    (ColumnTypeTag::Decimal8 | ColumnTypeTag::Decimal16 |
                     ColumnTypeTag::Decimal32 | ColumnTypeTag::Decimal64 |
                     ColumnTypeTag::Decimal128 | ColumnTypeTag::Decimal256,
                     ColumnTypeTag::Decimal8 | ColumnTypeTag::Decimal16 |
                     ColumnTypeTag::Decimal32 | ColumnTypeTag::Decimal64 |
                     ColumnTypeTag::Decimal128 | ColumnTypeTag::Decimal256) => {
                        column_type = to_column_type;
                    }
                    // Fixed integer → Decimal: keep source type for decode,
                    // post_convert widens and scales by 10^(target_scale).
                    (ColumnTypeTag::Byte | ColumnTypeTag::Short |
                     ColumnTypeTag::Int | ColumnTypeTag::Long,
                     dst) if is_decimal_tag(dst) => {
                        // Keep column_type as source fixed type.
                    }
                    _ => {
                        return Err(fmt_err!(
                            InvalidType,
                            "requested column type {} does not match file column type {}, column index: {}",
                            to_column_type,
                            column_type,
                            column_idx
                        ));
                    }
                }
            }

            let column_chunk_bufs = &mut row_group_bufs.column_bufs[dest_col_idx];

            // Get the column's format from the "questdb" key-value metadata stored in the file.
            let (column_top, format, ascii) = self
                .qdb_meta
                .as_ref()
                .and_then(|m| m.schema.get(column_idx))
                .map(|c| (c.column_top, c.format, c.ascii))
                .unwrap_or((0, None, None));

            if column_top >= row_group_hi as usize + accumulated_size {
                column_chunk_bufs.reset();
                continue;
            }

            let col_info = QdbMetaCol { column_type, column_top, format, ascii };
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

            // Post-decode conversions that cannot be handled by the decode dispatch.
            post_convert(original_column_type, to_column_type, column_chunk_bufs)?;

            // Timestamp nano additional scaling after post_convert.
            // post_convert handles μs↔ms (×/÷1000) for Timestamp↔Date.
            // When the Timestamp side is nano, an additional ×/÷1000 is needed
            // (TIMESTAMP_NANO↔TIMESTAMP or TIMESTAMP_NANO↔DATE).
            if original_column_type != to_column_type {
                let src_is_time = matches!(src_tag, ColumnTypeTag::Timestamp | ColumnTypeTag::Date);
                let dst_tag = to_column_type.tag();
                let dst_is_time = matches!(dst_tag, ColumnTypeTag::Timestamp | ColumnTypeTag::Date);
                if src_is_time && dst_is_time {
                    let src_nano = src_tag == ColumnTypeTag::Timestamp
                        && original_column_type.has_flag(QDB_TIMESTAMP_NS_COLUMN_TYPE_FLAG);
                    let dst_nano = dst_tag == ColumnTypeTag::Timestamp
                        && to_column_type.has_flag(QDB_TIMESTAMP_NS_COLUMN_TYPE_FLAG);
                    if src_nano != dst_nano {
                        scale_i64_in_place(&mut column_chunk_bufs.data_vec, 1000, src_nano);
                    }
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
                && (to_column_type.tag() == ColumnTypeTag::Varchar
                    || to_column_type.tag() == ColumnTypeTag::VarcharSlice
                    || to_column_type.tag() == ColumnTypeTag::String)
            {
                column_type = to_column_type;
            }

            // Allow requesting VarcharSlice when the file stores Varchar or String.
            // VarcharSlice is a zero-copy decode format; parquet stores both as UTF-8.
            if (column_type.tag() == ColumnTypeTag::Varchar
                || column_type.tag() == ColumnTypeTag::String)
                && to_column_type.tag() == ColumnTypeTag::VarcharSlice
            {
                column_type = to_column_type;
            }

            let original_column_type = column_type;
            let src_tag = column_type.tag();
            if column_type != to_column_type {
                match (src_tag, to_column_type.tag()) {
                    // Integer widening (Timestamp/Date share i64 representation with Long)
                    (ColumnTypeTag::Byte, ColumnTypeTag::Short | ColumnTypeTag::Int | ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp) |
                    (ColumnTypeTag::Short, ColumnTypeTag::Int | ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp) |
                    (ColumnTypeTag::Int, ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp) |
                    // Integer/float widening
                    (ColumnTypeTag::Byte | ColumnTypeTag::Short | ColumnTypeTag::Int, ColumnTypeTag::Float | ColumnTypeTag::Double) |
                    (ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp, ColumnTypeTag::Float | ColumnTypeTag::Double) |
                    (ColumnTypeTag::Float, ColumnTypeTag::Double) |
                    // Integer narrowing
                    (ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp, ColumnTypeTag::Int | ColumnTypeTag::Short | ColumnTypeTag::Byte) |
                    (ColumnTypeTag::Int, ColumnTypeTag::Short | ColumnTypeTag::Byte) |
                    (ColumnTypeTag::Short, ColumnTypeTag::Byte) |
                    // Float/double narrowing to integer
                    (ColumnTypeTag::Double, ColumnTypeTag::Float | ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp | ColumnTypeTag::Int | ColumnTypeTag::Short | ColumnTypeTag::Byte) |
                    (ColumnTypeTag::Float, ColumnTypeTag::Int | ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp | ColumnTypeTag::Short | ColumnTypeTag::Byte) |
                    // Timestamp/Date ↔ Long (same i64 representation)
                    (ColumnTypeTag::Timestamp, ColumnTypeTag::Long) |
                    (ColumnTypeTag::Long, ColumnTypeTag::Timestamp) |
                    (ColumnTypeTag::Date, ColumnTypeTag::Long) |
                    (ColumnTypeTag::Long, ColumnTypeTag::Date) |
                    // Date ↔ Timestamp (needs post-decode scaling)
                    (ColumnTypeTag::Date, ColumnTypeTag::Timestamp) |
                    (ColumnTypeTag::Timestamp, ColumnTypeTag::Date) |
                    // Timestamp nano ↔ micro (needs post-decode scaling)
                    (ColumnTypeTag::Timestamp, ColumnTypeTag::Timestamp) |
                    // Fixed → Boolean: keep source type for decode;
                    // post_convert contracts to 1-byte booleans.
                    (ColumnTypeTag::Byte | ColumnTypeTag::Short | ColumnTypeTag::Int |
                     ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp |
                     ColumnTypeTag::Float | ColumnTypeTag::Double,
                     ColumnTypeTag::Boolean) => {
                        // Keep column_type as source type.
                    }
                    // Boolean → Int: decode as boolean, post-expand to i32
                    (ColumnTypeTag::Boolean, ColumnTypeTag::Int) => {
                        // Keep column_type as Boolean for decode; post-processing
                        // expands the 1-byte boolean values to 4-byte integers.
                    }
                    // Boolean → other fixed types: decode as boolean, post-expand
                    (ColumnTypeTag::Boolean,
                     ColumnTypeTag::Byte | ColumnTypeTag::Short | ColumnTypeTag::Long |
                     ColumnTypeTag::Float | ColumnTypeTag::Double |
                     ColumnTypeTag::Date | ColumnTypeTag::Timestamp) => {
                        // Keep column_type as Boolean for decode; post_convert expands.
                    }
                    // Fixed → var-size (VARCHAR, STRING): decode as source fixed type,
                    // post_convert produces the target var-size format.
                    (ColumnTypeTag::Byte | ColumnTypeTag::Short | ColumnTypeTag::Int |
                     ColumnTypeTag::Long | ColumnTypeTag::Float | ColumnTypeTag::Double |
                     ColumnTypeTag::Date | ColumnTypeTag::Timestamp |
                     ColumnTypeTag::Boolean | ColumnTypeTag::IPv4 | ColumnTypeTag::Uuid |
                     ColumnTypeTag::Char,
                     ColumnTypeTag::Varchar | ColumnTypeTag::String) => {
                        // Keep column_type as source; post_convert produces var-size output.
                    }
                    // Var → fixed-size: decode as source var type.
                    // Java handles batch conversion after decode.
                    (ColumnTypeTag::Varchar | ColumnTypeTag::String, dst)
                        if is_var_to_fixed_target(dst) =>
                    {
                        // Keep column_type as source var type; Java converts.
                    }
                    // String ↔ Varchar ↔ Symbol: parquet stores all three as
                    // BYTE_ARRAY (UTF-8), so we just remap to the target type
                    // and decode directly.
                    (ColumnTypeTag::String | ColumnTypeTag::Varchar | ColumnTypeTag::Symbol,
                     ColumnTypeTag::String | ColumnTypeTag::Varchar) => {
                        column_type = to_column_type;
                    }
                    // Array pass-through: same element type and dimensions.
                    (src_t, dst_t) if src_t == dst_t
                        && src_t == ColumnTypeTag::Array => {
                        column_type = to_column_type;
                    }
                    // Decimal → Decimal: different precision/scale.
                    // The decoder sign-extends or truncates to the target size.
                    (ColumnTypeTag::Decimal8 | ColumnTypeTag::Decimal16 |
                     ColumnTypeTag::Decimal32 | ColumnTypeTag::Decimal64 |
                     ColumnTypeTag::Decimal128 | ColumnTypeTag::Decimal256,
                     ColumnTypeTag::Decimal8 | ColumnTypeTag::Decimal16 |
                     ColumnTypeTag::Decimal32 | ColumnTypeTag::Decimal64 |
                     ColumnTypeTag::Decimal128 | ColumnTypeTag::Decimal256) => {
                        column_type = to_column_type;
                    }
                    // Fixed integer → Decimal: keep source type for decode,
                    // post_convert widens and scales by 10^(target_scale).
                    (ColumnTypeTag::Byte | ColumnTypeTag::Short |
                     ColumnTypeTag::Int | ColumnTypeTag::Long,
                     dst) if is_decimal_tag(dst) => {
                        // Keep column_type as source fixed type.
                    }
                    _ => {
                        return Err(fmt_err!(
                            InvalidType,
                            "requested column type {} does not match file column type {}, column index: {}",
                            to_column_type,
                            column_type,
                            column_idx
                        ));
                    }
                }
            }

            let column_chunk_bufs = &mut row_group_bufs.column_bufs[dest_col_idx];

            // Get the column's format from the "questdb" key-value metadata stored in the file.
            let (column_top, format, ascii) = self
                .qdb_meta
                .as_ref()
                .and_then(|m| m.schema.get(column_idx))
                .map(|c| (c.column_top, c.format, c.ascii))
                .unwrap_or((0, None, None));

            if column_top >= row_group_hi as usize + accumulated_size {
                column_chunk_bufs.reset();
                continue;
            }

            let col_info = QdbMetaCol { column_type, column_top, format, ascii };

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

            // Post-decode conversions that cannot be handled by the decode dispatch.
            post_convert(original_column_type, to_column_type, column_chunk_bufs)?;

            // Timestamp nano additional scaling after post_convert.
            // post_convert handles μs↔ms (×/÷1000) for Timestamp↔Date.
            // When the Timestamp side is nano, an additional ×/÷1000 is needed
            // (TIMESTAMP_NANO↔TIMESTAMP or TIMESTAMP_NANO↔DATE).
            if original_column_type != to_column_type {
                let src_is_time = matches!(src_tag, ColumnTypeTag::Timestamp | ColumnTypeTag::Date);
                let dst_tag = to_column_type.tag();
                let dst_is_time = matches!(dst_tag, ColumnTypeTag::Timestamp | ColumnTypeTag::Date);
                if src_is_time && dst_is_time {
                    let src_nano = src_tag == ColumnTypeTag::Timestamp
                        && original_column_type.has_flag(QDB_TIMESTAMP_NS_COLUMN_TYPE_FLAG);
                    let dst_nano = dst_tag == ColumnTypeTag::Timestamp
                        && to_column_type.has_flag(QDB_TIMESTAMP_NS_COLUMN_TYPE_FLAG);
                    if src_nano != dst_nano {
                        scale_i64_in_place(&mut column_chunk_bufs.data_vec, 1000, src_nano);
                    }
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

        let is_varchar_slice = col_info.column_type.tag() == ColumnTypeTag::VarcharSlice;

        let DecodeContext {
            decompress_buffer,
            dict_decompress_buffer,
            varchar_slice_buf_pool,
            varchar_slice_page_bufs_scratch: varchar_slice_page_bufs,
            varchar_slice_dict_bufs_scratch: varchar_slice_dict_bufs,
            ..
        } = ctx;

        varchar_slice_buf_pool.append(&mut column_chunk_bufs.page_buffers);
        column_chunk_bufs.reset();

        // Reuse the hoisted scratch outer-vecs across calls so we don't pay an outer
        // allocation per column chunk. Clear at the top in case a prior call returned
        // early (the normal end-of-chunk path drains both via append).
        varchar_slice_page_bufs.clear();
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
                            let page = if is_varchar_slice {
                                decompress_varchar_slice_data(
                                    &page,
                                    decompress_buffer,
                                    varchar_slice_page_bufs,
                                    varchar_slice_buf_pool,
                                )?
                            } else {
                                decompress_sliced_data(&page, decompress_buffer)?
                            };
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
                            let page = if is_varchar_slice {
                                decompress_varchar_slice_data(
                                    &page,
                                    decompress_buffer,
                                    varchar_slice_page_bufs,
                                    varchar_slice_buf_pool,
                                )?
                            } else {
                                decompress_sliced_data(&page, decompress_buffer)?
                            };
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

                        let page = if is_varchar_slice {
                            decompress_varchar_slice_data(
                                &page,
                                decompress_buffer,
                                varchar_slice_page_bufs,
                                varchar_slice_buf_pool,
                            )?
                        } else {
                            decompress_sliced_data(&page, decompress_buffer)?
                        };
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

        if is_varchar_slice {
            if !column_chunk_bufs.data_vec.is_empty() {
                fixup_varchar_slice_spill_pointers(column_chunk_bufs);
            }
            // Move dict buffers in too — aux entries from RleDictVarcharSliceDecoder hold raw
            // pointers into them and require them to outlive the column chunk decode.
            varchar_slice_page_bufs.append(varchar_slice_dict_bufs);
            // Drain into the destination instead of replacing it: this preserves the hoisted
            // outer-vec capacity in the scratch field for the next column chunk.
            column_chunk_bufs
                .page_buffers
                .append(varchar_slice_page_bufs);
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

        let is_varchar_slice = col_info.column_type.tag() == ColumnTypeTag::VarcharSlice;

        let DecodeContext {
            decompress_buffer,
            dict_decompress_buffer,
            varchar_slice_buf_pool,
            varchar_slice_page_bufs_scratch: varchar_slice_page_bufs,
            varchar_slice_dict_bufs_scratch: varchar_slice_dict_bufs,
            ..
        } = ctx;

        varchar_slice_buf_pool.append(&mut column_chunk_bufs.page_buffers);
        column_chunk_bufs.reset();

        // Reuse the hoisted scratch outer-vecs across calls so we don't pay an outer
        // allocation per column chunk. Clear at the top in case a prior call returned
        // early (the normal end-of-chunk path drains both via append).
        varchar_slice_page_bufs.clear();
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
                        if row_group_lo < row_count + page_row_count && row_group_hi > row_count {
                            let page = if is_varchar_slice {
                                decompress_varchar_slice_data(
                                    &page,
                                    decompress_buffer,
                                    varchar_slice_page_bufs,
                                    varchar_slice_buf_pool,
                                )?
                            } else {
                                decompress_sliced_data(&page, decompress_buffer)?
                            };
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
                        let page = if is_varchar_slice {
                            decompress_varchar_slice_data(
                                &page,
                                decompress_buffer,
                                varchar_slice_page_bufs,
                                varchar_slice_buf_pool,
                            )?
                        } else {
                            decompress_sliced_data(&page, decompress_buffer)?
                        };
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

        if is_varchar_slice {
            if !column_chunk_bufs.data_vec.is_empty() {
                fixup_varchar_slice_spill_pointers(column_chunk_bufs);
            }
            // Move dict buffers in too — aux entries from RleDictVarcharSliceDecoder hold raw
            // pointers into them and require them to outlive the column chunk decode.
            varchar_slice_page_bufs.append(varchar_slice_dict_bufs);
            // Drain into the destination instead of replacing it: this preserves the hoisted
            // outer-vec capacity in the scratch field for the next column chunk.
            column_chunk_bufs
                .page_buffers
                .append(varchar_slice_page_bufs);
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
            // Allow Varchar/String->VarcharSlice and Symbol->Varchar/VarcharSlice remapping.
            // Parquet stores String, Varchar and Symbol all as UTF-8 BYTE_ARRAY.
            let types_match = column_type == to_column_type
                || ((column_type.tag() == ColumnTypeTag::Varchar
                    || column_type.tag() == ColumnTypeTag::String)
                    && to_column_type.tag() == ColumnTypeTag::VarcharSlice)
                || (column_type.tag() == ColumnTypeTag::Symbol
                    && (to_column_type.tag() == ColumnTypeTag::Varchar
                        || to_column_type.tag() == ColumnTypeTag::VarcharSlice
                        || to_column_type.tag() == ColumnTypeTag::String));
            if !types_match {
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
        filter_buf_end: u64,
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

            let filter_desc = ColumnFilterValues {
                count,
                ptr: packed_filter.ptr,
                buf_end: filter_buf_end,
            };
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
                        )?;
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
                        )?
                    {
                        return Ok(true);
                    }
                }
                FILTER_OP_LT | FILTER_OP_LE | FILTER_OP_GT | FILTER_OP_GE | FILTER_OP_BETWEEN => {
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
                        )?
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

    #[inline]
    fn validate_filter_span(
        ptr: *const u8,
        count: usize,
        element_size: usize,
        buf_end: u64,
    ) -> ParquetResult<()> {
        let bytes = count
            .checked_mul(element_size)
            .ok_or_else(|| fmt_err!(InvalidLayout, "filter values buffer out of bounds"))?;
        let base = ptr as usize;
        let limit = base
            .checked_add(bytes)
            .ok_or_else(|| fmt_err!(InvalidLayout, "filter values buffer out of bounds"))?;
        if limit > buf_end as usize {
            return Err(fmt_err!(
                InvalidLayout,
                "filter values buffer out of bounds"
            ));
        }
        Ok(())
    }

    #[inline]
    fn millis_to_parquet_day(millis: i64) -> i32 {
        millis.div_euclid(MILLIS_PER_DAY) as i32
    }

    fn all_values_absent_from_bloom(
        bitset: &[u8],
        physical_type: &PhysicalType,
        filter_desc: &ColumnFilterValues,
        has_nulls: bool,
        is_decimal: bool,
        qdb_column_type: i32,
    ) -> ParquetResult<bool> {
        let count = filter_desc.count as usize;
        if count == 0 {
            return Ok(false);
        }

        let ptr = filter_desc.ptr as *const u8;
        match physical_type {
            PhysicalType::Int32 => {
                let col_type_tag = qdb_column_type & 0xFF;
                let is_ipv4 = col_type_tag == ColumnTypeTag::IPv4 as i32;
                let is_date = col_type_tag == ColumnTypeTag::Date as i32;
                if is_date {
                    Self::validate_filter_span(ptr, count, size_of::<i64>(), filter_desc.buf_end)?;
                    for i in 0..count {
                        let millis = unsafe { (ptr as *const i64).add(i).read_unaligned() };
                        if millis == i64::MIN {
                            if has_nulls {
                                return Ok(false);
                            }
                        } else {
                            let days = Self::millis_to_parquet_day(millis);
                            if parquet2::bloom_filter::is_in_set(
                                bitset,
                                parquet2::bloom_filter::hash_native(days),
                            ) {
                                return Ok(false);
                            }
                        }
                    }
                    Ok(true)
                } else {
                    Self::validate_filter_span(ptr, count, size_of::<i32>(), filter_desc.buf_end)?;
                    for i in 0..count {
                        let v = unsafe { (ptr as *const i32).add(i).read_unaligned() };
                        let is_null = if is_ipv4 { v == 0 } else { v == i32::MIN };
                        if is_null {
                            if has_nulls {
                                return Ok(false);
                            }
                        } else if parquet2::bloom_filter::is_in_set(
                            bitset,
                            parquet2::bloom_filter::hash_native(v),
                        ) {
                            return Ok(false);
                        }
                    }
                    Ok(true)
                }
            }
            PhysicalType::Int64 => {
                Self::validate_filter_span(ptr, count, size_of::<i64>(), filter_desc.buf_end)?;
                for i in 0..count {
                    let v = unsafe { (ptr as *const i64).add(i).read_unaligned() };
                    if v == i64::MIN {
                        if has_nulls {
                            return Ok(false);
                        }
                    } else if parquet2::bloom_filter::is_in_set(
                        bitset,
                        parquet2::bloom_filter::hash_native(v),
                    ) {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            PhysicalType::Float => {
                Self::validate_filter_span(ptr, count, size_of::<f32>(), filter_desc.buf_end)?;
                for i in 0..count {
                    let v = unsafe { (ptr as *const f32).add(i).read_unaligned() };
                    if v.is_nan() {
                        if has_nulls {
                            return Ok(false);
                        }
                    } else {
                        // Canonicalize -0.0 to +0.0
                        let normalized = if v == 0.0 { 0.0f32 } else { v };
                        if parquet2::bloom_filter::is_in_set(
                            bitset,
                            parquet2::bloom_filter::hash_native(normalized),
                        ) {
                            return Ok(false);
                        }
                    }
                }
                Ok(true)
            }
            PhysicalType::Double => {
                Self::validate_filter_span(ptr, count, size_of::<f64>(), filter_desc.buf_end)?;
                for i in 0..count {
                    let v = unsafe { (ptr as *const f64).add(i).read_unaligned() };
                    if v.is_nan() {
                        if has_nulls {
                            return Ok(false);
                        }
                    } else {
                        // Canonicalize -0.0 to +0.0
                        let normalized = if v == 0.0 { 0.0f64 } else { v };
                        if parquet2::bloom_filter::is_in_set(
                            bitset,
                            parquet2::bloom_filter::hash_native(normalized),
                        ) {
                            return Ok(false);
                        }
                    }
                }
                Ok(true)
            }
            PhysicalType::ByteArray => {
                let buf_end = filter_desc.buf_end as usize;
                let base = ptr as usize;
                let mut offset = 0usize;
                for _ in 0..count {
                    let end = base
                        .checked_add(offset)
                        .and_then(|v| v.checked_add(size_of::<i32>()));
                    if end.is_none_or(|e| e > buf_end) {
                        return Err(fmt_err!(
                            InvalidLayout,
                            "filter values buffer out of bounds"
                        ));
                    }
                    let len = unsafe { (ptr.add(offset) as *const i32).read_unaligned() };
                    offset += size_of::<i32>();
                    if len < 0 {
                        if has_nulls {
                            return Ok(false);
                        }
                    } else {
                        let len = len as usize;
                        let end = base.checked_add(offset).and_then(|v| v.checked_add(len));
                        if end.is_none_or(|e| e > buf_end) {
                            return Err(fmt_err!(
                                InvalidLayout,
                                "filter values buffer out of bounds"
                            ));
                        }
                        let bytes = unsafe { slice::from_raw_parts(ptr.add(offset), len) };
                        offset += len;
                        if parquet2::bloom_filter::is_in_set(
                            bitset,
                            parquet2::bloom_filter::hash_byte(bytes),
                        ) {
                            return Ok(false);
                        }
                    }
                }
                Ok(true)
            }
            PhysicalType::FixedLenByteArray(size) => {
                let size = *size;
                if size == 0 {
                    return Ok(false);
                }
                Self::validate_filter_span(ptr, count, size, filter_desc.buf_end)?;
                let null_check = if is_decimal {
                    is_fixed_len_null_be
                } else {
                    is_fixed_len_null
                };
                for i in 0..count {
                    let bytes = unsafe { slice::from_raw_parts(ptr.add(i * size), size) };
                    if null_check(bytes) {
                        if has_nulls {
                            return Ok(false);
                        }
                    } else if parquet2::bloom_filter::is_in_set(
                        bitset,
                        parquet2::bloom_filter::hash_byte(bytes),
                    ) {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            _ => Ok(false),
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
    ) -> ParquetResult<bool> {
        let count = filter_desc.count as usize;
        if count == 0 {
            return Ok(false);
        }

        let ptr = filter_desc.ptr as *const u8;
        match physical_type {
            PhysicalType::Int32 if is_ipv4 => {
                Self::validate_filter_span(ptr, count, size_of::<u32>(), filter_desc.buf_end)?;
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
                            return Ok(false);
                        }
                    } else if let Some((min_val, max_val)) = min_max {
                        if v >= min_val && v <= max_val {
                            return Ok(false);
                        }
                    } else {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            // Signed Int32 (Byte, Short, Char, Int, Date).
            // DATE: filter values are i64 millis, converted to i32 days.
            // Others: filter values are i32, NULL = i32::MIN.
            PhysicalType::Int32 => {
                let elem_size = if is_date {
                    size_of::<i64>()
                } else {
                    size_of::<i32>()
                };
                Self::validate_filter_span(ptr, count, elem_size, filter_desc.buf_end)?;
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
                            (Self::millis_to_parquet_day(millis), false)
                        }
                    } else {
                        let v = unsafe { (ptr as *const i32).add(i).read_unaligned() };
                        (v, v == i32::MIN)
                    };
                    if is_null {
                        if has_nulls {
                            return Ok(false);
                        }
                    } else if let Some((min_val, max_val)) = min_max {
                        if v >= min_val && v <= max_val {
                            return Ok(false);
                        }
                    } else {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            // Signed Int64 (Long, Timestamp, Date): NULL = i64::MIN
            PhysicalType::Int64 => {
                Self::validate_filter_span(ptr, count, size_of::<i64>(), filter_desc.buf_end)?;
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
                            return Ok(false);
                        }
                    } else if let Some((min_val, max_val)) = min_max {
                        if v >= min_val && v <= max_val {
                            return Ok(false);
                        }
                    } else {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            PhysicalType::Float => {
                Self::validate_filter_span(ptr, count, size_of::<f32>(), filter_desc.buf_end)?;
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
                            return Ok(false);
                        }
                    } else if let Some((min_val, max_val)) = min_max {
                        if v >= min_val && v <= max_val {
                            return Ok(false);
                        }
                    } else {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            PhysicalType::Double => {
                Self::validate_filter_span(ptr, count, size_of::<f64>(), filter_desc.buf_end)?;
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
                            return Ok(false);
                        }
                    } else if let Some((min_val, max_val)) = min_max {
                        if v >= min_val && v <= max_val {
                            return Ok(false);
                        }
                    } else {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            PhysicalType::ByteArray => {
                let (min_b, max_b) = match (min_bytes, max_bytes) {
                    (Some(min_b), Some(max_b)) => (min_b, max_b),
                    _ => return Ok(false),
                };
                let buf_end = filter_desc.buf_end as usize;
                let base = ptr as usize;
                let mut offset = 0usize;
                for _ in 0..count {
                    let end = base
                        .checked_add(offset)
                        .and_then(|v| v.checked_add(size_of::<i32>()));
                    if end.is_none_or(|e| e > buf_end) {
                        return Err(fmt_err!(
                            InvalidLayout,
                            "filter values buffer out of bounds"
                        ));
                    }
                    let len = unsafe { (ptr.add(offset) as *const i32).read_unaligned() };
                    offset += size_of::<i32>();
                    if len < 0 {
                        if has_nulls {
                            return Ok(false);
                        }
                    } else {
                        let len = len as usize;
                        let end = base.checked_add(offset).and_then(|v| v.checked_add(len));
                        if end.is_none_or(|e| e > buf_end) {
                            return Err(fmt_err!(
                                InvalidLayout,
                                "filter values buffer out of bounds"
                            ));
                        }
                        let bytes = unsafe { slice::from_raw_parts(ptr.add(offset), len) };
                        offset += len;
                        let in_range = if is_decimal {
                            compare_signed_be_varlen(bytes, min_b) != cmp::Ordering::Less
                                && compare_signed_be_varlen(bytes, max_b) != cmp::Ordering::Greater
                        } else {
                            bytes >= min_b && bytes <= max_b
                        };
                        if in_range {
                            return Ok(false);
                        }
                    }
                }
                Ok(true)
            }
            PhysicalType::FixedLenByteArray(size) => {
                let size = *size;
                if size == 0 {
                    return Ok(false);
                }
                Self::validate_filter_span(ptr, count, size, filter_desc.buf_end)?;
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
                            return Ok(false);
                        }
                    } else if let Some((min_b, max_b)) = min_max {
                        if is_decimal {
                            if compare_signed_be(bytes, min_b) != cmp::Ordering::Less
                                && compare_signed_be(bytes, max_b) != cmp::Ordering::Greater
                            {
                                return Ok(false);
                            }
                        } else if bytes >= min_b && bytes <= max_b {
                            return Ok(false);
                        }
                    } else {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// Check if a range/between filter proves the row group can be skipped.
    ///
    /// For LT/LE/GT/GE (count=1):
    ///   LT: skip if min >= val, LE: skip if min > val,
    ///   GT: skip if max <= val, GE: skip if max < val.
    ///
    /// For BETWEEN (count=2): auto-swaps bounds, so we compute
    ///   lo=min(a,b), hi=max(a,b) and skip if max_stat < lo || min_stat > hi.
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
    ) -> ParquetResult<bool> {
        let count = filter_desc.count as usize;
        let is_between = op == FILTER_OP_BETWEEN;
        if is_between {
            if count != 2 {
                return Ok(false);
            }
        } else if count != 1 {
            return Ok(false);
        }
        let ptr = filter_desc.ptr as *const u8;

        match physical_type {
            PhysicalType::Int32 if is_ipv4 => {
                Self::validate_filter_span(ptr, count, size_of::<u32>(), filter_desc.buf_end)?;
                let (min_val, max_val) = match (min_bytes, max_bytes) {
                    (Some(min_b), Some(max_b)) if min_b.len() == 4 && max_b.len() == 4 => (
                        u32::from_le_bytes(min_b.try_into().unwrap()),
                        u32::from_le_bytes(max_b.try_into().unwrap()),
                    ),
                    _ => return Ok(false),
                };
                if is_between {
                    let a = unsafe { (ptr as *const u32).read_unaligned() };
                    let b = unsafe { (ptr.add(4) as *const u32).read_unaligned() };
                    if a == 0 || b == 0 {
                        return Ok(false);
                    }
                    Ok(max_val < a.min(b) || min_val > a.max(b))
                } else {
                    let v = unsafe { (ptr as *const u32).read_unaligned() };
                    if v == 0 {
                        return Ok(false);
                    }
                    Ok(match op {
                        FILTER_OP_LT => min_val >= v,
                        FILTER_OP_LE => min_val > v,
                        FILTER_OP_GT => max_val <= v,
                        FILTER_OP_GE => max_val < v,
                        _ => false,
                    })
                }
            }
            // Signed Int32 (Byte, Short, Char, Int, Date).
            // DATE: filter value is i64 millis, converted to i32 days.
            // Others: filter value is i32, NULL = i32::MIN.
            PhysicalType::Int32 => {
                let elem_size = if is_date {
                    size_of::<i64>()
                } else {
                    size_of::<i32>()
                };
                Self::validate_filter_span(ptr, count, elem_size, filter_desc.buf_end)?;
                let (min_val, max_val) = match (min_bytes, max_bytes) {
                    (Some(min_b), Some(max_b)) if min_b.len() == 4 && max_b.len() == 4 => (
                        i32::from_le_bytes(min_b.try_into().unwrap()),
                        i32::from_le_bytes(max_b.try_into().unwrap()),
                    ),
                    _ => return Ok(false),
                };
                if is_date {
                    let min_ms = min_val as i64 * MILLIS_PER_DAY;
                    let max_ms = max_val as i64 * MILLIS_PER_DAY;
                    if is_between {
                        let a_ms = unsafe { (ptr as *const i64).read_unaligned() };
                        let b_ms = unsafe { (ptr.add(8) as *const i64).read_unaligned() };
                        if a_ms == i64::MIN || b_ms == i64::MIN {
                            return Ok(false);
                        }
                        Ok(max_ms < a_ms.min(b_ms) || min_ms > a_ms.max(b_ms))
                    } else {
                        let millis = unsafe { (ptr as *const i64).read_unaligned() };
                        if millis == i64::MIN {
                            return Ok(false);
                        }
                        Ok(match op {
                            FILTER_OP_LT => min_ms >= millis,
                            FILTER_OP_LE => min_ms > millis,
                            FILTER_OP_GT => max_ms <= millis,
                            FILTER_OP_GE => max_ms < millis,
                            _ => false,
                        })
                    }
                } else if is_between {
                    let a = unsafe { (ptr as *const i32).read_unaligned() };
                    let b = unsafe { (ptr.add(4) as *const i32).read_unaligned() };
                    if a == i32::MIN || b == i32::MIN {
                        return Ok(false);
                    }
                    Ok(max_val < a.min(b) || min_val > a.max(b))
                } else {
                    let v = unsafe { (ptr as *const i32).read_unaligned() };
                    if v == i32::MIN {
                        return Ok(false);
                    }
                    Ok(match op {
                        FILTER_OP_LT => min_val >= v,
                        FILTER_OP_LE => min_val > v,
                        FILTER_OP_GT => max_val <= v,
                        FILTER_OP_GE => max_val < v,
                        _ => false,
                    })
                }
            }
            // Signed Int64 (Long, Timestamp, Date): NULL = i64::MIN
            PhysicalType::Int64 => {
                Self::validate_filter_span(ptr, count, size_of::<i64>(), filter_desc.buf_end)?;
                let (min_val, max_val) = match (min_bytes, max_bytes) {
                    (Some(min_b), Some(max_b)) if min_b.len() == 8 && max_b.len() == 8 => (
                        i64::from_le_bytes(min_b.try_into().unwrap()),
                        i64::from_le_bytes(max_b.try_into().unwrap()),
                    ),
                    _ => return Ok(false),
                };
                if is_between {
                    let a = unsafe { (ptr as *const i64).read_unaligned() };
                    let b = unsafe { (ptr.add(8) as *const i64).read_unaligned() };
                    if a == i64::MIN || b == i64::MIN {
                        return Ok(false);
                    }
                    Ok(max_val < a.min(b) || min_val > a.max(b))
                } else {
                    let v = unsafe { (ptr as *const i64).read_unaligned() };
                    if v == i64::MIN {
                        return Ok(false);
                    }
                    Ok(match op {
                        FILTER_OP_LT => min_val >= v,
                        FILTER_OP_LE => min_val > v,
                        FILTER_OP_GT => max_val <= v,
                        FILTER_OP_GE => max_val < v,
                        _ => false,
                    })
                }
            }
            PhysicalType::Float => {
                Self::validate_filter_span(ptr, count, size_of::<f32>(), filter_desc.buf_end)?;
                let (min_val, max_val) = match (min_bytes, max_bytes) {
                    (Some(min_b), Some(max_b)) if min_b.len() == 4 && max_b.len() == 4 => {
                        let min_val = f32::from_le_bytes(min_b.try_into().unwrap());
                        let max_val = f32::from_le_bytes(max_b.try_into().unwrap());
                        if min_val.is_nan() || max_val.is_nan() {
                            return Ok(false);
                        }
                        (min_val, max_val)
                    }
                    _ => return Ok(false),
                };
                if is_between {
                    let a = unsafe { (ptr as *const f32).read_unaligned() };
                    let b = unsafe { (ptr.add(4) as *const f32).read_unaligned() };
                    if a.is_nan() || b.is_nan() {
                        return Ok(false);
                    }
                    Ok(max_val < a.min(b) || min_val > a.max(b))
                } else {
                    let v = unsafe { (ptr as *const f32).read_unaligned() };
                    if v.is_nan() {
                        return Ok(false);
                    }
                    Ok(match op {
                        FILTER_OP_LT => min_val >= v,
                        FILTER_OP_LE => min_val > v,
                        FILTER_OP_GT => max_val <= v,
                        FILTER_OP_GE => max_val < v,
                        _ => false,
                    })
                }
            }
            PhysicalType::Double => {
                Self::validate_filter_span(ptr, count, size_of::<f64>(), filter_desc.buf_end)?;
                let (min_val, max_val) = match (min_bytes, max_bytes) {
                    (Some(min_b), Some(max_b)) if min_b.len() == 8 && max_b.len() == 8 => {
                        let min_val = f64::from_le_bytes(min_b.try_into().unwrap());
                        let max_val = f64::from_le_bytes(max_b.try_into().unwrap());
                        if min_val.is_nan() || max_val.is_nan() {
                            return Ok(false);
                        }
                        (min_val, max_val)
                    }
                    _ => return Ok(false),
                };
                if is_between {
                    let a = unsafe { (ptr as *const f64).read_unaligned() };
                    let b = unsafe { (ptr.add(8) as *const f64).read_unaligned() };
                    if a.is_nan() || b.is_nan() {
                        return Ok(false);
                    }
                    Ok(max_val < a.min(b) || min_val > a.max(b))
                } else {
                    let v = unsafe { (ptr as *const f64).read_unaligned() };
                    if v.is_nan() {
                        return Ok(false);
                    }
                    Ok(match op {
                        FILTER_OP_LT => min_val >= v,
                        FILTER_OP_LE => min_val > v,
                        FILTER_OP_GT => max_val <= v,
                        FILTER_OP_GE => max_val < v,
                        _ => false,
                    })
                }
            }
            PhysicalType::ByteArray => {
                let (min_b, max_b) = match (min_bytes, max_bytes) {
                    (Some(min_b), Some(max_b)) => (min_b, max_b),
                    _ => return Ok(false),
                };
                let buf_end = filter_desc.buf_end as usize;
                let base = ptr as usize;
                let end = base.checked_add(size_of::<i32>());
                if end.is_none_or(|e| e > buf_end) {
                    return Err(fmt_err!(
                        InvalidLayout,
                        "filter values buffer out of bounds"
                    ));
                }
                let len = unsafe { (ptr as *const i32).read_unaligned() };
                if len < 0 {
                    return Ok(false);
                }
                let len = len as usize;
                let end = base
                    .checked_add(size_of::<i32>())
                    .and_then(|v| v.checked_add(len));
                if end.is_none_or(|e| e > buf_end) {
                    return Err(fmt_err!(
                        InvalidLayout,
                        "filter values buffer out of bounds"
                    ));
                }
                let bytes1 = unsafe { slice::from_raw_parts(ptr.add(size_of::<i32>()), len) };

                if is_between {
                    let ptr2 = unsafe { ptr.add(size_of::<i32>() + len) };
                    let base2 = ptr2 as usize;
                    let end = base2.checked_add(size_of::<i32>());
                    if end.is_none_or(|e| e > buf_end) {
                        return Err(fmt_err!(
                            InvalidLayout,
                            "filter values buffer out of bounds"
                        ));
                    }
                    let len2 = unsafe { (ptr2 as *const i32).read_unaligned() };
                    if len2 < 0 {
                        return Ok(false);
                    }
                    let len2 = len2 as usize;
                    let end = base2
                        .checked_add(size_of::<i32>())
                        .and_then(|v| v.checked_add(len2));
                    if end.is_none_or(|e| e > buf_end) {
                        return Err(fmt_err!(
                            InvalidLayout,
                            "filter values buffer out of bounds"
                        ));
                    }
                    let bytes2 = unsafe { slice::from_raw_parts(ptr2.add(size_of::<i32>()), len2) };

                    Ok(if is_decimal {
                        let ord = compare_signed_be_varlen(bytes1, bytes2);
                        let (lo, hi) = if ord == cmp::Ordering::Greater {
                            (bytes2, bytes1)
                        } else {
                            (bytes1, bytes2)
                        };
                        compare_signed_be_varlen(max_b, lo) == cmp::Ordering::Less
                            || compare_signed_be_varlen(min_b, hi) == cmp::Ordering::Greater
                    } else {
                        let (lo, hi) = if bytes1 > bytes2 {
                            (bytes2, bytes1)
                        } else {
                            (bytes1, bytes2)
                        };
                        max_b < lo || min_b > hi
                    })
                } else {
                    Ok(if is_decimal {
                        let cmp_min = compare_signed_be_varlen(min_b, bytes1);
                        let cmp_max = compare_signed_be_varlen(max_b, bytes1);
                        match op {
                            FILTER_OP_LT => cmp_min != cmp::Ordering::Less,
                            FILTER_OP_LE => cmp_min == cmp::Ordering::Greater,
                            FILTER_OP_GT => cmp_max != cmp::Ordering::Greater,
                            FILTER_OP_GE => cmp_max == cmp::Ordering::Less,
                            _ => false,
                        }
                    } else {
                        match op {
                            FILTER_OP_LT => min_b >= bytes1,
                            FILTER_OP_LE => min_b > bytes1,
                            FILTER_OP_GT => max_b <= bytes1,
                            FILTER_OP_GE => max_b < bytes1,
                            _ => false,
                        }
                    })
                }
            }
            PhysicalType::FixedLenByteArray(size) => {
                let size = *size;
                if size == 0 {
                    return Ok(false);
                }
                Self::validate_filter_span(ptr, count, size, filter_desc.buf_end)?;
                let (min_b, max_b) = match (min_bytes, max_bytes) {
                    (Some(min_b), Some(max_b)) if min_b.len() == size && max_b.len() == size => {
                        (min_b, max_b)
                    }
                    _ => return Ok(false),
                };
                let null_check = if is_decimal {
                    is_fixed_len_null_be
                } else {
                    is_fixed_len_null
                };

                if is_between {
                    let bytes1 = unsafe { slice::from_raw_parts(ptr, size) };
                    let bytes2 = unsafe { slice::from_raw_parts(ptr.add(size), size) };
                    if null_check(bytes1) || null_check(bytes2) {
                        return Ok(false);
                    }
                    Ok(if is_decimal {
                        let ord = compare_signed_be(bytes1, bytes2);
                        let (lo, hi) = if ord == cmp::Ordering::Greater {
                            (bytes2, bytes1)
                        } else {
                            (bytes1, bytes2)
                        };
                        compare_signed_be(max_b, lo) == cmp::Ordering::Less
                            || compare_signed_be(min_b, hi) == cmp::Ordering::Greater
                    } else {
                        let (lo, hi) = if bytes1 > bytes2 {
                            (bytes2, bytes1)
                        } else {
                            (bytes1, bytes2)
                        };
                        max_b < lo || min_b > hi
                    })
                } else {
                    let bytes = unsafe { slice::from_raw_parts(ptr, size) };
                    if null_check(bytes) {
                        return Ok(false);
                    }
                    Ok(if is_decimal {
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
                    })
                }
            }
            _ => Ok(false),
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
            ascii: None,
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
    if a.is_empty() {
        return cmp::Ordering::Equal;
    }
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

#[cfg(test)]
mod multi_dict_tests {
    use super::*;
    use parquet2::compression::Compression;

    fn make_uncompressed_dict(buf: &[u8], num_values: usize) -> SlicedDictPage<'_> {
        SlicedDictPage {
            buffer: buf,
            compression: Compression::Uncompressed,
            uncompressed_size: buf.len(),
            num_values,
            is_sorted: false,
        }
    }

    fn make_snappy_dict(
        compressed: &[u8],
        uncompressed_size: usize,
        num_values: usize,
    ) -> SlicedDictPage<'_> {
        SlicedDictPage {
            buffer: compressed,
            compression: Compression::Snappy,
            uncompressed_size,
            num_values,
            is_sorted: false,
        }
    }

    fn snappy_compress(input: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        parquet2::compression::compress(
            parquet2::compression::CompressionOptions::Snappy,
            input,
            &mut out,
        )
        .unwrap();
        out
    }

    /// Verifies that consecutive dict-page decompressions through `decompress_varchar_slice_dict`
    /// produce buffers that do not alias each other. Aux entries decoded against the first dict
    /// must remain valid (point at the original bytes) after the second dict is decompressed.
    ///
    /// This is the core invariant the multi-dict-page-per-column-chunk reader path depends on.
    #[test]
    fn multiple_compressed_dict_pages_use_disjoint_buffers() {
        // Use distinct payloads so we can detect any cross-buffer corruption.
        let dict1_raw: Vec<u8> = (b'a'..=b'p').collect();
        let dict2_raw: Vec<u8> = (b'A'..=b'P').collect();

        let dict1_compressed = snappy_compress(&dict1_raw);
        let dict2_compressed = snappy_compress(&dict2_raw);

        let mut persistent: Vec<Vec<u8>> = Vec::new();
        let mut pool: Vec<Vec<u8>> = Vec::new();

        let dict1_page = make_snappy_dict(&dict1_compressed, dict1_raw.len(), 4);
        let page1 = decompress_varchar_slice_dict(dict1_page, &mut persistent, &mut pool).unwrap();
        let page1_ptr = page1.buffer.as_ptr();
        let page1_len = page1.buffer.len();
        // Capture the bytes via raw pointer so the borrow is released for the next call.
        // SAFETY: persistent owns the buffer; we only read from it.
        let page1_view: &[u8] = unsafe { std::slice::from_raw_parts(page1_ptr, page1_len) };
        assert_eq!(page1_view, dict1_raw.as_slice());
        let _ = page1;

        let dict2_page = make_snappy_dict(&dict2_compressed, dict2_raw.len(), 4);
        let page2 = decompress_varchar_slice_dict(dict2_page, &mut persistent, &mut pool).unwrap();
        assert_eq!(page2.buffer, dict2_raw.as_slice());
        // dict1 must still be intact after dict2 was decompressed.
        assert_eq!(page1_view, dict1_raw.as_slice());
        // The two buffers must live at different addresses.
        assert_ne!(page1_view.as_ptr(), page2.buffer.as_ptr());
        // Both buffers must end up persisted.
        assert_eq!(persistent.len(), 2);
    }

    /// Uncompressed dict pages reuse the input mmap slice directly, but the buffer pointer
    /// returned by the helper must still be stable across subsequent helper calls so that the
    /// reader's `dict` slot remains valid.
    #[test]
    fn uncompressed_dict_pages_do_not_alias() {
        let dict1_bytes: Vec<u8> = b"first dict bytes".to_vec();
        let dict2_bytes: Vec<u8> = b"second dict bytes!".to_vec();

        let mut persistent: Vec<Vec<u8>> = Vec::new();
        let mut pool: Vec<Vec<u8>> = Vec::new();

        let page1 = decompress_varchar_slice_dict(
            make_uncompressed_dict(&dict1_bytes, 1),
            &mut persistent,
            &mut pool,
        )
        .unwrap();
        let page1_ptr = page1.buffer.as_ptr();
        let page1_len = page1.buffer.len();
        let page1_view: &[u8] = unsafe { std::slice::from_raw_parts(page1_ptr, page1_len) };
        assert_eq!(page1_view, dict1_bytes.as_slice());
        let _ = page1;

        let page2 = decompress_varchar_slice_dict(
            make_uncompressed_dict(&dict2_bytes, 1),
            &mut persistent,
            &mut pool,
        )
        .unwrap();
        assert_eq!(page2.buffer, dict2_bytes.as_slice());
        // dict1's slice still points at the original input.
        assert_eq!(page1_view, dict1_bytes.as_slice());
        assert_ne!(page1_view.as_ptr(), page2.buffer.as_ptr());
        // Uncompressed pages do not allocate, so persistent stays empty.
        assert!(persistent.is_empty());
    }
}
