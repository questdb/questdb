use crate::allocator::{AcVec, QdbAllocator};
use crate::parquet::error::{fmt_err, ParquetErrorExt, ParquetResult};
use crate::parquet::qdb_metadata::{QdbMeta, QdbMetaCol};
use crate::parquet_read::column_sink::var::fixup_varchar_slice_spill_pointers;
use crate::parquet_read::decode::{
    decode_page, decode_page_filtered, decompress_sliced_data, decompress_sliced_dict,
    page_row_count, resize_decompress_buffer, sliced_page_row_count,
};
use crate::parquet_read::page::{DataPage, DictPage};
use crate::parquet_read::{
    ColumnChunkBuffers, ColumnFilterPacked, ColumnFilterValues, ColumnMeta, DecodeContext,
    VarcharSliceBufGuard, FILTER_OP_BETWEEN, FILTER_OP_EQ, FILTER_OP_GE, FILTER_OP_GT,
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
pub(crate) fn decompress_varchar_slice_data<'a>(
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
pub(super) fn post_convert(
    from_type: ColumnType,
    to_type: ColumnType,
    leading_nulls: usize,
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
        // Boolean has no in-band null sentinel, so its only nulls are the column-top prefix.
        // Byte/Short targets also lack a sentinel (column top stays 0, matching native), but
        // sentinel-bearing targets must stamp the target NULL over the column-top rows.
        (ColumnTypeTag::Boolean, ColumnTypeTag::Int) => {
            expand_bool::<i32>(&mut bufs.data_vec)?;
            stamp_leading_nulls(&mut bufs.data_vec, leading_nulls, nulls::INT);
        }
        (
            ColumnTypeTag::Boolean,
            ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp,
        ) => {
            expand_bool::<i64>(&mut bufs.data_vec)?;
            stamp_leading_nulls(&mut bufs.data_vec, leading_nulls, nulls::LONG);
        }
        (ColumnTypeTag::Boolean, ColumnTypeTag::Float) => {
            expand_bool::<f32>(&mut bufs.data_vec)?;
            stamp_leading_nulls(&mut bufs.data_vec, leading_nulls, f32::NAN);
        }
        (ColumnTypeTag::Boolean, ColumnTypeTag::Double) => {
            expand_bool::<f64>(&mut bufs.data_vec)?;
            stamp_leading_nulls(&mut bufs.data_vec, leading_nulls, f64::NAN);
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
        (
            ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp,
            ColumnTypeTag::Boolean,
        ) => {
            contract_to_bool::<i64>(&mut bufs.data_vec, |v| v == nulls::LONG);
        }
        (ColumnTypeTag::Float, ColumnTypeTag::Boolean) => {
            contract_to_bool::<f32>(&mut bufs.data_vec, |v| v.is_nan());
        }
        (ColumnTypeTag::Double, ColumnTypeTag::Boolean) => {
            contract_to_bool::<f64>(&mut bufs.data_vec, |v| v.is_nan());
        }
        // DATE (ms), TIMESTAMP (μs) and TIMESTAMP_NS (ns) differ only in time-unit
        // resolution, so a cross between any two is a single power-of-1000 rescale.
        // Folding the net factor here keeps the conversion one pass: DATE → TIMESTAMP_NS
        // scales ×1_000_000 directly instead of ×1000 (ms→μs) then ×1000 (μs→ns).
        // Matches the native converters (e.g. convert_ms_to_ns), which scale with a
        // single unchecked multiply; scale_i64_in_place mirrors that via wrapping_mul
        // and preserves the LONG null sentinel.
        (
            ColumnTypeTag::Date | ColumnTypeTag::Timestamp,
            ColumnTypeTag::Date | ColumnTypeTag::Timestamp,
        ) => {
            let src_pow = time_unit_pow10(from_type);
            let dst_pow = time_unit_pow10(to_type);
            if dst_pow > src_pow {
                scale_i64_in_place(&mut bufs.data_vec, 10i64.pow(dst_pow - src_pow), false);
            } else if dst_pow < src_pow {
                scale_i64_in_place(&mut bufs.data_vec, 10i64.pow(src_pow - dst_pow), true);
            }
        }
        // Fixed → Varchar: Java handles batch conversion after decode.
        (src, ColumnTypeTag::Varchar) if is_fixed_to_var_source(src) => {}
        // Fixed → String: Java handles batch conversion after decode.
        (src, ColumnTypeTag::String) if is_fixed_to_var_source(src) => {}
        // Var → fixed: Java handles batch conversion after decode.
        (ColumnTypeTag::Varchar | ColumnTypeTag::String, dst) if is_var_to_fixed_target(dst) => {}
        // Decimal → Decimal: rescale to the target scale and NULL out any value that does not fit
        // the target exactly (lossy scale-down, scale-up overflow, or magnitude beyond the target
        // precision), mirroring the native DecimalColumnTypeConverter. Runs for any genuine
        // conversion, even at equal scale, so a same-scale precision reduction still clamps
        // out-of-range values to NULL. A true identity read (from_type == to_type) requests no
        // conversion and must pass through untouched: the decoded bytes are already the target
        // representation, and a plain read reconstructs the ColumnType from file metadata that may
        // omit precision/scale (precision 0), which would otherwise clamp every value with
        // |v| >= 10^0 = 1 to NULL. Identity falls through to the `a == b` no-op arm below.
        (src, dst) if is_decimal_tag(src) && is_decimal_tag(dst) && from_type != to_type => {
            if decimal_tag_size(dst) < decimal_tag_size(src) {
                // Narrowing: plan_decode_conversion kept the source width (DecodeAs::Source), so the
                // buffer holds full-width source values; rescale, range-check, then narrow.
                convert_decimal_narrowing(
                    &mut bufs.data_vec,
                    src,
                    dst,
                    from_type.decimal_scale(),
                    to_type.decimal_scale(),
                    to_type.decimal_precision(),
                )?;
            } else {
                // Same width / widening: the decoder produced the target width (DecodeAs::Target).
                convert_decimal_in_place(
                    &mut bufs.data_vec,
                    dst,
                    from_type.decimal_scale(),
                    to_type.decimal_scale(),
                    to_type.decimal_precision(),
                )?;
            }
        }
        // Fixed integer → Decimal: widen to target size and scale by 10^(target_scale).
        // Byte/Short have no in-band null sentinel, so their column-top prefix is nulled via
        // `leading_nulls`; Int/Long carry i32::MIN/i64::MIN in-band (and may have scattered
        // nulls), handled by `is_int_null` inside convert_fixed_to_decimal, so pass 0 for them.
        (
            ColumnTypeTag::Byte | ColumnTypeTag::Short | ColumnTypeTag::Int | ColumnTypeTag::Long,
            dst,
        ) if is_decimal_tag(dst) => {
            let dec_leading_nulls = match src_tag {
                ColumnTypeTag::Byte | ColumnTypeTag::Short => leading_nulls,
                _ => 0,
            };
            convert_fixed_to_decimal(
                &mut bufs.data_vec,
                src_tag,
                dst,
                dec_leading_nulls,
                to_type.decimal_scale(),
                to_type.decimal_precision(),
            )?;
        }
        // Int32 → Int64 widening. Byte/Short → Long/Timestamp now decode straight to i64
        // (DecodeAs::Target), so only Byte/Short → Date reaches post_convert here; Date has
        // no dedicated DeltaBinaryPacked decode arm yet, so it stays a two-pass widen.
        // Byte/Short have no in-band null sentinel, so their only nulls are the column-top
        // prefix (def-level=0 on the OPTIONAL schema); `leading_nulls` rows are stamped with
        // the target sentinel to match the native ALTER path.
        (ColumnTypeTag::Byte, ColumnTypeTag::Date) => {
            convert_numeric_in_place::<i8, i64>(&mut bufs.data_vec, |_| false, 0i64, |v| v as i64)?;
            stamp_leading_nulls(&mut bufs.data_vec, leading_nulls, nulls::LONG);
        }
        (ColumnTypeTag::Short, ColumnTypeTag::Date) => {
            convert_numeric_in_place::<i16, i64>(
                &mut bufs.data_vec,
                |_| false,
                0i64,
                |v| v as i64,
            )?;
            stamp_leading_nulls(&mut bufs.data_vec, leading_nulls, nulls::LONG);
        }
        // Int → Long/Date/Timestamp stays Source: i32::MIN → i64::MIN via the value check.
        (
            ColumnTypeTag::Int,
            ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp,
        ) => {
            convert_numeric_in_place::<i32, i64>(
                &mut bufs.data_vec,
                |v| v == nulls::INT,
                nulls::LONG,
                |v| v as i64,
            )?;
        }
        // Int64 → Int32 narrowing (Long/Date/Timestamp → Byte/Short/Int).
        // Long null (i64::MIN) maps to dst null sentinel (0 for Byte/Short, i32::MIN for Int).
        (
            ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp,
            ColumnTypeTag::Byte,
        ) => {
            convert_numeric_in_place::<i64, i8>(
                &mut bufs.data_vec,
                |v| v == nulls::LONG,
                0i8,
                |v| v as i8,
            )?;
        }
        (
            ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp,
            ColumnTypeTag::Short,
        ) => {
            convert_numeric_in_place::<i64, i16>(
                &mut bufs.data_vec,
                |v| v == nulls::LONG,
                0i16,
                |v| v as i16,
            )?;
        }
        (
            ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp,
            ColumnTypeTag::Int,
        ) => {
            convert_numeric_in_place::<i64, i32>(
                &mut bufs.data_vec,
                |v| v == nulls::LONG,
                nulls::INT,
                |v| v as i32,
            )?;
        }
        // Int → Float
        (ColumnTypeTag::Byte, ColumnTypeTag::Float) => {
            convert_numeric_in_place::<i8, f32>(
                &mut bufs.data_vec,
                |_| false,
                0.0f32,
                |v| v as f32,
            )?;
            stamp_leading_nulls(&mut bufs.data_vec, leading_nulls, f32::NAN);
        }
        (ColumnTypeTag::Short, ColumnTypeTag::Float) => {
            convert_numeric_in_place::<i16, f32>(
                &mut bufs.data_vec,
                |_| false,
                0.0f32,
                |v| v as f32,
            )?;
            stamp_leading_nulls(&mut bufs.data_vec, leading_nulls, f32::NAN);
        }
        (ColumnTypeTag::Int, ColumnTypeTag::Float) => {
            convert_numeric_in_place::<i32, f32>(
                &mut bufs.data_vec,
                |v| v == nulls::INT,
                f32::NAN,
                |v| v as f32,
            )?;
        }
        (
            ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp,
            ColumnTypeTag::Float,
        ) => {
            convert_numeric_in_place::<i64, f32>(
                &mut bufs.data_vec,
                |v| v == nulls::LONG,
                f32::NAN,
                |v| v as f32,
            )?;
        }
        // Int → Double
        (ColumnTypeTag::Byte, ColumnTypeTag::Double) => {
            convert_numeric_in_place::<i8, f64>(
                &mut bufs.data_vec,
                |_| false,
                0.0f64,
                |v| v as f64,
            )?;
            stamp_leading_nulls(&mut bufs.data_vec, leading_nulls, f64::NAN);
        }
        (ColumnTypeTag::Short, ColumnTypeTag::Double) => {
            convert_numeric_in_place::<i16, f64>(
                &mut bufs.data_vec,
                |_| false,
                0.0f64,
                |v| v as f64,
            )?;
            stamp_leading_nulls(&mut bufs.data_vec, leading_nulls, f64::NAN);
        }
        (ColumnTypeTag::Int, ColumnTypeTag::Double) => {
            convert_numeric_in_place::<i32, f64>(
                &mut bufs.data_vec,
                |v| v == nulls::INT,
                f64::NAN,
                |v| v as f64,
            )?;
        }
        (
            ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp,
            ColumnTypeTag::Double,
        ) => {
            convert_numeric_in_place::<i64, f64>(
                &mut bufs.data_vec,
                |v| v == nulls::LONG,
                f64::NAN,
                |v| v as f64,
            )?;
        }
        // Float/Double → Byte/Short/Int/Long/Date/Timestamp now decode straight to the
        // target via the FloatToIntRangeCheckConverter arms (DecodeAs::Target), so they
        // reach the no-op block below instead of converting here.
        // Float <-> Double (infinity and out-of-range map to dst null sentinel)
        (ColumnTypeTag::Float, ColumnTypeTag::Double) => {
            convert_numeric_in_place::<f32, f64>(
                &mut bufs.data_vec,
                |v| v.is_nan() || v.is_infinite(),
                f64::NAN,
                |v| v as f64,
            )?;
        }
        (ColumnTypeTag::Double, ColumnTypeTag::Float) => {
            convert_numeric_in_place::<f64, f32>(
                &mut bufs.data_vec,
                |v| v.is_nan() || v > f32::MAX as f64 || v < f32::MIN as f64,
                f32::NAN,
                |v| v as f32,
            )?;
        }
        // No-op pairs reached when plan_decode_conversion chose DecodeAs::Target
        // (decoder produced target-physical bytes directly) or src tag == dst tag.
        // Enumerated explicitly so a new pair added to plan_decode_conversion
        // without a matching arm here fails loudly via the catch-all below
        // instead of silently leaving the buffer in the source layout.
        (a, b) if a == b => {}
        (
            ColumnTypeTag::Byte | ColumnTypeTag::Short | ColumnTypeTag::Int,
            ColumnTypeTag::Byte | ColumnTypeTag::Short | ColumnTypeTag::Int,
        ) => {}
        (
            ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp,
            ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp,
        ) => {}
        // Byte/Short -> Long/Timestamp: decoded straight to i64 (no in-band sentinel).
        (
            ColumnTypeTag::Byte | ColumnTypeTag::Short,
            ColumnTypeTag::Long | ColumnTypeTag::Timestamp,
        ) => {}
        // Float/Double -> int/time: decoded straight to the target by the range-check
        // converter, which already maps NaN/out-of-range to the target null sentinel.
        (
            ColumnTypeTag::Float | ColumnTypeTag::Double,
            ColumnTypeTag::Byte
            | ColumnTypeTag::Short
            | ColumnTypeTag::Int
            | ColumnTypeTag::Long
            | ColumnTypeTag::Date
            | ColumnTypeTag::Timestamp,
        ) => {}
        (
            ColumnTypeTag::String | ColumnTypeTag::Varchar | ColumnTypeTag::Symbol,
            ColumnTypeTag::String | ColumnTypeTag::Varchar,
        ) => {}
        (ColumnTypeTag::Array, ColumnTypeTag::Array) => {}
        _ => {
            return Err(fmt_err!(
                InvalidType,
                "post_convert: unsupported conversion {} -> {}",
                from_type,
                to_type,
            ));
        }
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
///
/// Multiplication uses `wrapping_mul` to match the native ALTER COLUMN TYPE
/// path in `core/src/main/c/share/converters.cpp` (e.g. `convert_ms_to_ns`),
/// which scales with plain C++ signed multiplication and wraps on overflow.
/// Both paths must produce identical results for the same input or a lazy
/// parquet read of a type-converted column diverges from the eager native
/// rewrite; see ParquetColumnTypeConversionTest#testDateToOtherFixedTypes.
pub(super) fn scale_i64_in_place(data: &mut AcVec<u8>, factor: i64, divide: bool) {
    let count = data.len() / size_of::<i64>();
    let ptr = data.as_mut_ptr() as *mut i64;
    for i in 0..count {
        let val = unsafe { ptr.add(i).read_unaligned() };
        let converted = if val == qdb_core::col_type::nulls::LONG {
            qdb_core::col_type::nulls::LONG
        } else if divide {
            val / factor
        } else {
            val.wrapping_mul(factor)
        };
        unsafe { ptr.add(i).write_unaligned(converted) };
    }
}

/// Power-of-ten resolution of a DATE / TIMESTAMP value relative to seconds: DATE
/// counts milliseconds (10^3), microsecond TIMESTAMP counts 10^6, and nanosecond
/// TIMESTAMP (the QDB_TIMESTAMP_NS flag) counts 10^9. The gap between two of these
/// exponents is the single power-of-1000 factor that converts one representation
/// to the other. Only DATE and TIMESTAMP column types reach this helper.
fn time_unit_pow10(col_type: ColumnType) -> u32 {
    match col_type.tag() {
        ColumnTypeTag::Date => 3,
        ColumnTypeTag::Timestamp if col_type.has_flag(QDB_TIMESTAMP_NS_COLUMN_TYPE_FLAG) => 9,
        // Microsecond TIMESTAMP.
        _ => 6,
    }
}

/// Convert numeric values in place between types of different sizes.
/// Handles null sentinel mapping (e.g. i64::MIN → f32::NAN, f64::NAN → i32::MIN).
/// For widening (smaller→larger), iterates backward to avoid overwriting unread data.
fn convert_numeric_in_place<S, D>(
    data: &mut AcVec<u8>,
    is_null: fn(S) -> bool,
    null_dst: D,
    convert: fn(S) -> D,
) -> ParquetResult<()>
where
    S: Copy,
    D: Copy,
{
    let src_size = size_of::<S>();
    let dst_size = size_of::<D>();
    let n = data.len() / src_size;
    if n == 0 {
        return Ok(());
    }
    let needed = n * dst_size;
    if needed > data.len() {
        data.reserve(needed - data.len())?;
    }
    unsafe { data.set_len(needed) };
    let ptr = data.as_mut_ptr();
    if dst_size <= src_size {
        for i in 0..n {
            let val: S = unsafe { (ptr.add(i * src_size) as *const S).read_unaligned() };
            let out = if is_null(val) { null_dst } else { convert(val) };
            unsafe { (ptr.add(i * dst_size) as *mut D).write_unaligned(out) };
        }
    } else {
        for i in (0..n).rev() {
            let val: S = unsafe { (ptr.add(i * src_size) as *const S).read_unaligned() };
            let out = if is_null(val) { null_dst } else { convert(val) };
            unsafe { (ptr.add(i * dst_size) as *mut D).write_unaligned(out) };
        }
    }
    Ok(())
}

/// Overwrite the first `count` elements of `data` (interpreted as `[D]`) with `null_value`.
///
/// Used for conversions whose source type has no in-band null sentinel (BYTE, SHORT, CHAR):
/// their only nulls are the contiguous column-top prefix, encoded as def-level=0 rows. The
/// decoder materialises those as an in-band 0 indistinguishable from a real 0, so the count
/// of leading nulls (`count`, derived from the column top) is needed to stamp the target
/// sentinel and keep the lazy parquet read in step with the native ALTER path.
fn stamp_leading_nulls<D: Copy>(data: &mut AcVec<u8>, count: usize, null_value: D) {
    if count == 0 {
        return;
    }
    let elem = size_of::<D>();
    let count = count.min(data.len() / elem);
    let ptr = data.as_mut_ptr();
    for i in 0..count {
        // SAFETY: i < count <= data.len()/size_of::<D>(), so the write stays in bounds.
        unsafe { (ptr.add(i * elem) as *mut D).write_unaligned(null_value) };
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

/// Convert decoded decimal values in place to the target scale, writing the target NULL sentinel
/// for any value that cannot be represented exactly in the target type. Mirrors the native
/// `DecimalColumnTypeConverter`: a value becomes NULL when a scale reduction would drop non-zero
/// digits, a scale increase overflows, or the magnitude exceeds the target precision.
///
/// The buffer already holds source values sign-extended to the target width (the decoder ran with
/// `DecodeAs::Target`), with target NULL sentinels written for source NULLs - those flow through
/// unchanged. Always run, even at equal scale, so a same-scale precision reduction still clamps
/// out-of-range values to NULL rather than reading a value that does not fit the target precision.
fn convert_decimal_in_place(
    data: &mut AcVec<u8>,
    target_tag: ColumnTypeTag,
    src_scale: u8,
    dst_scale: u8,
    dst_precision: u8,
) -> ParquetResult<()> {
    let scale_diff = (dst_scale as i32 - src_scale as i32).unsigned_abs();
    let divide = dst_scale < src_scale;
    match target_tag {
        ColumnTypeTag::Decimal8
        | ColumnTypeTag::Decimal16
        | ColumnTypeTag::Decimal32
        | ColumnTypeTag::Decimal64 => convert_decimal_i64(
            data,
            decimal_tag_size(target_tag),
            scale_diff,
            divide,
            dst_precision,
            null_i64_for_decimal(target_tag),
        )?,
        ColumnTypeTag::Decimal128 => convert_decimal_i128(data, scale_diff, divide, dst_precision)?,
        ColumnTypeTag::Decimal256 => convert_decimal_i256(data, scale_diff, divide, dst_precision)?,
        _ => {}
    }
    Ok(())
}

/// Rescale a single i64 unscaled decimal. A scale reduction (`divide`) rounds half away from zero
/// to the target scale - it never drops the row to NULL on a lost fraction, matching SQL
/// store-assignment. Returns `None` only when a scale increase (multiply) overflows i64.
#[inline]
fn rescale_one_i64(v: i64, factor: i64, divide: bool) -> Option<i64> {
    if factor == 1 {
        Some(v)
    } else if divide {
        Some(round_div_i64(v, factor))
    } else {
        v.checked_mul(factor)
    }
}

/// Integer division rounding half away from zero. `factor` (= 10^k, k >= 1 on the divide path) is
/// even and the remainder fits i64, so promoting to i128 for the `2*|r|` comparison cannot overflow.
#[inline]
fn round_div_i64(v: i64, factor: i64) -> i64 {
    let q = v / factor;
    let r = (v % factor) as i128;
    if 2 * r.abs() >= factor as i128 {
        q + v.signum()
    } else {
        q
    }
}

/// Integer division rounding half away from zero for i128. `factor` (= 10^k, k >= 1) is even, so
/// `factor / 2` is exact; comparing `|r| >= factor / 2` avoids the `2*|r|` overflow a factor near
/// 10^38 would cause.
#[inline]
fn round_div_i128(v: i128, factor: i128) -> i128 {
    let q = v / factor;
    let r = v % factor;
    if r.abs() >= factor / 2 {
        q + v.signum()
    } else {
        q
    }
}

/// Decimal8/16/32/64 (i64-backed, `size` = 1/2/4/8 bytes) conversion. For these widths the max
/// precision is 18, so 10^scale_diff and 10^precision both fit i64.
fn convert_decimal_i64(
    data: &mut AcVec<u8>,
    size: usize,
    scale_diff: u32,
    divide: bool,
    precision: u8,
    null: i64,
) -> ParquetResult<()> {
    let count = data.len() / size;
    let ptr = data.as_mut_ptr();
    let Some(factor) = 10i64.checked_pow(scale_diff) else {
        return Err(fmt_err!(
            InvalidLayout,
            "decimal scale_diff {} exceeds i64 range",
            scale_diff
        ));
    };
    let Some(limit) = 10i64.checked_pow(precision as u32) else {
        return Err(fmt_err!(
            InvalidLayout,
            "decimal precision {} exceeds i64 range",
            precision
        ));
    };
    for i in 0..count {
        let v = unsafe { read_le_i64_at(ptr, i, size) };
        if v == null {
            continue;
        }
        // |scaled| < 10^precision also guarantees the result fits the `size`-byte width, since the
        // precision of an N-byte decimal never exceeds the digits N bytes can hold.
        let out = match rescale_one_i64(v, factor, divide) {
            Some(scaled) if scaled > -limit && scaled < limit => scaled,
            _ => null,
        };
        unsafe { write_le_i64_at(ptr, i, size, out) };
    }
    Ok(())
}

/// Decimal128 conversion. Layout per element: [hi: i64 LE, lo: u64 LE]. Max precision 38, so
/// 10^scale_diff and 10^precision both fit i128.
fn convert_decimal_i128(
    data: &mut AcVec<u8>,
    scale_diff: u32,
    divide: bool,
    precision: u8,
) -> ParquetResult<()> {
    let count = data.len() / 16;
    let ptr = data.as_mut_ptr();
    let Some(factor) = 10i128.checked_pow(scale_diff) else {
        return Err(fmt_err!(
            InvalidLayout,
            "decimal scale_diff {} exceeds i128 range",
            scale_diff
        ));
    };
    let Some(limit) = 10i128.checked_pow(precision as u32) else {
        return Err(fmt_err!(
            InvalidLayout,
            "decimal precision {} exceeds i128 range",
            precision
        ));
    };
    for i in 0..count {
        let offset = i * 16;
        let hi = unsafe { (ptr.add(offset) as *const i64).read_unaligned() };
        let lo = unsafe { (ptr.add(offset + 8) as *const u64).read_unaligned() };
        if hi == i64::MIN && lo == 0 {
            continue;
        }
        let val = ((hi as i128) << 64) | (lo as i128);
        let scaled = if factor == 1 {
            Some(val)
        } else if divide {
            // Scale reduction rounds half away from zero; it never NULLs on a dropped fraction.
            Some(round_div_i128(val, factor))
        } else {
            val.checked_mul(factor)
        };
        let (nh, nl) = match scaled {
            Some(s) if s > -limit && s < limit => ((s >> 64) as i64, s as u64),
            _ => (i64::MIN, 0u64),
        };
        unsafe {
            (ptr.add(offset) as *mut i64).write_unaligned(nh);
            (ptr.add(offset + 8) as *mut u64).write_unaligned(nl);
        }
    }
    Ok(())
}

/// Decimal256 conversion. Layout per element: [w0(hi): i64 LE, w1, w2, w3: u64 LE].
fn convert_decimal_i256(
    data: &mut AcVec<u8>,
    scale_diff: u32,
    divide: bool,
    precision: u8,
) -> ParquetResult<()> {
    let count = data.len() / 32;
    let ptr = data.as_mut_ptr();
    // 10^precision as a positive i256; the magnitude words bound the representable range.
    let limit = pow10_i256(precision as u32)?;
    let limit_abs = (limit.0 as u64, limit.1, limit.2, limit.3);
    for i in 0..count {
        let offset = i * 32;
        let w0 = unsafe { (ptr.add(offset) as *const i64).read_unaligned() };
        let w1 = unsafe { (ptr.add(offset + 8) as *const u64).read_unaligned() };
        let w2 = unsafe { (ptr.add(offset + 16) as *const u64).read_unaligned() };
        let w3 = unsafe { (ptr.add(offset + 24) as *const u64).read_unaligned() };
        if w0 == i64::MIN && w1 == 0 && w2 == 0 && w3 == 0 {
            continue;
        }
        let scaled = if scale_diff == 0 {
            Some((w0, w1, w2, w3))
        } else if divide {
            // Scale reduction rounds half away from zero; it never NULLs on a dropped fraction.
            Some(round_div_i256_pow10(w0, w1, w2, w3, scale_diff))
        } else {
            checked_mul_i256_pow10(w0, w1, w2, w3, scale_diff)
        };
        let out = match scaled {
            Some(s) if !i256_abs_ge(s, limit_abs) => s,
            _ => (i64::MIN, 0, 0, 0),
        };
        unsafe {
            (ptr.add(offset) as *mut i64).write_unaligned(out.0);
            (ptr.add(offset + 8) as *mut u64).write_unaligned(out.1);
            (ptr.add(offset + 16) as *mut u64).write_unaligned(out.2);
            (ptr.add(offset + 24) as *mut u64).write_unaligned(out.3);
        }
    }
    Ok(())
}

/// Compute 10^exp as a sign-extended 256-bit integer (always positive). Errors if it overflows
/// i256, which cannot happen for a valid Decimal256 precision/scale (both bounded by 76).
fn pow10_i256(exp: u32) -> ParquetResult<(i64, u64, u64, u64)> {
    match checked_mul_i256_pow10(0, 0, 0, 1, exp) {
        Some(v) => Ok(v),
        None => Err(fmt_err!(
            InvalidLayout,
            "decimal 10^{} exceeds i256 range",
            exp
        )),
    }
}

/// Returns true when |value| (256-bit, `value.0` is the signed high word) is >= `limit`
/// (256-bit unsigned magnitude words, high word first). Used for the target-precision check.
#[inline]
fn i256_abs_ge(value: (i64, u64, u64, u64), limit: (u64, u64, u64, u64)) -> bool {
    let abs = if value.0 < 0 {
        let n = negate_i256(value.0, value.1, value.2, value.3);
        (n.0 as u64, n.1, n.2, n.3)
    } else {
        (value.0 as u64, value.1, value.2, value.3)
    };
    abs >= limit
}

/// The low 128 bits of a 256-bit value as i128 (two's complement). Only valid when the value is
/// known to fit i128 (the caller checks it against the target precision first).
#[inline]
fn i256_low_i128(words: (i64, u64, u64, u64)) -> i128 {
    (((words.2 as u128) << 64) | (words.3 as u128)) as i128
}

/// Null sentinel for a narrowing decimal target as i128 (targets are always <= Decimal128 here).
#[inline]
fn decimal_null_i128(tag: ColumnTypeTag) -> i128 {
    match tag {
        ColumnTypeTag::Decimal8 => i8::MIN as i128,
        ColumnTypeTag::Decimal16 => i16::MIN as i128,
        ColumnTypeTag::Decimal32 => i32::MIN as i128,
        ColumnTypeTag::Decimal64 => i64::MIN as i128,
        // Decimal128 null: hi = i64::MIN, lo = 0.
        _ => (i64::MIN as i128) << 64,
    }
}

/// Write `value` (already known to fit the target) at the `dst_size`-byte decimal slot `idx`.
#[inline]
unsafe fn write_decimal_le(ptr: *mut u8, idx: usize, dst_size: usize, value: i128) {
    let off = idx * dst_size;
    match dst_size {
        1 => *(ptr.add(off) as *mut i8) = value as i8,
        2 => (ptr.add(off) as *mut i16).write_unaligned(value as i16),
        4 => (ptr.add(off) as *mut i32).write_unaligned(value as i32),
        8 => (ptr.add(off) as *mut i64).write_unaligned(value as i64),
        // 16: Decimal128, [hi: i64 LE, lo: u64 LE].
        _ => {
            (ptr.add(off) as *mut i64).write_unaligned((value >> 64) as i64);
            (ptr.add(off + 8) as *mut u64).write_unaligned(value as u64);
        }
    }
}

/// Narrowing decimal->decimal: the decoder kept the SOURCE width (DecodeAs::Source), so read each
/// value at the source width, rescale to the target scale (scale-down rounds half away from zero),
/// NULL out only values whose magnitude overflows the target precision, and write the result at the
/// smaller target width. Writes trail reads (dst_size < src_size), so a forward in-place pass
/// is safe; the buffer is then shrunk to `count * dst_size`. This mirrors the native
/// DecimalColumnTypeConverter (widen -> rescale -> range-check -> narrow), keeping narrowing lazy.
fn convert_decimal_narrowing(
    data: &mut AcVec<u8>,
    src_tag: ColumnTypeTag,
    dst_tag: ColumnTypeTag,
    src_scale: u8,
    dst_scale: u8,
    dst_precision: u8,
) -> ParquetResult<()> {
    let src_size = decimal_tag_size(src_tag);
    let dst_size = decimal_tag_size(dst_tag);
    debug_assert!(dst_size < src_size);
    let count = data.len() / src_size;
    let scale_diff = (dst_scale as i32 - src_scale as i32).unsigned_abs();
    let divide = dst_scale < src_scale;
    let dst_null = decimal_null_i128(dst_tag);
    let ptr = data.as_mut_ptr();
    match src_tag {
        // Source fits i64 (Decimal16/32/64); the smaller target is also i64-backed.
        ColumnTypeTag::Decimal16 | ColumnTypeTag::Decimal32 | ColumnTypeTag::Decimal64 => {
            let src_null = null_i64_for_decimal(src_tag);
            let Some(factor) = 10i64.checked_pow(scale_diff) else {
                return Err(fmt_err!(
                    InvalidLayout,
                    "decimal scale_diff {} exceeds i64 range",
                    scale_diff
                ));
            };
            let Some(limit) = 10i64.checked_pow(dst_precision as u32) else {
                return Err(fmt_err!(
                    InvalidLayout,
                    "decimal precision {} exceeds i64 range",
                    dst_precision
                ));
            };
            for i in 0..count {
                let v = unsafe { read_le_i64_at(ptr, i, src_size) };
                let out = if v == src_null {
                    dst_null
                } else {
                    match rescale_one_i64(v, factor, divide) {
                        Some(s) if s > -limit && s < limit => s as i128,
                        _ => dst_null,
                    }
                };
                unsafe { write_decimal_le(ptr, i, dst_size, out) };
            }
        }
        // Source is Decimal128; the smaller target is Decimal8..64 (i64-backed).
        ColumnTypeTag::Decimal128 => {
            let Some(factor) = 10i128.checked_pow(scale_diff) else {
                return Err(fmt_err!(
                    InvalidLayout,
                    "decimal scale_diff {} exceeds i128 range",
                    scale_diff
                ));
            };
            let Some(limit) = 10i128.checked_pow(dst_precision as u32) else {
                return Err(fmt_err!(
                    InvalidLayout,
                    "decimal precision {} exceeds i128 range",
                    dst_precision
                ));
            };
            for i in 0..count {
                let offset = i * 16;
                let hi = unsafe { (ptr.add(offset) as *const i64).read_unaligned() };
                let lo = unsafe { (ptr.add(offset + 8) as *const u64).read_unaligned() };
                let out = if hi == i64::MIN && lo == 0 {
                    dst_null
                } else {
                    let val = ((hi as i128) << 64) | (lo as i128);
                    let scaled = if factor == 1 {
                        Some(val)
                    } else if divide {
                        // Scale reduction rounds half away from zero (no NULL on a dropped fraction).
                        Some(round_div_i128(val, factor))
                    } else {
                        val.checked_mul(factor)
                    };
                    match scaled {
                        Some(s) if s > -limit && s < limit => s,
                        _ => dst_null,
                    }
                };
                unsafe { write_decimal_le(ptr, i, dst_size, out) };
            }
        }
        // Source is Decimal256; the smaller target is Decimal8..128.
        ColumnTypeTag::Decimal256 => {
            let limit = pow10_i256(dst_precision as u32)?;
            let limit_abs = (limit.0 as u64, limit.1, limit.2, limit.3);
            for i in 0..count {
                let offset = i * 32;
                let w0 = unsafe { (ptr.add(offset) as *const i64).read_unaligned() };
                let w1 = unsafe { (ptr.add(offset + 8) as *const u64).read_unaligned() };
                let w2 = unsafe { (ptr.add(offset + 16) as *const u64).read_unaligned() };
                let w3 = unsafe { (ptr.add(offset + 24) as *const u64).read_unaligned() };
                let out = if w0 == i64::MIN && w1 == 0 && w2 == 0 && w3 == 0 {
                    dst_null
                } else {
                    let scaled = if scale_diff == 0 {
                        Some((w0, w1, w2, w3))
                    } else if divide {
                        // Scale reduction rounds half away from zero (no NULL on a dropped fraction).
                        Some(round_div_i256_pow10(w0, w1, w2, w3, scale_diff))
                    } else {
                        checked_mul_i256_pow10(w0, w1, w2, w3, scale_diff)
                    };
                    match scaled {
                        Some(s) if !i256_abs_ge(s, limit_abs) => i256_low_i128(s),
                        _ => dst_null,
                    }
                };
                unsafe { write_decimal_le(ptr, i, dst_size, out) };
            }
        }
        _ => {}
    }
    unsafe { data.set_len(count * dst_size) };
    Ok(())
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

/// Multiply a sign-extended 256-bit integer by 10^scale_diff with overflow checking.
/// Returns `None` if the magnitude overflows i256 at any intermediate step.
fn checked_mul_i256_pow10(
    w0: i64,
    w1: u64,
    w2: u64,
    w3: u64,
    scale_diff: u32,
) -> Option<(i64, u64, u64, u64)> {
    let mut r = (w0, w1, w2, w3);
    let mut remaining = scale_diff;
    while remaining > 0 {
        let step = remaining.min(18);
        r = checked_mul_i256_u64(r.0, r.1, r.2, r.3, 10u64.pow(step))?;
        remaining -= step;
    }
    Some(r)
}

/// Checked variant of [`mul_i256_u64`].
///
/// Returns `None` if the multiplication overflows i256. The check compares the
/// pre-multiplication sign of the high limb against the post-multiplication
/// sign after stripping the carry: any divergence indicates the high limb
/// truncated significant bits.
fn checked_mul_i256_u64(
    w0: i64,
    w1: u64,
    w2: u64,
    w3: u64,
    factor: u64,
) -> Option<(i64, u64, u64, u64)> {
    let f = factor as u128;
    let p3 = w3 as u128 * f;
    let p2 = w2 as u128 * f + (p3 >> 64);
    let p1 = w1 as u128 * f + (p2 >> 64);
    let p0_full = w0 as i128 * f as i128 + (p1 >> 64) as i128;
    // Overflow detection: a sign-extended i256 multiplied by a positive u64
    // factor preserves sign. The high limb (i64) after truncation must match the
    // sign of the full i128 product. If not, bits were lost.
    let p0_trunc = p0_full as i64;
    if p0_trunc as i128 != p0_full {
        return None;
    }
    Some((p0_trunc, p1 as u64, p2 as u64, p3 as u64))
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
    let (aw0, aw1, aw2, aw3) = if neg {
        negate_i256(w0, w1, w2, w3)
    } else {
        (w0, w1, w2, w3)
    };
    let d = divisor as u128;
    let p0 = aw0 as u64 as u128;
    let q0 = (p0 / d) as u64;
    let p1 = ((p0 % d) << 64) | aw1 as u128;
    let q1 = (p1 / d) as u64;
    let p2 = ((p1 % d) << 64) | aw2 as u128;
    let q2 = (p2 / d) as u64;
    let p3 = ((p2 % d) << 64) | aw3 as u128;
    let q3 = (p3 / d) as u64;
    if neg {
        negate_i256(q0 as i64, q1, q2, q3)
    } else {
        (q0 as i64, q1, q2, q3)
    }
}

fn negate_i256(w0: i64, w1: u64, w2: u64, w3: u64) -> (i64, u64, u64, u64) {
    let (n3, c3) = (!w3).overflowing_add(1);
    let (n2, c2) = (!w2).overflowing_add(c3 as u64);
    let (n1, c1) = (!w1).overflowing_add(c2 as u64);
    let n0 = (!(w0 as u64)).wrapping_add(c1 as u64) as i64;
    (n0, n1, n2, n3)
}

/// Add two 256-bit two's complement integers (wrapping, high word first). Used to bias a magnitude
/// by half the divisor before a truncating divide; callers keep the sum within i256.
#[inline]
fn add_i256(a: (i64, u64, u64, u64), b: (i64, u64, u64, u64)) -> (i64, u64, u64, u64) {
    let (r3, c3) = a.3.overflowing_add(b.3);
    let (s2, c2a) = a.2.overflowing_add(b.2);
    let (r2, c2b) = s2.overflowing_add(c3 as u64);
    let (s1, c1a) = a.1.overflowing_add(b.1);
    let (r1, c1b) = s1.overflowing_add((c2a || c2b) as u64);
    let r0 = (a.0 as u64)
        .wrapping_add(b.0 as u64)
        .wrapping_add((c1a || c1b) as u64) as i64;
    (r0, r1, r2, r3)
}

/// Divide a 256-bit value by 10^scale_diff (scale_diff >= 1), rounding half away from zero. Biases
/// the magnitude by half the divisor (5 * 10^(scale_diff-1)) then truncates toward zero, which is
/// round-half-away. The biased magnitude stays within i256: a valid Decimal256 magnitude is < 10^76
/// and the bias is < 5 * 10^75, so the sum is < 1.5 * 10^76 < i256::MAX.
fn round_div_i256_pow10(
    w0: i64,
    w1: u64,
    w2: u64,
    w3: u64,
    scale_diff: u32,
) -> (i64, u64, u64, u64) {
    let neg = w0 < 0;
    let abs = if neg {
        negate_i256(w0, w1, w2, w3)
    } else {
        (w0, w1, w2, w3)
    };
    // half divisor = 5 * 10^(scale_diff - 1) = (10^scale_diff) / 2
    let half = mul_i256_pow10(0, 0, 0, 5, scale_diff - 1);
    let biased = add_i256(abs, half);
    let q = div_i256_pow10(biased.0, biased.1, biased.2, biased.3, scale_diff);
    if neg {
        negate_i256(q.0, q.1, q.2, q.3)
    } else {
        q
    }
}

/// Convert decoded fixed integer values (BYTE/SHORT/INT/LONG) to a target decimal type.
/// Widens each value from the source size to the target decimal size, then multiplies
/// by 10^scale. Iterates backwards when the target is wider to avoid overwriting unread data.
fn convert_fixed_to_decimal(
    data: &mut AcVec<u8>,
    src_tag: ColumnTypeTag,
    dst_tag: ColumnTypeTag,
    leading_nulls: usize,
    dst_scale: u8,
    dst_precision: u8,
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
            // checked_pow rejects a corrupt/tampered scale (as the decimal->decimal path does).
            // For Decimal8..64, max scale is 18, so 10^18 fits i64 today; this guards
            // against future scale-bound changes.
            let Some(factor) = 10i64.checked_pow(dst_scale as u32) else {
                return Err(fmt_err!(
                    InvalidLayout,
                    "decimal scale {} exceeds i64 range for target {:?}",
                    dst_scale,
                    dst_tag
                ));
            };
            // Clamp to the target precision, not just the byte width: a precision can be tighter
            // than its width admits (e.g. DECIMAL(2,0) is a Decimal8 whose i8 width holds 127 but
            // precision admits only 99). |scaled| < 10^precision is the stricter bound and also
            // guarantees the result fits dst_size, mirroring the decimal->decimal i64 path. For an
            // i64-backed target precision <= 18, so 10^precision fits i64.
            let Some(limit) = 10i64.checked_pow(dst_precision as u32) else {
                return Err(fmt_err!(
                    InvalidLayout,
                    "decimal precision {} exceeds i64 range for target {:?}",
                    dst_precision,
                    dst_tag
                ));
            };
            let null_sentinel = null_i64_for_decimal(dst_tag);
            if dst_size >= src_size {
                for i in (0..count).rev() {
                    let val = unsafe { read_le_i64_at(ptr, i, src_size) };
                    let scaled = if i < leading_nulls || is_int_null(val, src_tag) {
                        null_sentinel
                    } else {
                        scale_or_null_i64(val, factor, limit, null_sentinel)
                    };
                    unsafe { write_le_i64_at(ptr, i, dst_size, scaled) };
                }
            } else {
                for i in 0..count {
                    let val = unsafe { read_le_i64_at(ptr, i, src_size) };
                    let scaled = if i < leading_nulls || is_int_null(val, src_tag) {
                        null_sentinel
                    } else {
                        scale_or_null_i64(val, factor, limit, null_sentinel)
                    };
                    unsafe { write_le_i64_at(ptr, i, dst_size, scaled) };
                }
            }
        }
        // Target Decimal128: widen to i128, scale, write as (hi, lo).
        ColumnTypeTag::Decimal128 => {
            // checked_pow rejects a corrupt/tampered scale (as the decimal->decimal path does).
            let Some(factor) = 10i128.checked_pow(dst_scale as u32) else {
                return Err(fmt_err!(
                    InvalidLayout,
                    "decimal scale {} exceeds i128 range for Decimal128",
                    dst_scale
                ));
            };
            // Precision <= 38 for Decimal128, so 10^precision fits i128.
            let Some(limit) = 10i128.checked_pow(dst_precision as u32) else {
                return Err(fmt_err!(
                    InvalidLayout,
                    "decimal precision {} exceeds i128 range for Decimal128",
                    dst_precision
                ));
            };
            for i in (0..count).rev() {
                let val = unsafe { read_le_i64_at(ptr, i, src_size) };
                let offset = i * 16;
                // i64 widened to i128 multiplied by 10^scale can exceed i128
                // (e.g. i64::MAX * 10^38 overflows). Use checked_mul and emit
                // the Decimal128 NULL pair on overflow; a result within i128 that
                // still exceeds the target precision (|scaled| >= 10^precision) is
                // also NULLed, mirroring the i64 path in scale_or_null_i64.
                let scaled = if i < leading_nulls || is_int_null(val, src_tag) {
                    None
                } else {
                    (val as i128).checked_mul(factor)
                };
                match scaled {
                    Some(s) if s > -limit && s < limit => unsafe {
                        (ptr.add(offset) as *mut i64).write_unaligned((s >> 64) as i64);
                        (ptr.add(offset + 8) as *mut u64).write_unaligned(s as u64);
                    },
                    _ => unsafe {
                        (ptr.add(offset) as *mut i64).write_unaligned(i64::MIN);
                        (ptr.add(offset + 8) as *mut u64).write_unaligned(0);
                    },
                }
            }
        }
        // Target Decimal256: widen to 256-bit, scale, write as (w0, w1, w2, w3).
        ColumnTypeTag::Decimal256 => {
            // Precision <= 76 for Decimal256, so 10^precision fits i256.
            let limit = pow10_i256(dst_precision as u32)?;
            let limit_abs = (limit.0 as u64, limit.1, limit.2, limit.3);
            for i in (0..count).rev() {
                let val = unsafe { read_le_i64_at(ptr, i, src_size) };
                let offset = i * 32;
                // Sign-extended i64 multiplied by 10^scale can exceed i256 for
                // large scale + large |val| (e.g. i64::MAX * 10^76 overflows).
                // checked_mul_i256_pow10 returns None on overflow; emit the
                // Decimal256 NULL on overflow. A value within i256 that still
                // exceeds the target precision (|scaled| >= 10^precision) is also
                // NULLed, mirroring the Decimal128 path.
                let scaled = if i < leading_nulls || is_int_null(val, src_tag) {
                    None
                } else {
                    let sign = if val < 0 { u64::MAX } else { 0 };
                    checked_mul_i256_pow10(sign as i64, sign, sign, val as u64, dst_scale as u32)
                };
                match scaled {
                    Some(s) if !i256_abs_ge(s, limit_abs) => unsafe {
                        (ptr.add(offset) as *mut i64).write_unaligned(s.0);
                        (ptr.add(offset + 8) as *mut u64).write_unaligned(s.1);
                        (ptr.add(offset + 16) as *mut u64).write_unaligned(s.2);
                        (ptr.add(offset + 24) as *mut u64).write_unaligned(s.3);
                    },
                    _ => unsafe {
                        (ptr.add(offset) as *mut i64).write_unaligned(i64::MIN);
                        (ptr.add(offset + 8) as *mut u64).write_unaligned(0);
                        (ptr.add(offset + 16) as *mut u64).write_unaligned(0);
                        (ptr.add(offset + 24) as *mut u64).write_unaligned(0);
                    },
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

/// Scales `val` by `factor` (= 10^scale) and clamps the result to the target precision.
/// Returns `null_sentinel` when the i64 multiplication overflows or `|scaled| >= limit`
/// (`limit` = 10^precision). The precision bound is stricter than the destination byte
/// width (a precision never needs more digits than its width holds), so a value that
/// passes it also fits the target width and `write_le_i64_at` will not truncate. Without
/// this guard the low bytes silently truncate and produce a corrupted decimal (see issue:
/// INT 2_000_000 -> Decimal8 scale 2 stored as 0, and INT 100 -> DECIMAL(2,0) stored as 100).
#[inline]
fn scale_or_null_i64(val: i64, factor: i64, limit: i64, null_sentinel: i64) -> i64 {
    let Some(scaled) = val.checked_mul(factor) else {
        return null_sentinel;
    };
    if scaled > -limit && scaled < limit {
        scaled
    } else {
        null_sentinel
    }
}

fn fixed_tag_size(tag: ColumnTypeTag) -> usize {
    match tag {
        ColumnTypeTag::Byte | ColumnTypeTag::Boolean => 1,
        ColumnTypeTag::Short | ColumnTypeTag::Char => 2,
        ColumnTypeTag::Int | ColumnTypeTag::IPv4 | ColumnTypeTag::Float => 4,
        ColumnTypeTag::Long
        | ColumnTypeTag::Double
        | ColumnTypeTag::Date
        | ColumnTypeTag::Timestamp => 8,
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

/// Outcome of validating a column-type conversion request.
#[derive(Clone, Copy)]
pub(super) enum DecodeAs {
    /// Decode using the target type directly: source and target share a physical
    /// representation, so the page decoder produces the target width.
    Target,
    /// Decode using the source type; `post_convert` (or Java, for var/fixed swaps)
    /// performs the conversion after decode.
    Source,
}

/// Validates a fixed-to-fixed type conversion request (ALTER COLUMN TYPE)
/// and decides what type the page-decode dispatch should use.
///
/// The output buffer is sized for the target type. Some conversions can be
/// decoded directly into the target representation (`Target`); others require
/// the source physical type during decode because encoding-specific decoders
/// (e.g. DeltaBinaryPacked) cannot cross physical type boundaries — those
/// return `Source` and rely on `post_convert` afterwards.
///
/// Returns `None` when the conversion is not supported.
pub(super) fn plan_decode_conversion(
    src_tag: ColumnTypeTag,
    dst_tag: ColumnTypeTag,
) -> Option<DecodeAs> {
    use ColumnTypeTag::*;
    match (src_tag, dst_tag) {
        // Int32-family widening/narrowing (Byte/Short/Int share Int32 physical).
        // Safe for all encodings: decoder produces target width directly.
        (Byte, Short | Int)
        | (Short, Int | Byte)
        | (Int, Short | Byte)
        // Int64-family reinterpretation (Long/Date/Timestamp share Int64 physical).
        | (Long | Date | Timestamp, Long)
        | (Long, Timestamp | Date)
        // Byte/Short -> Long/Timestamp: Byte/Short have no in-band null sentinel, so the
        // i32->i64 widening decode arm (filling nulls::LONG for def-level=0 rows) is
        // byte-identical to decoding at the source width and widening in post_convert.
        // (-> Date stays Source below: no DeltaBinaryPacked Date decode arm exists yet.)
        | (Byte | Short, Long | Timestamp)
        // Float/Double -> int/time: the FloatToIntRangeCheckConverter decode arms map
        // NaN / out-of-range to the target null sentinel with the same range constants
        // and LOWER_STRICT flag that post_convert uses, so decoding straight to the
        // target is byte-identical. DeltaBinaryPacked is impossible for FLOAT/DOUBLE
        // physical, so Plain + dictionary cover every encoding.
        | (Float | Double, Byte | Short | Int | Long | Date | Timestamp) => {
            Some(DecodeAs::Target)
        }

        // Cross-physical int (Int32 <-> Int64) and cross-family numeric:
        // keep source type for decode; post_convert converts.
        // Int is sentinel-bearing (i32::MIN), so a single-pass i32->i64 widening arm
        // (which has no in-band sentinel check) would diverge from the native ALTER on
        // a stored i32::MIN; keep Source so post_convert maps the sentinel to i64::MIN.
        (Int, Long | Date | Timestamp)
        // Byte/Short -> Date: no DeltaBinaryPacked Date decode arm (see Target block).
        | (Byte | Short, Date)
        | (Long | Date | Timestamp, Byte | Short | Int)
        | (Byte | Short | Int | Long | Date | Timestamp, Float | Double)
        | (Float, Double)
        | (Double, Float)
        // Date <-> Timestamp and Timestamp nano <-> micro need post-decode scaling.
        | (Date, Timestamp)
        | (Timestamp, Date)
        | (Timestamp, Timestamp)
        // Fixed -> Boolean: decode at source width, post_convert contracts to 1 byte
        // so that null sentinels (i32::MIN, i64::MIN, NaN) map to 0 (false).
        | (Byte | Short | Int | Long | Date | Timestamp | Float | Double, Boolean)
        // Boolean -> fixed: decode 1-byte booleans, post_convert expands.
        | (Boolean, Byte | Short | Int | Long | Float | Double | Date | Timestamp)
        // Fixed -> var-size (VARCHAR, STRING): post_convert produces var-size output.
        | (Byte | Short | Int | Long | Float | Double | Date | Timestamp
            | Boolean | IPv4 | Uuid | Char,
            Varchar | String) => Some(DecodeAs::Source),

        // Var -> fixed-size: decode as source var type; Java converts after decode.
        (Varchar | String, dst) if is_var_to_fixed_target(dst) => Some(DecodeAs::Source),

        // String / Varchar / Symbol -> String / Varchar: parquet stores all three as
        // BYTE_ARRAY (UTF-8), so just remap to the target type and decode directly.
        (String | Varchar | Symbol, String | Varchar) => Some(DecodeAs::Target),

        // Array pass-through: same element type and dimensions.
        (Array, Array) => Some(DecodeAs::Target),

        // Decimal -> Decimal: different precision/scale.
        // Widening / same width: the decoder sign-extends to the target width during decode, then
        // post_convert rescales and clamps out-of-range values to NULL.
        // Narrowing (target physically smaller): keep the SOURCE width during decode - truncating to
        // the target width here would corrupt the raw value before it could be rescaled - and let
        // post_convert rescale, range-check the target precision, and narrow.
        (
            s @ (Decimal8 | Decimal16 | Decimal32 | Decimal64 | Decimal128 | Decimal256),
            d @ (Decimal8 | Decimal16 | Decimal32 | Decimal64 | Decimal128 | Decimal256),
        ) => {
            if decimal_tag_size(d) < decimal_tag_size(s) {
                Some(DecodeAs::Source)
            } else {
                Some(DecodeAs::Target)
            }
        }

        // Fixed integer -> Decimal: keep source type for decode;
        // post_convert widens and scales by 10^(target_scale).
        (Byte | Short | Int | Long, dst) if is_decimal_tag(dst) => Some(DecodeAs::Source),

        _ => None,
    }
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
pub(super) fn decompress_varchar_slice_dict<'bufs>(
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
        // The grow-only resize_decompress_buffer won't zero a reused pool buffer;
        // clear so a malformed under-filling dict page can't expose stale tail bytes
        // (aux entries retain pointers into this dict for the whole column-chunk decode).
        buf.clear();
        resize_decompress_buffer(&mut buf, dict_page.uncompressed_size)?;
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
    pub fn row_group_column_has_encoding(
        &self,
        row_group_index: u32,
        column_index: u32,
        parquet_encoding: i32,
    ) -> ParquetResult<bool> {
        if row_group_index >= self.row_group_count {
            return Err(fmt_err!(
                InvalidLayout,
                "row group index {} out of range [0,{})",
                row_group_index,
                self.row_group_count
            ));
        }
        if column_index >= self.col_count {
            return Err(fmt_err!(
                InvalidLayout,
                "column index {} out of range [0,{})",
                column_index,
                self.col_count
            ));
        }

        let row_group = &self.metadata.row_groups[row_group_index as usize];
        let column = &row_group.columns()[column_index as usize];
        Ok(column
            .column_encoding()
            .iter()
            .any(|encoding| encoding.0 == parquet_encoding))
    }

    pub fn decode_row_group(
        &self,
        ctx: &mut DecodeContext,
        row_group_bufs: &mut RowGroupBuffers,
        columns: &[(ParquetColumnIndex, ColumnType)],
        row_group_index: u32,
        row_group_lo: u32,
        row_group_hi: u32,
    ) -> ParquetResult<usize> {
        // Release the varchar-slice reuse pool and scratch vecs on every exit
        // path, including the error returns below: buffers stranded in the
        // context after a failed decode are invisible to the Java cache budget.
        let mut ctx_guard = VarcharSliceBufGuard::new(ctx);
        let ctx = ctx_guard.ctx();
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
            if column_idx >= self.col_count as usize {
                return Err(fmt_err!(
                    InvalidType,
                    "column index {} out of range [0,{})",
                    column_idx,
                    self.col_count
                ));
            }
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
                // Fixed-to-fixed type conversion (ALTER COLUMN TYPE). The output
                // buffer is sized for the target type; the decode dispatch reads the
                // source physical type from parquet and either produces the target
                // width directly or relies on post_convert afterwards.
                match plan_decode_conversion(src_tag, to_column_type.tag()) {
                    Some(DecodeAs::Target) => column_type = to_column_type,
                    Some(DecodeAs::Source) => {} // post_convert handles the conversion
                    None => {
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

            let col_info = QdbMetaCol { column_type, column_top, format, ascii, id: None };
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

            // Number of this chunk's rows that fall in the column-top prefix. For a source
            // type with no in-band null sentinel (BYTE/SHORT/CHAR), these are its only nulls,
            // and post_convert stamps the target sentinel over them.
            let leading_nulls = column_top
                .saturating_sub(accumulated_size + row_group_lo as usize)
                .min(row_group_hi.saturating_sub(row_group_lo) as usize);
            // Surface the count to Java (read via chunkColumnTopOffset) for lazy fixed->var
            // conversions, where the source has no in-band null and Java must emit NULL here.
            column_chunk_bufs.column_top = leading_nulls;

            // Post-decode conversions that cannot be handled by the decode dispatch.
            post_convert(
                original_column_type,
                to_column_type,
                leading_nulls,
                column_chunk_bufs,
            )?;
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
        // Release the varchar-slice reuse pool and scratch vecs on every exit
        // path, including the error returns below: buffers stranded in the
        // context after a failed decode are invisible to the Java cache budget.
        let mut ctx_guard = VarcharSliceBufGuard::new(ctx);
        let ctx = ctx_guard.ctx();
        if row_group_index >= self.row_group_count {
            return Err(fmt_err!(
                InvalidLayout,
                "row group index {} out of range [0,{})",
                row_group_index,
                self.row_group_count
            ));
        }

        let output_count = if FILL_NULLS {
            row_group_hi.saturating_sub(row_group_lo) as usize
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
            if column_idx >= self.col_count as usize {
                return Err(fmt_err!(
                    InvalidType,
                    "column index {} out of range [0,{})",
                    column_idx,
                    self.col_count
                ));
            }
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
                match plan_decode_conversion(src_tag, to_column_type.tag()) {
                    Some(DecodeAs::Target) => column_type = to_column_type,
                    Some(DecodeAs::Source) => {} // post_convert handles the conversion
                    None => {
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

            let col_info = QdbMetaCol { column_type, column_top, format, ascii, id: None };

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

            // Column-top nulls for a no-sentinel source must be stamped with the target
            // sentinel. column_top here is partition-absolute, so make it window-relative the
            // same way as the non-filtered path above. rows_filter is window-relative and
            // ascending and the output preserves order, so the matched column-top rows are a
            // contiguous leading prefix of the (possibly compacted) buffer.
            let window_column_top = column_top
                .saturating_sub(accumulated_size + row_group_lo as usize)
                .min(row_group_hi.saturating_sub(row_group_lo) as usize);
            let leading_nulls = if FILL_NULLS {
                window_column_top
            } else {
                rows_filter.partition_point(|&r| (r as usize) < window_column_top)
            };
            column_chunk_bufs.column_top = leading_nulls;
            // Post-decode conversions that cannot be handled by the decode dispatch.
            post_convert(
                original_column_type,
                to_column_type,
                leading_nulls,
                column_chunk_bufs,
            )?;
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
        // allocation per column chunk. Defensive clear: the end-of-chunk append and the
        // row-group-level VarcharSliceBufGuard normally leave both empty already.
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

        column_chunk_bufs.refresh_ptrs()?;
        if FILL_NULLS {
            Ok(row_group_hi.saturating_sub(row_group_lo))
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
        // allocation per column chunk. Defensive clear: the end-of-chunk append and the
        // row-group-level VarcharSliceBufGuard normally leave both empty already.
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

        column_chunk_bufs.refresh_ptrs()?;
        Ok(row_count)
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

    pub(crate) fn all_values_absent_from_bloom(
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
    pub(crate) fn all_values_outside_min_max_with_stats(
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
    pub(crate) fn value_outside_range(
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
            id: None,
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

    /// post_convert must reject pairs that are not handled by an explicit
    /// conversion arm and are not recognised no-ops. The two upstream gates
    /// (SQL ALTER's columnConversionSupport matrix and plan_decode_conversion)
    /// already prevent these pairs from reaching post_convert today; the
    /// explicit catch-all guarantees a loud failure if either gate loosens
    /// in the future.
    #[test]
    fn post_convert_rejects_unsupported_pairs() {
        use crate::allocator::{AcVec, TestAllocatorState};
        use crate::parquet::tests::ColumnTypeTagExt;
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        // Pairs that have never been wired up in either gate: their entry to
        // post_convert would represent a real bug.
        let unsupported = [
            (ColumnTypeTag::Boolean, ColumnTypeTag::Char),
            (ColumnTypeTag::Char, ColumnTypeTag::Byte),
            (ColumnTypeTag::Char, ColumnTypeTag::Short),
            (ColumnTypeTag::Char, ColumnTypeTag::Int),
            (ColumnTypeTag::Char, ColumnTypeTag::Long),
            (ColumnTypeTag::Char, ColumnTypeTag::Float),
            (ColumnTypeTag::Char, ColumnTypeTag::Double),
        ];
        for (src, dst) in unsupported {
            let mut bufs = ColumnChunkBuffers {
                data_size: 0,
                data_ptr: std::ptr::null_mut(),
                data_vec: AcVec::new_in(allocator.clone()),
                aux_size: 0,
                aux_ptr: std::ptr::null_mut(),
                aux_vec: AcVec::new_in(allocator.clone()),
                page_buffers_size: 0,
                page_buffers: Vec::new(),
                column_top: 0,
                page_buffers_charged: 0,
                page_buffers_counted: 0,
            };
            let err = post_convert(src.into_type(), dst.into_type(), 0, &mut bufs).unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("post_convert: unsupported conversion"),
                "unexpected error for {src:?} -> {dst:?}: {msg}"
            );
        }
    }

    /// Same-physical and identity pairs reach post_convert legitimately when
    /// plan_decode_conversion chose DecodeAs::Target. Each must be accepted
    /// without an error.
    #[test]
    fn post_convert_accepts_known_no_op_pairs() {
        use crate::allocator::{AcVec, TestAllocatorState};
        use crate::parquet::tests::ColumnTypeTagExt;
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let no_ops = [
            (ColumnTypeTag::Byte, ColumnTypeTag::Short),
            (ColumnTypeTag::Short, ColumnTypeTag::Int),
            (ColumnTypeTag::Int, ColumnTypeTag::Byte),
            (ColumnTypeTag::Long, ColumnTypeTag::Date),
            (ColumnTypeTag::Date, ColumnTypeTag::Long),
            (ColumnTypeTag::Long, ColumnTypeTag::Timestamp),
            (ColumnTypeTag::Timestamp, ColumnTypeTag::Long),
            // Flipped to DecodeAs::Target: decoded straight to the target, no-op here.
            (ColumnTypeTag::Byte, ColumnTypeTag::Long),
            (ColumnTypeTag::Short, ColumnTypeTag::Timestamp),
            (ColumnTypeTag::Float, ColumnTypeTag::Int),
            (ColumnTypeTag::Float, ColumnTypeTag::Long),
            (ColumnTypeTag::Double, ColumnTypeTag::Short),
            (ColumnTypeTag::Double, ColumnTypeTag::Timestamp),
            (ColumnTypeTag::String, ColumnTypeTag::Varchar),
            (ColumnTypeTag::Varchar, ColumnTypeTag::String),
            (ColumnTypeTag::Symbol, ColumnTypeTag::Varchar),
            (ColumnTypeTag::Int, ColumnTypeTag::Int),
            (ColumnTypeTag::Boolean, ColumnTypeTag::Boolean),
        ];
        for (src, dst) in no_ops {
            let mut bufs = ColumnChunkBuffers {
                data_size: 0,
                data_ptr: std::ptr::null_mut(),
                data_vec: AcVec::new_in(allocator.clone()),
                aux_size: 0,
                aux_ptr: std::ptr::null_mut(),
                aux_vec: AcVec::new_in(allocator.clone()),
                page_buffers_size: 0,
                page_buffers: Vec::new(),
                column_top: 0,
                page_buffers_charged: 0,
                page_buffers_counted: 0,
            };
            post_convert(src.into_type(), dst.into_type(), 0, &mut bufs)
                .unwrap_or_else(|e| panic!("expected no-op for {src:?} -> {dst:?}, got {e}"));
        }
    }

    /// DATE (ms), TIMESTAMP (μs) and TIMESTAMP_NS (ns) conversions are pure
    /// power-of-1000 rescales. post_convert folds the cross-unit factor into a
    /// single pass (DATE → TIMESTAMP_NS is ×1_000_000, not ×1000 then ×1000) while
    /// preserving the LONG null sentinel and wrapping on i64 overflow like the
    /// native converters. Each non-null result is cross-checked against the prior
    /// two-pass formula to prove the fold is behaviour-preserving.
    #[test]
    fn post_convert_scales_time_units_in_one_pass() {
        use crate::allocator::{AcVec, TestAllocatorState};

        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        const NULL: i64 = i64::MIN;
        let date = ColumnType::new(ColumnTypeTag::Date, 0);
        let ts_us = ColumnType::new(ColumnTypeTag::Timestamp, 0);
        let ts_ns = ColumnType::new(ColumnTypeTag::Timestamp, QDB_TIMESTAMP_NS_COLUMN_TYPE_FLAG);

        let make = |vals: &[i64]| -> ColumnChunkBuffers {
            let mut data_vec = AcVec::new_in(allocator.clone());
            for &v in vals {
                data_vec.extend_from_slice(&v.to_ne_bytes()).unwrap();
            }
            ColumnChunkBuffers {
                data_size: 0,
                data_ptr: std::ptr::null_mut(),
                data_vec,
                aux_size: 0,
                aux_ptr: std::ptr::null_mut(),
                aux_vec: AcVec::new_in(allocator.clone()),
                page_buffers_size: 0,
                page_buffers: Vec::new(),
                column_top: 0,
                page_buffers_charged: 0,
                page_buffers_counted: 0,
            }
        };
        let read = |bufs: &ColumnChunkBuffers| -> Vec<i64> {
            bufs.data_vec
                .as_slice()
                .chunks_exact(8)
                .map(|c| i64::from_ne_bytes(c.try_into().unwrap()))
                .collect()
        };

        // DATE -> TIMESTAMP_NS: ×1_000_000 in one pass. Includes a value whose
        // ×1_000_000 overflows i64 (must wrap, not null) and the NULL sentinel.
        let ms = [1_592_222_400_000i64, 0, 253_402_300_799_999, NULL];
        let mut b = make(&ms);
        post_convert(date, ts_ns, 0, &mut b).unwrap();
        let got = read(&b);
        for (i, &v) in ms.iter().enumerate() {
            // Prior behaviour: ×1000 (ms->μs) then ×1000 (μs->ns).
            let expected = if v == NULL {
                NULL
            } else {
                v.wrapping_mul(1000).wrapping_mul(1000)
            };
            assert_eq!(got[i], expected, "DATE->TIMESTAMP_NS row {i} (v={v})");
        }
        // The overflowing row wraps to the native ×1_000_000 product, not NULL.
        assert_eq!(got[2], 253_402_300_799_999i64.wrapping_mul(1_000_000));
        assert_ne!(got[2], NULL);

        // TIMESTAMP_NS -> DATE: ÷1_000_000 in one pass, truncating toward zero.
        let ns = [1_592_222_400_123_456_789i64, 1_999_999, -1_999_999, NULL];
        let mut b = make(&ns);
        post_convert(ts_ns, date, 0, &mut b).unwrap();
        let got = read(&b);
        for (i, &v) in ns.iter().enumerate() {
            // Prior behaviour: ÷1000 (ns->μs) then ÷1000 (μs->ms).
            let expected = if v == NULL { NULL } else { v / 1000 / 1000 };
            assert_eq!(got[i], expected, "TIMESTAMP_NS->DATE row {i} (v={v})");
        }
        assert_eq!(got[0], 1_592_222_400_123); // sub-ms truncated
        assert_eq!(got[1], 1);
        assert_eq!(got[2], -1);

        // Single-step crosses (already one pass) keep scaling through the same arm.
        let mut b = make(&[5, NULL]); // DATE -> TIMESTAMP (μs): ×1000
        post_convert(date, ts_us, 0, &mut b).unwrap();
        assert_eq!(read(&b), vec![5000, NULL]);
        let mut b = make(&[7, NULL]); // TIMESTAMP (μs) -> TIMESTAMP_NS: ×1000
        post_convert(ts_us, ts_ns, 0, &mut b).unwrap();
        assert_eq!(read(&b), vec![7000, NULL]);
        let mut b = make(&[7654, NULL]); // TIMESTAMP_NS -> TIMESTAMP (μs): ÷1000
        post_convert(ts_ns, ts_us, 0, &mut b).unwrap();
        assert_eq!(read(&b), vec![7, NULL]);
        let mut b = make(&[42, NULL]); // same unit: no-op
        post_convert(ts_us, ts_us, 0, &mut b).unwrap();
        assert_eq!(read(&b), vec![42, NULL]);
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

    /// Pins the load-bearing `buf.clear()` in `decompress_varchar_slice_dict`:
    /// because `resize_decompress_buffer` is grow-only it never re-zeroes a reused
    /// pool buffer, so a malformed dict page whose codec under-fills the buffer
    /// would otherwise expose stale bytes from a previous page -- and the varchar
    /// aux entries hold pointers into this dict buffer for the whole column-chunk
    /// decode. Snappy writes only the real decompressed length and leaves the rest
    /// of the output untouched, so an over-claimed `uncompressed_size` under-fills.
    #[test]
    fn compressed_dict_clears_pooled_buffer_before_decompress() {
        let payload: Vec<u8> = (0..16u8).collect();
        let compressed = snappy_compress(&payload);

        let mut persistent: Vec<Vec<u8>> = Vec::new();
        // A dirty pooled buffer longer than the page's uncompressed_size: the
        // grow-only resize truncates it in place without re-zeroing, so absent the
        // clear() its tail would still read back as these stale 0xAB bytes.
        let mut pool: Vec<Vec<u8>> = vec![vec![0xABu8; 64]];

        // uncompressed_size (32) over-claims the real decompressed length (16), so
        // the codec under-fills: it writes 16 bytes and never touches the last 16.
        let dict_page = make_snappy_dict(&compressed, 32, 4);
        let page = decompress_varchar_slice_dict(dict_page, &mut persistent, &mut pool).unwrap();

        assert_eq!(page.buffer.len(), 32);
        assert_eq!(&page.buffer[..payload.len()], payload.as_slice());
        assert!(
            page.buffer[payload.len()..].iter().all(|&b| b == 0),
            "the under-filled tail must be zeroed by clear(), not stale pool bytes: {:?}",
            &page.buffer[payload.len()..]
        );
    }
}

#[cfg(test)]
mod decimal_convert_tests {
    use super::*;
    use crate::allocator::{AcVec, QdbAllocator, TestAllocatorState};

    fn buf(allocator: &QdbAllocator, bytes: &[u8]) -> AcVec<u8> {
        let mut v = AcVec::new_in(allocator.clone());
        v.extend_from_slice(bytes).unwrap();
        v
    }

    // --- Decimal8/16/32/64 (i64-backed) helpers ---

    fn le_small(vals: &[i64], size: usize) -> Vec<u8> {
        let mut out = Vec::new();
        for &x in vals {
            out.extend_from_slice(&x.to_le_bytes()[..size]);
        }
        out
    }

    fn read_small(v: &[u8], idx: usize, size: usize) -> i64 {
        let o = idx * size;
        match size {
            1 => v[o] as i8 as i64,
            2 => i16::from_le_bytes(v[o..o + 2].try_into().unwrap()) as i64,
            4 => i32::from_le_bytes(v[o..o + 4].try_into().unwrap()) as i64,
            _ => i64::from_le_bytes(v[o..o + 8].try_into().unwrap()),
        }
    }

    /// Decimal16 (size 2, max precision 4). Covers lossless up/down, lossy down, precision and
    /// width overflow, same-scale precision reduction, and source-NULL passthrough -- all in one
    /// buffer so per-row independence is exercised too.
    #[test]
    fn decimal16_unrepresentable_values_become_null() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let null = i16::MIN as i64;
        // [12.0->scale1 lossless, 5000(scale0)->scale1 overflows prec4, 125(=12.5)->scale0 lossy,
        //  120(=12.0)->scale0 lossless, NULL]
        // We pick a single (src_scale,dst_scale) per buffer, so split into two buffers.

        // Scale up 0 -> 1, precision 4. 12 -> 120; 5000 -> 50000 exceeds precision/width -> null.
        let mut up = buf(&allocator, &le_small(&[12, 5000, null], 2));
        convert_decimal_in_place(&mut up, ColumnTypeTag::Decimal16, 0, 1, 4).unwrap();
        assert_eq!(read_small(up.as_slice(), 0, 2), 120);
        assert_eq!(read_small(up.as_slice(), 1, 2), null);
        assert_eq!(read_small(up.as_slice(), 2, 2), null); // NULL stays NULL

        // Scale down 1 -> 0, precision 4. 120(=12.0)->12 exact; 125(=12.5)->rounds half away to 13.
        let mut down = buf(&allocator, &le_small(&[120, 125], 2));
        convert_decimal_in_place(&mut down, ColumnTypeTag::Decimal16, 1, 0, 4).unwrap();
        assert_eq!(read_small(down.as_slice(), 0, 2), 12);
        assert_eq!(read_small(down.as_slice(), 1, 2), 13);

        // Same scale, precision reduced to 3 (limit 1000): 1234 fits Decimal16 width but exceeds
        // precision 3 -> null; 999 survives.
        let mut prec = buf(&allocator, &le_small(&[1234, 999], 2));
        convert_decimal_in_place(&mut prec, ColumnTypeTag::Decimal16, 0, 0, 3).unwrap();
        assert_eq!(read_small(prec.as_slice(), 0, 2), null);
        assert_eq!(read_small(prec.as_slice(), 1, 2), 999);
    }

    // --- Decimal128 helpers. Layout: [hi: i64 LE @0, lo: u64 LE @8]. ---

    fn d128_bytes(val: i128) -> [u8; 16] {
        let mut b = [0u8; 16];
        b[0..8].copy_from_slice(&((val >> 64) as i64).to_le_bytes());
        b[8..16].copy_from_slice(&(val as u64).to_le_bytes());
        b
    }

    fn read_d128(v: &[u8], idx: usize) -> i128 {
        let o = idx * 16;
        let hi = i64::from_le_bytes(v[o..o + 8].try_into().unwrap());
        let lo = u64::from_le_bytes(v[o + 8..o + 16].try_into().unwrap());
        ((hi as i128) << 64) | (lo as i128)
    }

    fn d128_null() -> i128 {
        (i64::MIN as i128) << 64
    }

    #[test]
    fn decimal128_unrepresentable_values_become_null() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        // Scale up 4 -> 8, precision 38. A ~37-digit value * 10^4 overflows precision 38 -> null;
        // a small value scales cleanly.
        let big: i128 = 9_000_000_000_000_000_000_000_000_000_000_000_000; // 37 digits
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&d128_bytes(big));
        bytes.extend_from_slice(&d128_bytes(12_3456)); // 12.3456 at scale 4
        bytes.extend_from_slice(&d128_bytes(d128_null()));
        let mut up = buf(&allocator, &bytes);
        convert_decimal_in_place(&mut up, ColumnTypeTag::Decimal128, 4, 8, 38).unwrap();
        assert_eq!(read_d128(up.as_slice(), 0), d128_null()); // overflow -> null
        assert_eq!(read_d128(up.as_slice(), 1), 12_3456 * 10_000); // 12.34560000
        assert_eq!(read_d128(up.as_slice(), 2), d128_null()); // NULL stays NULL

        // Scale down 4 -> 2, precision 38. Lossy digits round half away from zero; exact -> value.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&d128_bytes(12_3456)); // 12.3456 -> scale 2 rounds half away to 12.35
        bytes.extend_from_slice(&d128_bytes(12_3400)); // 12.3400 -> 12.34 exact
        let mut down = buf(&allocator, &bytes);
        convert_decimal_in_place(&mut down, ColumnTypeTag::Decimal128, 4, 2, 38).unwrap();
        assert_eq!(read_d128(down.as_slice(), 0), 1235);
        assert_eq!(read_d128(down.as_slice(), 1), 1234);
    }

    // --- Decimal256 helpers. Layout: [w0(hi) i64 @0, w1 @8, w2 @16, w3(lo) @24], LE. ---

    fn d256_from_i128(val: i128) -> [u8; 32] {
        let sign: u64 = if val < 0 { u64::MAX } else { 0 };
        let mut b = [0u8; 32];
        b[0..8].copy_from_slice(&(sign as i64).to_le_bytes()); // w0 (bits 192-255)
        b[8..16].copy_from_slice(&sign.to_le_bytes()); // w1 (bits 128-191)
        b[16..24].copy_from_slice(&((val >> 64) as u64).to_le_bytes()); // w2 (bits 64-127)
        b[24..32].copy_from_slice(&(val as u64).to_le_bytes()); // w3 (bits 0-63)
        b
    }

    fn d256_null() -> [u8; 32] {
        let mut b = [0u8; 32];
        b[0..8].copy_from_slice(&i64::MIN.to_le_bytes());
        b
    }

    #[test]
    fn decimal256_unrepresentable_values_become_null() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        // Same scale, precision reduced to 5 (limit 100_000). 200_000 exceeds precision 5 -> null;
        // 99_999 survives. Exercises the i256 magnitude clamp (i256_abs_ge / pow10_i256).
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&d256_from_i128(200_000));
        bytes.extend_from_slice(&d256_from_i128(99_999));
        bytes.extend_from_slice(&d256_from_i128(-99_999));
        bytes.extend_from_slice(&d256_null());
        let mut prec = buf(&allocator, &bytes);
        convert_decimal_in_place(&mut prec, ColumnTypeTag::Decimal256, 0, 0, 5).unwrap();
        assert_eq!(&prec.as_slice()[0..32], &d256_null()); // exceeds precision -> null
        assert_eq!(&prec.as_slice()[32..64], &d256_from_i128(99_999));
        assert_eq!(&prec.as_slice()[64..96], &d256_from_i128(-99_999)); // negative magnitude
        assert_eq!(&prec.as_slice()[96..128], &d256_null()); // NULL stays NULL

        // Scale-down 2 -> 0 rounds half away from zero: 1250(=12.50) -> 13; 1234(=12.34) -> 12.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&d256_from_i128(1250));
        bytes.extend_from_slice(&d256_from_i128(1234));
        let mut down = buf(&allocator, &bytes);
        convert_decimal_in_place(&mut down, ColumnTypeTag::Decimal256, 2, 0, 40).unwrap();
        assert_eq!(&down.as_slice()[0..32], &d256_from_i128(13));
        assert_eq!(&down.as_slice()[32..64], &d256_from_i128(12));
    }

    /// Narrowing from an i64-backed source (Decimal64 -> Decimal16). The decoder kept the source
    /// width, so the input buffer is 8 bytes/elem and the output is 2 bytes/elem.
    #[test]
    fn narrowing_i64_source_clamps_and_shrinks() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let dst_null = i16::MIN as i64;

        // Same scale 2, target precision 4. 1234(=12.34) fits; 123456(=1234.56) exceeds prec 4 ->
        // null; source NULL (i64::MIN) -> dst null.
        let mut b = buf(&allocator, &le_small(&[1234, 123456, i64::MIN], 8));
        convert_decimal_narrowing(
            &mut b,
            ColumnTypeTag::Decimal64,
            ColumnTypeTag::Decimal16,
            2,
            2,
            4,
        )
        .unwrap();
        assert_eq!(b.as_slice().len(), 3 * 2); // shrunk
        assert_eq!(read_small(b.as_slice(), 0, 2), 1234);
        assert_eq!(read_small(b.as_slice(), 1, 2), dst_null);
        assert_eq!(read_small(b.as_slice(), 2, 2), dst_null);

        // Scale down 4 -> 2, precision 4. 123400(=12.3400)->1234 exact; 123456(=12.3456) rounds
        // half away to 12.35 (1235).
        let mut b = buf(&allocator, &le_small(&[123400, 123456], 8));
        convert_decimal_narrowing(
            &mut b,
            ColumnTypeTag::Decimal64,
            ColumnTypeTag::Decimal16,
            4,
            2,
            4,
        )
        .unwrap();
        assert_eq!(read_small(b.as_slice(), 0, 2), 1234);
        assert_eq!(read_small(b.as_slice(), 1, 2), 1235);
    }

    /// Narrowing from Decimal128 to Decimal32 (16 -> 4 bytes/elem).
    #[test]
    fn narrowing_i128_source_clamps_and_shrinks() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let dst_null = i32::MIN as i64;

        // Same scale 0, precision 9. 123456789 fits; 12345678901 exceeds prec 9 -> null; NULL -> null.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&d128_bytes(123_456_789));
        bytes.extend_from_slice(&d128_bytes(12_345_678_901));
        bytes.extend_from_slice(&d128_bytes(d128_null()));
        let mut b = buf(&allocator, &bytes);
        convert_decimal_narrowing(
            &mut b,
            ColumnTypeTag::Decimal128,
            ColumnTypeTag::Decimal32,
            0,
            0,
            9,
        )
        .unwrap();
        assert_eq!(b.as_slice().len(), 3 * 4);
        assert_eq!(read_small(b.as_slice(), 0, 4), 123_456_789);
        assert_eq!(read_small(b.as_slice(), 1, 4), dst_null);
        assert_eq!(read_small(b.as_slice(), 2, 4), dst_null);
    }

    /// Narrowing from Decimal256 to Decimal64 (32 -> 8 bytes/elem), exercising the i256 read +
    /// precision clamp + lossy detection on the narrowing path.
    #[test]
    fn narrowing_i256_source_clamps_and_shrinks() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let dst_null = i64::MIN;

        // Same scale 0, precision 18. 123456 fits; 10^18 and -10^18 exceed prec 18 -> null; NULL.
        let big = 1_000_000_000_000_000_000i128; // 10^18
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&d256_from_i128(123_456));
        bytes.extend_from_slice(&d256_from_i128(big));
        bytes.extend_from_slice(&d256_from_i128(-big));
        bytes.extend_from_slice(&d256_null());
        let mut b = buf(&allocator, &bytes);
        convert_decimal_narrowing(
            &mut b,
            ColumnTypeTag::Decimal256,
            ColumnTypeTag::Decimal64,
            0,
            0,
            18,
        )
        .unwrap();
        assert_eq!(b.as_slice().len(), 4 * 8);
        assert_eq!(read_small(b.as_slice(), 0, 8), 123_456);
        assert_eq!(read_small(b.as_slice(), 1, 8), dst_null);
        assert_eq!(read_small(b.as_slice(), 2, 8), dst_null);
        assert_eq!(read_small(b.as_slice(), 3, 8), dst_null);

        // Scale down 2 -> 0 rounds half away from zero: 1250(=12.50)->13; 1234(=12.34)->12;
        // -560(=-5.60)->-6 (negative rounds away from zero).
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&d256_from_i128(1250));
        bytes.extend_from_slice(&d256_from_i128(1234));
        bytes.extend_from_slice(&d256_from_i128(-560));
        let mut b = buf(&allocator, &bytes);
        convert_decimal_narrowing(
            &mut b,
            ColumnTypeTag::Decimal256,
            ColumnTypeTag::Decimal64,
            2,
            0,
            18,
        )
        .unwrap();
        assert_eq!(read_small(b.as_slice(), 0, 8), 13);
        assert_eq!(read_small(b.as_slice(), 1, 8), 12);
        assert_eq!(read_small(b.as_slice(), 2, 8), -6);
    }

    /// Regression: integer->decimal must clamp to the target PRECISION, not just the destination
    /// byte width. A precision can be tighter than its width admits (DECIMAL(2,0) is a Decimal8
    /// whose i8 width holds 127 but precision admits only 99; DECIMAL(9,0) is a Decimal32 whose
    /// i32 width holds ~2.1e9 but precision admits only 999_999_999; DECIMAL(18,18) scales 1 to
    /// 10^18, which fits i64 but is 19 digits). The lazy parquet decoder must NULL these to match
    /// the native DecimalColumnTypeConverter, which rejects them via comparePrecision.
    #[test]
    fn fixed_to_decimal_i64_target_clamps_to_precision() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        // INT -> Decimal8(2,0): 100/-100 fit the i8 width but exceed precision 2 -> null; 99/-99/0 survive.
        let null8 = i8::MIN as i64;
        let mut b = buf(&allocator, &le_small(&[100, 99, -100, -99, 0], 4));
        convert_fixed_to_decimal(&mut b, ColumnTypeTag::Int, ColumnTypeTag::Decimal8, 0, 0, 2)
            .unwrap();
        assert_eq!(read_small(b.as_slice(), 0, 1), null8);
        assert_eq!(read_small(b.as_slice(), 1, 1), 99);
        assert_eq!(read_small(b.as_slice(), 2, 1), null8);
        assert_eq!(read_small(b.as_slice(), 3, 1), -99);
        assert_eq!(read_small(b.as_slice(), 4, 1), 0);

        // INT -> Decimal32(9,0): 1_500_000_000 fits the i32 width but exceeds precision 9 -> null;
        // 999_999_999 survives. Same byte width (4) as the INT source, so no resize.
        let null32 = i32::MIN as i64;
        let mut b = buf(&allocator, &le_small(&[1_500_000_000, 999_999_999], 4));
        convert_fixed_to_decimal(
            &mut b,
            ColumnTypeTag::Int,
            ColumnTypeTag::Decimal32,
            0,
            0,
            9,
        )
        .unwrap();
        assert_eq!(read_small(b.as_slice(), 0, 4), null32);
        assert_eq!(read_small(b.as_slice(), 1, 4), 999_999_999);

        // INT -> Decimal64(18,18): 1 and 2 scale to 10^18 / 2*10^18, both fit i64 (< i64::MAX) but
        // are 19 digits, exceeding precision 18 -> null; 0 stays 0. Only the precision clamp catches
        // these; the old byte-width-only guard stored them verbatim.
        let null64 = i64::MIN;
        let mut b = buf(&allocator, &le_small(&[1, 0, 2], 4));
        convert_fixed_to_decimal(
            &mut b,
            ColumnTypeTag::Int,
            ColumnTypeTag::Decimal64,
            0,
            18,
            18,
        )
        .unwrap();
        assert_eq!(read_small(b.as_slice(), 0, 8), null64);
        assert_eq!(read_small(b.as_slice(), 1, 8), 0);
        assert_eq!(read_small(b.as_slice(), 2, 8), null64);
    }

    /// INT -> Decimal128(20,19): 100 scales to 10^21 (22 digits, exceeds precision 20) -> null;
    /// 5 scales to 5*10^19 (within precision 20) -> stored. Both fit i128, so only the precision
    /// clamp distinguishes them.
    #[test]
    fn fixed_to_decimal128_target_clamps_to_precision() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut b = buf(&allocator, &le_small(&[100, 5, 0], 4));
        convert_fixed_to_decimal(
            &mut b,
            ColumnTypeTag::Int,
            ColumnTypeTag::Decimal128,
            0,
            19,
            20,
        )
        .unwrap();
        assert_eq!(read_d128(b.as_slice(), 0), d128_null());
        assert_eq!(read_d128(b.as_slice(), 1), 5i128 * 10i128.pow(19));
        assert_eq!(read_d128(b.as_slice(), 2), 0);
    }

    /// INT -> Decimal256(76,75): 50 scales to 5*10^76, which fits i256 (< i256::MAX ~5.78e76) but
    /// exceeds precision 76 (limit 10^76) -> null; 5 scales to 5*10^75 (< 10^76) -> stored. The
    /// overflow guard (checked_mul_i256_pow10) alone would store the 50 case; only the precision
    /// clamp NULLs it.
    #[test]
    fn fixed_to_decimal256_target_clamps_to_precision() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut b = buf(&allocator, &le_small(&[50, 5], 4));
        convert_fixed_to_decimal(
            &mut b,
            ColumnTypeTag::Int,
            ColumnTypeTag::Decimal256,
            0,
            75,
            76,
        )
        .unwrap();
        assert_eq!(&b.as_slice()[0..32], &d256_null());
        assert_ne!(&b.as_slice()[32..64], &d256_null());
    }

    /// Pins round-half-away-from-zero on decimal scale reduction across the i64 and i128 backings,
    /// including the exact-half boundary and negative magnitudes (which round away from zero), so a
    /// scale-down conversion never NULLs on a dropped fraction. Matches the native
    /// DecimalColumnTypeConverter, which uses Decimal256.round(targetScale, RoundingMode.HALF_UP).
    #[test]
    fn decimal_scale_down_rounds_half_away_from_zero() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        // Decimal64 scale 1 -> 0, precision 18. 2.5->3, -2.5->-3 (exact half rounds away);
        // 2.4->2, -2.4->-2; 0 stays 0; NULL stays NULL.
        let null = i64::MIN;
        let mut b = buf(&allocator, &le_small(&[25, -25, 24, -24, 0, null], 8));
        convert_decimal_in_place(&mut b, ColumnTypeTag::Decimal64, 1, 0, 18).unwrap();
        assert_eq!(read_small(b.as_slice(), 0, 8), 3);
        assert_eq!(read_small(b.as_slice(), 1, 8), -3);
        assert_eq!(read_small(b.as_slice(), 2, 8), 2);
        assert_eq!(read_small(b.as_slice(), 3, 8), -2);
        assert_eq!(read_small(b.as_slice(), 4, 8), 0);
        assert_eq!(read_small(b.as_slice(), 5, 8), null);

        // Decimal128 scale 2 -> 0, precision 38. 2.50->3, -2.50->-3 (exact half away); 2.49->2.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&d128_bytes(250));
        bytes.extend_from_slice(&d128_bytes(-250));
        bytes.extend_from_slice(&d128_bytes(249));
        let mut b = buf(&allocator, &bytes);
        convert_decimal_in_place(&mut b, ColumnTypeTag::Decimal128, 2, 0, 38).unwrap();
        assert_eq!(read_d128(b.as_slice(), 0), 3);
        assert_eq!(read_d128(b.as_slice(), 1), -3);
        assert_eq!(read_d128(b.as_slice(), 2), 2);
    }
}
