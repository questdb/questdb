use std::{cmp, io, mem, slice};

use crate::parquet::error::ParquetResult;
use crate::parquet_write::encoders::numeric::SimdEncodable;
use crate::parquet_write::file::WriteOptions;
use parquet2::compression::CompressionOptions;
use parquet2::encoding::ceil8;
use parquet2::encoding::hybrid_rle::{encode_bool, encode_u32};
use parquet2::encoding::uleb128;
use parquet2::encoding::Encoding;
use parquet2::metadata::Descriptor;
use parquet2::page::{DataPage, DataPageHeader, DataPageHeaderV1, DataPageHeaderV2};
use parquet2::schema::types::{PhysicalType, PrimitiveLogicalType, PrimitiveType};
use parquet2::statistics::{serialize_statistics, BinaryStatistics, ParquetStatistics, Statistics};
use parquet2::types::NativeType;
use parquet2::write::Version;

#[derive(Debug, Clone, Copy)]
pub struct MaxMin<T> {
    pub max: Option<T>,
    pub min: Option<T>,
}

impl<T: Copy + NativeType> Default for MaxMin<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Copy + NativeType> MaxMin<T> {
    pub fn new() -> Self {
        MaxMin { max: None, min: None }
    }

    #[inline(always)]
    pub fn update(&mut self, x: T) {
        self.max = Some(if let Some(max) = self.max {
            cmp::max_by(max, x, |x, y| x.ord(y))
        } else {
            x
        });
        self.min = Some(if let Some(min) = self.min {
            cmp::min_by(min, x, |x, y| x.ord(y))
        } else {
            x
        });
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SimdMaxMin<T> {
    pub max: T,
    pub min: T,
}

impl<T: Copy + SimdEncodable> SimdMaxMin<T> {
    pub fn new() -> Self {
        SimdMaxMin { max: T::min(), min: T::max() }
    }

    #[inline(always)]
    pub fn update(&mut self, x: T) {
        if x.ord(&self.max) == cmp::Ordering::Greater {
            self.max = x;
        }
        if x.ord(&self.min) == cmp::Ordering::Less {
            self.min = x;
        }
    }

    pub fn to_minmax_stats(self, has_non_null: bool) -> MaxMin<T> {
        if has_non_null {
            MaxMin { max: Some(self.max), min: Some(self.min) }
        } else {
            MaxMin::new()
        }
    }
}

impl MaxMin<i32> {
    /// Updates max/min by interpreting `x` as an unsigned value for comparison.
    /// Useful for types like IPv4 where the bit pattern represents an unsigned
    /// value but is stored as `i32`.
    pub fn update_unsigned(&mut self, x: i32) {
        let xu = x as u32;
        self.max = Some(if let Some(max) = self.max {
            if xu > max as u32 {
                x
            } else {
                max
            }
        } else {
            x
        });
        self.min = Some(if let Some(min) = self.min {
            if xu < min as u32 {
                x
            } else {
                min
            }
        } else {
            x
        });
    }
}

pub struct BinaryMaxMinStats {
    primitive_type: PrimitiveType,
    max_value: Option<Vec<u8>>,
    min_value: Option<Vec<u8>>,
}

const SIZEOF_I64: usize = mem::size_of::<i64>();

impl BinaryMaxMinStats {
    pub fn new(primitive_type: &PrimitiveType) -> Self {
        Self {
            primitive_type: primitive_type.clone(),
            max_value: None,
            min_value: None,
        }
    }

    pub fn update(&mut self, value: &[u8]) {
        let val = if is_binary_column_type(&self.primitive_type) {
            &value[..value.len().min(SIZEOF_I64 + 1)]
        } else {
            value
        };
        match &mut self.max_value {
            None => {
                self.max_value = Some(val.to_vec());
            }
            Some(max) => {
                if val > max.as_slice() {
                    *max = val.to_vec();
                }
            }
        }
        match &mut self.min_value {
            None => {
                self.min_value = Some(val.to_vec());
            }
            Some(min) => {
                if val < min.as_slice() {
                    *min = val.to_vec();
                }
            }
        }
    }

    pub fn into_parquet_stats(self, null_count: usize) -> ParquetStatistics {
        let Self { primitive_type, min_value, max_value } = self;
        let (min_value, max_value, is_min_value_exact, is_max_value_exact) =
            if is_binary_column_type(&primitive_type) {
                // Opaque Binary: keep the min's <=9-byte prefix (a valid floor) and round
                // the max up via its incremented 8-byte prefix (a valid ceiling). Both can
                // be inexact (truncated prefix / rounded-up ceiling), so flag them like the
                // text path. update() clamps both to <=9 bytes, so len > 8 means len == 9.
                let min_clamped = min_value.as_ref().is_some_and(|min| min.len() > SIZEOF_I64);
                let (max_value, max_rounded) = match max_value {
                    Some(max) if max.len() > SIZEOF_I64 => match binary_upper_bound(max) {
                        // Rounded the 8-byte prefix up to a valid ceiling: inexact.
                        Some(bound) => (Some(bound), true),
                        // The 8-byte prefix is all 0xFF and has no short upper bound, so
                        // omit max_value: a missing max reads as "unbounded" (readers won't
                        // prune), whereas emitting the clamped prefix would be byte-wise <
                        // a longer all-0xFF value -- an invalid bound that drops live rows.
                        None => (None, false),
                    },
                    // Short (<= 8 bytes) or absent: stored verbatim and exact.
                    other => (other, false),
                };
                (
                    min_value,
                    max_value,
                    min_clamped.then_some(false),
                    max_rounded.then_some(false),
                )
            } else if is_utf8_column_type(&primitive_type) {
                // Text (String/Symbol/Varchar): bound min/max to UTF8_STATS_TRUNCATE_LEN
                // bytes on a UTF-8 codepoint boundary. Truncate the min down to a prefix
                // (byte-wise <= every value) and the max up to the next string (byte-wise
                // >= every value), so the bound stays conservative for QuestDB's own
                // row-group pruning and for external readers, while the footer (and the
                // _pm sidecar that copies these stats by value) no longer grows without
                // limit when a single value is multi-megabyte.
                // The min always shortens to a strict prefix once it exceeds the
                // bound, so it is inexact exactly when it was over the bound. The
                // max can fall back to the untruncated (exact) value when no
                // shorter ceiling exists, so truncate_max_utf8 reports whether it
                // actually widened the bound rather than inferring it from length.
                let min_truncated = min_value
                    .as_ref()
                    .is_some_and(|min| min.len() > UTF8_STATS_TRUNCATE_LEN);
                let min_value =
                    min_value.map(|min| truncate_min_utf8(min, UTF8_STATS_TRUNCATE_LEN));
                let (max_value, max_truncated) = match max_value {
                    Some(max) => {
                        let (bound, truncated) = truncate_max_utf8(max, UTF8_STATS_TRUNCATE_LEN);
                        (Some(bound), truncated)
                    }
                    None => (None, false),
                };
                (
                    min_value,
                    max_value,
                    min_truncated.then_some(false),
                    max_truncated.then_some(false),
                )
            } else {
                // Fixed-length byte arrays (Uuid/Long128/Long256/Decimal): fixed-width and
                // already short, so store the exact bounds verbatim.
                (min_value, max_value, None, None)
            };

        let stats = &BinaryStatistics {
            primitive_type,
            null_count: Some(null_count as i64),
            distinct_count: None,
            max_value,
            min_value,
            is_max_value_exact,
            is_min_value_exact,
        } as &dyn Statistics;
        serialize_statistics(stats)
    }
}

fn is_binary_column_type(primitive_type: &PrimitiveType) -> bool {
    primitive_type.physical_type == PhysicalType::ByteArray && primitive_type.logical_type.is_none()
}

/// True for the UTF-8 text types (String/Symbol/Varchar): a `ByteArray` carrying
/// the `String` logical type. The opaque `Binary` type (`ByteArray` with no
/// logical type) goes through `is_binary_column_type` instead, and the
/// fixed-length byte-array types (Uuid/Long128/Long256/Decimal) are neither.
fn is_utf8_column_type(primitive_type: &PrimitiveType) -> bool {
    primitive_type.physical_type == PhysicalType::ByteArray
        && primitive_type.logical_type == Some(PrimitiveLogicalType::String)
}

/// Byte-length bound applied to UTF-8 (String/Symbol/Varchar) min/max statistics.
/// Without it a single multi-megabyte value makes the column-chunk min and/or max
/// equally large in the footer (and in the `_pm` sidecar, which copies the chunk
/// stats by value). parquet-mr truncates byte-array bounds to 64 bytes by default;
/// we match that.
const UTF8_STATS_TRUNCATE_LEN: usize = 64;

#[inline]
fn is_utf8_continuation_byte(b: u8) -> bool {
    (b & 0xC0) == 0x80
}

/// Largest length `<= max_len` that ends on a UTF-8 codepoint boundary of `value`,
/// or `value.len()` when it already fits. Backs up off any multi-byte codepoint
/// that straddles `max_len` so the prefix never splits a codepoint.
fn utf8_floor_len(value: &[u8], max_len: usize) -> usize {
    if value.len() <= max_len {
        return value.len();
    }
    let mut end = max_len;
    while end > 0 && is_utf8_continuation_byte(value[end]) {
        end -= 1;
    }
    end
}

/// Truncate `value` down to at most `max_len` bytes on a codepoint boundary. The
/// result is a prefix of `value`, hence byte-wise `<= value`: a valid lower bound.
fn truncate_min_utf8(mut value: Vec<u8>, max_len: usize) -> Vec<u8> {
    value.truncate(utf8_floor_len(&value, max_len));
    value
}

/// Truncate `value` up to a valid upper bound of at most ~`max_len` bytes: it is
/// byte-wise `>= value`. Truncates to a codepoint boundary then advances the last
/// codepoint to the next scalar value (carrying into earlier codepoints when one is
/// already U+10FFFF). Falls back to the untruncated `value` when no shorter ceiling
/// exists (every retained codepoint is U+10FFFF) or `value` is not valid UTF-8.
///
/// Advancing a codepoint can grow its UTF-8 length by one byte (e.g. U+007F ->
/// U+0080), and only the last retained codepoint is advanced (a carry drops
/// trailing codepoints), so the result exceeds `max_len` by at most 1 byte --
/// still bounded, versus an untruncated value of arbitrary length.
///
/// Returns `(bound, is_inexact)`. `is_inexact` is `true` only when the bound was
/// actually widened; the fallback returns the original value unchanged, which is
/// an exact upper bound (`is_inexact == false`).
fn truncate_max_utf8(value: Vec<u8>, max_len: usize) -> (Vec<u8>, bool) {
    if value.len() <= max_len {
        return (value, false);
    }
    let end = utf8_floor_len(&value, max_len);
    match next_utf8_string(&value[..end]) {
        Some(bounded) => (bounded, true),
        None => (value, false),
    }
}

/// Smallest valid-UTF-8 byte string strictly greater (byte-wise) than every string
/// that has `prefix` as a prefix, or `None` when `prefix` is empty, not valid UTF-8,
/// or made up entirely of the maximum codepoint U+10FFFF. Advances the last codepoint
/// and, on carry, drops it and advances the preceding one.
fn next_utf8_string(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut chars: Vec<char> = std::str::from_utf8(prefix).ok()?.chars().collect();
    while let Some(last) = chars.pop() {
        if let Some(next) = next_char(last) {
            let mut result: String = chars.iter().collect();
            result.push(next);
            return Some(result.into_bytes());
        }
        // `last` is U+10FFFF; drop it and carry into the previous codepoint.
    }
    None
}

/// Next Unicode scalar value after `c`, skipping the UTF-16 surrogate range, or
/// `None` when `c` is the maximum scalar value U+10FFFF.
fn next_char(c: char) -> Option<char> {
    let code = c as u32 + 1;
    // U+D800..=U+DFFF are not scalar values; the only valid char that steps into the
    // range is U+D7FF, so jump straight to the first codepoint past it.
    let code = if code == 0xD800 { 0xE000 } else { code };
    char::from_u32(code)
}

/// Smallest 8-byte upper bound for an opaque-Binary max whose `update()`-clamped
/// prefix exceeds 8 bytes: read the leading 8 bytes big-endian, increment, write
/// back. Returns `None` when the prefix is all `0xFF` -- it can't be incremented,
/// and there is no short value that bounds a longer all-`0xFF`-prefixed value from
/// above. The caller then omits `max_value` (unbounded) rather than emit the
/// clamped prefix, which would be byte-wise *less* than such a value and so an
/// invalid upper bound that makes conformant readers prune the row group holding it.
pub(crate) fn binary_upper_bound(max_value: Vec<u8>) -> Option<Vec<u8>> {
    let val_slice_be: [u8; SIZEOF_I64] = max_value[..SIZEOF_I64].try_into().unwrap();
    let as_u64 = u64::from_be_bytes(val_slice_be);
    as_u64.checked_add(1).map(|inc| inc.to_be_bytes().to_vec())
}

pub struct ArrayStats {
    null_count: usize,
}

impl ArrayStats {
    pub fn new(null_count: usize) -> Self {
        Self { null_count }
    }

    pub fn into_parquet_stats(self) -> ParquetStatistics {
        ParquetStatistics {
            null_count: Some(self.null_count as i64),
            distinct_count: None,
            max_value: None,
            min_value: None,
            min: None,
            max: None,
            is_max_value_exact: None,
            is_min_value_exact: None,
        }
    }
}

pub struct ExactSizedIter<T, I: Iterator<Item = T>> {
    iter: I,
    remaining: usize,
}

impl<T, I: Iterator<Item = T> + Clone> Clone for ExactSizedIter<T, I> {
    fn clone(&self) -> Self {
        Self { iter: self.iter.clone(), remaining: self.remaining }
    }
}

impl<T, I: Iterator<Item = T>> ExactSizedIter<T, I> {
    pub fn new(iter: I, length: usize) -> Self {
        Self { iter, remaining: length }
    }
}

impl<T, I: Iterator<Item = T>> Iterator for ExactSizedIter<T, I> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().inspect(|_x| {
            self.remaining -= 1;
        })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

fn encode_primitive_def_levels_v1<I: Iterator<Item = bool>>(
    buffer: &mut Vec<u8>,
    iter: I,
    length: usize,
) -> io::Result<()> {
    buffer.extend_from_slice(&[0; 4]);
    let start = buffer.len();
    encode_bool(buffer, iter, length)?;
    let end = buffer.len();
    let length_bytes = end - start;

    // write the first 4 bytes as length
    let length_bytes = (length_bytes as i32).to_le_bytes();
    (0..4).for_each(|i| buffer[start - 4 + i] = length_bytes[i]);
    Ok(())
}

fn encode_primitive_def_levels_v2<I: Iterator<Item = bool>>(
    buffer: &mut Vec<u8>,
    iter: I,
    length: usize,
) -> io::Result<()> {
    encode_bool(buffer, iter, length)
}

pub fn encode_primitive_def_levels<I: Iterator<Item = bool>>(
    buffer: &mut Vec<u8>,
    iter: I,
    length: usize,
    version: Version,
) -> io::Result<()> {
    match version {
        Version::V1 => encode_primitive_def_levels_v1(buffer, iter, length),
        Version::V2 => encode_primitive_def_levels_v2(buffer, iter, length),
    }
}

fn encode_bitmap_def_levels_payload(
    buffer: &mut Vec<u8>,
    bits: &[u8],
    length: usize,
) -> io::Result<()> {
    let mut header = ceil8(length) as u64;
    header <<= 1;
    header |= 1;

    let mut container = [0; 10];
    let used = uleb128::encode(header, &mut container);
    buffer.extend_from_slice(&container[..used]);

    let used_bytes = length.saturating_add(7) / 8;
    if used_bytes == 0 {
        return Ok(());
    }

    let full_bytes = length / 8;
    buffer.extend_from_slice(&bits[..full_bytes]);
    if full_bytes < used_bytes {
        let trailing_bits = length % 8;
        if trailing_bits == 0 {
            buffer.extend_from_slice(&bits[full_bytes..used_bytes]);
        } else {
            let mask = ((1u16 << trailing_bits) - 1) as u8;
            buffer.push(bits[full_bytes] & mask);
        }
    }
    Ok(())
}

fn encode_primitive_def_levels_from_bitmap_v1(
    buffer: &mut Vec<u8>,
    bits: &[u8],
    length: usize,
) -> io::Result<()> {
    buffer.extend_from_slice(&[0; 4]);
    let start = buffer.len();
    encode_bitmap_def_levels_payload(buffer, bits, length)?;
    let end = buffer.len();
    let length_bytes = end - start;
    let length_bytes = (length_bytes as i32).to_le_bytes();
    (0..4).for_each(|i| buffer[start - 4 + i] = length_bytes[i]);
    Ok(())
}

fn encode_primitive_def_levels_from_bitmap_v2(
    buffer: &mut Vec<u8>,
    bits: &[u8],
    length: usize,
) -> io::Result<()> {
    encode_bitmap_def_levels_payload(buffer, bits, length)
}

pub fn encode_primitive_def_levels_from_bitmap(
    buffer: &mut Vec<u8>,
    bits: &[u8],
    length: usize,
    version: Version,
) -> io::Result<()> {
    match version {
        Version::V1 => encode_primitive_def_levels_from_bitmap_v1(buffer, bits, length),
        Version::V2 => encode_primitive_def_levels_from_bitmap_v2(buffer, bits, length),
    }
}

/// Encode def levels where every value is present (all 1s).
/// Uses a single RLE run which is ~3 bytes regardless of row count,
/// vs the general bitpacked path that scales with row count.
pub fn encode_all_ones_def_levels(buffer: &mut Vec<u8>, num_rows: usize, version: Version) {
    encode_constant_def_levels(buffer, num_rows, version, true);
}

/// Encode def levels where every value is null (all 0s).
pub fn encode_all_zeros_def_levels(buffer: &mut Vec<u8>, num_rows: usize, version: Version) {
    encode_constant_def_levels(buffer, num_rows, version, false);
}

fn encode_constant_def_levels(
    buffer: &mut Vec<u8>,
    num_rows: usize,
    version: Version,
    present: bool,
) {
    match version {
        Version::V1 => {
            // 4-byte LE length prefix, then RLE payload
            let start = buffer.len();
            buffer.extend_from_slice(&[0; 4]);
            let payload_start = buffer.len();
            encode_rle_bool(buffer, num_rows, present);
            let payload_len = (buffer.len() - payload_start) as i32;
            buffer[start..start + 4].copy_from_slice(&payload_len.to_le_bytes());
        }
        Version::V2 => {
            encode_rle_bool(buffer, num_rows, present);
        }
    }
}

/// Emit an RLE run of `count` constant def levels with bit_width=1.
/// Format: varint header (count << 1, even = RLE) + 1 value byte.
fn encode_rle_bool(buffer: &mut Vec<u8>, count: usize, present: bool) {
    let header = (count as u64) << 1; // even = RLE mode
    let mut container = [0u8; 10];
    let used = uleb128::encode(header, &mut container);
    buffer.extend_from_slice(&container[..used]);
    buffer.push(u8::from(present)); // ceil(bit_width/8) = 1
}

fn encode_group_levels_v1<I: Iterator<Item = u32>>(
    buffer: &mut Vec<u8>,
    iter: I,
    length: usize,
    num_bits: u32,
) -> io::Result<()> {
    buffer.extend_from_slice(&[0; 4]);
    let start = buffer.len();
    encode_u32(buffer, iter, length, num_bits)?;
    let end = buffer.len();
    let length_bytes = end - start;

    // write the first 4 bytes as length
    let length_bytes = (length_bytes as i32).to_le_bytes();
    (0..4).for_each(|i| buffer[start - 4 + i] = length_bytes[i]);
    Ok(())
}

fn encode_group_levels_v2<I: Iterator<Item = u32>>(
    buffer: &mut Vec<u8>,
    iter: I,
    length: usize,
    num_bits: u32,
) -> io::Result<()> {
    encode_u32(buffer, iter, length, num_bits)
}

pub fn encode_group_levels<I: Iterator<Item = u32>>(
    buffer: &mut Vec<u8>,
    iter: I,
    length: usize,
    max_level: u32,
    version: Version,
) -> io::Result<()> {
    let num_bits = bit_width(max_level as u64);
    match version {
        Version::V1 => encode_group_levels_v1(buffer, iter, length, num_bits.into()),
        Version::V2 => encode_group_levels_v2(buffer, iter, length, num_bits.into()),
    }
}

#[inline]
pub fn bit_width(max: u64) -> u8 {
    (64 - max.leading_zeros()) as u8
}

#[allow(clippy::too_many_arguments)]
pub fn build_plain_page(
    buffer: Vec<u8>,
    num_rows: usize,
    null_count: usize,
    definition_levels_byte_length: usize,
    statistics: Option<ParquetStatistics>,
    primitive_type: PrimitiveType,
    options: WriteOptions,
    encoding: Encoding,
    required: bool,
) -> ParquetResult<DataPage> {
    let header = match options.version {
        Version::V1 => DataPageHeader::V1(DataPageHeaderV1 {
            num_values: num_rows as i32,
            encoding: encoding.into(),
            definition_level_encoding: Encoding::Rle.into(),
            repetition_level_encoding: Encoding::Rle.into(),
            statistics,
        }),
        Version::V2 => DataPageHeader::V2(DataPageHeaderV2 {
            num_values: num_rows as i32,
            encoding: encoding.into(),
            num_nulls: null_count as i32,
            num_rows: num_rows as i32,
            definition_levels_byte_length: definition_levels_byte_length as i32,
            repetition_levels_byte_length: 0,
            is_compressed: Some(options.compression != CompressionOptions::Uncompressed),
            statistics,
        }),
    };
    Ok(DataPage::new(
        header,
        buffer,
        Descriptor {
            primitive_type,
            max_def_level: if required { 0 } else { 1 },
            max_rep_level: 0,
        },
        Some(num_rows),
    ))
}

/// # Safety
/// - `slice` must be properly aligned for `T`.
/// - The bytes in `slice` must represent valid values of `T`.
/// - The caller must ensure `slice.len()` is a multiple of `size_of::<T>()`.
///   Any trailing bytes are dropped on the typed view, so an ill-sized slice
///   would silently lose data; validate at the boundary (JNI/file read) before
///   calling this function.
pub unsafe fn transmute_slice<T>(slice: &[u8]) -> &[T] {
    let sizeof_t = mem::size_of::<T>();
    debug_assert_eq!(slice.len() % sizeof_t, 0);
    if slice.is_empty() {
        &[]
    } else {
        debug_assert!(
            (slice.as_ptr() as usize).is_multiple_of(mem::align_of::<T>()),
            "transmute_slice: pointer {:p} is not aligned for {} (align = {})",
            slice.as_ptr(),
            std::any::type_name::<T>(),
            mem::align_of::<T>(),
        );
        // SAFETY: Caller guarantees alignment, valid content, and length divisibility.
        slice::from_raw_parts(slice.as_ptr() as *const T, slice.len() / sizeof_t)
    }
}

#[cfg(test)]
mod tests {
    use parquet2::encoding::bitpacked;
    use parquet2::encoding::hybrid_rle::{Decoder, HybridEncoded};
    use parquet2::schema::types::PhysicalType;
    use parquet2::schema::types::PrimitiveType;

    use crate::parquet_write::util::{
        binary_upper_bound, encode_primitive_def_levels, BinaryMaxMinStats,
    };

    #[test]
    fn decode_bitmap_v2() {
        let bit_width = 1;
        let expected = &[
            false, false, true, false, true, false, true, false, true, false, false, false, true,
            true,
        ];
        let expectedu8 = expected
            .iter()
            .map(|x| if *x { 1u8 } else { 0u8 })
            .collect::<Vec<_>>();
        let mut buff = vec![];
        encode_primitive_def_levels(
            &mut buff,
            expected.iter().cloned(),
            expected.len(),
            parquet2::write::Version::V2,
        )
        .unwrap();

        let mut decoder = Decoder::new(buff.as_slice(), 1);
        let run = decoder.next().unwrap();

        if let HybridEncoded::Bitpacked(values) = run.unwrap() {
            let result = bitpacked::Decoder::<u8>::try_new(values, bit_width, expected.len())
                .unwrap()
                .collect::<Vec<_>>();
            assert_eq!(result, expectedu8);
        } else {
            panic!()
        };
    }

    #[test]
    fn decode_bitmap_v1() {
        let bit_width = 1;
        let expected = &[
            false, false, true, false, true, false, true, false, true, false, false, false, true,
            true,
        ];
        let expectedu8 = expected
            .iter()
            .map(|x| if *x { 1u8 } else { 0u8 })
            .collect::<Vec<_>>();
        let mut buff = vec![];
        encode_primitive_def_levels(
            &mut buff,
            expected.iter().cloned(),
            expected.len(),
            parquet2::write::Version::V1,
        )
        .unwrap();

        let length = i32::from_le_bytes(buff[..4].try_into().unwrap()) as usize;
        assert_eq!(length, buff.len() - 4);

        let mut decoder = Decoder::new(&buff[4..], 1);
        let run = decoder.next().unwrap();

        if let HybridEncoded::Bitpacked(values) = run.unwrap() {
            let result = bitpacked::Decoder::<u8>::try_new(values, bit_width, expected.len())
                .unwrap()
                .collect::<Vec<_>>();
            assert_eq!(result, expectedu8);
        } else {
            panic!()
        };
    }

    #[test]
    fn test_binary_upper_bound_normal() {
        let input = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xAB];
        let result = binary_upper_bound(input);
        assert_eq!(
            result,
            Some(vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02])
        );
    }

    #[test]
    fn test_binary_upper_bound_all_ff() {
        let input = vec![0xFF; 9];
        let result = binary_upper_bound(input);
        // The 8-byte prefix is all 0xFF and can't be incremented; there is no short
        // upper bound, so the caller must omit max rather than emit a prefix that
        // would understate a longer all-0xFF value.
        assert_eq!(result, None);
    }

    #[test]
    fn test_binary_upper_bound_near_max() {
        let mut input = vec![0xFF; 9];
        input[7] = 0xFE;
        let result = binary_upper_bound(input);
        assert_eq!(
            result,
            Some(vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF])
        );
    }

    #[test]
    fn test_binary_upper_bound_high_bit() {
        // 0x80_00_00_00_00_00_00_00 -- would be i64::MIN in signed, but unsigned
        // arithmetic gives the correct 0x80_00_00_00_00_00_00_01.
        let input = vec![0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x42];
        let result = binary_upper_bound(input);
        assert_eq!(
            result,
            Some(vec![0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01])
        );
    }

    #[test]
    fn test_binary_stats_all_ff_omits_max() {
        let primitive_type =
            PrimitiveType::from_physical("test".to_string(), PhysicalType::ByteArray);
        let mut stats = BinaryMaxMinStats::new(&primitive_type);
        stats.update(&[0xFF; 9]);

        let parquet_stats = stats.into_parquet_stats(0);
        // The 8-byte prefix is all 0xFF, so there is no representable short ceiling:
        // max is omitted (reads as unbounded) and its exactness flag is cleared too.
        assert_eq!(parquet_stats.max_value, None);
        assert_eq!(parquet_stats.is_max_value_exact, None);
        // The min is still the 9-byte clamped prefix: a valid, inexact floor.
        assert!(parquet_stats.min_value.is_some());
        assert_eq!(parquet_stats.is_min_value_exact, Some(false));
    }

    #[test]
    fn test_binary_stats_all_ff_max_never_understates_real_value() {
        // Regression guard for a silent-wrong-results bound: an opaque-Binary max
        // longer than 9 bytes whose first 8 bytes are all 0xFF was clamped to a
        // 9-byte prefix and emitted as-is -- byte-wise LESS than the real value, an
        // invalid upper bound that makes conformant readers prune the row group
        // holding it. The emitted max must be absent or >= the real value.
        let real_max = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xAA, 0xBB];
        let primitive_type = PrimitiveType::from_physical("b".to_string(), PhysicalType::ByteArray);
        let mut stats = BinaryMaxMinStats::new(&primitive_type);
        stats.update(&real_max);

        let parquet_stats = stats.into_parquet_stats(0);
        match &parquet_stats.max_value {
            None => {} // unbounded: safe, never prunes
            Some(max) => assert!(
                max.as_slice() >= real_max.as_slice(),
                "emitted max {max:?} understates the real value {real_max:?}"
            ),
        }
    }

    fn utf8_primitive_type() -> PrimitiveType {
        let mut pt = PrimitiveType::from_physical("s".to_string(), PhysicalType::ByteArray);
        pt.logical_type = Some(parquet2::schema::types::PrimitiveLogicalType::String);
        pt
    }

    #[test]
    fn test_utf8_floor_len() {
        // Short value: kept whole.
        assert_eq!(super::utf8_floor_len(b"abc", 64), 3);
        // ASCII over the bound: truncated exactly at the bound.
        assert_eq!(super::utf8_floor_len(&[b'a'; 100], 64), 64);
        // A 2-byte codepoint (U+00E9 'e-acute' = C3 A9) straddling the bound is
        // dropped whole: "ae" where the bound falls inside the second char.
        let s = "a\u{00e9}\u{00e9}".as_bytes(); // 61 C3 A9 C3 A9
        assert_eq!(super::utf8_floor_len(s, 2), 1); // backs off the 'e-acute' at [1..3]
        assert_eq!(super::utf8_floor_len(s, 3), 3); // [3] is a start byte, no back-off
    }

    #[test]
    fn test_truncate_min_utf8_is_valid_prefix() {
        // ASCII: a 64-byte prefix, byte-wise <= the original.
        let original = vec![b'm'; 200];
        let min = super::truncate_min_utf8(original.clone(), 64);
        assert_eq!(min.len(), 64);
        assert!(min.as_slice() <= original.as_slice());
        assert_eq!(min, original[..64]);

        // Multi-byte: truncation never splits a codepoint, stays valid UTF-8.
        let original = "\u{1F600}".repeat(40).into_bytes(); // 4 bytes each => 160 bytes
        let min = super::truncate_min_utf8(original.clone(), 64);
        assert!(min.len() <= 64);
        assert_eq!(min.len() % 4, 0, "must land on a 4-byte-codepoint boundary");
        assert!(std::str::from_utf8(&min).is_ok());
        assert!(min.as_slice() <= original.as_slice());
    }

    #[test]
    fn test_truncate_max_utf8_is_valid_ceiling() {
        // ASCII: incremented prefix, byte-wise >= the original, length still bounded.
        let original = vec![b'm'; 200];
        let (max, inexact) = super::truncate_max_utf8(original.clone(), 64);
        assert!(max.len() <= 64 + 1, "ceiling stays bounded");
        assert!(max.as_slice() >= original.as_slice());
        assert!(std::str::from_utf8(&max).is_ok());
        assert!(inexact, "a widened ceiling is inexact");

        // Multi-byte: never splits a codepoint, still a valid byte-wise ceiling.
        let original = "\u{00e9}".repeat(50).into_bytes(); // 100 bytes
        let (max, inexact) = super::truncate_max_utf8(original.clone(), 64);
        assert!(max.as_slice() >= original.as_slice());
        assert!(std::str::from_utf8(&max).is_ok());
        assert!(inexact);

        // Carry: when the truncated prefix ends in U+10FFFF the carry advances an
        // earlier codepoint, and the result still dominates the original byte-wise.
        let mut original = vec![b'a'; 60];
        original.extend_from_slice("\u{10FFFF}".repeat(5).as_bytes()); // 60 + 20 bytes
        let (max, inexact) = super::truncate_max_utf8(original.clone(), 64);
        assert!(max.as_slice() >= original.as_slice());
        assert!(std::str::from_utf8(&max).is_ok());
        assert!(inexact);
    }

    #[test]
    fn test_truncate_max_utf8_all_max_codepoint_falls_back() {
        // 16 * U+10FFFF = 64 bytes of the maximum codepoint, plus one more: no shorter
        // ceiling exists, so the untruncated value is kept (a valid, if loose, bound).
        let original = "\u{10FFFF}".repeat(17).into_bytes();
        assert_eq!(original.len(), 68);
        let (max, inexact) = super::truncate_max_utf8(original.clone(), 64);
        assert_eq!(max, original);
        // The bound was kept whole, so it is exact, not inexact.
        assert!(!inexact);
    }

    #[test]
    fn test_next_char() {
        assert_eq!(super::next_char('a'), Some('b'));
        // Steps over the UTF-16 surrogate range U+D800..=U+DFFF.
        assert_eq!(super::next_char('\u{D7FF}'), Some('\u{E000}'));
        // No scalar value past the maximum.
        assert_eq!(super::next_char('\u{10FFFF}'), None);
    }

    #[test]
    fn test_next_utf8_string() {
        assert_eq!(super::next_utf8_string(b"abc"), Some(b"abd".to_vec()));
        // Carry past a trailing max codepoint advances the preceding one.
        assert_eq!(
            super::next_utf8_string("a\u{10FFFF}".as_bytes()),
            Some(b"b".to_vec())
        );
        // Empty, all-max, and invalid UTF-8 inputs have no bounded successor.
        assert_eq!(super::next_utf8_string(b""), None);
        assert_eq!(super::next_utf8_string("\u{10FFFF}".as_bytes()), None);
        assert_eq!(super::next_utf8_string(&[0xFF, 0xFF]), None);
    }

    #[test]
    fn test_binary_stats_text_truncates_long_values() {
        // Two long distinct values; the chunk min/max must be bounded yet still
        // bracket both values byte-wise.
        let lo = vec![b'A'; 200];
        let hi = vec![b'Z'; 200];
        let primitive_type = utf8_primitive_type();
        let mut stats = BinaryMaxMinStats::new(&primitive_type);
        stats.update(&lo);
        stats.update(&hi);

        let parquet_stats = stats.into_parquet_stats(0);
        let min = parquet_stats.min_value.unwrap();
        let max = parquet_stats.max_value.unwrap();
        assert_eq!(min.len(), 64, "min truncated to the bound");
        assert!(max.len() <= 64 + 1, "max stays bounded");
        assert!(
            min.as_slice() <= lo.as_slice(),
            "min is a floor for both values"
        );
        assert!(
            max.as_slice() >= hi.as_slice(),
            "max is a ceiling for both values"
        );
        assert_eq!(
            parquet_stats.is_min_value_exact,
            Some(false),
            "truncated min must be marked inexact"
        );
        assert_eq!(
            parquet_stats.is_max_value_exact,
            Some(false),
            "truncated max must be marked inexact"
        );
    }

    #[test]
    fn test_binary_stats_text_all_max_codepoint_max_stays_exact() {
        // A long max value made entirely of U+10FFFF has no shorter ceiling, so the
        // max falls back to the untruncated (exact) value. The min still truncates
        // to a prefix and stays inexact; the max must NOT be flagged inexact.
        let primitive_type = utf8_primitive_type();
        let mut stats = BinaryMaxMinStats::new(&primitive_type);
        let value = "\u{10FFFF}".repeat(17).into_bytes(); // 68 bytes, all the max codepoint
        stats.update(&value);

        let parquet_stats = stats.into_parquet_stats(0);
        assert_eq!(
            parquet_stats.max_value.as_deref(),
            Some(value.as_slice()),
            "max kept whole as a valid exact ceiling"
        );
        assert_eq!(
            parquet_stats.is_max_value_exact, None,
            "kept-whole max is exact, not inexact"
        );
        // The min was truncated to a prefix, so it stays inexact.
        assert_eq!(parquet_stats.min_value.unwrap().len(), 64);
        assert_eq!(parquet_stats.is_min_value_exact, Some(false));
    }

    #[test]
    fn test_binary_stats_text_keeps_short_values() {
        // Values within the bound are stored verbatim (exact min/max).
        let primitive_type = utf8_primitive_type();
        let mut stats = BinaryMaxMinStats::new(&primitive_type);
        stats.update(b"apple");
        stats.update(b"banana");

        let parquet_stats = stats.into_parquet_stats(0);
        assert_eq!(
            parquet_stats.min_value.as_deref(),
            Some(b"apple".as_slice())
        );
        assert_eq!(
            parquet_stats.max_value.as_deref(),
            Some(b"banana".as_slice())
        );
        assert_eq!(parquet_stats.is_min_value_exact, None);
        assert_eq!(parquet_stats.is_max_value_exact, None);
    }

    #[test]
    fn test_binary_stats_text_boundary_64_is_exact() {
        // Exactly UTF8_STATS_TRUNCATE_LEN bytes: at the bound, not over it, so the value
        // is kept verbatim and both bounds stay exact (flagged None).
        let primitive_type = utf8_primitive_type();
        let mut stats = BinaryMaxMinStats::new(&primitive_type);
        let value = vec![b'm'; 64];
        stats.update(&value);

        let parquet_stats = stats.into_parquet_stats(0);
        assert_eq!(parquet_stats.min_value.as_deref(), Some(value.as_slice()));
        assert_eq!(parquet_stats.max_value.as_deref(), Some(value.as_slice()));
        assert_eq!(parquet_stats.is_min_value_exact, None);
        assert_eq!(parquet_stats.is_max_value_exact, None);
    }

    #[test]
    fn test_binary_stats_text_boundary_65_truncates() {
        // One byte past the bound triggers the first truncation, so both bounds become
        // inexact and the min is cut back to the bound.
        let primitive_type = utf8_primitive_type();
        let mut stats = BinaryMaxMinStats::new(&primitive_type);
        let value = vec![b'm'; 65];
        stats.update(&value);

        let parquet_stats = stats.into_parquet_stats(0);
        assert_eq!(parquet_stats.min_value.unwrap().len(), 64);
        assert_eq!(parquet_stats.is_min_value_exact, Some(false));
        assert_eq!(parquet_stats.is_max_value_exact, Some(false));
    }

    #[test]
    fn test_binary_stats_binary_path_unchanged() {
        // Regression guard: opaque Binary keeps its 8/9-byte prefix bound and is NOT
        // subject to the 64-byte text truncation.
        let primitive_type = PrimitiveType::from_physical("b".to_string(), PhysicalType::ByteArray);
        let mut stats = BinaryMaxMinStats::new(&primitive_type);
        stats.update(&[b'A'; 100]);

        let parquet_stats = stats.into_parquet_stats(0);
        // update() clamps to 9 bytes; max rounds up via binary_upper_bound to 8.
        assert_eq!(parquet_stats.min_value.unwrap().len(), 9);
        assert_eq!(parquet_stats.max_value.unwrap().len(), 8);
        // The 9-byte min is a truncated prefix and the max was rounded up: both inexact.
        assert_eq!(parquet_stats.is_min_value_exact, Some(false));
        assert_eq!(parquet_stats.is_max_value_exact, Some(false));
    }

    #[test]
    fn test_binary_stats_binary_short_value_is_exact() {
        // A short opaque Binary value (<= 8 bytes) is stored verbatim, so both bounds
        // stay exact (flagged None).
        let primitive_type = PrimitiveType::from_physical("b".to_string(), PhysicalType::ByteArray);
        let mut stats = BinaryMaxMinStats::new(&primitive_type);
        stats.update(&[b'A'; 8]);

        let parquet_stats = stats.into_parquet_stats(0);
        assert_eq!(
            parquet_stats.min_value.as_deref(),
            Some([b'A'; 8].as_slice())
        );
        assert_eq!(
            parquet_stats.max_value.as_deref(),
            Some([b'A'; 8].as_slice())
        );
        assert_eq!(parquet_stats.is_min_value_exact, None);
        assert_eq!(parquet_stats.is_max_value_exact, None);
    }

    #[test]
    fn test_binary_stats_fixed_len_not_truncated() {
        // Fixed-length byte arrays (Uuid/Long128/Long256/Decimal) take neither the
        // Binary nor the text branch: the exact bounds are stored verbatim.
        let primitive_type =
            PrimitiveType::from_physical("u".to_string(), PhysicalType::FixedLenByteArray(16));
        let mut stats = BinaryMaxMinStats::new(&primitive_type);
        let value = vec![0xABu8; 16];
        stats.update(&value);

        let parquet_stats = stats.into_parquet_stats(0);
        assert_eq!(parquet_stats.min_value.as_deref(), Some(value.as_slice()));
        assert_eq!(parquet_stats.max_value.as_deref(), Some(value.as_slice()));
        assert_eq!(parquet_stats.is_min_value_exact, None);
        assert_eq!(parquet_stats.is_max_value_exact, None);
    }

    #[test]
    fn encode_all_ones_v1() {
        use super::encode_all_ones_def_levels;

        for &num_rows in &[1, 7, 8, 14, 100, 1000, 100_000] {
            let mut buff = vec![];
            encode_all_ones_def_levels(&mut buff, num_rows, parquet2::write::Version::V1);

            // V1: first 4 bytes are LE length prefix
            let length = i32::from_le_bytes(buff[..4].try_into().unwrap()) as usize;
            assert_eq!(
                length,
                buff.len() - 4,
                "V1 length prefix mismatch for num_rows={num_rows}"
            );

            // Decode RLE and verify all values are 1
            let mut decoder = Decoder::new(&buff[4..], 1);
            let run = decoder.next().unwrap().unwrap();
            match run {
                HybridEncoded::Rle(value, count) => {
                    assert_eq!(
                        value[0] & 1,
                        1,
                        "RLE value should be 1 for num_rows={num_rows}"
                    );
                    assert_eq!(
                        count, num_rows,
                        "RLE count mismatch for num_rows={num_rows}"
                    );
                }
                HybridEncoded::Bitpacked(_) => {
                    panic!("expected RLE run, got Bitpacked for num_rows={num_rows}");
                }
            }
            assert!(
                decoder.next().is_none(),
                "expected single RLE run for num_rows={num_rows}"
            );
        }
    }

    #[test]
    fn encode_all_ones_v2() {
        use super::encode_all_ones_def_levels;

        for &num_rows in &[1, 7, 8, 14, 100, 1000, 100_000] {
            let mut buff = vec![];
            encode_all_ones_def_levels(&mut buff, num_rows, parquet2::write::Version::V2);

            // V2: no length prefix, raw RLE payload
            let mut decoder = Decoder::new(buff.as_slice(), 1);
            let run = decoder.next().unwrap().unwrap();
            match run {
                HybridEncoded::Rle(value, count) => {
                    assert_eq!(
                        value[0] & 1,
                        1,
                        "RLE value should be 1 for num_rows={num_rows}"
                    );
                    assert_eq!(
                        count, num_rows,
                        "RLE count mismatch for num_rows={num_rows}"
                    );
                }
                HybridEncoded::Bitpacked(_) => {
                    panic!("expected RLE run, got Bitpacked for num_rows={num_rows}");
                }
            }
            assert!(
                decoder.next().is_none(),
                "expected single RLE run for num_rows={num_rows}"
            );
        }
    }

    #[test]
    fn encode_all_zeros_v1() {
        use super::encode_all_zeros_def_levels;

        for &num_rows in &[1, 7, 8, 14, 100, 1000, 100_000] {
            let mut buff = vec![];
            encode_all_zeros_def_levels(&mut buff, num_rows, parquet2::write::Version::V1);

            let length = i32::from_le_bytes(buff[..4].try_into().unwrap()) as usize;
            assert_eq!(
                length,
                buff.len() - 4,
                "V1 length prefix mismatch for num_rows={num_rows}"
            );

            let mut decoder = Decoder::new(&buff[4..], 1);
            let run = decoder.next().unwrap().unwrap();
            match run {
                HybridEncoded::Rle(value, count) => {
                    assert_eq!(
                        value[0] & 1,
                        0,
                        "RLE value should be 0 for num_rows={num_rows}"
                    );
                    assert_eq!(
                        count, num_rows,
                        "RLE count mismatch for num_rows={num_rows}"
                    );
                }
                HybridEncoded::Bitpacked(_) => {
                    panic!("expected RLE run, got Bitpacked for num_rows={num_rows}");
                }
            }
            assert!(
                decoder.next().is_none(),
                "expected single RLE run for num_rows={num_rows}"
            );
        }
    }

    #[test]
    fn encode_all_zeros_v2() {
        use super::encode_all_zeros_def_levels;

        for &num_rows in &[1, 7, 8, 14, 100, 1000, 100_000] {
            let mut buff = vec![];
            encode_all_zeros_def_levels(&mut buff, num_rows, parquet2::write::Version::V2);

            let mut decoder = Decoder::new(buff.as_slice(), 1);
            let run = decoder.next().unwrap().unwrap();
            match run {
                HybridEncoded::Rle(value, count) => {
                    assert_eq!(
                        value[0] & 1,
                        0,
                        "RLE value should be 0 for num_rows={num_rows}"
                    );
                    assert_eq!(
                        count, num_rows,
                        "RLE count mismatch for num_rows={num_rows}"
                    );
                }
                HybridEncoded::Bitpacked(_) => {
                    panic!("expected RLE run, got Bitpacked for num_rows={num_rows}");
                }
            }
            assert!(
                decoder.next().is_none(),
                "expected single RLE run for num_rows={num_rows}"
            );
        }
    }

    /// Verify that encode_all_ones_def_levels produces output that decodes
    /// identically to encode_primitive_def_levels with all-true input.
    #[test]
    fn encode_all_ones_matches_general_encoder() {
        use super::{encode_all_ones_def_levels, encode_primitive_def_levels};

        for &version in &[parquet2::write::Version::V1, parquet2::write::Version::V2] {
            for &num_rows in &[1, 14, 100] {
                let mut general_buf = vec![];
                encode_primitive_def_levels(
                    &mut general_buf,
                    std::iter::repeat_n(true, num_rows),
                    num_rows,
                    version,
                )
                .unwrap();

                let mut optimized_buf = vec![];
                encode_all_ones_def_levels(&mut optimized_buf, num_rows, version);

                // The optimized RLE path should be smaller or equal in size.
                assert!(
                    optimized_buf.len() <= general_buf.len(),
                    "optimized should be <= general for num_rows={num_rows}, version={version:?}: {} vs {}",
                    optimized_buf.len(),
                    general_buf.len(),
                );

                // Both should decode to all-ones.
                let decode = |buf: &[u8], skip_prefix: bool| -> Vec<u8> {
                    let data = if skip_prefix { &buf[4..] } else { buf };
                    let decoder = Decoder::new(data, 1);
                    let mut result = Vec::new();
                    for run in decoder {
                        match run.unwrap() {
                            HybridEncoded::Bitpacked(values) => {
                                let remaining = num_rows - result.len();
                                result.extend(
                                    bitpacked::Decoder::<u8>::try_new(values, 1, remaining)
                                        .unwrap(),
                                );
                            }
                            HybridEncoded::Rle(value, count) => {
                                let val = value[0] & 1;
                                result.extend(std::iter::repeat_n(val, count));
                            }
                        }
                    }
                    result
                };

                let is_v1 = matches!(version, parquet2::write::Version::V1);
                let general_decoded = decode(&general_buf, is_v1);
                let optimized_decoded = decode(&optimized_buf, is_v1);

                assert_eq!(general_decoded.len(), num_rows);
                assert_eq!(optimized_decoded.len(), num_rows);
                assert!(general_decoded.iter().all(|&v| v == 1));
                assert!(optimized_decoded.iter().all(|&v| v == 1));
            }
        }
    }

    #[test]
    fn encode_all_zeros_matches_general_encoder() {
        use super::{encode_all_zeros_def_levels, encode_primitive_def_levels};

        for &version in &[parquet2::write::Version::V1, parquet2::write::Version::V2] {
            for &num_rows in &[1, 14, 100] {
                let mut general_buf = vec![];
                encode_primitive_def_levels(
                    &mut general_buf,
                    std::iter::repeat_n(false, num_rows),
                    num_rows,
                    version,
                )
                .unwrap();

                let mut optimized_buf = vec![];
                encode_all_zeros_def_levels(&mut optimized_buf, num_rows, version);

                assert!(
                    optimized_buf.len() <= general_buf.len(),
                    "optimized should be <= general for num_rows={num_rows}, version={version:?}: {} vs {}",
                    optimized_buf.len(),
                    general_buf.len(),
                );

                let decode = |buf: &[u8], skip_prefix: bool| -> Vec<u8> {
                    let data = if skip_prefix { &buf[4..] } else { buf };
                    let decoder = Decoder::new(data, 1);
                    let mut result = Vec::new();
                    for run in decoder {
                        match run.unwrap() {
                            HybridEncoded::Bitpacked(values) => {
                                let remaining = num_rows - result.len();
                                result.extend(
                                    bitpacked::Decoder::<u8>::try_new(values, 1, remaining)
                                        .unwrap(),
                                );
                            }
                            HybridEncoded::Rle(value, count) => {
                                let val = value[0] & 1;
                                result.extend(std::iter::repeat_n(val, count));
                            }
                        }
                    }
                    result
                };

                let is_v1 = matches!(version, parquet2::write::Version::V1);
                let general_decoded = decode(&general_buf, is_v1);
                let optimized_decoded = decode(&optimized_buf, is_v1);

                assert_eq!(general_decoded.len(), num_rows);
                assert_eq!(optimized_decoded.len(), num_rows);
                assert!(general_decoded.iter().all(|&v| v == 0));
                assert!(optimized_decoded.iter().all(|&v| v == 0));
            }
        }
    }

    #[test]
    fn encode_bitmap_def_levels_matches_general_encoder() {
        use super::{encode_primitive_def_levels, encode_primitive_def_levels_from_bitmap};

        let values = [
            true, false, true, true, false, false, true, false, true, true, false,
        ];
        let mut bitmap = vec![0b0100_1101, 0b1111_1011];
        bitmap[1] |= 0b1111_0000;

        for &version in &[parquet2::write::Version::V1, parquet2::write::Version::V2] {
            let mut general_buf = vec![];
            encode_primitive_def_levels(
                &mut general_buf,
                values.into_iter(),
                values.len(),
                version,
            )
            .unwrap();

            let mut bitmap_buf = vec![];
            encode_primitive_def_levels_from_bitmap(
                &mut bitmap_buf,
                &bitmap,
                values.len(),
                version,
            )
            .unwrap();

            assert_eq!(
                bitmap_buf, general_buf,
                "bitmap path should match general encoder for version={version:?}"
            );
        }
    }

    #[test]
    fn test_max_min_update_unsigned() {
        let mut mm: super::MaxMin<i32> = super::MaxMin::new();

        // i32::MIN (0x80000000) is largest as unsigned (2^31)
        // i32::MAX (0x7FFFFFFF) is 2^31 - 1 as unsigned
        mm.update_unsigned(0);
        assert_eq!(mm.min, Some(0));
        assert_eq!(mm.max, Some(0));

        mm.update_unsigned(i32::MAX); // 0x7FFFFFFF = 2147483647u32
        assert_eq!(mm.max, Some(i32::MAX));

        mm.update_unsigned(-1); // 0xFFFFFFFF = 4294967295u32 (max u32)
        assert_eq!(mm.max, Some(-1));

        mm.update_unsigned(i32::MIN); // 0x80000000 = 2147483648u32
        assert_eq!(mm.min, Some(0)); // 0 is still the smallest unsigned
    }

    /// Regression test for a bug where `SimdMaxMin::update` used an `else if`
    /// that skipped the `min` branch whenever the `max` branch fired. On
    /// strictly ascending input every value takes the max branch, leaving
    /// `min` at its initial `T::max()` sentinel.
    #[test]
    fn test_simd_max_min_update_ascending_i32() {
        let mut s: super::SimdMaxMin<i32> = super::SimdMaxMin::new();
        for v in [1, 2, 3, 4, 5] {
            s.update(v);
        }
        assert_eq!(s.min, 1);
        assert_eq!(s.max, 5);
    }

    #[test]
    fn test_simd_max_min_update_descending_i32() {
        let mut s: super::SimdMaxMin<i32> = super::SimdMaxMin::new();
        for v in [5, 4, 3, 2, 1] {
            s.update(v);
        }
        assert_eq!(s.min, 1);
        assert_eq!(s.max, 5);
    }

    #[test]
    fn test_simd_max_min_update_mixed_i32() {
        let mut s: super::SimdMaxMin<i32> = super::SimdMaxMin::new();
        for v in [3, 1, 4, 1, 5, 9, 2, 6] {
            s.update(v);
        }
        assert_eq!(s.min, 1);
        assert_eq!(s.max, 9);
    }

    #[test]
    fn test_simd_max_min_update_single_value_i32() {
        let mut s: super::SimdMaxMin<i32> = super::SimdMaxMin::new();
        s.update(42);
        assert_eq!(s.min, 42);
        assert_eq!(s.max, 42);
    }

    #[test]
    fn test_simd_max_min_update_ascending_f64() {
        let mut s: super::SimdMaxMin<f64> = super::SimdMaxMin::new();
        for v in [1.0_f64, 2.0, 3.0] {
            s.update(v);
        }
        assert_eq!(s.min, 1.0);
        assert_eq!(s.max, 3.0);
    }
}
