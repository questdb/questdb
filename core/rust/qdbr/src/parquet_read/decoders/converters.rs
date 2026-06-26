//! Conversion helpers used by `parquet_read` decoders.
//!
//! Decoders materialize Parquet physical values and then use these adapters to
//! produce QuestDB destination types (for example scaled decimals, timestamps or
//! UUID byte-order normalization).

use crate::parquet::error::{fmt_err, ParquetResult};
use num_traits::AsPrimitive;
use std::marker::PhantomData;

/// Converts decoded values from one representation into another.
pub trait Converter<A, B> {
    /// Marks converters that can be treated as a direct identity conversion.
    const IDENTITY: bool = false;

    fn convert(&self, input: A) -> B;
}

/// Generic conversion that delegates to `AsPrimitive`.
pub struct PrimitiveConverter<A, B>
where
    B: 'static + Copy,
    A: AsPrimitive<B>,
{
    _marker: PhantomData<(A, B)>,
}

impl<A, B> Default for PrimitiveConverter<A, B>
where
    B: 'static + Copy,
    A: AsPrimitive<B>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<A, B> PrimitiveConverter<A, B>
where
    B: 'static + Copy,
    A: AsPrimitive<B>,
{
    pub fn new() -> Self {
        Self { _marker: PhantomData }
    }
}

impl<A, B> Converter<A, B> for PrimitiveConverter<A, B>
where
    B: 'static + Copy,
    A: AsPrimitive<B>,
{
    const IDENTITY: bool = true;

    #[inline]
    fn convert(&self, input: A) -> B {
        input.as_()
    }
}

/// Largest f32 value `v` such that `v as i64` does not saturate to `i64::MAX`.
///
/// `i64::MAX = 2^63 - 1` is not representable in f32 (only 23 mantissa bits;
/// 63 needed), so `i64::MAX as f32` rounds up to `2^63` - one ULP above
/// `i64::MAX`. Using `i64::MAX as f32` as the upper bound with a non-strict
/// `<=` check therefore admits values equal to `2^63`, which then saturate to
/// `i64::MAX` under Rust's `as` cast and silently produce wrong data instead of
/// the NULL sentinel. Use this constant instead: it is the f32 immediately
/// below `2^63`, equal to `2^63 - 2^39`.
pub const F32_MAX_SAFE_FOR_I64: f32 = f32::from_bits(0x5EFF_FFFF);

/// f64 analog of [`F32_MAX_SAFE_FOR_I64`].
///
/// f64 has 52 explicit mantissa bits, still short of the 63 needed for
/// `i64::MAX`, so `i64::MAX as f64` also rounds up to `2^63`. This constant is
/// the f64 immediately below `2^63`, equal to `2^63 - 2^10`.
pub const F64_MAX_SAFE_FOR_I64: f64 = f64::from_bits(0x43DF_FFFF_FFFF_FFFF);

/// Largest f32 value `v` such that `v as i32` does not saturate to `i32::MAX`.
///
/// `i32::MAX = 2^31 - 1` exceeds f32 precision (23 mantissa bits, 31 needed),
/// so `i32::MAX as f32` rounds up to `2^31`. This constant is the f32
/// immediately below `2^31`, equal to `2^31 - 2^7`.
pub const F32_MAX_SAFE_FOR_I32: f32 = f32::from_bits(0x4EFF_FFFF);

/// Range-checked float-to-integer conversion.
///
/// Values that are NaN or outside the accepted range produce `null_value`
/// instead of saturating (which is what Rust's `as` cast does).
/// This matches the C++ `convert_from_type_to_type` behaviour for float→int.
///
/// The accepted range is `[min_val, max_val]` when `LOWER_STRICT = false`
/// and `(min_val, max_val]` when `LOWER_STRICT = true`.
///
/// Callers must pass a `max_val` that is itself in the destination integer's
/// range. For `(F, I)` pairs where `I::MAX as F` rounds up beyond `I::MAX` -
/// `(f32, i64)`, `(f64, i64)`, `(f32, i32)` - use the `F*_MAX_SAFE_FOR_I*`
/// constants above instead of `I::MAX as F`.
///
/// For `(F, I)` pairs where `I::MIN` is the destination NULL sentinel
/// (`i32` and `i64`), set `LOWER_STRICT = true` and pass `min_val = I::MIN as F`.
/// Without strict lower bound, a float input equal to `I::MIN as F` would
/// convert to `I::MIN` (the NULL sentinel), silently turning a legitimate value
/// into NULL.
pub struct FloatToIntRangeCheckConverter<F, I, const LOWER_STRICT: bool = false> {
    null_value: I,
    max_val: F,
    min_val: F,
    _marker: PhantomData<(F, I)>,
}

impl<F: Copy, I: Copy, const LOWER_STRICT: bool> FloatToIntRangeCheckConverter<F, I, LOWER_STRICT> {
    pub fn new(null_value: I, max_val: F, min_val: F) -> Self {
        Self { null_value, max_val, min_val, _marker: PhantomData }
    }
}

impl<F, I, const LOWER_STRICT: bool> Converter<F, I>
    for FloatToIntRangeCheckConverter<F, I, LOWER_STRICT>
where
    F: PartialOrd + Copy + AsPrimitive<I>,
    I: 'static + Copy,
{
    #[inline]
    fn convert(&self, input: F) -> I {
        // NaN fails both comparisons (PartialOrd), so falls through to null_value.
        let in_lower = if LOWER_STRICT {
            input > self.min_val
        } else {
            input >= self.min_val
        };
        if input <= self.max_val && in_lower {
            input.as_()
        } else {
            self.null_value
        }
    }
}

pub mod int32 {
    //! Converters for 32-bit integer-backed logical types.

    use super::*;

    /// Converts integer values with a fixed decimal scale into `f64`.
    pub struct Int32ToDoubleConverter {
        ratio: f64,
    }

    impl Int32ToDoubleConverter {
        /// Upper bound on the decimal *scale* this converter accepts, distinct
        /// from the parquet precision cap of 38. A scale beyond 76 cannot come
        /// from valid metadata and is rejected as malformed.
        const MAX_SCALE: usize = 76;

        pub fn try_new(scale: usize) -> ParquetResult<Self> {
            if scale > Self::MAX_SCALE {
                return Err(fmt_err!(
                    InvalidType,
                    "decimal scale {} exceeds maximum of {}",
                    scale,
                    Self::MAX_SCALE,
                ));
            }
            Ok(Self { ratio: 10f64.powi(scale as i32) })
        }
    }

    impl Converter<i32, f64> for Int32ToDoubleConverter {
        #[inline]
        fn convert(&self, input: i32) -> f64 {
            (input as f64) / self.ratio
        }
    }

    /// Converts "days since epoch" values into milliseconds.
    #[derive(Default)]
    pub struct DayToMillisConverter;

    impl Converter<i32, i64> for DayToMillisConverter {
        #[inline]
        fn convert(&self, input: i32) -> i64 {
            DayToMillisConverter::convert(input)
        }
    }

    impl DayToMillisConverter {
        pub fn new() -> Self {
            Self
        }

        #[inline]
        pub fn convert(input: i32) -> i64 {
            (input as i64) * 24 * 60 * 60 * 1000
        }
    }
}

pub mod int96 {
    //! Converters for legacy Parquet `INT96` timestamp payloads.

    use super::*;

    #[repr(C, packed)]
    #[derive(Debug, Copy, Clone)]
    pub struct Int96Timestamp {
        nanos: u64,
        julian_date: u32,
    }

    impl From<[u8; 12]> for Int96Timestamp {
        fn from(bytes: [u8; 12]) -> Self {
            unsafe { std::mem::transmute(bytes) }
        }
    }

    /// Converts Parquet `INT96` (Julian day + nanos) into epoch nanoseconds.
    #[derive(Default)]
    pub struct Int96ToTimestampConverter;

    impl Converter<Int96Timestamp, i64> for Int96ToTimestampConverter {
        #[inline]
        fn convert(&self, input: Int96Timestamp) -> i64 {
            Self::convert(input)
        }
    }

    impl Int96ToTimestampConverter {
        pub fn new() -> Self {
            Self
        }

        #[inline]
        pub fn convert(input: Int96Timestamp) -> i64 {
            const NANOS_PER_DAY: i64 = 86400 * 1_000_000_000;
            const JULIAN_UNIX_EPOCH_OFFSET: i64 = 2440588;

            // Convert Julian date to days since Unix epoch
            let days_since_epoch = input.julian_date as i64 - JULIAN_UNIX_EPOCH_OFFSET; // Julian date epoch to Unix epoch offset

            // Calculate total nanoseconds since Unix epoch
            days_since_epoch * NANOS_PER_DAY + input.nanos as i64
        }
    }
}

pub mod int128 {
    //! Converters for 128-bit values.

    use super::*;

    /// Converts Parquet UUID binary order into QuestDB in-memory order.
    #[derive(Default)]
    pub struct Int128ToUuidConverter {}

    impl Int128ToUuidConverter {
        pub fn new() -> Self {
            Self {}
        }
    }

    impl Converter<u128, u128> for Int128ToUuidConverter {
        #[inline]
        fn convert(&self, input: u128) -> u128 {
            // In QuestDB the UUID is stored as a little-endian int128, but in Parquet it's big-endian. We need to reverse the byte order.
            u128::from_be(input)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::int32::Int32ToDoubleConverter;
    use super::Converter;

    /// 10f64.powi(n) overflows to +inf for n >= 309 (f64::MAX ~= 1.8e308). A scale
    /// that high should be rejected as nonsense parquet metadata rather than
    /// silently producing `value / +inf == 0.0` for every row.
    #[test]
    fn int32_to_double_rejects_scale_that_overflows_f64() {
        assert!(Int32ToDoubleConverter::try_new(309).is_err());
    }

    /// QuestDB caps decimal precision at 76 and parquet at 38. Anything past the
    /// QuestDB ceiling is treated as malformed metadata, even if the resulting
    /// ratio would still be finite.
    #[test]
    fn int32_to_double_rejects_scale_above_questdb_max() {
        assert!(Int32ToDoubleConverter::try_new(77).is_err());
    }

    /// Legitimate parquet scales (0..=38 in spec, up to 76 in QuestDB) must be
    /// accepted and produce the correct ratio.
    #[test]
    fn int32_to_double_accepts_realistic_scales() {
        let conv = Int32ToDoubleConverter::try_new(2).unwrap();
        // 12345 with scale 2 represents 123.45.
        assert!((conv.convert(12345) - 123.45).abs() < 1e-12);

        // Maximum legitimate QuestDB scale.
        let conv = Int32ToDoubleConverter::try_new(76).unwrap();
        // i32::MAX / 1e76 is a sensible tiny positive number, not zero.
        assert!(conv.convert(i32::MAX) > 0.0);
    }
}
