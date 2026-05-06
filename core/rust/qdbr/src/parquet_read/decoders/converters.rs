//! Conversion helpers used by `parquet_read` decoders.
//!
//! Decoders materialize Parquet physical values and then use these adapters to
//! produce QuestDB destination types (for example scaled decimals, timestamps or
//! UUID byte-order normalization).

use num_traits::AsPrimitive;

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
    _marker: std::marker::PhantomData<(A, B)>,
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
        Self { _marker: std::marker::PhantomData }
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

pub mod int32 {
    //! Converters for 32-bit integer-backed logical types.

    use super::*;

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
