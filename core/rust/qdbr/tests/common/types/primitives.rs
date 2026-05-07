use std::fmt::Debug;

use parquet::{
    basic::LogicalType,
    data_type::{
        BoolType, DataType, DoubleType, FixedLenByteArray, FixedLenByteArrayType, FloatType,
        Int32Type, Int64Type, Int96, Int96Type,
    },
};
use qdb_core::col_type::{self, nulls, ColumnTypeTag};

use crate::common::Encoding;

pub trait PrimitiveType {
    type T: Debug + Copy + PartialEq;
    type U: DataType;
    const NULL: Self::T;
    const TAG: ColumnTypeTag;
    const ENCODINGS: &[Encoding] = &[
        Encoding::Plain,
        Encoding::DeltaBinaryPacked,
        Encoding::RleDictionary,
    ];
    const LOGICAL_TYPE: Option<LogicalType> = None;
    const FIXED_LEN: Option<i32> = None;

    /// Whether the QDB encoder detects nulls via a sentinel value in the data.
    /// Types without null sentinels (Boolean, Byte, Short, Char) are always Required.
    const HAS_NULL_SENTINEL: bool = true;

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T);

    /// Generate a non-null native value for encode tests.
    /// Override for types whose `generate_data` may produce the null sentinel.
    fn generate_non_null_native(s: usize) -> Self::T {
        Self::generate_data(s).1
    }

    fn eq(a: Self::T, b: Self::T) -> bool {
        a == b
    }
}

pub fn generate_data<T: PrimitiveType>(count: usize) -> (Vec<<T::U as DataType>::T>, Vec<T::T>) {
    let mut parquet = Vec::with_capacity(count);
    let mut native = Vec::with_capacity(count);

    for i in 0..count {
        let (p, n) = T::generate_data(i);
        parquet.push(p);
        native.push(n);
    }

    (parquet, native)
}

pub fn rnd(s: usize) -> usize {
    // Use a simple xorshift to generate pseudo-random data that changes more between rows, which can help catch edge cases in encoding/decoding.
    let mut l0 = s as i64 + 1;
    let mut l1 = s as i64 + 2;
    let mut l2 = s as i64 + 3;
    l0 ^= l0 << 13;
    l0 ^= l0 >> 17;
    l0 ^= l0 << 5;
    l1 ^= l1 << 13;
    l1 ^= l1 >> 17;
    l1 ^= l1 << 5;
    l2 ^= l2 << 13;
    l2 ^= l2 >> 17;
    l2 ^= l2 << 5;
    (l0 ^ l1 ^ l2) as usize
}

// --- Boolean type ---

pub struct Boolean;

impl PrimitiveType for Boolean {
    type T = u8;
    type U = BoolType;
    const NULL: Self::T = 0;
    const TAG: ColumnTypeTag = ColumnTypeTag::Boolean;
    const ENCODINGS: &[Encoding] = &[Encoding::Plain, Encoding::RleDictionary];
    const HAS_NULL_SENTINEL: bool = false;

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let v = s.is_multiple_of(2);
        (v, v as u8)
    }
}

// --- Int32-backed types ---

pub type GeoByte = ();

impl PrimitiveType for GeoByte {
    type T = i8;
    type U = Int32Type;
    const NULL: Self::T = nulls::GEOHASH_BYTE;
    const TAG: ColumnTypeTag = ColumnTypeTag::GeoByte;

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let s = rnd(s);
        let v = s as i8;
        (v as i32, v)
    }

    fn generate_non_null_native(s: usize) -> Self::T {
        (rnd(s) % 127) as i8 // avoid -1 (null sentinel)
    }
}

pub struct GeoShort;

impl PrimitiveType for GeoShort {
    type T = i16;
    type U = Int32Type;
    const NULL: Self::T = nulls::GEOHASH_SHORT;
    const TAG: ColumnTypeTag = ColumnTypeTag::GeoShort;

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let s = rnd(s);
        let v = s as i16;
        (v as i32, v)
    }

    fn generate_non_null_native(s: usize) -> Self::T {
        (rnd(s) % 32_000) as i16 // avoid -1 (null sentinel)
    }
}

pub struct GeoInt;

impl PrimitiveType for GeoInt {
    type T = i32;
    type U = Int32Type;
    const NULL: Self::T = nulls::GEOHASH_INT;
    const TAG: ColumnTypeTag = ColumnTypeTag::GeoInt;

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let s = rnd(s);
        let v = s as i32;
        (v, v)
    }

    fn generate_non_null_native(s: usize) -> Self::T {
        (rnd(s) as i32) & 0x7FFF_FFFE // avoid -1 (null sentinel)
    }
}

pub struct Byte;

impl PrimitiveType for Byte {
    type T = i8;
    type U = Int32Type;
    const NULL: Self::T = nulls::BYTE;
    const TAG: ColumnTypeTag = ColumnTypeTag::Byte;
    const HAS_NULL_SENTINEL: bool = false;

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let s = rnd(s);
        let v = s as i8;
        (v as i32, v)
    }
}

pub struct Short;

impl PrimitiveType for Short {
    type T = i16;
    type U = Int32Type;
    const NULL: Self::T = nulls::SHORT;
    const TAG: ColumnTypeTag = ColumnTypeTag::Short;
    const HAS_NULL_SENTINEL: bool = false;

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let s = rnd(s);
        let v = s as i16;
        (v as i32, v)
    }
}

pub struct Int;

impl PrimitiveType for Int {
    type T = i32;
    type U = Int32Type;
    const NULL: Self::T = nulls::INT;
    const TAG: ColumnTypeTag = ColumnTypeTag::Int;

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let s = rnd(s);
        let v = s as i32;
        (v, v)
    }
}

pub struct IPv4;

impl PrimitiveType for IPv4 {
    type T = i32;
    type U = Int32Type;
    const NULL: Self::T = nulls::IPV4;
    const TAG: ColumnTypeTag = ColumnTypeTag::IPv4;

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let s = rnd(s);
        let v = s as i32;
        (v, v)
    }

    fn generate_non_null_native(s: usize) -> Self::T {
        (rnd(s) as i32) | 1 // avoid 0 (null sentinel)
    }
}

// --- Int64-backed types ---

pub struct GeoLong;

impl PrimitiveType for GeoLong {
    type T = i64;
    type U = Int64Type;
    const NULL: Self::T = nulls::GEOHASH_LONG;
    const TAG: ColumnTypeTag = ColumnTypeTag::GeoLong;

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let s = rnd(s);
        let v = s as i64;
        (v, v)
    }

    fn generate_non_null_native(s: usize) -> Self::T {
        (rnd(s) as i64) & 0x7FFF_FFFF_FFFF_FFFE // avoid -1 (null sentinel)
    }
}

pub struct Long;

impl PrimitiveType for Long {
    type T = i64;
    type U = Int64Type;
    const NULL: Self::T = nulls::LONG;
    const TAG: ColumnTypeTag = ColumnTypeTag::Long;

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let s = rnd(s);
        let v = s as i64;
        (v, v)
    }
}

pub struct Timestamp;

impl PrimitiveType for Timestamp {
    type T = i64;
    type U = Int64Type;
    const NULL: Self::T = nulls::LONG;
    const TAG: ColumnTypeTag = ColumnTypeTag::Timestamp;

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let s = rnd(s);
        let v = s as i64 * 1_000_000;
        (v, v)
    }
}

pub struct Date;

impl PrimitiveType for Date {
    type T = i64;
    type U = Int64Type;
    const NULL: Self::T = nulls::LONG;
    const TAG: ColumnTypeTag = ColumnTypeTag::Date;

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let s = rnd(s);
        let v = s as i64 * 86_400_000;
        (v, v)
    }
}

pub struct DateInt32;

impl PrimitiveType for DateInt32 {
    type T = i64;
    type U = Int32Type;
    const NULL: Self::T = nulls::LONG;
    const TAG: ColumnTypeTag = ColumnTypeTag::Date;
    const ENCODINGS: &[Encoding] = &[Encoding::Plain];
    const LOGICAL_TYPE: Option<LogicalType> = Some(LogicalType::Date);

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let s = rnd(s);
        let days = 18000 + s as i32;
        let millis = days as i64 * 86_400_000;
        (days, millis)
    }
}

// --- Int32-backed Char type ---

pub struct Char;

impl PrimitiveType for Char {
    type T = i16;
    type U = Int32Type;
    const NULL: Self::T = nulls::SHORT;
    const TAG: ColumnTypeTag = ColumnTypeTag::Char;
    const HAS_NULL_SENTINEL: bool = false;

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let v = (s % 95 + 32) as i16;
        (v as i32, v)
    }
}

// --- Float-backed types ---

pub struct Float;

impl PrimitiveType for Float {
    type T = f32;
    type U = FloatType;
    const NULL: Self::T = nulls::FLOAT;
    const TAG: ColumnTypeTag = ColumnTypeTag::Float;
    const ENCODINGS: &[Encoding] = &[Encoding::Plain, Encoding::RleDictionary];

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let s = rnd(s);
        let v = s as f32 * 0.5;
        (v, v)
    }

    fn eq(a: Self::T, b: Self::T) -> bool {
        a.to_bits() == b.to_bits()
    }
}

// --- Double-backed types ---

pub struct Double;

impl PrimitiveType for Double {
    type T = f64;
    type U = DoubleType;
    const NULL: Self::T = nulls::DOUBLE;
    const TAG: ColumnTypeTag = ColumnTypeTag::Double;
    const ENCODINGS: &[Encoding] = &[Encoding::Plain, Encoding::RleDictionary];

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let s = rnd(s);
        let v = s as f64 * 0.5;
        (v, v)
    }

    fn eq(a: Self::T, b: Self::T) -> bool {
        a.to_bits() == b.to_bits()
    }
}

// --- FixedLenByteArray-backed types ---

pub struct Long128;

impl PrimitiveType for Long128 {
    type T = col_type::Long128;
    type U = FixedLenByteArrayType;
    const NULL: Self::T = col_type::Long128::NULL;
    const TAG: ColumnTypeTag = ColumnTypeTag::Long128;
    const ENCODINGS: &[Encoding] = &[Encoding::Plain, Encoding::RleDictionary];
    const FIXED_LEN: Option<i32> = Some(16);

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let s = rnd(s);
        let lo = (s as i64).to_le_bytes();
        let hi = (s as i64 + 5000).to_le_bytes();
        let mut bytes = [0u8; 16];
        bytes[0..8].copy_from_slice(&lo);
        bytes[8..16].copy_from_slice(&hi);
        (
            FixedLenByteArray::from(bytes.to_vec()),
            col_type::Long128 {
                lo: i64::from_le_bytes(lo),
                hi: i64::from_le_bytes(hi),
            },
        )
    }
}

pub struct Long256;

impl PrimitiveType for Long256 {
    type T = col_type::Long256;
    type U = FixedLenByteArrayType;
    const NULL: Self::T = col_type::Long256::NULL;
    const TAG: ColumnTypeTag = ColumnTypeTag::Long256;
    const ENCODINGS: &[Encoding] = &[Encoding::Plain, Encoding::RleDictionary];
    const FIXED_LEN: Option<i32> = Some(32);

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let s = rnd(s);
        let mut bytes = [0u8; 32];
        bytes[0..8].copy_from_slice(&(s as i64).to_le_bytes());
        bytes[8..16].copy_from_slice(&((s as i64 + 1000).to_le_bytes()));
        bytes[16..24].copy_from_slice(&((s as i64 + 2000).to_le_bytes()));
        bytes[24..32].copy_from_slice(&((s as i64 + 3000).to_le_bytes()));
        (
            FixedLenByteArray::from(bytes.to_vec()),
            col_type::Long256 {
                l0: i64::from_le_bytes(bytes[0..8].try_into().unwrap()),
                l1: i64::from_le_bytes(bytes[8..16].try_into().unwrap()),
                l2: i64::from_le_bytes(bytes[16..24].try_into().unwrap()),
                l3: i64::from_le_bytes(bytes[24..32].try_into().unwrap()),
            },
        )
    }
}

// --- UUID type ---

pub struct Uuid;

impl PrimitiveType for Uuid {
    type T = u128;
    type U = FixedLenByteArrayType;
    const NULL: Self::T = nulls::UUID;
    const TAG: ColumnTypeTag = ColumnTypeTag::Uuid;
    const ENCODINGS: &[Encoding] = &[Encoding::Plain, Encoding::RleDictionary];
    const FIXED_LEN: Option<i32> = Some(16);
    const LOGICAL_TYPE: Option<LogicalType> = Some(LogicalType::Uuid);

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let s = rnd(s);
        let hi = (s as u64).wrapping_add(7777);
        let lo = s as u64;
        let mut raw_bytes = [0u8; 16];
        raw_bytes[0..8].copy_from_slice(&hi.to_be_bytes());
        raw_bytes[8..16].copy_from_slice(&lo.to_be_bytes());
        let expected = u128::from_be_bytes(raw_bytes);
        (FixedLenByteArray::from(raw_bytes.to_vec()), expected)
    }
}

// --- Int96-backed types ---

pub struct TimestampInt96;

impl PrimitiveType for TimestampInt96 {
    type T = i64;
    type U = Int96Type;
    const NULL: Self::T = nulls::LONG;
    const TAG: ColumnTypeTag = ColumnTypeTag::Timestamp;
    const ENCODINGS: &[Encoding] = &[Encoding::Plain, Encoding::RleDictionary];

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        const JULIAN_UNIX_EPOCH: u32 = 2_440_588;
        const NANOS_PER_DAY: i64 = 86_400_000_000_000;

        let julian_date = JULIAN_UNIX_EPOCH + 18000 + (s / 1000) as u32;
        let nanos_in_day = (s % 1000) as u64 * 1_000_000_000;

        let mut int96 = Int96::new();
        int96.set_data(
            nanos_in_day as u32,
            (nanos_in_day >> 32) as u32,
            julian_date,
        );

        let days_since_epoch = julian_date as i64 - JULIAN_UNIX_EPOCH as i64;
        let expected_nanos = days_since_epoch * NANOS_PER_DAY + nanos_in_day as i64;
        (int96, expected_nanos)
    }
}
