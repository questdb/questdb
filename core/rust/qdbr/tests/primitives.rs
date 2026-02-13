mod common;

use std::fmt::Debug;
use std::sync::Arc;

use parquet::{
    basic::{LogicalType, Repetition},
    data_type::{
        BoolType, DataType, DoubleType, FixedLenByteArray, FixedLenByteArrayType, FloatType,
        Int32Type, Int64Type, Int96, Int96Type,
    },
    file::properties::WriterVersion,
    schema::types::Type,
};
use qdb_core::col_type::{nulls, ColumnTypeTag};

use common::{
    decode_file, generate_nulls, qdb_props, write_parquet_column, Encoding, Null, ALL_NULLS, COUNT,
    VERSIONS,
};

trait PrimitiveType {
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

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T);

    fn eq(a: Self::T, b: Self::T) -> bool {
        a == b
    }
}

fn generate_data<T: PrimitiveType>(count: usize) -> (Vec<<T::U as DataType>::T>, Vec<T::T>) {
    let mut parquet = Vec::with_capacity(count);
    let mut native = Vec::with_capacity(count);

    for i in 0..count {
        let (p, n) = T::generate_data(i);
        parquet.push(p);
        native.push(n);
    }

    (parquet, native)
}

fn encode_data<T: PrimitiveType>(
    values: &[<T::U as DataType>::T],
    nulls: &[bool],
    version: WriterVersion,
    encoding: Encoding,
    repetition: Repetition,
) -> Vec<u8>
where
    T::T: Clone,
{
    let mut col_builder =
        Type::primitive_type_builder("col", <T::U as DataType>::get_physical_type())
            .with_repetition(repetition);
    if let Some(len) = T::FIXED_LEN {
        col_builder = col_builder.with_length(len);
    }
    if let Some(lt) = T::LOGICAL_TYPE {
        if let LogicalType::Decimal { scale, precision } = lt {
            col_builder = col_builder.with_precision(precision).with_scale(scale);
        }
        col_builder = col_builder.with_logical_type(Some(lt));
    }
    let schema = Type::group_type_builder("schema")
        .with_fields(vec![Arc::new(col_builder.build().unwrap())])
        .build()
        .unwrap();
    let def_levels = common::def_levels_from_nulls(nulls);
    let props = qdb_props(T::TAG, version, encoding);
    write_parquet_column::<T::U>("col", schema, values, Some(&def_levels), Arc::new(props))
}

fn assert_decoding<T: PrimitiveType>(expected: Vec<T::T>, nulls: Vec<bool>, actual: Vec<u8>) {
    let null_count = nulls.iter().filter(|x| **x).count();
    assert_eq!(
        actual.len(),
        (expected.len() + null_count) * size_of::<T::T>()
    );
    assert_eq!(actual.len(), nulls.len() * size_of::<T::T>());

    let actual = actual.as_ptr().cast::<T::T>();
    let mut expected_offset = 0;
    for i in 0..nulls.len() {
        let current = unsafe { std::ptr::read_unaligned(actual.add(i)) };
        let expected = if nulls[i] {
            T::NULL
        } else {
            let exp = expected[expected_offset];
            expected_offset += 1;
            exp
        };
        assert!(
            T::eq(current, expected),
            "mismatch at index {i}: {current:?} != {expected:?}"
        );
    }
}

fn run_primitive_test<T: PrimitiveType>(
    version: WriterVersion,
    encoding: Encoding,
    null: Null,
    repetition: Repetition,
) {
    let nulls = generate_nulls(COUNT, null);
    let null_count = nulls.iter().filter(|x| **x).count();
    let (parquet, native) = generate_data::<T>(COUNT - null_count);

    let parquet_file = encode_data::<T>(&parquet, &nulls, version, encoding, repetition);
    let (data, aux) = decode_file(&parquet_file);
    assert_decoding::<T>(native, nulls, data);
    assert!(aux.is_empty());
}

fn run_all_combos<T: PrimitiveType>(name: &str) {
    for version in &VERSIONS {
        for encoding in T::ENCODINGS {
            for null in &ALL_NULLS {
                let repetition = if matches!(null, Null::None) {
                    Repetition::REQUIRED
                } else {
                    Repetition::OPTIONAL
                };
                eprintln!(
                    "Testing {name} with version={version:?}, encoding={encoding:?}, null={null:?}"
                );
                run_primitive_test::<T>(*version, *encoding, *null, repetition);
            }
        }
    }
}

// --- Boolean type ---

struct Boolean;

impl PrimitiveType for Boolean {
    type T = u8;
    type U = BoolType;
    const NULL: Self::T = 0;
    const TAG: ColumnTypeTag = ColumnTypeTag::Boolean;
    const ENCODINGS: &[Encoding] = &[Encoding::Plain, Encoding::RleDictionary];

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let v = s % 2 == 0;
        (v, v as u8)
    }
}

// --- Int32-backed types ---

type GeoByte = ();

impl PrimitiveType for GeoByte {
    type T = i8;
    type U = Int32Type;
    const NULL: Self::T = nulls::GEOHASH_BYTE;
    const TAG: ColumnTypeTag = ColumnTypeTag::GeoByte;

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let v = (s % 127) as i8;
        (v as i32, v)
    }
}

struct GeoShort;

impl PrimitiveType for GeoShort {
    type T = i16;
    type U = Int32Type;
    const NULL: Self::T = nulls::GEOHASH_SHORT;
    const TAG: ColumnTypeTag = ColumnTypeTag::GeoShort;

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let v = (s % 32000) as i16;
        (v as i32, v)
    }
}

struct GeoInt;

impl PrimitiveType for GeoInt {
    type T = i32;
    type U = Int32Type;
    const NULL: Self::T = nulls::GEOHASH_INT;
    const TAG: ColumnTypeTag = ColumnTypeTag::GeoInt;

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let v = (s % i32::MAX as usize) as i32;
        (v, v)
    }
}

struct Byte;

impl PrimitiveType for Byte {
    type T = i8;
    type U = Int32Type;
    const NULL: Self::T = nulls::BYTE;
    const TAG: ColumnTypeTag = ColumnTypeTag::Byte;

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let v = (s % 127) as i8;
        (v as i32, v)
    }
}

struct Short;

impl PrimitiveType for Short {
    type T = i16;
    type U = Int32Type;
    const NULL: Self::T = nulls::SHORT;
    const TAG: ColumnTypeTag = ColumnTypeTag::Short;

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let v = (s % 32000) as i16;
        (v as i32, v)
    }
}

struct Int;

impl PrimitiveType for Int {
    type T = i32;
    type U = Int32Type;
    const NULL: Self::T = nulls::INT;
    const TAG: ColumnTypeTag = ColumnTypeTag::Int;

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let v = (s % i32::MAX as usize) as i32;
        (v, v)
    }
}

struct IPv4;

impl PrimitiveType for IPv4 {
    type T = i32;
    type U = Int32Type;
    const NULL: Self::T = nulls::IPV4;
    const TAG: ColumnTypeTag = ColumnTypeTag::IPv4;

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let v = (s % i32::MAX as usize) as i32;
        (v, v)
    }
}

// --- Int64-backed types ---

struct GeoLong;

impl PrimitiveType for GeoLong {
    type T = i64;
    type U = Int64Type;
    const NULL: Self::T = nulls::GEOHASH_LONG;
    const TAG: ColumnTypeTag = ColumnTypeTag::GeoLong;

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let v = s as i64;
        (v, v)
    }
}

struct Long;

impl PrimitiveType for Long {
    type T = i64;
    type U = Int64Type;
    const NULL: Self::T = nulls::LONG;
    const TAG: ColumnTypeTag = ColumnTypeTag::Long;

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let v = s as i64;
        (v, v)
    }
}

struct Timestamp;

impl PrimitiveType for Timestamp {
    type T = i64;
    type U = Int64Type;
    const NULL: Self::T = nulls::LONG;
    const TAG: ColumnTypeTag = ColumnTypeTag::Timestamp;

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let v = s as i64 * 1_000_000;
        (v, v)
    }
}

struct Date;

impl PrimitiveType for Date {
    type T = i64;
    type U = Int64Type;
    const NULL: Self::T = nulls::LONG;
    const TAG: ColumnTypeTag = ColumnTypeTag::Date;

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let v = s as i64 * 86_400_000;
        (v, v)
    }
}

struct DateInt32;

impl PrimitiveType for DateInt32 {
    type T = i64;
    type U = Int32Type;
    const NULL: Self::T = nulls::LONG;
    const TAG: ColumnTypeTag = ColumnTypeTag::Date;
    const ENCODINGS: &[Encoding] = &[Encoding::Plain];
    const LOGICAL_TYPE: Option<LogicalType> = Some(LogicalType::Date);

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let days = 18000 + s as i32;
        let millis = days as i64 * 86_400_000;
        (days, millis)
    }
}

// --- Float-backed types ---

struct Float;

impl PrimitiveType for Float {
    type T = f32;
    type U = FloatType;
    const NULL: Self::T = nulls::FLOAT;
    const TAG: ColumnTypeTag = ColumnTypeTag::Float;
    const ENCODINGS: &[Encoding] = &[Encoding::Plain, Encoding::RleDictionary];

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let v = s as f32 * 0.5;
        (v, v)
    }

    fn eq(a: Self::T, b: Self::T) -> bool {
        a.to_bits() == b.to_bits()
    }
}

struct DoubleInt32;

impl PrimitiveType for DoubleInt32 {
    type T = f64;
    type U = Int32Type;
    const NULL: Self::T = nulls::DOUBLE;
    const TAG: ColumnTypeTag = ColumnTypeTag::Double;
    const ENCODINGS: &[Encoding] = &[Encoding::Plain, Encoding::RleDictionary];
    const LOGICAL_TYPE: Option<LogicalType> = Some(LogicalType::Decimal {
        scale: 2,
        precision: 9,
    });

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let v = s as i32;
        let expected = v as f64 / 100.0;
        (v, expected)
    }

    fn eq(a: Self::T, b: Self::T) -> bool {
        a.to_bits() == b.to_bits()
    }
}

// --- Double-backed types ---

struct Double;

impl PrimitiveType for Double {
    type T = f64;
    type U = DoubleType;
    const NULL: Self::T = nulls::DOUBLE;
    const TAG: ColumnTypeTag = ColumnTypeTag::Double;
    const ENCODINGS: &[Encoding] = &[Encoding::Plain, Encoding::RleDictionary];

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let v = s as f64 * 0.5;
        (v, v)
    }

    fn eq(a: Self::T, b: Self::T) -> bool {
        a.to_bits() == b.to_bits()
    }
}

// --- FixedLenByteArray-backed types ---

struct Long128;

impl PrimitiveType for Long128 {
    type T = [u8; 16];
    type U = FixedLenByteArrayType;
    const NULL: Self::T = [0u8; 16];
    const TAG: ColumnTypeTag = ColumnTypeTag::Long128;
    const ENCODINGS: &[Encoding] = &[Encoding::Plain];
    const FIXED_LEN: Option<i32> = Some(16);

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let lo = (s as i64).to_le_bytes();
        let hi = (s as i64 + 5000).to_le_bytes();
        let mut bytes = [0u8; 16];
        bytes[0..8].copy_from_slice(&lo);
        bytes[8..16].copy_from_slice(&hi);
        (FixedLenByteArray::from(bytes.to_vec()), bytes)
    }
}

struct Long256;

const LONG256_NULL: [u8; 32] = [
    0, 0, 0, 0, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0,
    0, 128,
];

impl PrimitiveType for Long256 {
    type T = [u8; 32];
    type U = FixedLenByteArrayType;
    const NULL: Self::T = LONG256_NULL;
    const TAG: ColumnTypeTag = ColumnTypeTag::Long256;
    const ENCODINGS: &[Encoding] = &[Encoding::Plain];
    const FIXED_LEN: Option<i32> = Some(32);

    fn generate_data(s: usize) -> (<Self::U as DataType>::T, Self::T) {
        let mut bytes = [0u8; 32];
        bytes[0..8].copy_from_slice(&(s as i64).to_le_bytes());
        bytes[8..16].copy_from_slice(&((s as i64 + 1000).to_le_bytes()));
        bytes[16..24].copy_from_slice(&((s as i64 + 2000).to_le_bytes()));
        bytes[24..32].copy_from_slice(&((s as i64 + 3000).to_le_bytes()));
        (FixedLenByteArray::from(bytes.to_vec()), bytes)
    }
}

// --- Int96-backed types ---

struct TimestampInt96;

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

// --- Tests ---

#[test]
fn test_boolean() {
    run_all_combos::<Boolean>("Boolean");
}

#[test]
fn test_geobyte() {
    run_all_combos::<GeoByte>("GeoByte");
}

#[test]
fn test_geoshort() {
    run_all_combos::<GeoShort>("GeoShort");
}

#[test]
fn test_geoint() {
    run_all_combos::<GeoInt>("GeoInt");
}

#[test]
fn test_geolong() {
    run_all_combos::<GeoLong>("GeoLong");
}

#[test]
fn test_byte() {
    run_all_combos::<Byte>("Byte");
}

#[test]
fn test_short() {
    run_all_combos::<Short>("Short");
}

#[test]
fn test_int() {
    run_all_combos::<Int>("Int");
}

#[test]
fn test_ipv4() {
    run_all_combos::<IPv4>("IPv4");
}

#[test]
fn test_long() {
    run_all_combos::<Long>("Long");
}

#[test]
fn test_timestamp() {
    run_all_combos::<Timestamp>("Timestamp");
}

#[test]
fn test_date() {
    run_all_combos::<Date>("Date");
}

#[test]
fn test_date_int32() {
    run_all_combos::<DateInt32>("DateInt32");
}

#[test]
fn test_float() {
    run_all_combos::<Float>("Float");
}

#[test]
fn test_double() {
    run_all_combos::<Double>("Double");
}

#[test]
fn test_double_int32() {
    run_all_combos::<DoubleInt32>("DoubleInt32");
}

#[test]
fn test_long128() {
    run_all_combos::<Long128>("Long128");
}

#[test]
fn test_long256() {
    run_all_combos::<Long256>("Long256");
}

#[test]
fn test_timestamp_int96() {
    run_all_combos::<TimestampInt96>("TimestampInt96");
}
