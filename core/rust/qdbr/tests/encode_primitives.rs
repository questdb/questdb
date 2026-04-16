mod common;

use arrow::array::{
    Array, BooleanArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, TimestampMicrosecondArray, TimestampMillisecondArray, UInt32Array,
};
use common::encode::{
    assert_single_column_bloom_metadata, generate_nulls, make_primitive_column,
    read_parquet_batches, write_parquet, write_parquet_with_bloom, ALL_BLOOM_FILTER_STATES,
    ALL_NULL_PATTERNS,
};
use common::types::primitives::*;
use qdb_core::col_type::{self, ColumnType};
use questdbr::parquet_write::schema::Partition;

use crate::common::Encoding;

const COUNT: usize = 1_000;

fn as_bytes<T>(data: &[T]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, std::mem::size_of_val(data)) }
}

fn encode_to_parquet<T: PrimitiveType>(
    data: &[T::T],
    row_count: usize,
    encoding: Encoding,
    bloom_enabled: bool,
) -> Vec<u8> {
    let bytes = as_bytes(data);
    let column = make_primitive_column(
        "col",
        ColumnType::new(T::TAG, 0).code(),
        bytes.as_ptr(),
        bytes.len(),
        row_count,
        encoding.config(),
    );
    let partition = Partition {
        table: "test_table".to_string(),
        columns: vec![column],
    };
    if bloom_enabled {
        write_parquet_with_bloom(partition)
    } else {
        write_parquet(partition)
    }
}

// =============================================================================
// EncodeVerify trait — per-type Arrow assertion
// =============================================================================

trait EncodeVerify: PrimitiveType {
    fn verify(parquet_bytes: &[u8], expected: &[Option<Self::T>], ctx: &str);
}

/// Macro for types whose Arrow array value type matches `Self::T` directly.
macro_rules! impl_verify_simple {
    ($type:ty, $arrow_arr:ty) => {
        impl EncodeVerify for $type {
            fn verify(
                parquet_bytes: &[u8],
                expected: &[Option<<Self as PrimitiveType>::T>],
                ctx: &str,
            ) {
                let batches = read_parquet_batches(parquet_bytes);
                let arr = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<$arrow_arr>()
                    .expect(stringify!($arrow_arr));
                assert_eq!(arr.len(), expected.len());
                for (i, exp) in expected.iter().enumerate() {
                    match exp {
                        Some(v) => {
                            assert!(!arr.is_null(i), "{ctx}: expected non-null at {i}");
                            assert_eq!(arr.value(i), *v, "{ctx}: mismatch at {i}");
                        }
                        None => {
                            assert!(arr.is_null(i), "{ctx}: expected null at {i}");
                        }
                    }
                }
            }
        }
    };
}

/// Macro for float types that need bitwise comparison.
macro_rules! impl_verify_float {
    ($type:ty, $arrow_arr:ty) => {
        impl EncodeVerify for $type {
            fn verify(
                parquet_bytes: &[u8],
                expected: &[Option<<Self as PrimitiveType>::T>],
                ctx: &str,
            ) {
                let batches = read_parquet_batches(parquet_bytes);
                let arr = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<$arrow_arr>()
                    .expect(stringify!($arrow_arr));
                assert_eq!(arr.len(), expected.len());
                for (i, exp) in expected.iter().enumerate() {
                    match exp {
                        Some(v) => {
                            assert!(!arr.is_null(i), "{ctx}: expected non-null at {i}");
                            assert_eq!(
                                arr.value(i).to_bits(),
                                v.to_bits(),
                                "{ctx}: mismatch at {i}"
                            );
                        }
                        None => {
                            assert!(arr.is_null(i), "{ctx}: expected null at {i}");
                        }
                    }
                }
            }
        }
    };
}

// Int8-backed
impl_verify_simple!(GeoByte, Int8Array);
impl_verify_simple!(Byte, Int8Array);

// Int16-backed
impl_verify_simple!(GeoShort, Int16Array);
impl_verify_simple!(Short, Int16Array);

// Int32-backed
impl_verify_simple!(Int, Int32Array);
impl_verify_simple!(GeoInt, Int32Array);

// IPv4 is stored as UInt32 in Parquet, but QuestDB uses i32 natively
impl EncodeVerify for IPv4 {
    fn verify(parquet_bytes: &[u8], expected: &[Option<<Self as PrimitiveType>::T>], ctx: &str) {
        let batches = read_parquet_batches(parquet_bytes);
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("UInt32Array");
        assert_eq!(arr.len(), expected.len());
        for (i, exp) in expected.iter().enumerate() {
            match exp {
                Some(v) => {
                    assert!(!arr.is_null(i), "{ctx}: expected non-null at {i}");
                    assert_eq!(arr.value(i) as i32, *v, "{ctx}: mismatch at {i}");
                }
                None => {
                    assert!(arr.is_null(i), "{ctx}: expected null at {i}");
                }
            }
        }
    }
}

// Int64-backed
impl_verify_simple!(Long, Int64Array);
impl_verify_simple!(GeoLong, Int64Array);
impl_verify_simple!(Date, TimestampMillisecondArray);
impl_verify_simple!(Timestamp, TimestampMicrosecondArray);

// Float-backed
impl_verify_float!(Float, Float32Array);
impl_verify_float!(Double, Float64Array);

// --- Boolean: u8 → BooleanArray ---

impl EncodeVerify for Boolean {
    fn verify(parquet_bytes: &[u8], expected: &[Option<u8>], ctx: &str) {
        let batches = read_parquet_batches(parquet_bytes);
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("BooleanArray");
        assert_eq!(arr.len(), expected.len());
        for (i, exp) in expected.iter().enumerate() {
            if let Some(v) = exp {
                assert_eq!(arr.value(i), *v != 0, "{ctx}: mismatch at {i}");
            }
        }
    }
}

// --- Char: Arrow rejects the schema, use QDB decoder ---

impl EncodeVerify for Char {
    fn verify(parquet_bytes: &[u8], expected: &[Option<i16>], ctx: &str) {
        let (decoded_data, decoded_aux) = common::decode_file(parquet_bytes);
        assert!(decoded_aux.is_empty());
        let count = expected.len();
        assert_eq!(decoded_data.len(), count * std::mem::size_of::<i16>());
        let decoded: &[i16] =
            unsafe { std::slice::from_raw_parts(decoded_data.as_ptr() as *const i16, count) };
        for (i, exp) in expected.iter().enumerate() {
            if let Some(v) = exp {
                assert_eq!(decoded[i], *v, "{ctx}: mismatch at {i}");
            }
        }
    }
}

// --- Long128: FixedSizeBinaryArray(16), no byte reversal ---

impl EncodeVerify for Long128 {
    fn verify(parquet_bytes: &[u8], expected: &[Option<col_type::Long128>], ctx: &str) {
        let batches = read_parquet_batches(parquet_bytes);
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("FixedSizeBinaryArray");
        assert_eq!(arr.len(), expected.len());
        for (i, exp) in expected.iter().enumerate() {
            match exp {
                Some(v) => {
                    assert!(!arr.is_null(i), "{ctx}: expected non-null at {i}");
                    let mut bytes = [0u8; 16];
                    bytes[0..8].copy_from_slice(&v.lo.to_le_bytes());
                    bytes[8..16].copy_from_slice(&v.hi.to_le_bytes());
                    assert_eq!(arr.value(i), &bytes[..], "{ctx}: mismatch at {i}");
                }
                None => {
                    assert!(arr.is_null(i), "{ctx}: expected null at {i}");
                }
            }
        }
    }
}

// --- Long256: FixedSizeBinaryArray(32), no byte reversal ---

impl EncodeVerify for Long256 {
    fn verify(parquet_bytes: &[u8], expected: &[Option<col_type::Long256>], ctx: &str) {
        let batches = read_parquet_batches(parquet_bytes);
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("FixedSizeBinaryArray");
        assert_eq!(arr.len(), expected.len());
        for (i, exp) in expected.iter().enumerate() {
            match exp {
                Some(v) => {
                    assert!(!arr.is_null(i), "{ctx}: expected non-null at {i}");
                    let mut bytes = [0u8; 32];
                    bytes[0..8].copy_from_slice(&v.l0.to_le_bytes());
                    bytes[8..16].copy_from_slice(&v.l1.to_le_bytes());
                    bytes[16..24].copy_from_slice(&v.l2.to_le_bytes());
                    bytes[24..32].copy_from_slice(&v.l3.to_le_bytes());
                    assert_eq!(arr.value(i), &bytes[..], "{ctx}: mismatch at {i}");
                }
                None => {
                    assert!(arr.is_null(i), "{ctx}: expected null at {i}");
                }
            }
        }
    }
}

// --- UUID: FixedSizeBinaryArray(16), encoder reverses all 16 bytes ---

impl EncodeVerify for Uuid {
    fn verify(parquet_bytes: &[u8], expected: &[Option<u128>], ctx: &str) {
        let batches = read_parquet_batches(parquet_bytes);
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("FixedSizeBinaryArray");
        assert_eq!(arr.len(), expected.len());
        for (i, exp) in expected.iter().enumerate() {
            match exp {
                Some(v) => {
                    assert!(!arr.is_null(i), "{ctx}: expected non-null at {i}");
                    let qdb_bytes = v.to_le_bytes();
                    let reversed: Vec<u8> = qdb_bytes.iter().rev().copied().collect();
                    assert_eq!(arr.value(i), reversed.as_slice(), "{ctx}: mismatch at {i}");
                }
                None => {
                    assert!(arr.is_null(i), "{ctx}: expected null at {i}");
                }
            }
        }
    }
}

// =============================================================================
// Generic test runner
// =============================================================================

fn run_encode_test<T: EncodeVerify>(name: &str) {
    let bloom_states: &[bool] = if matches!(T::TAG, qdb_core::col_type::ColumnTypeTag::Boolean) {
        &[false]
    } else {
        &ALL_BLOOM_FILTER_STATES
    };
    for &encoding in T::ENCODINGS {
        for &bloom_enabled in bloom_states {
            if T::HAS_NULL_SENTINEL {
                for &null_pattern in &ALL_NULL_PATTERNS {
                    let ctx = format!("{name} {encoding:?}/{null_pattern:?}/bloom={bloom_enabled}");
                    let nulls = generate_nulls(COUNT, null_pattern);
                    let mut data = Vec::with_capacity(COUNT);
                    let mut expected = Vec::with_capacity(COUNT);
                    let mut val_idx = 0;
                    for null in nulls.iter().take(COUNT) {
                        if *null {
                            data.push(T::NULL);
                            expected.push(None);
                        } else {
                            let v = T::generate_non_null_native(val_idx);
                            val_idx += 1;
                            data.push(v);
                            expected.push(Some(v));
                        }
                    }
                    let parquet_bytes =
                        encode_to_parquet::<T>(&data, COUNT, encoding, bloom_enabled);
                    if bloom_enabled {
                        assert_single_column_bloom_metadata(&parquet_bytes, true);
                    } else {
                        assert_single_column_bloom_metadata(&parquet_bytes, false);
                    }
                    T::verify(&parquet_bytes, &expected, &ctx);
                }
            } else {
                let ctx = format!("{name} {encoding:?}/bloom={bloom_enabled}");
                let data: Vec<T::T> = (0..COUNT).map(|i| T::generate_non_null_native(i)).collect();
                let expected: Vec<Option<T::T>> = data.iter().map(|&v| Some(v)).collect();
                let parquet_bytes = encode_to_parquet::<T>(&data, COUNT, encoding, bloom_enabled);
                if bloom_enabled {
                    assert_single_column_bloom_metadata(&parquet_bytes, true);
                } else {
                    assert_single_column_bloom_metadata(&parquet_bytes, false);
                }
                T::verify(&parquet_bytes, &expected, &ctx);
            }
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[test]
fn test_encode_boolean() {
    run_encode_test::<Boolean>("Boolean");
}

#[test]
fn test_encode_byte() {
    run_encode_test::<Byte>("Byte");
}

#[test]
fn test_encode_short() {
    run_encode_test::<Short>("Short");
}

#[test]
fn test_encode_char() {
    run_encode_test::<Char>("Char");
}

#[test]
fn test_encode_int() {
    run_encode_test::<Int>("Int");
}

#[test]
fn test_encode_ipv4() {
    run_encode_test::<IPv4>("IPv4");
}

#[test]
fn test_encode_long() {
    run_encode_test::<Long>("Long");
}

#[test]
fn test_encode_date() {
    run_encode_test::<Date>("Date");
}

#[test]
fn test_encode_timestamp() {
    run_encode_test::<Timestamp>("Timestamp");
}

#[test]
fn test_encode_float() {
    run_encode_test::<Float>("Float");
}

#[test]
fn test_encode_double() {
    run_encode_test::<Double>("Double");
}

#[test]
fn test_encode_geobyte() {
    run_encode_test::<GeoByte>("GeoByte");
}

#[test]
fn test_encode_geoshort() {
    run_encode_test::<GeoShort>("GeoShort");
}

#[test]
fn test_encode_geoint() {
    run_encode_test::<GeoInt>("GeoInt");
}

#[test]
fn test_encode_geolong() {
    run_encode_test::<GeoLong>("GeoLong");
}

#[test]
fn test_encode_long128() {
    run_encode_test::<Long128>("Long128");
}

#[test]
fn test_encode_long256() {
    run_encode_test::<Long256>("Long256");
}

#[test]
fn test_encode_uuid() {
    run_encode_test::<Uuid>("UUID");
}
