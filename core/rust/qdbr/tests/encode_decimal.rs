mod common;

use arrow::array::{Array, Decimal128Array, Decimal256Array};
use arrow::record_batch::RecordBatch;
use common::encode::{
    assert_single_column_bloom_metadata, generate_nulls, make_primitive_column,
    read_parquet_batches, write_parquet_no_stats, write_parquet_no_stats_with_bloom,
    ALL_BLOOM_FILTER_STATES, ALL_NULL_PATTERNS,
};
use common::types::primitives::rnd;
use qdb_core::col_type::ColumnType;
use questdbr::parquet_write::schema::Partition;

use crate::common::Encoding;

const COUNT: usize = 500;

const DECIMAL_ENCODINGS: [Encoding; 2] = [Encoding::Plain, Encoding::RleDictionary];

fn as_bytes<T>(data: &[T]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, std::mem::size_of_val(data)) }
}

fn encode_and_read(
    col_type: ColumnType,
    data: &[u8],
    row_count: usize,
    encoding: Encoding,
    bloom_enabled: bool,
) -> Vec<RecordBatch> {
    let column = make_primitive_column(
        "col",
        col_type.code(),
        data.as_ptr(),
        data.len(),
        row_count,
        encoding.config(),
    );
    let partition = Partition {
        table: "test_table".to_string(),
        columns: vec![column],
    };
    let bytes = if bloom_enabled {
        write_parquet_no_stats_with_bloom(partition)
    } else {
        write_parquet_no_stats(partition)
    };
    assert_single_column_bloom_metadata(&bytes, bloom_enabled);
    read_parquet_batches(&bytes)
}

// =============================================================================
// Decimal32 (precision 5, scale 2) -> Arrow Decimal128Array
// QDB stores i32 (LE), encoder writes 4-byte BE FLBA
// =============================================================================

#[test]
fn test_encode_decimal32() {
    let col_type = ColumnType::new_decimal(5, 2).expect("decimal32");
    for encoding in &DECIMAL_ENCODINGS {
        for bloom_enabled in ALL_BLOOM_FILTER_STATES {
            for null_pattern in &ALL_NULL_PATTERNS {
                let nulls = generate_nulls(COUNT, *null_pattern);
                let mut data: Vec<i32> = Vec::with_capacity(COUNT);
                let mut expected: Vec<Option<i128>> = Vec::with_capacity(COUNT);
                let mut val_idx = 0;
                for null in nulls.iter().take(COUNT) {
                    if *null {
                        data.push(i32::MIN); // Decimal32 null sentinel
                        expected.push(None);
                    } else {
                        let v = (rnd(val_idx) % 99_999) as i32;
                        val_idx += 1;
                        data.push(v);
                        expected.push(Some(v as i128));
                    }
                }
                let batches =
                    encode_and_read(col_type, as_bytes(&data), COUNT, *encoding, bloom_enabled);
                let arr = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .expect("Decimal128Array");
                assert_eq!(arr.len(), COUNT);
                for (i, exp) in expected.iter().enumerate() {
                    match exp {
                        Some(v) => {
                            assert!(
                                !arr.is_null(i),
                                "Decimal32 {encoding:?}/{null_pattern:?}/bloom={bloom_enabled}: expected non-null at {i}"
                            );
                            assert_eq!(
                                arr.value(i),
                                *v,
                                "Decimal32 {encoding:?}/{null_pattern:?}/bloom={bloom_enabled}: mismatch at {i}"
                            );
                        }
                        None => {
                            assert!(
                                arr.is_null(i),
                                "Decimal32 {encoding:?}/{null_pattern:?}/bloom={bloom_enabled}: expected null at {i}"
                            );
                        }
                    }
                }
            }
        }
    }
}

// =============================================================================
// Decimal64 (precision 10, scale 3) -> Arrow Decimal128Array
// QDB stores i64 (LE), encoder writes 8-byte BE FLBA
// =============================================================================

#[test]
fn test_encode_decimal64() {
    let col_type = ColumnType::new_decimal(10, 3).expect("decimal64");
    for encoding in &DECIMAL_ENCODINGS {
        for bloom_enabled in ALL_BLOOM_FILTER_STATES {
            for null_pattern in &ALL_NULL_PATTERNS {
                let nulls = generate_nulls(COUNT, *null_pattern);
                let mut data: Vec<i64> = Vec::with_capacity(COUNT);
                let mut expected: Vec<Option<i128>> = Vec::with_capacity(COUNT);
                let mut val_idx = 0;
                for null in nulls.iter().take(COUNT) {
                    if *null {
                        data.push(i64::MIN); // Decimal64 null sentinel
                        expected.push(None);
                    } else {
                        let v = rnd(val_idx) as i64 % 9_999_999_999;
                        val_idx += 1;
                        data.push(v);
                        expected.push(Some(v as i128));
                    }
                }
                let batches =
                    encode_and_read(col_type, as_bytes(&data), COUNT, *encoding, bloom_enabled);
                let arr = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .expect("Decimal128Array");
                assert_eq!(arr.len(), COUNT);
                for (i, exp) in expected.iter().enumerate() {
                    match exp {
                        Some(v) => {
                            assert!(
                                !arr.is_null(i),
                                "Decimal64 {encoding:?}/{null_pattern:?}/bloom={bloom_enabled}: expected non-null at {i}"
                            );
                            assert_eq!(
                                arr.value(i),
                                *v,
                                "Decimal64 {encoding:?}/{null_pattern:?}/bloom={bloom_enabled}: mismatch at {i}"
                            );
                        }
                        None => {
                            assert!(
                                arr.is_null(i),
                                "Decimal64 {encoding:?}/{null_pattern:?}/bloom={bloom_enabled}: expected null at {i}"
                            );
                        }
                    }
                }
            }
        }
    }
}

// =============================================================================
// Decimal8 (precision 2, scale 1) -> Arrow Decimal128Array
// QDB stores i8 (LE), encoder writes 1-byte BE FLBA
// =============================================================================

#[test]
fn test_encode_decimal8() {
    let col_type = ColumnType::new_decimal(2, 1).expect("decimal8");
    for encoding in &DECIMAL_ENCODINGS {
        for bloom_enabled in ALL_BLOOM_FILTER_STATES {
            for null_pattern in &ALL_NULL_PATTERNS {
                let nulls = generate_nulls(COUNT, *null_pattern);
                let mut data: Vec<i8> = Vec::with_capacity(COUNT);
                let mut expected: Vec<Option<i128>> = Vec::with_capacity(COUNT);
                let mut val_idx = 0;
                for null in nulls.iter().take(COUNT) {
                    if *null {
                        data.push(i8::MIN); // Decimal8 null sentinel
                        expected.push(None);
                    } else {
                        let v = ((rnd(val_idx) % 99) as i8).max(-127); // avoid MIN
                        val_idx += 1;
                        data.push(v);
                        expected.push(Some(v as i128));
                    }
                }
                let batches =
                    encode_and_read(col_type, as_bytes(&data), COUNT, *encoding, bloom_enabled);
                let arr = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .expect("Decimal128Array");
                assert_eq!(arr.len(), COUNT);
                for (i, exp) in expected.iter().enumerate() {
                    match exp {
                        Some(v) => {
                            assert!(
                                !arr.is_null(i),
                                "Decimal8 {encoding:?}/{null_pattern:?}/bloom={bloom_enabled}: expected non-null at {i}"
                            );
                            assert_eq!(
                                arr.value(i),
                                *v,
                                "Decimal8 {encoding:?}/{null_pattern:?}/bloom={bloom_enabled}: mismatch at {i}"
                            );
                        }
                        None => {
                            assert!(
                                arr.is_null(i),
                                "Decimal8 {encoding:?}/{null_pattern:?}/bloom={bloom_enabled}: expected null at {i}"
                            );
                        }
                    }
                }
            }
        }
    }
}

// =============================================================================
// Decimal16 (precision 4, scale 2) -> Arrow Decimal128Array
// QDB stores i16 (LE), encoder writes 2-byte BE FLBA
// =============================================================================

#[test]
fn test_encode_decimal16() {
    let col_type = ColumnType::new_decimal(4, 2).expect("decimal16");
    for encoding in &DECIMAL_ENCODINGS {
        for bloom_enabled in ALL_BLOOM_FILTER_STATES {
            for null_pattern in &ALL_NULL_PATTERNS {
                let nulls = generate_nulls(COUNT, *null_pattern);
                let mut data: Vec<i16> = Vec::with_capacity(COUNT);
                let mut expected: Vec<Option<i128>> = Vec::with_capacity(COUNT);
                let mut val_idx = 0;
                for null in nulls.iter().take(COUNT) {
                    if *null {
                        data.push(i16::MIN); // Decimal16 null sentinel
                        expected.push(None);
                    } else {
                        let v = ((rnd(val_idx) % 9_999) as i16).max(-32_767); // avoid MIN
                        val_idx += 1;
                        data.push(v);
                        expected.push(Some(v as i128));
                    }
                }
                let batches =
                    encode_and_read(col_type, as_bytes(&data), COUNT, *encoding, bloom_enabled);
                let arr = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .expect("Decimal128Array");
                assert_eq!(arr.len(), COUNT);
                for (i, exp) in expected.iter().enumerate() {
                    match exp {
                        Some(v) => {
                            assert!(
                                !arr.is_null(i),
                                "Decimal16 {encoding:?}/{null_pattern:?}/bloom={bloom_enabled}: expected non-null at {i}"
                            );
                            assert_eq!(
                                arr.value(i),
                                *v,
                                "Decimal16 {encoding:?}/{null_pattern:?}/bloom={bloom_enabled}: mismatch at {i}"
                            );
                        }
                        None => {
                            assert!(
                                arr.is_null(i),
                                "Decimal16 {encoding:?}/{null_pattern:?}/bloom={bloom_enabled}: expected null at {i}"
                            );
                        }
                    }
                }
            }
        }
    }
}

// =============================================================================
// Decimal128 (precision 20, scale 4) -> Arrow Decimal128Array
// QDB stores (i64 hi, u64 lo) LE, encoder writes 16-byte BE FLBA
// =============================================================================

#[test]
fn test_encode_decimal128() {
    let col_type = ColumnType::new_decimal(20, 4).expect("decimal128");
    for encoding in &DECIMAL_ENCODINGS {
        for bloom_enabled in ALL_BLOOM_FILTER_STATES {
            for null_pattern in &ALL_NULL_PATTERNS {
                let nulls = generate_nulls(COUNT, *null_pattern);
                // Decimal128 is (i64, u64) = 16 bytes. We build raw bytes.
                let mut data_bytes: Vec<u8> = Vec::with_capacity(COUNT * 16);
                let mut expected: Vec<Option<i128>> = Vec::with_capacity(COUNT);
                let mut val_idx = 0;
                for null in nulls.iter().take(COUNT) {
                    if *null {
                        // Null: (i64::MIN, 0)
                        data_bytes.extend_from_slice(&i64::MIN.to_le_bytes());
                        data_bytes.extend_from_slice(&0u64.to_le_bytes());
                        expected.push(None);
                    } else {
                        let s = rnd(val_idx);
                        val_idx += 1;
                        let hi = s as i64;
                        let lo = (s as u64).wrapping_mul(17);
                        data_bytes.extend_from_slice(&hi.to_le_bytes());
                        data_bytes.extend_from_slice(&lo.to_le_bytes());
                        // The encoder writes to_bytes() which is hi.to_be ++ lo.to_be (16 bytes BE)
                        // Arrow reads this 16-byte BE as i128
                        let val_i128 = ((hi as i128) << 64) | (lo as i128);
                        expected.push(Some(val_i128));
                    }
                }
                let batches =
                    encode_and_read(col_type, &data_bytes, COUNT, *encoding, bloom_enabled);
                let arr = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .expect("Decimal128Array");
                assert_eq!(arr.len(), COUNT);
                for (i, exp) in expected.iter().enumerate() {
                    match exp {
                        Some(v) => {
                            assert!(
                                !arr.is_null(i),
                                "Decimal128 {encoding:?}/{null_pattern:?}/bloom={bloom_enabled}: expected non-null at {i}"
                            );
                            assert_eq!(
                                arr.value(i),
                                *v,
                                "Decimal128 {encoding:?}/{null_pattern:?}/bloom={bloom_enabled}: mismatch at {i}"
                            );
                        }
                        None => {
                            assert!(
                                arr.is_null(i),
                                "Decimal128 {encoding:?}/{null_pattern:?}/bloom={bloom_enabled}: expected null at {i}"
                            );
                        }
                    }
                }
            }
        }
    }
}

// =============================================================================
// Decimal256 (precision 40, scale 5) -> Arrow Decimal256Array
// QDB stores (i64, u64, u64, u64) LE, encoder writes 32-byte BE FLBA
// =============================================================================

#[test]
fn test_encode_decimal256() {
    let col_type = ColumnType::new_decimal(40, 5).expect("decimal256");
    for encoding in &DECIMAL_ENCODINGS {
        for bloom_enabled in ALL_BLOOM_FILTER_STATES {
            for null_pattern in &ALL_NULL_PATTERNS {
                let nulls = generate_nulls(COUNT, *null_pattern);
                let mut data_bytes: Vec<u8> = Vec::with_capacity(COUNT * 32);
                let mut expected: Vec<Option<arrow::datatypes::i256>> = Vec::with_capacity(COUNT);
                let mut val_idx = 0;
                for null in nulls.iter().take(COUNT) {
                    if *null {
                        // Null: (i64::MIN, 0, 0, 0)
                        data_bytes.extend_from_slice(&i64::MIN.to_le_bytes());
                        data_bytes.extend_from_slice(&0u64.to_le_bytes());
                        data_bytes.extend_from_slice(&0u64.to_le_bytes());
                        data_bytes.extend_from_slice(&0u64.to_le_bytes());
                        expected.push(None);
                    } else {
                        let s = rnd(val_idx);
                        val_idx += 1;
                        let hh = s as i64;
                        let hi = (s as u64).wrapping_mul(3);
                        let lo = (s as u64).wrapping_mul(7);
                        let ll = (s as u64).wrapping_mul(13);
                        data_bytes.extend_from_slice(&hh.to_le_bytes());
                        data_bytes.extend_from_slice(&hi.to_le_bytes());
                        data_bytes.extend_from_slice(&lo.to_le_bytes());
                        data_bytes.extend_from_slice(&ll.to_le_bytes());
                        // Encoder writes 32 bytes BE: hh.to_be ++ hi.to_be ++ lo.to_be ++ ll.to_be
                        // Arrow reads as 256-bit BE integer stored as i256
                        let high = ((hh as i128) << 64) | (hi as i128);
                        let low = ((lo as u128) << 64) | (ll as u128);
                        let val = arrow::datatypes::i256::from_parts(low, high);
                        expected.push(Some(val));
                    }
                }
                let batches =
                    encode_and_read(col_type, &data_bytes, COUNT, *encoding, bloom_enabled);
                let arr = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Decimal256Array>()
                    .expect("Decimal256Array");
                assert_eq!(arr.len(), COUNT);
                for (i, exp) in expected.iter().enumerate() {
                    match exp {
                        Some(v) => {
                            assert!(
                                !arr.is_null(i),
                                "Decimal256 {encoding:?}/{null_pattern:?}/bloom={bloom_enabled}: expected non-null at {i}"
                            );
                            assert_eq!(
                                arr.value(i),
                                *v,
                                "Decimal256 {encoding:?}/{null_pattern:?}/bloom={bloom_enabled}: mismatch at {i}"
                            );
                        }
                        None => {
                            assert!(
                                arr.is_null(i),
                                "Decimal256 {encoding:?}/{null_pattern:?}/bloom={bloom_enabled}: expected null at {i}"
                            );
                        }
                    }
                }
            }
        }
    }
}
