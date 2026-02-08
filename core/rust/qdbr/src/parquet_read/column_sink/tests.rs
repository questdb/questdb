use crate::allocator::{AcVec, TestAllocatorState};
use crate::parquet_read::column_sink::fixed::{
    FixedColumnSink, IntDecimalColumnSink, NanoTimestampColumnSink, ReverseFixedColumnSink,
    SignExtendDecimalColumnSink, WordSwapDecimalColumnSink,
};
use crate::parquet_read::column_sink::Pushable;
use crate::parquet_read::slicer::DataPageFixedSlicer;
use crate::parquet_read::ColumnChunkBuffers;
use crate::parquet_write::decimal::{
    DECIMAL128_NULL, DECIMAL16_NULL, DECIMAL256_NULL, DECIMAL32_NULL, DECIMAL64_NULL,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::ptr;

fn create_buffers(allocator: &crate::allocator::QdbAllocator) -> ColumnChunkBuffers {
    ColumnChunkBuffers {
        data_size: 0,
        data_ptr: ptr::null_mut(),
        data_vec: AcVec::new_in(allocator.clone()),
        aux_size: 0,
        aux_ptr: ptr::null_mut(),
        aux_vec: AcVec::new_in(allocator.clone()),
    }
}

static INT_NULL: [u8; 4] = [0x00, 0x00, 0x00, 0x80]; // i32::MIN
static LONG_NULL: [u8; 8] = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80]; // i64::MIN

#[test]
fn test_fixed_column_sink_push_slice_4byte() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut buffers = create_buffers(&allocator);

    let data: Vec<u8> = vec![1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0];
    let mut slicer = DataPageFixedSlicer::<4>::new(&data, 5);

    {
        let mut sink = FixedColumnSink::<4, 4, _>::new(&mut slicer, &mut buffers, &INT_NULL);
        sink.reserve(5).unwrap();
        sink.push_slice(3).unwrap();
    }

    let result: Vec<i32> = buffers
        .data_vec
        .chunks(4)
        .map(|c| i32::from_le_bytes(c.try_into().unwrap()))
        .collect();
    assert_eq!(result, vec![1, 2, 3]);

    {
        let mut sink = FixedColumnSink::<4, 4, _>::new(&mut slicer, &mut buffers, &INT_NULL);
        sink.push_slice(2).unwrap();
    }

    let result: Vec<i32> = buffers
        .data_vec
        .chunks(4)
        .map(|c| i32::from_le_bytes(c.try_into().unwrap()))
        .collect();
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
}

#[test]
fn test_fixed_column_sink_push_slice_8byte() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut buffers = create_buffers(&allocator);

    let data: Vec<u8> = (1i64..=5).flat_map(|v| v.to_le_bytes()).collect();
    let mut slicer = DataPageFixedSlicer::<8>::new(&data, 5);
    let mut sink = FixedColumnSink::<8, 8, _>::new(&mut slicer, &mut buffers, &LONG_NULL);

    sink.reserve(5).unwrap();
    sink.push_slice(5).unwrap();

    let result: Vec<i64> = buffers
        .data_vec
        .chunks(8)
        .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
        .collect();
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
}

#[test]
fn test_fixed_column_sink_push_nulls_small() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();

    for count in 0..=4 {
        let mut buffers = create_buffers(&allocator);
        let data: Vec<u8> = vec![1, 0, 0, 0];
        let mut slicer = DataPageFixedSlicer::<4>::new(&data, 1);
        let mut sink = FixedColumnSink::<4, 4, _>::new(&mut slicer, &mut buffers, &INT_NULL);

        sink.reserve(count).unwrap();
        sink.push_nulls(count).unwrap();

        assert_eq!(buffers.data_vec.len(), count * 4, "count={}", count);
        for chunk in buffers.data_vec.chunks(4) {
            assert_eq!(chunk, &INT_NULL, "count={}", count);
        }
    }
}

#[test]
fn test_fixed_column_sink_push_nulls_large() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut buffers = create_buffers(&allocator);

    let data: Vec<u8> = vec![1, 0, 0, 0];
    let mut slicer = DataPageFixedSlicer::<4>::new(&data, 1);
    let mut sink = FixedColumnSink::<4, 4, _>::new(&mut slicer, &mut buffers, &INT_NULL);

    sink.reserve(10).unwrap();
    sink.push_nulls(10).unwrap();

    assert_eq!(buffers.data_vec.len(), 40);
    for chunk in buffers.data_vec.chunks(4) {
        assert_eq!(chunk, &INT_NULL);
    }
}

#[test]
fn test_fixed_column_sink_mixed_push_and_nulls() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut buffers = create_buffers(&allocator);

    let data: Vec<u8> = (1i32..=10).flat_map(|v| v.to_le_bytes()).collect();
    let mut slicer = DataPageFixedSlicer::<4>::new(&data, 10);
    let mut sink = FixedColumnSink::<4, 4, _>::new(&mut slicer, &mut buffers, &INT_NULL);

    sink.reserve(15).unwrap();
    sink.push_slice(2).unwrap(); // [1, 2]
    sink.push_nulls(3).unwrap(); // [null, null, null]
    sink.push_slice(3).unwrap(); // [3, 4, 5]
    sink.push_nulls(1).unwrap(); // [null]

    let result: Vec<i32> = buffers
        .data_vec
        .chunks(4)
        .map(|c| i32::from_le_bytes(c.try_into().unwrap()))
        .collect();
    assert_eq!(
        result,
        vec![1, 2, i32::MIN, i32::MIN, i32::MIN, 3, 4, 5, i32::MIN]
    );
}

#[test]
fn test_fixed_column_sink_int_to_short() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut buffers = create_buffers(&allocator);

    let data: Vec<u8> = vec![
        1, 0, 0, 0, // 1
        2, 0, 0, 0, // 2
        255, 0, 0, 0, // 255
    ];
    let mut slicer = DataPageFixedSlicer::<4>::new(&data, 3);

    static SHORT_NULL: [u8; 2] = [0x00, 0x80];
    let mut sink = FixedColumnSink::<2, 4, _>::new(&mut slicer, &mut buffers, &SHORT_NULL);

    sink.reserve(3).unwrap();
    sink.push_slice(3).unwrap();

    let result: Vec<i16> = buffers
        .data_vec
        .chunks(2)
        .map(|c| i16::from_le_bytes(c.try_into().unwrap()))
        .collect();
    assert_eq!(result, vec![1, 2, 255]);
}

#[test]
fn test_reverse_fixed_column_sink_push_slice() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut buffers = create_buffers(&allocator);

    // Big-endian data: 0x01020304, 0x05060708
    let data: Vec<u8> = vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
    let mut slicer = DataPageFixedSlicer::<4>::new(&data, 2);
    let mut sink = ReverseFixedColumnSink::<4, _>::new(&mut slicer, &mut buffers, INT_NULL);

    sink.reserve(2).unwrap();
    sink.push_slice(2).unwrap();

    // Should be reversed to little-endian
    assert_eq!(
        buffers.data_vec.as_slice(),
        &[0x04, 0x03, 0x02, 0x01, 0x08, 0x07, 0x06, 0x05]
    );
}

#[test]
fn test_reverse_fixed_column_sink_push_slice_small_counts() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();

    for count in 1..=4 {
        let mut buffers = create_buffers(&allocator);
        let data: Vec<u8> = (0..count * 4).map(|i| i as u8).collect();
        let mut slicer = DataPageFixedSlicer::<4>::new(&data, count);
        let mut sink = ReverseFixedColumnSink::<4, _>::new(&mut slicer, &mut buffers, INT_NULL);

        sink.reserve(count).unwrap();
        sink.push_slice(count).unwrap();

        for (i, chunk) in buffers.data_vec.chunks(4).enumerate() {
            let expected: Vec<u8> = (0..4).map(|j| (i * 4 + 3 - j) as u8).collect();
            assert_eq!(chunk, expected.as_slice(), "count={}, chunk={}", count, i);
        }
    }
}

#[test]
fn test_reverse_fixed_column_sink_push_nulls() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut buffers = create_buffers(&allocator);

    let data: Vec<u8> = vec![0x01, 0x02, 0x03, 0x04];
    let mut slicer = DataPageFixedSlicer::<4>::new(&data, 1);
    let null_value = [0xDE, 0xAD, 0xBE, 0xEF];
    let mut sink = ReverseFixedColumnSink::<4, _>::new(&mut slicer, &mut buffers, null_value);

    sink.reserve(6).unwrap();
    sink.push_nulls(6).unwrap();

    assert_eq!(buffers.data_vec.len(), 24);
    for chunk in buffers.data_vec.chunks(4) {
        assert_eq!(chunk, &null_value);
    }
}

#[test]
fn test_nano_timestamp_column_sink_push_slice() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut buffers = create_buffers(&allocator);

    let mut data = Vec::new();
    data.extend_from_slice(&0u64.to_le_bytes()); // nanos
    data.extend_from_slice(&2440588u32.to_le_bytes()); // julian
                                                       // Second timestamp: epoch + 1 day + 1000 nanos
    data.extend_from_slice(&1000u64.to_le_bytes()); // nanos
    data.extend_from_slice(&2440589u32.to_le_bytes()); // julian (next day)

    let mut slicer = DataPageFixedSlicer::<12>::new(&data, 2);
    static TS_NULL: [u8; 8] = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80];
    let mut sink = NanoTimestampColumnSink::new(&mut slicer, &mut buffers, &TS_NULL);

    sink.reserve(2).unwrap();
    sink.push_slice(2).unwrap();

    let result: Vec<i64> = buffers
        .data_vec
        .chunks(8)
        .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
        .collect();

    assert_eq!(result[0], 0);
    assert_eq!(result[1], 86400 * 1_000_000_000 + 1000);
}

#[test]
fn test_nano_timestamp_column_sink_push_nulls() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut buffers = create_buffers(&allocator);

    let data: Vec<u8> = vec![0; 12];
    let mut slicer = DataPageFixedSlicer::<12>::new(&data, 1);
    static TS_NULL: [u8; 8] = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80];
    let mut sink = NanoTimestampColumnSink::new(&mut slicer, &mut buffers, &TS_NULL);

    sink.reserve(5).unwrap();
    sink.push_nulls(5).unwrap();

    assert_eq!(buffers.data_vec.len(), 40);
    for chunk in buffers.data_vec.chunks(8) {
        assert_eq!(chunk, &TS_NULL);
    }
}

#[test]
fn test_int_decimal_column_sink_push_slice() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut buffers = create_buffers(&allocator);

    let data: Vec<u8> = vec![
        0x39, 0x30, 0x00, 0x00, // 12345
        0x78, 0xEC, 0xFF, 0xFF, // -5000
    ];
    let mut slicer = DataPageFixedSlicer::<4>::new(&data, 2);
    static DOUBLE_NULL: [u8; 8] = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF8, 0x7F];
    let mut sink = IntDecimalColumnSink::new(&mut slicer, &mut buffers, &DOUBLE_NULL, 2);

    sink.reserve(2).unwrap();
    sink.push_slice(2).unwrap();

    let result: Vec<f64> = buffers
        .data_vec
        .chunks(8)
        .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
        .collect();

    assert!((result[0] - 123.45).abs() < 0.001);
    assert!((result[1] - (-50.0)).abs() < 0.001);
}

#[test]
fn test_int_decimal_column_sink_push_slice_small_counts() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();

    for count in 1..=4 {
        let mut buffers = create_buffers(&allocator);
        let data: Vec<u8> = (1i32..=count as i32)
            .flat_map(|v| (v * 100).to_le_bytes())
            .collect();
        let mut slicer = DataPageFixedSlicer::<4>::new(&data, count);
        static DOUBLE_NULL: [u8; 8] = [0; 8];
        let mut sink = IntDecimalColumnSink::new(&mut slicer, &mut buffers, &DOUBLE_NULL, 2);

        sink.reserve(count).unwrap();
        sink.push_slice(count).unwrap();

        let result: Vec<f64> = buffers
            .data_vec
            .chunks(8)
            .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
            .collect();

        for (i, &val) in result.iter().enumerate() {
            let expected = (i + 1) as f64;
            assert!((val - expected).abs() < 0.001, "count={}, i={}", count, i);
        }
    }
}

#[test]
fn test_int_decimal_column_sink_push_nulls() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut buffers = create_buffers(&allocator);

    let data: Vec<u8> = vec![0; 4];
    let mut slicer = DataPageFixedSlicer::<4>::new(&data, 1);
    static DOUBLE_NULL: [u8; 8] = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF8, 0x7F];
    let mut sink = IntDecimalColumnSink::new(&mut slicer, &mut buffers, &DOUBLE_NULL, 2);

    sink.reserve(7).unwrap();
    sink.push_nulls(7).unwrap();

    assert_eq!(buffers.data_vec.len(), 56);
    for chunk in buffers.data_vec.chunks(8) {
        let val = f64::from_le_bytes(chunk.try_into().unwrap());
        assert!(val.is_nan());
    }
}

#[test]
fn test_word_swap_decimal128_push_slice() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut buffers = create_buffers(&allocator);

    let data: Vec<u8> = vec![
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E,
        0x1F, 0x20,
    ];
    let mut slicer = DataPageFixedSlicer::<16>::new(&data, 2);
    let mut sink =
        WordSwapDecimalColumnSink::<16, 2, _>::new(&mut slicer, &mut buffers, DECIMAL128_NULL);

    sink.reserve(2).unwrap();
    sink.push_slice(2).unwrap();

    let expected: Vec<u8> = vec![
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A,
        0x09, 0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11, 0x20, 0x1F, 0x1E, 0x1D, 0x1C, 0x1B,
        0x1A, 0x19,
    ];

    assert_eq!(buffers.data_vec.as_slice(), expected.as_slice());
}

#[test]
fn test_word_swap_decimal128_push_nulls() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut buffers = create_buffers(&allocator);

    let data: Vec<u8> = vec![0; 16];
    let mut slicer = DataPageFixedSlicer::<16>::new(&data, 6);
    let mut sink =
        WordSwapDecimalColumnSink::<16, 2, _>::new(&mut slicer, &mut buffers, DECIMAL128_NULL);

    sink.reserve(6).unwrap();
    sink.push_nulls(6).unwrap();

    assert_eq!(buffers.data_vec.len(), 96);
    for chunk in buffers.data_vec.chunks(16) {
        assert_eq!(chunk, &DECIMAL128_NULL);
    }
}

#[test]
fn test_word_swap_decimal256_push_slice() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut buffers = create_buffers(&allocator);

    let data: Vec<u8> = vec![
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E,
        0x1F, 0x20,
    ];
    let mut slicer = DataPageFixedSlicer::<32>::new(&data, 1);
    let mut sink =
        WordSwapDecimalColumnSink::<32, 4, _>::new(&mut slicer, &mut buffers, DECIMAL256_NULL);

    sink.reserve(1).unwrap();
    sink.push_slice(1).unwrap();

    let expected: Vec<u8> = vec![
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A,
        0x09, 0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11, 0x20, 0x1F, 0x1E, 0x1D, 0x1C, 0x1B,
        0x1A, 0x19,
    ];

    assert_eq!(buffers.data_vec.as_slice(), expected.as_slice());
}

#[test]
fn test_word_swap_decimal256_push_nulls() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut buffers = create_buffers(&allocator);

    let data: Vec<u8> = vec![0; 32];
    let mut slicer = DataPageFixedSlicer::<32>::new(&data, 5);
    let mut sink =
        WordSwapDecimalColumnSink::<32, 4, _>::new(&mut slicer, &mut buffers, DECIMAL256_NULL);

    sink.reserve(5).unwrap();
    sink.push_nulls(5).unwrap();

    assert_eq!(buffers.data_vec.len(), 160);
    for chunk in buffers.data_vec.chunks(32) {
        assert_eq!(chunk, &DECIMAL256_NULL);
    }
}

// ==================== SignExtendDecimalColumnSink Tests ====================

/// Generic test helper for SignExtendDecimalColumnSink.
/// Runs the sink with given input and asserts the output matches expected.
fn test_sign_extend<const N: usize, const R: usize>(
    input: &[u8],
    expected: &[u8],
    null_value: [u8; N],
) {
    assert_eq!(input.len(), R, "Input length must match R");
    assert_eq!(expected.len(), N, "Expected length must match N");

    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut buffers = create_buffers(&allocator);

    let mut slicer = DataPageFixedSlicer::<R>::new(input, 1);
    let mut sink =
        SignExtendDecimalColumnSink::<N, _>::new(&mut slicer, &mut buffers, null_value, R);

    sink.reserve(1).unwrap();
    sink.push_slice(1).unwrap();

    assert_eq!(buffers.data_vec.as_slice(), expected);
}

/// Generic test helper for multi-word SignExtendDecimalColumnSink (Decimal128/256).
fn test_sign_extend_multiword<const N: usize, const R: usize>(
    input: &[u8],
    expected: &[u8],
    null_value: [u8; N],
) {
    test_sign_extend::<N, R>(input, expected, null_value);
}

// ---- 1 byte -> 2 bytes (Decimal16) ----

#[test]
fn test_sign_extend_1_to_2_positive() {
    // 0x7F (127) -> [0x7F, 0x00] (127 in LE)
    test_sign_extend::<2, 1>(&[0x7F], &[0x7F, 0x00], DECIMAL16_NULL);
}

#[test]
fn test_sign_extend_1_to_2_negative() {
    // 0x80 (-128) -> [0x80, 0xFF] (-128 in LE, sign-extended)
    test_sign_extend::<2, 1>(&[0x80], &[0x80, 0xFF], DECIMAL16_NULL);
}

#[test]
fn test_sign_extend_1_to_2_minus_one() {
    // 0xFF (-1) -> [0xFF, 0xFF]
    test_sign_extend::<2, 1>(&[0xFF], &[0xFF, 0xFF], DECIMAL16_NULL);
}

#[test]
fn test_sign_extend_1_to_2_zero() {
    // 0x00 (0) -> [0x00, 0x00]
    test_sign_extend::<2, 1>(&[0x00], &[0x00, 0x00], DECIMAL16_NULL);
}

// ---- 1 byte -> 4 bytes (Decimal32) ----

#[test]
fn test_sign_extend_1_to_4_positive() {
    // 0x42 (66) -> [0x42, 0x00, 0x00, 0x00]
    test_sign_extend::<4, 1>(&[0x42], &[0x42, 0x00, 0x00, 0x00], DECIMAL32_NULL);
}

#[test]
fn test_sign_extend_1_to_4_negative() {
    // 0xFF (-1) -> [0xFF, 0xFF, 0xFF, 0xFF]
    test_sign_extend::<4, 1>(&[0xFF], &[0xFF, 0xFF, 0xFF, 0xFF], DECIMAL32_NULL);
}

// ---- 2 bytes -> 4 bytes (Decimal32) ----

#[test]
fn test_sign_extend_2_to_4_positive() {
    // 0x01, 0x23 (0x0123 = 291 BE) -> [0x23, 0x01, 0x00, 0x00] (291 in LE)
    test_sign_extend::<4, 2>(&[0x01, 0x23], &[0x23, 0x01, 0x00, 0x00], DECIMAL32_NULL);
}

#[test]
fn test_sign_extend_2_to_4_negative() {
    // 0x80, 0x00 (-32768 BE) -> [0x00, 0x80, 0xFF, 0xFF] (-32768 in LE)
    test_sign_extend::<4, 2>(&[0x80, 0x00], &[0x00, 0x80, 0xFF, 0xFF], DECIMAL32_NULL);
}

// ---- 3 bytes -> 4 bytes (Decimal32) ----

#[test]
fn test_sign_extend_3_to_4_positive() {
    // 0x12, 0x34, 0x56 (0x123456 BE) -> [0x56, 0x34, 0x12, 0x00]
    test_sign_extend::<4, 3>(
        &[0x12, 0x34, 0x56],
        &[0x56, 0x34, 0x12, 0x00],
        DECIMAL32_NULL,
    );
}

#[test]
fn test_sign_extend_3_to_4_negative() {
    // 0xFF, 0xFF, 0xFF (-1 BE) -> [0xFF, 0xFF, 0xFF, 0xFF]
    test_sign_extend::<4, 3>(
        &[0xFF, 0xFF, 0xFF],
        &[0xFF, 0xFF, 0xFF, 0xFF],
        DECIMAL32_NULL,
    );
}

// ---- 1 byte -> 8 bytes (Decimal64) ----

#[test]
fn test_sign_extend_1_to_8_positive() {
    // 0x7F (127) -> [0x7F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
    test_sign_extend::<8, 1>(
        &[0x7F],
        &[0x7F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        DECIMAL64_NULL,
    );
}

#[test]
fn test_sign_extend_1_to_8_negative() {
    // 0x80 (-128) -> [0x80, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
    test_sign_extend::<8, 1>(
        &[0x80],
        &[0x80, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
        DECIMAL64_NULL,
    );
}

// ---- 7 bytes -> 8 bytes (Decimal64) ----

#[test]
fn test_sign_extend_7_to_8_positive() {
    // [0x01..0x07] -> [0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x00]
    test_sign_extend::<8, 7>(
        &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07],
        &[0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x00],
        DECIMAL64_NULL,
    );
}

#[test]
fn test_sign_extend_7_to_8_negative() {
    // 0x80, 0x00... -> sign-extended
    test_sign_extend::<8, 7>(
        &[0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0xFF],
        DECIMAL64_NULL,
    );
}

// ---- 1 byte -> 16 bytes (Decimal128, multi-word) ----

#[test]
fn test_sign_extend_1_to_16_positive() {
    // 0x42 -> word0=[0x00 x8], word1=[0x42, 0x00 x7]
    test_sign_extend_multiword::<16, 1>(
        &[0x42],
        &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x42, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00,
        ],
        DECIMAL128_NULL,
    );
}

#[test]
fn test_sign_extend_1_to_16_negative() {
    // 0x80 (-128) -> all sign-extended
    test_sign_extend_multiword::<16, 1>(
        &[0x80],
        &[
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x80, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF,
        ],
        DECIMAL128_NULL,
    );
}

// ---- 8 bytes -> 16 bytes (Decimal128) ----

#[test]
fn test_sign_extend_8_to_16_positive() {
    test_sign_extend_multiword::<16, 8>(
        &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08],
        &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03,
            0x02, 0x01,
        ],
        DECIMAL128_NULL,
    );
}

#[test]
fn test_sign_extend_8_to_16_negative() {
    test_sign_extend_multiword::<16, 8>(
        &[0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01],
        &[
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x80,
        ],
        DECIMAL128_NULL,
    );
}

// ---- 9 bytes -> 16 bytes (Decimal128, crosses word boundary) ----

#[test]
fn test_sign_extend_9_to_16_positive() {
    test_sign_extend_multiword::<16, 9>(
        &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09],
        &[
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x08, 0x07, 0x06, 0x05, 0x04,
            0x03, 0x02,
        ],
        DECIMAL128_NULL,
    );
}

// ---- 15 bytes -> 16 bytes (Decimal128) ----

#[test]
fn test_sign_extend_15_to_16_positive() {
    test_sign_extend_multiword::<16, 15>(
        &[
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E,
            0x0F,
        ],
        &[
            0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x00, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A,
            0x09, 0x08,
        ],
        DECIMAL128_NULL,
    );
}

// ---- 1 byte -> 32 bytes (Decimal256) ----

#[test]
fn test_sign_extend_1_to_32_positive() {
    let mut expected = [0x00u8; 32];
    expected[24] = 0x42;
    test_sign_extend_multiword::<32, 1>(&[0x42], &expected, DECIMAL256_NULL);
}

#[test]
fn test_sign_extend_1_to_32_negative() {
    test_sign_extend_multiword::<32, 1>(&[0xFF], &[0xFF; 32], DECIMAL256_NULL);
}

// ---- 16 bytes -> 32 bytes (Decimal256) ----

#[test]
fn test_sign_extend_16_to_32_positive() {
    test_sign_extend_multiword::<32, 16>(
        &[
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E,
            0x0F, 0x10,
        ],
        &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x10, 0x0F, 0x0E, 0x0D,
            0x0C, 0x0B, 0x0A, 0x09,
        ],
        DECIMAL256_NULL,
    );
}

// ---- Null and mixed operation tests ----

#[test]
fn test_sign_extend_zero() {
    test_sign_extend::<4, 1>(&[0x00], &[0x00, 0x00, 0x00, 0x00], DECIMAL32_NULL);
}

#[test]
fn test_sign_extend_nulls() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut buffers = create_buffers(&allocator);

    let data: Vec<u8> = vec![0x42];
    let mut slicer = DataPageFixedSlicer::<1>::new(&data, 10);
    let mut sink =
        SignExtendDecimalColumnSink::<4, _>::new(&mut slicer, &mut buffers, DECIMAL32_NULL, 1);

    sink.reserve(5).unwrap();
    sink.push_nulls(5).unwrap();

    assert_eq!(buffers.data_vec.len(), 20);
    for chunk in buffers.data_vec.chunks(4) {
        assert_eq!(chunk, &DECIMAL32_NULL);
    }
}

#[test]
fn test_sign_extend_mixed_push_and_nulls() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut buffers = create_buffers(&allocator);

    let data: Vec<u8> = vec![0x7F, 0x80, 0x00, 0xFF];
    let mut slicer = DataPageFixedSlicer::<1>::new(&data, 10);
    let mut sink =
        SignExtendDecimalColumnSink::<2, _>::new(&mut slicer, &mut buffers, DECIMAL16_NULL, 1);

    sink.reserve(5).unwrap();
    sink.push_slice(2).unwrap();
    sink.push_nulls(1).unwrap();
    sink.push_slice(2).unwrap();

    assert_eq!(buffers.data_vec.len(), 10);
    assert_eq!(&buffers.data_vec[0..2], &[0x7F, 0x00]); // 127 positive
    assert_eq!(&buffers.data_vec[2..4], &[0x80, 0xFF]); // -128 negative
    assert_eq!(&buffers.data_vec[4..6], &DECIMAL16_NULL); // null
    assert_eq!(&buffers.data_vec[6..8], &[0x00, 0x00]); // 0
    assert_eq!(&buffers.data_vec[8..10], &[0xFF, 0xFF]); // -1
}

#[test]
fn test_sign_extend_push_slice_multiple() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut buffers = create_buffers(&allocator);

    // 3 values: 0x01, 0x80, 0xFF
    let data: Vec<u8> = vec![0x01, 0x80, 0xFF];
    let mut slicer = DataPageFixedSlicer::<1>::new(&data, 3);
    let mut sink =
        SignExtendDecimalColumnSink::<2, _>::new(&mut slicer, &mut buffers, DECIMAL16_NULL, 1);

    sink.reserve(3).unwrap();
    sink.push_slice(3).unwrap();

    assert_eq!(buffers.data_vec.len(), 6);
    assert_eq!(&buffers.data_vec[0..2], &[0x01, 0x00]); // 1
    assert_eq!(&buffers.data_vec[2..4], &[0x80, 0xFF]); // -128
    assert_eq!(&buffers.data_vec[4..6], &[0xFF, 0xFF]); // -1
}

// ==================== Fuzzy Tests for SignExtendDecimalColumnSink ====================

/// Helper function to sign-extend and reverse a big-endian byte array
/// This is the reference implementation to verify against
fn sign_extend_and_reverse_simple(src: &[u8], target_size: usize) -> Vec<u8> {
    let sign_byte = if src[0] & 0x80 != 0 { 0xFF } else { 0x00 };
    let mut result = vec![0u8; target_size];

    // Write source bytes in reverse order (LE output)
    for i in 0..src.len() {
        result[i] = src[src.len() - 1 - i];
    }
    // Fill remaining bytes with sign extension
    for i in src.len()..target_size {
        result[i] = sign_byte;
    }
    result
}

/// Helper function for multi-word sign extension (Decimal128/256)
fn sign_extend_multiword(src: &[u8], target_size: usize) -> Vec<u8> {
    let sign_byte = if src[0] & 0x80 != 0 { 0xFF } else { 0x00 };
    let words = target_size / 8;
    let mut result = vec![0u8; target_size];

    for w in 0..words {
        let word_start_in_extended = w * 8;
        let word_dest_start = w * 8;

        for i in 0..8 {
            let extended_pos = word_start_in_extended + 7 - i;
            let byte = if extended_pos < target_size - src.len() {
                sign_byte
            } else {
                src[extended_pos - (target_size - src.len())]
            };
            result[word_dest_start + i] = byte;
        }
    }
    result
}

#[test]
fn test_sign_extend_fuzzy_1_to_2() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut rng = StdRng::seed_from_u64(54321);

    for _ in 0..1000 {
        let mut buffers = create_buffers(&allocator);
        let src_byte: u8 = rng.random();
        let data = vec![src_byte];
        let mut slicer = DataPageFixedSlicer::<1>::new(&data, 1);
        let mut sink =
            SignExtendDecimalColumnSink::<2, _>::new(&mut slicer, &mut buffers, DECIMAL16_NULL, 1);

        sink.reserve(1).unwrap();
        sink.push_slice(1).unwrap();

        let expected = sign_extend_and_reverse_simple(&data, 2);
        assert_eq!(
            buffers.data_vec.as_slice(),
            expected.as_slice(),
            "Failed for src=0x{:02X}",
            src_byte
        );
    }
}

#[test]
fn test_sign_extend_fuzzy_1_to_4() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut rng = StdRng::seed_from_u64(54322);

    for _ in 0..1000 {
        let mut buffers = create_buffers(&allocator);
        let src_byte: u8 = rng.random();
        let data = vec![src_byte];
        let mut slicer = DataPageFixedSlicer::<1>::new(&data, 1);
        let mut sink =
            SignExtendDecimalColumnSink::<4, _>::new(&mut slicer, &mut buffers, DECIMAL32_NULL, 1);

        sink.reserve(1).unwrap();
        sink.push_slice(1).unwrap();

        let expected = sign_extend_and_reverse_simple(&data, 4);
        assert_eq!(
            buffers.data_vec.as_slice(),
            expected.as_slice(),
            "Failed for src=0x{:02X}",
            src_byte
        );
    }
}

#[test]
fn test_sign_extend_fuzzy_2_to_4() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut rng = StdRng::seed_from_u64(54323);

    for _ in 0..1000 {
        let mut buffers = create_buffers(&allocator);
        let data: Vec<u8> = (0..2).map(|_| rng.random()).collect();
        let mut slicer = DataPageFixedSlicer::<2>::new(&data, 1);
        let mut sink =
            SignExtendDecimalColumnSink::<4, _>::new(&mut slicer, &mut buffers, DECIMAL32_NULL, 2);

        sink.reserve(1).unwrap();
        sink.push_slice(1).unwrap();

        let expected = sign_extend_and_reverse_simple(&data, 4);
        assert_eq!(
            buffers.data_vec.as_slice(),
            expected.as_slice(),
            "Failed for src={:02X?}",
            data
        );
    }
}

#[test]
fn test_sign_extend_fuzzy_3_to_8() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut rng = StdRng::seed_from_u64(54324);

    for _ in 0..1000 {
        let mut buffers = create_buffers(&allocator);
        let data: Vec<u8> = (0..3).map(|_| rng.random()).collect();
        let mut slicer = DataPageFixedSlicer::<3>::new(&data, 1);
        let mut sink =
            SignExtendDecimalColumnSink::<8, _>::new(&mut slicer, &mut buffers, DECIMAL64_NULL, 3);

        sink.reserve(1).unwrap();
        sink.push_slice(1).unwrap();

        let expected = sign_extend_and_reverse_simple(&data, 8);
        assert_eq!(
            buffers.data_vec.as_slice(),
            expected.as_slice(),
            "Failed for src={:02X?}",
            data
        );
    }
}

#[test]
fn test_sign_extend_fuzzy_5_to_8() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut rng = StdRng::seed_from_u64(54325);

    for _ in 0..1000 {
        let mut buffers = create_buffers(&allocator);
        let data: Vec<u8> = (0..5).map(|_| rng.random()).collect();
        let mut slicer = DataPageFixedSlicer::<5>::new(&data, 1);
        let mut sink =
            SignExtendDecimalColumnSink::<8, _>::new(&mut slicer, &mut buffers, DECIMAL64_NULL, 5);

        sink.reserve(1).unwrap();
        sink.push_slice(1).unwrap();

        let expected = sign_extend_and_reverse_simple(&data, 8);
        assert_eq!(
            buffers.data_vec.as_slice(),
            expected.as_slice(),
            "Failed for src={:02X?}",
            data
        );
    }
}

#[test]
fn test_sign_extend_fuzzy_1_to_16_multiword() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut rng = StdRng::seed_from_u64(54326);

    for _ in 0..500 {
        let mut buffers = create_buffers(&allocator);
        let src_byte: u8 = rng.random();
        let data = vec![src_byte];
        let mut slicer = DataPageFixedSlicer::<1>::new(&data, 1);
        let mut sink = SignExtendDecimalColumnSink::<16, _>::new(
            &mut slicer,
            &mut buffers,
            DECIMAL128_NULL,
            1,
        );

        sink.reserve(1).unwrap();
        sink.push_slice(1).unwrap();

        let expected = sign_extend_multiword(&data, 16);
        assert_eq!(
            buffers.data_vec.as_slice(),
            expected.as_slice(),
            "Failed for src=0x{:02X}",
            src_byte
        );
    }
}

#[test]
fn test_sign_extend_fuzzy_8_to_16_multiword() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut rng = StdRng::seed_from_u64(54327);

    for _ in 0..500 {
        let mut buffers = create_buffers(&allocator);
        let data: Vec<u8> = (0..8).map(|_| rng.random()).collect();
        let mut slicer = DataPageFixedSlicer::<8>::new(&data, 1);
        let mut sink = SignExtendDecimalColumnSink::<16, _>::new(
            &mut slicer,
            &mut buffers,
            DECIMAL128_NULL,
            8,
        );

        sink.reserve(1).unwrap();
        sink.push_slice(1).unwrap();

        let expected = sign_extend_multiword(&data, 16);
        assert_eq!(
            buffers.data_vec.as_slice(),
            expected.as_slice(),
            "Failed for src={:02X?}",
            data
        );
    }
}

#[test]
fn test_sign_extend_fuzzy_12_to_16_multiword() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut rng = StdRng::seed_from_u64(54328);

    for _ in 0..500 {
        let mut buffers = create_buffers(&allocator);
        let data: Vec<u8> = (0..12).map(|_| rng.random()).collect();
        let mut slicer = DataPageFixedSlicer::<12>::new(&data, 1);
        let mut sink = SignExtendDecimalColumnSink::<16, _>::new(
            &mut slicer,
            &mut buffers,
            DECIMAL128_NULL,
            12,
        );

        sink.reserve(1).unwrap();
        sink.push_slice(1).unwrap();

        let expected = sign_extend_multiword(&data, 16);
        assert_eq!(
            buffers.data_vec.as_slice(),
            expected.as_slice(),
            "Failed for src={:02X?}",
            data
        );
    }
}

#[test]
fn test_sign_extend_fuzzy_1_to_32_multiword() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut rng = StdRng::seed_from_u64(54329);

    for _ in 0..500 {
        let mut buffers = create_buffers(&allocator);
        let src_byte: u8 = rng.random();
        let data = vec![src_byte];
        let mut slicer = DataPageFixedSlicer::<1>::new(&data, 1);
        let mut sink = SignExtendDecimalColumnSink::<32, _>::new(
            &mut slicer,
            &mut buffers,
            DECIMAL256_NULL,
            1,
        );

        sink.reserve(1).unwrap();
        sink.push_slice(1).unwrap();

        let expected = sign_extend_multiword(&data, 32);
        assert_eq!(
            buffers.data_vec.as_slice(),
            expected.as_slice(),
            "Failed for src=0x{:02X}",
            src_byte
        );
    }
}

#[test]
fn test_sign_extend_fuzzy_16_to_32_multiword() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut rng = StdRng::seed_from_u64(54330);

    for _ in 0..500 {
        let mut buffers = create_buffers(&allocator);
        let data: Vec<u8> = (0..16).map(|_| rng.random()).collect();
        let mut slicer = DataPageFixedSlicer::<16>::new(&data, 1);
        let mut sink = SignExtendDecimalColumnSink::<32, _>::new(
            &mut slicer,
            &mut buffers,
            DECIMAL256_NULL,
            16,
        );

        sink.reserve(1).unwrap();
        sink.push_slice(1).unwrap();

        let expected = sign_extend_multiword(&data, 32);
        assert_eq!(
            buffers.data_vec.as_slice(),
            expected.as_slice(),
            "Failed for src={:02X?}",
            data
        );
    }
}

#[test]
fn test_sign_extend_fuzzy_24_to_32_multiword() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut rng = StdRng::seed_from_u64(54331);

    for _ in 0..500 {
        let mut buffers = create_buffers(&allocator);
        let data: Vec<u8> = (0..24).map(|_| rng.random()).collect();
        let mut slicer = DataPageFixedSlicer::<24>::new(&data, 1);
        let mut sink = SignExtendDecimalColumnSink::<32, _>::new(
            &mut slicer,
            &mut buffers,
            DECIMAL256_NULL,
            24,
        );

        sink.reserve(1).unwrap();
        sink.push_slice(1).unwrap();

        let expected = sign_extend_multiword(&data, 32);
        assert_eq!(
            buffers.data_vec.as_slice(),
            expected.as_slice(),
            "Failed for src={:02X?}",
            data
        );
    }
}

#[test]
fn test_sign_extend_fuzzy_batch() {
    // Test pushing multiple random values at once
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut rng = StdRng::seed_from_u64(54332);

    for batch_size in [2, 5, 10, 50, 100] {
        let mut buffers = create_buffers(&allocator);
        let data: Vec<u8> = (0..batch_size * 2).map(|_| rng.random()).collect();
        let mut slicer = DataPageFixedSlicer::<2>::new(&data, batch_size);
        let mut sink =
            SignExtendDecimalColumnSink::<4, _>::new(&mut slicer, &mut buffers, DECIMAL32_NULL, 2);

        sink.reserve(batch_size).unwrap();
        sink.push_slice(batch_size).unwrap();

        assert_eq!(buffers.data_vec.len(), batch_size * 4);

        // Verify each value
        for i in 0..batch_size {
            let src = &data[i * 2..(i + 1) * 2];
            let expected = sign_extend_and_reverse_simple(src, 4);
            let actual = &buffers.data_vec[i * 4..(i + 1) * 4];
            assert_eq!(
                actual,
                expected.as_slice(),
                "Batch size {}, index {}, src={:02X?}",
                batch_size,
                i,
                src
            );
        }
    }
}

#[test]
fn test_sign_extend_fuzzy_interleaved_push_and_nulls() {
    // Test random interleaving of push and null operations
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut rng = StdRng::seed_from_u64(54333);

    for _ in 0..100 {
        let mut buffers = create_buffers(&allocator);

        // Generate random data (enough for up to 20 values)
        let data: Vec<u8> = (0..20).map(|_| rng.random()).collect();
        let mut slicer = DataPageFixedSlicer::<1>::new(&data, 100);
        let mut sink =
            SignExtendDecimalColumnSink::<2, _>::new(&mut slicer, &mut buffers, DECIMAL16_NULL, 1);

        // Track expected output
        let mut expected_output: Vec<u8> = Vec::new();
        let mut data_index = 0;

        // Random interleaving
        let num_ops: usize = rng.random_range(5..15);
        for _ in 0..num_ops {
            let is_null: bool = rng.random();
            let count: usize = rng.random_range(1..4);

            if is_null {
                sink.reserve(count).unwrap();
                sink.push_nulls(count).unwrap();
                for _ in 0..count {
                    expected_output.extend_from_slice(&DECIMAL16_NULL);
                }
            } else {
                let available = data.len() - data_index;
                let actual_count = count.min(available);
                if actual_count > 0 {
                    sink.reserve(actual_count).unwrap();
                    sink.push_slice(actual_count).unwrap();
                    for _ in 0..actual_count {
                        let extended =
                            sign_extend_and_reverse_simple(&data[data_index..data_index + 1], 2);
                        expected_output.extend_from_slice(&extended);
                        data_index += 1;
                    }
                }
            }
        }

        assert_eq!(
            buffers.data_vec.as_slice(),
            expected_output.as_slice(),
            "Interleaved test failed"
        );
    }
}
