use crate::allocator::{AcVec, TestAllocatorState};
use crate::parquet_read::column_sink::fixed::{
    FixedColumnSink, IntDecimalColumnSink, NanoTimestampColumnSink, ReverseFixedColumnSink,
    WordSwapDecimalColumnSink,
};
use crate::parquet_read::column_sink::Pushable;
use crate::parquet_read::slicer::DataPageFixedSlicer;
use crate::parquet_read::ColumnChunkBuffers;
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
        sink.reserve().unwrap();
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

    sink.reserve().unwrap();
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
        let mut slicer = DataPageFixedSlicer::<4>::new(&data, count);
        let mut sink = FixedColumnSink::<4, 4, _>::new(&mut slicer, &mut buffers, &INT_NULL);

        sink.reserve().unwrap();
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
    let mut slicer = DataPageFixedSlicer::<4>::new(&data, 10);
    let mut sink = FixedColumnSink::<4, 4, _>::new(&mut slicer, &mut buffers, &INT_NULL);

    sink.reserve().unwrap();
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
    let mut slicer = DataPageFixedSlicer::<4>::new(&data, 15);
    let mut sink = FixedColumnSink::<4, 4, _>::new(&mut slicer, &mut buffers, &INT_NULL);

    sink.reserve().unwrap();
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

    sink.reserve().unwrap();
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

    sink.reserve().unwrap();
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

        sink.reserve().unwrap();
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
    let mut slicer = DataPageFixedSlicer::<4>::new(&data, 6);
    let null_value = [0xDE, 0xAD, 0xBE, 0xEF];
    let mut sink = ReverseFixedColumnSink::<4, _>::new(&mut slicer, &mut buffers, null_value);

    sink.reserve().unwrap();
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

    sink.reserve().unwrap();
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
    let mut slicer = DataPageFixedSlicer::<12>::new(&data, 5);
    static TS_NULL: [u8; 8] = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80];
    let mut sink = NanoTimestampColumnSink::new(&mut slicer, &mut buffers, &TS_NULL);

    sink.reserve().unwrap();
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
        0x78, 0xEC, 0xFF, 0xFF, // -5000 (two's complement)
    ];
    let mut slicer = DataPageFixedSlicer::<4>::new(&data, 2);
    static DOUBLE_NULL: [u8; 8] = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF8, 0x7F]; // NaN
    let mut sink = IntDecimalColumnSink::new(&mut slicer, &mut buffers, &DOUBLE_NULL, 2);

    sink.reserve().unwrap();
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

        sink.reserve().unwrap();
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
    let mut slicer = DataPageFixedSlicer::<4>::new(&data, 7);
    static DOUBLE_NULL: [u8; 8] = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF8, 0x7F];
    let mut sink = IntDecimalColumnSink::new(&mut slicer, &mut buffers, &DOUBLE_NULL, 2);

    sink.reserve().unwrap();
    sink.push_nulls(7).unwrap();

    assert_eq!(buffers.data_vec.len(), 56);
    for chunk in buffers.data_vec.chunks(8) {
        let val = f64::from_le_bytes(chunk.try_into().unwrap());
        assert!(val.is_nan());
    }
}

// Decimal128 null: (i64::MIN, 0) in little-endian
static DECIMAL128_NULL: [u8; 16] = [
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, // i64::MIN LE
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 0 LE
];

// Decimal256 null: (i64::MIN, 0, 0, 0) in little-endian
static DECIMAL256_NULL: [u8; 32] = [
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, // i64::MIN LE
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 0 LE
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 0 LE
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 0 LE
];

#[test]
fn test_word_swap_decimal128_push_slice() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut buffers = create_buffers(&allocator);

    // Big-endian Decimal128 data: two 8-byte words per value
    // Value 1: high=0x0102030405060708, low=0x090A0B0C0D0E0F10
    // Value 2: high=0x1112131415161718, low=0x191A1B1C1D1E1F20
    let data: Vec<u8> = vec![
        // Value 1: big-endian
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // high BE
        0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, // low BE
        // Value 2: big-endian
        0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, // high BE
        0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20, // low BE
    ];
    let mut slicer = DataPageFixedSlicer::<16>::new(&data, 2);
    let mut sink =
        WordSwapDecimalColumnSink::<16, 2, _>::new(&mut slicer, &mut buffers, DECIMAL128_NULL);

    sink.reserve().unwrap();
    sink.push_slice(2).unwrap();

    // Each 8-byte word should be reversed within itself (BE to LE)
    // but the word order remains the same
    let expected: Vec<u8> = vec![
        // Value 1: each word reversed to little-endian
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, // high LE
        0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A, 0x09, // low LE
        // Value 2: each word reversed to little-endian
        0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11, // high LE
        0x20, 0x1F, 0x1E, 0x1D, 0x1C, 0x1B, 0x1A, 0x19, // low LE
    ];

    assert_eq!(buffers.data_vec.as_slice(), expected.as_slice());
}

#[test]
fn test_word_swap_decimal128_push_slice_small_counts() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();

    for count in 1..=4 {
        let mut buffers = create_buffers(&allocator);

        // Generate test data: count values, each 16 bytes
        let data: Vec<u8> = (0..count * 16).map(|i| i as u8).collect();
        let mut slicer = DataPageFixedSlicer::<16>::new(&data, count);
        let mut sink =
            WordSwapDecimalColumnSink::<16, 2, _>::new(&mut slicer, &mut buffers, DECIMAL128_NULL);

        sink.reserve().unwrap();
        sink.push_slice(count).unwrap();

        assert_eq!(buffers.data_vec.len(), count * 16, "count={}", count);

        // Verify each value has its words byte-swapped
        for (v, chunk) in buffers.data_vec.chunks(16).enumerate() {
            // Each 8-byte word should be reversed
            for w in 0..2 {
                let word_offset = w * 8;
                for i in 0..8 {
                    let expected_byte = (v * 16 + word_offset + 7 - i) as u8;
                    assert_eq!(
                        chunk[word_offset + i],
                        expected_byte,
                        "count={}, value={}, word={}, byte={}",
                        count,
                        v,
                        w,
                        i
                    );
                }
            }
        }
    }
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

    sink.reserve().unwrap();
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

    // Big-endian Decimal256 data: four 8-byte words per value
    let data: Vec<u8> = vec![
        // Value 1: big-endian (4 words)
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // word 0 BE
        0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, // word 1 BE
        0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, // word 2 BE
        0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20, // word 3 BE
    ];
    let mut slicer = DataPageFixedSlicer::<32>::new(&data, 1);
    let mut sink =
        WordSwapDecimalColumnSink::<32, 4, _>::new(&mut slicer, &mut buffers, DECIMAL256_NULL);

    sink.reserve().unwrap();
    sink.push_slice(1).unwrap();

    // Each 8-byte word should be reversed within itself
    let expected: Vec<u8> = vec![
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, // word 0 LE
        0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A, 0x09, // word 1 LE
        0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11, // word 2 LE
        0x20, 0x1F, 0x1E, 0x1D, 0x1C, 0x1B, 0x1A, 0x19, // word 3 LE
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

    sink.reserve().unwrap();
    sink.push_nulls(5).unwrap();

    assert_eq!(buffers.data_vec.len(), 160);
    for chunk in buffers.data_vec.chunks(32) {
        assert_eq!(chunk, &DECIMAL256_NULL);
    }
}

#[test]
fn test_word_swap_decimal128_mixed_push_and_nulls() {
    let tas = TestAllocatorState::new();
    let allocator = tas.allocator();
    let mut buffers = create_buffers(&allocator);

    // 3 values in big-endian format
    let data: Vec<u8> = (0..48).map(|i| i as u8).collect();
    let mut slicer = DataPageFixedSlicer::<16>::new(&data, 6);
    let mut sink =
        WordSwapDecimalColumnSink::<16, 2, _>::new(&mut slicer, &mut buffers, DECIMAL128_NULL);

    sink.reserve().unwrap();
    sink.push_slice(1).unwrap(); // value 0
    sink.push_nulls(2).unwrap(); // 2 nulls
    sink.push_slice(2).unwrap(); // values 1, 2

    assert_eq!(buffers.data_vec.len(), 80); // 5 values * 16 bytes

    // Check value 0 (bytes swapped within each word)
    let chunk0 = &buffers.data_vec[0..16];
    assert_eq!(chunk0[0], 7); // first word reversed: 0-7 -> 7-0
    assert_eq!(chunk0[7], 0);
    assert_eq!(chunk0[8], 15); // second word reversed: 8-15 -> 15-8
    assert_eq!(chunk0[15], 8);

    // Check nulls
    assert_eq!(&buffers.data_vec[16..32], &DECIMAL128_NULL);
    assert_eq!(&buffers.data_vec[32..48], &DECIMAL128_NULL);

    // Check value 1 (starts at input offset 16)
    let chunk3 = &buffers.data_vec[48..64];
    assert_eq!(chunk3[0], 23); // 16+7
    assert_eq!(chunk3[7], 16);
    assert_eq!(chunk3[8], 31); // 16+15
    assert_eq!(chunk3[15], 24);
}
