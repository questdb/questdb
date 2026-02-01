use crate::allocator::{AcVec, TestAllocatorState};
use crate::parquet_read::column_sink::fixed::{
    FixedColumnSink, IntDecimalColumnSink, NanoTimestampColumnSink, ReverseFixedColumnSink,
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
