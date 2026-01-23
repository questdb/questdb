use super::*;

/// A simple ByteSink for testing
struct TestSink(Vec<u8>);

impl TestSink {
    fn new() -> Self {
        Self(Vec::new())
    }

    fn into_inner(self) -> Vec<u8> {
        self.0
    }
}

impl ByteSink for TestSink {
    fn extend_from_slice(&mut self, data: &[u8]) -> ParquetResult<()> {
        self.0.extend_from_slice(data);
        Ok(())
    }

    fn extend_from_slice_safe(&mut self, data: &[u8]) -> ParquetResult<()> {
        self.0.extend_from_slice(data);
        Ok(())
    }
}

#[test]
fn test_fixed_slicer_next_into() {
    let data: Vec<u8> = vec![1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0];
    let mut slicer = DataPageFixedSlicer::<4>::new(&data, 4);
    let mut sink = TestSink::new();

    slicer.next_into(&mut sink).unwrap();
    assert_eq!(sink.into_inner(), vec![1, 0, 0, 0]);

    let mut sink = TestSink::new();
    slicer.next_into(&mut sink).unwrap();
    assert_eq!(sink.into_inner(), vec![2, 0, 0, 0]);
}

#[test]
fn test_fixed_slicer_next_slice_into() {
    let data: Vec<u8> = vec![1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0];
    let mut slicer = DataPageFixedSlicer::<4>::new(&data, 4);
    let mut sink = TestSink::new();

    slicer.next_slice_into(3, &mut sink).unwrap();
    assert_eq!(sink.into_inner(), vec![1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0]);

    let mut sink = TestSink::new();
    slicer.next_slice_into(1, &mut sink).unwrap();
    assert_eq!(sink.into_inner(), vec![4, 0, 0, 0]);
}

#[test]
fn test_fixed_slicer_8byte() {
    let data: Vec<u8> = vec![
        1, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 127,
    ];
    let mut slicer = DataPageFixedSlicer::<8>::new(&data, 2);
    let mut sink = TestSink::new();

    slicer.next_slice_into(2, &mut sink).unwrap();
    assert_eq!(sink.into_inner(), data);
}

#[test]
fn test_plain_var_slicer_next_into() {
    let mut data = Vec::new();
    data.extend_from_slice(&5u32.to_le_bytes());
    data.extend_from_slice(b"hello");
    data.extend_from_slice(&5u32.to_le_bytes());
    data.extend_from_slice(b"world");
    data.extend_from_slice(&1u32.to_le_bytes());
    data.extend_from_slice(b"!");

    let mut slicer = PlainVarSlicer::new(&data, 3);
    let mut sink = TestSink::new();

    slicer.next_into(&mut sink).unwrap();
    assert_eq!(sink.into_inner(), b"hello");

    let mut sink = TestSink::new();
    slicer.next_into(&mut sink).unwrap();
    assert_eq!(sink.into_inner(), b"world");

    let mut sink = TestSink::new();
    slicer.next_into(&mut sink).unwrap();
    assert_eq!(sink.into_inner(), b"!");
}

#[test]
fn test_plain_var_slicer_next_slice_into() {
    let mut data = Vec::new();
    data.extend_from_slice(&3u32.to_le_bytes());
    data.extend_from_slice(b"abc");
    data.extend_from_slice(&2u32.to_le_bytes());
    data.extend_from_slice(b"de");
    data.extend_from_slice(&4u32.to_le_bytes());
    data.extend_from_slice(b"fghi");

    let mut slicer = PlainVarSlicer::new(&data, 3);
    let mut sink = TestSink::new();

    slicer.next_slice_into(2, &mut sink).unwrap();
    assert_eq!(sink.into_inner(), b"abcde");

    let mut sink = TestSink::new();
    slicer.next_slice_into(1, &mut sink).unwrap();
    assert_eq!(sink.into_inner(), b"fghi");
}

#[test]
fn test_plain_var_slicer_empty_strings() {
    let mut data = Vec::new();
    data.extend_from_slice(&0u32.to_le_bytes()); // empty
    data.extend_from_slice(&3u32.to_le_bytes());
    data.extend_from_slice(b"abc");
    data.extend_from_slice(&0u32.to_le_bytes()); // empty

    let mut slicer = PlainVarSlicer::new(&data, 3);

    let mut sink = TestSink::new();
    slicer.next_into(&mut sink).unwrap();
    assert_eq!(sink.into_inner(), b"");

    let mut sink = TestSink::new();
    slicer.next_into(&mut sink).unwrap();
    assert_eq!(sink.into_inner(), b"abc");

    let mut sink = TestSink::new();
    slicer.next_into(&mut sink).unwrap();
    assert_eq!(sink.into_inner(), b"");
}

fn bools_to_bitmap(bools: &[bool]) -> Vec<u8> {
    let num_bytes = (bools.len() + 7) / 8;
    let mut bytes = vec![0u8; num_bytes];
    for (i, &b) in bools.iter().enumerate() {
        if b {
            bytes[i / 8] |= 1 << (i % 8);
        }
    }
    bytes
}

#[test]
fn test_boolean_bitmap_slicer_next_into() {
    let bools = [true, false, true, true, false];
    let bitmap = bools_to_bitmap(&bools);
    let mut slicer = BooleanBitmapSlicer::new(&bitmap, bools.len(), bools.len());

    for &expected in &bools {
        let mut sink = TestSink::new();
        slicer.next_into(&mut sink).unwrap();
        assert_eq!(sink.into_inner(), vec![expected as u8]);
    }
}

#[test]
fn test_boolean_bitmap_slicer_next_slice_into() {
    let bools: Vec<bool> = (0..20).map(|i| i % 3 == 0).collect();
    let bitmap = bools_to_bitmap(&bools);
    let mut slicer = BooleanBitmapSlicer::new(&bitmap, bools.len(), bools.len());
    let mut sink = TestSink::new();
    slicer.next_slice_into(20, &mut sink).unwrap();

    let expected: Vec<u8> = bools.iter().map(|&b| b as u8).collect();
    assert_eq!(sink.into_inner(), expected);
}

#[test]
fn test_boolean_bitmap_slicer_skip_then_slice() {
    let bools: Vec<bool> = (0..16).map(|i| i % 2 == 0).collect();
    let bitmap = bools_to_bitmap(&bools);
    let mut slicer = BooleanBitmapSlicer::new(&bitmap, bools.len(), bools.len());
    slicer.skip(5);

    let mut sink = TestSink::new();
    slicer.next_slice_into(6, &mut sink).unwrap();

    let expected: Vec<u8> = bools[5..11].iter().map(|&b| b as u8).collect();
    assert_eq!(sink.into_inner(), expected);
}

#[test]
fn test_boolean_bitmap_slicer_skip() {
    let bools: Vec<bool> = (0..20).map(|i| i % 3 == 0).collect();
    let bitmap = bools_to_bitmap(&bools);
    let mut slicer = BooleanBitmapSlicer::new(&bitmap, bools.len(), bools.len());
    slicer.skip(0);
    assert_eq!(slicer.next(), &[bools[0] as u8]);

    // Skip small values (fast path 1-4)
    for skip in 1..=4 {
        let mut slicer = BooleanBitmapSlicer::new(&bitmap, bools.len(), bools.len());
        slicer.skip(skip);
        assert_eq!(slicer.next(), &[bools[skip] as u8], "skip={}", skip);
    }

    // Skip large values and across byte boundary
    for skip in [5, 7, 10, 15] {
        let mut slicer = BooleanBitmapSlicer::new(&bitmap, bools.len(), bools.len());
        slicer.skip(skip);
        assert_eq!(slicer.next(), &[bools[skip] as u8], "skip={}", skip);
    }

    // Interleaved next and skip
    let mut slicer = BooleanBitmapSlicer::new(&bitmap, bools.len(), bools.len());
    assert_eq!(slicer.next(), &[bools[0] as u8]);
    slicer.skip(3);
    assert_eq!(slicer.next(), &[bools[4] as u8]);
    slicer.skip(5);
    assert_eq!(slicer.next(), &[bools[10] as u8]);

    // Skip all / skip more than available
    let mut slicer = BooleanBitmapSlicer::new(&bitmap, bools.len(), bools.len());
    slicer.skip(100);
    let _ = slicer.next();
    assert!(slicer.result().is_err());
}

#[test]
fn test_value_convert_slicer_next_into() {
    let data: Vec<u8> = vec![0, 0, 0, 0, 1, 0, 0, 0, 109, 1, 0, 0];
    let inner = DataPageFixedSlicer::<4>::new(&data, 3);
    let mut slicer = ValueConvertSlicer::<8, _, DaysToMillisConverter>::new(inner);

    // Day 0 = 0 millis
    let mut sink = TestSink::new();
    slicer.next_into(&mut sink).unwrap();
    let millis = i64::from_le_bytes(sink.into_inner().try_into().unwrap());
    assert_eq!(millis, 0);

    // Day 1 = 86400000 millis
    let mut sink = TestSink::new();
    slicer.next_into(&mut sink).unwrap();
    let millis = i64::from_le_bytes(sink.into_inner().try_into().unwrap());
    assert_eq!(millis, 86400 * 1000);

    // Day 365
    let mut sink = TestSink::new();
    slicer.next_into(&mut sink).unwrap();
    let millis = i64::from_le_bytes(sink.into_inner().try_into().unwrap());
    assert_eq!(millis, 365 * 86400 * 1000);
}

#[test]
fn test_value_convert_slicer_next_slice_into() {
    let data: Vec<u8> = vec![0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0];
    let inner = DataPageFixedSlicer::<4>::new(&data, 3);
    let mut slicer = ValueConvertSlicer::<8, _, DaysToMillisConverter>::new(inner);

    let mut sink = TestSink::new();
    slicer.next_slice_into(3, &mut sink).unwrap();

    let result = sink.into_inner();
    let millis: Vec<i64> = result
        .chunks(8)
        .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
        .collect();
    assert_eq!(millis, vec![0, 86400 * 1000, 2 * 86400 * 1000]);
}

#[test]
fn test_fixed_slicer_mixed_operations() {
    let data: Vec<u8> = (0u8..20).collect();
    let mut slicer = DataPageFixedSlicer::<2>::new(&data, 10);
    assert_eq!(slicer.next(), &[0, 1]);
    slicer.skip(2);
    let mut sink = TestSink::new();
    slicer.next_into(&mut sink).unwrap();
    assert_eq!(sink.into_inner(), vec![6, 7]);
    let mut sink = TestSink::new();
    slicer.next_slice_into(3, &mut sink).unwrap();
    assert_eq!(sink.into_inner(), vec![8, 9, 10, 11, 12, 13]);
}

#[test]
fn test_plain_var_slicer_mixed_operations() {
    let mut data = Vec::new();
    for i in 0..5 {
        let s = format!("str{}", i);
        data.extend_from_slice(&(s.len() as u32).to_le_bytes());
        data.extend_from_slice(s.as_bytes());
    }

    let mut slicer = PlainVarSlicer::new(&data, 5);
    assert_eq!(slicer.next(), b"str0");

    // skip
    slicer.skip(1);

    // next_into
    let mut sink = TestSink::new();
    slicer.next_into(&mut sink).unwrap();
    assert_eq!(sink.into_inner(), b"str2");

    // next_slice_into
    let mut sink = TestSink::new();
    slicer.next_slice_into(2, &mut sink).unwrap();
    assert_eq!(sink.into_inner(), b"str3str4");
}

#[test]
fn test_delta_binary_packed_slicer_next_into() {
    let values: Vec<i64> = vec![1, 2, 3, 4, 5];
    let mut encoded = Vec::new();
    parquet2::encoding::delta_bitpacked::encode(values.iter().copied(), &mut encoded);

    let mut slicer = DeltaBinaryPackedSlicer::<8>::try_new(&encoded, 5).unwrap();

    for expected in &values {
        let mut sink = TestSink::new();
        slicer.next_into(&mut sink).unwrap();
        let value = i64::from_le_bytes(sink.into_inner().try_into().unwrap());
        assert_eq!(value, *expected);
    }
    assert!(slicer.result().is_ok());
}

#[test]
fn test_delta_binary_packed_slicer_next_slice_into() {
    let values: Vec<i64> = vec![10, 20, 30, 40, 50];
    let mut encoded = Vec::new();
    parquet2::encoding::delta_bitpacked::encode(values.iter().copied(), &mut encoded);

    let mut slicer = DeltaBinaryPackedSlicer::<8>::try_new(&encoded, 5).unwrap();

    let mut sink = TestSink::new();
    slicer.next_slice_into(3, &mut sink).unwrap();

    let result: Vec<i64> = sink
        .into_inner()
        .chunks(8)
        .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
        .collect();
    assert_eq!(result, vec![10, 20, 30]);

    // Read remaining
    let mut sink = TestSink::new();
    slicer.next_slice_into(2, &mut sink).unwrap();
    let result: Vec<i64> = sink
        .into_inner()
        .chunks(8)
        .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
        .collect();
    assert_eq!(result, vec![40, 50]);
}

#[test]
fn test_delta_binary_packed_slicer_4byte() {
    let values: Vec<i64> = vec![100, 200, 300];
    let mut encoded = Vec::new();
    parquet2::encoding::delta_bitpacked::encode(values.iter().copied(), &mut encoded);
    let mut slicer = DeltaBinaryPackedSlicer::<4>::try_new(&encoded, 3).unwrap();

    let mut sink = TestSink::new();
    slicer.next_slice_into(3, &mut sink).unwrap();

    let result: Vec<i32> = sink
        .into_inner()
        .chunks(4)
        .map(|c| i32::from_le_bytes(c.try_into().unwrap()))
        .collect();
    assert_eq!(result, vec![100, 200, 300]);
}

#[test]
fn test_delta_binary_packed_slicer_skip() {
    let values: Vec<i64> = vec![1, 2, 3, 4, 5, 6, 7, 8];
    let mut encoded = Vec::new();
    parquet2::encoding::delta_bitpacked::encode(values.iter().copied(), &mut encoded);

    let mut slicer = DeltaBinaryPackedSlicer::<8>::try_new(&encoded, 8).unwrap();

    slicer.skip(3);

    let mut sink = TestSink::new();
    slicer.next_into(&mut sink).unwrap();
    let value = i64::from_le_bytes(sink.into_inner().try_into().unwrap());
    assert_eq!(value, 4);
}

#[test]
fn test_delta_length_array_slicer_next_into() {
    let strings = vec!["aa", "bbb", "c", "dd"];
    let mut encoded = Vec::new();
    parquet2::encoding::delta_length_byte_array::encode(
        strings.iter().map(|s| s.as_bytes()),
        &mut encoded,
    );

    let mut slicer = DeltaLengthArraySlicer::try_new(&encoded, 4, 4).unwrap();

    for expected in &strings {
        let mut sink = TestSink::new();
        slicer.next_into(&mut sink).unwrap();
        assert_eq!(sink.into_inner(), expected.as_bytes());
    }
}

#[test]
fn test_delta_length_array_slicer_next_slice_into() {
    let strings = vec!["hello", "world", "foo", "bar"];
    let mut encoded = Vec::new();
    parquet2::encoding::delta_length_byte_array::encode(
        strings.iter().map(|s| s.as_bytes()),
        &mut encoded,
    );
    let mut slicer = DeltaLengthArraySlicer::try_new(&encoded, 4, 4).unwrap();

    let mut sink = TestSink::new();
    slicer.next_slice_into(2, &mut sink).unwrap();
    assert_eq!(sink.into_inner(), b"helloworld");

    let mut sink = TestSink::new();
    slicer.next_slice_into(2, &mut sink).unwrap();
    assert_eq!(sink.into_inner(), b"foobar");
}

#[test]
fn test_delta_length_array_slicer_skip() {
    let strings = vec!["a", "bb", "ccc", "dddd", "eeeee"];
    let mut encoded = Vec::new();
    parquet2::encoding::delta_length_byte_array::encode(
        strings.iter().map(|s| s.as_bytes()),
        &mut encoded,
    );

    let mut slicer = DeltaLengthArraySlicer::try_new(&encoded, 5, 5).unwrap();
    slicer.skip(2);
    let mut sink = TestSink::new();
    slicer.next_into(&mut sink).unwrap();
    assert_eq!(sink.into_inner(), b"ccc");
    slicer.skip(1);

    let mut sink = TestSink::new();
    slicer.next_into(&mut sink).unwrap();
    assert_eq!(sink.into_inner(), b"eeeee");
}

#[test]
fn test_delta_bytes_array_slicer_next_into() {
    let strings: Vec<&[u8]> = vec![b"Hello", b"Helicopter", b"Help"];
    let mut encoded = Vec::new();
    parquet2::encoding::delta_byte_array::encode(strings.clone().into_iter(), &mut encoded);
    let mut slicer = DeltaBytesArraySlicer::try_new(&encoded, 3, 3).unwrap();

    for expected in &strings {
        let mut sink = TestSink::new();
        slicer.next_into(&mut sink).unwrap();
        assert_eq!(sink.into_inner(), *expected);
    }
}

#[test]
fn test_delta_bytes_array_slicer_next_slice_into() {
    let strings: Vec<&[u8]> = vec![b"apple", b"application", b"apply", b"banana"];
    let mut encoded = Vec::new();
    parquet2::encoding::delta_byte_array::encode(strings.clone().into_iter(), &mut encoded);
    let mut slicer = DeltaBytesArraySlicer::try_new(&encoded, 4, 4).unwrap();

    let mut sink = TestSink::new();
    slicer.next_slice_into(3, &mut sink).unwrap();
    assert_eq!(sink.into_inner(), b"appleapplicationapply");

    let mut sink = TestSink::new();
    slicer.next_slice_into(1, &mut sink).unwrap();
    assert_eq!(sink.into_inner(), b"banana");
}

#[test]
fn test_delta_bytes_array_slicer_skip() {
    let strings: Vec<&[u8]> = vec![b"test1", b"test2", b"test3", b"other"];
    let mut encoded = Vec::new();
    parquet2::encoding::delta_byte_array::encode(strings.clone().into_iter(), &mut encoded);
    let mut slicer = DeltaBytesArraySlicer::try_new(&encoded, 4, 4).unwrap();
    slicer.skip(2);

    let mut sink = TestSink::new();
    slicer.next_into(&mut sink).unwrap();
    assert_eq!(sink.into_inner(), b"test3");
}
