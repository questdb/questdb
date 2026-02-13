use super::*;
use crate::parquet_read::decoders::VarDictDecoder;
use crate::parquet_read::slicer::rle::RleDictionarySlicer;

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
    let num_bytes = bools.len().div_ceil(8);
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
fn test_delta_length_array_slicer_next_into() {
    let strings = ["aa", "bbb", "c", "dd"];
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
    let strings = ["hello", "world", "foo", "bar"];
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
    let strings = ["a", "bb", "ccc", "dddd", "eeeee"];
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

struct TestDictDecoder {
    values: Vec<Vec<u8>>,
}

impl TestDictDecoder {
    fn new(values: Vec<Vec<u8>>) -> Self {
        Self { values }
    }
}

impl VarDictDecoder for TestDictDecoder {
    fn get_dict_value(&self, index: u32) -> &[u8] {
        &self.values[index as usize]
    }

    fn avg_key_len(&self) -> f32 {
        if self.values.is_empty() {
            0.0
        } else {
            self.values.iter().map(|v| v.len()).sum::<usize>() as f32 / self.values.len() as f32
        }
    }

    fn len(&self) -> u32 {
        self.values.len() as u32
    }
}

fn encode_rle_data(values: &[u32], num_bits: u8) -> Vec<u8> {
    let mut buffer = vec![num_bits];
    parquet2::encoding::hybrid_rle::encode_u32(
        &mut buffer,
        values.iter().copied(),
        values.len(),
        num_bits as u32,
    )
    .unwrap();
    buffer
}

#[test]
fn test_rle_dictionary_slicer_skip_zero() {
    let dict = TestDictDecoder::new(vec![b"zero".to_vec(), b"one".to_vec(), b"two".to_vec()]);
    let encoded = encode_rle_data(&[0, 1, 2, 0, 1], 2);
    let error_value = b"ERR";

    let mut slicer = RleDictionarySlicer::try_new(&encoded, dict, 5, 5, error_value).unwrap();
    slicer.skip(0);
    assert_eq!(slicer.next(), b"zero");
}

#[test]
fn test_rle_dictionary_slicer_skip_small() {
    let values: Vec<u32> = vec![0, 1, 2, 3, 4, 0, 1, 2, 3, 4];
    let encoded = encode_rle_data(&values, 3);
    let error_value = b"ERR";

    for skip in 1..=4 {
        let dict = TestDictDecoder::new(vec![
            b"a".to_vec(),
            b"b".to_vec(),
            b"c".to_vec(),
            b"d".to_vec(),
            b"e".to_vec(),
        ]);
        let mut slicer = RleDictionarySlicer::try_new(&encoded, dict, 10, 10, error_value).unwrap();
        slicer.skip(skip);
        let expected = [b"a", b"b", b"c", b"d", b"e"][skip % 5];
        assert_eq!(slicer.next(), expected, "skip={}", skip);
    }
}

#[test]
fn test_rle_dictionary_slicer_skip_large() {
    let dict_values: Vec<Vec<u8>> = (0..10).map(|i| format!("val{}", i).into_bytes()).collect();
    let values: Vec<u32> = (0..100).map(|i| (i % 10) as u32).collect();
    let encoded = encode_rle_data(&values, 4);
    let error_value = b"ERR";

    for skip in [5, 15, 33, 77, 99] {
        let dict = TestDictDecoder::new(dict_values.clone());
        let mut slicer =
            RleDictionarySlicer::try_new(&encoded, dict, 100, 100, error_value).unwrap();
        slicer.skip(skip);
        let expected = &dict_values[skip % 10];
        assert_eq!(slicer.next(), expected.as_slice(), "skip={}", skip);
    }
}

#[test]
fn test_rle_dictionary_slicer_skip_interleaved() {
    let dict = TestDictDecoder::new(vec![
        b"zero".to_vec(),
        b"one".to_vec(),
        b"two".to_vec(),
        b"three".to_vec(),
        b"four".to_vec(),
    ]);
    let values: Vec<u32> = vec![0, 1, 2, 3, 4, 0, 1, 2, 3, 4];
    let encoded = encode_rle_data(&values, 3);
    let error_value = b"ERR";

    let mut slicer = RleDictionarySlicer::try_new(&encoded, dict, 10, 10, error_value).unwrap();

    assert_eq!(slicer.next(), b"zero");
    slicer.skip(2);
    assert_eq!(slicer.next(), b"three");
    slicer.skip(1);
    assert_eq!(slicer.next(), b"zero");
    let mut sink = TestSink::new();
    slicer.next_slice_into(3, &mut sink).unwrap();
    assert_eq!(sink.into_inner(), b"onetwothree");
}

#[test]
fn test_rle_dictionary_slicer_skip_repeated_values() {
    let dict = TestDictDecoder::new(vec![b"AAA".to_vec(), b"BBB".to_vec()]);
    let values: Vec<u32> = vec![0, 0, 0, 0, 0, 1, 1, 1, 1, 1];
    let encoded = encode_rle_data(&values, 1);
    let error_value = b"ERR";

    let mut slicer = RleDictionarySlicer::try_new(&encoded, dict, 10, 10, error_value).unwrap();
    slicer.skip(2);
    assert_eq!(slicer.next(), b"AAA");
    slicer.skip(2);
    assert_eq!(slicer.next(), b"BBB");
    slicer.skip(3);
    assert_eq!(slicer.next(), b"BBB");
}

#[test]
fn test_rle_dictionary_slicer_zero_bit_width() {
    let dict = TestDictDecoder::new(vec![b"only_value".to_vec()]);
    let buffer: Vec<u8> = vec![0];
    let error_value = b"ERR";

    let mut slicer = RleDictionarySlicer::try_new(&buffer, dict, 10, 10, error_value).unwrap();
    slicer.skip(5);
    assert_eq!(slicer.next(), b"only_value");

    slicer.skip(3);
    assert_eq!(slicer.next(), b"only_value");
}

#[test]
fn test_boolean_bitmap_slicer_advance_exact_byte_boundary() {
    let bools: Vec<bool> = (0..24).map(|i| i % 2 == 0).collect();
    let bitmap = bools_to_bitmap(&bools);

    // Start at bit 5, bits_left = 3, advance(3) should move to byte 1
    let mut slicer_advance = BooleanBitmapSlicer::new(&bitmap, bools.len(), bools.len());
    let mut slicer_next = BooleanBitmapSlicer::new(&bitmap, bools.len(), bools.len());

    slicer_advance.skip(5);
    slicer_next.skip(5);

    slicer_advance.skip(3);
    for _ in 0..3 {
        slicer_next.next();
    }

    for i in 8..16 {
        assert_eq!(
            slicer_advance.next(),
            slicer_next.next(),
            "mismatch at bit {}",
            i
        );
    }
}
