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

#[test]
fn test_fixed_slicer_mixed_operations() {
    let data: Vec<u8> = (0u8..20).collect();
    let mut slicer = DataPageFixedSlicer::<2>::new(&data, 10);
    assert_eq!(slicer.next().unwrap(), &[0, 1]);
    slicer.skip(2).unwrap();
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
    assert_eq!(slicer.next().unwrap(), b"str0");

    // skip
    slicer.skip(1).unwrap();

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
    slicer.skip(2).unwrap();
    let mut sink = TestSink::new();
    slicer.next_into(&mut sink).unwrap();
    assert_eq!(sink.into_inner(), b"ccc");
    slicer.skip(1).unwrap();

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
    slicer.skip(2).unwrap();

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

    let mut slicer = RleDictionarySlicer::try_new(&encoded, dict, 5, 5).unwrap();
    slicer.skip(0).unwrap();
    assert_eq!(slicer.next().unwrap(), b"zero");
}

#[test]
fn test_rle_dictionary_slicer_skip_small() {
    let values: Vec<u32> = vec![0, 1, 2, 3, 4, 0, 1, 2, 3, 4];
    let encoded = encode_rle_data(&values, 3);

    for skip in 1..=4 {
        let dict = TestDictDecoder::new(vec![
            b"a".to_vec(),
            b"b".to_vec(),
            b"c".to_vec(),
            b"d".to_vec(),
            b"e".to_vec(),
        ]);
        let mut slicer = RleDictionarySlicer::try_new(&encoded, dict, 10, 10).unwrap();
        slicer.skip(skip).unwrap();
        let expected = [b"a", b"b", b"c", b"d", b"e"][skip % 5];
        assert_eq!(slicer.next().unwrap(), expected, "skip={}", skip);
    }
}

#[test]
fn test_rle_dictionary_slicer_skip_large() {
    let dict_values: Vec<Vec<u8>> = (0..10).map(|i| format!("val{}", i).into_bytes()).collect();
    let values: Vec<u32> = (0..100).map(|i| (i % 10) as u32).collect();
    let encoded = encode_rle_data(&values, 4);

    for skip in [5, 15, 33, 77, 99] {
        let dict = TestDictDecoder::new(dict_values.clone());
        let mut slicer = RleDictionarySlicer::try_new(&encoded, dict, 100, 100).unwrap();
        slicer.skip(skip).unwrap();
        let expected = &dict_values[skip % 10];
        assert_eq!(slicer.next().unwrap(), expected.as_slice(), "skip={}", skip);
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

    let mut slicer = RleDictionarySlicer::try_new(&encoded, dict, 10, 10).unwrap();

    assert_eq!(slicer.next().unwrap(), b"zero");
    slicer.skip(2).unwrap();
    assert_eq!(slicer.next().unwrap(), b"three");
    slicer.skip(1).unwrap();
    assert_eq!(slicer.next().unwrap(), b"zero");
    let mut sink = TestSink::new();
    slicer.next_slice_into(3, &mut sink).unwrap();
    assert_eq!(sink.into_inner(), b"onetwothree");
}

#[test]
fn test_rle_dictionary_slicer_skip_repeated_values() {
    let dict = TestDictDecoder::new(vec![b"AAA".to_vec(), b"BBB".to_vec()]);
    let values: Vec<u32> = vec![0, 0, 0, 0, 0, 1, 1, 1, 1, 1];
    let encoded = encode_rle_data(&values, 1);

    let mut slicer = RleDictionarySlicer::try_new(&encoded, dict, 10, 10).unwrap();
    slicer.skip(2).unwrap();
    assert_eq!(slicer.next().unwrap(), b"AAA");
    slicer.skip(2).unwrap();
    assert_eq!(slicer.next().unwrap(), b"BBB");
    slicer.skip(3).unwrap();
    assert_eq!(slicer.next().unwrap(), b"BBB");
}

#[test]
fn test_rle_dictionary_slicer_zero_values_positive_bit_width() {
    // Regression: the writer can emit ``[bits_per_key, 0x01]`` for a
    // data page with zero non-null values but a non-empty global
    // dictionary (``bits_per_key > 0``). ``try_new`` must not surface
    // an "Unexpected end of rle iterator" error for this valid
    // payload -- eager decoding would do so because the hybrid-RLE
    // stream returns ``None`` after the bitpacked-zero-groups header.
    // Found on QuestDB-written ClickBench hits.parquet for mostly-null
    // SYMBOL columns split across multiple data pages.
    let dict = TestDictDecoder::new(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    let buffer = encode_rle_data(&[], 6);
    assert_eq!(
        buffer,
        vec![6, 0x01],
        "encoder emits bits_per_key byte + bitpacked-zero-groups header",
    );

    // Must succeed without eager decoding.
    let _ = RleDictionarySlicer::try_new(&buffer, dict, 0, 0)
        .expect("try_new must not fail on a valid zero-values stream");
}

#[test]
fn test_rle_dictionary_slicer_truncated_payload_errors_on_first_consume() {
    // The lazy-decode change shifts the error for a truncated payload
    // from `try_new` to the first consumer call. This test pins that
    // contract: a positive `bits_per_key` with no hybrid-RLE body
    // constructs successfully but errors the moment the caller tries
    // to read a value.
    let buffer: Vec<u8> = vec![6];

    // next(): single-value consumer path.
    {
        let dict = TestDictDecoder::new(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        let mut slicer = RleDictionarySlicer::try_new(&buffer, dict, 1, 1).unwrap();
        let err = slicer.next().unwrap_err();
        assert!(
            err.to_string().contains("Unexpected end of rle iterator"),
            "unexpected error: {err}"
        );
    }

    // next_into(): sink-based consumer path.
    {
        let dict = TestDictDecoder::new(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        let mut slicer = RleDictionarySlicer::try_new(&buffer, dict, 1, 1).unwrap();
        let mut sink = TestSink::new();
        let err = slicer.next_into(&mut sink).unwrap_err();
        assert!(
            err.to_string().contains("Unexpected end of rle iterator"),
            "unexpected error: {err}"
        );
    }

    // skip(): skip consumer path.
    {
        let dict = TestDictDecoder::new(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        let mut slicer = RleDictionarySlicer::try_new(&buffer, dict, 1, 1).unwrap();
        let err = slicer.skip(1).unwrap_err();
        assert!(
            err.to_string().contains("Unexpected end of rle iterator"),
            "unexpected error: {err}"
        );
    }
}

#[test]
fn test_rle_dictionary_slicer_empty_buffer_errors_on_construction() {
    // `try_new` must reject an empty buffer (no bit_width byte) with a
    // clear layout error rather than panicking.
    let dict = TestDictDecoder::new(vec![b"a".to_vec()]);
    let err = match RleDictionarySlicer::try_new(&[], dict, 0, 0) {
        Ok(_) => panic!("try_new must reject an empty buffer"),
        Err(e) => e,
    };
    assert!(
        err.to_string()
            .contains("RLE dictionary page is missing the initial byte with bit width"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_rle_dictionary_slicer_zero_bit_width() {
    let dict = TestDictDecoder::new(vec![b"only_value".to_vec()]);
    let buffer: Vec<u8> = vec![0];

    let mut slicer = RleDictionarySlicer::try_new(&buffer, dict, 10, 10).unwrap();
    slicer.skip(5).unwrap();
    assert_eq!(slicer.next().unwrap(), b"only_value");

    slicer.skip(3).unwrap();
    assert_eq!(slicer.next().unwrap(), b"only_value");
}
