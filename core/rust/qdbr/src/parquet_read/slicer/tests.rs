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
fn test_delta_slicers_empty_buffer_construct_without_panic() {
    // An all-null DELTA varlen page can arrive with an empty values buffer (no
    // delta header) from a foreign encoder via read_parquet(). Both slicers must
    // construct cleanly so the page's definition levels can drive push_null.
    // Without the empty-buffer guard the vendored parquet2 delta decoder divides
    // block_size/num_mini_blocks = 0/0 and panics, which aborts the JVM across the
    // JNI boundary (that panic would fail this test).
    assert!(DeltaLengthArraySlicer::try_new(&[], 10, 10).is_ok());
    assert!(DeltaBytesArraySlicer::try_new(&[], 10, 10).is_ok());
}

#[test]
fn test_delta_slicers_empty_buffer_value_request_errors() {
    // If a value IS requested from an empty-buffer slicer (a corrupt page whose
    // definition levels claim a non-null), both slicers must return a clean error
    // rather than indexing out of bounds and aborting the JVM.
    let mut length_slicer = DeltaLengthArraySlicer::try_new(&[], 10, 10).unwrap();
    assert!(length_slicer.next().is_err());
    let mut bytes_slicer = DeltaBytesArraySlicer::try_new(&[], 10, 10).unwrap();
    assert!(bytes_slicer.next().is_err());
}

#[test]
fn test_delta_length_array_slicer_oversized_length_errors() {
    // Encode one 4-byte value, then drop the 4 trailing data bytes so the decoded
    // length (4) exceeds the remaining values buffer. next()/next_into() must
    // return a clean error rather than slicing out of bounds and aborting the JVM.
    let strings = ["aaaa"];
    let mut encoded = Vec::new();
    parquet2::encoding::delta_length_byte_array::encode(
        strings.iter().map(|s| s.as_bytes()),
        &mut encoded,
    );
    encoded.truncate(encoded.len() - 4);

    let mut slicer = DeltaLengthArraySlicer::try_new(&encoded, 1, 1).unwrap();
    assert!(slicer.next().is_err());

    let mut slicer = DeltaLengthArraySlicer::try_new(&encoded, 1, 1).unwrap();
    let mut sink = TestSink::new();
    assert!(slicer.next_into(&mut sink).is_err());
}

#[test]
fn test_delta_bytes_array_slicer_oversized_suffix_errors() {
    // Encode one 5-byte value, then drop the 5 trailing data bytes so the decoded
    // suffix length (5) exceeds the remaining values buffer.
    let strings: Vec<&[u8]> = vec![b"Hello"];
    let mut encoded = Vec::new();
    parquet2::encoding::delta_byte_array::encode(strings.into_iter(), &mut encoded);
    encoded.truncate(encoded.len() - 5);

    let mut slicer = DeltaBytesArraySlicer::try_new(&encoded, 1, 1).unwrap();
    assert!(slicer.next().is_err());

    let mut slicer = DeltaBytesArraySlicer::try_new(&encoded, 1, 1).unwrap();
    let mut sink = TestSink::new();
    assert!(slicer.next_into(&mut sink).is_err());
}

#[test]
fn test_delta_length_array_slicer_negative_length_errors() {
    // Hand-crafted DELTA_LENGTH header: block_size=128, mini_blocks=1, count=1,
    // first_value=zigzag(1)=-1. A negative byte length must be rejected at decode.
    let data = [128u8, 1, 1, 1, 1];
    assert!(DeltaLengthArraySlicer::try_new(&data, 1, 1).is_err());
}

#[test]
fn test_delta_length_array_slicer_out_of_range_length_errors() {
    // Hand-crafted DELTA_LENGTH header with first_value = 2^31 (> i32::MAX):
    // zigzag(2^31) = 2^32 -> uleb128 [0x80, 0x80, 0x80, 0x80, 0x10]. A length that
    // does not fit i32 must be rejected at decode rather than wrapping.
    let data = [128u8, 1, 1, 1, 0x80, 0x80, 0x80, 0x80, 0x10];
    assert!(DeltaLengthArraySlicer::try_new(&data, 1, 1).is_err());
}

#[test]
fn collect_checked_lengths_rejects_unsatisfiable_count_instead_of_aborting() {
    // `limit` is the page's row count (num_values, attacker-controlled up to
    // i32::MAX). A bit-width-0 delta miniblock expands to that many values from
    // almost no buffer bytes, so the up-front reservation can be multi-gigabyte and
    // an infallible collect would abort the JVM when the allocator refuses it. A
    // real i32::MAX request may succeed on a large host, so pin the fallible path
    // with usize::MAX: try_reserve_exact fails with CapacityOverflow without
    // attempting (and aborting on) a real allocation. Proves the up-front sizing
    // surfaces a clean error rather than the process-aborting infallible collect.
    let empty = std::iter::empty::<Result<i64, ()>>();
    let err = collect_checked_lengths(empty, usize::MAX, "suffix")
        .expect_err("an unsatisfiable length count must error, not abort");
    assert!(
        err.to_string().contains("cannot allocate"),
        "unexpected error: {err}"
    );
    // Classify OutOfMemory, not Layout: on the write path (a parquet merge under
    // ApplyWal2TableJob) a Layout error suspends the table, whereas OutOfMemory
    // backs off and retries -- the correct response to transient memory pressure.
    assert!(
        matches!(
            err.reason(),
            crate::parquet::error::ParquetErrorReason::OutOfMemory(_)
        ),
        "allocation failure must be classified OutOfMemory, not Layout: {err:?}"
    );
}

#[test]
fn collect_checked_lengths_bounds_count_at_limit() {
    // The suffix collect previously had no take() and materialized every value the
    // delta stream declared (its own attacker-controlled total_count), independent
    // of the page's row count. Pin that collect_checked_lengths now caps the count
    // at `limit`: an iterator yielding far more values than the limit must produce
    // exactly `limit` entries, leaving the rest unread.
    let values = (0..1_000_000i64).map(Ok::<i64, ()>);
    let out = collect_checked_lengths(values, 8, "suffix").unwrap();
    assert_eq!(out.len(), 8, "count must be bounded by the limit");
    assert_eq!(out.first().copied(), Some(0));
    assert_eq!(out.last().copied(), Some(7));
}

#[test]
fn test_plain_var_slicer_oversized_length_errors() {
    // Length prefix claims 100 bytes but only 2 follow. next()/next_into() must
    // return a clean error rather than slicing out of bounds and aborting the JVM.
    let mut data = Vec::new();
    data.extend_from_slice(&100u32.to_le_bytes());
    data.extend_from_slice(b"ab");

    let mut slicer = PlainVarSlicer::new(&data, 1);
    assert!(slicer.next().is_err());

    let mut slicer = PlainVarSlicer::new(&data, 1);
    let mut sink = TestSink::new();
    assert!(slicer.next_into(&mut sink).is_err());
}

#[test]
fn test_plain_var_slicer_truncated_prefix_errors() {
    // Fewer than 4 bytes: not even a full length prefix. Every method must reject
    // it; previously next_into()/skip() read past the buffer (undefined behavior)
    // because only next() checked the prefix bound.
    let data = [1u8, 2];

    let mut slicer = PlainVarSlicer::new(&data, 1);
    assert!(slicer.next().is_err());

    let mut slicer = PlainVarSlicer::new(&data, 1);
    let mut sink = TestSink::new();
    assert!(slicer.next_into(&mut sink).is_err());

    let mut slicer = PlainVarSlicer::new(&data, 1);
    assert!(slicer.skip(1).is_err());
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
fn test_rle_dictionary_slicer_zero_bit_width() {
    let dict = TestDictDecoder::new(vec![b"only_value".to_vec()]);
    let buffer: Vec<u8> = vec![0];

    let mut slicer = RleDictionarySlicer::try_new(&buffer, dict, 10, 10).unwrap();
    slicer.skip(5).unwrap();
    assert_eq!(slicer.next().unwrap(), b"only_value");

    slicer.skip(3).unwrap();
    assert_eq!(slicer.next().unwrap(), b"only_value");
}

#[test]
fn test_delta_length_slicer_block_size_not_mult_128_errors() {
    // block_size=1 (not a multiple of 128): foreign/corrupt header. Must return a
    // clean error rather than panicking in parquet2 (decoder.rs:141) and aborting
    // the JVM over JNI.
    assert!(DeltaLengthArraySlicer::try_new(&[1, 1, 1, 0], 1, 1).is_err());
}

#[test]
fn test_delta_length_slicer_vpmb_not_mult_8_errors() {
    // block_size=128, mini_blocks=3 -> values_per_mini_block=42 (not a multiple of
    // 8): parquet2 decoder.rs:156. Must error, not panic.
    assert!(DeltaLengthArraySlicer::try_new(&[128, 1, 3, 1, 0], 1, 1).is_err());
}

#[test]
fn test_delta_length_slicer_zero_miniblocks_errors() {
    // block_size=0 (passes %128), mini_blocks=0 -> 0/0 divide at parquet2
    // decoder.rs:155. Must error, not panic.
    assert!(DeltaLengthArraySlicer::try_new(&[0, 0, 1, 0], 1, 1).is_err());
}

#[test]
fn test_delta_bytes_slicer_block_size_not_mult_128_errors() {
    // Same malformed header through DeltaBytesArraySlicer's first decoder (:451).
    assert!(DeltaBytesArraySlicer::try_new(&[1, 1, 1, 0], 1, 1).is_err());
}

#[test]
fn test_delta_bytes_slicer_empty_suffix_errors() {
    // Valid prefix header but an empty suffix region: the second parquet2
    // decoder would divide 0/0 (zero mini blocks). Must return a clean error
    // rather than panicking and aborting the JVM.
    assert!(DeltaBytesArraySlicer::try_new(&[128, 1, 1, 0], 1, 1).is_err());
}

// A foreign DELTA page whose first miniblock declares a bitwidth wider than the
// 64-bit values it unpacks into. The vendored parquet2 Block::advance_miniblock
// builds the first miniblock during Decoder::try_new (total_count >= 2); without
// its num_bits > 64 guard the bitpacked u64 unpacker hits unreachable!() and
// aborts the JVM over JNI. Header: block_size=128, mini_blocks=1, total_count=2,
// first_value=0, min_delta=0, bitwidth=65, then a 1040-byte miniblock.
fn delta_bitwidth_over_64_page() -> Vec<u8> {
    let mut data = vec![0x80u8, 0x01, 0x01, 0x02, 0x00, 0x00, 65];
    data.extend(std::iter::repeat_n(0u8, 1040));
    data
}

// A foreign DELTA page with block_size = 2^63: it passes the %128 header guard
// (2^63 % 128 == 0), so values_per_mini_block = 2^63. Without the checked_mul in
// advance_miniblock, values_per_mini_block * num_bits overflows usize and aborts
// the JVM (debug: the multiply; release: a wrapped short miniblock then a panic
// in the bitpacked decoder). Header: block_size=2^63, mini_blocks=1,
// total_count=2, first_value=0, min_delta=0, bitwidth=2, then a 64-byte tail.
fn delta_oversized_block_size_page() -> Vec<u8> {
    let mut data = vec![
        0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01, // block_size = 2^63
        0x01, // num_mini_blocks
        0x02, // total_count
        0x00, // first_value
        0x00, // min_delta
        0x02, // bitwidth
    ];
    data.extend(std::iter::repeat_n(0u8, 64));
    data
}

#[test]
fn test_delta_length_slicer_miniblock_bitwidth_over_64_errors() {
    assert!(DeltaLengthArraySlicer::try_new(&delta_bitwidth_over_64_page(), 2, 2).is_err());
}

#[test]
fn test_delta_length_slicer_oversized_block_size_errors() {
    assert!(DeltaLengthArraySlicer::try_new(&delta_oversized_block_size_page(), 2, 2).is_err());
}

#[test]
fn test_delta_bytes_slicer_miniblock_bitwidth_over_64_errors() {
    // Same malformed page through DeltaBytesArraySlicer's prefix decoder (:451).
    assert!(DeltaBytesArraySlicer::try_new(&delta_bitwidth_over_64_page(), 2, 2).is_err());
}

#[test]
fn test_delta_bytes_slicer_oversized_block_size_errors() {
    assert!(DeltaBytesArraySlicer::try_new(&delta_oversized_block_size_page(), 2, 2).is_err());
}

#[test]
fn test_delta_bytes_slicer_suffix_bitwidth_over_64_errors() {
    // A valid prefix block followed by a malformed suffix block drives the SECOND
    // parquet2 decoder in DeltaBytesArraySlicer::try_new (slicer/mod.rs:461, the
    // suffix lengths), which the prefix-malformed tests above never reach because
    // the prefix decoder fails first. The suffix declares miniblock bitwidth 65
    // and must surface a clean error, not abort the JVM. Prefix: block_size=128,
    // mini_blocks=1, total_count=1, first_value=0 (one length, no block).
    let mut data = vec![0x80u8, 0x01, 0x01, 0x01, 0x00];
    data.extend(delta_bitwidth_over_64_page());
    assert!(DeltaBytesArraySlicer::try_new(&data, 1, 1).is_err());
}

#[test]
fn test_delta_length_array_slicer_skip_beyond_lengths_errors() {
    // skip() must reject a count that runs past the decoded lengths via its
    // .get(index..index + count) bound, rather than panicking on the slice.
    let strings = ["aa", "bb"];
    let mut encoded = Vec::new();
    parquet2::encoding::delta_length_byte_array::encode(
        strings.iter().map(|s| s.as_bytes()),
        &mut encoded,
    );
    let mut slicer = DeltaLengthArraySlicer::try_new(&encoded, 2, 2).unwrap();
    let err = slicer.skip(3).unwrap_err();
    assert!(
        format!("{err}").contains("not enough length values to skip"),
        "got: {err}"
    );
}

#[test]
fn test_delta_length_array_slicer_next_into_beyond_lengths_errors() {
    // next_into()'s length-index bound must reject a request past the decoded
    // lengths rather than indexing out of bounds. (next() covers this via the
    // empty-buffer test; next_into has its own duplicate bound.)
    let strings = ["aa", "bb"];
    let mut encoded = Vec::new();
    parquet2::encoding::delta_length_byte_array::encode(
        strings.iter().map(|s| s.as_bytes()),
        &mut encoded,
    );
    let mut slicer = DeltaLengthArraySlicer::try_new(&encoded, 2, 2).unwrap();
    let mut sink = TestSink::new();
    slicer.next_into(&mut sink).unwrap(); // "aa"
    slicer.next_into(&mut sink).unwrap(); // "bb"
    let err = slicer.next_into(&mut sink).unwrap_err();
    assert!(
        format!("{err}").contains("not enough length values to iterate"),
        "got: {err}"
    );
}

#[test]
fn test_rle_dictionary_slicer_empty_buffer_errors() {
    // A foreign/corrupt dictionary page can arrive with an empty values buffer
    // (no leading bit-width byte) via read_parquet(). try_new must surface a
    // clean error rather than indexing buffer[0] out of bounds and aborting the
    // JVM across the JNI boundary.
    let dict = TestDictDecoder::new(vec![b"zero".to_vec(), b"one".to_vec()]);
    // .err().unwrap() rather than .unwrap_err(): the Ok type (RleDictionarySlicer)
    // is not Debug, which unwrap_err() would require.
    let err = RleDictionarySlicer::try_new(&[], dict, 5, 5).err().unwrap();
    assert!(
        format!("{err}").contains("missing the initial byte with bit width"),
        "got: {err}"
    );
}

#[test]
fn test_rle_dictionary_slicer_bitwidth_over_32_errors() {
    // A foreign RLE_DICTIONARY page whose index bit width (40) exceeds the 32-bit
    // u32 the indices unpack into. bitpacked::Decoder::<u32> only generates unpack
    // arms for 0..=32; a wider width would reach unreachable!("invalid num_bits
    // 40") and abort the JVM across JNI. next() must surface a clean error
    // instead. Wire format: [40 = bit width, 0x03 = bitpacked indicator (1 group
    // of 8 indices), then 40 data bytes = 8 * 40 bits].
    let dict = TestDictDecoder::new(vec![b"zero".to_vec(), b"one".to_vec()]);
    let mut encoded = vec![40u8, 0x03];
    encoded.extend(std::iter::repeat_n(0u8, 40));

    let mut slicer = RleDictionarySlicer::try_new(&encoded, dict, 8, 8).unwrap();
    let err = slicer.next().unwrap_err();
    assert!(
        format!("{err}").contains("exceeds"),
        "expected a clean bit-width error, got: {err}"
    );
}

#[test]
fn test_fixed_slicer_oversized_read_errors() {
    // A foreign/corrupt fixed-width page whose element count (driven by the
    // definition/repetition levels) exceeds the values buffer must surface a
    // clean error rather than indexing out of bounds and aborting the JVM. Here
    // the buffer holds 4 bytes but each element is 8.
    let data = [0u8; 4];

    let mut slicer = DataPageFixedSlicer::<8>::new(&data, 1);
    assert!(slicer.next().is_err());

    let mut slicer = DataPageFixedSlicer::<8>::new(&data, 1);
    let mut sink = TestSink::new();
    assert!(slicer.next_into(&mut sink).is_err());

    let mut slicer = DataPageFixedSlicer::<8>::new(&data, 1);
    let mut sink = TestSink::new();
    assert!(slicer.next_slice_into(1, &mut sink).is_err());

    // next_raw_slice signals "cannot provide a borrowed slice" with None rather
    // than panicking.
    let mut slicer = DataPageFixedSlicer::<8>::new(&data, 1);
    assert!(slicer.next_raw_slice(1).is_none());
}

#[test]
fn test_fixed_slicer_exact_buffer_reads_ok() {
    // Off-by-one guard for the bound above: an exactly-sized buffer (8 bytes,
    // one 8-byte element) must still read cleanly.
    let data = [1u8, 2, 3, 4, 5, 6, 7, 8];

    let mut slicer = DataPageFixedSlicer::<8>::new(&data, 1);
    assert_eq!(slicer.next().unwrap(), &data[..]);

    let mut slicer = DataPageFixedSlicer::<8>::new(&data, 1);
    assert_eq!(slicer.next_raw_slice(1), Some(&data[..]));

    let mut slicer = DataPageFixedSlicer::<8>::new(&data, 1);
    let mut sink = TestSink::new();
    slicer.next_slice_into(1, &mut sink).unwrap();
    assert_eq!(sink.into_inner(), data.to_vec());
}
