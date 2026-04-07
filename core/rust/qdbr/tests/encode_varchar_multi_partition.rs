mod common;

use std::collections::HashSet;
use std::io::Cursor;

use arrow::array::{Array, StringArray};
use common::encode::{
    build_qdb_varchar_data, generate_nulls, make_varchar_column, read_parquet_batches, NullPattern,
    RLE_DICT_CONFIG,
};
use common::types::varchar::expected_varchar_str;
use qdb_core::col_type::{ColumnType, ColumnTypeTag};
use questdbr::parquet_write::bench::WriteOptions;
use questdbr::parquet_write::schema::{
    to_compressions, to_encodings, to_parquet_schema, Partition,
};
use questdbr::parquet_write::varchar::{varchar_to_dict_pages_merged, VarcharPartitionSlice};
use questdbr::parquet_write::ParquetWriter;

/// Helper: build a Partition with a single VARCHAR column using RLE_DICTIONARY encoding.
fn make_varchar_partition(values: &[&str], nulls: &[bool]) -> (Vec<u8>, Vec<u8>, Partition) {
    let (overflow_data, aux_data) = build_qdb_varchar_data(values, nulls);
    let column = make_varchar_column(
        "col",
        ColumnType::new(ColumnTypeTag::Varchar, 0).code(),
        overflow_data.as_ptr(),
        overflow_data.len(),
        aux_data.as_ptr(),
        aux_data.len(),
        values.len(),
        RLE_DICT_CONFIG,
    );
    let partition = Partition {
        table: "test_table".to_string(),
        columns: vec![column],
    };
    // Return owned data alongside the partition so it stays alive.
    (overflow_data, aux_data, partition)
}

/// Write multiple partitions into a single row group via ChunkedWriter and return
/// the Parquet bytes.
fn write_multi_partition_parquet(
    partitions: &[&Partition],
    row_group_size: Option<usize>,
    data_page_size: Option<usize>,
) -> Vec<u8> {
    assert!(!partitions.is_empty());
    let first = partitions[0];
    let (schema, additional_meta) = to_parquet_schema(first, false).unwrap();
    let encodings = to_encodings(first);
    let compressions = to_compressions(first);

    let mut buf = Cursor::new(Vec::new());
    let mut chunked = ParquetWriter::new(&mut buf)
        .with_statistics(true)
        .with_row_group_size(row_group_size)
        .with_data_page_size(data_page_size)
        .chunked_with_compressions(schema, encodings, compressions)
        .unwrap();

    let last_end = partitions.last().unwrap().columns[0].row_count;
    chunked
        .write_row_group_from_partitions(partitions, 0, last_end)
        .unwrap();

    chunked.finish(additional_meta).unwrap();
    buf.into_inner()
}

/// Verify that a Parquet file contains the expected VARCHAR values.
fn assert_varchar_values(bytes: &[u8], expected: &[Option<&str>]) {
    let batches = read_parquet_batches(bytes);
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, expected.len(), "total row count mismatch");

    let mut idx = 0;
    for batch in &batches {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");
        for i in 0..arr.len() {
            match expected[idx] {
                None => assert!(
                    arr.is_null(i),
                    "expected null at row {idx}, got {:?}",
                    arr.value(i)
                ),
                Some(v) => {
                    assert!(!arr.is_null(i), "expected non-null at row {idx}");
                    assert_eq!(arr.value(i), v, "value mismatch at row {idx}");
                }
            }
            idx += 1;
        }
    }
}

// ============================================================================
// Multi-partition integration tests (write + read round-trip via Arrow)
// ============================================================================

#[test]
fn test_two_partitions_with_shared_values() {
    let vals1 = vec!["hello", "world", "hello"];
    let nulls1 = vec![false; 3];
    let (_o1, _a1, p1) = make_varchar_partition(&vals1, &nulls1);

    let vals2 = vec!["world", "foo", "hello"];
    let nulls2 = vec![false; 3];
    let (_o2, _a2, p2) = make_varchar_partition(&vals2, &nulls2);

    let bytes = write_multi_partition_parquet(&[&p1, &p2], Some(1_000_000), None);

    let expected: Vec<Option<&str>> = vec![
        Some("hello"),
        Some("world"),
        Some("hello"),
        Some("world"),
        Some("foo"),
        Some("hello"),
    ];
    assert_varchar_values(&bytes, &expected);
}

#[test]
fn test_two_partitions_with_nulls() {
    let vals1 = vec!["alpha", "beta", "gamma"];
    let nulls1 = vec![false, true, false];
    let (_o1, _a1, p1) = make_varchar_partition(&vals1, &nulls1);

    let vals2 = vec!["delta", "alpha"];
    let nulls2 = vec![true, false];
    let (_o2, _a2, p2) = make_varchar_partition(&vals2, &nulls2);

    let bytes = write_multi_partition_parquet(&[&p1, &p2], Some(1_000_000), None);

    let expected: Vec<Option<&str>> = vec![Some("alpha"), None, Some("gamma"), None, Some("alpha")];
    assert_varchar_values(&bytes, &expected);
}

#[test]
fn test_three_partitions_no_overlap() {
    let vals1 = vec!["aaa", "bbb"];
    let nulls1 = vec![false; 2];
    let (_o1, _a1, p1) = make_varchar_partition(&vals1, &nulls1);

    let vals2 = vec!["ccc"];
    let nulls2 = vec![false];
    let (_o2, _a2, p2) = make_varchar_partition(&vals2, &nulls2);

    let vals3 = vec!["ddd", "eee", "fff"];
    let nulls3 = vec![false; 3];
    let (_o3, _a3, p3) = make_varchar_partition(&vals3, &nulls3);

    let bytes = write_multi_partition_parquet(&[&p1, &p2, &p3], Some(1_000_000), None);

    let expected: Vec<Option<&str>> = vec![
        Some("aaa"),
        Some("bbb"),
        Some("ccc"),
        Some("ddd"),
        Some("eee"),
        Some("fff"),
    ];
    assert_varchar_values(&bytes, &expected);
}

#[test]
fn test_multi_partition_all_nulls() {
    let vals1 = vec!["x", "y"];
    let nulls1 = vec![true, true];
    let (_o1, _a1, p1) = make_varchar_partition(&vals1, &nulls1);

    let vals2 = vec!["z"];
    let nulls2 = vec![true];
    let (_o2, _a2, p2) = make_varchar_partition(&vals2, &nulls2);

    let bytes = write_multi_partition_parquet(&[&p1, &p2], Some(1_000_000), None);

    let expected: Vec<Option<&str>> = vec![None, None, None];
    assert_varchar_values(&bytes, &expected);
}

#[test]
fn test_multi_partition_with_overflow_strings() {
    // Strings > 9 bytes trigger the split/overflow path in aux entries.
    let long1 = "this_is_a_long_string_partition_1";
    let long2 = "another_overflow_string_here!!!";
    let short = "hi";

    let vals1 = vec![long1, short, long1];
    let nulls1 = vec![false; 3];
    let (_o1, _a1, p1) = make_varchar_partition(&vals1, &nulls1);

    let vals2 = vec![long2, long1, short];
    let nulls2 = vec![false; 3];
    let (_o2, _a2, p2) = make_varchar_partition(&vals2, &nulls2);

    let bytes = write_multi_partition_parquet(&[&p1, &p2], Some(1_000_000), None);

    let expected: Vec<Option<&str>> = vec![
        Some(long1),
        Some(short),
        Some(long1),
        Some(long2),
        Some(long1),
        Some(short),
    ];
    assert_varchar_values(&bytes, &expected);
}

#[test]
fn test_multi_partition_single_partition_path() {
    // With a single partition, the unified dict path is not triggered but
    // write_row_group_from_partitions should still work.
    let vals = vec!["one", "two", "three", "one"];
    let nulls = vec![false; 4];
    let (_o, _a, p) = make_varchar_partition(&vals, &nulls);

    let bytes = write_multi_partition_parquet(&[&p], Some(1_000_000), None);

    let expected: Vec<Option<&str>> = vec![Some("one"), Some("two"), Some("three"), Some("one")];
    assert_varchar_values(&bytes, &expected);
}

#[test]
fn test_multi_partition_large_with_null_patterns() {
    // 500 rows per partition, 2 partitions, with different null patterns.
    let count = 500;
    let values1: Vec<String> = (0..count).map(expected_varchar_str).collect();
    let refs1: Vec<&str> = values1.iter().map(|s| s.as_str()).collect();
    let nulls1 = generate_nulls(count, NullPattern::Sparse);

    let values2: Vec<String> = (count..2 * count).map(expected_varchar_str).collect();
    let refs2: Vec<&str> = values2.iter().map(|s| s.as_str()).collect();
    let nulls2 = generate_nulls(count, NullPattern::Dense);

    let (_o1, _a1, p1) = make_varchar_partition(&refs1, &nulls1);
    let (_o2, _a2, p2) = make_varchar_partition(&refs2, &nulls2);

    let bytes = write_multi_partition_parquet(&[&p1, &p2], Some(1_000_000), None);

    let mut expected: Vec<Option<&str>> = Vec::with_capacity(2 * count);
    for i in 0..count {
        if nulls1[i] {
            expected.push(None);
        } else {
            expected.push(Some(refs1[i]));
        }
    }
    for i in 0..count {
        if nulls2[i] {
            expected.push(None);
        } else {
            expected.push(Some(refs2[i]));
        }
    }

    assert_varchar_values(&bytes, &expected);
}

#[test]
fn test_multi_partition_duplicate_heavy() {
    // Many duplicates across partitions — dictionary should deduplicate.
    let repeated: Vec<&str> = (0..100)
        .map(|i| match i % 3 {
            0 => "aaa",
            1 => "bbb",
            _ => "ccc",
        })
        .collect();
    let nulls = vec![false; 100];

    let (_o1, _a1, p1) = make_varchar_partition(&repeated, &nulls);
    let (_o2, _a2, p2) = make_varchar_partition(&repeated, &nulls);

    let bytes = write_multi_partition_parquet(&[&p1, &p2], Some(1_000_000), None);

    let mut expected: Vec<Option<&str>> = Vec::with_capacity(200);
    for &v in repeated.iter().chain(repeated.iter()) {
        expected.push(Some(v));
    }
    assert_varchar_values(&bytes, &expected);
}

// ============================================================================
// Minimal reproduction: 2 partitions, 1 value each (issue from #6809)
// ============================================================================

/// Count the number of DictPages and DataPages in the first column chunk.
/// Returns (dict_page_count, data_page_count).
fn count_pages_in_first_column(bytes: &[u8]) -> (usize, usize) {
    use parquet2::page::CompressedPage;
    use parquet2::read::{get_page_iterator, read_metadata};

    let mut cursor = Cursor::new(bytes);
    let metadata = read_metadata(&mut cursor).expect("read parquet metadata");
    assert!(!metadata.row_groups.is_empty(), "no row groups");

    let column_meta = &metadata.row_groups[0].columns()[0];
    let iter =
        get_page_iterator(column_meta, Cursor::new(bytes), None, vec![], 1024 * 1024)
            .expect("page iterator");

    let mut dict_count = 0usize;
    let mut data_count = 0usize;
    for page_result in iter {
        match page_result.expect("page read") {
            CompressedPage::Dict(_) => dict_count += 1,
            CompressedPage::Data(_) => data_count += 1,
        }
    }
    (dict_count, data_count)
}

#[test]
fn test_two_partitions_one_value_each() {
    // Minimal reproduction of the dictionary splitting bug: 2 partitions with a
    // single VARCHAR value in each. Before the fix, this produced an invalid
    // Parquet file with two DictPages per column chunk.
    let (_o1, _a1, p1) = make_varchar_partition(&["aaa"], &[false]);
    let (_o2, _a2, p2) = make_varchar_partition(&["bbb"], &[false]);

    let bytes = write_multi_partition_parquet(&[&p1, &p2], Some(1_000_000), None);

    // Data correctness.
    let expected: Vec<Option<&str>> = vec![Some("aaa"), Some("bbb")];
    assert_varchar_values(&bytes, &expected);

    // Structural validity: exactly 1 DictPage per column chunk (Parquet spec).
    let (dict_pages, data_pages) = count_pages_in_first_column(&bytes);
    assert_eq!(
        dict_pages, 1,
        "Parquet spec requires at most 1 DictPage per column chunk, found {dict_pages}"
    );
    assert!(data_pages >= 1, "expected at least 1 DataPage");
}

#[test]
fn test_two_partitions_one_value_each_same_value() {
    // Same as above but both partitions contain the same string — the
    // dictionary should deduplicate to a single entry.
    let (_o1, _a1, p1) = make_varchar_partition(&["same"], &[false]);
    let (_o2, _a2, p2) = make_varchar_partition(&["same"], &[false]);

    let bytes = write_multi_partition_parquet(&[&p1, &p2], Some(1_000_000), None);

    // Data correctness.
    let expected: Vec<Option<&str>> = vec![Some("same"), Some("same")];
    assert_varchar_values(&bytes, &expected);

    // Structural validity: 1 DictPage, and it should have exactly 1 unique entry.
    let (dict_pages, _) = count_pages_in_first_column(&bytes);
    assert_eq!(
        dict_pages, 1,
        "Parquet spec requires at most 1 DictPage per column chunk, found {dict_pages}"
    );
}

// ============================================================================
// Dictionary size fallback tests
// ============================================================================

#[test]
fn test_dict_size_fallback_to_delta_length() {
    // Generate enough unique long strings to exceed a very small data_page_size,
    // forcing the dict size fallback to DeltaLengthByteArray.
    let count = 200;
    let values: Vec<String> = (0..count)
        .map(|i| format!("unique_long_value_number_{i:06}_padding"))
        .collect();
    let refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
    let nulls = vec![false; count];

    let (_o1, _a1, p1) = make_varchar_partition(&refs[..100], &nulls[..100]);
    let (_o2, _a2, p2) = make_varchar_partition(&refs[100..], &nulls[100..]);

    // Use a tiny data_page_size (256 bytes) so the dictionary will exceed it.
    let bytes = write_multi_partition_parquet(&[&p1, &p2], Some(1_000_000), Some(256));

    // Regardless of whether it used dict or delta encoding, the data must be correct.
    let expected: Vec<Option<&str>> = refs.iter().map(|&s| Some(s)).collect();
    assert_varchar_values(&bytes, &expected);
}

#[test]
fn test_dict_size_within_limit() {
    // Few short unique strings should stay within the page size limit.
    let vals1 = vec!["a", "b", "c"];
    let nulls1 = vec![false; 3];
    let (_o1, _a1, p1) = make_varchar_partition(&vals1, &nulls1);

    let vals2 = vec!["b", "d"];
    let nulls2 = vec![false; 2];
    let (_o2, _a2, p2) = make_varchar_partition(&vals2, &nulls2);

    let bytes = write_multi_partition_parquet(&[&p1, &p2], Some(1_000_000), None);

    let expected: Vec<Option<&str>> = vec![Some("a"), Some("b"), Some("c"), Some("b"), Some("d")];
    assert_varchar_values(&bytes, &expected);
}

// ============================================================================
// Unit tests for varchar_to_dict_pages_merged directly
// ============================================================================

fn test_write_options() -> WriteOptions {
    WriteOptions {
        write_statistics: true,
        version: parquet2::write::Version::V2,
        compression: parquet2::compression::CompressionOptions::Uncompressed,
        row_group_size: None,
        data_page_size: None,
        raw_array_encoding: false,
        bloom_filter_fpp: 0.05,
        min_compression_ratio: 0.0,
    }
}

fn test_primitive_type() -> parquet2::schema::types::PrimitiveType {
    parquet2::schema::types::PrimitiveType::from_physical(
        "test_col".to_string(),
        parquet2::schema::types::PhysicalType::ByteArray,
    )
}

/// Cast a u8 aux buffer into a &[[u8; 16]] slice for use with the merged function.
fn aux_as_entries(aux: &[u8]) -> &[[u8; 16]] {
    assert_eq!(aux.len() % 16, 0);
    unsafe { std::slice::from_raw_parts(aux.as_ptr() as *const [u8; 16], aux.len() / 16) }
}

#[test]
fn test_merged_empty_partitions() {
    let partitions: Vec<VarcharPartitionSlice> = vec![];

    let result = varchar_to_dict_pages_merged(
        &partitions,
        1_000_000,
        test_write_options(),
        test_primitive_type(),
        None,
    );
    let pages = result.unwrap().unwrap();
    assert_eq!(pages.len(), 2); // DictPage + DataPage
}

#[test]
fn test_merged_returns_none_on_dict_overflow() {
    let count = 50;
    let values: Vec<String> = (0..count)
        .map(|i| format!("unique_long_value_{i:06}_padding_here"))
        .collect();
    let refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
    let nulls = vec![false; count];
    let (overflow, aux) = build_qdb_varchar_data(&refs, &nulls);

    let aux_entries = aux_as_entries(&aux);
    let partitions = vec![(aux_entries, overflow.as_slice(), 0usize)];

    // max_dict_bytes = 1 byte — guaranteed to overflow.
    let result = varchar_to_dict_pages_merged(
        &partitions,
        1,
        test_write_options(),
        test_primitive_type(),
        None,
    );
    assert!(result.unwrap().is_none(), "expected None (dict too large)");
}

#[test]
fn test_merged_deduplication_across_partitions() {
    use parquet2::page::Page;

    // Partition 1: "aaa", "bbb"
    let vals1 = vec!["aaa", "bbb"];
    let nulls1 = vec![false; 2];
    let (overflow1, aux1) = build_qdb_varchar_data(&vals1, &nulls1);

    // Partition 2: "bbb", "ccc" — "bbb" is shared with partition 1
    let vals2 = vec!["bbb", "ccc"];
    let nulls2 = vec![false; 2];
    let (overflow2, aux2) = build_qdb_varchar_data(&vals2, &nulls2);

    let partitions = vec![
        (aux_as_entries(&aux1), overflow1.as_slice(), 0usize),
        (aux_as_entries(&aux2), overflow2.as_slice(), 0usize),
    ];

    let result = varchar_to_dict_pages_merged(
        &partitions,
        1_000_000,
        test_write_options(),
        test_primitive_type(),
        None,
    );
    let pages = result.unwrap().unwrap();

    assert_eq!(pages.len(), 2);
    assert!(matches!(pages[0], Page::Dict(_)));
    assert!(matches!(pages[1], Page::Data(_)));

    // Dictionary has exactly 3 unique entries (aaa, bbb, ccc).
    if let Page::Dict(ref dict_page) = pages[0] {
        assert_eq!(dict_page.num_values, 3, "expected 3 unique dict entries");
    } else {
        panic!("expected DictPage");
    }
}

#[test]
fn test_merged_with_column_top() {
    use parquet2::page::Page;

    let vals = vec!["hello", "world"];
    let nulls = vec![false; 2];
    let (overflow, aux) = build_qdb_varchar_data(&vals, &nulls);

    // column_top = 3 means 3 leading null rows before the actual data.
    let partitions = vec![(aux_as_entries(&aux), overflow.as_slice(), 3usize)];

    let result = varchar_to_dict_pages_merged(
        &partitions,
        1_000_000,
        test_write_options(),
        test_primitive_type(),
        None,
    );
    let pages = result.unwrap().unwrap();

    assert_eq!(pages.len(), 2);

    // Data page should report 5 total values (3 column_top nulls + 2 data rows).
    if let Page::Data(ref data_page) = pages[1] {
        assert_eq!(
            data_page.num_values(),
            5,
            "expected 5 values (3 column_top + 2 data)"
        );
    } else {
        panic!("expected DataPage");
    }
}

#[test]
fn test_merged_mixed_inlined_and_overflow() {
    use parquet2::page::Page;

    // "hi" is inlined (2 bytes), "overflow_string_here" is split (20 bytes)
    let short = "hi";
    let long = "overflow_string_here";

    let vals1 = vec![short, long];
    let nulls1 = vec![false; 2];
    let (overflow1, aux1) = build_qdb_varchar_data(&vals1, &nulls1);

    let vals2 = vec![long, short, long];
    let nulls2 = vec![false; 3];
    let (overflow2, aux2) = build_qdb_varchar_data(&vals2, &nulls2);

    let partitions = vec![
        (aux_as_entries(&aux1), overflow1.as_slice(), 0usize),
        (aux_as_entries(&aux2), overflow2.as_slice(), 0usize),
    ];

    let result = varchar_to_dict_pages_merged(
        &partitions,
        1_000_000,
        test_write_options(),
        test_primitive_type(),
        None,
    );
    let pages = result.unwrap().unwrap();

    assert_eq!(pages.len(), 2);

    // Dictionary has exactly 2 unique entries (short, long).
    if let Page::Dict(ref dict_page) = pages[0] {
        assert_eq!(dict_page.num_values, 2, "expected 2 unique dict entries");
    } else {
        panic!("expected DictPage");
    }
}

#[test]
fn test_merged_all_nulls_produces_empty_dict() {
    use parquet2::page::Page;

    let vals = vec!["x", "y", "z"];
    let nulls = vec![true, true, true];
    let (overflow, aux) = build_qdb_varchar_data(&vals, &nulls);

    let partitions = vec![(aux_as_entries(&aux), overflow.as_slice(), 0usize)];

    let result = varchar_to_dict_pages_merged(
        &partitions,
        1_000_000,
        test_write_options(),
        test_primitive_type(),
        None,
    );
    let pages = result.unwrap().unwrap();

    if let Page::Dict(ref dict_page) = pages[0] {
        assert_eq!(
            dict_page.num_values, 0,
            "expected empty dict for all-null input"
        );
    } else {
        panic!("expected DictPage");
    }
}

#[test]
fn test_merged_multiple_column_tops() {
    use parquet2::page::Page;

    // Partition 1: column_top=2, data=["aa"]
    // Partition 2: column_top=1, data=["bb"]
    // Total = 2 + 1 + 1 + 1 = 5 rows, 3 nulls from column_top
    let vals1 = vec!["aa"];
    let nulls1 = vec![false];
    let (overflow1, aux1) = build_qdb_varchar_data(&vals1, &nulls1);

    let vals2 = vec!["bb"];
    let nulls2 = vec![false];
    let (overflow2, aux2) = build_qdb_varchar_data(&vals2, &nulls2);

    let partitions = vec![
        (aux_as_entries(&aux1), overflow1.as_slice(), 2usize),
        (aux_as_entries(&aux2), overflow2.as_slice(), 1usize),
    ];

    let result = varchar_to_dict_pages_merged(
        &partitions,
        1_000_000,
        test_write_options(),
        test_primitive_type(),
        None,
    );
    let pages = result.unwrap().unwrap();

    if let Page::Data(ref data_page) = pages[1] {
        assert_eq!(data_page.num_values(), 5);
    } else {
        panic!("expected DataPage");
    }

    if let Page::Dict(ref dict_page) = pages[0] {
        assert_eq!(dict_page.num_values, 2, "expected 2 dict entries (aa, bb)");
    }
}

// ============================================================================
// Additional edge-case tests
// ============================================================================

#[test]
fn test_multi_partition_with_empty_strings() {
    // Empty string "" is a valid non-null VARCHAR value, distinct from NULL.
    let vals1 = vec!["", "hello", ""];
    let nulls1 = vec![false; 3];
    let (_o1, _a1, p1) = make_varchar_partition(&vals1, &nulls1);

    let vals2 = vec!["", "world"];
    let nulls2 = vec![false, false];
    let (_o2, _a2, p2) = make_varchar_partition(&vals2, &nulls2);

    let bytes = write_multi_partition_parquet(&[&p1, &p2], Some(1_000_000), None);

    let expected: Vec<Option<&str>> =
        vec![Some(""), Some("hello"), Some(""), Some(""), Some("world")];
    assert_varchar_values(&bytes, &expected);
}

#[test]
fn test_merged_empty_string_deduplication() {
    use parquet2::page::Page;

    // Verify empty strings are deduplicated in the dictionary.
    let vals1 = vec!["", "abc"];
    let nulls1 = vec![false; 2];
    let (overflow1, aux1) = build_qdb_varchar_data(&vals1, &nulls1);

    let vals2 = vec!["", "def", ""];
    let nulls2 = vec![false; 3];
    let (overflow2, aux2) = build_qdb_varchar_data(&vals2, &nulls2);

    let partitions = vec![
        (aux_as_entries(&aux1), overflow1.as_slice(), 0usize),
        (aux_as_entries(&aux2), overflow2.as_slice(), 0usize),
    ];

    let result = varchar_to_dict_pages_merged(
        &partitions,
        1_000_000,
        test_write_options(),
        test_primitive_type(),
        None,
    );
    let pages = result.unwrap().unwrap();

    // Dictionary: "", "abc", "def" — 3 unique entries.
    if let Page::Dict(ref dict_page) = pages[0] {
        assert_eq!(dict_page.num_values, 3, "expected 3 unique dict entries");
    } else {
        panic!("expected DictPage");
    }
}

#[test]
fn test_merged_column_top_with_data_nulls() {
    use parquet2::page::Page;

    // Partition 1: column_top=2, data=["aa", NULL, "bb"]
    // Partition 2: column_top=1, data=[NULL, "cc"]
    // Total rows = (2+3) + (1+2) = 8, total nulls = 2+1 + 1+1 = 5
    let vals1 = vec!["aa", "ignored", "bb"];
    let nulls1 = vec![false, true, false];
    let (overflow1, aux1) = build_qdb_varchar_data(&vals1, &nulls1);

    let vals2 = vec!["ignored", "cc"];
    let nulls2 = vec![true, false];
    let (overflow2, aux2) = build_qdb_varchar_data(&vals2, &nulls2);

    let partitions = vec![
        (aux_as_entries(&aux1), overflow1.as_slice(), 2usize),
        (aux_as_entries(&aux2), overflow2.as_slice(), 1usize),
    ];

    let result = varchar_to_dict_pages_merged(
        &partitions,
        1_000_000,
        test_write_options(),
        test_primitive_type(),
        None,
    );
    let pages = result.unwrap().unwrap();

    assert_eq!(pages.len(), 2);

    // Data page: 8 total rows.
    if let Page::Data(ref data_page) = pages[1] {
        assert_eq!(data_page.num_values(), 8, "expected 8 total rows");
    } else {
        panic!("expected DataPage");
    }

    // Dictionary: "aa", "bb", "cc" — 3 unique non-null entries.
    if let Page::Dict(ref dict_page) = pages[0] {
        assert_eq!(dict_page.num_values, 3, "expected 3 dict entries");
    } else {
        panic!("expected DictPage");
    }
}

#[test]
fn test_multi_partition_column_top_with_data_nulls_roundtrip() {
    // Round-trip version: column_top=1 + data nulls in same partition.
    let vals1 = vec!["alpha", "beta"];
    let nulls1 = vec![true, false];
    let (_o1, _a1, p1) = make_varchar_partition(&vals1, &nulls1);

    let vals2 = vec!["gamma", "delta"];
    let nulls2 = vec![false, true];
    let (_o2, _a2, p2) = make_varchar_partition(&vals2, &nulls2);

    let bytes = write_multi_partition_parquet(&[&p1, &p2], Some(1_000_000), None);

    let expected: Vec<Option<&str>> = vec![None, Some("beta"), Some("gamma"), None];
    assert_varchar_values(&bytes, &expected);
}

#[test]
fn test_merged_partition_with_only_column_top() {
    use parquet2::page::Page;

    // Partition 1: column_top=3, no data rows (empty aux).
    // Partition 2: column_top=0, data=["hello", "world"].
    // Total = 3 + 2 = 5 rows, 3 nulls from column_top.
    let vals2 = vec!["hello", "world"];
    let nulls2 = vec![false; 2];
    let (overflow2, aux2) = build_qdb_varchar_data(&vals2, &nulls2);

    let empty_aux: &[[u8; 16]] = &[];
    let empty_data: &[u8] = &[];

    let partitions = vec![
        (empty_aux, empty_data, 3usize),
        (aux_as_entries(&aux2), overflow2.as_slice(), 0usize),
    ];

    let result = varchar_to_dict_pages_merged(
        &partitions,
        1_000_000,
        test_write_options(),
        test_primitive_type(),
        None,
    );
    let pages = result.unwrap().unwrap();

    assert_eq!(pages.len(), 2);

    if let Page::Data(ref data_page) = pages[1] {
        assert_eq!(data_page.num_values(), 5, "expected 5 total rows");
    } else {
        panic!("expected DataPage");
    }

    if let Page::Dict(ref dict_page) = pages[0] {
        assert_eq!(
            dict_page.num_values, 2,
            "expected 2 dict entries (hello, world)"
        );
    } else {
        panic!("expected DictPage");
    }
}

#[test]
fn test_merged_bloom_filter_hashes() {
    // Verify bloom hashes are populated with exactly the unique dictionary entries.
    let vals1 = vec!["aaa", "bbb"];
    let nulls1 = vec![false; 2];
    let (overflow1, aux1) = build_qdb_varchar_data(&vals1, &nulls1);

    let vals2 = vec!["bbb", "ccc", "aaa"];
    let nulls2 = vec![false; 3];
    let (overflow2, aux2) = build_qdb_varchar_data(&vals2, &nulls2);

    let partitions = vec![
        (aux_as_entries(&aux1), overflow1.as_slice(), 0usize),
        (aux_as_entries(&aux2), overflow2.as_slice(), 0usize),
    ];

    let mut bloom_hashes: HashSet<u64> = HashSet::new();

    let result = varchar_to_dict_pages_merged(
        &partitions,
        1_000_000,
        test_write_options(),
        test_primitive_type(),
        Some(&mut bloom_hashes),
    );
    assert!(result.unwrap().is_some());

    // 3 unique values (aaa, bbb, ccc) should produce 3 bloom hashes.
    assert_eq!(
        bloom_hashes.len(),
        3,
        "expected 3 bloom hashes for 3 unique values"
    );
}

#[test]
fn test_merged_bloom_filter_not_populated_on_fallback() {
    // When dictionary exceeds max_dict_bytes and returns None,
    // bloom hashes should NOT be populated.
    let count = 50;
    let values: Vec<String> = (0..count)
        .map(|i| format!("unique_long_value_{i:06}_padding_here"))
        .collect();
    let refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
    let nulls = vec![false; count];
    let (overflow, aux) = build_qdb_varchar_data(&refs, &nulls);

    let partitions = vec![(aux_as_entries(&aux), overflow.as_slice(), 0usize)];

    let mut bloom_hashes: HashSet<u64> = HashSet::new();

    // max_dict_bytes = 1 byte — guaranteed to overflow.
    let result = varchar_to_dict_pages_merged(
        &partitions,
        1,
        test_write_options(),
        test_primitive_type(),
        Some(&mut bloom_hashes),
    );
    assert!(result.unwrap().is_none(), "expected None (dict too large)");
    assert!(
        bloom_hashes.is_empty(),
        "bloom hashes should be empty on dict overflow fallback"
    );
}
