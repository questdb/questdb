mod common;

use arrow::array::{Array, StringArray};
use common::encode::{
    assert_single_column_bloom_metadata, build_qdb_string_data, generate_nulls, make_string_column,
    read_parquet_batches, write_parquet, write_parquet_with_bloom, ALL_BLOOM_FILTER_STATES,
    ALL_NULL_PATTERNS,
};
use common::types::strings::expected_str_value;
use qdb_core::col_type::{ColumnType, ColumnTypeTag};
use questdbr::parquet_write::schema::Partition;

use crate::common::Encoding;

const COUNT: usize = 1_000;

const STRING_ENCODINGS: [Encoding; 3] = [
    Encoding::Plain,
    Encoding::RleDictionary,
    Encoding::DeltaLengthByteArray,
];

#[test]
fn test_encode_string() {
    for encoding in &STRING_ENCODINGS {
        for null_pattern in &ALL_NULL_PATTERNS {
            for bloom_enabled in ALL_BLOOM_FILTER_STATES {
                let nulls = generate_nulls(COUNT, *null_pattern);
                let values: Vec<String> = (0..COUNT).map(expected_str_value).collect();
                let value_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();

                let (primary, offsets) = build_qdb_string_data(&value_refs, &nulls);
                let offsets_bytes = unsafe {
                    std::slice::from_raw_parts(
                        offsets.as_ptr() as *const u8,
                        offsets.len() * std::mem::size_of::<i64>(),
                    )
                };

                let column = make_string_column(
                    "col",
                    ColumnType::new(ColumnTypeTag::String, 0).code(),
                    primary.as_ptr(),
                    primary.len(),
                    offsets_bytes.as_ptr(),
                    offsets_bytes.len(),
                    COUNT,
                    encoding.config(),
                );

                let partition = Partition {
                    table: "test_table".to_string(),
                    columns: vec![column],
                };
                let bytes = if bloom_enabled {
                    write_parquet_with_bloom(partition)
                } else {
                    write_parquet(partition)
                };
                assert_single_column_bloom_metadata(&bytes, bloom_enabled);
                let batches = read_parquet_batches(&bytes);

                let arr = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("StringArray");
                assert_eq!(arr.len(), COUNT);

                for i in 0..COUNT {
                    if nulls[i] {
                        assert!(
                            arr.is_null(i),
                            "String {encoding:?}/{null_pattern:?}/bloom={bloom_enabled}: expected null at {i}"
                        );
                    } else {
                        assert!(
                            !arr.is_null(i),
                            "String {encoding:?}/{null_pattern:?}/bloom={bloom_enabled}: expected non-null at {i}"
                        );
                        assert_eq!(
                            arr.value(i),
                            values[i].as_str(),
                            "String {encoding:?}/{null_pattern:?}/bloom={bloom_enabled}: mismatch at {i}"
                        );
                    }
                }
            }
        }
    }
}

// Regression coverage for an all-null STRING partition, driven through QuestDB's
// own writer and reader. The varlen writer always emits a value_count=0 DELTA
// header, so this parses correctly both before and after the all-null delta fix;
// it closes the gap where no 100%-null varlen page was round-tripped.
#[test]
fn all_null_string_delta_round_trips_through_qdb_reader() {
    let nulls = vec![true; COUNT];
    let values = vec![""; COUNT];
    let (primary, offsets) = build_qdb_string_data(&values, &nulls);
    let offsets_bytes = unsafe {
        std::slice::from_raw_parts(
            offsets.as_ptr() as *const u8,
            std::mem::size_of_val(&offsets[..]),
        )
    };

    let column = make_string_column(
        "s_top",
        ColumnType::new(ColumnTypeTag::String, 0).code(),
        primary.as_ptr(),
        primary.len(),
        offsets_bytes.as_ptr(),
        offsets_bytes.len(),
        COUNT,
        Encoding::DeltaLengthByteArray.config(),
    );
    let partition = Partition {
        table: "repro".to_string(),
        columns: vec![column],
    };
    let parquet = write_parquet(partition);

    let (data_out, aux_out) = common::decode_file(&parquet);
    // All-null string: (COUNT + 1) u64 aux offsets and one i32(-1) length per row.
    assert_eq!(aux_out.len(), (COUNT + 1) * 8, "string aux size");
    assert_eq!(data_out.len(), COUNT * 4, "string data size");
    for i in 0..COUNT {
        let len = i32::from_le_bytes(data_out[i * 4..i * 4 + 4].try_into().unwrap());
        assert_eq!(len, -1, "row {i} should decode as a null string");
    }
}
