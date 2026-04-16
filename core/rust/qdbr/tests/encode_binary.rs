mod common;

use arrow::array::{Array, BinaryArray};
use common::encode::{
    assert_single_column_bloom_metadata, build_qdb_binary_data, generate_nulls, make_string_column,
    read_parquet_batches, write_parquet, write_parquet_with_bloom, ALL_BLOOM_FILTER_STATES,
    ALL_NULL_PATTERNS,
};
use qdb_core::col_type::{ColumnType, ColumnTypeTag};
use questdbr::parquet_write::schema::Partition;

use crate::common::Encoding;

const COUNT: usize = 1_000;

const BINARY_ENCODINGS: [Encoding; 3] = [
    Encoding::Plain,
    Encoding::RleDictionary,
    Encoding::DeltaLengthByteArray,
];

fn generate_binary_value(i: usize) -> Vec<u8> {
    (0..10).map(|j| ((i * 7 + j) % 256) as u8).collect()
}

#[test]
fn test_encode_binary() {
    for encoding in &BINARY_ENCODINGS {
        for null_pattern in &ALL_NULL_PATTERNS {
            for bloom_enabled in ALL_BLOOM_FILTER_STATES {
                let nulls = generate_nulls(COUNT, *null_pattern);
                let values: Vec<Vec<u8>> = (0..COUNT).map(generate_binary_value).collect();
                let value_refs: Vec<&[u8]> = values.iter().map(|v| v.as_slice()).collect();

                let (primary, offsets) = build_qdb_binary_data(&value_refs, &nulls);
                let offsets_bytes = unsafe {
                    std::slice::from_raw_parts(
                        offsets.as_ptr() as *const u8,
                        offsets.len() * std::mem::size_of::<i64>(),
                    )
                };

                let column = make_string_column(
                    "col",
                    ColumnType::new(ColumnTypeTag::Binary, 0).code(),
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
                    .downcast_ref::<BinaryArray>()
                    .expect("BinaryArray");
                assert_eq!(arr.len(), COUNT);

                for i in 0..COUNT {
                    if nulls[i] {
                        assert!(
                            arr.is_null(i),
                            "Binary {encoding:?}/{null_pattern:?}/bloom={bloom_enabled}: expected null at {i}"
                        );
                    } else {
                        assert!(
                            !arr.is_null(i),
                            "Binary {encoding:?}/{null_pattern:?}/bloom={bloom_enabled}: expected non-null at {i}"
                        );
                        assert_eq!(
                            arr.value(i),
                            values[i].as_slice(),
                            "Binary {encoding:?}/{null_pattern:?}/bloom={bloom_enabled}: mismatch at {i}"
                        );
                    }
                }
            }
        }
    }
}
