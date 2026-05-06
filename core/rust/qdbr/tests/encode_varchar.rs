mod common;

use arrow::array::{Array, StringArray};
use common::encode::{
    assert_single_column_bloom_metadata, build_qdb_varchar_data, generate_nulls,
    make_varchar_column, read_parquet_batches, write_parquet, write_parquet_with_bloom,
    ALL_BLOOM_FILTER_STATES, ALL_NULL_PATTERNS,
};
use common::types::varchar::expected_varchar_str;
use qdb_core::col_type::{ColumnType, ColumnTypeTag};
use questdbr::parquet_write::schema::Partition;

use crate::common::Encoding;

const COUNT: usize = 1_000;

const VARCHAR_ENCODINGS: [Encoding; 3] = [
    Encoding::Plain,
    Encoding::RleDictionary,
    Encoding::DeltaLengthByteArray,
];

#[test]
fn test_encode_varchar() {
    for encoding in &VARCHAR_ENCODINGS {
        for null_pattern in &ALL_NULL_PATTERNS {
            for bloom_enabled in ALL_BLOOM_FILTER_STATES {
                let nulls = generate_nulls(COUNT, *null_pattern);
                let values: Vec<String> = (0..COUNT).map(expected_varchar_str).collect();
                let value_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();

                let (overflow_data, aux_data) = build_qdb_varchar_data(&value_refs, &nulls);

                let column = make_varchar_column(
                    "col",
                    ColumnType::new(ColumnTypeTag::Varchar, 0).code(),
                    overflow_data.as_ptr(),
                    overflow_data.len(),
                    aux_data.as_ptr(),
                    aux_data.len(),
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
                            "Varchar {encoding:?}/{null_pattern:?}/bloom={bloom_enabled}: expected null at {i}"
                        );
                    } else {
                        assert!(
                            !arr.is_null(i),
                            "Varchar {encoding:?}/{null_pattern:?}/bloom={bloom_enabled}: expected non-null at {i}"
                        );
                        assert_eq!(
                            arr.value(i),
                            values[i].as_str(),
                            "Varchar {encoding:?}/{null_pattern:?}/bloom={bloom_enabled}: mismatch at {i}"
                        );
                    }
                }
            }
        }
    }
}
