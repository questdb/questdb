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

// Regression coverage for an all-null varlen partition (e.g. a column-top
// varchar column whose older partitions predate it). Mirrors
// `round_trip_all_null_long` in encode_primitives.rs but for the varlen path.
//
// Unlike the integer writer, the varlen writer always emits a value_count=0
// DELTA header (never an empty buffer), so an all-null varchar page parsed
// correctly even before the all-null delta fix -- but no test drove a
// QuestDB-written 100%-null varlen page through QuestDB's own reader. These do.
fn round_trip_all_null_varchar(encoding: Encoding) {
    let nulls = vec![true; COUNT];
    // Values are ignored for null rows; build_qdb_varchar_data only reads flags.
    let values = vec![""; COUNT];
    let (overflow_data, aux_data) = build_qdb_varchar_data(&values, &nulls);

    let column = make_varchar_column(
        "v_top",
        ColumnType::new(ColumnTypeTag::Varchar, 0).code(),
        overflow_data.as_ptr(),
        overflow_data.len(),
        aux_data.as_ptr(),
        aux_data.len(),
        COUNT,
        encoding.config(),
    );
    let partition = Partition {
        table: "repro".to_string(),
        columns: vec![column],
    };
    let parquet = write_parquet(partition);

    let (data_out, aux_out) = common::decode_file(&parquet);
    // All-null varchar: one 16-byte aux entry per row with the NULL flag (bit 2)
    // set, and no overflow data.
    const HEADER_FLAG_NULL: u8 = 4;
    assert_eq!(aux_out.len(), COUNT * 16, "varchar aux size");
    assert!(data_out.is_empty(), "all-null varchar has no overflow data");
    for i in 0..COUNT {
        assert_eq!(
            aux_out[i * 16] & HEADER_FLAG_NULL,
            HEADER_FLAG_NULL,
            "row {i} should decode as a null varchar"
        );
    }
}

#[test]
fn all_null_varchar_delta_round_trips_through_qdb_reader() {
    round_trip_all_null_varchar(Encoding::DeltaLengthByteArray);
}

#[test]
fn all_null_varchar_plain_round_trips_through_qdb_reader() {
    round_trip_all_null_varchar(Encoding::Plain);
}
