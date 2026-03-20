mod common;

use arrow::array::{Array, StringArray};
use common::encode::{
    generate_nulls, make_symbol_column, read_parquet_batches, serialize_as_symbols, write_parquet,
    EncodeEncoding, ALL_NULL_PATTERNS,
};
use qdb_core::col_type::{ColumnType, ColumnTypeTag};
use questdbr::parquet_write::schema::Partition;

const COUNT: usize = 1_000;

const LABELS: [&str; 5] = ["alpha", "beta", "gamma", "delta", "epsilon"];

#[test]
fn test_encode_symbol() {
    // Symbol only supports RleDictionary
    let encoding = EncodeEncoding::RleDictionary;
    for null_pattern in &ALL_NULL_PATTERNS {
        let nulls = generate_nulls(COUNT, *null_pattern);
        let (chars_data, offsets) = serialize_as_symbols(&LABELS);

        let mut keys: Vec<i32> = Vec::with_capacity(COUNT);
        let mut expected: Vec<Option<&str>> = Vec::with_capacity(COUNT);
        let mut val_idx = 0;
        for null in nulls.iter().take(COUNT) {
            if *null {
                keys.push(i32::MIN); // Symbol null sentinel
                expected.push(None);
            } else {
                let label_idx = val_idx % LABELS.len();
                val_idx += 1;
                keys.push(label_idx as i32);
                expected.push(Some(LABELS[label_idx]));
            }
        }

        let keys_bytes = unsafe {
            std::slice::from_raw_parts(
                keys.as_ptr() as *const u8,
                keys.len() * std::mem::size_of::<i32>(),
            )
        };

        let column = make_symbol_column(
            "col",
            ColumnType::new(ColumnTypeTag::Symbol, 0).code(),
            keys_bytes.as_ptr(),
            keys_bytes.len(),
            chars_data.as_ptr(),
            chars_data.len(),
            offsets.as_ptr(),
            offsets.len(),
            COUNT,
            encoding.config(),
        );

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![column],
        };
        let bytes = write_parquet(partition);
        let batches = read_parquet_batches(&bytes);

        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");
        assert_eq!(arr.len(), COUNT);

        for (i, exp) in expected.iter().enumerate() {
            match exp {
                Some(v) => {
                    assert!(
                        !arr.is_null(i),
                        "Symbol {null_pattern:?}: expected non-null at {i}"
                    );
                    assert_eq!(arr.value(i), *v, "Symbol {null_pattern:?}: mismatch at {i}");
                }
                None => {
                    assert!(
                        arr.is_null(i),
                        "Symbol {null_pattern:?}: expected null at {i}"
                    );
                }
            }
        }
    }
}

#[test]
fn test_encode_symbol_all_nulls() {
    let (chars_data, offsets) = serialize_as_symbols(&[]);
    let keys: Vec<i32> = vec![i32::MIN; COUNT];
    let keys_bytes = unsafe {
        std::slice::from_raw_parts(
            keys.as_ptr() as *const u8,
            keys.len() * std::mem::size_of::<i32>(),
        )
    };

    let column = make_symbol_column(
        "col",
        ColumnType::new(ColumnTypeTag::Symbol, 0).code(),
        keys_bytes.as_ptr(),
        keys_bytes.len(),
        chars_data.as_ptr(),
        chars_data.len(),
        offsets.as_ptr(),
        offsets.len(),
        COUNT,
        EncodeEncoding::RleDictionary.config(),
    );

    let partition = Partition {
        table: "test_table".to_string(),
        columns: vec![column],
    };
    let bytes = write_parquet(partition);
    let batches = read_parquet_batches(&bytes);

    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("StringArray");
    assert_eq!(arr.len(), COUNT);
    for i in 0..COUNT {
        assert!(arr.is_null(i), "expected null at {i}");
    }
}
