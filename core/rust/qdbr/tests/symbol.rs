mod common;

use parquet::basic::LogicalType;
use parquet::data_type::{ByteArray, ByteArrayType};
use parquet::file::properties::WriterVersion;

use common::{
    decode_file, def_levels_from_nulls, generate_nulls, non_null_only, optional_byte_array_schema,
    qdb_meta_with_format, required_byte_array_schema, write_parquet_column, Null, ALL_NULLS, COUNT,
    VERSIONS,
};
use parquet::file::properties::WriterProperties;
use parquet::format::KeyValue;
use qdb_core::col_type::{ColumnType, ColumnTypeTag};
use std::sync::Arc;

fn symbol_dict_props(version: WriterVersion) -> WriterProperties {
    let qdb_json = qdb_meta_with_format(ColumnType::new(ColumnTypeTag::Symbol, 0), 1);
    WriterProperties::builder()
        .set_writer_version(version)
        .set_dictionary_enabled(true)
        .set_key_value_metadata(Some(vec![KeyValue::new("questdb".to_string(), qdb_json)]))
        .build()
}

fn generate_values(count: usize) -> Vec<ByteArray> {
    // Generate values from a small set to create a realistic dictionary
    let labels = ["alpha", "beta", "gamma", "delta", "epsilon"];
    (0..count)
        .map(|i| ByteArray::from(labels[i % labels.len()]))
        .collect()
}

/// `non_null_values` must be the filtered non-null subset (same order the writer received).
fn assert_symbol(non_null_values: &[ByteArray], nulls: &[bool], data: &[u8]) {
    let row_count = nulls.len();
    assert_eq!(
        data.len(),
        row_count * 4,
        "symbol data should be 4 bytes per row"
    );

    // Build the expected dict indices in insertion order
    let mut dict: Vec<&[u8]> = Vec::new();
    let mut expected_indices = Vec::with_capacity(row_count);
    let mut val_idx = 0;
    for i in 0..row_count {
        if nulls[i] {
            expected_indices.push(i32::MIN);
        } else {
            let val = non_null_values[val_idx].data();
            val_idx += 1;
            let idx = if let Some(pos) = dict.iter().position(|d| *d == val) {
                pos as i32
            } else {
                let pos = dict.len() as i32;
                dict.push(val);
                pos
            };
            expected_indices.push(idx);
        }
    }

    for i in 0..row_count {
        let actual = i32::from_le_bytes(data[i * 4..(i + 1) * 4].try_into().unwrap());
        assert_eq!(
            actual, expected_indices[i],
            "row {i}: symbol index mismatch"
        );
    }
}

fn run_symbol_test(name: &str) {
    for version in &VERSIONS {
        for null in &ALL_NULLS {
            eprintln!("Testing {name} with version={version:?}, null={null:?}");

            let nulls = generate_nulls(COUNT, *null);
            let values = generate_values(COUNT);
            let non_null_values = non_null_only(&values, &nulls);

            let schema = if matches!(null, Null::None) {
                required_byte_array_schema("col", Some(LogicalType::String))
            } else {
                optional_byte_array_schema("col", Some(LogicalType::String))
            };

            let def_levels = def_levels_from_nulls(&nulls);
            let props = symbol_dict_props(*version);
            let buf = write_parquet_column::<ByteArrayType>(
                "col",
                schema,
                &non_null_values,
                Some(&def_levels),
                Arc::new(props),
            );
            let (data, aux) = decode_file(&buf);
            assert!(aux.is_empty(), "symbol should have no aux data");
            assert_symbol(&non_null_values, &nulls, &data);
        }
    }
}

#[test]
fn test_symbol_rle_dictionary() {
    run_symbol_test("Symbol");
}
